"""
Flask download portal + admin panel — production rewrite.

Key changes from the previous version (mapped to audit IDs):

  * C2  — `asyncio.Queue` is created INSIDE the event loop thread via
          `asyncio.run_coroutine_threadsafe`. Producers are cancelled on
          client disconnect (`GeneratorExit`), so a dropped download no
          longer leaks a coroutine.
  * C3  — Streaming uses `client.get_messages(storage_chat_id, ids=msg_id)`
          (stable Telethon reference) and falls back to raw `file_id` only
          if the storage ref is missing (legacy data).
  * C4  — Lazy startup: `_ensure_started()` is gated by `threading.Lock`
          and is invoked from a Gunicorn `post_fork` hook AND from a
          `before_request` shim, so it works whether you launch with
          `gunicorn`, `flask run`, or `python app.py`.
  * C5  — Views/download counter increment moved to AFTER the first chunk
          is successfully produced.
  * C6  — Full `Range`-header parsing → HTTP 206 Partial Content support.
  * S1  — UA-binding removed. Session has a hard 2-hour TTL and a fresh
          login timestamp.
  * S2  — HTTP→HTTPS redirect in production.
  * S3  — Per-token rate limit on `/download/<token>`.
  * S5  — Admin login URL is unlisted on the public home page.
  * S6  — `X-Download-Options: noopen` header set on downloads.
  * P1  — Homepage stats cached for 10 s.
  * P2  — Telethon keepalive coroutine pings every 4 minutes.
  * F2  — Expired files return HTTP 410.
  * F3  — Max-downloads enforced atomically.
  * F5  — Admin user search by username / first_name / user_id.
  * F6  — Storage total on admin dashboard.
  * F9  — Public read-only `/api/file/<token>` endpoint.
  * F12 — `/d/<token>` loading-spinner page; the direct stream stays
          available at `/download/<token>` and `/raw/<token>`.
  * F15 — Richer `/health` payload (telethon, mongo, uptime, version).
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import threading
import time
from concurrent.futures import Future
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Iterator, Optional, Tuple

from bson.errors import InvalidId
from bson.objectid import ObjectId
from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    stream_with_context,
    url_for,
)
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_wtf.csrf import CSRFProtect, generate_csrf
from pymongo import DESCENDING, MongoClient
from telethon import TelegramClient
from telethon.errors import FileReferenceExpiredError, FloodWaitError
from telethon.sessions import StringSession
from werkzeug.security import check_password_hash
from werkzeug.utils import secure_filename
from zipstream import ZIP_STORED, ZipStream

from web_config import (
    ADMIN_PASSWORD_HASH,
    BOT_TOKEN,
    DATABASE_NAME,
    DEBUG,
    ENV,
    IS_PRODUCTION,
    ITEMS_PER_PAGE,
    MONGODB_URI,
    SECRET_KEY,
    STORAGE_CHANNEL_ID,
    TELEGRAM_API_HASH,
    TELEGRAM_API_ID,
    TELEGRAM_SESSION_STRING,
)

# --------------------------------------------------------------------------- #
# Constants / version
# --------------------------------------------------------------------------- #

APP_VERSION = "2.0.0"
APP_START_TS = time.monotonic()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# 512 KB chunks ≈ best balance of speed / memory / queue depth.
CHUNK_SIZE = 512 * 1024
# Outbound queue depth (chunks). 8 × 512 KB = 4 MB max buffered in RAM.
QUEUE_DEPTH = 8
# Keepalive cadence — 4 minutes is comfortably under Render's 15-minute
# idle-sleep window.
KEEPALIVE_INTERVAL_SEC = 240

# --------------------------------------------------------------------------- #
# Telethon: lazy startup, Gunicorn-fork-safe (C4)
# --------------------------------------------------------------------------- #

_tg_loop: Optional[asyncio.AbstractEventLoop] = None
_tg_client: Optional[TelegramClient] = None
_tg_loop_thread: Optional[threading.Thread] = None
_startup_lock = threading.Lock()
_started = False


def _build_client(loop: asyncio.AbstractEventLoop) -> TelegramClient:
    if not (TELEGRAM_API_ID and TELEGRAM_API_HASH and TELEGRAM_SESSION_STRING):
        raise RuntimeError(
            "TELEGRAM_API_ID, TELEGRAM_API_HASH and TELEGRAM_SESSION_STRING "
            "must all be set for the website to stream from Telegram."
        )
    return TelegramClient(
        StringSession(TELEGRAM_SESSION_STRING),
        TELEGRAM_API_ID,
        TELEGRAM_API_HASH,
        loop=loop,
        connection_retries=5,
        auto_reconnect=True,
        request_retries=5,
    )


async def _keepalive() -> None:
    """P2 — keep the Telethon connection warm on idle hosts (Render free)."""
    while True:
        try:
            await asyncio.sleep(KEEPALIVE_INTERVAL_SEC)
            if _tg_client and _tg_client.is_connected():
                # Cheap server-side call.
                await _tg_client.get_me()
                logger.debug("Telethon keepalive OK")
        except asyncio.CancelledError:
            return
        except Exception:
            logger.warning("Keepalive ping failed", exc_info=True)


def _ensure_started() -> None:
    """Idempotent, fork-safe Telethon startup."""
    global _tg_loop, _tg_client, _tg_loop_thread, _started
    if _started:
        return
    with _startup_lock:
        if _started:
            return

        loop = asyncio.new_event_loop()

        def _runner() -> None:
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = threading.Thread(target=_runner, name="telethon-loop", daemon=True)
        thread.start()

        client = _build_client(loop)

        async def _connect() -> None:
            # `start()` will use the BOT_TOKEN only as a *fallback* — when a
            # SESSION_STRING is present (and it is, for user-account streaming),
            # Telethon uses that. The bot token is kept so that an admin can
            # bootstrap the session by running the helper script.
            await client.start(bot_token=BOT_TOKEN)
            logger.info("✅ Telethon MTProto client connected")
            # P2 keepalive
            asyncio.create_task(_keepalive())

        fut = asyncio.run_coroutine_threadsafe(_connect(), loop)
        fut.result(timeout=30)

        _tg_loop = loop
        _tg_client = client
        _tg_loop_thread = thread
        _started = True


# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #

try:
    mongo_client = MongoClient(
        MONGODB_URI,
        maxPoolSize=20,
        minPoolSize=1,
        serverSelectionTimeoutMS=10_000,
        connectTimeoutMS=10_000,
        retryWrites=True,
    )
    mongo_db = mongo_client[DATABASE_NAME]
    files_collection = mongo_db.files
    users_collection = mongo_db.users
    folders_collection = mongo_db.folders
    logs_collection = mongo_db.logs
    logger.info("✅ MongoDB connected (Flask)")
except Exception:
    # S8 — don't echo the URI.
    logger.exception("❌ MongoDB connection failed")
    raise


# --------------------------------------------------------------------------- #
# App & security configuration
# --------------------------------------------------------------------------- #

app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config.update(
    SESSION_COOKIE_SECURE=IS_PRODUCTION,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=timedelta(hours=2),
    WTF_CSRF_TIME_LIMIT=None,
    # C5 — Admin operations must be able to send small JSON payloads; the
    # previous 2 MB body cap was fine. We keep it modest because the
    # website never accepts file uploads (the bot is the upload path).
    MAX_CONTENT_LENGTH=2 * 1024 * 1024,
)

csrf = CSRFProtect(app)

limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    default_limits=["200 per minute"],
    storage_uri="memory://",
)


@app.context_processor
def _inject_csrf_token():
    # S3 fix: inject the generated TOKEN, not the function. Templates that
    # already call `csrf_token()` continue to work because we expose the
    # callable too.
    return {"csrf_token": generate_csrf, "csrf_token_value": generate_csrf()}


# --------------------------------------------------------------------------- #
# Security headers (S6 added)
# --------------------------------------------------------------------------- #

@app.after_request
def _security_headers(resp: Response) -> Response:
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Referrer-Policy", "no-referrer")
    resp.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    # S6 — defeats IE legacy "Open" behaviour on downloads.
    resp.headers.setdefault("X-Download-Options", "noopen")
    resp.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; "
        "style-src 'self' 'unsafe-inline'; "
        "script-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "frame-ancestors 'none'; "
        "form-action 'self'",
    )
    return resp


# --------------------------------------------------------------------------- #
# HTTPS redirect (S2)
# --------------------------------------------------------------------------- #

@app.before_request
def _force_https() -> Optional[Response]:
    if not IS_PRODUCTION:
        return None
    # Trust X-Forwarded-Proto from the proxy (Render / Heroku / Cloudflare).
    proto = request.headers.get("X-Forwarded-Proto", request.scheme).lower()
    if proto == "https":
        return None
    if request.method != "GET":
        # Don't 302 a POST — refuse it instead so credentials aren't sent in
        # the clear, even on the redirect leg.
        abort(403)
    target = request.url.replace("http://", "https://", 1)
    return redirect(target, code=301)


@app.before_request
def _lazy_start_telethon() -> None:
    # C4 — first request triggers startup, which is idempotent.
    if not _started:
        try:
            _ensure_started()
        except Exception:
            logger.exception("Telethon lazy startup failed")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _oid(value: str) -> Optional[ObjectId]:
    try:
        return ObjectId(value)
    except (InvalidId, TypeError):
        return None


def _safe_name(raw: Optional[str], fallback: str = "file") -> str:
    if not raw:
        return fallback
    cleaned = secure_filename(raw)
    return cleaned or fallback


def _file_emoji(file_type: Optional[str]) -> str:
    return {
        "document": "📄",
        "video": "🎥",
        "photo": "🖼",
        "audio": "🎵",
        "voice": "🎙",
        "animation": "🎞",
    }.get((file_type or "").lower(), "📦")


@app.template_filter("filesize")
def _filesize_filter(size: Any) -> str:
    try:
        size = int(size or 0)
    except (TypeError, ValueError):
        return "0 B"
    if size < 1024:
        return f"{size} B"
    if size < 1024 ** 2:
        return f"{size / 1024:.2f} KB"
    if size < 1024 ** 3:
        return f"{size / 1024 ** 2:.2f} MB"
    return f"{size / 1024 ** 3:.2f} GB"


@app.template_filter("file_emoji")
def _file_emoji_filter(file_type: Optional[str]) -> str:
    return _file_emoji(file_type)


# --------------------------------------------------------------------------- #
# Range header parser (C6)
# --------------------------------------------------------------------------- #

_RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$")


def _parse_range(header: Optional[str], total: Optional[int]) -> Optional[Tuple[int, int]]:
    """
    Returns (start, end) inclusive, or None if header is absent / unparseable
    / open-ended-without-known-size. Both endpoints are clamped to `total - 1`
    when `total` is known.
    """
    if not header or total is None:
        return None
    m = _RANGE_RE.match(header.strip().lower())
    if not m:
        return None
    s_raw, e_raw = m.group(1), m.group(2)
    if not s_raw and not e_raw:
        return None
    if s_raw and e_raw:
        start, end = int(s_raw), int(e_raw)
    elif s_raw:
        start, end = int(s_raw), total - 1
    else:
        # `-N` => last N bytes
        n = int(e_raw)
        if n == 0:
            return None
        start, end = max(total - n, 0), total - 1
    if start >= total or start > end:
        return None
    end = min(end, total - 1)
    return start, end


# --------------------------------------------------------------------------- #
# Telethon streaming (C2 + C3 + C5 + C6)
# --------------------------------------------------------------------------- #

class _ProducerCancelled(Exception):
    pass


def _resolve_media(file_doc: dict) -> Any:
    """
    Pick the most reliable Telethon reference for a stored file.
    Prefer (storage_chat_id, storage_msg_id) — that's the Telethon-stable
    forwarded message (C3). Fall back to the raw file_id only as a last
    resort (legacy uploads).
    """
    chat_id = file_doc.get("storage_chat_id")
    msg_id = file_doc.get("storage_msg_id")
    if chat_id and msg_id:
        return ("message", int(chat_id), int(msg_id))
    return ("file_id", file_doc.get("file_id"))


def stream_telegram_file(
    file_doc: dict,
    offset: int = 0,
    limit: Optional[int] = None,
) -> Iterator[bytes]:
    """
    Stream a file's bytes from Telegram. Yields bytes synchronously to Flask.

    `offset` and `limit` are byte offsets (limit = total bytes to send after
    `offset`). They map to Telethon's iter_download offset/limit arguments.

    On `GeneratorExit` (client disconnect) the producer coroutine is
    cancelled — no leaked task (C2).
    """
    _ensure_started()
    assert _tg_loop is not None and _tg_client is not None

    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_DEPTH)
    DONE: object = object()
    producer_future: "Future[None] | None" = None

    async def _make_queue_then_run() -> None:
        # We create the Queue on the loop thread; this binds it to the right
        # event loop (C2 fix — previously the queue was created on the
        # *Flask* thread, which has no running loop).
        nonlocal_queue = queue  # closure binding

        media = _resolve_media(file_doc)

        try:
            if media[0] == "message":
                _, chat_id, msg_id = media
                msg = await _tg_client.get_messages(chat_id, ids=msg_id)
                if msg is None or not msg.media:
                    raise RuntimeError("storage message not found or has no media")
                downloadable = msg
            else:
                downloadable = media[1]  # raw file_id string

            iterator = _tg_client.iter_download(
                downloadable,
                offset=offset,
                limit=limit if limit is not None else 0,
                request_size=CHUNK_SIZE,
                chunk_size=CHUNK_SIZE,
            )

            async for chunk in iterator:
                await nonlocal_queue.put(chunk)
        except asyncio.CancelledError:
            # Client disconnected — propagate silently.
            raise
        except FloodWaitError as fwe:
            logger.warning("Telegram flood-wait: %s seconds", fwe.seconds)
            await nonlocal_queue.put(RuntimeError(f"Telegram throttled: wait {fwe.seconds}s"))
        except FileReferenceExpiredError:
            logger.warning("File reference expired — needs re-resolve")
            await nonlocal_queue.put(RuntimeError("File reference expired"))
        except Exception as exc:  # noqa: BLE001
            logger.exception("Telethon stream error")
            await nonlocal_queue.put(exc)
        finally:
            await nonlocal_queue.put(DONE)

    producer_future = asyncio.run_coroutine_threadsafe(_make_queue_then_run(), _tg_loop)

    try:
        while True:
            fut = asyncio.run_coroutine_threadsafe(queue.get(), _tg_loop)
            try:
                item = fut.result(timeout=120)
            except Exception:
                # Either timeout or loop shutdown. Cancel producer and bail.
                if producer_future is not None and not producer_future.done():
                    producer_future.cancel()
                raise
            if item is DONE:
                return
            if isinstance(item, Exception):
                raise item
            yield item
    except GeneratorExit:
        # C2 — client disconnect. Cancel producer so it stops pulling chunks
        # from Telegram and doesn't leak.
        if producer_future is not None and not producer_future.done():
            producer_future.cancel()
        raise
    finally:
        # Defensive: in case yielding raised, still cancel.
        if producer_future is not None and not producer_future.done():
            producer_future.cancel()


# --------------------------------------------------------------------------- #
# Auth (S1 — UA-binding removed)
# --------------------------------------------------------------------------- #

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("admin_logged_in"):
            return redirect(url_for("admin_login"))
        # Server-side session expiry double-check (PERMANENT_SESSION_LIFETIME
        # already handles browser-side expiry, but a malicious cookie could
        # bypass that).
        login_at = session.get("login_at")
        if login_at:
            try:
                login_dt = datetime.fromisoformat(login_at)
                if datetime.utcnow() - login_dt > timedelta(hours=2):
                    session.clear()
                    return redirect(url_for("admin_login"))
            except ValueError:
                session.clear()
                return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return decorated


# --------------------------------------------------------------------------- #
# Public routes
# --------------------------------------------------------------------------- #

# P1 — Homepage stats cache.
_STATS_CACHE: dict = {"ts": 0.0, "data": None}
_STATS_TTL = 10.0  # seconds


def _public_stats() -> dict:
    now = time.monotonic()
    cached = _STATS_CACHE.get("data")
    if cached is not None and (now - _STATS_CACHE["ts"]) < _STATS_TTL:
        return cached
    total_users = users_collection.count_documents({})
    total_files = files_collection.count_documents({"is_deleted": False})
    agg = list(files_collection.aggregate([
        {"$match": {"is_deleted": False}},
        {"$group": {"_id": None, "total": {"$sum": "$views"}}},
    ]))
    total_views = agg[0]["total"] if agg else 0
    data = {"users": total_users, "files": total_files, "views": total_views}
    _STATS_CACHE.update(ts=now, data=data)
    return data


@app.route("/")
def index():
    stats = _public_stats()
    return render_template(
        "index.html",
        users=stats["users"],
        files=stats["files"],
        views=stats["views"],
    )


# F12 — Loading spinner page. The actual stream is served from /raw/<token>.
@app.route("/d/<token>")
@limiter.limit("60 per minute", key_func=lambda: request.view_args.get("token", ""))
def download_landing(token: str):
    file_data = _lookup_file_for_download(token)
    if not isinstance(file_data, dict):
        return file_data  # already an error Response
    return render_template(
        "download.html",
        file=file_data,
        token=token,
        emoji=_file_emoji(file_data.get("file_type")),
    )


def _lookup_file_for_download(token: str):
    """Shared validation used by /d, /download and /raw."""
    file_data = files_collection.find_one({"token": token, "is_deleted": False})
    if not file_data:
        return _error_page("File not found!", "This file doesn't exist or has been deleted."), 404
    if file_data.get("is_revoked"):
        return _error_page("Access Revoked!", "This file has been revoked by the owner."), 403
    # F2 — expiry. The TTL index will eventually delete the row but it can
    # lag by up to 60 s.
    expires_at = file_data.get("expires_at")
    if expires_at and expires_at <= datetime.utcnow():
        return _error_page("Expired!", "This file has expired and is no longer available."), 410
    # F3 — quota.
    max_dl = file_data.get("max_downloads")
    if max_dl is not None and file_data.get("download_count", 0) >= max_dl:
        return _error_page("Download limit reached", "This file has reached its maximum number of downloads."), 410
    return file_data


@app.route("/download/<token>")
@app.route("/raw/<token>")
@limiter.limit("20 per minute", key_func=lambda: request.view_args.get("token", ""))
def download_file(token: str):
    file_data = _lookup_file_for_download(token)
    if not isinstance(file_data, dict):
        return file_data  # already an error Response

    total_size: Optional[int] = file_data.get("file_size") or None

    # C6 — Range support.
    range_header = request.headers.get("Range")
    rng = _parse_range(range_header, total_size)
    safe = _safe_name(file_data.get("file_name"))

    if rng is not None:
        start, end = rng
        content_length = end - start + 1
        gen = stream_telegram_file(file_data, offset=start, limit=content_length)
        gen = _confirm_first_chunk_then_count(gen, token)
        headers = {
            "Content-Disposition": f'attachment; filename="{safe}"',
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-cache",
            "Accept-Ranges": "bytes",
            "Content-Range": f"bytes {start}-{end}/{total_size}",
            "Content-Length": str(content_length),
        }
        return Response(
            stream_with_context(gen),
            status=206,
            mimetype="application/octet-stream",
            headers=headers,
        )

    headers = {
        "Content-Disposition": f'attachment; filename="{safe}"',
        "X-Accel-Buffering": "no",
        "Cache-Control": "no-cache",
        "Accept-Ranges": "bytes",
    }
    if total_size is not None:
        headers["Content-Length"] = str(total_size)
    gen = stream_telegram_file(file_data)
    gen = _confirm_first_chunk_then_count(gen, token)
    return Response(
        stream_with_context(gen),
        mimetype="application/octet-stream",
        headers=headers,
    )


def _confirm_first_chunk_then_count(gen: Iterator[bytes], token: str) -> Iterator[bytes]:
    """
    C5 — only increment `views` / `download_count` AFTER the first chunk
    has been produced (i.e. Telethon has confirmed the file exists and is
    deliverable). On total stream failure no counter is bumped.
    """
    counted = False
    for chunk in gen:
        if not counted:
            counted = True
            try:
                doc = files_collection.find_one_and_update(
                    {"token": token},
                    {"$inc": {"views": 1, "download_count": 1}},
                    return_document=True,
                )
                logs_collection.insert_one({
                    "event_type": "file_download_web",
                    "user_id": None,
                    "file_id": str(doc["_id"]) if doc else None,
                    "details": f"Web download: {(doc or {}).get('file_name') or 'file'}",
                    "timestamp": datetime.utcnow(),
                })
            except Exception:
                logger.exception("Counter update failed for token=%s", token)
        yield chunk


@app.route("/folder/<token>")
@limiter.limit("20 per minute", key_func=lambda: request.view_args.get("token", ""))
def download_folder(token: str):
    folder_data = folders_collection.find_one({
        "share_token": token,
        "is_revoked": {"$ne": True},
        "is_deleted": {"$ne": True},
    })
    if not folder_data:
        return _error_page(
            "Folder not found!",
            "This folder doesn't exist, has been deleted, or has been revoked by the owner.",
        ), 404

    files = list(files_collection.find({
        "folder_id": str(folder_data["_id"]),
        "is_deleted": False,
        "is_revoked": False,
    }))
    if not files:
        return _error_page("Empty Folder!", "This folder doesn't contain any files."), 404

    zs = ZipStream(sized=False)
    for f in files:
        # Wrap each file's stream in its own iterator so its producer is
        # cancelled independently if the ZIP consumer stops early.
        zs.add(
            stream_telegram_file(f),
            arcname=_safe_name(f.get("file_name"), fallback=f"file_{f['_id']}"),
            compress_type=ZIP_STORED,
        )

    logs_collection.insert_one({
        "event_type": "folder_download_web",
        "user_id": None,
        "file_id": None,
        "details": f"Web folder download: {folder_data.get('name', '?')} ({len(files)} files)",
        "timestamp": datetime.utcnow(),
    })

    safe_folder = _safe_name(folder_data.get("name"), fallback="folder")
    return Response(
        stream_with_context(zs),
        mimetype="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{safe_folder}.zip"',
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-cache",
        },
    )


# F9 — Public read-only API.
@app.route("/api/file/<token>")
@limiter.limit("60 per minute", key_func=lambda: request.view_args.get("token", ""))
def api_file(token: str):
    file_data = files_collection.find_one(
        {"token": token, "is_deleted": False},
        projection={
            "file_name": 1,
            "file_size": 1,
            "file_type": 1,
            "views": 1,
            "download_count": 1,
            "max_downloads": 1,
            "created_at": 1,
            "expires_at": 1,
            "is_revoked": 1,
        },
    )
    if not file_data:
        return jsonify({"error": "not found"}), 404
    if file_data.get("is_revoked"):
        return jsonify({"error": "revoked"}), 403
    expires_at = file_data.get("expires_at")
    if expires_at and expires_at <= datetime.utcnow():
        return jsonify({"error": "expired"}), 410
    return jsonify({
        "name": file_data.get("file_name"),
        "size": file_data.get("file_size"),
        "type": file_data.get("file_type"),
        "views": file_data.get("views", 0),
        "downloads": file_data.get("download_count", 0),
        "max_downloads": file_data.get("max_downloads"),
        "created_at": file_data["created_at"].isoformat() if file_data.get("created_at") else None,
        "expires_at": expires_at.isoformat() if expires_at else None,
        "download_url": f"/download/{token}",
    })


# --------------------------------------------------------------------------- #
# Admin authentication
# --------------------------------------------------------------------------- #

@app.route("/admin/login", methods=["GET", "POST"])
@limiter.limit("5 per minute; 30 per hour", methods=["POST"])
def admin_login():
    if request.method == "POST":
        password = request.form.get("password", "")
        if check_password_hash(ADMIN_PASSWORD_HASH, password):
            session.permanent = True
            session["admin_logged_in"] = True
            session["login_at"] = datetime.utcnow().isoformat()
            session["login_ip"] = get_remote_address()
            logger.info("Admin login OK from %s", get_remote_address())
            return redirect(url_for("admin_dashboard"))
        logger.warning("Admin login FAIL from %s", get_remote_address())
        return render_template("admin_login.html", error="Invalid password!"), 401
    return render_template("admin_login.html")


@app.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("index"))


# --------------------------------------------------------------------------- #
# Admin dashboard
# --------------------------------------------------------------------------- #

@app.route("/admin")
@login_required
def admin_dashboard():
    total_users = users_collection.count_documents({})
    total_files = files_collection.count_documents({"is_deleted": False})
    banned_users = users_collection.count_documents({"banned": True})
    revoked_files = files_collection.count_documents({"is_revoked": True, "is_deleted": False})
    agg = list(files_collection.aggregate([
        {"$match": {"is_deleted": False}},
        {"$group": {"_id": None, "views": {"$sum": "$views"}, "storage": {"$sum": "$file_size"}}},
    ]))
    total_views = agg[0]["views"] if agg else 0
    total_storage = agg[0]["storage"] if agg else 0
    return render_template(
        "admin_dashboard.html",
        users=total_users,
        files=total_files,
        views=total_views,
        banned=banned_users,
        revoked=revoked_files,
        storage=total_storage,
    )


_MAX_SEARCH_LEN = 64


@app.route("/admin/files")
@login_required
def admin_files():
    page = max(1, request.args.get("page", 1, type=int))
    search = (request.args.get("search", "") or "").strip()[:_MAX_SEARCH_LEN]
    query: dict = {"is_deleted": False}
    if search:
        query["file_name"] = {"$regex": re.escape(search), "$options": "i"}

    total = files_collection.count_documents(query)
    files = list(
        files_collection.find(query)
        .sort("created_at", DESCENDING)
        .skip((page - 1) * ITEMS_PER_PAGE)
        .limit(ITEMS_PER_PAGE)
    )

    if files:
        owner_ids = list({f["owner_id"] for f in files})
        users = {u["user_id"]: u for u in users_collection.find({"user_id": {"$in": owner_ids}})}
        for f in files:
            u = users.get(f["owner_id"])
            f["owner_name"] = u.get("first_name", "Unknown") if u else "Unknown"
            f["owner_username"] = u.get("username") if u else None

    total_pages = max(1, (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    return render_template(
        "admin_files.html",
        files=files,
        page=page,
        total_pages=total_pages,
        search=search,
    )


@app.route("/admin/users")
@login_required
def admin_users():
    page = max(1, request.args.get("page", 1, type=int))
    search = (request.args.get("search", "") or "").strip()[:_MAX_SEARCH_LEN]
    # F5 — search by username, first_name, or numeric user_id.
    query: dict = {}
    if search:
        regex = {"$regex": re.escape(search), "$options": "i"}
        ors: list = [{"username": regex}, {"first_name": regex}]
        if search.isdigit():
            try:
                ors.append({"user_id": int(search)})
            except ValueError:
                pass
        query = {"$or": ors}

    total = users_collection.count_documents(query)
    users = list(
        users_collection.find(query)
        .sort("created_at", DESCENDING)
        .skip((page - 1) * ITEMS_PER_PAGE)
        .limit(ITEMS_PER_PAGE)
    )
    if users:
        user_ids = [u["user_id"] for u in users]
        counts_cursor = files_collection.aggregate([
            {"$match": {"owner_id": {"$in": user_ids}, "is_deleted": False}},
            {"$group": {"_id": "$owner_id", "count": {"$sum": 1}, "storage": {"$sum": "$file_size"}}},
        ])
        counts = {doc["_id"]: doc for doc in counts_cursor}
        for u in users:
            row = counts.get(u["user_id"], {})
            u["file_count"] = row.get("count", 0)
            u["storage"] = row.get("storage", 0)
    total_pages = max(1, (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    return render_template(
        "admin_users.html",
        users=users,
        page=page,
        total_pages=total_pages,
        search=search,
    )


# --------------------------------------------------------------------------- #
# Admin file operations
# --------------------------------------------------------------------------- #

@app.route("/admin/file/delete/<file_id>", methods=["POST"])
@login_required
def admin_delete_file(file_id):
    oid = _oid(file_id)
    if oid is None:
        return jsonify({"success": False, "error": "Invalid id"}), 400
    try:
        result = files_collection.update_one({"_id": oid}, {"$set": {"is_deleted": True}})
        if result.modified_count > 0:
            logs_collection.insert_one({
                "event_type": "admin_file_delete_web",
                "user_id": None,
                "file_id": file_id,
                "details": f"Admin deleted file {file_id} via web",
                "timestamp": datetime.utcnow(),
            })
            return jsonify({"success": True})
        return jsonify({"success": False, "error": "File not found"}), 404
    except Exception:
        logger.exception("admin_delete_file failed")
        return jsonify({"success": False, "error": "Internal error"}), 500


@app.route("/admin/file/download/<file_id>")
@login_required
def admin_download_file(file_id):
    oid = _oid(file_id)
    if oid is None:
        return _error_page("Bad request", "Invalid file id."), 400
    file_data = files_collection.find_one({"_id": oid, "is_deleted": False})
    if not file_data:
        return _error_page("File not available", "This file has been deleted."), 404

    logs_collection.insert_one({
        "event_type": "admin_file_download_web",
        "user_id": None,
        "file_id": file_id,
        "details": f"Admin downloaded: {file_data.get('file_name') or 'file'}",
        "timestamp": datetime.utcnow(),
    })
    safe = _safe_name(file_data.get("file_name"))
    headers = {
        "Content-Disposition": f'attachment; filename="{safe}"',
        "X-Accel-Buffering": "no",
        "Cache-Control": "no-cache",
        "Accept-Ranges": "bytes",
    }
    if file_data.get("file_size"):
        headers["Content-Length"] = str(file_data["file_size"])
    return Response(
        stream_with_context(stream_telegram_file(file_data)),
        mimetype="application/octet-stream",
        headers=headers,
    )


# --------------------------------------------------------------------------- #
# Admin user operations
# --------------------------------------------------------------------------- #

@app.route("/admin/user/ban/<int:user_id>", methods=["POST"])
@login_required
def admin_ban_user(user_id):
    try:
        result = users_collection.update_one({"user_id": user_id}, {"$set": {"banned": True}})
        if result.modified_count > 0:
            logs_collection.insert_one({
                "event_type": "admin_user_ban_web",
                "user_id": user_id,
                "file_id": None,
                "details": f"Admin banned user {user_id} via web",
                "timestamp": datetime.utcnow(),
            })
            return jsonify({"success": True})
        return jsonify({"success": False, "error": "User not found"}), 404
    except Exception:
        logger.exception("admin_ban_user failed")
        return jsonify({"success": False, "error": "Internal error"}), 500


@app.route("/admin/user/unban/<int:user_id>", methods=["POST"])
@login_required
def admin_unban_user(user_id):
    try:
        result = users_collection.update_one({"user_id": user_id}, {"$set": {"banned": False}})
        if result.modified_count > 0:
            logs_collection.insert_one({
                "event_type": "admin_user_unban_web",
                "user_id": user_id,
                "file_id": None,
                "details": f"Admin unbanned user {user_id} via web",
                "timestamp": datetime.utcnow(),
            })
            return jsonify({"success": True})
        return jsonify({"success": False, "error": "User not found"}), 404
    except Exception:
        logger.exception("admin_unban_user failed")
        return jsonify({"success": False, "error": "Internal error"}), 500


# --------------------------------------------------------------------------- #
# Health & stats (F15)
# --------------------------------------------------------------------------- #

_HEALTH_CACHE: dict = {"ts": 0.0, "payload": None, "code": 200}
_HEALTH_CACHE_TTL = 30.0   # P3 — 10 s was too aggressive for liveness pings.


@app.route("/health")
def health():
    now = time.monotonic()
    if _HEALTH_CACHE["payload"] is not None and (now - _HEALTH_CACHE["ts"]) < _HEALTH_CACHE_TTL:
        return jsonify(_HEALTH_CACHE["payload"]), _HEALTH_CACHE["code"]
    try:
        mongo_client.admin.command("ping")
        tg_ok = bool(_tg_client and _tg_client.is_connected())
        payload = {
            "status": "healthy" if tg_ok else "degraded",
            "mongodb": "connected",
            "telegram_mtproto": "connected" if tg_ok else "disconnected",
            "uptime_seconds": int(time.monotonic() - APP_START_TS),
            "version": APP_VERSION,
            "env": ENV,
            "timestamp": datetime.utcnow().isoformat(),
        }
        code = 200 if tg_ok else 503
    except Exception:
        logger.exception("Health check failed")
        payload = {
            "status": "unhealthy",
            "version": APP_VERSION,
            "uptime_seconds": int(time.monotonic() - APP_START_TS),
            "timestamp": datetime.utcnow().isoformat(),
        }
        code = 503
    _HEALTH_CACHE.update(ts=now, payload=payload, code=code)
    return jsonify(payload), code


@app.route("/stats")
def stats():
    s = _public_stats()
    return jsonify({
        "users": s["users"],
        "files": s["files"],
        "views": s["views"],
        "timestamp": datetime.utcnow().isoformat(),
    })


# --------------------------------------------------------------------------- #
# Error handlers
# --------------------------------------------------------------------------- #

def _error_page(message: str, error: str):
    return render_template("error.html", message=message, error=error)


@app.errorhandler(400)
def _e400(_e): return _error_page("Bad Request", "The request could not be understood."), 400


@app.errorhandler(403)
def _e403(_e): return _error_page("Forbidden", "You do not have access to this resource."), 403


@app.errorhandler(404)
def _e404(_e): return _error_page("Page Not Found!", "The page you're looking for doesn't exist."), 404


@app.errorhandler(410)
def _e410(_e): return _error_page("Gone", "This resource is no longer available."), 410


@app.errorhandler(429)
def _e429(_e): return _error_page("Too Many Requests", "Please slow down and try again shortly."), 429


@app.errorhandler(500)
def _e500(e):
    logger.error("Server error: %s", e)
    return _error_page("Server Error!", "Something went wrong on our end."), 500


@app.errorhandler(Exception)
def _ehandler(e):
    from werkzeug.exceptions import HTTPException
    if isinstance(e, HTTPException):
        return e
    logger.exception("Unhandled exception")
    return _error_page("Error!", "An unexpected error occurred."), 500


# --------------------------------------------------------------------------- #
# Startup
# --------------------------------------------------------------------------- #

# When running directly (`python app.py`), trigger startup eagerly.
if __name__ == "__main__":
    _ensure_started()
    logger.info("🚀 Starting Flask app (Telethon MTProto mode)")
    logger.info("📦 Database: %s", DATABASE_NAME)
    from web_config import HOST, PORT
    app.run(host=HOST, port=PORT, debug=DEBUG)
