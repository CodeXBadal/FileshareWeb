"""
shared/config.py — Single source of truth for runtime configuration.

This is the CANONICAL copy. For separate-repo deployment (per audit issue C8),
copy this file VERBATIM into both `FileShare/shared_config.py` and
`Website/shared_config.py`. The bot and the website then both import their
local copy — no sys.path tricks, no `..` walks, no fragile package layout.

All values are loaded from environment variables (with `.env` loaded via
python-dotenv if present). Missing critical variables raise at import time —
fail loud and early instead of producing broken behaviour later.
"""

from __future__ import annotations

import os
import warnings
from pathlib import Path
from typing import List

from dotenv import load_dotenv

# Walk upwards from this file looking for the first `.env`. Works whether this
# file lives in /shared, /FileShare or /Website.
_HERE = Path(__file__).resolve().parent
for candidate in (_HERE, _HERE.parent, _HERE.parent.parent):
    env_file = candidate / ".env"
    if env_file.is_file():
        load_dotenv(env_file)
        break

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _required(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        raise RuntimeError(
            f"Required environment variable '{name}' is missing. "
            f"Copy .env.example to .env and fill it in."
        )
    return value.strip()


def _optional(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw.strip())
    except ValueError as exc:
        raise RuntimeError(f"Env var {name!r} must be an integer, got {raw!r}") from exc


def _bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _csv_ints(name: str) -> List[int]:
    raw = os.environ.get(name, "")
    out: List[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError as exc:
            raise RuntimeError(f"Env var {name!r} contains non-integer {part!r}") from exc
    return out


# --------------------------------------------------------------------------- #
# Telegram bot
# --------------------------------------------------------------------------- #

BOT_TOKEN: str = _required("BOT_TOKEN")
BOT_USERNAME: str = _required("BOT_USERNAME").lstrip("@")
ADMIN_IDS: List[int] = _csv_ints("ADMIN_IDS")

CHANNEL_URL: str = _optional("CHANNEL_URL")
DEVELOPER_URL: str = _optional("DEVELOPER_URL")
NOTIFICATION_CHANNEL: int = _int("NOTIFICATION_CHANNEL", 0)
LOGS_CHANNEL: int = _int("LOGS_CHANNEL", 0)
# Private channel where the bot forwards every upload so Telethon has a stable
# `(chat_id, message_id)` reference (fixes C3 — Bot-API file_id strings are
# unreliable with Telethon's iter_download).
STORAGE_CHANNEL_ID: int = _int("STORAGE_CHANNEL_ID", 0)


# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #

MONGODB_URI: str = _required("MONGODB_URI")
DATABASE_NAME: str = _optional("DATABASE_NAME", "file_sharing_bot")


# --------------------------------------------------------------------------- #
# Website
# --------------------------------------------------------------------------- #

FLASK_WEBSITE_URL: str = _required("FLASK_WEBSITE_URL").rstrip("/")
FLASK_SECRET_KEY: str = _required("FLASK_SECRET_KEY")
ADMIN_PASSWORD_HASH: str = _required("ADMIN_PASSWORD_HASH")


# --------------------------------------------------------------------------- #
# Telethon MTProto (Website streaming)
# --------------------------------------------------------------------------- #
# These three are required only on the Website side, but we surface them here
# so the SAME shared_config.py file can be dropped into both halves of the
# project. The bot ignores them; the website imports them.

TELEGRAM_API_ID: int = _int("TELEGRAM_API_ID", 0)
TELEGRAM_API_HASH: str = _optional("TELEGRAM_API_HASH")
TELEGRAM_SESSION_STRING: str = _optional("TELEGRAM_SESSION_STRING")


# --------------------------------------------------------------------------- #
# Constants / limits
# --------------------------------------------------------------------------- #
# C1: The old code raised RuntimeError when MAX_FILE_SIZE_MB > 20.
# Telethon MTProto handles arbitrarily large files now (no Bot-API 20 MB cap),
# so the new ceiling is Telegram's hard limit: 4096 MB (4 GiB) per file.

MAX_FILE_SIZE_MB: int = _int("MAX_FILE_SIZE_MB", 2000)
MAX_FILE_SIZE: int = MAX_FILE_SIZE_MB * 1024 * 1024
TELEGRAM_HARD_LIMIT_MB: int = 4096  # Telegram protocol ceiling
# Files smaller than this can ALSO be re-served by the Bot API (deep-link
# delivery inside Telegram itself). Larger ones are website-download-only.
TELEGRAM_BOT_API_DOWNLOAD_LIMIT: int = 20 * 1024 * 1024  # bytes (Q1 — kept and used)

ITEMS_PER_PAGE: int = _int("ITEMS_PER_PAGE", 5)
WEBSITE_ITEMS_PER_PAGE: int = _int("WEBSITE_ITEMS_PER_PAGE", 10)

HOST: str = _optional("HOST", "0.0.0.0")
PORT: int = _int("PORT", 5000)
FLASK_DEBUG: bool = _bool("FLASK_DEBUG", False)
ENV: str = _optional("ENV", "development").lower()
IS_PRODUCTION: bool = ENV == "production"

# Persistence path (C7) — must point to a writable dir on the target host.
# Render's free tier wipes everything except `/tmp` on redeploy.
BOT_PERSISTENCE_PATH: str = _optional("BOT_PERSISTENCE_PATH", "/tmp/bot_session.pkl")


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #

if not ADMIN_IDS:
    warnings.warn(
        "ADMIN_IDS is empty: admin commands and panel will be unreachable.",
        RuntimeWarning,
        stacklevel=2,
    )

if MAX_FILE_SIZE_MB > TELEGRAM_HARD_LIMIT_MB:
    raise RuntimeError(
        f"MAX_FILE_SIZE_MB={MAX_FILE_SIZE_MB} exceeds Telegram's hard "
        f"per-file limit of {TELEGRAM_HARD_LIMIT_MB} MB."
    )

if "mongodb" not in MONGODB_URI:
    # S8: Never echo the URI itself in the error message.
    raise RuntimeError("MONGODB_URI does not look like a MongoDB connection string")

# S6: Refuse to start in production with a weak secret key.
if IS_PRODUCTION and len(FLASK_SECRET_KEY) < 32:
    raise RuntimeError(
        "FLASK_SECRET_KEY must be at least 32 characters in production. "
        "Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\""
    )
