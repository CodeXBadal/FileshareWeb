"""
Gunicorn config — P4.

Threaded worker model is the right shape for this app:

  * 1 worker, 16 threads — Telethon runs on a single shared event loop in a
    *background* thread. Multiple workers would create multiple loops, each
    with its own session, which Telegram rejects (only one logged-in instance
    per session string). Threading gives us concurrent downloads without
    multiplying the Telethon connection.
  * `timeout=600` — large file downloads can legitimately take >60 s.
  * `worker_class="gthread"` — IO-bound streaming benefits from GIL-yielding
    Python threads.
  * `post_fork` hook — fires Telethon startup right after Gunicorn forks the
    worker, so the event loop is ready before the first request lands.
"""

from __future__ import annotations

import logging
import os

bind = f"0.0.0.0:{os.environ.get('PORT', '5000')}"
workers = int(os.environ.get("WEB_CONCURRENCY", "1"))
threads = int(os.environ.get("WEB_THREADS", "16"))
worker_class = "gthread"
timeout = int(os.environ.get("WEB_TIMEOUT", "600"))
keepalive = 5
graceful_timeout = 30
accesslog = "-"
errorlog = "-"
loglevel = os.environ.get("LOG_LEVEL", "info")
proc_name = "filebot-website"
preload_app = False  # don't preload; we want each worker to do its own fork-safe startup

logger = logging.getLogger(__name__)


def post_fork(server, worker):
    """Initialise Telethon before this worker takes its first request."""
    try:
        from app import _ensure_started
        _ensure_started()
        worker.log.info("post_fork: Telethon started for worker pid=%s", worker.pid)
    except Exception:
        worker.log.exception("post_fork: failed to start Telethon")


def worker_abort(worker):
    worker.log.warning("worker_abort signal received (pid=%s)", worker.pid)
