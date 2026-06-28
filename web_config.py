"""
Website/web_config.py — Re-export shim for the Flask app.

C8 fix: NO sys.path manipulation. We import directly from `shared_config`
which lives next to this file (a verbatim copy of `shared/config.py`).
"""

from __future__ import annotations

from shared_config import (  # noqa: F401
    ADMIN_PASSWORD_HASH,
    BOT_TOKEN,
    DATABASE_NAME,
    ENV,
    FLASK_DEBUG,
    FLASK_SECRET_KEY,
    FLASK_WEBSITE_URL,
    HOST,
    IS_PRODUCTION,
    MONGODB_URI,
    PORT,
    STORAGE_CHANNEL_ID,
    TELEGRAM_API_HASH,
    TELEGRAM_API_ID,
    TELEGRAM_BOT_API_DOWNLOAD_LIMIT,
    TELEGRAM_SESSION_STRING,
    WEBSITE_ITEMS_PER_PAGE,
)

# Aliases preserving the names used throughout the Flask app.
SECRET_KEY = FLASK_SECRET_KEY
DEBUG = FLASK_DEBUG
ITEMS_PER_PAGE = WEBSITE_ITEMS_PER_PAGE

__all__ = [
    "ADMIN_PASSWORD_HASH",
    "BOT_TOKEN",
    "DATABASE_NAME",
    "DEBUG",
    "ENV",
    "FLASK_WEBSITE_URL",
    "HOST",
    "IS_PRODUCTION",
    "ITEMS_PER_PAGE",
    "MONGODB_URI",
    "PORT",
    "SECRET_KEY",
    "STORAGE_CHANNEL_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_API_ID",
    "TELEGRAM_BOT_API_DOWNLOAD_LIMIT",
    "TELEGRAM_SESSION_STRING",
]
