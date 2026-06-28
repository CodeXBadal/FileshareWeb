# FileBot 2.0

A two-piece file-sharing system built around Telegram:

* **`FileShare/`** — Telegram bot (python-telegram-bot v20) that ingests files
  and hands out share links.
* **`Website/`** — Flask web app that streams those files directly from
  Telegram's CDN via MTProto (Telethon). No 20 MB Bot-API limit.

Both halves share a single MongoDB and a single canonical config file
(`shared/config.py`, copied verbatim into each side as `shared_config.py`).

See [`CHANGES.md`](./CHANGES.md) for a complete list of fixes and features.

---

## Quick start

### 1 · Prerequisites

* Python 3.10+
* A MongoDB Atlas (or self-hosted) cluster
* A Telegram bot token from [@BotFather](https://t.me/BotFather)
* A Telegram **user** account (for the website's Telethon streamer — bots
  can't read other people's uploaded files)
* A **private** Telegram channel that the bot is an admin of
  (the storage channel, see `STORAGE_CHANNEL_ID`)

### 2 · Configure

```bash
cp .env.example .env
# Fill in BOT_TOKEN, MONGODB_URI, FLASK_SECRET_KEY, STORAGE_CHANNEL_ID, etc.

python scripts/generate_admin_hash.py
# Paste the printed ADMIN_PASSWORD_HASH=... into .env

python scripts/generate_session_string.py
# Paste TELEGRAM_SESSION_STRING=... into .env
```

### 3 · Run the bot

```bash
cd FileShare
pip install -r requirements.txt
python bot.py
```

### 4 · Run the website

```bash
cd Website
pip install -r requirements.txt
# Dev:
python app.py
# Prod (Render / any PaaS):
gunicorn -c gunicorn_conf.py app:app
```

### 5 · Deploy to Render

Push the repo, then **Create New → Blueprint**, point at `render.yaml`.
Render will spin up two services (`filebot-web` + `filebot-bot`) with the
environment variables defined in the YAML. Fill in the `sync: false` ones
from the Render dashboard.

---

## Caption directives

When uploading a file you can append directives to the caption:

| Directive       | Meaning                                                                  |
|-----------------|--------------------------------------------------------------------------|
| `expires:7d`    | File auto-deletes after 7 days. Also accepts `Nh`, `Nm`, or `never`.     |
| `max:5`         | At most 5 downloads, then HTTP 410.                                      |

Example caption: `Holiday photos expires:30d max:10`.

The directives are stripped from the caption shown to the recipient.

---

## URLs the website exposes

| Path                      | Purpose                                                              |
|---------------------------|----------------------------------------------------------------------|
| `/`                       | Public homepage with stats.                                          |
| `/d/<token>`              | Loading-spinner landing page; auto-triggers the real download.       |
| `/download/<token>`       | Streams the file (supports Range / 206 Partial Content).             |
| `/raw/<token>`            | Same as `/download/<token>` — used by `/d/<token>`.                  |
| `/folder/<token>`         | Streams a whole folder as a ZIP.                                     |
| `/api/file/<token>`       | Public read-only JSON metadata.                                      |
| `/health`                 | Liveness probe — MongoDB + Telethon + uptime + version.              |
| `/stats`                  | Public JSON stats.                                                   |
| `/admin/login`            | Admin login (no link from the homepage — go here directly).          |
| `/admin`                  | Admin dashboard.                                                     |
| `/admin/files`            | Manage uploaded files.                                               |
| `/admin/users`            | Manage / ban / search users.                                         |

---

## Bot commands

| Command           | Description                                              |
|-------------------|----------------------------------------------------------|
| `/start`          | Show welcome + main menu. Handles `file_<token>` and `folder_<token>` deep links. |
| `/help`           | Help text.                                               |
| `/myfiles`        | Browse your uploaded files.                              |
| `/myfolders`      | Browse your folders.                                     |
| `/folder`         | Start a folder session — uploads land in one folder.     |
| `/done`           | Finish a folder session and get the share link.          |
| `/stats`          | Your personal stats (files, folders, views, storage).    |
| `/search <q>`     | Search your files by name.                               |
| `/cancel`         | Cancel folder or broadcast input.                        |
| `/admin`          | Admin panel (admins only).                               |

---

## Operations

* **Cleanup** — schedule `python scripts/cleanup_db.py` daily. Hard-deletes
  rows soft-deleted more than 30 days ago, scrubs orphans, defensively marks
  expired files.
* **Logs** — auto-purge after 90 days via MongoDB TTL.
* **Health** — point your platform's healthcheck at `/health`. Returns 503
  when Telethon is disconnected.

---

## Notes on architecture

* **Why a `STORAGE_CHANNEL_ID`?** Bot-API `file_id` strings are unreliable
  inputs to Telethon's `iter_download()` — they can return
  `FileIdInvalidError` for files uploaded by other users or after re-auth.
  Forwarding every upload to a private channel gives the streamer a
  permanent `(chat_id, message_id)` reference that Telegram never expires.

* **Why a Telethon string session?** The Bot API has a hard 20 MB download
  limit. A user account talking MTProto has access to the full per-file
  ceiling (4096 MB) and is the only way to stream big files.

* **Why 1 gunicorn worker?** Telethon maintains its own event loop in a
  background thread. A single Telegram session can only be logged in once
  at a time — multiple workers would either get session-locked or rotate
  flood-wait penalties. We get concurrency via threads instead.

---

License: do what you want; no warranty.
