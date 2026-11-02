#!/usr/bin/env python3
"""
Telegram → X (Twitter) Tweet Scheduler Bot
Max 5 posts/day. Photo + caption → queue → post via X API.
"""

import os
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
import tweepy
from apscheduler.schedulers.asyncio import AsyncIOScheduler

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
X_API_KEY = os.getenv("X_API_KEY")
X_API_SECRET = os.getenv("X_API_SECRET")
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN")
X_ACCESS_TOKEN_SECRET = os.getenv("X_ACCESS_TOKEN_SECRET")
ALLOWED_USER_ID = os.getenv("ALLOWED_USER_ID")  # optional numeric ID
MONITORED = [a.strip() for a in os.getenv("MONITORED_ACCOUNTS", "OpenAI").split(",") if a.strip()]
DAILY_LIMIT = 5
DB_PATH = Path("scheduler.db")
MEDIA_DIR = Path("media")
MEDIA_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ── Database ────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            caption TEXT,
            media_path TEXT,
            scheduled_at TEXT,
            status TEXT DEFAULT 'queued',
            posted_id TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()

def get_today_count() -> int:
    today = datetime.now(timezone.utc).date().isoformat()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT COUNT(*) FROM posts WHERE date(scheduled_at) = ? AND status = 'posted'",
        (today,),
    )
    count = c.fetchone()[0]
    conn.close()
    return count

def next_available_slot() -> datetime:
    """Next slot after current time, respecting daily limit (spread across day)."""
    now = datetime.now(timezone.utc)
    today = now.date()
    count = get_today_count()
    if count >= DAILY_LIMIT:
        # Next day, first slot ~09:00 UTC
        next_day = today + timedelta(days=1)
        return datetime(next_day.year, next_day.month, next_day.day, 9, 0, tzinfo=timezone.utc)
    # Spread remaining slots through the rest of the day (or next day if late)
    remaining = DAILY_LIMIT - count
    hours_left = max(1, 24 - now.hour)
    interval = hours_left / remaining
    slot = now + timedelta(hours=interval)
    return slot.replace(minute=0, second=0, microsecond=0)

def add_post(caption: str, media_path: str, scheduled_at: datetime) -> int:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO posts (caption, media_path, scheduled_at, created_at) VALUES (?, ?, ?, ?)",
        (caption, media_path, scheduled_at.isoformat(), datetime.now(timezone.utc).isoformat()),
    )
    pid = c.lastrowid
    conn.commit()
    conn.close()
    return pid

def get_due_posts():
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT id, caption, media_path FROM posts WHERE status = 'queued' AND scheduled_at <= ?",
        (now,),
    )
    rows = c.fetchall()
    conn.close()
    return rows

def mark_posted(pid: int, tweet_id: str):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE posts SET status = 'posted', posted_id = ? WHERE id = ?", (tweet_id, pid))
    conn.commit()
    conn.close()

# ── X / Twitter client ──────────────────────────────────────────────────────
def get_x_clients():
    auth = tweepy.OAuth1UserHandler(X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)          # v1.1 for media upload
    client = tweepy.Client(         # v2 for creating tweets
        consumer_key=X_API_KEY,
        consumer_secret=X_API_SECRET,
        access_token=X_ACCESS_TOKEN,
        access_token_secret=X_ACCESS_TOKEN_SECRET,
    )
    return api, client

async def post_to_x(caption: str, media_path: str | None) -> str | None:
    try:
        api, client = get_x_clients()
        media_ids = []
        if media_path and Path(media_path).exists():
            media = api.media_upload(filename=media_path)
            media_ids.append(media.media_id)
        resp = client.create_tweet(text=caption[:280], media_ids=media_ids or None)
        return str(resp.data["id"])
    except Exception as e:
        logger.error(f"X post failed: {e}")
        return None

# ── Scheduler job ───────────────────────────────────────────────────────────
async def check_and_post():
    due = get_due_posts()
    for pid, caption, media_path in due:
        tweet_id = await post_to_x(caption, media_path)
        if tweet_id:
            mark_posted(pid, tweet_id)
            logger.info(f"Posted {pid} → {tweet_id}")
        else:
            logger.warning(f"Failed to post {pid}")

# ── Telegram handlers ───────────────────────────────────────────────────────
def authorized(update: Update) -> bool:
    if not ALLOWED_USER_ID:
        return True
    return str(update.effective_user.id) == ALLOWED_USER_ID

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.reply_text(
        "Telegram Tweet Scheduler ready.\n"
        f"Daily limit: {DAILY_LIMIT} posts.\n"
        "Send a photo + caption to queue it.\n"
        "Commands: /status /accounts"
    )

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    count = get_today_count()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, scheduled_at, status FROM posts ORDER BY id DESC LIMIT 5")
    recent = c.fetchall()
    conn.close()
    text = f"Today posted: {count}/{DAILY_LIMIT}\nNext slot: {next_available_slot().isoformat()}\n\nRecent:\n"
    for r in recent:
        text += f"#{r[0]} {r[1][:16]} [{r[2]}]\n"
    await update.message.reply_text(text)

async def accounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.reply_text(f"Monitoring: {', '.join('@' + a for a in MONITORED)}\n(Reply logic is a stub — extend monitor_accounts)")

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    message = update.message
    caption = message.caption or "No caption"
    # Download largest photo
    photo = message.photo[-1]
    file = await photo.get_file()
    media_path = MEDIA_DIR / f"{photo.file_unique_id}.jpg"
    await file.download_to_drive(custom_path=str(media_path))

    slot = next_available_slot()
    count = get_today_count()
    if count >= DAILY_LIMIT:
        await message.reply_text(
            f"Daily quota full ({DAILY_LIMIT}/day).\n"
            f"Queued for next available: {slot.strftime('%Y-%m-%d %H:%M UTC')}"
        )
    else:
        await message.reply_text(
            f"Queued for {slot.strftime('%Y-%m-%d %H:%M UTC')}\n"
            f"({count + 1}/{DAILY_LIMIT} today)"
        )

    pid = add_post(caption, str(media_path), slot)
    logger.info(f"Queued post {pid} for {slot}")

# ── Stub for monitoring / replies ───────────────────────────────────────────
async def monitor_accounts():
    """Placeholder: poll monitored accounts and draft replies.
    Extend with your own AI call or simple templates.
    """
    logger.info(f"Monitor stub running for: {MONITORED}")
    # Example: you could use client.search_recent_tweets or get_users_tweets here
    # then generate reply and post via client.create_tweet(in_reply_to_tweet_id=...)

# ── Main ────────────────────────────────────────────────────────────────────
def main():
    if not TELEGRAM_TOKEN or not X_API_KEY:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN or X credentials in .env")

    init_db()

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("accounts", accounts))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))

    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_and_post, "interval", minutes=1)
    scheduler.add_job(monitor_accounts, "interval", minutes=15)
    scheduler.start()

    logger.info("Bot started. Polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
