import logging
import sqlite3
import asyncio
import os
import time
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.enums import ParseMode, ChatType
from aiogram.client.default import DefaultBotProperties
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ================== CONFIG ==================

API_TOKEN = os.getenv("BOT_TOKEN")
if not API_TOKEN:
    raise RuntimeError("BOT_TOKEN not set")

DB_PATH = "stats.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

bot = Bot(
    token=API_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

scheduler: AsyncIOScheduler | None = None

# ================== DATABASE ==================

conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS messages (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    today INTEGER DEFAULT 0,
    yesterday INTEGER DEFAULT 0,
    total INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS daily_stats (
    date TEXT PRIMARY KEY,
    total_messages INTEGER,
    active_users INTEGER,
    top_user_id INTEGER,
    top_user_count INTEGER
)
""")

conn.commit()

# ================== CACHE ==================

user_cache = {}
CACHE_TTL = 300

# ================== HELPERS ==================

async def get_sorted_members(chat_id):
    cache_key = f"sorted_{chat_id}"
    now = time.time()

    if cache_key in user_cache:
        data, ts = user_cache[cache_key]
        if now - ts < CACHE_TTL:
            return data

    cursor.execute("""
        SELECT user_id, username, today, yesterday, total
        FROM messages
        ORDER BY today DESC, total DESC
        LIMIT 20
    """)
    rows = cursor.fetchall()

    result = []
    for uid, name, today, yest, total in rows:
        result.append({
            "user_id": uid,
            "username": name,
            "mention": name,
            "today": today,
            "yesterday": yest,
            "total": total,
        })

    user_cache[cache_key] = (result, now)
    return result

# ================== COMMANDS ==================

@dp.message(Command("status"))
async def status(message: types.Message):
    if message.chat.type == ChatType.CHANNEL:
        return

    members = await get_sorted_members(message.chat.id)

    if not members:
        await message.reply("📊 Пока нет статистики.")
        return

    text = "<b>📊 Статистика сообщений</b>\n\n"
    for i, m in enumerate(members[:10], 1):
        text += (
            f"<b>{i}. {m['username']}</b>\n"
            f"Сегодня: {m['today']} | Вчера: {m['yesterday']} | Всего: {m['total']}\n\n"
        )

    await message.reply(text)

# ================== MESSAGE COUNTER ==================

@dp.message(F.text & ~F.text.startswith("/"))
async def count(message: types.Message):
    if message.from_user.is_bot or message.chat.type == ChatType.CHANNEL:
        return

    uid = message.from_user.id
    name = message.from_user.full_name
    now = datetime.utcnow()

    cursor.execute(
        "SELECT today, total, last_updated FROM messages WHERE user_id=?",
        (uid,)
    )
    row = cursor.fetchone()

    if row:
        last = datetime.fromisoformat(row[2])
        if last.date() < now.date():
            cursor.execute("""
                UPDATE messages
                SET yesterday=today,
                    today=1,
                    total=total+1,
                    username=?,
                    last_updated=CURRENT_TIMESTAMP
                WHERE user_id=?
            """, (name, uid))
        else:
            cursor.execute("""
                UPDATE messages
                SET today=today+1,
                    total=total+1,
                    username=?,
                    last_updated=CURRENT_TIMESTAMP
                WHERE user_id=?
            """, (name, uid))
    else:
        cursor.execute("""
            INSERT INTO messages (user_id, username, today, total)
            VALUES (?, ?, 1, 1)
        """, (uid, name))

    conn.commit()
    user_cache.clear()

# ================== SCHEDULER ==================

async def daily_reset():
    cursor.execute("""
        UPDATE messages
        SET yesterday=today,
            today=0,
            last_updated=CURRENT_TIMESTAMP
    """)
    conn.commit()
    logger.info("📅 Daily reset complete")

# ================== MAIN ==================

async def main():
    global scheduler

    me = await bot.get_me()
    logger.info(f"🤖 Bot started: @{me.username}")

    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(daily_reset, "cron", hour=0, minute=0)
    scheduler.start()

    try:
        await dp.start_polling(bot)
    finally:
        logger.info("Shutting down...")
        scheduler.shutdown(wait=False)
        await bot.session.close()
        conn.close()

if __name__ == "__main__":
    asyncio.run(main())