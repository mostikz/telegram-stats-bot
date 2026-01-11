import asyncio
import logging
import os
import sqlite3
import time
from datetime import datetime

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode, ChatType
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ================== НАСТРОЙКИ ==================

API_TOKEN = os.getenv("BOT_TOKEN")
if not API_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN не задан в переменных окружения")

DB_PATH = "/var/data/stats.db"  # Persistent Disk Render

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ================== BOT ==================

bot = Bot(
    token=API_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

# ================== DB ==================

conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS messages (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    today INTEGER DEFAULT 0,
    yesterday INTEGER DEFAULT 0,
    total INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

    data = await get_sorted_members(message.chat.id)

    if not data:
        await message.answer("📊 Пока нет статистики.")
        return

    text = "<b>📊 Статистика сообщений</b>\n\n"
    for i, m in enumerate(data[:10], 1):
        text += (
            f"<b>{i}. {m['username']}</b>\n"
            f"Сегодня: {m['today']} | Вчера: {m['yesterday']} | Всего: {m['total']}\n\n"
        )

    await message.answer(text)

# ================== MESSAGE COUNTER ==================

@dp.message(F.text & ~F.text.startswith("/"))
async def count(message: types.Message):
    if message.from_user.is_bot:
        return

    uid = message.from_user.id
    name = message.from_user.full_name
    now = datetime.utcnow()

    cursor.execute("SELECT today, total, last_updated FROM messages WHERE user_id=?", (uid,))
    row = cursor.fetchone()

    if row:
        last = datetime.fromisoformat(row[2])
        if last.date() < now.date():
            cursor.execute("""
                UPDATE messages
                SET yesterday = today,
                    today = 1,
                    total = total + 1,
                    username = ?,
                    last_updated = CURRENT_TIMESTAMP
                WHERE user_id=?
            """, (name, uid))
        else:
            cursor.execute("""
                UPDATE messages
                SET today = today + 1,
                    total = total + 1,
                    username = ?,
                    last_updated = CURRENT_TIMESTAMP
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
    cursor.execute("SELECT SUM(today) FROM messages")
    total = cursor.fetchone()[0] or 0

    cursor.execute("""
        UPDATE messages
        SET yesterday = today,
            today = 0,
            last_updated = CURRENT_TIMESTAMP
    """)
    conn.commit()

    logger.info(f"📅 День закрыт. Сообщений: {total}")

# ================== MAIN ==================

async def main():
    me = await bot.get_me()
    logger.info(f"🤖 Бот запущен: @{me.username}")

    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(daily_reset, "cron", hour=0, minute=0)
    scheduler.start()

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())