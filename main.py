import logging
import sqlite3
import asyncio
import os
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram import F
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime

API_TOKEN = os.getenv("BOT_TOKEN", "8280794130:AAE7VgMxB0mGR2adpu8FR3SBUS-YjKUydjI")

logging.basicConfig(level=logging.INFO)

# Правильное создание бота для aiogram 3.7.0+
bot = Bot(
    token=API_TOKEN, 
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

# Создаем соединение с базой данных
conn = sqlite3.connect("stats.db", check_same_thread=False)
cursor = conn.cursor()

# Добавляем поле для времени последнего обновления
cursor.execute("""
CREATE TABLE IF NOT EXISTS messages (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    today INTEGER DEFAULT 0,
    total INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()

# Функция для получения отсортированного списка участников
async def get_sorted_members(chat_id):
    try:
        # Получаем всех участников чата через администраторов
        members = await bot.get_chat_administrators(chat_id)
        
        # Фильтруем ботов и собираем участников
        chat_members = []
        for member in members:
            if not member.user.is_bot:
                chat_members.append(member.user)
        
        if not chat_members:
            return []
        
        # Получаем статистику для всех участников чата
        cursor.execute("""
            SELECT user_id, username, today, total 
            FROM messages 
            WHERE user_id IN ({})
            ORDER BY today DESC, total DESC
        """.format(','.join(['?'] * len(chat_members))), 
        [member.id for member in chat_members])
        
        db_stats = cursor.fetchall()
        db_dict = {row[0]: {'username': row[1], 'today': row[2], 'total': row[3]} for row in db_stats}
        
        # Создаем список участников с их статистикой
        members_with_stats = []
        for member in chat_members:
            user_id = member.id
            username = member.full_name
            
            if user_id in db_dict:
                user_data = db_dict[user_id]
                # Обновляем имя, если оно изменилось
                if user_data['username'] != username:
                    cursor.execute("UPDATE messages SET username=? WHERE user_id=?", 
                                 (username, user_id))
                    conn.commit()
                    user_data['username'] = username
                    
                today_count = user_data['today']
                total_count = user_data['total']
            else:
                # Добавляем пользователя в базу с нулевой статистикой
                cursor.execute("""
                    INSERT OR IGNORE INTO messages (user_id, username, today, total, last_updated)
                    VALUES (?, ?, 0, 0, CURRENT_TIMESTAMP)
                """, (user_id, username))
                conn.commit()
                today_count = 0
                total_count = 0
            
            members_with_stats.append({
                'user_id': user_id,
                'username': username,
                'today': today_count,
                'total': total_count
            })
        
        # Сортируем по количеству сообщений сегодня, затем по общему количеству
        members_with_stats.sort(key=lambda x: (x['today'], x['total']), reverse=True)
        
        return members_with_stats
        
    except Exception as e:
        logging.error(f"Error getting sorted members: {e}")
        return []

# Обработчик всех сообщений кроме команд
@dp.message(F.text & ~F.text.startswith('/'))
async def count_messages(message: types.Message):
    if not message.from_user:
        return

    user_id = message.from_user.id
    username = message.from_user.full_name

    # Проверяем, не бот ли это
    if message.from_user.is_bot:
        return

    cursor.execute("SELECT * FROM messages WHERE user_id=?", (user_id,))
    row = cursor.fetchone()

    if row:
        cursor.execute("""
            UPDATE messages
            SET today = today + 1,
                total = total + 1,
                username = ?,
                last_updated = CURRENT_TIMESTAMP
            WHERE user_id=?
        """, (username, user_id))
    else:
        cursor.execute("""
            INSERT INTO messages (user_id, username, today, total, last_updated)
            VALUES (?, ?, 1, 1, CURRENT_TIMESTAMP)
        """, (user_id, username))

    conn.commit()

# Обработчик команды /status
@dp.message(Command("status"))
async def handle_status(message: types.Message):
    logging.info(f"Command /status received from {message.from_user.id}")
    
    try:
        # Получаем ID чата
        chat_id = message.chat.id
        
        # Получаем отсортированный список участников
        members_with_stats = await get_sorted_members(chat_id)
        
        if not members_with_stats:
            # Если не удалось получить участников, показываем статистику из базы
            cursor.execute("""
                SELECT user_id, username, today, total 
                FROM messages 
                ORDER BY today DESC, total DESC
            """)
            rows = cursor.fetchall()
            
            if not rows:
                await message.reply("📊 Пока нет статистики сообщений.")
                return
            
            text = "<b>📊 Статистика сообщений (только из базы данных)</b>\n\n"
            text += f"<i>⚠️ Нет доступа к полному списку участников чата</i>\n\n"
            
            for i, (user_id, username, today, total) in enumerate(rows, 1):
                text += f"<b>{i}. 👤 {username}:</b>\n"
                text += f"   📅 Сегодня: {today} сообщ.\n"
                text += f"   📊 Всего: {total} сообщ.\n\n"
            
            await message.reply(text)
            return
        
        # Создаем текст для отображения
        text = "<b>📊 Статистика сообщений</b>\n\n"
        text += f"<i>Участников в чате: {len(members_with_stats)}</i>\n\n"
        
        # Добавляем участников в отсортированном порядке
        for i, member in enumerate(members_with_stats, 1):
            username = member['username']
            today_count = member['today']
            total_count = member['total']
            
            # Определяем эмодзи для топ-3
            emoji = "👑" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "👤"
            
            text += f"<b>{i}. {emoji} {username}:</b>\n"
            text += f"   📅 Сегодня: {today_count} сообщ.\n"
            text += f"   📊 Всего: {total_count} сообщ.\n\n"
        
        # Добавляем текущее время обновления
        text += f"<i>🕐 Обновлено: {datetime.now().strftime('%H:%M:%S')}</i>"
        
        await message.reply(text)
        
    except Exception as e:
        logging.error(f"Error in /status: {e}")
        await message.reply("⚠️ Произошла ошибка при получении статистики.")

# Обработчик команды /top
@dp.message(Command("top"))
async def handle_top(message: types.Message):
    """Показать топ участников по сообщениям"""
    try:
        chat_id = message.chat.id
        
        # Получаем отсортированный список участников
        members_with_stats = await get_sorted_members(chat_id)
        
        if not members_with_stats:
            await message.reply("📊 Пока нет статистики сообщений.")
            return
        
        # Создаем текст для топа
        text = "<b>🏆 Топ участников по сообщениям сегодня</b>\n\n"
        
        # Показываем только топ-10
        top_limit = min(10, len(members_with_stats))
        
        for i, member in enumerate(members_with_stats[:top_limit], 1):
            username = member['username']
            today_count = member['today']
            total_count = member['total']
            
            # Эмодзи для топа
            emoji = "👑" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            
            text += f"<b>{emoji} {username}:</b>\n"
            text += f"   📅 Сегодня: {today_count} сообщ. | 📊 Всего: {total_count}\n\n"
        
        # Добавляем информацию о общем количестве
        total_today = sum(member['today'] for member in members_with_stats)
        total_all = sum(member['total'] for member in members_with_stats)
        
        text += f"<b>📈 Итого по чату:</b>\n"
        text += f"   📅 Сегодня: {total_today} сообщ.\n"
        text += f"   📊 Всего: {total_all} сообщ.\n\n"
        text += f"<i>🕐 Обновлено: {datetime.now().strftime('%H:%M:%S')}</i>"
        
        await message.reply(text)
        
    except Exception as e:
        logging.error(f"Error in /top: {e}")
        await message.reply("⚠️ Произошла ошибка при получении топа.")

# Обработчик команды /reset_today
@dp.message(Command("reset_today"))
async def handle_reset_today(message: types.Message):
    """Сбросить счетчики на сегодня (только для администраторов)"""
    try:
        # Проверяем, является ли пользователь администратором
        chat_admins = await bot.get_chat_administrators(message.chat.id)
        admin_ids = [admin.user.id for admin in chat_admins]
        
        if message.from_user.id not in admin_ids:
            await message.reply("⚠️ Эта команда доступна только администраторам.")
            return
            
        # Сбрасываем счетчики на сегодня для всех участников
        cursor.execute("UPDATE messages SET today = 0")
        conn.commit()
        
        await message.reply("✅ Счетчики сообщений на сегодня сброшены для всех участников.")
        
    except Exception as e:
        logging.error(f"Error in /reset_today: {e}")
        await message.reply("⚠️ Произошла ошибка при сбросе счетчиков.")

# Обработчик всех других сообщений (не текст, не команды)
@dp.message()
async def handle_other_messages(message: types.Message):
    # Игнорируем все остальные типы сообщений (фото, стикеры и т.д.)
    pass

async def daily_report():
    # Получаем все чаты, где есть бот
    # В этом примере используем фиксированный CHAT_ID
    CHAT_ID = -1003573882529
    
    try:
        # Получаем отсортированный список участников
        members_with_stats = await get_sorted_members(CHAT_ID)
        
        if not members_with_stats:
            return
        
        text = "📊 Итоги дня\n\n"
        
        # Формируем отчет по участникам в отсортированном порядке
        for i, member in enumerate(members_with_stats, 1):
            username = member['username']
            today_count = member['today']
            total_count = member['total']
            
            # Эмодзи для топа
            emoji = "👑" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            
            text += f"{emoji} {username}: сегодня {today_count}, всего {total_count}\n"
            
            # Ограничиваем вывод 15 участниками
            if i >= 15:
                text += f"\n...и еще {len(members_with_stats) - 15} участников"
                break
        
        # Обнуляем счетчики для участников чата
        for member in members_with_stats:
            cursor.execute("""
                UPDATE messages 
                SET today = 0,
                    last_updated = CURRENT_TIMESTAMP
                WHERE user_id=?
            """, (member['user_id'],))
        
        conn.commit()
        
        await bot.send_message(CHAT_ID, text)
        
    except Exception as e:
        logging.error(f"Error in daily_report: {e}")

async def main():
    # Регистрируем команды для бота
    await bot.set_my_commands([
        types.BotCommand(command="status", description="Показать статистику сообщений"),
        types.BotCommand(command="top", description="Топ участников по сообщениям"),
        types.BotCommand(command="reset_today", description="Сбросить счетчики на сегодня (админы)")
    ])
    
    # Проверка токена перед запуском
    try:
        me = await bot.get_me()
        logging.info(f"Бот успешно авторизован: @{me.username} (ID: {me.id})")
        logging.info(f"Зарегистрированы команды: /status, /top, /reset_today")
    except Exception as e:
        logging.error(f"Ошибка авторизации: {e}")
        logging.error("Проверьте правильность токена API_TOKEN или переменной окружения BOT_TOKEN")
        return
    
    scheduler = AsyncIOScheduler()
    scheduler.add_job(daily_report, "cron", hour=22, minute=32)
    scheduler.start()
    
    try:
        await dp.start_polling(bot, skip_updates=True)
    except KeyboardInterrupt:
        logging.info("Остановка бота...")
    finally:
        await bot.session.close()
        conn.close()

if __name__ == "__main__":
    asyncio.run(main())
