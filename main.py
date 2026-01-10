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

cursor.execute("""
CREATE TABLE IF NOT EXISTS messages (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    today INTEGER DEFAULT 0,
    total INTEGER DEFAULT 0
)
""")
conn.commit()

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
                username = ?
            WHERE user_id=?
        """, (username, user_id))
    else:
        cursor.execute("""
            INSERT INTO messages (user_id, username, today, total)
            VALUES (?, ?, 1, 1)
        """, (user_id, username))

    conn.commit()

# Обработчик команды /status
@dp.message(Command("status"))
async def handle_status(message: types.Message):
    logging.info(f"Command /status received from {message.from_user.id}")
    
    try:
        # Получаем ID чата
        chat_id = message.chat.id
        
        # Получаем всех участников чата
        chat_members = []
        try:
            # Используем правильный метод для aiogram 3.x
            members = await bot.get_chat_administrators(chat_id)
            
            # Получаем всех участников (администраторов)
            for member in members:
                if not member.user.is_bot:
                    chat_members.append(member.user)
            
            # В больших группах лучше использовать отдельный метод для обычных участников
            # Но get_chat_member_count только возвращает количество, не список
            
        except Exception as e:
            logging.error(f"Error getting chat members: {e}")
            
            # Если не удалось получить список участников через администраторов,
            # попробуем получить хотя бы тех, кто есть в базе данных
            cursor.execute("SELECT user_id, username, today, total FROM messages ORDER BY today DESC, username ASC")
            rows = cursor.fetchall()
            
            if not rows:
                await message.reply("📊 Пока нет статистики сообщений.")
                return
            
            text = "<b>📊 Статистика сообщений</b>\n\n"
            text += "<i>⚠️ Нет доступа к полному списку участников</i>\n\n"
            
            for i, (user_id, username, today, total) in enumerate(rows, 1):
                text += f"<b>{i}. 👤 {username}:</b>\n"
                text += f"   📅 Сегодня: {today} сообщ.\n"
                text += f"   📊 Всего: {total} сообщ.\n\n"
            
            await message.reply(text)
            return
        
        # Получаем статистику из базы данных для участников чата
        if chat_members:
            cursor.execute("SELECT user_id, username, today, total FROM messages WHERE user_id IN ({}) ORDER BY today DESC, username ASC".format(
                ','.join(['?'] * len(chat_members))
            ), [member.id for member in chat_members])
        else:
            cursor.execute("SELECT user_id, username, today, total FROM messages ORDER BY today DESC, username ASC")
        
        db_stats = cursor.fetchall()
        db_dict = {row[0]: {'username': row[1], 'today': row[2], 'total': row[3]} for row in db_stats}
        
        # Создаем текст для отображения
        text = "<b>📊 Статистика сообщений</b>\n\n"
        text += f"<i>Участников в чате: {len(chat_members)}</i>\n\n"
        
        # Добавляем участников из чата
        for i, member in enumerate(chat_members, 1):
            user_id = member.id
            username = member.full_name
            
            # Получаем данные из базы или используем нули
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
                    INSERT OR IGNORE INTO messages (user_id, username, today, total)
                    VALUES (?, ?, 0, 0)
                """, (user_id, username))
                conn.commit()
                today_count = 0
                total_count = 0
            
            # Форматируем строку статистики
            text += f"<b>{i}. 👤 {username}:</b>\n"
            text += f"   📅 Сегодня: {today_count} сообщ.\n"
            text += f"   📊 Всего: {total_count} сообщ.\n\n"
        
        # Если участников чата не удалось получить, показываем только статистику из базы
        if not chat_members and db_stats:
            text = "<b>📊 Статистика сообщений</b>\n\n"
            for i, (user_id, username, today, total) in enumerate(db_stats, 1):
                text += f"<b>{i}. 👤 {username}:</b>\n"
                text += f"   📅 Сегодня: {today} сообщ.\n"
                text += f"   📊 Всего: {total} сообщ.\n\n"
        
        await message.reply(text)
        
    except Exception as e:
        logging.error(f"Error in /status: {e}")
        await message.reply("⚠️ Произошла ошибка при получении статистики.")

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
        # Получаем администраторов чата (это максимум, что можно получить без специальных прав)
        members = await bot.get_chat_administrators(CHAT_ID)
        
        # Фильтруем ботов
        chat_members = []
        for member in members:
            if not member.user.is_bot:
                chat_members.append(member.user)
        
        if not chat_members:
            return
        
        # Получаем статистику из базы
        cursor.execute("SELECT user_id, username, today, total FROM messages WHERE user_id IN ({}) ORDER BY today DESC".format(
            ','.join(['?'] * len(chat_members))
        ), [member.id for member in chat_members])
        
        rows = cursor.fetchall()
        db_dict = {row[0]: {'username': row[1], 'today': row[2], 'total': row[3]} for row in rows}
        
        text = "📊 Итоги дня\n\n"
        
        # Формируем отчет по участникам чата
        for i, member in enumerate(chat_members, 1):
            user_id = member.id
            username = member.full_name
            
            if user_id in db_dict:
                user_data = db_dict[user_id]
                today_count = user_data['today']
                total_count = user_data['total']
            else:
                today_count = 0
                total_count = 0
            
            text += f"{i}. {username}: сегодня {today_count}, всего {total_count}\n"
        
        await bot.send_message(CHAT_ID, text)
        
        # Обнуляем счетчики для участников чата
        for member in chat_members:
            cursor.execute("""
                INSERT OR REPLACE INTO messages (user_id, username, today, total)
                VALUES (?, ?, 0, 
                    COALESCE((SELECT total FROM messages WHERE user_id=?), 0))
            """, (member.id, member.full_name, member.id))
        
        conn.commit()
        
    except Exception as e:
        logging.error(f"Error in daily_report: {e}")

async def main():
    # Регистрируем команды для бота
    await bot.set_my_commands([
        types.BotCommand(command="status", description="Показать статистику сообщений")
    ])
    
    # Проверка токена перед запуском
    try:
        me = await bot.get_me()
        logging.info(f"Бот успешно авторизован: @{me.username} (ID: {me.id})")
        logging.info(f"Зарегистрирована команда: /status")
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
