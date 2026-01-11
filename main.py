import logging
import sqlite3
import asyncio
import os
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode, ChatType
from aiogram.client.default import DefaultBotProperties
from aiogram import F
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta
import time

API_TOKEN = os.getenv("BOT_TOKEN", "8280794130:AAE7VgMxB0mGR2adpu8FR3SBUS-YjKUydjI")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Правильное создание бота для aiogram 3.7.0+
bot = Bot(
    token=API_TOKEN, 
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

# Создаем соединение с базой данных с таймаутом
conn = sqlite3.connect("stats.db", check_same_thread=False, timeout=10)
cursor = conn.cursor()

# Улучшенная структура базы данных
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

# Таблица для хранения ежедневной статистики
cursor.execute("""
CREATE TABLE IF NOT EXISTS daily_stats (
    date DATE PRIMARY KEY,
    total_messages INTEGER DEFAULT 0,
    active_users INTEGER DEFAULT 0,
    top_user_id INTEGER,
    top_user_count INTEGER
)
""")

# Таблица для настроек чата
cursor.execute("""
CREATE TABLE IF NOT EXISTS chat_settings (
    chat_id INTEGER PRIMARY KEY,
    chat_type TEXT DEFAULT 'private',
    auto_reset_time TEXT DEFAULT '23:59',
    report_time TEXT DEFAULT '00:00',
    timezone TEXT DEFAULT 'UTC',
    is_active BOOLEAN DEFAULT 1
)
""")

conn.commit()

# Кэш для хранения данных о пользователях (оптимизация)
user_cache = {}
cache_timeout = 300  # 5 минут

async def get_chat_members_safe(chat_id, chat_type):
    """Безопасное получение участников чата с учетом типа чата"""
    try:
        if chat_type == ChatType.PRIVATE:
            # Для приватного чата возвращаем только текущего пользователя
            try:
                chat_member = await bot.get_chat_member(chat_id, chat_id)
                if not chat_member.user.is_bot:
                    return [chat_member.user]
            except Exception as e:
                logger.error(f"Error getting private chat member: {e}")
                return []
        
        elif chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
            try:
                # Для групп получаем администраторов
                members = await bot.get_chat_administrators(chat_id)
                # Фильтруем ботов
                chat_members = []
                for member in members:
                    if not member.user.is_bot:
                        chat_members.append(member.user)
                return chat_members
            except Exception as e:
                logger.error(f"Error getting group members: {e}")
                # Если не получается через администраторов, пробуем по-другому
                try:
                    # В некоторых группах можно получить участников напрямую
                    chat = await bot.get_chat(chat_id)
                    # Возвращаем пустой список - будем использовать данные из БД
                    return []
                except Exception as e2:
                    logger.error(f"Error getting chat info: {e2}")
                    return []
        
        elif chat_type == ChatType.CHANNEL:
            # Для каналов статистика не собирается
            return []
        
        return []
        
    except Exception as e:
        logger.error(f"Error in get_chat_members_safe: {e}")
        return []

# Функция для получения отсортированного списка участников с кэшированием
async def get_sorted_members(chat_id, force_update=False):
    try:
        cache_key = f"sorted_members_{chat_id}"
        current_time = time.time()
        
        # Проверяем кэш
        if not force_update and cache_key in user_cache:
            cached_data, timestamp = user_cache[cache_key]
            if current_time - timestamp < cache_timeout:
                return cached_data
        
        # Определяем тип чата
        try:
            chat = await bot.get_chat(chat_id)
            chat_type = chat.type
        except Exception as e:
            logger.error(f"Error getting chat type for {chat_id}: {e}")
            # По умолчанию считаем, что это группа
            chat_type = ChatType.GROUP
        
        # Получаем участников чата безопасным методом
        chat_members = await get_chat_members_safe(chat_id, chat_type)
        
        if not chat_members:
            # Если не удалось получить участников, используем данные из базы для этого чата
            logger.info(f"No members retrieved for chat {chat_id}, using DB data")
            cursor.execute("""
                SELECT user_id, username, today, yesterday, total 
                FROM messages 
                WHERE user_id IN (
                    SELECT DISTINCT user_id FROM messages 
                    WHERE user_id != ?
                )
                ORDER BY today DESC, total DESC
                LIMIT 50
            """, (chat_id,))  # Исключаем ID самого чата если это группа
            
            rows = cursor.fetchall()
            members_with_stats = []
            
            for row in rows:
                user_id, username, today, yesterday, total = row
                members_with_stats.append({
                    'user_id': user_id,
                    'username': username,
                    'mention': username,  # В приватном чате упоминания не работают
                    'today': today,
                    'yesterday': yesterday,
                    'total': total,
                    'is_new': False
                })
            
            # Сохраняем в кэш
            user_cache[cache_key] = (members_with_stats, current_time)
            return members_with_stats
        
        # Получаем статистику для всех участников чата
        placeholders = ','.join(['?'] * len(chat_members))
        cursor.execute(f"""
            SELECT user_id, username, today, yesterday, total 
            FROM messages 
            WHERE user_id IN ({placeholders})
            ORDER BY today DESC, total DESC
        """, [member.id for member in chat_members])
        
        db_stats = cursor.fetchall()
        db_dict = {row[0]: {
            'username': row[1], 
            'today': row[2], 
            'yesterday': row[3],
            'total': row[4]
        } for row in db_stats}
        
        # Создаем список участников с их статистикой
        members_with_stats = []
        for member in chat_members:
            user_id = member.id
            username = member.full_name
            
            # Формируем упоминание если это возможно
            if chat_type != ChatType.PRIVATE and member.username:
                mention = f"<a href='tg://user?id={user_id}'>{username}</a>"
            else:
                mention = username
            
            if user_id in db_dict:
                user_data = db_dict[user_id]
                # Обновляем имя, если оно изменилось
                if user_data['username'] != username:
                    cursor.execute("UPDATE messages SET username=? WHERE user_id=?", 
                                 (username, user_id))
                    conn.commit()
                    user_data['username'] = username
                    
                today_count = user_data['today']
                yesterday_count = user_data['yesterday']
                total_count = user_data['total']
            else:
                # Добавляем пользователя в базу с нулевой статистикой
                cursor.execute("""
                    INSERT OR IGNORE INTO messages 
                    (user_id, username, today, yesterday, total, first_seen)
                    VALUES (?, ?, 0, 0, 0, CURRENT_TIMESTAMP)
                """, (user_id, username))
                conn.commit()
                today_count = 0
                yesterday_count = 0
                total_count = 0
            
            members_with_stats.append({
                'user_id': user_id,
                'username': username,
                'mention': mention,
                'today': today_count,
                'yesterday': yesterday_count,
                'total': total_count,
                'is_new': user_id not in db_dict
            })
        
        # Сортируем по количеству сообщений сегодня, затем по общему количеству
        members_with_stats.sort(key=lambda x: (x['today'], x['total']), reverse=True)
        
        # Сохраняем в кэш
        user_cache[cache_key] = (members_with_stats, current_time)
        
        return members_with_stats
        
    except Exception as e:
        logger.error(f"Error getting sorted members for chat {chat_id}: {e}")
        # В случае ошибки возвращаем данные из базы
        cursor.execute("""
            SELECT user_id, username, today, yesterday, total 
            FROM messages 
            ORDER BY today DESC, total DESC
            LIMIT 20
        """)
        rows = cursor.fetchall()
        
        members_with_stats = []
        for row in rows:
            user_id, username, today, yesterday, total = row
            members_with_stats.append({
                'user_id': user_id,
                'username': username,
                'mention': username,
                'today': today,
                'yesterday': yesterday,
                'total': total,
                'is_new': False
            })
        
        return members_with_stats

# Обработчик команды /status с проверкой типа чата
@dp.message(Command("status"))
async def handle_status(message: types.Message):
    logger.info(f"Command /status received from {message.from_user.id} in chat {message.chat.id}")
    
    try:
        # Получаем ID чата
        chat_id = message.chat.id
        chat_type = message.chat.type
        
        # Проверяем тип чата
        if chat_type == ChatType.CHANNEL:
            await message.reply("⚠️ В каналах статистика не собирается.")
            return
        
        # Получаем отсортированный список участников
        members_with_stats = await get_sorted_members(chat_id)
        
        if not members_with_stats:
            await message.reply("📊 Пока нет статистики сообщений в этом чате.")
            return
        
        # Для приватных чатов показываем упрощенную статистику
        if chat_type == ChatType.PRIVATE:
            if len(members_with_stats) > 0:
                user_stats = members_with_stats[0]  # Первый пользователь в списке
                text = f"<b>📊 Ваша статистика</b>\n\n"
                text += f"👤 <b>{user_stats['username']}</b>\n"
                text += f"📅 <b>Сегодня:</b> {user_stats['today']} сообщений\n"
                text += f"🗓️ <b>Вчера:</b> {user_stats['yesterday']} сообщений\n"
                text += f"📊 <b>Всего:</b> {user_stats['total']} сообщений\n\n"
                text += f"<i>🕐 Обновлено: {datetime.now().strftime('%H:%M:%S')}</i>"
            else:
                text = "📊 Пока нет статистики сообщений."
        else:
            # Для групп и супергрупп показываем полную статистику
            text = "<b>📊 Статистика сообщений</b>\n\n"
            
            # Получаем информацию о чате
            try:
                chat = await bot.get_chat(chat_id)
                chat_title = chat.title
                text += f"<i>Чат: {chat_title}</i>\n"
                text += f"<i>Участников: {len(members_with_stats)}</i>\n\n"
            except:
                text += f"<i>Участников: {len(members_with_stats)}</i>\n\n"
            
            # Добавляем участников в отсортированном порядке (максимум 15)
            for i, member in enumerate(members_with_stats[:15], 1):
                mention = member['mention']
                today_count = member['today']
                yesterday_count = member['yesterday']
                total_count = member['total']
                
                # Определяем эмодзи для топ-3
                emoji = "👑" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "👤"
                
                text += f"<b>{i}. {emoji} {mention}:</b>\n"
                text += f"   📅 Сегодня: {today_count} | 🗓️ Вчера: {yesterday_count}\n"
                text += f"   📊 Всего: {total_count}\n\n"
        
        await message.reply(text)
        
    except Exception as e:
        logger.error(f"Error in /status: {e}")
        await message.reply("⚠️ Произошла ошибка при получении статистики.")

# Обработчик команды /top с проверкой типа чата
@dp.message(Command("top"))
async def handle_top(message: types.Message):
    """Показать топ участников по сообщениям"""
    try:
        chat_id = message.chat.id
        chat_type = message.chat.type
        
        # Проверяем тип чата
        if chat_type == ChatType.CHANNEL:
            await message.reply("⚠️ В каналах статистика не собирается.")
            return
        
        if chat_type == ChatType.PRIVATE:
            await message.reply("ℹ️ В личных чатах используйте команду /mystats для просмотра вашей статистики.")
            return
        
        # Получаем отсортированный список участников
        members_with_stats = await get_sorted_members(chat_id)
        
        if not members_with_stats:
            await message.reply("📊 Пока нет статистики сообщений в этом чате.")
            return
        
        # Создаем текст для топа
        text = "<b>🏆 Топ участников по сообщениям сегодня</b>\n\n"
        
        # Получаем информацию о чате
        try:
            chat = await bot.get_chat(chat_id)
            chat_title = chat.title
            text += f"<i>Чат: {chat_title}</i>\n\n"
        except:
            pass
        
        # Показываем только топ-10
        top_limit = min(10, len(members_with_stats))
        
        for i, member in enumerate(members_with_stats[:top_limit], 1):
            mention = member['mention']
            today_count = member['today']
            total_count = member['total']
            
            # Эмодзи для топа
            emoji = "👑" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            
            text += f"<b>{emoji} {mention}:</b>\n"
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
        logger.error(f"Error in /top: {e}")
        await message.reply("⚠️ Произошла ошибка при получении топа.")

# Обработчик команды /reset_today с проверкой типа чата
@dp.message(Command("reset_today"))
async def handle_reset_today(message: types.Message):
    """Сбросить счетчики на сегодня (только для администраторов)"""
    try:
        chat_type = message.chat.type
        
        # Проверяем тип чата
        if chat_type == ChatType.CHANNEL:
            await message.reply("⚠️ В каналах эта команда недоступна.")
            return
        
        if chat_type == ChatType.PRIVATE:
            await message.reply("ℹ️ В личных чатах используйте команду /mystats.")
            return
        
        # Для групп и супергрупп проверяем права администратора
        chat_id = message.chat.id
        
        try:
            chat_admins = await bot.get_chat_administrators(chat_id)
            admin_ids = [admin.user.id for admin in chat_admins]
            
            if message.from_user.id not in admin_ids:
                await message.reply("⚠️ Эта команда доступна только администраторам.")
                return
        except Exception as e:
            logger.error(f"Error checking admin rights: {e}")
            # Если не удалось проверить права, разрешаем только создателю бота
            me = await bot.get_me()
            if message.from_user.id != me.id:
                await message.reply("⚠️ Не удалось проверить права администратора.")
                return
            
        # Сохраняем данные за сегодня перед сбросом
        cursor.execute("SELECT SUM(today) FROM messages")
        total_today = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT COUNT(*) FROM messages WHERE today > 0")
        active_today = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT user_id, today FROM messages WHERE today > 0 ORDER BY today DESC LIMIT 1")
        top_user = cursor.fetchone()
        
        # Сохраняем в историю
        today_date = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("""
            INSERT OR REPLACE INTO daily_stats 
            (date, total_messages, active_users, top_user_id, top_user_count)
            VALUES (?, ?, ?, ?, ?)
        """, (
            today_date,
            total_today,
            active_today,
            top_user[0] if top_user else None,
            top_user[1] if top_user else 0
        ))
        
        # Сбрасываем счетчики на сегодня для всех участников
        cursor.execute("UPDATE messages SET yesterday = today, today = 0")
        conn.commit()
        
        # Инвалидируем кэш
        cache_key = f"sorted_members_{chat_id}"
        if cache_key in user_cache:
            del user_cache[cache_key]
        
        await message.reply(f"✅ Счетчики сообщений сброшены.\n📊 Сегодня было: {total_today} сообщений от {active_today} пользователей")
        
    except Exception as e:
        logger.error(f"Error in /reset_today: {e}")
        await message.reply("⚠️ Произошла ошибка при сбросе счетчиков.")

# Обновленный обработчик всех сообщений
@dp.message(F.text & ~F.text.startswith('/'))
async def count_messages(message: types.Message):
    if not message.from_user:
        return

    user_id = message.from_user.id
    username = message.from_user.full_name
    chat_id = message.chat.id
    chat_type = message.chat.type

    # Проверяем тип чата
    if chat_type == ChatType.CHANNEL:
        return  # В каналах не считаем сообщения

    # Проверяем, не бот ли это
    if message.from_user.is_bot:
        return

    # Получаем текущее время
    current_time = datetime.now()
    
    cursor.execute("SELECT * FROM messages WHERE user_id=?", (user_id,))
    row = cursor.fetchone()

    if row:
        # Проверяем, нужно ли сбросить счетчик "сегодня" (если прошли сутки)
        last_updated = datetime.fromisoformat(row[5]) if isinstance(row[5], str) else row[5]
        if current_time.date() > last_updated.date():
            # Переносим сегодняшние сообщения во вчерашние
            cursor.execute("""
                UPDATE messages
                SET yesterday = today,
                    today = 1,
                    total = total + 1,
                    username = ?,
                    last_updated = CURRENT_TIMESTAMP
                WHERE user_id=?
            """, (username, user_id))
        else:
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
            INSERT INTO messages (user_id, username, today, total, first_seen, last_updated)
            VALUES (?, ?, 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """, (user_id, username))

    conn.commit()
    
    # Инвалидируем кэш для этого чата
    cache_key = f"sorted_members_{chat_id}"
    if cache_key in user_cache:
        del user_cache[cache_key]

# Обновленная функция daily_report
async def daily_report():
    """Ежедневный отчет с учетом типов чатов"""
    try:
        # Получаем список активных чатов из базы данных
        cursor.execute("""
            SELECT chat_id, chat_type FROM chat_settings WHERE is_active = 1
        """)
        chat_settings = cursor.fetchall()
        
        # Если нет настроек, используем чаты, где бот активен
        if not chat_settings:
            # В этом упрощенном примере отправляем отчет в логи
            logger.info("Ежедневный отчет: нет настроек чатов")
            return
        
        for chat_id, chat_type_str in chat_settings:
            try:
                # Пропускаем каналы и приватные чаты для массовых отчетов
                if chat_type_str == 'channel':
                    continue
                
                # Получаем отсортированный список участников
                members_with_stats = await get_sorted_members(chat_id, force_update=True)
                
                if not members_with_stats:
                    continue
                
                # Только для групп отправляем отчет
                if chat_type_str in ['group', 'supergroup']:
                    text = "📊 <b>Ежедневный отчет</b>\n\n"
                    text += f"<i>Дата: {datetime.now().strftime('%d.%m.%Y')}</i>\n\n"
                    
                    # Формируем отчет по топ-5 участникам
                    for i, member in enumerate(members_with_stats[:5], 1):
                        mention = member['mention']
                        today_count = member['today']
                        
                        emoji = "👑" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                        text += f"{emoji} {mention}: {today_count} сообщ.\n"
                    
                    # Общая статистика
                    total_today = sum(member['today'] for member in members_with_stats)
                    active_today = sum(1 for member in members_with_stats if member['today'] > 0)
                    
                    if len(members_with_stats) > 5:
                        text += f"\n...и еще {len(members_with_stats) - 5} участников\n"
                    
                    text += f"\n<b>📈 Итоги дня:</b>\n"
                    text += f"📨 Сообщений: {total_today}\n"
                    text += f"👥 Активных: {active_today}\n\n"
                    text += f"<i>Статистика обнулена до завтра</i>"
                    
                    await bot.send_message(chat_id, text)
                
                # Сохраняем статистику дня и сбрасываем счетчики
                today_date = datetime.now().strftime('%Y-%m-%d')
                total_today = sum(member['today'] for member in members_with_stats)
                active_today = sum(1 for member in members_with_stats if member['today'] > 0)
                
                cursor.execute("""
                    INSERT OR REPLACE INTO daily_stats 
                    (date, total_messages, active_users, top_user_id, top_user_count)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    today_date,
                    total_today,
                    active_today,
                    members_with_stats[0]['user_id'] if members_with_stats else None,
                    members_with_stats[0]['today'] if members_with_stats else 0
                ))
                
                # Сбрасываем счетчики
                cursor.execute("""
                    UPDATE messages 
                    SET yesterday = today,
                        today = 0,
                        last_updated = CURRENT_TIMESTAMP
                """)
                
                conn.commit()
                
                # Инвалидируем кэш
                cache_key = f"sorted_members_{chat_id}"
                if cache_key in user_cache:
                    del user_cache[cache_key]
                    
            except Exception as e:
                logger.error(f"Error sending daily report to chat {chat_id}: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error in daily_report: {e}")

# Обновленная функция main
async def main():
    # Регистрируем команды для бота
    await bot.set_my_commands([
        types.BotCommand(command="status", description="📊 Статистика чата"),
        types.BotCommand(command="top", description="🏆 Топ-10 участников"),
        types.BotCommand(command="mystats", description="📈 Ваша статистика"),
        types.BotCommand(command="yesterday", description="🗓️ Топ за вчера"),
        types.BotCommand(command="weekly", description="📅 Статистика за неделю"),
        types.BotCommand(command="reset_today", description="🔄 Сбросить счетчики"),
        types.BotCommand(command="help", description="❓ Помощь по командам")
    ])
    
    # Проверка токена перед запуском
    try:
        me = await bot.get_me()
        logger.info(f"Бот успешно авторизован: @{me.username} (ID: {me.id})")
        logger.info(f"Типы чатов, которые поддерживает бот: приватные, группы, супергруппы")
    except Exception as e:
        logger.error(f"Ошибка авторизации: {e}")
        logger.error("Проверьте правильность токена API_TOKEN или переменной окружения BOT_TOKEN")
        return
    
    # Настройка планировщика
    scheduler = AsyncIOScheduler(timezone="UTC")
    
    # Ежедневный отчет в 23:59
    scheduler.add_job(daily_report, "cron", hour=23, minute=59)
    
    # Автосохранение статистики каждый час
    scheduler.add_job(auto_save_daily_stats, "cron", hour="*")
    
    # Автоматический сброс счетчиков в полночь
    scheduler.add_job(daily_report, "cron", hour=0, minute=1)
    
    scheduler.start()
    
    # Добавляем middleware для обработки ошибок
    @dp.errors()
    async def errors_handler(update: types.Update, exception: Exception):
        logger.error(f"Update {update} caused error: {exception}")
        return True
    
    try:
        await dp.start_polling(bot, skip_updates=True)
    except KeyboardInterrupt:
        logger.info("Остановка бота...")
    except Exception as e:
        logger.error(f"Fatal error in polling: {e}")
    finally:
        await bot.session.close()
        conn.close()

if __name__ == "__main__":
    asyncio.run(main())