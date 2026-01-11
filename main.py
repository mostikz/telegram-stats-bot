import logging
import sqlite3
import asyncio
import os
import signal
import sys
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode, ChatType
from aiogram.client.default import DefaultBotProperties
from aiogram import F
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta
import time
from aiohttp import web

API_TOKEN = os.getenv("BOT_TOKEN", "8280794130:AAE7VgMxB0mGR2adpu8FR3SBUS-YjKUydjI")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Глобальные переменные
bot_instance = None
scheduler_instance = None
is_shutting_down = False
http_server = None

# Получаем порт из переменной окружения (для Render.com)
PORT = int(os.getenv("PORT", 8080))

# Простой HTTP сервер для health checks
async def health_check(request):
    """Проверка здоровья сервиса"""
    return web.Response(text="Bot is running", status=200)

async def start_http_server():
    """Запуск HTTP сервера для health checks"""
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    
    logger.info(f"HTTP сервер запущен на порту {PORT}")
    return runner

async def shutdown():
    """Корректное завершение работы бота"""
    global is_shutting_down, http_server
    
    if is_shutting_down:
        return
        
    is_shutting_down = True
    logger.info("Начинаем корректное завершение работы...")
    
    try:
        # Останавливаем HTTP сервер
        if http_server:
            await http_server.cleanup()
            logger.info("HTTP сервер остановлен")
    except Exception as e:
        logger.error(f"Ошибка при остановке HTTP сервера: {e}")
    
    try:
        # Останавливаем планировщик
        if scheduler_instance and scheduler_instance.running:
            scheduler_instance.shutdown(wait=False)
            logger.info("Планировщик остановлен")
    except Exception as e:
        logger.error(f"Ошибка при остановке планировщика: {e}")
    
    try:
        # Закрываем сессию бота
        if bot_instance:
            await bot_instance.session.close()
            logger.info("Сессия бота закрыта")
    except Exception as e:
        logger.error(f"Ошибка при закрытии сессии бота: {e}")
    
    try:
        # Закрываем соединение с базой данных
        if 'conn' in globals():
            conn.close()
            logger.info("Соединение с БД закрыто")
    except Exception as e:
        logger.error(f"Ошибка при закрытии БД: {e}")
    
    logger.info("Завершение работы завершено")
    await asyncio.sleep(1)

def signal_handler(signum, frame):
    """Обработчик сигналов для graceful shutdown"""
    logger.info(f"Получен сигнал {signum}, инициируем shutdown...")
    asyncio.create_task(shutdown())

# Регистрируем обработчики сигналов
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

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
            cursor.execute("""
                SELECT user_id, username, today, yesterday, total 
                FROM messages 
                WHERE user_id IN (
                    SELECT DISTINCT user_id FROM messages 
                    WHERE user_id != ?
                )
                ORDER BY today DESC, total DESC
                LIMIT 50
            """, (chat_id,))
            
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
    if is_shutting_down:
        return
        
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
                user_stats = members_with_stats[0]
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
        try:
            await message.reply("⚠️ Произошла ошибка при получении статистики.")
        except:
            pass

# Обработчик команды /top с проверкой типа чата
@dp.message(Command("top"))
async def handle_top(message: types.Message):
    """Показать топ участников по сообщениям"""
    if is_shutting_down:
        return
        
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
        try:
            await message.reply("⚠️ Произошла ошибка при получении топа.")
        except:
            pass

# Обработчик команды /mystats
@dp.message(Command("mystats"))
async def handle_mystats(message: types.Message):
    """Показать статистику текущего пользователя"""
    if is_shutting_down:
        return
        
    try:
        user_id = message.from_user.id
        username = message.from_user.full_name
        
        # Получаем статистику из базы
        cursor.execute("""
            SELECT today, yesterday, total, first_seen 
            FROM messages WHERE user_id=?
        """, (user_id,))
        
        row = cursor.fetchone()
        
        if row:
            today, yesterday, total, first_seen = row
            # Обновляем имя пользователя если нужно
            cursor.execute("UPDATE messages SET username=? WHERE user_id=?", 
                         (username, user_id))
            conn.commit()
            
            # Рассчитываем среднее в день
            first_seen_date = datetime.fromisoformat(first_seen) if isinstance(first_seen, str) else first_seen
            days_active = (datetime.now() - first_seen_date).days
            avg_daily = round(total / max(days_active, 1), 1)
            
            text = f"<b>📊 Ваша статистика, {username}</b>\n\n"
            text += f"📅 <b>Сегодня:</b> {today} сообщений\n"
            text += f"🗓️ <b>Вчера:</b> {yesterday} сообщений\n"
            text += f"📊 <b>Всего:</b> {total} сообщений\n"
            text += f"📈 <b>Среднее в день:</b> {avg_daily} сообщений\n"
            text += f"📅 <b>С нами с:</b> {first_seen_date.strftime('%d.%m.%Y')} ({days_active} дней)\n\n"
            text += f"<i>Данные обновлены: {datetime.now().strftime('%H:%M:%S')}</i>"
        else:
            # Создаем новую запись
            cursor.execute("""
                INSERT INTO messages (user_id, username, today, total, first_seen)
                VALUES (?, ?, 0, 0, CURRENT_TIMESTAMP)
            """, (user_id, username))
            conn.commit()
            
            text = f"<b>📊 Ваша статистика, {username}</b>\n\n"
            text += "📊 Пока нет сообщений. Начните общаться!\n\n"
            text += f"<i>Вы с нами с: {datetime.now().strftime('%d.%m.%Y')}</i>"
        
        await message.reply(text)
        
    except Exception as e:
        logger.error(f"Error in /mystats: {e}")
        try:
            await message.reply("⚠️ Произошла ошибка при получении статистики.")
        except:
            pass

# Обработчик команды /help
@dp.message(Command("help", "start"))
async def handle_help(message: types.Message):
    """Показать справку по командам"""
    if is_shutting_down:
        return
        
    text = """
<b>🤖 Бот статистики сообщений</b>

<b>Основные команды:</b>
/status - Полная статистика по чату
/top - Топ-10 активных участников сегодня
/mystats - Ваша личная статистика
/help - Эта справка

<b>Как это работает:</b>
• Бот подсчитывает все текстовые сообщения
• Статистика обновляется в реальном времени
• Ежедневный автоматический сброс в полночь

<i>Для работы в группах боту нужны права администратора</i>
    """
    await message.reply(text)

# Обработчик команды /reset_today с проверкой типа чата
@dp.message(Command("reset_today"))
async def handle_reset_today(message: types.Message):
    """Сбросить счетчики на сегодня (только для администраторов)"""
    if is_shutting_down:
        return
        
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
            await message.reply("⚠️ Не удалось проверить права администратора.")
            return
            
        # Сбрасываем счетчики на сегодня для всех участников
        cursor.execute("UPDATE messages SET yesterday = today, today = 0")
        conn.commit()
        
        # Инвалидируем кэш
        cache_key = f"sorted_members_{chat_id}"
        if cache_key in user_cache:
            del user_cache[cache_key]
        
        await message.reply("✅ Счетчики сообщений на сегодня сброшены.")
        
    except Exception as e:
        logger.error(f"Error in /reset_today: {e}")
        try:
            await message.reply("⚠️ Произошла ошибка при сбросе счетчиков.")
        except:
            pass

# Обновленный обработчик всех сообщений
@dp.message(F.text & ~F.text.startswith('/'))
async def count_messages(message: types.Message):
    if is_shutting_down:
        return
        
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

# Обработчик всех других сообщений
@dp.message()
async def handle_other_messages(message: types.Message):
    if is_shutting_down:
        return
    # Игнорируем все остальные типы сообщений (фото, стикеры и т.д.)
    pass

async def daily_report():
    """Ежедневный отчет"""
    if is_shutting_down:
        return
        
    try:
        logger.info("Выполняется ежедневный отчет...")
        
        # Сбрасываем счетчики
        cursor.execute("""
            UPDATE messages 
            SET yesterday = today,
                today = 0,
                last_updated = CURRENT_TIMESTAMP
        """)
        
        conn.commit()
        
        # Очищаем кэш
        user_cache.clear()
        
        logger.info("Ежедневный сброс статистики выполнен.")
        
    except Exception as e:
        logger.error(f"Error in daily_report: {e}")

async def main():
    global bot_instance, scheduler_instance, http_server
    
    # Сохраняем глобальные ссылки
    bot_instance = bot
    
    logger.info("Запуск бота статистики сообщений...")
    
    # Запускаем HTTP сервер для health checks
    try:
        http_server = await start_http_server()
        logger.info(f"HTTP сервер запущен на порту {PORT}")
    except Exception as e:
        logger.error(f"Ошибка запуска HTTP сервера: {e}")
        # Продолжаем работу даже если HTTP сервер не запустился
    
    # Регистрируем команды для бота
    try:
        await bot.set_my_commands([
            types.BotCommand(command="status", description="📊 Статистика чата"),
            types.BotCommand(command="top", description="🏆 Топ-10 участников"),
            types.BotCommand(command="mystats", description="📈 Ваша статистика"),
            types.BotCommand(command="help", description="❓ Помощь по командам"),
            types.BotCommand(command="reset_today", description="🔄 Сбросить счетчики (админы)")
        ])
    except Exception as e:
        logger.error(f"Error setting bot commands: {e}")
    
    # Проверка токена перед запуском
    try:
        me = await bot.get_me()
        logger.info(f"Бот успешно авторизован: @{me.username} (ID: {me.id})")
    except Exception as e:
        logger.error(f"Ошибка авторизации: {e}")
        logger.error("Проверьте правильность токена API_TOKEN")
        return
    
    # Настройка планировщика
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler_instance = scheduler
    
    # Ежедневный сброс в полночь
    scheduler.add_job(daily_report, "cron", hour=0, minute=0, misfire_grace_time=60)
    
    try:
        scheduler.start()
        logger.info("Планировщик запущен")
    except Exception as e:
        logger.error(f"Ошибка запуска планировщика: {e}")
    
    # Добавляем middleware для обработки ошибок
    @dp.errors()
    async def errors_handler(update: types.Update, exception: Exception):
        if not is_shutting_down:
            logger.error(f"Update {update} caused error: {exception}")
        return True
    
    try:
        logger.info("Бот запущен и готов к работе...")
        await dp.start_polling(bot, skip_updates=True, handle_signals=False)
    except asyncio.CancelledError:
        logger.info("Получен сигнал отмены")
    except KeyboardInterrupt:
        logger.info("Получен KeyboardInterrupt")
    except Exception as e:
        logger.error(f"Fatal error in polling: {e}")
    finally:
        logger.info("Запускаем процедуру завершения...")
        await shutdown()

if __name__ == "__main__":
    # Устанавливаем обработчик исключений
    def handle_exception(loop, context):
        msg = context.get("exception", context["message"])
        logger.error(f"Caught exception in event loop: {msg}")
        
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.set_exception_handler(handle_exception)
    
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, shutting down...")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
    finally:
        # Закрываем event loop
        loop.close()
        logger.info("Бот завершил работу")
        sys.exit(0)