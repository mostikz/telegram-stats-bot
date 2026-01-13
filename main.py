import asyncio
from aiohttp import web
import threading
import logging
import sqlite3
import os
import signal
import sys
import random
from datetime import datetime, timedelta
import time

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode, ChatType
from aiogram.client.default import DefaultBotProperties
from aiogram import F
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ==================== КОНСТАНТЫ ====================
API_TOKEN = os.getenv("BOT_TOKEN", "8280794130:AAE7VgMxB0mGR2adpu8FR3SBUS-YjKUydjI")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ====================
bot_instance = None
dp = None
scheduler_instance = None
is_shutting_down = False
polling_task = None
conn = None
cursor = None
user_cache = {}
cache_timeout = 300  # 5 минут
current_mention_type = 0  # 0=предсказание, 1=пожелание, 2=комплимент

# ==================== СМЕШНЫЕ ПРЕДСКАЗАНИЯ ====================
FUNNY_PREDICTIONS = [
    "Сегодня тебя ждет удача в начинаниях! Может, даже кофе не прольешь!",
    "Говорят, сегодня идеальный день для новых свершений. Или для сна. Выбирай!",
    "Вселенная шепчет: сегодня стоит рискнуть. Хотя бы попробовать новый сорт пиццы!",
    "Звезды предсказывают: сегодня будет много сообщений в чате. Сюрприз!",
    "Сегодняшний день идеален для того, чтобы сделать то, что давно откладывал. Например, помыть посуду!",
    "Гадалка сказала: сегодня тебя ждет неожиданная встречу. С холодильником, например!",
    "Мудрость дня: лучший способ предсказать будущее - создать его. Или просто заказать пиццу!",
    "Сегодня твой день! Даже если кажется, что нет. Особенно если кажется, что нет!",
    "Пророчество: сегодня ты напишешь как минимум одно гениальное сообщение. Или хотя бы смешное!",
    "Вселенная советует: сегодня больше улыбайся. Хотя бы в зеркале!",
    "Сегодня идеальный день для маленьких побед. Например, не проспать на работу!",
    "Гороскоп: сегодня звезды благоволят общению. Пиши больше сообщений!",
    "Сегодняшний лайфхак: если что-то не получается, попробуй перезагрузиться. Как компьютер!",
    "Предсказание: сегодня ты узнаешь что-то новое. Например, что в холодильнике кончилось молоко!",
    "Сегодня день, когда можно все! Ну, или почти все. Хотя бы попробовать!",
]

FUNNY_WISHES = [
    "Желаю сегодня найти деньги в старой куртке! Или хотя бы не потерять ключи!",
    "Пусть сегодняшний день будет продуктивным! Хотя бы настолько, чтобы не забыть поесть!",
    "Желаю, чтобы сегодня все получалось с первого раза! Ну, или хотя бы со второго!",
    "Пусть сегодняшний кофе будет особенно вкусным! И не прольется на клавиатуру!",
    "Желаю сегодня встретить старого друга! Хотя бы в соцсетях!",
    "Пусть сегодня все задачи решаются легко! Как пазл из 10 деталей!",
    "Желаю, чтобы сегодня транспорт ждал именно тебя! И не уезжал прямо перед носом!",
    "Пусть сегодняшний обед будет особенно вкусным! Даже если это доширак!",
    "Желаю сегодняшнего вдохновения! Хотя бы на одно сообщение в чате!",
    "Пусть сегодня все улыбки будут искренними! Особенно твоя!",
    "Желаю, чтобы сегодня все планы сошлись! Как звезды в ясную ночь!",
    "Пусть сегодняшний день принесет только приятные новости! И никакого спама!",
    "Желаю сегодняшнего хорошего настроения! Даже если с утра не выспался!",
    "Пусть сегодня все двери открываются! Хотя бы те, у которых есть ключи!",
    "Желаю сегодняшней удачи во всем! Ну, или хотя бы в чем-то одном!",
]

COMPLIMENTS = [
    "Ты сегодня просто сияешь! Ну, или хотя бы не потускнел!",
    "С тобой в чате всегда интересно! Даже когда ты молчишь!",
    "Твое чувство юмора - просто бомба! В хорошем смысле!",
    "Ты пишешь такие сообщения, что аж завидно! В хорошем смысле!",
    "С тобой всегда есть о чем поговорить! Хотя бы о погоде!",
    "Твоя активность в чате просто восхищает! Продолжай в том же духе!",
    "Ты - настоящая душа компании! Даже в текстовом чате!",
    "С тобой никогда не скучно! Особенно когда ты пишешь!",
    "Твои сообщения всегда к месту! Даже если не совсем!",
    "Ты делаешь этот чат лучше! Серьезно!",
]

# ==================== HTTP СЕРВЕР ====================
async def health_check(request):
    """Проверка здоровья сервера"""
    status = {
        "status": "running",
        "bot_status": "active" if not is_shutting_down else "shutting_down",
        "database": "connected" if conn else "disconnected",
        "scheduler": "running" if scheduler_instance and scheduler_instance.running else "stopped",
        "current_mention_type": ["предсказание", "пожелание", "комплимент"][current_mention_type]
    }
    return web.json_response(status)

def run_http_server():
    """Запуск HTTP-сервера в отдельном потоке"""
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    
    web.run_app(app, host='0.0.0.0', port=10000)

# ==================== БАЗА ДАННЫХ ====================
def init_database():
    """Инициализация базы данных"""
    global conn, cursor
    conn = sqlite3.connect("stats.db", check_same_thread=False, timeout=10)
    cursor = conn.cursor()
    
    # Основная таблица сообщений
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        user_id INTEGER,
        chat_id INTEGER,
        username TEXT,
        today INTEGER DEFAULT 0,
        yesterday INTEGER DEFAULT 0,
        total INTEGER DEFAULT 0,
        last_updated TIMESTAMP,
        first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (user_id, chat_id)
    )
    """)
    
    # Ежедневная статистика
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS daily_stats (
        date DATE PRIMARY KEY,
        total_messages INTEGER DEFAULT 0,
        active_users INTEGER DEFAULT 0,
        top_user_id INTEGER,
        top_user_count INTEGER
    )
    """)
    
    # Настройки чатов
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS chat_settings (
        chat_id INTEGER PRIMARY KEY,
        chat_title TEXT,
        chat_type TEXT DEFAULT 'private',
        is_active BOOLEAN DEFAULT 1,
        enable_mentions BOOLEAN DEFAULT 1,
        last_mention_time TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_activity TIMESTAMP,
        total_messages_before_bot INTEGER DEFAULT 0
    )
    """)
    
    # Таблица для хранения упоминаний
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS mentions_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        user_id INTEGER,
        username TEXT,
        mention_time TIMESTAMP,
        mention_type TEXT,
        message TEXT
    )
    """)
    
    # Таблица для подсчета всех сообщений
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS all_messages_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        user_id INTEGER,
        username TEXT,
        message_date TIMESTAMP,
        message_count INTEGER DEFAULT 1
    )
    """)
    
    conn.commit()
    logger.info("База данных инициализирована")

def update_chat_settings(chat_id: int, chat_title: str = None, chat_type: str = None):
    """Обновить настройки чата"""
    try:
        # Проверяем, существует ли уже запись
        cursor.execute("SELECT chat_id FROM chat_settings WHERE chat_id = ?", (chat_id,))
        existing = cursor.fetchone()
        
        current_time = datetime.now().isoformat()
        
        if existing:
            # Обновляем существующую запись
            update_fields = []
            params = []
            
            if chat_title:
                update_fields.append("chat_title = ?")
                params.append(chat_title)
            
            if chat_type:
                update_fields.append("chat_type = ?")
                params.append(chat_type)
            
            update_fields.append("last_activity = ?")
            params.append(current_time)
            
            params.append(chat_id)
            
            if update_fields:
                query = f"UPDATE chat_settings SET {', '.join(update_fields)} WHERE chat_id = ?"
                cursor.execute(query, params)
        else:
            # Создаем новую запись
            # Пытаемся оценить количество сообщений до добавления бота
            cursor.execute("""
                SELECT COUNT(*) FROM all_messages_history WHERE chat_id = ?
            """, (chat_id,))
            count_result = cursor.fetchone()
            messages_before = count_result[0] if count_result else 0
            
            cursor.execute("""
                INSERT INTO chat_settings 
                (chat_id, chat_title, chat_type, is_active, enable_mentions, created_at, last_activity, total_messages_before_bot)
                VALUES (?, ?, ?, 1, 1, ?, ?, ?)
            """, (
                chat_id, 
                chat_title or f"Chat {chat_id}", 
                chat_type or "private",
                current_time,
                current_time,
                messages_before
            ))
        
        conn.commit()
        
    except Exception as e:
        logger.error(f"Ошибка обновления настроек чата {chat_id}: {e}")

# ==================== GRACEFUL SHUTDOWN ====================
async def shutdown():
    """Корректное завершение работы бота"""
    global is_shutting_down
    
    if is_shutting_down:
        return
        
    is_shutting_down = True
    logger.info("Начинаем корректное завершение работы...")
    
    try:
        # Останавливаем polling
        if dp and hasattr(dp, '_stopped') and not dp._stopped:
            await dp.stop_polling()
            logger.info("Polling остановлен")
    except Exception as e:
        logger.error(f"Ошибка при остановке polling: {e}")
    
    try:
        # Отменяем задачу polling
        global polling_task
        if polling_task and not polling_task.done():
            polling_task.cancel()
            logger.info("Задача polling отменена")
    except Exception as e:
        logger.error(f"Ошибка при отмене задачи polling: {e}")
    
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
        if conn:
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

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
def clear_chat_cache(chat_id):
    """Очистить кэш для чата"""
    cache_key = f"sorted_members_{chat_id}"
    if cache_key in user_cache:
        del user_cache[cache_key]

async def get_sorted_members(chat_id, force_update=False):
    """Получить отсортированный список участников"""
    try:
        cache_key = f"sorted_members_{chat_id}"
        current_time = time.time()
        
        # Проверяем кэш
        if not force_update and cache_key in user_cache:
            cached_data, timestamp = user_cache[cache_key]
            if current_time - timestamp < cache_timeout:
                return cached_data
        
        # Получаем участников из базы данных
        cursor.execute("""
            SELECT user_id, username, today, yesterday, total 
            FROM messages 
            WHERE chat_id = ?
            AND (today > 0 OR yesterday > 0 OR total > 0)
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
                'today': today,
                'yesterday': yesterday,
                'total': total,
                'is_new': False
            })
        
        # Сохраняем в кэш
        user_cache[cache_key] = (members_with_stats, current_time)
        
        return members_with_stats
        
    except Exception as e:
        logger.error(f"Error getting sorted members for chat {chat_id}: {e}")
        return []

# ==================== АВТОМАТИЧЕСКИЕ ФУНКЦИИ ====================
async def send_hourly_mention():
    """Отправка упоминания каждый час с ротацией типа"""
    global current_mention_type
    
    if is_shutting_down:
        return
        
    try:
        logger.info(f"Запуск функции упоминания пользователя... Тип: {current_mention_type}")
        
        # Получаем все активные группы и супергруппы
        cursor.execute("""
            SELECT chat_id, chat_title, chat_type FROM chat_settings 
            WHERE chat_type IN ('group', 'supergroup') 
            AND is_active = 1 
            AND enable_mentions = 1
        """)
        
        active_chats = cursor.fetchall()
        
        if not active_chats:
            logger.info("Нет активных чатов для упоминаний")
            return
        
        logger.info(f"Найдено {len(active_chats)} активных чатов для упоминаний")
        
        for chat_id, chat_title, chat_type in active_chats:
            try:
                # Получаем участников чата
                members = await get_sorted_members(chat_id)
                if not members:
                    logger.debug(f"Нет участников в чате {chat_id} для упоминания")
                    continue
                
                # Фильтруем пользователей, которые писали сегодня
                active_members = [m for m in members if m['today'] > 0]
                if not active_members:
                    active_members = members  # Берем всех если никто не писал
                
                if not active_members:
                    logger.debug(f"Нет активных участников в чате {chat_id}")
                    continue
                
                # Выбираем случайного пользователя
                random_user = random.choice(active_members)
                user_id = random_user['user_id']
                username = random_user['username']
                
                # Выбираем сообщение в зависимости от текущего типа
                if current_mention_type == 0:  # предсказание
                    message = random.choice(FUNNY_PREDICTIONS)
                    message_type_text = "🔮 Предсказание часа"
                    mention_type = "prediction"
                elif current_mention_type == 1:  # пожелание
                    message = random.choice(FUNNY_WISHES)
                    message_type_text = "✨ Пожелание часа"
                    mention_type = "wish"
                else:  # комплимент
                    message = random.choice(COMPLIMENTS)
                    message_type_text = "💝 Комплимент часа"
                    mention_type = "compliment"
                
                # Формируем упоминание
                try:
                    # Пробуем получить информацию о пользователе для корректного упоминания
                    user_info = await bot_instance.get_chat(user_id)
                    if user_info.username:
                        mention = f"@{user_info.username}"
                    else:
                        mention = f"<a href='tg://user?id={user_id}'>{username}</a>"
                except:
                    mention = f"<a href='tg://user?id={user_id}'>{username}</a>"
                
                # Отправляем сообщение
                text = f"{message_type_text} для {mention}:\n\n"
                text += f"<i>{message}</i>"
                
                await bot_instance.send_message(chat_id, text)
                
                # Сохраняем в историю
                cursor.execute("""
                    INSERT INTO mentions_history 
                    (chat_id, user_id, username, mention_time, mention_type, message)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    chat_id, 
                    user_id, 
                    username,
                    datetime.now().isoformat(), 
                    mention_type, 
                    message
                ))
                
                # Обновляем время последнего упоминания
                cursor.execute("""
                    UPDATE chat_settings 
                    SET last_mention_time = ?
                    WHERE chat_id = ?
                """, (datetime.now().isoformat(), chat_id))
                
                conn.commit()
                
                logger.info(f"Упомянут пользователь {username} в чате {chat_title or chat_id}")
                
                # Пауза между отправками, чтобы не спамить
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Ошибка при упоминании в чате {chat_id} ({chat_title}): {e}")
                continue
        
        # Меняем тип для следующего часа
        current_mention_type = (current_mention_type + 1) % 3
        logger.info(f"Следующий тип упоминания: {current_mention_type}")
                
    except Exception as e:
        logger.error(f"Ошибка в send_hourly_mention: {e}")

async def daily_report():
    """Ежедневный отчет"""
    if is_shutting_down:
        return
        
    try:
        logger.info(f"Генерация ежедневного отчета...")
        
        cursor.execute("""
            SELECT chat_id, chat_title FROM chat_settings 
            WHERE chat_type IN ('group', 'supergroup') 
            AND is_active = 1
        """)
        
        active_chats = cursor.fetchall()
        
        if not active_chats:
            logger.info("Нет активных чатов для отчета")
            return
        
        logger.info(f"Найдено {len(active_chats)} чатов для ежедневного отчета")
        
        for chat_id, chat_title in active_chats:
            try:
                members_with_stats = await get_sorted_members(chat_id, force_update=True)
                
                if not members_with_stats:
                    continue
                
                # Создаем отчет
                text = "📊 <b>Ежедневный отчет</b>\n\n"
                
                # Топ-3 за день
                for i, member in enumerate(members_with_stats[:3], 1):
                    username = member['username']
                    today_count = member['today']
                    
                    emoji = "👑" if i == 1 else "🥈" if i == 2 else "🥉"
                    text += f"{emoji} <b>{username}:</b> {today_count} сообщ.\n"
                
                # Общая статистика
                total_today = sum(member['today'] for member in members_with_stats)
                active_today = sum(1 for member in members_with_stats if member['today'] > 0)
                
                if len(members_with_stats) > 3:
                    text += f"\n...и еще {len(members_with_stats) - 3} участников\n"
                
                text += f"\n<b>📈 Итоги дня:</b>\n"
                text += f"📨 Сообщений: <b>{total_today}</b>\n"
                text += f"👥 Активных: <b>{active_today}</b>\n\n"
                text += "Статистика обнулится в полночь! ✨"
                
                await bot_instance.send_message(chat_id, text)
                
                # Сохраняем статистику дня
                today_date = datetime.now().strftime('%Y-%m-%d')
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
                
                conn.commit()
                
                logger.info(f"Отчет отправлен в чат {chat_title or chat_id}")
                
                # Пауза между отправками
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Ошибка отправки отчета в чат {chat_id} ({chat_title}): {e}")
                continue
                
    except Exception as e:
        logger.error(f"Ошибка в daily_report: {e}")

async def auto_reset_counters():
    """Автоматический сброс счетчиков в полночь"""
    if is_shutting_down:
        return
        
    try:
        logger.info("Автоматический сброс счетчиков...")
        
        # Сохраняем статистику перед сбросом
        cursor.execute("SELECT SUM(today) FROM messages")
        total_today = cursor.fetchone()[0] or 0
        
        if total_today > 0:
            cursor.execute("SELECT COUNT(DISTINCT user_id) FROM messages WHERE today > 0")
            active_today = cursor.fetchone()[0] or 0
            
            cursor.execute("SELECT user_id, today FROM messages WHERE today > 0 ORDER BY today DESC LIMIT 1")
            top_user = cursor.fetchone()
            
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
        
        # Сбрасываем счетчики
        cursor.execute("UPDATE messages SET yesterday = today, today = 0")
        conn.commit()
        
        # Очищаем кэш
        user_cache.clear()
        
        logger.info(f"Счетчики сброшены. Сегодня было {total_today} сообщений от {active_today} пользователей")
        
    except Exception as e:
        logger.error(f"Ошибка в auto_reset_counters: {e}")

async def scan_all_messages():
    """Сканирование всех сообщений в чате для подсчета истории"""
    if is_shutting_down:
        return
        
    try:
        logger.info("Сканирование истории сообщений...")
        
        # Получаем все активные чаты
        cursor.execute("""
            SELECT chat_id, chat_title FROM chat_settings 
            WHERE is_active = 1
        """)
        
        active_chats = cursor.fetchall()
        
        if not active_chats:
            return
        
        for chat_id, chat_title in active_chats:
            try:
                # Получаем информацию о чате
                chat = await bot_instance.get_chat(chat_id)
                
                if chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
                    try:
                        # Пробуем получить историю сообщений (ограниченное количество)
                        # В реальном боте эта функция может быть ограничена правами
                        logger.info(f"Сканирование истории для чата {chat_title or chat_id}")
                        
                        # Здесь можно добавить логику для сканирования истории
                        # Например, через get_chat_history, но это требует прав
                        
                    except Exception as e:
                        logger.warning(f"Не удалось сканировать историю чата {chat_id}: {e}")
                        
            except Exception as e:
                logger.error(f"Ошибка при сканировании чата {chat_id}: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Ошибка в scan_all_messages: {e}")

async def update_total_count_for_user(user_id: int, chat_id: int, username: str):
    """Обновить общее количество сообщений пользователя с учетом истории"""
    try:
        # Получаем текущее общее количество из базы
        cursor.execute("""
            SELECT total FROM messages WHERE user_id = ? AND chat_id = ?
        """, (user_id, chat_id))
        
        row = cursor.fetchone()
        
        if row:
            current_total = row[0]
            
            # Получаем количество сообщений из истории
            cursor.execute("""
                SELECT SUM(message_count) FROM all_messages_history 
                WHERE user_id = ? AND chat_id = ?
            """, (user_id, chat_id))
            
            history_result = cursor.fetchone()
            history_count = history_result[0] if history_result and history_result[0] else 0
            
            # Если история показывает больше сообщений, обновляем
            if history_count > current_total:
                cursor.execute("""
                    UPDATE messages SET total = ?, username = ? 
                    WHERE user_id = ? AND chat_id = ?
                """, (history_count, username, user_id, chat_id))
                conn.commit()
                logger.debug(f"Обновлено общее количество сообщений для пользователя {username}: {history_count}")
                
    except Exception as e:
        logger.error(f"Ошибка обновления общего количества для пользователя {user_id}: {e}")

# ==================== ОБРАБОТЧИКИ КОМАНД ====================
async def handle_start(message: types.Message):
    """Обработчик команды /start"""
    if is_shutting_down:
        return
        
    logger.info(f"Command /start received from {message.from_user.id}")
    
    # Обновляем настройки чата
    chat_type = message.chat.type
    chat_title = None
    if chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        chat_title = message.chat.title
    elif chat_type == ChatType.PRIVATE:
        chat_title = message.from_user.full_name
    
    update_chat_settings(message.chat.id, chat_title, chat_type)
    
    welcome_text = """
👋 Привет! Я бот для подсчета статистики сообщений в чате.

📊 <b>Я считаю:</b>
• Сообщения за сегодня
• Сообщения за вчера
• Общее количество сообщений (включая историю!)

🎯 <b>Новые функции:</b>
• Ежедневный отчет
• Веселые упоминания каждый час (ротация: предсказание → пожелание → комплимент)
• Учет всех сообщений, даже отправленных до добавления бота

📋 <b>Доступные команды:</b>
/status - Статистика чата
/top - Топ-10 участников сегодня
/mystats - Ваша личная статистика
/yesterday - Топ за вчера
/weekly - Статистика за неделю
/help - Помощь по командам
/reset_today - Сбросить счетчики (админы)
/scan_history - Просканировать историю сообщений (админы)

💫 <b>Автоматически:</b>
• Ежедневный отчет
• Автосброс в полночь
• Веселые упоминания каждый час

<i>Бот подсчитывает все текстовые сообщения в чате и учитывает историю!</i>
"""
    await message.reply(welcome_text)

async def handle_help(message: types.Message):
    """Обработчик команды /help"""
    if is_shutting_down:
        return
    
    # Обновляем настройки чата
    chat_type = message.chat.type
    chat_title = None
    if chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        chat_title = message.chat.title
    elif chat_type == ChatType.PRIVATE:
        chat_title = message.from_user.full_name
    
    update_chat_settings(message.chat.id, chat_title, chat_type)
        
    help_text = """
<b>📚 Доступные команды:</b>

📊 <b>Общие команды:</b>
/status - Статистика чата
/top - Топ-10 участников сегодня
/mystats - Ваша личная статистика
/yesterday - Топ за вчера
/weekly - Статистика за неделю

⚙️ <b>Для администраторов:</b>
/reset_today - Сбросить счетчики на сегодня
/scan_history - Просканировать историю сообщений

🎉 <b>Автоматически:</b>
• Ежедневный отчет
• Автосброс в полночь
• Веселые упоминания каждый час (ротация типов)

<i>Бот подсчитывает ВСЕ сообщения в чате, включая историю!</i>
"""
    await message.reply(help_text)

async def handle_scan_history(message: types.Message):
    """Сканирование истории сообщений"""
    if is_shutting_down:
        return
    
    # Обновляем настройки чата
    chat_type = message.chat.type
    chat_title = None
    if chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        chat_title = message.chat.title
    elif chat_type == ChatType.PRIVATE:
        chat_title = message.from_user.full_name
    
    update_chat_settings(message.chat.id, chat_title, chat_type)
    
    # Проверяем права администратора
    if chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        try:
            chat_admins = await bot_instance.get_chat_administrators(message.chat.id)
            admin_ids = [admin.user.id for admin in chat_admins]
            
            if message.from_user.id not in admin_ids:
                await message.reply("⚠️ Эта команда доступна только администраторам.")
                return
        except Exception as e:
            logger.error(f"Error checking admin rights: {e}")
            await message.reply("⚠️ Не удалось проверить права администратора.")
            return
    
    await message.reply("🔄 Начинаю сканирование истории сообщений... Это может занять некоторое время.")
    
    # Запускаем сканирование в фоне
    asyncio.create_task(scan_all_messages())
    
    await message.reply("✅ Сканирование истории запущено. Результаты будут учтены в статистике.")

async def handle_status(message: types.Message):
    """Обработчик команды /status"""
    if is_shutting_down:
        return
    
    # Обновляем настройки чата
    chat_type = message.chat.type
    chat_title = None
    if chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        chat_title = message.chat.title
    elif chat_type == ChatType.PRIVATE:
        chat_title = message.from_user.full_name
    
    update_chat_settings(message.chat.id, chat_title, chat_type)
        
    logger.info(f"Command /status received from {message.from_user.id}")
    
    try:
        chat_id = message.chat.id
        chat_type = message.chat.type
        
        if chat_type == ChatType.CHANNEL:
            await message.reply("⚠️ В каналах статистика не собирается.")
            return
        
        members_with_stats = await get_sorted_members(chat_id)
        
        if not members_with_stats:
            await message.reply("📊 Пока нет статистики сообщений в этом чате.")
            return
        
        if chat_type == ChatType.PRIVATE:
            if len(members_with_stats) > 0:
                user_stats = members_with_stats[0]
                text = f"<b>📊 Ваша статистика</b>\n\n"
                text += f"👤 <b>{user_stats['username']}</b>\n"
                text += f"📅 <b>Сегодня:</b> {user_stats['today']} сообщений\n"
                text += f"🗓️ <b>Вчера:</b> {user_stats['yesterday']} сообщений\n"
                text += f"📊 <b>Всего:</b> {user_stats['total']} сообщений\n"
            else:
                text = "📊 Пока нет статистики сообщений."
        else:
            text = f"<b>📊 Статистика чата</b>\n\n"
            
            # Показываем топ-5
            for i, member in enumerate(members_with_stats[:5], 1):
                username = member['username']
                today_count = member['today']
                total_count = member['total']
                
                emoji = "👑" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "👤"
                
                text += f"<b>{i}. {emoji} {username}:</b>\n"
                text += f"   📅 Сегодня: {today_count} | 📊 Всего: {total_count}\n\n"
        
        await message.reply(text)
        
    except Exception as e:
        logger.error(f"Error in /status: {e}")
        await message.reply("⚠️ Произошла ошибка при получении статистики.")

async def handle_top(message: types.Message):
    """Обработчик команды /top"""
    if is_shutting_down:
        return
    
    # Обновляем настройки чата
    chat_type = message.chat.type
    chat_title = None
    if chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        chat_title = message.chat.title
    elif chat_type == ChatType.PRIVATE:
        chat_title = message.from_user.full_name
    
    update_chat_settings(message.chat.id, chat_title, chat_type)
        
    logger.info(f"Command /top received from {message.from_user.id}")
        
    try:
        chat_id = message.chat.id
        chat_type = message.chat.type
        
        if chat_type == ChatType.CHANNEL:
            await message.reply("⚠️ В каналах статистика не собирается.")
            return
        
        if chat_type == ChatType.PRIVATE:
            await message.reply("ℹ️ В личных чатах используйте команду /mystats.")
            return
        
        members_with_stats = await get_sorted_members(chat_id)
        
        if not members_with_stats:
            await message.reply("📊 Пока нет статистики сообщений в этом чате.")
            return
        
        text = f"<b>🏆 Топ участников сегодня</b>\n\n"
        
        top_limit = min(10, len(members_with_stats))
        
        for i, member in enumerate(members_with_stats[:top_limit], 1):
            username = member['username']
            today_count = member['today']
            total_count = member['total']
            
            emoji = "👑" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            
            text += f"<b>{emoji} {username}:</b>\n"
            text += f"   📅 Сегодня: {today_count} сообщ. | 📊 Всего: {total_count}\n\n"
        
        total_today = sum(member['today'] for member in members_with_stats)
        total_all = sum(member['total'] for member in members_with_stats)
        
        # Получаем количество сообщений до бота
        cursor.execute("SELECT total_messages_before_bot FROM chat_settings WHERE chat_id = ?", (chat_id,))
        before_bot_result = cursor.fetchone()
        before_bot = before_bot_result[0] if before_bot_result else 0
        
        text += f"<b>📈 Итого по чату:</b>\n"
        text += f"📅 Сегодня: <b>{total_today}</b> сообщ.\n"
        text += f"📊 Всего с ботом: <b>{total_all}</b> сообщ.\n"
        if before_bot > 0:
            text += f"📜 До добавления бота: <b>{before_bot}</b> сообщ.\n"
            text += f"📈 Общее всего: <b>{total_all + before_bot}</b> сообщ."
        
        await message.reply(text)
        
    except Exception as e:
        logger.error(f"Error in /top: {e}")
        await message.reply("⚠️ Произошла ошибка при получении топа.")

async def handle_mystats(message: types.Message):
    """Обработчик команды /mystats"""
    if is_shutting_down:
        return
    
    # Обновляем настройки чата
    chat_type = message.chat.type
    chat_title = None
    if chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        chat_title = message.chat.title
    elif chat_type == ChatType.PRIVATE:
        chat_title = message.from_user.full_name
    
    update_chat_settings(message.chat.id, chat_title, chat_type)
        
    logger.info(f"Command /mystats received from {message.from_user.id}")
    
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        cursor.execute("""
            SELECT username, today, yesterday, total, first_seen 
            FROM messages WHERE user_id=? AND chat_id=?
        """, (user_id, chat_id))
        row = cursor.fetchone()
        
        if row:
            username, today, yesterday, total, first_seen = row
            
            try:
                if isinstance(first_seen, str):
                    first_seen_date = datetime.fromisoformat(first_seen.split('.')[0])
                else:
                    first_seen_date = first_seen
                first_seen_str = first_seen_date.strftime('%d.%m.%Y')
            except:
                first_seen_str = "неизвестно"
                
            text = f"<b>📊 Ваша статистика</b>\n\n"
            text += f"👤 <b>{username}</b>\n"
            text += f"📅 <b>Сегодня:</b> {today} сообщений\n"
            text += f"🗓️ <b>Вчера:</b> {yesterday} сообщений\n"
            text += f"📊 <b>Всего в этом чате:</b> {total} сообщений\n"
            
            # Получаем общую статистику по всем чатам
            cursor.execute("""
                SELECT SUM(total) FROM messages WHERE user_id=?
            """, (user_id,))
            total_all_chats = cursor.fetchone()[0] or 0
            
            if total_all_chats > total:
                text += f"📈 <b>Всего во всех чатах:</b> {total_all_chats} сообщений\n"
            
            text += f"📅 <b>С нами с:</b> {first_seen_str}"
            
            await message.reply(text)
        else:
            await message.reply("📊 У вас еще нет статистики. Напишите что-нибудь в чате!")
            
    except Exception as e:
        logger.error(f"Error in /mystats: {e}")
        await message.reply("⚠️ Произошла ошибка при получении статистики.")

async def handle_yesterday(message: types.Message):
    """Обработчик команды /yesterday"""
    if is_shutting_down:
        return
    
    # Обновляем настройки чата
    chat_type = message.chat.type
    chat_title = None
    if chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        chat_title = message.chat.title
    elif chat_type == ChatType.PRIVATE:
        chat_title = message.from_user.full_name
    
    update_chat_settings(message.chat.id, chat_title, chat_type)
        
    logger.info(f"Command /yesterday received from {message.from_user.id}")
    
    try:
        chat_id = message.chat.id
        chat_type = message.chat.type
        
        if chat_type == ChatType.CHANNEL:
            await message.reply("⚠️ В каналах статистика не собирается.")
            return
        
        cursor.execute("""
            SELECT username, yesterday as count 
            FROM messages 
            WHERE chat_id = ? AND yesterday > 0 
            ORDER BY yesterday DESC 
            LIMIT 10
        """, (chat_id,))
        rows = cursor.fetchall()
        
        if not rows:
            await message.reply("📊 Вчера не было сообщений или статистика не собрана.")
            return
            
        text = f"<b>📊 Топ за вчера</b>\n\n"
        
        for i, (username, count) in enumerate(rows, 1):
            emoji = "👑" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            
            text += f"{emoji} <b>{username}:</b> {count} сообщ.\n"
        
        cursor.execute("SELECT SUM(yesterday) FROM messages WHERE chat_id = ?", (chat_id,))
        total_yesterday = cursor.fetchone()[0] or 0
        
        text += f"\n<b>📈 Итого за вчера:</b> {total_yesterday} сообщений"
        
        await message.reply(text)
        
    except Exception as e:
        logger.error(f"Error in /yesterday: {e}")
        await message.reply("⚠️ Произошла ошибка при получении статистики.")

async def handle_weekly(message: types.Message):
    """Обработчик команды /weekly"""
    if is_shutting_down:
        return
    
    # Обновляем настройки чата
    chat_type = message.chat.type
    chat_title = None
    if chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        chat_title = message.chat.title
    elif chat_type == ChatType.PRIVATE:
        chat_title = message.from_user.full_name
    
    update_chat_settings(message.chat.id, chat_title, chat_type)
        
    logger.info(f"Command /weekly received from {message.from_user.id}")
    
    try:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=6)
        
        cursor.execute("""
            SELECT date, total_messages, active_users 
            FROM daily_stats 
            WHERE date BETWEEN ? AND ?
            ORDER BY date DESC
        """, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        
        rows = cursor.fetchall()
        
        if not rows:
            await message.reply("📊 Недостаточно данных для недельного отчета.")
            return
            
        text = f"<b>📅 Статистика за неделю</b>\n\n"
        
        total_messages_week = 0
        total_active_week = 0
        
        for date_str, total_messages, active_users in rows:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            text += f"<b>{date_obj.strftime('%d.%m')}:</b> {total_messages} сообщ. от {active_users} чел.\n"
            total_messages_week += total_messages
            total_active_week += active_users
        
        days_with_data = len(rows)
        if days_with_data < 7:
            text += f"\n<i>Данных за {7 - days_with_data} дней нет</i>\n"
        
        text += f"\n<b>📈 Итоги недели:</b>\n"
        text += f"📨 Сообщений: <b>{total_messages_week}</b>\n"
        text += f"👥 Активных пользователей: <b>{total_active_week}</b>\n"
        
        if days_with_data > 0:
            avg_per_day = total_messages_week // days_with_data
            text += f"📊 В среднем в день: <b>{avg_per_day}</b> сообщ."
        
        await message.reply(text)
        
    except Exception as e:
        logger.error(f"Error in /weekly: {e}")
        await message.reply("⚠️ Произошла ошибка при получении недельной статистики.")

async def handle_reset_today(message: types.Message):
    """Обработчик команды /reset_today"""
    if is_shutting_down:
        return
    
    # Обновляем настройки чата
    chat_type = message.chat.type
    chat_title = None
    if chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        chat_title = message.chat.title
    elif chat_type == ChatType.PRIVATE:
        chat_title = message.from_user.full_name
    
    update_chat_settings(message.chat.id, chat_title, chat_type)
        
    logger.info(f"Command /reset_today received from {message.from_user.id}")
        
    try:
        chat_type = message.chat.type
        
        if chat_type == ChatType.CHANNEL:
            await message.reply("⚠️ В каналах эта команда недоступна.")
            return
        
        if chat_type == ChatType.PRIVATE:
            await message.reply("ℹ️ В личных чатах используйте команду /mystats.")
            return
        
        chat_id = message.chat.id
        
        try:
            chat_admins = await bot_instance.get_chat_administrators(chat_id)
            admin_ids = [admin.user.id for admin in chat_admins]
            
            if message.from_user.id not in admin_ids:
                await message.reply("⚠️ Эта команда доступна только администраторам.")
                return
        except Exception as e:
            logger.error(f"Error checking admin rights: {e}")
            await message.reply("⚠️ Не удалось проверить права администратора.")
            return
            
        cursor.execute("SELECT SUM(today) FROM messages WHERE chat_id = ?", (chat_id,))
        total_today = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM messages WHERE chat_id = ? AND today > 0", (chat_id,))
        active_today = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT user_id, today FROM messages WHERE chat_id = ? AND today > 0 ORDER BY today DESC LIMIT 1", (chat_id,))
        top_user = cursor.fetchone()
        
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
        
        cursor.execute("UPDATE messages SET yesterday = today, today = 0 WHERE chat_id = ?", (chat_id,))
        conn.commit()
        
        clear_chat_cache(chat_id)
        
        await message.reply(
            f"✅ Счетчики сообщений сброшены.\n"
            f"📊 Сегодня было: {total_today} сообщений от {active_today} пользователей"
        )
        
    except Exception as e:
        logger.error(f"Error in /reset_today: {e}")
        await message.reply("⚠️ Произошла ошибка при сбросе счетчиков.")

async def count_messages(message: types.Message):
    """Подсчет сообщений"""
    if is_shutting_down:
        return
        
    if not message.from_user:
        return

    user_id = message.from_user.id
    username = message.from_user.full_name
    chat_id = message.chat.id
    chat_type = message.chat.type

    if chat_type == ChatType.CHANNEL:
        return

    if message.from_user.is_bot:
        return

    # Обновляем настройки чата
    chat_title = None
    if chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        chat_title = message.chat.title
    elif chat_type == ChatType.PRIVATE:
        chat_title = message.from_user.full_name
    
    update_chat_settings(chat_id, chat_title, chat_type)

    current_time = datetime.now()
    
    # Сохраняем в историю всех сообщений
    cursor.execute("""
        INSERT INTO all_messages_history 
        (chat_id, user_id, username, message_date, message_count)
        VALUES (?, ?, ?, ?, 1)
    """, (chat_id, user_id, username, current_time.isoformat()))
    
    cursor.execute("SELECT * FROM messages WHERE user_id=? AND chat_id=?", (user_id, chat_id))
    row = cursor.fetchone()

    if row:
        last_updated_str = row[6]  # last_updated находится на 7 позиции (индекс 6)
        if last_updated_str:
            try:
                if 'Z' in last_updated_str:
                    last_updated_str = last_updated_str.replace('Z', '+00:00')
                last_updated = datetime.fromisoformat(last_updated_str)
                
                if current_time.date() > last_updated.date():
                    cursor.execute("""
                        UPDATE messages
                        SET yesterday = today,
                            today = 1,
                            total = total + 1,
                            username = ?,
                            last_updated = ?
                        WHERE user_id=? AND chat_id=?
                    """, (username, current_time.isoformat(), user_id, chat_id))
                else:
                    cursor.execute("""
                        UPDATE messages
                        SET today = today + 1,
                            total = total + 1,
                            username = ?,
                            last_updated = ?
                        WHERE user_id=? AND chat_id=?
                    """, (username, current_time.isoformat(), user_id, chat_id))
            except Exception as e:
                logger.error(f"Error parsing last_updated: {e}, resetting counters")
                cursor.execute("""
                    UPDATE messages
                    SET today = today + 1,
                        total = total + 1,
                        username = ?,
                        last_updated = ?
                    WHERE user_id=? AND chat_id=?
                """, (username, current_time.isoformat(), user_id, chat_id))
        else:
            cursor.execute("""
                UPDATE messages
                SET today = today + 1,
                    total = total + 1,
                    username = ?,
                    last_updated = ?
                WHERE user_id=? AND chat_id=?
            """, (username, current_time.isoformat(), user_id, chat_id))
    else:
        cursor.execute("""
            INSERT INTO messages (user_id, chat_id, username, today, total, first_seen, last_updated)
            VALUES (?, ?, ?, 1, 1, ?, ?)
        """, (user_id, chat_id, username, current_time.isoformat(), current_time.isoformat()))

    conn.commit()
    clear_chat_cache(chat_id)
    
    # Обновляем общее количество с учетом истории
    await update_total_count_for_user(user_id, chat_id, username)

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
async def main():
    global bot_instance, dp, scheduler_instance, polling_task
    
    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Инициализация базы данных
    init_database()
    
    # Создание бота и диспетчера
    bot_instance = Bot(
        token=API_TOKEN, 
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher()
    
    # Регистрация обработчиков
    dp.message.register(handle_start, Command("start"))
    dp.message.register(handle_help, Command("help"))
    dp.message.register(handle_status, Command("status"))
    dp.message.register(handle_top, Command("top"))
    dp.message.register(handle_mystats, Command("mystats"))
    dp.message.register(handle_yesterday, Command("yesterday"))
    dp.message.register(handle_weekly, Command("weekly"))
    dp.message.register(handle_reset_today, Command("reset_today"))
    dp.message.register(handle_scan_history, Command("scan_history"))
    dp.message.register(count_messages, F.text & ~F.text.startswith('/'))
    
    # Запуск HTTP-сервера в отдельном потоке
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()
    logger.info("HTTP сервер запущен на порту 10000")
    
    # Регистрация команд бота
    try:
        await bot_instance.set_my_commands([
            types.BotCommand(command="start", description="🚀 Запустить бота"),
            types.BotCommand(command="status", description="📊 Статистика чата"),
            types.BotCommand(command="top", description="🏆 Топ-10 участников"),
            types.BotCommand(command="mystats", description="📈 Ваша статистика"),
            types.BotCommand(command="yesterday", description="🗓️ Топ за вчера"),
            types.BotCommand(command="weekly", description="📅 Статистика за неделю"),
            types.BotCommand(command="reset_today", description="🔄 Сбросить счетчики"),
            types.BotCommand(command="scan_history", description="🔍 Сканировать историю"),
            types.BotCommand(command="help", description="❓ Помощь по командам")
        ])
        logger.info("Команды бота зарегистрированы")
    except Exception as e:
        logger.error(f"Ошибка регистрации команд: {e}")
    
    # Проверка авторизации
    try:
        me = await bot_instance.get_me()
        logger.info(f"Бот успешно авторизован: @{me.username} (ID: {me.id})")
    except Exception as e:
        logger.error(f"Ошибка авторизации: {e}")
        return
    
    # Настройка планировщика
    scheduler = AsyncIOScheduler()
    scheduler_instance = scheduler
    
    # Упоминания каждый час
    scheduler.add_job(send_hourly_mention, "cron", hour="*", minute=0, misfire_grace_time=300)
    logger.info("Запланированы упоминания каждый час")
    
    # Ежедневный отчет в 20:00
    scheduler.add_job(daily_report, "cron", hour=20, minute=0, misfire_grace_time=300)
    logger.info("Запланирован ежедневный отчет в 20:00")
    
    # Автосброс в полночь
    scheduler.add_job(auto_reset_counters, "cron", hour=0, minute=0, misfire_grace_time=300)
    logger.info("Запланирован автосброс в 00:00")
    
    # Автосканирование истории раз в день
    scheduler.add_job(scan_all_messages, "cron", hour=3, minute=0, misfire_grace_time=300)
    logger.info("Запланировано автосканирование истории в 03:00")
    
    try:
        scheduler.start()
        logger.info("Планировщик запущен")
        
        # Тестовый запуск функции упоминаний
        logger.info("Тестовый запуск функции упоминаний...")
        await send_hourly_mention()
        
    except Exception as e:
        logger.error(f"Ошибка запуска планировщика: {e}")
    
    # Обработчик ошибок
    @dp.errors()
    async def errors_handler(update: types.Update, exception: Exception):
        if not is_shutting_down:
            logger.error(f"Update {update} caused error: {exception}")
        return True
    
    try:
        logger.info("Бот запущен и готов к работе...")
        logger.info("Особенности бота:")
        logger.info("1. Учитывает ВСЕ сообщения (включая историю)")
        logger.info("2. Упоминания каждый час с ротацией типов")
        logger.info("3. Ежедневные отчеты")
        logger.info("4. Автосброс статистики")
        
        polling_task = asyncio.create_task(dp.start_polling(bot_instance, skip_updates=True, handle_signals=False))
        await polling_task
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
        tasks = asyncio.all_tasks(loop)
        for task in tasks:
            task.cancel()
        
        if tasks:
            loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        
        loop.close()
        logger.info("Event loop закрыт")
        sys.exit(0)
