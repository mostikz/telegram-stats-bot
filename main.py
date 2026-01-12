# main.py
import asyncio
import logging
import signal
import sys
import threading
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pytz
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.filters import Command
from aiogram import F
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ==================== КОНСТАНТЫ И НАСТРОЙКИ ====================
API_TOKEN = "8280794130:AAE7VgMxB0mGR2adpu8FR3SBUS-YjKUydjI"  # Замените на ваш токен
MOSCOW_TZ = pytz.timezone('Europe/Moscow')
DB_PATH = "stats.db"
HTTP_PORT = 10000
CACHE_TIMEOUT = 300  # 5 минут

# ==================== НАСТРОЙКА ЛОГГИРОВАНИЯ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ====================
bot_instance: Optional[Bot] = None
dp_instance: Optional[Dispatcher] = None
scheduler_instance: Optional[AsyncIOScheduler] = None
is_shutting_down = False
polling_task: Optional[asyncio.Task] = None
user_cache: Dict[str, tuple] = {}
db_conn: Optional[sqlite3.Connection] = None

# ==================== УТИЛИТЫ ВРЕМЕНИ ====================
def get_moscow_time() -> datetime:
    """Получить текущее московское время"""
    return datetime.now(MOSCOW_TZ)

def format_time(dt: datetime, format_str: str = "%H:%M:%S") -> str:
    """Форматировать время"""
    if dt.tzinfo is None:
        dt = MOSCOW_TZ.localize(dt)
    return dt.strftime(format_str)

def should_reset_counters(last_updated: datetime) -> bool:
    """Проверить, нужно ли сбрасывать счетчики (новый день по Москве)"""
    current_time = get_moscow_time()
    return current_time.date() > last_updated.date()

# ==================== HTTP СЕРВЕР ====================
async def health_check(request):
    """Проверка здоровья сервера"""
    current_time = get_moscow_time()
    status = {
        "status": "running",
        "moscow_time": format_time(current_time),
        "cache_size": len(user_cache),
        "shutting_down": is_shutting_down
    }
    return web.json_response(status)

def run_http_server():
    """Запуск HTTP-сервера в отдельном потоке"""
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    
    web.run_app(app, host='0.0.0.0', port=HTTP_PORT)

# ==================== БАЗА ДАННЫХ ====================
def init_database():
    """Инициализация базы данных"""
    global db_conn
    db_conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
    cursor = db_conn.cursor()
    
    # Таблица сообщений
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        today INTEGER DEFAULT 0,
        yesterday INTEGER DEFAULT 0,
        total INTEGER DEFAULT 0,
        last_updated TIMESTAMP,
        first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Таблица ежедневной статистики
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS daily_stats (
        date DATE PRIMARY KEY,
        total_messages INTEGER DEFAULT 0,
        active_users INTEGER DEFAULT 0,
        top_user_id INTEGER,
        top_user_count INTEGER
    )
    """)
    
    # Таблица настроек чатов
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS chat_settings (
        chat_id INTEGER PRIMARY KEY,
        chat_type TEXT,
        auto_reset_time TEXT DEFAULT '00:00',
        report_time TEXT DEFAULT '23:59',
        timezone TEXT DEFAULT 'Europe/Moscow',
        is_active BOOLEAN DEFAULT 1
    )
    """)
    
    db_conn.commit()
    logger.info("База данных инициализирована")

def get_db_cursor():
    """Получить курсор базы данных"""
    return db_conn.cursor()

# ==================== СЕРВИС СТАТИСТИКИ ====================
class StatsService:
    """Сервис для работы со статистикой"""
    
    @staticmethod
    async def get_sorted_members(chat_id: int, force_update: bool = False) -> List[Dict]:
        """Получить отсортированный список участников чата"""
        global user_cache
        
        try:
            cache_key = f"sorted_members_{chat_id}"
            current_time = time.time()
            
            # Проверка кэша
            if not force_update and cache_key in user_cache:
                cached_data, timestamp = user_cache[cache_key]
                if current_time - timestamp < CACHE_TIMEOUT:
                    return cached_data
            
            cursor = get_db_cursor()
            
            # Получаем топ пользователей из базы данных
            cursor.execute("""
                SELECT user_id, username, today, yesterday, total
                FROM messages
                WHERE today > 0 OR yesterday > 0
                ORDER BY today DESC, total DESC
                LIMIT 50
            """)
            
            rows = cursor.fetchall()
            members_with_stats = []
            
            for row in rows:
                user_id, username, today, yesterday, total = row
                members_with_stats.append({
                    'user_id': user_id,
                    'username': username,
                    'today': today,
                    'yesterday': yesterday,
                    'total': total
                })
            
            # Сохраняем в кэш
            user_cache[cache_key] = (members_with_stats, current_time)
            return members_with_stats
            
        except Exception as e:
            logger.error(f"Ошибка получения участников: {e}")
            return []
    
    @staticmethod
    def update_user_message(user_id: int, username: str, chat_id: int):
        """Обновить статистику сообщений пользователя"""
        try:
            cursor = get_db_cursor()
            current_time = get_moscow_time()
            
            # Получаем текущую запись пользователя
            cursor.execute(
                "SELECT last_updated, today FROM messages WHERE user_id = ?",
                (user_id,)
            )
            row = cursor.fetchone()
            
            if row:
                last_updated_str, today_count = row
                
                # Преобразуем строку времени в datetime
                if last_updated_str:
                    if 'Z' in last_updated_str:
                        last_updated_str = last_updated_str.replace('Z', '+00:00')
                    last_updated = datetime.fromisoformat(last_updated_str)
                    
                    # Проверяем, нужно ли сбросить счетчики (новый день)
                    if should_reset_counters(last_updated):
                        # Переносим сегодняшние во вчерашние
                        cursor.execute("""
                            UPDATE messages
                            SET yesterday = today,
                                today = 1,
                                total = total + 1,
                                username = ?,
                                last_updated = ?
                            WHERE user_id = ?
                        """, (username, current_time.isoformat(), user_id))
                    else:
                        # Увеличиваем счетчик сегодня
                        cursor.execute("""
                            UPDATE messages
                            SET today = today + 1,
                                total = total + 1,
                                username = ?,
                                last_updated = ?
                            WHERE user_id = ?
                        """, (username, current_time.isoformat(), user_id))
                else:
                    # Если last_updated пустой, обрабатываем как новое сообщение
                    cursor.execute("""
                        UPDATE messages
                        SET today = today + 1,
                            total = total + 1,
                            username = ?,
                            last_updated = ?
                        WHERE user_id = ?
                    """, (username, current_time.isoformat(), user_id))
            else:
                # Новый пользователь
                cursor.execute("""
                    INSERT INTO messages 
                    (user_id, username, today, total, last_updated)
                    VALUES (?, ?, 1, 1, ?)
                """, (user_id, username, current_time.isoformat()))
            
            db_conn.commit()
            
            # Очищаем кэш для этого чата
            StatsService.clear_chat_cache(chat_id)
            
        except Exception as e:
            logger.error(f"Ошибка обновления статистики: {e}")
    
    @staticmethod
    def clear_chat_cache(chat_id: int):
        """Очистить кэш для чата"""
        global user_cache
        keys_to_remove = [k for k in user_cache.keys() if f"_{chat_id}" in k]
        for key in keys_to_remove:
            del user_cache[key]
    
    @staticmethod
    def get_user_stats(user_id: int) -> Optional[Dict]:
        """Получить статистику пользователя"""
        try:
            cursor = get_db_cursor()
            cursor.execute("""
                SELECT username, today, yesterday, total, first_seen
                FROM messages WHERE user_id = ?
            """, (user_id,))
            
            row = cursor.fetchone()
            if row:
                username, today, yesterday, total, first_seen = row
                return {
                    'username': username,
                    'today': today,
                    'yesterday': yesterday,
                    'total': total,
                    'first_seen': first_seen
                }
            return None
        except Exception as e:
            logger.error(f"Ошибка получения статистики пользователя: {e}")
            return None

# ==================== ОБРАБОТЧИКИ КОМАНД ====================
async def handle_start(message: types.Message):
    """Обработчик команды /start"""
    if is_shutting_down:
        return
    
    moscow_time = get_moscow_time()
    
    welcome_text = f"""
👋 Привет! Я бот для подсчета статистики сообщений в чате.

🕐 <b>Текущее время в Москве:</b> {format_time(moscow_time)}

📊 <b>Я считаю:</b>
• Сообщения за сегодня
• Сообщения за вчера
• Общее количество сообщений

📋 <b>Доступные команды:</b>

📊 <b>Общие команды:</b>
/status - Статистика чата
/top - Топ-10 участников сегодня
/mystats - Ваша личная статистика
/yesterday - Топ за вчера
/weekly - Статистика за неделю
/help - Помощь по командам

⚙️ <b>Для администраторов:</b>
/reset_today - Сбросить счетчики на сегодня

📅 <b>Автоматически:</b>
• Ежедневный отчет в 23:59 (МСК)
• Автосброс в 00:00 (МСК)

<i>Бот подсчитывает все текстовые сообщения в чате</i>
Добавьте меня в группу для лучшей работы!
"""
    await message.reply(welcome_text)

async def handle_help(message: types.Message):
    """Обработчик команды /help"""
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

📅 <b>Автоматически (по московскому времени):</b>
• Ежедневный отчет в 23:59
• Автосброс в 00:00

<i>Бот подсчитывает все текстовые сообщения в чате</i>

💡 <b>Совет:</b> Добавьте бота в группу для лучшей работы!
"""
    await message.reply(help_text)

async def handle_top(message: types.Message):
    """Показать топ участников по сообщениям сегодня"""
    if is_shutting_down:
        return
    
    try:
        chat_id = message.chat.id
        chat_type = message.chat.type
        
        if chat_type == ChatType.CHANNEL:
            await message.reply("⚠️ В каналах статистика не собирается.")
            return
        
        if chat_type == ChatType.PRIVATE:
            await message.reply("ℹ️ В личных чатах используйте команду /mystats.")
            return
        
        # Получаем отсортированный список участников
        members = await StatsService.get_sorted_members(chat_id)
        
        if not members:
            await message.reply("📊 Пока нет статистики сообщений в этом чате.")
            return
        
        # Формируем сообщение
        moscow_time = get_moscow_time()
        text = "<b>🏆 Топ участников сегодня</b>\n"
        text += f"<i>Время в Москве: {format_time(moscow_time)}</i>\n\n"
        
        # Показываем топ-10
        for i, member in enumerate(members[:10], 1):
            username = member['username']
            today_count = member['today']
            total_count = member['total']
            
            # Эмодзи для топа
            if i == 1:
                emoji = "👑"
            elif i == 2:
                emoji = "🥈"
            elif i == 3:
                emoji = "🥉"
            else:
                emoji = f"{i}."
            
            text += f"<b>{emoji} {username}:</b>\n"
            text += f"   📅 Сегодня: {today_count} | 📊 Всего: {total_count}\n\n"
        
        # Общая статистика
        total_today = sum(m['today'] for m in members)
        total_all = sum(m['total'] for m in members)
        
        text += f"<b>📈 Итого по чату:</b>\n"
        text += f"📅 Сегодня: <b>{total_today}</b> сообщ.\n"
        text += f"📊 Всего: <b>{total_all}</b> сообщ."
        
        await message.reply(text)
        
    except Exception as e:
        logger.error(f"Ошибка в /top: {e}")
        await message.reply("⚠️ Произошла ошибка при получении топа.")

async def handle_mystats(message: types.Message):
    """Показать личную статистику пользователя"""
    if is_shutting_down:
        return
    
    try:
        user_id = message.from_user.id
        user_stats = StatsService.get_user_stats(user_id)
        
        if user_stats:
            moscow_time = get_moscow_time()
            
            # Форматируем дату первого сообщения
            first_seen = user_stats['first_seen']
            if first_seen:
                try:
                    if isinstance(first_seen, str):
                        first_seen_date = datetime.fromisoformat(first_seen.split('.')[0])
                    else:
                        first_seen_date = first_seen
                    first_seen_str = first_seen_date.strftime('%d.%m.%Y')
                except:
                    first_seen_str = "неизвестно"
            else:
                first_seen_str = "недавно"
            
            text = f"<b>📊 Ваша статистика</b>\n\n"
            text += f"👤 <b>{user_stats['username']}</b>\n"
            text += f"📅 <b>Сегодня:</b> {user_stats['today']} сообщений\n"
            text += f"🗓️ <b>Вчера:</b> {user_stats['yesterday']} сообщений\n"
            text += f"📊 <b>Всего:</b> {user_stats['total']} сообщений\n"
            text += f"📅 <b>С нами с:</b> {first_seen_str}\n"
            text += f"🕐 <b>Московское время:</b> {format_time(moscow_time)}"
            
            await message.reply(text)
        else:
            await message.reply("📊 У вас еще нет статистики. Напишите что-нибудь в чате!")
            
    except Exception as e:
        logger.error(f"Ошибка в /mystats: {e}")
        await message.reply("⚠️ Произошла ошибка при получении статистики.")

async def handle_yesterday(message: types.Message):
    """Показать топ за вчера"""
    if is_shutting_down:
        return
    
    try:
        cursor = get_db_cursor()
        
        # Получаем статистику за вчера
        cursor.execute("""
            SELECT username, yesterday
            FROM messages 
            WHERE yesterday > 0 
            ORDER BY yesterday DESC 
            LIMIT 10
        """)
        
        rows = cursor.fetchall()
        
        if not rows:
            await message.reply("📊 Вчера не было сообщений или статистика не собрана.")
            return
        
        moscow_time = get_moscow_time()
        text = f"<b>📊 Топ за вчера</b>\n"
        text += f"<i>Московское время: {format_time(moscow_time)}</i>\n\n"
        
        for i, (username, count) in enumerate(rows, 1):
            if i == 1:
                emoji = "👑"
            elif i == 2:
                emoji = "🥈"
            elif i == 3:
                emoji = "🥉"
            else:
                emoji = f"{i}."
            
            text += f"{emoji} <b>{username}:</b> {count} сообщ.\n"
        
        # Общая статистика за вчера
        cursor.execute("SELECT SUM(yesterday) FROM messages")
        total_yesterday = cursor.fetchone()[0] or 0
        
        text += f"\n<b>📈 Итого за вчера:</b> {total_yesterday} сообщений"
        
        await message.reply(text)
        
    except Exception as e:
        logger.error(f"Ошибка в /yesterday: {e}")
        await message.reply("⚠️ Произошла ошибка при получении статистики.")

async def handle_weekly(message: types.Message):
    """Показать статистику за неделю"""
    if is_shutting_down:
        return
    
    try:
        cursor = get_db_cursor()
        
        # Получаем статистику за последние 7 дней
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
        
        moscow_time = get_moscow_time()
        text = f"<b>📅 Статистика за неделю</b>\n"
        text += f"<i>Московское время: {format_time(moscow_time)}</i>\n\n"
        
        total_messages_week = 0
        total_active_week = 0
        
        for date_str, total_messages, active_users in rows:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            text += f"<b>{date_obj.strftime('%d.%m')}:</b> {total_messages} сообщ. от {active_users} чел.\n"
            total_messages_week += total_messages
            total_active_week += active_users
        
        # Если есть дни без данных, добавляем информацию
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
        logger.error(f"Ошибка в /weekly: {e}")
        await message.reply("⚠️ Произошла ошибка при получении недельной статистики.")

async def handle_reset_today(message: types.Message):
    """Сбросить счетчики на сегодня (для администраторов)"""
    if is_shutting_down:
        return
    
    try:
        chat_type = message.chat.type
        
        if chat_type == ChatType.CHANNEL:
            await message.reply("⚠️ В каналах эта команда недоступна.")
            return
        
        if chat_type == ChatType.PRIVATE:
            await message.reply("ℹ️ В личных чатах используйте команду /mystats.")
            return
        
        # Для групп проверяем права администратора
        chat_id = message.chat.id
        
        try:
            chat_admins = await bot_instance.get_chat_administrators(chat_id)
            admin_ids = [admin.user.id for admin in chat_admins]
            
            if message.from_user.id not in admin_ids:
                await message.reply("⚠️ Эта команда доступна только администраторам.")
                return
        except Exception as e:
            logger.error(f"Ошибка проверки прав администратора: {e}")
            await message.reply("⚠️ Не удалось проверить права администратора.")
            return
        
        # Сохраняем данные за сегодня перед сбросом
        cursor = get_db_cursor()
        cursor.execute("SELECT SUM(today) FROM messages")
        total_today = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT COUNT(*) FROM messages WHERE today > 0")
        active_today = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT user_id, today FROM messages WHERE today > 0 ORDER BY today DESC LIMIT 1")
        top_user = cursor.fetchone()
        
        # Сохраняем в историю
        today_date = get_moscow_time().strftime('%Y-%m-%d')
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
        db_conn.commit()
        
        # Очищаем кэш
        StatsService.clear_chat_cache(chat_id)
        
        moscow_time = get_moscow_time()
        await message.reply(
            f"✅ Счетчики сообщений сброшены.\n"
            f"📊 Сегодня было: {total_today} сообщений от {active_today} пользователей\n"
            f"🕐 Московское время: {format_time(moscow_time)}"
        )
        
    except Exception as e:
        logger.error(f"Ошибка в /reset_today: {e}")
        await message.reply("⚠️ Произошла ошибка при сбросе счетчиков.")

async def handle_status(message: types.Message):
    """Показать общую статистику чата"""
    if is_shutting_down:
        return
    
    try:
        chat_id = message.chat.id
        chat_type = message.chat.type
        
        if chat_type == ChatType.CHANNEL:
            await message.reply("⚠️ В каналах статистика не собирается.")
            return
        
        # Получаем отсортированный список участников
        members = await StatsService.get_sorted_members(chat_id)
        
        if not members:
            await message.reply("📊 Пока нет статистики сообщений в этом чате.")
            return
        
        moscow_time = get_moscow_time()
        
        if chat_type == ChatType.PRIVATE:
            if members:
                user_stats = members[0]
                text = f"<b>📊 Ваша статистика</b>\n\n"
                text += f"👤 <b>{user_stats['username']}</b>\n"
                text += f"📅 <b>Сегодня:</b> {user_stats['today']} сообщений\n"
                text += f"🗓️ <b>Вчера:</b> {user_stats['yesterday']} сообщений\n"
                text += f"📊 <b>Всего:</b> {user_stats['total']} сообщений\n"
                text += f"🕐 <b>Московское время:</b> {format_time(moscow_time)}"
            else:
                text = "📊 Пока нет статистики сообщений."
        else:
            # Для групп
            text = f"<b>📊 Статистика чата</b>\n"
            text += f"<i>Московское время: {format_time(moscow_time)}</i>\n\n"
            
            # Показываем топ-5
            for i, member in enumerate(members[:5], 1):
                username = member['username']
                today_count = member['today']
                total_count = member['total']
                
                if i == 1:
                    emoji = "👑"
                elif i == 2:
                    emoji = "🥈"
                elif i == 3:
                    emoji = "🥉"
                else:
                    emoji = f"{i}."
                
                text += f"<b>{emoji} {username}:</b>\n"
                text += f"   📅 Сегодня: {today_count} | 📊 Всего: {total_count}\n\n"
        
        await message.reply(text)
        
    except Exception as e:
        logger.error(f"Ошибка в /status: {e}")
        await message.reply("⚠️ Произошла ошибка при получении статистики.")

# ==================== ОБРАБОТКА СООБЩЕНИЙ ====================
async def count_messages(message: types.Message):
    """Подсчет всех текстовых сообщений"""
    if is_shutting_down:
        return
    
    if not message.from_user or message.from_user.is_bot:
        return
    
    chat_type = message.chat.type
    if chat_type == ChatType.CHANNEL:
        return
    
    user_id = message.from_user.id
    username = message.from_user.full_name
    chat_id = message.chat.id
    
    # Обновляем статистику
    StatsService.update_user_message(user_id, username, chat_id)

# ==================== ПЛАНИРОВЩИК ЗАДАЧ ====================
async def daily_report():
    """Ежедневный отчет"""
    if is_shutting_down:
        return
    
    try:
        moscow_time = get_moscow_time()
        logger.info(f"Генерация ежедневного отчета в {format_time(moscow_time)}")
        
        # Здесь можно добавить отправку отчета в чаты
        # Пока просто логируем и сохраняем статистику
        cursor = get_db_cursor()
        cursor.execute("SELECT SUM(today) FROM messages")
        total_today = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT COUNT(*) FROM messages WHERE today > 0")
        active_today = cursor.fetchone()[0] or 0
        
        # Сохраняем статистику дня
        today_date = moscow_time.strftime('%Y-%m-%d')
        cursor.execute("""
            INSERT OR REPLACE INTO daily_stats 
            (date, total_messages, active_users)
            VALUES (?, ?, ?)
        """, (today_date, total_today, active_today))
        
        db_conn.commit()
        logger.info(f"Ежедневный отчет сохранен: {total_today} сообщений от {active_today} пользователей")
        
    except Exception as e:
        logger.error(f"Ошибка в daily_report: {e}")

async def auto_reset_counters():
    """Автоматический сброс счетчиков в полночь"""
    if is_shutting_down:
        return
    
    try:
        moscow_time = get_moscow_time()
        logger.info(f"Автосброс счетчиков в {format_time(moscow_time)}")
        
        cursor = get_db_cursor()
        
        # Сохраняем статистику перед сбросом
        cursor.execute("SELECT SUM(today) FROM messages")
        total_today = cursor.fetchone()[0] or 0
        
        if total_today > 0:
            cursor.execute("SELECT COUNT(*) FROM messages WHERE today > 0")
            active_today = cursor.fetchone()[0] or 0
            
            cursor.execute("SELECT user_id, today FROM messages WHERE today > 0 ORDER BY today DESC LIMIT 1")
            top_user = cursor.fetchone()
            
            today_date = moscow_time.strftime('%Y-%m-%d')
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
        db_conn.commit()
        
        # Очищаем кэш
        global user_cache
        user_cache.clear()
        
        logger.info("Счетчики успешно сброшены")
        
    except Exception as e:
        logger.error(f"Ошибка в auto_reset_counters: {e}")

async def auto_save_stats():
    """Автосохранение статистики каждый час"""
    if is_shutting_down:
        return
    
    try:
        cursor = get_db_cursor()
        cursor.execute("SELECT SUM(today) FROM messages")
        total_today = cursor.fetchone()[0] or 0
        
        if total_today > 0:
            cursor.execute("SELECT COUNT(*) FROM messages WHERE today > 0")
            active_today = cursor.fetchone()[0] or 0
            
            moscow_time = get_moscow_time()
            today_date = moscow_time.strftime('%Y-%m-%d')
            
            cursor.execute("""
                INSERT OR REPLACE INTO daily_stats 
                (date, total_messages, active_users)
                VALUES (?, ?, ?)
            """, (today_date, total_today, active_today))
            
            db_conn.commit()
            logger.debug(f"Статистика автосохранена: {total_today} сообщений")
            
    except Exception as e:
        logger.error(f"Ошибка в auto_save_stats: {e}")

def create_scheduler():
    """Создать и настроить планировщик задач"""
    scheduler = AsyncIOScheduler(timezone=MOSCOW_TZ)
    
    # Ежедневный отчет в 23:59 по Москве
    scheduler.add_job(
        daily_report,
        "cron",
        hour=23,
        minute=59,
        misfire_grace_time=60
    )
    
    # Автосброс в полночь по Москве
    scheduler.add_job(
        auto_reset_counters,
        "cron",
        hour=0,
        minute=0,
        misfire_grace_time=60
    )
    
    # Автосохранение каждый час
    scheduler.add_job(
        auto_save_stats,
        "cron",
        hour="*",
        misfire_grace_time=60
    )
    
    return scheduler

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
        if dp_instance:
            await dp_instance.stop_polling()
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
        if db_conn:
            db_conn.close()
            logger.info("Соединение с БД закрыто")
    except Exception as e:
        logger.error(f"Ошибка при закрытии БД: {e}")
    
    logger.info("Завершение работы завершено")
    await asyncio.sleep(1)

def signal_handler(signum, frame):
    """Обработчик сигналов для graceful shutdown"""
    logger.info(f"Получен сигнал {signum}, инициируем shutdown...")
    asyncio.create_task(shutdown())

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
async def main():
    global bot_instance, dp_instance, scheduler_instance, polling_task
    
    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Инициализация базы данных
    init_database()
    
    # Запуск HTTP-сервера в отдельном потоке
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()
    logger.info(f"HTTP сервер запущен на порту {HTTP_PORT}")
    
    # Создание бота
    bot_instance = Bot(
        token=API_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    
    # Создание диспетчера
    dp_instance = Dispatcher()
    
    # Регистрация обработчиков команд
    dp_instance.message.register(handle_start, Command("start"))
    dp_instance.message.register(handle_help, Command("help"))
    dp_instance.message.register(handle_top, Command("top"))
    dp_instance.message.register(handle_mystats, Command("mystats"))
    dp_instance.message.register(handle_yesterday, Command("yesterday"))
    dp_instance.message.register(handle_weekly, Command("weekly"))
    dp_instance.message.register(handle_reset_today, Command("
