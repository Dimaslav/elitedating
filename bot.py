import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
from html import escape
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

import aiosqlite
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    ErrorEvent,
    InlineKeyboardButton,
    Message,
    TelegramObject,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "0")

try: ADMIN_ID = int(ADMIN_ID_RAW)
except ValueError as exc: raise RuntimeError("ADMIN_ID должен быть числом.") from exc

if not BOT_TOKEN: raise RuntimeError("BOT_TOKEN не настроен в .env")
if ADMIN_ID <= 0: raise RuntimeError("ADMIN_ID не настроен.")

DB_NAME = os.getenv("DB_NAME", "dating_bot.db")
RULES_VERSION = 1

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

db: Optional[aiosqlite.Connection] = None
db_lock = asyncio.Lock()

# ============================================================
# КОНСТАНТЫ И ВАЛИДАЦИЯ
# ============================================================
MIN_AGE, MAX_AGE = 18, 99
MIN_NAME_LENGTH, MAX_NAME_LENGTH = 2, 50
MIN_CITY_LENGTH, MAX_CITY_LENGTH = 2, 50
MIN_BIO_LENGTH, MAX_BIO_LENGTH = 10, 500
MAX_REPORT_LENGTH = 500

VALID_GENDERS = {"male": "Парень", "female": "Девушка"}
VALID_SEARCH_GENDERS = {"male": "Парней", "female": "Девушек", "all": "Всех"}
VALID_MODES = {"local", "global", "likes"}
ALLOWED_USER_FIELDS = {"name", "age", "gender", "search_gender", "city", "bio", "photo_id", "username", "is_active"}
KNOWN_COMMANDS = {"/start", "/delete", "/help", "/stats"}

RULES_TEXT = (
    "⚠️ <b>Правила:</b>\n"
    "1. Сервис предназначен только для пользователей 18+.\n"
    "2. Запрещены оскорбления, мошенничество и спам.\n"
    "3. Уважайте личные границы и приватность других людей.\n"
    "4. Не публикуйте чужие фотографии и личные данные.\n\n"
    "Продолжая, вы подтверждаете, что вам исполнилось 18 лет и вы принимаете правила сервиса."
)

class LikeResult(Enum):
    LIKED = 1
    MATCHED = 2
    REJECTED = 3

def normalize_text(value: str) -> str: return " ".join(value.strip().split())
def validate_text(value: Optional[str], min_length: int, max_length: int) -> Optional[str]:
    if not value: return None
    normalized = normalize_text(value)
    return normalized if min_length <= len(normalized) <= max_length else None

def validate_age(value: Optional[str]) -> Optional[int]:
    if not value or not value.strip().isdigit(): return None
    age = int(value.strip())
    return age if MIN_AGE <= age <= MAX_AGE else None

def gender_text(code: str) -> Optional[str]: return VALID_GENDERS.get(code)
def search_gender_text(code: str) -> Optional[str]: return VALID_SEARCH_GENDERS.get(code)

def parse_profile_callback(data: Optional[str], expected_action: str) -> Optional[Tuple[int, str]]:
    if not data: return None
    try:
        action, raw_id, mode = data.split(":")
        p_id = int(raw_id)
    except (ValueError, TypeError): return None
    if action != expected_action or p_id <= 0 or mode not in VALID_MODES: return None
    return p_id, mode

# ============================================================
# PER-USER ACTION LOCKS (защита от двойного тапа)
# ============================================================
user_action_locks: Dict[int, asyncio.Lock] = {}

def get_user_lock(user_id: int) -> asyncio.Lock:
    lock = user_action_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        user_action_locks[user_id] = lock
    if len(user_action_locks) > 10000:
        for uid, l in list(user_action_locks.items()):
            if not l.locked() and uid != user_id:
                del user_action_locks[uid]
    return lock

# ============================================================
# FSM
# ============================================================
class Registration(StatesGroup):
    agreement, name, age, gender, search_gender, city, bio, photo = State(), State(), State(), State(), State(), State(), State(), State()

class EditProfile(StatesGroup):
    name, age, search_gender, city, bio, photo = State(), State(), State(), State(), State(), State()

class Report(StatesGroup):
    reason = State()

# ============================================================
# БАЗА ДАННЫХ И ТРАНЗАКЦИИ
# ============================================================
@asynccontextmanager
async def transaction():
    async with db_lock:
        try:
            await db.execute("BEGIN IMMEDIATE")
            yield
            await db.commit()
        except Exception:
            await db.rollback()
            raise

async def init_db() -> None:
    global db
    db = await aiosqlite.connect(DB_NAME)
    db.row_factory = aiosqlite.Row
    
    await db.execute("PRAGMA foreign_keys = ON")
    await db.execute("PRAGMA journal_mode = WAL")
    await db.execute("PRAGMA busy_timeout = 5000")

    await db.execute("""CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER NOT NULL CHECK(age BETWEEN 18 AND 99),
        gender TEXT NOT NULL CHECK(gender IN ('Парень', 'Девушка')),
        search_gender TEXT NOT NULL CHECK(search_gender IN ('Парней', 'Девушек', 'Всех')),
        city TEXT NOT NULL, bio TEXT NOT NULL, photo_id TEXT NOT NULL, username TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP, accepted_rules_at DATETIME,
        is_active INTEGER NOT NULL DEFAULT 1, accepted_rules_version INTEGER)""")

    await db.execute("""CREATE TABLE IF NOT EXISTS views (
        viewer_id INTEGER NOT NULL, viewed_id INTEGER NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (viewer_id, viewed_id), CHECK (viewer_id != viewed_id),
        FOREIGN KEY (viewer_id) REFERENCES users(user_id) ON DELETE CASCADE,
        FOREIGN KEY (viewed_id) REFERENCES users(user_id) ON DELETE CASCADE)""")

    await db.execute("""CREATE TABLE IF NOT EXISTS likes (
        liker_id INTEGER NOT NULL, liked_id INTEGER NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (liker_id, liked_id), CHECK (liker_id != liked_id),
        FOREIGN KEY (liker_id) REFERENCES users(user_id) ON DELETE CASCADE,
        FOREIGN KEY (liked_id) REFERENCES users(user_id) ON DELETE CASCADE)""")

    await db.execute("""CREATE TABLE IF NOT EXISTS matches (
        user1_id INTEGER NOT NULL, user2_id INTEGER NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (user1_id, user2_id), CHECK (user1_id < user2_id),
        FOREIGN KEY (user1_id) REFERENCES users(user_id) ON DELETE CASCADE,
        FOREIGN KEY (user2_id) REFERENCES users(user_id) ON DELETE CASCADE)""")

    await db.execute("""CREATE TABLE IF NOT EXISTS reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT, reporter_id INTEGER NOT NULL, reported_id INTEGER NOT NULL,
        reason TEXT NOT NULL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'accepted', 'rejected')),
        reviewed_by INTEGER, reviewed_at DATETIME, resolution TEXT,
        CHECK(reporter_id != reported_id),
        FOREIGN KEY (reporter_id) REFERENCES users(user_id) ON DELETE CASCADE,
        FOREIGN KEY (reported_id) REFERENCES users(user_id) ON DELETE CASCADE)""")

    await db.execute("""CREATE TABLE IF NOT EXISTS bans (
        user_id INTEGER PRIMARY KEY, reason TEXT NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)""")

    await db.execute("""CREATE TABLE IF NOT EXISTS blocks (
        blocker_id INTEGER NOT NULL, blocked_id INTEGER NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (blocker_id, blocked_id), CHECK (blocker_id != blocked_id),
        FOREIGN KEY (blocker_id) REFERENCES users(user_id) ON DELETE CASCADE,
        FOREIGN KEY (blocked_id) REFERENCES users(user_id) ON DELETE CASCADE)""")

    await db.execute("CREATE INDEX IF NOT EXISTS idx_users_gender_city ON users(gender, city)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_likes_liked ON likes(liked_id)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_blocks_blocked ON blocks(blocked_id)")
    await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_reports_pending_unique ON reports(reporter_id, reported_id) WHERE status = 'pending'")
    await db.commit()
    
    await migrate_db()
    logger.info("База данных успешно инициализирована.")

async def migrate_db():
    async with db.execute("PRAGMA user_version") as cursor:
        version = (await cursor.fetchone())[0]
        
    if version < 1:
        async with db.execute("PRAGMA table_info(reports)") as cursor:
            columns = [row[1] for row in await cursor.fetchall()]
        if "status" not in columns:
            await db.execute("ALTER TABLE reports ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'")
            await db.execute("ALTER TABLE reports ADD COLUMN reviewed_by INTEGER")
            await db.execute("ALTER TABLE reports ADD COLUMN reviewed_at DATETIME")
            await db.execute("ALTER TABLE reports ADD COLUMN resolution TEXT")
        await db.execute("DROP INDEX IF EXISTS idx_reports_unique")
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_reports_pending_unique ON reports(reporter_id, reported_id) WHERE status = 'pending'")
        await db.execute("PRAGMA user_version = 1")
        
    if version < 2:
        async with db.execute("PRAGMA table_info(users)") as cursor:
            columns = [row[1] for row in await cursor.fetchall()]
        if "is_active" not in columns:
            await db.execute("ALTER TABLE users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
        if "accepted_rules_version" not in columns:
            await db.execute("ALTER TABLE users ADD COLUMN accepted_rules_version INTEGER")
        await db.execute("PRAGMA user_version = 2")
        
    await db.commit()

async def check_ban(user_id: int) -> bool:
    async with db_lock:
        async with db.execute("SELECT 1 FROM bans WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone() is not None

async def get_user(user_id: int) -> Optional[aiosqlite.Row]:
    async with db_lock:
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def add_user_to_db(user_id: int, name: str, age: int, gender: str, search_gender: str, city: str, bio: str, photo_id: str, username: Optional[str], accepted_rules_at: str) -> None:
    async with transaction():
        await db.execute("""INSERT INTO users (user_id, name, age, gender, search_gender, city, bio, photo_id, username, accepted_rules_at, is_active, accepted_rules_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT(user_id) DO UPDATE SET name=excluded.name, age=excluded.age, gender=excluded.gender,
            search_gender=excluded.search_gender, city=excluded.city, bio=excluded.bio, photo_id=excluded.photo_id,
            username=excluded.username, accepted_rules_at=excluded.accepted_rules_at, is_active=1, accepted_rules_version=excluded.accepted_rules_version""",
            (user_id, name, age, gender, search_gender, city, bio, photo_id, username, accepted_rules_at, RULES_VERSION))

async def update_user_field(user_id: int, field: str, value: Any) -> None:
    if field not in ALLOWED_USER_FIELDS: raise ValueError(f"Недопустимое поле: {field}")
    async with db_lock:
        await db.execute(f"UPDATE users SET {field} = ? WHERE user_id = ?", (value, user_id))
        await db.commit()

async def set_rules_accepted(user_id: int, accepted_at: str) -> None:
    async with db_lock:
        await db.execute("UPDATE users SET accepted_rules_at = ?, accepted_rules_version = ? WHERE user_id = ?",
            (accepted_at, RULES_VERSION, user_id))
        await db.commit()

# Не удаляем бан при удалении анкеты
async def delete_user(user_id: int) -> None:
    async with transaction():
        await db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))

async def get_likes_count(user_id: int) -> int:
    async with db_lock:
        async with db.execute("""SELECT COUNT(*) FROM likes AS l JOIN users AS u ON u.user_id = l.liker_id
            WHERE l.liked_id = ? AND l.liker_id NOT IN (SELECT viewed_id FROM views WHERE viewer_id = ?)
            AND l.liker_id NOT IN (SELECT blocked_id FROM blocks WHERE blocker_id = ?)
            AND l.liker_id NOT IN (SELECT blocker_id FROM blocks WHERE blocked_id = ?)
            AND l.liker_id NOT IN (SELECT user_id FROM bans) AND u.is_active = 1""", (user_id, user_id, user_id, user_id)) as cursor:
            return (await cursor.fetchone())[0]

async def are_users_blocked(u1: int, u2: int) -> bool:
    async with db_lock:
        async with db.execute("SELECT 1 FROM blocks WHERE (blocker_id=? AND blocked_id=?) OR (blocker_id=? AND blocked_id=?)", (u1, u2, u2, u1)) as cursor:
            return await cursor.fetchone() is not None

async def add_view(viewer_id: int, viewed_id: int) -> None:
    if viewer_id == viewed_id: return
    async with db_lock:
        await db.execute("INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)", (viewer_id, viewed_id))
        await db.commit()

# Lifecycle лайков
async def add_like(liker_id: int, liked_id: int) -> LikeResult:
    if liker_id == liked_id: return LikeResult.REJECTED
    u1, u2 = min(liker_id, liked_id), max(liker_id, liked_id)
    
    async with transaction():
        async with db.execute("SELECT 1 FROM bans WHERE user_id IN (?, ?) LIMIT 1", (liker_id, liked_id)) as cur:
            if await cur.fetchone(): return LikeResult.REJECTED
        async with db.execute("SELECT 1 FROM blocks WHERE (blocker_id=? AND blocked_id=?) OR (blocker_id=? AND blocked_id=?)", (liker_id, liked_id, liked_id, liker_id)) as cur:
            if await cur.fetchone(): return LikeResult.REJECTED

        await db.execute("INSERT OR IGNORE INTO likes (liker_id, liked_id) VALUES (?, ?)", (liker_id, liked_id))
        await db.execute("INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)", (liker_id, liked_id))
        
        async with db.execute("SELECT 1 FROM likes WHERE liker_id=? AND liked_id=?", (liked_id, liker_id)) as cur:
            mutual = await cur.fetchone() is not None

        if not mutual:
            return LikeResult.LIKED

        # Взаимный мэтч: удаляем лайки, добавляем просмотры, создаем мэтч
        await db.execute("DELETE FROM likes WHERE (liker_id=? AND liked_id=?) OR (liker_id=? AND liked_id=?)", (liker_id, liked_id, liked_id, liker_id))
        await db.execute("INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)", (liked_id, liker_id))
        
        cursor = await db.execute("INSERT OR IGNORE INTO matches (user1_id, user2_id) VALUES (?, ?)", (u1, u2))
        return LikeResult.MATCHED if cursor.rowcount > 0 else LikeResult.LIKED

async def reject_like(user_id: int, liker_id: int) -> None:
    async with transaction():
        await db.execute("DELETE FROM likes WHERE liker_id = ? AND liked_id = ?", (liker_id, user_id))
        await db.execute("INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)", (user_id, liker_id))

async def block_user(blocker_id: int, blocked_id: int) -> None:
    if blocker_id == blocked_id: return
    u1, u2 = min(blocker_id, blocked_id), max(blocker_id, blocked_id)
    async with transaction():
        await db.execute("INSERT OR IGNORE INTO blocks (blocker_id, blocked_id) VALUES (?, ?)", (blocker_id, blocked_id))
        await db.execute("DELETE FROM likes WHERE (liker_id=? AND liked_id=?) OR (liker_id=? AND liked_id=?)", (blocker_id, blocked_id, blocked_id, blocker_id))
        await db.execute("DELETE FROM matches WHERE user1_id=? AND user2_id=?", (u1, u2))
        await db.execute("INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)", (blocker_id, blocked_id))

async def report_exists(reporter_id: int, reported_id: int) -> bool:
    async with db_lock:
        async with db.execute("SELECT 1 FROM reports WHERE reporter_id=? AND reported_id=? AND status='pending' LIMIT 1", (reporter_id, reported_id)) as cur:
            return await cur.fetchone() is not None

async def add_report(reporter_id: int, reported_id: int, reason: str) -> Optional[int]:
    async with db_lock:
        cursor = await db.execute("INSERT OR IGNORE INTO reports (reporter_id, reported_id, reason) VALUES (?, ?, ?)", (reporter_id, reported_id, reason))
        await db.commit()
        return cursor.lastrowid if cursor.rowcount > 0 else None

async def ban_user(user_id: int, reason: str, admin_id: Optional[int] = None, report_id: Optional[int] = None) -> None:
    async with transaction():
        await db.execute("INSERT INTO bans (user_id, reason) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET reason=excluded.reason", (user_id, reason))
        await db.execute("DELETE FROM likes WHERE liker_id=? OR liked_id=?", (user_id, user_id))
        await db.execute("DELETE FROM matches WHERE user1_id=? OR user2_id=?", (user_id, user_id))
        if admin_id and report_id:
            await db.execute("UPDATE reports SET status = 'accepted', reviewed_by = ?, reviewed_at = CURRENT_TIMESTAMP WHERE id = ?", (admin_id, report_id))

# Учет взаимных предпочтений
async def get_random_profile(user_id: int, user_gender: str, search_gender: str, user_city: Optional[str] = None, strict_city: bool = False) -> Optional[aiosqlite.Row]:
    query = """SELECT * FROM users 
        WHERE user_id != ? AND is_active = 1
        AND user_id NOT IN (SELECT viewed_id FROM views WHERE viewer_id = ?)
        AND user_id NOT IN (SELECT blocked_id FROM blocks WHERE blocker_id = ?) 
        AND user_id NOT IN (SELECT blocker_id FROM blocks WHERE blocked_id = ?)
        AND user_id NOT IN (SELECT user_id FROM bans)"""
    params = [user_id, user_id, user_id, user_id]
    
    if search_gender != "Всех":
        g_filter = {"Парней": "Парень", "Девушек": "Девушка"}.get(search_gender)
        if g_filter: 
            query += " AND gender = ?"
            params.append(g_filter)
            
    query += """ AND (
        search_gender = 'Всех'
        OR (search_gender = 'Парней' AND ? = 'Парень')
        OR (search_gender = 'Девушек' AND ? = 'Девушка')
    )"""
    params.extend([user_gender, user_gender])
            
    if strict_city:
        if user_city:
            query += " AND lower(city) = lower(?)"
            params.append(user_city)
        query += " ORDER BY RANDOM() LIMIT 1"
    else:
        if user_city:
            query += " ORDER BY CASE WHEN lower(city) = lower(?) THEN 0 ELSE 1 END, RANDOM() LIMIT 1"
            params.append(user_city)
        else:
            query += " ORDER BY RANDOM() LIMIT 1"
            
    async with db_lock:
        async with db.execute(query, tuple(params)) as cursor:
            return await cursor.fetchone()

async def get_next_liker(user_id: int) -> Optional[aiosqlite.Row]:
    async with db_lock:
        async with db.execute("""SELECT u.* FROM users AS u JOIN likes AS l ON u.user_id = l.liker_id WHERE l.liked_id = ?
            AND u.user_id NOT IN (SELECT viewed_id FROM views WHERE viewer_id = ?) AND u.user_id NOT IN (SELECT blocked_id FROM blocks WHERE blocker_id = ?)
            AND u.user_id NOT IN (SELECT blocker_id FROM blocks WHERE blocked_id = ?) AND u.user_id NOT IN (SELECT user_id FROM bans) AND u.is_active = 1
            ORDER BY l.created_at DESC LIMIT 1""", (user_id, user_id, user_id, user_id)) as cursor:
            return await cursor.fetchone()

async def verify_like_exists(liker_id: int, liked_id: int) -> bool:
    async with db_lock:
        async with db.execute("SELECT 1 FROM likes WHERE liker_id=? AND liked_id=?", (liker_id, liked_id)) as cur:
            return await cur.fetchone() is not None

async def get_matches(user_id: int) -> list:
    async with db_lock:
        async with db.execute("""SELECT u.user_id, u.name, m.created_at FROM matches m
            JOIN users u ON u.user_id = CASE WHEN m.user1_id = ? THEN m.user2_id ELSE m.user1_id END
            WHERE (m.user1_id = ? OR m.user2_id = ?) AND u.user_id != ? AND u.is_active = 1
            AND u.user_id NOT IN (SELECT user_id FROM bans)
            ORDER BY m.created_at DESC LIMIT 50""", (user_id, user_id, user_id, user_id)) as cursor:
            return await cursor.fetchall()

async def get_stats() -> Dict[str, int]:
    async with db_lock:
        async with db.execute("""SELECT
            (SELECT COUNT(*) FROM users) AS users_total,
            (SELECT COUNT(*) FROM users WHERE is_active = 1) AS users_active,
            (SELECT COUNT(*) FROM likes) AS likes,
            (SELECT COUNT(*) FROM matches) AS matches,
            (SELECT COUNT(*) FROM views) AS views,
            (SELECT COUNT(*) FROM blocks) AS blocks,
            (SELECT COUNT(*) FROM bans) AS bans,
            (SELECT COUNT(*) FROM reports WHERE status = 'pending') AS reports_pending""") as cursor:
            return dict(await cursor.fetchone())

# ============================================================
# MIDDLEWARE
# ============================================================
class SecurityMiddleware(BaseMiddleware):
    def __init__(self, cooldown: float = 0.5):
        self.last_action = {}
        self.cooldown = cooldown

    async def __call__(self, handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]], event: TelegramObject, data: Dict[str, Any]) -> Any:
        user = data.get("event_from_user")
        if not user: return await handler(event, data)

        first_word: Optional[str] = None
        if isinstance(event, Message) and event.text:
            first_word = event.text.split()[0].split("@")[0]

        if await check_ban(user.id):
            if first_word == "/delete":
                return await handler(event, data)
            if isinstance(event, Message): await event.answer("Вы заблокированы. Для удаления данных используйте /delete.")
            elif isinstance(event, CallbackQuery): await event.answer("Вы заблокированы.", show_alert=True)
            return

        if first_word in KNOWN_COMMANDS:
            return await handler(event, data)

        now = time.monotonic()
        if now - self.last_action.get(user.id, 0) < self.cooldown:
            if isinstance(event, CallbackQuery): await event.answer("Не так быстро. Подождите немного.")
            elif isinstance(event, Message): await event.answer("Не так быстро. Подождите немного.")
            return

        self.last_action[user.id] = now
        if len(self.last_action) > 10000: self.last_action = {k: v for k, v in self.last_action.items() if now - v < 3600}
        return await handler(event, data)

security_mw = SecurityMiddleware()
router.message.middleware(security_mw)
router.callback_query.middleware(security_mw)

# ============================================================
# КЛАВИАТУРЫ И ХЕЛПЕРЫ
# ============================================================
def format_profile(profile: aiosqlite.Row, prefix: str = "") -> str:
    return (f"{prefix}👤 <b>{escape(profile['name'])}</b>, {profile['age']} ({escape(profile['gender'])})\n"
            f"📍 {escape(profile['city'])}\n\n📝 {escape(profile['bio'])}")

# html-контент сообщения: у фото-сообщений текст лежит в caption, а не в text
def extract_html(message: Message) -> str:
    if message.photo:
        caption = getattr(message, "html_caption", None)
        return caption if caption else escape(message.caption or "")
    text = getattr(message, "html_text", None)
    return text if text else escape(message.text or "")

async def safe_edit_or_send(callback: CallbackQuery, text: str, reply_markup=None):
    if callback.message.photo:
        try: await callback.message.delete()
        except TelegramBadRequest: pass
        await callback.message.answer(text, reply_markup=reply_markup)
    else:
        try: await callback.message.edit_text(text, reply_markup=reply_markup)
        except TelegramBadRequest: await callback.message.answer(text, reply_markup=reply_markup)

async def send_menu(message: Message, user_id: int, text: str = "Главное меню:") -> None:
    await message.answer(text, reply_markup=menu_keyboard(await get_likes_count(user_id)))

def profile_card_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Редактировать", callback_data="edit_profile")
    builder.button(text="🔙 В меню", callback_data="menu")
    builder.adjust(2)
    return builder.as_markup()

async def send_profile_card(message: Message, user: aiosqlite.Row) -> None:
    status = "✅ Активна" if user["is_active"] else "❌ Неактивна (обновите фото)"
    text = (f"👤 <b>{escape(user['name'])}</b>, {user['age']} ({escape(user['gender'])})\n"
            f"🔍 Ищу: {escape(user['search_gender'])}\n📍 {escape(user['city'])}\n\n📝 {escape(user['bio'])}\n\n"
            f"💌 Новых лайков: {await get_likes_count(user['user_id'])}\nСтатус: {status}")
    try:
        await message.answer_photo(photo=user["photo_id"], caption=text, reply_markup=profile_card_keyboard())
    except TelegramBadRequest:
        await message.answer("Не удалось загрузить фото. Обновите его в разделе редактирования.", reply_markup=profile_card_keyboard())

async def show_profile(callback: CallbackQuery, state: FSMContext, mode: str = "global") -> None:
    user = await get_user(callback.from_user.id)
    if not user or not user["is_active"]:
        await safe_edit_or_send(callback, "Ваша анкета неактивна. Обновите фото через 'Моя анкета' -> 'Редактировать'.", reply_markup=back_to_menu_keyboard())
        return

    while True:
        strict = (mode == "local")
        profile = await get_random_profile(
            user_id=callback.from_user.id, 
            user_gender=user["gender"],
            search_gender=user["search_gender"], 
            user_city=user["city"], 
            strict_city=strict
        )
        if not profile:
            await safe_edit_or_send(callback, "Подходящие анкеты закончились.\nМожно просмотреть их заново или заглянуть позже.", reply_markup=no_profiles_keyboard())
            return
        try:
            msg = await callback.message.answer_photo(photo=profile["photo_id"], caption=format_profile(profile), reply_markup=profile_keyboard(profile["user_id"], mode))
            await state.update_data(active_profile_id=profile["user_id"], active_profile_mode=mode, active_profile_msg_id=msg.message_id)
            return
        except TelegramBadRequest as exc:
            if "wrong file identifier" in str(exc).lower() or "PHOTO_INVALID" in str(exc).upper():
                await update_user_field(profile["user_id"], "is_active", 0)
                continue
            await safe_edit_or_send(callback, "Не удалось показать анкету. Попробуйте позже.")
            return

async def show_next_liker(callback: CallbackQuery, state: FSMContext) -> None:
    user_id = callback.from_user.id
    while True:
        profile = await get_next_liker(user_id)
        if not profile:
            await safe_edit_or_send(callback, "Новых лайков нет.", reply_markup=menu_keyboard(await get_likes_count(user_id)))
            return
        try:
            msg = await callback.message.answer_photo(photo=profile["photo_id"], caption=format_profile(profile, prefix="💌 <b>Вас оценили!</b>\n\n"), reply_markup=profile_like_keyboard(profile["user_id"]))
            await state.update_data(active_profile_id=profile["user_id"], active_profile_mode="likes", active_profile_msg_id=msg.message_id)
            return
        except TelegramBadRequest as exc:
            if "wrong file identifier" in str(exc).lower() or "PHOTO_INVALID" in str(exc).upper():
                await update_user_field(profile["user_id"], "is_active", 0)
                continue
            await safe_edit_or_send(callback, "Не удалось показать анкету.")
            return

async def show_next(callback: CallbackQuery, state: FSMContext, mode: str) -> None:
    if mode == "likes": await show_next_liker(callback, state); return
    await show_profile(callback, state, mode)

async def delete_callback_message(callback: CallbackQuery) -> None:
    try: await callback.message.delete()
    except TelegramBadRequest: pass

def rules_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Мне есть 18 лет, принимаю правила", callback_data="agree_rules")
    return builder.as_markup()

def gender_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🚹 Парень", callback_data="gender_male"); builder.button(text="🚺 Девушка", callback_data="gender_female"); builder.adjust(2); return builder

def search_gender_keyboard(with_cancel: bool = False):
    builder = InlineKeyboardBuilder()
    builder.button(text="🚹 Парней", callback_data="search_gender_male"); builder.button(text="🚺 Девушек", callback_data="search_gender_female"); builder.button(text="🌍 Всех", callback_data="search_gender_all")
    if with_cancel: builder.button(text="❌ Отмена", callback_data="cancel_edit"); builder.adjust(2, 1, 1)
    else: builder.adjust(2, 1)
    return builder

def menu_keyboard(likes_count: int = 0):
    builder = InlineKeyboardBuilder()
    builder.button(text="🔍 Смотреть анкеты", callback_data="search_menu"); builder.button(text="👤 Моя анкета", callback_data="my_profile")
    builder.button(text="💑 Мои мэтчи", callback_data="my_matches")
    if likes_count > 0: builder.button(text=f"💌 Вам понравились: {likes_count}", callback_data="show_likes")
    builder.adjust(2, 2 if likes_count > 0 else 1); return builder.as_markup()

def back_to_menu_keyboard():
    builder = InlineKeyboardBuilder(); builder.button(text="🔙 В меню", callback_data="menu"); return builder.as_markup()

def no_profiles_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Показать анкеты заново", callback_data="reset_views"); builder.button(text="🔙 В меню", callback_data="menu")
    builder.adjust(1, 1); return builder.as_markup()

def search_menu_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🏢 Только мой город", callback_data="search_local"); builder.button(text="🌍 Везде (сначала мой город)", callback_data="search_global"); builder.button(text="🔙 В меню", callback_data="menu"); builder.adjust(1, 1, 1); return builder.as_markup()

def profile_keyboard(profile_id: int, mode: str):
    builder = InlineKeyboardBuilder()
    builder.button(text="❤️ Лайк", callback_data=f"like:{profile_id}:{mode}"); builder.button(text="👎 Пропустить", callback_data=f"dislike:{profile_id}:{mode}")
    builder.button(text="🚫 Блокировать", callback_data=f"block:{profile_id}:{mode}"); builder.button(text="🚩 Пожаловаться", callback_data=f"report:{profile_id}:{mode}")
    builder.button(text="🔚 В меню", callback_data="menu"); builder.adjust(2, 2, 1); return builder.as_markup()

def profile_like_keyboard(profile_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="❤️ Ответить взаимностью", callback_data=f"like:{profile_id}:likes"); builder.button(text="👎 Пропустить", callback_data=f"dislike:{profile_id}:likes")
    builder.button(text="🚫 Блокировать", callback_data=f"block:{profile_id}:likes"); builder.button(text="🚩 Пожаловаться", callback_data=f"report:{profile_id}:likes")
    builder.adjust(2, 2); return builder.as_markup()

def edit_profile_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Имя", callback_data="edit_name"); builder.button(text="✏️ Возраст", callback_data="edit_age"); builder.button(text="✏️ Кого ищу", callback_data="edit_search_gender")
    builder.button(text="✏️ Город", callback_data="edit_city"); builder.button(text="✏️ О себе", callback_data="edit_bio"); builder.button(text="✏️ Фото", callback_data="edit_photo")
    builder.button(text="🗑 Удалить анкету", callback_data="delete_profile"); builder.button(text="🔙 Назад", callback_data="my_profile"); builder.adjust(2, 2, 2, 1, 1); return builder.as_markup()

def cancel_keyboard():
    builder = InlineKeyboardBuilder(); builder.button(text="❌ Отмена", callback_data="cancel_edit"); return builder.as_markup()

def cancel_report_keyboard():
    builder = InlineKeyboardBuilder(); builder.button(text="❌ Отмена", callback_data="cancel_report"); return builder.as_markup()

def match_keyboard(target_user_id: int):
    builder = InlineKeyboardBuilder(); builder.button(text="✉️ Написать сообщение", url=f"tg://user?id={target_user_id}"); return builder.as_markup()

def admin_report_keyboard(report_id: int, reported_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="🚫 Забанить", callback_data=f"admin_ban:{reported_id}:{report_id}")
    builder.button(text="✅ Отклонить", callback_data=f"admin_reject:{report_id}")
    builder.adjust(2)
    return builder.as_markup()

# Строгая валидация по message_id для всех режимов
async def validate_active_card(callback: CallbackQuery, state: FSMContext, p_id: int, mode: str) -> bool:
    data = await state.get_data()
    if data.get("active_profile_id") != p_id or data.get("active_profile_mode") != mode:
        return False
    if data.get("active_profile_msg_id") != callback.message.message_id:
        return False
    if mode == "likes" and not await verify_like_exists(liker_id=p_id, liked_id=callback.from_user.id):
        return False
    return True

# ============================================================
# ГЛОБАЛЬНЫЙ ОБРАБОТЧИК ОШИБОК
# ============================================================
@router.errors()
async def error_handler(event: ErrorEvent):
    logger.exception("Unhandled exception: %s", event.exception)
    update = event.update
    try:
        if update.callback_query:
            await update.callback_query.answer("Произошла внутренняя ошибка. Попробуйте позже.", show_alert=True)
        elif update.message:
            await update.message.answer("Произошла внутренняя ошибка. Попробуйте позже.")
    except Exception:
        pass

# ============================================================
# РЕГИСТРАЦИЯ
# ============================================================
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    user = await get_user(user_id)
    if user:
        if user["username"] != message.from_user.username:
            await update_user_field(user_id, "username", message.from_user.username)
        await message.answer("С возвращением! Вот ваша анкета:")
        await send_profile_card(message, user)
        await send_menu(message, user_id)
        return

    await state.set_state(Registration.agreement)
    builder = InlineKeyboardBuilder(); builder.button(text="✅ Мне есть 18 лет, принимаю правила", callback_data="agree_rules")
    await message.answer("Добро пожаловать в бот знакомств!\n\n⚠️ <b>Правила:</b>\n1. Сервис предназначен только для пользователей 18+.\n2. Запрещены оскорбления, мошенничество и спам.\n3. Уважайте личные границы и приватность других людей.\n4. Не публикуйте чужие фотографии и личные данные.\n\nПродолжая, вы подтверждаете, что вам исполнилось 18 лет и вы принимаете правила сервиса.", reply_markup=builder.as_markup())

@router.message(Command("delete"))
async def cmd_delete(message: Message, state: FSMContext):
    await state.clear(); await delete_user(message.from_user.id)
    await message.answer("Ваша анкета и все связанные с ней данные удалены. Для новой регистрации напишите /start.")

@router.callback_query(Registration.agreement, F.data == "agree_rules")
async def registration_agreement(callback: CallbackQuery, state: FSMContext):
    await callback.answer(); await state.set_state(Registration.name)
    await state.update_data(accepted_rules_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
    await callback.message.edit_text(f"Как вас зовут?\nВведите имя от {MIN_NAME_LENGTH} до {MAX_NAME_LENGTH} символов.")

@router.message(Registration.name, F.text)
async def registration_name(message: Message, state: FSMContext):
    name = validate_text(message.text, MIN_NAME_LENGTH, MAX_NAME_LENGTH)
    if not name: await message.answer(f"Имя должно содержать от {MIN_NAME_LENGTH} до {MAX_NAME_LENGTH} символов."); return
    await state.update_data(name=name); await state.set_state(Registration.age)
    await message.answer(f"Сколько вам лет?\nВведите число от {MIN_AGE} до {MAX_AGE}.")

@router.message(Registration.age, F.text)
async def registration_age(message: Message, state: FSMContext):
    age = validate_age(message.text)
    if age is None: await message.answer(f"Введите корректный возраст: число от {MIN_AGE} до {MAX_AGE}."); return
    await state.update_data(age=age); await state.set_state(Registration.gender)
    await message.answer("Укажите ваш пол:", reply_markup=gender_keyboard().as_markup())

@router.callback_query(Registration.gender, F.data.startswith("gender_"))
async def registration_gender(callback: CallbackQuery, state: FSMContext):
    code = callback.data.split("_", maxsplit=1)[1] if "_" in callback.data else None
    gender = gender_text(code)
    if not gender: await callback.answer("Некорректный вариант.", show_alert=True); return
    await callback.answer()
    await state.update_data(gender=gender); await state.set_state(Registration.search_gender)
    await callback.message.edit_text(f"Ваш пол: <b>{escape(gender)}</b>\n\nКого вы ищете?", reply_markup=search_gender_keyboard().as_markup())

@router.callback_query(Registration.search_gender, F.data.startswith("search_gender_"))
async def registration_search_gender(callback: CallbackQuery, state: FSMContext):
    code = callback.data.split("_", maxsplit=2)[2] if len(callback.data.split("_")) > 2 else None
    sg = search_gender_text(code)
    if not sg: await callback.answer("Некорректный вариант.", show_alert=True); return
    await callback.answer()
    await state.update_data(search_gender=sg); await state.set_state(Registration.city)
    await callback.message.edit_text(f"Вы ищете: <b>{escape(sg)}</b>\n\nИз какого вы города?\nОт {MIN_CITY_LENGTH} до {MAX_CITY_LENGTH} символов.")

@router.message(Registration.city, F.text)
async def registration_city(message: Message, state: FSMContext):
    city = validate_text(message.text, MIN_CITY_LENGTH, MAX_CITY_LENGTH)
    if not city: await message.answer(f"Название города должно содержать от {MIN_CITY_LENGTH} до {MAX_CITY_LENGTH} символов."); return
    await state.update_data(city=" ".join(city.strip().split())); await state.set_state(Registration.bio)
    await message.answer(f"Город: <b>{escape(city)}</b>\n\nРасскажите немного о себе: от {MIN_BIO_LENGTH} до {MAX_BIO_LENGTH} символов.")

@router.message(Registration.bio, F.text)
async def registration_bio(message: Message, state: FSMContext):
    bio = validate_text(message.text, MIN_BIO_LENGTH, MAX_BIO_LENGTH)
    if not bio: await message.answer(f"Описание должно содержать от {MIN_BIO_LENGTH} до {MAX_BIO_LENGTH} символов."); return
    await state.update_data(bio=bio); await state.set_state(Registration.photo)
    await message.answer("Отлично. Теперь отправьте вашу фотографию.")

@router.message(Registration.photo, F.photo)
async def registration_photo(message: Message, state: FSMContext):
    data = await state.get_data(); photo_id = message.photo[-1].file_id
    await add_user_to_db(user_id=message.from_user.id, name=data["name"], age=data["age"], gender=data["gender"], search_gender=data["search_gender"], city=data["city"], bio=data["bio"], photo_id=photo_id, username=message.from_user.username, accepted_rules_at=data.get("accepted_rules_at"))
    await state.clear(); await message.answer("Регистрация успешно завершена! 🎉", reply_markup=menu_keyboard())

@router.message(Registration.photo)
async def registration_photo_invalid(message: Message): await message.answer("Пожалуйста, отправьте изображение именно как фотографию.")

# StateFilter — правильная OR-семантика по нескольким состояниям
@router.message(StateFilter(
    Registration.name, Registration.age, Registration.city, Registration.bio,
    EditProfile.name, EditProfile.age, EditProfile.city, EditProfile.bio,
))
async def text_required_invalid(message: Message):
    await message.answer("Пожалуйста, отправьте значение текстом.")

# ============================================================
# ПОИСК АНКЕТ И ВЗАИМОДЕЙСТВИЯ
# ============================================================
@router.callback_query(F.data == "search_menu")
async def search_menu(callback: CallbackQuery):
    await callback.answer()
    user = await get_user(callback.from_user.id)
    if not user or not user["is_active"]:
        await safe_edit_or_send(callback, "Ваша анкета неактивна. Обновите фото через 'Моя анкета'.", reply_markup=back_to_menu_keyboard())
        return
    await safe_edit_or_send(callback, "Где будем искать анкеты?", reply_markup=search_menu_keyboard())

@router.callback_query(F.data == "search_local")
async def search_local(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    lock = get_user_lock(uid)
    if lock.locked():
        await callback.answer("Пожалуйста, подождите."); return
    async with lock:
        await callback.answer()
        await show_profile(callback, state, mode="local")

@router.callback_query(F.data == "search_global")
async def search_global(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    lock = get_user_lock(uid)
    if lock.locked():
        await callback.answer("Пожалуйста, подождите."); return
    async with lock:
        await callback.answer()
        await show_profile(callback, state, mode="global")

@router.callback_query(F.data.startswith("like:"))
async def like_profile(callback: CallbackQuery, state: FSMContext):
    parsed = parse_profile_callback(callback.data, "like")
    if not parsed: await callback.answer("Некорректные данные.", show_alert=True); return
    liked_id, mode = parsed
    uid = callback.from_user.id

    lock = get_user_lock(uid)
    if lock.locked():
        await callback.answer("Пожалуйста, подождите."); return

    async with lock:
        if liked_id == uid: await callback.answer("Нельзя поставить лайк самому себе.", show_alert=True); return
        if not await validate_active_card(callback, state, liked_id, mode):
            await callback.answer("Эта анкета больше не доступна.", show_alert=True)
            await delete_callback_message(callback); await show_next(callback, state, mode); return

        other_user = await get_user(liked_id)
        if not other_user or await check_ban(liked_id) or await are_users_blocked(uid, liked_id):
            await callback.answer("Эта анкета больше недоступна.", show_alert=True)
            await delete_callback_message(callback); await show_next(callback, state, mode); return

        await callback.answer()
        result = await add_like(uid, liked_id)
        await delete_callback_message(callback)

        if result == LikeResult.REJECTED:
            await callback.message.answer("Не удалось поставить лайк (блокировка или бан).")
            await show_next(callback, state, mode); return

        if result == LikeResult.LIKED:
            await show_next(callback, state, mode); return

        # MATCHED
        current_user = await get_user(uid)
        if not current_user: return
        text_me = f"💖 <b>Это мэтч!</b>\n\nВы понравились друг другу с {escape(other_user['name'])}.\nНаписать можно через кнопку ниже."
        text_other = f"💖 <b>Это мэтч!</b>\n\nВы понравились друг другу с {escape(current_user['name'])}.\nНаписать можно через кнопку ниже."

        try: await bot.send_message(uid, text_me, reply_markup=match_keyboard(liked_id))
        except (TelegramBadRequest, TelegramForbiddenError): logger.warning("Match send fail to %s", uid)
        try: await bot.send_message(liked_id, text_other, reply_markup=match_keyboard(uid))
        except (TelegramBadRequest, TelegramForbiddenError): logger.warning("Match send fail to %s", liked_id)

        await callback.message.answer("У вас мэтч! ❤️\n\nПродолжить поиск можно через главное меню.", reply_markup=menu_keyboard(await get_likes_count(uid)))

@router.callback_query(F.data.startswith("dislike:"))
async def dislike_profile(callback: CallbackQuery, state: FSMContext):
    parsed = parse_profile_callback(callback.data, "dislike")
    if not parsed: await callback.answer("Некорректные данные.", show_alert=True); return
    p_id, mode = parsed; uid = callback.from_user.id

    lock = get_user_lock(uid)
    if lock.locked():
        await callback.answer("Пожалуйста, подождите."); return

    async with lock:
        if not await validate_active_card(callback, state, p_id, mode):
            await callback.answer("Эта анкета больше не доступна.", show_alert=True)
            await delete_callback_message(callback); await show_next(callback, state, mode); return

        if p_id != uid and await get_user(p_id):
            if mode == "likes": await reject_like(uid, p_id)
            else: await add_view(uid, p_id)
        await callback.answer(); await delete_callback_message(callback); await show_next(callback, state, mode)

@router.callback_query(F.data.startswith("block:"))
async def block_profile(callback: CallbackQuery, state: FSMContext):
    parsed = parse_profile_callback(callback.data, "block")
    if not parsed: await callback.answer("Некорректные данные.", show_alert=True); return
    blocked_id, mode = parsed; uid = callback.from_user.id

    lock = get_user_lock(uid)
    if lock.locked():
        await callback.answer("Пожалуйста, подождите."); return

    async with lock:
        if not await validate_active_card(callback, state, blocked_id, mode):
            await callback.answer("Эта анкета больше не доступна.", show_alert=True)
            await delete_callback_message(callback); await show_next(callback, state, mode); return

        if blocked_id == uid: await callback.answer("Нельзя заблокировать самого себя.", show_alert=True); return
        if not await get_user(blocked_id): await callback.answer("Анкета недоступна.", show_alert=True); return

        await block_user(uid, blocked_id)
        await callback.answer("Пользователь заблокирован. Все лайки и мэтчи удалены.")
        await delete_callback_message(callback); await show_next(callback, state, mode)

@router.callback_query(F.data == "show_likes")
async def show_likes(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    lock = get_user_lock(uid)
    if lock.locked():
        await callback.answer("Пожалуйста, подождите."); return
    async with lock:
        await callback.answer()
        user = await get_user(uid)
        if not user or not user["is_active"]:
            await safe_edit_or_send(callback, "Ваша анкета неактивна.")
            return
        await show_next_liker(callback, state)

# ============================================================
# ЖАЛОБЫ И АДМИН-БАН
# ============================================================
@router.callback_query(F.data.startswith("report:"))
async def start_report(callback: CallbackQuery, state: FSMContext):
    parsed = parse_profile_callback(callback.data, "report")
    if not parsed: await callback.answer("Некорректные данные.", show_alert=True); return
    rep_id, mode = parsed; uid = callback.from_user.id

    lock = get_user_lock(uid)
    if lock.locked():
        await callback.answer("Пожалуйста, подождите."); return

    async with lock:
        if not await validate_active_card(callback, state, rep_id, mode):
            await callback.answer("Эта анкета больше не доступна.", show_alert=True)
            await delete_callback_message(callback); await show_next(callback, state, mode); return

        if rep_id == uid: await callback.answer("Нельзя пожаловаться на самого себя.", show_alert=True); return
        if not await get_user(rep_id): await callback.answer("Анкета не найдена.", show_alert=True); return
        if await report_exists(uid, rep_id): await callback.answer("Вы уже отправляли жалобу на этого пользователя.", show_alert=True); return

        await state.set_state(Report.reason); await state.update_data(reported_id=rep_id, mode=mode)
        await callback.answer()
        try: await callback.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest: pass
        await callback.message.answer(f"Опишите причину жалобы.\nМаксимальная длина: {MAX_REPORT_LENGTH} символов.", reply_markup=cancel_report_keyboard())

@router.message(Report.reason, F.text)
async def process_report(message: Message, state: FSMContext):
    reason = validate_text(message.text, 1, MAX_REPORT_LENGTH)
    if not reason: await message.answer(f"Жалоба не должна быть пустой и не может превышать {MAX_REPORT_LENGTH} символов."); return

    data = await state.get_data()
    rep_id, mode = data.get("reported_id"), data.get("mode")
    if not isinstance(rep_id, int): await state.clear(); await message.answer("Ошибка данных. Попробуйте ещё раз."); return

    rep_user = await get_user(rep_id)
    if not rep_user: await state.clear(); await message.answer("Эта анкета уже недоступна.", reply_markup=menu_keyboard()); return

    r_id = await add_report(uid := message.from_user.id, rep_id, reason)
    if r_id is None:
        await state.clear(); await message.answer("Вы уже отправляли жалобу на этого пользователя.", reply_markup=menu_keyboard(await get_likes_count(uid))); return

    await add_view(uid, rep_id)
    await message.answer("Жалоба отправлена администрации. Спасибо!", reply_markup=menu_keyboard(await get_likes_count(uid)))

    rep_name, rep_un = escape(rep_user["name"]), f"@{escape(rep_user['username'])}" if rep_user["username"] else "нет username"
    text = (f"🚨 <b>Новая жалоба #{r_id}</b>\n\n<b>От кого:</b> {message.from_user.mention_html()} (ID: <code>{uid}</code>)\n"
            f"<b>На кого:</b> {rep_name} (ID: <code>{rep_id}</code>, {rep_un})\n\n<b>Причина:</b>\n{escape(reason)}")
    
    # Отправляем фото админу
    try:
        if rep_user["photo_id"]:
            await bot.send_photo(ADMIN_ID, photo=rep_user["photo_id"], caption=text, reply_markup=admin_report_keyboard(r_id, rep_id))
        else:
            await bot.send_message(ADMIN_ID, text, reply_markup=admin_report_keyboard(r_id, rep_id))
    except (TelegramBadRequest, TelegramForbiddenError) as e: 
        logger.exception("Fail send report to admin: %s", e)
    finally: 
        await state.clear()

@router.message(Report.reason)
async def report_invalid(message: Message): await message.answer("Отправьте причину жалобы текстом.")

@router.callback_query(Report.reason, F.data == "cancel_report")
async def cancel_report(callback: CallbackQuery, state: FSMContext):
    await callback.answer(); await state.clear()
    try: await callback.message.delete()
    except TelegramBadRequest: pass
    await callback.message.answer("Жалоба отменена.", reply_markup=menu_keyboard(await get_likes_count(callback.from_user.id)))

@router.callback_query(F.data.startswith("admin_ban:"))
async def admin_ban_user(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: await callback.answer("Недостаточно прав.", show_alert=True); return
    try:
        _, raw_uid, raw_rid = callback.data.split(":")
        u_id, r_id = int(raw_uid), int(raw_rid)
    except (ValueError, AttributeError): await callback.answer("Некорректные данные.", show_alert=True); return

    await ban_user(u_id, f"Бан по жалобе #{r_id}", callback.from_user.id, r_id)
    await callback.answer("Пользователь заблокирован. Жалоба закрыта.")
    try: await callback.message.edit_caption(caption=f"{callback.message.html_text}\n\n✅ Пользователь <code>{u_id}</code> забанен.", reply_markup=None)
    except TelegramBadRequest: 
        try: await callback.message.edit_text(f"{callback.message.html_text}\n\n✅ Пользователь <code>{u_id}</code> забанен.", reply_markup=None)
        except TelegramBadRequest: pass
    try: await bot.send_message(u_id, "Вы были заблокированы администрацией. Для удаления данных используйте /delete.")
    except (TelegramBadRequest, TelegramForbiddenError): pass

@router.callback_query(F.data.startswith("admin_reject:"))
async def admin_reject_report(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: await callback.answer("Недостаточно прав.", show_alert=True); return
    try:
        _, r_id = callback.data.split(":")
        r_id = int(r_id)
    except (ValueError, AttributeError): await callback.answer("Некорректные данные.", show_alert=True); return

    async with transaction():
        await db.execute("UPDATE reports SET status = 'rejected', reviewed_by = ?, reviewed_at = CURRENT_TIMESTAMP WHERE id = ?", (callback.from_user.id, r_id))
    
    await callback.answer("Жалоба отклонена.")
    try: await callback.message.edit_caption(caption=f"{callback.message.html_text}\n\n❌ Жалоба #{r_id} отклонена.", reply_markup=None)
    except TelegramBadRequest:
        try: await callback.message.edit_text(f"{callback.message.html_text}\n\n❌ Жалоба #{r_id} отклонена.", reply_markup=None)
        except TelegramBadRequest: pass

# ============================================================
# ПРОФИЛЬ И РЕДАКТИРОВАНИЕ
# ============================================================
@router.callback_query(F.data == "my_profile")
async def my_profile(callback: CallbackQuery):
    await callback.answer()
    user = await get_user(callback.from_user.id)
    if not user:
        await callback.message.answer("Анкета не найдена. Напишите /start.")
        return
    await send_profile_card(callback.message, user)

@router.callback_query(F.data == "edit_profile")
async def edit_profile_menu(callback: CallbackQuery):
    await callback.answer()
    await delete_callback_message(callback)
    await callback.message.answer("Что вы хотите изменить?", reply_markup=edit_profile_keyboard())

@router.callback_query(F.data.in_({"edit_name", "edit_age", "edit_search_gender", "edit_city", "edit_bio", "edit_photo"}))
async def edit_start(callback: CallbackQuery, state: FSMContext):
    action = callback.data; await callback.answer()
    if action == "edit_search_gender": await state.set_state(EditProfile.search_gender); await callback.message.answer("Кого вы ищете?", reply_markup=search_gender_keyboard(with_cancel=True).as_markup())
    elif action == "edit_name": await state.set_state(EditProfile.name); await callback.message.answer(f"Введите новое имя: от {MIN_NAME_LENGTH} до {MAX_NAME_LENGTH} символов.", reply_markup=cancel_keyboard())
    elif action == "edit_age": await state.set_state(EditProfile.age); await callback.message.answer(f"Введите новый возраст: от {MIN_AGE} до {MAX_AGE}.", reply_markup=cancel_keyboard())
    elif action == "edit_city": await state.set_state(EditProfile.city); await callback.message.answer(f"Введите новый город: от {MIN_CITY_LENGTH} до {MAX_CITY_LENGTH} символов.", reply_markup=cancel_keyboard())
    elif action == "edit_bio": await state.set_state(EditProfile.bio); await callback.message.answer(f"Введите новое описание: от {MIN_BIO_LENGTH} до {MAX_BIO_LENGTH} символов.", reply_markup=cancel_keyboard())
    elif action == "edit_photo": await state.set_state(EditProfile.photo); await callback.message.answer("Отправьте новую фотографию.", reply_markup=cancel_keyboard())

@router.callback_query(EditProfile.search_gender, F.data.startswith("search_gender_"))
async def edit_search_gender_save(callback: CallbackQuery, state: FSMContext):
    code = callback.data.split("_", maxsplit=2)[2] if len(callback.data.split("_")) > 2 else None
    sg = search_gender_text(code)
    if not sg: await callback.answer("Некорректный вариант.", show_alert=True); return
    await callback.answer()
    await update_user_field(callback.from_user.id, "search_gender", sg); await state.clear()
    await callback.message.edit_text(f"Предпочтения обновлены. Теперь вы ищете: <b>{escape(sg)}</b>.")
    await callback.message.answer("Главное меню:", reply_markup=menu_keyboard(await get_likes_count(callback.from_user.id)))

@router.message(EditProfile.name, F.text)
async def edit_name_save(message: Message, state: FSMContext):
    name = validate_text(message.text, MIN_NAME_LENGTH, MAX_NAME_LENGTH)
    if not name: await message.answer(f"Имя должно содержать от {MIN_NAME_LENGTH} до {MAX_NAME_LENGTH} символов."); return
    await update_user_field(message.from_user.id, "name", name); await state.clear(); await send_menu(message, message.from_user.id, "Имя успешно обновлено.")

@router.message(EditProfile.age, F.text)
async def edit_age_save(message: Message, state: FSMContext):
    age = validate_age(message.text)
    if age is None: await message.answer(f"Введите корректный возраст: от {MIN_AGE} до {MAX_AGE}."); return
    await update_user_field(message.from_user.id, "age", age); await state.clear(); await send_menu(message, message.from_user.id, "Возраст успешно обновлён.")

@router.message(EditProfile.city, F.text)
async def edit_city_save(message: Message, state: FSMContext):
    city = validate_text(message.text, MIN_CITY_LENGTH, MAX_CITY_LENGTH)
    if not city: await message.answer(f"Город должен содержать от {MIN_CITY_LENGTH} до {MAX_CITY_LENGTH} символов."); return
    city = " ".join(city.strip().split())
    await update_user_field(message.from_user.id, "city", city); await state.clear(); await send_menu(message, message.from_user.id, f"Город успешно обновлён: <b>{escape(city)}</b>.")

@router.message(EditProfile.bio, F.text)
async def edit_bio_save(message: Message, state: FSMContext):
    bio = validate_text(message.text, MIN_BIO_LENGTH, MAX_BIO_LENGTH)
    if not bio: await message.answer(f"Описание должно содержать от {MIN_BIO_LENGTH} до {MAX_BIO_LENGTH} символов."); return
    await update_user_field(message.from_user.id, "bio", bio); await state.clear(); await send_menu(message, message.from_user.id, "Описание успешно обновлено.")

@router.message(EditProfile.photo, F.photo)
async def edit_photo_save(message: Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    await update_user_field(message.from_user.id, "photo_id", photo_id)
    await update_user_field(message.from_user.id, "is_active", 1) # Восстанавливаем активность
    await state.clear(); await send_menu(message, message.from_user.id, "Фотография успешно обновлена. Анкета снова активна.")

@router.message(EditProfile.photo)
async def edit_photo_invalid(message: Message): await message.answer("Пожалуйста, отправьте изображение именно как фотографию.")

@router.callback_query(F.data == "cancel_edit")
async def cancel_editing(callback: CallbackQuery, state: FSMContext):
    await callback.answer(); await state.clear()
    try: await callback.message.delete()
    except TelegramBadRequest: pass
    await callback.message.answer("Действие отменено.", reply_markup=menu_keyboard(await get_likes_count(callback.from_user.id)))

@router.callback_query(F.data == "delete_profile")
async def delete_profile_confirm(callback: CallbackQuery):
    await callback.answer()
    builder = InlineKeyboardBuilder(); builder.button(text="✅ Да, удалить анкету", callback_data="confirm_delete"); builder.button(text="🔙 Назад", callback_data="edit_profile"); builder.adjust(1)
    await callback.message.edit_text("Вы уверены, что хотите удалить анкету?\n\nБудут удалены профиль, лайки, просмотры, блокировки и связанные данные. Это действие нельзя отменить.", reply_markup=builder.as_markup())

@router.callback_query(F.data == "confirm_delete")
async def delete_profile_execute(callback: CallbackQuery, state: FSMContext):
    await callback.answer(); await state.clear(); await delete_user(callback.from_user.id)
    await callback.message.edit_text("Ваша анкета и связанные данные удалены.\n\nЧтобы зарегистрироваться снова, напишите /start.")

@router.callback_query(F.data == "menu")
async def back_to_menu(callback: CallbackQuery, state: FSMContext):
    await callback.answer(); await state.clear()
    likes = await get_likes_count(callback.from_user.id)
    await safe_edit_or_send(callback, "Главное меню:", reply_markup=menu_keyboard(likes))

@router.callback_query()
async def unknown_callback(callback: CallbackQuery): await callback.answer("Эта кнопка устарела. Откройте меню заново.", show_alert=True)

# ============================================================
# ЗАПУСК
# ============================================================
async def main() -> None:
    await init_db()
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Бот запущен.")
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        if db: await db.close()
        await bot.session.close()

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Бот остановлен.")