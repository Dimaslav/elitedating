import asyncio
import json
import logging
import os
import re
import shutil
import signal
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from html import escape
from io import BytesIO
from logging.handlers import RotatingFileHandler
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple

import aiosqlite
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    ErrorEvent,
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

try:
    ADMIN_ID = int(ADMIN_ID_RAW)
except ValueError as exc:
    raise RuntimeError("ADMIN_ID должен быть числом.") from exc

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не настроен в .env")
if ADMIN_ID <= 0:
    raise RuntimeError("ADMIN_ID не настроен.")

DB_NAME = os.getenv("DB_NAME", "dating_bot.db")
RULES_VERSION = 2

REDIS_URL = os.getenv("REDIS_URL")            # опционально: persistent FSM storage
SENTRY_DSN = os.getenv("SENTRY_DSN")          # опционально: мониторинг ошибок
BACKUP_DIR = os.getenv("BACKUP_DIR", "backups")
BACKUP_INTERVAL_HOURS = float(os.getenv("BACKUP_INTERVAL_HOURS", "12"))
BACKUP_RETENTION = int(os.getenv("BACKUP_RETENTION", "14"))

REPORT_DAILY_LIMIT = int(os.getenv("REPORT_DAILY_LIMIT", "5"))
SUPERLIKE_DAILY_LIMIT = int(os.getenv("SUPERLIKE_DAILY_LIMIT", "1"))
MAX_EXTRA_PHOTOS = int(os.getenv("MAX_EXTRA_PHOTOS", "4"))

PHOTO_RECHECK_INTERVAL_HOURS = float(os.getenv("PHOTO_RECHECK_INTERVAL_HOURS", "24"))
PHOTO_RECHECK_BATCH = int(os.getenv("PHOTO_RECHECK_BATCH", "50"))

LIKES_COUNT_CACHE_TTL = 5.0  # секунды

# ------------------------------------------------------------
# Логирование (с ротацией файлов)
# ------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

_file_handler = RotatingFileHandler(
    "bot.log", maxBytes=10_000_000, backupCount=5, encoding="utf-8"
)
_file_handler.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
)
logging.getLogger().addHandler(_file_handler)

# ------------------------------------------------------------
# Опциональный Sentry (мониторинг необработанных ошибок)
# ------------------------------------------------------------

if SENTRY_DSN:
    try:
        import sentry_sdk

        sentry_sdk.init(dsn=SENTRY_DSN, traces_sample_rate=0.0)
        logger.info("Sentry инициализирован.")
    except ImportError:
        logger.warning("SENTRY_DSN задан, но пакет sentry_sdk не установлен (pip install sentry-sdk).")

# ------------------------------------------------------------
# Bot / Dispatcher (опционально Redis для персистентного FSM)
# ------------------------------------------------------------

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

if REDIS_URL:
    try:
        from aiogram.fsm.storage.redis import RedisStorage

        storage = RedisStorage.from_url(REDIS_URL)
        logger.info("Используется RedisStorage для FSM (персистентно к рестартам).")
    except ImportError:
        logger.warning("REDIS_URL задан, но пакет redis не установлен. Используется MemoryStorage.")
        storage = MemoryStorage()
else:
    logger.warning(
        "FSM хранится в памяти (MemoryStorage). При рестарте бота все незавершённые "
        "диалоги (регистрация/редактирование/жалобы/чат) будут потеряны. "
        "Задайте REDIS_URL и установите aiogram[redis] для персистентности."
    )
    storage = MemoryStorage()

dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

db: Optional[aiosqlite.Connection] = None
db_read: Optional[aiosqlite.Connection] = None

db_lock = asyncio.Lock()      # только для записи (transaction())
read_lock = asyncio.Lock()    # для чтения — не блокируется записью надолго

# ============================================================
# КОНСТАНТЫ И ВАЛИДАЦИЯ
# ============================================================

MIN_AGE, MAX_AGE = 18, 99
MIN_NAME_LENGTH, MAX_NAME_LENGTH = 2, 50
MIN_CITY_LENGTH, MAX_CITY_LENGTH = 2, 50
MIN_BIO_LENGTH, MAX_BIO_LENGTH = 10, 500
MAX_REPORT_LENGTH = 500
MAX_INTERESTS = 5

VALID_GENDERS = {"male": "Парень", "female": "Девушка"}
VALID_SEARCH_GENDERS = {"male": "Парней", "female": "Девушек", "all": "Всех"}
VALID_MODES = {"local", "global", "likes"}

ALLOWED_USER_FIELDS = {
    "name",
    "age",
    "gender",
    "search_gender",
    "city",
    "bio",
    "photo_id",
    "username",
    "age_min",
    "age_max",
    "interests",
}

KNOWN_COMMANDS = {"/start", "/delete", "/help", "/stats", "/unban", "/mydata", "/pending", "/banned", "/finduser"}

RULES_TEXT = (
    "⚠️ <b>Правила:</b>\n"
    "1. Сервис предназначен только для пользователей 18+.\n"
    "2. Запрещены оскорбления, мошенничество и спам.\n"
    "3. Уважайте личные границы и приватность других людей.\n"
    "4. Не публикуйте чужие фотографии и личные данные.\n\n"
    "Продолжая, вы подтверждаете, что вам исполнилось 18 лет и вы принимаете правила сервиса."
)

POPULAR_CITIES = [
    "Москва", "Санкт-Петербург", "Новосибирск", "Екатеринбург",
    "Казань", "Нижний Новгород", "Минск", "Алматы",
]

INTERESTS_LIST = [
    "🎵 Музыка", "🎮 Игры", "📚 Книги", "🏋️ Спорт",
    "🍳 Готовка", "✈️ Путешествия", "🎬 Кино", "🐾 Животные",
    "🎨 Творчество", "💻 IT",
]

REPORT_REASONS = [
    ("spam", "🚫 Спам / реклама"),
    ("scam", "💸 Мошенничество"),
    ("insult", "🤬 Оскорбления"),
    ("minor", "🔞 Несовершеннолетний"),
    ("fake_photo", "🖼 Чужие фотографии"),
    ("other", "✏️ Другое"),
]
REPORT_REASON_TEXT = dict(REPORT_REASONS)

# ------------------------------------------------------------
# Минимальная i18n-заготовка.
#
# ВАЖНО: ниже — расширяемый каркас, а НЕ перевод всего интерфейса.
# У бота уже есть тысячи строк на русском; чтобы честно "перевести всё",
# нужно явно решить, на какие языки, и вычитать тексты. Здесь показан
# работающий паттерн (колонка users.language + словарь + get_text),
# который можно постепенно применять к остальным строкам.
# ------------------------------------------------------------

TEXTS: Dict[str, Dict[str, str]] = {
    "ru": {
        "menu_title": "Главное меню:",
        "cancelled": "Действие отменено.",
    },
    "en": {
        "menu_title": "Main menu:",
        "cancelled": "Action cancelled.",
    },
}

def get_text(lang: Optional[str], key: str) -> str:
    lang = lang if lang in TEXTS else "ru"
    return TEXTS[lang].get(key, TEXTS["ru"].get(key, key))

class LikeResult(Enum):
    LIKED = 1
    MATCHED = 2
    ALREADY_MATCHED = 3
    REJECTED = 4

def normalize_text(value: str) -> str:
    return " ".join(value.strip().split())

def validate_text(value: Optional[str], min_length: int, max_length: int) -> Optional[str]:
    if not value:
        return None
    normalized = normalize_text(value)
    return normalized if min_length <= len(normalized) <= max_length else None

def validate_age(value: Optional[str]) -> Optional[int]:
    if not value or not value.strip().isdigit():
        return None
    age = int(value.strip())
    return age if MIN_AGE <= age <= MAX_AGE else None

def validate_age_range(value: Optional[str]) -> Optional[Tuple[int, int]]:
    """Парсит строку вида '18-35' в (age_min, age_max)."""
    if not value:
        return None
    match = re.match(r"^\s*(\d{1,3})\s*-\s*(\d{1,3})\s*$", value.strip())
    if not match:
        return None
    low, high = int(match.group(1)), int(match.group(2))
    if low > high:
        low, high = high, low
    low = max(MIN_AGE, low)
    high = min(MAX_AGE, high)
    if low > high:
        return None
    return low, high

def gender_text(code: str) -> Optional[str]:
    return VALID_GENDERS.get(code)

def search_gender_text(code: str) -> Optional[str]:
    return VALID_SEARCH_GENDERS.get(code)

def parse_profile_callback(data: Optional[str], expected_action: str) -> Optional[Tuple[int, str]]:
    if not data:
        return None
    try:
        action, raw_id, mode = data.split(":")
        p_id = int(raw_id)
    except (ValueError, TypeError):
        return None
    if action != expected_action or p_id <= 0 or mode not in VALID_MODES:
        return None
    return p_id, mode

def is_invalid_photo_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(
        marker in text
        for marker in (
            "wrong file identifier",
            "wrong file_id",
            "photo_invalid",
            "file is too old",
        )
    )

def parse_interests(raw: Optional[str]) -> Set[str]:
    if not raw:
        return set()
    return {item for item in raw.split(",") if item in INTERESTS_LIST}

def serialize_interests(items: Set[str]) -> str:
    return ",".join(sorted(items, key=INTERESTS_LIST.index))

# ============================================================
# PER-USER ACTION LOCKS (с TTL-очисткой, чтобы не течь по памяти)
# ============================================================

_user_action_locks: Dict[int, asyncio.Lock] = {}
_user_action_locks_last_used: Dict[int, float] = {}
_LOCKS_CLEANUP_THRESHOLD = 5000
_LOCKS_TTL_SECONDS = 3600

def get_user_lock(user_id: int) -> asyncio.Lock:
    now = time.monotonic()
    lock = _user_action_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _user_action_locks[user_id] = lock

    _user_action_locks_last_used[user_id] = now

    if len(_user_action_locks) > _LOCKS_CLEANUP_THRESHOLD:
        cutoff = now - _LOCKS_TTL_SECONDS
        stale = [
            uid for uid, ts in _user_action_locks_last_used.items()
            if ts < cutoff and not _user_action_locks[uid].locked()
        ]
        for uid in stale:
            _user_action_locks.pop(uid, None)
            _user_action_locks_last_used.pop(uid, None)

    return lock

# ============================================================
# ПРОСТОЙ КЭШ СЧЁТЧИКА ЛАЙКОВ (снижает нагрузку на тяжёлый запрос)
# ============================================================

_likes_count_cache: Dict[int, Tuple[int, float]] = {}

def invalidate_likes_cache(*user_ids: int) -> None:
    for uid in user_ids:
        _likes_count_cache.pop(uid, None)

# ============================================================
# FSM
# ============================================================

class Registration(StatesGroup):
    agreement = State()
    name = State()
    age = State()
    gender = State()
    search_gender = State()
    age_range = State()
    city = State()
    interests = State()
    bio = State()
    photo = State()
    extra_photos = State()

class EditProfile(StatesGroup):
    name = State()
    age = State()
    search_gender = State()
    age_range = State()
    city = State()
    interests = State()
    bio = State()
    photo = State()
    add_photo = State()

class Report(StatesGroup):
    reason = State()

class Relay(StatesGroup):
    chatting = State()

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

async def fetch_one(query: str, params: tuple = ()) -> Optional[aiosqlite.Row]:
    async with read_lock:
        async with db_read.execute(query, params) as cursor:
            return await cursor.fetchone()

async def fetch_all(query: str, params: tuple = ()) -> list:
    async with read_lock:
        async with db_read.execute(query, params) as cursor:
            return await cursor.fetchall()

async def _configure_connection(conn: aiosqlite.Connection) -> None:
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA foreign_keys = ON")
    await conn.execute("PRAGMA journal_mode = WAL")
    await conn.execute("PRAGMA synchronous = NORMAL")
    await conn.execute("PRAGMA busy_timeout = 5000")

async def init_db() -> None:
    global db, db_read

    db = await aiosqlite.connect(DB_NAME)
    await _configure_connection(db)

    db_read = await aiosqlite.connect(DB_NAME)
    await _configure_connection(db_read)

    await db.execute("""CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER NOT NULL CHECK(age BETWEEN 18 AND 99),
        gender TEXT NOT NULL CHECK(gender IN ('Парень', 'Девушка')),
        search_gender TEXT NOT NULL CHECK(search_gender IN ('Парней', 'Девушек', 'Всех')),
        city TEXT NOT NULL,
        bio TEXT NOT NULL,
        photo_id TEXT NOT NULL,
        username TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        accepted_rules_at DATETIME,
        is_active INTEGER NOT NULL DEFAULT 1,
        accepted_rules_version INTEGER
    )""")

    await db.execute("""CREATE TABLE IF NOT EXISTS views (
        viewer_id INTEGER NOT NULL,
        viewed_id INTEGER NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (viewer_id, viewed_id),
        CHECK (viewer_id != viewed_id),
        FOREIGN KEY (viewer_id) REFERENCES users(user_id) ON DELETE CASCADE,
        FOREIGN KEY (viewed_id) REFERENCES users(user_id) ON DELETE CASCADE
    )""")

    await db.execute("""CREATE TABLE IF NOT EXISTS likes (
        liker_id INTEGER NOT NULL,
        liked_id INTEGER NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (liker_id, liked_id),
        CHECK (liker_id != liked_id),
        FOREIGN KEY (liker_id) REFERENCES users(user_id) ON DELETE CASCADE,
        FOREIGN KEY (liked_id) REFERENCES users(user_id) ON DELETE CASCADE
    )""")

    await db.execute("""CREATE TABLE IF NOT EXISTS matches (
        user1_id INTEGER NOT NULL,
        user2_id INTEGER NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (user1_id, user2_id),
        CHECK (user1_id < user2_id),
        FOREIGN KEY (user1_id) REFERENCES users(user_id) ON DELETE CASCADE,
        FOREIGN KEY (user2_id) REFERENCES users(user_id) ON DELETE CASCADE
    )""")

    await db.execute("""CREATE TABLE IF NOT EXISTS reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reporter_id INTEGER NOT NULL,
        reported_id INTEGER NOT NULL,
        reason TEXT NOT NULL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'accepted', 'rejected')),
        reviewed_by INTEGER,
        reviewed_at DATETIME,
        resolution TEXT,
        CHECK(reporter_id != reported_id),
        FOREIGN KEY (reporter_id) REFERENCES users(user_id) ON DELETE CASCADE,
        FOREIGN KEY (reported_id) REFERENCES users(user_id) ON DELETE CASCADE
    )""")

    await db.execute("""CREATE TABLE IF NOT EXISTS bans (
        user_id INTEGER PRIMARY KEY,
        reason TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")

    await db.execute("""CREATE TABLE IF NOT EXISTS blocks (
        blocker_id INTEGER NOT NULL,
        blocked_id INTEGER NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (blocker_id, blocked_id),
        CHECK (blocker_id != blocked_id),
        FOREIGN KEY (blocker_id) REFERENCES users(user_id) ON DELETE CASCADE,
        FOREIGN KEY (blocked_id) REFERENCES users(user_id) ON DELETE CASCADE
    )""")

    await db.execute("CREATE INDEX IF NOT EXISTS idx_users_active_gender ON users(is_active, gender)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_users_gender_city_nocase ON users(gender, city COLLATE NOCASE)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_likes_liked ON likes(liked_id)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_views_viewed ON views(viewed_id)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_blocks_blocked ON blocks(blocked_id)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_matches_user2 ON matches(user2_id)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_reports_status_created ON reports(status, timestamp)")
    await db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_reports_pending_unique "
        "ON reports(reporter_id, reported_id) WHERE status = 'pending'"
    )
    await db.commit()

    await migrate_db()

    version = (await fetch_one("PRAGMA user_version"))[0]
    logger.info("DB path=%s | user_version=%s", os.path.abspath(DB_NAME), version)
    logger.info("База данных успешно инициализирована.")

async def _table_columns(table: str) -> List[str]:
    async with db.execute(f"PRAGMA table_info({table})") as cursor:
        return [row[1] for row in await cursor.fetchall()]

async def migrate_db():
    version = (await fetch_one("PRAGMA user_version"))[0]

    if version < 1:
        columns = await _table_columns("reports")
        if "status" not in columns:
            await db.execute("ALTER TABLE reports ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'")
            await db.execute("ALTER TABLE reports ADD COLUMN reviewed_by INTEGER")
            await db.execute("ALTER TABLE reports ADD COLUMN reviewed_at DATETIME")
            await db.execute("ALTER TABLE reports ADD COLUMN resolution TEXT")
        await db.execute("DROP INDEX IF EXISTS idx_reports_unique")
        await db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_reports_pending_unique "
            "ON reports(reporter_id, reported_id) WHERE status = 'pending'"
        )
        await db.execute("PRAGMA user_version = 1")

    if version < 2:
        columns = await _table_columns("users")
        if "is_active" not in columns:
            await db.execute("ALTER TABLE users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
        if "accepted_rules_version" not in columns:
            await db.execute("ALTER TABLE users ADD COLUMN accepted_rules_version INTEGER")
        await db.execute("PRAGMA user_version = 2")

    if version < 3:
        columns = await _table_columns("users")
        additions = {
            "age_min": "INTEGER NOT NULL DEFAULT 18",
            "age_max": "INTEGER NOT NULL DEFAULT 99",
            "is_paused": "INTEGER NOT NULL DEFAULT 0",
            "language": "TEXT NOT NULL DEFAULT 'ru'",
            "last_active": "DATETIME",
            "interests": "TEXT NOT NULL DEFAULT ''",
        }
        for col, ddl in additions.items():
            if col not in columns:
                await db.execute(f"ALTER TABLE users ADD COLUMN {col} {ddl}")

        match_columns = await _table_columns("matches")
        if "revealed_by1" not in match_columns:
            await db.execute("ALTER TABLE matches ADD COLUMN revealed_by1 INTEGER NOT NULL DEFAULT 0")
        if "revealed_by2" not in match_columns:
            await db.execute("ALTER TABLE matches ADD COLUMN revealed_by2 INTEGER NOT NULL DEFAULT 0")

        await db.execute("""CREATE TABLE IF NOT EXISTS photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            file_id TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        )""")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_photos_user ON photos(user_id, position)")

        await db.execute("""CREATE TABLE IF NOT EXISTS superlikes (
            liker_id INTEGER NOT NULL,
            liked_id INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (liker_id, liked_id),
            FOREIGN KEY (liker_id) REFERENCES users(user_id) ON DELETE CASCADE,
            FOREIGN KEY (liked_id) REFERENCES users(user_id) ON DELETE CASCADE
        )""")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_superlikes_liker_created ON superlikes(liker_id, created_at)")

        await db.execute("CREATE INDEX IF NOT EXISTS idx_reports_reported ON reports(reported_id, status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_reports_reporter_created ON reports(reporter_id, timestamp)")

        await db.execute("PRAGMA user_version = 3")

    await db.commit()

# ------------------------------------------------------------
# Базовые операции с пользователями
# ------------------------------------------------------------

async def get_user_status(user_id: int) -> dict:
    row = await fetch_one(
        """
        SELECT
            EXISTS(SELECT 1 FROM bans WHERE user_id=?) AS is_banned,
            EXISTS(SELECT 1 FROM users WHERE user_id=?) AS user_exists,
            (SELECT accepted_rules_version FROM users WHERE user_id=?) AS accepted_rules_version
        """,
        (user_id, user_id, user_id),
    )
    return {
        "is_banned": bool(row["is_banned"]),
        "exists": bool(row["user_exists"]),
        "rules_version": row["accepted_rules_version"],
    }

async def check_ban(user_id: int) -> bool:
    row = await fetch_one("SELECT 1 FROM bans WHERE user_id = ?", (user_id,))
    return row is not None

async def get_user(user_id: int) -> Optional[aiosqlite.Row]:
    return await fetch_one("SELECT * FROM users WHERE user_id = ?", (user_id,))

async def add_user_to_db(
    user_id: int,
    name: str,
    age: int,
    gender: str,
    search_gender: str,
    age_min: int,
    age_max: int,
    city: str,
    interests: str,
    bio: str,
    photo_id: str,
    username: Optional[str],
    accepted_rules_at: str,
) -> bool:
    async with transaction():
        async with db.execute("SELECT 1 FROM bans WHERE user_id=?", (user_id,)) as cur:
            if await cur.fetchone():
                return False

        await db.execute(
            """INSERT INTO users (
                user_id, name, age, gender, search_gender, age_min, age_max,
                city, interests, bio, photo_id,
                username, accepted_rules_at, is_active, accepted_rules_version,
                last_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                name=excluded.name,
                age=excluded.age,
                gender=excluded.gender,
                search_gender=excluded.search_gender,
                age_min=excluded.age_min,
                age_max=excluded.age_max,
                city=excluded.city,
                interests=excluded.interests,
                bio=excluded.bio,
                photo_id=excluded.photo_id,
                username=excluded.username,
                accepted_rules_at=excluded.accepted_rules_at,
                is_active=1,
                accepted_rules_version=excluded.accepted_rules_version""",
            (
                user_id, name, age, gender, search_gender, age_min, age_max,
                city, interests, bio, photo_id,
                username, accepted_rules_at, RULES_VERSION,
            ),
        )
    return True

async def update_user_field(user_id: int, field: str, value: Any) -> None:
    if field not in ALLOWED_USER_FIELDS:
        raise ValueError(f"Недопустимое поле: {field}")
    async with db_lock:
        await db.execute(f"UPDATE users SET {field} = ? WHERE user_id = ?", (value, user_id))
        await db.commit()

async def deactivate_user_profile(user_id: int) -> None:
    async with db_lock:
        await db.execute("UPDATE users SET is_active = 0 WHERE user_id = ?", (user_id,))
        await db.commit()

async def set_paused(user_id: int, paused: bool) -> None:
    async with db_lock:
        await db.execute("UPDATE users SET is_paused = ? WHERE user_id = ?", (1 if paused else 0, user_id))
        await db.commit()

async def update_photo(user_id: int, photo_id: str) -> bool:
    async with transaction():
        async with db.execute("SELECT 1 FROM bans WHERE user_id=?", (user_id,)) as cur:
            if await cur.fetchone():
                return False
        cur = await db.execute("UPDATE users SET photo_id=?, is_active=1 WHERE user_id=?", (photo_id, user_id))
        return cur.rowcount == 1

async def set_rules_accepted(user_id: int, accepted_at: str) -> None:
    async with db_lock:
        await db.execute(
            "UPDATE users SET accepted_rules_at = ?, accepted_rules_version = ? WHERE user_id = ?",
            (accepted_at, RULES_VERSION, user_id),
        )
        await db.commit()

async def delete_user(user_id: int) -> None:
    async with transaction():
        await db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))

_last_active_write: Dict[int, float] = {}
_LAST_ACTIVE_THROTTLE = 300  # не пишем чаще, чем раз в 5 минут на пользователя

async def touch_last_active(user_id: int) -> None:
    now = time.monotonic()
    if now - _last_active_write.get(user_id, 0) < _LAST_ACTIVE_THROTTLE:
        return
    _last_active_write[user_id] = now
    async with db_lock:
        await db.execute(
            "UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE user_id = ?", (user_id,)
        )
        await db.commit()

# ------------------------------------------------------------
# Фото (галерея)
# ------------------------------------------------------------

async def get_photos(user_id: int) -> list:
    return await fetch_all(
        "SELECT id, file_id, position FROM photos WHERE user_id = ? ORDER BY position ASC",
        (user_id,),
    )

async def add_photo(user_id: int, file_id: str) -> bool:
    photos = await get_photos(user_id)
    if len(photos) >= MAX_EXTRA_PHOTOS:
        return False
    next_position = (photos[-1]["position"] + 1) if photos else 0
    async with db_lock:
        await db.execute(
            "INSERT INTO photos (user_id, file_id, position) VALUES (?, ?, ?)",
            (user_id, file_id, next_position),
        )
        await db.commit()
    return True

async def delete_photo(user_id: int, photo_id: int) -> bool:
    async with db_lock:
        cur = await db.execute(
            "DELETE FROM photos WHERE id = ? AND user_id = ?", (photo_id, user_id)
        )
        await db.commit()
        return cur.rowcount > 0

# ------------------------------------------------------------
# Городa/условия поиска
# ------------------------------------------------------------

def build_eligibility_excludes(
    user_id: int,
    user_alias: str = "u",
    include_views: bool = True,
) -> Tuple[str, list]:
    """
    include_views=True  -> используется в обычном свайп-поиске (не показывать уже просмотренных).
    include_views=False -> используется в списке "кто меня лайкнул": просмотр карточки в общем
                            поиске НЕ должен скрывать людей, которые позже лайкнули пользователя.
    """
    parts = []
    params: list = []

    if include_views:
        parts.append(f"""
            AND NOT EXISTS (
                SELECT 1 FROM views v
                WHERE v.viewer_id=? AND v.viewed_id={user_alias}.user_id
            )
        """)
        params.append(user_id)

    parts.append(f"""
        AND NOT EXISTS (
            SELECT 1 FROM likes l
            WHERE l.liker_id=? AND l.liked_id={user_alias}.user_id
        )
        AND NOT EXISTS (
            SELECT 1 FROM blocks b
            WHERE b.blocker_id=? AND b.blocked_id={user_alias}.user_id
        )
        AND NOT EXISTS (
            SELECT 1 FROM blocks b
            WHERE b.blocker_id={user_alias}.user_id AND b.blocked_id=?
        )
        AND NOT EXISTS (
            SELECT 1 FROM bans b WHERE b.user_id={user_alias}.user_id
        )
        AND NOT EXISTS (
            SELECT 1 FROM matches m
            WHERE (m.user1_id=? AND m.user2_id={user_alias}.user_id)
               OR (m.user2_id=? AND m.user1_id={user_alias}.user_id)
        )
        AND NOT EXISTS (
            SELECT 1 FROM reports r
            WHERE r.reporter_id=? AND r.reported_id={user_alias}.user_id
        )
    """)
    params.extend([user_id, user_id, user_id, user_id, user_id, user_id])

    return "\n".join(parts), params

async def get_likes_count(user_id: int) -> int:
    cached = _likes_count_cache.get(user_id)
    now = time.monotonic()
    if cached and now - cached[1] < LIKES_COUNT_CACHE_TTL:
        return cached[0]

    exclude_sql, exclude_params = build_eligibility_excludes(user_id, "u", include_views=False)
    query = f"""
        SELECT COUNT(*) FROM likes AS l
        JOIN users AS u ON u.user_id = l.liker_id
        WHERE l.liked_id = ? AND u.is_active = 1 AND u.is_paused = 0
        {exclude_sql}
    """
    params = [user_id, *exclude_params]
    row = await fetch_one(query, tuple(params))
    count = row[0]
    _likes_count_cache[user_id] = (count, now)
    return count

async def are_users_blocked(u1: int, u2: int) -> bool:
    row = await fetch_one(
        "SELECT 1 FROM blocks WHERE (blocker_id=? AND blocked_id=?) OR (blocker_id=? AND blocked_id=?)",
        (u1, u2, u2, u1),
    )
    return row is not None

async def add_view(viewer_id: int, viewed_id: int) -> None:
    if viewer_id == viewed_id:
        return
    async with db_lock:
        await db.execute("INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)", (viewer_id, viewed_id))
        await db.commit()

async def add_like(liker_id: int, liked_id: int) -> LikeResult:
    if liker_id == liked_id:
        return LikeResult.REJECTED
    u1, u2 = min(liker_id, liked_id), max(liker_id, liked_id)

    async with transaction():
        async with db.execute(
            "SELECT user_id FROM users WHERE user_id IN (?, ?) AND is_active=1",
            (liker_id, liked_id),
        ) as cur:
            rows = await cur.fetchall()
        if len(rows) < 2:
            return LikeResult.REJECTED

        async with db.execute("SELECT 1 FROM bans WHERE user_id IN (?, ?) LIMIT 1", (liker_id, liked_id)) as cur:
            if await cur.fetchone():
                return LikeResult.REJECTED

        async with db.execute(
            "SELECT 1 FROM blocks WHERE (blocker_id=? AND blocked_id=?) OR (blocker_id=? AND blocked_id=?)",
            (liker_id, liked_id, liked_id, liker_id),
        ) as cur:
            if await cur.fetchone():
                return LikeResult.REJECTED

        async with db.execute("SELECT 1 FROM matches WHERE user1_id=? AND user2_id=?", (u1, u2)) as cur:
            if await cur.fetchone():
                return LikeResult.ALREADY_MATCHED

        await db.execute("INSERT OR IGNORE INTO likes (liker_id, liked_id) VALUES (?, ?)", (liker_id, liked_id))
        await db.execute("INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)", (liker_id, liked_id))

        async with db.execute("SELECT 1 FROM likes WHERE liker_id=? AND liked_id=?", (liked_id, liker_id)) as cur:
            mutual = await cur.fetchone() is not None

        if not mutual:
            invalidate_likes_cache(liked_id)
            return LikeResult.LIKED

        await db.execute(
            "DELETE FROM likes WHERE (liker_id=? AND liked_id=?) OR (liker_id=? AND liked_id=?)",
            (liker_id, liked_id, liked_id, liker_id),
        )
        await db.execute("INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)", (liked_id, liker_id))

        cursor = await db.execute("INSERT OR IGNORE INTO matches (user1_id, user2_id) VALUES (?, ?)", (u1, u2))
        invalidate_likes_cache(liker_id, liked_id)
        return LikeResult.MATCHED if cursor.rowcount > 0 else LikeResult.ALREADY_MATCHED

async def add_superlike(liker_id: int, liked_id: int) -> Tuple[bool, Optional[LikeResult]]:
    """Возвращает (разрешено_ли, результат_лайка). Лимит — SUPERLIKE_DAILY_LIMIT в сутки."""
    row = await fetch_one(
        "SELECT COUNT(*) FROM superlikes WHERE liker_id=? AND created_at >= datetime('now', '-1 day')",
        (liker_id,),
    )
    if row[0] >= SUPERLIKE_DAILY_LIMIT:
        return False, None

    async with db_lock:
        await db.execute(
            "INSERT OR IGNORE INTO superlikes (liker_id, liked_id) VALUES (?, ?)",
            (liker_id, liked_id),
        )
        await db.commit()

    result = await add_like(liker_id, liked_id)
    return True, result

async def reject_like(user_id: int, liker_id: int) -> None:
    async with transaction():
        await db.execute("DELETE FROM likes WHERE liker_id = ? AND liked_id = ?", (liker_id, user_id))
        await db.execute("INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)", (user_id, liker_id))
    invalidate_likes_cache(user_id)

async def block_user(blocker_id: int, blocked_id: int) -> bool:
    if blocker_id == blocked_id:
        return False

    u1, u2 = sorted((blocker_id, blocked_id))

    async with transaction():
        async with db.execute(
            "SELECT COUNT(*) FROM users WHERE user_id IN (?, ?)",
            (blocker_id, blocked_id),
        ) as cur:
            count = (await cur.fetchone())[0]

        if count != 2:
            return False

        await db.execute(
            "INSERT OR IGNORE INTO blocks (blocker_id, blocked_id) VALUES (?, ?)",
            (blocker_id, blocked_id),
        )
        await db.execute(
            """
            DELETE FROM likes
            WHERE (liker_id=? AND liked_id=?)
               OR (liker_id=? AND liked_id=?)
            """,
            (blocker_id, blocked_id, blocked_id, blocker_id),
        )
        await db.execute("DELETE FROM matches WHERE user1_id=? AND user2_id=?", (u1, u2))
        await db.execute("INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)", (blocker_id, blocked_id))
        return True

async def get_blocks(user_id: int) -> list:
    return await fetch_all(
        """SELECT u.user_id, u.name FROM blocks b
           JOIN users u ON u.user_id = b.blocked_id
           WHERE b.blocker_id = ?
           ORDER BY b.created_at DESC""",
        (user_id,),
    )

async def unblock_user(blocker_id: int, blocked_id: int) -> bool:
    async with transaction():
        cur = await db.execute(
            "DELETE FROM blocks WHERE blocker_id=? AND blocked_id=?",
            (blocker_id, blocked_id),
        )
        return cur.rowcount > 0

# ------------------------------------------------------------
# Жалобы
# ------------------------------------------------------------

async def report_exists(reporter_id: int, reported_id: int) -> bool:
    row = await fetch_one(
        "SELECT 1 FROM reports WHERE reporter_id=? AND reported_id=? AND status='pending' LIMIT 1",
        (reporter_id, reported_id),
    )
    return row is not None

async def reports_today_count(reporter_id: int) -> int:
    row = await fetch_one(
        "SELECT COUNT(*) FROM reports WHERE reporter_id=? AND timestamp >= datetime('now', '-1 day')",
        (reporter_id,),
    )
    return row[0]

async def add_report(reporter_id: int, reported_id: int, reason: str) -> Optional[int]:
    if reporter_id == reported_id:
        return None
    async with transaction():
        async with db.execute(
            """SELECT 1 FROM users u
               WHERE u.user_id=? AND u.is_active=1
                 AND NOT EXISTS (SELECT 1 FROM bans b WHERE b.user_id=u.user_id)""",
            (reported_id,),
        ) as cur:
            if not await cur.fetchone():
                return None

        try:
            cur = await db.execute(
                "INSERT OR IGNORE INTO reports (reporter_id, reported_id, reason) VALUES (?, ?, ?)",
                (reporter_id, reported_id, reason),
            )
            return cur.lastrowid if cur.rowcount > 0 else None
        except aiosqlite.IntegrityError:
            return None

async def ban_user_from_report(report_id: int, admin_id: int) -> Optional[int]:
    async with transaction():
        cur = await db.execute(
            """UPDATE reports
               SET status='accepted', reviewed_by=?, reviewed_at=CURRENT_TIMESTAMP, resolution='banned'
               WHERE id=? AND status='pending'""",
            (admin_id, report_id),
        )
        if cur.rowcount == 0:
            return None

        async with db.execute("SELECT reported_id FROM reports WHERE id=?", (report_id,)) as cur:
            row = await cur.fetchone()
            if not row:
                return None

        reported_id = row["reported_id"]

        await db.execute(
            """INSERT INTO bans (user_id, reason) VALUES (?, ?)
               ON CONFLICT(user_id) DO UPDATE SET reason=excluded.reason""",
            (reported_id, f"Бан по жалобе #{report_id}"),
        )
        await db.execute("DELETE FROM likes WHERE liker_id=? OR liked_id=?", (reported_id, reported_id))
        await db.execute("DELETE FROM matches WHERE user1_id=? OR user2_id=?", (reported_id, reported_id))

        await db.execute(
            """
            UPDATE reports
            SET status='accepted', reviewed_by=?, reviewed_at=CURRENT_TIMESTAMP, resolution='already_banned'
            WHERE reported_id=? AND id<>? AND status='pending'
            """,
            (admin_id, reported_id, report_id),
        )

        return reported_id

async def reject_report(report_id: int, admin_id: int) -> bool:
    async with transaction():
        cur = await db.execute(
            """UPDATE reports
               SET status='rejected', reviewed_by=?, reviewed_at=CURRENT_TIMESTAMP, resolution='rejected'
               WHERE id=? AND status='pending'""",
            (admin_id, report_id),
        )
        return cur.rowcount > 0

async def unban_user(user_id: int) -> bool:
    async with transaction():
        cur = await db.execute("DELETE FROM bans WHERE user_id = ?", (user_id,))
        return cur.rowcount > 0

async def get_pending_reports(limit: int = 5, offset: int = 0) -> list:
    return await fetch_all(
        """SELECT r.*, u1.name AS reporter_name, u2.name AS reported_name
           FROM reports r
           LEFT JOIN users u1 ON u1.user_id = r.reporter_id
           LEFT JOIN users u2 ON u2.user_id = r.reported_id
           WHERE r.status = 'pending'
           ORDER BY r.timestamp ASC
           LIMIT ? OFFSET ?""",
        (limit, offset),
    )

async def get_banned_users(limit: int = 10, offset: int = 0) -> list:
    return await fetch_all(
        "SELECT user_id, reason, created_at FROM bans ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (limit, offset),
    )

async def get_user_admin_info(user_id: int) -> Dict[str, Any]:
    user = await get_user(user_id)
    banned = await check_ban(user_id)
    reports_against = await fetch_one(
        "SELECT COUNT(*) FROM reports WHERE reported_id = ?", (user_id,)
    )
    reports_by = await fetch_one(
        "SELECT COUNT(*) FROM reports WHERE reporter_id = ?", (user_id,)
    )
    return {
        "user": user,
        "banned": banned,
        "reports_against": reports_against[0],
        "reports_by": reports_by[0],
    }

# ------------------------------------------------------------
# Мэтчи и раскрытие контакта
# ------------------------------------------------------------

async def get_match_row(u1: int, u2: int) -> Optional[aiosqlite.Row]:
    a, b = sorted((u1, u2))
    return await fetch_one(
        "SELECT * FROM matches WHERE user1_id = ? AND user2_id = ?", (a, b)
    )

async def request_reveal(requester_id: int, partner_id: int) -> Tuple[bool, bool]:
    """
    Возвращает (существует_ли_матч, оба_ли_раскрыли_контакт_после_этого_вызова).
    """
    a, b = sorted((requester_id, partner_id))

    async with transaction():
        async with db.execute(
            "SELECT * FROM matches WHERE user1_id=? AND user2_id=?", (a, b)
        ) as cur:
            row = await cur.fetchone()

        if not row:
            return False, False

        if row["user1_id"] == requester_id:
            await db.execute(
                "UPDATE matches SET revealed_by1 = 1 WHERE user1_id=? AND user2_id=?", (a, b)
            )
        else:
            await db.execute(
                "UPDATE matches SET revealed_by2 = 1 WHERE user1_id=? AND user2_id=?", (a, b)
            )

        async with db.execute(
            "SELECT revealed_by1, revealed_by2 FROM matches WHERE user1_id=? AND user2_id=?", (a, b)
        ) as cur:
            updated = await cur.fetchone()

        both = bool(updated["revealed_by1"]) and bool(updated["revealed_by2"])
        return True, both

async def is_fully_revealed(u1: int, u2: int) -> bool:
    row = await get_match_row(u1, u2)
    if not row:
        return False
    return bool(row["revealed_by1"]) and bool(row["revealed_by2"])

async def get_matches(user_id: int) -> list:
    return await fetch_all(
        """SELECT u.user_id, u.name, m.created_at FROM matches m
           JOIN users u ON u.user_id = CASE WHEN m.user1_id = ? THEN m.user2_id ELSE m.user1_id END
           WHERE (m.user1_id = ? OR m.user2_id = ?) AND u.user_id != ? AND u.is_active = 1
           AND u.user_id NOT IN (SELECT user_id FROM bans)
           ORDER BY m.created_at DESC LIMIT 200""",
        (user_id, user_id, user_id, user_id),
    )

async def get_stats() -> Dict[str, int]:
    row = await fetch_one(
        """
        SELECT
            (SELECT COUNT(*) FROM users) AS users_total,
            (
                SELECT COUNT(*) FROM users u
                WHERE u.is_active = 1
                  AND NOT EXISTS (SELECT 1 FROM bans b WHERE b.user_id = u.user_id)
            ) AS users_active,
            (SELECT COUNT(*) FROM likes) AS likes,
            (SELECT COUNT(*) FROM matches) AS matches,
            (SELECT COUNT(*) FROM views) AS views,
            (SELECT COUNT(*) FROM blocks) AS blocks,
            (SELECT COUNT(*) FROM bans) AS bans,
            (SELECT COUNT(*) FROM reports WHERE status = 'pending') AS reports_pending
        """
    )
    return dict(row)

# ------------------------------------------------------------
# Подбор анкет (с учётом диапазона возраста, пауз и "умной" сортировки)
# ------------------------------------------------------------

def _interest_ranking_sql(interests: Set[str], alias: str = "u") -> Tuple[str, list]:
    if not interests:
        return "0", []
    conditions = " OR ".join([f"{alias}.interests LIKE ?" for _ in interests])
    params = [f"%{tag}%" for tag in interests]
    return f"(CASE WHEN ({conditions}) THEN 0 ELSE 1 END)", params

async def get_random_profile(
    user_id: int,
    user_gender: str,
    search_gender: str,
    user_age: int,
    user_age_min: int,
    user_age_max: int,
    user_interests: Set[str],
    user_city: Optional[str] = None,
    strict_city: bool = False,
) -> Optional[aiosqlite.Row]:
    exclude_sql, exclude_params = build_eligibility_excludes(user_id, "u", include_views=True)
    query = f"""
        SELECT * FROM users u
        WHERE u.user_id != ? AND u.is_active = 1 AND u.is_paused = 0
        {exclude_sql}
    """
    params = [user_id, *exclude_params]

    if search_gender != "Всех":
        g_filter = {"Парней": "Парень", "Девушек": "Девушка"}.get(search_gender)
        if g_filter:
            query += " AND u.gender = ?"
            params.append(g_filter)

    query += """
        AND (
            u.search_gender = 'Всех'
            OR (u.search_gender = 'Парней' AND ? = 'Парень')
            OR (u.search_gender = 'Девушек' AND ? = 'Девушка')
        )
    """
    params.extend([user_gender, user_gender])

    # Взаимный диапазон возраста: кандидат должен попадать в диапазон
    # искателя, а искатель — в диапазон кандидата.
    query += " AND ? BETWEEN u.age_min AND u.age_max AND u.age BETWEEN ? AND ?"
    params.extend([user_age, user_age_min, user_age_max])

    interest_rank_sql, interest_params = _interest_ranking_sql(user_interests, "u")

    order_clauses = []

    if strict_city and user_city:
        query += " AND u.city = ? COLLATE NOCASE"
        params.append(user_city)
    elif user_city:
        order_clauses.append("CASE WHEN u.city = ? COLLATE NOCASE THEN 0 ELSE 1 END")
        params.append(user_city)

    order_clauses.append(interest_rank_sql)
    params.extend(interest_params)

    # "Умная" сортировка: чуть больший шанс у недавно активных анкет,
    # но не строго детерминированная — сохраняем элемент случайности.
    order_clauses.append(
        "RANDOM() * (1.0 + 1.0 / (1.0 + (julianday('now') - julianday(COALESCE(u.last_active, u.created_at)))))"
    )

    query += " ORDER BY " + ", ".join(order_clauses) + " DESC LIMIT 1"

    return await fetch_one(query, tuple(params))

async def get_next_liker(user_id: int) -> Optional[aiosqlite.Row]:
    exclude_sql, exclude_params = build_eligibility_excludes(user_id, "u", include_views=False)
    query = f"""
        SELECT u.* FROM users AS u
        JOIN likes AS l ON u.user_id = l.liker_id
        WHERE l.liked_id = ? AND u.is_active = 1 AND u.is_paused = 0
        {exclude_sql}
        ORDER BY l.created_at DESC LIMIT 1
    """
    params = [user_id, *exclude_params]
    return await fetch_one(query, tuple(params))

async def verify_like_exists(liker_id: int, liked_id: int) -> bool:
    row = await fetch_one("SELECT 1 FROM likes WHERE liker_id=? AND liked_id=?", (liker_id, liked_id))
    return row is not None

# ============================================================
# MIDDLEWARE
# ============================================================

class SecurityMiddleware(BaseMiddleware):
    def __init__(self):
        self.last_action: Dict[Tuple[int, str], float] = {}
        self.cooldown_callback = 0.4
        self.cooldown_message = 0.7
        self.cooldown_command = 2.0

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if not user:
            return await handler(event, data)

        asyncio.ensure_future(touch_last_active(user.id))

        fsm: Optional[FSMContext] = data.get("state")
        current_state = None
        if fsm is not None:
            try:
                current_state = await fsm.get_state()
            except Exception:
                current_state = None

        first_word: Optional[str] = None
        if isinstance(event, Message) and event.text:
            first_word = event.text.split()[0].split("@")[0]

        status = await get_user_status(user.id)

        if status["is_banned"]:
            if first_word == "/delete":
                return await handler(event, data)
            if isinstance(event, Message):
                await event.answer("Вы заблокированы. Для удаления данных используйте /delete.")
            elif isinstance(event, CallbackQuery):
                await event.answer("Вы заблокированы.", show_alert=True)
            return

        if status["exists"] and (status["rules_version"] or 0) < RULES_VERSION:
            if isinstance(event, Message) and first_word in {"/start", "/delete"}:
                return await handler(event, data)
            if isinstance(event, CallbackQuery) and event.data == "agree_rules":
                return await handler(event, data)
            if isinstance(event, CallbackQuery):
                await event.answer("Правила сервиса обновились. Напишите /start.", show_alert=True)
            elif isinstance(event, Message):
                await event.answer("Правила сервиса обновились. Пожалуйста, напишите /start.")
            return

        bypass_states = {
            Registration.agreement.state,
            Registration.name.state,
            Registration.age.state,
            Registration.gender.state,
            Registration.search_gender.state,
            Registration.age_range.state,
            Registration.city.state,
            Registration.interests.state,
            Registration.bio.state,
            Registration.photo.state,
            Registration.extra_photos.state,
            EditProfile.name.state,
            EditProfile.age.state,
            EditProfile.search_gender.state,
            EditProfile.age_range.state,
            EditProfile.city.state,
            EditProfile.interests.state,
            EditProfile.bio.state,
            EditProfile.photo.state,
            EditProfile.add_photo.state,
            Report.reason.state,
        }

        if current_state in bypass_states:
            return await handler(event, data)

        is_command = first_word in KNOWN_COMMANDS
        if is_command:
            action_type = "command"
            cooldown = self.cooldown_command
        elif isinstance(event, CallbackQuery):
            action_type = "callback"
            cooldown = self.cooldown_callback
        else:
            action_type = "message"
            cooldown = self.cooldown_message

        key = (user.id, action_type)
        now = time.monotonic()

        if now - self.last_action.get(key, 0) < cooldown:
            if isinstance(event, CallbackQuery):
                await event.answer("Не так быстро. Подождите немного.")
            elif isinstance(event, Message):
                await event.answer("Не так быстро. Подождите немного.")
            return

        self.last_action[key] = now

        if len(self.last_action) > 10000:
            self.last_action = {
                k: v for k, v in self.last_action.items()
                if now - v < 3600
            }

        return await handler(event, data)

security_mw = SecurityMiddleware()
router.message.middleware(security_mw)
router.callback_query.middleware(security_mw)

# ============================================================
# КЛАВИАТУРЫ И ХЕЛПЕРЫ
# ============================================================

def format_profile(profile: aiosqlite.Row, prefix: str = "") -> str:
    interests = parse_interests(profile["interests"]) if "interests" in profile.keys() else set()
    interests_line = f"\n🏷 {escape(', '.join(interests))}" if interests else ""
    return (
        f"{prefix}👤 <b>{escape(profile['name'])}</b>, {profile['age']} ({escape(profile['gender'])})\n"
        f"📍 {escape(profile['city'])}{interests_line}\n\n📝 {escape(profile['bio'])}"
    )

def extract_html(message: Message) -> str:
    if message.photo:
        caption = getattr(message, "html_caption", None)
        return caption if caption else escape(message.caption or "")
    text = getattr(message, "html_text", None)
    return text if text else escape(message.text or "")

async def safe_edit_or_send(callback: CallbackQuery, text: str, reply_markup=None):
    if callback.message.photo:
        try:
            await callback.message.delete()
        except TelegramBadRequest:
            pass
        await callback.message.answer(text, reply_markup=reply_markup)
    else:
        try:
            await callback.message.edit_text(text, reply_markup=reply_markup)
        except TelegramBadRequest:
            await callback.message.answer(text, reply_markup=reply_markup)

async def send_menu(message: Message, user_id: int, text: str = "Главное меню:") -> None:
    await message.answer(text, reply_markup=menu_keyboard(await get_likes_count(user_id)))

def profile_card_keyboard(photos_count: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Редактировать", callback_data="edit_profile")
    if photos_count > 0:
        builder.button(text=f"🖼 Фото (0/{photos_count + 1})", callback_data="gallery:0")
    builder.button(text="🚫 Мои блокировки", callback_data="my_blocks")
    builder.button(text="🔙 В меню", callback_data="menu")
    builder.adjust(2, 1, 1)
    return builder.as_markup()

async def send_profile_card(message: Message, user: aiosqlite.Row) -> None:
    status_bits = []
    status_bits.append("✅ Активна" if user["is_active"] else "❌ Неактивна (обновите фото)")
    if user["is_paused"]:
        status_bits.append("⏸ На паузе (не показывается в поиске)")
    status = " | ".join(status_bits)

    photos = await get_photos(user["user_id"])

    text = (
        f"👤 <b>{escape(user['name'])}</b>, {user['age']} ({escape(user['gender'])})\n"
        f"🔍 Ищу: {escape(user['search_gender'])} ({user['age_min']}-{user['age_max']} лет)\n"
        f"📍 {escape(user['city'])}\n\n📝 {escape(user['bio'])}\n\n"
        f"💌 Новых лайков: {await get_likes_count(user['user_id'])}\nСтатус: {status}"
    )
    try:
        await message.answer_photo(
            photo=user["photo_id"], caption=text, reply_markup=profile_card_keyboard(len(photos))
        )
    except TelegramBadRequest as exc:
        logger.warning("Failed to send own profile photo for user=%s: %s", user["user_id"], exc)
        if is_invalid_photo_error(exc):
            await deactivate_user_profile(user["user_id"])
            await message.answer(
                "Ваше фото недействительно. Анкета временно деактивирована. Обновите фото в разделе редактирования.",
                reply_markup=profile_card_keyboard(len(photos)),
            )
        else:
            await message.answer(
                "Не удалось загрузить фото. Попробуйте позже или обновите его в разделе редактирования.",
                reply_markup=profile_card_keyboard(len(photos)),
            )

async def handle_stale_callback(callback: CallbackQuery, text: str = "Это действие уже недоступно.") -> bool:
    await callback.answer(text, show_alert=True)
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass
    return True

async def show_profile(callback: CallbackQuery, state: FSMContext, mode: str = "global") -> None:
    user = await get_user(callback.from_user.id)
    if not user or not user["is_active"]:
        await safe_edit_or_send(
            callback,
            "Ваша анкета неактивна. Обновите фото через 'Моя анкета' -> 'Редактировать'.",
            reply_markup=back_to_menu_keyboard(),
        )
        return

    if user["is_paused"]:
        await safe_edit_or_send(
            callback,
            "Ваша анкета на паузе. Включите показ анкеты в разделе редактирования, чтобы искать других.",
            reply_markup=back_to_menu_keyboard(),
        )
        return

    interests = parse_interests(user["interests"])

    while True:
        strict = (mode == "local")
        profile = await get_random_profile(
            user_id=callback.from_user.id,
            user_gender=user["gender"],
            search_gender=user["search_gender"],
            user_age=user["age"],
            user_age_min=user["age_min"],
            user_age_max=user["age_max"],
            user_interests=interests,
            user_city=user["city"],
            strict_city=strict,
        )
        if not profile:
            await safe_edit_or_send(
                callback,
                "Подходящие анкеты закончились.\nМожно просмотреть их заново или заглянуть позже.",
                reply_markup=no_profiles_keyboard(),
            )
            return
        try:
            msg = await callback.message.answer_photo(
                photo=profile["photo_id"],
                caption=format_profile(profile),
                reply_markup=profile_keyboard(profile["user_id"], mode),
            )
            await state.update_data(
                active_profile_id=profile["user_id"],
                active_profile_mode=mode,
                active_profile_msg_id=msg.message_id,
            )
            return
        except TelegramBadRequest as exc:
            if is_invalid_photo_error(exc):
                await deactivate_user_profile(profile["user_id"])
                continue
            await safe_edit_or_send(callback, "Не удалось показать анкету. Попробуйте позже.")
            return

async def show_next_liker(callback: CallbackQuery, state: FSMContext) -> None:
    user_id = callback.from_user.id
    while True:
        profile = await get_next_liker(user_id)
        if not profile:
            await safe_edit_or_send(
                callback,
                "Новых лайков нет.",
                reply_markup=menu_keyboard(await get_likes_count(user_id)),
            )
            return
        try:
            msg = await callback.message.answer_photo(
                photo=profile["photo_id"],
                caption=format_profile(profile, prefix="💌 <b>Вас оценили!</b>\n\n"),
                reply_markup=profile_like_keyboard(profile["user_id"]),
            )
            await state.update_data(
                active_profile_id=profile["user_id"],
                active_profile_mode="likes",
                active_profile_msg_id=msg.message_id,
            )
            return
        except TelegramBadRequest as exc:
            if is_invalid_photo_error(exc):
                await deactivate_user_profile(profile["user_id"])
                continue
            await safe_edit_or_send(callback, "Не удалось показать анкету.")
            return

async def show_next(callback: CallbackQuery, state: FSMContext, mode: str) -> None:
    if mode == "likes":
        await show_next_liker(callback, state)
        return
    await show_profile(callback, state, mode)

async def delete_callback_message(callback: CallbackQuery) -> None:
    try:
        await callback.message.delete()
    except TelegramBadRequest:
        pass

def rules_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Мне есть 18 лет, принимаю правила", callback_data="agree_rules")
    return builder.as_markup()

def gender_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🚹 Парень", callback_data="gender_male")
    builder.button(text="🚺 Девушка", callback_data="gender_female")
    builder.adjust(2)
    return builder

def search_gender_keyboard(with_cancel: bool = False):
    builder = InlineKeyboardBuilder()
    builder.button(text="🚹 Парней", callback_data="search_gender_male")
    builder.button(text="🚺 Девушек", callback_data="search_gender_female")
    builder.button(text="🌍 Всех", callback_data="search_gender_all")
    if with_cancel:
        builder.button(text="❌ Отмена", callback_data="cancel_edit")
        builder.adjust(2, 1, 1)
    else:
        builder.adjust(2, 1)
    return builder

def city_keyboard(with_cancel: bool = False):
    builder = InlineKeyboardBuilder()
    for city in POPULAR_CITIES:
        builder.button(text=city, callback_data=f"city:{city}")
    builder.button(text="✏️ Другой город", callback_data="city_other")
    if with_cancel:
        builder.button(text="❌ Отмена", callback_data="cancel_edit")
    builder.adjust(2)
    return builder.as_markup()

def interests_keyboard(selected: Set[str], prefix: str, with_cancel: bool = False):
    builder = InlineKeyboardBuilder()
    for tag in INTERESTS_LIST:
        mark = "✅ " if tag in selected else ""
        builder.button(text=f"{mark}{tag}", callback_data=f"{prefix}:{INTERESTS_LIST.index(tag)}")
    builder.button(text="➡️ Готово", callback_data=f"{prefix}:done")
    if with_cancel:
        builder.button(text="❌ Отмена", callback_data="cancel_edit")
    builder.adjust(2)
    return builder.as_markup()

def menu_keyboard(likes_count: int = 0):
    builder = InlineKeyboardBuilder()
    builder.button(text="🔍 Смотреть анкеты", callback_data="search_menu")
    builder.button(text="👤 Моя анкета", callback_data="my_profile")
    builder.button(text="💑 Мои мэтчи", callback_data="my_matches")
    if likes_count > 0:
        builder.button(text=f"💌 Вам понравились: {likes_count}", callback_data="show_likes")
    builder.adjust(2, 2 if likes_count > 0 else 1)
    return builder.as_markup()

def back_to_menu_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 В меню", callback_data="menu")
    return builder.as_markup()

def no_profiles_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Показать анкеты заново", callback_data="reset_views")
    builder.button(text="🔙 В меню", callback_data="menu")
    builder.adjust(1, 1)
    return builder.as_markup()

def search_menu_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🏢 Только мой город", callback_data="search_local")
    builder.button(text="🌍 Везде (сначала мой город)", callback_data="search_global")
    builder.button(text="🔙 В меню", callback_data="menu")
    builder.adjust(1, 1, 1)
    return builder.as_markup()

def profile_keyboard(profile_id: int, mode: str):
    builder = InlineKeyboardBuilder()
    builder.button(text="❤️ Лайк", callback_data=f"like:{profile_id}:{mode}")
    builder.button(text="🌟 Суперлайк", callback_data=f"superlike:{profile_id}:{mode}")
    builder.button(text="👎 Пропустить", callback_data=f"dislike:{profile_id}:{mode}")
    builder.button(text="🚫 Блокировать", callback_data=f"block:{profile_id}:{mode}")
    builder.button(text="🚩 Пожаловаться", callback_data=f"report:{profile_id}:{mode}")
    builder.button(text="🔚 В меню", callback_data="menu")
    builder.adjust(2, 2, 1, 1)
    return builder.as_markup()

def profile_like_keyboard(profile_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="❤️ Ответить взаимностью", callback_data=f"like:{profile_id}:likes")
    builder.button(text="👎 Пропустить", callback_data=f"dislike:{profile_id}:likes")
    builder.button(text="🚫 Блокировать", callback_data=f"block:{profile_id}:likes")
    builder.button(text="🚩 Пожаловаться", callback_data=f"report:{profile_id}:likes")
    builder.adjust(2, 2)
    return builder.as_markup()

def edit_profile_keyboard(is_paused: bool):
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Имя", callback_data="edit_name")
    builder.button(text="✏️ Возраст", callback_data="edit_age")
    builder.button(text="✏️ Кого ищу", callback_data="edit_search_gender")
    builder.button(text="✏️ Диапазон возраста", callback_data="edit_age_range")
    builder.button(text="✏️ Город", callback_data="edit_city")
    builder.button(text="✏️ Интересы", callback_data="edit_interests")
    builder.button(text="✏️ О себе", callback_data="edit_bio")
    builder.button(text="🖼 Фото", callback_data="edit_photos_menu")
    pause_text = "▶️ Возобновить показ" if is_paused else "⏸ Поставить на паузу"
    builder.button(text=pause_text, callback_data="toggle_pause")
    builder.button(text="🗑 Удалить анкету", callback_data="delete_profile")
    builder.button(text="🔙 Назад", callback_data="my_profile")
    builder.adjust(2, 2, 2, 1, 1, 1)
    return builder.as_markup()

def edit_photos_menu_keyboard(photos_count: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="🔁 Заменить главное фото", callback_data="edit_photo")
    if photos_count < MAX_EXTRA_PHOTOS:
        builder.button(text="➕ Добавить фото", callback_data="edit_add_photo")
    if photos_count > 0:
        builder.button(text="➖ Удалить доп. фото", callback_data="edit_remove_photo")
    builder.button(text="🔙 Назад", callback_data="edit_profile")
    builder.adjust(1)
    return builder.as_markup()

def remove_photo_keyboard(photos: list):
    builder = InlineKeyboardBuilder()
    for idx, photo in enumerate(photos, start=1):
        builder.button(text=f"🗑 Фото {idx}", callback_data=f"remove_photo:{photo['id']}")
    builder.button(text="🔙 Назад", callback_data="edit_photos_menu")
    builder.adjust(1)
    return builder.as_markup()

def cancel_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="cancel_edit")
    return builder.as_markup()

def cancel_report_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="cancel_report")
    return builder.as_markup()

def report_reason_keyboard(profile_id: int, mode: str):
    builder = InlineKeyboardBuilder()
    for code, label in REPORT_REASONS:
        builder.button(text=label, callback_data=f"reportreason:{code}:{profile_id}:{mode}")
    builder.button(text="❌ Отмена", callback_data="cancel_report")
    builder.adjust(1)
    return builder.as_markup()

def match_keyboard(target_user_id: int, revealed: bool):
    builder = InlineKeyboardBuilder()
    if revealed:
        builder.button(text="✉️ Написать сообщение", url=f"tg://user?id={target_user_id}")
    else:
        builder.button(text="💬 Написать анонимно", callback_data=f"relay_open:{target_user_id}")
        builder.button(text="📇 Показать контакт", callback_data=f"reveal:{target_user_id}")
        builder.adjust(1)
    return builder.as_markup()

def admin_report_keyboard(report_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="🚫 Забанить", callback_data=f"admin_ban:{report_id}")
    builder.button(text="✅ Отклонить", callback_data=f"admin_reject:{report_id}")
    builder.adjust(2)
    return builder.as_markup()

def blocks_list_keyboard(blocks: list):
    builder = InlineKeyboardBuilder()
    for row in blocks:
        name = row["name"] if len(row["name"]) <= 25 else row["name"][:24] + "…"
        builder.button(text=f"🔓 {name}", callback_data=f"unblock:{row['user_id']}")
    builder.button(text="🔙 В меню", callback_data="menu")
    builder.adjust(1)
    return builder.as_markup()

def relay_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="📇 Показать контакт", callback_data="relay_reveal")
    builder.button(text="🚪 Выйти из чата", callback_data="relay_stop")
    builder.adjust(1)
    return builder.as_markup()

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
# РЕГИСТРАЦИЯ И БАЗОВЫЕ КОМАНДЫ
# ============================================================

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    logger.info("COMMAND /start | user_id=%s username=%r", message.from_user.id, message.from_user.username)

    await state.clear()
    user_id = message.from_user.id
    user = await get_user(user_id)

    if user:
        if user["username"] != message.from_user.username:
            await update_user_field(user_id, "username", message.from_user.username)

        if not user["accepted_rules_at"] or (user["accepted_rules_version"] or 0) < RULES_VERSION:
            await state.set_state(Registration.agreement)
            await state.update_data(reaccept=True)
            await message.answer(
                "Правила сервиса обновились. Пожалуйста, ознакомьтесь и подтвердите их:",
                reply_markup=rules_keyboard(),
            )
            return

        await message.answer("С возвращением! Вот ваша анкета:")
        await send_profile_card(message, user)
        await send_menu(message, user_id)
        return

    await state.set_state(Registration.agreement)
    await message.answer(
        "Добро пожаловать в бот знакомств!\n\n" + RULES_TEXT,
        reply_markup=rules_keyboard(),
    )

@router.message(Command("delete"))
async def cmd_delete(message: Message, state: FSMContext):
    await state.clear()
    await delete_user(message.from_user.id)
    await message.answer(
        "Ваша анкета, фотографии, лайки, мэтчи, блокировки и жалобы удалены.\n\n"
        "ℹ️ Запись о бане (если он был наложен администрацией) сохраняется "
        "в целях безопасности сервиса и не связана с вашей анкетой.\n\n"
        "Для новой регистрации напишите /start."
    )

@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "ℹ️ <b>Справка</b>\n\n"
        "Бот помогает найти людей для знакомства: смотрите анкеты, ставьте лайки, а при взаимной симпатии получите мэтч и сможете общаться.\n\n"
        "<b>Команды:</b>\n"
        "/start — показать анкету и главное меню\n"
        "/help — эта справка\n"
        "/delete — удалить анкету и связанные пользовательские данные\n"
        "/mydata — выгрузить свои данные файлом\n\n" + RULES_TEXT
    )

@router.message(Command("mydata"))
async def cmd_mydata(message: Message):
    user = await get_user(message.from_user.id)
    if not user:
        await message.answer("Анкета не найдена.")
        return

    photos = await get_photos(message.from_user.id)
    matches = await get_matches(message.from_user.id)
    my_likes = await fetch_all(
        "SELECT liked_id FROM likes WHERE liker_id = ?", (message.from_user.id,)
    )
    my_reports = await fetch_all(
        "SELECT reported_id, reason, status, timestamp FROM reports WHERE reporter_id = ?",
        (message.from_user.id,),
    )

    payload = {
        "profile": dict(user),
        "extra_photos": [row["file_id"] for row in photos],
        "matches": [{"user_id": m["user_id"], "name": m["name"]} for m in matches],
        "likes_given_to": [row["liked_id"] for row in my_likes],
        "reports_made": [dict(r) for r in my_reports],
        "exported_at": datetime.now(timezone.utc).isoformat(),
    }

    buffer = BytesIO(json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8"))
    document = BufferedInputFile(buffer.read(), filename=f"my_data_{message.from_user.id}.json")
    await message.answer_document(document, caption="Ваши данные во вложении.")

@router.message(Command("unban"))
async def cmd_unban(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        u_id = int(message.text.split()[1])
    except (IndexError, ValueError):
        await message.answer("Использование: /unban <user_id>")
        return
    if await unban_user(u_id):
        await message.answer(f"✅ Пользователь {u_id} разбанен.")
    else:
        await message.answer(f"❌ Пользователь {u_id} не найден в бан-листе.")

@router.message(Command("stats"))
async def cmd_stats(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Команда доступна только администратору.")
        return
    s = await get_stats()
    await message.answer(
        "📊 <b>Статистика бота</b>\n\n"
        f"👥 Анкет: {s['users_total']} (активных: {s['users_active']})\n"
        f"❤️ Лайков: {s['likes']}\n"
        f"💑 Мэтчей: {s['matches']}\n"
        f"👀 Обработанных анкет: {s['views']}\n"
        f"🚫 Блокировок: {s['blocks']}\n"
        f"⛔ Банов: {s['bans']}\n"
        f"🚩 Жалоб в ожидании: {s['reports_pending']}"
    )

@router.message(Command("pending"))
async def cmd_pending(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    reports = await get_pending_reports(limit=5, offset=0)
    if not reports:
        await message.answer("Нет жалоб в ожидании рассмотрения.")
        return
    for r in reports:
        text = (
            f"🚨 <b>Жалоба #{r['id']}</b>\n"
            f"От: {escape(r['reporter_name'] or '—')} (<code>{r['reporter_id']}</code>)\n"
            f"На: {escape(r['reported_name'] or '—')} (<code>{r['reported_id']}</code>)\n"
            f"Причина: {escape(r['reason'])}\n"
            f"Дата: {r['timestamp']}"
        )
        await message.answer(text, reply_markup=admin_report_keyboard(r["id"]))

@router.message(Command("banned"))
async def cmd_banned(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    banned = await get_banned_users(limit=20)
    if not banned:
        await message.answer("Бан-лист пуст.")
        return
    lines = [f"<code>{row['user_id']}</code> — {escape(row['reason'])} ({row['created_at']})" for row in banned]
    await message.answer("⛔ <b>Забаненные пользователи (последние 20):</b>\n\n" + "\n".join(lines))

@router.message(Command("finduser"))
async def cmd_finduser(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        target_id = int(message.text.split()[1])
    except (IndexError, ValueError):
        await message.answer("Использование: /finduser <user_id>")
        return

    info = await get_user_admin_info(target_id)
    user = info["user"]
    if not user:
        await message.answer("Пользователь с анкетой не найден.")
        return

    text = (
        f"👤 <b>{escape(user['name'])}</b> (<code>{target_id}</code>)\n"
        f"Возраст: {user['age']}, пол: {escape(user['gender'])}\n"
        f"Город: {escape(user['city'])}\n"
        f"Активна: {'да' if user['is_active'] else 'нет'}, на паузе: {'да' if user['is_paused'] else 'нет'}\n"
        f"Забанен: {'да' if info['banned'] else 'нет'}\n"
        f"Жалоб на него: {info['reports_against']}, жалоб от него: {info['reports_by']}"
    )
    await message.answer(text)

@router.callback_query(Registration.agreement, F.data == "agree_rules")
async def registration_agreement(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    accepted_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    if (await state.get_data()).get("reaccept"):
        await set_rules_accepted(callback.from_user.id, accepted_at)
        await state.clear()
        await callback.message.edit_text("Спасибо! Правила приняты. Приятного пользования! 💖")
        user = await get_user(callback.from_user.id)
        if user:
            await send_profile_card(callback.message, user)
        await send_menu(callback.message, callback.from_user.id)
        return

    await state.set_state(Registration.name)
    await state.update_data(accepted_rules_at=accepted_at)
    await callback.message.edit_text(
        f"Как вас зовут?\nВведите имя от {MIN_NAME_LENGTH} до {MAX_NAME_LENGTH} символов."
    )

@router.message(Registration.name, F.text)
async def registration_name(message: Message, state: FSMContext):
    name = validate_text(message.text, MIN_NAME_LENGTH, MAX_NAME_LENGTH)
    if not name:
        await message.answer(f"Имя должно содержать от {MIN_NAME_LENGTH} до {MAX_NAME_LENGTH} символов.")
        return

    await state.update_data(name=name)
    await state.set_state(Registration.age)
    await message.answer(f"Сколько вам лет?\nВведите число от {MIN_AGE} до {MAX_AGE}.")

@router.message(Registration.age, F.text)
async def registration_age(message: Message, state: FSMContext):
    age = validate_age(message.text)
    if age is None:
        await message.answer(f"Введите корректный возраст: число от {MIN_AGE} до {MAX_AGE}.")
        return
    await state.update_data(age=age)
    await state.set_state(Registration.gender)
    await message.answer("Укажите ваш пол:", reply_markup=gender_keyboard().as_markup())

@router.callback_query(Registration.gender, F.data.startswith("gender_"))
async def registration_gender(callback: CallbackQuery, state: FSMContext):
    code = callback.data.split("_", maxsplit=1)[1] if "_" in callback.data else None
    gender = gender_text(code)
    if not gender:
        await callback.answer("Некорректный вариант.", show_alert=True)
        return
    await callback.answer()
    await state.update_data(gender=gender)
    await state.set_state(Registration.search_gender)
    await callback.message.edit_text(
        f"Ваш пол: <b>{escape(gender)}</b>\n\nКого вы ищете?",
        reply_markup=search_gender_keyboard().as_markup(),
    )

@router.callback_query(Registration.search_gender, F.data.startswith("search_gender_"))
async def registration_search_gender(callback: CallbackQuery, state: FSMContext):
    code = callback.data.split("_", maxsplit=2)[2] if len(callback.data.split("_")) > 2 else None
    sg = search_gender_text(code)
    if not sg:
        await callback.answer("Некорректный вариант.", show_alert=True)
        return
    await callback.answer()
    await state.update_data(search_gender=sg)
    await state.set_state(Registration.age_range)
    await callback.message.edit_text(
        f"Вы ищете: <b>{escape(sg)}</b>\n\n"
        f"Какой возрастной диапазон вас интересует?\n"
        f"Введите в формате <code>18-35</code> (от {MIN_AGE} до {MAX_AGE})."
    )

@router.message(Registration.age_range, F.text)
async def registration_age_range(message: Message, state: FSMContext):
    parsed = validate_age_range(message.text)
    if not parsed:
        await message.answer("Введите диапазон в формате <code>18-35</code>.")
        return
    age_min, age_max = parsed
    await state.update_data(age_min=age_min, age_max=age_max)
    await state.set_state(Registration.city)
    await message.answer(
        f"Диапазон: <b>{age_min}-{age_max}</b>\n\nВыберите город или введите свой:",
        reply_markup=city_keyboard(),
    )

@router.callback_query(Registration.city, F.data.startswith("city:"))
async def registration_city_quick(callback: CallbackQuery, state: FSMContext):
    city = callback.data.split(":", 1)[1]
    await callback.answer()
    await state.update_data(city=city)
    await state.set_state(Registration.interests)
    await callback.message.edit_text(
        f"Город: <b>{escape(city)}</b>\n\n"
        f"Выберите до {MAX_INTERESTS} интересов (можно пропустить, нажав «Готово»):",
        reply_markup=interests_keyboard(set(), "reg_int"),
    )

@router.callback_query(Registration.city, F.data == "city_other")
async def registration_city_other(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.edit_text(
        f"Введите название города текстом.\nОт {MIN_CITY_LENGTH} до {MAX_CITY_LENGTH} символов."
    )

@router.message(Registration.city, F.text)
async def registration_city(message: Message, state: FSMContext):
    city = validate_text(message.text, MIN_CITY_LENGTH, MAX_CITY_LENGTH)
    if not city:
        await message.answer(f"Название города должно содержать от {MIN_CITY_LENGTH} до {MAX_CITY_LENGTH} символов.")
        return
    await state.update_data(city=city)
    await state.set_state(Registration.interests)
    await message.answer(
        f"Город: <b>{escape(city)}</b>\n\n"
        f"Выберите до {MAX_INTERESTS} интересов (можно пропустить, нажав «Готово»):",
        reply_markup=interests_keyboard(set(), "reg_int"),
    )

@router.callback_query(Registration.interests, F.data.startswith("reg_int:"))
async def registration_interests_toggle(callback: CallbackQuery, state: FSMContext):
    payload = callback.data.split(":", 1)[1]
    data = await state.get_data()
    selected: Set[str] = set(data.get("interests_selected", []))

    if payload == "done":
        await callback.answer()
        await state.update_data(interests=serialize_interests(selected))
        await state.set_state(Registration.bio)
        await callback.message.edit_text(
            f"Расскажите немного о себе: от {MIN_BIO_LENGTH} до {MAX_BIO_LENGTH} символов."
        )
        return

    try:
        idx = int(payload)
        tag = INTERESTS_LIST[idx]
    except (ValueError, IndexError):
        await callback.answer("Некорректный вариант.", show_alert=True)
        return

    if tag in selected:
        selected.discard(tag)
    elif len(selected) < MAX_INTERESTS:
        selected.add(tag)
    else:
        await callback.answer(f"Можно выбрать не более {MAX_INTERESTS}.", show_alert=True)
        return

    await state.update_data(interests_selected=list(selected))
    await callback.answer()
    try:
        await callback.message.edit_reply_markup(reply_markup=interests_keyboard(selected, "reg_int"))
    except TelegramBadRequest:
        pass

@router.message(Registration.bio, F.text)
async def registration_bio(message: Message, state: FSMContext):
    bio = validate_text(message.text, MIN_BIO_LENGTH, MAX_BIO_LENGTH)
    if not bio:
        await message.answer(f"Описание должно содержать от {MIN_BIO_LENGTH} до {MAX_BIO_LENGTH} символов.")
        return
    await state.update_data(bio=bio)
    await state.set_state(Registration.photo)
    await message.answer("Отлично. Теперь отправьте вашу главную фотографию.")

@router.message(Registration.photo, F.photo)
async def registration_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    photo_id = message.photo[-1].file_id
    ok = await add_user_to_db(
        user_id=message.from_user.id,
        name=data["name"],
        age=data["age"],
        gender=data["gender"],
        search_gender=data["search_gender"],
        age_min=data["age_min"],
        age_max=data["age_max"],
        city=data["city"],
        interests=data.get("interests", ""),
        bio=data["bio"],
        photo_id=photo_id,
        username=message.from_user.username,
        accepted_rules_at=data.get("accepted_rules_at"),
    )
    if not ok:
        await state.clear()
        await message.answer("Вы заблокированы и не можете зарегистрироваться.")
        return

    await state.set_state(Registration.extra_photos)
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Готово", callback_data="extra_photos_done")
    await message.answer(
        f"Главное фото сохранено. Хотите добавить ещё фото в галерею "
        f"(до {MAX_EXTRA_PHOTOS} шт.)? Отправьте фото или нажмите «Готово».",
        reply_markup=builder.as_markup(),
    )

@router.message(Registration.photo)
async def registration_photo_invalid(message: Message):
    await message.answer("Пожалуйста, отправьте изображение именно как фотографию.")

@router.message(Registration.extra_photos, F.photo)
async def registration_extra_photo(message: Message, state: FSMContext):
    added = await add_photo(message.from_user.id, message.photo[-1].file_id)
    photos = await get_photos(message.from_user.id)

    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Готово", callback_data="extra_photos_done")

    if not added:
        await message.answer(
            f"Достигнут лимит в {MAX_EXTRA_PHOTOS} доп. фото. Нажмите «Готово».",
            reply_markup=builder.as_markup(),
        )
        return

    await message.answer(
        f"Фото добавлено ({len(photos)}/{MAX_EXTRA_PHOTOS}). Можно добавить ещё или нажать «Готово».",
        reply_markup=builder.as_markup(),
    )

@router.callback_query(Registration.extra_photos, F.data == "extra_photos_done")
async def registration_extra_photos_done(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await callback.message.edit_text("Регистрация успешно завершена! 🎉")
    await callback.message.answer("Главное меню:", reply_markup=menu_keyboard())

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
        await callback.answer("Пожалуйста, подождите.")
        return
    async with lock:
        await callback.answer()
        await show_profile(callback, state, mode="local")

@router.callback_query(F.data == "search_global")
async def search_global(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    lock = get_user_lock(uid)
    if lock.locked():
        await callback.answer("Пожалуйста, подождите.")
        return
    async with lock:
        await callback.answer()
        await show_profile(callback, state, mode="global")

@router.callback_query(F.data == "reset_views")
async def reset_views(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    lock = get_user_lock(uid)
    if lock.locked():
        await callback.answer("Пожалуйста, подождите.")
        return
    async with lock:
        user = await get_user(uid)
        if not user or not user["is_active"]:
            await callback.answer("Ваша анкета неактивна. Обновите фото через 'Моя анкета'.", show_alert=True)
            return
        async with transaction():
            await db.execute("DELETE FROM views WHERE viewer_id = ?", (uid,))
        await callback.answer("История показов сброшена. Лайкнутые, мэтчи, блокировки и жалобы не появятся повторно.")
        await show_profile(callback, state, mode="global")

@router.callback_query(F.data.startswith("like:"))
async def like_profile(callback: CallbackQuery, state: FSMContext):
    parsed = parse_profile_callback(callback.data, "like")
    if not parsed:
        await callback.answer("Некорректные данные.", show_alert=True)
        return
    liked_id, mode = parsed
    uid = callback.from_user.id

    lock = get_user_lock(uid)
    if lock.locked():
        await callback.answer("Пожалуйста, подождите.")
        return

    async with lock:
        if liked_id == uid:
            await callback.answer("Нельзя поставить лайк самому себе.", show_alert=True)
            return
        if not await validate_active_card(callback, state, liked_id, mode):
            return await handle_stale_callback(callback)

        other_user = await get_user(liked_id)
        if not other_user or await check_ban(liked_id) or await are_users_blocked(uid, liked_id):
            await callback.answer("Эта анкета больше недоступна.", show_alert=True)
            await delete_callback_message(callback)
            await show_next(callback, state, mode)
            return

        await callback.answer()
        result = await add_like(uid, liked_id)
        await delete_callback_message(callback)
        await _handle_like_result(callback, state, mode, uid, liked_id, other_user, result)

@router.callback_query(F.data.startswith("superlike:"))
async def superlike_profile(callback: CallbackQuery, state: FSMContext):
    parsed = parse_profile_callback(callback.data, "superlike")
    if not parsed:
        await callback.answer("Некорректные данные.", show_alert=True)
        return
    liked_id, mode = parsed
    uid = callback.from_user.id

    lock = get_user_lock(uid)
    if lock.locked():
        await callback.answer("Пожалуйста, подождите.")
        return

    async with lock:
        if liked_id == uid:
            await callback.answer("Нельзя поставить суперлайк самому себе.", show_alert=True)
            return
        if not await validate_active_card(callback, state, liked_id, mode):
            return await handle_stale_callback(callback)

        other_user = await get_user(liked_id)
        if not other_user or await check_ban(liked_id) or await are_users_blocked(uid, liked_id):
            await callback.answer("Эта анкета больше недоступна.", show_alert=True)
            await delete_callback_message(callback)
            await show_next(callback, state, mode)
            return

        allowed, result = await add_superlike(uid, liked_id)
        if not allowed:
            await callback.answer(
                f"Суперлайк можно использовать {SUPERLIKE_DAILY_LIMIT} раз(а) в сутки.", show_alert=True
            )
            return

        await callback.answer("🌟 Суперлайк отправлен!")
        await delete_callback_message(callback)

        current_user = await get_user(uid)
        if current_user and result != LikeResult.MATCHED:
            try:
                await bot.send_message(
                    liked_id,
                    f"🌟 <b>{escape(current_user['name'])}</b> поставил(а) вам суперлайк! "
                    f"Загляните в раздел «Вам понравились» в меню.",
                )
            except (TelegramBadRequest, TelegramForbiddenError):
                pass

        await _handle_like_result(callback, state, mode, uid, liked_id, other_user, result)

async def _handle_like_result(callback, state, mode, uid, liked_id, other_user, result):
    if result == LikeResult.REJECTED:
        await callback.message.answer("Не удалось поставить лайк (блокировка или бан).")
        await show_next(callback, state, mode)
        return

    if result == LikeResult.ALREADY_MATCHED:
        await callback.message.answer("Вы уже в мэтчах с этим пользователем.")
        await show_next(callback, state, mode)
        return

    if result == LikeResult.LIKED:
        await show_next(callback, state, mode)
        return

    current_user = await get_user(uid)
    if not current_user:
        await show_next(callback, state, mode)
        return

    try:
        await callback.message.answer(
            f"💖 <b>Это мэтч!</b>\n\nВы понравились друг другу с {escape(other_user['name'])}.",
            reply_markup=match_keyboard(liked_id, revealed=False),
        )
    except (TelegramBadRequest, TelegramForbiddenError):
        logger.warning("Match send fail to %s", uid)

    try:
        await bot.send_message(
            liked_id,
            f"💖 <b>Это мэтч!</b>\n\nВы понравились друг другу с {escape(current_user['name'])}.",
            reply_markup=match_keyboard(uid, revealed=False),
        )
    except (TelegramBadRequest, TelegramForbiddenError):
        logger.warning("Match send fail to %s", liked_id)

    await show_next(callback, state, mode)

@router.callback_query(F.data.startswith("dislike:"))
async def dislike_profile(callback: CallbackQuery, state: FSMContext):
    parsed = parse_profile_callback(callback.data, "dislike")
    if not parsed:
        await callback.answer("Некорректные данные.", show_alert=True)
        return
    p_id, mode = parsed
    uid = callback.from_user.id

    lock = get_user_lock(uid)
    if lock.locked():
        await callback.answer("Пожалуйста, подождите.")
        return

    async with lock:
        if not await validate_active_card(callback, state, p_id, mode):
            return await handle_stale_callback(callback)

        if p_id != uid and await get_user(p_id):
            if mode == "likes":
                await reject_like(uid, p_id)
            else:
                await add_view(uid, p_id)
        await callback.answer()
        await delete_callback_message(callback)
        await show_next(callback, state, mode)

@router.callback_query(F.data.startswith("block:"))
async def block_profile(callback: CallbackQuery, state: FSMContext):
    parsed = parse_profile_callback(callback.data, "block")
    if not parsed:
        await callback.answer("Некорректные данные.", show_alert=True)
        return
    blocked_id, mode = parsed
    uid = callback.from_user.id

    lock = get_user_lock(uid)
    if lock.locked():
        await callback.answer("Пожалуйста, подождите.")
        return

    async with lock:
        if not await validate_active_card(callback, state, blocked_id, mode):
            return await handle_stale_callback(callback)

        if blocked_id == uid:
            await callback.answer("Нельзя заблокировать самого себя.", show_alert=True)
            return
        if not await get_user(blocked_id):
            await callback.answer("Анкета недоступна.", show_alert=True)
            return

        if not await block_user(uid, blocked_id):
            await callback.answer("Не удалось заблокировать пользователя.", show_alert=True)
            return

        invalidate_likes_cache(uid, blocked_id)
        await callback.answer("Пользователь заблокирован. Все лайки и мэтчи удалены.")
        await delete_callback_message(callback)
        await show_next(callback, state, mode)

@router.callback_query(F.data == "show_likes")
async def show_likes(callback: CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    lock = get_user_lock(uid)
    if lock.locked():
        await callback.answer("Пожалуйста, подождите.")
        return
    async with lock:
        await callback.answer()
        user = await get_user(uid)
        if not user or not user["is_active"]:
            await safe_edit_or_send(callback, "Ваша анкета неактивна.", reply_markup=back_to_menu_keyboard())
            return
        await show_next_liker(callback, state)

# ============================================================
# БЛОКИРОВКИ ПОЛЬЗОВАТЕЛЯ (просмотр и снятие)
# ============================================================

@router.callback_query(F.data == "my_blocks")
async def my_blocks(callback: CallbackQuery):
    await callback.answer()
    blocks = await get_blocks(callback.from_user.id)
    if not blocks:
        await safe_edit_or_send(callback, "У вас нет заблокированных пользователей.", reply_markup=back_to_menu_keyboard())
        return
    await safe_edit_or_send(
        callback,
        f"🚫 <b>Заблокированные пользователи ({len(blocks)}):</b>\n\nНажмите, чтобы снять блокировку.",
        reply_markup=blocks_list_keyboard(blocks),
    )

@router.callback_query(F.data.startswith("unblock:"))
async def unblock_profile(callback: CallbackQuery):
    try:
        target_id = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректные данные.", show_alert=True)
        return

    ok = await unblock_user(callback.from_user.id, target_id)
    if ok:
        await callback.answer("Пользователь разблокирован.")
    else:
        await callback.answer("Уже не заблокирован.", show_alert=True)

    blocks = await get_blocks(callback.from_user.id)
    if not blocks:
        await safe_edit_or_send(callback, "У вас нет заблокированных пользователей.", reply_markup=back_to_menu_keyboard())
        return
    await safe_edit_or_send(
        callback,
        f"🚫 <b>Заблокированные пользователи ({len(blocks)}):</b>\n\nНажмите, чтобы снять блокировку.",
        reply_markup=blocks_list_keyboard(blocks),
    )

# ============================================================
# ЖАЛОБЫ (с кнопками причин и антиспам-лимитом) И АДМИН-БАН
# ============================================================

@router.callback_query(F.data.startswith("report:"))
async def start_report(callback: CallbackQuery, state: FSMContext):
    parsed = parse_profile_callback(callback.data, "report")
    if not parsed:
        await callback.answer("Некорректные данные.", show_alert=True)
        return
    rep_id, mode = parsed
    uid = callback.from_user.id

    lock = get_user_lock(uid)
    if lock.locked():
        await callback.answer("Пожалуйста, подождите.")
        return

    async with lock:
        if not await validate_active_card(callback, state, rep_id, mode):
            return await handle_stale_callback(callback)

        if rep_id == uid:
            await callback.answer("Нельзя пожаловаться на самого себя.", show_alert=True)
            return
        if not await get_user(rep_id):
            await callback.answer("Анкета не найдена.", show_alert=True)
            return
        if await report_exists(uid, rep_id):
            await callback.answer("Вы уже отправляли жалобу на этого пользователя.", show_alert=True)
            return
        if await reports_today_count(uid) >= REPORT_DAILY_LIMIT:
            await callback.answer(
                f"Превышен лимит жалоб ({REPORT_DAILY_LIMIT} в сутки). Попробуйте завтра.", show_alert=True
            )
            return

        await callback.answer()
        await callback.message.answer(
            "Выберите причину жалобы:",
            reply_markup=report_reason_keyboard(rep_id, mode),
        )

@router.callback_query(F.data.startswith("reportreason:"))
async def choose_report_reason(callback: CallbackQuery, state: FSMContext):
    try:
        _, code, raw_id, mode = callback.data.split(":")
        rep_id = int(raw_id)
    except (ValueError, IndexError):
        await callback.answer("Некорректные данные.", show_alert=True)
        return

    if mode not in VALID_MODES or code not in REPORT_REASON_TEXT:
        await callback.answer("Некорректные данные.", show_alert=True)
        return

    uid = callback.from_user.id

    if await report_exists(uid, rep_id):
        await callback.answer("Вы уже отправляли жалобу на этого пользователя.", show_alert=True)
        return
    if await reports_today_count(uid) >= REPORT_DAILY_LIMIT:
        await callback.answer(f"Превышен лимит жалоб ({REPORT_DAILY_LIMIT} в сутки).", show_alert=True)
        return

    await callback.answer()

    if code == "other":
        await state.set_state(Report.reason)
        await state.update_data(reported_id=rep_id, mode=mode)
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest:
            pass
        await callback.message.answer(
            f"Опишите причину жалобы текстом.\nМаксимальная длина: {MAX_REPORT_LENGTH} символов.",
            reply_markup=cancel_report_keyboard(),
        )
        return

    reason_text = REPORT_REASON_TEXT[code]
    await _finalize_report(callback.message, uid, rep_id, reason_text)

@router.message(Report.reason, F.text)
async def process_report(message: Message, state: FSMContext):
    reason = validate_text(message.text, 1, MAX_REPORT_LENGTH)
    if not reason:
        await message.answer(f"Жалоба не должна быть пустой и не может превышать {MAX_REPORT_LENGTH} символов.")
        return

    data = await state.get_data()
    rep_id = data.get("reported_id")
    if not isinstance(rep_id, int):
        await state.clear()
        await message.answer("Ошибка данных. Попробуйте ещё раз.")
        return

    await state.clear()
    await _finalize_report(message, message.from_user.id, rep_id, reason)

async def _finalize_report(message: Message, uid: int, rep_id: int, reason: str) -> None:
    r_id = await add_report(uid, rep_id, reason)
    if r_id is None:
        await message.answer(
            "Не удалось отправить жалобу (пользователь недоступен, вы уже жаловались или превышен лимит).",
            reply_markup=menu_keyboard(await get_likes_count(uid)),
        )
        return

    rep_user = await get_user(rep_id)
    if not rep_user:
        await message.answer(
            "Пользователь удалил анкету сразу после жалобы.",
            reply_markup=menu_keyboard(await get_likes_count(uid)),
        )
        return

    await add_view(uid, rep_id)
    await message.answer("Жалоба отправлена администрации. Спасибо!", reply_markup=menu_keyboard(await get_likes_count(uid)))

    rep_name = escape(rep_user["name"])
    rep_un = f"@{escape(rep_user['username'])}" if rep_user["username"] else "нет username"
    text = (
        f"🚨 <b>Новая жалоба #{r_id}</b>\n\n"
        f"<b>От кого:</b> ID <code>{uid}</code>\n"
        f"<b>На кого:</b> {rep_name} (ID: <code>{rep_id}</code>, {rep_un})\n\n"
        f"<b>Причина:</b>\n{escape(reason)}"
    )

    try:
        if rep_user["photo_id"]:
            await bot.send_photo(ADMIN_ID, photo=rep_user["photo_id"], caption=text, reply_markup=admin_report_keyboard(r_id))
        else:
            await bot.send_message(ADMIN_ID, text, reply_markup=admin_report_keyboard(r_id))
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        logger.exception("Fail send report to admin: %s", e)

@router.message(Report.reason)
async def report_invalid(message: Message):
    await message.answer("Отправьте причину жалобы текстом.")

@router.callback_query(Report.reason, F.data == "cancel_report")
async def cancel_report(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    try:
        await callback.message.delete()
    except TelegramBadRequest:
        pass
    await callback.message.answer("Жалоба отменена.", reply_markup=menu_keyboard(await get_likes_count(callback.from_user.id)))

@router.callback_query(F.data == "cancel_report")
async def cancel_report_no_state(callback: CallbackQuery):
    await callback.answer()
    try:
        await callback.message.delete()
    except TelegramBadRequest:
        pass

@router.callback_query(F.data.startswith("admin_ban:"))
async def admin_ban_user(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await handle_stale_callback(callback, "Недостаточно прав.")
    try:
        _, raw_rid = callback.data.split(":")
        r_id = int(raw_rid)
    except (ValueError, AttributeError):
        return await handle_stale_callback(callback, "Некорректные данные.")

    reported_id = await ban_user_from_report(r_id, callback.from_user.id)
    if not reported_id:
        return await handle_stale_callback(callback, "Эта жалоба уже обработана.")

    await callback.answer("Пользователь заблокирован. Жалоба закрыта.")
    original = extract_html(callback.message)
    new_caption = f"{original}\n\n✅ Пользователь <code>{reported_id}</code> забанен."
    try:
        await callback.message.edit_caption(caption=new_caption, reply_markup=None)
    except TelegramBadRequest:
        try:
            await callback.message.edit_text(new_caption, reply_markup=None)
        except TelegramBadRequest:
            pass
    try:
        await bot.send_message(reported_id, "Вы были заблокированы администрацией. Для удаления данных используйте /delete.")
    except (TelegramBadRequest, TelegramForbiddenError):
        pass

@router.callback_query(F.data.startswith("admin_reject:"))
async def admin_reject_report(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await handle_stale_callback(callback, "Недостаточно прав.")
    try:
        _, raw_rid = callback.data.split(":")
        r_id = int(raw_rid)
    except (ValueError, AttributeError):
        return await handle_stale_callback(callback, "Некорректные данные.")

    updated = await reject_report(r_id, callback.from_user.id)
    if not updated:
        return await handle_stale_callback(callback, "Эта жалоба уже обработана.")

    await callback.answer("Жалоба отклонена.")
    original = extract_html(callback.message)
    new_caption = f"{original}\n\n❌ Жалоба #{r_id} отклонена."
    try:
        await callback.message.edit_caption(caption=new_caption, reply_markup=None)
    except TelegramBadRequest:
        try:
            await callback.message.edit_text(new_caption, reply_markup=None)
        except TelegramBadRequest:
            pass

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

@router.callback_query(F.data.startswith("gallery:"))
async def view_gallery(callback: CallbackQuery):
    try:
        index = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        index = 0

    user = await get_user(callback.from_user.id)
    if not user:
        await callback.answer("Анкета не найдена.", show_alert=True)
        return

    photos = await get_photos(callback.from_user.id)
    all_photos = [user["photo_id"]] + [p["file_id"] for p in photos]

    if not all_photos:
        await callback.answer("Фото нет.", show_alert=True)
        return

    index = index % len(all_photos)

    builder = InlineKeyboardBuilder()
    builder.button(text="◀️", callback_data=f"gallery:{(index - 1) % len(all_photos)}")
    builder.button(text=f"{index + 1}/{len(all_photos)}", callback_data="noop")
    builder.button(text="▶️", callback_data=f"gallery:{(index + 1) % len(all_photos)}")
    builder.button(text="🔙 Назад", callback_data="my_profile")
    builder.adjust(3, 1)

    await callback.answer()
    try:
        await bot.edit_message_media(
            chat_id=callback.message.chat.id,
            message_id=callback.message.message_id,
            media={"type": "photo", "media": all_photos[index]},
            reply_markup=builder.as_markup(),
        )
    except TelegramBadRequest:
        await callback.message.answer_photo(all_photos[index], reply_markup=builder.as_markup())

@router.callback_query(F.data == "noop")
async def noop_callback(callback: CallbackQuery):
    await callback.answer()

@router.callback_query(F.data == "my_matches")
async def my_matches(callback: CallbackQuery):
    await callback.answer()
    matches = await get_matches(callback.from_user.id)
    if not matches:
        await safe_edit_or_send(callback, "У вас пока нет мэтчей. Всё ещё впереди — продолжайте искать! 💘", reply_markup=back_to_menu_keyboard())
        return
    builder = InlineKeyboardBuilder()
    for m in matches[:50]:
        name = m["name"] if len(m["name"]) <= 30 else m["name"][:29] + "…"
        revealed = await is_fully_revealed(callback.from_user.id, m["user_id"])
        if revealed:
            builder.button(text=f"✉️ {name}", url=f"tg://user?id={m['user_id']}")
        else:
            builder.button(text=f"💬 {name}", callback_data=f"relay_open:{m['user_id']}")
    builder.button(text="🔙 В меню", callback_data="menu")
    builder.adjust(1)
    await safe_edit_or_send(
        callback,
        f"💑 <b>Ваши мэтчи ({len(matches)}):</b>\n\nНажмите на имя, чтобы написать человеку.",
        reply_markup=builder.as_markup(),
    )

@router.callback_query(F.data == "edit_profile")
async def edit_profile_menu(callback: CallbackQuery):
    await callback.answer()
    user = await get_user(callback.from_user.id)
    await delete_callback_message(callback)
    await callback.message.answer(
        "Что вы хотите изменить?",
        reply_markup=edit_profile_keyboard(bool(user["is_paused"]) if user else False),
    )

@router.callback_query(F.data == "toggle_pause")
async def toggle_pause(callback: CallbackQuery):
    user = await get_user(callback.from_user.id)
    if not user:
        await callback.answer("Анкета не найдена.", show_alert=True)
        return
    new_state = not bool(user["is_paused"])
    await set_paused(callback.from_user.id, new_state)
    await callback.answer("Анкета поставлена на паузу." if new_state else "Показ анкеты возобновлён.")
    await callback.message.edit_reply_markup(reply_markup=edit_profile_keyboard(new_state))

@router.callback_query(F.data == "edit_photos_menu")
async def edit_photos_menu(callback: CallbackQuery):
    await callback.answer()
    photos = await get_photos(callback.from_user.id)
    await callback.message.edit_text(
        f"🖼 Фотографии: главное + {len(photos)}/{MAX_EXTRA_PHOTOS} доп.",
        reply_markup=edit_photos_menu_keyboard(len(photos)),
    )

@router.callback_query(F.data == "edit_add_photo")
async def edit_add_photo_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(EditProfile.add_photo)
    await callback.message.answer("Отправьте дополнительное фото.", reply_markup=cancel_keyboard())

@router.message(EditProfile.add_photo, F.photo)
async def edit_add_photo_save(message: Message, state: FSMContext):
    added = await add_photo(message.from_user.id, message.photo[-1].file_id)
    await state.clear()
    if added:
        await send_menu(message, message.from_user.id, "Фото добавлено в галерею.")
    else:
        await send_menu(message, message.from_user.id, f"Достигнут лимит в {MAX_EXTRA_PHOTOS} доп. фото.")

@router.message(EditProfile.add_photo)
async def edit_add_photo_invalid(message: Message):
    await message.answer("Пожалуйста, отправьте изображение именно как фотографию.")

@router.callback_query(F.data == "edit_remove_photo")
async def edit_remove_photo_menu(callback: CallbackQuery):
    await callback.answer()
    photos = await get_photos(callback.from_user.id)
    if not photos:
        await callback.answer("Нет дополнительных фото.", show_alert=True)
        return
    await callback.message.edit_text("Выберите фото для удаления:", reply_markup=remove_photo_keyboard(photos))

@router.callback_query(F.data.startswith("remove_photo:"))
async def remove_photo_callback(callback: CallbackQuery):
    try:
        photo_id = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректные данные.", show_alert=True)
        return

    ok = await delete_photo(callback.from_user.id, photo_id)
    await callback.answer("Фото удалено." if ok else "Не удалось удалить фото.")

    photos = await get_photos(callback.from_user.id)
    if not photos:
        await callback.message.edit_text(
            "Дополнительных фото больше нет.", reply_markup=edit_photos_menu_keyboard(0)
        )
        return
    await callback.message.edit_reply_markup(reply_markup=remove_photo_keyboard(photos))

@router.callback_query(F.data.in_({"edit_name", "edit_age", "edit_search_gender", "edit_age_range", "edit_city", "edit_interests", "edit_bio", "edit_photo"}))
async def edit_start(callback: CallbackQuery, state: FSMContext):
    action = callback.data
    await callback.answer()
    if action == "edit_search_gender":
        await state.set_state(EditProfile.search_gender)
        await callback.message.answer("Кого вы ищете?", reply_markup=search_gender_keyboard(with_cancel=True).as_markup())
    elif action == "edit_name":
        await state.set_state(EditProfile.name)
        await callback.message.answer(f"Введите новое имя: от {MIN_NAME_LENGTH} до {MAX_NAME_LENGTH} символов.", reply_markup=cancel_keyboard())
    elif action == "edit_age":
        await state.set_state(EditProfile.age)
        await callback.message.answer(f"Введите новый возраст: от {MIN_AGE} до {MAX_AGE}.", reply_markup=cancel_keyboard())
    elif action == "edit_age_range":
        await state.set_state(EditProfile.age_range)
        await callback.message.answer(
            f"Введите новый диапазон возраста поиска в формате <code>18-35</code>.", reply_markup=cancel_keyboard()
        )
    elif action == "edit_city":
        await state.set_state(EditProfile.city)
        await callback.message.answer("Выберите город или введите свой:", reply_markup=city_keyboard(with_cancel=True))
    elif action == "edit_interests":
        user = await get_user(callback.from_user.id)
        selected = parse_interests(user["interests"]) if user else set()
        await state.set_state(EditProfile.interests)
        await state.update_data(interests_selected=list(selected))
        await callback.message.answer(
            f"Выберите до {MAX_INTERESTS} интересов:",
            reply_markup=interests_keyboard(selected, "edit_int", with_cancel=True),
        )
    elif action == "edit_bio":
        await state.set_state(EditProfile.bio)
        await callback.message.answer(f"Введите новое описание: от {MIN_BIO_LENGTH} до {MAX_BIO_LENGTH} символов.", reply_markup=cancel_keyboard())
    elif action == "edit_photo":
        await state.set_state(EditProfile.photo)
        await callback.message.answer("Отправьте новую главную фотографию.", reply_markup=cancel_keyboard())

@router.callback_query(EditProfile.search_gender, F.data.startswith("search_gender_"))
async def edit_search_gender_save(callback: CallbackQuery, state: FSMContext):
    code = callback.data.split("_", maxsplit=2)[2] if len(callback.data.split("_")) > 2 else None
    sg = search_gender_text(code)
    if not sg:
        await callback.answer("Некорректный вариант.", show_alert=True)
        return
    await callback.answer()
    await update_user_field(callback.from_user.id, "search_gender", sg)
    await state.clear()
    await callback.message.edit_text(f"Предпочтения обновлены. Теперь вы ищете: <b>{escape(sg)}</b>.")
    await callback.message.answer("Главное меню:", reply_markup=menu_keyboard(await get_likes_count(callback.from_user.id)))

@router.message(EditProfile.name, F.text)
async def edit_name_save(message: Message, state: FSMContext):
    name = validate_text(message.text, MIN_NAME_LENGTH, MAX_NAME_LENGTH)
    if not name:
        await message.answer(f"Имя должно содержать от {MIN_NAME_LENGTH} до {MAX_NAME_LENGTH} символов.")
        return
    await update_user_field(message.from_user.id, "name", name)
    await state.clear()
    await send_menu(message, message.from_user.id, "Имя успешно обновлено.")

@router.message(EditProfile.age, F.text)
async def edit_age_save(message: Message, state: FSMContext):
    age = validate_age(message.text)
    if age is None:
        await message.answer(f"Введите корректный возраст: от {MIN_AGE} до {MAX_AGE}.")
        return
    await update_user_field(message.from_user.id, "age", age)
    await state.clear()
    await send_menu(message, message.from_user.id, "Возраст успешно обновлён.")

@router.message(EditProfile.age_range, F.text)
async def edit_age_range_save(message: Message, state: FSMContext):
    parsed = validate_age_range(message.text)
    if not parsed:
        await message.answer("Введите диапазон в формате <code>18-35</code>.")
        return
    age_min, age_max = parsed
    await update_user_field(message.from_user.id, "age_min", age_min)
    await update_user_field(message.from_user.id, "age_max", age_max)
    await state.clear()
    await send_menu(message, message.from_user.id, f"Диапазон возраста обновлён: {age_min}-{age_max}.")

@router.callback_query(EditProfile.city, F.data.startswith("city:"))
async def edit_city_quick(callback: CallbackQuery, state: FSMContext):
    city = callback.data.split(":", 1)[1]
    await callback.answer()
    await update_user_field(callback.from_user.id, "city", city)
    await state.clear()
    await callback.message.edit_text(f"Город обновлён: <b>{escape(city)}</b>.")
    await callback.message.answer("Главное меню:", reply_markup=menu_keyboard(await get_likes_count(callback.from_user.id)))

@router.callback_query(EditProfile.city, F.data == "city_other")
async def edit_city_other(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text(
        f"Введите новый город текстом: от {MIN_CITY_LENGTH} до {MAX_CITY_LENGTH} символов."
    )

@router.message(EditProfile.city, F.text)
async def edit_city_save(message: Message, state: FSMContext):
    city = validate_text(message.text, MIN_CITY_LENGTH, MAX_CITY_LENGTH)
    if not city:
        await message.answer(f"Город должен содержать от {MIN_CITY_LENGTH} до {MAX_CITY_LENGTH} символов.")
        return
    await update_user_field(message.from_user.id, "city", city)
    await state.clear()
    await send_menu(message, message.from_user.id, f"Город успешно обновлён: <b>{escape(city)}</b>.")

@router.callback_query(EditProfile.interests, F.data.startswith("edit_int:"))
async def edit_interests_toggle(callback: CallbackQuery, state: FSMContext):
    payload = callback.data.split(":", 1)[1]
    data = await state.get_data()
    selected: Set[str] = set(data.get("interests_selected", []))

    if payload == "done":
        await callback.answer()
        await update_user_field(callback.from_user.id, "interests", serialize_interests(selected))
        await state.clear()
        await callback.message.edit_text("Интересы обновлены.")
        await callback.message.answer("Главное меню:", reply_markup=menu_keyboard(await get_likes_count(callback.from_user.id)))
        return

    try:
        idx = int(payload)
        tag = INTERESTS_LIST[idx]
    except (ValueError, IndexError):
        await callback.answer("Некорректный вариант.", show_alert=True)
        return

    if tag in selected:
        selected.discard(tag)
    elif len(selected) < MAX_INTERESTS:
        selected.add(tag)
    else:
        await callback.answer(f"Можно выбрать не более {MAX_INTERESTS}.", show_alert=True)
        return

    await state.update_data(interests_selected=list(selected))
    await callback.answer()
    try:
        await callback.message.edit_reply_markup(reply_markup=interests_keyboard(selected, "edit_int", with_cancel=True))
    except TelegramBadRequest:
        pass

@router.message(EditProfile.bio, F.text)
async def edit_bio_save(message: Message, state: FSMContext):
    bio = validate_text(message.text, MIN_BIO_LENGTH, MAX_BIO_LENGTH)
    if not bio:
        await message.answer(f"Описание должно содержать от {MIN_BIO_LENGTH} до {MAX_BIO_LENGTH} символов.")
        return
    await update_user_field(message.from_user.id, "bio", bio)
    await state.clear()
    await send_menu(message, message.from_user.id, "Описание успешно обновлено.")

@router.message(EditProfile.photo, F.photo)
async def edit_photo_save(message: Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    if await update_photo(message.from_user.id, photo_id):
        await state.clear()
        await send_menu(message, message.from_user.id, "Фотография успешно обновлена. Анкета снова активна.")
    else:
        await state.clear()
        await send_menu(message, message.from_user.id, "Не удалось обновить фото (пользователь заблокирован).")

@router.message(EditProfile.photo)
async def edit_photo_invalid(message: Message):
    await message.answer("Пожалуйста, отправьте изображение именно как фотографию.")

@router.callback_query(F.data == "cancel_edit")
async def cancel_editing(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    try:
        await callback.message.delete()
    except TelegramBadRequest:
        pass
    await callback.message.answer("Действие отменено.", reply_markup=menu_keyboard(await get_likes_count(callback.from_user.id)))

@router.callback_query(F.data == "delete_profile")
async def delete_profile_confirm(callback: CallbackQuery):
    await callback.answer()
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, удалить анкету", callback_data="confirm_delete")
    builder.button(text="🔙 Назад", callback_data="edit_profile")
    builder.adjust(1)
    await callback.message.edit_text(
        "Вы уверены, что хотите удалить анкету?\n\n"
        "Будут удалены профиль, лайки, просмотры, блокировки и связанные данные. Это действие нельзя отменить.",
        reply_markup=builder.as_markup(),
    )

@router.callback_query(F.data == "confirm_delete")
async def delete_profile_execute(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await delete_user(callback.from_user.id)
    await callback.message.edit_text(
        "Ваша анкета и связанные данные удалены.\n\n"
        "Чтобы зарегистрироваться снова, напишите /start."
    )

@router.callback_query(F.data == "menu")
async def back_to_menu(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    likes = await get_likes_count(callback.from_user.id)
    await safe_edit_or_send(callback, "Главное меню:", reply_markup=menu_keyboard(likes))

# ============================================================
# АНОНИМНЫЙ ЧАТ ДО РАСКРЫТИЯ КОНТАКТА
# ============================================================

@router.callback_query(F.data.startswith("relay_open:"))
async def relay_open(callback: CallbackQuery, state: FSMContext):
    try:
        partner_id = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректные данные.", show_alert=True)
        return

    match_row = await get_match_row(callback.from_user.id, partner_id)
    if not match_row:
        await callback.answer("Мэтч не найден (возможно, был удалён).", show_alert=True)
        return

    if await are_users_blocked(callback.from_user.id, partner_id):
        await callback.answer("Диалог недоступен.", show_alert=True)
        return

    partner = await get_user(partner_id)
    if not partner:
        await callback.answer("Собеседник недоступен.", show_alert=True)
        return

    await callback.answer()
    await state.set_state(Relay.chatting)
    await state.update_data(relay_partner_id=partner_id)
    await callback.message.answer(
        f"💬 Вы в анонимном чате с <b>{escape(partner['name'])}</b>.\n"
        f"Сообщения будут пересылаться без раскрытия контакта.\n\n"
        f"Чтобы обменяться контактами, оба должны нажать «Показать контакт».",
        reply_markup=relay_keyboard(),
    )

@router.callback_query(F.data.startswith("reveal:"))
async def reveal_button(callback: CallbackQuery):
    try:
        partner_id = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректные данные.", show_alert=True)
        return
    await _process_reveal(callback, partner_id)

@router.callback_query(Relay.chatting, F.data == "relay_reveal")
async def relay_reveal(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    partner_id = data.get("relay_partner_id")
    if not isinstance(partner_id, int):
        await callback.answer("Сессия чата не найдена.", show_alert=True)
        return
    await _process_reveal(callback, partner_id)

async def _process_reveal(callback: CallbackQuery, partner_id: int) -> None:
    uid = callback.from_user.id
    exists, both = await request_reveal(uid, partner_id)
    if not exists:
        await callback.answer("Мэтч не найден.", show_alert=True)
        return

    if not both:
        await callback.answer("Запрос отправлен. Ждём согласия собеседника.", show_alert=True)
        return

    await callback.answer("Контакты раскрыты! 🎉")
    current_user = await get_user(uid)
    partner = await get_user(partner_id)

    try:
        await callback.message.answer(
            f"📇 Контакт раскрыт! Можете писать напрямую:",
            reply_markup=match_keyboard(partner_id, revealed=True),
        )
    except (TelegramBadRequest, TelegramForbiddenError):
        pass

    if current_user:
        try:
            await bot.send_message(
                partner_id,
                f"📇 <b>{escape(current_user['name'])}</b> согласился(-ась) показать контакт. "
                f"Теперь вы оба видите друг друга напрямую!",
                reply_markup=match_keyboard(uid, revealed=True),
            )
        except (TelegramBadRequest, TelegramForbiddenError):
            pass

@router.callback_query(Relay.chatting, F.data == "relay_stop")
async def relay_stop(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await callback.message.answer("Вы вышли из анонимного чата.", reply_markup=menu_keyboard(await get_likes_count(callback.from_user.id)))

@router.message(Relay.chatting, F.text)
async def relay_message(message: Message, state: FSMContext):
    data = await state.get_data()
    partner_id = data.get("relay_partner_id")
    if not isinstance(partner_id, int):
        await state.clear()
        await message.answer("Сессия чата не найдена. Откройте чат заново из карточки мэтча.")
        return

    if await are_users_blocked(message.from_user.id, partner_id) or await check_ban(partner_id):
        await state.clear()
        await message.answer("Собеседник недоступен. Чат завершён.")
        return

    sender = await get_user(message.from_user.id)
    sender_name = escape(sender["name"]) if sender else "Аноним"

    try:
        await bot.send_message(
            partner_id,
            f"💬 <b>{sender_name} (анонимно)</b>:\n{escape(message.text)}",
        )
        await message.answer("✅ Отправлено.")
    except (TelegramBadRequest, TelegramForbiddenError):
        await message.answer("Не удалось доставить сообщение собеседнику.")

@router.message(Relay.chatting)
async def relay_message_unsupported(message: Message):
    await message.answer("В анонимном чате поддерживаются только текстовые сообщения.")

@router.callback_query()
async def unknown_callback(callback: CallbackQuery):
    await callback.answer("Эта кнопка устарела. Откройте меню заново.", show_alert=True)

# ============================================================
# КАТЧ-ОЛЛ ОБРАБОТЧИКИ СООБЩЕНИЙ (ДОЛЖНЫ БЫТЬ ПОСЛЕДНИМИ!)
# ============================================================

@router.message(StateFilter(
    Registration.name, Registration.age, Registration.city, Registration.bio, Registration.age_range,
    EditProfile.name, EditProfile.age, EditProfile.city, EditProfile.bio, EditProfile.age_range,
))
async def text_required_invalid(message: Message):
    await message.answer("Пожалуйста, отправьте значение текстом.")

@router.message()
async def unknown_message(message: Message, state: FSMContext):
    current_state = await state.get_state()
    logger.warning(
        "UNHANDLED MESSAGE | user_id=%s | state=%r | text=%r",
        message.from_user.id if message.from_user else None,
        current_state,
        message.text,
    )
    await message.answer(
        "Сообщение не распознано. Если вы начали регистрацию — попробуйте ещё раз или напишите /start."
    )

# ============================================================
# ФОНОВЫЕ ЗАДАЧИ: бэкап БД и ревалидация фото
# ============================================================

async def perform_backup() -> Optional[str]:
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(BACKUP_DIR, f"{os.path.basename(DB_NAME)}.{timestamp}.bak")

    try:
        # Используем встроенный backup API sqlite3 через отдельное соединение,
        # чтобы не мешать основным aiosqlite-соединениям.
        def _do_backup():
            source = sqlite3.connect(DB_NAME)
            try:
                dest = sqlite3.connect(backup_path)
                try:
                    source.backup(dest)
                finally:
                    dest.close()
            finally:
                source.close()

        await asyncio.to_thread(_do_backup)
        logger.info("Backup создан: %s", backup_path)
    except Exception as exc:
        logger.exception("Ошибка создания backup: %s", exc)
        return None

    # Ротация старых бэкапов.
    try:
        files = sorted(
            (f for f in os.listdir(BACKUP_DIR) if f.startswith(os.path.basename(DB_NAME))),
            reverse=True,
        )
        for stale in files[BACKUP_RETENTION:]:
            os.remove(os.path.join(BACKUP_DIR, stale))
    except OSError:
        pass

    return backup_path

async def backup_loop(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            await perform_backup()
        except Exception:
            logger.exception("backup_loop: непредвиденная ошибка")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=BACKUP_INTERVAL_HOURS * 3600)
        except asyncio.TimeoutError:
            continue

async def photo_recheck_loop(stop_event: asyncio.Event) -> None:
    """Периодически пытается получить file_id главных фото активных анкет,
    чтобы деактивировать анкету заранее, а не в момент показа другому пользователю."""
    while not stop_event.is_set():
        try:
            rows = await fetch_all(
                "SELECT user_id, photo_id FROM users WHERE is_active = 1 ORDER BY RANDOM() LIMIT ?",
                (PHOTO_RECHECK_BATCH,),
            )
            for row in rows:
                try:
                    await bot.get_file(row["photo_id"])
                except TelegramBadRequest as exc:
                    if is_invalid_photo_error(exc):
                        await deactivate_user_profile(row["user_id"])
                        logger.info("Фото пользователя %s деактивировано при ревалидации.", row["user_id"])
                except Exception:
                    pass
                await asyncio.sleep(0.1)
        except Exception:
            logger.exception("photo_recheck_loop: непредвиденная ошибка")

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=PHOTO_RECHECK_INTERVAL_HOURS * 3600)
        except asyncio.TimeoutError:
            continue

# ============================================================
# ЗАПУСК И GRACEFUL SHUTDOWN
# ============================================================

async def main() -> None:
    await init_db()

    stop_event = asyncio.Event()
    background_tasks = [
        asyncio.create_task(backup_loop(stop_event)),
        asyncio.create_task(photo_recheck_loop(stop_event)),
    ]

    loop = asyncio.get_running_loop()

    def _request_shutdown():
        logger.info("Получен сигнал остановки, завершаем работу...")
        stop_event.set()
        asyncio.create_task(dp.stop_polling())

    if os.name != "nt":
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_shutdown)
            except NotImplementedError:
                pass

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Бот запущен.")
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        stop_event.set()
        for task in background_tasks:
            task.cancel()
        await asyncio.gather(*background_tasks, return_exceptions=True)

        if db:
            await db.close()
        if db_read:
            await db_read.close()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен.")
