import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
from html import escape
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

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
    InputMediaPhoto,
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
MAX_PHOTOS = 6

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

db: Optional[aiosqlite.Connection] = None
db_lock = asyncio.Lock()

# Кэш последнего обновления last_seen (чтобы не писать в БД каждое сообщение)
_last_seen_cache: Dict[int, float] = {}
LAST_SEEN_THROTTLE = 300  # 5 минут

# ============================================================
# КОНСТАНТЫ
# ============================================================

MIN_AGE, MAX_AGE = 18, 99
MIN_NAME_LENGTH, MAX_NAME_LENGTH = 2, 50
MIN_CITY_LENGTH, MAX_CITY_LENGTH = 2, 50
MIN_BIO_LENGTH, MAX_BIO_LENGTH = 10, 500
MAX_REPORT_LENGTH = 500

VALID_GENDERS = {"male": "Парень", "female": "Девушка"}
VALID_SEARCH_GENDERS = {"male": "Парней", "female": "Девушек", "all": "Всех"}
VALID_MODES = {"local", "global", "likes"}

ALLOWED_USER_FIELDS = {
    "name", "age", "gender", "search_gender", "city", "bio",
    "photo_id", "username", "age_min", "age_max",
}

KNOWN_COMMANDS = {"/start", "/delete", "/help", "/stats", "/unban", "/cancel", "/admin"}

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
    ALREADY_MATCHED = 3
    REJECTED = 4

# ============================================================
# ВАЛИДАЦИЯ
# ============================================================

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

def parse_age_range(value: Optional[str]) -> Optional[Tuple[int, int]]:
    if not value:
        return None
    text = value.strip().replace(" ", "").replace("–", "-").replace("—", "-")
    if "-" in text:
        parts = text.split("-", 1)
        if len(parts) != 2:
            return None
        try:
            a, b = int(parts[0]), int(parts[1])
        except ValueError:
            return None
        if a > b:
            a, b = b, a
        return (a, b) if MIN_AGE <= a <= b <= MAX_AGE else None
    if text.isdigit():
        v = int(text)
        return (v, v) if MIN_AGE <= v <= MAX_AGE else None
    return None

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
        for marker in ("wrong file identifier", "wrong file_id", "photo_invalid", "file is too old")
    )

def format_last_seen(last_seen_at: Optional[str]) -> str:
    if not last_seen_at:
        return ""
    try:
        dt = datetime.strptime(last_seen_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return ""
    delta = (datetime.now(timezone.utc) - dt).total_seconds()
    if delta < 300:
        return "🟢 Онлайн"
    if delta < 3600:
        return f"🟡 Был(а) {int(delta // 60)} мин назад"
    if delta < 86400:
        return f"⚪ Был(а) {int(delta // 3600)} ч назад"
    return f"⚪ Был(а) {int(delta // 86400)} дн назад"

# ============================================================
# PER-USER LOCKS
# ============================================================

user_action_locks: Dict[int, asyncio.Lock] = {}

def get_user_lock(user_id: int) -> asyncio.Lock:
    lock = user_action_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        user_action_locks[user_id] = lock
    return lock

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
    bio = State()
    photo = State()

class EditProfile(StatesGroup):
    name = State()
    age = State()
    search_gender = State()
    age_range_min = State()
    age_range_max = State()
    city = State()
    bio = State()
    photo = State()

class Report(StatesGroup):
    reason = State()

class AdminStates(StatesGroup):
    broadcast_text = State()
    find_user = State()

# ============================================================
# БАЗА ДАННЫХ
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
    await db.execute("PRAGMA synchronous = NORMAL")
    await db.execute("PRAGMA busy_timeout = 5000")

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
        accepted_rules_version INTEGER,
        age_min INTEGER NOT NULL DEFAULT 18,
        age_max INTEGER NOT NULL DEFAULT 99,
        last_seen_at DATETIME
    )""")

    await db.execute("""CREATE TABLE IF NOT EXISTS user_photos (
        user_id INTEGER NOT NULL,
        photo_id TEXT NOT NULL,
        position INTEGER NOT NULL DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (user_id, photo_id),
        FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
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
    await db.execute("CREATE INDEX IF NOT EXISTS idx_users_age ON users(age)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_users_active_age_gender ON users(is_active, age, gender)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_user_photos_user_pos ON user_photos(user_id, position)")
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

    async with db.execute("PRAGMA user_version") as cursor:
        version = (await cursor.fetchone())[0]
    logger.info("DB path=%s | user_version=%s", os.path.abspath(DB_NAME), version)
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
        await db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_reports_pending_unique "
            "ON reports(reporter_id, reported_id) WHERE status = 'pending'"
        )
        await db.execute("PRAGMA user_version = 1")

    if version < 2:
        async with db.execute("PRAGMA table_info(users)") as cursor:
            columns = [row[1] for row in await cursor.fetchall()]
        if "is_active" not in columns:
            await db.execute("ALTER TABLE users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
        if "accepted_rules_version" not in columns:
            await db.execute("ALTER TABLE users ADD COLUMN accepted_rules_version INTEGER")
        await db.execute("PRAGMA user_version = 2")

    if version < 3:
        async with db.execute("PRAGMA table_info(users)") as cursor:
            columns = [row[1] for row in await cursor.fetchall()]
        if "age_min" not in columns:
            await db.execute("ALTER TABLE users ADD COLUMN age_min INTEGER NOT NULL DEFAULT 18")
        if "age_max" not in columns:
            await db.execute("ALTER TABLE users ADD COLUMN age_max INTEGER NOT NULL DEFAULT 99")
        await db.execute("PRAGMA user_version = 3")

    if version < 4:
        await db.execute("""CREATE TABLE IF NOT EXISTS user_photos (
            user_id INTEGER NOT NULL,
            photo_id TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, photo_id),
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        )""")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_user_photos_user_pos ON user_photos(user_id, position)")
        async with db.execute("PRAGMA table_info(users)") as cursor:
            columns = [row[1] for row in await cursor.fetchall()]
        if "last_seen_at" not in columns:
            await db.execute("ALTER TABLE users ADD COLUMN last_seen_at DATETIME")
        # Заполним user_photos из users.photo_id для существующих пользователей
        await db.execute("""
            INSERT OR IGNORE INTO user_photos (user_id, photo_id, position)
            SELECT user_id, photo_id, 0 FROM users WHERE photo_id IS NOT NULL AND photo_id != ''
        """)
        await db.execute("PRAGMA user_version = 4")

    await db.commit()

# ============================================================
# CRUD
# ============================================================

async def get_user_status(user_id: int) -> dict:
    async with db_lock:
        async with db.execute(
            """
            SELECT
                EXISTS(SELECT 1 FROM bans WHERE user_id=?) AS is_banned,
                EXISTS(SELECT 1 FROM users WHERE user_id=?) AS user_exists,
                (SELECT accepted_rules_version FROM users WHERE user_id=?) AS accepted_rules_version
            """,
            (user_id, user_id, user_id),
        ) as cursor:
            row = await cursor.fetchone()
    return {
        "is_banned": bool(row["is_banned"]),
        "exists": bool(row["user_exists"]),
        "rules_version": row["accepted_rules_version"],
    }

async def check_ban(user_id: int) -> bool:
    async with db_lock:
        async with db.execute("SELECT 1 FROM bans WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone() is not None

async def get_user(user_id: int) -> Optional[aiosqlite.Row]:
    async with db_lock:
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def get_user_photos(user_id: int) -> List[str]:
    async with db_lock:
        async with db.execute(
            "SELECT photo_id FROM user_photos WHERE user_id=? ORDER BY position, created_at",
            (user_id,),
        ) as cur:
            return [r["photo_id"] for r in await cur.fetchall()]

async def add_user_photo(user_id: int, photo_id: str) -> bool:
    async with transaction():
        async with db.execute("SELECT COUNT(*) FROM user_photos WHERE user_id=?", (user_id,)) as cur:
            cnt = (await cur.fetchone())[0]
        if cnt >= MAX_PHOTOS:
            return False
        async with db.execute("SELECT COALESCE(MAX(position), -1) FROM user_photos WHERE user_id=?", (user_id,)) as cur:
            max_pos = (await cur.fetchone())[0]
        await db.execute(
            "INSERT OR IGNORE INTO user_photos (user_id, photo_id, position) VALUES (?, ?, ?)",
            (user_id, photo_id, max_pos + 1),
        )
        return True

async def delete_user_photo(user_id: int, photo_id: str) -> bool:
    async with transaction():
        cur = await db.execute("DELETE FROM user_photos WHERE user_id=? AND photo_id=?", (user_id, photo_id))
        if cur.rowcount == 0:
            return False
        # синхронизируем главное фото
        async with db.execute(
            "SELECT photo_id FROM user_photos WHERE user_id=? ORDER BY position, created_at LIMIT 1",
            (user_id,),
        ) as cur:
            row = await cur.fetchone()
        if row:
            await db.execute("UPDATE users SET photo_id=? WHERE user_id=?", (row["photo_id"], user_id))
        else:
            await db.execute("UPDATE users SET is_active=0 WHERE user_id=?", (user_id,))
        return True

async def set_main_photo(user_id: int, photo_id: str) -> bool:
    async with transaction():
        async with db.execute(
            "SELECT 1 FROM user_photos WHERE user_id=? AND photo_id=?", (user_id, photo_id)
        ) as cur:
            if not await cur.fetchone():
                return False
        await db.execute("UPDATE users SET photo_id=? WHERE user_id=?", (photo_id, user_id))
        return True

async def touch_last_seen(user_id: int) -> None:
    now = time.monotonic()
    last = _last_seen_cache.get(user_id, 0)
    if now - last < LAST_SEEN_THROTTLE:
        return
    _last_seen_cache[user_id] = now
    try:
        async with db_lock:
            await db.execute("UPDATE users SET last_seen_at = CURRENT_TIMESTAMP WHERE user_id = ?", (user_id,))
            await db.commit()
    except Exception as exc:
        logger.debug("touch_last_seen failed: %s", exc)

async def add_user_to_db(
    user_id: int, name: str, age: int, gender: str, search_gender: str,
    city: str, bio: str, photo_id: str, username: Optional[str],
    accepted_rules_at: str, age_min: int = MIN_AGE, age_max: int = MAX_AGE,
) -> bool:
    async with transaction():
        async with db.execute("SELECT 1 FROM bans WHERE user_id=?", (user_id,)) as cur:
            if await cur.fetchone():
                return False
        await db.execute(
            """INSERT INTO users (
                user_id, name, age, gender, search_gender, city, bio, photo_id,
                username, accepted_rules_at, is_active, accepted_rules_version,
                age_min, age_max, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                name=excluded.name, age=excluded.age, gender=excluded.gender,
                search_gender=excluded.search_gender, city=excluded.city,
                bio=excluded.bio, photo_id=excluded.photo_id,
                username=excluded.username, accepted_rules_at=excluded.accepted_rules_at,
                is_active=1, accepted_rules_version=excluded.accepted_rules_version,
                age_min=excluded.age_min, age_max=excluded.age_max""",
            (user_id, name, age, gender, search_gender, city, bio, photo_id,
             username, accepted_rules_at, RULES_VERSION, age_min, age_max),
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

async def set_rules_accepted(user_id: int, accepted_at: str) -> None:
    async with db_lock:
        await db.execute(
            "UPDATE users SET accepted_rules_at = ?, accepted_rules_version = ? WHERE user_id = ?",
            (accepted_at, RULES_VERSION, user_id),
        )
        await db.commit()

async def delete_user(user_id: int) -> None:
    async with transaction():
        await db.execute("DELETE FROM views WHERE viewer_id=? OR viewed_id=?", (user_id, user_id))
        await db.execute("DELETE FROM likes WHERE liker_id=? OR liked_id=?", (user_id, user_id))
        await db.execute("DELETE FROM matches WHERE user1_id=? OR user2_id=?", (user_id, user_id))
        await db.execute("DELETE FROM blocks WHERE blocker_id=? OR blocked_id=?", (user_id, user_id))
        await db.execute("DELETE FROM reports WHERE reporter_id=? OR reported_id=?", (user_id, user_id))
        await db.execute("DELETE FROM user_photos WHERE user_id=?", (user_id,))
        await db.execute("DELETE FROM users WHERE user_id=?", (user_id,))

def build_eligibility_excludes(user_id: int, user_alias: str = "u") -> Tuple[str, list]:
    sql = f"""
        AND NOT EXISTS (SELECT 1 FROM views v WHERE v.viewer_id=? AND v.viewed_id={user_alias}.user_id)
        AND NOT EXISTS (SELECT 1 FROM likes l WHERE l.liker_id=? AND l.liked_id={user_alias}.user_id)
        AND NOT EXISTS (SELECT 1 FROM blocks b WHERE b.blocker_id=? AND b.blocked_id={user_alias}.user_id)
        AND NOT EXISTS (SELECT 1 FROM blocks b WHERE b.blocker_id={user_alias}.user_id AND b.blocked_id=?)
        AND NOT EXISTS (SELECT 1 FROM bans b WHERE b.user_id={user_alias}.user_id)
        AND NOT EXISTS (
            SELECT 1 FROM matches m WHERE
                (m.user1_id=? AND m.user2_id={user_alias}.user_id) OR
                (m.user2_id=? AND m.user1_id={user_alias}.user_id)
        )
        AND NOT EXISTS (SELECT 1 FROM reports r WHERE r.reporter_id=? AND r.reported_id={user_alias}.user_id)
    """
    return sql, [user_id] * 7

async def get_likes_count(user_id: int) -> int:
    exclude_sql, exclude_params = build_eligibility_excludes(user_id, "u")
    query = f"""
        SELECT COUNT(*) FROM likes AS l
        JOIN users AS u ON u.user_id = l.liker_id
        WHERE l.liked_id = ? AND u.is_active = 1
        {exclude_sql}
    """
    params = [user_id, *exclude_params]
    async with db_lock:
        async with db.execute(query, tuple(params)) as cursor:
            return (await cursor.fetchone())[0]

async def are_users_blocked(u1: int, u2: int) -> bool:
    async with db_lock:
        async with db.execute(
            "SELECT 1 FROM blocks WHERE (blocker_id=? AND blocked_id=?) OR (blocker_id=? AND blocked_id=?)",
            (u1, u2, u2, u1),
        ) as cursor:
            return await cursor.fetchone() is not None

async def add_view(viewer_id: int, viewed_id: int) -> None:
    if viewer_id == viewed_id:
        return
    async with db_lock:
        await db.execute("INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)", (viewer_id, viewed_id))
        await db.commit()

async def remove_view(viewer_id: int, viewed_id: int) -> None:
    async with db_lock:
        await db.execute("DELETE FROM views WHERE viewer_id=? AND viewed_id=?", (viewer_id, viewed_id))
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
            return LikeResult.LIKED

        await db.execute(
            "DELETE FROM likes WHERE (liker_id=? AND liked_id=?) OR (liker_id=? AND liked_id=?)",
            (liker_id, liked_id, liked_id, liker_id),
        )
        await db.execute("INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)", (liked_id, liker_id))

        cursor = await db.execute("INSERT OR IGNORE INTO matches (user1_id, user2_id) VALUES (?, ?)", (u1, u2))
        return LikeResult.MATCHED if cursor.rowcount > 0 else LikeResult.ALREADY_MATCHED

async def reject_like(user_id: int, liker_id: int) -> None:
    async with transaction():
        await db.execute("DELETE FROM likes WHERE liker_id = ? AND liked_id = ?", (liker_id, user_id))
        await db.execute("INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)", (user_id, liker_id))

async def block_user(blocker_id: int, blocked_id: int) -> bool:
    if blocker_id == blocked_id:
        return False
    u1, u2 = sorted((blocker_id, blocked_id))
    async with transaction():
        async with db.execute("SELECT COUNT(*) FROM users WHERE user_id IN (?, ?)", (blocker_id, blocked_id)) as cur:
            count = (await cur.fetchone())[0]
        if count != 2:
            return False
        await db.execute("INSERT OR IGNORE INTO blocks (blocker_id, blocked_id) VALUES (?, ?)", (blocker_id, blocked_id))
        await db.execute(
            "DELETE FROM likes WHERE (liker_id=? AND liked_id=?) OR (liker_id=? AND liked_id=?)",
            (blocker_id, blocked_id, blocked_id, blocker_id),
        )
        await db.execute("DELETE FROM matches WHERE user1_id=? AND user2_id=?", (u1, u2))
        await db.execute("INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)", (blocker_id, blocked_id))
        return True

async def report_exists(reporter_id: int, reported_id: int) -> bool:
    async with db_lock:
        async with db.execute(
            "SELECT 1 FROM reports WHERE reporter_id=? AND reported_id=? AND status='pending' LIMIT 1",
            (reporter_id, reported_id),
        ) as cur:
            return await cur.fetchone() is not None

async def add_report(reporter_id: int, reported_id: int, reason: str) -> Optional[int]:
    if reporter_id == reported_id:
        return None
    async with transaction():
        async with db.execute(
            """SELECT 1 FROM users u WHERE u.user_id=? AND u.is_active=1
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
            """UPDATE reports SET status='accepted', reviewed_by=?, reviewed_at=CURRENT_TIMESTAMP, resolution='banned'
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
            """UPDATE reports SET status='accepted', reviewed_by=?, reviewed_at=CURRENT_TIMESTAMP,
               resolution='already_banned'
               WHERE reported_id=? AND id<>? AND status='pending'""",
            (admin_id, reported_id, report_id),
        )
        return reported_id

async def reject_report(report_id: int, admin_id: int) -> bool:
    async with transaction():
        cur = await db.execute(
            """UPDATE reports SET status='rejected', reviewed_by=?, reviewed_at=CURRENT_TIMESTAMP, resolution='rejected'
               WHERE id=? AND status='pending'""",
            (admin_id, report_id),
        )
        return cur.rowcount > 0

async def unban_user(user_id: int) -> bool:
    async with transaction():
        cur = await db.execute("DELETE FROM bans WHERE user_id = ?", (user_id,))
        return cur.rowcount > 0

async def get_random_profile(
    user_id: int, user_gender: str, search_gender: str,
    user_city: Optional[str] = None, strict_city: bool = False,
    age_min: int = MIN_AGE, age_max: int = MAX_AGE,
) -> Optional[aiosqlite.Row]:
    exclude_sql, exclude_params = build_eligibility_excludes(user_id, "u")
    query = f"""
        SELECT * FROM users u
        WHERE u.user_id != ? AND u.is_active = 1
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
    query += " AND u.age BETWEEN ? AND ?"
    params.extend([age_min, age_max])

    if strict_city and user_city:
        query += " AND u.city = ? COLLATE NOCASE"
        params.append(user_city)
        query += " ORDER BY RANDOM() LIMIT 1"
    elif user_city:
        query += " ORDER BY CASE WHEN u.city = ? COLLATE NOCASE THEN 0 ELSE 1 END, RANDOM() LIMIT 1"
        params.append(user_city)
    else:
        query += " ORDER BY RANDOM() LIMIT 1"

    async with db_lock:
        async with db.execute(query, tuple(params)) as cursor:
            return await cursor.fetchone()

async def get_next_liker(user_id: int) -> Optional[aiosqlite.Row]:
    exclude_sql, exclude_params = build_eligibility_excludes(user_id, "u")
    query = f"""
        SELECT u.* FROM users AS u
        JOIN likes AS l ON u.user_id = l.liker_id
        WHERE l.liked_id = ? AND u.is_active = 1
        {exclude_sql}
        ORDER BY l.created_at DESC LIMIT 1
    """
    params = [user_id, *exclude_params]
    async with db_lock:
        async with db.execute(query, tuple(params)) as cursor:
            return await cursor.fetchone()

async def verify_like_exists(liker_id: int, liked_id: int) -> bool:
    async with db_lock:
        async with db.execute("SELECT 1 FROM likes WHERE liker_id=? AND liked_id=?", (liker_id, liked_id)) as cur:
            return await cur.fetchone() is not None

async def get_matches(user_id: int) -> list:
    async with db_lock:
        async with db.execute(
            """SELECT u.user_id, u.name, m.created_at FROM matches m
               JOIN users u ON u.user_id = CASE WHEN m.user1_id = ? THEN m.user2_id ELSE m.user1_id END
               WHERE (m.user1_id = ? OR m.user2_id = ?) AND u.user_id != ? AND u.is_active = 1
               AND u.user_id NOT IN (SELECT user_id FROM bans)
               ORDER BY m.created_at DESC LIMIT 50""",
            (user_id, user_id, user_id, user_id),
        ) as cursor:
            return await cursor.fetchall()

async def get_stats() -> Dict[str, int]:
    async with db_lock:
        async with db.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM users) AS users_total,
                (SELECT COUNT(*) FROM users u WHERE u.is_active = 1
                    AND NOT EXISTS (SELECT 1 FROM bans b WHERE b.user_id = u.user_id)) AS users_active,
                (SELECT COUNT(*) FROM likes) AS likes,
                (SELECT COUNT(*) FROM matches) AS matches,
                (SELECT COUNT(*) FROM views) AS views,
                (SELECT COUNT(*) FROM blocks) AS blocks,
                (SELECT COUNT(*) FROM bans) AS bans,
                (SELECT COUNT(*) FROM reports WHERE status = 'pending') AS reports_pending,
                (SELECT COUNT(*) FROM users WHERE last_seen_at > datetime('now', '-10 minutes')) AS online_now,
                (SELECT COUNT(*) FROM users WHERE created_at > datetime('now', '-1 day')) AS new_today
            """
        ) as cursor:
            return dict(await cursor.fetchone())

async def get_user_stats(user_id: int) -> Dict[str, Any]:
    async with db_lock:
        async with db.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM likes WHERE liker_id=?) AS likes_sent,
                (SELECT COUNT(*) FROM likes WHERE liked_id=?) AS likes_received,
                (SELECT COUNT(*) FROM matches WHERE user1_id=? OR user2_id=?) AS matches_count,
                (SELECT COUNT(*) FROM views WHERE viewed_id=?) AS views_count,
                (SELECT COUNT(*) FROM blocks WHERE blocker_id=? OR blocked_id=?) AS blocks_count,
                (SELECT created_at FROM users WHERE user_id=?) AS created_at
            """,
            (user_id, user_id, user_id, user_id, user_id, user_id, user_id, user_id),
        ) as cur:
            row = await cur.fetchone()
    return dict(row) if row else {}

async def get_pending_reports(offset: int, limit: int = 5) -> list:
    async with db_lock:
        async with db.execute(
            """SELECT r.id, r.reporter_id, r.reported_id, r.reason, r.timestamp,
                      u.name AS reported_name, u.username AS reported_username
               FROM reports r
               LEFT JOIN users u ON u.user_id = r.reported_id
               WHERE r.status='pending'
               ORDER BY r.timestamp DESC
               LIMIT ? OFFSET ?""",
            (limit, offset),
        ) as cur:
            return await cur.fetchall()

async def count_pending_reports() -> int:
    async with db_lock:
        async with db.execute("SELECT COUNT(*) FROM reports WHERE status='pending'") as cur:
            return (await cur.fetchone())[0]

async def get_all_active_user_ids() -> List[int]:
    async with db_lock:
        async with db.execute(
            """SELECT user_id FROM users u WHERE u.is_active=1
               AND NOT EXISTS (SELECT 1 FROM bans b WHERE b.user_id=u.user_id)"""
        ) as cur:
            return [r["user_id"] for r in await cur.fetchall()]

async def find_users_by_query(q: str, limit: int = 5) -> list:
    q = q.strip()
    if not q:
        return []
    async with db_lock:
        if q.isdigit():
            async with db.execute("SELECT * FROM users WHERE user_id=?", (int(q),)) as cur:
                return await cur.fetchall()
        uname = q.lstrip("@")
        async with db.execute(
            "SELECT * FROM users WHERE username LIKE ? COLLATE NOCASE OR name LIKE ? COLLATE NOCASE LIMIT ?",
            (f"%{uname}%", f"%{q}%", limit),
        ) as cur:
            return await cur.fetchall()

# ============================================================
# MIDDLEWARE
# ============================================================

class SecurityMiddleware(BaseMiddleware):
    def __init__(self):
        self.last_action: Dict[Tuple[int, str], float] = {}
        self.cooldown_callback = 0.4
        self.cooldown_message = 0.7
        self.cooldown_command = 2.0

    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if not user:
            return await handler(event, data)

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

        # Online-статус: обновляем раз в 5 минут, если пользователь существует
        if status["exists"]:
            await touch_last_seen(user.id)

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
            Registration.agreement.state, Registration.name.state, Registration.age.state,
            Registration.gender.state, Registration.search_gender.state, Registration.age_range.state,
            Registration.city.state, Registration.bio.state, Registration.photo.state,
            EditProfile.name.state, EditProfile.age.state, EditProfile.search_gender.state,
            EditProfile.age_range_min.state, EditProfile.age_range_max.state,
            EditProfile.city.state, EditProfile.bio.state, EditProfile.photo.state,
            Report.reason.state, AdminStates.broadcast_text.state, AdminStates.find_user.state,
        }
        if current_state in bypass_states:
            return await handler(event, data)

        is_command = first_word in KNOWN_COMMANDS
        if is_command:
            action_type, cooldown = "command", self.cooldown_command
        elif isinstance(event, CallbackQuery):
            action_type, cooldown = "callback", self.cooldown_callback
        else:
            action_type, cooldown = "message", self.cooldown_message

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
            self.last_action = {k: v for k, v in self.last_action.items() if now - v < 3600}

        return await handler(event, data)

security_mw = SecurityMiddleware()
router.message.middleware(security_mw)
router.callback_query.middleware(security_mw)

# ============================================================
# КЛАВИАТУРЫ
# ============================================================

def menu_keyboard(likes_count: int = 0):
    builder = InlineKeyboardBuilder()
    builder.button(text="🔍 Смотреть анкеты", callback_data="search_menu")
    builder.button(text="👤 Моя анкета", callback_data="my_profile")
    builder.button(text="💑 Мои мэтчи", callback_data="my_matches")
    if likes_count > 0:
        builder.button(text=f"💌 Вам понравились: {likes_count}", callback_data="show_likes")
    builder.adjust(2, 2 if likes_count > 0 else 1)
    return builder.as_markup()

def profile_card_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Редактировать", callback_data="edit_profile")
    builder.button(text="📊 Статистика", callback_data="my_stats")
    builder.button(text="📸 Фото", callback_data="my_photos")
    builder.button(text="🔙 В меню", callback_data="menu")
    builder.adjust(2, 2)
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
    builder.button(text="👎 Пропустить", callback_data=f"dislike:{profile_id}:{mode}")
    builder.button(text="📖 Подробнее", callback_data=f"details:{profile_id}:{mode}")
    builder.button(text="🚫 Блокировать", callback_data=f"block:{profile_id}:{mode}")
    builder.button(text="🚩 Пожаловаться", callback_data=f"report:{profile_id}:{mode}")
    builder.button(text="🔚 В меню", callback_data="menu")
    builder.adjust(2, 2, 1, 1)
    return builder.as_markup()

def profile_like_keyboard(profile_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="❤️ Ответить взаимностью", callback_data=f"like:{profile_id}:likes")
    builder.button(text="👎 Пропустить", callback_data=f"dislike:{profile_id}:likes")
    builder.button(text="📖 Подробнее", callback_data=f"details:{profile_id}:likes")
    builder.button(text="🚫 Блокировать", callback_data=f"block:{profile_id}:likes")
    builder.button(text="🚩 Пожаловаться", callback_data=f"report:{profile_id}:likes")
    builder.adjust(2, 2, 1)
    return builder.as_markup()

def rewind_keyboard(profile_id: int, mode: str):
    builder = InlineKeyboardBuilder()
    builder.button(text="↩️ Вернуть анкету", callback_data=f"rewind:{profile_id}:{mode}")
    return builder.as_markup()

def edit_profile_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Имя", callback_data="edit_name")
    builder.button(text="✏️ Возраст", callback_data="edit_age")
    builder.button(text="✏️ Кого ищу", callback_data="edit_search_gender")
    builder.button(text="🎂 Возраст в поиске", callback_data="edit_age_range")
    builder.button(text="✏️ Город", callback_data="edit_city")
    builder.button(text="✏️ О себе", callback_data="edit_bio")
    builder.button(text="📸 Фото", callback_data="my_photos")
    builder.button(text="🗑 Удалить анкету", callback_data="delete_profile")
    builder.button(text="🔙 Назад", callback_data="my_profile")
    builder.adjust(2, 2, 2, 1, 1, 1)
    return builder.as_markup()

def cancel_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="cancel_edit")
    return builder.as_markup()

def cancel_report_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="cancel_report")
    return builder.as_markup()

def match_keyboard(target_user_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="✉️ Написать в Telegram", url=f"tg://user?id={target_user_id}")
    return builder.as_markup()

def admin_report_keyboard(report_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="🚫 Забанить", callback_data=f"admin_ban:{report_id}")
    builder.button(text="✅ Отклонить", callback_data=f"admin_reject:{report_id}")
    builder.adjust(2)
    return builder.as_markup()

def admin_menu_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Статистика", callback_data="admin_panel:stats")
    builder.button(text="🚩 Жалобы", callback_data="admin_panel:reports:0")
    builder.button(text="📢 Рассылка", callback_data="admin_panel:broadcast")
    builder.button(text="🔍 Найти юзера", callback_data="admin_panel:find")
    builder.button(text="✖️ Закрыть", callback_data="admin_panel:close")
    builder.adjust(2, 2, 1)
    return builder.as_markup()

def admin_reports_keyboard(page: int, total: int, has_next: bool):
    builder = InlineKeyboardBuilder()
    if page > 0:
        builder.button(text="⬅️ Назад", callback_data=f"admin_panel:reports:{page - 1}")
    if has_next:
        builder.button(text="Вперёд ➡️", callback_data=f"admin_panel:reports:{page + 1}")
    builder.button(text="🔙 В админку", callback_data="admin_panel:menu")
    builder.adjust(2, 1)
    return builder.as_markup()

def photos_manage_keyboard(photos: List[str], main_photo: str):
    """Клавиатура управления мультифото: удалить/сделать главной."""
    builder = InlineKeyboardBuilder()
    for i, pid in enumerate(photos):
        marker = "⭐" if pid == main_photo else "📷"
        builder.button(text=f"{marker} Фото {i+1}", callback_data=f"photo_manage:{i}")
    builder.button(text="➕ Добавить", callback_data="photo_add")
    builder.button(text="🔙 Назад", callback_data="my_profile")
    builder.adjust(1)
    return builder.as_markup()

def photo_actions_keyboard(idx: int, is_main: bool):
    builder = InlineKeyboardBuilder()
    if not is_main:
        builder.button(text="⭐ Сделать главной", callback_data=f"photo_setmain:{idx}")
    builder.button(text="🗑 Удалить", callback_data=f"photo_delete:{idx}")
    builder.button(text="🔙 К списку", callback_data="my_photos")
    builder.adjust(1)
    return builder.as_markup()

def photos_done_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Завершить регистрацию", callback_data="photos_done")
    return builder.as_markup()

# ============================================================
# ХЕЛПЕРЫ
# ============================================================

def format_profile(profile: aiosqlite.Row, prefix: str = "") -> str:
    last_seen = ""
    try:
        if "last_seen_at" in profile.keys():
            last_seen = format_last_seen(profile["last_seen_at"])
    except Exception:
        pass
    age_min = profile["age_min"] if "age_min" in profile.keys() else MIN_AGE
    age_max = profile["age_max"] if "age_max" in profile.keys() else MAX_AGE
    parts = [
        f"{prefix}👤 <b>{escape(profile['name'])}</b>, {profile['age']} ({escape(profile['gender'])})",
    ]
    if last_seen:
        parts.append(last_seen)
    parts.append(f"🔍 Ищет: {escape(profile['search_gender'])} · 🎂 {age_min}–{age_max}")
    parts.append(f"📍 {escape(profile['city'])}")
    parts.append("")
    parts.append(f"📝 {escape(profile['bio'])}")
    return "\n".join(parts)

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

async def notify_like(liked_id: int, liker: aiosqlite.Row) -> None:
    if not liker:
        return
    try:
        builder = InlineKeyboardBuilder()
        builder.button(text="💌 Посмотреть", callback_data="show_likes")
        await bot.send_message(
            liked_id,
            f"💌 <b>Вас лайкнули!</b>\n\n"
            f"<b>{escape(liker['name'])}</b>, {liker['age']} · 📍 {escape(liker['city'])}\n\n"
            f"Откройте «Вам понравились» в меню.",
            reply_markup=builder.as_markup(),
        )
    except (TelegramBadRequest, TelegramForbiddenError):
        pass

async def send_profile_photos(message: Message, user_id: int, caption: str, reply_markup) -> Optional[Message]:
    """Отправляет все фото пользователя (медиагруппу) или одно фото.
    Возвращает сообщение с кнопками (его id используется для валидации)."""
    photos = await get_user_photos(user_id)
    if not photos:
        user = await get_user(user_id)
        if user and user["photo_id"]:
            photos = [user["photo_id"]]
    if not photos:
        return None

    if len(photos) == 1:
        try:
            return await message.answer_photo(photo=photos[0], caption=caption, reply_markup=reply_markup)
        except TelegramBadRequest:
            raise

    media = [
        InputMediaPhoto(media=p, caption=caption if i == 0 else None, parse_mode=ParseMode.HTML)
        for i, p in enumerate(photos)
    ]
    try:
        await message.answer_media_group(media=media)
    except TelegramBadRequest:
        # fallback: одно фото
        try:
            return await message.answer_photo(photo=photos[0], caption=caption, reply_markup=reply_markup)
        except TelegramBadRequest:
            raise
    return await message.answer("👇 Управление анкетой:", reply_markup=reply_markup)

async def send_profile_card(message: Message, user: aiosqlite.Row) -> None:
    status = "✅ Активна" if user["is_active"] else "❌ Неактивна (обновите фото)"
    age_min = user["age_min"] if "age_min" in user.keys() else MIN_AGE
    age_max = user["age_max"] if "age_max" in user.keys() else MAX_AGE
    photos_count = len(await get_user_photos(user["user_id"]))
    text = (
        f"👤 <b>{escape(user['name'])}</b>, {user['age']} ({escape(user['gender'])})\n"
        f"🔍 Ищу: {escape(user['search_gender'])} · 🎂 {age_min}–{age_max}\n"
        f"📍 {escape(user['city'])}\n\n📝 {escape(user['bio'])}\n\n"
        f"📸 Фото: {photos_count}/{MAX_PHOTOS}\n"
        f"💌 Новых лайков: {await get_likes_count(user['user_id'])}\nСтатус: {status}"
    )
    try:
        await send_profile_photos(message, user["user_id"], text, profile_card_keyboard())
    except TelegramBadRequest as exc:
        logger.warning("Failed to send own profile photo user=%s: %s", user["user_id"], exc)
        if is_invalid_photo_error(exc):
            await deactivate_user_profile(user["user_id"])
            await message.answer(
                "Ваше фото недействительно. Обновите фото в разделе редактирования.",
                reply_markup=profile_card_keyboard(),
            )
        else:
            await message.answer("Не удалось загрузить фото.", reply_markup=profile_card_keyboard())

async def handle_stale_callback(callback: CallbackQuery, text: str = "Это действие уже недоступно.") -> bool:
    try:
        await callback.answer(text, show_alert=True)
    except Exception:
        pass
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass
    return True

async def delete_callback_message(callback: CallbackQuery) -> None:
    try:
        await callback.message.delete()
    except TelegramBadRequest:
        pass

async def show_profile(callback: CallbackQuery, state: FSMContext, mode: str = "global") -> None:
    user = await get_user(callback.from_user.id)
    if not user or not user["is_active"]:
        await safe_edit_or_send(
            callback, "Ваша анкета неактивна. Обновите фото через 'Моя анкета'.",
            reply_markup=back_to_menu_keyboard(),
        )
        return

    age_min = user["age_min"] if "age_min" in user.keys() else MIN_AGE
    age_max = user["age_max"] if "age_max" in user.keys() else MAX_AGE

    for _ in range(20):
        strict = (mode == "local")
        profile = await get_random_profile(
            user_id=callback.from_user.id, user_gender=user["gender"],
            search_gender=user["search_gender"], user_city=user["city"],
            strict_city=strict, age_min=age_min, age_max=age_max,
        )
        if not profile:
            await safe_edit_or_send(
                callback,
                "Подходящие анкеты закончились.\nМожно просмотреть их заново или заглянуть позже.",
                reply_markup=no_profiles_keyboard(),
            )
            return
        try:
            msg = await send_profile_photos(
                callback.message, profile["user_id"],
                format_profile(profile),
                profile_keyboard(profile["user_id"], mode),
            )
            await state.update_data(
                active_profile_id=profile["user_id"],
                active_profile_mode=mode,
                active_profile_msg_id=msg.message_id if msg else None,
            )
            return
        except TelegramBadRequest as exc:
            if is_invalid_photo_error(exc):
                await deactivate_user_profile(profile["user_id"])
                continue
            await safe_edit_or_send(callback, "Не удалось показать анкету. Попробуйте позже.")
            return

    await safe_edit_or_send(callback, "Слишком много невалидных анкет. Попробуйте позже.",
                            reply_markup=back_to_menu_keyboard())

async def show_next_liker(callback: CallbackQuery, state: FSMContext) -> None:
    user_id = callback.from_user.id
    for _ in range(20):
        profile = await get_next_liker(user_id)
        if not profile:
            await safe_edit_or_send(callback, "Новых лайков нет.",
                                    reply_markup=menu_keyboard(await get_likes_count(user_id)))
            return
        try:
            msg = await send_profile_photos(
                callback.message, profile["user_id"],
                format_profile(profile, prefix="💌 <b>Вас оценили!</b>\n\n"),
                profile_like_keyboard(profile["user_id"]),
            )
            await state.update_data(
                active_profile_id=profile["user_id"],
                active_profile_mode="likes",
                active_profile_msg_id=msg.message_id if msg else None,
            )
            return
        except TelegramBadRequest as exc:
            if is_invalid_photo_error(exc):
                await deactivate_user_profile(profile["user_id"])
                continue
            await safe_edit_or_send(callback, "Не удалось показать анкету.")
            return
    await safe_edit_or_send(callback, "Не удалось загрузить анкеты.",
                            reply_markup=menu_keyboard(await get_likes_count(user_id)))

async def show_next(callback: CallbackQuery, state: FSMContext, mode: str) -> None:
    if mode == "likes":
        await show_next_liker(callback, state)
        return
    await show_profile(callback, state, mode)

async def validate_active_card(callback: CallbackQuery, state: FSMContext, p_id: int, mode: str) -> bool:
    data = await state.get_data()
    if data.get("active_profile_id") != p_id or data.get("active_profile_mode") != mode:
        return False
    msg_id = data.get("active_profile_msg_id")
    if msg_id is not None and msg_id != callback.message.message_id:
        return False
    if mode == "likes" and not await verify_like_exists(liker_id=p_id, liked_id=callback.from_user.id):
        return False
    return True

# ============================================================
# ОБРАБОТЧИК ОШИБОК
# ============================================================

@router.errors()
async def error_handler(event: ErrorEvent):
    logger.exception("Unhandled exception: %s", event.exception)
    update = event.update
    try:
        if update.callback_query:
            await update.callback_query.answer("Произошла ошибка. Попробуйте позже.", show_alert=True)
        elif update.message:
            await update.message.answer("Произошла ошибка. Попробуйте позже.")
    except Exception:
        pass

# ============================================================
# КОМАНДЫ
# ============================================================

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
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
        "Для новой регистрации напишите /start."
    )

@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "ℹ️ <b>Справка</b>\n\n"
        "Бот помогает найти людей для знакомства: смотрите анкеты, ставьте лайки, "
        "а при взаимной симпатии получите мэтч.\n\n"
        "<b>Команды:</b>\n"
        "/start — анкета и главное меню\n"
        "/cancel — отменить текущее действие\n"
        "/help — эта справка\n"
        "/delete — удалить анкету и связанные данные\n\n" + RULES_TEXT
    )

@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    current = await state.get_state()
    if current is None:
        await message.answer("Нечего отменять.")
        return
    await state.clear()
    user = await get_user(message.from_user.id)
    if user and user["is_active"]:
        await message.answer("Действие отменено.", reply_markup=menu_keyboard(await get_likes_count(message.from_user.id)))
    else:
        await message.answer("Действие отменено. Напишите /start.")

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
        f"🟢 Онлайн (10 мин): {s['online_now']}\n"
        f"🆕 Новых за сутки: {s['new_today']}\n"
        f"❤️ Лайков: {s['likes']}\n"
        f"💑 Мэтчей: {s['matches']}\n"
        f"👀 Просмотров: {s['views']}\n"
        f"🚫 Блокировок: {s['blocks']}\n"
        f"⛔ Банов: {s['bans']}\n"
        f"🚩 Жалоб в ожидании: {s['reports_pending']}"
    )

# ============================================================
# РЕГИСТРАЦИЯ
# ============================================================

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

@router.callback_query(Registration.agreement, F.data == "agree_rules")
async def registration_agreement(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    accepted_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    if (await state.get_data()).get("reaccept"):
        await set_rules_accepted(callback.from_user.id, accepted_at)
        await state.clear()
        await callback.message.edit_text("Спасибо! Правила приняты.")
        user = await get_user(callback.from_user.id)
        if user:
            await send_profile_card(callback.message, user)
        await send_menu(callback.message, callback.from_user.id)
        return

    await state.set_state(Registration.name)
    await state.update_data(accepted_rules_at=accepted_at, photos=[])
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
    parts = callback.data.split("_")
    code = parts[2] if len(parts) > 2 else None
    sg = search_gender_text(code)
    if not sg:
        await callback.answer("Некорректный вариант.", show_alert=True)
        return
    await callback.answer()
    await state.update_data(search_gender=sg)
    await state.set_state(Registration.age_range)
    await callback.message.edit_text(
        f"Вы ищете: <b>{escape(sg)}</b>\n\n"
        f"В каком возрасте искать?\n"
        f"Отправьте диапазон в формате <code>18-30</code> или одно число <code>25</code>."
    )

@router.message(Registration.age_range, F.text)
async def registration_age_range(message: Message, state: FSMContext):
    parsed = parse_age_range(message.text)
    if not parsed:
        await message.answer(f"Введите диапазон <code>18-30</code> или одно число. Границы: {MIN_AGE}–{MAX_AGE}.")
        return
    age_min, age_max = parsed
    await state.update_data(age_min=age_min, age_max=age_max)
    await state.set_state(Registration.city)
    await message.answer(
        f"Возраст поиска: <b>{age_min}–{age_max}</b>\n\n"
        f"Из какого вы города?\nОт {MIN_CITY_LENGTH} до {MAX_CITY_LENGTH} символов."
    )

@router.message(Registration.city, F.text)
async def registration_city(message: Message, state: FSMContext):
    city = validate_text(message.text, MIN_CITY_LENGTH, MAX_CITY_LENGTH)
    if not city:
        await message.answer(f"Название города: от {MIN_CITY_LENGTH} до {MAX_CITY_LENGTH} символов.")
        return
    await state.update_data(city=" ".join(city.strip().split()))
    await state.set_state(Registration.bio)
    await message.answer(
        f"Город: <b>{escape(city)}</b>\n\n"
        f"Расскажите о себе: от {MIN_BIO_LENGTH} до {MAX_BIO_LENGTH} символов."
    )

@router.message(Registration.bio, F.text)
async def registration_bio(message: Message, state: FSMContext):
    bio = validate_text(message.text, MIN_BIO_LENGTH, MAX_BIO_LENGTH)
    if not bio:
        await message.answer(f"Описание: от {MIN_BIO_LENGTH} до {MAX_BIO_LENGTH} символов.")
        return
    await state.update_data(bio=bio, photos=[])
    await state.set_state(Registration.photo)
    await message.answer(
        f"Отлично! Теперь пришлите фотографии.\n"
        f"Можно загрузить до {MAX_PHOTOS} штук. После каждого фото нажимайте «✅ Завершить», когда закончите.",
        reply_markup=photos_done_keyboard(),
    )

@router.message(Registration.photo, F.photo)
async def registration_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    photos: List[str] = data.get("photos", [])
    if len(photos) >= MAX_PHOTOS:
        await message.answer(f"Максимум {MAX_PHOTOS} фото. Нажмите «✅ Завершить».")
        return
    photo_id = message.photo[-1].file_id
    photos.append(photo_id)
    await state.update_data(photos=photos)
    await message.answer(
        f"📸 Фото {len(photos)}/{MAX_PHOTOS} добавлено. Пришлите ещё или завершите.",
        reply_markup=photos_done_keyboard(),
    )

@router.message(Registration.photo)
async def registration_photo_invalid(message: Message):
    await message.answer("Пожалуйста, отправьте фото или нажмите «✅ Завершить».")

@router.callback_query(Registration.photo, F.data == "photos_done")
async def registration_photos_done(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    photos: List[str] = data.get("photos", [])
    if not photos:
        await callback.answer("Отправьте хотя бы одно фото.", show_alert=True)
        return
    await callback.answer()
    first_photo = photos[0]

    ok = await add_user_to_db(
        user_id=callback.from_user.id,
        name=data["name"], age=data["age"], gender=data["gender"],
        search_gender=data["search_gender"], city=data["city"], bio=data["bio"],
        photo_id=first_photo, username=callback.from_user.username,
        accepted_rules_at=data.get("accepted_rules_at"),
        age_min=data.get("age_min", MIN_AGE), age_max=data.get("age_max", MAX_AGE),
    )
    if not ok:
        await state.clear()
        await callback.message.answer("Вы заблокированы и не можете зарегистрироваться.")
        return

    # сохраняем все фото
    async with transaction():
        await db.execute("DELETE FROM user_photos WHERE user_id=?", (callback.from_user.id,))
        for i, pid in enumerate(photos):
            await db.execute(
                "INSERT OR IGNORE INTO user_photos (user_id, photo_id, position) VALUES (?, ?, ?)",
                (callback.from_user.id, pid, i),
            )

    await state.clear()
    try:
        await callback.message.edit_text("Регистрация завершена! 🎉")
    except TelegramBadRequest:
        pass
    await send_menu(callback.message, callback.from_user.id, "Добро пожаловать! Главное меню:")

# ============================================================
# ПОИСК
# ============================================================

@router.callback_query(F.data == "search_menu")
async def search_menu(callback: CallbackQuery):
    await callback.answer()
    user = await get_user(callback.from_user.id)
    if not user or not user["is_active"]:
        await safe_edit_or_send(callback, "Ваша анкета неактивна.", reply_markup=back_to_menu_keyboard())
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
            await callback.answer("Ваша анкета неактивна.", show_alert=True)
            return
        async with transaction():
            await db.execute("DELETE FROM views WHERE viewer_id = ?", (uid,))
        await callback.answer("История показов сброшена.")
        await show_profile(callback, state, mode="global")

# ============================================================
# ЛАЙК / ДИЗЛАЙК / REWIND
# ============================================================

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
            await callback.answer("Нельзя лайкнуть себя.", show_alert=True)
            return
        if not await validate_active_card(callback, state, liked_id, mode):
            return await handle_stale_callback(callback)

        other_user = await get_user(liked_id)
        if not other_user or await check_ban(liked_id) or await are_users_blocked(uid, liked_id):
            await callback.answer("Анкета недоступна.", show_alert=True)
            await delete_callback_message(callback)
            await show_next(callback, state, mode)
            return

        await callback.answer()
        result = await add_like(uid, liked_id)
        await delete_callback_message(callback)
        current_user = await get_user(uid)

        if result == LikeResult.REJECTED:
            await callback.message.answer("Не удалось поставить лайк.")
            await show_next(callback, state, mode)
            return
        if result == LikeResult.ALREADY_MATCHED:
            await callback.message.answer("Вы уже в мэтчах с этим пользователем.")
            await show_next(callback, state, mode)
            return
        if result == LikeResult.LIKED:
            if current_user:
                await notify_like(liked_id, current_user)
            await show_next(callback, state, mode)
            return

        # MATCHED
        if not current_user:
            await show_next(callback, state, mode)
            return
        try:
            await callback.message.answer(
                f"💖 <b>Это мэтч!</b>\n\nВы понравились друг другу с {escape(other_user['name'])}.",
                reply_markup=match_keyboard(liked_id),
            )
        except (TelegramBadRequest, TelegramForbiddenError):
            pass
        try:
            await bot.send_message(
                liked_id,
                f"💖 <b>Это мэтч!</b>\n\nВы понравились друг другу с {escape(current_user['name'])}.",
                reply_markup=match_keyboard(uid),
            )
        except (TelegramBadRequest, TelegramForbiddenError):
            pass
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
        # Показываем короткое сообщение с кнопкой "вернуть"
        try:
            await callback.message.answer(
                "Анкета пропущена.",
                reply_markup=rewind_keyboard(p_id, mode),
            )
        except TelegramBadRequest:
            pass
        await show_next(callback, state, mode)

@router.callback_query(F.data.startswith("rewind:"))
async def rewind_profile(callback: CallbackQuery, state: FSMContext):
    parsed = parse_profile_callback(callback.data, "rewind")
    if not parsed:
        await callback.answer()
        return
    p_id, mode = parsed
    uid = callback.from_user.id

    # убираем запись о просмотре
    await remove_view(uid, p_id)

    profile = await get_user(p_id)
    if not profile or not profile["is_active"]:
        await callback.answer("Анкета больше недоступна.", show_alert=True)
        return

    try:
        await callback.message.delete()
    except TelegramBadRequest:
        pass

    await callback.answer("Возвращено")
    try:
        msg = await send_profile_photos(
            callback.message, p_id,
            format_profile(profile),
            profile_keyboard(p_id, mode),
        )
        await state.update_data(
            active_profile_id=p_id,
            active_profile_mode=mode,
            active_profile_msg_id=msg.message_id if msg else None,
        )
    except TelegramBadRequest:
        await callback.message.answer("Не удалось показать анкету.")

# ============================================================
# ПОДРОБНЕЕ
# ============================================================

@router.callback_query(F.data.startswith("details:"))
async def show_details(callback: CallbackQuery, state: FSMContext):
    parsed = parse_profile_callback(callback.data, "details")
    if not parsed:
        await callback.answer()
        return
    p_id, mode = parsed
    if not await validate_active_card(callback, state, p_id, mode):
        return await handle_stale_callback(callback)

    profile = await get_user(p_id)
    if not profile:
        await callback.answer("Анкета недоступна.", show_alert=True)
        return

    username_str = f"@{escape(profile['username'])}" if profile["username"] else "скрыт"
    last_seen = format_last_seen(profile["last_seen_at"]) if "last_seen_at" in profile.keys() else ""
    age_min = profile["age_min"] if "age_min" in profile.keys() else MIN_AGE
    age_max = profile["age_max"] if "age_max" in profile.keys() else MAX_AGE

    text = (
        f"📖 <b>Анкета полностью</b>\n\n"
        f"👤 <b>{escape(profile['name'])}</b>, {profile['age']}\n"
        f"🚻 {escape(profile['gender'])}\n"
        f"🔍 Ищет: {escape(profile['search_gender'])} · 🎂 {age_min}–{age_max}\n"
        f"📍 {escape(profile['city'])}\n"
        f"💬 {username_str}\n"
        f"{last_seen}\n\n"
        f"📝 <b>О себе:</b>\n{escape(profile['bio'])}"
    )
    await callback.message.answer(text)
    await callback.answer()

# ============================================================
# БЛОКИРОВКА / ЖАЛОБЫ
# ============================================================

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
            await callback.answer("Нельзя заблокировать себя.", show_alert=True)
            return
        if not await get_user(blocked_id):
            await callback.answer("Анкета недоступна.", show_alert=True)
            return
        if not await block_user(uid, blocked_id):
            await callback.answer("Не удалось заблокировать.", show_alert=True)
            return
        await callback.answer("Пользователь заблокирован.")
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
            await callback.answer("Нельзя пожаловаться на себя.", show_alert=True)
            return
        if not await get_user(rep_id):
            await callback.answer("Анкета не найдена.", show_alert=True)
            return
        if await report_exists(uid, rep_id):
            await callback.answer("Вы уже жаловались на этого пользователя.", show_alert=True)
            return
        await state.set_state(Report.reason)
        await state.update_data(reported_id=rep_id, mode=mode)
        await callback.answer()
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest:
            pass
        await callback.message.answer(
            f"Опишите причину жалобы.\nМаксимум: {MAX_REPORT_LENGTH} символов.",
            reply_markup=cancel_report_keyboard(),
        )

@router.message(Report.reason, F.text)
async def process_report(message: Message, state: FSMContext):
    reason = validate_text(message.text, 1, MAX_REPORT_LENGTH)
    if not reason:
        await message.answer(f"Жалоба не должна быть пустой (макс. {MAX_REPORT_LENGTH}).")
        return
    data = await state.get_data()
    rep_id = data.get("reported_id")
    if not isinstance(rep_id, int):
        await state.clear()
        await message.answer("Ошибка данных.")
        return
    uid = message.from_user.id
    r_id = await add_report(uid, rep_id, reason)
    if r_id is None:
        await state.clear()
        await message.answer("Не удалось отправить жалобу.", reply_markup=menu_keyboard(await get_likes_count(uid)))
        return
    rep_user = await get_user(rep_id)
    if not rep_user:
        await state.clear()
        await message.answer("Пользователь удалил анкету.", reply_markup=menu_keyboard(await get_likes_count(uid)))
        return
    await add_view(uid, rep_id)
    await message.answer("Жалоба отправлена. Спасибо!", reply_markup=menu_keyboard(await get_likes_count(uid)))

    rep_name = escape(rep_user["name"])
    rep_un = f"@{escape(rep_user['username'])}" if rep_user["username"] else "нет username"
    text = (
        f"🚨 <b>Жалоба #{r_id}</b>\n\n"
        f"<b>От:</b> {message.from_user.mention_html()} (ID: <code>{uid}</code>)\n"
        f"<b>На:</b> {rep_name} (ID: <code>{rep_id}</code>, {rep_un})\n\n"
        f"<b>Причина:</b>\n{escape(reason)}"
    )
    try:
        await bot.send_message(ADMIN_ID, text, reply_markup=admin_report_keyboard(r_id))
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        logger.exception("Fail send report to admin: %s", e)
    finally:
        await state.clear()

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
        return await handle_stale_callback(callback, "Жалоба уже обработана.")
    await callback.answer("Пользователь забанен.")
    original = extract_html(callback.message)
    new_caption = f"{original}\n\n✅ Пользователь <code>{reported_id}</code> забанен."
    try:
        await callback.message.edit_text(new_caption, reply_markup=None)
    except TelegramBadRequest:
        try:
            await callback.message.edit_caption(caption=new_caption, reply_markup=None)
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
        return await handle_stale_callback(callback, "Жалоба уже обработана.")
    await callback.answer("Жалоба отклонена.")
    original = extract_html(callback.message)
    new_caption = f"{original}\n\n❌ Жалоба #{r_id} отклонена."
    try:
        await callback.message.edit_text(new_caption, reply_markup=None)
    except TelegramBadRequest:
        try:
            await callback.message.edit_caption(caption=new_caption, reply_markup=None)
        except TelegramBadRequest:
            pass

# ============================================================
# ПРОФИЛЬ, МУЛЬТИФОТО, СТАТИСТИКА
# ============================================================

@router.callback_query(F.data == "my_profile")
async def my_profile(callback: CallbackQuery):
    await callback.answer()
    user = await get_user(callback.from_user.id)
    if not user:
        await callback.message.answer("Анкета не найдена. Напишите /start.")
        return
    await send_profile_card(callback.message, user)

@router.callback_query(F.data == "my_stats")
async def my_stats(callback: CallbackQuery):
    await callback.answer()
    s = await get_user_stats(callback.from_user.id)
    if not s:
        await callback.message.answer("Нет данных.")
        return
    created = s.get("created_at", "")
    days = "—"
    if created:
        try:
            dt = datetime.strptime(created, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            days = (datetime.now(timezone.utc) - dt).days + 1
        except Exception:
            pass
    await safe_edit_or_send(
        callback,
        f"📊 <b>Ваша статистика</b>\n\n"
        f"❤️ Лайков отправлено: <b>{s['likes_sent']}</b>\n"
        f"💌 Лайков получено: <b>{s['likes_received']}</b>\n"
        f"💑 Мэтчей: <b>{s['matches_count']}</b>\n"
        f"👀 Просмотров вашей анкеты: <b>{s['views_count']}</b>\n"
        f"🚫 Блокировок: <b>{s['blocks_count']}</b>\n"
        f"📅 В сервисе: <b>{days}</b> дн.",
        reply_markup=back_to_menu_keyboard(),
    )

@router.callback_query(F.data == "my_matches")
async def my_matches(callback: CallbackQuery):
    await callback.answer()
    matches = await get_matches(callback.from_user.id)
    if not matches:
        await safe_edit_or_send(
            callback,
            "У вас пока нет мэтчей. Продолжайте искать! 💘",
            reply_markup=back_to_menu_keyboard(),
        )
        return
    builder = InlineKeyboardBuilder()
    for m in matches:
        name = m["name"] if len(m["name"]) <= 30 else m["name"][:29] + "…"
        builder.button(text=f"✉️ {name}", url=f"tg://user?id={m['user_id']}")
    builder.button(text="🔙 В меню", callback_data="menu")
    builder.adjust(1)
    await safe_edit_or_send(
        callback,
        f"💑 <b>Ваши мэтчи ({len(matches)}):</b>\n\nНажмите на имя, чтобы написать.",
        reply_markup=builder.as_markup(),
    )

# ---------- Управление мультифото ----------

@router.callback_query(F.data == "my_photos")
async def my_photos(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    photos = await get_user_photos(callback.from_user.id)
    user = await get_user(callback.from_user.id)
    if not user:
        return
    main = user["photo_id"]
    if not photos:
        await safe_edit_or_send(
            callback,
            "У вас нет фото. Добавьте хотя бы одно, чтобы анкета была активна.",
            reply_markup=InlineKeyboardBuilder()
                .button(text="➕ Добавить", callback_data="photo_add")
                .button(text="🔙 Назад", callback_data="my_profile")
                .adjust(1).as_markup(),
        )
        return
    await safe_edit_or_send(
        callback,
        f"📸 <b>Ваши фото ({len(photos)}/{MAX_PHOTOS})</b>\n\n"
        f"⭐ — главное фото. Нажмите на фото, чтобы управлять.",
        reply_markup=photos_manage_keyboard(photos, main),
    )

@router.callback_query(F.data.startswith("photo_manage:"))
async def photo_manage(callback: CallbackQuery):
    await callback.answer()
    try:
        _, raw_idx = callback.data.split(":")
        idx = int(raw_idx)
    except (ValueError, IndexError):
        return await handle_stale_callback(callback)
    photos = await get_user_photos(callback.from_user.id)
    user = await get_user(callback.from_user.id)
    if not user or idx < 0 or idx >= len(photos):
        return await handle_stale_callback(callback)
    pid = photos[idx]
    is_main = (user["photo_id"] == pid)
    await safe_edit_or_send(
        callback,
        f"📷 <b>Фото {idx+1}</b>{' ⭐' if is_main else ''}\n\nЧто сделать?",
        reply_markup=photo_actions_keyboard(idx, is_main),
    )

@router.callback_query(F.data.startswith("photo_setmain:"))
async def photo_setmain(callback: CallbackQuery):
    await callback.answer()
    try:
        _, raw_idx = callback.data.split(":")
        idx = int(raw_idx)
    except (ValueError, IndexError):
        return await handle_stale_callback(callback)
    photos = await get_user_photos(callback.from_user.id)
    if idx < 0 or idx >= len(photos):
        return await handle_stale_callback(callback)
    ok = await set_main_photo(callback.from_user.id, photos[idx])
    if ok:
        await callback.answer("Главное фото обновлено.", show_alert=False)
    await callback.message.delete()
    await callback.message.answer("Главное фото обновлено.", reply_markup=InlineKeyboardBuilder().button(text="📸 К фото", callback_data="my_photos").as_markup())

@router.callback_query(F.data.startswith("photo_delete:"))
async def photo_delete(callback: CallbackQuery):
    try:
        _, raw_idx = callback.data.split(":")
        idx = int(raw_idx)
    except (ValueError, IndexError):
        return await handle_stale_callback(callback)
    photos = await get_user_photos(callback.from_user.id)
    if idx < 0 or idx >= len(photos):
        return await handle_stale_callback(callback)
    if len(photos) <= 1:
        await callback.answer("Нельзя удалить последнее фото — анкета будет деактивирована.", show_alert=True)
        return
    ok = await delete_user_photo(callback.from_user.id, photos[idx])
    if ok:
        await callback.answer("Фото удалено.")
    else:
        await callback.answer("Не удалось удалить.")
    await callback.message.delete()
    await callback.message.answer("Фото удалено.", reply_markup=InlineKeyboardBuilder().button(text="📸 К фото", callback_data="my_photos").as_markup())

@router.callback_query(F.data == "photo_add")
async def photo_add(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(EditProfile.photo)
    await callback.message.answer(
        f"Отправьте новое фото. Максимум {MAX_PHOTOS} штук.",
        reply_markup=cancel_keyboard(),
    )

@router.message(EditProfile.photo, F.photo)
async def edit_photo_add(message: Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    ok = await add_user_photo(message.from_user.id, photo_id)
    if not ok:
        await message.answer(f"Не удалось добавить фото (лимит {MAX_PHOTOS}).")
        return
    # если у пользователя не было фото — обновим главное и активируем
    user = await get_user(message.from_user.id)
    if user and not user["photo_id"]:
        await update_user_field(message.from_user.id, "photo_id", photo_id)
        await db.execute("UPDATE users SET is_active=1 WHERE user_id=?", (message.from_user.id,))
        await db.commit()
    await state.clear()
    photos = await get_user_photos(message.from_user.id)
    await send_menu(message, message.from_user.id, f"Фото добавлено ({len(photos)}/{MAX_PHOTOS}).")

@router.message(EditProfile.photo)
async def edit_photo_invalid(message: Message):
    await message.answer("Пожалуйста, отправьте фото.")

# ---------- Редактирование полей ----------

@router.callback_query(F.data == "edit_profile")
async def edit_profile_menu(callback: CallbackQuery):
    await callback.answer()
    await delete_callback_message(callback)
    await callback.message.answer("Что вы хотите изменить?", reply_markup=edit_profile_keyboard())

@router.callback_query(F.data.in_({
    "edit_name", "edit_age", "edit_search_gender", "edit_city",
    "edit_bio", "edit_age_range",
}))
async def edit_start(callback: CallbackQuery, state: FSMContext):
    action = callback.data
    await callback.answer()
    if action == "edit_search_gender":
        await state.set_state(EditProfile.search_gender)
        await callback.message.answer("Кого вы ищете?", reply_markup=search_gender_keyboard(with_cancel=True).as_markup())
    elif action == "edit_age_range":
        user = await get_user(callback.from_user.id)
        if not user:
            await callback.message.answer("Анкета не найдена.")
            return
        age_min = user["age_min"] if "age_min" in user.keys() else MIN_AGE
        age_max = user["age_max"] if "age_max" in user.keys() else MAX_AGE
        await state.set_state(EditProfile.age_range_min)
        await callback.message.answer(
            f"Текущий диапазон: <b>{age_min}–{age_max}</b>\n\n"
            f"Введите <b>минимальный</b> возраст (от {MIN_AGE} до {MAX_AGE}):",
            reply_markup=cancel_keyboard(),
        )
    elif action == "edit_name":
        await state.set_state(EditProfile.name)
        await callback.message.answer(f"Введите новое имя (от {MIN_NAME_LENGTH} до {MAX_NAME_LENGTH}):", reply_markup=cancel_keyboard())
    elif action == "edit_age":
        await state.set_state(EditProfile.age)
        await callback.message.answer(f"Введите новый возраст ({MIN_AGE}–{MAX_AGE}):", reply_markup=cancel_keyboard())
    elif action == "edit_city":
        await state.set_state(EditProfile.city)
        await callback.message.answer(f"Введите новый город:", reply_markup=cancel_keyboard())
    elif action == "edit_bio":
        await state.set_state(EditProfile.bio)
        await callback.message.answer(f"Введите новое описание ({MIN_BIO_LENGTH}–{MAX_BIO_LENGTH}):", reply_markup=cancel_keyboard())

@router.callback_query(EditProfile.search_gender, F.data.startswith("search_gender_"))
async def edit_search_gender_save(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    code = parts[2] if len(parts) > 2 else None
    sg = search_gender_text(code)
    if not sg:
        await callback.answer("Некорректно.", show_alert=True)
        return
    await callback.answer()
    await update_user_field(callback.from_user.id, "search_gender", sg)
    await state.clear()
    await callback.message.edit_text(f"Обновлено: <b>{escape(sg)}</b>.")
    await callback.message.answer("Главное меню:", reply_markup=menu_keyboard(await get_likes_count(callback.from_user.id)))

@router.message(EditProfile.age_range_min, F.text)
async def edit_age_range_min(message: Message, state: FSMContext):
    value = validate_age(message.text)
    if value is None:
        await message.answer(f"Введите число от {MIN_AGE} до {MAX_AGE}.")
        return
    await state.update_data(age_range_min=value)
    await state.set_state(EditProfile.age_range_max)
    await message.answer(f"Минимум: <b>{value}</b>.\nВведите максимум (от {value} до {MAX_AGE}):", reply_markup=cancel_keyboard())

@router.message(EditProfile.age_range_max, F.text)
async def edit_age_range_max(message: Message, state: FSMContext):
    value = validate_age(message.text)
    if value is None:
        await message.answer(f"Введите число от {MIN_AGE} до {MAX_AGE}.")
        return
    data = await state.get_data()
    age_min = data.get("age_range_min", MIN_AGE)
    if value < age_min:
        await message.answer(f"Максимум не может быть меньше {age_min}.")
        return
    await update_user_field(message.from_user.id, "age_min", age_min)
    await update_user_field(message.from_user.id, "age_max", value)
    await state.clear()
    await send_menu(message, message.from_user.id, f"Диапазон: <b>{age_min}–{value}</b>.")

@router.message(EditProfile.name, F.text)
async def edit_name_save(message: Message, state: FSMContext):
    name = validate_text(message.text, MIN_NAME_LENGTH, MAX_NAME_LENGTH)
    if not name:
        await message.answer(f"Имя: от {MIN_NAME_LENGTH} до {MAX_NAME_LENGTH}.")
        return
    await update_user_field(message.from_user.id, "name", name)
    await state.clear()
    await send_menu(message, message.from_user.id, "Имя обновлено.")

@router.message(EditProfile.age, F.text)
async def edit_age_save(message: Message, state: FSMContext):
    age = validate_age(message.text)
    if age is None:
        await message.answer(f"Возраст: {MIN_AGE}–{MAX_AGE}.")
        return
    await update_user_field(message.from_user.id, "age", age)
    await state.clear()
    await send_menu(message, message.from_user.id, "Возраст обновлён.")

@router.message(EditProfile.city, F.text)
async def edit_city_save(message: Message, state: FSMContext):
    city = validate_text(message.text, MIN_CITY_LENGTH, MAX_CITY_LENGTH)
    if not city:
        await message.answer(f"Город: от {MIN_CITY_LENGTH} до {MAX_CITY_LENGTH}.")
        return
    await update_user_field(message.from_user.id, "city", " ".join(city.strip().split()))
    await state.clear()
    await send_menu(message, message.from_user.id, "Город обновлён.")

@router.message(EditProfile.bio, F.text)
async def edit_bio_save(message: Message, state: FSMContext):
    bio = validate_text(message.text, MIN_BIO_LENGTH, MAX_BIO_LENGTH)
    if not bio:
        await message.answer(f"Описание: {MIN_BIO_LENGTH}–{MAX_BIO_LENGTH}.")
        return
    await update_user_field(message.from_user.id, "bio", bio)
    await state.clear()
    await send_menu(message, message.from_user.id, "Описание обновлено.")

@router.callback_query(F.data == "cancel_edit")
async def cancel_editing(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    try:
        await callback.message.delete()
    except TelegramBadRequest:
        pass
    await callback.message.answer("Отменено.", reply_markup=menu_keyboard(await get_likes_count(callback.from_user.id)))

@router.callback_query(F.data == "delete_profile")
async def delete_profile_confirm(callback: CallbackQuery):
    await callback.answer()
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, удалить", callback_data="confirm_delete")
    builder.button(text="🔙 Назад", callback_data="edit_profile")
    builder.adjust(1)
    await callback.message.edit_text(
        "Удалить анкету?\n\nБудут удалены профиль, фото, лайки, просмотры, блокировки.",
        reply_markup=builder.as_markup(),
    )

@router.callback_query(F.data == "confirm_delete")
async def delete_profile_execute(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await delete_user(callback.from_user.id)
    await callback.message.edit_text("Анкета удалена.\n\nДля новой регистрации напишите /start.")

@router.callback_query(F.data == "menu")
async def back_to_menu(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    likes = await get_likes_count(callback.from_user.id)
    await safe_edit_or_send(callback, "Главное меню:", reply_markup=menu_keyboard(likes))

# ============================================================
# АДМИН-ПАНЕЛЬ
# ============================================================

@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await message.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu_keyboard())

@router.callback_query(F.data == "admin_panel:close")
async def admin_close(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await handle_stale_callback(callback, "Недоступно.")
    await callback.answer()
    await state.clear()
    try:
        await callback.message.delete()
    except TelegramBadRequest:
        pass

@router.callback_query(F.data == "admin_panel:menu")
async def admin_menu(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await handle_stale_callback(callback, "Недоступно.")
    await callback.answer()
    await state.clear()
    await safe_edit_or_send(callback, "🛠 <b>Админ-панель</b>", reply_markup=admin_menu_keyboard())

@router.callback_query(F.data == "admin_panel:stats")
async def admin_stats(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await handle_stale_callback(callback, "Недоступно.")
    await callback.answer()
    s = await get_stats()
    await safe_edit_or_send(
        callback,
        "📊 <b>Статистика</b>\n\n"
        f"👥 Анкет: {s['users_total']} (активных: {s['users_active']})\n"
        f"🟢 Онлайн (10 мин): {s['online_now']}\n"
        f"🆕 Новых за сутки: {s['new_today']}\n"
        f"❤️ Лайков: {s['likes']}\n"
        f"💑 Мэтчей: {s['matches']}\n"
        f"👀 Просмотров: {s['views']}\n"
        f"🚫 Блокировок: {s['blocks']}\n"
        f"⛔ Банов: {s['bans']}\n"
        f"🚩 Жалоб в ожидании: {s['reports_pending']}",
        reply_markup=InlineKeyboardBuilder()
            .button(text="🔄 Обновить", callback_data="admin_panel:stats")
            .button(text="🔙 В админку", callback_data="admin_panel:menu")
            .adjust(2).as_markup(),
    )

@router.callback_query(F.data.startswith("admin_panel:reports:"))
async def admin_reports(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await handle_stale_callback(callback, "Недоступно.")
    try:
        _, _, raw_page = callback.data.split(":")
        page = max(0, int(raw_page))
    except (ValueError, IndexError):
        return await handle_stale_callback(callback)

    await callback.answer()
    limit = 5
    reports = await get_pending_reports(offset=page * limit, limit=limit)
    total = await count_pending_reports()

    if not reports:
        await safe_edit_or_send(
            callback,
            f"🚩 Жалоб в ожидании: <b>{total}</b>.\n\nПусто.",
            reply_markup=InlineKeyboardBuilder()
                .button(text="🔙 В админку", callback_data="admin_panel:menu")
                .as_markup(),
        )
        return

    text = f"🚩 <b>Жалобы (стр. {page + 1}, всего {total})</b>\n\n"
    for r in reports:
        rep_name = escape(r["reported_name"] or "—")
        rep_un = f"@{escape(r['reported_username'])}" if r["reported_username"] else "нет username"
        text += (
            f"<b>#{r['id']}</b> | {escape(r['reason'][:120])}\n"
            f"👤 От: <code>{r['reporter_id']}</code> · На: {rep_name} (<code>{r['reported_id']}</code>, {rep_un})\n"
            f"🕒 {r['timestamp']}\n\n"
        )

    await safe_edit_or_send(
        callback, text,
        reply_markup=admin_reports_keyboard(page, total, has_next=(page + 1) * limit < total),
    )

@router.callback_query(F.data == "admin_panel:broadcast")
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await handle_stale_callback(callback, "Недоступно.")
    await callback.answer()
    await state.set_state(AdminStates.broadcast_text)
    await callback.message.answer(
        "📢 Отправьте текст рассылки (можно с HTML-разметкой).\nОтмена: /cancel.",
        reply_markup=cancel_keyboard(),
    )

@router.message(AdminStates.broadcast_text, F.text)
async def admin_broadcast_send(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
    text = message.html_text or escape(message.text or "")
    await state.clear()
    await message.answer("Рассылка запущена...")
    user_ids = await get_all_active_user_ids()
    sent, failed = 0, 0
    for uid in user_ids:
        try:
            await bot.send_message(uid, text)
            sent += 1
        except (TelegramBadRequest, TelegramForbiddenError):
            failed += 1
        except Exception as e:
            logger.warning("Broadcast to %s failed: %s", uid, e)
            failed += 1
        await asyncio.sleep(0.05)  # ~20 msg/sec
    await message.answer(
        f"✅ Рассылка завершена.\n\nДоставлено: <b>{sent}</b>\nОшибок: <b>{failed}</b>"
    )

@router.callback_query(F.data == "admin_panel:find")
async def admin_find_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return await handle_stale_callback(callback, "Недоступно.")
    await callback.answer()
    await state.set_state(AdminStates.find_user)
    await callback.message.answer(
        "🔍 Введите ID, @username или имя для поиска.\nОтмена: /cancel.",
        reply_markup=cancel_keyboard(),
    )

@router.message(AdminStates.find_user, F.text)
async def admin_find_run(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
    query = message.text.strip()
    await state.clear()
    users = await find_users_by_query(query)
    if not users:
        await message.answer("Ничего не найдено.", reply_markup=admin_menu_keyboard())
        return
    for u in users:
        last_seen = format_last_seen(u["last_seen_at"]) if "last_seen_at" in u.keys() else ""
        uname = f"@{u['username']}" if u["username"] else "—"
        text = (
            f"👤 <b>{escape(u['name'])}</b>, {u['age']}\n"
            f"ID: <code>{u['user_id']}</code> | {uname}\n"
            f"📍 {escape(u['city'])} | {escape(u['gender'])} → {escape(u['search_gender'])}\n"
            f"🎂 Поиск: {u['age_min']}–{u['age_max']}\n"
            f"📅 Создан: {u['created_at']}\n"
            f"Статус: {'✅ активна' if u['is_active'] else '❌ неактивна'}\n"
            f"{last_seen}"
        )
        builder = InlineKeyboardBuilder()
        builder.button(text="🚫 Забанить", callback_data=f"admin_action:ban:{u['user_id']}")
        builder.button(text="🗑 Удалить", callback_data=f"admin_action:delete:{u['user_id']}")
        builder.adjust(2)
        await message.answer(text, reply_markup=builder.as_markup())

@router.callback_query(F.data.startswith("admin_action:"))
async def admin_action(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return await handle_stale_callback(callback, "Недоступно.")
    try:
        _, action, raw_uid = callback.data.split(":")
        target_uid = int(raw_uid)
    except (ValueError, IndexError):
        return await handle_stale_callback(callback, "Некорректные данные.")

    if action == "ban":
        async with transaction():
            await db.execute(
                "INSERT INTO bans (user_id, reason) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET reason=excluded.reason",
                (target_uid, f"Ручной бан администратором {ADMIN_ID}"),
            )
            await db.execute("DELETE FROM likes WHERE liker_id=? OR liked_id=?", (target_uid, target_uid))
            await db.execute("DELETE FROM matches WHERE user1_id=? OR user2_id=?", (target_uid, target_uid))
        await callback.answer("Забанен.")
        try:
            await bot.send_message(target_uid, "Вы заблокированы администрацией.")
        except Exception:
            pass
    elif action == "delete":
        await delete_user(target_uid)
        await callback.answer("Удалён.")
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass

# ============================================================
# КАТЧ-ОЛЛ
# ============================================================

@router.message(StateFilter(
    Registration.name, Registration.age, Registration.city, Registration.bio,
    EditProfile.name, EditProfile.age, EditProfile.city, EditProfile.bio,
    EditProfile.age_range_min, EditProfile.age_range_max,
))
async def text_required_invalid(message: Message):
    await message.answer("Пожалуйста, отправьте значение текстом.")

@router.callback_query()
async def unknown_callback(callback: CallbackQuery):
    try:
        await callback.answer("Эта кнопка устарела. Откройте меню заново.", show_alert=True)
    except Exception:
        pass

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
        "Сообщение не распознано. Напишите /start для меню."
    )

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
        if db:
            await db.close()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен.")