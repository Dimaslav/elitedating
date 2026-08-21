import asyncio
import logging
import os
import secrets
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
from html import escape
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, List

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
    LabeledPrice,
    Message,
    PreCheckoutQuery,
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
DB_SCHEMA_VERSION = 5
RULES_VERSION = 2

STARS_PROVIDER_TOKEN = ""
SUPPORT_CONTACT = os.getenv("SUPPORT_CONTACT", "@your_support_username")

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
ALLOWED_USER_FIELDS = {"name", "age", "gender", "search_gender", "city", "bio", "photo_id", "username"}
KNOWN_COMMANDS = {
    "/start",
    "/delete",
    "/help",
    "/stats",
    "/unban",
    "/donate",
    "/admin_users",
    "/paysupport",
}

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


class PaymentResult(Enum):
    PAID = 1
    ALREADY_PAID = 2
    INVALID = 3


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


def is_invalid_photo_error(exc: TelegramBadRequest) -> bool:
    text = str(exc).lower()
    return any(
        marker in text
        for marker in (
            "wrong file identifier",
            "photo_invalid",
            "wrong file_id",
        )
    )


# ============================================================
# PER-USER ACTION LOCKS
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
    city = State()
    bio = State()
    photo = State()


class EditProfile(StatesGroup):
    name = State()
    age = State()
    search_gender = State()
    city = State()
    bio = State()
    photo = State()


class Report(StatesGroup):
    reason = State()


# ============================================================
# БАЗА ДАННЫХ И МИГРАЦИИ
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

    await migrate_db()
    logger.info("База данных успешно инициализирована.")


async def migrate_db() -> None:
    async with db.execute("PRAGMA user_version") as cursor:
        version = (await cursor.fetchone())[0]

    if version > DB_SCHEMA_VERSION:
        raise RuntimeError("База данных новее текущей версии приложения.")

    if version < 1:
        await _migrate_v1()
        async with db_lock:
            await db.execute("PRAGMA user_version = 1")
            await db.commit()

    if version < 2:
        await _migrate_v2()
        async with db_lock:
            await db.execute("PRAGMA user_version = 2")
            await db.commit()

    if version < 3:
        await _migrate_v3()
        async with db_lock:
            await db.execute("PRAGMA user_version = 3")
            await db.commit()

    if version < 4:
        await _migrate_v4()
        async with db_lock:
            await db.execute("PRAGMA user_version = 4")
            await db.commit()

    if version < 5:
        await _migrate_v5()
        async with db_lock:
            await db.execute("PRAGMA user_version = 5")
            await db.commit()


async def _migrate_v1() -> None:
    async with transaction():
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
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
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS views (
                viewer_id INTEGER NOT NULL,
                viewed_id INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (viewer_id, viewed_id),
                CHECK (viewer_id != viewed_id),
                FOREIGN KEY (viewer_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (viewed_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS likes (
                liker_id INTEGER NOT NULL,
                liked_id INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (liker_id, liked_id),
                CHECK (liker_id != liked_id),
                FOREIGN KEY (liker_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (liked_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS matches (
                user1_id INTEGER NOT NULL,
                user2_id INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user1_id, user2_id),
                CHECK (user1_id < user2_id),
                FOREIGN KEY (user1_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (user2_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS reports (
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
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS bans (
                user_id INTEGER PRIMARY KEY,
                reason TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS blocks (
                blocker_id INTEGER NOT NULL,
                blocked_id INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (blocker_id, blocked_id),
                CHECK (blocker_id != blocked_id),
                FOREIGN KEY (blocker_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (blocked_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
            """
        )

        # Индексы для reports
        await db.execute("DROP INDEX IF EXISTS idx_reports_unique")
        await db.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_reports_pending_unique
            ON reports(reporter_id, reported_id)
            WHERE status = 'pending'
            """
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_reports_status_created ON reports(status, timestamp)")

        # Индексы для остальных таблиц
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_active_gender ON users(is_active, gender)")
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_users_gender_city_nocase ON users(gender, city COLLATE NOCASE)"
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_likes_liked ON likes(liked_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_views_viewed ON views(viewed_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_blocks_blocked ON blocks(blocked_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_matches_user2 ON matches(user2_id)")


async def _migrate_v2() -> None:
    async with transaction():
        async with db.execute("PRAGMA table_info(users)") as cursor:
            columns = [row[1] for row in await cursor.fetchall()]
        if "is_active" not in columns:
            await db.execute("ALTER TABLE users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
        if "accepted_rules_version" not in columns:
            await db.execute("ALTER TABLE users ADD COLUMN accepted_rules_version INTEGER")


async def _migrate_v3() -> None:
    async with transaction():
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS donations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL CHECK(amount > 0),
                currency TEXT NOT NULL DEFAULT 'XTR',
                invoice_payload TEXT NOT NULL,
                telegram_payment_charge_id TEXT UNIQUE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS payment_intents (
                payload TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL CHECK(amount > 0),
                currency TEXT NOT NULL DEFAULT 'XTR',
                status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'paid', 'cancelled')),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                paid_at DATETIME,
                telegram_payment_charge_id TEXT UNIQUE,
                expires_at DATETIME,
                CHECK (
                    (status = 'pending' AND telegram_payment_charge_id IS NULL)
                    OR
                    (status = 'paid' AND telegram_payment_charge_id IS NOT NULL)
                    OR
                    status = 'cancelled'
                )
            )
            """
        )


async def _migrate_v4() -> None:
    async with transaction():
        # Пересоздаём donations с NOT NULL UNIQUE для transaction ID
        async with db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='donations'"
        ) as cursor:
            donations_exists = await cursor.fetchone() is not None

        if donations_exists:
            # Проверяем наличие telegram_payment_charge_id
            async with db.execute("PRAGMA table_info(donations)") as cursor:
                cols = [row[1] for row in await cursor.fetchall()]

            if "telegram_payment_charge_id" in cols:
                await db.execute(
                    """
                    CREATE TABLE donations_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        amount INTEGER NOT NULL CHECK(amount > 0),
                        currency TEXT NOT NULL DEFAULT 'XTR',
                        invoice_payload TEXT NOT NULL,
                        telegram_payment_charge_id TEXT NOT NULL UNIQUE,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                # Переносим, заменяя NULL на сгенерированный legacy ID
                await db.execute(
                    """
                    INSERT INTO donations_new (
                        id, user_id, amount, currency, invoice_payload,
                        telegram_payment_charge_id, created_at
                    )
                    SELECT
                        id,
                        user_id,
                        amount,
                        'XTR',
                        invoice_payload,
                        COALESCE(telegram_payment_charge_id, 'legacy_' || id),
                        created_at
                    FROM donations
                    """
                )
                await db.execute("DROP TABLE donations")
                await db.execute("ALTER TABLE donations_new RENAME TO donations")
            else:
                # Старая таблица без charge_id – пересоздаём с добавлением
                await db.execute(
                    """
                    CREATE TABLE donations_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        amount INTEGER NOT NULL CHECK(amount > 0),
                        currency TEXT NOT NULL DEFAULT 'XTR',
                        invoice_payload TEXT NOT NULL,
                        telegram_payment_charge_id TEXT NOT NULL UNIQUE,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                await db.execute(
                    """
                    INSERT INTO donations_new (id, user_id, amount, currency, invoice_payload, created_at)
                    SELECT id, user_id, amount, 'XTR', invoice_payload, created_at
                    FROM donations
                    """
                )
                await db.execute("DROP TABLE donations")
                await db.execute("ALTER TABLE donations_new RENAME TO donations")

        # Уникальный индекс на invoice_payload для гарантии "один платёж - одна запись"
        await db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_donations_invoice_payload ON donations(invoice_payload)"
        )


async def _migrate_v5() -> None:
    """Пересоздаём reports с ON DELETE SET NULL и добавляем snapshots."""
    async with transaction():
        await db.execute(
            """
            CREATE TABLE reports_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reporter_id INTEGER,
                reported_id INTEGER,
                reported_name_snapshot TEXT,
                reported_username_snapshot TEXT,
                reason TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'accepted', 'rejected')),
                reviewed_by INTEGER,
                reviewed_at DATETIME,
                resolution TEXT,
                FOREIGN KEY (reporter_id) REFERENCES users(user_id) ON DELETE SET NULL,
                FOREIGN KEY (reported_id) REFERENCES users(user_id) ON DELETE SET NULL
            )
            """
        )
        # Копируем данные
        await db.execute(
            """
            INSERT INTO reports_new (
                id, reporter_id, reported_id, reason, timestamp,
                status, reviewed_by, reviewed_at, resolution
            )
            SELECT id, reporter_id, reported_id, reason, timestamp,
                   status, reviewed_by, reviewed_at, resolution
            FROM reports
            """
        )
        await db.execute("DROP TABLE reports")
        await db.execute("ALTER TABLE reports_new RENAME TO reports")

        # Уникальный индекс на pending, только когда оба ID не NULL
        await db.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_reports_pending_unique
            ON reports(reporter_id, reported_id)
            WHERE status = 'pending' AND reporter_id IS NOT NULL AND reported_id IS NOT NULL
            """
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_reports_status_created ON reports(status, timestamp)"
        )


# ============================================================
# ВСПОМОГАТЕЛЬНЫЙ ПОСТРОИТЕЛЬ УСЛОВИЙ ELIGIBILITY
# ============================================================
def build_eligibility_excludes(user_id: int, user_alias: str = "u") -> Tuple[str, List[int]]:
    sql = f"""
        AND NOT EXISTS (SELECT 1 FROM bans b WHERE b.user_id = {user_alias}.user_id)
        AND NOT EXISTS (SELECT 1 FROM views v WHERE v.viewer_id=? AND v.viewed_id={user_alias}.user_id)
        AND NOT EXISTS (SELECT 1 FROM likes l WHERE l.liker_id=? AND l.liked_id={user_alias}.user_id)
        AND NOT EXISTS (SELECT 1 FROM blocks b WHERE b.blocker_id=? AND b.blocked_id={user_alias}.user_id)
        AND NOT EXISTS (SELECT 1 FROM blocks b WHERE b.blocker_id={user_alias}.user_id AND b.blocked_id=?)
        AND NOT EXISTS (
            SELECT 1 FROM matches m
            WHERE (m.user1_id=? AND m.user2_id={user_alias}.user_id)
               OR (m.user2_id=? AND m.user1_id={user_alias}.user_id)
        )
        AND NOT EXISTS (
            SELECT 1 FROM reports r
            WHERE r.reporter_id=? AND r.reported_id={user_alias}.user_id
        )
    """
    params = [user_id] * 7
    return sql, params


# ============================================================
# ФУНКЦИИ РАБОТЫ С БАЗОЙ ДАННЫХ
# ============================================================
async def get_user_status(user_id: int) -> dict:
    async with db_lock:
        async with db.execute(
            """
            SELECT
                EXISTS(SELECT 1 FROM bans WHERE user_id = ?) AS is_banned,
                (SELECT accepted_rules_version FROM users WHERE user_id = ?) AS accepted_rules_version,
                EXISTS(SELECT 1 FROM users WHERE user_id = ?) AS exists
            """,
            (user_id, user_id, user_id),
        ) as cursor:
            row = await cursor.fetchone()
    return {
        "is_banned": bool(row["is_banned"]),
        "rules_version": row["accepted_rules_version"],
        "exists": bool(row["exists"]),
    }


async def check_ban(user_id: int) -> bool:
    async with db_lock:
        async with db.execute("SELECT 1 FROM bans WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone() is not None


async def get_user(user_id: int) -> Optional[aiosqlite.Row]:
    async with db_lock:
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()


async def add_user_to_db(
    user_id: int,
    name: str,
    age: int,
    gender: str,
    search_gender: str,
    city: str,
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
            """
            INSERT INTO users (
                user_id, name, age, gender, search_gender, city, bio, photo_id,
                username, accepted_rules_at, is_active, accepted_rules_version
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                name=excluded.name,
                age=excluded.age,
                gender=excluded.gender,
                search_gender=excluded.search_gender,
                city=excluded.city,
                bio=excluded.bio,
                photo_id=excluded.photo_id,
                username=excluded.username,
                accepted_rules_at=excluded.accepted_rules_at,
                is_active=1,
                accepted_rules_version=excluded.accepted_rules_version
            """,
            (user_id, name, age, gender, search_gender, city, bio, photo_id, username, accepted_rules_at, RULES_VERSION),
        )
    return True


async def update_user_field(user_id: int, field: str, value: Any) -> None:
    if field not in ALLOWED_USER_FIELDS:
        raise ValueError(f"Недопустимое поле: {field}")
    async with db_lock:
        await db.execute(f"UPDATE users SET {field} = ? WHERE user_id = ?", (value, user_id))
        await db.commit()


async def update_photo(user_id: int, photo_id: str) -> bool:
    async with transaction():
        async with db.execute("SELECT 1 FROM bans WHERE user_id=?", (user_id,)) as cur:
            if await cur.fetchone():
                return False
        cur = await db.execute(
            "UPDATE users SET photo_id=?, is_active=1 WHERE user_id=?",
            (photo_id, user_id),
        )
        return cur.rowcount == 1


async def deactivate_user_profile(user_id: int) -> None:
    async with db_lock:
        await db.execute("UPDATE users SET is_active=0 WHERE user_id=?", (user_id,))
        await db.commit()


async def set_rules_accepted(user_id: int, accepted_at: str) -> None:
    async with db_lock:
        await db.execute(
            "UPDATE users SET accepted_rules_at = ?, accepted_rules_version = ? WHERE user_id = ?",
            (accepted_at, RULES_VERSION, user_id),
        )
        await db.commit()


async def delete_user(user_id: int) -> None:
    """Удаление пользователя. Reports теперь сохраняются с SET NULL."""
    async with transaction():
        await db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))


async def get_likes_count(user_id: int) -> int:
    exclude_sql, exclude_params = build_eligibility_excludes(user_id, "u")
    query = f"""
        SELECT COUNT(*)
        FROM likes AS l
        JOIN users AS u ON u.user_id = l.liker_id
        WHERE l.liked_id = ?
          AND u.is_active = 1
          {exclude_sql}
    """
    params = [user_id] + exclude_params
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
        await db.execute(
            "INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)",
            (viewer_id, viewed_id),
        )
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

        async with db.execute(
            "SELECT 1 FROM bans WHERE user_id IN (?, ?) LIMIT 1",
            (liker_id, liked_id),
        ) as cur:
            if await cur.fetchone():
                return LikeResult.REJECTED

        async with db.execute(
            "SELECT 1 FROM blocks WHERE (blocker_id=? AND blocked_id=?) OR (blocker_id=? AND blocked_id=?)",
            (liker_id, liked_id, liked_id, liker_id),
        ) as cur:
            if await cur.fetchone():
                return LikeResult.REJECTED

        async with db.execute(
            "SELECT 1 FROM matches WHERE user1_id=? AND user2_id=?",
            (u1, u2),
        ) as cur:
            if await cur.fetchone():
                return LikeResult.ALREADY_MATCHED

        await db.execute(
            "INSERT OR IGNORE INTO likes (liker_id, liked_id) VALUES (?, ?)",
            (liker_id, liked_id),
        )
        await db.execute(
            "INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)",
            (liker_id, liked_id),
        )

        async with db.execute(
            "SELECT 1 FROM likes WHERE liker_id=? AND liked_id=?",
            (liked_id, liker_id),
        ) as cur:
            mutual = await cur.fetchone() is not None

        if not mutual:
            return LikeResult.LIKED

        await db.execute(
            "DELETE FROM likes WHERE (liker_id=? AND liked_id=?) OR (liker_id=? AND liked_id=?)",
            (liker_id, liked_id, liked_id, liker_id),
        )
        await db.execute(
            "INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)",
            (liked_id, liker_id),
        )

        cursor = await db.execute(
            "INSERT OR IGNORE INTO matches (user1_id, user2_id) VALUES (?, ?)",
            (u1, u2),
        )
        return LikeResult.MATCHED if cursor.rowcount > 0 else LikeResult.ALREADY_MATCHED


async def reject_like(user_id: int, liker_id: int) -> None:
    async with transaction():
        await db.execute(
            "DELETE FROM likes WHERE liker_id = ? AND liked_id = ?",
            (liker_id, user_id),
        )
        await db.execute(
            "INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)",
            (user_id, liker_id),
        )


async def block_user(blocker_id: int, blocked_id: int) -> None:
    if blocker_id == blocked_id:
        return
    u1, u2 = min(blocker_id, blocked_id), max(blocker_id, blocked_id)
    async with transaction():
        await db.execute(
            "INSERT OR IGNORE INTO blocks (blocker_id, blocked_id) VALUES (?, ?)",
            (blocker_id, blocked_id),
        )
        await db.execute(
            "DELETE FROM likes WHERE (liker_id=? AND liked_id=?) OR (liker_id=? AND liked_id=?)",
            (blocker_id, blocked_id, blocked_id, blocker_id),
        )
        await db.execute(
            "DELETE FROM matches WHERE user1_id=? AND user2_id=?",
            (u1, u2),
        )
        await db.execute(
            "INSERT OR IGNORE INTO views (viewer_id, viewed_id) VALUES (?, ?)",
            (blocker_id, blocked_id),
        )


async def report_exists(reporter_id: int, reported_id: int) -> bool:
    """Проверяем, существует ли уже любая жалоба от этого пользователя на другого."""
    async with db_lock:
        async with db.execute(
            "SELECT 1 FROM reports WHERE reporter_id=? AND reported_id=? LIMIT 1",
            (reporter_id, reported_id),
        ) as cur:
            return await cur.fetchone() is not None


async def add_report(reporter_id: int, reported_id: int, reason: str) -> Optional[int]:
    if reporter_id == reported_id:
        return None
    async with transaction():
        # Проверяем, что цель активна и не забанена
        async with db.execute(
            """
            SELECT 1 FROM users u
            WHERE u.user_id=? AND u.is_active=1
              AND NOT EXISTS (SELECT 1 FROM bans b WHERE b.user_id=u.user_id)
            """,
            (reported_id,),
        ) as cur:
            if not await cur.fetchone():
                return None

        # Получаем snapshots для сохранения данных о нарушителе
        async with db.execute(
            "SELECT name, username FROM users WHERE user_id=?",
            (reported_id,),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        reported_name = row["name"]
        reported_username = row["username"]

        try:
            cur = await db.execute(
                """
                INSERT INTO reports (
                    reporter_id, reported_id, reported_name_snapshot,
                    reported_username_snapshot, reason
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (reporter_id, reported_id, reported_name, reported_username, reason),
            )
            return cur.lastrowid
        except aiosqlite.IntegrityError:
            return None


async def ban_user_from_report(report_id: int, admin_id: int) -> Optional[int]:
    async with transaction():
        cur = await db.execute(
            """
            UPDATE reports
            SET status='accepted', reviewed_by=?, reviewed_at=CURRENT_TIMESTAMP, resolution='banned'
            WHERE id=? AND status='pending'
            """,
            (admin_id, report_id),
        )
        if cur.rowcount == 0:
            return None

        async with db.execute("SELECT reported_id FROM reports WHERE id=?", (report_id,)) as cur:
            row = await cur.fetchone()
            if not row or row["reported_id"] is None:
                return None

        reported_id = row["reported_id"]

        await db.execute(
            """
            INSERT INTO bans (user_id, reason) VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET reason=excluded.reason
            """,
            (reported_id, f"Бан по жалобе #{report_id}"),
        )
        await db.execute(
            "DELETE FROM likes WHERE liker_id=? OR liked_id=?",
            (reported_id, reported_id),
        )
        await db.execute(
            "DELETE FROM matches WHERE user1_id=? OR user2_id=?",
            (reported_id, reported_id),
        )

        await db.execute(
            """
            UPDATE reports
            SET status='accepted',
                reviewed_by=?,
                reviewed_at=CURRENT_TIMESTAMP,
                resolution='already_banned'
            WHERE reported_id=?
              AND id<>?
              AND status='pending'
            """,
            (admin_id, reported_id, report_id),
        )

        return reported_id


async def reject_report(report_id: int, admin_id: int) -> bool:
    async with transaction():
        cur = await db.execute(
            """
            UPDATE reports SET status='rejected', reviewed_by=?, reviewed_at=CURRENT_TIMESTAMP, resolution='rejected'
            WHERE id=? AND status='pending'
            """,
            (admin_id, report_id),
        )
        return cur.rowcount > 0


async def unban_user(user_id: int) -> bool:
    async with transaction():
        cur = await db.execute("DELETE FROM bans WHERE user_id=?", (user_id,))
        return cur.rowcount > 0


async def get_random_profile(
    user_id: int,
    user_gender: str,
    search_gender: str,
    user_city: Optional[str] = None,
    strict_city: bool = False,
) -> Optional[aiosqlite.Row]:
    exclude_sql, exclude_params = build_eligibility_excludes(user_id, "u")

    query = f"""
        SELECT * FROM users u
        WHERE u.user_id != ?
          AND u.is_active = 1
          {exclude_sql}
    """
    params = [user_id] + exclude_params

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
        WHERE l.liked_id = ?
          AND u.is_active = 1
          {exclude_sql}
        ORDER BY l.created_at DESC LIMIT 1
    """
    params = [user_id] + exclude_params
    async with db_lock:
        async with db.execute(query, tuple(params)) as cursor:
            return await cursor.fetchone()


async def verify_like_exists(liker_id: int, liked_id: int) -> bool:
    async with db_lock:
        async with db.execute(
            "SELECT 1 FROM likes WHERE liker_id=? AND liked_id=?",
            (liker_id, liked_id),
        ) as cur:
            return await cur.fetchone() is not None


async def get_matches(user_id: int) -> list:
    async with db_lock:
        async with db.execute(
            """
            SELECT u.user_id, u.name, m.created_at
            FROM matches m
            JOIN users u ON u.user_id = CASE WHEN m.user1_id = ? THEN m.user2_id ELSE m.user1_id END
            WHERE (m.user1_id = ? OR m.user2_id = ?) AND u.user_id != ? AND u.is_active = 1
              AND NOT EXISTS (SELECT 1 FROM bans b WHERE b.user_id = u.user_id)
            ORDER BY m.created_at DESC LIMIT 50
            """,
            (user_id, user_id, user_id, user_id),
        ) as cursor:
            return await cursor.fetchall()


async def get_stats() -> Dict[str, int]:
    async with db_lock:
        async with db.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM users) AS users_total,
                (SELECT COUNT(*) FROM users u
                 WHERE u.is_active = 1
                   AND NOT EXISTS (SELECT 1 FROM bans b WHERE b.user_id = u.user_id)
                ) AS users_active,
                (SELECT COUNT(*) FROM likes) AS likes,
                (SELECT COUNT(*) FROM matches) AS matches,
                (SELECT COUNT(*) FROM views) AS views,
                (SELECT COUNT(*) FROM blocks) AS blocks,
                (SELECT COUNT(*) FROM bans) AS bans,
                (SELECT COUNT(*) FROM reports WHERE status = 'pending') AS reports_pending,
                (SELECT COUNT(*) FROM donations) AS donations_total
            """
        ) as cursor:
            return dict(await cursor.fetchone())


async def get_all_users(limit: int = 20, offset: int = 0) -> List[aiosqlite.Row]:
    async with db_lock:
        async with db.execute(
            """
            SELECT
                u.user_id,
                u.name,
                u.age,
                u.gender,
                u.city,
                u.is_active,
                u.username,
                u.created_at,
                EXISTS(SELECT 1 FROM bans b WHERE b.user_id = u.user_id) AS is_banned
            FROM users u
            ORDER BY u.created_at DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ) as cursor:
            return await cursor.fetchall()


# ============================================================
# ПЛАТЕЖИ И ДОНАТЫ
# ============================================================
async def create_payment_intent(user_id: int, amount: int, payload: str) -> None:
    expires_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S", datetime.now(timezone.utc).timetuple())
    # Правильнее использовать datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    from datetime import timedelta
    expires = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    async with db_lock:
        await db.execute(
            """
            INSERT INTO payment_intents (payload, user_id, amount, currency, status, expires_at)
            VALUES (?, ?, ?, 'XTR', 'pending', ?)
            """,
            (payload, user_id, amount, expires),
        )
        await db.commit()


async def cancel_payment_intent(payload: str) -> None:
    async with db_lock:
        await db.execute(
            "UPDATE payment_intents SET status='cancelled' WHERE payload=? AND status='pending'",
            (payload,),
        )
        await db.commit()


async def get_payment_intent(payload: str) -> Optional[aiosqlite.Row]:
    async with db_lock:
        async with db.execute(
            "SELECT * FROM payment_intents WHERE payload = ?",
            (payload,),
        ) as cursor:
            return await cursor.fetchone()


async def mark_payment_paid(
    payload: str,
    user_id: int,
    amount: int,
    currency: str,
    charge_id: str,
) -> PaymentResult:
    async with transaction():
        # Проверяем существование intent
        intent = await get_payment_intent(payload)
        if not intent:
            return PaymentResult.INVALID
        if intent["status"] == "paid":
            # Уже оплачен – проверяем совпадение всех данных
            if (
                intent["user_id"] == user_id
                and intent["amount"] == amount
                and intent["currency"] == currency
                and intent["telegram_payment_charge_id"] == charge_id
            ):
                return PaymentResult.ALREADY_PAID
            return PaymentResult.INVALID
        if intent["status"] != "pending":
            return PaymentResult.INVALID
        if intent["user_id"] != user_id or intent["amount"] != amount or intent["currency"] != currency:
            return PaymentResult.INVALID

        # Проверяем срок действия
        if intent["expires_at"] and intent["expires_at"] < datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"):
            return PaymentResult.INVALID

        # Обновляем intent
        cur = await db.execute(
            """
            UPDATE payment_intents
            SET status='paid',
                paid_at=CURRENT_TIMESTAMP,
                telegram_payment_charge_id=?
            WHERE payload=? AND status='pending'
            """,
            (charge_id, payload),
        )
        if cur.rowcount == 0:
            return PaymentResult.INVALID

        # Вставляем donation
        try:
            await db.execute(
                """
                INSERT INTO donations (user_id, amount, currency, invoice_payload, telegram_payment_charge_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, amount, currency, payload, charge_id),
            )
        except aiosqlite.IntegrityError:
            # Дубликат charge_id – откатываем транзакцию
            return PaymentResult.INVALID

        return PaymentResult.PAID


# ============================================================
# MIDDLEWARE
# ============================================================
class SecurityMiddleware(BaseMiddleware):
    def __init__(self):
        self.last_action = {}
        self.cooldown_callback = 0.4
        self.cooldown_message = 0.7
        self.cooldown_command = 2.0

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        # Пропускаем successful_payment без ограничений
        if isinstance(event, Message) and event.successful_payment:
            return await handler(event, data)

        user = data.get("event_from_user")
        if not user:
            return await handler(event, data)

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

        if status["exists"]:
            rules_ver = status["rules_version"] or 0
            if rules_ver < RULES_VERSION:
                if isinstance(event, Message) and first_word in ["/start", "/delete"]:
                    return await handler(event, data)
                if isinstance(event, CallbackQuery) and event.data == "agree_rules":
                    return await handler(event, data)
                if isinstance(event, CallbackQuery):
                    await event.answer("Правила сервиса обновились. Напишите /start.", show_alert=True)
                elif isinstance(event, Message):
                    await event.answer("Правила сервиса обновились. Пожалуйста, напишите /start.")
                return

        is_command = first_word in KNOWN_COMMANDS
        cooldown = self.cooldown_command if is_command else (
            self.cooldown_callback if isinstance(event, CallbackQuery) else self.cooldown_message
        )

        now = time.monotonic()
        if now - self.last_action.get(user.id, 0) < cooldown:
            if isinstance(event, CallbackQuery):
                await event.answer("Не так быстро. Подождите немного.")
            elif isinstance(event, Message):
                await event.answer("Не так быстро. Подождите немного.")
            return

        self.last_action[user.id] = now
        if len(self.last_action) > 10000:
            self.last_action = {k: v for k, v in self.last_action.items() if now - v < 3600}
        return await handler(event, data)


security_mw = SecurityMiddleware()
router.message.middleware(security_mw)
router.callback_query.middleware(security_mw)


# ============================================================
# КЛАВИАТУРЫ И ХЕЛПЕРЫ
# ============================================================
def format_profile(profile: aiosqlite.Row, prefix: str = "") -> str:
    return (
        f"{prefix}👤 <b>{escape(profile['name'])}</b>, {profile['age']} ({escape(profile['gender'])})\n"
        f"📍 {escape(profile['city'])}\n\n📝 {escape(profile['bio'])}"
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
        except TelegramBadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                await callback.message.answer(text, reply_markup=reply_markup)


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
    text = (
        f"👤 <b>{escape(user['name'])}</b>, {user['age']} ({escape(user['gender'])})\n"
        f"🔍 Ищу: {escape(user['search_gender'])}\n📍 {escape(user['city'])}\n\n📝 {escape(user['bio'])}\n\n"
        f"💌 Новых лайков: {await get_likes_count(user['user_id'])}\nСтатус: {status}"
    )
    try:
        await message.answer_photo(photo=user["photo_id"], caption=text, reply_markup=profile_card_keyboard())
    except TelegramBadRequest as exc:
        logger.warning("Failed to send profile card for user %s: %s", user["user_id"], exc)
        await message.answer(
            "Не удалось загрузить фото. Обновите его в разделе редактирования.",
            reply_markup=profile_card_keyboard(),
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

    while True:
        strict = mode == "local"
        profile = await get_random_profile(
            user_id=callback.from_user.id,
            user_gender=user["gender"],
            search_gender=user["search_gender"],
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
            logger.warning("Failed to show profile %s: %s", profile["user_id"], exc)
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
            logger.warning("Failed to show liker %s: %s", profile["user_id"], exc)
            await safe_edit_or_send(callback, "Не удалось показать анкету.")
            return


async def show_next(callback: CallbackQuery, state: FSMContext, mode: str) -> None:
    if mode == "likes":
        await show_next_liker(callback, state)
    else:
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


def menu_keyboard(likes_count: int = 0):
    builder = InlineKeyboardBuilder()
    builder.button(text="🔍 Смотреть анкеты", callback_data="search_menu")
    builder.button(text="👤 Моя анкета", callback_data="my_profile")
    builder.button(text="💑 Мои мэтчи", callback_data="my_matches")
    if likes_count > 0:
        builder.button(text=f"💌 Вам понравились: {likes_count}", callback_data="show_likes")
    builder.button(text="⭐ Поддержать проект", callback_data="donate_menu")
    builder.adjust(2, 2 if likes_count > 0 else 1, 1)
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
    builder.button(text="🚫 Блокировать", callback_data=f"block:{profile_id}:{mode}")
    builder.button(text="🚩 Пожаловаться", callback_data=f"report:{profile_id}:{mode}")
    builder.button(text="🔚 В меню", callback_data="menu")
    builder.adjust(2, 2, 1)
    return builder.as_markup()


def profile_like_keyboard(profile_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="❤️ Ответить взаимностью", callback_data=f"like:{profile_id}:likes")
    builder.button(text="👎 Пропустить", callback_data=f"dislike:{profile_id}:likes")
    builder.button(text="🚫 Блокировать", callback_data=f"block:{profile_id}:likes")
    builder.button(text="🚩 Пожаловаться", callback_data=f"report:{profile_id}:likes")
    builder.adjust(2, 2)
    return builder.as_markup()


def edit_profile_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Имя", callback_data="edit_name")
    builder.button(text="✏️ Возраст", callback_data="edit_age")
    builder.button(text="✏️ Кого ищу", callback_data="edit_search_gender")
    builder.button(text="✏️ Город", callback_data="edit_city")
    builder.button(text="✏️ О себе", callback_data="edit_bio")
    builder.button(text="✏️ Фото", callback_data="edit_photo")
    builder.button(text="🗑 Удалить анкету", callback_data="delete_profile")
    builder.button(text="🔙 Назад", callback_data="my_profile")
    builder.adjust(2, 2, 2, 1, 1)
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
    builder.button(text="✉️ Написать сообщение", url=f"tg://user?id={target_user_id}")
    return builder.as_markup()


def admin_report_keyboard(report_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="🚫 Забанить", callback_data=f"admin_ban:{report_id}")
    builder.button(text="✅ Отклонить", callback_data=f"admin_reject:{report_id}")
    builder.adjust(2)
    return builder.as_markup()


def donate_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="⭐ 10 звёзд", callback_data="donate_10")
    builder.button(text="⭐ 25 звёзд", callback_data="donate_25")
    builder.button(text="⭐ 50 звёзд", callback_data="donate_50")
    builder.button(text="🔙 Назад", callback_data="menu")
    builder.adjust(2, 1)
    return builder.as_markup()


def admin_users_keyboard(page: int, total_pages: int):
    builder = InlineKeyboardBuilder()
    if page > 0:
        builder.button(text="◀️ Назад", callback_data=f"admin_users_page_{page-1}")
    if page < total_pages - 1:
        builder.button(text="Вперёд ▶️", callback_data=f"admin_users_page_{page+1}")
    builder.button(text="🔙 В меню", callback_data="menu")
    builder.adjust(2)
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
    await message.answer("Добро пожаловать в бот знакомств!\n\n" + RULES_TEXT, reply_markup=rules_keyboard())


@router.message(Command("delete"))
async def cmd_delete(message: Message, state: FSMContext):
    await state.clear()
    await delete_user(message.from_user.id)
    await message.answer(
        "Ваша анкета и связанные с ней данные удалены из базы бота.\n\n"
        "ℹ️ Запись о бане (если он был наложен администрацией) сохраняется "
        "в целях безопасности сервиса и не связана с вашей анкетой.\n"
        "Жалобы и платежные записи сохраняются в соответствии с требованиями.\n\n"
        "Для новой регистрации напишите /start."
    )


@router.message(Command("help"))
async def cmd_help(message: Message):
    text = (
        "ℹ️ <b>Справка</b>\n\n"
        "Бот помогает найти людей для знакомства: смотрите анкеты, ставьте лайки, "
        "а при взаимной симпатии получите мэтч и сможете общаться.\n\n"
        "<b>Команды:</b>\n"
        "/start — показать анкету и главное меню\n"
        "/help — эта справка\n"
        "/delete — удалить анкету и связанные пользовательские данные\n"
        "/donate — поддержать проект звёздами\n"
        "/paysupport — помощь по платежам\n"
    )
    if message.from_user.id == ADMIN_ID:
        text += "\n<b>Админские команды:</b>\n/admin_users, /stats, /unban\n"
    text += "\n" + RULES_TEXT
    await message.answer(text)


@router.message(Command("paysupport"))
async def cmd_paysupport(message: Message):
    await message.answer(f"По вопросам платежей обратитесь: {SUPPORT_CONTACT}")


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
        f"🚩 Жалоб в ожидании: {s['reports_pending']}\n"
        f"⭐ Донатов: {s['donations_total']}"
    )


@router.message(Command("admin_users"))
async def admin_users(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    limit = 20
    users = await get_all_users(limit, 0)
    if not users:
        await message.answer("Нет зарегистрированных пользователей.")
        return
    total = await get_stats()
    total_pages = max(1, (total["users_total"] + limit - 1) // limit)
    text = "👥 <b>Список пользователей (последние зарегистрированные):</b>\n\n"
    for u in users:
        status = "⛔ забанен" if u["is_banned"] else ("✅ активен" if u["is_active"] else "❌ неактивен")
        text += (
            f"ID: <code>{u['user_id']}</code>, {escape(u['name'])}, {u['age']}, "
            f"{escape(u['gender'])}, г. {escape(u['city'])}, {status}\n"
        )
    text += f"\nСтраница 1 из {total_pages}"
    await message.answer(text, reply_markup=admin_users_keyboard(0, total_pages))


@router.callback_query(F.data.startswith("admin_users_page_"))
async def admin_users_page(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Недостаточно прав.", show_alert=True)
        return

    try:
        page = int(callback.data.split("_")[-1])
    except ValueError:
        await callback.answer("Некорректная страница.", show_alert=True)
        return

    if page < 0:
        await callback.answer("Некорректная страница.", show_alert=True)
        return

    limit = 20
    total = await get_stats()
    total_users = total["users_total"]
    total_pages = max(1, (total_users + limit - 1) // limit)

    if page >= total_pages:
        page = total_pages - 1

    users = await get_all_users(limit, page * limit)
    if not users:
        await callback.answer("Нет пользователей на этой странице.", show_alert=True)
        return

    text = "👥 <b>Список пользователей (последние зарегистрированные):</b>\n\n"
    for u in users:
        status = "⛔ забанен" if u["is_banned"] else ("✅ активен" if u["is_active"] else "❌ неактивен")
        text += (
            f"ID: <code>{u['user_id']}</code>, {escape(u['name'])}, {u['age']}, "
            f"{escape(u['gender'])}, г. {escape(u['city'])}, {status}\n"
        )
    text += f"\nСтраница {page+1} из {total_pages}"
    await callback.answer()
    await callback.message.edit_text(text, reply_markup=admin_users_keyboard(page, total_pages))


# ============================================================
# ДОНАТЫ (TELEGRAM STARS)
# ============================================================
@router.message(Command("donate"))
async def cmd_donate(message: Message):
    await message.answer("⭐ Поддержите развитие бота!\nВыберите сумму в звёздах:", reply_markup=donate_keyboard())


@router.callback_query(F.data == "donate_menu")
async def donate_menu(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text(
        "⭐ Поддержите развитие бота!\nВыберите сумму в звёздах:",
        reply_markup=donate_keyboard(),
    )


@router.callback_query(F.data.startswith("donate_"))
async def donate_amount(callback: CallbackQuery):
    try:
        amount = int(callback.data.split("_")[1])
    except (ValueError, IndexError):
        await callback.answer("Недопустимая сумма.", show_alert=True)
        return

    if amount not in (10, 25, 50):
        await callback.answer("Недопустимая сумма.", show_alert=True)
        return

    user_id = callback.from_user.id
    payload = f"donate:{user_id}:{secrets.token_urlsafe(16)}"

    # Подтверждаем callback сразу, чтобы избежать истечения
    await callback.answer()

    try:
        await create_payment_intent(user_id, amount, payload)
    except Exception as e:
        logger.exception("Failed to create payment intent: %s", e)
        await callback.message.answer("Не удалось создать платёж. Попробуйте позже.")
        return

    prices = [LabeledPrice(label="Поддержка бота", amount=amount)]
    try:
        await bot.send_invoice(
            chat_id=user_id,
            title="⭐ Поддержка бота знакомств",
            description="Спасибо за вашу поддержку! Это поможет развитию проекта.",
            payload=payload,
            provider_token=STARS_PROVIDER_TOKEN,
            currency="XTR",
            prices=prices,
            start_parameter="donate",
        )
    except Exception as e:
        logger.exception("Ошибка при выставлении счёта: %s", e)
        await cancel_payment_intent(payload)
        await callback.message.answer("Не удалось создать платёж. Попробуйте позже.")


@router.pre_checkout_query()
async def pre_checkout_query_handler(query: PreCheckoutQuery):
    intent = await get_payment_intent(query.invoice_payload)
    if (
        not intent
        or intent["user_id"] != query.from_user.id
        or intent["amount"] != query.total_amount
        or intent["currency"] != query.currency
        or intent["status"] != "pending"
        or (intent["expires_at"] and intent["expires_at"] < datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
    ):
        await query.answer(ok=False, error_message="Платёж не может быть обработан.")
        return
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def successful_payment_handler(message: Message):
    payment = message.successful_payment
    user_id = message.from_user.id
    amount = payment.total_amount
    payload = payment.invoice_payload
    currency = payment.currency
    charge_id = payment.telegram_payment_charge_id

    if currency != "XTR":
        logger.warning("Unexpected payment currency: %s", currency)
        return

    result = await mark_payment_paid(payload, user_id, amount, currency, charge_id)
    if result == PaymentResult.PAID:
        await message.answer(
            f"⭐ Спасибо за поддержку! Вы перевели {amount} звёзд. Мы очень ценим ваш вклад! ❤️"
        )
    elif result == PaymentResult.ALREADY_PAID:
        # Платёж уже обработан ранее – молча подтверждаем
        logger.info("Duplicate successful payment update: %s", charge_id)
    else:
        logger.warning("Invalid payment update: %s", charge_id)
        await message.answer("Платёж не может быть обработан. Обратитесь в поддержку.")


# ============================================================
# РЕГИСТРАЦИЯ (продолжение)
# ============================================================
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
    await state.set_state(Registration.city)
    await callback.message.edit_text(
        f"Вы ищете: <b>{escape(sg)}</b>\n\nИз какого вы города?\nОт {MIN_CITY_LENGTH} до {MAX_CITY_LENGTH} символов."
    )


@router.message(Registration.city, F.text)
async def registration_city(message: Message, state: FSMContext):
    city = validate_text(message.text, MIN_CITY_LENGTH, MAX_CITY_LENGTH)
    if not city:
        await message.answer(f"Название города должно содержать от {MIN_CITY_LENGTH} до {MAX_CITY_LENGTH} символов.")
        return
    city = " ".join(city.strip().split())
    await state.update_data(city=city)
    await state.set_state(Registration.bio)
    await message.answer(
        f"Город: <b>{escape(city)}</b>\n\nРасскажите немного о себе: от {MIN_BIO_LENGTH} до {MAX_BIO_LENGTH} символов."
    )


@router.message(Registration.bio, F.text)
async def registration_bio(message: Message, state: FSMContext):
    bio = validate_text(message.text, MIN_BIO_LENGTH, MAX_BIO_LENGTH)
    if not bio:
        await message.answer(f"Описание должно содержать от {MIN_BIO_LENGTH} до {MAX_BIO_LENGTH} символов.")
        return
    await state.update_data(bio=bio)
    await state.set_state(Registration.photo)
    await message.answer("Отлично. Теперь отправьте вашу фотографию.")


@router.message(Registration.photo, F.photo)
async def registration_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    photo_id = message.photo[-1].file_id
    if await add_user_to_db(
        user_id=message.from_user.id,
        name=data["name"],
        age=data["age"],
        gender=data["gender"],
        search_gender=data["search_gender"],
        city=data["city"],
        bio=data["bio"],
        photo_id=photo_id,
        username=message.from_user.username,
        accepted_rules_at=data.get("accepted_rules_at"),
    ):
        await state.clear()
        await message.answer("Регистрация успешно завершена! 🎉", reply_markup=menu_keyboard())
    else:
        await state.clear()
        await message.answer("Вы заблокированы и не можете зарегистрироваться.")


@router.message(Registration.photo)
async def registration_photo_invalid(message: Message):
    await message.answer("Пожалуйста, отправьте изображение именно как фотографию.")


@router.message(
    StateFilter(
        Registration.name,
        Registration.age,
        Registration.city,
        Registration.bio,
        EditProfile.name,
        EditProfile.age,
        EditProfile.city,
        EditProfile.bio,
    )
)
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
        await safe_edit_or_send(
            callback,
            "Ваша анкета неактивна. Обновите фото через 'Моя анкета'.",
            reply_markup=back_to_menu_keyboard(),
        )
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
                reply_markup=match_keyboard(liked_id),
            )
        except (TelegramBadRequest, TelegramForbiddenError):
            logger.warning("Match send fail to %s", uid)
        try:
            await bot.send_message(
                liked_id,
                f"💖 <b>Это мэтч!</b>\n\nВы понравились друг другу с {escape(current_user['name'])}.",
                reply_markup=match_keyboard(uid),
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

        await block_user(uid, blocked_id)
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
# ЖАЛОБЫ И АДМИН-БАН
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

        await state.set_state(Report.reason)
        await state.update_data(reported_id=rep_id, mode=mode)
        await callback.answer()
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest:
            pass
        await callback.message.answer(
            f"Опишите причину жалобы.\nМаксимальная длина: {MAX_REPORT_LENGTH} символов.",
            reply_markup=cancel_report_keyboard(),
        )


@router.message(Report.reason, F.text)
async def process_report(message: Message, state: FSMContext):
    reason = validate_text(message.text, 1, MAX_REPORT_LENGTH)
    if not reason:
        await message.answer(f"Жалоба не должна быть пустой и не может превышать {MAX_REPORT_LENGTH} символов.")
        return

    data = await state.get_data()
    rep_id, mode = data.get("reported_id"), data.get("mode")
    if not isinstance(rep_id, int):
        await state.clear()
        await message.answer("Ошибка данных. Попробуйте ещё раз.")
        return

    uid = message.from_user.id
    r_id = await add_report(uid, rep_id, reason)
    if r_id is None:
        await state.clear()
        await message.answer(
            "Не удалось отправить жалобу (пользователь недоступен или вы уже жаловались).",
            reply_markup=menu_keyboard(await get_likes_count(uid)),
        )
        return

    rep_user = await get_user(rep_id)
    if not rep_user:
        await state.clear()
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
        f"<b>От кого:</b> {message.from_user.mention_html()} (ID: <code>{uid}</code>)\n"
        f"<b>На кого:</b> {rep_name} (ID: <code>{rep_id}</code>, {rep_un})\n\n"
        f"<b>Причина:</b>\n{escape(reason)}"
    )

    try:
        if rep_user["photo_id"]:
            await bot.send_photo(
                ADMIN_ID,
                photo=rep_user["photo_id"],
                caption=text,
                reply_markup=admin_report_keyboard(r_id),
            )
        else:
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
    await callback.message.answer(
        "Жалоба отменена.",
        reply_markup=menu_keyboard(await get_likes_count(callback.from_user.id)),
    )


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
        await bot.send_message(
            reported_id,
            "Вы были заблокированы администрацией. Для удаления данных используйте /delete.",
        )
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


@router.callback_query(F.data == "my_matches")
async def my_matches(callback: CallbackQuery):
    await callback.answer()
    matches = await get_matches(callback.from_user.id)
    if not matches:
        await safe_edit_or_send(
            callback,
            "У вас пока нет мэтчей. Всё ещё впереди — продолжайте искать! 💘",
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
        f"💑 <b>Ваши мэтчи ({len(matches)}):</b>\n\nНажмите на имя, чтобы написать человеку.",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data == "edit_profile")
async def edit_profile_menu(callback: CallbackQuery):
    await callback.answer()
    await delete_callback_message(callback)
    await callback.message.answer("Что вы хотите изменить?", reply_markup=edit_profile_keyboard())


@router.callback_query(
    F.data.in_({"edit_name", "edit_age", "edit_search_gender", "edit_city", "edit_bio", "edit_photo"})
)
async def edit_start(callback: CallbackQuery, state: FSMContext):
    action = callback.data
    await callback.answer()
    if action == "edit_search_gender":
        await state.set_state(EditProfile.search_gender)
        await callback.message.answer(
            "Кого вы ищете?",
            reply_markup=search_gender_keyboard(with_cancel=True).as_markup(),
        )
    elif action == "edit_name":
        await state.set_state(EditProfile.name)
        await callback.message.answer(
            f"Введите новое имя: от {MIN_NAME_LENGTH} до {MAX_NAME_LENGTH} символов.",
            reply_markup=cancel_keyboard(),
        )
    elif action == "edit_age":
        await state.set_state(EditProfile.age)
        await callback.message.answer(
            f"Введите новый возраст: от {MIN_AGE} до {MAX_AGE}.",
            reply_markup=cancel_keyboard(),
        )
    elif action == "edit_city":
        await state.set_state(EditProfile.city)
        await callback.message.answer(
            f"Введите новый город: от {MIN_CITY_LENGTH} до {MAX_CITY_LENGTH} символов.",
            reply_markup=cancel_keyboard(),
        )
    elif action == "edit_bio":
        await state.set_state(EditProfile.bio)
        await callback.message.answer(
            f"Введите новое описание: от {MIN_BIO_LENGTH} до {MAX_BIO_LENGTH} символов.",
            reply_markup=cancel_keyboard(),
        )
    elif action == "edit_photo":
        await state.set_state(EditProfile.photo)
        await callback.message.answer("Отправьте новую фотографию.", reply_markup=cancel_keyboard())


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
    await callback.message.answer(
        "Главное меню:",
        reply_markup=menu_keyboard(await get_likes_count(callback.from_user.id)),
    )


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


@router.message(EditProfile.city, F.text)
async def edit_city_save(message: Message, state: FSMContext):
    city = validate_text(message.text, MIN_CITY_LENGTH, MAX_CITY_LENGTH)
    if not city:
        await message.answer(f"Город должен содержать от {MIN_CITY_LENGTH} до {MAX_CITY_LENGTH} символов.")
        return
    city = " ".join(city.strip().split())
    await update_user_field(message.from_user.id, "city", city)
    await state.clear()
    await send_menu(message, message.from_user.id, f"Город успешно обновлён: <b>{escape(city)}</b>.")


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
    await callback.message.answer(
        "Действие отменено.",
        reply_markup=menu_keyboard(await get_likes_count(callback.from_user.id)),
    )


@router.callback_query(F.data == "delete_profile")
async def delete_profile_confirm(callback: CallbackQuery):
    await callback.answer()
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, удалить анкету", callback_data="confirm_delete")
    builder.button(text="🔙 Назад", callback_data="edit_profile")
    builder.adjust(1)
    await callback.message.edit_text(
        "Вы уверены, что хотите удалить анкету?\n\n"
        "Будут удалены профиль, лайки, просмотры, блокировки и связанные данные. "
        "Это действие нельзя отменить.",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data == "confirm_delete")
async def delete_profile_execute(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await delete_user(callback.from_user.id)
    await callback.message.edit_text(
        "Ваша анкета и связанные данные удалены.\n\nЧтобы зарегистрироваться снова, напишите /start."
    )


@router.callback_query(F.data == "menu")
async def back_to_menu(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    likes = await get_likes_count(callback.from_user.id)
    await safe_edit_or_send(callback, "Главное меню:", reply_markup=menu_keyboard(likes))


@router.callback_query()
async def unknown_callback(callback: CallbackQuery):
    await callback.answer("Эта кнопка устарела. Откройте меню заново.", show_alert=True)


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
