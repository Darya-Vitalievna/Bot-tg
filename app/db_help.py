# DB_HELP_FIXED_STATUS_2026_01_15
import sqlite3
import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

from .config import DB_PATH_HELP, ADMIN_USER_IDS

ALL_GUILD_IDS = [1, 2, 3, 4, 5]


def _resolve_db_path() -> str:
    """
    Приводим DB_PATH_HELP к абсолютному пути.
    Если DB_PATH_HELP относительный, считаем от корня проекта BookBot (папка над app/).
    """
    p = Path(DB_PATH_HELP)
    if p.is_absolute():
        return str(p)

    project_root = Path(__file__).resolve().parent.parent  # .../BookBot
    return str((project_root / p).resolve())


def _connect() -> sqlite3.Connection:
    path = _resolve_db_path()
    # Чтобы ты сразу видела, какую БД реально использует бот (важно для проблемы "удалил экспертов, но они всё равно эксперты")
    print(f"[DB_HELP] Using DB: {path}")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _now() -> str:
    return datetime.datetime.now().isoformat(timespec="seconds")


def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None


def _has_column(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r["name"] for r in cur.fetchall()]
    return col in cols


def _add_column(cur: sqlite3.Cursor, table: str, col: str, ddl: str) -> None:
    if not _has_column(cur, table, col):
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")


# ----------------- roles -----------------

def is_admin(user_id: int) -> bool:
    return int(user_id) in set(int(x) for x in (ADMIN_USER_IDS or []))


def is_expert(user_id: int) -> bool:
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM experts WHERE user_id=? LIMIT 1", (int(user_id),))
        return cur.fetchone() is not None
    finally:
        conn.close()


def expert_guilds(user_id: int) -> List[int]:
    return ALL_GUILD_IDS[:] if is_expert(user_id) else []


def validate_expert_part(user_id: int, guild_id: int) -> bool:
    return is_expert(user_id)


# ----------------- init + migrations -----------------

def init_help_db() -> None:
    conn = _connect()
    cur = conn.cursor()

    # experts
    cur.execute("""
    CREATE TABLE IF NOT EXISTS experts (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        full_name TEXT,
        added_at TEXT,
        added_by INTEGER
    )
    """)
    _add_column(cur, "experts", "username", "TEXT")
    _add_column(cur, "experts", "full_name", "TEXT")
    _add_column(cur, "experts", "added_at", "TEXT")
    _add_column(cur, "experts", "added_by", "INTEGER")

    # help_requests
    if not _table_exists(cur, "help_requests"):
        cur.execute("""
        CREATE TABLE help_requests (
            request_id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_user_id INTEGER,
            guild_id INTEGER,
            problem_essence TEXT,
            problem_since TEXT,
            tried_actions TEXT,
            desired_result TEXT,
            status TEXT DEFAULT 'active',
            is_closed INTEGER DEFAULT 0,
            is_deleted INTEGER DEFAULT 0,
            created_at TEXT
        )
        """)
    else:
        _add_column(cur, "help_requests", "student_user_id", "INTEGER")
        _add_column(cur, "help_requests", "guild_id", "INTEGER")
        _add_column(cur, "help_requests", "problem_essence", "TEXT")
        _add_column(cur, "help_requests", "problem_since", "TEXT")
        _add_column(cur, "help_requests", "tried_actions", "TEXT")
        _add_column(cur, "help_requests", "desired_result", "TEXT")
        # ВАЖНО: status (у тебя он уже есть как NOT NULL, поэтому insert обязан его заполнять)
        _add_column(cur, "help_requests", "status", "TEXT DEFAULT 'active'")
        _add_column(cur, "help_requests", "is_closed", "INTEGER DEFAULT 0")
        _add_column(cur, "help_requests", "is_deleted", "INTEGER DEFAULT 0")
        _add_column(cur, "help_requests", "created_at", "TEXT")

    # help_answers
    cur.execute("""
    CREATE TABLE IF NOT EXISTS help_answers (
        answer_id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id INTEGER,
        expert_user_id INTEGER,
        answer_text TEXT,
        created_at TEXT,
        expert_username TEXT,
        expert_full_name TEXT
    )
    """)
    _add_column(cur, "help_answers", "created_at", "TEXT")
    _add_column(cur, "help_answers", "expert_username", "TEXT")
    _add_column(cur, "help_answers", "expert_full_name", "TEXT")

    # expert_applications
    cur.execute("""
    CREATE TABLE IF NOT EXISTS expert_applications (
        application_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        username TEXT,
        full_name TEXT,
        about_text TEXT,
        status TEXT DEFAULT 'pending',
        created_at TEXT
    )
    """)

    # индексы
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_answers_req ON help_answers(request_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_answers_expert ON help_answers(expert_user_id)")
    except Exception:
        pass

    # уникальность ответа: один эксперт - один ответ на запрос (если в базе уже есть дубли, индекс не создастся, это ок)
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_answers_req_expert ON help_answers(request_id, expert_user_id)")
    except Exception:
        pass

    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_requests_user ON help_requests(student_user_id, created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_requests_open ON help_requests(is_closed, created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_requests_deleted ON help_requests(is_deleted, is_closed, created_at)")
    except Exception:
        pass

    conn.commit()
    conn.close()


# ----------------- experts management -----------------

def add_expert(
    user_id: int,
    added_by: Optional[int] = None,
    username: Optional[str] = None,
    full_name: Optional[str] = None
) -> Tuple[bool, str]:
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO experts(user_id, username, full_name, added_at, added_by) VALUES (?, ?, ?, ?, ?)",
            (int(user_id), username, full_name, _now(), int(added_by) if added_by is not None else None),
        )
        conn.commit()
        return True, "OK"
    except sqlite3.IntegrityError:
        return False, "Эксперт уже существует"
    finally:
        conn.close()


def remove_expert(user_id: int) -> bool:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM experts WHERE user_id=?", (int(user_id),))
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def list_experts() -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT user_id, username, full_name, added_at, added_by FROM experts ORDER BY user_id ASC")
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ----------------- requests -----------------

def create_help_request(user_id: int, guild_id: int, essence: str, since: str, tried: str, result: str) -> int:
    conn = _connect()
    cur = conn.cursor()

    # КРИТИЧНО: если в таблице есть status (а у тебя он NOT NULL), заполняем его всегда
    has_status = _has_column(cur, "help_requests", "status")

    if has_status:
        cur.execute("""
            INSERT INTO help_requests
            (student_user_id, guild_id, problem_essence, problem_since, tried_actions, desired_result,
             status, is_closed, is_deleted, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?)
        """, (int(user_id), int(guild_id), essence, since, tried, result, "active", _now()))
    else:
        cur.execute("""
            INSERT INTO help_requests
            (student_user_id, guild_id, problem_essence, problem_since, tried_actions, desired_result,
             is_closed, is_deleted, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?)
        """, (int(user_id), int(guild_id), essence, since, tried, result, _now()))

    conn.commit()
    rid = int(cur.lastrowid)
    conn.close()
    return rid


def get_request(request_id: int) -> Optional[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM help_requests WHERE request_id=?", (int(request_id),))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def list_user_requests(user_id: int, limit: int = 20) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT r.*,
               (SELECT COUNT(*) FROM help_answers a WHERE a.request_id = r.request_id) AS answers_count
        FROM help_requests r
        WHERE r.student_user_id = ?
        ORDER BY COALESCE(r.created_at, '') DESC, r.request_id DESC
        LIMIT ?
    """, (int(user_id), int(limit)))
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def list_open_requests(limit: int = 20) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()

    where = "COALESCE(r.is_closed, 0) = 0"
    if _has_column(cur, "help_requests", "is_deleted"):
        where += " AND COALESCE(r.is_deleted, 0) = 0"

    cur.execute(f"""
        SELECT r.*,
               (SELECT COUNT(*) FROM help_answers a WHERE a.request_id = r.request_id) AS answers_count
        FROM help_requests r
        WHERE {where}
        ORDER BY COALESCE(r.created_at, '') DESC, r.request_id DESC
        LIMIT ?
    """, (int(limit),))
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def close_request(request_id: int, user_id: Optional[int] = None) -> bool:
    conn = _connect()
    cur = conn.cursor()

    has_status = _has_column(cur, "help_requests", "status")

    if user_id is None:
        if has_status:
            cur.execute("""
                UPDATE help_requests
                SET is_closed = 1, status = 'closed'
                WHERE request_id = ? AND COALESCE(is_closed,0) = 0
            """, (int(request_id),))
        else:
            cur.execute("""
                UPDATE help_requests
                SET is_closed = 1
                WHERE request_id = ? AND COALESCE(is_closed,0) = 0
            """, (int(request_id),))
    else:
        if has_status:
            cur.execute("""
                UPDATE help_requests
                SET is_closed = 1, status = 'closed'
                WHERE request_id = ?
                  AND student_user_id = ?
                  AND COALESCE(is_closed,0) = 0
            """, (int(request_id), int(user_id)))
        else:
            cur.execute("""
                UPDATE help_requests
                SET is_closed = 1
                WHERE request_id = ?
                  AND student_user_id = ?
                  AND COALESCE(is_closed,0) = 0
            """, (int(request_id), int(user_id)))

    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def is_request_closed(request_id: int) -> bool:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(is_closed,0) AS is_closed FROM help_requests WHERE request_id=?", (int(request_id),))
    row = cur.fetchone()
    conn.close()
    if not row:
        return True
    return int(row["is_closed"]) == 1


# ----------------- answers -----------------

def try_save_answer(
    expert_user_id: int,
    request_id: int,
    answer_text: str,
    expert_username: Optional[str] = None,
    expert_full_name: Optional[str] = None
) -> bool:
    txt = (answer_text or "").strip()
    if not txt:
        return False
    if is_request_closed(request_id):
        return False

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO help_answers(request_id, expert_user_id, answer_text, created_at, expert_username, expert_full_name)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (int(request_id), int(expert_user_id), txt, _now(), expert_username, expert_full_name))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        # UNIQUE(request_id, expert_user_id) => повторный ответ запрещён
        return False
    finally:
        conn.close()


def list_answers_for_request(request_id: int) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM help_answers
        WHERE request_id = ?
        ORDER BY COALESCE(created_at,'') ASC, answer_id ASC
    """, (int(request_id),))
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]
