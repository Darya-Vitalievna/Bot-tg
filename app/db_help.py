# DB_HELP_COMPAT_MIGR_2026_01_19
import sqlite3
import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

from .config import DB_PATH_HELP, ADMIN_USER_IDS

ALL_GUILD_IDS = [1, 2, 3, 4, 5]


def _resolve_db_path() -> str:
    p = Path(DB_PATH_HELP)
    if p.is_absolute():
        return str(p)
    project_root = Path(__file__).resolve().parent.parent
    return str((project_root / p).resolve())


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_resolve_db_path())
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
    cur.execute("SELECT 1 FROM experts WHERE user_id=? LIMIT 1", (int(user_id),))
    ok = cur.fetchone() is not None
    conn.close()
    return ok


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
        added_at TEXT,
        added_by INTEGER
    )
    """)
    _add_column(cur, "experts", "username", "TEXT")
    _add_column(cur, "experts", "full_name", "TEXT")

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
        _add_column(cur, "help_requests", "status", "TEXT DEFAULT 'active'")
        _add_column(cur, "help_requests", "is_closed", "INTEGER DEFAULT 0")
        _add_column(cur, "help_requests", "is_deleted", "INTEGER DEFAULT 0")
        _add_column(cur, "help_requests", "created_at", "TEXT")

        # если status уже был NOT NULL без default — это ок, мы будем всегда вставлять status
        # старые строки с NULL статусом: подстрахуемся
        try:
            cur.execute("UPDATE help_requests SET status='active' WHERE status IS NULL")
        except Exception:
            pass

    # help_answers
    cur.execute("""
    CREATE TABLE IF NOT EXISTS help_answers (
        answer_id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id INTEGER NOT NULL,
        expert_user_id INTEGER NOT NULL,
        answer_text TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """)
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

    # Индексы
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_requests_open ON help_requests(is_closed, is_deleted, created_at)")
    except Exception:
        pass
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_answers_req ON help_answers(request_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_answers_expert ON help_answers(expert_user_id)")
    except Exception:
        pass
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_apps_status ON expert_applications(status, created_at)")
    except Exception:
        pass

    # UNIQUE(request_id, expert_user_id) — чтобы один эксперт отвечал один раз
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_answers_req_expert ON help_answers(request_id, expert_user_id)")
    except Exception:
        pass

    conn.commit()
    conn.close()


# ----------------- experts management -----------------

def add_expert(user_id: int, added_by: Optional[int] = None,
              username: Optional[str] = None, full_name: Optional[str] = None) -> Tuple[bool, str]:
    conn = _connect()
    cur = conn.cursor()
    try:
        if _has_column(cur, "experts", "username") and _has_column(cur, "experts", "full_name"):
            cur.execute(
                "INSERT INTO experts(user_id, added_at, added_by, username, full_name) VALUES (?, ?, ?, ?, ?)",
                (int(user_id), _now(), int(added_by) if added_by is not None else None,
                 (username or "").lstrip("@") if username else None,
                 full_name),
            )
        else:
            cur.execute(
                "INSERT INTO experts(user_id, added_at, added_by) VALUES (?, ?, ?)",
                (int(user_id), _now(), int(added_by) if added_by is not None else None),
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


def list_experts(guild_id: Optional[int] = None) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    if _has_column(cur, "experts", "username") and _has_column(cur, "experts", "full_name"):
        cur.execute("SELECT user_id, username, full_name, added_at, added_by FROM experts ORDER BY user_id ASC")
    else:
        cur.execute("SELECT user_id, added_at, added_by FROM experts ORDER BY user_id ASC")
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ----------------- requests -----------------

def create_help_request(user_id: int, guild_id: int, essence: str, since: str, tried: str, result: str) -> int:
    conn = _connect()
    cur = conn.cursor()

    has_status = _has_column(cur, "help_requests", "status")
    has_deleted = _has_column(cur, "help_requests", "is_deleted")

    if has_status and has_deleted:
        cur.execute("""
            INSERT INTO help_requests
            (student_user_id, guild_id, problem_essence, problem_since, tried_actions, desired_result,
             status, is_closed, is_deleted, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?)
        """, (int(user_id), int(guild_id), essence, since, tried, result, "active", _now()))
    elif has_status:
        cur.execute("""
            INSERT INTO help_requests
            (student_user_id, guild_id, problem_essence, problem_since, tried_actions, desired_result,
             status, is_closed, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
        """, (int(user_id), int(guild_id), essence, since, tried, result, "active", _now()))
    else:
        cur.execute("""
            INSERT INTO help_requests
            (student_user_id, guild_id, problem_essence, problem_since, tried_actions, desired_result, is_closed, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 0, ?)
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
    has_deleted = _has_column(cur, "help_requests", "is_deleted")
    if has_deleted:
        cur.execute("""
            SELECT r.*,
                   (SELECT COUNT(*) FROM help_answers a WHERE a.request_id = r.request_id) AS answers_count
            FROM help_requests r
            WHERE COALESCE(r.is_closed, 0) = 0
              AND COALESCE(r.is_deleted, 0) = 0
            ORDER BY COALESCE(r.created_at, '') DESC, r.request_id DESC
            LIMIT ?
        """, (int(limit),))
    else:
        cur.execute("""
            SELECT r.*,
                   (SELECT COUNT(*) FROM help_answers a WHERE a.request_id = r.request_id) AS answers_count
            FROM help_requests r
            WHERE COALESCE(r.is_closed, 0) = 0
            ORDER BY COALESCE(r.created_at, '') DESC, r.request_id DESC
            LIMIT ?
        """, (int(limit),))
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def is_request_closed(request_id: int) -> bool:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(is_closed,0) AS c FROM help_requests WHERE request_id=? LIMIT 1", (int(request_id),))
    row = cur.fetchone()
    conn.close()
    return bool(row and int(row["c"]) == 1)


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


def mark_request_deleted(request_id: int) -> bool:
    conn = _connect()
    cur = conn.cursor()
    if not _has_column(cur, "help_requests", "is_deleted"):
        conn.close()
        return False
    cur.execute("UPDATE help_requests SET is_deleted=1 WHERE request_id=?", (int(request_id),))
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok


# ----------------- answers -----------------

def list_answers_for_request(request_id: int) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    if _has_column(cur, "help_answers", "expert_username") and _has_column(cur, "help_answers", "expert_full_name"):
        cur.execute("""
            SELECT answer_id, request_id, expert_user_id, answer_text, created_at, expert_username, expert_full_name
            FROM help_answers
            WHERE request_id=?
            ORDER BY created_at ASC, answer_id ASC
        """, (int(request_id),))
    else:
        cur.execute("""
            SELECT answer_id, request_id, expert_user_id, answer_text, created_at
            FROM help_answers
            WHERE request_id=?
            ORDER BY created_at ASC, answer_id ASC
        """, (int(request_id),))
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def try_save_answer(expert_user_id: int, request_id: int, answer_text: str,
                    expert_username: Optional[str] = None, expert_full_name: Optional[str] = None) -> bool:
    if is_request_closed(request_id):
        return False

    conn = _connect()
    cur = conn.cursor()

    try:
        if _has_column(cur, "help_answers", "expert_username") and _has_column(cur, "help_answers", "expert_full_name"):
            cur.execute("""
                INSERT INTO help_answers(request_id, expert_user_id, answer_text, created_at, expert_username, expert_full_name)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (int(request_id), int(expert_user_id), answer_text, _now(),
                  (expert_username or "").lstrip("@") if expert_username else None,
                  expert_full_name))
        else:
            cur.execute("""
                INSERT INTO help_answers(request_id, expert_user_id, answer_text, created_at)
                VALUES (?, ?, ?, ?)
            """, (int(request_id), int(expert_user_id), answer_text, _now()))

        conn.commit()
        return True
    except sqlite3.IntegrityError:
        # UNIQUE(request_id, expert_user_id) — второй ответ запрещён
        return False
    finally:
        conn.close()


# совместимость (если где-то зовут add_answer)
def add_answer(request_id: int, expert_user_id: int, answer_text: str) -> bool:
    return try_save_answer(expert_user_id, request_id, answer_text)


# ----------------- expert applications -----------------

def get_pending_application(user_id: int) -> Optional[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM expert_applications
        WHERE user_id=? AND status='pending'
        ORDER BY application_id DESC
        LIMIT 1
    """, (int(user_id),))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def create_expert_application(user_id: int, username: Optional[str], full_name: Optional[str], about_text: str) -> int:
    # запрет: более одной pending
    if get_pending_application(user_id):
        return -1

    conn = _connect()
    cur = conn.cursor()

    # --- Compatibility with stricter schemas ---
    # Some DB versions require NOT NULL columns like topic/experience/motivation.
    # We fill them from the collected about_text (3 answers) or fall back to about_text.
    def _extract(label_keywords):
        if not about_text:
            return None
        for line in str(about_text).splitlines():
            low = line.strip().lower()
            for kw in label_keywords:
                if low.startswith(kw):
                    # take text after ':' if present
                    if ':' in line:
                        return line.split(':', 1)[1].strip() or None
        return None

    motivation = _extract(["почему", "мотивац"]) or about_text
    experience = _extract(["в чем", "компет", "опыт"]) or about_text
    contact = _extract(["сколько", "врем", "недел"])  # optional
    topic_value = "apply_expert"

    cols = ["user_id"]
    vals = [int(user_id)]

    if _has_column(cur, "expert_applications", "username"):
        cols.append("username")
        vals.append((username or "").lstrip("@") if username else None)

    if _has_column(cur, "expert_applications", "full_name"):
        cols.append("full_name")
        vals.append(full_name)

    if _has_column(cur, "expert_applications", "about_text"):
        cols.append("about_text")
        vals.append(about_text)

    # Required in some schemas
    if _has_column(cur, "expert_applications", "topic"):
        cols.append("topic")
        vals.append(topic_value)

    if _has_column(cur, "expert_applications", "experience"):
        cols.append("experience")
        vals.append(experience or "")

    if _has_column(cur, "expert_applications", "motivation"):
        cols.append("motivation")
        vals.append(motivation or "")

    if _has_column(cur, "expert_applications", "contact"):
        cols.append("contact")
        vals.append(contact)

    # Status / timestamps
    cols.append("status")
    vals.append("pending")

    cols.append("created_at")
    vals.append(_now())

    placeholders = ",".join(["?"] * len(cols))
    col_sql = ", ".join(cols)

    cur.execute(f"INSERT INTO expert_applications({col_sql}) VALUES ({placeholders})", tuple(vals))

    conn.commit()
    app_id = int(cur.lastrowid)
    conn.close()
    return app_id


def list_pending_applications(limit: int = 50) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM expert_applications
        WHERE status='pending'
        ORDER BY application_id ASC
        LIMIT ?
    """, (int(limit),))
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def set_application_status(application_id: int, status: str) -> bool:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("UPDATE expert_applications SET status=? WHERE application_id=?", (status, int(application_id)))
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok
