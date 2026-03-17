# DB_HELP_COMPAT_MIGR_2026_01_19
import sqlite3
import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

from .config import DB_PATH_HELP, ADMIN_USER_IDS

ALL_GUILD_IDS = [1, 2, 3, 4]


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


def current_month_key() -> str:
    return datetime.datetime.now().strftime("%Y-%m")


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


def _clean_username(username: Optional[str]) -> Optional[str]:
    if not username:
        return None
    value = str(username).strip().lstrip("@")
    return value or None


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
    return get_expert_niches(user_id) if is_expert(user_id) else []


def validate_expert_part(user_id: int, guild_id: int) -> bool:
    return (guild_id in set(get_expert_niches(user_id))) if is_expert(user_id) else False


# ----------------- init + migrations -----------------

def init_help_db() -> None:
    conn = _connect()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS telegram_users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        full_name TEXT,
        last_seen_at TEXT
    )
    """)

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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS exercise_records (
        exercise_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        exercise_name TEXT NOT NULL,
        amount INTEGER NOT NULL,
        unit TEXT NOT NULL,
        ai_status TEXT NOT NULL DEFAULT 'Принято',
        video_file_id TEXT,
        created_at TEXT NOT NULL
    )
    """)
    _add_column(cur, "exercise_records", "ai_status", "TEXT NOT NULL DEFAULT 'Принято'")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS book_records (
        book_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        book_name TEXT NOT NULL,
        questions_text TEXT,
        answers_text TEXT,
        result_text TEXT,
        created_at TEXT NOT NULL
    )
    """)

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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tg_users_username ON telegram_users(username)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_exercise_records_user_created ON exercise_records(user_id, created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_book_records_user_created ON book_records(user_id, created_at)")
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

    # expert_niches (expert can have up to 2 niches)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS expert_niches (
        user_id INTEGER NOT NULL,
        niche_id INTEGER NOT NULL,
        added_at TEXT,
        added_by INTEGER,
        PRIMARY KEY (user_id, niche_id)
    )
    """)

    # expert_applications: niche_id
    _add_column(cur, "expert_applications", "niche_id", "INTEGER")

    # migration: if there are legacy requests with guild_id=5, hide them from expert feed (is_deleted=1)
    try:
        cur.execute("UPDATE help_requests SET is_deleted=1 WHERE guild_id=5 AND (is_deleted IS NULL OR is_deleted=0)")
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
    try:
        cur.execute("DELETE FROM expert_niches WHERE user_id=?", (int(user_id),))
    except Exception:
        pass
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


def upsert_telegram_user(user_id: int, username: Optional[str], full_name: Optional[str]) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO telegram_users(user_id, username, full_name, last_seen_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            full_name=excluded.full_name,
            last_seen_at=excluded.last_seen_at
        """,
        (int(user_id), _clean_username(username), full_name, _now()),
    )
    conn.commit()
    conn.close()


def save_exercise_record(
    user_id: int,
    exercise_name: str,
    amount: int,
    unit: str,
    ai_status: str = "Принято",
    video_file_id: Optional[str] = None,
    username: Optional[str] = None,
    full_name: Optional[str] = None,
) -> int:
    upsert_telegram_user(user_id, username, full_name)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO exercise_records(user_id, exercise_name, amount, unit, ai_status, video_file_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (int(user_id), exercise_name, int(amount), unit, ai_status, video_file_id, _now()),
    )
    conn.commit()
    exercise_id = int(cur.lastrowid)
    conn.close()
    return exercise_id


def list_exercise_records(user_id: int, limit: int = 1000) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    ai_status_sql = "ai_status" if _has_column(cur, "exercise_records", "ai_status") else "'Принято' AS ai_status"
    cur.execute(
        f"""
        SELECT exercise_id, user_id, exercise_name, amount, unit, {ai_status_sql}, video_file_id, created_at
        FROM exercise_records
        WHERE user_id=?
        ORDER BY created_at DESC, exercise_id DESC
        LIMIT ?
        """,
        (int(user_id), int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def save_book_record(
    user_id: int,
    book_name: str,
    questions: List[str],
    answers: List[str],
    result_text: str,
    username: Optional[str] = None,
    full_name: Optional[str] = None,
) -> int:
    upsert_telegram_user(user_id, username, full_name)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO book_records(user_id, book_name, questions_text, answers_text, result_text, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            int(user_id),
            book_name,
            "\n".join(questions),
            "\n".join(answers),
            result_text,
            _now(),
        ),
    )
    conn.commit()
    book_id = int(cur.lastrowid)
    conn.close()
    return book_id


def list_book_records(user_id: int, limit: int = 1000) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT book_id, user_id, book_name, questions_text, answers_text, result_text, created_at
        FROM book_records
        WHERE user_id=?
        ORDER BY created_at DESC, book_id DESC
        LIMIT ?
        """,
        (int(user_id), int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def export_books_and_exercises_rows(month_key: Optional[str] = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    conn = _connect()
    cur = conn.cursor()
    ai_status_sql = "e.ai_status" if _has_column(cur, "exercise_records", "ai_status") else "'Принято' AS ai_status"

    exercise_where = ""
    exercise_params: List[Any] = []
    book_where = ""
    book_params: List[Any] = []
    if month_key:
        exercise_where = "WHERE substr(e.created_at, 1, 7) = ?"
        exercise_params.append(month_key)
        book_where = "WHERE substr(b.created_at, 1, 7) = ?"
        book_params.append(month_key)

    cur.execute(
        f"""
        SELECT
            e.exercise_id,
            e.created_at,
            e.user_id,
            u.username,
            u.full_name,
            e.exercise_name,
            e.amount,
            e.unit,
            {ai_status_sql},
            e.video_file_id
        FROM exercise_records e
        LEFT JOIN telegram_users u ON u.user_id = e.user_id
        {exercise_where}
        ORDER BY e.created_at DESC, e.exercise_id DESC
        """,
        exercise_params,
    )
    exercise_rows = [dict(r) for r in cur.fetchall()]

    cur.execute(
        f"""
        SELECT
            b.book_id,
            b.created_at,
            b.user_id,
            u.username,
            u.full_name,
            b.book_name,
            b.result_text,
            b.questions_text,
            b.answers_text
        FROM book_records b
        LEFT JOIN telegram_users u ON u.user_id = b.user_id
        {book_where}
        ORDER BY b.created_at DESC, b.book_id DESC
        """,
        book_params,
    )
    book_rows = [dict(r) for r in cur.fetchall()]

    conn.close()
    return exercise_rows, book_rows


def export_help_requests_and_answers_rows(month_key: Optional[str] = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    conn = _connect()
    cur = conn.cursor()

    request_where = ""
    request_params: List[Any] = []
    answer_where = ""
    answer_params: List[Any] = []
    if month_key:
        request_where = "WHERE substr(r.created_at, 1, 7) = ?"
        request_params.append(month_key)
        answer_where = "WHERE substr(a.created_at, 1, 7) = ?"
        answer_params.append(month_key)

    cur.execute(
        f"""
        SELECT
            r.request_id,
            r.created_at,
            r.guild_id,
            r.student_user_id,
            su.username AS student_username,
            su.full_name AS student_full_name,
            r.problem_essence,
            r.problem_since,
            r.tried_actions,
            r.desired_result,
            r.status,
            r.is_closed,
            r.is_deleted,
            (
                SELECT COUNT(*)
                FROM help_answers a
                WHERE a.request_id = r.request_id
            ) AS answers_count
        FROM help_requests r
        LEFT JOIN telegram_users su ON su.user_id = r.student_user_id
        {request_where}
        ORDER BY r.created_at DESC, r.request_id DESC
        """,
        request_params,
    )
    request_rows = [dict(r) for r in cur.fetchall()]

    cur.execute(
        f"""
        SELECT
            a.answer_id,
            a.created_at,
            a.request_id,
            r.guild_id,
            r.student_user_id,
            su.username AS student_username,
            su.full_name AS student_full_name,
            r.problem_essence,
            r.desired_result,
            a.expert_user_id,
            COALESCE(a.expert_username, e.username) AS expert_username,
            COALESCE(a.expert_full_name, e.full_name) AS expert_full_name,
            a.answer_text
        FROM help_answers a
        LEFT JOIN help_requests r ON r.request_id = a.request_id
        LEFT JOIN telegram_users su ON su.user_id = r.student_user_id
        LEFT JOIN experts e ON e.user_id = a.expert_user_id
        {answer_where}
        ORDER BY a.created_at DESC, a.answer_id DESC
        """,
        answer_params,
    )
    answer_rows = [dict(r) for r in cur.fetchall()]

    conn.close()
    return request_rows, answer_rows




# ----------------- expert niches -----------------

def get_expert_niches(user_id: int) -> List[int]:
    """Return niche ids (1..4) for expert."""
    conn = _connect()
    cur = conn.cursor()
    try:
        if _table_exists(cur, "expert_niches"):
            cur.execute("SELECT niche_id FROM expert_niches WHERE user_id=? ORDER BY niche_id ASC", (int(user_id),))
            rows = cur.fetchall()
            return [int(r["niche_id"] if isinstance(r, sqlite3.Row) else r[0]) for r in rows]
    finally:
        conn.close()
    return []


def add_expert_niche(user_id: int, niche_id: int, added_by: Optional[int] = None) -> bool:
    """Attach niche to expert. Returns True if inserted, False if already exists."""
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO expert_niches(user_id, niche_id, added_at, added_by) VALUES (?, ?, ?, ?)",
            (int(user_id), int(niche_id), _now(), int(added_by) if added_by is not None else None),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def remove_expert_niches(user_id: int) -> None:
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM expert_niches WHERE user_id=?", (int(user_id),))
        conn.commit()
    finally:
        conn.close()


def get_experts_for_niche(niche_id: int) -> List[int]:
    """List expert user_ids for a niche."""
    conn = _connect()
    cur = conn.cursor()
    try:
        if _table_exists(cur, "expert_niches"):
            cur.execute("SELECT user_id FROM expert_niches WHERE niche_id=? ORDER BY user_id ASC", (int(niche_id),))
            return [int(r["user_id"] if isinstance(r, sqlite3.Row) else r[0]) for r in cur.fetchall()]
    finally:
        conn.close()
    # fallback
    return [int(e["user_id"]) for e in (list_experts() or []) if e.get("user_id") is not None]

# ----------------- requests -----------------

def create_help_request(
    user_id: int,
    guild_id: int,
    essence: str,
    since: str,
    tried: str,
    result: str,
    username: Optional[str] = None,
    full_name: Optional[str] = None,
) -> int:
    upsert_telegram_user(user_id, username, full_name)

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


def list_open_requests(limit: int = 20, guild_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
    """List active requests for experts. Excludes is_deleted=1. Optionally filters by guild_ids."""
    conn = _connect()
    cur = conn.cursor()
    has_deleted = _has_column(cur, "help_requests", "is_deleted")
    has_status = _has_column(cur, "help_requests", "status")

    where = []
    params: List[Any] = []

    if _has_column(cur, "help_requests", "is_closed"):
        where.append("(is_closed=0 OR is_closed IS NULL)")
    if has_status:
        where.append("(status IS NULL OR status='active')")
    if has_deleted:
        where.append("(is_deleted=0 OR is_deleted IS NULL)")

    if guild_ids:
        gids = [int(x) for x in guild_ids if int(x) > 0]
        if gids:
            placeholders = ",".join(["?"] * len(gids))
            where.append("guild_id IN ({})".format(placeholders))
            params.extend(gids)

    where_sql = " AND ".join(where) if where else "1=1"

    sql = (
        "SELECT r.*, "
        "(SELECT COUNT(*) FROM help_answers a WHERE a.request_id=r.request_id) AS answers_count "
        "FROM help_requests r "
        "WHERE " + where_sql + " "
        "ORDER BY r.created_at DESC "
        "LIMIT ?"
    )

    cur.execute(sql, (*params, int(limit)))
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

    upsert_telegram_user(expert_user_id, expert_username, expert_full_name)

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


def create_expert_application(user_id: int, username: Optional[str], full_name: Optional[str], about_text: str, niche_id: Optional[int] = None) -> int:
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

    if niche_id is not None and _has_column(cur, "expert_applications", "niche_id"):
        cols.append("niche_id")
        vals.append(int(niche_id))

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


# ----------------- Niches (expert_niches) -----------------

def get_expert_niches(user_id: int) -> List[int]:
    conn = _connect()
    cur = conn.cursor()
    if not _table_exists(cur, "expert_niches"):
        conn.close()
        return []
    cur.execute("SELECT niche_id FROM expert_niches WHERE user_id=? ORDER BY niche_id ASC", (int(user_id),))
    rows = cur.fetchall()
    conn.close()
    return [int(r["niche_id"]) for r in rows]


def add_expert_niche(user_id: int, niche_id: int, added_by: Optional[int] = None) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO expert_niches(user_id, niche_id, added_at, added_by)
        VALUES (?, ?, ?, ?)
    """, (int(user_id), int(niche_id), _now(), int(added_by) if added_by is not None else None))
    conn.commit()
    conn.close()


def remove_expert_niche(user_id: int, niche_id: int) -> bool:
    conn = _connect()
    cur = conn.cursor()
    if not _table_exists(cur, "expert_niches"):
        conn.close()
        return False
    cur.execute("DELETE FROM expert_niches WHERE user_id=? AND niche_id=?", (int(user_id), int(niche_id)))
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok


def get_expert_user_ids_by_niche(niche_id: int) -> List[int]:
    conn = _connect()
    cur = conn.cursor()
    if not _table_exists(cur, "expert_niches"):
        conn.close()
        return []
    cur.execute("SELECT user_id FROM expert_niches WHERE niche_id=? ORDER BY user_id ASC", (int(niche_id),))
    rows = cur.fetchall()
    conn.close()
    return [int(r["user_id"]) for r in rows]


def list_experts_with_niches() -> List[Dict[str, Any]]:
    """Return experts with a stable order and their niches as list[int]."""
    experts = list_experts() or []
    out = []
    for e in experts:
        uid = int((e or {}).get("user_id") or 0)
        out.append({
            **(e or {}),
            "niches": get_expert_niches(uid),
        })
    return out


def list_open_requests_for_niches(niche_ids: List[int], limit: int = 10) -> List[Dict[str, Any]]:
    """Active requests for expert feed, filtered by niche ids and excluding deleted."""
    niche_ids = [int(x) for x in (niche_ids or []) if int(x) > 0]
    if not niche_ids:
        return []
    conn = _connect()
    cur = conn.cursor()
    placeholders = ",".join(["?"] * len(niche_ids))
    cur.execute(f"""
        SELECT * FROM help_requests
        WHERE is_closed=0 AND (is_deleted IS NULL OR is_deleted=0)
          AND guild_id IN ({placeholders})
        ORDER BY request_id ASC
        LIMIT ?
    """, tuple(niche_ids) + (int(limit),))
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]
