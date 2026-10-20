"""Persistence for Social Media Alert Sessions & Run History.
Primary: shaktidb_social (PostgreSQL tables: sm_alert_sessions, sm_alert_session_runs, sm_alert_session_run_posts, sm_alert_session_run_deliveries).
Fallback: Local JSON files (sm_alert_sessions.json & sm_alert_session_runs.json) if Postgres is unreachable.
"""

import json
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
import psycopg2
from sm_summarizer.summarizer_service import DB_CONFIG

logger = logging.getLogger("sm_summarizer.alert_session_store")

_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(_DATA_DIR, exist_ok=True)

_SESSIONS_FILE = os.path.join(_DATA_DIR, "alert_sessions.json")
_RUNS_FILE = os.path.join(_DATA_DIR, "alert_session_runs.json")
_file_lock = threading.Lock()

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sm_alert_sessions (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL DEFAULT 1,
    name            TEXT NOT NULL,
    keyword         TEXT NOT NULL DEFAULT '',
    handles         TEXT[] NOT NULL DEFAULT '{}',
    channels        TEXT[] NOT NULL DEFAULT '{}',
    to_emails       TEXT[] NOT NULL DEFAULT '{}',
    schedule_mode   TEXT NOT NULL DEFAULT 'preset',
    interval_hours  INTEGER NOT NULL DEFAULT 24,
    custom_times    TEXT[] NOT NULL DEFAULT '{}',
    timezone        TEXT NOT NULL DEFAULT 'Asia/Kolkata',
    enabled         BOOLEAN NOT NULL DEFAULT true,
    mode            TEXT NOT NULL DEFAULT 'summary',
    lookback        TEXT NOT NULL DEFAULT '24h',
    start_date      DATE,
    end_date        DATE,
    initial_run_done BOOLEAN NOT NULL DEFAULT false,
    last_window_end TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sm_alert_session_runs (
    id              TEXT PRIMARY KEY,
    session_id      BIGINT NOT NULL REFERENCES sm_alert_sessions(id) ON DELETE CASCADE,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    status          TEXT NOT NULL,
    post_count      INTEGER NOT NULL DEFAULT 0,
    summary         TEXT,
    error           TEXT,
    config_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS sm_alert_session_run_posts (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES sm_alert_session_runs(id) ON DELETE CASCADE,
    platform        TEXT,
    author          TEXT,
    content         TEXT,
    date            TIMESTAMPTZ,
    url             TEXT
);

CREATE TABLE IF NOT EXISTS sm_alert_session_run_deliveries (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES sm_alert_session_runs(id) ON DELETE CASCADE,
    recipient_email TEXT NOT NULL,
    status          TEXT NOT NULL,
    sent_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    error           TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS sm_alert_session_runs_window_unique
ON sm_alert_session_runs(session_id, window_start, window_end);

CREATE INDEX IF NOT EXISTS sm_alert_sessions_user_idx ON sm_alert_sessions(user_id);
"""

def _get_db_conn():
    config = dict(DB_CONFIG)
    config["connect_timeout"] = 5
    return psycopg2.connect(**config)

def _row_to_dict(cur, row):
    if row is None:
        return None
    columns = [d[0] for d in cur.description]
    return dict(zip(columns, row))

def _load_json_file(path, default):
    with _file_lock:
        if not os.path.exists(path):
            return default
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default

def _sanitize_for_json(obj):
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    elif hasattr(obj, 'isoformat'):
        return obj.isoformat()
    return obj

def _save_json_file(path, data):
    sanitized = _sanitize_for_json(data)
    with _file_lock:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(sanitized, f, indent=2, ensure_ascii=False)

def _init_db_schema():
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute(_SCHEMA_SQL)
        cur.execute("""
            ALTER TABLE sm_alert_sessions ADD COLUMN IF NOT EXISTS lookback TEXT DEFAULT '24h';
            ALTER TABLE sm_alert_sessions ADD COLUMN IF NOT EXISTS start_date DATE;
            ALTER TABLE sm_alert_sessions ADD COLUMN IF NOT EXISTS end_date DATE;
            ALTER TABLE sm_alert_sessions ADD COLUMN IF NOT EXISTS initial_run_done BOOLEAN DEFAULT false;
        """)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("PostgreSQL schema initialization for sm_alert_sessions failed/skipped: %s", e)

# Run schema init in background thread to avoid blocking startup
threading.Thread(target=_init_db_schema, daemon=True).start()

_db_offline = False

# --- SESSIONS CRUD ---

def list_all_sessions(user_id: int = None) -> list[dict]:
    """Fetch all alert sessions for a user or system."""
    global _db_offline
    if not _db_offline:
        try:
            conn = _get_db_conn()
            cur = conn.cursor()
            if user_id:
                cur.execute("SELECT * FROM sm_alert_sessions WHERE user_id = %s ORDER BY id ASC;", (user_id,))
            else:
                cur.execute("SELECT * FROM sm_alert_sessions ORDER BY id ASC;")
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            sessions = [dict(zip(cols, r)) for r in rows]
            cur.close()
            conn.close()
            return sessions
        except Exception as e:
            logger.warning("DB list_all_sessions failed, using local JSON fallback: %s", e)
            _db_offline = True

    local_data = _load_json_file(_SESSIONS_FILE, [])
    if user_id:
        return [s for s in local_data if s.get("user_id") == user_id]
    return local_data

def get_session(session_id: int) -> dict | None:
    """Fetch a single alert session by ID."""
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM sm_alert_sessions WHERE id = %s;", (session_id,))
        row = cur.fetchone()
        res = _row_to_dict(cur, row) if row else None
        cur.close()
        conn.close()
        if res:
            return res
    except Exception as e:
        logger.warning("DB get_session failed, trying local JSON fallback: %s", e)

    local_data = _load_json_file(_SESSIONS_FILE, [])
    for s in local_data:
        if s.get("id") == session_id:
            return s
    return None

def create_session(data: dict) -> dict:
    """Create a new alert session."""
    user_id = data.get("user_id", 1)
    name = data.get("name", "Untitled Session")
    keyword = data.get("keyword", "")
    handles = data.get("handles") or []
    channels = data.get("channels") or []
    to_emails = data.get("to_emails") or []
    schedule_mode = data.get("schedule_mode", "preset")
    interval_hours = int(data.get("interval_hours", 24))
    custom_times = data.get("custom_times") or []
    timezone = data.get("timezone", "Asia/Kolkata")
    enabled = bool(data.get("enabled", True))
    mode = data.get("mode", "summary")
    lookback = data.get("lookback") or "24h"
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    initial_run_done = False

    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO sm_alert_sessions
                (user_id, name, keyword, handles, channels, to_emails, schedule_mode, interval_hours, custom_times, timezone, enabled, mode, lookback, start_date, end_date, initial_run_done, last_window_end, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, now(), now())
            RETURNING *;
            """,
            (user_id, name, keyword, handles, channels, to_emails, schedule_mode, interval_hours, custom_times, timezone, enabled, mode, lookback, start_date, end_date, initial_run_done)
        )
        row = cur.fetchone()
        session = _row_to_dict(cur, row)
        conn.commit()
        cur.close()
        conn.close()
        _sync_session_to_json(session)
        return session
    except Exception as e:
        logger.warning("DB create_session failed, writing to local JSON fallback: %s", e)
        local_data = _load_json_file(_SESSIONS_FILE, [])
        new_id = max([s.get("id", 0) for s in local_data] + [0]) + 1
        now_str = datetime.now(timezone.utc if hasattr(timezone, 'utc') else None).isoformat()
        session = {
            "id": new_id, "user_id": user_id, "name": name, "keyword": keyword,
            "handles": handles, "channels": channels, "to_emails": to_emails,
            "schedule_mode": schedule_mode, "interval_hours": interval_hours,
            "custom_times": custom_times, "timezone": timezone, "enabled": enabled,
            "mode": mode, "lookback": lookback, "start_date": start_date, "end_date": end_date,
            "initial_run_done": initial_run_done, "last_window_end": None,
            "created_at": now_str, "updated_at": now_str
        }
        local_data.append(session)
        _save_json_file(_SESSIONS_FILE, local_data)
        return session

def update_session(session_id: int, data: dict) -> dict | None:
    """Update an existing alert session."""
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE sm_alert_sessions
            SET name = %s, keyword = %s, handles = %s, channels = %s, to_emails = %s,
                schedule_mode = %s, interval_hours = %s, custom_times = %s,
                timezone = %s, enabled = %s, mode = %s, lookback = %s,
                start_date = %s, end_date = %s, updated_at = now()
            WHERE id = %s
            RETURNING *;
            """,
            (
                data.get("name", "Alert Session"), data.get("keyword", ""), data.get("handles") or [],
                data.get("channels") or [], data.get("to_emails") or [], data.get("schedule_mode", "preset"),
                int(data.get("interval_hours", 24)), data.get("custom_times") or [],
                data.get("timezone", "Asia/Kolkata"), bool(data.get("enabled", True)),
                data.get("mode", "summary"), data.get("lookback", "24h"),
                data.get("start_date"), data.get("end_date"), session_id
            )
        )
        row = cur.fetchone()
        session = _row_to_dict(cur, row)
        conn.commit()
        cur.close()
        conn.close()
        if session:
            _sync_session_to_json(session)
            return session
    except Exception as e:
        logger.warning("DB update_session failed, updating local JSON fallback: %s", e)

    local_data = _load_json_file(_SESSIONS_FILE, [])
    for i, s in enumerate(local_data):
        if s.get("id") == session_id:
            s.update(data)
            s["updated_at"] = datetime.now(timezone.utc).isoformat()
            local_data[i] = s
            _save_json_file(_SESSIONS_FILE, local_data)
            return s
    return None

def mark_initial_run_done(session_id: int, watermark_dt) -> None:
    """Marks initial lookback run as completed and updates last_window_end watermark."""
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE sm_alert_sessions
            SET initial_run_done = true, last_window_end = %s, updated_at = now()
            WHERE id = %s;
            """,
            (watermark_dt, session_id)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("DB mark_initial_run_done failed: %s", e)

    local_data = _load_json_file(_SESSIONS_FILE, [])
    for s in local_data:
        if s.get("id") == session_id:
            s["initial_run_done"] = True
            s["last_window_end"] = watermark_dt.isoformat() if hasattr(watermark_dt, 'isoformat') else str(watermark_dt)
            s["updated_at"] = datetime.now(timezone.utc).isoformat()
            break
    _save_json_file(_SESSIONS_FILE, local_data)

def update_session_watermark(session_id: int, watermark_dt) -> None:
    """Update last_window_end watermark timestamp for a session."""
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE sm_alert_sessions SET last_window_end = %s, updated_at = now() WHERE id = %s;",
            (watermark_dt, session_id)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("DB update_session_watermark failed: %s", e)

    local_data = _load_json_file(_SESSIONS_FILE, [])
    w_str = watermark_dt.isoformat() if hasattr(watermark_dt, 'isoformat') else str(watermark_dt)
    for s in local_data:
        if s.get("id") == session_id:
            s["last_window_end"] = w_str
            break
    _save_json_file(_SESSIONS_FILE, local_data)

def delete_session(session_id: int) -> bool:
    """Delete an alert session and its history."""
    deleted = False
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM sm_alert_sessions WHERE id = %s;", (session_id,))
        deleted = cur.rowcount > 0
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("DB delete_session failed: %s", e)

    local_data = _load_json_file(_SESSIONS_FILE, [])
    new_data = [s for s in local_data if s.get("id") != session_id]
    if len(new_data) < len(local_data):
        deleted = True
        _save_json_file(_SESSIONS_FILE, new_data)
    return deleted

def _sync_session_to_json(session: dict):
    local_data = _load_json_file(_SESSIONS_FILE, [])
    found = False
    for i, s in enumerate(local_data):
        if s.get("id") == session["id"]:
            local_data[i] = session
            found = True
            break
    if not found:
        local_data.append(session)
    _save_json_file(_SESSIONS_FILE, local_data)

# --- RUNS & HISTORY STORE ---

def create_run(
    session_id: int,
    window_start,
    window_end,
    status: str,
    post_count: int = 0,
    summary: str = "",
    error: str = None,
    config_snapshot: dict = None
) -> dict:
    """Record an alert session run."""
    run_id = str(uuid.uuid4()).replace("-", "")
    config_snapshot = config_snapshot or {}
    now_dt = datetime.now(timezone.utc)
    now_iso = now_dt.isoformat()

    w_start_str = window_start.isoformat() if hasattr(window_start, 'isoformat') else str(window_start)
    w_end_str = window_end.isoformat() if hasattr(window_end, 'isoformat') else str(window_end)

    run = {
        "id": run_id,
        "session_id": session_id,
        "window_start": w_start_str,
        "window_end": w_end_str,
        "run_at": now_iso,
        "status": status,
        "post_count": post_count,
        "summary": summary,
        "error": error,
        "config_snapshot": config_snapshot
    }

    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO sm_alert_session_runs
                (id, session_id, window_start, window_end, run_at, status, post_count, summary, error, config_snapshot)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (session_id, window_start, window_end) DO UPDATE SET
                run_at = EXCLUDED.run_at, status = EXCLUDED.status, post_count = EXCLUDED.post_count,
                summary = EXCLUDED.summary, error = EXCLUDED.error, config_snapshot = EXCLUDED.config_snapshot
            RETURNING *;
            """,
            (run_id, session_id, window_start, window_end, now_dt, status, post_count, summary, error, json.dumps(config_snapshot))
        )
        row = cur.fetchone()
        saved_run = _row_to_dict(cur, row)
        conn.commit()
        cur.close()
        conn.close()
        if saved_run:
            run = saved_run
    except Exception as e:
        logger.warning("DB create_run failed, logging to local JSON fallback: %s", e)

    _append_run_to_json(run)
    return run

def create_run_posts(run_id: str, posts: list[dict]):
    """Save posts matched in a run."""
    if not posts:
        return
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        for p in posts:
            cur.execute(
                """
                INSERT INTO sm_alert_session_run_posts (run_id, platform, author, content, date, url)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (run_id, p.get("platform"), p.get("author"), p.get("content"), p.get("date"), p.get("url"))
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("DB create_run_posts failed: %s", e)

def create_run_deliveries(run_id: str, deliveries: list[dict]):
    """Save email recipient delivery statuses for a run."""
    if not deliveries:
        return
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        for d in deliveries:
            cur.execute(
                """
                INSERT INTO sm_alert_session_run_deliveries (run_id, recipient_email, status, sent_at, error)
                VALUES (%s, %s, %s, now(), %s);
                """,
                (run_id, d.get("recipient_email"), d.get("status"), d.get("error"))
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("DB create_run_deliveries failed: %s", e)

def list_runs(session_id: int, limit: int = 50) -> list[dict]:
    """Fetch run history for a session."""
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM sm_alert_session_runs WHERE session_id = %s ORDER BY window_end DESC LIMIT %s;",
            (session_id, limit)
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        runs = [dict(zip(cols, r)) for r in rows]
        
        # Attach delivery logs
        for r in runs:
            cur.execute("SELECT recipient_email, status, sent_at, error FROM sm_alert_session_run_deliveries WHERE run_id = %s;", (r["id"],))
            d_rows = cur.fetchall()
            d_cols = [d[0] for d in cur.description]
            r["deliveries"] = [dict(zip(d_cols, dr)) for dr in d_rows]
            
        cur.close()
        conn.close()
        return runs
    except Exception as e:
        logger.warning("DB list_runs failed, reading local JSON fallback: %s", e)
        local_runs = _load_json_file(_RUNS_FILE, [])
        matched = [r for r in local_runs if r.get("session_id") == session_id]
        return sorted(matched, key=lambda x: str(x.get("window_end", "")), reverse=True)[:limit]

def find_non_failed_run(session_id: int, window_start, window_end) -> dict | None:
    """Check if a window has already been successfully processed."""
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM sm_alert_session_runs
            WHERE session_id = %s AND window_start = %s AND window_end = %s AND status != 'failed';
            """,
            (session_id, window_start, window_end)
        )
        row = cur.fetchone()
        res = _row_to_dict(cur, row) if row else None
        cur.close()
        conn.close()
        if res:
            return res
    except Exception as e:
        logger.warning("DB find_non_failed_run failed: %s", e)

    w_start_str = window_start.isoformat() if hasattr(window_start, 'isoformat') else str(window_start)
    w_end_str = window_end.isoformat() if hasattr(window_end, 'isoformat') else str(window_end)

    local_runs = _load_json_file(_RUNS_FILE, [])
    for r in local_runs:
        if r.get("session_id") == session_id and str(r.get("window_start")) == w_start_str and str(r.get("window_end")) == w_end_str and r.get("status") != "failed":
            return r
    return None

def _append_run_to_json(run: dict):
    local_runs = _load_json_file(_RUNS_FILE, [])
    # Replace existing or append
    found = False
    for i, r in enumerate(local_runs):
        if r.get("session_id") == run["session_id"] and r.get("window_start") == run["window_start"] and r.get("window_end") == run["window_end"]:
            local_runs[i] = run
            found = True
            break
    if not found:
        local_runs.append(run)
    _save_json_file(_RUNS_FILE, local_runs)
