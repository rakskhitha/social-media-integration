"""Persistence for Social Media daily digest settings and run history.
Primary: shaktidb_social (PostgreSQL tables sm_digest_settings & sm_digest_history).
Fallback: Local JSON files (sm_digest_settings.json & sm_digest_history.json) if Postgres is unreachable.
"""

import json
import logging
import os
import threading
import uuid
import psycopg2
from sm_summarizer.summarizer_service import DB_CONFIG

logger = logging.getLogger("sm_summarizer.digest_store")

_MAX_HISTORY = 90
_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(_DATA_DIR, exist_ok=True)

_SETTINGS_FILE = os.path.join(_DATA_DIR, "digest_settings.json")
_HISTORY_FILE = os.path.join(_DATA_DIR, "digest_history.json")
_file_lock = threading.Lock()

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sm_digest_settings (
    user_id     BIGINT PRIMARY KEY,
    keyword     TEXT NOT NULL DEFAULT '',
    handles     TEXT[] NOT NULL DEFAULT '{}',
    channels    TEXT[] NOT NULL DEFAULT '{}',
    to_emails   TEXT[] NOT NULL DEFAULT '{}',
    send_time   TEXT NOT NULL DEFAULT '08:00',
    timezone    TEXT NOT NULL DEFAULT 'Asia/Kolkata',
    enabled     BOOLEAN NOT NULL DEFAULT true,
    mode        TEXT NOT NULL DEFAULT 'summary',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sm_digest_history (
    id      TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    date    TEXT NOT NULL,
    keyword TEXT NOT NULL,
    count   INTEGER NOT NULL DEFAULT 0,
    status  TEXT NOT NULL,
    error   TEXT,
    sent_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sm_digest_history_user_sent_at ON sm_digest_history(user_id, sent_at DESC);
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

# --- LOCAL JSON FALLBACK & DB SYNC HELPERS ---
def _load_json_file(path, default):
    with _file_lock:
        if not os.path.exists(path):
            return default
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default

def _save_json_file(path, data):
    with _file_lock:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

def _sync_local_to_db(conn):
    """Automatically syncs any settings or history logged in local JSON files into shaktidb_social."""
    try:
        cur = conn.cursor()
        cur.execute(_SCHEMA_SQL)
        # 1. Sync Settings
        local_settings = _load_json_file(_SETTINGS_FILE, {})
        for uid_str, settings in local_settings.items():
            user_id = int(uid_str)
            cur.execute(
                """
                INSERT INTO sm_digest_settings
                    (user_id, keyword, handles, channels, to_emails, send_time, timezone, enabled, mode, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (user_id) DO UPDATE SET
                    keyword = EXCLUDED.keyword, handles = EXCLUDED.handles, channels = EXCLUDED.channels,
                    to_emails = EXCLUDED.to_emails, send_time = EXCLUDED.send_time, timezone = EXCLUDED.timezone,
                    enabled = EXCLUDED.enabled, mode = EXCLUDED.mode, updated_at = now()
                """,
                (
                    user_id, settings.get("keyword", ""), settings.get("handles") or [],
                    settings.get("channels") or [], settings.get("to_emails") or [],
                    settings.get("send_time", "08:00"), settings.get("timezone", "Asia/Kolkata"),
                    settings.get("enabled", True), settings.get("mode", "summary")
                )
            )

        # 2. Sync History Logs
        local_history = _load_json_file(_HISTORY_FILE, {})
        for uid_str, entries in local_history.items():
            user_id = int(uid_str)
            for entry in entries:
                cur.execute(
                    "INSERT INTO sm_digest_history (id, user_id, date, keyword, count, status, error, sent_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                    (
                        entry.get("id") or uuid.uuid4().hex, user_id, entry.get("date", ""),
                        entry.get("keyword", ""), entry.get("count", 0), entry.get("status", "sent"),
                        entry.get("error"), entry.get("sent_at")
                    )
                )
        conn.commit()
        cur.close()
    except Exception as e:
        logger.warning(f"Failed auto-syncing local JSON to DB: {e}")

_db_offline = False

# --- PUBLIC DIGEST STORE API ---

def load_settings(user_id: int) -> dict | None:
    global _db_offline
    # 1. Try PostgreSQL if available
    if not _db_offline:
        try:
            conn = _get_db_conn()
            _sync_local_to_db(conn)
            cur = conn.cursor()
            cur.execute(
                "SELECT user_id, keyword, handles, channels, to_emails, send_time, timezone, enabled, mode, updated_at "
                "FROM sm_digest_settings WHERE user_id = %s", (user_id,)
            )
            row = cur.fetchone()
            settings = _row_to_dict(cur, row)
            cur.close()
            conn.close()
            if settings is not None:
                settings["updated_at"] = settings["updated_at"].isoformat() if hasattr(settings["updated_at"], 'isoformat') else str(settings["updated_at"])
                return settings
        except Exception as e:
            logger.warning(f"PostgreSQL digest load failed, using local file store: {e}")
            _db_offline = True

    # 2. Fallback to Local JSON
    all_settings = _load_json_file(_SETTINGS_FILE, {})
    return all_settings.get(str(user_id))

def save_settings(user_id: int, settings: dict) -> dict:
    # 1. Save to Local JSON File first for safety
    all_settings = _load_json_file(_SETTINGS_FILE, {})
    all_settings[str(user_id)] = settings
    _save_json_file(_SETTINGS_FILE, all_settings)

    # 2. Try PostgreSQL & Auto-Sync
    try:
        conn = _get_db_conn()
        _sync_local_to_db(conn)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO sm_digest_settings
                (user_id, keyword, handles, channels, to_emails, send_time, timezone, enabled, mode, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (user_id) DO UPDATE SET
                keyword = EXCLUDED.keyword,
                handles = EXCLUDED.handles,
                channels = EXCLUDED.channels,
                to_emails = EXCLUDED.to_emails,
                send_time = EXCLUDED.send_time,
                timezone = EXCLUDED.timezone,
                enabled = EXCLUDED.enabled,
                mode = EXCLUDED.mode,
                updated_at = now()
            """,
            (
                user_id,
                settings.get("keyword", ""),
                settings.get("handles") or [],
                settings.get("channels") or [],
                settings.get("to_emails") or [],
                settings.get("send_time", "08:00"),
                settings.get("timezone", "Asia/Kolkata"),
                settings.get("enabled", True),
                settings.get("mode", "summary")
            )
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"PostgreSQL digest save failed (saved to local file): {e}")

    return settings

def list_all_settings() -> list[dict]:
    # 1. Try PostgreSQL
    try:
        conn = _get_db_conn()
        _sync_local_to_db(conn)
        cur = conn.cursor()
        cur.execute(
            "SELECT user_id, keyword, handles, channels, to_emails, send_time, timezone, enabled, mode, updated_at "
            "FROM sm_digest_settings"
        )
        rows = cur.fetchall()
        results = []
        for row in rows:
            d = _row_to_dict(cur, row)
            d["updated_at"] = d["updated_at"].isoformat() if hasattr(d["updated_at"], 'isoformat') else str(d["updated_at"])
            results.append(d)
        cur.close()
        conn.close()
        if results:
            return results
    except Exception as e:
        logger.warning(f"PostgreSQL list_all_settings failed, using local file store: {e}")

    # 2. Fallback to Local JSON
    all_settings = _load_json_file(_SETTINGS_FILE, {})
    return list(all_settings.values())

def load_history(user_id: int) -> list[dict]:
    # 1. Try PostgreSQL
    try:
        conn = _get_db_conn()
        _sync_local_to_db(conn)
        cur = conn.cursor()
        cur.execute(
            "SELECT id, user_id, date, keyword, count, status, error, sent_at "
            "FROM sm_digest_history WHERE user_id = %s ORDER BY sent_at DESC",
            (user_id,)
        )
        rows = cur.fetchall()
        history = []
        for row in rows:
            entry = _row_to_dict(cur, row)
            entry["sent_at"] = entry["sent_at"].isoformat() if hasattr(entry["sent_at"], 'isoformat') else str(entry["sent_at"])
            history.append(entry)
        cur.close()
        conn.close()
        if history:
            return history
    except Exception as e:
        logger.warning(f"PostgreSQL load_history failed, using local file store: {e}")

    # 2. Fallback to Local JSON
    all_history = _load_json_file(_HISTORY_FILE, {})
    user_hist = all_history.get(str(user_id), [])
    return sorted(user_hist, key=lambda x: x.get("sent_at", ""), reverse=True)

def append_history(user_id: int, entry: dict) -> dict:
    entry_id = entry.get("id") or uuid.uuid4().hex
    entry["id"] = entry_id
    entry["user_id"] = user_id

    # 1. Save to Local JSON File
    all_history = _load_json_file(_HISTORY_FILE, {})
    user_hist = all_history.get(str(user_id), [])
    user_hist.insert(0, entry)
    user_hist = user_hist[:_MAX_HISTORY]
    all_history[str(user_id)] = user_hist
    _save_json_file(_HISTORY_FILE, all_history)

    # 2. Try PostgreSQL
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute(_SCHEMA_SQL)
        cur.execute(
            "INSERT INTO sm_digest_history (id, user_id, date, keyword, count, status, error, sent_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                entry["id"], user_id, entry["date"], entry.get("keyword", ""), entry.get("count", 0),
                entry.get("status", "sent"), entry.get("error"), entry.get("sent_at")
            )
        )
        cur.execute(
            "DELETE FROM sm_digest_history WHERE user_id = %s AND id NOT IN ("
            "  SELECT id FROM sm_digest_history WHERE user_id = %s ORDER BY sent_at DESC LIMIT %s"
            ")",
            (user_id, user_id, _MAX_HISTORY)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"PostgreSQL append_history failed (saved to local file): {e}")

    return entry

def delete_history_entry(user_id: int, entry_id: str) -> bool:
    # 1. Delete from Local JSON File
    all_history = _load_json_file(_HISTORY_FILE, {})
    user_hist = all_history.get(str(user_id), [])
    new_hist = [item for item in user_hist if item.get("id") != entry_id]
    all_history[str(user_id)] = new_hist
    _save_json_file(_HISTORY_FILE, all_history)

    # 2. Try PostgreSQL
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute(_SCHEMA_SQL)
        cur.execute("DELETE FROM sm_digest_history WHERE id = %s AND user_id = %s", (entry_id, user_id))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"PostgreSQL delete_history_entry failed: {e}")

    return True
