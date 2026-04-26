"""Per-project SQLite: system tables + settings (docs 04/05)."""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, Optional


def init_project_db(conn: sqlite3.Connection, *, seed_readme: bool = True) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS _table_registry (
            table_name TEXT PRIMARY KEY,
            layer TEXT NOT NULL DEFAULT 'dynamic',
            purpose TEXT,
            readme TEXT,
            schema_json TEXT,
            validation_status TEXT NOT NULL DEFAULT 'unknown'
        );

        CREATE TABLE IF NOT EXISTS _dependency_graph (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_table TEXT NOT NULL,
            from_column TEXT NOT NULL,
            to_table TEXT NOT NULL,
            to_column TEXT NOT NULL,
            edge_type TEXT NOT NULL DEFAULT 'ref'
        );

        CREATE TABLE IF NOT EXISTS _formula_registry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            formula TEXT NOT NULL,
            UNIQUE(table_name, column_name)
        );

        CREATE TABLE IF NOT EXISTS _snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT NOT NULL,
            created_at TEXT NOT NULL,
            note TEXT
        );

        CREATE TABLE IF NOT EXISTS project_settings (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pipeline_state (
            current_step TEXT NOT NULL DEFAULT '',
            completed_steps TEXT NOT NULL DEFAULT '[]',
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS _cell_provenance (
            table_name TEXT NOT NULL,
            row_id TEXT NOT NULL,
            column_name TEXT NOT NULL,
            source_tag TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (table_name, row_id, column_name)
        );

        CREATE TABLE IF NOT EXISTS _glossary (
            term_en TEXT PRIMARY KEY,
            term_zh TEXT NOT NULL,
            kind TEXT NOT NULL DEFAULT 'noun',
            brief TEXT,
            scope_table TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS _glossary_usage (
            term_en TEXT NOT NULL,
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (term_en, table_name, column_name)
        );

        CREATE TABLE IF NOT EXISTS _constants (
            name_en TEXT PRIMARY KEY,
            name_zh TEXT NOT NULL DEFAULT '',
            value_json TEXT NOT NULL,
            brief TEXT,
            scope_table TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS _agent_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            step_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'running',
            started_at TEXT NOT NULL,
            finished_at TEXT,
            design_text TEXT NOT NULL DEFAULT '',
            review_text TEXT NOT NULL DEFAULT '',
            execute_text TEXT NOT NULL DEFAULT '',
            tools_json TEXT NOT NULL DEFAULT '[]',
            error_text TEXT
        );
        """
    )
    if seed_readme:
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        cur = conn.execute(
            "SELECT 1 FROM project_settings WHERE key = ?",
            ("global_readme",),
        )
        if cur.fetchone() is None:
            readme = (
                "# 全局 README\n\n"
                "本文件由 Numflow 初始化。后续由 Agent / 用户维护项目级说明。\n"
            )
            conn.execute(
                "INSERT INTO project_settings (key, value_json, updated_at) VALUES (?,?,?)",
                ("global_readme", json.dumps({"text": readme}, ensure_ascii=False), now),
            )
    conn.commit()


def get_setting(conn: sqlite3.Connection, key: str) -> Optional[Any]:
    cur = conn.execute(
        "SELECT value_json FROM project_settings WHERE key = ?",
        (key,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return json.loads(row[0])


def set_setting(conn: sqlite3.Connection, key: str, value: Any) -> None:
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute(
        """
        INSERT INTO project_settings (key, value_json, updated_at)
        VALUES (?,?,?)
        ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json,
            updated_at = excluded.updated_at
        """,
        (key, json.dumps(value, ensure_ascii=False), now),
    )
    conn.commit()


def get_pipeline_state(conn: sqlite3.Connection) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT current_step, completed_steps, updated_at FROM pipeline_state LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        return {"current_step": "", "completed_steps": [], "updated_at": ""}
    return {
        "current_step": row[0] or "",
        "completed_steps": json.loads(row[1] or "[]"),
        "updated_at": row[2] or "",
    }


def set_pipeline_state(
    conn: sqlite3.Connection,
    *,
    current_step: str,
    completed_steps: list,
) -> None:
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    cur = conn.execute("SELECT COUNT(*) FROM pipeline_state")
    if cur.fetchone()[0] == 0:
        conn.execute(
            "INSERT INTO pipeline_state (current_step, completed_steps, updated_at) VALUES (?,?,?)",
            (current_step, json.dumps(completed_steps, ensure_ascii=False), now),
        )
    else:
        conn.execute(
            "UPDATE pipeline_state SET current_step = ?, completed_steps = ?, updated_at = ?",
            (current_step, json.dumps(completed_steps, ensure_ascii=False), now),
        )
    conn.commit()


# ─── Agent Session CRUD ───────────────────────────────────────────────────────

def cleanup_stale_running_sessions(conn: sqlite3.Connection, step_id: str) -> int:
    """Mark any lingering 'running' sessions for this step as 'error' (stale from server restart)."""
    cur = conn.execute(
        "UPDATE _agent_sessions SET status='error', error_text='stale_running_on_new_start' "
        "WHERE step_id=? AND status='running'",
        (step_id,),
    )
    conn.commit()
    return cur.rowcount


def create_agent_session(conn: sqlite3.Connection, step_id: str) -> int:
    """Create a new running session for a step; returns the session id."""
    cleanup_stale_running_sessions(conn, step_id)
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    cur = conn.execute(
        "INSERT INTO _agent_sessions (step_id, status, started_at) VALUES (?,?,?)",
        (step_id, "running", now),
    )
    conn.commit()
    return cur.lastrowid  # type: ignore[return-value]


def update_agent_session(
    conn: sqlite3.Connection,
    session_id: int,
    *,
    status: Optional[str] = None,
    design_text: Optional[str] = None,
    review_text: Optional[str] = None,
    execute_text: Optional[str] = None,
    tools_json: Optional[str] = None,
    error_text: Optional[str] = None,
    finished: bool = False,
) -> None:
    """Partial update of a session record (only non-None fields are updated)."""
    fields = []
    params: list = []
    if status is not None:
        fields.append("status = ?"); params.append(status)
    if design_text is not None:
        fields.append("design_text = ?"); params.append(design_text)
    if review_text is not None:
        fields.append("review_text = ?"); params.append(review_text)
    if execute_text is not None:
        fields.append("execute_text = ?"); params.append(execute_text)
    if tools_json is not None:
        fields.append("tools_json = ?"); params.append(tools_json)
    if error_text is not None:
        fields.append("error_text = ?"); params.append(error_text)
    if finished:
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        fields.append("finished_at = ?"); params.append(now)
    if not fields:
        return
    params.append(session_id)
    conn.execute(f"UPDATE _agent_sessions SET {', '.join(fields)} WHERE id = ?", params)
    conn.commit()


def list_agent_sessions(
    conn: sqlite3.Connection,
    *,
    limit: int = 50,
    step_id: Optional[str] = None,
) -> list:
    """Return all agent sessions (newest first), optionally filtered by step_id."""
    if step_id:
        cur = conn.execute(
            """SELECT id, step_id, status, started_at, finished_at,
                      design_text, review_text, execute_text, tools_json, error_text
               FROM _agent_sessions WHERE step_id = ?
               ORDER BY id DESC LIMIT ?""",
            (step_id, int(limit)),
        )
    else:
        cur = conn.execute(
            """SELECT id, step_id, status, started_at, finished_at,
                      design_text, review_text, execute_text, tools_json, error_text
               FROM _agent_sessions ORDER BY id DESC LIMIT ?""",
            (int(limit),),
        )
    out: list = []
    for row in cur.fetchall():
        try:
            tools = json.loads(row[8] or "[]")
        except json.JSONDecodeError:
            tools = []
        out.append({
            "id": row[0],
            "step_id": row[1],
            "status": row[2],
            "started_at": row[3],
            "finished_at": row[4],
            "design_text": row[5] or "",
            "review_text": row[6] or "",
            "execute_text": row[7] or "",
            "tools": tools,
            "error_text": row[9],
        })
    return out


def get_latest_agent_session(conn: sqlite3.Connection, step_id: str) -> Optional[Dict[str, Any]]:
    """Return the most recent session for a step_id, or None if none exists."""
    cur = conn.execute(
        """SELECT id, step_id, status, started_at, finished_at,
                  design_text, review_text, execute_text, tools_json, error_text
           FROM _agent_sessions WHERE step_id = ? ORDER BY id DESC LIMIT 1""",
        (step_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    tools = []
    try:
        tools = json.loads(row[8] or "[]")
    except json.JSONDecodeError:
        pass
    return {
        "id": row[0],
        "step_id": row[1],
        "status": row[2],
        "started_at": row[3],
        "finished_at": row[4],
        "design_text": row[5] or "",
        "review_text": row[6] or "",
        "execute_text": row[7] or "",
        "tools": tools,
        "error_text": row[9],
    }
