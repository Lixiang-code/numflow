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

        CREATE TABLE IF NOT EXISTS _matrix_formula_registry (
            table_name TEXT NOT NULL,
            row_key TEXT NOT NULL,
            col_key TEXT NOT NULL,
            formula TEXT NOT NULL,
            formula_type TEXT NOT NULL DEFAULT 'row',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (table_name, row_key, col_key)
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
            formula TEXT,
            brief TEXT,
            design_intent TEXT NOT NULL DEFAULT '',
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
            events_json TEXT NOT NULL DEFAULT '[]',
            error_text TEXT
        );

        CREATE TABLE IF NOT EXISTS _const_tags (
            name TEXT PRIMARY KEY,
            parent TEXT,
            brief TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS _skills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            step_id TEXT NOT NULL DEFAULT '',
            summary TEXT NOT NULL DEFAULT '',
            description TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT 'user',
            template_key TEXT UNIQUE,
            default_exposed INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            display_order INTEGER NOT NULL DEFAULT 9999,
            usage_count INTEGER NOT NULL DEFAULT 0,
            generated_file_path TEXT NOT NULL DEFAULT '',
            generated_content TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS _skill_modules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            skill_id INTEGER NOT NULL,
            module_key TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL DEFAULT '',
            required INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(skill_id, module_key),
            FOREIGN KEY(skill_id) REFERENCES _skills(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS _skill_usage_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            skill_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            step_id TEXT NOT NULL DEFAULT '',
            meta_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            FOREIGN KEY(skill_id) REFERENCES _skills(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS _prompt_overrides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            prompt_key TEXT NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            summary TEXT NOT NULL DEFAULT '',
            description TEXT NOT NULL DEFAULT '',
            reference_note TEXT NOT NULL DEFAULT '',
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(category, prompt_key)
        );

        CREATE TABLE IF NOT EXISTS _prompt_override_modules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prompt_override_id INTEGER NOT NULL,
            module_key TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL DEFAULT '',
            required INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(prompt_override_id, module_key),
            FOREIGN KEY(prompt_override_id) REFERENCES _prompt_overrides(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS _gameplay_table_registry (
            table_id TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            readme TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '未开始',
            started_at TEXT,
            order_num INTEGER NOT NULL DEFAULT 0,
            dependencies TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        -- AI 工具反馈：记录 AI 在使用工具过程中发现的问题和需求
        -- 反馈文件位置：各项目 project.db 的 _tool_feedback 表 + 可用 sqlite3 命令行查看
        CREATE TABLE IF NOT EXISTS _tool_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_slug TEXT NOT NULL DEFAULT '',
            pipeline_step TEXT NOT NULL DEFAULT '',
            category TEXT NOT NULL DEFAULT 'bug',
            title TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            tool_names TEXT NOT NULL DEFAULT '[]',
            context TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        );
        """
    )
    # 增量迁移：为旧库补 _constants.tags（JSON 数组字符串）
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(_constants)")}
        if "tags" not in cols:
            conn.execute("ALTER TABLE _constants ADD COLUMN tags TEXT NOT NULL DEFAULT '[]'")
    except Exception:  # noqa: BLE001
        pass
    # 增量迁移：为旧库补 _constants.design_intent（设计意图）
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(_constants)")}
        if "design_intent" not in cols:
            conn.execute("ALTER TABLE _constants ADD COLUMN design_intent TEXT NOT NULL DEFAULT ''")
    except Exception:  # noqa: BLE001
        pass
    # 增量迁移：为旧库补 _table_registry.tags（JSON 数组字符串）
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(_table_registry)")}
        if "tags" not in cols:
            conn.execute("ALTER TABLE _table_registry ADD COLUMN tags TEXT NOT NULL DEFAULT '[]'")
    except Exception:  # noqa: BLE001
        pass
    try:
        from app.services.skill_library import ensure_default_skills
        ensure_default_skills(conn)
    except Exception:  # noqa: BLE001
        pass
    conn.commit()
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
    try:
        from app.services.skill_library import ensure_default_skills
        ensure_default_skills(conn)
    except Exception:  # noqa: BLE001
        pass
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
    events_json: Optional[str] = None,
    error_text: Optional[str] = None,
    messages_json: Optional[str] = None,
    user_message: Optional[str] = None,
    model_used: Optional[str] = None,
    current_phase: Optional[str] = None,
    completed_phases: Optional[str] = None,
    gather_context_json: Optional[str] = None,
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
    if events_json is not None:
        fields.append("events_json = ?"); params.append(events_json)
    if error_text is not None:
        fields.append("error_text = ?"); params.append(error_text)
    if messages_json is not None:
        try:
            fields.append("messages_json = ?"); params.append(messages_json)
        except Exception:  # noqa: BLE001
            pass
    if user_message is not None:
        try:
            fields.append("user_message = ?"); params.append(user_message)
        except Exception:  # noqa: BLE001
            pass
    if model_used is not None:
        try:
            fields.append("model_used = ?"); params.append(model_used)
        except Exception:  # noqa: BLE001
            pass
    if current_phase is not None:
        try:
            fields.append("current_phase = ?"); params.append(current_phase)
        except Exception:  # noqa: BLE001
            pass
    if completed_phases is not None:
        try:
            fields.append("completed_phases = ?"); params.append(completed_phases)
        except Exception:  # noqa: BLE001
            pass
    if gather_context_json is not None:
        try:
            fields.append("gather_context_json = ?"); params.append(gather_context_json)
        except Exception:  # noqa: BLE001
            pass
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
    cols = (
            "id, step_id, status, started_at, finished_at, "
            "design_text, review_text, execute_text, tools_json, events_json, error_text, "
            "COALESCE(user_message,'') AS user_message, "
            "COALESCE(model_used,'') AS model_used"
        )
    if step_id:
        cur = conn.execute(
            f"SELECT {cols} FROM _agent_sessions WHERE step_id = ? ORDER BY id DESC LIMIT ?",
            (step_id, int(limit)),
        )
    else:
        cur = conn.execute(
            f"SELECT {cols} FROM _agent_sessions ORDER BY id DESC LIMIT ?",
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
            "error_text": row[10],
            "user_message": row[11] or "",
            "model_used": row[12] or "",
        })
    return out


def get_agent_session_messages(conn: sqlite3.Connection, session_id: int) -> Optional[Dict[str, Any]]:
    """Return a single session with its full messages_json trace."""
    try:
        cur = conn.execute(
            """SELECT id, step_id, status, started_at, finished_at,
                      design_text, review_text, execute_text, tools_json, COALESCE(events_json,'[]') AS events_json, error_text,
                      COALESCE(messages_json,'[]') AS messages_json,
                      COALESCE(user_message,'') AS user_message,
                      COALESCE(model_used,'') AS model_used
               FROM _agent_sessions WHERE id = ?""",
            (session_id,),
        )
        row = cur.fetchone()
    except Exception:  # noqa: BLE001
        return None
    if not row:
        return None
    try:
        tools = json.loads(row[8] or "[]")
    except json.JSONDecodeError:
        tools = []
    try:
        events = json.loads(row[9] or "[]")
    except json.JSONDecodeError:
        events = []
    try:
        messages = json.loads(row[11] or "[]")
    except json.JSONDecodeError:
        messages = []
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
        "events": events,
        "error_text": row[10],
        "messages": messages,
        "user_message": row[12] or "",
        "model_used": row[13] or "",
    }


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


def get_resumable_session(conn: sqlite3.Connection, step_id: str) -> Optional[Dict[str, Any]]:
    """获取可恢复的session（失败但有部分完成内容的session）。
    
    返回包含恢复所需上下文的session信息，如果没有可恢复的session则返回None。
    """
    # 查找最近的非成功session（error或running状态）
    cur = conn.execute(
        """SELECT id, step_id, status, started_at, finished_at,
                  design_text, review_text, execute_text, tools_json, error_text,
                  COALESCE(messages_json,'[]') AS messages_json,
                  COALESCE(current_phase,'') AS current_phase,
                  COALESCE(completed_phases,'[]') AS completed_phases,
                  COALESCE(gather_context_json,'') AS gather_context_json,
                  COALESCE(user_message,'') AS user_message
           FROM _agent_sessions 
           WHERE step_id = ? AND status IN ('error', 'running') 
           ORDER BY id DESC LIMIT 1""",
        (step_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    
    session_id = row[0]
    status = row[2]
    design_text = row[5] or ""
    review_text = row[6] or ""
    execute_text = row[7] or ""
    error_text = row[9] or ""
    current_phase = row[11] or ""
    
    # 解析completed_phases
    try:
        completed_phases = json.loads(row[12] or "[]")
    except json.JSONDecodeError:
        completed_phases = []
    
    # 解析messages_json获取对话历史
    try:
        messages = json.loads(row[10] or "[]")
    except json.JSONDecodeError:
        messages = []
    
    # 解析gather_context
    gather_context = []
    if row[13]:
        try:
            gather_context = json.loads(row[13])
        except json.JSONDecodeError:
            pass
    
    # 判断可以从哪个阶段恢复
    # 优先使用 completed_phases（更可靠），其次 current_phase，最后 fallback 到文本推断
    resumable_from = None
    
    if completed_phases:
        # 基于已完成阶段判断下一个需要恢复的阶段
        if "review" in completed_phases:
            # review 已完成，但从 execute 恢复（可能 execute 已部分执行）
            resumable_from = "execute"
        elif "design" in completed_phases:
            resumable_from = "review"
        elif "gather" in completed_phases:
            resumable_from = "design"
    
    # 如果 completed_phases 没有给出明确信息，使用 current_phase
    if not resumable_from and current_phase:
        resumable_from = current_phase
    
    # 最后 fallback：根据已有文本内容推断
    if not resumable_from:
        if execute_text or (messages and any(m.get("role") == "tool" for m in messages)):
            resumable_from = "execute"
        elif review_text:
            resumable_from = "execute"  # review完成，从execute开始
        elif design_text:
            resumable_from = "review"  # design完成，从review开始
        elif gather_context:
            resumable_from = "design"  # gather完成，从design开始
    
    # gather 阶段失败则不需要恢复（重新开始更可靠）
    if resumable_from and resumable_from not in ("design", "review", "execute"):
        return None
    
    if not resumable_from:
        return None
    
    return {
        "session_id": session_id,
        "step_id": step_id,
        "status": status,
        "resumable_from": resumable_from,
        "design_text": design_text,
        "review_text": review_text,
        "execute_text": execute_text,
        "error_text": error_text,
        "gather_context": gather_context,
        "messages": messages,
        "user_message": row[14] or "",
    }
