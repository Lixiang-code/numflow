"""项目库打开时执行的轻量迁移（加列、补表）。"""

from __future__ import annotations

import sqlite3


def _add_column_if_missing(conn: sqlite3.Connection, table: str, col: str, col_def: str) -> None:
    """幂等加列：列已存在则跳过（防并发 TOCTOU）。"""
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}")
    except sqlite3.OperationalError as e:
        if "duplicate column" not in str(e):
            raise


def ensure_project_migrations(conn: sqlite3.Connection) -> None:
    _add_column_if_missing(conn, "_table_registry", "validation_rules_json", "TEXT")
    _add_column_if_missing(conn, "_snapshots", "payload_json", "TEXT")
    _add_column_if_missing(conn, "_formula_registry", "formula_type", "TEXT NOT NULL DEFAULT 'sql'")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _validation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT,
            created_at TEXT NOT NULL,
            result_json TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _matrix_formula_registry (
            table_name TEXT NOT NULL,
            row_key TEXT NOT NULL,
            col_key TEXT NOT NULL,
            formula TEXT NOT NULL,
            formula_type TEXT NOT NULL DEFAULT 'row',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (table_name, row_key, col_key)
        )
        """
    )

    # 优化文档：术语映射表 + 常数注册表（中英名词混淆 / 常数裸塞修复）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _glossary (
            term_en TEXT PRIMARY KEY,
            term_zh TEXT NOT NULL,
            kind TEXT NOT NULL DEFAULT 'noun',
            brief TEXT,
            scope_table TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _glossary_usage (
            term_en TEXT NOT NULL,
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (term_en, table_name, column_name)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _constants (
            name_en TEXT PRIMARY KEY,
            name_zh TEXT NOT NULL DEFAULT '',
            value_json TEXT NOT NULL,
            brief TEXT,
            scope_table TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    # column_meta 增加 display_lang（@cn=/@en=/@= 渲染模式）
    _add_column_if_missing(conn, "_table_registry", "display_lang_default", "TEXT NOT NULL DEFAULT ''")

    # Agent 全链路对话追踪：存储完整的消息序列（system/user/assistant/tool）
    _add_column_if_missing(conn, "_agent_sessions", "messages_json", "TEXT NOT NULL DEFAULT '[]'")
    _add_column_if_missing(conn, "_agent_sessions", "events_json", "TEXT NOT NULL DEFAULT '[]'")
    _add_column_if_missing(conn, "_agent_sessions", "user_message", "TEXT NOT NULL DEFAULT ''")
    _add_column_if_missing(conn, "_agent_sessions", "model_used", "TEXT NOT NULL DEFAULT ''")

    # 第二轮优化：常量标签系统（每个常量必须 ≥1 标签；标签可挂父系统名）
    _add_column_if_missing(conn, "_constants", "tags", "TEXT NOT NULL DEFAULT '[]'")
    # 公式常量支持：存储公式字符串，value_json 存计算结果
    _add_column_if_missing(conn, "_constants", "formula", "TEXT")
    # 第三轮优化：常量设计意图（AI 编写意图/限制说明，与 brief 的概念定义分离）
    _add_column_if_missing(conn, "_constants", "design_intent", "TEXT NOT NULL DEFAULT ''")
    # AI 工具反馈状态（open/closed）
    _add_column_if_missing(conn, "_tool_feedback", "status", "TEXT NOT NULL DEFAULT 'open'")
    # AI 工具反馈处理备注（拒绝/搁置时必填说明）
    _add_column_if_missing(conn, "_tool_feedback", "resolution_note", "TEXT NOT NULL DEFAULT ''")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _const_tags (
            name TEXT PRIMARY KEY,
            parent TEXT,
            brief TEXT,
            created_at TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _skill_usage_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            skill_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            step_id TEXT NOT NULL DEFAULT '',
            meta_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            FOREIGN KEY(skill_id) REFERENCES _skills(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
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
        )
        """
    )

    # 第三轮优化：表目录管理 / Matrix 表 / Calculator 注册
    _add_column_if_missing(conn, "_table_registry", "directory", "TEXT NOT NULL DEFAULT ''")
    _add_column_if_missing(conn, "_table_registry", "matrix_meta_json", "TEXT NOT NULL DEFAULT ''")

    # 第四轮优化：表标签系统（每张表必须 ≥1 标签；用于相关常数筛选）
    _add_column_if_missing(conn, "_table_registry", "tags", "TEXT NOT NULL DEFAULT '[]'")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _calculators (
            name TEXT PRIMARY KEY,
            kind TEXT NOT NULL,                  -- 'matrix_attr' / 'matrix_resource' / 'lookup'
            table_name TEXT NOT NULL,
            axes_json TEXT NOT NULL,             -- 维度声明 [{name, source}]
            value_column TEXT NOT NULL DEFAULT 'value',
            brief TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    # 子系统暴露参数（父系统 → 子系统设计提示词）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _step_exposed_params (
            owner_step TEXT NOT NULL,            -- 暴露源（父系统步骤 ID）
            target_step TEXT NOT NULL,           -- 接收方（子系统步骤 ID 或 'subsystems:<owner>'）
            key TEXT NOT NULL,
            value_json TEXT NOT NULL,
            brief TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',  -- pending / acknowledged / acted_on
            read_at TEXT,                        -- 首次被 list_exposed_params 读取的时间
            created_at TEXT NOT NULL,
            PRIMARY KEY (owner_step, target_step, key)
        )
        """
    )
    # 迁移：为旧库的 _step_exposed_params 添加 status / read_at 列
    try:
        conn.execute("ALTER TABLE _step_exposed_params ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'")
    except Exception:  # noqa: BLE001
        pass
    try:
        conn.execute("ALTER TABLE _step_exposed_params ADD COLUMN read_at TEXT")
    except Exception:  # noqa: BLE001
        pass

    # 玩法表二次修订请求队列
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _table_revision_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_id TEXT NOT NULL,              -- 目标玩法表 table_id
            reason TEXT NOT NULL DEFAULT '',     -- 修订原因
            requested_by_step TEXT NOT NULL DEFAULT '',  -- 发起步骤 ID
            status TEXT NOT NULL DEFAULT 'pending',      -- pending / in_progress / done
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    try:
        from app.services.skill_library import ensure_default_skills
        ensure_default_skills(conn)
    except Exception:  # noqa: BLE001
        pass

    # 玩法规划：玩法表注册清单
    conn.execute(
        """
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
        )
        """
    )

    # SKILL库迁移：gameplay_landing_tables* → gameplay_table*（动态流水线重构）
    try:
        conn.execute(
            "UPDATE _skills SET step_id = REPLACE(step_id, 'gameplay_landing_tables', 'gameplay_table') "
            "WHERE step_id LIKE 'gameplay_landing_tables%'"
        )
    except Exception:  # noqa: BLE001
        pass

    # 状态迁移：需修订 → 待修订（语义更准确）
    try:
        conn.execute(
            "UPDATE _gameplay_table_registry SET status='待修订' WHERE status='需修订'"
        )
    except Exception:  # noqa: BLE001
        pass

    # 为旧库补 started_at，并为存量「进行中」记录回填更新时间，避免永久孤儿状态
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(_gameplay_table_registry)")}
        if "started_at" not in cols:
            conn.execute("ALTER TABLE _gameplay_table_registry ADD COLUMN started_at TEXT")
        conn.execute(
            """
            UPDATE _gameplay_table_registry
            SET started_at = updated_at
            WHERE status='进行中' AND COALESCE(started_at, '') = ''
            """
        )
    except Exception:  # noqa: BLE001
        pass

    # Agent 失败恢复：进度追踪字段
    _add_column_if_missing(conn, "_agent_sessions", "current_phase", "TEXT NOT NULL DEFAULT ''")
    _add_column_if_missing(conn, "_agent_sessions", "completed_phases", "TEXT NOT NULL DEFAULT '[]'")
    _add_column_if_missing(conn, "_agent_sessions", "gather_context_json", "TEXT NOT NULL DEFAULT ''")

    conn.commit()
