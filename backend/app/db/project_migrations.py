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

    # 表分类系统：配置表/计算表/混合表 + 混合表逐列标注
    _add_column_if_missing(conn, "_table_registry", "table_kind", "TEXT NOT NULL DEFAULT ''")
    _add_column_if_missing(conn, "_table_registry", "column_kinds_json", "TEXT NOT NULL DEFAULT '{}'")

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

    # 性能优化（A1）：高频元数据索引 + 性能日志表 + 业务表 lookup 复合索引
    _ensure_perf_indexes(conn)
    _ensure_perf_log_table(conn)
    try:
        ensure_calculator_indexes(conn)
    except Exception:  # noqa: BLE001
        # 索引补建不应阻断启动；详细错误由调用方按需排查
        pass

    conn.commit()


# ───────────────────────── 性能优化：索引与日志 ─────────────────────────

# 高频元数据/系统表索引：仅在缺失时创建。
_PERF_META_INDEXES: tuple = (
    ("idx_dep_graph_to", "_dependency_graph", "(to_table, to_column)"),
    ("idx_dep_graph_from", "_dependency_graph", "(from_table, from_column)"),
    ("idx_formula_registry_tc", "_formula_registry", "(table_name, column_name)"),
    ("idx_cell_provenance_tc", "_cell_provenance", "(table_name, column_name)"),
    ("idx_calculators_table", "_calculators", "(table_name)"),
    ("idx_constants_name", "_constants", "(name_en)"),
)


def _ensure_perf_indexes(conn) -> None:
    for idx_name, table, cols in _PERF_META_INDEXES:
        try:
            conn.execute(
                f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table}" {cols}'
            )
        except Exception:  # noqa: BLE001
            # 任一目标表缺失时跳过（旧库迁移容错）
            continue


def _ensure_perf_log_table(conn) -> None:
    """性能计时日志表：记录关键计算入口耗时。"""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _perf_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            op TEXT NOT NULL,
            table_name TEXT NOT NULL DEFAULT '',
            column_name TEXT NOT NULL DEFAULT '',
            n_rows INTEGER NOT NULL DEFAULT 0,
            elapsed_ms REAL NOT NULL DEFAULT 0,
            extra_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        )
        """
    )
    try:
        conn.execute(
            'CREATE INDEX IF NOT EXISTS "idx_perf_log_op_time" ON "_perf_log" (op, created_at)'
        )
    except Exception:  # noqa: BLE001
        pass


def ensure_calculator_indexes(conn) -> int:
    """根据 _calculators.axes_json 给业务表自动建复合索引。

    每个 calculator 至少建一个索引：(axis.source..., level[, grain])。
    索引名采用 idx_calc__<short_hash> 防止超长。
    """
    import hashlib
    import json as _json

    cur = conn.execute("SELECT name, table_name, axes_json FROM _calculators")
    rows = cur.fetchall()
    created = 0
    for r in rows:
        name = r[0] if not isinstance(r, dict) else r["name"]
        table_name = r[1] if not isinstance(r, dict) else r["table_name"]
        axes_json = r[2] if not isinstance(r, dict) else r["axes_json"]
        if not table_name:
            continue
        try:
            axes = _json.loads(axes_json or "[]")
        except Exception:  # noqa: BLE001
            continue
        # 检查目标表存在且非视图
        try:
            exist = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
                (table_name,),
            ).fetchone()
        except Exception:  # noqa: BLE001
            exist = None
        if not exist:
            continue
        try:
            cols_in_table = {row[1] for row in conn.execute(f'PRAGMA table_info("{table_name}")')}
        except Exception:  # noqa: BLE001
            continue
        # 收集 source 列；level/grain 末尾追加
        sources: list = []
        level_col = ""
        grain_col = ""
        for a in axes:
            if not isinstance(a, dict):
                continue
            nm = str(a.get("name") or "").strip()
            src = str(a.get("source") or "").strip()
            if nm == "level":
                if src and src in cols_in_table:
                    level_col = src
                elif "level" in cols_in_table:
                    level_col = "level"
                continue
            if nm == "grain":
                if src and src in cols_in_table:
                    grain_col = src
                elif "grain" in cols_in_table:
                    grain_col = "grain"
                continue
            if not src or src not in cols_in_table:
                continue
            if src not in sources:
                sources.append(src)
        order: list = list(sources)
        if level_col and level_col not in order:
            order.append(level_col)
        if grain_col and grain_col not in order:
            order.append(grain_col)
        if not order:
            continue
        sig = f"{table_name}|{','.join(order)}"
        short = hashlib.sha1(sig.encode("utf-8")).hexdigest()[:10]
        idx_name = f"idx_calc__{short}"
        col_sql = ", ".join(f'"{c}"' for c in order)
        try:
            conn.execute(
                f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table_name}" ({col_sql})'
            )
            created += 1
        except Exception:  # noqa: BLE001
            continue
    return created
