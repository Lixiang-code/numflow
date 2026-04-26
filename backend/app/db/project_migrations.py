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
    _add_column_if_missing(conn, "_agent_sessions", "user_message", "TEXT NOT NULL DEFAULT ''")
    _add_column_if_missing(conn, "_agent_sessions", "model_used", "TEXT NOT NULL DEFAULT ''")

    # 第二轮优化：常量标签系统（每个常量必须 ≥1 标签；标签可挂父系统名）
    _add_column_if_missing(conn, "_constants", "tags", "TEXT NOT NULL DEFAULT '[]'")
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

    # 第三轮优化：表目录管理 / Matrix 表 / Calculator 注册
    _add_column_if_missing(conn, "_table_registry", "directory", "TEXT NOT NULL DEFAULT ''")
    _add_column_if_missing(conn, "_table_registry", "matrix_meta_json", "TEXT NOT NULL DEFAULT ''")

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
            created_at TEXT NOT NULL,
            PRIMARY KEY (owner_step, target_step, key)
        )
        """
    )

    conn.commit()
