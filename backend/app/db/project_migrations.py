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
    conn.commit()
