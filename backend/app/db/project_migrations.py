"""项目库打开时执行的轻量迁移（加列、补表）。"""

from __future__ import annotations

import sqlite3


def _pragma_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {str(r[1]) for r in cur.fetchall()}


def ensure_project_migrations(conn: sqlite3.Connection) -> None:
    cols = _pragma_columns(conn, "_table_registry")
    if "validation_rules_json" not in cols:
        conn.execute("ALTER TABLE _table_registry ADD COLUMN validation_rules_json TEXT")

    sn_cols = _pragma_columns(conn, "_snapshots")
    if "payload_json" not in sn_cols:
        conn.execute("ALTER TABLE _snapshots ADD COLUMN payload_json TEXT")

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
