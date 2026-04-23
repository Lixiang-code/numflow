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
