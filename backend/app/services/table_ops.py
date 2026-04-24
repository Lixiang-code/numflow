"""动态建表（供 /data 与 Agent 工具复用）。"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Tuple, Union

from app.util.identifiers import assert_table_or_column as assert_ident


def create_dynamic_table(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    columns: List[Tuple[str, str]],
    readme: str = "",
    purpose: str = "",
) -> Dict[str, Any]:
    """columns: (列名, TEXT|REAL|INTEGER)，不含 row_id。"""
    t = assert_ident(table_name)
    cur = conn.execute("SELECT 1 FROM _table_registry WHERE table_name = ?", (t,))
    if cur.fetchone():
        raise ValueError("表已存在")
    cols_sql = ["row_id TEXT PRIMARY KEY"]
    schema_cols: List[Dict[str, str]] = [{"name": "row_id", "sql_type": "TEXT"}]
    for name, sql_type in columns:
        st = str(sql_type).upper()
        if st not in ("TEXT", "REAL", "INTEGER"):
            raise ValueError(f"非法列类型: {sql_type}")
        cn = assert_ident(name)
        cols_sql.append(f'"{cn}" {st} NULL')
        schema_cols.append({"name": cn, "sql_type": st})
    ddl = f'CREATE TABLE "{t}" ({", ".join(cols_sql)})'
    conn.execute(ddl)
    conn.execute(
        """
        INSERT INTO _table_registry (table_name, layer, purpose, readme, schema_json, validation_status)
        VALUES (?,?,?,?,?, 'unknown')
        """,
        (t, "dynamic", purpose, readme, json.dumps({"columns": schema_cols}, ensure_ascii=False)),
    )
    conn.commit()
    return {"ok": True, "table_name": t}


def delete_dynamic_table(conn: sqlite3.Connection, *, table_name: str, confirm: Union[bool, str, int]) -> Dict[str, Any]:
    """删除动态表及元数据；若有公式依赖本表列则拒绝。"""
    if confirm not in (True, "true", "True", 1, "1"):
        raise ValueError("confirm 须显式为 true")
    t = assert_ident(table_name)
    cur = conn.execute("SELECT 1 FROM _table_registry WHERE table_name = ?", (t,))
    if not cur.fetchone():
        raise ValueError(f"未知表 {t}")
    cur = conn.execute(
        """
        SELECT from_table, from_column, to_table, to_column
        FROM _dependency_graph WHERE to_table = ?
        """,
        (t,),
    )
    blockers = [dict(r) for r in cur.fetchall()]
    if blockers:
        return {
            "ok": False,
            "error": "存在公式依赖本表列，拒绝删除",
            "blockers": blockers,
        }
    conn.execute(f'DROP TABLE IF EXISTS "{t}"')
    conn.execute("DELETE FROM _table_registry WHERE table_name = ?", (t,))
    conn.execute("DELETE FROM _dependency_graph WHERE from_table = ? OR to_table = ?", (t, t))
    conn.execute("DELETE FROM _formula_registry WHERE table_name = ?", (t,))
    conn.execute("DELETE FROM _cell_provenance WHERE table_name = ?", (t,))
    conn.commit()
    return {"ok": True, "deleted_table": t}
