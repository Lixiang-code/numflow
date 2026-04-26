"""动态建表（供 /data 与 Agent 工具复用）。"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Tuple, Union

from app.util.identifiers import assert_table_or_column as assert_ident

# 系统保留表名，Agent 不得创建
_SYSTEM_TABLES = frozenset({
    "project_settings",
    "_table_registry",
    "_dependency_graph",
    "_formula_registry",
    "_snapshots",
    "pipeline_state",
    "_cell_provenance",
})


def create_dynamic_table(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    columns: List[Tuple[str, str]],
    readme: str = "",
    purpose: str = "",
    display_name: str = "",
    column_meta: Union[List[Dict[str, str]], None] = None,
) -> Dict[str, Any]:
    """columns: (列名, TEXT|REAL|INTEGER)，不含 row_id。

    可选参数：
    - display_name: 表的中文显示名（如「基础属性表」）
    - column_meta: [{name, display_name, dtype}]，dtype 是语义类型（int/float/str/percent/...）
                   按 name 与 columns 关联；列出现在 columns 但 column_meta 缺失则用空串。
    """
    t = assert_ident(table_name)
    if t in _SYSTEM_TABLES:
        raise ValueError(f"表名 {t!r} 是系统保留名，不允许通过工具创建")
    cur = conn.execute("SELECT 1 FROM _table_registry WHERE table_name = ?", (t,))
    if cur.fetchone():
        raise ValueError(f"表 {t!r} 已存在（已注册为动态表）")
    cur2 = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,))
    if cur2.fetchone():
        raise ValueError(f"表 {t!r} 已存在于数据库中")
    meta_map: Dict[str, Dict[str, str]] = {}
    if column_meta:
        for it in column_meta:
            nm = str(it.get("name") or "").strip()
            if nm:
                meta_map[nm] = {
                    "display_name": str(it.get("display_name") or ""),
                    "dtype": str(it.get("dtype") or ""),
                    "number_format": str(it.get("number_format") or ""),
                    "display_lang": str(it.get("display_lang") or ""),
                }
    cols_sql = ["row_id TEXT PRIMARY KEY"]
    schema_cols: List[Dict[str, str]] = [{
        "name": "row_id", "sql_type": "TEXT",
        "display_name": "ID", "dtype": "id", "number_format": "",
        "display_lang": "",
    }]
    for name, sql_type in columns:
        st = str(sql_type).upper()
        if st not in ("TEXT", "REAL", "INTEGER"):
            raise ValueError(f"非法列类型: {sql_type}")
        cn = assert_ident(name)
        cols_sql.append(f'"{cn}" {st} NULL')
        m = meta_map.get(cn, {})
        schema_cols.append({
            "name": cn, "sql_type": st,
            "display_name": m.get("display_name", ""),
            "dtype": m.get("dtype", ""),
            "number_format": m.get("number_format", ""),
            "display_lang": m.get("display_lang", ""),
        })
    ddl = f'CREATE TABLE "{t}" ({", ".join(cols_sql)})'
    conn.execute(ddl)
    schema_payload = {
        "columns": schema_cols,
        "display_name": display_name or "",
    }
    conn.execute(
        """
        INSERT INTO _table_registry (table_name, layer, purpose, readme, schema_json, validation_status)
        VALUES (?,?,?,?,?, 'unknown')
        """,
        (t, "dynamic", purpose, readme, json.dumps(schema_payload, ensure_ascii=False)),
    )
    conn.commit()
    return {"ok": True, "table_name": t, "display_name": display_name}


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
        FROM _dependency_graph
        WHERE to_table = ?
          AND from_table != ?
        """,
        (t, t),
    )
    blockers = [dict(r) for r in cur.fetchall()]
    if blockers:
        return {
            "ok": False,
            "error": "存在其他表的公式依赖本表列，拒绝删除",
            "blockers": blockers,
        }
    conn.execute(f'DROP TABLE IF EXISTS "{t}"')
    conn.execute("DELETE FROM _table_registry WHERE table_name = ?", (t,))
    conn.execute("DELETE FROM _dependency_graph WHERE from_table = ? OR to_table = ?", (t, t))
    conn.execute("DELETE FROM _formula_registry WHERE table_name = ?", (t,))
    conn.execute("DELETE FROM _cell_provenance WHERE table_name = ?", (t,))
    conn.commit()
    return {"ok": True, "deleted_table": t}
