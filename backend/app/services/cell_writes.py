"""单元格批量写入（供 /data 与 Agent 工具复用）。"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Literal

import sqlite3

from app.util.identifiers import assert_table_or_column as assert_col_or_table


def apply_write_cells(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    updates: List[Dict[str, Any]],
    source_tag: Literal["ai_generated", "algorithm_derived", "formula_computed"],
) -> Dict[str, Any]:
    t = assert_col_or_table(table_name)
    cur = conn.execute("SELECT 1 FROM _table_registry WHERE table_name = ?", (t,))
    if not cur.fetchone():
        raise ValueError(f"未知表 {t}")

    # 缓存当前列名集合，避免每行都 PRAGMA
    existing_cols: set = {row[1] for row in conn.execute(f'PRAGMA table_info("{t}")')}
    added_cols: List[str] = []

    def _ensure_column(col: str, sample_val: Any) -> None:
        if col in existing_cols:
            return
        sql_type = "REAL" if isinstance(sample_val, (int, float)) else "TEXT"
        conn.execute(f'ALTER TABLE "{t}" ADD COLUMN "{col}" {sql_type} NULL')
        existing_cols.add(col)
        added_cols.append(col)
        # 同步更新 _table_registry schema_json
        row3 = conn.execute("SELECT schema_json FROM _table_registry WHERE table_name = ?", (t,)).fetchone()
        if row3:
            import json as _json
            schema = _json.loads(row3[0] or '{"columns":[]}')
            schema.setdefault("columns", []).append({"name": col, "sql_type": sql_type})
            conn.execute(
                "UPDATE _table_registry SET schema_json = ? WHERE table_name = ?",
                (_json.dumps(schema, ensure_ascii=False), t),
            )

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    skipped: List[Dict[str, str]] = []
    applied = 0
    for u in updates:
        row_id = str(u.get("row_id", ""))
        col = assert_col_or_table(str(u.get("column", "")))
        val = u.get("value")
        prow = conn.execute(
            "SELECT source_tag FROM _cell_provenance WHERE table_name = ? AND row_id = ? AND column_name = ?",
            (t, row_id, col),
        ).fetchone()
        if prow and prow["source_tag"] == "user_manual":
            skipped.append({"row_id": row_id, "column": col, "reason": "protected"})
            continue
        # 确保列存在（自动补列）
        _ensure_column(col, val)
        if conn.execute(f'SELECT 1 FROM "{t}" WHERE row_id = ?', (row_id,)).fetchone():
            conn.execute(f'UPDATE "{t}" SET "{col}" = ? WHERE row_id = ?', (val, row_id))
        else:
            conn.execute(f'INSERT INTO "{t}" (row_id, "{col}") VALUES (?,?)', (row_id, val))
        conn.execute(
            """
            INSERT INTO _cell_provenance (table_name, row_id, column_name, source_tag, updated_at)
            VALUES (?,?,?,?,?)
            ON CONFLICT(table_name, row_id, column_name)
            DO UPDATE SET source_tag = excluded.source_tag, updated_at = excluded.updated_at
            """,
            (t, row_id, col, source_tag, now),
        )
        applied += 1
    conn.commit()
    result: Dict[str, Any] = {"applied": applied, "skipped": skipped}
    if added_cols:
        result["auto_added_columns"] = added_cols
    return result
