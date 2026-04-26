"""Matrix（行/列双向语义）表实现。

第3轮优化新增：让 AI 创建 "行=玩法、列=属性/资源" 的分配表。
存储仍走 SQLite 长表（row_axis_value, col_axis_value, level, value, note），
但前端展示与 AI 读写都按宽表（rows × cols）做。

每张 matrix 表都会自动创建一个 calculator，让 AI 用 fun(level, row, col) 取值。
"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, List, Optional, Sequence

from app.util.identifiers import assert_english_ident
from app.services.calculator_ops import register_calculator


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


_KIND_META: Dict[str, Dict[str, str]] = {
    "matrix_attr": {
        "row_axis": "gameplay",   # 行=玩法（含子系统）
        "col_axis": "attr",       # 列=属性
        "value_kind": "ratio",    # 值=投放比例（小数 0.4 = 40%）
    },
    "matrix_resource": {
        "row_axis": "gameplay",
        "col_axis": "res_id",
        "value_kind": "ratio",
    },
}


def create_matrix_table(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    display_name: str,
    kind: str,
    rows: Sequence[Dict[str, str]],          # [{key, display_name, brief}]
    cols: Sequence[Dict[str, str]],          # [{key, display_name, brief}]
    levels: Optional[Sequence[int]] = None,  # 若为空表示无 level 维（单个值）
    directory: str = "",
    readme: str = "",
    purpose: str = "",
    value_dtype: str = "float",              # float / percent / int
    value_format: str = "0.00%",
    register_calc: bool = True,
) -> Dict[str, Any]:
    """创建 matrix 表。"""
    if kind not in _KIND_META:
        raise ValueError(f"未知 matrix kind={kind}（允许：{list(_KIND_META)}）")
    t = assert_english_ident(table_name, field="表名")
    cur = conn.execute("SELECT 1 FROM _table_registry WHERE table_name = ?", (t,))
    if cur.fetchone():
        raise ValueError(f"表 {t!r} 已存在")

    meta = _KIND_META[kind]
    row_axis_name = meta["row_axis"]
    col_axis_name = meta["col_axis"]

    # 物理表：长表
    ddl = f'''CREATE TABLE "{t}" (
        row_id TEXT PRIMARY KEY,
        {row_axis_name} TEXT NOT NULL,
        {col_axis_name} TEXT NOT NULL,
        level INTEGER,
        value REAL,
        note TEXT
    )'''
    conn.execute(ddl)

    # 唯一索引保证一个 (row, col, level) 只一条
    conn.execute(
        f'CREATE UNIQUE INDEX "{t}__rcl" ON "{t}" ({row_axis_name}, {col_axis_name}, level)'
    )

    matrix_meta = {
        "kind": kind,
        "row_axis": row_axis_name,
        "col_axis": col_axis_name,
        "value_kind": meta["value_kind"],
        "value_dtype": value_dtype,
        "value_format": value_format,
        "rows": [dict(r) for r in rows],
        "cols": [dict(c) for c in cols],
        "levels": list(levels) if levels else [],
    }

    schema_payload = {
        "columns": [
            {"name": "row_id", "sql_type": "TEXT", "display_name": "ID", "dtype": "id", "number_format": ""},
            {"name": row_axis_name, "sql_type": "TEXT", "display_name": "行(玩法)", "dtype": "ref", "number_format": ""},
            {"name": col_axis_name, "sql_type": "TEXT", "display_name": "列", "dtype": "ref", "number_format": ""},
            {"name": "level", "sql_type": "INTEGER", "display_name": "等级", "dtype": "int", "number_format": "0"},
            {"name": "value", "sql_type": "REAL", "display_name": "值", "dtype": value_dtype, "number_format": value_format},
            {"name": "note", "sql_type": "TEXT", "display_name": "备注", "dtype": "str", "number_format": "@"},
        ],
        "display_name": display_name,
    }

    conn.execute(
        """
        INSERT INTO _table_registry
            (table_name, layer, purpose, readme, schema_json, validation_status, directory, matrix_meta_json)
        VALUES (?,?,?,?,?, 'unknown', ?, ?)
        """,
        (
            t,
            "matrix",
            purpose,
            readme,
            json.dumps(schema_payload, ensure_ascii=False),
            directory or "",
            json.dumps(matrix_meta, ensure_ascii=False),
        ),
    )
    conn.commit()

    calc_name = ""
    if register_calc:
        # 默认 calculator 名 = <table>_lookup
        calc_name = f"{t}_lookup"
        try:
            register_calculator(
                conn,
                name=calc_name,
                kind=kind,
                table_name=t,
                axes=[
                    {"name": "level", "source": "level"},
                    {"name": row_axis_name, "source": row_axis_name},
                    {"name": col_axis_name, "source": col_axis_name},
                ],
                value_column="value",
                brief=(
                    f"按 (level, {row_axis_name}, {col_axis_name}) 查询 {display_name} 的投放比例。"
                    "若 level 维为空表示该 matrix 不分等级。"
                ),
            )
        except Exception:  # noqa: BLE001
            calc_name = ""

    return {
        "ok": True,
        "table_name": t,
        "display_name": display_name,
        "matrix_meta": matrix_meta,
        "calculator": calc_name,
        "directory": directory or "",
    }


def write_matrix_cells(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    cells: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """批量写入 matrix 单元格。

    每项: {row, col, level (optional), value, note (optional)}
    """
    t = table_name
    cur = conn.execute(
        "SELECT layer, matrix_meta_json FROM _table_registry WHERE table_name = ?", (t,)
    )
    row = cur.fetchone()
    if not row or row[0] != "matrix":
        raise ValueError(f"{t} 不是 matrix 表")
    meta = json.loads(row[1] or "{}") or {}
    row_axis = meta.get("row_axis") or "row_key"
    col_axis = meta.get("col_axis") or "col_key"

    written = 0
    for c in cells:
        r = str(c.get("row") or c.get(row_axis) or "").strip()
        co = str(c.get("col") or c.get(col_axis) or "").strip()
        if not r or not co:
            continue
        lv = c.get("level")
        lv_int: Optional[int] = int(lv) if lv is not None and str(lv) != "" else None
        v = c.get("value")
        note = c.get("note") or ""
        rid = f"{r}__{co}__{lv_int if lv_int is not None else 'na'}"
        conn.execute(
            f'''INSERT INTO "{t}" (row_id, {row_axis}, {col_axis}, level, value, note)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(row_id) DO UPDATE SET
                    value = excluded.value,
                    note = excluded.note''',
            (rid, r, co, lv_int, v, note),
        )
        written += 1
    conn.commit()
    return {"ok": True, "written": written, "table_name": t}


def read_matrix(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    level: Optional[int] = None,
    rows: Optional[Sequence[str]] = None,
    cols: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """以宽表形式读取 matrix。

    返回：{ "rows": [...], "cols": [...], "levels": [...], "data": {row: {col: {level: value}}} }
    """
    cur = conn.execute(
        "SELECT matrix_meta_json FROM _table_registry WHERE table_name = ?", (table_name,)
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"未知表 {table_name}")
    meta = json.loads(row[0] or "{}") or {}
    row_axis = meta.get("row_axis") or "row_key"
    col_axis = meta.get("col_axis") or "col_key"

    where: List[str] = []
    params: List[Any] = []
    if level is not None:
        where.append("level = ?"); params.append(int(level))
    if rows:
        placeholders = ",".join("?" * len(rows))
        where.append(f"{row_axis} IN ({placeholders})")
        params.extend(rows)
    if cols:
        placeholders = ",".join("?" * len(cols))
        where.append(f"{col_axis} IN ({placeholders})")
        params.extend(cols)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f'SELECT {row_axis}, {col_axis}, level, value, note FROM "{table_name}" {where_sql}'
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for r in conn.execute(sql, params):
        rk, ck, lv, val, note = r
        out.setdefault(rk, {}).setdefault(ck, {})[str(lv) if lv is not None else "_"] = {
            "value": val, "note": note,
        }
    return {
        "ok": True,
        "table_name": table_name,
        "row_axis": row_axis,
        "col_axis": col_axis,
        "rows": [r["key"] for r in (meta.get("rows") or [])],
        "cols": [c["key"] for c in (meta.get("cols") or [])],
        "levels": meta.get("levels") or [],
        "data": out,
    }


def list_matrix_tables(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    cur = conn.execute(
        "SELECT table_name, directory, matrix_meta_json, schema_json FROM _table_registry WHERE layer='matrix'"
    )
    out: List[Dict[str, Any]] = []
    for r in cur.fetchall():
        meta = json.loads(r[2] or "{}") or {}
        sch = json.loads(r[3] or "{}") or {}
        out.append({
            "table_name": r[0],
            "directory": r[1] or "",
            "display_name": sch.get("display_name", ""),
            "kind": meta.get("kind"),
            "row_count": len(meta.get("rows") or []),
            "col_count": len(meta.get("cols") or []),
            "level_count": len(meta.get("levels") or []),
        })
    return out
