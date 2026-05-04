"""动态建表（供 /data 与 Agent 工具复用）。

设计要点（第二轮矫正）：
- 表名/列名严格英文 snake_case（`assert_english_ident`）；中文一律走 display_name。
- 建表时按 kind 自动挂载默认校验规则到 `_table_registry.validation_rules_json`。
- 建表时把表名 / 各列名（若有 display_name）自动写入 `_glossary`，让中英对照不再依赖 AI 调用。
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from app.util.identifiers import (
    assert_english_ident,
    assert_table_or_column as assert_ident_loose,
)
from app.services.validation_report import attach_default_rules

# 系统保留表名，Agent 不得创建
_SYSTEM_TABLES = frozenset({
    "project_settings",
    "_table_registry",
    "_dependency_graph",
    "_formula_registry",
    "_snapshots",
    "pipeline_state",
    "_cell_provenance",
    "_glossary",
    "_glossary_usage",
    "_constants",
    "_validation_history",
    "_agent_sessions",
    "_column_meta",
})


_KIND_HINTS: Tuple[Tuple[str, str], ...] = (
    ("_alloc", "alloc"),
    ("allocation", "alloc"),
    ("_attr", "attr"),
    ("attribute", "attr"),
    ("_quant", "quant"),
    ("_landing", "landing"),
    ("_resource", "resource"),
    ("base_attr", "base"),
)


def _infer_kind(table_name: str, explicit: str = "") -> str:
    if explicit:
        return explicit
    n = (table_name or "").lower()
    for hint, kind in _KIND_HINTS:
        if hint in n:
            return kind
    return "unknown"


def _glossary_register_terms(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    display_name: str,
    column_meta: List[Dict[str, str]],
) -> None:
    """将表名/列名 → 中文 display_name 写入 _glossary。已存在的不覆盖中文（避免冲掉手工注册）。"""
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    pairs: List[Tuple[str, str, str, str]] = []  # (term_en, term_zh, kind, scope)
    if display_name:
        pairs.append((table_name, display_name, "noun", ""))
    for col in column_meta or []:
        en = str(col.get("name") or "").strip()
        zh = str(col.get("display_name") or "").strip()
        if en and zh and en != "row_id":
            pairs.append((en, zh, "metric", table_name))
    for term_en, term_zh, kind, scope in pairs:
        try:
            conn.execute(
                """
                INSERT INTO _glossary (term_en, term_zh, kind, brief, scope_table, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(term_en) DO UPDATE SET
                    term_zh = COALESCE(NULLIF(_glossary.term_zh, ''), excluded.term_zh),
                    scope_table = COALESCE(NULLIF(_glossary.scope_table, ''), excluded.scope_table),
                    updated_at = excluded.updated_at
                """,
                (term_en, term_zh, kind, "", scope or None, now, now),
            )
        except sqlite3.OperationalError:
            # _glossary 表可能尚未迁移
            return


def create_dynamic_table(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    columns: List[Tuple[str, str]],
    readme: str = "",
    purpose: str = "",
    display_name: str = "",
    column_meta: Union[List[Dict[str, str]], None] = None,
    kind: str = "",
    directory: str = "",
    tags: Union[List[str], None] = None,
) -> Dict[str, Any]:
    """建表入口。

    严格规则（新建路径）：
    - table_name / columns[].name 必须为英文 snake_case；中文写到 display_name / column_meta[].display_name。
    - 建表后自动：(a) 注册术语对照到 _glossary；(b) 按 kind 挂载默认 validation 规则。
    """
    t = assert_english_ident(table_name, field="表名")
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
        cn = assert_english_ident(name, field="列名")
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
        INSERT INTO _table_registry (table_name, layer, purpose, readme, schema_json, validation_status, directory, tags)
        VALUES (?,?,?,?,?, 'unknown', ?, ?)
        """,
        (t, "dynamic", purpose, readme, json.dumps(schema_payload, ensure_ascii=False), directory or "", json.dumps(tags or [], ensure_ascii=False)),
    )
    conn.commit()

    # 自动术语注册（中英对照）
    _glossary_register_terms(
        conn,
        table_name=t,
        display_name=display_name or "",
        column_meta=list(meta_map_to_list(meta_map)),
    )

    # 自动挂载默认校验规则
    inferred_kind = _infer_kind(t, kind)
    try:
        attach_default_rules(
            conn,
            t,
            kind=inferred_kind,
            schema_columns=schema_cols,
            formula_columns=[],
        )
    except sqlite3.OperationalError:
        pass

    return {
        "ok": True,
        "table_name": t,
        "display_name": display_name,
        "kind": inferred_kind,
        "auto_rules": True,
        "directory": directory or "",
    }


def meta_map_to_list(meta_map: Dict[str, Dict[str, str]]) -> List[Dict[str, str]]:
    out = []
    for name, m in meta_map.items():
        out.append({"name": name, **m})
    return out


def create_3d_table(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    display_name: str,
    dim1: Dict[str, Any],    # {col_name, display_name, keys: [{key, display_name}]}
    dim2: Dict[str, Any],    # {col_name, display_name, keys: [{key, display_name}]}
    cols: List[Dict[str, Any]],   # [{key, display_name, dtype, number_format, formula?}]
    readme: str = "",
    purpose: str = "",
    directory: str = "",
    tags: Union[List[str], None] = None,
) -> Dict[str, Any]:
    """创建三维矩阵表（行有两个维度，列是属性，可为每列注册公式）。

    物理存储为普通动态表（layer='dynamic'），行用 row_id="{dim1_key}_{dim2_key}"，
    并预插所有 (dim1 × dim2) 组合行。
    公式可引用 @dim1_col_name 与 @dim2_col_name（同行语法）。
    matrix_meta_json 中记录 kind='3d_matrix'，供前端高亮维度列。
    """
    t = assert_english_ident(table_name, field="表名")
    if t in _SYSTEM_TABLES:
        raise ValueError(f"表名 {t!r} 是系统保留名")
    cur = conn.execute("SELECT 1 FROM _table_registry WHERE table_name = ?", (t,))
    if cur.fetchone():
        raise ValueError(f"表 {t!r} 已存在")
    cur2 = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,))
    if cur2.fetchone():
        raise ValueError(f"表 {t!r} 已存在于数据库中")

    dim1_col = assert_english_ident(dim1["col_name"], field="dim1 列名")
    dim2_col = assert_english_ident(dim2["col_name"], field="dim2 列名")
    dim1_keys = [str(k["key"]) for k in dim1.get("keys", [])]
    dim2_keys = [str(k["key"]) for k in dim2.get("keys", [])]
    if not dim1_keys:
        raise ValueError("dim1.keys 不能为空")
    if not dim2_keys:
        raise ValueError("dim2.keys 不能为空")

    # 推断 dim1 SQL type（全部为整数则用 INTEGER，否则 TEXT）
    def _all_int(keys: List[str]) -> bool:
        return all(k.isdigit() or (k.startswith("-") and k[1:].isdigit()) for k in keys)
    dim1_sql = "INTEGER" if _all_int(dim1_keys) else "TEXT"
    dim2_sql = "INTEGER" if _all_int(dim2_keys) else "TEXT"

    # 构建 DDL
    attr_defs: List[Tuple[str, str]] = []
    for c in cols:
        cn = assert_english_ident(c["key"], field="attr 列名")
        dtype = str(c.get("dtype") or "float").lower()
        sql_t = "INTEGER" if dtype == "int" else "REAL"
        attr_defs.append((cn, sql_t))

    schema_cols: List[Dict[str, str]] = [
        {"name": "row_id", "sql_type": "TEXT", "display_name": "ID", "dtype": "id", "number_format": "", "display_lang": ""},
        {"name": dim1_col, "sql_type": dim1_sql, "display_name": dim1.get("display_name", dim1_col), "dtype": "int" if dim1_sql == "INTEGER" else "text", "number_format": "", "display_lang": ""},
        {"name": dim2_col, "sql_type": dim2_sql, "display_name": dim2.get("display_name", dim2_col), "dtype": "text", "number_format": "", "display_lang": ""},
    ]
    for c in cols:
        cn = assert_english_ident(c["key"], field="attr 列名")
        dtype = str(c.get("dtype") or "float").lower()
        sql_t = "INTEGER" if dtype == "int" else "REAL"
        schema_cols.append({
            "name": cn,
            "sql_type": sql_t,
            "display_name": str(c.get("display_name") or cn),
            "dtype": dtype,
            "number_format": str(c.get("number_format") or ""),
            "display_lang": "",
        })

    ddl_parts = [
        "row_id TEXT PRIMARY KEY",
        f'"{dim1_col}" {dim1_sql}',
        f'"{dim2_col}" {dim2_sql}',
    ] + [f'"{cn}" {st} NULL' for cn, st in attr_defs]
    conn.execute(f'CREATE TABLE "{t}" ({", ".join(ddl_parts)})')

    # 预插所有 (dim1 × dim2) 行
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    row_count = 0
    for d1 in dim1_keys:
        for d2 in dim2_keys:
            rid = f"{d1}_{d2}"
            d1_val = int(d1) if dim1_sql == "INTEGER" else d1
            d2_val = int(d2) if dim2_sql == "INTEGER" else d2
            conn.execute(
                f'INSERT INTO "{t}" (row_id, "{dim1_col}", "{dim2_col}") VALUES (?,?,?)',
                (rid, d1_val, d2_val),
            )
            row_count += 1

    # 构建 matrix_meta_json（供前端识别 3d_matrix）
    matrix_meta = {
        "kind": "3d_matrix",
        "dim1": {
            "col_name": dim1_col,
            "display_name": dim1.get("display_name", dim1_col),
            "keys": list(dim1.get("keys", [])),
        },
        "dim2": {
            "col_name": dim2_col,
            "display_name": dim2.get("display_name", dim2_col),
            "keys": list(dim2.get("keys", [])),
        },
        "cols": [
            {
                "key": c["key"],
                "display_name": c.get("display_name", c["key"]),
                "dtype": c.get("dtype", "float"),
                "number_format": c.get("number_format", ""),
                "formula": c.get("formula", ""),
            }
            for c in cols
        ],
    }

    schema_payload = {"columns": schema_cols, "display_name": display_name or ""}
    conn.execute(
        """
        INSERT INTO _table_registry
            (table_name, layer, purpose, readme, schema_json, validation_status, directory, matrix_meta_json, tags)
        VALUES (?,?,?,?,?, 'unknown', ?, ?, ?)
        """,
        (
            t, "dynamic", purpose, readme,
            json.dumps(schema_payload, ensure_ascii=False),
            directory or "",
            json.dumps(matrix_meta, ensure_ascii=False),
            json.dumps(tags or [], ensure_ascii=False),
        ),
    )
    conn.commit()

    # 术语注册
    _glossary_register_terms(conn, table_name=t, display_name=display_name or "", column_meta=schema_cols[1:])

    # 注册公式（有 formula 字段的属性列）
    formula_errors: List[str] = []
    from app.services.formula_exec import execute_formula_on_column, register_row_formula  # lazy import
    formula_cols: List[str] = []
    for c in cols:
        formula_str = str(c.get("formula") or "").strip()
        if not formula_str:
            continue
        try:
            reg_result = register_row_formula(conn, t, c["key"], formula_str)
            if reg_result.get("formula_type") != "row":
                formula_cols.append(str(c["key"]))
        except Exception as fe:  # noqa: BLE001
            formula_errors.append(f"{c['key']}: {fe}")

    for col_name in formula_cols:
        try:
            execute_formula_on_column(conn, t, col_name)
        except Exception as fe:  # noqa: BLE001
            formula_errors.append(f"{col_name}: {fe}")

    return {
        "ok": True,
        "table_name": t,
        "display_name": display_name,
        "row_count": row_count,
        "formula_errors": formula_errors,
        "directory": directory or "",
    }


def read_3d_table(conn: sqlite3.Connection, *, table_name: str) -> Dict[str, Any]:
    """读取三维数据表快照，供前端三轴查看器 / 工具切片渲染。"""
    t = assert_ident_loose(table_name)
    cur = conn.execute(
        "SELECT schema_json, matrix_meta_json FROM _table_registry WHERE table_name = ?",
        (t,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"未知表 {t}")
    schema = json.loads(row[0] or "{}") or {}
    matrix_meta = json.loads(row[1] or "{}") or {}
    if matrix_meta.get("kind") != "3d_matrix":
        raise ValueError(f"{t} 不是三维表")

    dim1 = matrix_meta.get("dim1") or {}
    dim2 = matrix_meta.get("dim2") or {}
    cols = matrix_meta.get("cols") or []
    dim1_col = str(dim1.get("col_name") or "")
    dim2_col = str(dim2.get("col_name") or "")
    if not dim1_col or not dim2_col:
        raise ValueError(f"{t} 缺少三维表维度定义")

    data: Dict[str, Dict[str, Dict[str, Any]]] = {}
    cur = conn.execute(f'SELECT * FROM "{t}"')
    col_names = [str(item[0]) for item in (cur.description or [])]
    for rec in cur.fetchall():
        d = dict(rec) if isinstance(rec, sqlite3.Row) else dict(zip(col_names, rec))
        dim1_key = str(d.get(dim1_col))
        dim2_key = str(d.get(dim2_col))
        values: Dict[str, Any] = {}
        for col in cols:
            key = str(col.get("key") or "").strip()
            if key:
                values[key] = d.get(key)
        data.setdefault(dim1_key, {})[dim2_key] = values

    cur = conn.execute(
        """
        SELECT column_name, formula, COALESCE(formula_type, 'sql') AS formula_type
        FROM _formula_registry
        WHERE table_name = ?
        ORDER BY column_name
        """,
        (t,),
    )
    column_formulas = {}
    for rec in cur.fetchall():
        if isinstance(rec, sqlite3.Row):
            column_name = str(rec["column_name"])
            formula = str(rec["formula"])
            formula_type = str(rec["formula_type"])
        else:
            column_name = str(rec[0])
            formula = str(rec[1])
            formula_type = str(rec[2])
        column_formulas[column_name] = {"formula": formula, "type": formula_type}

    return {
        "ok": True,
        "table_name": t,
        "display_name": (schema.get("display_name") if isinstance(schema, dict) else "") or "",
        "dim1": dim1,
        "dim2": dim2,
        "cols": cols,
        "column_formulas": column_formulas,
        "row_count": sum(len(v) for v in data.values()),
        "data": data,
    }


def delete_dynamic_table(conn: sqlite3.Connection, *, table_name: str, confirm: Union[bool, str, int]) -> Dict[str, Any]:
    """删除动态表及元数据；若有公式依赖本表列则拒绝。"""
    if confirm not in (True, "true", "True", 1, "1"):
        raise ValueError("confirm 须显式为 true")
    # 删除路径仍允许中文老表（兼容老库）
    t = assert_ident_loose(table_name)
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
