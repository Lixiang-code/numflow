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
