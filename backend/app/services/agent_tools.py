"""Agent 可调用的工具实现（对齐文档 06，与现有 HTTP 能力一致）。"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Optional, Union

from app.deps import ProjectDB
from app.services import algorithms
from app.services.cell_writes import apply_write_cells, assert_col_or_table
from app.services.formula_exec import (
    execute_formula_on_column,
    recalculate_downstream,
    register_formula,
)
from app.data.default_rules_02 import get_default_rules_payload
from app.services.snapshot_ops import compare_snapshot, create_snapshot, list_snapshots
from app.services.table_ops import create_dynamic_table, delete_dynamic_table
from app.services.tool_envelope import wrap_tool_payload
from app.services.validation_report import build_validation_report, list_validation_history

TOOLS_OPENAI: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_project_config",
            "description": "读取项目配置与 project_settings（含 global_readme、fixed_layer_config 等）",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_list",
            "description": "列出所有业务表及验证状态",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_table",
            "description": "读取表数据；可选 columns、filters（每项 column+value 相等）、level_column+level_min/max、include_source_stats",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "limit": {"type": "integer", "default": 50},
                    "columns": {"type": "array", "items": {"type": "string"}},
                    "filters": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column": {"type": "string"},
                                "value": {},
                            },
                            "required": ["column"],
                        },
                    },
                    "level_column": {"type": "string"},
                    "level_min": {"type": "number"},
                    "level_max": {"type": "number"},
                    "include_source_stats": {"type": "boolean", "default": False},
                },
                "required": ["table_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_cell",
            "description": "读取单个单元格的值及来源标记",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "row_id": {"type": "string"},
                    "column_name": {"type": "string"},
                },
                "required": ["table_name", "row_id", "column_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_protected_cells",
            "description": "列出指定表中 user_manual 保护单元格坐标",
            "parameters": {
                "type": "object",
                "properties": {"table_name": {"type": "string"}},
                "required": ["table_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_dependency_graph",
            "description": "依赖边列表；direction: upstream|downstream|full（与 /meta/dependency-graph 一致）",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "direction": {
                        "type": "string",
                        "enum": ["upstream", "downstream", "full"],
                        "default": "full",
                    },
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_readme",
            "description": "读取指定业务表的 README 文本",
            "parameters": {
                "type": "object",
                "properties": {"table_name": {"type": "string"}},
                "required": ["table_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_table_readme",
            "description": "覆盖更新指定表的 README（需写权限）",
            "parameters": {
                "type": "object",
                "properties": {"table_name": {"type": "string"}, "content": {"type": "string"}},
                "required": ["table_name", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_global_readme",
            "description": "更新项目全局 README",
            "parameters": {
                "type": "object",
                "properties": {"content": {"type": "string"}},
                "required": ["content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "set_project_setting",
            "description": (
                "写入 project_settings 中的一个键值对。"
                "用于设置 max_level / currencies / stat_keys / resource_keys 等顶层项目参数。"
                "value 可为任意 JSON 值（字符串/数字/数组/对象）。"
                "注意：fixed_layer_config 受保护不可覆盖；global_readme 请用 update_global_readme。"
                "调用示例：{\"key\": \"max_level\", \"value\": 200}"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "key": {"type": "string", "description": "设置键名（如 max_level / currencies / stat_keys / resource_keys）"},
                    "value": {"description": "设置值，任意 JSON 类型（数字/字符串/数组/对象均可）"},
                },
                "required": ["key", "value"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_table",
            "description": "创建动态业务表并写入 _table_registry；columns 每项含 name、sql_type(TEXT|REAL|INTEGER)",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "columns": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "sql_type": {"type": "string"},
                            },
                            "required": ["name", "sql_type"],
                        },
                    },
                    "readme": {"type": "string", "default": ""},
                    "purpose": {"type": "string", "default": ""},
                },
                "required": ["table_name", "columns"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_cells",
            "description": "批量写入单元格；跳过 user_manual。updates 每项含 row_id、column、value",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "updates": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "row_id": {"type": "string"},
                                "column": {"type": "string"},
                                "value": {},
                            },
                            "required": ["row_id", "column"],
                        },
                    },
                    "source_tag": {
                        "type": "string",
                        "enum": ["ai_generated", "algorithm_derived", "formula_computed"],
                        "default": "ai_generated",
                    },
                },
                "required": ["table_name", "updates"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "register_formula",
            "description": "为指定列注册公式字符串（@表名[列名] 引用）；更新依赖图",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "column_name": {"type": "string"},
                    "formula_string": {"type": "string"},
                },
                "required": ["table_name", "column_name", "formula_string"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "execute_formula",
            "description": "执行已注册公式并写回列；可选 level_column+level_min/max 仅更新区间内行；写 _cell_provenance=formula_computed",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "column_name": {"type": "string"},
                    "level_column": {"type": "string"},
                    "level_min": {"type": "number"},
                    "level_max": {"type": "number"},
                },
                "required": ["table_name", "column_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "recalculate_downstream",
            "description": "从指定上游列沿依赖图重算下游公式列",
            "parameters": {
                "type": "object",
                "properties": {"table_name": {"type": "string"}, "column_name": {"type": "string"}},
                "required": ["table_name", "column_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_algorithm_api_list",
            "description": "列出已注册算法 API 元数据",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "call_algorithm_api",
            "description": "调用算法层 API（如 echo_sum），返回结构化结果",
            "parameters": {
                "type": "object",
                "properties": {
                    "api_name": {"type": "string"},
                    "params": {"type": "object"},
                },
                "required": ["api_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_validation",
            "description": "运行校验报告（含表级规则 violations）；可选 table_name 仅针对单表",
            "parameters": {
                "type": "object",
                "properties": {"table_name": {"type": "string"}},
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "delete_table",
            "description": "删除动态表；confirm 须为 true；若存在公式依赖本表列则拒绝",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "confirm": {"type": "boolean"},
                },
                "required": ["table_name", "confirm"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_snapshot",
            "description": "创建当前所有业务表快照（表级+列级哈希，便于 compare_snapshot）",
            "parameters": {
                "type": "object",
                "properties": {"label": {"type": "string"}, "note": {"type": "string", "default": ""}},
                "required": ["label"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_snapshots",
            "description": "列出最近快照元数据",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compare_snapshot",
            "description": "将当前表哈希与指定快照对比，返回变更表列表",
            "parameters": {
                "type": "object",
                "properties": {"snapshot_id": {"type": "integer"}},
                "required": ["snapshot_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_balance_check",
            "description": "平衡指标检查（占位实现）",
            "parameters": {
                "type": "object",
                "properties": {
                    "level_min": {"type": "integer"},
                    "level_max": {"type": "integer"},
                    "metrics": {"type": "array", "items": {"type": "string"}},
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_validation_history",
            "description": "最近校验历史摘要（按表过滤可选）",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "limit": {"type": "integer", "default": 20},
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_default_system_rules",
            "description": "读取文档 02 默认系统细则（全局可机读子集）",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
]


def _get_project_config(conn: sqlite3.Connection) -> Dict[str, Any]:
    cur = conn.execute("SELECT key, value_json FROM project_settings")
    settings: Dict[str, Any] = {}
    for k, v in cur.fetchall():
        try:
            settings[k] = json.loads(v)
        except json.JSONDecodeError:
            settings[k] = v
    return {"settings": settings}


def _get_table_list(conn: sqlite3.Connection) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT table_name, layer, purpose, validation_status FROM _table_registry ORDER BY table_name"
    )
    return {"tables": [dict(r) for r in cur.fetchall()]}


def _provenance_stats(
    conn: sqlite3.Connection,
    table_name: str,
    row_ids: List[str],
    limit_rows: int,
) -> Dict[str, Dict[str, int]]:
    if not row_ids:
        return {}
    stats: Dict[str, Dict[str, int]] = {}
    chunk_size = 400
    for i in range(0, min(len(row_ids), limit_rows), chunk_size):
        chunk = row_ids[i : i + chunk_size]
        ph = ",".join("?" * len(chunk))
        cur = conn.execute(
            f"""
            SELECT column_name, source_tag, COUNT(*) AS n
            FROM _cell_provenance
            WHERE table_name = ? AND row_id IN ({ph})
            GROUP BY column_name, source_tag
            """,
            (table_name, *chunk),
        )
        for r in cur.fetchall():
            col = str(r["column_name"])
            tag = str(r["source_tag"])
            n = int(r["n"])
            stats.setdefault(col, {})
            stats[col][tag] = stats[col].get(tag, 0) + n
    return stats


def _read_table(
    conn: sqlite3.Connection,
    table_name: str,
    limit: int = 50,
    columns: Optional[List[str]] = None,
    filters: Optional[List[Dict[str, Any]]] = None,
    level_column: Optional[str] = None,
    level_min: Optional[float] = None,
    level_max: Optional[float] = None,
    include_source_stats: bool = False,
) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    if not cur.fetchone():
        return {"error": f"未知表 {table_name}"}
    lim = max(1, min(int(limit or 50), 200))
    try:
        t = assert_col_or_table(table_name)
    except ValueError as e:
        return {"error": str(e)}
    where_parts: List[str] = []
    params: List[Any] = []
    if filters:
        for f in filters:
            if not isinstance(f, dict):
                continue
            coln = str(f.get("column", "")).strip()
            if not coln:
                continue
            try:
                cq = assert_col_or_table(coln)
            except ValueError:
                return {"error": f"非法列 filter {coln}"}
            where_parts.append(f'"{cq}" = ?')
            params.append(f.get("value"))
    if level_column is not None and level_min is not None and level_max is not None:
        try:
            lc = assert_col_or_table(str(level_column))
        except ValueError as e:
            return {"error": str(e)}
        where_parts.append(f'CAST("{lc}" AS REAL) BETWEEN ? AND ?')
        params.extend([float(level_min), float(level_max)])
    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

    if columns:
        parts = ["row_id"]
        seen = {"row_id"}
        for raw in columns:
            c = str(raw).strip()
            if not c or c in seen:
                continue
            try:
                parts.append(f'"{assert_col_or_table(c)}"')
            except ValueError as e:
                return {"error": str(e)}
            seen.add(c)
        sel = ", ".join(parts)
    else:
        sel = "*"
    sql = f'SELECT {sel} FROM "{t}"{where_sql} LIMIT ?'
    params.append(lim)
    cur = conn.execute(sql, tuple(params))
    rows = [dict(r) for r in cur.fetchall()]
    out: Dict[str, Any] = {"rows": rows}
    if include_source_stats and rows:
        rids = [str(r["row_id"]) for r in rows if r.get("row_id") is not None]
        out["provenance_stats"] = _provenance_stats(conn, t, rids, len(rows))
    return out


def _read_cell(conn: sqlite3.Connection, table_name: str, row_id: str, column_name: str) -> Dict[str, Any]:
    try:
        t = assert_col_or_table(table_name)
        col = assert_col_or_table(column_name)
    except ValueError as e:
        return {"error": str(e)}
    cur = conn.execute(f'SELECT "{col}" AS v FROM "{t}" WHERE row_id = ?', (str(row_id),))
    row = cur.fetchone()
    if not row:
        return {"error": "行不存在"}
    cur = conn.execute(
        """
        SELECT source_tag FROM _cell_provenance
        WHERE table_name = ? AND row_id = ? AND column_name = ?
        """,
        (t, str(row_id), col),
    )
    pr = cur.fetchone()
    src = pr["source_tag"] if pr else None
    return {"value": row["v"], "source_tag": src}


def _get_protected_cells(conn: sqlite3.Connection, table_name: str) -> Dict[str, Any]:
    try:
        t = assert_col_or_table(table_name)
    except ValueError as e:
        return {"error": str(e)}
    cur = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?",
        (t,),
    )
    if not cur.fetchone():
        return {"error": f"未知表 {t}"}
    cur = conn.execute(
        """
        SELECT row_id, column_name FROM _cell_provenance
        WHERE table_name = ? AND source_tag = 'user_manual'
        """,
        (t,),
    )
    return {"cells": [{"row_id": r["row_id"], "column": r["column_name"]} for r in cur.fetchall()]}


def _dependency_edges(
    conn: sqlite3.Connection,
    table_name: Optional[str],
    direction: str = "full",
) -> Dict[str, Any]:
    d = (direction or "full").lower()
    if d not in ("upstream", "downstream", "full"):
        return {"error": "direction 须为 upstream / downstream / full"}
    if table_name:
        if d == "upstream":
            cur = conn.execute(
                "SELECT * FROM _dependency_graph WHERE to_table = ?",
                (table_name,),
            )
        elif d == "downstream":
            cur = conn.execute(
                "SELECT * FROM _dependency_graph WHERE from_table = ?",
                (table_name,),
            )
        else:
            cur = conn.execute(
                "SELECT * FROM _dependency_graph WHERE from_table = ? OR to_table = ?",
                (table_name, table_name),
            )
    else:
        cur = conn.execute("SELECT * FROM _dependency_graph")
    return {"edges": [dict(r) for r in cur.fetchall()]}


def _get_table_readme(conn: sqlite3.Connection, table_name: str) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT readme FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    row = cur.fetchone()
    if not row:
        return {"error": f"未知表 {table_name}"}
    return {"table_name": table_name, "readme": row["readme"] or ""}


def _update_table_readme(conn: sqlite3.Connection, table_name: str, content: str) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    if not cur.fetchone():
        return {"error": f"未知表 {table_name}"}
    conn.execute(
        "UPDATE _table_registry SET readme = ? WHERE table_name = ?",
        (content, table_name),
    )
    conn.commit()
    return {"ok": True}


def _update_global_readme(conn: sqlite3.Connection, content: str) -> Dict[str, Any]:
    import time

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute(
        """
        INSERT INTO project_settings (key, value_json, updated_at)
        VALUES ('global_readme', ?, ?)
        ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json,
            updated_at = excluded.updated_at
        """,
        (json.dumps({"text": content}, ensure_ascii=False), now),
    )
    conn.commit()
    return {"ok": True}


_PROTECTED_SETTINGS = frozenset({"fixed_layer_config"})


def _set_project_setting(conn: sqlite3.Connection, key: str, value: Any) -> Dict[str, Any]:
    """写入 project_settings 中的任意键值对（保护 fixed_layer_config 不被覆盖）。"""
    import time

    if not key or not key.strip():
        raise ValueError("key 不能为空")
    if key in _PROTECTED_SETTINGS:
        raise ValueError(f"键 {key!r} 受保护，请勿覆盖；如需更新全局 README 请用 update_global_readme")
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    value_json = json.dumps(value, ensure_ascii=False)
    conn.execute(
        """
        INSERT INTO project_settings (key, value_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json,
            updated_at = excluded.updated_at
        """,
        (key, value_json, now),
    )
    conn.commit()
    return {"ok": True, "key": key, "value_preview": value_json[:200]}


def dispatch_tool(name: str, arguments: Union[str, Dict[str, Any], None], p: ProjectDB) -> str:
    conn = p.conn
    args: Dict[str, Any] = {}
    if arguments:
        if isinstance(arguments, str):
            try:
                args = json.loads(arguments or "{}")
            except json.JSONDecodeError as exc:
                return json.dumps(
                    {
                        "status": "error",
                        "data": None,
                        "warnings": [
                            f"tool_call arguments JSON decode failed: {exc!r}; raw={arguments[:200]!r}"
                        ],
                        "blocked_cells": [],
                    },
                    ensure_ascii=False,
                )
        else:
            args = dict(arguments)

    out: Dict[str, Any]
    if name == "get_project_config":
        out = {**_get_project_config(conn), "can_write": p.can_write}
    elif name == "get_table_list":
        out = _get_table_list(conn)
    elif name == "read_table":
        cols = args.get("columns")
        col_list: Optional[List[str]] = cols if isinstance(cols, list) else None
        flt = args.get("filters") if isinstance(args.get("filters"), list) else None
        lm = args.get("level_min")
        lx = args.get("level_max")
        out = _read_table(
            conn,
            str(args.get("table_name", "")),
            int(args.get("limit", 50)),
            col_list,
            flt,
            args.get("level_column"),
            float(lm) if lm is not None else None,
            float(lx) if lx is not None else None,
            bool(args.get("include_source_stats")),
        )
    elif name == "read_cell":
        out = _read_cell(conn, str(args.get("table_name", "")), str(args.get("row_id", "")), str(args.get("column_name", "")))
    elif name == "get_protected_cells":
        out = _get_protected_cells(conn, str(args.get("table_name", "")))
    elif name == "get_dependency_graph":
        out = _dependency_edges(conn, args.get("table_name"), str(args.get("direction", "full")))
    elif name == "get_table_readme":
        out = _get_table_readme(conn, str(args.get("table_name", "")))
    elif name == "update_table_readme":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            out = _update_table_readme(conn, str(args.get("table_name", "")), str(args.get("content", "")))
    elif name == "update_global_readme":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            out = _update_global_readme(conn, str(args.get("content", "")))
    elif name == "set_project_setting":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            try:
                out = _set_project_setting(conn, str(args.get("key", "")), args.get("value"))
            except ValueError as e:
                out = {"error": str(e)}
    elif name == "create_table":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            raw_cols = args.get("columns") or []
            pairs: List[tuple[str, str]] = []
            try:
                for item in raw_cols:
                    if not isinstance(item, dict):
                        continue
                    pairs.append((str(item.get("name", "")), str(item.get("sql_type", "TEXT"))))
                out = create_dynamic_table(
                    conn,
                    table_name=str(args.get("table_name", "")),
                    columns=pairs,
                    readme=str(args.get("readme", "")),
                    purpose=str(args.get("purpose", "")),
                )
            except ValueError as e:
                out = {"error": str(e)}
    elif name == "write_cells":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            tag = args.get("source_tag") or "ai_generated"
            if tag not in ("ai_generated", "algorithm_derived", "formula_computed"):
                out = {"error": "非法 source_tag"}
            else:
                try:
                    out = apply_write_cells(
                        conn,
                        table_name=str(args.get("table_name", "")),
                        updates=list(args.get("updates") or []),
                        source_tag=tag,
                    )
                except ValueError as e:
                    out = {"error": str(e)}
    elif name == "register_formula":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            try:
                out = register_formula(
                    conn,
                    str(args.get("table_name", "")),
                    str(args.get("column_name", "")),
                    str(args.get("formula_string", "")),
                )
            except ValueError as e:
                out = {"error": str(e)}
    elif name == "execute_formula":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            try:
                lm = args.get("level_min")
                lx = args.get("level_max")
                out = execute_formula_on_column(
                    conn,
                    str(args.get("table_name", "")),
                    str(args.get("column_name", "")),
                    level_column=str(args["level_column"]) if args.get("level_column") else None,
                    level_min=float(lm) if lm is not None else None,
                    level_max=float(lx) if lx is not None else None,
                )
            except ValueError as e:
                out = {"error": str(e)}
    elif name == "recalculate_downstream":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            out = recalculate_downstream(
                conn,
                str(args.get("table_name", "")),
                str(args.get("column_name", "")),
            )
    elif name == "get_algorithm_api_list":
        out = {"apis": algorithms.list_apis()}
    elif name == "call_algorithm_api":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            try:
                params = args.get("params") if isinstance(args.get("params"), dict) else {}
                out = {"result": algorithms.call_api(str(args.get("api_name", "")), params)}
            except Exception as e:  # noqa: BLE001
                out = {"error": str(e)}
    elif name == "run_validation":
        tn = args.get("table_name")
        ft = str(tn) if tn else None
        out = build_validation_report(conn, filter_table=ft)
    elif name == "delete_table":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            try:
                out = delete_dynamic_table(
                    conn,
                    table_name=str(args.get("table_name", "")),
                    confirm=args.get("confirm"),
                )
            except ValueError as e:
                out = {"error": str(e)}
    elif name == "create_snapshot":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            try:
                out = create_snapshot(
                    conn,
                    label=str(args.get("label", "snapshot")),
                    note=str(args.get("note", "")),
                )
            except Exception as e:  # noqa: BLE001
                out = {"error": str(e)}
    elif name == "list_snapshots":
        out = {"snapshots": list_snapshots(conn)}
    elif name == "compare_snapshot":
        try:
            out = compare_snapshot(conn, int(args.get("snapshot_id", 0)))
        except (ValueError, TypeError) as e:
            out = {"error": str(e)}
    elif name == "run_balance_check":
        out = {
            "metrics": [],
            "level_min": args.get("level_min"),
            "level_max": args.get("level_max"),
            "note": "占位：待接入平衡模型（文档 06 run_balance_check）",
        }
    elif name == "get_validation_history":
        lim = int(args.get("limit", 20))
        tn = args.get("table_name")
        ft = str(tn) if tn else None
        out = {"history": list_validation_history(conn, table_name=ft, limit=lim)}
    elif name == "get_default_system_rules":
        out = get_default_rules_payload()
    else:
        out = {"error": f"未知工具 {name}"}
    return json.dumps(wrap_tool_payload(out), ensure_ascii=False)
