"""Agent 可调用的工具实现（对齐文档 06 子集）。"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Optional, Union

from app.deps import ProjectDB

TOOLS_OPENAI: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_project_config",
            "description": "读取项目配置与 project_settings（含 global_readme 等）",
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
            "description": "读取指定表的数据（最多 200 行）",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "limit": {"type": "integer", "default": 50},
                },
                "required": ["table_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_dependency_graph",
            "description": "返回依赖边列表，可选过滤表名",
            "parameters": {
                "type": "object",
                "properties": {"table_name": {"type": "string"}},
                "additionalProperties": False,
            },
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


def _read_table(conn: sqlite3.Connection, table_name: str, limit: int = 50) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    if not cur.fetchone():
        return {"error": f"未知表 {table_name}"}
    lim = max(1, min(int(limit or 50), 200))
    cur = conn.execute(f'SELECT * FROM "{table_name}" LIMIT ?', (lim,))
    return {"rows": [dict(r) for r in cur.fetchall()]}


def _get_dependency_graph(conn: sqlite3.Connection, table_name: Optional[str]) -> Dict[str, Any]:
    if table_name:
        cur = conn.execute(
            "SELECT * FROM _dependency_graph WHERE from_table = ? OR to_table = ?",
            (table_name, table_name),
        )
    else:
        cur = conn.execute("SELECT * FROM _dependency_graph")
    return {"edges": [dict(r) for r in cur.fetchall()]}


def dispatch_tool(name: str, arguments: Union[str, Dict[str, Any], None], p: ProjectDB) -> str:
    conn = p.conn
    args: Dict[str, Any] = {}
    if arguments:
        if isinstance(arguments, str):
            args = json.loads(arguments or "{}")
        else:
            args = dict(arguments)
    if name == "get_project_config":
        out = _get_project_config(conn)
    elif name == "get_table_list":
        out = _get_table_list(conn)
    elif name == "read_table":
        out = _read_table(conn, args.get("table_name", ""), int(args.get("limit", 50)))
    elif name == "get_dependency_graph":
        out = _get_dependency_graph(conn, args.get("table_name"))
    else:
        out = {"error": f"未知工具 {name}"}
    return json.dumps(out, ensure_ascii=False)
