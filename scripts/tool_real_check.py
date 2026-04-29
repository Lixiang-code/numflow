#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Real API check for tool understanding with deepseek-v4-flash."""

from __future__ import annotations

import json
import pathlib
import sqlite3
import sys
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app.config import DEEPSEEK_API_KEY
from app.db.project_migrations import ensure_project_migrations
from app.db.project_schema import init_project_db
from app.services.agent_tools import TOOLS_OPENAI, dispatch_tool
from app.services.qwen_client import get_client_for_model
from app.services.table_ops import create_3d_table, create_dynamic_table

MODEL = "deepseek-v4-flash"
TOOL_NAMES = {"get_table_list", "get_table_schema", "read_3d_table", "read_table"}
TOOLS = [tool for tool in TOOLS_OPENAI if tool["function"]["name"] in TOOL_NAMES]


def make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_project_db(conn, seed_readme=False)
    ensure_project_migrations(conn)
    return conn


def seed_tables(conn: sqlite3.Connection) -> None:
    create_3d_table(
        conn,
        table_name="gem_attr_3d",
        display_name="宝石三维属性表",
        dim1={
            "col_name": "level",
            "display_name": "等级",
            "keys": [{"key": "1", "display_name": "1级"}, {"key": "2", "display_name": "2级"}],
        },
        dim2={
            "col_name": "gem_type",
            "display_name": "宝石类型",
            "keys": [{"key": "atk", "display_name": "攻击宝石"}, {"key": "def", "display_name": "防御宝石"}],
        },
        cols=[
            {
                "key": "atk_bonus",
                "display_name": "攻击加成",
                "dtype": "float",
                "number_format": "0.0000",
                "formula": "@level * 1.056487454",
            }
        ],
        readme="用于验证真实三维表工具理解。",
        purpose="real api tool check",
        directory="落地表/宝石",
        tags=["宝石", "属性"],
    )
    create_dynamic_table(
        conn,
        table_name="mount_landing",
        display_name="坐骑落地表",
        columns=[("stage_key", "TEXT"), ("exp_cost", "INTEGER"), ("attr_multiplier", "REAL")],
        readme="空表结构测试",
        purpose="real api schema check",
        column_meta=[
            {"name": "stage_key", "display_name": "阶段键", "dtype": "id", "number_format": ""},
            {"name": "exp_cost", "display_name": "经验消耗", "dtype": "int", "number_format": "0"},
            {"name": "attr_multiplier", "display_name": "属性倍率", "dtype": "float", "number_format": "0.00"},
        ],
        directory="落地表/坐骑",
        tags=["坐骑", "落地"],
    )


def build_assistant_message(msg: Any, *, tool_calls: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    data: Dict[str, Any] = {"role": "assistant", "content": msg.content or ""}
    reasoning = getattr(msg, "reasoning_content", None)
    if reasoning:
        data["reasoning_content"] = reasoning
    if tool_calls is not None:
        data["tool_calls"] = tool_calls
    return data


def run_case(client: Any, project: Any, case: Dict[str, Any]) -> Dict[str, Any]:
    messages: List[Dict[str, Any]] = [
        {
            "role": "system",
            "content": (
                "你是 Numflow 工具验证代理。你只能依据工具返回作答。"
                "回答前必须先调用至少一个工具；不要臆测工具未返回的信息。"
                "最终回答用简洁中文，明确列出你看到的结构结论。"
            ),
        },
        {"role": "user", "content": case["prompt"]},
    ]
    tool_names: List[str] = []
    final_text = ""
    for _ in range(6):
        resp = client.chat.completions.create(
            model=MODEL,
            messages=messages,
            tools=TOOLS,
            tool_choice="auto",
            temperature=0,
            max_tokens=1000,
        )
        msg = resp.choices[0].message
        tool_calls = getattr(msg, "tool_calls", None) or []
        if tool_calls:
            payload = []
            for tc in tool_calls:
                payload.append(
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {"name": tc.function.name, "arguments": tc.function.arguments or "{}"},
                    }
                )
            messages.append(build_assistant_message(msg, tool_calls=payload))
            for tc in tool_calls:
                tool_names.append(tc.function.name)
                result = dispatch_tool(tc.function.name, tc.function.arguments or "{}", project)
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})
            continue
        final_text = (msg.content or "").strip()
        messages.append(build_assistant_message(msg))
        break

    if not final_text:
        raise RuntimeError(f"case={case['name']} 未获得最终回答")

    lowered = final_text.lower()
    missing = [kw for kw in case["must_contain"] if kw.lower() not in lowered]
    used_required = any(name in case["must_use_one_of"] for name in tool_names)
    return {
        "name": case["name"],
        "tool_names": tool_names,
        "final_text": final_text,
        "missing_keywords": missing,
        "pass": used_required and not missing,
    }


def main() -> int:
    if not DEEPSEEK_API_KEY:
        print(json.dumps({"ok": False, "error": "DEEPSEEK_API_KEY 未配置"}, ensure_ascii=False, indent=2))
        return 2

    conn = make_conn()
    seed_tables(conn)
    project = SimpleNamespace(row={"slug": "tool-real-check"}, conn=conn, can_write=True)
    client = get_client_for_model(MODEL)
    cases = [
        {
            "name": "3d_table_layout",
            "prompt": (
                "请检查 gem_attr_3d 的真实结构，并回答："
                "1）它是不是二维伪装表；2）两个维度分别是什么；3）前端更适合按什么方式展示。"
            ),
            "must_use_one_of": {"read_3d_table", "get_table_schema"},
            "must_contain": ["三维", "等级", "宝石类型", "三轴"],
        },
        {
            "name": "empty_table_schema",
            "prompt": (
                "mount_landing 现在还是空表。请不要猜，直接用工具确认它的列结构、目录和标签。"
            ),
            "must_use_one_of": {"get_table_schema"},
            "must_contain": ["stage_key", "exp_cost", "attr_multiplier", "落地表/坐骑"],
        },
    ]

    results = [run_case(client, project, case) for case in cases]
    summary = {"ok": all(item["pass"] for item in results), "model": MODEL, "results": results}
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
