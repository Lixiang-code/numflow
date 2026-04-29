#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Real API check for SKILL-library understanding with deepseek-v4-flash."""

from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app.config import DEEPSEEK_API_KEY
from app.db.paths import get_project_dir
from app.db.project_migrations import ensure_project_migrations
from app.db.project_schema import init_project_db
from app.services.agent_tools import TOOLS_OPENAI, dispatch_tool
from app.services.qwen_client import get_client_for_model
from app.services.skill_library import get_skill_detail

MODEL = "deepseek-v4-flash"
SKILL_TOOLS = {"list_skills", "get_skill_detail", "render_skill_file"}
TOOLS = [tool for tool in TOOLS_OPENAI if tool["function"]["name"] in SKILL_TOOLS]


def make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_project_db(conn, seed_readme=False)
    ensure_project_migrations(conn)
    return conn


def build_assistant_message(msg: Any, tool_calls: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
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
                "你是 Numflow 的 SKILL 验证代理。你唯一允许依赖的玩法知识来自工具返回的 SKILL 内容。"
                "回答前必须先调用至少一个 SKILL 工具；不要臆测工具未提供的信息。"
                "最终回答使用简洁中文，并明确列出表名、关键约束和一个常见误区。"
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
            max_tokens=1200,
        )
        msg = resp.choices[0].message
        tool_calls = getattr(msg, "tool_calls", None) or []
        if tool_calls:
            calls_payload = []
            for tc in tool_calls:
                calls_payload.append(
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments or "{}",
                        },
                    }
                )
            messages.append(build_assistant_message(msg, tool_calls=calls_payload))
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
    used_skill_tool = any(name in {"get_skill_detail", "render_skill_file"} for name in tool_names)
    return {
        "name": case["name"],
        "tool_names": tool_names,
        "used_skill_tool": used_skill_tool,
        "final_text": final_text,
        "missing_keywords": missing,
        "pass": used_skill_tool and not missing,
    }


def main() -> int:
    if not DEEPSEEK_API_KEY:
        print(json.dumps({"ok": False, "error": "DEEPSEEK_API_KEY 未配置"}, ensure_ascii=False, indent=2))
        return 2

    client = get_client_for_model(MODEL)
    conn = make_conn()
    slug = f"skill-real-check-{int(time.time())}"
    project = SimpleNamespace(row={"slug": slug}, conn=conn, can_write=True)
    cases = [
        {
            "name": "gem",
            "prompt": (
                "当前目标步骤是 gameplay_landing_tables.gem。请先查阅相关 SKILL，"
                "然后说明：1）应该产出哪些表；2）为什么不能把角色标准等级 1..N 直接当成宝石 N 级；"
                "3）一个值得保留的可选扩展模块。"
            ),
            "must_contain": ["gem_landing", "gem_attr", "品阶"],
        },
        {
            "name": "mount",
            "prompt": (
                "当前目标步骤是 gameplay_landing_tables.mount。请先查阅相关 SKILL，"
                "然后说明：1）阶段轴应由什么驱动；2）至少需要哪些主表；3）为什么不能硬编码 30 级。"
            ),
            "must_contain": ["mount_landing", "mount_attr", "mount_cultivation_quant", "system_level_caps"],
        },
    ]

    results: List[Dict[str, Any]] = []
    try:
        for case in cases:
            results.append(run_case(client, project, case))
        gem = get_skill_detail(conn, "gem-landing")
        mount = get_skill_detail(conn, "mount-landing")
        summary = {
            "ok": all(item["pass"] for item in results),
            "model": MODEL,
            "results": results,
            "usage_counts": {
                "gem-landing": gem["usage_count"] if gem else None,
                "mount-landing": mount["usage_count"] if mount else None,
            },
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0 if summary["ok"] else 1
    finally:
        project_dir = get_project_dir(slug)
        if project_dir.exists():
            shutil.rmtree(project_dir)


if __name__ == "__main__":
    raise SystemExit(main())
