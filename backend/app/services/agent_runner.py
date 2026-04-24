"""Agent 调度：工具循环 + SSE 事件输出。"""

from __future__ import annotations

import json
from typing import Any, Dict, Generator, Iterable, List

from app.config import QWEN_MODEL
from app.deps import ProjectDB
from app.services.agent_tools import TOOLS_OPENAI, dispatch_tool
from app.services.qwen_client import get_client


def sse_event(obj: Dict[str, Any]) -> bytes:
    return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n".encode("utf-8")


def run_agent_sse(
    user_message: str,
    p: ProjectDB,
    *,
    mode: str = "maintain",
) -> Generator[bytes, None, None]:
    mode_norm = mode if mode in ("init", "maintain") else "maintain"
    role = "初始化 Agent" if mode_norm == "init" else "维护 Agent"
    yield sse_event({"type": "log", "message": f"开始调度 Agent（{role}）"})

    if mode_norm == "init":
        role_block = (
            "【1/4 角色】你是 Numflow「初始化 Agent」。职责：根据项目配置推导待建表、依赖与 README，"
            "按文档 03 顺序分阶段推进；未经用户确认前不执行破坏性写操作；不得跳阶段。\n"
            "约束：不覆盖 user_manual 单元格（工具层会跳过并返回 skipped）。"
        )
    else:
        role_block = (
            "【1/4 角色】你是 Numflow「维护 Agent」。职责：理解变更—定界影响—执行写入—验证—更新 README。\n"
            "约束：写入前用 get_protected_cells / read_cell 确认范围；不覆盖用户手动格；不确定时先提问。"
        )

    system = "\n".join(
        [
            role_block,
            "【2/4 项目上下文】每次任务先 get_project_config，再按需 get_dependency_graph、get_table_readme；"
            "勿假设未读到的表结构。新建或修改「*系统_落地」、各子系统**行轴**、消耗与属性投放时，**须先** "
            "get_default_system_rules 对照 02 机读默认；禁止各系统无差别复用同一张仅「标准等级+两列消耗」的落地模板。 "
            "宝石的默认数据轴是**品阶/合成**（3 同阶→1 高 1 品）与**解锁门槛/属性池/分配**列，**不是**把标准等级 1..N 逐行 1:1 当成「宝石 N 级表」。"
            "坐骑/副本须体现开放等级与 02 约定（如坐骑 30 级、副本默认门槛等），列里要有玩法含义而非只有金币+掉率。",
            "【3/4 工具规范】读工具可自由组合；写表/写 README/公式/算法调用仅在执行阶段且需写权限；"
            "每次写入必须带合法 source_tag；大批量用 read_table 的 limit/columns 切片。\n"
            "工具 JSON 固定含字段：status（success|error|partial）、data、warnings、blocked_cells；"
            "遇 partial/error 须阅读 warnings/blocked_cells 再决定是否继续。",
            "【4/4 输出】先简述计划再调用工具；最终回答简洁并引用工具结果 data 中的关键字段。",
        ]
    )
    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": system},
        {"role": "user", "content": user_message},
    ]
    client = get_client()
    max_rounds = 6
    for round_i in range(max_rounds):
        yield sse_event({"type": "log", "message": f"模型推理轮次 {round_i + 1}"})
        resp = client.chat.completions.create(
            model=QWEN_MODEL,
            messages=messages,
            tools=TOOLS_OPENAI,
            tool_choice="auto",
            temperature=0.2,
            max_tokens=2048,
        )
        choice = resp.choices[0]
        msg = choice.message
        if getattr(msg, "tool_calls", None):
            messages.append(
                {
                    "role": "assistant",
                    "content": msg.content or "",
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.function.name,
                                "arguments": tc.function.arguments or "{}",
                            },
                        }
                        for tc in msg.tool_calls
                    ],
                }
            )
            for tc in msg.tool_calls:
                name = tc.function.name
                args = tc.function.arguments or "{}"
                yield sse_event({"type": "tool_call", "name": name, "arguments": args})
                result = dispatch_tool(name, args, p)
                yield sse_event(
                    {
                        "type": "tool_result",
                        "name": name,
                        "preview": result[:2000],
                        "hint": "检查 JSON 内 status/warnings/blocked_cells",
                    }
                )
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": result,
                    }
                )
            continue
        text = msg.content or ""
        for chunk in _chunk_text(text, 80):
            yield sse_event({"type": "token", "text": chunk})
        yield sse_event({"type": "done", "full_text": text})
        return
    yield sse_event({"type": "error", "message": "超过最大工具轮次"})


def _chunk_text(text: str, size: int) -> Iterable[str]:
    for i in range(0, len(text), size):
        yield text[i : i + size]
