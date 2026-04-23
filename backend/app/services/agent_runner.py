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


def run_agent_sse(user_message: str, p: ProjectDB) -> Generator[bytes, None, None]:
    yield sse_event({"type": "log", "message": "开始调度 Agent（维护模式）"})

    system = (
        "你是 Numflow 游戏数值设计助手的「维护 Agent」。\n"
        "必须遵守：不覆盖用户手动单元格（工具层已拦截）；不确定时先提问。\n"
        "按需调用工具获取项目与表数据，回答应简洁并引用工具结果。"
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
                yield sse_event({"type": "tool_result", "name": name, "preview": result[:2000]})
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
