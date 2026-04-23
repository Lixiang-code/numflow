"""DashScope Qwen via OpenAI-compatible Chat Completions."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from openai import OpenAI

from app.config import DASHSCOPE_API_KEY, DASHSCOPE_BASE_URL, QWEN_MODEL


def get_client() -> OpenAI:
    if not DASHSCOPE_API_KEY:
        raise RuntimeError("DASHSCOPE_API_KEY 未配置（backend/.env 或环境变量）")
    return OpenAI(api_key=DASHSCOPE_API_KEY, base_url=DASHSCOPE_BASE_URL)


def usage_to_dict(usage: Any) -> Optional[Dict[str, Any]]:
    if usage is None:
        return None
    d: Dict[str, Any] = {
        "prompt_tokens": getattr(usage, "prompt_tokens", None),
        "completion_tokens": getattr(usage, "completion_tokens", None),
        "total_tokens": getattr(usage, "total_tokens", None),
    }
    ptd = getattr(usage, "prompt_tokens_details", None)
    if ptd is None:
        return d
    if hasattr(ptd, "model_dump"):
        d["prompt_tokens_details"] = ptd.model_dump()
    elif isinstance(ptd, dict):
        d["prompt_tokens_details"] = ptd
    else:
        d["prompt_tokens_details"] = {
            "cached_tokens": getattr(ptd, "cached_tokens", None),
            "audio_tokens": getattr(ptd, "audio_tokens", None),
        }
    # 部分兼容层会把 cache 相关字段平铺在 usage 顶层
    for k in (
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
        "cached_tokens",
    ):
        v = getattr(usage, k, None)
        if v is not None:
            d[k] = v
    return d


def long_cacheable_system_block() -> str:
    """显式缓存要求 >1024 tokens；用长固定前缀满足下限。"""
    unit = (
        "【Numflow 数值工具固定上下文】"
        "本段为项目级只读说明，用于表格校验、Agent 工具约束与 README 摘要缓存测试。"
        "重复内容仅用于触发 DashScope 显式上下文缓存（ephemeral），无业务含义。"
    )
    return unit * 420


def build_system_message_with_explicit_cache() -> Dict[str, Any]:
    return {
        "role": "system",
        "content": [
            {
                "type": "text",
                "text": long_cacheable_system_block(),
                "cache_control": {"type": "ephemeral"},
            }
        ],
    }


def chat_once(
    messages: List[Dict[str, Any]],
    *,
    temperature: float = 0.2,
    max_tokens: int = 256,
) -> tuple[str, Dict[str, Any]]:
    client = get_client()
    completion = client.chat.completions.create(
        model=QWEN_MODEL,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    text = (completion.choices[0].message.content or "").strip()
    meta = {
        "model": completion.model,
        "id": completion.id,
        "finish_reason": completion.choices[0].finish_reason,
        "usage": usage_to_dict(completion.usage),
    }
    return text, meta
