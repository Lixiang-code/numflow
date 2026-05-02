"""DashScope Qwen / DeepSeek via OpenAI-compatible Chat Completions."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import httpx
from openai import OpenAI

from app.config import (
    DASHSCOPE_API_KEY, DASHSCOPE_BASE_URL, QWEN_MODEL,
    DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL, DEEPSEEK_MODELS,
    MIMO_API_KEY, MIMO_BASE_URL,
)

# 流式响应超时：每 chunk 之间最长等待 120 秒
# 解决 DeepSeek/DashScope streaming 卡住导致 agent 永久挂起的问题
_STREAM_READ_TIMEOUT = 120.0


def _is_deepseek_model(model: str) -> bool:
    """Only v4 models route to DeepSeek's own API; older deepseek-* models are served by DashScope."""
    return model in DEEPSEEK_MODELS


def _is_mimo_model(model: str) -> bool:
    return model.startswith("mimo-")


def get_client() -> OpenAI:
    if not DASHSCOPE_API_KEY:
        raise RuntimeError("DASHSCOPE_API_KEY 未配置（backend/.env 或环境变量）")
    return OpenAI(
        api_key=DASHSCOPE_API_KEY, base_url=DASHSCOPE_BASE_URL,
        timeout=httpx.Timeout(connect=10.0, read=_STREAM_READ_TIMEOUT, write=30.0, pool=10.0),
    )


def get_deepseek_client() -> OpenAI:
    if not DEEPSEEK_API_KEY:
        raise RuntimeError("DEEPSEEK_API_KEY 未配置（backend/.env 或环境变量）")
    return OpenAI(
        api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL,
        timeout=httpx.Timeout(connect=10.0, read=_STREAM_READ_TIMEOUT, write=30.0, pool=10.0),
    )


def get_mimo_client() -> OpenAI:
    if not MIMO_API_KEY:
        raise RuntimeError("MIMO_API_KEY 未配置（backend/.env 或环境变量）")
    return OpenAI(
        api_key=MIMO_API_KEY, base_url=MIMO_BASE_URL,
        timeout=httpx.Timeout(connect=10.0, read=_STREAM_READ_TIMEOUT, write=30.0, pool=10.0),
    )


def get_client_for_model(model: str) -> OpenAI:
    """Return the appropriate OpenAI-compatible client based on model name."""
    if _is_deepseek_model(model):
        return get_deepseek_client()
    if _is_mimo_model(model):
        return get_mimo_client()
    return get_client()


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
    """显式缓存要求 >1024 tokens；用长固定前缀满足下限。

    ⚠️ 仅供 /api/agent/diagnostics/run 自检 ephemeral 缓存命中率使用。
    生产 Agent 调用链（agent_runner.run_agent_sse）不会拼接此内容，
    避免无业务含义的填充字符进入实际 prompt。
    """
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
    model: Optional[str] = None,
) -> tuple[str, Dict[str, Any]]:
    _model = model or QWEN_MODEL
    client = get_client_for_model(_model)
    completion = client.chat.completions.create(
        model=_model,
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
