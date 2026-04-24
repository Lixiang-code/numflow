"""千问 Agent（OpenAI 兼容）连通性与显式上下文缓存自检。"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Body, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.deps import ProjectDB, get_project_write

from app.config import DASHSCOPE_API_KEY, QWEN_MODEL
from app.services import qwen_client
from app.services.agent_runner import run_agent_sse

router = APIRouter()


class ChatBody(BaseModel):
    message: str = Field(min_length=1, max_length=8000)
    mode: Literal["init", "maintain"] = Field(
        default="maintain",
        description="init=首次建模（文档 06）；maintain=日常增量（五步循环）",
    )


@router.post("/chat")
def agent_chat(body: ChatBody, p: ProjectDB = Depends(get_project_write)):
    if not DASHSCOPE_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="未配置 DASHSCOPE_API_KEY",
        )
    return StreamingResponse(
        run_agent_sse(body.message, p, mode=body.mode),
        media_type="text/event-stream",
    )


class DiagnosticsRunBody(BaseModel):
    """可选：自定义连通性测试里的用户句（仍走同一套自检流程）。"""

    connectivity_user: Optional[str] = Field(
        default=None,
        description="覆盖连通性测试 user 内容；默认使用内置短提示",
    )


@router.get("/diagnostics")
def diagnostics_config() -> Dict[str, Any]:
    """不发起外呼：仅看密钥是否已加载、当前模型名。"""
    return {
        "dashscope_api_key_configured": bool(DASHSCOPE_API_KEY),
        "model": QWEN_MODEL,
        "hint": "POST /api/agent/diagnostics/run 执行真实调用与缓存对比",
    }


@router.post("/diagnostics/run")
def diagnostics_run(
    body: DiagnosticsRunBody = Body(default_factory=DiagnosticsRunBody),
) -> Dict[str, Any]:
    """
    1) 短对话验证 OpenAI 兼容 Chat Completions 可用。
    2) 连续两次带显式 ephemeral 缓存的长 system，比较 usage 里缓存相关字段。
    """
    if not DASHSCOPE_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="未配置 DASHSCOPE_API_KEY，请在 backend/.env 或 systemd EnvironmentFile 中设置",
        )

    out: Dict[str, Any] = {
        "model": QWEN_MODEL,
        "explicit_cache_min_tokens_doc": 1024,
        "long_system_block_chars": len(qwen_client.long_cacheable_system_block()),
    }

    # --- 连通性（短提示词，文档 3.2 风格）---
    try:
        ping_messages: List[Dict[str, Any]] = [
            {
                "role": "system",
                "content": "你是游戏数值策划助手。回答务必简短，不要 Markdown。",
            },
            {
                "role": "user",
                "content": body.connectivity_user
                or "你是谁？用一句话说明模型名或身份。",
            },
        ]
        ping_text, ping_meta = qwen_client.chat_once(ping_messages, temperature=0, max_tokens=64)
        out["connectivity"] = {
            "ok": True,
            "assistant_preview": ping_text[:400],
            "raw_meta": ping_meta,
        }
    except Exception as e:  # noqa: BLE001 — 诊断接口需要原始错误文本
        out["connectivity"] = {"ok": False, "error": repr(e)}
        return out

    # --- 显式缓存：两次相同长 system，不同 user ---
    sys_msg = qwen_client.build_system_message_with_explicit_cache()
    try:
        m1 = [sys_msg, {"role": "user", "content": "只输出字面量：ROUND_ONE"}]
        t1, meta1 = qwen_client.chat_once(m1, temperature=0, max_tokens=32)
        m2 = [sys_msg, {"role": "user", "content": "只输出字面量：ROUND_TWO"}]
        t2, meta2 = qwen_client.chat_once(m2, temperature=0, max_tokens=32)
    except Exception as e:  # noqa: BLE001
        out["cache_rounds"] = {"ok": False, "error": repr(e)}
        return out

    u1 = (meta1.get("usage") or {}) if isinstance(meta1, dict) else {}
    u2 = (meta2.get("usage") or {}) if isinstance(meta2, dict) else {}

    def cache_signals(u: Dict[str, Any]) -> Dict[str, Any]:
        ptd = u.get("prompt_tokens_details") or {}
        if not isinstance(ptd, dict):
            ptd = {}
        return {
            "cached_tokens": ptd.get("cached_tokens"),
            "cache_creation_input_tokens": ptd.get("cache_creation_input_tokens")
            or u.get("cache_creation_input_tokens"),
            "cache_read_input_tokens": ptd.get("cache_read_input_tokens")
            or u.get("cache_read_input_tokens"),
            "prompt_tokens": u.get("prompt_tokens"),
            "raw_prompt_tokens_details": ptd,
        }

    s1 = cache_signals(u1)
    s2 = cache_signals(u2)

    # 命中显式缓存时，常见表现是第二次出现 cached_tokens > 0（具体字段以百炼返回为准）
    hit = bool(s2.get("cached_tokens")) and int(s2.get("cached_tokens") or 0) > 0

    def creation_tokens(sig: Dict[str, Any]) -> Optional[int]:
        v = sig.get("cache_creation_input_tokens")
        if isinstance(v, int) and v > 0:
            return v
        ptd = sig.get("raw_prompt_tokens_details") or {}
        if isinstance(ptd, dict):
            v2 = ptd.get("cache_creation_input_tokens")
            if isinstance(v2, int) and v2 > 0:
                return v2
            cc = ptd.get("cache_creation") or {}
            v3 = cc.get("ephemeral_5m_input_tokens")
            if isinstance(v3, int) and v3 > 0:
                return v3
        return None

    out["cache_summary"] = {
        "round1_cache_creation_input_tokens": creation_tokens(s1),
        "round1_cached_tokens_read": s1.get("cached_tokens"),
        "round2_cache_creation_input_tokens": creation_tokens(s2),
        "round2_cached_tokens_read": s2.get("cached_tokens"),
        "explicit_ephemeral_cache_hit": hit,
        "note_round1_zero_creation": "若第 1 轮创建为 0 但第 2 轮命中，多为 5 分钟内复用同一前缀的已有 ephemeral 缓存。",
    }

    out["cache_rounds"] = {
        "ok": True,
        "round1": {"assistant": t1[:200], "usage": u1, "cache_signals": s1},
        "round2": {"assistant": t2[:200], "usage": u2, "cache_signals": s2},
        "interpretation": {
            "ephemeral_explicit_cache_likely_hit": hit,
            "note": "第二次请求的 prompt_tokens_details.cached_tokens > 0 通常表示显式缓存命中；"
            "若两轮均为 0，可能是该账号/地域尚未返回细分字段，或前缀未满足创建条件。",
        },
    }
    return out
