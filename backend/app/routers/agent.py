"""千问 Agent（OpenAI 兼容）连通性与显式上下文缓存自检。"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, Generator, List, Literal, Optional

from fastapi import APIRouter, Body, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.deps import ProjectDB, get_project_read, get_project_write

from app.config import DASHSCOPE_API_KEY, DEEPSEEK_API_KEY, QWEN_MODEL
from app.db.project_schema import (
    create_agent_session,
    get_agent_session_messages,
    list_agent_sessions,
    update_agent_session,
)
from app.services import qwen_client
from app.services.agent_runner import run_agent_sse

router = APIRouter()


class ChatBody(BaseModel):
    message: str = Field(min_length=1, max_length=8000)
    mode: Literal["init", "maintain", "recovery"] = Field(
        default="maintain",
        description="init=首次建模；maintain=日常增量；recovery=失败修复",
    )
    step_id: Optional[str] = Field(
        default=None,
        description="当前 pipeline 步骤 ID（用于服务端持久化 session，刷新后可恢复）",
    )
    strict_review: bool = Field(
        default=False,
        description="为 True 时，execute 阶段每次写工具调用前都过一遍轻量 reviewer（可拒绝该 tool_call）",
    )
    failure_context: Optional[Dict[str, Any]] = Field(
        default=None,
        description="mode=recovery 时必填：{step_id, error, tool_history, partial_design}",
    )


def _session_tracking_wrapper(
    gen: Generator[bytes, None, None],
    conn: sqlite3.Connection,
    session_id: int,
) -> Generator[bytes, None, None]:
    """Intercept SSE events from `gen`, persist state to DB, then yield the same bytes."""
    design_buf: list[str] = []
    review_buf: list[str] = []
    execute_buf: list[str] = []
    tools: list[dict] = []
    tool_map: dict[str, dict] = {}
    tracked_events: list[dict] = []

    # Full conversation tracking: list of {phase, round?, messages:[{role,content}]}
    conversation_turns: list[dict] = []
    _user_message_stored = False
    _model_stored = ""
    _tracked_event_types = {
        "user_message",
        "prompt_route",
        "phase_messages",
        "tools_meta",
        "tool_call",
        "tool_result",
        "reviewer_verdict",
        "done",
        "error",
    }

    def _flush_tools() -> None:
        try:
            update_agent_session(conn, session_id, tools_json=json.dumps(tools, ensure_ascii=False))
        except Exception:  # noqa: BLE001
            pass

    def _flush_messages() -> None:
        try:
            update_agent_session(
                conn, session_id,
                messages_json=json.dumps(conversation_turns, ensure_ascii=False),
            )
        except Exception:  # noqa: BLE001
            pass

    def _track_event(raw: dict) -> None:
        if str(raw.get("type", "")) not in _tracked_event_types:
            return
        try:
            tracked_events.append(raw)
            update_agent_session(
                conn,
                session_id,
                events_json=json.dumps(tracked_events, ensure_ascii=False),
            )
        except Exception:  # noqa: BLE001
            pass

    try:
        for chunk in gen:
            # Intercept and parse each SSE event
            if chunk.startswith(b"data: "):
                try:
                    raw = json.loads(chunk[6:].decode("utf-8").strip())
                    phase = str(raw.get("phase", ""))
                    etype = str(raw.get("type", ""))
                    _track_event(raw)

                    # ── 用户消息 & 模型元信息 ──
                    if etype == "user_message":
                        if not _user_message_stored:
                            _user_message_stored = True
                            _model_stored = str(raw.get("model", ""))
                            try:
                                update_agent_session(
                                    conn, session_id,
                                    user_message=str(raw.get("content", ""))[:4000],
                                    model_used=_model_stored,
                                )
                            except Exception:  # noqa: BLE001
                                pass

                    # ── 完整消息快照 ──
                    elif etype == "phase_messages":
                        msgs = raw.get("messages", [])
                        if msgs:
                            phase_name = str(raw.get("phase", phase))
                            rnd = raw.get("round")
                            turn = {"phase": phase_name, "messages": msgs}
                            if rnd is not None:
                                turn["round"] = rnd
                                # Execute 中间轮次：原地覆盖最后一条同 phase 记录
                                # 避免 O(n²) 存储（每轮快照包含所有历史内容）
                                replaced = False
                                for i in range(len(conversation_turns) - 1, -1, -1):
                                    if conversation_turns[i].get("phase") == phase_name:
                                        conversation_turns[i] = turn
                                        replaced = True
                                        break
                                if not replaced:
                                    conversation_turns.append(turn)
                            else:
                                conversation_turns.append(turn)
                            _flush_messages()

                    elif etype == "token":
                        text = str(raw.get("text", ""))
                        if phase == "design":
                            design_buf.append(text)
                        elif phase == "review":
                            review_buf.append(text)
                            # Save design text once review starts (phase transition checkpoint)
                            if len(review_buf) == 1 and design_buf:
                                try:
                                    update_agent_session(conn, session_id, design_text="".join(design_buf))
                                except Exception:  # noqa: BLE001
                                    pass
                        elif phase == "execute":
                            execute_buf.append(text)
                            # Save review text once execute starts (phase transition checkpoint)
                            if len(execute_buf) == 1 and review_buf:
                                try:
                                    update_agent_session(conn, session_id, review_text="".join(review_buf))
                                except Exception:  # noqa: BLE001
                                    pass

                    elif etype == "tool_call":
                        call_id = str(raw.get("call_id", ""))
                        entry = {
                            "callId": call_id,
                            "name": str(raw.get("name", "")),
                            "label": str(raw.get("label", "")),
                            "arguments": str(raw.get("arguments", "")),
                            "status": "pending",
                            "resultPreview": None,
                        }
                        tools.append(entry)
                        if call_id:
                            tool_map[call_id] = entry
                        _flush_tools()

                    elif etype == "tool_result":
                        call_id = str(raw.get("call_id", ""))
                        status = str(raw.get("status", "done"))
                        if call_id in tool_map:
                            tool_map[call_id]["status"] = status
                            tool_map[call_id]["resultPreview"] = str(raw.get("preview", ""))[:600]
                        _flush_tools()
                        # Also persist accumulated execute text so far (on each tool result)
                        try:
                            update_agent_session(
                                conn, session_id,
                                execute_text="".join(execute_buf),
                            )
                        except Exception:  # noqa: BLE001
                            pass

                    elif etype == "done" and phase == "execute":
                        # All three phases complete — final save
                        try:
                            update_agent_session(
                                conn, session_id,
                                status="done",
                                design_text="".join(design_buf),
                                review_text="".join(review_buf),
                                execute_text=str(raw.get("full_text", "".join(execute_buf))),
                                tools_json=json.dumps(tools, ensure_ascii=False),
                                events_json=json.dumps(tracked_events, ensure_ascii=False),
                                messages_json=json.dumps(conversation_turns, ensure_ascii=False),
                                finished=True,
                            )
                        except Exception:  # noqa: BLE001
                            pass

                    elif etype == "error":
                        try:
                            update_agent_session(
                                conn, session_id,
                                status="error",
                                design_text="".join(design_buf),
                                review_text="".join(review_buf),
                                execute_text="".join(execute_buf),
                                tools_json=json.dumps(tools, ensure_ascii=False),
                                events_json=json.dumps(tracked_events, ensure_ascii=False),
                                messages_json=json.dumps(conversation_turns, ensure_ascii=False),
                                error_text=str(raw.get("message", ""))[:2000],
                                finished=True,
                            )
                        except Exception:  # noqa: BLE001
                            pass

                except Exception:  # noqa: BLE001 — never crash the stream
                    pass

            yield chunk

    except GeneratorExit:
        # Client disconnected — session stays 'running' (partial data already persisted)
        pass


@router.post("/chat")
def agent_chat(body: ChatBody, p: ProjectDB = Depends(get_project_write)):
    if not DASHSCOPE_API_KEY and not DEEPSEEK_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="未配置任何 AI API Key（DASHSCOPE_API_KEY 或 DEEPSEEK_API_KEY）",
        )

    # 读取项目绑定的 AI 模型（未设置则 None，由 run_agent_sse 回退到全局默认）
    _project_model: Optional[str] = None
    try:
        import json as _json
        row = p.conn.execute(
            "SELECT value_json FROM project_settings WHERE key = 'agent_model'"
        ).fetchone()
        if row:
            _project_model = _json.loads(row[0]) if row[0] else None
    except Exception:  # noqa: BLE001
        pass

    gen = run_agent_sse(
        body.message,
        p,
        mode=body.mode,
        strict_review=body.strict_review,
        failure_context=body.failure_context,
        model=_project_model,
    )

    # Persist session for ALL agent runs (so AgentMonitor can browse project history).
    # init/recovery use the explicit step_id; maintain runs use synthetic step_id "maintain"
    step_id = body.step_id or (
        body.failure_context.get("step_id") if body.failure_context else None
    )
    if not step_id:
        step_id = body.mode  # "maintain" / "init" / "recovery"
    try:
        session_id = create_agent_session(p.conn, step_id)
        gen = _session_tracking_wrapper(gen, p.conn, session_id)
    except Exception:  # noqa: BLE001 — don't break streaming if session creation fails
        pass

    return StreamingResponse(gen, media_type="text/event-stream")


@router.get("/sessions")
def agent_sessions(
    limit: int = 50,
    step_id: Optional[str] = None,
    p: ProjectDB = Depends(get_project_read),
) -> Dict[str, Any]:
    """List recent agent sessions for this project (for AgentMonitor history)."""
    try:
        sessions = list_agent_sessions(p.conn, limit=max(1, min(int(limit), 200)), step_id=step_id)
    except Exception:  # noqa: BLE001
        sessions = []
    return {"sessions": sessions}


@router.get("/sessions/{agent_session_id}")
def agent_session_detail(
    agent_session_id: int,
    p: ProjectDB = Depends(get_project_read),
) -> Dict[str, Any]:
    """Return a single session with its complete messages trace (system/user/assistant/tool)."""
    try:
        session = get_agent_session_messages(p.conn, agent_session_id)
    except Exception:  # noqa: BLE001
        session = None
    if not session:
        raise HTTPException(status_code=404, detail="session not found")
    return session


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
        "deepseek_api_key_configured": bool(DEEPSEEK_API_KEY),
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
    if not DASHSCOPE_API_KEY and not DEEPSEEK_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="未配置任何 AI API Key，请在 backend/.env 中设置 DASHSCOPE_API_KEY 或 DEEPSEEK_API_KEY",
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
