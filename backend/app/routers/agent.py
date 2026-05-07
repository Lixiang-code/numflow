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
    get_resumable_session,
    list_agent_sessions,
    update_agent_session,
)
from app.services import qwen_client
from app.services.agent_runner import run_agent_sse, sse_event
from app.services.maintain_agent import (
    append_maintain_session_messages,
    create_maintain_session,
    delete_maintain_session,
    get_maintain_session_messages,
    init_maintain_sessions_table,
    list_maintain_sessions,
    rename_maintain_session,
    run_maintain_agent_sse,
)

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
    _current_phase = ""  # 追踪当前阶段
    _completed_phases: list[str] = []  # 追踪已完成的阶段
    _gather_context_saved = False  # 是否已保存gather上下文
    _tracked_event_types = {
        "user_message",
        "prompt_route",
        "prompt_sources",
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

    def _collect_gather_context() -> list[dict]:
        gather_ctx = []
        for turn in conversation_turns:
            if turn.get("phase") == "gather":
                gather_ctx.extend(turn.get("messages", []))
        return gather_ctx

    def _persist_error_state(error_text: str) -> None:
        try:
            update_agent_session(
                conn,
                session_id,
                status="error",
                design_text="".join(design_buf),
                review_text="".join(review_buf),
                execute_text="".join(execute_buf),
                tools_json=json.dumps(tools, ensure_ascii=False),
                events_json=json.dumps(tracked_events, ensure_ascii=False),
                messages_json=json.dumps(conversation_turns, ensure_ascii=False),
                error_text=error_text[:2000],
                current_phase=_current_phase,
                completed_phases=json.dumps(_completed_phases),
                gather_context_json=json.dumps(_collect_gather_context(), ensure_ascii=False),
                finished=True,
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
                            # 更新当前阶段
                            if _current_phase != "design":
                                _current_phase = "design"
                                try:
                                    update_agent_session(conn, session_id, current_phase="design")
                                except Exception:  # noqa: BLE001
                                    pass
                        elif phase == "review":
                            review_buf.append(text)
                            # 更新当前阶段
                            if _current_phase != "review":
                                _current_phase = "review"
                                if "design" not in _completed_phases:
                                    _completed_phases.append("design")
                                try:
                                    update_agent_session(
                                        conn, session_id,
                                        current_phase="review",
                                        completed_phases=json.dumps(_completed_phases),
                                    )
                                except Exception:  # noqa: BLE001
                                    pass
                            # Save design text once review starts (phase transition checkpoint)
                            if len(review_buf) == 1 and design_buf:
                                try:
                                    update_agent_session(conn, session_id, design_text="".join(design_buf))
                                except Exception:  # noqa: BLE001
                                    pass
                        elif phase == "execute":
                            execute_buf.append(text)
                            # 更新当前阶段
                            if _current_phase != "execute":
                                _current_phase = "execute"
                                if "review" not in _completed_phases:
                                    _completed_phases.append("review")
                                try:
                                    update_agent_session(
                                        conn, session_id,
                                        current_phase="execute",
                                        completed_phases=json.dumps(_completed_phases),
                                    )
                                except Exception:  # noqa: BLE001
                                    pass
                            # Save review text once execute starts (phase transition checkpoint)
                            if len(execute_buf) == 1 and review_buf:
                                try:
                                    update_agent_session(conn, session_id, review_text="".join(review_buf))
                                except Exception:  # noqa: BLE001
                                    pass
                        elif phase == "gather":
                            # 更新当前阶段
                            if _current_phase != "gather":
                                _current_phase = "gather"
                                try:
                                    update_agent_session(conn, session_id, current_phase="gather")
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

                    elif etype == "done" and phase == "gather":
                        # Gather阶段完成，保存gather_context
                        if not _gather_context_saved:
                            _gather_context_saved = True
                            if "gather" not in _completed_phases:
                                _completed_phases.append("gather")
                            # 从conversation_turns中提取gather阶段的消息作为context
                            gather_msgs = []
                            for turn in conversation_turns:
                                if turn.get("phase") == "gather":
                                    gather_msgs.extend(turn.get("messages", []))
                            try:
                                update_agent_session(
                                    conn, session_id,
                                    current_phase="design",
                                    completed_phases=json.dumps(_completed_phases),
                                    gather_context_json=json.dumps(gather_msgs, ensure_ascii=False),
                                )
                            except Exception:  # noqa: BLE001
                                pass

                    elif etype == "done" and phase == "execute":
                        # All three phases complete — final save
                        if "execute" not in _completed_phases:
                            _completed_phases.append("execute")
                        # 优先使用事件中携带的 design/review 文本（恢复模式下 design_buf/review_buf 可能为空）
                        _design_final = str(raw.get("design") or "") or "".join(design_buf)
                        _review_final = str(raw.get("review") or "") or "".join(review_buf)
                        try:
                            update_agent_session(
                                conn, session_id,
                                status="done",
                                design_text=_design_final,
                                review_text=_review_final,
                                execute_text=str(raw.get("full_text", "".join(execute_buf))),
                                tools_json=json.dumps(tools, ensure_ascii=False),
                                events_json=json.dumps(tracked_events, ensure_ascii=False),
                                messages_json=json.dumps(conversation_turns, ensure_ascii=False),
                                completed_phases=json.dumps(_completed_phases),
                                finished=True,
                            )
                        except Exception:  # noqa: BLE001
                            pass

                    elif etype == "error":
                        # 保存错误状态和当前进度，以便恢复
                        _persist_error_state(str(raw.get("message", "")))

                except Exception:  # noqa: BLE001 — never crash the stream
                    pass

            yield chunk

    except GeneratorExit:
        _persist_error_state("client_disconnected")
    except Exception as exc:  # noqa: BLE001
        _persist_error_state(f"unexpected_stream_error: {exc}")


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

    # Persist session for ALL agent runs (so AgentMonitor can browse project history).
    # init/recovery use the explicit step_id; maintain runs use synthetic step_id "maintain"
    step_id = body.step_id or (
        body.failure_context.get("step_id") if body.failure_context else None
    )
    if not step_id:
        step_id = body.mode  # "maintain" / "init" / "recovery"
    
    # 检查是否有可恢复的session
    resume_context: Optional[Dict[str, Any]] = None
    try:
        resume_context = get_resumable_session(p.conn, step_id)
        if resume_context:
            # 标记旧session为已恢复（避免重复恢复）
            old_session_id = resume_context.get("session_id")
            if old_session_id:
                update_agent_session(p.conn, old_session_id, status="resumed")
    except Exception:  # noqa: BLE001
        resume_context = None
    
    session_id: Optional[int] = None
    try:
        session_id = create_agent_session(p.conn, step_id)
    except Exception:  # noqa: BLE001 — don't break streaming if session creation fails
        pass

    gen = run_agent_sse(
        body.message,
        p,
        mode=body.mode,
        strict_review=body.strict_review,
        failure_context=body.failure_context,
        model=_project_model,
        session_id=session_id,
        resume_context=resume_context,
    )

    try:
        if session_id:
            gen = _session_tracking_wrapper(gen, p.conn, session_id)
    except Exception:  # noqa: BLE001
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


# ─── 维护 Agent API ─────────────────────────────────────────────────────────


class MaintainChatBody(BaseModel):
    message: str = Field(min_length=1, max_length=8000)
    project_id: str = Field(default="", description="项目 slug / ID")
    session_id: Optional[int] = Field(
        default=None,
        description="None 表示新建会话；传入已有 session_id 则继续对话",
    )
    current_table: Optional[str] = Field(
        default=None,
        description="用户当前查看的表名，注入到 Agent 上下文",
    )
    cell_selection: Optional[str] = Field(
        default=None,
        description="用户当前选中的单元格/区域描述（如 '表 equip_base 的 main_hand_purple 行, atk 列，值: 150'）",
    )


@router.post("/maintain/chat")
def maintain_chat(
    body: MaintainChatBody,
    p: ProjectDB = Depends(get_project_write),
):
    """维护 Agent 主聊天接口（SSE 流式）。"""
    if not DASHSCOPE_API_KEY and not DEEPSEEK_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="未配置任何 AI API Key",
        )

    # 确保维护会话表存在
    try:
        init_maintain_sessions_table(p.conn)
    except Exception:  # noqa: BLE001
        pass

    # 读取项目绑定的 AI 模型
    _project_model: Optional[str] = None
    try:
        row = p.conn.execute(
            "SELECT value_json FROM project_settings WHERE key = 'agent_model'"
        ).fetchone()
        if row:
            _project_model = json.loads(row[0]) if row[0] else None
    except Exception:  # noqa: BLE001
        pass

    # 会话管理
    session_id = body.session_id
    session_messages: List[Dict[str, Any]] = []

    if session_id is not None:
        # 继续已有会话
        try:
            session_messages = get_maintain_session_messages(p.conn, session_id)
        except Exception:  # noqa: BLE001
            session_messages = []
    else:
        # 新建会话
        try:
            session_id = create_maintain_session(p.conn, body.message)
        except Exception:  # noqa: BLE001
            session_id = None

    # 运行维护 Agent
    gen = run_maintain_agent_sse(
        body.message,
        p.conn,
        project_db=p,
        server_conn=p.server_conn,
        current_table=body.current_table,
        cell_selection=body.cell_selection,
        session_messages=session_messages,
        model=_project_model,
    )

    def _tracked_gen():
        # 第一个事件：把 session_id 发回客户端，保证同一窗口多轮对话连续
        if session_id is not None:
            yield sse_event({"type": "session_init", "session_id": session_id})

        chat_history: List[Dict[str, Any]] = [
            {"role": "user", "content": body.message},
        ]
        assistant_content = ""
        tool_call_results: List[Dict[str, Any]] = []

        for chunk in gen:
            yield chunk
            try:
                line = chunk.decode("utf-8").strip()
                if line.startswith("data: "):
                    data = json.loads(line[6:])
                    etype = data.get("type", "")
                    if etype == "token":
                        assistant_content += data.get("text", "")
                    elif etype == "tool_call":
                        tool_call_results.append({
                            "type": "tool_call",
                            "name": data.get("name", ""),
                            "arguments": data.get("arguments", ""),
                        })
                    elif etype == "tool_result":
                        tool_call_results.append({
                            "type": "tool_result",
                            "name": data.get("name", ""),
                            "result": data.get("result", ""),
                        })
                    elif etype == "done":
                        msg = {"role": "assistant", "content": assistant_content}
                        if tool_call_results:
                            msg["tool_details"] = tool_call_results
                        chat_history.append(msg)
                        try:
                            if session_id:
                                append_maintain_session_messages(p.conn, session_id, chat_history)
                        except Exception:  # noqa: BLE001
                            pass
            except Exception:  # noqa: BLE001
                pass

    return StreamingResponse(_tracked_gen(), media_type="text/event-stream")


@router.post("/maintain/sessions/{maint_session_id}/generate_title")
def maintain_session_generate_title(
    maint_session_id: int,
    p: ProjectDB = Depends(get_project_write),
) -> Dict[str, Any]:
    """为维护会话生成简短 AI 标题并更新 DB，返回生成的标题。"""
    # 读取首条用户消息
    try:
        msgs = get_maintain_session_messages(p.conn, maint_session_id)
    except Exception:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="session not found")

    first_user = next((m.get("content", "") for m in msgs if m.get("role") == "user"), "")
    if not first_user:
        raise HTTPException(status_code=400, detail="no user message found")

    # 读取项目绑定模型
    _project_model: Optional[str] = None
    try:
        row = p.conn.execute(
            "SELECT value_json FROM project_settings WHERE key = 'agent_model'"
        ).fetchone()
        if row:
            _project_model = json.loads(row[0]) if row[0] else None
    except Exception:  # noqa: BLE001
        pass

    title_msgs = [
        {
            "role": "system",
            "content": (
                "根据用户的消息，用4到10个字给本次对话起一个简洁标题。"
                "只输出标题文字，不要引号、序号或任何其他内容。"
            ),
        },
        {"role": "user", "content": first_user[:500]},
    ]
    try:
        title, _ = qwen_client.chat_once(
            title_msgs,
            temperature=0,
            max_tokens=24,
            model=_project_model,
        )
        title = title.strip().strip('"\'《》').strip()[:60]
        if not title:
            raise ValueError("empty title")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"title generation failed: {exc}") from exc

    rename_maintain_session(p.conn, maint_session_id, title)
    return {"session_id": maint_session_id, "session_name": title}


@router.get("/maintain/sessions")
def maintain_sessions(
    limit: int = 30,
    p: ProjectDB = Depends(get_project_read),
) -> Dict[str, Any]:
    """列出项目的维护会话列表。"""
    try:
        init_maintain_sessions_table(p.conn)
    except Exception:  # noqa: BLE001
        pass
    try:
        sessions = list_maintain_sessions(p.conn, limit=max(1, min(int(limit), 100)))
    except Exception:  # noqa: BLE001
        sessions = []
    return {"sessions": sessions}


@router.get("/maintain/sessions/{maint_session_id}")
def maintain_session_detail(
    maint_session_id: int,
    p: ProjectDB = Depends(get_project_read),
) -> Dict[str, Any]:
    """获取单个维护会话的完整消息历史。"""
    try:
        messages = get_maintain_session_messages(p.conn, maint_session_id)
    except Exception:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="session not found")
    return {"session_id": maint_session_id, "messages": messages}


@router.delete("/maintain/sessions/{maint_session_id}")
def maintain_session_delete(
    maint_session_id: int,
    p: ProjectDB = Depends(get_project_write),
) -> Dict[str, Any]:
    """删除维护会话。"""
    try:
        delete_maintain_session(p.conn, maint_session_id)
    except Exception:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="session not found")
    return {"ok": True}


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
