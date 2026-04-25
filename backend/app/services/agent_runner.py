"""Agent 调度：design → review → execute 三阶段 SSE，含可选 reviewer 旁路。"""

from __future__ import annotations

import json
from typing import Any, Dict, Generator, Iterable, List, Optional

from app.config import QWEN_MODEL
from app.db.project_schema import get_pipeline_state
from app.deps import ProjectDB
from app.services.agent_tools import TOOLS_OPENAI, dispatch_tool, _get_project_config
from app.services.prompt_router import route_prompt
from app.services.qwen_client import get_client


WRITE_TOOLS = {
    "write_cells",
    "create_table",
    "delete_table",
    "register_formula",
    "recalculate_downstream",
    "update_table_readme",
    "update_global_readme",
}


def sse_event(obj: Dict[str, Any]) -> bytes:
    return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n".encode("utf-8")


def _emit(phase: str, obj: Dict[str, Any]) -> bytes:
    payload = {"phase": phase, **obj}
    return sse_event(payload)


def _chunk_text(text: str, size: int) -> Iterable[str]:
    for i in range(0, len(text), size):
        yield text[i : i + size]


# ---------- system prompts ----------

def _role_block(mode_norm: str) -> str:
    if mode_norm == "init":
        return (
            "【1/4 角色】你是 Numflow「初始化 Agent」。职责：根据项目配置推导待建表、依赖与 README，"
            "按文档 03 顺序分阶段推进；未经用户确认前不执行破坏性写操作；不得跳阶段。\n"
            "约束：不覆盖 user_manual 单元格（工具层会跳过并返回 skipped）。"
        )
    return (
        "【1/4 角色】你是 Numflow「维护 Agent」。职责：理解变更—定界影响—执行写入—验证—更新 README。\n"
        "约束：写入前用 get_protected_cells / read_cell 确认范围；不覆盖用户手动格；不确定时先提问。"
    )


def _common_system(mode_norm: str) -> str:
    role_block = _role_block(mode_norm)
    return "\n".join(
        [
            role_block,
            "【2/4 项目上下文】每次任务先 get_project_config，再按需 get_dependency_graph、get_table_readme；"
            "勿假设未读到的表结构。新建或修改「*系统_落地」、各子系统**行轴**、消耗与属性投放时，**须先** "
            "get_default_system_rules 对照 02 机读默认；禁止各系统无差别复用同一张仅「标准等级+两列消耗」的落地模板。 "
            "宝石的默认数据轴是**品阶/合成**（3 同阶→1 高 1 品）与**解锁门槛/属性池/分配**列，**不是**把标准等级 1..N 逐行 1:1 当成「宝石 N 级表」。"
            "坐骑/副本须体现开放等级与 02 约定（如坐骑 30 级、副本默认门槛等），列里要有玩法含义而非只有金币+掉率。",
            "【3/4 工具规范】读工具可自由组合；写表/写 README/公式/算法调用仅在 execute 阶段且需写权限；"
            "每次写入必须带合法 source_tag；大批量用 read_table 的 limit/columns 切片。\n"
            "工具 JSON 固定含字段：status（success|error|partial）、data、warnings、blocked_cells；"
            "遇 partial/error 须阅读 warnings/blocked_cells 再决定是否继续。",
            "【4/4 输出与流程】本次会话严格按三阶段执行：design → review → execute。\n"
            "  · design 阶段：必须用以下三段式 CoT 输出，禁止任何工具调用：\n"
            "      ## 1. 我对用户需求的理解\n"
            "      ## 2. 我对游戏性的设计理解（结合 02-系统与子系统默认细则与项目核心定义）\n"
            "      ## 3. 我的最终设计\n"
            "  · review 阶段：把 design 输出再喂回，自审找问题并给出最终操作方案，仍禁止工具调用；你需要意识到第二轮会被强制要求自审。\n"
            "  · execute 阶段：才允许调用工具；先简述计划再调用，最终回答简洁并引用工具结果 data 的关键字段。\n"
            "【README 必含字段】任何写 README 的工具调用必须覆盖：目的（goal）/上游输入/产出/必备表与列/验收标准/常见踩坑。",
        ]
    )


_DESIGN_SYSTEM_TAIL = (
    "【当前阶段=design】只输出三段式 CoT，**严禁**任何工具调用。"
    "格式必须严格使用三个二级标题：\n"
    "## 1. 我对用户需求的理解\n"
    "## 2. 我对游戏性的设计理解\n"
    "## 3. 我的最终设计\n"
    "在第 2 段中显式引用 02 默认细则与项目核心定义；第 3 段必须给出可执行的表/列/公式清单。"
)

_REVIEW_SYSTEM_TAIL = (
    "【当前阶段=review】对上一段 design 进行自审，必须严格使用以下两个二级标题：\n"
    "## 自审问题与风险\n"
    "（列出 design 中存在的问题、风险、与 02 默认细则的偏离）\n"
    "## 最终操作方案\n"
    "（修订后的最终操作方案：表名/列名/验收标准/写入顺序，必须可被 execute 阶段直接执行）\n"
    "**严禁**任何工具调用；两个标题缺一不可；标题字面必须为「最终操作方案」。"
)

_EXECUTE_SYSTEM_TAIL = (
    "【当前阶段=execute】按 review 给出的最终方案执行。允许调用工具；"
    "每步先简述本步骤目的，再调用相应工具；遇到 partial/error 优先读 warnings/blocked_cells 修正；"
    "全部完成后用简短自然语言总结，引用工具结果的关键字段。"
)


_REVIEWER_SYSTEM = (
    "你是 Numflow 写操作 Reviewer。你不会调用工具。"
    "给定一个即将执行的写工具调用（name + arguments JSON），"
    "判断是否安全/合理：是否覆盖 user_manual、是否带 source_tag、是否破坏依赖、是否违背 02 默认细则。"
    "返回严格 JSON：{\"verdict\":\"approve\"|\"reject\",\"reason\":\"<<=200字理由>\"}。"
)


# ---------- phase helpers ----------

def _stream_phase_text(
    client,
    messages: List[Dict[str, Any]],
    *,
    phase: str,
    max_tokens: int,
    temperature: float = 0.2,
) -> Generator[bytes, None, str]:
    """无工具的纯文本阶段：调用一次模型，按 token 切片 emit；返回完整文本。"""
    resp = client.chat.completions.create(
        model=QWEN_MODEL,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    text = (resp.choices[0].message.content or "").strip()
    for chunk in _chunk_text(text, 80):
        yield _emit(phase, {"type": "token", "text": chunk})
    return text


def _project_config_summary(p: ProjectDB) -> str:
    try:
        cfg = _get_project_config(p.conn)
        return json.dumps(cfg, ensure_ascii=False)
    except Exception as e:  # noqa: BLE001
        return f"(get_project_config 失败: {e!r})"


def _current_step_id(p: ProjectDB) -> str:
    try:
        st = get_pipeline_state(p.conn)
        cur = st.get("current_step") or ""
        if cur:
            return cur
        done = st.get("completed_steps") or []
        # fallback：未推进但已有 completed → 下一个
        from app.routers.pipeline import PIPELINE_STEPS
        n = len(done)
        return PIPELINE_STEPS[n] if n < len(PIPELINE_STEPS) else ""
    except Exception:
        return ""


def _reviewer_check(client, tool_name: str, tool_args: str) -> Dict[str, Any]:
    """轻量 reviewer：返回 {'verdict': 'approve'|'reject', 'reason': str}。"""
    try:
        resp = client.chat.completions.create(
            model=QWEN_MODEL,
            messages=[
                {"role": "system", "content": _REVIEWER_SYSTEM},
                {
                    "role": "user",
                    "content": f"tool_name: {tool_name}\narguments:\n{tool_args}",
                },
            ],
            temperature=0.1,
            max_tokens=512,
            response_format={"type": "json_object"},
        )
        raw = (resp.choices[0].message.content or "").strip()
        data = json.loads(raw)
        verdict = data.get("verdict", "approve")
        if verdict not in ("approve", "reject"):
            verdict = "approve"
        return {"verdict": verdict, "reason": str(data.get("reason") or "")[:400]}
    except Exception as e:  # noqa: BLE001
        return {"verdict": "approve", "reason": f"reviewer_fallback_approve: {e!r}"}


# ---------- main entry ----------

def run_agent_sse(
    user_message: str,
    p: ProjectDB,
    *,
    mode: str = "maintain",
    strict_review: bool = False,
) -> Generator[bytes, None, None]:
    mode_norm = mode if mode in ("init", "maintain") else "maintain"
    role_label = "初始化 Agent" if mode_norm == "init" else "维护 Agent"
    yield _emit("route", {"type": "log", "message": f"开始调度 Agent（{role_label}）"})

    client = get_client()

    # ---- prompt 路由 ----
    step_id = _current_step_id(p)
    cfg_summary = _project_config_summary(p)
    yield _emit("route", {"type": "log", "message": f"提示词路由：step={step_id or '(none)'}"})
    try:
        route = route_prompt(step_id, user_message, cfg_summary)
    except Exception as e:  # noqa: BLE001
        route = {
            "hit": False,
            "prompt": "",
            "rationale": f"route_exception: {e!r}",
        }
    yield _emit(
        "route",
        {
            "type": "prompt_route",
            "hit": bool(route.get("hit")),
            "prompt": route.get("prompt", ""),
            "rationale": route.get("rationale", ""),
            "step_id": step_id,
        },
    )

    base_system = _common_system(mode_norm)
    routed_prompt = (route.get("prompt") or "").strip()
    routed_block = (
        "【5/4 路由提示词】" + routed_prompt if routed_prompt else ""
    )

    # ---- 1) design ----
    design_messages: List[Dict[str, Any]] = [
        {"role": "system", "content": base_system},
    ]
    if routed_block:
        design_messages.append({"role": "system", "content": routed_block})
    design_messages.append({"role": "system", "content": _DESIGN_SYSTEM_TAIL})
    design_messages.append({"role": "user", "content": user_message})

    yield _emit("design", {"type": "log", "message": "design 阶段开始（无工具，三段式 CoT，流式）"})
    design_text = ""
    try:
        stream = client.chat.completions.create(
            model=QWEN_MODEL,
            messages=design_messages,
            temperature=0.2,
            max_tokens=1200,
            stream=True,
        )
        for chunk in stream:
            try:
                delta = chunk.choices[0].delta.content if chunk.choices else None
            except Exception:
                delta = None
            if delta:
                design_text += delta
                yield _emit("design", {"type": "token", "text": delta})
    except Exception as e:  # noqa: BLE001
        yield _emit("design", {"type": "error", "message": f"design 调用失败: {e!r}"})
        return
    design_text = design_text.strip()
    yield _emit("design", {"type": "log", "message": f"design 阶段结束（{len(design_text)} chars）"})

    # ---- 2) review ----
    review_messages: List[Dict[str, Any]] = [
        {"role": "system", "content": base_system},
    ]
    if routed_block:
        review_messages.append({"role": "system", "content": routed_block})
    review_messages.append({"role": "system", "content": _REVIEW_SYSTEM_TAIL})
    review_messages.append({"role": "user", "content": user_message})
    review_messages.append(
        {
            "role": "user",
            "content": "以下是 design 阶段的输出，请自审并给出最终操作方案：\n\n" + design_text,
        }
    )

    yield _emit("review", {"type": "log", "message": "review 阶段开始（无工具，自审，流式）"})
    review_text = ""
    try:
        stream = client.chat.completions.create(
            model=QWEN_MODEL,
            messages=review_messages,
            temperature=0.2,
            max_tokens=900,
            stream=True,
        )
        for chunk in stream:
            try:
                delta = chunk.choices[0].delta.content if chunk.choices else None
            except Exception:
                delta = None
            if delta:
                review_text += delta
                yield _emit("review", {"type": "token", "text": delta})
    except Exception as e:  # noqa: BLE001
        yield _emit("review", {"type": "error", "message": f"review 调用失败: {e!r}"})
        return
    review_text = review_text.strip()
    yield _emit("review", {"type": "log", "message": f"review 阶段结束（{len(review_text)} chars）"})

    # ---- 3) execute ----
    execute_messages: List[Dict[str, Any]] = [
        {"role": "system", "content": base_system},
    ]
    if routed_block:
        execute_messages.append({"role": "system", "content": routed_block})
    execute_messages.append({"role": "system", "content": _EXECUTE_SYSTEM_TAIL})
    execute_messages.append({"role": "user", "content": user_message})
    execute_messages.append(
        {
            "role": "assistant",
            "content": "[design]\n" + design_text + "\n\n[review]\n" + review_text,
        }
    )
    execute_messages.append(
        {
            "role": "user",
            "content": "请按上述 review 的最终操作方案执行（execute 阶段，可调用工具）。",
        }
    )

    yield _emit("execute", {"type": "log", "message": "execute 阶段开始（启用工具循环）"})
    max_rounds = 8
    final_text = ""
    for round_i in range(max_rounds):
        yield _emit(
            "execute",
            {"type": "log", "message": f"模型推理轮次 {round_i + 1}/{max_rounds}"},
        )
        try:
            resp = client.chat.completions.create(
                model=QWEN_MODEL,
                messages=execute_messages,
                tools=TOOLS_OPENAI,
                tool_choice="auto",
                temperature=0.2,
                max_tokens=2048,
            )
        except Exception as e:  # noqa: BLE001
            yield _emit("execute", {"type": "error", "message": f"execute 调用失败: {e!r}"})
            return
        choice = resp.choices[0]
        msg = choice.message
        tool_calls = getattr(msg, "tool_calls", None) or []

        if tool_calls:
            def _safe_args(raw: Optional[str]) -> str:
                """确保 function.arguments 是合法 JSON，防止下一轮请求被 DashScope 400 拒绝。"""
                if not raw:
                    return "{}"
                try:
                    json.loads(raw)
                    return raw
                except json.JSONDecodeError:
                    return "{}"

            execute_messages.append(
                {
                    "role": "assistant",
                    "content": msg.content or "",
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.function.name,
                                "arguments": _safe_args(tc.function.arguments),
                            },
                        }
                        for tc in tool_calls
                    ],
                }
            )

            for tc in tool_calls:
                name = tc.function.name
                args = tc.function.arguments or "{}"
                yield _emit(
                    "execute",
                    {"type": "tool_call", "name": name, "arguments": args},
                )

                # ---- 可选 reviewer 旁路 ----
                if strict_review and name in WRITE_TOOLS:
                    verdict_obj = _reviewer_check(client, name, args)
                    yield _emit(
                        "execute",
                        {
                            "type": "reviewer_verdict",
                            "name": name,
                            "verdict": verdict_obj["verdict"],
                            "reason": verdict_obj["reason"],
                        },
                    )
                    if verdict_obj["verdict"] == "reject":
                        reject_payload = json.dumps(
                            {
                                "status": "error",
                                "data": None,
                                "warnings": ["reviewer_rejected"],
                                "blocked_cells": [],
                                "reviewer_reason": verdict_obj["reason"],
                            },
                            ensure_ascii=False,
                        )
                        execute_messages.append(
                            {
                                "role": "tool",
                                "tool_call_id": tc.id,
                                "content": reject_payload,
                            }
                        )
                        continue

                result = dispatch_tool(name, args, p)
                yield _emit(
                    "execute",
                    {
                        "type": "tool_result",
                        "name": name,
                        "preview": result[:2000],
                        "hint": "检查 JSON 内 status/warnings/blocked_cells",
                    },
                )
                execute_messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": result,
                    }
                )
            continue

        final_text = msg.content or ""
        for chunk in _chunk_text(final_text, 80):
            yield _emit("execute", {"type": "token", "text": chunk})
        yield _emit(
            "execute",
            {
                "type": "done",
                "full_text": final_text,
                "design": design_text,
                "review": review_text,
            },
        )
        return

    yield _emit("execute", {"type": "error", "message": "超过最大工具轮次"})
