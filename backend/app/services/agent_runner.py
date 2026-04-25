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
    "execute_formula",
    "recalculate_downstream",
    "update_table_readme",
    "update_global_readme",
    "set_project_setting",
    "call_algorithm_api",
    "bulk_register_and_compute",
    "setup_level_table",
    "create_snapshot",
}

# 工具名称 → 中文标签（用于前端监控显示）
_TOOL_LABELS: Dict[str, str] = {
    "create_table": "创建数值表",
    "read_table": "读取表数据",
    "write_cells": "写入单元格",
    "delete_table": "删除表",
    "list_tables": "列举所有表",
    "register_formula": "注册公式",
    "execute_formula": "执行公式",
    "recalculate_downstream": "重算下游依赖",
    "update_table_readme": "更新表 README",
    "update_global_readme": "更新全局 README",
    "get_readme": "读取 README",
    "validate_table": "校验表数据",
    "get_validation_report": "获取校验报告",
    "create_snapshot": "创建快照",
    "list_snapshots": "列举快照",
    "restore_snapshot": "还原快照",
    "bulk_register_and_compute": "批量注册公式",
    "setup_level_table": "构建等级表",
    "create_dynamic_table": "创建动态表",
    "call_algorithm_api": "调用算法库",
    "get_cell_provenance": "查询单元格来源",
    "list_formulas": "列举公式",
    "get_formula_detail": "查看公式详情",
    "delete_formula": "删除公式",
    "read_project_settings": "读取项目配置",
    "set_project_setting": "更新项目配置",
    "global_search": "全局搜索",
    "suggest_action": "获取 Agent 建议",
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
            "公式引用语法：@表名[列名]=逐行取同行值（数学计算用）；@@表名[列名]=整列数组（VLOOKUP/INDEX/MATCH/SUM/AVERAGE 等查找聚合用）。\n"
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
    "【当前阶段=execute】按 review 最终方案执行工具调用。\n\n"

    "═══ ★ 每回合固定格式（违反=低质量）★ ═══\n"
    "每次生成工具调用时，**必须先输出以下两行，再调工具**：\n"
    "  第1行: `当前: [x]已完成项目 | [ ]本轮目标`（用你的 TODO 状态）\n"
    "  第2行: `行动: <本轮操作，15字内>`\n"
    "例：`当前: [x]读配置 | [ ]创建角色表` / `行动: 创建角色_养成分配`\n\n"

    "═══ ★ TODO 清单（第一回合必须先输出，再调工具）★ ═══\n"
    "格式：`- [ ] 任务` / `- [x] 已完成` / `- [!] 阻塞：<原因>`\n"
    "规则：所有 `- [ ]` 变成 `[x]` 或 `[!]` 后，才能输出最终总结。\n\n"

    "═══ ★ 错误处理协议 ★ ═══\n"
    "工具返回 error / ok=false 时，**禁止直接重试**，必须先输出：\n"
    "  `失败: <根因，20字内>` / `绕行: <替代方案，20字内>`\n"
    "同一操作失败 2 次 → 标记 `- [!] 阻塞` → 立即跳到下一 TODO 项。\n\n"

    "═══ ★ 效率硬规则 ★ ═══\n"
    "① 等级/数值序列（规律递增/递减/公式可算）→ 必须用 setup_level_table 或 bulk_register_and_compute，**禁止** write_cells 逐行写。\n"
    "② write_cells 只用于：分类标签、名称、描述、少量手工配置等**非规律内容**。\n"
    "③ 同一回合内所有独立工具 → **一次性并行调用**，不要一个一个排队。\n"
    "④ setup_level_table：所有列公式同时放入 columns 数组，一次调用完成。\n"
    "⑤ write_cells 单次 ≤30 行，超出分多次调用。\n"
    "⑥ 最终总结：必须包含 TODO 完成状态 + executed_count/rows_updated 关键数字。"
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
    model: Optional[str] = None,
) -> Generator[bytes, None, str]:
    """无工具的纯文本阶段：调用一次模型，按 token 切片 emit；返回完整文本。"""
    resp = client.chat.completions.create(
        model=model or QWEN_MODEL,
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


def _reviewer_check(client, tool_name: str, tool_args: str, *, model: Optional[str] = None) -> Dict[str, Any]:
    """轻量 reviewer：返回 {'verdict': 'approve'|'reject', 'reason': str}。"""
    try:
        resp = client.chat.completions.create(
            model=model or QWEN_MODEL,
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


def _make_state_anchor(
    round_i: int,
    user_message: str,
    success_count: int,
    error_count: int,
    is_after_error: bool = False,
) -> str:
    """生成状态锚点消息，注入到 execute_messages 让模型重新定向。

    小模型在长对话中注意力会偏向近期内容而遗忘系统提示，
    将当前状态作为 user message 注入可在上下文末尾提供强有力的重定向信号。
    """
    prefix = "⚠ 错误恢复检查" if is_after_error else f"── 第 {round_i} 轮状态检查"
    return (
        f"[{prefix}]\n"
        f"本轮已调用工具：成功 {success_count} 次，失败 {error_count} 次。\n"
        f"原始任务：{user_message[:200]}\n"
        "提醒：\n"
        "① 查看你的 TODO 清单，找到下一个 `- [ ]` 项继续执行\n"
        "② 每次工具调用前先输出：`当前: [状态] | 行动: <目标>`\n"
        "③ 遇错先输出 `失败: <根因>` / `绕行: <方案>`，再调工具\n"
        "④ 所有 TODO 为 [x] 或 [!] 后才能结束\n"
        "继续执行未完成的 TODO 项。"
    )


# ---------- main entry ----------

def run_agent_sse(
    user_message: str,
    p: ProjectDB,
    *,
    mode: str = "maintain",
    strict_review: bool = False,
    failure_context: Optional[Dict[str, Any]] = None,
    model: Optional[str] = None,
) -> Generator[bytes, None, None]:
    _model = model or QWEN_MODEL
    # recovery 模式：专门用于分析失败原因并尝试修复
    if mode == "recovery" and failure_context:
        yield from _run_recovery_sse(user_message, p, failure_context, model=_model)
        return

    mode_norm = mode if mode in ("init", "maintain") else "maintain"
    role_label = "初始化 Agent" if mode_norm == "init" else "维护 Agent"
    yield _emit("route", {"type": "log", "message": f"开始调度 Agent（{role_label}）"})

    client = get_client()

    # ---- prompt 路由 ----
    step_id = _current_step_id(p)
    cfg_summary = _project_config_summary(p)
    yield _emit("route", {"type": "log", "message": f"提示词路由：step={step_id or '(none)'}"})
    try:
        route = route_prompt(step_id, user_message, cfg_summary, model=_model)
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
            model=_model,
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
            model=_model,
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
    final_text = ""
    round_i = 0
    consec_errors = 0   # 连续错误计数（重置于成功）
    total_errors = 0    # 累计错误（不重置）
    total_success = 0   # 累计成功
    MAX_CONSEC_ERRORS = 4  # 连续失败4次强制注入分析提示
    while True:
        round_i += 1

        # ---- 每20轮发出一次进度警告（不强制终止）----
        if round_i > 1 and round_i % 20 == 0:
            yield _emit("execute", {"type": "log", "message": f"⏱ 已进行 {round_i} 轮推理，成功={total_success} 失败={total_errors}"})

        # ---- 每5轮注入状态锚点（防止小模型目标漂移）----
        if round_i > 1 and round_i % 5 == 0:
            anchor = _make_state_anchor(round_i, user_message, total_success, total_errors)
            execute_messages.append({"role": "user", "content": anchor})
            yield _emit("execute", {"type": "log", "message": f"第 {round_i} 轮：注入状态锚点"})

        yield _emit(
            "execute",
            {"type": "log", "message": f"模型推理轮次 {round_i}"},
        )
        try:
            resp = client.chat.completions.create(
                model=_model,
                messages=execute_messages,
                tools=TOOLS_OPENAI,
                tool_choice="auto",
                parallel_tool_calls=True,
                temperature=0.2,
                max_tokens=8192,
            )
        except Exception as e:  # noqa: BLE001
            yield _emit("execute", {"type": "error", "message": f"execute 调用失败: {e!r}"})
            return
        choice = resp.choices[0]
        finish_reason = getattr(choice, "finish_reason", None) or ""
        msg = choice.message
        tool_calls = getattr(msg, "tool_calls", None) or []

        # ---- 输出被 token 限制截断：注入修复提示让模型重新分批 ----
        if finish_reason == "length":
            yield _emit("execute", {"type": "log", "message": "⚠ 模型输出被 max_tokens 截断，注入重试提示"})
            execute_messages.append({"role": "assistant", "content": msg.content or ""})
            execute_messages.append({
                "role": "user",
                "content": (
                    "你的上一次输出因超过 token 限制而被截断，部分工具调用参数不完整。"
                    "请重新规划并分批执行：\n"
                    "1. 优先使用 register_formula/bulk_register_and_compute 替代逐行 write_cells\n"
                    "2. 若必须 write_cells，每次不超过 30 行（分多轮调用）\n"
                    "3. 重新生成完整的工具调用参数"
                ),
            })
            continue

        if tool_calls:
            def _safe_args(raw: Optional[str]) -> str:
                """确保 function.arguments 是合法 JSON，防止下一轮请求被 DashScope 400 拒绝。
                截断的 JSON 用 {"_truncated": true} 标记，让模型感知并重试。"""
                if not raw:
                    return "{}"
                try:
                    json.loads(raw)
                    return raw
                except json.JSONDecodeError:
                    # 保留截断信息，而非静默丢弃
                    return json.dumps({"_truncated": True, "_raw_prefix": raw[:120]}, ensure_ascii=False)

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
                call_id = tc.id
                label = _TOOL_LABELS.get(name, name)
                yield _emit(
                    "execute",
                    {"type": "tool_call", "call_id": call_id, "name": name, "label": label, "arguments": args},
                )

                # ---- 可选 reviewer 旁路 ----
                if strict_review and name in WRITE_TOOLS:
                    verdict_obj = _reviewer_check(client, name, args, model=_model)
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
                        yield _emit(
                            "execute",
                            {"type": "tool_result", "call_id": call_id, "name": name, "status": "error",
                             "preview": reject_payload[:500], "hint": "reviewer 拒绝"},
                        )
                        continue

                try:
                    result = dispatch_tool(name, args, p)
                except Exception as tool_exc:  # noqa: BLE001
                    # 工具执行异常（含 sqlite3.OperationalError 等）转为错误 JSON
                    # 返回给 LLM，让 Agent 自行决策（而不是崩溃整个流）
                    err_msg = f"工具执行异常: {tool_exc!r}"
                    yield _emit("execute", {"type": "log", "message": err_msg})
                    result = json.dumps({"ok": False, "error": err_msg}, ensure_ascii=False)

                tool_status = "error" if ('"status": "error"' in result or '"ok": false' in result.lower()) else "success"
                if tool_status == "error":
                    consec_errors += 1
                    total_errors += 1
                else:
                    consec_errors = 0
                    total_success += 1
                yield _emit(
                    "execute",
                    {
                        "type": "tool_result",
                        "call_id": call_id,
                        "name": name,
                        "status": tool_status,
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
            # ---- 错误后立即注入状态锚点（首次错误时触发）----
            if consec_errors == 1:
                anchor = _make_state_anchor(round_i, user_message, total_success, total_errors, is_after_error=True)
                execute_messages.append({"role": "user", "content": anchor})
                yield _emit("execute", {"type": "log", "message": "注入错误恢复锚点"})
            # ---- 连续失败达阈值：强制要求阻塞分析 ----
            if consec_errors >= MAX_CONSEC_ERRORS:
                stop_msg = f"⚠ 连续 {MAX_CONSEC_ERRORS} 次失败，注入强制阻塞分析提示"
                yield _emit("execute", {"type": "log", "message": stop_msg})
                execute_messages.append({
                    "role": "user",
                    "content": (
                        f"STOP — 你已连续 {MAX_CONSEC_ERRORS} 次工具调用失败。\n"
                        "必须立即输出：\n"
                        "  `失败: <根本原因，20字>` \n"
                        "  `阻塞: <将受阻 TODO 标记为 [!]>`\n"
                        "  `绕行: <替代方案或放弃该项继续下一项>`\n"
                        "然后继续完成其余未阻塞的 TODO 项。禁止重试相同失败操作。"
                    ),
                })
                consec_errors = 0
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


# ─── Recovery Agent ──────────────────────────────────────────────────────────

_RECOVERY_SYSTEM = """\
【角色】你是 Numflow「修复 Agent」（Recovery Agent）。你的工作是分析一次失败的 pipeline 步骤，找出根本原因，并通过调用工具执行必要的修复操作，使该步骤能够在下次重试时成功。

【工作流程】
1. 分析：阅读失败上下文（失败步骤、错误信息、已完成的工具调用历史），诊断根本原因。
2. 修复计划：列出需要执行的修复操作（可能包括：删除已部分创建的冲突表、清理脏数据、更新 README 等）。
3. 执行修复：调用工具完成修复；调用顺序：先清理冲突→再补全遗漏→最后验证状态。
4. 汇报：输出结构化修复报告，明确说明已修复内容、未能修复内容、建议的重试策略。

【约束】
- 只做清理/恢复性操作，不做超出失败步骤范围的新建工作（新建工作留给重试）。
- 遇到无法自动修复的问题，必须明确在报告中说明，不要无限重试。
- 修复报告末尾必须以 RECOVERY_DONE 或 RECOVERY_PARTIAL 或 RECOVERY_FAILED 结尾（机器可读信号）。
"""


def _run_recovery_sse(
    user_message: str,
    p: ProjectDB,
    failure_context: Dict[str, Any],
    *,
    model: Optional[str] = None,
) -> Generator[bytes, None, None]:
    """Recovery Agent SSE：分析失败上下文，调用工具修复，输出修复报告。"""
    _model = model or QWEN_MODEL
    client = get_client()

    step_id = failure_context.get("step_id", "unknown")
    error_msg = failure_context.get("error", "")
    tool_history = failure_context.get("tool_history", [])  # [{name, arguments, result}]
    partial_design = failure_context.get("partial_design", "")

    yield _emit("route", {"type": "log", "message": f"修复 Agent 启动（失败步骤: {step_id}）"})
    yield _emit("route", {
        "type": "prompt_route",
        "hit": True,
        "prompt": "recovery",
        "rationale": f"失败步骤={step_id}，错误={error_msg[:200]}",
        "step_id": step_id,
    })

    # ─── 构建上下文消息 ─────────────────────────────────────────
    context_lines = [
        f"## 失败步骤\n{step_id}",
        f"## 错误信息\n```\n{error_msg}\n```",
    ]
    if partial_design:
        context_lines.append(f"## 失败前 design 阶段输出（部分）\n{partial_design[:1500]}")
    if tool_history:
        context_lines.append("## 失败前工具调用历史")
        for i, th in enumerate(tool_history[-10:]):  # 最多显示最近10条
            context_lines.append(
                f"### 工具 {i+1}: {th.get('name','?')}\n"
                f"参数: {str(th.get('arguments', {}))[:300]}\n"
                f"结果: {str(th.get('result', ''))[:300]}"
            )

    context_block = "\n\n".join(context_lines)

    # ─── design 阶段：分析失败原因 ─────────────────────────────
    design_messages: List[Dict[str, Any]] = [
        {"role": "system", "content": _RECOVERY_SYSTEM},
        {
            "role": "system",
            "content": (
                "【当前阶段=design（修复分析）】\n"
                "仔细阅读下方失败上下文，输出两段式分析（禁止工具调用）：\n"
                "## 根本原因分析\n（具体说明为何失败，涉及哪些表/工具/数据）\n"
                "## 修复计划\n（按顺序列出每个修复操作，说明调用哪个工具、参数是什么）"
            ),
        },
        {"role": "user", "content": f"以下是失败上下文：\n\n{context_block}\n\n原始失败消息：{user_message}"},
    ]

    yield _emit("design", {"type": "log", "message": "修复分析阶段开始…"})
    design_text = ""
    try:
        stream = client.chat.completions.create(
            model=_model,
            messages=design_messages,
            temperature=0.1,
            max_tokens=1000,
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
        yield _emit("design", {"type": "error", "message": f"修复分析失败: {e!r}"})
        return
    design_text = design_text.strip()
    yield _emit("design", {"type": "log", "message": f"修复分析完成（{len(design_text)} chars）"})

    # ─── execute 阶段：执行修复操作 ────────────────────────────
    yield _emit("execute", {"type": "log", "message": "修复执行阶段开始…"})

    execute_messages: List[Dict[str, Any]] = [
        {"role": "system", "content": _RECOVERY_SYSTEM},
        {
            "role": "system",
            "content": (
                "【当前阶段=execute（修复执行）】\n"
                "按修复计划调用工具执行修复操作；完成后输出修复报告。\n"
                "报告末尾必须有且仅有一个状态标记（单独一行）：\n"
                "- RECOVERY_DONE：所有修复已完成，可以安全重试原步骤\n"
                "- RECOVERY_PARTIAL：部分修复完成，重试原步骤可能成功\n"
                "- RECOVERY_FAILED：无法自动修复，需要人工介入"
            ),
        },
        {"role": "user", "content": f"失败上下文：\n{context_block}"},
        {"role": "assistant", "content": design_text},
        {"role": "user", "content": "请按照修复计划执行修复操作，完成后输出修复报告。"},
    ]

    recovery_text = ""
    _round = 0

    while True:
        _round += 1
        try:
            resp = client.chat.completions.create(
                model=_model,
                messages=execute_messages,
                tools=TOOLS_OPENAI,
                tool_choice="auto",
                parallel_tool_calls=True,
                temperature=0.1,
                max_tokens=8192,
            )
        except Exception as e:  # noqa: BLE001
            yield _emit("execute", {"type": "error", "message": f"修复执行调用失败: {e!r}"})
            return

        msg = resp.choices[0].message if resp.choices else None
        if msg is None:
            break

        execute_messages.append({"role": "assistant", "content": msg.content or "", "tool_calls": [
            {"id": tc.id, "type": "function", "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
            for tc in (msg.tool_calls or [])
        ]})

        if not msg.tool_calls:
            recovery_text = msg.content or ""
            for chunk in _chunk_text(recovery_text, 80):
                yield _emit("execute", {"type": "token", "text": chunk})
            # 判断修复结果
            if "RECOVERY_DONE" in recovery_text:
                status = "done"
            elif "RECOVERY_PARTIAL" in recovery_text:
                status = "partial"
            else:
                status = "failed"
            yield _emit("execute", {
                "type": "done",
                "full_text": recovery_text,
                "design": design_text,
                "review": "",
                "recovery_status": status,
            })
            return

        # 执行工具调用
        for tc in msg.tool_calls:
            try:
                name = tc.function.name
                args: Dict[str, Any] = {}
                try:
                    args = json.loads(tc.function.arguments or "{}")
                except json.JSONDecodeError:
                    pass
                call_id = tc.id
                label = _TOOL_LABELS.get(name, name)
                yield _emit("execute", {
                    "type": "tool_call",
                    "call_id": call_id,
                    "name": name,
                    "label": label,
                    "arguments": tc.function.arguments or "{}",
                })
                try:
                    result = dispatch_tool(name, args, p)
                except Exception as tool_exc:  # noqa: BLE001
                    err_msg = f"工具执行异常: {tool_exc!r}"
                    yield _emit("execute", {"type": "log", "message": err_msg})
                    result = json.dumps({"ok": False, "error": err_msg}, ensure_ascii=False)
                tool_status = "error" if ('"status": "error"' in result or '"ok": false' in result.lower()) else "success"
                yield _emit("execute", {
                    "type": "tool_result",
                    "call_id": call_id,
                    "name": name,
                    "status": tool_status,
                    "preview": result[:2000],
                })
                execute_messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result,
                })
            except Exception as e:  # noqa: BLE001
                yield _emit("execute", {"type": "log", "message": f"工具循环异常: {e!r}"})

