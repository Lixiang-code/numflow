"""维护 Agent 运行器 —— 自由工具调用循环 + 结束复查，无固定 COT 阶段。"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, Generator, List, Optional, Tuple

from app.config import QWEN_MODEL
from app.deps import ProjectDB
from app.services.agent_runner import (
    _build_assistant_msg,
    _chunk_text,
    _ending_review_prompt_text,
    _emit,
    _extract_text_tool_calls,
    _resolve_agent_system_prompt,
    _retry_llm_call,
    _tool_schema_payload,
    sse_event,
)
from app.services.agent_tools import (
    TOOLS_OPENAI,
    _get_project_config,
    build_tools_openai,
    dispatch_tool,
)
from app.services.qwen_client import get_client_for_model

# 工具标签（与 agent_runner.py 中的 _TOOL_LABELS 保持一致）
_TOOL_LABELS: Dict[str, str] = {
    "get_project_config": "读取项目配置",
    "get_table_list": "列出所有表",
    "get_table_schema": "读取表结构",
    "get_table_readme": "读取表 README",
    "read_table": "读取表数据",
    "read_3d_table": "读取 3D 表切片",
    "read_3d_table_full": "读取 3D 表全量",
    "sparse_sample": "稀疏采样",
    "list_directories": "列出目录",
    "list_calculators": "列出计算器",
    "call_calculator": "调用计算器",
    "get_dependency_graph": "查看依赖图",
    "get_gameplay_table_list": "列出玩法表",
    "get_gameplay_table_detail": "查看玩法表详情",
    "list_exposed_params": "查看暴露参数",
    "create_table": "创建表",
    "write_cells": "写入单元格",
    "write_cells_series": "序列写入",
    "add_column": "追加列",
    "add_columns": "批量追加列",
    "register_formula": "注册公式",
    "execute_formula": "执行公式",
    "recalculate_downstream": "重算下游",
    "update_table_readme": "更新表 README",
    "update_global_readme": "更新全局 README",
    "create_snapshot": "创建快照",
    "create_3d_table": "创建 3D 表",
    "create_matrix_table": "创建矩阵表",
    "write_matrix_cells": "写入矩阵单元格",
    "register_calculator": "注册计算器",
    "register_gameplay_table": "注册玩法表",
    "set_gameplay_table_status": "更新玩法表状态",
    "request_table_revision": "请求修订",
    "expose_param_to_subsystems": "暴露参数",
    "setup_level_table": "创建等级表",
    "delete_table": "删除表",
    "const_register": "注册常量",
    "const_list": "列出常量",
    "const_set": "修改常量",
    "const_delete": "删除常量",
    "const_tag_register": "注册常量标签",
    "const_tag_list": "列出常量标签",
    "glossary_register": "注册术语",
    "glossary_lookup": "查找术语",
    "glossary_list": "列出术语",
    "create_validation_rule": "创建校验规则",
    "confirm_validation_rule": "确认校验规则",
    "call_algorithm_api": "调用算法 API",
    "bulk_register_and_compute": "批量注册并计算",
    "set_project_setting": "修改项目设置",
    "get_protected_cells": "查看保护单元格",
    "list_skills": "列出技能",
    "get_skill_detail": "查看技能详情",
    "render_skill_file": "渲染技能文件",
    "submit_feedback": "提交反馈",
    "classify_table": "分类表",
}

_READ_TOOLS: set[str] = {
    "get_project_config", "get_table_list", "get_table_schema", "get_table_readme",
    "read_table", "read_3d_table", "read_3d_table_full", "sparse_sample",
    "list_directories", "list_calculators", "call_calculator",
    "get_dependency_graph", "get_gameplay_table_list", "get_gameplay_table_detail",
    "list_exposed_params", "const_list", "const_tag_list",
    "glossary_lookup", "glossary_list", "get_protected_cells",
    "list_skills", "get_skill_detail", "render_skill_file", "classify_table",
}
_WRITE_TOOLS: set[str] = {
    "create_table", "write_cells", "write_cells_series", "add_column", "add_columns",
    "register_formula", "execute_formula", "recalculate_downstream",
    "update_table_readme", "update_global_readme", "create_snapshot",
    "create_3d_table", "create_matrix_table", "write_matrix_cells",
    "register_calculator", "register_gameplay_table", "set_gameplay_table_status",
    "request_table_revision", "expose_param_to_subsystems", "setup_level_table",
    "delete_table", "const_register", "const_set", "const_delete",
    "const_tag_register", "glossary_register", "bulk_register_and_compute",
    "create_validation_rule", "confirm_validation_rule", "call_algorithm_api",
    "set_project_setting", "submit_feedback",
}
_ALL_TOOLS = _READ_TOOLS | _WRITE_TOOLS


# ─── system prompt ────────────────────────────────────────────────────────


def _table_context_block(conn: sqlite3.Connection, table_name: Optional[str]) -> str:
    """构建当前表上下文信息块。"""
    if not table_name:
        return "当前未指定具体表，用户可能在浏览项目全局结构。"
    try:
        schema_row = conn.execute(
            "SELECT schema_json, readme FROM _table_registry WHERE table_name = ?",
            (table_name,),
        ).fetchone()
        if not schema_row:
            return (
                f"用户当前正在查看表 `{table_name}`，但该表在 _table_registry 中不存在，"
                f"可能为新创建或外部表。"
            )
        schema_json_str, readme = schema_row
        schema = json.loads(schema_json_str) if schema_json_str else {}
        col_info = schema.get("columns", [])
        cols_list = ", ".join(
            f"{c.get('name', '?')} ({c.get('display_name', '?')})" for c in col_info[:12]
        )
        if len(col_info) > 12:
            cols_list += f" ...（共 {len(col_info)} 列）"

        row_count = conn.execute(
            f'SELECT COUNT(*) FROM "{table_name}"'
        ).fetchone()[0]

        lines = [
            f"【当前表上下文】用户正在查看表 `{table_name}`。",
            f"- 列（前12列）：{cols_list}",
            f"- 行数：{row_count}",
        ]
        if readme:
            lines.append(f"- README 摘要：{readme[:300]}")
        return "\n".join(lines)
    except Exception as e:  # noqa: BLE001
        return f"用户当前正在查看表 `{table_name}`（读取 schema 时出错：{e}）。"


def _directory_summary(conn: sqlite3.Connection) -> str:
    """构建项目目录结构速查。"""
    try:
        rows = conn.execute(
            "SELECT table_name, directory FROM _table_registry ORDER BY directory, table_name"
        ).fetchall()
        if not rows:
            return "（项目暂无表）"
        by_dir: Dict[str, List[str]] = {}
        for name, d in rows:
            d = d or "根目录"
            by_dir.setdefault(d, []).append(name)
        parts = []
        for d, names in sorted(by_dir.items()):
            sn = ", ".join(names[:10])
            if len(names) > 10:
                sn += f" ...（共 {len(names)} 张）"
            parts.append(f"  {d}/ {sn}")
        return "项目表结构：\n" + "\n".join(parts[:20])
    except Exception:  # noqa: BLE001
        return "（无法读取目录结构）"


def _project_brief(conn: sqlite3.Connection) -> str:
    """项目简要信息。"""
    try:
        cfg = _get_project_config(conn)
        core = cfg.get("settings", {}).get("fixed_layer_config", {}).get("core", {})
        max_lv = core.get("level_cap", cfg.get("settings", {}).get("max_level", "?"))
        df = core.get("defense_formula", "?")
        game = core.get("game_type", "?")
        attrs = cfg.get("settings", {}).get("stat_keys", [])
        attrs_str = ", ".join(attrs[:8]) if attrs else "?"
        if len(attrs) > 8:
            attrs_str += f" ...（共 {len(attrs)} 个）"
        return (
            f"【项目概况】max_level={max_lv}，防御公式={df}，"
            f"游戏类型={game}，属性={attrs_str}"
        )
    except Exception:  # noqa: BLE001
        return "（无法读取项目配置）"


def build_maintain_system(
    conn: sqlite3.Connection,
    current_table: Optional[str] = None,
    cell_selection: Optional[str] = None,
    global_conn: Optional[sqlite3.Connection] = None,
) -> str:
    """构建维护 Agent 的完整 system prompt。

    基础模板从提示词库读取（prompt_key=agent_maintain_system，支持覆盖），
    动态上下文（项目概况、目录结构、当前表、单元格选区）自动拼接在后。
    """
    # 从提示词覆盖系统读取基础模板
    base_template = _resolve_agent_system_prompt(conn, "agent_maintain_system", global_conn=global_conn)

    # 拼接动态上下文
    dynamic_parts: List[str] = []
    dynamic_parts.append(_project_brief(conn))
    dynamic_parts.append("")
    dynamic_parts.append(_directory_summary(conn))
    dynamic_parts.append("")
    dynamic_parts.append(_table_context_block(conn, current_table))
    if cell_selection:
        dynamic_parts.append("")
        dynamic_parts.append(
            f"【用户当前选中的单元格】{cell_selection}。"
            f"如果用户的修改请求与具体单元格相关，优先以此为参考。"
        )

    return base_template + "\n" + "\n".join(dynamic_parts)


# ─── 工具调用循环 ──────────────────────────────────────────────────────────


def run_maintain_agent_sse(
    user_message: str,
    conn: sqlite3.Connection,
    *,
    project_db: Any = None,
    server_conn: Optional[sqlite3.Connection] = None,
    current_table: Optional[str] = None,
    cell_selection: Optional[str] = None,
    session_messages: Optional[List[Dict[str, Any]]] = None,
    model: Optional[str] = None,
) -> Generator[bytes, None, None]:
    """维护 Agent 主流程：自由工具调用循环 + 结束复查。

    无固定 COT 阶段，Agent 自主决定何时调工具、何时回复用户。
    project_db 为完整的 ProjectDB 实例（含 row 属性），用于工具分发。
    """
    _model = model or QWEN_MODEL
    client = get_client_for_model(_model)

    # 用于工具调度的 ProjectDB（优先使用传入的完整实例）
    _dispatch_p: Any = project_db
    if _dispatch_p is None:
        _dispatch_p = ProjectDB(conn=conn, server_conn=server_conn, can_write=True)

    # 构建 system prompt
    system_content = build_maintain_system(conn, current_table, cell_selection, global_conn=server_conn)
    messages: List[Dict[str, Any]] = [{"role": "system", "content": system_content}]

    # 合并历史消息（连续对话）
    if session_messages:
        messages.extend(session_messages)

    # 追加用户消息
    messages.append({"role": "user", "content": user_message})

    yield _emit("chat", {"type": "log", "message": "维护 Agent 开始处理任务"})
    yield _emit("chat", {
        "type": "tools_meta",
        "phase": "chat",
        "tools": sorted(_ALL_TOOLS),
        "tool_schemas": _tool_schema_payload(
            build_tools_openai(conn, global_conn=server_conn), _ALL_TOOLS
        ),
        "parallel_tool_calls": True,
        "tool_choice": "auto",
    })

    round_i = 0
    consec_no_tool = 0  # 连续无工具调用轮次
    ending_prompt_injected = False
    MAX_ROUNDS = 50

    while True:
        round_i += 1
        if round_i > MAX_ROUNDS:
            yield _emit("chat", {
                "type": "log",
                "message": f"⛔ 已达最大轮次 {MAX_ROUNDS}，强制结束。",
            })
            break

        yield _emit("chat", {"type": "log", "message": f"推理轮次 {round_i}"})

        # 每 10 轮注入状态锚点
        if round_i > 1 and round_i % 10 == 0:
            anchor = (
                f"（系统提示）当前为第 {round_i} 轮推理。"
                "请勿重复已验证的工具调用；若任务已完成，直接输出总结。"
            )
            messages.append({"role": "user", "content": anchor})

        try:
            tools = build_tools_openai(conn, global_conn=server_conn)
            resp = _retry_llm_call(
                lambda: client.chat.completions.create(
                    model=_model,
                    messages=messages,
                    tools=tools,
                    tool_choice="auto",
                    parallel_tool_calls=True,
                    temperature=0.2,
                    max_tokens=16384,
                ),
                attempts=4,
                base_delay=1.0,
            )
        except Exception as e:  # noqa: BLE001
            yield _emit("chat", {
                "type": "error",
                "message": f"LLM 调用最终失败（已重试 4 次）：{e!r}",
            })
            return

        choice = resp.choices[0]
        finish_reason = getattr(choice, "finish_reason", None) or ""
        msg = choice.message
        tool_calls = getattr(msg, "tool_calls", None) or []

        # 检测文本内嵌工具调用（模型不支持原生 function calling 时）
        if not tool_calls and msg.content:
            parsed = _extract_text_tool_calls(msg.content or "")
            if parsed:
                tool_calls = parsed
                yield _emit("chat", {
                    "type": "log",
                    "message": f"检测到文本嵌入工具调用，解析到 {len(tool_calls)} 个",
                })

        if finish_reason == "length":
            yield _emit("chat", {"type": "log", "message": "⚠ 输出被截断，注入重试提示"})
            messages.append(_build_assistant_msg(msg))
            messages.append({
                "role": "user",
                "content": "你的上一次输出因超过 token 限制被截断。请分批执行（每次 write_cells ≤ 30 行），重新生成完整的参数。",
            })
            continue

        if tool_calls:
            consec_no_tool = 0
            # 保存 assistant 消息（含 tool_calls）
            tc_dicts = []
            for tc in tool_calls:
                args_str = tc.function.arguments or "{}"
                try:
                    json.loads(args_str)
                except json.JSONDecodeError:
                    args_str = json.dumps({"_truncated": True, "_raw_prefix": args_str[:120]}, ensure_ascii=False)
                tc_dicts.append({
                    "id": tc.id,
                    "type": "function",
                    "function": {"name": tc.function.name, "arguments": args_str},
                })
            messages.append(_build_assistant_msg(msg, tool_calls=tc_dicts))

            # 执行每个工具，将结果追加为 tool 消息
            for tc in tool_calls:
                name = tc.function.name
                args = tc.function.arguments or "{}"
                call_id = tc.id
                label = _TOOL_LABELS.get(name, name)

                yield _emit("chat", {
                    "type": "tool_call",
                    "call_id": call_id,
                    "name": name,
                    "label": label,
                    "arguments": args,
                })

                # 分发工具
                try:
                    raw = dispatch_tool(name, args, _dispatch_p)
                    result_obj = json.loads(raw) if isinstance(raw, str) else raw
                except Exception as e:  # noqa: BLE001
                    result_obj = {"error": str(e)}

                result_json = json.dumps(result_obj, ensure_ascii=False, default=str)

                yield _emit("chat", {
                    "type": "tool_result",
                    "call_id": call_id,
                    "name": name,
                    "result": result_json[:4000],
                })

                messages.append({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": result_json,
                })
        else:
            # 无工具调用 → 纯文本回复
            consec_no_tool += 1
            content = msg.content or ""

            # 流式输出文本 token
            for chunk_text in _chunk_text(content, 80):
                yield _emit("chat", {"type": "token", "text": chunk_text})

            # 连续 2 次无工具调用 → 注入结束复查
            if consec_no_tool >= 2 and not ending_prompt_injected:
                ending_prompt_injected = True
                messages.append(_build_assistant_msg(msg))
                messages.append({
                    "role": "user",
                    "content": _ending_review_prompt_text(),
                })
                yield _emit("chat", {
                    "type": "log",
                    "message": "⏹ 进入结束审核阶段",
                })
                continue

            # 正常结束
            yield _emit("chat", {
                "type": "done",
                "full_text": content,
            })
            return


# ─── 会话持久化工具 ─────────────────────────────────────────────────────────


def init_maintain_sessions_table(conn: sqlite3.Connection) -> None:
    """创建维护 Agent 专用会话表。"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _maintain_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_name TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            messages_json TEXT NOT NULL DEFAULT '[]'
        )
    """)
    conn.commit()


def create_maintain_session(conn: sqlite3.Connection, first_message: str = "") -> int:
    """新建维护会话，返回 session_id。"""
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    name = (first_message[:40] + "...") if len(first_message) > 40 else first_message or "新会话"
    cur = conn.execute(
        "INSERT INTO _maintain_sessions (session_name, created_at, updated_at, messages_json) VALUES (?,?,?,?)",
        (name, now, now, "[]"),
    )
    conn.commit()
    return cur.lastrowid


def get_maintain_session_messages(conn: sqlite3.Connection, session_id: int) -> List[Dict[str, Any]]:
    """获取维护会话的历史消息列表。"""
    row = conn.execute(
        "SELECT messages_json FROM _maintain_sessions WHERE id = ?", (session_id,)
    ).fetchone()
    if not row or not row[0]:
        return []
    try:
        return json.loads(row[0])
    except json.JSONDecodeError:
        return []


def append_maintain_session_messages(
    conn: sqlite3.Connection,
    session_id: int,
    messages: List[Dict[str, Any]],
) -> None:
    """向维护会话追加消息。"""
    existing = get_maintain_session_messages(conn, session_id)
    existing.extend(messages)
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute(
        "UPDATE _maintain_sessions SET messages_json = ?, updated_at = ? WHERE id = ?",
        (json.dumps(existing, ensure_ascii=False), now, session_id),
    )
    conn.commit()


def rename_maintain_session(conn: sqlite3.Connection, session_id: int, name: str) -> None:
    """重命名维护会话（用于 AI 生成标题后更新）。"""
    conn.execute(
        "UPDATE _maintain_sessions SET session_name = ? WHERE id = ?",
        (name[:80], session_id),
    )
    conn.commit()


def list_maintain_sessions(conn: sqlite3.Connection, limit: int = 30) -> List[Dict[str, Any]]:
    """列出维护会话列表。"""
    rows = conn.execute(
        "SELECT id, session_name, created_at, updated_at FROM _maintain_sessions ORDER BY updated_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [
        {"id": r[0], "session_name": r[1], "created_at": r[2], "updated_at": r[3]}
        for r in rows
    ]


def delete_maintain_session(conn: sqlite3.Connection, session_id: int) -> bool:
    """删除维护会话。"""
    conn.execute("DELETE FROM _maintain_sessions WHERE id = ?", (session_id,))
    conn.commit()
    return True
