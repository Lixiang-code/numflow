"""Agent 调度：design → review → execute 三阶段 SSE，含可选 reviewer 旁路。"""

from __future__ import annotations

import json
import time
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional

from app.config import QWEN_MODEL
from app.db.project_schema import get_pipeline_state
from app.deps import ProjectDB
from app.services.agent_tools import TOOLS_OPENAI, build_tools_openai, dispatch_tool, _get_project_config
from app.services.prompt_overrides import (
    get_prompt_override,
    merge_prompt_item,
    render_prompt_text,
)
from app.services.prompt_router import route_prompt
from app.services.qwen_client import get_client_for_model


def _retry_llm_call(
    fn: Callable[[], Any],
    *,
    attempts: int = 4,
    base_delay: float = 1.0,
    on_retry: Optional[Callable[[int, Exception, float], None]] = None,
) -> Any:
    """对 LLM 调用做指数退避重试。

    AI 服务（DashScope/DeepSeek）偶发 5xx/超时/连接重置，不应让整个 step 重做。
    重试 ``attempts`` 次（含首次），失败时仅在用尽后再向上抛出。
    """
    last_exc: Optional[Exception] = None
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 — 网络/服务端错误均需要重试
            last_exc = exc
            if i >= attempts:
                break
            delay = base_delay * (2 ** (i - 1))
            if on_retry:
                try:
                    on_retry(i, exc, delay)
                except Exception:  # noqa: BLE001
                    pass
            time.sleep(delay)
    assert last_exc is not None
    raise last_exc


WRITE_TOOLS = {
    "write_cells",
    "write_cells_series",
    "create_table",
    "create_matrix_table",
    "write_matrix_cells",
    "create_3d_table",
    "delete_table",
    "register_formula",
    "execute_formula",
    "recalculate_downstream",
    "register_calculator",
    "update_table_readme",
    "update_global_readme",
    "set_project_setting",
    "set_table_directory",
    "call_algorithm_api",
    "bulk_register_and_compute",
    "setup_level_table",
    "create_snapshot",
    "confirm_validation_rule",
    "glossary_register",
    "const_register",
    "const_tag_register",
    "const_set",
    "const_delete",
    "expose_param_to_subsystems",
}

# 只读工具白名单（gather 阶段只允许调用这些）
READ_TOOLS = {
    "get_project_config",
    "get_table_list",
    "get_table_schema",
    "read_table",
    "read_matrix",
    "read_3d_table",
    "read_3d_table_full",
    "read_cell",
    "get_protected_cells",
    "get_dependency_graph",
    "get_table_readme",
    "list_skills",
    "get_skill_detail",
    "render_skill_file",
    "get_algorithm_api_list",
    "run_validation",
    "list_snapshots",
    "compare_snapshot",
    "run_balance_check",
    "get_validation_history",
    "get_default_system_rules",
    "glossary_lookup",
    "glossary_list",
    "const_list",
    "const_tag_list",
    "sparse_sample",
    "list_directories",
    "list_calculators",
    "call_calculator",
    "list_exposed_params",
}

# Recovery Agent 只允许用只读工具 + 清理类写工具（delete_table / delete_column 等）
RECOVERY_CLEANUP_TOOLS = READ_TOOLS | {"delete_table", "update_table_readme", "update_global_readme"}

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
    "list_skills": "列出 SKILL",
    "get_skill_detail": "读取 SKILL 详情",
    "render_skill_file": "查看 SKILL 文件",
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


def _filter_tools_openai(all_tools: List[Dict[str, Any]], tool_names: set[str]) -> List[Dict[str, Any]]:
    return [tool for tool in all_tools if str((tool.get("function") or {}).get("name") or "") in tool_names]


def _tool_schema_payload(all_tools: List[Dict[str, Any]], tool_names: set[str]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for tool in all_tools:
        fn = tool.get("function") or {}
        name = str(fn.get("name", ""))
        if name in tool_names:
            items.append(
                {
                    "name": name,
                    "description": str(fn.get("description", "")),
                    "parameters": fn.get("parameters") or {},
                }
            )
    items.sort(key=lambda item: item["name"])
    return items


def _chunk_text(text: str, size: int) -> Iterable[str]:
    for i in range(0, len(text), size):
        yield text[i : i + size]


def _build_assistant_msg(msg: Any, *, tool_calls: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Build an assistant message dict, preserving reasoning_content when present.

    DeepSeek thinking models include `reasoning_content` in responses and require
    it to be echoed back in subsequent turns; omitting it causes a 400 error.
    """
    d: Dict[str, Any] = {"role": "assistant", "content": msg.content or ""}
    rc = getattr(msg, "reasoning_content", None)
    if rc:
        d["reasoning_content"] = rc
    if tool_calls is not None:
        d["tool_calls"] = tool_calls
    return d


# ---------- system prompts ----------

def _base_role_block(mode_norm: str) -> str:
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


def _base_common_system(mode_norm: str) -> str:
    role_block = _base_role_block(mode_norm)
    return "\n".join(
        [
            role_block,
            "【中英文命名强制规则】"
            "①所有名词（表名、列名、玩法名、资源名、子系统名、属性名）首次出现必须先调用 "
            "glossary_register(term_en, term_zh, brief, ...)；"
            "②正文/README/cell 中引用任何已注册名词必须使用 $term_en$ 引用符号，"
            "禁止裸中文专名或裸英文专名；"
            "③英文 term_en 必须为 snake_case 全小写（例：equip_base, gem_synth）；"
            "④客户端会按列的 display_lang 自动渲染 $name$，禁止手工硬编码语言混用。",
            "【matrix 表使用规则】"
            "①创建 2D 分配矩阵用 create_matrix_table（kind=attr_alloc 或 res_alloc）；"
            "②写入用 write_matrix_cells（行=玩法子系统，列=属性或资源）；"
            "③创建后必须调用 register_calculator 注册 fun(level, gameplay, attr|res[, grain])，"
            "brief 必须 ≥8 字符；下游一律用 call_calculator 取值，避免硬编码。",
            "【三维数据表（create_3d_table）】"
            "当一张表需要两个真实维度时（如 等级×宝石类型、等级×装备部位），必须用 create_3d_table，"
            "不要把其中一维硬塞成 level=1 或伪二维表。\n"
            "典型场景：宝石属性表（dim1=等级1~N, dim2=宝石类型atk/def/…, cols=atk_bonus/def_bonus/…），"
            "属性列只允许数值型。\n"
            "dim1 通常为数字等级（key 填整数字符串'1','2'…），dim2 为分类（key 填英文标识符）。\n"
            "属性列（cols[]）可附 formula 字段，公式可用 @dim1列名/@dim2列名 同行引用：\n"
            "  例：atk_bonus 公式 = @level * ${gem_base_atk}（需先注册常量 gem_base_atk）\n"
            "若公式含 ${常量}，先 const_register；常量就绪后再重算整表，确保不要手填展开值。\n"
            "读完整三轴结构用 read_3d_table_full；按任意维度切片用 read_3d_table。\n"
            "【伪三维表（matrix_resource）规则】第三维轴值（如等级）可手填；限制的是内容："
            "单切片允许常量，多切片必须全表 formula，不能混写。\n"
            "create_3d_table 同样必须传 display_name（中文）、directory、tags（≥1个）。",
            "【表命名与标签规范（严格）】"
            "①每张表的 display_name 必须为中文，如「基础属性表」，不得省略或留空；"
            "②每次调用 create_table 或 create_matrix_table 必须传 tags 参数（数组，至少1个标签），"
            "如 ['属性', '基础'] 或 ['资源', '养成']；标签决定右侧面板的「相关常数」筛选——"
            "只有与表 tags 有交集的全局常数才会显示，请在 design 阶段为每张表规划好合适的 tags；"
            "③tags 应与已注册的 const_tag_register 标签对齐（先 const_tag_register，再用相同标签）。",
            "【表目录管理】调用 create_table / create_matrix_table 时必须传 directory 参数（"
            "如 '基础/属性'、'分配/玩法属性'、'落地/装备'）以便目录化管理；"
            "可用 list_directories / set_table_directory 查询和移动。",
            "【SKILL 库使用】当前步骤可能已自动暴露默认 SKILL；当你需要核对更细的玩法制作说明时，"
            "优先使用 list_skills / get_skill_detail / render_skill_file 查询，不要凭空补写玩法规则。",
            "【2/4 项目上下文】信息收集（gather）阶段已主动读取了项目配置与现有表结构；"
            "design/review 阶段直接引用已收集信息，**无需**再重复调用读取工具。"
            "新建或修改「*系统_落地」、各子系统**行轴**、消耗与属性投放时，**须先** "
            "get_default_system_rules 对照 02 机读默认；禁止各系统无差别复用同一张仅「标准等级+两列消耗」的落地模板。 "
            "宝石的默认数据轴是**品阶/合成**（3 同阶→1 高 1 品）与**解锁门槛/属性池/分配**列，**不是**把标准等级 1..N 逐行 1:1 当成「宝石 N 级表」。"
            "坐骑/副本须体现开放等级与 02 约定（如坐骑 30 级、副本默认门槛等），列里要有玩法含义而非只有金币+掉率。\n"
            "【游戏类型适配】game_type 字段在 fixed_layer_config.core 中，已在 gather 阶段读取；设计时必须适配：\n"
            "  · rpg_turn（回合制）：战斗以回合为单位，速度影响行动顺序，无攻击间隔概念；\n"
            "  · rpg_realtime（即时制 Action RPG）：战斗实时进行，核心差异属性为 atk_spd/move_spd/base_atk_interval；\n"
            "  两种类型在 HP/ATK/DEF/暴击/暴伤等属性上无本质差异，但即时制需额外处理速度类属性公式与上下限。\n"
            "【智能子系统设计】当 fixed_layer_config.game_systems.ai_design_subsystems === true 时：\n"
            "  · 子系统维度由 AI 自主设计，不受 subsystemsByPath 里的预设选项约束；\n"
            "  · 应根据系统类型、游戏风格和项目定位，合理设计该系统的子维度（如宝石→套色/强化/解锁门槛；坐骑→天赋树/激活/外观；装备→强化/精炼/套装）；\n"
            "  · 在 README 中说明各系统子系统维度的设计理由与玩法意图；\n"
            "  · ai_design_subsystems=false 时，以用户在 subsystemsByPath 中的勾选为准。",
            "【3/4 工具规范】读工具可自由组合；写表/写 README/公式/算法调用仅在 execute 阶段且需写权限；"
            "每次写入必须带合法 source_tag；大批量用 read_table 的 limit/columns 切片。\n"
            "公式引用语法：@表名[列名]=逐行取同行值（数学计算用）；@@表名[列名]=整列数组（VLOOKUP/INDEX/MATCH/SUM/AVERAGE 等查找聚合用）。\n"
            "工具 JSON 固定含字段：status（success|error|partial）、data、warnings、blocked_cells；"
            "遇 partial/error 须阅读 warnings/blocked_cells 再决定是否继续。",
            "【4/4 输出与流程】本次会话严格按四阶段执行：gather → design → review → execute。\n"
            "  · gather 阶段：主动调用只读工具收集项目信息，完成后输出收集总结。\n"
            "  · design 阶段：基于 gather 已收集的信息，输出三段式 CoT，禁止任何工具调用：\n"
            "      ## 1. 我对用户需求的理解\n"
            "      ## 2. 我对游戏性的设计理解（结合 02-系统与子系统默认细则与项目核心定义）\n"
            "      ## 3. 我的最终设计\n"
            "  · review 阶段：把 design 输出再喂回，自审找问题并给出最终操作方案，仍禁止工具调用；你需要意识到第二轮会被强制要求自审。\n"
            "  · execute 阶段：才允许调用工具；先简述计划再调用，最终回答简洁并引用工具结果 data 的关键字段。\n"
            "【README 必含字段】任何写 README 的工具调用必须覆盖：目的（goal）/上游输入/产出/必备表与列/验收标准/常见踩坑。",
        ]
    )


def _agent_system_prompt_defaults() -> Dict[str, Dict[str, Any]]:
    return {
        "agent_common_init": {
            "category": "system",
            "prompt_key": "agent_common_init",
            "title": "Agent 通用系统提示词（初始化）",
            "summary": "初始化 Agent 在 design/review/execute 三阶段共享的基础 system prompt。",
            "description": "用于初始化模式的主 system prompt，包含命名纪律、表规则、流程与执行规范。",
            "reference_note": "在 agent_runner.run_agent_sse 中，当 mode=init 时作为 design/review/execute 三阶段的基础 system prompt 注入；修改会直接影响初始化 Agent 的行为边界与写入纪律。",
            "enabled": True,
            "modules": [
                {
                    "module_key": "body",
                    "title": "完整提示词",
                    "content": _base_common_system("init"),
                    "required": True,
                    "enabled": True,
                    "sort_order": 1,
                }
            ],
        },
        "agent_common_maintain": {
            "category": "system",
            "prompt_key": "agent_common_maintain",
            "title": "Agent 通用系统提示词（维护）",
            "summary": "维护 Agent 在 design/review/execute 三阶段共享的基础 system prompt。",
            "description": "用于维护模式的主 system prompt，包含命名纪律、表规则、流程与执行规范。",
            "reference_note": "在 agent_runner.run_agent_sse 中，当 mode=maintain 时作为 design/review/execute 三阶段的基础 system prompt 注入；修改会直接影响维护 Agent 的读写策略与流程要求。",
            "enabled": True,
            "modules": [
                {
                    "module_key": "body",
                    "title": "完整提示词",
                    "content": _base_common_system("maintain"),
                    "required": True,
                    "enabled": True,
                    "sort_order": 1,
                }
            ],
        },
        "agent_gather": {
            "category": "system",
            "prompt_key": "agent_gather",
            "title": "Agent gather 阶段提示词",
            "summary": "限定 gather 阶段只能读取项目信息并输出收集总结。",
            "description": "用于 gather 阶段的系统提示词。",
            "reference_note": "在 agent_runner._run_gather_phase 中作为唯一阶段 system prompt 注入，用于约束 gather 只读收集，不允许提前设计或写入。",
            "enabled": True,
            "modules": [{"module_key": "body", "title": "完整提示词", "content": _GATHER_SYSTEM, "required": True, "enabled": True, "sort_order": 1}],
        },
        "agent_design_tail": {
            "category": "system",
            "prompt_key": "agent_design_tail",
            "title": "Agent design 阶段尾提示词",
            "summary": "要求 design 阶段只输出三段式 CoT。",
            "description": "用于 design 阶段的附加 system prompt。",
            "reference_note": "在 design 阶段附加到通用 system prompt 后，用于固定输出格式并禁止工具调用。",
            "enabled": True,
            "modules": [{"module_key": "body", "title": "完整提示词", "content": _DESIGN_SYSTEM_TAIL, "required": True, "enabled": True, "sort_order": 1}],
        },
        "agent_review_tail": {
            "category": "system",
            "prompt_key": "agent_review_tail",
            "title": "Agent review 阶段尾提示词",
            "summary": "要求 review 阶段输出自审问题与最终操作方案。",
            "description": "用于 review 阶段的附加 system prompt。",
            "reference_note": "在 review 阶段附加到通用 system prompt 后，用于强制自审并输出可执行操作方案。",
            "enabled": True,
            "modules": [{"module_key": "body", "title": "完整提示词", "content": _REVIEW_SYSTEM_TAIL, "required": True, "enabled": True, "sort_order": 1}],
        },
        "agent_execute_tail": {
            "category": "system",
            "prompt_key": "agent_execute_tail",
            "title": "Agent execute 阶段尾提示词",
            "summary": "约束 execute 阶段的工具调用、自诉、TODO 与收尾规范。",
            "description": "用于 execute 阶段的附加 system prompt。",
            "reference_note": "在 execute 阶段附加到通用 system prompt 后，用于规范实际工具调用批次、写入顺序、校验和收尾。",
            "enabled": True,
            "modules": [{"module_key": "body", "title": "完整提示词", "content": _EXECUTE_SYSTEM_TAIL, "required": True, "enabled": True, "sort_order": 1}],
        },
        "agent_reviewer": {
            "category": "system",
            "prompt_key": "agent_reviewer",
            "title": "写操作 Reviewer 提示词",
            "summary": "对即将执行的写工具调用进行审批的 reviewer system prompt。",
            "description": "用于 reviewer 旁路模型的 system prompt。",
            "reference_note": "在 reviewer 审批写操作时使用，用于判断写工具调用是否安全、是否违反默认细则或覆盖用户手工内容。",
            "enabled": True,
            "modules": [{"module_key": "body", "title": "完整提示词", "content": _REVIEWER_SYSTEM, "required": True, "enabled": True, "sort_order": 1}],
        },
    }


def get_agent_system_prompt_catalog(conn: Optional[sqlite3.Connection] = None) -> List[Dict[str, Any]]:
    defaults = list(_agent_system_prompt_defaults().values())
    if conn is None:
        return defaults
    items: List[Dict[str, Any]] = []
    for default in defaults:
        override = get_prompt_override(conn, category="system", prompt_key=str(default["prompt_key"]))
        items.append(merge_prompt_item(default, override))
    return items


def _resolve_agent_system_prompt(conn: Optional[sqlite3.Connection], prompt_key: str) -> str:
    default = _agent_system_prompt_defaults()[prompt_key]
    if conn is None:
        return render_prompt_text(default)
    override = get_prompt_override(conn, category="system", prompt_key=prompt_key)
    return render_prompt_text(merge_prompt_item(default, override))


def _common_system(mode_norm: str, conn: Optional[sqlite3.Connection] = None) -> str:
    prompt_key = "agent_common_init" if mode_norm == "init" else "agent_common_maintain"
    return _resolve_agent_system_prompt(conn, prompt_key)


_GATHER_SYSTEM = (
    "【当前阶段=gather（信息收集）】你的唯一目标是主动读取足够的项目信息，为后续设计做准备。\n\n"
    "首轮必须并行调用（一次请求同时发出）：\n"
    "  · get_project_config — 核心定义（game_type/level_cap/游戏系统等）\n"
    "  · get_default_system_rules — 02 机读设计约束\n"
    "  · get_table_list — 已有表清单\n\n"
    "根据上述结果，按需追加调用（**应**并行，独立读取一次发出）：\n"
    "  · get_table_readme / read_table — 查看关键表的结构与数据\n"
    "  · get_dependency_graph — 了解表间依赖\n"
    "  · glossary_list / const_list — 已注册术语和常数\n\n"
    "收集完毕后，输出纯文字的「收集总结」，**禁止输出任何设计内容**：\n"
    "## 收集完毕\n"
    "- 项目类型与核心配置：...\n"
    "- 现有表及关键结构：...\n"
    "- 与本次任务相关的 02 设计约束：...\n\n"
    "**严禁调用任何写入工具。**"
)


_DESIGN_SYSTEM_TAIL = (
    "【当前阶段=design】你已在 gather 阶段读取了项目信息，现在基于这些信息进行设计分析。"
    "只输出三段式 CoT，**严禁**任何工具调用。"
    "格式必须严格使用三个二级标题：\n"
    "## 1. 我对用户需求的理解\n"
    "## 2. 我对游戏性的设计理解\n"
    "## 3. 我的最终设计\n"
    "在第 2 段中显式引用 gather 阶段读取到的 02 默认细则与项目核心定义；第 3 段必须给出可执行的表/列/公式清单。"
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

    "═══ ★ 操作前自诉协议（每回合批次写操作前统一输出一次）★ ═══\n"
    "每回合批次写操作前，**统一输出一次 6 行自诉**（并行批次整体视为一次，一批并行写调用只输出一次，不是每个工具单独输出一次）：\n"
    "  目标项: <表名/列名 或子系统名>\n"
    "  功能: <这张表/这一列在玩法中起什么作用，<=20字>\n"
    "  数值设计注意点: <2-3条；含值域/格式/单调性/产量量级/累计一致性等>\n"
    "  术语登记: <表名/各列英文-中文是否已在 _glossary 注册？未注册则先调 glossary_register>\n"
    "  常数登记: <本次公式涉及的字面量是否已 const_register？是→列出 ${名字}；否→先 const_register>\n"
    "  行数依据: <max_level / system_level_caps[<sys>] / 品阶枚举 / IFS 分段 之一；不允许写 30 / 60 / 100 等魔法数>\n"
    "缺任一行视为低质量，需在下一回合自我纠正。\n\n"

    "═══ ★ 数值设计原则（违反=校验失败）★ ═══\n"
    "① 概率/百分比类（暴击率/闪避/命中/抗性/各种 *_rate/*_ratio）：值域 [0, 0.95]，存储为小数，"
    "  number_format='0.00%'；禁止把 35% 写成 35 或 350。\n"
    "② 暴伤/伤害倍率：存储为小数（150% → 1.5），number_format='0.00%'；上限 ≤10。\n"
    "③ 性价比/单位收益类列：必须存在阶段性拐点或饱和，**禁止严格单调递增**。\n"
    "④ 产量/消耗：以「小时产量」衡量；普通资源量大、高级资源量小；"
    "  日产量 × 天数 ≈ 累计消耗（用 CUMSUM_TO_HERE/CUMSUM_PREV 校验一致）。\n"
    "⑤ 等级行覆盖：数值表行数=该子系统的开放等级上限。"
    "  优先读 `get_project_config().settings.fixed_layer_config.system_level_caps[<system>]`，未配则回退 max_level。"
    "  **禁止硬编码** 30/60/100；坐骑/翅膀/装备等子系统若需独立上限请引导用户配置 system_level_caps。"
    "  批量整数列必须用 IFS / setup_level_table / bulk_register_and_compute。\n"
    "⑥ ID 列、等级列、注册了公式的列**不允许任何空值**；写入后调 run_validation 自检。\n"
    "⑦ 公式中**禁止字面量浮点**（如 0.85、1000、0.6）。先 `const_register('xxx', value)`，再以 `${xxx}` 引用。\n\n"

    "═══ ★ 每回合固定格式（违反=低质量）★ ═══\n"
    "每回合批次调用前，**统一输出以下两行（并行批次视为一次，不是每个工具单独输出一次）**：\n"
    "  第1行: `当前: [x]已完成项目 | [ ]本轮目标`（用你的 TODO 状态）\n"
    "  第2行: `行动: <本轮操作，15字内>`\n"
    "例：`当前: [x]读配置 | [ ]创建角色表` / `行动: 创建 base_attr_table`\n\n"

    "═══ ★ TODO 清单（第一回合必须先输出，再调工具）★ ═══\n"
    "格式：`- [ ] 任务` / `- [x] 已完成` / `- [!] 阻塞：<原因>`\n"
    "规则：所有 `- [ ]` 变成 `[x]` 或 `[!]` 后，才能输出最终总结。\n\n"

    "═══ ★ 命名规则（严格）★ ═══\n"
    "table_name / columns[].name：**必须是英文 snake_case**（仅 a-z/0-9/_，首字小写字母），用于公式引用和存储\n"
    "  ✓ base_attr_table / hp_max / equip_alloc / mount_landing\n"
    "  ✗ 基础属性表 / HP上限 / 装备_属性分配 / 坐骑_落地（含中文 → 后端会拒绝）\n"
    "display_name（表级）/ columns[].display_name：**必须是中文**，用于展示\n"
    "  ✓ 「基础属性表」/「HP上限」/「装备·属性分配」/「坐骑·落地」\n"
    "建表前必须 `glossary_register` 把英文-中文注册一遍（如未存在）；建表本身也会自动 upsert glossary。\n"
    "两者同时必填，绝不能相同，绝不能混淆。\n\n"

    "═══ ★ 数值格式（每列必须设置 number_format）★ ═══\n"
    "  整数: '0' | 1位小数: '0.0' | 2位小数: '0.00' | 百分比: '0.00%'\n"
    "  千分位整数: '#,##0' | 千分位小数: '#,##0.00' | 字符串: '@'\n"
    "格式仅影响表格阅读显示，不影响公式计算和存储的真实值。\n\n"

    "═══ ★ 错误处理协议 ★ ═══\n"
    "工具返回 error / ok=false 时，**禁止直接重试**，必须先输出：\n"
    "  `失败: <根因，20字内>` / `绕行: <替代方案，20字内>`\n"
    "同一操作失败 2 次 → 标记 `- [!] 阻塞` → 立即跳到下一 TODO 项。\n"
    "若错误是「非法表名/列名」→ 立即把中文移到 display_name，table_name/列名改英文 snake_case 重试。\n\n"

    "═══ ★ 效率硬规则 ★ ═══\n"
    "① **同一回合内所有独立工具 → 一次性并行调用（批量发出），不要一个一个排队**。"
    "  例：需要读 A、B、C 三张表 → 三个 read_table 在同一回合同时发出；需要建多张表 → 多个 create_table 并行发出。\n"
    "② 等级/数值序列（规律递增/递减/公式可算）→ 必须用 setup_level_table 或 bulk_register_and_compute，**禁止** write_cells 逐行写。\n"
    "③ write_cells 只用于：分类标签、名称、描述、少量手工配置等**非规律内容**。\n"
    "④ setup_level_table：所有列公式同时放入 columns 数组，一次调用完成。\n"
    "⑤ write_cells 单次 ≤30 行，超出分多次调用；**对连续 row_id 的批量写入**优先使用 write_cells_series（用 row_id_template + start..end + value_list/expr，一次扩展数十/数百行，避免长 JSON 截断）。\n"
    "⑥ 最终总结：必须包含 TODO 完成状态 + executed_count/rows_updated 关键数字。\n\n"

    "═══ ★ 公式语法 ★ ═══\n"
    "幂运算用 `^` 或 `**` 均可（引擎内部统一为 `**`）。\n"
    "在公式中可以使用 `@T[col]` 或 `@this[col]` 表示『当前正在注册公式的这张表』，引擎会自动替换为真实表名；\n"
    "因此 register_formula(table_name='unit_table', formula='@T[lvl]^2 + ${base_atk}') 等价于显式写 `@unit_table[lvl]**2 + ${base_atk}`。\n\n"

    "═══ ★ 常数标签 / brief 规范 ★ ═══\n"
    "const_register 必填 tags（数组，至少 1 个）：常数会按 tags 分组在前端常量页中展示。\n"
    "  - 标签通常是『主系统名』(如 combat / economy / mount / wing) 或语义类别 (如 level_curve / drop_rate)。\n"
    "  - 若标签未注册可直接传，系统会自动登记；建议先用 const_tag_register 显式定义层级。\n"
    "brief 字段是对常数的**描述性介绍**（含义、单位、用途），应以自然语言说明，不应出现具体数值；\n"
    "  数值已由 value 字段承载，brief 里写数值属于信息冗余且降低可读性。\n"
    "  ✓ 'HP 基础值，单位：生命点' / '暴击伤害放大倍率，小数表示' / '防御减伤公式中的平衡系数'\n"
    "  ✗ 'HP 基础值=100' / '默认 0.85' / 'K=500'\n\n"

    "═══ ★ 收尾操作限制（每个 session 仅允许一次）★ ═══\n"
    "create_snapshot：整个 execute 阶段只允许调用 **1次**（最终收尾时），严禁在循环中多次调用。\n"
    "recalculate_downstream + run_validation：每张表只验证一次，验证通过后禁止重复调用。\n"
    "完成最终快照后，**立即**输出最终总结并结束任务，禁止再调用任何工具。"
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
    def _do() -> Any:
        return client.chat.completions.create(
            model=model or QWEN_MODEL,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )

    resp = _retry_llm_call(_do, attempts=4, base_delay=1.0)
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


def _build_exposed_params_block(p: ProjectDB, step_id: str) -> str:
    """读取 _step_exposed_params 中针对当前步骤（含父步通配）的暴露参数，渲染为 system 提示。"""
    if not step_id:
        return ""
    try:
        from app.services.agent_tools import _list_exposed_params
        items = (_list_exposed_params(p.conn, step_id) or {}).get("items") or []
    except Exception:
        return ""
    if not items:
        return ""
    lines = [
        "【父系统暴露参数】以下参数由上游步骤通过 expose_param_to_subsystems 主动暴露，"
        "本步设计时必须考虑（不要忽视）：",
    ]
    for it in items:
        key = it.get("key", "")
        val = it.get("value")
        brief = it.get("brief", "") or ""
        owner = it.get("owner_step", "")
        lines.append(f"- ${key}$ = {val!r}  (来自 {owner})  // {brief}")
    return "\n".join(lines)



def _reviewer_check(client, tool_name: str, tool_args: str, *, model: Optional[str] = None) -> Dict[str, Any]:
    """轻量 reviewer：返回 {'verdict': 'approve'|'reject', 'reason': str}。"""
    try:
        resp = client.chat.completions.create(
            model=model or QWEN_MODEL,
            messages=[
                {"role": "system", "content": _resolve_agent_system_prompt(p.conn, "agent_reviewer")},
                {
                    "role": "user",
                    "content": f"tool_name: {tool_name}\narguments:\n{tool_args}",
                },
            ],
            temperature=0.1,
            max_tokens=2048,
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
        "② 每回合批次前先输出：`当前: [状态] | 行动: <目标>`（并行批次视为一次）\n"
        "③ 遇错先输出 `失败: <根因>` / `绕行: <方案>`，再调工具\n"
        "④ 所有 TODO 为 [x] 或 [!] 后才能结束\n"
        "继续执行未完成的 TODO 项。"
    )


# ---------- gather phase ----------

def _run_gather_phase(
    client: Any,
    user_message: str,
    p: ProjectDB,
    *,
    model: str,
    routed_block: str = "",
) -> Generator[bytes, None, List[Dict[str, Any]]]:
    """信息收集阶段：AI 主动调用只读工具获取项目信息。

    yields: SSE events (phase="gather")
    returns: 本阶段产生的消息列表（assistant+tool exchanges），供 design 阶段注入上下文。
    """
    gather_messages: List[Dict[str, Any]] = [
        {"role": "system", "content": _resolve_agent_system_prompt(p.conn, "agent_gather")},
    ]
    if routed_block:
        gather_messages.append({"role": "system", "content": routed_block})
    gather_messages.append({"role": "user", "content": user_message})

    exchange_messages: List[Dict[str, Any]] = []
    MAX_GATHER_ROUNDS = 6

    # 发出初始消息快照（供监控查看 system prompt）
    yield _emit("gather", {"type": "phase_messages", "phase": "gather", "round": 0, "messages": list(gather_messages)})
    # 发出工具元信息（供监控查看可用工具与并行设置）
    yield _emit("gather", {
        "type": "tools_meta", "phase": "gather",
        "tools": sorted(READ_TOOLS),
        "tool_schemas": _tool_schema_payload(build_tools_openai(p.conn), READ_TOOLS),
        "parallel_tool_calls": True,
        "tool_choice": "auto",
    })

    for round_i in range(1, MAX_GATHER_ROUNDS + 1):
        yield _emit("gather", {"type": "log", "message": f"信息收集轮次 {round_i}"})
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=gather_messages,
                tools=_filter_tools_openai(build_tools_openai(p.conn), READ_TOOLS),
                tool_choice="auto",
                parallel_tool_calls=True,
                temperature=0.1,
                max_tokens=8192,
            )
        except Exception as e:  # noqa: BLE001
            yield _emit("gather", {"type": "error", "message": f"信息收集调用失败: {e!r}"})
            return exchange_messages

        msg = resp.choices[0].message
        tool_calls = getattr(msg, "tool_calls", None) or []

        if tool_calls:
            tc_dicts = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {"name": tc.function.name, "arguments": tc.function.arguments or "{}"},
                }
                for tc in tool_calls
            ]
            assistant_msg = _build_assistant_msg(msg, tool_calls=tc_dicts)
            gather_messages.append(assistant_msg)
            exchange_messages.append(assistant_msg)

            for tc in tool_calls:
                name = tc.function.name
                args = tc.function.arguments or "{}"
                label = _TOOL_LABELS.get(name, name)
                yield _emit("gather", {
                    "type": "tool_call", "call_id": tc.id,
                    "name": name, "label": label, "arguments": args,
                })
                try:
                    result = dispatch_tool(name, args, p)
                except Exception as tool_exc:  # noqa: BLE001
                    result = json.dumps(
                        {"ok": False, "error": f"工具执行异常: {tool_exc!r}"},
                        ensure_ascii=False,
                    )
                tool_msg: Dict[str, Any] = {"role": "tool", "tool_call_id": tc.id, "content": result}
                gather_messages.append(tool_msg)
                exchange_messages.append(tool_msg)
                yield _emit("gather", {
                    "type": "tool_result", "call_id": tc.id,
                    "name": name, "status": "done", "preview": result[:500],
                })
        else:
            # AI produced the gather summary — done
            summary_text = (msg.content or "").strip()
            for chunk in _chunk_text(summary_text, 80):
                yield _emit("gather", {"type": "token", "text": chunk})
            summary_msg = _build_assistant_msg(msg)
            exchange_messages.append(summary_msg)
            gather_messages.append(summary_msg)
            # 最终快照（含完整会话记录，供监控查看）
            yield _emit("gather", {"type": "phase_messages", "phase": "gather", "round": round_i, "messages": list(gather_messages)})
            yield _emit("gather", {"type": "done", "summary": summary_text[:800]})
            return exchange_messages

    yield _emit("gather", {"type": "log", "message": f"⚠ 信息收集达到最大轮次 {MAX_GATHER_ROUNDS}，继续推进"})
    return exchange_messages


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

    client = get_client_for_model(_model)

    # ---- prompt 路由 ----
    step_id = _current_step_id(p)
    cfg_summary = _project_config_summary(p)
    yield _emit("route", {"type": "log", "message": f"提示词路由：step={step_id or '(none)'}"})
    try:
        route = route_prompt(step_id, user_message, cfg_summary, model=_model, conn=p.conn)
    except Exception as e:  # noqa: BLE001
        route = {
            "hit": False,
            "prompt": "",
            "gather_hint": "",
            "rationale": f"route_exception: {e!r}",
            "skills": [],
        }
    yield _emit(
        "route",
        {
            "type": "prompt_route",
            "hit": bool(route.get("hit")),
            "prompt": route.get("prompt", ""),
            "gather_hint": route.get("gather_hint", ""),
            "route_system": route.get("route_system", ""),
            "rationale": route.get("rationale", ""),
            "step_id": step_id,
            "skills": route.get("skills", []),
        },
    )

    base_system = _common_system(mode_norm, p.conn)
    routed_prompt = (route.get("prompt") or "").strip()
    routed_block = (
        "【5/4 路由提示词】" + routed_prompt if routed_prompt else ""
    )

    gather_hint = (route.get("gather_hint") or "").strip()

    exposed_block = _build_exposed_params_block(p, step_id)
    if exposed_block:
        yield _emit("route", {"type": "log", "message": "已注入父系统暴露参数到 prompt"})

    # ---- 用户消息事件（供监控追踪）----
    yield _emit("meta", {"type": "user_message", "content": user_message, "model": _model})

    # ---- 0) gather: AI reads project info before designing ----
    # 注意：gather 阶段只注入步骤目标提示（去掉写操作指令），防止 AI 在收集阶段执行写操作
    yield _emit("gather", {"type": "log", "message": "gather 阶段开始（只读工具，AI 主动收集项目信息）"})
    gather_gen = _run_gather_phase(
        client, user_message, p,
        model=_model, routed_block=gather_hint,  # 只传步骤目标 hint，不含写操作指令
    )
    gather_context: List[Dict[str, Any]] = yield from gather_gen
    yield _emit("gather", {"type": "log", "message": f"gather 阶段结束（收集 {len(gather_context)} 条上下文消息）"})

    # ---- 1) design: CoT with gathered context ----
    design_messages: List[Dict[str, Any]] = [
        {"role": "system", "content": base_system},
    ]
    if routed_block:
        design_messages.append({"role": "system", "content": routed_block})
    if exposed_block:
        design_messages.append({"role": "system", "content": exposed_block})
    design_messages.append({"role": "system", "content": _resolve_agent_system_prompt(p.conn, "agent_design_tail")})
    design_messages.append({"role": "user", "content": user_message})
    # Inject gather phase context so design sees real project data
    design_messages.extend(gather_context)
    design_messages.append({
        "role": "user",
        "content": "以上是你在信息收集阶段主动读取的项目信息。请基于这些信息，开始 design 阶段（三段式 CoT，严禁工具调用）。",
    })

    yield _emit("design", {"type": "log", "message": "design 阶段开始（无工具，三段式 CoT，流式）"})
    # 发出完整消息快照供监控
    yield _emit("design", {"type": "phase_messages", "phase": "design", "messages": design_messages})
    design_text = ""
    try:
        stream = client.chat.completions.create(
            model=_model,
            messages=design_messages,
            temperature=0.2,
            max_tokens=16384,
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
    if exposed_block:
        review_messages.append({"role": "system", "content": exposed_block})
    review_messages.append({"role": "system", "content": _resolve_agent_system_prompt(p.conn, "agent_review_tail")})
    review_messages.append({"role": "user", "content": user_message})
    review_messages.append(
        {
            "role": "user",
            "content": "以下是 design 阶段的输出，请自审并给出最终操作方案：\n\n" + design_text,
        }
    )

    yield _emit("review", {"type": "log", "message": "review 阶段开始（无工具，自审，流式）"})
    # 发出完整消息快照供监控
    yield _emit("review", {"type": "phase_messages", "phase": "review", "messages": review_messages})
    review_text = ""
    try:
        stream = client.chat.completions.create(
            model=_model,
            messages=review_messages,
            temperature=0.2,
            max_tokens=32768,
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
    if exposed_block:
        execute_messages.append({"role": "system", "content": exposed_block})
    execute_messages.append({"role": "system", "content": _resolve_agent_system_prompt(p.conn, "agent_execute_tail")})
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
    # 发出工具元信息（供监控查看可用工具与并行设置）
    yield _emit("execute", {
        "type": "tools_meta", "phase": "execute",
        "tools": sorted(WRITE_TOOLS | READ_TOOLS),
        "tool_schemas": _tool_schema_payload(build_tools_openai(p.conn), WRITE_TOOLS | READ_TOOLS),
        "parallel_tool_calls": True,
        "tool_choice": "auto",
    })
    # 发出初始消息快照（后续每轮 LLM 调用前也会更新）
    yield _emit("execute", {"type": "phase_messages", "phase": "execute", "messages": list(execute_messages)})
    final_text = ""
    round_i = 0
    consec_errors = 0   # 连续错误计数（重置于成功）
    total_errors = 0    # 累计错误（不重置）
    total_success = 0   # 累计成功
    MAX_CONSEC_ERRORS = 4  # 连续失败4次强制注入分析提示
    MAX_EXECUTE_ROUNDS = 80  # execute 阶段总轮次硬上限（兜底防失控；放宽以容纳大批量数值表）
    # ---- 反循环计数器 ----
    _snapshot_count = 0        # create_snapshot 调用次数
    _validation_count = 0      # run_validation 调用次数
    _recalc_count = 0          # recalculate_downstream 调用次数
    _recent_tools: List[str] = []  # 最近20次工具名（用于检测重复模式）
    _final_validation_injected = False  # 收尾前主动反馈违反，只注入一次
    while True:
        round_i += 1

        # ---- 总轮次硬上限：超过则强制收尾 ----
        if round_i > MAX_EXECUTE_ROUNDS:
            yield _emit(
                "execute",
                {
                    "type": "log",
                    "message": f"⛔ 总轮次硬上限 {MAX_EXECUTE_ROUNDS} 已触发，强制结束 execute 阶段。",
                },
            )
            break

        # ---- 每20轮发出一次进度警告（不强制终止）----
        if round_i > 1 and round_i % 20 == 0:
            yield _emit("execute", {"type": "log", "message": f"⏱ 已进行 {round_i} 轮推理，成功={total_success} 失败={total_errors}"})

        # ---- 每5轮注入状态锚点（防止小模型目标漂移）----
        if round_i > 1 and round_i % 5 == 0:
            anchor = _make_state_anchor(round_i, user_message, total_success, total_errors)
            execute_messages.append({"role": "user", "content": anchor})
            yield _emit("execute", {"type": "log", "message": f"第 {round_i} 轮：注入状态锚点"})

        # ---- 反循环检测：快照/验证次数超限，强制结束 ----
        if _snapshot_count >= 3:
            loop_msg = (
                "⚠ 反循环保护触发：你已调用 create_snapshot 超过 3 次，陷入验证-快照死循环。\n"
                "立即停止任何进一步的 recalculate_downstream / run_validation / create_snapshot 调用。\n"
                "直接输出最终总结（包含 TODO 完成状态 + executed_count/rows_updated 关键数字）并结束任务。"
            )
            execute_messages.append({"role": "user", "content": loop_msg})
            yield _emit("execute", {"type": "log", "message": "⚠ 反循环保护：快照次数超限，注入强制结束提示"})
            _snapshot_count = -9999  # 防止重复触发
        elif _validation_count >= 8:
            loop_msg = (
                f"⚠ 反循环保护触发：你已调用 run_validation {_validation_count} 次，存在过度验证循环。\n"
                "每张表只需验证一次。请检查 TODO 清单，若所有项目已完成，直接输出最终总结并结束任务。"
            )
            execute_messages.append({"role": "user", "content": loop_msg})
            yield _emit("execute", {"type": "log", "message": f"⚠ 反循环保护：验证次数={_validation_count}，注入提示"})
            _validation_count = -9999

        yield _emit(
            "execute",
            {"type": "log", "message": f"模型推理轮次 {round_i}"},
        )
        # 每轮 LLM 调用前发出完整消息快照（包含历史工具调用/结果）
        if round_i > 1:
            yield _emit("execute", {"type": "phase_messages", "phase": "execute", "round": round_i, "messages": list(execute_messages)})
        try:
            _retry_log: List[Dict[str, Any]] = []
            def _do_call() -> Any:
                return client.chat.completions.create(
                    model=_model,
                    messages=execute_messages,
                    tools=build_tools_openai(p.conn),
                    tool_choice="auto",
                    parallel_tool_calls=True,
                    temperature=0.2,
                    max_tokens=16384,
                )

            def _on_retry(i: int, exc: Exception, delay: float) -> None:
                _retry_log.append({"i": i, "err": repr(exc)[:300], "delay": delay})

            resp = _retry_llm_call(_do_call, attempts=4, base_delay=1.0, on_retry=_on_retry)
            for entry in _retry_log:
                yield _emit(
                    "execute",
                    {
                        "type": "log",
                        "message": f"⚠ LLM 调用第 {entry['i']} 次失败，{entry['delay']:.1f}s 后重试：{entry['err']}",
                    },
                )
        except Exception as e:  # noqa: BLE001
            yield _emit(
                "execute",
                {"type": "error", "message": f"execute 调用最终失败（已重试 4 次）: {e!r}"},
            )
            return
        choice = resp.choices[0]
        finish_reason = getattr(choice, "finish_reason", None) or ""
        msg = choice.message
        tool_calls = getattr(msg, "tool_calls", None) or []

        # ---- 输出被 token 限制截断：注入修复提示让模型重新分批 ----
        if finish_reason == "length":
            yield _emit("execute", {"type": "log", "message": "⚠ 模型输出被 max_tokens 截断，注入重试提示"})
            execute_messages.append(_build_assistant_msg(msg))
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
                _build_assistant_msg(msg, tool_calls=[
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": _safe_args(tc.function.arguments),
                        },
                    }
                    for tc in tool_calls
                ])
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
                # ---- 反循环计数 ----
                _recent_tools.append(name)
                if len(_recent_tools) > 20:
                    _recent_tools.pop(0)
                if name == "create_snapshot" and tool_status == "success":
                    _snapshot_count += 1
                elif name == "run_validation" and tool_status == "success":
                    _validation_count += 1
                elif name == "recalculate_downstream" and tool_status == "success":
                    _recalc_count += 1
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
        # ---- 收尾前主动校验：若仍有违反，注入一条 user 消息要求修正再继续 ----
        if not _final_validation_injected:
            _final_validation_injected = True
            try:
                vresult = dispatch_tool("run_validation", "{}", p)
                vdata = json.loads(vresult) if isinstance(vresult, str) else {}
                vpayload = vdata.get("data") if isinstance(vdata, dict) else None
                violations = []
                if isinstance(vpayload, dict):
                    violations = vpayload.get("violations") or []
                viol_count = len(violations) if isinstance(violations, list) else 0
                if viol_count > 0:
                    yield _emit(
                        "execute",
                        {
                            "type": "log",
                            "message": f"⚠ 收尾自检：仍有 {viol_count} 条规则违反，注入修正请求继续执行",
                        },
                    )
                    # 截取前若干条违反，避免上下文过长
                    sample = violations[:30]
                    feedback = (
                        f"自动收尾校验触发：当前仍有 {viol_count} 条规则违反，必须修正后再结束。\n"
                        f"违反明细（最多展示前 30 条）：\n```json\n"
                        f"{json.dumps(sample, ensure_ascii=False, indent=2)}\n```\n"
                        "请按以下顺序处理：\n"
                        "1. 逐条分析根因（数据错误 / 公式错误 / 规则过严）；\n"
                        "2. 优先用 register_formula / write_cells 修正数据；\n"
                        "3. 若规则本身不合理，用 update_validation_rules 调整后再 run_validation 复核；\n"
                        "4. 直到 run_validation 全部通过或无法处理后再输出最终总结。"
                    )
                    execute_messages.append({"role": "user", "content": feedback})
                    continue
            except Exception as exc:  # noqa: BLE001
                yield _emit(
                    "execute",
                    {"type": "log", "message": f"收尾自检失败（忽略）：{exc!r}"},
                )
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
【角色】你是 Numflow「状态修复 Agent」（Recovery Agent）。
你的唯一职责是：检查上一次 pipeline 步骤因崩溃/中断留下的"孤儿状态"（部分创建的表、脏数据），并清理它，使下一次重试能从干净状态开始。

【触发前提】
本 Agent 只在"执行阶段有部分写操作成功后崩溃"时触发。
如果错误是网络/连接/超时问题，或者没有任何写操作成功，应直接输出 RECOVERY_RETRY，无需任何工具调用。

【工作流程】
1. 诊断：阅读失败上下文，判断是否真的有状态污染（孤儿表/不完整数据）。
   - 若无污染（纯瞬态错误 / 无写入成功） → 立即输出 RECOVERY_RETRY，结束。
2. 检查：调用 get_table_list / read_table 确认实际残留状态。
3. 清理：仅调用 delete_table 删除孤儿表（不做任何新建/写入）。
4. 汇报：输出结构化修复报告，末尾必须有且仅有一个状态标记（单独一行）：
   - RECOVERY_RETRY  ：无需清理，可直接重试
   - RECOVERY_DONE   ：已清理完毕，可安全重试
   - RECOVERY_PARTIAL：部分清理，重试可能成功
   - RECOVERY_FAILED ：无法自动处理，需人工介入

【约束】
- 只能调用只读工具和 delete_table / update_table_readme / update_global_readme。
- 绝对不能创建表、写入数据、注册公式——那是重试步骤的工作。
- 遇到无法判断的情况，优先选 RECOVERY_RETRY，让重试去自然发现。
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
    client = get_client_for_model(_model)

    step_id = failure_context.get("step_id", "unknown")
    error_msg = failure_context.get("error", "")
    tool_history = failure_context.get("tool_history", [])  # [{name, arguments, result}]
    partial_design = failure_context.get("partial_design", "")

    yield _emit("route", {"type": "log", "message": f"修复 Agent 启动（失败步骤: {step_id}）"})
    yield _emit("route", {
        "type": "prompt_route",
        "hit": True,
        "prompt": "recovery",
        "gather_hint": "",
        "route_system": "",
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
            max_tokens=8192,
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
                tools=_filter_tools_openai(build_tools_openai(p.conn), RECOVERY_CLEANUP_TOOLS),
                tool_choice="auto",
                parallel_tool_calls=True,
                temperature=0.1,
                max_tokens=16384,
            )
        except Exception as e:  # noqa: BLE001
            yield _emit("execute", {"type": "error", "message": f"修复执行调用失败: {e!r}"})
            return

        msg = resp.choices[0].message if resp.choices else None
        if msg is None:
            break

        execute_messages.append(_build_assistant_msg(msg, tool_calls=[
            {"id": tc.id, "type": "function", "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
            for tc in (msg.tool_calls or [])
        ]))

        if not msg.tool_calls:
            recovery_text = msg.content or ""
            for chunk in _chunk_text(recovery_text, 80):
                yield _emit("execute", {"type": "token", "text": chunk})
            # 判断修复结果
            if "RECOVERY_RETRY" in recovery_text:
                status = "retry"
            elif "RECOVERY_DONE" in recovery_text:
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
