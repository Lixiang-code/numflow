"""提示词路由：按 pipeline 步骤匹配默认玩法/系统提示词，否则让 LLM 现编一段。

第二轮矫正核心要点：
- 全部表/列名采用英文 snake_case，中文走 display_name；提示词样例同步换英文。
- 删除所有"魔法数"硬编码（HP 1000/49000、坐骑 30、副本 10/20…）；改为引用 `${name}` 常数或
  `system_level_caps.<sys>` 派生上限；要求模型先 `const_register` 再写公式。
- 显式要求模型在建表/写公式前调用 `glossary_register` / `const_register`，让对照项目里
  `_glossary` / `_constants` 不再为 0 行。
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Optional

from app.config import QWEN_MODEL
from app.services.prompt_overrides import get_prompt_override, merge_prompt_item_layers, render_prompt_text
from app.services.qwen_client import get_client_for_model
from app.services.skill_library import build_default_skill_prompt


# === 全部步骤通用前缀（命名 + 术语 + 常数纪律），插在每段提示词最前 ===
_NAMING_HEADER = (
    "★ 命名纪律（每张表/每列 100% 必守）★\n"
    "  · table_name / columns[].name 必须是英文 snake_case（a-z/0-9/_），首字小写字母；"
    "中文一律写到 display_name / columns[].display_name。\n"
    "  · 建表前先 `glossary_lookup` 确认术语，未注册的英文-中文对必须先 `glossary_register`；"
    "建表后系统会自动把 display_name 写入 _glossary，如已 register 则保留你的中文。\n"
    "  · 公式中的字面量浮点数（HP 起止值、占比、衰减系数等）请先 `const_register('xxx', value)`，"
    "再在公式里以 `${xxx}` 引用，禁止把魔法数直接抄进公式字符串。\n"
    "  · 等级行数：必须读 `get_project_config().settings.fixed_layer_config.system_level_caps[<sys>]`，"
    "未配置则回退 `max_level`；**禁止硬编码 30 / 60 / 100**。\n"
)


def _extract_gather_hint(prompt: str) -> str:
    """从完整路由提示词中提取 gather 阶段轻量上下文。

    只保留步骤编号标题行、目标说明行、必产出表名信息（「必产出」/「验收」行），
    去掉 _NAMING_HEADER 和所有写操作指令（const_register/glossary_register/
    setup_level_table/write_cells/create_table 等），防止 gather 阶段 AI 执行写操作。
    """
    if not prompt:
        return ""
    # 去掉 _NAMING_HEADER 前缀
    stripped = prompt.replace(_NAMING_HEADER, "")
    # 只保留 目标/步骤/必产出/验收/先读/read 相关行；排除含写操作关键词的行
    write_keywords = (
        "const_register", "glossary_register", "setup_level_table",
        "write_cells", "create_table", "update_global_readme",
        "update_table_readme", "set_project_setting", "bulk_register",
        "register_formula", "execute_formula", "glossary_register",
        "const_tag_register", "glossary_batch", "update_rows",
        "register_gameplay_table", "set_gameplay_table_status",
    )
    kept: list[str] = []
    for line in stripped.splitlines():
        low = line.lower()
        if any(kw in low for kw in write_keywords):
            continue
        kept.append(line)
    result = "\n".join(kept).strip()
    if result:
        result = "【gather 阶段步骤参考（仅供了解需读取哪些信息，禁止任何写操作）】\n" + result
    return result


def _normalize_prompt_step_id(step_id: str) -> str:
    if step_id == "gameplay_landing_tables":
        return "gameplay_table"
    if step_id.startswith("gameplay_landing_tables."):
        return step_id.replace("gameplay_landing_tables", "gameplay_table", 1)
    return step_id


def _router_prompt_keys(step_id: str) -> List[str]:
    normalized = _normalize_prompt_step_id(step_id)
    keys = [f"route_step::{normalized}"]
    if normalized != step_id:
        keys.insert(0, f"route_step::{step_id}")
    return keys


def _router_default_prompt_keys(step_id: str) -> List[str]:
    exact = _router_prompt_keys(step_id)
    normalized = _normalize_prompt_step_id(step_id)
    base_id = normalized.split(".", 1)[0] if "." in normalized else ""
    if not base_id:
        return exact
    base = _router_prompt_keys(base_id)
    out: List[str] = []
    seen: set[str] = set()
    for key in exact + base:
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


# 与 routers/pipeline.py PIPELINE_STEPS 一一对应的默认提示词模板。
# 每段简短描述：本步必产出（表名/列名/接受标准），便于 design 阶段对齐。
DEFAULT_STEP_PROMPTS: Dict[str, str] = {
    "environment_global_readme": (
        _NAMING_HEADER
        + "【步骤 1/7+N 环境与全局 README】\n"
        "（流水线共 7 个固定步骤 + N 个动态玩法落地步骤；本步为第 1 步）\n"
        "目标：固化项目级元数据与全局 README，为后续所有步骤提供数值基线。\n"
        "【操作流程】\n"
        "1. 调用 `get_project_config` 读取现有配置，重点提取 fixed_layer_config.core 下："
        "level_cap、lifecycle_days、game_type、business_model、theme、magnitude、"
        "defense_formula、combat_rhythm；以及 fixed_layer_config.system_level_caps（若有）。\n"
        "2. 用 `set_project_setting` 写入顶层键："
        "max_level（=level_cap）、currencies（{gold,bound_diamond,...}）、"
        "stat_keys（核心属性ID数组，来自 attribute_systems.selectedAttrs）、"
        "resource_keys（养成资源ID数组）。\n"
        "3. 用 `glossary_register` 把核心术语登记一遍：项目中所有英文 ID（如 `hp_max`、`atk`、`crit_rate` "
        "等）都注册一对中英对照；该步骤之后整个流水线都默认这些术语已存在。\n"
        "4. 用 `const_register` 写入项目级常数：max_level等\n"
        "5. 用 `update_global_readme` 写包含 6 字段的全局 README："
        "goal / upstream_input / output / required_tables_cols / acceptance_criteria / pitfalls；"
        "并附项目定位/核心循环/术语表概览。\n"
        "【重要】project_settings 是系统表已存在，禁止 `create_table('project_settings')`；"
        "全局 README 走 `update_global_readme` 而非 `write_cells`。\n"
        "验收：`get_project_config` 含 max_level/currencies/stat_keys/resource_keys；"
        "`glossary_list` ≥ 8 条；`const_list` ≥ 5 条。"
    ),
    "base_attribute_framework": (
        _NAMING_HEADER
        + "【步骤 3/7+N 基础属性框架（除 HP）】\n"
        "目标：定义角色基础属性骨架，输出 1..max_level 行的标准等级基础属性表（hp 列暂留空，下一步反推）。\n"
        "规则：\n"
        "  · 主战斗属性（攻击/防御类）按膨胀速率单一公式贯穿全等级（禁止分段）；\n"
        "  · 项目 stat_keys 勾选的所有属性都必须在表中且必须有膨胀（百分比类属性给合理曲线，仍单调）；\n"
        "  · 单调线性或单调指数膨胀，禁止分段；\n"
        "  · 本步不填 hp 列，hp 由下一步 hp_formula_derivation 通过战斗公式反推。\n"
        "先 `get_project_config` → 读 core.game_type 与 attribute_systems.selectedAttrs（即 stat_keys）。\n"
        "必产出表：`num_base_framework`（display_name=「基础属性·标准等级」），\n"
        "  列包含 level + 全部 stat_keys 勾选属性（hp 列建好但可为空；属性列名须与 stat_keys 完全一致）。\n"
        "★ 强制效率方式：用 `setup_level_table` 一次完成。常数先 `const_register`，再以 `${name}` 引用。\n\n"
        "★★ 乘法防御公式时必须执行以下流程（减法公式不需要）：\n"
        "  1. 确认项目的防御属性列名（从 stat_keys 读取，如 def / defense / armor 等），设计其成长曲线后，\n"
        "     **必须设计 K 值**（战斗公式为 net_dmg = 1 - def_attr/(def_attr+K)）。\n"
        "     K 值决定减伤曲线斜率，需自行权衡（建议先用中等级攻击力对齐目标减伤率 30~50%）。\n"
        "  2. `const_register('<def_attr>_K', 值, tags=['combat'])` 注册 K 值（<def_attr> 替换为实际列名）。\n"
        "  3. 在 num_base_framework 中增加 `<def_attr>_reduction` 列（display_name=「防御减伤率」），\n"
        "     公式：<def_attr>_reduction = <def_attr> / (<def_attr> + ${<def_attr>_K})。\n"
        "  4. 用 `sparse_sample(table_name='num_base_framework', columns=['level','<def_attr>','<def_attr>_reduction'], n=20)`\n"
        "     采样 20 行检查减伤曲线，确认低/中/高等级减伤率在设计目标范围内（如低区 10~20%、高区 40~60%）。\n"
        "  5. 若曲线不合理：调整 K 常数值，重新计算，再次 sparse_sample 复查；**可多轮迭代**。"
    ),
    "hp_formula_derivation": (
        _NAMING_HEADER
        + "【步骤 4/7+N HP 反推公式推导】\n"
        "目标：基于已建立的攻击/防御曲线与战斗节奏假设，通过公式反推推导出 hp 列，\n"
        "确保每一等级的 HP 都有战斗设计依据，而非拍脑袋填数。\n"
        "先 `get_project_config` → 读取 stat_keys，确认本项目攻击属性列名（如 atk）和防御属性列名（如 def）。\n"
        "核心推导路径：\n"
        "  hp(L) = atk_ref(L) × net_dmg_ratio(L) × expected_survive_seconds(L) × attacks_per_second\n"
        "  其中 net_dmg_ratio = 1 - def_ratio（减法模型：net = 1 - def_attr/(def_attr+K)；乘法模型：net = 1 - def_factor）\n"
        "  atk_ref 和 def_attr 的列名从 stat_keys 读取，代入实际列名。\n"
        "操作步骤：\n"
        "  1. `const_register('expected_survive_seconds', 默认8.0, tag='combat_rhythm')` "
        "（允许按等级区段分档：低区5s/中区8s/高区12s，分别 const_register 不同 key）；\n"
        "  2. `const_register('attacks_per_second', 1.0, tag='combat_rhythm')`；\n"
        "  3. 写出 hp_formula 表达式（用实际属性列名），`update_formula('hp_formula', expression, ...)` 登记到 _formula_registry；\n"
        "  4. 用 `update_rows` 把 hp 列写入 num_base_framework（不重建表）；\n"
        "  5. `update_table_readme`：写出公式、战斗节奏假设、level1/mid/max 的 HP 合理性校验。\n"
        "★ review 阶段：对比攻击属性 vs hp/atk 比值趋势，确保高等级 HP 膨胀曲线合理。\n"
        "★ 攻击方 atk_ref 使用同等级自身攻击属性（PvE 对手等级同玩家），不要用 level1 固定值。\n"
        "★ expected_survive_seconds 必须 const_register 到 _constants，禁止硬编码。"
    ),
    "gameplay_allocation": (
        _NAMING_HEADER
        + "【步骤 5/7+N 玩法属性分配（matrix 表）】\n"
        "目标：以步骤 2 注册的所有玩法表为行、勾选属性为列，建立玩法属性分配矩阵。\n"
        "操作：\n"
        "  1. `get_gameplay_table_list()` → 获取步骤 2 已注册的全部玩法表，\n"
        "     取其 table_id 列表作为 matrix 的行（rows）；先 `glossary_register` 每个 table_id；\n"
        "  2. `get_project_config` → 读取 attribute_systems.selectedAttrs 作为 matrix 的列（cols）；\n"
        "  3. `create_matrix_table(name='gameplay_attr_alloc', kind='matrix_attr', scale_mode='none', "
        "rows=[从步骤1读取的 table_id 列表], cols=[从步骤2读取的属性列表], directory='分配/玩法属性')`；\n"
        "     【scale_mode='none' = 纯2D表，无等级维，write_matrix_cells 不传 level，勿改此默认值】\n"
        "  4. `write_matrix_cells` 填投放占比（0..1，允许 0 表示该子系统不投放该属性）；\n"
        "  5. `register_calculator(name='gameplay_attr_alloc_lookup', kind='matrix_attr', "
        "table='gameplay_attr_alloc', axes=[{name:'gameplay',source:'gameplay'},{name:'attr',source:'attr'}], "
        "brief='查询玩法子系统在指定属性上的投放占比，返回 0~1 小数，无等级维')`；\n"
        "  6. `update_table_readme`：写每行子系统选这些属性的设计意图、留 0 的原因。\n"
        "★ 行覆盖步骤 2 注册的所有玩法表 table_id；列覆盖所有勾选属性；≥80% 属性出现在 ≥2 个子系统中。\n"
        "★ 若 get_gameplay_table_list 返回空，须暂停并提示用户：步骤 2 玩法规划尚未完成。\n"
        "★ register_calculator 的 brief 必填，应说明用途与入参语义。"
    ),
    "cultivation_resource_framework": (
        _NAMING_HEADER
        + "【步骤 6/7+N 养成资源框架】\n"
        "目标：设计资源产出曲线，建立资源框架表。\n"
        "操作：\n"
        "  1. 设计阶段先列出所有资源（≥2 货币 + 各父玩法的专属道具；RPG 类型必须含 experience），"
        "先 `glossary_register` 每个资源；\n"
        "  2. 创建 `num_resource_framework`（display_name=「养成资源·框架」, directory='基础/资源'），"
        "行=level（1..max_level），列至少含：\n"
        "     `level / time_weight / stay_hours_per_level / stay_hours_cumulative` "
        "+ 每个资源三档：`<res>_per_hour / <res>_per_level / <res>_cumulative`；\n"
        "  3. `time_weight` 必须单调递增（先 const_register 起止与曲线指数）；\n"
        "  4. `stay_hours_per_level = (time_weight / SUM(@@T[time_weight])) * ${lifecycle_days} * "
        "${daily_play_hours}`，公式登记到 _formula_registry；\n"
        "  5. `stay_hours_cumulative = CUMSUM_TO_HERE(@@T[stay_hours_per_level])`；\n"
        "  6. 对每个资源 res：`<res>_per_hour` 自行设计单调曲线；`<res>_per_level = @T[<res>_per_hour] * @T[stay_hours_per_level]`；"
        "`<res>_cumulative = CUMSUM_TO_HERE(@@T[<res>_per_level])`。\n"
        "★ 单位统一为小时；带小数精度；末行 stay_hours_cumulative ≈ 生命周期总时长。\n"
        "★ 所有资源名先 glossary_register，README 用 $name$ 引用。"
    ),
    "cultivation_allocation": (
        _NAMING_HEADER
        + "【步骤 7/7+N 养成资源分配（matrix 表）】\n"
        "目标：行=玩法子系统（与 gameplay_attr_alloc 一致，来自步骤 2 玩法规划），\n"
        "      列=资源（来自步骤 6 资源框架），单元格=该子系统对该资源的投放比例。\n"
        "操作：\n"
        "  1. `get_gameplay_table_list()` → 获取步骤 2 注册的全部玩法表 table_id 列表（作为行）；\n"
        "  2. `get_project_config` 或读取 num_resource_framework 列名 → 确认资源列表（作为列）；\n"
        "  3. `create_matrix_table(name='gameplay_res_alloc', kind='matrix_resource', "
        "rows=[步骤1取到的 table_id 列表], cols=[步骤2确认的资源列表], directory='分配/玩法资源')`；\n"
        "  4. `write_matrix_cells` 填二维基准比例（允许 0 表示不投放）；第三维轴值（如等级）允许手填，"
        "但若同表出现多个第三维切片，内容必须统一改成 formula，不能手填多切片常量；\n"
        "  5. `register_calculator(name='gameplay_res_alloc_lookup', kind='matrix_lookup', "
        "table='gameplay_res_alloc', axes=[{name:'gameplay',source:'row'},{name:'res',source:'col'},"
        "{name:'grain',source:'param',values:['per_hour','per_level','cumulative']}], "
        "brief='查询玩法子系统在指定资源上的投放量；grain 选 per_hour/per_level/cumulative，"
        "内部从 num_resource_framework 取对应列再乘以分配比例')`；\n"
        "  6. README 列出 (玩法×资源) 切片示例。\n"
        "★ register_calculator 必须含 grain 形参；brief 必填，应说明用途。\n"
        "★ 留 0 的单元格在 README 注明设计原因（scope 隔离）。\n"
        "★ 若 get_gameplay_table_list 返回空，须暂停并提示用户：步骤 2 玩法规划尚未完成。"
    ),
    "gameplay_planning": (
        _NAMING_HEADER
        + "【步骤 2/7+N 玩法规划】\n"
        "（本步完成后将注册 N 张动态玩法落地表，形成步骤 8..7+N）\n"
        "目标：分析游戏配置，规划所有需要单独出落地表的玩法系统，注册到玩法表清单。\n"
        "顺序：\n"
        "1. 生产资源的挑战类玩法逐个落地\n"
        "2. 生产属性的养成类玩法逐个落地，同时每个养成玩法完成后还要额外完成本玩法的玩家属性模型\n"
        "3. 玩家属性模型（免费玩家\\标准玩家\\付费玩家）逐个落地\n"
        "4. 怪物数值逐个落地\n"
        "5. 技能设计逐个落地\n"
        "**你应当通读所有SKILL了解各项数值工作的原则，做出优质的初步设计**\n"
        "操作：\n"
        "  0. `list_skills(step_id=\"gameplay_planning\")` + `render_skill_file` → 通读可用 SKILL 制作说明；\n"
        "  1. `get_project_config` → 读取 fixed_layer_config.game_systems 了解启用的玩法系统；\n"
        "  2. `get_default_system_rules` → 了解每个系统的默认子维度约束；\n"
        "  3. 分析每个启用的父系统，根据 get_default_system_rules 的约束自行拆分子维度\n"
        "     （示例仅供参考：equip 可能拆为 equip_base + equip_enhance；具体拆法由游戏设计决定）；\n"
        "  4. 规划依赖关系：有互相引用关系的表（如 equip_enhance 引用 equip_base 数值）需在 dependencies 中声明；\n"
        "  5. 按合理顺序（依赖先行）设置 order_num（从 1 开始）；\n"
        "  6. 逐个调用 `register_gameplay_table(table_id, display_name, readme, order_num, dependencies)` 完成注册；\n"
        "     · table_id：英文 snake_case（如 equip_enhance）\n"
        "     · display_name：中文表名\n"
        "     · readme：至少 50 字，说明玩法目标、关键列、依赖关系\n"
        "  7. 最后调用 `get_gameplay_table_list` 确认所有表均已注册且状态为「未开始」。\n"
        "★ 本步严禁调用 create_table / write_cells / setup_level_table 等建表写数工具。\n"
        "★ 每个在 game_systems 中启用的玩法系统至少注册 1 张表；\n"
        "  根据系统复杂度和 get_default_system_rules 的细则决定是否拆子表。\n"
        "验收：get_gameplay_table_list 返回非空列表，所有表状态均为「未开始」。"
    ),
    "gameplay_table": (
        _NAMING_HEADER
        + "【步骤 8+/7+N 玩法落地表（动态步骤，任务队列模式）】\n"
        "目标：从任务队列中选择一张最合适的玩法表，完成其数值设计并标记完成。\n"
        "每次只做一张表；系统会自动检查队列，若仍有未完成任务则发起下一轮 agent。\n\n"
        "【步骤一：选择任务（必须执行）】\n"
        "  1. `get_gameplay_table_list()` → 获取完整任务队列（含每张表的 status、order_num、dependencies、revision_reason）；\n"
        "  2. 从列表中选择**一张**最合适的任务，选择规则（按优先级）：\n"
        "     a. 优先选 status='未开始'、依赖均已完成（dependencies 中各表均为 '已完成'）、order_num 最小者；\n"
        "     b. 若无符合条件的「未开始」任务，再考虑 status='待修订'、依赖均已完成的表；\n"
        "     c. 若所有表都有阻塞依赖，选 order_num 最小的那张并说明情况后继续；\n"
        "  3. 记录选中的 table_id，后续步骤全部围绕它展开。\n\n"
        "【步骤二：执行任务】\n"
        "  1. `set_gameplay_table_status(table_id, '进行中')` → 标记开始；\n"
        "  2. gather 阶段（只读）：\n"
        "     · `list_exposed_params(gameplay_table.<table_id>)` → 【必须调用】获取上游暴露参数；\n"
        "     · 若有 dependencies，读取被依赖表的结构和数据；\n"
        "     · 若是「待修订」任务：`revision_reason` 字段已在 get_gameplay_table_list 结果中，无需额外工具；\n"
        "  3. design → execute：\n"
        "     · 若是「未开始」任务：按 readme 完成该表的完整数值设计；\n"
        "     · 若是「待修订」任务：按 revision_reason 对已有数据执行修订；\n"
        "     · 属性值：`call_calculator(gameplay_attr_alloc_lookup, ...)` 取，不硬编码；\n"
        "     · 资源消耗：`call_calculator(gameplay_res_alloc_lookup, ...)` 取；\n"
        "     · 若发现另一张已完成表数值需调整：`request_table_revision(table_id, reason)` 入队等待后续处理；\n"
        "     · 若需向兄弟表暴露约束参数：`expose_param_to_subsystems`；\n"
        "  4. 若你认为当前工作需要的必要前置工作没有完成或者信息不足，`request_table_revision(table_id, reason)` 入队等待后续处理，详细描述你需要其完成的前置工作内容；若当前无法推进，结束本次任务并将本任务标记为「未开始」方便后续回归；\n"
        "  5. 若任务完成，`set_gameplay_table_status(table_id, '已完成')` → 标记完成；\n"
        "  6. `update_table_readme` 更新表 README（包含设计决策和关键数值说明）。\n\n"
        "★ 完成以上步骤后即可结束。系统会在本步结束后自动检查队列，若有剩余任务将发起新一轮 agent。\n"
        "★ list_exposed_params 返回空列表时说明无上游约束，继续执行即可。\n"
        "★ 每次 agent 只完成一张表，专注完成好一张，不要尝试处理多张。"
    ),
}


_ROUTE_SYSTEM = (
    "你是 Numflow 的提示词路由器。"
    "给定当前 pipeline 步骤、用户需求与项目配置摘要，"
    "判断默认提示词模板是否能直接覆盖本次任务。"
    "返回严格 JSON：{\"hit\": true|false, \"rationale\": \"一句话理由\"}。"
    "若用户描述明显偏离默认（提出新机制、跨步骤、特殊定制），返回 hit=false。"
)

# Public alias for agent_runner to embed in SSE events
ROUTE_SYSTEM = _ROUTE_SYSTEM

# 步骤 ID → 提示词库展示标题（中文）
_STEP_ID_TITLE_ZH: Dict[str, str] = {
    "environment_global_readme":       "01 环境与全局说明",
    "gameplay_planning":               "02 玩法规划",
    "base_attribute_framework":        "03 基础属性框架",
    "hp_formula_derivation":           "04 HP公式推导",
    "gameplay_allocation":             "05 玩法属性分配",
    "cultivation_resource_framework":  "06 养成资源框架",
    "cultivation_allocation":          "07 养成资源分配",
    "gameplay_table":                  "08+ 玩法落地表（动态）",
}

_ROUTER_PROMPT_GROUP_META: Dict[str, tuple] = {
    "sys_router":      ("路由控制",     30, "判断是否命中步骤模板，未命中时临时生成专属提示词。"),
    "sys_route_steps": ("步骤默认模板", 40, "各步骤在路由命中时直接注入的默认提示词。"),
}


def _router_sys_meta(group_key: str, name_zh: str, summary_zh: str) -> Dict[str, Any]:
    label, order, hint = _ROUTER_PROMPT_GROUP_META[group_key]
    return {
        "tool_group_key": group_key,
        "tool_group_label": label,
        "tool_group_order": order,
        "tool_group_hint": hint,
        "tool_name_zh": name_zh,
        "tool_summary_zh": summary_zh,
    }


def _router_prompt_defaults() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = [
        {
            "category": "system",
            "prompt_key": "router_system",
            "title": "提示词路由判断提示词",
            "summary": "用于判断默认步骤提示词是否能直接覆盖当前任务。",
            "description": "prompt_router 的路由判断 system prompt。",
            "reference_note": "在 prompt_router.route_prompt 中作为路由判断的 system prompt 使用，直接影响默认步骤模板是否命中。",
            "enabled": True,
            "display_order": 1,
            "modules": [
                {
                    "module_key": "body",
                    "title": "完整提示词",
                    "content": _ROUTE_SYSTEM,
                    "required": True,
                    "enabled": True,
                    "sort_order": 1,
                }
            ],
            **_router_sys_meta("sys_router", "路由判断提示词", "判断当前任务是否命中某个默认步骤提示词模板。"),
        },
        {
            "category": "system",
            "prompt_key": "router_writer",
            "title": "路由兜底提示词撰写器",
            "summary": "当默认模板未命中时，用于让模型现写一段当前步骤提示词。",
            "description": "prompt_router 的兜底 system prompt。",
            "reference_note": "在 prompt_router.route_prompt 中，当默认模板未命中时作为 system prompt 调用，用来生成一段新的 routed_prompt。",
            "enabled": True,
            "display_order": 2,
            "modules": [
                {
                    "module_key": "body",
                    "title": "完整提示词",
                    "content": (
                        "你是 Numflow 的提示词撰写器。基于当前 pipeline 步骤、用户需求与项目配置，"
                        "写一段简短（<=300 字）的「玩法/系统」提示词，明确本次任务必产出（表名/列名/验收标准）。"
                        "**所有表/列名必须英文 snake_case，中文走 display_name；公式中的浮点字面量必须以 ${name} 引用常数；"
                        "等级行数必须从 system_level_caps[<system>] 或 max_level 派生，禁止硬编码 30 / 60 / 100。**"
                        "不要寒暄；直接输出提示词本体。"
                    ),
                    "required": True,
                    "enabled": True,
                    "sort_order": 1,
                }
            ],
            **_router_sys_meta("sys_router", "路由兜底生成器", "当路由未命中时，临时为当前步骤生成一段专属提示词。"),
        },
    ]
    for idx, (step_id, prompt) in enumerate(DEFAULT_STEP_PROMPTS.items(), start=10):
        title_zh = _STEP_ID_TITLE_ZH.get(step_id, step_id)
        items.append(
            {
                "category": "system",
                "prompt_key": f"route_step::{step_id}",
                "title": f"步骤默认提示词：{title_zh}",
                "summary": f"步骤【{title_zh}】的默认路由提示词模板。",
                "description": "当默认 SKILL 未覆盖且路由命中时使用。",
                "reference_note": f"在 prompt_router.route_prompt 中，当 step_id={step_id} 且默认模板命中时，这段提示词会作为 routed_prompt 注入 agent 的 design/review/execute 三阶段。",
                "enabled": True,
                "display_order": idx,
                "modules": [
                    {
                        "module_key": "body",
                        "title": "完整提示词",
                        "content": prompt,
                        "required": True,
                        "enabled": True,
                        "sort_order": 1,
                    }
                ],
                **_router_sys_meta("sys_route_steps", f"步骤模板：{title_zh}", f"路由命中 {step_id} 时直接注入的默认提示词。"),
            }
        )
    return items


def get_router_prompt_catalog(
    conn: Optional[sqlite3.Connection] = None,
    global_conn: Optional[sqlite3.Connection] = None,
) -> List[Dict[str, Any]]:
    defaults = _router_prompt_defaults()
    if conn is None and global_conn is None:
        return defaults
    items: List[Dict[str, Any]] = []
    for default in defaults:
        prompt_key = str(default["prompt_key"])
        global_override = get_prompt_override(global_conn, category="system", prompt_key=prompt_key) if global_conn is not None else None
        project_override = get_prompt_override(conn, category="system", prompt_key=prompt_key) if conn is not None else None
        items.append(merge_prompt_item_layers(default, [global_override, project_override]))
    items.sort(key=lambda item: (int(item.get("display_order") or 0), str(item.get("title") or "")))
    return items


def _resolve_router_prompt(
    conn: Optional[sqlite3.Connection],
    prompt_key: str,
    *,
    global_conn: Optional[sqlite3.Connection] = None,
) -> str:
    defaults = {str(item["prompt_key"]): item for item in _router_prompt_defaults()}
    prompt_keys = [prompt_key]
    default_keys = [prompt_key]
    if prompt_key.startswith("route_step::"):
        step_id = prompt_key.split("::", 1)[1]
        prompt_keys = _router_prompt_keys(step_id)
        default_keys = _router_default_prompt_keys(step_id)
    default = None
    for key in default_keys:
        default = defaults.get(key)
        if default is not None:
            break
    if default is None:
        raise KeyError(prompt_key)
    if conn is None and global_conn is None:
        return render_prompt_text(default)
    global_override = None
    if global_conn is not None:
        for key in prompt_keys:
            global_override = get_prompt_override(global_conn, category="system", prompt_key=key)
            if global_override is not None:
                break
    override = None
    if conn is not None:
        for key in prompt_keys:
            override = get_prompt_override(conn, category="system", prompt_key=key)
            if override is not None:
                break
    return render_prompt_text(merge_prompt_item_layers(default, [global_override, override]))


def get_route_system_prompt(
    conn: Optional[sqlite3.Connection] = None,
    global_conn: Optional[sqlite3.Connection] = None,
) -> str:
    return _resolve_router_prompt(conn, "router_system", global_conn=global_conn)


def get_default_step_prompt(
    step_id: str,
    conn: Optional[sqlite3.Connection] = None,
    global_conn: Optional[sqlite3.Connection] = None,
) -> str:
    return _resolve_router_prompt(conn, f"route_step::{step_id}", global_conn=global_conn)


def route_prompt(
    step_id: str,
    user_message: str,
    project_config_summary: str,
    *,
    model: str = None,
    conn: Optional[sqlite3.Connection] = None,
    global_conn: Optional[sqlite3.Connection] = None,
) -> Dict[str, Any]:
    """决定本次对话使用哪段提示词。

    返回：{"hit": bool, "prompt": str, "rationale": str}
      - hit=True：使用 DEFAULT_STEP_PROMPTS[step_id]
      - hit=False：让千问现编一段提示词
    """
    if conn is not None:
        try:
            skill_bundle = build_default_skill_prompt(
                conn,
                step_id,
                record_usage_events=True,
            )
        except Exception:
            skill_bundle = {"skills": [], "prompt": ""}
        skill_prompt = str(skill_bundle.get("prompt") or "").strip()
        if skill_prompt:
            skill_items = skill_bundle.get("skills") or []
            return {
                "hit": True,
                "prompt": skill_prompt,
                "gather_hint": _extract_gather_hint(skill_prompt),
                "rationale": "skill_library_default_exposure",
                "skills": skill_items,
                "route_system": get_route_system_prompt(conn, global_conn=global_conn),
            }

    # 确定步骤默认提示词：精确匹配 → 父步骤 fallback（如 gameplay_table.equip_enhance → gameplay_table）
    normalized_step_id = _normalize_prompt_step_id(step_id)
    if normalized_step_id in DEFAULT_STEP_PROMPTS:
        default_prompt = get_default_step_prompt(step_id, conn, global_conn=global_conn)
    else:
        base_id = normalized_step_id.split(".")[0] if "." in normalized_step_id else ""
        default_prompt = get_default_step_prompt(base_id, conn, global_conn=global_conn) if base_id in DEFAULT_STEP_PROMPTS else ""
    client = get_client_for_model(model or QWEN_MODEL)

    judge_user = (
        f"当前 pipeline 步骤 ID: {step_id or '(未知)'}\n"
        f"默认模板（可能为空）:\n{default_prompt or '(无默认模板)'}\n\n"
        f"用户需求:\n{user_message}\n\n"
        f"项目配置摘要:\n{project_config_summary[:1200]}\n\n"
        "请只输出 JSON。"
    )

    try:
        resp = client.chat.completions.create(
            model=model or QWEN_MODEL,
            messages=[
                {"role": "system", "content": get_route_system_prompt(conn, global_conn=global_conn)},
                {"role": "user", "content": judge_user},
            ],
            temperature=0.1,
            max_tokens=400,
            response_format={"type": "json_object"},
        )
        raw = (resp.choices[0].message.content or "").strip()
        verdict = json.loads(raw)
    except Exception as e:  # noqa: BLE001
        fallback_prompt = default_prompt or "（路由失败且无默认模板，按通用 Numflow 数值策划助手处理本任务。）"
        return {
            "hit": bool(default_prompt),
            "prompt": fallback_prompt,
            "gather_hint": _extract_gather_hint(fallback_prompt),
            "rationale": f"router_fallback: {e!r}",
            "route_system": get_route_system_prompt(conn, global_conn=global_conn),
        }

    hit = bool(verdict.get("hit")) and bool(default_prompt)
    rationale = str(verdict.get("rationale") or "")[:400]

    if hit:
        return {
            "hit": True,
            "prompt": default_prompt,
            "gather_hint": _extract_gather_hint(default_prompt),
            "rationale": rationale,
            "route_system": get_route_system_prompt(conn, global_conn=global_conn),
        }

    # 未命中：让千问现写一段对应本步骤的提示词（仍套上命名纪律前缀）
    try:
        gen = client.chat.completions.create(
            model=model or QWEN_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": _resolve_router_prompt(conn, "router_writer", global_conn=global_conn),
                },
                {
                    "role": "user",
                    "content": judge_user,
                },
            ],
            temperature=0.2,
            max_tokens=400,
        )
        custom = (gen.choices[0].message.content or "").strip()
        # 模型偶尔会把提示词裹进 {"prompt": "..."} JSON；解开避免噪声
        if custom.startswith("{") and "\"prompt\"" in custom[:40]:
            try:
                obj = json.loads(custom)
                if isinstance(obj, dict) and isinstance(obj.get("prompt"), str):
                    custom = obj["prompt"].strip()
            except Exception:
                pass
        # 保证命名纪律前缀贴在最前
        if _NAMING_HEADER.strip() not in custom:
            custom = _NAMING_HEADER + custom
    except Exception as e:  # noqa: BLE001
        custom = default_prompt or f"（提示词生成失败：{e!r}；按通用助手处理）"

    return {
        "hit": False,
        "prompt": custom,
        "gather_hint": _extract_gather_hint(custom),
        "rationale": rationale or "default_template_not_matched",
        "route_system": get_route_system_prompt(conn, global_conn=global_conn),
    }
