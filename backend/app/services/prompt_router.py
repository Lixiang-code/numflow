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
from typing import Any, Dict

from app.config import QWEN_MODEL
from app.services.qwen_client import get_client


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


# 与 routers/pipeline.py PIPELINE_STEPS 一一对应的默认提示词模板。
# 每段简短描述：本步必产出（表名/列名/接受标准），便于 design 阶段对齐。
DEFAULT_STEP_PROMPTS: Dict[str, str] = {
    "environment_global_readme": (
        _NAMING_HEADER
        + "【步骤 1/11 环境与全局 README】\n"
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
        "4. 用 `const_register` 写入项目级常数：max_level、各属性的起步值/封顶值（如 `hp_lv1`、`hp_max_cap`）、"
        "曲线指数（`growth_exp` 默认 0.85）、暴击/暴伤上限等。\n"
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
        + "【步骤 2/11 基础属性框架】\n"
        "目标：定义角色基础属性骨架，输出 1..max_level 行的标准等级基础属性表。\n"
        "先 `get_project_config` → 读 fixed_layer_config.core.game_type 与 attribute_systems.selectedAttrs："
        "  · rpg_turn 通常用 hp_max/atk/def/hit/dodge/crit_rate/crit_dmg；\n"
        "  · rpg_realtime 额外含 atk_spd/move_spd/base_atk_interval；\n"
        "  · 实际属性键以项目 stat_keys 为准。\n"
        "必产出表：`base_attr_table`（display_name=「基础属性·标准等级」）；列由 stat_keys 推导。\n"
        "★ 强制效率方式：用 `setup_level_table` 一次完成（建表+max_level 行+所有列公式注册执行）。\n"
        "★ 公式必须使用常数引用，不允许字面量。先 `const_register` 起止值与曲线指数，再写公式：\n"
        "  hp_max  : ROUND(${hp_lv1} + (${hp_max_cap}-${hp_lv1})*POWER((@T[level]-1)/(${max_level}-1), ${growth_exp}), 0)\n"
        "  atk     : ROUND(${atk_lv1}+ (${atk_max_cap}-${atk_lv1})*POWER((@T[level]-1)/(${max_level}-1), ${growth_exp}), 0)\n"
        "  atk_spd : ROUND(${atk_spd_lv1}+(${atk_spd_max}-${atk_spd_lv1})*POWER((@T[level]-1)/(${max_level}-1),${atk_spd_exp}), 2)\n"
        "建表完成后调 `update_table_readme` 写完整 6 字段 README。"
    ),
    "gameplay_attribute_scheme": (
        _NAMING_HEADER
        + "【步骤 3/11 玩法属性方案】\n"
        "目标：列出各玩法系统拟提供的属性维度与占比策略。\n"
        "必产出表：`gameplay_attr_scheme`（display_name=「玩法属性方案」），"
        "列：system / provided_stats / share_strategy / cap_note / acquire_rhythm。\n"
        "约束：每个 02 文档约定的核心系统都有一行；`provided_stats` 的属性键必须来自 stat_keys。"
    ),
    "gameplay_allocation_tables": (
        _NAMING_HEADER
        + "【步骤 4/11 玩法属性分配表】\n"
        "目标：把方案细化为「按系统×标准等级」的属性占比表。\n"
        "必产出：每个核心系统一张 `<system>_alloc`（如 `equip_alloc`、`mount_alloc`），"
        "display_name=「<系统中文>·属性分配」；行=标准等级 1..max_level（坐骑等可独立子上限），"
        "列=该系统提供的各属性的占比（小数 0..1，number_format='0.00%'）。\n"
        "★ 强制效率方式：每张分配表用 `setup_level_table` 一次建好；占比固定时常量列公式 `${equip_hp_share}`；"
        "随等级渐变 `${equip_hp_share_lv1} + (${equip_hp_share_max}-${equip_hp_share_lv1})*((@T[level]-1)/(${max_level}-1))`。\n"
        "★ 行数 = `system_level_caps[<system>]` 若存在否则 `max_level`；**严禁** 硬编码 30/60/100，"
        "**严禁** 逐行 `write_cells`。\n"
        "★ kind=alloc，建表时建议显式传 `kind: 'alloc'` 让系统自动挂 percent_bounds 校验。\n"
        "验收：横向加总 ≤ 1.0；空缺列在 README 注明理由。"
    ),
    "second_order_framework": (
        _NAMING_HEADER
        + "【步骤 5/11 二阶属性框架】\n"
        "目标：派生战力、伤害公式相关二阶属性。\n"
        "必产出：`second_order_formula`（列：metric / formula / depends_on / lower / upper）。"
        "如已 const_register 暴击率上限/暴伤上限，公式中以 `${crit_rate_cap}` 等引用。\n"
        "验收：与 02 默认细则一致；`recalculate_downstream` 能跑通。"
    ),
    "gameplay_attribute_tables": (
        _NAMING_HEADER
        + "【步骤 6/11 玩法属性表】\n"
        "目标：分配比例 × 标准等级基础属性 → 各系统每级实际属性。\n"
        "必产出：每个系统一张 `<system>_attr`（如 `equip_attr`、`mount_attr`），"
        "display_name=「<系统中文>·属性表」；行 = 该系统等级/品阶/阶段，列 = 具体属性数值。\n"
        "★ 注意：宝石使用品阶/合成体系（3 同阶=1 高 1 品）→ 表名 `gem_attr`，行=品阶（rank/tier），不是 1..N 等级。\n"
        "★ 坐骑等子系统的开放等级行数 = `system_level_caps.mount`（缺省 = max_level），**不要写 30**。\n"
        "★ 强制效率方式：用 `setup_level_table` 或 `bulk_register_and_compute`，公式直接引用上游表："
        "  hp_max : ROUND(@base_attr_table[hp_max] * @<system>_alloc[hp_share], 0)。\n"
        "**严禁** 逐行写。"
    ),
    "cultivation_resource_design": (
        _NAMING_HEADER
        + "【步骤 7/11 养成资源设计】\n"
        "目标：列出每个系统的养成资源（材料、消耗道具、产出节奏）。\n"
        "必产出：`cultivation_resource_list`（列：resource_id / resource_name / source / sink / "
        "rarity / typical_daily_yield）。\n"
        "约束：覆盖全部需要养成的系统；resource_id 必须存在于 `_project_settings.resource_keys` 中。"
    ),
    "cultivation_resource_framework": (
        _NAMING_HEADER
        + "【步骤 8/11 养成资源框架】\n"
        "目标：搭建资源在系统间的流转框架（产出→背包→消耗→升级→属性）。\n"
        "必产出：每个系统一张骨架表 `<system>_cultivation_node`（升级节点 / 消耗资源 / 产出属性档），"
        "并在 README 标注瓶颈点与产出口径。"
    ),
    "cultivation_allocation_tables": (
        _NAMING_HEADER
        + "【步骤 9/11 养成分配表】\n"
        "目标：把资源在各系统、各等级上的消耗量初稿铺开。\n"
        "必产出：每个系统一张 `<system>_cultivation_alloc`（行=系统等级，列=各资源消耗量）。\n"
        "★ 强制效率方式：用 `setup_level_table`，公式样例（先 const_register 起步/缩放系数）：\n"
        "  ROUND(${cost_base} + ${cost_growth}*POWER(@T[level], ${cost_exp}), 0)\n"
        "  或分段：IFS(@T[level]<=${tier1_cap}, ${cost_t1}, @T[level]<=${tier2_cap}, ${cost_t2}, ${cost_tn})\n"
        "★ 行数 = `system_level_caps[<system>]` 否则 `max_level`；纵向递增合理；不出现 0 消耗跳级。"
    ),
    "cultivation_quant_tables": (
        _NAMING_HEADER
        + "【步骤 10/11 养成量化表】\n"
        "目标：在分配表基础上做量化（具体数值定稿），可注册公式自动推导。\n"
        "必产出：每个系统一张 `<system>_cultivation_quant`（列：单级消耗 / 累计消耗 / 对应属性 / 性价比）。\n"
        "★ 强制效率方式：`bulk_register_and_compute` 一次注册多个跨表公式；累计列 = `CUMSUM_TO_HERE(@@T[cost])`；"
        "性价比 = `@T[stat_gain] / @T[cum_cost]`。\n"
        "★ 性价比/单位收益类列必须存在阶段性拐点或饱和（system 自带 monotone_warning 校验）。"
    ),
    "gameplay_landing_tables": (
        _NAMING_HEADER
        + "【步骤 11/11 玩法落地表（汇总入口）】\n"
        "目标：本步骤已被拆为 per-system 子步（11.equip / 11.gem / 11.dungeon ...）。\n"
        "通用要求：\n"
        "（1）禁止「仅标准等级+两列消耗」的偷懒模板；列必须有玩法含义。\n"
        "（2）数值列若可由公式生成 → 必须 `setup_level_table` / `bulk_register_and_compute` 注册公式并执行；不留空。\n"
        "（3）行数 = `system_level_caps[<system>]` 否则 `max_level`；行数缺失=验收失败。\n"
        "（4）暴击/闪避/命中/抗性等百分比列存为 [0, 0.95] 小数，number_format='0.00%'；"
        "暴伤存为小数（150% → 1.5），number_format='0.00%'。\n"
        "（5）资源/材料消耗以「日产量×天数 ≈ 累计消耗」自检（CUMSUM_TO_HERE）。"
    ),
    "gameplay_landing_tables.equip": (
        _NAMING_HEADER
        + "【步骤 11.装备 — 落地表】\n"
        "产出：\n"
        "  · `equip_landing`（display_name=「装备·落地」；列：slot / quality / refine_level / "
        "enhance_level / unlock_level / attr_pool）；\n"
        "  · `equip_attr`（行=强化等级，列=hp_max/atk/def/...，公式 = "
        "`@base_attr_table[hp_max] * @equip_alloc[hp_share]`）；\n"
        "  · `equip_cultivation_quant`（每级消耗 / 累计消耗 / 性价比）。\n"
        "★ 主属性覆盖比若为常数请先 `const_register('equip_main_attr_ratio', 0.6)` 再以 `${equip_main_attr_ratio}` 引用。\n"
        "★ 暴击/闪避在 [0, 0.95] 小数 + 0.00%；性价比禁严格单调递增。"
    ),
    "gameplay_landing_tables.gem": (
        _NAMING_HEADER
        + "【步骤 11.宝石 — 落地表】\n"
        "产出：\n"
        "  · `gem_landing`（display_name=「宝石·落地」；列：color / tier / synthesis_rule / unlock_level / attr_pool / share）；\n"
        "  · `gem_attr`（行=品阶 tier，列=具体属性）。\n"
        "★ 合成路径以 const_register 抽出（如 `gem_synth_input=3`、`gem_synth_output=1`），"
        "在 README 文字阐述「3 同阶 → 1 高 1 品」并在 schema 中以列体现。\n"
        "★ 不要把标准等级 1..N 当成「宝石 N 级」。"
    ),
    "gameplay_landing_tables.mount": (
        _NAMING_HEADER
        + "【步骤 11.坐骑 — 落地表】\n"
        "产出：\n"
        "  · `mount_landing`（display_name=「坐骑·落地」；列：stage / unlock_level / activate_cond / advance_cost）；\n"
        "  · `mount_attr`（行=阶段，列=hp_max/atk 等）；\n"
        "  · `mount_cultivation_quant`（每阶消耗 / 累计 / 性价比）。\n"
        "★ 行数与等级范围：从 `get_project_config().settings.fixed_layer_config.system_level_caps.mount` 读取，"
        "若未配置则回退 `max_level`。**严禁硬编码 30**。如设计师确实希望坐骑只到某个独立上限，"
        "请引导用户在初始化中心配置 `system_level_caps.mount`，然后由该值驱动。\n"
        "★ 进阶曲线非线性（用 ${mount_growth_exp} 等常数控制），列必须有玩法含义。"
    ),
    "gameplay_landing_tables.wing": (
        _NAMING_HEADER
        + "【步骤 11.翅膀 — 落地表】\n"
        "产出：`wing_landing`（stage/feather_cost/attr_pool）+ `wing_attr` + `wing_cultivation_quant`。\n"
        "★ 消耗资源 ID 来自 `_project_settings.resource_keys`；行数从 `system_level_caps.wing` 派生。"
    ),
    "gameplay_landing_tables.fashion": (
        _NAMING_HEADER
        + "【步骤 11.时装 — 落地表】\n"
        "产出：`fashion_landing`（suite/quality/attr_bonus/aesthetic_vs_combat 标签）。\n"
        "★ 纯外观时装可 0 战斗属性；战斗时装需明确属性增量列。"
    ),
    "gameplay_landing_tables.dungeon": (
        _NAMING_HEADER
        + "【步骤 11.副本 — 落地表】\n"
        "产出：`dungeon_landing`（列：dungeon_id / open_level / ticket_cost / daily_max_count / "
        "reward_drop / cumulative_ticket / value_per_ticket）。\n"
        "★ dungeon_id 用 IFS 公式分段批量生成。分段阈值不允许硬编码：\n"
        "  先 const_register 各阶段上限 ${dungeon_tier1_cap}, ${dungeon_tier2_cap}, ...；再写：\n"
        "  IFS(@T[level]<=${dungeon_tier1_cap}, 1, @T[level]<=${dungeon_tier2_cap}, 2, 3)\n"
        "★ cumulative_ticket = CUMSUM_TO_HERE(@@T[ticket_cost])；value_per_ticket = `@T[reward_value]/@T[cumulative_ticket]`。\n"
        "★ 注册公式后必须 execute（无空值）；性价比禁严格单调递增。"
    ),
    "gameplay_landing_tables.skill": (
        _NAMING_HEADER
        + "【步骤 11.技能 — 落地表】\n"
        "产出：`skill_landing`（skill_id/type/unlock_level/cooldown/dmg_ratio/resource_cost）+ "
        "`skill_cultivation_quant`（每级提升 / 累计消耗）。\n"
        "★ dmg_ratio 存小数 + 0.00%；MP/能量消耗与技能强度匹配。"
    ),
}


_ROUTE_SYSTEM = (
    "你是 Numflow 的提示词路由器。"
    "给定当前 pipeline 步骤、用户需求与项目配置摘要，"
    "判断默认提示词模板是否能直接覆盖本次任务。"
    "返回严格 JSON：{\"hit\": true|false, \"rationale\": \"一句话理由\"}。"
    "若用户描述明显偏离默认（提出新机制、跨步骤、特殊定制），返回 hit=false。"
)


def route_prompt(
    step_id: str,
    user_message: str,
    project_config_summary: str,
    *,
    model: str = None,
) -> Dict[str, Any]:
    """决定本次对话使用哪段提示词。

    返回：{"hit": bool, "prompt": str, "rationale": str}
      - hit=True：使用 DEFAULT_STEP_PROMPTS[step_id]
      - hit=False：让千问现编一段提示词
    """
    default_prompt = DEFAULT_STEP_PROMPTS.get(step_id, "")
    client = get_client()

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
                {"role": "system", "content": _ROUTE_SYSTEM},
                {"role": "user", "content": judge_user},
            ],
            temperature=0.1,
            max_tokens=400,
            response_format={"type": "json_object"},
        )
        raw = (resp.choices[0].message.content or "").strip()
        verdict = json.loads(raw)
    except Exception as e:  # noqa: BLE001
        return {
            "hit": bool(default_prompt),
            "prompt": default_prompt
            or "（路由失败且无默认模板，按通用 Numflow 数值策划助手处理本任务。）",
            "rationale": f"router_fallback: {e!r}",
        }

    hit = bool(verdict.get("hit")) and bool(default_prompt)
    rationale = str(verdict.get("rationale") or "")[:400]

    if hit:
        return {"hit": True, "prompt": default_prompt, "rationale": rationale}

    # 未命中：让千问现写一段对应本步骤的提示词（仍套上命名纪律前缀）
    try:
        gen = client.chat.completions.create(
            model=model or QWEN_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "你是 Numflow 的提示词撰写器。基于当前 pipeline 步骤、用户需求与项目配置，"
                        "写一段简短（<=300 字）的「玩法/系统」提示词，明确本次任务必产出（表名/列名/验收标准）。"
                        "**所有表/列名必须英文 snake_case，中文走 display_name；公式中的浮点字面量必须以 ${name} 引用常数；"
                        "等级行数必须从 system_level_caps[<system>] 或 max_level 派生，禁止硬编码 30 / 60 / 100。**"
                        "不要寒暄；直接输出提示词本体。"
                    ),
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
        "rationale": rationale or "default_template_not_matched",
    }
