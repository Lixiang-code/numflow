"""文档 02 子集：可机读默认规则（占位结构，可逐步扩充）。"""

from __future__ import annotations

from typing import Any, Dict

DEFAULT_RULES_02: Dict[str, Any] = {
    "version": 3,
    "source_doc": "游戏数值系统AI化自动开发-02-系统与子系统默认细则.md",
    "assumptions": {
        "max_standard_level": 200,
        "lifecycle_days": 60,
        "power_curve": "分段线性，高等级边际递减",
    },
    "equipment": {
        "slots": 6,
        "slot_roles": ["主手", "副手", "铠甲", "下装", "鞋子", "饰品"],
        "slot_weights_note": "主手攻击1.0 / 副手0.8 / 铠甲防御1.0 / 下装0.8 / 鞋子速度 / 饰品暴击",
        "tier_every_levels": 10,
        "opens_at_level": 1,
        "affix_pools": {
            "attack_pct": {"tiers": [5, 8, 12, 18], "slot_bias": "主手>饰品>副手"},
            "def_pct": {"tiers": [5, 8, 12], "slot_bias": "铠甲>下装"},
        },
    },
    "mount": {
        "unlock_level_default": 30,
        "speed_bonus_tiers": [0.05, 0.08, 0.12],
        "note": "坐骑属性与移动/战斗外围加成挂钩，具体表由项目定义",
        "landing_note": "落地表若以标准等级为轴，须含「是否已开放(≥ 开放等级)」；未开放时消耗/属性为 0 或 N/A，不得与装备/宝石同构为仅金币+掉率。",
    },
    "artifact": {
        "rarity_order": ["N", "R", "SR", "SSR"],
        "star_cap_by_rarity": {"N": 3, "R": 5, "SR": 6, "SSR": 6},
        "reforge_cost_currency": "dust",
    },
    "currencies": {
        "gold": {"bind": False, "overflow_policy": "cap_at_int_max"},
        "bound_diamond": {"bind": True},
        "dust": {"bind": True, "primary_sink": "artifact_reforge"},
    },
    "combat_attrs": {
        "primary": ["atk", "def", "hp", "spd", "crit_rate", "crit_dmg", "hit", "dodge"],
        "derived_notes": "暴击伤害默认 1.5x 基线，可由装备/神器改写",
    },
    # 文档 02 子系统「宝石」及落地轴说明（供 Agent/表结构对齐，避免所有玩法共用「角色 1..N 级」一行一行的机械套表）
    "gem": {
        "synthesis": "3 个同品阶的较低宝石 → 1 个高 1 个品阶的宝石（链式+1 品）",
        "row_axis": "**品阶/合成阶** 为主数据轴；**不是**默认用「角色标准等级 1..200」与宝石表逐行 1:1 对齐",
        "level_relation": "需显式定义：孔位/宝石槽的开放节奏与**标准等级**的**门槛**映射；低阶可在早期开放，高品阶用更高标准等级门槛 + 资源门槛",
        "attr_relation": "落地表须同时包含**消耗**与**属性/池子或分配**（单孔/多孔主属性类型与权重），与一阶/二阶或玩法属性表可对账",
    },
    "gameplay_landing": {
        "per_system_row_axis": "各主系统/子系统落地**不得**无差别复用同一张 60/200 行「仅标准等级+消耗」模板；应参考本文件 equipment/mount/gem 与 03 落地章节分别建模。",
    },
    "note": "用户未指定时以本结构为最低优先级默认；与 01 勾选系统联动时由 Agent 解释。",
}


def get_default_rules_payload() -> Dict[str, Any]:
    return DEFAULT_RULES_02
