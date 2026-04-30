"""Project-level SKILL library: seed defaults, render markdown, persist files, track usage."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

from app.db.paths import get_project_dir
from app.util_slug import slugify, unique_slug


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


DEFAULT_SKILLS: List[Dict[str, Any]] = [
    {
        "template_key": "landing_common",
        "slug": "landing-common",
        "title": "玩法落地通用制作说明",
        "step_id": "gameplay_table",
        "summary": "所有玩法落地表的共用制作框架，负责统一取值来源、结构纪律、数值格式、README 与验收口径。",
        "description": (
            "当当前步骤处于 `gameplay_table` 或其子步骤时，这个 SKILL 应先作为公共框架使用，"
            "再叠加具体玩法 SKILL。它不替代具体玩法规则，而是负责保证所有落地表遵守同一套生产纪律。"
        ),
        "default_exposed": True,
        "modules": [
            {
                "module_key": "scope",
                "title": "使用时机与职责边界",
                "required": True,
                "enabled": True,
                "content": (
                    "- 本 SKILL 适用于装备、宝石、坐骑、翅膀、时装、副本、技能等所有玩法落地步骤。\n"
                    "- 先用它统一落地方法，再叠加具体玩法的专属 SKILL。\n"
                    "- 它关注的是“如何把玩法做成合格落地表”，不是替代具体系统的设计决策。"
                ),
            },
            {
                "module_key": "production_rules",
                "title": "落地产出与取值来源",
                "required": True,
                "enabled": True,
                "content": (
                    "1. 所有玩法落地表都要明确：主表、属性表、养成/消耗量化表是否齐备。\n"
                    "2. 属性投放、资源投放优先从 `gameplay_attr_alloc` / `gameplay_res_alloc` 及其 calculator 读取，"
                    "不要把分配比例和最终数值硬编码进落地表。\n"
                    "3. 如果存在累计消耗、阶段差值、性价比等列，必须明确是“本级值”还是“累计值”，"
                    "必要时增加辅助列避免语义混乱。"
                ),
            },
            {
                "module_key": "structure_rules",
                "title": "结构纪律与字段要求",
                "required": True,
                "enabled": True,
                "content": (
                    "- 表名、列名全部英文 snake_case；中文只放在 display_name/readme。\n"
                    "- 每张表都要有玩法语义明确的关键列，不能退化成“等级 + 两列消耗”的空壳模板。\n"
                    "- 行轴必须有依据：`system_level_caps[sys]`、`max_level`、品阶枚举、阶段枚举、分段枚举等，"
                    "不得硬编码 30/60/100。\n"
                    "- 百分比/概率类列存小数并带 `0.00%`；倍率类列存小数，避免把 35% 写成 35。"
                ),
            },
            {
                "module_key": "acceptance",
                "title": "README 与验收标准",
                "required": True,
                "enabled": True,
                "content": (
                    "README 至少应覆盖：目标、上游输入、产出表、关键列说明、验收标准、常见误区。\n"
                    "验收时重点检查：\n"
                    "- 行数完整、无关键空值；\n"
                    "- 关键列含义清晰；\n"
                    "- 取值来源可追溯；\n"
                    "- 量纲、格式、累计关系一致；\n"
                    "- 性价比/收益类列存在合理拐点，不是机械单调递增。"
                ),
            },
            {
                "module_key": "pitfalls",
                "title": "常见误区",
                "required": False,
                "enabled": False,
                "content": (
                    "- 把玩法落地表做成通用模板复用，导致各玩法失去差异。\n"
                    "- 直接把基础属性表整列抄到玩法表里，没有体现投放比例或玩法结构。\n"
                    "- 只给出累计消耗，不给本级消耗/阶段消耗，后续运营难以理解。\n"
                    "- 用自然语言写规则，但没有在表结构中落成可读列。"
                ),
            },
        ],
    },
    {
        "template_key": "equip_landing",
        "slug": "equip-landing",
        "title": "装备制作说明",
        "step_id": "gameplay_table.equip",
        "summary": "定义装备的落地主表、属性成长表与养成量化表，体现部位、品质、强化/精炼等装备养成结构。",
        "description": "适用于角色装备系统的落地实现，重点是把装备差异、成长轴和资源消耗做成可运营、可扩展的表结构。",
        "default_exposed": True,
        "modules": [
            {
                "module_key": "goal",
                "title": "设计目标",
                "required": True,
                "enabled": True,
                "content": (
                    "装备系统要同时回答三件事：\n"
                    "1. 玩家穿什么（部位、品质、解锁条件）；\n"
                    "2. 装备怎么长（强化、精炼、增幅等成长轴）；\n"
                    "3. 为什么值得养（属性收益、资源曲线、性价比拐点）。"
                ),
            },
            {
                "module_key": "outputs",
                "title": "建议产出表",
                "required": True,
                "enabled": True,
                "content": (
                    "- `equip_landing`：装备主表，至少体现 `slot / quality / unlock_level / enhance_level / refine_level / attr_pool`。\n"
                    "- `equip_attr`：按成长轴展开的属性表，明确不同部位/品质/等级对应的属性增量。\n"
                    "- `equip_cultivation_quant`：每级消耗、累计消耗、阶段性价比、战力收益等量化表。"
                ),
            },
            {
                "module_key": "field_rules",
                "title": "字段与结构规则",
                "required": True,
                "enabled": True,
                "content": (
                    "- `slot` 必须能区分装备部位；不要把所有装备混成一条成长轴。\n"
                    "- `quality` 与成长系数/属性池应有明确关系；品质不是仅用于展示。\n"
                    "- `attr_pool` 要表达主属性/副属性来源，不要只给最终值。\n"
                    "- 强化与精炼是不同轴时，建议分列或分表表示，避免单列混义。\n"
                    "- 如果主属性覆盖比是固定值，先注册常量再引用，不要在公式里裸写。"
                ),
            },
            {
                "module_key": "growth_and_cost",
                "title": "成长与消耗建议",
                "required": True,
                "enabled": True,
                "content": (
                    "装备成长建议体现“前期直观、中期拉开、高期受资源约束”的曲线：\n"
                    "- 前期强化成本可较平缓，让玩家快速感知成长；\n"
                    "- 中后期通过材料与金币双约束拉开差异；\n"
                    "- 性价比列不应严格单调递增，应出现阶段性平台或回落，鼓励玩家换档决策。"
                ),
            },
            {
                "module_key": "acceptance",
                "title": "验收关注点",
                "required": True,
                "enabled": True,
                "content": (
                    "- 部位差异明确，不同部位不是同一张复制表；\n"
                    "- 品质、强化、精炼至少有一条清晰成长轴；\n"
                    "- 属性与消耗都能回溯到来源；\n"
                    "- 资源曲线与战力收益能解释玩法节奏。"
                ),
            },
            {
                "module_key": "extensions",
                "title": "可选扩展模块",
                "required": False,
                "enabled": False,
                "content": (
                    "如项目需要，可增加：\n"
                    "- 套装效果表；\n"
                    "- 词条库表；\n"
                    "- 洗练/重铸规则；\n"
                    "- 装备来源掉落映射；\n"
                    "- 强化失败保护或保底机制。"
                ),
            },
        ],
    },
    {
        "template_key": "gem_landing",
        "slug": "gem-landing",
        "title": "宝石制作说明",
        "step_id": "gameplay_table.gem",
        "summary": "定义宝石的品类、品阶、合成规则、解锁条件与属性池，强调“品阶/合成”而不是把宝石做成伪等级表。",
        "description": "适用于宝石镶嵌、合成、属性补强类系统，重点在于属性池、品阶成长、合成消耗和解锁节奏的统一。",
        "default_exposed": True,
        "modules": [
            {
                "module_key": "positioning",
                "title": "系统定位",
                "required": True,
                "enabled": True,
                "content": (
                    "宝石系统更像“属性模块化补强 + 资源转化系统”，重点是颜色/类型差异、品阶成长和合成门槛，"
                    "不是单纯复制角色等级曲线。"
                ),
            },
            {
                "module_key": "outputs",
                "title": "建议产出表",
                "required": True,
                "enabled": True,
                "content": (
                    "- `gem_landing`：宝石定义表，至少包含 `color / tier / synthesis_rule / unlock_level / attr_pool / share`。\n"
                    "- `gem_attr`：若存在“品阶/等级 × 宝石类型 × 属性列”三个维度，必须使用真实三维表；"
                    "推荐 `dim1=tier`、`dim2=gem_type`、`cols=atk_bonus/def_bonus/...`，并按多 sheet 方式展示。\n"
                    "- 如有养成量化，可补 `gem_cultivation_quant` 表达单次合成投入、累计投入、回报。"
                ),
            },
            {
                "module_key": "core_rules",
                "title": "核心规则",
                "required": True,
                "enabled": True,
                "content": (
                    "- 先定义宝石类型（如攻击、防御、生存、控制等），再定义每类对应的属性池。\n"
                    "- `tier` 表示品阶或星级，不要把 `level 1..N` 直接映射成“宝石 N 级”。\n"
                    "- 当属性同时受 `tier` 和 `gem_type` 影响时，不要把第二维折叠进 if 串或手填表，直接用三维表 + 公式生成。\n"
                    "- 合成规则建议落成可追踪列，如输入数量、输出品阶、是否保留副属性、失败保护等。\n"
                    "- 合成路径的常量（如 3 合 1）先注册为常量，再在 README 与公式中引用。"
                ),
            },
            {
                "module_key": "growth_and_unlock",
                "title": "成长与解锁节奏",
                "required": True,
                "enabled": True,
                "content": (
                    "宝石要体现两个节奏：\n"
                    "1. 解锁节奏：玩家什么时候接触新孔位/新品类；\n"
                    "2. 合成节奏：资源何时从低阶转向高阶。\n"
                    "同一类宝石的成长可相对平滑，但不同颜色/类型要有明确的投放意图和定位差异。"
                ),
            },
            {
                "module_key": "acceptance",
                "title": "验收关注点",
                "required": True,
                "enabled": True,
                "content": (
                    "- 属性池和颜色/类型关系清楚；\n"
                    "- 合成规则可被表结构解释；\n"
                    "- 没有把标准等级误当宝石等级；\n"
                    "- 解锁门槛、属性成长、合成成本三者一致。"
                ),
            },
            {
                "module_key": "extensions",
                "title": "可选扩展模块",
                "required": False,
                "enabled": False,
                "content": (
                    "可扩展：孔位解锁表、镶嵌位规则、套色加成、共鸣加成、宝石拆解返还、保底合成等。"
                ),
            },
        ],
    },
    {
        "template_key": "mount_landing",
        "slug": "mount-landing",
        "title": "坐骑制作说明",
        "step_id": "gameplay_table.mount",
        "summary": "定义坐骑进阶、激活、属性成长和养成消耗，重点体现阶段系统和开放等级约束。",
        "description": "适用于以阶段进阶为主的坐骑/伙伴载具系统，需要明确激活、升阶、属性收益和外显感受之间的平衡。",
        "default_exposed": True,
        "modules": [
            {
                "module_key": "goal",
                "title": "设计目标",
                "required": True,
                "enabled": True,
                "content": (
                    "坐骑通常承担“中长期成长 + 形象反馈 + 战力补强”三重职责。落地时要让每一阶都有明确意义，"
                    "而不是只做一个无限拉长的资源黑洞。"
                ),
            },
            {
                "module_key": "outputs",
                "title": "建议产出表",
                "required": True,
                "enabled": True,
                "content": (
                    "- `mount_landing`：阶段主表，包含 `stage / unlock_level / activate_cond / advance_cost` 等。\n"
                    "- `mount_attr`：按阶段表达属性增量。\n"
                    "- `mount_cultivation_quant`：记录本阶消耗、累计消耗、阶段性价比。"
                ),
            },
            {
                "module_key": "structure_rules",
                "title": "结构规则",
                "required": True,
                "enabled": True,
                "content": (
                    "- 阶段数与行轴优先读取 `system_level_caps.mount`，未配置才回退 `max_level`。\n"
                    "- 每一阶至少要能解释：开放条件、消耗、属性提升、玩家感知变化。\n"
                    "- `advance_cost` 不应只有一个资源字段；如项目有多资源约束，应显式拆列。\n"
                    "- `activate_cond` 推荐可读化，区分首开激活与后续进阶。"
                ),
            },
            {
                "module_key": "growth_curve",
                "title": "成长曲线建议",
                "required": True,
                "enabled": True,
                "content": (
                    "坐骑成长适合“前期明显、中期拉开、高期非线性抬升”的阶段曲线：\n"
                    "- 前几阶给玩家明显反馈；\n"
                    "- 中期靠资源和开放等级控制速度；\n"
                    "- 高期非线性抬升，但仍应受常量控制，避免硬编码固定台阶。"
                ),
            },
            {
                "module_key": "acceptance",
                "title": "验收关注点",
                "required": True,
                "enabled": True,
                "content": (
                    "- 阶段数和开放等级来源正确；\n"
                    "- 每阶属性收益与资源成本成体系；\n"
                    "- 不存在只有“金币+战力”两列的空心表；\n"
                    "- 高阶曲线可解释，不依赖魔法数。"
                ),
            },
            {
                "module_key": "extensions",
                "title": "可选扩展模块",
                "required": False,
                "enabled": False,
                "content": "可扩展天赋树、外观皮肤、羁绊、出战技能、坐骑装备、升阶保底等模块。",
            },
        ],
    },
    {
        "template_key": "wing_landing",
        "slug": "wing-landing",
        "title": "翅膀制作说明",
        "step_id": "gameplay_table.wing",
        "summary": "定义翅膀阶段、羽毛消耗、属性成长与表现反馈，适合做为中后期外显型成长系统。",
        "description": "翅膀系统和坐骑类似，但更偏向外显与身份感，属性增量和资源消耗应体现美术反馈与战力反馈同步推进。",
        "default_exposed": True,
        "modules": [
            {
                "module_key": "outputs",
                "title": "建议产出表",
                "required": True,
                "enabled": True,
                "content": (
                    "- `wing_landing`：阶段定义表，如 `stage / feather_cost / unlock_level / attr_pool`。\n"
                    "- `wing_attr`：按阶段展开属性增量。\n"
                    "- `wing_cultivation_quant`：养成量化表，可记录本阶投入与累计投入。"
                ),
            },
            {
                "module_key": "rules",
                "title": "核心规则",
                "required": True,
                "enabled": True,
                "content": (
                    "- 阶段数优先读取 `system_level_caps.wing`；\n"
                    "- `feather_cost` 之外如还有金币/材料，请显式拆列；\n"
                    "- `attr_pool` 要说明翅膀偏向哪类属性，不建议与坐骑完全同质。"
                ),
            },
            {
                "module_key": "growth",
                "title": "成长节奏",
                "required": True,
                "enabled": True,
                "content": (
                    "翅膀更适合做“节点式解锁”而非过细长表：\n"
                    "- 重要阶段应有明显外显变化；\n"
                    "- 属性增量应配合阶段节点拉开；\n"
                    "- 资源消耗应兼顾收藏驱动和战力驱动。"
                ),
            },
            {
                "module_key": "acceptance",
                "title": "验收关注点",
                "required": True,
                "enabled": True,
                "content": (
                    "- 节点感明确；\n"
                    "- 成本、属性、外显三者一致；\n"
                    "- 与坐骑系统有区隔，而不是换壳复制。"
                ),
            },
            {
                "module_key": "extensions",
                "title": "可选扩展模块",
                "required": False,
                "enabled": False,
                "content": "可扩展染色、翅膀套装、翅膀技能、翅膀羁绊、外观收集加成等内容。",
            },
        ],
    },
    {
        "template_key": "fashion_landing",
        "slug": "fashion-landing",
        "title": "时装制作说明",
        "step_id": "gameplay_table.fashion",
        "summary": "定义时装套装、品质、属性加成与外观/战斗双定位，避免把时装系统做成纯数值壳或纯描述壳。",
        "description": "适用于时装、皮肤、套装外观类系统，重点是区分纯外观、外观带属性、套装激活等不同设计路线。",
        "default_exposed": True,
        "modules": [
            {
                "module_key": "positioning",
                "title": "系统定位",
                "required": True,
                "enabled": True,
                "content": (
                    "时装系统先回答“它主要卖什么”：是纯外观、轻战力、还是套装收集？\n"
                    "定位不同，表结构应完全不同，不能只有一个 `attr_bonus` 草草带过。"
                ),
            },
            {
                "module_key": "outputs",
                "title": "建议产出表",
                "required": True,
                "enabled": True,
                "content": (
                    "- `fashion_landing`：主表，至少包含 `suite / quality / attr_bonus / aesthetic_vs_combat`。\n"
                    "- 如存在套装激活，建议增加套装条件或套装效果拆分列。\n"
                    "- 如存在养成过程，可再加 `fashion_cultivation_quant`。"
                ),
            },
            {
                "module_key": "rules",
                "title": "规则建议",
                "required": True,
                "enabled": True,
                "content": (
                    "- 纯外观时装允许 0 战斗属性，但要在字段中明确定位。\n"
                    "- 战斗型时装必须给出具体属性方向，不要泛写“战力提升”。\n"
                    "- 套装时装建议体现件数门槛、激活效果、是否可替换。"
                ),
            },
            {
                "module_key": "acceptance",
                "title": "验收关注点",
                "required": True,
                "enabled": True,
                "content": (
                    "- 能区分外观型与战斗型；\n"
                    "- 套装结构可读；\n"
                    "- 字段足以支持展示、收集、激活、属性加成。"
                ),
            },
            {
                "module_key": "extensions",
                "title": "可选扩展模块",
                "required": False,
                "enabled": False,
                "content": "可扩展染色、收集册、羁绊、主题季活动关联、时装分解返还等内容。",
            },
        ],
    },
    {
        "template_key": "dungeon_landing",
        "slug": "dungeon-landing",
        "title": "副本制作说明",
        "step_id": "gameplay_table.dungeon",
        "summary": "定义副本开放、门票/体力消耗、每日次数、奖励价值与长期累计效率，强调玩法门槛与资源循环。",
        "description": "适用于主线、副本、挑战关卡等内容，落地时要体现开放节奏、投入限制和奖励效率，而不是只列出掉落表。",
        "default_exposed": True,
        "modules": [
            {
                "module_key": "goal",
                "title": "设计目标",
                "required": True,
                "enabled": True,
                "content": (
                    "副本系统要解释：何时开放、玩家每天能打多少、每次消耗什么、打完获得什么、长期投入值不值。"
                ),
            },
            {
                "module_key": "outputs",
                "title": "建议产出表",
                "required": True,
                "enabled": True,
                "content": (
                    "- `dungeon_landing`：至少包含 `dungeon_id / open_level / ticket_cost / daily_max_count / reward_drop / cumulative_ticket / value_per_ticket`。\n"
                    "- 如副本种类多，可拆主表与奖励表，但主表必须保留开放门槛和效率指标。"
                ),
            },
            {
                "module_key": "core_rules",
                "title": "核心规则",
                "required": True,
                "enabled": True,
                "content": (
                    "- `dungeon_id` 可以按阶段或区间生成，但分段阈值必须来自常量，不要直接硬编码。\n"
                    "- `daily_max_count`、`ticket_cost`、`reward_drop` 应共同决定玩家日循环，而不是各自孤立。\n"
                    "- `cumulative_ticket` 与 `value_per_ticket` 能帮助评估长期效率，应明确保留。"
                ),
            },
            {
                "module_key": "reward_logic",
                "title": "奖励效率逻辑",
                "required": True,
                "enabled": True,
                "content": (
                    "副本奖励不应只看单次掉落，还要看累计投入后的单位价值：\n"
                    "- 低门槛副本更偏日常稳定产出；\n"
                    "- 高门槛副本可以更稀缺，但要解释开放时机与资源消耗；\n"
                    "- `value_per_ticket` 不宜无限上升，否则会破坏副本分层。"
                ),
            },
            {
                "module_key": "acceptance",
                "title": "验收关注点",
                "required": True,
                "enabled": True,
                "content": (
                    "- 开放等级来源正确；\n"
                    "- 次数、门票、奖励三者可解释；\n"
                    "- 存在长期效率指标；\n"
                    "- 分段阈值和门槛常量化。"
                ),
            },
            {
                "module_key": "extensions",
                "title": "可选扩展模块",
                "required": False,
                "enabled": False,
                "content": "可扩展星级评价、扫荡规则、首通奖励、限时副本、掉落池明细表等模块。",
            },
        ],
    },
    {
        "template_key": "skill_landing",
        "slug": "skill-landing",
        "title": "技能制作说明",
        "step_id": "gameplay_table.skill",
        "summary": "定义技能主表与技能养成量化表，体现技能类型、解锁、冷却、伤害倍率、资源消耗和成长路线。",
        "description": "适用于主动/被动/终极技等技能系统，重点在于让技能强度、资源消耗、冷却与成长形成统一设计语言。",
        "default_exposed": True,
        "modules": [
            {
                "module_key": "positioning",
                "title": "系统定位",
                "required": True,
                "enabled": True,
                "content": (
                    "技能不是单纯一列 `dmg_ratio`。要先区分主动、被动、终结技、辅助技等类型，再定义各自的解锁、消耗和成长逻辑。"
                ),
            },
            {
                "module_key": "outputs",
                "title": "建议产出表",
                "required": True,
                "enabled": True,
                "content": (
                    "- `skill_landing`：至少包含 `skill_id / type / unlock_level / cooldown / dmg_ratio / resource_cost`。\n"
                    "- `skill_cultivation_quant`：记录技能升级带来的单级提升、累计消耗、效率拐点。"
                ),
            },
            {
                "module_key": "core_rules",
                "title": "核心规则",
                "required": True,
                "enabled": True,
                "content": (
                    "- `type` 先明确技能职责，再决定字段差异；\n"
                    "- `cooldown`、`resource_cost`、`dmg_ratio` 必须成套出现，才能解释技能强度；\n"
                    "- 伤害倍率存小数并用百分比格式展示；\n"
                    "- 若项目有能量/怒气/MP 等多类消耗，应显式区分。"
                ),
            },
            {
                "module_key": "growth_logic",
                "title": "成长逻辑",
                "required": True,
                "enabled": True,
                "content": (
                    "技能成长建议体现：\n"
                    "- 前期升级带来显著感知；\n"
                    "- 中后期通过资源消耗抬高决策成本；\n"
                    "- 高强度技能不应同时拥有过低冷却和过低资源消耗。"
                ),
            },
            {
                "module_key": "acceptance",
                "title": "验收关注点",
                "required": True,
                "enabled": True,
                "content": (
                    "- 技能类型清晰；\n"
                    "- 解锁、冷却、倍率、消耗之间有对应关系；\n"
                    "- 升级收益与累计消耗可解释；\n"
                    "- 没有把技能设计压缩成单一数值表。"
                ),
            },
            {
                "module_key": "extensions",
                "title": "可选扩展模块",
                "required": False,
                "enabled": False,
                "content": "可扩展被动触发条件、BUFF/DEBUFF 参数、技能连携、符文/奥义分支、技能树门槛等模块。",
            },
        ],
    },
]


def ensure_default_skills(conn: sqlite3.Connection) -> None:
    now = _now()
    for order, skill in enumerate(DEFAULT_SKILLS, start=1):
        cur = conn.execute(
            "SELECT id FROM _skills WHERE template_key = ?",
            (skill["template_key"],),
        )
        row = cur.fetchone()
        if row:
            continue
        cur = conn.execute(
            """
            INSERT INTO _skills (
                slug, title, step_id, summary, description, source, template_key,
                default_exposed, enabled, display_order, usage_count,
                generated_file_path, generated_content, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                skill["slug"],
                skill["title"],
                skill["step_id"],
                skill["summary"],
                skill["description"],
                "system",
                skill["template_key"],
                1 if skill.get("default_exposed") else 0,
                1,
                order,
                0,
                "",
                "",
                now,
                now,
            ),
        )
        skill_id = int(cur.lastrowid)
        for idx, module in enumerate(skill.get("modules") or [], start=1):
            conn.execute(
                """
                INSERT INTO _skill_modules (
                    skill_id, module_key, title, content, required, enabled, sort_order, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    skill_id,
                    module["module_key"],
                    module["title"],
                    module["content"],
                    1 if module.get("required") else 0,
                    1 if module.get("enabled", module.get("required", False)) else 0,
                    idx,
                    now,
                    now,
                ),
            )
    conn.commit()


def _dict_from_row(row: sqlite3.Row) -> Dict[str, Any]:
    return {k: row[k] for k in row.keys()}


def _load_modules(conn: sqlite3.Connection, skill_id: int) -> List[Dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT id, module_key, title, content, required, enabled, sort_order
        FROM _skill_modules
        WHERE skill_id = ?
        ORDER BY sort_order ASC, id ASC
        """,
        (skill_id,),
    )
    items: List[Dict[str, Any]] = []
    for row in cur.fetchall():
        items.append(
            {
                "id": row["id"],
                "module_key": row["module_key"],
                "title": row["title"],
                "content": row["content"],
                "required": bool(row["required"]),
                "enabled": bool(row["enabled"]) or bool(row["required"]),
                "sort_order": int(row["sort_order"]),
            }
        )
    return items


def _selected_modules(modules: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [m for m in modules if m.get("required") or m.get("enabled")]


def _yaml_lines(meta: Dict[str, Any]) -> List[str]:
    lines = ["---"]
    for key, value in meta.items():
        if isinstance(value, bool):
            lines.append(f"{key}: {'true' if value else 'false'}")
        elif isinstance(value, list):
            lines.append(f"{key}:")
            for item in value:
                lines.append(f"  - {json.dumps(item, ensure_ascii=False)}")
        else:
            lines.append(f"{key}: {json.dumps(value, ensure_ascii=False)}")
    lines.append("---")
    return lines


def render_skill_markdown(skill: Dict[str, Any], modules: Sequence[Dict[str, Any]]) -> str:
    chosen = _selected_modules(modules)
    meta = {
        "skill_slug": skill["slug"],
        "title": skill["title"],
        "step_id": skill.get("step_id") or "",
        "source": skill.get("source") or "user",
        "default_exposed": bool(skill.get("default_exposed")),
        "enabled_module_keys": [m["module_key"] for m in chosen],
    }
    parts = _yaml_lines(meta)
    parts.append(f"# {skill['title']}")
    if skill.get("summary"):
        parts.append("")
        parts.append(f"> {skill['summary']}")
    if skill.get("description"):
        parts.append("")
        parts.append(skill["description"])
    for module in chosen:
        parts.append("")
        parts.append(f"## {module['title']}")
        parts.append(module["content"].strip())
    return "\n".join(parts).strip() + "\n"


def render_skill_prompt_bundle(skills: Sequence[Dict[str, Any]]) -> str:
    if not skills:
        return ""
    parts = [
        "【SKILL 默认暴露】以下为当前步骤自动暴露的 SKILL 内容。",
        "如需进一步检索或核对，可继续调用 list_skills / get_skill_detail / render_skill_file。",
    ]
    for skill in skills:
        parts.append("")
        parts.append(f"### SKILL：{skill['title']} ({skill['slug']})")
        parts.append(skill["rendered_markdown"].strip())
    return "\n".join(parts).strip()


def _resolve_step_candidates(step_id: str) -> List[str]:
    if not step_id:
        return []
    normalized = step_id.strip()
    alias_map = []
    if normalized == "gameplay_landing_tables":
        alias_map.append("gameplay_table")
    elif normalized.startswith("gameplay_landing_tables."):
        alias_map.append(normalized.replace("gameplay_landing_tables", "gameplay_table", 1))
    elif normalized == "gameplay_table":
        alias_map.append("gameplay_landing_tables")
    elif normalized.startswith("gameplay_table."):
        alias_map.append(normalized.replace("gameplay_table", "gameplay_landing_tables", 1))

    out: List[str] = []
    seen: set[str] = set()

    def _append_parts(candidate: str) -> None:
        parts = candidate.split(".")
        for idx in range(1, len(parts) + 1):
            part = ".".join(parts[:idx])
            if part in seen:
                continue
            seen.add(part)
            out.append(part)

    _append_parts(normalized)
    for alias in alias_map:
        _append_parts(alias)
    # parent/common should appear before exact step
    return out


def record_skill_usage(
    conn: sqlite3.Connection,
    *,
    skill_id: int,
    event_type: str,
    step_id: str = "",
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    now = _now()
    conn.execute(
        """
        INSERT INTO _skill_usage_log (skill_id, event_type, step_id, meta_json, created_at)
        VALUES (?,?,?,?,?)
        """,
        (skill_id, event_type, step_id, json.dumps(meta or {}, ensure_ascii=False), now),
    )
    conn.execute(
        "UPDATE _skills SET usage_count = COALESCE(usage_count, 0) + 1, updated_at = ? WHERE id = ?",
        (now, skill_id),
    )
    conn.commit()


def _fetch_skill_rows(
    conn: sqlite3.Connection,
    *,
    include_disabled: bool = True,
) -> List[sqlite3.Row]:
    where = "" if include_disabled else "WHERE enabled = 1"
    cur = conn.execute(
        f"""
        SELECT id, slug, title, step_id, summary, description, source, template_key,
               default_exposed, enabled, display_order, usage_count,
               generated_file_path, generated_content, created_at, updated_at
        FROM _skills
        {where}
        ORDER BY display_order ASC, id ASC
        """
    )
    return list(cur.fetchall())


def list_skills(
    conn: sqlite3.Connection,
    *,
    include_disabled: bool = True,
    include_modules: bool = True,
    project_slug: Optional[str] = None,
) -> List[Dict[str, Any]]:
    ensure_default_skills(conn)
    rows = _fetch_skill_rows(conn, include_disabled=include_disabled)
    items: List[Dict[str, Any]] = []
    for row in rows:
        skill = _dict_from_row(row)
        modules = _load_modules(conn, int(row["id"])) if include_modules else []
        rendered = render_skill_markdown(skill, modules)
        if project_slug:
            _persist_generated_skill(conn, project_slug, skill, modules, rendered)
            skill["generated_file_path"] = f"skills/{skill['slug']}.md"
            skill["generated_content"] = rendered
        skill["id"] = int(skill["id"])
        skill["default_exposed"] = bool(skill["default_exposed"])
        skill["enabled"] = bool(skill["enabled"])
        skill["usage_count"] = int(skill["usage_count"] or 0)
        skill["modules"] = modules
        skill["enabled_module_count"] = len(_selected_modules(modules))
        skill["rendered_markdown"] = rendered
        items.append(skill)
    return items


def _get_skill_row_by_identifier(conn: sqlite3.Connection, identifier: Union[str, int]) -> Optional[sqlite3.Row]:
    if isinstance(identifier, int) or str(identifier).isdigit():
        cur = conn.execute(
            """
            SELECT id, slug, title, step_id, summary, description, source, template_key,
                   default_exposed, enabled, display_order, usage_count,
                   generated_file_path, generated_content, created_at, updated_at
            FROM _skills WHERE id = ?
            """,
            (int(identifier),),
        )
    else:
        cur = conn.execute(
            """
            SELECT id, slug, title, step_id, summary, description, source, template_key,
                   default_exposed, enabled, display_order, usage_count,
                   generated_file_path, generated_content, created_at, updated_at
            FROM _skills WHERE slug = ?
            """,
            (str(identifier),),
        )
    return cur.fetchone()


def get_skill_detail(
    conn: sqlite3.Connection,
    identifier: Union[str, int],
    *,
    project_slug: Optional[str] = None,
    record_usage_event: Optional[str] = None,
    step_id: str = "",
) -> Optional[Dict[str, Any]]:
    ensure_default_skills(conn)
    row = _get_skill_row_by_identifier(conn, identifier)
    if not row:
        return None
    skill = _dict_from_row(row)
    modules = _load_modules(conn, int(skill["id"]))
    rendered = render_skill_markdown(skill, modules)
    if project_slug:
        _persist_generated_skill(conn, project_slug, skill, modules, rendered)
        skill["generated_file_path"] = f"skills/{skill['slug']}.md"
        skill["generated_content"] = rendered
    if record_usage_event:
        record_skill_usage(
            conn,
            skill_id=int(skill["id"]),
            event_type=record_usage_event,
            step_id=step_id,
            meta={"skill_slug": skill["slug"]},
        )
        skill["usage_count"] = int(skill["usage_count"] or 0) + 1
    skill["default_exposed"] = bool(skill["default_exposed"])
    skill["enabled"] = bool(skill["enabled"])
    skill["usage_count"] = int(skill["usage_count"] or 0)
    skill["modules"] = modules
    skill["enabled_module_count"] = len(_selected_modules(modules))
    skill["rendered_markdown"] = rendered
    return skill


def _persist_generated_skill(
    conn: sqlite3.Connection,
    project_slug: str,
    skill: Dict[str, Any],
    modules: Sequence[Dict[str, Any]],
    rendered: Optional[str] = None,
) -> Dict[str, Any]:
    body = rendered or render_skill_markdown(skill, modules)
    rel_path = f"skills/{skill['slug']}.md"
    out_path: Path = get_project_dir(project_slug) / rel_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(body, encoding="utf-8")
    conn.execute(
        """
        UPDATE _skills
        SET generated_file_path = ?, generated_content = ?, updated_at = ?
        WHERE id = ?
        """,
        (rel_path, body, _now(), int(skill["id"])),
    )
    conn.commit()
    return {"path": rel_path, "content": body}


def render_skill_file(
    conn: sqlite3.Connection,
    identifier: Union[str, int],
    *,
    project_slug: Optional[str] = None,
    record_usage_event: Optional[str] = None,
    step_id: str = "",
) -> Optional[Dict[str, Any]]:
    detail = get_skill_detail(
        conn,
        identifier,
        project_slug=None,
        record_usage_event=record_usage_event,
        step_id=step_id,
    )
    if not detail:
        return None
    modules = detail.get("modules") or []
    rendered = detail["rendered_markdown"]
    if project_slug:
        persisted = _persist_generated_skill(conn, project_slug, detail, modules, rendered)
        detail["generated_file_path"] = persisted["path"]
        detail["generated_content"] = persisted["content"]
    return {
        "id": detail["id"],
        "slug": detail["slug"],
        "title": detail["title"],
        "generated_file_path": detail.get("generated_file_path") or "",
        "generated_content": detail.get("generated_content") or rendered,
    }


def get_default_exposed_skills_for_step(
    conn: sqlite3.Connection,
    step_id: str,
    *,
    record_usage_events: bool = False,
) -> List[Dict[str, Any]]:
    ensure_default_skills(conn)
    candidates = _resolve_step_candidates(step_id)
    if not candidates:
        return []
    rows: List[sqlite3.Row] = []
    for idx, candidate in enumerate(candidates):
        cur = conn.execute(
            """
            SELECT id, slug, title, step_id, summary, description, source, template_key,
                   default_exposed, enabled, display_order, usage_count,
                   generated_file_path, generated_content, created_at, updated_at
            FROM _skills
            WHERE enabled = 1 AND default_exposed = 1 AND step_id = ?
            ORDER BY display_order ASC, id ASC
            """,
            (candidate,),
        )
        rows.extend(cur.fetchall())
    items: List[Dict[str, Any]] = []
    seen: set[int] = set()
    for row in rows:
        sid = int(row["id"])
        if sid in seen:
            continue
        seen.add(sid)
        skill = _dict_from_row(row)
        modules = _load_modules(conn, sid)
        skill["modules"] = modules
        skill["rendered_markdown"] = render_skill_markdown(skill, modules)
        skill["usage_count"] = int(skill["usage_count"] or 0)
        skill["default_exposed"] = bool(skill["default_exposed"])
        skill["enabled"] = bool(skill["enabled"])
        items.append(skill)
        if record_usage_events:
            record_skill_usage(
                conn,
                skill_id=sid,
                event_type="auto_exposed",
                step_id=step_id,
                meta={"step_id": step_id, "skill_slug": skill["slug"]},
            )
            skill["usage_count"] += 1
    return items


def build_default_skill_prompt(
    conn: sqlite3.Connection,
    step_id: str,
    *,
    record_usage_events: bool = False,
) -> Dict[str, Any]:
    """返回所有 default_exposed=1 的 skill（不按 step_id 过滤）。"""
    ensure_default_skills(conn)
    cur = conn.execute(
        """
        SELECT id, slug, title, step_id, summary, description, source, template_key,
               default_exposed, enabled, display_order, usage_count,
               generated_file_path, generated_content, created_at, updated_at
        FROM _skills
        WHERE enabled = 1 AND default_exposed = 1
        ORDER BY display_order ASC, id ASC
        """,
    )
    rows = cur.fetchall()
    skills: List[Dict[str, Any]] = []
    for row in rows:
        skill = _dict_from_row(row)
        modules = _load_modules(conn, int(row["id"]))
        skill["modules"] = modules
        skill["rendered_markdown"] = render_skill_markdown(skill, modules)
        skill["usage_count"] = int(skill["usage_count"] or 0)
        skill["default_exposed"] = bool(skill["default_exposed"])
        skill["enabled"] = bool(skill["enabled"])
        skills.append(skill)
        if record_usage_events:
            record_skill_usage(
                conn,
                skill_id=int(row["id"]),
                event_type="auto_exposed",
                step_id=step_id,
                meta={"step_id": step_id, "skill_slug": skill["slug"]},
            )
    return {
        "skills": [{"id": s["id"], "slug": s["slug"], "title": s["title"]} for s in skills],
        "prompt": render_skill_prompt_bundle(skills),
    }


def _existing_slugs(conn: sqlite3.Connection) -> Iterable[str]:
    cur = conn.execute("SELECT slug FROM _skills")
    return [str(row[0]) for row in cur.fetchall()]


def upsert_skill(
    conn: sqlite3.Connection,
    *,
    project_slug: str,
    skill_id: Optional[int],
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    ensure_default_skills(conn)
    now = _now()
    title = str(payload.get("title") or "").strip()
    if not title:
        raise ValueError("title 必填")
    step_id = str(payload.get("step_id") or "").strip()
    summary = str(payload.get("summary") or "").strip()
    description = str(payload.get("description") or "").strip()
    source = str(payload.get("source") or "user").strip() or "user"
    default_exposed = bool(payload.get("default_exposed"))
    enabled = bool(payload.get("enabled", True))
    raw_slug = str(payload.get("slug") or "").strip()
    base_slug = slugify(raw_slug or title or "skill")
    if not raw_slug and base_slug == "project":
        base_slug = "skill"

    if skill_id is None:
        existing = set(_existing_slugs(conn))

        def taken(val: str) -> bool:
            return val in existing

        slug = unique_slug(base_slug, taken)
        cur = conn.execute(
            """
            INSERT INTO _skills (
                slug, title, step_id, summary, description, source, template_key,
                default_exposed, enabled, display_order, usage_count,
                generated_file_path, generated_content, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                slug,
                title,
                step_id,
                summary,
                description,
                source,
                None,
                1 if default_exposed else 0,
                1 if enabled else 0,
                9999,
                0,
                "",
                "",
                now,
                now,
            ),
        )
        skill_id = int(cur.lastrowid)
    else:
        row = _get_skill_row_by_identifier(conn, skill_id)
        if not row:
            raise ValueError("skill 不存在")
        existing = set(_existing_slugs(conn)) - {str(row["slug"])}

        def taken(val: str) -> bool:
            return val in existing

        slug = unique_slug(base_slug or str(row["slug"]), taken)
        conn.execute(
            """
            UPDATE _skills
            SET slug = ?, title = ?, step_id = ?, summary = ?, description = ?,
                default_exposed = ?, enabled = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                slug,
                title,
                step_id,
                summary,
                description,
                1 if default_exposed else 0,
                1 if enabled else 0,
                now,
                skill_id,
            ),
        )

    modules = payload.get("modules")
    if not isinstance(modules, list):
        modules = []

    keep_ids: List[int] = []
    seen_module_keys: set[str] = set()
    for idx, raw in enumerate(modules, start=1):
        if not isinstance(raw, dict):
            continue
        module_title = str(raw.get("title") or "").strip()
        if not module_title:
            continue
        raw_module_key = str(raw.get("module_key") or "").strip()
        module_key = slugify(raw_module_key or module_title or f"module-{idx}")
        if not raw_module_key and module_key == "project":
            module_key = f"module-{idx}"
        if module_key in seen_module_keys:
            module_key = f"{module_key}-{idx}"
        seen_module_keys.add(module_key)
        required = bool(raw.get("required"))
        enabled_module = bool(raw.get("enabled", required)) or required
        content = str(raw.get("content") or "").strip()
        sort_order = int(raw.get("sort_order") or idx)
        module_id = raw.get("id")
        if module_id:
            conn.execute(
                """
                UPDATE _skill_modules
                SET module_key = ?, title = ?, content = ?, required = ?, enabled = ?, sort_order = ?, updated_at = ?
                WHERE id = ? AND skill_id = ?
                """,
                (
                    module_key,
                    module_title,
                    content,
                    1 if required else 0,
                    1 if enabled_module else 0,
                    sort_order,
                    now,
                    int(module_id),
                    skill_id,
                ),
            )
            keep_ids.append(int(module_id))
        else:
            cur = conn.execute(
                """
                INSERT INTO _skill_modules (
                    skill_id, module_key, title, content, required, enabled, sort_order, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    skill_id,
                    module_key,
                    module_title,
                    content,
                    1 if required else 0,
                    1 if enabled_module else 0,
                    sort_order,
                    now,
                    now,
                ),
            )
            keep_ids.append(int(cur.lastrowid))

    if keep_ids:
        placeholders = ",".join("?" for _ in keep_ids)
        conn.execute(
            f"DELETE FROM _skill_modules WHERE skill_id = ? AND id NOT IN ({placeholders})",
            (skill_id, *keep_ids),
        )
    else:
        conn.execute("DELETE FROM _skill_modules WHERE skill_id = ?", (skill_id,))
    conn.commit()
    detail = get_skill_detail(conn, skill_id, project_slug=project_slug)
    assert detail is not None
    return detail
