"""流水线 11 步的规格说明（StepSpec）资产。

设计目的：
- 让每个 `PIPELINE_STEPS` 步骤拥有可机读、可渲染为 Markdown 的设计规格；
- 为「步骤级 README」提供初始模板，并给 Agent 提供 design→review→execute
  三阶段的强约束；
- 表/列名以 docs/真实Agent与全流水线测试-2026-04-24.md 的表级映射为准，
  设计要点遵循 docs/03、默认规则遵循 docs/02。

字段释义见 docs/游戏数值系统AI化自动开发-07-步骤README规范.md。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional


@dataclass
class StepSpec:
    step_id: str
    title_zh: str
    goal: str
    inputs: List[str]
    outputs: List[str]
    required_tables: List[str]
    required_columns: Dict[str, List[str]]
    acceptance: List[str]
    agent_hint: str
    common_pitfalls: List[str]
    upstream_steps: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)

    def render_markdown(self) -> str:
        return render_spec_markdown(self)


_AGENT_THREE_PHASE_HINT = (
    "必须按 design→review→execute 三阶段输出："
    "①design：列出本步将要新建/修改的表、列、READMЕ 段落与关键假设；"
    "②review：对照本 spec 的 acceptance 与 common_pitfalls 自检，"
    "并显式调用 get_table_readme / read_table 验证已有上游产物；"
    "③execute：仅在用户确认或 maintain 模式带写权限时调用写工具，"
    "每次写入必须带合法 source_tag，并在结束时 update_table_readme 回填。"
    "READMЕ 必须包含字段：设计目标、关键决策、列含义、与上游表关系、"
    "未决问题/TODO、本次 acceptance 勾选结果。"
)


PIPELINE_STEP_SPECS: List[StepSpec] = [
    StepSpec(
        step_id="environment_global_readme",
        title_zh="整体环境确认与全局 README",
        goal=(
            "把项目核心定义（最大等级、生命周期、玩法系统、属性勾选）和 02 默认细则"
            "对齐成一份可被后续所有步骤引用的全局 README；明确哪些细则被忽略、需要"
            "用户补充什么。"
        ),
        inputs=[
            "项目配置 get_project_config（核心定义、玩法系统树、属性勾选）",
            "docs/02 默认细则的机读版本 get_default_system_rules",
            "用户显式给出的设计要求（若有）",
        ],
        outputs=[
            "project_settings.global_readme（覆盖核心规则、玩法系统、属性体系、被忽略项）",
            "_table_registry 索引行：e2e_pipeline_index 或同等索引说明",
        ],
        required_tables=[],
        required_columns={},
        acceptance=[
            "全局 README 中显式列出所有被勾选的玩法系统与属性",
            "对每条 02 默认细则，标明「采用 / 用户覆盖 / 忽略+原因」",
            "明确写出最大标准等级与游戏生命周期（默认 200 级 / 60 天，除非用户改）",
            "README 末尾给出本步的 acceptance 勾选与未决问题列表",
        ],
        agent_hint=_AGENT_THREE_PHASE_HINT + " 本步不得创建任何业务数据表。",
        common_pitfalls=[
            "把 02 默认值当成不可变事实写入，忽略了用户已在 01 配置阶段做出的覆盖",
            "README 只罗列玩法名却没说明每个玩法的开放等级与默认细则归属",
            "忽略「无法贴合细则」的项目却没在 README 标注，导致后续步骤无依据",
        ],
        upstream_steps=[],
    ),
    StepSpec(
        step_id="base_attribute_framework",
        title_zh="基本属性基础框架表",
        goal=(
            "以全等级（默认 60/200 行）拉出基础一阶属性曲线：攻防、暴击/对抗、双技能"
            "系数、生命与等级时间/经验；为后续分配与二阶提供原始基线。"
        ),
        inputs=[
            "全局 README 中的属性勾选与公式选择（减法/乘法）",
            "默认常数与曲线策略（02 + 用户覆盖）",
            "标准等级数 settings.core.level_max",
        ],
        outputs=[
            "num_base_framework：全等级一阶属性表",
            "num_level_pacing：等级停留时间与升级经验表",
            "对应表的 README（设计动机、曲线分段、对抗投放策略、自击杀校验结果）",
        ],
        required_tables=["num_base_framework", "num_level_pacing"],
        required_columns={
            "num_base_framework": [
                "level", "atk", "def", "hp",
                "crit_rate", "crit_resist",
                "skill_dmg_coef", "skill_heal_coef",
            ],
            "num_level_pacing": [
                "level", "stay_minutes", "exp_per_minute", "exp_to_next",
            ],
        },
        acceptance=[
            "num_base_framework 行数 == level_max（默认 60，不允许只填 2 行示例）",
            "hp 列在等级轴上单调递增，不出现凹点",
            "对抗属性满足攻方 > 守方（例如暴击率曲线 > 抗暴击率曲线）",
            "技能系数 ≥ 120% 起步并按设计膨胀",
            "num_level_pacing 中前期快、平台期慢的节奏在 README 中有曲线分段说明",
            "若 hp/atk 比值 <1 或 >1000，README 必须记录二次修正过程",
        ],
        agent_hint=_AGENT_THREE_PHASE_HINT + (
            " design 阶段须明确：减法/乘法公式选择、防御常数、各属性曲线分段；"
            " execute 阶段对每个 cell 写入用 source_tag=base_framework_v1 之类可追溯标签。"
        ),
        common_pitfalls=[
            "只写 L1/L60 两行示例，被自动化判负",
            "防御与攻击数值关系失衡却不重新设计常数",
            "把暴击率/抗暴击率写成同曲线，丧失对抗博弈",
            "经验表只按线性膨胀，忽略停留时间分段",
        ],
        upstream_steps=["environment_global_readme"],
    ),
    StepSpec(
        step_id="gameplay_attribute_scheme",
        title_zh="玩法系统属性方案",
        goal=(
            "为每个被启用的玩法系统（装备、神器、阵法、坐骑等）确定要投放哪些属性，"
            "确保所有属性至少有 1 个落脚系统、绝大多数属性有 ≥2 个落脚系统。"
        ),
        inputs=[
            "全局 README 中的玩法清单与属性清单",
            "num_base_framework 已存在的属性列",
            "02 默认细则中每个系统的默认主属性",
        ],
        outputs=[
            "num_gameplay_attr_scheme：每玩法 1 行的方案说明",
            "对应 README：每个玩法为什么投这些属性、对哪些属性形成主/副落脚",
        ],
        required_tables=["num_gameplay_attr_scheme"],
        required_columns={
            "num_gameplay_attr_scheme": [
                "system_id", "system_name",
                "primary_attrs", "secondary_attrs",
                "open_level", "design_note",
            ],
        },
        acceptance=[
            "每个被启用的玩法系统都在表中有 1 行",
            "全部属性都至少出现在一个 system 的 primary 或 secondary 集合中",
            "≥80% 的属性出现在 ≥2 个系统中（README 中应显式核对）",
            "open_level 与 02 默认细则一致或在 README 标注覆盖原因",
        ],
        agent_hint=_AGENT_THREE_PHASE_HINT + (
            " review 阶段必须做「属性 × 系统」覆盖矩阵自检，输出未覆盖属性清单。"
        ),
        common_pitfalls=[
            "套用同一份默认主属性到所有系统，不考虑用户在 01 阶段的覆盖",
            "遗漏「副手 / 鞋子 / 饰品」等子部位的属性投放",
            "把宝石、神器等子玩法当成等级表的等级 1..N 来设计（应按品阶/等阶轴）",
        ],
        upstream_steps=["environment_global_readme", "base_attribute_framework"],
    ),
    StepSpec(
        step_id="gameplay_allocation_tables",
        title_zh="玩法系统属性分配表",
        goal=(
            "对每个 (系统, 属性) 在每个等级写出百分比分配，允许行/列求和≠100%；"
            "支持「按系统看分配」和「按属性看分配」两种切片视图。"
        ),
        inputs=[
            "num_gameplay_attr_scheme：方案",
            "num_base_framework：等级轴",
            "全局 README 中的开放等级与启动节奏",
        ],
        outputs=[
            "num_alloc_const：(system × attr × level) 分配百分比",
            "README：分配策略、为什么允许求和≠100、各系统启动等级",
        ],
        required_tables=["num_alloc_const"],
        required_columns={
            "num_alloc_const": [
                "system_id", "attr", "level", "pct", "note",
            ],
        },
        acceptance=[
            "覆盖所有 (启用系统 × 该系统方案中的属性 × 全等级) 组合",
            "README 显式说明「不要求求和=100%」并给出本项目的总分配曲线说明",
            "对未开放等级，pct=0 或留空并在 README 注明",
            "至少抽样 1 个系统给出「按系统看」与「按属性看」两个切片示例",
        ],
        agent_hint=_AGENT_THREE_PHASE_HINT + (
            " execute 时分批 write：按 system_id 分批，避免单批超 1k 行；"
            " review 阶段对求和分布 (min/median/max) 在 README 记录。"
        ),
        common_pitfalls=[
            "强行把每行/每列拉成 100% 求和，违背 03 规则",
            "把所有玩法都从 L1 启动，忽略 02 中的开放等级",
            "对随等级膨胀的属性写成常数 pct，丧失后期分化",
        ],
        upstream_steps=["gameplay_attribute_scheme"],
    ),
    StepSpec(
        step_id="second_order_framework",
        title_zh="基本属性二阶框架表",
        goal=(
            "用 (一阶基础值 × Σ 各系统分配比) 合成实际投放值；验证战斗节奏（尤其是"
            "防御平衡），失衡则回到一阶或分配阶段迭代。"
        ),
        inputs=[
            "num_base_framework：一阶曲线",
            "num_alloc_const：分配百分比",
        ],
        outputs=[
            "num_second_order：全等级二阶属性表",
            "README：合成公式、平衡校验结果、是否触发回退迭代",
        ],
        required_tables=["num_second_order"],
        required_columns={
            "num_second_order": [
                "level", "atk", "def", "hp",
                "crit_rate", "crit_resist",
                "skill_dmg_coef", "skill_heal_coef",
            ],
        },
        acceptance=[
            "num_second_order 行数 == level_max",
            "对每列，二阶值 ≈ Σ(系统分配 pct) × 一阶值（公式在 _formula_registry 登记）",
            "防御/攻击比值在 README 标注的合理区间内（默认减法 0.3~0.7、乘法 0.8~1.2）",
            "若发生回退迭代，README 记录调整前后的关键参数",
        ],
        agent_hint=_AGENT_THREE_PHASE_HINT + (
            " 公式必须通过 update_formula 登记到 _formula_registry，"
            " 不允许只在 README 文字描述。"
        ),
        common_pitfalls=[
            "直接复制一阶表当二阶，漏算分配权重",
            "发现失衡后只在二阶硬调数值，不回到一阶/分配阶段",
            "公式仅写在 README，没有 _formula_registry 记录",
        ],
        upstream_steps=["base_attribute_framework", "gameplay_allocation_tables"],
    ),
    StepSpec(
        step_id="gameplay_attribute_tables",
        title_zh="玩法系统属性表",
        goal=(
            "为每个玩法系统在每个等级生成具体属性数值（一阶 × 该系统分配 pct），"
            "形成「(系统 × 等级) → 实际属性」可直接消费的表。"
        ),
        inputs=[
            "num_base_framework：一阶",
            "num_alloc_const：分配",
            "num_gameplay_attr_scheme：每系统的属性集合",
        ],
        outputs=[
            "num_gameplay_by_system：每系统每等级的具体属性值",
            "README：表的切片用法、与二阶表的对照说明",
        ],
        required_tables=["num_gameplay_by_system"],
        required_columns={
            "num_gameplay_by_system": [
                "system_id", "level",
                "atk", "def", "hp",
                "crit_rate", "crit_resist",
                "skill_dmg_coef", "skill_heal_coef",
            ],
        },
        acceptance=[
            "行数 == 启用系统数 × level_max（如 4×60=240）",
            "每个 (system, level) 单元有 source_tag 指向 num_alloc_const 与 num_base_framework",
            "README 给出抽样 1 个系统的完整曲线截图/示例行",
        ],
        agent_hint=_AGENT_THREE_PHASE_HINT + (
            " 写入时优先 batch_write_cells；列与 num_base_framework 同名以方便 @ 引用。"
        ),
        common_pitfalls=[
            "把所有系统压成一张「(标准等级 + 两列消耗)」的扁表（明确禁止）",
            "对宝石/神器这类按品阶轴的玩法用标准等级 1..N 直接拷贝",
            "未启用系统也生成行，造成数据冗余",
        ],
        upstream_steps=[
            "second_order_framework",
            "gameplay_allocation_tables",
            "gameplay_attribute_scheme",
        ],
    ),
    StepSpec(
        step_id="cultivation_resource_design",
        title_zh="养成资源设计",
        goal=(
            "枚举所有养成资源：≥2 种基础货币 + 各玩法专属道具；定义资源归属玩法，"
            "对多玩法复用资源给出权重分配草案。"
        ),
        inputs=[
            "num_gameplay_attr_scheme：玩法清单",
            "全局 README 中的养成偏好",
            "02 默认细则中的子系统（宝石、增幅等）规则",
        ],
        outputs=[
            "cult_res_catalog：资源目录",
            "README：资源命名、归属、复用权重、隔离原则",
        ],
        required_tables=["cult_res_catalog"],
        required_columns={
            "cult_res_catalog": [
                "res_id", "res_name", "is_currency",
                "scope_systems", "default_weight", "note",
            ],
        },
        acceptance=[
            "至少 2 种 is_currency=1 的基础货币",
            "每个被启用的玩法系统都至少绑定 1 种专属或共用资源",
            "对多玩法资源给出 default_weight（数值，非空）并在 README 解释",
            "README 记录「资源隔离 vs 复用」的本项目策略",
        ],
        agent_hint=_AGENT_THREE_PHASE_HINT + (
            " design 阶段须先列资源候选清单供用户/上游确认，再 execute 写表。"
        ),
        common_pitfalls=[
            "所有玩法都共用同一种货币，违背「专属隔离」默认原则",
            "把同一种资源既标专属又跨多玩法复用，权重缺失",
            "漏掉宝石合成等子玩法的专属道具",
        ],
        upstream_steps=["gameplay_attribute_scheme"],
    ),
    StepSpec(
        step_id="cultivation_resource_framework",
        title_zh="养成资源基础框架表",
        goal=(
            "为每个资源在每个等级定义单位时间产出与膨胀策略；结合 num_level_pacing 的"
            "停留时间得到每等级实际产量与累计总产量。"
        ),
        inputs=[
            "cult_res_catalog：资源目录",
            "num_level_pacing：等级停留时间",
        ],
        outputs=[
            "num_res_per_level：每资源每等级的产出与累计",
            "README：膨胀策略选择（线性/指数/分段）、与停留时间的对照",
        ],
        required_tables=["num_res_per_level"],
        required_columns={
            "num_res_per_level": [
                "res_id", "level",
                "per_minute", "stay_minutes",
                "amount_this_level", "amount_cumulative",
            ],
        },
        acceptance=[
            "行数 == 资源数 × level_max",
            "amount_this_level == per_minute × stay_minutes（公式登记）",
            "amount_cumulative 单调不减",
            "README 标注哪些资源采用膨胀、膨胀率范围",
        ],
        agent_hint=_AGENT_THREE_PHASE_HINT + (
            " 公式 amount_this_level / amount_cumulative 须 update_formula 登记。"
        ),
        common_pitfalls=[
            "对所有资源套用同一膨胀率，忽视「越来越多」感是否适用",
            "用每等级独立产量代替累计，下游分配阶段算错差值",
            "忽略 num_level_pacing 实际停留时间，凭空设产量",
        ],
        upstream_steps=["cultivation_resource_design", "base_attribute_framework"],
    ),
    StepSpec(
        step_id="cultivation_allocation_tables",
        title_zh="养成资源分配表",
        goal=(
            "对每个 (资源, 玩法, 等级) 给出消耗百分比，允许求和≠100%；为定量表与"
            "落地表提供权重输入。"
        ),
        inputs=[
            "cult_res_catalog：资源目录与归属",
            "num_res_per_level：每等级总产量",
            "num_gameplay_attr_scheme：玩法清单",
        ],
        outputs=[
            "num_cult_to_system：资源→玩法权重表",
            "README：分配策略、为什么允许求和≠100、与启动等级的关系",
        ],
        required_tables=["num_cult_to_system"],
        required_columns={
            "num_cult_to_system": [
                "res_id", "system_id", "level", "weight", "note",
            ],
        },
        acceptance=[
            "覆盖所有 (资源 × 该资源 scope 内玩法 × 全等级)",
            "weight 求和分布在 README 记录（不强制=1）",
            "未开放等级 weight=0 并在 README 注明",
            "README 给出「按资源看」与「按玩法看」两个切片示例",
        ],
        agent_hint=_AGENT_THREE_PHASE_HINT + (
            " 仅对 cult_res_catalog.scope_systems 内的玩法写入 weight，避免越界。"
        ),
        common_pitfalls=[
            "把所有资源摊到所有玩法，忽视 scope_systems 隔离",
            "强行 weight 求和=1，违背 03 规则",
            "对随等级膨胀的玩法（如后期开放系统）用常数 weight",
        ],
        upstream_steps=["cultivation_resource_framework", "cultivation_resource_design"],
    ),
    StepSpec(
        step_id="cultivation_quant_tables",
        title_zh="养成资源定量表",
        goal=(
            "结合资源框架与分配权重，得出每个 (玩法, 等级) 的可用资源数量；玩法落地表"
            "可以 @ 这张表来知道「升到 L 时还能花多少」。"
        ),
        inputs=[
            "num_res_per_level：累计/每等级产量",
            "num_cult_to_system：分配权重",
        ],
        outputs=[
            "num_cult_available：(系统 × 等级) 可用资源量",
            "README：差值计算口径（目标等级总产量 - 上一目标等级总产量）",
        ],
        required_tables=["num_cult_available"],
        required_columns={
            "num_cult_available": [
                "system_id", "res_id", "level",
                "available_this_level", "available_cumulative",
            ],
        },
        acceptance=[
            "行数 == 启用系统 × 资源 × level_max（按 scope 收敛后的实际组合）",
            "available_this_level 通过差值法（不是直接累加 per_minute）",
            "公式登记到 _formula_registry",
            "README 给出 1~2 个玩法的可用资源曲线示例",
        ],
        agent_hint=_AGENT_THREE_PHASE_HINT + (
            " 强调「差值口径」：available_this_level(L) = "
            "Σ_res(amount_cumulative(L) × weight(L)) - 上一档累计，必须在 README 写清。"
        ),
        common_pitfalls=[
            "用每等级独立 per_minute × weight 累加，忽略累计差值，导致前期低估",
            "把不在 scope 内的资源也分配给玩法",
            "未把公式登记到 _formula_registry，下游 @ 引用失效",
        ],
        upstream_steps=["cultivation_allocation_tables", "cultivation_resource_framework"],
    ),
    StepSpec(
        step_id="gameplay_landing_tables",
        title_zh="玩法系统落地表",
        goal=(
            "把每个玩法的具体玩法机制（装备穿戴、宝石合成、副本掉落、洗练概率…）"
            "落到具体的属性投放、消耗、概率与权重；这是与客户端策划交付的最终表。"
        ),
        inputs=[
            "num_gameplay_by_system：每系统每等级的属性",
            "num_cult_available：每系统每等级的可用资源",
            "全局 README 与 02 子系统默认细则",
        ],
        outputs=[
            "num_gameplay_landing：(系统 × 等级 × 玩法机制) 落地行",
            "各玩法子表（如有）：宝石按品阶轴、副本按门槛轴等",
            "README：每玩法的概率/消耗/权重设计原则、生命周期期望曲线",
        ],
        required_tables=["num_gameplay_landing"],
        required_columns={
            "num_gameplay_landing": [
                "system_id", "level",
                "consume_res_id", "consume_amount",
                "drop_weight", "probability",
                "design_note",
            ],
        },
        acceptance=[
            "覆盖每个启用系统在每等级的落地行（默认 4×60 = 240）",
            "concept_note 字段或 README 段落必须区分「按等级轴」vs「按品阶轴」玩法",
            "对概率类玩法（洗练、合成）给出生命周期期望图说明",
            "consume_amount 与 num_cult_available 对照，不超出可用量（差值口径）",
            "README 列出本项目的「权重设计曲线」与允许的凹点",
        ],
        agent_hint=_AGENT_THREE_PHASE_HINT + (
            " 严禁所有系统复用「标准等级 + 金币消耗 + 掉率」三列模板；"
            " 宝石必须以品阶/合成（3 同阶→1 高 1 品）为轴，"
            " 副本必须含 02 约定的开放等级与门槛字段。"
        ),
        common_pitfalls=[
            "所有系统共用一张落地模板，被 02/03 判负",
            "宝石按标准等级 1..N 拉行，丧失合成轴语义",
            "坐骑/副本只有金币+掉率，没有玩法特征列",
            "消耗超出 num_cult_available 差值仍写入，未在 README 标注调整",
            "概率类玩法没有期望曲线说明，无法对齐生命周期",
        ],
        upstream_steps=[
            "gameplay_attribute_tables",
            "cultivation_quant_tables",
        ],
    ),
]


_BY_ID: Dict[str, StepSpec] = {s.step_id: s for s in PIPELINE_STEP_SPECS}


_LANDING_SUB_TITLES: Dict[str, str] = {
    "equip": "装备落地表",
    "gem": "宝石落地表",
    "mount": "坐骑落地表",
    "wing": "翅膀落地表",
    "fashion": "时装落地表",
    "dungeon": "副本落地表",
    "skill": "技能落地表",
}


def _build_landing_sub_spec(sub: str) -> StepSpec:
    base = _BY_ID.get("gameplay_landing_tables")
    title = _LANDING_SUB_TITLES.get(sub, f"{sub} 落地表")
    extra_acceptance: List[str] = []
    if sub == "dungeon":
        extra_acceptance = [
            "副本_落地 必含列：dungeon_id / open_level / ticket_cost / daily_max_count / cumulative_ticket / 性价比",
            "cumulative_ticket = CUMSUM_TO_HERE(@@同表[ticket_cost])，注册公式后必须 execute（无空值）",
            "性价比禁严格单调递增；通关门槛由 IFS 条件公式批量生成",
        ]
    elif sub == "equip":
        extra_acceptance = [
            "暴击/闪避/命中/抗性等百分比列存为 [0, 0.95] 小数，number_format='0.00%'",
            "暴伤存小数（150% → 1.5），上限 ≤10，number_format='0.00%'",
            "主属性覆盖比若为常量请用 const_register('equip_main_attr_ratio', 0.6) 后用 ${equip_main_attr_ratio} 引用",
        ]
    elif sub == "gem":
        extra_acceptance = [
            "宝石按品阶/合成轴（3 同阶→1 高 1 品），不要按 1..N 标准等级拉行",
            "颜色/属性绑定与解锁门槛在 README 写清",
        ]
    elif sub == "mount":
        extra_acceptance = ["开放等级 30 默认；进阶曲线非线性；列必须有玩法含义"]
    return StepSpec(
        step_id=f"gameplay_landing_tables.{sub}",
        title_zh=title,
        goal=(base.goal if base else "") + f"\n（本子步只产出 {sub} 子系统的落地表）",
        inputs=list(base.inputs) if base else [],
        outputs=list(base.outputs) if base else [],
        required_tables=list(base.required_tables) if base else [],
        required_columns=dict(base.required_columns) if base else {},
        acceptance=list(base.acceptance) + extra_acceptance if base else extra_acceptance,
        agent_hint=(base.agent_hint if base else "")
        + f"\n本子步范围={sub}：禁止越界产出其他系统的表。",
        common_pitfalls=list(base.common_pitfalls) if base else [],
        upstream_steps=list(base.upstream_steps) if base else [],
    )


def get_step_spec(step_id: str) -> Optional[StepSpec]:
    if step_id in _BY_ID:
        return _BY_ID[step_id]
    if step_id.startswith("gameplay_landing_tables."):
        sub = step_id.split(".", 1)[1]
        if sub in _LANDING_SUB_TITLES:
            return _build_landing_sub_spec(sub)
    return None


def list_step_specs() -> List[StepSpec]:
    return list(PIPELINE_STEP_SPECS)


def render_spec_markdown(spec: StepSpec) -> str:
    """把 StepSpec 渲染为初始 README（Markdown）。"""

    def _ul(items: List[str]) -> str:
        return "\n".join(f"- {x}" for x in items) if items else "- （待补充）"

    def _table_columns_block() -> str:
        if not spec.required_columns:
            return "- （本步无需新建业务表）"
        lines = []
        for tbl, cols in spec.required_columns.items():
            cols_s = ", ".join(f"`{c}`" for c in cols)
            lines.append(f"- `{tbl}`：{cols_s}")
        return "\n".join(lines)

    upstream = ", ".join(f"`{s}`" for s in spec.upstream_steps) or "（无）"
    required_tables = ", ".join(f"`{t}`" for t in spec.required_tables) or "（无）"

    return (
        f"# 步骤 README — {spec.title_zh}\n"
        f"\n"
        f"- 步骤 ID：`{spec.step_id}`\n"
        f"- 上游步骤：{upstream}\n"
        f"- 必备表：{required_tables}\n"
        f"\n"
        f"## 设计目标\n\n{spec.goal}\n"
        f"\n"
        f"## 输入\n\n{_ul(spec.inputs)}\n"
        f"\n"
        f"## 产出\n\n{_ul(spec.outputs)}\n"
        f"\n"
        f"## 关键列\n\n{_table_columns_block()}\n"
        f"\n"
        f"## 验收清单（每条可勾选）\n\n"
        + "\n".join(f"- [ ] {a}" for a in spec.acceptance)
        + "\n\n"
        f"## Agent 执行提示（design → review → execute）\n\n{spec.agent_hint}\n"
        f"\n"
        f"## 已知坑 / 常见错误\n\n{_ul(spec.common_pitfalls)}\n"
        f"\n"
        f"---\n"
        f"\n"
        f"> 本 README 是步骤资产，需持续维护。每次 advance 后由 Agent 回填实际"
        f" acceptance 勾选与未决问题；用户也可手动编辑。\n"
    )
