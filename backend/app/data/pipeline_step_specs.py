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


_NAMING_RULE_HINT = (
    "【中英文命名强制规则】"
    "①所有名词（表名、列名、玩法名、资源名、子系统名、属性名）首次出现必须先调用 "
    "glossary_register(term_en, term_zh, brief, ...)；"
    "②正文/README/cell 中引用任何已注册名词必须使用 $term_en$ 引用符号，"
    "禁止裸中文或裸英文专名出现；"
    "③英文 term_en 必须为 snake_case 全小写，例：equip_base, gem_synth；"
    "④客户端会按列的 display_lang 自动渲染 $name$，不要手工硬编码语言。"
)

_MATRIX_TABLE_HINT = (
    "【matrix 表使用规则】"
    "①使用 create_matrix_table 创建（kind=matrix_attr 或 res_alloc）；"
    "②写入用 write_matrix_cells，行=玩法子系统(如 equip_base/equip_enhance)，列=属性或资源；"
    "③创建后必须 register_calculator 注册 fun(level, gameplay, attr|res[, grain])，"
    "brief 字段必须 ≥8 字符，写清楚函数语义、grain 含义、单位；"
    "④下游可用 call_calculator 取值，不要手工 read_matrix 拼装。"
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
            "以全等级（默认 60/200 行）拉出基础一阶属性曲线。"
            "本步遵循新规则（第3轮）：以攻击力按膨胀速率公式贯穿全等级（不分段），"
            "所有勾选属性必须在表中且必须有膨胀；高级属性（暴击率、闪避、命中、抗性等）"
            "由 AI 基于设计意图给出更合理的曲线（仍单调，线性或指数，禁止分段）；"
            "HP 不能拍脑袋，必须由 (atk, def, 战斗节奏=期望生存时间) 反推。"
        ),
        inputs=[
            "全局 README 中的属性勾选与公式选择（减法/乘法）",
            "默认常数与曲线策略（02 + 用户覆盖）",
            "标准等级数 settings.core.level_max",
            "战斗节奏：期望生存秒数（默认 8s 同级 PvE，可被用户覆盖）",
        ],
        outputs=[
            "num_base_framework：全等级一阶属性表（覆盖所有勾选属性）",
            "对应 README：写明攻击膨胀公式、各高级属性曲线、HP 反推公式与战斗节奏假设",
        ],
        required_tables=["num_base_framework"],
        required_columns={
            "num_base_framework": [
                "level", "atk", "def", "hp",
            ],
        },
        acceptance=[
            "num_base_framework 行数 == level_max（默认 60，禁止只填 2 行示例）",
            "atk 列严格单调递增，使用单一膨胀速率公式贯穿全等级（禁止分段）",
            "用户勾选的所有属性都有同名列且全等级单调（线性或指数，禁止分段）",
            "高级属性（暴击率、闪避、命中等百分比类）必须有合理上下界且单调",
            "hp 列由公式 hp = fn(atk_attacker, def_self, expected_survive_seconds) 反推得到，"
            "公式必须 update_formula 登记到 _formula_registry",
            "README 必须显式写出战斗节奏假设（期望生存秒数）与反推过程",
            "所有名词（每列名、属性中文名）必须先 glossary_register，README 用 $name$ 引用",
        ],
        agent_hint=_AGENT_THREE_PHASE_HINT + "\n" + _NAMING_RULE_HINT + (
            " design 阶段：明确攻击膨胀公式（线性 a+bL 或指数 a*r^L）、高级属性曲线方案、"
            "HP 反推公式与战斗节奏；review 阶段对照单调性与膨胀连续性自检；"
            "execute 时对每个 cell 写入用 source_tag=base_framework_v1。"
        ),
        common_pitfalls=[
            "攻击力分段膨胀（被新规则禁止）",
            "用户勾选了某属性但未在表中出现",
            "HP 直接填线性数列，未由攻防+战斗节奏反推",
            "高级属性写常数不膨胀",
        ],
        upstream_steps=["environment_global_readme"],
    ),
    StepSpec(
        step_id="gameplay_allocation",
        title_zh="玩法系统属性分配（行列双向语义 matrix 表）",
        goal=(
            "把第2轮的『玩法属性方案』+『玩法属性分配表』合并为一张 matrix 表。"
            "行=玩法子系统（必须把每个父系统拆为子系统，如 equip_base / equip_enhance / equip_amplify），"
            "列=每个属性，单元格=该子系统在该属性上的投放占比（百分数小数或 0~1）。"
            "AI 必须用 create_matrix_table 创建，再 register_calculator 注册 "
            "fun(level, gameplay, attr) 查询入口供下游 landing 表调用。"
        ),
        inputs=[
            "全局 README 的玩法清单与属性清单",
            "用户在 01 阶段勾选的子系统结构",
            "num_base_framework 的属性列",
        ],
        outputs=[
            "gameplay_attr_alloc（matrix 表，kind=matrix_attr）",
            "calculator: gameplay_attr_alloc_lookup（自动注册）+ AI 显式注册的 "
            "fun(level, gameplay, attr) 查询函数",
            "README：每行（玩法子系统）为什么投这些属性、留空(=0) 的设计意图",
        ],
        required_tables=["gameplay_attr_alloc"],
        required_columns={},
        acceptance=[
            "gameplay_attr_alloc 用 create_matrix_table(kind='matrix_attr') 创建",
            "行覆盖所有启用的玩法子系统（每个父系统至少拆出 1~3 个子系统）",
            "列覆盖所有勾选属性（缺一不可）",
            "全部属性都至少出现在 ≥1 个子系统的非零单元格中",
            "≥80% 的属性出现在 ≥2 个子系统中（README 中显式核对）",
            "为这张表 register_calculator，brief 字段 ≥8 字符并写清 grain 语义",
            "下游可用 call_calculator(name, level=L, gameplay=G, attr=A) 拿到值",
        ],
        agent_hint=_AGENT_THREE_PHASE_HINT + "\n" + _MATRIX_TABLE_HINT + "\n" + _NAMING_RULE_HINT + (
            " 不再使用旧 num_alloc_const / num_gameplay_attr_scheme 表，统一改用 matrix 表。"
            " 行命名建议：equip_base / equip_enhance / gem_synth / mount_advance 等；先 glossary_register。"
        ),
        common_pitfalls=[
            "把父系统当一行（应该拆子系统）",
            "强行让每行/每列求和=100%（不要求）",
            "忘了 register_calculator 或 brief 太短",
            "列里漏了某些勾选属性",
        ],
        upstream_steps=["base_attribute_framework"],
    ),
    StepSpec(
        step_id="cultivation_resource_framework",
        title_zh="养成资源基础框架表",
        goal=(
            "把第2轮的『养成资源设计』+『养成资源基础框架表』合并为单步。"
            "AI 先在 design 阶段列出所有资源（≥2 货币 + 各玩法专属道具，"
            "RPG 类型必须包含 experience），并 glossary_register；"
            "然后创建 num_resource_framework 表，按等级展开三档产量：小时/本级/累计；"
            "并设计单调递增的 time_weight 曲线，结合游戏生命周期反推 stay_hours。"
        ),
        inputs=[
            "玩法系统清单与子系统列表",
            "全局 README 中的养成偏好与游戏生命周期（默认 60 天）",
            "level_max",
        ],
        outputs=[
            "num_resource_framework：(level) × (time_weight, stay_hours_per_level, "
            "stay_hours_cumulative, <res>_per_hour, <res>_per_level, <res>_cumulative ...) ",
            "README：列出资源清单、每个资源的小时产量曲线策略、time_weight 设计原则",
        ],
        required_tables=["num_resource_framework"],
        required_columns={
            "num_resource_framework": [
                "level",
                "time_weight",
                "stay_hours_per_level",
                "stay_hours_cumulative",
            ],
        },
        acceptance=[
            "num_resource_framework 行数 == level_max",
            "time_weight 单调递增（严格 > 上一行）",
            "stay_hours_per_level 由 (time_weight / Σtime_weight) × (生命周期天数 × 每日游戏小时) 反推",
            "stay_hours_cumulative 单调不减且末行 ≈ 生命周期总时长",
            "≥2 种货币资源 + 至少每个父玩法有 1 种专属资源",
            "RPG 类型项目必须包含 experience 资源（同名列前缀 experience_*）",
            "对每个资源 res，必须有三列：${res}_per_hour、${res}_per_level、${res}_cumulative",
            "${res}_per_level = ${res}_per_hour × stay_hours_per_level（公式登记到 _formula_registry）",
            "${res}_cumulative 单调不减且 = Σ ${res}_per_level",
            "所有资源名先 glossary_register，README 用 $name$ 引用",
        ],
        agent_hint=_AGENT_THREE_PHASE_HINT + "\n" + _NAMING_RULE_HINT + (
            " design 阶段先列资源候选清单与每个资源的归属玩法，征得用户/上游确认后 execute；"
            " 公式 stay_hours / per_level / cumulative 都要 update_formula 登记。"
        ),
        common_pitfalls=[
            "time_weight 写常数（应单调递增）",
            "RPG 项目漏掉 experience 资源",
            "三档产量只写 per_hour 或 cumulative，没全（必须三档都有）",
            "把多个资源压缩成一张子表（应当用同一张表的多列）",
            "用 minutes 单位（本轮统一用 hours）",
        ],
        upstream_steps=["base_attribute_framework"],
    ),
    StepSpec(
        step_id="cultivation_allocation",
        title_zh="养成资源分配（行列双向语义 matrix 表）",
        goal=(
            "做一张 matrix 表，行=玩法子系统、列=资源、单元格=该子系统对该资源的投放比例。"
            "并 register_calculator 注册 fun(level, gameplay, res, grain) 查询入口，"
            "grain 取 per_hour / per_level / cumulative 之一。"
        ),
        inputs=[
            "num_resource_framework：每等级三档产量",
            "玩法子系统清单（与 gameplay_attr_alloc 行一致）",
        ],
        outputs=[
            "gameplay_res_alloc（matrix 表，kind=matrix_resource）",
            "calculator: gameplay_res_alloc_lookup + AI 显式注册的 "
            "fun(level, gameplay, res, grain) 查询函数",
            "README：分配策略、为什么允许求和≠100、与启动等级的关系",
        ],
        required_tables=["gameplay_res_alloc"],
        required_columns={},
        acceptance=[
            "gameplay_res_alloc 用 create_matrix_table(kind='matrix_resource') 创建",
            "行覆盖所有玩法子系统（同 gameplay_attr_alloc 的行）",
            "列覆盖 num_resource_framework 中出现的所有资源",
            "register_calculator 必须包含 grain 形参，brief ≥8 字符并描述三档语义",
            "下游可用 call_calculator(name, level=L, gameplay=G, res=R, grain='per_level') 取值",
            "未覆盖的 (玩法×资源) 单元格留空或 0，README 注明设计原因",
        ],
        agent_hint=_AGENT_THREE_PHASE_HINT + "\n" + _MATRIX_TABLE_HINT + "\n" + _NAMING_RULE_HINT + (
            " grain 的实现方式建议：把 grain 拼到列名（如 currency_a@per_level）"
            "或注册时记录在 axes 上，由 calculator 查询逻辑映射到 num_resource_framework 的对应列。"
        ),
        common_pitfalls=[
            "把资源摊到所有玩法（违背 scope 隔离）",
            "强行 weight 求和=1",
            "register_calculator 未含 grain 形参",
            "行与 gameplay_attr_alloc 不一致（应一致）",
        ],
        upstream_steps=["cultivation_resource_framework", "gameplay_allocation"],
    ),
    StepSpec(
        step_id="gameplay_landing_tables",
        title_zh="玩法系统落地表",
        goal=(
            "把每个玩法/子系统的具体机制（穿戴、合成、洗练、掉落、强化…）落到具体的"
            "属性投放、消耗、概率与权重。子步按子系统（equip_base / equip_enhance / "
            "gem_synth / mount_advance / dungeon_main 等）独立产出，每个子步只负责一张表。"
        ),
        inputs=[
            "gameplay_attr_alloc（matrix）+ 同名 _lookup calculator",
            "gameplay_res_alloc（matrix）+ 同名 _lookup calculator",
            "num_resource_framework（每等级三档产量）",
            "num_base_framework（每等级一阶属性）",
            "list_exposed_params(target_step) 注入的父系统暴露参数",
        ],
        outputs=[
            "<subsystem>_landing 表：(等级/品阶 × 落地字段) 行，列含义随子系统而定",
            "辅助列（如果计算复杂）：在同表内创建辅助列即可，不要拆多表",
            "README：本子系统的属性来源(call_calculator)、资源消耗来源、概率/权重曲线设计",
        ],
        required_tables=[],
        required_columns={},
        acceptance=[
            "表名必须先 glossary_register（term_en=表英文名、term_zh=表中文名）",
            "属性投放值通过 call_calculator(gameplay_attr_alloc_*, level=L, "
            "gameplay=本子系统, attr=A) 取，不要硬编码",
            "资源消耗通过 call_calculator(gameplay_res_alloc_*, level, gameplay, res, "
            "grain='per_level' 或 'cumulative') 取",
            "如需累计差值（例如 L 升 L+1 的实际消耗），自行创建辅助列并登记公式",
            "概率/权重列必须单调或在 README 显式说明非单调原因",
            "若本子系统需要向其他子系统暴露设计参数，调用 expose_param_to_subsystems "
            "声明，并在 README 末尾列出 exposed_params",
            "README 用 $term_en$ 引用所有名词，禁止裸中英文专名",
        ],
        agent_hint=_AGENT_THREE_PHASE_HINT + "\n" + _MATRIX_TABLE_HINT + "\n" + _NAMING_RULE_HINT + (
            " design 阶段：先 list_exposed_params(本步) 看父系统传过来的设计常数；"
            " 列出本表的轴（按等级 / 按品阶 / 按门槛）、需要的辅助列、要调用哪些 calculator；"
            " review 阶段对照 acceptance；execute 时分批写入。"
            " 子系统命名：equip_base / equip_enhance / equip_amplify / gem_synth / "
            "mount_advance / dungeon_main 等；与 matrix 表的行键保持一致。"
        ),
        common_pitfalls=[
            "所有子系统复用同一个三列模板（被 02/03 判负）",
            "宝石按 1..N 标准等级拉行，丧失品阶/合成轴",
            "硬编码属性数值，没有 call_calculator",
            "需要的辅助列不创建，直接写常数",
            "应当向兄弟子系统暴露的参数没有 expose_param_to_subsystems 声明",
        ],
        upstream_steps=[
            "gameplay_allocation",
            "cultivation_allocation",
            "cultivation_resource_framework",
            "base_attribute_framework",
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
