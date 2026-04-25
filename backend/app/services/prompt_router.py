"""提示词路由：按 pipeline 步骤匹配默认玩法/系统提示词，否则让 LLM 现编一段。"""

from __future__ import annotations

import json
from typing import Any, Dict

from app.config import QWEN_MODEL
from app.services.qwen_client import get_client


# 与 routers/pipeline.py PIPELINE_STEPS 一一对应的默认提示词模板。
# 每段简短描述：本步必产出（表名/列名/接受标准），便于 design 阶段对齐。
DEFAULT_STEP_PROMPTS: Dict[str, str] = {
    "environment_global_readme": (
        "【步骤 1/11 环境与全局 README】"
        "目标：固化项目级元数据与全局 README，为后续所有步骤提供数值基线。\n"
        "【操作流程】\n"
        "1. 调用 get_project_config 读取现有配置，重点提取 fixed_layer_config.core 下的字段："
        "   level_cap(最大等级)、lifecycle_days、game_type、business_model、theme、magnitude、"
        "   defense_formula、combat_rhythm 等。\n"
        "2. 使用 set_project_setting 写入以下顶层键（从 fixed_layer_config 中推导）："
        "   - max_level: 整数（来自 level_cap）"
        "   - currencies: 对象，例如 {\"gold\": \"软通货\", \"bound_diamond\": \"绑定硬通货\", \"dust\": \"玩法专属\"}"
        "   - stat_keys: 数组，列出核心属性ID（从 attribute_systems.selectedAttrs 提取）"
        "   - resource_keys: 数组，主要养成资源ID列表（根据 game_systems 推导）\n"
        "3. 使用 update_global_readme 写入全面的全局 README（必须包含6个字段）："
        "   goal / upstream_input / output / required_tables_cols / acceptance_criteria / pitfalls"
        "   以及项目定位/核心循环/数值哲学/版本节奏/术语表。\n"
        "【重要】不要尝试 create_table('project_settings')——project_settings 是系统表已存在！"
        "【重要】不要尝试 write_cells 写 project_settings——应使用 set_project_setting 工具。\n"
        "验收：get_project_config 返回包含 max_level/currencies/stat_keys/resource_keys 键；"
        "global_readme 含完整6字段且非占位符。"
    ),
    "base_attribute_framework": (
        "【步骤 2/11 基础属性框架】"
        "目标：定义角色基础属性骨架，输出标准等级 1..max_level 的基础属性增长表。"
        "必须先读 get_project_config，从 fixed_layer_config.core.game_type 判断游戏类型：\n"
        "  · rpg_turn（回合制）：核心属性通常为 HP / ATK / DEF / 命中 / 闪避 / 暴击 / 暴伤 等；\n"
        "  · rpg_realtime（即时制）：额外需要 移动速度(move_spd) / 攻击速度(atk_spd) / 基础攻击间隔(base_atk_interval) 等；\n"
        "  具体以项目 attribute_systems.selectedAttrs 为准，不要生搬列名。\n"
        "必产出表：基础属性_标准等级（行=等级，列=项目所选属性），"
        "并写入 README（goal/上游输入/产出/必备表与列/验收/常见踩坑）。"
        "验收：每行均有数值、增长曲线连续、与 _project_settings.max_level 对齐。\n"
        "【★ 强制效率方式 ★】\n"
        "  · **必须用 `setup_level_table`** 一次完成「建表+填 max_level 行+所有列公式注册并执行」，所有列的 formula_string 同时塞进 columns 数组里。\n"
        "  · **严禁** 逐行 `write_cells` 数值。\n"
        "  · 公式示例（@T 会被自动替换为本表名，max_level=200 时）：\n"
        "      HP:       ROUND(1000 + 49000*POWER((@T[等级]-1)/(199), 0.85), 0)\n"
        "      ATK:      ROUND(100  + 4900 *POWER((@T[等级]-1)/(199), 0.85), 0)\n"
        "      DEF:      ROUND(60   + 2940 *POWER((@T[等级]-1)/(199), 0.85), 0)\n"
        "      atk_spd（即时制）: ROUND(1.0 + 0.5*POWER((@T[等级]-1)/(199), 0.5), 2)\n"
        "  · 写完后 update_table_readme 一次性补 README；与 get_default_system_rules / get_project_setting('max_level') 对齐。"
    ),
    "gameplay_attribute_scheme": (
        "【步骤 3/11 玩法属性方案】"
        "目标：列出各玩法系统（装备/宝石/坐骑/翅膀/时装/副本…）拟提供的属性维度与占比策略，"
        "产出 玩法属性方案（系统、提供属性集合、属性占比/上限、获取节奏说明）。"
        "验收：每个 02 文档约定的核心系统都有一行；属性键来自 _project_settings.stat_keys。"
    ),
    "gameplay_allocation_tables": (
        "【步骤 4/11 玩法属性分配表】"
        "目标：把方案细化为「按系统×标准等级」的属性分配比例表（行=标准等级，列=该系统提供的各属性数值或百分比）。"
        "必产出：每个核心系统一张「<系统>_属性分配」表；总和不超过当级总属性预算。"
        "验收：横向加总符合方案占比；空缺列必须有理由（README 备注）。\n"
        "【★ 强制效率方式 ★】每张分配表用 `setup_level_table` 一次建好；"
        "占比固定时用常量列公式，如 `0.4`；随等级渐变用 `0.3 + 0.1*((@T[等级]-1)/(199))`；"
        "**严禁** 逐行 write_cells。"
    ),
    "second_order_framework": (
        "【步骤 5/11 二阶属性框架】"
        "目标：派生战力、伤害公式相关二阶属性（暴击率→实际暴伤、命中差→命中率等）。"
        "必产出：二阶属性_公式（属性名、公式、依赖一阶属性、上下限）；可选注册到 formula_engine。"
        "验收：与 02 默认细则的一致；recalculate_downstream 能跑通。"
    ),
    "gameplay_attribute_tables": (
        "【步骤 6/11 玩法属性表】"
        "目标：把分配比例×标准等级基础属性 → 各系统每级实际属性表。"
        "必产出：每个系统一张「<系统>_属性表」（行=该系统等级/品阶，列=具体属性数值）。"
        "宝石请用品阶/合成体系（3 同阶=1 高 1 品），不要把标准等级 1..N 当成「宝石 N 级」。"
        "验收：与分配表数值闭环；坐骑等开放等级遵循 02（坐骑 30 级等）。\n"
        "【★ 强制效率方式 ★】首选 `setup_level_table` 或 `bulk_register_and_compute`，公式直接引用上游表，"
        "如 `@基础属性_标准等级[HP] * @装备_属性分配[HP占比]`。**严禁**逐行写。"
    ),
    "cultivation_resource_design": (
        "【步骤 7/11 养成资源设计】"
        "目标：列出每个系统的养成资源（材料、消耗道具、产出节奏），形成资源清单。"
        "必产出：养成资源_清单（资源 ID、名称、产出来源、消耗去向、稀有度、典型日产量）。"
        "验收：覆盖全部需要养成的系统；与 _project_settings.resource_keys 一致。"
    ),
    "cultivation_resource_framework": (
        "【步骤 8/11 养成资源框架】"
        "目标：搭建资源在系统间的流转框架（产出→背包→消耗→升级→属性）。"
        "必产出：资源流转图说明 + 各系统「养成节点」表骨架（升级节点、消耗资源、产出属性档）。"
        "验收：每条消耗链有明确产出口径与瓶颈点；README 标注。"
    ),
    "cultivation_allocation_tables": (
        "【步骤 9/11 养成分配表】"
        "目标：把资源在各系统、各等级上的消耗量初稿铺开（按系统×等级=资源消耗）。"
        "必产出：每个系统一张「<系统>_养成分配」（行=系统等级，列=各资源消耗数量）。"
        "验收：纵向递增合理；与玩家日产量节奏一致；不出现 0 消耗跳级。\n"
        "【★ 强制效率方式 ★】用 `setup_level_table`，公式样例："
        "`ROUND(100 + 50*POWER(@T[等级], 1.5), 0)` 或 `IFS(@T[等级]<=10, 100, @T[等级]<=50, 500, 2000)`；"
        "复杂分段也可调 `call_algorithm_api` 的 `linear_resource_cost / piecewise_curve` 拿到列向量再 write_cells。"
    ),
    "cultivation_quant_tables": (
        "【步骤 10/11 养成量化表】"
        "目标：在分配表基础上做量化（金币/绑钻/材料的具体数值定稿），可注册公式自动推导。"
        "必产出：每个系统一张「<系统>_养成量化」（含资源数量、累计消耗、对应属性、性价比）。"
        "验收：累计消耗与日产量曲线对齐；性价比单调或有意为之的拐点。\n"
        "【★ 强制效率方式 ★】用 `bulk_register_and_compute` 一次注册多个跨表公式；"
        "累计列 = 单级 * 等级（近似）或调 `call_algorithm_api(linear_resource_cost)` 取累计向量；"
        "性价比 = `@同表[属性增益] / @同表[消耗]`。**禁止逐格写**。"
    ),
    "gameplay_landing_tables": (
        "【步骤 11/11 玩法落地表】"
        "目标：把每个玩法系统的最终落地表写出来——区别于「仅标准等级+两列消耗」的偷懒模板，"
        "宝石用品阶/合成与解锁门槛/属性池/分配；坐骑/副本带开放等级与玩法含义列。"
        "必产出：每个系统一张「<系统>_落地」表，列要有玩法含义而非只有金币+掉率。"
        "验收：与 02 默认细则全部对齐；get_default_system_rules 比对通过。"
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

    # 未命中：让千问现写一段对应本步骤的提示词
    try:
        gen = client.chat.completions.create(
            model=model or QWEN_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "你是 Numflow 的提示词撰写器。基于当前 pipeline 步骤、用户需求与项目配置，"
                        "写一段简短（<=300 字）的「玩法/系统」提示词，明确本次任务必产出（表名/列名/验收标准）。"
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
    except Exception as e:  # noqa: BLE001
        custom = default_prompt or f"（提示词生成失败：{e!r}；按通用助手处理）"

    return {
        "hit": False,
        "prompt": custom,
        "rationale": rationale or "default_template_not_matched",
    }
