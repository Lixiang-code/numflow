"""Agent 可调用的工具实现（对齐文档 06，与现有 HTTP 能力一致）。"""

from __future__ import annotations

import copy
import json
import sqlite3
from itertools import product
from typing import Any, Dict, List, Optional, Union

from app.deps import ProjectDB
from app.services import algorithms
from app.services.cell_writes import apply_write_cells, assert_col_or_table
from app.services.formula_engine import normalize_self_table_refs
from app.services.formula_exec import (
    execute_formula_on_column,
    recalculate_downstream,
    register_formula,
)
from app.services.prompt_overrides import get_prompt_override, merge_prompt_item_layers
from app.data.default_rules_02 import get_default_rules_payload
from app.services.gameplay_table_registry import list_registered_gameplay_tables, utc_now_iso
from app.services.skill_library import (
    get_skill_detail as _get_skill_detail,
    list_skills as _list_skills,
    render_skill_file as _render_skill_file,
)
from app.services.snapshot_ops import compare_snapshot, create_snapshot, list_snapshots
from app.services.table_ops import create_dynamic_table, delete_dynamic_table, create_3d_table, read_3d_table
from app.services.tool_envelope import wrap_tool_payload
from app.services.validation_report import build_validation_report, list_validation_history, confirm_validation_rule as _confirm_validation_rule

TOOLS_OPENAI: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_project_config",
            "description": "读取项目配置与 project_settings（含 global_readme、fixed_layer_config 等）",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_list",
            "description": "列出所有业务表及验证状态（cols+rows 紧凑格式）",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_schema",
            "description": "读取指定表的结构信息：列定义、目录、标签、矩阵/三维元信息、公式摘要；适合空表或改表前先看结构",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "目标表名，建议先通过 get_table_list 获取"},
                    "include_readme_excerpt": {"type": "boolean", "default": True, "description": "是否附带 README 摘要而非全文"},
                    "include_formulas": {"type": "boolean", "default": True, "description": "是否附带该表已注册公式摘要"},
                },
                "required": ["table_name"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_table",
            "description": "读取表数据（返回紧凑行列格式：cols + rows）；可选 columns、filters（每项 column+value 相等）、level_column+level_min/max、include_source_stats",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "limit": {"type": "integer", "default": 50},
                    "columns": {"type": "array", "items": {"type": "string"}},
                    "filters": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column": {"type": "string"},
                                "value": {},
                            },
                            "required": ["column"],
                        },
                    },
                    "level_column": {"type": "string"},
                    "level_min": {"type": "number"},
                    "level_max": {"type": "number"},
                    "include_source_stats": {"type": "boolean", "default": False},
                },
                "required": ["table_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_cell",
            "description": "读取单个单元格的值及来源标记",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "row_id": {"type": "string"},
                    "column_name": {"type": "string"},
                },
                "required": ["table_name", "row_id", "column_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_protected_cells",
            "description": "列出指定表中 user_manual 保护单元格坐标",
            "parameters": {
                "type": "object",
                "properties": {"table_name": {"type": "string"}},
                "required": ["table_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_dependency_graph",
            "description": "依赖边列表（cols+rows 紧凑格式）；direction: upstream|downstream|full（与 /meta/dependency-graph 一致）",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "direction": {
                        "type": "string",
                        "enum": ["upstream", "downstream", "full"],
                        "default": "full",
                    },
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_readme",
            "description": "读取指定业务表的 README 文本",
            "parameters": {
                "type": "object",
                "properties": {"table_name": {"type": "string"}},
                "required": ["table_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_3d_table",
            "description": (
                "读取三维数据表切片。默认返回兼容旧行为的按 dim2 分组紧凑投影视图；"
                "若传 keep_axes，则可按 dim1 / dim2 / metric 任意组合切片，例如"
                " keep_axes=['dim1','metric'] + dim2_keys=['atk'] 查看“所有攻击宝石的属性”，"
                " keep_axes=['dim2','metric'] + dim1_keys=['1'] 查看“所有 1 级宝石的属性”，"
                " keep_axes=['metric'] + dim1_keys=['1'] + dim2_keys=['atk'] 查看“1级攻击宝石的全部属性”。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "目标三维数据表名，必须由 create_3d_table 创建"},
                    "dim1_keys": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "可选，筛选第一维 key（如等级）",
                    },
                    "dim2_keys": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "可选，筛选第二维 key（如宝石类型 / 装备部位）",
                    },
                    "metric_keys": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "可选，筛选属性列 key（如 atk_bonus / hp_bonus）",
                    },
                    "keep_axes": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["dim1", "dim2", "metric"]},
                        "description": "可选，指定保留为切片输出的轴；长度 1 或 2。未传时走兼容旧行为的紧凑投影。",
                    },
                    "limit_dim1": {
                        "type": "integer",
                        "default": 30,
                        "description": "兼容旧行为：未指定 dim1_keys 且未使用 keep_axes 时，最多返回多少个 dim1 行",
                    },
                    "limit_per_axis": {
                        "type": "integer",
                        "default": 50,
                        "description": "使用 keep_axes 时，未显式筛选的每个轴最多返回多少个 key，避免结果过大",
                    },
                    "include_formulas": {
                        "type": "boolean",
                        "default": True,
                        "description": "是否附带相关属性列公式",
                    },
                },
                "required": ["table_name"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_3d_table_full",
            "description": (
                "完整读取三维数据表的 canonical 三轴结构。"
                "会返回 dim1 / dim2 / metric 三个轴的全部 key、完整嵌套 data，以及属性列公式；"
                "适合需要整体建模、推导或自行决定切片方式的场景。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "目标三维数据表名"},
                    "include_formulas": {
                        "type": "boolean",
                        "default": True,
                        "description": "是否附带属性列公式元信息",
                    },
                },
                "required": ["table_name"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_skills",
            "description": "列出当前项目可用的 SKILL（可按 step_id 过滤）；返回 slug、标题、摘要、默认暴露与调用次数",
            "parameters": {
                "type": "object",
                "properties": {
                    "step_id": {"type": "string"},
                    "include_disabled": {"type": "boolean", "default": False},
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_skill_detail",
            "description": "读取指定 SKILL 的完整详情：基础信息、模块列表、启用状态、生成结果摘要",
            "parameters": {
                "type": "object",
                "properties": {
                    "skill_slug": {"type": "string", "description": "SKILL 的 slug，建议先通过 list_skills 获取"},
                },
                "required": ["skill_slug"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "render_skill_file",
            "description": "查看指定 SKILL 按当前用户配置生成出的实际 Markdown 文件内容",
            "parameters": {
                "type": "object",
                "properties": {
                    "skill_slug": {"type": "string", "description": "SKILL 的 slug，建议先通过 list_skills 获取"},
                },
                "required": ["skill_slug"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_table_readme",
            "description": "覆盖更新指定表的 README（需写权限）",
            "parameters": {
                "type": "object",
                "properties": {"table_name": {"type": "string"}, "content": {"type": "string"}},
                "required": ["table_name", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_global_readme",
            "description": "更新项目全局 README",
            "parameters": {
                "type": "object",
                "properties": {"content": {"type": "string"}},
                "required": ["content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "set_project_setting",
            "description": (
                "写入 project_settings 中的一个键值对。"
                "用于设置 max_level / currencies / stat_keys / resource_keys 等顶层项目参数。"
                "value 可为任意 JSON 值（字符串/数字/数组/对象）。"
                "注意：fixed_layer_config 受保护不可覆盖；global_readme 请用 update_global_readme。"
                "调用示例：{\"key\": \"max_level\", \"value\": 200}"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "key": {"type": "string", "description": "设置键名（如 max_level / currencies / stat_keys / resource_keys）"},
                    "value": {"description": "设置值，任意 JSON 类型（数字/字符串/数组/对象均可）"},
                },
                "required": ["key", "value"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_table",
            "description": (
                "创建动态业务表并写入 _table_registry。\n"
                "命名规则（严格）：\n"
                "  table_name：英文 snake_case，如 base_attr_table\n"
                "  display_name（表级）：中文，如「基础属性表」，必填\n"
                "  columns[].name：英文 snake_case 标识符，用于公式引用/存储，**只能含英文+数字+下划线**\n"
                "  columns[].display_name：中文列名，用于表头展示，必填，如「攻击力」\n"
                "columns 每项含：name / sql_type(TEXT|REAL|INTEGER) / display_name / dtype / number_format\n"
                "number_format 格式说明见下方参数描述。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "英文表名（snake_case，只含英文/数字/下划线）"},
                    "display_name": {"type": "string", "description": "表的中文显示名，必填，如「基础属性表」"},
                    "columns": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string", "description": "英文列名（snake_case），用于公式 @引用 和数据库存储，必须是纯英文"},
                                "sql_type": {"type": "string", "enum": ["TEXT", "REAL", "INTEGER"]},
                                "display_name": {"type": "string", "description": "中文列名，仅用于表头展示，必填，如「攻击力」"},
                                "dtype": {"type": "string", "description": "语义类型: int/float/percent/str/bool/id/ref/enum/json"},
                                "number_format": {
                                    "type": "string",
                                    "description": (
                                        "数值显示格式（仅影响表格阅读，不影响存储值）。"
                                        "常用格式：\n"
                                        "  整数:    '0'\n"
                                        "  1位小数: '0.0'\n"
                                        "  2位小数: '0.00'\n"
                                        "  百分比:  '0.00%'\n"
                                        "  千分位:  '#,##0'\n"
                                        "  千分位+小数: '#,##0.00'\n"
                                        "  字符串:   '@'\n"
                                        "  不设置则留空"
                                    ),
                                },
                            },
                            "required": ["name", "sql_type", "display_name", "dtype"],
                        },
                    },
                    "readme": {"type": "string", "default": ""},
                    "purpose": {"type": "string", "default": ""},
                    "kind": {
                        "type": "string",
                        "enum": ["base", "alloc", "attr", "quant", "landing", "resource", "unknown"],
                        "description": (
                            "表类型，用于自动挂载默认校验规则："
                            "base=基础属性、alloc=分配比例、attr=玩法属性、"
                            "quant=养成量化、landing=落地表、resource=资源；"
                            "若不传则按表名启发式推断。"
                        ),
                    },
                    "directory": {
                        "type": "string",
                        "description": "目录路径（强烈建议填写，'/' 分隔），如 '基础属性' / '落地表/装备' / '养成资源'。便于工作台目录树管理。",
                    },
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "表的标签列表（至少1个，用于相关常数筛选），如 ['属性', '基础']。与常量的 tags 做交集匹配，决定右侧面板显示哪些相关常数。",
                        "minItems": 1,
                    },
                },
                "required": ["table_name", "display_name", "columns"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_cells",
    "description": (
                "批量写入单元格值；跳过 user_manual 保护单元格。"
                "适用场景：① 分类/标签/描述等非规律内容（系统名、道具名、备注等）"
                "② 少量手工配置值（≤30行/次）。"
                "禁止场景：等级序列/规律数值 → 用 setup_level_table；"
                "整列计算值 → 用 register_formula+execute_formula。"
                "updates 每项含 row_id、column、value；source_tag 默认 ai_generated。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "updates": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "row_id": {"type": "string"},
                                "column": {"type": "string"},
                                "value": {},
                            },
                            "required": ["row_id", "column"],
                        },
                    },
                    "source_tag": {
                        "type": "string",
                        "enum": ["ai_generated", "algorithm_derived", "formula_computed"],
                        "default": "ai_generated",
                    },
                },
                "required": ["table_name", "updates"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_cells_series",
            "description": (
                "★ 系列填充：用模板生成连续 row_id（如 lv_1..lv_50）的写入，避免一次性贴数百行 JSON。"
                "row_id_template 必须包含 {i} 占位符；start..end 闭区间生成索引；"
                "value_list 与索引一一对应（长度需 = end-start+1），"
                "或用 value_template（含 {i}）+ 表达式 expr 计算（expr 可用 i 变量，如 'i*100+50'）。"
                "value_list 与 expr 二选一。column 是目标列名。适用于：等级表数值列、批量配置项。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "row_id_template": {
                        "type": "string",
                        "description": "row_id 模板，必须包含 {i}，例如 'lv_{i}' 或 'item_{i}'",
                    },
                    "column": {"type": "string"},
                    "start": {"type": "integer"},
                    "end": {"type": "integer"},
                    "value_list": {
                        "type": "array",
                        "items": {},
                        "description": "与 [start..end] 一一对应的值数组（长度严格相等）",
                    },
                    "expr": {
                        "type": "string",
                        "description": "受限算术表达式（变量 i 表示当前索引），如 'i*100+50'、'2**i'",
                    },
                    "source_tag": {
                        "type": "string",
                        "enum": ["ai_generated", "algorithm_derived", "formula_computed"],
                        "default": "ai_generated",
                    },
                },
                "required": ["table_name", "row_id_template", "column", "start", "end"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "register_formula",
            "description": "为指定列注册公式字符串（@表名[列名] 引用）；更新依赖图",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "column_name": {"type": "string"},
                    "formula_string": {"type": "string"},
                },
                "required": ["table_name", "column_name", "formula_string"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "execute_formula",
            "description": "执行已注册公式并写回列；可选 level_column+level_min/max 仅更新区间内行；写 _cell_provenance=formula_computed",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "column_name": {"type": "string"},
                    "level_column": {"type": "string"},
                    "level_min": {"type": "number"},
                    "level_max": {"type": "number"},
                },
                "required": ["table_name", "column_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "recalculate_downstream",
            "description": "从指定上游列沿依赖图重算下游公式列",
            "parameters": {
                "type": "object",
                "properties": {"table_name": {"type": "string"}, "column_name": {"type": "string"}},
                "required": ["table_name", "column_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_algorithm_api_list",
            "description": "列出已注册算法 API 元数据",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "call_algorithm_api",
            "description": "调用算法层 API（如 echo_sum），返回结构化结果",
            "parameters": {
                "type": "object",
                "properties": {
                    "api_name": {"type": "string"},
                    "params": {"type": "object"},
                },
                "required": ["api_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_validation",
            "description": "运行校验报告（含表级规则 violations）；可选 table_name 仅针对单表",
            "parameters": {
                "type": "object",
                "properties": {"table_name": {"type": "string"}},
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "confirm_validation_rule",
            "description": (
                "将指定校验规则标记为「已确认通过」，后续 run_validation 将跳过该规则报警。\n"
                "典型用途：当 percent_bounds 报告某列值超出 [0,1] 但设计本身合理时（如暴击伤害倍率=1.5），"
                "调用此工具确认，填写 reason 说明理由。确认后该 rule_id 不再触发报警。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "规则所在的表"},
                    "rule_id": {"type": "string", "description": "要确认的规则 ID（来自 run_validation 的 rule_id 字段）"},
                    "reason": {"type": "string", "description": "确认理由，说明为何此设计合理（选填但建议填写）"},
                },
                "required": ["table_name", "rule_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "delete_table",
            "description": "删除动态表；confirm 须为 true；若存在公式依赖本表列则拒绝",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "confirm": {"type": "boolean"},
                },
                "required": ["table_name", "confirm"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_snapshot",
            "description": "创建当前所有业务表快照（表级+列级哈希，便于 compare_snapshot）",
            "parameters": {
                "type": "object",
                "properties": {"label": {"type": "string"}, "note": {"type": "string", "default": ""}},
                "required": ["label"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_snapshots",
            "description": "列出最近快照元数据（cols+rows 紧凑格式）",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compare_snapshot",
            "description": "将当前表哈希与指定快照对比，返回变更表列表",
            "parameters": {
                "type": "object",
                "properties": {"snapshot_id": {"type": "integer"}},
                "required": ["snapshot_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_balance_check",
            "description": "平衡指标检查（占位实现）",
            "parameters": {
                "type": "object",
                "properties": {
                    "level_min": {"type": "integer"},
                    "level_max": {"type": "integer"},
                    "metrics": {"type": "array", "items": {"type": "string"}},
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_validation_history",
            "description": "最近校验历史摘要（按表过滤可选）",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "limit": {"type": "integer", "default": 20},
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "bulk_register_and_compute",
            "description": (
                "【高效】一次注册并执行多列公式（最常用）。"
                "items 每项含 column_name + formula_string，可选 level_column/level_min/level_max；"
                "可选 register_only=true 只注册不执行。"
                "公式语法：\n"
                "  逐行引用：@表名[列名]（同行取值，用于数学运算）\n"
                "  整列引用：@@表名[列名]（整列 list，用于 VLOOKUP/INDEX/MATCH/SUM/AVERAGE 及逐元素比较）\n"
                "  运算：+ - * / ** %、ROUND/FLOOR/CEIL/ABS/SQRT/EXP/LOG/POW/POWER/MIN/MAX/CLAMP/"
                "IF/IFS/PIECEWISE/AND/OR/NOT/MOD（大小写不敏感）\n"
                "  比较：< <= > >= == !=（支持 @@col 与标量/@@col2 逐元素广播，返回 bool 列表）\n"
                "  查找：VLOOKUP(val,@@lkup,@@ret,[exact]) / XLOOKUP(val,@@lkup,@@ret,[ifna]) / "
                "INDEX(@@col,row) / MATCH(val,@@col) / LOOKUP(val,@@lkup,@@ret)\n"
                "  聚合：SUM(@@col) / AVERAGE(@@col) / COUNT(@@col)\n"
                "  条件聚合：SUM(IF(@@col < @表[col], @@val, 0))（典型：累计经验 / 前缀和）\n"
                "  累计求和：CUMSUM_TO_HERE(@@col)（含本行）/ CUMSUM_PREV(@@col)（截至上一行）\n"
                "一个公式即可填满整列（200 行/8 列只需 8 次调用，请优先使用，禁止逐行 write_cells）。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column_name": {"type": "string"},
                                "formula_string": {"type": "string"},
                                "level_column": {"type": "string"},
                                "level_min": {"type": "number"},
                                "level_max": {"type": "number"},
                            },
                            "required": ["column_name", "formula_string"],
                        },
                    },
                    "register_only": {"type": "boolean", "default": False},
                },
                "required": ["table_name", "items"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "setup_level_table",
            "description": (
                "【高效·一步建好等级表】建表 + 自动生成 1..max_level 行 + 批量公式（每列一个公式）+ 立即执行。"
                "适用于「随等级递增」的属性表/消耗表/经验表等规律表格。"
                "level_column 默认 'level'（英文）；columns 每项含 name + sql_type（默认 'REAL'）+ 可选 formula_string。"
                "公式中 @T[列] 用于同行逐行引用；@@表名[列] 用于查找函数整列引用；@T 会自动替换为本表名。"
                "示例：columns=[{name:'level',sql_type:'INTEGER',display_name:'等级'},"
                "{name:'hp',formula_string:'ROUND(${hp_lv1}+(${hp_max}-${hp_lv1})*POWER((@T[level]-1)/(${max_level}-1),0.85),0)',display_name:'HP'}]"
                "★ 注意：name 必须英文 snake_case（a-z/0-9/_），中文写到 display_name。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "英文表名（snake_case）"},
                    "max_level": {"type": "integer"},
                    "level_column": {"type": "string", "default": "level"},
                    "columns": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string", "description": "英文列名（snake_case），用于公式 @引用"},
                                "sql_type": {"type": "string", "default": "REAL"},
                                "display_name": {"type": "string", "description": "中文列名，用于表头展示，如「攻击力」"},
                                "formula_string": {"type": "string"},
                                "number_format": {"type": "string", "description": "数值格式: '0'整数 / '0.00'2位小数 / '0.00%'百分比 / '#,##0'千分位 / '@'字符串"},
                            },
                            "required": ["name"],
                        },
                    },
                    "readme": {"type": "string", "default": ""},
                    "purpose": {"type": "string", "default": ""},
                },
                "required": ["table_name", "max_level", "columns"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_default_system_rules",
            "description": "读取文档 02 默认系统细则（全局可机读子集）",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "glossary_register",
            "description": "注册或更新一个术语（中英对照）。term_en 必须 snake_case；term_zh 必须中文；scope_table 可空表示全局。",
            "parameters": {
                "type": "object",
                "properties": {
                    "term_en": {"type": "string", "description": "英文 snake_case 名"},
                    "term_zh": {"type": "string", "description": "中文展示名"},
                    "kind": {"type": "string", "enum": ["noun", "metric", "system", "resource", "stat"], "default": "noun"},
                    "brief": {"type": "string"},
                    "scope_table": {"type": "string", "description": "可选，限定该术语只用于某张表"},
                },
                "required": ["term_en", "term_zh"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "glossary_lookup",
            "description": "按 term_en 或 term_zh 查询术语（任一条件即可，term_en 优先）",
            "parameters": {
                "type": "object",
                "properties": {
                    "term_en": {"type": "string"},
                    "term_zh": {"type": "string"},
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "glossary_list",
            "description": (
                "列出所有术语（返回紧凑行列格式：cols + rows）。"
                "可按 scope_table 或 kind 过滤；支持 limit/offset 分页（默认最多 500 条）。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "scope_table": {"type": "string", "description": "只返回该表（及全局）的术语"},
                    "kind_filter": {"type": "string", "description": "按 kind 过滤，如 stat/noun/verb"},
                    "limit": {"type": "integer", "description": "每页条数，默认 500，0=不限"},
                    "offset": {"type": "integer", "description": "分页偏移，默认 0"},
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "const_register",
            "description": (
                "注册项目常数（用于公式中的 ${name} 替换；同名 upsert）。"
                "value 必须为 number 或可转 number 的字符串。"
                "★ tags 必填且至少 1 个：用于在前端常量页按『主系统/分类』聚合展示，"
                "可使用 const_tag_register 预先创建标签；通常至少包含所属主系统名。"
                "★ brief 是对常数的描述性介绍（含义/单位/用途），应以自然语言说明，不应出现具体数值（数值由 value 承载）。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "name_en": {"type": "string"},
                    "name_zh": {"type": "string"},
                    "value": {"type": ["number", "string"]},
                    "brief": {
                        "type": "string",
                        "description": "语义描述，禁止出现具体数值（如 '10'、'0.5'）",
                    },
                    "scope_table": {"type": "string"},
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                        "description": "至少 1 个分类标签（如主系统名 'combat'/'economy'）",
                    },
                },
                "required": ["name_en", "value", "tags"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "const_tag_register",
            "description": (
                "注册常数分类标签（如主系统名 combat / economy / level_curve），"
                "用于 const_register.tags 取值与前端常量页聚合。同名 upsert。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "parent": {"type": "string", "description": "父标签（可选，构成层级）"},
                    "brief": {"type": "string"},
                },
                "required": ["name"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "const_tag_list",
            "description": "列出所有已注册的常数标签（cols+rows 紧凑格式）",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "const_set",
            "description": "更新已存在常数的 value（不存在则报 error）",
            "parameters": {
                "type": "object",
                "properties": {
                    "name_en": {"type": "string"},
                    "value": {"type": ["number", "string"]},
                },
                "required": ["name_en", "value"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "const_list",
            "description": (
                "列出所有常数（返回紧凑行列格式：cols + rows）。"
                "可按 scope_table 或 tags_filter 过滤；支持 limit/offset 分页（默认最多 500 条）。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "scope_table": {"type": "string", "description": "只返回该表（及全局）的常数"},
                    "tags_filter": {
                        "oneOf": [
                            {"type": "array", "items": {"type": "string"}},
                            {"type": "string"},
                        ],
                        "description": "按标签过滤（任意匹配），如 ['combat'] 或 'combat,economy'",
                    },
                    "limit": {"type": "integer", "description": "每页条数，默认 500，0=不限"},
                    "offset": {"type": "integer", "description": "分页偏移，默认 0"},
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "const_delete",
            "description": "删除常数（按 name_en）",
            "parameters": {
                "type": "object",
                "properties": {"name_en": {"type": "string"}},
                "required": ["name_en"],
                "additionalProperties": False,
            },
        },
    },
    # ─── 第3轮新增：表目录管理 ────────────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_directories",
            "description": "按目录列出所有表（目录树视图）。每个表都应该归属一个 directory（如 '落地表/装备'、'养成资源'）。",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "set_table_directory",
            "description": "为已存在的表设置目录路径（如 '落地表/装备'）。新建表时也应在 create_table/create_matrix_table 时直接传 directory。",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "directory": {"type": "string", "description": "目录路径，'/' 分隔，如 '落地表/装备'"},
                },
                "required": ["table_name", "directory"],
                "additionalProperties": False,
            },
        },
    },
    # ─── 第3轮新增：Matrix（行/列双向语义）表 ──────────────────────────
    {
        "type": "function",
        "function": {
            "name": "create_matrix_table",
            "description": (
                "创建『行/列双向语义』分配表。\n"
                "用途：分配方案表（行=玩法/子系统，列=属性 或 资源，交叉=投放比例/权重）。\n"
                "kind=matrix_attr：玩法×属性 投放比例；kind=matrix_resource：玩法×资源 分配比例。\n"
                "rows/cols 每项为 {key:'装备_基础', display_name:'装备·基础', brief:''}；\n"
                "【重要】scale_mode 决定 level 维的处理策略：\n"
                "  - 'none'（默认 matrix_attr）：无等级维，2D 表，调用时忽略 level 参数，无需填 levels。\n"
                "  - 'fallback'（默认 matrix_resource）：第三维轴值（如 level）允许手填，但限制的是内容。\n"
                "     若第三维切片数只有 1，可写常量；若切片数 > 1，则整表内容必须改为 formula。\n"
                "     call_calculator 会优先按公式计算 level 切片；仅在单切片常量模式下才会回退基准值。\n"
                "  - 'static'：仅保留给历史非 matrix_resource 场景，matrix_resource 禁用。\n"
                "建表后会自动注册一个名为 <table>_lookup 的 calculator，供后续 call_calculator 查询。\n"
                "【缺省值】default_value（推荐 0）：未显式写入的单元格调用 call_calculator 时返回该值。"
                "agent 只需写入非零（非缺省）的单元格，稀疏矩阵场景下可大幅减少写入量。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "英文 snake_case"},
                    "display_name": {"type": "string", "description": "中文表名"},
                    "kind": {"type": "string", "enum": ["matrix_attr", "matrix_resource"]},
                    "directory": {"type": "string", "description": "目录路径（必填，如 '分配表'）"},
                    "scale_mode": {
                        "type": "string",
                        "enum": ["none", "fallback", "static"],
                        "description": "等级维策略：none=无等级；fallback=matrix_resource 的单切片常量/多切片公式模式；static=历史全量预存（matrix_resource 禁用）",
                    },
                    "rows": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "key": {"type": "string"},
                                "display_name": {"type": "string"},
                                "brief": {"type": "string"},
                            },
                            "required": ["key", "display_name"],
                        },
                    },
                    "cols": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "key": {"type": "string"},
                                "display_name": {"type": "string"},
                                "brief": {"type": "string"},
                            },
                            "required": ["key", "display_name"],
                        },
                    },
                    "levels": {"type": "array", "items": {"type": "integer"}, "description": "第三维轴值本身可手填；这里只给历史 static 场景保留，matrix_resource 不建议再用"},
                    "value_dtype": {"type": "string", "enum": ["float", "percent", "int"], "default": "float"},
                    "value_format": {"type": "string", "default": "0.00%"},
                    "readme": {"type": "string", "default": ""},
                    "purpose": {"type": "string", "default": ""},
                    "readme": {"type": "string", "default": ""},
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "表的标签列表（至少1个），用于相关常数筛选。",
                        "minItems": 1,
                    },
                    "default_value": {
                        "type": "number",
                        "default": 0,
                        "description": "未显式写入的单元格的缺省返回值。分配表（matrix_attr/matrix_resource）强烈建议填 0：这样 agent 只需写非零单元格，空单元格查询自动返回 0 而非 null。",
                    },
                },
                "required": ["table_name", "display_name", "kind", "directory", "rows", "cols"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_matrix_cells",
            "description": (
                "向 matrix 表批量写入交叉点值。每项 {row, col, level (可空), value, note, formula}。一次 ≤200 条。\n"
                "scale_mode='none' 时 level 字段自动忽略（存 NULL）；\n"
                "matrix_resource + scale_mode='fallback' 时：第三维轴值可手填；但若出现多个第三维切片，则整表内容必须全用 formula，不能混写常量。\n"
                "【稀疏写入】若建表时设置了 default_value（通常为 0），则只需写入非缺省值的单元格；"
                "未写入的单元格通过 call_calculator 查询时自动返回 default_value，无需显式写 0。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "cells": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "row": {"type": "string"},
                                "col": {"type": "string"},
                                "level": {"type": "integer"},
                                "value": {"type": "number"},
                                "note": {"type": "string"},
                                "formula": {"type": "string", "description": "仅 matrix_resource 使用；当第三维切片数 > 1 时必须使用。支持参数公式与 piecewise/ifs 分段公式"},
                            },
                            "required": ["row", "col"],
                        },
                    },
                },
                "required": ["table_name", "cells"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_matrix",
            "description": "以宽表形式读取 matrix。可按 level / 行子集 / 列子集 切片。",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "level": {"type": "integer"},
                    "rows": {"type": "array", "items": {"type": "string"}},
                    "cols": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["table_name"],
            },
        },
    },
    # ─── 第3轮新增：Calculator 注册（fun(level, gameplay, attr) 风格查询）────
    {
        "type": "function",
        "function": {
            "name": "register_calculator",
            "description": (
                "把一张 matrix 表（或普通表）注册为可被查询的 calculator。"
                "axes 描述形参 → 数据库列的映射。brief 必填，至少 8 字符，必须说明用途与入参语义。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "calculator 名称（snake_case）"},
                    "kind": {"type": "string", "enum": ["matrix_attr", "matrix_resource", "lookup"]},
                    "table_name": {"type": "string"},
                    "axes": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string", "description": "形参名（call 时用）"},
                                "source": {"type": "string", "description": "对应数据库列名"},
                            },
                            "required": ["name", "source"],
                        },
                    },
                    "value_column": {"type": "string", "default": "value"},
                    "brief": {"type": "string", "description": "用途说明，必填，≥8 字符"},
                    "grain": {"type": "string", "description": "可选：matrix_resource 时的粒度（hourly/per_level/cumulative）"},
                },
                "required": ["name", "kind", "table_name", "axes", "brief"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_calculators",
            "description": "列出所有已注册 calculator（cols+rows 紧凑格式，含 brief 说明，便于 AI 自检与下游引用）",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "call_calculator",
            "description": "调用已注册的 calculator。kwargs 为入参字典（与 axes.name 对应）。",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "kwargs": {"type": "object"},
                },
                "required": ["name", "kwargs"],
            },
        },
    },
    # ─── 第3轮新增：子系统参数暴露 ──────────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "expose_param_to_subsystems",
            "description": (
                "向下游或兄弟步骤暴露关键数值参数，接收方步骤的设计提示词会自动注入这些参数。\n\n"
                "【使用流程】\n"
                "1. 先调用 get_gameplay_table_list 获取所有已注册玩法表 ID，从中选择目标步骤 ID。\n"
                "2. 指定 target_step：\n"
                "   - 单个目标：'gameplay_table.<table_id>'（例如 'gameplay_table.equip_enhance'）\n"
                "   - 广播全部玩法表：'subsystems:gameplay_table'（所有 gameplay_table.* 步骤均可见）\n"
                "   - 广播养成系统：'subsystems:cultivation_allocation' 等常规步骤 ID 也可使用前缀广播\n"
                "3. 参数创建后 status='pending'；接收方步骤调用 list_exposed_params 后自动标记为 acknowledged；\n"
                "   接收方步骤标记为 已完成 后自动标记为 acted_on。\n\n"
                "示例：equip_base 步骤暴露 equip_max_atk=1200 给 equip_enhance 步骤，后者设计时自动看到此值。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "owner_step": {"type": "string", "description": "暴露源的步骤 ID（本步骤 ID）"},
                    "target_step": {
                        "type": "string",
                        "description": (
                            "接收方步骤 ID。\n"
                            "可选格式：\n"
                            "- 'gameplay_table.<table_id>'：指定某个玩法表步骤\n"
                            "- 'subsystems:gameplay_table'：广播给所有 gameplay_table.* 步骤\n"
                            "- 'subsystems:<步骤前缀>'：广播给该前缀下的所有步骤\n"
                            "- 普通步骤 ID（如 'cultivation_allocation'）：精确指向\n"
                            "建议先用 get_gameplay_table_list 确认有效的 table_id 列表"
                        ),
                    },
                    "key": {"type": "string", "description": "参数键名（snake_case 英文）"},
                    "value": {"description": "参数值（数值、字符串均可）"},
                    "brief": {"type": "string", "description": "参数说明（接收方 AI 会看到此说明，需清晰描述含义和单位）"},
                },
                "required": ["owner_step", "target_step", "key", "value", "brief"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_exposed_params",
            "description": (
                "列出针对某个步骤的所有上游暴露参数，调用后自动将参数状态标记为 acknowledged（已读）。\n"
                "在本步骤设计开始前调用，确认上游是否有关键约束参数需要遵守。\n"
                "返回字段：owner_step / key / value / brief / status（pending=未读 / acknowledged=已读 / acted_on=已落地）"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "target_step": {"type": "string", "description": "当前步骤 ID（即本步骤 ID）"},
                },
                "required": ["target_step"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "request_table_revision",
            "description": (
                "对已完成的玩法表发起二次修订请求。\n"
                "调用后目标表状态重置为 '待修订'，修订请求入队，下一轮 gameplay_table agent 循环时会自动看到并酌情处理。\n"
                "适用场景：本步骤完成时发现另一个已完成的玩法表的数值需要调整（如依赖参数变化、数值平衡偏差等）。\n"
                "注意：仅能对已注册的玩法表发起修订（先用 get_gameplay_table_list 确认 table_id）。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "需要修订的玩法表 table_id（从 get_gameplay_table_list 获取）",
                    },
                    "reason": {
                        "type": "string",
                        "description": "修订原因，说明为什么已完成的表需要修改（需具体，如：equip_enhance 基础值调整后旧装备属性上限需重新计算）",
                    },
                    "requested_by_step": {
                        "type": "string",
                        "description": "发起修订的步骤 ID（即本步骤 ID）",
                    },
                },
                "required": ["table_id", "reason"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "sparse_sample",
            "description": (
                "从表中均匀采样 N 行，用于在不读取全表的情况下直观检查曲线形态（如减伤曲线、属性膨胀曲线）。"
                "按 level 列（或 row_id）升序排列后，等间距抽取 N 行，返回指定列的值（cols+rows 紧凑格式）。"
                "典型用途：设计防御 K 值后采样减伤曲线验证，或检查 HP/ATK 膨胀趋势。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "目标表名"},
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "要采样的列名列表（建议包含 level 列，方便阅读）",
                    },
                    "n": {
                        "type": "integer",
                        "description": "采样行数，默认 20，最大 100",
                        "default": 20,
                    },
                    "order_by": {
                        "type": "string",
                        "description": "排序列，默认 level（不存在则回退 row_id）",
                        "default": "level",
                    },
                },
                "required": ["table_name", "columns"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_3d_table",
            "description": (
                "创建三维数据表：行同时包含两个维度（如 等级 × 宝石类型），列是属性。\n"
                "典型场景：宝石属性表（dim1=等级1~30, dim2=宝石类型, cols=atk_bonus/def_bonus/...）。\n"
                "系统自动预插所有 (dim1 × dim2) 组合行（row_id='{d1}_{d2}'）；\n"
                "dim1/dim2 的轴值本身可以手填（例如等级 1..30、宝石类型 atk/def）。\n"
                "属性列只支持数值列，可设置 formula（支持 @dim1列名、@dim2列名 以及同行 @其他列）。\n"
                "若公式只依赖维度列/同行列，系统会自动计算全表；若含未注册 ${常量}，会先保存为运行时模板，"
                "需先 const_register 再 recalculate_table/重算。\n"
                "前端使用三轴查看器：可自由选择行轴、列轴，并固定剩余第三维切片。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "英文 snake_case 表名"},
                    "display_name": {"type": "string", "description": "中文显示名，必填"},
                    "dim1": {
                        "type": "object",
                        "description": "第一维度（行维度1，通常是等级）",
                        "properties": {
                            "col_name": {"type": "string", "description": "维度列名（英文，如 level）"},
                            "display_name": {"type": "string", "description": "维度中文名（如 等级）"},
                            "keys": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "key": {"type": "string", "description": "维度值（如 '1','2','30'，数字字符串会自动转为 INTEGER）"},
                                        "display_name": {"type": "string"},
                                    },
                                    "required": ["key", "display_name"],
                                },
                                "description": "所有维度值，顺序即行顺序",
                            },
                        },
                        "required": ["col_name", "display_name", "keys"],
                    },
                    "dim2": {
                        "type": "object",
                        "description": "第二维度（行维度2，通常是分类，如宝石类型）",
                        "properties": {
                            "col_name": {"type": "string"},
                            "display_name": {"type": "string"},
                            "keys": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "key": {"type": "string"},
                                        "display_name": {"type": "string"},
                                    },
                                    "required": ["key", "display_name"],
                                },
                            },
                        },
                        "required": ["col_name", "display_name", "keys"],
                    },
                    "cols": {
                        "type": "array",
                        "description": "属性列定义，每列可附加 formula 公式",
                        "items": {
                            "type": "object",
                            "properties": {
                                "key": {"type": "string", "description": "列英文名"},
                                "display_name": {"type": "string", "description": "列中文名，必填"},
                                "dtype": {"type": "string", "enum": ["float", "int", "percent"], "default": "float"},
                                "number_format": {"type": "string", "default": "0.00"},
                                "formula": {
                                    "type": "string",
                                        "description": (
                                            "可选，同行公式（支持 @列名 引用本表其他列，含维度列）。\n"
                                            "示例：@level * ${gem_base_atk} * 0.01\n"
                                            "若 ${常量} 已注册会立即计算；未注册则保留为运行时模板，后续可重算。"
                                        ),
                                    },
                                },
                            "required": ["key", "display_name"],
                        },
                        "minItems": 1,
                    },
                    "readme": {"type": "string", "default": ""},
                    "purpose": {"type": "string", "default": ""},
                    "directory": {"type": "string", "description": "目录路径（如 '落地表/宝石'）", "default": ""},
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "表标签（至少1个），用于相关常数筛选",
                        "minItems": 1,
                    },
                },
                "required": ["table_name", "display_name", "dim1", "dim2", "cols", "directory", "tags"],
            },
        },
    },
    # ─── 玩法规划：玩法表注册工具 ────────────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "register_gameplay_table",
            "description": (
                "在'玩法规划'步骤中注册一个玩法落地表到规划清单。\n"
                "每个表仅注册一次（同名 upsert），注册后状态自动为'未开始'。\n"
                "table_id 必须是英文 snake_case；display_name 必须是中文。\n"
                "readme 需说明该表的玩法设计目标、关键列、与其他表的依赖关系。\n"
                "order_num 决定执行顺序（越小越优先），dependencies 列出需要先完成的其他 table_id。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string", "description": "英文 snake_case 标识符，如 equip_enhance"},
                    "display_name": {"type": "string", "description": "中文展示名，如 「装备强化落地表」"},
                    "readme": {"type": "string", "description": "该玩法表的设计说明（玩法目标/关键列/依赖关系/验收标准），至少 50 字"},
                    "order_num": {"type": "integer", "description": "推荐执行顺序编号（1开始，越小越优先）"},
                    "dependencies": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "依赖的其他 table_id 列表（需在本表之前完成），如 ['equip_base']",
                        "default": [],
                    },
                },
                "required": ["table_id", "display_name", "readme", "order_num"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_gameplay_table_list",
            "description": "读取所有已注册的玩法落地表及其当前状态（未开始/进行中/已完成）、设计说明和依赖关系。",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "set_gameplay_table_status",
            "description": (
                "更新玩法落地表的执行状态。\n"
                "在开始设计某个玩法表前调用（状态=进行中），完成后调用（状态=已完成）。\n"
                "适用场景：\n"
                "- 正常处理：未开始 → 进行中 → 已完成\n"
                "- 修订处理：待修订 → 进行中 → 已完成（完成时自动将对应修订请求标记为 done）"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string", "description": "玩法表的英文标识符"},
                    "status": {"type": "string", "enum": ["进行中", "已完成"], "description": "新状态"},
                },
                "required": ["table_id", "status"],
                "additionalProperties": False,
            },
        },
    },
]


_TOOL_GROUP_META: Dict[str, Dict[str, Any]] = {
    "read_core": {"label": "读取：项目与表", "order": 10, "hint": "查看项目配置、表结构、表数据和依赖关系。"},
    "read_assets": {"label": "读取：技能与资产", "order": 20, "hint": "查看项目里的 SKILL、README 和其他只读资产。"},
    "write_core": {"label": "写入：文档与业务表", "order": 30, "hint": "直接改 README、项目配置或业务表内容。"},
    "compute_formula": {"label": "计算：公式与批量生成", "order": 40, "hint": "批量填表、注册公式并触发重算。"},
    "validation_snapshot": {"label": "校验：验证与快照", "order": 50, "hint": "检查规则、做快照、比对改动和平衡结果。"},
    "meta_dictionary": {"label": "元数据：术语、常数与目录", "order": 60, "hint": "维护术语表、常数、标签和目录结构。"},
    "advanced_modeling": {"label": "高级建模：矩阵、三维与计算器", "order": 70, "hint": "处理矩阵表、三维表、计算器和跨系统参数暴露。"},
}

_TOOL_TITLE_ZH: Dict[str, str] = {
    "get_project_config": "读取项目配置",
    "get_table_list": "列出业务表",
    "get_table_schema": "查看表结构",
    "read_table": "读取表数据",
    "read_cell": "读取单元格",
    "get_protected_cells": "查看保护单元格",
    "get_dependency_graph": "查看依赖关系",
    "get_table_readme": "读取表 README",
    "read_3d_table": "读取三维表切片",
    "read_3d_table_full": "读取完整三维表",
    "list_skills": "列出可用 SKILL",
    "get_skill_detail": "查看 SKILL 详情",
    "render_skill_file": "预览 SKILL 文件",
    "update_table_readme": "更新表 README",
    "update_global_readme": "更新全局 README",
    "set_project_setting": "设置项目参数",
    "create_table": "创建业务表",
    "write_cells": "批量写单元格",
    "write_cells_series": "按序列批量写入",
    "register_formula": "注册列公式",
    "execute_formula": "执行列公式",
    "recalculate_downstream": "重算下游公式",
    "get_algorithm_api_list": "列出算法接口",
    "call_algorithm_api": "调用算法接口",
    "run_validation": "运行校验",
    "confirm_validation_rule": "确认校验规则",
    "delete_table": "删除业务表",
    "create_snapshot": "创建快照",
    "list_snapshots": "列出快照",
    "compare_snapshot": "对比快照",
    "run_balance_check": "运行平衡检查",
    "get_validation_history": "查看校验历史",
    "bulk_register_and_compute": "批量注册并计算公式",
    "setup_level_table": "一键生成等级表",
    "get_default_system_rules": "读取默认系统规则",
    "glossary_register": "登记术语",
    "glossary_lookup": "查询术语",
    "glossary_list": "列出术语",
    "const_register": "登记常数",
    "const_tag_register": "登记常数标签",
    "const_tag_list": "列出常数标签",
    "const_set": "修改常数值",
    "const_list": "列出常数",
    "const_delete": "删除常数",
    "list_directories": "查看目录树",
    "set_table_directory": "设置表目录",
    "create_matrix_table": "创建矩阵表",
    "write_matrix_cells": "写入矩阵单元格",
    "read_matrix": "读取矩阵表",
    "register_calculator": "注册计算器",
    "list_calculators": "列出计算器",
    "call_calculator": "调用计算器",
    "expose_param_to_subsystems": "暴露参数给子系统",
    "list_exposed_params": "列出已暴露参数",
    "sparse_sample": "均匀采样表数据",
    "create_3d_table": "创建三维数据表",
    "register_gameplay_table": "注册玩法落地表",
    "get_gameplay_table_list": "读取玩法表清单",
    "set_gameplay_table_status": "更新玩法表状态",
}

_TOOL_GROUP_BY_NAME: Dict[str, str] = {
    "get_project_config": "read_core",
    "get_table_list": "read_core",
    "get_table_schema": "read_core",
    "read_table": "read_core",
    "read_cell": "read_core",
    "get_protected_cells": "read_core",
    "get_dependency_graph": "read_core",
    "get_table_readme": "read_assets",
    "list_skills": "read_assets",
    "get_skill_detail": "read_assets",
    "render_skill_file": "read_assets",
    "update_table_readme": "write_core",
    "update_global_readme": "write_core",
    "set_project_setting": "write_core",
    "create_table": "write_core",
    "write_cells": "write_core",
    "write_cells_series": "write_core",
    "delete_table": "write_core",
    "register_formula": "compute_formula",
    "execute_formula": "compute_formula",
    "recalculate_downstream": "compute_formula",
    "get_algorithm_api_list": "compute_formula",
    "call_algorithm_api": "compute_formula",
    "bulk_register_and_compute": "compute_formula",
    "setup_level_table": "compute_formula",
    "run_validation": "validation_snapshot",
    "confirm_validation_rule": "validation_snapshot",
    "create_snapshot": "validation_snapshot",
    "list_snapshots": "validation_snapshot",
    "compare_snapshot": "validation_snapshot",
    "run_balance_check": "validation_snapshot",
    "get_validation_history": "validation_snapshot",
    "get_default_system_rules": "meta_dictionary",
    "glossary_register": "meta_dictionary",
    "glossary_lookup": "meta_dictionary",
    "glossary_list": "meta_dictionary",
    "const_register": "meta_dictionary",
    "const_tag_register": "meta_dictionary",
    "const_tag_list": "meta_dictionary",
    "const_set": "meta_dictionary",
    "const_list": "meta_dictionary",
    "const_delete": "meta_dictionary",
    "list_directories": "meta_dictionary",
    "set_table_directory": "meta_dictionary",
    "read_3d_table": "advanced_modeling",
    "read_3d_table_full": "advanced_modeling",
    "create_matrix_table": "advanced_modeling",
    "write_matrix_cells": "advanced_modeling",
    "read_matrix": "advanced_modeling",
    "register_calculator": "advanced_modeling",
    "list_calculators": "advanced_modeling",
    "call_calculator": "advanced_modeling",
    "expose_param_to_subsystems": "advanced_modeling",
    "list_exposed_params": "advanced_modeling",
    "sparse_sample": "advanced_modeling",
    "create_3d_table": "advanced_modeling",
    "register_gameplay_table": "advanced_modeling",
    "get_gameplay_table_list": "advanced_modeling",
    "set_gameplay_table_status": "advanced_modeling",
}


_TOOL_SUMMARY_ZH: Dict[str, str] = {
    "get_project_config": "获取当前项目的名称、类型等基础配置信息。",
    "get_table_list": "列出当前项目下所有已创建的业务数据表（cols+rows 紧凑格式）。",
    "get_table_schema": "查看指定表的列定义、数据类型和约束信息。",
    "read_table": "按行列范围读取业务表的实际数据内容（cols+rows 紧凑格式，支持 columns/filters/level_range 过滤）。",
    "read_cell": "精确读取表中单个单元格的值。",
    "get_protected_cells": "查看表中标记为写保护的单元格列表。",
    "get_dependency_graph": "获取各表与公式之间的依赖关系图谱（cols+rows 紧凑格式）。",
    "get_table_readme": "读取指定业务表的 README 说明文档。",
    "read_3d_table": "按指定维度切片读取三维数据表的一部分。",
    "read_3d_table_full": "读取三维数据表的完整结构与数据。",
    "list_skills": "列出当前项目所有可用的 SKILL 技能模板。",
    "get_skill_detail": "查看指定 SKILL 的详细配置和触发条件。",
    "render_skill_file": "预览 SKILL 文件编译后的实际可用内容。",
    "update_table_readme": "更新或创建指定业务表的 README 说明文档。",
    "update_global_readme": "更新项目级全局 README 说明文档。",
    "set_project_setting": "修改项目的全局设置参数（如版本、规则等）。",
    "create_table": "在当前项目中新建一个业务数据表。",
    "write_cells": "批量向指定行列位置写入单元格数据。",
    "write_cells_series": "按列序列规则批量写入一组单元格数据。",
    "register_formula": "为某列注册计算公式，供后续触发重算使用。",
    "execute_formula": "立即执行指定列的注册公式并更新数据。",
    "recalculate_downstream": "重算依赖于某列的所有下游公式列。",
    "get_algorithm_api_list": "列出当前项目可调用的外部算法接口清单。",
    "call_algorithm_api": "调用指定算法接口并获取计算结果。",
    "run_validation": "对指定表运行数值校验规则并返回违规结果。",
    "confirm_validation_rule": "确认并保存一条数值校验规则。",
    "delete_table": "删除指定的业务数据表（不可恢复）。",
    "create_snapshot": "为当前项目创建一个数据快照版本以便回溯。",
    "list_snapshots": "列出当前项目的所有历史数据快照（cols+rows 紧凑格式）。",
    "compare_snapshot": "对比两个快照版本之间的数据差异。",
    "run_balance_check": "执行游戏数值平衡检查并生成分析报告。",
    "get_validation_history": "查看历史校验任务的执行记录与结果。",
    "bulk_register_and_compute": "批量注册多列公式并一次性触发全部计算。",
    "setup_level_table": "根据配置参数一键生成等级成长数据表。",
    "get_default_system_rules": "读取系统内置的默认校验规则配置。",
    "glossary_register": "向术语表中登记一个新的游戏术语及其解释。",
    "glossary_lookup": "在术语表中查询指定术语的中文定义。",
    "glossary_list": "列出术语表中所有已登记的术语条目（cols+rows 紧凑格式，支持 kind_filter 过滤和 limit/offset 分页）。",
    "const_register": "在常数表中登记一个新的数值常量。",
    "const_tag_register": "为常数创建或登记一个分类标签。",
    "const_tag_list": "列出所有已定义的常数分类标签（cols+rows 紧凑格式）。",
    "const_set": "修改已登记常数的数值。",
    "const_list": "列出所有已登记的常数（cols+rows 紧凑格式，支持 tags_filter 过滤和 limit/offset 分页）。",
    "const_delete": "删除一个已登记的常数条目。",
    "list_directories": "查看项目业务表的目录树结构。",
    "set_table_directory": "将指定表归入某个目录分类节点。",
    "create_matrix_table": "新建一个矩阵式二维数据表。",
    "write_matrix_cells": "向矩阵表中指定行列位置写入数据。",
    "read_matrix": "读取矩阵表中的数据内容。",
    "register_calculator": "注册一个数值计算器配置供后续调用。",
    "list_calculators": "列出所有已注册的计算器及其配置（cols+rows 紧凑格式）。",
    "call_calculator": "调用指定计算器执行数值计算并返回结果。",
    "expose_param_to_subsystems": "将指定项目参数暴露给关联子系统使用。",
    "list_exposed_params": "列出当前已暴露给子系统的参数清单。",
    "sparse_sample": "对大型表进行均匀采样，获取代表性数据子集（cols+rows 紧凑格式）。",
    "create_3d_table": "新建一个支持多维度切片的三维数据表。",
    "register_gameplay_table": "在玩法规划步骤中将一个玩法落地表注册到规划清单。",
    "get_gameplay_table_list": "列出所有已注册的玩法落地表及其执行状态。",
    "set_gameplay_table_status": "将玩法落地表的状态更新为进行中或已完成。",
}


def _tool_display_meta(name: str, desc: str) -> Dict[str, Any]:
    group_key = _TOOL_GROUP_BY_NAME.get(name, "read_core")
    group_meta = _TOOL_GROUP_META[group_key]
    return {
        "tool_group_key": group_key,
        "tool_group_label": str(group_meta["label"]),
        "tool_group_order": int(group_meta["order"]),
        "tool_group_hint": str(group_meta["hint"]),
        "tool_name_zh": _TOOL_TITLE_ZH.get(name, name),
        "tool_summary_zh": _TOOL_SUMMARY_ZH.get(name, (desc or "").strip()[:100]),
    }


def _tool_reference_note(name: str) -> str:
    return (
        f"该提示词来自工具 `{name}` 的 function schema。agent_runner 在向模型暴露可用工具时，"
        f"会把这里的函数说明与参数说明一起发送给模型；修改后会直接影响 AI 何时选择 `{name}`、"
        "以及它如何组织参数。"
    )


def _schema_module_title(path: str) -> str:
    if path == "function.description":
        return "函数说明"
    return f"说明：{path}"


def _collect_schema_description_modules(
    node: Any,
    *,
    path: str,
    out: List[Dict[str, Any]],
) -> None:
    if isinstance(node, dict):
        desc = node.get("description")
        if isinstance(desc, str) and desc.strip():
            module_path = f"{path}.description" if path else "description"
            out.append(
                {
                    "module_key": module_path,
                    "title": _schema_module_title(module_path),
                    "content": desc,
                    "required": True,
                    "enabled": True,
                    "sort_order": len(out) + 1,
                }
            )
        for key, value in node.items():
            if isinstance(value, dict):
                child_path = f"{path}.{key}" if path else key
                _collect_schema_description_modules(value, path=child_path, out=out)


def _tool_prompt_default_item(tool: Dict[str, Any], display_order: int) -> Dict[str, Any]:
    fn = tool.get("function") or {}
    name = str(fn.get("name") or "")
    modules: List[Dict[str, Any]] = []
    _collect_schema_description_modules(fn, path="function", out=modules)
    desc = str(fn.get("description") or "")
    meta = _tool_display_meta(name, desc)
    return {
        "category": "tool",
        "prompt_key": name,
        "title": str(meta["tool_name_zh"]),
        "summary": str(meta["tool_summary_zh"])[:200],
        "description": desc,
        "reference_note": _tool_reference_note(name),
        "enabled": True,
        "display_order": display_order,
        "modules": modules,
        **meta,
    }


def get_tool_prompt_catalog(
    conn: Optional[sqlite3.Connection] = None,
    global_conn: Optional[sqlite3.Connection] = None,
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for idx, tool in enumerate(TOOLS_OPENAI, start=1):
        default = _tool_prompt_default_item(tool, idx)
        if conn is None and global_conn is None:
            items.append(default)
            continue
        prompt_key = str(default["prompt_key"])
        global_override = get_prompt_override(global_conn, category="tool", prompt_key=prompt_key) if global_conn is not None else None
        project_override = get_prompt_override(conn, category="tool", prompt_key=prompt_key) if conn is not None else None
        items.append(merge_prompt_item_layers(default, [global_override, project_override]))
    items.sort(key=lambda item: (int(item.get("display_order") or 0), str(item.get("prompt_key") or "")))
    return items


def _set_nested_description(target: Dict[str, Any], path: str, content: str) -> None:
    parts = path.split(".")
    cur: Any = target
    for part in parts[:-1]:
        if not isinstance(cur, dict):
            return
        cur = cur.get(part)
    if isinstance(cur, dict):
        cur[parts[-1]] = content


def build_tools_openai(
    conn: Optional[sqlite3.Connection] = None,
    global_conn: Optional[sqlite3.Connection] = None,
) -> List[Dict[str, Any]]:
    tools = copy.deepcopy(TOOLS_OPENAI)
    if conn is None and global_conn is None:
        return tools
    prompt_items = {str(item["prompt_key"]): item for item in get_tool_prompt_catalog(conn, global_conn=global_conn)}
    for tool in tools:
        fn = tool.get("function") or {}
        name = str(fn.get("name") or "")
        prompt_item = prompt_items.get(name)
        if not prompt_item:
            continue
        for module in prompt_item.get("modules") or []:
            if not (module.get("required") or module.get("enabled")):
                continue
            module_key = str(module.get("module_key") or "")
            content = str(module.get("content") or "")
            if module_key and content:
                _set_nested_description(tool, module_key, content)
    return tools


def _list_known_tables(conn: sqlite3.Connection) -> List[str]:
    """返回 _table_registry 中所有表名，用于在错误消息里给模型提示。"""
    cur = conn.execute("SELECT table_name FROM _table_registry ORDER BY table_name")
    return [r[0] for r in cur.fetchall()]


def _list_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    """返回指定表的列名列表（排除 row_id），用于列相关错误提示。"""
    try:
        cur = conn.execute(f'PRAGMA table_info("{table_name}")')
        return [r["name"] for r in cur.fetchall() if r["name"] != "row_id"]
    except Exception:  # noqa: BLE001
        return []


_PROJECT_DOC_PREVIEW_CHARS = 1600
_PROJECT_DOC_HEADING_LIMIT = 12


def _doc_excerpt(text: str) -> Dict[str, Any]:
    normalized = str(text or "")
    headings = [
        line.strip()
        for line in normalized.splitlines()
        if line.lstrip().startswith("#")
    ][:_PROJECT_DOC_HEADING_LIMIT]
    excerpt = normalized[:_PROJECT_DOC_PREVIEW_CHARS]
    out: Dict[str, Any] = {
        "excerpt": excerpt,
        "text_length": len(normalized),
        "headings": headings,
        "truncated": len(normalized) > _PROJECT_DOC_PREVIEW_CHARS,
    }
    if not excerpt and normalized:
        out["excerpt"] = normalized
    return out


def _compact_project_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    compact: Dict[str, Any] = {}
    step_readme_keys: List[str] = []

    for key, value in settings.items():
        if key.startswith("step_readme."):
            step_readme_keys.append(key.removeprefix("step_readme."))
            continue
        if key == "global_readme" and isinstance(value, dict) and isinstance(value.get("text"), str):
            compact[key] = _doc_excerpt(value.get("text") or "")
            continue
        compact[key] = value

    if step_readme_keys:
        compact["step_readmes"] = {
            "count": len(step_readme_keys),
            "steps": sorted(step_readme_keys),
        }
    return compact


def _get_project_config(conn: sqlite3.Connection) -> Dict[str, Any]:
    cur = conn.execute("SELECT key, value_json FROM project_settings")
    settings: Dict[str, Any] = {}
    for k, v in cur.fetchall():
        try:
            settings[k] = json.loads(v)
        except json.JSONDecodeError:
            settings[k] = v
    return {"settings": _compact_project_settings(settings)}


def _get_table_list(conn: sqlite3.Connection) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT table_name, layer, purpose, validation_status, "
        "COALESCE(directory,'') AS directory FROM _table_registry ORDER BY directory, table_name"
    )
    rows_dicts = [dict(r) for r in cur.fetchall()]
    if rows_dicts:
        cols = list(rows_dicts[0].keys())
        return {"cols": cols, "rows": [[r[c] for c in cols] for r in rows_dicts], "total": len(rows_dicts)}
    return {"cols": [], "rows": [], "total": 0}


def _get_table_schema(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    include_readme_excerpt: bool = True,
    include_formulas: bool = True,
) -> Dict[str, Any]:
    cur = conn.execute(
        """
        SELECT table_name, layer, purpose, readme, schema_json, validation_status,
               COALESCE(directory, '') AS directory, COALESCE(matrix_meta_json, '') AS matrix_meta_json,
               COALESCE(tags, '[]') AS tags
        FROM _table_registry
        WHERE table_name = ?
        """,
        (table_name,),
    )
    row = cur.fetchone()
    if not row:
        return {"error": f"未知表 '{table_name}'", "fix": f"用 get_table_list 确认表名，当前已注册: {_list_known_tables(conn)}"}

    try:
        schema = json.loads(row["schema_json"] or "{}")
    except json.JSONDecodeError:
        schema = {}
    try:
        matrix_meta = json.loads(row["matrix_meta_json"] or "{}")
    except json.JSONDecodeError:
        matrix_meta = {}
    try:
        tags = json.loads(row["tags"] or "[]")
    except json.JSONDecodeError:
        tags = []

    columns = schema.get("columns") if isinstance(schema, dict) else []
    if not isinstance(columns, list):
        columns = []

    out: Dict[str, Any] = {
        "table_name": row["table_name"],
        "display_name": (schema.get("display_name") if isinstance(schema, dict) else "") or "",
        "layer": row["layer"],
        "purpose": row["purpose"] or "",
        "validation_status": row["validation_status"] or "",
        "directory": row["directory"] or "",
        "tags": tags if isinstance(tags, list) else [],
        "column_count": len(columns),
        "columns": columns,
    }
    if include_readme_excerpt:
        out["readme_excerpt"] = _doc_excerpt(str(row["readme"] or ""))
    if isinstance(matrix_meta, dict) and matrix_meta:
        out["matrix_meta"] = matrix_meta
        out["matrix_kind"] = str(matrix_meta.get("kind") or "")
    if include_formulas:
        cur = conn.execute(
            """
            SELECT column_name, formula, COALESCE(formula_type, 'sql') AS formula_type
            FROM _formula_registry
            WHERE table_name = ?
            ORDER BY column_name
            """,
            (table_name,),
        )
        out["formulas"] = [
            {
                "column_name": str(rec["column_name"]),
                "formula": str(rec["formula"]),
                "formula_type": str(rec["formula_type"]),
            }
            for rec in cur.fetchall()
        ]
    return out


def _list_directories(conn: sqlite3.Connection) -> Dict[str, Any]:
    """目录树聚合视图：按 directory 字段分组所有表。"""
    cur = conn.execute(
        "SELECT COALESCE(directory,'') AS directory, table_name, layer, validation_status "
        "FROM _table_registry ORDER BY directory, table_name"
    )
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in cur.fetchall():
        d = r["directory"] or "(根目录)"
        groups.setdefault(d, []).append({
            "table_name": r["table_name"],
            "layer": r["layer"],
            "validation_status": r["validation_status"],
        })
    return {"directories": [{"path": k, "tables": v} for k, v in groups.items()]}


def _set_table_directory(conn: sqlite3.Connection, table_name: str, directory: str) -> Dict[str, Any]:
    cur = conn.execute("SELECT 1 FROM _table_registry WHERE table_name=?", (table_name,))
    if not cur.fetchone():
        return {"ok": False, "error": f"未知表 {table_name}"}
    conn.execute("UPDATE _table_registry SET directory=? WHERE table_name=?", (directory or "", table_name))
    conn.commit()
    return {"ok": True, "table_name": table_name, "directory": directory or ""}


def _provenance_stats(
    conn: sqlite3.Connection,
    table_name: str,
    row_ids: List[str],
    limit_rows: int,
) -> Dict[str, Dict[str, int]]:
    if not row_ids:
        return {}
    stats: Dict[str, Dict[str, int]] = {}
    chunk_size = 400
    for i in range(0, min(len(row_ids), limit_rows), chunk_size):
        chunk = row_ids[i : i + chunk_size]
        ph = ",".join("?" * len(chunk))
        cur = conn.execute(
            f"""
            SELECT column_name, source_tag, COUNT(*) AS n
            FROM _cell_provenance
            WHERE table_name = ? AND row_id IN ({ph})
            GROUP BY column_name, source_tag
            """,
            (table_name, *chunk),
        )
        for r in cur.fetchall():
            col = str(r["column_name"])
            tag = str(r["source_tag"])
            n = int(r["n"])
            stats.setdefault(col, {})
            stats[col][tag] = stats[col].get(tag, 0) + n
    return stats


def _read_table(
    conn: sqlite3.Connection,
    table_name: str,
    limit: int = 50,
    columns: Optional[List[str]] = None,
    filters: Optional[List[Dict[str, Any]]] = None,
    level_column: Optional[str] = None,
    level_min: Optional[float] = None,
    level_max: Optional[float] = None,
    include_source_stats: bool = False,
) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    if not cur.fetchone():
        return {"error": f"未知表 '{table_name}'", "fix": f"用 get_table_list 确认表名，当前已注册: {_list_known_tables(conn)}"}
    lim = max(1, min(int(limit or 50), 200))
    try:
        t = assert_col_or_table(table_name)
    except ValueError as e:
        return {"error": str(e)}
    table_columns = set(_list_table_columns(conn, t))
    where_parts: List[str] = []
    params: List[Any] = []
    if filters:
        for f in filters:
            if not isinstance(f, dict):
                continue
            coln = str(f.get("column", "")).strip()
            if not coln:
                continue
            try:
                cq = assert_col_or_table(coln)
            except ValueError:
                known_cols = _list_table_columns(conn, table_name)
                return {"error": f"filter 列名 '{coln}' 非法（含非法字符或格式错误）", "fix": f"表 '{table_name}' 的可用列: {known_cols}"}
            where_parts.append(f'"{cq}" = ?')
            params.append(f.get("value"))
    if level_min is not None or level_max is not None:
        if level_min is None or level_max is None:
            return {
                "error": "level_range 需同时提供 level_min 与 level_max",
                "fix": "若要按等级区间读取，请同时传 level_min 和 level_max；未传 level_column 时会默认优先使用 level，否则回退 row_id",
            }
        raw_level_column = str(level_column).strip() if level_column is not None else ""
        level_col_name = raw_level_column or ("level" if "level" in table_columns else "row_id")
        if level_col_name != "row_id" and level_col_name not in table_columns:
            known_cols = sorted(table_columns)
            return {
                "error": f"等级列 '{level_col_name}' 不存在",
                "fix": f"表 '{table_name}' 的可用列: {known_cols}；若未提供 level_column，则默认优先用 level，否则回退 row_id",
            }
        try:
            lc = assert_col_or_table(level_col_name)
        except ValueError as e:
            return {"error": str(e)}
        where_parts.append(f'CAST("{lc}" AS REAL) BETWEEN ? AND ?')
        params.extend([float(level_min), float(level_max)])
    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

    if columns:
        parts = ["row_id"]
        seen = {"row_id"}
        for raw in columns:
            c = str(raw).strip()
            if not c or c in seen:
                continue
            try:
                parts.append(f'"{assert_col_or_table(c)}"')
            except ValueError as e:
                return {"error": str(e)}
            seen.add(c)
        sel = ", ".join(parts)
    else:
        sel = "*"
    sql = f'SELECT {sel} FROM "{t}"{where_sql} LIMIT ?'
    params.append(lim)
    cur = conn.execute(sql, tuple(params))
    rows_dicts = [dict(r) for r in cur.fetchall()]
    if rows_dicts:
        col_names = list(rows_dicts[0].keys())
        rows_list = [[row.get(c) for c in col_names] for row in rows_dicts]
        out: Dict[str, Any] = {"cols": col_names, "rows": rows_list, "total": len(rows_list)}
    else:
        out = {"cols": [], "rows": [], "total": 0}
    if include_source_stats and rows_dicts:
        rids = [str(r["row_id"]) for r in rows_dicts if r.get("row_id") is not None]
        out["provenance_stats"] = _provenance_stats(conn, t, rids, len(rows_dicts))
    return out


def _read_cell(conn: sqlite3.Connection, table_name: str, row_id: str, column_name: str) -> Dict[str, Any]:
    try:
        t = assert_col_or_table(table_name)
        col = assert_col_or_table(column_name)
    except ValueError as e:
        return {"error": str(e)}
    cur = conn.execute(f'SELECT "{col}" AS v FROM "{t}" WHERE row_id = ?', (str(row_id),))
    row = cur.fetchone()
    if not row:
        cur2 = conn.execute(f'SELECT MIN(row_id), MAX(row_id), COUNT(*) FROM "{t}"')
        meta = cur2.fetchone()
        return {
            "error": f"表 '{t}' 中 row_id='{row_id}' 不存在",
            "fix": f"该表共 {meta[2]} 行，row_id 范围 [{meta[0]}, {meta[1]}]，请确认 row_id 正确",
        }
    cur = conn.execute(
        """
        SELECT source_tag FROM _cell_provenance
        WHERE table_name = ? AND row_id = ? AND column_name = ?
        """,
        (t, str(row_id), col),
    )
    pr = cur.fetchone()
    src = pr["source_tag"] if pr else None
    return {"value": row["v"], "source_tag": src}


def _get_protected_cells(conn: sqlite3.Connection, table_name: str) -> Dict[str, Any]:
    try:
        t = assert_col_or_table(table_name)
    except ValueError as e:
        return {"error": str(e)}
    cur = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?",
        (t,),
    )
    if not cur.fetchone():
        return {"error": f"未知表 '{t}'", "fix": f"用 get_table_list 确认表名，当前已注册: {_list_known_tables(conn)}"}
    cur = conn.execute(
        """
        SELECT row_id, column_name FROM _cell_provenance
        WHERE table_name = ? AND source_tag = 'user_manual'
        """,
        (t,),
    )
    return {"cells": [{"row_id": r["row_id"], "column": r["column_name"]} for r in cur.fetchall()]}


def _dependency_edges(
    conn: sqlite3.Connection,
    table_name: Optional[str],
    direction: str = "full",
) -> Dict[str, Any]:
    d = (direction or "full").lower()
    if d not in ("upstream", "downstream", "full"):
        return {"error": "direction 须为 upstream / downstream / full"}
    if table_name:
        if d == "upstream":
            cur = conn.execute(
                "SELECT * FROM _dependency_graph WHERE to_table = ?",
                (table_name,),
            )
        elif d == "downstream":
            cur = conn.execute(
                "SELECT * FROM _dependency_graph WHERE from_table = ?",
                (table_name,),
            )
        else:
            cur = conn.execute(
                "SELECT * FROM _dependency_graph WHERE from_table = ? OR to_table = ?",
                (table_name, table_name),
            )
    else:
        cur = conn.execute("SELECT * FROM _dependency_graph")
    edges = [
        {
            "from_table": r["from_table"],
            "from_column": r["from_column"],
            "to_table": r["to_table"],
            "to_column": r["to_column"],
            "edge_type": r["edge_type"],
        }
        for r in cur.fetchall()
    ]
    if edges:
        cols = list(edges[0].keys())
        return {"edge_count": len(edges), "cols": cols, "rows": [[e[c] for c in cols] for e in edges]}
    return {"edge_count": 0, "cols": [], "rows": []}


def _get_table_readme(conn: sqlite3.Connection, table_name: str) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT readme FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    row = cur.fetchone()
    if not row:
        return {"error": f"未知表 '{table_name}'", "fix": f"用 get_table_list 确认表名，当前已注册: {_list_known_tables(conn)}"}
    return {"table_name": table_name, "readme": row["readme"] or ""}


_THREE_D_AXES = ("dim1", "dim2", "metric")


def _round_tool_value(value: Any) -> Any:
    if isinstance(value, bool) or value is None or isinstance(value, int) or isinstance(value, str):
        return value
    if isinstance(value, float):
        return round(value, 4)
    if isinstance(value, list):
        return [_round_tool_value(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _round_tool_value(v) for k, v in value.items()}
    return value


def _build_3d_axis_catalog(raw: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    dim1 = raw.get("dim1") or {}
    dim2 = raw.get("dim2") or {}
    cols = raw.get("cols") or []
    data = raw.get("data") or {}

    dim1_keys = [str(item.get("key")) for item in dim1.get("keys") or [] if isinstance(item, dict) and str(item.get("key") or "").strip()]
    dim2_keys = [str(item.get("key")) for item in dim2.get("keys") or [] if isinstance(item, dict) and str(item.get("key") or "").strip()]
    metric_keys = [str(item.get("key")) for item in cols if isinstance(item, dict) and str(item.get("key") or "").strip()]

    if not dim1_keys:
        dim1_keys = sorted(str(key) for key in data.keys())
    if not dim2_keys:
        dim2_seen = {str(dim2_key) for rows in data.values() if isinstance(rows, dict) for dim2_key in rows.keys()}
        dim2_keys = sorted(dim2_seen)
    if not metric_keys:
        metric_seen = {
            str(metric_key)
            for rows in data.values()
            if isinstance(rows, dict)
            for metrics in rows.values()
            if isinstance(metrics, dict)
            for metric_key in metrics.keys()
        }
        metric_keys = sorted(metric_seen)

    dim1_display = {str(item.get("key")): str(item.get("display_name") or item.get("key") or "") for item in dim1.get("keys") or [] if isinstance(item, dict)}
    dim2_display = {str(item.get("key")): str(item.get("display_name") or item.get("key") or "") for item in dim2.get("keys") or [] if isinstance(item, dict)}
    metric_display = {str(item.get("key")): str(item.get("display_name") or item.get("key") or "") for item in cols if isinstance(item, dict) and str(item.get("key") or "").strip()}

    return {
        "dim1": {
            "label": str(dim1.get("display_name") or dim1.get("col_name") or "dim1"),
            "col_name": str(dim1.get("col_name") or ""),
            "keys": dim1_keys,
            "display": dim1_display,
        },
        "dim2": {
            "label": str(dim2.get("display_name") or dim2.get("col_name") or "dim2"),
            "col_name": str(dim2.get("col_name") or ""),
            "keys": dim2_keys,
            "display": dim2_display,
        },
        "metric": {
            "label": "属性列",
            "col_name": "metric",
            "keys": metric_keys,
            "display": metric_display,
        },
    }


def _select_3d_axis_keys(
    axis: str,
    requested: Optional[List[str]],
    axis_catalog: Dict[str, Dict[str, Any]],
    *,
    limit: int,
) -> tuple[List[str], bool]:
    ordered = axis_catalog[axis]["keys"] or []
    picked: List[str] = []
    for item in requested or []:
        key = str(item or "").strip()
        if key and key not in picked:
            picked.append(key)
    unknown = [key for key in picked if key not in ordered]
    if unknown:
        raise ValueError(f"{axis}_keys 包含未知 key: {', '.join(unknown)}")
    if picked:
        return picked, False
    return ordered[:limit], len(ordered) > limit


def _lookup_3d_value(data: Dict[str, Any], *, dim1_key: str, dim2_key: str, metric_key: str) -> Any:
    dim1_row = data.get(dim1_key)
    if not isinstance(dim1_row, dict):
        return None
    dim2_row = dim1_row.get(dim2_key)
    if not isinstance(dim2_row, dict):
        return None
    return dim2_row.get(metric_key)


def _metric_formula_subset(raw: Dict[str, Any], metric_keys: List[str]) -> Dict[str, Any]:
    all_formulas = raw.get("column_formulas") or {}
    return {key: all_formulas[key] for key in metric_keys if key in all_formulas}


def _axis_key_payload(axis: str, key: str, axis_catalog: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "axis": axis,
        "label": axis_catalog[axis]["label"],
        "key": key,
        "display_name": axis_catalog[axis]["display"].get(key, key),
    }


def _slice_3d_table_result(
    raw: Dict[str, Any],
    *,
    keep_axes: Optional[List[str]] = None,
    dim1_keys: Optional[List[str]] = None,
    dim2_keys: Optional[List[str]] = None,
    metric_keys: Optional[List[str]] = None,
    limit_per_axis: int = 50,
    include_formulas: bool = True,
) -> Dict[str, Any]:
    keep = [str(axis).strip() for axis in (keep_axes or ["dim1", "metric"]) if str(axis).strip()]
    if len(keep) not in (1, 2):
        raise ValueError("keep_axes 只能保留 1 个或 2 个轴，且仅支持 dim1 / dim2 / metric")
    if any(axis not in _THREE_D_AXES for axis in keep):
        raise ValueError("keep_axes 只能包含 dim1 / dim2 / metric")
    if len(set(keep)) != len(keep):
        raise ValueError("keep_axes 不能重复")

    axis_catalog = _build_3d_axis_catalog(raw)
    limit = max(1, min(int(limit_per_axis or 50), 200))
    selected_dim1, truncated_dim1 = _select_3d_axis_keys("dim1", dim1_keys, axis_catalog, limit=limit)
    selected_dim2, truncated_dim2 = _select_3d_axis_keys("dim2", dim2_keys, axis_catalog, limit=limit)
    selected_metric, truncated_metric = _select_3d_axis_keys("metric", metric_keys, axis_catalog, limit=limit)
    data = raw.get("data") or {}

    selected_map = {
        "dim1": selected_dim1,
        "dim2": selected_dim2,
        "metric": selected_metric,
    }
    truncated_map = {
        "dim1": truncated_dim1,
        "dim2": truncated_dim2,
        "metric": truncated_metric,
    }
    fixed_axes = [axis for axis in _THREE_D_AXES if axis not in keep]
    fixed_value_lists = [selected_map[axis] for axis in fixed_axes]
    combinations = list(product(*fixed_value_lists)) if fixed_axes else [()]

    slices: List[Dict[str, Any]] = []
    if len(keep) == 2:
        row_axis, col_axis = keep
        for combo in combinations:
            fixed = {axis: key for axis, key in zip(fixed_axes, combo)}
            rows_out: List[Dict[str, Any]] = []
            returned_cell_count = 0
            for row_key in selected_map[row_axis]:
                row_values: Dict[str, Any] = {}
                for col_key in selected_map[col_axis]:
                    selectors = dict(fixed)
                    selectors[row_axis] = row_key
                    selectors[col_axis] = col_key
                    value = _lookup_3d_value(
                        data,
                        dim1_key=str(selectors["dim1"]),
                        dim2_key=str(selectors["dim2"]),
                        metric_key=str(selectors["metric"]),
                    )
                    if value is None:
                        continue
                    row_values[col_key] = _round_tool_value(value)
                if row_values:
                    returned_cell_count += len(row_values)
                    rows_out.append(
                        {
                            "key": row_key,
                            "display_name": axis_catalog[row_axis]["display"].get(row_key, row_key),
                            "values": row_values,
                        }
                    )
            if not rows_out:
                continue
            metric_scope = selected_map["metric"] if "metric" in keep else ([fixed["metric"]] if "metric" in fixed else [])
            slice_payload: Dict[str, Any] = {
                "fixed": {axis: _axis_key_payload(axis, key, axis_catalog) for axis, key in fixed.items()},
                "row_axis": row_axis,
                "row_axis_label": axis_catalog[row_axis]["label"],
                "col_axis": col_axis,
                "col_axis_label": axis_catalog[col_axis]["label"],
                "row_keys": selected_map[row_axis],
                "col_keys": selected_map[col_axis],
                "returned_row_count": len(rows_out),
                "returned_cell_count": returned_cell_count,
                "rows": rows_out,
            }
            if include_formulas:
                slice_payload["column_formulas"] = _metric_formula_subset(raw, metric_scope)
            slices.append(slice_payload)
    else:
        keep_axis = keep[0]
        for combo in combinations:
            fixed = {axis: key for axis, key in zip(fixed_axes, combo)}
            items: List[Dict[str, Any]] = []
            for axis_key in selected_map[keep_axis]:
                selectors = dict(fixed)
                selectors[keep_axis] = axis_key
                value = _lookup_3d_value(
                    data,
                    dim1_key=str(selectors["dim1"]),
                    dim2_key=str(selectors["dim2"]),
                    metric_key=str(selectors["metric"]),
                )
                if value is None:
                    continue
                item: Dict[str, Any] = {
                    "key": axis_key,
                    "display_name": axis_catalog[keep_axis]["display"].get(axis_key, axis_key),
                    "value": _round_tool_value(value),
                }
                if include_formulas and keep_axis == "metric":
                    formula_info = _metric_formula_subset(raw, [axis_key]).get(axis_key)
                    if formula_info:
                        item["formula"] = formula_info
                items.append(item)
            if not items:
                continue
            metric_scope = selected_map["metric"] if keep_axis == "metric" else ([fixed["metric"]] if "metric" in fixed else [])
            slice_payload = {
                "fixed": {axis: _axis_key_payload(axis, key, axis_catalog) for axis, key in fixed.items()},
                "axis": keep_axis,
                "axis_label": axis_catalog[keep_axis]["label"],
                "returned_item_count": len(items),
                "items": items,
            }
            if include_formulas:
                slice_payload["column_formulas"] = _metric_formula_subset(raw, metric_scope)
            slices.append(slice_payload)

    return {
        "table_name": raw.get("table_name"),
        "display_name": raw.get("display_name"),
        "view_mode": "grid" if len(keep) == 2 else "list",
        "keep_axes": keep,
        "axes": {
            axis: {
                "label": axis_catalog[axis]["label"],
                "col_name": axis_catalog[axis]["col_name"],
                "total_keys": len(axis_catalog[axis]["keys"]),
                "selected_keys": selected_map[axis],
                "truncated": truncated_map[axis],
            }
            for axis in _THREE_D_AXES
        },
        "slice_count": len(slices),
        "slices": slices,
    }


def _full_3d_table_result(raw: Dict[str, Any], *, include_formulas: bool = True) -> Dict[str, Any]:
    axis_catalog = _build_3d_axis_catalog(raw)
    data = raw.get("data") or {}
    cell_count = 0
    for dim1_rows in data.values():
        if not isinstance(dim1_rows, dict):
            continue
        for metrics in dim1_rows.values():
            if isinstance(metrics, dict):
                cell_count += len(metrics)
    out: Dict[str, Any] = {
        "table_name": raw.get("table_name"),
        "display_name": raw.get("display_name"),
        "kind": "3d_matrix",
        "row_count": raw.get("row_count"),
        "cell_count": cell_count,
        "values_are_numeric_only": True,
        "axes": {
            axis: {
                "label": axis_catalog[axis]["label"],
                "col_name": axis_catalog[axis]["col_name"],
                "keys": [_axis_key_payload(axis, key, axis_catalog) for key in axis_catalog[axis]["keys"]],
            }
            for axis in _THREE_D_AXES
        },
        "data": _round_tool_value(data),
    }
    if include_formulas:
        out["column_formulas"] = raw.get("column_formulas") or {}
    if cell_count > 2000:
        out["warning"] = "当前返回的是完整三轴结构；若只需局部视图，优先改用 read_3d_table 做切片。"
    return out


def _compact_3d_table_result(
    raw: Dict[str, Any],
    *,
    dim1_keys: Optional[List[str]] = None,
    dim2_keys: Optional[List[str]] = None,
    limit_dim1: int = 30,
    include_formulas: bool = True,
) -> Dict[str, Any]:
    dim1 = raw.get("dim1") or {}
    dim2 = raw.get("dim2") or {}
    data = raw.get("data") or {}
    dim1_meta = dim1.get("keys") or []
    dim2_meta = dim2.get("keys") or []
    dim1_display = {str(item.get("key")): str(item.get("display_name") or item.get("key") or "") for item in dim1_meta if isinstance(item, dict)}
    dim2_display = {str(item.get("key")): str(item.get("display_name") or item.get("key") or "") for item in dim2_meta if isinstance(item, dict)}

    ordered_dim1 = [str(item.get("key")) for item in dim1_meta if isinstance(item, dict) and str(item.get("key") or "").strip()]
    ordered_dim2 = [str(item.get("key")) for item in dim2_meta if isinstance(item, dict) and str(item.get("key") or "").strip()]
    if not ordered_dim1:
        ordered_dim1 = sorted(str(key) for key in data.keys())
    if not ordered_dim2:
        dim2_seen = {str(dim2_key) for rows in data.values() if isinstance(rows, dict) for dim2_key in rows.keys()}
        ordered_dim2 = sorted(dim2_seen)

    selected_dim1 = [str(key) for key in (dim1_keys or []) if str(key).strip()] or ordered_dim1[: max(1, min(int(limit_dim1 or 30), 200))]
    selected_dim2 = [str(key) for key in (dim2_keys or []) if str(key).strip()] or ordered_dim2

    sheets: List[Dict[str, Any]] = []
    returned_row_count = 0
    for dim2_key in selected_dim2:
        rows_out: List[Dict[str, Any]] = []
        for dim1_key in selected_dim1:
            values = ((data.get(dim1_key) or {}).get(dim2_key) if isinstance(data.get(dim1_key), dict) else None)
            if values is None:
                continue
            rows_out.append(
                {
                    "dim1_key": dim1_key,
                    "dim1_display_name": dim1_display.get(dim1_key, dim1_key),
                    "values": values,
                }
            )
        if rows_out:
            returned_row_count += len(rows_out)
            sheets.append(
                {
                    "dim2_key": dim2_key,
                    "dim2_display_name": dim2_display.get(dim2_key, dim2_key),
                    "row_count": len(rows_out),
                    "rows": rows_out,
                }
            )

    out: Dict[str, Any] = {
        "table_name": raw.get("table_name"),
        "display_name": raw.get("display_name"),
        "row_count": raw.get("row_count"),
        "returned_row_count": returned_row_count,
        "dim1": {
            "col_name": dim1.get("col_name"),
            "display_name": dim1.get("display_name"),
            "total_keys": len(ordered_dim1),
            "returned_keys": selected_dim1,
            "truncated": not dim1_keys and len(selected_dim1) < len(ordered_dim1),
        },
        "dim2": {
            "col_name": dim2.get("col_name"),
            "display_name": dim2.get("display_name"),
            "total_keys": len(ordered_dim2),
            "returned_keys": selected_dim2,
        },
        "cols": raw.get("cols") or [],
        "sheets": sheets,
    }
    if include_formulas:
        out["column_formulas"] = raw.get("column_formulas") or {}
    return out


def _update_table_readme(conn: sqlite3.Connection, table_name: str, content: str) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    if not cur.fetchone():
        return {"error": f"未知表 '{table_name}'", "fix": f"用 get_table_list 确认表名，当前已注册: {_list_known_tables(conn)}"}
    conn.execute(
        "UPDATE _table_registry SET readme = ? WHERE table_name = ?",
        (content, table_name),
    )
    conn.commit()
    return {"ok": True}


def _update_global_readme(conn: sqlite3.Connection, content: str) -> Dict[str, Any]:
    import time

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute(
        """
        INSERT INTO project_settings (key, value_json, updated_at)
        VALUES ('global_readme', ?, ?)
        ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json,
            updated_at = excluded.updated_at
        """,
        (json.dumps({"text": content}, ensure_ascii=False), now),
    )
    conn.commit()
    return {"ok": True}


_PROTECTED_SETTINGS = frozenset({"fixed_layer_config"})


def _compact_compare_snapshot_result(raw: Dict[str, Any]) -> Dict[str, Any]:
    changed_tables: List[Dict[str, Any]] = []
    for item in raw.get("changed_tables") or []:
        if not isinstance(item, dict):
            continue
        compact: Dict[str, Any] = {
            "table_name": item.get("table_name"),
            "row_count_previous": item.get("row_count_previous"),
            "row_count_current": item.get("row_count_current"),
        }
        for key in ("changed_columns", "added_columns", "removed_columns"):
            val = item.get(key)
            if val:
                compact[key] = val
        note = item.get("column_diff_note")
        if note:
            compact["column_diff_note"] = note
        changed_tables.append(compact)
    return {
        "snapshot_id": raw.get("snapshot_id"),
        "label": raw.get("label"),
        "changed_count": len(changed_tables),
        "changed_tables": changed_tables,
        "legacy_compare": bool(raw.get("legacy_compare")),
    }


def _compact_call_calculator_result(raw: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": bool(raw.get("ok")),
        "value": raw.get("value"),
        "found": bool(raw.get("found")),
    }
    if raw.get("fallback") is not None:
        out["fallback"] = bool(raw.get("fallback"))
    if raw.get("error"):
        out["error"] = raw.get("error")
    return out


def _set_project_setting(conn: sqlite3.Connection, key: str, value: Any) -> Dict[str, Any]:
    """写入 project_settings 中的任意键值对（保护 fixed_layer_config 不被覆盖）。"""
    import time

    if not key or not key.strip():
        raise ValueError("key 不能为空")
    if key in _PROTECTED_SETTINGS:
        raise ValueError(f"键 {key!r} 受保护，请勿覆盖；如需更新全局 README 请用 update_global_readme")
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    value_json = json.dumps(value, ensure_ascii=False)
    conn.execute(
        """
        INSERT INTO project_settings (key, value_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json,
            updated_at = excluded.updated_at
        """,
        (key, value_json, now),
    )
    conn.commit()
    return {"ok": True, "key": key, "value_preview": value_json[:200]}


def _bulk_register_and_compute(
    conn: sqlite3.Connection,
    table_name: str,
    items: List[Dict[str, Any]],
    register_only: bool,
) -> Dict[str, Any]:
    if not table_name:
        return {"error": "缺少 table_name", "fix": "请提供 table_name 参数，可通过 get_table_list 查看已有表"}
    if not items:
        return {"error": "items 不能为空", "fix": "items 是公式注册列表，每项含 column_name 和 formula_string"}
    registered: List[Dict[str, Any]] = []
    executed: List[Dict[str, Any]] = []
    errors: List[str] = []
    for it in items:
        col = str(it.get("column_name", ""))
        formula = str(it.get("formula_string", ""))
        if not col or not formula:
            errors.append(f"item 缺少 column_name/formula_string: {it!r}")
            continue
        try:
            register_formula(conn, table_name, col, formula)
            registered.append({"column": col, "formula": formula})
        except ValueError as e:
            known_cols = _list_table_columns(conn, table_name)
            errors.append(f"register '{col}' 失败: {e}。表 '{table_name}' 已有列: {known_cols}")
            continue
        if register_only:
            continue
        try:
            lm = it.get("level_min")
            lx = it.get("level_max")
            res = execute_formula_on_column(
                conn,
                table_name,
                col,
                level_column=str(it["level_column"]) if it.get("level_column") else None,
                level_min=float(lm) if lm is not None else None,
                level_max=float(lx) if lx is not None else None,
            )
            executed.append({"column": col, **res})
        except ValueError as e:
            errors.append(f"execute '{col}' 失败: {e}。检查公式中 @col 引用是否与表列名一致")
    out: Dict[str, Any] = {
        "table_name": table_name,
        "registered_count": len(registered),
        "executed_count": len(executed),
        "registered": registered,
        "executed": executed,
    }
    if errors:
        out["errors"] = errors
    return out


def _setup_level_table(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    max_level: int,
    level_column: str,
    columns: List[Dict[str, Any]],
    readme: str = "",
    purpose: str = "",
) -> Dict[str, Any]:
    if not table_name:
        return {"error": "缺少 table_name"}
    if max_level < 1:
        return {"error": "max_level 必须 ≥ 1"}
    if not columns:
        return {"error": "columns 不能为空"}

    pairs: List[tuple[str, str]] = []
    col_meta_list: List[Dict[str, str]] = []
    has_level = False
    for i, c in enumerate(columns):
        cname = str(c.get("name", ""))
        if not cname:
            return {"error": f"columns[{i}] 缺少 name 字段", "fix": "每个列定义必须包含 name(英文标识)、sql_type(TEXT|REAL|INTEGER)、display_name(中文名)"}
        ctype = str(c.get("sql_type") or "REAL")
        pairs.append((cname, ctype))
        col_meta_list.append({
            "name": cname,
            "display_name": str(c.get("display_name") or ""),
            "dtype": str(c.get("dtype") or ("int" if ctype == "INTEGER" else "float")),
            "number_format": str(c.get("number_format") or ""),
        })
        if cname == level_column:
            has_level = True
    if not has_level:
        pairs.insert(0, (level_column, "INTEGER"))
        col_meta_list.insert(0, {
            "name": level_column, "display_name": "等级",
            "dtype": "int", "number_format": "0",
        })
    try:
        create_dynamic_table(
            conn,
            table_name=table_name,
            columns=pairs,
            readme=readme,
            purpose=purpose,
            column_meta=col_meta_list,
        )
    except ValueError as e:
        msg = str(e)
        if "已存在" not in msg and "exists" not in msg.lower():
            known = _list_known_tables(conn)
            return {"error": f"建表失败: {e}", "fix": f"若表名冲突请先 delete_table，或换个表名。当前已有表: {known}"}

    now_rows = 0
    for lv in range(1, max_level + 1):
        rid = str(lv)
        conn.execute(
            f'INSERT OR IGNORE INTO "{table_name}" (row_id, "{level_column}") VALUES (?, ?)',
            (rid, lv),
        )
        now_rows += 1
    conn.commit()

    formula_items: List[Dict[str, Any]] = []
    for c in columns:
        cname = str(c.get("name", ""))
        if cname == level_column:
            continue
        formula = c.get("formula_string")
        if not formula:
            continue
        formula = normalize_self_table_refs(str(formula), table_name)
        formula_items.append({"column_name": cname, "formula_string": formula})

    bulk = _bulk_register_and_compute(conn, table_name, formula_items, False) if formula_items else {
        "registered_count": 0,
        "executed_count": 0,
    }
    return {
        "table_name": table_name,
        "rows_inserted": now_rows,
        "level_column": level_column,
        "max_level": max_level,
        "bulk": bulk,
    }


def _register_gameplay_table(
    conn,
    table_id: str,
    display_name: str,
    readme: str,
    order_num: int,
    dependencies: list,
) -> Dict[str, Any]:
    """注册玩法落地表到 _gameplay_table_registry。"""
    import time as _time
    if not table_id or not table_id.replace("_", "").isalnum():
        return {"status": "error", "data": None, "warnings": [f"table_id 必须为英文 snake_case: {table_id!r}"], "blocked_cells": []}
    if not display_name:
        return {"status": "error", "data": None, "warnings": ["display_name 不能为空"], "blocked_cells": []}
    now = _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime())
    deps_json = json.dumps(dependencies or [], ensure_ascii=False)
    try:
        conn.execute(
            """
            INSERT INTO _gameplay_table_registry
                (table_id, display_name, readme, status, started_at, order_num, dependencies, created_at, updated_at)
            VALUES (?, ?, ?, '未开始', NULL, ?, ?, ?, ?)
            ON CONFLICT(table_id) DO UPDATE SET
                display_name=excluded.display_name,
                readme=excluded.readme,
                order_num=excluded.order_num,
                dependencies=excluded.dependencies,
                updated_at=excluded.updated_at
            """,
            (table_id, display_name, readme, order_num, deps_json, now, now),
        )
        conn.commit()
        return {
            "status": "success",
            "data": {
                "table_id": table_id,
                "display_name": display_name,
                "order_num": order_num,
                "status": "未开始",
            },
            "warnings": [],
            "blocked_cells": [],
        }
    except Exception as e:  # noqa: BLE001
        return {"status": "error", "data": None, "warnings": [str(e)], "blocked_cells": []}


def _get_gameplay_table_list(conn) -> Dict[str, Any]:
    """读取所有已注册的玩法落地表。待修订表附带最新修订原因。"""
    try:
        items = list_registered_gameplay_tables(conn, readme_limit=500)
        return {"status": "success", "data": {"tables": items, "total": len(items)}, "warnings": [], "blocked_cells": []}
    except Exception as e:  # noqa: BLE001
        return {"status": "error", "data": None, "warnings": [str(e)], "blocked_cells": []}


def _set_gameplay_table_status(conn, table_id: str, status: str) -> Dict[str, Any]:
    """更新玩法落地表的执行状态。目标状态只允许 '进行中' 或 '已完成'。
    若要将表标记为 '待修订'，请使用 request_table_revision 工具。"""
    if status not in ("进行中", "已完成"):
        return {"status": "error", "data": None, "warnings": [f"非法状态: {status!r}，只允许 '进行中' / '已完成'；若要标记为待修订请使用 request_table_revision"], "blocked_cells": []}
    now = utc_now_iso()
    try:
        started_at = now if status == "进行中" else None
        cur = conn.execute(
            "UPDATE _gameplay_table_registry SET status=?, started_at=?, updated_at=? WHERE table_id=?",
            (status, started_at, now, table_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            return {"status": "error", "data": None, "warnings": [f"找不到 table_id: {table_id!r}，请先 register_gameplay_table"], "blocked_cells": []}
        # 当标记为已完成时：1) 将针对此表的已读参数升级为 acted_on; 2) 关闭此表的待处理修订请求
        if status == "已完成":
            step_id = f"gameplay_table.{table_id}"
            broadcast_key = "subsystems:gameplay_table"
            conn.execute(
                "UPDATE _step_exposed_params SET status='acted_on' "
                "WHERE (target_step = ? OR target_step = ?) AND status = 'acknowledged'",
                (step_id, broadcast_key),
            )
            conn.execute(
                "UPDATE _table_revision_requests SET status='done', updated_at=? "
                "WHERE table_id=? AND status='pending'",
                (utc_now_iso(), table_id),
            )
            conn.commit()
        return {
            "status": "success",
            "data": {"table_id": table_id, "new_status": status},
            "warnings": [],
            "blocked_cells": [],
        }
    except Exception as e:  # noqa: BLE001
        return {"status": "error", "data": None, "warnings": [str(e)], "blocked_cells": []}


def _request_table_revision(conn, table_id: str, reason: str, requested_by_step: str) -> Dict[str, Any]:
    """对已注册玩法表发起二次修订请求。将表状态重置为'待修订'，并在队列中记录请求。"""
    import time as _time
    if not table_id:
        return {"status": "error", "data": None, "warnings": ["table_id 必填"], "blocked_cells": []}
    if not reason:
        return {"status": "error", "data": None, "warnings": ["reason 必填，需说明为何要修订"], "blocked_cells": []}
    # 验证 table_id 存在
    row = conn.execute(
        "SELECT table_id, status FROM _gameplay_table_registry WHERE table_id=?",
        (table_id,),
    ).fetchone()
    if not row:
        return {
            "status": "error", "data": None,
            "warnings": [f"找不到 table_id: {table_id!r}，请先用 get_gameplay_table_list 查看有效的玩法表"],
            "blocked_cells": [],
        }
    now = _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime())
    # 创建修订请求
    conn.execute(
        """
        INSERT INTO _table_revision_requests (table_id, reason, requested_by_step, status, created_at, updated_at)
        VALUES (?, ?, ?, 'pending', ?, ?)
        """,
        (table_id, reason, requested_by_step or "", now, now),
    )
    # 将表状态标记为待修订
    conn.execute(
        "UPDATE _gameplay_table_registry SET status='待修订', updated_at=? WHERE table_id=?",
        (now, table_id),
    )
    conn.commit()
    return {
        "status": "success",
        "data": {
            "table_id": table_id,
            "previous_status": row[1],
            "new_status": "待修订",
            "reason": reason,
            "requested_by_step": requested_by_step or "",
            "hint": f"玩法表 '{table_id}' 已标记为待修订，修订请求已入队。后续 gameplay_table agent 循环时会自动看到并处理此任务。",
        },
        "warnings": [],
        "blocked_cells": [],
    }



def dispatch_tool(name: str, arguments: Union[str, Dict[str, Any], None], p: ProjectDB) -> str:
    conn = p.conn
    args: Dict[str, Any] = {}
    if arguments:
        if isinstance(arguments, str):
            try:
                args = json.loads(arguments or "{}")
            except json.JSONDecodeError as exc:
                return json.dumps(
                    {
                        "status": "error",
                        "data": None,
                        "warnings": [
                            f"参数 JSON 解析失败（很可能是输出被 max_tokens 截断）：{exc!r}；"
                            f"请将此次调用拆分为更小的批次重试。raw前缀={arguments[:120]!r}"
                        ],
                        "blocked_cells": [],
                        "hint": "请减少单次 write_cells 的 updates 数量（建议≤30行），或优先用公式代替逐行写入",
                    },
                    ensure_ascii=False,
                )
        else:
            args = dict(arguments)

    out: Dict[str, Any]
    if name == "get_project_config":
        out = {**_get_project_config(conn), "can_write": p.can_write}
    elif name == "get_table_list":
        out = _get_table_list(conn)
    elif name == "get_table_schema":
        out = _get_table_schema(
            conn,
            table_name=str(args.get("table_name", "")),
            include_readme_excerpt=bool(args.get("include_readme_excerpt", True)),
            include_formulas=bool(args.get("include_formulas", True)),
        )
    elif name == "read_table":
        cols = args.get("columns")
        col_list: Optional[List[str]] = cols if isinstance(cols, list) else None
        flt = args.get("filters") if isinstance(args.get("filters"), list) else None
        lm = args.get("level_min")
        lx = args.get("level_max")
        out = _read_table(
            conn,
            str(args.get("table_name", "")),
            int(args.get("limit", 50)),
            col_list,
            flt,
            args.get("level_column"),
            float(lm) if lm is not None else None,
            float(lx) if lx is not None else None,
            bool(args.get("include_source_stats")),
        )
    elif name == "read_cell":
        out = _read_cell(conn, str(args.get("table_name", "")), str(args.get("row_id", "")), str(args.get("column_name", "")))
    elif name == "get_protected_cells":
        out = _get_protected_cells(conn, str(args.get("table_name", "")))
    elif name == "get_dependency_graph":
        out = _dependency_edges(conn, args.get("table_name"), str(args.get("direction", "full")))
    elif name == "get_table_readme":
        out = _get_table_readme(conn, str(args.get("table_name", "")))
    elif name == "read_3d_table":
        try:
            raw = read_3d_table(conn, table_name=str(args.get("table_name", "")))
            if "keep_axes" in args or isinstance(args.get("metric_keys"), list):
                out = _slice_3d_table_result(
                    raw,
                    keep_axes=args.get("keep_axes") if isinstance(args.get("keep_axes"), list) else None,
                    dim1_keys=args.get("dim1_keys") if isinstance(args.get("dim1_keys"), list) else None,
                    dim2_keys=args.get("dim2_keys") if isinstance(args.get("dim2_keys"), list) else None,
                    metric_keys=args.get("metric_keys") if isinstance(args.get("metric_keys"), list) else None,
                    limit_per_axis=int(args.get("limit_per_axis", 50)),
                    include_formulas=bool(args.get("include_formulas", True)),
                )
            else:
                out = _compact_3d_table_result(
                    raw,
                    dim1_keys=args.get("dim1_keys") if isinstance(args.get("dim1_keys"), list) else None,
                    dim2_keys=args.get("dim2_keys") if isinstance(args.get("dim2_keys"), list) else None,
                    limit_dim1=int(args.get("limit_dim1", 30)),
                    include_formulas=bool(args.get("include_formulas", True)),
                )
        except ValueError as e:
            out = {
                "error": str(e),
                "fix": "read_3d_table 支持 keep_axes + dim1_keys/dim2_keys/metric_keys 做任意切片；若需要完整三轴结构，请改用 read_3d_table_full。",
            }
    elif name == "read_3d_table_full":
        try:
            out = _full_3d_table_result(
                read_3d_table(conn, table_name=str(args.get("table_name", ""))),
                include_formulas=bool(args.get("include_formulas", True)),
            )
        except ValueError as e:
            out = {
                "error": str(e),
                "fix": "确认目标表由 create_3d_table 创建；若只需某个切片，可改用 read_3d_table。",
            }
    elif name == "list_skills":
        items = _list_skills(
            conn,
            include_disabled=bool(args.get("include_disabled", False)),
            include_modules=False,
            project_slug=str(p.row["slug"]),
        )
        step_filter = str(args.get("step_id", "")).strip()
        if step_filter:
            items = [it for it in items if str(it.get("step_id", "")) == step_filter]
        out = {
            "items": [
                {
                    "id": it["id"],
                    "slug": it["slug"],
                    "title": it["title"],
                    "step_id": it.get("step_id", ""),
                    "summary": it.get("summary", ""),
                    "default_exposed": bool(it.get("default_exposed")),
                    "enabled": bool(it.get("enabled")),
                    "usage_count": int(it.get("usage_count", 0)),
                    "generated_file_path": it.get("generated_file_path", ""),
                }
                for it in items
            ]
        }
    elif name == "get_skill_detail":
        detail = _get_skill_detail(
            conn,
            str(args.get("skill_slug", "")),
            project_slug=str(p.row["slug"]),
            record_usage_event="tool_detail",
        )
        out = detail or {"error": "SKILL 不存在"}
    elif name == "render_skill_file":
        rendered = _render_skill_file(
            conn,
            str(args.get("skill_slug", "")),
            project_slug=str(p.row["slug"]),
            record_usage_event="render_file",
        )
        out = rendered or {"error": "SKILL 不存在"}
    elif name == "update_table_readme":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            out = _update_table_readme(conn, str(args.get("table_name", "")), str(args.get("content", "")))
    elif name == "update_global_readme":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            out = _update_global_readme(conn, str(args.get("content", "")))
    elif name == "set_project_setting":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            try:
                out = _set_project_setting(conn, str(args.get("key", "")), args.get("value"))
            except ValueError as e:
                out = {"error": str(e)}
    elif name == "create_table":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            raw_cols = args.get("columns") or []
            pairs: List[tuple[str, str]] = []
            col_meta: List[Dict[str, str]] = []
            try:
                for item in raw_cols:
                    if not isinstance(item, dict):
                        continue
                    cn = str(item.get("name", ""))
                    pairs.append((cn, str(item.get("sql_type", "TEXT"))))
                    col_meta.append({
                        "name": cn,
                        "display_name": str(item.get("display_name", "")),
                        "dtype": str(item.get("dtype", "")),
                        "number_format": str(item.get("number_format", "")),
                    })
                out = create_dynamic_table(
                    conn,
                    table_name=str(args.get("table_name", "")),
                    columns=pairs,
                    readme=str(args.get("readme", "")),
                    purpose=str(args.get("purpose", "")),
                    display_name=str(args.get("display_name", "")),
                    column_meta=col_meta,
                    kind=str(args.get("kind", "")),
                    directory=str(args.get("directory", "")),
                    tags=args.get("tags") or [],
                )
            except ValueError as e:
                tname = str(args.get("table_name", ""))
                known = _list_known_tables(conn)
                out = {
                    "error": f"create_table '{tname}' 失败: {e}",
                    "fix": f"若表已存在请先 delete_table 或换表名。当前已有表: {known}",
                }
    elif name == "write_cells":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            tag = args.get("source_tag") or "ai_generated"
            if tag not in ("ai_generated", "algorithm_derived", "formula_computed"):
                out = {
                    "error": f"非法 source_tag: '{tag}'",
                    "fix": "source_tag 合法值: ai_generated（AI 生成） | algorithm_derived（算法推导） | formula_computed（公式计算），通常应使用 ai_generated",
                }
            else:
                try:
                    out = apply_write_cells(
                        conn,
                        table_name=str(args.get("table_name", "")),
                        updates=list(args.get("updates") or []),
                        source_tag=tag,
                    )
                except ValueError as e:
                    out = {"error": str(e)}
    elif name == "write_cells_series":
        out = _write_cells_series(conn, args, p.can_write)
    elif name == "register_formula":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            tname = str(args.get("table_name", ""))
            cname = str(args.get("column_name", ""))
            try:
                out = register_formula(conn, tname, cname, str(args.get("formula_string", "")))
            except ValueError as e:
                known_cols = _list_table_columns(conn, tname)
                out = {
                    "error": f"register_formula 失败 (表='{tname}', 列='{cname}'): {e}",
                    "fix": f"检查列名是否存在于表中。表 '{tname}' 的列: {known_cols}；公式中 @引用 的列名必须与表列名完全一致",
                }
    elif name == "execute_formula":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            tname = str(args.get("table_name", ""))
            cname = str(args.get("column_name", ""))
            try:
                lm = args.get("level_min")
                lx = args.get("level_max")
                out = execute_formula_on_column(
                    conn, tname, cname,
                    level_column=str(args["level_column"]) if args.get("level_column") else None,
                    level_min=float(lm) if lm is not None else None,
                    level_max=float(lx) if lx is not None else None,
                )
            except ValueError as e:
                known_cols = _list_table_columns(conn, tname)
                out = {
                    "error": f"execute_formula 失败 (表='{tname}', 列='{cname}'): {e}",
                    "fix": f"确认公式已注册且 @col 引用存在。表 '{tname}' 的列: {known_cols}",
                }
    elif name == "recalculate_downstream":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            out = recalculate_downstream(
                conn,
                str(args.get("table_name", "")),
                str(args.get("column_name", "")),
            )
    elif name == "get_algorithm_api_list":
        out = {"apis": algorithms.list_apis()}
    elif name == "call_algorithm_api":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            try:
                params = args.get("params") if isinstance(args.get("params"), dict) else {}
                out = {"result": algorithms.call_api(str(args.get("api_name", "")), params)}
            except Exception as e:  # noqa: BLE001
                out = {"error": str(e)}
    elif name == "run_validation":
        tn = args.get("table_name")
        ft = str(tn) if tn else None
        out = build_validation_report(conn, filter_table=ft)
    elif name == "confirm_validation_rule":
        out = _confirm_validation_rule(
            conn,
            str(args.get("table_name", "")),
            str(args.get("rule_id", "")),
            str(args.get("reason", "")),
        )
    elif name == "delete_table":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            tname = str(args.get("table_name", ""))
            try:
                out = delete_dynamic_table(conn, table_name=tname, confirm=args.get("confirm"))
            except ValueError as e:
                msg = str(e)
                known = _list_known_tables(conn)
                if "依赖" in msg or "depend" in msg.lower():
                    out = {
                        "error": f"delete_table '{tname}' 被阻塞: {msg}",
                        "fix": "先用 get_dependency_graph 查看哪些表依赖它，或先删除/修改下游表的公式",
                    }
                else:
                    out = {
                        "error": f"delete_table '{tname}' 失败: {msg}",
                        "fix": f"确认表名正确。当前已有表: {known}",
                    }
    elif name == "create_snapshot":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            try:
                out = create_snapshot(
                    conn,
                    label=str(args.get("label", "snapshot")),
                    note=str(args.get("note", "")),
                )
            except Exception as e:  # noqa: BLE001
                out = {"error": str(e)}
    elif name == "list_snapshots":
        _SKIP = frozenset({"created_at", "updated_at"})
        snaps = list_snapshots(conn)
        if snaps:
            cols = [k for k in snaps[0].keys() if k not in _SKIP]
            out = {"cols": cols, "rows": [[r[c] for c in cols] for r in snaps], "total": len(snaps)}
        else:
            out = {"cols": [], "rows": [], "total": 0}
    elif name == "compare_snapshot":
        try:
            out = _compact_compare_snapshot_result(compare_snapshot(conn, int(args.get("snapshot_id", 0))))
        except (ValueError, TypeError) as e:
            out = {"error": str(e)}
    elif name == "run_balance_check":
        out = {
            "metrics": [],
            "level_min": args.get("level_min"),
            "level_max": args.get("level_max"),
            "note": "占位：待接入平衡模型（文档 06 run_balance_check）",
        }
    elif name == "get_validation_history":
        lim = int(args.get("limit", 20))
        tn = args.get("table_name")
        ft = str(tn) if tn else None
        out = {"history": list_validation_history(conn, table_name=ft, limit=lim)}
    elif name == "bulk_register_and_compute":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            out = _bulk_register_and_compute(
                conn,
                str(args.get("table_name", "")),
                args.get("items") or [],
                bool(args.get("register_only", False)),
            )
    elif name == "setup_level_table":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            out = _setup_level_table(
                conn,
                table_name=str(args.get("table_name", "")),
                max_level=int(args.get("max_level", 1)),
                level_column=str(args.get("level_column") or "level"),
                columns=args.get("columns") or [],
                readme=str(args.get("readme", "")),
                purpose=str(args.get("purpose", "")),
            )
    elif name == "get_default_system_rules":
        row = conn.execute(
            "SELECT value_json FROM project_settings WHERE key='default_rules_02'"
        ).fetchone()
        if row:
            try:
                out = json.loads(row["value_json"])
            except Exception:  # noqa: BLE001
                out = get_default_rules_payload()
        else:
            out = get_default_rules_payload()
    elif name == "glossary_register":
        out = _glossary_register(conn, args, p.can_write)
    elif name == "glossary_lookup":
        out = _glossary_lookup(conn, args)
    elif name == "glossary_list":
        out = _glossary_list(conn, args)
    elif name == "const_register":
        out = _const_register(conn, args, p.can_write)
    elif name == "const_set":
        out = _const_set(conn, args, p.can_write)
    elif name == "const_list":
        out = _const_list(conn, args)
    elif name == "const_delete":
        out = _const_delete(conn, args, p.can_write)
    elif name == "const_tag_register":
        out = _const_tag_register(conn, args, p.can_write)
    elif name == "const_tag_list":
        out = _const_tag_list(conn, args)
    # ─── 第3轮新增：表目录 / matrix / calculator / 暴露参数 ──────────────
    elif name == "list_directories":
        out = _list_directories(conn)
    elif name == "set_table_directory":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            out = _set_table_directory(conn, str(args.get("table_name", "")), str(args.get("directory", "")))
    elif name == "create_matrix_table":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            try:
                from app.services.matrix_table_ops import create_matrix_table as _create_mtx
                out = _create_mtx(
                    conn,
                    table_name=str(args.get("table_name", "")),
                    display_name=str(args.get("display_name", "")),
                    kind=str(args.get("kind", "")),
                    rows=args.get("rows") or [],
                    cols=args.get("cols") or [],
                    levels=args.get("levels"),
                    directory=str(args.get("directory", "")),
                    readme=str(args.get("readme", "")),
                    purpose=str(args.get("purpose", "")),
                    value_dtype=str(args.get("value_dtype", "float")),
                    value_format=str(args.get("value_format", "0.00%")),
                    scale_mode=args.get("scale_mode") or None,
                    tags=args.get("tags") or [],
                    default_value=args.get("default_value"),
                )
            except ValueError as e:
                out = {
                    "error": str(e),
                    "fix": "matrix_resource 规则：第三维轴值（如 level）可手填；若第三维切片数只有 1，可写常量；若切片数 > 1，整表内容必须改为 formula。",
                }
    elif name == "write_matrix_cells":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            try:
                from app.services.matrix_table_ops import write_matrix_cells as _wmc
                out = _wmc(
                    conn,
                    table_name=str(args.get("table_name", "")),
                    cells=args.get("cells") or [],
                )
            except ValueError as e:
                out = {
                    "error": str(e),
                    "fix": "matrix_resource 写入规则：单切片可写常量；多切片必须全表 formula。不要在同一张表里混写常量切片和公式切片。",
                }
    elif name == "read_matrix":
        try:
            from app.services.matrix_table_ops import read_matrix as _rm
            out = _rm(
                conn,
                table_name=str(args.get("table_name", "")),
                level=args.get("level"),
                rows=args.get("rows"),
                cols=args.get("cols"),
            )
        except ValueError as e:
            out = {"error": str(e)}
    elif name == "register_calculator":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            try:
                from app.services.calculator_ops import register_calculator as _rc
                out = _rc(
                    conn,
                    name=str(args.get("name", "")),
                    kind=str(args.get("kind", "")),
                    table_name=str(args.get("table_name", "")),
                    axes=args.get("axes") or [],
                    value_column=str(args.get("value_column", "value")),
                    brief=str(args.get("brief", "")),
                    grain=args.get("grain"),
                )
            except ValueError as e:
                out = {"error": str(e)}
    elif name == "list_calculators":
        from app.services.calculator_ops import list_calculators as _lc
        _SKIP = frozenset({"created_at", "updated_at"})
        calcs = _lc(conn)
        if calcs:
            cols = [k for k in calcs[0].keys() if k not in _SKIP]
            out = {"cols": cols, "rows": [[r[c] for c in cols] for r in calcs], "total": len(calcs)}
        else:
            out = {"cols": [], "rows": [], "total": 0}
    elif name == "call_calculator":
        from app.services.calculator_ops import call_calculator as _cc
        out = _compact_call_calculator_result(
            _cc(conn, name=str(args.get("name", "")), kwargs=args.get("kwargs") or {})
        )
    elif name == "expose_param_to_subsystems":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            out = _expose_param(conn, args)
    elif name == "list_exposed_params":
        out = _list_exposed_params(conn, str(args.get("target_step", "")))
    elif name == "register_gameplay_table":
        out = _register_gameplay_table(
            conn,
            table_id=str(args.get("table_id", "")),
            display_name=str(args.get("display_name", "")),
            readme=str(args.get("readme", "")),
            order_num=int(args.get("order_num", 0)),
            dependencies=args.get("dependencies") if isinstance(args.get("dependencies"), list) else [],
        )
    elif name == "get_gameplay_table_list":
        out = _get_gameplay_table_list(conn)
    elif name == "set_gameplay_table_status":
        out = _set_gameplay_table_status(
            conn,
            table_id=str(args.get("table_id", "")),
            status=str(args.get("status", "")),
        )
    elif name == "request_table_revision":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            out = _request_table_revision(
                conn,
                table_id=str(args.get("table_id", "")),
                reason=str(args.get("reason", "")),
                requested_by_step=str(args.get("requested_by_step", "")),
            )
    elif name == "sparse_sample":
        out = _sparse_sample(conn, args)
    elif name == "create_3d_table":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            try:
                out = create_3d_table(
                    conn,
                    table_name=str(args.get("table_name", "")),
                    display_name=str(args.get("display_name", "")),
                    dim1=args.get("dim1") or {},
                    dim2=args.get("dim2") or {},
                    cols=args.get("cols") or [],
                    readme=str(args.get("readme", "")),
                    purpose=str(args.get("purpose", "")),
                    directory=str(args.get("directory", "")),
                    tags=args.get("tags") or [],
                )
            except Exception as e:  # noqa: BLE001
                out = {
                    "error": str(e),
                    "fix": "create_3d_table 用于三维数据表：dim1/dim2 是可手填的轴值集合，但属性列只能存数值。若变化本质上来自维度展开，优先把变化写进 cols[].formula，而不是手填展开后的整表常量。",
                }
    else:
        out = {"error": f"未知工具 {name}"}
    return json.dumps(wrap_tool_payload(out), ensure_ascii=False)


def _expose_param(conn: sqlite3.Connection, args: Dict[str, Any]) -> Dict[str, Any]:
    owner = str(args.get("owner_step", "")).strip()
    target = str(args.get("target_step", "")).strip()
    key = str(args.get("key", "")).strip()
    brief = str(args.get("brief", "")).strip()
    if not owner or not target or not key:
        return {"error": "owner_step / target_step / key 均必填"}
    if not brief:
        return {"error": "brief 必填，用于子系统步骤上下文中向 AI 解释参数含义"}
    val_json = json.dumps(args.get("value"), ensure_ascii=False)
    import time
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute(
        """
        INSERT INTO _step_exposed_params (owner_step, target_step, key, value_json, brief, status, read_at, created_at)
        VALUES (?,?,?,?,?,'pending',NULL,?)
        ON CONFLICT(owner_step, target_step, key) DO UPDATE SET
            value_json = excluded.value_json,
            brief = excluded.brief,
            status = 'pending',
            read_at = NULL
        """,
        (owner, target, key, val_json, brief, now),
    )
    conn.commit()
    return {"ok": True, "owner_step": owner, "target_step": target, "key": key, "status": "pending"}


def _list_exposed_params(conn: sqlite3.Connection, target_step: str) -> Dict[str, Any]:
    if not target_step:
        return {"items": []}
    # 直接命中 + 通配（subsystems:<namespace>，如 subsystems:gameplay_table）
    broadcast_key = "subsystems:" + target_step.split(".")[0]
    cur = conn.execute(
        "SELECT owner_step, target_step, key, value_json, brief, status, read_at, created_at "
        "FROM _step_exposed_params "
        "WHERE target_step = ? OR target_step = ?",
        (target_step, broadcast_key),
    )
    rows = cur.fetchall()
    if not rows:
        return {"items": []}
    # 标记未读的条目为 acknowledged，记录 read_at 时间
    import time as _time
    now = _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime())
    pending_keys = [
        (r[0], r[1], r[2]) for r in rows if r[5] == "pending"
    ]
    for owner_step, ts, key in pending_keys:
        conn.execute(
            "UPDATE _step_exposed_params SET status='acknowledged', read_at=? "
            "WHERE owner_step=? AND target_step=? AND key=?",
            (now, owner_step, ts, key),
        )
    if pending_keys:
        conn.commit()
    items: List[Dict[str, Any]] = []
    for r in rows:
        try:
            val = json.loads(r[3])
        except Exception:  # noqa: BLE001
            val = r[3]
        # 如果刚被 acknowledged，反映最新状态
        status = "acknowledged" if (r[0], r[1], r[2]) in pending_keys else r[5]
        items.append({
            "owner_step": r[0],
            "target_step": r[1],
            "key": r[2],
            "value": val,
            "brief": r[4],
            "status": status,
            "read_at": now if (r[0], r[1], r[2]) in pending_keys else r[6],
            "created_at": r[7],
        })
    return {"items": items}


# ─── 术语 / 常数：实现 ───────────────────────────────────────────────


def _now_iso() -> str:
    import time as _t

    return _t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime())


def _require_write(can_write: bool, op: str) -> Optional[Dict[str, Any]]:
    if not can_write:
        return {"error": f"{op} 需要写权限（execute 阶段）", "status": "error"}
    return None


def _glossary_register(conn: sqlite3.Connection, args: Dict[str, Any], can_write: bool) -> Dict[str, Any]:
    err = _require_write(can_write, "glossary_register")
    if err:
        return err
    term_en = str(args.get("term_en", "")).strip()
    term_zh = str(args.get("term_zh", "")).strip()
    if not term_en or not term_zh:
        return {"error": "term_en 与 term_zh 必填"}
    if not __import__("re").match(r"^[a-z][a-z0-9_]*$", term_en):
        return {"error": f"term_en 必须 snake_case 英文：{term_en}"}
    kind = str(args.get("kind", "noun"))
    brief = str(args.get("brief", ""))
    scope_table = args.get("scope_table") or None
    now = _now_iso()
    conn.execute(
        """
        INSERT INTO _glossary (term_en, term_zh, kind, brief, scope_table, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?)
        ON CONFLICT(term_en) DO UPDATE SET
            term_zh = excluded.term_zh,
            kind = excluded.kind,
            brief = excluded.brief,
            scope_table = excluded.scope_table,
            updated_at = excluded.updated_at
        """,
        (term_en, term_zh, kind, brief, scope_table, now, now),
    )
    conn.commit()
    return {"ok": True, "term_en": term_en, "term_zh": term_zh}


def _glossary_lookup(conn: sqlite3.Connection, args: Dict[str, Any]) -> Dict[str, Any]:
    term_en = (args.get("term_en") or "").strip()
    term_zh = (args.get("term_zh") or "").strip()
    if not term_en and not term_zh:
        return {"error": "需要 term_en 或 term_zh"}
    if term_en:
        cur = conn.execute("SELECT * FROM _glossary WHERE term_en = ?", (term_en,))
    else:
        cur = conn.execute("SELECT * FROM _glossary WHERE term_zh = ?", (term_zh,))
    rows = [dict(r) for r in cur.fetchall()]
    return {"ok": True, "matches": rows, "count": len(rows)}


def _glossary_list(conn: sqlite3.Connection, args: Dict[str, Any]) -> Dict[str, Any]:
    """列出术语表，返回紧凑行列格式以节省 token。

    支持参数：
    - scope_table: 按表过滤（同时包含全局术语）
    - kind_filter: 按 kind 过滤（如 "stat"/"noun"/"verb"）
    - limit: 每页条数（默认 500，0=不限）
    - offset: 跳过前 N 条（默认 0）

    返回 cols + rows 行列格式，避免每行重复字段名，比对象列表节省约 35% token。
    """
    scope = args.get("scope_table")
    kind_filter = (args.get("kind_filter") or "").strip()
    limit = int(args.get("limit", 500))
    offset = int(args.get("offset", 0))

    conditions: List[str] = []
    params: List[Any] = []

    if scope:
        conditions.append("(scope_table IS NULL OR scope_table = ?)")
        params.append(scope)
    if kind_filter:
        conditions.append("kind = ?")
        params.append(kind_filter)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    total_row = conn.execute(
        f"SELECT count(*) FROM _glossary {where}", params
    ).fetchone()
    total = total_row[0] if total_row else 0

    page_params = list(params)
    if limit > 0:
        page_params += [limit, offset]
        page_sql = f"SELECT term_en, term_zh, kind, brief, scope_table FROM _glossary {where} ORDER BY term_en LIMIT ? OFFSET ?"
    else:
        page_sql = f"SELECT term_en, term_zh, kind, brief, scope_table FROM _glossary {where} ORDER BY term_en"

    cur = conn.execute(page_sql, page_params)
    rows = [list(r) for r in cur.fetchall()]

    result: Dict[str, Any] = {
        "ok": True,
        "total": total,
        "cols": ["term_en", "term_zh", "kind", "brief", "scope_table"],
        "rows": rows,
    }
    if limit > 0 and total > offset + len(rows):
        result["has_more"] = True
        result["next_offset"] = offset + len(rows)
    return result


def _coerce_value_json(v: Any) -> str:
    """常数值统一存为 JSON 串；优先解析为数值。"""
    if isinstance(v, (int, float)):
        return json.dumps(v)
    if isinstance(v, str):
        s = v.strip()
        try:
            return json.dumps(float(s))
        except (TypeError, ValueError):
            return json.dumps(s)
    return json.dumps(v)


_BRIEF_NUMBER_RE = None  # 已移除：brief 允许包含阿拉伯数字


def _validate_brief_no_value(brief: str) -> Optional[str]:
    """已废弃（brief 允许包含数值，无需校验）。"""
    return None


def _const_register(conn: sqlite3.Connection, args: Dict[str, Any], can_write: bool) -> Dict[str, Any]:
    err = _require_write(can_write, "const_register")
    if err:
        return err
    name_en = str(args.get("name_en", "")).strip()
    if not __import__("re").match(r"^[A-Za-z_][A-Za-z0-9_]*$", name_en):
        return {"error": f"name_en 不合法：{name_en}"}
    if "value" not in args:
        return {"error": "value 必填"}
    name_zh = str(args.get("name_zh", ""))
    brief = str(args.get("brief", ""))
    brief_err = _validate_brief_no_value(brief)
    if brief_err:
        return {"error": brief_err}
    scope_table = args.get("scope_table") or None
    raw_tags = args.get("tags") or []
    if not isinstance(raw_tags, list) or not raw_tags:
        return {
            "error": "tags 必填且至少 1 项",
            "fix": "请先用 const_tag_register 注册主系统标签（如 'combat'/'economy'），再传入 tags=['<标签>']",
        }
    tags = [str(t).strip() for t in raw_tags if str(t).strip()]
    if not tags:
        return {"error": "tags 不能为空字符串"}
    # 自动建标签（缺则插入），但不强制要求 parent
    now = _now_iso()
    for t in tags:
        conn.execute(
            "INSERT OR IGNORE INTO _const_tags (name, parent, brief, created_at) VALUES (?,?,?,?)",
            (t, None, "", now),
        )
    value_json = _coerce_value_json(args["value"])
    tags_json = json.dumps(tags, ensure_ascii=False)
    conn.execute(
        """
        INSERT INTO _constants (name_en, name_zh, value_json, brief, scope_table, tags, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(name_en) DO UPDATE SET
            name_zh = excluded.name_zh,
            value_json = excluded.value_json,
            brief = excluded.brief,
            scope_table = excluded.scope_table,
            tags = excluded.tags,
            updated_at = excluded.updated_at
        """,
        (name_en, name_zh, value_json, brief, scope_table, tags_json, now, now),
    )
    conn.commit()
    return {"ok": True, "name_en": name_en, "value": json.loads(value_json), "tags": tags}


def _const_set(conn: sqlite3.Connection, args: Dict[str, Any], can_write: bool) -> Dict[str, Any]:
    err = _require_write(can_write, "const_set")
    if err:
        return err
    name_en = str(args.get("name_en", "")).strip()
    cur = conn.execute("SELECT 1 FROM _constants WHERE name_en = ?", (name_en,))
    if not cur.fetchone():
        return {"error": f"常数 {name_en} 不存在；请先 const_register"}
    value_json = _coerce_value_json(args["value"])
    conn.execute(
        "UPDATE _constants SET value_json = ?, updated_at = ? WHERE name_en = ?",
        (value_json, _now_iso(), name_en),
    )
    conn.commit()
    return {"ok": True, "name_en": name_en, "value": json.loads(value_json)}


def _const_list(conn: sqlite3.Connection, args: Dict[str, Any]) -> Dict[str, Any]:
    """列出常数，返回紧凑行列格式以节省 token。

    支持参数：
    - scope_table: 按表过滤（同时包含全局常数）
    - tags_filter: 按标签过滤，列表或逗号分隔字符串（任意匹配一个即返回）
    - limit: 每页条数（默认 500，0=不限）
    - offset: 跳过前 N 条（默认 0）

    返回 cols + rows 行列格式，避免每行重复字段名，比对象列表节省约 35% token。
    """
    scope = args.get("scope_table")
    raw_tags_filter = args.get("tags_filter")
    limit = int(args.get("limit", 500))
    offset = int(args.get("offset", 0))

    # 解析 tags_filter：支持列表或逗号分隔字符串
    tags_filter: List[str] = []
    if isinstance(raw_tags_filter, list):
        tags_filter = [str(t).strip() for t in raw_tags_filter if str(t).strip()]
    elif isinstance(raw_tags_filter, str) and raw_tags_filter.strip():
        tags_filter = [t.strip() for t in raw_tags_filter.split(",") if t.strip()]

    conditions: List[str] = []
    params: List[Any] = []

    if scope:
        conditions.append("(scope_table IS NULL OR scope_table = ?)")
        params.append(scope)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    base_sql = (
        f"SELECT name_en, name_zh, value_json, brief, scope_table, tags "
        f"FROM _constants {where} ORDER BY name_en"
    )

    def _parse_row(r: Any) -> List[Any]:
        try:
            value = json.loads(r[2])
        except Exception:  # noqa: BLE001
            value = None
        raw_tags = r[5]
        try:
            item_tags: List[str] = json.loads(raw_tags) if isinstance(raw_tags, str) and raw_tags else []
        except Exception:  # noqa: BLE001
            item_tags = []
        return [r[0], r[1], value, r[3], r[4], item_tags]

    result: Dict[str, Any] = {
        "ok": True,
        "cols": ["name_en", "name_zh", "value", "brief", "scope_table", "tags"],
    }

    if tags_filter:
        # 全量取出后 Python 过滤，再分页——保证 total/has_more 对过滤后集合计算
        all_rows = [
            _parse_row(r)
            for r in conn.execute(base_sql, params).fetchall()
            if any(t in (json.loads(r[5]) if isinstance(r[5], str) and r[5] else []) for t in tags_filter)
        ]
        total_filtered = len(all_rows)
        if offset:
            all_rows = all_rows[offset:]
        if limit > 0 and len(all_rows) > limit:
            all_rows = all_rows[:limit]
            result["has_more"] = True
            result["next_offset"] = offset + limit
        result["total"] = total_filtered
        result["rows"] = all_rows
    else:
        total_row = conn.execute(f"SELECT count(*) FROM _constants {where}", params).fetchone()
        total = total_row[0] if total_row else 0
        if limit > 0:
            page_params = list(params) + [limit, offset]
            cur = conn.execute(f"{base_sql} LIMIT ? OFFSET ?", page_params)
        else:
            cur = conn.execute(base_sql, params)
        rows = [_parse_row(r) for r in cur.fetchall()]
        result["total"] = total
        result["rows"] = rows
        if limit > 0 and total > offset + len(rows):
            result["has_more"] = True
            result["next_offset"] = offset + len(rows)

    return result


def _const_tag_register(conn: sqlite3.Connection, args: Dict[str, Any], can_write: bool) -> Dict[str, Any]:
    err = _require_write(can_write, "const_tag_register")
    if err:
        return err
    name = str(args.get("name", "")).strip()
    if not name:
        return {"error": "name 必填"}
    parent = args.get("parent") or None
    brief = str(args.get("brief", ""))
    now = _now_iso()
    conn.execute(
        """
        INSERT INTO _const_tags (name, parent, brief, created_at) VALUES (?,?,?,?)
        ON CONFLICT(name) DO UPDATE SET parent = excluded.parent, brief = excluded.brief
        """,
        (name, parent, brief, now),
    )
    conn.commit()
    return {"ok": True, "name": name, "parent": parent}


def _const_tag_list(conn: sqlite3.Connection, args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        cur = conn.execute("SELECT name, parent, brief FROM _const_tags ORDER BY name")
        rows_dicts = [dict(r) for r in cur.fetchall()]
    except Exception:  # noqa: BLE001
        rows_dicts = []
    if rows_dicts:
        cols = list(rows_dicts[0].keys())
        return {"ok": True, "cols": cols, "rows": [[r[c] for c in cols] for r in rows_dicts], "total": len(rows_dicts)}
    return {"ok": True, "cols": [], "rows": [], "total": 0}


# ─── 系列填充：实现 ───────────────────────────────────────────────


_SAFE_EXPR_CHARS = __import__("re").compile(r"^[\d\s+\-*/().%i]+$")


def _eval_series_expr(expr: str, i: int) -> Any:
    """安全求值：仅允许数字、运算符、括号与变量 i。"""
    if not _SAFE_EXPR_CHARS.match(expr):
        raise ValueError(
            f"expr 含非法字符：{expr!r}；仅允许数字、+-*/()%、空格 与变量 i"
        )
    return eval(expr, {"__builtins__": {}}, {"i": i})  # noqa: S307 — 字符集已限制


def _write_cells_series(
    conn: sqlite3.Connection, args: Dict[str, Any], can_write: bool
) -> Dict[str, Any]:
    err = _require_write(can_write, "write_cells_series")
    if err:
        return err
    table_name = str(args.get("table_name", "")).strip()
    template = str(args.get("row_id_template", "")).strip()
    column = str(args.get("column", "")).strip()
    if not (table_name and template and column):
        return {"error": "table_name / row_id_template / column 均必填"}
    if "{i}" not in template:
        return {"error": "row_id_template 必须包含 {i} 占位符"}
    try:
        start = int(args.get("start"))
        end = int(args.get("end"))
    except (TypeError, ValueError):
        return {"error": "start / end 必须为整数"}
    if end < start:
        return {"error": "end 必须 ≥ start"}
    n = end - start + 1
    if n > 2000:
        return {"error": f"单次系列填充上限 2000 行，当前 {n}"}
    value_list = args.get("value_list")
    expr = args.get("expr")
    if (value_list is None) == (expr is None):
        return {"error": "value_list 与 expr 必须二选一"}
    if value_list is not None:
        if not isinstance(value_list, list) or len(value_list) != n:
            return {"error": f"value_list 长度需等于 {n}（end-start+1）"}
    tag = args.get("source_tag") or "ai_generated"
    if tag not in ("ai_generated", "algorithm_derived", "formula_computed"):
        return {"error": f"非法 source_tag: '{tag}'"}
    updates: List[Dict[str, Any]] = []
    for offset, idx in enumerate(range(start, end + 1)):
        if value_list is not None:
            v = value_list[offset]
        else:
            try:
                v = _eval_series_expr(str(expr), idx)
            except Exception as e:  # noqa: BLE001
                return {"error": f"i={idx} 求值失败: {e}"}
        updates.append(
            {"row_id": template.replace("{i}", str(idx)), "column": column, "value": v}
        )
    try:
        result = apply_write_cells(
            conn, table_name=table_name, updates=updates, source_tag=tag
        )
    except ValueError as e:
        return {"error": str(e)}
    if isinstance(result, dict):
        result["expanded_rows"] = n
    return result


def _const_delete(conn: sqlite3.Connection, args: Dict[str, Any], can_write: bool) -> Dict[str, Any]:
    err = _require_write(can_write, "const_delete")
    if err:
        return err
    name_en = str(args.get("name_en", "")).strip()
    conn.execute("DELETE FROM _constants WHERE name_en = ?", (name_en,))
    conn.commit()
    return {"ok": True, "name_en": name_en}


def _sparse_sample(conn: sqlite3.Connection, args: Dict[str, Any]) -> Dict[str, Any]:
    """均匀采样表中 N 行，用于曲线形态检查。"""
    table_name = str(args.get("table_name", "")).strip()
    columns = args.get("columns")
    if not table_name:
        return {"error": "table_name 必填"}
    if not isinstance(columns, list) or not columns:
        return {"error": "columns 必填，传列名数组"}
    n = max(2, min(int(args.get("n", 20)), 100))
    order_by = str(args.get("order_by", "level")).strip() or "level"

    # 验证表存在
    exists = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?", (table_name,)
    ).fetchone()
    if not exists:
        return {"error": f"表 {table_name!r} 不存在"}

    # 确认排序列是否存在，否则回退 row_id
    try:
        conn.execute(f'SELECT "{order_by}" FROM "{table_name}" LIMIT 1')
    except Exception:  # noqa: BLE001
        order_by = "row_id"

    # 获取总行数
    total = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
    if total == 0:
        return {"table": table_name, "total_rows": 0, "sampled": 0, "cols": columns, "rows": []}

    # 均匀采样：计算每隔 step 取一行
    safe_cols = ", ".join(f'"{c}"' for c in columns)
    if total <= n:
        # 行数不超过 n，全取
        cur = conn.execute(f'SELECT {safe_cols} FROM "{table_name}" ORDER BY "{order_by}"')
    else:
        # 用 ROW_NUMBER 等间隔采样
        step = total / n
        indices = [int(i * step) for i in range(n)]
        placeholders = ",".join(str(i) for i in indices)
        cur = conn.execute(
            f"""
            WITH ranked AS (
                SELECT {safe_cols}, ROW_NUMBER() OVER (ORDER BY "{order_by}") - 1 AS rn
                FROM "{table_name}"
            )
            SELECT {safe_cols} FROM ranked WHERE rn IN ({placeholders})
            ORDER BY rn
            """
        )

    rows = []
    for r in cur.fetchall():
        row_vals = []
        for col in columns:
            v = r[col] if col in r.keys() else None
            row_vals.append(round(v, 6) if isinstance(v, float) else v)
        rows.append(row_vals)

    return {
        "table": table_name,
        "total_rows": total,
        "sampled": len(rows),
        "order_by": order_by,
        "cols": columns,
        "rows": rows,
    }
