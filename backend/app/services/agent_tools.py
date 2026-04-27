"""Agent 可调用的工具实现（对齐文档 06，与现有 HTTP 能力一致）。"""

from __future__ import annotations

import json
import sqlite3
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
from app.data.default_rules_02 import get_default_rules_payload
from app.services.snapshot_ops import compare_snapshot, create_snapshot, list_snapshots
from app.services.table_ops import create_dynamic_table, delete_dynamic_table
from app.services.tool_envelope import wrap_tool_payload
from app.services.validation_report import build_validation_report, list_validation_history

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
            "description": "列出所有业务表及验证状态",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_table",
            "description": "读取表数据；可选 columns、filters（每项 column+value 相等）、level_column+level_min/max、include_source_stats",
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
            "description": "依赖边列表；direction: upstream|downstream|full（与 /meta/dependency-graph 一致）",
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
            "description": "列出最近快照元数据",
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
                    "level_column": {"type": "string", "default": "等级"},
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
            "description": "列出所有术语（可按 scope_table 过滤）",
            "parameters": {
                "type": "object",
                "properties": {"scope_table": {"type": "string"}},
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
                "★ brief 描述常数语义/单位/取值范围，可以包含具体数值（如『起始值=100』）方便阅读。"
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
            "description": "列出所有已注册的常数标签",
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
            "description": "列出所有常数（可按 scope_table 过滤）",
            "parameters": {
                "type": "object",
                "properties": {"scope_table": {"type": "string"}},
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
                "【重要】scale_mode 决定 level 维的处理策略（避免写入爆炸）：\n"
                "  - 'none'（默认 matrix_attr）：无等级维，2D 表，调用时忽略 level 参数，无需填 levels。\n"
                "  - 'fallback'（默认 matrix_resource）：只写 level=NULL 基准值，有特殊等级时覆盖写；\n"
                "     call_calculator 先查精确 level，找不到自动回退 level=NULL 基准，无需预填所有等级。\n"
                "  - 'static'：旧行为，要求 AI 填满所有 (row,col,level) 组合，慎用。\n"
                "建表后会自动注册一个名为 <table>_lookup 的 calculator，供后续 call_calculator 查询。"
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
                        "description": "等级维策略：none=无等级（matrix_attr 默认）；fallback=懒触发（matrix_resource 默认）；static=全量预存",
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
                    "levels": {"type": "array", "items": {"type": "integer"}, "description": "仅 scale_mode='static' 时需要填写"},
                    "value_dtype": {"type": "string", "enum": ["float", "percent", "int"], "default": "float"},
                    "value_format": {"type": "string", "default": "0.00%"},
                    "readme": {"type": "string", "default": ""},
                    "purpose": {"type": "string", "default": ""},
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
                "向 matrix 表批量写入交叉点值。每项 {row, col, level (可空), value, note}。一次 ≤200 条。\n"
                "scale_mode='none' 时 level 字段自动忽略（存 NULL）；\n"
                "scale_mode='fallback' 时不传 level 即写入基准值（level=NULL），传 level 写精确覆盖。"
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
                            },
                            "required": ["row", "col", "value"],
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
            "description": "列出所有已注册 calculator（含 brief 说明，便于 AI 自检与下游引用）",
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
                "在父系统步骤里把关键参数暴露给所有子系统步骤。\n"
                "示例：装备_基础步骤暴露 `equip_base_to_upgrade_ratio=0.6`，"
                "装备_升级 / 装备_增幅 步骤的 prompt 会自动看到此参数。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "owner_step": {"type": "string", "description": "暴露源的步骤 ID"},
                    "target_step": {"type": "string", "description": "目标步骤 ID 或 'subsystems:<owner_step>'（广播）"},
                    "key": {"type": "string"},
                    "value": {},
                    "brief": {"type": "string"},
                },
                "required": ["owner_step", "target_step", "key", "value", "brief"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_exposed_params",
            "description": "列出针对某个步骤的所有 exposed params（子步骤启动 prompt 自动注入）。",
            "parameters": {
                "type": "object",
                "properties": {
                    "target_step": {"type": "string"},
                },
                "required": ["target_step"],
            },
        },
    },
]


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


def _get_project_config(conn: sqlite3.Connection) -> Dict[str, Any]:
    cur = conn.execute("SELECT key, value_json FROM project_settings")
    settings: Dict[str, Any] = {}
    for k, v in cur.fetchall():
        try:
            settings[k] = json.loads(v)
        except json.JSONDecodeError:
            settings[k] = v
    return {"settings": settings}


def _get_table_list(conn: sqlite3.Connection) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT table_name, layer, purpose, validation_status, "
        "COALESCE(directory,'') AS directory FROM _table_registry ORDER BY directory, table_name"
    )
    return {"tables": [dict(r) for r in cur.fetchall()]}


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
    if level_column is not None and level_min is not None and level_max is not None:
        try:
            lc = assert_col_or_table(str(level_column))
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
    rows = [dict(r) for r in cur.fetchall()]
    out: Dict[str, Any] = {"rows": rows}
    if include_source_stats and rows:
        rids = [str(r["row_id"]) for r in rows if r.get("row_id") is not None]
        out["provenance_stats"] = _provenance_stats(conn, t, rids, len(rows))
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
    return {"edges": [dict(r) for r in cur.fetchall()]}


def _get_table_readme(conn: sqlite3.Connection, table_name: str) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT readme FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    row = cur.fetchone()
    if not row:
        return {"error": f"未知表 '{table_name}'", "fix": f"用 get_table_list 确认表名，当前已注册: {_list_known_tables(conn)}"}
    return {"table_name": table_name, "readme": row["readme"] or ""}


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
        out = {"snapshots": list_snapshots(conn)}
    elif name == "compare_snapshot":
        try:
            out = compare_snapshot(conn, int(args.get("snapshot_id", 0)))
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
                )
            except ValueError as e:
                out = {"error": str(e)}
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
                out = {"error": str(e)}
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
        out = {"items": _lc(conn)}
    elif name == "call_calculator":
        from app.services.calculator_ops import call_calculator as _cc
        out = _cc(conn, name=str(args.get("name", "")), kwargs=args.get("kwargs") or {})
    elif name == "expose_param_to_subsystems":
        if not p.can_write:
            out = {"error": "无写权限"}
        else:
            out = _expose_param(conn, args)
    elif name == "list_exposed_params":
        out = _list_exposed_params(conn, str(args.get("target_step", "")))
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
        INSERT INTO _step_exposed_params (owner_step, target_step, key, value_json, brief, created_at)
        VALUES (?,?,?,?,?,?)
        ON CONFLICT(owner_step, target_step, key) DO UPDATE SET
            value_json = excluded.value_json,
            brief = excluded.brief
        """,
        (owner, target, key, val_json, brief, now),
    )
    conn.commit()
    return {"ok": True, "owner_step": owner, "target_step": target, "key": key}


def _list_exposed_params(conn: sqlite3.Connection, target_step: str) -> Dict[str, Any]:
    if not target_step:
        return {"items": []}
    # 直接命中 + 通配（subsystems:<owner>，调用方自行解析 owner）
    cur = conn.execute(
        "SELECT owner_step, target_step, key, value_json, brief FROM _step_exposed_params "
        "WHERE target_step = ? OR target_step = ?",
        (target_step, "subsystems:" + target_step.split(".")[0]),
    )
    items: List[Dict[str, Any]] = []
    for r in cur.fetchall():
        try:
            val = json.loads(r[3])
        except Exception:  # noqa: BLE001
            val = r[3]
        items.append({
            "owner_step": r[0],
            "target_step": r[1],
            "key": r[2],
            "value": val,
            "brief": r[4],
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
    scope = args.get("scope_table")
    if scope:
        cur = conn.execute(
            "SELECT * FROM _glossary WHERE scope_table IS NULL OR scope_table = ? ORDER BY term_en",
            (scope,),
        )
    else:
        cur = conn.execute("SELECT * FROM _glossary ORDER BY term_en")
    return {"ok": True, "items": [dict(r) for r in cur.fetchall()]}


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
    scope = args.get("scope_table")
    if scope:
        cur = conn.execute(
            "SELECT * FROM _constants WHERE scope_table IS NULL OR scope_table = ? ORDER BY name_en",
            (scope,),
        )
    else:
        cur = conn.execute("SELECT * FROM _constants ORDER BY name_en")
    items = []
    for r in cur.fetchall():
        d = dict(r)
        try:
            d["value"] = json.loads(d.pop("value_json"))
        except Exception:  # noqa: BLE001
            d["value"] = None
        # 兼容旧库未有 tags 列的情况
        raw_tags = d.get("tags")
        try:
            d["tags"] = json.loads(raw_tags) if isinstance(raw_tags, str) and raw_tags else []
        except Exception:  # noqa: BLE001
            d["tags"] = []
        items.append(d)
    return {"ok": True, "items": items}


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
        cur = conn.execute("SELECT name, parent, brief, created_at FROM _const_tags ORDER BY name")
        items = [dict(r) for r in cur.fetchall()]
    except Exception:  # noqa: BLE001
        items = []
    return {"ok": True, "items": items}


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
