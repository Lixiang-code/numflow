"""Agent 可调用的工具实现（对齐文档 06，与现有 HTTP 能力一致）。"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Optional, Union

from app.deps import ProjectDB
from app.services import algorithms
from app.services.cell_writes import apply_write_cells, assert_col_or_table
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
                "  整列引用：@@表名[列名]（整列 list，用于 VLOOKUP/INDEX/MATCH/SUM/AVERAGE）\n"
                "  运算：+ - * / ** %、ROUND/FLOOR/CEIL/ABS/SQRT/EXP/LOG/POW/POWER/MIN/MAX/CLAMP/"
                "IF/IFS/PIECEWISE/AND/OR/NOT/MOD（大小写不敏感）\n"
                "  比较：< <= > >= == !=\n"
                "  查找：VLOOKUP(val,@@lkup,@@ret,[exact]) / XLOOKUP(val,@@lkup,@@ret,[ifna]) / "
                "INDEX(@@col,row) / MATCH(val,@@col) / LOOKUP(val,@@lkup,@@ret)\n"
                "  聚合：SUM(@@col) / AVERAGE(@@col) / COUNT(@@col)\n"
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
                "level_column 默认 '等级'；columns 每项含 name + sql_type（默认 'REAL'）+ 可选 formula_string。"
                "公式中 @T[列] 用于同行逐行引用；@@表名[列] 用于查找函数整列引用；@T 会自动替换为本表名。"
                "示例：columns=[{name:'等级',sql_type:'INTEGER'},{name:'HP',formula_string:'ROUND(1000+49000*POWER((@T[等级]-1)/199,0.85),0)'}]"
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
        "SELECT table_name, layer, purpose, validation_status FROM _table_registry ORDER BY table_name"
    )
    return {"tables": [dict(r) for r in cur.fetchall()]}


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
        formula = str(formula).replace("@T[", f"@{table_name}[")
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
                )
            except ValueError as e:
                tname = str(args.get("table_name", ""))
                known = _list_known_tables(conn)
                out = {
                    "error": f"create_table '{tname}' 失败: {e}",
                    "fix": f"若表已存在请先 delete_table 或换表名。当前已有表: {known}",
                }
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
                level_column=str(args.get("level_column") or "等级"),
                columns=args.get("columns") or [],
                readme=str(args.get("readme", "")),
                purpose=str(args.get("purpose", "")),
            )
    elif name == "get_default_system_rules":
        out = get_default_rules_payload()
    else:
        out = {"error": f"未知工具 {name}"}
    return json.dumps(wrap_tool_payload(out), ensure_ascii=False)
