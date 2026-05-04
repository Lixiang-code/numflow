"""DuckDB 计算骨架（B1 + B2）。

设计原则
========
- **存储仍在 SQLite**：本模块只负责"计算"。读取最小列集 → DuckDB 上跑 SQL →
  结果回写交给 `formula_exec._batch_apply_updates / _batch_apply_provenance`。
- **默认关闭**：受 `perf.use_duckdb_compute` 控制，且只对"白名单公式形态"生效；
  不在白名单的公式抛 `NotSupported`，由调用方 fallback 到 Pandas 路径。
- **单进程内最大可回退**：未安装 duckdb / 翻译失败 / 执行失败 → 抛
  `NotSupported`，调用方继续走原路径，不影响业务。

B1 白名单（首版）
================
- 仅引用本表列（`@<table>[col]`），且 `<table>` 等于目标表（即"同表四则运算 +
  常量"）。
- 表达式只能由：标识符替换、整数/浮点字面量、`+ - * / ( ) **/^`、
  `min(...)/max(...)/abs(...)/round(x[,n])` 构成。

B2 白名单扩展
=============
- 支持 `@@table[col]` 整列引用（跨表加载为 DuckDB list）
- 支持 `INDEX(arr, idx)` → `list_element(arr, idx)`（1-indexed 对齐）
- 支持同表 `@table[col]` 和常量四则运算 + min/max/abs/round/sqrt/const_value
- 禁用：IF/PIECEWISE/call_calculator/cumsum_*/vlookup/xlookup 等
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd


class NotSupported(Exception):
    """公式不在 DuckDB 白名单内，调用方应 fallback 到 Pandas 路径。"""


def is_enabled(conn: sqlite3.Connection) -> bool:
    from app.services.perf_flags import perf_flag

    if not perf_flag(conn, "use_duckdb_compute"):
        return False
    try:
        import duckdb  # noqa: F401
    except Exception:  # noqa: BLE001
        return False
    return True


_REF_PATTERN = re.compile(r"(?<!@)@(?!@)(\w+)\[(\w+)\]")
_AAREF_PATTERN = re.compile(r"@@(\w+)\[(\w+)\]")
_INDEX_PATTERN = re.compile(r"\bINDEX\(@@(\w+)\[(\w+)\]\s*,\s*(.+?)\)", re.IGNORECASE)

_DISALLOWED_TOKENS = re.compile(
    r"\b(call_calculator|cumsum_to_here|cumsum_prev|vlookup|xlookup|"
    r"piecewise|interp|coalesce|ifnull|sum_arr|average_arr|count_arr|"
    r"counta_arr|match|lookup|text|num|bitand_concat)\b",
    re.IGNORECASE,
)
_ALLOWED_FUNCS = {"min", "max", "abs", "round", "sqrt", "const_value", "floor", "ceil", "list_element", "if", "least", "greatest"}
_SQL_KEYWORDS = {"case", "when", "then", "else", "end", "is", "not", "null", "and", "or", "true", "false", "in", "cast", "as", "integer", "double", "varchar", "between", "like", "bigint", "int"}
_NAME_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_CONST_VALUE_PATTERN = re.compile(r"const_value\(([\-\d\.]+)\)")


def _split_if_args(inner: str) -> List[str]:
    """按逗号分割 IF 的三个参数，正确处理嵌套括号和引号。"""
    args: List[str] = []
    depth = 0
    current: List[str] = []
    in_quote = False
    quote_char = ""
    for c in inner:
        if in_quote:
            current.append(c)
            if c == quote_char:
                in_quote = False
        elif c in ("'", '"'):
            in_quote = True
            quote_char = c
            current.append(c)
        elif c == '(':
            depth += 1
            current.append(c)
        elif c == ')':
            depth -= 1
            current.append(c)
        elif c == ',' and depth == 0:
            args.append("".join(current).strip())
            current = []
        else:
            current.append(c)
    if current:
        args.append("".join(current).strip())
    return args


def _check_whitelist(
    formula: str,
    table_name: str,
    columns_in_table: Set[str],
) -> Tuple[Set[str], str, Dict[Tuple[str, str], int]]:
    """B2 白名单校验。返回 (used_cols, sql_expr, aref_map)。

    aref_map: {(source_table, col_name): array_index}  — 需要加载的整列引用。
    """
    if _DISALLOWED_TOKENS.search(formula):
        raise NotSupported("不支持高级函数 / call_calculator")
    if "&" in formula or "|" in formula:
        raise NotSupported("不支持位运算/字符串拼接（&/|）")

    aref_map: Dict[Tuple[str, str], int] = {}
    a_idx = 0

    # 替换 @@table[col] → 临时数组变量名
    rewritten = formula
    for tbl, col in _AAREF_PATTERN.findall(formula):
        token = f"@@{tbl}[{col}]"
        if token not in rewritten:
            continue
        key = (tbl, col)
        if key not in aref_map:
            aref_map[key] = a_idx
            a_idx += 1
        arr_name = f"__a{aref_map[key]}"
        rewritten = rewritten.replace(token, arr_name)

    # 替换同表 @table[col] → "col"（DuckDB 列引用）
    used_cols: Set[str] = set()
    for tbl, col in _REF_PATTERN.findall(rewritten):
        token = f"@{tbl}[{col}]"
        if token not in rewritten:
            continue
        if tbl != table_name:
            raise NotSupported(f"B2 暂不支持跨表 @{tbl}[{col}]（请用 @@ + INDEX）")
        if col not in columns_in_table:
            raise NotSupported(f"引用列不存在：{col}")
        used_cols.add(col)
        rewritten = rewritten.replace(token, f'"{col}"')

    rewritten = rewritten.replace("^", "**")

    # 翻译 IF(cond, a, b) → CASE WHEN cond THEN a ELSE b END（递归处理嵌套）
    _IF_PATTERN = re.compile(r"\bIF\s*\(", re.IGNORECASE)
    while _IF_PATTERN.search(rewritten):
        pos = _IF_PATTERN.search(rewritten).start()
        depth = 0
        start_pos = pos + 2  # skip "IF"
        for i, c in enumerate(rewritten[start_pos:]):
            if c == '(':
                depth += 1
            elif c == ')':
                depth -= 1
                if depth == 0:
                    full_end = start_pos + i + 1
                    break
        else:
            raise NotSupported("IF 括号不匹配")
        if_call = rewritten[pos:full_end]
        inner = if_call[len("IF("):-1]  # strip IF( and )

        # 分割三个参数（需处理嵌套括号和字符串）
        params = _split_if_args(inner)
        if len(params) != 3:
            raise NotSupported(f"IF 参数数量错误：{len(params)}")
        cond, true_val, false_val = params
        rewritten = rewritten[:pos] + f"(CASE WHEN {cond} THEN {true_val} ELSE {false_val} END)" + rewritten[full_end:]

    # 翻译 INDEX(arr, expr) → list_element(arr, expr)
    # 需要处理嵌套 INDEX，使用递归替换
    while "INDEX(" in rewritten.upper():
        m = re.search(r"\bINDEX\((\w+)\s*,\s*(.+?)\)", rewritten, re.IGNORECASE)
        if not m:
            break
        arr_name, index_expr = m.group(1), m.group(2)
        # 匹配完整的 INDEX(...) 调用（处理嵌套括号）
        start = m.start()
        end = start + len(m.group(0))
        # 检查是否有嵌套括号
        depth = 0
        for i, c in enumerate(rewritten[start:]):
            if c == '(':
                depth += 1
            elif c == ')':
                depth -= 1
                if depth == 0:
                    end = start + i + 1
                    break
        full_match = rewritten[start:end]
        inner = full_match[len("INDEX("):-1]  # 去掉 INDEX( 和 )
        # 重新分割 arr_name 和 index_expr
        inner_m = re.match(r"^(\w+)\s*,\s*(.+)$", inner)
        if not inner_m:
            raise NotSupported(f"INDEX 语法错误：{full_match[:40]}")
        arr_name = inner_m.group(1)
        index_expr = inner_m.group(2).strip()
        rewritten = rewritten[:start] + f"list_element({arr_name}, CAST({index_expr} AS BIGINT))" + rewritten[end:]

    # 安全检查
    scan = re.sub(r'\'[^\']*\'', '', rewritten)  # 单引号字符串
    scan = re.sub(r'"[^"]*"', '', scan)            # 双引号列名
    for name in _NAME_PATTERN.findall(scan):
        if name.lower() in _ALLOWED_FUNCS:
            continue
        if name.lower() in _SQL_KEYWORDS:
            continue
        if re.match(r"^__a\d+$", name):
            continue
        raise NotSupported(f"不支持的标识符：{name}")

    rewritten = _CONST_VALUE_PATTERN.sub(r"\1", rewritten)

    # DuckDB 中 MIN/MAX 是聚合函数，标量版用 LEAST/GREATEST
    rewritten = re.sub(r"\bMIN\b", "LEAST", rewritten, flags=re.IGNORECASE)
    rewritten = re.sub(r"\bMAX\b", "GREATEST", rewritten, flags=re.IGNORECASE)

    return used_cols, rewritten, aref_map


def compute_column_via_duckdb(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    column_name: str,
    formula: str,
    level_column: Optional[str] = None,
    level_min: Optional[float] = None,
    level_max: Optional[float] = None,
) -> List[Tuple[Any, Any]]:
    """白名单公式走 DuckDB 计算，返回 `[(value, row_id), ...]` 待批量回写。"""
    if not is_enabled(conn):
        raise NotSupported("DuckDB 未启用")

    try:
        import duckdb
    except Exception as exc:
        raise NotSupported(f"duckdb 未安装：{exc}") from exc

    try:
        existing_cols = {row[1] for row in conn.execute(f'PRAGMA table_info("{table_name}")')}
    except Exception as exc:
        raise NotSupported(f"读取表结构失败：{exc}") from exc
    if "row_id" not in existing_cols:
        raise NotSupported("表缺少 row_id 列")

    used_cols, sql_expr, aref_map = _check_whitelist(formula, table_name, existing_cols)

    # 加载主表
    wanted: List[str] = ["row_id"] + sorted(used_cols)
    extra_level = (level_column or ("level" if (level_min is not None) else None))
    if extra_level and extra_level in existing_cols and extra_level not in wanted:
        wanted.append(extra_level)
    cols_sql = ", ".join(f'"{c}"' for c in wanted)
    df = pd.read_sql_query(f'SELECT {cols_sql} FROM "{table_name}"', conn)
    if df.empty:
        return []

    # 加载 @@ 整列引用到 pandas DataFrame 以便 DuckDB 使用
    array_dfs: Dict[str, pd.DataFrame] = {}
    for (src_tbl, src_col), idx in sorted(aref_map.items(), key=lambda x: x[1]):
        try:
            rows = conn.execute(
                f'SELECT "{src_col}" FROM "{src_tbl}" ORDER BY CAST(row_id AS INTEGER)'
            ).fetchall()
        except Exception as exc:
            raise NotSupported(f"加载 @@{src_tbl}[{src_col}] 失败：{exc}") from exc
        vals: List[Any] = []
        for r in rows:
            v = r[0]
            try:
                v = float(v) if v is not None else 0.0
            except (TypeError, ValueError):
                v = 0.0
            vals.append(v)
        array_dfs[f"__t_a{idx}"] = pd.DataFrame({"arr": [vals]})

    # 构建 SQL：将 @@ 引用替换为 (SELECT arr FROM __t_aN) 形式的 list 访问
    # list_element(list, idx) 在 DuckDB 中是 1-indexed，与 Pandas INDEX(arr, idx) 一致
    for name in array_dfs:
        var = name.replace("__t_a", "__a")
        sql_expr = sql_expr.replace(var, f"(SELECT arr FROM {name})")

    where_clauses: List[str] = []
    params: List[Any] = []
    if level_min is not None and level_max is not None:
        if extra_level and extra_level in df.columns:
            where_clauses.append(f'CAST("{extra_level}" AS DOUBLE) BETWEEN ? AND ?')
            params.extend([float(level_min), float(level_max)])

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    sql = f'SELECT "row_id", ({sql_expr}) AS __value FROM df{where_sql}'

    try:
        dcon = duckdb.connect(database=":memory:")
        try:
            dcon.register("df", df)
            for name, arr_df in array_dfs.items():
                dcon.register(name, arr_df)
            pairs: List[Tuple[Any, Any]] = []
            result = dcon.execute(sql, params)
            for row_data in result.fetchall():
                v = row_data[1]
                rid = row_data[0]
                try:
                    v = float(v) if v is not None else None
                except (TypeError, ValueError):
                    pass
                pairs.append((v, rid))
        finally:
            dcon.close()
    except Exception as exc:
        raise NotSupported(f"DuckDB 执行失败：{exc}") from exc

    return pairs
