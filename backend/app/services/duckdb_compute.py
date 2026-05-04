"""DuckDB 计算骨架（B1）。

设计原则
========
- **存储仍在 SQLite**：本模块只负责"计算"。读取最小列集 → DuckDB 上跑 SQL →
  结果回写交给 `formula_exec._batch_apply_updates / _batch_apply_provenance`。
- **默认关闭**：受 `perf.use_duckdb_compute` 控制，且只对"白名单公式形态"生效；
  不在白名单的公式抛 `NotSupported`，由调用方 fallback 到 Pandas 路径。
- **单进程内最大可回退**：未安装 duckdb / 翻译失败 / 执行失败 → 抛
  `NotSupported`，调用方继续走原路径，不影响业务。

白名单公式（首版）
==================
- 仅引用本表列（`@<table>[col]`），且 `<table>` 等于目标表（即"同表四则运算 +
  常量"）。
- 表达式只能由：标识符替换、整数/浮点字面量、`+ - * / ( ) **/^`、
  `min(...)/max(...)/abs(...)/round(x[,n])` 构成。
- 不含 `call_calculator`、`@@`、`cumsum_*`、`vlookup`/`xlookup`/`if`/`piecewise`
  等高级函数 —— 这些走 Pandas。
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple


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


_REF_PATTERN = re.compile(r"@(\w+)\[(\w+)\]")
_AAREF_PATTERN = re.compile(r"@@(\w+)\[(\w+)\]")
_DISALLOWED_TOKENS = re.compile(
    r"\b(call_calculator|cumsum_to_here|cumsum_prev|vlookup|xlookup|"
    r"piecewise|interp|coalesce|ifnull|sum_arr|average_arr|count_arr|"
    r"counta_arr|match|index|lookup|text|num|bitand_concat)\b",
    re.IGNORECASE,
)
_ALLOWED_FUNCS = {"min", "max", "abs", "round", "sqrt"}
_NAME_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_LITERAL_OK = re.compile(r"^[\s\d\.\+\-\*\/\(\),%]+$")


def _check_whitelist(formula: str, table_name: str, columns_in_table: Set[str]) -> Tuple[Set[str], str]:
    """校验 + 收集本表被引用的列；返回 (used_cols, sql_safe_formula)。

    - sql_safe_formula 中所有 `@table[col]` 已被替换为 `"col"`（DuckDB 列引用）。
    - `^` → DuckDB 不支持位运算时上下文，但作为幂运算时使用 `**`，故统一替换为 `**`。
    """
    if _AAREF_PATTERN.search(formula):
        raise NotSupported("不支持 @@table[col]（整列引用）")
    if _DISALLOWED_TOKENS.search(formula):
        raise NotSupported("不支持高级函数 / call_calculator")
    if "&" in formula or "|" in formula:
        raise NotSupported("不支持位运算/字符串拼接（&/|）")

    used_cols: Set[str] = set()
    rewritten = formula
    for tbl, col in _REF_PATTERN.findall(formula):
        if tbl != table_name:
            raise NotSupported(f"跨表引用 {tbl}.{col}（首版仅支持同表）")
        if col not in columns_in_table:
            raise NotSupported(f"引用列不存在：{col}")
        used_cols.add(col)
        rewritten = rewritten.replace(f"@{tbl}[{col}]", f'"{col}"')

    rewritten = rewritten.replace("^", "**")

    # 安全检查：剩余的标识符必须是允许的函数名（剔除 "col" 形式的引用列）
    scan = re.sub(r'"[^"]*"', "", rewritten)
    for name in _NAME_PATTERN.findall(scan):
        if name.lower() in _ALLOWED_FUNCS:
            continue
        raise NotSupported(f"不支持的标识符：{name}")

    return used_cols, rewritten


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
    """白名单公式走 DuckDB 计算，返回 `[(value, row_id), ...]` 待批量回写。

    任何异常都包装成 `NotSupported`，调用方应 fallback 到 Pandas。
    """
    if not is_enabled(conn):
        raise NotSupported("DuckDB 未启用")

    try:
        import duckdb  # noqa: WPS433
    except Exception as exc:  # noqa: BLE001
        raise NotSupported(f"duckdb 未安装：{exc}") from exc

    try:
        existing_cols = {row[1] for row in conn.execute(f'PRAGMA table_info("{table_name}")')}
    except Exception as exc:  # noqa: BLE001
        raise NotSupported(f"读取表结构失败：{exc}") from exc
    if "row_id" not in existing_cols:
        raise NotSupported("表缺少 row_id 列")

    used_cols, sql_expr = _check_whitelist(formula, table_name, existing_cols)

    # 拉最小列集
    wanted: List[str] = ["row_id"] + sorted(used_cols)
    extra_level = (level_column or ("level" if (level_min is not None) else None))
    if extra_level and extra_level in existing_cols and extra_level not in wanted:
        wanted.append(extra_level)
    cols_sql = ", ".join(f'"{c}"' for c in wanted)
    import pandas as pd

    df = pd.read_sql_query(f'SELECT {cols_sql} FROM "{table_name}"', conn)
    if df.empty:
        return []

    where_clauses: List[str] = []
    params: List[Any] = []
    if level_min is not None and level_max is not None:
        if extra_level not in df.columns:
            raise NotSupported("level 列不存在")
        where_clauses.append(f'CAST("{extra_level}" AS DOUBLE) BETWEEN ? AND ?')
        params.extend([float(level_min), float(level_max)])

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    sql = f'SELECT "row_id", ({sql_expr}) AS __value FROM df{where_sql}'

    try:
        dcon = duckdb.connect(database=":memory:")
        try:
            dcon.register("df", df)
            result_df = dcon.execute(sql, params).df()
        finally:
            dcon.close()
    except Exception as exc:  # noqa: BLE001
        raise NotSupported(f"DuckDB 执行失败：{exc}") from exc

    pairs: List[Tuple[Any, Any]] = []
    for _, r in result_df.iterrows():
        v = r["__value"]
        rid = r["row_id"]
        try:
            v = float(v) if v is not None else None
        except (TypeError, ValueError):
            pass
        pairs.append((v, rid))
    return pairs
