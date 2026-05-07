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

B2/B3 白名单扩展
================
- 支持 `@@table[col]` 整列引用（跨表加载为 DuckDB list）
- 支持 `INDEX(arr, idx)` → `list_element(arr, idx)`（1-indexed 对齐）
- 支持 `MATCH(lookup, arr, 0)` → `list_position(arr, lookup)`（精确匹配）
- 支持 `IF(cond, a, b)` → `CASE WHEN cond THEN a ELSE b END`
- 支持 `CONCAT(a, b, ...)` → `concat(a, b, ...)`
- 支持精确匹配 `VLOOKUP/XLOOKUP`
- 支持同表 `@table[col]` 和常量四则运算 + min/max/abs/round/sqrt/const_value
- 禁用：PIECEWISE/call_calculator/cumsum_* 等高级函数
"""

from __future__ import annotations

import json
import os
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
_DISALLOWED_TOKENS = re.compile(
    r"\b(call_calculator|cumsum_to_here|cumsum_prev|"
    r"piecewise|interp|coalesce|ifnull|sum_arr|average_arr|count_arr|"
    r"counta_arr|lookup|text|num|bitand_concat)\b",
    re.IGNORECASE,
)
_ALLOWED_FUNCS = {
    "min", "max", "abs", "round", "sqrt", "const_value", "floor", "ceil",
    "list_element", "list_position", "if", "least", "greatest", "concat",
}
_SQL_KEYWORDS = {"case", "when", "then", "else", "end", "is", "not", "null", "and", "or", "true", "false", "in", "cast", "as", "integer", "double", "varchar", "between", "like", "bigint", "int"}
_NAME_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_CONST_VALUE_PATTERN = re.compile(r"const_value\(([\-\d\.]+)\)")
_NUMERIC_ROW_ID = re.compile(r"^-?\d+$")
_SUFFIX_NUMERIC_ROW_ID = re.compile(r"^(.*?)(-?\d+)$")
_SQLITE_SCHEMA = "__nf_sqlite"


def _cross_ref_aliases(formula: str, table_name: str) -> Dict[Tuple[str, str], str]:
    aliases: Dict[Tuple[str, str], str] = {}
    idx = 0
    for tbl, col in _REF_PATTERN.findall(formula):
        if tbl == table_name:
            continue
        key = (tbl, col)
        if key in aliases:
            continue
        aliases[key] = f"__r{idx}"
        idx += 1
    return aliases


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
    if in_quote:
        raise NotSupported("引号未闭合")
    if depth != 0:
        raise NotSupported("括号不匹配")
    if current:
        args.append("".join(current).strip())
    return args


def _find_matching_paren(expr: str, open_idx: int) -> int:
    depth = 0
    in_quote = False
    quote_char = ""
    i = open_idx
    while i < len(expr):
        c = expr[i]
        if in_quote:
            if c == quote_char:
                in_quote = False
        else:
            if c in ("'", '"'):
                in_quote = True
                quote_char = c
            elif c == '(':
                depth += 1
            elif c == ')':
                depth -= 1
                if depth == 0:
                    return i
        i += 1
    if in_quote:
        raise NotSupported("引号未闭合")
    raise NotSupported("括号不匹配")


def _replace_function_calls(
    expr: str,
    func_name: str,
    replacer,
) -> str:
    pattern = re.compile(rf"\b{re.escape(func_name)}\s*\(", re.IGNORECASE)
    out = expr
    while True:
        match = pattern.search(out)
        if not match:
            return out
        open_idx = out.find("(", match.start())
        close_idx = _find_matching_paren(out, open_idx)
        inner = out[open_idx + 1:close_idx]
        replacement = replacer(inner)
        out = out[:match.start()] + replacement + out[close_idx + 1:]


def _row_sort_key(row_id: Any, rownum: int) -> Tuple[Any, ...]:
    raw = "" if row_id is None else str(row_id)
    if _NUMERIC_ROW_ID.fullmatch(raw):
        return (0, int(raw), rownum)
    suffix = _SUFFIX_NUMERIC_ROW_ID.fullmatch(raw)
    if suffix:
        return (1, suffix.group(1), int(suffix.group(2)), rownum)
    return (2, raw, rownum)


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

    cross_ref_map = _cross_ref_aliases(formula, table_name)

    # 替换同表 @table[col] / 跨表 @table[col] → DuckDB 列引用
    used_cols: Set[str] = set()
    for tbl, col in _REF_PATTERN.findall(rewritten):
        token = f"@{tbl}[{col}]"
        if token not in rewritten:
            continue
        if tbl == table_name:
            if col not in columns_in_table:
                raise NotSupported(f"引用列不存在：{col}")
            used_cols.add(col)
            rewritten = rewritten.replace(token, f'"{col}"')
            continue
        alias = cross_ref_map.get((tbl, col))
        if not alias:
            raise NotSupported(f"跨表引用解析失败：@{tbl}[{col}]")
        rewritten = rewritten.replace(token, f'"{alias}"')

    rewritten = rewritten.replace("^", "**")

    # 翻译 IF(cond, a, b) → CASE WHEN cond THEN a ELSE b END
    def _replace_if(inner: str) -> str:
        params = _split_if_args(inner)
        if len(params) != 3:
            raise NotSupported(f"IF 参数数量错误：{len(params)}")
        cond, true_val, false_val = params
        return f"(CASE WHEN {cond} THEN {true_val} ELSE {false_val} END)"

    rewritten = _replace_function_calls(rewritten, "IF", _replace_if)

    def _replace_match(inner: str) -> str:
        params = _split_if_args(inner)
        if len(params) not in (2, 3):
            raise NotSupported(f"MATCH 参数数量错误：{len(params)}")
        lookup_expr, arr_expr = params[0], params[1]
        if len(params) == 3:
            match_type = params[2].strip()
            if match_type not in {"0", "0.0", "+0", "+0.0"}:
                raise NotSupported("DuckDB 路径仅支持 MATCH(..., ..., 0) 精确匹配")
        return f"list_position({arr_expr}, {lookup_expr})"

    rewritten = _replace_function_calls(rewritten, "MATCH", _replace_match)

    def _replace_index(inner: str) -> str:
        params = _split_if_args(inner)
        if len(params) != 2:
            raise NotSupported(f"INDEX 参数数量错误：{len(params)}")
        arr_expr, index_expr = params
        return f"list_element({arr_expr}, CAST({index_expr} AS BIGINT))"

    rewritten = _replace_function_calls(rewritten, "INDEX", _replace_index)

    def _exact_lookup_expr(
        lookup_expr: str,
        lookup_arr_expr: str,
        return_arr_expr: str,
        if_not_found_expr: str,
    ) -> str:
        match_expr = f"list_position({lookup_arr_expr}, {lookup_expr})"
        index_expr = f"list_element({return_arr_expr}, CAST({match_expr} AS BIGINT))"
        return f"(CASE WHEN {match_expr} IS NULL THEN {if_not_found_expr} ELSE {index_expr} END)"

    def _replace_vlookup(inner: str) -> str:
        params = _split_if_args(inner)
        if len(params) not in (3, 4):
            raise NotSupported(f"VLOOKUP 参数数量错误：{len(params)}")
        lookup_expr, lookup_arr_expr, return_arr_expr = params[:3]
        if len(params) == 4:
            exact_flag = params[3].strip().lower()
            if exact_flag not in {"0", "0.0", "+0", "+0.0", "false"}:
                raise NotSupported("DuckDB 路径仅支持 VLOOKUP(..., ..., ..., 0/FALSE) 精确匹配")
        return _exact_lookup_expr(lookup_expr, lookup_arr_expr, return_arr_expr, "NULL")

    rewritten = _replace_function_calls(rewritten, "VLOOKUP", _replace_vlookup)

    def _replace_xlookup(inner: str) -> str:
        params = _split_if_args(inner)
        if len(params) not in (3, 4):
            raise NotSupported(f"XLOOKUP 参数数量错误：{len(params)}")
        lookup_expr, lookup_arr_expr, return_arr_expr = params[:3]
        if_not_found_expr = params[3] if len(params) == 4 else "NULL"
        return _exact_lookup_expr(lookup_expr, lookup_arr_expr, return_arr_expr, if_not_found_expr)

    rewritten = _replace_function_calls(rewritten, "XLOOKUP", _replace_xlookup)

    # 安全检查
    scan = re.sub(r'\'[^\']*\'', '', rewritten)  # 单引号字符串
    scan = re.sub(r'"[^"]*"', '', scan)            # 双引号列名
    for name in _NAME_PATTERN.findall(scan):
        if name.lower() in _ALLOWED_FUNCS:
            continue
        if name.lower() in _SQL_KEYWORDS:
            continue
        if re.match(r"^__(?:a|r)\d+$", name):
            continue
        raise NotSupported(f"不支持的标识符：{name}")

    rewritten = _CONST_VALUE_PATTERN.sub(r"\1", rewritten)

    # DuckDB 中 MIN/MAX 是聚合函数，标量版用 LEAST/GREATEST
    rewritten = re.sub(r"\bMIN\b", "LEAST", rewritten, flags=re.IGNORECASE)
    rewritten = re.sub(r"\bMAX\b", "GREATEST", rewritten, flags=re.IGNORECASE)

    return used_cols, rewritten, aref_map


def _normalize_array_values(values: List[Any]) -> List[Any]:
    non_null = [v for v in values if v is not None]
    if not non_null:
        return values

    numeric_values: List[Any] = []
    numeric_ok = True
    for value in values:
        if value is None:
            numeric_values.append(None)
            continue
        try:
            numeric_values.append(float(value))
        except (TypeError, ValueError):
            numeric_ok = False
            break
    if numeric_ok:
        return numeric_values
    return [None if value is None else str(value) for value in values]


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _sqlite_db_path(conn: sqlite3.Connection) -> Optional[str]:
    try:
        rows = conn.execute("PRAGMA database_list").fetchall()
    except Exception:  # noqa: BLE001
        return None
    for row in rows:
        name = row["name"] if isinstance(row, sqlite3.Row) else row[1]
        path = row["file"] if isinstance(row, sqlite3.Row) else row[2]
        if str(name) != "main":
            continue
        raw = str(path or "").strip()
        if not raw or raw == ":memory:":
            return None
        if raw.startswith("file:"):
            return raw
        return os.path.abspath(raw)
    return None


def _load_sqlite_extension(dcon: Any) -> bool:
    attempts = [
        ("LOAD sqlite",),
        ("INSTALL sqlite", "LOAD sqlite"),
        ("LOAD sqlite_scanner",),
        ("INSTALL sqlite_scanner", "LOAD sqlite_scanner"),
    ]
    for stmts in attempts:
        try:
            for stmt in stmts:
                dcon.execute(stmt)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _attach_sqlite_database(dcon: Any, conn: sqlite3.Connection) -> Optional[str]:
    db_path = _sqlite_db_path(conn)
    if not db_path:
        return None
    if not _load_sqlite_extension(dcon):
        return None
    escaped = db_path.replace("'", "''")
    try:
        dcon.execute(f"ATTACH '{escaped}' AS {_quote_ident(_SQLITE_SCHEMA)} (TYPE sqlite)")
    except Exception:  # noqa: BLE001
        return None
    return _SQLITE_SCHEMA


def open_duckdb_session(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    if not is_enabled(conn):
        return None
    try:
        import duckdb
    except Exception:  # noqa: BLE001
        return None
    dcon = duckdb.connect(database=":memory:")
    sqlite_schema = _attach_sqlite_database(dcon, conn)
    if not sqlite_schema:
        dcon.close()
        return None
    return {"conn": dcon, "sqlite_schema": sqlite_schema}


def close_duckdb_session(session: Optional[Dict[str, Any]]) -> None:
    if not session:
        return
    dcon = session.get("conn")
    if dcon is None:
        return
    try:
        dcon.close()
    except Exception:  # noqa: BLE001
        pass


def _table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    return [row[1] for row in conn.execute(f'PRAGMA table_info("{table_name}")')]


def _empty_table_frame(conn: sqlite3.Connection, table_name: str) -> pd.DataFrame:
    df = pd.DataFrame(columns=_table_columns(conn, table_name))
    try:
        row = conn.execute(
            "SELECT schema_json, COALESCE(matrix_meta_json, '') AS matrix_meta_json FROM _table_registry WHERE table_name = ?",
            (table_name,),
        ).fetchone()
    except Exception:  # noqa: BLE001
        row = None
    if row:
        schema_json = row["schema_json"] if isinstance(row, sqlite3.Row) else row[0]
        matrix_meta_json = row["matrix_meta_json"] if isinstance(row, sqlite3.Row) else row[1]
        try:
            df.attrs["schema_columns"] = list((json.loads(schema_json or "{}") or {}).get("columns") or [])
        except Exception:  # noqa: BLE001
            df.attrs["schema_columns"] = []
        try:
            df.attrs["matrix_meta"] = json.loads(matrix_meta_json or "{}") or {}
        except Exception:  # noqa: BLE001
            df.attrs["matrix_meta"] = {}
    return df


def _resolve_join_pairs_for_scanner(
    conn: sqlite3.Connection,
    dcon: Any,
    *,
    sqlite_schema: str,
    target_table: str,
    ref_table: str,
    ref_col: str,
) -> Optional[List[Tuple[str, str]]]:
    from app.services.formula_engine import _candidate_join_columns

    target_df = _empty_table_frame(conn, target_table)
    ref_df = _empty_table_frame(conn, ref_table)
    table_sql = f"{_quote_ident(sqlite_schema)}.{_quote_ident(target_table)}"
    ref_sql = f"{_quote_ident(sqlite_schema)}.{_quote_ident(ref_table)}"
    for join_pairs in _candidate_join_columns(target_df, ref_df):
        right_cols = [right for _left, right in join_pairs]
        group_sql = ", ".join(_quote_ident(col) for col in right_cols)
        try:
            dup = dcon.execute(
                f"SELECT 1 FROM {ref_sql} GROUP BY {group_sql} HAVING COUNT(*) > 1 LIMIT 1"
            ).fetchone()
        except Exception:  # noqa: BLE001
            continue
        if dup is not None:
            continue
        on_sql = " AND ".join(
            f't.{_quote_ident(left)} = r.{_quote_ident(right)}'
            for left, right in join_pairs
        )
        try:
            missing = dcon.execute(
                f"""
                SELECT 1
                FROM {table_sql} AS t
                LEFT JOIN {ref_sql} AS r ON {on_sql}
                WHERE r.{_quote_ident(ref_col)} IS NULL
                LIMIT 1
                """
            ).fetchone()
        except Exception:  # noqa: BLE001
            continue
        if missing is not None:
            continue
        return join_pairs
    return None


def _row_order_sql(*, row_id_expr: str, row_num_expr: str) -> str:
    rid = f"COALESCE(CAST({row_id_expr} AS VARCHAR), '')"
    numeric = f"regexp_full_match({rid}, '^-?[0-9]+$')"
    suffix = f"regexp_full_match({rid}, '^(.*?)(-?[0-9]+)$')"
    return ", ".join(
        [
            f"CASE WHEN {numeric} THEN 0 WHEN {suffix} THEN 1 ELSE 2 END",
            f"CASE WHEN {numeric} THEN CAST({rid} AS BIGINT) ELSE NULL END",
            f"CASE WHEN {suffix} THEN regexp_extract({rid}, '^(.*?)(-?[0-9]+)$', 1) ELSE {rid} END",
            f"CASE WHEN {suffix} THEN CAST(regexp_extract({rid}, '^(.*?)(-?[0-9]+)$', 2) AS BIGINT) ELSE NULL END",
            row_num_expr,
        ]
    )


def _scanner_array_sql(*, sqlite_schema: str, table_name: str, column_name: str) -> str:
    table_sql = f"{_quote_ident(sqlite_schema)}.{_quote_ident(table_name)}"
    order_sql = _row_order_sql(row_id_expr='src."row_id"', row_num_expr="src.rowid")
    return f'(SELECT list(src.{_quote_ident(column_name)} ORDER BY {order_sql}) FROM {table_sql} AS src)'


def _compute_via_sqlite_scanner(
    conn: sqlite3.Connection,
    *,
    dcon: Any,
    sqlite_schema: str,
    table_name: str,
    formula: str,
    existing_cols: Set[str],
    used_cols: Set[str],
    sql_expr: str,
    aref_map: Dict[Tuple[str, str], int],
    cross_ref_map: Dict[Tuple[str, str], str],
    level_column: Optional[str],
    level_min: Optional[float],
    level_max: Optional[float],
) -> List[Tuple[Any, Any]]:
    from app.services.formula_exec import _join_hint_columns

    table_sql = f"{_quote_ident(sqlite_schema)}.{_quote_ident(table_name)}"
    wanted_main = sorted(set(used_cols) | _join_hint_columns(conn, table_name))
    extra_level = level_column or ("level" if (level_min is not None) else None)
    if extra_level and extra_level in existing_cols:
        wanted_main.append(extra_level)

    select_cols: List[str] = ['t."row_id"']
    seen_cols = {"row_id"}
    for col in wanted_main:
        if col not in existing_cols or col in seen_cols:
            continue
        select_cols.append(f't.{_quote_ident(col)}')
        seen_cols.add(col)

    joins: List[str] = []
    for idx, ((src_tbl, src_col), alias) in enumerate(cross_ref_map.items()):
        join_pairs = _resolve_join_pairs_for_scanner(
            conn,
            dcon,
            sqlite_schema=sqlite_schema,
            target_table=table_name,
            ref_table=src_tbl,
            ref_col=src_col,
        )
        if not join_pairs:
            raise NotSupported(f"无法对齐跨表引用 @{src_tbl}[{src_col}]")
        ref_alias = f"r{idx}"
        ref_sql = f"{_quote_ident(sqlite_schema)}.{_quote_ident(src_tbl)}"
        on_sql = " AND ".join(
            f't.{_quote_ident(left)} = {ref_alias}.{_quote_ident(right)}'
            for left, right in join_pairs
        )
        joins.append(f"LEFT JOIN {ref_sql} AS {ref_alias} ON {on_sql}")
        select_cols.append(f'{ref_alias}.{_quote_ident(src_col)} AS {_quote_ident(alias)}')

    scanner_expr = sql_expr
    for (src_tbl, src_col), idx in aref_map.items():
        scanner_expr = scanner_expr.replace(
            f"__a{idx}",
            _scanner_array_sql(sqlite_schema=sqlite_schema, table_name=src_tbl, column_name=src_col),
        )

    inner_sql = f'SELECT {", ".join(select_cols)} FROM {table_sql} AS t'
    if joins:
        inner_sql += " " + " ".join(joins)

    where_clauses: List[str] = []
    params: List[Any] = []
    if level_min is not None and level_max is not None and extra_level and extra_level in seen_cols:
        where_clauses.append(f'CAST("{extra_level}" AS DOUBLE) BETWEEN ? AND ?')
        params.extend([float(level_min), float(level_max)])
    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    sql = f'SELECT "row_id", ({scanner_expr}) AS __value FROM ({inner_sql}) AS df{where_sql}'

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
    return pairs


def compute_column_via_duckdb(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    column_name: str,
    formula: str,
    level_column: Optional[str] = None,
    level_min: Optional[float] = None,
    level_max: Optional[float] = None,
    table_cache: Optional[Dict[str, pd.DataFrame]] = None,
    duckdb_conn: Any = None,
    sqlite_schema: Optional[str] = None,
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
    cross_ref_map = _cross_ref_aliases(formula, table_name)

    from app.services.formula_engine import _align_scalar_series
    from app.services.formula_exec import _join_hint_columns, load_table_df

    local_dcon = duckdb_conn
    close_local_dcon = False
    if local_dcon is None:
        local_dcon = duckdb.connect(database=":memory:")
        close_local_dcon = True
        sqlite_schema = _attach_sqlite_database(local_dcon, conn)

    if sqlite_schema:
        try:
            return _compute_via_sqlite_scanner(
                conn,
                dcon=local_dcon,
                sqlite_schema=sqlite_schema,
                table_name=table_name,
                formula=formula,
                existing_cols=existing_cols,
                used_cols=used_cols,
                sql_expr=sql_expr,
                aref_map=aref_map,
                cross_ref_map=cross_ref_map,
                level_column=level_column,
                level_min=level_min,
                level_max=level_max,
            )
        except Exception:  # noqa: BLE001
            pass

    # 加载主表
    if cross_ref_map:
        df = load_table_df(conn, table_name, table_cache=table_cache)
    else:
        wanted = sorted(set(used_cols) | _join_hint_columns(conn, table_name))
        extra_level = (level_column or ("level" if (level_min is not None) else None))
        if extra_level and extra_level in existing_cols:
            wanted.append(extra_level)
        df = load_table_df(conn, table_name, wanted, table_cache=table_cache)
    if df.empty:
        return []

    ref_frames: Dict[str, pd.DataFrame] = {}
    for (src_tbl, src_col), alias in cross_ref_map.items():
        ref_df = ref_frames.get(src_tbl)
        if ref_df is None:
            ref_df = load_table_df(conn, src_tbl, table_cache=table_cache)
            ref_frames[src_tbl] = ref_df
        if src_col not in ref_df.columns:
            raise NotSupported(f"跨表引用列不存在：{src_tbl}.{src_col}")
        aligned = _align_scalar_series(df, ref_df, src_col)
        if aligned is None:
            raise NotSupported(f"无法对齐跨表引用 @{src_tbl}[{src_col}]")
        df[alias] = aligned.tolist()

    # 加载 @@ 整列引用到 pandas DataFrame 以便 DuckDB 使用
    array_dfs: Dict[str, pd.DataFrame] = {}
    for (src_tbl, src_col), idx in sorted(aref_map.items(), key=lambda x: x[1]):
        try:
            rows = conn.execute(
                f'SELECT rowid, row_id, "{src_col}" FROM "{src_tbl}" ORDER BY rowid'
            ).fetchall()
        except Exception as exc:
            raise NotSupported(f"加载 @@{src_tbl}[{src_col}] 失败：{exc}") from exc
        sorted_rows = sorted(rows, key=lambda r: _row_sort_key(r[1], int(r[0])))
        vals = _normalize_array_values([r[2] for r in sorted_rows])
        array_dfs[f"__t_a{idx}"] = pd.DataFrame({"arr": [vals]})

    # 构建 SQL：将 @@ 引用替换为 (SELECT arr FROM __t_aN) 形式的 list 访问
    # list_element(list, idx) 在 DuckDB 中是 1-indexed，与 Pandas INDEX(arr, idx) 一致
    for name in array_dfs:
        var = name.replace("__t_a", "__a")
        sql_expr = sql_expr.replace(var, f"(SELECT arr FROM {name})")

    where_clauses: List[str] = []
    params: List[Any] = []
    extra_level = (level_column or ("level" if (level_min is not None) else None))
    if level_min is not None and level_max is not None:
        if extra_level and extra_level in df.columns:
            where_clauses.append(f'CAST("{extra_level}" AS DOUBLE) BETWEEN ? AND ?')
            params.extend([float(level_min), float(level_max)])

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    sql = f'SELECT "row_id", ({sql_expr}) AS __value FROM df{where_sql}'

    try:
        local_dcon.register("df", df)
        for name, arr_df in array_dfs.items():
            local_dcon.register(name, arr_df)
        pairs: List[Tuple[Any, Any]] = []
        result = local_dcon.execute(sql, params)
        for row_data in result.fetchall():
            v = row_data[1]
            rid = row_data[0]
            try:
                v = float(v) if v is not None else None
            except (TypeError, ValueError):
                pass
            pairs.append((v, rid))
        return pairs
    except Exception as exc:
        raise NotSupported(f"DuckDB 执行失败：{exc}") from exc
    finally:
        for name in ["df", *array_dfs.keys()]:
            try:
                local_dcon.unregister(name)
            except Exception:  # noqa: BLE001
                pass
        if close_local_dcon:
            local_dcon.close()
