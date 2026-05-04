"""公式执行与注册（供 /compute 与 Agent 工具复用）。"""

from __future__ import annotations

import json
import re
import sqlite3
import time
from contextlib import contextmanager
from collections import defaultdict, deque
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd

from app.services.formula_engine import (
    eval_row_formula,
    eval_series,
    inject_call_calculator,
    normalize_self_table_refs,
    parse_constant_refs,
    parse_formula_refs,
    parse_row_refs,
    preprocess_formula,
    reset_call_calculator,
    substitute_constants,
)
from app.services.perf_flags import PerfTimer, perf_flag

Node = Tuple[str, str]


def _rewrite_3d_dim_aliases(
    conn: sqlite3.Connection,
    table_name: str,
    formula: str,
) -> str:
    if "@dim1_key" not in formula and "@dim2_key" not in formula:
        return formula
    row = conn.execute(
        "SELECT matrix_meta_json FROM _table_registry WHERE table_name = ?",
        (table_name,),
    ).fetchone()
    if not row:
        return formula
    try:
        meta = json.loads(row[0] or "{}") or {}
    except Exception:  # noqa: BLE001
        return formula
    if meta.get("kind") != "3d_matrix":
        return formula
    dim1_col = str((meta.get("dim1") or {}).get("col_name") or "").strip()
    dim2_col = str((meta.get("dim2") or {}).get("col_name") or "").strip()
    if dim1_col:
        formula = re.sub(r"(?<!@)@dim1_key(?![\[\w])", f"@{dim1_col}", formula)
    if dim2_col:
        formula = re.sub(r"(?<!@)@dim2_key(?![\[\w])", f"@{dim2_col}", formula)
    return formula


@contextmanager
def _formula_call_calculator_context(conn: sqlite3.Connection):
    """注入 `call_calculator()` 函数；可选启用元数据 + 结果缓存（A4）。

    缓存仅作用于本次公式执行（context 生命周期），避免跨执行的语义偏差。
    关闭 `perf.use_batch_lookup` 时退化为旧路径（每次都查 `_calculators`）。
    """
    from app.services.calculator_ops import (
        call_calculator as resolve_calculator,
        get_calculator_meta,
    )

    use_cache = perf_flag(conn, "use_batch_lookup")
    # 元数据缓存：name -> calculator meta
    meta_cache: Dict[str, Dict[str, Any]] = {}
    # 结果缓存：(name, frozenset(kwargs.items 可哈希化)) -> value
    result_cache: Dict[Tuple[str, Any], Any] = {}

    def _hashable_kwargs(kwargs: Dict[str, Any]) -> Optional[Any]:
        try:
            return tuple(sorted((str(k), _to_hashable(v)) for k, v in kwargs.items()))
        except Exception:  # noqa: BLE001
            return None

    def _to_hashable(v: Any) -> Any:
        if v is None or isinstance(v, (str, int, float, bool)):
            return v
        try:
            return json.dumps(v, ensure_ascii=False, sort_keys=True, default=str)
        except Exception:  # noqa: BLE001
            return str(v)

    def _formula_call_calculator(name: Any, *args: Any) -> Any:
        calc_name = str(name).strip()
        if not calc_name:
            raise ValueError("call_calculator 第 1 个参数必须是 calculator 名称")

        meta: Optional[Dict[str, Any]] = None
        axes: List[Dict[str, Any]]
        if use_cache:
            if calc_name not in meta_cache:
                meta = get_calculator_meta(conn, calc_name)
                if not meta:
                    raise ValueError(f"未知 calculator {calc_name!r}")
                meta_cache[calc_name] = meta
            else:
                meta = meta_cache[calc_name]
            axes = list(meta.get("axes") or [])
        else:
            row = conn.execute(
                "SELECT axes_json FROM _calculators WHERE name = ?",
                (calc_name,),
            ).fetchone()
            if not row:
                raise ValueError(f"未知 calculator {calc_name!r}")
            axes_json = row["axes_json"] if isinstance(row, sqlite3.Row) else row[0]
            try:
                axes = json.loads(axes_json or "[]")
            except Exception as exc:  # noqa: BLE001
                raise ValueError(f"calculator {calc_name!r} 配置损坏") from exc

        if len(args) > len(axes):
            raise ValueError(
                f"call_calculator({calc_name}) 参数过多：期望最多 {len(axes)} 个，实际 {len(args)} 个"
            )
        kwargs: Dict[str, Any] = {}
        for axis, value in zip(axes, args):
            axis_name = str((axis or {}).get("name") or "").strip()
            if not axis_name:
                continue
            kwargs[axis_name] = value

        cache_key: Optional[Tuple[str, Any]] = None
        if use_cache:
            hk = _hashable_kwargs(kwargs)
            if hk is not None:
                cache_key = (calc_name, hk)
                if cache_key in result_cache:
                    return result_cache[cache_key]

        result = resolve_calculator(conn, name=calc_name, kwargs=kwargs, meta=meta)
        if not result.get("ok"):
            raise ValueError(str(result.get("error") or f"call_calculator({calc_name}) 执行失败"))
        value = result.get("value")
        if cache_key is not None:
            result_cache[cache_key] = value
        return value

    token = inject_call_calculator(_formula_call_calculator)
    try:
        yield
    finally:
        reset_call_calculator(token)


def _graph_has_cycle(edges: List[Tuple[Node, Node]]) -> bool:
    adj: Dict[Node, List[Node]] = defaultdict(list)
    nodes: Set[Node] = set()
    for u, v in edges:
        adj[u].append(v)
        nodes.add(u)
        nodes.add(v)
    WHITE, GRAY, BLACK = 0, 1, 2
    color: Dict[Node, int] = {}

    def dfs(u: Node) -> bool:
        color[u] = GRAY
        for v in adj.get(u, []):
            cv = color.get(v, WHITE)
            if cv == GRAY:
                return True
            if cv == WHITE and dfs(v):
                return True
        color[u] = BLACK
        return False

    for n in nodes:
        if color.get(n, WHITE) == WHITE and dfs(n):
            return True
    return False


def assert_formula_dependency_acyclic(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    refs: Set[Tuple[str, str]],
) -> None:
    """边语义：from_table.from_column 依赖 to_table.to_column；若出现有向环则拒绝注册。"""
    edges: List[Tuple[Node, Node]] = []
    cur = conn.execute(
        """
        SELECT from_table, from_column, to_table, to_column FROM _dependency_graph
        WHERE NOT (from_table = ? AND from_column = ?)
        """,
        (table_name, column_name),
    )
    for r in cur.fetchall():
        edges.append(((r[0], r[1]), (r[2], r[3])))
    u0: Node = (table_name, column_name)
    for rt, rc in refs:
        edges.append((u0, (rt, rc)))
    if _graph_has_cycle(edges):
        raise ValueError("循环依赖：公式引用形成有向环")


def load_table_df(
    conn: sqlite3.Connection,
    table: str,
    columns: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """从业务表加载 DataFrame。

    - `columns=None`：保留旧行为 `SELECT *`。
    - 传入列白名单：仅 `SELECT` 给定列；`row_id` 自动补齐；不存在的列静默忽略
      （以保持公式引擎对缺列的报错语义不变 → 由后续 `eval_series` 抛出）。
    """
    cur = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?",
        (table,),
    )
    if not cur.fetchone():
        raise ValueError(f"未知表 {table}")
    if columns is None:
        return pd.read_sql_query(f'SELECT * FROM "{table}"', conn)
    try:
        existing = {row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')}
    except Exception:  # noqa: BLE001
        existing = set()
    wanted: List[str] = []
    seen: Set[str] = set()
    if "row_id" in existing:
        wanted.append("row_id")
        seen.add("row_id")
    for c in columns:
        if not c or c in seen:
            continue
        if c in existing:
            wanted.append(c)
            seen.add(c)
    if not wanted:
        # 没有任何已知列，回退全表加载以保留旧行为
        return pd.read_sql_query(f'SELECT * FROM "{table}"', conn)
    cols_sql = ", ".join(f'"{c}"' for c in wanted)
    return pd.read_sql_query(f'SELECT {cols_sql} FROM "{table}"', conn)


def _columns_for_table(
    refs: Set[Tuple[str, str]],
    table: str,
    *,
    target_column: Optional[str] = None,
    extra: Iterable[str] = (),
) -> Set[str]:
    """从公式引用集合中抽取某张表所需的列（不含 row_id，自动由 load_table_df 补）。"""
    cols: Set[str] = {c for t, c in refs if t == table}
    if target_column:
        cols.add(target_column)
    for c in extra:
        if c:
            cols.add(c)
    return cols


# ───────────────────────── 批量回写工具（A3） ─────────────────────────


def _batch_apply_updates(
    conn: sqlite3.Connection,
    *,
    table: str,
    column: str,
    pairs: Sequence[Tuple[Any, Any]],
) -> int:
    """`UPDATE table SET col=? WHERE row_id=?` 的 executemany 版本。

    `pairs` 形如 `[(value, row_id), ...]`。返回提交的元组数（不等于真正受影响行数）。
    """
    if not pairs:
        return 0
    conn.executemany(
        f'UPDATE "{table}" SET "{column}" = ? WHERE row_id = ?',
        list(pairs),
    )
    return len(pairs)


def _batch_apply_provenance(
    conn: sqlite3.Connection,
    *,
    table: str,
    column: str,
    row_ids: Sequence[Any],
    now: str,
    source_tag: str = "formula_computed",
) -> int:
    if not row_ids:
        return 0
    rows = [(table, str(rid), column, source_tag, now) for rid in row_ids]
    conn.executemany(
        """
        INSERT INTO _cell_provenance (table_name, row_id, column_name, source_tag, updated_at)
        VALUES (?,?,?,?,?)
        ON CONFLICT(table_name, row_id, column_name)
        DO UPDATE SET source_tag = excluded.source_tag, updated_at = excluded.updated_at
        """,
        rows,
    )
    return len(rows)


def _upsert_formula_provenance(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    row_id: str,
    column_name: str,
    now: str,
) -> None:
    conn.execute(
        """
        INSERT INTO _cell_provenance (table_name, row_id, column_name, source_tag, updated_at)
        VALUES (?,?,?,?,?)
        ON CONFLICT(table_name, row_id, column_name)
        DO UPDATE SET source_tag = excluded.source_tag, updated_at = excluded.updated_at
        """,
        (table_name, row_id, column_name, "formula_computed", now),
    )


def _load_constants(conn: sqlite3.Connection, names: Set[str]) -> Tuple[Dict[str, Any], List[str]]:
    """从 _constants 表批量取值；不存在的常量记入 missing。"""
    if not names:
        return {}, []
    out: Dict[str, Any] = {}
    missing: List[str] = []
    try:
        cur = conn.execute(
            f"SELECT name_en, value_json FROM _constants WHERE name_en IN ({','.join(['?'] * len(names))})",
            tuple(names),
        )
        rows = cur.fetchall()
    except sqlite3.OperationalError:
        return {}, list(names)
    found = set()
    for r in rows:
        try:
            import json as _json
            if isinstance(r, sqlite3.Row):
                name_en = r["name_en"]
                value_json = r["value_json"]
            else:
                name_en = r[0]
                value_json = r[1]
            out[str(name_en)] = _json.loads(value_json)
        except Exception:  # noqa: BLE001
            continue
        found.add(str(name_en))
    for n in names:
        if n not in found:
            missing.append(n)
    return out, missing


def execute_formula_on_column(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    *,
    level_column: Optional[str] = None,
    level_min: Optional[float] = None,
    level_max: Optional[float] = None,
) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT formula FROM _formula_registry WHERE table_name = ? AND column_name = ?",
        (table_name, column_name),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError("未注册公式")
    formula = row["formula"] if isinstance(row, sqlite3.Row) else row[0]
    formula = _rewrite_3d_dim_aliases(conn, table_name, formula)
    # 兼容历史存储里的 @T[col]/@this[col]
    formula = normalize_self_table_refs(formula, table_name)
    # ${name} 常量预替换
    const_names = parse_constant_refs(formula)
    if const_names:
        consts, missing = _load_constants(conn, const_names)
        if missing:
            raise ValueError(f"公式引用未注册常量：{', '.join(missing)}")
        formula, _miss = substitute_constants(formula, consts)
    refs = parse_formula_refs(formula)

    use_min_cols = perf_flag(conn, "use_min_column_load")
    use_batch_write = perf_flag(conn, "use_batch_writeback")

    # B1：DuckDB 路径（默认关闭）。命中白名单则直接走向量化执行；否则 fallback。
    duckdb_used = False
    duckdb_pairs: Optional[List[Tuple[Any, Any]]] = None
    try:
        from app.services import duckdb_compute as _dd

        if _dd.is_enabled(conn):
            duckdb_pairs = _dd.compute_column_via_duckdb(
                conn,
                table_name=table_name,
                column_name=column_name,
                formula=formula,
                level_column=level_column,
                level_min=level_min,
                level_max=level_max,
            )
            duckdb_used = True
    except Exception:  # noqa: BLE001
        # NotSupported / 未安装 / 任何执行错误 → 静默 fallback
        duckdb_used = False
        duckdb_pairs = None

    if duckdb_used and duckdb_pairs is not None:
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        # 检查列类型决定写入格式
        col_sql_type = "REAL"
        try:
            for r in conn.execute(f'PRAGMA table_info("{table_name}")'):
                name = r["name"] if isinstance(r, sqlite3.Row) else r[1]
                if name == column_name:
                    raw_type = r["type"] if isinstance(r, sqlite3.Row) else r[2]
                    col_sql_type = str(raw_type or "REAL").upper()
                    break
        except Exception:
            pass
        is_text_col = col_sql_type == "TEXT"
        normalized: List[Tuple[Any, Any]] = []
        for v, rid in duckdb_pairs:
            db_val = (str(v) if v is not None else None) if is_text_col else (
                float(v) if v is not None else None
            )
            normalized.append((db_val, rid))
        with PerfTimer(
            conn,
            op="execute_formula_on_column",
            table_name=table_name,
            column_name=column_name,
            extra={"engine": "duckdb"},
        ) as t2:
            _batch_apply_updates(conn, table=table_name, column=column_name, pairs=normalized)
            _batch_apply_provenance(
                conn,
                table=table_name,
                column=column_name,
                row_ids=[rid for _, rid in normalized],
                now=now,
            )
            conn.commit()
            t2.set_rows(len(normalized))
        # rows_total 在 DuckDB 路径下使用结果集长度；与 Pandas 路径返回结构兼容
        return {"ok": True, "rows_updated": len(normalized), "rows_total": len(normalized), "engine": "duckdb"}

    with PerfTimer(
        conn,
        op="execute_formula_on_column",
        table_name=table_name,
        column_name=column_name,
        extra={
            "use_min_cols": use_min_cols,
            "use_batch_write": use_batch_write,
            "ref_tables": sorted({t for t, _ in refs}),
        },
    ) as timer:
        # 主表加载：附加 level 列以便 mask 过滤
        main_extra: List[str] = []
        if level_column:
            main_extra.append(str(level_column).strip())
        elif level_min is not None or level_max is not None:
            main_extra.append("level")
        if use_min_cols:
            main_cols = _columns_for_table(
                refs, table_name, target_column=column_name, extra=main_extra
            )
            frames: Dict[str, pd.DataFrame] = {table_name: load_table_df(conn, table_name, main_cols)}
        else:
            frames = {table_name: load_table_df(conn, table_name)}
        for rt, _rc in refs:
            if rt in frames:
                continue
            if use_min_cols:
                ref_cols = _columns_for_table(refs, rt)
                frames[rt] = load_table_df(conn, rt, ref_cols)
            else:
                frames[rt] = load_table_df(conn, rt)
        try:
            formula = preprocess_formula(formula)
            with _formula_call_calculator_context(conn):
                series = eval_series(formula, frames)
        except Exception as e:  # noqa: BLE001
            raise ValueError(str(e)) from e
        df = frames[table_name]
        if len(series) != len(df):
            raise ValueError("公式结果行数与目标表不一致")
        col = column_name
        if (level_min is not None or level_max is not None) and (level_min is None or level_max is None):
            raise ValueError("level_range 需同时提供 level_min 与 level_max")
        mask: Optional[pd.Series] = None
        if level_min is not None and level_max is not None:
            raw_level_column = str(level_column).strip() if level_column is not None else ""
            lc = raw_level_column or ("level" if "level" in df.columns else "row_id")
            if lc not in df.columns:
                raise ValueError(f"等级列 {lc} 不存在")
            lv = pd.to_numeric(df[lc], errors="coerce")
            mask = (lv >= float(level_min)) & (lv <= float(level_max))
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        # 检查列类型，TEXT 列不强制 float 转换
        col_sql_type = "REAL"
        try:
            for r in conn.execute(f'PRAGMA table_info("{table_name}")'):
                name = r["name"] if isinstance(r, sqlite3.Row) else r[1]
                if name == col:
                    raw_type = r["type"] if isinstance(r, sqlite3.Row) else r[2]
                    col_sql_type = str(raw_type or "REAL").upper()
                    break
        except Exception:
            pass
        is_text_col = col_sql_type == "TEXT"

        # 收集本次需要写入的 (value, row_id) 对，统一批量回写
        pending: List[Tuple[Any, Any]] = []
        row_ids = df["row_id"].tolist()
        for i, rid in enumerate(row_ids):
            if mask is not None and not bool(mask.iloc[i]):
                continue
            val = series.iloc[i]
            db_val = str(val) if is_text_col else float(val)
            pending.append((db_val, rid))

        if use_batch_write:
            _batch_apply_updates(conn, table=table_name, column=col, pairs=pending)
            _batch_apply_provenance(
                conn,
                table=table_name,
                column=col,
                row_ids=[rid for _, rid in pending],
                now=now,
            )
        else:
            for db_val, rid in pending:
                conn.execute(
                    f'UPDATE "{table_name}" SET "{col}" = ? WHERE row_id = ?',
                    (db_val, rid),
                )
                _upsert_formula_provenance(
                    conn, table_name=table_name, row_id=str(rid), column_name=col, now=now,
                )
        updated = len(pending)
        conn.commit()
        timer.set_rows(updated)
        timer.add_extra(rows_total=len(df))
        return {"ok": True, "rows_updated": updated, "rows_total": len(df)}


def register_formula(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    formula: str,
    *,
    defer: bool = False,
) -> Dict[str, Any]:
    formula = _rewrite_3d_dim_aliases(conn, table_name, formula)
    # 把 @T[col]/@this[col] 中的 T/this 统一为当前表名，避免引擎报"未加载表 T"
    formula = normalize_self_table_refs(formula, table_name)
    refs: Set[Tuple[str, str]] = parse_formula_refs(formula)
    cur = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    if not cur.fetchone():
        raise ValueError("目标表不存在")
    assert_formula_dependency_acyclic(conn, table_name, column_name, refs)
    conn.execute(
        """
        INSERT INTO _formula_registry (table_name, column_name, formula)
        VALUES (?,?,?)
        ON CONFLICT(table_name, column_name) DO UPDATE SET formula = excluded.formula
        """,
        (table_name, column_name, formula),
    )
    conn.execute(
        "DELETE FROM _dependency_graph WHERE from_table = ? AND from_column = ?",
        (table_name, column_name),
    )
    for rt, rc in refs:
        conn.execute(
            """
            INSERT INTO _dependency_graph (from_table, from_column, to_table, to_column, edge_type)
            VALUES (?,?,?,?, 'formula')
            """,
            (table_name, column_name, rt, rc),
        )
    conn.commit()
    # 注册成功后尝试自动执行一次（除非 defer=True）。失败不影响注册结果。
    auto_executed: Optional[Dict[str, Any]] = None
    auto_error: Optional[str] = None
    if not defer:
        try:
            auto_executed = execute_formula_on_column(conn, table_name, column_name)
        except Exception as e:  # noqa: BLE001
            auto_error = str(e)
    out: Dict[str, Any] = {"ok": True, "refs": [{"table": t, "column": c} for t, c in sorted(refs)]}
    if auto_executed is not None:
        out["auto_executed"] = auto_executed
    if auto_error is not None:
        out["auto_execute_error"] = auto_error
    # constants-gate: 公式中如有浮点字面量（非 0/1/小整数），提示先 const_register
    try:
        import re as _re
        suspect: List[str] = []
        # 抽出所有数值字面量（含小数点 或 >= 10 的整数）
        for m in _re.findall(r"(?<![\w.])-?\d+(?:\.\d+)?", formula or ""):
            try:
                v = float(m)
            except ValueError:
                continue
            # 0、1、-1、小于 10 的纯整数视为索引/小常量；其余建议命名常数
            if "." in m or abs(v) >= 10:
                suspect.append(m)
        if suspect:
            out["warnings"] = [
                f"公式包含字面量 {suspect[:5]}{'...' if len(suspect) > 5 else ''}，"
                "建议先调用 const_register 命名后用 ${name} 引用，避免魔法数。"
            ]
    except Exception:  # noqa: BLE001
        pass
    return out


def recalculate_downstream(conn: sqlite3.Connection, table_name: str, column_name: str) -> Dict[str, Any]:
    """重算所有直接或间接依赖于 (table_name, column_name) 的下游公式。

    `perf.use_dag_recalc` 开启（默认）时走 DAG 批量入口；关闭时走旧的"直接下游一次性"行为。
    """
    if perf_flag(conn, "use_dag_recalc"):
        return recalculate_downstream_dag(conn, [(table_name, column_name)])

    # 旧路径：仅一跳直接下游
    cur = conn.execute(
        """
        SELECT DISTINCT from_table, from_column FROM _dependency_graph
        WHERE to_table = ? AND to_column = ?
        """,
        (table_name, column_name),
    )
    rows = cur.fetchall()
    jobs = [
        (r["from_table"], r["from_column"]) if isinstance(r, sqlite3.Row) else (r[0], r[1])
        for r in rows
    ]
    done: List[Dict[str, str]] = []
    errors: List[str] = []
    for ft, fc in jobs:
        try:
            execute_formula_on_column(conn, ft, fc)
            done.append({"table": ft, "column": fc})
        except ValueError as e:
            errors.append(f"{ft}.{fc}: {e}")
    return {"executed": done, "errors": errors}


def _execute_node(conn: sqlite3.Connection, table: str, column: str) -> None:
    """根据公式类型选择执行入口（sql/row/row_template）。"""
    cur = conn.execute(
        "SELECT formula_type FROM _formula_registry WHERE table_name = ? AND column_name = ?",
        (table, column),
    )
    r = cur.fetchone()
    if not r:
        raise ValueError(f"未注册公式：{table}.{column}")
    ftype = (r["formula_type"] if isinstance(r, sqlite3.Row) else r[0]) or "sql"
    if ftype == "sql":
        execute_formula_on_column(conn, table, column)
    else:
        execute_row_formula(conn, table, column)


def recalculate_downstream_dag(
    conn: sqlite3.Connection,
    seeds: Sequence[Tuple[str, str]],
) -> Dict[str, Any]:
    """DAG 批量重算：

    1. 反向（from→to 即"我依赖谁"）BFS 从 seeds 出发，**收集**所有受影响节点
       —— 即所有 `from_table.from_column` 直接或间接依赖任一 seed 的列。
       因此实际遍历方向是按 `_dependency_graph` 的 edge 找 `to == seed` 的 from-side。
    2. 对受影响节点收集子图 + 拓扑排序（Kahn）；环已在注册时拒绝，但带兜底检测。
    3. 按拓扑顺序逐个执行，每个节点仅执行一次；若某节点失败，则依赖它的下游节点
       标记为 blocked，避免基于过期上游值继续写回。
    """
    seed_set: Set[Node] = {(t, c) for t, c in seeds}
    if not seed_set:
        return {"executed": [], "errors": [], "skipped": []}

    # Step 1: BFS 反向收集所有受影响节点（不含 seeds 本身）
    affected: Set[Node] = set()
    queue: deque = deque(seed_set)
    visited_to: Set[Node] = set()
    while queue:
        target = queue.popleft()
        if target in visited_to:
            continue
        visited_to.add(target)
        cur = conn.execute(
            """
            SELECT DISTINCT from_table, from_column FROM _dependency_graph
            WHERE to_table = ? AND to_column = ?
            """,
            (target[0], target[1]),
        )
        for r in cur.fetchall():
            ft = r["from_table"] if isinstance(r, sqlite3.Row) else r[0]
            fc = r["from_column"] if isinstance(r, sqlite3.Row) else r[1]
            node = (ft, fc)
            if node in affected or node in seed_set:
                continue
            affected.add(node)
            queue.append(node)

    if not affected:
        return {"executed": [], "errors": [], "skipped": []}

    # Step 2: 在 affected 子图内构造依赖边
    nodes = set(affected)
    indeg: Dict[Node, int] = {n: 0 for n in nodes}
    forward: Dict[Node, List[Node]] = defaultdict(list)
    deps: Dict[Node, Set[Node]] = defaultdict(set)
    for n in nodes:
        cur = conn.execute(
            """
            SELECT DISTINCT to_table, to_column FROM _dependency_graph
            WHERE from_table = ? AND from_column = ?
            """,
            (n[0], n[1]),
        )
        for r in cur.fetchall():
            tt = r["to_table"] if isinstance(r, sqlite3.Row) else r[0]
            tc = r["to_column"] if isinstance(r, sqlite3.Row) else r[1]
            dep = (tt, tc)
            # 仅在 dep 也属于 affected 时纳入子图（seeds 视为已就绪）
            if dep in nodes:
                # 边方向：dep → n （n 依赖 dep，所以先算 dep）
                forward[dep].append(n)
                deps[n].add(dep)
                indeg[n] += 1

    # Step 3: Kahn 拓扑
    ready: deque = deque(sorted([n for n in nodes if indeg[n] == 0]))
    order: List[Node] = []
    indeg_local = dict(indeg)
    while ready:
        n = ready.popleft()
        order.append(n)
        for m in forward.get(n, []):
            indeg_local[m] -= 1
            if indeg_local[m] == 0:
                ready.append(m)

    cycle_nodes = [f"{t}.{c}" for (t, c), d in indeg_local.items() if d > 0]
    if cycle_nodes:
        # 兜底：注册时已拒环，理论上不会出现；若出现，把环内节点跳过
        order_set = set(order)
        skipped_cycle = [n for n in nodes if n not in order_set]
    else:
        skipped_cycle = []

    # Step 4: 按层执行
    executed: List[Dict[str, str]] = []
    errors: List[str] = []
    skipped: List[Dict[str, Any]] = []
    failed_nodes: Set[Node] = set()
    blocked_nodes: Set[Node] = set()
    timer = PerfTimer(
        conn,
        op="recalculate_downstream_dag",
        extra={
            "seeds": [f"{t}.{c}" for t, c in sorted(seed_set)],
            "n_affected": len(nodes),
            "cycle_skipped": len(skipped_cycle),
        },
    )
    with timer:
        timer.set_rows(len(order))
        for t, c in order:
            node = (t, c)
            blocked_by = sorted(
                f"{dt}.{dc}"
                for dt, dc in deps.get(node, set())
                if (dt, dc) in failed_nodes or (dt, dc) in blocked_nodes
            )
            if blocked_by:
                blocked_nodes.add(node)
                skipped.append(
                    {
                        "table": t,
                        "column": c,
                        "reason": "blocked_by_failed_dependency",
                        "blocked_by": blocked_by,
                    }
                )
                continue
            try:
                _execute_node(conn, t, c)
                executed.append({"table": t, "column": c})
            except Exception as e:  # noqa: BLE001
                failed_nodes.add(node)
                errors.append(f"{t}.{c}: {e}")
        timer.add_extra(errors=len(errors), blocked=len(skipped))

    out: Dict[str, Any] = {"executed": executed, "errors": errors, "skipped": skipped}
    if skipped_cycle:
        out["cycle_skipped"] = [f"{t}.{c}" for t, c in skipped_cycle]
    return out


# ────────────────────────── 同行列公式（row / row_template） ──────────────────────────


def register_row_formula(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    raw_formula: str,
) -> Dict[str, Any]:
    """注册同行列公式（@col_name 语法）。
    所有引用列均在表内 → formula_type='row'，立即计算所有行。
    存在外部参数 → formula_type='row_template'，仅记录不计算。
    """
    cur = conn.execute("SELECT 1 FROM _table_registry WHERE table_name = ?", (table_name,))
    if not cur.fetchone():
        raise ValueError(f"表 {table_name} 不存在")
    raw_formula = _rewrite_3d_dim_aliases(conn, table_name, raw_formula)

    use_min_cols = perf_flag(conn, "use_min_column_load")
    use_batch_write = perf_flag(conn, "use_batch_writeback")

    # 先按"全列"探测一次结构，便于判定 external_refs；若启用最小列加载则随后只取需要的列
    if use_min_cols:
        try:
            existing_cols = {row[1] for row in conn.execute(f'PRAGMA table_info("{table_name}")')}
            existing_cols.discard("row_id")
        except Exception:  # noqa: BLE001
            existing_cols = set()
        available_cols = existing_cols
        refs = parse_row_refs(raw_formula)
        wanted = (refs & existing_cols) | {column_name}
        df = load_table_df(conn, table_name, wanted)
    else:
        df = load_table_df(conn, table_name)
        available_cols = set(df.columns) - {"row_id"}
        refs = parse_row_refs(raw_formula)
    external_refs = refs - available_cols
    internal_refs: Set[Tuple[str, str]] = {
        (table_name, ref)
        for ref in refs
        if ref in available_cols
    }
    const_names = parse_constant_refs(raw_formula)
    constants, missing_consts = _load_constants(conn, const_names)
    formula_for_compute = raw_formula
    if const_names and not missing_consts:
        formula_for_compute, missing_after_substitute = substitute_constants(raw_formula, constants)
        missing_consts = list(set(missing_consts) | set(missing_after_substitute))
    is_computable = len(external_refs) == 0 and len(missing_consts) == 0
    formula_type = "row" if is_computable else "row_template"
    assert_formula_dependency_acyclic(conn, table_name, column_name, internal_refs)

    conn.execute(
        """
        INSERT INTO _formula_registry (table_name, column_name, formula, formula_type)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(table_name, column_name) DO UPDATE SET
            formula = excluded.formula,
            formula_type = excluded.formula_type
        """,
        (table_name, column_name, raw_formula, formula_type),
    )
    conn.execute(
        "DELETE FROM _dependency_graph WHERE from_table = ? AND from_column = ?",
        (table_name, column_name),
    )
    for rt, rc in sorted(internal_refs):
        conn.execute(
            """
            INSERT INTO _dependency_graph (from_table, from_column, to_table, to_column, edge_type)
            VALUES (?,?,?,?, 'row_formula')
            """,
            (table_name, column_name, rt, rc),
        )

    computed_count = 0
    warnings: List[str] = []

    if is_computable and len(df) > 0:
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        pending: List[Tuple[Any, Any]] = []
        with _formula_call_calculator_context(conn):
            for _, row_data in df.iterrows():
                row_dict: Dict[str, Any] = {c: row_data[c] for c in df.columns}
                val, missing = eval_row_formula(formula_for_compute, row_dict, available_cols)
                if missing:
                    warnings.append(f"行 {row_dict.get('row_id')}: 缺少 {missing}")
                    continue
                try:
                    val = round(float(val), 6) if val is not None else None
                except (TypeError, ValueError):
                    pass
                pending.append((val, str(row_dict["row_id"])))
                computed_count += 1
        if use_batch_write:
            _batch_apply_updates(conn, table=table_name, column=column_name, pairs=pending)
            _batch_apply_provenance(
                conn,
                table=table_name,
                column=column_name,
                row_ids=[rid for _, rid in pending],
                now=now,
            )
        else:
            for val, rid in pending:
                conn.execute(
                    f'UPDATE "{table_name}" SET "{column_name}" = ? WHERE row_id = ?',
                    (val, rid),
                )
                _upsert_formula_provenance(
                    conn, table_name=table_name, row_id=rid, column_name=column_name, now=now,
                )
        conn.commit()
    else:
        warnings = [f"外部参数 {r} 不在表内（运行时模板，需外部系统计算）" for r in sorted(external_refs)]
        warnings.extend(f"公式引用未注册常量：{r}" for r in sorted(set(missing_consts)))
        conn.commit()

    return {
        "ok": True,
        "formula_type": formula_type,
        "is_computable": is_computable,
        "external_refs": sorted(external_refs),
        "computed_rows": computed_count,
        "warnings": warnings,
    }


def execute_row_formula(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
) -> Dict[str, Any]:
    """重新执行已注册的同行公式，计算所有行。"""
    cur = conn.execute(
        "SELECT formula, formula_type FROM _formula_registry WHERE table_name = ? AND column_name = ?",
        (table_name, column_name),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError("未注册公式")

    raw_formula = row[0]
    formula_type = row[1] if row[1] else "sql"
    raw_formula = _rewrite_3d_dim_aliases(conn, table_name, raw_formula)

    if formula_type == "sql":
        return execute_formula_on_column(conn, table_name, column_name)

    use_min_cols = perf_flag(conn, "use_min_column_load")
    use_batch_write = perf_flag(conn, "use_batch_writeback")

    if use_min_cols:
        try:
            existing_cols = {row[1] for row in conn.execute(f'PRAGMA table_info("{table_name}")')}
            existing_cols.discard("row_id")
        except Exception:  # noqa: BLE001
            existing_cols = set()
        available_cols = existing_cols
        refs = parse_row_refs(raw_formula)
        wanted = (refs & existing_cols) | {column_name}
        df = load_table_df(conn, table_name, wanted)
    else:
        df = load_table_df(conn, table_name)
        available_cols = set(df.columns) - {"row_id"}
        refs = parse_row_refs(raw_formula)
    external_refs = refs - available_cols
    const_names = parse_constant_refs(raw_formula)
    constants, missing_consts = _load_constants(conn, const_names)
    formula_for_compute = raw_formula
    if const_names and not missing_consts:
        formula_for_compute, missing_after_substitute = substitute_constants(raw_formula, constants)
        missing_consts = list(set(missing_consts) | set(missing_after_substitute))

    if external_refs or missing_consts:
        conn.execute(
            """
            UPDATE _formula_registry
            SET formula_type = 'row_template'
            WHERE table_name = ? AND column_name = ?
            """,
            (table_name, column_name),
        )
        conn.commit()
        errors: List[str] = []
        if external_refs:
            errors.extend(f"缺少同行列引用：{r}" for r in sorted(external_refs))
        if missing_consts:
            errors.extend(f"缺少常量：{r}" for r in sorted(set(missing_consts)))
        return {"ok": False, "rows_updated": 0, "rows_total": len(df), "errors": errors}

    if formula_type != "row":
        conn.execute(
            """
            UPDATE _formula_registry
            SET formula_type = 'row'
            WHERE table_name = ? AND column_name = ?
            """,
            (table_name, column_name),
        )
        conn.commit()

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    col_sql_type = "REAL"
    try:
        for r in conn.execute(f'PRAGMA table_info("{table_name}")'):
            name = r["name"] if isinstance(r, sqlite3.Row) else r[1]
            if name == column_name:
                raw_type = r["type"] if isinstance(r, sqlite3.Row) else r[2]
                col_sql_type = str(raw_type or "REAL").upper()
                break
    except Exception:
        pass
    is_text_col = col_sql_type == "TEXT"
    updated = 0
    errors: List[str] = []

    pending: List[Tuple[Any, Any]] = []
    with _formula_call_calculator_context(conn):
        for _, row_data in df.iterrows():
            row_dict: Dict[str, Any] = {c: row_data[c] for c in df.columns}
            val, missing = eval_row_formula(formula_for_compute, row_dict, available_cols)
            if missing:
                errors.append(f"行 {row_dict.get('row_id')}: 缺少参数 {missing}")
                continue
            try:
                val = str(val) if is_text_col else round(float(val), 6) if val is not None else None
            except (TypeError, ValueError):
                pass
            pending.append((val, str(row_dict["row_id"])))
            updated += 1
    if use_batch_write:
        _batch_apply_updates(conn, table=table_name, column=column_name, pairs=pending)
        _batch_apply_provenance(
            conn,
            table=table_name,
            column=column_name,
            row_ids=[rid for _, rid in pending],
            now=now,
        )
    else:
        for val, rid in pending:
            conn.execute(
                f'UPDATE "{table_name}" SET "{column_name}" = ? WHERE row_id = ?',
                (val, rid),
            )
            _upsert_formula_provenance(
                conn, table_name=table_name, row_id=rid, column_name=column_name, now=now,
            )
    conn.commit()
    return {"ok": True, "rows_updated": updated, "rows_total": len(df), "errors": errors}


def delete_column_formula(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
) -> Dict[str, Any]:
    """从注册表删除列公式（SQL 或 row 类型均可删）。"""
    conn.execute(
        "DELETE FROM _formula_registry WHERE table_name = ? AND column_name = ?",
        (table_name, column_name),
    )
    conn.execute(
        "DELETE FROM _dependency_graph WHERE from_table = ? AND from_column = ?",
        (table_name, column_name),
    )
    conn.commit()
    return {"ok": True}


def recalculate_row_formulas_for_table(
    conn: sqlite3.Connection,
    table_name: str,
) -> Dict[str, Any]:
    """重新计算表内所有同行公式；row_template 若常量已补齐也会尝试转正。"""
    cur = conn.execute(
        "SELECT column_name FROM _formula_registry WHERE table_name = ? AND formula_type IN ('row', 'row_template')",
        (table_name,),
    )
    cols = [r[0] for r in cur.fetchall()]
    done: List[str] = []
    errors: List[str] = []
    for c in cols:
        try:
            execute_row_formula(conn, table_name, c)
            done.append(c)
        except Exception as e:  # noqa: BLE001
            errors.append(f"{c}: {e}")
    return {"recalculated": done, "errors": errors}
