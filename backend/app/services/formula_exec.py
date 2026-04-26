"""公式执行与注册（供 /compute 与 Agent 工具复用）。"""

from __future__ import annotations

import sqlite3
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from app.services.formula_engine import (
    eval_row_formula,
    eval_series,
    parse_constant_refs,
    parse_formula_refs,
    parse_row_refs,
    substitute_constants,
)

Node = Tuple[str, str]


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


def load_table_df(conn: sqlite3.Connection, table: str) -> pd.DataFrame:
    cur = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?",
        (table,),
    )
    if not cur.fetchone():
        raise ValueError(f"未知表 {table}")
    return pd.read_sql_query(f'SELECT * FROM "{table}"', conn)


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

            out[r["name_en"]] = _json.loads(r["value_json"])
        except Exception:  # noqa: BLE001
            continue
        found.add(r["name_en"])
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
    formula = row["formula"]
    # ${name} 常量预替换
    const_names = parse_constant_refs(formula)
    if const_names:
        consts, missing = _load_constants(conn, const_names)
        if missing:
            raise ValueError(f"公式引用未注册常量：{', '.join(missing)}")
        formula, _miss = substitute_constants(formula, consts)
    refs = parse_formula_refs(formula)
    frames: Dict[str, pd.DataFrame] = {table_name: load_table_df(conn, table_name)}
    for rt, _rc in refs:
        if rt not in frames:
            frames[rt] = load_table_df(conn, rt)
    try:
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
        lc = level_column or "row_id"
        if lc not in df.columns:
            raise ValueError(f"等级列 {lc} 不存在")
        lv = pd.to_numeric(df[lc], errors="coerce")
        mask = (lv >= float(level_min)) & (lv <= float(level_max))
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    updated = 0
    for i, rid in enumerate(df["row_id"].tolist()):
        if mask is not None and not bool(mask.iloc[i]):
            continue
        val = series.iloc[i]
        conn.execute(
            f'UPDATE "{table_name}" SET "{col}" = ? WHERE row_id = ?',
            (float(val), rid),
        )
        _upsert_formula_provenance(conn, table_name=table_name, row_id=str(rid), column_name=col, now=now)
        updated += 1
    conn.commit()
    return {"ok": True, "rows_updated": updated, "rows_total": len(df)}


def register_formula(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    formula: str,
    *,
    defer: bool = False,
) -> Dict[str, Any]:
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
    cur = conn.execute(
        """
        SELECT DISTINCT from_table, from_column FROM _dependency_graph
        WHERE to_table = ? AND to_column = ?
        """,
        (table_name, column_name),
    )
    jobs = [(r["from_table"], r["from_column"]) for r in cur.fetchall()]
    done: List[Dict[str, str]] = []
    errors: List[str] = []
    for ft, fc in jobs:
        try:
            execute_formula_on_column(conn, ft, fc)
            done.append({"table": ft, "column": fc})
        except ValueError as e:
            errors.append(f"{ft}.{fc}: {e}")
    return {"executed": done, "errors": errors}


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

    df = load_table_df(conn, table_name)
    available_cols: Set[str] = set(df.columns) - {"row_id"}

    refs = parse_row_refs(raw_formula)
    external_refs = refs - available_cols
    is_computable = len(external_refs) == 0
    formula_type = "row" if is_computable else "row_template"

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

    computed_count = 0
    warnings: List[str] = []

    if is_computable and len(df) > 0:
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        for _, row_data in df.iterrows():
            row_dict: Dict[str, Any] = {c: row_data[c] for c in df.columns}
            val, missing = eval_row_formula(raw_formula, row_dict, available_cols)
            if missing:
                warnings.append(f"行 {row_dict.get('row_id')}: 缺少 {missing}")
                continue
            try:
                val = round(float(val), 6) if val is not None else None
            except (TypeError, ValueError):
                pass
            conn.execute(
                f'UPDATE "{table_name}" SET "{column_name}" = ? WHERE row_id = ?',
                (val, str(row_dict["row_id"])),
            )
            _upsert_formula_provenance(
                conn,
                table_name=table_name,
                row_id=str(row_dict["row_id"]),
                column_name=column_name,
                now=now,
            )
            computed_count += 1
        conn.commit()
    else:
        warnings = [f"外部参数 {r} 不在表内（运行时模板，需外部系统计算）" for r in sorted(external_refs)]
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

    formula = row[0]
    formula_type = row[1] if row[1] else "sql"

    if formula_type == "sql":
        return execute_formula_on_column(conn, table_name, column_name)

    df = load_table_df(conn, table_name)
    available_cols: Set[str] = set(df.columns) - {"row_id"}
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    updated = 0
    errors: List[str] = []

    for _, row_data in df.iterrows():
        row_dict: Dict[str, Any] = {c: row_data[c] for c in df.columns}
        val, missing = eval_row_formula(formula, row_dict, available_cols)
        if missing:
            errors.append(f"行 {row_dict.get('row_id')}: 缺少参数 {missing}")
            continue
        try:
            val = round(float(val), 6) if val is not None else None
        except (TypeError, ValueError):
            pass
        conn.execute(
            f'UPDATE "{table_name}" SET "{column_name}" = ? WHERE row_id = ?',
            (val, str(row_dict["row_id"])),
        )
        _upsert_formula_provenance(
            conn,
            table_name=table_name,
            row_id=str(row_dict["row_id"]),
            column_name=column_name,
            now=now,
        )
        updated += 1
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
    conn.commit()
    return {"ok": True}


def recalculate_row_formulas_for_table(
    conn: sqlite3.Connection,
    table_name: str,
) -> Dict[str, Any]:
    """重新计算表内所有 row 类型公式（不含 row_template）。"""
    cur = conn.execute(
        "SELECT column_name FROM _formula_registry WHERE table_name = ? AND formula_type = 'row'",
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

