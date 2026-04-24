"""公式执行与注册（供 /compute 与 Agent 工具复用）。"""

from __future__ import annotations

import sqlite3
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from app.services.formula_engine import eval_series, parse_formula_refs

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


def register_formula(conn: sqlite3.Connection, table_name: str, column_name: str, formula: str) -> Dict[str, Any]:
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
    return {"ok": True, "refs": [{"table": t, "column": c} for t, c in sorted(refs)]}


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
