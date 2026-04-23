"""公式与算法 API（/compute）。"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.deps import ProjectDB, get_project_read, get_project_write
from app.services import algorithms
from app.services.formula_engine import eval_series, parse_formula_refs

router = APIRouter(prefix="/compute", tags=["compute"])


def _load_table_df(conn: sqlite3.Connection, table: str) -> pd.DataFrame:
    cur = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?",
        (table,),
    )
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail=f"未知表 {table}")
    return pd.read_sql_query(f'SELECT * FROM "{table}"', conn)


def _execute_formula_core(conn: sqlite3.Connection, table_name: str, column_name: str) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT formula FROM _formula_registry WHERE table_name = ? AND column_name = ?",
        (table_name, column_name),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="未注册公式")
    formula = row["formula"]
    refs = parse_formula_refs(formula)
    frames: Dict[str, pd.DataFrame] = {table_name: _load_table_df(conn, table_name)}
    for rt, _rc in refs:
        if rt not in frames:
            frames[rt] = _load_table_df(conn, rt)
    try:
        series = eval_series(formula, frames)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(e)) from e
    df = frames[table_name]
    if len(series) != len(df):
        raise HTTPException(status_code=400, detail="公式结果行数与目标表不一致")
    col = column_name
    for i, rid in enumerate(df["row_id"].tolist()):
        val = series.iloc[i]
        conn.execute(
            f'UPDATE "{table_name}" SET "{col}" = ? WHERE row_id = ?',
            (float(val), rid),
        )
    conn.commit()
    return {"ok": True, "rows": len(df)}


class RegisterFormulaBody(BaseModel):
    table_name: str
    column_name: str
    formula: str


@router.post("/formulas/register")
def register_formula(body: RegisterFormulaBody, p: ProjectDB = Depends(get_project_write)):
    conn = p.conn
    refs = parse_formula_refs(body.formula)
    cur = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?",
        (body.table_name,),
    )
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="目标表不存在")
    conn.execute(
        """
        INSERT INTO _formula_registry (table_name, column_name, formula)
        VALUES (?,?,?)
        ON CONFLICT(table_name, column_name) DO UPDATE SET formula = excluded.formula
        """,
        (body.table_name, body.column_name, body.formula),
    )
    conn.execute(
        "DELETE FROM _dependency_graph WHERE from_table = ? AND from_column = ?",
        (body.table_name, body.column_name),
    )
    for rt, rc in refs:
        conn.execute(
            """
            INSERT INTO _dependency_graph (from_table, from_column, to_table, to_column, edge_type)
            VALUES (?,?,?,?, 'formula')
            """,
            (body.table_name, body.column_name, rt, rc),
        )
    conn.commit()
    return {"ok": True, "refs": [{"table": t, "column": c} for t, c in sorted(refs)]}


class ExecuteFormulaBody(BaseModel):
    table_name: str
    column_name: str


@router.post("/formulas/execute")
def execute_formula(
    body: ExecuteFormulaBody,
    p: ProjectDB = Depends(get_project_write),
    level_range: Optional[str] = Query(None, description="占位"),
):
    del level_range
    return _execute_formula_core(p.conn, body.table_name, body.column_name)


class CallAlgoBody(BaseModel):
    api_name: str
    params: Dict[str, Any] = Field(default_factory=dict)


@router.get("/algorithm-apis")
def list_algorithm_apis():
    return {"apis": algorithms.list_apis()}


@router.post("/algorithm-apis/call")
def call_algorithm_api(body: CallAlgoBody, p: ProjectDB = Depends(get_project_write)):
    del p
    try:
        out = algorithms.call_api(body.api_name, body.params)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"result": out}


@router.post("/recalculate-downstream")
def recalculate_downstream(
    table_name: str = Query(...),
    column_name: str = Query(...),
    p: ProjectDB = Depends(get_project_write),
):
    conn = p.conn
    cur = conn.execute(
        """
        SELECT DISTINCT from_table, from_column FROM _dependency_graph
        WHERE to_table = ? AND to_column = ?
        """,
        (table_name, column_name),
    )
    jobs = [(r["from_table"], r["from_column"]) for r in cur.fetchall()]
    done: List[Dict[str, str]] = []
    for ft, fc in jobs:
        _execute_formula_core(conn, ft, fc)
        done.append({"table": ft, "column": fc})
    return {"executed": done}
