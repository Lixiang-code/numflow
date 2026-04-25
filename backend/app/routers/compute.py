"""公式与算法 API（/compute）。"""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.deps import ProjectDB, get_project_read, get_project_write
from app.services import algorithms
from app.services.formula_exec import (
    delete_column_formula as delete_column_formula_svc,
    execute_formula_on_column,
    execute_row_formula as execute_row_formula_svc,
    recalculate_downstream as recalc_downstream_svc,
    recalculate_row_formulas_for_table as recalc_row_svc,
    register_formula as register_formula_svc,
    register_row_formula as register_row_formula_svc,
)

router = APIRouter(prefix="/compute", tags=["compute"])


def _http_from_formula_error(e: ValueError) -> HTTPException:
    msg = str(e)
    if msg.startswith("未知表") or msg in ("未注册公式", "目标表不存在"):
        return HTTPException(status_code=404, detail=msg)
    return HTTPException(status_code=400, detail=msg)


class RegisterFormulaBody(BaseModel):
    table_name: str
    column_name: str
    formula: str


@router.post("/formulas/register")
def register_formula(body: RegisterFormulaBody, p: ProjectDB = Depends(get_project_write)):
    try:
        return register_formula_svc(p.conn, body.table_name, body.column_name, body.formula)
    except ValueError as e:
        raise _http_from_formula_error(e) from e


class ExecuteFormulaBody(BaseModel):
    table_name: str
    column_name: str
    level_column: Optional[str] = None
    level_min: Optional[float] = None
    level_max: Optional[float] = None


@router.post("/formulas/execute")
def execute_formula(
    body: ExecuteFormulaBody,
    p: ProjectDB = Depends(get_project_write),
):
    try:
        return execute_formula_on_column(
            p.conn,
            body.table_name,
            body.column_name,
            level_column=body.level_column,
            level_min=body.level_min,
            level_max=body.level_max,
        )
    except ValueError as e:
        raise _http_from_formula_error(e) from e


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
    return recalc_downstream_svc(p.conn, table_name, column_name)


class ColumnFormulaBody(BaseModel):
    table_name: str = Field(min_length=1)
    column_name: str = Field(min_length=1)
    formula: str = Field(min_length=1)


@router.put("/column-formula")
def put_column_formula(body: ColumnFormulaBody, p: ProjectDB = Depends(get_project_write)):
    """注册或更新列公式（@col_name 同行引用语法）。"""
    try:
        return register_row_formula_svc(p.conn, body.table_name, body.column_name, body.formula)
    except ValueError as e:
        raise _http_from_formula_error(e) from e


@router.delete("/column-formula")
def delete_column_formula(
    table_name: str = Query(...),
    column_name: str = Query(...),
    p: ProjectDB = Depends(get_project_write),
):
    """删除列公式注册（不清空已写入的单元格值）。"""
    try:
        return delete_column_formula_svc(p.conn, table_name, column_name)
    except ValueError as e:
        raise _http_from_formula_error(e) from e


@router.post("/column-formula/recalculate")
def recalculate_column_formula(
    table_name: str = Query(...),
    column_name: str = Query(...),
    p: ProjectDB = Depends(get_project_write),
):
    """重新执行指定列的行公式。"""
    try:
        return execute_row_formula_svc(p.conn, table_name, column_name)
    except ValueError as e:
        raise _http_from_formula_error(e) from e


@router.post("/column-formula/recalculate-table")
def recalculate_table_row_formulas(
    table_name: str = Query(...),
    p: ProjectDB = Depends(get_project_write),
):
    """重新计算指定表所有 row 类型公式列。"""
    return recalc_row_svc(p.conn, table_name)
