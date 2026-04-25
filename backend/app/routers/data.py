"""表数据 API（/data）：动态建表、读写单元格、来源标记与保护格。"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.deps import ProjectDB, get_project_read, get_project_write
from app.services.cell_writes import apply_write_cells
from app.services.table_ops import create_dynamic_table, delete_dynamic_table

from app.util.identifiers import assert_table_or_column

router = APIRouter(prefix="/data", tags=["data"])


def _assert_table_name(name: str) -> str:
    try:
        return assert_table_or_column(name, field="表名")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


def _assert_column_name(name: str) -> str:
    try:
        return assert_table_or_column(name, field="列名")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


class ColumnSpec(BaseModel):
    name: str
    sql_type: Literal["TEXT", "REAL", "INTEGER"]


class CreateTableBody(BaseModel):
    table_name: str
    columns: List[ColumnSpec]
    readme: str = ""
    purpose: str = ""


@router.post("/tables")
def create_table(body: CreateTableBody, p: ProjectDB = Depends(get_project_write)):
    t = _assert_table_name(body.table_name)
    cols = [(_assert_column_name(c.name), c.sql_type) for c in body.columns]
    try:
        return create_dynamic_table(
            p.conn,
            table_name=t,
            columns=cols,
            readme=body.readme,
            purpose=body.purpose,
        )
    except ValueError as e:
        msg = str(e)
        code = 400 if "未知" not in msg else 404
        raise HTTPException(status_code=code, detail=msg) from e


@router.delete("/tables/{table_name}")
def delete_table_route(
    table_name: str,
    confirm: bool = Query(False, description="必须为 true"),
    p: ProjectDB = Depends(get_project_write),
):
    if not confirm:
        raise HTTPException(status_code=400, detail="须传 query 参数 confirm=true")
    t = _assert_table_name(table_name)
    try:
        r = delete_dynamic_table(p.conn, table_name=t, confirm=True)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not r.get("ok"):
        raise HTTPException(status_code=400, detail=r)
    return r


@router.get("/tables/{table_name}")
def describe_table(table_name: str, p: ProjectDB = Depends(get_project_read)):
    t = _assert_table_name(table_name)
    conn = p.conn
    cur = conn.execute(
        """
        SELECT readme, schema_json, validation_status, validation_rules_json
        FROM _table_registry WHERE table_name = ?
        """,
        (t,),
    )
    meta = cur.fetchone()
    if not meta:
        raise HTTPException(status_code=404, detail="未知表")
    md = dict(meta)
    vr = md.get("validation_rules_json")
    rules_parsed = None
    if vr:
        try:
            rules_parsed = json.loads(vr)
        except json.JSONDecodeError:
            rules_parsed = None
    curf = conn.execute(
        "SELECT column_name, formula FROM _formula_registry WHERE table_name = ?",
        (t,),
    )
    column_formulas = {str(r["column_name"]): str(r["formula"]) for r in curf.fetchall()}

    return {
        "table_name": t,
        "readme": meta["readme"],
        "schema": json.loads(meta["schema_json"] or "{}"),
        "validation_status": meta["validation_status"],
        "validation_rules": rules_parsed,
        "column_formulas": column_formulas,
        "display_name": (json.loads(meta["schema_json"] or "{}") or {}).get("display_name", ""),
    }


@router.get("/tables/{table_name}/rows")
def read_rows(
    table_name: str,
    p: ProjectDB = Depends(get_project_read),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    t = _assert_table_name(table_name)
    conn = p.conn
    cur = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?",
        (t,),
    )
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="未知表")
    cur = conn.execute(f'SELECT * FROM "{t}" LIMIT ? OFFSET ?', (limit, offset))
    rows = [dict(r) for r in cur.fetchall()]
    return {"rows": rows, "limit": limit, "offset": offset}


class WriteCellItem(BaseModel):
    row_id: str
    column: str
    value: Any


class WriteCellsBody(BaseModel):
    table_name: str
    updates: List[WriteCellItem]
    source_tag: Literal["ai_generated", "algorithm_derived", "formula_computed"]


@router.post("/cells/write")
def write_cells(body: WriteCellsBody, p: ProjectDB = Depends(get_project_write)):
    t = _assert_table_name(body.table_name)
    try:
        return apply_write_cells(
            p.conn,
            table_name=t,
            updates=[u.model_dump() for u in body.updates],
            source_tag=body.source_tag,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


class MarkManualBody(BaseModel):
    table_name: str
    row_id: str
    column: str


@router.post("/cells/mark-manual")
def mark_manual(body: MarkManualBody, p: ProjectDB = Depends(get_project_write)):
    t = _assert_table_name(body.table_name)
    col = _assert_table_name(body.column)
    conn = p.conn
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute(
        """
        INSERT INTO _cell_provenance (table_name, row_id, column_name, source_tag, updated_at)
        VALUES (?,?,?,?,?)
        ON CONFLICT(table_name, row_id, column_name)
        DO UPDATE SET source_tag = 'user_manual', updated_at = excluded.updated_at
        """,
        (t, body.row_id, col, "user_manual", now),
    )
    conn.commit()
    return {"ok": True}


@router.get("/cells/protected/{table_name}")
def list_protected(table_name: str, p: ProjectDB = Depends(get_project_read)):
    t = _assert_table_name(table_name)
    conn = p.conn
    cur = conn.execute(
        """
        SELECT row_id, column_name FROM _cell_provenance
        WHERE table_name = ? AND source_tag = 'user_manual'
        """,
        (t,),
    )
    return {"cells": [{"row_id": r["row_id"], "column": r["column_name"]} for r in cur.fetchall()]}


@router.get("/cells/{table_name}/{row_id}/{column_name}")
def read_cell(table_name: str, row_id: str, column_name: str, p: ProjectDB = Depends(get_project_read)):
    t = _assert_table_name(table_name)
    col = _assert_table_name(column_name)
    conn = p.conn
    cur = conn.execute(f'SELECT "{col}" AS v FROM "{t}" WHERE row_id = ?', (row_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="行不存在")
    cur = conn.execute(
        """
        SELECT source_tag FROM _cell_provenance
        WHERE table_name = ? AND row_id = ? AND column_name = ?
        """,
        (t, row_id, col),
    )
    pr = cur.fetchone()
    src = pr["source_tag"] if pr else None
    return {"value": row["v"], "source_tag": src}
