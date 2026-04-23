"""表数据 API（/data）：动态建表、读写单元格、来源标记与保护格。"""

from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.deps import ProjectDB, get_project_read, get_project_write

router = APIRouter(prefix="/data", tags=["data"])

_TABLE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,62}$")


def _assert_table_name(name: str) -> str:
    if not _TABLE_RE.match(name) or name.startswith("_"):
        raise HTTPException(status_code=400, detail="非法表名")
    return name


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
    conn = p.conn
    cur = conn.execute("SELECT 1 FROM _table_registry WHERE table_name = ?", (t,))
    if cur.fetchone():
        raise HTTPException(status_code=400, detail="表已存在")
    cols_sql = ["row_id TEXT PRIMARY KEY"]
    for c in body.columns:
        cn = _assert_table_name(c.name)
        cols_sql.append(f'"{cn}" {c.sql_type} NULL')
    ddl = f'CREATE TABLE "{t}" ({", ".join(cols_sql)})'
    conn.execute(ddl)
    schema = {"columns": [{"name": "row_id", "sql_type": "TEXT"}, *[c.model_dump() for c in body.columns]]}
    conn.execute(
        """
        INSERT INTO _table_registry (table_name, layer, purpose, readme, schema_json, validation_status)
        VALUES (?,?,?,?,?, 'unknown')
        """,
        (t, "dynamic", body.purpose, body.readme, json.dumps(schema, ensure_ascii=False)),
    )
    conn.commit()
    return {"ok": True, "table_name": t}


@router.get("/tables/{table_name}")
def describe_table(table_name: str, p: ProjectDB = Depends(get_project_read)):
    t = _assert_table_name(table_name)
    conn = p.conn
    cur = conn.execute(
        "SELECT readme, schema_json, validation_status FROM _table_registry WHERE table_name = ?",
        (t,),
    )
    meta = cur.fetchone()
    if not meta:
        raise HTTPException(status_code=404, detail="未知表")
    return {
        "table_name": t,
        "readme": meta["readme"],
        "schema": json.loads(meta["schema_json"] or "{}"),
        "validation_status": meta["validation_status"],
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
    conn = p.conn
    cur = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?",
        (t,),
    )
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="未知表")
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    skipped: List[Dict[str, str]] = []
    applied = 0
    for u in body.updates:
        col = _assert_table_name(u.column)
        cur = conn.execute(
            """
            SELECT source_tag FROM _cell_provenance
            WHERE table_name = ? AND row_id = ? AND column_name = ?
            """,
            (t, u.row_id, col),
        )
        prow = cur.fetchone()
        if prow and prow["source_tag"] == "user_manual":
            skipped.append({"row_id": u.row_id, "column": col, "reason": "protected"})
            continue
        cur = conn.execute(f'SELECT 1 FROM "{t}" WHERE row_id = ?', (u.row_id,))
        if cur.fetchone():
            conn.execute(
                f'UPDATE "{t}" SET "{col}" = ? WHERE row_id = ?',
                (u.value, u.row_id),
            )
        else:
            conn.execute(f'INSERT INTO "{t}" (row_id, "{col}") VALUES (?,?)', (u.row_id, u.value))
        conn.execute(
            """
            INSERT INTO _cell_provenance (table_name, row_id, column_name, source_tag, updated_at)
            VALUES (?,?,?,?,?)
            ON CONFLICT(table_name, row_id, column_name)
            DO UPDATE SET source_tag = excluded.source_tag, updated_at = excluded.updated_at
            """,
            (t, u.row_id, col, body.source_tag, now),
        )
        applied += 1
    conn.commit()
    return {"applied": applied, "skipped": skipped}


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
