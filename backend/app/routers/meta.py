"""元信息 API（对齐文档 /meta 与工具 get_project_config 等）。"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.deps import ProjectDB, get_project_read, get_project_write

router = APIRouter(prefix="/meta", tags=["meta"])


@router.get("/project-config")
def get_project_config(p: ProjectDB = Depends(get_project_read)):
    conn = p.conn
    cur = conn.execute("SELECT key, value_json FROM project_settings")
    settings: Dict[str, Any] = {}
    for k, v in cur.fetchall():
        try:
            settings[k] = json.loads(v)
        except json.JSONDecodeError:
            settings[k] = v
    return {"project": dict(p.row), "settings": settings}


@router.get("/tables")
def get_table_list(p: ProjectDB = Depends(get_project_read)):
    cur = p.conn.execute(
        "SELECT table_name, layer, purpose, validation_status FROM _table_registry ORDER BY table_name"
    )
    return {"tables": [dict(r) for r in cur.fetchall()]}


@router.get("/tables/{table_name}/readme")
def get_table_readme(table_name: str, p: ProjectDB = Depends(get_project_read)):
    cur = p.conn.execute(
        "SELECT readme FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="未知表")
    return {"table_name": table_name, "readme": row["readme"] or ""}


@router.get("/dependency-graph")
def get_dependency_graph(
    table_name: Optional[str] = Query(None),
    direction: str = Query("full", pattern="^(upstream|downstream|full)$"),
    p: ProjectDB = Depends(get_project_read),
):
    conn = p.conn
    if table_name:
        if direction == "upstream":
            cur = conn.execute(
                """
                SELECT * FROM _dependency_graph
                WHERE to_table = ?
                """,
                (table_name,),
            )
        elif direction == "downstream":
            cur = conn.execute(
                """
                SELECT * FROM _dependency_graph
                WHERE from_table = ?
                """,
                (table_name,),
            )
        else:
            cur = conn.execute(
                """
                SELECT * FROM _dependency_graph
                WHERE from_table = ? OR to_table = ?
                """,
                (table_name, table_name),
            )
    else:
        cur = conn.execute("SELECT * FROM _dependency_graph")
    return {"edges": [dict(r) for r in cur.fetchall()]}


from pydantic import BaseModel


class ReadmeBody(BaseModel):
    content: str


@router.put("/tables/{table_name}/readme")
def update_table_readme(
    table_name: str,
    body: ReadmeBody,
    p: ProjectDB = Depends(get_project_write),
):
    conn = p.conn
    cur = conn.execute(
        "SELECT 1 FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="未知表")
    conn.execute(
        "UPDATE _table_registry SET readme = ? WHERE table_name = ?",
        (body.content, table_name),
    )
    conn.commit()
    return {"ok": True}


class GlobalReadmeBody(BaseModel):
    content: str


@router.put("/global-readme")
def update_global_readme(body: GlobalReadmeBody, p: ProjectDB = Depends(get_project_write)):
    import time

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn = p.conn
    conn.execute(
        """
        INSERT INTO project_settings (key, value_json, updated_at)
        VALUES ('global_readme', ?, ?)
        ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json,
            updated_at = excluded.updated_at
        """,
        (json.dumps({"text": body.content}, ensure_ascii=False), now),
    )
    conn.commit()
    return {"ok": True}
