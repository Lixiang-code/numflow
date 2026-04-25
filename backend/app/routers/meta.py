"""元信息 API（对齐文档 /meta 与工具 get_project_config 等）。"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.data.default_rules_02 import get_default_rules_payload
from app.deps import ProjectDB, get_project_read, get_project_write
from app.services.snapshot_ops import compare_snapshot, create_snapshot, list_snapshots

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
    return {"project": dict(p.row), "settings": settings, "can_write": p.can_write}


@router.get("/tables")
def get_table_list(p: ProjectDB = Depends(get_project_read)):
    cur = p.conn.execute(
        "SELECT table_name, layer, purpose, validation_status, schema_json FROM _table_registry ORDER BY table_name"
    )
    rows = []
    for r in cur.fetchall():
        d = dict(r)
        sj = d.pop("schema_json", None) or "{}"
        try:
            parsed = json.loads(sj) if isinstance(sj, str) else {}
        except json.JSONDecodeError:
            parsed = {}
        d["display_name"] = (parsed.get("display_name") if isinstance(parsed, dict) else "") or ""
        rows.append(d)
    return {"tables": rows}


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


@router.get("/default-rules")
def get_default_rules():
    """文档 02 子集：可机读默认规则（全局，非项目内）。"""
    return get_default_rules_payload()


class SnapshotCreateBody(BaseModel):
    label: str = Field(min_length=1, max_length=120)
    note: str = ""


@router.post("/snapshots")
def post_snapshot(body: SnapshotCreateBody, p: ProjectDB = Depends(get_project_write)):
    return create_snapshot(p.conn, label=body.label.strip(), note=body.note.strip())


@router.get("/snapshots")
def get_snapshots(p: ProjectDB = Depends(get_project_read)):
    return {"snapshots": list_snapshots(p.conn)}


@router.get("/snapshots/{snapshot_id}/compare")
def get_snapshot_compare(snapshot_id: int, p: ProjectDB = Depends(get_project_read)):
    try:
        return compare_snapshot(p.conn, snapshot_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


class ValidationRulesBody(BaseModel):
    """MVP：{ \"rules\": [ { \"id\": \"r1\", \"type\": \"not_null\", \"column\": \"atk\" } ] }"""

    rules: List[Dict[str, Any]] = Field(default_factory=list)


@router.put("/tables/{table_name}/validation-rules")
def put_validation_rules(
    table_name: str,
    body: ValidationRulesBody,
    p: ProjectDB = Depends(get_project_write),
):
    conn = p.conn
    cur = conn.execute("SELECT 1 FROM _table_registry WHERE table_name = ?", (table_name,))
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="未知表")
    payload = json.dumps({"rules": body.rules}, ensure_ascii=False)
    conn.execute(
        "UPDATE _table_registry SET validation_rules_json = ? WHERE table_name = ?",
        (payload, table_name),
    )
    conn.commit()
    return {"ok": True}
