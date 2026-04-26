from __future__ import annotations

import json
import shutil
import time
from typing import Any, Dict, List, Optional

import sqlite3
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.db.paths import get_project_db_path, get_project_dir
from app.db.project_schema import init_project_db
from app.db.server import connect_sqlite_file
from app.deps import ensure_project_access, get_optional_user, get_server_db, require_user
from app.util_slug import slugify, unique_slug

router = APIRouter(prefix="/projects", tags=["projects"])


class CreateProjectBody(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    slug: Optional[str] = None
    """核心定义等 JSON（阶段 B/F 使用）；可为空对象。"""
    settings: Optional[Dict[str, Any]] = None
    ai_model: Optional[str] = Field(None, max_length=100)


@router.get("")
def list_projects(
    conn: sqlite3.Connection = Depends(get_server_db),
    user: dict = Depends(require_user),
):
    uid = user["id"]
    cur = conn.execute(
        """
        SELECT id, name, slug, is_template, owner_user_id
        FROM projects
        WHERE is_template = 1 OR owner_user_id = ? OR (? = 1)
        ORDER BY is_template DESC, id ASC
        """,
        (uid, 1 if user.get("is_admin") else 0),
    )
    rows = cur.fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        is_tpl = bool(r["is_template"])
        owner = r["owner_user_id"]
        can_write = bool(user.get("is_admin")) if is_tpl else (owner == uid or user.get("is_admin"))
        out.append(
            {
                "id": r["id"],
                "name": r["name"],
                "slug": r["slug"],
                "is_template": is_tpl,
                "can_write": can_write,
            }
        )
    return {"projects": out}


@router.post("")
def create_project(
    body: CreateProjectBody,
    conn: sqlite3.Connection = Depends(get_server_db),
    user: dict = Depends(require_user),
):
    base = slugify(body.slug or body.name)

    def taken(s: str) -> bool:
        c = conn.execute("SELECT 1 FROM projects WHERE slug = ?", (s,))
        return c.fetchone() is not None

    slug = unique_slug(base, taken)
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute(
        """
        INSERT INTO projects (owner_user_id, name, slug, is_template, created_at)
        VALUES (?,?,?,0,?)
        """,
        (user["id"], body.name.strip(), slug, now),
    )
    conn.commit()
    cur = conn.execute("SELECT id FROM projects WHERE slug = ?", (slug,))
    pid = cur.fetchone()[0]

    db_path = get_project_db_path(slug)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    pc = connect_sqlite_file(db_path)
    try:
        init_project_db(pc, seed_readme=True)
        settings = body.settings or {}
        settings.setdefault("core", {})
        pc.execute(
            "INSERT INTO project_settings (key, value_json, updated_at) VALUES (?,?,?)",
            ("fixed_layer_config", json.dumps(settings, ensure_ascii=False), now),
        )
        if body.ai_model:
            pc.execute(
                """INSERT INTO project_settings (key, value_json, updated_at)
                   VALUES ('agent_model', ?, ?)
                   ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json,
                                                  updated_at = excluded.updated_at""",
                (json.dumps(body.ai_model), now),
            )
        pc.commit()
    finally:
        pc.close()

    return {"id": pid, "slug": slug, "name": body.name.strip()}


@router.get("/{project_id}")
def get_project(
    project_id: int,
    conn: sqlite3.Connection = Depends(get_server_db),
    user: dict = Depends(require_user),
):
    row = ensure_project_access(conn, user, project_id, need_write=False)
    return {
        "id": row["id"],
        "name": row["name"],
        "slug": row["slug"],
        "is_template": bool(row["is_template"]),
    }


@router.delete("/{project_id}")
def delete_project(
    project_id: int,
    conn: sqlite3.Connection = Depends(get_server_db),
    user: dict = Depends(require_user),
):
    row = ensure_project_access(conn, user, project_id, need_write=True)
    if row["is_template"]:
        raise HTTPException(status_code=403, detail="模板项目不能删除")
    slug = row["slug"]
    # Remove from server DB
    conn.execute("DELETE FROM projects WHERE id = ?", (project_id,))
    conn.commit()
    # Remove project data directory
    project_dir = get_project_dir(slug)
    if project_dir.exists():
        shutil.rmtree(project_dir)
    return {"ok": True, "deleted_id": project_id}
