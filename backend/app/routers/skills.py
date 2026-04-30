"""Project-level SKILL library APIs."""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.db.paths import get_project_db_path
from app.db.server import connect_sqlite_file, get_server_db
from app.deps import ProjectDB, ensure_project_access, get_project_read, get_project_write, require_user
from app.services.skill_library import (
    get_skill_detail,
    list_skills,
    render_skill_file,
    upsert_skill,
)

router = APIRouter(prefix="/skills", tags=["skills"])


class SkillModuleBody(BaseModel):
    id: Optional[int] = None
    module_key: Optional[str] = Field(default=None, max_length=120)
    title: str = Field(min_length=1, max_length=120)
    content: str = ""
    required: bool = False
    enabled: bool = True
    sort_order: int = 0


class SkillUpsertBody(BaseModel):
    slug: Optional[str] = Field(default=None, max_length=120)
    title: str = Field(min_length=1, max_length=120)
    step_id: str = Field(default="", max_length=200)
    summary: str = Field(default="", max_length=500)
    description: str = ""
    source: str = Field(default="user", max_length=40)
    default_exposed: bool = False
    enabled: bool = True
    modules: List[SkillModuleBody] = Field(default_factory=list)


class ImportSkillsBody(BaseModel):
    source_project_id: int
    skill_slugs: List[str]
    overwrite: bool = True


@router.get("/from-project/{source_project_id}")
def skills_from_project(
    source_project_id: int,
    p: ProjectDB = Depends(get_project_read),
    sconn: sqlite3.Connection = Depends(get_server_db),
    user: dict = Depends(require_user),
) -> Dict[str, Any]:
    """List skills from another project (for cross-project import preview)."""
    source_row = ensure_project_access(sconn, user, source_project_id, need_write=False)
    source_path = get_project_db_path(str(source_row["slug"]))
    if not source_path.exists():
        raise HTTPException(status_code=404, detail="源项目数据库不存在")
    source_conn = connect_sqlite_file(source_path)
    try:
        skills = list_skills(source_conn, include_disabled=True, include_modules=True)
        return {
            "skills": skills,
            "source_project": {"id": source_project_id, "name": source_row["name"]},
        }
    finally:
        source_conn.close()


@router.post("/import")
def import_skills_from_project(
    body: ImportSkillsBody,
    p: ProjectDB = Depends(get_project_write),
    sconn: sqlite3.Connection = Depends(get_server_db),
    user: dict = Depends(require_user),
) -> Dict[str, Any]:
    """Copy selected skills from another project into this project."""
    source_row = ensure_project_access(sconn, user, body.source_project_id, need_write=False)
    source_path = get_project_db_path(str(source_row["slug"]))
    if not source_path.exists():
        raise HTTPException(status_code=404, detail="源项目数据库不存在")
    source_conn = connect_sqlite_file(source_path)
    try:
        all_source = list_skills(source_conn, include_disabled=True, include_modules=True)
        source_by_slug = {s["slug"]: s for s in all_source}

        cur = p.conn.execute("SELECT id, slug FROM _skills")
        target_by_slug = {str(row["slug"]): int(row["id"]) for row in cur.fetchall()}

        imported: List[str] = []
        skipped: List[str] = []
        for slug in body.skill_slugs:
            skill = source_by_slug.get(slug)
            if not skill:
                continue
            existing_id = target_by_slug.get(slug)
            if existing_id and not body.overwrite:
                skipped.append(slug)
                continue
            payload = {
                "slug": skill["slug"],
                "title": skill["title"],
                "step_id": skill.get("step_id", ""),
                "summary": skill.get("summary", ""),
                "description": skill.get("description", ""),
                "source": skill.get("source", "user"),
                "default_exposed": skill.get("default_exposed", False),
                "enabled": skill.get("enabled", True),
                "modules": [
                    {
                        "module_key": m.get("module_key", ""),
                        "title": m.get("title", ""),
                        "content": m.get("content", ""),
                        "required": m.get("required", False),
                        "enabled": m.get("enabled", True),
                        "sort_order": m.get("sort_order", 0),
                    }
                    for m in (skill.get("modules") or [])
                ],
            }
            upsert_skill(
                p.conn,
                project_slug=str(p.row["slug"]),
                skill_id=existing_id,
                payload=payload,
            )
            imported.append(slug)
        return {"imported": imported, "skipped": skipped}
    finally:
        source_conn.close()


@router.get("")
def skills_list(p: ProjectDB = Depends(get_project_read)) -> Dict[str, Any]:
    return {
        "skills": list_skills(
            p.conn,
            include_disabled=True,
            include_modules=True,
            project_slug=str(p.row["slug"]),
        ),
        "can_write": p.can_write,
    }


@router.get("/{skill_id}")
def skill_detail(skill_id: int, p: ProjectDB = Depends(get_project_read)) -> Dict[str, Any]:
    skill = get_skill_detail(
        p.conn,
        skill_id,
        project_slug=str(p.row["slug"]),
    )
    if not skill:
        raise HTTPException(status_code=404, detail="SKILL 不存在")
    return skill


@router.post("")
def skill_create(body: SkillUpsertBody, p: ProjectDB = Depends(get_project_write)) -> Dict[str, Any]:
    try:
        return upsert_skill(
            p.conn,
            project_slug=str(p.row["slug"]),
            skill_id=None,
            payload=body.model_dump(),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.put("/{skill_id}")
def skill_update(
    skill_id: int,
    body: SkillUpsertBody,
    p: ProjectDB = Depends(get_project_write),
) -> Dict[str, Any]:
    try:
        return upsert_skill(
            p.conn,
            project_slug=str(p.row["slug"]),
            skill_id=skill_id,
            payload=body.model_dump(),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/{skill_id}/generate")
def skill_generate(skill_id: int, p: ProjectDB = Depends(get_project_write)) -> Dict[str, Any]:
    result = render_skill_file(
        p.conn,
        skill_id,
        project_slug=str(p.row["slug"]),
    )
    if not result:
        raise HTTPException(status_code=404, detail="SKILL 不存在")
    return result

