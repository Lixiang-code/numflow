"""Project-level SKILL library APIs."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.deps import ProjectDB, get_project_read, get_project_write
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

