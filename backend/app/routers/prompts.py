"""Project-level prompt library APIs for system/tool prompt management."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.deps import ProjectDB, get_project_read, get_project_write
from app.services.agent_runner import get_agent_system_prompt_catalog
from app.services.agent_tools import get_tool_prompt_catalog
from app.services.prompt_overrides import delete_prompt_override, upsert_prompt_override
from app.services.prompt_router import get_router_prompt_catalog

router = APIRouter(prefix="/prompts", tags=["prompts"])

PromptCategory = Literal["system", "tool"]


class PromptModuleBody(BaseModel):
    module_key: str = Field(min_length=1, max_length=200)
    title: str = Field(min_length=1, max_length=200)
    content: str = ""
    required: bool = False
    enabled: bool = True
    sort_order: int = 0


class PromptUpsertBody(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    summary: str = ""
    description: str = ""
    reference_note: str = ""
    enabled: bool = True
    modules: List[PromptModuleBody] = Field(default_factory=list)


def _system_catalog(conn) -> List[Dict[str, Any]]:
    items = get_agent_system_prompt_catalog(conn)
    items.extend(get_router_prompt_catalog(conn))
    items.sort(key=lambda item: (int(item.get("display_order") or 0), str(item.get("title") or "")))
    return items


def _catalog_for_category(conn, category: PromptCategory) -> List[Dict[str, Any]]:
    if category == "tool":
        return get_tool_prompt_catalog(conn)
    return _system_catalog(conn)


@router.get("")
def prompt_list(
    category: PromptCategory = Query(...),
    p: ProjectDB = Depends(get_project_read),
) -> Dict[str, Any]:
    return {
        "items": _catalog_for_category(p.conn, category),
        "can_write": p.can_write,
    }


@router.get("/{category}/{prompt_key:path}")
def prompt_detail(
    category: PromptCategory,
    prompt_key: str,
    p: ProjectDB = Depends(get_project_read),
) -> Dict[str, Any]:
    items = _catalog_for_category(p.conn, category)
    for item in items:
        if str(item.get("prompt_key") or "") == prompt_key:
            return item
    raise HTTPException(status_code=404, detail="提示词不存在")


@router.put("/{category}/{prompt_key:path}")
def prompt_update(
    category: PromptCategory,
    prompt_key: str,
    body: PromptUpsertBody,
    p: ProjectDB = Depends(get_project_write),
) -> Dict[str, Any]:
    try:
        return upsert_prompt_override(
            p.conn,
            category=category,
            prompt_key=prompt_key,
            payload=body.model_dump(),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.delete("/{category}/{prompt_key:path}")
def prompt_reset(
    category: PromptCategory,
    prompt_key: str,
    p: ProjectDB = Depends(get_project_write),
) -> Dict[str, Any]:
    return {"ok": delete_prompt_override(p.conn, category=category, prompt_key=prompt_key)}

