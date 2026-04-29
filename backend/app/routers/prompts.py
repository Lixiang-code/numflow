"""Project-level prompt library APIs for system/tool prompt management."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.db.server import get_server_db
from app.deps import ProjectDB, get_project_read, get_project_write, require_user
from app.services.agent_runner import get_agent_system_prompt_catalog
from app.services.agent_tools import get_tool_prompt_catalog
from app.services.prompt_overrides import (
    build_prompt_editor_item,
    delete_prompt_override,
    get_prompt_override,
    merge_prompt_item_layers,
    upsert_prompt_override,
)
from app.services.prompt_router import get_router_prompt_catalog

router = APIRouter(prefix="/prompts", tags=["prompts"])

PromptCategory = Literal["system", "tool"]
PromptScope = Literal["project", "global"]


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


def _system_catalog(conn=None, *, global_conn=None) -> List[Dict[str, Any]]:
    items = get_agent_system_prompt_catalog(conn, global_conn=global_conn)
    items.extend(get_router_prompt_catalog(conn, global_conn=global_conn))
    items.sort(key=lambda item: (int(item.get("display_order") or 0), str(item.get("title") or "")))
    return items


def _catalog_for_category(conn, category: PromptCategory, *, merged: bool = True, global_conn=None) -> List[Dict[str, Any]]:
    if category == "tool":
        return get_tool_prompt_catalog(conn if merged else None, global_conn=global_conn if merged else None)
    return _system_catalog(conn if merged else None, global_conn=global_conn if merged else None)


def _override_scope(global_override: Optional[Dict[str, Any]], project_override: Optional[Dict[str, Any]]) -> str:
    if project_override:
        return "project"
    if global_override:
        return "global"
    return "default"


def _build_prompt_item_for_scope(
    *,
    default_item: Dict[str, Any],
    category: PromptCategory,
    scope: PromptScope,
    project_conn,
    global_conn,
) -> Dict[str, Any]:
    prompt_key = str(default_item.get("prompt_key") or "")
    global_override = get_prompt_override(global_conn, category=category, prompt_key=prompt_key)
    if scope == "global":
        item = build_prompt_editor_item(default_item=default_item, override_item=global_override, category=category)
        item["global_override"] = bool(global_override)
        item["project_override"] = False
        item["override_scope"] = "global" if global_override else "default"
        item["scope"] = "global"
        return item

    project_override = get_prompt_override(project_conn, category=category, prompt_key=prompt_key)
    effective_default = merge_prompt_item_layers(default_item, [global_override])
    item = build_prompt_editor_item(default_item=effective_default, override_item=project_override, category=category)
    item["global_override"] = bool(global_override)
    item["project_override"] = bool(project_override)
    item["override_scope"] = _override_scope(global_override, project_override)
    item["scope"] = "project"
    return item


@router.get("")
def prompt_list(
    category: PromptCategory = Query(...),
    scope: PromptScope = Query("project"),
    p: ProjectDB = Depends(get_project_read),
    sconn=Depends(get_server_db),
    user: Dict[str, Any] = Depends(require_user),
) -> Dict[str, Any]:
    defaults = _catalog_for_category(None, category, merged=False)
    return {
        "items": [
            _build_prompt_item_for_scope(
                default_item=default_item,
                category=category,
                scope=scope,
                project_conn=p.conn,
                global_conn=sconn,
            )
            for default_item in defaults
        ],
        "can_write": bool(user.get("is_admin")) if scope == "global" else p.can_write,
        "can_global_write": bool(user.get("is_admin")),
        "scope": scope,
    }


@router.get("/{category}/{prompt_key:path}")
def prompt_detail(
    category: PromptCategory,
    prompt_key: str,
    scope: PromptScope = Query("project"),
    p: ProjectDB = Depends(get_project_read),
    sconn=Depends(get_server_db),
) -> Dict[str, Any]:
    defaults = _catalog_for_category(None, category, merged=False)
    for default_item in defaults:
        if str(default_item.get("prompt_key") or "") == prompt_key:
            return _build_prompt_item_for_scope(
                default_item=default_item,
                category=category,
                scope=scope,
                project_conn=p.conn,
                global_conn=sconn,
            )
    raise HTTPException(status_code=404, detail="提示词不存在")


@router.put("/{category}/{prompt_key:path}")
def prompt_update(
    category: PromptCategory,
    prompt_key: str,
    body: PromptUpsertBody,
    scope: PromptScope = Query("project"),
    p: ProjectDB = Depends(get_project_write),
    sconn=Depends(get_server_db),
    user: Dict[str, Any] = Depends(require_user),
) -> Dict[str, Any]:
    try:
        target_conn = p.conn
        if scope == "global":
            if not bool(user.get("is_admin")):
                raise HTTPException(status_code=403, detail="仅管理员可执行全局修改")
            target_conn = sconn
        return upsert_prompt_override(
            target_conn,
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
    scope: PromptScope = Query("project"),
    p: ProjectDB = Depends(get_project_write),
    sconn=Depends(get_server_db),
    user: Dict[str, Any] = Depends(require_user),
) -> Dict[str, Any]:
    target_conn = p.conn
    if scope == "global":
        if not bool(user.get("is_admin")):
            raise HTTPException(status_code=403, detail="仅管理员可执行全局修改")
        target_conn = sconn
    return {"ok": delete_prompt_override(target_conn, category=category, prompt_key=prompt_key)}
