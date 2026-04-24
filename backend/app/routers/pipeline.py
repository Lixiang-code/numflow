"""首次数值建模流水线状态（文档 03，顺序约束）。"""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.db.project_schema import get_pipeline_state, set_pipeline_state
from app.deps import ProjectDB, get_project_read, get_project_write
from app.services.snapshot_ops import create_snapshot

router = APIRouter(prefix="/pipeline", tags=["pipeline"])

# 与 03 文档章节对齐的稳定 ID（可随产品细化）
PIPELINE_STEPS: List[str] = [
    "environment_global_readme",
    "base_attribute_framework",
    "gameplay_attribute_scheme",
    "gameplay_allocation_tables",
    "second_order_framework",
    "gameplay_attribute_tables",
    "cultivation_resource_design",
    "cultivation_resource_framework",
    "cultivation_allocation_tables",
    "cultivation_quant_tables",
    "gameplay_landing_tables",
]


@router.get("/status")
def pipeline_status(p: ProjectDB = Depends(get_project_read)):
    st = get_pipeline_state(p.conn)
    n = len(st.get("completed_steps") or [])
    next_step = PIPELINE_STEPS[n] if n < len(PIPELINE_STEPS) else None
    finished = n >= len(PIPELINE_STEPS)
    return {
        "steps_order": PIPELINE_STEPS,
        "completed_steps": st.get("completed_steps") or [],
        "current_step": st.get("current_step") or "",
        "next_expected_step": None if finished else next_step,
        "finished": finished,
    }


class AdvanceBody(BaseModel):
    step: str


@router.post("/advance")
def pipeline_advance(body: AdvanceBody, p: ProjectDB = Depends(get_project_write)):
    st = get_pipeline_state(p.conn)
    done: List[str] = list(st.get("completed_steps") or [])
    n = len(done)
    if n >= len(PIPELINE_STEPS):
        raise HTTPException(status_code=400, detail="流水线已完成")
    expected = PIPELINE_STEPS[n]
    if body.step != expected:
        raise HTTPException(
            status_code=400,
            detail=f"顺序错误：下一步应为 {expected}，收到 {body.step}",
        )
    done.append(expected)
    nxt = PIPELINE_STEPS[n + 1] if n + 1 < len(PIPELINE_STEPS) else ""
    set_pipeline_state(p.conn, current_step=nxt, completed_steps=done)
    snap = create_snapshot(
        p.conn,
        label=f"pipeline:{expected}",
        note=f"流水线自动快照：完成步骤 {expected}",
    )
    return {
        "ok": True,
        "completed_steps": done,
        "next_expected": nxt or None,
        "snapshot": snap,
    }
