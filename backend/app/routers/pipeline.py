"""首次数值建模流水线状态（文档 03，顺序约束）。"""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.data.pipeline_step_specs import (
    get_step_spec,
    list_step_specs,
    render_spec_markdown,
)
from app.db.project_schema import (
    get_latest_agent_session,
    get_pipeline_state,
    get_setting,
    set_pipeline_state,
    set_setting,
)
from app.deps import ProjectDB, get_project_read, get_project_write
from app.services.snapshot_ops import create_snapshot


def _readme_setting_key(step_id: str) -> str:
    return f"step_readme.{step_id}"

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
        expected_spec = get_step_spec(expected)
        raise HTTPException(
            status_code=400,
            detail={
                "message": f"顺序错误：下一步应为 {expected}，收到 {body.step}",
                "expected_step": expected,
                "expected_title": expected_spec.title_zh if expected_spec else expected,
                "expected_goal": expected_spec.goal if expected_spec else "",
            },
        )
    done.append(expected)
    nxt = PIPELINE_STEPS[n + 1] if n + 1 < len(PIPELINE_STEPS) else ""
    set_pipeline_state(p.conn, current_step=nxt, completed_steps=done)

    # 自动落 spec 模板：仅在没有人写过 step README 时回填
    spec = get_step_spec(expected)
    readme_seeded = False
    if spec is not None:
        existing = get_setting(p.conn, _readme_setting_key(expected))
        existing_text = (existing or {}).get("text") if isinstance(existing, dict) else None
        if not existing_text:
            set_setting(
                p.conn,
                _readme_setting_key(expected),
                {"text": render_spec_markdown(spec), "source": "spec_template"},
            )
            readme_seeded = True

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
        "readme_seeded": readme_seeded,
    }


@router.get("/step/{step_id}/spec")
def pipeline_step_spec(step_id: str, p: ProjectDB = Depends(get_project_read)):
    spec = get_step_spec(step_id)
    if spec is None:
        raise HTTPException(status_code=404, detail=f"未知步骤：{step_id}")
    return spec.to_dict()


@router.get("/step/{step_id}/readme")
def pipeline_step_readme(step_id: str, p: ProjectDB = Depends(get_project_read)):
    spec = get_step_spec(step_id)
    if spec is None:
        raise HTTPException(status_code=404, detail=f"未知步骤：{step_id}")
    stored = get_setting(p.conn, _readme_setting_key(step_id))
    if isinstance(stored, dict) and stored.get("text"):
        return {
            "step_id": step_id,
            "title_zh": spec.title_zh,
            "text": stored["text"],
            "source": stored.get("source", "user"),
        }
    return {
        "step_id": step_id,
        "title_zh": spec.title_zh,
        "text": render_spec_markdown(spec),
        "source": "spec_template",
    }


class StepReadmeBody(BaseModel):
    text: str


@router.put("/step/{step_id}/readme")
def pipeline_step_readme_put(
    step_id: str,
    body: StepReadmeBody,
    p: ProjectDB = Depends(get_project_write),
):
    spec = get_step_spec(step_id)
    if spec is None:
        raise HTTPException(status_code=404, detail=f"未知步骤：{step_id}")
    set_setting(
        p.conn,
        _readme_setting_key(step_id),
        {"text": body.text, "source": "user"},
    )
    return {"ok": True, "step_id": step_id, "length": len(body.text)}


@router.get("/specs")
def pipeline_specs(p: ProjectDB = Depends(get_project_read)):
    return {"specs": [s.to_dict() for s in list_step_specs()]}


@router.get("/step/{step_id}/session")
def pipeline_step_session(step_id: str, p: ProjectDB = Depends(get_project_read)):
    """Return the latest agent session for a pipeline step (for client-side recovery on refresh)."""
    session = get_latest_agent_session(p.conn, step_id)
    if session is None:
        return {"session": None}
    return {"session": session}


@router.delete("/step/{step_id}/session")
def pipeline_step_session_clear(step_id: str, p: ProjectDB = Depends(get_project_write)):
    """Clear the latest session for a step so it will re-run from scratch."""
    try:
        p.conn.execute("DELETE FROM _agent_sessions WHERE step_id = ?", (step_id,))
        p.conn.commit()
    except Exception:  # noqa: BLE001
        pass
    return {"ok": True, "step_id": step_id}
