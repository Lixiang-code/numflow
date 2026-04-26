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
from app.services.validation_report import build_validation_report


def _readme_setting_key(step_id: str) -> str:
    return f"step_readme.{step_id}"

router = APIRouter(prefix="/pipeline", tags=["pipeline"])

# 与第3轮优化文档对齐的稳定 ID（顺序约束）—— 第3.5轮：HP 单独成步
PIPELINE_STEPS_BASE: List[str] = [
    "environment_global_readme",
    "base_attribute_framework",
    "hp_formula_derivation",
    "gameplay_allocation",
    "cultivation_resource_framework",
    "cultivation_allocation",
    "gameplay_landing_tables",
]
# Backward compat alias（部分老代码引用 PIPELINE_STEPS）
PIPELINE_STEPS: List[str] = PIPELINE_STEPS_BASE

# step 11 默认子系统集合（可被项目 game_systems 覆盖）
LANDING_SUBSYSTEMS_DEFAULT: List[str] = [
    "equip", "gem", "mount", "wing", "fashion", "dungeon", "skill",
]


def _enabled_landing_subsystems(conn) -> List[str]:
    """从 fixed_layer_config.game_systems 推导启用的子系统列表。"""
    try:
        flc = get_setting(conn, "fixed_layer_config") or {}
        gs = (flc or {}).get("game_systems") or {}
    except Exception:  # noqa: BLE001
        gs = {}
    enabled: List[str] = []
    # gs 形如 {"equip": True, "gem": True, "ai_design_subsystems": True}
    if isinstance(gs, dict):
        for k, v in gs.items():
            if k in ("ai_design_subsystems", "subsystemsByPath"):
                continue
            if v in (True, "true", 1, "1"):
                enabled.append(str(k))
    if not enabled:
        return list(LANDING_SUBSYSTEMS_DEFAULT)
    # 仅保留已知子系统（避免乱入）
    out = [s for s in enabled if s in LANDING_SUBSYSTEMS_DEFAULT]
    return out or list(LANDING_SUBSYSTEMS_DEFAULT)


def _expand_pipeline_steps(conn) -> List[str]:
    """把 step 11 展开为 per-system 子步。"""
    base = list(PIPELINE_STEPS_BASE[:-1])
    subs = _enabled_landing_subsystems(conn)
    return base + [f"gameplay_landing_tables.{s}" for s in subs]


def _normalize_completed(done: List[str], expanded: List[str]) -> List[str]:
    """兼容旧库：若 'gameplay_landing_tables' 在 done 中，则视为所有子步均已完成。"""
    out: List[str] = []
    has_legacy = "gameplay_landing_tables" in done
    sub_done = {s for s in done if s.startswith("gameplay_landing_tables.")}
    for s in expanded:
        if s in done:
            out.append(s)
        elif s.startswith("gameplay_landing_tables.") and (has_legacy or s in sub_done):
            out.append(s)
        elif s in done:
            out.append(s)
    return out


@router.get("/status")
def pipeline_status(p: ProjectDB = Depends(get_project_read)):
    st = get_pipeline_state(p.conn)
    raw_done = list(st.get("completed_steps") or [])
    expanded = _expand_pipeline_steps(p.conn)
    done = _normalize_completed(raw_done, expanded)
    n = len(done)
    next_step = expanded[n] if n < len(expanded) else None
    finished = n >= len(expanded)
    return {
        "steps_order": expanded,
        "completed_steps": done,
        "current_step": st.get("current_step") or "",
        "next_expected_step": None if finished else next_step,
        "finished": finished,
    }


class AdvanceBody(BaseModel):
    step: str


@router.post("/advance")
def pipeline_advance(body: AdvanceBody, p: ProjectDB = Depends(get_project_write)):
    st = get_pipeline_state(p.conn)
    raw_done: List[str] = list(st.get("completed_steps") or [])
    expanded = _expand_pipeline_steps(p.conn)
    done = _normalize_completed(raw_done, expanded)
    n = len(done)
    if n >= len(expanded):
        raise HTTPException(status_code=400, detail="流水线已完成")
    expected = expanded[n]
    if body.step != expected:
        # 兼容客户端用旧 step 名 'gameplay_landing_tables' 推进的情况：
        # 若 expected 是 gameplay_landing_tables.* 子步，且客户端送的是基名，提示错误。
        expected_id = expected.split(".")[0]
        expected_spec = get_step_spec(expected) or get_step_spec(expected_id)
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
    nxt = expanded[n + 1] if n + 1 < len(expanded) else ""
    set_pipeline_state(p.conn, current_step=nxt, completed_steps=done)

    # 自动落 spec 模板：仅在没有人写过 step README 时回填
    spec = get_step_spec(expected) or get_step_spec(expected.split(".")[0])
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

    # 自动跑一次全表校验，把 unknown 状态收敛掉，并把违规数注入下一步上下文
    try:
        report = build_validation_report(p.conn)
        validation_summary = {
            "passed": report.get("passed"),
            "violations_count": len(report.get("violations") or []),
            "warnings_count": len(report.get("warnings") or []),
        }
    except Exception as e:  # noqa: BLE001
        validation_summary = {"error": str(e)}

    return {
        "ok": True,
        "completed_steps": done,
        "next_expected": nxt or None,
        "snapshot": snap,
        "readme_seeded": readme_seeded,
        "validation": validation_summary,
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
