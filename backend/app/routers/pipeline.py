"""首次数值建模流水线状态（文档 03，顺序约束）。"""

from __future__ import annotations

import json
import time
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

# 稳定 ID 顺序（不含动态展开的 gameplay_table.* 子步）
PIPELINE_STEPS_BASE: List[str] = [
    "environment_global_readme",
    "gameplay_planning",
    "base_attribute_framework",
    "hp_formula_derivation",
    "gameplay_allocation",
    "cultivation_resource_framework",
    "cultivation_allocation",
    # gameplay_table.<id> 步骤由 _gameplay_table_registry 动态生成
]

# Backward compat alias
PIPELINE_STEPS: List[str] = PIPELINE_STEPS_BASE


def _get_registered_gameplay_tables(conn) -> List[Dict[str, Any]]:
    """从 _gameplay_table_registry 读取所有已注册的玩法表，按 order_num 排序。"""
    try:
        rows = conn.execute(
            "SELECT table_id, display_name, readme, status, order_num, dependencies "
            "FROM _gameplay_table_registry ORDER BY order_num, table_id"
        ).fetchall()
        result = []
        for row in rows:
            result.append({
                "table_id": row[0],
                "display_name": row[1],
                "readme": row[2] or "",
                "status": row[3] or "未开始",
                "order_num": row[4] or 0,
                "dependencies": json.loads(row[5] or "[]"),
            })
        return result
    except Exception:  # noqa: BLE001
        return []


def _expand_pipeline_steps(conn) -> List[str]:
    """把基础步骤 + 动态玩法表步骤展开为完整列表。"""
    base = list(PIPELINE_STEPS_BASE)
    tables = _get_registered_gameplay_tables(conn)
    if not tables:
        return base
    return base + [f"gameplay_table.{t['table_id']}" for t in tables]


def _normalize_completed(done: List[str], expanded: List[str]) -> List[str]:
    """兼容旧库：若 'gameplay_landing_tables' 在 done 中视为所有子步均已完成。
    同时处理新的 gameplay_table.* 步骤。"""
    out: List[str] = []
    has_legacy = "gameplay_landing_tables" in done
    for s in expanded:
        if s in done:
            out.append(s)
        elif s.startswith("gameplay_table.") and has_legacy:
            # 旧库有 gameplay_landing_tables → 跳过（不追溯完成）
            pass
        elif s.startswith("gameplay_landing_tables.") and has_legacy:
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


@router.get("/design-history")
def pipeline_design_history(p: ProjectDB = Depends(get_project_read)):
    """返回所有已完成步骤的 README / design_text，用于前端只读抽屉展示。"""
    st = get_pipeline_state(p.conn)
    raw_done = list(st.get("completed_steps") or [])
    expanded = _expand_pipeline_steps(p.conn)
    done = _normalize_completed(raw_done, expanded)
    entries = []
    for step_id in done:
        stored = get_setting(p.conn, _readme_setting_key(step_id))
        text = ""
        if isinstance(stored, dict):
            text = stored.get("text") or ""
        entries.append({"step_id": step_id, "design_text": text})
    return {"entries": entries}


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


@router.get("/gameplay-tables")
def pipeline_gameplay_tables(p: ProjectDB = Depends(get_project_read)):
    """返回已注册的玩法表清单（由 gameplay_planning 步骤的 Agent 注册）。"""
    tables = _get_registered_gameplay_tables(p.conn)
    return {"tables": tables}


@router.get("/revision-requests")
def pipeline_revision_requests(p: ProjectDB = Depends(get_project_read)):
    """返回玩法表二次修订请求队列。"""
    try:
        rows = p.conn.execute(
            "SELECT id, table_id, reason, requested_by_step, status, created_at, updated_at "
            "FROM _table_revision_requests ORDER BY created_at DESC"
        ).fetchall()
        items = [
            {
                "id": r[0],
                "table_id": r[1],
                "reason": r[2],
                "requested_by_step": r[3],
                "status": r[4],
                "created_at": r[5],
                "updated_at": r[6],
            }
            for r in rows
        ]
        return {"items": items, "total": len(items)}
    except Exception:  # noqa: BLE001
        return {"items": [], "total": 0}


class RevisionStatusBody(BaseModel):
    status: str  # pending / in_progress / done


@router.patch("/revision-requests/{request_id}/status")
def pipeline_revision_request_update_status(
    request_id: int,
    body: RevisionStatusBody,
    p: ProjectDB = Depends(get_project_write),
):
    """更新修订请求状态（前端手动确认修订完成时调用）。"""
    if body.status not in ("pending", "in_progress", "done"):
        raise HTTPException(status_code=400, detail="status 只允许 pending / in_progress / done")
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    cur = p.conn.execute(
        "UPDATE _table_revision_requests SET status=?, updated_at=? WHERE id=?",
        (body.status, now, request_id),
    )
    p.conn.commit()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"找不到修订请求 id={request_id}")
    # 若标记为 done，也尝试将对应的表状态从 待修订 → 已完成（若当前还是待修订）
    if body.status == "done":
        row = p.conn.execute(
            "SELECT table_id FROM _table_revision_requests WHERE id=?", (request_id,)
        ).fetchone()
        if row:
            table_id = row[0]
            p.conn.execute(
                "UPDATE _gameplay_table_registry SET status='已完成', updated_at=? "
                "WHERE table_id=? AND status='待修订'",
                (now, table_id),
            )
            p.conn.commit()
    return {"ok": True, "id": request_id, "new_status": body.status}
