"""验证与分析 API（/validate）。"""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Depends

from app.deps import ProjectDB, get_project_read

router = APIRouter(prefix="/validate", tags=["validate"])


@router.post("/run")
def run_validations(p: ProjectDB = Depends(get_project_read)):
    """占位：返回结构化报告；后续接入规则引擎。"""
    conn = p.conn
    cur = conn.execute(
        "SELECT table_name, validation_status FROM _table_registry ORDER BY table_name"
    )
    tables: List[Dict[str, Any]] = [dict(r) for r in cur.fetchall()]
    warnings: List[str] = []
    for t in tables:
        if t.get("validation_status") == "unknown":
            warnings.append(f"表 {t['table_name']} 尚未验证")
    return {
        "passed": len(warnings) == 0,
        "warnings": warnings,
        "tables": tables,
    }
