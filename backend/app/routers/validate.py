"""验证与分析 API（/validate）。"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.deps import ProjectDB, get_project_read
from app.services.validation_report import build_validation_report

router = APIRouter(prefix="/validate", tags=["validate"])


@router.post("/run")
def run_validations(
    p: ProjectDB = Depends(get_project_read),
    table_name: Optional[str] = Query(None, description="仅校验指定表（可选）"),
):
    """占位：返回结构化报告；后续接入规则引擎。"""
    return build_validation_report(p.conn, filter_table=table_name)
