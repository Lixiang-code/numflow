"""文档 06：工具统一返回结构。"""

from __future__ import annotations

import copy
from typing import Any, Dict, List


def wrap_tool_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    将内部裸字典规范为:
    {status: success|error|partial, data, warnings, blocked_cells}
    """
    if "error" in raw:
        msg = str(raw["error"])
        extra = [str(w) for w in raw.get("warnings", [])] if isinstance(raw.get("warnings"), list) else []
        blockers = raw.get("blockers")
        err_data: Any = None
        if isinstance(blockers, list) and blockers:
            err_data = {"blockers": blockers}
        return {
            "status": "error",
            "data": err_data,
            "warnings": [msg] + extra,
            "blocked_cells": [],
        }

    if "applied" in raw and "skipped" in raw:
        skipped = raw.get("skipped") or []
        blocked: List[Dict[str, str]] = []
        for s in skipped:
            if not isinstance(s, dict):
                continue
            blocked.append(
                {
                    "row_id": str(s.get("row_id", "")),
                    "column": str(s.get("column", "")),
                    "reason": str(s.get("reason", "protected")),
                }
            )
        applied = int(raw.get("applied", 0) or 0)
        if skipped and applied == 0:
            st = "partial"
        elif skipped:
            st = "partial"
        else:
            st = "success"
        warns: List[str] = []
        if skipped:
            warns.append(f"已跳过 {len(skipped)} 个受保护单元格（user_manual）")
        return {
            "status": st,
            "data": {"applied": applied, "skipped": skipped},
            "warnings": warns,
            "blocked_cells": blocked,
        }

    if "passed" in raw and "violations" in raw:
        vlist = raw.get("violations") or []
        warns = list(raw.get("warnings") or [])
        st = "success" if raw.get("passed") and not vlist and not warns else "partial"
        return {
            "status": st,
            "data": copy.deepcopy(raw),
            "warnings": [str(w) for w in warns],
            "blocked_cells": [],
        }

    if "executed" in raw and "errors" in raw:
        errs = raw.get("errors") or []
        if errs:
            return {
                "status": "partial",
                "data": copy.deepcopy(raw),
                "warnings": [str(e) for e in errs],
                "blocked_cells": [],
            }
        return {
            "status": "success",
            "data": copy.deepcopy(raw),
            "warnings": [],
            "blocked_cells": [],
        }

    return {
        "status": "success",
        "data": copy.deepcopy(raw),
        "warnings": [],
        "blocked_cells": [],
    }
