"""文档 06：工具统一返回结构。"""

from __future__ import annotations

import copy
from typing import Any, Dict, List


def wrap_tool_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    将内部裸字典规范为:
    {status: success|error|partial, data, warnings, blocked_cells, fix?}
    fix 字段仅在 error 时出现，提供可操作的修复建议。
    """
    if "error" in raw:
        msg = str(raw["error"])
        fix_hint: str = str(raw["fix"]) if raw.get("fix") else _infer_fix_hint(msg)
        extra = [str(w) for w in raw.get("warnings", [])] if isinstance(raw.get("warnings"), list) else []
        blockers = raw.get("blockers")
        err_data: Any = None
        if isinstance(blockers, list) and blockers:
            err_data = {"blockers": blockers}
        result: Dict[str, Any] = {
            "status": "error",
            "data": err_data,
            "warnings": [msg] + extra,
            "blocked_cells": [],
        }
        if fix_hint:
            result["fix"] = fix_hint
        return result

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


def _infer_fix_hint(error_msg: str) -> str:
    """根据常见错误模式推断修复建议（兜底）。"""
    m = error_msg.lower()
    if "未知表" in error_msg or "unknown table" in m:
        return "先调用 get_table_list 确认表名是否存在"
    if "source_tag" in m:
        return "source_tag 合法值: ai_generated | algorithm_derived | formula_computed"
    if "无写权限" in error_msg:
        return "当前会话无写权限，检查项目权限设置"
    if "行不存在" in error_msg or "row" in m and "exist" in m:
        return "用 read_table 确认 row_id 实际存在"
    if "列" in error_msg and ("非法" in error_msg or "不存在" in error_msg):
        return "用 read_table 的返回结果确认列名（注意中英文、下划线）"
    if "公式" in error_msg or "formula" in m:
        return "检查公式中 @引用 的列名是否与表中列名完全一致，可用 read_table 确认"
    if "json" in m or "解析" in error_msg:
        return "参数格式有误，检查 JSON 结构；如是 write_cells，请减少单次 updates 数量（≤30行）"
    if "已存在" in error_msg or "exists" in m:
        return "表已存在，如需重建请先 delete_table，或直接写入现有表"
    if "max_level" in m:
        return "max_level 须 ≥ 1，检查传入的数值"
    return ""
