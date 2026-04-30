"""文档 06：工具统一返回结构。"""

from __future__ import annotations

import copy
import math
from typing import Any, Dict, List

# 从工具返回中剥除的噪声字段（对 AI 无用，占用 token）
_STRIP_KEYS = frozenset({"created_at", "updated_at"})

# 小数精度：保留 4 位有效小数（忽略小数点后的前导零）
_DECIMAL_SIG = 4
_PRUNE = object()
_RETAIN_EMPTY_LIST_KEYS = frozenset({
    "rows",
    "cols",
    "tables",
    "items",
    "cells",
    "edges",
    "sheets",
    "slices",
    "history",
    "sessions",
    "snapshots",
    "skills",
    "violations",
})


def _round_float(v: float) -> float:
    """保留 4 位有效小数，前导零不计入有效位数。

    规则：
    - |v| >= 1：保留 4 位小数（round to 4 decimal places）
    - |v| < 1：找到小数点后第一个非零位的位置 P，保留 P+3 位小数
    示例：0.0004648678 → 0.0004649；1.056487454 → 1.0565
    """
    if not math.isfinite(v) or v == 0.0:
        return v
    abs_v = abs(v)
    if abs_v >= 1.0:
        dp = _DECIMAL_SIG
    else:
        # floor(log10(abs_v)) 是负数，其绝对值等于小数点后前导零个数+1
        exp = math.floor(math.log10(abs_v))  # e.g. -4 for 0.0004...
        dp = -exp + _DECIMAL_SIG - 1         # e.g. 4+4-1=7 → 7位小数
    rounded = round(v, dp)
    # 消除浮点精度噪声（round有时产生1.0565000000001之类）
    return float(f"{rounded:.{dp}f}".rstrip('0').rstrip('.') or '0') if '.' in f"{rounded:.{dp}f}" else rounded


def _clean_output(obj: Any) -> Any:
    """递归：删除时间戳字段 + 对浮点数保留4位有效小数。"""
    if isinstance(obj, dict):
        return {k: _clean_output(v) for k, v in obj.items() if k not in _STRIP_KEYS}
    if isinstance(obj, list):
        return [_clean_output(item) for item in obj]
    if isinstance(obj, float):
        return _round_float(obj)
    return obj


def _strip_timestamps(obj: Any) -> Any:
    """兼容旧调用入口，现在直接走 _clean_output。"""
    return _clean_output(obj)


def _prune_empty_values(obj: Any, *, parent_key: str = "") -> Any:
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for key, value in obj.items():
            pruned = _prune_empty_values(value, parent_key=str(key))
            if pruned is _PRUNE:
                continue
            if isinstance(pruned, dict) and not pruned:
                continue
            if isinstance(pruned, list) and not pruned and str(key) not in _RETAIN_EMPTY_LIST_KEYS:
                continue
            out[key] = pruned
        return out
    if isinstance(obj, list):
        out_list = [_prune_empty_values(item, parent_key=parent_key) for item in obj]
        out_list = [item for item in out_list if item is not _PRUNE]
        if not out_list and parent_key not in _RETAIN_EMPTY_LIST_KEYS:
            return _PRUNE
        return out_list
    return obj


def _finalize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {"status": payload["status"]}
    data = payload.get("data")
    if data is not None:
        pruned_data = _prune_empty_values(data)
        if pruned_data not in (None, {}):
            result["data"] = pruned_data
    warnings = payload.get("warnings")
    if isinstance(warnings, list) and warnings:
        result["warnings"] = warnings
    blocked = payload.get("blocked_cells")
    if isinstance(blocked, list) and blocked:
        result["blocked_cells"] = blocked
    fix = payload.get("fix")
    if fix:
        result["fix"] = fix
    return result


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
        return _finalize_payload(result)

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
        return _finalize_payload({
            "status": st,
            "data": {"applied": applied, "skipped": skipped},
            "warnings": warns,
            "blocked_cells": blocked,
        })

    if "passed" in raw and "violations" in raw:
        vlist = raw.get("violations") or []
        warns = list(raw.get("warnings") or [])
        st = "success" if raw.get("passed") and not vlist and not warns else "partial"
        return _finalize_payload({
            "status": st,
            "data": _strip_timestamps(raw),
            "warnings": [str(w) for w in warns],
            "blocked_cells": [],
        })

    if "executed" in raw and "errors" in raw:
        errs = raw.get("errors") or []
        if errs:
            return _finalize_payload({
                "status": "partial",
                "data": _strip_timestamps(raw),
                "warnings": [str(e) for e in errs],
                "blocked_cells": [],
            })
        return _finalize_payload({
            "status": "success",
            "data": _strip_timestamps(raw),
            "warnings": [],
            "blocked_cells": [],
        })

    return _finalize_payload({
        "status": "success",
        "data": _strip_timestamps(raw),
        "warnings": [],
        "blocked_cells": [],
    })


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
