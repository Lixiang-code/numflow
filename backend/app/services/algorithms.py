"""算法层占位 API（文档 05）。"""

from __future__ import annotations

from typing import Any, Dict, List


def list_apis() -> List[Dict[str, Any]]:
    return [
        {
            "name": "echo_sum",
            "description": "示例：返回 params 中 numbers 列表之和",
            "params": {"numbers": "list[float]"},
            "returns": {"sum": "float"},
        }
    ]


def call_api(name: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if name == "echo_sum":
        nums = params.get("numbers") or []
        return {"sum": float(sum(float(x) for x in nums))}
    raise ValueError(f"未知算法 API: {name}")
