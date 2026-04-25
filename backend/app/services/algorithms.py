"""算法层占位 API（文档 05）。"""

from __future__ import annotations

import math
from typing import Any, Dict, List


def _api_specs() -> Dict[str, Dict[str, Any]]:
    return {
        "echo_sum": {
            "description": "示例：返回 params 中 numbers 列表之和",
            "params_schema": [
                {"name": "numbers", "required": True, "kind": "list", "elem": "number"},
            ],
            "returns": {"sum": "float"},
        },
        "growth_curve": {
            "description": (
                "幂函数成长曲线：base + (cap-base) * ((level-1)/(max_level-1))^exponent，"
                "返回各 level 数值列表，可用于属性、伤害随等级成长"
            ),
            "params_schema": [
                {"name": "base", "required": True, "kind": "number"},
                {"name": "cap", "required": True, "kind": "number"},
                {"name": "max_level", "required": True, "kind": "number"},
                {"name": "exponent", "required": False, "kind": "number"},
                {"name": "round_digits", "required": False, "kind": "number"},
            ],
            "returns": {"values": "list[number]"},
        },
        "piecewise_curve": {
            "description": (
                "分段曲线：在 [from_level, to_level] 区间按 base / cap / exponent 计算，"
                "支持多段拼接；breakpoints=[{from,to,base,cap,exponent}, ...]"
            ),
            "params_schema": [
                {"name": "max_level", "required": True, "kind": "number"},
                {"name": "breakpoints", "required": True, "kind": "list", "elem": "object"},
                {"name": "round_digits", "required": False, "kind": "number"},
            ],
            "returns": {"values": "list[number]"},
        },
        "linear_resource_cost": {
            "description": (
                "线性/幂函数升级消耗：cost(L) = a*L + b*L^k + c，返回各 level 消耗"
            ),
            "params_schema": [
                {"name": "max_level", "required": True, "kind": "number"},
                {"name": "a", "required": False, "kind": "number"},
                {"name": "b", "required": False, "kind": "number"},
                {"name": "k", "required": False, "kind": "number"},
                {"name": "c", "required": False, "kind": "number"},
                {"name": "round_digits", "required": False, "kind": "number"},
            ],
            "returns": {"values": "list[number]"},
        },
    }


def list_apis() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for name, spec in _api_specs().items():
        params_hint: Dict[str, str] = {}
        for p in spec.get("params_schema") or []:
            pn = str(p.get("name", ""))
            req = "必填" if p.get("required") else "可选"
            kind = str(p.get("kind", ""))
            elem = str(p.get("elem", ""))
            params_hint[pn] = f"{kind}[{elem}]（{req}）" if elem else f"{kind}（{req}）"
        out.append(
            {
                "name": name,
                "description": spec.get("description", ""),
                "params": params_hint,
                "params_schema": spec.get("params_schema") or [],
                "returns": spec.get("returns") or {},
            }
        )
    return out


def _validate_params(api_name: str, params: Dict[str, Any]) -> List[str]:
    spec = _api_specs().get(api_name)
    if not spec:
        return [f"未知算法 API: {api_name}"]
    errs: List[str] = []
    for p in spec.get("params_schema") or []:
        pname = str(p.get("name", ""))
        if not pname:
            continue
        required = bool(p.get("required"))
        if required and pname not in params:
            errs.append(f"缺少必填参数 {pname}（期望 {p.get('kind')}[{p.get('elem', '')}]）")
            continue
        if pname not in params:
            continue
        val = params[pname]
        kind = str(p.get("kind", ""))
        elem = str(p.get("elem", ""))
        if kind == "list":
            if not isinstance(val, (list, tuple)):
                errs.append(f"参数 {pname} 须为数组，收到 {type(val).__name__}")
                continue
            if elem == "number":
                for i, x in enumerate(val):
                    try:
                        float(x)
                    except (TypeError, ValueError):
                        errs.append(f"参数 {pname}[{i}] 非数值: {x!r}")
                        break
        elif kind == "number":
            try:
                float(val)
            except (TypeError, ValueError):
                errs.append(f"参数 {pname} 须为数值，收到 {val!r}")
    return errs


def _round(v: float, digits: Any) -> float:
    if digits is None:
        return float(v)
    try:
        d = int(digits)
    except (TypeError, ValueError):
        return float(v)
    return round(float(v), d)


def call_api(name: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(params, dict):
        raise ValueError("params 必须为 JSON 对象")
    errs = _validate_params(name, params)
    if errs:
        raise ValueError("; ".join(errs))
    if name == "echo_sum":
        nums = params.get("numbers") or []
        return {"sum": float(sum(float(x) for x in nums))}
    if name == "growth_curve":
        base = float(params["base"])
        cap = float(params["cap"])
        max_level = int(float(params["max_level"]))
        if max_level < 1:
            raise ValueError("max_level 必须 ≥ 1")
        exp = float(params.get("exponent", 1.0))
        rd = params.get("round_digits", 0)
        if max_level == 1:
            return {"values": [_round(base, rd)]}
        values = []
        for lv in range(1, max_level + 1):
            t = (lv - 1) / (max_level - 1)
            v = base + (cap - base) * (t ** exp)
            values.append(_round(v, rd))
        return {"values": values, "level_from": 1, "level_to": max_level}
    if name == "piecewise_curve":
        max_level = int(float(params["max_level"]))
        bps = params.get("breakpoints") or []
        if not isinstance(bps, list) or not bps:
            raise ValueError("breakpoints 须为非空数组")
        rd = params.get("round_digits", 0)
        values: List[float] = []
        for lv in range(1, max_level + 1):
            seg = None
            for b in bps:
                if int(float(b.get("from", 1))) <= lv <= int(float(b.get("to", max_level))):
                    seg = b
                    break
            if seg is None:
                raise ValueError(f"等级 {lv} 未匹配任何分段")
            base = float(seg.get("base", 0))
            cap = float(seg.get("cap", base))
            exp = float(seg.get("exponent", 1.0))
            f = int(float(seg.get("from", 1)))
            t = int(float(seg.get("to", max_level)))
            denom = max(1, t - f)
            ratio = (lv - f) / denom
            v = base + (cap - base) * (ratio ** exp)
            values.append(_round(v, rd))
        return {"values": values, "level_from": 1, "level_to": max_level}
    if name == "linear_resource_cost":
        max_level = int(float(params["max_level"]))
        a = float(params.get("a", 0))
        b = float(params.get("b", 0))
        k = float(params.get("k", 1.0))
        c = float(params.get("c", 0))
        rd = params.get("round_digits", 0)
        values = [_round(a * lv + b * (lv ** k) + c, rd) for lv in range(1, max_level + 1)]
        return {"values": values, "level_from": 1, "level_to": max_level}
    raise ValueError(f"未知算法 API: {name}")
