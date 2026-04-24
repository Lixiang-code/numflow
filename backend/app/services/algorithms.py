"""算法层占位 API（文档 05）。"""

from __future__ import annotations

from typing import Any, Dict, List


def _api_specs() -> Dict[str, Dict[str, Any]]:
    return {
        "echo_sum": {
            "description": "示例：返回 params 中 numbers 列表之和",
            "params_schema": [
                {
                    "name": "numbers",
                    "required": True,
                    "kind": "list",
                    "elem": "number",
                },
            ],
            "returns": {"sum": "float"},
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
                bad = False
                for i, x in enumerate(val):
                    try:
                        float(x)
                    except (TypeError, ValueError):
                        errs.append(f"参数 {pname}[{i}] 非数值: {x!r}")
                        bad = True
                        break
                if bad:
                    continue
    return errs


def call_api(name: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(params, dict):
        raise ValueError("params 必须为 JSON 对象")
    errs = _validate_params(name, params)
    if errs:
        raise ValueError("; ".join(errs))
    if name == "echo_sum":
        nums = params.get("numbers") or []
        return {"sum": float(sum(float(x) for x in nums))}
    raise ValueError(f"未知算法 API: {name}")
