"""受限公式：仅数字运算与 @表名[列名] 同表或跨表引用（简化实现）。"""

from __future__ import annotations

import ast
import math
import operator
import re
from typing import Any, Callable, Dict, List, Set, Tuple

import pandas as pd

# 表名/列名支持中文标识（与 app.util.identifiers 一致）
_REF = re.compile(
    r"@([\u4e00-\u9fffA-Za-z_][\u4e00-\u9fffA-Za-z0-9_]*)\[([\u4e00-\u9fffA-Za-z_][\u4e00-\u9fffA-Za-z0-9_]*)\]"
)


def parse_formula_refs(formula: str) -> Set[Tuple[str, str]]:
    return set(_REF.findall(formula))


_ALLOWED_NODES = (
    ast.Expression,
    ast.BinOp,
    ast.UnaryOp,
    ast.BoolOp,
    ast.Compare,
    ast.IfExp,
    ast.Load,
    ast.Constant,
    ast.Num,  # py<3.8
    ast.Name,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Pow,
    ast.Mod,
    ast.USub,
    ast.UAdd,
    ast.Not,
    ast.And,
    ast.Or,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.Call,
    ast.Tuple,
)


def _to_bool(v: Any) -> bool:
    return bool(v)


def _safe_log(x: float, base: float | None = None) -> float:
    return math.log(x) if base is None else math.log(x, base)


def _piecewise(*args: Any) -> Any:
    """piecewise(cond1, val1, cond2, val2, ..., default)
    依次取第一个为真的 cond 对应 val；末位为默认值。"""
    if len(args) < 1:
        raise ValueError("piecewise 至少需要 1 个参数")
    default = args[-1]
    pairs = args[:-1]
    if len(pairs) % 2 != 0:
        raise ValueError("piecewise 需要 cond/val 配对加默认值，参数个数应为奇数")
    for i in range(0, len(pairs), 2):
        if _to_bool(pairs[i]):
            return pairs[i + 1]
    return default


def _if(cond: Any, a: Any, b: Any) -> Any:
    return a if _to_bool(cond) else b


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _and_fn(*args: Any) -> bool:
    return all(_to_bool(a) for a in args)


def _or_fn(*args: Any) -> bool:
    return any(_to_bool(a) for a in args)


def _not_fn(a: Any) -> bool:
    return not _to_bool(a)


def _round(x: float, n: int = 0) -> float:
    n = int(n)
    return round(float(x), n)


def _mod(a: float, b: float) -> float:
    return a % b


# 函数表（key 全部小写）
_FUNCTIONS: Dict[str, Callable[..., Any]] = {
    "round": _round,
    "floor": math.floor,
    "ceil": math.ceil,
    "ceiling": math.ceil,
    "trunc": math.trunc,
    "int": lambda x: int(x),
    "abs": abs,
    "sqrt": math.sqrt,
    "exp": math.exp,
    "log": _safe_log,
    "log2": math.log2,
    "log10": math.log10,
    "ln": math.log,
    "pow": math.pow,
    "power": math.pow,
    "min": min,
    "max": max,
    "mod": _mod,
    "sign": lambda x: (0 if x == 0 else (1 if x > 0 else -1)),
    "if": _if,
    "ifs": _piecewise,
    "piecewise": _piecewise,
    "clamp": _clamp,
    "and": _and_fn,
    "or": _or_fn,
    "not": _not_fn,
    "true": lambda: True,
    "false": lambda: False,
    "pi": lambda: math.pi,
    "e": lambda: math.e,
}


_CMP_OPS = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
}


def _eval_ast(node: ast.AST, names: Dict[str, Any]) -> Any:
    if isinstance(node, ast.Expression):
        return _eval_ast(node.body, names)
    if isinstance(node, ast.Constant):
        if isinstance(node.value, (int, float, bool)):
            return node.value
        raise ValueError("仅允许数字/布尔常量")
    if isinstance(node, ast.Num):  # pragma: no cover
        return node.n
    if isinstance(node, ast.Name):
        nid = node.id
        # 标识符大小写不敏感地匹配函数（无参常量 pi/e/true/false 也接受这里）
        if nid in names:
            return names[nid]
        low = nid.lower()
        if low in _FUNCTIONS and low in {"true", "false", "pi", "e"}:
            return _FUNCTIONS[low]()
        raise ValueError(f"未知标识符 {nid}")
    if isinstance(node, ast.UnaryOp):
        if isinstance(node.op, (ast.USub, ast.UAdd)):
            v = _eval_ast(node.operand, names)
            return -v if isinstance(node.op, ast.USub) else v
        if isinstance(node.op, ast.Not):
            return not _to_bool(_eval_ast(node.operand, names))
    if isinstance(node, ast.BinOp):
        ops = {
            ast.Add: operator.add,
            ast.Sub: operator.sub,
            ast.Mult: operator.mul,
            ast.Div: operator.truediv,
            ast.FloorDiv: operator.floordiv,
            ast.Pow: operator.pow,
            ast.Mod: operator.mod,
        }
        fn = ops.get(type(node.op))
        if fn is not None:
            return fn(_eval_ast(node.left, names), _eval_ast(node.right, names))
    if isinstance(node, ast.BoolOp):
        vals = [_eval_ast(v, names) for v in node.values]
        if isinstance(node.op, ast.And):
            return all(_to_bool(v) for v in vals)
        if isinstance(node.op, ast.Or):
            return any(_to_bool(v) for v in vals)
    if isinstance(node, ast.Compare):
        left = _eval_ast(node.left, names)
        for op, right_node in zip(node.ops, node.comparators):
            right = _eval_ast(right_node, names)
            cmp = _CMP_OPS.get(type(op))
            if cmp is None:
                raise ValueError("不支持的比较运算")
            if not cmp(left, right):
                return False
            left = right
        return True
    if isinstance(node, ast.IfExp):
        cond = _eval_ast(node.test, names)
        return _eval_ast(node.body if _to_bool(cond) else node.orelse, names)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        fname = node.func.id.lower()
        fn = _FUNCTIONS.get(fname)
        if fn is None:
            raise ValueError(f"不支持的函数 {node.func.id}")
        args = [_eval_ast(a, names) for a in node.args]
        return fn(*args)
    raise ValueError(f"不支持的表达式结构: {type(node).__name__}")


def safe_eval_scalar(expr: str, names: Dict[str, Any]) -> float:
    tree = ast.parse(expr, mode="eval")
    for n in ast.walk(tree):
        if not isinstance(n, _ALLOWED_NODES):
            raise ValueError(f"禁止的语法: {type(n).__name__}")
    return float(_eval_ast(tree, names))


def substitute_refs(
    formula: str,
    *,
    frames: Dict[str, pd.DataFrame],
) -> Tuple[str, Dict[str, pd.Series]]:
    """将 @T[c] 替换为占位符 __s0 并返回 series 映射；同一引用出现多次时全部替换。"""
    names: Dict[str, pd.Series] = {}
    out = formula
    idx = 0
    for tbl, col in parse_formula_refs(formula):
        if tbl not in frames:
            raise ValueError(f"未加载表 {tbl}")
        df = frames[tbl]
        if col not in df.columns:
            raise ValueError(f"表 {tbl} 无列 {col}")
        key = f"__s{idx}"
        names[key] = pd.to_numeric(df[col], errors="coerce")
        token = f"@{tbl}[{col}]"
        out = out.replace(token, key)
        idx += 1
    return out, names


def eval_series(formula: str, frames: Dict[str, pd.DataFrame]) -> pd.Series:
    expr, series_map = substitute_refs(formula, frames=frames)
    if not series_map:
        v = safe_eval_scalar(expr, {})
        return pd.Series([v] * max(len(df) for df in frames.values()))
    first = next(iter(series_map.values()))
    n = len(first)
    names: Dict[str, Any] = {}
    for k, s in series_map.items():
        if len(s) != n:
            raise ValueError("引用列长度不一致")
        names[k] = s
    return pd.Series(safe_eval_vector(expr, names))


def safe_eval_vector(expr: str, names: Dict[str, pd.Series]) -> list:
    """对逐元素应用 safe_eval_scalar（小表可接受）。"""
    if not names:
        return [safe_eval_scalar(expr, {})] * 1
    n = len(next(iter(names.values())))
    out = []
    for i in range(n):
        env = {k: float(v.iloc[i]) for k, v in names.items()}
        out.append(safe_eval_scalar(expr, env))
    return out
