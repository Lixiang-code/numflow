"""受限公式：仅数字运算与 @表名[列名] 同表或跨表引用（简化实现）。"""

from __future__ import annotations

import ast
import operator
import re
from typing import Any, Dict, Set, Tuple

import pandas as pd

_REF = re.compile(r"@([A-Za-z][A-Za-z0-9_]*)\[([A-Za-z][A-Za-z0-9_]*)\]")


def parse_formula_refs(formula: str) -> Set[Tuple[str, str]]:
    return set(_REF.findall(formula))


_ALLOWED_NODES = (
    ast.Expression,
    ast.BinOp,
    ast.UnaryOp,
    ast.Load,
    ast.Constant,
    ast.Num,  # py<3.8
    ast.Name,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.Pow,
    ast.Mod,
    ast.USub,
    ast.UAdd,
    ast.Call,
    ast.Tuple,
)


def _eval_ast(node: ast.AST, names: Dict[str, Any]) -> Any:
    if isinstance(node, ast.Expression):
        return _eval_ast(node.body, names)
    if isinstance(node, ast.Constant):
        if isinstance(node.value, (int, float)):
            return node.value
        raise ValueError("仅允许数字常量")
    if isinstance(node, ast.Num):  # pragma: no cover
        return node.n
    if isinstance(node, ast.Name):
        if node.id not in names:
            raise ValueError(f"未知标识符 {node.id}")
        return names[node.id]
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        v = _eval_ast(node.operand, names)
        return -v if isinstance(node.op, ast.USub) else v
    if isinstance(node, ast.BinOp) and isinstance(
        node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow, ast.Mod)
    ):
        op_type = type(node.op)
        ops = {
            ast.Add: operator.add,
            ast.Sub: operator.sub,
            ast.Mult: operator.mul,
            ast.Div: operator.truediv,
            ast.Pow: operator.pow,
            ast.Mod: operator.mod,
        }
        fn = ops[op_type]
        return fn(_eval_ast(node.left, names), _eval_ast(node.right, names))
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        if node.func.id == "min" and len(node.args) == 2:
            return min(_eval_ast(node.args[0], names), _eval_ast(node.args[1], names))
        if node.func.id == "max" and len(node.args) == 2:
            return max(_eval_ast(node.args[0], names), _eval_ast(node.args[1], names))
    raise ValueError("不支持的表达式结构")


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
    """将 @T[c] 替换为占位符 __s0 并返回 series 映射。"""
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
        out = out.replace(f"@{tbl}[{col}]", key, 1)
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
