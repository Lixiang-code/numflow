"""受限公式：数字运算、逻辑、@表名[列名] 逐行引用、@@表名[列名] 整列引用与查找函数。"""

from __future__ import annotations

import ast
import contextvars
import math
import operator
import re
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import pandas as pd

# 当前行索引（仅在 safe_eval_vector 中设置；用于 CUMSUM_TO_HERE / CUMSUM_PREV）
_CURRENT_ROW_INDEX: "contextvars.ContextVar[int]" = contextvars.ContextVar(
    "_current_row_index", default=0
)

# 逐行引用：@T[col]（注意先匹配 @@ 再匹配 @，避免混淆）
_REF = re.compile(
    r"(?<!@)@(?!@)([\u4e00-\u9fffA-Za-z_][\u4e00-\u9fffA-Za-z0-9_]*)\[([\u4e00-\u9fffA-Za-z_][\u4e00-\u9fffA-Za-z0-9_]*)\]"
)
# 整列数组引用：@@T[col]
_AREF = re.compile(
    r"@@([\u4e00-\u9fffA-Za-z_][\u4e00-\u9fffA-Za-z0-9_]*)\[([\u4e00-\u9fffA-Za-z_][\u4e00-\u9fffA-Za-z0-9_]*)\]"
)


def parse_formula_refs(formula: str) -> Set[Tuple[str, str]]:
    """返回公式中所有 @T[c] 与 @@T[c] 引用的 (table, col) 集合。"""
    s = set(_REF.findall(formula))
    s |= set(_AREF.findall(formula))
    return s


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
    # Element-wise IF when condition is a list (e.g. from array comparison)
    if isinstance(cond, (list, pd.Series)):
        cond_list = list(cond)
        a_list = list(a) if isinstance(a, (list, pd.Series)) else None
        b_list = list(b) if isinstance(b, (list, pd.Series)) else None
        result = []
        for i, c in enumerate(cond_list):
            av = a_list[i] if a_list is not None else a
            bv = b_list[i] if b_list is not None else b
            result.append(av if _to_bool(c) else bv)
        return result
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


# ---------- 查找与引用函数 ----------

_NAN = float("nan")


def _coerce(v: Any) -> Any:
    """尝试转 float，失败保持原值用于字符串比较。"""
    try:
        return float(v)
    except (TypeError, ValueError):
        return v


def _values_equal(a: Any, b: Any) -> bool:
    try:
        return float(a) == float(b)
    except (TypeError, ValueError):
        return str(a) == str(b)


def _vlookup(
    lookup_val: Any,
    lookup_arr: List[Any],
    return_arr: List[Any],
    exact: Any = True,
) -> Any:
    """VLOOKUP(查找值, @@查找列, @@返回列, [exact=TRUE])
    exact=True(默认)：精确匹配；exact=False：近似（≤ 最大值，需升序排列）。
    未找到返回 NaN。"""
    if not isinstance(lookup_arr, (list, pd.Series)):
        raise ValueError("VLOOKUP 第 2 参须为 @@列 整列引用")
    if not isinstance(return_arr, (list, pd.Series)):
        raise ValueError("VLOOKUP 第 3 参须为 @@列 整列引用")
    exact_bool = _to_bool(exact) if not isinstance(exact, bool) else exact
    if exact_bool:
        for lv, rv in zip(lookup_arr, return_arr):
            if _values_equal(lv, lookup_val):
                return _coerce(rv)
        return _NAN
    else:
        # 近似：最后一个 <= lookup_val 的行
        result = _NAN
        for lv, rv in zip(lookup_arr, return_arr):
            try:
                if float(lv) <= float(lookup_val):
                    result = _coerce(rv)
                else:
                    break
            except (TypeError, ValueError):
                continue
        return result


def _xlookup(
    lookup_val: Any,
    lookup_arr: List[Any],
    return_arr: List[Any],
    if_not_found: Any = _NAN,
) -> Any:
    """XLOOKUP(查找值, @@查找列, @@返回列, [未找到时返回])  精确匹配。"""
    if not isinstance(lookup_arr, (list, pd.Series)):
        raise ValueError("XLOOKUP 第 2 参须为 @@列 整列引用")
    if not isinstance(return_arr, (list, pd.Series)):
        raise ValueError("XLOOKUP 第 3 参须为 @@列 整列引用")
    for lv, rv in zip(lookup_arr, return_arr):
        if _values_equal(lv, lookup_val):
            return _coerce(rv)
    return if_not_found


def _match(
    lookup_val: Any,
    lookup_arr: List[Any],
    match_type: Any = 0,
) -> Any:
    """MATCH(查找值, @@查找列, [match_type=0])
    match_type 0=精确；1=≤最大（升序）；-1=≥最小（降序）。
    返回 1 起的行号，未找到返回 NaN。"""
    if not isinstance(lookup_arr, (list, pd.Series)):
        raise ValueError("MATCH 第 2 参须为 @@列 整列引用")
    mt = int(float(match_type))
    if mt == 0:
        for i, lv in enumerate(lookup_arr):
            if _values_equal(lv, lookup_val):
                return float(i + 1)
        return _NAN
    elif mt == 1:
        result_i = _NAN
        for i, lv in enumerate(lookup_arr):
            try:
                if float(lv) <= float(lookup_val):
                    result_i = float(i + 1)
                else:
                    break
            except (TypeError, ValueError):
                continue
        return result_i
    else:  # -1
        result_i = _NAN
        for i, lv in enumerate(lookup_arr):
            try:
                if float(lv) >= float(lookup_val):
                    result_i = float(i + 1)
                else:
                    break
            except (TypeError, ValueError):
                continue
        return result_i


def _index_fn(arr: List[Any], row_num: Any, col_num: Any = None) -> Any:
    """INDEX(@@列, 行号)  行号 1 起。"""
    if not isinstance(arr, (list, pd.Series)):
        raise ValueError("INDEX 第 1 参须为 @@列 整列引用")
    idx = int(float(row_num)) - 1
    if 0 <= idx < len(arr):
        return _coerce(arr[idx] if isinstance(arr, list) else arr.iloc[idx])
    return _NAN


def _lookup(
    lookup_val: Any,
    lookup_arr: List[Any],
    return_arr: Optional[List[Any]] = None,
) -> Any:
    """LOOKUP(查找值, @@查找列, [@@返回列])  升序近似匹配，未提供返回列则返回查找列对应值。"""
    if not isinstance(lookup_arr, (list, pd.Series)):
        raise ValueError("LOOKUP 第 2 参须为 @@列 整列引用")
    ret = lookup_arr if return_arr is None else return_arr
    result = _NAN
    for lv, rv in zip(lookup_arr, ret):
        try:
            if float(lv) <= float(lookup_val):
                result = _coerce(rv)
            else:
                break
        except (TypeError, ValueError):
            continue
    return result


def _sum_arr(arr: Any) -> float:
    if isinstance(arr, pd.Series):
        return float(arr.sum())
    if isinstance(arr, (list, tuple)):
        return float(sum(float(x) for x in arr if x is not None))
    return float(arr)


def _average_arr(arr: Any) -> float:
    if isinstance(arr, pd.Series):
        return float(arr.mean())
    if isinstance(arr, (list, tuple)):
        vals = [float(x) for x in arr if x is not None]
        if not vals:
            raise ValueError("AVERAGE 空列")
        return sum(vals) / len(vals)
    return float(arr)


def _count_arr(arr: Any) -> float:
    if isinstance(arr, pd.Series):
        return float(arr.count())
    if isinstance(arr, (list, tuple)):
        return float(len([x for x in arr if x is not None]))
    return 1.0


def _counta_arr(arr: Any) -> float:
    if isinstance(arr, pd.Series):
        return float(arr.notna().sum())
    if isinstance(arr, (list, tuple)):
        return float(len([x for x in arr if x is not None]))
    return 1.0


def _arr_to_floats(arr: Any) -> List[float]:
    if isinstance(arr, pd.Series):
        return [float(x) if pd.notna(x) else 0.0 for x in arr.tolist()]
    if isinstance(arr, (list, tuple)):
        out: List[float] = []
        for x in arr:
            try:
                out.append(float(x) if x is not None else 0.0)
            except (TypeError, ValueError):
                out.append(0.0)
        return out
    try:
        return [float(arr)]
    except (TypeError, ValueError):
        return [0.0]


def _cumsum_to_here(arr: Any) -> float:
    """CUMSUM_TO_HERE(@@T[col])：累计求和（含本行）。仅在逐行求值时返回正确值。"""
    i = _CURRENT_ROW_INDEX.get()
    vals = _arr_to_floats(arr)
    return float(sum(vals[: i + 1]))


def _cumsum_prev(arr: Any) -> float:
    """CUMSUM_PREV(@@T[col])：截止上一行的累计求和（第 1 行 = 0）。"""
    i = _CURRENT_ROW_INDEX.get()
    vals = _arr_to_floats(arr)
    return float(sum(vals[:i]))


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
    # 查找与引用
    "vlookup": _vlookup,
    "xlookup": _xlookup,
    "match": _match,
    "index": _index_fn,
    "lookup": _lookup,
    # 聚合（作用于 @@列 整列）
    "sum": _sum_arr,
    "average": _average_arr,
    "avg": _average_arr,
    "count": _count_arr,
    "counta": _counta_arr,
    # 累计求和（用于副本/养成累计门票/累计消耗等场景）
    "cumsum_to_here": _cumsum_to_here,
    "cumsum_prev": _cumsum_prev,
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
            # Element-wise broadcast: list vs scalar, scalar vs list, or list vs list
            left_is_arr = isinstance(left, (list, pd.Series))
            right_is_arr = isinstance(right, (list, pd.Series))
            if left_is_arr or right_is_arr:
                lv = _arr_to_floats(left) if left_is_arr else None
                rv = _arr_to_floats(right) if right_is_arr else None
                if lv is not None and rv is not None:
                    left = [cmp(a, b) for a, b in zip(lv, rv)]
                elif lv is not None:
                    r = float(right)
                    left = [cmp(a, r) for a in lv]
                else:
                    l = float(left)
                    left = [cmp(l, b) for b in rv]
            else:
                if not cmp(left, right):
                    return False
                left = right
        return left
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


def safe_eval_scalar(expr: str, names: Dict[str, Any]) -> Any:
    """对表达式求值，允许返回 float / bool / list（整列聚合时）。"""
    tree = ast.parse(expr, mode="eval")
    for n in ast.walk(tree):
        if not isinstance(n, _ALLOWED_NODES):
            raise ValueError(f"禁止的语法: {type(n).__name__}")
    return _eval_ast(tree, names)


def substitute_refs(
    formula: str,
    *,
    frames: Dict[str, pd.DataFrame],
) -> Tuple[str, Dict[str, pd.Series], Dict[str, list]]:
    """将 @T[c] 替换为 __s<n>（逐行标量），@@T[c] 替换为 __a<n>（整列 list）。
    返回 (表达式, scalar_map, array_map)。"""
    scalar_map: Dict[str, pd.Series] = {}
    array_map: Dict[str, list] = {}
    out = formula
    s_idx = 0
    a_idx = 0

    # 先处理 @@（双@，整列引用），避免被单@ regex 误匹配
    for tbl, col in _AREF.findall(formula):
        token = f"@@{tbl}[{col}]"
        if token in out:  # 可能同一引用多次
            if tbl not in frames:
                raise ValueError(f"未加载表 {tbl}")
            df = frames[tbl]
            if col not in df.columns:
                raise ValueError(f"表 {tbl} 无列 {col}")
            # 查是否已分配
            existing = next(
                (k for k, v in array_map.items() if v is not None and
                 id(v) == id(getattr(df[col], "tolist", lambda: None)())), None
            )
            key = f"__a{a_idx}"
            a_idx += 1
            array_map[key] = pd.to_numeric(df[col], errors="coerce").tolist()
            out = out.replace(token, key)

    # 再处理 @（逐行引用）
    for tbl, col in _REF.findall(formula):
        token = f"@{tbl}[{col}]"
        if token not in out:
            continue
        if tbl not in frames:
            raise ValueError(f"未加载表 {tbl}")
        df = frames[tbl]
        if col not in df.columns:
            raise ValueError(f"表 {tbl} 无列 {col}")
        key = f"__s{s_idx}"
        s_idx += 1
        scalar_map[key] = pd.to_numeric(df[col], errors="coerce")
        out = out.replace(token, key)

    return out, scalar_map, array_map


def eval_series(formula: str, frames: Dict[str, pd.DataFrame]) -> pd.Series:
    expr, scalar_map, array_map = substitute_refs(formula, frames=frames)
    if not scalar_map and not array_map:
        v = safe_eval_scalar(expr, {})
        return pd.Series([v] * max(len(df) for df in frames.values()))
    # 累计求和需要逐行求值（即便没有 scalar_map）
    needs_rowwise = bool(re.search(r"\bcumsum_(to_here|prev)\b", expr.lower()))
    if scalar_map:
        first = next(iter(scalar_map.values()))
        n = len(first)
        for k, s in scalar_map.items():
            if len(s) != n:
                raise ValueError("引用列长度不一致")
        return pd.Series(safe_eval_vector(expr, scalar_map, array_map))
    if needs_rowwise:
        n = max(len(df) for df in frames.values())
        static_env: Dict[str, Any] = {k: v for k, v in array_map.items()}
        out: List[Any] = []
        for i in range(n):
            token = _CURRENT_ROW_INDEX.set(i)
            try:
                out.append(safe_eval_scalar(expr, static_env))
            finally:
                _CURRENT_ROW_INDEX.reset(token)
        return pd.Series(out)
    # 只有 array_map（纯聚合公式），返回全表相同值
    n = max(len(df) for df in frames.values())
    static_env = {k: v for k, v in array_map.items()}
    v = safe_eval_scalar(expr, static_env)
    return pd.Series([float(v)] * n)


# 同行列引用：@col_name（无表前缀，无括号），不匹配 @T[col] 与 @@T[col]
_SAME_ROW_REF = re.compile(
    r"(?<!@)@(?!@)([\u4e00-\u9fffA-Za-z_][\u4e00-\u9fffA-Za-z0-9_]*)(?!\[)"
)


def preprocess_formula(formula: str) -> str:
    """将 Excel 风格运算符（大写 AND/OR/NOT/TRUE/FALSE）转为 Python 兼容写法。"""
    formula = re.sub(r"\bAND\b", "and", formula)
    formula = re.sub(r"\bOR\b", "or", formula)
    formula = re.sub(r"\bNOT\b", "not", formula)
    formula = re.sub(r"\bTRUE\b", "True", formula)
    formula = re.sub(r"\bFALSE\b", "False", formula)
    return formula


# ${name} 常数引用（与 $name$ 术语占位区分）
_CONST_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def parse_constant_refs(formula: str) -> Set[str]:
    """返回公式串中所有 ${name} 常量引用的名字集合。"""
    return set(_CONST_REF.findall(formula))


def substitute_constants(formula: str, constants: Dict[str, Any]) -> Tuple[str, List[str]]:
    """把 ${name} 替换为 constants[name] 的字面量；返回 (新公式, 未解析名列表)。"""
    missing: List[str] = []

    def _rep(m: "re.Match[str]") -> str:
        nm = m.group(1)
        if nm not in constants:
            missing.append(nm)
            return m.group(0)
        v = constants[nm]
        # 数值直接转字符串；其他类型回退为 0
        try:
            return repr(float(v))
        except (TypeError, ValueError):
            try:
                return repr(int(v))
            except (TypeError, ValueError):
                missing.append(nm)
                return m.group(0)

    out = _CONST_REF.sub(_rep, formula)
    return out, missing


def parse_row_refs(formula: str) -> Set[str]:
    """返回公式中所有同行列引用 @col（无表前缀、无括号）的列名集合。"""
    return set(_SAME_ROW_REF.findall(formula))


def eval_row_formula(
    formula: str,
    row_dict: Dict[str, Any],
    available_cols: Set[str],
) -> Tuple[Any, Set[str]]:
    """对单行求值同行列公式（@col_name 语法）。
    返回 (值, 外部参数集合)。若外部参数集合非空，表示公式无法自动计算（运行时模板）。
    """
    refs = set(_SAME_ROW_REF.findall(formula))
    external_refs = {r for r in refs if r not in available_cols}
    if external_refs:
        return None, external_refs

    expr = preprocess_formula(formula)
    name_map: Dict[str, Any] = {}
    ref_to_key: Dict[str, str] = {}
    for i, ref in enumerate(sorted(refs)):
        key = f"__r{i}__"
        ref_to_key[ref] = key
        val = row_dict.get(ref)
        try:
            name_map[key] = float(val) if val is not None else 0.0
        except (TypeError, ValueError):
            name_map[key] = 0.0

    for ref, key in ref_to_key.items():
        expr = re.sub(r"@" + re.escape(ref) + r"(?!\[)", key, expr)

    try:
        result = safe_eval_scalar(expr, name_map)
        return result, set()
    except Exception as e:  # noqa: BLE001
        return None, {f"__eval_error__: {e}"}


def safe_eval_vector(
    expr: str,
    scalar_map: Dict[str, pd.Series],
    array_map: Optional[Dict[str, list]] = None,
) -> list:
    """逐行应用 safe_eval_scalar；array_map 中的整列引用每行保持不变。"""
    if not scalar_map:
        static_env: Dict[str, Any] = dict(array_map or {})
        return [safe_eval_scalar(expr, static_env)] * 1
    n = len(next(iter(scalar_map.values())))
    static: Dict[str, Any] = dict(array_map or {})
    out = []
    for i in range(n):
        env: Dict[str, Any] = {k: float(v.iloc[i]) for k, v in scalar_map.items()}
        env.update(static)
        token = _CURRENT_ROW_INDEX.set(i)
        try:
            out.append(safe_eval_scalar(expr, env))
        finally:
            _CURRENT_ROW_INDEX.reset(token)
    return out
