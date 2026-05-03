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
    """IFS/PIECEWISE(cond1, val1, cond2, val2, ..., default)
    依次取第一个为真的 cond 对应 val；末位为默认值。"""
    if len(args) < 1:
        raise ValueError("IFS/PIECEWISE 至少需要 1 个参数（cond1,val1,...,default）")
    default = args[-1]
    pairs = args[:-1]
    if len(pairs) % 2 != 0:
        raise ValueError("IFS/PIECEWISE 需要 cond/val 配对加默认值，参数个数应为奇数（cond1,val1,...,default）")
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
    exact: Any = False,
) -> Any:
    """VLOOKUP(查找值, @@查找列, @@返回列, [exact=FALSE])
    与 Excel 语义一致：FALSE/0=精确匹配；TRUE/1=近似匹配（≤ 最大值，需升序排列）。
    默认精确匹配。未找到返回 NaN。"""
    if not isinstance(lookup_arr, (list, pd.Series)):
        raise ValueError("VLOOKUP 第 2 参须为 @@列 整列引用")
    if not isinstance(return_arr, (list, pd.Series)):
        raise ValueError("VLOOKUP 第 3 参须为 @@列 整列引用")
    # Excel 语义：FALSE/0 → 精确匹配，TRUE/1 → 近似匹配
    is_exact = not (_to_bool(exact) if not isinstance(exact, bool) else exact)
    if is_exact:
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


def _concat(*args: Any) -> str:
    """concat(s1, s2, ...)：将所有参数转为字符串后拼接。"""
    return "".join(str(a) for a in args)


def _text(val: Any) -> str:
    """text(val)：将数值转为字符串文本。"""
    return str(val)


def _num(val: Any) -> float:
    """num(val)：将字符串转为数值。"""
    try:
        return float(val)
    except (TypeError, ValueError):
        raise ValueError(f"num() 无法将 {val!r} 转为数值") from None


def _interp(x: float, *points: Any) -> float:
    """interp(x, x1, y1, x2, y2, ...)：分段线性插值。
    定位 x 在点对序列中的区间，线性插值计算 y。
    超出范围时夹持到首/末点对的 y 值。
    x1..xn 必须单调递增，否则报错。
    """
    if len(points) < 4 or len(points) % 2 != 0:
        raise ValueError("interp 需要至少 2 组点对（x1,y1,x2,y2）")
    pairs = [(float(points[i]), float(points[i + 1])) for i in range(0, len(points), 2)]
    for i in range(1, len(pairs)):
        if pairs[i][0] < pairs[i - 1][0]:
            raise ValueError(f"interp 的 x 点必须单调递增，但 x{i}={pairs[i][0]} < x{i-1}={pairs[i-1][0]}")
    if x <= pairs[0][0]:
        return pairs[0][1]
    if x >= pairs[-1][0]:
        return pairs[-1][1]
    for i in range(len(pairs) - 1):
        x1, y1 = pairs[i]
        x2, y2 = pairs[i + 1]
        if x1 <= x <= x2:
            return y1 + (y2 - y1) * (x - x1) / (x2 - x1)
    return pairs[-1][1]  # 兜底


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
    # 字符串操作
    "concat": _concat,
    "text": _text,
    "num": _num,
    # 插值
    "interp": _interp,
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
        if isinstance(node.value, (int, float, bool, str)) or node.value is None:
            return node.value
        raise ValueError("仅允许数字/布尔/字符串常量")
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
                lv = list(left) if left_is_arr else None
                rv = list(right) if right_is_arr else None
                if lv is not None and rv is not None:
                    left = [cmp(a, b) for a, b in zip(lv, rv)]
                elif lv is not None:
                    left = [cmp(a, right) for a in lv]
                else:
                    left = [cmp(left, b) for b in rv]
            else:
                if not cmp(left, right):
                    return False
                left = right
        # 单个比较操作返回布尔值；链式比较保持现有行为（返回最后右值，truthy/falsy 语义）
        if len(node.ops) == 1 and not isinstance(left, (list, pd.Series)):
            return bool(left)
        return left
    if isinstance(node, ast.IfExp):
        cond = _eval_ast(node.test, names)
        return _eval_ast(node.body if _to_bool(cond) else node.orelse, names)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        fname = node.func.id.lower()
        fn = _FUNCTIONS.get(fname)
        # interp 端点 y 值必须是公式或常量引用，禁止裸数字硬编码
        if fname == "interp":
            for i, a in enumerate(node.args):
                if i >= 2 and i % 2 == 0:  # y 值：args[2], args[4], args[6]...
                    if isinstance(a, ast.Constant) and isinstance(a.value, (int, float)):
                        raise ValueError(
                            f"interp 端点值 {a.value!r} 禁止裸数字，"
                            f"请改为对常量的引用（先 const_register 再 ${{name}}）或公式表达式"
                        )
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
            array_map[key] = df[col].tolist()
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
        scalar_map[key] = df[col]
        out = out.replace(token, key)

    return out, scalar_map, array_map


def eval_series(formula: str, frames: Dict[str, pd.DataFrame]) -> pd.Series:
    formula = preprocess_formula(formula)
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
# 使用 (?![\[\w]) 防止回溯匹配：@other_table[hp] 不会误匹配 @other_tabl
_SAME_ROW_REF = re.compile(
    r"(?<!@)@(?!@)([\u4e00-\u9fffA-Za-z_][\u4e00-\u9fffA-Za-z0-9_]*)(?![\[\w])"
)


_POW_CARET = re.compile(r"\^")


def normalize_self_table_refs(formula: str, table_name: str) -> str:
    """把 @T[col] / @this[col] / @@T[col] / @@this[col] / 裸 @col 统一重写为完整表名引用。

    AI 经常使用 ``@T[col]``、``@this[col]`` 或裸 ``@col`` 这三种写法；
    此处统一把它们重写成 ``@table_name[col]``，避免引擎找不到表或语法不匹配。
    
    **顺序重要**：先处理裸 @col（无括号），再处理 @T[col]/@this[col]（有括号），
    避免已转换的 @table_name[col] 被 _SAME_ROW_REF 回溯匹配。
    """
    if not table_name:
        return formula
    # 第1步：裸 @col → @table_name[col]（先处理，因为不含括号不会干扰后续转换）
    formula = _SAME_ROW_REF.sub(
        lambda m: f"@{table_name}[{m.group(1)}]", formula
    )
    # 第2步：@@T[col] / @@this[col] → @@table_name[col]（双@ 整列引用）
    formula = re.sub(r"@@T\[", f"@@{table_name}[", formula)
    formula = re.sub(r"@@this\[", f"@@{table_name}[", formula)
    # 第3步：@T[col] / @this[col] → @table_name[col]（单@ 逐行引用）
    formula = re.sub(r"(?<!@)@T\[", f"@{table_name}[", formula)
    formula = re.sub(r"(?<!@)@this\[", f"@{table_name}[", formula)
    return formula


def preprocess_formula(formula: str) -> str:
    """将 Excel 风格运算符（大写 AND/OR/NOT/TRUE/FALSE、^ 幂、|| 拼接、=比较）转为 Python 兼容写法。"""
    # ^ 在 Python AST 中是按位异或；公式里几乎都意为"幂"，统一改为 **
    formula = _POW_CARET.sub("**", formula)
    formula = re.sub(r"\bAND\b", "and", formula)
    formula = re.sub(r"\bOR\b", "or", formula)
    formula = re.sub(r"\bNOT\b", "not", formula)
    formula = re.sub(r"\bTRUE\b", "True", formula)
    formula = re.sub(r"\bFALSE\b", "False", formula)
    # = 转为 ==（仅转换比较运算符，保留 !=、<=、>=、==）
    formula = re.sub(r"(?<![!<>=])=(?!=)", "==", formula)
    # || 字符串拼接 → concat() 函数调用（支持链式如 'a' || 'b' || 'c'）
    if "||" in formula:
        parts = [p.strip() for p in formula.split("||")]
        formula = "concat(" + ", ".join(parts) + ")"
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
        # 尝试数值，失败则尝试字符串字面量
        try:
            return repr(float(v))
        except (TypeError, ValueError):
            try:
                return repr(int(v))
            except (TypeError, ValueError):
                try:
                    if isinstance(v, str):
                        return repr(v)
                    return repr(float(v))
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
        name_map[key] = val if val is not None else 0.0

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
        env: Dict[str, Any] = {k: v.iloc[i] for k, v in scalar_map.items()}
        env.update(static)
        token = _CURRENT_ROW_INDEX.set(i)
        try:
            out.append(safe_eval_scalar(expr, env))
        finally:
            _CURRENT_ROW_INDEX.reset(token)
    return out
