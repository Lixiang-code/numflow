"""Calculator 注册与查询。

第3轮优化：用户希望 AI 把 matrix 表（甚至普通表）注册成
``fun(level, gameplay, attr)`` 这样的查询入口，并对 brief 强制要求。
"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, List, Optional, Sequence


_VALID_KINDS = {"matrix_attr", "matrix_resource", "lookup"}


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def register_calculator(
    conn: sqlite3.Connection,
    *,
    name: str,
    kind: str,
    table_name: str,
    axes: Sequence[Dict[str, str]],     # [{name, source}]：调用形参 → 数据库列
    value_column: str = "value",
    brief: str = "",
    grain: Optional[str] = None,        # matrix_resource: hourly/per_level/cumulative
) -> Dict[str, Any]:
    if kind not in _VALID_KINDS:
        raise ValueError(f"未知 calculator kind={kind}（允许：{sorted(_VALID_KINDS)}）")
    if not name or not name.replace("_", "").isalnum():
        raise ValueError(f"calculator 名称非法：{name!r}（要求 a-z/0-9/_）")
    if not brief or len(brief.strip()) < 8:
        raise ValueError("brief 至少 8 字符，必须说明本 calculator 的用途、入参语义、返回值含义")
    cur = conn.execute("SELECT 1 FROM _table_registry WHERE table_name = ?", (table_name,))
    if not cur.fetchone():
        raise ValueError(f"目标表 {table_name} 不在 _table_registry")

    payload_axes = [dict(a) for a in axes]
    if grain:
        payload_axes.append({"name": "grain", "source": "_grain", "default": grain})
    now = _now()
    conn.execute(
        """
        INSERT INTO _calculators (name, kind, table_name, axes_json, value_column, brief, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(name) DO UPDATE SET
            kind = excluded.kind,
            table_name = excluded.table_name,
            axes_json = excluded.axes_json,
            value_column = excluded.value_column,
            brief = excluded.brief,
            updated_at = excluded.updated_at
        """,
        (name, kind, table_name, json.dumps(payload_axes, ensure_ascii=False),
         value_column, brief.strip(), now, now),
    )
    conn.commit()
    return {"ok": True, "name": name, "axes": payload_axes}


def list_calculators(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    cur = conn.execute(
        "SELECT name, kind, table_name, axes_json, value_column, brief, updated_at FROM _calculators ORDER BY name"
    )
    out: List[Dict[str, Any]] = []
    for r in cur.fetchall():
        out.append({
            "name": r[0],
            "kind": r[1],
            "table_name": r[2],
            "axes": json.loads(r[3] or "[]"),
            "value_column": r[4],
            "brief": r[5] or "",
            "updated_at": r[6],
        })
    return out


def call_calculator(
    conn: sqlite3.Connection,
    *,
    name: str,
    kwargs: Dict[str, Any],
) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT kind, table_name, axes_json, value_column FROM _calculators WHERE name = ?",
        (name,),
    )
    r = cur.fetchone()
    if not r:
        return {"ok": False, "error": f"未知 calculator {name!r}"}
    kind, table_name, axes_json, value_column = r
    axes = json.loads(axes_json or "[]")

    where: List[str] = []
    params: List[Any] = []
    grain_value: Optional[str] = None
    for a in axes:
        nm = a.get("name")
        src = a.get("source")
        if nm == "grain":
            grain_value = str(kwargs.get("grain") or a.get("default") or "")
            continue
        if nm not in kwargs:
            continue
        v = kwargs.get(nm)
        if v is None or v == "":
            continue
        where.append(f'"{src}" = ?')
        params.append(v)

    # matrix_resource 三档（hourly / per_level / cumulative）通过 col_axis 值选择
    # 由调用方在 kwargs 直接传 res_id + grain；calculator 把 grain 拼到 col 选择里
    sel_col = value_column
    if kind == "matrix_resource" and grain_value:
        # 约定：col 名为 res_id；行存 per_minute/amount_this_level/amount_cumulative 三种 col 之一
        # 这里是直接按列名规约：col_axis 值为 res_id::grain
        params_with_grain: List[Any] = []
        new_where: List[str] = []
        for w, pv in zip(where, params):
            new_where.append(w); params_with_grain.append(pv)
        where = new_where
        params = params_with_grain
        # 简化：grain 由调用者写到 col 值后缀 res_id::hourly
        # 这里不再做特殊拼接，让上层 calculator 注册时把 grain 编码到 col 字段值

    sql = f'SELECT "{sel_col}" FROM "{table_name}"' + (" WHERE " + " AND ".join(where) if where else "") + " LIMIT 1"
    try:
        rr = conn.execute(sql, params).fetchone()
    except sqlite3.OperationalError as e:
        return {"ok": False, "error": f"查询失败: {e}", "sql": sql}
    if not rr:
        return {"ok": True, "value": None, "found": False, "sql": sql, "params": params}
    return {"ok": True, "value": rr[0], "found": True}


def delete_calculator(conn: sqlite3.Connection, name: str) -> Dict[str, Any]:
    conn.execute("DELETE FROM _calculators WHERE name = ?", (name,))
    conn.commit()
    return {"ok": True, "name": name}
