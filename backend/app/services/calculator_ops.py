"""Calculator 注册与查询。

第3轮优化：用户希望 AI 把 matrix 表（甚至普通表）注册成
``fun(level, gameplay, attr)`` 这样的查询入口，并对 brief 强制要求。
"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, List, Optional, Sequence

from app.services.matrix_table_ops import _matrix_resource_state, evaluate_matrix_formula_value


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
    if not brief or not brief.strip():
        raise ValueError("brief 必填，应说明本 calculator 的用途、入参语义、返回值含义")
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

    # 查出 matrix 的 scale_mode（若是 matrix 表）
    scale_mode = "static"
    mm: Dict[str, Any] = {}
    try:
        mm_row = conn.execute(
            "SELECT matrix_meta_json FROM _table_registry WHERE table_name = ?", (table_name,)
        ).fetchone()
        if mm_row and mm_row[0]:
            mm = json.loads(mm_row[0])
            scale_mode = mm.get("scale_mode") or "static"
    except Exception:  # noqa: BLE001
        pass

    where: List[str] = []
    params: List[Any] = []
    grain_value: Optional[str] = None
    level_value: Optional[Any] = None

    for a in axes:
        nm = a.get("name")
        src = a.get("source")
        if nm == "grain":
            grain_value = str(kwargs.get("grain") or a.get("default") or "")
            continue
        if nm == "level":
            # scale_mode=none → 跳过 level，不加入 WHERE（查 level=NULL 的行）
            if scale_mode == "none":
                continue
            level_value = kwargs.get("level")
            if level_value is not None and str(level_value) != "":
                where.append(f'"{src}" = ?')
                params.append(int(level_value))
            continue
        if nm not in kwargs:
            continue
        v = kwargs.get(nm)
        if v is None or v == "":
            continue
        where.append(f'"{src}" = ?')
        params.append(v)

    sel_col = value_column
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    sql = f'SELECT "{sel_col}" FROM "{table_name}"{where_sql} LIMIT 1'

    try:
        rr = conn.execute(sql, params).fetchone()
    except sqlite3.OperationalError as e:
        return {"ok": False, "error": f"查询失败: {e}", "sql": sql}

    if not rr and mm.get("kind") == "matrix_resource" and level_value is not None:
        row_axis = str(mm.get("row_axis") or "")
        col_axis = str(mm.get("col_axis") or "")
        row_key = kwargs.get(row_axis)
        col_key = kwargs.get(col_axis)
        if row_axis and col_axis and row_key not in (None, "") and col_key not in (None, ""):
            formula_result = evaluate_matrix_formula_value(
                conn,
                table_name=table_name,
                row_axis=row_axis,
                col_axis=col_axis,
                row_key=str(row_key),
                col_key=str(col_key),
                level=int(level_value),
                extra_env={str(k): v for k, v in kwargs.items()},
            )
            if formula_result.get("ok"):
                return {
                    "ok": True,
                    "value": formula_result.get("value"),
                    "found": True,
                    "source": "formula",
                    "formula": formula_result.get("formula"),
                    "formula_type": formula_result.get("type"),
                }
            if formula_result.get("found"):
                return {
                    "ok": False,
                    "error": str(formula_result.get("error") or "matrix_resource 公式计算失败"),
                    "formula": formula_result.get("formula"),
                    "formula_type": formula_result.get("type"),
                }

    # fallback 模式：精确 level 找不到时，若该表未进入公式模式，则回退查 level=NULL 的基准值
    if not rr and scale_mode == "fallback" and level_value is not None:
        formula_mode = False
        if mm.get("kind") == "matrix_resource":
            formula_mode = bool(_matrix_resource_state(conn, table_name=table_name).get("formula_count"))
        if formula_mode:
            return {"ok": True, "value": None, "found": False, "reason": "formula_mode_no_literal_fallback"}
        fallback_where = [w for w, _ in zip(where, params) if '"level"' not in w]
        fallback_params = [p for w, p in zip(where, params) if '"level"' not in w]
        fb_sql = (
            f'SELECT "{sel_col}" FROM "{table_name}"'
            + (" WHERE " + " AND ".join(fallback_where) + " AND level IS NULL" if fallback_where else " WHERE level IS NULL")
            + " LIMIT 1"
        )
        try:
            rr = conn.execute(fb_sql, fallback_params).fetchone()
        except sqlite3.OperationalError:
            pass
        if rr:
            return {"ok": True, "value": rr[0], "found": True, "fallback": True}

    if not rr:
        default_value = mm.get("default_value")
        if default_value is not None:
            return {"ok": True, "value": default_value, "found": False, "source": "default"}
        return {"ok": True, "value": None, "found": False, "sql": sql, "params": params}
    return {"ok": True, "value": rr[0], "found": True}


def delete_calculator(conn: sqlite3.Connection, name: str) -> Dict[str, Any]:
    conn.execute("DELETE FROM _calculators WHERE name = ?", (name,))
    conn.commit()
    return {"ok": True, "name": name}
