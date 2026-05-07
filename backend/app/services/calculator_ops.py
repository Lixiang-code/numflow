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
    cur = conn.execute(
        "SELECT matrix_meta_json FROM _table_registry WHERE table_name = ?",
        (table_name,),
    )
    table_row = cur.fetchone()
    if not table_row:
        raise ValueError(f"目标表 {table_name} 不在 _table_registry")
    try:
        mm = json.loads((table_row["matrix_meta_json"] if isinstance(table_row, sqlite3.Row) else table_row[0]) or "{}") or {}
    except Exception:  # noqa: BLE001
        mm = {}
    mm_kind = str(mm.get("kind") or "")

    payload_axes = []
    normalized_axes = False
    for axis in axes:
        axis_item = dict(axis)
        src = str(axis_item.get("source") or "").strip()
        if mm_kind in {"matrix_attr", "matrix_resource"}:
            if src == "row" and mm.get("row_axis"):
                axis_item["source"] = str(mm.get("row_axis"))
                normalized_axes = True
            elif src == "col" and mm.get("col_axis"):
                axis_item["source"] = str(mm.get("col_axis"))
                normalized_axes = True
        elif mm_kind == "3d_matrix":
            dim1_col = str(((mm.get("dim1") or {}).get("col_name")) or "").strip()
            dim2_col = str(((mm.get("dim2") or {}).get("col_name")) or "").strip()
            if src in {"dim1", "@dim1", "dim1_key"} and dim1_col:
                axis_item["source"] = dim1_col
                normalized_axes = True
            elif src in {"dim2", "@dim2", "dim2_key"} and dim2_col:
                axis_item["source"] = dim2_col
                normalized_axes = True
        payload_axes.append(axis_item)
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
    # 自动给业务表补复合索引（基于本次注册的 axes）。失败不影响注册结果。
    try:
        from app.db.project_migrations import ensure_calculator_indexes
        ensure_calculator_indexes(conn)
        conn.commit()
    except Exception:  # noqa: BLE001
        pass
    result = {"ok": True, "name": name, "axes": payload_axes}
    warnings: List[str] = []
    if normalized_axes:
        warnings.append("已将 axes.source 的简写别名自动展开为目标表的实际列名。")
    if mm_kind == "3d_matrix" and kind != "lookup":
        warnings.append("目标表是 3D 表，register_calculator 的 kind 建议使用 lookup。")
    if warnings:
        result["warnings"] = warnings
    return result


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


def get_calculator_meta(conn: sqlite3.Connection, name: str) -> Optional[Dict[str, Any]]:
    cur = conn.execute(
        "SELECT kind, table_name, axes_json, value_column FROM _calculators WHERE name = ?",
        (name,),
    )
    r = cur.fetchone()
    if not r:
        return None
    kind, table_name, axes_json, value_column = r
    axes = json.loads(axes_json or "[]")

    scale_mode = "static"
    mm: Dict[str, Any] = {}
    try:
        mm_row = conn.execute(
            "SELECT matrix_meta_json FROM _table_registry WHERE table_name = ?",
            (table_name,),
        ).fetchone()
        if mm_row and mm_row[0]:
            mm = json.loads(mm_row[0])
            scale_mode = mm.get("scale_mode") or "static"
    except Exception:  # noqa: BLE001
        pass

    return {
        "name": name,
        "kind": kind,
        "table_name": table_name,
        "axes": axes,
        "value_column": value_column,
        "matrix_meta": mm,
        "scale_mode": scale_mode,
    }


def call_calculator(
    conn: sqlite3.Connection,
    *,
    name: str,
    kwargs: Dict[str, Any],
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    meta_info = dict(meta or {})
    if not meta_info:
        loaded = get_calculator_meta(conn, name)
        if loaded is None:
            return {"ok": False, "error": f"未知 calculator {name!r}"}
        meta_info = loaded
    if not meta_info:
        return {"ok": False, "error": f"未知 calculator {name!r}"}
    kind = meta_info["kind"]
    table_name = meta_info["table_name"]
    axes = list(meta_info.get("axes") or [])
    value_column = meta_info["value_column"]
    scale_mode = str(meta_info.get("scale_mode") or "static")
    mm = dict(meta_info.get("matrix_meta") or {})

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
            # name 未命中时，再尝试 source 列名作为调用键（兼容 AI 直接用数据库列名调用）
            if src and src in kwargs:
                nm = src
            else:
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
