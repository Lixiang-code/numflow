"""Matrix（行/列双向语义）表实现。

第3轮优化新增：让 AI 创建 "行=玩法、列=属性/资源" 的分配表。
存储仍走 SQLite 长表（row_axis_value, col_axis_value, level, value, note），
但前端展示与 AI 读写都按宽表（rows × cols）做。

每张 matrix 表都会自动创建一个 calculator，让 AI 用 fun(level, row, col) 取值。
"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.util.identifiers import assert_english_ident


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_matrix_formula_registry(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _matrix_formula_registry (
            table_name TEXT NOT NULL,
            row_key TEXT NOT NULL,
            col_key TEXT NOT NULL,
            formula TEXT NOT NULL,
            formula_type TEXT NOT NULL DEFAULT 'row',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (table_name, row_key, col_key)
        )
        """
    )


def _load_matrix_formulas(
    conn: sqlite3.Connection,
    *,
    table_name: str,
) -> Dict[Tuple[str, str], Dict[str, str]]:
    ensure_matrix_formula_registry(conn)
    cur = conn.execute(
        """
        SELECT row_key, col_key, formula, formula_type
        FROM _matrix_formula_registry
        WHERE table_name = ?
        """,
        (table_name,),
    )
    out: Dict[Tuple[str, str], Dict[str, str]] = {}
    for row_key, col_key, formula, formula_type in cur.fetchall():
        out[(str(row_key), str(col_key))] = {
            "formula": str(formula or ""),
            "type": str(formula_type or "row"),
        }
    return out


def _classify_matrix_formula(
    conn: sqlite3.Connection,
    *,
    row_axis: str,
    col_axis: str,
    raw_formula: str,
) -> str:
    from app.services.formula_engine import parse_constant_refs, parse_row_refs, substitute_constants
    from app.services.formula_exec import _load_constants

    refs = parse_row_refs(raw_formula)
    allowed_refs = {"level", row_axis, col_axis}
    external_refs = refs - allowed_refs
    const_names = parse_constant_refs(raw_formula)
    constants, missing_consts = _load_constants(conn, const_names)
    if const_names and not missing_consts:
        _, missing_after_substitute = substitute_constants(raw_formula, constants)
        missing_consts = list(set(missing_consts) | set(missing_after_substitute))
    return "row" if not external_refs and not missing_consts else "row_template"


def _upsert_matrix_formula(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    row_key: str,
    col_key: str,
    row_axis: str,
    col_axis: str,
    formula: str,
) -> str:
    ensure_matrix_formula_registry(conn)
    formula_type = _classify_matrix_formula(
        conn,
        row_axis=row_axis,
        col_axis=col_axis,
        raw_formula=formula,
    )
    now = _now()
    conn.execute(
        """
        INSERT INTO _matrix_formula_registry
            (table_name, row_key, col_key, formula, formula_type, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?)
        ON CONFLICT(table_name, row_key, col_key) DO UPDATE SET
            formula = excluded.formula,
            formula_type = excluded.formula_type,
            updated_at = excluded.updated_at
        """,
        (table_name, row_key, col_key, formula, formula_type, now, now),
    )
    return formula_type


def _delete_matrix_formula(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    row_key: str,
    col_key: str,
) -> None:
    ensure_matrix_formula_registry(conn)
    conn.execute(
        """
        DELETE FROM _matrix_formula_registry
        WHERE table_name = ? AND row_key = ? AND col_key = ?
        """,
        (table_name, row_key, col_key),
    )


def _matrix_resource_state(conn: sqlite3.Connection, *, table_name: str) -> Dict[str, Any]:
    ensure_matrix_formula_registry(conn)
    formula_count = int(
        conn.execute(
            "SELECT COUNT(1) FROM _matrix_formula_registry WHERE table_name = ?",
            (table_name,),
        ).fetchone()[0]
    )
    cur = conn.execute(
        f'''
        SELECT level, COUNT(1)
        FROM "{table_name}"
        WHERE value IS NOT NULL
        GROUP BY level
        '''
    )
    literal_levels: List[Optional[int]] = []
    literal_rows = 0
    for level, count in cur.fetchall():
        literal_levels.append(None if level is None else int(level))
        literal_rows += int(count or 0)
    return {
        "formula_count": formula_count,
        "literal_rows": literal_rows,
        "has_base_literal": any(level is None for level in literal_levels),
        "explicit_levels": {int(level) for level in literal_levels if level is not None},
    }


def evaluate_matrix_formula_value(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    row_axis: str,
    col_axis: str,
    row_key: str,
    col_key: str,
    level: Optional[int],
    extra_env: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    from app.services.formula_engine import eval_row_formula, parse_constant_refs, substitute_constants
    from app.services.formula_exec import _load_constants

    formula_entry = _load_matrix_formulas(conn, table_name=table_name).get((row_key, col_key))
    if not formula_entry:
        return {"ok": False, "found": False}

    formula = formula_entry["formula"]
    formula_type = formula_entry["type"]
    const_names = parse_constant_refs(formula)
    constants, missing_consts = _load_constants(conn, const_names)
    compiled = formula
    if const_names and not missing_consts:
        compiled, missing_after_substitute = substitute_constants(formula, constants)
        missing_consts = list(set(missing_consts) | set(missing_after_substitute))
    if missing_consts:
        return {
            "ok": False,
            "found": True,
            "formula": formula,
            "type": formula_type,
            "error": f"缺少常量：{', '.join(sorted(set(missing_consts)))}",
        }

    row_dict: Dict[str, Any] = {
        row_axis: row_key,
        col_axis: col_key,
        "level": 0 if level is None else level,
    }
    if extra_env:
        for key, value in extra_env.items():
            if value is not None and key not in row_dict:
                row_dict[str(key)] = value
    value, missing_refs = eval_row_formula(compiled, row_dict, set(row_dict.keys()))
    if missing_refs:
        return {
            "ok": False,
            "found": True,
            "formula": formula,
            "type": "row_template",
            "error": f"缺少参数：{', '.join(sorted(missing_refs))}",
        }
    try:
        value = round(float(value), 6) if value is not None else None
    except (TypeError, ValueError):
        pass
    return {
        "ok": True,
        "found": True,
        "value": value,
        "formula": formula,
        "type": formula_type,
    }


_KIND_META: Dict[str, Dict[str, str]] = {
    "matrix_attr": {
        "row_axis": "gameplay",   # 行=玩法（含子系统）
        "col_axis": "attr",       # 列=属性
        "value_kind": "ratio",    # 值=投放比例（小数 0.4 = 40%）
    },
    "matrix_resource": {
        "row_axis": "gameplay",
        "col_axis": "res_id",
        "value_kind": "ratio",
    },
}


def create_matrix_table(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    display_name: str,
    kind: str,
    rows: Sequence[Dict[str, str]],          # [{key, display_name, brief}]
    cols: Sequence[Dict[str, str]],          # [{key, display_name, brief}]
    levels: Optional[Sequence[int]] = None,  # 若为空或 scale_mode='none' 则不分等级
    directory: str = "",
    readme: str = "",
    purpose: str = "",
    value_dtype: str = "float",              # float / percent / int
    value_format: str = "0.00%",
    register_calc: bool = True,
    scale_mode: Optional[str] = None,        # none | fallback | static(旧)
    tags: Optional[List[str]] = None,
    default_value: Optional[float] = None,   # 未写入单元格的缺省返回值；分配表通常填 0
    # none    = 无等级维（matrix_attr 默认）；所有 cell 存 level=NULL，调用时忽略 level 参数
    # fallback= matrix_resource 的“第三维=切片/公式”模式：
    #           单切片时可写常量；切片数 > 1 时内容必须改为公式
    # static  = 全量预存（旧行为，matrix_resource 已禁用）
) -> Dict[str, Any]:
    """创建 matrix 表。

    推荐规则：
    - matrix_attr → scale_mode='none'：只存 2D（行×列），level=NULL，全等级同值
    - matrix_resource → scale_mode='fallback'：
      第三维轴值（如等级）本身允许手填；但限制的是内容——
      若第三维切片数只有 1，可写常量/字符串；
      若第三维切片数 > 1，则整表内容必须改为公式，不允许手填多个切片常量
    - scale_mode='static' 保留给非 matrix_resource 旧表，matrix_resource 不允许再使用
    """
    if kind not in _KIND_META:
        raise ValueError(f"未知 matrix kind={kind}（允许：{list(_KIND_META)}）")
    t = assert_english_ident(table_name, field="表名")
    cur = conn.execute("SELECT 1 FROM _table_registry WHERE table_name = ?", (t,))
    if cur.fetchone():
        raise ValueError(f"表 {t!r} 已存在")

    # 根据 kind 设置默认 scale_mode
    if scale_mode is None:
        scale_mode = "none" if kind == "matrix_attr" else "fallback"
    if scale_mode not in ("none", "fallback", "static"):
        raise ValueError(f"scale_mode 必须为 none / fallback / static，得到 {scale_mode!r}")
    if kind == "matrix_resource" and scale_mode == "static":
        raise ValueError("matrix_resource 不再允许 static 手填第三维；请改用 fallback + formula")

    # none 模式强制 levels 为空
    effective_levels: List[int] = []
    if scale_mode == "static":
        effective_levels = list(levels) if levels else []

    meta = _KIND_META[kind]
    row_axis_name = meta["row_axis"]
    col_axis_name = meta["col_axis"]

    # 物理表：长表
    ddl = f'''CREATE TABLE "{t}" (
        row_id TEXT PRIMARY KEY,
        {row_axis_name} TEXT NOT NULL,
        {col_axis_name} TEXT NOT NULL,
        level INTEGER,
        value REAL,
        note TEXT
    )'''
    conn.execute(ddl)

    # 唯一索引保证一个 (row, col, level) 只一条（level 可为 NULL，NULL 视为唯一）
    conn.execute(
        f'CREATE UNIQUE INDEX "{t}__rcl" ON "{t}" ({row_axis_name}, {col_axis_name}, level)'
    )

    matrix_meta = {
        "kind": kind,
        "scale_mode": scale_mode,
        "row_axis": row_axis_name,
        "col_axis": col_axis_name,
        "value_kind": meta["value_kind"],
        "value_dtype": value_dtype,
        "value_format": value_format,
        "rows": [dict(r) for r in rows],
        "cols": [dict(c) for c in cols],
        "levels": effective_levels,
        "third_axis_name": "level" if kind == "matrix_resource" and scale_mode != "none" else "",
        "third_axis_values_manual": bool(kind == "matrix_resource" and scale_mode != "none"),
        "third_axis_content_rule": "single_slice_literal_or_formula" if kind == "matrix_resource" and scale_mode != "none" else "",
        "default_value": default_value,
    }

    schema_payload = {
        "columns": [
            {"name": "row_id", "sql_type": "TEXT", "display_name": "ID", "dtype": "id", "number_format": ""},
            {"name": row_axis_name, "sql_type": "TEXT", "display_name": "行(玩法)", "dtype": "ref", "number_format": ""},
            {"name": col_axis_name, "sql_type": "TEXT", "display_name": "列", "dtype": "ref", "number_format": ""},
            {"name": "level", "sql_type": "INTEGER", "display_name": "等级", "dtype": "int", "number_format": "0"},
            {"name": "value", "sql_type": "REAL", "display_name": "值", "dtype": value_dtype, "number_format": value_format},
            {"name": "note", "sql_type": "TEXT", "display_name": "备注", "dtype": "str", "number_format": "@"},
        ],
        "display_name": display_name,
    }

    conn.execute(
        """
        INSERT INTO _table_registry
            (table_name, layer, purpose, readme, schema_json, validation_status, directory, matrix_meta_json, tags)
        VALUES (?,?,?,?,?, 'unknown', ?, ?, ?)
        """,
        (
            t,
            "matrix",
            purpose,
            readme,
            json.dumps(schema_payload, ensure_ascii=False),
            directory or "",
            json.dumps(matrix_meta, ensure_ascii=False),
            json.dumps(tags or [], ensure_ascii=False),
        ),
    )
    conn.commit()

    calc_name = ""
    if register_calc:
        from app.services.calculator_ops import register_calculator

        # 默认 calculator 名 = <table>_lookup
        calc_name = f"{t}_lookup"
        try:
            axes = []
            if scale_mode != "none":
                axes.append({"name": "level", "source": "level"})
            axes.append({"name": row_axis_name, "source": row_axis_name})
            axes.append({"name": col_axis_name, "source": col_axis_name})
            register_calculator(
                conn,
                name=calc_name,
                kind=kind,
                table_name=t,
                axes=axes,
                value_column="value",
                brief=(
                    f"按 ({row_axis_name}, {col_axis_name}) 查询 {display_name} 的投放比例；"
                    f"scale_mode={scale_mode}。matrix_resource 在 fallback 下：单切片可常量，多切片必须公式。"
                ),
            )
        except Exception:  # noqa: BLE001
            calc_name = ""

    return {
        "ok": True,
        "table_name": t,
        "display_name": display_name,
        "matrix_meta": matrix_meta,
        "calculator": calc_name,
        "directory": directory or "",
        "scale_mode": scale_mode,
    }


def write_matrix_cells(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    cells: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """批量写入 matrix 单元格。

    每项: {row, col, level (optional), value, note (optional), formula (optional)}

    scale_mode='none' 时 level 参数被忽略，统一存 level=NULL（避免写入爆炸）。
    scale_mode='fallback' 时：
        - matrix_resource：第三维轴值（如 level）可手填；但若字面量切片数 > 1，则拒绝写入并要求改为 formula
        - 其他 matrix：不指定 level → 写入 level=NULL（作为兜底基准值）；指定 level → 写入精确等级覆盖值
    scale_mode='static' 时与旧行为一致。
    """
    t = table_name
    cur = conn.execute(
        "SELECT layer, matrix_meta_json FROM _table_registry WHERE table_name = ?", (t,)
    )
    row = cur.fetchone()
    if not row or row[0] != "matrix":
        raise ValueError(f"{t} 不是 matrix 表")
    meta = json.loads(row[1] or "{}") or {}
    kind = str(meta.get("kind") or "")
    row_axis = meta.get("row_axis") or "row_key"
    col_axis = meta.get("col_axis") or "col_key"
    scale_mode = meta.get("scale_mode") or "static"

    state = _matrix_resource_state(conn, table_name=t) if kind == "matrix_resource" else {}
    written = 0
    formula_written = 0
    batch_formula_pairs: List[Tuple[str, str]] = []
    batch_literal_pairs: List[Tuple[str, str]] = []
    batch_has_base_literal = False
    batch_explicit_levels: set[int] = set()
    for c in cells:
        r = str(c.get("row") or c.get(row_axis) or "").strip()
        co = str(c.get("col") or c.get(col_axis) or "").strip()
        if not r or not co:
            continue
        raw_lv = c.get("level")
        has_formula_key = "formula" in c
        formula = str(c.get("formula") or "").strip()
        v = c.get("value")
        note = c.get("note") or ""

        # 根据 scale_mode 决定存入的 level 值
        if scale_mode == "none":
            lv_int: Optional[int] = None          # 强制 NULL，忽略调用方传的 level
        elif scale_mode == "fallback":
            lv_int = int(raw_lv) if raw_lv is not None and str(raw_lv) != "" else None
        else:  # static
            lv_int = int(raw_lv) if raw_lv is not None and str(raw_lv) != "" else None

        if formula and kind != "matrix_resource":
            raise ValueError("仅 matrix_resource 支持按 cell 注册第三维公式")
        if formula and v is not None:
            raise ValueError("同一 cell 不能同时填写 value 与 formula；多切片第三维请只保留 formula")
        if kind == "matrix_resource":
            if formula:
                batch_formula_pairs.append((r, co))
            elif v is not None:
                batch_literal_pairs.append((r, co))
                if lv_int is None:
                    batch_has_base_literal = True
                else:
                    batch_explicit_levels.add(lv_int)

        if kind == "matrix_resource" and scale_mode == "fallback":
            if batch_formula_pairs and batch_literal_pairs:
                raise ValueError("matrix_resource 不能混写常量与公式；第三维切片数 > 1 时整表内容必须是公式")
            if batch_formula_pairs:
                if state.get("literal_rows", 0) > 0:
                    raise ValueError("当前 matrix_resource 表里已有常量内容；若要启用多切片第三维，请新建公式表或先清理旧常量")
            elif batch_literal_pairs:
                if state.get("formula_count", 0) > 0:
                    raise ValueError("当前 matrix_resource 表已进入公式模式；不能再追加常量内容")
                effective_levels = set(state.get("explicit_levels") or set()) | batch_explicit_levels
                has_base_literal = bool(state.get("has_base_literal")) or batch_has_base_literal
                literal_slice_count = len(effective_levels) + (1 if has_base_literal else 0)
                if literal_slice_count > 1:
                    raise ValueError("matrix_resource 的第三维切片数 > 1 时，内容必须全部改为 formula，不能手填多个常量切片")

        if has_formula_key:
            if formula:
                _upsert_matrix_formula(
                    conn,
                    table_name=t,
                    row_key=r,
                    col_key=co,
                    row_axis=str(row_axis),
                    col_axis=str(col_axis),
                    formula=formula,
                )
                formula_written += 1
            else:
                _delete_matrix_formula(conn, table_name=t, row_key=r, col_key=co)

        if v is None and formula and not note:
            # 纯公式定义场景不需要写物理行；仍保留公式注册。
            continue
        # row_id 按照 (row, col, level_or_null) 构成唯一键
        rid = f"{r}__{co}__{lv_int if lv_int is not None else 'na'}"
        conn.execute(
            f'''INSERT INTO "{t}" (row_id, {row_axis}, {col_axis}, level, value, note)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(row_id) DO UPDATE SET
                    value = excluded.value,
                    note = excluded.note''',
            (rid, r, co, lv_int, v, note),
        )
        written += 1
    conn.commit()
    return {"ok": True, "written": written, "formula_written": formula_written, "table_name": t}


def read_matrix(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    level: Optional[int] = None,
    rows: Optional[Sequence[str]] = None,
    cols: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """以宽表形式读取 matrix。

    返回：{ "rows": [...], "cols": [...], "levels": [...], "data": {row: {col: {level: value}}}, "formula_cells": ... }
    """
    cur = conn.execute(
        "SELECT matrix_meta_json FROM _table_registry WHERE table_name = ?", (table_name,)
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"未知表 {table_name}")
    meta = json.loads(row[0] or "{}") or {}
    kind = str(meta.get("kind") or "")
    row_axis = meta.get("row_axis") or "row_key"
    col_axis = meta.get("col_axis") or "col_key"

    where: List[str] = []
    params: List[Any] = []
    scale_mode = meta.get("scale_mode") or "static"

    if level is not None:
        if scale_mode == "none":
            # none 模式：所有 cell 存 level=NULL，level 参数忽略
            where.append("level IS NULL")
        elif scale_mode == "fallback":
            # fallback 模式：先精确 level，结果合并 NULL 基准（读取时不过滤，全部返回）
            pass  # 不加 level 过滤，让调用方自行选择
        else:
            where.append("level = ?"); params.append(int(level))
    if rows:
        placeholders = ",".join("?" * len(rows))
        where.append(f"{row_axis} IN ({placeholders})")
        params.extend(rows)
    if cols:
        placeholders = ",".join("?" * len(cols))
        where.append(f"{col_axis} IN ({placeholders})")
        params.extend(cols)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f'SELECT {row_axis}, {col_axis}, level, value, note FROM "{table_name}" {where_sql}'
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for r in conn.execute(sql, params):
        rk, ck, lv, val, note = r
        out.setdefault(rk, {}).setdefault(ck, {})[str(lv) if lv is not None else "_"] = {
            "value": val, "note": note,
        }
    formula_map = _load_matrix_formulas(conn, table_name=table_name)
    formula_cells: Dict[str, Dict[str, Dict[str, str]]] = {}
    for (rk, ck), formula_info in formula_map.items():
        if rows and rk not in rows:
            continue
        if cols and ck not in cols:
            continue
        formula_cells.setdefault(rk, {})[ck] = formula_info
        if kind == "matrix_resource" and level is not None:
            exact_cell = out.get(rk, {}).get(ck, {}).get(str(level))
            if exact_cell is not None:
                continue
            evaluated = evaluate_matrix_formula_value(
                conn,
                table_name=table_name,
                row_axis=str(row_axis),
                col_axis=str(col_axis),
                row_key=rk,
                col_key=ck,
                level=level,
            )
            if evaluated.get("ok"):
                base_note = ((out.get(rk) or {}).get(ck) or {}).get("_", {}).get("note")
                out.setdefault(rk, {}).setdefault(ck, {})[str(level)] = {
                    "value": evaluated.get("value"),
                    "note": base_note,
                    "source": "formula",
                }
    return {
        "ok": True,
        "table_name": table_name,
        "kind": kind,
        "row_axis": row_axis,
        "col_axis": col_axis,
        "rows": [r["key"] for r in (meta.get("rows") or [])],
        "cols": [c["key"] for c in (meta.get("cols") or [])],
        "levels": meta.get("levels") or [],
        "preview_level": level,
        "formula_cells": formula_cells,
        "data": out,
    }


def list_matrix_tables(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    cur = conn.execute(
        "SELECT table_name, directory, matrix_meta_json, schema_json FROM _table_registry WHERE layer='matrix'"
    )
    out: List[Dict[str, Any]] = []
    for r in cur.fetchall():
        meta = json.loads(r[2] or "{}") or {}
        sch = json.loads(r[3] or "{}") or {}
        out.append({
            "table_name": r[0],
            "directory": r[1] or "",
            "display_name": sch.get("display_name", ""),
            "kind": meta.get("kind"),
            "row_count": len(meta.get("rows") or []),
            "col_count": len(meta.get("cols") or []),
            "level_count": len(meta.get("levels") or []),
        })
    return out
