"""第3轮新增功能的最小覆盖测试：matrix 表 / calculator 注册 / 表目录 / 暴露参数。"""
from __future__ import annotations

import pathlib
import sqlite3
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from app.db.project_schema import init_project_db
from app.db.project_migrations import ensure_project_migrations
from app.services.matrix_table_ops import (
    create_matrix_table,
    write_matrix_cells,
    read_matrix,
    list_matrix_tables,
)
from app.services.calculator_ops import (
    register_calculator,
    list_calculators,
    call_calculator,
)
from app.services.table_ops import create_dynamic_table


def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    init_project_db(conn, seed_readme=False)
    ensure_project_migrations(conn)
    return conn


# ---------- migration ----------

def test_migration_adds_directory_and_matrix_meta():
    conn = _new_conn()
    cur = conn.execute("PRAGMA table_info(_table_registry)")
    cols = {r[1] for r in cur.fetchall()}
    assert "directory" in cols
    assert "matrix_meta_json" in cols


def test_migration_creates_calculators_and_exposed_params_tables():
    conn = _new_conn()
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name IN ('_calculators','_step_exposed_params')"
    )
    names = {r[0] for r in cur.fetchall()}
    assert names == {"_calculators", "_step_exposed_params"}


# ---------- table directory ----------

def test_create_table_with_directory():
    conn = _new_conn()
    create_dynamic_table(
        conn,
        table_name="t_demo",
        display_name="演示",
        columns=[("level","INTEGER")],
        directory="基础/演示",
    )
    cur = conn.execute(
        "SELECT directory FROM _table_registry WHERE table_name = ?", ("t_demo",)
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] == "基础/演示"


# ---------- matrix table ----------

def test_create_matrix_table_and_read_back():
    conn = _new_conn()
    rows = [
        {"key": "equip_base", "display_name": "装备·基础", "brief": "基础装备槽位"},
        {"key": "equip_enhance", "display_name": "装备·强化", "brief": "强化产出"},
    ]
    cols = [
        {"key": "atk", "display_name": "攻击力", "brief": ""},
        {"key": "hp", "display_name": "生命", "brief": ""},
    ]
    res = create_matrix_table(
        conn,
        table_name="gameplay_attr_alloc",
        display_name="玩法属性分配",
        kind="matrix_attr",
        rows=rows,
        cols=cols,
        levels=[1, 2],
        directory="分配/玩法属性",
        register_calc=True,
    )
    assert res.get("status") == "success" or res.get("ok") or "table_name" in str(res)

    write_matrix_cells(
        conn,
        table_name="gameplay_attr_alloc",
        cells=[
            {"row": "equip_base", "col": "atk", "value": 0.4},
            {"row": "equip_base", "col": "hp", "value": 0.5},
            {"row": "equip_enhance", "col": "atk", "value": 0.2},
        ],
    )
    # scale_mode='none': level stored as NULL, read_matrix ignores level param
    snap = read_matrix(conn, table_name="gameplay_attr_alloc")
    data = snap["data"]
    assert data["equip_base"]["atk"]["_"]["value"] == 0.4
    assert data["equip_base"]["hp"]["_"]["value"] == 0.5

    listed = list_matrix_tables(conn)
    names = [t.get("table_name") for t in listed]
    assert "gameplay_attr_alloc" in names


def test_matrix_auto_lookup_calculator_registered():
    conn = _new_conn()
    create_matrix_table(
        conn,
        table_name="x_alloc",
        display_name="X分配",
        kind="matrix_attr",
        rows=[{"key": "r1", "display_name": "R1", "brief": ""}],
        cols=[{"key": "c1", "display_name": "C1", "brief": ""}],
        levels=[1],
        directory="测试",
        register_calc=True,
    )
    listed = list_calculators(conn)
    names = [c.get("name") for c in listed]
    assert any("x_alloc" in (n or "") for n in names), names


# ---------- calculator registry ----------

def test_register_calculator_brief_min_length():
    conn = _new_conn()
    create_dynamic_table(
        conn,
        table_name="t_brief",
        display_name="brief 校验",
        columns=[("level","INTEGER")],
    )
    try:
        register_calculator(
            conn,
            name="too_short",
            kind="lookup",
            table_name="t_brief",
            axes=[{"name": "level", "source": "level"}],
            brief="short",
        )
    except Exception as e:
        assert "8" in str(e) or "brief" in str(e).lower()
    else:
        raise AssertionError("brief<8 应被拒绝")


def test_register_and_call_lookup_calculator():
    conn = _new_conn()
    create_dynamic_table(
        conn,
        table_name="t_lookup",
        display_name="查询表",
        columns=[("level", "INTEGER"), ("value", "REAL")],
    )
    conn.execute("INSERT INTO t_lookup(level, value) VALUES (1, 100.0), (2, 200.0)")
    conn.commit()
    register_calculator(
        conn,
        name="t_lookup_calc",
        kind="lookup",
        table_name="t_lookup",
        axes=[{"name": "level", "source": "level"}],
        value_column="value",
        brief="按等级查询数值，参数：level；返回 value 列",
    )
    res = call_calculator(conn, name="t_lookup_calc", kwargs={"level": 2})
    assert res.get("value") == 200.0 or res.get("data") == 200.0 or res.get("result") == 200.0


# ---------- exposed params ----------

def test_expose_param_round_trip():
    conn = _new_conn()
    from app.services.agent_tools import _expose_param, _list_exposed_params

    _expose_param(
        conn,
        {
            "owner_step": "gameplay_landing_tables.equip_base",
            "target_step": "subsystems:gameplay_landing_tables",
            "key": "equip_base_attr_ratio",
            "value": 0.6,
            "brief": "装备_基础对攻击属性的覆盖比",
        },
    )
    out = _list_exposed_params(conn, "gameplay_landing_tables.equip_enhance")
    items = out.get("items", [])
    assert len(items) == 1
    assert items[0]["key"] == "equip_base_attr_ratio"
    assert items[0]["value"] == 0.6


# ---------- scale_mode fallback ----------

def test_matrix_fallback_scale_mode():
    """fallback 模式：只写 NULL 基准，call_calculator 能按 level 查到回退值。"""
    conn = _new_conn()
    create_matrix_table(
        conn,
        table_name="res_alloc",
        display_name="资源分配",
        kind="matrix_resource",
        rows=[{"key": "equip_base", "display_name": "装备·基础", "brief": ""}],
        cols=[{"key": "gold", "display_name": "金币", "brief": ""}],
        directory="分配/资源",
        scale_mode="fallback",
        register_calc=True,
    )
    # 只写基准值（无 level）
    write_matrix_cells(
        conn,
        table_name="res_alloc",
        cells=[{"row": "equip_base", "col": "gold", "value": 100.0}],
    )
    # 查 level=5（不存在精确行），应回退到 NULL 基准
    res = call_calculator(conn, name="res_alloc_lookup",
                          kwargs={"gameplay": "equip_base", "res_id": "gold", "level": 5})
    assert res["found"] is True
    assert res["value"] == 100.0
    assert res.get("fallback") is True


def test_matrix_none_scale_mode_ignores_level():
    """none 模式：写入时忽略 level，read_matrix 不过滤 level。"""
    conn = _new_conn()
    create_matrix_table(
        conn,
        table_name="attr_alloc2",
        display_name="属性分配2",
        kind="matrix_attr",
        rows=[{"key": "skill", "display_name": "技能", "brief": ""}],
        cols=[{"key": "atk", "display_name": "攻击", "brief": ""}],
        directory="分配/测试",
        scale_mode="none",
        register_calc=True,
    )
    # 传 level 也应被忽略（存 NULL）
    write_matrix_cells(
        conn,
        table_name="attr_alloc2",
        cells=[{"row": "skill", "col": "atk", "level": 99, "value": 0.3}],
    )
    snap = read_matrix(conn, table_name="attr_alloc2")
    data = snap["data"]
    assert data["skill"]["atk"]["_"]["value"] == 0.3  # key='_' 表示 level=NULL


# ---------- pipeline step specs ----------

def test_pipeline_has_hp_formula_step():
    """流水线应包含独立的 hp_formula_derivation 步骤。"""
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
    from app.data.pipeline_step_specs import list_step_specs
    ids = [s.step_id for s in list_step_specs()]
    assert "hp_formula_derivation" in ids
    assert "base_attribute_framework" in ids
    hp_idx = ids.index("hp_formula_derivation")
    base_idx = ids.index("base_attribute_framework")
    assert hp_idx == base_idx + 1  # hp 步紧跟 base


def test_pipeline_base_attr_does_not_require_hp():
    """base_attribute_framework 步骤不应把 hp 列列为必填列。"""
    from app.data.pipeline_step_specs import get_step_spec
    spec = get_step_spec("base_attribute_framework")
    assert spec is not None
    required = spec.required_columns.get("num_base_framework") or []
    assert "hp" not in required
