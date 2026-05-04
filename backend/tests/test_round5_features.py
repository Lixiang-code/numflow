"""第5轮反馈修复的最小覆盖测试。"""
from __future__ import annotations

import math
import json
import pathlib
import sqlite3
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from app.deps import ProjectDB
from app.db.project_migrations import ensure_project_migrations
from app.db.project_schema import init_project_db
from app.services.agent_tools import _const_register, dispatch_tool
from app.services.calculator_ops import register_calculator
from app.services.formula_engine import eval_row_formula, safe_eval_scalar
from app.services.formula_exec import register_formula
from app.services.table_ops import create_3d_table, create_dynamic_table, read_3d_table


def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_project_db(conn, seed_readme=False)
    ensure_project_migrations(conn)
    return conn


def _project_db(conn: sqlite3.Connection) -> ProjectDB:
    return ProjectDB(
        row={"id": 1, "name": "测试项目", "slug": "round5-test"},
        conn=conn,
        can_write=True,
    )


def test_formula_engine_supports_coalesce_and_ifnull_for_null_like_values():
    assert safe_eval_scalar("coalesce(None, 5, 7)", {}) == 5
    assert safe_eval_scalar("ifnull(v, 9)", {"v": math.nan}) == 9


def test_formula_engine_multiplies_arrays_elementwise():
    result = safe_eval_scalar("__a0 * __a1", {"__a0": [True, False, True], "__a1": [True, True, False]})
    assert result == [1, 0, 0]
    assert safe_eval_scalar("2 * __a0", {"__a0": [1, 2, 3]}) == [2, 4, 6]


def test_row_formula_supports_ampersand_string_concat():
    value, missing = eval_row_formula(
        "@stage_id & '_' & @monster_type",
        {"stage_id": "stage_1", "monster_type": "elite"},
        {"stage_id", "monster_type"},
    )
    assert missing == set()
    assert value == "stage_1_elite"


def test_const_register_does_not_warn_for_round_integer_values():
    conn = _new_conn()
    result = _const_register(
        conn,
        {
            "name_en": "max_level",
            "name_zh": "最大等级",
            "value": 200,
            "brief": "角色等级上限",
            "tags": ["combat"],
        },
        True,
    )
    assert result["ok"] is True
    assert "warning" not in result


def test_create_3d_table_rewrites_dim2_key_alias_and_executes_formula():
    conn = _new_conn()
    result = create_3d_table(
        conn,
        table_name="artifact_attr_3d",
        display_name="神器属性",
        dim1={
            "col_name": "tier",
            "display_name": "阶数",
            "keys": [{"key": "1", "display_name": "1阶"}],
        },
        dim2={
            "col_name": "artifact_type",
            "display_name": "神器类型",
            "keys": [{"key": "atk", "display_name": "攻击"}, {"key": "def", "display_name": "防御"}],
        },
        cols=[
            {"key": "base_value", "display_name": "基础值", "formula": "IF(@dim2_key == 'atk', 10, 20)"},
        ],
    )
    assert result["formula_errors"] == []

    snap = read_3d_table(conn, table_name="artifact_attr_3d")
    assert snap["data"]["1"]["atk"]["base_value"] == 10.0
    assert snap["data"]["1"]["def"]["base_value"] == 20.0
    assert snap["column_formulas"]["base_value"]["formula"] == "IF(@artifact_type == 'atk', 10, 20)"


def test_create_3d_table_auto_executes_index_formula_columns():
    conn = _new_conn()
    result = create_3d_table(
        conn,
        table_name="gem_attr_idx",
        display_name="宝石索引表",
        dim1={
            "col_name": "tier",
            "display_name": "阶数",
            "keys": [{"key": "1", "display_name": "1阶"}],
        },
        dim2={
            "col_name": "gem_type",
            "display_name": "宝石类型",
            "keys": [{"key": "atk", "display_name": "攻击"}, {"key": "def", "display_name": "防御"}],
        },
        cols=[
            {"key": "base_value", "display_name": "基础值", "formula": "IF(@gem_type == 'atk', 10, 20)"},
            {
                "key": "indexed_value",
                "display_name": "索引值",
                "formula": "INDEX(@@gem_attr_idx[base_value], MATCH(@gem_type, @@gem_attr_idx[gem_type], 0))",
            },
        ],
    )
    assert result["formula_errors"] == []

    snap = read_3d_table(conn, table_name="gem_attr_idx")
    assert snap["data"]["1"]["atk"]["indexed_value"] == 10.0
    assert snap["data"]["1"]["def"]["indexed_value"] == 20.0


def test_register_formula_supports_call_calculator():
    conn = _new_conn()
    create_dynamic_table(
        conn,
        table_name="attr_lookup_src",
        display_name="属性查询源",
        columns=[("level", "INTEGER"), ("gameplay", "TEXT"), ("attr", "TEXT"), ("value", "REAL")],
    )
    conn.execute(
        'INSERT INTO "attr_lookup_src" (row_id, "level", "gameplay", "attr", "value") VALUES (?, ?, ?, ?, ?)',
        ("1_mount_atk", 1, "mount_system", "atk", 123.0),
    )
    register_calculator(
        conn,
        name="attr_lookup_calc",
        kind="lookup",
        table_name="attr_lookup_src",
        axes=[
            {"name": "level", "source": "level"},
            {"name": "gameplay", "source": "gameplay"},
            {"name": "attr", "source": "attr"},
        ],
        value_column="value",
        brief="按等级/玩法/属性查询数值",
    )
    create_dynamic_table(
        conn,
        table_name="attr_formula_target",
        display_name="属性落地表",
        columns=[("level", "INTEGER"), ("gameplay", "TEXT"), ("result", "REAL")],
    )
    conn.execute(
        'INSERT INTO "attr_formula_target" (row_id, "level", "gameplay", "result") VALUES (?, ?, ?, ?)',
        ("row_1", 1, "mount_system", 0.0),
    )
    conn.commit()

    result = register_formula(
        conn,
        "attr_formula_target",
        "result",
        "call_calculator('attr_lookup_calc', @level, @gameplay, 'atk')",
    )
    assert result["ok"] is True
    assert result["auto_executed"]["rows_updated"] == 1

    row = conn.execute('SELECT "result" FROM "attr_formula_target" WHERE row_id = ?', ("row_1",)).fetchone()
    value = row["result"] if isinstance(row, sqlite3.Row) else row[0]
    assert value == 123.0


def test_create_3d_table_supports_call_calculator_formula():
    conn = _new_conn()
    create_dynamic_table(
        conn,
        table_name="attr_lookup_3d_src",
        display_name="3D 属性查询源",
        columns=[("level", "INTEGER"), ("gameplay", "TEXT"), ("attr", "TEXT"), ("value", "REAL")],
    )
    conn.executemany(
        'INSERT INTO "attr_lookup_3d_src" (row_id, "level", "gameplay", "attr", "value") VALUES (?, ?, ?, ?, ?)',
        [
            ("1_mount_atk", 1, "mount_system", "atk", 10.0),
            ("1_mount_def", 1, "mount_system", "def", 20.0),
        ],
    )
    register_calculator(
        conn,
        name="attr_lookup_3d_calc",
        kind="lookup",
        table_name="attr_lookup_3d_src",
        axes=[
            {"name": "level", "source": "level"},
            {"name": "gameplay", "source": "gameplay"},
            {"name": "attr", "source": "attr"},
        ],
        value_column="value",
        brief="按等级/玩法/属性查询 3D 值",
    )
    conn.commit()

    result = create_3d_table(
        conn,
        table_name="mount_attr_3d",
        display_name="坐骑属性",
        dim1={
            "col_name": "level",
            "display_name": "等级",
            "keys": [{"key": "1", "display_name": "1级"}],
        },
        dim2={
            "col_name": "attr_type",
            "display_name": "属性类型",
            "keys": [{"key": "atk", "display_name": "攻击"}, {"key": "def", "display_name": "防御"}],
        },
        cols=[
            {
                "key": "attr_value",
                "display_name": "属性值",
                "formula": "call_calculator('attr_lookup_3d_calc', @level, 'mount_system', @attr_type)",
            },
        ],
    )
    assert result["formula_errors"] == []

    snap = read_3d_table(conn, table_name="mount_attr_3d")
    assert snap["data"]["1"]["atk"]["attr_value"] == 10.0
    assert snap["data"]["1"]["def"]["attr_value"] == 20.0


def test_register_formula_allows_interp_with_constant_refs():
    conn = _new_conn()
    _const_register(
        conn,
        {"name_en": "hp_low", "name_zh": "低级生命", "value": 10, "brief": "低级生命基准", "tags": ["combat"]},
        True,
    )
    _const_register(
        conn,
        {"name_en": "hp_high", "name_zh": "高级生命", "value": 100, "brief": "高级生命基准", "tags": ["combat"]},
        True,
    )
    create_dynamic_table(
        conn,
        table_name="interp_target",
        display_name="插值目标表",
        columns=[("level", "INTEGER"), ("result", "REAL")],
    )
    conn.execute(
        'INSERT INTO "interp_target" (row_id, "level", "result") VALUES (?, ?, ?)',
        ("row_5", 5, 0.0),
    )
    conn.commit()

    result = register_formula(
        conn,
        "interp_target",
        "result",
        "interp(@level, 1, ${hp_low}, 10, ${hp_high})",
    )
    assert result["ok"] is True
    assert result["auto_executed"]["rows_updated"] == 1

    row = conn.execute('SELECT "result" FROM "interp_target" WHERE row_id = ?', ("row_5",)).fetchone()
    value = row["result"] if isinstance(row, sqlite3.Row) else row[0]
    assert value == 50.0


def test_write_cells_returns_large_payload_warning():
    conn = _new_conn()
    create_dynamic_table(
        conn,
        table_name="write_warn_target",
        display_name="写入预警表",
        columns=[("note", "TEXT")],
    )
    updates = [
        {"row_id": f"row_{i}", "column": "note", "value": f"备注内容_{i}_" + ("x" * 40)}
        for i in range(20)
    ]
    result = json.loads(
        dispatch_tool(
            "write_cells",
            {"table_name": "write_warn_target", "updates": updates},
            _project_db(conn),
        )
    )
    assert result["status"] == "success"
    assert any("payload 较长" in warning for warning in result.get("warnings", []))
