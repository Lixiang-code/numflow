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
from app.db.project_schema import create_agent_session, init_project_db
from app.routers.agent import _session_tracking_wrapper
from app.routers.meta import PatchConstantBody, patch_constant
from app.services.agent_tools import _const_register, _expose_param, _list_exposed_params, dispatch_tool
from app.services.calculator_ops import call_calculator, register_calculator
from app.services.formula_engine import eval_row_formula, safe_eval_scalar
from app.services.formula_exec import register_formula, register_row_formula
from app.services.table_ops import create_3d_table, create_dynamic_table, read_3d_table
from app.services.validation_report import default_rules_for


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


def test_register_row_formula_normalizes_same_table_explicit_refs_to_same_row_refs():
    conn = _new_conn()
    create_dynamic_table(
        conn,
        table_name="row_ref_target",
        display_name="同行引用目标表",
        columns=[("base_value", "REAL"), ("derived_value", "REAL")],
    )
    conn.execute(
        'INSERT INTO "row_ref_target" (row_id, "base_value", "derived_value") VALUES (?, ?, ?)',
        ("r1", 2.0, 0.0),
    )
    conn.commit()

    result = register_row_formula(conn, "row_ref_target", "derived_value", "@row_ref_target[base_value] + 1")

    assert result["ok"] is True
    assert result["formula_type"] == "row"
    stored = conn.execute(
        "SELECT formula FROM _formula_registry WHERE table_name = ? AND column_name = ?",
        ("row_ref_target", "derived_value"),
    ).fetchone()[0]
    assert stored == "@base_value + 1"
    assert conn.execute('SELECT "derived_value" FROM "row_ref_target" WHERE row_id = ?', ("r1",)).fetchone()[0] == 3.0


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


def test_const_register_does_not_warn_for_decimal_numeric_values():
    conn = _new_conn()
    result = _const_register(
        conn,
        {
            "name_en": "base_crit_rate",
            "name_zh": "基础暴击率",
            "value": 0.05,
            "brief": "基础暴击率",
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


def test_patch_constant_surfaces_recalc_warning(monkeypatch):
    conn = _new_conn()
    _const_register(
        conn,
        {"name_en": "growth_base", "name_zh": "成长基数", "value": 2, "brief": "成长基数", "tags": ["combat"]},
        True,
    )
    create_dynamic_table(
        conn,
        table_name="const_warning_target",
        display_name="常量预警目标表",
        columns=[("input", "REAL"), ("output", "REAL")],
    )
    conn.execute(
        'INSERT INTO "const_warning_target" (row_id, "input", "output") VALUES (?, ?, ?)',
        ("r1", 1.0, 0.0),
    )
    conn.commit()
    register_formula(conn, "const_warning_target", "output", "@const_warning_target[input] + ${growth_base}", defer=True)

    def _boom(*_args, **_kwargs):
        raise RuntimeError("recalc failed")

    monkeypatch.setattr("app.services.formula_exec.recalculate_downstream_dag", _boom)

    result = patch_constant("growth_base", PatchConstantBody(value=3), _project_db(conn))

    assert result["ok"] is True
    assert result["value"] == 3
    assert "warning" in result
    assert "recalc failed" in result["warning"]


def _sse_event(payload: dict) -> bytes:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8")


def test_session_tracking_wrapper_marks_disconnected_session_as_error():
    conn = _new_conn()
    session_id = create_agent_session(conn, "gameplay_table.monster_final")

    def _stream():
        yield _sse_event({"type": "token", "phase": "design", "text": "partial design"})
        yield _sse_event({"type": "token", "phase": "design", "text": "should not arrive"})

    wrapped = _session_tracking_wrapper(_stream(), conn, session_id)
    first = next(wrapped)
    assert b"partial design" in first

    wrapped.close()

    row = conn.execute(
        "SELECT status, error_text, design_text, current_phase, finished_at FROM _agent_sessions WHERE id = ?",
        (session_id,),
    ).fetchone()
    assert row is not None
    assert row["status"] == "error"
    assert row["error_text"] == "client_disconnected"
    assert row["design_text"] == "partial design"
    assert row["current_phase"] == "design"
    assert row["finished_at"]


def test_session_tracking_wrapper_marks_unexpected_stream_errors():
    conn = _new_conn()
    session_id = create_agent_session(conn, "gameplay_table.player_model")

    def _broken_stream():
        yield _sse_event({"type": "token", "phase": "execute", "text": "partial execute"})
        raise RuntimeError("boom")

    wrapped = _session_tracking_wrapper(_broken_stream(), conn, session_id)
    chunks = list(wrapped)
    assert len(chunks) == 1

    row = conn.execute(
        "SELECT status, error_text, execute_text, current_phase, finished_at FROM _agent_sessions WHERE id = ?",
        (session_id,),
    ).fetchone()
    assert row is not None
    assert row["status"] == "error"
    assert row["execute_text"] == "partial execute"
    assert row["current_phase"] == "execute"
    assert row["finished_at"]
    assert row["error_text"].startswith("unexpected_stream_error:")
    assert "boom" in row["error_text"]


def test_list_exposed_params_trims_invalid_json_escape_sequences_safely():
    conn = _new_conn()
    _expose_param(
        conn,
        {
            "owner_step": "gameplay_table.source",
            "target_step": "gameplay_table.target",
            "key": "long_text",
            "value": ("a" * 198) + "\n",
            "brief": "长文本参数",
        },
    )

    result = _list_exposed_params(conn, "gameplay_table.target")

    item = result["items"][0]
    assert item["key"] == "long_text"
    assert isinstance(item["value"], str)
    assert item["value"].endswith("...")


def test_register_formula_supports_cumsum_group_by_for_3d_partitions():
    conn = _new_conn()
    create_3d_table(
        conn,
        table_name="artifact_cost_3d",
        display_name="神器成本",
        dim1={
            "col_name": "artifact_id",
            "display_name": "神器",
            "keys": [{"key": "1", "display_name": "神器1"}, {"key": "2", "display_name": "神器2"}],
        },
        dim2={
            "col_name": "tier",
            "display_name": "阶数",
            "keys": [{"key": "1", "display_name": "1阶"}, {"key": "2", "display_name": "2阶"}],
        },
        cols=[
            {"key": "cost", "display_name": "单阶成本"},
            {"key": "cumulative_cost", "display_name": "累计成本"},
        ],
    )
    conn.executemany(
        'UPDATE "artifact_cost_3d" SET "cost" = ? WHERE row_id = ?',
        [(10.0, "1_1"), (20.0, "1_2"), (5.0, "2_1"), (7.0, "2_2")],
    )
    conn.commit()

    result = register_formula(
        conn,
        "artifact_cost_3d",
        "cumulative_cost",
        "CUMSUM_GROUP_BY(@@artifact_cost_3d[artifact_id], @@artifact_cost_3d[cost])",
    )

    assert result["ok"] is True
    rows = conn.execute(
        'SELECT row_id, cumulative_cost FROM "artifact_cost_3d" ORDER BY row_id'
    ).fetchall()
    assert [(row["row_id"], row["cumulative_cost"]) for row in rows] == [
        ("1_1", 10.0),
        ("1_2", 30.0),
        ("2_1", 5.0),
        ("2_2", 12.0),
    ]


def test_register_formula_aligns_cross_table_refs_by_common_dimension():
    conn = _new_conn()
    create_dynamic_table(
        conn,
        table_name="player_base",
        display_name="玩家基础表",
        columns=[("level", "INTEGER"), ("hp", "REAL")],
    )
    conn.executemany(
        'INSERT INTO "player_base" (row_id, "level", "hp") VALUES (?, ?, ?)',
        [("p1", 1, 100.0), ("p2", 2, 200.0)],
    )
    create_3d_table(
        conn,
        table_name="monster_standard",
        display_name="怪物标准表",
        dim1={
            "col_name": "level",
            "display_name": "等级",
            "keys": [{"key": "1", "display_name": "1级"}, {"key": "2", "display_name": "2级"}],
        },
        dim2={
            "col_name": "monster_type",
            "display_name": "类型",
            "keys": [{"key": "a", "display_name": "A"}, {"key": "b", "display_name": "B"}],
        },
        cols=[{"key": "hp_copy", "display_name": "生命复制"}],
    )
    conn.commit()

    result = register_formula(conn, "monster_standard", "hp_copy", "@player_base[hp]")

    assert result["ok"] is True
    assert result["auto_executed"]["rows_updated"] == 4
    rows = conn.execute(
        'SELECT row_id, hp_copy FROM "monster_standard" ORDER BY row_id'
    ).fetchall()
    assert [(row["row_id"], row["hp_copy"]) for row in rows] == [
        ("1_a", 100.0),
        ("1_b", 100.0),
        ("2_a", 200.0),
        ("2_b", 200.0),
    ]


def test_add_column_tool_updates_physical_table_and_schema():
    conn = _new_conn()
    create_dynamic_table(
        conn,
        table_name="resource_alloc",
        display_name="资源分配",
        columns=[("level", "INTEGER")],
    )

    result = json.loads(
        dispatch_tool(
            "add_column",
            {
                "table_name": "resource_alloc",
                "column_name": "note",
                "sql_type": "TEXT",
                "display_name": "备注",
                "number_format": "@",
            },
            _project_db(conn),
        )
    )

    assert result["status"] == "success"
    cols = {row[1]: row[2] for row in conn.execute('PRAGMA table_info("resource_alloc")').fetchall()}
    assert cols["note"] == "TEXT"
    schema_json = conn.execute(
        "SELECT schema_json FROM _table_registry WHERE table_name = ?",
        ("resource_alloc",),
    ).fetchone()[0]
    schema = json.loads(schema_json)
    note_meta = next(col for col in schema["columns"] if col["name"] == "note")
    assert note_meta["display_name"] == "备注"
    assert note_meta["number_format"] == "@"


def test_write_cells_series_supports_text_columns():
    conn = _new_conn()
    create_dynamic_table(
        conn,
        table_name="notes_tbl",
        display_name="备注表",
        columns=[("note", "TEXT")],
    )

    result = json.loads(
        dispatch_tool(
            "write_cells_series",
            {
                "table_name": "notes_tbl",
                "row_id_template": "r_{i}",
                "column": "note",
                "start": 1,
                "end": 3,
                "text_template": "note_{i}",
            },
            _project_db(conn),
        )
    )

    assert result["status"] == "success"
    rows = conn.execute('SELECT row_id, note FROM "notes_tbl" ORDER BY row_id').fetchall()
    assert [(row["row_id"], row["note"]) for row in rows] == [
        ("r_1", "note_1"),
        ("r_2", "note_2"),
        ("r_3", "note_3"),
    ]


def test_write_cells_series_supports_text_value_list():
    conn = _new_conn()
    create_dynamic_table(
        conn,
        table_name="notes_value_tbl",
        display_name="备注值列表表",
        columns=[("note", "TEXT")],
    )

    result = json.loads(
        dispatch_tool(
            "write_cells_series",
            {
                "table_name": "notes_value_tbl",
                "row_id_template": "r_{i}",
                "column": "note",
                "start": 1,
                "end": 3,
                "value_list": ["A", "B", "C"],
            },
            _project_db(conn),
        )
    )

    assert result["status"] == "success"
    rows = conn.execute('SELECT row_id, note FROM "notes_value_tbl" ORDER BY row_id').fetchall()
    assert [(row["row_id"], row["note"]) for row in rows] == [
        ("r_1", "A"),
        ("r_2", "B"),
        ("r_3", "C"),
    ]


def test_sparse_sample_returns_actual_numeric_values():
    conn = _new_conn()
    create_dynamic_table(
        conn,
        table_name="sample_tbl",
        display_name="采样表",
        columns=[("level", "INTEGER"), ("hp", "REAL")],
    )
    conn.executemany(
        'INSERT INTO "sample_tbl" (row_id, "level", "hp") VALUES (?, ?, ?)',
        [(f"r{i}", i, i * 10.0) for i in range(1, 6)],
    )
    conn.commit()

    result = json.loads(
        dispatch_tool(
            "sparse_sample",
            {"table_name": "sample_tbl", "columns": ["level", "hp"], "n": 3},
            _project_db(conn),
        )
    )

    assert result["status"] == "success"
    assert result["data"]["cols"] == ["level", "hp"]
    assert result["data"]["rows"][0] == [1, 10.0]
    assert isinstance(result["data"]["rows"][0][1], float)


def test_lookup_calculator_returns_values_on_3d_table():
    conn = _new_conn()
    create_3d_table(
        conn,
        table_name="lookup_3d",
        display_name="三维查询表",
        dim1={
            "col_name": "level",
            "display_name": "等级",
            "keys": [{"key": "1", "display_name": "1级"}],
        },
        dim2={
            "col_name": "attr_type",
            "display_name": "属性",
            "keys": [{"key": "atk", "display_name": "攻击"}, {"key": "def", "display_name": "防御"}],
        },
        cols=[{"key": "value", "display_name": "值"}],
    )
    conn.execute('UPDATE "lookup_3d" SET "value" = 10 WHERE row_id = ?', ("1_atk",))
    conn.execute('UPDATE "lookup_3d" SET "value" = 20 WHERE row_id = ?', ("1_def",))
    conn.commit()
    register_calculator(
        conn,
        name="lookup3dcalc",
        kind="lookup",
        table_name="lookup_3d",
        axes=[
            {"name": "level", "source": "level"},
            {"name": "attr_type", "source": "attr_type"},
        ],
        value_column="value",
        brief="三维查询",
    )

    result = call_calculator(conn, name="lookup3dcalc", kwargs={"level": 1, "attr_type": "atk"})

    assert result["ok"] is True
    assert result["found"] is True
    assert result["value"] == 10.0


def test_default_rules_for_alloc_only_adds_percent_bounds_for_percent_formats():
    rules = default_rules_for(
        "alloc",
        [
            {"name": "row_id", "sql_type": "TEXT", "number_format": ""},
            {"name": "damage_value", "sql_type": "REAL", "number_format": "#,##0"},
            {"name": "crit_rate", "sql_type": "REAL", "number_format": "0.00%"},
        ],
        [],
    )

    rule_ids = {rule["id"] for rule in rules}
    assert "pct_crit_rate_bounds" in rule_ids
    assert "alloc_damage_value_bounds" not in rule_ids


def test_lookup_formulas_accept_row_arithmetic_arguments():
    conn = _new_conn()
    create_dynamic_table(
        conn,
        table_name="lookup_src",
        display_name="查找源",
        columns=[("key", "INTEGER"), ("val", "REAL")],
    )
    create_dynamic_table(
        conn,
        table_name="lookup_target",
        display_name="查找目标",
        columns=[("key", "INTEGER"), ("out", "REAL")],
    )
    for i, value in [(1, 100.0), (2, 200.0), (3, 300.0)]:
        conn.execute(
            'INSERT INTO "lookup_src" (row_id, "key", "val") VALUES (?, ?, ?)',
            (f"s{i}", i, value),
        )
        conn.execute(
            'INSERT INTO "lookup_target" (row_id, "key", "out") VALUES (?, ?, ?)',
            (f"t{i}", i, 0.0),
        )
    conn.commit()

    first = register_formula(
        conn,
        "lookup_target",
        "out",
        "IF(@lookup_target[key] > 1, VLOOKUP(@this[key]-1, @@lookup_src[key], @@lookup_src[val], 0), 0)",
    )
    second = register_formula(
        conn,
        "lookup_target",
        "out",
        "XLOOKUP(@key-1, @@lookup_src[key], @@lookup_src[val], 0)",
    )

    assert first["ok"] is True
    assert second["ok"] is True


def test_register_formula_auto_executes_string_formulas_for_text_columns():
    conn = _new_conn()
    create_dynamic_table(
        conn,
        table_name="text_formula_target",
        display_name="文本公式目标表",
        columns=[("note", "TEXT")],
    )
    conn.execute(
        'INSERT INTO "text_formula_target" (row_id, "note") VALUES (?, ?)',
        ("row_1", ""),
    )
    conn.commit()

    result = register_formula(conn, "text_formula_target", "note", "CONCAT('prefix_', 'done')")

    assert result["ok"] is True
    assert result["auto_executed"]["rows_updated"] == 1
    row = conn.execute(
        'SELECT "note" FROM "text_formula_target" WHERE row_id = ?',
        ("row_1",),
    ).fetchone()
    value = row["note"] if isinstance(row, sqlite3.Row) else row[0]
    assert value == "prefix_done"
