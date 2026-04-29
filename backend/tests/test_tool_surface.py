from __future__ import annotations

import json
import pathlib
import sqlite3
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from app.db.project_migrations import ensure_project_migrations
from app.db.project_schema import init_project_db
from app.deps import ProjectDB
from app.services.agent_runner import READ_TOOLS, WRITE_TOOLS, _tool_schema_payload
from app.services.agent_tools import build_tools_openai, dispatch_tool
from app.services.table_ops import create_3d_table


def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_project_db(conn, seed_readme=False)
    ensure_project_migrations(conn)
    return conn


def _project_db(conn: sqlite3.Connection) -> ProjectDB:
    return ProjectDB(
        row={"id": 1, "name": "测试项目", "slug": "tool-surface-test"},
        conn=conn,
        can_write=True,
    )


def _prepare_3d_table(conn: sqlite3.Connection) -> None:
    create_3d_table(
        conn,
        table_name="gem_attr_3d",
        display_name="宝石三维属性表",
        dim1={
            "col_name": "level",
            "display_name": "等级",
            "keys": [{"key": "1", "display_name": "1级"}, {"key": "2", "display_name": "2级"}],
        },
        dim2={
            "col_name": "gem_type",
            "display_name": "宝石类型",
            "keys": [{"key": "atk", "display_name": "攻击宝石"}, {"key": "def", "display_name": "防御宝石"}],
        },
        cols=[
            {
                "key": "atk_bonus",
                "display_name": "攻击加成",
                "dtype": "float",
                "number_format": "0.0000",
                "formula": "@level * 1.056487454",
            }
        ],
        readme="# 宝石表\n" + ("说明" * 1000),
        purpose="测试真实三维表工具读取",
        directory="落地表/宝石",
        tags=["宝石", "属性"],
    )


def test_tool_whitelists_expose_critical_existing_and_new_tools():
    must_read = {
        "get_table_schema",
        "read_matrix",
        "read_3d_table",
        "read_3d_table_full",
        "list_directories",
        "list_exposed_params",
        "const_tag_list",
    }
    must_write = {
        "write_cells_series",
        "glossary_register",
        "const_register",
        "const_tag_register",
        "const_set",
        "const_delete",
        "set_table_directory",
        "create_matrix_table",
        "write_matrix_cells",
        "register_calculator",
        "expose_param_to_subsystems",
        "create_3d_table",
    }

    assert must_read.issubset(READ_TOOLS)
    assert must_write.issubset(WRITE_TOOLS)

    payload = _tool_schema_payload(build_tools_openai(), READ_TOOLS | WRITE_TOOLS)
    names = {item["name"] for item in payload}
    assert must_read.issubset(names)
    assert must_write.issubset(names)


def test_get_table_schema_returns_compact_matrix_metadata():
    conn = _new_conn()
    _prepare_3d_table(conn)

    result = json.loads(dispatch_tool("get_table_schema", {"table_name": "gem_attr_3d"}, _project_db(conn)))
    data = result["data"]
    data_json = json.dumps(data, ensure_ascii=False)

    assert result["status"] == "success"
    assert data["table_name"] == "gem_attr_3d"
    assert data["display_name"] == "宝石三维属性表"
    assert data["matrix_kind"] == "3d_matrix"
    assert data["directory"] == "落地表/宝石"
    assert data["tags"] == ["宝石", "属性"]
    assert data["column_count"] == 4
    assert "readme_excerpt" in data
    assert "readme" not in data
    assert data["readme_excerpt"]["truncated"] is True
    assert data["formulas"][0]["column_name"] == "atk_bonus"
    assert "created_at" not in data_json
    assert "updated_at" not in data_json


def test_read_3d_table_returns_sheet_view_and_rounded_values():
    conn = _new_conn()
    _prepare_3d_table(conn)

    result = json.loads(
        dispatch_tool(
            "read_3d_table",
            {"table_name": "gem_attr_3d", "dim2_keys": ["atk"], "limit_dim1": 1},
            _project_db(conn),
        )
    )
    data = result["data"]
    row = data["sheets"][0]["rows"][0]

    assert result["status"] == "success"
    assert data["dim1"]["truncated"] is True
    assert data["dim1"]["returned_keys"] == ["1"]
    assert data["dim2"]["returned_keys"] == ["atk"]
    assert data["returned_row_count"] == 1
    assert data["sheets"][0]["dim2_display_name"] == "攻击宝石"
    assert row["dim1_display_name"] == "1级"
    assert row["values"]["atk_bonus"] == 1.0565
    assert "created_at" not in json.dumps(data, ensure_ascii=False)


def test_read_3d_table_supports_arbitrary_slice_and_formula_view():
    conn = _new_conn()
    _prepare_3d_table(conn)

    result = json.loads(
        dispatch_tool(
            "read_3d_table",
            {
                "table_name": "gem_attr_3d",
                "keep_axes": ["metric"],
                "dim1_keys": ["1"],
                "dim2_keys": ["atk"],
            },
            _project_db(conn),
        )
    )
    data = result["data"]
    slice0 = data["slices"][0]
    item0 = slice0["items"][0]

    assert result["status"] == "success"
    assert data["view_mode"] == "list"
    assert data["keep_axes"] == ["metric"]
    assert data["axes"]["dim1"]["selected_keys"] == ["1"]
    assert data["axes"]["dim2"]["selected_keys"] == ["atk"]
    assert slice0["fixed"]["dim1"]["display_name"] == "1级"
    assert slice0["fixed"]["dim2"]["display_name"] == "攻击宝石"
    assert item0["key"] == "atk_bonus"
    assert item0["value"] == 1.0565
    assert item0["formula"]["formula"] == "@level * 1.056487454"


def test_read_3d_table_full_returns_canonical_three_axis_payload():
    conn = _new_conn()
    _prepare_3d_table(conn)

    result = json.loads(
        dispatch_tool(
            "read_3d_table_full",
            {"table_name": "gem_attr_3d"},
            _project_db(conn),
        )
    )
    data = result["data"]

    assert result["status"] == "success"
    assert data["kind"] == "3d_matrix"
    assert data["values_are_numeric_only"] is True
    assert data["axes"]["metric"]["keys"][0]["display_name"] == "攻击加成"
    assert data["column_formulas"]["atk_bonus"]["type"] == "row"
    assert data["data"]["1"]["atk"]["atk_bonus"] == 1.0565
