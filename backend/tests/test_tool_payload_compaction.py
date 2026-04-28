from __future__ import annotations

import json
import pathlib
import sqlite3
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from app.db.project_migrations import ensure_project_migrations
from app.db.project_schema import init_project_db
from app.deps import ProjectDB
from app.services.agent_tools import (
    dispatch_tool,
    _compact_compare_snapshot_result,
)
from app.services.calculator_ops import register_calculator
from app.services.table_ops import create_dynamic_table


def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_project_db(conn, seed_readme=False)
    ensure_project_migrations(conn)
    return conn


def _project_db(conn: sqlite3.Connection) -> ProjectDB:
    return ProjectDB(
        row={"id": 1, "name": "测试项目", "slug": "test"},
        conn=conn,
        can_write=True,
    )


def test_get_project_config_compacts_large_docs():
    conn = _new_conn()
    long_text = "# 标题\n" + ("内容" * 1200)
    conn.execute(
        "INSERT INTO project_settings (key, value_json, updated_at) VALUES (?,?,?)",
        ("global_readme", json.dumps({"text": long_text}, ensure_ascii=False), "2026-01-01T00:00:00Z"),
    )
    conn.execute(
        "INSERT INTO project_settings (key, value_json, updated_at) VALUES (?,?,?)",
        ("step_readme.alpha", json.dumps("alpha step readme", ensure_ascii=False), "2026-01-01T00:00:00Z"),
    )
    conn.execute(
        "INSERT INTO project_settings (key, value_json, updated_at) VALUES (?,?,?)",
        ("fixed_layer_config", json.dumps({"core": {"level_cap": 200}}, ensure_ascii=False), "2026-01-01T00:00:00Z"),
    )
    conn.commit()

    result = json.loads(dispatch_tool("get_project_config", {}, _project_db(conn)))
    settings = result["data"]["settings"]

    assert "step_readme.alpha" not in settings
    assert settings["step_readmes"] == {"count": 1, "steps": ["alpha"]}
    assert settings["fixed_layer_config"]["core"]["level_cap"] == 200
    assert settings["global_readme"]["text_length"] == len(long_text)
    assert settings["global_readme"]["truncated"] is True
    assert settings["global_readme"]["excerpt"] == long_text[:1600]
    assert settings["global_readme"]["headings"] == ["# 标题"]


def test_get_dependency_graph_hides_internal_ids():
    conn = _new_conn()
    conn.execute(
        """
        INSERT INTO _dependency_graph (from_table, from_column, to_table, to_column, edge_type)
        VALUES ('src_table', 'atk', 'dst_table', 'hp', 'formula')
        """
    )
    conn.commit()

    result = json.loads(
        dispatch_tool(
            "get_dependency_graph",
            {"table_name": "src_table", "direction": "full"},
            _project_db(conn),
        )
    )
    data = result["data"]

    assert data["edge_count"] == 1
    assert data["edges"] == [
        {
            "from_table": "src_table",
            "from_column": "atk",
            "to_table": "dst_table",
            "to_column": "hp",
            "edge_type": "formula",
        }
    ]


def test_compare_snapshot_compaction_drops_hash_noise():
    compact = _compact_compare_snapshot_result(
        {
            "snapshot_id": 23,
            "label": "snap",
            "changed_tables": [
                {
                    "table_name": "gameplay_attr_alloc",
                    "row_count_previous": 199,
                    "row_count_current": 230,
                    "previous_table_hash": "aaa",
                    "current_table_hash": "bbb",
                    "changed_columns": ["value"],
                    "added_columns": [],
                    "removed_columns": [],
                    "column_diff_note": None,
                }
            ],
            "legacy_compare": False,
        }
    )

    assert compact == {
        "snapshot_id": 23,
        "label": "snap",
        "changed_count": 1,
        "changed_tables": [
            {
                "table_name": "gameplay_attr_alloc",
                "row_count_previous": 199,
                "row_count_current": 230,
                "changed_columns": ["value"],
            }
        ],
        "legacy_compare": False,
    }


def test_call_calculator_not_found_omits_sql_and_params():
    conn = _new_conn()
    create_dynamic_table(
        conn,
        table_name="lookup_demo",
        display_name="查询演示",
        columns=[("level", "INTEGER"), ("value", "REAL")],
    )
    conn.execute('INSERT INTO "lookup_demo" (row_id, level, value) VALUES ("1", 1, 100.0)')
    conn.commit()
    register_calculator(
        conn,
        name="lookup_demo_calc",
        kind="lookup",
        table_name="lookup_demo",
        axes=[{"name": "level", "source": "level"}],
        value_column="value",
        brief="按等级查询演示数值",
    )

    result = json.loads(
        dispatch_tool(
            "call_calculator",
            {"name": "lookup_demo_calc", "kwargs": {"level": 2}},
            _project_db(conn),
        )
    )
    data = result["data"]

    assert data == {"ok": True, "value": None, "found": False}

