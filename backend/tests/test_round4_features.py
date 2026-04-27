"""第4轮 UI 补齐——新增 API 端点及服务层的最小覆盖测试。"""
from __future__ import annotations

import pathlib
import sqlite3
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from app.db.project_schema import init_project_db
from app.db.project_migrations import ensure_project_migrations
from app.services.calculator_ops import list_calculators, call_calculator
from app.services.agent_tools import _list_exposed_params


# ---------- helpers ----------

def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    init_project_db(conn, seed_readme=False)
    ensure_project_migrations(conn)
    return conn


# ---------- migrations ----------

def test_directory_field_in_registry():
    conn = _new_conn()
    cur = conn.execute("PRAGMA table_info(_table_registry)")
    cols = {r[1] for r in cur.fetchall()}
    assert "directory" in cols


def test_matrix_meta_json_field_in_registry():
    conn = _new_conn()
    cur = conn.execute("PRAGMA table_info(_table_registry)")
    cols = {r[1] for r in cur.fetchall()}
    assert "matrix_meta_json" in cols


def test_glossary_table_exists():
    conn = _new_conn()
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='_glossary'"
    )
    assert cur.fetchone() is not None


# ---------- list_calculators ----------

def test_list_calculators_service_returns_list():
    conn = _new_conn()
    result = list_calculators(conn)
    assert isinstance(result, list)


# ---------- call_calculator unknown ----------

def test_call_calculator_unknown_returns_not_found():
    conn = _new_conn()
    result = call_calculator(conn, name="nonexistent_calc", kwargs={})
    # call_calculator returns dict with ok=False when calculator not found
    assert result.get("ok") is False


# ---------- exposed-params ----------

def test_exposed_params_service_returns_items():
    conn = _new_conn()
    result = _list_exposed_params(conn, "data_collection")
    assert isinstance(result, dict)
    # Returns {"items": [...]}
    assert "items" in result


# ---------- directory update ----------

def test_update_directory_via_sql():
    conn = _new_conn()
    # create a minimal entry in _table_registry
    conn.execute(
        "INSERT INTO _table_registry (table_name, schema_json) VALUES ('dir_test_tbl', '{}')"
    )
    conn.commit()
    conn.execute(
        "UPDATE _table_registry SET directory = ? WHERE table_name = ?",
        ("分类A", "dir_test_tbl"),
    )
    conn.commit()
    cur = conn.execute(
        "SELECT directory FROM _table_registry WHERE table_name = 'dir_test_tbl'"
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] == "分类A"


