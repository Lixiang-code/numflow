from __future__ import annotations

import pathlib
import sqlite3
import sys
import time

import pytest
from fastapi import HTTPException

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from app.db.project_migrations import ensure_project_migrations
from app.db.project_schema import init_project_db
from app.deps import ProjectDB
from app.routers.pipeline import pipeline_gameplay_table_reset
from app.services.agent_tools import _get_gameplay_table_list, _set_gameplay_table_status
from app.services.gameplay_table_registry import (
    GAMEPLAY_TABLE_IN_PROGRESS_TIMEOUT_SECONDS,
    utc_now_iso,
)


def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_project_db(conn, seed_readme=False)
    ensure_project_migrations(conn)
    return conn


def _project_db(conn: sqlite3.Connection) -> ProjectDB:
    return ProjectDB(
        row={"id": 1, "name": "测试项目", "slug": "gameplay-table-recovery"},
        conn=conn,
        can_write=True,
    )


def _insert_gameplay_table(
    conn: sqlite3.Connection,
    *,
    table_id: str = "gem_table",
    status: str = "未开始",
    started_at: str | None = None,
    updated_at: str | None = None,
) -> None:
    now = updated_at or utc_now_iso()
    conn.execute(
        """
        INSERT INTO _gameplay_table_registry
            (table_id, display_name, readme, status, started_at, order_num, dependencies, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            table_id,
            "宝石玩法",
            "# 宝石说明",
            status,
            started_at,
            1,
            "[]",
            now,
            now,
        ),
    )
    conn.commit()


def test_gameplay_table_registry_has_started_at_column():
    conn = _new_conn()
    cols = {row[1] for row in conn.execute("PRAGMA table_info(_gameplay_table_registry)").fetchall()}
    assert "started_at" in cols


def test_migrations_backfill_started_at_for_in_progress_rows():
    conn = _new_conn()
    stale_started = utc_now_iso(time.time() - 600)
    _insert_gameplay_table(conn, status="进行中", started_at=None, updated_at=stale_started)

    ensure_project_migrations(conn)

    row = conn.execute(
        "SELECT started_at FROM _gameplay_table_registry WHERE table_id='gem_table'"
    ).fetchone()
    assert row is not None
    assert row["started_at"] == stale_started


def test_set_gameplay_table_status_tracks_started_at_lifecycle():
    conn = _new_conn()
    _insert_gameplay_table(conn)

    result = _set_gameplay_table_status(conn, "gem_table", "进行中")
    row = conn.execute(
        "SELECT status, started_at FROM _gameplay_table_registry WHERE table_id='gem_table'"
    ).fetchone()
    assert result["status"] == "success"
    assert row["status"] == "进行中"
    assert row["started_at"]

    result = _set_gameplay_table_status(conn, "gem_table", "已完成")
    row = conn.execute(
        "SELECT status, started_at FROM _gameplay_table_registry WHERE table_id='gem_table'"
    ).fetchone()
    assert result["status"] == "success"
    assert row["status"] == "已完成"
    assert row["started_at"] is None


def test_get_gameplay_table_list_recovers_stale_in_progress_table():
    conn = _new_conn()
    stale_started = utc_now_iso(time.time() - GAMEPLAY_TABLE_IN_PROGRESS_TIMEOUT_SECONDS - 60)
    _insert_gameplay_table(conn, status="进行中", started_at=stale_started, updated_at=stale_started)

    result = _get_gameplay_table_list(conn)
    item = result["data"]["tables"][0]
    row = conn.execute(
        "SELECT status, started_at FROM _gameplay_table_registry WHERE table_id='gem_table'"
    ).fetchone()

    assert result["status"] == "success"
    assert item["status"] == "未开始"
    assert row["status"] == "未开始"
    assert row["started_at"] is None


def test_get_gameplay_table_list_restores_revision_queue_for_stale_table():
    conn = _new_conn()
    stale_started = utc_now_iso(time.time() - GAMEPLAY_TABLE_IN_PROGRESS_TIMEOUT_SECONDS - 60)
    _insert_gameplay_table(conn, status="进行中", started_at=stale_started, updated_at=stale_started)
    conn.execute(
        """
        INSERT INTO _table_revision_requests (table_id, reason, requested_by_step, status, created_at, updated_at)
        VALUES (?, ?, ?, 'pending', ?, ?)
        """,
        ("gem_table", "伤害成长过高，需要回调", "balance_review", stale_started, stale_started),
    )
    conn.commit()

    result = _get_gameplay_table_list(conn)
    item = result["data"]["tables"][0]
    row = conn.execute(
        "SELECT status, started_at FROM _gameplay_table_registry WHERE table_id='gem_table'"
    ).fetchone()

    assert result["status"] == "success"
    assert item["status"] == "待修订"
    assert item["revision_reason"] == "伤害成长过高，需要回调"
    assert row["status"] == "待修订"
    assert row["started_at"] is None


def test_pipeline_gameplay_table_reset_restores_available_status():
    conn = _new_conn()
    _insert_gameplay_table(conn, status="进行中", started_at=utc_now_iso())

    result = pipeline_gameplay_table_reset("gem_table", _project_db(conn))
    row = conn.execute(
        "SELECT status, started_at FROM _gameplay_table_registry WHERE table_id='gem_table'"
    ).fetchone()

    assert result["ok"] is True
    assert result["new_status"] == "未开始"
    assert row["status"] == "未开始"
    assert row["started_at"] is None


def test_pipeline_gameplay_table_reset_rejects_non_in_progress_table():
    conn = _new_conn()
    _insert_gameplay_table(conn, status="已完成")

    with pytest.raises(HTTPException) as exc:
        pipeline_gameplay_table_reset("gem_table", _project_db(conn))

    assert exc.value.status_code == 400
