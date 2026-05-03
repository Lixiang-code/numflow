"""Shared helpers for gameplay table registry state management."""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, List

GAMEPLAY_TABLE_IN_PROGRESS_TIMEOUT_SECONDS = 2 * 60 * 60


def utc_now_iso(epoch: float | None = None) -> str:
    ts = time.time() if epoch is None else epoch
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def _latest_pending_revisions(conn: sqlite3.Connection) -> Dict[str, Dict[str, str]]:
    rows = conn.execute(
        """
        SELECT table_id, reason, requested_by_step, created_at
        FROM _table_revision_requests
        WHERE status='pending'
        ORDER BY table_id, created_at DESC
        """
    ).fetchall()
    latest: Dict[str, Dict[str, str]] = {}
    for row in rows:
        table_id = row[0]
        if table_id in latest:
            continue
        latest[table_id] = {
            "reason": row[1] or "",
            "requested_by_step": row[2] or "",
            "created_at": row[3] or "",
        }
    return latest


def gameplay_table_available_status(
    table_id: str,
    pending_revisions: Dict[str, Dict[str, str]],
) -> str:
    return "待修订" if table_id in pending_revisions else "未开始"


def reset_stale_in_progress_gameplay_tables(
    conn: sqlite3.Connection,
    *,
    now_epoch: float | None = None,
    timeout_seconds: int = GAMEPLAY_TABLE_IN_PROGRESS_TIMEOUT_SECONDS,
) -> List[Dict[str, str]]:
    cutoff = utc_now_iso((time.time() if now_epoch is None else now_epoch) - timeout_seconds)
    rows = conn.execute(
        """
        SELECT table_id
        FROM _gameplay_table_registry
        WHERE status='进行中'
          AND COALESCE(started_at, updated_at, '') != ''
          AND COALESCE(started_at, updated_at, '') < ?
        ORDER BY order_num, table_id
        """,
        (cutoff,),
    ).fetchall()
    if not rows:
        return []

    pending_revisions = _latest_pending_revisions(conn)
    now = utc_now_iso(now_epoch)
    recovered: List[Dict[str, str]] = []
    for row in rows:
        table_id = row[0]
        new_status = gameplay_table_available_status(table_id, pending_revisions)
        conn.execute(
            """
            UPDATE _gameplay_table_registry
            SET status=?, started_at=NULL, updated_at=?
            WHERE table_id=?
            """,
            (new_status, now, table_id),
        )
        recovered.append(
            {
                "table_id": table_id,
                "previous_status": "进行中",
                "new_status": new_status,
            }
        )
    conn.commit()
    return recovered


def list_registered_gameplay_tables(
    conn: sqlite3.Connection,
    *,
    recover_stale: bool = True,
    readme_limit: int | None = None,
) -> List[Dict[str, Any]]:
    if recover_stale:
        reset_stale_in_progress_gameplay_tables(conn)
    pending_revisions = _latest_pending_revisions(conn)
    rows = conn.execute(
        """
        SELECT table_id, display_name, readme, status, order_num, dependencies, started_at
        FROM _gameplay_table_registry
        ORDER BY order_num, table_id
        """
    ).fetchall()
    items: List[Dict[str, Any]] = []
    for row in rows:
        readme = row[2] or ""
        if readme_limit is not None:
            readme = readme[:readme_limit]
        item: Dict[str, Any] = {
            "table_id": row[0],
            "display_name": row[1],
            "readme": readme,
            "status": row[3] or "未开始",
            "order_num": row[4] or 0,
            "dependencies": json.loads(row[5] or "[]"),
        }
        revision = pending_revisions.get(row[0])
        if item["status"] == "待修订" and revision:
            item["revision_reason"] = revision["reason"]
            item["revision_requested_by"] = revision["requested_by_step"]
            item["revision_created_at"] = revision["created_at"]
        items.append(item)
    return items


def get_gameplay_table_detail(
    conn: sqlite3.Connection,
    table_ids: List[str],
) -> List[Dict[str, Any]]:
    """按 table_id 列表查询任务完整详情（含完整 readme）。"""
    if not table_ids:
        return []
    placeholders = ",".join(["?"] * len(table_ids))
    pending_revisions = _latest_pending_revisions(conn)
    rows = conn.execute(
        f"""SELECT table_id, display_name, readme, status, order_num, dependencies, started_at
        FROM _gameplay_table_registry
        WHERE table_id IN ({placeholders})
        ORDER BY order_num, table_id""",
        table_ids,
    ).fetchall()
    items: List[Dict[str, Any]] = []
    found = set()
    for row in rows:
        item: Dict[str, Any] = {
            "table_id": row[0],
            "display_name": row[1],
            "readme": row[2] or "",
            "status": row[3] or "未开始",
            "order_num": row[4] or 0,
            "dependencies": json.loads(row[5] or "[]"),
        }
        revision = pending_revisions.get(row[0])
        if item["status"] == "待修订" and revision:
            item["revision_reason"] = revision["reason"]
            item["revision_requested_by"] = revision["requested_by_step"]
            item["revision_created_at"] = revision["created_at"]
        items.append(item)
        found.add(row[0])
    not_found = [tid for tid in table_ids if tid not in found]
    if not_found:
        # 将结果包装在 dict 中以区分返回格式
        pass  # handled by caller
    return items


def reset_gameplay_table_to_available(
    conn: sqlite3.Connection,
    table_id: str,
    *,
    now_epoch: float | None = None,
) -> Dict[str, str]:
    row = conn.execute(
        "SELECT status FROM _gameplay_table_registry WHERE table_id=?",
        (table_id,),
    ).fetchone()
    if not row:
        raise LookupError(table_id)
    current_status = row[0] or "未开始"
    if current_status != "进行中":
        raise ValueError(current_status)

    pending_revisions = _latest_pending_revisions(conn)
    now = utc_now_iso(now_epoch)
    new_status = gameplay_table_available_status(table_id, pending_revisions)
    conn.execute(
        """
        UPDATE _gameplay_table_registry
        SET status=?, started_at=NULL, updated_at=?
        WHERE table_id=?
        """,
        (new_status, now, table_id),
    )
    conn.commit()
    return {
        "table_id": table_id,
        "previous_status": current_status,
        "new_status": new_status,
    }
