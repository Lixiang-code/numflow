"""快照创建与对比（表级哈希 + 列级哈希，便于对比变更列）。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple


def _hash_bytes(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


def _table_detail(conn: sqlite3.Connection, table_name: str) -> Dict[str, Any]:
    cur = conn.execute(f'SELECT * FROM "{table_name}" ORDER BY row_id')
    rows = [dict(r) for r in cur.fetchall()]
    blob = json.dumps(rows, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    table_hash = _hash_bytes(blob)
    column_hashes: Dict[str, str] = {}
    if rows:
        for k in sorted(rows[0].keys()):
            vals = [r.get(k) for r in rows]
            col_blob = json.dumps(vals, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
            column_hashes[k] = _hash_bytes(col_blob)
    return {"row_count": len(rows), "table_hash": table_hash, "column_hashes": column_hashes}


def _all_tables_payload(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    cur = conn.execute("SELECT table_name FROM _table_registry ORDER BY table_name")
    return {str(tn): _table_detail(conn, str(tn)) for (tn,) in cur.fetchall()}


def _coerce_stored_table_entry(val: Any) -> Tuple[bool, Dict[str, Any]]:
    """返回 (is_legacy_string_hash, normalized_dict)。"""
    if isinstance(val, str):
        return True, {"table_hash": val, "column_hashes": {}, "row_count": None}
    if isinstance(val, dict):
        if "table_hash" in val or "column_hashes" in val:
            return False, {
                "table_hash": val.get("table_hash"),
                "column_hashes": dict(val.get("column_hashes") or {}),
                "row_count": val.get("row_count"),
            }
        return True, {"table_hash": None, "column_hashes": {}, "row_count": None}
    return True, {"table_hash": None, "column_hashes": {}, "row_count": None}


def create_snapshot(conn: sqlite3.Connection, *, label: str, note: str = "") -> Dict[str, Any]:
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    tables = _all_tables_payload(conn)
    payload = {"format": 2, "tables": tables}
    conn.execute(
        """
        INSERT INTO _snapshots (label, created_at, note, payload_json)
        VALUES (?,?,?,?)
        """,
        (label, now, note, json.dumps(payload, ensure_ascii=False)),
    )
    sid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.commit()
    return {"snapshot_id": sid, "label": label, "table_count": len(tables)}


def list_snapshots(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    cur = conn.execute(
        "SELECT id, label, created_at, note FROM _snapshots ORDER BY id DESC LIMIT 100"
    )
    return [dict(r) for r in cur.fetchall()]


def compare_snapshot(conn: sqlite3.Connection, snapshot_id: int) -> Dict[str, Any]:
    cur = conn.execute(
        "SELECT id, label, payload_json FROM _snapshots WHERE id = ?",
        (snapshot_id,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError("未知快照")
    old_raw = json.loads(row["payload_json"] or "{}")
    old_tables = old_raw.get("tables") or {}
    current = _all_tables_payload(conn)
    changed: List[Dict[str, Any]] = []
    names = sorted(set(old_tables.keys()) | set(current.keys()))
    for tn in names:
        legacy, old_norm = _coerce_stored_table_entry(old_tables.get(tn))
        new_norm = current.get(tn) or {"table_hash": None, "column_hashes": {}, "row_count": 0}
        oh = old_norm.get("table_hash")
        nh = new_norm.get("table_hash")
        old_cols: Dict[str, str] = dict(old_norm.get("column_hashes") or {})
        new_cols: Dict[str, str] = dict(new_norm.get("column_hashes") or {})
        orow = old_norm.get("row_count")
        nrow = new_norm.get("row_count")

        if oh == nh and orow == nrow:
            keys = set(old_cols) | set(new_cols)
            if all(old_cols.get(k) == new_cols.get(k) for k in keys):
                continue

        if legacy and not old_cols:
            added_cols: List[str] = []
            removed_cols = []
            changed_cols_out: Optional[List[str]] = None
            column_diff_note = "旧快照仅为表级哈希，无列级明细；若表哈希变化则无法列出具体列名"
        else:
            added_cols = sorted(k for k in new_cols if k not in old_cols)
            removed_cols = sorted(k for k in old_cols if k not in new_cols)
            changed_cols_out = sorted(
                k for k in (set(old_cols) & set(new_cols)) if old_cols.get(k) != new_cols.get(k)
            )
            column_diff_note = None

        changed.append(
            {
                "table_name": tn,
                "row_count_previous": orow,
                "row_count_current": nrow,
                "previous_table_hash": oh,
                "current_table_hash": nh,
                "changed_columns": changed_cols_out,
                "added_columns": added_cols,
                "removed_columns": removed_cols,
                "column_diff_note": column_diff_note,
            }
        )
    return {
        "snapshot_id": snapshot_id,
        "label": row["label"],
        "changed_tables": changed,
        "legacy_compare": old_raw.get("format") != 2,
    }
