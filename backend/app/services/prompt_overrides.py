"""Project-level prompt override storage for system/tool prompt management."""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_prompt_override_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _prompt_overrides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            prompt_key TEXT NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            summary TEXT NOT NULL DEFAULT '',
            description TEXT NOT NULL DEFAULT '',
            reference_note TEXT NOT NULL DEFAULT '',
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(category, prompt_key)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _prompt_override_modules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prompt_override_id INTEGER NOT NULL,
            module_key TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL DEFAULT '',
            required INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(prompt_override_id, module_key),
            FOREIGN KEY(prompt_override_id) REFERENCES _prompt_overrides(id) ON DELETE CASCADE
        )
        """
    )
    conn.commit()


def _clone_modules(modules: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, module in enumerate(modules, start=1):
        out.append(
            {
                "module_key": str(module.get("module_key") or f"module_{idx}"),
                "title": str(module.get("title") or f"模块 {idx}"),
                "content": str(module.get("content") or ""),
                "required": bool(module.get("required")),
                "enabled": bool(module.get("enabled", module.get("required", False))),
                "sort_order": int(module.get("sort_order") or idx),
            }
        )
    out.sort(key=lambda item: (int(item.get("sort_order") or 0), item["module_key"]))
    return out


def _load_override_modules(conn: sqlite3.Connection, override_id: int) -> List[Dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT id, module_key, title, content, required, enabled, sort_order
        FROM _prompt_override_modules
        WHERE prompt_override_id = ?
        ORDER BY sort_order ASC, id ASC
        """,
        (override_id,),
    )
    modules: List[Dict[str, Any]] = []
    for row in cur.fetchall():
        if isinstance(row, sqlite3.Row):
            modules.append(
                {
                    "id": int(row["id"]),
                    "module_key": str(row["module_key"]),
                    "title": str(row["title"]),
                    "content": str(row["content"] or ""),
                    "required": bool(row["required"]),
                    "enabled": bool(row["enabled"]) or bool(row["required"]),
                    "sort_order": int(row["sort_order"]),
                }
            )
        else:
            modules.append(
                {
                    "id": int(row[0]),
                    "module_key": str(row[1]),
                    "title": str(row[2]),
                    "content": str(row[3] or ""),
                    "required": bool(row[4]),
                    "enabled": bool(row[5]) or bool(row[4]),
                    "sort_order": int(row[6]),
                }
            )
    return modules


def get_prompt_override(
    conn: sqlite3.Connection,
    *,
    category: str,
    prompt_key: str,
) -> Optional[Dict[str, Any]]:
    ensure_prompt_override_tables(conn)
    cur = conn.execute(
        """
        SELECT id, category, prompt_key, title, summary, description, reference_note, enabled, created_at, updated_at
        FROM _prompt_overrides
        WHERE category = ? AND prompt_key = ?
        """,
        (category, prompt_key),
    )
    row = cur.fetchone()
    if not row:
        return None
    if isinstance(row, sqlite3.Row):
        item = {key: row[key] for key in row.keys()}
    else:
        item = {
            "id": row[0],
            "category": row[1],
            "prompt_key": row[2],
            "title": row[3],
            "summary": row[4],
            "description": row[5],
            "reference_note": row[6],
            "enabled": row[7],
            "created_at": row[8],
            "updated_at": row[9],
        }
    item["id"] = int(item["id"])
    item["enabled"] = bool(item["enabled"])
    item["modules"] = _load_override_modules(conn, int(item["id"]))
    return item


def merge_prompt_item(default_item: Dict[str, Any], override_item: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged = {
        **default_item,
        "modules": _clone_modules(default_item.get("modules") or []),
    }
    if not override_item:
        merged["override"] = False
        return merged
    for key in ("title", "summary", "description", "reference_note", "enabled"):
        if key in override_item:
            merged[key] = override_item[key]
    modules = override_item.get("modules")
    if isinstance(modules, list) and modules:
        merged_modules = {str(module["module_key"]): module for module in _clone_modules(merged["modules"])}
        for module in _clone_modules(modules):
            key = str(module["module_key"])
            if key in merged_modules:
                merged_modules[key] = {**merged_modules[key], **module}
            else:
                merged_modules[key] = module
        merged["modules"] = sorted(
            merged_modules.values(),
            key=lambda item: (int(item.get("sort_order") or 0), str(item.get("module_key") or "")),
        )
    merged["override"] = True
    return merged


def list_prompt_items(
    conn: sqlite3.Connection,
    *,
    defaults: Sequence[Dict[str, Any]],
    category: str,
) -> List[Dict[str, Any]]:
    ensure_prompt_override_tables(conn)
    override_map = {
        str(item["prompt_key"]): item
        for item in (
            get_prompt_override(conn, category=category, prompt_key=str(default_item["prompt_key"]))
            for default_item in defaults
        )
        if item
    }
    merged_items: List[Dict[str, Any]] = []
    for idx, default_item in enumerate(defaults, start=1):
        item = merge_prompt_item(default_item, override_map.get(str(default_item["prompt_key"])))
        item["display_order"] = int(default_item.get("display_order") or idx)
        merged_items.append(item)
    merged_items.sort(key=lambda item: (int(item.get("display_order") or 0), str(item.get("title") or "")))
    return merged_items


def render_prompt_text(item: Dict[str, Any]) -> str:
    parts: List[str] = []
    for module in _clone_modules(item.get("modules") or []):
        if module.get("required") or module.get("enabled"):
            text = str(module.get("content") or "").strip()
            if text:
                parts.append(text)
    return "\n".join(parts).strip()


def upsert_prompt_override(
    conn: sqlite3.Connection,
    *,
    category: str,
    prompt_key: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    ensure_prompt_override_tables(conn)
    now = _now()
    title = str(payload.get("title") or "").strip()
    if not title:
        raise ValueError("title 必填")
    summary = str(payload.get("summary") or "").strip()
    description = str(payload.get("description") or "").strip()
    reference_note = str(payload.get("reference_note") or "").strip()
    enabled = bool(payload.get("enabled", True))
    modules = _clone_modules(payload.get("modules") or [])

    cur = conn.execute(
        "SELECT id FROM _prompt_overrides WHERE category = ? AND prompt_key = ?",
        (category, prompt_key),
    )
    row = cur.fetchone()
    if row:
        override_id = int(row[0] if not isinstance(row, sqlite3.Row) else row["id"])
        conn.execute(
            """
            UPDATE _prompt_overrides
            SET title = ?, summary = ?, description = ?, reference_note = ?, enabled = ?, updated_at = ?
            WHERE id = ?
            """,
            (title, summary, description, reference_note, 1 if enabled else 0, now, override_id),
        )
        conn.execute("DELETE FROM _prompt_override_modules WHERE prompt_override_id = ?", (override_id,))
    else:
        cur = conn.execute(
            """
            INSERT INTO _prompt_overrides (
                category, prompt_key, title, summary, description, reference_note, enabled, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (category, prompt_key, title, summary, description, reference_note, 1 if enabled else 0, now, now),
        )
        override_id = int(cur.lastrowid)

    for idx, module in enumerate(modules, start=1):
        conn.execute(
            """
            INSERT INTO _prompt_override_modules (
                prompt_override_id, module_key, title, content, required, enabled, sort_order, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                override_id,
                module["module_key"],
                module["title"],
                module["content"],
                1 if module.get("required") else 0,
                1 if module.get("enabled", module.get("required", False)) else 0,
                idx,
                now,
                now,
            ),
        )
    conn.commit()
    return get_prompt_override(conn, category=category, prompt_key=prompt_key) or {}


def delete_prompt_override(conn: sqlite3.Connection, *, category: str, prompt_key: str) -> bool:
    ensure_prompt_override_tables(conn)
    cur = conn.execute(
        "DELETE FROM _prompt_overrides WHERE category = ? AND prompt_key = ?",
        (category, prompt_key),
    )
    conn.commit()
    return int(cur.rowcount or 0) > 0
