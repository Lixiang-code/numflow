"""表级重算去重锁。

锁信息存放在 `project_settings`，键格式为 `_recalc_lock:<table_name>`，值为最近一次
重算触发时间戳（毫秒）。
"""

from __future__ import annotations

import json
import sqlite3
import time


def _lock_key(table_name: str) -> str:
    return f"_recalc_lock:{table_name}"


def try_acquire_recalc_lock(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    cooldown_ms: int = 3000,
    now_ms: int | None = None,
) -> bool:
    """原子尝试获取表级重算锁。

    返回 True 表示本次成功取得锁；False 表示冷却窗口内已有更近的一次重算请求。
    """
    lock_now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    updated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    cur = conn.execute(
        """
        INSERT INTO project_settings (key, value_json, updated_at)
        VALUES (?,?,?)
        ON CONFLICT(key) DO UPDATE SET
            value_json = excluded.value_json,
            updated_at = excluded.updated_at
        WHERE CAST(COALESCE(NULLIF(project_settings.value_json, ''), '0') AS INTEGER) <= ?
        """,
        (
            _lock_key(table_name),
            json.dumps(lock_now_ms),
            updated_at,
            lock_now_ms - int(cooldown_ms),
        ),
    )
    conn.commit()
    return bool(cur.rowcount)


def set_recalc_lock(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    now_ms: int | None = None,
    commit: bool = True,
) -> None:
    """直接写入/刷新表级重算锁时间戳。"""
    lock_now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    conn.execute(
        """
        INSERT INTO project_settings (key, value_json, updated_at)
        VALUES (?,?,?)
        ON CONFLICT(key) DO UPDATE SET
            value_json = excluded.value_json,
            updated_at = excluded.updated_at
        """,
        (
            _lock_key(table_name),
            json.dumps(lock_now_ms),
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        ),
    )
    if commit:
        conn.commit()
