"""性能优化总开关与计时记录。

通过 `project_settings` 中的 `perf` 命名空间控制各项优化是否启用。
所有新功能在开关关闭时必须回退到旧路径，保证最大可回退性。

默认值（与部署文档一致）：
- perf.use_min_column_load     = True   # A2 最小列加载
- perf.use_batch_writeback     = True   # A3 批量回写
- perf.use_batch_lookup        = True   # A4 calculator 批量 lookup / 缓存
- perf.use_dag_recalc          = True   # A5 DAG 批量重算
- perf.use_duckdb_compute      = False  # B1/B2 DuckDB 计算（默认关闭）
"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, Optional


_DEFAULTS: Dict[str, bool] = {
    "use_min_column_load": True,
    "use_batch_writeback": True,
    "use_batch_lookup": True,
    "use_dag_recalc": True,
    "use_duckdb_compute": False,
}


def _load_perf_settings(conn: sqlite3.Connection) -> Dict[str, Any]:
    try:
        row = conn.execute(
            "SELECT value_json FROM project_settings WHERE key = ?",
            ("perf",),
        ).fetchone()
    except sqlite3.OperationalError:
        return {}
    if not row:
        return {}
    raw = row[0] if not isinstance(row, sqlite3.Row) else row["value_json"]
    try:
        data = json.loads(raw or "{}")
    except Exception:  # noqa: BLE001
        return {}
    return data if isinstance(data, dict) else {}


def perf_flag(conn: sqlite3.Connection, key: str, default: Optional[bool] = None) -> bool:
    """读取性能开关；缺省值优先取 _DEFAULTS，再退到入参 default 或 False。"""
    if default is None:
        default = _DEFAULTS.get(key, False)
    settings = _load_perf_settings(conn)
    val = settings.get(key, default)
    return bool(val)


def perf_status(conn: sqlite3.Connection) -> Dict[str, Any]:
    """诊断用：返回当前所有性能开关的有效值（合并默认值与项目覆盖）。"""
    overrides = _load_perf_settings(conn)
    out: Dict[str, Any] = {}
    for k, default_v in _DEFAULTS.items():
        out[k] = bool(overrides.get(k, default_v))
    # 透出非 _DEFAULTS 的额外键，方便调试
    for k, v in overrides.items():
        if k not in out:
            out[k] = v
    return {"flags": out, "defaults": dict(_DEFAULTS), "overrides": overrides}


def perf_record(
    conn: sqlite3.Connection,
    *,
    op: str,
    elapsed_ms: float,
    table_name: str = "",
    column_name: str = "",
    n_rows: int = 0,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """写入 _perf_log；任何异常静默吞掉，避免影响主流程。"""
    try:
        conn.execute(
            """
            INSERT INTO _perf_log (op, table_name, column_name, n_rows, elapsed_ms, extra_json, created_at)
            VALUES (?,?,?,?,?,?,?)
            """,
            (
                op,
                table_name,
                column_name,
                int(n_rows),
                float(elapsed_ms),
                json.dumps(extra or {}, ensure_ascii=False),
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            ),
        )
    except Exception:  # noqa: BLE001
        return


class PerfTimer:
    """轻量上下文计时器：with PerfTimer(conn, op='...', table=..., column=...) as t: ... t.set_rows(n)"""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        op: str,
        table_name: str = "",
        column_name: str = "",
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.conn = conn
        self.op = op
        self.table_name = table_name
        self.column_name = column_name
        self.extra = dict(extra or {})
        self._t0 = 0.0
        self._n_rows = 0

    def set_rows(self, n: int) -> None:
        self._n_rows = int(n)

    def add_extra(self, **kw: Any) -> None:
        self.extra.update(kw)

    def __enter__(self) -> "PerfTimer":
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        ms = (time.perf_counter() - self._t0) * 1000.0
        extra = dict(self.extra)
        if exc is not None:
            extra["error"] = str(exc)[:200]
        perf_record(
            self.conn,
            op=self.op,
            elapsed_ms=ms,
            table_name=self.table_name,
            column_name=self.column_name,
            n_rows=self._n_rows,
            extra=extra,
        )
