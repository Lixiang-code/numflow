"""性能优化（A1–A5 + B1）回归与对照测试。"""

from __future__ import annotations

import json
import pathlib
import sqlite3
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from app.db.project_migrations import (
    _PERF_META_INDEXES,
    ensure_calculator_indexes,
    ensure_project_migrations,
)
from app.db.project_schema import init_project_db
from app.services import duckdb_compute
from app.services.calculator_ops import register_calculator
from app.services.formula_exec import (
    execute_formula_on_column,
    load_table_df,
    recalculate_downstream,
    recalculate_downstream_dag,
    register_formula,
    register_row_formula,
)
from app.services.perf_flags import perf_flag, perf_status
from app.services.recalc_lock import try_acquire_recalc_lock


def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_project_db(conn, seed_readme=False)
    ensure_project_migrations(conn)
    return conn


def _make_table(conn: sqlite3.Connection, name: str, *cols: str) -> None:
    col_defs = ", ".join(f'"{c}" REAL' for c in cols)
    conn.execute(f'CREATE TABLE "{name}" (row_id TEXT PRIMARY KEY, {col_defs})')
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute(
        "INSERT INTO _table_registry (table_name, layer) VALUES (?, 'dynamic')",
        (name,),
    )


def _insert_rows(conn: sqlite3.Connection, name: str, rows):
    cols = list(rows[0].keys())
    placeholders = ",".join("?" for _ in cols)
    col_sql = ",".join(f'"{c}"' for c in cols)
    conn.executemany(
        f'INSERT INTO "{name}" ({col_sql}) VALUES ({placeholders})',
        [tuple(r[c] for c in cols) for r in rows],
    )
    conn.commit()


def _set_perf(conn: sqlite3.Connection, **flags):
    overrides = perf_status(conn)["overrides"]
    overrides.update({k: bool(v) for k, v in flags.items()})
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute(
        "INSERT INTO project_settings (key, value_json, updated_at) VALUES (?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at",
        ("perf", json.dumps(overrides), now),
    )
    conn.commit()


# ───────────────────── A1: 索引存在性 ─────────────────────


def test_a1_meta_indexes_exist():
    conn = _new_conn()
    names = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )
    }
    for idx_name, _, _ in _PERF_META_INDEXES:
        assert idx_name in names, f"缺少索引 {idx_name}"


def test_a1_calculator_index_auto_built():
    conn = _new_conn()
    _make_table(conn, "lookup_t", "level", "gameplay", "value")
    _insert_rows(
        conn,
        "lookup_t",
        [{"row_id": "r1", "level": 1, "gameplay": 1, "value": 10.0}],
    )
    register_calculator(
        conn,
        name="lookup_calc",
        kind="lookup",
        table_name="lookup_t",
        axes=[{"name": "gameplay", "source": "gameplay"}, {"name": "level", "source": "level"}],
        value_column="value",
        brief="测试 calculator 索引自动建立",
    )
    idx_names = [
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='lookup_t'"
        )
    ]
    assert any(n.startswith("idx_calc__") for n in idx_names)


# ───────────────────── A2: 最小列加载 ─────────────────────


def test_a2_load_table_df_with_columns_whitelist():
    conn = _new_conn()
    _make_table(conn, "t1", "a", "b", "c", "d")
    _insert_rows(conn, "t1", [{"row_id": "r1", "a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0}])
    df_full = load_table_df(conn, "t1")
    df_min = load_table_df(conn, "t1", ["a", "b"])
    assert set(df_full.columns) == {"row_id", "a", "b", "c", "d"}
    assert set(df_min.columns) == {"row_id", "a", "b"}


# ───────────────────── A3: 批量回写正确性 ─────────────────────


def test_a3_batch_writeback_1k_rows_correct():
    conn = _new_conn()
    _make_table(conn, "src", "x", "y")
    rows = [{"row_id": f"r{i}", "x": float(i), "y": 0.0} for i in range(1000)]
    _insert_rows(conn, "src", rows)
    register_formula(conn, "src", "y", "@src[x] * 2")
    df = load_table_df(conn, "src")
    assert len(df) == 1000
    expected = {f"r{i}": float(i) * 2 for i in range(1000)}
    actual = dict(conn.execute('SELECT row_id, y FROM src').fetchall())
    assert actual == expected
    # provenance 全量写入
    n = conn.execute(
        "SELECT count(*) FROM _cell_provenance WHERE table_name='src' AND column_name='y'"
    ).fetchone()[0]
    assert n == 1000


# ───────────────────── A4: perf 开关行为对照 ─────────────────────


def _make_chain_env() -> sqlite3.Connection:
    conn = _new_conn()
    _make_table(conn, "base", "x", "y")
    _make_table(conn, "mid", "v", "w")
    _make_table(conn, "top", "z")
    _insert_rows(conn, "base", [{"row_id": f"r{i}", "x": float(i), "y": 0.0} for i in range(50)])
    _insert_rows(conn, "mid", [{"row_id": f"r{i}", "v": 0.0, "w": 0.0} for i in range(50)])
    _insert_rows(conn, "top", [{"row_id": f"r{i}", "z": 0.0} for i in range(50)])
    register_formula(conn, "base", "y", "@base[x] + 1")
    register_formula(conn, "mid", "v", "@base[y] * 2")
    register_formula(conn, "mid", "w", "@base[y] + 3")
    register_formula(conn, "top", "z", "@mid[v] + @mid[w]")
    return conn


def test_a4_results_match_under_flag_toggle():
    # Run with all flags on (default), capture results
    conn1 = _make_chain_env()
    res_on = dict(conn1.execute('SELECT row_id, z FROM top').fetchall())

    # Run with all perf flags off (legacy paths)
    conn2 = _make_chain_env()
    _set_perf(
        conn2,
        use_min_column_load=False,
        use_batch_writeback=False,
        use_batch_lookup=False,
        use_dag_recalc=False,
    )
    # Re-execute everything
    execute_formula_on_column(conn2, "base", "y")
    execute_formula_on_column(conn2, "mid", "v")
    execute_formula_on_column(conn2, "mid", "w")
    execute_formula_on_column(conn2, "top", "z")
    res_off = dict(conn2.execute('SELECT row_id, z FROM top').fetchall())

    assert res_on == res_off


# ───────────────────── A5: DAG 去重 ─────────────────────


def test_a5_dag_each_node_executes_once():
    """A → B、A → C、B → C：触发 A 重算时，C 只算一次。"""
    conn = _new_conn()
    _make_table(conn, "A", "v", "x")
    _make_table(conn, "B", "y")
    _make_table(conn, "C", "z")
    _insert_rows(conn, "A", [{"row_id": f"r{i}", "v": float(i), "x": 1.0} for i in range(5)])
    _insert_rows(conn, "B", [{"row_id": f"r{i}", "y": 0.0} for i in range(5)])
    _insert_rows(conn, "C", [{"row_id": f"r{i}", "z": 0.0} for i in range(5)])
    register_formula(conn, "A", "x", "@A[v] + 1")
    register_formula(conn, "B", "y", "@A[x] * 10")
    # C 同时依赖 A.x 与 B.y → 经典菱形依赖
    register_formula(conn, "C", "z", "@A[x] + @B[y]")

    out = recalculate_downstream_dag(conn, [("A", "x")])
    executed = [(r["table"], r["column"]) for r in out["executed"]]
    # 每个 (table,col) 只出现一次
    assert len(executed) == len(set(executed))
    # 拓扑顺序：B 在 C 之前
    assert executed.index(("B", "y")) < executed.index(("C", "z"))
    # 验证最终值正确：x=v+1=[1..5], y=x*10=[10..50], z=x+y=[11..55]
    actual_z = dict(conn.execute("SELECT row_id, z FROM C").fetchall())
    expected = {f"r{i}": float(i + 1) + (float(i + 1) * 10) for i in range(5)}
    assert actual_z == expected


def test_a5_recalculate_downstream_uses_dag_when_enabled():
    conn = _make_chain_env()
    out = recalculate_downstream(conn, "base", "y")
    executed = {(r["table"], r["column"]) for r in out["executed"]}
    # base.y 自身是 seed 不在 executed；下游应包含 mid.v, mid.w, top.z
    assert {("mid", "v"), ("mid", "w"), ("top", "z")} <= executed


def test_a5_recalculate_downstream_legacy_when_flag_off():
    conn = _make_chain_env()
    _set_perf(conn, use_dag_recalc=False)
    out = recalculate_downstream(conn, "base", "y")
    executed = {(r["table"], r["column"]) for r in out["executed"]}
    # 旧路径只算一跳直接下游
    assert executed == {("mid", "v"), ("mid", "w")}


def test_a5_failed_dependency_blocks_downstream():
    conn = _new_conn()
    _make_table(conn, "A", "x")
    _make_table(conn, "B", "y")
    _make_table(conn, "C", "z")
    _insert_rows(conn, "A", [{"row_id": "r1", "x": 5.0}])
    _insert_rows(conn, "B", [{"row_id": "r1", "y": 100.0}])
    _insert_rows(conn, "C", [{"row_id": "r1", "z": 0.0}])
    register_formula(conn, "B", "y", "@A[x] + ${missing_const}", defer=True)
    register_formula(conn, "C", "z", "@B[y] + 5", defer=True)

    out = recalculate_downstream_dag(conn, [("A", "x")])

    assert out["executed"] == []
    assert out["errors"] == ["B.y: 公式引用未注册常量：missing_const"]
    assert out["skipped"] == [
        {
            "table": "C",
            "column": "z",
            "reason": "blocked_by_failed_dependency",
            "blocked_by": ["B.y"],
        }
    ]
    assert conn.execute("SELECT z FROM C WHERE row_id='r1'").fetchone()[0] == 0.0


def test_a5_row_formula_participates_in_dag():
    conn = _new_conn()
    _make_table(conn, "T", "v", "x", "y")
    _insert_rows(conn, "T", [{"row_id": "r1", "v": 1.0, "x": 0.0, "y": 0.0}])
    register_formula(conn, "T", "x", "@T[v] + 1", defer=True)
    register_row_formula(conn, "T", "y", "@x + 1")
    execute_formula_on_column(conn, "T", "x")

    out = recalculate_downstream(conn, "T", "x")

    executed = {(r["table"], r["column"]) for r in out["executed"]}
    assert executed == {("T", "y")}
    assert conn.execute("SELECT y FROM T WHERE row_id='r1'").fetchone()[0] == 3.0


def test_a5_dag_can_execute_seed_formulas_when_requested():
    conn = _new_conn()
    _make_table(conn, "A", "x")
    _make_table(conn, "B", "y")
    _insert_rows(conn, "A", [{"row_id": "r1", "x": 0.0}])
    _insert_rows(conn, "B", [{"row_id": "r1", "y": 0.0}])
    register_formula(conn, "A", "x", "const_value(5)", defer=True)
    register_formula(conn, "B", "y", "@A[x] + 1", defer=True)

    out = recalculate_downstream_dag(conn, [("A", "x")], execute_seeds=True)

    executed = [(r["table"], r["column"]) for r in out["executed"]]
    assert executed == [("A", "x"), ("B", "y")]
    assert conn.execute("SELECT x FROM A WHERE row_id='r1'").fetchone()[0] == 5.0
    assert conn.execute("SELECT y FROM B WHERE row_id='r1'").fetchone()[0] == 6.0


def test_a5_recalc_lock_is_single_statement_and_respects_cooldown():
    conn = _new_conn()

    assert try_acquire_recalc_lock(conn, table_name="T", now_ms=10_000) is True
    assert try_acquire_recalc_lock(conn, table_name="T", now_ms=12_999) is False
    assert try_acquire_recalc_lock(conn, table_name="T", now_ms=13_000) is True


# ───────────────────── B1: DuckDB 路径 ─────────────────────


def test_b1_default_disabled():
    conn = _new_conn()
    assert duckdb_compute.is_enabled(conn) is False


def test_b1_complex_formula_falls_back():
    """call_calculator 不在白名单 → NotSupported；上层应 fallback 不报错。"""
    conn = _new_conn()
    _make_table(conn, "lk", "k", "value")
    _insert_rows(conn, "lk", [{"row_id": "r1", "k": 1.0, "value": 99.0}])
    register_calculator(
        conn, name="lc", kind="lookup", table_name="lk",
        axes=[{"name": "k", "source": "k"}],
        value_column="value",
        brief="测试",
    )
    _make_table(conn, "t", "k", "out")
    _insert_rows(conn, "t", [{"row_id": "r1", "k": 1.0, "out": 0.0}])
    register_formula(conn, "t", "out", "call_calculator('lc', @t[k])")
    # 即使开启 DuckDB，含 call_calculator 也应自动 fallback
    _set_perf(conn, use_duckdb_compute=True)
    res = execute_formula_on_column(conn, "t", "out")
    assert res["ok"]
    assert conn.execute("SELECT out FROM t WHERE row_id='r1'").fetchone()[0] == 99.0


def test_b1_whitelist_check_rejects_cross_table():
    conn = _new_conn()
    _make_table(conn, "t", "a", "b")
    _insert_rows(conn, "t", [{"row_id": "r1", "a": 1.0, "b": 0.0}])
    try:
        duckdb_compute._check_whitelist(
            "@t[a] + @other[a]", "t", {"row_id", "a", "b"}
        )
    except duckdb_compute.NotSupported:
        return
    raise AssertionError("应拒绝跨表引用")


def test_b1_whitelist_accepts_simple_arithmetic():
    used, sql = duckdb_compute._check_whitelist(
        "@t[a] + @t[b] * 2 - 1", "t", {"row_id", "a", "b"}
    )
    assert used == {"a", "b"}
    assert "\"a\"" in sql and "\"b\"" in sql


def test_a4_batch_lookup_reuses_calculator_metadata_query():
    conn = _new_conn()
    _make_table(conn, "lk", "k", "value")
    _insert_rows(
        conn,
        "lk",
        [
            {"row_id": "r1", "k": 1.0, "value": 11.0},
            {"row_id": "r2", "k": 2.0, "value": 22.0},
            {"row_id": "r3", "k": 3.0, "value": 33.0},
        ],
    )
    register_calculator(
        conn,
        name="lc",
        kind="lookup",
        table_name="lk",
        axes=[{"name": "k", "source": "k"}],
        value_column="value",
        brief="测试 metadata cache",
    )
    _make_table(conn, "t", "k", "out")
    _insert_rows(
        conn,
        "t",
        [
            {"row_id": "r1", "k": 1.0, "out": 0.0},
            {"row_id": "r2", "k": 2.0, "out": 0.0},
            {"row_id": "r3", "k": 3.0, "out": 0.0},
        ],
    )
    register_formula(conn, "t", "out", "call_calculator('lc', @t[k])", defer=True)

    count = 0

    def _trace(sql: str) -> None:
        nonlocal count
        if 'FROM _calculators WHERE name = ' in sql:
            count += 1

    conn.set_trace_callback(_trace)
    try:
        execute_formula_on_column(conn, "t", "out")
    finally:
        conn.set_trace_callback(None)

    assert count == 1
    assert dict(conn.execute('SELECT row_id, out FROM t').fetchall()) == {
        "r1": 11.0,
        "r2": 22.0,
        "r3": 33.0,
    }


def test_a1_calculator_index_uses_level_alias_source():
    conn = _new_conn()
    _make_table(conn, "lookup_alias", "lvl", "gameplay", "value")
    _insert_rows(
        conn,
        "lookup_alias",
        [{"row_id": "r1", "lvl": 1.0, "gameplay": 2.0, "value": 3.0}],
    )
    register_calculator(
        conn,
        name="lookup_alias_calc",
        kind="lookup",
        table_name="lookup_alias",
        axes=[{"name": "level", "source": "lvl"}, {"name": "gameplay", "source": "gameplay"}],
        value_column="value",
        brief="测试 level alias 索引",
    )

    sqls = [
        r[0]
        for r in conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='lookup_alias' AND name LIKE 'idx_calc__%'"
        ).fetchall()
        if r[0]
    ]
    assert any('"gameplay"' in sql and '"lvl"' in sql for sql in sqls)
