"""性能优化（A1–A5 + B1）回归与对照测试。"""

from __future__ import annotations

import json
import os
import pathlib
import sqlite3
import sys
import tempfile
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from app.db.project_migrations import (
    _PERF_META_INDEXES,
    ensure_calculator_indexes,
    ensure_project_migrations,
)
from app.db.project_schema import init_project_db
from app.services import duckdb_compute, formula_engine, formula_exec
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
from app.services.table_ops import create_3d_table


class _CountingConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.commit_count = 0

    def commit(self):
        self.commit_count += 1
        return super().commit()


def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_project_db(conn, seed_readme=False)
    ensure_project_migrations(conn)
    return conn


def _new_counting_conn() -> _CountingConnection:
    conn = sqlite3.connect(":memory:", factory=_CountingConnection)
    conn.row_factory = sqlite3.Row
    init_project_db(conn, seed_readme=False)
    ensure_project_migrations(conn)
    return conn


def _new_file_conn() -> tuple[sqlite3.Connection, str]:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    init_project_db(conn, seed_readme=False)
    ensure_project_migrations(conn)
    return conn, path


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


def test_a2_load_table_df_expands_cached_projection_with_missing_columns(monkeypatch):
    conn = _new_conn()
    _make_table(conn, "t1", "a", "b", "c", "d")
    _insert_rows(conn, "t1", [{"row_id": "r1", "a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0}])
    table_cache = {}
    sqls = []
    original_read_sql_query = formula_exec.pd.read_sql_query

    def _recording_read_sql_query(sql, *args, **kwargs):
        sqls.append(sql)
        return original_read_sql_query(sql, *args, **kwargs)

    monkeypatch.setattr(formula_exec.pd, "read_sql_query", _recording_read_sql_query)

    first = load_table_df(conn, "t1", ["a", "b"], table_cache=table_cache)
    second = load_table_df(conn, "t1", ["a", "b", "c"], table_cache=table_cache)

    assert list(first.columns) == ["row_id", "a", "b"]
    assert list(second.columns) == ["row_id", "a", "b", "c"]
    assert sqls == [
        'SELECT "row_id", "a", "b" FROM "t1"',
        'SELECT "row_id", "c" FROM "t1"',
    ]


def test_a2_load_table_df_reuses_cached_superset_without_sql(monkeypatch):
    conn = _new_conn()
    _make_table(conn, "t1", "a", "b", "c", "d")
    _insert_rows(conn, "t1", [{"row_id": "r1", "a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0}])
    table_cache = {}
    sqls = []
    original_read_sql_query = formula_exec.pd.read_sql_query

    def _recording_read_sql_query(sql, *args, **kwargs):
        sqls.append(sql)
        return original_read_sql_query(sql, *args, **kwargs)

    monkeypatch.setattr(formula_exec.pd, "read_sql_query", _recording_read_sql_query)

    full = load_table_df(conn, "t1", ["a", "b", "c"], table_cache=table_cache)
    sub = load_table_df(conn, "t1", ["a", "c"], table_cache=table_cache)

    assert list(full.columns) == ["row_id", "a", "b", "c"]
    assert list(sub.columns) == ["row_id", "a", "c"]
    assert sqls == ['SELECT "row_id", "a", "b", "c" FROM "t1"']


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
    # base.y 自身是 seed 不在 executed；直接下游会执行。
    assert {("mid", "v"), ("mid", "w")} <= executed
    assert ("top", "z") not in executed
    assert out["skipped"] == [{"table": "top", "column": "z", "reason": "upstream_unchanged"}]


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


def test_a5_dag_batches_commit_for_multiple_nodes():
    conn = _new_counting_conn()
    _make_table(conn, "A", "x")
    _make_table(conn, "B", "y")
    _make_table(conn, "C", "z")
    _insert_rows(conn, "A", [{"row_id": "r1", "x": 0.0}])
    _insert_rows(conn, "B", [{"row_id": "r1", "y": 0.0}])
    _insert_rows(conn, "C", [{"row_id": "r1", "z": 0.0}])
    register_formula(conn, "A", "x", "const_value(5)", defer=True)
    register_formula(conn, "B", "y", "@A[x] + 1", defer=True)
    register_formula(conn, "C", "z", "@B[y] + 1", defer=True)

    conn.commit_count = 0
    out = recalculate_downstream_dag(conn, [("A", "x")], execute_seeds=True)

    assert out["errors"] == []
    assert conn.commit_count == 1
    assert conn.execute("SELECT x FROM A WHERE row_id='r1'").fetchone()[0] == 5.0
    assert conn.execute("SELECT y FROM B WHERE row_id='r1'").fetchone()[0] == 6.0
    assert conn.execute("SELECT z FROM C WHERE row_id='r1'").fetchone()[0] == 7.0


def test_a5_dag_skips_downstream_when_seed_result_is_unchanged():
    conn = _new_conn()
    _make_table(conn, "A", "x")
    _make_table(conn, "B", "y")
    _insert_rows(conn, "A", [{"row_id": "r1", "x": 0.0}])
    _insert_rows(conn, "B", [{"row_id": "r1", "y": 0.0}])
    register_formula(conn, "A", "x", "const_value(5)", defer=True)
    register_formula(conn, "B", "y", "@A[x] + 1", defer=True)

    first = recalculate_downstream_dag(conn, [("A", "x")], execute_seeds=True)
    assert [(r["table"], r["column"]) for r in first["executed"]] == [("A", "x"), ("B", "y")]
    assert conn.execute("SELECT x FROM A WHERE row_id='r1'").fetchone()[0] == 5.0
    assert conn.execute("SELECT y FROM B WHERE row_id='r1'").fetchone()[0] == 6.0

    second = recalculate_downstream_dag(conn, [("A", "x")], execute_seeds=True)

    assert second["executed"] == [{"table": "A", "column": "x", "rows_changed": 0, "engine": ""}]
    assert second["skipped"] == [{"table": "B", "column": "y", "reason": "upstream_unchanged"}]
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


def test_b4_whitelist_accepts_cross_table_refs_with_alias_columns():
    used, sql, aref = duckdb_compute._check_whitelist(
        "@t[a] + @other[a]",
        "t",
        {"row_id", "a", "b"},
    )
    assert used == {"a"}
    assert aref == {}
    assert '"__r0"' in sql


def test_b1_whitelist_accepts_simple_arithmetic():
    used, sql, aref = duckdb_compute._check_whitelist(
        "@t[a] + @t[b] * 2 - 1", "t", {"row_id", "a", "b"}
    )
    assert used == {"a", "b"}
    assert "\"a\"" in sql and "\"b\"" in sql


def test_b2_whitelist_translates_index_match_if_and_minmax():
    used, sql, aref = duckdb_compute._check_whitelist(
        "IF(@t[k] > 1, MIN(10, INDEX(@@src[value], MATCH(@t[k], @@src[key], 0))), MAX(0, 1))",
        "t",
        {"row_id", "k", "out"},
    )
    assert used == {"k"}
    assert aref == {("src", "value"): 0, ("src", "key"): 1}
    assert "CASE WHEN" in sql
    assert "LEAST(" in sql
    assert "GREATEST(" in sql
    assert "list_position(__a1, \"k\")" in sql
    assert "list_element(__a0, CAST(list_position(__a1, \"k\") AS BIGINT))" in sql


def test_b3_whitelist_translates_vlookup_xlookup_and_concat():
    used, sql, aref = duckdb_compute._check_whitelist(
        "IF(@t[level] > 0, VLOOKUP(CONCAT(@t[level], '_', @t[kind]), @@src[row_id], @@src[value], 0), XLOOKUP(@t[level], @@src_num[key], @@src_num[value], 0))",
        "t",
        {"row_id", "level", "kind", "out"},
    )
    assert used == {"level", "kind"}
    assert aref == {
        ("src", "row_id"): 0,
        ("src", "value"): 1,
        ("src_num", "key"): 2,
        ("src_num", "value"): 3,
    }
    assert "CONCAT(\"level\", '_', \"kind\")" in sql
    assert "list_position(__a0, CONCAT(\"level\", '_', \"kind\"))" in sql
    assert "list_element(__a1, CAST(list_position(__a0, CONCAT(\"level\", '_', \"kind\")) AS BIGINT))" in sql
    assert "list_element(__a3, CAST(list_position(__a2, \"level\") AS BIGINT))" in sql


def test_b2_whitelist_rejects_unclosed_if_quote():
    conn = _new_conn()
    _make_table(conn, "t", "a", "out")
    _insert_rows(conn, "t", [{"row_id": "r1", "a": 1.0, "out": 0.0}])
    try:
        duckdb_compute._check_whitelist(
            "IF(@t[a] > 0, 'oops, 1)",
            "t",
            {"row_id", "a", "out"},
        )
    except duckdb_compute.NotSupported as exc:
        assert "引号未闭合" in str(exc)
        return
    raise AssertionError("应拒绝未闭合引号")


def test_b2_execute_formula_supports_index_match_via_duckdb():
    conn = _new_conn()
    _make_table(conn, "src", "key", "value")
    conn.executemany(
        'INSERT INTO "src" (row_id, "key", "value") VALUES (?, ?, ?)',
        [("r1", 1.0, 10.0), ("r2", 2.0, 20.0), ("r3", 3.0, 30.0)],
    )
    _make_table(conn, "t", "k", "out")
    _insert_rows(
        conn,
        "t",
        [
            {"row_id": "r1", "k": 2.0, "out": 0.0},
            {"row_id": "r2", "k": 1.0, "out": 0.0},
            {"row_id": "r3", "k": 3.0, "out": 0.0},
        ],
    )
    register_formula(conn, "t", "out", "INDEX(@@src[value], MATCH(@t[k], @@src[key], 0))", defer=True)
    _set_perf(conn, use_duckdb_compute=True)

    out = execute_formula_on_column(conn, "t", "out")

    assert out["ok"] is True
    assert out["engine"] == "duckdb"
    assert dict(conn.execute('SELECT row_id, out FROM t ORDER BY row_id').fetchall()) == {
        "r1": 20.0,
        "r2": 10.0,
        "r3": 30.0,
    }


def test_b2_execute_formula_orders_suffix_numeric_row_ids():
    conn = _new_conn()
    _make_table(conn, "src", "value")
    conn.executemany(
        'INSERT INTO "src" (row_id, "value") VALUES (?, ?)',
        [("r10", 100.0), ("r2", 20.0), ("r1", 10.0)],
    )
    _make_table(conn, "t", "idx", "out")
    _insert_rows(
        conn,
        "t",
        [
            {"row_id": "row_1", "idx": 1.0, "out": 0.0},
            {"row_id": "row_2", "idx": 2.0, "out": 0.0},
            {"row_id": "row_3", "idx": 3.0, "out": 0.0},
        ],
    )
    register_formula(conn, "t", "out", "INDEX(@@src[value], @t[idx])", defer=True)
    _set_perf(conn, use_duckdb_compute=True)

    out = execute_formula_on_column(conn, "t", "out")

    assert out["ok"] is True
    assert out["engine"] == "duckdb"
    assert dict(conn.execute('SELECT row_id, out FROM t ORDER BY row_id').fetchall()) == {
        "row_1": 10.0,
        "row_2": 20.0,
        "row_3": 100.0,
    }


def test_b2_execute_formula_supports_if_and_minmax_via_duckdb():
    conn = _new_conn()
    _make_table(conn, "t", "a", "out")
    _insert_rows(
        conn,
        "t",
        [
            {"row_id": "r1", "a": 0.0, "out": 0.0},
            {"row_id": "r2", "a": 4.0, "out": 0.0},
            {"row_id": "r3", "a": 7.0, "out": 0.0},
        ],
    )
    register_formula(conn, "t", "out", "IF(@t[a] > 3, MIN(@t[a], 5), MAX(@t[a], 1))", defer=True)
    _set_perf(conn, use_duckdb_compute=True)

    out = execute_formula_on_column(conn, "t", "out")

    assert out["ok"] is True
    assert out["engine"] == "duckdb"
    assert dict(conn.execute('SELECT row_id, out FROM t ORDER BY row_id').fetchall()) == {
        "r1": 1.0,
        "r2": 4.0,
        "r3": 5.0,
    }


def test_b3_execute_formula_supports_vlookup_concat_via_duckdb():
    conn = _new_conn()
    _make_table(conn, "src", "value")
    conn.executemany(
        'INSERT INTO "src" (row_id, "value") VALUES (?, ?)',
        [("1_atk", 10.0), ("1_def", 20.0), ("2_atk", 30.0)],
    )
    conn.execute('CREATE TABLE "t" (row_id TEXT PRIMARY KEY, "level" INTEGER, "kind" TEXT, "out" REAL)')
    conn.execute(
        "INSERT INTO _table_registry (table_name, layer) VALUES (?, 'dynamic')",
        ("t",),
    )
    conn.executemany(
        'INSERT INTO "t" (row_id, "level", "kind", "out") VALUES (?, ?, ?, ?)',
        [("r1", 1, "atk", 0.0), ("r2", 1, "def", 0.0), ("r3", 2, "atk", 0.0)],
    )
    conn.commit()
    register_formula(conn, "t", "out", "VLOOKUP(CONCAT(@t[level], '_', @t[kind]), @@src[row_id], @@src[value], 0)", defer=True)
    _set_perf(conn, use_duckdb_compute=True)

    out = execute_formula_on_column(conn, "t", "out")

    assert out["ok"] is True
    assert out["engine"] == "duckdb"
    assert dict(conn.execute('SELECT row_id, out FROM t ORDER BY row_id').fetchall()) == {
        "r1": 10.0,
        "r2": 20.0,
        "r3": 30.0,
    }


def test_b3_execute_formula_supports_xlookup_via_duckdb():
    conn = _new_conn()
    _make_table(conn, "src", "key", "value")
    conn.executemany(
        'INSERT INTO "src" (row_id, "key", "value") VALUES (?, ?, ?)',
        [("r1", 1.0, 10.0), ("r2", 2.0, 20.0), ("r3", 3.0, 30.0)],
    )
    _make_table(conn, "t", "k", "out")
    _insert_rows(
        conn,
        "t",
        [
            {"row_id": "r1", "k": 2.0, "out": 0.0},
            {"row_id": "r2", "k": 1.0, "out": 0.0},
            {"row_id": "r3", "k": 3.0, "out": 0.0},
        ],
    )
    register_formula(conn, "t", "out", "XLOOKUP(@t[k], @@src[key], @@src[value], 0)", defer=True)
    _set_perf(conn, use_duckdb_compute=True)

    out = execute_formula_on_column(conn, "t", "out")

    assert out["ok"] is True
    assert out["engine"] == "duckdb"
    assert dict(conn.execute('SELECT row_id, out FROM t ORDER BY row_id').fetchall()) == {
        "r1": 20.0,
        "r2": 10.0,
        "r3": 30.0,
    }


def test_b3_execute_text_concat_formula_via_duckdb():
    conn = _new_conn()
    conn.execute('CREATE TABLE "t" (row_id TEXT PRIMARY KEY, "prefix" TEXT, "kind" TEXT, "note" TEXT)')
    conn.execute(
        "INSERT INTO _table_registry (table_name, layer) VALUES (?, 'dynamic')",
        ("t",),
    )
    conn.executemany(
        'INSERT INTO "t" (row_id, "prefix", "kind", "note") VALUES (?, ?, ?, ?)',
        [("r1", "hero", "atk", ""), ("r2", "monster", "def", "")],
    )
    conn.commit()
    register_formula(conn, "t", "note", "CONCAT(@t[prefix], '_', @t[kind])", defer=True)
    _set_perf(conn, use_duckdb_compute=True)

    out = execute_formula_on_column(conn, "t", "note")

    assert out["ok"] is True
    assert out["engine"] == "duckdb"
    assert dict(conn.execute('SELECT row_id, note FROM t ORDER BY row_id').fetchall()) == {
        "r1": "hero_atk",
        "r2": "monster_def",
    }


def test_b4_execute_formula_supports_cross_table_refs_via_duckdb():
    conn = _new_conn()
    _make_table(conn, "src", "level", "value")
    _insert_rows(
        conn,
        "src",
        [
            {"row_id": "s1", "level": 1.0, "value": 10.0},
            {"row_id": "s2", "level": 2.0, "value": 20.0},
            {"row_id": "s3", "level": 3.0, "value": 30.0},
        ],
    )
    _make_table(conn, "t", "level", "base", "out")
    _insert_rows(
        conn,
        "t",
        [
            {"row_id": "t1", "level": 1.0, "base": 1.0, "out": 0.0},
            {"row_id": "t2", "level": 2.0, "base": 2.0, "out": 0.0},
            {"row_id": "t3", "level": 3.0, "base": 3.0, "out": 0.0},
        ],
    )
    register_formula(conn, "t", "out", "@t[base] + @src[value]", defer=True)
    _set_perf(conn, use_duckdb_compute=True)

    out = execute_formula_on_column(conn, "t", "out")

    assert out["ok"] is True
    assert out["engine"] == "duckdb"
    assert dict(conn.execute('SELECT row_id, out FROM t ORDER BY row_id').fetchall()) == {
        "t1": 11.0,
        "t2": 22.0,
        "t3": 33.0,
    }


def test_b4_execute_formula_supports_cross_table_refs_with_3d_dim_alignment_via_duckdb():
    conn = _new_conn()
    create_3d_table(
        conn,
        table_name="src_3d",
        display_name="来源3D",
        dim1={
            "col_name": "tier",
            "display_name": "阶",
            "keys": [{"key": "1", "display_name": "1"}, {"key": "2", "display_name": "2"}],
        },
        dim2={
            "col_name": "kind",
            "display_name": "类",
            "keys": [{"key": "atk", "display_name": "攻"}, {"key": "def", "display_name": "防"}],
        },
        cols=[{"key": "value", "display_name": "值"}],
    )
    conn.executemany(
        'UPDATE "src_3d" SET "value" = ? WHERE row_id = ?',
        [(10.0, "1_atk"), (20.0, "1_def"), (30.0, "2_atk"), (40.0, "2_def")],
    )
    create_3d_table(
        conn,
        table_name="dst_3d",
        display_name="目标3D",
        dim1={
            "col_name": "level_band",
            "display_name": "阶",
            "keys": [{"key": "1", "display_name": "1"}, {"key": "2", "display_name": "2"}],
        },
        dim2={
            "col_name": "damage_kind",
            "display_name": "类",
            "keys": [{"key": "atk", "display_name": "攻"}, {"key": "def", "display_name": "防"}],
        },
        cols=[{"key": "out", "display_name": "输出"}],
    )
    conn.commit()
    register_formula(conn, "dst_3d", "out", "@src_3d[value]", defer=True)
    _set_perf(conn, use_duckdb_compute=True)

    out = execute_formula_on_column(conn, "dst_3d", "out")

    assert out["ok"] is True
    assert out["engine"] == "duckdb"
    assert dict(conn.execute('SELECT row_id, out FROM "dst_3d" ORDER BY row_id').fetchall()) == {
        "1_atk": 10.0,
        "1_def": 20.0,
        "2_atk": 30.0,
        "2_def": 40.0,
    }


def test_b6_execute_formula_uses_sqlite_scanner_without_pandas_loads(monkeypatch):
    conn, path = _new_file_conn()
    try:
        _make_table(conn, "src", "key", "value")
        conn.executemany(
            'INSERT INTO "src" (row_id, "key", "value") VALUES (?, ?, ?)',
            [("r1", 1.0, 10.0), ("r2", 2.0, 20.0), ("r3", 3.0, 30.0)],
        )
        _make_table(conn, "t", "k", "out")
        _insert_rows(
            conn,
            "t",
            [
                {"row_id": "r1", "k": 2.0, "out": 0.0},
                {"row_id": "r2", "k": 1.0, "out": 0.0},
                {"row_id": "r3", "k": 3.0, "out": 0.0},
            ],
        )
        register_formula(conn, "t", "out", "INDEX(@@src[value], MATCH(@t[k], @@src[key], 0))", defer=True)
        _set_perf(conn, use_duckdb_compute=True, use_duckdb_sqlite_scanner=True)

        def _fail_read_sql_query(*args, **kwargs):
            raise AssertionError("DuckDB SQLite scanner path should not call pandas.read_sql_query")

        monkeypatch.setattr(formula_exec.pd, "read_sql_query", _fail_read_sql_query)

        out = execute_formula_on_column(conn, "t", "out")

        assert out["ok"] is True
        assert out["engine"] == "duckdb"
        assert dict(conn.execute('SELECT row_id, out FROM t ORDER BY row_id').fetchall()) == {
            "r1": 20.0,
            "r2": 10.0,
            "r3": 30.0,
        }
    finally:
        conn.close()
        os.remove(path)


def test_b6_dag_reuses_shared_duckdb_sqlite_session(monkeypatch):
    conn, path = _new_file_conn()
    try:
        _make_table(conn, "base", "x")
        _make_table(conn, "mid", "y")
        _make_table(conn, "top", "z")
        _insert_rows(conn, "base", [{"row_id": "r1", "x": 0.0}])
        _insert_rows(conn, "mid", [{"row_id": "r1", "y": 0.0}])
        _insert_rows(conn, "top", [{"row_id": "r1", "z": 0.0}])
        register_formula(conn, "base", "x", "const_value(5)", defer=True)
        register_formula(conn, "mid", "y", "@base[x] + 1", defer=True)
        register_formula(conn, "top", "z", "@mid[y] + 1", defer=True)
        _set_perf(conn, use_duckdb_compute=True)

        original_compute = duckdb_compute.compute_column_via_duckdb
        session_ids = []

        def _wrapped_compute(*args, **kwargs):
            session_ids.append(id(kwargs.get("duckdb_conn")))
            return original_compute(*args, **kwargs)

        monkeypatch.setattr(duckdb_compute, "compute_column_via_duckdb", _wrapped_compute)

        out = recalculate_downstream_dag(conn, [("base", "x")], execute_seeds=True)

        assert out["errors"] == []
        assert len(session_ids) == 3
        assert len(set(session_ids)) == 1
        assert session_ids[0] != id(None)
        assert conn.execute("SELECT x FROM base WHERE row_id='r1'").fetchone()[0] == 5.0
        assert conn.execute("SELECT y FROM mid WHERE row_id='r1'").fetchone()[0] == 6.0
        assert conn.execute("SELECT z FROM top WHERE row_id='r1'").fetchone()[0] == 7.0
    finally:
        conn.close()
        os.remove(path)


def test_b6_dag_reuses_projection_cache_for_duckdb_nodes(monkeypatch):
    conn = _new_conn()
    _make_table(conn, "base", "k", "x")
    _make_table(conn, "ref", "k", "v")
    _make_table(conn, "mid", "k", "y")
    _make_table(conn, "top", "k", "z")
    _insert_rows(
        conn,
        "base",
        [{"row_id": f"r{i}", "k": float(i), "x": float(i)} for i in range(1, 4)],
    )
    _insert_rows(
        conn,
        "ref",
        [{"row_id": f"r{i}", "k": float(i), "v": float(i * 10)} for i in range(1, 4)],
    )
    _insert_rows(
        conn,
        "mid",
        [{"row_id": f"r{i}", "k": float(i), "y": 0.0} for i in range(1, 4)],
    )
    _insert_rows(
        conn,
        "top",
        [{"row_id": f"r{i}", "k": float(i), "z": 0.0} for i in range(1, 4)],
    )
    register_formula(conn, "mid", "y", "@base[x] + @ref[v]", defer=True)
    register_formula(conn, "top", "z", "@base[x] + @ref[v] * 2", defer=True)
    _set_perf(conn, use_duckdb_compute=True)

    ref_sqls = []
    original_read_sql_query = formula_exec.pd.read_sql_query

    def _counting_read_sql_query(sql, *args, **kwargs):
        if 'FROM "ref"' in sql:
            ref_sqls.append(sql)
        return original_read_sql_query(sql, *args, **kwargs)

    monkeypatch.setattr(formula_exec.pd, "read_sql_query", _counting_read_sql_query)

    out = recalculate_downstream_dag(conn, [("base", "x")])

    assert out["errors"] == []
    assert len(ref_sqls) == 1
    assert 'SELECT "row_id", "k", "v" FROM "ref"' in ref_sqls[0]
    assert dict(conn.execute('SELECT row_id, y FROM mid ORDER BY row_id').fetchall()) == {
        "r1": 11.0,
        "r2": 22.0,
        "r3": 33.0,
    }
    assert dict(conn.execute('SELECT row_id, z FROM top ORDER BY row_id').fetchall()) == {
        "r1": 21.0,
        "r2": 42.0,
        "r3": 63.0,
    }


def test_a5_dag_reuses_shared_table_cache_for_pandas_nodes(monkeypatch):
    conn = _new_conn()
    _make_table(conn, "base", "k", "x")
    _make_table(conn, "ref", "k", "v")
    _make_table(conn, "mid", "k", "y")
    _make_table(conn, "top", "k", "z")
    _insert_rows(
        conn,
        "base",
        [{"row_id": f"r{i}", "k": float(i), "x": float(i)} for i in range(1, 4)],
    )
    _insert_rows(
        conn,
        "ref",
        [{"row_id": f"r{i}", "k": float(i), "v": float(i * 10)} for i in range(1, 4)],
    )
    _insert_rows(
        conn,
        "mid",
        [{"row_id": f"r{i}", "k": float(i), "y": 0.0} for i in range(1, 4)],
    )
    _insert_rows(
        conn,
        "top",
        [{"row_id": f"r{i}", "k": float(i), "z": 0.0} for i in range(1, 4)],
    )
    register_formula(conn, "mid", "y", "@base[x] + @ref[v]", defer=True)
    register_formula(conn, "top", "z", "@base[x] + @ref[v] * 2", defer=True)

    read_sql_calls = 0
    original_read_sql_query = formula_exec.pd.read_sql_query

    def _counting_read_sql_query(sql, *args, **kwargs):
        nonlocal read_sql_calls
        if 'FROM "ref"' in sql:
            read_sql_calls += 1
        return original_read_sql_query(sql, *args, **kwargs)

    monkeypatch.setattr(formula_exec.pd, "read_sql_query", _counting_read_sql_query)

    out = recalculate_downstream_dag(conn, [("base", "x")])

    assert out["errors"] == []
    assert read_sql_calls == 1
    assert dict(conn.execute('SELECT row_id, y FROM mid ORDER BY row_id').fetchall()) == {
        "r1": 11.0,
        "r2": 22.0,
        "r3": 33.0,
    }
    assert dict(conn.execute('SELECT row_id, z FROM top ORDER BY row_id').fetchall()) == {
        "r1": 21.0,
        "r2": 42.0,
        "r3": 63.0,
    }


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


def test_a4_vlookup_exact_uses_lookup_cache(monkeypatch):
    conn = _new_conn()
    _make_table(conn, "lk", "k", "value")
    _insert_rows(
        conn,
        "lk",
        [{"row_id": f"r{i}", "k": float(i), "value": float(i * 10)} for i in range(1, 11)],
    )
    _make_table(conn, "t", "k", "out")
    _insert_rows(
        conn,
        "t",
        [{"row_id": f"r{i}", "k": float((i % 10) + 1), "out": 0.0} for i in range(100)],
    )
    register_formula(conn, "t", "out", "VLOOKUP(@t[k], @@lk[k], @@lk[value], 0)", defer=True)

    compare_calls = 0
    original_values_equal = formula_engine._values_equal

    def _counting_values_equal(a, b):
        nonlocal compare_calls
        compare_calls += 1
        return original_values_equal(a, b)

    monkeypatch.setattr("app.services.formula_engine._values_equal", _counting_values_equal)

    execute_formula_on_column(conn, "t", "out")
    cached_calls = compare_calls

    compare_calls = 0
    _set_perf(conn, use_batch_lookup=False)
    conn.execute('UPDATE "t" SET "out" = 0')
    conn.commit()
    execute_formula_on_column(conn, "t", "out")
    uncached_calls = compare_calls

    assert cached_calls < uncached_calls
    assert dict(conn.execute('SELECT row_id, out FROM t WHERE row_id IN ("r0", "r1", "r9")').fetchall()) == {
        "r0": 10.0,
        "r1": 20.0,
        "r9": 100.0,
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
