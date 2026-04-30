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


def test_list_directories_empty_result_omits_empty_payload_fields():
    conn = _new_conn()

    result = json.loads(dispatch_tool("list_directories", {}, _project_db(conn)))

    assert result == {"status": "success"}


def test_read_table_empty_rows_are_retained_as_meaningful_result():
    conn = _new_conn()
    create_dynamic_table(
        conn,
        table_name="empty_read_demo",
        display_name="空读取演示",
        columns=[("level", "INTEGER"), ("value", "REAL")],
    )
    conn.commit()

    result = json.loads(
        dispatch_tool(
            "read_table",
            {"table_name": "empty_read_demo"},
            _project_db(conn),
        )
    )

    assert result == {"status": "success", "data": {"rows": []}}


# ─── const_list / glossary_list 紧凑格式测试 ────────────────────────────────


def _register_n_consts(conn, n: int) -> None:
    """在内存库中注册 n 个常数，用于尺寸测试。"""
    import time
    now = "2026-01-01T00:00:00Z"
    for i in range(n):
        conn.execute(
            """INSERT OR REPLACE INTO _constants
               (name_en, name_zh, value_json, brief, scope_table, tags, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                f"const_{i:04d}",
                f"常数{i}",
                str(float(i)),
                f"这是第{i}号常数的说明文字，用于测试响应大小",
                None,
                '["test_tag"]',
                now,
                now,
            ),
        )
    conn.commit()


def _register_n_terms(conn, n: int) -> None:
    """在内存库中注册 n 个术语，用于尺寸测试。"""
    now = "2026-01-01T00:00:00Z"
    for i in range(n):
        conn.execute(
            """INSERT OR REPLACE INTO _glossary
               (term_en, term_zh, kind, brief, scope_table, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?)""",
            (
                f"term_{i:04d}",
                f"术语{i}",
                "stat",
                f"这是第{i}号术语的说明，用于测试响应大小",
                None,
                now,
                now,
            ),
        )
    conn.commit()


def test_const_list_returns_cols_rows_format():
    """const_list 应返回 cols+rows 行列格式，不再返回 items 对象列表。"""
    conn = _new_conn()
    _register_n_consts(conn, 5)
    p = _project_db(conn)

    result = json.loads(dispatch_tool("const_list", {}, p))
    data = result["data"]

    assert "cols" in data, "应包含 cols 字段"
    assert "rows" in data, "应包含 rows 字段"
    assert "items" not in data, "不应再出现旧版 items 字段"
    assert data["cols"] == ["name_en", "name_zh", "value", "brief", "scope_table", "tags"]
    assert data["total"] == 5
    assert len(data["rows"]) == 5
    # 验证行结构：每行应有 6 个元素
    for row in data["rows"]:
        assert len(row) == 6


def test_glossary_list_returns_cols_rows_format():
    """glossary_list 应返回 cols+rows 行列格式，不再返回 items 对象列表。"""
    conn = _new_conn()
    _register_n_terms(conn, 5)
    p = _project_db(conn)

    result = json.loads(dispatch_tool("glossary_list", {}, p))
    data = result["data"]

    assert "cols" in data, "应包含 cols 字段"
    assert "rows" in data, "应包含 rows 字段"
    assert "items" not in data, "不应再出现旧版 items 字段"
    assert data["cols"] == ["term_en", "term_zh", "kind", "brief", "scope_table"]
    assert data["total"] == 5
    assert len(data["rows"]) == 5
    for row in data["rows"]:
        assert len(row) == 5


def test_const_list_compact_format_smaller_than_objects(tmp_path):
    """const_list 紧凑格式的 JSON 尺寸应显著小于旧版对象列表格式。"""
    N = 100
    conn = _new_conn()
    _register_n_consts(conn, N)
    p = _project_db(conn)

    new_json = dispatch_tool("const_list", {}, p)
    new_size = len(new_json.encode("utf-8"))

    # 构造旧版对象格式的等价 JSON 以对比
    data = json.loads(new_json)["data"]
    cols = data["cols"]
    old_items = [dict(zip(cols, row)) for row in data["rows"]]
    old_json = json.dumps({"status": "success", "data": {"items": old_items}}, ensure_ascii=False)
    old_size = len(old_json.encode("utf-8"))

    reduction = (old_size - new_size) / old_size
    assert reduction >= 0.25, (
        f"紧凑格式应比对象列表至少节省 25%，实际节省 {reduction:.1%}（旧={old_size}B 新={new_size}B）"
    )


def test_glossary_list_compact_format_smaller_than_objects():
    """glossary_list 紧凑格式的 JSON 尺寸应显著小于旧版对象列表格式。"""
    N = 100
    conn = _new_conn()
    _register_n_terms(conn, N)
    p = _project_db(conn)

    new_json = dispatch_tool("glossary_list", {}, p)
    new_size = len(new_json.encode("utf-8"))

    data = json.loads(new_json)["data"]
    cols = data["cols"]
    old_items = [dict(zip(cols, row)) for row in data["rows"]]
    old_json = json.dumps({"status": "success", "data": {"items": old_items}}, ensure_ascii=False)
    old_size = len(old_json.encode("utf-8"))

    reduction = (old_size - new_size) / old_size
    assert reduction >= 0.25, (
        f"紧凑格式应比对象列表至少节省 25%，实际节省 {reduction:.1%}（旧={old_size}B 新={new_size}B）"
    )


def test_const_list_tags_filter():
    """const_list tags_filter 应只返回含指定标签的常数。"""
    conn = _new_conn()
    now = "2026-01-01T00:00:00Z"
    conn.execute(
        "INSERT INTO _constants (name_en,name_zh,value_json,brief,scope_table,tags,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)",
        ("combat_atk", "攻击", "100.0", "战斗攻击力", None, '["combat"]', now, now),
    )
    conn.execute(
        "INSERT INTO _constants (name_en,name_zh,value_json,brief,scope_table,tags,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)",
        ("econ_gold", "金币", "500.0", "经济金币量", None, '["economy"]', now, now),
    )
    conn.commit()
    p = _project_db(conn)

    result = json.loads(dispatch_tool("const_list", {"tags_filter": ["combat"]}, p))
    rows = result["data"]["rows"]
    assert len(rows) == 1
    assert rows[0][0] == "combat_atk"


def test_const_list_pagination():
    """const_list 支持 limit/offset 分页，has_more 标记正确。"""
    N = 10
    conn = _new_conn()
    _register_n_consts(conn, N)
    p = _project_db(conn)

    page1 = json.loads(dispatch_tool("const_list", {"limit": 4, "offset": 0}, p))
    d1 = page1["data"]
    assert len(d1["rows"]) == 4
    assert d1.get("has_more") is True
    assert d1.get("next_offset") == 4

    page3 = json.loads(dispatch_tool("const_list", {"limit": 4, "offset": 8}, p))
    d3 = page3["data"]
    assert len(d3["rows"]) == 2
    assert "has_more" not in d3  # 已到末尾


def test_glossary_list_kind_filter():
    """glossary_list kind_filter 应只返回指定 kind 的术语。"""
    conn = _new_conn()
    now = "2026-01-01T00:00:00Z"
    conn.execute(
        "INSERT INTO _glossary (term_en,term_zh,kind,brief,scope_table,created_at,updated_at) VALUES (?,?,?,?,?,?,?)",
        ("hp", "生命值", "stat", "角色生命", None, now, now),
    )
    conn.execute(
        "INSERT INTO _glossary (term_en,term_zh,kind,brief,scope_table,created_at,updated_at) VALUES (?,?,?,?,?,?,?)",
        ("equip", "装备", "noun", "装备道具", None, now, now),
    )
    conn.commit()
    p = _project_db(conn)

    result = json.loads(dispatch_tool("glossary_list", {"kind_filter": "stat"}, p))
    rows = result["data"]["rows"]
    assert len(rows) == 1
    assert rows[0][0] == "hp"


def test_const_list_no_timestamps_in_output():
    """const_list 结果中不应包含 created_at / updated_at 字段。"""
    conn = _new_conn()
    _register_n_consts(conn, 3)
    p = _project_db(conn)

    raw = dispatch_tool("const_list", {}, p)
    assert "created_at" not in raw
    assert "updated_at" not in raw


def test_glossary_list_no_timestamps_in_output():
    """glossary_list 结果中不应包含 created_at / updated_at 字段。"""
    conn = _new_conn()
    _register_n_terms(conn, 3)
    p = _project_db(conn)

    raw = dispatch_tool("glossary_list", {}, p)
    assert "created_at" not in raw
    assert "updated_at" not in raw



# ─── tags_filter + limit/offset 组合回归测试 ────────────────────────────────


def _register_interleaved_consts(conn, total: int = 20) -> None:
    """注册 total 个常数，奇偶交替打 'combat' 和 'economy' 标签。"""
    now = "2026-01-01T00:00:00Z"
    for i in range(total):
        tag = "combat" if i % 2 == 0 else "economy"
        conn.execute(
            "INSERT OR REPLACE INTO _constants"
            " (name_en, name_zh, value_json, brief, scope_table, tags, created_at, updated_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (f"const_{i:02d}", f"常数{i}", str(float(i)), f"说明{i}", None, f'["{tag}"]', now, now),
        )
    conn.commit()


def test_tags_filter_total_reflects_full_filtered_count():
    """tags_filter 时 total 应为过滤后的全部条数，而非 SQL 分页截断后的条数。"""
    conn = _new_conn()
    _register_interleaved_consts(conn, 20)
    p = _project_db(conn)

    result = json.loads(dispatch_tool("const_list", {"tags_filter": ["combat"], "limit": 5, "offset": 0}, p))
    d = result["data"]
    # 20 条中 10 条是 combat（索引 0,2,4...18）
    assert d["total"] == 10, f"total 应为 10，实际={d['total']}"
    assert len(d["rows"]) == 5, f"返回行数应为 5，实际={len(d['rows'])}"


def test_tags_filter_has_more_appears_when_paged():
    """tags_filter + limit 不足以覆盖全部过滤结果时，应出现 has_more 和 next_offset。"""
    conn = _new_conn()
    _register_interleaved_consts(conn, 20)
    p = _project_db(conn)

    result = json.loads(dispatch_tool("const_list", {"tags_filter": ["combat"], "limit": 4, "offset": 0}, p))
    d = result["data"]
    assert d.get("has_more") is True, "应出现 has_more"
    assert d.get("next_offset") == 4, f"next_offset 应为 4，实际={d.get('next_offset')}"


def test_tags_filter_offset_pages_correctly():
    """tags_filter + offset 应在过滤后集合上正确跳过前 N 条。"""
    conn = _new_conn()
    _register_interleaved_consts(conn, 20)
    p = _project_db(conn)

    # 无偏移：前 3 个 combat 常数
    r0 = json.loads(dispatch_tool("const_list", {"tags_filter": ["combat"], "limit": 3, "offset": 0}, p))
    names_0 = [row[0] for row in r0["data"]["rows"]]

    # 偏移 3：应得到第 4~6 个 combat 常数（与前面不重叠）
    r1 = json.loads(dispatch_tool("const_list", {"tags_filter": ["combat"], "limit": 3, "offset": 3}, p))
    names_1 = [row[0] for row in r1["data"]["rows"]]

    assert names_0 != names_1, "两页应返回不同常数"
    assert not set(names_0) & set(names_1), "两页不应有重叠"


def test_tags_filter_without_limit_returns_all_matching():
    """tags_filter 不带 limit 时，应返回全部过滤后的条目。"""
    conn = _new_conn()
    _register_interleaved_consts(conn, 20)
    p = _project_db(conn)

    result = json.loads(dispatch_tool("const_list", {"tags_filter": ["combat"]}, p))
    d = result["data"]
    assert d["total"] == 10
    assert len(d["rows"]) == 10
    assert d.get("has_more") is None
