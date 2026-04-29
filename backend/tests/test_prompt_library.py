"""提示词库覆盖测试：系统提示词 / 工具提示词 override 生效。"""

from __future__ import annotations

import pathlib
import sqlite3
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from app.db.project_migrations import ensure_project_migrations
from app.db.project_schema import init_project_db
from app.services.agent_runner import _common_system
from app.services.agent_tools import build_tools_openai, get_tool_prompt_catalog
from app.services.prompt_overrides import build_prompt_editor_item, get_prompt_override, upsert_prompt_override
from app.services.prompt_router import get_default_step_prompt, get_route_system_prompt


def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_project_db(conn, seed_readme=False)
    ensure_project_migrations(conn)
    return conn


def _new_global_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def test_system_prompt_override_applies_to_agent_common():
    conn = _new_conn()
    upsert_prompt_override(
        conn,
        category="system",
        prompt_key="agent_common_maintain",
        payload={
            "title": "维护 Agent",
            "summary": "测试覆盖",
            "description": "测试覆盖",
            "reference_note": "测试覆盖",
            "enabled": True,
            "modules": [
                {
                    "module_key": "body",
                    "title": "完整提示词",
                    "content": "【覆盖版维护 Agent 提示词】",
                    "required": True,
                    "enabled": True,
                    "sort_order": 1,
                }
            ],
        },
    )
    assert "覆盖版维护 Agent" in _common_system("maintain", conn)


def test_global_system_prompt_override_applies_to_agent_common():
    gconn = _new_global_conn()
    upsert_prompt_override(
        gconn,
        category="system",
        prompt_key="agent_common_maintain",
        payload={
            "title": "维护 Agent（全局）",
            "summary": "测试覆盖",
            "description": "测试覆盖",
            "reference_note": "测试覆盖",
            "enabled": True,
            "modules": [
                {
                    "module_key": "body",
                    "title": "完整提示词",
                    "content": "【全局覆盖版维护 Agent 提示词】",
                    "required": True,
                    "enabled": True,
                    "sort_order": 1,
                }
            ],
        },
    )
    assert "全局覆盖版维护 Agent" in _common_system("maintain", global_conn=gconn)


def test_project_system_prompt_override_beats_global():
    conn = _new_conn()
    gconn = _new_global_conn()
    upsert_prompt_override(
        gconn,
        category="system",
        prompt_key="agent_common_maintain",
        payload={
            "title": "维护 Agent（全局）",
            "summary": "测试覆盖",
            "description": "测试覆盖",
            "reference_note": "测试覆盖",
            "enabled": True,
            "modules": [
                {
                    "module_key": "body",
                    "title": "完整提示词",
                    "content": "【全局覆盖版维护 Agent 提示词】",
                    "required": True,
                    "enabled": True,
                    "sort_order": 1,
                }
            ],
        },
    )
    upsert_prompt_override(
        conn,
        category="system",
        prompt_key="agent_common_maintain",
        payload={
            "title": "维护 Agent（项目）",
            "summary": "测试覆盖",
            "description": "测试覆盖",
            "reference_note": "测试覆盖",
            "enabled": True,
            "modules": [
                {
                    "module_key": "body",
                    "title": "完整提示词",
                    "content": "【项目覆盖版维护 Agent 提示词】",
                    "required": True,
                    "enabled": True,
                    "sort_order": 1,
                }
            ],
        },
    )
    assert "项目覆盖版维护 Agent" in _common_system("maintain", conn, global_conn=gconn)


def test_route_prompt_override_applies():
    conn = _new_conn()
    upsert_prompt_override(
        conn,
        category="system",
        prompt_key="route_step::gameplay_landing_tables.gem",
        payload={
            "title": "宝石步骤",
            "summary": "测试覆盖",
            "description": "测试覆盖",
            "reference_note": "测试覆盖",
            "enabled": True,
            "modules": [
                {
                    "module_key": "body",
                    "title": "完整提示词",
                    "content": "【宝石步骤覆盖提示词】",
                    "required": True,
                    "enabled": True,
                    "sort_order": 1,
                }
            ],
        },
    )
    assert get_default_step_prompt("gameplay_landing_tables.gem", conn) == "【宝石步骤覆盖提示词】"


def test_global_route_prompt_override_applies():
    gconn = _new_global_conn()
    upsert_prompt_override(
        gconn,
        category="system",
        prompt_key="route_step::gameplay_planning",
        payload={
            "title": "玩法规划（全局）",
            "summary": "测试覆盖",
            "description": "测试覆盖",
            "reference_note": "测试覆盖",
            "enabled": True,
            "modules": [
                {
                    "module_key": "body",
                    "title": "完整提示词",
                    "content": "【全局玩法规划覆盖提示词】",
                    "required": True,
                    "enabled": True,
                    "sort_order": 1,
                }
            ],
        },
    )
    assert get_default_step_prompt("gameplay_planning", global_conn=gconn) == "【全局玩法规划覆盖提示词】"


def test_route_system_override_applies():
    conn = _new_conn()
    upsert_prompt_override(
        conn,
        category="system",
        prompt_key="router_system",
        payload={
            "title": "路由器",
            "summary": "测试覆盖",
            "description": "测试覆盖",
            "reference_note": "测试覆盖",
            "enabled": True,
            "modules": [
                {
                    "module_key": "body",
                    "title": "完整提示词",
                    "content": "【覆盖版路由 system】",
                    "required": True,
                    "enabled": True,
                    "sort_order": 1,
                }
            ],
        },
    )
    assert get_route_system_prompt(conn) == "【覆盖版路由 system】"


def test_tool_prompt_override_applies_to_schema_descriptions():
    conn = _new_conn()
    base = get_prompt_override(conn, category="tool", prompt_key="read_table")
    assert base is None
    upsert_prompt_override(
        conn,
        category="tool",
        prompt_key="read_table",
        payload={
            "title": "工具：read_table",
            "summary": "测试覆盖",
            "description": "测试覆盖",
            "reference_note": "测试覆盖",
            "enabled": True,
            "modules": [
                {
                    "module_key": "function.description",
                    "title": "函数说明",
                    "content": "读取表数据（覆盖版）",
                    "required": True,
                    "enabled": True,
                    "sort_order": 1,
                },
                {
                    "module_key": "function.parameters.properties.table_name.description",
                    "title": "表名参数",
                    "content": "目标表名（覆盖版）",
                    "required": True,
                    "enabled": True,
                    "sort_order": 2,
                },
            ],
        },
    )
    tools = build_tools_openai(conn)
    read_table = next(tool for tool in tools if tool["function"]["name"] == "read_table")
    assert read_table["function"]["description"] == "读取表数据（覆盖版）"
    assert read_table["function"]["parameters"]["properties"]["table_name"]["description"] == "目标表名（覆盖版）"


def test_global_tool_prompt_override_applies_to_schema_descriptions():
    gconn = _new_global_conn()
    upsert_prompt_override(
        gconn,
        category="tool",
        prompt_key="read_table",
        payload={
            "title": "工具：read_table",
            "summary": "测试覆盖",
            "description": "测试覆盖",
            "reference_note": "测试覆盖",
            "enabled": True,
            "modules": [
                {
                    "module_key": "function.description",
                    "title": "函数说明",
                    "content": "读取表数据（全局覆盖版）",
                    "required": True,
                    "enabled": True,
                    "sort_order": 1,
                },
                {
                    "module_key": "function.parameters.properties.table_name.description",
                    "title": "表名参数",
                    "content": "目标表名（全局覆盖版）",
                    "required": True,
                    "enabled": True,
                    "sort_order": 2,
                },
            ],
        },
    )
    tools = build_tools_openai(global_conn=gconn)
    read_table = next(tool for tool in tools if tool["function"]["name"] == "read_table")
    assert read_table["function"]["description"] == "读取表数据（全局覆盖版）"
    assert read_table["function"]["parameters"]["properties"]["table_name"]["description"] == "目标表名（全局覆盖版）"


def test_tool_prompt_editor_item_marks_orphan_modules():
    conn = _new_conn()
    upsert_prompt_override(
        conn,
        category="tool",
        prompt_key="read_table",
        payload={
            "title": "工具：read_table",
            "summary": "测试 orphan",
            "description": "测试 orphan",
            "reference_note": "测试 orphan",
            "enabled": True,
            "modules": [
                {
                    "module_key": "function.description",
                    "title": "函数说明",
                    "content": "读取表数据（覆盖版）",
                    "required": True,
                    "enabled": True,
                    "sort_order": 1,
                },
                {
                    "module_key": "function.parameters.properties.ghost_arg.description",
                    "title": "幽灵参数",
                    "content": "这个字段已经不存在",
                    "required": False,
                    "enabled": True,
                    "sort_order": 2,
                },
            ],
        },
    )
    default_item = next(item for item in get_tool_prompt_catalog() if item["prompt_key"] == "read_table")
    editor_item = build_prompt_editor_item(
        default_item,
        get_prompt_override(conn, category="tool", prompt_key="read_table"),
        category="tool",
    )

    assert editor_item["default_modules"]
    assert "function.parameters.properties.ghost_arg.description" in editor_item["diagnostics"]["orphan_module_keys"]
