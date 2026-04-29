from __future__ import annotations

import pathlib
import sqlite3
import sys
import tempfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from app.db.project_migrations import ensure_project_migrations
from app.db.project_schema import init_project_db
from app.services import skill_library
from app.services.prompt_router import route_prompt


def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_project_db(conn, seed_readme=False)
    ensure_project_migrations(conn)
    return conn


def test_default_skills_seeded_and_listed():
    conn = _new_conn()
    items = skill_library.list_skills(conn, include_disabled=True, include_modules=True)
    slugs = {item["slug"] for item in items}
    assert "landing-common" in slugs
    assert "gem-landing" in slugs
    assert "mount-landing" in slugs
    gem = next(item for item in items if item["slug"] == "gem-landing")
    assert gem["enabled_module_count"] >= 4
    assert any(m["title"] == "建议产出表" for m in gem["modules"])


def test_render_skill_file_persists_markdown():
    conn = _new_conn()
    with tempfile.TemporaryDirectory() as tmp:
        original = skill_library.get_project_dir
        try:
            skill_library.get_project_dir = lambda slug: pathlib.Path(tmp) / slug  # type: ignore[assignment]
            rendered = skill_library.render_skill_file(conn, "gem-landing", project_slug="demo")
        finally:
            skill_library.get_project_dir = original  # type: ignore[assignment]
    assert rendered is not None
    assert rendered["generated_file_path"] == "skills/gem-landing.md"
    assert "宝石制作说明" in rendered["generated_content"]
    assert "enabled_module_keys:" in rendered["generated_content"]


def test_route_prompt_prefers_skill_library_for_gameplay_steps():
    conn = _new_conn()
    routed = route_prompt(
        "gameplay_landing_tables.gem",
        "请制作宝石玩法落地表",
        "{}",
        conn=conn,
    )
    assert routed["rationale"] == "skill_library_default_exposure"
    assert "SKILL 默认暴露" in routed["prompt"]
    assert any(item["slug"] == "gem-landing" for item in routed["skills"])


def test_get_skill_detail_records_usage():
    conn = _new_conn()
    before = skill_library.get_skill_detail(conn, "skill-landing")
    assert before is not None
    base_count = before["usage_count"]
    after = skill_library.get_skill_detail(conn, "skill-landing", record_usage_event="tool_detail")
    assert after is not None
    assert after["usage_count"] == base_count + 1


def test_upsert_skill_handles_chinese_titles():
    conn = _new_conn()
    with tempfile.TemporaryDirectory() as tmp:
        original = skill_library.get_project_dir
        try:
            skill_library.get_project_dir = lambda slug: pathlib.Path(tmp) / slug  # type: ignore[assignment]
            created = skill_library.upsert_skill(
                conn,
                project_slug="demo",
                skill_id=None,
                payload={
                    "title": "宝石高级扩展",
                    "step_id": "gameplay_landing_tables.gem",
                    "summary": "测试中文标题",
                    "description": "测试中文标题时的 slug/module_key 生成",
                    "default_exposed": False,
                    "enabled": True,
                    "modules": [
                        {"title": "核心目标", "content": "A", "required": True, "enabled": True},
                        {"title": "核心目标", "content": "B", "required": False, "enabled": False},
                    ],
                },
            )
        finally:
            skill_library.get_project_dir = original  # type: ignore[assignment]
    assert created["slug"].startswith("skill")
    assert len(created["modules"]) == 2
    keys = [m["module_key"] for m in created["modules"]]
    assert keys[0] != keys[1]
