"""提示词回潮检测：防止中文表名/魔法数/缺失术语登记钩子重新潜入。"""
from __future__ import annotations

import re
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from app.services.prompt_router import DEFAULT_STEP_PROMPTS, _NAMING_HEADER


FORBIDDEN_CN_TABLE_TOKENS = [
    "_属性分配", "_属性表", "_落地", "_养成分配", "_养成量化",
    "基础属性_标准等级",
    "@T[等级]",
]

FORBIDDEN_MAGIC = [
    "坐骑 30", "坐骑30", "30 默认", "30级封顶",
    "开放等级=30", "开放等级 30",
]


def assert_eq(cond, msg):
    if not cond:
        raise AssertionError(msg)


def test_naming_header_has_required_directives():
    for kw in ["snake_case", "display_name", "glossary_register",
               "const_register", "system_level_caps"]:
        assert_eq(kw in _NAMING_HEADER, f"_NAMING_HEADER missing keyword: {kw}")


def test_no_chinese_table_names_in_prompts():
    for step, prompt in DEFAULT_STEP_PROMPTS.items():
        for tok in FORBIDDEN_CN_TABLE_TOKENS:
            assert_eq(tok not in prompt, f"step={step} 残留禁词 {tok!r}")


def test_no_magic_constants_in_prompts():
    for step, prompt in DEFAULT_STEP_PROMPTS.items():
        for tok in FORBIDDEN_MAGIC:
            assert_eq(tok not in prompt, f"step={step} 残留魔法数 {tok!r}")


def test_creation_steps_mention_glossary():
    for step in ("base_attribute_framework",
                 "gameplay_landing_tables.equip",
                 "gameplay_landing_tables.mount",
                 "gameplay_landing_tables.wing",
                 "gameplay_landing_tables.gem"):
        if step in DEFAULT_STEP_PROMPTS:
            p = DEFAULT_STEP_PROMPTS[step]
            assert_eq("glossary" in p, f"step={step} 必须引导 glossary_register")


def test_mount_step_uses_system_level_caps():
    for key in ("gameplay_landing_tables.mount", "gameplay_landing_tables.wing"):
        if key in DEFAULT_STEP_PROMPTS:
            p = DEFAULT_STEP_PROMPTS[key]
            assert_eq("system_level_caps" in p or "max_level" in p,
                      f"step={key} 必须引用 system_level_caps 或 max_level")
            assert_eq(not re.search(r"(?:^|[^0-9])30\s*[级]", p),
                      f"step={key} 出现 '30级' 硬编码")


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL {t.__name__}: {e}")
    print(f"\n{len(tests)-failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
