"""文档 02 子集：可机读默认规则（占位结构，可逐步扩充）。"""

from __future__ import annotations

from typing import Any, Dict

DEFAULT_RULES_02: Dict[str, Any] = {
    "version": 3,
    "source_doc": "(已弃用)02-系统与子系统默认细则.md，请以SKILL为准"
}


def get_default_rules_payload() -> Dict[str, Any]:
    return DEFAULT_RULES_02
