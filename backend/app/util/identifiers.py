"""表名/列名：允许中文、英文、数字、下划线；首字不可为数字、不可为 _ 开头。"""

from __future__ import annotations

import re
from typing import Optional

# 与 SQLite 引号标识符搭配使用；CJK 首字 + 后续混合
_IDENT_RE = re.compile(r"^[\u4e00-\u9fffA-Za-z_][\u4e00-\u9fffA-Za-z0-9_]{0,62}$")


def is_valid_table_or_column_name(name: str) -> bool:
    if not name or name.startswith("_") or not _IDENT_RE.match(name):
        return False
    return True


def assert_table_or_column(name: str, *, field: str = "表名或列名") -> str:
    s = (name or "").strip()
    if not is_valid_table_or_column_name(s):
        raise ValueError(f"非法{field}：{name!r}")
    return s
