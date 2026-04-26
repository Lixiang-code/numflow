"""表名/列名：
- `assert_table_or_column`：宽松版（兼容老库的中文名），用于"读取"路径。
- `assert_english_ident`：严格英文 snake_case，**新建**表/列必须用此版本。
"""

from __future__ import annotations

import re

# 兼容老库（含中文）的宽松正则；仅用于读路径
_IDENT_RE = re.compile(r"^[\u4e00-\u9fffA-Za-z_][\u4e00-\u9fffA-Za-z0-9_]{0,62}$")

# 严格 ASCII snake_case：首字小写字母，后续 a-z/0-9/_，长度 1..63
_ENGLISH_IDENT_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def is_valid_table_or_column_name(name: str) -> bool:
    if not name or name.startswith("_") or not _IDENT_RE.match(name):
        return False
    return True


def assert_table_or_column(name: str, *, field: str = "表名或列名") -> str:
    s = (name or "").strip()
    if not is_valid_table_or_column_name(s):
        raise ValueError(f"非法{field}：{name!r}")
    return s


def is_english_ident(name: str) -> bool:
    return bool(name) and not name.startswith("_") and bool(_ENGLISH_IDENT_RE.match(name))


def assert_english_ident(name: str, *, field: str = "标识符") -> str:
    """新建表/列名严格校验：a-z/0-9/_，首字必须小写字母。
    不允许中文/大写/连字符；中文一律走 display_name。
    """
    s = (name or "").strip()
    if not is_english_ident(s):
        raise ValueError(
            f"非法{field}：{name!r}。新建表/列名必须为英文 snake_case "
            f"（仅 a-z/0-9/_，首字为小写字母）。中文请通过 display_name / 中文展示名传入。"
        )
    return s
