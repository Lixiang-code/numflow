#!/usr/bin/env python3
"""
删除 E2E/实机测试产生的无用项目：server.sqlite 中 projects 行 + data/projects/{slug} 目录。

默认匹配：slug 以 03e2e- / e2e- 开头，或 name 以 03E2E 开头、真实E2E 等。
可设环境变量 NUMFLOW_CLEANUP_DRY=1 只打印不删。

用法：在仓库根或 backend 父级执行
  python3 scripts/cleanup_test_projects.py
"""
from __future__ import annotations

import os
import re
import shutil
import sqlite3
import sys
from pathlib import Path

DRY = os.environ.get("NUMFLOW_CLEANUP_DRY", "").lower() in ("1", "true", "yes")

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.environ.get("NUMFLOW_DATA_DIR", str(ROOT / "data"))).resolve()
SERVER_DB = DATA_DIR / "server.sqlite"
PROJECTS_DIR = DATA_DIR / "projects"

# 勿删模板
PROTECT = {"template"}


def is_test_row(slug: str, name: str) -> bool:
    s, n = (slug or "").lower(), name or ""
    if s in PROTECT:
        return False
    if s.startswith("03e2e-") or s.startswith("e2e-") or s.startswith("e2e_owned"):
        return True
    if n.startswith("03E2E") or "真实E2E" in n or n.startswith("实机E2E"):
        return True
    if re.match(r"^nf_real_\d+$", n):  # 可能作为 name? 一般是 username; 用 slug 更稳
        return False
    return False


def main() -> int:
    if not SERVER_DB.is_file():
        print("skip: 无", SERVER_DB, file=sys.stderr)
        return 0
    conn = sqlite3.connect(str(SERVER_DB))
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT id, name, slug, is_template FROM projects")
    to_del: list[sqlite3.Row] = [
        r for r in cur.fetchall() if not r["is_template"] and is_test_row(r["slug"], r["name"])
    ]
    for r in to_del:
        slug = r["slug"]
        pdir = PROJECTS_DIR / slug
        if DRY:
            print("would delete", r["id"], slug, pdir, "exists" if pdir.is_dir() else "no_dir")
        else:
            if pdir.is_dir():
                shutil.rmtree(pdir, ignore_errors=True)
            conn.execute("DELETE FROM projects WHERE id = ?", (r["id"],))
            print("deleted", r["id"], slug)
    if not DRY and to_del:
        conn.commit()
    conn.close()
    print("done, rows:", len(to_del), "dry" if DRY else "applied")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
