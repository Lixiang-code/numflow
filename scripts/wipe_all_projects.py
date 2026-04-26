#!/usr/bin/env python3
"""
清空所有项目数据（第3轮优化要求：删除所有旧项目以适配新流水线/新 schema）。

危险操作：
  - 会删除 server.sqlite 中 projects 表所有行；
  - 会删除 data/projects/ 下所有项目目录（保留模板 'template' 与显式白名单）。

用法：
  NUMFLOW_WIPE_CONFIRM=YES python3 scripts/wipe_all_projects.py

环境变量：
  NUMFLOW_WIPE_CONFIRM   必须为 'YES' 才真正执行
  NUMFLOW_DATA_DIR       数据根目录（默认 ../data）
  NUMFLOW_WIPE_KEEP      逗号分隔的保留 slug（默认 'template'）
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.environ.get("NUMFLOW_DATA_DIR", str(ROOT / "data"))).resolve()
SERVER_DB = DATA_DIR / "server.sqlite"
PROJECTS_DIR = DATA_DIR / "projects"
KEEP = {s.strip() for s in os.environ.get("NUMFLOW_WIPE_KEEP", "template").split(",") if s.strip()}
CONFIRM = os.environ.get("NUMFLOW_WIPE_CONFIRM", "").upper() == "YES"


def main() -> int:
    print(f"DATA_DIR    = {DATA_DIR}")
    print(f"SERVER_DB   = {SERVER_DB}")
    print(f"PROJECTS    = {PROJECTS_DIR}")
    print(f"保留 slug   = {sorted(KEEP)}")
    print(f"CONFIRM     = {CONFIRM}")
    if not SERVER_DB.exists() and not PROJECTS_DIR.exists():
        print("无任何数据，已是干净状态。")
        return 0
    if not CONFIRM:
        print("\n（DRY RUN）未设置 NUMFLOW_WIPE_CONFIRM=YES，仅枚举将被删除项：\n")
    deleted_rows = 0
    deleted_dirs: list[str] = []

    if SERVER_DB.exists():
        conn = sqlite3.connect(str(SERVER_DB))
        try:
            cur = conn.execute("SELECT slug FROM projects")
            slugs = [r[0] for r in cur.fetchall() if r and r[0] not in KEEP]
            for s in slugs:
                print(f"  - DB 行: {s}")
            if CONFIRM and slugs:
                ph = ",".join("?" for _ in slugs)
                conn.execute(f"DELETE FROM projects WHERE slug IN ({ph})", slugs)
                deleted_rows = len(slugs)
                conn.commit()
        except sqlite3.OperationalError:
            print("  (server.sqlite 中无 projects 表)")
        finally:
            conn.close()

    if PROJECTS_DIR.exists():
        for child in sorted(PROJECTS_DIR.iterdir()):
            if not child.is_dir() or child.name in KEEP:
                continue
            print(f"  - 目录: {child.name}")
            if CONFIRM:
                shutil.rmtree(child, ignore_errors=True)
                deleted_dirs.append(child.name)

    if CONFIRM:
        print(f"\n完成：删除项目行 {deleted_rows} 条 / 目录 {len(deleted_dirs)} 个。")
    else:
        print("\n如需真正执行，请：NUMFLOW_WIPE_CONFIRM=YES python3 scripts/wipe_all_projects.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
