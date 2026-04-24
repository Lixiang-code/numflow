from __future__ import annotations

import sqlite3
import time
from typing import Any, Dict, Generator, Optional

from fastapi import Cookie, Depends, HTTPException, status

from dataclasses import dataclass

from fastapi import Header

from app.config import SESSION_COOKIE_NAME
from app.db.paths import get_project_db_path
from app.db.project_migrations import ensure_project_migrations
from app.db.server import connect_sqlite_file, get_server_db


def _session_user(
    conn: sqlite3.Connection,
    session_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    if not session_id:
        return None
    now = time.time()
    cur = conn.execute(
        """
        SELECT u.id, u.username, u.is_admin
        FROM sessions s
        JOIN users u ON u.id = s.user_id
        WHERE s.id = ? AND s.expires_at > ?
        """,
        (session_id, now),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {"id": row["id"], "username": row["username"], "is_admin": bool(row["is_admin"])}


def get_optional_user(
    conn: sqlite3.Connection = Depends(get_server_db),
    session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE_NAME),
) -> Optional[Dict[str, Any]]:
    return _session_user(conn, session_id)


def require_user(
    user: Optional[Dict[str, Any]] = Depends(get_optional_user),
) -> Dict[str, Any]:
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="未登录")
    return user


def get_project_row(conn: sqlite3.Connection, project_id: int) -> sqlite3.Row:
    cur = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="项目不存在")
    return row


def ensure_project_access(
    conn: sqlite3.Connection,
    user: Dict[str, Any],
    project_id: int,
    *,
    need_write: bool,
) -> sqlite3.Row:
    row = get_project_row(conn, project_id)
    is_template = bool(row["is_template"])
    owner_id = row["owner_user_id"]
    uid = user["id"]
    is_admin = bool(user["is_admin"])

    if is_template:
        if need_write and not is_admin:
            raise HTTPException(status_code=403, detail="模板项目仅管理员可写")
        return row

    if owner_id != uid and not is_admin:
        raise HTTPException(status_code=403, detail="无权访问该项目")
    return row


def compute_project_can_write(row: sqlite3.Row, user: Dict[str, Any]) -> bool:
    if bool(row["is_template"]):
        return bool(user.get("is_admin"))
    return True


@dataclass
class ProjectDB:
    row: sqlite3.Row
    conn: sqlite3.Connection
    can_write: bool


def get_project_read(
    project_id: int = Header(..., alias="X-Project-Id"),
    sconn: sqlite3.Connection = Depends(get_server_db),
    user: Dict[str, Any] = Depends(require_user),
) -> Generator[ProjectDB, None, None]:
    row = ensure_project_access(sconn, user, project_id, need_write=False)
    path = get_project_db_path(row["slug"])
    if not path.exists():
        raise HTTPException(status_code=500, detail="项目数据库缺失")
    conn = connect_sqlite_file(path)
    try:
        ensure_project_migrations(conn)
        yield ProjectDB(
            row=row,
            conn=conn,
            can_write=compute_project_can_write(row, user),
        )
    finally:
        conn.close()


def get_project_write(
    project_id: int = Header(..., alias="X-Project-Id"),
    sconn: sqlite3.Connection = Depends(get_server_db),
    user: Dict[str, Any] = Depends(require_user),
) -> Generator[ProjectDB, None, None]:
    row = ensure_project_access(sconn, user, project_id, need_write=True)
    path = get_project_db_path(row["slug"])
    if not path.exists():
        raise HTTPException(status_code=500, detail="项目数据库缺失")
    conn = connect_sqlite_file(path)
    try:
        ensure_project_migrations(conn)
        yield ProjectDB(row=row, conn=conn, can_write=True)
    finally:
        conn.close()
