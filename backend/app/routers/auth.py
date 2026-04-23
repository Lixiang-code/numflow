from __future__ import annotations

import secrets
import time
from typing import Optional

import sqlite3
from fastapi import APIRouter, Cookie, Depends, HTTPException, Response, status
from pydantic import BaseModel, Field

from app.config import INVITE_CODE, SESSION_COOKIE_NAME, SESSION_DAYS
from app.db.server import hash_password, verify_password
from app.deps import get_optional_user, get_server_db

router = APIRouter(prefix="/auth", tags=["auth"])


class RegisterBody(BaseModel):
    username: str = Field(min_length=2, max_length=32)
    password: str = Field(min_length=6, max_length=128)
    invite_code: str


class LoginBody(BaseModel):
    username: str
    password: str


def _set_session_cookie(response: Response, session_id: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_id,
        httponly=True,
        samesite="lax",
        max_age=SESSION_DAYS * 86400,
        path="/",
    )


@router.post("/register")
def register(
    body: RegisterBody,
    response: Response,
    conn: sqlite3.Connection = Depends(get_server_db),
):
    if body.invite_code != INVITE_CODE:
        raise HTTPException(status_code=400, detail="邀请码无效")

    uname = body.username.strip()
    cur = conn.execute("SELECT id FROM users WHERE username = ?", (uname,))
    if cur.fetchone():
        raise HTTPException(status_code=400, detail="用户名已存在")

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute(
        "INSERT INTO users (username, password_hash, is_admin, created_at) VALUES (?,?,0,?)",
        (uname, hash_password(body.password), now),
    )
    conn.commit()
    cur = conn.execute("SELECT id FROM users WHERE username = ?", (uname,))
    uid = cur.fetchone()[0]
    sid = secrets.token_urlsafe(32)
    exp = time.time() + SESSION_DAYS * 86400
    conn.execute(
        "INSERT INTO sessions (id, user_id, expires_at) VALUES (?,?,?)",
        (sid, uid, exp),
    )
    conn.commit()
    _set_session_cookie(response, sid)
    return {"ok": True, "user_id": uid}


@router.post("/login")
def login(
    body: LoginBody,
    response: Response,
    conn: sqlite3.Connection = Depends(get_server_db),
):
    cur = conn.execute(
        "SELECT id, password_hash, is_admin FROM users WHERE username = ?",
        (body.username.strip(),),
    )
    row = cur.fetchone()
    if not row or not verify_password(body.password, row["password_hash"]):
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    sid = secrets.token_urlsafe(32)
    exp = time.time() + SESSION_DAYS * 86400
    conn.execute(
        "INSERT INTO sessions (id, user_id, expires_at) VALUES (?,?,?)",
        (sid, row["id"], exp),
    )
    conn.commit()
    _set_session_cookie(response, sid)
    return {"ok": True, "user_id": row["id"], "is_admin": bool(row["is_admin"])}


@router.post("/logout")
def logout(
    response: Response,
    conn: sqlite3.Connection = Depends(get_server_db),
    session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE_NAME),
):
    if session_id:
        conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
        conn.commit()
    response.delete_cookie(SESSION_COOKIE_NAME, path="/")
    return {"ok": True}


@router.get("/me")
def me(user: Optional[dict] = Depends(get_optional_user)):
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="未登录")
    return {"id": user["id"], "username": user["username"], "is_admin": user["is_admin"]}
