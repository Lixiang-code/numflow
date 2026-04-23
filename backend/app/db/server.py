"""Global server.sqlite: users, sessions, project index."""

from __future__ import annotations

import contextlib
import sqlite3
import time
from pathlib import Path
from typing import Generator

import bcrypt

from app.config import DATA_DIR, PROJECTS_DIR, SERVER_DB_PATH
from app.db.paths import get_project_db_path
from app.db.project_schema import init_project_db


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except ValueError:
        return False


def connect_sqlite_file(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextlib.contextmanager
def get_server_connection() -> Generator[sqlite3.Connection, None, None]:
    conn = connect_sqlite_file(SERVER_DB_PATH)
    try:
        yield conn
    finally:
        conn.close()


def init_server_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PROJECTS_DIR.mkdir(parents=True, exist_ok=True)
    with get_server_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                is_admin INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                expires_at REAL NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_user_id INTEGER,
                name TEXT NOT NULL,
                slug TEXT NOT NULL UNIQUE,
                is_template INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                FOREIGN KEY (owner_user_id) REFERENCES users(id) ON DELETE SET NULL
            );
            """
        )
        conn.commit()

        cur = conn.execute("SELECT COUNT(*) AS c FROM users")
        if cur.fetchone()["c"] == 0:
            now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            admin_hash = hash_password("e8cTY7er")
            conn.execute(
                "INSERT INTO users (username, password_hash, is_admin, created_at) VALUES (?,?,?,?)",
                ("lixiang", admin_hash, 1, now),
            )
            conn.commit()

        cur = conn.execute(
            "SELECT id FROM projects WHERE slug = ?",
            ("template",),
        )
        if cur.fetchone() is None:
            now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            conn.execute(
                """
                INSERT INTO projects (owner_user_id, name, slug, is_template, created_at)
                VALUES (NULL, ?, ?, 1, ?)
                """,
                ("模板项目", "template", now),
            )
            conn.commit()
            slug = "template"
            db_path = get_project_db_path(slug)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            pc = connect_sqlite_file(db_path)
            try:
                init_project_db(pc, seed_readme=True)
            finally:
                pc.close()


def get_server_db():
    with get_server_connection() as conn:
        yield conn
