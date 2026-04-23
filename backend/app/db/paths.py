from __future__ import annotations

from pathlib import Path

from app.config import PROJECTS_DIR


def get_project_dir(project_slug: str) -> Path:
    return PROJECTS_DIR / project_slug


def get_project_db_path(project_slug: str) -> Path:
    return get_project_dir(project_slug) / "project.db"
