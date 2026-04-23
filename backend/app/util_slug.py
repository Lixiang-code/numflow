from __future__ import annotations

import re
import secrets


def slugify(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    if not s:
        s = "project"
    if len(s) > 48:
        s = s[:48].rstrip("-")
    return s


def unique_slug(base: str, exists) -> str:
    """exists(slug) -> bool if taken."""
    slug = base
    if not exists(slug):
        return slug
    for _ in range(20):
        cand = f"{base}-{secrets.token_hex(3)}"
        if not exists(cand):
            return cand
    return f"{base}-{secrets.token_hex(6)}"
