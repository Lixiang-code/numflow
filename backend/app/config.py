"""Load backend/.env early; expose DashScope / Qwen settings."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

BACKEND_ROOT = Path(__file__).resolve().parent.parent
NUMFLOW_ROOT = BACKEND_ROOT.parent
load_dotenv(BACKEND_ROOT / ".env")

DATA_DIR = Path(os.getenv("NUMFLOW_DATA_DIR", str(NUMFLOW_ROOT / "data"))).resolve()
SERVER_DB_PATH = DATA_DIR / "server.sqlite"
PROJECTS_DIR = DATA_DIR / "projects"

INVITE_CODE = os.getenv("NUMFLOW_INVITE_CODE", "lixiang_B22jUD7F").strip()

DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY", "").strip()
DASHSCOPE_BASE_URL = os.getenv(
    "DASHSCOPE_BASE_URL",
    "https://dashscope.aliyuncs.com/compatible-mode/v1",
).strip()
QWEN_MODEL = os.getenv("QWEN_MODEL", "qwen3.6-plus").strip()

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "").strip()
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com").strip()
DEEPSEEK_MODELS = {"deepseek-v4-flash", "deepseek-v4-pro"}

MIMO_API_KEY = os.getenv("MIMO_API_KEY", "").strip()
MIMO_BASE_URL = os.getenv("MIMO_BASE_URL", "https://token-plan-sgp.xiaomimimo.com/v1").strip()
MIMO_CHAT_MODELS = ["mimo-v2-flash", "mimo-v2-pro", "mimo-v2.5", "mimo-v2.5-pro"]

# CORS: comma-separated origins; required when frontend uses credentials
FRONTEND_ORIGINS = [
    o.strip()
    for o in os.getenv(
        "NUMFLOW_FRONTEND_ORIGINS",
        "http://localhost:5173,http://127.0.0.1:5173,http://localhost:8010,http://127.0.0.1:8010",
    ).split(",")
    if o.strip()
]

SESSION_COOKIE_NAME = "numflow_session"
SESSION_DAYS = int(os.getenv("NUMFLOW_SESSION_DAYS", "14"))
