"""FastAPI entrypoint. API lives under /api for alignment with Nginx reverse proxy."""

import app.config  # noqa: F401 — 尽早加载 backend/.env

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import FRONTEND_ORIGINS
from app.db.server import init_server_db
from app.routers import api_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_server_db()
    yield


app = FastAPI(title="Numflow", version="0.2.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=FRONTEND_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api")


@app.get("/")
def root():
    return {"service": "numflow", "docs": "/docs"}
