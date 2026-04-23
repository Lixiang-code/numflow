"""FastAPI entrypoint. API lives under /api for alignment with Nginx reverse proxy."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import api_router

app = FastAPI(title="Numflow", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api")


@app.get("/")
def root():
    return {"service": "numflow", "docs": "/docs"}
