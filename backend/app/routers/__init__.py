from fastapi import APIRouter

from app.routers import health

api_router = APIRouter()
api_router.include_router(health.router, tags=["health"])
