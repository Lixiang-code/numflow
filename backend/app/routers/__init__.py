from fastapi import APIRouter

from app.routers import agent, auth, compute, data, health, meta, pipeline, projects, validate

api_router = APIRouter()
api_router.include_router(health.router, tags=["health"])
api_router.include_router(auth.router, tags=["auth"])
api_router.include_router(projects.router, tags=["projects"])
api_router.include_router(meta.router, tags=["meta"])
api_router.include_router(data.router, tags=["data"])
api_router.include_router(compute.router, tags=["compute"])
api_router.include_router(validate.router, tags=["validate"])
api_router.include_router(pipeline.router, tags=["pipeline"])
api_router.include_router(agent.router, prefix="/agent", tags=["agent"])
