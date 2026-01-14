from fastapi import APIRouter
from app.core.config import settings

router = APIRouter(tags=["health"])


@router.get("/health")
def health():
    return {
        "status": "ok",
        "service": settings.app_name,
        "environment": settings.environment,
    }