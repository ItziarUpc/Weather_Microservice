from fastapi import FastAPI
from app.core.config import settings
from app.routers.health import router as health_router


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        description="Weather API: ingestion + query endpoints",
    )

    app.include_router(health_router)

    return app


app = create_app()