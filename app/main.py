from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.core.init_db import init_db
from app.core.config import settings
from app.routers.health import router as health_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan context manager.

    This function is executed during the startup and shutdown phases
    of the FastAPI application lifecycle.

    On startup:
    - Initializes the database schema (development/MVP setup).

    On shutdown:
    - Cleans up resources if needed (currently no-op).
    """
    await init_db()
    yield


def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application.

    This factory function:
    - Initializes the FastAPI app with metadata and documentation endpoints.
    - Registers all API routers.
    - Applies the application lifespan handler.

    Returns:
        Configured FastAPI application instance.
    """
    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        description="Weather API: ingestion and query endpoints",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # Register API routers
    app.include_router(health_router)

    return app


# Application entry point
app = create_app()