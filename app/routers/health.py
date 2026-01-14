from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.db import get_db

router = APIRouter(tags=["Health"])


@router.get(
    "/health",
    summary="Service health check",
    description=(
        "Checks whether the API service is running and returns basic service information. "
        "This endpoint **does not** verify database connectivity."
    ),
    response_description="Service status",
)
def health():
    """
    Basic health check for the API.

    **Returns:**
    - `status`: Always `ok` if the service is running
    - `service`: Service name (configured via `APP_NAME`)
    - `environment`: Current environment (configured via `ENVIRONMENT`, e.g. local/dev/prod)
    """
    return {
        "status": "ok",
        "service": settings.app_name,
        "environment": settings.environment,
    }


@router.get(
    "/health/db",
    summary="Database health check",
    description=(
        "Checks whether the API can connect to PostgreSQL by executing a simple query (`SELECT 1`). "
        "If this endpoint fails, it usually indicates that the database is down or the "
        "`DATABASE_URL` configuration is incorrect."
    ),
    response_description="Database connection status",
)
async def health_db(db: AsyncSession = Depends(get_db)):
    """
    Database connectivity health check.

    Executes `SELECT 1` using an asynchronous SQLAlchemy session.

    **Returns:**
    - `status`: `ok` if the query executes successfully
    - `db`: `ok` if the database connection is healthy

    **Errors:**
    - Returns HTTP 500 if the database connection fails.
    """
    await db.execute(text("SELECT 1"))
    return {"status": "ok", "db": "ok"}