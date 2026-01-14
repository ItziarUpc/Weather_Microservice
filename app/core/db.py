from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import settings


# ---------------------------------------------------------------------
# Database engine
# ---------------------------------------------------------------------

# Asynchronous SQLAlchemy engine.
# Uses the database URL provided via environment variables.
engine: AsyncEngine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,  # Validates connections before using them
)


# ---------------------------------------------------------------------
# Database session factory
# ---------------------------------------------------------------------

# Factory for asynchronous database sessions.
# `expire_on_commit=False` prevents objects from being expired
# after committing, which is convenient when returning data
# from repositories or services.
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


# ---------------------------------------------------------------------
# Dependency injection
# ---------------------------------------------------------------------

async def get_db() -> AsyncSession:
    """
    FastAPI dependency that provides an asynchronous database session.

    A new `AsyncSession` is created for each request and automatically
    closed once the request lifecycle ends.

    Usage example:
    ```python
    @router.get("/items")
    async def list_items(db: AsyncSession = Depends(get_db)):
        ...
    ```
    """
    async with AsyncSessionLocal() as session:
        yield session