from app.core.db import engine
from app.models import Base


async def init_db() -> None:
    """
    Initialize the database schema.

    This function creates all database tables defined in the SQLAlchemy
    ORM models if they do not already exist.

    It is intended to be executed during application startup in
    development or early-stage deployments.

    Notes:
    - This uses `Base.metadata.create_all`, which is suitable for
      development and prototyping.
    - In production environments, database migrations should be handled
      using a migration tool such as Alembic.
    """
    async with engine.begin() as conn:
        # Run the synchronous SQLAlchemy `create_all` operation
        # inside an asynchronous context.
        await conn.run_sync(Base.metadata.create_all)