import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from app.core.db import get_db
from app.models import Base
from app.main import app

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@pytest_asyncio.fixture(scope="session")
async def test_engine():
    """
    Create a single in-memory SQLite async engine for the whole test session
    and create all tables once.
    """
    engine = create_async_engine(TEST_DB_URL, future=True)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(test_engine):
    """
    Provide a fresh AsyncSession for each test.
    """
    TestingSessionLocal = sessionmaker(
        bind=test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with TestingSessionLocal() as session:
        yield session


@pytest_asyncio.fixture(autouse=True)
async def clean_db(db_session):
    """
    Ensure each test starts with an empty database.
    """
    await db_session.execute(text("DELETE FROM weather_observations"))
    await db_session.execute(text("DELETE FROM stations"))
    await db_session.commit()
    yield


@pytest.fixture
def test_app(db_session):
    """
    Return a FastAPI app instance with get_db overridden to use the test session.
    Note: this fixture is sync, but it *overrides* an async dependency.
    """
    async def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    yield app
    app.dependency_overrides.clear()