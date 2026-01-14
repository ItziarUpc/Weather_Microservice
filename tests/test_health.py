import pytest
from httpx import AsyncClient, ASGITransport


@pytest.mark.asyncio
async def test_health_ok(test_app):
    """
    Test the basic service health endpoint.

    This test verifies that:
    - The `/health` endpoint responds with HTTP 200.
    - The response body contains a `status` field with value `ok`.
    - The response includes the `service` field identifying the API.
    """
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.get("/health")

    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert "service" in data


@pytest.mark.asyncio
async def test_health_db_ok(test_app):
    """
    Test the database health endpoint.

    This test verifies that:
    - The `/health/db` endpoint responds with HTTP 200.
    - The API can successfully execute a simple query against the database.
    - The response confirms database connectivity with `db = ok`.
    """
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.get("/health/db")

    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert data["db"] == "ok"