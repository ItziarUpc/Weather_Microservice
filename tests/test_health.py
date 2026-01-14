from fastapi.testclient import TestClient
from app.main import app

# Test client for the FastAPI application
client = TestClient(app)


def test_health_ok():
    """
    Test the basic service health endpoint.

    This test verifies that:
    - The `/health` endpoint responds with HTTP 200.
    - The response body contains a `status` field with value `ok`.
    - The response includes the `service` field identifying the API.
    """
    response = client.get("/health")

    assert response.status_code == 200

    data = response.json()
    assert data["status"] == "ok"
    assert "service" in data


def test_health_db_ok():
    """
    Test the database health endpoint.

    This test verifies that:
    - The `/health/db` endpoint responds with HTTP 200.
    - The API can successfully execute a simple query against the database.
    - The response confirms database connectivity with `db = ok`.
    """
    response = client.get("/health/db")

    assert response.status_code == 200

    data = response.json()
    assert data["status"] == "ok"
    assert data["db"] == "ok"