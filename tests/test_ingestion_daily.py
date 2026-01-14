import pytest
from httpx import AsyncClient, ASGITransport
from unittest.mock import AsyncMock, patch
from datetime import date
from sqlalchemy import select

from app.models.station import Station
from app.models.weather_observation import WeatherObservation

# Import the module where IngestionService lives
import app.services.ingestion_service as ingestion_service_module


@pytest.mark.anyio
async def test_ingestion_daily_full_flow(test_app, db_session):
    """
    Full integration test for POST /ingestion/daily.

    - External providers (AEMET, Meteocat) are mocked
    - Database writes are real (SQLite in-memory)
    - Ensures stations and observations are created
    """

    fake_aemet_stations = [
        {"idema": "A1", "nombre": "AEMET Station 1"},
        {"idema": "A2", "nombre": "AEMET Station 2"},
    ]

    fake_aemet_daily = [
        {"indicativo": "A1", "tmin": "5,0", "tmax": "15,0", "prec": "1,2"},
        {"indicativo": "A2", "tmin": "6,0", "tmax": "16,0", "prec": "0,0"},
    ]

    fake_meteocat_stations = [{"codi": "M1", "nom": "Meteocat Station 1"}]
    fake_meteocat_daily = {"dummy": "raw_meteocat_payload"}

    # Patch the symbols used inside ingestion_service.py (guaranteed)
    with patch.object(ingestion_service_module, "AemetClient") as MockAemet, patch.object(
        ingestion_service_module, "MeteocatClient"
    ) as MockMeteocat:

        aemet_instance = MockAemet.return_value
        aemet_instance.list_stations = AsyncMock(return_value=fake_aemet_stations)
        aemet_instance.daily_all_stations = AsyncMock(return_value=fake_aemet_daily)
        aemet_instance.parse_numeric.side_effect = lambda x: float(x.replace(",", ".")) if x else None

        meteocat_instance = MockMeteocat.return_value
        meteocat_instance.list_stations = AsyncMock(return_value=fake_meteocat_stations)
        meteocat_instance.daily_by_station = AsyncMock(return_value=fake_meteocat_daily)

        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            response = await ac.post("/ingestion/daily", json={"date": "2026-01-01T00:00:00Z"})

    assert response.status_code == 200, response.text
    body = response.json()

    assert body["date"] == "2026-01-01"
    assert body["stations_upserted"]["aemet"] == 2
    assert body["stations_upserted"]["meteocat"] == 1
    assert body["observations_upserted"]["aemet"] == 2
    assert body["observations_upserted"]["meteocat"] == 1

    stations = (await db_session.execute(select(Station))).scalars().all()
    observations = (await db_session.execute(select(WeatherObservation))).scalars().all()

    assert len(stations) == 3
    assert len(observations) == 3
    assert {obs.ts.date() for obs in observations} == {date(2026, 1, 1)}