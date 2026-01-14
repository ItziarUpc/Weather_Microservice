import pytest
from datetime import datetime, date, timezone
from httpx import AsyncClient, ASGITransport

from app.models.station import Station
from app.models.weather_observation import WeatherObservation


@pytest.mark.asyncio
async def test_get_observations_by_station_id(test_app, db_session):
    station = Station(source="aemet", source_station_id="B013X", name="Test station")
    db_session.add(station)
    await db_session.flush()

    obs = WeatherObservation(
        station_id=station.id,
        ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
        tmin=1.0,
        tmax=5.0,
        precip=0.2,
        wind=None,
    )
    db_session.add(obs)
    await db_session.commit()

    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.get(
            "/observations",
            params={
                "station_id": station.id,
                "start_date": "2026-01-01",
                "end_date": "2026-01-01",
            },
        )

    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 1
    assert data["items"][0]["tmin"] == 1.0