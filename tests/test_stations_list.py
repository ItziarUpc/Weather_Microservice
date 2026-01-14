import pytest
from httpx import AsyncClient, ASGITransport

from app.models.station import Station


@pytest.mark.anyio
async def test_list_stations_returns_items(test_app, db_session):
    """
    Ensure GET /stations returns stored stations and a valid total count.
    """
    db_session.add(Station(source="aemet", source_station_id="0252D", name="Station A"))
    db_session.add(Station(source="meteocat", source_station_id="Z8", name="Station B"))
    await db_session.commit()

    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.get("/stations?limit=10&offset=0")

    assert r.status_code == 200
    data = r.json()

    assert data["total"] == 2
    assert len(data["items"]) == 2
    assert {x["source"] for x in data["items"]} == {"aemet", "meteocat"}


@pytest.mark.anyio
async def test_list_stations_filter_by_source(test_app, db_session):
    """
    Ensure filtering by provider source works as expected.
    """
    db_session.add(Station(source="aemet", source_station_id="AAA", name="A"))
    db_session.add(Station(source="meteocat", source_station_id="BBB", name="B"))
    await db_session.commit()

    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.get("/stations?source=aemet")

    assert r.status_code == 200
    data = r.json()

    assert data["total"] == 1
    assert len(data["items"]) == 1
    assert data["items"][0]["source"] == "aemet"