from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.core.db import get_db
from app.models.station import Station
from app.repositories.station_repository import StationRepository
from app.schemas.stations import StationListResponse, StationOut

router = APIRouter(prefix="/stations", tags=["Stations"])


@router.get(
    "",
    response_model=StationListResponse,
    summary="List stations",
    description="Returns stations stored in PostgreSQL. Optionally filter by provider source.",
)
async def list_stations(
    source: Optional[str] = Query(default=None, description="Filter by provider source: 'aemet' or 'meteocat'"),
    limit: int = Query(default=200, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
) -> StationListResponse:
    """
    List stored stations.

    This endpoint is useful to:
    - confirm station ingestion worked
    - provide station lists to clients
    - support UI dropdowns / station selectors
    """

    repo = StationRepository(db)

    items = await repo.list_stations(source=source, limit=limit, offset=offset)

    # Total count for pagination
    stmt = select(func.count(Station.id))
    if source:
        stmt = stmt.where(Station.source == source)
    total = (await db.execute(stmt)).scalar_one()

    return StationListResponse(
        items=[StationOut.model_validate(x) for x in items],
        total=int(total),
    )