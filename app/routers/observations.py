from datetime import date, datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_db
from app.repositories.station_repository import StationRepository
from app.repositories.weather_observation_repository import WeatherObservationRepository
from app.schemas.observations import ObservationListResponse, ObservationOut, ObservationStationOut

router = APIRouter(prefix="/observations", tags=["Observations"])


@router.get(
    "",
    response_model=ObservationListResponse,
    summary="Query weather observations",
    description=(
        "Retrieve daily weather observations for a station and date range.\n\n"
        "You must provide either:\n"
        "- station_id, OR\n"
        "- (source + source_station_id)"
    ),
)
async def list_observations(
    start_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: date = Query(..., description="End date (YYYY-MM-DD)"),
    station_id: Optional[int] = Query(None),
    source: Optional[str] = Query(None),
    source_station_id: Optional[str] = Query(None),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    if not station_id and not (source and source_station_id):
        raise HTTPException(
            status_code=400,
            detail="Provide station_id or (source + source_station_id)",
        )

    station_repo = StationRepository(db)
    obs_repo = WeatherObservationRepository(db)

    if station_id:
        station = await station_repo.get_by_id(station_id)
    else:
        station = await station_repo.get_by_source_id(source, source_station_id)

    if not station:
        raise HTTPException(status_code=404, detail="Station not found")

    start_ts = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
    end_ts = datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc)

    items, total = await obs_repo.get_range_by_station(
        station_id=station.id,
        start_ts=start_ts,
        end_ts=end_ts,
        limit=limit,
        offset=offset,
    )

    return ObservationListResponse(
        station=ObservationStationOut.model_validate(station),
        items=[
            ObservationOut(
                date=o.ts.date(),
                tmin=o.tmin,
                tmax=o.tmax,
                precip=o.precip,
                wind=o.wind,
            )
            for o in items
        ],
        total=total,
    )