from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.weather_observation import WeatherObservation


class WeatherObservationRepository:
    """
    Repository for managing weather observations persistence.

    This repository encapsulates all database operations related to
    `WeatherObservation` entities, including insertion, update (upsert),
    and retrieval of observations over time.
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize the repository with an active database session.

        Args:
            db: Asynchronous SQLAlchemy session.
        """
        self.db = db

    async def upsert_observation(
        self,
        station_id: int,
        ts: datetime,
        tmin: Optional[float] = None,
        tmax: Optional[float] = None,
        precip: Optional[float] = None,
        wind: Optional[float] = None,
        raw: Optional[dict] = None,
    ) -> WeatherObservation:
        """
        Insert or update a weather observation for a given station and timestamp.

        If an observation already exists for the `(station_id, ts)` pair,
        it is updated with the new values. Otherwise, a new observation
        is created.

        This method provides idempotent behavior and is suitable for
        daily ingestion jobs.

        Args:
            station_id: Internal identifier of the weather station.
            ts: Timestamp of the observation.
            tmin: Minimum temperature value.
            tmax: Maximum temperature value.
            precip: Precipitation amount.
            wind: Wind speed.
            raw: Raw provider payload stored for traceability.

        Returns:
            The existing or newly created `WeatherObservation` instance.
        """
        stmt = select(WeatherObservation).where(
            WeatherObservation.station_id == station_id,
            WeatherObservation.ts == ts,
        )
        result = await self.db.execute(stmt)
        obs = result.scalar_one_or_none()

        if obs:
            obs.tmin = tmin
            obs.tmax = tmax
            obs.precip = precip
            obs.wind = wind
            obs.raw = raw
            await self.db.flush()
            return obs

        obs = WeatherObservation(
            station_id=station_id,
            ts=ts,
            tmin=tmin,
            tmax=tmax,
            precip=precip,
            wind=wind,
            raw=raw,
        )
        self.db.add(obs)
        await self.db.flush()

        return obs

    async def get_range_by_station(
        self,
        station_id: int,
        start_ts: datetime,
        end_ts: datetime,
    ) -> list[WeatherObservation]:
        """
        Retrieve all observations for a given station within a time range.

        Observations are returned ordered chronologically.

        Args:
            station_id: Internal identifier of the weather station.
            start_ts: Start timestamp (inclusive).
            end_ts: End timestamp (inclusive).

        Returns:
            A list of `WeatherObservation` instances ordered by timestamp.
        """
        stmt = (
            select(WeatherObservation)
            .where(
                WeatherObservation.station_id == station_id,
                WeatherObservation.ts >= start_ts,
                WeatherObservation.ts <= end_ts,
            )
            .order_by(WeatherObservation.ts.asc())
        )
        result = await self.db.execute(stmt)
        return list(result.scalars().all())