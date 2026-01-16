from datetime import datetime
from typing import Optional

from sqlalchemy import select, func
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
        tavg: Optional[float] = None,
        precip: Optional[float] = None,
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
            obs.tavg = tavg
            obs.raw = raw
            await self.db.flush()
            return obs

        obs = WeatherObservation(
            station_id=station_id,
            ts=ts,
            tmin=tmin,
            tmax=tmax,
            tavg=tavg,
            precip=precip,
            raw=raw,
        )
        print("Inserting new observation:", station_id, ts, obs)
        self.db.add(obs)
        await self.db.flush()
        await self.db.commit()

        return obs

    async def get_range_by_station(
        self,
        station_id: int,
        start_ts: datetime,
        end_ts: datetime,
        limit: int,
        offset: int,
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
            .limit(limit)
            .offset(offset)
        )

        items = (await self.db.execute(stmt)).scalars().all()

        count_stmt = select(func.count()).select_from(WeatherObservation).where(
            WeatherObservation.station_id == station_id,
            WeatherObservation.ts >= start_ts,
            WeatherObservation.ts <= end_ts,
        )

        total = (await self.db.execute(count_stmt)).scalar_one()

        return items, total
    
    async def get_latest_ts_by_station(self, station_id: int) -> Optional[datetime]:
        """
        Returns the latest observation timestamp stored for a station, or None if none exist.
        """
        stmt = select(func.max(WeatherObservation.ts)).where(
            WeatherObservation.station_id == station_id
        )
        res = await self.db.execute(stmt)
        return res.scalar_one()