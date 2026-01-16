from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.station import Station


class StationRepository:
    """
    Repository for managing weather station persistence.

    This repository encapsulates all database operations related to
    `Station` entities, providing a clean abstraction over SQLAlchemy
    queries and avoiding direct database access from services or routers.
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize the repository with an active database session.

        Args:
            db: Asynchronous SQLAlchemy session.
        """
        self.db = db

    async def get_by_id(self, station_id: int) -> Optional[Station]:
        """
        Return a station by its internal DB id, or None if not found.
        """
        stmt = select(Station).where(Station.id == station_id)
        res = await self.db.execute(stmt)
        return res.scalar_one_or_none()

    async def get_by_source_id(
        self,
        source: str,
        source_station_id: str,
    ) -> Optional[Station]:
        """
        Retrieve a station by its external provider identifier.

        Args:
            source: Name of the data provider (e.g. 'aemet', 'meteocat').
            source_station_id: Station identifier defined by the provider.

        Returns:
            The matching `Station` if found, otherwise `None`.
        """
        stmt = select(Station).where(
            Station.source == source,
            Station.source_station_id == source_station_id,
        )
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def create_if_not_exists(
        self,
        source: str,
        source_station_id: str,
        name: Optional[str] = None,
    ) -> Station:
        """
        Create a station if it does not already exist.

        This method ensures idempotent station creation by first checking
        for an existing station with the same provider and identifier.

        Args:
            source: Name of the data provider (e.g. 'aemet', 'meteocat').
            source_station_id: Station identifier defined by the provider.
            name: Optional human-readable station name.

        Returns:
            The existing or newly created `Station` instance.
        """
        station = await self.get_by_source_id(source, source_station_id)
        if station:
            return station

        station = Station(
            source=source,
            source_station_id=source_station_id,
            name=name,
        )
        self.db.add(station)

        # Flush to obtain the generated primary key without committing
        await self.db.flush()

        return station
    
    async def list_stations(self, source: Optional[str] = None, limit: int = 1000, offset: int = 0) -> List[Station]:
        """
        List stations with optional provider filtering and pagination.

        Args:
            source: Optional provider filter (e.g. 'aemet' or 'meteocat').
            limit: Max items to return.
            offset: Pagination offset.

        Returns:
            A list of Station models.
        """
        
        stmt = select(Station).order_by(Station.id.asc()).limit(limit).offset(offset)
        if source:
            stmt = stmt.where(Station.source == source)

        res = await self.db.execute(stmt)
        return list(res.scalars().all())