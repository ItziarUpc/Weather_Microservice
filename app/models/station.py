from typing import Optional

from sqlalchemy import Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class Station(Base):
    """
    Weather station entity.

    Represents a physical or virtual weather station provided by an
    external data source (e.g. AEMET or Meteocat).

    Each station is uniquely identified by the combination of:
    - `source` (data provider)
    - `source_station_id` (station identifier in that provider)
    """

    __tablename__ = "stations"

    # ------------------------------------------------------------------
    # Columns
    # ------------------------------------------------------------------

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Internal unique identifier for the station",
    )

    source: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="Data provider name (e.g. 'aemet', 'meteocat')",
    )

    source_station_id: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="Station identifier as defined by the external provider",
    )

    name: Mapped[Optional[str]] = mapped_column(
        String(256),
        nullable=True,
        comment="Human-readable station name",
    )

    # ------------------------------------------------------------------
    # Relationships
    # ------------------------------------------------------------------

    observations = relationship(
        "WeatherObservation",
        back_populates="station",
        cascade="all, delete-orphan",
    )

    # ------------------------------------------------------------------
    # Constraints
    # ------------------------------------------------------------------

    __table_args__ = (
        UniqueConstraint(
            "source",
            "source_station_id",
            name="uq_station_source_id",
        ),
    )