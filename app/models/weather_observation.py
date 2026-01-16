from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Integer,
    DateTime,
    Float,
    ForeignKey,
    JSON,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class WeatherObservation(Base):
    """
    Weather observation entity.

    Represents a single meteorological observation recorded at a specific
    weather station and timestamp.

    Observations are linked to a station and may contain multiple
    meteorological variables (temperature, precipitation, etc.).
    """

    __tablename__ = "weather_observations"

    # ------------------------------------------------------------------
    # Columns
    # ------------------------------------------------------------------

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Internal unique identifier for the observation",
    )

    station_id: Mapped[int] = mapped_column(
        ForeignKey("stations.id"),
        nullable=False,
        comment="Reference to the weather station that produced this observation",
    )

    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="Timestamp of the observation (UTC)",
    )

    tmin: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True,
        comment="Minimum temperature for the observation period",
    )

    tmax: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True,
        comment="Maximum temperature for the observation period",
    )

    tavg: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True,
        comment="Average temperature for the observation period",
    )

    precip: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True,
        comment="Precipitation amount",
    )

    raw: Mapped[Optional[dict]] = mapped_column(
        JSON,
        nullable=True,
        comment="Raw observation payload as received from the external provider",
    )

    # ------------------------------------------------------------------
    # Relationships
    # ------------------------------------------------------------------

    station = relationship(
        "Station",
        back_populates="observations",
    )

    # ------------------------------------------------------------------
    # Constraints
    # ------------------------------------------------------------------

    __table_args__ = (
        UniqueConstraint(
            "station_id",
            "ts",
            name="uq_obs_station_ts",
        ),
    )