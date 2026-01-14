from datetime import date as datetime
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict


class ObservationOut(BaseModel):
    """
    Public representation of a daily weather observation.
    """
    model_config = ConfigDict(from_attributes=True)

    date: datetime = Field(..., description="Observation date (UTC day)")
    tmin: Optional[float]
    tmax: Optional[float]
    precip: Optional[float]
    wind: Optional[float]


class ObservationStationOut(BaseModel):
    """
    Station metadata returned together with observations.
    """
    model_config = ConfigDict(from_attributes=True)

    id: int
    source: str
    source_station_id: str
    name: Optional[str]


class ObservationListResponse(BaseModel):
    """
    Response payload for weather observation queries.
    """
    station: ObservationStationOut
    items: list[ObservationOut]
    total: int