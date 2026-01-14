from typing import Optional
from pydantic import BaseModel, ConfigDict, Field


class StationOut(BaseModel):
    """
    Public representation of a stored weather station.
    """

    model_config = ConfigDict(from_attributes=True)

    id: int
    source: str
    source_station_id: str
    name: Optional[str] = None


class StationListResponse(BaseModel):
    """
    Response payload for listing stations with pagination.
    """
    
    items: list[StationOut] = Field(default_factory=list)
    total: int