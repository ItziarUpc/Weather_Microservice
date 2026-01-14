from datetime import date as datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class IngestionDailyRequest(BaseModel):
    """
    Request body for triggering a daily ingestion job.

    Notes:
    - We accept a `datetime` to be friendly with most clients (JS, Swagger UI).
    - The application will normalize it to a UTC `date` (day granularity).
    """

    from_date: Optional[datetime] = Field(
        default=None,
        description="Force ingestion start date (otherwise uses per-station last stored date, or 2024-01-01 if none).",
        examples=["2024-01-01"],
    )


class IngestionDailyResponse(BaseModel):
    """
    Response payload for a daily ingestion run.
    """
    
    date: datetime = Field(..., description="Ingested date (UTC day).")
    stations_upserted: Dict[str, int] = Field(
        default_factory=dict,
        description="Number of stations upserted per provider (aemet/meteocat).",
        examples=[{"aemet": 850, "meteocat": 190}],
    )
    observations_upserted: Dict[str, int] = Field(
        default_factory=dict,
        description="Number of observations upserted per provider (aemet/meteocat).",
        examples=[{"aemet": 820, "meteocat": 185}],
    )
    failures: Dict[str, Any] = Field(
        default_factory=dict,
        description="Provider-specific errors, if any (kept compact).",
    )