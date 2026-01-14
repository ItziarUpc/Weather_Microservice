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

    date: Optional[datetime] = Field(
        default=None,
        description="Target date to ingest. If omitted, the service ingests yesterday (UTC).",
        examples=["2026-01-13"],
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