from datetime import date
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_db
from app.schemas.ingestion import IngestionDailyRequest, IngestionDailyResponse
from app.services.ingestion_service import IngestionService

router = APIRouter(prefix="/ingestion", tags=["Ingestion"])


@router.post(
    "/daily",
    response_model=IngestionDailyResponse,
    summary="Daily ingestion for all stations (AEMET + Meteocat)",
    description=(
        "Triggers ingestion from AEMET and Meteocat for all known stations.\n\n"
        "- If `date` is omitted, the service ingests yesterday (UTC).\n"
        "- If a provider fails, the response includes the failure reason.\n"
        "- Meteocat can produce per-station errors (e.g. stations without data for that day); "
        "those are counted and reported but do not abort ingestion."
    ),
)
async def ingestion_daily(payload: IngestionDailyRequest, db: AsyncSession = Depends(get_db)):
    """
    Daily ingestion orchestration endpoint.
    """
    
    try:
        service = IngestionService(db=db)
        result = await service.ingest_daily(target_date=payload.date)
        return result
    except Exception as e:
        # In prod: log exception with stacktrace
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {e}")