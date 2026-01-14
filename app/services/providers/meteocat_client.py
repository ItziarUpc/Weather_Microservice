from __future__ import annotations

from datetime import date
from typing import Any, Dict, List
import httpx

from app.core.config import settings


class MeteocatClient:
    """
    Meteocat XEMA client.

    Station metadata endpoint is documented as:
    Base URL: https://api.meteo.cat/xema/v1
    Resource: /estacions/metadades?estat={estat}&data={data} :contentReference[oaicite:5]{index=5}

    For daily data, many implementations use endpoints like:
    /xema/v1/estacions/mesurades/{codi}/{yyyy}/{mm}/{dd}
    (as in your JS example). We'll keep that strategy.
    """

    BASE = "https://api.meteo.cat/xema/v1"

    def __init__(self, api_key: str | None = None, timeout_s: float = 30.0):
        self.api_key = api_key or settings.meteocat_api_key
        if not self.api_key:
            raise RuntimeError("METEOCAT_API_KEY is not configured")
        self.timeout = timeout_s

    async def _get_json(self, url: str) -> Any:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.get(url, headers={"x-api-key": self.api_key, "accept": "application/json"})
            r.raise_for_status()
            return r.json()

    async def list_stations(self) -> List[Dict[str, Any]]:
        # Keep it simple: request all stations (no filters).
        # The docs show optional query params like estat/data. :contentReference[oaicite:6]{index=6}
        url = f"{self.BASE}/estacions/metadades"
        data = await self._get_json(url)
        # Depending on Meteocat response shape, it may wrap in {"content":[...]} or return list directly.
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("content", "estacions", "data"):
                if key in data and isinstance(data[key], list):
                    return data[key]
        return []

    async def daily_by_station(self, code: str, d: date) -> Any:
        yyyy = f"{d.year:04d}"
        mm = f"{d.month:02d}"
        dd = f"{d.day:02d}"
        url = f"{self.BASE}/estacions/mesurades/{code}/{yyyy}/{mm}/{dd}"
        return await self._get_json(url)