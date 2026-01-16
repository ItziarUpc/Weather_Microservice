from __future__ import annotations

from datetime import date
import json
from typing import Any, Dict, List, Optional
import httpx

from app.core.config import settings


class AemetClient:
    """
    AEMET OpenData client.

    Key endpoints used:
    - Stations inventory (all stations): /api/valores/climatologicos/inventarioestaciones/todasestaciones :contentReference[oaicite:3]{index=3}
    - Daily climatological values for all stations: /api/valores/climatologicos/diarios/datos/fechaini/{...}/fechafin/{...}/todasestaciones :contentReference[oaicite:4]{index=4}
    """

    BASE = "https://opendata.aemet.es/opendata/api"

    def __init__(self, api_key: str | None = None, timeout_s: float = 30.0):
        self.api_key = api_key or settings.aemet_api_key
        if not self.api_key:
            raise RuntimeError("AEMET_API_KEY is not configured")
        self.timeout = timeout_s

    async def _get_json(self, url: str):
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.get(url, headers={"accept": "application/json", "api_key": self.api_key})
            r.raise_for_status()

            # Try the declared encoding first; fallback to latin-1.
            encoding = r.encoding or "utf-8"
            try:
                return json.loads(r.content.decode(encoding))
            except UnicodeDecodeError:
                return json.loads(r.content.decode("latin-1"))

    async def _follow_data_url(self, api_url: str) -> Any:
        """
        AEMET often returns a JSON with a 'datos' URL. We must fetch it to get real data.
        """
        meta = await self._get_json(api_url)
        # Typical: {"estado":200, "datos":"https://.../sh/xxxx", ...}
        data_url = meta.get("datos")
        if not data_url:
            return []
        return await self._get_json(data_url)

    async def list_stations(self) -> List[Dict[str, Any]]:
        api_url = f"{self.BASE}/valores/climatologicos/inventarioestaciones/todasestaciones"
        data = await self._follow_data_url(api_url)
        # AEMET inventory returns a list of station dicts (idema, nombre, latitud, longitud, etc.)
        return list(data) if isinstance(data, list) else []

    async def daily_all_stations(self, d: date) -> List[Dict[str, Any]]:
        """
        Downloads daily climatological values for ALL stations for one day.
        We query [d 00:00 UTC, d 23:59 UTC] but AEMET expects a specific timestamp format.
        """
        # AEMET examples commonly use: YYYY-MM-DDT00:00:00UTC / ...T23:59:00UTC
        start = f"{d.isoformat()}T00:00:00UTC"
        end = f"{d.isoformat()}T23:59:00UTC"
        api_url = f"{self.BASE}/valores/climatologicos/diarios/datos/fechaini/{start}/fechafin/{end}/todasestaciones"
        data = await self._follow_data_url(api_url)
        return list(data) if isinstance(data, list) else []
    
    async def daily_range_by_station(self, source_station_id: str, start_date: date, end_date: date) -> List[dict]:
        """
        Returns daily rows for one station between start_date and end_date inclusive.
        """
        start = f"{start_date.isoformat()}T00:00:00UTC"
        end = f"{end_date.isoformat()}T23:59:00UTC"
        api_url = f"{self.BASE}/valores/climatologicos/diarios/datos/fechaini/{start}/fechafin/{end}/estacion/{source_station_id}"
        data = await self._follow_data_url(api_url)
        return list(data) if isinstance(data, list) else []


    @staticmethod
    def parse_numeric(v: Optional[str]) -> Optional[float]:
        if v is None:
            return None
        # AEMET often uses commas as decimal separators
        try:
            return float(v.replace(",", "."))
        except Exception:
            return None