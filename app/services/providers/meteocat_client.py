from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional
import httpx
from sqlalchemy import Tuple

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
        print("Meteocat daily URL:", url)
        return await self._get_json(url)
    
    @staticmethod
    def parse_daily_payload(raw: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        """
        Extracts daily aggregates from Meteocat payload.

        Mirrors the JS logic:
        - codi '35': precipitation -> average of lecture.valor
        - codi '32': avg air temperature -> average of lecture.valor
        - codi '40': max temperature -> max(lecture.valor)
        - codi '42': min temperature -> min(lecture.valor)

        Returns:
            (tmin, tmax, precip, tavg)
        """
        if not raw:
            return None, None, None, None

        # API sometimes returns a list, as in your JS: bodyAsObject[0]
        root = raw[0] if isinstance(raw, list) and raw else raw
        variables = root.get("variables") if isinstance(root, dict) else None
        if not variables:
            return None, None, None, None

        totals = {
            "35": {"total": 0.0, "count": 0},  # precip
            "32": {"total": 0.0, "count": 0},  # avg temp
        }
        tmax = None
        tmin = None

        for var in variables:
            code = str(var.get("codi"))
            lectures = var.get("lectures") or []

            if code in totals:
                for lec in lectures:
                    v = lec.get("valor")
                    if v is None:
                        continue
                    totals[code]["total"] += float(v)
                    totals[code]["count"] += 1

            elif code == "40":  # max temp
                for lec in lectures:
                    v = lec.get("valor")
                    if v is None:
                        continue
                    v = float(v)
                    tmax = v if tmax is None else max(tmax, v)

            elif code == "42":  # min temp
                for lec in lectures:
                    v = lec.get("valor")
                    if v is None:
                        continue
                    v = float(v)
                    tmin = v if tmin is None else min(tmin, v)

        tavg = None
        precip = None

        if totals["32"]["count"] > 0:
            tavg = totals["32"]["total"] / totals["32"]["count"]

        if totals["35"]["count"] > 0:
            precip = totals["35"]["total"] / totals["35"]["count"]  # igual que tu JS

        return tmin, tmax, precip, tavg