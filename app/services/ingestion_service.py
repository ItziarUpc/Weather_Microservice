from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Dict, Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.station_repository import StationRepository
from app.repositories.weather_observation_repository import WeatherObservationRepository
from app.schemas.ingestion import IngestionDailyResponse
from app.services.providers.aemet_client import AemetClient
from app.services.providers.meteocat_client import MeteocatClient


class IngestionService:
    BACKFILL_START = date(2024, 1, 1)

    def __init__(self, db: AsyncSession):
        self.db = db
        self.station_repo = StationRepository(db)
        self.obs_repo = WeatherObservationRepository(db)

    @staticmethod
    def _ts_for_day(d: date) -> datetime:
        # Store at 00:00 UTC for that day.
        return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)

    @staticmethod
    def _today_utc() -> date:
        return datetime.now(timezone.utc).date()

    def _available_end_date(self, provider: str) -> date:
        today = self._today_utc()
        return today - timedelta(days=1)

    async def _station_next_start_date(self, station_id: int, forced_from: Optional[date]) -> date:
        if forced_from:
            return forced_from

        latest_ts = await self.obs_repo.get_latest_ts_by_station(station_id)
        if not latest_ts:
            return self.BACKFILL_START

        # next day after latest stored
        latest_day = latest_ts.astimezone(timezone.utc).date()
        return latest_day + timedelta(days=1)

    async def sync(self, forced_from: Optional[date] = None) -> IngestionDailyResponse:
        """
        Sync/backfill all stations incrementally.
        - First run: starts at 2024-01-01 for stations with no observations.
        - Next runs: start at (last stored day + 1).
        - Ends at provider availability (AEMET: yesterday, Meteocat: today-3).
        """

        stations_upserted: Dict[str, int] = {"aemet": 0, "meteocat": 0}
        observations_upserted: Dict[str, int] = {"aemet": 0, "meteocat": 0}
        failures: Dict[str, Any] = {}

        # -------- Ensure stations exist first (metadata refresh) --------
        # (Puedes decidir refrescar stations cada vez o solo en un endpoint aparte.)
        try:
            aemet = AemetClient()
            aemet_stations = await aemet.list_stations()
            for st in aemet_stations:
                sid = st.get("idema") or st.get("indicativo") or st.get("id")
                if not sid:
                    continue
                name = st.get("nombre") or st.get("name")
                await self.station_repo.create_if_not_exists("aemet", str(sid), name=name)
                stations_upserted["aemet"] += 1
        except Exception as e:
            failures["aemet_station_list"] = str(e)

        try:
            meteocat = MeteocatClient()
            m_stations = await meteocat.list_stations()
            for st in m_stations:
                code = st.get("codi") or st.get("code") or st.get("id")
                if not code:
                    continue
                name = st.get("nom") or st.get("name")
                await self.station_repo.create_if_not_exists("meteocat", str(code), name=name)
                stations_upserted["meteocat"] += 1
        except Exception as e:
            failures["meteocat_station_list"] = str(e)

        # -------- AEMET: backfill day-by-day (bulk per day) --------
        # Estrategia recomendada:
        # - Determinar el rango global mínimo/máximo que falta
        # - Iterar días y llamar daily_all_stations(d) (1 llamada por día)
        # Esto es mucho más eficiente que por estación.
        try:
            aemet_end = self._available_end_date("aemet")

            # Para saber desde cuándo empezar globalmente:
            # buscamos la MIN fecha faltante entre estaciones.
            # (Versión simple: empezamos desde forced_from o BACKFILL_START)
            aemet_start_global = forced_from or self.BACKFILL_START

            aemet_client = AemetClient()

            d = aemet_start_global
            while d <= aemet_end:
                print(f"AEMET: processing date {d.isoformat()}")
                ts = self._ts_for_day(d)
                daily_rows = await aemet_client.daily_all_stations(d)  # devuelve lista del día
                # Si viene vacío, no necesariamente significa que no hay datos; pero normalmente sí.
                # Aun así, seguimos al siguiente día.

                for item in daily_rows:
                    station_code = item.get("indicativo") or item.get("idema")
                    if not station_code:
                        continue

                    st = await self.station_repo.get_by_source_id("aemet", str(station_code))
                    if not st:
                        st = await self.station_repo.create_if_not_exists("aemet", str(station_code), name=None)

                    tmin = aemet_client.parse_numeric(item.get("tmin"))
                    tmax = aemet_client.parse_numeric(item.get("tmax"))
                    precip = aemet_client.parse_numeric(item.get("prec"))
                    wind = None

                    await self.obs_repo.upsert_observation(
                        station_id=st.id,
                        ts=ts,
                        tmin=tmin,
                        tmax=tmax,
                        precip=precip,
                        wind=wind,
                        raw=item,
                    )
                    observations_upserted["aemet"] += 1

                d += timedelta(days=1)

        except Exception as e:
            failures["aemet"] = str(e)

        # -------- METEOCAT: incremental per station (porque endpoint es por estación/día) --------
        # Aquí sí tiene sentido ir por estación, calcular start_date = last+1, end_date = today-3.
        """ try:
            meteocat_end = self._available_end_date("meteocat")
            meteocat_client = MeteocatClient()

            stations = await self.station_repo.list_stations(source="meteocat", limit=100000, offset=0)

            meteocat_station_errors = 0

            for st in stations:
                start_d = await self._station_next_start_date(st.id, forced_from)
                if start_d > meteocat_end:
                    continue

                d = start_d
                while d <= meteocat_end:
                    ts = self._ts_for_day(d)
                    try:
                        raw = await meteocat_client.daily_by_station(st.source_station_id, d)

                        await self.obs_repo.upsert_observation(
                            station_id=st.id,
                            ts=ts,
                            tmin=None,
                            tmax=None,
                            precip=None,
                            wind=None,
                            raw=raw,
                        )
                        observations_upserted["meteocat"] += 1

                    except Exception:
                        # no abortamos: solo contamos error por estación/día
                        meteocat_station_errors += 1
                    d += timedelta(days=1)

            if meteocat_station_errors:
                failures["meteocat_station_errors"] = meteocat_station_errors

        except Exception as e:
            failures["meteocat"] = str(e)"""

        await self.db.commit()

        # Para compatibilidad, devolvemos "date" como el día de fin global (o hoy)
        return IngestionDailyResponse(
            date=self._today_utc(),
            stations_upserted=stations_upserted,
            observations_upserted=observations_upserted,
            failures=failures,
        )
