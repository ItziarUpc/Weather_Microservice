from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Dict, Any, Optional

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.station_repository import StationRepository
from app.repositories.weather_observation_repository import WeatherObservationRepository
from app.schemas.ingestion import IngestionDailyResponse
from app.services.providers.aemet_client import AemetClient
from app.services.providers.meteocat_client import MeteocatClient


class IngestionService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.station_repo = StationRepository(db)
        self.obs_repo = WeatherObservationRepository(db)

    @staticmethod
    def _default_target_date_utc() -> date:
        # Daily ingestion usually means: ingest yesterday once the day is complete.
        today_utc = datetime.now(timezone.utc).date()
        return today_utc - timedelta(days=1)

    @staticmethod
    def _ts_for_day(d: date) -> datetime:
        # Store at 00:00 UTC for that day.
        return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)

    async def ingest_daily(self, target_date: Optional[date]) -> IngestionDailyResponse:
        d = target_date or self._default_target_date_utc()
        ts = self._ts_for_day(d)

        stations_upserted: Dict[str, int] = {"aemet": 0, "meteocat": 0}
        observations_upserted: Dict[str, int] = {"aemet": 0, "meteocat": 0}
        failures: Dict[str, Any] = {}

        try:
            # ---------------- AEMET ----------------
            try:
                aemet = AemetClient()

                # 1) Upsert ALL stations
                aemet_stations = await aemet.list_stations()
                for st in aemet_stations:
                    source_station_id = st.get("idema") or st.get("indicativo") or st.get("id")
                    if not source_station_id:
                        continue

                    name = st.get("nombre") or st.get("name")
                    await self.station_repo.create_if_not_exists(
                        source="aemet",
                        source_station_id=str(source_station_id),
                        name=name,
                    )
                    stations_upserted["aemet"] += 1

                # 2) Download daily data for ALL stations (single call)
                daily = await aemet.daily_all_stations(d)

                for item in daily:
                    station_code = item.get("indicativo") or item.get("idema") or item.get("station")
                    if not station_code:
                        continue

                    st = await self.station_repo.get_by_source_id("aemet", str(station_code))
                    if not st:
                        st = await self.station_repo.create_if_not_exists("aemet", str(station_code), name=None)

                    tmin = aemet.parse_numeric(item.get("tmin"))
                    tmax = aemet.parse_numeric(item.get("tmax"))
                    precip = aemet.parse_numeric(item.get("prec"))
                    wind = None  # map later if needed

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

            except Exception as e:
                failures["aemet"] = str(e)

            # ---------------- METEOCAT ----------------
            try:
                meteocat = MeteocatClient()

                # 1) Upsert ALL stations from metadata
                m_stations = await meteocat.list_stations()

                def station_code_of(x: dict) -> Optional[str]:
                    for k in ("codi", "code", "id", "codiEstacio"):
                        if k in x and x[k]:
                            return str(x[k])
                    return None

                def station_name_of(x: dict) -> Optional[str]:
                    for k in ("nom", "name", "nomEstacio"):
                        if k in x and x[k]:
                            return str(x[k])
                    return None

                codes: list[str] = []
                for st_meta in m_stations:
                    code = station_code_of(st_meta)
                    if not code:
                        continue
                    codes.append(code)

                    await self.station_repo.create_if_not_exists(
                        source="meteocat",
                        source_station_id=code,
                        name=station_name_of(st_meta),
                    )
                    stations_upserted["meteocat"] += 1

                # 2) Fetch daily data station-by-station
                meteocat_station_errors = 0

                for code in codes:
                    try:
                        raw = await meteocat.daily_by_station(code, d)

                        st = await self.station_repo.get_by_source_id("meteocat", code)
                        if not st:
                            st = await self.station_repo.create_if_not_exists("meteocat", code, name=None)

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

                    except httpx.HTTPStatusError:
                        # Common: station has no data for that day, or code not valid for this resource
                        meteocat_station_errors += 1
                        continue
                    except Exception:
                        meteocat_station_errors += 1
                        continue

                if meteocat_station_errors:
                    failures["meteocat_station_errors"] = meteocat_station_errors

            except Exception as e:
                failures["meteocat"] = str(e)

            # Commit if everything above didn't raise outside provider-level try/except
            await self.db.commit()

        except Exception as e:
            # If anything unexpected bubbles up, rollback transaction
            await self.db.rollback()
            raise

        return IngestionDailyResponse(
            date=d,
            stations_upserted=stations_upserted,
            observations_upserted=observations_upserted,
            failures=failures,
        )