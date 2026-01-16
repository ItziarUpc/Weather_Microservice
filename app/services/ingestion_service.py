from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Any, Iterator, Optional

import httpx
from typing import Tuple
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
    
    async def _compute_start_date(self, station_id: int) -> date:
        """
        If station has observations -> last_date + 1
        Else -> BACKFILL_START
        """
        latest_ts = await self.obs_repo.get_latest_ts_by_station(station_id)
        if not latest_ts:
            return self.BACKFILL_START
        latest_day = latest_ts.astimezone(timezone.utc).date()
        return latest_day + timedelta(days=1)
    
    @staticmethod
    async def fetch_with_backoff(fn, *, max_retries: int = 3, sleep_seconds: int = 60):
        """
        Executes an async callable with automatic retry on HTTP 429 errors.

        Args:
            fn: Async callable with no arguments.
            max_retries: Maximum retry attempts.
            sleep_seconds: Seconds to wait after a 429 response.

        Returns:
            The result of fn().

        Raises:
            RuntimeError if max retries are exceeded.
            Any non-429 exception is re-raised immediately.
        """
        for attempt in range(1, max_retries + 1):
            try:
                return await fn()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    if attempt >= max_retries:
                        raise RuntimeError("AEMET rate limit exceeded (max retries reached)") from e

                    print(
                        f"AEMET rate limit hit (429). "
                        f"Retrying in {sleep_seconds}s (attempt {attempt}/{max_retries})"
                    )
                    await asyncio.sleep(sleep_seconds)
                else:
                    raise

    @staticmethod
    def add_months(d: date, months: int) -> date:
        """Return date shifted by N months, keeping day when possible."""
        y = d.year + (d.month - 1 + months) // 12
        m = (d.month - 1 + months) % 12 + 1

        # clamp day to last day of target month
        # (no calendar module needed)
        first_next_month = date(y + (m // 12), (m % 12) + 1, 1) if m < 12 else date(y + 1, 1, 1)
        last_day = first_next_month - timedelta(days=1)
        day = min(d.day, last_day.day)
        return date(y, m, day)


    def iter_chunks_max_6_months(self, start_d: date, end_d: date) -> Iterator[Tuple[date, date]]:
        """
        Yield (chunk_start, chunk_end) where each chunk length is <= 6 calendar months.
        We implement '6 months' as: chunk_end = min(end_d, add_months(chunk_start, 6) - 1 day)
        """
        cur = start_d
        while cur <= end_d:
            chunk_end = self.add_months(cur, 6) - timedelta(days=1)
            if chunk_end > end_d:
                chunk_end = end_d
            yield cur, chunk_end
            cur = chunk_end + timedelta(days=1)

    async def sync(self) -> IngestionDailyResponse:
        """
        Sync/backfill all stations incrementally.
        - First run: starts at 2024-01-01 for stations with no observations.
        - Next runs: start at (last stored day + 1).
        - Ends at provider availability (AEMET: yesterday, Meteocat: today-3).
        """

        stations_upserted: Dict[str, int] = {"aemet": 0, "meteocat": 0}
        observations_upserted: Dict[str, int] = {"aemet": 0, "meteocat": 0}
        failures: Dict[str, Any] = {}

        # -------------------------
        # AEMET (per-station range)
        # -------------------------
        try:
            aemet = AemetClient()
            aemet_end = self._available_end_date("aemet")

            aemet_stations = await aemet.list_stations()

            print("AEMET stations to process:", len(aemet_stations))

            for meta in aemet_stations:
                sid = meta.get("idema") or meta.get("indicativo") or meta.get("id")
                if not sid:
                    continue

                sid = str(sid)
                name = meta.get("nombre") or meta.get("name")

                print("Processing AEMET station:", sid, name)

                # 1) upsert station
                st = await self.station_repo.create_if_not_exists(
                    source="aemet",
                    source_station_id=sid,
                    name=name,
                )
                stations_upserted["aemet"] += 1

                # 2) compute station-specific start/end
                start_d = await self._compute_start_date(st.id)
                print("  start date:", start_d)
                end_d = aemet_end
                print("  end date:", end_d)

                if start_d > end_d:
                    print("  station already up-to-date")
                    continue  # station already up-to-date

                
                try:
                    for chunk_start, chunk_end in self.iter_chunks_max_6_months(start_d, end_d):
                        rows = await self.fetch_with_backoff(
                            lambda: aemet.daily_range_by_station(
                                source_station_id=sid,
                                start_date=chunk_start,
                                end_date=chunk_end,
                            )
                        )

                        # 4) upsert rows
                        for item in rows:
                            # AEMET includes fecha per row; we trust it more than loop date
                            fecha = item.get("fecha")
                            if not fecha:
                                continue

                            # fecha typically "YYYY-MM-DD"
                            day = date.fromisoformat(fecha)
                            ts = self._ts_for_day(day)

                            tmin = aemet.parse_numeric(item.get("tmin"))
                            tmax = aemet.parse_numeric(item.get("tmax"))
                            precip = aemet.parse_numeric(item.get("prec"))
                            tmed = aemet.parse_numeric(item.get("tmed"))

                            await self.obs_repo.upsert_observation(
                                station_id=st.id,
                                ts=ts,
                                tmin=tmin,
                                tmax=tmax,
                                precip=precip,
                                tavg=tmed,
                                raw=item,
                            )
                            observations_upserted["aemet"] += 1
                        
                        #await asyncio.sleep(1.0)  # be nice with AEMET API

                except Exception as e:
                    # keep compact: one error per station
                    print("  AEMET station data error:", str(e))
                    failures.setdefault("aemet_station_errors", 0)
                    failures["aemet_station_errors"] += 1
                    continue

                

        except Exception as e:
            print("AEMET sync error:", str(e))
            failures["aemet"] = str(e)

        # -------------------------
        # METEOCAT (per-station loop)
        # -------------------------
        try:
            meteocat = MeteocatClient()
            meteocat_end = self._available_end_date("meteocat")

            m_stations = await meteocat.list_stations()

            print("Meteocat stations to process:", len(m_stations))

            for meta in m_stations:
                code = meta.get("codi") or meta.get("code") or meta.get("id")
                if not code:
                    continue

                code = str(code)
                name = meta.get("nom") or meta.get("name")

                print("Processing Meteocat station:", code, name)

                # 1) upsert station
                st = await self.station_repo.create_if_not_exists(
                    source="meteocat",
                    source_station_id=code,
                    name=name,
                )
                stations_upserted["meteocat"] += 1

                # 2) compute station-specific range
                start_d = await self._compute_start_date(st.id)
                print("  start date:", start_d)
                end_d = meteocat_end
                print("  end date:", end_d)

                if start_d > end_d:
                    print("  station already up-to-date")
                    continue

                # 3) fetch day by day (meteocat api is per day)
                d = start_d
                while d <= end_d:
                    print("  fetching date:", d)
                    ts = self._ts_for_day(d)
                    try:
                        raw = await meteocat.daily_by_station(code, d)
                        tmin, tmax, precip, tavg = meteocat.parse_daily_payload(raw)
                        print(f"  fetched data for {d.isoformat()}")
                        await self.obs_repo.upsert_observation(
                            station_id=st.id,
                            ts=ts,
                            tmin=tmin,
                            tmax=tmax,
                            precip=precip,
                            tavg=tavg,
                            raw=raw,
                        )
                        print(f"  upserted observation for {d.isoformat()}")
                        observations_upserted["meteocat"] += 1
                    except Exception as e:
                        print("  Meteocat station data error for date:", d, str(e))
                        failures.setdefault("meteocat_station_errors", 0)
                        failures["meteocat_station_errors"] += 1
                        break  # skip to next station on error
                    d += timedelta(days=1)

        except Exception as e:
            print("Meteocat sync error:", str(e))
            failures["meteocat"] = str(e)

        return IngestionDailyResponse(
            date=self._today_utc(),
            stations_upserted=stations_upserted,
            observations_upserted=observations_upserted,
            failures=failures,
        )
