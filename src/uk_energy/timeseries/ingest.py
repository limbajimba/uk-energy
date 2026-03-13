"""
ingest.py — Fetch all available BMRS data and store in DuckDB.

Fetches:
  1. Generation by fuel (24h rolling from BMRS)
  2. Demand INDO + ITSDO (30d rolling from BMRS)
  3. System prices SSP/SBP (yesterday + today)
  4. IC flows bidirectional (30d rolling)
  5. Demand forecast (day-ahead, ~55 periods)
  6. Generation availability (daily by fuel, ~13 days)
  7. System frequency (24h, 1-second resolution)

All fetches are independent — one failure doesn't block others.
All inserts are idempotent — safe to run repeatedly.

Usage:
  python -m uk_energy ts-ingest          # fetch + store everything
  python -m uk_energy ts-ingest --stats  # show storage stats
  python -m uk_energy ts-backfill        # backfill historical prices
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import httpx
import pandas as pd
from loguru import logger

from uk_energy.timeseries.bmrs_live import (
    BASE_URL,
    BMRS_FUEL_MAP,
    IC_NAME_MAP,
    _get,
    fetch_all,
)
from uk_energy.timeseries.store import TimeSeriesStore


def ingest_all(store: TimeSeriesStore | None = None) -> dict[str, int]:
    """Fetch all available BMRS data and store. Returns {table: rows_inserted}."""
    own_store = store is None
    if own_store:
        store = TimeSeriesStore()

    results: dict[str, int] = {}
    today = date.today()
    yesterday = today - timedelta(days=1)

    # 1. Generation + demand + prices + IC flows (from bmrs_live)
    try:
        raw = fetch_all(from_date=yesterday, to_date=today)

        # Generation (domestic only)
        gen = raw.get("generation", pd.DataFrame())
        if not gen.empty:
            domestic = gen[~gen.get("is_ic", False)].copy()
            results["generation"] = store.ingest_generation(domestic)

        # Demand
        results["demand"] = store.ingest_demand(raw.get("demand", pd.DataFrame()))

        # System prices
        results["system_prices"] = store.ingest_system_prices(raw.get("prices", pd.DataFrame()))

        # IC flows
        results["ic_flows"] = store.ingest_ic_flows(raw.get("interconnectors", pd.DataFrame()))

    except Exception as e:
        logger.error(f"fetch_all failed: {e}")

    # 2. Market depth
    try:
        from uk_energy.timeseries.bmrs_live import fetch_market_depth
        md = fetch_market_depth(today)
        results["market_depth"] = store.ingest_market_depth(md)
    except Exception as e:
        logger.error(f"Market depth failed: {e}")
        results["market_depth"] = 0

    # 3. Wind forecast
    try:
        from uk_energy.timeseries.bmrs_live import fetch_wind_forecast
        wf = fetch_wind_forecast()
        results["wind_forecast"] = store.ingest_wind_forecast(wf)
    except Exception as e:
        logger.error(f"Wind forecast failed: {e}")
        results["wind_forecast"] = 0

    # 4. Demand forecast
    try:
        results["demand_forecast"] = _ingest_demand_forecast(store)
    except Exception as e:
        logger.error(f"Demand forecast failed: {e}")
        results["demand_forecast"] = 0

    # 3. Generation availability
    try:
        results["gen_availability"] = _ingest_gen_availability(store)
    except Exception as e:
        logger.error(f"Gen availability failed: {e}")
        results["gen_availability"] = 0

    # 4. System frequency
    try:
        results["frequency"] = _ingest_frequency(store)
    except Exception as e:
        logger.error(f"Frequency failed: {e}")
        results["frequency"] = 0

    # 5. Weather data (Open-Meteo)
    try:
        results["weather"], results["weather_index"] = _ingest_weather(store)
    except Exception as e:
        logger.error(f"Weather failed: {e}")
        results["weather"] = 0
        results["weather_index"] = 0

    if own_store:
        store.close()

    total = sum(v for v in results.values() if isinstance(v, int))
    logger.info(f"Ingestion complete: {total} new rows across {len(results)} tables")
    return results


def backfill_market_depth(store: TimeSeriesStore | None = None, days: int = 14) -> int:
    """Backfill market depth for the last N days."""
    from uk_energy.timeseries.bmrs_live import fetch_market_depth

    own_store = store is None
    if own_store:
        store = TimeSeriesStore()

    total = 0
    today = date.today()

    for i in range(days):
        d = today - timedelta(days=i)
        try:
            df = fetch_market_depth(d)
            if not df.empty:
                n = store.ingest_market_depth(df)
                total += n
                if n > 0:
                    logger.info(f"Backfilled market depth {d}: {n} records")
        except Exception as e:
            logger.warning(f"Market depth backfill failed for {d}: {e}")

    if own_store:
        store.close()

    logger.info(f"Market depth backfill complete: {total} new rows over {days} days")
    return total


def backfill_prices(store: TimeSeriesStore | None = None, days: int = 30) -> int:
    """Backfill system prices for the last N days."""
    own_store = store is None
    if own_store:
        store = TimeSeriesStore()

    total = 0
    today = date.today()

    for i in range(days):
        d = today - timedelta(days=i)
        try:
            data = _get(f"/balancing/settlement/system-prices/{d.isoformat()}")
            records = data if isinstance(data, list) else data.get("data", [])
            if not records:
                continue

            rows = []
            for rec in records:
                rows.append({
                    "timestamp": pd.Timestamp(rec.get("startTime")),
                    "settlement_period": rec.get("settlementPeriod"),
                    "settlement_date": rec.get("settlementDate"),
                    "ssp_gbp_mwh": rec.get("systemSellPrice", 0),
                    "sbp_gbp_mwh": rec.get("systemBuyPrice", 0),
                    "niv_mw": rec.get("netImbalanceVolume", 0),
                    "reserve_scarcity_price": rec.get("reserveScarcityPrice", 0),
                    "accepted_offer_vol": rec.get("totalAcceptedOfferVolume", 0),
                    "accepted_bid_vol": rec.get("totalAcceptedBidVolume", 0),
                    "price_derivation_code": rec.get("priceDerivationCode"),
                })

            df = pd.DataFrame(rows)
            n = store.ingest_system_prices(df)
            total += n
            if n > 0:
                logger.info(f"Backfilled {d}: {n} price records")
        except httpx.HTTPStatusError:
            logger.debug(f"No price data for {d}")
        except Exception as e:
            logger.warning(f"Price backfill failed for {d}: {e}")

    if own_store:
        store.close()

    logger.info(f"Price backfill complete: {total} new rows over {days} days")
    return total


# ─── Forecast + frequency fetchers ──────────────────────────────────────────

def _ingest_weather(store: TimeSeriesStore) -> tuple[int, int]:
    """Fetch and store weather data from Open-Meteo."""
    from uk_energy.timeseries.weather import fetch_weather, fetch_wind_index

    raw = fetch_weather(past_days=7, forecast_days=3)
    raw_n = store.ingest_weather(raw)

    index = fetch_wind_index(past_days=7, forecast_days=3)
    idx_n = store.ingest_weather_index(index)

    return raw_n, idx_n


def _ingest_demand_forecast(store: TimeSeriesStore) -> int:
    """Fetch and store day-ahead demand forecast."""
    data = _get("/forecast/demand/day-ahead")
    records = data if isinstance(data, list) else data.get("data", [])
    if not records:
        return 0

    rows = []
    for rec in records:
        rows.append({
            "publish_time": pd.Timestamp(rec.get("publishTime")),
            "forecast_timestamp": pd.Timestamp(rec.get("startTime")),
            "settlement_period": rec.get("settlementPeriod"),
            "national_demand_mw": rec.get("nationalDemand"),
            "transmission_demand_mw": rec.get("transmissionSystemDemand"),
        })

    df = pd.DataFrame(rows)
    return store.ingest_demand_forecast(df)


def _ingest_gen_availability(store: TimeSeriesStore) -> int:
    """Fetch and store generation availability forecast."""
    data = _get("/forecast/availability/daily")
    records = data if isinstance(data, list) else data.get("data", [])
    if not records:
        return 0

    rows = []
    for rec in records:
        ft_raw = rec.get("fuelType", "")
        ft = BMRS_FUEL_MAP.get(ft_raw, ft_raw.lower())
        avail = rec.get("outputUsable") or rec.get("availableCapacity") or rec.get("generation")
        if avail is not None:
            rows.append({
                "forecast_date": rec.get("forecastDate"),
                "fuel_type": ft,
                "available_mw": float(avail),
            })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    return store.ingest_gen_availability(df)


def _ingest_frequency(store: TimeSeriesStore) -> int:
    """Fetch and store system frequency (1-second resolution)."""
    data = _get("/system/frequency")
    records = data if isinstance(data, list) else data.get("data", [])
    if not records:
        return 0

    rows = []
    for rec in records:
        freq = rec.get("frequency")
        if freq is not None:
            rows.append({
                "timestamp": pd.Timestamp(rec.get("measurementTime")),
                "frequency_hz": float(freq),
            })

    df = pd.DataFrame(rows)
    return store.ingest_frequency(df)
