"""
bmrs_live.py — BMRS Insights API client for real-time and historical data.

All endpoints are free, no API key required.
Data is half-hourly (settlement periods), aligned to UK BST/GMT.

Endpoints used:
  - /generation/outturn/summary       → generation by fuel type
  - /demand/outturn                   → system demand (INDO + ITSDO)
  - /balancing/pricing/market-index   → day-ahead prices
  - /generation/outturn/interconnectors → interconnector flows
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import httpx
import pandas as pd
from loguru import logger

BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"

# BMRS fuel type → our canonical type
BMRS_FUEL_MAP: dict[str, str] = {
    "BIOMASS": "biomass",
    "CCGT": "gas_ccgt",
    "COAL": "coal",
    "NUCLEAR": "nuclear",
    "OCGT": "gas_ocgt",
    "OIL": "oil",
    "WIND": "wind",
    "NPSHYD": "hydro",
    "PS": "pumped_storage",
    "OTHER": "other",
    # Interconnectors (prefixed INT)
    "INTFR": "ic_france",
    "INTIRL": "ic_ireland",
    "INTNED": "ic_netherlands",
    "INTEW": "ic_ewic",
    "INTELEC": "ic_eleclink",
    "INTIFA2": "ic_ifa2",
    "INTNEM": "ic_nemo",
    "INTNSL": "ic_nsl",
    "INTVKL": "ic_viking",
    "INTGRNL": "ic_greenlink",
}

_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(timeout=30, follow_redirects=True)
    return _client


def _get(endpoint: str, params: dict | None = None) -> Any:
    """Make a GET request to BMRS API."""
    url = f"{BASE_URL}{endpoint}"
    r = _get_client().get(url, params=params or {})
    r.raise_for_status()
    return r.json()


# ─── Generation by fuel type ────────────────────────────────────────────────

def fetch_generation_mix(
    from_date: date | None = None,
    to_date: date | None = None,
) -> pd.DataFrame:
    """
    Fetch half-hourly generation outturn by fuel type.

    Returns DataFrame with columns:
      timestamp, settlement_period, fuel_type, generation_mw
    """
    params: dict[str, str] = {}
    if from_date:
        params["from"] = from_date.isoformat()
    if to_date:
        params["to"] = to_date.isoformat()

    data = _get("/generation/outturn/summary", params)

    rows = []
    records = data if isinstance(data, list) else data.get("data", [])
    for period in records:
        ts = period.get("startTime")
        sp = period.get("settlementPeriod")
        for fuel in period.get("data", []):
            rows.append({
                "timestamp": pd.Timestamp(ts),
                "settlement_period": sp,
                "bmrs_fuel": fuel["fuelType"],
                "fuel_type": BMRS_FUEL_MAP.get(fuel["fuelType"], fuel["fuelType"].lower()),
                "generation_mw": fuel["generation"],
            })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("timestamp")
    logger.info(f"Fetched generation mix: {len(df)} records, {df['timestamp'].nunique()} periods")
    return df


# ─── System demand ───────────────────────────────────────────────────────────

def fetch_demand(
    from_date: date | None = None,
    to_date: date | None = None,
) -> pd.DataFrame:
    """
    Fetch half-hourly demand outturn (INDO and ITSDO).

    INDO = Initial National Demand Outturn (net of station self-consumption)
    ITSDO = Initial Transmission System Demand Outturn (what the grid sees)
    """
    params: dict[str, str] = {}
    if from_date:
        params["from"] = from_date.isoformat()
    if to_date:
        params["to"] = to_date.isoformat()

    data = _get("/demand/outturn", params)
    records = data if isinstance(data, list) else data.get("data", [])

    rows = []
    for rec in records:
        rows.append({
            "timestamp": pd.Timestamp(rec.get("startTime")),
            "settlement_period": rec.get("settlementPeriod"),
            "demand_mw": rec.get("initialDemandOutturn"),
            "transmission_demand_mw": rec.get("initialTransmissionSystemDemandOutturn"),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"])
    logger.info(f"Fetched demand: {len(df)} records")
    return df


# ─── Day-ahead prices ───────────────────────────────────────────────────────

def fetch_prices(
    from_date: date | None = None,
    to_date: date | None = None,
) -> pd.DataFrame:
    """Fetch day-ahead market index prices (£/MWh)."""
    params: dict[str, str] = {}
    if from_date:
        params["from"] = from_date.isoformat()
    if to_date:
        params["to"] = to_date.isoformat()

    data = _get("/balancing/pricing/market-index", params)
    records = data if isinstance(data, list) else data.get("data", [])

    rows = []
    for rec in records:
        rows.append({
            "timestamp": pd.Timestamp(rec.get("startTime")),
            "settlement_period": rec.get("settlementPeriod"),
            "price_gbp_mwh": rec.get("price"),
            "volume_mwh": rec.get("volume"),
            "provider": rec.get("dataProvider"),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("timestamp")
    logger.info(f"Fetched prices: {len(df)} records")
    return df


# ─── Interconnector flows ───────────────────────────────────────────────────

def fetch_interconnector_flows(
    from_date: date | None = None,
    to_date: date | None = None,
) -> pd.DataFrame:
    """Fetch half-hourly interconnector generation/flow data."""
    params: dict[str, str] = {}
    if from_date:
        params["from"] = from_date.isoformat()
    if to_date:
        params["to"] = to_date.isoformat()

    data = _get("/generation/outturn/interconnectors", params)
    records = data if isinstance(data, list) else data.get("data", [])

    rows = []
    for rec in records:
        rows.append({
            "timestamp": pd.Timestamp(rec.get("startTime")),
            "settlement_period": rec.get("settlementPeriod"),
            "interconnector": rec.get("interconnectorName", ""),
            "flow_mw": rec.get("generation", 0),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("timestamp")
    logger.info(f"Fetched interconnector flows: {len(df)} records")
    return df


# ─── Convenience: get everything for a date range ───────────────────────────

def fetch_all(
    from_date: date | None = None,
    to_date: date | None = None,
) -> dict[str, pd.DataFrame]:
    """Fetch all time-series data for a date range."""
    return {
        "generation": fetch_generation_mix(from_date, to_date),
        "demand": fetch_demand(from_date, to_date),
        "prices": fetch_prices(from_date, to_date),
        "interconnectors": fetch_interconnector_flows(from_date, to_date),
    }
