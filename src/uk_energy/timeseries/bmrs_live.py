"""
bmrs_live.py — BMRS Insights API client for real-time and historical data.

All endpoints are free, no API key required.
Data is half-hourly (settlement periods), aligned to UK clock.

Endpoints used:
  /generation/outturn/summary               generation by fuel type (24h rolling)
  /demand/outturn                           system demand INDO + ITSDO (30d rolling)
  /balancing/settlement/system-prices/{d}   SSP & SBP per settlement period
  /generation/outturn/interconnectors       IC flows with direction (+import/-export)

Data caveats (documented for honesty):
  - Generation summary is transmission-metered only. Solar, most small wind,
    and embedded CHP are invisible. ~10-15 GW of generation is "missing".
  - Generation summary ICs are import-only (never negative). The dedicated
    IC endpoint has bidirectional flows. We use the dedicated endpoint.
  - "OTHER" in gen summary ≈ pumped storage gen + embedded gen. Cannot decompose.
  - All wind is lumped as "WIND" — no onshore/offshore split.
  - System prices are SSP/SBP (imbalance settlement), NOT day-ahead auction.
    Day-ahead prices (EPEX/N2EX) are behind a paywall.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import httpx
import pandas as pd
from loguru import logger

BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"

# BMRS fuel type → our canonical type (generation summary only)
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
    # Interconnectors in gen summary (import-only, never negative)
    "INTFR": "ic_ifa",
    "INTIRL": "ic_moyle",
    "INTNED": "ic_britned",
    "INTEW": "ic_ewic",
    "INTELEC": "ic_eleclink",
    "INTIFA2": "ic_ifa2",
    "INTNEM": "ic_nemo",
    "INTNSL": "ic_nsl",
    "INTVKL": "ic_viking",
    "INTGRNL": "ic_greenlink",
}

# IC dedicated endpoint → canonical name
IC_NAME_MAP: dict[str, str] = {
    "Eleclink (INTELEC)": "ElecLink",
    "Ireland(East-West)": "EWIC",
    "France(IFA)": "IFA",
    "Ireland (Greenlink)": "Greenlink",
    "IFA2 (INTIFA2)": "IFA2",
    "Northern Ireland(Moyle)": "Moyle",
    "Netherlands(BritNed)": "BritNed",
    "Belgium (Nemolink)": "Nemo Link",
    "North Sea Link (INTNSL)": "NSL",
    "Denmark (Viking link)": "Viking Link",
}

# IC canonical → rated capacity MW (for chart context)
IC_CAPACITY: dict[str, int] = {
    "IFA": 2000, "IFA2": 1000, "ElecLink": 1000, "Nemo Link": 1000,
    "BritNed": 1000, "NSL": 1400, "Viking Link": 1400, "EWIC": 500,
    "Moyle": 500, "Greenlink": 500,
}

_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(timeout=30, follow_redirects=True)
    return _client


def _get(endpoint: str, params: dict | None = None) -> Any:
    """Make a GET request to BMRS API. Returns parsed JSON."""
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

    Returns DataFrame: timestamp, settlement_period, bmrs_fuel, fuel_type, generation_mw

    NOTE: ICs in this endpoint are import-only (never negative).
    Use fetch_interconnector_flows() for bidirectional IC data.
    """
    params: dict[str, str] = {}
    if from_date:
        params["from"] = from_date.isoformat()
    if to_date:
        params["to"] = to_date.isoformat()

    data = _get("/generation/outturn/summary", params)
    records = data if isinstance(data, list) else data.get("data", [])

    rows = []
    for period in records:
        ts = period.get("startTime")
        sp = period.get("settlementPeriod")
        for fuel in period.get("data", []):
            ft_raw = fuel["fuelType"]
            rows.append({
                "timestamp": pd.Timestamp(ts),
                "settlement_period": sp,
                "bmrs_fuel": ft_raw,
                "fuel_type": BMRS_FUEL_MAP.get(ft_raw, ft_raw.lower()),
                "generation_mw": fuel["generation"],
                "is_ic": ft_raw.startswith("INT"),
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

    INDO = Initial National Demand Outturn
         = total generation - station self-consumption - exports + imports
         = what consumers actually use (approximately)

    ITSDO = Initial Transmission System Demand Outturn
          = what the transmission network sees (excludes embedded generation)
          = always higher than INDO because embedded gen is invisible

    For a system dashboard, ITSDO is more comparable to transmission-metered
    generation, while INDO is the "real" demand metric.
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


# ─── System prices (SSP / SBP) ──────────────────────────────────────────────

def fetch_system_prices(
    settlement_date: date | None = None,
) -> pd.DataFrame:
    """
    Fetch System Sell Price (SSP) and System Buy Price (SBP) per settlement period.

    SSP = price paid to generators for surplus energy (when system is long)
    SBP = price charged for deficit energy (when system is short)

    When SSP = SBP, the system was in balance (no balancing actions needed).

    These are NOT day-ahead or wholesale prices. They are imbalance settlement
    prices. Day-ahead (EPEX/N2EX) is not freely available via BMRS API.

    Returns DataFrame: timestamp, settlement_period, ssp_gbp_mwh, sbp_gbp_mwh, niv_mw
    """
    target = settlement_date or date.today()
    yesterday = target - timedelta(days=1)

    all_rows = []
    for d in [yesterday, target]:
        try:
            data = _get(f"/balancing/settlement/system-prices/{d.isoformat()}")
            records = data if isinstance(data, list) else data.get("data", [])
            for rec in records:
                all_rows.append({
                    "timestamp": pd.Timestamp(rec.get("startTime")),
                    "settlement_date": rec.get("settlementDate"),
                    "settlement_period": rec.get("settlementPeriod"),
                    "ssp_gbp_mwh": rec.get("systemSellPrice", 0),
                    "sbp_gbp_mwh": rec.get("systemBuyPrice", 0),
                    "niv_mw": rec.get("netImbalanceVolume", 0),
                })
        except httpx.HTTPStatusError:
            logger.warning(f"No system prices for {d}")

    df = pd.DataFrame(all_rows)
    if not df.empty:
        df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"])
    logger.info(f"Fetched system prices: {len(df)} records")
    return df


# ─── Interconnector flows (bidirectional) ────────────────────────────────────

def fetch_interconnector_flows(
    from_date: date | None = None,
    to_date: date | None = None,
) -> pd.DataFrame:
    """
    Fetch half-hourly interconnector flows from dedicated endpoint.

    IMPORTANT: This is the ONLY endpoint with bidirectional data.
    Positive = import to GB, Negative = export from GB.

    The generation summary endpoint only shows imports (>=0).

    Returns DataFrame: timestamp, settlement_period, ic_name, ic_raw_name, flow_mw
    """
    params: dict[str, str] = {}
    if from_date:
        params["from"] = from_date.isoformat()
    if to_date:
        params["to"] = to_date.isoformat()

    data = _get("/generation/outturn/interconnectors", params)
    records = data if isinstance(data, list) else data.get("data", [])

    rows = []
    for rec in records:
        raw_name = rec.get("interconnectorName", "")
        canonical = IC_NAME_MAP.get(raw_name, raw_name)
        rows.append({
            "timestamp": pd.Timestamp(rec.get("startTime")),
            "settlement_period": rec.get("settlementPeriod"),
            "ic_name": canonical,
            "ic_raw_name": raw_name,
            "flow_mw": rec.get("generation", 0),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("timestamp")
    logger.info(f"Fetched IC flows: {len(df)} records, {df['ic_name'].nunique() if not df.empty else 0} interconnectors")
    return df


# ─── Convenience ─────────────────────────────────────────────────────────────

def fetch_all(
    from_date: date | None = None,
    to_date: date | None = None,
) -> dict[str, pd.DataFrame]:
    """Fetch all time-series data. Each endpoint is independent (one failure doesn't block others)."""
    results: dict[str, pd.DataFrame] = {}

    for name, fn, kwargs in [
        ("generation", fetch_generation_mix, {"from_date": from_date, "to_date": to_date}),
        ("demand", fetch_demand, {"from_date": from_date, "to_date": to_date}),
        ("prices", fetch_system_prices, {"settlement_date": to_date or date.today()}),
        ("interconnectors", fetch_interconnector_flows, {"from_date": from_date, "to_date": to_date}),
    ]:
        try:
            results[name] = fn(**kwargs)
        except Exception as e:
            logger.error(f"Failed to fetch {name}: {e}")
            results[name] = pd.DataFrame()

    return results
