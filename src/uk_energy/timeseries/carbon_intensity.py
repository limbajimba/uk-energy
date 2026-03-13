"""
carbon_intensity.py — Carbon Intensity API client.

Supplements BMRS with:
  - Solar generation (% of mix)
  - Regional generation breakdown (18 regions)
  - Carbon intensity (gCO2/kWh)

API docs: https://carbon-intensity.github.io/api-definitions/
Rate limit: No key needed. Be reasonable.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import httpx
import pandas as pd
from loguru import logger

BASE_URL = "https://api.carbonintensity.org.uk"

_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(timeout=15, follow_redirects=True)
    return _client


def _get(endpoint: str) -> Any:
    url = f"{BASE_URL}{endpoint}"
    r = _get_client().get(url)
    r.raise_for_status()
    return r.json()


def fetch_current_mix() -> dict[str, float]:
    """Current national generation mix as {fuel: percentage}."""
    data = _get("/generation")
    mix = {}
    for g in data.get("data", {}).get("generationmix", []):
        mix[g["fuel"]] = g["perc"]
    return mix


def fetch_generation_24h() -> pd.DataFrame:
    """Half-hourly generation mix for the last 24h (percentages)."""
    yesterday = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%MZ")
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%MZ")

    data = _get(f"/generation/{yesterday}/{now}")
    rows = []
    for period in data.get("data", []):
        ts = period.get("from")
        for g in period.get("generationmix", []):
            rows.append({
                "timestamp": pd.Timestamp(ts),
                "fuel": g["fuel"],
                "percentage": g["perc"],
            })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("timestamp")
    logger.info(f"Carbon Intensity 24h: {len(df)} records, {df['timestamp'].nunique()} periods")
    return df


def fetch_intensity() -> dict[str, Any]:
    """Current carbon intensity."""
    data = _get("/intensity")
    d = data["data"][0]
    return {
        "forecast_gco2": d["intensity"]["forecast"],
        "actual_gco2": d["intensity"].get("actual"),
        "index": d["intensity"]["index"],
    }


def fetch_regional_mix() -> pd.DataFrame:
    """Current generation mix by region (18 DNO regions)."""
    data = _get("/regional")
    rows = []
    for region in data.get("data", [{}])[0].get("regions", []):
        region_name = region.get("shortname", region.get("dnoregion", ""))
        region_id = region.get("regionid")
        for g in region.get("generationmix", []):
            rows.append({
                "region_id": region_id,
                "region": region_name,
                "fuel": g["fuel"],
                "percentage": g["perc"],
            })

    df = pd.DataFrame(rows)
    logger.info(f"Regional mix: {len(df)} records, {df['region'].nunique()} regions")
    return df
