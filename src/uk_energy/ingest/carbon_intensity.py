"""
carbon_intensity.py — Carbon Intensity API ingestion.

Endpoints:
  GET /regional    → 14 DNO regions with generation mix + intensity
  GET /generation  → National generation mix percentages

No API key required.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from loguru import logger

from uk_energy.config import CARBON_BASE, CARBON_GENERATION, CARBON_RAW, CARBON_REGIONAL
from uk_energy.ingest._http import RateLimitedClient


def _out(filename: str) -> Path:
    CARBON_RAW.mkdir(parents=True, exist_ok=True)
    return CARBON_RAW / filename


def fetch_regional(force: bool = False) -> Path:
    """
    Fetch regional carbon intensity and generation mix data.

    Returns 14 DNO regions each with:
      - regionid, dnoregion, shortname
      - intensity.forecast, intensity.index
      - generationmix[] with fuel and perc (percentage)
    """
    out = _out("regional.json")
    if out.exists() and not force:
        logger.info(f"Regional data already downloaded: {out}")
        return out

    logger.info("Fetching regional carbon intensity data...")
    with RateLimitedClient(rps=1.0) as client:
        try:
            response = client.get(
                CARBON_REGIONAL,
                headers={"Accept": "application/json"},
            )
            data = response.json()
            out.write_text(json.dumps(data, indent=2))
            regions = data.get("data", [])
            logger.success(f"Fetched {len(regions)} regions → {out}")
        except Exception as exc:
            logger.error(f"Failed to fetch regional data: {exc}")
            out.write_text(json.dumps({"status": "error", "error": str(exc)}))
    return out


def fetch_generation_mix(force: bool = False) -> Path:
    """
    Fetch national generation mix percentages.

    Returns fuel breakdown: gas, coal, nuclear, wind, hydro, solar, biomass, other, imports
    """
    out = _out("generation_mix.json")
    if out.exists() and not force:
        logger.info(f"Generation mix already downloaded: {out}")
        return out

    logger.info("Fetching national generation mix...")
    with RateLimitedClient(rps=1.0) as client:
        try:
            response = client.get(
                CARBON_GENERATION,
                headers={"Accept": "application/json"},
            )
            data = response.json()
            out.write_text(json.dumps(data, indent=2))
            logger.success(f"Fetched generation mix → {out}")
        except Exception as exc:
            logger.error(f"Failed to fetch generation mix: {exc}")
            out.write_text(json.dumps({"status": "error", "error": str(exc)}))
    return out


def parse_regional() -> pd.DataFrame:
    """
    Parse regional JSON into a DataFrame.

    Returns one row per (region, fuel) combination.
    """
    path = _out("regional.json")
    if not path.exists():
        fetch_regional()

    data = json.loads(path.read_text())
    rows: list[dict] = []

    regions_data = data.get("data", [])
    for region in regions_data:
        region_id = region.get("regionid")
        dno_region = region.get("dnoregion", "")
        shortname = region.get("shortname", "")
        intensity = region.get("intensity", {})
        forecast = intensity.get("forecast")
        index = intensity.get("index", "")

        for fuel_entry in region.get("generationmix", []):
            rows.append({
                "region_id": region_id,
                "dno_region": dno_region,
                "shortname": shortname,
                "intensity_forecast": forecast,
                "intensity_index": index,
                "fuel": fuel_entry.get("fuel", ""),
                "perc": fuel_entry.get("perc", 0.0),
            })

    df = pd.DataFrame(rows)
    logger.info(f"Parsed {len(df)} region-fuel rows from Carbon Intensity API")
    return df


def parse_generation_mix() -> pd.DataFrame:
    """Parse national generation mix JSON into a DataFrame."""
    path = _out("generation_mix.json")
    if not path.exists():
        fetch_generation_mix()

    data = json.loads(path.read_text())
    gen_data = data.get("data", {})
    if isinstance(gen_data, list):
        gen_data = gen_data[0] if gen_data else {}

    mix = gen_data.get("generationmix", [])
    df = pd.DataFrame(mix)
    logger.info(f"Parsed {len(df)} fuel types from national generation mix")
    return df


def ingest_all(force: bool = False) -> None:
    """Run all Carbon Intensity ingestion steps."""
    logger.info("=== Carbon Intensity Ingestion ===")
    fetch_regional(force=force)
    fetch_generation_mix(force=force)
    logger.success("Carbon Intensity ingestion complete")


if __name__ == "__main__":
    ingest_all()
