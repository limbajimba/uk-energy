"""
bmrs.py — Elexon BMRS data ingestion.

Sources:
  - BM Unit reference list (all registered BM units)
  - B1610 — Actual Generation Output per Generation Unit
  - B1620 — Actual Aggregated Generation per Type

No API key required. Rate limit: 1 req/sec.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from loguru import logger

from uk_energy.config import (
    BMRS_B1610,
    BMRS_B1620,
    BMRS_BMUNITS_ALL,
    BMRS_RATE_LIMIT_RPS,
    BMRS_RAW,
)
from uk_energy.ingest._http import RateLimitedClient


def _out(filename: str) -> Path:
    BMRS_RAW.mkdir(parents=True, exist_ok=True)
    return BMRS_RAW / filename


def fetch_bm_units(force: bool = False) -> Path:
    """
    Fetch the complete list of all BM Units from BMRS reference API.

    Endpoint: GET /reference/bmunits/all
    Returns JSON array of {bmUnit, elexonBmUnit, leadPartyName, ...}
    """
    out = _out("bm_units_all.json")
    if out.exists() and not force:
        logger.info(f"BM units already downloaded: {out}")
        return out

    logger.info("Fetching all BM units from BMRS...")
    with RateLimitedClient(rps=BMRS_RATE_LIMIT_RPS) as client:
        try:
            response = client.get(BMRS_BMUNITS_ALL, params={"format": "json"})
            data = response.json()
            out.write_text(json.dumps(data, indent=2))
            count = len(data) if isinstance(data, list) else "?"
            logger.success(f"Fetched {count} BM units → {out}")
        except Exception as exc:
            logger.error(f"Failed to fetch BM units: {exc}")
            raise
    return out


def fetch_b1610(
    settlement_date: datetime | None = None,
    period: int = 1,
    force: bool = False,
) -> Path:
    """
    Fetch B1610 — Actual Generation Output per Generation Unit.

    Args:
        settlement_date: Date to fetch (defaults to yesterday).
        period: Settlement period (1–50, 30-min intervals). 0 = all periods.
        force: Re-download even if file exists.
    """
    if settlement_date is None:
        settlement_date = datetime.now(tz=timezone.utc) - timedelta(days=1)

    date_str = settlement_date.strftime("%Y-%m-%d")
    out = _out(f"b1610_{date_str}.json")

    if out.exists() and not force:
        logger.info(f"B1610 already downloaded: {out}")
        return out

    logger.info(f"Fetching B1610 for {date_str} (period {period})...")
    params: dict[str, str | int] = {
        "settlementDate": date_str,
        "format": "json",
    }
    if period > 0:
        params["settlementPeriod"] = period

    with RateLimitedClient(rps=BMRS_RATE_LIMIT_RPS) as client:
        try:
            response = client.get(BMRS_B1610, params=params)
            data = response.json()
            out.write_text(json.dumps(data, indent=2))
            logger.success(f"Fetched B1610 {date_str} → {out}")
        except Exception as exc:
            logger.error(f"Failed to fetch B1610: {exc}")
            raise
    return out


def fetch_b1620(
    settlement_date: datetime | None = None,
    force: bool = False,
) -> Path:
    """
    Fetch B1620 — Actual Aggregated Generation per Type.

    Args:
        settlement_date: Date to fetch (defaults to yesterday).
        force: Re-download even if file exists.
    """
    if settlement_date is None:
        settlement_date = datetime.now(tz=timezone.utc) - timedelta(days=1)

    date_str = settlement_date.strftime("%Y-%m-%d")
    out = _out(f"b1620_{date_str}.json")

    if out.exists() and not force:
        logger.info(f"B1620 already downloaded: {out}")
        return out

    logger.info(f"Fetching B1620 for {date_str}...")
    params = {"settlementDate": date_str, "format": "json"}

    with RateLimitedClient(rps=BMRS_RATE_LIMIT_RPS) as client:
        try:
            response = client.get(BMRS_B1620, params=params)
            data = response.json()
            out.write_text(json.dumps(data, indent=2))
            logger.success(f"Fetched B1620 {date_str} → {out}")
        except Exception as exc:
            logger.warning(f"B1620 unavailable (endpoint may be deprecated): {exc}")
            # Save empty placeholder so we don't retry
            out.write_text(json.dumps({"status": "unavailable", "note": str(exc)}))
    return out


def load_bm_units() -> pd.DataFrame:
    """Load BM units JSON into a DataFrame."""
    path = _out("bm_units_all.json")
    if not path.exists():
        fetch_bm_units()
    with open(path) as f:
        data = json.load(f)
    # Handle both list and dict-with-data responses
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = data.get("data", data.get("results", [data]))
    else:
        rows = []
    df = pd.DataFrame(rows)
    logger.info(f"Loaded {len(df)} BM units")
    return df


def load_b1620(settlement_date: datetime | None = None) -> pd.DataFrame:
    """Load B1620 generation-by-fuel-type JSON into a DataFrame."""
    if settlement_date is None:
        settlement_date = datetime.now(tz=timezone.utc) - timedelta(days=1)
    date_str = settlement_date.strftime("%Y-%m-%d")
    path = _out(f"b1620_{date_str}.json")
    if not path.exists():
        fetch_b1620(settlement_date)
    with open(path) as f:
        data = json.load(f)
    if isinstance(data, dict):
        rows = data.get("data", [])
    else:
        rows = data
    df = pd.DataFrame(rows)
    logger.info(f"Loaded {len(df)} B1620 records for {date_str}")
    return df


def ingest_all(force: bool = False) -> None:
    """Run all BMRS ingestion steps."""
    logger.info("=== BMRS Ingestion ===")
    fetch_bm_units(force=force)
    fetch_b1610(force=force)
    fetch_b1620(force=force)
    logger.success("BMRS ingestion complete")


if __name__ == "__main__":
    ingest_all()
