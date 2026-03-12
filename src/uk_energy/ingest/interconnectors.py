"""
interconnectors.py — UK electrical interconnectors reference data + live flows.

Creates a comprehensive static reference file with all 10 current UK
interconnectors, including precise lat/lon for both endpoints.

Also fetches live interconnector flows from BMRS.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from loguru import logger

from uk_energy.config import (
    BMRS_BASE,
    BMRS_RATE_LIMIT_RPS,
    INTERCONNECTORS_REF,
    RAW_DIR,
)
from uk_energy.ingest._http import RateLimitedClient

# ─── Static Reference Data ────────────────────────────────────────────────────

INTERCONNECTORS: list[dict] = [
    {
        "id": "IFA",
        "name": "IFA (Interconnexion France–Angleterre)",
        "countries": ["GB", "FR"],
        "capacity_mw": 2000,
        "type": "HVDC",
        "cable_type": "subsea",
        "status": "operational",
        "commissioned_year": 1986,
        "operators": ["National Grid", "RTE"],
        "gb_terminal": {
            "name": "Sellindge",
            "location": "Kent, England",
            "lat": 51.1005,
            "lon": 0.9889,
        },
        "foreign_terminal": {
            "name": "Les Mandarins, Calais",
            "location": "Pas-de-Calais, France",
            "lat": 50.9213,
            "lon": 1.8627,
        },
        "length_km": 73,
        "notes": "Oldest UK interconnector, runs under the English Channel",
    },
    {
        "id": "IFA2",
        "name": "IFA2",
        "countries": ["GB", "FR"],
        "capacity_mw": 1000,
        "type": "HVDC",
        "cable_type": "subsea",
        "status": "operational",
        "commissioned_year": 2021,
        "operators": ["National Grid", "RTE"],
        "gb_terminal": {
            "name": "Chilling",
            "location": "Fareham, Hampshire, England",
            "lat": 50.8278,
            "lon": -1.2722,
        },
        "foreign_terminal": {
            "name": "Tourbe, Caen",
            "location": "Normandy, France",
            "lat": 49.1829,
            "lon": -0.3707,
        },
        "length_km": 240,
        "notes": "Second France-GB interconnector",
    },
    {
        "id": "BritNed",
        "name": "BritNed",
        "countries": ["GB", "NL"],
        "capacity_mw": 1000,
        "type": "HVDC",
        "cable_type": "subsea",
        "status": "operational",
        "commissioned_year": 2011,
        "operators": ["National Grid", "TenneT"],
        "gb_terminal": {
            "name": "Isle of Grain",
            "location": "Kent, England",
            "lat": 51.4467,
            "lon": 0.7178,
        },
        "foreign_terminal": {
            "name": "Maasvlakte",
            "location": "Rotterdam, Netherlands",
            "lat": 51.9503,
            "lon": 4.0046,
        },
        "length_km": 260,
        "notes": "Connects GB to Netherlands",
    },
    {
        "id": "NemoLink",
        "name": "Nemo Link",
        "countries": ["GB", "BE"],
        "capacity_mw": 1000,
        "type": "HVDC",
        "cable_type": "subsea",
        "status": "operational",
        "commissioned_year": 2019,
        "operators": ["National Grid", "Elia"],
        "gb_terminal": {
            "name": "Richborough",
            "location": "Kent, England",
            "lat": 51.3053,
            "lon": 1.3509,
        },
        "foreign_terminal": {
            "name": "Herdersbrug, Bruges",
            "location": "West Flanders, Belgium",
            "lat": 51.2194,
            "lon": 3.1975,
        },
        "length_km": 140,
        "notes": "First UK-Belgium interconnector",
    },
    {
        "id": "NSL",
        "name": "North Sea Link (NSL)",
        "countries": ["GB", "NO"],
        "capacity_mw": 1400,
        "type": "HVDC",
        "cable_type": "subsea",
        "status": "operational",
        "commissioned_year": 2021,
        "operators": ["National Grid", "Statnett"],
        "gb_terminal": {
            "name": "Blyth",
            "location": "Northumberland, England",
            "lat": 55.1253,
            "lon": -1.5094,
        },
        "foreign_terminal": {
            "name": "Kvilldal",
            "location": "Rogaland, Norway",
            "lat": 59.5853,
            "lon": 6.5289,
        },
        "length_km": 720,
        "notes": "World's longest subsea power cable at commissioning",
    },
    {
        "id": "Moyle",
        "name": "Moyle Interconnector",
        "countries": ["GB", "NIR"],
        "capacity_mw": 500,
        "type": "HVDC",
        "cable_type": "subsea",
        "status": "operational",
        "commissioned_year": 2001,
        "operators": ["SONI", "SP Manweb"],
        "gb_terminal": {
            "name": "Auchencrosh",
            "location": "South Ayrshire, Scotland",
            "lat": 55.1167,
            "lon": -4.9667,
        },
        "foreign_terminal": {
            "name": "Ballycronan More",
            "location": "Antrim, Northern Ireland",
            "lat": 54.8667,
            "lon": -5.7667,
        },
        "length_km": 63,
        "notes": "Connects GB to Northern Ireland",
    },
    {
        "id": "EWIC",
        "name": "East West Interconnector (EWIC)",
        "countries": ["GB", "IE"],
        "capacity_mw": 500,
        "type": "HVDC",
        "cable_type": "subsea",
        "status": "operational",
        "commissioned_year": 2012,
        "operators": ["National Grid", "EirGrid"],
        "gb_terminal": {
            "name": "Shotton",
            "location": "Flintshire, Wales",
            "lat": 53.2127,
            "lon": -3.0457,
        },
        "foreign_terminal": {
            "name": "Woodland",
            "location": "County Meath, Ireland",
            "lat": 53.5667,
            "lon": -6.6833,
        },
        "length_km": 261,
        "notes": "Connects GB (Wales) to Republic of Ireland",
    },
    {
        "id": "ElecLink",
        "name": "ElecLink",
        "countries": ["GB", "FR"],
        "capacity_mw": 1000,
        "type": "HVDC",
        "cable_type": "tunnel",
        "status": "operational",
        "commissioned_year": 2022,
        "operators": ["ElecLink Ltd"],
        "gb_terminal": {
            "name": "Folkestone",
            "location": "Kent, England",
            "lat": 51.0775,
            "lon": 1.1699,
        },
        "foreign_terminal": {
            "name": "Coquelles",
            "location": "Pas-de-Calais, France",
            "lat": 50.9289,
            "lon": 1.8267,
        },
        "length_km": 51,
        "notes": "Runs through the Channel Tunnel (Eurotunnel)",
    },
    {
        "id": "VikingLink",
        "name": "Viking Link",
        "countries": ["GB", "DK"],
        "capacity_mw": 1400,
        "type": "HVDC",
        "cable_type": "subsea",
        "status": "operational",
        "commissioned_year": 2023,
        "operators": ["National Grid", "Energinet"],
        "gb_terminal": {
            "name": "Bicker Fen",
            "location": "Lincolnshire, England",
            "lat": 52.9736,
            "lon": -0.2281,
        },
        "foreign_terminal": {
            "name": "Revsing",
            "location": "South Jutland, Denmark",
            "lat": 55.3686,
            "lon": 9.1514,
        },
        "length_km": 765,
        "notes": "World's longest subsea power cable, connects Lincolnshire to Denmark",
    },
    {
        "id": "Greenlink",
        "name": "Greenlink",
        "countries": ["GB", "IE"],
        "capacity_mw": 500,
        "type": "HVDC",
        "cable_type": "subsea",
        "status": "operational",
        "commissioned_year": 2024,
        "operators": ["Green Interconnection Group"],
        "gb_terminal": {
            "name": "Pembroke",
            "location": "Pembrokeshire, Wales",
            "lat": 51.6747,
            "lon": -5.0211,
        },
        "foreign_terminal": {
            "name": "Great Island",
            "location": "County Wexford, Ireland",
            "lat": 52.3167,
            "lon": -6.9167,
        },
        "length_km": 190,
        "notes": "Second Wales-Ireland interconnector",
    },
]


def create_interconnector_reference(force: bool = False) -> Path:
    """
    Write the static interconnector reference JSON file.
    """
    INTERCONNECTORS_REF.parent.mkdir(parents=True, exist_ok=True)

    if INTERCONNECTORS_REF.exists() and not force:
        logger.info(f"Interconnector reference already exists: {INTERCONNECTORS_REF}")
        return INTERCONNECTORS_REF

    payload = {
        "metadata": {
            "description": "UK electrical interconnectors — all operational links",
            "total_capacity_mw": sum(ic["capacity_mw"] for ic in INTERCONNECTORS),
            "count": len(INTERCONNECTORS),
            "last_updated": datetime.now(tz=timezone.utc).isoformat(),
        },
        "interconnectors": INTERCONNECTORS,
    }
    INTERCONNECTORS_REF.write_text(json.dumps(payload, indent=2))
    total_mw = payload["metadata"]["total_capacity_mw"]
    logger.success(
        f"Created interconnector reference: {len(INTERCONNECTORS)} links, "
        f"{total_mw:,} MW total capacity → {INTERCONNECTORS_REF}"
    )
    return INTERCONNECTORS_REF


def fetch_live_flows(force: bool = False) -> Path:
    """
    Fetch live interconnector flow data from BMRS.

    Uses the INTERFUELHH dataset which includes interconnector flows.
    """
    out_dir = RAW_DIR / "bmrs"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "interconnector_flows.json"

    if out.exists() and not force:
        logger.info(f"Interconnector flows already downloaded: {out}")
        return out

    logger.info("Fetching live interconnector flows from BMRS...")
    yesterday = (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    endpoints_to_try = [
        f"{BMRS_BASE}/datasets/INTERFUELHH",
        f"{BMRS_BASE}/datasets/B1630",  # Cross-border physical flow
    ]

    with RateLimitedClient(rps=BMRS_RATE_LIMIT_RPS) as client:
        for endpoint in endpoints_to_try:
            try:
                response = client.get(
                    endpoint,
                    params={"settlementDate": yesterday, "format": "json"},
                )
                data = response.json()
                out.write_text(json.dumps(data, indent=2))
                logger.success(f"Fetched interconnector flows → {out}")
                return out
            except Exception as exc:
                logger.warning(f"Failed {endpoint}: {exc}")

    logger.warning("Could not fetch live interconnector flows")
    out.write_text(json.dumps({"status": "unavailable", "note": "fetch failed"}))
    return out


def ingest_all(force: bool = False) -> None:
    """Create reference data and fetch live flows."""
    logger.info("=== Interconnectors Ingestion ===")
    create_interconnector_reference(force=force)
    fetch_live_flows(force=force)
    logger.success("Interconnectors ingestion complete")


if __name__ == "__main__":
    ingest_all()
