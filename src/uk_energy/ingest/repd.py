"""
repd.py — Renewable Energy Planning Database (REPD) ingestion.

The REPD is published quarterly by DESNZ and lists every renewable energy
project in the UK with planning status, technology type, capacity, and
Ordnance Survey grid coordinates (OSGB36 / EPSG:27700).

Pipeline:
  1. Download CSV (try CKAN API discovery → known URLs → OSUKED mirror)
  2. Parse and standardise column names
  3. Convert OSGB36 eastings/northings → WGS84 lat/lon
  4. Map development statuses to canonical values
  5. Save processed CSV
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

from uk_energy.config import PROCESSED_DIR, REPD_RAW
from uk_energy.ingest._http import RateLimitedClient

# ─── Download URLs (in priority order) ─────────────────────────────────────

REPD_CKAN_API = "https://www.data.gov.uk/api/3/action/package_show"
REPD_DATASET_ID = "a5b0ed13-c960-49ce-b1f6-3a6bbe0db1b7"

# OSUKED mirror is the most reliable source (gov.uk URLs change quarterly)
OSUKED_REPD_URL = (
    "https://raw.githubusercontent.com/OSUKED/Power-Station-Dictionary/"
    "main/data/linked-datapackages/renewable-energy-planning-database/"
    "repd-january-2023.csv"
)

REPD_FALLBACK_URLS: list[str] = [
    "https://assets.publishing.service.gov.uk/media/repd-q3-october-2023.csv",
    OSUKED_REPD_URL,
]

# ─── Status mapping ─────────────────────────────────────────────────────────

STATUS_MAP: dict[str, str] = {
    "operational": "operational",
    "under construction": "construction",
    "awaiting construction": "consented",
    "planning application submitted": "planning",
    "planning permission granted": "consented",
    "planning permission refused": "refused",
    "planning permission expired": "expired",
    "planning application withdrawn": "withdrawn",
    "appeal lodged": "planning",
    "appeal granted": "consented",
    "appeal refused": "refused",
    "appeal withdrawn": "withdrawn",
    "revised": "planning",
    "abandoned": "withdrawn",
    "no application required": "consented",
    "scoping": "planning",
    "decommissioned": "decommissioned",
}


def _out(filename: str) -> Path:
    REPD_RAW.mkdir(parents=True, exist_ok=True)
    return REPD_RAW / filename


def _discover_repd_url(client: RateLimitedClient) -> str | None:
    """Use data.gov.uk CKAN API to find the latest REPD CSV URL."""
    try:
        response = client.get(REPD_CKAN_API, params={"id": REPD_DATASET_ID})
        data = response.json()
        if not data.get("success"):
            return None
        resources = data["result"].get("resources", [])
        csv_resources = [
            r for r in resources
            if r.get("format", "").upper() in ("CSV", "TEXT/CSV") and r.get("url")
        ]
        if csv_resources:
            csv_resources.sort(
                key=lambda r: r.get("last_modified") or r.get("created") or "",
                reverse=True,
            )
            url = csv_resources[0]["url"]
            logger.info(f"Discovered REPD URL via CKAN: {url}")
            return url
    except Exception as exc:
        logger.debug(f"CKAN discovery failed: {exc}")
    return None


def _convert_osgb36_to_wgs84(
    df: pd.DataFrame,
    easting_col: str,
    northing_col: str,
) -> pd.DataFrame:
    """
    Convert OSGB36 (EPSG:27700) eastings/northings to WGS84 lat/lon.

    Adds 'lat' and 'lon' columns. Handles dirty data (non-numeric chars,
    out-of-bounds values).
    """
    try:
        from pyproj import Transformer
    except ImportError:
        logger.warning("pyproj not installed — coordinates will not be converted")
        df["lat"] = np.nan
        df["lon"] = np.nan
        return df

    transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326", always_xy=True)

    # Clean coordinate columns: strip non-numeric chars, convert to float
    for col in [easting_col, northing_col]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r"[^\d.]", "", regex=True)
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Valid UK OSGB36 bounds: eastings 0-700000, northings 0-1300000
    has_coords = (
        df[easting_col].notna()
        & df[northing_col].notna()
        & df[easting_col].between(0, 700_000)
        & df[northing_col].between(0, 1_300_000)
    )

    df["lat"] = np.nan
    df["lon"] = np.nan

    if has_coords.any():
        lons, lats = transformer.transform(
            df.loc[has_coords, easting_col].values,
            df.loc[has_coords, northing_col].values,
        )
        df.loc[has_coords, "lat"] = lats
        df.loc[has_coords, "lon"] = lons
        logger.info(f"Converted {has_coords.sum():,} coordinates from OSGB36 → WGS84")
    else:
        logger.warning("No valid OSGB36 coordinates found for conversion")

    return df


def _map_status(raw: str) -> str:
    """Map a raw REPD status string to a canonical value."""
    if not raw or not isinstance(raw, str):
        return "unknown"
    raw_lower = raw.lower().strip()
    # Exact match
    if raw_lower in STATUS_MAP:
        return STATUS_MAP[raw_lower]
    # Partial match
    for key, value in STATUS_MAP.items():
        if key in raw_lower:
            return value
    return "unknown"


def fetch_repd(force: bool = False) -> Path:
    """Download the latest REPD CSV."""
    raw_path = _out("repd_raw.csv")
    if raw_path.exists() and raw_path.stat().st_size > 1000 and not force:
        logger.info(f"REPD already downloaded: {raw_path}")
        return raw_path

    logger.info("Fetching REPD CSV...")
    with RateLimitedClient(rps=0.5, timeout=120) as client:
        url = _discover_repd_url(client)
        urls_to_try = ([url] if url else []) + REPD_FALLBACK_URLS

        for try_url in urls_to_try:
            try:
                logger.info(f"Trying: {try_url}")
                response = client.get(try_url)
                text = response.text
                # Validate: CSV should have common REPD headers
                if len(text) > 5000 and any(
                    h in text for h in ("Site Name", "Technology Type", "Ref ID")
                ):
                    raw_path.write_text(text, encoding="utf-8")
                    lines = text.count("\n")
                    logger.success(f"Downloaded REPD ({lines:,} rows) → {raw_path}")
                    return raw_path
                logger.warning(f"Response from {try_url} doesn't look like REPD CSV")
            except Exception as exc:
                logger.warning(f"Failed {try_url}: {exc}")

        logger.error("Could not download REPD CSV from any source")
        raw_path.write_text("# REPD download failed\n")

    return raw_path


def parse_repd(raw_path: Path | None = None) -> pd.DataFrame:
    """
    Parse raw REPD CSV → standardised DataFrame with WGS84 coordinates.

    Handles:
      - Multiple CSV encodings
      - Variable column naming across REPD versions
      - OSGB36 → WGS84 coordinate conversion
      - Status normalisation
    """
    if raw_path is None:
        raw_path = _out("repd_raw.csv")
    if not raw_path.exists() or raw_path.stat().st_size < 1000:
        logger.warning("No valid REPD raw file, fetching...")
        fetch_repd(force=True)

    logger.info(f"Parsing REPD from {raw_path}...")

    # Try multiple encodings
    df: pd.DataFrame | None = None
    for encoding in ("utf-8", "latin-1", "cp1252"):
        try:
            df = pd.read_csv(raw_path, encoding=encoding, low_memory=False)
            break
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue

    if df is None or df.empty:
        logger.error("Could not parse REPD CSV")
        return pd.DataFrame()

    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")

    # Normalise column names
    df.columns = pd.Index([
        re.sub(r"[^a-z0-9]+", "_", c.strip().lower()).strip("_")
        for c in df.columns
    ])

    # Find coordinate columns (names vary between REPD versions)
    easting_col = next(
        (c for c in df.columns if "x_coord" in c or c == "easting"),
        None,
    )
    northing_col = next(
        (c for c in df.columns if "y_coord" in c or c == "northing"),
        None,
    )

    # Convert OSGB36 → WGS84
    if easting_col and northing_col:
        df = _convert_osgb36_to_wgs84(df, easting_col, northing_col)
    else:
        logger.warning(f"No coordinate columns found (have: {list(df.columns)[:10]})")
        df["lat"] = np.nan
        df["lon"] = np.nan

    # Map status
    status_col = next(
        (c for c in df.columns if c in ("development_status", "status")),
        None,
    )
    dev_short_col = next(
        (c for c in df.columns if "development_status_short" in c),
        None,
    )

    if status_col:
        df["status"] = df[status_col].apply(_map_status)
    elif dev_short_col:
        df["status"] = df[dev_short_col].apply(_map_status)
    else:
        df["status"] = "unknown"

    # Rename key columns
    rename_map = {
        "ref_id": "repd_id",
        "site_name": "name",
        "technology_type": "technology",
        "installed_capacity_mwelec": "capacity_mw",
        "operator_or_applicant": "developer",
        "region": "region",
        "country": "country",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    if "capacity_mw" in df.columns:
        df["capacity_mw"] = pd.to_numeric(df["capacity_mw"], errors="coerce")

    # Save processed
    out_path = PROCESSED_DIR / "repd_processed.csv"
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    with_coords = df["lat"].notna().sum()
    logger.success(
        f"Processed REPD: {len(df)} projects, {with_coords} with coordinates → {out_path}"
    )
    return df


def ingest_all(force: bool = False) -> pd.DataFrame:
    """Run REPD ingestion pipeline."""
    logger.info("=== REPD Ingestion ===")
    fetch_repd(force=force)
    return parse_repd()


if __name__ == "__main__":
    ingest_all()
