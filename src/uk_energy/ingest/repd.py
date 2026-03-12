"""
repd.py — Renewable Energy Planning Database (REPD) ingestion.

Downloads the latest REPD CSV from data.gov.uk and parses it into a
clean DataFrame with standardised column names and fuel type mapping.

~10,000+ rows, updated quarterly by DESNZ.
"""

from __future__ import annotations

import io
import re
from pathlib import Path

import pandas as pd
from loguru import logger

from uk_energy.config import PROCESSED_DIR, REPD_RAW
from uk_energy.ingest._http import RateLimitedClient

# Known-good REPD download URLs (try in order)
REPD_URLS: list[str] = [
    "https://assets.publishing.service.gov.uk/media/repd-q3-october-2023.csv",
    "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/repd-q3-october-2023.csv",
    "https://www.data.gov.uk/dataset/a5b0ed13-c960-49ce-b1f6-3a6bbe0db1b7/repd",
]

# The data.gov.uk CKAN API endpoint for REPD
REPD_CKAN_API = "https://www.data.gov.uk/api/3/action/package_show"
REPD_DATASET_ID = "a5b0ed13-c960-49ce-b1f6-3a6bbe0db1b7"


def _out(filename: str) -> Path:
    REPD_RAW.mkdir(parents=True, exist_ok=True)
    return REPD_RAW / filename


def _discover_repd_url(client: RateLimitedClient) -> str | None:
    """Use data.gov.uk CKAN API to find the latest REPD CSV URL."""
    try:
        response = client.get(
            REPD_CKAN_API,
            params={"id": REPD_DATASET_ID},
        )
        data = response.json()
        if not data.get("success"):
            return None
        resources = data["result"].get("resources", [])
        # Find the most recent CSV
        csv_resources = [
            r for r in resources
            if r.get("format", "").upper() in ("CSV", "TEXT/CSV")
            and r.get("url")
        ]
        if csv_resources:
            # Sort by last_modified descending
            csv_resources.sort(
                key=lambda r: r.get("last_modified") or r.get("created") or "",
                reverse=True,
            )
            url: str = csv_resources[0]["url"]
            logger.info(f"Discovered REPD URL via CKAN: {url}")
            return url
    except Exception as exc:
        logger.warning(f"CKAN discovery failed: {exc}")
    return None


def fetch_repd(force: bool = False) -> Path:
    """
    Download the latest REPD CSV from data.gov.uk.

    Returns path to the raw CSV file.
    """
    raw_path = _out("repd_raw.csv")
    if raw_path.exists() and not force:
        logger.info(f"REPD already downloaded: {raw_path}")
        return raw_path

    logger.info("Fetching REPD CSV from data.gov.uk...")
    with RateLimitedClient(rps=0.5, timeout=120) as client:
        # Try CKAN API discovery first
        url = _discover_repd_url(client)

        # Fall back to known URLs
        urls_to_try = ([url] if url else []) + REPD_URLS
        last_exc: Exception | None = None

        for try_url in urls_to_try:
            try:
                logger.info(f"Trying: {try_url}")
                response = client.get(try_url)
                content_type = response.headers.get("content-type", "")
                # Verify it looks like CSV (not HTML redirect)
                text = response.text
                if len(text) > 1000 and ("Site Name" in text or "Technology Type" in text or ",," in text):
                    raw_path.write_text(text, encoding="utf-8")
                    logger.success(f"Downloaded REPD ({len(text):,} chars) → {raw_path}")
                    return raw_path
                else:
                    logger.warning(f"Response from {try_url} doesn't look like REPD CSV")
            except Exception as exc:
                last_exc = exc
                logger.warning(f"Failed {try_url}: {exc}")
                continue

        # If all fail, save a placeholder
        logger.error(f"Could not download REPD CSV. Last error: {last_exc}")
        raw_path.write_text("# REPD download failed - run again or download manually\n")

    return raw_path


def parse_repd(raw_path: Path | None = None) -> pd.DataFrame:
    """
    Parse the raw REPD CSV into a clean DataFrame.

    Standardises column names, maps fuel types, handles encoding issues.
    """
    if raw_path is None:
        raw_path = _out("repd_raw.csv")

    if not raw_path.exists():
        logger.warning("REPD raw file not found, fetching...")
        fetch_repd()

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

    # Normalise column names: strip, lowercase, replace spaces/special with _
    df.columns = pd.Index([
        re.sub(r"[^a-z0-9]+", "_", c.strip().lower()).strip("_")
        for c in df.columns
    ])

    # Standard column mapping (REPD column names vary between versions)
    col_map: dict[str, str] = {
        "site_name": "name",
        "technology_type": "technology",
        "installed_capacity_mwelec": "capacity_mw",
        "x_co_ord": "easting",
        "y_co_ord": "northing",
        "latitude": "lat",
        "longitude": "lon",
        "development_status": "status",
        "operator_or_applicant": "developer",
        "local_planning_authority": "planning_authority",
        "region": "region",
        "planning_application_submitted": "planning_submitted",
        "under_construction": "under_construction",
        "operational": "operational",
        "country": "country",
        "ref_id": "repd_id",
    }
    # Only rename columns that actually exist
    rename = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename)

    # Try to coerce capacity to numeric
    if "capacity_mw" in df.columns:
        df["capacity_mw"] = pd.to_numeric(df["capacity_mw"], errors="coerce")

    # Normalise status
    if "status" in df.columns:
        status_map = {
            "operational": "operational",
            "under construction": "construction",
            "awaiting construction": "consented",
            "planning permission expired": "planning",
            "planning refused": "refused",
            "application submitted": "planning",
            "application withdrawn": "withdrawn",
            "scoping stage": "planning",
            "pre-application stage": "planning",
            "decommissioned": "decommissioned",
        }
        df["status"] = df["status"].str.lower().str.strip().map(
            lambda x: next((v for k, v in status_map.items() if k in str(x).lower()), "unknown")
        )

    # Save processed
    out_path = PROCESSED_DIR / "repd_processed.csv"
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    logger.success(f"Saved processed REPD ({len(df)} rows) → {out_path}")
    return df


def ingest_all(force: bool = False) -> pd.DataFrame:
    """Run REPD ingestion and parsing."""
    logger.info("=== REPD Ingestion ===")
    fetch_repd(force=force)
    df = parse_repd()
    logger.success("REPD ingestion complete")
    return df


if __name__ == "__main__":
    ingest_all()
