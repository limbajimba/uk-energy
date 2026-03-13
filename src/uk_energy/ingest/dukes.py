"""
dukes.py — DUKES (Digest of UK Energy Statistics) Chapter 5 ingestion.

Downloads and parses DUKES Table 5.11: "Power stations in the United Kingdom"
from the DESNZ gov.uk statistics page.

The Excel file has a known structure:
  - Sheet "5.11 Full list" contains the actual power station data
  - Header row: Company Name, Site Name, Technology, Type, CHP, Primary Fuel,
    Secondary Fuel, InstalledCapacity (MW), Grid Connection Type, Country,
    Region, Postcode, OS Reference, X-Coordinate, Y-Coordinate,
    Year Commissioned, DESNZ site code

Coordinates are OSGB36 (EPSG:27700) — we convert to WGS84.
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

from uk_energy.config import DUKES_RAW, PROCESSED_DIR
from uk_energy.ingest._http import RateLimitedClient

# Known DUKES download URLs (updated annually by DESNZ)
DUKES_GOV_PAGE = (
    "https://www.gov.uk/government/statistics/"
    "electricity-chapter-5-digest-of-united-kingdom-energy-statistics-dukes"
)

# The table we want: 5.11 Major power stations
TARGET_SHEET = "5.11 Full list"

# The header row in 5.11 starts with "Company Name" (0-indexed row 5)
EXPECTED_HEADER_ROW = 5

# Canonical column mapping from DUKES headers
COLUMN_MAP: dict[str, str] = {
    "Company Name [note 30]": "company",
    "Company Name": "company",
    "Site Name": "name",
    "Technology": "technology",
    "Type": "type",
    "CHP": "chp",
    "Primary Fuel": "fuel",
    "Secondary Fuel": "secondary_fuel",
    "InstalledCapacity (MW)": "capacity_mw",
    "Installed Capacity (MW)": "capacity_mw",
    "Grid Connection Type": "grid_connection",
    "Country": "country",
    "Region": "region",
    "Postcode": "postcode",
    "OS Reference": "os_reference",
    "X-Coordinate": "easting",
    "Y-Coordinate": "northing",
    "Year Commissioned": "year_commissioned",
    "DESNZ site code": "dukes_id",
}


def _out(filename: str) -> Path:
    DUKES_RAW.mkdir(parents=True, exist_ok=True)
    return DUKES_RAW / filename


def _scrape_download_links(client: RateLimitedClient) -> list[dict[str, str]]:
    """Scrape the DUKES gov.uk page for current Excel download links."""
    links: list[dict[str, str]] = []
    try:
        response = client.get(DUKES_GOV_PAGE)
        pattern = r'href=["\']([^"\']*(?:dukes|5[\._]11)[^"\']*\.xlsx)["\']'
        matches = re.findall(pattern, response.text, re.IGNORECASE)
        for m in matches:
            url = m if m.startswith("http") else f"https://www.gov.uk{m}"
            fname = url.split("/")[-1]
            links.append({"name": fname.replace(".xlsx", ""), "url": url})
        logger.info(f"Found {len(links)} DUKES Excel links on gov.uk")
    except Exception as exc:
        logger.warning(f"Could not scrape DUKES page: {exc}")
    return links


def fetch_dukes(force: bool = False) -> list[Path]:
    """Download DUKES Excel files from gov.uk."""
    existing = list(DUKES_RAW.glob("*.xlsx")) if DUKES_RAW.exists() else []
    if existing and not force:
        logger.info(f"DUKES already downloaded: {[p.name for p in existing]}")
        return existing

    logger.info("Fetching DUKES Chapter 5 Excel files...")
    downloaded: list[Path] = []

    with RateLimitedClient(rps=0.3, timeout=120) as client:
        links = _scrape_download_links(client)

        seen: set[str] = set()
        for item in links:
            url = item["url"]
            if url in seen:
                continue
            seen.add(url)

            name = item.get("name", url.split("/")[-1].replace(".xlsx", ""))
            out = _out(f"{name}.xlsx")
            if out.exists() and not force:
                downloaded.append(out)
                continue

            try:
                logger.info(f"Downloading: {url}")
                response = client.get(url)
                if len(response.content) > 10_000:
                    out.write_bytes(response.content)
                    logger.success(f"Downloaded {out.name} ({len(response.content):,} bytes)")
                    downloaded.append(out)
            except Exception as exc:
                logger.warning(f"Failed {url}: {exc}")

    if not downloaded:
        logger.warning("No DUKES files downloaded")
    return downloaded


def _find_511_sheet(path: Path) -> str | None:
    """Find the sheet name containing the 5.11 full list."""
    try:
        xl = pd.ExcelFile(path, engine="openpyxl")
        for name in xl.sheet_names:
            if "5.11" in name and ("full" in name.lower() or "list" in name.lower()):
                return name
            if name == TARGET_SHEET:
                return name
        # Fallback: any sheet with "5.11" in the name
        for name in xl.sheet_names:
            if "5.11" in name:
                return name
    except Exception as exc:
        logger.warning(f"Could not read sheet names from {path.name}: {exc}")
    return None


def _find_header_row(path: Path, sheet: str) -> int:
    """Find the row index where the actual data header starts."""
    try:
        raw = pd.read_excel(path, sheet_name=sheet, header=None, nrows=15, engine="openpyxl")
        for i, row in raw.iterrows():
            vals = [str(v).strip() for v in row if pd.notna(v)]
            if any("Site Name" in v for v in vals):
                return int(i)
    except Exception:
        pass
    return EXPECTED_HEADER_ROW


def _convert_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """Convert OSGB36 eastings/northings to WGS84 lat/lon."""
    if "easting" not in df.columns or "northing" not in df.columns:
        df["lat"] = np.nan
        df["lon"] = np.nan
        return df

    try:
        from pyproj import Transformer
    except ImportError:
        logger.warning("pyproj not installed — DUKES coordinates will not be converted")
        df["lat"] = np.nan
        df["lon"] = np.nan
        return df

    transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326", always_xy=True)

    df["easting"] = pd.to_numeric(df["easting"], errors="coerce")
    df["northing"] = pd.to_numeric(df["northing"], errors="coerce")

    has_coords = (
        df["easting"].notna()
        & df["northing"].notna()
        & df["easting"].between(0, 700_000)
        & df["northing"].between(0, 1_300_000)
    )

    df["lat"] = np.nan
    df["lon"] = np.nan

    if has_coords.any():
        lons, lats = transformer.transform(
            df.loc[has_coords, "easting"].values,
            df.loc[has_coords, "northing"].values,
        )
        df.loc[has_coords, "lat"] = lats
        df.loc[has_coords, "lon"] = lons
        logger.info(f"Converted {has_coords.sum():,} DUKES coordinates to WGS84")

    return df


def parse_dukes_511(path: Path) -> pd.DataFrame:
    """
    Parse DUKES Table 5.11 — the official UK power station register.

    Returns a DataFrame with standardised column names and WGS84 coordinates.
    """
    logger.info(f"Parsing DUKES 5.11 from {path.name}...")

    sheet = _find_511_sheet(path)
    if sheet is None:
        logger.warning(f"No 5.11 sheet found in {path.name}")
        return pd.DataFrame()

    header_row = _find_header_row(path, sheet)
    logger.debug(f"Using sheet '{sheet}', header row {header_row}")

    try:
        df = pd.read_excel(
            path,
            sheet_name=sheet,
            header=header_row,
            engine="openpyxl",
        )
    except Exception as exc:
        logger.error(f"Failed to read Excel: {exc}")
        return pd.DataFrame()

    # Clean column names and apply mapping
    df.columns = pd.Index([str(c).strip() for c in df.columns])
    rename = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    # Drop rows without a station name (footer rows, totals, etc.)
    if "name" in df.columns:
        df = df[df["name"].notna() & (df["name"].astype(str).str.strip() != "")]
    else:
        logger.warning("No 'name' column found after parsing")
        return pd.DataFrame()

    # Convert capacity to numeric
    if "capacity_mw" in df.columns:
        df["capacity_mw"] = pd.to_numeric(df["capacity_mw"], errors="coerce")

    # Convert coordinates
    df = _convert_coordinates(df)

    # All DUKES plants are operational (it only lists currently operating stations)
    df["status"] = "operational"
    df["source"] = "dukes_511"

    logger.success(
        f"Parsed {len(df)} power stations from DUKES 5.11 "
        f"({df['lat'].notna().sum()} with coordinates)"
    )
    return df


def ingest_all(force: bool = False) -> pd.DataFrame:
    """Run DUKES ingestion: download Excel files + parse 5.11."""
    logger.info("=== DUKES Ingestion ===")
    paths = fetch_dukes(force=force)

    all_dfs: list[pd.DataFrame] = []
    for path in paths:
        if not path.suffix == ".xlsx":
            continue
        # Check if this file contains 5.11
        sheet = _find_511_sheet(path)
        if sheet:
            df = parse_dukes_511(path)
            if not df.empty:
                all_dfs.append(df)

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        # Deduplicate by DESNZ site code
        if "dukes_id" in combined.columns:
            before = len(combined)
            combined = combined.drop_duplicates(subset="dukes_id", keep="first")
            dupes = before - len(combined)
            if dupes:
                logger.info(f"Removed {dupes} duplicate stations")

        out = PROCESSED_DIR / "dukes_processed.csv"
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        combined.to_csv(out, index=False)
        logger.success(f"Saved DUKES data ({len(combined)} stations) → {out}")
        return combined

    logger.warning("No DUKES data parsed")
    return pd.DataFrame()


if __name__ == "__main__":
    ingest_all()
