"""
dukes.py — DUKES (Digest of UK Energy Statistics) Chapter 5 power station data.

Downloads Excel tables from gov.uk:
  - Table 5.11: Major power stations
  - Table 5.12: Generating capacity by fuel type

DUKES Excel files have complex multi-row headers — we handle this carefully.
"""

from __future__ import annotations

import io
import re
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

from uk_energy.config import DUKES_RAW, PROCESSED_DIR
from uk_energy.ingest._http import RateLimitedClient

# Known DUKES download URLs (updated annually — check gov.uk if these fail)
DUKES_URLS: list[dict[str, str]] = [
    {
        "name": "dukes_chapter5",
        "url": "https://assets.publishing.service.gov.uk/media/dukes-2023-chapter-5.xlsx",
        "desc": "DUKES 2023 Chapter 5",
    },
    {
        "name": "dukes_5_11",
        "url": "https://assets.publishing.service.gov.uk/media/dukes-2023-table-5-11.xlsx",
        "desc": "DUKES 2023 Table 5.11 Major Power Stations",
    },
    {
        "name": "dukes_5_12",
        "url": "https://assets.publishing.service.gov.uk/media/dukes-2023-table-5-12.xlsx",
        "desc": "DUKES 2023 Table 5.12 Capacity by Fuel",
    },
]

DUKES_GOV_PAGE = (
    "https://www.gov.uk/government/statistics/"
    "electricity-chapter-5-digest-of-united-kingdom-energy-statistics-dukes"
)


def _out(filename: str) -> Path:
    DUKES_RAW.mkdir(parents=True, exist_ok=True)
    return DUKES_RAW / filename


def _find_dukes_links(client: RateLimitedClient) -> list[dict[str, str]]:
    """Scrape the DUKES gov.uk page to find current Excel download links."""
    import re as _re
    links = []
    try:
        response = client.get(DUKES_GOV_PAGE)
        text = response.text
        # Find .xlsx links
        pattern = r'href=["\']([^"\']*(?:dukes|chapter.?5|table.?5-1)[^"\']*\.xlsx)["\']'
        matches = _re.findall(pattern, text, _re.IGNORECASE)
        for m in matches:
            url = m if m.startswith("http") else f"https://www.gov.uk{m}"
            fname = url.split("/")[-1]
            links.append({"name": fname.replace(".xlsx", ""), "url": url})
        logger.info(f"Found {len(links)} DUKES Excel links on gov.uk")
    except Exception as exc:
        logger.warning(f"Could not scrape DUKES page: {exc}")
    return links


def fetch_dukes(force: bool = False) -> list[Path]:
    """
    Download DUKES Chapter 5 Excel files from gov.uk.

    Tries known URLs first, then scrapes the gov.uk page for current links.
    """
    downloaded: list[Path] = []
    existing = list(DUKES_RAW.glob("*.xlsx")) if DUKES_RAW.exists() else []

    if existing and not force:
        logger.info(f"DUKES files already downloaded: {[p.name for p in existing]}")
        return existing

    logger.info("Fetching DUKES Chapter 5 Excel files...")
    with RateLimitedClient(rps=0.3, timeout=120) as client:
        # Try scraping the gov.uk page first
        scraped_links = _find_dukes_links(client)
        all_links = scraped_links + DUKES_URLS

        seen_urls: set[str] = set()
        for item in all_links:
            url = item["url"]
            if url in seen_urls:
                continue
            seen_urls.add(url)

            name = item.get("name", url.split("/")[-1].replace(".xlsx", ""))
            out = _out(f"{name}.xlsx")

            if out.exists() and not force:
                logger.info(f"Already exists: {out}")
                downloaded.append(out)
                continue

            try:
                logger.info(f"Downloading: {url}")
                response = client.get(url)
                content_type = response.headers.get("content-type", "")
                if "excel" in content_type or "spreadsheet" in content_type or len(response.content) > 10000:
                    out.write_bytes(response.content)
                    logger.success(f"Downloaded {out.name} ({len(response.content):,} bytes)")
                    downloaded.append(out)
                else:
                    logger.warning(f"Response doesn't look like Excel: {content_type}")
            except Exception as exc:
                logger.warning(f"Failed to download {url}: {exc}")

    if not downloaded:
        logger.warning("No DUKES Excel files downloaded — pipeline will continue without DUKES data")

    return downloaded


def _parse_excel_with_multi_header(
    path: Path,
    sheet_name: str | int = 0,
    header_rows: int = 4,
) -> pd.DataFrame:
    """
    Parse an Excel sheet that has complex multi-row headers.

    Strategy:
    1. Load raw without header parsing
    2. Identify the actual header rows
    3. Merge them into single column names
    """
    try:
        # Read raw (all rows as data)
        raw = pd.read_excel(path, sheet_name=sheet_name, header=None, dtype=str)
        logger.debug(f"Raw Excel shape: {raw.shape}")

        if raw.empty:
            return pd.DataFrame()

        # Find the row that looks like a header (has text in multiple cells)
        header_row_idx = 0
        for i in range(min(10, len(raw))):
            non_null = raw.iloc[i].dropna()
            if len(non_null) >= 3:
                # Check if it looks like a header (text, not all numbers)
                text_vals = [v for v in non_null if not str(v).replace(".", "").isdigit()]
                if len(text_vals) >= 2:
                    header_row_idx = i
                    break

        # Re-read with proper header
        df = pd.read_excel(
            path,
            sheet_name=sheet_name,
            header=header_row_idx,
            dtype=str,
        )
        # Drop completely empty rows/cols
        df = df.dropna(how="all").dropna(axis=1, how="all")
        # Clean column names
        df.columns = pd.Index([
            re.sub(r"[^a-z0-9]+", "_", str(c).strip().lower()).strip("_")
            if not str(c).startswith("unnamed") else f"col_{i}"
            for i, c in enumerate(df.columns)
        ])
        return df

    except Exception as exc:
        logger.error(f"Failed to parse {path.name}: {exc}")
        return pd.DataFrame()


def parse_dukes_511(path: Path) -> pd.DataFrame:
    """
    Parse DUKES Table 5.11 — Major Power Stations.

    Extracts: station name, company, fuel type, capacity MW, commissioned year, location.
    """
    logger.info(f"Parsing DUKES Table 5.11 from {path.name}...")

    # Try to find the right sheet
    try:
        xl = pd.ExcelFile(path)
        sheet_names = xl.sheet_names
        logger.debug(f"Sheets in {path.name}: {sheet_names}")

        # Look for "5.11" or "Major Power" sheet
        target_sheet: str | int = 0
        for s in sheet_names:
            if "5.11" in str(s) or "major" in str(s).lower() or "power station" in str(s).lower():
                target_sheet = s
                break

        df = _parse_excel_with_multi_header(path, sheet_name=target_sheet)

        if df.empty:
            return df

        # Map to standard column names
        col_map: dict[str, str] = {}
        for col in df.columns:
            col_lower = col.lower()
            if any(x in col_lower for x in ("station", "name", "plant")):
                col_map[col] = "name"
            elif any(x in col_lower for x in ("company", "owner", "operator")):
                col_map[col] = "company"
            elif any(x in col_lower for x in ("fuel", "type")):
                col_map[col] = "fuel_type"
            elif any(x in col_lower for x in ("capacity", "mw", "megawatt")):
                col_map[col] = "capacity_mw"
            elif any(x in col_lower for x in ("year", "commis", "opened")):
                col_map[col] = "commissioned_year"
            elif any(x in col_lower for x in ("locat", "region", "site")):
                col_map[col] = "location"

        if col_map:
            df = df.rename(columns=col_map)

        # Try to coerce capacity to numeric
        if "capacity_mw" in df.columns:
            df["capacity_mw"] = pd.to_numeric(df["capacity_mw"], errors="coerce")

        # Drop rows with no name or capacity
        if "name" in df.columns:
            df = df[df["name"].notna() & (df["name"].str.strip() != "")]

        df["source"] = "dukes_511"
        logger.success(f"Parsed {len(df)} major power stations from DUKES 5.11")
        return df

    except Exception as exc:
        logger.error(f"Failed to parse DUKES 5.11: {exc}")
        return pd.DataFrame()


def parse_dukes_512(path: Path) -> pd.DataFrame:
    """
    Parse DUKES Table 5.12 — Generating Capacity by Fuel Type.
    """
    logger.info(f"Parsing DUKES Table 5.12 from {path.name}...")
    try:
        xl = pd.ExcelFile(path)
        target_sheet: str | int = 0
        for s in xl.sheet_names:
            if "5.12" in str(s) or "capacity" in str(s).lower():
                target_sheet = s
                break

        df = _parse_excel_with_multi_header(path, sheet_name=target_sheet)
        df["source"] = "dukes_512"
        logger.success(f"Parsed {len(df)} rows from DUKES 5.12")
        return df
    except Exception as exc:
        logger.error(f"Failed to parse DUKES 5.12: {exc}")
        return pd.DataFrame()


def ingest_all(force: bool = False) -> pd.DataFrame:
    """Run DUKES ingestion: download + parse all relevant tables."""
    logger.info("=== DUKES Ingestion ===")
    paths = fetch_dukes(force=force)

    all_dfs: list[pd.DataFrame] = []
    for path in paths:
        if not path.suffix == ".xlsx":
            continue
        # Parse whichever table this is
        name_lower = path.stem.lower()
        if "5_11" in name_lower or "511" in name_lower or "major" in name_lower:
            df = parse_dukes_511(path)
        else:
            df = parse_dukes_512(path)
        if not df.empty:
            all_dfs.append(df)

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        out = PROCESSED_DIR / "dukes_processed.csv"
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        combined.to_csv(out, index=False)
        logger.success(f"Saved DUKES data ({len(combined)} rows) → {out}")
        return combined
    else:
        logger.warning("No DUKES data parsed — returning empty DataFrame")
        return pd.DataFrame()


if __name__ == "__main__":
    ingest_all()
