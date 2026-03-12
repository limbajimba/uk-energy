"""
wri.py — WRI Global Power Plant Database ingestion.

Downloads the full database (CSV ~6MB) and filters for GB plants
(country_long == "United Kingdom", country_code == "GBR").

Fields extracted:
  name, gppd_idnr, capacity_mw, latitude, longitude, primary_fuel,
  other_fuel1-3, owner, commissioning_year, generation_gwh_2013-2019
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from loguru import logger

from uk_energy.config import PROCESSED_DIR, WRI_GITHUB_URL, WRI_RAW, WRI_ZENODO_URL
from uk_energy.ingest._http import RateLimitedClient

WRI_URLS = [WRI_GITHUB_URL, WRI_ZENODO_URL]
WRI_COLS = [
    "name",
    "gppd_idnr",
    "capacity_mw",
    "latitude",
    "longitude",
    "primary_fuel",
    "other_fuel1",
    "other_fuel2",
    "other_fuel3",
    "country",
    "country_long",
    "owner",
    "commissioning_year",
    "source",
    "url",
    "generation_gwh_2013",
    "generation_gwh_2014",
    "generation_gwh_2015",
    "generation_gwh_2016",
    "generation_gwh_2017",
    "generation_gwh_2018",
    "generation_gwh_2019",
    "estimated_generation_gwh_2013",
    "estimated_generation_gwh_2014",
    "estimated_generation_gwh_2015",
    "estimated_generation_gwh_2016",
    "estimated_generation_gwh_2017",
]


def _out(filename: str) -> Path:
    WRI_RAW.mkdir(parents=True, exist_ok=True)
    return WRI_RAW / filename


def fetch_wri(force: bool = False) -> Path:
    """
    Download the WRI Global Power Plant Database CSV.

    Returns path to the raw (full, worldwide) CSV.
    """
    raw_path = _out("global_power_plant_database.csv")
    if raw_path.exists() and not force:
        logger.info(f"WRI database already downloaded: {raw_path}")
        return raw_path

    logger.info("Fetching WRI Global Power Plant Database...")
    with RateLimitedClient(rps=0.5, timeout=120) as client:
        last_exc: Exception | None = None
        for url in WRI_URLS:
            try:
                logger.info(f"Trying: {url}")
                response = client.get(url)
                text = response.text
                if "gppd_idnr" in text or "capacity_mw" in text:
                    raw_path.write_text(text, encoding="utf-8")
                    line_count = text.count("\n")
                    logger.success(f"Downloaded WRI database ({line_count:,} lines) → {raw_path}")
                    return raw_path
                else:
                    logger.warning(f"Response from {url} doesn't look like WRI CSV")
            except Exception as exc:
                last_exc = exc
                logger.warning(f"Failed {url}: {exc}")

        logger.error(f"Could not download WRI database. Last error: {last_exc}")
        raw_path.write_text("# WRI download failed\n")

    return raw_path


def parse_wri_gb(raw_path: Path | None = None) -> pd.DataFrame:
    """
    Load WRI CSV and filter for GB plants.

    Returns DataFrame with standardised columns.
    """
    if raw_path is None:
        raw_path = _out("global_power_plant_database.csv")

    if not raw_path.exists():
        fetch_wri()

    logger.info("Filtering WRI database for GB plants...")

    try:
        df = pd.read_csv(raw_path, low_memory=False)
    except Exception as exc:
        logger.error(f"Could not read WRI CSV: {exc}")
        return pd.DataFrame()

    if df.empty:
        return df

    # Filter for UK
    gb_mask = (
        (df.get("country", pd.Series(dtype=str)) == "GBR") |
        (df.get("country_long", pd.Series(dtype=str)).str.contains("United Kingdom", case=False, na=False))
    )
    df_gb = df[gb_mask].copy()
    logger.info(f"Filtered {len(df_gb)} GB plants from {len(df)} total")

    # Keep only columns that exist
    keep_cols = [c for c in WRI_COLS if c in df_gb.columns]
    df_gb = df_gb[keep_cols]

    # Compute latest generation estimate
    gen_cols = [c for c in df_gb.columns if c.startswith("generation_gwh_")]
    if gen_cols:
        df_gb["generation_gwh_latest"] = df_gb[gen_cols].apply(
            lambda row: next(
                (row[c] for c in reversed(gen_cols) if pd.notna(row[c]) and row[c] > 0),
                None,
            ),
            axis=1,
        )

    # Normalise fuel types to lowercase
    if "primary_fuel" in df_gb.columns:
        df_gb["primary_fuel"] = df_gb["primary_fuel"].str.lower().str.strip()

    # Save
    out = PROCESSED_DIR / "wri_gb_plants.csv"
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df_gb.to_csv(out, index=False)
    logger.success(f"Saved WRI GB plants ({len(df_gb)} rows) → {out}")
    return df_gb


def ingest_all(force: bool = False) -> pd.DataFrame:
    """Run WRI ingestion: download + filter."""
    logger.info("=== WRI Ingestion ===")
    fetch_wri(force=force)
    df = parse_wri_gb()
    logger.success("WRI ingestion complete")
    return df


if __name__ == "__main__":
    ingest_all()
