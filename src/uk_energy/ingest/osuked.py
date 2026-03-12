"""
osuked.py — OSUKED Power Station Dictionary ingestion.

This is the Rosetta Stone that cross-references:
  BMU IDs ↔ REPD IDs ↔ DUKES names ↔ WRI IDs ↔ common names

Source: https://github.com/OSUKED/Power-Station-Dictionary
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from loguru import logger

from uk_energy.config import OSUKED_DICTIONARY_CSV, OSUKED_FUEL_TYPES_CSV, OSUKED_NAMES_CSV, OSUKED_RAW, OSUKED_LOCATIONS_CSV
from uk_energy.ingest._http import RateLimitedClient

OSUKED_FILES: list[dict[str, str]] = [
    {
        "name": "dictionary",
        "url": OSUKED_DICTIONARY_CSV,
        "desc": "Main cross-reference dictionary (BMU ↔ REPD ↔ DUKES ↔ WRI)",
    },
    {
        "name": "plant_locations",
        "url": OSUKED_LOCATIONS_CSV,
        "desc": "GPS coordinates for each plant",
    },
    {
        "name": "fuel_types",
        "url": OSUKED_FUEL_TYPES_CSV,
        "desc": "Fuel type classifications",
    },
    {
        "name": "common_names",
        "url": OSUKED_NAMES_CSV,
        "desc": "Common names / aliases for each plant",
    },
]

# Alternative paths within the OSUKED repo (different branches/layouts)
OSUKED_ALT_URLS: dict[str, list[str]] = {
    "dictionary": [
        "https://raw.githubusercontent.com/OSUKED/Power-Station-Dictionary/main/data/raw/dictionary.csv",
        "https://raw.githubusercontent.com/OSUKED/Power-Station-Dictionary/master/data/raw/dictionary.csv",
        "https://raw.githubusercontent.com/OSUKED/Power-Station-Dictionary/main/data/dictionary.csv",
    ],
    "plant_locations": [
        "https://raw.githubusercontent.com/OSUKED/Power-Station-Dictionary/main/data/raw/plant_locations.csv",
        "https://raw.githubusercontent.com/OSUKED/Power-Station-Dictionary/master/data/raw/plant_locations.csv",
    ],
    "fuel_types": [
        "https://raw.githubusercontent.com/OSUKED/Power-Station-Dictionary/main/data/raw/fuel_types.csv",
        "https://raw.githubusercontent.com/OSUKED/Power-Station-Dictionary/master/data/raw/fuel_types.csv",
    ],
    "common_names": [
        "https://raw.githubusercontent.com/OSUKED/Power-Station-Dictionary/main/data/raw/common_names.csv",
        "https://raw.githubusercontent.com/OSUKED/Power-Station-Dictionary/master/data/raw/common_names.csv",
    ],
}


def _out(filename: str) -> Path:
    OSUKED_RAW.mkdir(parents=True, exist_ok=True)
    return OSUKED_RAW / filename


def _fetch_csv(client: RateLimitedClient, urls: list[str], out: Path) -> bool:
    """Try multiple URLs until one returns a valid CSV."""
    for url in urls:
        try:
            logger.info(f"Trying: {url}")
            response = client.get(url)
            text = response.text
            if len(text) > 100 and "," in text:
                out.write_text(text, encoding="utf-8")
                lines = text.count("\n")
                logger.success(f"Downloaded {out.name} ({lines} lines) → {out}")
                return True
            else:
                logger.warning(f"Response doesn't look like CSV: {url}")
        except Exception as exc:
            logger.warning(f"Failed {url}: {exc}")
    return False


def fetch_osuked(force: bool = False) -> dict[str, Path]:
    """
    Download all OSUKED Power Station Dictionary CSV files.

    Returns dict of {name: path} for each downloaded file.
    """
    paths: dict[str, Path] = {}

    with RateLimitedClient(rps=0.5) as client:
        for item in OSUKED_FILES:
            name = item["name"]
            out = _out(f"{name}.csv")

            if out.exists() and not force:
                logger.info(f"Already downloaded: {out}")
                paths[name] = out
                continue

            urls = OSUKED_ALT_URLS.get(name, [item["url"]])
            if _fetch_csv(client, urls, out):
                paths[name] = out
            else:
                logger.error(f"Could not download OSUKED {name}")

    return paths


def load_dictionary() -> pd.DataFrame:
    """
    Load the OSUKED cross-reference dictionary.

    The dictionary links each plant to IDs across multiple databases.
    """
    path = _out("dictionary.csv")
    if not path.exists():
        logger.warning("OSUKED dictionary not found, fetching...")
        fetch_osuked()

    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, low_memory=False)
    logger.info(f"Loaded OSUKED dictionary: {len(df)} entries, columns: {list(df.columns)}")
    return df


def load_plant_locations() -> pd.DataFrame:
    """Load OSUKED plant locations (lat/lon)."""
    path = _out("plant_locations.csv")
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    logger.info(f"Loaded OSUKED plant locations: {len(df)} entries")
    return df


def load_fuel_types() -> pd.DataFrame:
    """Load OSUKED fuel type classifications."""
    path = _out("fuel_types.csv")
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    logger.info(f"Loaded OSUKED fuel types: {len(df)} entries")
    return df


def load_common_names() -> pd.DataFrame:
    """Load OSUKED common names/aliases."""
    path = _out("common_names.csv")
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    logger.info(f"Loaded OSUKED common names: {len(df)} entries")
    return df


def build_unified_reference() -> pd.DataFrame:
    """
    Merge all OSUKED files into a single unified reference DataFrame.

    This is the master cross-reference table.
    """
    dictionary = load_dictionary()
    if dictionary.empty:
        logger.warning("OSUKED dictionary is empty — cross-referencing will be limited")
        return pd.DataFrame()

    locations = load_plant_locations()
    fuel_types = load_fuel_types()
    common_names = load_common_names()

    # Find the common join key (usually 'osuked_id' or similar)
    id_col: str | None = None
    for candidate in ("osuked_id", "dictionary_id", "id", "plant_id", "site_id"):
        if candidate in dictionary.columns:
            id_col = candidate
            break

    if id_col is None and not dictionary.empty:
        id_col = dictionary.columns[0]

    result = dictionary.copy()
    # Normalise the join key to string to avoid int64/str merge conflicts
    if id_col:
        result[id_col] = result[id_col].astype(str)

    def _safe_merge(left: pd.DataFrame, right: pd.DataFrame, key: str) -> pd.DataFrame:
        """Merge DataFrames, coercing key to string first."""
        right = right.copy()
        right[key] = right[key].astype(str)
        return left.merge(right, on=key, how="left", suffixes=("", "_r"))

    if id_col and not locations.empty:
        loc_id = next(
            (c for c in locations.columns if c in (id_col, "osuked_id", "dictionary_id", "id")),
            locations.columns[0] if not locations.empty else None,
        )
        if loc_id:
            result = _safe_merge(result, locations.rename(columns={loc_id: id_col}), id_col)

    if id_col and not fuel_types.empty:
        ft_id = next(
            (c for c in fuel_types.columns if c in (id_col, "osuked_id", "dictionary_id", "id")),
            fuel_types.columns[0] if not fuel_types.empty else None,
        )
        if ft_id:
            result = _safe_merge(result, fuel_types.rename(columns={ft_id: id_col}), id_col)

    if id_col and not common_names.empty:
        cn_id = next(
            (c for c in common_names.columns if c in (id_col, "osuked_id", "dictionary_id", "id")),
            common_names.columns[0] if not common_names.empty else None,
        )
        if cn_id:
            result = _safe_merge(result, common_names.rename(columns={cn_id: id_col}), id_col)

    logger.success(f"Built OSUKED unified reference: {len(result)} plants, {len(result.columns)} columns")
    return result


def ingest_all(force: bool = False) -> pd.DataFrame:
    """Run OSUKED ingestion."""
    logger.info("=== OSUKED Ingestion ===")
    fetch_osuked(force=force)
    df = build_unified_reference()
    if not df.empty:
        out = OSUKED_RAW / "unified_reference.csv"
        df.to_csv(out, index=False)
        logger.success(f"Saved OSUKED unified reference → {out}")
    logger.success("OSUKED ingestion complete")
    return df


if __name__ == "__main__":
    ingest_all()
