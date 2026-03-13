"""
config.py — Centralised configuration for the UK Energy project.

All API endpoints, file paths, constants, and taxonomy definitions live here.
Import this module instead of hard-coding magic strings elsewhere.
"""

from __future__ import annotations

import os
from pathlib import Path

# ─── Project Root ─────────────────────────────────────────────────────────────
# Supports both installed-package and editable-install layouts.
_HERE: Path = Path(__file__).resolve().parent
PROJECT_ROOT: Path = _HERE.parents[1]  # src/uk_energy/ → project root

# Allow override via environment variable (useful in CI / tests).
if _root_override := os.environ.get("UK_ENERGY_ROOT"):
    PROJECT_ROOT = Path(_root_override).resolve()

# ─── Data Directories ─────────────────────────────────────────────────────────
DATA_DIR: Path = PROJECT_ROOT / "data"
RAW_DIR: Path = DATA_DIR / "raw"
PROCESSED_DIR: Path = DATA_DIR / "processed"
REFERENCE_DIR: Path = DATA_DIR / "reference"
OUTPUT_DIR: Path = PROJECT_ROOT / "output"
LOGS_DIR: Path = PROJECT_ROOT / "logs"

# Per-source raw directories
BMRS_RAW: Path = RAW_DIR / "bmrs"
NESO_RAW: Path = RAW_DIR / "neso"
OSM_RAW: Path = RAW_DIR / "osm"
CARBON_RAW: Path = RAW_DIR / "carbon_intensity"
WRI_RAW: Path = RAW_DIR / "wri"
OSUKED_RAW: Path = RAW_DIR / "osuked"
REPD_RAW: Path = RAW_DIR / "repd"
DUKES_RAW: Path = RAW_DIR / "dukes"

# Processed outputs
PLANTS_UNIFIED: Path = PROCESSED_DIR / "plants_unified.parquet"

# Reference files
INTERCONNECTORS_REF: Path = REFERENCE_DIR / "interconnectors.json"
DNO_REGIONS_REF: Path = REFERENCE_DIR / "dno_regions.json"
FUEL_TYPE_MAPPING_REF: Path = REFERENCE_DIR / "fuel_type_mapping.json"
GSP_GROUPS_REF: Path = REFERENCE_DIR / "gsp_groups.json"

# Output files
MAP_OUTPUT: Path = OUTPUT_DIR / "uk_energy_map.html"
NETWORK_OUTPUT: Path = OUTPUT_DIR / "uk_grid_network.html"
GRAPHML_OUTPUT: Path = OUTPUT_DIR / "uk_grid.graphml"
GRAPH_PICKLE: Path = OUTPUT_DIR / "uk_grid.pkl"
STATS_CSV: Path = OUTPUT_DIR / "grid_stats.csv"


def ensure_dirs() -> None:
    """Create all required project directories if they do not already exist."""
    for directory in (
        BMRS_RAW, NESO_RAW, OSM_RAW, CARBON_RAW, WRI_RAW, OSUKED_RAW,
        REPD_RAW, DUKES_RAW, PROCESSED_DIR, REFERENCE_DIR, OUTPUT_DIR, LOGS_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)


# ─── API Endpoints ────────────────────────────────────────────────────────────

# Elexon BMRS v1 — no API key required
# Docs: https://bmrs.elexon.co.uk/api-documentation
BMRS_BASE: str = "https://data.elexon.co.uk/bmrs/api/v1"
BMRS_BMUNITS_ALL: str = f"{BMRS_BASE}/reference/bmunits/all"
BMRS_B1610: str = f"{BMRS_BASE}/datasets/B1610"       # Actual generation per unit (30 min)
BMRS_B1620: str = f"{BMRS_BASE}/datasets/B1620"       # Aggregated generation by fuel type
BMRS_INTERCONNECTOR_FLOWS: str = f"{BMRS_BASE}/datasets/INTERFUELHH"

# NESO CKAN data portal
# Docs: https://api.neso.energy/api/3
NESO_CKAN_BASE: str = "https://api.neso.energy/api/3/action"
NESO_PACKAGE_SEARCH: str = f"{NESO_CKAN_BASE}/package_search"
NESO_RESOURCE_SHOW: str = f"{NESO_CKAN_BASE}/resource_show"
NESO_PACKAGE_SHOW: str = f"{NESO_CKAN_BASE}/package_show"

# National Grid ESO Carbon Intensity API
# Docs: https://carbon-intensity.github.io/api-definitions/
CARBON_BASE: str = "https://api.carbonintensity.org.uk"
CARBON_REGIONAL: str = f"{CARBON_BASE}/regional"
CARBON_GENERATION: str = f"{CARBON_BASE}/generation"

# Overpass API (OpenStreetMap)
# Docs: https://wiki.openstreetmap.org/wiki/Overpass_API
OVERPASS_URL: str = "https://overpass-api.de/api/interpreter"
OVERPASS_TIMEOUT_S: int = 180

# Great Britain bounding box (S, W, N, E) — covers mainland + islands.
GB_BBOX: tuple[float, float, float, float] = (49.9, -8.2, 60.9, 1.8)

# WRI Global Power Plant Database v1.3.0
# Docs: https://datasets.wri.org/dataset/globalpowerplantdatabase
WRI_GITHUB_URL: str = (
    "https://raw.githubusercontent.com/wri/global-power-plant-database/"
    "master/output_database/global_power_plant_database.csv"
)
WRI_ZENODO_URL: str = (
    "https://zenodo.org/record/5012294/files/global_power_plant_database.csv"
)

# OSUKED Power Station Dictionary
# Repo: https://github.com/OSUKED/Power-Station-Dictionary
# These are the canonical raw-data URLs; the actual data has been pre-downloaded
# to data/raw/osuked/ — see ingest/osuked.py for the loader.
OSUKED_GITHUB_BASE: str = (
    "https://raw.githubusercontent.com/OSUKED/Power-Station-Dictionary/main"
)
OSUKED_DICTIONARY_CSV: str = f"{OSUKED_GITHUB_BASE}/data/dictionary/ids.csv"
OSUKED_LOCATIONS_CSV: str = (
    f"{OSUKED_GITHUB_BASE}/data/linked-datapackages/plant-locations/plant-locations.csv"
)
OSUKED_FUEL_TYPES_CSV: str = (
    f"{OSUKED_GITHUB_BASE}/data/linked-datapackages/bmu-fuel-types/fuel_types.csv"
)
OSUKED_NAMES_CSV: str = (
    f"{OSUKED_GITHUB_BASE}/archive/docs/attribute_sources/common-names/common-names.csv"
)

# REPD (Renewable Energy Planning Database — data.gov.uk)
REPD_DATA_GOV_URL: str = (
    "https://assets.publishing.service.gov.uk/media/repd-q3-october-2023.csv"
)
REPD_FALLBACK_URL: str = (
    "https://www.data.gov.uk/dataset/a5b0ed13-c960-49ce-b1f6-3a6bbe0db1b7/repd"
)

# DUKES (Digest of UK Energy Statistics, Chapter 5 — DESNZ / BEIS)
DUKES_BASE: str = "https://assets.publishing.service.gov.uk/media"
DUKES_SEARCH_URL: str = (
    "https://www.gov.uk/government/statistics/"
    "electricity-chapter-5-digest-of-united-kingdom-energy-statistics-dukes"
)

# ─── Rate Limiting ────────────────────────────────────────────────────────────
BMRS_RATE_LIMIT_RPS: float = 1.0   # Elexon requests-per-second
DEFAULT_TIMEOUT: int = 30           # Default HTTP timeout in seconds

# ─── Fuel Type Taxonomy ───────────────────────────────────────────────────────
# Canonical fuel-type identifiers used throughout the pipeline (lowercase, underscored).
FUEL_TYPES: list[str] = [
    "coal",
    "gas_ccgt",
    "gas_ocgt",
    "gas_chp",
    "nuclear",
    "oil",
    "biomass",
    "hydro_run_of_river",
    "hydro_pumped_storage",
    "wind_onshore",
    "wind_offshore",
    "solar_pv",
    "wave_tidal",
    "geothermal",
    "hydrogen",
    "battery_storage",
    "interconnector",
    "demand_response",
    "other",
    "unknown",
]

# ─── DNO Regions ──────────────────────────────────────────────────────────────
# 14 GB Distribution Network Operator regions as defined by the Carbon Intensity API.
DNO_REGION_IDS: dict[int, str] = {
    1:  "North Scotland",
    2:  "South Scotland",
    3:  "North West England",
    4:  "North East England",
    5:  "Yorkshire",
    6:  "North Wales & Mersey",
    7:  "South Wales",
    8:  "West Midlands",
    9:  "East Midlands",
    10: "East England",
    11: "South West England",
    12: "South England",
    13: "London",
    14: "South East England",
}

# ─── Logging ──────────────────────────────────────────────────────────────────
LOG_LEVEL: str = os.environ.get("UK_ENERGY_LOG_LEVEL", "INFO")
LOG_FORMAT: str = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "<level>{message}</level>"
)
