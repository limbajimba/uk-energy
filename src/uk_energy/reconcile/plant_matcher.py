"""
plant_matcher.py — Cross-source entity reconciliation for power plants.

Strategy:
1. Use OSUKED dictionary as primary cross-reference (links BMU ↔ REPD ↔ DUKES ↔ WRI)
2. For plants not in OSUKED: fuzzy name matching + location proximity
3. Output unified plants_unified.parquet

Output schema:
  plant_id (str): canonical UUID
  name (str): best available name
  lat, lon (float): best available coordinates
  fuel_type (str): canonical fuel type
  technology (str): detailed technology class
  capacity_mw (float)
  capacity_de_rated_mw (float | None)
  bmu_ids (list[str])
  repd_id (str | None)
  dukes_id (str | None)
  wri_id (str | None)
  osuked_id (str | None)
  owner (str | None)
  operator (str | None)
  status (str): operational/construction/consented/decommissioned/unknown
  commissioned_year (int | None)
  gsp_group (str | None)
  dno_region (str | None)
  source_bmrs, source_repd, source_dukes, source_wri, source_osuked (bool)
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from uk_energy.config import OSUKED_RAW, PLANTS_UNIFIED, PROCESSED_DIR, REFERENCE_DIR
from uk_energy.ingest.osuked import load_dictionary, load_plant_locations, load_fuel_types

# Fuzzy matching requires rapidfuzz
try:
    from rapidfuzz import fuzz, process as rfuzz_process
    HAS_RAPIDFUZZ = True
except ImportError:
    HAS_RAPIDFUZZ = False
    logger.warning("rapidfuzz not installed — fuzzy name matching disabled")


def _make_plant_id(name: str, source: str = "") -> str:
    """Create a deterministic canonical plant ID from name + source."""
    key = f"{name.lower().strip()}{source}".encode()
    return "plant_" + hashlib.sha256(key).hexdigest()[:12]


def _normalise_name(name: Any) -> str:
    """Normalise plant name for matching."""
    if not name or not isinstance(name, str):
        return ""
    # Remove common suffixes, strip punctuation
    name = str(name).lower().strip()
    name = re.sub(r"\s+(power\s+station|wind\s+farm|solar\s+park|cchp|ccgt|ocgt|ltd|limited|plc)\s*$", "", name)
    name = re.sub(r"[^\w\s]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute great-circle distance in km between two points."""
    R = 6371.0
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat / 2) ** 2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon / 2) ** 2
    return float(2 * R * np.arcsin(np.sqrt(a)))


def _load_fuel_mapping() -> dict[str, str]:
    """Load fuel type normalisation mapping from reference file."""
    ref_path = REFERENCE_DIR / "fuel_type_mapping.json"
    if ref_path.exists():
        with open(ref_path) as f:
            mapping: dict = json.load(f)
        return {k.lower(): v for k, v in mapping.get("mapping", {}).items()}
    # Minimal fallback
    return {
        "wind": "wind_onshore",
        "offshore wind": "wind_offshore",
        "onshore wind": "wind_onshore",
        "solar": "solar_pv",
        "photovoltaic": "solar_pv",
        "gas": "gas_ccgt",
        "ccgt": "gas_ccgt",
        "ocgt": "gas_ocgt",
        "hydro": "hydro_run_of_river",
        "pumped storage": "hydro_pumped_storage",
        "nuclear": "nuclear",
        "coal": "coal",
        "oil": "oil",
        "biomass": "biomass",
        "bioenergy": "biomass",
        "battery": "battery_storage",
        "storage": "battery_storage",
        "interconnector": "interconnector",
    }


def _map_fuel_type(raw: Any, mapping: dict[str, str]) -> str:
    """Map a raw fuel type string to canonical form."""
    if not raw or not isinstance(raw, str):
        return "unknown"
    raw_lower = raw.lower().strip()

    # Direct lookup (exact match)
    if raw_lower in mapping:
        return mapping[raw_lower]

    # Prioritised keyword matching (order matters — more specific first)
    keyword_map = [
        ("pumped storage", "hydro_pumped_storage"),
        ("pumped hydro", "hydro_pumped_storage"),
        ("pump storage", "hydro_pumped_storage"),
        ("hydrogen", "hydrogen"),
        ("offshore wind", "wind_offshore"),
        ("wind offshore", "wind_offshore"),
        ("onshore wind", "wind_onshore"),
        ("wind onshore", "wind_onshore"),
        ("solar photovoltaic", "solar_pv"),
        ("solar pv", "solar_pv"),
        ("solar", "solar_pv"),
        ("photovoltaic", "solar_pv"),
        ("battery", "battery_storage"),
        ("storage", "battery_storage"),
        ("nuclear", "nuclear"),
        ("biomass", "biomass"),
        ("bioenergy", "biomass"),
        ("biogas", "biomass"),
        ("landfill", "biomass"),
        ("sewage", "biomass"),
        ("anaerobic", "biomass"),
        ("efw", "biomass"),
        ("waste", "biomass"),
        ("incineration", "biomass"),
        ("ccgt", "gas_ccgt"),
        ("combined cycle", "gas_ccgt"),
        ("natural gas", "gas_ccgt"),
        ("ocgt", "gas_ocgt"),
        ("gas turbine", "gas_ocgt"),
        ("chp", "gas_chp"),
        ("coal", "coal"),
        ("oil", "oil"),
        ("diesel", "oil"),
        ("wind", "wind_onshore"),
        ("hydro", "hydro_run_of_river"),
        ("tidal", "wave_tidal"),
        ("wave", "wave_tidal"),
        ("geothermal", "geothermal"),
        ("interconnect", "interconnector"),
    ]

    for keyword, fuel_type in keyword_map:
        if keyword in raw_lower:
            return fuel_type

    # Fallback: check the mapping dict with partial match
    for key, val in mapping.items():
        if key in raw_lower:
            return val

    return "unknown"


def _load_wri() -> pd.DataFrame:
    """Load WRI GB plants."""
    path = PROCESSED_DIR / "wri_gb_plants.csv"
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def _load_repd() -> pd.DataFrame:
    """Load REPD processed plants."""
    path = PROCESSED_DIR / "repd_processed.csv"
    if path.exists():
        return pd.read_csv(path, low_memory=False)
    return pd.DataFrame()


def _load_dukes() -> pd.DataFrame:
    """Load DUKES processed plants."""
    path = PROCESSED_DIR / "dukes_processed.csv"
    if path.exists():
        return pd.read_csv(path, low_memory=False)
    return pd.DataFrame()


def _load_bmrs() -> pd.DataFrame:
    """Load BMRS BM unit list."""
    from uk_energy.config import BMRS_RAW
    path = BMRS_RAW / "bm_units_all.json"
    if path.exists():
        with open(path) as f:
            data = json.load(f)
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            rows = data.get("data", data.get("results", []))
            return pd.DataFrame(rows)
    return pd.DataFrame()


class PlantMatcher:
    """
    Reconciles plant entities across BMRS, REPD, DUKES, WRI using OSUKED
    as the primary key map.
    """

    def __init__(self) -> None:
        self.fuel_mapping = _load_fuel_mapping()
        self.osuked_dict: pd.DataFrame = pd.DataFrame()
        self.osuked_locations: pd.DataFrame = pd.DataFrame()
        self.osuked_fuel_types: pd.DataFrame = pd.DataFrame()
        self.wri: pd.DataFrame = pd.DataFrame()
        self.repd: pd.DataFrame = pd.DataFrame()
        self.dukes: pd.DataFrame = pd.DataFrame()
        self.bmrs: pd.DataFrame = pd.DataFrame()

    def load_all_sources(self) -> None:
        """Load all source DataFrames."""
        logger.info("Loading all data sources for reconciliation...")
        self.osuked_dict = load_dictionary()
        self.osuked_locations = load_plant_locations()
        self.osuked_fuel_types = load_fuel_types()
        self.wri = _load_wri()
        self.repd = _load_repd()
        self.dukes = _load_dukes()
        self.bmrs = _load_bmrs()
        logger.info(
            f"Source counts: OSUKED={len(self.osuked_dict)}, "
            f"WRI={len(self.wri)}, REPD={len(self.repd)}, "
            f"DUKES={len(self.dukes)}, BMRS={len(self.bmrs)}"
        )

    def _build_from_osuked(self) -> list[dict[str, Any]]:
        """
        Build plant records using OSUKED as the master cross-reference.

        OSUKED dictionary schema:
          dictionary_id, gppd_idnr, name, sett_bmu_id, ngc_bmu_id,
          old_repd_id, new_repd_id, ...
        Locations: dictionary_id, latitude, longitude
        Fuel types: ngc_bmu_id, fuel_type
        Common names: dictionary_id, common_name
        """
        if self.osuked_dict.empty:
            return []

        plants: list[dict[str, Any]] = []
        id_col = "dictionary_id"

        # Build location lookup: dictionary_id → {latitude, longitude}
        loc_by_id: dict[str, dict] = {}
        if not self.osuked_locations.empty and id_col in self.osuked_locations.columns:
            for _, row in self.osuked_locations.iterrows():
                oid = str(int(row[id_col])) if pd.notna(row[id_col]) else ""
                if oid:
                    loc_by_id[oid] = row.to_dict()

        # Build common name lookup: dictionary_id → common_name
        name_by_id: dict[str, str] = {}
        if not self.osuked_fuel_types.empty:
            # fuel_types is keyed by ngc_bmu_id, not dictionary_id
            pass  # handled separately below

        cn_by_id: dict[str, str] = {}
        if "dictionary_id" in self.osuked_fuel_types.columns:
            pass  # not in this file

        # Build fuel type lookup: ngc_bmu_id → fuel_type
        ft_by_bmu: dict[str, str] = {}
        if not self.osuked_fuel_types.empty and "ngc_bmu_id" in self.osuked_fuel_types.columns:
            ft_val_col = next(
                (c for c in self.osuked_fuel_types.columns if "fuel" in c.lower()),
                None,
            )
            if ft_val_col:
                for _, row in self.osuked_fuel_types.iterrows():
                    bmu = str(row.get("ngc_bmu_id", "")).strip()
                    if bmu:
                        ft_by_bmu[bmu] = str(row[ft_val_col])

        for _, row in self.osuked_dict.iterrows():
            raw_id = row.get(id_col)
            if pd.isna(raw_id):
                continue
            osuked_id = str(int(raw_id))
            loc = loc_by_id.get(osuked_id, {})

            # Collect BMU IDs
            bmu_ids: list[str] = []
            for bmu_col in ("sett_bmu_id", "ngc_bmu_id"):
                val = row.get(bmu_col)
                if pd.notna(val) and str(val).strip():
                    # May be comma-separated
                    for b in str(val).split(","):
                        b = b.strip()
                        if b and b not in bmu_ids:
                            bmu_ids.append(b)

            # Get fuel type from fuel_types file via ngc_bmu_id
            fuel_raw = ""
            ngc_bmu = str(row.get("ngc_bmu_id", "") or "").strip()
            if ngc_bmu and ngc_bmu in ft_by_bmu:
                fuel_raw = ft_by_bmu[ngc_bmu]

            name_raw = row.get("name", "")
            name = str(name_raw).strip() if pd.notna(name_raw) and name_raw else f"plant_{osuked_id}"

            lat_raw = loc.get("latitude", np.nan)
            lon_raw = loc.get("longitude", np.nan)
            lat = float(lat_raw) if pd.notna(lat_raw) else None
            lon = float(lon_raw) if pd.notna(lon_raw) else None

            # REPD and WRI cross-references
            gppd = str(row.get("gppd_idnr", "") or "").strip()
            repd_id_old = str(row.get("old_repd_id", "") or "").strip()
            repd_id_new = str(row.get("new_repd_id", "") or "").strip()
            repd_id = repd_id_new or repd_id_old or None

            plant: dict[str, Any] = {
                "plant_id": _make_plant_id(name, "osuked"),
                "osuked_id": osuked_id,
                "name": name,
                "lat": lat,
                "lon": lon,
                "fuel_type": _map_fuel_type(fuel_raw, self.fuel_mapping),
                "technology": fuel_raw or "unknown",
                "capacity_mw": None,
                "capacity_de_rated_mw": None,
                "bmu_ids": bmu_ids,
                "repd_id": repd_id if repd_id else None,
                "dukes_id": None,
                "wri_id": gppd if gppd else None,
                "owner": None,
                "operator": None,
                "status": "operational",
                "commissioned_year": None,
                "gsp_group": None,
                "dno_region": None,
                "source_bmrs": bool(bmu_ids),
                "source_repd": bool(repd_id),
                "source_dukes": False,
                "source_wri": bool(gppd),
                "source_osuked": True,
            }
            plants.append(plant)

        logger.info(f"Built {len(plants)} plants from OSUKED dictionary")
        return plants

    def _build_from_wri(self, existing_wri_ids: set[str]) -> list[dict[str, Any]]:
        """Build plant records from WRI for plants not already in OSUKED."""
        if self.wri.empty:
            return []

        plants: list[dict[str, Any]] = []
        for _, row in self.wri.iterrows():
            wri_id = str(row.get("gppd_idnr", ""))
            if wri_id in existing_wri_ids:
                continue

            lat = float(row.get("latitude", np.nan) or np.nan)
            lon = float(row.get("longitude", np.nan) or np.nan)
            name = str(row.get("name", wri_id))

            plant: dict[str, Any] = {
                "plant_id": _make_plant_id(name, "wri"),
                "osuked_id": None,
                "name": name,
                "lat": lat if not np.isnan(lat) else None,
                "lon": lon if not np.isnan(lon) else None,
                "fuel_type": _map_fuel_type(row.get("primary_fuel", ""), self.fuel_mapping),
                "technology": str(row.get("primary_fuel", "unknown")),
                "capacity_mw": float(row.get("capacity_mw", np.nan) or np.nan) or None,
                "capacity_de_rated_mw": None,
                "bmu_ids": [],
                "repd_id": None,
                "dukes_id": None,
                "wri_id": wri_id,
                "owner": str(row.get("owner", "")) or None,
                "operator": None,
                "status": "operational",
                "commissioned_year": int(row["commissioning_year"]) if pd.notna(row.get("commissioning_year")) else None,
                "gsp_group": None,
                "dno_region": None,
                "source_bmrs": False,
                "source_repd": False,
                "source_dukes": False,
                "source_wri": True,
                "source_osuked": False,
            }
            plants.append(plant)

        logger.info(f"Added {len(plants)} WRI-only plants (not in OSUKED)")
        return plants

    def _build_from_dukes(self, existing_names: set[str]) -> list[dict[str, Any]]:
        """Build plant records from DUKES for plants not matched elsewhere."""
        if self.dukes.empty:
            return []

        plants: list[dict[str, Any]] = []
        for _, row in self.dukes.iterrows():
            name = str(row.get("name", row.get("station_name", row.get("Station Name", "")))).strip()
            if not name:
                continue

            norm = _normalise_name(name)
            if norm in existing_names:
                continue

            # DUKES has capacity and fuel but often no coordinates
            fuel_raw = row.get("fuel", row.get("fuel_type", row.get("Fuel", "")))
            cap_raw = row.get("capacity_mw", row.get("installed_capacity", row.get("Installed Capacity (MW)", np.nan)))

            # DUKES processed CSV now has lat/lon (converted from OSGB36)
            lat_val = row.get("lat", np.nan)
            lon_val = row.get("lon", np.nan)

            plant: dict[str, Any] = {
                "plant_id": _make_plant_id(name, "dukes"),
                "osuked_id": None,
                "name": name,
                "lat": float(lat_val) if pd.notna(lat_val) else None,
                "lon": float(lon_val) if pd.notna(lon_val) else None,
                "fuel_type": _map_fuel_type(fuel_raw, self.fuel_mapping),
                "technology": str(fuel_raw or "unknown"),
                "capacity_mw": float(cap_raw) if pd.notna(cap_raw) else None,
                "capacity_de_rated_mw": None,
                "bmu_ids": [],
                "repd_id": None,
                "dukes_id": str(row.get("dukes_id", row.get("ref", ""))) or None,
                "wri_id": None,
                "owner": str(row.get("company", row.get("Company", ""))) or None,
                "operator": None,
                "status": "operational",  # DUKES only lists operational stations
                "commissioned_year": None,
                "gsp_group": None,
                "dno_region": None,
                "source_bmrs": False,
                "source_repd": False,
                "source_dukes": True,
                "source_wri": False,
                "source_osuked": False,
            }
            plants.append(plant)

        logger.info(f"Added {len(plants)} DUKES-only plants")
        return plants

    def _build_from_repd(self, existing_names: set[str]) -> list[dict[str, Any]]:
        """Build plant records from REPD for plants not matched elsewhere."""
        if self.repd.empty:
            return []

        plants: list[dict[str, Any]] = []
        for _, row in self.repd.iterrows():
            name = str(row.get("name", row.get("site_name", ""))).strip()
            if not name:
                continue

            norm = _normalise_name(name)
            if norm in existing_names:
                continue

            # Map REPD status properly — use development_status_short if available
            raw_status = str(row.get("status", "unknown")).lower().strip()
            dev_status = str(row.get("development_status_short", "")).lower().strip()

            # Map REPD statuses to canonical values
            if raw_status == "operational" or dev_status == "operational":
                status = "operational"
            elif raw_status == "construction" or dev_status in ("under construction",):
                status = "construction"
            elif dev_status in ("awaiting construction",):
                status = "consented"  # has planning permission, not yet built
            elif raw_status == "planning" or dev_status in ("planning application submitted", "application submitted"):
                status = "planning"
            elif raw_status in ("refused", "withdrawn") or dev_status in (
                "application refused", "appeal refused", "application withdrawn",
                "appeal withdrawn", "abandoned",
            ):
                continue  # skip refused/withdrawn
            elif dev_status == "revised":
                status = "planning"  # revised applications
            else:
                status = raw_status if raw_status != "unknown" else "unknown"

            lat_raw = row.get("lat", row.get("latitude", np.nan))
            lon_raw = row.get("lon", row.get("longitude", np.nan))
            lat = float(lat_raw) if pd.notna(lat_raw) else np.nan
            lon = float(lon_raw) if pd.notna(lon_raw) else np.nan

            plant: dict[str, Any] = {
                "plant_id": _make_plant_id(name, "repd"),
                "osuked_id": None,
                "name": name,
                "lat": lat if not np.isnan(lat) else None,
                "lon": lon if not np.isnan(lon) else None,
                "fuel_type": _map_fuel_type(row.get("technology", ""), self.fuel_mapping),
                "technology": str(row.get("technology", "unknown")),
                "capacity_mw": float(row.get("capacity_mw", np.nan) or np.nan) or None,
                "capacity_de_rated_mw": None,
                "bmu_ids": [],
                "repd_id": str(row.get("repd_id", "")) or None,
                "dukes_id": None,
                "wri_id": None,
                "owner": str(row.get("developer", "")) or None,
                "operator": None,
                "status": status,
                "commissioned_year": None,
                "gsp_group": None,
                "dno_region": None,
                "source_bmrs": False,
                "source_repd": True,
                "source_dukes": False,
                "source_wri": False,
                "source_osuked": False,
            }
            plants.append(plant)

        logger.info(f"Added {len(plants)} REPD-only plants")
        return plants

    def reconcile(self) -> pd.DataFrame:
        """
        Main reconciliation entry point.

        Returns unified plants DataFrame and saves to parquet.
        """
        logger.info("Starting plant entity reconciliation...")
        self.load_all_sources()

        # 1. Start with OSUKED (the master reference)
        all_plants: list[dict[str, Any]] = self._build_from_osuked()

        # Track which WRI / name IDs we've already covered
        covered_wri_ids = {p["wri_id"] for p in all_plants if p.get("wri_id")}
        covered_names = {_normalise_name(p["name"]) for p in all_plants if p.get("name")}

        # 2. Add WRI plants not in OSUKED
        wri_plants = self._build_from_wri(covered_wri_ids)
        all_plants.extend(wri_plants)
        covered_names.update(_normalise_name(p["name"]) for p in wri_plants if p.get("name"))

        # 3. Add DUKES plants not yet covered (authoritative gov source)
        dukes_plants = self._build_from_dukes(covered_names)
        all_plants.extend(dukes_plants)
        covered_names.update(_normalise_name(p["name"]) for p in dukes_plants if p.get("name"))

        # 4. Add REPD plants not yet covered
        repd_plants = self._build_from_repd(covered_names)
        all_plants.extend(repd_plants)

        if not all_plants:
            logger.warning("No plants reconciled — all sources may be empty")
            return pd.DataFrame()

        df = pd.DataFrame(all_plants)

        # Ensure correct dtypes
        for col in ("lat", "lon", "capacity_mw", "capacity_de_rated_mw"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

        for col in ("source_bmrs", "source_repd", "source_dukes", "source_wri", "source_osuked"):
            if col in df.columns:
                df[col] = df[col].fillna(False).astype(bool)

        # Save
        PLANTS_UNIFIED.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(PLANTS_UNIFIED, index=False, engine="pyarrow")
        logger.success(
            f"Reconciliation complete: {len(df)} plants → {PLANTS_UNIFIED}\n"
            f"  Sources: OSUKED={df['source_osuked'].sum()}, "
            f"WRI={df['source_wri'].sum()}, "
            f"DUKES={df['source_dukes'].sum()}, "
            f"REPD={df['source_repd'].sum()}, "
            f"BMRS={df['source_bmrs'].sum()}"
        )
        return df


def reconcile_plants() -> pd.DataFrame:
    """Convenience wrapper."""
    matcher = PlantMatcher()
    return matcher.reconcile()


if __name__ == "__main__":
    reconcile_plants()
