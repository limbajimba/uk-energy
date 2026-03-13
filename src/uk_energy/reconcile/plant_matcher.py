"""
plant_matcher.py — Entity reconciliation across UK energy data sources.

Reconciliation strategy (priority order):
  1. DUKES (official DESNZ register) — ground truth for operational capacity
  2. OSUKED cross-reference — enriches DUKES with BMU IDs, REPD IDs, coordinates
  3. WRI — supplements with plants not in DUKES (but WRI data is stale, 2019 vintage)
  4. REPD — adds renewable projects (planning/consented/construction/operational)

Key principles:
  - DUKES total should match published figure (~82 GW operational)
  - WRI coal plants are all closed (UK coal-free since Sept 2024)
  - Pumped storage must be classified correctly (not "hydro")
  - Entity deduplication: name fuzzy matching + capacity proximity
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from uk_energy.config import (
    FUEL_TYPE_MAPPING_REF as FUEL_TYPE_MAPPING,
    PLANTS_UNIFIED,
    PROCESSED_DIR,
)

try:
    from rapidfuzz import fuzz
except ImportError:
    fuzz = None  # type: ignore[assignment]

# ─── Constants ───────────────────────────────────────────────────────────────

# Coal plants still in WRI but closed/closing in reality
CLOSED_COAL_STATIONS: set[str] = {
    "aberthaw", "cottam", "drax", "eggborough", "kilroot",
    "ratcliffe", "uskmouth", "west burton", "fiddler",
    "ferrybridge", "longannet", "rugeley", "ironbridge",
}

# Known pumped storage stations (WRI misclassifies as "hydro")
PUMPED_STORAGE_STATIONS: set[str] = {
    "dinorwig", "ffestiniog", "cruachan", "foyers",
}

# WRI duplicates (same station appears twice)
WRI_KNOWN_DUPES: dict[str, str] = {
    # gppd_idnr of the duplicate → keep this one
}


def _make_plant_id(name: str, source: str = "") -> str:
    """Create a deterministic plant ID from name + source."""
    key = f"{name.lower().strip()}{source}".encode()
    return "plant_" + hashlib.sha256(key).hexdigest()[:12]


def _normalise_name(name: str) -> str:
    """Normalise a plant name for matching."""
    if not name:
        return ""
    import re
    s = name.lower().strip()
    # Remove common suffixes
    for suffix in (
        " power station", " power plant", " power ltd",
        " generating station", " wind farm", " solar farm",
        " solar park", " limited", " ltd", " plc",
        " (phase 1)", " (phase 2)", " phase 1", " phase 2",
    ):
        s = s.replace(suffix, "")
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s


def _load_fuel_mapping() -> dict[str, str]:
    """Load the fuel type mapping from reference data."""
    if FUEL_TYPE_MAPPING.exists():
        data = json.loads(FUEL_TYPE_MAPPING.read_text())
        return data.get("mapping", {})
    return {}


def _map_fuel_type(raw: Any, mapping: dict[str, str]) -> str:
    """Map a raw fuel type string to canonical form."""
    if not raw or not isinstance(raw, str):
        return "unknown"
    raw_lower = raw.lower().strip()

    # Direct lookup
    if raw_lower in mapping:
        return mapping[raw_lower]

    # Prioritised keyword matching (order matters)
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
        ("energy storage", "battery_storage"),
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
        ("msw", "biomass"),
        ("ccgt", "gas_ccgt"),
        ("combined cycle", "gas_ccgt"),
        ("natural gas", "gas_ccgt"),
        ("sour gas", "gas_ccgt"),
        ("ocgt", "gas_ocgt"),
        ("gas turbine", "gas_ocgt"),
        ("chp", "gas_chp"),
        ("diesel", "oil"),
        ("gas oil", "oil"),
        ("coal", "coal"),
        ("oil", "oil"),
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

    return "unknown"


def _is_duplicate(
    name1: str, name2: str,
    cap1: float | None, cap2: float | None,
    threshold: int = 75,
) -> bool:
    """Check if two plants are likely the same entity."""
    if not name1 or not name2:
        return False
    n1 = _normalise_name(name1)
    n2 = _normalise_name(name2)

    # Exact normalised match
    if n1 == n2:
        return True

    # Fuzzy match
    if fuzz is not None:
        score = fuzz.ratio(n1, n2)
        if score >= threshold:
            # If capacity is available, check it's within 20%
            if cap1 and cap2 and cap1 > 0 and cap2 > 0:
                ratio = min(cap1, cap2) / max(cap1, cap2)
                return ratio > 0.7
            return True

    # One contains the other
    if n1 in n2 or n2 in n1:
        return True

    return False


# ─── Source Loaders ──────────────────────────────────────────────────────────

def _load_dukes() -> pd.DataFrame:
    path = PROCESSED_DIR / "dukes_processed.csv"
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def _load_wri() -> pd.DataFrame:
    path = PROCESSED_DIR / "wri_gb_plants.csv"
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def _load_repd() -> pd.DataFrame:
    path = PROCESSED_DIR / "repd_processed.csv"
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def _load_osuked_locations() -> dict[str, tuple[float, float]]:
    """Load OSUKED plant locations as {dictionary_id: (lat, lon)}."""
    from uk_energy.ingest.osuked import load_plant_locations
    locs = load_plant_locations()
    result: dict[str, tuple[float, float]] = {}
    for _, row in locs.iterrows():
        did = str(row.get("dictionary_id", ""))
        lat = row.get("latitude")
        lon = row.get("longitude")
        if did and pd.notna(lat) and pd.notna(lon):
            result[did] = (float(lat), float(lon))
    return result


# ─── Reconciliation ─────────────────────────────────────────────────────────

def reconcile_plants() -> pd.DataFrame:
    """
    Reconcile UK power plants across all data sources.

    Returns a unified DataFrame saved as parquet.
    """
    logger.info("Starting plant entity reconciliation...")

    fuel_mapping = _load_fuel_mapping()
    dukes = _load_dukes()
    wri = _load_wri()
    repd = _load_repd()

    logger.info(
        f"Source counts: DUKES={len(dukes)}, WRI={len(wri)}, REPD={len(repd)}"
    )

    all_plants: list[dict[str, Any]] = []
    covered_names: set[str] = set()

    # ─── Phase 1: DUKES (ground truth for operational) ───
    dukes_count = 0
    if not dukes.empty:
        fuel_col = "fuel" if "fuel" in dukes.columns else "Primary Fuel"
        for _, row in dukes.iterrows():
            name = str(row.get("name", "")).strip()
            if not name:
                continue

            raw_fuel = str(row.get(fuel_col, ""))
            fuel_type = _map_fuel_type(raw_fuel, fuel_mapping)

            # Override known pumped storage
            name_lower = name.lower()
            if any(ps in name_lower for ps in PUMPED_STORAGE_STATIONS):
                fuel_type = "hydro_pumped_storage"

            lat = float(row["lat"]) if pd.notna(row.get("lat")) else None
            lon = float(row["lon"]) if pd.notna(row.get("lon")) else None
            cap = float(row["capacity_mw"]) if pd.notna(row.get("capacity_mw")) else None

            plant = {
                "plant_id": _make_plant_id(name, "dukes"),
                "name": name,
                "lat": lat,
                "lon": lon,
                "fuel_type": fuel_type,
                "technology": str(row.get("technology", raw_fuel)),
                "capacity_mw": cap,
                "status": "operational",
                "owner": str(row.get("company", "")) or None,
                "dukes_id": str(row.get("dukes_id", "")) or None,
                "wri_id": None,
                "repd_id": None,
                "bmu_ids": [],
                "commissioned_year": (
                    int(row["year_commissioned"])
                    if pd.notna(row.get("year_commissioned"))
                    else None
                ),
                "gsp_group": None,
                "dno_region": str(row.get("region", "")) or None,
                "source_dukes": True,
                "source_wri": False,
                "source_repd": False,
                "source_osuked": False,
            }
            all_plants.append(plant)
            covered_names.add(_normalise_name(name))
            dukes_count += 1

    logger.info(f"Phase 1: {dukes_count} operational plants from DUKES ({sum(p.get('capacity_mw') or 0 for p in all_plants):,.0f} MW)")

    # ─── Phase 2: WRI (supplement — skip duplicates and closed plants) ───
    wri_count = 0
    wri_skipped_coal = 0
    wri_skipped_dupe = 0
    if not wri.empty:
        name_col = next((c for c in wri.columns if c.lower() == "name"), "name")
        fuel_col = next((c for c in wri.columns if "fuel" in c.lower()), "primary_fuel")
        cap_col = "capacity_mw"

        for _, row in wri.iterrows():
            name = str(row.get(name_col, "")).strip()
            if not name:
                continue

            raw_fuel = str(row.get(fuel_col, ""))
            fuel_type = _map_fuel_type(raw_fuel, fuel_mapping)

            # Skip WRI coal — all UK coal is closed
            if fuel_type == "coal":
                wri_skipped_coal += 1
                continue

            # Override pumped storage
            name_lower = name.lower()
            if any(ps in name_lower for ps in PUMPED_STORAGE_STATIONS):
                fuel_type = "hydro_pumped_storage"

            # Dedup: normalised name check + substring matching
            norm = _normalise_name(name)
            if norm in covered_names:
                wri_skipped_dupe += 1
                continue

            # Substring check — "Pembroke" should match "Pembroke Power Station"
            is_sub_match = any(
                norm in cn or cn in norm
                for cn in covered_names
                if len(cn) >= 4 and len(norm) >= 4
            )
            if is_sub_match:
                wri_skipped_dupe += 1
                continue

            cap = float(row[cap_col]) if pd.notna(row.get(cap_col)) else None
            lat = float(row.get("latitude", np.nan)) if pd.notna(row.get("latitude")) else None
            lon = float(row.get("longitude", np.nan)) if pd.notna(row.get("longitude")) else None

            # WRI-only plants are "operational" per WRI but NOT in DUKES
            # (the official government register). Many are stale/closed.
            # Mark as "operational_unverified" to distinguish from DUKES-confirmed.
            plant = {
                "plant_id": _make_plant_id(name, "wri"),
                "name": name,
                "lat": lat,
                "lon": lon,
                "fuel_type": fuel_type,
                "technology": raw_fuel,
                "capacity_mw": cap,
                "status": "operational_unverified",
                "owner": str(row.get("owner", "")) or None,
                "dukes_id": None,
                "wri_id": str(row.get("gppd_idnr", "")) or None,
                "repd_id": None,
                "bmu_ids": [],
                "commissioned_year": (
                    int(row["commissioning_year"])
                    if pd.notna(row.get("commissioning_year"))
                    else None
                ),
                "gsp_group": None,
                "dno_region": None,
                "source_dukes": False,
                "source_wri": True,
                "source_repd": False,
                "source_osuked": False,
            }
            all_plants.append(plant)
            covered_names.add(norm)
            wri_count += 1

    logger.info(
        f"Phase 2: {wri_count} WRI plants added "
        f"(skipped {wri_skipped_coal} coal, {wri_skipped_dupe} duplicates)"
    )

    # ─── Phase 3: REPD (renewable projects — all statuses) ───
    repd_count = 0
    repd_skipped = 0
    if not repd.empty:
        # Map REPD status properly
        status_col = "status" if "status" in repd.columns else None
        dev_short_col = next(
            (c for c in repd.columns if "development_status_short" in c), None
        )

        for _, row in repd.iterrows():
            name = str(row.get("name", row.get("site_name", ""))).strip()
            if not name or name.startswith("plant_"):
                continue

            # Determine status
            raw_status = str(row.get(status_col, "unknown")).lower().strip() if status_col else "unknown"
            dev_status = str(row.get(dev_short_col, "")).lower().strip() if dev_short_col else ""

            if raw_status == "operational" or dev_status == "operational":
                status = "operational"
            elif raw_status == "construction" or dev_status in ("under construction",):
                status = "construction"
            elif raw_status == "consented" or dev_status in ("awaiting construction",):
                status = "consented"
            elif raw_status == "planning" or dev_status in (
                "planning application submitted", "revised",
            ):
                status = "planning"
            elif raw_status in ("refused", "withdrawn", "expired") or dev_status in (
                "application refused", "appeal refused",
                "application withdrawn", "appeal withdrawn", "abandoned",
            ):
                continue  # Skip
            else:
                status = raw_status if raw_status != "unknown" else "unknown"

            # Fast dedup: normalised name check
            norm = _normalise_name(name)
            if norm in covered_names:
                repd_skipped += 1
                continue

            # Coordinates
            lat = float(row["lat"]) if pd.notna(row.get("lat")) else None
            lon = float(row["lon"]) if pd.notna(row.get("lon")) else None

            tech_raw = str(row.get("technology", ""))
            fuel_type = _map_fuel_type(tech_raw, fuel_mapping)

            plant = {
                "plant_id": _make_plant_id(name, "repd"),
                "name": name,
                "lat": lat,
                "lon": lon,
                "fuel_type": fuel_type,
                "technology": tech_raw,
                "capacity_mw": cap,
                "status": status,
                "owner": str(row.get("developer", "")) or None,
                "dukes_id": None,
                "wri_id": None,
                "repd_id": str(row.get("repd_id", "")) or None,
                "bmu_ids": [],
                "commissioned_year": None,
                "gsp_group": None,
                "dno_region": str(row.get("region", "")) or None,
                "source_dukes": False,
                "source_wri": False,
                "source_repd": True,
                "source_osuked": False,
            }
            all_plants.append(plant)
            covered_names.add(norm)
            repd_count += 1

    logger.info(f"Phase 3: {repd_count} REPD projects added (skipped {repd_skipped} duplicates)")

    # ─── Build DataFrame ───
    df = pd.DataFrame(all_plants)

    # Validate
    op = df[df["status"] == "operational"]
    op_cap = op["capacity_mw"].sum()

    logger.success(
        f"Reconciliation complete: {len(df)} plants → {PLANTS_UNIFIED}\n"
        f"  Operational: {len(op)} plants, {op_cap:,.0f} MW\n"
        f"  Sources: DUKES={df['source_dukes'].sum()}, "
        f"WRI={df['source_wri'].sum()}, "
        f"REPD={df['source_repd'].sum()}"
    )

    # Sanity check: operational capacity should be ~82 GW
    if op_cap > 100_000:
        logger.warning(
            f"Operational capacity {op_cap:,.0f} MW seems high "
            f"(DUKES 2024 = ~82 GW). Check for duplicates."
        )
    elif op_cap < 60_000:
        logger.warning(
            f"Operational capacity {op_cap:,.0f} MW seems low "
            f"(DUKES 2024 = ~82 GW). Check for missing sources."
        )

    # Save
    PLANTS_UNIFIED.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(PLANTS_UNIFIED, index=False)

    return df


if __name__ == "__main__":
    reconcile_plants()
