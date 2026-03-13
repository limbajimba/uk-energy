"""
data.py — Dashboard data layer.

Combines:
  - Static: DUKES asset register (plants, installed capacity)
  - BMRS: half-hourly MW generation, demand, prices, interconnector flows
  - Carbon Intensity API: solar %, regional mix, carbon intensity

Key design: BMRS gives MW but no solar/battery. Carbon Intensity gives
percentages including solar. We combine them: multiply CI percentages
by BMRS total domestic generation to estimate solar MW.
"""

from __future__ import annotations

import functools
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import pandas as pd
from loguru import logger

from uk_energy.config import PLANTS_UNIFIED, INTERCONNECTORS_REF, PROCESSED_DIR, RAW_DIR


@dataclass
class StaticData:
    """Static asset register."""
    plants: pd.DataFrame
    operational: pd.DataFrame
    interconnectors: list[dict]

    # Pre-aggregated
    fuel_capacity: dict[str, float]  # fuel → installed MW
    regional_capacity: dict[str, float]  # region → installed MW
    total_installed_mw: float = 0.0

    # Data source inventory
    sources: list[dict] = field(default_factory=list)


@dataclass
class LiveData:
    """Live data from BMRS + Carbon Intensity."""
    # Raw
    generation: pd.DataFrame
    demand: pd.DataFrame
    prices: pd.DataFrame
    ic_flows: pd.DataFrame
    ci_mix: dict[str, float]  # Carbon Intensity current mix {fuel: %}
    ci_regional: pd.DataFrame
    carbon_intensity: dict  # {forecast, actual, index}

    fetch_time: datetime = field(default_factory=datetime.utcnow)
    n_periods: int = 0

    # Derived (computed from BMRS + CI)
    current_gen: dict[str, float] = field(default_factory=dict)  # fuel → MW (corrected)
    total_domestic_mw: float = 0.0
    total_import_mw: float = 0.0
    demand_mw: float = 0.0
    price_gbp_mwh: float = 0.0
    wind_pct: float = 0.0
    solar_mw: float = 0.0
    carbon_gco2: float = 0.0


def _inventory_sources() -> list[dict]:
    """Check what data files exist and their freshness."""
    import os

    sources = []

    checks = [
        ("DUKES (DESNZ)", PROCESSED_DIR / "dukes_processed.csv"),
        ("WRI Global Power", PROCESSED_DIR / "wri_gb_plants.csv"),
        ("REPD (Renewables)", PROCESSED_DIR / "repd_processed.csv"),
        ("OSUKED Dictionary", PROCESSED_DIR / "osuked_dictionary.csv" if (PROCESSED_DIR / "osuked_dictionary.csv").exists() else RAW_DIR / "osuked" / "ids.csv"),
        ("BMRS BM Units", RAW_DIR / "bmrs" / "bm_units_all.json"),
        ("OSM Substations", RAW_DIR / "osm" / "substations.json"),
        ("Plants Unified", PLANTS_UNIFIED),
        ("Interconnectors", INTERCONNECTORS_REF),
        ("Fuel Type Mapping", PROCESSED_DIR.parent / "reference" / "fuel_type_mapping.json"),
    ]

    for name, path in checks:
        if path.exists():
            stat = path.stat()
            size_mb = stat.st_size / 1024 / 1024
            mtime = datetime.fromtimestamp(stat.st_mtime)
            age_days = (datetime.now() - mtime).days
            rows = "—"
            if path.suffix == ".csv":
                try:
                    rows = f"{sum(1 for _ in open(path)) - 1:,}"
                except Exception:
                    pass
            elif path.suffix == ".parquet":
                try:
                    rows = f"{len(pd.read_parquet(path)):,}"
                except Exception:
                    pass
            elif path.suffix == ".json":
                try:
                    d = json.loads(path.read_text())
                    if isinstance(d, list):
                        rows = f"{len(d):,}"
                    elif isinstance(d, dict) and "data" in d:
                        rows = f"{len(d['data']):,}"
                    elif isinstance(d, dict) and "elements" in d:
                        rows = f"{len(d['elements']):,}"
                except Exception:
                    pass

            sources.append({
                "name": name,
                "path": str(path.name),
                "size_mb": round(size_mb, 1),
                "rows": rows,
                "age_days": age_days,
                "status": "fresh" if age_days < 7 else "stale" if age_days < 30 else "old",
            })
        else:
            sources.append({
                "name": name,
                "path": str(path.name),
                "size_mb": 0,
                "rows": "—",
                "age_days": -1,
                "status": "missing",
            })

    return sources


@functools.lru_cache(maxsize=1)
def load_data() -> StaticData:
    """Load static asset data (cached)."""
    plants = pd.read_parquet(PLANTS_UNIFIED) if PLANTS_UNIFIED.exists() else pd.DataFrame()
    op = plants[plants["status"] == "operational"].copy()
    dukes = op[op["source_dukes"] == True].copy()

    ics = []
    if INTERCONNECTORS_REF.exists():
        ics = json.loads(INTERCONNECTORS_REF.read_text()).get("interconnectors", [])

    fuel_cap = dukes.groupby("fuel_type")["capacity_mw"].sum().to_dict()
    regional_cap = dukes.groupby("dno_region")["capacity_mw"].sum().to_dict()

    sources = _inventory_sources()

    return StaticData(
        plants=plants,
        operational=dukes,
        interconnectors=ics,
        fuel_capacity=fuel_cap,
        regional_capacity=regional_cap,
        total_installed_mw=dukes["capacity_mw"].sum(),
        sources=sources,
    )


def load_live_data() -> LiveData:
    """Fetch latest from BMRS + Carbon Intensity."""
    from uk_energy.timeseries.bmrs_live import fetch_all
    from uk_energy.timeseries.carbon_intensity import (
        fetch_current_mix,
        fetch_intensity,
        fetch_regional_mix,
    )

    today = date.today()
    yesterday = today - timedelta(days=1)

    # BMRS
    try:
        raw = fetch_all(from_date=yesterday, to_date=today)
    except Exception as e:
        logger.error(f"BMRS fetch failed: {e}")
        raw = {"generation": pd.DataFrame(), "demand": pd.DataFrame(),
               "prices": pd.DataFrame(), "interconnectors": pd.DataFrame()}

    gen = raw["generation"]
    demand = raw["demand"]
    prices = raw["prices"]
    ic = raw["interconnectors"]

    # Carbon Intensity
    try:
        ci_mix = fetch_current_mix()
        ci_regional = fetch_regional_mix()
        ci_intensity = fetch_intensity()
    except Exception as e:
        logger.error(f"Carbon Intensity fetch failed: {e}")
        ci_mix = {}
        ci_regional = pd.DataFrame()
        ci_intensity = {"forecast_gco2": 0, "actual_gco2": None, "index": "unknown"}

    # ─── Derive current generation snapshot ───
    # BMRS latest period
    current_gen: dict[str, float] = {}
    total_domestic = 0.0
    total_import = 0.0

    if not gen.empty:
        latest_ts = gen["timestamp"].max()
        latest = gen[gen["timestamp"] == latest_ts]
        for _, r in latest.iterrows():
            ft = r["fuel_type"]
            mw = r["generation_mw"]
            current_gen[ft] = mw
            if ft.startswith("ic_"):
                total_import += max(0, mw)
            else:
                total_domestic += max(0, mw)

    # Estimate solar MW from Carbon Intensity percentage
    solar_pct = ci_mix.get("solar", 0)
    # CI percentages include imports in the total. Domestic gen only:
    # total_with_imports = total_domestic + total_import
    # But CI "solar" is % of total including imports
    total_with_imports = total_domestic + total_import
    solar_mw = total_with_imports * solar_pct / 100 if solar_pct > 0 else 0.0
    current_gen["solar"] = solar_mw

    # Wind share
    wind_mw = current_gen.get("wind", 0)
    wind_pct = (wind_mw / total_domestic * 100) if total_domestic > 0 else 0

    # Latest demand
    demand_mw = demand.iloc[-1].get("demand_mw", 0) if not demand.empty else 0

    # Latest price
    price = prices.iloc[-1].get("price_gbp_mwh", 0) if not prices.empty else 0

    # Carbon intensity
    carbon = ci_intensity.get("actual_gco2") or ci_intensity.get("forecast_gco2", 0)

    return LiveData(
        generation=gen,
        demand=demand,
        prices=prices,
        ic_flows=ic,
        ci_mix=ci_mix,
        ci_regional=ci_regional,
        carbon_intensity=ci_intensity,
        fetch_time=datetime.utcnow(),
        n_periods=gen["timestamp"].nunique() if not gen.empty else 0,
        current_gen=current_gen,
        total_domestic_mw=total_domestic,
        total_import_mw=total_import,
        demand_mw=demand_mw,
        price_gbp_mwh=price,
        wind_pct=wind_pct,
        solar_mw=solar_mw,
        carbon_gco2=carbon,
    )
