"""
data.py — Dashboard data layer.

Combines:
  - Static: DUKES-verified asset register (plants, installed capacity)
  - BMRS: half-hourly generation, demand, system prices, IC flows
  - Carbon Intensity API: solar %, regional mix, gCO₂/kWh

Data accuracy notes:
  - BMRS generation is transmission-metered only (~70% of actual generation).
    Embedded solar, small wind, CHP are invisible.
  - BMRS "OTHER" ≈ pumped storage gen + embedded gen. Not decomposable.
  - System prices (SSP/SBP) are imbalance settlement prices, NOT day-ahead.
  - IC flows from generation summary are import-only. We use the dedicated
    IC endpoint which has bidirectional data (+import, -export).
  - Carbon Intensity API percentages are modelled estimates, not metered.
"""

from __future__ import annotations

import functools
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from loguru import logger

from uk_energy.config import PLANTS_UNIFIED, INTERCONNECTORS_REF, PROCESSED_DIR, RAW_DIR


@dataclass
class StaticData:
    """Static asset register from DUKES + reconciliation."""
    plants: pd.DataFrame
    operational: pd.DataFrame
    interconnectors: list[dict]

    fuel_capacity: dict[str, float]  # fuel → installed MW (DUKES-verified only)
    regional_capacity: dict[str, float]  # region → installed MW
    total_installed_mw: float = 0.0

    sources: list[dict] = field(default_factory=list)


@dataclass
class LiveData:
    """Live data from BMRS + Carbon Intensity API."""
    # Raw DataFrames
    generation: pd.DataFrame  # Domestic gen by fuel (excludes IC rows)
    demand: pd.DataFrame
    prices: pd.DataFrame  # SSP/SBP system prices
    ic_flows: pd.DataFrame  # Bidirectional IC flows from dedicated endpoint
    ci_mix: dict[str, float]  # {fuel: pct} from Carbon Intensity
    ci_regional: pd.DataFrame  # Regional mix (14 DNO regions only, no aggregates)
    carbon_intensity: dict  # {forecast_gco2, actual_gco2, index}

    fetch_time: datetime = field(default_factory=datetime.utcnow)
    n_periods: int = 0

    # Derived from latest period
    current_gen: dict[str, float] = field(default_factory=dict)  # fuel → MW (domestic only)
    current_ic: dict[str, float] = field(default_factory=dict)  # ic_name → MW (signed)
    total_domestic_mw: float = 0.0
    total_import_mw: float = 0.0
    total_export_mw: float = 0.0
    net_ic_mw: float = 0.0  # positive = net import
    demand_mw: float = 0.0
    ssp_gbp_mwh: float = 0.0  # System Sell Price
    sbp_gbp_mwh: float = 0.0  # System Buy Price
    niv_mw: float = 0.0  # Net Imbalance Volume
    wind_pct: float = 0.0
    solar_mw: float = 0.0
    carbon_gco2: float = 0.0


@dataclass
class HistoricalData:
    """Historical data from DuckDB store."""
    prices: pd.DataFrame  # 30d SSP/SBP
    market_depth: pd.DataFrame
    wind_forecast: pd.DataFrame
    demand_forecast: pd.DataFrame
    gen_availability: pd.DataFrame
    weather_index: pd.DataFrame
    frequency_stats: dict = field(default_factory=dict)
    store_stats: pd.DataFrame = field(default_factory=pd.DataFrame)


def load_historical() -> HistoricalData:
    """Load historical data from DuckDB. Non-cached (fresh each call)."""
    from uk_energy.timeseries.store import TimeSeriesStore, DB_PATH

    if not DB_PATH.exists():
        return HistoricalData(
            prices=pd.DataFrame(), market_depth=pd.DataFrame(),
            wind_forecast=pd.DataFrame(), demand_forecast=pd.DataFrame(),
            gen_availability=pd.DataFrame(), weather_index=pd.DataFrame(),
        )

    store = TimeSeriesStore()
    try:
        prices = store.query("SELECT * FROM system_prices ORDER BY timestamp")
        md = store.query("SELECT * FROM market_depth ORDER BY timestamp")
        wf = store.query("SELECT * FROM wind_forecast ORDER BY timestamp")
        df_fc = store.query("SELECT * FROM demand_forecast ORDER BY forecast_timestamp")
        ga = store.query("SELECT * FROM gen_availability ORDER BY forecast_date, fuel_type")
        wi = store.query("SELECT * FROM weather_index ORDER BY timestamp")
        stats = store.table_stats()

        # Frequency summary
        freq_stats = {}
        try:
            r = store.query("""
                SELECT ROUND(AVG(frequency_hz), 4) as mean,
                       ROUND(MIN(frequency_hz), 4) as min_f,
                       ROUND(MAX(frequency_hz), 4) as max_f,
                       ROUND(STDDEV(frequency_hz), 4) as stddev,
                       COUNT(*) as total,
                       COUNT(*) FILTER (WHERE frequency_hz < 49.8) as below_49_8,
                       COUNT(*) FILTER (WHERE frequency_hz > 50.2) as above_50_2
                FROM frequency
            """)
            if not r.empty:
                freq_stats = r.iloc[0].to_dict()
        except Exception:
            pass

        return HistoricalData(
            prices=prices, market_depth=md, wind_forecast=wf,
            demand_forecast=df_fc, gen_availability=ga,
            weather_index=wi, frequency_stats=freq_stats,
            store_stats=stats,
        )
    finally:
        store.close()


def _inventory_sources() -> list[dict]:
    """Check what data files exist and their freshness."""
    sources = []
    checks = [
        ("DUKES (DESNZ)", PROCESSED_DIR / "dukes_processed.csv"),
        ("WRI Global Power", PROCESSED_DIR / "wri_gb_plants.csv"),
        ("REPD (Renewables)", PROCESSED_DIR / "repd_processed.csv"),
        ("OSUKED Dictionary", RAW_DIR / "osuked" / "ids.csv"),
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
                "name": name, "path": str(path.name),
                "size_mb": round(size_mb, 1), "rows": rows,
                "age_days": age_days,
                "status": "fresh" if age_days < 7 else "stale" if age_days < 30 else "old",
            })
        else:
            sources.append({
                "name": name, "path": str(path.name),
                "size_mb": 0, "rows": "—", "age_days": -1, "status": "missing",
            })

    return sources


@functools.lru_cache(maxsize=1)
def load_data() -> StaticData:
    """Load static asset data (cached for app lifetime)."""
    plants = pd.read_parquet(PLANTS_UNIFIED) if PLANTS_UNIFIED.exists() else pd.DataFrame()
    op = plants[plants["status"] == "operational"].copy()
    dukes = op[op["source_dukes"] == True].copy()

    ics = []
    if INTERCONNECTORS_REF.exists():
        ics = json.loads(INTERCONNECTORS_REF.read_text()).get("interconnectors", [])

    fuel_cap = dukes.groupby("fuel_type")["capacity_mw"].sum().to_dict()
    regional_cap = dukes.groupby("dno_region")["capacity_mw"].sum().to_dict()

    return StaticData(
        plants=plants, operational=dukes, interconnectors=ics,
        fuel_capacity=fuel_cap, regional_capacity=regional_cap,
        total_installed_mw=dukes["capacity_mw"].sum(),
        sources=_inventory_sources(),
    )


def load_live_data() -> LiveData:
    """Fetch latest from BMRS + Carbon Intensity. Each source independent."""
    from uk_energy.timeseries.bmrs_live import fetch_all
    from uk_energy.timeseries.carbon_intensity import (
        fetch_current_mix,
        fetch_intensity,
        fetch_regional_mix,
    )

    today = date.today()
    yesterday = today - timedelta(days=1)

    # BMRS — each endpoint fetched independently (one failure doesn't block others)
    raw = fetch_all(from_date=yesterday, to_date=today)
    gen = raw.get("generation", pd.DataFrame())
    demand = raw.get("demand", pd.DataFrame())
    prices = raw.get("prices", pd.DataFrame())
    ic_flows = raw.get("interconnectors", pd.DataFrame())

    # Carbon Intensity — each call independent
    ci_mix: dict[str, float] = {}
    ci_regional = pd.DataFrame()
    ci_intensity: dict = {"forecast_gco2": 0, "actual_gco2": None, "index": "unknown"}

    try:
        ci_mix = fetch_current_mix()
    except Exception as e:
        logger.error(f"Carbon Intensity mix failed: {e}")

    try:
        ci_regional_raw = fetch_regional_mix()
        # Filter to 14 actual DNO regions (IDs 1-14), exclude aggregates (15-18)
        if not ci_regional_raw.empty and "region_id" in ci_regional_raw.columns:
            ci_regional = ci_regional_raw[ci_regional_raw["region_id"] <= 14].copy()
        else:
            ci_regional = ci_regional_raw
    except Exception as e:
        logger.error(f"Carbon Intensity regional failed: {e}")

    try:
        ci_intensity = fetch_intensity()
    except Exception as e:
        logger.error(f"Carbon Intensity intensity failed: {e}")

    # ─── Derive current generation snapshot (DOMESTIC ONLY) ───
    current_gen: dict[str, float] = {}
    total_domestic = 0.0

    if not gen.empty:
        # Only domestic generation (not ICs from gen summary)
        domestic_gen = gen[~gen["is_ic"]]
        if not domestic_gen.empty:
            latest_ts = domestic_gen["timestamp"].max()
            latest = domestic_gen[domestic_gen["timestamp"] == latest_ts]
            for _, r in latest.iterrows():
                ft = r["fuel_type"]
                mw = r["generation_mw"]
                current_gen[ft] = mw
                total_domestic += max(0, mw)

    # ─── Derive IC snapshot from DEDICATED endpoint (bidirectional) ───
    current_ic: dict[str, float] = {}
    total_import = 0.0
    total_export = 0.0

    if not ic_flows.empty:
        latest_ic_ts = ic_flows["timestamp"].max()
        latest_ics = ic_flows[ic_flows["timestamp"] == latest_ic_ts]
        for _, r in latest_ics.iterrows():
            name = r["ic_name"]
            flow = r["flow_mw"]
            current_ic[name] = flow
            if flow > 0:
                total_import += flow
            else:
                total_export += abs(flow)

    net_ic = total_import - total_export

    # ─── Solar MW estimate from Carbon Intensity ───
    solar_pct = ci_mix.get("solar", 0)
    # CI percentages cover all generation including embedded + imports
    # Rough estimate: solar_mw ≈ (total_domestic + net_ic) × solar_pct / 100
    total_supply = total_domestic + net_ic
    solar_mw = total_supply * solar_pct / 100 if solar_pct > 0 else 0.0
    current_gen["solar"] = solar_mw

    # ─── Wind share ───
    wind_mw = current_gen.get("wind", 0)
    wind_pct = (wind_mw / total_domestic * 100) if total_domestic > 0 else 0

    # ─── Latest demand (INDO) ───
    demand_mw = demand.iloc[-1].get("demand_mw", 0) if not demand.empty else 0

    # ─── Latest system prices ───
    ssp = 0.0
    sbp = 0.0
    niv = 0.0
    if not prices.empty:
        latest_price = prices.iloc[-1]
        ssp = latest_price.get("ssp_gbp_mwh", 0)
        sbp = latest_price.get("sbp_gbp_mwh", 0)
        niv = latest_price.get("niv_mw", 0)

    # ─── Carbon intensity ───
    carbon = ci_intensity.get("actual_gco2") or ci_intensity.get("forecast_gco2", 0)

    # Separate domestic gen from IC gen for the generation DataFrame
    gen_domestic = gen[~gen["is_ic"]].copy() if not gen.empty else pd.DataFrame()

    return LiveData(
        generation=gen_domestic,
        demand=demand,
        prices=prices,
        ic_flows=ic_flows,
        ci_mix=ci_mix,
        ci_regional=ci_regional,
        carbon_intensity=ci_intensity,
        fetch_time=datetime.utcnow(),
        n_periods=gen["timestamp"].nunique() if not gen.empty else 0,
        current_gen=current_gen,
        current_ic=current_ic,
        total_domestic_mw=total_domestic,
        total_import_mw=total_import,
        total_export_mw=total_export,
        net_ic_mw=net_ic,
        demand_mw=demand_mw,
        ssp_gbp_mwh=ssp,
        sbp_gbp_mwh=sbp,
        niv_mw=niv,
        wind_pct=wind_pct,
        solar_mw=solar_mw,
        carbon_gco2=carbon,
    )
