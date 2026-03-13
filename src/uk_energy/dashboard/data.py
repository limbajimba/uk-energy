"""
data.py — Data loading for dashboard. Static assets + live BMRS feed.
"""

from __future__ import annotations

import functools
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import pandas as pd
from loguru import logger

from uk_energy.config import PLANTS_UNIFIED, INTERCONNECTORS_REF


@dataclass
class StaticData:
    """Static asset register (changes rarely)."""
    plants: pd.DataFrame
    operational: pd.DataFrame
    interconnectors: list[dict]
    fuel_capacity: pd.DataFrame  # fuel_type → installed MW
    regional_capacity: pd.DataFrame  # region → installed MW


@dataclass
class LiveData:
    """Live/recent time-series from BMRS."""
    generation: pd.DataFrame  # half-hourly generation by fuel
    demand: pd.DataFrame  # half-hourly demand
    prices: pd.DataFrame  # day-ahead prices
    ic_flows: pd.DataFrame  # interconnector flows
    fetch_time: datetime = field(default_factory=datetime.utcnow)
    n_periods: int = 0

    # Derived
    latest_gen: dict[str, float] = field(default_factory=dict)  # fuel → MW
    latest_demand_mw: float = 0.0
    latest_price: float = 0.0
    wind_share_pct: float = 0.0
    total_generation_mw: float = 0.0


@functools.lru_cache(maxsize=1)
def load_data() -> StaticData:
    """Load static asset data (cached)."""
    plants = pd.read_parquet(PLANTS_UNIFIED) if PLANTS_UNIFIED.exists() else pd.DataFrame()
    op = plants[plants["status"] == "operational"].copy()
    dukes = op[op["source_dukes"] == True].copy()

    ics = []
    if INTERCONNECTORS_REF.exists():
        ics = json.loads(INTERCONNECTORS_REF.read_text()).get("interconnectors", [])

    fuel_cap = (
        dukes.groupby("fuel_type")["capacity_mw"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )

    regional = (
        dukes.groupby("dno_region")["capacity_mw"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )

    return StaticData(
        plants=plants,
        operational=dukes,
        interconnectors=ics,
        fuel_capacity=fuel_cap,
        regional_capacity=regional,
    )


def load_live_data() -> LiveData:
    """Fetch latest BMRS data (not cached — called on refresh)."""
    from uk_energy.timeseries.bmrs_live import fetch_all

    today = date.today()
    yesterday = today - timedelta(days=1)

    try:
        raw = fetch_all(from_date=yesterday, to_date=today)
    except Exception as e:
        logger.error(f"BMRS fetch failed: {e}")
        return LiveData(
            generation=pd.DataFrame(),
            demand=pd.DataFrame(),
            prices=pd.DataFrame(),
            ic_flows=pd.DataFrame(),
        )

    gen = raw["generation"]
    demand = raw["demand"]
    prices = raw["prices"]
    ic = raw["interconnectors"]

    # Compute latest snapshot
    latest_gen: dict[str, float] = {}
    total_gen = 0.0
    wind_gen = 0.0
    if not gen.empty:
        latest_ts = gen["timestamp"].max()
        latest = gen[gen["timestamp"] == latest_ts]
        for _, r in latest.iterrows():
            ft = r["fuel_type"]
            mw = r["generation_mw"]
            latest_gen[ft] = mw
            if not ft.startswith("ic_"):
                total_gen += max(0, mw)
            if ft == "wind":
                wind_gen = mw

    latest_demand = 0.0
    if not demand.empty:
        latest_demand = demand.iloc[-1].get("demand_mw", 0)

    latest_price = 0.0
    if not prices.empty:
        latest_price = prices.iloc[-1].get("price_gbp_mwh", 0)

    return LiveData(
        generation=gen,
        demand=demand,
        prices=prices,
        ic_flows=ic,
        fetch_time=datetime.utcnow(),
        n_periods=gen["timestamp"].nunique() if not gen.empty else 0,
        latest_gen=latest_gen,
        latest_demand_mw=latest_demand,
        latest_price=latest_price,
        wind_share_pct=(wind_gen / total_gen * 100) if total_gen > 0 else 0,
        total_generation_mw=total_gen,
    )
