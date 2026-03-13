"""
data.py — Data loading and caching for the dashboard.

All dashboard pages read from this module. Data is loaded once and cached.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass, field

import pandas as pd
from loguru import logger

from uk_energy.config import PLANTS_UNIFIED, INTERCONNECTORS_REF

import json


@dataclass
class DashboardData:
    """Container for all dashboard data."""

    plants: pd.DataFrame
    operational: pd.DataFrame
    dukes_operational: pd.DataFrame
    interconnectors: list[dict]

    # Pre-computed summaries
    total_capacity_mw: float = 0.0
    total_operational_mw: float = 0.0
    n_plants: int = 0
    n_operational: int = 0
    fuel_mix: pd.DataFrame = field(default_factory=pd.DataFrame)
    regional_capacity: pd.DataFrame = field(default_factory=pd.DataFrame)
    status_summary: pd.DataFrame = field(default_factory=pd.DataFrame)


@functools.lru_cache(maxsize=1)
def load_data() -> DashboardData:
    """Load and cache all dashboard data."""
    logger.info("Loading dashboard data...")

    # Plants
    plants = pd.read_parquet(PLANTS_UNIFIED) if PLANTS_UNIFIED.exists() else pd.DataFrame()
    operational = plants[plants["status"] == "operational"].copy()
    dukes_op = operational[operational["source_dukes"] == True].copy()

    # Interconnectors
    ics = []
    if INTERCONNECTORS_REF.exists():
        ic_data = json.loads(INTERCONNECTORS_REF.read_text())
        ics = ic_data.get("interconnectors", [])

    # Pre-compute summaries
    fuel_mix = (
        dukes_op.groupby("fuel_type")["capacity_mw"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    fuel_mix.columns = ["fuel_type", "capacity_mw"]

    regional = (
        dukes_op.groupby("dno_region")["capacity_mw"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    regional.columns = ["region", "capacity_mw"]

    status_summary = (
        plants.groupby("status")
        .agg(count=("name", "size"), capacity_mw=("capacity_mw", "sum"))
        .sort_values("capacity_mw", ascending=False)
        .reset_index()
    )

    data = DashboardData(
        plants=plants,
        operational=operational,
        dukes_operational=dukes_op,
        interconnectors=ics,
        total_capacity_mw=plants["capacity_mw"].sum(),
        total_operational_mw=dukes_op["capacity_mw"].sum(),
        n_plants=len(plants),
        n_operational=len(dukes_op),
        fuel_mix=fuel_mix,
        regional_capacity=regional,
        status_summary=status_summary,
    )

    logger.info(
        f"Dashboard data loaded: {data.n_plants} plants, "
        f"{data.total_operational_mw:,.0f} MW operational"
    )
    return data
