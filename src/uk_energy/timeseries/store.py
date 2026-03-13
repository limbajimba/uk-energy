"""
store.py — DuckDB time-series storage for UK energy system data.

Single append-only analytical database. Columnar storage optimised for
time-range aggregations (typical quant desk queries).

Tables:
  generation       Half-hourly generation by fuel (BMRS, transmission-metered)
  demand           Half-hourly INDO + ITSDO demand
  system_prices    Half-hourly SSP/SBP imbalance settlement prices
  ic_flows         Half-hourly bidirectional interconnector flows
  demand_forecast  Day-ahead demand forecast (national + transmission)
  gen_availability Daily generation availability by fuel (includes outages)
  frequency        1-second system frequency (50 Hz nominal)
  carbon_intensity Half-hourly carbon intensity + generation mix %

All inserts are idempotent — duplicate timestamps are silently skipped.
Data is partitioned by month internally via DuckDB's columnar storage.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
from loguru import logger

from uk_energy.config import DATA_DIR

DB_PATH = DATA_DIR / "timeseries.duckdb"

# ─── Schema ──────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS generation (
    timestamp     TIMESTAMPTZ NOT NULL,
    settlement_period INTEGER,
    fuel_type     VARCHAR NOT NULL,
    generation_mw DOUBLE NOT NULL,
    PRIMARY KEY (timestamp, fuel_type)
);

CREATE TABLE IF NOT EXISTS demand (
    timestamp     TIMESTAMPTZ NOT NULL PRIMARY KEY,
    settlement_period INTEGER,
    indo_mw       DOUBLE,
    itsdo_mw      DOUBLE
);

CREATE TABLE IF NOT EXISTS system_prices (
    timestamp     TIMESTAMPTZ NOT NULL PRIMARY KEY,
    settlement_period INTEGER,
    settlement_date VARCHAR,
    ssp_gbp_mwh   DOUBLE,
    sbp_gbp_mwh   DOUBLE,
    niv_mw         DOUBLE
);

CREATE TABLE IF NOT EXISTS ic_flows (
    timestamp     TIMESTAMPTZ NOT NULL,
    settlement_period INTEGER,
    ic_name       VARCHAR NOT NULL,
    flow_mw       DOUBLE NOT NULL,
    PRIMARY KEY (timestamp, ic_name)
);

CREATE TABLE IF NOT EXISTS demand_forecast (
    publish_time       TIMESTAMPTZ NOT NULL,
    forecast_timestamp TIMESTAMPTZ NOT NULL,
    settlement_period  INTEGER,
    national_demand_mw DOUBLE,
    transmission_demand_mw DOUBLE,
    PRIMARY KEY (publish_time, forecast_timestamp)
);

CREATE TABLE IF NOT EXISTS gen_availability (
    forecast_date  DATE NOT NULL,
    fuel_type      VARCHAR NOT NULL,
    available_mw   DOUBLE,
    PRIMARY KEY (forecast_date, fuel_type)
);

CREATE TABLE IF NOT EXISTS frequency (
    timestamp     TIMESTAMPTZ NOT NULL PRIMARY KEY,
    frequency_hz  DOUBLE NOT NULL
);

CREATE TABLE IF NOT EXISTS carbon_intensity (
    timestamp     TIMESTAMPTZ NOT NULL PRIMARY KEY,
    forecast_gco2 DOUBLE,
    actual_gco2   DOUBLE,
    index_label   VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS ingestion_seq START 1;

CREATE TABLE IF NOT EXISTS ingestion_log (
    id            INTEGER PRIMARY KEY DEFAULT (nextval('ingestion_seq')),
    table_name    VARCHAR NOT NULL,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    rows_inserted INTEGER NOT NULL,
    rows_skipped  INTEGER NOT NULL DEFAULT 0,
    time_range_start TIMESTAMPTZ,
    time_range_end   TIMESTAMPTZ,
    source        VARCHAR
);
"""


class TimeSeriesStore:
    """DuckDB-backed time-series store for UK energy data."""

    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._con = duckdb.connect(str(self.db_path))
        self._init_schema()

    def _init_schema(self) -> None:
        """Create tables if they don't exist."""
        for stmt in _SCHEMA.split(";"):
            stmt = stmt.strip()
            if stmt:
                self._con.execute(stmt)

    def close(self) -> None:
        self._con.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ─── Ingestion methods ───────────────────────────────────────────────

    def ingest_generation(self, df: pd.DataFrame) -> int:
        """Ingest generation data. Expects: timestamp, fuel_type, generation_mw, settlement_period."""
        if df.empty:
            return 0
        before = self._count("generation")
        self._con.execute("""
            INSERT OR IGNORE INTO generation (timestamp, settlement_period, fuel_type, generation_mw)
            SELECT timestamp, settlement_period, fuel_type, generation_mw
            FROM df
        """)
        inserted = self._count("generation") - before
        self._log_ingestion("generation", inserted, len(df) - inserted, df["timestamp"], "bmrs")
        return inserted

    def ingest_demand(self, df: pd.DataFrame) -> int:
        """Ingest demand data. Expects: timestamp, settlement_period, demand_mw, transmission_demand_mw."""
        if df.empty:
            return 0
        # Rename columns to match schema
        renamed = df.rename(columns={
            "demand_mw": "indo_mw",
            "transmission_demand_mw": "itsdo_mw",
        })
        before = self._count("demand")
        self._con.execute("""
            INSERT OR IGNORE INTO demand (timestamp, settlement_period, indo_mw, itsdo_mw)
            SELECT timestamp, settlement_period, indo_mw, itsdo_mw
            FROM renamed
        """)
        inserted = self._count("demand") - before
        self._log_ingestion("demand", inserted, len(df) - inserted, df["timestamp"], "bmrs")
        return inserted

    def ingest_system_prices(self, df: pd.DataFrame) -> int:
        """Ingest system prices. Expects: timestamp, settlement_period, settlement_date, ssp_gbp_mwh, sbp_gbp_mwh, niv_mw."""
        if df.empty:
            return 0
        before = self._count("system_prices")
        self._con.execute("""
            INSERT OR IGNORE INTO system_prices
                (timestamp, settlement_period, settlement_date, ssp_gbp_mwh, sbp_gbp_mwh, niv_mw)
            SELECT timestamp, settlement_period, settlement_date, ssp_gbp_mwh, sbp_gbp_mwh, niv_mw
            FROM df
        """)
        inserted = self._count("system_prices") - before
        self._log_ingestion("system_prices", inserted, len(df) - inserted, df["timestamp"], "bmrs")
        return inserted

    def ingest_ic_flows(self, df: pd.DataFrame) -> int:
        """Ingest IC flows. Expects: timestamp, settlement_period, ic_name, flow_mw."""
        if df.empty:
            return 0
        before = self._count("ic_flows")
        self._con.execute("""
            INSERT OR IGNORE INTO ic_flows (timestamp, settlement_period, ic_name, flow_mw)
            SELECT timestamp, settlement_period, ic_name, flow_mw
            FROM df
        """)
        inserted = self._count("ic_flows") - before
        self._log_ingestion("ic_flows", inserted, len(df) - inserted, df["timestamp"], "bmrs")
        return inserted

    def ingest_demand_forecast(self, df: pd.DataFrame) -> int:
        """Ingest demand forecast. Expects: publish_time, forecast_timestamp, settlement_period, national_demand_mw, transmission_demand_mw."""
        if df.empty:
            return 0
        before = self._count("demand_forecast")
        self._con.execute("""
            INSERT OR IGNORE INTO demand_forecast
                (publish_time, forecast_timestamp, settlement_period, national_demand_mw, transmission_demand_mw)
            SELECT publish_time, forecast_timestamp, settlement_period, national_demand_mw, transmission_demand_mw
            FROM df
        """)
        inserted = self._count("demand_forecast") - before
        self._log_ingestion("demand_forecast", inserted, len(df) - inserted, df["forecast_timestamp"], "bmrs")
        return inserted

    def ingest_gen_availability(self, df: pd.DataFrame) -> int:
        """Ingest generation availability. Expects: forecast_date, fuel_type, available_mw."""
        if df.empty:
            return 0
        before = self._count("gen_availability")
        self._con.execute("""
            INSERT OR IGNORE INTO gen_availability (forecast_date, fuel_type, available_mw)
            SELECT forecast_date, fuel_type, available_mw
            FROM df
        """)
        inserted = self._count("gen_availability") - before
        return inserted

    def ingest_frequency(self, df: pd.DataFrame) -> int:
        """Ingest frequency data. Expects: timestamp, frequency_hz."""
        if df.empty:
            return 0
        before = self._count("frequency")
        self._con.execute("""
            INSERT OR IGNORE INTO frequency (timestamp, frequency_hz)
            SELECT timestamp, frequency_hz
            FROM df
        """)
        inserted = self._count("frequency") - before
        self._log_ingestion("frequency", inserted, len(df) - inserted, df["timestamp"], "bmrs")
        return inserted

    # ─── Query methods ───────────────────────────────────────────────────

    def query(self, sql: str, params: list | None = None) -> pd.DataFrame:
        """Run arbitrary SQL and return DataFrame."""
        return self._con.execute(sql, params or []).fetchdf()

    def generation_by_fuel(
        self,
        start: datetime | date | None = None,
        end: datetime | date | None = None,
        fuel_type: str | None = None,
    ) -> pd.DataFrame:
        """Query generation data with optional filters."""
        where = []
        params = []
        if start:
            where.append("timestamp >= ?")
            params.append(start)
        if end:
            where.append("timestamp <= ?")
            params.append(end)
        if fuel_type:
            where.append("fuel_type = ?")
            params.append(fuel_type)
        clause = " AND ".join(where) if where else "TRUE"
        return self._con.execute(
            f"SELECT * FROM generation WHERE {clause} ORDER BY timestamp, fuel_type",
            params,
        ).fetchdf()

    def demand_range(
        self,
        start: datetime | date | None = None,
        end: datetime | date | None = None,
    ) -> pd.DataFrame:
        where = []
        params = []
        if start:
            where.append("timestamp >= ?")
            params.append(start)
        if end:
            where.append("timestamp <= ?")
            params.append(end)
        clause = " AND ".join(where) if where else "TRUE"
        return self._con.execute(
            f"SELECT * FROM demand WHERE {clause} ORDER BY timestamp",
            params,
        ).fetchdf()

    def prices_range(
        self,
        start: datetime | date | None = None,
        end: datetime | date | None = None,
    ) -> pd.DataFrame:
        where = []
        params = []
        if start:
            where.append("timestamp >= ?")
            params.append(start)
        if end:
            where.append("timestamp <= ?")
            params.append(end)
        clause = " AND ".join(where) if where else "TRUE"
        return self._con.execute(
            f"SELECT * FROM system_prices WHERE {clause} ORDER BY timestamp",
            params,
        ).fetchdf()

    def table_stats(self) -> pd.DataFrame:
        """Get row counts and time ranges for all tables."""
        tables = ["generation", "demand", "system_prices", "ic_flows",
                   "demand_forecast", "gen_availability", "frequency", "carbon_intensity"]
        rows = []
        for t in tables:
            try:
                count = self._count(t)
                if count > 0:
                    ts_col = "forecast_date" if t == "gen_availability" else "forecast_timestamp" if t == "demand_forecast" else "timestamp"
                    r = self._con.execute(f"SELECT MIN({ts_col}), MAX({ts_col}) FROM {t}").fetchone()
                    rows.append({"table": t, "rows": count, "earliest": r[0], "latest": r[1]})
                else:
                    rows.append({"table": t, "rows": 0, "earliest": None, "latest": None})
            except Exception:
                rows.append({"table": t, "rows": 0, "earliest": None, "latest": None})
        return pd.DataFrame(rows)

    # ─── Helpers ─────────────────────────────────────────────────────────

    def _count(self, table: str) -> int:
        return self._con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def _log_ingestion(self, table: str, inserted: int, skipped: int, ts_series: pd.Series, source: str) -> None:
        if inserted > 0:
            logger.info(f"Stored {inserted} rows in {table} ({skipped} duplicates skipped) [{source}]")
        try:
            self._con.execute("""
                INSERT INTO ingestion_log (table_name, rows_inserted, rows_skipped, time_range_start, time_range_end, source)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [table, inserted, skipped, ts_series.min(), ts_series.max(), source])
        except Exception:
            pass  # Log table might not exist yet during init
