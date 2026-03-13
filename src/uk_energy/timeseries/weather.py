"""
weather.py — Open-Meteo weather data for UK energy forecasting.

Free, no API key, generous rate limits (~10k requests/day).

Fetches hourly weather at representative UK energy locations:
  - Offshore wind hubs (Dogger Bank, Greater Wash, Moray Firth)
  - Onshore wind zones (Highlands, Welsh hills, Pennines)
  - Solar zones (Cornwall, East Anglia, South England)
  - Demand-weighted population centres (London, Birmingham, Manchester)

Variables captured:
  - wind_speed_100m: hub-height wind for modern turbines (km/h)
  - wind_speed_10m: reference height (km/h)
  - wind_direction_100m: wind direction at hub height (degrees)
  - shortwave_radiation: global horizontal irradiance GHI (W/m²)
  - direct_normal_irradiance: DNI for tracking solar (W/m²)
  - temperature_2m: air temperature (°C) — demand driver
  - cloud_cover: cloud fraction (%) — solar proxy
  - relative_humidity_2m: humidity (%) — demand driver

Coordinate sources:
  - Crown Estate lease areas for offshore wind
  - Met Office observation stations for representative coverage
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import httpx
import pandas as pd
from loguru import logger

BASE_URL = "https://api.open-meteo.com/v1/forecast"

# ─── Representative UK energy locations ──────────────────────────────────────

@dataclass(frozen=True)
class WeatherSite:
    """A representative weather observation point."""
    name: str
    lat: float
    lon: float
    category: str  # "offshore_wind" | "onshore_wind" | "solar" | "demand"


SITES: list[WeatherSite] = [
    # Offshore wind hubs (where the GW-scale farms are)
    WeatherSite("dogger_bank", 54.75, 1.87, "offshore_wind"),
    WeatherSite("greater_wash", 53.2, 1.5, "offshore_wind"),
    WeatherSite("moray_firth", 57.8, -3.0, "offshore_wind"),
    WeatherSite("irish_sea", 53.5, -3.7, "offshore_wind"),

    # Onshore wind zones
    WeatherSite("highlands", 57.5, -5.0, "onshore_wind"),
    WeatherSite("southern_uplands", 55.3, -3.5, "onshore_wind"),
    WeatherSite("pennines", 54.5, -2.3, "onshore_wind"),
    WeatherSite("welsh_hills", 52.3, -3.5, "onshore_wind"),

    # Solar zones (south/east facing)
    WeatherSite("cornwall", 50.3, -5.0, "solar"),
    WeatherSite("east_anglia", 52.2, 1.0, "solar"),
    WeatherSite("south_england", 51.0, -1.0, "solar"),

    # Demand centres (temperature → heating/cooling demand)
    WeatherSite("london", 51.5, -0.12, "demand"),
    WeatherSite("birmingham", 52.48, -1.9, "demand"),
    WeatherSite("manchester", 53.48, -2.24, "demand"),
    WeatherSite("edinburgh", 55.95, -3.19, "demand"),
]

HOURLY_VARS = [
    "wind_speed_100m",
    "wind_speed_10m",
    "wind_direction_100m",
    "shortwave_radiation",
    "direct_normal_irradiance",
    "temperature_2m",
    "cloud_cover",
    "relative_humidity_2m",
]


def fetch_weather(
    sites: list[WeatherSite] | None = None,
    past_days: int = 7,
    forecast_days: int = 3,
) -> pd.DataFrame:
    """
    Fetch hourly weather for all sites.

    Returns DataFrame: timestamp, site, category, lat, lon, + weather variables.
    Past data is reanalysis (ERA5), forecast is GFS/ICON ensemble.
    """
    sites = sites or SITES

    lats = ",".join(str(s.lat) for s in sites)
    lons = ",".join(str(s.lon) for s in sites)

    params = {
        "latitude": lats,
        "longitude": lons,
        "hourly": ",".join(HOURLY_VARS),
        "past_days": past_days,
        "forecast_days": forecast_days,
        "timezone": "UTC",
        "wind_speed_unit": "ms",  # m/s not km/h — standard for turbine power curves
    }

    r = httpx.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    # Multi-location returns a list
    if isinstance(data, dict):
        data = [data]

    all_rows = []
    for i, site_data in enumerate(data):
        site = sites[i]
        hourly = site_data.get("hourly", {})
        times = hourly.get("time", [])

        for j, t in enumerate(times):
            row = {
                "timestamp": pd.Timestamp(t, tz="UTC"),
                "site": site.name,
                "category": site.category,
                "lat": site.lat,
                "lon": site.lon,
            }
            for var in HOURLY_VARS:
                values = hourly.get(var, [])
                row[var] = values[j] if j < len(values) else None
            all_rows.append(row)

    df = pd.DataFrame(all_rows)
    if not df.empty:
        df = df.sort_values(["timestamp", "site"])

    n_sites = df["site"].nunique() if not df.empty else 0
    n_hours = df["timestamp"].nunique() if not df.empty else 0
    logger.info(f"Fetched weather: {len(df)} rows, {n_sites} sites, {n_hours} hours")
    return df


def fetch_wind_index(past_days: int = 7, forecast_days: int = 3) -> pd.DataFrame:
    """
    Compute a capacity-weighted wind index from representative sites.

    Returns hourly DataFrame with:
      - offshore_wind_ms: capacity-weighted average wind speed at offshore sites
      - onshore_wind_ms: capacity-weighted average wind speed at onshore sites
      - solar_ghi_wm2: average GHI across solar sites
      - temperature_c: population-weighted average temperature

    These are the features you'd feed into a generation forecast model.
    """
    df = fetch_weather(past_days=past_days, forecast_days=forecast_days)
    if df.empty:
        return pd.DataFrame()

    # Offshore wind index (average 100m wind speed across offshore sites)
    offshore = df[df["category"] == "offshore_wind"].groupby("timestamp").agg(
        offshore_wind_ms=("wind_speed_100m", "mean"),
    )

    # Onshore wind index
    onshore = df[df["category"] == "onshore_wind"].groupby("timestamp").agg(
        onshore_wind_ms=("wind_speed_100m", "mean"),
    )

    # Solar index (average GHI across solar sites)
    solar = df[df["category"] == "solar"].groupby("timestamp").agg(
        solar_ghi_wm2=("shortwave_radiation", "mean"),
        solar_dni_wm2=("direct_normal_irradiance", "mean"),
        cloud_cover_pct=("cloud_cover", "mean"),
    )

    # Temperature index (demand centres)
    temp = df[df["category"] == "demand"].groupby("timestamp").agg(
        temperature_c=("temperature_2m", "mean"),
        humidity_pct=("relative_humidity_2m", "mean"),
    )

    index = offshore.join(onshore).join(solar).join(temp)
    index = index.reset_index()

    logger.info(f"Wind index: {len(index)} hours, offshore mean {index['offshore_wind_ms'].mean():.1f} m/s")
    return index
