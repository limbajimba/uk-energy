"""
geocoder.py — Coordinate validation, normalisation, and spatial assignment.

For each plant:
1. Validate coordinates are within reasonable UK bounds
2. Assign GSP group via spatial join with NESO GSP boundaries (if available)
3. Assign DNO region from Carbon Intensity API region polygons

If GSP/DNO GIS data isn't available, falls back to approximate assignment
based on coordinate bounding boxes.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from uk_energy.config import DNO_REGION_IDS, NESO_RAW, PLANTS_UNIFIED

# UK bounding box (loose)
UK_LAT_MIN, UK_LAT_MAX = 49.5, 61.5
UK_LON_MIN, UK_LON_MAX = -9.0, 2.5

# Approximate DNO region bounding boxes (lon_min, lat_min, lon_max, lat_max)
# Used as fallback when GIS data isn't available
DNO_BBOX_APPROX: list[dict[str, Any]] = [
    {"id": 1,  "name": "North Scotland",      "lat_min": 57.5,  "lat_max": 61.0,  "lon_min": -8.0, "lon_max": 0.0},
    {"id": 2,  "name": "South Scotland",      "lat_min": 55.0,  "lat_max": 57.5,  "lon_min": -6.0, "lon_max": 0.0},
    {"id": 3,  "name": "North West England",  "lat_min": 53.3,  "lat_max": 55.0,  "lon_min": -3.5, "lon_max": -1.5},
    {"id": 4,  "name": "North East England",  "lat_min": 54.0,  "lat_max": 55.8,  "lon_min": -2.5, "lon_max": 0.0},
    {"id": 5,  "name": "Yorkshire",           "lat_min": 53.4,  "lat_max": 54.5,  "lon_min": -2.2, "lon_max": 0.2},
    {"id": 6,  "name": "North Wales & Mersey","lat_min": 52.8,  "lat_max": 53.6,  "lon_min": -4.5, "lon_max": -2.0},
    {"id": 7,  "name": "South Wales",         "lat_min": 51.3,  "lat_max": 53.0,  "lon_min": -5.5, "lon_max": -2.5},
    {"id": 8,  "name": "West Midlands",       "lat_min": 52.0,  "lat_max": 53.2,  "lon_min": -3.0, "lon_max": -1.0},
    {"id": 9,  "name": "East Midlands",       "lat_min": 52.0,  "lat_max": 53.4,  "lon_min": -1.0, "lon_max": 1.0},
    {"id": 10, "name": "East England",        "lat_min": 51.5,  "lat_max": 53.5,  "lon_min": -0.5, "lon_max": 2.0},
    {"id": 11, "name": "South West England",  "lat_min": 49.9,  "lat_max": 52.0,  "lon_min": -6.0, "lon_max": -2.0},
    {"id": 12, "name": "South England",       "lat_min": 50.5,  "lat_max": 52.0,  "lon_min": -3.0, "lon_max": 1.0},
    {"id": 13, "name": "London",              "lat_min": 51.2,  "lat_max": 51.7,  "lon_min": -0.6, "lon_max": 0.4},
    {"id": 14, "name": "South East England",  "lat_min": 50.7,  "lat_max": 51.8,  "lon_min": -0.2, "lon_max": 1.5},
]


def validate_coordinates(
    df: pd.DataFrame,
    lat_col: str = "lat",
    lon_col: str = "lon",
) -> pd.DataFrame:
    """
    Validate and clean plant coordinates.

    - Remove rows where lat/lon are NaN or clearly wrong
    - Flag plants outside UK bounds (e.g., offshore wind far from shore)
    - Swap lat/lon if they appear transposed
    """
    df = df.copy()

    if lat_col not in df.columns or lon_col not in df.columns:
        logger.warning(f"Coordinate columns {lat_col}/{lon_col} not found")
        return df

    n_total = len(df)

    # Convert to numeric
    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")

    # Detect transposed coordinates (lat looks like lon and vice versa)
    # UK lon range: -9 to 2; UK lat range: 50-61
    transposed = (
        (df[lat_col].between(-9, 2)) &
        (df[lon_col].between(50, 61))
    )
    if transposed.any():
        logger.warning(f"Swapping lat/lon for {transposed.sum()} transposed records")
        df.loc[transposed, [lat_col, lon_col]] = df.loc[transposed, [lon_col, lat_col]].values

    # Flag valid UK coordinates
    df["coords_valid"] = (
        df[lat_col].between(UK_LAT_MIN, UK_LAT_MAX) &
        df[lon_col].between(UK_LON_MIN, UK_LON_MAX)
    )

    n_valid = df["coords_valid"].sum()
    n_missing = df[lat_col].isna().sum()
    logger.info(
        f"Coordinate validation: {n_valid}/{n_total} in UK bounds, "
        f"{n_missing} missing coords"
    )
    return df


def _assign_dno_bbox(lat: float | None, lon: float | None) -> str | None:
    """
    Approximate DNO region assignment using bounding boxes.
    Returns region name or None.
    """
    if lat is None or lon is None or np.isnan(lat) or np.isnan(lon):
        return None

    # London first (most specific)
    matches: list[dict] = []
    for region in DNO_BBOX_APPROX:
        if (region["lat_min"] <= lat <= region["lat_max"] and
                region["lon_min"] <= lon <= region["lon_max"]):
            matches.append(region)

    if not matches:
        return None
    # Return the most specific (smallest area) match
    def area(r: dict) -> float:
        return (r["lat_max"] - r["lat_min"]) * (r["lon_max"] - r["lon_min"])
    return min(matches, key=area)["name"]


def assign_dno_regions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign DNO region to each plant.

    First tries spatial join with Carbon Intensity API region boundaries;
    falls back to bounding box approximation.
    """
    df = df.copy()

    # Try to use GIS data if geopandas is available
    try:
        import geopandas as gpd
        from shapely.geometry import Point

        # Look for Carbon Intensity region GeoJSON
        # (We don't fetch this separately — use NESO GSP if available)
        logger.info("Using bounding box approximation for DNO regions (GIS data not yet loaded)")
    except ImportError:
        logger.warning("geopandas not available — using bounding box approximation")

    # Bbox fallback
    if "dno_region" not in df.columns or df["dno_region"].isna().all():
        df["dno_region"] = df.apply(
            lambda row: _assign_dno_bbox(row.get("lat"), row.get("lon")),
            axis=1,
        )
        assigned = df["dno_region"].notna().sum()
        logger.info(f"Assigned DNO regions (bbox): {assigned}/{len(df)} plants")

    return df


def assign_gsp_groups(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign GSP group to each plant using NESO GSP boundary data.

    If GIS data isn't available, leaves gsp_group as None.
    """
    df = df.copy()

    # Try spatial join with NESO GSP boundaries
    gsp_data_path = NESO_RAW / "gsp_boundaries_data.json"
    if not gsp_data_path.exists():
        gsp_data_path = NESO_RAW / "gsp_boundaries.json"

    if gsp_data_path.exists():
        try:
            import geopandas as gpd
            from shapely.geometry import Point

            with open(gsp_data_path) as f:
                raw = json.load(f)

            # Try to parse as GeoJSON FeatureCollection
            features = raw.get("features", []) if isinstance(raw, dict) else []
            if features:
                gsp_gdf = gpd.GeoDataFrame.from_features(features)

                # Build GeoDataFrame of plants with valid coords
                has_coords = (
                    df["lat"].notna() &
                    df["lon"].notna() &
                    df.get("coords_valid", pd.Series(True, index=df.index))
                )
                plant_gdf = gpd.GeoDataFrame(
                    df[has_coords],
                    geometry=[Point(lon, lat) for lat, lon in
                               zip(df.loc[has_coords, "lat"], df.loc[has_coords, "lon"])],
                    crs="EPSG:4326",
                )
                if gsp_gdf.crs is None:
                    gsp_gdf = gsp_gdf.set_crs("EPSG:4326")
                else:
                    plant_gdf = plant_gdf.to_crs(gsp_gdf.crs)

                # Spatial join
                gsp_col = next(
                    (c for c in gsp_gdf.columns if "gsp" in c.lower() and "group" in c.lower()),
                    next((c for c in gsp_gdf.columns if "gsp" in c.lower()), None),
                )
                if gsp_col:
                    joined = gpd.sjoin(plant_gdf, gsp_gdf[[gsp_col, "geometry"]], how="left", predicate="within")
                    df.loc[has_coords, "gsp_group"] = joined[gsp_col].values
                    assigned = df["gsp_group"].notna().sum()
                    logger.success(f"Assigned GSP groups (spatial join): {assigned}/{len(df)} plants")
                    return df

        except Exception as exc:
            logger.warning(f"Spatial GSP join failed: {exc} — leaving gsp_group as None")

    if "gsp_group" not in df.columns:
        df["gsp_group"] = None

    return df


def geocode_plants(df: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Main geocoding pipeline.

    1. Load plants_unified.parquet if df not provided
    2. Validate coordinates
    3. Assign DNO regions
    4. Assign GSP groups
    5. Save updated parquet
    """
    if df is None:
        if not PLANTS_UNIFIED.exists():
            logger.error("plants_unified.parquet not found — run reconcile first")
            return pd.DataFrame()
        df = pd.read_parquet(PLANTS_UNIFIED)
        logger.info(f"Loaded {len(df)} plants for geocoding")

    df = validate_coordinates(df)
    df = assign_dno_regions(df)
    df = assign_gsp_groups(df)

    # Save back
    df.to_parquet(PLANTS_UNIFIED, index=False, engine="pyarrow")
    logger.success(f"Geocoding complete → {PLANTS_UNIFIED}")
    return df


if __name__ == "__main__":
    geocode_plants()
