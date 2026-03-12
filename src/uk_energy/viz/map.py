"""
map.py — Interactive Folium map of the UK electricity system.

Features:
  - All generation plants, colour-coded by fuel type
  - Layer controls to toggle fuel types on/off
  - Popup with plant details (name, capacity, fuel, owner, status)
  - Transmission line overlay from OSM
  - Interconnector lines with tooltips
  - GSP markers
  - Saves as output/uk_energy_map.html
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

from uk_energy.config import INTERCONNECTORS_REF, MAP_OUTPUT, OSM_RAW, PLANTS_UNIFIED

# Fuel type → hex colour
FUEL_COLOURS: dict[str, str] = {
    "wind_onshore":       "#2196F3",  # blue
    "wind_offshore":      "#0D47A1",  # dark blue
    "solar_pv":           "#FFC107",  # amber
    "gas_ccgt":           "#FF5722",  # deep orange
    "gas_ocgt":           "#FF7043",  # orange
    "gas_chp":            "#FFAB40",  # light orange
    "nuclear":            "#9C27B0",  # purple
    "coal":               "#212121",  # near-black
    "oil":                "#795548",  # brown
    "biomass":            "#4CAF50",  # green
    "hydro_run_of_river": "#00BCD4",  # cyan
    "hydro_pumped_storage":"#006064",  # dark cyan
    "battery_storage":    "#E91E63",  # pink
    "interconnector":     "#607D8B",  # blue-grey
    "wave_tidal":         "#80DEEA",  # light cyan
    "other":              "#9E9E9E",  # grey
    "unknown":            "#BDBDBD",  # light grey
}


def _fuel_colour(fuel: str) -> str:
    """Get hex colour for a fuel type."""
    return FUEL_COLOURS.get(fuel, FUEL_COLOURS["unknown"])


def _capacity_radius(capacity_mw: float | None) -> float:
    """Scale marker radius by capacity (MW)."""
    if capacity_mw is None or capacity_mw <= 0:
        return 4
    import math
    return max(4, min(25, 4 + math.log10(max(1, capacity_mw)) * 4))


def create_map(plants_df: pd.DataFrame | None = None) -> Path:
    """
    Create the interactive Folium map.

    Args:
        plants_df: Pre-loaded plants DataFrame. If None, loads from parquet.

    Returns:
        Path to the saved HTML file.
    """
    try:
        import folium
    except ImportError:
        logger.error("folium not installed — cannot create map")
        return MAP_OUTPUT

    # Load data
    if plants_df is None:
        if PLANTS_UNIFIED.exists():
            plants_df = pd.read_parquet(PLANTS_UNIFIED)
            logger.info(f"Loaded {len(plants_df)} plants for map")
        else:
            logger.warning("No plants data — creating empty map")
            plants_df = pd.DataFrame()

    logger.info("Creating interactive Folium map...")

    # Base map centred on UK
    m = folium.Map(
        location=[54.5, -2.5],
        zoom_start=6,
        tiles="CartoDB positron",
        control_scale=True,
    )

    # Add multiple tile layers
    folium.TileLayer("CartoDB dark_matter", name="Dark Mode", show=False).add_to(m)
    folium.TileLayer("OpenStreetMap", name="OpenStreetMap", show=False).add_to(m)

    # ─── Generation Plants ────────────────────────────────────────────────────
    # One FeatureGroup per fuel type for layer control
    fuel_groups: dict[str, folium.FeatureGroup] = {}
    all_fuels = sorted(set(
        str(row.get("fuel_type", "unknown"))
        for _, row in plants_df.iterrows()
        if pd.notna(row.get("fuel_type"))
    )) if not plants_df.empty else []

    for fuel in all_fuels:
        label = fuel.replace("_", " ").title()
        group = folium.FeatureGroup(name=f"⚡ {label}", show=True)
        fuel_groups[fuel] = group

    # Other group for unknowns
    fuel_groups["unknown"] = folium.FeatureGroup(name="❓ Unknown", show=False)

    plant_count = 0
    if not plants_df.empty:
        for _, row in plants_df.iterrows():
            lat = row.get("lat")
            lon = row.get("lon")
            if lat is None or lon is None:
                continue
            try:
                lat_f = float(lat)
                lon_f = float(lon)
            except (TypeError, ValueError):
                continue
            if not (-90 <= lat_f <= 90 and -180 <= lon_f <= 180):
                continue

            fuel = str(row.get("fuel_type", "unknown"))
            name = str(row.get("name", "Unknown Plant"))
            capacity = row.get("capacity_mw")
            status = str(row.get("status", "unknown"))
            owner = str(row.get("owner") or "")
            dno = str(row.get("dno_region") or "")

            colour = _fuel_colour(fuel)
            radius = _capacity_radius(float(capacity) if pd.notna(capacity) else None)

            popup_html = f"""
            <div style="font-family: sans-serif; min-width: 200px;">
                <h4 style="margin:0 0 8px 0;">{name}</h4>
                <table style="font-size:12px;">
                    <tr><td><b>Fuel:</b></td><td>{fuel.replace("_", " ").title()}</td></tr>
                    <tr><td><b>Capacity:</b></td><td>{f"{capacity:.0f} MW" if pd.notna(capacity) else "Unknown"}</td></tr>
                    <tr><td><b>Status:</b></td><td>{status.title()}</td></tr>
                    <tr><td><b>Owner:</b></td><td>{owner or "Unknown"}</td></tr>
                    <tr><td><b>Region:</b></td><td>{dno or "Unknown"}</td></tr>
                </table>
            </div>
            """

            marker = folium.CircleMarker(
                location=[lat_f, lon_f],
                radius=radius,
                color=colour,
                fill=True,
                fill_color=colour,
                fill_opacity=0.75,
                weight=1,
                popup=folium.Popup(popup_html, max_width=280),
                tooltip=f"{name} ({capacity:.0f} MW)" if pd.notna(capacity) else name,
            )

            group = fuel_groups.get(fuel, fuel_groups["unknown"])
            marker.add_to(group)
            plant_count += 1

    for group in fuel_groups.values():
        group.add_to(m)

    # ─── Interconnectors ─────────────────────────────────────────────────────
    ic_group = folium.FeatureGroup(name="🔗 Interconnectors", show=True)

    if INTERCONNECTORS_REF.exists():
        try:
            ic_data = json.loads(INTERCONNECTORS_REF.read_text())
            for ic in ic_data.get("interconnectors", []):
                gb = ic.get("gb_terminal", {})
                foreign = ic.get("foreign_terminal", {})

                if all(k in gb for k in ("lat", "lon")) and all(k in foreign for k in ("lat", "lon")):
                    # Draw the interconnector cable
                    folium.PolyLine(
                        locations=[
                            [gb["lat"], gb["lon"]],
                            [foreign["lat"], foreign["lon"]],
                        ],
                        color="#607D8B",
                        weight=3,
                        opacity=0.8,
                        tooltip=(
                            f"{ic['name']}: {ic['capacity_mw']} MW "
                            f"({ic['countries'][0]} ↔ {ic['countries'][1]})"
                        ),
                        popup=folium.Popup(
                            f"<b>{ic['name']}</b><br>"
                            f"Capacity: {ic['capacity_mw']:,} MW<br>"
                            f"Route: {ic['countries'][0]} ↔ {ic['countries'][1]}<br>"
                            f"Type: {ic.get('cable_type', 'HVDC')}<br>"
                            f"Commissioned: {ic.get('commissioned_year', 'N/A')}<br>"
                            f"Length: {ic.get('length_km', '?')} km",
                            max_width=250,
                        ),
                        dash_array="10 5",
                    ).add_to(ic_group)

                    # GB terminal marker
                    folium.CircleMarker(
                        location=[gb["lat"], gb["lon"]],
                        radius=8,
                        color="#37474F",
                        fill=True,
                        fill_color="#607D8B",
                        fill_opacity=0.9,
                        tooltip=f"{ic['id']} — GB terminal: {gb.get('name', '')}",
                    ).add_to(ic_group)

        except Exception as exc:
            logger.warning(f"Could not add interconnectors to map: {exc}")

    ic_group.add_to(m)

    # ─── OSM Substations ────────────────────────────────────────────────────
    sub_group = folium.FeatureGroup(name="⬡ Substations (OSM)", show=False)
    osm_sub_path = OSM_RAW / "substations.json"
    if osm_sub_path.exists():
        try:
            osm_data = json.loads(osm_sub_path.read_text())
            sub_count = 0
            for el in osm_data.get("elements", []):
                if el.get("type") == "node" and "lat" in el and "lon" in el:
                    tags = el.get("tags", {})
                    name = tags.get("name", tags.get("ref", f"Substation {el['id']}"))
                    voltage = tags.get("voltage", "?")
                    folium.CircleMarker(
                        location=[el["lat"], el["lon"]],
                        radius=3,
                        color="#455A64",
                        fill=True,
                        fill_color="#78909C",
                        fill_opacity=0.6,
                        weight=1,
                        tooltip=f"{name} ({voltage}V)",
                    ).add_to(sub_group)
                    sub_count += 1
            logger.info(f"Added {sub_count} substations to map")
        except Exception as exc:
            logger.warning(f"Could not add OSM substations to map: {exc}")
    sub_group.add_to(m)

    # ─── Layer Control ────────────────────────────────────────────────────────
    folium.LayerControl(collapsed=False).add_to(m)

    # ─── Title ───────────────────────────────────────────────────────────────
    title_html = f"""
    <div style="position: fixed; top: 10px; left: 60px; z-index: 1000;
                background: white; padding: 10px 15px; border-radius: 8px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.3); font-family: sans-serif;">
        <h3 style="margin: 0 0 4px 0;">🇬🇧 UK Electricity System</h3>
        <p style="margin: 0; font-size: 12px; color: #666;">
            {plant_count:,} generation assets | {len(ic_data.get("interconnectors", [])) if INTERCONNECTORS_REF.exists() else 0} interconnectors
        </p>
    </div>
    """
    m.get_root().html.add_child(folium.Element(title_html))

    # Save
    MAP_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    m.save(str(MAP_OUTPUT))
    logger.success(f"Interactive map saved → {MAP_OUTPUT} ({plant_count:,} plants)")
    return MAP_OUTPUT


if __name__ == "__main__":
    create_map()
