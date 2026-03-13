"""
map.py — Interactive Folium map of the UK electricity system.

Features:
  - Generation plants colour-coded by fuel type
  - Status-based layer groups (operational, construction, consented, planning)
  - Marker clustering for performance with 13k+ plants
  - Popup with plant details (name, capacity, fuel, owner, status)
  - Transmission substations (optional layer)
  - Interconnector lines with capacity labels
  - Saves as output/uk_energy_map.html
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

from uk_energy.config import INTERCONNECTORS_REF, MAP_OUTPUT, OSM_RAW, OUTPUT_DIR, PLANTS_UNIFIED

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
    "hydro_pumped_storage": "#006064",  # dark cyan
    "battery_storage":    "#E91E63",  # pink
    "hydrogen":           "#00E676",  # bright green
    "interconnector":     "#607D8B",  # blue-grey
    "wave_tidal":         "#80DEEA",  # light cyan
    "geothermal":         "#FF6F00",  # dark amber
    "other":              "#9E9E9E",  # grey
    "unknown":            "#BDBDBD",  # light grey
}

# Human-readable fuel labels
FUEL_LABELS: dict[str, str] = {
    "wind_onshore": "Wind (Onshore)",
    "wind_offshore": "Wind (Offshore)",
    "solar_pv": "Solar PV",
    "gas_ccgt": "Gas CCGT",
    "gas_ocgt": "Gas OCGT",
    "gas_chp": "Gas CHP",
    "nuclear": "Nuclear",
    "coal": "Coal",
    "oil": "Oil",
    "biomass": "Biomass",
    "hydro_run_of_river": "Hydro",
    "hydro_pumped_storage": "Pumped Storage",
    "battery_storage": "Battery Storage",
    "hydrogen": "Hydrogen",
    "wave_tidal": "Wave & Tidal",
    "geothermal": "Geothermal",
    "interconnector": "Interconnector",
    "other": "Other",
    "unknown": "Unknown",
}


def _fuel_colour(fuel: str) -> str:
    return FUEL_COLOURS.get(fuel, FUEL_COLOURS["unknown"])


def _fuel_label(fuel: str) -> str:
    return FUEL_LABELS.get(fuel, fuel.replace("_", " ").title())


def _capacity_radius(capacity_mw: float | None) -> float:
    if capacity_mw is None or capacity_mw <= 0:
        return 3
    return max(3, min(20, 3 + math.log10(max(1, capacity_mw)) * 3.5))


def create_map(plants_df: pd.DataFrame | None = None) -> Path:
    """
    Create an interactive Folium map of the UK electricity system.

    Optimised for performance:
    - Uses MarkerCluster for non-operational plants
    - Operational plants shown individually (most important)
    - Substations as a togglable layer (off by default)
    """
    import folium
    from folium.plugins import MarkerCluster

    if plants_df is None:
        if not PLANTS_UNIFIED.exists():
            logger.error("plants_unified.parquet not found")
            return MAP_OUTPUT
        plants_df = pd.read_parquet(PLANTS_UNIFIED)
        logger.info(f"Loaded {len(plants_df)} plants for map")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Creating interactive Folium map...")

    # Centre on UK
    m = folium.Map(
        location=[54.5, -2.5],
        zoom_start=6,
        tiles="CartoDB positron",
        prefer_canvas=True,  # Much faster rendering for many markers
    )

    # Add alternative tile layers
    folium.TileLayer("OpenStreetMap", name="OpenStreetMap").add_to(m)
    folium.TileLayer("CartoDB dark_matter", name="Dark Mode").add_to(m)

    # Filter to plants with valid UK coordinates
    valid = plants_df[
        plants_df["lat"].notna() &
        plants_df["lon"].notna() &
        plants_df["lat"].between(49.5, 61.5) &
        plants_df["lon"].between(-9, 2.5)
    ].copy()

    logger.info(f"Mapping {len(valid)} plants with valid coordinates")

    # ─── OPERATIONAL PLANTS (shown by default, individual markers) ───
    operational = valid[valid["status"] == "operational"]
    op_group = folium.FeatureGroup(name=f"⚡ Operational ({len(operational):,})", show=True)

    # Group operational plants by fuel type for cleaner legends
    for fuel_type in sorted(operational["fuel_type"].unique()):
        fuel_plants = operational[operational["fuel_type"] == fuel_type]
        colour = _fuel_colour(fuel_type)
        label = _fuel_label(fuel_type)

        for _, row in fuel_plants.iterrows():
            cap = row.get("capacity_mw")
            cap_str = f"{cap:,.0f} MW" if pd.notna(cap) and cap > 0 else "N/A"
            name = str(row.get("name", "Unknown"))
            owner = str(row.get("owner", "")) or "N/A"

            popup_html = (
                f"<b>{name}</b><br>"
                f"<b>Fuel:</b> {label}<br>"
                f"<b>Capacity:</b> {cap_str}<br>"
                f"<b>Owner:</b> {owner}<br>"
                f"<b>Status:</b> Operational"
            )

            folium.CircleMarker(
                location=[row["lat"], row["lon"]],
                radius=_capacity_radius(cap),
                color=colour,
                fill=True,
                fill_color=colour,
                fill_opacity=0.7,
                weight=1,
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=f"{name} ({cap_str})",
            ).add_to(op_group)

    op_group.add_to(m)
    logger.info(f"Added {len(operational)} operational plants")

    # ─── NON-OPERATIONAL PLANTS (clustered, off by default) ───
    for status_name, show_default in [
        ("construction", False),
        ("consented", False),
        ("planning", False),
    ]:
        status_plants = valid[valid["status"] == status_name]
        if status_plants.empty:
            continue

        status_label = status_name.title()
        cap_total = status_plants["capacity_mw"].sum()
        group = folium.FeatureGroup(
            name=f"{'🏗️' if status_name == 'construction' else '📋' if status_name == 'consented' else '📝'} "
                 f"{status_label} ({len(status_plants):,}, {cap_total:,.0f} MW)",
            show=show_default,
        )

        cluster = MarkerCluster(
            options={"maxClusterRadius": 50, "disableClusteringAtZoom": 10}
        ).add_to(group)

        for _, row in status_plants.iterrows():
            cap = row.get("capacity_mw")
            cap_str = f"{cap:,.0f} MW" if pd.notna(cap) and cap > 0 else "N/A"
            name = str(row.get("name", "Unknown"))
            fuel = _fuel_label(str(row.get("fuel_type", "unknown")))
            colour = _fuel_colour(str(row.get("fuel_type", "unknown")))

            popup_html = (
                f"<b>{name}</b><br>"
                f"<b>Fuel:</b> {fuel}<br>"
                f"<b>Capacity:</b> {cap_str}<br>"
                f"<b>Status:</b> {status_label}"
            )

            folium.CircleMarker(
                location=[row["lat"], row["lon"]],
                radius=max(3, _capacity_radius(cap) * 0.7),
                color=colour,
                fill=True,
                fill_color=colour,
                fill_opacity=0.4,
                weight=1,
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=f"{name} ({cap_str}) [{status_label}]",
            ).add_to(cluster)

        group.add_to(m)
        logger.info(f"Added {len(status_plants)} {status_name} plants (clustered)")

    # ─── INTERCONNECTORS ───
    if INTERCONNECTORS_REF.exists():
        try:
            ic_data = json.loads(INTERCONNECTORS_REF.read_text())
            ic_group = folium.FeatureGroup(name="🔗 Interconnectors (10.3 GW)", show=True)

            for ic in ic_data.get("interconnectors", []):
                gb = ic.get("gb_terminal", {})
                foreign = ic.get("foreign_terminal", {})
                if not all(k in gb for k in ("lat", "lon")) or not all(k in foreign for k in ("lat", "lon")):
                    continue

                name = ic.get("name", "Unknown")
                cap = ic.get("capacity_mw", "?")
                route = ic.get("route", "")
                country = ic.get("foreign_country", "")

                # Dashed line for interconnector
                folium.PolyLine(
                    locations=[
                        [gb["lat"], gb["lon"]],
                        [foreign["lat"], foreign["lon"]],
                    ],
                    color="#455A64",
                    weight=3,
                    dash_array="10 6",
                    opacity=0.8,
                    tooltip=f"{name}: {cap} MW ({route})",
                ).add_to(ic_group)

                # GB terminal marker
                folium.CircleMarker(
                    location=[gb["lat"], gb["lon"]],
                    radius=6,
                    color="#455A64",
                    fill=True,
                    fill_color="#78909C",
                    fill_opacity=0.9,
                    weight=2,
                    tooltip=f"{name} (GB) — {cap} MW",
                ).add_to(ic_group)

            ic_group.add_to(m)
            logger.info(f"Added interconnector lines")
        except Exception as exc:
            logger.warning(f"Could not add interconnectors: {exc}")

    # ─── TRANSMISSION SUBSTATIONS (off by default) ───
    sub_group = folium.FeatureGroup(name="⬡ Transmission Substations (≥132kV)", show=False)
    osm_sub_path = OSM_RAW / "substations.json"
    if osm_sub_path.exists():
        try:
            osm_data = json.loads(osm_sub_path.read_text())
            sub_count = 0
            for el in osm_data.get("elements", []):
                if el.get("type") != "node" or "lat" not in el or "lon" not in el:
                    continue
                tags = el.get("tags", {})
                voltage_str = tags.get("voltage", "")
                voltage_kv = 0.0
                try:
                    voltages = [float(v.strip()) for v in voltage_str.split(";") if v.strip()]
                    if voltages:
                        voltage_kv = max(voltages) / 1000.0
                except (ValueError, AttributeError):
                    pass
                has_name = bool(tags.get("name"))
                if voltage_kv < 132 and not has_name:
                    continue

                name = tags.get("name", tags.get("ref", f"Substation {el['id']}"))
                voltage_display = f"{voltage_kv:.0f}kV" if voltage_kv > 0 else "?"
                folium.CircleMarker(
                    location=[el["lat"], el["lon"]],
                    radius=2,
                    color="#455A64",
                    fill=True,
                    fill_color="#78909C",
                    fill_opacity=0.5,
                    weight=0.5,
                    tooltip=f"{name} ({voltage_display})",
                ).add_to(sub_group)
                sub_count += 1
            logger.info(f"Added {sub_count} transmission substations to map")
        except Exception as exc:
            logger.warning(f"Could not add substations: {exc}")
    sub_group.add_to(m)

    # ─── LEGEND ───
    legend_html = """
    <div style="position: fixed; bottom: 30px; left: 30px; z-index: 1000;
         background: white; padding: 12px 16px; border-radius: 8px;
         box-shadow: 0 2px 8px rgba(0,0,0,0.15); font-size: 12px;
         max-height: 400px; overflow-y: auto;">
    <b>🇬🇧 UK Electricity System</b><br>
    <b style="font-size:11px; color:#666;">Fuel Types</b><br>
    """
    for fuel, colour in FUEL_COLOURS.items():
        if fuel in ("interconnector", "other", "unknown"):
            continue
        label = _fuel_label(fuel)
        legend_html += f'<span style="color:{colour}; font-size:16px;">●</span> {label}<br>'
    legend_html += """
    <br><b style="font-size:11px; color:#666;">Marker Size = Capacity (MW)</b>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    # ─── LAYER CONTROL ───
    folium.LayerControl(collapsed=False).add_to(m)

    # ─── SAVE ───
    m.save(str(MAP_OUTPUT))
    op_cap = operational["capacity_mw"].sum()
    logger.success(
        f"Interactive map saved → {MAP_OUTPUT} "
        f"({len(operational)} operational, {op_cap:,.0f} MW)"
    )
    return MAP_OUTPUT


if __name__ == "__main__":
    create_map()
