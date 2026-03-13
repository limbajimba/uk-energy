"""
map.py — Interactive Folium map of the UK electricity system.

Performance-optimised: uses GeoJSON + canvas rendering instead of individual
DOM markers. Keeps file size under 5 MB for smooth browser experience.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd
from loguru import logger

from uk_energy.config import INTERCONNECTORS_REF, MAP_OUTPUT, OUTPUT_DIR, PLANTS_UNIFIED

FUEL_COLOURS: dict[str, str] = {
    "wind_onshore":       "#2196F3",
    "wind_offshore":      "#0D47A1",
    "solar_pv":           "#FFC107",
    "gas_ccgt":           "#FF5722",
    "gas_ocgt":           "#FF7043",
    "gas_chp":            "#FFAB40",
    "nuclear":            "#9C27B0",
    "coal":               "#212121",
    "oil":                "#795548",
    "biomass":            "#4CAF50",
    "hydro_run_of_river": "#00BCD4",
    "hydro_pumped_storage": "#006064",
    "battery_storage":    "#E91E63",
    "hydrogen":           "#00E676",
    "wave_tidal":         "#80DEEA",
    "geothermal":         "#FF6F00",
    "other":              "#9E9E9E",
    "unknown":            "#BDBDBD",
}

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
}


def _fuel_colour(fuel: str) -> str:
    return FUEL_COLOURS.get(fuel, "#BDBDBD")


def _fuel_label(fuel: str) -> str:
    return FUEL_LABELS.get(fuel, fuel.replace("_", " ").title())


def _capacity_radius(cap: float | None) -> float:
    if not cap or cap <= 0:
        return 3
    return max(3, min(18, 3 + math.log10(max(1, cap)) * 3))


def _plants_to_geojson(df: pd.DataFrame) -> dict:
    """Convert a plant DataFrame to a GeoJSON FeatureCollection."""
    features = []
    for _, row in df.iterrows():
        lat = row.get("lat")
        lon = row.get("lon")
        if pd.isna(lat) or pd.isna(lon):
            continue
        cap = row.get("capacity_mw")
        cap_val = float(cap) if pd.notna(cap) and cap > 0 else 0
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
            "properties": {
                "name": str(row.get("name", "Unknown")),
                "fuel": str(row.get("fuel_type", "unknown")),
                "cap": cap_val,
                "status": str(row.get("status", "unknown")),
                "owner": str(row.get("owner", "")) or "",
            },
        })
    return {"type": "FeatureCollection", "features": features}


def create_map(plants_df: pd.DataFrame | None = None) -> Path:
    """Create a performance-optimised interactive map of the UK electricity system."""
    import folium

    if plants_df is None:
        if not PLANTS_UNIFIED.exists():
            logger.error("plants_unified.parquet not found")
            return MAP_OUTPUT
        plants_df = pd.read_parquet(PLANTS_UNIFIED)
        logger.info(f"Loaded {len(plants_df)} plants for map")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Filter valid UK coordinates
    valid = plants_df[
        plants_df["lat"].notna()
        & plants_df["lon"].notna()
        & plants_df["lat"].between(49.5, 61.5)
        & plants_df["lon"].between(-9, 2.5)
    ].copy()

    # ─── Only map operational + construction (keep it focused) ───
    operational = valid[valid["status"] == "operational"]
    construction = valid[valid["status"] == "construction"]

    logger.info(
        f"Mapping {len(operational)} operational + {len(construction)} construction plants"
    )

    m = folium.Map(
        location=[54.5, -2.5],
        zoom_start=6,
        tiles="CartoDB positron",
        prefer_canvas=True,
    )

    folium.TileLayer("OpenStreetMap", name="OpenStreetMap").add_to(m)

    # ─── OPERATIONAL (individual circle markers — only ~2,700) ───
    op_group = folium.FeatureGroup(
        name=f"⚡ Operational ({len(operational):,} plants, "
        f"{operational['capacity_mw'].sum() / 1000:,.1f} GW)",
        show=True,
    )

    for _, row in operational.iterrows():
        cap = row.get("capacity_mw")
        cap_val = float(cap) if pd.notna(cap) and cap > 0 else 0
        cap_str = f"{cap_val:,.0f} MW" if cap_val > 0 else "N/A"
        name = str(row.get("name", "Unknown"))
        fuel = str(row.get("fuel_type", "unknown"))
        colour = _fuel_colour(fuel)

        popup = (
            f"<b>{name}</b><br>"
            f"Fuel: {_fuel_label(fuel)}<br>"
            f"Capacity: {cap_str}<br>"
            f"Owner: {row.get('owner', '') or 'N/A'}<br>"
            f"Status: Operational"
        )

        folium.CircleMarker(
            location=[float(row["lat"]), float(row["lon"])],
            radius=_capacity_radius(cap_val),
            color=colour,
            fill=True,
            fill_color=colour,
            fill_opacity=0.7,
            weight=1,
            popup=folium.Popup(popup, max_width=250),
            tooltip=f"{name} ({cap_str})",
        ).add_to(op_group)

    op_group.add_to(m)

    # ─── CONSTRUCTION (smaller markers, togglable) ───
    if not construction.empty:
        con_group = folium.FeatureGroup(
            name=f"🏗️ Construction ({len(construction)}, "
            f"{construction['capacity_mw'].sum() / 1000:,.1f} GW)",
            show=False,
        )
        for _, row in construction.iterrows():
            cap = row.get("capacity_mw")
            cap_val = float(cap) if pd.notna(cap) and cap > 0 else 0
            name = str(row.get("name", "Unknown"))
            fuel = str(row.get("fuel_type", "unknown"))

            folium.CircleMarker(
                location=[float(row["lat"]), float(row["lon"])],
                radius=max(2, _capacity_radius(cap_val) * 0.6),
                color=_fuel_colour(fuel),
                fill=True,
                fill_color=_fuel_colour(fuel),
                fill_opacity=0.4,
                weight=0.5,
                tooltip=f"{name} ({cap_val:,.0f} MW) [Construction]",
            ).add_to(con_group)
        con_group.add_to(m)

    # ─── INTERCONNECTORS ───
    if INTERCONNECTORS_REF.exists():
        try:
            ic_data = json.loads(INTERCONNECTORS_REF.read_text())
            ic_group = folium.FeatureGroup(name="🔗 Interconnectors", show=True)

            for ic in ic_data.get("interconnectors", []):
                gb = ic.get("gb_terminal", {})
                foreign = ic.get("foreign_terminal", {})
                if not all(k in gb for k in ("lat", "lon")):
                    continue
                if not all(k in foreign for k in ("lat", "lon")):
                    continue

                name = ic.get("name", "")
                cap = ic.get("capacity_mw", "?")

                folium.PolyLine(
                    locations=[
                        [gb["lat"], gb["lon"]],
                        [foreign["lat"], foreign["lon"]],
                    ],
                    color="#455A64",
                    weight=3,
                    dash_array="10 6",
                    opacity=0.8,
                    tooltip=f"{name}: {cap} MW",
                ).add_to(ic_group)

                folium.CircleMarker(
                    location=[gb["lat"], gb["lon"]],
                    radius=5,
                    color="#455A64",
                    fill=True,
                    fill_color="#78909C",
                    fill_opacity=0.9,
                    weight=2,
                    tooltip=f"{name} (GB terminal) — {cap} MW",
                ).add_to(ic_group)

            ic_group.add_to(m)
        except Exception as exc:
            logger.warning(f"Interconnectors: {exc}")

    # ─── LEGEND ───
    legend_items = ""
    for fuel, colour in FUEL_COLOURS.items():
        if fuel in ("other", "unknown"):
            continue
        # Only show fuels that exist in operational data
        if fuel in operational["fuel_type"].values:
            cap_gw = operational[operational["fuel_type"] == fuel]["capacity_mw"].sum() / 1000
            legend_items += (
                f'<span style="color:{colour}; font-size:14px;">●</span> '
                f'{_fuel_label(fuel)} ({cap_gw:.1f} GW)<br>'
            )

    legend_html = f"""
    <div style="position: fixed; bottom: 30px; left: 10px; z-index: 1000;
         background: white; padding: 10px 14px; border-radius: 6px;
         box-shadow: 0 2px 6px rgba(0,0,0,0.15); font-size: 11px;
         max-height: 350px; overflow-y: auto; line-height: 1.6;">
    <b>🇬🇧 UK Electricity System</b><br>
    <span style="color:#888; font-size:10px;">
      {len(operational):,} operational plants · {operational['capacity_mw'].sum()/1000:,.1f} GW
    </span><br><br>
    {legend_items}
    <br><span style="color:#888; font-size:10px;">Marker size ∝ log(capacity)</span>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    folium.LayerControl(collapsed=True).add_to(m)

    m.save(str(MAP_OUTPUT))
    size_mb = MAP_OUTPUT.stat().st_size / 1024 / 1024
    logger.success(f"Map saved → {MAP_OUTPUT} ({size_mb:.1f} MB)")
    return MAP_OUTPUT


if __name__ == "__main__":
    create_map()
