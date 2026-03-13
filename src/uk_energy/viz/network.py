"""
network.py — Plotly network diagram of the UK grid topology.

Two views:
  1. Operational generation assets: scatter geo plot, size=capacity, colour=fuel
  2. Regional summary: 14 DNO regions with total capacity, fuel split, interconnectors
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

from uk_energy.config import INTERCONNECTORS_REF, NETWORK_OUTPUT, OUTPUT_DIR, PLANTS_UNIFIED


FUEL_COLOURS: dict[str, str] = {
    "wind_onshore":       "#2196F3",
    "wind_offshore":      "#0D47A1",
    "solar_pv":           "#FFC107",
    "gas_ccgt":           "#FF5722",
    "gas_ocgt":           "#FF7043",
    "gas_chp":            "#FFAB40",
    "nuclear":            "#9C27B0",
    "coal":               "#424242",
    "oil":                "#795548",
    "biomass":            "#4CAF50",
    "hydro_run_of_river": "#00BCD4",
    "hydro_pumped_storage": "#006064",
    "battery_storage":    "#E91E63",
    "hydrogen":           "#00E676",
    "interconnector":     "#607D8B",
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

DNO_REGION_CENTROIDS: dict[str, tuple[float, float]] = {
    "North Scotland":       (57.5, -4.0),
    "South Scotland":       (55.8, -3.5),
    "North West England":   (53.7, -2.5),
    "North East England":   (54.8, -1.5),
    "Yorkshire":            (53.8, -1.5),
    "North Wales & Mersey": (53.1, -3.0),
    "South Wales":          (51.7, -3.5),
    "West Midlands":        (52.5, -2.0),
    "East Midlands":        (52.7, -1.0),
    "East England":         (52.5, 0.5),
    "South West England":   (51.0, -3.5),
    "South England":        (51.2, -1.5),
    "London":               (51.5, -0.1),
    "South East England":   (51.2, 0.5),
}


def _create_operational_scatter(df: pd.DataFrame) -> Any:
    """
    Plotly scatter geo of operational generation assets.
    """
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.error("plotly not installed")
        return None

    # Filter to operational with valid coords
    op = df[
        (df["status"] == "operational") &
        df["lat"].notna() & df["lon"].notna() &
        df["lat"].between(49.5, 61.5) & df["lon"].between(-9, 2.5)
    ].copy()

    if op.empty:
        logger.warning("No operational plants for scatter")
        return None

    op["marker_size"] = op["capacity_mw"].apply(
        lambda x: max(3, min(25, 3 + math.log10(max(1, x or 1)) * 4))
    )

    traces = []
    # Sort fuel types by total capacity (biggest first in legend)
    fuel_caps = op.groupby("fuel_type")["capacity_mw"].sum().sort_values(ascending=False)

    for fuel in fuel_caps.index:
        subset = op[op["fuel_type"] == fuel]
        colour = FUEL_COLOURS.get(fuel, FUEL_COLOURS["unknown"])
        label = FUEL_LABELS.get(fuel, fuel.replace("_", " ").title())
        total_cap = subset["capacity_mw"].sum()

        traces.append(go.Scattergeo(
            lon=subset["lon"],
            lat=subset["lat"],
            mode="markers",
            name=f"{label} ({total_cap:,.0f} MW)",
            marker=dict(
                size=subset["marker_size"],
                color=colour,
                opacity=0.75,
                line=dict(width=0.5, color="white"),
            ),
            text=subset.apply(
                lambda r: (
                    f"<b>{r.get('name', '?')}</b><br>"
                    f"Fuel: {label}<br>"
                    f"Capacity: {r.get('capacity_mw', 0):,.0f} MW<br>"
                    f"Owner: {r.get('owner', 'N/A')}"
                ),
                axis=1,
            ),
            hovertemplate="%{text}<extra></extra>",
        ))

    fig = go.Figure(data=traces)

    total_op = op["capacity_mw"].sum()
    fig.update_layout(
        title=dict(
            text=f"UK Operational Generation Assets — {total_op:,.0f} MW",
            x=0.5, xanchor="center", font=dict(size=18),
        ),
        geo=dict(
            scope="europe",
            projection_type="natural earth",
            showland=True, landcolor="#F5F5F5",
            showocean=True, oceancolor="#E3F2FD",
            showcoastlines=True, coastlinecolor="#BDBDBD",
            showcountries=True, countrycolor="#E0E0E0",
            lataxis=dict(range=[49, 61]),
            lonaxis=dict(range=[-9, 3]),
        ),
        legend=dict(
            title="Fuel Type",
            itemsizing="constant",
            font=dict(size=11),
        ),
        height=800,
        margin=dict(l=0, r=250, t=50, b=0),
        paper_bgcolor="white",
    )

    return fig


def _create_regional_network(df: pd.DataFrame) -> Any:
    """
    Regional network: 14 DNO regions as nodes, interconnectors as edges.
    """
    try:
        import plotly.graph_objects as go
    except ImportError:
        return None

    # Operational capacity by region
    op = df[(df["status"] == "operational") & df["dno_region"].notna()]

    regional_cap: dict[str, float] = {}
    regional_fuel: dict[str, dict[str, float]] = {}
    regional_count: dict[str, int] = {}

    for _, row in op.iterrows():
        region = str(row["dno_region"])
        cap = float(row.get("capacity_mw") or 0)
        fuel = str(row.get("fuel_type") or "unknown")
        regional_cap[region] = regional_cap.get(region, 0) + cap
        regional_count[region] = regional_count.get(region, 0) + 1
        if region not in regional_fuel:
            regional_fuel[region] = {}
        regional_fuel[region][fuel] = regional_fuel[region].get(fuel, 0) + cap

    # Build region nodes
    node_lats, node_lons, node_text, node_sizes, node_colours = [], [], [], [], []

    for region, (lat, lon) in DNO_REGION_CENTROIDS.items():
        cap = regional_cap.get(region, 0)
        count = regional_count.get(region, 0)
        fuels = regional_fuel.get(region, {})
        top_fuel = max(fuels, key=fuels.get) if fuels else "unknown"
        colour = FUEL_COLOURS.get(top_fuel, "#9E9E9E")

        # Build fuel breakdown for hover
        fuel_lines = []
        for f, c in sorted(fuels.items(), key=lambda x: -x[1])[:5]:
            fl = FUEL_LABELS.get(f, f.replace("_", " ").title())
            fuel_lines.append(f"{fl}: {c:,.0f} MW")

        hover = (
            f"<b>{region}</b><br>"
            f"Total: {cap:,.0f} MW ({count} plants)<br>"
            f"{'<br>'.join(fuel_lines)}"
        )

        node_lats.append(lat)
        node_lons.append(lon)
        node_text.append(hover)
        node_sizes.append(max(15, min(60, cap / 300))  if cap > 0 else 10)
        node_colours.append(colour)

    fig = go.Figure()

    # Interconnector edges
    if INTERCONNECTORS_REF.exists():
        try:
            ic_data = json.loads(INTERCONNECTORS_REF.read_text())
            for ic in ic_data.get("interconnectors", []):
                gb = ic.get("gb_terminal", {})
                foreign = ic.get("foreign_terminal", {})
                if all(k in gb for k in ("lat", "lon")) and all(k in foreign for k in ("lat", "lon")):
                    fig.add_trace(go.Scattergeo(
                        lon=[gb["lon"], foreign["lon"]],
                        lat=[gb["lat"], foreign["lat"]],
                        mode="lines+text",
                        name=ic.get("name", ""),
                        line=dict(width=2, color="#607D8B", dash="dash"),
                        text=["", f"{ic.get('name', '')}<br>{ic.get('capacity_mw', '?')} MW"],
                        textposition="middle right",
                        textfont=dict(size=9, color="#607D8B"),
                        hovertemplate=(
                            f"<b>{ic.get('name', '')}</b><br>"
                            f"Capacity: {ic.get('capacity_mw', '?')} MW<br>"
                            f"Route: {ic.get('route', '')}<extra></extra>"
                        ),
                        showlegend=False,
                    ))
        except Exception:
            pass

    # Region nodes
    fig.add_trace(go.Scattergeo(
        lon=node_lons,
        lat=node_lats,
        mode="markers+text",
        name="DNO Regions",
        marker=dict(
            size=node_sizes,
            color=node_colours,
            opacity=0.85,
            line=dict(width=1.5, color="white"),
        ),
        text=[r.split(" ")[0] for r in DNO_REGION_CENTROIDS.keys()],
        textposition="top center",
        textfont=dict(size=10, color="#333"),
        hovertext=node_text,
        hovertemplate="%{hovertext}<extra></extra>",
    ))

    total_op = op["capacity_mw"].sum()
    fig.update_layout(
        title=dict(
            text=f"UK Grid — Regional Generation ({total_op:,.0f} MW Operational)",
            x=0.5, xanchor="center", font=dict(size=18),
        ),
        geo=dict(
            scope="europe",
            projection_type="natural earth",
            showland=True, landcolor="#F5F5F5",
            showocean=True, oceancolor="#E3F2FD",
            showcoastlines=True, coastlinecolor="#BDBDBD",
            showcountries=True, countrycolor="#E0E0E0",
            lataxis=dict(range=[48, 62]),
            lonaxis=dict(range=[-11, 5]),
        ),
        height=700,
        margin=dict(l=0, r=0, t=50, b=0),
        paper_bgcolor="white",
        showlegend=False,
    )

    return fig


def create_network_diagram(
    plants_df: pd.DataFrame | None = None,
    output_path: Path | None = None,
) -> Path:
    """Generate and save both views to a single HTML file."""
    try:
        import plotly.io as pio
    except ImportError:
        logger.error("plotly not installed")
        output_path = output_path or NETWORK_OUTPUT
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("<html><body><p>plotly required</p></body></html>")
        return output_path

    if output_path is None:
        output_path = NETWORK_OUTPUT

    if plants_df is None and PLANTS_UNIFIED.exists():
        plants_df = pd.read_parquet(PLANTS_UNIFIED)
        logger.info(f"Loaded {len(plants_df)} plants for network diagram")

    if plants_df is None:
        plants_df = pd.DataFrame()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Creating Plotly network diagrams...")

    scatter_fig = _create_operational_scatter(plants_df)
    regional_fig = _create_regional_network(plants_df)

    html_parts = [
        "<!DOCTYPE html><html><head>",
        "<meta charset='utf-8'>",
        "<title>UK Grid Network Diagrams</title>",
        "<style>",
        "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; padding: 20px 40px; background: #fafafa; }",
        "h1 { color: #1a237e; } h2 { color: #333; margin-top: 40px; }",
        "hr { border: none; border-top: 1px solid #e0e0e0; margin: 30px 0; }",
        ".stats { display: flex; gap: 20px; flex-wrap: wrap; margin: 20px 0; }",
        ".stat-card { background: white; padding: 16px 24px; border-radius: 8px; box-shadow: 0 1px 4px rgba(0,0,0,0.1); }",
        ".stat-card .value { font-size: 28px; font-weight: 700; color: #1a237e; }",
        ".stat-card .label { font-size: 13px; color: #666; }",
        "</style>",
        "</head><body>",
        "<h1>🇬🇧 UK Electricity System — Network Diagrams</h1>",
    ]

    # Summary stats
    if not plants_df.empty:
        op = plants_df[plants_df["status"] == "operational"]
        total_plants = len(plants_df)
        op_count = len(op)
        op_cap = op["capacity_mw"].sum()
        html_parts.append(
            '<div class="stats">'
            f'<div class="stat-card"><div class="value">{total_plants:,}</div><div class="label">Total Plants</div></div>'
            f'<div class="stat-card"><div class="value">{op_count:,}</div><div class="label">Operational</div></div>'
            f'<div class="stat-card"><div class="value">{op_cap:,.0f} MW</div><div class="label">Operational Capacity</div></div>'
            f'<div class="stat-card"><div class="value">10,300 MW</div><div class="label">Interconnector Capacity</div></div>'
            '</div>'
        )

    html_parts.append("<hr/>")

    for title, fig in [
        ("Operational Generation Assets", scatter_fig),
        ("Regional Generation Network", regional_fig),
    ]:
        html_parts.append(f"<h2>{title}</h2>")
        if fig is not None:
            html_parts.append(pio.to_html(fig, full_html=False, include_plotlyjs="cdn"))
        else:
            html_parts.append("<p>No data available</p>")

    html_parts.append("</body></html>")

    output_path.write_text("\n".join(html_parts))
    logger.success(f"Network diagram saved → {output_path}")
    return output_path


if __name__ == "__main__":
    create_network_diagram()
