"""
network.py — Plotly network diagram of the UK grid topology.

Two views:
  1. Plant-level: all generation nodes with size=capacity, colour=fuel type
  2. Regional aggregate: 14 DNO regions as nodes, interconnectors + top transmission as edges
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import networkx as nx
import pandas as pd
from loguru import logger

from uk_energy.config import INTERCONNECTORS_REF, NETWORK_OUTPUT, OUTPUT_DIR, PLANTS_UNIFIED
from uk_energy.graph.model import NodeType


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
    "hydro_pumped_storage":"#006064",
    "battery_storage":    "#E91E63",
    "interconnector":     "#607D8B",
    "other":              "#9E9E9E",
    "unknown":            "#BDBDBD",
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


def create_plant_scatter(
    plants_df: pd.DataFrame,
    G: nx.DiGraph | None = None,
) -> Any:
    """
    Create a Plotly scatter geo plot of all generation plants.

    Node size ∝ log(capacity), colour = fuel type.
    """
    try:
        import plotly.express as px
        import plotly.graph_objects as go
    except ImportError:
        logger.error("plotly not installed")
        return None

    if plants_df.empty:
        logger.warning("No plant data for scatter plot")
        return None

    # Filter to plants with valid coordinates
    df = plants_df[
        plants_df["lat"].notna() &
        plants_df["lon"].notna() &
        plants_df["lat"].between(49.5, 61.5) &
        plants_df["lon"].between(-9, 2)
    ].copy()

    # Compute marker size from log capacity
    df["marker_size"] = df["capacity_mw"].apply(
        lambda x: max(3, min(20, 3 + math.log10(max(1, x or 1)) * 3))
    )
    df["fuel_label"] = df["fuel_type"].str.replace("_", " ").str.title()
    df["colour"] = df["fuel_type"].map(FUEL_COLOURS).fillna(FUEL_COLOURS["unknown"])

    # Build scatter traces per fuel type
    traces: list[Any] = []
    for fuel in df["fuel_type"].unique():
        subset = df[df["fuel_type"] == fuel]
        colour = FUEL_COLOURS.get(fuel, FUEL_COLOURS["unknown"])
        label = fuel.replace("_", " ").title()

        trace = go.Scattergeo(
            lon=subset["lon"],
            lat=subset["lat"],
            mode="markers",
            name=label,
            marker=dict(
                size=subset["marker_size"],
                color=colour,
                opacity=0.75,
                line=dict(width=0.5, color="white"),
            ),
            text=subset.apply(
                lambda r: (
                    f"<b>{r.get('name', 'Unknown')}</b><br>"
                    f"Fuel: {label}<br>"
                    f"Capacity: {r.get('capacity_mw', 'N/A')} MW<br>"
                    f"Status: {r.get('status', 'N/A')}<br>"
                    f"Owner: {r.get('owner', 'N/A')}"
                ),
                axis=1,
            ),
            hovertemplate="%{text}<extra></extra>",
        )
        traces.append(trace)

    fig = go.Figure(data=traces)
    fig.update_layout(
        title={
            "text": "UK Generation Assets",
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 20},
        },
        geo=dict(
            scope="europe",
            projection_type="mercator",
            showland=True,
            landcolor="#F5F5F5",
            showocean=True,
            oceancolor="#E3F2FD",
            showcoastlines=True,
            coastlinecolor="#BDBDBD",
            lataxis=dict(range=[49, 62]),
            lonaxis=dict(range=[-10, 3]),
            center=dict(lat=54.5, lon=-2.5),
        ),
        legend=dict(
            title="Fuel Type",
            itemsizing="constant",
            x=1.0,
            y=0.5,
        ),
        height=800,
        margin=dict(l=0, r=0, t=50, b=0),
        paper_bgcolor="white",
        plot_bgcolor="white",
    )

    return fig


def create_regional_network(
    G: nx.DiGraph | None = None,
    plants_df: pd.DataFrame | None = None,
) -> Any:
    """
    Create a Plotly Scattergeo network diagram with DNO regions as nodes.

    Node size = total regional capacity, edges = interconnectors + major links.
    """
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.error("plotly not installed")
        return None

    # Compute regional capacity from plants
    regional_capacity: dict[str, float] = {}
    regional_fuel_split: dict[str, dict[str, float]] = {}

    if plants_df is not None and not plants_df.empty:
        for _, row in plants_df.iterrows():
            region = str(row.get("dno_region") or "Unknown")
            cap = float(row.get("capacity_mw") or 0)
            fuel = str(row.get("fuel_type") or "unknown")
            regional_capacity[region] = regional_capacity.get(region, 0) + cap
            if region not in regional_fuel_split:
                regional_fuel_split[region] = {}
            regional_fuel_split[region][fuel] = regional_fuel_split[region].get(fuel, 0) + cap

    elif G is not None:
        for _, data in G.nodes(data=True):
            if data.get("node_type") == NodeType.GENERATION_PLANT.value:
                region = str(data.get("dno_region") or "Unknown")
                cap = float(data.get("capacity_mw") or 0)
                regional_capacity[region] = regional_capacity.get(region, 0) + cap

    # Region nodes
    node_lats, node_lons, node_text, node_sizes, node_colours = [], [], [], [], []

    for region, (lat, lon) in DNO_REGION_CENTROIDS.items():
        cap = regional_capacity.get(region, 0)
        # Top fuel type
        fuels = regional_fuel_split.get(region, {})
        top_fuel = max(fuels, key=fuels.get) if fuels else "unknown"
        colour = FUEL_COLOURS.get(top_fuel, "#9E9E9E")

        node_lats.append(lat)
        node_lons.append(lon)
        node_text.append(
            f"<b>{region}</b><br>"
            f"Total: {cap:,.0f} MW<br>"
            f"Top fuel: {top_fuel.replace('_', ' ').title()}"
        )
        node_sizes.append(max(10, min(60, cap / 500)))  # scale 0–60px
        node_colours.append(colour)

    # Interconnector edges
    edge_lats_all: list[float | None] = []
    edge_lons_all: list[float | None] = []
    edge_labels: list[str] = []

    if INTERCONNECTORS_REF.exists():
        try:
            ic_data = json.loads(INTERCONNECTORS_REF.read_text())
            for ic in ic_data.get("interconnectors", []):
                gb = ic.get("gb_terminal", {})
                foreign = ic.get("foreign_terminal", {})
                if all(k in gb for k in ("lat", "lon")) and all(k in foreign for k in ("lat", "lon")):
                    edge_lats_all.extend([gb["lat"], foreign["lat"], None])
                    edge_lons_all.extend([gb["lon"], foreign["lon"], None])
        except Exception:
            pass

    fig = go.Figure()

    # Add interconnector lines
    if edge_lats_all:
        fig.add_trace(go.Scattergeo(
            lon=edge_lons_all,
            lat=edge_lats_all,
            mode="lines",
            name="Interconnectors",
            line=dict(width=2, color="#607D8B"),
            hoverinfo="none",
        ))

    # Add region nodes
    fig.add_trace(go.Scattergeo(
        lon=node_lons,
        lat=node_lats,
        mode="markers+text",
        name="DNO Regions",
        marker=dict(
            size=node_sizes,
            color=node_colours,
            opacity=0.85,
            line=dict(width=1, color="white"),
        ),
        text=[r.split(" ")[0] for r in DNO_REGION_CENTROIDS.keys()],
        textposition="top center",
        hovertext=node_text,
        hovertemplate="%{hovertext}<extra></extra>",
    ))

    fig.update_layout(
        title={
            "text": "UK Grid — Regional Generation Capacity",
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 18},
        },
        geo=dict(
            scope="europe",
            projection_type="mercator",
            showland=True,
            landcolor="#F5F5F5",
            showocean=True,
            oceancolor="#E3F2FD",
            showcoastlines=True,
            coastlinecolor="#BDBDBD",
            lataxis=dict(range=[48, 63]),
            lonaxis=dict(range=[-12, 5]),
        ),
        height=700,
        margin=dict(l=0, r=0, t=50, b=0),
        paper_bgcolor="white",
    )

    return fig


def create_network_diagram(
    G: nx.DiGraph | None = None,
    plants_df: pd.DataFrame | None = None,
    output_path: Path | None = None,
) -> Path:
    """
    Generate and save both network visualisation views to a single HTML file.
    """
    try:
        import plotly.io as pio
        from plotly.subplots import make_subplots
        import plotly.graph_objects as go
    except ImportError:
        logger.error("plotly not installed — cannot create network diagram")
        output_path = output_path or NETWORK_OUTPUT
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("<html><body><p>plotly not installed</p></body></html>")
        return output_path

    if output_path is None:
        output_path = NETWORK_OUTPUT

    # Load plants if not provided
    if plants_df is None and PLANTS_UNIFIED.exists():
        plants_df = pd.read_parquet(PLANTS_UNIFIED)
        logger.info(f"Loaded {len(plants_df)} plants for network diagram")

    if plants_df is None:
        plants_df = pd.DataFrame()

    logger.info("Creating Plotly network diagrams...")

    scatter_fig = create_plant_scatter(plants_df, G)
    regional_fig = create_regional_network(G, plants_df)

    # Save each figure + combine into single HTML
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    html_parts: list[str] = [
        "<html><head>",
        "<title>UK Grid Network Diagrams</title>",
        "<style>body{font-family:sans-serif;margin:0;padding:20px;background:#fafafa;}</style>",
        "</head><body>",
        "<h1>🇬🇧 UK Electricity System — Network Diagrams</h1>",
        "<hr/>",
    ]

    for title, fig in [("Generation Assets", scatter_fig), ("Regional Network", regional_fig)]:
        if fig is not None:
            html_parts.append(f"<h2>{title}</h2>")
            html_parts.append(pio.to_html(fig, full_html=False, include_plotlyjs="cdn"))
        else:
            html_parts.append(f"<h2>{title}</h2><p>No data available</p>")

    html_parts.append("</body></html>")

    output_path.write_text("\n".join(html_parts))
    logger.success(f"Network diagram saved → {output_path}")
    return output_path


if __name__ == "__main__":
    create_network_diagram()
