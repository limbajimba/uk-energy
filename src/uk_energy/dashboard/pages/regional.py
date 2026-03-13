"""
regional.py — Regional capacity analysis page.

Stacked bar by region + fuel, choropleth-style comparison, regional fuel mix.
"""

from __future__ import annotations

import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html
import pandas as pd

from uk_energy.dashboard.data import DashboardData
from uk_energy.dashboard.theme import FUEL_COLOURS, FUEL_LABELS, FUEL_ORDER, card


def layout(data: DashboardData) -> html.Div:
    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(card("Regional Capacity by Fuel Type", _stacked_bar(data)), md=12),
                ],
                className="mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(card("Regional Comparison", _region_scatter(data)), md=7),
                    dbc.Col(card("Regional Summary", _region_table(data)), md=5),
                ],
            ),
        ]
    )


def _stacked_bar(data: DashboardData) -> dcc.Graph:
    df = data.dukes_operational.copy()
    df["fuel_label"] = df["fuel_type"].map(FUEL_LABELS).fillna(df["fuel_type"])

    # Pivot: region × fuel_type → capacity
    pivot = (
        df.groupby(["dno_region", "fuel_type"])["capacity_mw"]
        .sum()
        .reset_index()
    )
    pivot["fuel_label"] = pivot["fuel_type"].map(FUEL_LABELS).fillna(pivot["fuel_type"])
    pivot["gw"] = pivot["capacity_mw"] / 1000

    # Order regions by total capacity
    region_order = (
        pivot.groupby("dno_region")["gw"].sum().sort_values(ascending=False).index.tolist()
    )

    # Order fuels
    fuel_order_labels = [FUEL_LABELS.get(f, f) for f in FUEL_ORDER if f in df["fuel_type"].values]

    fig = go.Figure()
    for fuel in FUEL_ORDER:
        if fuel not in df["fuel_type"].values:
            continue
        label = FUEL_LABELS.get(fuel, fuel)
        colour = FUEL_COLOURS.get(fuel, "#999")
        fuel_data = pivot[pivot["fuel_type"] == fuel]

        fig.add_trace(
            go.Bar(
                x=fuel_data["dno_region"],
                y=fuel_data["gw"],
                name=label,
                marker_color=colour,
                hovertemplate=f"<b>{label}</b><br>%{{x}}<br>%{{y:.1f}} GW<extra></extra>",
            )
        )

    fig.update_layout(
        barmode="stack",
        xaxis=dict(categoryorder="array", categoryarray=region_order, tickangle=-35),
        yaxis_title="Capacity (GW)",
        margin=dict(t=20, b=100, l=50, r=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ddd"),
        height=450,
        legend=dict(orientation="h", y=-0.35, font=dict(size=10, color="#ccc")),
        yaxis=dict(gridcolor="rgba(255,255,255,0.1)"),
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _region_scatter(data: DashboardData) -> dcc.Graph:
    """Bubble chart: x=renewable%, y=total capacity, size=plant count."""
    df = data.dukes_operational.copy()

    renewable_fuels = {
        "wind_onshore", "wind_offshore", "solar_pv",
        "hydro_run_of_river", "wave_tidal", "biomass",
    }

    regional = []
    for region, group in df.groupby("dno_region"):
        if pd.isna(region):
            continue
        total = group["capacity_mw"].sum()
        renewable = group[group["fuel_type"].isin(renewable_fuels)]["capacity_mw"].sum()
        regional.append({
            "region": region,
            "total_gw": total / 1000,
            "renewable_pct": (renewable / total * 100) if total > 0 else 0,
            "n_plants": len(group),
        })

    rdf = pd.DataFrame(regional)

    fig = px.scatter(
        rdf,
        x="renewable_pct",
        y="total_gw",
        size="n_plants",
        text="region",
        hover_data={"renewable_pct": ":.1f", "total_gw": ":.1f", "n_plants": True},
        size_max=40,
    )
    fig.update_traces(
        textposition="top center",
        marker=dict(color="#2196F3", opacity=0.7),
        textfont=dict(size=10, color="#ccc"),
    )
    fig.update_layout(
        xaxis_title="Renewable Share (%)",
        yaxis_title="Total Capacity (GW)",
        margin=dict(t=20, b=40, l=50, r=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ddd"),
        height=400,
        xaxis=dict(gridcolor="rgba(255,255,255,0.1)", range=[0, 100]),
        yaxis=dict(gridcolor="rgba(255,255,255,0.1)"),
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _region_table(data: DashboardData) -> html.Div:
    df = data.dukes_operational.copy()

    renewable_fuels = {
        "wind_onshore", "wind_offshore", "solar_pv",
        "hydro_run_of_river", "wave_tidal", "biomass",
    }

    rows = []
    for region, group in df.groupby("dno_region"):
        if pd.isna(region):
            continue
        total = group["capacity_mw"].sum()
        renewable = group[group["fuel_type"].isin(renewable_fuels)]["capacity_mw"].sum()
        ren_pct = renewable / total * 100 if total > 0 else 0
        top_fuel = group.groupby("fuel_type")["capacity_mw"].sum().idxmax()

        rows.append({
            "region": region,
            "total_gw": total / 1000,
            "renewable_pct": ren_pct,
            "n_plants": len(group),
            "top_fuel": top_fuel,
        })

    rdf = pd.DataFrame(rows).sort_values("total_gw", ascending=False)

    table_rows = []
    for _, r in rdf.iterrows():
        fuel_label = FUEL_LABELS.get(r["top_fuel"], r["top_fuel"])
        fuel_colour = FUEL_COLOURS.get(r["top_fuel"], "#999")
        table_rows.append(
            html.Tr(
                [
                    html.Td(r["region"], style={"fontWeight": "500"}),
                    html.Td(f"{r['total_gw']:.1f} GW", style={"textAlign": "right"}),
                    html.Td(f"{r['renewable_pct']:.0f}%", style={"textAlign": "right"}),
                    html.Td(f"{r['n_plants']}", style={"textAlign": "right"}),
                    html.Td(
                        [html.Span("●", style={"color": fuel_colour, "marginRight": "4px"}), fuel_label],
                        style={"fontSize": "12px"},
                    ),
                ]
            )
        )

    return html.Table(
        [
            html.Thead(
                html.Tr(
                    [
                        html.Th("Region"),
                        html.Th("Capacity", style={"textAlign": "right"}),
                        html.Th("Renew.", style={"textAlign": "right"}),
                        html.Th("Plants", style={"textAlign": "right"}),
                        html.Th("Dominant"),
                    ]
                )
            ),
            html.Tbody(table_rows),
        ],
        className="table table-dark table-sm table-hover",
        style={"fontSize": "12px"},
    )
