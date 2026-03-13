"""
generation.py — Generation mix analysis page.

Fuel type breakdown with treemap, horizontal bars, and capacity factor readiness.
"""

from __future__ import annotations

import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html

from uk_energy.dashboard.data import DashboardData
from uk_energy.dashboard.theme import FUEL_COLOURS, FUEL_LABELS, FUEL_ORDER, card


def layout(data: DashboardData) -> html.Div:
    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(card("Capacity Treemap", _treemap(data)), md=7),
                    dbc.Col(card("Fuel Type Breakdown", _fuel_bars(data)), md=5),
                ],
                className="mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(card("Dispatchable vs Variable", _dispatch_chart(data)), md=6),
                    dbc.Col(card("Technology Deep Dive", _tech_table(data)), md=6),
                ],
            ),
        ]
    )


def _treemap(data: DashboardData) -> dcc.Graph:
    df = data.dukes_operational.copy()
    df["fuel_label"] = df["fuel_type"].map(FUEL_LABELS).fillna(df["fuel_type"])
    df["gw"] = df["capacity_mw"] / 1000
    df["colour"] = df["fuel_type"].map(FUEL_COLOURS).fillna("#999")

    fig = px.treemap(
        df[df["capacity_mw"] > 10],  # Skip tiny plants for readability
        path=["fuel_label", "name"],
        values="capacity_mw",
        color="fuel_label",
        color_discrete_map={FUEL_LABELS.get(f, f): c for f, c in FUEL_COLOURS.items()},
        hover_data={"capacity_mw": ":.0f"},
    )
    fig.update_layout(
        margin=dict(t=10, b=10, l=10, r=10),
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ddd"),
        height=500,
    )
    fig.update_traces(
        hovertemplate="<b>%{label}</b><br>%{value:,.0f} MW<extra></extra>",
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _fuel_bars(data: DashboardData) -> dcc.Graph:
    df = data.fuel_mix.copy()
    df["label"] = df["fuel_type"].map(FUEL_LABELS).fillna(df["fuel_type"])
    df["colour"] = df["fuel_type"].map(FUEL_COLOURS).fillna("#999")
    df["gw"] = df["capacity_mw"] / 1000
    df = df.sort_values("capacity_mw")

    fig = go.Figure(
        go.Bar(
            y=df["label"],
            x=df["gw"],
            orientation="h",
            marker_color=df["colour"].tolist(),
            text=df["gw"].apply(lambda x: f"{x:.1f} GW"),
            textposition="auto",
            hovertemplate="<b>%{y}</b><br>%{x:.1f} GW<extra></extra>",
        )
    )
    fig.update_layout(
        xaxis_title="Capacity (GW)",
        yaxis_title="",
        margin=dict(t=10, b=40, l=120, r=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ddd"),
        height=500,
        xaxis=dict(gridcolor="rgba(255,255,255,0.1)"),
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _dispatch_chart(data: DashboardData) -> dcc.Graph:
    """Dispatchable (gas, nuclear, biomass, hydro, oil) vs Variable (wind, solar)."""
    df = data.dukes_operational.copy()

    dispatchable = {"gas_ccgt", "gas_ocgt", "gas_chp", "nuclear", "biomass",
                    "hydro_pumped_storage", "hydro_run_of_river", "oil"}
    variable = {"wind_onshore", "wind_offshore", "solar_pv"}
    storage = {"battery_storage", "hydro_pumped_storage"}

    categories = []
    for _, row in df.iterrows():
        ft = row["fuel_type"]
        if ft in storage:
            categories.append("Storage / Flexible")
        elif ft in variable:
            categories.append("Variable Renewable")
        elif ft in dispatchable:
            categories.append("Dispatchable")
        else:
            categories.append("Other")

    df["category"] = categories
    cat_cap = df.groupby("category")["capacity_mw"].sum().reset_index()
    cat_cap["gw"] = cat_cap["capacity_mw"] / 1000

    cat_colours = {
        "Dispatchable": "#FF5722",
        "Variable Renewable": "#2196F3",
        "Storage / Flexible": "#E91E63",
        "Other": "#9E9E9E",
    }

    fig = go.Figure(
        go.Pie(
            labels=cat_cap["category"],
            values=cat_cap["gw"],
            marker=dict(colors=[cat_colours.get(c, "#999") for c in cat_cap["category"]]),
            textinfo="label+value",
            texttemplate="%{label}<br>%{value:.1f} GW",
            hovertemplate="<b>%{label}</b><br>%{value:.1f} GW<br>%{percent}<extra></extra>",
        )
    )
    fig.update_layout(
        margin=dict(t=20, b=20, l=20, r=20),
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ddd"),
        height=380,
        showlegend=True,
        legend=dict(font=dict(color="#ccc")),
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _tech_table(data: DashboardData) -> html.Div:
    df = data.dukes_operational.copy()
    summary = (
        df.groupby("fuel_type")
        .agg(
            plants=("name", "size"),
            total_mw=("capacity_mw", "sum"),
            avg_mw=("capacity_mw", "mean"),
            max_mw=("capacity_mw", "max"),
        )
        .sort_values("total_mw", ascending=False)
        .reset_index()
    )

    rows = []
    for _, r in summary.iterrows():
        fuel = r["fuel_type"]
        label = FUEL_LABELS.get(fuel, fuel)
        colour = FUEL_COLOURS.get(fuel, "#999")
        rows.append(
            html.Tr(
                [
                    html.Td(
                        [html.Span("●", style={"color": colour, "marginRight": "6px"}), label],
                    ),
                    html.Td(f"{r['plants']:,}", style={"textAlign": "right"}),
                    html.Td(f"{r['total_mw'] / 1000:.1f} GW", style={"textAlign": "right", "fontWeight": "600"}),
                    html.Td(f"{r['avg_mw']:.0f} MW", style={"textAlign": "right"}),
                    html.Td(f"{r['max_mw']:,.0f} MW", style={"textAlign": "right"}),
                ]
            )
        )

    return html.Table(
        [
            html.Thead(
                html.Tr(
                    [
                        html.Th("Fuel Type"),
                        html.Th("Plants", style={"textAlign": "right"}),
                        html.Th("Total", style={"textAlign": "right"}),
                        html.Th("Avg", style={"textAlign": "right"}),
                        html.Th("Max", style={"textAlign": "right"}),
                    ]
                )
            ),
            html.Tbody(rows),
        ],
        className="table table-dark table-sm table-hover",
        style={"fontSize": "13px"},
    )
