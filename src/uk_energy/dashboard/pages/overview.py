"""
overview.py — System overview dashboard page.

KPI cards + fuel mix donut + status breakdown + interconnector summary.
"""

from __future__ import annotations

import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html

from uk_energy.dashboard.data import DashboardData
from uk_energy.dashboard.theme import FUEL_COLOURS, FUEL_LABELS, card, kpi_card


def layout(data: DashboardData) -> html.Div:
    ic_capacity = sum(ic.get("capacity_mw", 0) for ic in data.interconnectors)

    return html.Div(
        [
            # ─── KPI Row ───
            dbc.Row(
                [
                    dbc.Col(kpi_card("Operational Capacity", f"{data.total_operational_mw / 1000:,.1f} GW", "fa-bolt", "success"), md=3),
                    dbc.Col(kpi_card("Operational Plants", f"{data.n_operational:,}", "fa-industry", "primary"), md=3),
                    dbc.Col(kpi_card("Interconnectors", f"{ic_capacity / 1000:,.1f} GW", "fa-right-left", "info"), md=3),
                    dbc.Col(kpi_card("Total Pipeline", f"{data.total_capacity_mw / 1000:,.0f} GW", "fa-chart-line", "warning"), md=3),
                ],
                className="mb-4",
            ),
            # ─── Charts Row ───
            dbc.Row(
                [
                    dbc.Col(card("Generation Mix (Operational)", _fuel_donut(data)), md=6),
                    dbc.Col(card("Capacity by Status", _status_bar(data)), md=6),
                ],
                className="mb-4",
            ),
            # ─── Bottom Row ───
            dbc.Row(
                [
                    dbc.Col(card("Top 10 Stations", _top_stations_table(data)), md=6),
                    dbc.Col(card("Interconnectors", _interconnector_table(data)), md=6),
                ],
            ),
        ]
    )


def _fuel_donut(data: DashboardData) -> dcc.Graph:
    df = data.fuel_mix.copy()
    df["label"] = df["fuel_type"].map(FUEL_LABELS).fillna(df["fuel_type"])
    df["colour"] = df["fuel_type"].map(FUEL_COLOURS).fillna("#999")
    df["gw"] = df["capacity_mw"] / 1000

    fig = go.Figure(
        go.Pie(
            labels=df["label"],
            values=df["gw"],
            hole=0.55,
            marker=dict(colors=df["colour"].tolist()),
            textinfo="label+percent",
            textposition="outside",
            hovertemplate="<b>%{label}</b><br>%{value:.1f} GW<br>%{percent}<extra></extra>",
        )
    )
    fig.update_layout(
        showlegend=False,
        margin=dict(t=20, b=20, l=20, r=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ddd"),
        height=380,
        annotations=[
            dict(
                text=f"<b>{data.total_operational_mw / 1000:.1f}</b><br>GW",
                x=0.5, y=0.5, font_size=22, showarrow=False,
                font=dict(color="#eee"),
            )
        ],
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _status_bar(data: DashboardData) -> dcc.Graph:
    df = data.status_summary.copy()
    df["gw"] = df["capacity_mw"] / 1000

    status_colours = {
        "operational": "#4CAF50",
        "operational_unverified": "#8BC34A",
        "consented": "#2196F3",
        "construction": "#FF9800",
        "planning": "#9E9E9E",
        "decommissioned": "#F44336",
        "unknown": "#616161",
    }
    df["colour"] = df["status"].map(status_colours).fillna("#999")

    fig = go.Figure(
        go.Bar(
            x=df["status"],
            y=df["gw"],
            marker_color=df["colour"].tolist(),
            text=df.apply(lambda r: f"{r['gw']:.1f} GW<br>{r['count']:,}", axis=1),
            textposition="auto",
            hovertemplate="<b>%{x}</b><br>%{y:.1f} GW<br>%{text}<extra></extra>",
        )
    )
    fig.update_layout(
        xaxis_title="",
        yaxis_title="Capacity (GW)",
        margin=dict(t=20, b=40, l=50, r=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ddd"),
        height=380,
        yaxis=dict(gridcolor="rgba(255,255,255,0.1)"),
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _top_stations_table(data: DashboardData) -> html.Div:
    top = data.dukes_operational.nlargest(10, "capacity_mw")
    rows = []
    for _, r in top.iterrows():
        fuel = FUEL_LABELS.get(r["fuel_type"], r["fuel_type"])
        colour = FUEL_COLOURS.get(r["fuel_type"], "#999")
        rows.append(
            html.Tr(
                [
                    html.Td(r["name"], style={"fontWeight": "500"}),
                    html.Td(
                        [html.Span("●", style={"color": colour, "marginRight": "6px"}), fuel],
                    ),
                    html.Td(f"{r['capacity_mw']:,.0f} MW", style={"textAlign": "right"}),
                    html.Td(r.get("dno_region", ""), style={"color": "#aaa"}),
                ]
            )
        )

    return html.Table(
        [
            html.Thead(
                html.Tr(
                    [html.Th("Station"), html.Th("Fuel"), html.Th("Capacity", style={"textAlign": "right"}), html.Th("Region")]
                )
            ),
            html.Tbody(rows),
        ],
        className="table table-dark table-sm table-hover",
        style={"fontSize": "13px"},
    )


def _interconnector_table(data: DashboardData) -> html.Div:
    rows = []
    for ic in sorted(data.interconnectors, key=lambda x: x.get("capacity_mw", 0), reverse=True):
        rows.append(
            html.Tr(
                [
                    html.Td(ic.get("name", ""), style={"fontWeight": "500"}),
                    html.Td(ic.get("route", "")),
                    html.Td(f"{ic.get('capacity_mw', 0):,} MW", style={"textAlign": "right"}),
                    html.Td(ic.get("foreign_country", ""), style={"color": "#aaa"}),
                ]
            )
        )

    return html.Table(
        [
            html.Thead(
                html.Tr(
                    [html.Th("Name"), html.Th("Route"), html.Th("Capacity", style={"textAlign": "right"}), html.Th("Country")]
                )
            ),
            html.Tbody(rows),
        ],
        className="table table-dark table-sm table-hover",
        style={"fontSize": "13px"},
    )
