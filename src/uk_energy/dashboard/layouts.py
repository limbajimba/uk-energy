"""
layouts.py — Dense, functional dashboard layout.

No emoji, no decoration, no donut charts. Information per pixel.
"""

from __future__ import annotations

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import dcc, html
import pandas as pd
import numpy as np

from uk_energy.dashboard.data import StaticData, LiveData
from uk_energy.dashboard.theme import FUEL_COLOURS, FUEL_LABELS

# ─── Colour palette ─────────────────────────────────────────────────────────

BMRS_COLOURS = {
    "wind": "#2196F3",
    "gas_ccgt": "#FF5722",
    "nuclear": "#9C27B0",
    "biomass": "#4CAF50",
    "hydro": "#00BCD4",
    "pumped_storage": "#006064",
    "gas_ocgt": "#FF7043",
    "oil": "#795548",
    "coal": "#212121",
    "other": "#9E9E9E",
}

IC_COLOURS = {
    "ic_france": "#1565C0",
    "ic_ifa2": "#1976D2",
    "ic_eleclink": "#1E88E5",
    "ic_nemo": "#42A5F5",
    "ic_netherlands": "#FF8F00",
    "ic_nsl": "#00897B",
    "ic_viking": "#00ACC1",
    "ic_ewic": "#43A047",
    "ic_ireland": "#66BB6A",
    "ic_greenlink": "#81C784",
}


def build_layout(static: StaticData, live: LiveData) -> html.Div:
    """Build the full dashboard layout."""
    return html.Div(
        [
            # Row 1: Live system status + generation stack
            dbc.Row(
                [
                    dbc.Col(_system_status(static, live), width=3),
                    dbc.Col(_generation_stack(live), width=9),
                ],
                className="mb-2",
            ),
            # Row 2: Demand + price + interconnectors
            dbc.Row(
                [
                    dbc.Col(_demand_chart(live), width=4),
                    dbc.Col(_price_chart(live), width=4),
                    dbc.Col(_interconnector_chart(live), width=4),
                ],
                className="mb-2",
            ),
            # Row 3: Capacity factors + installed vs actual
            dbc.Row(
                [
                    dbc.Col(_capacity_factor_table(static, live), width=5),
                    dbc.Col(_installed_vs_actual(static, live), width=7),
                ],
            ),
        ]
    )


# ─── System Status Panel ────────────────────────────────────────────────────

def _system_status(static: StaticData, live: LiveData) -> html.Div:
    """Dense text panel: current system state."""
    gen = live.latest_gen
    domestic = {k: v for k, v in gen.items() if not k.startswith("ic_")}
    imports = {k: v for k, v in gen.items() if k.startswith("ic_")}
    total_import = sum(max(0, v) for v in imports.values())
    total_export = sum(abs(min(0, v)) for v in imports.values())

    # System metrics
    installed = static.operational["capacity_mw"].sum()
    headroom = installed - live.total_generation_mw

    rows = [
        _status_row("GENERATION", f"{live.total_generation_mw:,.0f} MW", "#4CAF50"),
        _status_row("DEMAND", f"{live.latest_demand_mw:,.0f} MW", "#2196F3"),
        _status_row("PRICE", f"£{live.latest_price:.2f}/MWh", _price_colour(live.latest_price)),
        _status_row("WIND", f"{gen.get('wind', 0):,.0f} MW ({live.wind_share_pct:.0f}%)", "#2196F3"),
        _status_row("IMPORTS", f"{total_import:,.0f} MW", "#FF9800"),
        html.Hr(style={"borderColor": "#444", "margin": "6px 0"}),
        _status_row("INSTALLED", f"{installed:,.0f} MW", "#888"),
        _status_row("HEADROOM", f"{headroom:,.0f} MW", "#4CAF50" if headroom > 5000 else "#FF5722"),
    ]

    # Fuel breakdown — compact
    rows.append(html.Hr(style={"borderColor": "#444", "margin": "6px 0"}))
    rows.append(html.Div("GENERATION BY FUEL", style={"color": "#888", "fontSize": "10px", "marginBottom": "4px"}))

    for fuel in ["wind", "gas_ccgt", "nuclear", "biomass", "hydro", "pumped_storage", "gas_ocgt", "oil", "other"]:
        mw = gen.get(fuel, 0)
        if mw > 0:
            colour = BMRS_COLOURS.get(fuel, "#999")
            pct = mw / live.total_generation_mw * 100 if live.total_generation_mw > 0 else 0
            bar_width = max(2, pct)
            rows.append(
                html.Div(
                    [
                        html.Span(fuel.replace("_", " ").upper(), style={"width": "80px", "display": "inline-block", "color": "#aaa"}),
                        html.Span(
                            "",
                            style={
                                "display": "inline-block",
                                "width": f"{bar_width}%",
                                "maxWidth": "45%",
                                "height": "10px",
                                "backgroundColor": colour,
                                "marginRight": "6px",
                                "verticalAlign": "middle",
                            },
                        ),
                        html.Span(f"{mw:,.0f}", style={"color": "#ddd"}),
                        html.Span(f" ({pct:.0f}%)", style={"color": "#888"}),
                    ],
                    style={"fontSize": "11px", "lineHeight": "18px"},
                )
            )

    return html.Div(
        rows,
        style={
            "backgroundColor": "#1a1a1a",
            "border": "1px solid #333",
            "borderRadius": "4px",
            "padding": "10px",
            "height": "100%",
        },
    )


def _status_row(label: str, value: str, colour: str) -> html.Div:
    return html.Div(
        [
            html.Span(label, style={"color": "#888", "width": "90px", "display": "inline-block"}),
            html.Span(value, style={"color": colour, "fontWeight": "600"}),
        ],
        style={"lineHeight": "22px"},
    )


def _price_colour(price: float) -> str:
    if price < 30:
        return "#4CAF50"  # cheap
    elif price < 80:
        return "#FFC107"  # normal
    elif price < 150:
        return "#FF9800"  # elevated
    return "#F44336"  # expensive


# ─── Generation Stack Chart ─────────────────────────────────────────────────

def _generation_stack(live: LiveData) -> dcc.Graph:
    """Stacked area chart of generation by fuel over the last 24h."""
    gen = live.generation
    if gen.empty:
        return dcc.Graph(figure=go.Figure(), style={"height": "350px"})

    # Pivot: timestamp × fuel_type → generation
    domestic = gen[~gen["fuel_type"].str.startswith("ic_")]
    pivot = domestic.pivot_table(
        index="timestamp", columns="fuel_type", values="generation_mw", aggfunc="sum"
    ).fillna(0)

    # Order by mean contribution (largest at bottom)
    fuel_order = pivot.mean().sort_values(ascending=True).index.tolist()

    fig = go.Figure()
    for fuel in fuel_order:
        if fuel not in pivot.columns:
            continue
        colour = BMRS_COLOURS.get(fuel, "#999")
        fig.add_trace(
            go.Scatter(
                x=pivot.index,
                y=pivot[fuel],
                name=fuel.replace("_", " "),
                stackgroup="gen",
                line=dict(width=0),
                fillcolor=colour,
                marker=dict(color=colour),
                hovertemplate=f"<b>{fuel.replace('_', ' ')}</b><br>%{{y:,.0f}} MW<br>%{{x}}<extra></extra>",
            )
        )

    fig.update_layout(
        title=dict(text="GENERATION BY FUEL (24h)", font=dict(size=11, color="#888"), x=0),
        margin=dict(t=30, b=30, l=50, r=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#1a1a1a",
        font=dict(color="#ccc", size=10),
        height=350,
        xaxis=dict(gridcolor="#333", showgrid=True),
        yaxis=dict(gridcolor="#333", title="MW"),
        legend=dict(orientation="h", y=-0.15, font=dict(size=9)),
        hovermode="x unified",
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


# ─── Demand Chart ────────────────────────────────────────────────────────────

def _demand_chart(live: LiveData) -> dcc.Graph:
    """Demand time series."""
    demand = live.demand
    if demand.empty:
        return dcc.Graph(figure=go.Figure(), style={"height": "280px"})

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=demand["timestamp"],
            y=demand["demand_mw"],
            name="Demand (INDO)",
            line=dict(color="#2196F3", width=1.5),
            hovertemplate="%{y:,.0f} MW<extra>Demand</extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=demand["timestamp"],
            y=demand["transmission_demand_mw"],
            name="Transmission (ITSDO)",
            line=dict(color="#90CAF9", width=1, dash="dot"),
            hovertemplate="%{y:,.0f} MW<extra>Transmission</extra>",
        )
    )

    fig.update_layout(
        title=dict(text="SYSTEM DEMAND", font=dict(size=11, color="#888"), x=0),
        margin=dict(t=30, b=30, l=50, r=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#1a1a1a",
        font=dict(color="#ccc", size=10),
        height=280,
        xaxis=dict(gridcolor="#333"),
        yaxis=dict(gridcolor="#333", title="MW"),
        legend=dict(orientation="h", y=-0.2, font=dict(size=9)),
        hovermode="x unified",
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


# ─── Price Chart ─────────────────────────────────────────────────────────────

def _price_chart(live: LiveData) -> dcc.Graph:
    """Price time series with colour coding."""
    prices = live.prices
    if prices.empty:
        return dcc.Graph(figure=go.Figure(), style={"height": "280px"})

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=prices["timestamp"],
            y=prices["price_gbp_mwh"],
            name="Day-Ahead",
            line=dict(color="#FFC107", width=1.5),
            fill="tozeroy",
            fillcolor="rgba(255,193,7,0.1)",
            hovertemplate="£%{y:.2f}/MWh<extra></extra>",
        )
    )

    # Reference lines
    fig.add_hline(y=50, line_dash="dot", line_color="#666", annotation_text="£50", annotation_font_color="#888")

    fig.update_layout(
        title=dict(text="DAY-AHEAD PRICE (£/MWh)", font=dict(size=11, color="#888"), x=0),
        margin=dict(t=30, b=30, l=50, r=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#1a1a1a",
        font=dict(color="#ccc", size=10),
        height=280,
        xaxis=dict(gridcolor="#333"),
        yaxis=dict(gridcolor="#333", title="£/MWh"),
        showlegend=False,
        hovermode="x unified",
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


# ─── Interconnector Chart ────────────────────────────────────────────────────

def _interconnector_chart(live: LiveData) -> dcc.Graph:
    """Interconnector flows — bar showing current import/export by link."""
    gen = live.latest_gen
    ic_data = {k: v for k, v in gen.items() if k.startswith("ic_")}

    if not ic_data:
        return dcc.Graph(figure=go.Figure(), style={"height": "280px"})

    names = [k.replace("ic_", "").upper() for k in ic_data.keys()]
    values = list(ic_data.values())
    colours = ["#4CAF50" if v > 0 else "#F44336" for v in values]

    fig = go.Figure(
        go.Bar(
            y=names,
            x=values,
            orientation="h",
            marker_color=colours,
            text=[f"{v:+,.0f}" for v in values],
            textposition="auto",
            hovertemplate="<b>%{y}</b><br>%{x:+,.0f} MW<extra></extra>",
        )
    )

    fig.add_vline(x=0, line_color="#666")

    fig.update_layout(
        title=dict(text="INTERCONNECTOR FLOWS (MW)", font=dict(size=11, color="#888"), x=0),
        margin=dict(t=30, b=20, l=80, r=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#1a1a1a",
        font=dict(color="#ccc", size=10),
        height=280,
        xaxis=dict(gridcolor="#333", title="Import → | ← Export"),
        yaxis=dict(gridcolor="#333"),
        showlegend=False,
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


# ─── Capacity Factor Table ──────────────────────────────────────────────────

def _capacity_factor_table(static: StaticData, live: LiveData) -> html.Div:
    """Installed capacity vs current dispatch → capacity factor."""
    gen = live.latest_gen

    # Map BMRS fuels back to our installed capacity fuels
    fuel_map = {
        "wind": ["wind_onshore", "wind_offshore"],
        "gas_ccgt": ["gas_ccgt"],
        "nuclear": ["nuclear"],
        "biomass": ["biomass"],
        "hydro": ["hydro_run_of_river"],
        "pumped_storage": ["hydro_pumped_storage"],
        "gas_ocgt": ["gas_ocgt"],
        "oil": ["oil"],
    }

    installed = static.fuel_capacity.set_index("fuel_type")["capacity_mw"].to_dict()

    rows = []
    for bmrs_fuel, our_fuels in fuel_map.items():
        inst = sum(installed.get(f, 0) for f in our_fuels)
        actual = gen.get(bmrs_fuel, 0)
        cf = (actual / inst * 100) if inst > 0 else 0
        colour = BMRS_COLOURS.get(bmrs_fuel, "#999")

        # CF bar
        cf_bar_colour = "#4CAF50" if cf > 50 else "#FFC107" if cf > 20 else "#F44336"

        rows.append(
            html.Tr(
                [
                    html.Td(
                        html.Span("■ ", style={"color": colour}),
                    ),
                    html.Td(bmrs_fuel.replace("_", " ").upper(), style={"fontWeight": "500"}),
                    html.Td(f"{inst:,.0f}", style={"textAlign": "right", "color": "#888"}),
                    html.Td(f"{actual:,.0f}", style={"textAlign": "right", "color": "#ddd"}),
                    html.Td(
                        html.Div(
                            [
                                html.Div(
                                    style={
                                        "width": f"{min(cf, 100):.0f}%",
                                        "height": "8px",
                                        "backgroundColor": cf_bar_colour,
                                        "borderRadius": "2px",
                                    }
                                ),
                            ],
                            style={"width": "60px", "backgroundColor": "#333", "borderRadius": "2px"},
                        ),
                    ),
                    html.Td(f"{cf:.0f}%", style={"textAlign": "right", "color": cf_bar_colour}),
                ],
                style={"lineHeight": "24px"},
            )
        )

    return html.Div(
        [
            html.Div("CAPACITY FACTORS (CURRENT)", style={"color": "#888", "fontSize": "11px", "marginBottom": "8px", "fontWeight": "600"}),
            html.Table(
                [
                    html.Thead(
                        html.Tr(
                            [
                                html.Th("", style={"width": "20px"}),
                                html.Th("FUEL"),
                                html.Th("INSTALLED", style={"textAlign": "right"}),
                                html.Th("DISPATCH", style={"textAlign": "right"}),
                                html.Th(""),
                                html.Th("CF", style={"textAlign": "right"}),
                            ],
                            style={"color": "#666", "fontSize": "10px"},
                        )
                    ),
                    html.Tbody(rows),
                ],
                style={"width": "100%", "borderCollapse": "collapse"},
            ),
        ],
        style={
            "backgroundColor": "#1a1a1a",
            "border": "1px solid #333",
            "borderRadius": "4px",
            "padding": "10px",
        },
    )


# ─── Installed vs Actual ────────────────────────────────────────────────────

def _installed_vs_actual(static: StaticData, live: LiveData) -> dcc.Graph:
    """Grouped bar: installed capacity vs current dispatch by fuel."""
    gen = live.latest_gen
    installed = static.fuel_capacity.set_index("fuel_type")["capacity_mw"].to_dict()

    fuel_map = {
        "wind": ["wind_onshore", "wind_offshore"],
        "gas_ccgt": ["gas_ccgt"],
        "nuclear": ["nuclear"],
        "biomass": ["biomass"],
        "hydro": ["hydro_run_of_river"],
        "pumped_storage": ["hydro_pumped_storage"],
        "gas_ocgt": ["gas_ocgt"],
        "oil": ["oil"],
    }

    fuels = []
    inst_vals = []
    disp_vals = []
    for bmrs_fuel, our_fuels in fuel_map.items():
        inst = sum(installed.get(f, 0) for f in our_fuels) / 1000
        actual = gen.get(bmrs_fuel, 0) / 1000
        fuels.append(bmrs_fuel.replace("_", " ").upper())
        inst_vals.append(inst)
        disp_vals.append(actual)

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=fuels, y=inst_vals, name="Installed",
            marker_color="rgba(255,255,255,0.15)",
            hovertemplate="%{y:.1f} GW<extra>Installed</extra>",
        )
    )
    fig.add_trace(
        go.Bar(
            x=fuels, y=disp_vals, name="Dispatched",
            marker_color=[BMRS_COLOURS.get(f.lower().replace(" ", "_"), "#999") for f in fuels],
            hovertemplate="%{y:.1f} GW<extra>Dispatched</extra>",
        )
    )

    fig.update_layout(
        title=dict(text="INSTALLED vs DISPATCHED (GW)", font=dict(size=11, color="#888"), x=0),
        barmode="overlay",
        margin=dict(t=30, b=30, l=50, r=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#1a1a1a",
        font=dict(color="#ccc", size=10),
        height=300,
        xaxis=dict(gridcolor="#333"),
        yaxis=dict(gridcolor="#333", title="GW"),
        legend=dict(orientation="h", y=-0.15, font=dict(size=9)),
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})
