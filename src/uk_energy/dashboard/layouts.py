"""
layouts.py — Dashboard tab layouts.

Three tabs:
  1. Live System: generation stack, demand, prices, capacity factors, IC flows
  2. Asset Map: Plotly Mapbox scatter of all plants (replaces Folium)
  3. Data Sources: ingestion inventory, freshness, row counts
"""

from __future__ import annotations

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import dcc, html
import pandas as pd
import numpy as np

from uk_energy.dashboard.data import StaticData, LiveData
from uk_energy.dashboard.theme import FUEL_COLOURS, FUEL_LABELS

# ─── Colour maps ─────────────────────────────────────────────────────────────

BMRS_COLOURS = {
    "wind": "#2196F3",
    "solar": "#FFC107",
    "nuclear": "#9C27B0",
    "gas_ccgt": "#FF5722",
    "gas_ocgt": "#FF7043",
    "biomass": "#4CAF50",
    "hydro": "#00BCD4",
    "pumped_storage": "#006064",
    "oil": "#795548",
    "coal": "#424242",
    "other": "#9E9E9E",
}

# BMRS fuel → installed capacity fuels (for capacity factor)
FUEL_INSTALLED_MAP = {
    "wind": ["wind_onshore", "wind_offshore"],
    "solar": ["solar_pv"],
    "nuclear": ["nuclear"],
    "gas_ccgt": ["gas_ccgt"],
    "gas_ocgt": ["gas_ocgt"],
    "biomass": ["biomass"],
    "hydro": ["hydro_run_of_river"],
    "pumped_storage": ["hydro_pumped_storage"],
    "oil": ["oil"],
}


def _panel(children, **style_overrides) -> html.Div:
    """Dark panel wrapper."""
    style = {
        "backgroundColor": "#1a1a1a",
        "border": "1px solid #333",
        "borderRadius": "3px",
        "padding": "8px",
        **style_overrides,
    }
    return html.Div(children, style=style)


def _label(text: str) -> html.Div:
    return html.Div(text, style={"color": "#666", "fontSize": "10px", "fontWeight": "600", "marginBottom": "4px"})


def _chart_layout(title: str = "", height: int = 280) -> dict:
    """Common chart layout settings."""
    return dict(
        title=dict(text=title, font=dict(size=10, color="#666"), x=0, y=0.98) if title else None,
        margin=dict(t=25 if title else 10, b=25, l=45, r=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#1a1a1a",
        font=dict(color="#aaa", size=9, family="'JetBrains Mono', monospace"),
        height=height,
        xaxis=dict(gridcolor="#2a2a2a", zeroline=False),
        yaxis=dict(gridcolor="#2a2a2a", zeroline=False),
        hovermode="x unified",
        legend=dict(orientation="h", y=-0.2, font=dict(size=8)),
    )


# ═════════════════════════════════════════════════════════════════════════════
# TAB 1: LIVE SYSTEM
# ═════════════════════════════════════════════════════════════════════════════

def build_live_tab(static: StaticData, live: LiveData) -> html.Div:
    return html.Div(
        [
            # Row 1: Status panel + generation stack
            dbc.Row(
                [
                    dbc.Col(_system_panel(static, live), width=3),
                    dbc.Col(_panel([_generation_stack(live)]), width=9),
                ],
                className="mb-2 g-2",
            ),
            # Row 2: Demand + price + IC flows
            dbc.Row(
                [
                    dbc.Col(_panel([_demand_chart(live)]), width=4),
                    dbc.Col(_panel([_price_chart(live)]), width=4),
                    dbc.Col(_panel([_ic_bars(live)]), width=4),
                ],
                className="mb-2 g-2",
            ),
            # Row 3: Capacity factors + regional mix
            dbc.Row(
                [
                    dbc.Col(_panel([_capacity_table(static, live)]), width=5),
                    dbc.Col(_panel([_regional_chart(live)]), width=7),
                ],
                className="g-2",
            ),
        ]
    )


def _system_panel(static: StaticData, live: LiveData) -> html.Div:
    """Compact text panel: system vitals."""
    gen = live.current_gen
    domestic_fuels = {k: v for k, v in gen.items() if not k.startswith("ic_")}

    def row(label, value, colour="#ddd"):
        return html.Div(
            [
                html.Span(label, style={"color": "#666", "width": "75px", "display": "inline-block"}),
                html.Span(value, style={"color": colour, "fontWeight": "600"}),
            ],
            style={"lineHeight": "20px"},
        )

    price_col = "#4CAF50" if live.price_gbp_mwh < 30 else "#FFC107" if live.price_gbp_mwh < 80 else "#FF5722"
    headroom = static.total_installed_mw - live.total_domestic_mw
    headroom_col = "#4CAF50" if headroom > 10000 else "#FFC107" if headroom > 5000 else "#FF5722"
    ci_col = "#4CAF50" if live.carbon_gco2 < 100 else "#FFC107" if live.carbon_gco2 < 200 else "#FF5722"

    items = [
        row("GEN", f"{live.total_domestic_mw:,.0f} MW", "#eee"),
        row("DEMAND", f"{live.demand_mw:,.0f} MW", "#90CAF9"),
        row("PRICE", f"£{live.price_gbp_mwh:.2f}/MWh", price_col),
        row("CARBON", f"{live.carbon_gco2:.0f} gCO₂/kWh", ci_col),
        row("IMPORTS", f"{live.total_import_mw:,.0f} MW", "#FF9800"),
        row("HEADROOM", f"{headroom:,.0f} MW", headroom_col),
        html.Hr(style={"borderColor": "#333", "margin": "6px 0"}),
    ]

    # Fuel bars
    items.append(_label("DISPATCH"))
    sorted_fuels = sorted(domestic_fuels.items(), key=lambda x: -x[1])
    for fuel, mw in sorted_fuels:
        if mw <= 0:
            continue
        colour = BMRS_COLOURS.get(fuel, "#999")
        pct = mw / live.total_domestic_mw * 100 if live.total_domestic_mw > 0 else 0
        items.append(
            html.Div(
                [
                    html.Span(
                        fuel.replace("_", " "),
                        style={"width": "65px", "display": "inline-block", "color": "#888", "textTransform": "uppercase"},
                    ),
                    html.Span(
                        style={
                            "display": "inline-block",
                            "width": f"{max(2, min(pct, 50))}%",
                            "height": "8px",
                            "backgroundColor": colour,
                            "verticalAlign": "middle",
                            "marginRight": "4px",
                            "borderRadius": "1px",
                        },
                    ),
                    html.Span(f"{mw:,.0f}", style={"color": "#ccc"}),
                    html.Span(f" {pct:.0f}%", style={"color": "#666", "marginLeft": "3px"}),
                ],
                style={"lineHeight": "16px", "fontSize": "10px"},
            )
        )

    return _panel(items, height="100%")


def _generation_stack(live: LiveData) -> dcc.Graph:
    gen = live.generation
    if gen.empty:
        return dcc.Graph(figure=go.Figure(), style={"height": "330px"})

    domestic = gen[~gen["fuel_type"].str.startswith("ic_")]
    pivot = domestic.pivot_table(index="timestamp", columns="fuel_type", values="generation_mw", aggfunc="sum").fillna(0)
    fuel_order = pivot.mean().sort_values(ascending=True).index.tolist()

    fig = go.Figure()
    for fuel in fuel_order:
        colour = BMRS_COLOURS.get(fuel, "#999")
        fig.add_trace(go.Scatter(
            x=pivot.index, y=pivot[fuel], name=fuel.replace("_", " "),
            stackgroup="gen", line=dict(width=0), fillcolor=colour, marker=dict(color=colour),
            hovertemplate=f"{fuel}: %{{y:,.0f}} MW<extra></extra>",
        ))

    fig.update_layout(**_chart_layout("GENERATION BY FUEL (24h)", 330))
    fig.update_yaxes(title_text="MW")
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _demand_chart(live: LiveData) -> dcc.Graph:
    d = live.demand
    if d.empty:
        return dcc.Graph(figure=go.Figure(), style={"height": "260px"})

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=d["timestamp"], y=d["demand_mw"], name="INDO",
        line=dict(color="#2196F3", width=1.5),
        hovertemplate="%{y:,.0f} MW<extra>Demand</extra>",
    ))
    fig.update_layout(**_chart_layout("SYSTEM DEMAND", 260))
    fig.update_yaxes(title_text="MW")
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _price_chart(live: LiveData) -> dcc.Graph:
    p = live.prices
    if p.empty:
        return dcc.Graph(figure=go.Figure(), style={"height": "260px"})

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=p["timestamp"], y=p["price_gbp_mwh"], name="Day-Ahead",
        line=dict(color="#FFC107", width=1.5),
        fill="tozeroy", fillcolor="rgba(255,193,7,0.08)",
        hovertemplate="£%{y:.2f}/MWh<extra></extra>",
    ))
    fig.add_hline(y=0, line_color="#555", line_width=1)
    fig.update_layout(**_chart_layout("DAY-AHEAD PRICE (£/MWh)", 260))
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _ic_bars(live: LiveData) -> dcc.Graph:
    ic_data = {k: v for k, v in live.current_gen.items() if k.startswith("ic_")}
    if not ic_data:
        return dcc.Graph(figure=go.Figure(), style={"height": "260px"})

    names = [k.replace("ic_", "").replace("_", " ").upper() for k in ic_data]
    vals = list(ic_data.values())
    colours = ["#4CAF50" if v > 0 else "#F44336" for v in vals]

    fig = go.Figure(go.Bar(
        y=names, x=vals, orientation="h", marker_color=colours,
        text=[f"{v:+,.0f}" for v in vals], textposition="auto",
        textfont=dict(size=9),
        hovertemplate="<b>%{y}</b> %{x:+,.0f} MW<extra></extra>",
    ))
    fig.add_vline(x=0, line_color="#555")
    fig.update_layout(**_chart_layout("INTERCONNECTORS (MW)", 260))
    fig.update_layout(showlegend=False)
    fig.update_xaxes(title_text="Import → | ← Export")
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _capacity_table(static: StaticData, live: LiveData) -> html.Div:
    gen = live.current_gen
    installed = static.fuel_capacity

    rows = []
    for bmrs_fuel, our_fuels in FUEL_INSTALLED_MAP.items():
        inst = sum(installed.get(f, 0) for f in our_fuels)
        actual = gen.get(bmrs_fuel, 0)
        cf = (actual / inst * 100) if inst > 0 and actual > 0 else 0
        colour = BMRS_COLOURS.get(bmrs_fuel, "#999")
        cf_col = "#4CAF50" if cf > 50 else "#FFC107" if cf > 20 else "#666" if cf == 0 else "#F44336"

        rows.append(
            html.Tr([
                html.Td(html.Span("■", style={"color": colour}), style={"width": "16px", "padding": "2px 4px"}),
                html.Td(bmrs_fuel.replace("_", " ").upper(), style={"padding": "2px 6px"}),
                html.Td(f"{inst / 1000:.1f}", style={"textAlign": "right", "color": "#888", "padding": "2px 6px"}),
                html.Td(f"{actual / 1000:.1f}" if actual > 0 else "—", style={"textAlign": "right", "padding": "2px 6px"}),
                html.Td(
                    html.Div(style={
                        "width": f"{min(cf, 100):.0f}%", "height": "6px",
                        "backgroundColor": cf_col, "borderRadius": "1px",
                    }),
                    style={"width": "50px", "backgroundColor": "#2a2a2a", "borderRadius": "1px", "padding": "5px 2px"},
                ),
                html.Td(f"{cf:.0f}%" if cf > 0 else "—", style={"textAlign": "right", "color": cf_col, "padding": "2px 6px"}),
            ], style={"lineHeight": "20px"})
        )

    return html.Div([
        _label("CAPACITY FACTORS"),
        html.Table(
            [
                html.Thead(html.Tr([
                    html.Th(""), html.Th("FUEL"),
                    html.Th("INST GW", style={"textAlign": "right"}),
                    html.Th("NOW GW", style={"textAlign": "right"}),
                    html.Th(""), html.Th("CF", style={"textAlign": "right"}),
                ], style={"color": "#555", "fontSize": "9px"})),
                html.Tbody(rows),
            ],
            style={"width": "100%", "borderCollapse": "collapse"},
        ),
    ])


def _regional_chart(live: LiveData) -> dcc.Graph:
    """Regional generation mix from Carbon Intensity API."""
    reg = live.ci_regional
    if reg.empty:
        return dcc.Graph(figure=go.Figure(), style={"height": "300px"})

    # Pivot: region × fuel → percentage
    pivot = reg.pivot_table(index="region", columns="fuel", values="percentage", aggfunc="sum").fillna(0)
    region_order = pivot.sum(axis=1).sort_values(ascending=True).index.tolist()

    ci_fuel_colours = {
        "wind": "#2196F3", "solar": "#FFC107", "nuclear": "#9C27B0",
        "gas": "#FF5722", "biomass": "#4CAF50", "hydro": "#00BCD4",
        "imports": "#FF9800", "coal": "#424242", "other": "#9E9E9E",
    }

    fig = go.Figure()
    for fuel in ["nuclear", "wind", "solar", "biomass", "hydro", "gas", "imports", "coal", "other"]:
        if fuel not in pivot.columns:
            continue
        fig.add_trace(go.Bar(
            y=pivot.index, x=pivot[fuel], name=fuel.title(),
            orientation="h", marker_color=ci_fuel_colours.get(fuel, "#999"),
            hovertemplate=f"{fuel}: %{{x:.1f}}%<extra></extra>",
        ))

    fig.update_layout(**_chart_layout("REGIONAL GENERATION MIX (%)", 300))
    fig.update_layout(barmode="stack", yaxis=dict(categoryorder="array", categoryarray=region_order))
    fig.update_xaxes(title_text="%", range=[0, 100])
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


# ═════════════════════════════════════════════════════════════════════════════
# TAB 2: ASSET MAP
# ═════════════════════════════════════════════════════════════════════════════

def build_map_tab(static: StaticData) -> html.Div:
    """Interactive Mapbox scatter of all operational plants."""
    import plotly.express as px

    op = static.operational.copy()
    valid = op[op["lat"].notna() & op["lon"].notna()].copy()
    valid["fuel_label"] = valid["fuel_type"].map(FUEL_LABELS).fillna(valid["fuel_type"])
    valid["cap_str"] = valid["capacity_mw"].apply(lambda x: f"{x:,.0f} MW" if pd.notna(x) else "")
    valid["size"] = valid["capacity_mw"].clip(lower=1).apply(lambda x: max(3, min(30, x ** 0.35)))

    fig = px.scatter_mapbox(
        valid,
        lat="lat", lon="lon",
        color="fuel_label",
        size="size",
        hover_name="name",
        hover_data={"cap_str": True, "dno_region": True, "fuel_label": True, "lat": False, "lon": False, "size": False},
        color_discrete_map={FUEL_LABELS.get(f, f): c for f, c in FUEL_COLOURS.items()},
        zoom=5.3,
        center={"lat": 54.5, "lon": -2},
        mapbox_style="carto-darkmatter",
    )
    fig.update_layout(
        margin=dict(t=0, b=0, l=0, r=0),
        paper_bgcolor="#1a1a1a",
        font=dict(color="#ccc", size=9, family="'JetBrains Mono', monospace"),
        height=750,
        legend=dict(
            title="", font=dict(size=9), bgcolor="rgba(0,0,0,0.7)",
            orientation="v", x=0.01, y=0.99,
        ),
    )

    summary = (
        f"{len(valid):,} operational plants · "
        f"{valid['capacity_mw'].sum() / 1000:,.1f} GW installed · "
        f"DUKES-verified"
    )

    return html.Div([
        html.Div(summary, style={"color": "#666", "fontSize": "10px", "marginBottom": "4px"}),
        dcc.Graph(figure=fig, config={"displayModeBar": False}, style={"height": "750px"}),
    ])


# ═════════════════════════════════════════════════════════════════════════════
# TAB 3: DATA SOURCES
# ═════════════════════════════════════════════════════════════════════════════

def build_sources_tab(static: StaticData) -> html.Div:
    """Data source inventory — what we have, how fresh it is."""
    rows = []
    for src in static.sources:
        status = src["status"]
        status_colour = {"fresh": "#4CAF50", "stale": "#FFC107", "old": "#FF5722", "missing": "#F44336"}.get(status, "#999")
        age_str = f"{src['age_days']}d" if src["age_days"] >= 0 else "—"

        rows.append(html.Tr([
            html.Td(html.Span("●", style={"color": status_colour, "marginRight": "6px"})),
            html.Td(src["name"], style={"fontWeight": "500"}),
            html.Td(src["path"], style={"color": "#888"}),
            html.Td(src["rows"], style={"textAlign": "right"}),
            html.Td(f"{src['size_mb']:.1f} MB" if src["size_mb"] > 0 else "—", style={"textAlign": "right", "color": "#888"}),
            html.Td(age_str, style={"textAlign": "right", "color": status_colour}),
            html.Td(status.upper(), style={"color": status_colour, "fontSize": "9px", "textAlign": "right"}),
        ]))

    # Live feeds
    live_feeds = [
        {"name": "BMRS Generation", "endpoint": "/generation/outturn/summary", "freq": "30min", "key": "No"},
        {"name": "BMRS Demand", "endpoint": "/demand/outturn", "freq": "30min", "key": "No"},
        {"name": "BMRS Prices", "endpoint": "/balancing/pricing/market-index", "freq": "30min", "key": "No"},
        {"name": "BMRS Interconnectors", "endpoint": "/generation/outturn/interconnectors", "freq": "30min", "key": "No"},
        {"name": "Carbon Intensity", "endpoint": "api.carbonintensity.org.uk", "freq": "30min", "key": "No"},
        {"name": "CI Regional", "endpoint": "/regional", "freq": "30min", "key": "No"},
    ]
    live_rows = []
    for feed in live_feeds:
        live_rows.append(html.Tr([
            html.Td(html.Span("●", style={"color": "#4CAF50", "marginRight": "6px"})),
            html.Td(feed["name"], style={"fontWeight": "500"}),
            html.Td(feed["endpoint"], style={"color": "#888"}),
            html.Td(feed["freq"], style={"textAlign": "right"}),
            html.Td(feed["key"], style={"textAlign": "right", "color": "#4CAF50"}),
            html.Td("LIVE", style={"color": "#4CAF50", "fontSize": "9px", "textAlign": "right"}),
        ]))

    return html.Div([
        _panel([
            _label("STATIC DATA FILES"),
            html.Table(
                [
                    html.Thead(html.Tr([
                        html.Th(""), html.Th("SOURCE"), html.Th("FILE"),
                        html.Th("ROWS", style={"textAlign": "right"}),
                        html.Th("SIZE", style={"textAlign": "right"}),
                        html.Th("AGE", style={"textAlign": "right"}),
                        html.Th("", style={"textAlign": "right"}),
                    ], style={"color": "#555", "fontSize": "9px"})),
                    html.Tbody(rows),
                ],
                style={"width": "100%", "borderCollapse": "collapse"},
            ),
        ]),
        html.Div(style={"height": "12px"}),
        _panel([
            _label("LIVE FEEDS"),
            html.Table(
                [
                    html.Thead(html.Tr([
                        html.Th(""), html.Th("SOURCE"), html.Th("ENDPOINT"),
                        html.Th("FREQ", style={"textAlign": "right"}),
                        html.Th("KEY", style={"textAlign": "right"}),
                        html.Th("", style={"textAlign": "right"}),
                    ], style={"color": "#555", "fontSize": "9px"})),
                    html.Tbody(live_rows),
                ],
                style={"width": "100%", "borderCollapse": "collapse"},
            ),
        ]),
        html.Div(style={"height": "12px"}),
        _panel([
            _label("PIPELINE ARCHITECTURE"),
            html.Pre(
                "STATIC SOURCES                     LIVE FEEDS\n"
                "─────────────                      ──────────\n"
                "DUKES (DESNZ) ──┐                  BMRS Insights API ──┐\n"
                "WRI Global    ──┤                    /generation        │\n"
                "REPD          ──┼─→ Reconciler ──→   /demand          ─┼─→ Dashboard\n"
                "OSUKED        ──┤    (plant_       /prices             │\n"
                "BMRS BM Units ──┤     matcher)     /interconnectors   ─┘\n"
                "OSM           ──┘       │\n"
                "                        ↓          Carbon Intensity API\n"
                "                   plants_unified     /generation ──────→\n"
                "                   .parquet           /regional ────────→\n"
                "                        │             /intensity ───────→\n"
                "                        ↓\n"
                "                   NetworkX Graph\n"
                "                   (29k nodes)\n",
                style={"color": "#888", "fontSize": "10px", "lineHeight": "16px", "margin": 0},
            ),
        ]),
    ])
