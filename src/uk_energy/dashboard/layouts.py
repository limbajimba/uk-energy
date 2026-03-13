"""
layouts.py — Dashboard tab layouts.

Three tabs:
  1. Live System: generation stack, demand, prices, utilisation, IC flows, regional
  2. Asset Map: Plotly Mapbox scatter of all plants
  3. Data Sources: ingestion inventory, freshness, pipeline diagram
"""

from __future__ import annotations

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import dcc, html
import pandas as pd
import numpy as np

from uk_energy.dashboard.data import StaticData, LiveData
from uk_energy.dashboard.theme import FUEL_COLOURS, FUEL_LABELS

# ─── Constants ───────────────────────────────────────────────────────────────

# BMRS fuel colours
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
    "other": "#616161",
}

# BMRS fuel → installed capacity fuels (for utilisation comparison)
FUEL_INSTALLED_MAP = {
    "wind": ["wind_onshore", "wind_offshore"],
    "solar": ["solar_pv"],
    "nuclear": ["nuclear"],
    "gas_ccgt": ["gas_ccgt"],
    "biomass": ["biomass"],
    "hydro": ["hydro_run_of_river"],
    "pumped_storage": ["hydro_pumped_storage"],
    "oil": ["oil"],
}

# Proper interconnector names (BMRS code → engineering name)
IC_NAMES = {
    "ic_france": "IFA",
    "ic_ifa2": "IFA2",
    "ic_eleclink": "ElecLink",
    "ic_nemo": "Nemo Link",
    "ic_netherlands": "BritNed",
    "ic_nsl": "NSL",
    "ic_viking": "Viking Link",
    "ic_ewic": "EWIC",
    "ic_ireland": "Moyle",
    "ic_greenlink": "Greenlink",
}

# All 10 IC codes (to ensure we show zeroes for idle links)
ALL_ICS = list(IC_NAMES.keys())

# Dispatchable fuel types (for margin calculation)
DISPATCHABLE = {"gas_ccgt", "gas_ocgt", "nuclear", "biomass", "oil", "hydro_pumped_storage", "hydro_run_of_river"}


def _panel(children, **style_overrides) -> html.Div:
    style = {
        "backgroundColor": "#1a1a1a",
        "border": "1px solid #333",
        "borderRadius": "3px",
        "padding": "8px",
        **style_overrides,
    }
    return html.Div(children, style=style)


def _label(text: str) -> html.Div:
    return html.Div(text, style={"color": "#555", "fontSize": "10px", "fontWeight": "600", "marginBottom": "4px"})


def _chart_layout(title: str = "", height: int = 280) -> dict:
    return dict(
        title=dict(text=title, font=dict(size=10, color="#555"), x=0, y=0.98) if title else None,
        margin=dict(t=25 if title else 10, b=25, l=45, r=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#1a1a1a",
        font=dict(color="#999", size=9, family="'JetBrains Mono', monospace"),
        height=height,
        xaxis=dict(gridcolor="#262626", zeroline=False),
        yaxis=dict(gridcolor="#262626", zeroline=False),
        hovermode="x unified",
        legend=dict(orientation="h", y=-0.2, font=dict(size=8)),
    )


# ═════════════════════════════════════════════════════════════════════════════
# TAB 1: LIVE SYSTEM
# ═════════════════════════════════════════════════════════════════════════════

def build_live_tab(static: StaticData, live: LiveData) -> html.Div:
    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(_system_panel(static, live), width=3),
                    dbc.Col(_panel([_generation_stack(live)]), width=9),
                ],
                className="mb-2 g-2",
            ),
            dbc.Row(
                [
                    dbc.Col(_panel([_demand_chart(live)]), width=4),
                    dbc.Col(_panel([_price_chart(live)]), width=4),
                    dbc.Col(_panel([_ic_bars(live)]), width=4),
                ],
                className="mb-2 g-2",
            ),
            dbc.Row(
                [
                    dbc.Col(_panel([_utilisation_table(static, live)]), width=5),
                    dbc.Col(_panel([_regional_chart(live)]), width=7),
                ],
                className="g-2",
            ),
        ]
    )


def _system_panel(static: StaticData, live: LiveData) -> html.Div:
    gen = live.current_gen
    domestic = {k: v for k, v in gen.items() if not k.startswith("ic_")}

    # Dispatchable margin = dispatchable installed capacity - current demand
    disp_installed = sum(
        static.fuel_capacity.get(f, 0)
        for f in DISPATCHABLE
    )
    margin = disp_installed - live.demand_mw

    def row(label, value, colour="#ddd", note=""):
        items = [
            html.Span(label, style={"color": "#555", "width": "65px", "display": "inline-block"}),
            html.Span(value, style={"color": colour, "fontWeight": "600"}),
        ]
        if note:
            items.append(html.Span(f" {note}", style={"color": "#444", "fontSize": "9px"}))
        return html.Div(items, style={"lineHeight": "20px"})

    price_col = "#4CAF50" if live.price_gbp_mwh < 30 else "#FFC107" if live.price_gbp_mwh < 80 else "#FF5722"
    margin_col = "#4CAF50" if margin > 10000 else "#FFC107" if margin > 5000 else "#FF5722"
    ci_col = "#4CAF50" if live.carbon_gco2 < 100 else "#FFC107" if live.carbon_gco2 < 200 else "#FF5722"

    items = [
        row("GEN", f"{live.total_domestic_mw:,.0f} MW", "#eee", "(transmission-metered)"),
        row("DEMAND", f"{live.demand_mw:,.0f} MW", "#90CAF9"),
        row("PRICE", f"£{live.price_gbp_mwh:.2f}/MWh", price_col),
        row("CO₂", f"{live.carbon_gco2:.0f} gCO₂/kWh", ci_col, f"({live.carbon_intensity.get('index', '')})"),
        row("IMPORTS", f"{live.total_import_mw:,.0f} MW", "#FF9800"),
        row("MARGIN", f"{margin:,.0f} MW", margin_col, "(dispatchable - demand)"),
        html.Hr(style={"borderColor": "#2a2a2a", "margin": "5px 0"}),
    ]

    # Dispatch breakdown — only fuels with >0 MW
    items.append(_label("DISPATCH"))
    sorted_fuels = sorted(
        [(k, v) for k, v in domestic.items() if v > 0],
        key=lambda x: -x[1],
    )
    for fuel, mw in sorted_fuels:
        colour = BMRS_COLOURS.get(fuel, "#999")
        pct = mw / live.total_domestic_mw * 100 if live.total_domestic_mw > 0 else 0
        items.append(
            html.Div(
                [
                    html.Span(
                        fuel.replace("_", " "),
                        style={"width": "60px", "display": "inline-block", "color": "#777", "textTransform": "uppercase"},
                    ),
                    html.Span(
                        style={
                            "display": "inline-block",
                            "width": f"{max(2, min(pct * 0.8, 40))}%",
                            "height": "7px",
                            "backgroundColor": colour,
                            "verticalAlign": "middle",
                            "marginRight": "4px",
                            "borderRadius": "1px",
                        },
                    ),
                    html.Span(f"{mw:,.0f}", style={"color": "#ccc"}),
                    html.Span(f" {pct:.0f}%", style={"color": "#555", "marginLeft": "2px"}),
                ],
                style={"lineHeight": "15px", "fontSize": "10px"},
            )
        )

    # Note about OTHER
    other_mw = domestic.get("other", 0)
    if other_mw > 0:
        items.append(
            html.Div(
                f"'other' includes pumped storage, embedded solar/CHP",
                style={"color": "#444", "fontSize": "8px", "marginTop": "4px", "fontStyle": "italic"},
            )
        )

    return _panel(items, height="100%")


def _generation_stack(live: LiveData) -> dcc.Graph:
    gen = live.generation
    if gen.empty:
        return dcc.Graph(figure=go.Figure(), style={"height": "330px"})

    # Only domestic generation, exclude fuels with all-zero values
    domestic = gen[~gen["fuel_type"].str.startswith("ic_")]
    pivot = domestic.pivot_table(index="timestamp", columns="fuel_type", values="generation_mw", aggfunc="sum").fillna(0)

    # Remove fuels that are always 0
    nonzero = pivot.columns[pivot.sum() > 0].tolist()
    pivot = pivot[nonzero]
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

    # Only show last 7 days to match generation window better
    cutoff = d["timestamp"].max() - pd.Timedelta(days=7)
    d = d[d["timestamp"] >= cutoff]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=d["timestamp"], y=d["demand_mw"], name="INDO",
        line=dict(color="#2196F3", width=1),
        hovertemplate="%{y:,.0f} MW<extra>Demand</extra>",
    ))
    fig.update_layout(**_chart_layout("SYSTEM DEMAND (7d)", 260))
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
    fig.add_hline(y=0, line_color="#444", line_width=1)
    fig.update_layout(**_chart_layout("DAY-AHEAD PRICE (£/MWh)", 260))
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _ic_bars(live: LiveData) -> dcc.Graph:
    """All 10 interconnectors, including zeroes for idle links."""
    gen = live.current_gen

    # Build full list of all 10 ICs
    names = []
    vals = []
    for ic_key in ALL_ICS:
        names.append(IC_NAMES[ic_key])
        vals.append(gen.get(ic_key, 0))

    # Sort by absolute value (most active at top)
    pairs = sorted(zip(names, vals), key=lambda x: abs(x[1]), reverse=True)
    names = [p[0] for p in pairs]
    vals = [p[1] for p in pairs]

    colours = []
    for v in vals:
        if v > 0:
            colours.append("#4CAF50")
        elif v < 0:
            colours.append("#F44336")
        else:
            colours.append("#333")

    fig = go.Figure(go.Bar(
        y=names, x=vals, orientation="h", marker_color=colours,
        text=[f"{v:+,.0f}" if v != 0 else "0" for v in vals],
        textposition="auto",
        textfont=dict(size=8, color=["#ccc" if v != 0 else "#555" for v in vals]),
        hovertemplate="<b>%{y}</b> %{x:+,.0f} MW<extra></extra>",
    ))
    fig.add_vline(x=0, line_color="#444")
    fig.update_layout(**_chart_layout("INTERCONNECTORS — ALL 10 (MW)", 260))
    fig.update_layout(showlegend=False, yaxis=dict(autorange="reversed"))
    fig.update_xaxes(title_text="← Export | Import →")
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _utilisation_table(static: StaticData, live: LiveData) -> html.Div:
    """Installed capacity vs current output. NOT called 'capacity factor'."""
    gen = live.current_gen
    installed = static.fuel_capacity

    rows = []
    for bmrs_fuel, our_fuels in FUEL_INSTALLED_MAP.items():
        inst = sum(installed.get(f, 0) for f in our_fuels)
        actual = gen.get(bmrs_fuel, 0)
        util = (actual / inst * 100) if inst > 0 else 0
        colour = BMRS_COLOURS.get(bmrs_fuel, "#999")

        if actual > 0:
            util_col = "#4CAF50" if util > 50 else "#FFC107" if util > 20 else "#F44336"
            now_str = f"{actual / 1000:.1f}"
            util_str = f"{util:.0f}%"
        else:
            util_col = "#444"
            now_str = "0.0"
            util_str = "0%"

        rows.append(
            html.Tr([
                html.Td(html.Span("■", style={"color": colour}), style={"width": "14px", "padding": "2px 3px"}),
                html.Td(bmrs_fuel.replace("_", " ").upper(), style={"padding": "2px 4px"}),
                html.Td(f"{inst / 1000:.1f}", style={"textAlign": "right", "color": "#666", "padding": "2px 4px"}),
                html.Td(now_str, style={"textAlign": "right", "padding": "2px 4px"}),
                html.Td(
                    html.Div(style={
                        "width": f"{min(util, 100):.0f}%", "height": "5px",
                        "backgroundColor": util_col, "borderRadius": "1px",
                    }),
                    style={"width": "50px", "backgroundColor": "#262626", "borderRadius": "1px", "padding": "6px 2px"},
                ),
                html.Td(util_str, style={"textAlign": "right", "color": util_col, "padding": "2px 4px"}),
            ], style={"lineHeight": "18px"})
        )

    return html.Div([
        _label("CURRENT UTILISATION (installed vs output)"),
        html.Table(
            [
                html.Thead(html.Tr([
                    html.Th("", style={"width": "14px"}), html.Th("FUEL"),
                    html.Th("INST GW", style={"textAlign": "right"}),
                    html.Th("NOW GW", style={"textAlign": "right"}),
                    html.Th(""), html.Th("UTIL", style={"textAlign": "right"}),
                ], style={"color": "#444", "fontSize": "9px"})),
                html.Tbody(rows),
            ],
            style={"width": "100%", "borderCollapse": "collapse"},
        ),
        html.Div(
            "Instantaneous utilisation, not capacity factor. "
            "CF requires energy/time integration. "
            "BMRS 'other' (not shown) includes PS gen + embedded solar/CHP.",
            style={"color": "#3a3a3a", "fontSize": "8px", "marginTop": "6px", "lineHeight": "12px"},
        ),
    ])


def _regional_chart(live: LiveData) -> dcc.Graph:
    reg = live.ci_regional
    if reg.empty:
        return dcc.Graph(figure=go.Figure(), style={"height": "300px"})

    pivot = reg.pivot_table(index="region", columns="fuel", values="percentage", aggfunc="sum").fillna(0)

    # Sort regions by wind % (most renewable at top)
    wind_col = "wind" if "wind" in pivot.columns else pivot.columns[0]
    region_order = pivot[wind_col].sort_values(ascending=True).index.tolist()

    ci_fuel_colours = {
        "nuclear": "#9C27B0", "wind": "#2196F3", "solar": "#FFC107",
        "biomass": "#4CAF50", "hydro": "#00BCD4", "gas": "#FF5722",
        "imports": "#FF9800", "coal": "#424242", "other": "#616161",
    }
    fuel_order = ["nuclear", "wind", "solar", "biomass", "hydro", "gas", "imports", "coal", "other"]

    fig = go.Figure()
    for fuel in fuel_order:
        if fuel not in pivot.columns:
            continue
        fig.add_trace(go.Bar(
            y=pivot.index, x=pivot[fuel], name=fuel.title(),
            orientation="h", marker_color=ci_fuel_colours.get(fuel, "#999"),
            hovertemplate=f"{fuel}: %{{x:.1f}}%<extra></extra>",
        ))

    fig.update_layout(**_chart_layout("REGIONAL GENERATION MIX — Carbon Intensity API (%)", 300))
    fig.update_layout(barmode="stack", yaxis=dict(categoryorder="array", categoryarray=region_order))
    fig.update_xaxes(title_text="%", range=[0, 100])
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


# ═════════════════════════════════════════════════════════════════════════════
# TAB 2: ASSET MAP
# ═════════════════════════════════════════════════════════════════════════════

def build_map_tab(static: StaticData) -> html.Div:
    import plotly.express as px

    op = static.operational.copy()
    valid = op[op["lat"].notna() & op["lon"].notna()].copy()
    valid["fuel_label"] = valid["fuel_type"].map(FUEL_LABELS).fillna(valid["fuel_type"])
    valid["cap_str"] = valid["capacity_mw"].apply(lambda x: f"{x:,.0f} MW" if pd.notna(x) else "")
    valid["size"] = valid["capacity_mw"].clip(lower=1).apply(lambda x: max(3, min(30, x ** 0.35)))

    fig = px.scatter_mapbox(
        valid, lat="lat", lon="lon",
        color="fuel_label", size="size",
        hover_name="name",
        hover_data={"cap_str": True, "dno_region": True, "fuel_label": True, "lat": False, "lon": False, "size": False},
        color_discrete_map={FUEL_LABELS.get(f, f): c for f, c in FUEL_COLOURS.items()},
        zoom=5.3, center={"lat": 54.5, "lon": -2},
        mapbox_style="carto-darkmatter",
    )
    fig.update_layout(
        margin=dict(t=0, b=0, l=0, r=0),
        paper_bgcolor="#1a1a1a",
        font=dict(color="#ccc", size=9, family="'JetBrains Mono', monospace"),
        height=750,
        legend=dict(title="", font=dict(size=9), bgcolor="rgba(0,0,0,0.7)", orientation="v", x=0.01, y=0.99),
    )

    return html.Div([
        html.Div(
            f"{len(valid):,} DUKES-verified operational plants · "
            f"{valid['capacity_mw'].sum() / 1000:,.1f} GW installed · "
            f"Source: DESNZ DUKES Table 5.11",
            style={"color": "#555", "fontSize": "10px", "marginBottom": "4px"},
        ),
        dcc.Graph(figure=fig, config={"displayModeBar": False}, style={"height": "750px"}),
    ])


# ═════════════════════════════════════════════════════════════════════════════
# TAB 3: DATA SOURCES
# ═════════════════════════════════════════════════════════════════════════════

def build_sources_tab(static: StaticData) -> html.Div:
    rows = []
    for src in static.sources:
        status = src["status"]
        status_col = {"fresh": "#4CAF50", "stale": "#FFC107", "old": "#FF5722", "missing": "#F44336"}.get(status, "#999")
        age_str = f"{src['age_days']}d" if src["age_days"] >= 0 else "—"
        rows.append(html.Tr([
            html.Td(html.Span("●", style={"color": status_col, "marginRight": "4px"})),
            html.Td(src["name"], style={"fontWeight": "500"}),
            html.Td(src["path"], style={"color": "#666"}),
            html.Td(src["rows"], style={"textAlign": "right"}),
            html.Td(f"{src['size_mb']:.1f} MB" if src["size_mb"] > 0 else "—", style={"textAlign": "right", "color": "#666"}),
            html.Td(age_str, style={"textAlign": "right", "color": status_col}),
            html.Td(status.upper(), style={"color": status_col, "fontSize": "9px", "textAlign": "right"}),
        ]))

    live_feeds = [
        ("BMRS Generation", "/generation/outturn/summary", "30min", "No", "Transmission-metered generation by fuel. No solar, no PS breakdown."),
        ("BMRS Demand", "/demand/outturn", "30min", "No", "INDO (national) + ITSDO (transmission system)."),
        ("BMRS Prices", "/balancing/pricing/market-index", "30min", "No", "Day-ahead EPEX/N2EX market index."),
        ("BMRS Interconnectors", "/generation/outturn/interconnectors", "30min", "No", "Per-IC half-hourly flows."),
        ("Carbon Intensity", "carbonintensity.org.uk", "30min", "No", "Solar %, regional mix, gCO₂/kWh. Percentage-based (no MW)."),
    ]
    live_rows = []
    for name, ep, freq, key, note in live_feeds:
        live_rows.append(html.Tr([
            html.Td(html.Span("●", style={"color": "#4CAF50", "marginRight": "4px"})),
            html.Td(name, style={"fontWeight": "500"}),
            html.Td(ep, style={"color": "#666"}),
            html.Td(freq, style={"textAlign": "right"}),
            html.Td("LIVE", style={"color": "#4CAF50", "fontSize": "9px", "textAlign": "right"}),
        ]))

    return html.Div([
        _panel([
            _label("STATIC DATA FILES"),
            html.Table([
                html.Thead(html.Tr([
                    html.Th(""), html.Th("SOURCE"), html.Th("FILE"),
                    html.Th("ROWS", style={"textAlign": "right"}),
                    html.Th("SIZE", style={"textAlign": "right"}),
                    html.Th("AGE", style={"textAlign": "right"}),
                    html.Th("", style={"textAlign": "right"}),
                ], style={"color": "#444", "fontSize": "9px"})),
                html.Tbody(rows),
            ], style={"width": "100%", "borderCollapse": "collapse"}),
        ]),
        html.Div(style={"height": "8px"}),
        _panel([
            _label("LIVE API FEEDS"),
            html.Table([
                html.Thead(html.Tr([
                    html.Th(""), html.Th("SOURCE"), html.Th("ENDPOINT"),
                    html.Th("FREQ", style={"textAlign": "right"}),
                    html.Th("", style={"textAlign": "right"}),
                ], style={"color": "#444", "fontSize": "9px"})),
                html.Tbody(live_rows),
            ], style={"width": "100%", "borderCollapse": "collapse"}),
        ]),
        html.Div(style={"height": "8px"}),
        _panel([
            _label("DATA LIMITATIONS"),
            html.Pre(
                "• BMRS generation summary is transmission-metered only.\n"
                "  Solar PV is mostly distribution-connected → invisible to BMRS.\n"
                "  Solar MW estimated from Carbon Intensity API percentage × total gen.\n"
                "\n"
                "• BMRS 'OTHER' (~900 MW) = pumped storage generation + embedded CHP\n"
                "  + embedded wind/solar + other non-transmission-metered generation.\n"
                "  Cannot decompose without per-unit BM data (B1610 endpoint).\n"
                "\n"
                "• BMRS reports all wind as 'WIND' — no onshore/offshore split.\n"
                "  Installed capacity split uses Crown Estate lease name matching.\n"
                "\n"
                "• DUKES classifies all gas as 'Natural Gas' — no CCGT/OCGT split.\n"
                "  BMRS does split CCGT/OCGT but installed figures don't align.\n"
                "\n"
                "• Carbon Intensity API provides percentages, not MW.\n"
                "  Regional mix is directionally correct but not metered.\n"
                "\n"
                "• Interconnector flows: BMRS omits ICs at 0 MW from response.\n"
                "  Dashboard shows all 10 with explicit zeroes.\n",
                style={"color": "#666", "fontSize": "9px", "lineHeight": "14px", "margin": 0, "whiteSpace": "pre-wrap"},
            ),
        ]),
    ])
