"""
layouts.py — Dashboard tab layouts.

Five tabs: Live | Prices & Balancing | Forecasts | Asset Map | Data
"""

from __future__ import annotations

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import dcc, html
import pandas as pd
import numpy as np

from uk_energy.dashboard.data import StaticData, LiveData, HistoricalData
from uk_energy.dashboard.theme import FUEL_COLOURS, FUEL_LABELS
from uk_energy.timeseries.bmrs_live import IC_CAPACITY

# ─── Constants ───────────────────────────────────────────────────────────────

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

# BMRS fuel → installed capacity fuel types
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

# Dispatchable fuels (for margin calculation)
DISPATCHABLE = {"gas_ccgt", "gas_ocgt", "nuclear", "biomass", "oil", "hydro_pumped_storage", "hydro_run_of_river"}

# All 10 IC names (ensures we show all even when 0)
ALL_ICS = list(IC_CAPACITY.keys())


def _panel(children, **overrides) -> html.Div:
    style = {"backgroundColor": "#1a1a1a", "border": "1px solid #333", "borderRadius": "3px", "padding": "8px", **overrides}
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
    return html.Div([
        dbc.Row([
            dbc.Col(_system_panel(static, live), width=3),
            dbc.Col(_panel([_generation_stack(live)]), width=9),
        ], className="mb-2 g-2"),
        dbc.Row([
            dbc.Col(_panel([_demand_chart(live)]), width=4),
            dbc.Col(_panel([_price_chart(live)]), width=4),
            dbc.Col(_panel([_ic_chart(live)]), width=4),
        ], className="mb-2 g-2"),
        dbc.Row([
            dbc.Col(_panel([_utilisation_table(static, live)]), width=5),
            dbc.Col(_panel([_regional_chart(live)]), width=7),
        ], className="g-2"),
    ])


def _system_panel(static: StaticData, live: LiveData) -> html.Div:
    """Left-side summary panel with system vitals and dispatch breakdown."""
    disp_installed = sum(static.fuel_capacity.get(f, 0) for f in DISPATCHABLE)
    margin = disp_installed - live.demand_mw

    def row(label, value, colour="#ddd", note=""):
        items = [
            html.Span(label, style={"color": "#555", "width": "60px", "display": "inline-block", "fontSize": "10px"}),
            html.Span(value, style={"color": colour, "fontWeight": "600", "fontSize": "11px"}),
        ]
        if note:
            items.append(html.Span(f" {note}", style={"color": "#3a3a3a", "fontSize": "8px"}))
        return html.Div(items, style={"lineHeight": "18px"})

    price_col = "#4CAF50" if live.ssp_gbp_mwh < 30 else "#FFC107" if live.ssp_gbp_mwh < 80 else "#FF5722"
    margin_col = "#4CAF50" if margin > 10000 else "#FFC107" if margin > 5000 else "#FF5722"
    ci_col = "#4CAF50" if live.carbon_gco2 < 100 else "#FFC107" if live.carbon_gco2 < 200 else "#FF5722"

    # NIV direction
    niv_label = f"NIV {live.niv_mw:+,.0f} MW" if live.niv_mw != 0 else "NIV 0 MW"
    niv_note = "(system long)" if live.niv_mw > 0 else "(system short)" if live.niv_mw < 0 else "(balanced)"

    items = [
        row("GEN", f"{live.total_domestic_mw:,.0f} MW", "#eee", "(transmission-metered)"),
        row("DEMAND", f"{live.demand_mw:,.0f} MW", "#90CAF9", "(INDO)"),
        row("SSP", f"£{live.ssp_gbp_mwh:.2f}/MWh", price_col, "(imbalance settlement)"),
        row("CO₂", f"{live.carbon_gco2:.0f} gCO₂/kWh", ci_col, f"({live.carbon_intensity.get('index', '')})"),
        row("NET IC", f"{live.net_ic_mw:+,.0f} MW", "#FF9800",
            f"(↑{live.total_import_mw:,.0f} ↓{live.total_export_mw:,.0f})"),
        row("MARGIN", f"{margin:,.0f} MW", margin_col, "(dispatchable − demand)"),
        row("NIV", niv_label.split("NIV ")[1], "#888", niv_note),
        html.Hr(style={"borderColor": "#2a2a2a", "margin": "4px 0"}),
    ]

    # Dispatch breakdown — domestic fuels with >0 only
    items.append(_label("DISPATCH"))
    sorted_fuels = sorted(
        [(k, v) for k, v in live.current_gen.items() if v > 0],
        key=lambda x: -x[1],
    )
    for fuel, mw in sorted_fuels:
        colour = BMRS_COLOURS.get(fuel, "#999")
        pct = mw / live.total_domestic_mw * 100 if live.total_domestic_mw > 0 else 0
        items.append(html.Div([
            html.Span(
                fuel.replace("_", " "),
                style={"width": "55px", "display": "inline-block", "color": "#777", "textTransform": "uppercase", "fontSize": "9px"},
            ),
            html.Span(style={
                "display": "inline-block", "width": f"{max(2, min(pct * 0.8, 40))}%",
                "height": "6px", "backgroundColor": colour, "verticalAlign": "middle",
                "marginRight": "3px", "borderRadius": "1px",
            }),
            html.Span(f"{mw:,.0f}", style={"color": "#ccc", "fontSize": "10px"}),
            html.Span(f" {pct:.0f}%", style={"color": "#555", "marginLeft": "2px", "fontSize": "9px"}),
        ], style={"lineHeight": "14px"}))

    # Caveats
    other_mw = live.current_gen.get("other", 0)
    notes = []
    if other_mw > 0:
        notes.append(f"'other' {other_mw:,.0f} MW = PS gen + embedded solar/CHP")
    solar_mw = live.current_gen.get("solar", 0)
    if solar_mw > 0:
        notes.append(f"solar {solar_mw:,.0f} MW estimated from CI API ({live.ci_mix.get('solar', 0):.1f}%)")
    elif solar_mw == 0 and live.ci_mix.get("solar", 0) == 0:
        notes.append("solar 0 MW (night / CI API reports 0%)")

    for note in notes:
        items.append(html.Div(note, style={"color": "#3a3a3a", "fontSize": "8px", "marginTop": "2px", "fontStyle": "italic"}))

    return _panel(items, height="100%")


def _generation_stack(live: LiveData) -> dcc.Graph:
    """Stacked area: domestic generation by fuel over 24h."""
    gen = live.generation
    if gen.empty:
        return dcc.Graph(figure=go.Figure(), style={"height": "330px"})

    pivot = gen.pivot_table(index="timestamp", columns="fuel_type", values="generation_mw", aggfunc="sum").fillna(0)

    # Only fuels with meaningful generation (>10 MW average avoids noise)
    nonzero = pivot.columns[pivot.mean() > 10].tolist()
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

    fig.update_layout(**_chart_layout("GENERATION BY FUEL — transmission-metered (24h)", 330))
    fig.update_yaxes(title_text="MW")
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _demand_chart(live: LiveData) -> dcc.Graph:
    """INDO demand, 7 days."""
    d = live.demand
    if d.empty:
        return dcc.Graph(figure=go.Figure(), style={"height": "260px"})

    cutoff = d["timestamp"].max() - pd.Timedelta(days=7)
    d = d[d["timestamp"] >= cutoff]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=d["timestamp"], y=d["demand_mw"], name="INDO",
        line=dict(color="#2196F3", width=1),
        hovertemplate="%{y:,.0f} MW<extra>INDO</extra>",
    ))
    # Also show ITSDO if available
    if "transmission_demand_mw" in d.columns:
        fig.add_trace(go.Scatter(
            x=d["timestamp"], y=d["transmission_demand_mw"], name="ITSDO",
            line=dict(color="#2196F3", width=0.5, dash="dot"),
            opacity=0.4,
            hovertemplate="%{y:,.0f} MW<extra>ITSDO</extra>",
        ))

    fig.update_layout(**_chart_layout("SYSTEM DEMAND — INDO + ITSDO (7d)", 260))
    fig.update_yaxes(title_text="MW")
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _price_chart(live: LiveData) -> dcc.Graph:
    """System Sell Price (SSP) — imbalance settlement price."""
    p = live.prices
    if p.empty:
        return dcc.Graph(
            figure=go.Figure().add_annotation(
                text="No system price data available",
                xref="paper", yref="paper", x=0.5, y=0.5,
                showarrow=False, font=dict(color="#666", size=11),
            ).update_layout(**_chart_layout("", 260)),
            config={"displayModeBar": False},
        )

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=p["timestamp"], y=p["ssp_gbp_mwh"], name="SSP",
        line=dict(color="#FFC107", width=1.5),
        fill="tozeroy", fillcolor="rgba(255,193,7,0.08)",
        hovertemplate="SSP: £%{y:.2f}/MWh<extra></extra>",
    ))
    # Show SBP only when different from SSP (i.e., when balancing actions occurred)
    if "sbp_gbp_mwh" in p.columns:
        diff = (p["sbp_gbp_mwh"] - p["ssp_gbp_mwh"]).abs()
        if diff.max() > 0.01:
            fig.add_trace(go.Scatter(
                x=p["timestamp"], y=p["sbp_gbp_mwh"], name="SBP",
                line=dict(color="#FF9800", width=0.5, dash="dot"),
                hovertemplate="SBP: £%{y:.2f}/MWh<extra></extra>",
            ))

    fig.add_hline(y=0, line_color="#444", line_width=1)
    fig.update_layout(**_chart_layout("SYSTEM PRICE — SSP/SBP (£/MWh, imbalance settlement)", 260))
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _ic_chart(live: LiveData) -> dcc.Graph:
    """All 10 interconnectors with bidirectional flows."""
    ic = live.current_ic

    # Show all 10 ICs, even if flow is 0
    names = []
    vals = []
    caps = []
    for ic_name in ALL_ICS:
        names.append(ic_name)
        vals.append(ic.get(ic_name, 0))
        caps.append(IC_CAPACITY.get(ic_name, 0))

    # Sort by absolute flow (most active at top)
    triples = sorted(zip(names, vals, caps), key=lambda x: abs(x[1]), reverse=True)
    names = [t[0] for t in triples]
    vals = [t[1] for t in triples]
    caps = [t[2] for t in triples]

    colours = ["#4CAF50" if v > 0 else "#F44336" if v < 0 else "#333" for v in vals]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=names, x=vals, orientation="h", marker_color=colours,
        text=[f"{v:+,.0f}" if v != 0 else "0" for v in vals],
        textposition="auto",
        textfont=dict(size=8, color=["#ccc" if v != 0 else "#555" for v in vals]),
        hovertemplate="<b>%{y}</b>: %{x:+,.0f} MW<extra></extra>",
    ))

    # Rated capacity markers (faded)
    fig.add_trace(go.Scatter(
        y=names, x=caps, mode="markers", name="Rated MW",
        marker=dict(color="rgba(255,255,255,0.15)", size=4, symbol="line-ns"),
        hovertemplate="%{y} rated: %{x:,} MW<extra></extra>",
    ))

    fig.add_vline(x=0, line_color="#444")
    fig.update_layout(**_chart_layout("INTERCONNECTORS (MW) — dedicated endpoint, bidirectional", 260))
    fig.update_layout(showlegend=False)
    fig.update_xaxes(title_text="← Export | Import →")
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _utilisation_table(static: StaticData, live: LiveData) -> html.Div:
    """Installed capacity vs current output. Not capacity factor."""
    gen = live.current_gen
    installed = static.fuel_capacity

    rows = []
    for bmrs_fuel, our_fuels in FUEL_INSTALLED_MAP.items():
        inst = sum(installed.get(f, 0) for f in our_fuels)
        actual = gen.get(bmrs_fuel, 0)
        util = (actual / inst * 100) if inst > 0 else 0
        colour = BMRS_COLOURS.get(bmrs_fuel, "#999")

        util_col = "#4CAF50" if util > 50 else "#FFC107" if util > 20 else "#F44336" if util > 0 else "#444"
        now_str = f"{actual / 1000:.1f}"
        util_str = f"{util:.0f}%"

        rows.append(html.Tr([
            html.Td(html.Span("■", style={"color": colour}), style={"width": "12px", "padding": "2px 3px"}),
            html.Td(bmrs_fuel.replace("_", " ").upper(), style={"padding": "2px 4px", "fontSize": "9px"}),
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
        ], style={"lineHeight": "16px"}))

    return html.Div([
        _label("CURRENT UTILISATION — installed vs dispatched"),
        html.Table([
            html.Thead(html.Tr([
                html.Th(""), html.Th("FUEL"),
                html.Th("INST GW", style={"textAlign": "right"}),
                html.Th("NOW GW", style={"textAlign": "right"}),
                html.Th(""), html.Th("UTIL", style={"textAlign": "right"}),
            ], style={"color": "#444", "fontSize": "9px"})),
            html.Tbody(rows),
        ], style={"width": "100%", "borderCollapse": "collapse"}),
        html.Div(
            "Instantaneous utilisation (output ÷ installed), not capacity factor. "
            "CF requires ∫energy/∫time. "
            "BMRS 'other' includes PS gen + embedded solar/CHP (not shown above).",
            style={"color": "#3a3a3a", "fontSize": "8px", "marginTop": "4px", "lineHeight": "12px"},
        ),
    ])


def _regional_chart(live: LiveData) -> dcc.Graph:
    """Regional generation mix from Carbon Intensity API (14 DNO regions only)."""
    reg = live.ci_regional
    if reg.empty:
        return dcc.Graph(figure=go.Figure(), style={"height": "300px"})

    pivot = reg.pivot_table(index="region", columns="fuel", values="percentage", aggfunc="sum").fillna(0)

    # Sort by wind % (most renewable at top)
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

    fig.update_layout(**_chart_layout("REGIONAL MIX — 14 DNO regions (Carbon Intensity API, %, modelled)", 300))
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

def build_data_tab(static: StaticData) -> html.Div:
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
        ("BMRS Generation", "/generation/outturn/summary", "24h rolling", "No",
         "Transmission-metered only. No solar, no PS breakdown."),
        ("BMRS Demand", "/demand/outturn", "30d rolling", "No",
         "INDO + ITSDO. INDO ≈ consumer demand. ITSDO = transmission-level."),
        ("BMRS System Prices", "/balancing/settlement/system-prices/{date}", "Per SP", "No",
         "SSP/SBP imbalance settlement. NOT day-ahead auction prices."),
        ("BMRS Interconnectors", "/generation/outturn/interconnectors", "30d rolling", "No",
         "Bidirectional (+ import, − export). All 10 ICs."),
        ("Carbon Intensity", "carbonintensity.org.uk", "30min", "No",
         "Modelled estimates (%, not MW). Includes embedded solar."),
    ]
    live_rows = []
    for name, ep, freq, key, note in live_feeds:
        live_rows.append(html.Tr([
            html.Td(html.Span("●", style={"color": "#4CAF50", "marginRight": "4px"})),
            html.Td(name, style={"fontWeight": "500"}),
            html.Td(ep, style={"color": "#666", "fontSize": "9px"}),
            html.Td(freq, style={"textAlign": "right"}),
            html.Td(note, style={"color": "#555", "fontSize": "8px", "maxWidth": "250px"}),
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
                    html.Th("NOTES"),
                ], style={"color": "#444", "fontSize": "9px"})),
                html.Tbody(live_rows),
            ], style={"width": "100%", "borderCollapse": "collapse"}),
        ]),
        html.Div(style={"height": "8px"}),
        _panel([
            _label("DATA LIMITATIONS — read before trusting any number"),
            html.Pre(
                "GENERATION\n"
                "  BMRS generation summary = transmission-metered output only.\n"
                "  Missing: ~15 GW embedded solar, small wind, CHP, battery storage.\n"
                "  'OTHER' (~800-1000 MW) = pumped storage gen + embedded gen. Cannot decompose.\n"
                "  All wind is lumped as 'WIND' — no onshore/offshore split in real-time.\n"
                "\n"
                "PRICES\n"
                "  System Sell Price (SSP) and System Buy Price (SBP) = imbalance settlement.\n"
                "  These are NOT the wholesale day-ahead or intraday auction prices.\n"
                "  Day-ahead EPEX/N2EX prices are NOT freely available via BMRS API.\n"
                "  SSP/SBP diverge from wholesale during system stress events.\n"
                "\n"
                "INTERCONNECTORS\n"
                "  Dedicated IC endpoint has bidirectional flows (+import, -export).\n"
                "  Generation summary ICs are import-only (always ≥ 0) — we don't use those.\n"
                "  Dedicated endpoint may lag generation summary by 1-2 days.\n"
                "  When IC flow = 0, some ICs may be missing from response.\n"
                "\n"
                "SOLAR\n"
                "  Solar MW is estimated: Carbon Intensity API % × total supply.\n"
                "  CI percentages are National Grid ESO modelled estimates, not metered.\n"
                "  At night, solar correctly shows 0 MW / 0%.\n"
                "\n"
                "REGIONAL\n"
                "  Carbon Intensity regional mix is modelled, not metered.\n"
                "  14 DNO regions shown (aggregate England/Scotland/Wales/GB excluded).\n"
                "  Percentages may not sum to 100% due to rounding.\n"
                "\n"
                "INSTALLED CAPACITY\n"
                "  DUKES Table 5.11 is the authoritative source for UK installed capacity.\n"
                "  DUKES classifies all gas as 'Natural Gas' — no CCGT/OCGT split.\n"
                "  BMRS reports CCGT + OCGT separately; installed figures may not align.\n",
                style={"color": "#666", "fontSize": "9px", "lineHeight": "14px", "margin": 0, "whiteSpace": "pre-wrap"},
            ),
        ]),
    ])


# ═════════════════════════════════════════════════════════════════════════════
# TAB: PRICES & BALANCING
# ═════════════════════════════════════════════════════════════════════════════

def build_prices_tab(live: LiveData, hist: HistoricalData) -> html.Div:
    return html.Div([
        dbc.Row([
            dbc.Col(_panel([_ssp_history(hist)]), width=8),
            dbc.Col(_panel([_price_distribution(hist)]), width=4),
        ], className="mb-2 g-2"),
        dbc.Row([
            dbc.Col(_panel([_niv_chart(hist)]), width=6),
            dbc.Col(_panel([_market_depth_chart(hist)]), width=6),
        ], className="mb-2 g-2"),
        dbc.Row([
            dbc.Col(_panel([_price_by_hour(hist)]), width=6),
            dbc.Col(_panel([_price_stats_table(hist)]), width=6),
        ], className="g-2"),
    ])


def _ssp_history(hist: HistoricalData) -> dcc.Graph:
    """30-day SSP/SBP time series."""
    p = hist.prices
    if p.empty:
        return dcc.Graph(figure=go.Figure().add_annotation(
            text="No price data — run 'python -m uk_energy ts-backfill'",
            xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False,
            font=dict(color="#666", size=11),
        ).update_layout(**_chart_layout("", 320)), config={"displayModeBar": False})

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=p["timestamp"], y=p["ssp_gbp_mwh"], name="SSP",
        line=dict(color="#FFC107", width=1),
        hovertemplate="SSP: £%{y:.2f}<extra></extra>",
    ))
    # SBP only when different
    diff = (p["sbp_gbp_mwh"] - p["ssp_gbp_mwh"]).abs()
    if diff.max() > 0.5:
        fig.add_trace(go.Scatter(
            x=p["timestamp"], y=p["sbp_gbp_mwh"], name="SBP",
            line=dict(color="#FF9800", width=0.5, dash="dot"),
            hovertemplate="SBP: £%{y:.2f}<extra></extra>",
        ))
    fig.add_hline(y=0, line_color="#444")
    fig.update_layout(**_chart_layout("SYSTEM PRICES — SSP/SBP (30d, imbalance settlement)", 320))
    fig.update_yaxes(title_text="£/MWh")
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _price_distribution(hist: HistoricalData) -> dcc.Graph:
    """SSP histogram."""
    p = hist.prices
    if p.empty:
        return dcc.Graph(figure=go.Figure(), style={"height": "320px"})

    fig = go.Figure(go.Histogram(
        x=p["ssp_gbp_mwh"], nbinsx=50, marker_color="#FFC107", opacity=0.8,
        hovertemplate="£%{x:.0f}: %{y} periods<extra></extra>",
    ))
    # Mark current price
    if not p.empty:
        latest = p.iloc[-1]["ssp_gbp_mwh"]
        fig.add_vline(x=latest, line_color="#F44336", line_width=2,
                      annotation_text=f"Now £{latest:.0f}", annotation_font_color="#F44336")
    fig.update_layout(**_chart_layout("SSP DISTRIBUTION (30d)", 320))
    fig.update_xaxes(title_text="£/MWh")
    fig.update_yaxes(title_text="Settlement periods")
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _niv_chart(hist: HistoricalData) -> dcc.Graph:
    """Net Imbalance Volume — system long/short indicator."""
    p = hist.prices
    if p.empty or "niv_mw" not in p.columns:
        return dcc.Graph(figure=go.Figure(), style={"height": "260px"})

    colours = ["#4CAF50" if v > 0 else "#F44336" for v in p["niv_mw"]]

    fig = go.Figure(go.Bar(
        x=p["timestamp"], y=p["niv_mw"], marker_color=colours,
        hovertemplate="%{x}<br>NIV: %{y:+,.0f} MW<extra></extra>",
    ))
    fig.add_hline(y=0, line_color="#666")
    fig.update_layout(**_chart_layout("NET IMBALANCE VOLUME — green=long, red=short (30d)", 260))
    fig.update_layout(showlegend=False)
    fig.update_yaxes(title_text="MW")
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _market_depth_chart(hist: HistoricalData) -> dcc.Graph:
    """Market depth — offer/bid volumes and indicated imbalance."""
    md = hist.market_depth
    if md.empty:
        return dcc.Graph(figure=go.Figure().add_annotation(
            text="No market depth data", xref="paper", yref="paper", x=0.5, y=0.5,
            showarrow=False, font=dict(color="#666"),
        ).update_layout(**_chart_layout("", 260)), config={"displayModeBar": False})

    fig = go.Figure()
    if "offer_volume" in md.columns:
        fig.add_trace(go.Bar(
            x=md["timestamp"], y=md["offer_volume"], name="Offers",
            marker_color="rgba(76,175,80,0.5)",
            hovertemplate="Offers: %{y:,.0f} MW<extra></extra>",
        ))
    if "bid_volume" in md.columns:
        fig.add_trace(go.Bar(
            x=md["timestamp"], y=-md["bid_volume"].abs(), name="Bids",
            marker_color="rgba(244,67,54,0.5)",
            hovertemplate="Bids: %{y:,.0f} MW<extra></extra>",
        ))
    if "indicated_imbalance" in md.columns:
        fig.add_trace(go.Scatter(
            x=md["timestamp"], y=md["indicated_imbalance"], name="Indicated Imbalance",
            line=dict(color="#FFC107", width=1.5),
            hovertemplate="Imbalance: %{y:+,.0f} MW<extra></extra>",
        ))
    fig.update_layout(**_chart_layout("MARKET DEPTH — offers/bids/imbalance (MW)", 260))
    fig.update_layout(barmode="overlay")
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _price_by_hour(hist: HistoricalData) -> dcc.Graph:
    """Average SSP by hour of day — the daily price shape."""
    p = hist.prices
    if p.empty:
        return dcc.Graph(figure=go.Figure(), style={"height": "280px"})

    p = p.copy()
    p["hour"] = p["timestamp"].dt.hour
    hourly = p.groupby("hour").agg(
        mean=("ssp_gbp_mwh", "mean"),
        p25=("ssp_gbp_mwh", lambda x: x.quantile(0.25)),
        p75=("ssp_gbp_mwh", lambda x: x.quantile(0.75)),
    ).reset_index()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=hourly["hour"], y=hourly["p75"], name="P75",
        line=dict(width=0), showlegend=False,
    ))
    fig.add_trace(go.Scatter(
        x=hourly["hour"], y=hourly["p25"], name="P25-P75",
        line=dict(width=0), fill="tonexty", fillcolor="rgba(255,193,7,0.15)",
    ))
    fig.add_trace(go.Scatter(
        x=hourly["hour"], y=hourly["mean"], name="Mean SSP",
        line=dict(color="#FFC107", width=2),
        hovertemplate="Hour %{x}: £%{y:.1f}/MWh<extra></extra>",
    ))
    fig.update_layout(**_chart_layout("SSP BY HOUR OF DAY — mean + P25/P75 band (30d)", 280))
    fig.update_xaxes(title_text="Hour (UTC)", dtick=3)
    fig.update_yaxes(title_text="£/MWh")
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _price_stats_table(hist: HistoricalData) -> html.Div:
    """Key price statistics."""
    p = hist.prices
    if p.empty:
        return html.Div("No price data", style={"color": "#555"})

    ssp = p["ssp_gbp_mwh"]
    niv = p.get("niv_mw", pd.Series(dtype=float))

    stats = [
        ("Mean SSP", f"£{ssp.mean():.2f}/MWh"),
        ("Median SSP", f"£{ssp.median():.2f}/MWh"),
        ("StdDev", f"£{ssp.std():.2f}"),
        ("Min", f"£{ssp.min():.2f}"),
        ("Max", f"£{ssp.max():.2f}"),
        ("P10", f"£{ssp.quantile(0.1):.2f}"),
        ("P90", f"£{ssp.quantile(0.9):.2f}"),
        ("Negative periods", f"{(ssp < 0).sum()} ({(ssp < 0).mean() * 100:.1f}%)"),
        ("Spike >£150", f"{(ssp > 150).sum()} ({(ssp > 150).mean() * 100:.1f}%)"),
        ("Periods", f"{len(ssp):,}"),
    ]

    if not niv.empty and niv.notna().any():
        stats.extend([
            ("", ""),
            ("Mean NIV", f"{niv.mean():+,.0f} MW"),
            ("System long %", f"{(niv > 0).mean() * 100:.0f}%"),
            ("System short %", f"{(niv < 0).mean() * 100:.0f}%"),
        ])

    # Accepted volumes
    if "accepted_offer_vol" in p.columns:
        offers = p["accepted_offer_vol"]
        bids = p["accepted_bid_vol"]
        if offers.notna().any():
            stats.extend([
                ("", ""),
                ("Avg accepted offers", f"{offers.mean():,.0f} MW"),
                ("Avg accepted bids", f"{bids.abs().mean():,.0f} MW"),
            ])

    rows = []
    for label, value in stats:
        if label == "":
            rows.append(html.Tr([html.Td("", colSpan=2, style={"height": "6px"})]))
        else:
            rows.append(html.Tr([
                html.Td(label, style={"color": "#777", "padding": "2px 6px"}),
                html.Td(value, style={"textAlign": "right", "padding": "2px 6px", "fontWeight": "500"}),
            ]))

    return html.Div([
        _label("PRICE STATISTICS (30d)"),
        html.Table(rows, style={"width": "100%", "borderCollapse": "collapse"}),
    ])


# ═════════════════════════════════════════════════════════════════════════════
# TAB: FORECASTS
# ═════════════════════════════════════════════════════════════════════════════

def build_forecasts_tab(live: LiveData, hist: HistoricalData, static: StaticData) -> html.Div:
    return html.Div([
        dbc.Row([
            dbc.Col(_panel([_wind_forecast_chart(hist, live)]), width=8),
            dbc.Col(_panel([_gen_availability_table(hist, static)]), width=4),
        ], className="mb-2 g-2"),
        dbc.Row([
            dbc.Col(_panel([_weather_wind_chart(hist)]), width=6),
            dbc.Col(_panel([_weather_solar_chart(hist)]), width=6),
        ], className="mb-2 g-2"),
        dbc.Row([
            dbc.Col(_panel([_demand_forecast_chart(hist, live)]), width=6),
            dbc.Col(_panel([_frequency_summary(hist)]), width=6),
        ], className="g-2"),
    ])


def _wind_forecast_chart(hist: HistoricalData, live: LiveData) -> dcc.Graph:
    """Wind generation forecast vs actual."""
    wf = hist.wind_forecast
    gen = live.generation

    fig = go.Figure()

    # Actual wind generation (from live)
    if not gen.empty:
        wind = gen[gen["fuel_type"] == "wind"].copy()
        if not wind.empty:
            fig.add_trace(go.Scatter(
                x=wind["timestamp"], y=wind["generation_mw"], name="Actual",
                line=dict(color="#2196F3", width=2),
                hovertemplate="Actual: %{y:,.0f} MW<extra></extra>",
            ))

    # Forecast
    if not wf.empty:
        fig.add_trace(go.Scatter(
            x=wf["timestamp"], y=wf["generation_mw"], name="Forecast",
            line=dict(color="#FFC107", width=1.5, dash="dot"),
            hovertemplate="Forecast: %{y:,.0f} MW<extra></extra>",
        ))

    fig.update_layout(**_chart_layout("WIND GENERATION — actual vs ESO forecast (MW)", 300))
    fig.update_yaxes(title_text="MW")
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _gen_availability_table(hist: HistoricalData, static: StaticData) -> html.Div:
    """Generation availability vs installed capacity."""
    ga = hist.gen_availability
    if ga.empty:
        return html.Div("No availability data", style={"color": "#555"})

    # Get the earliest forecast date (soonest)
    latest = ga.groupby("fuel_type")["available_mw"].first().to_dict()
    installed = static.fuel_capacity

    fuel_display = {
        "nuclear": "Nuclear", "gas_ccgt": "Gas CCGT", "gas_ocgt": "Gas OCGT",
        "biomass": "Biomass", "wind": "Wind", "hydro": "Hydro",
        "pumped_storage": "Pumped Storage", "oil": "Oil", "other": "Other",
        "coal": "Coal",
    }

    rows = []
    for fuel, avail_mw in sorted(latest.items(), key=lambda x: -x[1]):
        if fuel.startswith("ic_"):
            continue
        display = fuel_display.get(fuel, fuel)
        inst_fuels = FUEL_INSTALLED_MAP.get(fuel, [fuel])
        inst = sum(installed.get(f, 0) for f in inst_fuels)
        pct = (avail_mw / inst * 100) if inst > 0 else 0
        outage = max(0, inst - avail_mw)

        colour = "#4CAF50" if pct > 80 else "#FFC107" if pct > 50 else "#F44336"
        rows.append(html.Tr([
            html.Td(display, style={"padding": "2px 4px", "fontSize": "9px"}),
            html.Td(f"{inst / 1000:.1f}", style={"textAlign": "right", "color": "#666", "padding": "2px 4px"}),
            html.Td(f"{avail_mw / 1000:.1f}", style={"textAlign": "right", "padding": "2px 4px"}),
            html.Td(f"{outage / 1000:.1f}", style={"textAlign": "right", "color": "#F44336" if outage > 100 else "#555", "padding": "2px 4px"}),
            html.Td(f"{pct:.0f}%", style={"textAlign": "right", "color": colour, "padding": "2px 4px"}),
        ], style={"lineHeight": "16px"}))

    return html.Div([
        _label("GENERATION AVAILABILITY (forecast)"),
        html.Table([
            html.Thead(html.Tr([
                html.Th("FUEL"), html.Th("INST GW", style={"textAlign": "right"}),
                html.Th("AVAIL GW", style={"textAlign": "right"}),
                html.Th("OUTAGE GW", style={"textAlign": "right"}),
                html.Th("AVAIL %", style={"textAlign": "right"}),
            ], style={"color": "#444", "fontSize": "9px"})),
            html.Tbody(rows),
        ], style={"width": "100%", "borderCollapse": "collapse"}),
        html.Div(
            "Source: BMRS /forecast/availability/daily. Includes planned + forced outages.",
            style={"color": "#3a3a3a", "fontSize": "8px", "marginTop": "4px"},
        ),
    ])


def _weather_wind_chart(hist: HistoricalData) -> dcc.Graph:
    """Wind speed index — offshore and onshore."""
    wi = hist.weather_index
    if wi.empty:
        return dcc.Graph(figure=go.Figure(), style={"height": "260px"})

    fig = go.Figure()
    if "offshore_wind_ms" in wi.columns:
        fig.add_trace(go.Scatter(
            x=wi["timestamp"], y=wi["offshore_wind_ms"], name="Offshore (100m)",
            line=dict(color="#2196F3", width=1.5),
            hovertemplate="%{y:.1f} m/s<extra>Offshore</extra>",
        ))
    if "onshore_wind_ms" in wi.columns:
        fig.add_trace(go.Scatter(
            x=wi["timestamp"], y=wi["onshore_wind_ms"], name="Onshore (100m)",
            line=dict(color="#64B5F6", width=1, dash="dot"),
            hovertemplate="%{y:.1f} m/s<extra>Onshore</extra>",
        ))
    # Cut-in and cut-out reference lines
    fig.add_hline(y=3, line_color="#444", line_width=0.5, annotation_text="cut-in",
                  annotation_font_color="#555", annotation_font_size=8)
    fig.add_hline(y=25, line_color="#F44336", line_width=0.5, annotation_text="cut-out",
                  annotation_font_color="#F44336", annotation_font_size=8)

    fig.update_layout(**_chart_layout("WIND SPEED INDEX — 15 UK sites (7d + 3d forecast, m/s)", 260))
    fig.update_yaxes(title_text="m/s")
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _weather_solar_chart(hist: HistoricalData) -> dcc.Graph:
    """Solar irradiance + temperature."""
    wi = hist.weather_index
    if wi.empty:
        return dcc.Graph(figure=go.Figure(), style={"height": "260px"})

    fig = go.Figure()
    if "solar_ghi_wm2" in wi.columns:
        fig.add_trace(go.Scatter(
            x=wi["timestamp"], y=wi["solar_ghi_wm2"], name="GHI (W/m²)",
            line=dict(color="#FFC107", width=1.5),
            fill="tozeroy", fillcolor="rgba(255,193,7,0.1)",
            hovertemplate="%{y:.0f} W/m²<extra>GHI</extra>",
        ))
    if "temperature_c" in wi.columns:
        fig.add_trace(go.Scatter(
            x=wi["timestamp"], y=wi["temperature_c"], name="Temp (°C)",
            line=dict(color="#F44336", width=1, dash="dot"),
            yaxis="y2",
            hovertemplate="%{y:.1f}°C<extra>Temp</extra>",
        ))

    fig.update_layout(**_chart_layout("SOLAR IRRADIANCE + TEMPERATURE (7d + 3d forecast)", 260))
    fig.update_layout(
        yaxis2=dict(overlaying="y", side="right", gridcolor="#262626",
                    title="°C", title_font_color="#F44336"),
    )
    fig.update_yaxes(title_text="W/m²")
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _demand_forecast_chart(hist: HistoricalData, live: LiveData) -> dcc.Graph:
    """Demand forecast vs actual."""
    fc = hist.demand_forecast
    demand = live.demand

    fig = go.Figure()
    if not demand.empty:
        fig.add_trace(go.Scatter(
            x=demand["timestamp"], y=demand["demand_mw"], name="Actual (INDO)",
            line=dict(color="#2196F3", width=1.5),
            hovertemplate="%{y:,.0f} MW<extra>Actual</extra>",
        ))
    if not fc.empty:
        fig.add_trace(go.Scatter(
            x=fc["forecast_timestamp"], y=fc["national_demand_mw"], name="Forecast",
            line=dict(color="#FFC107", width=1.5, dash="dot"),
            hovertemplate="%{y:,.0f} MW<extra>Forecast</extra>",
        ))

    fig.update_layout(**_chart_layout("DEMAND — actual INDO vs ESO day-ahead forecast (MW)", 260))
    fig.update_yaxes(title_text="MW")
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _frequency_summary(hist: HistoricalData) -> html.Div:
    """System frequency statistics."""
    fs = hist.frequency_stats
    if not fs:
        return html.Div("No frequency data", style={"color": "#555"})

    items = [
        _label("SYSTEM FREQUENCY (24h, 1-second resolution)"),
    ]

    stats = [
        ("Mean", f"{fs.get('mean', 0):.4f} Hz"),
        ("Min", f"{fs.get('min_f', 0):.4f} Hz"),
        ("Max", f"{fs.get('max_f', 0):.4f} Hz"),
        ("StdDev", f"{fs.get('stddev', 0):.4f} Hz"),
        ("Samples", f"{int(fs.get('total', 0)):,}"),
        ("Below 49.8 Hz", f"{int(fs.get('below_49_8', 0)):,}"),
        ("Above 50.2 Hz", f"{int(fs.get('above_50_2', 0)):,}"),
    ]

    for label, value in stats:
        items.append(html.Div([
            html.Span(label, style={"color": "#777", "width": "100px", "display": "inline-block"}),
            html.Span(value, style={"fontWeight": "500"}),
        ], style={"lineHeight": "18px"}))

    items.append(html.Div(
        "Nominal: 50.000 Hz. Statutory limits: 49.5–50.5 Hz. "
        "Below 49.8 or above 50.2 = frequency event.",
        style={"color": "#3a3a3a", "fontSize": "8px", "marginTop": "8px"},
    ))

    return _panel(items)
