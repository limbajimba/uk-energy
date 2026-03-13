"""
app.py — UK Energy System Dashboard.

Single entry point. Tabbed layout: Live System | Asset Map | Data Sources.
Run: python -m uk_energy dashboard
"""

from __future__ import annotations

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, callback, dcc, html

from uk_energy.dashboard.data import load_data, load_live_data

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.CYBORG],
    title="UK Energy System",
    update_title=None,
)
server = app.server

MONO = "'JetBrains Mono', 'Consolas', 'SF Mono', monospace"

app.layout = html.Div(
    [
        # ─── Header ───
        html.Div(
            [
                html.Span("UK ENERGY SYSTEM", style={"fontWeight": "700", "letterSpacing": "1px", "fontSize": "13px"}),
                html.Span(id="header-status", className="ms-3", style={"color": "#666", "fontSize": "11px"}),
                dbc.Button("↻", id="refresh-btn", size="sm", color="secondary", className="ms-auto", style={"fontSize": "11px"}),
            ],
            className="d-flex align-items-center px-3 py-1",
            style={"borderBottom": "1px solid #333"},
        ),
        # ─── Tabs ───
        dbc.Tabs(
            [
                dbc.Tab(label="LIVE SYSTEM", tab_id="live", label_style={"fontSize": "11px", "padding": "4px 12px"}),
                dbc.Tab(label="ASSET MAP", tab_id="map", label_style={"fontSize": "11px", "padding": "4px 12px"}),
                dbc.Tab(label="DATA SOURCES", tab_id="sources", label_style={"fontSize": "11px", "padding": "4px 12px"}),
            ],
            id="tabs",
            active_tab="live",
            className="mt-1 px-2",
        ),
        dcc.Interval(id="interval", interval=300_000, n_intervals=0),
        html.Div(id="tab-content", className="p-2"),
    ],
    style={"fontFamily": MONO, "fontSize": "11px"},
)


@callback(
    [Output("tab-content", "children"), Output("header-status", "children")],
    [Input("tabs", "active_tab"), Input("interval", "n_intervals"), Input("refresh-btn", "n_clicks")],
)
def render_tab(tab, _n, _clicks):
    static = load_data()

    if tab == "map":
        from uk_energy.dashboard.layouts import build_map_tab
        return build_map_tab(static), ""

    if tab == "sources":
        from uk_energy.dashboard.layouts import build_sources_tab
        return build_sources_tab(static), ""

    # Live system (default)
    live = load_live_data()
    from uk_energy.dashboard.layouts import build_live_tab
    layout = build_live_tab(static, live)
    status = (
        f"{live.fetch_time.strftime('%H:%M:%S')} · "
        f"{live.n_periods} periods · "
        f"{live.carbon_gco2:.0f} gCO₂/kWh ({live.carbon_intensity.get('index', '?')})"
    )
    return layout, status


def main(host: str = "127.0.0.1", port: int = 8050, debug: bool = True) -> None:
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
