"""
app.py — UK Energy System Dashboard.

Tabs: Live System | Prices & Balancing | Forecasts | Asset Map | Data
"""

from __future__ import annotations

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, callback, dcc, html

from uk_energy.dashboard.data import load_data, load_live_data, load_historical

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
        html.Div(
            [
                html.Span("UK ENERGY SYSTEM", style={"fontWeight": "700", "letterSpacing": "1px", "fontSize": "13px"}),
                html.Span(id="header-status", className="ms-3", style={"color": "#666", "fontSize": "11px"}),
                dbc.Button("↻", id="refresh-btn", size="sm", color="secondary", className="ms-auto", style={"fontSize": "11px"}),
            ],
            className="d-flex align-items-center px-3 py-1",
            style={"borderBottom": "1px solid #333"},
        ),
        dbc.Tabs(
            [
                dbc.Tab(label="LIVE", tab_id="live", label_style={"fontSize": "11px", "padding": "4px 12px"}),
                dbc.Tab(label="PRICES", tab_id="prices", label_style={"fontSize": "11px", "padding": "4px 12px"}),
                dbc.Tab(label="FORECASTS", tab_id="forecasts", label_style={"fontSize": "11px", "padding": "4px 12px"}),
                dbc.Tab(label="MAP", tab_id="map", label_style={"fontSize": "11px", "padding": "4px 12px"}),
                dbc.Tab(label="DATA", tab_id="data", label_style={"fontSize": "11px", "padding": "4px 12px"}),
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
    from uk_energy.dashboard import layouts

    static = load_data()

    if tab == "map":
        return layouts.build_map_tab(static), ""

    if tab == "data":
        return layouts.build_data_tab(static), ""

    # Tabs that need live data
    live = load_live_data()
    status = (
        f"{live.fetch_time.strftime('%H:%M:%S')} · "
        f"{live.n_periods} SP · "
        f"{live.carbon_gco2:.0f} gCO₂/kWh ({live.carbon_intensity.get('index', '?')})"
    )

    if tab == "prices":
        hist = load_historical()
        return layouts.build_prices_tab(live, hist), status

    if tab == "forecasts":
        hist = load_historical()
        return layouts.build_forecasts_tab(live, hist, static), status

    # Default: live
    return layouts.build_live_tab(static, live), status


def main(host: str = "127.0.0.1", port: int = 8050, debug: bool = True) -> None:
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
