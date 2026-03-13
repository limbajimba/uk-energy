"""
app.py — UK Energy System Dashboard.

Single-page, dense layout. Real data from BMRS + static asset register.
Run: python -m uk_energy dashboard
"""

from __future__ import annotations

from datetime import date, timedelta

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


app.layout = html.Div(
    [
        # Header bar — minimal
        html.Div(
            [
                html.Span("UK ENERGY SYSTEM", style={"fontWeight": "700", "letterSpacing": "1px"}),
                html.Span(id="header-status", className="ms-3", style={"color": "#888", "fontSize": "12px"}),
                dbc.Button("Refresh", id="refresh-btn", size="sm", color="secondary", className="ms-auto"),
            ],
            className="d-flex align-items-center px-3 py-2",
            style={"borderBottom": "1px solid #333", "fontSize": "13px"},
        ),
        # Auto-refresh every 5 minutes
        dcc.Interval(id="interval", interval=300_000, n_intervals=0),
        # Main content
        html.Div(id="main-content", className="p-2"),
    ],
    style={"fontFamily": "'JetBrains Mono', 'Consolas', monospace", "fontSize": "12px"},
)


@callback(
    [Output("main-content", "children"), Output("header-status", "children")],
    [Input("interval", "n_intervals"), Input("refresh-btn", "n_clicks")],
)
def update_dashboard(_n, _clicks):
    from uk_energy.dashboard.layouts import build_layout
    static = load_data()
    live = load_live_data()
    layout = build_layout(static, live)
    status_text = f"Last update: {live.fetch_time.strftime('%H:%M:%S')} · {live.n_periods} periods"
    return layout, status_text


def main(host: str = "127.0.0.1", port: int = 8050, debug: bool = True) -> None:
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
