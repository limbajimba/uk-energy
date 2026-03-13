"""
app.py — Main Dash application entry point.

Run with: python -m uk_energy.dashboard.app
Or:       python -m uk_energy dashboard
"""

from __future__ import annotations

import dash
import dash_bootstrap_components as dbc
from dash import html

from uk_energy.dashboard.data import load_data
from uk_energy.dashboard.pages import overview, generation, regional, explorer

# ─── App Setup ───────────────────────────────────────────────────────────────

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.SLATE, dbc.icons.FONT_AWESOME],
    suppress_callback_exceptions=True,
    title="UK Energy System",
    update_title=None,
)

server = app.server  # For gunicorn deployment

# ─── Navigation ──────────────────────────────────────────────────────────────

TABS = [
    {"id": "overview", "label": "Overview", "icon": "fa-solid fa-gauge-high"},
    {"id": "generation", "label": "Generation Mix", "icon": "fa-solid fa-bolt"},
    {"id": "regional", "label": "Regional", "icon": "fa-solid fa-map"},
    {"id": "explorer", "label": "Plant Explorer", "icon": "fa-solid fa-magnifying-glass"},
]


def make_navbar() -> dbc.Navbar:
    return dbc.Navbar(
        dbc.Container(
            [
                dbc.NavbarBrand(
                    [
                        html.I(className="fa-solid fa-plug-circle-bolt me-2"),
                        "UK Energy System",
                    ],
                    className="fs-5 fw-bold",
                ),
                dbc.Nav(
                    [
                        dbc.NavItem(
                            dbc.NavLink(
                                [html.I(className=f"{t['icon']} me-1"), t["label"]],
                                id=f"nav-{t['id']}",
                                href=f"/{t['id']}" if t["id"] != "overview" else "/",
                                active="exact",
                            )
                        )
                        for t in TABS
                    ],
                    navbar=True,
                ),
            ],
            fluid=True,
        ),
        color="dark",
        dark=True,
        sticky="top",
    )


# ─── Layout ──────────────────────────────────────────────────────────────────

app.layout = html.Div(
    [
        make_navbar(),
        dash.page_container
        if hasattr(dash, "page_container")
        else html.Div(id="page-content", className="p-3"),
    ]
)


# ─── Routing (manual — Dash pages API is fiddly) ────────────────────────────

from dash import Input, Output, callback, dcc  # noqa: E402

app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        make_navbar(),
        html.Div(id="page-content", className="px-3 py-3"),
    ]
)


@callback(Output("page-content", "children"), Input("url", "pathname"))
def render_page(pathname: str):
    data = load_data()
    if pathname == "/generation":
        return generation.layout(data)
    elif pathname == "/regional":
        return regional.layout(data)
    elif pathname == "/explorer":
        return explorer.layout(data)
    return overview.layout(data)


# ─── Callbacks ───────────────────────────────────────────────────────────────
# Register page-specific callbacks
explorer.register_callbacks(app)


def main(host: str = "127.0.0.1", port: int = 8050, debug: bool = True) -> None:
    """Run the dashboard server."""
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
