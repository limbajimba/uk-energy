"""
explorer.py — Plant explorer with interactive filters.

Searchable, filterable table of all plants with scatter map.
"""

from __future__ import annotations

import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, callback, dash_table, dcc, html
import pandas as pd

from uk_energy.dashboard.data import DashboardData, load_data
from uk_energy.dashboard.theme import FUEL_COLOURS, FUEL_LABELS, card


def layout(data: DashboardData) -> html.Div:
    fuel_options = [
        {"label": FUEL_LABELS.get(f, f), "value": f}
        for f in sorted(data.plants["fuel_type"].dropna().unique())
    ]
    status_options = [
        {"label": s.replace("_", " ").title(), "value": s}
        for s in sorted(data.plants["status"].dropna().unique())
    ]
    region_options = [
        {"label": r, "value": r}
        for r in sorted(data.plants["dno_region"].dropna().unique())
    ]

    return html.Div(
        [
            # ─── Filters ───
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.Label("Search", html_for="search-input", className="small"),
                            dbc.Input(
                                id="search-input",
                                placeholder="Plant name...",
                                type="text",
                                debounce=True,
                                className="bg-dark text-light border-secondary",
                            ),
                        ],
                        md=3,
                    ),
                    dbc.Col(
                        [
                            dbc.Label("Fuel Type", className="small"),
                            dcc.Dropdown(
                                id="fuel-filter",
                                options=fuel_options,
                                multi=True,
                                placeholder="All fuel types",
                                className="dash-dark-dropdown",
                            ),
                        ],
                        md=3,
                    ),
                    dbc.Col(
                        [
                            dbc.Label("Status", className="small"),
                            dcc.Dropdown(
                                id="status-filter",
                                options=status_options,
                                value=["operational"],
                                multi=True,
                                placeholder="All statuses",
                                className="dash-dark-dropdown",
                            ),
                        ],
                        md=3,
                    ),
                    dbc.Col(
                        [
                            dbc.Label("Region", className="small"),
                            dcc.Dropdown(
                                id="region-filter",
                                options=region_options,
                                multi=True,
                                placeholder="All regions",
                                className="dash-dark-dropdown",
                            ),
                        ],
                        md=3,
                    ),
                ],
                className="mb-3",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.Label("Min Capacity (MW)", className="small"),
                            dbc.Input(
                                id="min-cap-input",
                                type="number",
                                value=0,
                                min=0,
                                className="bg-dark text-light border-secondary",
                            ),
                        ],
                        md=2,
                    ),
                    dbc.Col(
                        html.Div(id="filter-summary", className="mt-4 text-muted small"),
                        md=10,
                    ),
                ],
                className="mb-3",
            ),
            # ─── Map + Table ───
            dbc.Row(
                [
                    dbc.Col(html.Div(id="explorer-map"), md=7),
                    dbc.Col(html.Div(id="explorer-table"), md=5),
                ],
            ),
        ]
    )


def register_callbacks(app: Dash) -> None:
    @app.callback(
        [
            Output("explorer-map", "children"),
            Output("explorer-table", "children"),
            Output("filter-summary", "children"),
        ],
        [
            Input("search-input", "value"),
            Input("fuel-filter", "value"),
            Input("status-filter", "value"),
            Input("region-filter", "value"),
            Input("min-cap-input", "value"),
        ],
    )
    def update_explorer(search, fuels, statuses, regions, min_cap):
        data = load_data()
        df = data.plants.copy()

        # Apply filters
        if search:
            df = df[df["name"].str.contains(search, case=False, na=False)]
        if fuels:
            df = df[df["fuel_type"].isin(fuels)]
        if statuses:
            df = df[df["status"].isin(statuses)]
        if regions:
            df = df[df["dno_region"].isin(regions)]
        if min_cap and min_cap > 0:
            df = df[df["capacity_mw"] >= min_cap]

        # Limit for performance
        display_df = df.nlargest(500, "capacity_mw") if len(df) > 500 else df

        # Summary
        summary = (
            f"Showing {len(display_df):,} of {len(df):,} plants · "
            f"{df['capacity_mw'].sum() / 1000:,.1f} GW total capacity"
        )

        # Map
        map_df = display_df[
            display_df["lat"].notna() & display_df["lon"].notna()
        ].copy()
        map_df["fuel_label"] = map_df["fuel_type"].map(FUEL_LABELS).fillna(map_df["fuel_type"])
        map_df["cap_str"] = map_df["capacity_mw"].apply(
            lambda x: f"{x:,.0f} MW" if pd.notna(x) else "N/A"
        )

        if not map_df.empty:
            fig = px.scatter_mapbox(
                map_df,
                lat="lat",
                lon="lon",
                color="fuel_label",
                size="capacity_mw",
                size_max=20,
                hover_name="name",
                hover_data={"cap_str": True, "status": True, "fuel_label": True, "lat": False, "lon": False, "capacity_mw": False},
                color_discrete_map={FUEL_LABELS.get(f, f): c for f, c in FUEL_COLOURS.items()},
                zoom=5,
                center={"lat": 54.5, "lon": -2.5},
                mapbox_style="carto-darkmatter",
            )
            fig.update_layout(
                margin=dict(t=0, b=0, l=0, r=0),
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#ddd"),
                height=600,
                legend=dict(
                    title="",
                    font=dict(size=10, color="#ccc"),
                    bgcolor="rgba(0,0,0,0.5)",
                ),
            )
            map_chart = dcc.Graph(figure=fig, config={"displayModeBar": False})
        else:
            map_chart = html.P("No plants with coordinates match your filters.", className="text-muted")

        # Table
        table_df = display_df[["name", "fuel_type", "capacity_mw", "status", "dno_region", "owner"]].copy()
        table_df["fuel_type"] = table_df["fuel_type"].map(FUEL_LABELS).fillna(table_df["fuel_type"])
        table_df["capacity_mw"] = table_df["capacity_mw"].apply(
            lambda x: f"{x:,.0f}" if pd.notna(x) else ""
        )
        table_df.columns = ["Name", "Fuel", "MW", "Status", "Region", "Owner"]

        table = dash_table.DataTable(
            data=table_df.to_dict("records"),
            columns=[{"name": c, "id": c} for c in table_df.columns],
            sort_action="native",
            page_size=20,
            style_table={"overflowX": "auto"},
            style_header={
                "backgroundColor": "#2a2a2a",
                "color": "#ddd",
                "fontWeight": "600",
                "fontSize": "12px",
            },
            style_cell={
                "backgroundColor": "#1e1e1e",
                "color": "#ccc",
                "fontSize": "12px",
                "padding": "6px 10px",
                "border": "1px solid #333",
                "textOverflow": "ellipsis",
                "maxWidth": "200px",
            },
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": "#252525"},
            ],
        )

        return map_chart, table, summary
