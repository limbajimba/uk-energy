"""
theme.py — Shared UI components and colour scheme.
"""

from __future__ import annotations

import dash_bootstrap_components as dbc
from dash import html


# ─── Fuel Colours ────────────────────────────────────────────────────────────

FUEL_COLOURS: dict[str, str] = {
    "gas_ccgt":           "#FF5722",
    "wind_offshore":      "#0D47A1",
    "wind_onshore":       "#2196F3",
    "nuclear":            "#9C27B0",
    "solar_pv":           "#FFC107",
    "biomass":            "#4CAF50",
    "hydro_pumped_storage": "#006064",
    "hydro_run_of_river": "#00BCD4",
    "oil":                "#795548",
    "gas_ocgt":           "#FF7043",
    "gas_chp":            "#FFAB40",
    "battery_storage":    "#E91E63",
    "hydrogen":           "#00E676",
    "wave_tidal":         "#80DEEA",
    "geothermal":         "#FF6F00",
    "coal":               "#212121",
    "other":              "#9E9E9E",
    "unknown":            "#BDBDBD",
}

FUEL_LABELS: dict[str, str] = {
    "gas_ccgt":           "Gas CCGT",
    "gas_ocgt":           "Gas OCGT",
    "gas_chp":            "Gas CHP",
    "wind_onshore":       "Wind (Onshore)",
    "wind_offshore":      "Wind (Offshore)",
    "nuclear":            "Nuclear",
    "solar_pv":           "Solar PV",
    "biomass":            "Biomass",
    "hydro_run_of_river": "Hydro",
    "hydro_pumped_storage": "Pumped Storage",
    "battery_storage":    "Battery Storage",
    "hydrogen":           "Hydrogen",
    "oil":                "Oil",
    "coal":               "Coal",
    "wave_tidal":         "Wave & Tidal",
    "geothermal":         "Geothermal",
    "other":              "Other",
    "unknown":            "Unknown",
}

FUEL_ORDER: list[str] = [
    "gas_ccgt", "wind_offshore", "wind_onshore", "nuclear", "solar_pv",
    "biomass", "hydro_pumped_storage", "hydro_run_of_river", "oil",
    "gas_ocgt", "gas_chp", "battery_storage", "hydrogen",
    "wave_tidal", "geothermal", "coal",
]


# ─── Reusable Components ────────────────────────────────────────────────────

def kpi_card(title: str, value: str, icon: str, colour: str = "primary") -> dbc.Card:
    """Create a KPI metric card."""
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    [
                        html.I(className=f"fa-solid {icon}", style={"fontSize": "1.8rem", "opacity": "0.6"}),
                        html.Div(
                            [
                                html.P(title, className="text-muted mb-0", style={"fontSize": "0.8rem"}),
                                html.H4(value, className="mb-0 fw-bold"),
                            ],
                        ),
                    ],
                    className="d-flex align-items-center gap-3",
                ),
            ]
        ),
        className=f"border-start border-{colour} border-3",
        style={"borderColor": "transparent"},
    )


def card(title: str, content, subtitle: str | None = None) -> dbc.Card:
    """Wrap content in a dark card with title."""
    header_children = [html.H6(title, className="mb-0")]
    if subtitle:
        header_children.append(
            html.Small(subtitle, className="text-muted ms-2")
        )

    return dbc.Card(
        [
            dbc.CardHeader(html.Div(header_children, className="d-flex align-items-center")),
            dbc.CardBody(content),
        ]
    )
