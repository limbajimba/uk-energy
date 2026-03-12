"""
export.py — Export the grid graph to various formats.

Outputs:
  - GeoJSON (for web maps)
  - GraphML (for graph analysis tools like Gephi, Cytoscape)
  - NetworkX pickle (for Python reload)
  - Summary statistics CSV
"""

from __future__ import annotations

import csv
import json
import pickle
from pathlib import Path
from typing import Any

import networkx as nx
from loguru import logger

from uk_energy.config import GRAPH_PICKLE, GRAPHML_OUTPUT, OUTPUT_DIR, STATS_CSV
from uk_energy.graph.model import NodeType
from uk_energy.graph.topology import full_analysis


def export_geojson(G: nx.DiGraph, output_path: Path | None = None) -> Path:
    """
    Export all nodes with coordinates to GeoJSON.

    Creates a FeatureCollection of all generation plants, GSPs,
    substations, and interconnector terminals.
    """
    if output_path is None:
        output_path = OUTPUT_DIR / "uk_grid_nodes.geojson"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    features: list[dict[str, Any]] = []

    for node_id, data in G.nodes(data=True):
        lat = data.get("lat")
        lon = data.get("lon")
        if lat is None or lon is None:
            continue

        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except (TypeError, ValueError):
            continue

        # Build properties (exclude geometry-related)
        props = {k: v for k, v in data.items() if k not in ("lat", "lon")}
        # Serialise list types
        for k, v in props.items():
            if isinstance(v, list):
                props[k] = json.dumps(v)

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon_f, lat_f],
            },
            "properties": {"node_id": node_id, **props},
        }
        features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}
    output_path.write_text(json.dumps(geojson, indent=2))
    logger.success(f"Exported {len(features)} nodes to GeoJSON → {output_path}")
    return output_path


def export_graphml(G: nx.DiGraph, output_path: Path | None = None) -> Path:
    """
    Export to GraphML for use in Gephi, Cytoscape, yEd, etc.

    GraphML requires string/numeric attributes only — we stringify lists/dicts.
    """
    if output_path is None:
        output_path = GRAPHML_OUTPUT

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # GraphML doesn't support complex types — flatten
    G_export = nx.DiGraph()
    for node_id, data in G.nodes(data=True):
        flat = {}
        for k, v in data.items():
            if isinstance(v, (list, dict)):
                flat[k] = json.dumps(v)
            elif isinstance(v, bool):
                flat[k] = str(v)
            elif v is None:
                flat[k] = ""
            else:
                flat[k] = v
        G_export.add_node(node_id, **flat)

    for u, v, data in G.edges(data=True):
        flat = {}
        for k, val in data.items():
            if isinstance(val, (list, dict)):
                flat[k] = json.dumps(val)
            elif isinstance(val, bool):
                flat[k] = str(val)
            elif val is None:
                flat[k] = ""
            else:
                flat[k] = val
        G_export.add_edge(u, v, **flat)

    nx.write_graphml(G_export, str(output_path))
    logger.success(f"Exported graph to GraphML → {output_path}")
    return output_path


def export_pickle(G: nx.DiGraph, output_path: Path | None = None) -> Path:
    """Export NetworkX graph as pickle for fast Python reload."""
    if output_path is None:
        output_path = GRAPH_PICKLE

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        pickle.dump(G, f, protocol=pickle.HIGHEST_PROTOCOL)
    size_mb = output_path.stat().st_size / 1e6
    logger.success(f"Exported graph pickle ({size_mb:.1f} MB) → {output_path}")
    return output_path


def export_stats_csv(G: nx.DiGraph, output_path: Path | None = None) -> Path:
    """
    Export summary statistics to CSV.

    Includes node counts, capacity totals, regional breakdown.
    """
    if output_path is None:
        output_path = STATS_CSV

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    analysis = full_analysis(G)

    rows: list[dict[str, Any]] = []

    # Graph summary
    summary = analysis.get("graph_summary", {})
    rows.append({"metric": "total_nodes", "value": summary.get("nodes", 0), "unit": "count"})
    rows.append({"metric": "total_edges", "value": summary.get("edges", 0), "unit": "count"})
    rows.append({"metric": "total_capacity_mw", "value": summary.get("total_capacity_mw", 0), "unit": "MW"})
    rows.append({"metric": "plant_count", "value": summary.get("plant_count", 0), "unit": "count"})

    # Interconnectors
    ic = analysis.get("interconnectors", {})
    rows.append({"metric": "interconnector_count", "value": ic.get("total_interconnector_count", 0), "unit": "count"})
    rows.append({"metric": "total_interconnector_capacity_mw", "value": ic.get("total_import_capacity_mw", 0), "unit": "MW"})

    # Fuel capacity
    for fuel_row in analysis.get("fuel_capacity", []):
        rows.append({
            "metric": f"capacity_{fuel_row['fuel_type']}_mw",
            "value": fuel_row["total_capacity_mw"],
            "unit": "MW",
        })

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["metric", "value", "unit"])
        writer.writeheader()
        writer.writerows(rows)

    logger.success(f"Exported {len(rows)} stats to CSV → {output_path}")
    return output_path


def export_all(G: nx.DiGraph) -> dict[str, Path]:
    """Export graph in all formats."""
    logger.info("Exporting graph in all formats...")
    results = {
        "geojson": export_geojson(G),
        "graphml": export_graphml(G),
        "pickle": export_pickle(G),
        "stats_csv": export_stats_csv(G),
    }
    logger.success(f"All exports complete: {list(results.keys())}")
    return results
