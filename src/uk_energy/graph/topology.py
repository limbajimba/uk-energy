"""
topology.py — Grid topology analysis.

Analyses:
  - Graph connectivity (is GB connected? identify islands)
  - Critical nodes (articulation points / cut vertices)
  - Critical edges (bridges)
  - Regional capacity vs demand
  - Interconnector dependency
  - Max flow estimates between regions
"""

from __future__ import annotations

from typing import Any

import networkx as nx
import pandas as pd
from loguru import logger

from uk_energy.graph.model import NodeType


def analyse_connectivity(G: nx.DiGraph) -> dict[str, Any]:
    """
    Analyse overall graph connectivity.

    Returns:
      is_weakly_connected: all nodes reachable ignoring edge direction
      is_strongly_connected: all nodes reachable respecting edge direction
      num_weakly_connected_components: count of isolated subgraphs
      largest_component_nodes: size of biggest connected component
      isolated_nodes: list of node IDs with no connections
    """
    if G.number_of_nodes() == 0:
        return {"error": "Empty graph"}

    undirected = G.to_undirected()
    components = list(nx.connected_components(undirected))
    components_sorted = sorted(components, key=len, reverse=True)

    isolated = [n for n in G.nodes() if G.degree(n) == 0]

    return {
        "is_weakly_connected": nx.is_weakly_connected(G),
        "is_strongly_connected": nx.is_strongly_connected(G),
        "num_weakly_connected_components": nx.number_weakly_connected_components(G),
        "largest_component_nodes": len(components_sorted[0]) if components_sorted else 0,
        "component_sizes": [len(c) for c in components_sorted[:10]],
        "isolated_nodes": isolated[:20],  # cap for readability
        "total_isolated": len(isolated),
    }


def find_critical_nodes(G: nx.DiGraph) -> list[str]:
    """
    Find articulation points (removing which disconnects the graph).

    These are infrastructure vulnerabilities.
    """
    undirected = G.to_undirected()
    try:
        cuts = list(nx.articulation_points(undirected))
        logger.info(f"Found {len(cuts)} critical nodes (articulation points)")
        return cuts
    except Exception as exc:
        logger.warning(f"Could not compute articulation points: {exc}")
        return []


def find_critical_edges(G: nx.DiGraph) -> list[tuple[str, str]]:
    """
    Find bridges (removing which disconnects the graph).
    """
    undirected = G.to_undirected()
    try:
        bridges = list(nx.bridges(undirected))
        logger.info(f"Found {len(bridges)} critical edges (bridges)")
        return bridges
    except Exception as exc:
        logger.warning(f"Could not compute bridges: {exc}")
        return []


def regional_capacity_summary(G: nx.DiGraph) -> pd.DataFrame:
    """
    Summarise generation capacity by DNO region.

    Returns DataFrame with columns:
      dno_region, total_capacity_mw, plant_count, fuel_breakdown
    """
    rows: list[dict[str, Any]] = []

    region_data: dict[str, dict[str, Any]] = {}

    for node_id, data in G.nodes(data=True):
        if data.get("node_type") != NodeType.GENERATION_PLANT.value:
            continue

        region = data.get("dno_region") or "Unknown"
        fuel = data.get("fuel_type") or "unknown"
        capacity = data.get("capacity_mw") or 0

        if region not in region_data:
            region_data[region] = {
                "dno_region": region,
                "total_capacity_mw": 0.0,
                "plant_count": 0,
                "fuels": {},
            }

        region_data[region]["total_capacity_mw"] += capacity
        region_data[region]["plant_count"] += 1
        region_data[region]["fuels"][fuel] = (
            region_data[region]["fuels"].get(fuel, 0) + capacity
        )

    for region_info in region_data.values():
        rows.append({
            "dno_region": region_info["dno_region"],
            "total_capacity_mw": round(region_info["total_capacity_mw"], 1),
            "plant_count": region_info["plant_count"],
            "top_fuel": max(region_info["fuels"], key=region_info["fuels"].get) if region_info["fuels"] else "unknown",
        })

    df = pd.DataFrame(rows).sort_values("total_capacity_mw", ascending=False)
    return df


def interconnector_analysis(G: nx.DiGraph) -> dict[str, Any]:
    """
    Summarise UK interconnector capacity and dependency.
    """
    ic_nodes = {
        nid: data
        for nid, data in G.nodes(data=True)
        if data.get("node_type") == NodeType.INTERCONNECTOR_TERMINAL.value
        and data.get("side") == "gb"
    }

    total_import_capacity = sum(n.get("capacity_mw") or 0 for n in ic_nodes.values())

    by_country: dict[str, float] = {}
    by_id: dict[str, dict[str, Any]] = {}

    for nid, data in ic_nodes.items():
        ic_id = data.get("interconnector_id", nid)
        cap = data.get("capacity_mw") or 0
        name = data.get("interconnector_name", ic_id)
        by_id[ic_id] = {"name": name, "capacity_mw": cap}

    for _, data in G.nodes(data=True):
        if data.get("node_type") != NodeType.INTERCONNECTOR_TERMINAL.value:
            continue
        if data.get("side") == "foreign":
            country = data.get("country", "XX")
            # Find the corresponding GB terminal capacity
            ic_id = data.get("interconnector_id")
            if ic_id in by_id:
                by_country[country] = by_country.get(country, 0) + by_id[ic_id]["capacity_mw"]

    return {
        "total_interconnector_count": len(by_id),
        "total_import_capacity_mw": total_import_capacity,
        "by_interconnector": by_id,
        "by_country": by_country,
    }


def fuel_capacity_summary(G: nx.DiGraph) -> pd.DataFrame:
    """Summarise total installed capacity by fuel type."""
    fuel_data: dict[str, dict[str, Any]] = {}

    for _, data in G.nodes(data=True):
        if data.get("node_type") != NodeType.GENERATION_PLANT.value:
            continue
        fuel = data.get("fuel_type") or "unknown"
        cap = data.get("capacity_mw") or 0

        if fuel not in fuel_data:
            fuel_data[fuel] = {"fuel_type": fuel, "total_capacity_mw": 0.0, "plant_count": 0}
        fuel_data[fuel]["total_capacity_mw"] += cap
        fuel_data[fuel]["plant_count"] += 1

    df = pd.DataFrame(list(fuel_data.values()))
    if not df.empty:
        df = df.sort_values("total_capacity_mw", ascending=False)
    return df


def full_analysis(G: nx.DiGraph) -> dict[str, Any]:
    """
    Run all topology analyses and return a comprehensive report.
    """
    logger.info("Running full topology analysis...")

    connectivity = analyse_connectivity(G)
    critical_nodes = find_critical_nodes(G)[:20]  # top 20
    critical_edges = find_critical_edges(G)[:20]

    regional = regional_capacity_summary(G)
    ic_analysis = interconnector_analysis(G)
    fuel_summary = fuel_capacity_summary(G)

    metadata = G.graph.get("metadata", {})

    report = {
        "graph_summary": {
            "nodes": G.number_of_nodes(),
            "edges": G.number_of_edges(),
            "total_capacity_mw": metadata.get("total_capacity_mw"),
            "plant_count": metadata.get("plant_count"),
        },
        "connectivity": connectivity,
        "critical_nodes_sample": critical_nodes,
        "critical_edges_sample": critical_edges,
        "regional_capacity": regional.to_dict(orient="records"),
        "interconnectors": ic_analysis,
        "fuel_capacity": fuel_summary.to_dict(orient="records"),
    }

    # Log key stats
    logger.info(
        f"Topology analysis complete:\n"
        f"  Connected: {connectivity.get('is_weakly_connected')}\n"
        f"  Components: {connectivity.get('num_weakly_connected_components')}\n"
        f"  Critical nodes: {len(critical_nodes)}\n"
        f"  Interconnector capacity: {ic_analysis['total_import_capacity_mw']:,.0f} MW"
    )

    return report
