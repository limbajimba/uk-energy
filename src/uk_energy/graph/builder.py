"""
builder.py — Construct the UK grid topology graph from reconciled data.

Builds a NetworkX DiGraph with:
  - Generation plant nodes (from plants_unified.parquet)
  - GSP nodes (from NESO data or inferred from plants)
  - Substation nodes (from OSM)
  - Interconnector terminal nodes + cables
  - Transmission line edges (from OSM)
  - Generation connection edges (plant → nearest GSP)
  - Distribution feeder edges (GSP → demand zone)
"""

from __future__ import annotations

import json
import math
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import networkx as nx
import numpy as np
import pandas as pd
from loguru import logger

from uk_energy.config import (
    GRAPH_PICKLE,
    INTERCONNECTORS_REF,
    NESO_RAW,
    OSM_RAW,
    OUTPUT_DIR,
    PLANTS_UNIFIED,
)
from uk_energy.graph.model import (
    CableType,
    DemandZone,
    GenerationConnection,
    GenerationPlant,
    GridGraphMetadata,
    GridSupplyPoint,
    InterconnectorCable,
    InterconnectorTerminal,
    NodeType,
    PlantStatus,
    Substation,
    SubstationType,
    TransmissionLine,
)

# Approximate GSP locations (subset for bootstrap when GIS data unavailable)
# Format: {gsp_id: (lat, lon, name, dno_region)}
GSP_BOOTSTRAP: dict[str, tuple[float, float, str, str]] = {
    "EMEB": (52.6, -1.3, "East Midlands", "East Midlands"),
    "MANW": (53.5, -2.2, "Manchester West", "North West England"),
    "SEEB": (51.5, -0.1, "South East", "London"),
    "NORW": (52.6, 1.3, "Norfolk", "East England"),
    "SWEB": (50.7, -3.5, "South West", "South West England"),
    "NGET": (51.6, -1.8, "National Grid ET", "South England"),
}


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


class GridGraphBuilder:
    """Constructs the NetworkX DiGraph representation of the UK grid."""

    def __init__(self) -> None:
        self.G: nx.DiGraph = nx.DiGraph()
        self.gsp_nodes: dict[str, tuple[float, float]] = {}  # gsp_id → (lat, lon)
        self._plant_count = 0
        self._edge_count = 0

    def _add_node(self, node_id: str, attrs: dict[str, Any]) -> None:
        self.G.add_node(node_id, **attrs)

    def _add_edge(self, u: str, v: str, attrs: dict[str, Any]) -> None:
        self.G.add_edge(u, v, **attrs)
        self._edge_count += 1

    # ─── Plants ──────────────────────────────────────────────────────────────

    def add_generation_plants(self, plants_df: pd.DataFrame) -> int:
        """Add all generation plant nodes from the reconciled DataFrame."""
        count = 0
        for _, row in plants_df.iterrows():
            plant_id = str(row.get("plant_id", ""))
            if not plant_id:
                continue

            try:
                status_str = str(row.get("status", "unknown")).lower()
                try:
                    status = PlantStatus(status_str)
                except ValueError:
                    status = PlantStatus.UNKNOWN

                bmu_ids_raw = row.get("bmu_ids", [])
                if isinstance(bmu_ids_raw, str):
                    bmu_ids = [b.strip() for b in bmu_ids_raw.strip("[]'\"").split(",") if b.strip()]
                elif hasattr(bmu_ids_raw, '__len__'):
                    # Handle numpy arrays, lists, etc.
                    bmu_ids = [str(b).strip() for b in bmu_ids_raw if pd.notna(b) and str(b).strip()]
                else:
                    bmu_ids = []

                plant = GenerationPlant(
                    node_id=plant_id,
                    name=str(row.get("name", plant_id)),
                    lat=float(row["lat"]) if pd.notna(row.get("lat")) else None,
                    lon=float(row["lon"]) if pd.notna(row.get("lon")) else None,
                    fuel_type=str(row.get("fuel_type", "unknown")),
                    technology=str(row.get("technology", "unknown")),
                    capacity_mw=float(row["capacity_mw"]) if pd.notna(row.get("capacity_mw")) else None,
                    status=status,
                    bmu_ids=bmu_ids,
                    repd_id=str(row["repd_id"]) if pd.notna(row.get("repd_id")) else None,
                    wri_id=str(row["wri_id"]) if pd.notna(row.get("wri_id")) else None,
                    osuked_id=str(row["osuked_id"]) if pd.notna(row.get("osuked_id")) else None,
                    owner=str(row["owner"]) if pd.notna(row.get("owner")) else None,
                    gsp_group=str(row["gsp_group"]) if pd.notna(row.get("gsp_group")) else None,
                    dno_region=str(row["dno_region"]) if pd.notna(row.get("dno_region")) else None,
                    source_bmrs=bool(row.get("source_bmrs", False)),
                    source_repd=bool(row.get("source_repd", False)),
                    source_dukes=bool(row.get("source_dukes", False)),
                    source_wri=bool(row.get("source_wri", False)),
                    source_osuked=bool(row.get("source_osuked", False)),
                )
                self._add_node(plant_id, plant.to_dict())
                count += 1
            except Exception as exc:
                logger.debug(f"Skipping plant {plant_id}: {exc}")

        logger.info(f"Added {count} generation plant nodes")
        self._plant_count = count
        return count

    # ─── GSPs ────────────────────────────────────────────────────────────────

    def add_gsp_nodes(self) -> int:
        """
        Add Grid Supply Point nodes.

        Sources:
        1. NESO GIS data (if available)
        2. Bootstrap set (fallback)
        """
        count = 0
        gsp_data_loaded = False

        # Try NESO GIS data
        neso_gsp = NESO_RAW / "gsp_boundaries.json"
        if neso_gsp.exists():
            try:
                data = json.loads(neso_gsp.read_text())
                resources = data.get("resources", [])
                for res in resources:
                    gsp_id = str(res.get("gsp_id", res.get("id", "")))
                    name = str(res.get("name", res.get("gsp_name", gsp_id)))
                    lat = float(res.get("lat", 0) or 0)
                    lon = float(res.get("lon", 0) or 0)
                    if gsp_id and lat and lon:
                        gsp = GridSupplyPoint(
                            node_id=f"gsp_{gsp_id}",
                            gsp_id=gsp_id,
                            name=name,
                            lat=lat,
                            lon=lon,
                        )
                        self._add_node(f"gsp_{gsp_id}", gsp.to_dict())
                        self.gsp_nodes[gsp_id] = (lat, lon)
                        count += 1
                gsp_data_loaded = count > 0
            except Exception as exc:
                logger.debug(f"Could not load NESO GSP data: {exc}")

        # Bootstrap GSPs if no data loaded
        if not gsp_data_loaded:
            for gsp_id, (lat, lon, name, region) in GSP_BOOTSTRAP.items():
                gsp = GridSupplyPoint(
                    node_id=f"gsp_{gsp_id}",
                    gsp_id=gsp_id,
                    name=name,
                    lat=lat,
                    lon=lon,
                    dno_region=region,
                )
                self._add_node(f"gsp_{gsp_id}", gsp.to_dict())
                self.gsp_nodes[gsp_id] = (lat, lon)
                count += 1

        logger.info(f"Added {count} GSP nodes")
        return count

    # ─── Substations from OSM ────────────────────────────────────────────────

    def add_substations_from_osm(self) -> int:
        """Add substation nodes from OSM data."""
        count = 0
        osm_path = OSM_RAW / "substations.json"
        if not osm_path.exists():
            logger.info("No OSM substation data found")
            return 0

        try:
            data = json.loads(osm_path.read_text())
            elements = data.get("elements", [])

            for el in elements:
                if el.get("type") != "node":
                    continue
                lat = el.get("lat")
                lon = el.get("lon")
                if not lat or not lon:
                    continue

                tags = el.get("tags", {})
                osm_id = str(el["id"])
                name = tags.get("name", tags.get("ref", f"substation_{osm_id}"))
                voltage_str = tags.get("voltage", "")
                voltage_kv: float | None = None
                try:
                    voltage_kv = float(voltage_str.split(";")[0]) / 1000.0
                except (ValueError, AttributeError):
                    pass

                sub = Substation(
                    node_id=f"sub_{osm_id}",
                    name=name,
                    lat=lat,
                    lon=lon,
                    voltage_kv=voltage_kv,
                    osm_id=osm_id,
                    operator=tags.get("operator"),
                )
                self._add_node(f"sub_{osm_id}", sub.to_dict())
                count += 1

            logger.info(f"Added {count} substation nodes from OSM")
        except Exception as exc:
            logger.warning(f"Failed to load OSM substations: {exc}")

        return count

    # ─── Interconnectors ─────────────────────────────────────────────────────

    def add_interconnectors(self) -> int:
        """Add interconnector terminal nodes and cable edges."""
        if not INTERCONNECTORS_REF.exists():
            logger.warning("Interconnector reference file not found")
            return 0

        data = json.loads(INTERCONNECTORS_REF.read_text())
        interconnectors = data.get("interconnectors", [])
        count = 0

        for ic in interconnectors:
            ic_id = ic["id"]
            gb_term = ic.get("gb_terminal", {})
            foreign_term = ic.get("foreign_terminal", {})

            gb_node_id = f"ic_{ic_id}_gb"
            foreign_node_id = f"ic_{ic_id}_foreign"

            # GB terminal
            gb_terminal = InterconnectorTerminal(
                node_id=gb_node_id,
                name=f"{ic['name']} (GB: {gb_term.get('name', '')})",
                lat=gb_term.get("lat"),
                lon=gb_term.get("lon"),
                interconnector_id=ic_id,
                interconnector_name=ic["name"],
                country="GB",
                side="gb",
                capacity_mw=float(ic.get("capacity_mw", 0)),
            )
            self._add_node(gb_node_id, gb_terminal.to_dict())

            # Foreign terminal
            countries = ic.get("countries", ["GB", "XX"])
            foreign_country = next((c for c in countries if c != "GB"), "XX")
            foreign_terminal = InterconnectorTerminal(
                node_id=foreign_node_id,
                name=f"{ic['name']} ({foreign_term.get('name', '')})",
                lat=foreign_term.get("lat"),
                lon=foreign_term.get("lon"),
                interconnector_id=ic_id,
                interconnector_name=ic["name"],
                country=foreign_country,
                side="foreign",
                capacity_mw=float(ic.get("capacity_mw", 0)),
            )
            self._add_node(foreign_node_id, foreign_terminal.to_dict())

            # Cable edge (bidirectional)
            cable = InterconnectorCable(
                edge_id=f"cable_{ic_id}",
                from_node=gb_node_id,
                to_node=foreign_node_id,
                interconnector_id=ic_id,
                capacity_mw=float(ic.get("capacity_mw", 0)),
                length_km=float(ic.get("length_km", 0)) if ic.get("length_km") else None,
                cable_type=CableType(ic.get("cable_type", "subsea")),
                countries=countries,
                commissioned_year=ic.get("commissioned_year"),
            )
            self._add_edge(gb_node_id, foreign_node_id, cable.to_dict())
            self._add_edge(foreign_node_id, gb_node_id, cable.to_dict())
            count += 1

        logger.info(f"Added {count} interconnectors ({count * 2} terminal nodes, {count * 2} cable edges)")
        return count

    # ─── Transmission Lines ───────────────────────────────────────────────────

    def add_transmission_lines_from_osm(self) -> int:
        """
        Add transmission line edges from OSM data.

        Resolves OSM ways into edges by:
        1. Building a node-coordinate lookup from skel elements
        2. For each way, extracting the first and last node coordinates
        3. Snapping way endpoints to the nearest substation nodes in the graph
        4. Creating transmission line edges between connected substations
        """
        count = 0

        # Only add if we have substation nodes to connect
        substations: dict[str, tuple[float, float]] = {}
        for nid, ndata in self.G.nodes(data=True):
            if ndata.get("node_type") == NodeType.SUBSTATION.value:
                lat, lon = ndata.get("lat"), ndata.get("lon")
                if lat is not None and lon is not None:
                    substations[nid] = (float(lat), float(lon))

        if not substations:
            logger.info("No substation nodes to connect with transmission lines")
            return 0

        sub_list = list(substations.items())  # [(node_id, (lat, lon))]

        def _snap_to_substation(lat: float, lon: float, max_km: float = 10.0) -> str | None:
            """Find nearest substation within max_km, or None."""
            best_id, best_dist = None, max_km
            for sid, (slat, slon) in sub_list:
                d = _haversine(lat, lon, slat, slon)
                if d < best_dist:
                    best_id, best_dist = sid, d
            return best_id

        for region in ("south", "north"):
            osm_path = OSM_RAW / f"transmission_lines_{region}.json"
            if not osm_path.exists():
                continue

            try:
                data = json.loads(osm_path.read_text())
                elements = data.get("elements", [])

                # Build OSM node coordinate lookup from skeleton nodes
                osm_coords: dict[int, tuple[float, float]] = {}
                for el in elements:
                    if el.get("type") == "node" and "lat" in el and "lon" in el:
                        osm_coords[el["id"]] = (el["lat"], el["lon"])

                # Process ways — each way is a sequence of node refs
                ways = [e for e in elements if e.get("type") == "way"]
                for way in ways:
                    nodes = way.get("nodes", [])
                    if len(nodes) < 2:
                        continue

                    # Get first and last node coordinates
                    first_id, last_id = nodes[0], nodes[-1]
                    first_coord = osm_coords.get(first_id)
                    last_coord = osm_coords.get(last_id)

                    if not first_coord or not last_coord:
                        continue

                    # Snap to nearest substations
                    from_sub = _snap_to_substation(first_coord[0], first_coord[1])
                    to_sub = _snap_to_substation(last_coord[0], last_coord[1])

                    if not from_sub or not to_sub or from_sub == to_sub:
                        continue

                    # Avoid duplicate edges
                    if self.G.has_edge(from_sub, to_sub):
                        continue

                    tags = way.get("tags", {})
                    voltage_str = tags.get("voltage", "")
                    voltage_kv: float | None = None
                    try:
                        voltage_kv = float(voltage_str.split(";")[0]) / 1000.0
                    except (ValueError, AttributeError, IndexError):
                        pass

                    # Estimate line length from endpoint coords
                    length_km = _haversine(
                        first_coord[0], first_coord[1],
                        last_coord[0], last_coord[1],
                    )

                    osm_id = str(way.get("id", ""))
                    line = TransmissionLine(
                        edge_id=f"tline_{osm_id}",
                        from_node=from_sub,
                        to_node=to_sub,
                        voltage_kv=voltage_kv,
                        length_km=round(length_km, 2),
                        cable_type=CableType.OVERHEAD,
                        osm_id=osm_id,
                        operator=tags.get("operator"),
                    )
                    self._add_edge(from_sub, to_sub, line.to_dict())
                    # Also add reverse direction (transmission is bidirectional)
                    self._add_edge(to_sub, from_sub, line.to_dict())
                    count += 1

                logger.info(f"OSM {region}: {len(ways)} ways → {count} transmission line edges")
            except Exception as exc:
                logger.warning(f"Could not process OSM transmission lines ({region}): {exc}")

        logger.info(f"Added {count} transmission line edges from OSM")
        return count

    # ─── Generation → GSP Connections ────────────────────────────────────────

    def connect_plants_to_gsps(self) -> int:
        """
        Connect each generation plant to its nearest GSP.

        Uses gsp_group attribute if available, otherwise spatial proximity.
        """
        if not self.gsp_nodes:
            logger.warning("No GSP nodes available — skipping plant-GSP connections")
            return 0

        count = 0
        gsp_coords = list(self.gsp_nodes.items())  # [(gsp_id, (lat, lon))]

        for node_id, data in self.G.nodes(data=True):
            if data.get("node_type") != NodeType.GENERATION_PLANT.value:
                continue

            plant_lat = data.get("lat")
            plant_lon = data.get("lon")
            if plant_lat is None or plant_lon is None:
                continue

            # Check if gsp_group is already assigned
            gsp_id = data.get("gsp_group")
            if gsp_id and gsp_id in self.gsp_nodes:
                target_gsp = f"gsp_{gsp_id}"
            else:
                # Find nearest GSP by distance
                nearest_gsp_id, _ = min(
                    gsp_coords,
                    key=lambda x: _haversine(plant_lat, plant_lon, x[1][0], x[1][1]),
                )
                target_gsp = f"gsp_{nearest_gsp_id}"

            if target_gsp not in self.G:
                continue

            conn = GenerationConnection(
                edge_id=f"genconn_{node_id}",
                from_node=node_id,
                to_node=target_gsp,
                capacity_mw=data.get("capacity_mw"),
            )
            self._add_edge(node_id, target_gsp, conn.to_dict())
            count += 1

        logger.info(f"Connected {count} plants to GSPs")
        return count

    # ─── Demand Zones ────────────────────────────────────────────────────────

    def add_demand_zones(self) -> int:
        """Add demand zone nodes for each DNO region."""
        from uk_energy.config import DNO_REGION_IDS

        # Approximate DNO region centroids
        dno_centroids: dict[str, tuple[float, float]] = {
            "North Scotland": (57.5, -4.0),
            "South Scotland": (55.8, -3.5),
            "North West England": (53.7, -2.5),
            "North East England": (54.8, -1.5),
            "Yorkshire": (53.8, -1.5),
            "North Wales & Mersey": (53.1, -3.0),
            "South Wales": (51.7, -3.5),
            "West Midlands": (52.5, -2.0),
            "East Midlands": (52.7, -1.0),
            "East England": (52.5, 0.5),
            "South West England": (51.0, -3.5),
            "South England": (51.2, -1.5),
            "London": (51.5, -0.1),
            "South East England": (51.2, 0.5),
        }

        count = 0
        for region_id, region_name in DNO_REGION_IDS.items():
            centroid = dno_centroids.get(region_name, (54.0, -2.0))
            zone = DemandZone(
                node_id=f"dno_{region_id}",
                name=region_name,
                lat=centroid[0],
                lon=centroid[1],
                dno_region=region_name,
                region_id=region_id,
            )
            self._add_node(f"dno_{region_id}", zone.to_dict())
            count += 1

        logger.info(f"Added {count} demand zone nodes")
        return count

    # ─── Derived Attributes ───────────────────────────────────────────────────

    def compute_derived_attributes(self) -> None:
        """Compute and attach aggregate attributes to GSP nodes."""
        # Total capacity connected to each GSP
        gsp_capacity: dict[str, float] = {}

        for u, v, data in self.G.edges(data=True):
            if data.get("edge_type") == "generation_connection":
                cap = data.get("capacity_mw") or 0
                gsp_capacity[v] = gsp_capacity.get(v, 0) + cap

        for gsp_node, total_cap in gsp_capacity.items():
            if gsp_node in self.G:
                self.G.nodes[gsp_node]["total_connected_generation_mw"] = total_cap

        # Regional capacity totals
        regional_cap: dict[str, float] = {}
        for nid, data in self.G.nodes(data=True):
            if data.get("node_type") == NodeType.GENERATION_PLANT.value:
                region = data.get("dno_region", "unknown")
                cap = data.get("capacity_mw") or 0
                regional_cap[region] = regional_cap.get(region, 0) + cap

        self.G.graph["regional_capacity_mw"] = regional_cap
        logger.info(f"Computed derived attributes for {len(gsp_capacity)} GSPs")

    # ─── Build ────────────────────────────────────────────────────────────────

    def build(self, plants_df: pd.DataFrame | None = None) -> nx.DiGraph:
        """
        Main build method. Constructs the full grid graph.
        """
        logger.info("Building UK grid topology graph...")

        self.G = nx.DiGraph()
        self.G.graph["name"] = "UK Grid Topology"
        self.G.graph["build_timestamp"] = datetime.now(tz=timezone.utc).isoformat()

        # Load plants
        if plants_df is None:
            if PLANTS_UNIFIED.exists():
                plants_df = pd.read_parquet(PLANTS_UNIFIED)
                logger.info(f"Loaded {len(plants_df)} plants from parquet")
            else:
                logger.warning("No plants data found")
                plants_df = pd.DataFrame()

        # Add nodes
        self.add_gsp_nodes()
        self.add_demand_zones()
        if not plants_df.empty:
            self.add_generation_plants(plants_df)
        self.add_substations_from_osm()
        self.add_interconnectors()

        # Add edges
        self.connect_plants_to_gsps()
        self.add_transmission_lines_from_osm()

        # Derived attributes
        self.compute_derived_attributes()

        # Metadata
        meta = GridGraphMetadata(
            node_count=self.G.number_of_nodes(),
            edge_count=self.G.number_of_edges(),
            plant_count=self._plant_count,
            total_capacity_mw=sum(
                d.get("capacity_mw") or 0
                for _, d in self.G.nodes(data=True)
                if d.get("node_type") == NodeType.GENERATION_PLANT.value
            ),
            interconnector_capacity_mw=sum(
                d.get("capacity_mw") or 0
                for _, d in self.G.nodes(data=True)
                if d.get("node_type") == NodeType.INTERCONNECTOR_TERMINAL.value
                and d.get("side") == "gb"
            ),
            gsp_count=sum(
                1 for _, d in self.G.nodes(data=True)
                if d.get("node_type") == NodeType.GRID_SUPPLY_POINT.value
            ),
            data_sources=["OSUKED", "WRI", "REPD", "BMRS", "OSM", "NESO"],
            build_timestamp=self.G.graph["build_timestamp"],
            is_connected=nx.is_weakly_connected(self.G) if self.G.number_of_nodes() > 0 else False,
        )
        self.G.graph["metadata"] = meta.model_dump()

        logger.success(
            f"Graph built: {meta.node_count} nodes, {meta.edge_count} edges, "
            f"{meta.plant_count} plants, {meta.total_capacity_mw:,.0f} MW total capacity"
        )
        return self.G


def build_grid_graph(plants_df: pd.DataFrame | None = None) -> nx.DiGraph:
    """Convenience function: build and return the grid graph."""
    builder = GridGraphBuilder()
    return builder.build(plants_df=plants_df)
