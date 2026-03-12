"""
model.py — Pydantic models for UK grid graph nodes and edges.

Node types:
  GenerationPlant, GridSupplyPoint, Substation,
  InterconnectorTerminal, DemandZone

Edge types:
  TransmissionLine, InterconnectorCable,
  GenerationConnection, DistributionFeeder
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


# ─── Enums ───────────────────────────────────────────────────────────────────

class NodeType(str, Enum):
    GENERATION_PLANT = "generation_plant"
    GRID_SUPPLY_POINT = "grid_supply_point"
    SUBSTATION = "substation"
    INTERCONNECTOR_TERMINAL = "interconnector_terminal"
    DEMAND_ZONE = "demand_zone"


class EdgeType(str, Enum):
    TRANSMISSION_LINE = "transmission_line"
    INTERCONNECTOR_CABLE = "interconnector_cable"
    GENERATION_CONNECTION = "generation_connection"
    DISTRIBUTION_FEEDER = "distribution_feeder"


class PlantStatus(str, Enum):
    OPERATIONAL = "operational"
    CONSTRUCTION = "construction"
    CONSENTED = "consented"
    PLANNING = "planning"
    DECOMMISSIONED = "decommissioned"
    UNKNOWN = "unknown"


class SubstationType(str, Enum):
    TRANSMISSION = "transmission"
    DISTRIBUTION = "distribution"
    CONVERTER = "converter"  # HVDC converter station


class CableType(str, Enum):
    SUBSEA = "subsea"
    TUNNEL = "tunnel"
    OVERHEAD = "overhead"


# ─── Node Models ─────────────────────────────────────────────────────────────

class BaseNode(BaseModel):
    """Base fields shared by all node types."""
    node_id: str = Field(..., description="Unique node identifier")
    node_type: NodeType
    name: str = Field(..., description="Human-readable name")
    lat: float | None = Field(None, ge=-90, le=90, description="WGS84 latitude")
    lon: float | None = Field(None, ge=-180, le=180, description="WGS84 longitude")

    @property
    def has_location(self) -> bool:
        return self.lat is not None and self.lon is not None

    def to_dict(self) -> dict[str, Any]:
        """Serialise for NetworkX node attribute storage."""
        return self.model_dump(mode="json")


class GenerationPlant(BaseNode):
    """A generation asset (power station, wind farm, solar park, etc.)."""
    node_type: Literal[NodeType.GENERATION_PLANT] = NodeType.GENERATION_PLANT

    fuel_type: str = Field(..., description="Canonical fuel type (wind_onshore, solar_pv, etc.)")
    technology: str = Field("unknown", description="Detailed technology description")
    capacity_mw: float | None = Field(None, ge=0, description="Installed capacity (MW)")
    capacity_de_rated_mw: float | None = Field(None, ge=0, description="De-rated capacity (MW)")
    status: PlantStatus = PlantStatus.UNKNOWN

    bmu_ids: list[str] = Field(default_factory=list, description="BMRS BM Unit IDs")
    repd_id: str | None = None
    dukes_id: str | None = None
    wri_id: str | None = None
    osuked_id: str | None = None

    owner: str | None = None
    operator: str | None = None
    commissioned_year: int | None = None

    gsp_group: str | None = None
    dno_region: str | None = None

    # Source provenance flags
    source_bmrs: bool = False
    source_repd: bool = False
    source_dukes: bool = False
    source_wri: bool = False
    source_osuked: bool = False


class GridSupplyPoint(BaseNode):
    """A Grid Supply Point (interface between transmission and distribution)."""
    node_type: Literal[NodeType.GRID_SUPPLY_POINT] = NodeType.GRID_SUPPLY_POINT

    gsp_id: str = Field(..., description="GSP identifier (e.g. EMEB)")
    dno_region: str | None = None
    demand_mw: float | None = Field(None, description="Peak demand (MW)")
    voltage_kv: float | None = Field(None, description="Primary voltage level (kV)")


class Substation(BaseNode):
    """An electrical substation (transmission or distribution)."""
    node_type: Literal[NodeType.SUBSTATION] = NodeType.SUBSTATION

    voltage_kv: float | None = Field(None, ge=0, description="Voltage level (kV)")
    substation_type: SubstationType = SubstationType.TRANSMISSION
    osm_id: str | None = None
    operator: str | None = None


class InterconnectorTerminal(BaseNode):
    """One end of a UK electrical interconnector."""
    node_type: Literal[NodeType.INTERCONNECTOR_TERMINAL] = NodeType.INTERCONNECTOR_TERMINAL

    interconnector_id: str = Field(..., description="Interconnector identifier (e.g. IFA)")
    interconnector_name: str
    country: str = Field(..., description="ISO 3166-1 alpha-2 country code")
    side: Literal["gb", "foreign"]
    capacity_mw: float | None = None


class DemandZone(BaseNode):
    """A demand zone corresponding to a DNO distribution region."""
    node_type: Literal[NodeType.DEMAND_ZONE] = NodeType.DEMAND_ZONE

    dno_region: str
    region_id: int | None = None
    population: int | None = None
    peak_demand_mw: float | None = None
    annual_demand_gwh: float | None = None
    dno_operator: str | None = None


# ─── Edge Models ─────────────────────────────────────────────────────────────

class BaseEdge(BaseModel):
    """Base fields shared by all edge types."""
    edge_id: str = Field(..., description="Unique edge identifier")
    edge_type: EdgeType
    from_node: str = Field(..., description="Source node ID")
    to_node: str = Field(..., description="Target node ID")

    def to_dict(self) -> dict[str, Any]:
        """Serialise for NetworkX edge attribute storage."""
        return self.model_dump(mode="json")


class TransmissionLine(BaseEdge):
    """A high-voltage AC transmission line (>=132kV)."""
    edge_type: Literal[EdgeType.TRANSMISSION_LINE] = EdgeType.TRANSMISSION_LINE

    voltage_kv: float | None = Field(None, description="Line voltage (kV)")
    capacity_mw: float | None = Field(None, description="Thermal rating (MW)")
    length_km: float | None = Field(None, description="Line length (km)")
    cable_type: CableType = CableType.OVERHEAD
    circuits: int = 1
    osm_id: str | None = None
    operator: str | None = None


class InterconnectorCable(BaseEdge):
    """An HVDC interconnector cable between GB and another country."""
    edge_type: Literal[EdgeType.INTERCONNECTOR_CABLE] = EdgeType.INTERCONNECTOR_CABLE

    interconnector_id: str
    capacity_mw: float | None = None
    length_km: float | None = None
    cable_type: CableType = CableType.SUBSEA
    countries: list[str] = Field(default_factory=list)
    commissioned_year: int | None = None


class GenerationConnection(BaseEdge):
    """Connection from a generation plant to its grid connection point."""
    edge_type: Literal[EdgeType.GENERATION_CONNECTION] = EdgeType.GENERATION_CONNECTION

    capacity_mw: float | None = None
    voltage_kv: float | None = None
    connection_type: str = "direct"  # direct, aggregated


class DistributionFeeder(BaseEdge):
    """Connection from a GSP to a demand zone."""
    edge_type: Literal[EdgeType.DISTRIBUTION_FEEDER] = EdgeType.DISTRIBUTION_FEEDER

    peak_flow_mw: float | None = None
    voltage_kv: float | None = None


# ─── Graph Metadata ──────────────────────────────────────────────────────────

class GridGraphMetadata(BaseModel):
    """Metadata for the entire grid graph."""
    node_count: int = 0
    edge_count: int = 0
    plant_count: int = 0
    total_capacity_mw: float = 0.0
    interconnector_capacity_mw: float = 0.0
    gsp_count: int = 0
    substation_count: int = 0
    transmission_line_km: float = 0.0
    data_sources: list[str] = Field(default_factory=list)
    build_timestamp: str | None = None
    is_connected: bool = False
    largest_component_size: int = 0
