# UK Energy System Modelling

A quant-grade Python toolkit for modelling the UK electricity system — ingesting data from nine open sources, reconciling plant entities across datasets, and constructing a topological grid graph ready for power-flow ML.

## Architecture

```
uk-energy/
├── src/uk_energy/
│   ├── config.py              # All endpoints, paths, constants
│   ├── cli.py                 # Click CLI entry-point
│   ├── ingest/
│   │   ├── bmrs.py            # Elexon BMRS API (BM units, generation)
│   │   ├── neso.py            # NESO CKAN API (GSP boundaries, forecasts)
│   │   ├── repd.py            # Renewable Energy Planning Database
│   │   ├── dukes.py           # DUKES Chapter 5 power station Excel tables
│   │   ├── osm.py             # OpenStreetMap via Overpass API
│   │   ├── wri.py             # WRI Global Power Plant Database
│   │   ├── carbon_intensity.py# Carbon Intensity API (regional mix)
│   │   ├── interconnectors.py # UK interconnectors (static ref + live flows)
│   │   └── osuked.py          # OSUKED Power Station Dictionary (Rosetta Stone)
│   ├── reconcile/
│   │   ├── plant_matcher.py   # Cross-source entity matching
│   │   └── geocoder.py        # Coordinate validation + GSP/DNO assignment
│   ├── graph/
│   │   ├── model.py           # Pydantic node/edge models
│   │   ├── builder.py         # NetworkX DiGraph construction
│   │   ├── topology.py        # Graph analysis (connectivity, max-flow, etc.)
│   │   └── export.py          # GeoJSON, GraphML, pickle, CSV export
│   └── viz/
│       ├── map.py             # Folium interactive map
│       └── network.py         # Plotly network diagram
├── data/
│   ├── raw/                   # Unprocessed downloads (git-ignored)
│   ├── processed/             # Cleaned / reconciled files
│   └── reference/             # Static reference data (committed)
├── notebooks/                 # Exploratory analysis
├── output/                    # Generated maps and graphs
└── logs/                      # Execution logs
```

## Data Sources

| Source | Description | Endpoint / URL |
|--------|-------------|----------------|
| **BMRS** | Elexon BM Unit reference + live generation | `data.elexon.co.uk/bmrs/api/v1` |
| **NESO** | GSP boundaries, demand/generation forecasts | `api.neso.energy/api/3/action/` |
| **REPD** | Renewable Energy Planning Database (~10k sites) | `data.gov.uk` |
| **DUKES** | Digest of UK Energy Statistics Ch.5 power stations | `gov.uk` |
| **OSM** | Transmission lines, substations, power plants | Overpass API |
| **WRI** | Global Power Plant Database (GBR subset) | `datasets.wri.org` |
| **Carbon Intensity** | Regional generation mix (14 DNO regions) | `api.carbonintensity.org.uk` |
| **Interconnectors** | All 10 UK interconnectors (static ref + live flows) | BMRS |
| **OSUKED** | Power Station Dictionary — cross-reference IDs | GitHub |

## Quick Start

```bash
# 1. Install dependencies (uses uv if available, else pip)
make setup

# 2. Activate virtual environment
source .venv/bin/activate

# 3. Ingest all data sources
make ingest-all

# 4. Reconcile plant entities
make reconcile

# 5. Build grid graph
make build-graph

# 6. Generate visualisations
make viz

# 7. Print summary statistics
make stats
```

## CLI Reference

```bash
# Ingest
python -m uk_energy ingest --all
python -m uk_energy ingest --source bmrs
python -m uk_energy ingest --source neso
python -m uk_energy ingest --source repd
python -m uk_energy ingest --source dukes
python -m uk_energy ingest --source osm
python -m uk_energy ingest --source wri
python -m uk_energy ingest --source carbon
python -m uk_energy ingest --source interconnectors
python -m uk_energy ingest --source osuked

# Pipeline
python -m uk_energy reconcile
python -m uk_energy build-graph

# Visualise
python -m uk_energy viz --map
python -m uk_energy viz --network

# Stats
python -m uk_energy stats
```

## Output Files

| File | Description |
|------|-------------|
| `data/processed/plants_unified.parquet` | Reconciled plant database (all sources merged) |
| `data/reference/interconnectors.json` | UK interconnector metadata with lat/lon endpoints |
| `data/reference/dno_regions.json` | 14 DNO regions with operators |
| `data/reference/fuel_type_mapping.json` | Canonical fuel type taxonomy |
| `data/reference/gsp_groups.json` | GSP group codes and names |
| `output/uk_energy_map.html` | Interactive Folium map |
| `output/uk_grid_network.html` | Plotly network diagram |
| `output/uk_grid.graphml` | Graph in GraphML format |
| `output/uk_grid.pkl` | NetworkX graph pickle |
| `output/grid_stats.csv` | Summary statistics CSV |

## Graph Model

The grid graph is a **NetworkX DiGraph** with the following node/edge types:

**Nodes:**
- `GenerationPlant` — fuel type, capacity, coordinates, BMU IDs, status
- `GridSupplyPoint` — GSP ID, name, coordinates, DNO region, demand
- `Substation` — name, voltage kV, coordinates, transmission/distribution
- `InterconnectorTerminal` — country, interconnector name, coordinates
- `DemandZone` — DNO region, population, peak demand

**Edges:**
- `TransmissionLine` — voltage kV, capacity, length km
- `InterconnectorCable` — capacity, length, subsea/tunnel
- `GenerationConnection` — plant → GSP/substation
- `DistributionFeeder` — GSP → demand zone

## Entity Reconciliation

Plants are matched across sources using the **OSUKED Power Station Dictionary** as the primary cross-reference (links BMU IDs ↔ REPD IDs ↔ DUKES refs ↔ WRI IDs). For plants not in OSUKED, fuzzy name matching + spatial proximity is used.

Output: `data/processed/plants_unified.parquet` with canonical plant IDs and full provenance flags.

## Requirements

- Python 3.11+
- All data sources are free/open — no API keys required
- ~2–5 GB disk space for raw data (OSM transmission network is large)
- Overpass API queries can take 2–5 minutes for GB-scale

## Notes on Data Quality

- BMRS covers all GB BM units (>1000 units including storage, demand response)
- REPD has ~10,000 entries including planning-stage projects
- OSM transmission data is community-maintained — expect some gaps below 132kV
- DUKES table headers are complex multi-row — parsing handles this carefully
- WRI GBR subset covers ~300 plants with capacity ≥ 1 MW

## Design Principles

1. **Idempotent** — all fetchers check for existing raw files before downloading
2. **Resilient** — API failures are caught and logged; pipeline continues
3. **Typed** — Pydantic models for all data structures, full type hints
4. **Observable** — structured logging to `logs/` with timestamps
5. **Reproducible** — raw data is deterministic, processed data is derived

## Roadmap

- **Phase 2:** Power flow modelling (DC OPF, ML-augmented dispatch)
- **Phase 3:** Real-time data streaming (BMRS live feeds)
- **Phase 4:** Scenario analysis (net-zero pathways, CfD auctions)
