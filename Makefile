.PHONY: setup ingest-all ingest-bmrs ingest-neso ingest-repd ingest-dukes ingest-osm \
        ingest-wri ingest-carbon ingest-interconnectors ingest-osuked \
        reconcile build-graph viz viz-map viz-network stats clean help

# ─── Variables ──────────────────────────────────────────────────────────────
PYTHON := python
UV := $(shell which uv 2>/dev/null)
PIP := pip

# ─── Help ────────────────────────────────────────────────────────────────────
help:
	@echo "UK Energy System Modelling - Available targets:"
	@echo ""
	@echo "  setup               Install dependencies and set up virtual environment"
	@echo "  ingest-all          Run ALL data ingestion sources"
	@echo "  ingest-bmrs         Ingest Elexon BMRS data"
	@echo "  ingest-neso         Ingest NESO Data Portal datasets"
	@echo "  ingest-repd         Ingest Renewable Energy Planning Database"
	@echo "  ingest-dukes        Ingest DUKES power station data"
	@echo "  ingest-osm          Ingest OpenStreetMap grid infrastructure"
	@echo "  ingest-wri          Ingest WRI Global Power Plant Database"
	@echo "  ingest-carbon       Ingest Carbon Intensity API data"
	@echo "  ingest-interconnectors  Build interconnector reference + live flows"
	@echo "  ingest-osuked       Ingest OSUKED Power Station Dictionary"
	@echo "  reconcile           Run entity reconciliation across all sources"
	@echo "  build-graph         Construct the grid topology graph"
	@echo "  viz                 Generate all visualisations"
	@echo "  viz-map             Generate interactive Folium map"
	@echo "  viz-network         Generate Plotly network diagram"
	@echo "  stats               Print summary statistics"
	@echo "  clean               Remove generated outputs"
	@echo ""

# ─── Setup ───────────────────────────────────────────────────────────────────
setup:
ifdef UV
	@echo "Using uv for dependency management..."
	uv venv .venv
	uv pip install -e ".[dev]"
else
	@echo "uv not found, using pip..."
	$(PYTHON) -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -e ".[dev]"
endif
	@echo ""
	@echo "Setup complete. Activate with: source .venv/bin/activate"
	@mkdir -p data/raw/bmrs data/raw/neso data/raw/osm data/raw/carbon_intensity \
	          data/raw/wri data/raw/osuked data/processed data/reference \
	          notebooks output logs

# ─── Ingestion ───────────────────────────────────────────────────────────────
ingest-all:
	$(PYTHON) -m uk_energy ingest --all

ingest-bmrs:
	$(PYTHON) -m uk_energy ingest --source bmrs

ingest-neso:
	$(PYTHON) -m uk_energy ingest --source neso

ingest-repd:
	$(PYTHON) -m uk_energy ingest --source repd

ingest-dukes:
	$(PYTHON) -m uk_energy ingest --source dukes

ingest-osm:
	$(PYTHON) -m uk_energy ingest --source osm

ingest-wri:
	$(PYTHON) -m uk_energy ingest --source wri

ingest-carbon:
	$(PYTHON) -m uk_energy ingest --source carbon

ingest-interconnectors:
	$(PYTHON) -m uk_energy ingest --source interconnectors

ingest-osuked:
	$(PYTHON) -m uk_energy ingest --source osuked

# ─── Pipeline ────────────────────────────────────────────────────────────────
reconcile:
	$(PYTHON) -m uk_energy reconcile

build-graph:
	$(PYTHON) -m uk_energy build-graph

# ─── Visualisation ───────────────────────────────────────────────────────────
viz: viz-map viz-network

viz-map:
	$(PYTHON) -m uk_energy viz --map

viz-network:
	$(PYTHON) -m uk_energy viz --network

# ─── Stats ───────────────────────────────────────────────────────────────────
stats:
	$(PYTHON) -m uk_energy stats

# ─── Clean ───────────────────────────────────────────────────────────────────
clean:
	@echo "Removing generated outputs..."
	rm -rf output/*.html output/*.json output/*.graphml output/*.pkl
	rm -rf data/processed/plants_unified.parquet
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "Clean complete. Raw data preserved."

clean-all: clean
	@echo "WARNING: Also removing raw data..."
	rm -rf data/raw/
	mkdir -p data/raw/bmrs data/raw/neso data/raw/osm data/raw/carbon_intensity \
	         data/raw/wri data/raw/osuked
