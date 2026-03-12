"""
neso.py — NESO (National Energy System Operator) Data Portal ingestion.

Uses the CKAN API to discover and download datasets:
  - GSP (Grid Supply Point) GIS boundaries
  - Demand forecasts
  - Generation forecasts

No authentication required.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

from uk_energy.config import (
    NESO_CKAN_BASE,
    NESO_PACKAGE_SEARCH,
    NESO_PACKAGE_SHOW,
    NESO_RESOURCE_SHOW,
    NESO_RAW,
)
from uk_energy.ingest._http import RateLimitedClient

# Known NESO dataset IDs/slugs
GSP_BOUNDARIES_DATASET = "gis-boundaries-gb-grid-supply-points"
DEMAND_FORECAST_DATASET = "demand-forecast"
GENERATION_FORECAST_DATASET = "generation-forecast"


def _out(filename: str) -> Path:
    NESO_RAW.mkdir(parents=True, exist_ok=True)
    return NESO_RAW / filename


def _ckan_request(
    client: RateLimitedClient,
    action: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Make a CKAN API request and return the result dict."""
    url = f"{NESO_CKAN_BASE}/{action}"
    response = client.get(url, params=params)
    data: dict[str, Any] = response.json()
    if not data.get("success"):
        raise RuntimeError(f"CKAN API error for {action}: {data.get('error')}")
    return data["result"]  # type: ignore[return-value]


def fetch_gsp_boundaries(force: bool = False) -> Path:
    """
    Fetch GSP GIS boundary data from NESO Data Portal.

    The GSP boundaries define the geographic areas for each of the ~400
    Grid Supply Points in GB.
    """
    out = _out("gsp_boundaries.json")
    if out.exists() and not force:
        logger.info(f"GSP boundaries already downloaded: {out}")
        return out

    logger.info("Fetching GSP boundaries from NESO CKAN...")
    with RateLimitedClient(rps=0.5) as client:
        try:
            # Search for the dataset
            result = _ckan_request(
                client, "package_search",
                {"q": "gis boundaries grid supply points", "rows": 10},
            )
            packages = result.get("results", [])
            logger.debug(f"Found {len(packages)} matching datasets")

            # Try to get direct package
            pkg = None
            for p in packages:
                name = p.get("name", "")
                if "gsp" in name.lower() or "grid-supply" in name.lower():
                    pkg = p
                    break

            if pkg is None:
                # Try direct package_show
                try:
                    pkg = _ckan_request(
                        client, "package_show",
                        {"id": GSP_BOUNDARIES_DATASET},
                    )
                except Exception:
                    logger.warning("Could not find GSP boundaries dataset by slug")
                    pkg = packages[0] if packages else None

            if pkg is None:
                logger.warning("No GSP boundaries dataset found, saving empty placeholder")
                out.write_text(json.dumps({"status": "not_found", "resources": []}))
                return out

            # Find a GeoJSON or CSV resource
            resources = pkg.get("resources", [])
            out.write_text(json.dumps({"dataset": pkg, "resources": resources}, indent=2))
            logger.success(f"Saved GSP boundaries metadata → {out} ({len(resources)} resources)")

            # Try to download the actual GeoJSON if available
            for res in resources:
                fmt = res.get("format", "").upper()
                url = res.get("url", "")
                if fmt in ("GEOJSON", "JSON", "CSV", "ZIP") and url:
                    try:
                        logger.info(f"Downloading GSP resource: {url}")
                        dl_response = client.get(url)
                        ext = fmt.lower()
                        dl_path = _out(f"gsp_boundaries_data.{ext}")
                        dl_path.write_bytes(dl_response.content)
                        logger.success(f"Downloaded GSP data → {dl_path}")
                        break
                    except Exception as exc:
                        logger.warning(f"Could not download resource {url}: {exc}")

        except Exception as exc:
            logger.error(f"Failed to fetch GSP boundaries: {exc}")
            out.write_text(json.dumps({"status": "error", "error": str(exc)}))

    return out


def fetch_demand_forecast(force: bool = False) -> Path:
    """
    Fetch demand forecast data from NESO.
    """
    out = _out("demand_forecast.json")
    if out.exists() and not force:
        logger.info(f"Demand forecast already downloaded: {out}")
        return out

    logger.info("Fetching demand forecast from NESO CKAN...")
    with RateLimitedClient(rps=0.5) as client:
        try:
            result = _ckan_request(
                client, "package_search",
                {"q": "demand forecast", "rows": 20},
            )
            packages = result.get("results", [])
            out.write_text(json.dumps({"packages": packages}, indent=2))
            logger.success(f"Saved demand forecast metadata ({len(packages)} datasets) → {out}")
        except Exception as exc:
            logger.error(f"Failed to fetch demand forecast: {exc}")
            out.write_text(json.dumps({"status": "error", "error": str(exc)}))

    return out


def fetch_generation_forecast(force: bool = False) -> Path:
    """
    Fetch generation forecast data from NESO.
    """
    out = _out("generation_forecast.json")
    if out.exists() and not force:
        logger.info(f"Generation forecast already downloaded: {out}")
        return out

    logger.info("Fetching generation forecast from NESO CKAN...")
    with RateLimitedClient(rps=0.5) as client:
        try:
            result = _ckan_request(
                client, "package_search",
                {"q": "generation forecast renewable", "rows": 20},
            )
            packages = result.get("results", [])
            out.write_text(json.dumps({"packages": packages}, indent=2))
            logger.success(f"Saved generation forecast metadata ({len(packages)} datasets) → {out}")
        except Exception as exc:
            logger.error(f"Failed to fetch generation forecast: {exc}")
            out.write_text(json.dumps({"status": "error", "error": str(exc)}))

    return out


def fetch_neso_catalogue(force: bool = False) -> Path:
    """
    Fetch full NESO data catalogue for discovery.
    """
    out = _out("neso_catalogue.json")
    if out.exists() and not force:
        logger.info(f"NESO catalogue already downloaded: {out}")
        return out

    logger.info("Fetching NESO full data catalogue...")
    with RateLimitedClient(rps=0.5) as client:
        try:
            result = _ckan_request(
                client, "package_search",
                {"q": "*:*", "rows": 200, "start": 0},
            )
            out.write_text(json.dumps(result, indent=2))
            count = result.get("count", "?")
            logger.success(f"Saved NESO catalogue ({count} datasets) → {out}")
        except Exception as exc:
            logger.error(f"Failed to fetch NESO catalogue: {exc}")
            out.write_text(json.dumps({"status": "error", "error": str(exc)}))

    return out


def ingest_all(force: bool = False) -> None:
    """Run all NESO ingestion steps."""
    logger.info("=== NESO Ingestion ===")
    fetch_neso_catalogue(force=force)
    fetch_gsp_boundaries(force=force)
    fetch_demand_forecast(force=force)
    fetch_generation_forecast(force=force)
    logger.success("NESO ingestion complete")


if __name__ == "__main__":
    ingest_all()
