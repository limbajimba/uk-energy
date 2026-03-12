"""
osm.py — OpenStreetMap grid infrastructure via Overpass API.

Queries GB transmission infrastructure:
  - power=line (voltage >= 132kV)
  - power=substation
  - power=plant
  - power=generator

Uses the Overpass API with a 180-second timeout. Large queries are split
into northern/southern GB halves to avoid memory limits.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from loguru import logger

from uk_energy.config import GB_BBOX, OSM_RAW, OVERPASS_TIMEOUT_S, OVERPASS_URL
from uk_energy.ingest._http import RateLimitedClient

# GB split into two halves to avoid Overpass memory limits
GB_SOUTH: tuple[float, float, float, float] = (49.9, -8.2, 55.0, 1.8)  # S, W, N, E
GB_NORTH: tuple[float, float, float, float] = (55.0, -8.2, 60.9, 1.8)


def _out(filename: str) -> Path:
    OSM_RAW.mkdir(parents=True, exist_ok=True)
    return OSM_RAW / filename


def _bbox_str(bbox: tuple[float, float, float, float]) -> str:
    """Format bbox as Overpass-compatible string: S,W,N,E."""
    return f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"


def _build_overpass_query(
    feature_type: str,
    bbox: tuple[float, float, float, float],
    extra_filter: str = "",
    timeout: int = OVERPASS_TIMEOUT_S,
) -> str:
    """Build an Overpass QL query for a given power feature."""
    bb = _bbox_str(bbox)
    return (
        f"[out:json][timeout:{timeout}];\n"
        f"(\n"
        f'  node["power"="{feature_type}"]{extra_filter}({bb});\n'
        f'  way["power"="{feature_type}"]{extra_filter}({bb});\n'
        f'  relation["power"="{feature_type}"]{extra_filter}({bb});\n'
        f");\n"
        f"out body;\n"
        f">;\n"
        f"out skel qt;"
    )


def _query_overpass(
    client: RateLimitedClient,
    query: str,
    out_path: Path,
    force: bool = False,
) -> dict[str, Any] | None:
    """Execute an Overpass query and save results."""
    if out_path.exists() and not force:
        logger.info(f"Already exists: {out_path}")
        return json.loads(out_path.read_text())

    logger.info(f"Querying Overpass API → {out_path.name}...")
    try:
        response = client.post(OVERPASS_URL, data={"data": query})
        data: dict[str, Any] = response.json()
        element_count = len(data.get("elements", []))
        out_path.write_text(json.dumps(data, indent=2))
        logger.success(f"Got {element_count} elements → {out_path}")
        return data
    except Exception as exc:
        logger.error(f"Overpass query failed: {exc}")
        return None


def fetch_transmission_lines(force: bool = False) -> list[Path]:
    """
    Fetch transmission lines (voltage >= 132kV) from OSM.

    Splits GB into south/north halves.
    """
    paths: list[Path] = []
    with RateLimitedClient(rps=0.1, timeout=OVERPASS_TIMEOUT_S + 60) as client:
        for region, bbox in [("south", GB_SOUTH), ("north", GB_NORTH)]:
            out = _out(f"transmission_lines_{region}.json")
            # 132kV filter using regex
            query = (
                f"[out:json][timeout:{OVERPASS_TIMEOUT_S}];\n"
                f"(\n"
                f'  way["power"="line"]["voltage"~"^(132000|275000|400000|500000|132|275|400)"]'
                f'({_bbox_str(bbox)});\n'
                f");\n"
                f"out body;\n"
                f">;\n"
                f"out skel qt;"
            )
            result = _query_overpass(client, query, out, force=force)
            if result:
                paths.append(out)
            # Be polite to Overpass
            if not out.exists() or force:
                time.sleep(5)

    return paths


def fetch_substations(force: bool = False) -> Path:
    """Fetch power substations from OSM across all GB."""
    out = _out("substations.json")
    query = _build_overpass_query("substation", GB_BBOX)
    with RateLimitedClient(rps=0.1, timeout=OVERPASS_TIMEOUT_S + 60) as client:
        _query_overpass(client, query, out, force=force)
    return out


def fetch_power_plants(force: bool = False) -> Path:
    """Fetch power plants (plant + generator) from OSM across GB."""
    out = _out("power_plants.json")
    query = (
        f"[out:json][timeout:{OVERPASS_TIMEOUT_S}];\n"
        f"(\n"
        f'  node["power"="plant"]({_bbox_str(GB_BBOX)});\n'
        f'  way["power"="plant"]({_bbox_str(GB_BBOX)});\n'
        f'  relation["power"="plant"]({_bbox_str(GB_BBOX)});\n'
        f'  node["power"="generator"]({_bbox_str(GB_BBOX)});\n'
        f'  way["power"="generator"]({_bbox_str(GB_BBOX)});\n'
        f");\n"
        f"out body;\n"
        f">;\n"
        f"out skel qt;"
    )
    with RateLimitedClient(rps=0.1, timeout=OVERPASS_TIMEOUT_S + 60) as client:
        _query_overpass(client, query, out, force=force)
    return out


def parse_osm_nodes_to_geojson(
    data: dict[str, Any],
    feature_type: str,
) -> dict[str, Any]:
    """
    Convert raw Overpass JSON response to GeoJSON FeatureCollection.

    Only handles nodes with lat/lon (not ways/relations — those need
    full coordinate resolution).
    """
    features = []
    for el in data.get("elements", []):
        if el.get("type") == "node" and "lat" in el and "lon" in el:
            tags = el.get("tags", {})
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [el["lon"], el["lat"]],
                },
                "properties": {
                    "osm_id": el["id"],
                    "osm_type": "node",
                    "power": tags.get("power", feature_type),
                    "name": tags.get("name", tags.get("ref", "")),
                    "voltage": tags.get("voltage", ""),
                    "operator": tags.get("operator", ""),
                    "cables": tags.get("cables", ""),
                    "wires": tags.get("wires", ""),
                    "plant_source": tags.get("plant:source", ""),
                    "plant_output": tags.get("plant:output:electricity", ""),
                    **{k: v for k, v in tags.items()},
                },
            }
            features.append(feature)

    return {"type": "FeatureCollection", "features": features}


def save_geojson(data: dict[str, Any], path: Path) -> None:
    """Save parsed GeoJSON to disk."""
    path.write_text(json.dumps(data, indent=2))
    logger.success(f"Saved GeoJSON ({len(data.get('features', []))} features) → {path}")


def ingest_all(force: bool = False) -> None:
    """Run all OSM ingestion steps."""
    logger.info("=== OSM Ingestion ===")
    logger.info("Note: Overpass queries can take 2-5 minutes for GB scale")

    fetch_substations(force=force)
    fetch_power_plants(force=force)
    fetch_transmission_lines(force=force)

    # Convert nodes to GeoJSON for easy loading
    for json_file in OSM_RAW.glob("*.json"):
        geojson_path = OSM_RAW / json_file.name.replace(".json", "_nodes.geojson")
        if not geojson_path.exists() or force:
            try:
                data = json.loads(json_file.read_text())
                if "elements" in data:
                    geojson = parse_osm_nodes_to_geojson(data, json_file.stem)
                    if geojson["features"]:
                        save_geojson(geojson, geojson_path)
            except Exception as exc:
                logger.warning(f"Could not convert {json_file.name} to GeoJSON: {exc}")

    logger.success("OSM ingestion complete")


if __name__ == "__main__":
    ingest_all()
