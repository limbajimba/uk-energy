"""
cli.py — Click CLI for the uk-energy pipeline.

Usage:
  python -m uk_energy ingest --all
  python -m uk_energy ingest --source bmrs
  python -m uk_energy reconcile
  python -m uk_energy build-graph
  python -m uk_energy viz --map
  python -m uk_energy viz --network
  python -m uk_energy stats
"""

from __future__ import annotations

import sys
from pathlib import Path

import click
from loguru import logger

from uk_energy.config import LOG_LEVEL, LOGS_DIR, ensure_dirs

# ─── Logging Setup ────────────────────────────────────────────────────────────

def _setup_logging(verbose: bool = False) -> None:
    """Configure loguru logging to stderr + file."""
    ensure_dirs()
    logger.remove()
    level = "DEBUG" if verbose else LOG_LEVEL
    logger.add(
        sys.stderr,
        level=level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
        colorize=True,
    )
    log_file = LOGS_DIR / "uk_energy_{time:YYYY-MM-DD}.log"
    logger.add(
        str(log_file),
        level="DEBUG",
        rotation="1 day",
        retention="30 days",
        compression="gz",
    )


# ─── Main CLI Group ───────────────────────────────────────────────────────────

@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
@click.version_option(package_name="uk-energy")
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """UK Energy System Modelling — Phase 1 Pipeline."""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    _setup_logging(verbose)


# ─── Ingest ───────────────────────────────────────────────────────────────────

INGEST_SOURCES = ["bmrs", "neso", "repd", "dukes", "osm", "wri", "carbon", "interconnectors", "osuked"]


@cli.command("ingest")
@click.option("--all", "ingest_all", is_flag=True, help="Run all ingestion sources")
@click.option(
    "--source", "-s",
    type=click.Choice(INGEST_SOURCES, case_sensitive=False),
    help="Specific source to ingest",
)
@click.option("--force", "-f", is_flag=True, help="Re-download even if files exist")
@click.pass_context
def ingest_cmd(ctx: click.Context, ingest_all: bool, source: str | None, force: bool) -> None:
    """Ingest data from one or all sources."""
    _setup_logging(ctx.obj.get("verbose", False))

    if not ingest_all and not source:
        click.echo("Error: specify --all or --source <name>", err=True)
        ctx.exit(1)
        return

    sources_to_run = INGEST_SOURCES if ingest_all else [source]

    for src in sources_to_run:
        logger.info(f"Ingesting: {src}")
        try:
            if src == "bmrs":
                from uk_energy.ingest.bmrs import ingest_all as run
                run(force=force)
            elif src == "neso":
                from uk_energy.ingest.neso import ingest_all as run
                run(force=force)
            elif src == "repd":
                from uk_energy.ingest.repd import ingest_all as run
                run(force=force)
            elif src == "dukes":
                from uk_energy.ingest.dukes import ingest_all as run
                run(force=force)
            elif src == "osm":
                from uk_energy.ingest.osm import ingest_all as run
                run(force=force)
            elif src == "wri":
                from uk_energy.ingest.wri import ingest_all as run
                run(force=force)
            elif src == "carbon":
                from uk_energy.ingest.carbon_intensity import ingest_all as run
                run(force=force)
            elif src == "interconnectors":
                from uk_energy.ingest.interconnectors import ingest_all as run
                run(force=force)
            elif src == "osuked":
                from uk_energy.ingest.osuked import ingest_all as run
                run(force=force)
        except Exception as exc:
            logger.error(f"Ingestion failed for {src}: {exc}")
            if not ingest_all:
                raise

    click.echo("✅ Ingestion complete")


# ─── Reconcile ───────────────────────────────────────────────────────────────

@cli.command("reconcile")
@click.pass_context
def reconcile_cmd(ctx: click.Context) -> None:
    """Reconcile plant entities across all data sources."""
    _setup_logging(ctx.obj.get("verbose", False))
    logger.info("Starting entity reconciliation...")

    try:
        from uk_energy.reconcile.plant_matcher import reconcile_plants
        df = reconcile_plants()
        click.echo(f"✅ Reconciled {len(df)} plants")

        from uk_energy.reconcile.geocoder import geocode_plants
        df = geocode_plants(df)
        click.echo(f"✅ Geocoded {len(df)} plants")

    except Exception as exc:
        logger.error(f"Reconciliation failed: {exc}")
        raise


# ─── Build Graph ─────────────────────────────────────────────────────────────

@cli.command("build-graph")
@click.option("--export", is_flag=True, default=True, help="Export graph to all formats")
@click.pass_context
def build_graph_cmd(ctx: click.Context, export: bool) -> None:
    """Construct the UK grid topology graph."""
    _setup_logging(ctx.obj.get("verbose", False))
    logger.info("Building grid graph...")

    try:
        from uk_energy.graph.builder import build_grid_graph
        G = build_grid_graph()
        click.echo(
            f"✅ Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges"
        )

        if export:
            from uk_energy.graph.export import export_all
            paths = export_all(G)
            for fmt, path in paths.items():
                click.echo(f"   Exported {fmt}: {path}")

    except Exception as exc:
        logger.error(f"Graph build failed: {exc}")
        raise


# ─── Visualise ───────────────────────────────────────────────────────────────

@cli.command("viz")
@click.option("--map", "gen_map", is_flag=True, help="Generate Folium interactive map")
@click.option("--network", "gen_network", is_flag=True, help="Generate Plotly network diagram")
@click.option("--all", "gen_all", is_flag=True, help="Generate all visualisations")
@click.pass_context
def viz_cmd(ctx: click.Context, gen_map: bool, gen_network: bool, gen_all: bool) -> None:
    """Generate visualisations."""
    _setup_logging(ctx.obj.get("verbose", False))

    if not any([gen_map, gen_network, gen_all]):
        click.echo("Specify at least one: --map, --network, or --all", err=True)
        ctx.exit(1)
        return

    if gen_map or gen_all:
        logger.info("Generating Folium map...")
        try:
            from uk_energy.viz.map import create_map
            path = create_map()
            click.echo(f"✅ Map saved: {path}")
        except Exception as exc:
            logger.error(f"Map generation failed: {exc}")

    if gen_network or gen_all:
        logger.info("Generating network diagram...")
        try:
            from uk_energy.viz.network import create_network_diagram
            path = create_network_diagram()
            click.echo(f"✅ Network diagram saved: {path}")
        except Exception as exc:
            logger.error(f"Network diagram failed: {exc}")


# ─── Stats ────────────────────────────────────────────────────────────────────

@cli.command("stats")
@click.pass_context
def stats_cmd(ctx: click.Context) -> None:
    """Print summary statistics for the current dataset."""
    _setup_logging(ctx.obj.get("verbose", False))

    click.echo("\n" + "=" * 60)
    click.echo("  UK Energy System — Summary Statistics")
    click.echo("=" * 60)

    # Plants
    from uk_energy.config import PLANTS_UNIFIED, BMRS_RAW, NESO_RAW, WRI_RAW, OSUKED_RAW
    import json

    if PLANTS_UNIFIED.exists():
        try:
            import pandas as pd
            df = pd.read_parquet(PLANTS_UNIFIED)
            click.echo(f"\n📊 Unified Plant Database")
            click.echo(f"   Total plants:        {len(df):>8,}")
            click.echo(f"   With coordinates:    {df['lat'].notna().sum():>8,}")
            total_cap = df["capacity_mw"].sum()
            click.echo(f"   Total capacity:      {total_cap:>8,.0f} MW")
            click.echo(f"   Operational:         {(df['status']=='operational').sum():>8,}")

            click.echo(f"\n🔋 Fuel Type Breakdown (top 10 by capacity):")
            fuel_summary = (
                df.groupby("fuel_type")["capacity_mw"]
                .sum()
                .sort_values(ascending=False)
                .head(10)
            )
            for fuel, cap in fuel_summary.items():
                bar = "█" * int(min(30, cap / max(1, total_cap / 100) * 30))
                click.echo(f"   {fuel:<25} {cap:>8,.0f} MW  {bar}")

            click.echo(f"\n🗺️  Source Coverage:")
            for src in ("source_osuked", "source_wri", "source_repd", "source_bmrs", "source_dukes"):
                if src in df.columns:
                    n = df[src].sum()
                    label = src.replace("source_", "").upper()
                    click.echo(f"   {label:<12}  {n:>6,} plants")
        except Exception as exc:
            click.echo(f"  Error reading plants: {exc}")
    else:
        click.echo("  ⚠️  plants_unified.parquet not found — run reconcile first")

    # Interconnectors
    from uk_energy.config import INTERCONNECTORS_REF
    if INTERCONNECTORS_REF.exists():
        try:
            ic_data = json.loads(INTERCONNECTORS_REF.read_text())
            ics = ic_data.get("interconnectors", [])
            total_mw = sum(ic["capacity_mw"] for ic in ics)
            click.echo(f"\n🔗 Interconnectors")
            click.echo(f"   Count:               {len(ics):>8,}")
            click.echo(f"   Total capacity:      {total_mw:>8,} MW")
            for ic in ics:
                countries = " ↔ ".join(ic["countries"])
                click.echo(f"   {ic['id']:<12}  {ic['capacity_mw']:>6,} MW  ({countries})")
        except Exception:
            pass

    # Raw data inventory
    click.echo(f"\n📂 Raw Data Inventory:")
    raw_dirs = {
        "BMRS": BMRS_RAW,
        "NESO": NESO_RAW,
        "WRI": WRI_RAW,
        "OSUKED": OSUKED_RAW,
    }
    for name, d in raw_dirs.items():
        if d.exists():
            files = list(d.iterdir())
            size_kb = sum(f.stat().st_size for f in files if f.is_file()) / 1024
            click.echo(f"   {name:<10}  {len(files):>4} files  {size_kb:>8,.0f} KB")
        else:
            click.echo(f"   {name:<10}  (not downloaded)")

    click.echo("\n" + "=" * 60 + "\n")


# ─── Dashboard ────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--host", default="127.0.0.1", help="Host to bind to")
@click.option("--port", default=8050, type=int, help="Port to serve on")
@click.option("--debug/--no-debug", default=True, help="Enable debug mode")
def dashboard(host: str, port: int, debug: bool) -> None:
    """Launch the interactive dashboard."""
    from uk_energy.dashboard.app import main as run_dashboard
    click.echo(f"🚀 Starting dashboard at http://{host}:{port}")
    run_dashboard(host=host, port=port, debug=debug)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    """Entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
