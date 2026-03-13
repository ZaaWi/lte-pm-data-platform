from __future__ import annotations

from pathlib import Path

from lte_pm_platform.pipeline.ingest.topology_reference_seed import (
    load_topology_entity_site_map_seed,
    load_topology_region_seed,
    load_topology_reporting_seed,
    load_topology_site_seed,
)


def load_topology_regions(*, repository, csv_path: Path) -> dict[str, object]:
    rows = load_topology_region_seed(csv_path)
    inserted = repository.upsert_regions(rows)
    return {"rows_loaded": inserted, "csv": str(csv_path)}


def load_topology_sites(*, repository, csv_path: Path) -> dict[str, object]:
    rows = load_topology_site_seed(csv_path)
    inserted = repository.upsert_sites(rows)
    return {"rows_loaded": inserted, "csv": str(csv_path)}


def load_topology_reporting(*, repository, csv_path: Path) -> dict[str, object]:
    rows = load_topology_reporting_seed(csv_path)
    inserted = repository.upsert_reporting_hierarchy(rows)
    return {"rows_loaded": inserted, "csv": str(csv_path)}


def load_topology_entity_site_map(*, repository, csv_path: Path) -> dict[str, object]:
    rows = load_topology_entity_site_map_seed(csv_path)
    inserted = repository.upsert_entity_site_mappings(rows)
    return {"rows_loaded": inserted, "csv": str(csv_path)}


def sync_topology_enrichment(*, repository) -> dict[str, object]:
    rows_synced = repository.refresh_topology_enrichment()
    return {"rows_synced": rows_synced}
