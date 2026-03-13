from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TopologyRegionSeedRow:
    region_code: str
    region_name: str
    notes: str | None


@dataclass(frozen=True)
class TopologySiteSeedRow:
    site_code: str
    site_name: str | None
    region_code: str | None
    notes: str | None


@dataclass(frozen=True)
class TopologyReportingSeedRow:
    reporting_key: str
    reporting_name: str
    reporting_level: str | None
    parent_reporting_key: str | None
    notes: str | None


@dataclass(frozen=True)
class TopologyEntitySiteMapSeedRow:
    logical_entity_key: str
    site_code: str | None
    reporting_key: str | None
    mapping_source: str | None
    notes: str | None


def load_topology_region_seed(csv_path: Path) -> list[TopologyRegionSeedRow]:
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return [parse_region_row(row) for row in reader]


def load_topology_site_seed(csv_path: Path) -> list[TopologySiteSeedRow]:
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return [parse_site_row(row) for row in reader]


def load_topology_reporting_seed(csv_path: Path) -> list[TopologyReportingSeedRow]:
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return [parse_reporting_row(row) for row in reader]


def load_topology_entity_site_map_seed(csv_path: Path) -> list[TopologyEntitySiteMapSeedRow]:
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return [parse_entity_site_map_row(row) for row in reader]


def parse_region_row(row: dict[str, str | None]) -> TopologyRegionSeedRow:
    return TopologyRegionSeedRow(
        region_code=clean_required(row.get("region_code"), "region_code"),
        region_name=clean_required(row.get("region_name"), "region_name"),
        notes=clean_optional(row.get("notes")),
    )


def parse_site_row(row: dict[str, str | None]) -> TopologySiteSeedRow:
    return TopologySiteSeedRow(
        site_code=clean_required(row.get("site_code"), "site_code"),
        site_name=clean_optional(row.get("site_name")),
        region_code=clean_optional(row.get("region_code")),
        notes=clean_optional(row.get("notes")),
    )


def parse_reporting_row(row: dict[str, str | None]) -> TopologyReportingSeedRow:
    return TopologyReportingSeedRow(
        reporting_key=clean_required(row.get("reporting_key"), "reporting_key"),
        reporting_name=clean_required(row.get("reporting_name"), "reporting_name"),
        reporting_level=clean_optional(row.get("reporting_level")),
        parent_reporting_key=clean_optional(row.get("parent_reporting_key")),
        notes=clean_optional(row.get("notes")),
    )


def parse_entity_site_map_row(row: dict[str, str | None]) -> TopologyEntitySiteMapSeedRow:
    return TopologyEntitySiteMapSeedRow(
        logical_entity_key=clean_required(row.get("logical_entity_key"), "logical_entity_key"),
        site_code=clean_optional(row.get("site_code")),
        reporting_key=clean_optional(row.get("reporting_key")),
        mapping_source=clean_optional(row.get("mapping_source")),
        notes=clean_optional(row.get("notes")),
    )


def clean_required(value: str | None, field_name: str) -> str:
    cleaned = clean_optional(value)
    if cleaned is None:
        raise ValueError(f"Missing required field: {field_name}")
    return cleaned


def clean_optional(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None
