from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

VALID_VERIFICATION_STATUSES = {"VERIFIED", "PROVISIONAL", "UNVERIFIED", "UNKNOWN"}


@dataclass(frozen=True)
class SemanticCounterDictionarySeedRow:
    dataset_family: str
    counter_id: str
    counter_alias: str
    counter_name: str
    unit: str | None
    aggregation_behavior: str
    verification_status: str
    source_note: str | None
    group_code: str | None
    group_name: str | None
    group_notes: str | None


@dataclass(frozen=True)
class SemanticKpiDefinitionSeedRow:
    kpi_code: str
    kpi_name: str
    formula_expression: str
    grain: str
    unit: str | None
    verification_status: str
    topology_rollup_allowed: bool
    notes: str | None
    input_alias: str | None
    dataset_family: str | None
    counter_alias: str | None
    required: bool
    input_notes: str | None


@dataclass(frozen=True)
class VendorIndicatorSeedRow:
    indicator_code: str
    indicator_name: str
    semantic_alias: str
    aggregation_method: str
    unit: str | None
    verification_status: str
    source: str
    lineage_expression: str
    lineage_type: str
    raw_counter_dependencies: str


def load_counter_dictionary_seed(csv_path: Path) -> list[SemanticCounterDictionarySeedRow]:
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return [parse_counter_dictionary_row(row) for row in reader]


def load_kpi_definition_seed(csv_path: Path) -> list[SemanticKpiDefinitionSeedRow]:
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return [parse_kpi_definition_row(row) for row in reader]


def load_vendor_indicator_seed(csv_path: Path) -> list[VendorIndicatorSeedRow]:
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return [parse_vendor_indicator_row(row, csv_path=csv_path) for row in reader]


def parse_counter_dictionary_row(row: dict[str, str | None]) -> SemanticCounterDictionarySeedRow:
    verification_status = clean_optional(row.get("verification_status")) or "UNKNOWN"
    if verification_status not in VALID_VERIFICATION_STATUSES:
        raise ValueError(f"Invalid verification_status: {verification_status}")

    return SemanticCounterDictionarySeedRow(
        dataset_family=clean_required(row.get("dataset_family"), "dataset_family"),
        counter_id=clean_required(row.get("counter_id"), "counter_id"),
        counter_alias=clean_required(row.get("counter_alias"), "counter_alias"),
        counter_name=clean_required(row.get("counter_name"), "counter_name"),
        unit=clean_optional(row.get("unit")),
        aggregation_behavior=clean_required(
            row.get("aggregation_behavior"),
            "aggregation_behavior",
        ),
        verification_status=verification_status,
        source_note=clean_optional(row.get("source_note")),
        group_code=clean_optional(row.get("group_code")),
        group_name=clean_optional(row.get("group_name")),
        group_notes=clean_optional(row.get("group_notes")),
    )


def parse_kpi_definition_row(row: dict[str, str | None]) -> SemanticKpiDefinitionSeedRow:
    verification_status = clean_optional(row.get("verification_status")) or "UNKNOWN"
    if verification_status not in VALID_VERIFICATION_STATUSES:
        raise ValueError(f"Invalid verification_status: {verification_status}")

    input_alias = clean_optional(row.get("input_alias"))
    dataset_family = clean_optional(row.get("dataset_family"))
    counter_alias = clean_optional(row.get("counter_alias"))

    if input_alias is not None and dataset_family is None:
        raise ValueError("dataset_family is required when input_alias is provided")
    if input_alias is not None and counter_alias is None:
        raise ValueError("counter_alias is required when input_alias is provided")
    if input_alias is None and (dataset_family is not None or counter_alias is not None):
        raise ValueError("input_alias is required when dataset_family or counter_alias is provided")

    return SemanticKpiDefinitionSeedRow(
        kpi_code=clean_required(row.get("kpi_code"), "kpi_code"),
        kpi_name=clean_required(row.get("kpi_name"), "kpi_name"),
        formula_expression=clean_required(row.get("formula_expression"), "formula_expression"),
        grain=clean_required(row.get("grain"), "grain"),
        unit=clean_optional(row.get("unit")),
        verification_status=verification_status,
        topology_rollup_allowed=parse_bool(row.get("topology_rollup_allowed")),
        notes=clean_optional(row.get("notes")),
        input_alias=input_alias,
        dataset_family=dataset_family,
        counter_alias=counter_alias,
        required=parse_bool(row.get("required"), default=True),
        input_notes=clean_optional(row.get("input_notes")),
    )


def parse_vendor_indicator_row(
    row: dict[str, str | None],
    *,
    csv_path: Path,
) -> VendorIndicatorSeedRow:
    verification_status = clean_optional(row.get("verification_status")) or "VERIFIED"
    if verification_status not in VALID_VERIFICATION_STATUSES:
        raise ValueError(f"Invalid verification_status: {verification_status}")

    lineage_expression = clean_required(
        row.get("counter_lineage_expression") or row.get("lineage_expression"),
        "counter_lineage_expression",
    )

    raw_counter_dependencies = clean_optional(row.get("raw_counter_dependencies"))
    if raw_counter_dependencies is None:
        raw_counter_dependencies = "|".join(extract_raw_counter_dependencies(lineage_expression))

    lineage_type = clean_optional(row.get("lineage_type"))
    if lineage_type is None:
        lineage_type = infer_lineage_type(raw_counter_dependencies)

    source = clean_optional(row.get("source")) or csv_path.name

    return VendorIndicatorSeedRow(
        indicator_code=clean_required(
            row.get("vendor_indicator_code") or row.get("indicator_code"),
            "vendor_indicator_code",
        ),
        indicator_name=clean_required(
            row.get("vendor_indicator_name") or row.get("indicator_name"),
            "vendor_indicator_name",
        ),
        semantic_alias=clean_required(
            row.get("proposed_counter_alias") or row.get("semantic_alias"),
            "proposed_counter_alias",
        ),
        aggregation_method=clean_required(row.get("aggregation_method"), "aggregation_method"),
        unit=clean_optional(row.get("unit")),
        verification_status=verification_status,
        source=source,
        lineage_expression=lineage_expression,
        lineage_type=lineage_type,
        raw_counter_dependencies=raw_counter_dependencies,
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


def parse_bool(value: str | None, *, default: bool = False) -> bool:
    cleaned = clean_optional(value)
    if cleaned is None:
        return default
    normalized = cleaned.lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"Invalid boolean value: {cleaned}")


def extract_raw_counter_dependencies(lineage_expression: str) -> list[str]:
    tokens = [token.strip() for token in lineage_expression.split("|") if token.strip()]
    return tokens


def infer_lineage_type(raw_counter_dependencies: str) -> str:
    tokens = [token for token in raw_counter_dependencies.split("|") if token]
    if len(tokens) <= 1:
        return "direct"
    return "composed"
