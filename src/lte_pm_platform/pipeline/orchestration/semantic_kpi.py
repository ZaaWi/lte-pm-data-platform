from __future__ import annotations

from pathlib import Path

from lte_pm_platform.pipeline.ingest.semantic_kpi_seed import (
    load_counter_dictionary_seed,
    load_kpi_definition_seed,
)


def load_counter_dictionary(*, repository, csv_path: Path) -> dict[str, object]:
    rows = load_counter_dictionary_seed(csv_path)
    payload = repository.upsert_counter_dictionary(rows)
    payload["csv"] = str(csv_path)
    return payload


def load_kpi_definitions(*, repository, csv_path: Path) -> dict[str, object]:
    rows = load_kpi_definition_seed(csv_path)
    payload = repository.upsert_kpi_definitions(rows)
    payload["csv"] = str(csv_path)
    return payload
