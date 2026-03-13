from __future__ import annotations

from pathlib import Path

from lte_pm_platform.pipeline.ingest.semantic_kpi_seed import load_vendor_indicator_seed


def load_vendor_indicator_seed_file(*, repository, csv_path: Path) -> dict[str, object]:
    rows = load_vendor_indicator_seed(csv_path)
    payload = repository.upsert_vendor_indicators(rows)
    payload["csv"] = str(csv_path)
    return payload
