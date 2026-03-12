from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

VALID_VERIFICATION_STATUSES = {"VERIFIED", "UNVERIFIED", "UNKNOWN"}


@dataclass(frozen=True)
class CounterReferenceSeedRow:
    counter_id: str
    vendor: str
    technology: str
    object_type: str | None
    description: str | None
    unit: str | None
    source_type: str | None
    source_reference: str | None
    verification_status: str
    verified_at: datetime | None
    notes: str | None


def load_counter_reference_seed(csv_path: Path) -> list[CounterReferenceSeedRow]:
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = [parse_seed_row(row) for row in reader]
    return rows


def parse_seed_row(row: dict[str, str | None]) -> CounterReferenceSeedRow:
    verification_status = clean_optional(row.get("verification_status")) or "UNKNOWN"
    if verification_status not in VALID_VERIFICATION_STATUSES:
        raise ValueError(f"Invalid verification_status: {verification_status}")

    verified_at = parse_verified_at(row.get("verified_at"))
    if verification_status == "VERIFIED" and verified_at is None:
        verified_at = datetime.now(UTC).replace(tzinfo=None)

    counter_id = clean_required(row.get("counter_id"), "counter_id")
    vendor = clean_required(row.get("vendor"), "vendor")
    technology = clean_required(row.get("technology"), "technology")

    return CounterReferenceSeedRow(
        counter_id=counter_id,
        vendor=vendor,
        technology=technology,
        object_type=clean_optional(row.get("object_type")),
        description=clean_optional(row.get("description")),
        unit=clean_optional(row.get("unit")),
        source_type=clean_optional(row.get("source_type")),
        source_reference=clean_optional(row.get("source_reference")),
        verification_status=verification_status,
        verified_at=verified_at,
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


def parse_verified_at(value: str | None) -> datetime | None:
    cleaned = clean_optional(value)
    if cleaned is None:
        return None
    return datetime.fromisoformat(cleaned)
