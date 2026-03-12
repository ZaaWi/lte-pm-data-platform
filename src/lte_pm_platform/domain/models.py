from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class NormalizedPmRecord:
    source_file: str
    dataset_family: str | None
    interval_start: datetime | None
    revision: int | None
    csv_name: str
    collect_time: datetime
    trncmeid: str | None
    ani: str | None
    sbnid: str | None
    enbid: str | None
    enodebid: str | None
    cellid: str | None
    meid: str | None
    systemmode: str | None
    midflag: str | None
    netype: str | None
    counter_id: str
    counter_value: float | None


@dataclass
class IngestSummary:
    source_file: str
    run_id: UUID
    trigger_type: str
    source_type: str
    file_hash: str | None = None
    csv_files_found: int = 0
    input_rows_read: int = 0
    normalized_rows_emitted: int = 0
    rows_inserted: int = 0
    unknown_columns: set[str] = field(default_factory=set)
    null_counter_values: int = 0
    status: str = "PENDING"
    error_message: str | None = None
    lifecycle_status: str = "PENDING"
    lifecycle_action: str | None = None
    normalization_status: str = "PENDING"
    final_file_path: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_file": self.source_file,
            "run_id": str(self.run_id),
            "trigger_type": self.trigger_type,
            "source_type": self.source_type,
            "file_hash": self.file_hash,
            "csv_files_found": self.csv_files_found,
            "input_rows_read": self.input_rows_read,
            "normalized_rows_emitted": self.normalized_rows_emitted,
            "rows_inserted": self.rows_inserted,
            "unknown_columns": sorted(self.unknown_columns),
            "null_counter_values": self.null_counter_values,
            "status": self.status,
            "error_message": self.error_message,
            "lifecycle_status": self.lifecycle_status,
            "lifecycle_action": self.lifecycle_action,
            "normalization_status": self.normalization_status,
            "final_file_path": self.final_file_path,
        }
