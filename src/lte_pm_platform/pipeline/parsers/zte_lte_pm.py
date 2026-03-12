from __future__ import annotations

import csv
from collections.abc import Iterable, Iterator

from lte_pm_platform.domain.counter_rules import KNOWN_DIMENSION_COLUMNS, is_counter_column
from lte_pm_platform.domain.models import IngestSummary, NormalizedPmRecord
from lte_pm_platform.pipeline.ingest.file_discovery import parse_archive_filename
from lte_pm_platform.pipeline.parsers.base import BasePmParser
from lte_pm_platform.pipeline.validators.pm_row_validator import validate_required_columns
from lte_pm_platform.utils.time import parse_zte_timestamp


def parse_counter_value(raw_value: str | None) -> float | None:
    if raw_value is None:
        return None
    cleaned = raw_value.strip()
    if cleaned == "":
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def clean_dimension_value(raw_value: str | None) -> str | None:
    if raw_value is None:
        return None
    cleaned = raw_value.strip()
    if cleaned == "":
        return None
    return cleaned


class ZteLtePmParser(BasePmParser):
    def parse_csv(
        self,
        *,
        text_stream,
        source_file: str,
        csv_name: str,
        summary: IngestSummary | None = None,
    ) -> Iterator[NormalizedPmRecord]:
        reader = csv.DictReader(text_stream)
        yield from self.parse(
            rows=reader,
            source_file=source_file,
            csv_name=csv_name,
            summary=summary,
        )

    def parse(
        self,
        *,
        rows: Iterable[dict[str, str]],
        source_file: str,
        csv_name: str,
        summary: IngestSummary | None = None,
    ) -> Iterator[NormalizedPmRecord]:
        parsed_source = parse_archive_filename(source_file)
        for row in rows:
            normalized_row = {key.strip().upper(): value for key, value in row.items() if key}
            if summary is not None:
                summary.input_rows_read += 1
            validate_required_columns(normalized_row)

            collect_time = parse_zte_timestamp(normalized_row["COLLECTTIME"])
            dimensions = {
                column: clean_dimension_value(normalized_row.get(column))
                for column in KNOWN_DIMENSION_COLUMNS
            }
            if summary is not None:
                summary.unknown_columns.update(find_unknown_columns(normalized_row))

            dataset_family = parsed_source.dataset_family if parsed_source is not None else None
            interval_start = parsed_source.interval_start if parsed_source is not None else None
            revision = parsed_source.revision if parsed_source is not None else None

            for column_name, raw_value in normalized_row.items():
                if not is_counter_column(column_name):
                    continue
                counter_value = parse_counter_value(raw_value)
                if summary is not None:
                    summary.normalized_rows_emitted += 1
                    if counter_value is None:
                        summary.null_counter_values += 1
                yield NormalizedPmRecord(
                    source_file=source_file,
                    dataset_family=dataset_family,
                    interval_start=interval_start,
                    revision=revision,
                    csv_name=csv_name,
                    collect_time=collect_time,
                    trncmeid=dimensions.get("TRNCMEID"),
                    ani=dimensions.get("ANI"),
                    sbnid=dimensions.get("SBNID"),
                    enbid=dimensions.get("ENBID"),
                    enodebid=dimensions.get("ENODEBID"),
                    cellid=dimensions.get("CELLID"),
                    meid=dimensions.get("MEID"),
                    systemmode=dimensions.get("SYSTEMMODE"),
                    midflag=dimensions.get("MIDFLAG"),
                    netype=dimensions.get("NETYPE"),
                    counter_id=column_name,
                    counter_value=counter_value,
                )


def find_unknown_columns(row: dict[str, str]) -> set[str]:
    return {
        column_name
        for column_name in row
        if column_name not in KNOWN_DIMENSION_COLUMNS and not is_counter_column(column_name)
    }
