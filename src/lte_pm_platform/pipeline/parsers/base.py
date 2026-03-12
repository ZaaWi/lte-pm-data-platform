from __future__ import annotations

from collections.abc import Iterable, Iterator

from lte_pm_platform.domain.models import IngestSummary, NormalizedPmRecord


class BasePmParser:
    def parse(
        self,
        *,
        rows: Iterable[dict[str, str]],
        source_file: str,
        csv_name: str,
        summary: IngestSummary | None = None,
    ) -> Iterator[NormalizedPmRecord]:
        raise NotImplementedError
