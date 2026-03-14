from __future__ import annotations

from datetime import datetime

from psycopg import Connection

from lte_pm_platform.db.repositories.semantic_kpi_repository import SemanticKpiRepository

_ALLOWED_FAMILIES = {"prb", "bler", "rrc"}
_ALLOWED_GRAINS = {"entity-time", "site-time", "region-time"}


class KpiService:
    def __init__(self, connection: Connection) -> None:
        self.repository = SemanticKpiRepository(connection)

    def list_results(
        self,
        *,
        family: str,
        grain: str,
        limit: int,
        offset: int,
        dataset_family: str | None,
        site_code: str | None,
        region_code: str | None,
        collect_time_from: datetime | None,
        collect_time_to: datetime | None,
    ) -> list[dict]:
        self._validate_family(family)
        self._validate_grain(grain)
        if grain == "entity-time" and not dataset_family:
            raise ValueError("dataset_family is required for entity-time KPI results")
        return self.repository.list_verified_kpi_results(
            family=family,
            grain=grain,
            limit=limit,
            offset=offset,
            dataset_family=dataset_family,
            site_code=site_code,
            region_code=region_code,
            collect_time_from=collect_time_from,
            collect_time_to=collect_time_to,
        )

    def list_validation(self, *, family: str, grain: str) -> list[dict]:
        self._validate_family(family)
        self._validate_grain(grain)
        return self.repository.list_verified_kpi_validation(family=family, grain=grain)

    @staticmethod
    def _validate_family(family: str) -> None:
        if family not in _ALLOWED_FAMILIES:
            raise ValueError(f"unsupported KPI family: {family}")

    @staticmethod
    def _validate_grain(grain: str) -> None:
        if grain not in _ALLOWED_GRAINS:
            raise ValueError(f"unsupported KPI grain: {grain}")
