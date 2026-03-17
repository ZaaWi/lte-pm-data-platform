from __future__ import annotations

from psycopg import Connection

from lte_pm_platform.db.repositories.topology_reference_repository import TopologyReferenceRepository


class TopologyService:
    def __init__(self, connection: Connection) -> None:
        self.repository = TopologyReferenceRepository(connection)

    def list_unmapped_entities(self, *, limit: int) -> list[dict]:
        return self.repository.list_unmapped_entities(limit=limit)

    def summarize_site_coverage(self, *, limit: int) -> list[dict]:
        return self.repository.summarize_site_coverage(limit=limit)

    def summarize_region_coverage(self, *, limit: int) -> list[dict]:
        return self.repository.summarize_region_coverage(limit=limit)

    def summarize_topology_overview(self) -> dict:
        return self.repository.summarize_topology_overview()
