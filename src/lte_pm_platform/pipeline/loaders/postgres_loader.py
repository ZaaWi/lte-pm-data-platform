from __future__ import annotations

from collections.abc import Iterable

from psycopg import Connection

from lte_pm_platform.db.repositories.pm_sample_repository import PmSampleRepository
from lte_pm_platform.domain.models import NormalizedPmRecord


class PostgresLoader:
    def __init__(self, connection: Connection, batch_size: int = 1000) -> None:
        self.connection = connection
        self.repository = PmSampleRepository(connection)
        self.batch_size = batch_size

    def load(self, records: Iterable[NormalizedPmRecord]) -> int:
        total = 0
        batch: list[NormalizedPmRecord] = []
        for record in records:
            batch.append(record)
            if len(batch) >= self.batch_size:
                total += self.repository.insert_batch(batch)
                batch = []
        if batch:
            total += self.repository.insert_batch(batch)
        return total

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()
