from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from psycopg import Connection

from lte_pm_platform.config import Settings


@contextmanager
def get_connection(settings: Settings) -> Iterator[Connection]:
    with psycopg.connect(settings.postgres_dsn) as connection:
        yield connection
