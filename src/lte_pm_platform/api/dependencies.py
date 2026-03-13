from collections.abc import Iterator

from fastapi import Depends
from psycopg import Connection

from lte_pm_platform.config import Settings, get_settings
from lte_pm_platform.db.connection import get_connection



def get_api_settings() -> Settings:
    return get_settings()



def get_db_connection(settings: Settings = Depends(get_api_settings)) -> Iterator[Connection]:
    with get_connection(settings) as connection:
        yield connection
