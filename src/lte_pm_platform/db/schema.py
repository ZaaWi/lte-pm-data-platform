from __future__ import annotations

from pathlib import Path

from psycopg import Connection

from lte_pm_platform.utils.paths import sql_init_dir

SQL_INIT_ORDER = (
    "001_extensions.sql",
    "002_tables_raw.sql",
    "003_tables_audit.sql",
    "003a_tables_ftp_registry.sql",
    "004_reference_tables.sql",
    "006_kpi_foundation.sql",
    "008_kpi_counter_mapping.sql",
    "009_counter_reference_provenance.sql",
    "005_analytics_views.sql",
    "007_kpi_views.sql",
)


def ordered_sql_files() -> list[Path]:
    init_dir = sql_init_dir()
    available_files = {path.name: path for path in Path(init_dir).glob("*.sql")}
    missing_files = [filename for filename in SQL_INIT_ORDER if filename not in available_files]
    if missing_files:
        missing = ", ".join(missing_files)
        raise FileNotFoundError(f"Missing SQL init files: {missing}")
    extras = sorted(name for name in available_files if name not in SQL_INIT_ORDER)
    if extras:
        extra_names = ", ".join(extras)
        raise ValueError(f"Unordered SQL init files found: {extra_names}")
    return [available_files[filename] for filename in SQL_INIT_ORDER]


def initialize_schema(connection: Connection) -> None:
    for sql_file in ordered_sql_files():
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql_file.read_text())
        except Exception as exc:
            connection.rollback()
            raise RuntimeError(f"Failed applying SQL init file: {sql_file.name}") from exc
        connection.commit()
