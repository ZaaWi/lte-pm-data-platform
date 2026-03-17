from __future__ import annotations

import json
import logging
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import typer

from lte_pm_platform.config import get_settings
from lte_pm_platform.db.connection import get_connection
from lte_pm_platform.db.repositories.counter_reference_repository import CounterReferenceRepository
from lte_pm_platform.db.repositories.entity_reference_repository import EntityReferenceRepository
from lte_pm_platform.db.repositories.file_audit_repository import FileAuditRepository
from lte_pm_platform.db.repositories.ftp_remote_file_repository import FtpRemoteFileRepository
from lte_pm_platform.db.repositories.kpi_repository import KpiRepository
from lte_pm_platform.db.repositories.pm_sample_repository import PmSampleRepository
from lte_pm_platform.db.repositories.semantic_kpi_repository import SemanticKpiRepository
from lte_pm_platform.db.repositories.topology_reference_repository import TopologyReferenceRepository
from lte_pm_platform.db.schema import initialize_schema
from lte_pm_platform.pipeline.ingest.counter_reference_seed import load_counter_reference_seed
from lte_pm_platform.pipeline.ingest.file_discovery import (
    DEFAULT_LOCAL_SOURCE_ROOTS,
    RevisionPolicy,
    discover_local_files,
)
from lte_pm_platform.pipeline.ingest.ftp_client import FtpClient
from lte_pm_platform.pipeline.loaders.postgres_loader import PostgresLoader
from lte_pm_platform.pipeline.orchestration.ftp_staged_flow import (
    build_operational_status,
    download_registry_files,
    fetch_via_staged_flow,
    ingest_registry_files,
    inspect_failure_row,
    inspect_failure_rows,
    reconcile_registry_rows,
    retry_download_registry_files,
    retry_ingest_registry_files,
    run_locked_ftp_cycle,
    scan_remote_files,
)
from lte_pm_platform.pipeline.orchestration.run_lock import PipelineCycleLockError
from lte_pm_platform.pipeline.orchestration.sample_pipeline import SamplePipeline
from lte_pm_platform.pipeline.orchestration.semantic_kpi import (
    load_counter_dictionary,
    load_kpi_definitions,
)
from lte_pm_platform.pipeline.orchestration.topology_enrichment import (
    load_topology_entity_site_map,
    load_topology_regions,
    load_topology_reporting,
    load_topology_sites,
    sync_topology_enrichment,
)
from lte_pm_platform.pipeline.orchestration.vendor_indicator_semantics import (
    load_vendor_indicator_seed_file,
)
from lte_pm_platform.utils.paths import data_input_dir, ftp_cycle_lock_path, ftp_download_dir

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = typer.Typer(help="LTE PM CLI")
ZIP_OPTION = typer.Option(..., exists=True, dir_okay=False, readable=True)
LIMIT_OPTION = typer.Option(10, min=1, max=1000)
EXPECTED_CELLS_OPTION = typer.Option(..., "--expected", min=1)
KPI_NAME_OPTION = typer.Option(..., "--name")
CSV_OPTION = typer.Option(..., exists=True, dir_okay=False, readable=True)
COUNTER_ID_OPTION = typer.Option(..., "--id")
START_OPTION = typer.Option(None, help="Inclusive interval start: YYYYMMDDHHMM or YYYYMMDD_HHMM")
END_OPTION = typer.Option(None, help="Inclusive interval end: YYYYMMDDHHMM or YYYYMMDD_HHMM")
REVISION_POLICY_OPTION = typer.Option(
    "additive",
    help="Revision policy: additive, base-only, revisions-only, latest-only",
)
FAMILY_OPTION = typer.Option(None, "--family", help="Repeat to limit to specific dataset families")
FTP_SOURCE_NAME = "default"
FTP_REGISTRY_ID_OPTION = typer.Option(None, "--id", help="Registry row id")
FTP_REGISTRY_IDS_OPTION = typer.Option(None, "--id", help="Repeat --id for registry row ids")


def parse_interval_option(value: str | None) -> tuple[datetime | None, bool]:
    if value is None:
        return None, False
    for pattern in ("%Y%m%d%H%M", "%Y%m%d_%H%M", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(value, pattern), False
        except ValueError:
            continue
    try:
        return datetime.strptime(value, "%Y-%m-%d"), True
    except ValueError:
        pass
    raise typer.BadParameter(f"Unsupported datetime format: {value}")


def parse_time_window(
    start: str | None,
    end: str | None,
) -> tuple[datetime | None, datetime | None]:
    parsed_start, start_is_date_only = parse_interval_option(start)
    parsed_end, end_is_date_only = parse_interval_option(end)

    normalized_start = parsed_start
    if start_is_date_only and parsed_start is not None:
        normalized_start = parsed_start.replace(hour=0, minute=0, second=0, microsecond=0)

    normalized_end = parsed_end
    if parsed_end is not None:
        if end_is_date_only:
            normalized_end = (
                parsed_end.replace(hour=0, minute=0, second=0, microsecond=0)
                + timedelta(days=1)
            )
        else:
            normalized_end = parsed_end + timedelta(minutes=1)

    return normalized_start, normalized_end


def parse_revision_policy(value: str) -> RevisionPolicy:
    allowed: set[str] = {"additive", "base-only", "revisions-only", "latest-only"}
    if value not in allowed:
        raise typer.BadParameter(f"Unsupported revision policy: {value}")
    return value  # type: ignore[return-value]


def get_ftp_client() -> FtpClient:
    settings = get_settings()
    if not settings.ftp_host:
        raise typer.BadParameter("FTP_HOST is not configured.")
    if not settings.ftp_username:
        raise typer.BadParameter("FTP_USERNAME is not configured.")
    if not settings.ftp_password:
        raise typer.BadParameter("FTP_PASSWORD is not configured.")
    return FtpClient(
        host=settings.ftp_host,
        port=settings.ftp_port,
        username=settings.ftp_username,
        password=settings.ftp_password,
        remote_directory=settings.ftp_remote_directory,
        passive_mode=settings.ftp_passive_mode,
    )


def get_ftp_remote_directories() -> tuple[str, ...]:
    settings = get_settings()
    directories = getattr(settings, "ftp_remote_directories", ())
    if directories:
        return tuple(directories)
    remote_directory = getattr(settings, "ftp_remote_directory", "/")
    return (remote_directory,)


@app.command("init-db")
def init_db() -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        initialize_schema(connection)
    typer.echo("Database schema initialized.")


@app.command("load-sample")
def load_sample(zip: Path = ZIP_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        pipeline = SamplePipeline(
            loader=PostgresLoader(connection),
            audit_repository=FileAuditRepository(connection),
        )
        summary = pipeline.load_zip(zip, trigger_type="local_cli", source_type="local")
    typer.echo(json.dumps(summary.as_dict(), indent=2))


@app.command("query-sample")
def query_sample(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = PmSampleRepository(connection).fetch_recent(limit=limit)
    typer.echo(f"Returned {len(rows)} rows:")
    for row in rows:
        typer.echo(json.dumps(row, default=str))


@app.command("list-counters")
def list_counters(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = PmSampleRepository(connection).list_seen_counters(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("top-counters")
def top_counters(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = PmSampleRepository(connection).top_counters(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("list-counter-reference")
def list_counter_reference(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = CounterReferenceRepository(connection).fetch_all(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("summarize-entities")
def summarize_entities(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = EntityReferenceRepository(connection).summarize_entities(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("show-entity-shape")
def show_entity_shape(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = EntityReferenceRepository(connection).show_entity_shape(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("sync-entities")
def sync_entities() -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        entity_repository = EntityReferenceRepository(connection)
        audit_repository = FileAuditRepository(connection)
        try:
            count = entity_repository.refresh_from_raw_entities()
            audits_updated = audit_repository.mark_success_normalization_completed()
            connection.commit()
        except Exception as exc:
            connection.rollback()
            audits_updated = audit_repository.mark_success_normalization_failed(str(exc))
            connection.commit()
            raise
    typer.echo(json.dumps({"rows_synced": count, "audits_updated": audits_updated}, indent=2))


@app.command("load-topology-regions")
def load_topology_regions_command(csv: Path = CSV_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        payload = load_topology_regions(
            repository=TopologyReferenceRepository(connection),
            csv_path=csv,
        )
    typer.echo(json.dumps(payload, indent=2))


@app.command("load-topology-sites")
def load_topology_sites_command(csv: Path = CSV_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        payload = load_topology_sites(
            repository=TopologyReferenceRepository(connection),
            csv_path=csv,
        )
    typer.echo(json.dumps(payload, indent=2))


@app.command("load-topology-reporting")
def load_topology_reporting_command(csv: Path = CSV_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        payload = load_topology_reporting(
            repository=TopologyReferenceRepository(connection),
            csv_path=csv,
        )
    typer.echo(json.dumps(payload, indent=2))


@app.command("load-topology-entity-map")
def load_topology_entity_map_command(csv: Path = CSV_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        payload = load_topology_entity_site_map(
            repository=TopologyReferenceRepository(connection),
            csv_path=csv,
        )
    typer.echo(json.dumps(payload, indent=2))


@app.command("sync-topology")
def sync_topology() -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        payload = sync_topology_enrichment(
            repository=TopologyReferenceRepository(connection),
        )
        connection.commit()
    typer.echo(json.dumps(payload, indent=2))


@app.command("show-counter")
def show_counter(counter_id: str = COUNTER_ID_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        row = CounterReferenceRepository(connection).fetch_by_id(counter_id)
    typer.echo(json.dumps(row, indent=2, default=str))


@app.command("load-counter-reference")
def load_counter_reference(csv: Path = CSV_OPTION) -> None:
    seed_rows = load_counter_reference_seed(csv)
    settings = get_settings()
    with get_connection(settings) as connection:
        inserted = CounterReferenceRepository(connection).upsert_many(seed_rows)
    typer.echo(json.dumps({"rows_loaded": inserted, "csv": str(csv)}, indent=2))


@app.command("load-counter-dictionary")
def load_counter_dictionary_command(csv: Path = CSV_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        payload = load_counter_dictionary(
            repository=SemanticKpiRepository(connection),
            csv_path=csv,
        )
    typer.echo(json.dumps(payload, indent=2))


@app.command("load-kpi-definitions")
def load_kpi_definitions_command(csv: Path = CSV_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        payload = load_kpi_definitions(
            repository=SemanticKpiRepository(connection),
            csv_path=csv,
        )
    typer.echo(json.dumps(payload, indent=2))


@app.command("load-vendor-indicator-seed")
def load_vendor_indicator_seed_command(csv: Path = CSV_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        payload = load_vendor_indicator_seed_file(
            repository=SemanticKpiRepository(connection),
            csv_path=csv,
        )
    typer.echo(json.dumps(payload, indent=2))


@app.command("list-unmapped-entities")
def list_unmapped_entities(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = TopologyReferenceRepository(connection).list_unmapped_entities(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("summarize-site-coverage")
def summarize_site_coverage(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = TopologyReferenceRepository(connection).summarize_site_coverage(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("summarize-region-coverage")
def summarize_region_coverage(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = TopologyReferenceRepository(connection).summarize_region_coverage(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("list-unmapped-counters")
def list_unmapped_counters(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).list_unmapped_counters(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("list-provisional-kpis")
def list_provisional_kpis(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).list_provisional_kpis(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("summarize-kpi-input-coverage")
def summarize_kpi_input_coverage(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).summarize_kpi_input_coverage(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("list-verified-prb-kpi-outputs")
def list_verified_prb_kpi_outputs(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).list_verified_prb_kpi_outputs(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("summarize-verified-prb-kpi-execution")
def summarize_verified_prb_kpi_execution(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).summarize_verified_prb_kpi_execution(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("list-verified-bler-kpi-outputs")
def list_verified_bler_kpi_outputs(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).list_verified_bler_kpi_outputs(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("summarize-verified-bler-kpi-execution")
def summarize_verified_bler_kpi_execution(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).summarize_verified_bler_kpi_execution(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("list-verified-prb-kpi-entity-time")
def list_verified_prb_kpi_entity_time(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).list_verified_prb_kpi_outputs(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("list-verified-bler-kpi-entity-time")
def list_verified_bler_kpi_entity_time(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).list_verified_bler_kpi_outputs(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("list-verified-rrc-kpi-entity-time")
def list_verified_rrc_kpi_entity_time(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).list_verified_rrc_kpi_entity_time(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("list-verified-prb-kpi-site-time")
def list_verified_prb_kpi_site_time(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).list_verified_prb_kpi_site_time(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("list-verified-bler-kpi-site-time")
def list_verified_bler_kpi_site_time(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).list_verified_bler_kpi_site_time(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("list-verified-rrc-kpi-site-time")
def list_verified_rrc_kpi_site_time(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).list_verified_rrc_kpi_site_time(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("list-verified-prb-kpi-region-time")
def list_verified_prb_kpi_region_time(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).list_verified_prb_kpi_region_time(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("list-verified-bler-kpi-region-time")
def list_verified_bler_kpi_region_time(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).list_verified_bler_kpi_region_time(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("list-verified-rrc-kpi-region-time")
def list_verified_rrc_kpi_region_time(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).list_verified_rrc_kpi_region_time(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("validate-verified-rrc-kpi-entity-time")
def validate_verified_rrc_kpi_entity_time() -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).validate_verified_rrc_kpi_entity_time()
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("validate-verified-prb-kpi-site-time")
def validate_verified_prb_kpi_site_time() -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).validate_verified_prb_kpi_site_time()
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("validate-verified-bler-kpi-site-time")
def validate_verified_bler_kpi_site_time() -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).validate_verified_bler_kpi_site_time()
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("validate-verified-rrc-kpi-site-time")
def validate_verified_rrc_kpi_site_time() -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).validate_verified_rrc_kpi_site_time()
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("validate-verified-prb-kpi-region-time")
def validate_verified_prb_kpi_region_time() -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).validate_verified_prb_kpi_region_time()
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("validate-verified-bler-kpi-region-time")
def validate_verified_bler_kpi_region_time() -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).validate_verified_bler_kpi_region_time()
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("validate-verified-rrc-kpi-region-time")
def validate_verified_rrc_kpi_region_time() -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).validate_verified_rrc_kpi_region_time()
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("list-vendor-indicators")
def list_vendor_indicators(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = SemanticKpiRepository(connection).list_vendor_indicators(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("summarize-files")
def summarize_files(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = PmSampleRepository(connection).summarize_by_source_file(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("summarize-source-files")
def summarize_source_files(limit: int = LIMIT_OPTION) -> None:
    summarize_files(limit=limit)


@app.command("summarize-time")
def summarize_time(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = PmSampleRepository(connection).summarize_by_collect_time(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("summarize-ani")
def summarize_ani(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = PmSampleRepository(connection).summarize_by_ani(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("summarize-entity-fields")
def summarize_entity_fields(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = PmSampleRepository(connection).summarize_entity_fields(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("count-distinct-cells")
def count_distinct_cells(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = PmSampleRepository(connection).count_distinct_cells(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("summarize-entity-counters")
def summarize_entity_counters(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = PmSampleRepository(connection).summarize_entity_counters(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("summarize-entity-intervals")
def summarize_entity_intervals(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = PmSampleRepository(connection).summarize_entity_intervals(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("summarize-dataset-family")
def summarize_dataset_family(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = PmSampleRepository(connection).summarize_by_dataset_family(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("summarize-coverage")
def summarize_coverage(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = PmSampleRepository(connection).summarize_coverage(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("compare-expected-cells")
def compare_expected_cells(expected: int = EXPECTED_CELLS_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = PmSampleRepository(connection).compare_expected_cells(expected=expected)
    typer.echo(
        json.dumps(
            {
                "expected_cells": expected,
                "count": len(rows),
                "rows": rows,
            },
            indent=2,
            default=str,
        )
    )


@app.command("summarize-coverage-timeline")
def summarize_coverage_timeline(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = PmSampleRepository(connection).summarize_coverage_timeline(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("compare-expected-cells-timeline")
def compare_expected_cells_timeline(
    expected: int = EXPECTED_CELLS_OPTION,
    limit: int = LIMIT_OPTION,
) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = PmSampleRepository(connection).compare_expected_cells_timeline(
            expected=expected,
            limit=limit,
        )
    typer.echo(
        json.dumps(
            {
                "expected_cells": expected,
                "count": len(rows),
                "rows": rows,
            },
            indent=2,
            default=str,
        )
    )


@app.command("reconcile-ingest-files")
def reconcile_ingest_files(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        audit_rows = FileAuditRepository(connection).fetch_recent_for_reconciliation(limit=limit)
        raw_counts = PmSampleRepository(connection).count_rows_by_source_files(
            row["source_file"] for row in audit_rows
        )

    mismatches: list[dict[str, object]] = []
    for row in audit_rows:
        issues: list[str] = []
        final_file_path = row["final_file_path"]
        raw_rows = raw_counts.get(row["source_file"], 0)

        if row["status"] == "SUCCESS" and raw_rows == 0:
            issues.append("success_without_raw_rows")
        if row["status"] == "FAILED" and raw_rows > 0:
            issues.append("failed_with_raw_rows")
        if row["lifecycle_status"] != "COMPLETED":
            issues.append(f"lifecycle_{str(row['lifecycle_status']).lower()}")
        if final_file_path and not Path(final_file_path).exists():
            issues.append("final_file_missing_on_disk")
        if row["status"] in {"SUCCESS", "SKIPPED_DUPLICATE"} and not final_file_path:
            issues.append("missing_final_file_path")

        if issues:
            mismatches.append(
                {
                    "source_file": row["source_file"],
                    "run_id": str(row["run_id"]),
                    "status": row["status"],
                    "raw_rows": raw_rows,
                    "lifecycle_status": row["lifecycle_status"],
                    "lifecycle_action": row["lifecycle_action"],
                    "final_file_path": final_file_path,
                    "issues": issues,
                }
            )

    typer.echo(
        json.dumps(
            {
                "audits_checked": len(audit_rows),
                "mismatch_count": len(mismatches),
                "rows": mismatches,
            },
            indent=2,
            default=str,
        )
    )


@app.command("backfill-lifecycle-status")
def backfill_lifecycle_status(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        audit_repository = FileAuditRepository(connection)
        pending_rows = audit_repository.fetch_pending_lifecycle(limit=limit)
        updated = 0
        for row in pending_rows:
            final_file_path = row["final_file_path"]
            lifecycle_status = (
                "COMPLETED"
                if final_file_path and Path(final_file_path).exists()
                else "FAILED"
            )
            error_message = row["error_message"]
            if lifecycle_status == "FAILED" and error_message is None:
                error_message = "Legacy lifecycle backfill could not confirm final file on disk."
            updated += audit_repository.update_lifecycle(
                run_id=str(row["run_id"]),
                lifecycle_status=lifecycle_status,
                lifecycle_action=row["lifecycle_action"],
                final_file_path=final_file_path,
                error_message=error_message,
            )
        connection.commit()

    typer.echo(
        json.dumps(
            {
                "pending_rows_checked": len(pending_rows),
                "rows_updated": updated,
            },
            indent=2,
        )
    )


@app.command("list-kpis")
def list_kpis(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        rows = KpiRepository(connection).list_definitions(limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("summarize-kpi")
def summarize_kpi(name: str = KPI_NAME_OPTION, limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        repository = KpiRepository(connection)
        definition = repository.get_definition(name)
        rows = repository.summarize_kpi(name, limit=limit)
    typer.echo(
        json.dumps(
            {"definition": definition, "count": len(rows), "rows": rows},
            indent=2,
            default=str,
        )
    )


@app.command("ftp-list")
def ftp_list(
    start: str | None = START_OPTION,
    end: str | None = END_OPTION,
    revision_policy: str = REVISION_POLICY_OPTION,
) -> None:
    settings = get_settings()
    client = get_ftp_client()
    window_start, window_end = parse_time_window(start, end)
    parsed_candidates = []
    for remote_directory in get_ftp_remote_directories():
        parsed_candidates.extend(
            client.list_candidate_details(
                remote_directory=remote_directory,
                start=window_start,
                end=window_end,
                revision_policy=parse_revision_policy(revision_policy),
            )
        )
    typer.echo(
        json.dumps(
            {
                "count": len(parsed_candidates),
                "files": [candidate.as_dict() for candidate in parsed_candidates],
            },
            indent=2,
        )
    )


@app.command("ftp-fetch")
def ftp_fetch(
    limit: int = LIMIT_OPTION,
    start: str | None = START_OPTION,
    end: str | None = END_OPTION,
    revision_policy: str = REVISION_POLICY_OPTION,
) -> None:
    settings = get_settings()
    client = get_ftp_client()
    window_start, window_end = parse_time_window(start, end)
    download_dir = ftp_download_dir()

    with get_connection(settings) as connection:
        repository = FtpRemoteFileRepository(connection)
        pipeline = SamplePipeline(
            loader=PostgresLoader(connection),
            audit_repository=FileAuditRepository(connection),
        )
        payload = fetch_via_staged_flow(
            repository=repository,
            client=client,
            pipeline=pipeline,
            source_name=FTP_SOURCE_NAME,
            remote_directory=settings.ftp_remote_directory,
            remote_directories=get_ftp_remote_directories(),
            download_dir=download_dir,
            start=window_start,
            end=window_end,
            revision_policy=parse_revision_policy(revision_policy),
            limit=limit,
            trigger_type="ftp_fetch",
            source_type="ftp",
        )
        connection.commit()

    typer.echo(json.dumps(payload, indent=2))


@app.command("ftp-run-cycle")
def ftp_run_cycle(
    limit: int = LIMIT_OPTION,
    start: str | None = START_OPTION,
    end: str | None = END_OPTION,
    revision_policy: str = REVISION_POLICY_OPTION,
    family: list[str] | None = FAMILY_OPTION,
    retry_failed: bool = typer.Option(
        False,
        "--retry-failed",
        help="Retry failures encountered during this cycle",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what the cycle would do without mutating registry or files",
    ),
) -> None:
    settings = get_settings()
    client = get_ftp_client()
    window_start, window_end = parse_time_window(start, end)
    download_dir = ftp_download_dir()

    with get_connection(settings) as connection:
        repository = FtpRemoteFileRepository(connection)
        pipeline = SamplePipeline(
            loader=PostgresLoader(connection),
            audit_repository=FileAuditRepository(connection),
        )
        try:
            payload = run_locked_ftp_cycle(
                lock_path=ftp_cycle_lock_path(),
                repository=repository,
                client=client,
                pipeline=pipeline,
                source_name=FTP_SOURCE_NAME,
                remote_directory=settings.ftp_remote_directory,
                remote_directories=get_ftp_remote_directories(),
                download_dir=download_dir,
                start=window_start,
                end=window_end,
                revision_policy=parse_revision_policy(revision_policy),
                limit=limit,
                families=family,
                retry_failed=retry_failed,
                dry_run=dry_run,
                trigger_type="ftp_run_cycle",
                source_type="ftp",
            )
        except PipelineCycleLockError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1) from exc
        if not dry_run:
            connection.commit()

    typer.echo(json.dumps(payload, indent=2, default=str))


@app.command("ftp-scan")
def ftp_scan(
    start: str | None = START_OPTION,
    end: str | None = END_OPTION,
    revision_policy: str = REVISION_POLICY_OPTION,
) -> None:
    settings = get_settings()
    client = get_ftp_client()
    window_start, window_end = parse_time_window(start, end)

    with get_connection(settings) as connection:
        repository = FtpRemoteFileRepository(connection)
        result = scan_remote_files(
            repository=repository,
            client=client,
            source_name=FTP_SOURCE_NAME,
            remote_directory=settings.ftp_remote_directory,
            remote_directories=get_ftp_remote_directories(),
            start=window_start,
            end=window_end,
            revision_policy=parse_revision_policy(revision_policy),
        )
        summary = result["summary"]
        candidates = result["candidates"]
        connection.commit()

    typer.echo(
        json.dumps(
            {
                "count": len(candidates),
                "summary": summary,
                "files": [candidate.as_dict() for candidate in candidates],
            },
            indent=2,
        )
    )


@app.command("ftp-download")
def ftp_download(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    client = get_ftp_client()
    download_dir = ftp_download_dir()
    results: list[dict[str, object]] = []

    with get_connection(settings) as connection:
        repository = FtpRemoteFileRepository(connection)
        results = download_registry_files(
            repository=repository,
            client=client,
            source_name=FTP_SOURCE_NAME,
            download_dir=download_dir,
            limit=limit,
        )
        connection.commit()

    public_results = []
    for result in results:
        row = {
            "remote_path": result["remote_path"],
            "status": result["status"],
        }
        if "local_staged_path" in result:
            row["local_staged_path"] = result["local_staged_path"]
        if "error" in result:
            row["error"] = result["error"]
        public_results.append(row)

    typer.echo(json.dumps({"count": len(public_results), "results": public_results}, indent=2))


@app.command("ftp-ingest")
def ftp_ingest(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    results: list[dict[str, object]] = []

    with get_connection(settings) as connection:
        repository = FtpRemoteFileRepository(connection)
        pipeline = SamplePipeline(
            loader=PostgresLoader(connection),
            audit_repository=FileAuditRepository(connection),
        )
        results = ingest_registry_files(
            repository=repository,
            pipeline=pipeline,
            source_name=FTP_SOURCE_NAME,
            limit=limit,
            trigger_type="ftp_stage_ingest",
            source_type="ftp",
        )
        connection.commit()

    public_results = []
    for result in results:
        row = {
            "remote_path": result["remote_path"],
            "status": result["status"],
        }
        if "file_hash" in result:
            row["file_hash"] = result["file_hash"]
        if "ingest_run_id" in result:
            row["ingest_run_id"] = result["ingest_run_id"]
        if "final_file_path" in result:
            row["final_file_path"] = result["final_file_path"]
        if "error" in result:
            row["error"] = result["error"]
        public_results.append(row)

    typer.echo(json.dumps({"count": len(public_results), "results": public_results}, indent=2))


@app.command("ftp-status")
def ftp_status(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        repository = FtpRemoteFileRepository(connection)
        payload = build_operational_status(repository=repository, limit=limit)

    typer.echo(
        json.dumps(
            payload,
            indent=2,
            default=str,
        )
    )


@app.command("ftp-failures")
def ftp_failures(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        repository = FtpRemoteFileRepository(connection)
        rows = inspect_failure_rows(repository=repository, limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("ftp-failure-show")
def ftp_failure_show(remote_file_id: int = typer.Option(..., "--id", min=1)) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        repository = FtpRemoteFileRepository(connection)
        row = inspect_failure_row(repository=repository, remote_file_id=remote_file_id)
    if row is None:
        raise typer.BadParameter(f"FTP registry row not found: {remote_file_id}")
    typer.echo(json.dumps(row, indent=2, default=str))


@app.command("ftp-retry-download")
def ftp_retry_download(
    ids: list[int] = typer.Option(..., "--id", min=1, help="Repeat --id for failed download rows"),
) -> None:
    settings = get_settings()
    client = get_ftp_client()
    download_dir = ftp_download_dir()
    with get_connection(settings) as connection:
        repository = FtpRemoteFileRepository(connection)
        payload = retry_download_registry_files(
            repository=repository,
            client=client,
            source_name=FTP_SOURCE_NAME,
            download_dir=download_dir,
            remote_file_ids=ids,
        )
        connection.commit()
    public_results = []
    for result in payload["results"]:
        row = {
            "remote_path": result["remote_path"],
            "status": result["status"],
        }
        if "local_staged_path" in result:
            row["local_staged_path"] = result["local_staged_path"]
        if "error" in result:
            row["error"] = result["error"]
        public_results.append(row)
    typer.echo(
        json.dumps(
            {
                "requested_ids": payload["requested_ids"],
                "processed_ids": payload["processed_ids"],
                "not_retryable_ids": payload["not_retryable_ids"],
                "count": len(public_results),
                "results": public_results,
            },
            indent=2,
        )
    )


@app.command("ftp-retry-ingest")
def ftp_retry_ingest(
    ids: list[int] = typer.Option(..., "--id", min=1, help="Repeat --id for failed ingest rows"),
) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        repository = FtpRemoteFileRepository(connection)
        pipeline = SamplePipeline(
            loader=PostgresLoader(connection),
            audit_repository=FileAuditRepository(connection),
        )
        payload = retry_ingest_registry_files(
            repository=repository,
            pipeline=pipeline,
            source_name=FTP_SOURCE_NAME,
            trigger_type="ftp_stage_ingest",
            source_type="ftp",
            remote_file_ids=ids,
        )
        connection.commit()
    public_results = []
    for result in payload["results"]:
        row = {
            "remote_path": result["remote_path"],
            "status": result["status"],
        }
        if "classification" in result:
            row["classification"] = result["classification"]
        if "file_hash" in result:
            row["file_hash"] = result["file_hash"]
        if "ingest_run_id" in result:
            row["ingest_run_id"] = result["ingest_run_id"]
        if "final_file_path" in result:
            row["final_file_path"] = result["final_file_path"]
        if "error" in result:
            row["error"] = result["error"]
        public_results.append(row)
    typer.echo(
        json.dumps(
            {
                "requested_ids": payload["requested_ids"],
                "processed_ids": payload["processed_ids"],
                "not_retryable_ids": payload["not_retryable_ids"],
                "count": len(public_results),
                "results": public_results,
            },
            indent=2,
        )
    )


@app.command("ftp-reconcile")
def ftp_reconcile(limit: int = LIMIT_OPTION) -> None:
    settings = get_settings()
    with get_connection(settings) as connection:
        repository = FtpRemoteFileRepository(connection)
        rows = reconcile_registry_rows(repository=repository, limit=limit)
    typer.echo(json.dumps({"count": len(rows), "rows": rows}, indent=2, default=str))


@app.command("local-list")
def local_list(
    family: list[str] | None = FAMILY_OPTION,
    start: str | None = START_OPTION,
    end: str | None = END_OPTION,
    revision_policy: str = REVISION_POLICY_OPTION,
) -> None:
    window_start, window_end = parse_time_window(start, end)
    candidates = discover_local_files(
        families=family,
        start=window_start,
        end=window_end,
        revision_policy=parse_revision_policy(revision_policy),
    )
    typer.echo(
        json.dumps(
            {
                "count": len(candidates),
                "source_roots": {
                    family_name: str(path)
                    for family_name, path in DEFAULT_LOCAL_SOURCE_ROOTS.items()
                },
                "files": [candidate.as_dict() for candidate in candidates],
            },
            indent=2,
        )
    )


@app.command("local-load-range")
def local_load_range(
    family: list[str] | None = FAMILY_OPTION,
    start: str | None = START_OPTION,
    end: str | None = END_OPTION,
    revision_policy: str = REVISION_POLICY_OPTION,
    limit: int = LIMIT_OPTION,
) -> None:
    settings = get_settings()
    window_start, window_end = parse_time_window(start, end)
    candidates = discover_local_files(
        families=family,
        start=window_start,
        end=window_end,
        revision_policy=parse_revision_policy(revision_policy),
    )[:limit]
    results: list[dict[str, object]] = []

    with get_connection(settings) as connection:
        pipeline = SamplePipeline(
            loader=PostgresLoader(connection),
            audit_repository=FileAuditRepository(connection),
        )
        for candidate in candidates:
            staged_dir = data_input_dir() / "local_selection"
            staged_dir.mkdir(parents=True, exist_ok=True)
            staged_path = staged_dir / Path(candidate.path).name
            shutil.copy2(candidate.path, staged_path)
            summary = pipeline.load_zip(
                staged_path,
                trigger_type="local_range_cli",
                source_type="local",
            )
            results.append(
                {
                    "dataset_family": candidate.dataset_family,
                    "interval_start": candidate.interval_start.strftime("%Y-%m-%d %H:%M:%S"),
                    "revision": candidate.revision,
                    "path": candidate.path,
                    "staged_path": str(staged_path),
                    "ingest_summary": summary.as_dict(),
                }
            )

    typer.echo(json.dumps({"count": len(results), "results": results}, indent=2))


if __name__ == "__main__":
    app()
