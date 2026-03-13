from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic

from lte_pm_platform.domain.models import IngestSummary
from lte_pm_platform.pipeline.ingest.file_discovery import ParsedArchiveFile, RevisionPolicy
from lte_pm_platform.pipeline.orchestration.run_lock import pipeline_cycle_lock


def scan_remote_files(
    *,
    repository,
    client,
    source_name: str,
    remote_directory: str,
    start: datetime | None,
    end: datetime | None,
    revision_policy: RevisionPolicy,
    families: Sequence[str] | None = None,
    persist: bool = True,
) -> dict[str, object]:
    candidates = client.list_candidate_details(
        start=start,
        end=end,
        revision_policy=revision_policy,
    )
    if families is not None:
        selected_families = set(families)
        candidates = [candidate for candidate in candidates if candidate.dataset_family in selected_families]
    if persist:
        summary = repository.upsert_discovered_files(
            source_name=source_name,
            remote_directory=remote_directory,
            files=candidates,
        )
    else:
        summary = {"discovered": 0, "updated": 0}
    return {"candidates": candidates, "summary": summary}


def download_registry_files(
    *,
    repository,
    client,
    source_name: str,
    download_dir: Path,
    limit: int,
    remote_paths: Sequence[str] | None = None,
) -> list[dict[str, object]]:
    rows = repository.fetch_pending_downloads(
        source_name=source_name,
        limit=limit,
        remote_paths=remote_paths,
    )
    results: list[dict[str, object]] = []
    for row in rows:
        try:
            local_path = client.download_file(row["remote_path"], download_dir)
            repository.mark_download_succeeded(
                remote_file_id=row["id"],
                local_staged_path=str(local_path),
            )
            results.append(
                {
                    "remote_file_id": row["id"],
                    "remote_path": row["remote_path"],
                    "remote_filename": row.get("remote_filename"),
                    "status": "DOWNLOADED",
                    "local_staged_path": str(local_path),
                }
            )
        except Exception as exc:
            repository.mark_download_failed(
                remote_file_id=row["id"],
                error_message=str(exc),
            )
            results.append(
                {
                    "remote_file_id": row["id"],
                    "remote_path": row["remote_path"],
                    "remote_filename": row.get("remote_filename"),
                    "status": "FAILED_DOWNLOAD",
                    "error": str(exc),
                }
            )
    return results


def retry_download_registry_files(
    *,
    repository,
    client,
    source_name: str,
    download_dir: Path,
    remote_file_ids: Sequence[int],
) -> dict[str, object]:
    rows = repository.fetch_retry_download_rows(
        source_name=source_name,
        remote_file_ids=remote_file_ids,
    )
    requested_ids = list(remote_file_ids)
    eligible_ids = [row["id"] for row in rows]
    results = _download_rows(
        repository=repository,
        client=client,
        rows=rows,
        download_dir=download_dir,
    )
    return {
        "requested_ids": requested_ids,
        "processed_ids": eligible_ids,
        "not_retryable_ids": [remote_file_id for remote_file_id in requested_ids if remote_file_id not in set(eligible_ids)],
        "results": results,
    }


def ingest_registry_files(
    *,
    repository,
    pipeline,
    source_name: str,
    limit: int,
    trigger_type: str,
    source_type: str,
    remote_file_ids: Sequence[int] | None = None,
) -> list[dict[str, object]]:
    rows = repository.fetch_pending_ingests(
        source_name=source_name,
        limit=limit,
        remote_file_ids=remote_file_ids,
    )
    results: list[dict[str, object]] = []
    for row in rows:
        local_staged_path = row.get("local_staged_path")
        if not local_staged_path:
            error_message = "local_staged_path is missing for staged ingest."
            repository.mark_ingest_failed(
                remote_file_id=row["id"],
                error_message=error_message,
            )
            results.append(
                {
                    "remote_file_id": row["id"],
                    "remote_path": row["remote_path"],
                    "status": "FAILED_INGEST",
                    "error": error_message,
                    "ingest_summary": {
                        "status": "FAILED_INGEST",
                        "error_message": error_message,
                    },
                }
            )
            continue

        staged_path = Path(local_staged_path)
        if not staged_path.exists():
            error_message = f"staged file not found: {staged_path}"
            repository.mark_ingest_failed(
                remote_file_id=row["id"],
                error_message=error_message,
            )
            results.append(
                {
                    "remote_file_id": row["id"],
                    "remote_path": row["remote_path"],
                    "status": "FAILED_INGEST",
                    "error": error_message,
                    "ingest_summary": {
                        "status": "FAILED_INGEST",
                        "error_message": error_message,
                    },
                }
            )
            continue

        try:
            summary = pipeline.load_zip(
                staged_path,
                trigger_type=trigger_type,
                source_type=source_type,
            )
            results.append(_map_ingest_summary(repository, row["id"], row["remote_path"], summary))
        except Exception as exc:
            repository.mark_ingest_failed(
                remote_file_id=row["id"],
                error_message=str(exc),
            )
            results.append(
                {
                    "remote_file_id": row["id"],
                    "remote_path": row["remote_path"],
                    "status": "FAILED_INGEST",
                    "error": str(exc),
                    "ingest_summary": {
                        "status": "FAILED_INGEST",
                        "error_message": str(exc),
                    },
                }
            )
    return results


def retry_ingest_registry_files(
    *,
    repository,
    pipeline,
    source_name: str,
    trigger_type: str,
    source_type: str,
    remote_file_ids: Sequence[int],
) -> dict[str, object]:
    rows = repository.fetch_retry_ingest_rows(
        source_name=source_name,
        remote_file_ids=remote_file_ids,
    )
    requested_ids = list(remote_file_ids)
    eligible_ids = [row["id"] for row in rows]
    results = _ingest_rows(
        repository=repository,
        pipeline=pipeline,
        rows=rows,
        trigger_type=trigger_type,
        source_type=source_type,
    )
    return {
        "requested_ids": requested_ids,
        "processed_ids": eligible_ids,
        "not_retryable_ids": [remote_file_id for remote_file_id in requested_ids if remote_file_id not in set(eligible_ids)],
        "results": results,
    }


def inspect_failure_rows(*, repository, limit: int) -> list[dict[str, object]]:
    rows = repository.fetch_failure_rows(limit=limit)
    latest_scan_at = repository.fetch_latest_scan_at()
    return [annotate_registry_row(row, latest_scan_at=latest_scan_at) for row in rows]


def inspect_failure_row(*, repository, remote_file_id: int) -> dict[str, object] | None:
    row = repository.fetch_remote_file_by_id(remote_file_id=remote_file_id)
    if row is None:
        return None
    return annotate_registry_row(row, latest_scan_at=repository.fetch_latest_scan_at())


def reconcile_registry_rows(*, repository, limit: int) -> list[dict[str, object]]:
    rows = repository.fetch_registry_rows(
        statuses=["DOWNLOADED", "FAILED_INGEST"],
        limit=limit,
    )
    latest_scan_at = repository.fetch_latest_scan_at()
    annotated = [annotate_registry_row(row, latest_scan_at=latest_scan_at) for row in rows]
    return [row for row in annotated if row["classification"] == "reconciliation_needed"]


def build_operational_status(*, repository, limit: int) -> dict[str, object]:
    status_counts = repository.summarize_status_counts()
    status_map = {row["status"]: row["file_count"] for row in status_counts}
    recent_failures = inspect_failure_rows(repository=repository, limit=limit)
    recon_rows = reconcile_registry_rows(repository=repository, limit=limit)
    latest_scan_at = repository.fetch_latest_scan_at()
    retryable_ingests = 0
    repeatedly_failing_downloads = 0
    repeatedly_failing_ingests = 0
    stale_pending_rows = 0
    no_longer_seen_rows = 0
    for row in repository.fetch_registry_rows(limit=10000):
        annotated = annotate_registry_row(row, latest_scan_at=latest_scan_at)
        classification = annotated["classification"]
        if annotated["classification"] == "retryable_ingest":
            retryable_ingests += 1
        if classification == "repeatedly_failing_download":
            repeatedly_failing_downloads += 1
        if classification == "repeatedly_failing_ingest":
            repeatedly_failing_ingests += 1
        if classification == "stale_pending":
            stale_pending_rows += 1
        if classification == "not_seen_in_latest_scan":
            no_longer_seen_rows += 1
    return {
        "status_counts": status_counts,
        "summary": {
            "pending_downloads": status_map.get("DISCOVERED", 0),
            "pending_ingests": status_map.get("DOWNLOADED", 0),
            "failed_downloads": status_map.get("FAILED_DOWNLOAD", 0),
            "failed_ingests": status_map.get("FAILED_INGEST", 0),
            "retryable_downloads": status_map.get("FAILED_DOWNLOAD", 0),
            "retryable_ingests": retryable_ingests,
            "reconciliation_needed": len(recon_rows),
            "not_seen_in_latest_scan": no_longer_seen_rows,
            "repeatedly_failing_downloads": repeatedly_failing_downloads,
            "repeatedly_failing_ingests": repeatedly_failing_ingests,
            "stale_pending_rows": stale_pending_rows,
        },
        "latest_scan_at": latest_scan_at,
        "recent_failures": recent_failures,
    }


def run_ftp_cycle(
    *,
    repository,
    client,
    pipeline,
    source_name: str,
    remote_directory: str,
    download_dir: Path,
    start: datetime | None,
    end: datetime | None,
    revision_policy: RevisionPolicy,
    limit: int,
    families: Sequence[str] | None = None,
    retry_failed: bool = False,
    dry_run: bool = False,
    trigger_type: str,
    source_type: str,
) -> dict[str, object]:
    started = monotonic()
    scan_result = scan_remote_files(
        repository=repository,
        client=client,
        source_name=source_name,
        remote_directory=remote_directory,
        start=start,
        end=end,
        revision_policy=revision_policy,
        families=families,
        persist=not dry_run,
    )
    candidates = scan_result["candidates"]
    candidate_by_path = {candidate.path: candidate for candidate in candidates}

    if dry_run:
        planned_downloads = repository.fetch_pending_downloads(
            source_name=source_name,
            limit=limit,
            remote_paths=list(candidate_by_path),
        )
        duration_seconds = round(monotonic() - started, 3)
        return {
            "dry_run": True,
            "retry_failed": retry_failed,
            "families": list(families) if families is not None else None,
            "summary": {
                "scanned": len(candidates),
                "downloaded": 0,
                "ingested": 0,
                "skipped_duplicates": 0,
                "failed_downloads": 0,
                "failed_ingests": 0,
                "reconciliation_needed": len(reconcile_registry_rows(repository=repository, limit=10000)),
                "duration_seconds": duration_seconds,
            },
            "stages": {
                "scan": scan_result["summary"],
                "planned_downloads": len(planned_downloads),
                "planned_remote_paths": [row["remote_path"] for row in planned_downloads],
            },
        }

    download_results = download_registry_files(
        repository=repository,
        client=client,
        source_name=source_name,
        download_dir=download_dir,
        limit=limit,
        remote_paths=list(candidate_by_path),
    )
    downloaded_ids = [
        result["remote_file_id"]
        for result in download_results
        if result["status"] == "DOWNLOADED"
    ]
    ingest_results = ingest_registry_files(
        repository=repository,
        pipeline=pipeline,
        source_name=source_name,
        limit=len(downloaded_ids),
        trigger_type=trigger_type,
        source_type=source_type,
        remote_file_ids=downloaded_ids,
    )

    retry_download_payload: dict[str, object] | None = None
    retry_ingest_payload: dict[str, object] | None = None
    if retry_failed:
        failed_download_ids = [
            result["remote_file_id"]
            for result in download_results
            if result["status"] == "FAILED_DOWNLOAD"
        ]
        retry_download_results: list[dict[str, object]] = []
        if failed_download_ids:
            retry_download_payload = retry_download_registry_files(
                repository=repository,
                client=client,
                source_name=source_name,
                download_dir=download_dir,
                remote_file_ids=failed_download_ids,
            )
            retry_download_results = retry_download_payload["results"]
            retried_downloaded_ids = [
                result["remote_file_id"]
                for result in retry_download_results
                if result["status"] == "DOWNLOADED"
            ]
            if retried_downloaded_ids:
                ingest_results.extend(
                    ingest_registry_files(
                        repository=repository,
                        pipeline=pipeline,
                        source_name=source_name,
                        limit=len(retried_downloaded_ids),
                        trigger_type=trigger_type,
                        source_type=source_type,
                        remote_file_ids=retried_downloaded_ids,
                    )
                )

        retryable_ingest_ids = [
            result["remote_file_id"]
            for result in ingest_results
            if result["status"] == "FAILED_INGEST"
            and result.get("classification") != "reconciliation_needed"
        ]
        if retryable_ingest_ids:
            retry_ingest_payload = retry_ingest_registry_files(
                repository=repository,
                pipeline=pipeline,
                source_name=source_name,
                trigger_type=trigger_type,
                source_type=source_type,
                remote_file_ids=retryable_ingest_ids,
            )
            _replace_retry_ingest_results(ingest_results, retry_ingest_payload["results"])

    duration_seconds = round(monotonic() - started, 3)
    final_download_results = download_results
    if retry_download_payload is not None:
        final_download_results = _replace_retry_download_results(
            download_results,
            retry_download_payload["results"],
        )

    payload = {
        "dry_run": False,
        "retry_failed": retry_failed,
        "families": list(families) if families is not None else None,
        "summary": {
            "scanned": len(candidates),
            "downloaded": sum(1 for result in final_download_results if result["status"] == "DOWNLOADED"),
            "ingested": sum(1 for result in ingest_results if result["status"] == "INGESTED"),
            "skipped_duplicates": sum(1 for result in ingest_results if result["status"] == "SKIPPED_DUPLICATE"),
            "failed_downloads": sum(1 for result in final_download_results if result["status"] == "FAILED_DOWNLOAD"),
            "failed_ingests": sum(1 for result in ingest_results if result["status"] == "FAILED_INGEST"),
            "reconciliation_needed": sum(
                1
                for result in ingest_results
                if result.get("classification") == "reconciliation_needed"
            ),
            "duration_seconds": duration_seconds,
        },
        "stages": {
            "scan": scan_result["summary"],
            "download_results": final_download_results,
            "ingest_results": ingest_results,
            "retry_download": retry_download_payload,
            "retry_ingest": retry_ingest_payload,
        },
    }
    return payload


def run_locked_ftp_cycle(*, lock_path: Path, **kwargs) -> dict[str, object]:
    with pipeline_cycle_lock(lock_path):
        return run_ftp_cycle(**kwargs)


def fetch_via_staged_flow(
    *,
    repository,
    client,
    pipeline,
    source_name: str,
    remote_directory: str,
    download_dir: Path,
    start: datetime | None,
    end: datetime | None,
    revision_policy: RevisionPolicy,
    limit: int,
    trigger_type: str,
    source_type: str,
) -> dict[str, object]:
    scan_result = scan_remote_files(
        repository=repository,
        client=client,
        source_name=source_name,
        remote_directory=remote_directory,
        start=start,
        end=end,
        revision_policy=revision_policy,
    )
    candidates = scan_result["candidates"]
    candidate_by_path = {candidate.path: candidate for candidate in candidates}
    download_results = download_registry_files(
        repository=repository,
        client=client,
        source_name=source_name,
        download_dir=download_dir,
        limit=limit,
        remote_paths=list(candidate_by_path),
    )
    downloaded_ids = [
        result["remote_file_id"]
        for result in download_results
        if result["status"] == "DOWNLOADED"
    ]
    ingest_results = ingest_registry_files(
        repository=repository,
        pipeline=pipeline,
        source_name=source_name,
        limit=len(downloaded_ids),
        trigger_type=trigger_type,
        source_type=source_type,
        remote_file_ids=downloaded_ids,
    )
    ingest_by_id = {result["remote_file_id"]: result for result in ingest_results}

    merged_results: list[dict[str, object]] = []
    for download_result in download_results:
        remote_path = str(download_result["remote_path"])
        candidate = candidate_by_path.get(remote_path)
        ingest_result = ingest_by_id.get(download_result["remote_file_id"])
        merged_results.append(
            {
                "remote_file": (
                    candidate.filename
                    if candidate is not None
                    else download_result.get("remote_filename") or Path(remote_path).name
                ),
                "dataset_family": candidate.dataset_family if candidate is not None else None,
                "interval_start": (
                    candidate.interval_start.strftime("%Y-%m-%d %H:%M:%S")
                    if candidate is not None
                    else None
                ),
                "revision": candidate.revision if candidate is not None else None,
                "downloaded_to": download_result.get("local_staged_path"),
                "ingest_summary": ingest_result.get("ingest_summary") if ingest_result else None,
                "status": ingest_result["status"] if ingest_result else download_result["status"],
                "error": download_result.get("error") or (ingest_result.get("error") if ingest_result else None),
            }
        )

    return {"count": len(merged_results), "results": merged_results}


def _replace_retry_download_results(
    original_results: Sequence[dict[str, object]],
    retry_results: Sequence[dict[str, object]],
) -> list[dict[str, object]]:
    retry_by_id = {result["remote_file_id"]: result for result in retry_results}
    merged: list[dict[str, object]] = []
    for result in original_results:
        merged.append(retry_by_id.get(result["remote_file_id"], result))
    return merged


def _replace_retry_ingest_results(
    original_results: list[dict[str, object]],
    retry_results: Sequence[dict[str, object]],
) -> None:
    retry_by_id = {result["remote_file_id"]: result for result in retry_results}
    for index, result in enumerate(original_results):
        replacement = retry_by_id.get(result["remote_file_id"])
        if replacement is not None:
            original_results[index] = replacement


def _map_ingest_summary(repository, remote_file_id: int, remote_path: str, summary: IngestSummary) -> dict[str, object]:
    if summary.status == "SUCCESS":
        repository.mark_ingest_succeeded(
            remote_file_id=remote_file_id,
            file_hash=summary.file_hash,
            ingest_run_id=str(summary.run_id),
            final_file_path=summary.final_file_path,
        )
        return {
            "remote_file_id": remote_file_id,
            "remote_path": remote_path,
            "status": "INGESTED",
            "file_hash": summary.file_hash,
            "ingest_run_id": str(summary.run_id),
            "final_file_path": summary.final_file_path,
            "ingest_summary": summary.as_dict(),
        }
    if summary.status == "SKIPPED_DUPLICATE":
        repository.mark_ingest_skipped_duplicate(
            remote_file_id=remote_file_id,
            file_hash=summary.file_hash,
            ingest_run_id=str(summary.run_id),
            final_file_path=summary.final_file_path,
        )
        return {
            "remote_file_id": remote_file_id,
            "remote_path": remote_path,
            "status": "SKIPPED_DUPLICATE",
            "file_hash": summary.file_hash,
            "ingest_run_id": str(summary.run_id),
            "final_file_path": summary.final_file_path,
            "ingest_summary": summary.as_dict(),
        }

    error_message = f"unexpected pipeline status: {summary.status}"
    repository.mark_ingest_failed(
        remote_file_id=remote_file_id,
        error_message=error_message,
    )
    return {
        "remote_file_id": remote_file_id,
        "remote_path": remote_path,
        "status": "FAILED_INGEST",
        "error": error_message,
        "ingest_summary": {
            "status": "FAILED_INGEST",
            "error_message": error_message,
        },
    }


def annotate_registry_row(
    row: dict[str, object],
    *,
    latest_scan_at: datetime | None = None,
) -> dict[str, object]:
    annotated = dict(row)
    classification, reason = classify_registry_row(row, latest_scan_at=latest_scan_at)
    annotated["classification"] = classification
    annotated["classification_reason"] = reason
    return annotated


def classify_registry_row(
    row: dict[str, object],
    *,
    latest_scan_at: datetime | None = None,
) -> tuple[str, str]:
    status = row["status"]
    download_attempt_count = int(row.get("download_attempt_count") or 0)
    ingest_attempt_count = int(row.get("ingest_attempt_count") or 0)
    local_staged_path = row.get("local_staged_path")
    status_updated_at = row.get("status_updated_at")
    row_last_seen_at = row.get("last_seen_at")
    if status == "FAILED_DOWNLOAD" and download_attempt_count >= 3:
        return "repeatedly_failing_download", "download has failed at least three times"
    if status == "FAILED_INGEST" and ingest_attempt_count >= 3:
        if _stage_exists(local_staged_path):
            return "repeatedly_failing_ingest", "ingest has failed at least three times"
        return "reconciliation_needed", _missing_stage_reason(local_staged_path)
    if status == "FAILED_DOWNLOAD":
        return "retryable_download", "download failed and can be retried from the registry"
    if status == "DISCOVERED":
        if _is_stale_pending(status_updated_at):
            return "stale_pending", "discovered row has been pending longer than the stale threshold"
        if latest_scan_at is not None and row_last_seen_at is not None and row_last_seen_at < latest_scan_at:
            return "not_seen_in_latest_scan", "row was discovered previously but not seen in the latest scan"
        return "pending_download", "remote file discovered but not yet downloaded"
    if status in {"DOWNLOADED", "FAILED_INGEST"}:
        if not _stage_exists(local_staged_path):
            return "reconciliation_needed", _missing_stage_reason(local_staged_path)
        if status == "FAILED_INGEST":
            return "retryable_ingest", "staged file exists and ingest can be retried"
        if _is_stale_pending(status_updated_at):
            return "stale_pending", "downloaded row has been pending longer than the stale threshold"
        if latest_scan_at is not None and row_last_seen_at is not None and row_last_seen_at < latest_scan_at:
            return "not_seen_in_latest_scan", "row was discovered previously but not seen in the latest scan"
        return "pending_ingest", "downloaded and staged for ingest"
    if status == "INGESTED":
        return "completed", "ingest completed successfully"
    if status == "SKIPPED_DUPLICATE":
        return "completed", "ingest skipped as duplicate"
    return "unknown", f"unrecognized status: {status}"


def _stage_exists(local_staged_path: object) -> bool:
    if not local_staged_path:
        return False
    return Path(str(local_staged_path)).exists()


def _missing_stage_reason(local_staged_path: object) -> str:
    if not local_staged_path:
        return "local_staged_path is missing"
    return f"staged file not found: {Path(str(local_staged_path))}"


def _is_stale_pending(status_updated_at: object) -> bool:
    if not isinstance(status_updated_at, datetime):
        return False
    threshold = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=24)
    return status_updated_at < threshold


def _download_rows(
    *,
    repository,
    client,
    rows: Sequence[dict],
    download_dir: Path,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for row in rows:
        try:
            local_path = client.download_file(row["remote_path"], download_dir)
            repository.mark_download_succeeded(
                remote_file_id=row["id"],
                local_staged_path=str(local_path),
            )
            results.append(
                {
                    "remote_file_id": row["id"],
                    "remote_path": row["remote_path"],
                    "remote_filename": row.get("remote_filename"),
                    "status": "DOWNLOADED",
                    "local_staged_path": str(local_path),
                }
            )
        except Exception as exc:
            repository.mark_download_failed(
                remote_file_id=row["id"],
                error_message=str(exc),
            )
            results.append(
                {
                    "remote_file_id": row["id"],
                    "remote_path": row["remote_path"],
                    "remote_filename": row.get("remote_filename"),
                    "status": "FAILED_DOWNLOAD",
                    "error": str(exc),
                }
            )
    return results


def _ingest_rows(
    *,
    repository,
    pipeline,
    rows: Sequence[dict],
    trigger_type: str,
    source_type: str,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for row in rows:
        local_staged_path = row.get("local_staged_path")
        if not local_staged_path:
            error_message = "local_staged_path is missing for staged ingest."
            repository.mark_ingest_failed(
                remote_file_id=row["id"],
                error_message=error_message,
            )
            results.append(
                {
                    "remote_file_id": row["id"],
                    "remote_path": row["remote_path"],
                    "status": "FAILED_INGEST",
                    "error": error_message,
                    "classification": "reconciliation_needed",
                    "ingest_summary": {
                        "status": "FAILED_INGEST",
                        "error_message": error_message,
                    },
                }
            )
            continue

        staged_path = Path(str(local_staged_path))
        if not staged_path.exists():
            error_message = f"staged file not found: {staged_path}"
            repository.mark_ingest_failed(
                remote_file_id=row["id"],
                error_message=error_message,
            )
            results.append(
                {
                    "remote_file_id": row["id"],
                    "remote_path": row["remote_path"],
                    "status": "FAILED_INGEST",
                    "error": error_message,
                    "classification": "reconciliation_needed",
                    "ingest_summary": {
                        "status": "FAILED_INGEST",
                        "error_message": error_message,
                    },
                }
            )
            continue

        try:
            summary = pipeline.load_zip(
                staged_path,
                trigger_type=trigger_type,
                source_type=source_type,
            )
            results.append(_map_ingest_summary(repository, row["id"], row["remote_path"], summary))
        except Exception as exc:
            repository.mark_ingest_failed(
                remote_file_id=row["id"],
                error_message=str(exc),
            )
            results.append(
                {
                    "remote_file_id": row["id"],
                    "remote_path": row["remote_path"],
                    "status": "FAILED_INGEST",
                    "error": str(exc),
                    "classification": "retryable_ingest",
                    "ingest_summary": {
                        "status": "FAILED_INGEST",
                        "error_message": str(exc),
                    },
                }
            )
    return results
