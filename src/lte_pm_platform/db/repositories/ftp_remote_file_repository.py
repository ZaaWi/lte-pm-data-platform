from __future__ import annotations

from collections.abc import Sequence

from psycopg import Connection
from psycopg.rows import dict_row

from lte_pm_platform.pipeline.ingest.file_discovery import ParsedArchiveFile


class FtpRemoteFileRepository:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def commit(self) -> None:
        self.connection.commit()

    def upsert_discovered_files(
        self,
        *,
        source_name: str,
        remote_directory: str,
        files: Sequence[ParsedArchiveFile],
    ) -> dict[str, int]:
        if not files:
            return {"discovered": 0, "updated": 0}

        remote_paths = [file.path for file in files]
        existing_paths = self._fetch_existing_paths(
            source_name=source_name,
            remote_paths=remote_paths,
        )
        discovered = 0
        updated = 0

        with self.connection.cursor() as cursor:
            for file in files:
                params = (
                    source_name,
                    remote_directory,
                    file.filename,
                    file.path,
                    file.dataset_family,
                    file.interval_start,
                    file.revision,
                    file.extension,
                    file.remote_size_bytes,
                    file.remote_modified_at,
                )
                if file.path in existing_paths:
                    cursor.execute(
                        """
                        UPDATE ftp_remote_file
                        SET
                            remote_directory = %s,
                            remote_filename = %s,
                            dataset_family = %s,
                            interval_start = %s,
                            revision = %s,
                            extension = %s,
                            remote_size_bytes = %s,
                            remote_modified_at = %s,
                            last_seen_at = NOW(),
                            last_scan_at = NOW(),
                            updated_at = NOW()
                        WHERE source_name = %s
                          AND remote_path = %s
                        """,
                        (
                            remote_directory,
                            file.filename,
                            file.dataset_family,
                            file.interval_start,
                            file.revision,
                            file.extension,
                            file.remote_size_bytes,
                            file.remote_modified_at,
                            source_name,
                            file.path,
                        ),
                    )
                    updated += 1
                    continue

                cursor.execute(
                    """
                    INSERT INTO ftp_remote_file (
                        source_name,
                        remote_directory,
                        remote_filename,
                        remote_path,
                        dataset_family,
                        interval_start,
                        revision,
                        extension,
                        remote_size_bytes,
                        remote_modified_at,
                        status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'DISCOVERED')
                    """,
                    params,
                )
                discovered += 1

        return {"discovered": discovered, "updated": updated}

    def summarize_status_counts(self) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    status,
                    COUNT(*) AS file_count
                FROM ftp_remote_file
                GROUP BY status
                ORDER BY status
                """
            )
            return list(cursor.fetchall())

    def summarize_source_intervals(self, *, limit: int = 50) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    interval_start,
                    COUNT(*) AS total_files,
                    ARRAY_AGG(DISTINCT dataset_family ORDER BY dataset_family)
                        FILTER (WHERE dataset_family IS NOT NULL) AS families_present,
                    COUNT(DISTINCT dataset_family) AS family_count,
                    ARRAY_AGG(DISTINCT status ORDER BY status)
                        FILTER (WHERE status IS NOT NULL) AS statuses_present,
                    MAX(revision) AS max_revision,
                    MAX(last_seen_at) AS last_seen_at,
                    MAX(last_scan_at) AS last_scan_at
                FROM ftp_remote_file
                WHERE interval_start IS NOT NULL
                GROUP BY interval_start
                ORDER BY interval_start DESC
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def list_ingested_interval_source_files(
        self,
        *,
        interval_starts: Sequence[object],
        dataset_families: Sequence[str],
    ) -> list[dict]:
        if not interval_starts or not dataset_families:
            return []
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT DISTINCT
                    interval_start,
                    dataset_family,
                    remote_filename
                FROM ftp_remote_file
                WHERE interval_start = ANY(%s)
                  AND dataset_family = ANY(%s)
                  AND status = 'INGESTED'
                  AND remote_filename IS NOT NULL
                ORDER BY interval_start DESC, dataset_family, remote_filename
                """,
                (list(interval_starts), list(dataset_families)),
            )
            return list(cursor.fetchall())

    def fetch_remote_file_by_id(self, *, remote_file_id: int) -> dict | None:
        rows = self.fetch_registry_rows(remote_file_ids=[remote_file_id], limit=1)
        return rows[0] if rows else None

    def fetch_registry_rows(
        self,
        *,
        statuses: Sequence[str] | None = None,
        remote_file_ids: Sequence[int] | None = None,
        limit: int = 100,
    ) -> list[dict]:
        query = """
                SELECT
                    id,
                    source_name,
                    remote_directory,
                    remote_filename,
                    remote_path,
                    dataset_family,
                    interval_start,
                    revision,
                    extension,
                    remote_size_bytes,
                    remote_modified_at,
                    status,
                    download_attempt_count,
                    ingest_attempt_count,
                    last_download_attempt_at,
                    last_ingest_attempt_at,
                    download_attempts,
                    local_staged_path,
                    file_hash,
                    ingest_run_id,
                    final_file_path,
                    last_error,
                    first_seen_at,
                    last_seen_at,
                    last_scan_at,
                    status_updated_at,
                    updated_at
                FROM ftp_remote_file
                WHERE TRUE
        """
        params: list[object] = []
        if statuses is not None:
            query += """
                  AND status = ANY(%s)
            """
            params.append(list(statuses))
        if remote_file_ids is not None:
            query += """
                  AND id = ANY(%s)
            """
            params.append(list(remote_file_ids))
        query += """
                ORDER BY status_updated_at DESC, id DESC
                LIMIT %s
        """
        params.append(limit)
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())

    def fetch_failure_rows(self, *, limit: int = 100) -> list[dict]:
        return self.fetch_registry_rows(
            statuses=["FAILED_DOWNLOAD", "FAILED_INGEST"],
            limit=limit,
        )

    def fetch_retry_download_rows(
        self,
        *,
        source_name: str,
        remote_file_ids: Sequence[int],
    ) -> list[dict]:
        query = """
                SELECT
                    id,
                    source_name,
                    remote_directory,
                    remote_filename,
                    remote_path,
                    dataset_family,
                    interval_start,
                    revision,
                    extension,
                    remote_size_bytes,
                    remote_modified_at,
                    status
                FROM ftp_remote_file
                WHERE source_name = %s
                  AND status = 'FAILED_DOWNLOAD'
                  AND id = ANY(%s)
                ORDER BY interval_start NULLS LAST, revision, remote_path
        """
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, (source_name, list(remote_file_ids)))
            return list(cursor.fetchall())

    def fetch_retry_ingest_rows(
        self,
        *,
        source_name: str,
        remote_file_ids: Sequence[int],
    ) -> list[dict]:
        query = """
                SELECT
                    id,
                    source_name,
                    remote_directory,
                    remote_filename,
                    remote_path,
                    local_staged_path,
                    dataset_family,
                    interval_start,
                    revision,
                    extension,
                    remote_size_bytes,
                    remote_modified_at,
                    status
                FROM ftp_remote_file
                WHERE source_name = %s
                  AND status = 'FAILED_INGEST'
                  AND id = ANY(%s)
                ORDER BY interval_start NULLS LAST, revision, remote_path
        """
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, (source_name, list(remote_file_ids)))
            return list(cursor.fetchall())

    def fetch_pending_downloads(
        self,
        *,
        source_name: str,
        limit: int,
        remote_paths: Sequence[str] | None = None,
    ) -> list[dict]:
        query = """
                SELECT
                    id,
                    source_name,
                    remote_directory,
                    remote_filename,
                    remote_path,
                    dataset_family,
                    interval_start,
                    revision,
                    extension,
                    status
                FROM ftp_remote_file
                WHERE source_name = %s
                  AND status IN ('DISCOVERED', 'FAILED_DOWNLOAD')
            """
        params: list[object] = [source_name]
        if remote_paths is not None:
            query += """
                  AND remote_path = ANY(%s)
            """
            params.append(list(remote_paths))
        query += """
                ORDER BY interval_start NULLS LAST, revision, remote_path
                LIMIT %s
        """
        params.append(limit)
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())

    def mark_download_succeeded(
        self,
        *,
        remote_file_id: int,
        local_staged_path: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ftp_remote_file
                SET
                    status = 'DOWNLOADED',
                    download_attempts = download_attempts + 1,
                    download_attempt_count = download_attempt_count + 1,
                    last_download_attempt_at = NOW(),
                    local_staged_path = %s,
                    last_error = NULL,
                    status_updated_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (local_staged_path, remote_file_id),
            )

    def mark_download_failed(
        self,
        *,
        remote_file_id: int,
        error_message: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ftp_remote_file
                SET
                    status = 'FAILED_DOWNLOAD',
                    download_attempts = download_attempts + 1,
                    download_attempt_count = download_attempt_count + 1,
                    last_download_attempt_at = NOW(),
                    last_error = %s,
                    status_updated_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (error_message, remote_file_id),
            )

    def fetch_pending_ingests(
        self,
        *,
        source_name: str,
        limit: int,
        remote_file_ids: Sequence[int] | None = None,
    ) -> list[dict]:
        query = """
                SELECT
                    id,
                    source_name,
                    remote_directory,
                    remote_filename,
                    remote_path,
                    local_staged_path,
                    dataset_family,
                    interval_start,
                    revision,
                    extension,
                    remote_size_bytes,
                    remote_modified_at,
                    status
                FROM ftp_remote_file
                WHERE source_name = %s
                  AND status IN ('DOWNLOADED', 'FAILED_INGEST')
            """
        params: list[object] = [source_name]
        if remote_file_ids is not None:
            query += """
                  AND id = ANY(%s)
            """
            params.append(list(remote_file_ids))
        query += """
                ORDER BY interval_start NULLS LAST, revision, remote_path
                LIMIT %s
        """
        params.append(limit)
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())

    def mark_ingest_succeeded(
        self,
        *,
        remote_file_id: int,
        file_hash: str | None,
        ingest_run_id: str,
        final_file_path: str | None,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ftp_remote_file
                SET
                    status = 'INGESTED',
                    ingest_attempt_count = ingest_attempt_count + 1,
                    last_ingest_attempt_at = NOW(),
                    file_hash = %s,
                    ingest_run_id = %s,
                    final_file_path = %s,
                    last_error = NULL,
                    status_updated_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (file_hash, ingest_run_id, final_file_path, remote_file_id),
            )

    def mark_ingest_skipped_duplicate(
        self,
        *,
        remote_file_id: int,
        file_hash: str | None,
        ingest_run_id: str,
        final_file_path: str | None,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ftp_remote_file
                SET
                    status = 'SKIPPED_DUPLICATE',
                    ingest_attempt_count = ingest_attempt_count + 1,
                    last_ingest_attempt_at = NOW(),
                    file_hash = %s,
                    ingest_run_id = %s,
                    final_file_path = %s,
                    last_error = NULL,
                    status_updated_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (file_hash, ingest_run_id, final_file_path, remote_file_id),
            )

    def mark_ingest_failed(
        self,
        *,
        remote_file_id: int,
        error_message: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ftp_remote_file
                SET
                    status = 'FAILED_INGEST',
                    ingest_attempt_count = ingest_attempt_count + 1,
                    last_ingest_attempt_at = NOW(),
                    last_error = %s,
                    status_updated_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (error_message, remote_file_id),
            )

    def fetch_recent_failures(self, limit: int = 10) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    source_name,
                    remote_path,
                    status,
                    last_error,
                    remote_size_bytes,
                    remote_modified_at,
                    first_seen_at,
                    last_seen_at,
                    last_scan_at,
                    download_attempt_count,
                    ingest_attempt_count,
                    last_download_attempt_at,
                    last_ingest_attempt_at,
                    updated_at,
                FROM ftp_remote_file
                WHERE status IN ('FAILED_DOWNLOAD', 'FAILED_INGEST')
                ORDER BY updated_at DESC, remote_path
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def fetch_latest_scan_at(self) -> object | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT MAX(last_scan_at)
                FROM ftp_remote_file
                """,
                (),
            )
            row = cursor.fetchone()
        return row[0] if row is not None else None

    def _fetch_existing_paths(
        self,
        *,
        source_name: str,
        remote_paths: Sequence[str],
    ) -> set[str]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT remote_path
                FROM ftp_remote_file
                WHERE source_name = %s
                  AND remote_path = ANY(%s)
                """,
                (source_name, list(remote_paths)),
            )
            rows = cursor.fetchall()
        return {row[0] for row in rows}
