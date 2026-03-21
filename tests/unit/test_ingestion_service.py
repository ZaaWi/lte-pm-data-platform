from __future__ import annotations

from lte_pm_platform.services.ingestion_service import IngestionService


class FakeConnection:
    pass


def test_list_source_intervals_adds_partial_interval_quality(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        "lte_pm_platform.services.ingestion_service.FtpRemoteFileRepository",
        lambda connection: type(
            "Repo",
            (),
            {
                "summarize_source_intervals": lambda self, limit: [
                    {
                        "interval_start": "2026-03-15T13:45:00",
                        "total_files": 2,
                        "families_present": ["PM/itbbu/ltefdd", "PM/sdr/ltefdd"],
                        "family_count": 2,
                        "statuses_present": ["DISCOVERED"],
                        "max_revision": 0,
                        "last_seen_at": "2026-03-15T23:54:31.977638",
                        "last_scan_at": "2026-03-15T23:54:31.977638",
                    }
                ],
                "list_ingested_interval_source_files": lambda self, **kwargs: [
                    {
                        "interval_start": "2026-03-15T13:45:00",
                        "dataset_family": "PM/itbbu/ltefdd",
                        "remote_filename": "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260315_1345.tar.gz",
                    },
                    {
                        "interval_start": "2026-03-15T13:45:00",
                        "dataset_family": "PM/sdr/ltefdd",
                        "remote_filename": "UMEID_LTEFDD_PM_COMMON_ZTE_20260315_1345.tar.gz",
                    },
                ],
            },
        )(),
    )
    monkeypatch.setattr(
        "lte_pm_platform.services.ingestion_service.PmSampleRepository",
        lambda connection: type(
            "PmRepo",
            (),
            {
                "summarize_interval_topology_coverage": lambda self, **kwargs: [
                    {
                        "interval_start": "2026-03-15T13:45:00",
                        "topology_mapped_count": 12,
                        "topology_unmapped_count": 3,
                    }
                ]
            },
        )(),
    )

    rows = IngestionService(FakeConnection()).list_source_intervals(limit=10)

    assert rows[0]["families_present"] == ["PM/itbbu/ltefdd", "PM/sdr/ltefdd"]
    assert rows[0]["missing_families"] == []
    assert rows[0]["partial_interval"] is False
    assert rows[0]["quality_status"] == "complete"
    assert rows[0]["quality_notes"] == "Required LTE PM families discovered for this interval."
    assert rows[0]["topology_mapped_count"] == 12
    assert rows[0]["topology_unmapped_count"] == 3
    assert rows[0]["topology_coverage_pct"] == 80.0


def test_list_source_intervals_marks_missing_required_family_as_partial(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        "lte_pm_platform.services.ingestion_service.FtpRemoteFileRepository",
        lambda connection: type(
            "Repo",
            (),
            {
                "summarize_source_intervals": lambda self, limit: [
                    {
                        "interval_start": "2026-03-15T13:45:00",
                        "total_files": 2,
                        "families_present": ["PM/itbbu/itbbuplat", "PM/sdr/ltefdd"],
                        "family_count": 2,
                        "statuses_present": ["DISCOVERED"],
                        "max_revision": 0,
                        "last_seen_at": "2026-03-15T23:54:31.977638",
                        "last_scan_at": "2026-03-15T23:54:31.977638",
                    }
                ],
                "list_ingested_interval_source_files": lambda self, **kwargs: [],
            },
        )(),
    )
    monkeypatch.setattr(
        "lte_pm_platform.services.ingestion_service.PmSampleRepository",
        lambda connection: type(
            "PmRepo",
            (),
            {"summarize_interval_topology_coverage": lambda self, **kwargs: []},
        )(),
    )

    rows = IngestionService(FakeConnection()).list_source_intervals(limit=10)

    assert rows[0]["families_present"] == ["PM/itbbu/itbbuplat", "PM/sdr/ltefdd"]
    assert rows[0]["missing_families"] == ["PM/itbbu/ltefdd"]
    assert rows[0]["partial_interval"] is True
    assert rows[0]["quality_status"] == "partial"
    assert rows[0]["quality_notes"] == "Missing expected families: PM/itbbu/ltefdd"
    assert rows[0]["topology_mapped_count"] == 0
    assert rows[0]["topology_unmapped_count"] == 0
    assert rows[0]["topology_coverage_pct"] is None


def test_list_source_intervals_marks_all_expected_families_present(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        "lte_pm_platform.services.ingestion_service.FtpRemoteFileRepository",
        lambda connection: type(
            "Repo",
            (),
            {
                "summarize_source_intervals": lambda self, limit: [
                    {
                        "interval_start": "2026-03-15T13:45:00",
                        "total_files": 3,
                        "families_present": ["PM/itbbu/itbbuplat", "PM/itbbu/ltefdd", "PM/sdr/ltefdd"],
                        "family_count": 3,
                        "statuses_present": ["DISCOVERED"],
                        "max_revision": 0,
                        "last_seen_at": "2026-03-15T23:54:31.977638",
                        "last_scan_at": "2026-03-15T23:54:31.977638",
                    }
                ],
                "list_ingested_interval_source_files": lambda self, **kwargs: [],
            },
        )(),
    )
    monkeypatch.setattr(
        "lte_pm_platform.services.ingestion_service.PmSampleRepository",
        lambda connection: type(
            "PmRepo",
            (),
            {"summarize_interval_topology_coverage": lambda self, **kwargs: []},
        )(),
    )

    rows = IngestionService(FakeConnection()).list_source_intervals(limit=10)

    assert rows[0]["missing_families"] == []
    assert rows[0]["partial_interval"] is False
    assert rows[0]["quality_status"] == "complete"
    assert rows[0]["quality_notes"] == "Required LTE PM families discovered for this interval."


def test_list_source_intervals_uses_only_ingested_source_files_for_topology(monkeypatch) -> None:  # noqa: ANN001
    recorded: dict[str, object] = {}

    monkeypatch.setattr(
        "lte_pm_platform.services.ingestion_service.FtpRemoteFileRepository",
        lambda connection: type(
            "Repo",
            (),
            {
                "summarize_source_intervals": lambda self, limit: [
                    {
                        "interval_start": "2026-03-15T13:45:00",
                        "total_files": 3,
                        "families_present": ["PM/itbbu/ltefdd", "PM/sdr/ltefdd"],
                        "family_count": 2,
                        "statuses_present": ["DISCOVERED", "INGESTED"],
                        "max_revision": 0,
                        "last_seen_at": "2026-03-15T23:54:31.977638",
                        "last_scan_at": "2026-03-15T23:54:31.977638",
                    }
                ],
                "list_ingested_interval_source_files": lambda self, **kwargs: [
                    {
                        "interval_start": "2026-03-15T13:45:00",
                        "dataset_family": "PM/itbbu/ltefdd",
                        "remote_filename": "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260315_1345.tar.gz",
                    },
                    {
                        "interval_start": "2026-03-15T13:45:00",
                        "dataset_family": "PM/sdr/ltefdd",
                        "remote_filename": "UMEID_LTEFDD_PM_COMMON_ZTE_20260315_1345.tar.gz",
                    },
                ],
            },
        )(),
    )

    def summarize_interval_topology_coverage(self, *, source_files):  # noqa: ANN001, ANN202
        recorded["source_files"] = list(source_files)
        return [
            {
                "interval_start": "2026-03-15T13:45:00",
                "topology_mapped_count": 4,
                "topology_unmapped_count": 1,
            }
        ]

    monkeypatch.setattr(
        "lte_pm_platform.services.ingestion_service.PmSampleRepository",
        lambda connection: type(
            "PmRepo",
            (),
            {"summarize_interval_topology_coverage": summarize_interval_topology_coverage},
        )(),
    )

    rows = IngestionService(FakeConnection()).list_source_intervals(limit=10)

    assert recorded["source_files"] == [
        "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260315_1345.tar.gz",
        "UMEID_LTEFDD_PM_COMMON_ZTE_20260315_1345.tar.gz",
    ]
    assert rows[0]["topology_mapped_count"] == 4
    assert rows[0]["topology_unmapped_count"] == 1
    assert rows[0]["topology_coverage_pct"] == 80.0
