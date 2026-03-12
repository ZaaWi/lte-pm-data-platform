from datetime import datetime
from pathlib import Path

from lte_pm_platform.pipeline.ingest.file_discovery import (
    apply_revision_policy,
    discover_local_files,
    filter_by_time_range,
    parse_archive_filename,
    select_parsed_files,
)


def test_parse_archive_filename_for_base_file() -> None:
    parsed = parse_archive_filename("UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1330.tar.gz")

    assert parsed is not None
    assert parsed.dataset_family == "PM/itbbu/ltefdd"
    assert parsed.interval_start == datetime(2026, 3, 5, 13, 30)
    assert parsed.revision == 0


def test_parse_archive_filename_for_revision_file() -> None:
    parsed = parse_archive_filename("UMEID_LTEFDD_PM_COMMON_ZTE_20260305_1415_R2.tar.gz")

    assert parsed is not None
    assert parsed.dataset_family == "PM/sdr/ltefdd"
    assert parsed.interval_start == datetime(2026, 3, 5, 14, 15)
    assert parsed.revision == 2


def test_filter_by_time_range_is_inclusive() -> None:
    parsed_files = select_parsed_files(
        [
            "UMEID_ITBBU_ITBBUPLAT_PM_COMMON_ZTE_20260305_1315.tar.gz",
            "UMEID_ITBBU_ITBBUPLAT_PM_COMMON_ZTE_20260305_1330.tar.gz",
            "UMEID_ITBBU_ITBBUPLAT_PM_COMMON_ZTE_20260305_1345.tar.gz",
        ]
    )

    filtered = filter_by_time_range(
        parsed_files,
        start=datetime(2026, 3, 5, 13, 30),
        end=datetime(2026, 3, 5, 14, 0),
    )

    assert [file.filename for file in filtered] == [
        "UMEID_ITBBU_ITBBUPLAT_PM_COMMON_ZTE_20260305_1330.tar.gz",
        "UMEID_ITBBU_ITBBUPLAT_PM_COMMON_ZTE_20260305_1345.tar.gz",
    ]


def test_filter_by_time_range_uses_exclusive_upper_bound() -> None:
    parsed_files = select_parsed_files(
        [
            "UMEID_ITBBU_ITBBUPLAT_PM_COMMON_ZTE_20260305_1330.tar.gz",
            "UMEID_ITBBU_ITBBUPLAT_PM_COMMON_ZTE_20260305_1345.tar.gz",
        ]
    )

    filtered = filter_by_time_range(
        parsed_files,
        start=datetime(2026, 3, 5, 13, 30),
        end=datetime(2026, 3, 5, 13, 45),
    )

    assert [file.filename for file in filtered] == [
        "UMEID_ITBBU_ITBBUPLAT_PM_COMMON_ZTE_20260305_1330.tar.gz"
    ]


def test_revision_policy_additive_keeps_base_and_revisions() -> None:
    selected = select_parsed_files(
        [
            "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1330.tar.gz",
            "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1330_R1.tar.gz",
            "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1330_R2.tar.gz",
        ],
        revision_policy="additive",
    )

    assert [file.revision for file in selected] == [0, 1, 2]


def test_revision_policy_latest_only_keeps_highest_revision_per_interval() -> None:
    selected = select_parsed_files(
        [
            "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1330.tar.gz",
            "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1330_R1.tar.gz",
            "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1345.tar.gz",
        ],
        revision_policy="latest-only",
    )

    assert [(file.interval_start, file.revision) for file in selected] == [
        (datetime(2026, 3, 5, 13, 30), 1),
        (datetime(2026, 3, 5, 13, 45), 0),
    ]


def test_revision_policy_base_only_filters_out_revisions() -> None:
    selected = select_parsed_files(
        [
            "UMEID_LTEFDD_PM_COMMON_ZTE_20260305_1415.tar.gz",
            "UMEID_LTEFDD_PM_COMMON_ZTE_20260305_1415_R1.tar.gz",
        ],
        revision_policy="base-only",
    )

    assert [file.filename for file in selected] == [
        "UMEID_LTEFDD_PM_COMMON_ZTE_20260305_1415.tar.gz"
    ]


def test_revision_policy_revisions_only_filters_out_base() -> None:
    selected = select_parsed_files(
        [
            "UMEID_LTEFDD_PM_COMMON_ZTE_20260305_1415.tar.gz",
            "UMEID_LTEFDD_PM_COMMON_ZTE_20260305_1415_R1.tar.gz",
        ],
        revision_policy="revisions-only",
    )

    assert [file.filename for file in selected] == [
        "UMEID_LTEFDD_PM_COMMON_ZTE_20260305_1415_R1.tar.gz"
    ]


def test_discover_local_files_applies_family_time_and_revision_filters(tmp_path: Path) -> None:
    itbbu_root = tmp_path / "itbbu_ltefdd"
    sdr_root = tmp_path / "sdr_ltefdd"
    itbbu_root.mkdir()
    sdr_root.mkdir()
    (itbbu_root / "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1330.tar.gz").write_bytes(b"a")
    (itbbu_root / "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1330_R1.tar.gz").write_bytes(b"b")
    (itbbu_root / "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1400.tar.gz").write_bytes(b"c")
    (sdr_root / "UMEID_LTEFDD_PM_COMMON_ZTE_20260305_1330.tar.gz").write_bytes(b"d")

    selected = discover_local_files(
        source_roots={
            "PM/itbbu/ltefdd": itbbu_root,
            "PM/sdr/ltefdd": sdr_root,
        },
        families=["PM/itbbu/ltefdd"],
        start=datetime(2026, 3, 5, 13, 30),
        end=datetime(2026, 3, 5, 13, 31),
        revision_policy="additive",
    )

    assert [Path(file.path).name for file in selected] == [
        "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1330.tar.gz",
        "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260305_1330_R1.tar.gz",
    ]


def test_apply_revision_policy_latest_only_with_base_and_r1() -> None:
    parsed_files = [
        parse_archive_filename("UMEID_ITBBU_ITBBUPLAT_PM_COMMON_ZTE_20260305_1330.tar.gz"),
        parse_archive_filename("UMEID_ITBBU_ITBBUPLAT_PM_COMMON_ZTE_20260305_1330_R1.tar.gz"),
    ]

    selected = apply_revision_policy(
        [file for file in parsed_files if file is not None],
        "latest-only",
    )

    assert len(selected) == 1
    assert selected[0].revision == 1
