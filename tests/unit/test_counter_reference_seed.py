from pathlib import Path

import pytest

from lte_pm_platform.pipeline.ingest.counter_reference_seed import load_counter_reference_seed


def test_load_counter_reference_seed_parses_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "counter_reference.csv"
    csv_path.write_text(
        (
            "counter_id,vendor,technology,object_type,description,unit,source_type,source_reference,"
            "verification_status,verified_at,notes\n"
            "C1,ZTE,LTE,cell,PRB used,percent,vendor_doc,"
            "zte_doc.pdf#C1,VERIFIED,2026-03-12T10:00:00,ok\n"
        ),
        encoding="utf-8",
    )

    rows = load_counter_reference_seed(csv_path)

    assert len(rows) == 1
    assert rows[0].counter_id == "C1"
    assert rows[0].verification_status == "VERIFIED"
    assert rows[0].source_reference == "zte_doc.pdf#C1"


def test_load_counter_reference_seed_rejects_invalid_verification_status(tmp_path: Path) -> None:
    csv_path = tmp_path / "counter_reference.csv"
    csv_path.write_text(
        (
            "counter_id,vendor,technology,object_type,description,unit,source_type,source_reference,"
            "verification_status,verified_at,notes\n"
            "C1,ZTE,LTE,cell,PRB used,percent,vendor_doc,"
            "zte_doc.pdf#C1,BAD,2026-03-12T10:00:00,ok\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid verification_status"):
        load_counter_reference_seed(csv_path)


def test_load_counter_reference_seed_defaults_verified_timestamp_for_verified_rows(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "counter_reference.csv"
    csv_path.write_text(
        (
            "counter_id,vendor,technology,object_type,description,unit,source_type,source_reference,"
            "verification_status,verified_at,notes\n"
            "C1,ZTE,LTE,cell,PRB used,percent,vendor_doc,zte_doc.pdf#C1,VERIFIED,,ok\n"
        ),
        encoding="utf-8",
    )

    rows = load_counter_reference_seed(csv_path)

    assert rows[0].verified_at is not None
