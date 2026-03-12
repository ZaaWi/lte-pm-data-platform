from uuid import uuid4

from lte_pm_platform.domain.models import IngestSummary


def test_ingest_summary_as_dict_sorts_unknown_columns() -> None:
    summary = IngestSummary(
        source_file="sample.zip",
        run_id=uuid4(),
        trigger_type="local_cli",
        source_type="local",
        file_hash="abc123",
        csv_files_found=1,
        input_rows_read=2,
        normalized_rows_emitted=4,
        rows_inserted=4,
        unknown_columns={"Z_COL", "A_COL"},
        null_counter_values=1,
        status="SUCCESS",
        lifecycle_action="archived",
        final_file_path="/tmp/archive/sample.zip",
    )

    payload = summary.as_dict()

    assert payload["source_file"] == "sample.zip"
    assert payload["run_id"] == str(summary.run_id)
    assert payload["trigger_type"] == "local_cli"
    assert payload["source_type"] == "local"
    assert payload["file_hash"] == "abc123"
    assert payload["csv_files_found"] == 1
    assert payload["input_rows_read"] == 2
    assert payload["normalized_rows_emitted"] == 4
    assert payload["rows_inserted"] == 4
    assert payload["unknown_columns"] == ["A_COL", "Z_COL"]
    assert payload["null_counter_values"] == 1
    assert payload["status"] == "SUCCESS"
    assert payload["error_message"] is None
    assert payload["lifecycle_action"] == "archived"
    assert payload["final_file_path"] == "/tmp/archive/sample.zip"
