from lte_pm_platform.db.repositories.pm_sample_repository import PmSampleRepository


class FakeCursor:
    def __init__(self, rows) -> None:  # noqa: ANN001
        self.rows = rows
        self.executed: list[tuple[str, tuple]] = []

    def execute(self, query: str, params: tuple) -> None:
        self.executed.append((query, params))

    def fetchall(self):  # noqa: ANN201
        return self.rows

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


class FakeConnection:
    def __init__(self, rows) -> None:  # noqa: ANN001
        self.cursor_obj = FakeCursor(rows)

    def cursor(self, **kwargs):  # noqa: ANN003, ANN201
        return self.cursor_obj


def test_list_seen_counters_returns_grouped_rows() -> None:
    rows = [{"counter_id": "C1", "seen_rows": 10}]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.list_seen_counters(limit=3)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (3,)


def test_top_counters_returns_grouped_rows() -> None:
    rows = [{"counter_id": "C1", "row_count": 10}]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.top_counters(limit=11)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (11,)


def test_summarize_by_source_file_returns_grouped_rows() -> None:
    rows = [{"source_file": "sample.zip", "normalized_rows": 4}]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.summarize_by_source_file(limit=2)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (2,)


def test_summarize_by_collect_time_returns_grouped_rows() -> None:
    rows = [{"collect_time": "2026-03-06 10:00:00", "row_count": 2}]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.summarize_by_collect_time(limit=4)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (4,)


def test_summarize_by_ani_returns_grouped_rows() -> None:
    rows = [{"ani": "CELL_30", "row_count": 2}]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.summarize_by_ani(limit=6)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (6,)


def test_summarize_by_dataset_family_returns_grouped_rows() -> None:
    rows = [{"dataset_family": "ITBBU_LTEFDD", "row_count": 42}]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.summarize_by_dataset_family(limit=12)

    assert result == rows
    assert "COALESCE(dataset_family, 'UNKNOWN')" in repository.connection.cursor_obj.executed[0][0]
    assert repository.connection.cursor_obj.executed[0][1] == (12,)


def test_summarize_counter_aggregates_returns_grouped_rows() -> None:
    rows = [{"collect_time": "2026-03-06 10:00:00", "counter_id": "C1", "sum_counter_value": 13.0}]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.summarize_counter_aggregates(limit=8)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (8,)


def test_summarize_ani_counter_aggregates_returns_grouped_rows() -> None:
    rows = [{"collect_time": "2026-03-06 10:00:00", "ani": "CELL_30", "counter_id": "C1"}]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.summarize_ani_counter_aggregates(limit=9)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (9,)


def test_summarize_entity_fields_returns_grouped_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "distinct_cellid": 100}]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.summarize_entity_fields(limit=5)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (5,)


def test_count_distinct_cells_returns_grouped_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "distinct_cell_keys": 100}]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.count_distinct_cells(limit=7)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (7,)


def test_summarize_entity_counters_returns_grouped_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "counter_id": "C1", "row_count": 10}]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.summarize_entity_counters(limit=13)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (13,)


def test_summarize_entity_intervals_returns_grouped_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "logical_entity_key": "entity-1"}]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.summarize_entity_intervals(limit=14)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (14,)


def test_summarize_coverage_returns_grouped_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "distinct_cell_entities": 5456}]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.summarize_coverage(limit=15)

    assert result == rows
    assert "vw_pm_family_interval_coverage" in repository.connection.cursor_obj.executed[0][0]
    assert repository.connection.cursor_obj.executed[0][1] == (15,)


def test_summarize_logical_entity_counts_returns_grouped_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "distinct_logical_entity_keys": 5456}]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.summarize_logical_entity_counts(limit=16)

    assert result == rows
    assert "vw_pm_logical_entity_counts_by_time" in repository.connection.cursor_obj.executed[0][0]
    assert repository.connection.cursor_obj.executed[0][1] == (16,)


def test_compare_expected_cells_returns_grouped_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "observed_cells": 5456, "expected_cells": 10251}]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.compare_expected_cells(expected=10251)

    assert result == rows
    assert "vw_pm_cell_entity_counts_by_interval" in repository.connection.cursor_obj.executed[0][0]
    assert repository.connection.cursor_obj.executed[0][1] == (10251, 10251, 10251, 10251, 10251)


def test_summarize_coverage_timeline_returns_grouped_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "distinct_cell_entities": 5456}]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.summarize_coverage_timeline(limit=17)

    assert result == rows
    assert "vw_pm_cell_entity_counts_timeline" in repository.connection.cursor_obj.executed[0][0]
    assert repository.connection.cursor_obj.executed[0][1] == (17,)


def test_compare_expected_cells_timeline_returns_grouped_rows() -> None:
    rows = [
        {
            "collect_time": "2026-03-05 00:00:00",
            "observed_cells": 10222,
            "expected_cells": 10251,
        }
    ]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.compare_expected_cells_timeline(expected=10251, limit=18)

    assert result == rows
    assert (
        "vw_pm_combined_ltefdd_cell_coverage_timeline"
        in repository.connection.cursor_obj.executed[0][0]
    )
    assert repository.connection.cursor_obj.executed[0][1] == (10251, 10251, 10251, 18)


def test_count_rows_by_source_files_returns_mapping() -> None:
    rows = [
        {"source_file": "a.tar.gz", "row_count": 10},
        {"source_file": "b.tar.gz", "row_count": 20},
    ]
    repository = PmSampleRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.count_rows_by_source_files(["a.tar.gz", "b.tar.gz"])

    assert result == {"a.tar.gz": 10, "b.tar.gz": 20}
    assert repository.connection.cursor_obj.executed[0][1] == (["a.tar.gz", "b.tar.gz"],)
