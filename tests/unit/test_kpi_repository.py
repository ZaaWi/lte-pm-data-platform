from lte_pm_platform.db.repositories.kpi_repository import KpiRepository


class FakeCursor:
    def __init__(self, rows, one_row=None) -> None:  # noqa: ANN001
        self.rows = rows
        self.one_row = one_row
        self.executed: list[tuple[str, tuple]] = []

    def execute(self, query: str, params: tuple) -> None:
        self.executed.append((query, params))

    def fetchall(self):  # noqa: ANN201
        return self.rows

    def fetchone(self):  # noqa: ANN201
        return self.one_row

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


class FakeConnection:
    def __init__(self, cursor_results: list[tuple[list[dict], dict | None]]) -> None:
        self.cursor_results = cursor_results
        self.cursor_calls = 0
        self.cursors: list[FakeCursor] = []

    def cursor(self, **kwargs):  # noqa: ANN003, ANN201
        rows, one_row = self.cursor_results[self.cursor_calls]
        self.cursor_calls += 1
        cursor = FakeCursor(rows, one_row=one_row)
        self.cursors.append(cursor)
        return cursor


def test_list_definitions_returns_rows() -> None:
    rows = [
        {
            "kpi_name": "lte_prb_utilization",
            "status": "PENDING_COUNTER_MAPPING",
            "numerator_counter_ids": [],
            "denominator_counter_ids": [],
            "unverified_counter_ids": [],
            "all_mapped_counters_verified": False,
        }
    ]
    repository = KpiRepository(FakeConnection([(rows, None)]))  # type: ignore[arg-type]

    result = repository.list_definitions(limit=5)

    assert result == rows
    assert repository.connection.cursors[0].executed[0][1] == (5,)


def test_get_definition_returns_one_row() -> None:
    one_row = {
        "kpi_name": "lte_prb_utilization",
        "status": "PENDING_COUNTER_MAPPING",
        "numerator_counter_ids": [],
        "denominator_counter_ids": [],
        "unverified_counter_ids": [],
        "all_mapped_counters_verified": False,
    }
    repository = KpiRepository(FakeConnection([([], one_row)]))  # type: ignore[arg-type]

    result = repository.get_definition("lte_prb_utilization")

    assert result == one_row
    assert repository.connection.cursors[0].executed[0][1] == ("lte_prb_utilization",)


def test_summarize_kpi_returns_rows() -> None:
    rows = [{"kpi_name": "lte_prb_utilization", "kpi_value": None}]
    repository = KpiRepository(FakeConnection([(rows, None)]))  # type: ignore[arg-type]

    result = repository.summarize_kpi("lte_prb_utilization", limit=10)

    assert result == rows
    assert repository.connection.cursors[0].executed[0][1] == ("lte_prb_utilization", 10)


def test_pending_kpi_definition_can_show_empty_mapping_lists() -> None:
    one_row = {
        "kpi_name": "lte_dl_throughput",
        "status": "PENDING_COUNTER_MAPPING",
        "pending_reason": "Verified ZTE traffic-volume and time counters are not yet mapped.",
        "numerator_counter_ids": [],
        "denominator_counter_ids": [],
        "unverified_counter_ids": [],
        "all_mapped_counters_verified": False,
    }
    repository = KpiRepository(FakeConnection([([], one_row)]))  # type: ignore[arg-type]

    result = repository.get_definition("lte_dl_throughput")

    assert result is not None
    assert result["status"] == "PENDING_COUNTER_MAPPING"
    assert result["numerator_counter_ids"] == []
    assert result["denominator_counter_ids"] == []
    assert result["unverified_counter_ids"] == []
    assert result["all_mapped_counters_verified"] is False


def test_pending_kpi_returns_no_computed_rows() -> None:
    repository = KpiRepository(FakeConnection([([], None)]))  # type: ignore[arg-type]

    result = repository.summarize_kpi("lte_ul_throughput", limit=5)

    assert result == []
