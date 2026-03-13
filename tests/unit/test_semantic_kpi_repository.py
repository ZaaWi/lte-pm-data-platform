from lte_pm_platform.db.repositories.semantic_kpi_repository import SemanticKpiRepository
from lte_pm_platform.pipeline.ingest.semantic_kpi_seed import (
    SemanticCounterDictionarySeedRow,
    SemanticKpiDefinitionSeedRow,
    VendorIndicatorSeedRow,
)


class FakeCursor:
    def __init__(self, rows) -> None:  # noqa: ANN001
        self.rows = rows
        self.executed: list[tuple[str, tuple]] = []
        self.executemany_calls: list[tuple[str, list[tuple]]] = []

    def execute(self, query: str, params: tuple = ()) -> None:
        self.executed.append((query, params))

    def executemany(self, query: str, payload: list[tuple]) -> None:
        self.executemany_calls.append((query, payload))

    def fetchall(self):  # noqa: ANN201
        return self.rows

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


class FakeConnection:
    def __init__(self, rows) -> None:  # noqa: ANN001
        self.cursor_obj = FakeCursor(rows)
        self.commits = 0

    def cursor(self, **kwargs):  # noqa: ANN003, ANN201
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1


def test_upsert_counter_dictionary_updates_groups_and_members() -> None:
    connection = FakeConnection([])
    repository = SemanticKpiRepository(connection)  # type: ignore[arg-type]

    payload = repository.upsert_counter_dictionary(
        [
            SemanticCounterDictionarySeedRow(
                dataset_family="PM/sdr/ltefdd",
                counter_id="C1",
                counter_alias="dl_prb_used",
                counter_name="DL PRB Used",
                unit="percent",
                aggregation_behavior="sum",
                verification_status="VERIFIED",
                source_note="vendor",
                group_code="PRB",
                group_name="PRB Group",
                group_notes="radio",
            )
        ]
    )

    assert payload == {
        "dictionary_rows_loaded": 1,
        "group_rows_loaded": 1,
        "group_member_rows_loaded": 1,
    }
    assert connection.commits == 1
    assert "INSERT INTO ref_semantic_counter_group" in connection.cursor_obj.executemany_calls[0][0]
    assert "DELETE FROM ref_semantic_counter_group_member" in connection.cursor_obj.executed[0][0]
    assert "INSERT INTO ref_semantic_counter_group_member" in connection.cursor_obj.executemany_calls[2][0]


def test_upsert_kpi_definitions_replaces_formula_inputs() -> None:
    connection = FakeConnection([])
    repository = SemanticKpiRepository(connection)  # type: ignore[arg-type]

    payload = repository.upsert_kpi_definitions(
        [
            SemanticKpiDefinitionSeedRow(
                kpi_code="lte_prb_util",
                kpi_name="LTE PRB Util",
                formula_expression="100 * numerator / denominator",
                grain="entity_time",
                unit="percent",
                verification_status="PROVISIONAL",
                topology_rollup_allowed=True,
                notes="test",
                input_alias="numerator",
                dataset_family="PM/sdr/ltefdd",
                counter_alias="dl_prb_used",
                required=True,
                input_notes="sum",
            )
        ]
    )

    assert payload == {"kpi_rows_loaded": 1, "input_rows_loaded": 1}
    assert connection.commits == 1
    assert "INSERT INTO ref_semantic_kpi_definition" in connection.cursor_obj.executemany_calls[0][0]
    assert "DELETE FROM ref_semantic_kpi_formula_input" in connection.cursor_obj.executed[0][0]
    assert "INSERT INTO ref_semantic_kpi_formula_input" in connection.cursor_obj.executemany_calls[1][0]


def test_list_unmapped_counters_returns_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "counter_id": "C1", "row_count": 10}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.list_unmapped_counters(limit=5)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (5,)


def test_list_provisional_kpis_returns_rows() -> None:
    rows = [{"kpi_code": "lte_prb_util", "verification_status": "PROVISIONAL"}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.list_provisional_kpis(limit=7)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (7,)


def test_summarize_kpi_input_coverage_returns_rows() -> None:
    rows = [{"kpi_code": "lte_prb_util", "row_count": 24}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.summarize_kpi_input_coverage(limit=9)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (9,)


def test_list_verified_prb_kpi_outputs_returns_rows() -> None:
    rows = [{"kpi_code": "dl_prb_utilization", "kpi_value": 42.5}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.list_verified_prb_kpi_outputs(limit=11)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (11,)


def test_summarize_verified_prb_kpi_execution_returns_rows() -> None:
    rows = [{"kpi_code": "dl_prb_utilization", "executed_rows": 24}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.summarize_verified_prb_kpi_execution(limit=13)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (13,)


def test_list_verified_bler_kpi_outputs_returns_rows() -> None:
    rows = [{"kpi_code": "dl_bler", "kpi_value": 4.2}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.list_verified_bler_kpi_outputs(limit=15)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (15,)


def test_summarize_verified_bler_kpi_execution_returns_rows() -> None:
    rows = [{"kpi_code": "dl_bler", "executed_rows": 24}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.summarize_verified_bler_kpi_execution(limit=17)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (17,)


def test_list_verified_rrc_kpi_entity_time_returns_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "rrc_connected_users_max": 5}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.list_verified_rrc_kpi_entity_time(limit=19)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (19,)


def test_validate_verified_rrc_kpi_entity_time_returns_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "entity_time_rows": 24}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.validate_verified_rrc_kpi_entity_time()

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == ()


def test_list_verified_prb_kpi_site_time_returns_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "site": "SITE1", "dl_prb_utilization": 50.0}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.list_verified_prb_kpi_site_time(limit=21)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (21,)


def test_validate_verified_prb_kpi_site_time_returns_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "site_time_rows": 10}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.validate_verified_prb_kpi_site_time()

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == ()


def test_list_verified_bler_kpi_site_time_returns_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "site": "SITE1", "dl_bler": 1.2}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.list_verified_bler_kpi_site_time(limit=23)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (23,)


def test_validate_verified_bler_kpi_site_time_returns_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "site_time_rows": 10}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.validate_verified_bler_kpi_site_time()

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == ()


def test_list_verified_rrc_kpi_site_time_returns_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "site": "SITE1", "rrc_connected_users_online": 12}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.list_verified_rrc_kpi_site_time(limit=25)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (25,)


def test_validate_verified_rrc_kpi_site_time_returns_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "site_time_rows": 10}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.validate_verified_rrc_kpi_site_time()

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == ()


def test_list_verified_prb_kpi_region_time_returns_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "region": "REG1", "dl_prb_utilization": 50.0}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.list_verified_prb_kpi_region_time(limit=27)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (27,)


def test_validate_verified_prb_kpi_region_time_returns_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "region_time_rows": 10}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.validate_verified_prb_kpi_region_time()

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == ()


def test_list_verified_bler_kpi_region_time_returns_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "region": "REG1", "dl_bler": 1.2}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.list_verified_bler_kpi_region_time(limit=29)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (29,)


def test_validate_verified_bler_kpi_region_time_returns_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "region_time_rows": 10}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.validate_verified_bler_kpi_region_time()

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == ()


def test_list_verified_rrc_kpi_region_time_returns_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "region": "REG1", "rrc_connected_users_online": 12}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.list_verified_rrc_kpi_region_time(limit=31)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (31,)


def test_validate_verified_rrc_kpi_region_time_returns_rows() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "region_time_rows": 10}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.validate_verified_rrc_kpi_region_time()

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == ()


def test_upsert_vendor_indicators_updates_dictionary_and_lineage() -> None:
    connection = FakeConnection([])
    repository = SemanticKpiRepository(connection)  # type: ignore[arg-type]

    payload = repository.upsert_vendor_indicators(
        [
            VendorIndicatorSeedRow(
                indicator_code="PA1",
                indicator_name="DL PRB Used",
                semantic_alias="dl_prb_used",
                aggregation_method="SUM",
                unit="Number",
                verification_status="VERIFIED",
                source="starter.csv",
                lineage_expression="C373424610",
                lineage_type="direct",
                raw_counter_dependencies="C373424610",
            )
        ]
    )

    assert payload == {"vendor_indicator_rows_loaded": 1, "lineage_rows_loaded": 1}
    assert connection.commits == 1
    assert "INSERT INTO ref_vendor_indicator_dictionary" in connection.cursor_obj.executemany_calls[0][0]
    assert "INSERT INTO ref_vendor_indicator_lineage" in connection.cursor_obj.executemany_calls[1][0]


def test_list_vendor_indicators_returns_rows() -> None:
    rows = [{"indicator_code": "PA1", "semantic_alias": "dl_prb_used"}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.list_vendor_indicators(limit=4)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (4,)


def test_fetch_vendor_indicators_by_aliases_returns_rows() -> None:
    rows = [{"indicator_code": "PA1", "semantic_alias": "dl_prb_used"}]
    repository = SemanticKpiRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.fetch_vendor_indicators_by_aliases(["dl_prb_used", "ul_prb_used"])

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (["dl_prb_used", "ul_prb_used"],)
