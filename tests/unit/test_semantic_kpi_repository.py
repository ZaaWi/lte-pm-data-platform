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

    def fetchone(self):  # noqa: ANN201
        if not self.rows:
            return None
        first = self.rows[0]
        if isinstance(first, tuple):
            return first
        if isinstance(first, dict):
            return tuple(first.values())
        return first

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


class MultiCursorConnection:
    def __init__(self, cursor_rows) -> None:  # noqa: ANN001
        self.cursors = [FakeCursor(rows) for rows in cursor_rows]
        self.used_cursors: list[FakeCursor] = []
        self.commits = 0

    def cursor(self, **kwargs):  # noqa: ANN003, ANN201
        cursor = self.cursors.pop(0)
        self.used_cursors.append(cursor)
        return cursor

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


def test_list_verified_kpi_results_entity_time_requires_latest_window_fast_path() -> None:
    connection = MultiCursorConnection(
        [
            [("2026-03-05 00:30:00",)],
            [
                {"counter_id": "C373424609", "counter_alias": "dl_prb_available"},
                {"counter_id": "C373424610", "counter_alias": "dl_prb_used"},
                {"counter_id": "C373424608", "counter_alias": "ul_prb_used"},
                {"counter_id": "C373424611", "counter_alias": "ul_prb_available"},
            ],
            [{"dataset_family": "PM/sdr/ltefdd", "dl_prb_utilization": 50.0}],
        ]
    )
    repository = SemanticKpiRepository(connection)  # type: ignore[arg-type]

    result = repository.list_verified_kpi_results(
        family="prb",
        grain="entity-time",
        limit=50,
        offset=0,
        dataset_family="PM/sdr/ltefdd",
        collect_time_from=None,
        collect_time_to=None,
    )

    assert result == [{"dataset_family": "PM/sdr/ltefdd", "dl_prb_utilization": 50.0}]
    latest_query, latest_params = connection.used_cursors[0].executed[0]
    counter_query, counter_params = connection.used_cursors[1].executed[0]
    result_query, result_params = connection.used_cursors[2].executed[0]

    assert "SELECT MAX(collect_time)" in latest_query
    assert latest_params == ("PM/sdr/ltefdd",)
    assert "FROM ref_semantic_counter_dictionary" in counter_query
    assert counter_params[0] == "PM/sdr/ltefdd"
    assert "FROM vw_pm_raw_with_entity_topology AS r" in result_query
    assert result_params[2] == "PM/sdr/ltefdd"
    assert result_params[-2:] == (50, 0)


def test_list_verified_kpi_results_site_time_applies_offset() -> None:
    connection = MultiCursorConnection(
        [
            [("2026-03-05 00:30:00",)],
            [
                {"counter_id": "C373424609", "counter_alias": "dl_prb_available"},
                {"counter_id": "C373424610", "counter_alias": "dl_prb_used"},
                {"counter_id": "C373424608", "counter_alias": "ul_prb_used"},
                {"counter_id": "C373424611", "counter_alias": "ul_prb_available"},
            ],
            [{"dataset_family": "PM/sdr/ltefdd", "site_code": "S1"}],
        ]
    )
    repository = SemanticKpiRepository(connection)  # type: ignore[arg-type]

    result = repository.list_verified_kpi_results(
        family="prb",
        grain="site-time",
        limit=25,
        offset=50,
        dataset_family="PM/sdr/ltefdd",
    )

    assert result == [{"dataset_family": "PM/sdr/ltefdd", "site_code": "S1"}]
    latest_query, latest_params = connection.used_cursors[0].executed[0]
    counter_query, counter_params = connection.used_cursors[1].executed[0]
    result_query, result_params = connection.used_cursors[2].executed[0]
    assert "SELECT MAX(collect_time)" in latest_query
    assert latest_params == ("PM/sdr/ltefdd",)
    assert "FROM ref_semantic_counter_dictionary" in counter_query
    assert counter_params[0] == "PM/sdr/ltefdd"
    assert "FROM pm_ltefdd_sample AS s" in result_query
    assert "JOIN ref_lte_entity_topology_enrichment AS t" in result_query
    assert "GROUP BY dataset_family, site_code, site_name, collect_time" in result_query
    assert result_params[2] == "PM/sdr/ltefdd"
    assert result_params[-2:] == (25, 50)


def test_list_verified_kpi_results_region_time_uses_fast_path() -> None:
    connection = MultiCursorConnection(
        [
            [("2026-03-05 00:30:00",)],
            [
                {"counter_id": "C373454800", "counter_alias": "dl_tb_error_blocks"},
                {"counter_id": "C373454801", "counter_alias": "dl_tb_total_blocks"},
                {"counter_id": "C373454802", "counter_alias": "ul_tb_error_blocks"},
                {"counter_id": "C373454803", "counter_alias": "ul_tb_total_blocks"},
            ],
            [{"dataset_family": "PM/sdr/ltefdd", "region_code": "RU"}],
        ]
    )
    repository = SemanticKpiRepository(connection)  # type: ignore[arg-type]

    result = repository.list_verified_kpi_results(
        family="bler",
        grain="region-time",
        limit=10,
        offset=0,
        dataset_family="PM/sdr/ltefdd",
        region_code="RU",
    )

    assert result == [{"dataset_family": "PM/sdr/ltefdd", "region_code": "RU"}]
    result_query, result_params = connection.used_cursors[2].executed[0]
    assert "FROM pm_ltefdd_sample AS s" in result_query
    assert "AND region_code = %s" in result_query
    assert "GROUP BY dataset_family, region_code, region_name, collect_time" in result_query
    assert result_params[-3:] == ("RU", 10, 0)


def test_list_verified_kpi_validation_entity_time_uses_fast_path_for_prb() -> None:
    connection = MultiCursorConnection(
        [
            [
                {
                    "kpi_code": "dl_prb_utilization",
                    "dataset_family": "PM/sdr/ltefdd",
                    "input_alias": "numerator",
                    "counter_id": "C373424610",
                    "counter_verification_status": "VERIFIED",
                },
                {
                    "kpi_code": "dl_prb_utilization",
                    "dataset_family": "PM/sdr/ltefdd",
                    "input_alias": "denominator",
                    "counter_id": "C373424611",
                    "counter_verification_status": "VERIFIED",
                },
                {
                    "kpi_code": "ul_prb_utilization",
                    "dataset_family": "PM/sdr/ltefdd",
                    "input_alias": "numerator",
                    "counter_id": "C373424608",
                    "counter_verification_status": "VERIFIED",
                },
                {
                    "kpi_code": "ul_prb_utilization",
                    "dataset_family": "PM/sdr/ltefdd",
                    "input_alias": "denominator",
                    "counter_id": "C373424609",
                    "counter_verification_status": "VERIFIED",
                },
            ],
            [
                {
                    "kpi_code": "dl_prb_utilization",
                    "input_alias": "numerator",
                    "distinct_collect_times": 2,
                    "distinct_entities": 5458,
                },
                {
                    "kpi_code": "dl_prb_utilization",
                    "input_alias": "denominator",
                    "distinct_collect_times": 2,
                    "distinct_entities": 5458,
                },
            ],
            [
                {
                    "kpi_code": "dl_prb_utilization",
                    "executed_rows": 5458,
                    "executed_collect_times": 2,
                    "executed_entities": 5458,
                }
            ],
        ]
    )
    repository = SemanticKpiRepository(connection)  # type: ignore[arg-type]

    result = repository.list_verified_kpi_validation(family="prb", grain="entity-time")

    assert result[0]["kpi_code"] == "dl_prb_utilization"
    spec_query, spec_params = connection.used_cursors[0].executed[0]
    coverage_query, coverage_params = connection.used_cursors[1].executed[0]
    result_query, result_params = connection.used_cursors[2].executed[0]

    assert "FROM ref_semantic_kpi_definition AS d" in spec_query
    assert spec_params == (["dl_prb_utilization", "ul_prb_utilization"],)
    assert "FROM pm_ltefdd_sample AS s" in coverage_query
    assert "COUNT(DISTINCT s.collect_time)" in coverage_query
    assert coverage_params[3] == "PM/sdr/ltefdd"
    assert coverage_params[4] == ["C373424608", "C373424609", "C373424610", "C373424611"]
    assert "FROM pm_ltefdd_sample AS s" in result_query
    assert "FROM vw_verified_prb_kpi_execution_validation" not in result_query
    assert result_params[4] == "PM/sdr/ltefdd"
    assert result_params[5] == ["C373424608", "C373424609", "C373424610", "C373424611"]


def test_list_verified_kpi_validation_entity_time_uses_fast_path_for_bler() -> None:
    connection = MultiCursorConnection(
        [
            [
                {
                    "kpi_code": "dl_bler",
                    "dataset_family": "PM/sdr/ltefdd",
                    "input_alias": "numerator",
                    "counter_id": "C373454800",
                    "counter_verification_status": "VERIFIED",
                },
                {
                    "kpi_code": "dl_bler",
                    "dataset_family": "PM/sdr/ltefdd",
                    "input_alias": "denominator",
                    "counter_id": "C373454801",
                    "counter_verification_status": "VERIFIED",
                },
                {
                    "kpi_code": "ul_bler",
                    "dataset_family": "PM/sdr/ltefdd",
                    "input_alias": "numerator",
                    "counter_id": "C373454802",
                    "counter_verification_status": "VERIFIED",
                },
                {
                    "kpi_code": "ul_bler",
                    "dataset_family": "PM/sdr/ltefdd",
                    "input_alias": "denominator",
                    "counter_id": "C373454803",
                    "counter_verification_status": "VERIFIED",
                },
            ],
            [
                {
                    "kpi_code": "dl_bler",
                    "input_alias": "numerator",
                    "distinct_collect_times": 2,
                    "distinct_entities": 5458,
                },
                {
                    "kpi_code": "dl_bler",
                    "input_alias": "denominator",
                    "distinct_collect_times": 2,
                    "distinct_entities": 5458,
                },
            ],
            [
                {
                    "kpi_code": "dl_bler",
                    "executed_rows": 5458,
                    "executed_collect_times": 2,
                    "executed_entities": 5458,
                }
            ],
        ]
    )
    repository = SemanticKpiRepository(connection)  # type: ignore[arg-type]

    result = repository.list_verified_kpi_validation(family="bler", grain="entity-time")

    assert result[0]["kpi_code"] == "dl_bler"
    spec_query, spec_params = connection.used_cursors[0].executed[0]
    coverage_query, coverage_params = connection.used_cursors[1].executed[0]
    result_query, result_params = connection.used_cursors[2].executed[0]

    assert "FROM ref_semantic_kpi_definition AS d" in spec_query
    assert spec_params == (["dl_bler", "ul_bler"],)
    assert "FROM pm_ltefdd_sample AS s" in coverage_query
    assert "COUNT(DISTINCT s.collect_time)" in coverage_query
    assert coverage_params[3] == "PM/sdr/ltefdd"
    assert coverage_params[4] == ["C373454800", "C373454801", "C373454802", "C373454803"]
    assert "FROM pm_ltefdd_sample AS s" in result_query
    assert "FROM vw_verified_bler_kpi_execution_validation" not in result_query
    assert result_params[4] == "PM/sdr/ltefdd"
    assert result_params[5] == ["C373454800", "C373454801", "C373454802", "C373454803"]



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
