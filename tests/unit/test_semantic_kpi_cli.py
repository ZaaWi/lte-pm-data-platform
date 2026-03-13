import json
from pathlib import Path

from typer.testing import CliRunner

from lte_pm_platform import cli

runner = CliRunner()


class FakeSemanticKpiRepository:
    def __init__(self, connection) -> None:  # noqa: ANN001
        self.connection = connection

    def list_unmapped_counters(self, limit: int = 100) -> list[dict]:
        return [{"dataset_family": "PM/sdr/ltefdd", "counter_id": "C1", "row_count": 10}]

    def list_provisional_kpis(self, limit: int = 100) -> list[dict]:
        return [{"kpi_code": "lte_prb_util", "verification_status": "PROVISIONAL"}]

    def summarize_kpi_input_coverage(self, limit: int = 100) -> list[dict]:
        return [{"kpi_code": "lte_prb_util", "input_alias": "numerator", "row_count": 42}]

    def list_verified_prb_kpi_outputs(self, limit: int = 100) -> list[dict]:
        return [{"kpi_code": "dl_prb_utilization", "kpi_value": 50.0}]

    def summarize_verified_prb_kpi_execution(self, limit: int = 100) -> list[dict]:
        return [{"kpi_code": "dl_prb_utilization", "executed_rows": 24}]

    def list_verified_bler_kpi_outputs(self, limit: int = 100) -> list[dict]:
        return [{"kpi_code": "dl_bler", "kpi_value": 4.2}]

    def summarize_verified_bler_kpi_execution(self, limit: int = 100) -> list[dict]:
        return [{"kpi_code": "dl_bler", "executed_rows": 24}]

    def list_vendor_indicators(self, limit: int = 100) -> list[dict]:
        return [{"indicator_code": "PA1", "semantic_alias": "dl_prb_used"}]

    def list_verified_rrc_kpi_entity_time(self, limit: int = 100) -> list[dict]:
        return [{"dataset_family": "PM/sdr/ltefdd", "rrc_connected_users_max": 5}]

    def validate_verified_rrc_kpi_entity_time(self) -> list[dict]:
        return [{"dataset_family": "PM/sdr/ltefdd", "entity_time_rows": 24}]

    def list_verified_prb_kpi_site_time(self, limit: int = 100) -> list[dict]:
        return [{"dataset_family": "PM/sdr/ltefdd", "site": "SITE1", "dl_prb_utilization": 50.0}]

    def validate_verified_prb_kpi_site_time(self) -> list[dict]:
        return [{"dataset_family": "PM/sdr/ltefdd", "site_time_rows": 8}]

    def list_verified_bler_kpi_site_time(self, limit: int = 100) -> list[dict]:
        return [{"dataset_family": "PM/sdr/ltefdd", "site": "SITE1", "dl_bler": 1.2}]

    def validate_verified_bler_kpi_site_time(self) -> list[dict]:
        return [{"dataset_family": "PM/sdr/ltefdd", "site_time_rows": 8}]

    def list_verified_rrc_kpi_site_time(self, limit: int = 100) -> list[dict]:
        return [{"dataset_family": "PM/sdr/ltefdd", "site": "SITE1", "rrc_connected_users_online": 12}]

    def validate_verified_rrc_kpi_site_time(self) -> list[dict]:
        return [{"dataset_family": "PM/sdr/ltefdd", "site_time_rows": 8}]

    def list_verified_prb_kpi_region_time(self, limit: int = 100) -> list[dict]:
        return [{"dataset_family": "PM/sdr/ltefdd", "region": "REG1", "dl_prb_utilization": 51.0}]

    def validate_verified_prb_kpi_region_time(self) -> list[dict]:
        return [{"dataset_family": "PM/sdr/ltefdd", "region_time_rows": 4}]

    def list_verified_bler_kpi_region_time(self, limit: int = 100) -> list[dict]:
        return [{"dataset_family": "PM/sdr/ltefdd", "region": "REG1", "dl_bler": 1.3}]

    def validate_verified_bler_kpi_region_time(self) -> list[dict]:
        return [{"dataset_family": "PM/sdr/ltefdd", "region_time_rows": 4}]

    def list_verified_rrc_kpi_region_time(self, limit: int = 100) -> list[dict]:
        return [{"dataset_family": "PM/sdr/ltefdd", "region": "REG1", "rrc_connected_users_online": 33}]

    def validate_verified_rrc_kpi_region_time(self) -> list[dict]:
        return [{"dataset_family": "PM/sdr/ltefdd", "region_time_rows": 4}]


class FakeConnection:
    def commit(self) -> None:
        return None


class FakeConnectionContext:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def __enter__(self) -> FakeConnection:
        return self.connection

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


def test_load_counter_dictionary_cli(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    csv_path = tmp_path / "counter_dictionary.csv"
    csv_path.write_text(
        "dataset_family,counter_id,counter_alias,counter_name,aggregation_behavior\n"
        "PM/sdr/ltefdd,C1,dl_prb_used,DL PRB Used,sum\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)
    monkeypatch.setattr(
        cli,
        "load_counter_dictionary",
        lambda *, repository, csv_path: {"dictionary_rows_loaded": 1, "csv": str(csv_path)},
    )

    result = runner.invoke(cli.app, ["load-counter-dictionary", "--csv", str(csv_path)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["dictionary_rows_loaded"] == 1


def test_load_kpi_definitions_cli(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    csv_path = tmp_path / "kpi_definitions.csv"
    csv_path.write_text(
        "kpi_code,kpi_name,formula_expression,grain\n"
        "lte_prb_util,LTE PRB Util,100 * numerator / denominator,entity_time\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)
    monkeypatch.setattr(
        cli,
        "load_kpi_definitions",
        lambda *, repository, csv_path: {"kpi_rows_loaded": 1, "csv": str(csv_path)},
    )

    result = runner.invoke(cli.app, ["load-kpi-definitions", "--csv", str(csv_path)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["kpi_rows_loaded"] == 1


def test_load_vendor_indicator_seed_cli(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    csv_path = tmp_path / "vendor_indicator_seed.csv"
    csv_path.write_text(
        "vendor_indicator_code,vendor_indicator_name,proposed_counter_alias,aggregation_method,unit,counter_lineage_expression\n"
        "PA1,DL PRB Used,dl_prb_used,SUM,Number,C373424610\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)
    monkeypatch.setattr(
        cli,
        "load_vendor_indicator_seed_file",
        lambda *, repository, csv_path: {"vendor_indicator_rows_loaded": 1, "csv": str(csv_path)},
    )

    result = runner.invoke(cli.app, ["load-vendor-indicator-seed", "--csv", str(csv_path)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["vendor_indicator_rows_loaded"] == 1


def test_list_unmapped_counters_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["list-unmapped-counters", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 1
    assert payload["rows"][0]["counter_id"] == "C1"


def test_list_provisional_kpis_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["list-provisional-kpis", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["kpi_code"] == "lte_prb_util"


def test_summarize_kpi_input_coverage_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["summarize-kpi-input-coverage", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["input_alias"] == "numerator"


def test_list_verified_prb_kpi_outputs_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["list-verified-prb-kpi-outputs", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 1
    assert payload["rows"][0]["kpi_code"] == "dl_prb_utilization"


def test_summarize_verified_prb_kpi_execution_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["summarize-verified-prb-kpi-execution", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 1
    assert payload["rows"][0]["executed_rows"] == 24


def test_list_verified_bler_kpi_outputs_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["list-verified-bler-kpi-outputs", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 1
    assert payload["rows"][0]["kpi_code"] == "dl_bler"


def test_summarize_verified_bler_kpi_execution_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["summarize-verified-bler-kpi-execution", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 1
    assert payload["rows"][0]["executed_rows"] == 24


def test_list_vendor_indicators_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["list-vendor-indicators", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 1
    assert payload["rows"][0]["indicator_code"] == "PA1"


def test_list_verified_rrc_kpi_entity_time_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["list-verified-rrc-kpi-entity-time", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 1
    assert payload["rows"][0]["rrc_connected_users_max"] == 5


def test_validate_verified_rrc_kpi_entity_time_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["validate-verified-rrc-kpi-entity-time"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["entity_time_rows"] == 24


def test_list_verified_prb_kpi_site_time_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["list-verified-prb-kpi-site-time", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["site"] == "SITE1"


def test_validate_verified_prb_kpi_site_time_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["validate-verified-prb-kpi-site-time"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["site_time_rows"] == 8


def test_list_verified_bler_kpi_site_time_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["list-verified-bler-kpi-site-time", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["site"] == "SITE1"


def test_validate_verified_bler_kpi_site_time_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["validate-verified-bler-kpi-site-time"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["site_time_rows"] == 8


def test_list_verified_rrc_kpi_site_time_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["list-verified-rrc-kpi-site-time", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["site"] == "SITE1"


def test_validate_verified_rrc_kpi_site_time_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["validate-verified-rrc-kpi-site-time"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["site_time_rows"] == 8


def test_list_verified_prb_kpi_region_time_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["list-verified-prb-kpi-region-time", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["region"] == "REG1"


def test_validate_verified_prb_kpi_region_time_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["validate-verified-prb-kpi-region-time"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["region_time_rows"] == 4


def test_list_verified_bler_kpi_region_time_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["list-verified-bler-kpi-region-time", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["region"] == "REG1"


def test_validate_verified_bler_kpi_region_time_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["validate-verified-bler-kpi-region-time"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["region_time_rows"] == 4


def test_list_verified_rrc_kpi_region_time_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["list-verified-rrc-kpi-region-time", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["region"] == "REG1"


def test_validate_verified_rrc_kpi_region_time_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "SemanticKpiRepository", FakeSemanticKpiRepository)

    result = runner.invoke(cli.app, ["validate-verified-rrc-kpi-region-time"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["region_time_rows"] == 4
