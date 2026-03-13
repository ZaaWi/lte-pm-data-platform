import json
from pathlib import Path

from typer.testing import CliRunner

from lte_pm_platform import cli

runner = CliRunner()


class FakeTopologyRepository:
    def __init__(self, connection) -> None:  # noqa: ANN001
        self.connection = connection

    def list_unmapped_entities(self, limit: int = 100) -> list[dict]:
        return [{"logical_entity_key": "x", "dataset_family": "PM/sdr/ltefdd", "entity_level": "cell"}]

    def summarize_site_coverage(self, limit: int = 100) -> list[dict]:
        return [{"site_code": "S1", "site_name": "Site 1", "row_count": 100}]

    def summarize_region_coverage(self, limit: int = 100) -> list[dict]:
        return [{"region_code": "R1", "region_name": "North", "row_count": 200}]


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


def test_list_unmapped_entities_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "TopologyReferenceRepository", FakeTopologyRepository)

    result = runner.invoke(cli.app, ["list-unmapped-entities", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 1
    assert payload["rows"][0]["logical_entity_key"] == "x"


def test_summarize_site_coverage_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "TopologyReferenceRepository", FakeTopologyRepository)

    result = runner.invoke(cli.app, ["summarize-site-coverage", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["site_code"] == "S1"


def test_summarize_region_coverage_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "TopologyReferenceRepository", FakeTopologyRepository)

    result = runner.invoke(cli.app, ["summarize-region-coverage", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["region_code"] == "R1"


def test_load_topology_regions_cli(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    csv_path = tmp_path / "regions.csv"
    csv_path.write_text("region_code,region_name\nR1,North\n", encoding="utf-8")
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "TopologyReferenceRepository", FakeTopologyRepository)
    monkeypatch.setattr(
        cli,
        "load_topology_regions",
        lambda *, repository, csv_path: {"rows_loaded": 1, "csv": str(csv_path)},
    )

    result = runner.invoke(cli.app, ["load-topology-regions", "--csv", str(csv_path)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows_loaded"] == 1


def test_sync_topology_cli(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(FakeConnection()))
    monkeypatch.setattr(cli, "TopologyReferenceRepository", FakeTopologyRepository)
    monkeypatch.setattr(cli, "sync_topology_enrichment", lambda *, repository: {"rows_synced": 4})

    result = runner.invoke(cli.app, ["sync-topology"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rows_synced"] == 4
