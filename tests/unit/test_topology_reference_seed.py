from pathlib import Path

import pytest

from lte_pm_platform.pipeline.ingest.topology_reference_seed import (
    load_topology_entity_site_map_seed,
    load_topology_region_seed,
    load_topology_reporting_seed,
    load_topology_site_seed,
)


def test_load_topology_region_seed_parses_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "regions.csv"
    csv_path.write_text(
        "region_code,region_name,notes\nR1,North,primary\n",
        encoding="utf-8",
    )

    rows = load_topology_region_seed(csv_path)

    assert len(rows) == 1
    assert rows[0].region_code == "R1"
    assert rows[0].region_name == "North"


def test_load_topology_site_seed_parses_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "sites.csv"
    csv_path.write_text(
        "site_code,site_name,region_code,notes\nS1,Site 1,R1,metro\n",
        encoding="utf-8",
    )

    rows = load_topology_site_seed(csv_path)

    assert len(rows) == 1
    assert rows[0].site_code == "S1"
    assert rows[0].region_code == "R1"


def test_load_topology_reporting_seed_parses_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "reporting.csv"
    csv_path.write_text(
        "reporting_key,reporting_name,reporting_level,parent_reporting_key,notes\nH1,Cluster 1,cluster,,ops\n",
        encoding="utf-8",
    )

    rows = load_topology_reporting_seed(csv_path)

    assert len(rows) == 1
    assert rows[0].reporting_key == "H1"
    assert rows[0].reporting_level == "cluster"


def test_load_topology_entity_site_map_seed_parses_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "entity_map.csv"
    csv_path.write_text(
        "logical_entity_key,site_code,reporting_key,mapping_source,notes\nfamily=PM/sdr/ltefdd|x,S1,H1,curated,ok\n",
        encoding="utf-8",
    )

    rows = load_topology_entity_site_map_seed(csv_path)

    assert len(rows) == 1
    assert rows[0].site_code == "S1"
    assert rows[0].reporting_key == "H1"


def test_topology_seed_loader_rejects_missing_required_fields(tmp_path: Path) -> None:
    csv_path = tmp_path / "regions.csv"
    csv_path.write_text(
        "region_code,region_name\n,North\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Missing required field: region_code"):
        load_topology_region_seed(csv_path)
