from pathlib import Path

from openpyxl import Workbook

from lte_pm_platform.pipeline.ingest.topology_workbook import (
    extract_release_date_from_filename,
    parse_topology_workbook,
)


def test_extract_release_date_from_filename() -> None:
    result = extract_release_date_from_filename("topology_reference_workbook_20260301.xlsx")

    assert str(result) == "2026-03-01"


def test_parse_topology_workbook_generates_dual_family_rows(tmp_path: Path) -> None:
    workbook_path = tmp_path / "topology_reference_workbook_20260301.xlsx"
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "4G LTE"
    sheet.append(["SubnetID", "eNodeBid", "ENODEBName", "CellID", "CELLNAME", "SiteName", "Region", "Area", "ClusterID", "TEAM"])
    sheet.append(["45", "3517", "NODE001", "1", "NODE001-1", "SITE001", "REG1", "Area1", "Cluster01", "6"])
    workbook.save(workbook_path)

    parsed = parse_topology_workbook(workbook_path)

    assert parsed.topology_release_date is not None
    assert parsed.workbook_row_count == 1
    assert len(parsed.normalized_rows) == 2
    assert {row.dataset_family for row in parsed.normalized_rows} == {"PM/sdr/ltefdd", "PM/itbbu/ltefdd"}
    assert parsed.parser_errors == []
