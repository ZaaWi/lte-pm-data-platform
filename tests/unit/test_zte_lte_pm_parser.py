from io import StringIO
from uuid import uuid4

from lte_pm_platform.domain.models import IngestSummary
from lte_pm_platform.pipeline.parsers.zte_lte_pm import ZteLtePmParser


def test_parser_normalizes_rows_into_counter_records() -> None:
    csv_text = (
        "collecttime,TRNCMEID,ANI,SBNID,eNBID,ENODEBID,CellID,MEID,SYSTEMMODE,"
        "MIDFLAG,NETYPE,C380340003,C380340004,IGNORED\n"
        "20260304143000,TR1,CELL_1,41,1010,,1,ME1,FDD,MID_A,ENODEB,12.5,,noop\n"
    )
    parser = ZteLtePmParser()
    summary = IngestSummary(
        source_file="UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260304_1430_R1.tar.gz",
        run_id=uuid4(),
        trigger_type="local_cli",
        source_type="local",
    )

    records = list(
        parser.parse_csv(
            text_stream=StringIO(csv_text),
            source_file="UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260304_1430_R1.tar.gz",
            csv_name="pm.csv",
            summary=summary,
        )
    )

    assert len(records) == 2
    assert records[0].source_file == "UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_20260304_1430_R1.tar.gz"
    assert records[0].dataset_family == "PM/itbbu/ltefdd"
    assert records[0].revision == 1
    assert records[0].csv_name == "pm.csv"
    assert records[0].counter_id == "C380340003"
    assert records[0].counter_value == 12.5
    assert records[0].ani == "CELL_1"
    assert records[0].sbnid == "41"
    assert records[0].enbid == "1010"
    assert records[0].enodebid is None
    assert records[0].cellid == "1"
    assert records[0].meid == "ME1"
    assert records[0].trncmeid == "TR1"
    assert records[1].counter_id == "C380340004"
    assert records[1].counter_value is None
    assert summary.input_rows_read == 1
    assert summary.normalized_rows_emitted == 2
    assert summary.null_counter_values == 1
    assert summary.unknown_columns == {"IGNORED"}
