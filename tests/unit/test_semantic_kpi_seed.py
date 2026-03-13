from pathlib import Path

import pytest

from lte_pm_platform.pipeline.ingest.semantic_kpi_seed import (
    load_counter_dictionary_seed,
    load_kpi_definition_seed,
    load_vendor_indicator_seed,
)


def test_load_counter_dictionary_seed_parses_group_metadata(tmp_path: Path) -> None:
    csv_path = tmp_path / "counter_dictionary.csv"
    csv_path.write_text(
        "dataset_family,counter_id,counter_alias,counter_name,unit,aggregation_behavior,verification_status,source_note,group_code,group_name,group_notes\n"
        "PM/sdr/ltefdd,C1,dl_prb_used,DL PRB Used,percent,sum,VERIFIED,vendor doc,PRB,PRB Group,radio\n",
        encoding="utf-8",
    )

    rows = load_counter_dictionary_seed(csv_path)

    assert len(rows) == 1
    assert rows[0].counter_alias == "dl_prb_used"
    assert rows[0].group_code == "PRB"


def test_load_kpi_definition_seed_parses_input_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "kpi_definitions.csv"
    csv_path.write_text(
        "kpi_code,kpi_name,formula_expression,grain,unit,verification_status,topology_rollup_allowed,notes,input_alias,dataset_family,counter_alias,required,input_notes\n"
        "lte_prb_util,LTE PRB Util,100 * numerator / denominator,entity_time,percent,PROVISIONAL,true,test,numerator,PM/sdr/ltefdd,dl_prb_used,true,sum of used PRBs\n",
        encoding="utf-8",
    )

    rows = load_kpi_definition_seed(csv_path)

    assert len(rows) == 1
    assert rows[0].topology_rollup_allowed is True
    assert rows[0].input_alias == "numerator"
    assert rows[0].required is True


def test_load_kpi_definition_seed_rejects_partial_input_mapping(tmp_path: Path) -> None:
    csv_path = tmp_path / "kpi_definitions.csv"
    csv_path.write_text(
        "kpi_code,kpi_name,formula_expression,grain,verification_status,input_alias,dataset_family\n"
        "lte_prb_util,LTE PRB Util,100 * numerator / denominator,entity_time,PROVISIONAL,numerator,PM/sdr/ltefdd\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="counter_alias is required when input_alias is provided"):
        load_kpi_definition_seed(csv_path)


def test_load_counter_dictionary_seed_rejects_invalid_verification_status(tmp_path: Path) -> None:
    csv_path = tmp_path / "counter_dictionary.csv"
    csv_path.write_text(
        "dataset_family,counter_id,counter_alias,counter_name,aggregation_behavior,verification_status\n"
        "PM/sdr/ltefdd,C1,dl_prb_used,DL PRB Used,sum,BAD\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid verification_status"):
        load_counter_dictionary_seed(csv_path)


def test_load_vendor_indicator_seed_parses_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "vendor_indicator_seed.csv"
    csv_path.write_text(
        "vendor_indicator_code,vendor_indicator_name,proposed_counter_alias,aggregation_method,unit,counter_lineage_expression\n"
        "PA1,DL PRB Used,dl_prb_used,SUM,Number,C373424610\n",
        encoding="utf-8",
    )

    rows = load_vendor_indicator_seed(csv_path)

    assert len(rows) == 1
    assert rows[0].indicator_code == "PA1"
    assert rows[0].semantic_alias == "dl_prb_used"
    assert rows[0].lineage_type == "direct"
    assert rows[0].raw_counter_dependencies == "C373424610"


def test_load_vendor_indicator_seed_infers_composed_lineage(tmp_path: Path) -> None:
    csv_path = tmp_path / "vendor_indicator_seed.csv"
    csv_path.write_text(
        "vendor_indicator_code,vendor_indicator_name,proposed_counter_alias,aggregation_method,unit,counter_lineage_expression\n"
        "PA2,DL Volume,dl_volume_bits,SUM,bit,C374107514|C374107515\n",
        encoding="utf-8",
    )

    rows = load_vendor_indicator_seed(csv_path)

    assert rows[0].lineage_type == "composed"
    assert rows[0].raw_counter_dependencies == "C374107514|C374107515"
