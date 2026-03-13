from pathlib import Path

from lte_pm_platform.pipeline.ingest.counter_reference_seed import load_counter_reference_seed
from lte_pm_platform.pipeline.ingest.semantic_kpi_seed import (
    load_counter_dictionary_seed,
    load_kpi_definition_seed,
)


def test_verified_prb_semantic_counter_dictionary_seed_has_expected_aliases() -> None:
    csv_path = Path('data/reference/proposed_verified_prb_semantic_counter_dictionary.csv')

    rows = load_counter_dictionary_seed(csv_path)

    assert len(rows) == 8
    aliases = {(row.dataset_family, row.counter_alias) for row in rows}
    assert ('PM/itbbu/ltefdd', 'dl_prb_used') in aliases
    assert ('PM/itbbu/ltefdd', 'dl_prb_available') in aliases
    assert ('PM/sdr/ltefdd', 'ul_prb_used') in aliases
    assert ('PM/sdr/ltefdd', 'ul_prb_available') in aliases
    assert all(row.verification_status == 'VERIFIED' for row in rows)


def test_verified_prb_kpi_definition_seed_has_expected_inputs() -> None:
    csv_path = Path('data/reference/proposed_verified_prb_kpi_definitions.csv')

    rows = load_kpi_definition_seed(csv_path)

    assert len(rows) == 8
    kpi_codes = {row.kpi_code for row in rows}
    assert kpi_codes == {'dl_prb_utilization', 'ul_prb_utilization'}
    assert all(row.verification_status == 'VERIFIED' for row in rows)
    assert all(row.formula_expression == '100 * numerator / denominator' for row in rows)
    assert {row.counter_alias for row in rows if row.kpi_code == 'dl_prb_utilization'} == {
        'dl_prb_used',
        'dl_prb_available',
    }
    assert {row.counter_alias for row in rows if row.kpi_code == 'ul_prb_utilization'} == {
        'ul_prb_used',
        'ul_prb_available',
    }
