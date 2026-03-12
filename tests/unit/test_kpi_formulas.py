from lte_pm_platform.domain.kpi_formulas import safe_ratio


def test_safe_ratio_returns_none_for_null_numerator() -> None:
    assert safe_ratio(None, 10.0, 100.0) is None


def test_safe_ratio_returns_none_for_null_denominator() -> None:
    assert safe_ratio(5.0, None, 100.0) is None


def test_safe_ratio_returns_none_for_zero_denominator() -> None:
    assert safe_ratio(5.0, 0.0, 100.0) is None


def test_safe_ratio_applies_scale_factor() -> None:
    assert safe_ratio(5.0, 10.0, 100.0) == 50.0
