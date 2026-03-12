from datetime import datetime

import pytest

from lte_pm_platform.domain.counter_rules import is_counter_column
from lte_pm_platform.utils.time import parse_zte_timestamp


def test_parse_zte_timestamp() -> None:
    parsed = parse_zte_timestamp("20260304143000")
    assert parsed == datetime(2026, 3, 4, 14, 30, 0)


def test_parse_zte_timestamp_rejects_invalid_value() -> None:
    with pytest.raises(ValueError):
        parse_zte_timestamp("2026-03-04")


def test_counter_column_detection() -> None:
    assert is_counter_column("C380340003")
    assert is_counter_column("c123")
    assert not is_counter_column("ANI")
    assert not is_counter_column("COUNTER_1")
