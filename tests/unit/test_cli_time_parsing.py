from datetime import datetime

import pytest
import typer

from lte_pm_platform.cli import parse_interval_option, parse_time_window


def test_parse_interval_option_accepts_compact_datetime() -> None:
    parsed, is_date_only = parse_interval_option("202603051330")

    assert parsed == datetime(2026, 3, 5, 13, 30)
    assert is_date_only is False


def test_parse_interval_option_accepts_spaced_datetime() -> None:
    parsed, is_date_only = parse_interval_option("2026-03-05 13:30")

    assert parsed == datetime(2026, 3, 5, 13, 30)
    assert is_date_only is False


def test_parse_interval_option_accepts_date_only() -> None:
    parsed, is_date_only = parse_interval_option("2026-03-05")

    assert parsed == datetime(2026, 3, 5, 0, 0)
    assert is_date_only is True


def test_parse_time_window_for_date_only_covers_full_day() -> None:
    start, end = parse_time_window("2026-03-05", "2026-03-05")

    assert start == datetime(2026, 3, 5, 0, 0)
    assert end == datetime(2026, 3, 6, 0, 0)


def test_parse_time_window_for_exact_datetime_uses_next_minute_upper_bound() -> None:
    start, end = parse_time_window("202603051330", "2026-03-05 13:30")

    assert start == datetime(2026, 3, 5, 13, 30)
    assert end == datetime(2026, 3, 5, 13, 31)


def test_parse_interval_option_rejects_unsupported_format() -> None:
    with pytest.raises(typer.BadParameter):
        parse_interval_option("03/05/2026")
