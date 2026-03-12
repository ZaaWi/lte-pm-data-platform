from __future__ import annotations

from datetime import datetime


def parse_zte_timestamp(value: str) -> datetime:
    return datetime.strptime(value.strip(), "%Y%m%d%H%M%S")
