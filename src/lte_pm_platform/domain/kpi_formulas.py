from __future__ import annotations


def safe_ratio(
    numerator: float | None,
    denominator: float | None,
    scale_factor: float = 1.0,
) -> float | None:
    if numerator is None:
        return None
    if denominator in {None, 0}:
        return None
    return (numerator / denominator) * scale_factor
