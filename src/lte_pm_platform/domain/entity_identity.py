from __future__ import annotations

from typing import Literal

EntityLevel = Literal["cell", "meid", "raw"]

# Empirical working interpretation from validated raw files:
# - PM/sdr/ltefdd and PM/itbbu/ltefdd behave like cell-level datasets.
# - PM/itbbu/itbbuplat behaves more like node/BBU/platform-level data.
# This is a practical modeling rule for current normalization, not vendor-confirmed truth.
FAMILY_IDENTITY_FIELDS: dict[str, tuple[str, ...]] = {
    "PM/sdr/ltefdd": ("sbnid", "enodebid", "cellid"),
    "PM/itbbu/ltefdd": ("sbnid", "enbid", "cellid"),
    "PM/itbbu/itbbuplat": ("sbnid", "meid"),
}

FALLBACK_IDENTITY_FIELDS: tuple[str, ...] = ("sbnid", "enbid", "enodebid", "cellid", "meid", "ani")


def identity_fields_for_family(dataset_family: str | None) -> tuple[str, ...]:
    if dataset_family is None:
        return FALLBACK_IDENTITY_FIELDS
    return FAMILY_IDENTITY_FIELDS.get(dataset_family, FALLBACK_IDENTITY_FIELDS)


def entity_level_for_family(dataset_family: str | None) -> EntityLevel:
    if dataset_family in {"PM/sdr/ltefdd", "PM/itbbu/ltefdd"}:
        return "cell"
    if dataset_family == "PM/itbbu/itbbuplat":
        return "meid"
    return "raw"


def build_logical_entity_key(
    *,
    dataset_family: str | None,
    sbnid: str | None = None,
    enbid: str | None = None,
    enodebid: str | None = None,
    cellid: str | None = None,
    meid: str | None = None,
    ani: str | None = None,
) -> str:
    values = {
        "sbnid": sbnid,
        "enbid": enbid,
        "enodebid": enodebid,
        "cellid": cellid,
        "meid": meid,
        "ani": ani,
    }
    parts = [f"family={dataset_family or 'UNKNOWN'}"]
    for field_name in identity_fields_for_family(dataset_family):
        parts.append(f"{field_name}={_clean(values.get(field_name))}")
    return "|".join(parts)


def _clean(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip()
