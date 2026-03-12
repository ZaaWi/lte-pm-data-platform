import re

COUNTER_PATTERN = re.compile(r"^C\d+$")

KNOWN_DIMENSION_COLUMNS = {
    "COLLECTTIME",
    "TRNCMEID",
    "ANI",
    "SBNID",
    "ENBID",
    "ENODEBID",
    "CELLID",
    "MEID",
    "SYSTEMMODE",
    "MIDFLAG",
    "NETYPE",
    "DESTIPADDR",
    "DETECTPRO",
    "DETECTPORT",
}


def is_counter_column(column_name: str) -> bool:
    return bool(COUNTER_PATTERN.fullmatch(column_name.strip().upper()))
