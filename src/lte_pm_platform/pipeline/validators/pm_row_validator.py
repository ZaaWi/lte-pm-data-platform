REQUIRED_COLUMNS = {"COLLECTTIME"}


def validate_required_columns(row: dict[str, str]) -> None:
    missing = sorted(column for column in REQUIRED_COLUMNS if column not in row)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")
