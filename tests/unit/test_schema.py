from pathlib import Path

import pytest

from lte_pm_platform.db import schema


class FakeCursor:
    def __init__(self, *, fail_on: str | None = None) -> None:
        self.fail_on = fail_on
        self.executed_sql: list[str] = []

    def execute(self, sql: str) -> None:
        self.executed_sql.append(sql)
        if self.fail_on is not None and self.fail_on in sql:
            raise RuntimeError("boom")

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


class FakeConnection:
    def __init__(self, *, fail_on: str | None = None) -> None:
        self.fail_on = fail_on
        self.cursor_calls = 0
        self.cursor_objects: list[FakeCursor] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, **kwargs):  # noqa: ANN003, ANN201
        self.cursor_calls += 1
        cursor = FakeCursor(fail_on=self.fail_on)
        self.cursor_objects.append(cursor)
        return cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_ordered_sql_files_uses_explicit_order(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    init_dir = tmp_path / "init"
    init_dir.mkdir()
    for filename in schema.SQL_INIT_ORDER:
        (init_dir / filename).write_text(f"-- {filename}\n")

    monkeypatch.setattr(schema, "sql_init_dir", lambda: init_dir)

    ordered = schema.ordered_sql_files()

    assert [path.name for path in ordered] == list(schema.SQL_INIT_ORDER)


def test_ordered_sql_files_rejects_unordered_extras(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    init_dir = tmp_path / "init"
    init_dir.mkdir()
    for filename in schema.SQL_INIT_ORDER:
        (init_dir / filename).write_text(f"-- {filename}\n")
    (init_dir / "999_extra.sql").write_text("-- extra\n")

    monkeypatch.setattr(schema, "sql_init_dir", lambda: init_dir)

    with pytest.raises(ValueError, match="Unordered SQL init files found"):
        schema.ordered_sql_files()


def test_initialize_schema_commits_each_file_in_order(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    init_dir = tmp_path / "init"
    init_dir.mkdir()
    for filename in schema.SQL_INIT_ORDER:
        (init_dir / filename).write_text(f"-- {filename}\n")

    monkeypatch.setattr(schema, "sql_init_dir", lambda: init_dir)
    connection = FakeConnection()

    schema.initialize_schema(connection)  # type: ignore[arg-type]

    assert connection.commits == len(schema.SQL_INIT_ORDER)
    assert connection.rollbacks == 0
    assert [
        cursor.executed_sql[0].strip().removeprefix("-- ")
        for cursor in connection.cursor_objects
    ] == list(schema.SQL_INIT_ORDER)


def test_initialize_schema_rolls_back_failed_file_and_names_it(
    tmp_path: Path,
    monkeypatch,  # noqa: ANN001
) -> None:
    init_dir = tmp_path / "init"
    init_dir.mkdir()
    for filename in schema.SQL_INIT_ORDER:
        sql = f"-- {filename}\n"
        if filename == "004_reference_tables.sql":
            sql += "FAIL_MARKER\n"
        (init_dir / filename).write_text(sql)

    monkeypatch.setattr(schema, "sql_init_dir", lambda: init_dir)
    connection = FakeConnection(fail_on="FAIL_MARKER")

    with pytest.raises(RuntimeError, match="004_reference_tables.sql"):
        schema.initialize_schema(connection)  # type: ignore[arg-type]

    assert connection.rollbacks == 1
    assert connection.commits == 4
