from datetime import datetime

from lte_pm_platform.db.repositories.counter_reference_repository import CounterReferenceRepository
from lte_pm_platform.db.repositories.entity_reference_repository import EntityReferenceRepository
from lte_pm_platform.domain.entity_identity import (
    build_logical_entity_key,
    entity_level_for_family,
    identity_fields_for_family,
)
from lte_pm_platform.pipeline.ingest.counter_reference_seed import CounterReferenceSeedRow


class FakeCursor:
    def __init__(self, rows) -> None:  # noqa: ANN001
        self.rows = rows
        self.executed: list[tuple[str, tuple]] = []
        self.executemany_calls: list[tuple[str, list[tuple]]] = []
        self.rowcount = 3

    def execute(self, query: str, params: tuple = ()) -> None:
        self.executed.append((query, params))

    def fetchall(self):  # noqa: ANN201
        return self.rows

    def fetchone(self):  # noqa: ANN201
        return self.rows[0] if self.rows else None

    def executemany(self, query: str, payload: list[tuple]) -> None:
        self.executemany_calls.append((query, payload))

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


class FakeConnection:
    def __init__(self, rows) -> None:  # noqa: ANN001
        self.cursor_obj = FakeCursor(rows)
        self.commits = 0

    def cursor(self, **kwargs):  # noqa: ANN003, ANN201
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1


def test_counter_reference_repository_fetches_rows() -> None:
    rows = [
        {
            "counter_id": "C1",
            "vendor": "ZTE",
            "technology": "LTE",
            "verification_status": "VERIFIED",
        }
    ]
    repository = CounterReferenceRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.fetch_all(limit=5)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (5,)


def test_counter_reference_repository_fetches_one_row() -> None:
    rows = [{"counter_id": "C1", "verification_status": "VERIFIED"}]
    repository = CounterReferenceRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.fetch_by_id("C1")

    assert result == rows[0]
    assert repository.connection.cursor_obj.executed[0][1] == ("C1",)


def test_counter_reference_repository_upserts_rows() -> None:
    connection = FakeConnection([])
    repository = CounterReferenceRepository(connection)  # type: ignore[arg-type]
    rows = [
        CounterReferenceSeedRow(
            counter_id="C1",
            vendor="ZTE",
            technology="LTE",
            object_type=None,
            description="Test counter",
            unit=None,
            source_type="vendor_doc",
            source_reference="zte_pm.pdf#counter-c1",
            verification_status="VERIFIED",
            verified_at=datetime(2026, 3, 12, 10, 0, 0),
            notes="verified",
        )
    ]

    inserted = repository.upsert_many(rows)

    assert inserted == 1
    assert connection.commits == 1
    payload = connection.cursor_obj.executemany_calls[0][1]
    assert payload[0][0] == "C1"
    assert payload[0][8] == "zte_pm.pdf#counter-c1"
    assert payload[0][9] == "VERIFIED"


def test_entity_reference_repository_fetches_rows() -> None:
    rows = [{"logical_entity_key": "family=PM/sdr/ltefdd|sbnid=43|enodebid=4282|cellid=6"}]
    repository = EntityReferenceRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.fetch_all(limit=7)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (7,)


def test_entity_identity_rules_are_family_specific() -> None:
    assert identity_fields_for_family("PM/sdr/ltefdd") == ("sbnid", "enodebid", "cellid")
    assert identity_fields_for_family("PM/itbbu/ltefdd") == ("sbnid", "enbid", "cellid")
    assert identity_fields_for_family("PM/itbbu/itbbuplat") == ("sbnid", "meid")
    assert entity_level_for_family("PM/sdr/ltefdd") == "cell"
    assert entity_level_for_family("PM/itbbu/itbbuplat") == "meid"


def test_build_logical_entity_key_is_stable() -> None:
    key = build_logical_entity_key(
        dataset_family="PM/itbbu/ltefdd",
        sbnid="41",
        enbid="1010",
        cellid="3",
        meid="1010",
    )

    assert key == "family=PM/itbbu/ltefdd|sbnid=41|enbid=1010|cellid=3"


def test_entity_reference_repository_summarizes_entities() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "distinct_logical_entity_keys": 5456}]
    repository = EntityReferenceRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.summarize_entities(limit=9)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (9,)


def test_entity_reference_repository_shows_entity_shape() -> None:
    rows = [{"dataset_family": "PM/sdr/ltefdd", "logical_entity_key": "x"}]
    repository = EntityReferenceRepository(FakeConnection(rows))  # type: ignore[arg-type]

    result = repository.show_entity_shape(limit=11)

    assert result == rows
    assert repository.connection.cursor_obj.executed[0][1] == (11,)


def test_entity_reference_repository_refreshes_from_raw_entities() -> None:
    connection = FakeConnection([])
    repository = EntityReferenceRepository(connection)  # type: ignore[arg-type]

    result = repository.refresh_from_raw_entities()

    assert result == connection.cursor_obj.rowcount
    assert connection.commits == 0
