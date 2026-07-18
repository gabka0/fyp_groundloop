"""Dependency-free checks for the typed snapshot capture boundary."""

from helpers import SUPPORT_SCORES, insert_document, make_repo, observe, register_answer

from groundloop.incremental import IncrementalMaintenanceEngine
from groundloop.postgres import PostgresSnapshot


def test_capture_preserves_history_currency_and_materialized_state() -> None:
    repository = make_repo()
    register_answer(repository)
    insert_document(
        repository,
        "ev-insert",
        "doc",
        "dv",
        (("p", "evidence"),),
    )
    observe(repository, "ev-observe-1", "o1", "c1", "p", SUPPORT_SCORES)
    observe(repository, "ev-observe-2", "o2", "c1", "p", SUPPORT_SCORES)
    engine = IncrementalMaintenanceEngine.from_repository(repository)

    snapshot = PostgresSnapshot.capture(repository, engine)

    assert snapshot.revision == 4
    assert tuple(row.event_id for row in snapshot.epochs) == (
        "ev-policy-initial",
        "ev-insert",
        "ev-observe-1",
        "ev-observe-2",
    )
    assert tuple(item.observation_id for item in snapshot.observations) == (
        "o1",
        "o2",
    )
    assert snapshot.current_observation_ids == ("o2",)
    assert snapshot.materialized_claims[0].state.support_count == 1
    assert snapshot.materialized_claims[0].certificate.support_observation_id == "o2"
