from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

import pytest
from psycopg import Connection

from groundloop.errors import (
    DanglingReferenceError,
    EventConflictError,
    InvalidEventError,
    ValidationError,
)
from groundloop.m4.evaluation_overlay import (
    ClaimJobDelta,
    EvaluationLifecycle,
    EvaluationObjectType,
    EvaluationRevisionConflict,
    EvaluationTransition,
    EvaluationTransitionKind,
    PostgresEvaluationOverlayStore,
)
from groundloop.m4.runtime.epoch import EvaluationState

if TYPE_CHECKING:
    from conftest import OverlayDatabase


def _store(database: OverlayDatabase) -> PostgresEvaluationOverlayStore:
    return PostgresEvaluationOverlayStore(database.connection)


def _row_count(connection: Connection[Any], table: str, epoch_id: int) -> int:
    if table not in {
        "groundloop_m4_evaluation_override_counter",
        "groundloop_m4_evaluation_counter_transition",
    }:
        raise AssertionError("test helper received an unexpected table")
    row = connection.execute(
        f"SELECT count(*) FROM {table} WHERE epoch_id = %s",  # noqa: S608
        (epoch_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_scope_default_required_projection_and_effective_points(
    overlay_database: OverlayDatabase,
) -> None:
    store = _store(overlay_database)
    declared = store.declare_epoch(
        2,
        revision=4,
        confirmed_as_of_epoch=1,
        open_discovery_scope_count=1,
    )
    assert not declared.replayed
    assert declared.default.default_state is EvaluationState.PENDING
    inherited = store.read_effective(
        2, EvaluationObjectType.CLAIM, "required-2"
    )
    assert inherited.state is EvaluationState.PENDING
    assert inherited.inherited_default
    assert inherited.open_discovery_scope_count == 1

    receipt = store.apply_transition(
        2,
        EvaluationTransition(
            "close-scope-and-declare-children",
            expected_revision=4,
            scope_delta=-1,
            claim_job_deltas=(
                ClaimJobDelta("required-1", 2),
                ClaimJobDelta("optional-1", 3),
            ),
        ),
    )
    assert receipt.override_rows_written == 3
    assert store.read_default(2).open_discovery_scope_count == 0
    assert store.read_effective(
        2, EvaluationObjectType.CLAIM, "required-1"
    ).open_required_job_count == 2
    assert store.read_effective(
        2, EvaluationObjectType.CLAIM, "optional-1"
    ).open_required_job_count == 3
    answer = store.read_effective(2, EvaluationObjectType.ANSWER, "answer-1")
    assert answer.state is EvaluationState.PENDING
    assert answer.open_required_job_count == 2

    store.apply_transition(
        2,
        EvaluationTransition(
            "finish-required-children",
            expected_revision=5,
            claim_job_deltas=(ClaimJobDelta("required-1", -2),),
        ),
    )
    answer = store.read_effective(2, EvaluationObjectType.ANSWER, "answer-1")
    optional_claim = store.read_effective(
        2, EvaluationObjectType.CLAIM, "optional-1"
    )
    assert answer.state is EvaluationState.COMPLETE
    assert answer.inherited_default
    assert optional_claim.state is EvaluationState.PENDING
    assert not optional_claim.inherited_default


@pytest.mark.parametrize("fanout", (1, 10_000))
def test_multi_child_signed_delta_has_scale_independent_row_writes(
    overlay_database: OverlayDatabase, fanout: int
) -> None:
    epoch_id = 3 if fanout == 1 else 4
    store = _store(overlay_database)
    store.declare_epoch(
        epoch_id,
        revision=0,
        confirmed_as_of_epoch=1,
        open_discovery_scope_count=0,
    )
    opened = store.apply_transition(
        epoch_id,
        EvaluationTransition(
            "open-many-children",
            expected_revision=0,
            claim_job_deltas=(ClaimJobDelta("required-1", fanout),),
        ),
    )
    assert opened.override_rows_written == 2
    assert _row_count(
        overlay_database.connection,
        "groundloop_m4_evaluation_override_counter",
        epoch_id,
    ) == 2
    assert store.read_effective(
        epoch_id, EvaluationObjectType.ANSWER, "answer-1"
    ).open_required_job_count == fanout

    closed = store.apply_transition(
        epoch_id,
        EvaluationTransition(
            "close-many-children",
            expected_revision=1,
            claim_job_deltas=(ClaimJobDelta("required-1", -fanout),),
        ),
    )
    assert closed.override_rows_written == 2
    assert _row_count(
        overlay_database.connection,
        "groundloop_m4_evaluation_override_counter",
        epoch_id,
    ) == 0
    effective = store.read_effective(
        epoch_id, EvaluationObjectType.CLAIM, "required-1"
    )
    assert effective.state is EvaluationState.COMPLETE
    assert effective.inherited_default


def test_exact_replay_conflict_and_stale_revision_are_atomic(
    overlay_database: OverlayDatabase,
) -> None:
    store = _store(overlay_database)
    store.declare_epoch(
        5,
        revision=7,
        confirmed_as_of_epoch=1,
        open_discovery_scope_count=0,
    )
    transition = EvaluationTransition(
        "job-open",
        expected_revision=7,
        claim_job_deltas=(ClaimJobDelta("required-1", 1),),
    )
    first = store.apply_transition(5, transition)
    replay = store.apply_transition(5, transition)
    assert not first.replayed
    assert replay.replayed
    assert replay == first.__class__(
        first.epoch_id,
        first.transition_id,
        first.from_revision,
        first.to_revision,
        first.override_rows_written,
        True,
    )
    assert store.read_default(5).revision == 8
    assert _row_count(
        overlay_database.connection,
        "groundloop_m4_evaluation_counter_transition",
        5,
    ) == 1

    with pytest.raises(EventConflictError):
        store.apply_transition(
            5,
            EvaluationTransition(
                "job-open",
                expected_revision=7,
                claim_job_deltas=(ClaimJobDelta("required-1", 2),),
            ),
        )
    with pytest.raises(EvaluationRevisionConflict):
        store.apply_transition(
            5,
            EvaluationTransition("stale", expected_revision=7, scope_delta=1),
        )
    assert store.read_default(5).revision == 8
    assert store.read_effective(
        5, EvaluationObjectType.CLAIM, "required-1"
    ).open_required_job_count == 1


def test_concurrent_exact_replay_and_competing_cas(
    overlay_database: OverlayDatabase,
) -> None:
    store = _store(overlay_database)
    store.declare_epoch(
        6,
        revision=0,
        confirmed_as_of_epoch=1,
        open_discovery_scope_count=0,
    )
    transition = EvaluationTransition(
        "concurrent-same",
        expected_revision=0,
        claim_job_deltas=(ClaimJobDelta("required-1", 1),),
    )

    def apply_same() -> bool:
        with overlay_database.connect() as connection:
            return PostgresEvaluationOverlayStore(connection).apply_transition(
                6, transition
            ).replayed

    with ThreadPoolExecutor(max_workers=2) as executor:
        replay_flags = sorted(executor.map(lambda _: apply_same(), range(2)))
    assert replay_flags == [False, True]
    assert store.read_default(6).revision == 1
    assert store.read_effective(
        6, EvaluationObjectType.CLAIM, "required-1"
    ).open_required_job_count == 1

    store.declare_epoch(
        7,
        revision=0,
        confirmed_as_of_epoch=1,
        open_discovery_scope_count=0,
    )

    def compete(index: int) -> str:
        with overlay_database.connect() as connection:
            competing_store = PostgresEvaluationOverlayStore(connection)
            try:
                competing_store.apply_transition(
                    7,
                    EvaluationTransition(
                        f"competitor-{index}",
                        expected_revision=0,
                        scope_delta=1,
                    ),
                )
            except EvaluationRevisionConflict:
                return "stale"
            return "applied"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = sorted(executor.map(compete, range(2)))
    assert results == ["applied", "stale"]
    assert store.read_default(7).open_discovery_scope_count == 1
    assert store.read_default(7).revision == 1


def test_failure_and_seal_are_terminal_exact_replay_safe_transitions(
    overlay_database: OverlayDatabase,
) -> None:
    store = _store(overlay_database)
    store.declare_epoch(
        8,
        revision=0,
        confirmed_as_of_epoch=1,
        open_discovery_scope_count=0,
    )
    store.apply_transition(
        8,
        EvaluationTransition(
            "open-before-failure",
            expected_revision=0,
            claim_job_deltas=(ClaimJobDelta("required-1", 1),),
        ),
    )
    failure = EvaluationTransition(
        "epoch-failed",
        expected_revision=1,
        kind=EvaluationTransitionKind.FAIL,
    )
    assert not store.apply_transition(8, failure).replayed
    assert store.apply_transition(8, failure).replayed
    failed = store.read_default(8)
    assert failed.lifecycle is EvaluationLifecycle.FAILED
    assert store.read_effective(
        8, EvaluationObjectType.ANSWER, "answer-1"
    ).state is EvaluationState.FAILED
    with pytest.raises(InvalidEventError):
        store.apply_transition(
            8, EvaluationTransition("after-failure", expected_revision=2)
        )

    store.declare_epoch(
        9,
        revision=3,
        confirmed_as_of_epoch=1,
        open_discovery_scope_count=0,
    )
    seal = EvaluationTransition(
        "epoch-sealed",
        expected_revision=3,
        kind=EvaluationTransitionKind.SEAL,
    )
    assert not store.apply_transition(9, seal).replayed
    assert store.apply_transition(9, seal).replayed
    sealed = store.read_default(9)
    assert sealed.lifecycle is EvaluationLifecycle.SEALED
    assert sealed.confirmed_as_of_epoch == 9
    assert sealed.revision == 4
    assert store.read_effective(
        9, EvaluationObjectType.CLAIM, "required-2"
    ).state is EvaluationState.COMPLETE


def test_seal_rejects_open_scope_or_override_without_partial_writes(
    overlay_database: OverlayDatabase,
) -> None:
    store = _store(overlay_database)
    store.declare_epoch(
        10,
        revision=0,
        confirmed_as_of_epoch=1,
        open_discovery_scope_count=1,
    )
    with pytest.raises(ValidationError, match="open discovery"):
        store.apply_transition(
            10,
            EvaluationTransition(
                "bad-seal-scope",
                expected_revision=0,
                kind=EvaluationTransitionKind.SEAL,
            ),
        )
    assert store.read_default(10).revision == 0
    assert _row_count(
        overlay_database.connection,
        "groundloop_m4_evaluation_counter_transition",
        10,
    ) == 0

    store.declare_epoch(
        11,
        revision=0,
        confirmed_as_of_epoch=1,
        open_discovery_scope_count=0,
    )
    store.apply_transition(
        11,
        EvaluationTransition(
            "open-job",
            expected_revision=0,
            claim_job_deltas=(ClaimJobDelta("optional-only", 1),),
        ),
    )
    with pytest.raises(ValidationError, match="open required job"):
        store.apply_transition(
            11,
            EvaluationTransition(
                "bad-seal-job",
                expected_revision=1,
                kind=EvaluationTransitionKind.SEAL,
            ),
        )
    assert store.read_default(11).revision == 1
    optional_answer = store.read_effective(
        11, EvaluationObjectType.ANSWER, "answer-optional"
    )
    assert optional_answer.state is EvaluationState.COMPLETE
    assert optional_answer.inherited_default


def test_negative_counters_and_missing_claims_rollback_whole_transition(
    overlay_database: OverlayDatabase,
) -> None:
    store = _store(overlay_database)
    store.declare_epoch(
        12,
        revision=0,
        confirmed_as_of_epoch=1,
        open_discovery_scope_count=0,
    )
    with pytest.raises(ValidationError, match="cannot become negative"):
        store.apply_transition(
            12,
            EvaluationTransition(
                "negative-job",
                expected_revision=0,
                claim_job_deltas=(ClaimJobDelta("required-1", -1),),
            ),
        )
    with pytest.raises(DanglingReferenceError, match="missing claims"):
        store.apply_transition(
            12,
            EvaluationTransition(
                "missing-claim",
                expected_revision=0,
                claim_job_deltas=(ClaimJobDelta("not-registered", 1),),
            ),
        )
    assert store.read_default(12).revision == 0
    assert _row_count(
        overlay_database.connection,
        "groundloop_m4_evaluation_override_counter",
        12,
    ) == 0


def test_override_point_lookup_is_backed_by_composite_primary_key(
    overlay_database: OverlayDatabase,
) -> None:
    columns = overlay_database.connection.execute(
        """
        SELECT array_agg(attribute.attname ORDER BY key_columns.ordinality)
        FROM pg_index AS index_row
        JOIN pg_class AS table_row ON table_row.oid = index_row.indrelid
        JOIN unnest(index_row.indkey) WITH ORDINALITY AS key_columns(attnum, ordinality)
          ON true
        JOIN pg_attribute AS attribute
          ON attribute.attrelid = table_row.oid
         AND attribute.attnum = key_columns.attnum
        WHERE table_row.oid =
              'groundloop_m4_evaluation_override_counter'::regclass
          AND index_row.indisprimary
        """
    ).fetchone()
    assert columns is not None
    assert list(columns[0]) == ["epoch_id", "object_type", "object_id"]


def test_scope_counter_preserves_multiplicity_without_object_writes(
    overlay_database: OverlayDatabase,
) -> None:
    store = _store(overlay_database)
    store.declare_epoch(
        14,
        revision=0,
        confirmed_as_of_epoch=1,
        open_discovery_scope_count=2,
    )
    store.apply_transition(
        14,
        EvaluationTransition("close-one-of-two", expected_revision=0, scope_delta=-1),
    )
    one_open = store.read_default(14)
    assert one_open.open_discovery_scope_count == 1
    assert one_open.default_state is EvaluationState.PENDING
    store.apply_transition(
        14,
        EvaluationTransition("open-two-more", expected_revision=1, scope_delta=2),
    )
    store.apply_transition(
        14,
        EvaluationTransition("close-all-three", expected_revision=2, scope_delta=-3),
    )
    closed = store.read_default(14)
    assert closed.open_discovery_scope_count == 0
    assert closed.default_state is EvaluationState.COMPLETE
    assert _row_count(
        overlay_database.connection,
        "groundloop_m4_evaluation_override_counter",
        14,
    ) == 0


def test_declaration_exact_replay_and_conflict(
    overlay_database: OverlayDatabase,
) -> None:
    store = _store(overlay_database)
    first = store.declare_epoch(
        13,
        revision=2,
        confirmed_as_of_epoch=1,
        open_discovery_scope_count=2,
    )
    replay = store.declare_epoch(
        13,
        revision=2,
        confirmed_as_of_epoch=1,
        open_discovery_scope_count=2,
    )
    assert not first.replayed
    assert replay.replayed
    with pytest.raises(EventConflictError):
        store.declare_epoch(
            13,
            revision=2,
            confirmed_as_of_epoch=1,
            open_discovery_scope_count=1,
        )
