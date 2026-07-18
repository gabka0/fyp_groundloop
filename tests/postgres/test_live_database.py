"""Live PostgreSQL three-oracle, constraint, transaction, and plan tests."""

from __future__ import annotations

import random

import pytest
from helpers import (
    REFUTE_SCORES,
    SUPPORT_SCORES,
    insert_document,
    make_observation,
    make_repo,
    observe,
    register_answer,
)
from psycopg import Connection, errors

from groundloop.differential import DifferentialRunner
from groundloop.domain import (
    AnswerVersion,
    Claim,
    DecisionPolicy,
    ModelStamp,
    Question,
    SemanticObservation,
    SubjectKind,
)
from groundloop.events import (
    ChunkInput,
    DeleteDocumentVersionEvent,
    Event,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    ReplaceDocumentVersionEvent,
    apply_event,
)
from groundloop.incremental import IncrementalMaintenanceEngine
from groundloop.postgres import (
    MismatchCounts,
    PostgresEventConflictError,
    PostgresSnapshot,
    load_snapshot,
    read_mismatch_counts,
    read_oracle_states,
    read_server_metadata,
    record_epoch,
    temporary_m2_schema,
)
from groundloop.reference import compute_all_states
from groundloop.repository import InMemoryRepository

STAMP = ModelStamp("postgres-tests", "v1", "p1")


def _assert_three_way(
    connection: Connection[tuple[object, ...]],
    repository: InMemoryRepository,
    engine: IncrementalMaintenanceEngine,
) -> None:
    expected_claims, expected_answers = compute_all_states(repository)
    snapshot = PostgresSnapshot.capture(repository, engine)
    with temporary_m2_schema(connection):
        load_snapshot(connection, snapshot)
        oracle = read_oracle_states(connection)
        assert oracle.claims == expected_claims == engine.claim_states
        assert oracle.answers == expected_answers == engine.answer_states
        assert read_mismatch_counts(connection) == MismatchCounts(0, 0, 0)


def test_server_and_pgvector_are_available(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    with temporary_m2_schema(live_connection):
        metadata = read_server_metadata(live_connection)
        assert 160000 <= metadata.postgres_version_num < 170000
        assert metadata.pgvector_available_version is not None
        assert (
            metadata.pgvector_installed_version
            == metadata.pgvector_available_version
        )


def test_deterministic_support_refute_conflict_dedup_and_supersession(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    repository = make_repo()
    register_answer(repository)
    insert_document(
        repository,
        "ev-doc-1",
        "doc-1",
        "dv-1",
        (("p-1", "duplicate evidence"),),
    )
    insert_document(
        repository,
        "ev-doc-2",
        "doc-2",
        "dv-2",
        (("p-2", "duplicate evidence"),),
    )
    observe(repository, "ev-o-1", "o-1", "c1", "p-1", SUPPORT_SCORES)
    observe(repository, "ev-o-2", "o-2", "c1", "p-2", SUPPORT_SCORES)
    engine = IncrementalMaintenanceEngine.from_repository(repository)
    _assert_three_way(live_connection, repository, engine)

    runner = DifferentialRunner(repository, engine)
    runner.apply(
        ObserveEvent(
            "ev-o-2-new",
            make_observation("o-2-new", "c1", "p-2", REFUTE_SCORES),
        )
    )
    _assert_three_way(live_connection, runner.repository, runner.engine)


def test_deletion_replacement_inactive_chunk_and_policy_change(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    repository = make_repo()
    register_answer(repository)
    insert_document(
        repository,
        "ev-doc",
        "doc",
        "dv-1",
        (("p-1", "support evidence"),),
    )
    observe(repository, "ev-support", "o-support", "c1", "p-1", SUPPORT_SCORES)
    runner = DifferentialRunner.from_repository(repository)
    runner.apply(
        ReplaceDocumentVersionEvent(
            event_id="ev-replace",
            document_id="doc",
            old_document_version_id="dv-1",
            new_document_version_id="dv-2",
            content_hash="hash-dv-2",
            chunks=(ChunkInput("p-2", 0, "refuting evidence"),),
        )
    )
    runner.apply(
        ObserveEvent(
            "ev-late",
            make_observation("o-late", "c1", "p-1", REFUTE_SCORES),
        )
    )
    runner.apply(
        ObserveEvent(
            "ev-refute",
            make_observation("o-refute", "c1", "p-2", REFUTE_SCORES),
        )
    )
    runner.apply(
        PolicyChangeEvent(
            "ev-policy-high",
            DecisionPolicy("k-high", support_threshold=0.95, refute_threshold=0.95),
        )
    )
    _assert_three_way(live_connection, runner.repository, runner.engine)
    runner.apply(DeleteDocumentVersionEvent("ev-delete", "dv-2"))
    _assert_three_way(live_connection, runner.repository, runner.engine)


def test_snapshot_load_rolls_back_injected_failure_and_retries(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    repository = make_repo()
    register_answer(repository)
    insert_document(
        repository,
        "ev-doc",
        "doc",
        "dv",
        (("p", "evidence"),),
    )
    observe(repository, "ev-observe", "o", "c1", "p", SUPPORT_SCORES)
    engine = IncrementalMaintenanceEngine.from_repository(repository)
    snapshot = PostgresSnapshot.capture(repository, engine)

    def fail_after_observations(stage: str) -> None:
        if stage == "observations_loaded":
            raise RuntimeError("injected persistence failure")

    with temporary_m2_schema(live_connection):
        with pytest.raises(RuntimeError, match="injected persistence failure"):
            load_snapshot(live_connection, snapshot, fail_after_observations)
        remaining = live_connection.execute(
            "SELECT count(*) FROM groundloop_epoch"
        ).fetchone()
        assert remaining == (0,)
        load_snapshot(live_connection, snapshot)
        assert read_mismatch_counts(live_connection) == MismatchCounts(0, 0, 0)


def test_event_replay_is_idempotent_and_payload_conflict_is_rejected(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    with temporary_m2_schema(live_connection):
        first = record_epoch(
            live_connection, event_id="event", payload_hash="a" * 64
        )
        replay = record_epoch(
            live_connection, event_id="event", payload_hash="a" * 64
        )
        assert first[1] is True
        assert replay == (first[0], False)
        with pytest.raises(PostgresEventConflictError):
            record_epoch(
                live_connection, event_id="event", payload_hash="b" * 64
            )


def test_database_constraints_cover_active_version_currency_and_required_claim(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    with temporary_m2_schema(live_connection):
        epoch_id, _ = record_epoch(
            live_connection, event_id="event", payload_hash="a" * 64
        )
        live_connection.execute(
            "INSERT INTO groundloop_document VALUES ('doc', NULL, 'test')"
        )
        live_connection.execute(
            """
            INSERT INTO groundloop_document_version VALUES
                ('dv-1', 'doc', 'h1', %s, NULL)
            """,
            (epoch_id,),
        )
        with pytest.raises(errors.UniqueViolation):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    INSERT INTO groundloop_document_version VALUES
                        ('dv-2', 'doc', 'h2', %s, NULL)
                    """,
                    (epoch_id,),
                )

        live_connection.execute(
            "INSERT INTO groundloop_question VALUES ('q', 'q', %s)", (epoch_id,)
        )
        live_connection.execute(
            """
            INSERT INTO groundloop_answer_version VALUES
                ('a', 'q', 'a', 'm', 'v', 'p', %s)
            """,
            (epoch_id,),
        )
        with pytest.raises(errors.RaiseException, match="no required claim"):
            with live_connection.transaction():
                live_connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        live_connection.execute(
            """
            INSERT INTO groundloop_claim VALUES
                ('c', 'a', 'claim', 'm', 'v', 'p', true)
            """
        )
        live_connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        live_connection.execute("SET CONSTRAINTS ALL DEFERRED")


def _randomized_runner(seed: int) -> tuple[DifferentialRunner, list[Event]]:
    rng = random.Random(seed)
    repository = InMemoryRepository()
    apply_event(
        repository,
        PolicyChangeEvent("random-policy-0", DecisionPolicy("random-k0", 0.8, 0.8)),
    )
    repository.register_question(Question("random-q", "question"))
    repository.register_answer(
        AnswerVersion("random-a", "random-q", "answer", STAMP),
        (
            Claim("random-c0", "random-a", "claim 0", STAMP, True),
            Claim("random-c1", "random-a", "claim 1", STAMP, True),
        ),
    )
    events: list[Event] = []
    for index in range(4):
        events.append(
            InsertDocumentEvent(
                f"random-insert-{index}",
                f"random-doc-{index}",
                f"random-dv-{index}",
                f"hash-{index}",
                (ChunkInput(f"random-p-{index}", 0, f"evidence {index % 2}"),),
            )
        )
    for index in range(16):
        chunk_index = rng.randrange(4)
        claim_index = rng.randrange(2)
        observation_id = f"random-o-{index}"
        events.append(
            ObserveEvent(
                f"random-observe-{index}",
                SemanticObservation(
                    observation_id,
                    SubjectKind.CLAIM,
                    f"random-c{claim_index}",
                    f"random-p-{chunk_index}",
                    "verify",
                    rng.random(),
                    rng.random(),
                    rng.random(),
                    STAMP,
                    f"input-{observation_id}",
                ),
            )
        )
    return DifferentialRunner.from_repository(repository), events


def test_bounded_randomized_three_way_after_every_event(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    runner, events = _randomized_runner(20260718)
    for event in events:
        runner.apply(event)
        _assert_three_way(live_connection, runner.repository, runner.engine)


def _plan_lines(
    connection: Connection[tuple[object, ...]], query: str
) -> tuple[str, ...]:
    rows = connection.execute(f"EXPLAIN (ANALYZE, BUFFERS) {query}").fetchall()
    return tuple(str(row[0]) for row in rows)


def test_intended_indexes_are_usable_for_current_and_policy_range_queries(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    repository = make_repo()
    register_answer(repository)
    insert_document(
        repository, "ev-doc", "doc", "dv", (("p", "evidence"),)
    )
    observe(repository, "ev-observe", "o", "c1", "p", SUPPORT_SCORES)
    snapshot = PostgresSnapshot.capture(
        repository, IncrementalMaintenanceEngine.from_repository(repository)
    )
    with temporary_m2_schema(live_connection):
        load_snapshot(live_connection, snapshot)
        live_connection.execute("SET LOCAL enable_seqscan = off")
        current_plan = _plan_lines(
            live_connection,
            """
            SELECT observation_id FROM groundloop_observation_currency
            WHERE chunk_version_id = 'p'
            """,
        )
        policy_plan = _plan_lines(
            live_connection,
            """
            SELECT observation_id FROM groundloop_semantic_observation
            WHERE support_score >= 0.8 AND support_score < 0.95
            """,
        )
        assert any(
            "groundloop_current_observations_by_chunk" in line
            for line in current_plan
        )
        assert any(
            "groundloop_observation_support_scores" in line for line in policy_plan
        )
