"""Differential and failure-atomicity tests for the M4 PostgreSQL store."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from psycopg import Connection, Cursor

from groundloop.errors import EventConflictError, InvalidEventError
from groundloop.m4.contracts import (
    CandidatePolicyManifest,
    ChildClosure,
    CorpusUpdateIdentity,
    DiscoveryScope,
    JobAttempt,
    JobCompletion,
    JobKind,
    JobState,
    LogicalJobSpec,
    PairKey,
    UpdateKind,
    VectorIndexKind,
)
from groundloop.m4.persistence import PostgresM4RuntimeStore
from groundloop.m4.runtime.epoch import (
    CompletionPlan,
    RuntimeEpochState,
    TransitionResult,
)
from groundloop.postgres import record_epoch


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _seed_base(connection: Connection[tuple[object, ...]]) -> int:
    sealed_epoch, _ = record_epoch(
        connection, event_id="base-event", payload_hash=_hash("base")
    )
    connection.execute(
        """
        UPDATE groundloop_epoch
        SET revision = 1, structural_status = 'committed',
            semantic_status = 'sealed', evaluation_state = 'complete',
            publication_mode = 'strict', sealed_at = now()
        WHERE epoch_id = %s
        """,
        (sealed_epoch,),
    )
    connection.execute(
        "INSERT INTO groundloop_document VALUES ('doc', 'doc.txt', 'test')"
    )
    connection.execute(
        "INSERT INTO groundloop_document_version VALUES ('dv', 'doc', 'raw', %s, NULL)",
        (sealed_epoch,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_chunk_version VALUES
            ('chunk-1', 'dv', 0, 'evidence', %s, 'fixed-char-v1', %s, NULL)
        """,
        (_hash("evidence"), sealed_epoch),
    )
    connection.execute(
        "INSERT INTO groundloop_question VALUES ('question', 'question?', %s)",
        (sealed_epoch,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_answer_version VALUES
            ('answer', 'question', 'answer', 'generator', 'revision', 'prompt', %s)
        """,
        (sealed_epoch,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_claim VALUES
            ('claim-1', 'answer', 'claim', 'extractor', 'revision', 'prompt', true)
        """
    )
    connection.execute(
        """
        INSERT INTO groundloop_decision_policy VALUES
            ('decision-v1', 0.8, 0.8, 'v1', NULL, NULL, %s, NULL)
        """,
        (sealed_epoch,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_model_artifact (
            model_artifact_id, task, provider, model_id, immutable_revision,
            tokenizer_revision, license_id, config_hash
        ) VALUES ('embedder', 'embedding', 'test', 'embedder', 'rev', 'rev',
                  'MIT', %s)
        """,
        (_hash("embedder-config"),),
    )
    connection.execute(
        "INSERT INTO groundloop_m4_publication_head(epoch_id) VALUES (%s)",
        (sealed_epoch,),
    )
    return sealed_epoch


def _advance_head(cursor: Cursor[Any], epoch_id: int) -> None:
    cursor.execute(
        """
        INSERT INTO groundloop_m4_publication_head(singleton, epoch_id)
        VALUES (true, %s)
        ON CONFLICT (singleton) DO UPDATE
        SET epoch_id = EXCLUDED.epoch_id, updated_at = now()
        """,
        (epoch_id,),
    )


def _no_structural(_cursor: Cursor[Any], _epoch_id: int) -> None:
    return


def _policy(cap: int = 4) -> CandidatePolicyManifest:
    return CandidatePolicyManifest.build(
        policy_id="candidate-v1",
        embedding_model_artifact_id="embedder",
        claim_role_template_hash=_hash("claim-role"),
        chunk_role_template_hash=_hash("chunk-role"),
        vector_method_version="reverse-bge-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=_hash("build"),
        vector_search_config_hash=_hash("search"),
        lexical_method_version="lexical-v1",
        lexical_config_hash=_hash("lexical"),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        claim_registry_snapshot_id="registry-1",
        claim_count=1,
        fusion_version="interleave-v1",
        approximate_cap_per_inserted_chunk=cap,
        frontier_depth=4,
        verifier_execution_spec_hash=_hash("verifier"),
        decision_policy_version="decision-v1",
    )


def _update(event_id: str, previous: int | None) -> CorpusUpdateIdentity:
    return CorpusUpdateIdentity(
        event_id=event_id,
        payload_hash=_hash(f"payload:{event_id}"),
        update_kind=UpdateKind.INSERT,
        previous_published_epoch_id=previous,
        candidate_policy_id="candidate-v1",
    )


def _job(
    kind: JobKind,
    *,
    event_id: str,
    parent: str | None = None,
) -> LogicalJobSpec:
    execution_hash = _hash(f"execution:{kind.value}")
    claim_id = "claim-1" if kind is not JobKind.IMPACT_DISCOVERY else ""
    chunk_id = "chunk-1" if kind is not JobKind.FRONTIER_RETRIEVE else ""
    job_id = LogicalJobSpec.derive_job_id(
        event_id=event_id,
        kind=kind,
        candidate_policy_id="candidate-v1",
        execution_spec_hash=execution_hash,
        parent_job_id=parent or "",
        claim_id=claim_id,
        chunk_version_id=chunk_id,
    )
    return LogicalJobSpec(
        job_id=job_id,
        event_id=event_id,
        kind=kind,
        candidate_policy_id="candidate-v1",
        payload_hash=_hash(f"job:{job_id}"),
        execution_spec_hash=execution_hash,
        parent_job_id=parent,
        pair=(PairKey("claim-1", "chunk-1") if kind is JobKind.VERIFY_PAIR else None),
        target_claim_id="claim-1" if kind is JobKind.FRONTIER_RETRIEVE else None,
        target_chunk_version_id=(
            "chunk-1" if kind is JobKind.IMPACT_DISCOVERY else None
        ),
        expandable=kind is not JobKind.VERIFY_PAIR,
    )


def _attempt(spec: LogicalJobSpec, ordinal: int = 1) -> JobAttempt:
    return JobAttempt(
        attempt_id=f"attempt-{spec.job_id[:12]}-{ordinal}",
        job_id=spec.job_id,
        execution_spec_hash=spec.execution_spec_hash,
        attempt_ordinal=ordinal,
        lease_token_hash=_hash(f"lease:{spec.job_id}:{ordinal}"),
    )


def _completion(
    spec: LogicalJobSpec,
    state: JobState,
    children: tuple[LogicalJobSpec, ...] = (),
) -> JobCompletion:
    result_hash = _hash(f"result:{spec.job_id}:{state.value}")
    closure = (
        ChildClosure.build(
            parent_job_id=spec.job_id,
            result_artifact_hash=result_hash,
            child_job_ids=tuple(child.job_id for child in children),
        )
        if spec.expandable
        else None
    )
    return JobCompletion.build(
        job_id=spec.job_id,
        payload_hash=spec.payload_hash,
        execution_spec_hash=spec.execution_spec_hash,
        result_artifact_id=f"artifact:{spec.job_id}",
        result_artifact_hash=result_hash,
        terminal_state=state,
        child_closure=closure,
    )


def _opened_store(
    connection: Connection[tuple[object, ...]], event_id: str = "event-1"
) -> tuple[PostgresM4RuntimeStore, int, LogicalJobSpec]:
    previous = _seed_base(connection)
    store = PostgresM4RuntimeStore(connection)
    assert store.register_candidate_policy(_policy())
    root = _job(JobKind.IMPACT_DISCOVERY, event_id=event_id)
    scope = DiscoveryScope(root.job_id, "registry-1", ("claim-1",))
    opened = store.open_epoch(
        _update(event_id, previous),
        (root,),
        (scope,),
        registry_snapshot_id="registry-1",
        structural_action=_no_structural,
        event_manifest={"fixture": "runtime-store-v1"},
    )
    return store, opened.epoch.epoch_id, root


def _complete_store(
    store: PostgresM4RuntimeStore,
    plan: CompletionPlan,
    attempt: JobAttempt,
    *,
    active_chunk_ids: frozenset[str],
) -> TransitionResult:
    return store.complete(
        plan,
        active_chunk_ids=active_chunk_ids,
        attempt_id=attempt.attempt_id,
        lease_token_hash=attempt.lease_token_hash,
        lease_expected_revision=plan.expected_revision,
    )


def test_candidate_policy_registration_is_content_validated(
    m4_connection: Connection[tuple[object, ...]],
) -> None:
    _seed_base(m4_connection)
    store = PostgresM4RuntimeStore(m4_connection)

    assert store.register_candidate_policy(_policy())
    assert not store.register_candidate_policy(_policy())
    assert store.read_candidate_policy("candidate-v1") == _policy()
    with pytest.raises(EventConflictError, match="different content"):
        store.register_candidate_policy(_policy(cap=5))


def test_parent_expansion_retry_child_completion_and_seal_match_pure_model(
    m4_connection: Connection[tuple[object, ...]],
) -> None:
    store, epoch_id, root = _opened_store(m4_connection)
    opened = store.read_book()
    assert opened.active_epoch_id == epoch_id
    assert store.open_epoch(
        opened.epochs[-1].update,
        (root,),
        (DiscoveryScope(root.job_id, "registry-1", ("claim-1",)),),
        registry_snapshot_id="registry-1",
        structural_action=_no_structural,
        event_manifest={"fixture": "runtime-store-v1"},
    ).replayed

    first = _attempt(root)
    store.start_attempt(
        epoch_id,
        first,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    store.mark_retryable_failure(epoch_id, root.job_id, first.attempt_id)
    second = _attempt(root, 2)
    store.start_attempt(epoch_id, second)
    child = _job(JobKind.VERIFY_PAIR, event_id="event-1", parent=root.job_id)
    before_parent = store.read_epoch(epoch_id)
    parent_plan = CompletionPlan(
        epoch_id,
        before_parent.revision,
        _completion(root, JobState.COMPLETED_ACTIVE, (child,)),
        (child,),
    )
    _complete_store(store, parent_plan, second, active_chunk_ids=frozenset({"chunk-1"}))
    assert _complete_store(
        store, parent_plan, second, active_chunk_ids=frozenset({"chunk-1"})
    ).replayed
    conflicting_completion = replace(
        parent_plan,
        completion=_completion(root, JobState.COMPLETED_INACTIVE, (child,)),
    )
    with pytest.raises(EventConflictError, match="different content"):
        _complete_store(
            store,
            conflicting_completion,
            second,
            active_chunk_ids=frozenset({"chunk-1"}),
        )
    child_attempt = _attempt(child)
    store.start_attempt(epoch_id, child_attempt)
    before_child = store.read_epoch(epoch_id)
    child_plan = CompletionPlan(
        epoch_id,
        before_child.revision,
        _completion(child, JobState.COMPLETED_ACTIVE),
    )
    _complete_store(
        store, child_plan, child_attempt, active_chunk_ids=frozenset({"chunk-1"})
    )
    complete_epoch = store.read_epoch(epoch_id)
    assert complete_epoch.state is RuntimeEpochState.SEMANTIC_COMPLETE
    sealed = store.seal_epoch(
        epoch_id,
        complete_epoch.revision,
        publication_action=_advance_head,
    )

    assert sealed.book.last_sealed_epoch_id == epoch_id
    assert store.read_epoch(epoch_id).state is RuntimeEpochState.SEALED


def test_completion_failure_injection_rolls_back_children_parent_and_revision(
    m4_connection: Connection[tuple[object, ...]],
) -> None:
    store, epoch_id, root = _opened_store(m4_connection)
    root_attempt = _attempt(root)
    store.start_attempt(epoch_id, root_attempt)
    child = _job(JobKind.VERIFY_PAIR, event_id="event-1", parent=root.job_id)
    before = store.read_book()
    plan = CompletionPlan(
        epoch_id,
        before.epochs[-1].revision,
        _completion(root, JobState.COMPLETED_ACTIVE, (child,)),
        (child,),
    )

    def crash(point: str) -> None:
        if point == "completion_children_written":
            raise RuntimeError("injected completion crash")

    with pytest.raises(RuntimeError, match="injected completion crash"):
        store.complete(
            plan,
            active_chunk_ids=frozenset({"chunk-1"}),
            attempt_id=root_attempt.attempt_id,
            lease_token_hash=root_attempt.lease_token_hash,
            lease_expected_revision=plan.expected_revision,
            failure_injector=crash,
        )

    assert store.read_book() == before
    assert m4_connection.execute(
        "SELECT count(*) FROM groundloop_semantic_job_dependency WHERE epoch_id = %s",
        (epoch_id,),
    ).fetchone() == (0,)


def test_completion_rejects_stale_attempt_and_updates_only_bound_attempt(
    m4_connection: Connection[tuple[object, ...]],
) -> None:
    store, epoch_id, root = _opened_store(m4_connection)
    first = _attempt(root)
    first_started = store.start_attempt(epoch_id, first)
    first_revision = next(
        epoch.revision
        for epoch in first_started.book.epochs
        if epoch.epoch_id == epoch_id
    )
    store.mark_retryable_failure(epoch_id, root.job_id, first.attempt_id)
    second = _attempt(root, 2)
    second_started = store.start_attempt(epoch_id, second)
    current = next(
        epoch for epoch in second_started.book.epochs if epoch.epoch_id == epoch_id
    )
    plan = CompletionPlan(
        epoch_id,
        current.revision,
        _completion(root, JobState.COMPLETED_ACTIVE),
    )
    before = store.read_book()

    with pytest.raises(EventConflictError, match="stale attempt"):
        store.complete(
            plan,
            active_chunk_ids=frozenset({"chunk-1"}),
            attempt_id=first.attempt_id,
            lease_token_hash=first.lease_token_hash,
            lease_expected_revision=first_revision,
        )
    with pytest.raises(EventConflictError, match="lease token"):
        store.complete(
            plan,
            active_chunk_ids=frozenset({"chunk-1"}),
            attempt_id=second.attempt_id,
            lease_token_hash=_hash("wrong-lease-token"),
            lease_expected_revision=current.revision,
        )
    assert store.read_book() == before
    assert m4_connection.execute(
        """
        SELECT attempt_id, attempt_state
        FROM groundloop_semantic_job_attempt
        WHERE job_id = %s ORDER BY attempt_ordinal
        """,
        (root.job_id,),
    ).fetchall() == [
        (first.attempt_id, "failed"),
        (second.attempt_id, "leased"),
    ]

    completed = _complete_store(
        store, plan, second, active_chunk_ids=frozenset({"chunk-1"})
    )
    assert not completed.replayed
    assert m4_connection.execute(
        """
        SELECT attempt_id, attempt_state
        FROM groundloop_semantic_job_attempt
        WHERE job_id = %s ORDER BY attempt_ordinal
        """,
        (root.job_id,),
    ).fetchall() == [
        (first.attempt_id, "failed"),
        (second.attempt_id, "completed"),
    ]


def test_seal_failure_injection_preserves_complete_unsealed_epoch(
    m4_connection: Connection[tuple[object, ...]],
) -> None:
    store, epoch_id, root = _opened_store(m4_connection)
    root_attempt = _attempt(root)
    store.start_attempt(epoch_id, root_attempt)
    before_done = store.read_epoch(epoch_id)
    completion_plan = CompletionPlan(
        epoch_id,
        before_done.revision,
        _completion(root, JobState.COMPLETED_ACTIVE),
    )
    _complete_store(
        store,
        completion_plan,
        root_attempt,
        active_chunk_ids=frozenset({"chunk-1"}),
    )
    before_seal = store.read_book()

    def crash(point: str) -> None:
        if point == "seal_epoch_written":
            raise RuntimeError("injected seal crash")

    with pytest.raises(RuntimeError, match="injected seal crash"):
        store.seal_epoch(
            epoch_id,
            before_seal.epochs[-1].revision,
            publication_action=_advance_head,
            failure_injector=crash,
        )

    assert store.read_book() == before_seal
    assert store.read_epoch(epoch_id).state is RuntimeEpochState.SEMANTIC_COMPLETE
    assert m4_connection.execute(
        "SELECT epoch_id FROM groundloop_m4_publication_head WHERE singleton"
    ).fetchone() == (before_seal.epochs[-1].update.previous_published_epoch_id,)


def test_failed_epoch_replay_conflict_and_late_inactive_completion(
    m4_connection: Connection[tuple[object, ...]],
) -> None:
    store, epoch_id, root = _opened_store(m4_connection)
    root_attempt = _attempt(root)
    store.start_attempt(epoch_id, root_attempt)
    running = store.read_epoch(epoch_id)
    failed = store.fail_epoch(epoch_id, running.revision, "operator stop")
    assert failed.book.active_epoch_id is None
    assert store.fail_epoch(epoch_id, running.revision, "operator stop").replayed
    with pytest.raises(EventConflictError, match="another reason"):
        store.fail_epoch(epoch_id, running.revision, "different reason")
    with pytest.raises(InvalidEventError, match="no new attempts"):
        store.start_attempt(epoch_id, _attempt(root, 2))

    current = store.read_epoch(epoch_id)
    late = CompletionPlan(
        epoch_id,
        current.revision,
        _completion(root, JobState.COMPLETED_INACTIVE),
    )
    _complete_store(store, late, root_attempt, active_chunk_ids=frozenset())
    persisted = store.read_epoch(epoch_id)
    assert persisted.state is RuntimeEpochState.FAILED
    assert persisted.failure_reason == "operator stop"
    assert m4_connection.execute(
        """
        SELECT attempt_state FROM groundloop_semantic_job_attempt
        WHERE attempt_id = %s
        """,
        (root_attempt.attempt_id,),
    ).fetchone() == ("completed",)


def test_serialization_rejects_second_epoch_until_first_seals(
    m4_connection: Connection[tuple[object, ...]],
) -> None:
    store, epoch_id, _ = _opened_store(m4_connection)
    second_update = _update("event-2", previous=epoch_id)
    with pytest.raises(InvalidEventError, match="already active"):
        store.open_epoch(
            second_update,
            (),
            registry_snapshot_id="registry-1",
            structural_action=_no_structural,
        )
    with pytest.raises(EventConflictError, match="different declaration"):
        original = store.read_epoch(epoch_id)
        store.open_epoch(
            replace(original.update, payload_hash=_hash("conflict")),
            original.jobs[0:0],
            registry_snapshot_id="registry-1",
            structural_action=_no_structural,
        )


def test_structural_callback_and_epoch_declaration_roll_back_together(
    m4_connection: Connection[tuple[object, ...]],
) -> None:
    published_epoch = _seed_base(m4_connection)
    store = PostgresM4RuntimeStore(m4_connection)
    store.register_candidate_policy(_policy())
    root = _job(JobKind.IMPACT_DISCOVERY, event_id="atomic-open-event")

    def write_structural(cursor: Cursor[Any], epoch_id: int) -> None:
        cursor.execute(
            """
            INSERT INTO groundloop_document
                (document_id, source_uri, authority_class)
            VALUES (%s, %s, %s)
            """,
            (f"new-doc-{epoch_id}", "new.txt", "test"),
        )

    def crash(point: str) -> None:
        if point == "open_structural_written":
            raise RuntimeError("injected structural crash")

    with pytest.raises(RuntimeError, match="injected structural crash"):
        store.open_epoch(
            _update("atomic-open-event", published_epoch),
            (root,),
            (DiscoveryScope(root.job_id, "registry-1", ("claim-1",)),),
            registry_snapshot_id="registry-1",
            structural_action=write_structural,
            failure_injector=crash,
        )

    assert m4_connection.execute(
        "SELECT count(*) FROM groundloop_epoch WHERE event_id = 'atomic-open-event'"
    ).fetchone() == (0,)
    assert m4_connection.execute(
        "SELECT count(*) FROM groundloop_document WHERE source_uri = 'new.txt'"
    ).fetchone() == (0,)
    assert m4_connection.execute(
        "SELECT count(*) FROM groundloop_semantic_job WHERE job_id = %s",
        (root.job_id,),
    ).fetchone() == (0,)


def test_initialized_publication_head_is_authoritative_over_unrelated_seal(
    m4_connection: Connection[tuple[object, ...]],
) -> None:
    published_epoch = _seed_base(m4_connection)
    unrelated_epoch, _ = record_epoch(
        m4_connection,
        event_id="unrelated-sealed-event",
        payload_hash=_hash("unrelated"),
    )
    m4_connection.execute(
        """
        UPDATE groundloop_epoch
        SET structural_status = 'committed', semantic_status = 'sealed',
            evaluation_state = 'complete', publication_mode = 'strict',
            sealed_at = now()
        WHERE epoch_id = %s
        """,
        (unrelated_epoch,),
    )
    store = PostgresM4RuntimeStore(m4_connection)
    store.register_candidate_policy(_policy())
    root = _job(JobKind.IMPACT_DISCOVERY, event_id="head-event")
    opened = store.open_epoch(
        _update("head-event", published_epoch),
        (root,),
        (DiscoveryScope(root.job_id, "registry-1", ("claim-1",)),),
        registry_snapshot_id="registry-1",
        structural_action=_no_structural,
    )

    assert opened.epoch.update.previous_published_epoch_id == published_epoch
