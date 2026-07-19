"""Differential and failure-atomicity tests for the M4 PostgreSQL store."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from psycopg import Connection

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
from groundloop.m4.runtime.epoch import CompletionPlan, RuntimeEpochState
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
    return sealed_epoch


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
        event_manifest={"fixture": "runtime-store-v1"},
    )
    return store, opened.epoch.epoch_id, root


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
        event_manifest={"fixture": "runtime-store-v1"},
    ).replayed

    first = _attempt(root)
    store.start_attempt(
        epoch_id,
        first,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    store.mark_retryable_failure(epoch_id, root.job_id, first.attempt_id)
    store.start_attempt(epoch_id, _attempt(root, 2))
    child = _job(JobKind.VERIFY_PAIR, event_id="event-1", parent=root.job_id)
    before_parent = store.read_epoch(epoch_id)
    parent_plan = CompletionPlan(
        epoch_id,
        before_parent.revision,
        _completion(root, JobState.COMPLETED_ACTIVE, (child,)),
        (child,),
    )
    store.complete(parent_plan, active_chunk_ids=frozenset({"chunk-1"}))
    assert store.complete(parent_plan, active_chunk_ids=frozenset({"chunk-1"})).replayed
    conflicting_completion = replace(
        parent_plan,
        completion=_completion(root, JobState.COMPLETED_INACTIVE, (child,)),
    )
    with pytest.raises(EventConflictError, match="different content"):
        store.complete(
            conflicting_completion,
            active_chunk_ids=frozenset({"chunk-1"}),
        )
    store.start_attempt(epoch_id, _attempt(child))
    before_child = store.read_epoch(epoch_id)
    child_plan = CompletionPlan(
        epoch_id,
        before_child.revision,
        _completion(child, JobState.COMPLETED_ACTIVE),
    )
    store.complete(child_plan, active_chunk_ids=frozenset({"chunk-1"}))
    complete_epoch = store.read_epoch(epoch_id)
    assert complete_epoch.state is RuntimeEpochState.SEMANTIC_COMPLETE
    sealed = store.seal_epoch(epoch_id, complete_epoch.revision)

    assert sealed.book.last_sealed_epoch_id == epoch_id
    assert store.read_epoch(epoch_id).state is RuntimeEpochState.SEALED


def test_completion_failure_injection_rolls_back_children_parent_and_revision(
    m4_connection: Connection[tuple[object, ...]],
) -> None:
    store, epoch_id, root = _opened_store(m4_connection)
    store.start_attempt(epoch_id, _attempt(root))
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
            failure_injector=crash,
        )

    assert store.read_book() == before
    assert m4_connection.execute(
        "SELECT count(*) FROM groundloop_semantic_job_dependency WHERE epoch_id = %s",
        (epoch_id,),
    ).fetchone() == (0,)


def test_seal_failure_injection_preserves_complete_unsealed_epoch(
    m4_connection: Connection[tuple[object, ...]],
) -> None:
    store, epoch_id, root = _opened_store(m4_connection)
    store.start_attempt(epoch_id, _attempt(root))
    before_done = store.read_epoch(epoch_id)
    store.complete(
        CompletionPlan(
            epoch_id,
            before_done.revision,
            _completion(root, JobState.COMPLETED_ACTIVE),
        ),
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
            failure_injector=crash,
        )

    assert store.read_book() == before_seal
    assert store.read_epoch(epoch_id).state is RuntimeEpochState.SEMANTIC_COMPLETE


def test_failed_epoch_replay_conflict_and_late_inactive_completion(
    m4_connection: Connection[tuple[object, ...]],
) -> None:
    store, epoch_id, root = _opened_store(m4_connection)
    store.start_attempt(epoch_id, _attempt(root))
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
    store.complete(late, active_chunk_ids=frozenset())
    persisted = store.read_epoch(epoch_id)
    assert persisted.state is RuntimeEpochState.FAILED
    assert persisted.failure_reason == "operator stop"


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
        )
    with pytest.raises(EventConflictError, match="different declaration"):
        original = store.read_epoch(epoch_id)
        store.open_epoch(
            replace(original.update, payload_hash=_hash("conflict")),
            original.jobs[0:0],
            registry_snapshot_id="registry-1",
        )
