"""Black-box acceptance tests for point-bounded M4 PostgreSQL coordination."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from psycopg import Connection, Cursor

from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
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


def _seed_base(
    connection: Connection[tuple[object, ...]], *, claim_count: int = 2
) -> int:
    sealed_epoch, _ = record_epoch(
        connection, event_id="point-base", payload_hash=_hash("point-base")
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
        "INSERT INTO groundloop_document_version VALUES "
        "('dv', 'doc', 'raw', %s, NULL)",
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
    for index in range(claim_count):
        claim_id = f"claim-{index + 1}"
        connection.execute(
            """
            INSERT INTO groundloop_claim VALUES
                (%s, 'answer', %s, 'extractor', 'revision', 'prompt', true)
            """,
            (claim_id, f"claim text {index + 1}"),
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


def _policy(claim_count: int) -> CandidatePolicyManifest:
    return CandidatePolicyManifest.build(
        policy_id="point-policy",
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
        claim_registry_snapshot_id="point-registry",
        claim_count=claim_count,
        fusion_version="interleave-v1",
        approximate_cap_per_inserted_chunk=4,
        frontier_depth=4,
        verifier_execution_spec_hash=_hash("verifier"),
        decision_policy_version="decision-v1",
    )


def _update(event_id: str, previous: int) -> CorpusUpdateIdentity:
    return CorpusUpdateIdentity(
        event_id=event_id,
        payload_hash=_hash(f"payload:{event_id}"),
        update_kind=UpdateKind.INSERT,
        previous_published_epoch_id=previous,
        candidate_policy_id="point-policy",
    )


def _job(
    kind: JobKind,
    *,
    event_id: str,
    claim_id: str = "claim-1",
    parent: str | None = None,
) -> LogicalJobSpec:
    execution_hash = _hash(f"execution:{kind.value}")
    target_claim = claim_id if kind is not JobKind.IMPACT_DISCOVERY else ""
    target_chunk = "chunk-1" if kind is not JobKind.FRONTIER_RETRIEVE else ""
    job_id = LogicalJobSpec.derive_job_id(
        event_id=event_id,
        kind=kind,
        candidate_policy_id="point-policy",
        execution_spec_hash=execution_hash,
        parent_job_id=parent or "",
        claim_id=target_claim,
        chunk_version_id=target_chunk,
    )
    return LogicalJobSpec(
        job_id=job_id,
        event_id=event_id,
        kind=kind,
        candidate_policy_id="point-policy",
        payload_hash=_hash(f"job:{job_id}"),
        execution_spec_hash=execution_hash,
        parent_job_id=parent,
        pair=(
            PairKey(claim_id, "chunk-1") if kind is JobKind.VERIFY_PAIR else None
        ),
        target_claim_id=claim_id if kind is JobKind.FRONTIER_RETRIEVE else None,
        target_chunk_version_id=(
            "chunk-1" if kind is JobKind.IMPACT_DISCOVERY else None
        ),
        expandable=kind is not JobKind.VERIFY_PAIR,
    )


def _attempt(spec: LogicalJobSpec, ordinal: int = 1) -> JobAttempt:
    return JobAttempt(
        attempt_id=f"attempt-{spec.job_id[:16]}-{ordinal}",
        job_id=spec.job_id,
        execution_spec_hash=spec.execution_spec_hash,
        attempt_ordinal=ordinal,
        lease_token_hash=_hash(f"lease:{spec.job_id}:{ordinal}"),
    )


def _completion(
    spec: LogicalJobSpec,
    state: JobState,
    children: tuple[LogicalJobSpec, ...] = (),
    *,
    result_suffix: str = "",
) -> JobCompletion:
    result_hash = _hash(f"result:{spec.job_id}:{state.value}:{result_suffix}")
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
        result_artifact_id=f"artifact-{spec.job_id[:12]}-{result_suffix or 'v1'}",
        result_artifact_hash=result_hash,
        terminal_state=state,
        child_closure=closure,
    )


def _forbid_full_reads(store: PostgresM4RuntimeStore, monkeypatch: Any) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("point runtime invoked a full runtime read")

    monkeypatch.setattr(store, "read_epoch", forbidden)
    monkeypatch.setattr(store, "read_book", forbidden)


def test_multi_child_completion_replay_and_stale_lease_are_point_bounded(
    point_connection: Connection[tuple[object, ...]], monkeypatch: Any
) -> None:
    base = _seed_base(point_connection)
    audit = PostgresM4RuntimeStore(point_connection)
    audit.register_candidate_policy(_policy(2))
    root = _job(JobKind.IMPACT_DISCOVERY, event_id="multi")
    scope = DiscoveryScope(
        root_job_id=root.job_id,
        registry_snapshot_id="point-registry",
        registered_claim_ids=("claim-1", "claim-2"),
    )
    opened = audit.open_epoch(
        _update("multi", base),
        (root,),
        (scope,),
        registry_snapshot_id="point-registry",
        structural_action=_no_structural,
    )
    store = PostgresM4RuntimeStore(point_connection, audit_transitions=False)
    _forbid_full_reads(store, monkeypatch)

    header = store.read_epoch_header_point(opened.epoch.epoch_id)
    assert (header.open_job_count, header.open_scope_count) == (1, 1)
    attempt = _attempt(root)
    started = store.start_attempt_point(
        header.epoch_id,
        header.revision,
        attempt,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=2),
    )
    child_one = _job(
        JobKind.VERIFY_PAIR,
        event_id="multi",
        claim_id="claim-1",
        parent=root.job_id,
    )
    child_two = _job(
        JobKind.VERIFY_PAIR,
        event_id="multi",
        claim_id="claim-2",
        parent=root.job_id,
    )
    children = tuple(sorted((child_one, child_two), key=lambda item: item.job_id))
    completion = _completion(root, JobState.COMPLETED_ACTIVE, children)
    plan = CompletionPlan(
        expected_epoch_id=header.epoch_id,
        expected_revision=started.header.revision,
        completion=completion,
        child_jobs=children,
    )

    def fail_after_parent(point: str) -> None:
        if point == "point_completion_parent_written":
            raise RuntimeError("injected completion failure")

    with pytest.raises(RuntimeError, match="injected completion failure"):
        store.complete_point(
            plan,
            attempt_id=attempt.attempt_id,
            lease_token_hash=attempt.lease_token_hash,
            lease_expected_revision=started.header.revision,
            failure_injector=fail_after_parent,
        )
    assert store.read_epoch_header_point(header.epoch_id) == started.header
    assert store.read_children_point(header.epoch_id, root.job_id) == ()
    assert store.read_job_point(header.epoch_id, root.job_id).state is JobState.RUNNING

    completed = store.complete_point(
        plan,
        attempt_id=attempt.attempt_id,
        lease_token_hash=attempt.lease_token_hash,
        lease_expected_revision=started.header.revision,
    )
    assert not completed.replayed
    assert completed.header.state is RuntimeEpochState.SEMANTIC_PENDING
    assert (completed.header.open_job_count, completed.header.open_scope_count) == (
        2,
        0,
    )
    assert store.read_children_point(header.epoch_id, root.job_id) == children

    replay = store.complete_point(
        plan,
        attempt_id=attempt.attempt_id,
        lease_token_hash=attempt.lease_token_hash,
        lease_expected_revision=started.header.revision,
    )
    assert replay.replayed
    assert replay.header == completed.header

    conflicting = CompletionPlan(
        expected_epoch_id=header.epoch_id,
        expected_revision=completed.header.revision,
        completion=_completion(
            root,
            JobState.COMPLETED_ACTIVE,
            children,
            result_suffix="conflict",
        ),
        child_jobs=children,
    )
    with pytest.raises(EventConflictError):
        store.complete_point(
            conflicting,
            attempt_id=attempt.attempt_id,
            lease_token_hash=attempt.lease_token_hash,
            lease_expected_revision=started.header.revision,
        )

    target = children[0]
    first = _attempt(target)
    first_start = store.start_attempt_point(
        header.epoch_id, completed.header.revision, first
    )
    failed = store.mark_retryable_failure_point(
        header.epoch_id,
        first_start.header.revision,
        target.job_id,
        first.attempt_id,
    )
    second = _attempt(target, ordinal=2)
    second_start = store.start_attempt_point(
        header.epoch_id, failed.header.revision, second
    )
    target_plan = CompletionPlan(
        expected_epoch_id=header.epoch_id,
        expected_revision=second_start.header.revision,
        completion=_completion(target, JobState.COMPLETED_ACTIVE),
    )
    with pytest.raises(EventConflictError, match="stale attempt"):
        store.complete_point(
            target_plan,
            attempt_id=first.attempt_id,
            lease_token_hash=first.lease_token_hash,
            lease_expected_revision=first_start.header.revision,
        )
    assert store.read_epoch_header_point(header.epoch_id) == second_start.header


def test_registry_build_is_immutable_and_enables_compact_measured_scope(
    point_connection: Connection[tuple[object, ...]],
) -> None:
    base = _seed_base(point_connection)
    audit = PostgresM4RuntimeStore(point_connection)
    audit.register_candidate_policy(_policy(2))
    assert audit.register_claim_registry_snapshot(
        "point-registry", ("claim-1", "claim-2")
    )
    assert not audit.register_claim_registry_snapshot(
        "point-registry", ("claim-1", "claim-2")
    )
    with pytest.raises(EventConflictError):
        audit.register_claim_registry_snapshot("point-registry", ("claim-1",))
    with pytest.raises(ValidationError):
        audit.register_claim_registry_snapshot(
            "another-registry", ("claim-2", "claim-1")
        )

    root = _job(JobKind.IMPACT_DISCOVERY, event_id="compact")
    compact_scope = DiscoveryScope(
        root_job_id=root.job_id,
        registry_snapshot_id="point-registry",
        registered_claim_ids=(),
    )
    with pytest.raises(ValidationError, match="all-claims scope"):
        audit.open_epoch(
            _update("compact", base),
            (root,),
            (compact_scope,),
            registry_snapshot_id="point-registry",
            structural_action=_no_structural,
        )

    measured = PostgresM4RuntimeStore(point_connection, audit_transitions=False)
    opened = measured.open_epoch(
        _update("compact", base),
        (root,),
        (compact_scope,),
        registry_snapshot_id="point-registry",
        structural_action=_no_structural,
    )
    assert opened.epoch.discovery_scopes == (compact_scope,)
    assert measured.read_epoch_header_point(opened.epoch.epoch_id).open_scope_count == 1


def test_failed_epoch_accepts_only_late_inactive_completion(
    point_connection: Connection[tuple[object, ...]], monkeypatch: Any
) -> None:
    base = _seed_base(point_connection)
    audit = PostgresM4RuntimeStore(point_connection)
    audit.register_candidate_policy(_policy(2))
    root = _job(JobKind.IMPACT_DISCOVERY, event_id="late")
    scope = DiscoveryScope(
        root_job_id=root.job_id,
        registry_snapshot_id="point-registry",
        registered_claim_ids=("claim-1", "claim-2"),
    )
    opened = audit.open_epoch(
        _update("late", base),
        (root,),
        (scope,),
        registry_snapshot_id="point-registry",
        structural_action=_no_structural,
    )
    store = PostgresM4RuntimeStore(point_connection, audit_transitions=False)
    _forbid_full_reads(store, monkeypatch)
    attempt = _attempt(root)
    started = store.start_attempt_point(
        opened.epoch.epoch_id, opened.epoch.revision, attempt
    )
    failed = store.fail_epoch_point(
        opened.epoch.epoch_id, started.header.revision, "worker deadline"
    )
    assert failed.header.state is RuntimeEpochState.FAILED
    with pytest.raises(InvalidEventError):
        store.start_attempt_point(
            opened.epoch.epoch_id,
            failed.header.revision,
            JobAttempt(
                attempt_id="new-attempt",
                job_id=root.job_id,
                execution_spec_hash=root.execution_spec_hash,
                attempt_ordinal=2,
                lease_token_hash=_hash("new-lease"),
            ),
        )

    completion = _completion(root, JobState.COMPLETED_INACTIVE)
    plan = CompletionPlan(
        expected_epoch_id=opened.epoch.epoch_id,
        expected_revision=failed.header.revision,
        completion=completion,
    )
    late = store.complete_point(
        plan,
        attempt_id=attempt.attempt_id,
        lease_token_hash=attempt.lease_token_hash,
        lease_expected_revision=started.header.revision,
    )
    assert late.header.state is RuntimeEpochState.FAILED
    assert (late.header.open_job_count, late.header.open_scope_count) == (0, 0)
    assert not late.header.seal_ready
    assert store.complete_point(
        plan,
        attempt_id=attempt.attempt_id,
        lease_token_hash=attempt.lease_token_hash,
        lease_expected_revision=started.header.revision,
    ).replayed
    with pytest.raises(InvalidEventError):
        store.seal_epoch_point(
            late.header.epoch_id,
            late.header.revision,
            publication_action=_advance_head,
        )


def test_point_seal_uses_counter_header_without_aggregate_scans(
    point_connection: Connection[tuple[object, ...]], monkeypatch: Any
) -> None:
    base = _seed_base(point_connection)
    audit = PostgresM4RuntimeStore(point_connection)
    audit.register_candidate_policy(_policy(2))
    root = _job(JobKind.VERIFY_PAIR, event_id="seal")
    opened = audit.open_epoch(
        _update("seal", base),
        (root,),
        registry_snapshot_id="point-registry",
        structural_action=_no_structural,
    )
    store = PostgresM4RuntimeStore(point_connection, audit_transitions=False)
    _forbid_full_reads(store, monkeypatch)
    attempt = _attempt(root)
    started = store.start_attempt_point(
        opened.epoch.epoch_id, opened.epoch.revision, attempt
    )
    completed = store.complete_point(
        CompletionPlan(
            expected_epoch_id=opened.epoch.epoch_id,
            expected_revision=started.header.revision,
            completion=_completion(root, JobState.COMPLETED_ACTIVE),
        ),
        attempt_id=attempt.attempt_id,
        lease_token_hash=attempt.lease_token_hash,
        lease_expected_revision=started.header.revision,
    )
    assert completed.header.seal_ready
    sealed = store.seal_epoch_point(
        completed.header.epoch_id,
        completed.header.revision,
        publication_action=_advance_head,
    )
    assert sealed.header.state is RuntimeEpochState.SEALED
    assert store.seal_epoch_point(
        sealed.header.epoch_id,
        sealed.header.revision,
        publication_action=_advance_head,
    ).replayed


def test_counter_contract_preserves_the_full_audit_transition_path(
    point_connection: Connection[tuple[object, ...]],
) -> None:
    base = _seed_base(point_connection)
    audit = PostgresM4RuntimeStore(point_connection)
    audit.register_candidate_policy(_policy(2))
    root = _job(JobKind.VERIFY_PAIR, event_id="audit-compatible")
    opened = audit.open_epoch(
        _update("audit-compatible", base),
        (root,),
        registry_snapshot_id="point-registry",
        structural_action=_no_structural,
    )
    attempt = _attempt(root)
    started = audit.start_attempt(opened.epoch.epoch_id, attempt)
    started_epoch = next(
        epoch
        for epoch in started.book.epochs
        if epoch.epoch_id == opened.epoch.epoch_id
    )
    audit.complete(
        CompletionPlan(
            expected_epoch_id=opened.epoch.epoch_id,
            expected_revision=started_epoch.revision,
            completion=_completion(root, JobState.COMPLETED_ACTIVE),
        ),
        active_chunk_ids=frozenset({"chunk-1"}),
        attempt_id=attempt.attempt_id,
        lease_token_hash=attempt.lease_token_hash,
        lease_expected_revision=started_epoch.revision,
    )
    header = audit.read_epoch_header_point(opened.epoch.epoch_id)
    assert header.seal_ready
    assert (header.open_job_count, header.open_scope_count) == (0, 0)


class _CountingConnection:
    def __init__(self, connection: Connection[tuple[object, ...]]) -> None:
        self.connection = connection
        self.query_count = 0

    def execute(self, query: object, params: object = None) -> Any:
        self.query_count += 1
        return self.connection.execute(cast(Any, query), cast(Any, params))

    def transaction(self) -> Any:
        return self.connection.transaction()

    def cursor(self) -> Any:
        return self.connection.cursor()

    def reset(self) -> None:
        self.query_count = 0


def test_point_sql_statement_count_is_independent_of_unrelated_job_scale(
    point_connection: Connection[tuple[object, ...]], monkeypatch: Any
) -> None:
    base = _seed_base(point_connection, claim_count=128)
    audit = PostgresM4RuntimeStore(point_connection)
    audit.register_candidate_policy(_policy(128))
    counted = _CountingConnection(point_connection)
    store = PostgresM4RuntimeStore(cast(Any, counted), audit_transitions=False)
    _forbid_full_reads(store, monkeypatch)

    results: list[tuple[int, int]] = []
    for scale in (2, 128):
        event_id = f"scale-{scale}"
        roots = tuple(
            sorted(
                (
                    _job(
                        JobKind.VERIFY_PAIR,
                        event_id=event_id,
                        claim_id=f"claim-{index + 1}",
                    )
                    for index in range(scale)
                ),
                key=lambda item: item.job_id,
            )
        )
        opened = audit.open_epoch(
            _update(event_id, base),
            roots,
            registry_snapshot_id="point-registry",
            structural_action=_no_structural,
        )
        target = roots[0]
        attempt = _attempt(target)
        counted.reset()
        started = store.start_attempt_point(
            opened.epoch.epoch_id, opened.epoch.revision, attempt
        )
        start_queries = counted.query_count
        counted.reset()
        completed = store.complete_point(
            CompletionPlan(
                expected_epoch_id=opened.epoch.epoch_id,
                expected_revision=started.header.revision,
                completion=_completion(target, JobState.COMPLETED_ACTIVE),
            ),
            attempt_id=attempt.attempt_id,
            lease_token_hash=attempt.lease_token_hash,
            lease_expected_revision=started.header.revision,
        )
        completion_queries = counted.query_count
        results.append((start_queries, completion_queries))
        store.fail_epoch_point(
            opened.epoch.epoch_id,
            completed.header.revision,
            f"close scale {scale}",
        )

    assert results == [(9, 9), (9, 9)]
