from __future__ import annotations

import hashlib

import pytest

from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.m4.contracts import (
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
)
from groundloop.m4.runtime.epoch import (
    CompletionPlan,
    EvaluationState,
    RuntimeBook,
    RuntimeEpoch,
    RuntimeEpochState,
    RuntimeJob,
    answer_evaluation_state,
    apply_completion,
    claim_evaluation_state,
    fail_epoch,
    mark_retryable_failure,
    mark_terminal_failure,
    open_epoch,
    seal_epoch,
    start_attempt,
)


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _update(
    event_id: str = "event-1", previous: int | None = None
) -> CorpusUpdateIdentity:
    return CorpusUpdateIdentity(
        event_id=event_id,
        payload_hash=_hash(f"payload:{event_id}"),
        update_kind=UpdateKind.REPLACE,
        previous_published_epoch_id=previous,
        candidate_policy_id="policy-1",
    )


def _spec(
    kind: JobKind,
    *,
    event_id: str = "event-1",
    parent: str | None = None,
    claim_id: str = "claim-1",
    chunk_id: str = "chunk-1",
    execution: str = "execution-1",
) -> LogicalJobSpec:
    execution_hash = _hash(execution)
    job_id = LogicalJobSpec.derive_job_id(
        event_id=event_id,
        kind=kind,
        candidate_policy_id="policy-1",
        execution_spec_hash=execution_hash,
        parent_job_id=parent or "",
        claim_id=claim_id if kind is not JobKind.IMPACT_DISCOVERY else "",
        chunk_version_id=chunk_id if kind is not JobKind.FRONTIER_RETRIEVE else "",
    )
    return LogicalJobSpec(
        job_id=job_id,
        event_id=event_id,
        kind=kind,
        candidate_policy_id="policy-1",
        payload_hash=_hash(f"payload:{job_id}"),
        execution_spec_hash=execution_hash,
        parent_job_id=parent,
        pair=PairKey(claim_id, chunk_id) if kind is JobKind.VERIFY_PAIR else None,
        target_claim_id=claim_id if kind is JobKind.FRONTIER_RETRIEVE else None,
        target_chunk_version_id=(
            chunk_id if kind is JobKind.IMPACT_DISCOVERY else None
        ),
        expandable=kind is not JobKind.VERIFY_PAIR,
    )


def _attempt(spec: LogicalJobSpec, ordinal: int = 1) -> JobAttempt:
    return JobAttempt(
        attempt_id=f"attempt-{spec.job_id[:8]}-{ordinal}",
        job_id=spec.job_id,
        execution_spec_hash=spec.execution_spec_hash,
        attempt_ordinal=ordinal,
        lease_token_hash=_hash(f"lease:{spec.job_id}:{ordinal}"),
    )


def _completion(
    spec: LogicalJobSpec,
    state: JobState,
    child_specs: tuple[LogicalJobSpec, ...] = (),
    result: str = "result-1",
) -> JobCompletion:
    result_hash = _hash(result)
    closure = (
        ChildClosure.build(
            parent_job_id=spec.job_id,
            result_artifact_hash=result_hash,
            child_job_ids=tuple(child.job_id for child in child_specs),
        )
        if spec.expandable
        else None
    )
    return JobCompletion.build(
        job_id=spec.job_id,
        payload_hash=spec.payload_hash,
        execution_spec_hash=spec.execution_spec_hash,
        result_artifact_id=f"artifact:{result}",
        result_artifact_hash=result_hash,
        terminal_state=state,
        child_closure=closure,
    )


def _epoch(book: RuntimeBook, epoch_id: int = 1) -> RuntimeEpoch:
    return next(epoch for epoch in book.epochs if epoch.epoch_id == epoch_id)


def _running_root(
    root: LogicalJobSpec,
    *,
    scope: DiscoveryScope | None = None,
) -> RuntimeBook:
    opened = open_epoch(
        RuntimeBook(),
        _update(),
        (root,),
        () if scope is None else (scope,),
    ).book
    return start_attempt(opened, 1, _attempt(root)).book


def test_expandable_completion_atomically_installs_children_and_closes_scope() -> None:
    root = _spec(JobKind.IMPACT_DISCOVERY)
    scope = DiscoveryScope(root.job_id, "registry-1", ("claim-1", "claim-2"))
    before = _running_root(root, scope=scope)
    child = _spec(JobKind.VERIFY_PAIR, parent=root.job_id)
    completion = _completion(root, JobState.COMPLETED_ACTIVE, (child,))
    plan = CompletionPlan(1, _epoch(before).revision, completion, (child,))

    after = apply_completion(
        before, plan, active_chunk_ids=frozenset({"chunk-1"})
    ).book
    epoch = _epoch(after)

    assert _epoch(before).revision == 2
    assert len(_epoch(before).jobs) == 1  # crash before apply changes nothing
    assert epoch.revision == 3
    assert {job.spec.job_id for job in epoch.jobs} == {root.job_id, child.job_id}
    assert epoch.discovery_scopes[0].closed
    assert epoch.state is RuntimeEpochState.SEMANTIC_PENDING
    assert claim_evaluation_state(epoch, "claim-1") is EvaluationState.PENDING
    assert claim_evaluation_state(epoch, "claim-2") is EvaluationState.COMPLETE


def test_empty_child_closure_completes_epoch_and_exact_replay_is_noop() -> None:
    root = _spec(JobKind.IMPACT_DISCOVERY)
    scope = DiscoveryScope(root.job_id, "registry-1", ("claim-1",))
    before = _running_root(root, scope=scope)
    completion = _completion(root, JobState.COMPLETED_ACTIVE)
    plan = CompletionPlan(1, _epoch(before).revision, completion)

    result = apply_completion(
        before, plan, active_chunk_ids=frozenset({"chunk-1"})
    )
    replay = apply_completion(
        result.book, plan, active_chunk_ids=frozenset({"chunk-1"})
    )

    assert _epoch(result.book).state is RuntimeEpochState.SEMANTIC_COMPLETE
    assert replay.replayed
    assert replay.book == result.book


def test_conflicting_child_set_and_result_are_rejected() -> None:
    root = _spec(JobKind.FRONTIER_RETRIEVE)
    before = _running_root(root)
    child = _spec(JobKind.VERIFY_PAIR, parent=root.job_id)
    first = _completion(root, JobState.COMPLETED_ACTIVE, (child,))
    applied = apply_completion(
        before,
        CompletionPlan(1, _epoch(before).revision, first, (child,)),
        active_chunk_ids=frozenset({"chunk-1"}),
    ).book
    other = _spec(
        JobKind.VERIFY_PAIR,
        parent=root.job_id,
        chunk_id="chunk-2",
    )
    conflicting = _completion(root, JobState.COMPLETED_ACTIVE, (other,), "other")
    with pytest.raises(EventConflictError):
        apply_completion(
            applied,
            CompletionPlan(1, 2, conflicting, (other,)),
            active_chunk_ids=frozenset({"chunk-1", "chunk-2"}),
        )


def test_child_completion_is_required_before_sealing_and_epoch_id_is_stable() -> None:
    root = _spec(JobKind.FRONTIER_RETRIEVE)
    before = _running_root(root)
    child = _spec(JobKind.VERIFY_PAIR, parent=root.job_id)
    parent_done = apply_completion(
        before,
        CompletionPlan(
            1,
            _epoch(before).revision,
            _completion(root, JobState.COMPLETED_ACTIVE, (child,)),
            (child,),
        ),
        active_chunk_ids=frozenset({"chunk-1"}),
    ).book
    with pytest.raises(InvalidEventError, match="semantically complete"):
        seal_epoch(parent_done, 1, _epoch(parent_done).revision)

    child_running = start_attempt(parent_done, 1, _attempt(child)).book
    child_done = apply_completion(
        child_running,
        CompletionPlan(
            1,
            _epoch(child_running).revision,
            _completion(child, JobState.COMPLETED_ACTIVE),
        ),
        active_chunk_ids=frozenset({"chunk-1"}),
    ).book
    sealed = seal_epoch(child_done, 1, _epoch(child_done).revision).book

    assert _epoch(sealed).epoch_id == 1
    assert _epoch(sealed).revision == 6
    assert _epoch(sealed).state is RuntimeEpochState.SEALED


def test_stale_completion_cas_changes_nothing() -> None:
    first = _spec(JobKind.VERIFY_PAIR)
    second = _spec(JobKind.VERIFY_PAIR, claim_id="claim-2")
    roots = tuple(sorted((first, second), key=lambda item: item.job_id))
    opened = open_epoch(RuntimeBook(), _update(), roots).book
    first_running = start_attempt(opened, 1, _attempt(first)).book
    stale_revision = _epoch(first_running).revision
    both_running = start_attempt(first_running, 1, _attempt(second)).book
    plan = CompletionPlan(
        1,
        stale_revision,
        _completion(first, JobState.COMPLETED_ACTIVE),
    )

    with pytest.raises(EventConflictError, match="stale"):
        apply_completion(
            both_running,
            plan,
            active_chunk_ids=frozenset({"chunk-1"}),
        )
    assert all(job.completion is None for job in _epoch(both_running).jobs)


def test_public_snapshot_records_cannot_bypass_closed_child_set() -> None:
    root = _spec(JobKind.IMPACT_DISCOVERY)
    child = _spec(JobKind.VERIFY_PAIR, parent=root.job_id)
    completion = _completion(root, JobState.COMPLETED_ACTIVE, (child,))
    completed_root = RuntimeJob(
        spec=root,
        state=JobState.COMPLETED_ACTIVE,
        attempts=(_attempt(root),),
        completion=completion,
    )
    closed_scope = DiscoveryScope(
        root.job_id, "registry-1", ("claim-1",), closed=True
    )

    with pytest.raises(ValidationError, match="child set"):
        RuntimeEpoch(
            epoch_id=1,
            update=_update(),
            state=RuntimeEpochState.SEMANTIC_COMPLETE,
            revision=3,
            jobs=(completed_root,),
            discovery_scopes=(closed_scope,),
        )


def test_child_scope_and_nonexpandable_closure_are_rejected() -> None:
    root = _spec(JobKind.IMPACT_DISCOVERY)
    scope = DiscoveryScope(root.job_id, "registry-1", ("claim-1",))
    before = _running_root(root, scope=scope)
    escaped = _spec(
        JobKind.VERIFY_PAIR,
        parent=root.job_id,
        chunk_id="wrong-chunk",
    )
    with pytest.raises(ValidationError, match="escaped chunk"):
        apply_completion(
            before,
            CompletionPlan(
                1,
                _epoch(before).revision,
                _completion(root, JobState.COMPLETED_ACTIVE, (escaped,)),
                (escaped,),
            ),
            active_chunk_ids=frozenset({"chunk-1", "wrong-chunk"}),
        )

    verify = _spec(JobKind.VERIFY_PAIR)
    running = _running_root(verify)
    child = _spec(JobKind.VERIFY_PAIR, parent=verify.job_id, chunk_id="chunk-2")
    closure = ChildClosure.build(
        parent_job_id=verify.job_id,
        result_artifact_hash=_hash("bad"),
        child_job_ids=(child.job_id,),
    )
    invalid = JobCompletion.build(
        job_id=verify.job_id,
        payload_hash=verify.payload_hash,
        execution_spec_hash=verify.execution_spec_hash,
        result_artifact_id="artifact:bad",
        result_artifact_hash=_hash("bad"),
        terminal_state=JobState.COMPLETED_ACTIVE,
        child_closure=closure,
    )
    with pytest.raises(ValidationError, match="non-expandable"):
        apply_completion(
            running,
            CompletionPlan(1, _epoch(running).revision, invalid, (child,)),
            active_chunk_ids=frozenset({"chunk-1", "chunk-2"}),
        )


def test_attempt_retry_identity_and_ordinal_are_strict() -> None:
    root = _spec(JobKind.VERIFY_PAIR)
    opened = open_epoch(RuntimeBook(), _update(), (root,)).book
    first = _attempt(root)
    running = start_attempt(opened, 1, first).book
    assert start_attempt(running, 1, first).replayed
    retryable = mark_retryable_failure(running, 1, root.job_id, first.attempt_id).book
    assert mark_retryable_failure(
        retryable, 1, root.job_id, first.attempt_id
    ).replayed
    with pytest.raises(InvalidEventError, match="ordinal"):
        start_attempt(retryable, 1, _attempt(root, 3))
    second = start_attempt(retryable, 1, _attempt(root, 2)).book
    assert _epoch(second).revision == 4


def test_failed_epoch_forbids_new_attempt_but_accepts_late_inactive_result() -> None:
    running_spec = _spec(JobKind.VERIFY_PAIR)
    waiting_spec = _spec(JobKind.VERIFY_PAIR, claim_id="claim-2")
    roots = tuple(
        sorted((running_spec, waiting_spec), key=lambda item: item.job_id)
    )
    opened = open_epoch(
        RuntimeBook(), _update(), roots
    ).book
    running = start_attempt(opened, 1, _attempt(running_spec)).book
    failed = fail_epoch(running, 1, _epoch(running).revision, "operator stop").book
    with pytest.raises(InvalidEventError, match="no new attempts"):
        start_attempt(failed, 1, _attempt(waiting_spec))

    completion = _completion(running_spec, JobState.COMPLETED_INACTIVE)
    late = apply_completion(
        failed,
        CompletionPlan(1, _epoch(failed).revision, completion),
        active_chunk_ids=frozenset(),
    ).book
    assert _epoch(late).state is RuntimeEpochState.FAILED
    completed_job = next(
        job for job in _epoch(late).jobs if job.spec.job_id == running_spec.job_id
    )
    assert completed_job.completion is not None
    assert claim_evaluation_state(_epoch(late), "claim-1") is EvaluationState.FAILED


def test_serial_epochs_sealing_and_event_replay() -> None:
    root = _spec(JobKind.VERIFY_PAIR)
    opened = open_epoch(RuntimeBook(), _update(), (root,)).book
    with pytest.raises(InvalidEventError, match="already active"):
        open_epoch(opened, _update("event-2"), ())
    running = start_attempt(opened, 1, _attempt(root)).book
    completed = apply_completion(
        running,
        CompletionPlan(
            1,
            _epoch(running).revision,
            _completion(root, JobState.COMPLETED_ACTIVE),
        ),
        active_chunk_ids=frozenset({"chunk-1"}),
    ).book
    sealed = seal_epoch(completed, 1, _epoch(completed).revision).book
    assert _epoch(sealed).state is RuntimeEpochState.SEALED
    assert sealed.last_sealed_epoch_id == 1
    assert open_epoch(sealed, _update(), (root,)).replayed
    second = open_epoch(sealed, _update("event-2", previous=1), ()).book
    assert _epoch(second, 2).state is RuntimeEpochState.SEMANTIC_COMPLETE


def test_terminal_failure_blocks_sealing_and_propagates_answer_failure() -> None:
    root = _spec(JobKind.VERIFY_PAIR)
    opened = open_epoch(RuntimeBook(), _update(), (root,)).book
    failed_job = mark_terminal_failure(opened, 1, root.job_id).book
    epoch = _epoch(failed_job)
    assert claim_evaluation_state(epoch, "claim-1") is EvaluationState.FAILED
    assert answer_evaluation_state(epoch, ("claim-1",)) is EvaluationState.FAILED
    with pytest.raises(InvalidEventError, match="semantically complete"):
        seal_epoch(failed_job, 1, epoch.revision)


def test_open_discovery_scope_makes_registry_pending_until_atomic_close() -> None:
    root = _spec(JobKind.IMPACT_DISCOVERY)
    scope = DiscoveryScope(root.job_id, "registry-1", ("claim-1", "claim-2"))
    running = _running_root(root, scope=scope)
    epoch = _epoch(running)
    assert (
        answer_evaluation_state(epoch, ("claim-1", "claim-2"))
        is EvaluationState.PENDING
    )
    completion = _completion(root, JobState.COMPLETED_ACTIVE)
    complete = apply_completion(
        running,
        CompletionPlan(1, epoch.revision, completion),
        active_chunk_ids=frozenset({"chunk-1"}),
    ).book
    assert answer_evaluation_state(
        _epoch(complete), ("claim-1", "claim-2")
    ) is EvaluationState.COMPLETE


def test_open_epoch_rejects_missing_scope_and_event_payload_conflict() -> None:
    impact = _spec(JobKind.IMPACT_DISCOVERY)
    with pytest.raises(ValidationError, match="exactly one scope"):
        open_epoch(RuntimeBook(), _update(), (impact,))
    verify = _spec(JobKind.VERIFY_PAIR)
    opened = open_epoch(RuntimeBook(), _update(), (verify,)).book
    changed = CorpusUpdateIdentity(
        event_id="event-1",
        payload_hash=_hash("different"),
        update_kind=UpdateKind.REPLACE,
        previous_published_epoch_id=None,
        candidate_policy_id="policy-1",
    )
    with pytest.raises(EventConflictError, match="event_id"):
        open_epoch(opened, changed, (verify,))
