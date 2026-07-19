"""Immutable M4 epoch and dynamic-job transition system.

The functions in this module are deliberately persistence-free.  Each accepted
microtransaction returns a new snapshot; rejected transitions leave the input
snapshot untouched.  A PostgreSQL adapter can therefore implement the same
contract with one compare-and-swap transaction on ``revision``.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum

from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.m4.contracts import (
    CorpusUpdateIdentity,
    DiscoveryScope,
    JobAttempt,
    JobCompletion,
    JobKind,
    JobState,
    LogicalJobSpec,
    stable_m4_digest,
)


class RuntimeEpochState(StrEnum):
    """Runtime states needed by strict M4 CORE publication."""

    SEMANTIC_PENDING = "semantic_pending"
    SEMANTIC_COMPLETE = "semantic_complete"
    SEALED = "sealed"
    FAILED = "failed"


class EvaluationState(StrEnum):
    """Provisional evaluation visibility for claims and answers."""

    COMPLETE = "complete"
    PENDING = "pending"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class RuntimeJob:
    spec: LogicalJobSpec
    state: JobState = JobState.DECLARED
    attempts: tuple[JobAttempt, ...] = ()
    completion: JobCompletion | None = None

    def __post_init__(self) -> None:
        attempt_ids: set[str] = set()
        for ordinal, attempt in enumerate(self.attempts, start=1):
            if attempt.attempt_id in attempt_ids:
                raise ValidationError("runtime attempt IDs must be unique")
            attempt_ids.add(attempt.attempt_id)
            if attempt.job_id != self.spec.job_id:
                raise ValidationError("runtime attempt belongs to another job")
            if attempt.execution_spec_hash != self.spec.execution_spec_hash:
                raise ValidationError("runtime attempt execution identity differs")
            if attempt.attempt_ordinal != ordinal:
                raise ValidationError("runtime attempt ordinals must be contiguous")
        if self.state is JobState.RUNNING and not self.attempts:
            raise ValidationError("running job requires an attempt")
        if self.state is JobState.RETRYABLE_FAILED and not self.attempts:
            raise ValidationError("retryable failure requires an attempt")
        completed_states = {
            JobState.COMPLETED_ACTIVE,
            JobState.COMPLETED_INACTIVE,
        }
        if (self.completion is None) != (self.state not in completed_states):
            raise ValidationError("runtime completion and job state disagree")
        if self.completion is not None:
            if not self.attempts:
                raise ValidationError("completed job requires an attempt")
            if self.completion.job_id != self.spec.job_id:
                raise ValidationError("runtime completion belongs to another job")
            if self.completion.terminal_state is not self.state:
                raise ValidationError("runtime completion terminal state differs")
            if self.completion.payload_hash != self.spec.payload_hash:
                raise ValidationError("runtime completion payload differs")
            if self.completion.execution_spec_hash != self.spec.execution_spec_hash:
                raise ValidationError("runtime completion execution identity differs")
            if self.spec.expandable != (self.completion.child_closure is not None):
                raise ValidationError("runtime completion closure shape differs")

    @property
    def open(self) -> bool:
        return not self.state.terminal


@dataclass(frozen=True, slots=True)
class RuntimeEpoch:
    epoch_id: int
    update: CorpusUpdateIdentity
    state: RuntimeEpochState
    revision: int
    jobs: tuple[RuntimeJob, ...]
    discovery_scopes: tuple[DiscoveryScope, ...] = ()
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        if self.epoch_id <= 0:
            raise ValidationError("epoch_id must be positive")
        if self.revision <= 0:
            raise ValidationError("revision must be positive")
        job_ids = tuple(job.spec.job_id for job in self.jobs)
        if job_ids != tuple(sorted(set(job_ids))):
            raise ValidationError("runtime jobs must be sorted and unique")
        scope_ids = tuple(scope.root_job_id for scope in self.discovery_scopes)
        if scope_ids != tuple(sorted(set(scope_ids))):
            raise ValidationError("discovery scopes must be sorted and unique")
        by_id = {job.spec.job_id: job for job in self.jobs}
        for job in self.jobs:
            if job.spec.event_id != self.update.event_id:
                raise ValidationError("runtime job belongs to another event")
            if job.spec.candidate_policy_id != self.update.candidate_policy_id:
                raise ValidationError("runtime job belongs to another policy")
            parent_id = job.spec.parent_job_id
            if parent_id is None:
                continue
            parent = by_id.get(parent_id)
            if parent is None:
                raise ValidationError("runtime child references an unknown parent")
            if parent.spec.parent_job_id is not None or not parent.spec.expandable:
                raise ValidationError("runtime graph exceeds one expansion level")
        for job in self.jobs:
            if job.completion is None or not job.spec.expandable:
                continue
            closure = job.completion.child_closure
            if closure is None:  # RuntimeJob validates this; narrows the type.
                raise ValidationError("expandable completion requires a closure")
            actual_children = tuple(
                child.spec.job_id
                for child in self.jobs
                if child.spec.parent_job_id == job.spec.job_id
            )
            if closure.child_job_ids != actual_children:
                raise ValidationError("runtime child set differs from parent closure")
        impact_roots = {
            job.spec.job_id
            for job in self.jobs
            if job.spec.parent_job_id is None
            and job.spec.kind is JobKind.IMPACT_DISCOVERY
        }
        if {scope.root_job_id for scope in self.discovery_scopes} != impact_roots:
            raise ValidationError("runtime impact roots and discovery scopes differ")
        successful = {JobState.COMPLETED_ACTIVE, JobState.COMPLETED_INACTIVE}
        semantically_complete = all(
            job.state in successful for job in self.jobs
        ) and all(scope.closed for scope in self.discovery_scopes)
        if self.state in {
            RuntimeEpochState.SEMANTIC_COMPLETE,
            RuntimeEpochState.SEALED,
        } and not semantically_complete:
            raise ValidationError("complete epoch contains open semantic work")
        if self.state is RuntimeEpochState.SEMANTIC_PENDING and semantically_complete:
            raise ValidationError("pending epoch contains no open semantic work")
        if self.state is RuntimeEpochState.FAILED:
            if self.failure_reason is None or not self.failure_reason.strip():
                raise ValidationError("failed epoch requires a failure reason")
        elif self.failure_reason is not None:
            raise ValidationError("non-failed epoch cannot have a failure reason")


@dataclass(frozen=True, slots=True)
class RuntimeBook:
    """All runtime epochs, including immutable failed and sealed history."""

    next_epoch_id: int = 1
    epochs: tuple[RuntimeEpoch, ...] = ()
    active_epoch_id: int | None = None
    last_sealed_epoch_id: int | None = None

    def __post_init__(self) -> None:
        epoch_ids = tuple(epoch.epoch_id for epoch in self.epochs)
        if epoch_ids != tuple(sorted(set(epoch_ids))):
            raise ValidationError("runtime epochs must be sorted and unique")
        expected_next = (epoch_ids[-1] + 1) if epoch_ids else 1
        if self.next_epoch_id != expected_next:
            raise ValidationError("next_epoch_id must follow immutable history")
        by_id = {epoch.epoch_id: epoch for epoch in self.epochs}
        if self.active_epoch_id is not None:
            active = by_id.get(self.active_epoch_id)
            if active is None or active.state not in {
                RuntimeEpochState.SEMANTIC_PENDING,
                RuntimeEpochState.SEMANTIC_COMPLETE,
            }:
                raise ValidationError("active_epoch_id does not name an open epoch")
        if self.last_sealed_epoch_id is not None:
            sealed = by_id.get(self.last_sealed_epoch_id)
            if sealed is None or sealed.state is not RuntimeEpochState.SEALED:
                raise ValidationError("last_sealed_epoch_id does not name a seal")


@dataclass(frozen=True, slots=True)
class TransitionResult:
    book: RuntimeBook
    replayed: bool


@dataclass(frozen=True, slots=True)
class CompletionPlan:
    """CAS input for one atomic job-completion microtransaction."""

    expected_epoch_id: int
    expected_revision: int
    completion: JobCompletion
    child_jobs: tuple[LogicalJobSpec, ...] = ()

    def __post_init__(self) -> None:
        if self.expected_epoch_id <= 0 or self.expected_revision <= 0:
            raise ValidationError("completion CAS identifiers must be positive")
        child_ids = tuple(child.job_id for child in self.child_jobs)
        if child_ids != tuple(sorted(set(child_ids))):
            raise ValidationError("completion child jobs must be sorted and unique")


def _epoch(book: RuntimeBook, epoch_id: int) -> RuntimeEpoch:
    for epoch in book.epochs:
        if epoch.epoch_id == epoch_id:
            return epoch
    raise InvalidEventError(f"unknown epoch_id: {epoch_id}")


def _job(epoch: RuntimeEpoch, job_id: str) -> RuntimeJob:
    for job in epoch.jobs:
        if job.spec.job_id == job_id:
            return job
    raise InvalidEventError(f"unknown job_id in epoch: {job_id}")


def _replace_epoch(book: RuntimeBook, changed: RuntimeEpoch) -> RuntimeBook:
    epochs = tuple(
        changed if epoch.epoch_id == changed.epoch_id else epoch
        for epoch in book.epochs
    )
    return replace(book, epochs=epochs)


def _replace_job(epoch: RuntimeEpoch, changed: RuntimeJob) -> RuntimeEpoch:
    jobs = tuple(
        changed if job.spec.job_id == changed.spec.job_id else job
        for job in epoch.jobs
    )
    return replace(epoch, jobs=jobs)


def _semantic_state(epoch: RuntimeEpoch) -> RuntimeEpochState:
    if epoch.state in {RuntimeEpochState.FAILED, RuntimeEpochState.SEALED}:
        return epoch.state
    successful = {JobState.COMPLETED_ACTIVE, JobState.COMPLETED_INACTIVE}
    all_jobs_complete = all(job.state in successful for job in epoch.jobs)
    all_scopes_closed = all(scope.closed for scope in epoch.discovery_scopes)
    return (
        RuntimeEpochState.SEMANTIC_COMPLETE
        if all_jobs_complete and all_scopes_closed
        else RuntimeEpochState.SEMANTIC_PENDING
    )


def _with_revision(epoch: RuntimeEpoch) -> RuntimeEpoch:
    return replace(
        epoch,
        revision=epoch.revision + 1,
        state=_semantic_state(epoch),
    )


def open_epoch(
    book: RuntimeBook,
    update: CorpusUpdateIdentity,
    root_jobs: tuple[LogicalJobSpec, ...],
    discovery_scopes: tuple[DiscoveryScope, ...] = (),
) -> TransitionResult:
    """Structurally open one serialized epoch, or replay its exact declaration."""
    canonical_jobs = tuple(sorted(root_jobs, key=lambda item: item.job_id))
    canonical_scopes = tuple(
        sorted(discovery_scopes, key=lambda item: item.root_job_id)
    )
    for existing in book.epochs:
        if existing.update.event_id != update.event_id:
            continue
        declared_roots = tuple(
            job.spec for job in existing.jobs if job.spec.parent_job_id is None
        )
        declared_scopes = tuple(
            replace(scope, closed=False) for scope in existing.discovery_scopes
        )
        if (
            existing.update == update
            and declared_roots == canonical_jobs
            and declared_scopes == canonical_scopes
        ):
            return TransitionResult(book, replayed=True)
        raise EventConflictError("event_id was reused with a different declaration")
    if book.active_epoch_id is not None:
        raise InvalidEventError("a structural epoch is already active")
    if update.previous_published_epoch_id != book.last_sealed_epoch_id:
        raise InvalidEventError("update does not name the last sealed epoch")
    if len({spec.job_id for spec in canonical_jobs}) != len(canonical_jobs):
        raise ValidationError("root job IDs must be unique")
    for spec in canonical_jobs:
        if spec.parent_job_id is not None:
            raise ValidationError("root jobs cannot name a parent")
        if spec.event_id != update.event_id:
            raise ValidationError("root job event does not match epoch event")
        if spec.candidate_policy_id != update.candidate_policy_id:
            raise ValidationError("root job policy does not match epoch policy")
    by_id = {spec.job_id: spec for spec in canonical_jobs}
    if len({scope.root_job_id for scope in canonical_scopes}) != len(
        canonical_scopes
    ):
        raise ValidationError("discovery-scope roots must be unique")
    impact_roots = {
        spec.job_id for spec in canonical_jobs if spec.kind is JobKind.IMPACT_DISCOVERY
    }
    scope_roots = {scope.root_job_id for scope in canonical_scopes}
    if scope_roots != impact_roots:
        raise ValidationError("each impact-discovery root requires exactly one scope")
    for scope in canonical_scopes:
        if scope.closed:
            raise ValidationError("a new discovery scope must be open")
        if scope.root_job_id not in by_id:
            raise ValidationError("discovery scope references an unknown root")
    jobs = tuple(RuntimeJob(spec=spec) for spec in canonical_jobs)
    initial_state = (
        RuntimeEpochState.SEMANTIC_COMPLETE
        if not jobs and all(scope.closed for scope in canonical_scopes)
        else RuntimeEpochState.SEMANTIC_PENDING
    )
    epoch = RuntimeEpoch(
        epoch_id=book.next_epoch_id,
        update=update,
        state=initial_state,
        revision=1,
        jobs=jobs,
        discovery_scopes=canonical_scopes,
    )
    changed = replace(
        book,
        next_epoch_id=book.next_epoch_id + 1,
        epochs=(*book.epochs, epoch),
        active_epoch_id=epoch.epoch_id,
    )
    return TransitionResult(changed, replayed=False)


def start_attempt(
    book: RuntimeBook, epoch_id: int, attempt: JobAttempt
) -> TransitionResult:
    """Start an immutable attempt under the job's frozen execution identity."""
    epoch = _epoch(book, epoch_id)
    job = _job(epoch, attempt.job_id)
    for known_job in epoch.jobs:
        for known in known_job.attempts:
            if known.attempt_id == attempt.attempt_id:
                if known == attempt:
                    return TransitionResult(book, replayed=True)
                raise EventConflictError("attempt_id was reused with different content")
    if epoch.state in {RuntimeEpochState.FAILED, RuntimeEpochState.SEALED}:
        raise InvalidEventError("failed or sealed epochs start no new attempts")
    if job.state not in {JobState.DECLARED, JobState.RETRYABLE_FAILED}:
        raise InvalidEventError("job is not eligible to start an attempt")
    if attempt.execution_spec_hash != job.spec.execution_spec_hash:
        raise EventConflictError("attempt execution identity differs from job")
    if attempt.attempt_ordinal != len(job.attempts) + 1:
        raise InvalidEventError("attempt ordinal is not the next ordinal")
    changed_job = replace(
        job,
        state=JobState.RUNNING,
        attempts=(*job.attempts, attempt),
    )
    changed_epoch = _with_revision(_replace_job(epoch, changed_job))
    return TransitionResult(_replace_epoch(book, changed_epoch), replayed=False)


def mark_retryable_failure(
    book: RuntimeBook, epoch_id: int, job_id: str, attempt_id: str
) -> TransitionResult:
    epoch = _epoch(book, epoch_id)
    job = _job(epoch, job_id)
    if job.state is JobState.RETRYABLE_FAILED:
        if job.attempts and job.attempts[-1].attempt_id == attempt_id:
            return TransitionResult(book, replayed=True)
        raise EventConflictError("failure does not name the failed attempt")
    if epoch.state in {RuntimeEpochState.FAILED, RuntimeEpochState.SEALED}:
        raise InvalidEventError("epoch cannot accept a retryable failure")
    if job.state is not JobState.RUNNING or not job.attempts:
        raise InvalidEventError("only a running job can fail retryably")
    if job.attempts[-1].attempt_id != attempt_id:
        raise EventConflictError("failure does not name the active attempt")
    changed = _with_revision(
        _replace_job(epoch, replace(job, state=JobState.RETRYABLE_FAILED))
    )
    return TransitionResult(_replace_epoch(book, changed), replayed=False)


def _terminalize_job(
    book: RuntimeBook,
    epoch_id: int,
    job_id: str,
    terminal_state: JobState,
) -> TransitionResult:
    epoch = _epoch(book, epoch_id)
    job = _job(epoch, job_id)
    if job.state is terminal_state:
        return TransitionResult(book, replayed=True)
    if epoch.state in {RuntimeEpochState.FAILED, RuntimeEpochState.SEALED}:
        raise InvalidEventError("epoch cannot terminalize a job")
    if job.state.terminal:
        raise EventConflictError("job already has a different terminal state")
    changed = _with_revision(_replace_job(epoch, replace(job, state=terminal_state)))
    return TransitionResult(_replace_epoch(book, changed), replayed=False)


def mark_terminal_failure(
    book: RuntimeBook, epoch_id: int, job_id: str
) -> TransitionResult:
    return _terminalize_job(book, epoch_id, job_id, JobState.TERMINAL_FAILED)


def cancel_job(book: RuntimeBook, epoch_id: int, job_id: str) -> TransitionResult:
    return _terminalize_job(book, epoch_id, job_id, JobState.CANCELLED)


def _validate_completion(
    epoch: RuntimeEpoch,
    job: RuntimeJob,
    plan: CompletionPlan,
    active_chunk_ids: frozenset[str],
) -> None:
    completion = plan.completion
    spec = job.spec
    if completion.job_id != spec.job_id:
        raise EventConflictError("completion belongs to another job")
    if completion.payload_hash != spec.payload_hash:
        raise EventConflictError("completion payload differs from job payload")
    if completion.execution_spec_hash != spec.execution_spec_hash:
        raise EventConflictError("completion execution identity differs from job")
    target_chunk_id = (
        spec.pair.chunk_version_id
        if spec.pair is not None
        else spec.target_chunk_version_id
    )
    expected_state = (
        JobState.COMPLETED_INACTIVE
        if target_chunk_id is not None and target_chunk_id not in active_chunk_ids
        else JobState.COMPLETED_ACTIVE
    )
    if completion.terminal_state is not expected_state:
        raise InvalidEventError("completion state disagrees with target activity")
    child_ids = tuple(child.job_id for child in plan.child_jobs)
    closure = completion.child_closure
    if not spec.expandable:
        if closure is not None or plan.child_jobs:
            raise ValidationError("non-expandable completion cannot declare children")
        return
    if closure is None:
        raise ValidationError("expandable completion requires explicit child closure")
    if closure.child_job_ids != child_ids:
        raise EventConflictError("child closure and declared child set differ")
    expected_closure_digest = stable_m4_digest(
        "m4-expandable-completion-v1",
        spec.job_id,
        completion.result_artifact_hash,
        closure.child_set_hash,
    )
    if closure.completion_digest != expected_closure_digest:
        raise EventConflictError("child closure is bound to another result")
    if completion.terminal_state is JobState.COMPLETED_INACTIVE and plan.child_jobs:
        raise InvalidEventError("inactive expandable target cannot create children")
    for child in plan.child_jobs:
        if child.kind is not JobKind.VERIFY_PAIR or child.pair is None:
            raise ValidationError("expansion children must be VERIFY_PAIR jobs")
        if child.parent_job_id != spec.job_id:
            raise ValidationError("child names the wrong parent")
        if child.event_id != epoch.update.event_id:
            raise ValidationError("child event differs from parent epoch")
        if child.candidate_policy_id != spec.candidate_policy_id:
            raise ValidationError("child policy differs from parent policy")
        if spec.kind is JobKind.IMPACT_DISCOVERY:
            if child.pair.chunk_version_id != spec.target_chunk_version_id:
                raise ValidationError("impact-discovery child escaped chunk scope")
        elif spec.kind is JobKind.FRONTIER_RETRIEVE:
            if child.pair.claim_id != spec.target_claim_id:
                raise ValidationError("frontier child escaped claim scope")


def apply_completion(
    book: RuntimeBook,
    plan: CompletionPlan,
    *,
    active_chunk_ids: frozenset[str],
) -> TransitionResult:
    """Atomically complete a job, install children, and close its child set."""
    epoch = _epoch(book, plan.expected_epoch_id)
    job = _job(epoch, plan.completion.job_id)
    if job.completion is not None:
        existing_child_ids = tuple(
            child.spec.job_id
            for child in epoch.jobs
            if child.spec.parent_job_id == job.spec.job_id
        )
        requested_child_ids = tuple(child.job_id for child in plan.child_jobs)
        exact_replay = (
            job.completion == plan.completion
            and existing_child_ids == requested_child_ids
        )
        if exact_replay:
            return TransitionResult(book, replayed=True)
        raise EventConflictError("job already completed with different content")
    if epoch.revision != plan.expected_revision:
        raise EventConflictError("stale epoch revision")
    if job.state is not JobState.RUNNING:
        raise InvalidEventError("only a running job can complete")
    _validate_completion(epoch, job, plan, active_chunk_ids)
    if epoch.state is RuntimeEpochState.SEALED:
        raise InvalidEventError("sealed epoch cannot accept completion")
    if epoch.state is RuntimeEpochState.FAILED:
        if plan.completion.terminal_state is not JobState.COMPLETED_INACTIVE:
            raise InvalidEventError(
                "failed epoch accepts only late inactive completion"
            )
        if plan.child_jobs:
            raise InvalidEventError("failed epoch cannot expand late work")
    existing = {known.spec.job_id: known.spec for known in epoch.jobs}
    for child in plan.child_jobs:
        collision = existing.get(child.job_id)
        if collision is not None:
            raise EventConflictError("completion would redeclare a child job")
    changed_parent = replace(
        job,
        state=plan.completion.terminal_state,
        completion=plan.completion,
    )
    jobs = [
        changed_parent if known.spec.job_id == job.spec.job_id else known
        for known in epoch.jobs
    ]
    jobs.extend(RuntimeJob(spec=child) for child in plan.child_jobs)
    scopes = tuple(
        replace(scope, closed=True)
        if scope.root_job_id == job.spec.job_id
        else scope
        for scope in epoch.discovery_scopes
    )
    successful = {JobState.COMPLETED_ACTIVE, JobState.COMPLETED_INACTIVE}
    all_jobs_complete = all(runtime_job.state in successful for runtime_job in jobs)
    all_scopes_closed = all(scope.closed for scope in scopes)
    next_state = epoch.state
    if epoch.state is not RuntimeEpochState.FAILED:
        next_state = (
            RuntimeEpochState.SEMANTIC_COMPLETE
            if all_jobs_complete and all_scopes_closed
            else RuntimeEpochState.SEMANTIC_PENDING
        )
    changed = replace(
        epoch,
        state=next_state,
        revision=epoch.revision + 1,
        jobs=tuple(sorted(jobs, key=lambda item: item.spec.job_id)),
        discovery_scopes=scopes,
    )
    return TransitionResult(_replace_epoch(book, changed), replayed=False)


def fail_epoch(
    book: RuntimeBook, epoch_id: int, expected_revision: int, reason: str
) -> TransitionResult:
    if not reason.strip():
        raise ValidationError("epoch failure reason must be non-empty")
    epoch = _epoch(book, epoch_id)
    if epoch.state is RuntimeEpochState.FAILED:
        if epoch.failure_reason == reason:
            return TransitionResult(book, replayed=True)
        raise EventConflictError("failed epoch already records another reason")
    if epoch.state is RuntimeEpochState.SEALED:
        raise InvalidEventError("sealed epoch cannot fail")
    if epoch.revision != expected_revision:
        raise EventConflictError("stale epoch revision")
    changed = replace(
        epoch,
        state=RuntimeEpochState.FAILED,
        revision=epoch.revision + 1,
        failure_reason=reason,
    )
    epochs = tuple(
        changed if known.epoch_id == changed.epoch_id else known
        for known in book.epochs
    )
    return TransitionResult(replace(book, epochs=epochs, active_epoch_id=None), False)


def seal_epoch(
    book: RuntimeBook, epoch_id: int, expected_revision: int
) -> TransitionResult:
    epoch = _epoch(book, epoch_id)
    if epoch.state is RuntimeEpochState.SEALED:
        return TransitionResult(book, replayed=True)
    if epoch.state is not RuntimeEpochState.SEMANTIC_COMPLETE:
        raise InvalidEventError("only a semantically complete epoch can seal")
    if epoch.revision != expected_revision:
        raise EventConflictError("stale epoch revision")
    changed = replace(
        epoch,
        state=RuntimeEpochState.SEALED,
        revision=epoch.revision + 1,
    )
    epochs = tuple(
        changed if known.epoch_id == changed.epoch_id else known
        for known in book.epochs
    )
    return TransitionResult(
        replace(
            book,
            epochs=epochs,
            active_epoch_id=None,
            last_sealed_epoch_id=epoch_id,
        ),
        replayed=False,
    )


def _job_claim_id(job: RuntimeJob) -> str | None:
    if job.spec.pair is not None:
        return job.spec.pair.claim_id
    return job.spec.target_claim_id


def claim_evaluation_state(epoch: RuntimeEpoch, claim_id: str) -> EvaluationState:
    """Return provisional visibility implied by scopes and explicit jobs."""
    if not claim_id.strip():
        raise ValidationError("claim_id must be non-empty")
    if epoch.state is RuntimeEpochState.FAILED:
        return EvaluationState.FAILED
    if any(scope.contains(claim_id) for scope in epoch.discovery_scopes):
        return EvaluationState.PENDING
    targeted = tuple(job for job in epoch.jobs if _job_claim_id(job) == claim_id)
    if any(
        job.state in {JobState.TERMINAL_FAILED, JobState.CANCELLED}
        for job in targeted
    ):
        return EvaluationState.FAILED
    if any(job.open for job in targeted):
        return EvaluationState.PENDING
    return EvaluationState.COMPLETE


def answer_evaluation_state(
    epoch: RuntimeEpoch, required_claim_ids: tuple[str, ...]
) -> EvaluationState:
    states = tuple(
        claim_evaluation_state(epoch, claim_id) for claim_id in required_claim_ids
    )
    if EvaluationState.FAILED in states:
        return EvaluationState.FAILED
    if EvaluationState.PENDING in states:
        return EvaluationState.PENDING
    return EvaluationState.COMPLETE
