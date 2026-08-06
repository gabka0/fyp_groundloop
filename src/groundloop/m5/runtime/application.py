"""Persistence-neutral orchestration for the activated typed M5 runtime.

The coordinator in this module deliberately owns no database transaction and
no retrieval or verifier implementation.  It constructs the complete frozen
root declaration, sequences transaction-owning ports, keeps every external
call outside those transactions, and asks the persistence layer to seal only
after both the direct and requirement subgraphs have reached a terminal
working state.

The PostgreSQL implementation is intentionally a later composition layer.  In
particular, this module never calls the public M4 dispatcher and never mutates
an M4 or M5 persistence relation directly.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Protocol

from groundloop.errors import GroundLoopError, ValidationError
from groundloop.m4.application import OpenEventReceipt, StructuralWithdrawal
from groundloop.m4.contracts import (
    LogicalJobSpec as M4LogicalJobSpec,
)
from groundloop.m5.events import RegisterGroupEvent, ReplaceGroupEvent
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    M5AttemptCompletionReceipt,
    M5AttemptOutput,
    M5CandidatePolicyManifest,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5EventRunResult,
    M5JobKind,
    M5JobLease,
    M5LogicalJobSpec,
    M5RequirementDiscoveryResult,
    M5RequirementFallbackKey,
    M5RequirementPairInput,
    M5RequirementVerifierArtifact,
    M5RequirementWithdrawalPlan,
    M5RootBarrierReceipt,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeTiming,
    M5RuntimeWork,
    M5TypedEventPlan,
)
from groundloop.m5.runtime.frontier import coalesce_forward_root_keys


def _require_tuple(name: str, value: object) -> None:
    if not isinstance(value, tuple):
        raise ValidationError(f"{name} must be an immutable tuple")


def _sum_work(*items: M5RuntimeWork) -> M5RuntimeWork:
    """Add exact counters without carrying any source digest forward."""

    values = {
        name: sum(getattr(item, name) for item in items)
        for name in M5RuntimeWork.counter_names()
    }
    return M5RuntimeWork(**values)


@dataclass(frozen=True, slots=True)
class M5DirectOpenPlan:
    """The exact M4-v1 declaration to stage inside the typed open transaction."""

    withdrawal: StructuralWithdrawal | None
    root_jobs: tuple[M4LogicalJobSpec, ...] = ()

    def __post_init__(self) -> None:
        _require_tuple("direct root jobs", self.root_jobs)
        root_ids = tuple(job.job_id for job in self.root_jobs)
        if root_ids != tuple(sorted(set(root_ids))):
            raise ValidationError("direct root jobs must be ID-sorted and unique")

    @classmethod
    def empty(cls) -> M5DirectOpenPlan:
        return cls(None)


@dataclass(frozen=True, slots=True)
class M5DirectExecutionReceipt:
    """Result of running only missing direct-M4 work for one typed epoch."""

    resulting_revision: int
    call_work: M5RuntimeWork = M5RuntimeWork()
    blocked_reason: M5RunFailureReason | None = None
    terminal_failure_reason: M5RunFailureReason | None = None

    def __post_init__(self) -> None:
        if isinstance(self.resulting_revision, bool) or self.resulting_revision < 1:
            raise ValidationError("direct execution revision must be positive")
        if self.blocked_reason is not None and self.terminal_failure_reason is not None:
            raise ValidationError("direct execution cannot be blocked and terminal")
        if self.blocked_reason is not None and self.blocked_reason not in {
            M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
            M5RunFailureReason.VERIFIER_UNAVAILABLE,
        }:
            raise ValidationError("a blocked direct result requires unavailability")
        if self.terminal_failure_reason in {
            M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
            M5RunFailureReason.VERIFIER_UNAVAILABLE,
        }:
            raise ValidationError("terminal direct failure cannot remain retryable")


@dataclass(frozen=True, slots=True)
class M5DiscoveryExecution:
    """One external root result plus its immutable attempt output."""

    result: M5RequirementDiscoveryResult
    attempt_output: M5AttemptOutput
    eligible_snapshot_exhausted: bool
    call_work: M5RuntimeWork

    def __post_init__(self) -> None:
        if not isinstance(self.eligible_snapshot_exhausted, bool):
            raise ValidationError("snapshot-exhaustion evidence must be boolean")


@dataclass(frozen=True, slots=True)
class M5RequirementRootDeclaration:
    """One immutable scope contract paired with its deterministic root job."""

    scope: M5DiscoveryScopeContract
    job: M5LogicalJobSpec

    def __post_init__(self) -> None:
        if self.job.job_kind is M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL:
            expected_direction = M5DiscoveryDirection.FORWARD_REQUIREMENT
        elif self.job.job_kind is M5JobKind.REVERSE_REQUIREMENT_DISCOVERY:
            expected_direction = M5DiscoveryDirection.REVERSE_CHUNK
        else:
            raise ValidationError("root declaration requires an expandable root job")
        if (
            self.scope.direction is not expected_direction
            or self.job.scope_contract_digest != self.scope.scope_contract_digest
            or not self.job.expandable
            or self.job.parent_job_id is not None
            or self.job.pair is not None
        ):
            raise ValidationError("root job and discovery scope disagree")


@dataclass(frozen=True, slots=True)
class M5VerifierExecution:
    """One external verifier result plus its immutable attempt output."""

    pair_input: M5RequirementPairInput
    artifact: M5RequirementVerifierArtifact
    attempt_output: M5AttemptOutput
    call_work: M5RuntimeWork


class M5ExternalWorkFailure(GroundLoopError):
    """Typed external failure with an exact persisted work contribution."""

    def __init__(
        self,
        reason: M5RunFailureReason,
        *,
        retryable: bool,
        call_work: M5RuntimeWork,
        error_hash: str,
    ) -> None:
        if reason is M5RunFailureReason.INVARIANT_FAILURE:
            raise ValidationError("external work cannot report invariant_failure")
        unavailable = reason in {
            M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
            M5RunFailureReason.VERIFIER_UNAVAILABLE,
        }
        if retryable != unavailable:
            raise ValidationError("external failure reason/retryability disagree")
        if (
            len(error_hash) != 64
            or error_hash != error_hash.lower()
            or any(character not in "0123456789abcdef" for character in error_hash)
        ):
            raise ValidationError("external failure hash must be a lowercase SHA-256")
        self.reason = reason
        self.retryable = retryable
        self.call_work = call_work
        self.error_hash = error_hash
        super().__init__(reason.value)


class M5CandidatePolicyPort(Protocol):
    def candidate_policy(
        self, candidate_policy_id: str
    ) -> M5CandidatePolicyManifest: ...


class M5TypedStructuralPort(Protocol):
    """Own exact withdrawal planning and the single typed open transaction."""

    def plan_exact_requirement_withdrawal(
        self, event: M5TypedEventPlan
    ) -> M5RequirementWithdrawalPlan: ...

    def open_typed_event_atomically(
        self,
        event: M5TypedEventPlan,
        direct_withdrawal: StructuralWithdrawal | None,
        requirement_withdrawal: M5RequirementWithdrawalPlan,
        direct_roots: tuple[M4LogicalJobSpec, ...],
        requirement_roots: tuple[M5RequirementRootDeclaration, ...],
        requirement_root_set_hash: str,
    ) -> OpenEventReceipt: ...


class M5DirectSubgraphPort(Protocol):
    """Plan and resume the preserved M4-v1 direct subgraph without sealing it."""

    def plan_direct_open(self, event: M5TypedEventPlan) -> M5DirectOpenPlan: ...

    def run_pending_direct(
        self,
        epoch_id: int,
        expected_revision: int,
        event: M5TypedEventPlan,
    ) -> M5DirectExecutionReceipt: ...


class M5RequirementDiscoveryPort(Protocol):
    def discover_requirement_scope(
        self,
        epoch_id: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        manifest: M5CandidatePolicyManifest,
        event: M5TypedEventPlan,
    ) -> M5DiscoveryExecution: ...


class M5RequirementVerifierPort(Protocol):
    def verify_requirement_pair(
        self,
        epoch_id: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        manifest: M5CandidatePolicyManifest,
        event: M5TypedEventPlan,
    ) -> M5VerifierExecution: ...


class M5RuntimePersistencePort(Protocol):
    """Frozen transaction-owning runtime transitions used by this coordinator."""

    def read_typed_event_result(
        self, event_id: str, payload_hash: str
    ) -> M5EventRunResult | None: ...

    def acquire_m5_job(
        self, epoch_id: int, expected_revision: int, job: M5LogicalJobSpec
    ) -> M5JobLease: ...

    def mark_m5_retryable_failure(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5JobLease,
        error_hash: str,
    ) -> M5AttemptCompletionReceipt: ...

    def stage_m5_discovery_result_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        result: M5RequirementDiscoveryResult,
        attempt_output: M5AttemptOutput,
    ) -> M5AttemptCompletionReceipt: ...

    def close_m5_requirement_roots_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        requirement_root_set_hash: str,
    ) -> M5RootBarrierReceipt: ...

    def complete_m5_verifier_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        pair_input: M5RequirementPairInput,
        verifier_artifact: M5RequirementVerifierArtifact,
        attempt_output: M5AttemptOutput,
    ) -> M5AttemptCompletionReceipt: ...

    def fail_typed_epoch_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        failure_reason: M5RunFailureReason,
    ) -> M5EventRunResult: ...

    def request_typed_seal_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        event: M5TypedEventPlan,
    ) -> M5EventRunResult: ...


class M5RuntimeReadPort(Protocol):
    """Read-only reconnect hydration absent from the frozen mutator list."""

    def verifier_jobs(self, epoch_id: int) -> tuple[M5LogicalJobSpec, ...]: ...

    def current_event_work(self, epoch_id: int) -> M5RuntimeWork: ...

    def current_revision(self, epoch_id: int) -> int: ...


class M5PostSealAuditPort(Protocol):
    def audit_after_seal(
        self, event: M5TypedEventPlan, result: M5EventRunResult
    ) -> None: ...


@dataclass(slots=True)
class M5TypedApplication:
    """Coordinate one activated typed event without owning a transaction."""

    policies: M5CandidatePolicyPort
    structural: M5TypedStructuralPort
    direct: M5DirectSubgraphPort
    runtime: M5RuntimePersistencePort
    runtime_reads: M5RuntimeReadPort
    discovery: M5RequirementDiscoveryPort
    verifier: M5RequirementVerifierPort
    post_seal_audit: M5PostSealAuditPort | None = None

    def run_event(self, event: M5TypedEventPlan) -> M5EventRunResult:
        terminal = self.runtime.read_typed_event_result(
            event.structural_event_id, event.payload_hash
        )
        if terminal is not None:
            self._validate_terminal_replay(event, terminal)
            return terminal

        manifest = self.policies.candidate_policy(event.candidate_policy_id)
        if (
            manifest.candidate_policy_id != event.candidate_policy_id
            or manifest.manifest_hash != event.candidate_policy_manifest_hash
        ):
            raise ValidationError("typed event binds another candidate manifest")

        requirement_withdrawal = self.structural.plan_exact_requirement_withdrawal(
            event
        )
        if requirement_withdrawal.event_id != event.structural_event_id:
            raise ValidationError("requirement withdrawal belongs to another event")
        expected_deactivated_chunks = (
            ()
            if event.direct_plan is None
            else event.direct_plan.deactivated_chunk_version_ids
        )
        if (
            requirement_withdrawal.deactivated_chunk_version_ids
            != expected_deactivated_chunks
        ):
            raise ValidationError("requirement withdrawal disagrees with direct plan")
        direct_open = (
            self.direct.plan_direct_open(event)
            if event.direct_plan is not None
            else M5DirectOpenPlan.empty()
        )
        if event.direct_plan is not None:
            if direct_open.withdrawal is None:
                raise ValidationError("direct event lacks an exact M4 withdrawal")
            if (
                direct_open.withdrawal.plan.deactivated_chunk_ids
                != event.direct_plan.deactivated_chunk_version_ids
            ):
                raise ValidationError("direct M4 withdrawal disagrees with typed plan")
            for direct_root in direct_open.root_jobs:
                if (
                    direct_root.event_id != event.structural_event_id
                    or direct_root.candidate_policy_id != event.candidate_policy_id
                ):
                    raise ValidationError("direct M4 root disagrees with typed event")
        requirement_roots = self._requirement_roots(
            event, manifest, requirement_withdrawal
        )
        root_set_hash = digests.requirement_root_set_digest(
            declaration.job.logical_job_id for declaration in requirement_roots
        )
        opened = self.structural.open_typed_event_atomically(
            event,
            direct_open.withdrawal,
            requirement_withdrawal,
            direct_open.root_jobs,
            requirement_roots,
            root_set_hash,
        )
        if opened.already_sealed or opened.already_failed:
            terminal = self.runtime.read_typed_event_result(
                event.structural_event_id, event.payload_hash
            )
            if terminal is None:
                raise ValidationError("terminal open receipt lacks durable M5 result")
            self._validate_terminal_replay(event, terminal)
            return terminal

        revision = self.runtime_reads.current_revision(opened.epoch_id)
        call_work = M5RuntimeWork()

        direct_result = self.direct.run_pending_direct(opened.epoch_id, revision, event)
        revision = direct_result.resulting_revision
        call_work = _sum_work(call_work, direct_result.call_work)
        if direct_result.terminal_failure_reason is not None:
            return self._fail(
                event,
                opened,
                revision,
                direct_result.terminal_failure_reason,
                call_work,
            )
        if direct_result.blocked_reason is not None:
            return self._blocked(event, opened, direct_result.blocked_reason, call_work)

        for declaration in requirement_roots:
            root = declaration.job
            lease = self.runtime.acquire_m5_job(opened.epoch_id, revision, root)
            revision = lease.resulting_revision
            if not lease.should_execute:
                continue
            try:
                discovery_execution = self.discovery.discover_requirement_scope(
                    opened.epoch_id, lease, root, manifest, event
                )
            except M5ExternalWorkFailure as error:
                call_work = _sum_work(call_work, error.call_work)
                if error.retryable:
                    receipt = self.runtime.mark_m5_retryable_failure(
                        opened.epoch_id,
                        revision,
                        lease,
                        error.error_hash,
                    )
                    revision = receipt.resulting_revision
                    return self._blocked(event, opened, error.reason, call_work)
                return self._fail(event, opened, revision, error.reason, call_work)
            self._validate_discovery_execution(
                opened.epoch_id,
                declaration,
                lease,
                manifest,
                event,
                discovery_execution,
            )
            call_work = _sum_work(call_work, discovery_execution.call_work)
            staged = self.runtime.stage_m5_discovery_result_atomically(
                opened.epoch_id,
                revision,
                lease,
                root,
                discovery_execution.result,
                discovery_execution.attempt_output,
            )
            revision = staged.resulting_revision

        if requirement_roots:
            barrier = self.runtime.close_m5_requirement_roots_atomically(
                opened.epoch_id, revision, root_set_hash
            )
            if barrier.requirement_root_set_hash != root_set_hash:
                raise ValidationError("root barrier returned another declaration set")
            revision = barrier.resulting_revision

        verifier_jobs = self.runtime_reads.verifier_jobs(opened.epoch_id)
        if verifier_jobs != tuple(
            sorted(verifier_jobs, key=lambda job: job.logical_job_id)
        ) or len({job.logical_job_id for job in verifier_jobs}) != len(verifier_jobs):
            raise ValidationError("runtime verifier jobs are not ID-sorted unique")
        for job in verifier_jobs:
            if job.job_kind is not M5JobKind.VERIFY_REQUIREMENT_PAIR:
                raise ValidationError("runtime exposed a non-verifier child")
            lease = self.runtime.acquire_m5_job(opened.epoch_id, revision, job)
            revision = lease.resulting_revision
            if not lease.should_execute:
                continue
            try:
                verifier_execution = self.verifier.verify_requirement_pair(
                    opened.epoch_id, lease, job, manifest, event
                )
            except M5ExternalWorkFailure as error:
                call_work = _sum_work(call_work, error.call_work)
                if error.retryable:
                    receipt = self.runtime.mark_m5_retryable_failure(
                        opened.epoch_id,
                        revision,
                        lease,
                        error.error_hash,
                    )
                    revision = receipt.resulting_revision
                    return self._blocked(event, opened, error.reason, call_work)
                return self._fail(event, opened, revision, error.reason, call_work)
            self._validate_verifier_execution(
                opened.epoch_id,
                job,
                lease,
                manifest,
                event,
                verifier_execution,
            )
            call_work = _sum_work(call_work, verifier_execution.call_work)
            completed = self.runtime.complete_m5_verifier_atomically(
                opened.epoch_id,
                revision,
                lease,
                job,
                verifier_execution.pair_input,
                verifier_execution.artifact,
                verifier_execution.attempt_output,
            )
            revision = completed.resulting_revision

        sealed = self.runtime.request_typed_seal_atomically(
            opened.epoch_id, revision, event
        )
        sealed = replace(sealed, call_work=call_work)
        self._validate_terminal_result(
            event, opened.epoch_id, M5RunState.SEALED, sealed
        )
        if self.post_seal_audit is not None:
            self.post_seal_audit.audit_after_seal(event, sealed)
        return sealed

    @staticmethod
    def _validate_discovery_execution(
        epoch_id: int,
        declaration: M5RequirementRootDeclaration,
        lease: M5JobLease,
        manifest: M5CandidatePolicyManifest,
        event: M5TypedEventPlan,
        execution: M5DiscoveryExecution,
    ) -> None:
        job = declaration.job
        if lease.attempt is None:
            raise ValidationError("executable discovery lease lacks an attempt")
        result = execution.result
        if (
            result.root_job_id != job.logical_job_id
            or result.scope_contract_digest != job.scope_contract_digest
            or execution.attempt_output.attempt_id != lease.attempt.attempt_id
            or execution.attempt_output.logical_job_id != job.logical_job_id
            or execution.attempt_output.job_epoch_id != epoch_id
            or execution.attempt_output.payload_hash != job.payload_hash
            or execution.attempt_output.result_artifact_id != result.result_artifact_id
            or execution.attempt_output.result_artifact_hash
            != result.result_artifact_hash
        ):
            raise ValidationError("discovery execution does not bind its lease/job")
        direction = (
            M5DiscoveryDirection.FORWARD_REQUIREMENT
            if job.job_kind is M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL
            else M5DiscoveryDirection.REVERSE_CHUNK
        )
        result.validate_policy(
            direction=direction,
            manifest=manifest,
            eligible_snapshot_exhausted=execution.eligible_snapshot_exhausted,
        )
        for hit in result.channel_hits:
            if (
                hit.epoch_id != epoch_id
                or hit.candidate_policy_id != manifest.candidate_policy_id
            ):
                raise ValidationError("discovery hit has another epoch or policy")
            declaration.scope.validate_pair(hit.pair)
            hit.pair.validate_snapshots(
                event.requirement_registry_snapshot, event.active_chunk_snapshot
            )
        for selection in result.selections:
            declaration.scope.validate_pair(selection.pair)
            selection.pair.validate_snapshots(
                event.requirement_registry_snapshot, event.active_chunk_snapshot
            )

    @staticmethod
    def _validate_verifier_execution(
        epoch_id: int,
        job: M5LogicalJobSpec,
        lease: M5JobLease,
        manifest: M5CandidatePolicyManifest,
        event: M5TypedEventPlan,
        execution: M5VerifierExecution,
    ) -> None:
        if lease.attempt is None or job.pair is None:
            raise ValidationError("executable verifier lacks attempt or pair")
        requirement = event.requirement_registry_snapshot.member(job.pair.subject_id)
        chunk = event.active_chunk_snapshot.member(job.pair.chunk_version_id)
        execution.pair_input.validate_bound_rows(
            requirement_entry=requirement, chunk_entry=chunk
        )
        if (
            execution.pair_input.pair != job.pair
            or execution.pair_input.scope_contract_digest != job.scope_contract_digest
            or execution.pair_input.candidate_policy_id != job.candidate_policy_id
            or execution.artifact.pair != job.pair
            or execution.artifact.pair_input_hash
            != execution.pair_input.pair_input_hash
            or execution.artifact.execution_spec_hash != job.execution_spec_hash
            or execution.artifact.decision_policy_version
            != manifest.decision_policy_version
            or execution.attempt_output.attempt_id != lease.attempt.attempt_id
            or execution.attempt_output.logical_job_id != job.logical_job_id
            or execution.attempt_output.job_epoch_id != epoch_id
            or execution.attempt_output.payload_hash != job.payload_hash
            or execution.attempt_output.result_artifact_id
            != execution.artifact.artifact_id
            or execution.attempt_output.result_artifact_hash
            != execution.artifact.artifact_hash
        ):
            raise ValidationError("verifier execution does not bind its lease/job")

    def _requirement_roots(
        self,
        event: M5TypedEventPlan,
        manifest: M5CandidatePolicyManifest,
        withdrawal: M5RequirementWithdrawalPlan,
    ) -> tuple[M5RequirementRootDeclaration, ...]:
        new_requirement_ids: tuple[str, ...]
        if isinstance(event.event, RegisterGroupEvent):
            new_requirement_ids = tuple(
                requirement.requirement_version_id
                for requirement in event.event.group.requirements
            )
        elif isinstance(event.event, ReplaceGroupEvent):
            new_requirement_ids = tuple(
                requirement.requirement_version_id
                for requirement in event.event.successor.requirements
            )
        else:
            new_requirement_ids = ()

        forward_keys = coalesce_forward_root_keys(
            new_requirement_keys=tuple(
                M5RequirementFallbackKey(
                    requirement_version_id, event.candidate_policy_id
                )
                for requirement_version_id in new_requirement_ids
            ),
            fallback_keys=withdrawal.fallback_keys,
        )
        if any(
            key.candidate_policy_id != event.candidate_policy_id for key in forward_keys
        ):
            raise ValidationError("typed event cannot mix candidate policies")

        roots: list[M5RequirementRootDeclaration] = []
        for key in forward_keys:
            scope = M5DiscoveryScopeContract.build(
                direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
                requirement_version_id=key.requirement_version_id,
                inserted_chunk_version_id=None,
                candidate_policy_id=event.candidate_policy_id,
                requirement_registry_snapshot_digest=(
                    event.requirement_registry_snapshot.requirement_registry_snapshot_digest
                ),
                active_chunk_snapshot_digest=(
                    event.active_chunk_snapshot.active_chunk_snapshot_digest
                ),
            )
            scope.validate_snapshots(
                event.requirement_registry_snapshot, event.active_chunk_snapshot
            )
            roots.append(
                M5RequirementRootDeclaration(
                    scope=scope,
                    job=M5LogicalJobSpec.build(
                        structural_event_id=event.structural_event_id,
                        job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
                        manifest=manifest,
                        scope=scope,
                    ),
                )
            )

        inserted_chunk_ids = (
            ()
            if event.direct_plan is None
            else event.direct_plan.inserted_chunk_version_ids
        )
        for chunk_version_id in inserted_chunk_ids:
            scope = M5DiscoveryScopeContract.build(
                direction=M5DiscoveryDirection.REVERSE_CHUNK,
                requirement_version_id=None,
                inserted_chunk_version_id=chunk_version_id,
                candidate_policy_id=event.candidate_policy_id,
                requirement_registry_snapshot_digest=(
                    event.requirement_registry_snapshot.requirement_registry_snapshot_digest
                ),
                active_chunk_snapshot_digest=(
                    event.active_chunk_snapshot.active_chunk_snapshot_digest
                ),
            )
            scope.validate_snapshots(
                event.requirement_registry_snapshot, event.active_chunk_snapshot
            )
            roots.append(
                M5RequirementRootDeclaration(
                    scope=scope,
                    job=M5LogicalJobSpec.build(
                        structural_event_id=event.structural_event_id,
                        job_kind=M5JobKind.REVERSE_REQUIREMENT_DISCOVERY,
                        manifest=manifest,
                        scope=scope,
                    ),
                )
            )
        ordered = tuple(
            sorted(roots, key=lambda declaration: declaration.job.logical_job_id)
        )
        if len({root.job.logical_job_id for root in ordered}) != len(ordered):
            raise ValidationError(
                "typed root declaration contains an identity collision"
            )
        return ordered

    def _blocked(
        self,
        event: M5TypedEventPlan,
        opened: OpenEventReceipt,
        reason: M5RunFailureReason,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        return M5EventRunResult.build(
            event_id=event.structural_event_id,
            payload_hash=event.payload_hash,
            epoch_id=opened.epoch_id,
            state=M5RunState.BLOCKED,
            replayed_outcome=None,
            open_receipt=opened,
            publication_receipt=None,
            event_work=self.runtime_reads.current_event_work(opened.epoch_id),
            call_work=call_work,
            event_timing=M5RuntimeTiming(),
            call_timing=M5RuntimeTiming(),
            combined_deltas=(),
            changed_state_references=(),
            failure_reason=reason,
        )

    def _fail(
        self,
        event: M5TypedEventPlan,
        opened: OpenEventReceipt,
        revision: int,
        reason: M5RunFailureReason,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        failed = self.runtime.fail_typed_epoch_atomically(
            opened.epoch_id, revision, reason
        )
        failed = replace(failed, call_work=call_work)
        self._validate_terminal_result(
            event, opened.epoch_id, M5RunState.FAILED, failed
        )
        return failed

    @staticmethod
    def _validate_terminal_replay(
        event: M5TypedEventPlan, result: M5EventRunResult
    ) -> None:
        if (
            result.event_id != event.structural_event_id
            or result.payload_hash != event.payload_hash
            or result.state is not M5RunState.REPLAYED
            or not result.call_work.is_zero
        ):
            raise ValidationError("durable typed replay has an invalid envelope")

    @staticmethod
    def _validate_terminal_result(
        event: M5TypedEventPlan,
        epoch_id: int,
        expected_state: M5RunState,
        result: M5EventRunResult,
    ) -> None:
        if (
            result.event_id != event.structural_event_id
            or result.payload_hash != event.payload_hash
            or result.epoch_id != epoch_id
            or result.state is not expected_state
        ):
            raise ValidationError("typed terminal result has another envelope")


__all__ = [
    "M5CandidatePolicyPort",
    "M5DirectExecutionReceipt",
    "M5DirectOpenPlan",
    "M5DirectSubgraphPort",
    "M5DiscoveryExecution",
    "M5ExternalWorkFailure",
    "M5PostSealAuditPort",
    "M5RequirementDiscoveryPort",
    "M5RequirementRootDeclaration",
    "M5RequirementVerifierPort",
    "M5RuntimePersistencePort",
    "M5RuntimeReadPort",
    "M5TypedApplication",
    "M5TypedStructuralPort",
    "M5VerifierExecution",
]
