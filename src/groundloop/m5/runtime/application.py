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

from copy import deepcopy
from dataclasses import dataclass, fields, replace
from typing import Protocol

from groundloop.domain import StatusDelta
from groundloop.errors import GroundLoopError, ValidationError
from groundloop.m4.application import (
    ObservationCompletionReceipt,
    OpenEventReceipt,
    PublicationReceipt,
    StructuralWithdrawal,
)
from groundloop.m4.contracts import (
    DiscoveryScope as M4DiscoveryScope,
)
from groundloop.m4.contracts import (
    JobKind as M4JobKind,
)
from groundloop.m4.contracts import (
    LogicalJobSpec as M4LogicalJobSpec,
)
from groundloop.m4.contracts import stable_m4_digest
from groundloop.m4.pipeline import StructuralPayload
from groundloop.m5.events import RegisterGroupEvent, ReplaceGroupEvent
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    M5AcquisitionDisposition,
    M5AttemptCompletionReceipt,
    M5AttemptOutput,
    M5CandidatePolicyManifest,
    M5ChangedStateReference,
    M5DirectAttemptReturnReceipt,
    M5DirectLateReturnReceipt,
    M5DirectNormalReturnReceipt,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5EventRunResult,
    M5ExecutionEvidenceDisposition,
    M5JobKind,
    M5JobLease,
    M5JobState,
    M5LeaseTerminalProjection,
    M5LogicalJobSpec,
    M5ReplayedOutcome,
    M5RequirementAttemptReturnReceipt,
    M5RequirementChannelHit,
    M5RequirementDiscoveryResult,
    M5RequirementFallbackKey,
    M5RequirementPairInput,
    M5RequirementReturnDisposition,
    M5RequirementScopeSelection,
    M5RequirementVerifierArtifact,
    M5RequirementWithdrawalPlan,
    M5RootBarrierReceipt,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeTiming,
    M5RuntimeTimingCoverage,
    M5RuntimeTimingObservation,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5StateReferenceKind,
    M5TerminalReason,
    M5TransitionTimingAnchor,
    M5TransitionTimingReceipt,
    M5TypedDirectReturnKind,
    M5TypedEventPlan,
    SemanticPairKey,
    validate_changed_state_references,
    validate_combined_deltas,
)
from groundloop.m5.runtime.frontier import coalesce_forward_root_keys


def _require_tuple(name: str, value: object) -> None:
    if type(value) is not tuple:
        raise ValidationError(f"{name} must be an immutable tuple")


def _validate_exact_runtime_work(work: M5RuntimeWork) -> None:
    if type(work) is not M5RuntimeWork:
        raise ValidationError("runtime work must be exact M5RuntimeWork")
    if any(type(value) is not int for value in work.counter_values()):
        raise ValidationError("runtime work contains a nonexact counter")
    if type(work.work_digest) is not str:
        raise ValidationError("runtime work contains a nonexact digest")
    replace(work)


def _sum_work(*items: M5RuntimeWork) -> M5RuntimeWork:
    """Add exact counters without carrying any source digest forward."""

    for item in items:
        _validate_exact_runtime_work(item)
    values = {
        name: sum(getattr(item, name) for item in items)
        for name in M5RuntimeWork.counter_names()
    }
    return M5RuntimeWork(**values)


@dataclass(frozen=True, slots=True)
class M5DirectOpenPlan:
    """The exact M4-v1 declaration to stage inside the typed open transaction."""

    structural_payload: StructuralPayload | None
    withdrawal: StructuralWithdrawal | None
    root_jobs: tuple[M4LogicalJobSpec, ...] = ()
    discovery_scopes: tuple[M4DiscoveryScope, ...] = ()

    def __post_init__(self) -> None:
        _require_tuple("direct root jobs", self.root_jobs)
        _require_tuple("direct discovery scopes", self.discovery_scopes)
        root_ids = tuple(job.job_id for job in self.root_jobs)
        if root_ids != tuple(sorted(set(root_ids))):
            raise ValidationError("direct root jobs must be ID-sorted and unique")
        scope_ids = tuple(scope.root_job_id for scope in self.discovery_scopes)
        if scope_ids != tuple(sorted(set(scope_ids))):
            raise ValidationError("direct scopes must be root-ID-sorted and unique")
        impact_ids = tuple(
            job.job_id
            for job in self.root_jobs
            if job.kind is M4JobKind.IMPACT_DISCOVERY
        )
        if scope_ids != impact_ids:
            raise ValidationError("each direct impact root requires one scope")
        if (self.structural_payload is None) != (self.withdrawal is None):
            raise ValidationError(
                "direct structural payload and withdrawal are jointly present"
            )

    @classmethod
    def empty(cls) -> M5DirectOpenPlan:
        return cls(None, None)


@dataclass(frozen=True, slots=True)
class M5DirectExecutionReceipt:
    """Result of running only missing direct-M4 work for one typed epoch.

    The three ``selected_successful_outer_*`` fields are jointly present only
    when this application invocation must project a checked successful outer
    settlement that lost the terminal cutoff.  The copied invoked kind and job
    identity bind the selected receipt without changing a frozen direct DTO.
    """

    resulting_revision: int
    call_work: M5RuntimeWork = M5RuntimeWork()
    blocked_reason: M5RunFailureReason | None = None
    terminal_failure_reason: M5RunFailureReason | None = None
    selected_successful_outer_receipt: M5DirectAttemptReturnReceipt | None = None
    selected_successful_outer_return_kind: M5TypedDirectReturnKind | None = None
    selected_successful_outer_job_id: str | None = None

    def __post_init__(self) -> None:
        if type(self.resulting_revision) is not int or self.resulting_revision < 1:
            raise ValidationError("direct execution revision must be positive")
        if type(self.call_work) is not M5RuntimeWork:
            raise ValidationError("direct execution call work must be exact")
        _validate_exact_runtime_work(self.call_work)
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
        selected_values = (
            self.selected_successful_outer_receipt,
            self.selected_successful_outer_return_kind,
            self.selected_successful_outer_job_id,
        )
        if any(value is not None for value in selected_values) != all(
            value is not None for value in selected_values
        ):
            raise ValidationError(
                "selected direct outer receipt binding must be jointly present"
            )
        if self.selected_successful_outer_receipt is not None:
            selected = self.selected_successful_outer_receipt
            return_kind = self.selected_successful_outer_return_kind
            job_id = self.selected_successful_outer_job_id
            if type(selected) is not M5DirectAttemptReturnReceipt:
                raise ValidationError(
                    "selected direct outer receipt has another receipt type"
                )
            if type(return_kind) is not M5TypedDirectReturnKind:
                raise ValidationError(
                    "selected direct outer receipt binding has another return kind"
                )
            if type(job_id) is not str or not job_id:
                raise ValidationError(
                    "selected direct outer receipt binding lacks its job ID"
                )
            if (
                self.blocked_reason is not None
                or self.terminal_failure_reason is not None
            ):
                raise ValidationError(
                    "selected direct outer receipt requires successful execution"
                )
            if selected.return_kind is not return_kind:
                raise ValidationError(
                    "selected direct outer receipt changed its invoked return kind"
                )
            branch = selected.normal if selected.normal is not None else selected.late
            if branch is None:
                raise ValidationError(
                    "selected direct outer receipt lacks its selected branch"
                )
            if (
                type(branch.job_id) is not str
                or type(branch.attempt_id) is not str
                or not branch.job_id
                or not branch.attempt_id
            ):
                raise ValidationError(
                    "selected direct outer receipt has an invalid job binding"
                )
            replace(branch)
            replace(selected)
            if branch.job_id != job_id:
                raise ValidationError(
                    "selected direct outer receipt changed its invoked job"
                )
            if branch.resulting_revision != self.resulting_revision:
                raise ValidationError(
                    "selected direct outer receipt returned another revision"
                )
            if type(branch.current_terminal_logical_result_hash) is not str:
                raise ValidationError(
                    "selected direct outer receipt lacks a terminal result hash"
                )


@dataclass(frozen=True, slots=True)
class M5DiscoveryExecution:
    """One external root result plus its immutable attempt output."""

    result: M5RequirementDiscoveryResult
    attempt_output: M5AttemptOutput
    eligible_snapshot_exhausted: bool
    execution_disposition: M5ExecutionEvidenceDisposition
    call_work: M5RuntimeWork
    attempt_timing: M5RuntimeTiming | None

    def __post_init__(self) -> None:
        if not isinstance(self.eligible_snapshot_exhausted, bool):
            raise ValidationError("snapshot-exhaustion evidence must be boolean")
        _validate_successful_execution_evidence(
            self.execution_disposition, self.call_work, self.attempt_timing
        )


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
    execution_disposition: M5ExecutionEvidenceDisposition
    call_work: M5RuntimeWork
    attempt_timing: M5RuntimeTiming | None

    def __post_init__(self) -> None:
        _validate_successful_execution_evidence(
            self.execution_disposition, self.call_work, self.attempt_timing
        )


def _validate_successful_execution_evidence(
    disposition: M5ExecutionEvidenceDisposition,
    call_work: M5RuntimeWork,
    attempt_timing: M5RuntimeTiming | None,
) -> None:
    if not isinstance(call_work, M5RuntimeWork):
        raise ValidationError("execution call_work must be M5RuntimeWork")
    if attempt_timing is not None and not isinstance(attempt_timing, M5RuntimeTiming):
        raise ValidationError("attempt_timing must be M5RuntimeTiming or None")
    if not isinstance(disposition, M5ExecutionEvidenceDisposition):
        raise ValidationError(
            "execution_disposition must be M5ExecutionEvidenceDisposition"
        )
    if disposition not in {
        M5ExecutionEvidenceDisposition.RETURNED,
        M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
    }:
        raise ValidationError(
            "successful execution requires returned or reused_artifact evidence"
        )
    if disposition is M5ExecutionEvidenceDisposition.REUSED_ARTIFACT and any(
        getattr(call_work, name)
        for name in M5RuntimeWork.counter_names()
        if name not in {"bytes_hashed", "bytes_serialized"}
    ):
        raise ValidationError(
            "reused execution can charge only hashing and serialization bytes"
        )


class M5ExternalWorkFailure(GroundLoopError):
    """Typed external failure with an exact persisted work contribution."""

    def __init__(
        self,
        reason: M5RunFailureReason,
        *,
        retryable: bool,
        call_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
        error_hash: str,
    ) -> None:
        if not isinstance(reason, M5RunFailureReason):
            raise ValidationError("external failure reason must be M5RunFailureReason")
        if not isinstance(retryable, bool):
            raise ValidationError("external failure retryable must be boolean")
        if not isinstance(call_work, M5RuntimeWork):
            raise ValidationError("external failure call_work must be M5RuntimeWork")
        if attempt_timing is not None and not isinstance(
            attempt_timing, M5RuntimeTiming
        ):
            raise ValidationError("attempt_timing must be M5RuntimeTiming or None")
        if reason in {
            M5RunFailureReason.WORK_IN_PROGRESS,
            M5RunFailureReason.INVARIANT_FAILURE,
        }:
            raise ValidationError(
                "external work cannot report work_in_progress or invariant_failure"
            )
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
        self.attempt_timing = attempt_timing
        self.error_hash = error_hash
        super().__init__(reason.value)


@dataclass(frozen=True, slots=True)
class M5TerminalInvocationTelemetry:
    """One fresh caller-owned identity and completed terminal-call timing."""

    invocation_id: str
    call_timing: M5RuntimeTiming | None

    def __post_init__(self) -> None:
        if not isinstance(self.invocation_id, str) or not self.invocation_id:
            raise ValidationError("terminal invocation ID must be nonempty")
        if self.call_timing is not None and not isinstance(
            self.call_timing, M5RuntimeTiming
        ):
            raise ValidationError(
                "terminal call timing must be M5RuntimeTiming or None"
            )


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
        direct_payload: StructuralPayload | None,
        direct_withdrawal: StructuralWithdrawal | None,
        requirement_withdrawal: M5RequirementWithdrawalPlan,
        direct_roots: tuple[M4LogicalJobSpec, ...],
        direct_scopes: tuple[M4DiscoveryScope, ...],
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
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> M5AttemptCompletionReceipt: ...

    def mark_m5_terminal_failure(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5JobLease,
        terminal_reason: M5TerminalReason,
        error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> M5AttemptCompletionReceipt: ...

    def stage_m5_discovery_result_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        result: M5RequirementDiscoveryResult,
        attempt_output: M5AttemptOutput,
        execution_disposition: M5ExecutionEvidenceDisposition,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
        *,
        eligible_snapshot_exhausted: bool,
    ) -> M5RequirementAttemptReturnReceipt: ...

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
        execution_disposition: M5ExecutionEvidenceDisposition,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> M5RequirementAttemptReturnReceipt: ...

    def fail_typed_epoch_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        failure_reason: M5RunFailureReason,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult: ...

    def request_typed_seal_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        event: M5TypedEventPlan,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult: ...

    def append_transition_call_timing(
        self,
        epoch_id: int,
        contribution_kind: M5RuntimeWorkContributionKind,
        source_id: str,
        contribution_key_digest: str,
        anchor_revision: int,
        observed_timing: M5RuntimeTiming | None,
    ) -> M5TransitionTimingReceipt: ...

    def append_terminal_invocation_telemetry(
        self,
        invocation_id: str,
        event_id: str,
        epoch_id: int,
        terminal_logical_result_hash: str,
        call_timing: M5RuntimeTiming | None,
        call_timing_coverage: M5RuntimeTimingCoverage,
    ) -> None: ...


class M5RuntimeReadPort(Protocol):
    """Read-only reconnect hydration absent from the frozen mutator list."""

    def verifier_jobs(self, epoch_id: int) -> tuple[M5LogicalJobSpec, ...]: ...

    def current_event_work(self, epoch_id: int) -> M5RuntimeWork: ...

    def current_event_timing(
        self, epoch_id: int
    ) -> tuple[M5RuntimeTiming, M5RuntimeTimingCoverage]: ...

    def current_revision(self, epoch_id: int) -> int: ...


class M5RuntimeMeasurementPort(Protocol):
    """Measure only intervals that have ended at the application boundary."""

    def transition_call_timing(
        self, anchor: M5TransitionTimingAnchor
    ) -> M5RuntimeTiming | None: ...

    def terminal_invocation(
        self, event: M5TypedEventPlan, result: M5EventRunResult
    ) -> M5TerminalInvocationTelemetry: ...


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
    measurements: M5RuntimeMeasurementPort
    post_seal_audit: M5PostSealAuditPort | None = None

    def run_event(self, event: M5TypedEventPlan) -> M5EventRunResult:
        terminal = self.runtime.read_typed_event_result(
            event.structural_event_id, event.payload_hash
        )
        if terminal is not None:
            self._validate_terminal_replay(event, terminal)
            return self._finish_terminal_invocation(event, terminal)

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
            direct_open.structural_payload,
            direct_open.withdrawal,
            requirement_withdrawal,
            direct_open.root_jobs,
            direct_open.discovery_scopes,
            requirement_roots,
            root_set_hash,
        )
        self._validate_open_event_receipt(opened)
        if opened.already_sealed or opened.already_failed:
            terminal = self.runtime.read_typed_event_result(
                event.structural_event_id, event.payload_hash
            )
            if terminal is None:
                raise ValidationError("terminal open receipt lacks durable M5 result")
            self._validate_terminal_replay(
                event,
                terminal,
                expected_epoch_id=opened.epoch_id,
            )
            if terminal.open_receipt != opened:
                raise ValidationError(
                    "terminal open receipt disagrees with durable M5 result"
                )
            return self._finish_terminal_invocation(event, terminal)

        held_opened_snapshot = replace(opened)
        revision = self.runtime_reads.current_revision(opened.epoch_id)
        call_work = M5RuntimeWork()
        if not opened.replayed:
            revision = self._append_transition_anchor(
                M5TransitionTimingAnchor.build(
                    epoch_id=opened.epoch_id,
                    contribution_kind=(M5RuntimeWorkContributionKind.STRUCTURAL_OPEN),
                    source_id=event.structural_event_id,
                    anchor_revision=1,
                    terminal_transition=False,
                )
            )

        direct_result = self.direct.run_pending_direct(opened.epoch_id, revision, event)
        self._validate_direct_execution_receipt(direct_result)
        revision = direct_result.resulting_revision
        call_work = _sum_work(call_work, direct_result.call_work)
        if direct_result.selected_successful_outer_receipt is not None:
            return self._finish_selected_direct_terminal_projection(
                event,
                opened,
                held_opened_snapshot,
                direct_result,
                call_work,
            )
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
            disposition = self._require_acquisition_disposition(lease, root)
            if disposition in {
                M5AcquisitionDisposition.DISPATCH_NEW,
                M5AcquisitionDisposition.DISPATCH_TAKEOVER,
            }:
                revision = self._append_acquisition_anchor(opened.epoch_id, lease)
            elif disposition is M5AcquisitionDisposition.LIVE_LEASE:
                return self._blocked(
                    event,
                    opened,
                    M5RunFailureReason.WORK_IN_PROGRESS,
                    call_work,
                )
            elif disposition is M5AcquisitionDisposition.RESULT_RESERVED:
                continue
            elif disposition is M5AcquisitionDisposition.TERMINAL:
                projected = self._handle_terminal_projection(
                    event, opened, revision, lease, call_work
                )
                if projected is not None:
                    return projected
                continue
            else:
                raise ValidationError("unsupported requirement acquisition disposition")
            try:
                discovery_execution = self.discovery.discover_requirement_scope(
                    opened.epoch_id, lease, root, manifest, event
                )
            except M5ExternalWorkFailure as error:
                self._validate_external_work_failure(error)
                call_work = _sum_work(call_work, error.call_work)
                return self._settle_external_failure(
                    event, opened, revision, lease, error, call_work
                )
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
                discovery_execution.execution_disposition,
                discovery_execution.call_work,
                discovery_execution.attempt_timing,
                eligible_snapshot_exhausted=(
                    discovery_execution.eligible_snapshot_exhausted
                ),
            )
            revision, early = self._handle_requirement_return(
                event,
                opened,
                root,
                lease,
                staged,
                M5RuntimeWorkContributionKind.ROOT_RESULT_STAGE,
                call_work,
            )
            if early is not None:
                return early

        if requirement_roots:
            barrier = self.runtime.close_m5_requirement_roots_atomically(
                opened.epoch_id, revision, root_set_hash
            )
            if not isinstance(barrier, M5RootBarrierReceipt):
                raise ValidationError("root barrier returned another receipt type")
            replace(barrier)
            if barrier.requirement_root_set_hash != root_set_hash:
                raise ValidationError("root barrier returned another declaration set")
            revision = barrier.resulting_revision
            if not barrier.exact_replay:
                revision = self._append_transition_anchor(
                    M5TransitionTimingAnchor.build(
                        epoch_id=opened.epoch_id,
                        contribution_kind=M5RuntimeWorkContributionKind.ROOT_BARRIER,
                        source_id=event.structural_event_id,
                        anchor_revision=revision,
                        terminal_transition=False,
                    )
                )

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
            disposition = self._require_acquisition_disposition(lease, job)
            if disposition in {
                M5AcquisitionDisposition.DISPATCH_NEW,
                M5AcquisitionDisposition.DISPATCH_TAKEOVER,
            }:
                revision = self._append_acquisition_anchor(opened.epoch_id, lease)
            elif disposition is M5AcquisitionDisposition.LIVE_LEASE:
                return self._blocked(
                    event,
                    opened,
                    M5RunFailureReason.WORK_IN_PROGRESS,
                    call_work,
                )
            elif disposition is M5AcquisitionDisposition.RESULT_RESERVED:
                raise ValidationError("verifier acquisition cannot be result_reserved")
            elif disposition is M5AcquisitionDisposition.TERMINAL:
                projected = self._handle_terminal_projection(
                    event, opened, revision, lease, call_work
                )
                if projected is not None:
                    return projected
                continue
            else:
                raise ValidationError("unsupported verifier acquisition disposition")
            try:
                verifier_execution = self.verifier.verify_requirement_pair(
                    opened.epoch_id, lease, job, manifest, event
                )
            except M5ExternalWorkFailure as error:
                self._validate_external_work_failure(error)
                call_work = _sum_work(call_work, error.call_work)
                return self._settle_external_failure(
                    event, opened, revision, lease, error, call_work
                )
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
                verifier_execution.execution_disposition,
                verifier_execution.call_work,
                verifier_execution.attempt_timing,
            )
            revision, early = self._handle_requirement_return(
                event,
                opened,
                job,
                lease,
                completed,
                M5RuntimeWorkContributionKind.VERIFIER_COMPLETION,
                call_work,
            )
            if early is not None:
                return early

        sealed = self.runtime.request_typed_seal_atomically(
            opened.epoch_id, revision, event, call_work
        )
        if sealed.state is M5RunState.REPLAYED:
            sealed = self._finish_active_terminal_projection(
                event,
                opened,
                call_work,
                sealed,
            )
        else:
            self._validate_terminal_result(
                event,
                opened.epoch_id,
                M5RunState.SEALED,
                sealed,
                expected_open_receipt=opened,
            )
            if sealed.call_work != call_work:
                raise ValidationError("seal result changed invocation call work")
            sealed = self._finish_terminal_invocation(event, sealed)
        if sealed.state is M5RunState.SEALED and self.post_seal_audit is not None:
            self.post_seal_audit.audit_after_seal(event, sealed)
        return sealed

    @staticmethod
    def _require_acquisition_disposition(
        lease: M5JobLease,
        job: M5LogicalJobSpec,
    ) -> M5AcquisitionDisposition:
        if not isinstance(lease, M5JobLease):
            raise ValidationError("acquisition must return an M5JobLease")
        if not isinstance(job, M5LogicalJobSpec):
            raise ValidationError("acquisition request must be an M5LogicalJobSpec")
        if lease.attempt is not None:
            replace(lease.attempt)
        if lease.terminal_projection is not None:
            replace(lease.terminal_projection)
        replace(lease)
        if lease.logical_job_id != job.logical_job_id:
            raise ValidationError("acquisition lease belongs to another job")
        if (
            lease.attempt is not None
            and lease.attempt.execution_spec_hash != job.execution_spec_hash
        ):
            raise ValidationError("acquisition attempt binds another execution spec")
        if not isinstance(lease.disposition, M5AcquisitionDisposition):
            raise ValidationError("typed application requires a total D24 acquisition")
        if lease.terminal_projection is not None:
            lease.terminal_projection.validate_job(job.logical_job_id)
            if lease.terminal_projection.terminal_state in {
                M5JobState.CANCELLED,
                M5JobState.TERMINAL_FAILED,
            }:
                expected_completion_digest = digests.job_completion_digest(
                    logical_job_id_value=job.logical_job_id,
                    payload_hash=job.payload_hash,
                    execution_spec_hash=job.execution_spec_hash,
                    terminal_state=lease.terminal_projection.terminal_state,
                    result_artifact_id=None,
                    result_artifact_hash=None,
                    scope_closure_digest=None,
                    child_set_hash=None,
                    archive_reason=lease.terminal_projection.terminal_reason,
                )
                if (
                    lease.terminal_projection.completion_digest
                    != expected_completion_digest
                ):
                    raise ValidationError(
                        "terminal acquisition completion digest changed its reason"
                    )
        return lease.disposition

    def _append_acquisition_anchor(self, epoch_id: int, lease: M5JobLease) -> int:
        if lease.dispatch_record_digest is None:
            raise ValidationError("dispatch acquisition lacks its durable record")
        return self._append_transition_anchor(
            M5TransitionTimingAnchor.build(
                epoch_id=epoch_id,
                contribution_kind=M5RuntimeWorkContributionKind.M5_ACQUISITION,
                source_id=lease.dispatch_record_digest,
                anchor_revision=lease.resulting_revision,
                terminal_transition=False,
            )
        )

    def _append_transition_anchor(self, anchor: M5TransitionTimingAnchor) -> int:
        if not isinstance(anchor, M5TransitionTimingAnchor):
            raise ValidationError("transition timing requires an exact anchor")
        replace(anchor)
        observed = self.measurements.transition_call_timing(anchor)
        if observed is not None:
            if not isinstance(observed, M5RuntimeTiming):
                raise ValidationError("transition measurement returned invalid timing")
            replace(observed)
        observation = M5RuntimeTimingObservation.build(observed)
        receipt = self.runtime.append_transition_call_timing(
            anchor.epoch_id,
            anchor.contribution_kind,
            anchor.source_id,
            anchor.contribution_key_digest,
            anchor.anchor_revision,
            observed,
        )
        if not isinstance(receipt, M5TransitionTimingReceipt):
            raise ValidationError("transition timing returned another receipt type")
        if not isinstance(receipt.anchor, M5TransitionTimingAnchor):
            raise ValidationError("transition timing receipt changed anchor type")
        if not isinstance(receipt.event_timing, M5RuntimeTiming):
            raise ValidationError("transition timing receipt changed timing type")
        if not isinstance(receipt.event_timing_coverage, M5RuntimeTimingCoverage):
            raise ValidationError("transition timing receipt changed coverage type")
        replace(receipt.anchor)
        replace(receipt.event_timing)
        replace(receipt.event_timing_coverage)
        replace(receipt)
        if receipt.anchor != anchor:
            raise ValidationError("transition timing receipt changed its anchor")
        receipt.validate_observation(observation)
        return receipt.resulting_revision

    def _settle_external_failure(
        self,
        event: M5TypedEventPlan,
        opened: OpenEventReceipt,
        revision: int,
        lease: M5JobLease,
        error: M5ExternalWorkFailure,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        if lease.attempt is None:
            raise ValidationError("external failure lease lacks an attempt")
        if error.retryable:
            receipt = self.runtime.mark_m5_retryable_failure(
                opened.epoch_id,
                revision,
                lease,
                error.error_hash,
                error.call_work,
                error.attempt_timing,
            )
        else:
            receipt = self.runtime.mark_m5_terminal_failure(
                opened.epoch_id,
                revision,
                lease,
                self._terminal_reason_for_failure(error.reason),
                error.error_hash,
                error.call_work,
                error.attempt_timing,
            )
        if not isinstance(receipt, M5AttemptCompletionReceipt):
            raise ValidationError("failure settlement returned another receipt type")
        replace(receipt)
        if (
            receipt.logical_job_id != lease.logical_job_id
            or receipt.attempt_id != lease.attempt.attempt_id
        ):
            raise ValidationError("failure receipt binds another job/attempt")
        revision = receipt.resulting_revision
        if not receipt.exact_replay:
            revision = self._append_transition_anchor(
                M5TransitionTimingAnchor.build(
                    epoch_id=opened.epoch_id,
                    contribution_kind=(
                        M5RuntimeWorkContributionKind.M5_ATTEMPT_EXECUTION
                    ),
                    source_id=lease.attempt.attempt_id,
                    anchor_revision=revision,
                    terminal_transition=False,
                )
            )
        if error.retryable:
            return self._blocked(event, opened, error.reason, call_work)
        return self._fail(
            event,
            opened,
            revision,
            error.reason,
            call_work,
            project_checked_requirement_replay=True,
        )

    def _handle_requirement_return(
        self,
        event: M5TypedEventPlan,
        opened: OpenEventReceipt,
        job: M5LogicalJobSpec,
        lease: M5JobLease,
        receipt: M5RequirementAttemptReturnReceipt,
        expected_anchor_kind: M5RuntimeWorkContributionKind,
        call_work: M5RuntimeWork,
    ) -> tuple[int, M5EventRunResult | None]:
        if not isinstance(receipt, M5RequirementAttemptReturnReceipt):
            raise ValidationError("successful return produced another receipt type")
        replace(receipt)
        if lease.attempt is None:
            raise ValidationError("successful return lease lacks an attempt")
        if (
            receipt.logical_job_id != job.logical_job_id
            or receipt.attempt_id != lease.attempt.attempt_id
        ):
            raise ValidationError("successful return receipt binds another job/attempt")
        receipt.validate_anchor_context(
            epoch_id=opened.epoch_id,
            expected_kind=(
                expected_anchor_kind
                if receipt.disposition is M5RequirementReturnDisposition.APPLIED
                else M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN
            ),
        )
        revision = receipt.resulting_revision
        if receipt.current_terminal_logical_result_hash is not None:
            terminal = self._hydrate_terminal_result(
                event,
                opened.epoch_id,
                receipt.current_terminal_logical_result_hash,
            )
            return revision, self._finish_active_terminal_projection(
                event,
                opened,
                call_work,
                terminal,
                expected_logical_result_hash=(
                    receipt.current_terminal_logical_result_hash
                ),
            )
        if receipt.transition_anchor is not None:
            revision = self._append_transition_anchor(receipt.transition_anchor)
        if receipt.disposition is M5RequirementReturnDisposition.APPLIED:
            return revision, None
        if receipt.disposition is M5RequirementReturnDisposition.EXPIRED_PRETERMINAL:
            return revision, self._blocked(
                event,
                opened,
                M5RunFailureReason.WORK_IN_PROGRESS,
                call_work,
            )
        if (
            receipt.disposition
            is M5RequirementReturnDisposition.TERMINAL_AUDIT_PRETERMINAL
        ):
            resumed = self.runtime.acquire_m5_job(opened.epoch_id, revision, job)
            resumed_disposition = self._require_acquisition_disposition(resumed, job)
            if resumed_disposition is not M5AcquisitionDisposition.TERMINAL:
                raise ValidationError(
                    "terminal-audit return did not resume to terminal scheduler state"
                )
            return resumed.resulting_revision, self._handle_terminal_projection(
                event,
                opened,
                resumed.resulting_revision,
                resumed,
                call_work,
            )
        raise ValidationError("postterminal requirement return lacks terminal result")

    def _handle_terminal_projection(
        self,
        event: M5TypedEventPlan,
        opened: OpenEventReceipt,
        revision: int,
        lease: M5JobLease,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult | None:
        projection = lease.terminal_projection
        if not isinstance(projection, M5LeaseTerminalProjection):
            raise ValidationError("terminal acquisition lacks its durable projection")
        projection.validate_job(lease.logical_job_id)
        if projection.terminal_reason is M5TerminalReason.EPOCH_FAILED:
            if projection.terminal_state not in {
                M5JobState.COMPLETED_INACTIVE,
                M5JobState.CANCELLED,
            }:
                raise ValidationError(
                    "epoch_failed projection has an invalid terminal job state"
                )
            terminal = self.runtime.read_typed_event_result(
                event.structural_event_id, event.payload_hash
            )
            if terminal is None:
                raise ValidationError(
                    "epoch_failed projection lacks the atomic durable event result"
                )
            return self._finish_active_terminal_projection(
                event,
                opened,
                call_work,
                terminal,
                expected_outcome=M5ReplayedOutcome.FAILED,
            )
        if projection.terminal_state is M5JobState.TERMINAL_FAILED:
            return self._fail(
                event,
                opened,
                revision,
                self._run_reason_from_projection(projection),
                call_work,
                project_checked_requirement_replay=True,
            )
        return None

    def _hydrate_terminal_result(
        self,
        event: M5TypedEventPlan,
        epoch_id: int,
        logical_result_hash: str,
    ) -> M5EventRunResult:
        terminal = self.runtime.read_typed_event_result(
            event.structural_event_id, event.payload_hash
        )
        if (
            terminal is None
            or type(terminal) is not M5EventRunResult
            or type(terminal.event_id) is not str
            or type(terminal.payload_hash) is not str
            or type(terminal.epoch_id) is not int
            or type(terminal.logical_result_hash) is not str
            or terminal.epoch_id != epoch_id
            or terminal.logical_result_hash != logical_result_hash
        ):
            raise ValidationError("terminal return projection lacks its exact result")
        self._validate_terminal_replay(event, terminal)
        return terminal

    @staticmethod
    def _terminal_reason_for_failure(
        reason: M5RunFailureReason,
    ) -> M5TerminalReason:
        mapping = {
            M5RunFailureReason.RETRY_EXHAUSTED: M5TerminalReason.RETRY_EXHAUSTED,
            M5RunFailureReason.RETRIEVAL_ERROR: M5TerminalReason.RETRIEVAL_ERROR,
            M5RunFailureReason.VERIFIER_ERROR: M5TerminalReason.VERIFIER_ERROR,
            M5RunFailureReason.INVALID_ARTIFACT: M5TerminalReason.INVALID_ARTIFACT,
        }
        try:
            return mapping[reason]
        except KeyError as error:
            raise ValidationError(
                "failure reason has no terminal job mapping"
            ) from error

    @staticmethod
    def _run_reason_from_projection(
        projection: M5LeaseTerminalProjection,
    ) -> M5RunFailureReason:
        assert projection.terminal_reason is not None
        mapping = {
            M5TerminalReason.RETRY_EXHAUSTED: M5RunFailureReason.RETRY_EXHAUSTED,
            M5TerminalReason.RETRIEVAL_ERROR: M5RunFailureReason.RETRIEVAL_ERROR,
            M5TerminalReason.VERIFIER_ERROR: M5RunFailureReason.VERIFIER_ERROR,
            M5TerminalReason.INVALID_ARTIFACT: M5RunFailureReason.INVALID_ARTIFACT,
        }
        try:
            return mapping[projection.terminal_reason]
        except KeyError as error:
            raise ValidationError(
                "terminal-failed projection has no exact run failure mapping"
            ) from error

    def _finish_terminal_invocation(
        self, event: M5TypedEventPlan, result: M5EventRunResult
    ) -> M5EventRunResult:
        self._validate_terminal_nested_contracts(result)
        self._validate_terminal_logical_result_hash(result)
        logical_result_hash = result.logical_result_hash
        if type(logical_result_hash) is not str:
            raise ValidationError("terminal result lacks its logical result hash")
        event_id = event.structural_event_id
        payload_hash = event.payload_hash
        if type(event_id) is not str or type(payload_hash) is not str:
            raise ValidationError("terminal event binding has a nonexact identity")
        epoch_id = result.epoch_id
        frozen_result = deepcopy(result)
        telemetry = self.measurements.terminal_invocation(event, result)
        self._validate_terminal_nested_contracts(result)
        self._validate_terminal_logical_result_hash(result)
        if result != frozen_result:
            raise ValidationError("terminal measurement changed its result envelope")
        if (
            type(event.structural_event_id) is not str
            or type(event.payload_hash) is not str
            or event.structural_event_id != event_id
            or event.payload_hash != payload_hash
        ):
            raise ValidationError("terminal measurement changed its event binding")
        if type(telemetry) is not M5TerminalInvocationTelemetry:
            raise ValidationError(
                "terminal measurement returned another telemetry type"
            )
        if type(telemetry.invocation_id) is not str or not telemetry.invocation_id:
            raise ValidationError("terminal measurement returned an invalid ID")
        replace(telemetry)
        if telemetry.call_timing is not None:
            if type(telemetry.call_timing) is not M5RuntimeTiming:
                raise ValidationError("terminal telemetry returned invalid call timing")
            self._validate_exact_runtime_timing(telemetry.call_timing)
        coverage = M5RuntimeTimingCoverage.single_point(
            telemetry.call_timing,
            terminal_client_roundtrip_included=telemetry.call_timing is not None,
        )
        self.runtime.append_terminal_invocation_telemetry(
            telemetry.invocation_id,
            event_id,
            epoch_id,
            logical_result_hash,
            telemetry.call_timing,
            coverage,
        )
        return replace(
            result,
            call_timing=(telemetry.call_timing or M5RuntimeTiming()),
            call_timing_coverage=coverage,
        )

    def _finish_active_terminal_projection(
        self,
        event: M5TypedEventPlan,
        opened: OpenEventReceipt,
        call_work: M5RuntimeWork,
        canonical: M5EventRunResult,
        *,
        expected_outcome: M5ReplayedOutcome | None = None,
        expected_failure_reason: M5RunFailureReason | None = None,
        expected_logical_result_hash: str | None = None,
    ) -> M5EventRunResult:
        """Validate, project, and only then append terminal-call telemetry."""

        self._validate_terminal_replay(
            event,
            canonical,
            expected_epoch_id=opened.epoch_id,
            expected_outcome=expected_outcome,
            expected_failure_reason=expected_failure_reason,
            expected_logical_result_hash=expected_logical_result_hash,
        )
        self._validate_active_open_receipt(opened, canonical.epoch_id)
        if not isinstance(call_work, M5RuntimeWork):
            raise ValidationError("active terminal call_work must be M5RuntimeWork")

        projected = replace(
            canonical,
            open_receipt=opened,
            call_work=call_work,
        )
        self._validate_active_terminal_projection(
            event,
            canonical,
            projected,
            opened,
            call_work,
        )
        return self._finish_terminal_invocation(event, projected)

    def _finish_selected_direct_terminal_projection(
        self,
        event: M5TypedEventPlan,
        opened: OpenEventReceipt,
        held_opened_snapshot: OpenEventReceipt,
        direct_result: M5DirectExecutionReceipt,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        """Hydrate one checked successful direct cutoff and project active work."""

        selected = direct_result.selected_successful_outer_receipt
        if selected is None:
            raise ValidationError("direct terminal projection lacks its outer receipt")
        if event.direct_plan is None:
            raise ValidationError(
                "selected direct outer receipt requires a typed direct event"
            )
        if opened != held_opened_snapshot:
            raise ValidationError(
                "selected direct terminal projection changed its held open receipt"
            )
        self._validate_active_open_receipt(opened, held_opened_snapshot.epoch_id)
        branch = self._validate_selected_direct_outer_receipt(selected)
        if branch.epoch_id != opened.epoch_id:
            raise ValidationError(
                "selected direct outer receipt belongs to another epoch"
            )
        if branch.resulting_revision != direct_result.resulting_revision:
            raise ValidationError(
                "selected direct outer receipt returned another revision"
            )
        logical_result_hash = branch.current_terminal_logical_result_hash
        if logical_result_hash is None:
            raise ValidationError(
                "selected direct outer receipt lacks a terminal result hash"
            )

        canonical = self._hydrate_terminal_result(
            event,
            opened.epoch_id,
            logical_result_hash,
        )
        if opened != held_opened_snapshot:
            raise ValidationError(
                "canonical direct hydration changed its held open receipt"
            )
        return self._finish_active_terminal_projection(
            event,
            opened,
            call_work,
            canonical,
            expected_logical_result_hash=logical_result_hash,
        )

    @staticmethod
    def _validate_active_open_receipt(
        opened: OpenEventReceipt, expected_epoch_id: int
    ) -> None:
        M5TypedApplication._validate_open_event_receipt(opened)
        if (
            opened.epoch_id != expected_epoch_id
            or opened.already_sealed is not False
            or opened.publication_id is not None
            or opened.already_failed is not False
            or opened.failure_reason is not None
        ):
            raise ValidationError(
                "active terminal projection requires the held nonterminal receipt"
            )

    @staticmethod
    def _validate_open_event_receipt(opened: OpenEventReceipt) -> None:
        if (
            type(opened) is not OpenEventReceipt
            or type(opened.epoch_id) is not int
            or opened.epoch_id < 1
            or type(opened.replayed) is not bool
            or type(opened.already_sealed) is not bool
            or type(opened.already_failed) is not bool
            or opened.already_sealed != (opened.publication_id is not None)
            or opened.already_failed != (opened.failure_reason is not None)
            or (opened.already_sealed and opened.already_failed)
            or (
                opened.publication_id is not None
                and (
                    type(opened.publication_id) is not str or not opened.publication_id
                )
            )
            or (
                opened.failure_reason is not None
                and (
                    type(opened.failure_reason) is not str or not opened.failure_reason
                )
            )
        ):
            raise ValidationError("structural open returned an invalid receipt")
        replace(opened)

    @classmethod
    def _validate_active_terminal_projection(
        cls,
        event: M5TypedEventPlan,
        canonical: M5EventRunResult,
        projected: M5EventRunResult,
        opened: OpenEventReceipt,
        call_work: M5RuntimeWork,
    ) -> None:
        if (
            type(canonical) is not M5EventRunResult
            or type(projected) is not M5EventRunResult
            or projected.event_id != event.structural_event_id
            or projected.payload_hash != event.payload_hash
            or projected.state is not M5RunState.REPLAYED
            or projected.open_receipt is not opened
            or projected.call_work is not call_work
        ):
            raise ValidationError("active terminal projection has an invalid envelope")
        frozen_names = (
            "event_id",
            "payload_hash",
            "epoch_id",
            "state",
            "replayed_outcome",
            "publication_receipt",
            "event_work",
            "event_timing",
            "call_timing",
            "combined_deltas",
            "changed_state_references",
            "failure_reason",
            "logical_result_hash",
            "event_timing_coverage",
            "call_timing_coverage",
        )
        if any(
            getattr(projected, name) != getattr(canonical, name)
            for name in frozen_names
        ):
            raise ValidationError("active terminal projection changed durable result")
        cls._validate_active_open_receipt(opened, projected.epoch_id)
        cls._validate_terminal_nested_contracts(projected)
        cls._validate_terminal_logical_result_hash(projected)

    @classmethod
    def _validate_direct_execution_receipt(
        cls,
        receipt: M5DirectExecutionReceipt,
    ) -> None:
        if type(receipt) is not M5DirectExecutionReceipt:
            raise ValidationError("direct execution returned another receipt type")
        if type(receipt.resulting_revision) is not int:
            raise ValidationError("direct execution returned an invalid revision")
        if type(receipt.call_work) is not M5RuntimeWork:
            raise ValidationError("direct execution returned invalid call work")
        if receipt.blocked_reason is not None and not isinstance(
            receipt.blocked_reason, M5RunFailureReason
        ):
            raise ValidationError("direct execution returned invalid blocked reason")
        if receipt.terminal_failure_reason is not None and not isinstance(
            receipt.terminal_failure_reason, M5RunFailureReason
        ):
            raise ValidationError("direct execution returned invalid terminal reason")
        _validate_exact_runtime_work(receipt.call_work)
        selected = receipt.selected_successful_outer_receipt
        selected_values = (
            selected,
            receipt.selected_successful_outer_return_kind,
            receipt.selected_successful_outer_job_id,
        )
        if any(value is not None for value in selected_values) != all(
            value is not None for value in selected_values
        ):
            raise ValidationError(
                "selected direct outer receipt binding must be jointly present"
            )
        if selected is not None:
            if (
                receipt.blocked_reason is not None
                or receipt.terminal_failure_reason is not None
            ):
                raise ValidationError(
                    "selected direct outer receipt requires successful execution"
                )
            return_kind = receipt.selected_successful_outer_return_kind
            job_id = receipt.selected_successful_outer_job_id
            if type(return_kind) is not M5TypedDirectReturnKind:
                raise ValidationError(
                    "selected direct outer receipt binding has another return kind"
                )
            if type(job_id) is not str or not job_id:
                raise ValidationError(
                    "selected direct outer receipt binding lacks its job ID"
                )
            branch = cls._validate_selected_direct_outer_receipt(selected)
            if selected.return_kind is not return_kind:
                raise ValidationError(
                    "selected direct outer receipt changed its invoked return kind"
                )
            if branch.job_id != job_id:
                raise ValidationError(
                    "selected direct outer receipt changed its invoked job"
                )
        replace(receipt)

    @staticmethod
    def _validate_selected_direct_outer_receipt(
        receipt: M5DirectAttemptReturnReceipt,
    ) -> M5DirectNormalReturnReceipt | M5DirectLateReturnReceipt:
        if type(receipt) is not M5DirectAttemptReturnReceipt:
            raise ValidationError("selected direct return is not an outer receipt")
        if type(receipt.return_kind) is not M5TypedDirectReturnKind:
            raise ValidationError("selected direct return has another return kind")
        if (receipt.normal is None) == (receipt.late is None):
            raise ValidationError(
                "selected direct return requires exactly one outer branch"
            )

        branch: M5DirectNormalReturnReceipt | M5DirectLateReturnReceipt
        if receipt.normal is not None:
            branch = receipt.normal
            if type(branch) is not M5DirectNormalReturnReceipt:
                raise ValidationError("selected normal direct return has another type")
            observation = branch.observation_completion
            if receipt.return_kind is M5TypedDirectReturnKind.DISCOVERY:
                if observation is not None:
                    raise ValidationError(
                        "selected discovery return carries verifier observation"
                    )
            elif receipt.return_kind is M5TypedDirectReturnKind.VERIFIER:
                if type(observation) is not ObservationCompletionReceipt:
                    raise ValidationError(
                        "selected verifier return lacks its M4 observation receipt"
                    )
                if (
                    type(observation.artifact_stored) is not bool
                    or type(observation.made_effective) is not bool
                ):
                    raise ValidationError(
                        "selected verifier observation receipt is malformed"
                    )
                replace(observation)
            else:
                raise ValidationError("selected direct return kind is unsupported")
        else:
            late = receipt.late
            if type(late) is not M5DirectLateReturnReceipt:
                raise ValidationError("selected late direct return has another type")
            branch = late

        if type(branch.epoch_id) is not int:
            raise ValidationError("selected direct return has an invalid epoch")
        if type(branch.job_id) is not str or not branch.job_id:
            raise ValidationError("selected direct return has an invalid job ID")
        if type(branch.attempt_id) is not str or not branch.attempt_id:
            raise ValidationError("selected direct return has an invalid attempt ID")
        if type(branch.resulting_revision) is not int:
            raise ValidationError("selected direct return has an invalid revision")
        if type(branch.exact_replay) is not bool:
            raise ValidationError("selected direct return has an invalid replay flag")
        if type(branch.current_terminal_logical_result_hash) is not str:
            raise ValidationError("selected direct return lacks an exact terminal hash")

        string_fields = (
            (
                branch.execution_evidence_digest,
                branch.return_artifact_digest,
                branch.direct_transition_source_id,
                branch.direct_transition_source_identity_hash,
                branch.direct_transition_contribution_key_digest,
            )
            if isinstance(branch, M5DirectNormalReturnReceipt)
            else (
                branch.envelope_digest,
                branch.execution_evidence_digest,
            )
        )
        if any(type(value) is not str for value in string_fields):
            raise ValidationError("selected direct return has a nonexact string field")
        if (
            isinstance(branch, M5DirectLateReturnReceipt)
            and branch.expired_return_digest is not None
            and type(branch.expired_return_digest) is not str
        ):
            raise ValidationError("selected direct return has an invalid expired hash")

        anchor = branch.transition_anchor
        if anchor is not None:
            if type(anchor) is not M5TransitionTimingAnchor:
                raise ValidationError("selected direct return has another anchor type")
            replace(anchor)
        replace(branch)
        replace(receipt)
        return branch

    @staticmethod
    def _validate_external_work_failure(error: M5ExternalWorkFailure) -> None:
        if not isinstance(error, M5ExternalWorkFailure):
            raise ValidationError("provider raised another external failure type")
        if not isinstance(error.reason, M5RunFailureReason):
            raise ValidationError("external failure returned an invalid reason")
        if not isinstance(error.retryable, bool):
            raise ValidationError("external failure returned invalid retryability")
        if not isinstance(error.call_work, M5RuntimeWork):
            raise ValidationError("external failure returned invalid call work")
        if error.attempt_timing is not None and not isinstance(
            error.attempt_timing, M5RuntimeTiming
        ):
            raise ValidationError("external failure returned invalid attempt timing")
        if not isinstance(error.error_hash, str):
            raise ValidationError("external failure returned an invalid error hash")
        replace(error.call_work)
        if error.attempt_timing is not None:
            replace(error.attempt_timing)
        M5ExternalWorkFailure(
            error.reason,
            retryable=error.retryable,
            call_work=error.call_work,
            attempt_timing=error.attempt_timing,
            error_hash=error.error_hash,
        )

    @staticmethod
    def _validate_discovery_execution(
        epoch_id: int,
        declaration: M5RequirementRootDeclaration,
        lease: M5JobLease,
        manifest: M5CandidatePolicyManifest,
        event: M5TypedEventPlan,
        execution: M5DiscoveryExecution,
    ) -> None:
        if not isinstance(execution, M5DiscoveryExecution):
            raise ValidationError("discovery provider returned another execution type")
        if not isinstance(execution.result, M5RequirementDiscoveryResult):
            raise ValidationError("discovery provider returned another result type")
        if not isinstance(execution.attempt_output, M5AttemptOutput):
            raise ValidationError("discovery provider returned another attempt output")
        if not isinstance(execution.call_work, M5RuntimeWork):
            raise ValidationError("discovery provider returned invalid call work")
        if execution.attempt_timing is not None and not isinstance(
            execution.attempt_timing, M5RuntimeTiming
        ):
            raise ValidationError("discovery provider returned invalid attempt timing")
        for hit in execution.result.channel_hits:
            if not isinstance(hit, M5RequirementChannelHit):
                raise ValidationError("discovery result contains another hit type")
            if not isinstance(hit.pair, SemanticPairKey):
                raise ValidationError("discovery hit contains another pair type")
            replace(hit.pair)
            replace(hit)
        for selection in execution.result.selections:
            if not isinstance(selection, M5RequirementScopeSelection):
                raise ValidationError(
                    "discovery result contains another selection type"
                )
            if not isinstance(selection.pair, SemanticPairKey):
                raise ValidationError("discovery selection contains another pair type")
            replace(selection.pair)
            replace(selection)
        replace(execution.result)
        replace(execution.attempt_output)
        replace(execution.call_work)
        if execution.attempt_timing is not None:
            replace(execution.attempt_timing)
        replace(execution)
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
        if not isinstance(execution, M5VerifierExecution):
            raise ValidationError("verifier provider returned another execution type")
        if not isinstance(execution.pair_input, M5RequirementPairInput):
            raise ValidationError("verifier provider returned another pair input type")
        if not isinstance(execution.artifact, M5RequirementVerifierArtifact):
            raise ValidationError("verifier provider returned another artifact type")
        if not isinstance(execution.attempt_output, M5AttemptOutput):
            raise ValidationError("verifier provider returned another attempt output")
        if not isinstance(execution.call_work, M5RuntimeWork):
            raise ValidationError("verifier provider returned invalid call work")
        if execution.attempt_timing is not None and not isinstance(
            execution.attempt_timing, M5RuntimeTiming
        ):
            raise ValidationError("verifier provider returned invalid attempt timing")
        if not isinstance(execution.pair_input.pair, SemanticPairKey):
            raise ValidationError("verifier pair input contains another pair type")
        if not isinstance(execution.artifact.pair, SemanticPairKey):
            raise ValidationError("verifier artifact contains another pair type")
        replace(execution.pair_input.pair)
        replace(execution.artifact.pair)
        replace(execution.pair_input)
        replace(execution.artifact)
        replace(execution.attempt_output)
        replace(execution.call_work)
        if execution.attempt_timing is not None:
            replace(execution.attempt_timing)
        replace(execution)
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
        self._validate_active_open_receipt(opened, opened.epoch_id)
        if (
            not isinstance(reason, M5RunFailureReason)
            or reason is M5RunFailureReason.INVARIANT_FAILURE
        ):
            raise ValidationError("blocked result requires an exact nonterminal reason")
        if not isinstance(call_work, M5RuntimeWork):
            raise ValidationError("blocked result requires exact invocation work")
        replace(call_work)

        event_work = self.runtime_reads.current_event_work(opened.epoch_id)
        if not isinstance(event_work, M5RuntimeWork):
            raise ValidationError("blocked hydration returned invalid event work")
        replace(event_work)

        timing_result = self.runtime_reads.current_event_timing(opened.epoch_id)
        if not isinstance(timing_result, tuple) or len(timing_result) != 2:
            raise ValidationError("blocked hydration returned invalid timing tuple")
        event_timing, event_timing_coverage = timing_result
        if not isinstance(event_timing, M5RuntimeTiming) or not isinstance(
            event_timing_coverage, M5RuntimeTimingCoverage
        ):
            raise ValidationError("blocked hydration returned invalid timing values")
        replace(event_timing)
        replace(event_timing_coverage)
        event_timing_coverage.validate_aggregate(event_timing)
        if event_timing_coverage.terminal_client_roundtrip_included:
            raise ValidationError(
                "blocked event timing cannot include terminal client roundtrip"
            )
        call_timing_coverage = M5RuntimeTimingCoverage.single_point(
            None, terminal_client_roundtrip_included=False
        )
        return M5EventRunResult.build(
            event_id=event.structural_event_id,
            payload_hash=event.payload_hash,
            epoch_id=opened.epoch_id,
            state=M5RunState.BLOCKED,
            replayed_outcome=None,
            open_receipt=opened,
            publication_receipt=None,
            event_work=event_work,
            call_work=call_work,
            event_timing=event_timing,
            call_timing=M5RuntimeTiming(),
            combined_deltas=(),
            changed_state_references=(),
            failure_reason=reason,
            event_timing_coverage=event_timing_coverage,
            call_timing_coverage=call_timing_coverage,
        )

    def _fail(
        self,
        event: M5TypedEventPlan,
        opened: OpenEventReceipt,
        revision: int,
        reason: M5RunFailureReason,
        call_work: M5RuntimeWork,
        *,
        project_checked_requirement_replay: bool = False,
    ) -> M5EventRunResult:
        failed = self.runtime.fail_typed_epoch_atomically(
            opened.epoch_id, revision, reason, call_work
        )
        if failed.state is M5RunState.REPLAYED:
            if project_checked_requirement_replay:
                return self._finish_active_terminal_projection(
                    event,
                    opened,
                    call_work,
                    failed,
                    expected_outcome=M5ReplayedOutcome.FAILED,
                    expected_failure_reason=reason,
                )
            self._validate_terminal_replay(
                event,
                failed,
                expected_epoch_id=opened.epoch_id,
                expected_outcome=M5ReplayedOutcome.FAILED,
                expected_failure_reason=reason,
            )
            raise ValidationError(
                "generic failure replay lacks active-projection authority"
            )
        else:
            self._validate_terminal_result(
                event,
                opened.epoch_id,
                M5RunState.FAILED,
                failed,
                expected_open_receipt=opened,
            )
            if failed.failure_reason is not reason:
                raise ValidationError("failure result changed requested reason")
            if failed.call_work != call_work:
                raise ValidationError("failure result changed invocation call work")
        return self._finish_terminal_invocation(event, failed)

    @classmethod
    def _validate_terminal_replay(
        cls,
        event: M5TypedEventPlan,
        result: M5EventRunResult,
        *,
        expected_epoch_id: int | None = None,
        expected_outcome: M5ReplayedOutcome | None = None,
        expected_failure_reason: M5RunFailureReason | None = None,
        expected_logical_result_hash: str | None = None,
    ) -> None:
        if (
            type(result) is not M5EventRunResult
            or type(result.event_id) is not str
            or result.event_id != event.structural_event_id
            or type(result.payload_hash) is not str
            or result.payload_hash != event.payload_hash
            or type(result.epoch_id) is not int
            or result.epoch_id < 1
            or (expected_epoch_id is not None and result.epoch_id != expected_epoch_id)
            or result.state is not M5RunState.REPLAYED
            or type(result.logical_result_hash) is not str
            or type(result.event_work) is not M5RuntimeWork
            or type(result.call_work) is not M5RuntimeWork
            or not result.call_work.is_zero
            or type(result.open_receipt) is not OpenEventReceipt
            or result.open_receipt.epoch_id != result.epoch_id
        ):
            raise ValidationError("durable typed replay has an invalid envelope")

        opened = result.open_receipt
        cls._validate_open_event_receipt(opened)
        if result.replayed_outcome is M5ReplayedOutcome.SEALED:
            publication = result.publication_receipt
            if (
                type(publication) is not PublicationReceipt
                or result.failure_reason is not None
                or opened.replayed is not True
                or opened.already_sealed is not True
                or opened.publication_id != publication.publication_id
                or opened.already_failed is not False
                or opened.failure_reason is not None
            ):
                raise ValidationError("durable sealed replay shape is invalid")
            cls._validate_publication_receipt(
                publication,
                expected_epoch_id=result.epoch_id,
                expected_replayed=True,
            )
        elif result.replayed_outcome is M5ReplayedOutcome.FAILED:
            failure = result.failure_reason
            if (
                result.publication_receipt is not None
                or type(failure) is not M5RunFailureReason
                or failure is M5RunFailureReason.WORK_IN_PROGRESS
                or opened.replayed is not True
                or opened.already_sealed is not False
                or opened.publication_id is not None
                or opened.already_failed is not True
                or opened.failure_reason != failure.value
            ):
                raise ValidationError("durable failed replay shape is invalid")
        else:
            raise ValidationError("durable replay lacks its exact terminal outcome")

        cls._validate_terminal_nested_contracts(result)
        if (
            expected_outcome is not None
            and result.replayed_outcome is not expected_outcome
        ):
            raise ValidationError("durable replay has another terminal outcome")
        if (
            expected_failure_reason is not None
            and result.failure_reason is not expected_failure_reason
        ):
            raise ValidationError("durable replay changed requested failure reason")
        if (
            expected_logical_result_hash is not None
            and result.logical_result_hash != expected_logical_result_hash
        ):
            raise ValidationError("durable replay changed expected logical result hash")
        cls._validate_terminal_logical_result_hash(result)

    @classmethod
    def _validate_terminal_result(
        cls,
        event: M5TypedEventPlan,
        epoch_id: int,
        expected_state: M5RunState,
        result: M5EventRunResult,
        *,
        expected_open_receipt: OpenEventReceipt,
    ) -> None:
        if (
            type(result) is not M5EventRunResult
            or expected_state not in {M5RunState.SEALED, M5RunState.FAILED}
            or type(epoch_id) is not int
            or epoch_id < 1
            or type(result.event_id) is not str
            or result.event_id != event.structural_event_id
            or type(result.payload_hash) is not str
            or result.payload_hash != event.payload_hash
            or result.epoch_id != epoch_id
            or result.state is not expected_state
            or result.replayed_outcome is not None
            or type(result.open_receipt) is not OpenEventReceipt
            or result.open_receipt.epoch_id != epoch_id
            or type(result.open_receipt.replayed) is not bool
            or result.open_receipt.already_sealed is not False
            or result.open_receipt.publication_id is not None
            or result.open_receipt.already_failed is not False
            or result.open_receipt.failure_reason is not None
            or result.open_receipt != expected_open_receipt
            or type(result.logical_result_hash) is not str
            or type(result.event_work) is not M5RuntimeWork
            or type(result.call_work) is not M5RuntimeWork
        ):
            raise ValidationError("typed terminal result has another envelope")
        cls._validate_terminal_nested_contracts(result)
        cls._validate_open_event_receipt(result.open_receipt)
        if expected_state is M5RunState.SEALED:
            publication = result.publication_receipt
            if (
                type(publication) is not PublicationReceipt
                or result.failure_reason is not None
            ):
                raise ValidationError("typed sealed result has an invalid outcome")
            cls._validate_publication_receipt(
                publication,
                expected_epoch_id=epoch_id,
                expected_replayed=False,
            )
        elif (
            result.publication_receipt is not None
            or type(result.failure_reason) is not M5RunFailureReason
            or result.failure_reason is M5RunFailureReason.WORK_IN_PROGRESS
        ):
            raise ValidationError("typed failed result has an invalid outcome")
        cls._validate_terminal_logical_result_hash(result)

    @staticmethod
    def _validate_publication_receipt(
        publication: PublicationReceipt,
        *,
        expected_epoch_id: int,
        expected_replayed: bool,
    ) -> None:
        if (
            type(publication) is not PublicationReceipt
            or type(publication.epoch_id) is not int
            or publication.epoch_id < 1
            or publication.epoch_id != expected_epoch_id
            or type(publication.publication_id) is not str
            or publication.publication_id
            != stable_m4_digest("m4-publication-v1", str(expected_epoch_id))
            or type(publication.replayed) is not bool
            or publication.replayed is not expected_replayed
        ):
            raise ValidationError("terminal publication receipt is invalid")
        replace(publication)

    @staticmethod
    def _validate_exact_runtime_timing(timing: M5RuntimeTiming) -> None:
        if type(timing) is not M5RuntimeTiming:
            raise ValidationError("terminal timing must be exact M5RuntimeTiming")
        for descriptor in fields(M5RuntimeTiming):
            value = getattr(timing, descriptor.name)
            if value is not None and type(value) is not int:
                raise ValidationError("terminal timing contains a nonexact counter")
        replace(timing)

    @staticmethod
    def _validate_exact_timing_coverage(
        coverage: M5RuntimeTimingCoverage,
    ) -> None:
        if type(coverage) is not M5RuntimeTimingCoverage:
            raise ValidationError(
                "terminal timing coverage must be exact M5RuntimeTimingCoverage"
            )
        for descriptor in fields(M5RuntimeTimingCoverage):
            value = getattr(coverage, descriptor.name)
            expected_type = (
                bool if descriptor.name == "terminal_client_roundtrip_included" else int
            )
            if type(value) is not expected_type:
                raise ValidationError(
                    "terminal timing coverage contains a nonexact value"
                )
        replace(coverage)

    @classmethod
    def _validate_terminal_nested_contracts(cls, result: M5EventRunResult) -> None:
        if (
            type(result) is not M5EventRunResult
            or type(result.event_id) is not str
            or type(result.payload_hash) is not str
            or type(result.epoch_id) is not int
            or (
                result.logical_result_hash is not None
                and type(result.logical_result_hash) is not str
            )
            or type(result.open_receipt) is not OpenEventReceipt
            or type(result.event_work) is not M5RuntimeWork
            or type(result.call_work) is not M5RuntimeWork
            or type(result.event_timing) is not M5RuntimeTiming
            or type(result.call_timing) is not M5RuntimeTiming
            or type(result.event_timing_coverage) is not M5RuntimeTimingCoverage
            or type(result.call_timing_coverage) is not M5RuntimeTimingCoverage
        ):
            raise ValidationError("terminal result requires complete timing coverage")

        cls._validate_open_event_receipt(result.open_receipt)
        publication = result.publication_receipt
        if publication is not None:
            if (
                type(publication) is not PublicationReceipt
                or type(publication.epoch_id) is not int
                or type(publication.publication_id) is not str
                or type(publication.replayed) is not bool
            ):
                raise ValidationError(
                    "terminal result contains another publication receipt"
                )
            replace(publication)

        _require_tuple("terminal combined deltas", result.combined_deltas)
        _require_tuple(
            "terminal changed-state references", result.changed_state_references
        )
        for delta in result.combined_deltas:
            if type(delta) is not StatusDelta or any(
                type(getattr(delta, name)) is not str
                for name in (
                    "event_id",
                    "object_type",
                    "object_id",
                    "old_status",
                    "new_status",
                    "reason",
                )
            ):
                raise ValidationError("terminal result contains another delta type")
            replace(delta)
        validate_combined_deltas(result.combined_deltas, result.event_id)
        for reference in result.changed_state_references:
            if (
                type(reference) is not M5ChangedStateReference
                or type(reference.kind) is not M5StateReferenceKind
                or type(reference.object_id) is not str
                or type(reference.epoch_id) is not int
                or type(reference.revision) is not int
                or type(reference.state_artifact_hash) is not str
                or type(reference.reference_digest) is not str
            ):
                raise ValidationError(
                    "terminal result contains another state-reference type"
                )
            replace(reference)
        validate_changed_state_references(result.changed_state_references)

        event_coverage = result.event_timing_coverage
        call_coverage = result.call_timing_coverage
        assert event_coverage is not None
        assert call_coverage is not None
        _validate_exact_runtime_work(result.event_work)
        _validate_exact_runtime_work(result.call_work)
        cls._validate_exact_runtime_timing(result.event_timing)
        cls._validate_exact_runtime_timing(result.call_timing)
        cls._validate_exact_timing_coverage(event_coverage)
        cls._validate_exact_timing_coverage(call_coverage)
        event_coverage.validate_aggregate(result.event_timing)
        call_coverage.validate_aggregate(result.call_timing)
        if event_coverage.terminal_client_roundtrip_included is not False:
            raise ValidationError(
                "durable event timing cannot include terminal client roundtrip"
            )
        if call_coverage.required_expected_count != 1:
            raise ValidationError(
                "terminal call timing coverage must describe exactly one point"
            )
        expected_roundtrip = call_coverage.required_observed_count == 1
        if call_coverage.terminal_client_roundtrip_included is not expected_roundtrip:
            raise ValidationError(
                "terminal roundtrip flag disagrees with call timing coverage"
            )
        replace(result)

    @staticmethod
    def _validate_terminal_logical_result_hash(result: M5EventRunResult) -> None:
        if (
            type(result) is not M5EventRunResult
            or type(result.logical_result_hash) is not str
            or result.logical_result_hash != result.expected_logical_result_hash
        ):
            raise ValidationError("terminal logical result hash is invalid")


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
    "M5RuntimeMeasurementPort",
    "M5TerminalInvocationTelemetry",
    "M5TypedApplication",
    "M5TypedStructuralPort",
    "M5VerifierExecution",
]
