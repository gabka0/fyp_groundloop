"""Pure application evidence for the activated M5-D24 R2b lane.

The tests deliberately use the transaction-aware fake ports.  They exercise
the application boundary only; they are not PostgreSQL or provider evidence.
"""

from __future__ import annotations

import inspect
from collections.abc import Iterator
from copy import copy, deepcopy
from dataclasses import dataclass, replace
from typing import cast

import pytest
from m5.runtime.fake_ports import (
    FAKE_LEASE_BASE,
    FAKE_OBSERVED_TIMING,
    FakeDirect,
    FakeDirectInterruption,
    FakeDirectSelectedOutcome,
    FakeDiscovery,
    FakeDiscoveryOutcome,
    FakeHarness,
    FakeMeasurements,
    FakeRuntime,
    FakeStructural,
    FakeVerifier,
    FakeVerifierOutcome,
    make_harness,
    make_repository,
    make_typed_plan,
    sha,
)

from groundloop.domain import SubjectKind
from groundloop.errors import EventConflictError, ValidationError
from groundloop.events import ChunkInput, InsertDocumentEvent, apply_event
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
    LogicalJobSpec as M4LogicalJobSpec,
)
from groundloop.m4.contracts import stable_m4_digest
from groundloop.m4.pipeline import StructuralPayload
from groundloop.m5.domain import (
    ConstructionKind,
    EvidenceGroupVersion,
    EvidenceRequirementVersion,
)
from groundloop.m5.events import RegisterGroupEvent
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.application import (
    M5DirectExecutionReceipt,
    M5DiscoveryExecution,
    M5ExternalWorkFailure,
    M5RequirementRootDeclaration,
    M5TerminalInvocationTelemetry,
    M5VerifierExecution,
)
from groundloop.m5.runtime.contracts import (
    M5AcquisitionDisposition,
    M5AttemptOutput,
    M5CandidatePolicyManifest,
    M5ChangedStateReference,
    M5DirectAttemptReturnReceipt,
    M5DirectCursorContributionReceipt,
    M5DiscoveryDirection,
    M5EventRunResult,
    M5ExecutionEvidenceDisposition,
    M5JobAttempt,
    M5JobKind,
    M5JobLease,
    M5JobState,
    M5LeaseTerminalProjection,
    M5LogicalJobSpec,
    M5ReplayedOutcome,
    M5RequirementAttemptReturnReceipt,
    M5RequirementChannelHit,
    M5RequirementDiscoveryResult,
    M5RequirementPairInput,
    M5RequirementReturnDisposition,
    M5RequirementVerifierArtifact,
    M5RequirementWithdrawalPlan,
    M5RootBarrierReceipt,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeTiming,
    M5RuntimeTimingCoverage,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TerminalReason,
    M5TransitionTimingAnchor,
    M5TransitionTimingReceipt,
    M5TypedDirectReturnKind,
    M5TypedEventPlan,
    SemanticPairKey,
)


class SyntheticCrash(RuntimeError):
    """An injected process loss after an already committed fake transition."""


def _group(
    *,
    group_id: str,
    requirement_id: str,
) -> EvidenceGroupVersion:
    requirement = EvidenceRequirementVersion(
        requirement_version_id=requirement_id,
        group_version_id=group_id,
        ordinal=0,
        requirement_text=f"evidence needed for {requirement_id}",
        supersedes_requirement_version_id=None,
    )
    return EvidenceGroupVersion(
        group_version_id=group_id,
        group_family_id=f"family:{group_id}",
        owner_claim_id="claim-required",
        requirements=(requirement,),
        construction_kind=ConstructionKind.CONTROLLED,
        construction_source_id="d24-r2b-tests-v1",
        supersedes_group_version_id=None,
    )


def _register_case(
    name: str,
    *,
    with_pair: bool,
) -> tuple[FakeHarness, M5TypedEventPlan, SemanticPairKey | None]:
    repository = make_repository()
    chunk_id = f"chunk:{name}"
    apply_event(
        repository.base,
        InsertDocumentEvent(
            event_id=f"seed:{name}",
            document_id=f"document:{name}",
            document_version_id=f"version:{name}",
            content_hash=sha(f"content:{name}"),
            chunks=(ChunkInput(chunk_id, 0, f"evidence for {name}"),),
        ),
    )
    harness = make_harness(repository)
    requirement_id = f"requirement:{name}"
    event = RegisterGroupEvent(
        f"event:{name}",
        _group(group_id=f"group:{name}", requirement_id=requirement_id),
    )
    plan = make_typed_plan(harness.world, event)
    pair = (
        SemanticPairKey(SubjectKind.REQUIREMENT, requirement_id, chunk_id)
        if with_pair
        else None
    )
    harness.world.discovery_outcomes[
        (event.event_id, M5DiscoveryDirection.FORWARD_REQUIREMENT, requirement_id)
    ] = [FakeDiscoveryOutcome(() if pair is None else (pair,))]
    return harness, plan, pair


def _register_two_requirement_case(
    name: str,
) -> tuple[FakeHarness, M5TypedEventPlan]:
    repository = make_repository()
    chunk_id = f"chunk:{name}"
    apply_event(
        repository.base,
        InsertDocumentEvent(
            event_id=f"seed:{name}",
            document_id=f"document:{name}",
            document_version_id=f"version:{name}",
            content_hash=sha(f"content:{name}"),
            chunks=(ChunkInput(chunk_id, 0, f"evidence for {name}"),),
        ),
    )
    harness = make_harness(repository)
    group_id = f"group:{name}"
    requirements = tuple(
        EvidenceRequirementVersion(
            requirement_version_id=f"requirement:{name}:{ordinal}",
            group_version_id=group_id,
            ordinal=ordinal,
            requirement_text=f"evidence needed for {name} {ordinal}",
            supersedes_requirement_version_id=None,
        )
        for ordinal in range(2)
    )
    event = RegisterGroupEvent(
        f"event:{name}",
        EvidenceGroupVersion(
            group_version_id=group_id,
            group_family_id=f"family:{group_id}",
            owner_claim_id="claim-required",
            requirements=requirements,
            construction_kind=ConstructionKind.CONTROLLED,
            construction_source_id="d24-r2b-tests-v1",
            supersedes_group_version_id=None,
        ),
    )
    return harness, make_typed_plan(harness.world, event)


def _requirement_declarations(
    harness: FakeHarness, plan: M5TypedEventPlan
) -> tuple[M5RequirementRootDeclaration, ...]:
    withdrawal = harness.structural.plan_exact_requirement_withdrawal(plan)
    return harness.application._requirement_roots(  # noqa: SLF001
        plan, harness.world.manifest, withdrawal
    )


def _set_first_discovery_work(
    harness: FakeHarness,
    plan: M5TypedEventPlan,
    *,
    nonzero: bool,
    pair: SemanticPairKey | None,
) -> str:
    first = _requirement_declarations(harness, plan)[0]
    requirement_id = first.scope.requirement_version_id
    assert requirement_id is not None
    harness.world.discovery_outcomes[
        (plan.structural_event_id, _forward_direction(), requirement_id)
    ] = [
        FakeDiscoveryOutcome(
            pairs=() if pair is None else (pair,),
            execution_disposition=(
                M5ExecutionEvidenceDisposition.RETURNED
                if nonzero
                else M5ExecutionEvidenceDisposition.REUSED_ARTIFACT
            ),
        )
    ]
    return first.job.logical_job_id


def _prepare_held_receipt(
    harness: FakeHarness,
    plan: M5TypedEventPlan,
    *,
    resumed: bool,
) -> None:
    if not resumed:
        return
    crashing = _CrashDiscoveryOnce(harness.world)
    harness.discovery = crashing
    harness.application.discovery = crashing
    with pytest.raises(SyntheticCrash):
        harness.application.run_event(plan)
    first = _requirement_declarations(harness, plan)[0]
    harness.world.takeover_jobs_once.add(first.job.logical_job_id)


def _expected_discovery_work(*, nonzero: bool) -> M5RuntimeWork:
    if not nonzero:
        return M5RuntimeWork()
    return M5RuntimeWork(
        requirement_forward_retrieval_call_count=1,
        embedding_model_call_count=1,
    )


_FROZEN_RESULT_FIELDS = (
    "event_id",
    "payload_hash",
    "epoch_id",
    "replayed_outcome",
    "publication_receipt",
    "event_work",
    "event_timing",
    "event_timing_coverage",
    "combined_deltas",
    "changed_state_references",
    "failure_reason",
    "logical_result_hash",
)


def _assert_active_projection_and_reconnect(
    harness: FakeHarness,
    plan: M5TypedEventPlan,
    result: M5EventRunResult,
    *,
    resumed: bool,
    expected_work: M5RuntimeWork,
    origin_marker: str,
) -> None:
    canonical = harness.runtime.read_typed_event_result(
        plan.structural_event_id, plan.payload_hash
    )
    assert canonical is not None
    assert canonical.state is M5RunState.REPLAYED
    assert canonical.call_work.is_zero
    assert canonical.open_receipt.replayed
    assert canonical.open_receipt.already_sealed is (
        canonical.replayed_outcome is M5ReplayedOutcome.SEALED
    )
    assert canonical.open_receipt.already_failed is (
        canonical.replayed_outcome is M5ReplayedOutcome.FAILED
    )
    assert result.state is M5RunState.REPLAYED
    assert result.open_receipt.replayed is resumed
    assert not result.open_receipt.already_sealed
    assert not result.open_receipt.already_failed
    assert result.open_receipt.publication_id is None
    assert result.open_receipt.failure_reason is None
    assert result.call_work == expected_work
    for field_name in _FROZEN_RESULT_FIELDS:
        assert getattr(result, field_name) == getattr(canonical, field_name)
    assert len(harness.world.terminal_telemetry) == 1
    assert all(
        len(payload) == 5
        and not any(isinstance(value, M5RuntimeWork) for value in payload)
        for payload in harness.world.terminal_telemetry.values()
    )
    log = harness.world.operation_log
    origin_index = max(
        index for index, marker in enumerate(log) if marker == origin_marker
    )
    measure_index = max(
        index
        for index, marker in enumerate(log)
        if marker.startswith("measure-terminal:")
    )
    telemetry_index = max(
        index
        for index, marker in enumerate(log)
        if marker.startswith("terminal-telemetry:")
    )
    assert origin_index < measure_index < telemetry_index

    external_calls = harness.world.external_call_count
    telemetry_count = len(harness.world.terminal_telemetry)
    reconnect = harness.application.run_event(plan)
    assert reconnect.state is M5RunState.REPLAYED
    assert reconnect.open_receipt.replayed
    assert reconnect.open_receipt.already_sealed is (
        reconnect.replayed_outcome is M5ReplayedOutcome.SEALED
    )
    assert reconnect.open_receipt.already_failed is (
        reconnect.replayed_outcome is M5ReplayedOutcome.FAILED
    )
    assert reconnect.call_work.is_zero
    assert harness.world.external_call_count == external_calls
    assert len(harness.world.terminal_telemetry) == telemetry_count + 1


def _forward_direction() -> M5DiscoveryDirection:
    return M5DiscoveryDirection.FORWARD_REQUIREMENT


def _root_job_id(harness: FakeHarness, plan: M5TypedEventPlan) -> str:
    withdrawal = harness.structural.plan_exact_requirement_withdrawal(plan)
    roots = harness.application._requirement_roots(  # noqa: SLF001
        plan, harness.world.manifest, withdrawal
    )
    assert len(roots) == 1
    return roots[0].job.logical_job_id


def _install_runtime(harness: FakeHarness, runtime: FakeRuntime) -> None:
    harness.runtime = runtime
    harness.application.runtime = runtime
    harness.application.runtime_reads = runtime


@dataclass(slots=True)
class _CrashBarrierOnceRuntime(FakeRuntime):
    crash_once: bool = True

    def close_m5_requirement_roots_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        requirement_root_set_hash: str,
    ) -> M5RootBarrierReceipt:
        if self.crash_once:
            self.crash_once = False
            self.world.operation_log.append("crash:before-root-barrier")
            raise SyntheticCrash("lost after root result, before barrier")
        return FakeRuntime.close_m5_requirement_roots_atomically(
            self, epoch_id, expected_revision, requirement_root_set_hash
        )


@dataclass(slots=True)
class _CrashEpochFailureOnceRuntime(FakeRuntime):
    crash_once: bool = True

    def fail_typed_epoch_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        failure_reason: M5RunFailureReason,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        if self.crash_once:
            self.crash_once = False
            self.world.operation_log.append("crash:before-epoch-failure")
            raise SyntheticCrash("lost after terminal attempt settlement")
        return FakeRuntime.fail_typed_epoch_atomically(
            self, epoch_id, expected_revision, failure_reason, call_work
        )


@dataclass(slots=True)
class _CrashDiscoveryOnce(FakeDiscovery):
    crash_once: bool = True

    def discover_requirement_scope(
        self,
        epoch_id: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        manifest: M5CandidatePolicyManifest,
        event: M5TypedEventPlan,
    ) -> M5DiscoveryExecution:
        if self.crash_once:
            self.crash_once = False
            self.world.operation_log.append("crash:after-acquisition")
            raise SyntheticCrash("lost after durable acquisition")
        return FakeDiscovery.discover_requirement_scope(
            self, epoch_id, lease, job, manifest, event
        )


@dataclass(slots=True)
class _CorruptNestedDiscovery(FakeDiscovery):
    def discover_requirement_scope(
        self,
        epoch_id: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        manifest: M5CandidatePolicyManifest,
        event: M5TypedEventPlan,
    ) -> M5DiscoveryExecution:
        execution = FakeDiscovery.discover_requirement_scope(
            self, epoch_id, lease, job, manifest, event
        )
        assert execution.result.channel_hits
        first_hit = execution.result.channel_hits[0]
        corrupted_hit = cast(
            M5RequirementChannelHit,
            _unsafe_set(first_hit, score=float("nan")),
        )
        corrupted_result = cast(
            M5RequirementDiscoveryResult,
            _unsafe_set(
                execution.result,
                channel_hits=(corrupted_hit, *execution.result.channel_hits[1:]),
            ),
        )
        return cast(
            M5DiscoveryExecution,
            _unsafe_set(execution, result=corrupted_result),
        )


@dataclass(slots=True)
class _CorruptExternalFailureDiscovery(FakeDiscovery):
    def discover_requirement_scope(
        self,
        epoch_id: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        manifest: M5CandidatePolicyManifest,
        event: M5TypedEventPlan,
    ) -> M5DiscoveryExecution:
        try:
            return FakeDiscovery.discover_requirement_scope(
                self, epoch_id, lease, job, manifest, event
            )
        except M5ExternalWorkFailure as error:
            error.error_hash = "invalid"
            raise


@dataclass(slots=True)
class _CorruptNestedVerifier(FakeVerifier):
    def verify_requirement_pair(
        self,
        epoch_id: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        manifest: M5CandidatePolicyManifest,
        event: M5TypedEventPlan,
    ) -> M5VerifierExecution:
        execution = FakeVerifier.verify_requirement_pair(
            self, epoch_id, lease, job, manifest, event
        )
        corrupted_artifact = cast(
            M5RequirementVerifierArtifact,
            _unsafe_set(
                execution.artifact,
                support_score=float("nan"),
            ),
        )
        return cast(
            M5VerifierExecution,
            _unsafe_set(execution, artifact=corrupted_artifact),
        )


@dataclass(slots=True)
class _ZeroWorkDirectFailure(FakeDirect):
    def run_pending_direct(
        self, epoch_id: int, expected_revision: int, event: M5TypedEventPlan
    ) -> M5DirectExecutionReceipt:
        return M5DirectExecutionReceipt(
            expected_revision,
            M5RuntimeWork(),
            terminal_failure_reason=M5RunFailureReason.RETRIEVAL_ERROR,
        )


@dataclass(slots=True)
class _CorruptStructuralOpen(FakeStructural):
    mode: str = "epoch_bool"

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
    ) -> OpenEventReceipt:
        opened = FakeStructural.open_typed_event_atomically(
            self,
            event,
            direct_payload,
            direct_withdrawal,
            requirement_withdrawal,
            direct_roots,
            direct_scopes,
            requirement_roots,
            requirement_root_set_hash,
        )
        if self.mode == "epoch_bool":
            return _unsafe_set(opened, epoch_id=True)  # type: ignore[return-value]
        if self.mode == "receipt_subclass":
            return _EqualitySpoofingOpenReceipt(
                epoch_id=opened.epoch_id,
                replayed=opened.replayed,
                already_sealed=opened.already_sealed,
                publication_id=opened.publication_id,
                already_failed=opened.already_failed,
                failure_reason=opened.failure_reason,
            )
        return _unsafe_set(opened, replayed=1)  # type: ignore[return-value]


@dataclass(slots=True)
class _CorruptRootBarrierRuntime(FakeRuntime):
    def close_m5_requirement_roots_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        requirement_root_set_hash: str,
    ) -> M5RootBarrierReceipt:
        receipt = FakeRuntime.close_m5_requirement_roots_atomically(
            self, epoch_id, expected_revision, requirement_root_set_hash
        )
        return _unsafe_set(receipt, exact_replay=1)  # type: ignore[return-value]


@dataclass(slots=True)
class _CorruptTransitionTimingRuntime(FakeRuntime):
    def append_transition_call_timing(
        self,
        epoch_id: int,
        contribution_kind: M5RuntimeWorkContributionKind,
        source_id: str,
        contribution_key_digest: str,
        anchor_revision: int,
        observed_timing: M5RuntimeTiming | None,
    ) -> M5TransitionTimingReceipt:
        receipt = FakeRuntime.append_transition_call_timing(
            self,
            epoch_id,
            contribution_kind,
            source_id,
            contribution_key_digest,
            anchor_revision,
            observed_timing,
        )
        return _unsafe_set(receipt, exact_replay=1)  # type: ignore[return-value]


@dataclass(slots=True)
class _CorruptBlockedReadRuntime(FakeRuntime):
    def current_event_work(self, epoch_id: int) -> M5RuntimeWork:
        current = FakeRuntime.current_event_work(self, epoch_id)
        return cast(
            M5RuntimeWork,
            _unsafe_set(current, requirement_cancelled_job_count=-1),
        )


@dataclass(slots=True)
class _CorruptTerminalMeasurements(FakeMeasurements):
    def terminal_invocation(
        self, event: M5TypedEventPlan, result: M5EventRunResult
    ) -> M5TerminalInvocationTelemetry:
        measurement = FakeMeasurements.terminal_invocation(self, event, result)
        return _unsafe_set(  # type: ignore[return-value]
            measurement, invocation_id=""
        )


@dataclass(slots=True)
class _MutatingTerminalMeasurements(FakeMeasurements):
    mode: str = "open_receipt"

    def terminal_invocation(
        self, event: M5TypedEventPlan, result: M5EventRunResult
    ) -> M5TerminalInvocationTelemetry:
        telemetry = FakeMeasurements.terminal_invocation(self, event, result)
        if self.mode == "open_receipt":
            object.__setattr__(
                result.open_receipt,
                "replayed",
                not result.open_receipt.replayed,
            )
        elif self.mode == "call_work":
            object.__setattr__(
                result,
                "call_work",
                M5RuntimeWork(bytes_hashed=result.call_work.bytes_hashed + 1),
            )
        elif self.mode == "event_id":
            object.__setattr__(event, "structural_event_id", "changed-event-id")
        else:
            raise AssertionError(f"unknown measurement mutation mode: {self.mode}")
        return telemetry


def _unsafe_replayed_outcome_none(result: M5EventRunResult) -> M5EventRunResult:
    malformed = copy(result)
    object.__setattr__(malformed, "replayed_outcome", None)
    return malformed


@dataclass(slots=True)
class _TerminalCutoffRuntime(FakeRuntime):
    target: str = "discovery"
    disposition: M5RequirementReturnDisposition = (
        M5RequirementReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL
    )
    read_mode: str = "canonical"
    cutoff_done: bool = False
    canonical_read: M5EventRunResult | None = None

    def _cutoff(self, epoch_id: int, job: M5LogicalJobSpec) -> None:
        epoch = self.world.epoch(epoch_id)
        terminal = FakeRuntime.fail_typed_epoch_atomically(
            self,
            epoch_id,
            epoch.revision,
            M5RunFailureReason.RETRIEVAL_ERROR,
            M5RuntimeWork(),
        )
        assert terminal.logical_result_hash is not None
        self.world.return_dispositions_by_job[job.logical_job_id] = [self.disposition]
        self.cutoff_done = True
        self.world.operation_log.append(f"cutoff:{self.target}")

    def _projected_read(self) -> M5EventRunResult | None:
        epoch = next(iter(self.world.epochs_by_event.values()))
        canonical = FakeRuntime.read_typed_event_result(
            self, epoch.plan.structural_event_id, epoch.plan.payload_hash
        )
        assert canonical is not None
        if self.read_mode == "missing":
            return None
        if self.read_mode == "wrong_event":
            canonical = M5EventRunResult.build(
                event_id="wrong-event",
                payload_hash=canonical.payload_hash,
                epoch_id=canonical.epoch_id,
                state=canonical.state,
                replayed_outcome=canonical.replayed_outcome,
                open_receipt=canonical.open_receipt,
                publication_receipt=canonical.publication_receipt,
                event_work=canonical.event_work,
                call_work=canonical.call_work,
                event_timing=canonical.event_timing,
                call_timing=canonical.call_timing,
                combined_deltas=canonical.combined_deltas,
                changed_state_references=canonical.changed_state_references,
                failure_reason=canonical.failure_reason,
                event_timing_coverage=canonical.event_timing_coverage,
                call_timing_coverage=canonical.call_timing_coverage,
            )
        elif self.read_mode == "wrong_payload":
            canonical = M5EventRunResult.build(
                event_id=canonical.event_id,
                payload_hash=sha("wrong-payload"),
                epoch_id=canonical.epoch_id,
                state=canonical.state,
                replayed_outcome=canonical.replayed_outcome,
                open_receipt=canonical.open_receipt,
                publication_receipt=canonical.publication_receipt,
                event_work=canonical.event_work,
                call_work=canonical.call_work,
                event_timing=canonical.event_timing,
                call_timing=canonical.call_timing,
                combined_deltas=canonical.combined_deltas,
                changed_state_references=canonical.changed_state_references,
                failure_reason=canonical.failure_reason,
                event_timing_coverage=canonical.event_timing_coverage,
                call_timing_coverage=canonical.call_timing_coverage,
            )
        elif self.read_mode == "wrong_epoch":
            wrong_epoch = canonical.epoch_id + 10
            assert canonical.failure_reason is not None
            opened = OpenEventReceipt(
                wrong_epoch,
                True,
                False,
                already_failed=True,
                failure_reason=canonical.failure_reason.value,
            )
            canonical = M5EventRunResult.build(
                event_id=canonical.event_id,
                payload_hash=canonical.payload_hash,
                epoch_id=wrong_epoch,
                state=canonical.state,
                replayed_outcome=canonical.replayed_outcome,
                open_receipt=opened,
                publication_receipt=None,
                event_work=canonical.event_work,
                call_work=canonical.call_work,
                event_timing=canonical.event_timing,
                call_timing=canonical.call_timing,
                combined_deltas=canonical.combined_deltas,
                changed_state_references=canonical.changed_state_references,
                failure_reason=canonical.failure_reason,
                event_timing_coverage=canonical.event_timing_coverage,
                call_timing_coverage=canonical.call_timing_coverage,
            )
        elif self.read_mode == "active_shape":
            canonical = replace(
                canonical,
                open_receipt=OpenEventReceipt(canonical.epoch_id, False, False),
            )
        elif self.read_mode == "malformed_outcome":
            canonical = _unsafe_replayed_outcome_none(canonical)
        elif self.read_mode == "coverage_none":
            canonical = cast(
                M5EventRunResult,
                _unsafe_set(
                    canonical,
                    event_timing_coverage=None,
                    call_timing_coverage=None,
                ),
            )
        self.canonical_read = canonical
        return canonical

    def _receipt_after_cutoff(
        self, receipt: M5RequirementAttemptReturnReceipt
    ) -> M5RequirementAttemptReturnReceipt:
        projected_hash: str | None
        if self.read_mode == "hash_mismatch":
            projected_hash = sha("receipt-hash-mismatch")
        elif self.read_mode == "missing":
            epoch = next(iter(self.world.epochs_by_event.values()))
            assert epoch.terminal_result is not None
            projected_hash = epoch.terminal_result.logical_result_hash
        else:
            projected = self._projected_read()
            assert projected is not None
            projected_hash = projected.logical_result_hash
        assert projected_hash is not None
        projected_receipt = replace(
            receipt,
            current_terminal_logical_result_hash=projected_hash,
            transition_anchor=None,
            resulting_revision=self.world.epoch(
                next(iter(self.world.epochs_by_event.values())).epoch_id
            ).revision,
        )
        if self.read_mode == "malformed_receipt":
            return _unsafe_set(  # type: ignore[return-value]
                projected_receipt,
                disposition=M5RequirementReturnDisposition.APPLIED,
                exact_replay=False,
                transition_anchor=None,
            )
        return projected_receipt

    def read_typed_event_result(
        self, event_id: str, payload_hash: str
    ) -> M5EventRunResult | None:
        if self.cutoff_done:
            self.world.operation_log.append("read:cutoff-canonical")
            if self.canonical_read is not None:
                return self.canonical_read
            return self._projected_read()
        return FakeRuntime.read_typed_event_result(self, event_id, payload_hash)

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
    ) -> M5RequirementAttemptReturnReceipt:
        if self.target != "discovery":
            return FakeRuntime.stage_m5_discovery_result_atomically(
                self,
                epoch_id,
                expected_revision,
                lease,
                job,
                result,
                attempt_output,
                execution_disposition,
                attempt_work,
                attempt_timing,
                eligible_snapshot_exhausted=eligible_snapshot_exhausted,
            )
        self._cutoff(epoch_id, job)
        receipt = FakeRuntime.stage_m5_discovery_result_atomically(
            self,
            epoch_id,
            self.world.epoch(epoch_id).revision,
            lease,
            job,
            result,
            attempt_output,
            execution_disposition,
            attempt_work,
            attempt_timing,
            eligible_snapshot_exhausted=eligible_snapshot_exhausted,
        )
        return self._receipt_after_cutoff(receipt)

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
    ) -> M5RequirementAttemptReturnReceipt:
        if self.target != "verifier":
            return FakeRuntime.complete_m5_verifier_atomically(
                self,
                epoch_id,
                expected_revision,
                lease,
                job,
                pair_input,
                verifier_artifact,
                attempt_output,
                execution_disposition,
                attempt_work,
                attempt_timing,
            )
        self._cutoff(epoch_id, job)
        receipt = FakeRuntime.complete_m5_verifier_atomically(
            self,
            epoch_id,
            self.world.epoch(epoch_id).revision,
            lease,
            job,
            pair_input,
            verifier_artifact,
            attempt_output,
            execution_disposition,
            attempt_work,
            attempt_timing,
        )
        return self._receipt_after_cutoff(receipt)


def _unsafe_set(object_: object, **values: object) -> object:
    changed = copy(object_)
    for name, value in values.items():
        object.__setattr__(changed, name, value)
    return changed


@dataclass(slots=True)
class _CorruptC6AcquisitionRuntime(FakeRuntime):
    lease_mode: str = "canonical"
    read_mode: str = "canonical"
    corrupt_canonical_read: bool = False
    skip_entry_read_once: bool = False

    def acquire_m5_job(
        self, epoch_id: int, expected_revision: int, job: M5LogicalJobSpec
    ) -> M5JobLease:
        lease = FakeRuntime.acquire_m5_job(self, epoch_id, expected_revision, job)
        projection = lease.terminal_projection
        if (
            lease.disposition is not M5AcquisitionDisposition.TERMINAL
            or projection is None
            or projection.terminal_reason is not M5TerminalReason.EPOCH_FAILED
        ):
            return lease
        self.corrupt_canonical_read = True
        if self.lease_mode == "wrong_job":
            return _unsafe_set(lease, logical_job_id=sha("wrong-job"))  # type: ignore[return-value]
        if self.lease_mode == "wrong_execution":
            attempt = M5JobAttempt.build(
                logical_job_id=lease.logical_job_id,
                attempt_ordinal=1,
                execution_spec_hash=sha("wrong-execution"),
                lease_token_hash=sha("wrong-execution-lease"),
                lease_expires_at=FAKE_LEASE_BASE,
                attempt_work_digest=M5RuntimeWork().work_digest,
            )
            return _unsafe_set(  # type: ignore[return-value]
                lease,
                attempt=attempt,
                lease_expires_at=attempt.lease_expires_at,
                dispatch_record_digest=sha("wrong-execution-dispatch"),
            )
        if self.lease_mode == "wrong_disposition":
            return _unsafe_set(  # type: ignore[return-value]
                lease, disposition=M5AcquisitionDisposition.LIVE_LEASE
            )
        if self.lease_mode == "wrong_reason":
            wrong_projection = M5LeaseTerminalProjection.build(
                logical_job_id=lease.logical_job_id,
                terminal_state=M5JobState.CANCELLED,
                terminal_reason=M5TerminalReason.SCOPE_RETIRED,
                completion_digest=projection.completion_digest,
            )
            return replace(lease, terminal_projection=wrong_projection)
        if self.lease_mode == "wrong_terminal_identity":
            wrong_projection = cast(
                M5LeaseTerminalProjection,
                _unsafe_set(
                    projection, terminal_identity_hash=sha("wrong-terminal-identity")
                ),
            )
            return _unsafe_set(  # type: ignore[return-value]
                lease, terminal_projection=wrong_projection
            )
        return lease

    def read_typed_event_result(
        self, event_id: str, payload_hash: str
    ) -> M5EventRunResult | None:
        if self.skip_entry_read_once:
            self.skip_entry_read_once = False
            self.world.operation_log.append("read:forced-terminal-open")
            return None
        canonical = FakeRuntime.read_typed_event_result(self, event_id, payload_hash)
        if canonical is None or not self.corrupt_canonical_read:
            return canonical
        if self.read_mode == "wrong_epoch":
            wrong_epoch = canonical.epoch_id + 100
            assert canonical.failure_reason is not None
            return M5EventRunResult.build(
                event_id=canonical.event_id,
                payload_hash=canonical.payload_hash,
                epoch_id=wrong_epoch,
                state=M5RunState.REPLAYED,
                replayed_outcome=M5ReplayedOutcome.FAILED,
                open_receipt=OpenEventReceipt(
                    wrong_epoch,
                    True,
                    False,
                    already_failed=True,
                    failure_reason=canonical.failure_reason.value,
                ),
                publication_receipt=None,
                event_work=canonical.event_work,
                call_work=M5RuntimeWork(),
                event_timing=canonical.event_timing,
                call_timing=canonical.call_timing,
                combined_deltas=canonical.combined_deltas,
                changed_state_references=canonical.changed_state_references,
                failure_reason=canonical.failure_reason,
                event_timing_coverage=canonical.event_timing_coverage,
                call_timing_coverage=canonical.call_timing_coverage,
            )
        if self.read_mode == "wrong_outcome":
            publication = PublicationReceipt(
                canonical.epoch_id,
                stable_m4_digest("m4-publication-v1", str(canonical.epoch_id)),
                True,
            )
            return M5EventRunResult.build(
                event_id=canonical.event_id,
                payload_hash=canonical.payload_hash,
                epoch_id=canonical.epoch_id,
                state=M5RunState.REPLAYED,
                replayed_outcome=M5ReplayedOutcome.SEALED,
                open_receipt=OpenEventReceipt(
                    canonical.epoch_id,
                    True,
                    True,
                    publication.publication_id,
                ),
                publication_receipt=publication,
                event_work=canonical.event_work,
                call_work=M5RuntimeWork(),
                event_timing=canonical.event_timing,
                call_timing=canonical.call_timing,
                combined_deltas=canonical.combined_deltas,
                changed_state_references=canonical.changed_state_references,
                failure_reason=None,
                event_timing_coverage=canonical.event_timing_coverage,
                call_timing_coverage=canonical.call_timing_coverage,
            )
        if self.read_mode == "malformed_branch":
            return _unsafe_replayed_outcome_none(canonical)
        if self.read_mode == "wrong_terminal_receipt":
            assert canonical.failure_reason is not None
            return _unsafe_set(  # type: ignore[return-value]
                canonical,
                open_receipt=OpenEventReceipt(
                    canonical.epoch_id,
                    True,
                    False,
                    already_failed=True,
                    failure_reason=M5RunFailureReason.VERIFIER_ERROR.value,
                ),
            )
        if self.read_mode == "bool_nested_epoch":
            wrong_open = _unsafe_set(canonical.open_receipt, epoch_id=True)
            return _unsafe_set(  # type: ignore[return-value]
                canonical, open_receipt=wrong_open
            )
        if self.read_mode == "stale_changed_reference":
            assert canonical.changed_state_references
            first_reference = canonical.changed_state_references[0]
            wrong_reference = cast(
                M5ChangedStateReference,
                _unsafe_set(first_reference, revision=first_reference.revision + 1),
            )
            assert wrong_reference.reference_digest == first_reference.reference_digest
            return _unsafe_set(  # type: ignore[return-value]
                canonical,
                changed_state_references=(
                    wrong_reference,
                    *canonical.changed_state_references[1:],
                ),
            )
        if self.read_mode == "negative_event_work":
            wrong_work = cast(
                M5RuntimeWork,
                _unsafe_set(
                    canonical.event_work,
                    requirement_cancelled_job_count=-1,
                ),
            )
            return _unsafe_set(  # type: ignore[return-value]
                canonical, event_work=wrong_work
            )
        if self.read_mode == "negative_event_timing":
            wrong_timing = cast(
                M5RuntimeTiming,
                _unsafe_set(
                    canonical.event_timing,
                    coordinator_non_db_non_neural_ns=-1,
                ),
            )
            return _unsafe_set(  # type: ignore[return-value]
                canonical, event_timing=wrong_timing
            )
        if self.read_mode == "coverage_none":
            return _unsafe_set(  # type: ignore[return-value]
                canonical,
                event_timing_coverage=None,
                call_timing_coverage=None,
            )
        return canonical


@dataclass(slots=True)
class _MalformedCheckedReplayRuntime(FakeRuntime):
    target: str = "failure"

    def fail_typed_epoch_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        failure_reason: M5RunFailureReason,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        result = FakeRuntime.fail_typed_epoch_atomically(
            self, epoch_id, expected_revision, failure_reason, call_work
        )
        if self.target == "failure" and result.state is M5RunState.REPLAYED:
            return _unsafe_replayed_outcome_none(result)
        return result

    def request_typed_seal_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        event: M5TypedEventPlan,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        result = FakeRuntime.request_typed_seal_atomically(
            self, epoch_id, expected_revision, event, call_work
        )
        if self.target == "seal" and result.state is M5RunState.REPLAYED:
            return _unsafe_replayed_outcome_none(result)
        return result


@dataclass(slots=True)
class _CorruptFirstTerminalReceiptRuntime(FakeRuntime):
    target: str = "failure"

    @staticmethod
    def _wrong_receipt(result: M5EventRunResult) -> M5EventRunResult:
        return _unsafe_set(  # type: ignore[return-value]
            result,
            open_receipt=OpenEventReceipt(result.epoch_id, True, False),
        )

    def fail_typed_epoch_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        failure_reason: M5RunFailureReason,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        result = FakeRuntime.fail_typed_epoch_atomically(
            self, epoch_id, expected_revision, failure_reason, call_work
        )
        if self.target == "failure" and result.state is M5RunState.FAILED:
            return self._wrong_receipt(result)
        return result

    def request_typed_seal_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        event: M5TypedEventPlan,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        result = FakeRuntime.request_typed_seal_atomically(
            self, epoch_id, expected_revision, event, call_work
        )
        if self.target == "seal" and result.state is M5RunState.SEALED:
            return self._wrong_receipt(result)
        return result


@pytest.mark.parametrize("mode", ["epoch_bool", "flag_non_bool", "receipt_subclass"])
def test_malformed_structural_open_rejects_before_anchor_or_external_action(
    mode: str,
) -> None:
    harness, plan, _ = _register_case(f"bad-open-{mode}", with_pair=False)
    structural = _CorruptStructuralOpen(harness.world, mode=mode)
    harness.structural = structural
    harness.application.structural = structural

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert "timing:structural_open" not in harness.world.operation_log
    assert harness.world.external_call_count == 0
    assert harness.world.terminal_telemetry == {}


def test_malformed_root_barrier_rejects_before_anchor_or_verifier_action() -> None:
    harness, plan, pair = _register_case("bad-root-barrier", with_pair=True)
    assert pair is not None
    runtime = _CorruptRootBarrierRuntime(harness.world)
    _install_runtime(harness, runtime)

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert "root-barrier" in harness.world.operation_log
    assert "timing:root_barrier" not in harness.world.operation_log
    assert "external:requirement-verifier" not in harness.world.operation_log
    assert harness.world.terminal_telemetry == {}


def test_malformed_transition_timing_receipt_rejects_before_next_action() -> None:
    harness, plan, _ = _register_case("bad-transition-receipt", with_pair=False)
    runtime = _CorruptTransitionTimingRuntime(harness.world)
    _install_runtime(harness, runtime)

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert "timing:structural_open" in harness.world.operation_log
    assert not any(
        marker.startswith("acquire:") for marker in harness.world.operation_log
    )
    assert harness.world.external_call_count == 0
    assert harness.world.terminal_telemetry == {}


def test_malformed_terminal_measurement_rejects_before_telemetry_write() -> None:
    harness, plan, _ = _register_case("bad-terminal-measurement", with_pair=False)
    measurements = _CorruptTerminalMeasurements(harness.world)
    harness.measurements = measurements
    harness.application.measurements = measurements

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert any(
        marker.startswith("measure-terminal:") for marker in harness.world.operation_log
    )
    assert harness.world.terminal_telemetry == {}


def test_nested_discovery_nan_score_with_stale_digest_rejects_before_stage() -> None:
    harness, plan, pair = _register_case("bad-discovery-nested", with_pair=True)
    assert pair is not None
    discovery = _CorruptNestedDiscovery(harness.world)
    harness.discovery = discovery
    harness.application.discovery = discovery

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert "external:requirement-discovery" in harness.world.operation_log
    assert "discovery-staged" not in harness.world.operation_log
    assert "timing:root_result_stage" not in harness.world.operation_log
    assert harness.world.terminal_telemetry == {}


@pytest.mark.parametrize(
    ("reason", "retryable"),
    [
        (M5RunFailureReason.RETRIEVAL_UNAVAILABLE, True),
        (M5RunFailureReason.RETRIEVAL_ERROR, False),
    ],
)
def test_mutated_external_failure_hash_rejects_before_settlement(
    reason: M5RunFailureReason,
    retryable: bool,
) -> None:
    harness, plan, _ = _register_case(
        f"bad-external-failure-{reason.value}", with_pair=False
    )
    first = _requirement_declarations(harness, plan)[0]
    requirement_id = first.scope.requirement_version_id
    assert requirement_id is not None
    harness.world.discovery_outcomes[
        (plan.structural_event_id, _forward_direction(), requirement_id)
    ] = [FakeDiscoveryOutcome(failure_reason=reason, retryable=retryable)]
    discovery = _CorruptExternalFailureDiscovery(harness.world)
    harness.discovery = discovery
    harness.application.discovery = discovery

    with pytest.raises(ValidationError, match="hash"):
        harness.application.run_event(plan)

    assert "retryable-failure" not in harness.world.operation_log
    assert "terminal-failure" not in harness.world.operation_log
    assert "timing:m5_attempt_execution" not in harness.world.operation_log
    assert harness.world.terminal_telemetry == {}


def test_nested_verifier_nan_score_with_stale_hash_rejects_before_completion() -> None:
    harness, plan, pair = _register_case("bad-verifier-nested", with_pair=True)
    assert pair is not None
    verifier = _CorruptNestedVerifier(harness.world)
    harness.verifier = verifier
    harness.application.verifier = verifier

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert "external:requirement-verifier" in harness.world.operation_log
    assert "verifier-complete" not in harness.world.operation_log
    assert "timing:verifier_completion" not in harness.world.operation_log
    assert harness.world.terminal_telemetry == {}


def test_success_orders_each_anchor_before_the_next_external_action() -> None:
    harness, plan, pair = _register_case("anchor-order", with_pair=True)
    assert pair is not None

    result = harness.application.run_event(plan)

    assert result.state is M5RunState.SEALED
    log = harness.world.operation_log
    expected = (
        "typed-open",
        "timing:structural_open",
        "acquire:forward_requirement_retrieval",
        "timing:m5_acquisition",
        "external:requirement-discovery",
        "discovery-staged",
        "timing:root_result_stage",
        "root-barrier",
        "timing:root_barrier",
        "acquire:verify_requirement_pair",
        "timing:m5_acquisition",
        "external:requirement-verifier",
        "verifier-complete",
        "timing:verifier_completion",
        "typed-seal",
    )
    cursor = -1
    for marker in expected:
        cursor = log.index(marker, cursor + 1)
    timing_count = sum(item.startswith("timing:") for item in log)
    external_count = harness.world.external_call_count

    replay = harness.application.run_event(plan)

    assert replay.state is M5RunState.REPLAYED
    assert replay.call_work.is_zero
    assert harness.world.external_call_count == external_count
    assert sum(item.startswith("timing:") for item in log) == timing_count
    terminal_measure = next(
        index
        for index, marker in enumerate(log)
        if marker.startswith("measure-terminal:")
    )
    assert log.index("typed-seal") < terminal_measure


def test_transition_timing_replay_is_an_exact_noop() -> None:
    harness, plan, _ = _register_case("timing-idempotence", with_pair=False)
    result = harness.application.run_event(plan)
    epoch = harness.world.epoch(result.epoch_id)
    key = next(
        key
        for key, observation in epoch.transition_observations.items()
        if observation.required_interval_observed
    )
    observed_timing = epoch.transition_observations[key].timing
    assert observed_timing is not None
    kind, source_id, revision = key
    anchor = M5TransitionTimingAnchor.build(
        epoch_id=epoch.epoch_id,
        contribution_kind=kind,
        source_id=source_id,
        anchor_revision=revision,
        terminal_transition=False,
    )
    prior_revision = epoch.revision
    prior_work = epoch.event_work
    prior_timing_points = tuple(epoch.timing_points)
    timing_writes = harness.world.operation_log.count(f"timing:{kind.value}")

    receipt = harness.runtime.append_transition_call_timing(
        epoch.epoch_id,
        kind,
        source_id,
        anchor.contribution_key_digest,
        revision,
        observed_timing,
    )

    assert receipt.exact_replay
    assert receipt.anchor == anchor
    assert epoch.revision == prior_revision
    assert epoch.event_work == prior_work
    assert tuple(epoch.timing_points) == prior_timing_points
    assert harness.world.operation_log.count(f"timing:{kind.value}") == timing_writes


def test_live_lease_blocks_without_redispatch_or_new_anchor() -> None:
    harness, plan, _ = _register_case("live-lease", with_pair=False)
    crashing = _CrashDiscoveryOnce(harness.world)
    harness.discovery = crashing
    harness.application.discovery = crashing
    with pytest.raises(SyntheticCrash):
        harness.application.run_event(plan)
    calls = harness.world.external_call_count
    timing_count = sum(
        item.startswith("timing:m5_acquisition") for item in harness.world.operation_log
    )

    blocked = harness.application.run_event(plan)

    assert blocked.state is M5RunState.BLOCKED
    assert blocked.failure_reason is M5RunFailureReason.WORK_IN_PROGRESS
    assert blocked.open_receipt.replayed
    assert harness.world.external_call_count == calls
    assert (
        sum(
            item.startswith("timing:m5_acquisition")
            for item in harness.world.operation_log
        )
        == timing_count
    )


def test_blocked_read_rejects_negative_work_with_stale_digest() -> None:
    harness, plan, _ = _register_case("blocked-corrupt-work", with_pair=False)
    crashing = _CrashDiscoveryOnce(harness.world)
    harness.discovery = crashing
    harness.application.discovery = crashing
    with pytest.raises(SyntheticCrash):
        harness.application.run_event(plan)
    runtime = _CorruptBlockedReadRuntime(harness.world)
    _install_runtime(harness, runtime)
    calls = harness.world.external_call_count

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert harness.world.external_call_count == calls
    assert harness.world.terminal_telemetry == {}


def test_takeover_uses_a_new_attempt_and_one_new_acquisition_anchor() -> None:
    harness, plan, _ = _register_case("takeover", with_pair=False)
    crashing = _CrashDiscoveryOnce(harness.world)
    harness.discovery = crashing
    harness.application.discovery = crashing
    with pytest.raises(SyntheticCrash):
        harness.application.run_event(plan)
    epoch = next(iter(harness.world.epochs_by_event.values()))
    root_id = _root_job_id(harness, plan)
    harness.world.takeover_jobs_once.add(root_id)

    result = harness.application.run_event(plan)

    assert result.state is M5RunState.SEALED
    assert len(epoch.jobs[root_id].attempts) == 2
    assert (
        sum(item == "timing:m5_acquisition" for item in harness.world.operation_log)
        == 2
    )


def test_result_reserved_reconnect_skips_external_root_execution() -> None:
    harness, plan, _ = _register_case("result-reserved", with_pair=False)
    runtime = _CrashBarrierOnceRuntime(harness.world)
    _install_runtime(harness, runtime)
    with pytest.raises(SyntheticCrash):
        harness.application.run_event(plan)
    calls = harness.world.external_call_count
    epoch = next(iter(harness.world.epochs_by_event.values()))
    root_id = _root_job_id(harness, plan)
    assert root_id in epoch.root_results

    result = harness.application.run_event(plan)

    assert result.state is M5RunState.SEALED
    assert harness.world.external_call_count == calls
    assert len(epoch.jobs[root_id].attempts) == 1


def test_terminal_acquisition_resumes_epoch_failure_without_external_retry() -> None:
    harness, plan, _ = _register_case("terminal-acquisition", with_pair=False)
    root_id = _root_job_id(harness, plan)
    event_id = plan.structural_event_id
    requirement_id = plan.requirement_registry_snapshot.entries[
        0
    ].requirement_version_id
    harness.world.discovery_outcomes[
        (event_id, _forward_direction(), requirement_id)
    ] = [
        FakeDiscoveryOutcome(
            failure_reason=M5RunFailureReason.RETRIEVAL_ERROR,
            retryable=False,
        )
    ]
    runtime = _CrashEpochFailureOnceRuntime(harness.world)
    _install_runtime(harness, runtime)
    with pytest.raises(SyntheticCrash):
        harness.application.run_event(plan)
    epoch = next(iter(harness.world.epochs_by_event.values()))
    assert epoch.jobs[root_id].state is M5JobState.TERMINAL_FAILED
    calls = harness.world.external_call_count

    result = harness.application.run_event(plan)

    assert result.state is M5RunState.FAILED
    assert result.failure_reason is M5RunFailureReason.RETRIEVAL_ERROR
    assert harness.world.external_call_count == calls


@pytest.mark.parametrize(
    ("reason", "retryable", "expected_state"),
    [
        (M5RunFailureReason.RETRIEVAL_UNAVAILABLE, True, M5RunState.BLOCKED),
        (M5RunFailureReason.RETRIEVAL_ERROR, False, M5RunState.FAILED),
    ],
)
def test_attempt_failure_persists_exact_work_timing_before_result(
    reason: M5RunFailureReason,
    retryable: bool,
    expected_state: M5RunState,
) -> None:
    harness, plan, _ = _register_case(f"failure-{reason.value}", with_pair=False)
    requirement_id = plan.requirement_registry_snapshot.entries[
        0
    ].requirement_version_id
    harness.world.discovery_outcomes[
        (plan.structural_event_id, _forward_direction(), requirement_id)
    ] = [FakeDiscoveryOutcome(failure_reason=reason, retryable=retryable)]

    result = harness.application.run_event(plan)

    assert result.state is expected_state
    assert result.call_work.requirement_forward_retrieval_call_count == 1
    epoch = next(iter(harness.world.epochs_by_event.values()))
    failure = next(iter(epoch.attempt_failures.values()))
    _receipt, _error_hash, work, timing, terminal_reason = failure
    assert work == result.call_work
    assert timing == FAKE_OBSERVED_TIMING
    assert (terminal_reason is None) is retryable
    log = harness.world.operation_log
    settlement = "retryable-failure" if retryable else "terminal-failure"
    assert log.index("external:requirement-discovery") < log.index(settlement)
    assert log.index(settlement) < log.index("timing:m5_attempt_execution")
    if retryable:
        current_timing, current_coverage = harness.runtime.current_event_timing(
            result.epoch_id
        )
        assert result.event_timing == current_timing
        assert result.event_timing_coverage == current_coverage
        assert current_coverage.required_observed_count > 0
    else:
        assert result.logical_result_hash is not None
        assert any(item.startswith("terminal-telemetry:") for item in log)


def test_expired_preterminal_return_blocks_after_one_late_anchor() -> None:
    harness, plan, _ = _register_case("expired-preterminal", with_pair=False)
    root_id = _root_job_id(harness, plan)
    harness.world.return_dispositions_by_job[root_id] = [
        M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
    ]

    result = harness.application.run_event(plan)

    assert result.state is M5RunState.BLOCKED
    assert result.failure_reason is M5RunFailureReason.WORK_IN_PROGRESS
    assert result.event_work.requirement_late_attempt_artifact_count == 1
    assert harness.world.operation_log.count("timing:preterminal_late_return") == 1


@dataclass(slots=True)
class _VerifierTerminalAuditRuntime(FakeRuntime):
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
    ) -> M5RequirementAttemptReturnReceipt:
        self.world.return_dispositions_by_job[job.logical_job_id] = [
            M5RequirementReturnDisposition.TERMINAL_AUDIT_PRETERMINAL
        ]
        return FakeRuntime.complete_m5_verifier_atomically(
            self,
            epoch_id,
            expected_revision,
            lease,
            job,
            pair_input,
            verifier_artifact,
            attempt_output,
            execution_disposition,
            attempt_work,
            attempt_timing,
        )


def test_verifier_terminal_audit_reacquires_terminal_state_then_resumes() -> None:
    harness, plan, _ = _register_case("terminal-audit", with_pair=True)
    runtime = _VerifierTerminalAuditRuntime(harness.world)
    _install_runtime(harness, runtime)

    result = harness.application.run_event(plan)

    assert result.state is M5RunState.SEALED
    log = harness.world.operation_log
    assert log.count("timing:preterminal_late_return") == 1
    assert log.index("verifier-complete") < log.index("typed-seal")
    epoch = harness.world.epoch(result.epoch_id)
    verifier = next(
        job
        for job in epoch.jobs.values()
        if job.spec.job_kind.value == "verify_requirement_pair"
    )
    assert verifier.state is M5JobState.CANCELLED


@pytest.mark.parametrize(
    ("target", "resumed", "reused", "disposition"),
    [
        (
            "discovery",
            False,
            False,
            M5RequirementReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL,
        ),
        (
            "discovery",
            True,
            True,
            M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL,
        ),
        (
            "verifier",
            False,
            False,
            M5RequirementReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL,
        ),
        (
            "verifier",
            True,
            True,
            M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL,
        ),
    ],
)
def test_c5_active_cutoff_retains_held_receipt_and_exact_current_call_work(
    target: str,
    resumed: bool,
    reused: bool,
    disposition: M5RequirementReturnDisposition,
) -> None:
    harness, plan, pair = _register_case(
        f"c5-{target}-{resumed}-{reused}", with_pair=target == "verifier"
    )
    if target == "verifier":
        assert pair is not None
    if resumed:
        if target == "verifier":
            preparation = _CrashBarrierOnceRuntime(harness.world)
            _install_runtime(harness, preparation)
            with pytest.raises(SyntheticCrash):
                harness.application.run_event(plan)
        else:
            crashing = _CrashDiscoveryOnce(harness.world)
            harness.discovery = crashing
            harness.application.discovery = crashing
            with pytest.raises(SyntheticCrash):
                harness.application.run_event(plan)
            harness.world.takeover_jobs_once.add(_root_job_id(harness, plan))
    requirement_id = plan.requirement_registry_snapshot.entries[
        0
    ].requirement_version_id
    if reused and target == "discovery":
        harness.world.discovery_outcomes[
            (plan.structural_event_id, _forward_direction(), requirement_id)
        ] = [
            FakeDiscoveryOutcome(
                execution_disposition=M5ExecutionEvidenceDisposition.REUSED_ARTIFACT
            )
        ]
    if reused and target == "verifier":
        assert pair is not None
        harness.world.verifier_outcomes[(pair.subject_id, pair.chunk_version_id)] = [
            FakeVerifierOutcome(
                execution_disposition=M5ExecutionEvidenceDisposition.REUSED_ARTIFACT
            )
        ]
    runtime = _TerminalCutoffRuntime(
        harness.world,
        target=target,
        disposition=disposition,
    )
    _install_runtime(harness, runtime)
    calls_before = harness.world.external_call_count

    result = harness.application.run_event(plan)

    canonical = runtime.canonical_read
    assert canonical is not None
    assert result.state is M5RunState.REPLAYED
    assert result.open_receipt.replayed is resumed
    assert not result.open_receipt.already_sealed
    assert not result.open_receipt.already_failed
    assert result.call_work.is_zero is reused
    expected_external_calls = 0 if reused else (2 if target == "verifier" else 1)
    assert harness.world.external_call_count - calls_before == expected_external_calls
    assert result.event_work == canonical.event_work
    assert result.event_timing == canonical.event_timing
    assert result.event_timing_coverage == canonical.event_timing_coverage
    assert result.combined_deltas == canonical.combined_deltas
    assert result.changed_state_references == canonical.changed_state_references
    assert result.publication_receipt == canonical.publication_receipt
    assert result.failure_reason == canonical.failure_reason
    assert result.logical_result_hash == canonical.logical_result_hash
    log = harness.world.operation_log
    read_index = max(
        index for index, marker in enumerate(log) if marker == "read:cutoff-canonical"
    )
    measure_index = max(
        index
        for index, marker in enumerate(log)
        if marker.startswith("measure-terminal:")
    )
    telemetry_index = max(
        index
        for index, marker in enumerate(log)
        if marker.startswith("terminal-telemetry:")
    )
    assert read_index < measure_index < telemetry_index
    assert len(harness.world.terminal_telemetry) == 1


@pytest.mark.parametrize(
    "read_mode",
    [
        "missing",
        "hash_mismatch",
        "wrong_event",
        "wrong_payload",
        "wrong_epoch",
        "active_shape",
        "malformed_outcome",
        "coverage_none",
        "malformed_receipt",
    ],
)
def test_c5_cutoff_rejects_missing_conflicting_or_noncanonical_read(
    read_mode: str,
) -> None:
    harness, plan, _ = _register_case(f"c5-invalid-{read_mode}", with_pair=False)
    runtime = _TerminalCutoffRuntime(harness.world, read_mode=read_mode)
    _install_runtime(harness, runtime)

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert harness.world.terminal_telemetry == {}


@pytest.mark.parametrize("target", ["discovery", "verifier"])
@pytest.mark.parametrize("resumed", [False, True], ids=["fresh", "resumed"])
@pytest.mark.parametrize("nonzero", [False, True], ids=["zero", "nonzero"])
def test_c6_epoch_failed_acquisition_retains_exact_active_invocation(
    target: str,
    resumed: bool,
    nonzero: bool,
) -> None:
    if target == "discovery":
        harness, plan = _register_two_requirement_case(
            f"c6-acquire-{target}-{resumed}-{nonzero}"
        )
        pair = None
        job_kind = M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL
        skip = 1
    else:
        harness, plan, pair = _register_case(
            f"c6-acquire-{target}-{resumed}-{nonzero}", with_pair=True
        )
        assert pair is not None
        job_kind = M5JobKind.VERIFY_REQUIREMENT_PAIR
        skip = 0
    _set_first_discovery_work(harness, plan, nonzero=nonzero, pair=pair)
    _prepare_held_receipt(harness, plan, resumed=resumed)
    harness.world.terminal_acquisition_after_by_kind[job_kind] = skip
    calls_before = harness.world.external_call_count

    result = harness.application.run_event(plan)

    assert result.replayed_outcome is M5ReplayedOutcome.FAILED
    assert result.failure_reason is M5RunFailureReason.RETRIEVAL_ERROR
    assert harness.world.external_call_count - calls_before == int(nonzero)
    _assert_active_projection_and_reconnect(
        harness,
        plan,
        result,
        resumed=resumed,
        expected_work=_expected_discovery_work(nonzero=nonzero),
        origin_marker=f"race:acquisition-epoch-failed:{job_kind.value}",
    )


@pytest.mark.parametrize("resumed", [False, True], ids=["fresh", "resumed"])
@pytest.mark.parametrize("nonzero", [False, True], ids=["zero", "nonzero"])
def test_c6_checked_failure_mutator_replay_retains_exact_active_invocation(
    resumed: bool,
    nonzero: bool,
) -> None:
    harness, plan, _ = _register_case(
        f"c6-failure-{resumed}-{nonzero}", with_pair=False
    )
    first = _requirement_declarations(harness, plan)[0]
    requirement_id = first.scope.requirement_version_id
    assert requirement_id is not None
    harness.world.discovery_outcomes[
        (plan.structural_event_id, _forward_direction(), requirement_id)
    ] = [
        FakeDiscoveryOutcome(
            failure_reason=M5RunFailureReason.RETRIEVAL_ERROR,
            retryable=False,
            execution_disposition=(
                M5ExecutionEvidenceDisposition.RETURNED
                if nonzero
                else M5ExecutionEvidenceDisposition.REUSED_ARTIFACT
            ),
        )
    ]
    _prepare_held_receipt(harness, plan, resumed=resumed)
    harness.world.failure_mutator_race_reason = M5RunFailureReason.RETRIEVAL_ERROR
    calls_before = harness.world.external_call_count

    result = harness.application.run_event(plan)

    assert result.replayed_outcome is M5ReplayedOutcome.FAILED
    assert result.failure_reason is M5RunFailureReason.RETRIEVAL_ERROR
    assert harness.world.external_call_count - calls_before == int(nonzero)
    _assert_active_projection_and_reconnect(
        harness,
        plan,
        result,
        resumed=resumed,
        expected_work=_expected_discovery_work(nonzero=nonzero),
        origin_marker="race:failure-mutator:retrieval_error",
    )


@pytest.mark.parametrize("resumed", [False, True], ids=["fresh", "resumed"])
@pytest.mark.parametrize("nonzero", [False, True], ids=["zero", "nonzero"])
def test_c6_terminal_failed_acquisition_uses_checked_failure_projection(
    resumed: bool,
    nonzero: bool,
) -> None:
    harness, plan, pair = _register_case(
        f"c6-terminal-job-{resumed}-{nonzero}", with_pair=True
    )
    assert pair is not None
    _set_first_discovery_work(harness, plan, nonzero=nonzero, pair=pair)
    _prepare_held_receipt(harness, plan, resumed=resumed)
    harness.world.terminal_job_failure_after_by_kind[
        M5JobKind.VERIFY_REQUIREMENT_PAIR
    ] = 0
    harness.world.failure_mutator_race_reason = M5RunFailureReason.RETRIEVAL_ERROR
    calls_before = harness.world.external_call_count

    result = harness.application.run_event(plan)

    assert result.replayed_outcome is M5ReplayedOutcome.FAILED
    assert result.failure_reason is M5RunFailureReason.RETRIEVAL_ERROR
    assert harness.world.external_call_count - calls_before == int(nonzero)
    assert (
        "race:terminal-job-failed:verify_requirement_pair"
        in harness.world.operation_log
    )
    _assert_active_projection_and_reconnect(
        harness,
        plan,
        result,
        resumed=resumed,
        expected_work=_expected_discovery_work(nonzero=nonzero),
        origin_marker="race:failure-mutator:retrieval_error",
    )


@pytest.mark.parametrize(
    "outcome", [M5ReplayedOutcome.SEALED, M5ReplayedOutcome.FAILED]
)
@pytest.mark.parametrize("resumed", [False, True], ids=["fresh", "resumed"])
@pytest.mark.parametrize("nonzero", [False, True], ids=["zero", "nonzero"])
def test_c6_fake_seal_replay_preserves_exact_durable_branch_and_active_work(
    outcome: M5ReplayedOutcome,
    resumed: bool,
    nonzero: bool,
) -> None:
    harness, plan, _ = _register_case(
        f"c6-seal-{outcome.value}-{resumed}-{nonzero}", with_pair=False
    )
    _set_first_discovery_work(harness, plan, nonzero=nonzero, pair=None)
    _prepare_held_receipt(harness, plan, resumed=resumed)
    harness.world.seal_mutator_race_outcome = outcome
    calls_before = harness.world.external_call_count

    result = harness.application.run_event(plan)

    assert result.replayed_outcome is outcome
    assert result.failure_reason is (
        None
        if outcome is M5ReplayedOutcome.SEALED
        else M5RunFailureReason.RETRIEVAL_ERROR
    )
    assert harness.world.external_call_count - calls_before == int(nonzero)
    _assert_active_projection_and_reconnect(
        harness,
        plan,
        result,
        resumed=resumed,
        expected_work=_expected_discovery_work(nonzero=nonzero),
        origin_marker=f"race:seal-mutator:{outcome.value}",
    )


@pytest.mark.parametrize(
    "lease_mode",
    [
        "wrong_job",
        "wrong_execution",
        "wrong_disposition",
        "wrong_reason",
        "wrong_terminal_identity",
    ],
)
def test_c6_epoch_failed_acquisition_rejects_wrong_total_lease_before_telemetry(
    lease_mode: str,
) -> None:
    harness, plan, pair = _register_case(f"c6-bad-lease-{lease_mode}", with_pair=True)
    assert pair is not None
    _set_first_discovery_work(harness, plan, nonzero=True, pair=pair)
    runtime = _CorruptC6AcquisitionRuntime(harness.world, lease_mode=lease_mode)
    _install_runtime(harness, runtime)
    harness.world.terminal_acquisition_after_by_kind[
        M5JobKind.VERIFY_REQUIREMENT_PAIR
    ] = 0

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert harness.world.external_call_count == 1
    assert harness.world.terminal_telemetry == {}


@pytest.mark.parametrize(
    "read_mode",
    [
        "wrong_epoch",
        "wrong_outcome",
        "malformed_branch",
        "coverage_none",
        "bool_nested_epoch",
    ],
)
def test_c6_epoch_failed_acquisition_rejects_wrong_canonical_branch(
    read_mode: str,
) -> None:
    harness, plan, pair = _register_case(f"c6-bad-read-{read_mode}", with_pair=True)
    assert pair is not None
    _set_first_discovery_work(harness, plan, nonzero=True, pair=pair)
    runtime = _CorruptC6AcquisitionRuntime(harness.world, read_mode=read_mode)
    _install_runtime(harness, runtime)
    harness.world.terminal_acquisition_after_by_kind[
        M5JobKind.VERIFY_REQUIREMENT_PAIR
    ] = 0

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert harness.world.external_call_count == 1
    assert harness.world.terminal_telemetry == {}


def test_c6_checked_failure_replay_rejects_different_reason_without_telemetry() -> None:
    harness, plan, _ = _register_case("c6-failure-reason-mismatch", with_pair=False)
    first = _requirement_declarations(harness, plan)[0]
    requirement_id = first.scope.requirement_version_id
    assert requirement_id is not None
    harness.world.discovery_outcomes[
        (plan.structural_event_id, _forward_direction(), requirement_id)
    ] = [
        FakeDiscoveryOutcome(
            failure_reason=M5RunFailureReason.RETRIEVAL_ERROR,
            retryable=False,
        )
    ]
    harness.world.failure_mutator_race_reason = M5RunFailureReason.VERIFIER_ERROR

    with pytest.raises(EventConflictError, match="same failed outcome and reason"):
        harness.application.run_event(plan)

    assert harness.world.terminal_telemetry == {}


@pytest.mark.parametrize("target", ["failure", "seal"])
def test_c6_checked_mutator_replay_rejects_malformed_branch_before_telemetry(
    target: str,
) -> None:
    harness, plan, _ = _register_case(f"c6-malformed-{target}", with_pair=False)
    runtime = _MalformedCheckedReplayRuntime(harness.world, target=target)
    _install_runtime(harness, runtime)
    if target == "failure":
        first = _requirement_declarations(harness, plan)[0]
        requirement_id = first.scope.requirement_version_id
        assert requirement_id is not None
        harness.world.discovery_outcomes[
            (plan.structural_event_id, _forward_direction(), requirement_id)
        ] = [
            FakeDiscoveryOutcome(
                failure_reason=M5RunFailureReason.RETRIEVAL_ERROR,
                retryable=False,
            )
        ]
        harness.world.failure_mutator_race_reason = M5RunFailureReason.RETRIEVAL_ERROR
    else:
        harness.world.seal_mutator_race_outcome = M5ReplayedOutcome.SEALED

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert harness.world.terminal_telemetry == {}


@pytest.mark.parametrize("target", ["failure", "seal"])
def test_first_terminal_winner_rejects_receipt_different_from_held_before_telemetry(
    target: str,
) -> None:
    harness, plan, _ = _register_case(f"winner-receipt-{target}", with_pair=False)
    runtime = _CorruptFirstTerminalReceiptRuntime(harness.world, target=target)
    _install_runtime(harness, runtime)
    if target == "failure":
        first = _requirement_declarations(harness, plan)[0]
        requirement_id = first.scope.requirement_version_id
        assert requirement_id is not None
        harness.world.discovery_outcomes[
            (plan.structural_event_id, _forward_direction(), requirement_id)
        ] = [
            FakeDiscoveryOutcome(
                failure_reason=M5RunFailureReason.RETRIEVAL_ERROR,
                retryable=False,
            )
        ]

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert harness.world.terminal_telemetry == {}


@pytest.mark.parametrize(
    "read_mode",
    [
        "wrong_epoch",
        "wrong_terminal_receipt",
        "negative_event_work",
        "negative_event_timing",
    ],
)
def test_terminal_open_rejects_canonical_result_binding_before_telemetry(
    read_mode: str,
) -> None:
    harness, plan, _ = _register_case(f"terminal-open-{read_mode}", with_pair=False)
    first_declaration = _requirement_declarations(harness, plan)[0]
    requirement_id = first_declaration.scope.requirement_version_id
    assert requirement_id is not None
    harness.world.discovery_outcomes[
        (plan.structural_event_id, _forward_direction(), requirement_id)
    ] = [
        FakeDiscoveryOutcome(
            failure_reason=M5RunFailureReason.RETRIEVAL_ERROR,
            retryable=False,
        )
    ]
    first = harness.application.run_event(plan)
    assert first.state is M5RunState.FAILED
    telemetry_count = len(harness.world.terminal_telemetry)
    runtime = _CorruptC6AcquisitionRuntime(
        harness.world,
        read_mode=read_mode,
        corrupt_canonical_read=True,
        skip_entry_read_once=True,
    )
    _install_runtime(harness, runtime)

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert len(harness.world.terminal_telemetry) == telemetry_count


def test_terminal_open_rejects_stale_changed_state_reference_before_telemetry() -> None:
    harness, plan, _ = _register_case(
        "terminal-open-stale-changed-reference", with_pair=False
    )
    first = harness.application.run_event(plan)
    assert first.state is M5RunState.SEALED
    assert first.changed_state_references
    telemetry_count = len(harness.world.terminal_telemetry)
    runtime = _CorruptC6AcquisitionRuntime(
        harness.world,
        read_mode="stale_changed_reference",
        corrupt_canonical_read=True,
        skip_entry_read_once=True,
    )
    _install_runtime(harness, runtime)

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert len(harness.world.terminal_telemetry) == telemetry_count


@pytest.mark.parametrize("nonzero", [False, True], ids=["zero", "nonzero"])
def test_generic_direct_failure_replay_is_not_a_c6_projection(
    nonzero: bool,
) -> None:
    repository = make_repository()
    event = InsertDocumentEvent(
        event_id=f"event:direct-exclusion:{nonzero}",
        document_id=f"document:direct-exclusion:{nonzero}",
        document_version_id=f"version:direct-exclusion:{nonzero}",
        content_hash=sha(f"content:direct-exclusion:{nonzero}"),
        chunks=(ChunkInput(f"chunk:direct-exclusion:{nonzero}", 0, "direct work"),),
    )
    harness = make_harness(repository)
    plan = make_typed_plan(harness.world, event)
    if not nonzero:
        direct = _ZeroWorkDirectFailure(harness.world)
        harness.direct = direct
        harness.application.direct = direct
    harness.world.direct_failed_by_event[event.event_id] = (
        M5RunFailureReason.RETRIEVAL_ERROR
    )
    harness.world.failure_mutator_race_reason = M5RunFailureReason.RETRIEVAL_ERROR

    with pytest.raises(ValidationError, match="active-projection authority"):
        harness.application.run_event(plan)

    assert harness.world.external_call_count == int(nonzero)
    assert harness.world.terminal_telemetry == {}


def test_fake_failure_replay_rejects_sealed_and_different_reason() -> None:
    sealed_harness, sealed_plan, _ = _register_case(
        "fake-failure-sealed-mismatch", with_pair=False
    )
    sealed = sealed_harness.application.run_event(sealed_plan)
    with pytest.raises(EventConflictError, match="same failed outcome and reason"):
        sealed_harness.runtime.fail_typed_epoch_atomically(
            sealed.epoch_id,
            sealed_harness.runtime.current_revision(sealed.epoch_id),
            M5RunFailureReason.RETRIEVAL_ERROR,
            M5RuntimeWork(),
        )

    failed_harness, failed_plan, _ = _register_case(
        "fake-failure-reason-mismatch", with_pair=False
    )
    first = _requirement_declarations(failed_harness, failed_plan)[0]
    requirement_id = first.scope.requirement_version_id
    assert requirement_id is not None
    failed_harness.world.discovery_outcomes[
        (failed_plan.structural_event_id, _forward_direction(), requirement_id)
    ] = [
        FakeDiscoveryOutcome(
            failure_reason=M5RunFailureReason.RETRIEVAL_ERROR,
            retryable=False,
        )
    ]
    failed = failed_harness.application.run_event(failed_plan)
    with pytest.raises(EventConflictError, match="same failed outcome and reason"):
        failed_harness.runtime.fail_typed_epoch_atomically(
            failed.epoch_id,
            failed_harness.runtime.current_revision(failed.epoch_id),
            M5RunFailureReason.VERIFIER_ERROR,
            M5RuntimeWork(),
        )


def test_ordinary_terminal_entry_replay_is_zero_work_and_no_external_action() -> None:
    harness, plan, _ = _register_case("ordinary-replay", with_pair=False)
    first = harness.application.run_event(plan)
    calls = harness.world.external_call_count
    telemetry = len(harness.world.terminal_telemetry)

    replay = harness.application.run_event(plan)

    assert first.state is M5RunState.SEALED
    assert replay.state is M5RunState.REPLAYED
    assert replay.replayed_outcome is M5ReplayedOutcome.SEALED
    assert replay.open_receipt.already_sealed
    assert replay.open_receipt.replayed
    assert replay.call_work.is_zero
    assert replay.event_work == first.event_work
    assert replay.logical_result_hash == first.logical_result_hash
    assert harness.world.external_call_count == calls
    assert len(harness.world.terminal_telemetry) == telemetry + 1


def test_successful_execution_dtos_require_explicit_disposition_and_timing() -> None:
    for dto in (M5DiscoveryExecution, M5VerifierExecution):
        parameters = inspect.signature(dto).parameters
        assert parameters["execution_disposition"].default is inspect.Parameter.empty
        assert parameters["call_work"].default is inspect.Parameter.empty
        assert parameters["attempt_timing"].default is inspect.Parameter.empty
    failure_parameters = inspect.signature(M5ExternalWorkFailure).parameters
    assert failure_parameters["call_work"].default is inspect.Parameter.empty
    assert failure_parameters["attempt_timing"].default is inspect.Parameter.empty


@pytest.mark.parametrize(
    ("reason", "retryable", "message"),
    [
        (M5RunFailureReason.WORK_IN_PROGRESS, False, "cannot report"),
        (M5RunFailureReason.INVARIANT_FAILURE, False, "cannot report"),
        (M5RunFailureReason.RETRIEVAL_UNAVAILABLE, False, "retryability"),
        (M5RunFailureReason.RETRIEVAL_ERROR, True, "retryability"),
    ],
)
def test_external_failure_rejects_blocked_internal_or_mismatched_shapes(
    reason: M5RunFailureReason,
    retryable: bool,
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        M5ExternalWorkFailure(
            reason,
            retryable=retryable,
            call_work=M5RuntimeWork(),
            attempt_timing=None,
            error_hash=sha("bad-retryability"),
        )


def test_fake_terminal_read_preserves_publication_identity_on_replay() -> None:
    harness, plan, _ = _register_case("publication-replay", with_pair=False)
    sealed = harness.application.run_event(plan)
    replay = harness.runtime.read_typed_event_result(
        plan.structural_event_id, plan.payload_hash
    )

    assert sealed.publication_receipt is not None
    assert replay is not None
    assert replay.publication_receipt == PublicationReceipt(
        sealed.epoch_id,
        sealed.publication_receipt.publication_id,
        replayed=True,
    )
    assert replay.failure_reason is None
    assert deepcopy(replay.event_work) == sealed.event_work


def _direct_selected_work(
    return_kind: M5TypedDirectReturnKind, *, nonzero: bool
) -> M5RuntimeWork:
    if not nonzero:
        return M5RuntimeWork()
    if return_kind is M5TypedDirectReturnKind.DISCOVERY:
        return M5RuntimeWork(
            direct_discovery_call_count=1,
            embedding_model_call_count=1,
        )
    return M5RuntimeWork(
        direct_verifier_call_count=1,
        verifier_model_call_count=1,
    )


def _direct_selected_case(
    name: str,
    *,
    return_kind: M5TypedDirectReturnKind,
    branch: str,
    outcome: M5ReplayedOutcome,
    nonzero: bool,
    resumed: bool,
) -> tuple[FakeHarness, M5TypedEventPlan, M5RuntimeWork]:
    event = InsertDocumentEvent(
        event_id=f"event:direct-selected:{name}",
        document_id=f"document:direct-selected:{name}",
        document_version_id=f"version:direct-selected:{name}",
        content_hash=sha(f"content:direct-selected:{name}"),
        chunks=(
            ChunkInput(
                f"chunk:direct-selected:{name}",
                0,
                f"direct selected evidence {name}",
            ),
        ),
    )
    harness = make_harness(make_repository())
    plan = make_typed_plan(harness.world, event)
    call_work = _direct_selected_work(return_kind, nonzero=nonzero)
    harness.world.direct_selected_outcomes_by_event[event.event_id] = (
        FakeDirectSelectedOutcome(return_kind, branch, outcome, call_work)
    )
    if resumed:
        harness.world.direct_interruptions_by_event[event.event_id] = 1
        with pytest.raises(FakeDirectInterruption):
            harness.application.run_event(plan)
        epoch = next(iter(harness.world.epochs_by_event.values()))
        assert epoch.terminal_result is None
        assert not epoch.direct_done
    return harness, plan, call_work


@pytest.mark.parametrize(
    "return_kind",
    tuple(M5TypedDirectReturnKind),
    ids=lambda value: value.value,
)
@pytest.mark.parametrize("branch", ["normal", "late"])
@pytest.mark.parametrize("resumed", [False, True], ids=["fresh", "resumed"])
@pytest.mark.parametrize("nonzero", [False, True], ids=["zero", "nonzero"])
@pytest.mark.parametrize(
    "outcome",
    tuple(M5ReplayedOutcome),
    ids=lambda value: value.value,
)
def test_r2d_selected_direct_outer_cutoff_cartesian_matrix_and_reconnect(
    return_kind: M5TypedDirectReturnKind,
    branch: str,
    resumed: bool,
    nonzero: bool,
    outcome: M5ReplayedOutcome,
) -> None:
    name = f"{return_kind.value}:{branch}:{resumed}:{nonzero}:{outcome.value}"
    harness, plan, expected_work = _direct_selected_case(
        name,
        return_kind=return_kind,
        branch=branch,
        outcome=outcome,
        nonzero=nonzero,
        resumed=resumed,
    )
    calls_before = harness.world.external_call_count

    result = harness.application.run_event(plan)

    assert result.state is M5RunState.REPLAYED
    assert result.replayed_outcome is outcome
    assert result.open_receipt.replayed is resumed
    assert result.call_work == expected_work
    assert len(harness.measurements.terminal_invocation_results) == 1
    measured = harness.measurements.terminal_invocation_results[0]
    expected_active_open = OpenEventReceipt(result.epoch_id, resumed, False)
    assert measured.open_receipt == expected_active_open
    assert measured.open_receipt is result.open_receipt
    assert measured.call_work == expected_work
    assert measured.call_work is result.call_work
    assert measured.state is M5RunState.REPLAYED
    assert measured.replayed_outcome is outcome
    assert measured.logical_result_hash == result.logical_result_hash
    assert measured == replace(
        result,
        call_timing=measured.call_timing,
        call_timing_coverage=measured.call_timing_coverage,
    )
    assert measured.call_timing == M5RuntimeTiming()
    assert measured.call_timing_coverage == M5RuntimeTimingCoverage.single_point(
        None,
        terminal_client_roundtrip_included=False,
    )
    assert result.call_timing == FAKE_OBSERVED_TIMING
    assert result.call_timing_coverage == M5RuntimeTimingCoverage.single_point(
        FAKE_OBSERVED_TIMING,
        terminal_client_roundtrip_included=True,
    )
    expected_event_work = expected_work if branch == "normal" else M5RuntimeWork()
    assert result.event_work.direct_discovery_call_count == (
        expected_event_work.direct_discovery_call_count
    )
    assert result.event_work.direct_verifier_call_count == (
        expected_event_work.direct_verifier_call_count
    )
    assert result.event_work.embedding_model_call_count == (
        expected_event_work.embedding_model_call_count
    )
    assert result.event_work.verifier_model_call_count == (
        expected_event_work.verifier_model_call_count
    )
    assert harness.world.external_call_count - calls_before == int(nonzero)
    selected = harness.world.direct_selected_receipts_by_event[plan.structural_event_id]
    selected_branch = selected.normal if selected.normal is not None else selected.late
    assert selected.return_kind is return_kind
    assert selected_branch is not None
    assert selected_branch.epoch_id == result.epoch_id
    assert selected_branch.current_terminal_logical_result_hash == (
        result.logical_result_hash
    )
    epoch = harness.world.epoch(result.epoch_id)
    assert len(epoch.direct_roots) == 1
    if return_kind is M5TypedDirectReturnKind.DISCOVERY:
        assert selected_branch.job_id == epoch.direct_roots[0].job_id
    else:
        assert selected_branch.job_id != epoch.direct_roots[0].job_id
        if selected.normal is not None:
            assert isinstance(
                selected.normal.observation_completion,
                ObservationCompletionReceipt,
            )

    origin = f"direct-selected-outer:{return_kind.value}:{branch}"
    log = harness.world.operation_log
    origin_index = log.index(origin)
    canonical_index = log.index("read:terminal-result", origin_index + 1)
    measure_index = next(
        index
        for index in range(canonical_index + 1, len(log))
        if log[index].startswith("measure-terminal:")
    )
    telemetry_index = next(
        index
        for index in range(measure_index + 1, len(log))
        if log[index].startswith("terminal-telemetry:")
    )
    assert origin_index < canonical_index < measure_index < telemetry_index
    active_suffix = log[origin_index + 1 : telemetry_index + 1]
    assert not any(marker.startswith("external:") for marker in active_suffix)
    assert not any(marker.startswith("timing:") for marker in active_suffix)
    assert not any(marker.startswith("tx:") for marker in active_suffix)
    assert "typed-seal" not in active_suffix
    assert "typed-failed" not in active_suffix
    assert not any(marker == "timing:direct_transition" for marker in log)
    assert not any(marker.startswith("external:requirement") for marker in log)
    assert harness.audit.calls == 0

    external_before_reconnect = harness.world.external_call_count
    durable_fields = {
        field_name: getattr(result, field_name) for field_name in _FROZEN_RESULT_FIELDS
    }
    reconnect = harness.application.run_event(plan)
    assert reconnect.state is M5RunState.REPLAYED
    assert reconnect.replayed_outcome is outcome
    assert reconnect.open_receipt.replayed
    assert reconnect.open_receipt.already_sealed is (
        outcome is M5ReplayedOutcome.SEALED
    )
    assert reconnect.open_receipt.already_failed is (
        outcome is M5ReplayedOutcome.FAILED
    )
    assert reconnect.call_work.is_zero
    assert harness.world.external_call_count == external_before_reconnect
    assert log.count(origin) == 1
    for field_name, field_value in durable_fields.items():
        assert getattr(reconnect, field_name) == field_value
    assert len(harness.world.terminal_telemetry) == 2
    assert len(harness.measurements.terminal_invocation_results) == 2


class _DirectOuterReceiptSubclass(M5DirectAttemptReturnReceipt):
    """A structurally identical but unauthorized outer-receipt subtype."""


class _EqualitySpoofingStr(str):
    """A string subtype whose comparisons conceal a different stored value."""

    __hash__ = str.__hash__

    def __eq__(self, other: object) -> bool:
        return True

    def __ne__(self, other: object) -> bool:
        return False


class _EqualitySpoofingInt(int):
    """An integer subtype whose comparisons conceal a different stored value."""

    __hash__ = int.__hash__

    def __eq__(self, other: object) -> bool:
        return True

    def __ne__(self, other: object) -> bool:
        return False


class _ArithmeticSpoofingInt(int):
    """An accepted int subtype that changes addition at the call-work boundary."""

    def __radd__(self, other: object) -> int:
        assert isinstance(other, int)
        return int(other) + int(self) + 777


class _EqualitySpoofingOpenReceipt(OpenEventReceipt):
    """An open-receipt subtype whose equality conceals field mutation."""

    __slots__ = ()

    def __eq__(self, other: object) -> bool:
        return True

    def __ne__(self, other: object) -> bool:
        return False


class _RuntimeWorkSubclass(M5RuntimeWork):
    """A zero-work subtype forbidden at a canonical-result boundary."""


class _HashBypassingEventRunResult(M5EventRunResult):
    """A result subtype that hides changed durable fields from hash checks."""

    __slots__ = ()

    @property
    def expected_logical_result_hash(self) -> str:
        assert self.logical_result_hash is not None
        return self.logical_result_hash


class _IterationSpoofingTuple(tuple[object, ...]):
    """A tuple subtype whose iterator conceals its stored elements."""

    def __iter__(self) -> Iterator[object]:
        return iter(())


class _AlternatingRuntimeWork(M5RuntimeWork):
    """An unsafe subtype that changes its counters between successive reads."""

    _counter_read_count: int
    __slots__ = ("_counter_read_count",)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_counter_read_count", 0)
        super().__post_init__()

    def counter_values(self) -> tuple[int, ...]:
        read_count = self._counter_read_count + 1
        object.__setattr__(self, "_counter_read_count", read_count)
        values = list(super().counter_values())
        if read_count % 2 == 0:
            index = self.counter_names().index("bytes_hashed")
            values[index] += 1
        return tuple(values)


_SPOOFED_SELECTED_JOB_ID = "different-selected-direct-job"
_SPOOFED_TERMINAL_HASH = sha("different-selected-terminal-hash")
_SPOOFED_CANONICAL_EVENT_ID = "different-canonical-event"
_SPOOFED_CANONICAL_PAYLOAD_HASH = sha("different-canonical-payload")


@dataclass(frozen=True, slots=True)
class _DuckDirectOuterReceipt:
    return_kind: object
    normal: object
    late: object


@dataclass(slots=True)
class _CorruptSelectedDirect(FakeDirect):
    mode: str = "receipt_only"
    injected_call_work: M5RuntimeWork | None = None

    def run_pending_direct(
        self, epoch_id: int, expected_revision: int, event: M5TypedEventPlan
    ) -> M5DirectExecutionReceipt:
        result = FakeDirect.run_pending_direct(self, epoch_id, expected_revision, event)
        selected = result.selected_successful_outer_receipt
        assert selected is not None
        selected_branch = (
            selected.normal if selected.normal is not None else selected.late
        )
        assert selected_branch is not None

        if self.mode == "receipt_only":
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(
                    result,
                    selected_successful_outer_return_kind=None,
                    selected_successful_outer_job_id=None,
                ),
            )
        if self.mode == "context_only":
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(result, selected_successful_outer_receipt=None),
            )
        if self.mode == "missing_job_context":
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(result, selected_successful_outer_job_id=None),
            )
        if self.mode == "missing_kind_context":
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(result, selected_successful_outer_return_kind=None),
            )
        if self.mode == "selected_with_blocked":
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(
                    result,
                    blocked_reason=M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
                ),
            )
        if self.mode == "selected_with_failure":
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(
                    result,
                    terminal_failure_reason=M5RunFailureReason.RETRIEVAL_ERROR,
                ),
            )
        if self.mode == "selected_wrong_type":
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(result, selected_successful_outer_receipt=object()),
            )
        if self.mode == "selected_subclass":
            subclass = _DirectOuterReceiptSubclass(
                selected.return_kind,
                selected.normal,
                selected.late,
            )
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(result, selected_successful_outer_receipt=subclass),
            )
        if self.mode == "selected_duck_type":
            duck = _DuckDirectOuterReceipt(
                selected.return_kind,
                selected.normal,
                selected.late,
            )
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(result, selected_successful_outer_receipt=duck),
            )
        if self.mode == "direct_revision_float":
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(
                    result, resulting_revision=float(result.resulting_revision)
                ),
            )
        if self.mode == "call_work_subclass":
            self.injected_call_work = _AlternatingRuntimeWork()
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(result, call_work=self.injected_call_work),
            )
        if self.mode == "call_work_counter_subclass":
            self.injected_call_work = M5RuntimeWork(
                bytes_hashed=_ArithmeticSpoofingInt(0)
            )
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(result, call_work=self.injected_call_work),
            )
        if self.mode == "cursor_local_receipt":
            cursor = M5DirectCursorContributionReceipt(
                epoch_id=selected_branch.epoch_id,
                job_id=selected_branch.job_id,
                attempt_id=selected_branch.attempt_id,
                execution_evidence_digest=sha("r2d-cursor-execution"),
                attempt_execution_contribution_key_digest=(
                    digests.runtime_work_contribution_key_digest(
                        epoch_id=selected_branch.epoch_id,
                        contribution_kind=(
                            M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION
                        ),
                        source_id=selected_branch.attempt_id,
                    )
                ),
                direct_transition_source_id=None,
                direct_transition_source_identity_hash=None,
                direct_transition_contribution_key_digest=None,
                observation_completion=None,
            )
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(result, selected_successful_outer_receipt=cursor),
            )
        if self.mode == "wrapper_both":
            alternate = FakeDirect._selected_outer_receipt(  # noqa: SLF001
                epoch_id=selected_branch.epoch_id,
                resulting_revision=selected_branch.resulting_revision,
                job_id=selected_branch.job_id,
                return_kind=selected.return_kind,
                branch=("late" if selected.normal is not None else "normal"),
                terminal_logical_result_hash=(
                    selected_branch.current_terminal_logical_result_hash or ""
                ),
            )
            malformed = cast(
                M5DirectAttemptReturnReceipt,
                _unsafe_set(
                    selected,
                    normal=selected.normal or alternate.normal,
                    late=selected.late or alternate.late,
                ),
            )
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(result, selected_successful_outer_receipt=malformed),
            )
        if self.mode == "wrapper_neither":
            malformed = cast(
                M5DirectAttemptReturnReceipt,
                _unsafe_set(selected, normal=None, late=None),
            )
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(result, selected_successful_outer_receipt=malformed),
            )
        if self.mode == "wrong_invoked_kind":
            other_kind = (
                M5TypedDirectReturnKind.VERIFIER
                if selected.return_kind is M5TypedDirectReturnKind.DISCOVERY
                else M5TypedDirectReturnKind.DISCOVERY
            )
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(result, selected_successful_outer_return_kind=other_kind),
            )
        if self.mode == "wrong_invoked_job":
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(
                    result,
                    selected_successful_outer_job_id="another-direct-job",
                ),
            )
        if self.mode == "context_job_str_subclass":
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(
                    result,
                    selected_successful_outer_job_id=_EqualitySpoofingStr(
                        _SPOOFED_SELECTED_JOB_ID
                    ),
                ),
            )
        if self.mode in {
            "attempt_id_str_subclass",
            "terminal_hash_str_subclass",
            "transition_source_str_subclass",
        }:
            string_changes: dict[str, object] = {}
            if self.mode == "attempt_id_str_subclass":
                string_changes["attempt_id"] = _EqualitySpoofingStr(
                    selected_branch.attempt_id
                )
            elif self.mode == "terminal_hash_str_subclass":
                string_changes["current_terminal_logical_result_hash"] = (
                    _EqualitySpoofingStr(_SPOOFED_TERMINAL_HASH)
                )
            else:
                assert selected.normal is not None
                string_changes["direct_transition_source_id"] = _EqualitySpoofingStr(
                    selected.normal.direct_transition_source_id
                )
            malformed_branch = _unsafe_set(selected_branch, **string_changes)
            malformed_selected = _unsafe_set(
                selected,
                normal=(malformed_branch if selected.normal is not None else None),
                late=(malformed_branch if selected.late is not None else None),
            )
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(
                    result, selected_successful_outer_receipt=malformed_selected
                ),
            )
        if self.mode in {"empty_job", "non_string_job"}:
            wrong_job: object = "" if self.mode == "empty_job" else 7
            malformed_branch = _unsafe_set(selected_branch, job_id=wrong_job)
            malformed_selected = _unsafe_set(
                selected,
                normal=(malformed_branch if selected.normal is not None else None),
                late=(malformed_branch if selected.late is not None else None),
            )
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(
                    result,
                    selected_successful_outer_receipt=malformed_selected,
                    selected_successful_outer_job_id=wrong_job,
                ),
            )
        if self.mode in {
            "wrong_epoch",
            "wrong_revision",
            "branch_revision_float",
            "missing_hash",
        }:
            changes: dict[str, object] = {}
            if self.mode == "wrong_epoch":
                changes["epoch_id"] = selected_branch.epoch_id + 1
            elif self.mode == "wrong_revision":
                changes["resulting_revision"] = selected_branch.resulting_revision + 1
            elif self.mode == "branch_revision_float":
                changes["resulting_revision"] = float(
                    selected_branch.resulting_revision
                )
            else:
                changes["current_terminal_logical_result_hash"] = None
            malformed_branch = _unsafe_set(selected_branch, **changes)
            malformed_selected = _unsafe_set(
                selected,
                normal=(malformed_branch if selected.normal is not None else None),
                late=(malformed_branch if selected.late is not None else None),
            )
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(
                    result, selected_successful_outer_receipt=malformed_selected
                ),
            )
        if self.mode == "terminal_hash_mismatch":
            malformed_branch = _unsafe_set(
                selected_branch,
                current_terminal_logical_result_hash=sha("wrong-terminal-hash"),
            )
            malformed_selected = _unsafe_set(
                selected,
                normal=(malformed_branch if selected.normal is not None else None),
                late=(malformed_branch if selected.late is not None else None),
            )
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(
                    result, selected_successful_outer_receipt=malformed_selected
                ),
            )
        if self.mode == "discovery_observation":
            assert selected.normal is not None
            malformed_normal = _unsafe_set(
                selected.normal,
                observation_completion=ObservationCompletionReceipt(True, True),
            )
            malformed_selected = _unsafe_set(selected, normal=malformed_normal)
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(
                    result, selected_successful_outer_receipt=malformed_selected
                ),
            )
        if self.mode == "verifier_missing_observation":
            assert selected.normal is not None
            malformed_normal = _unsafe_set(selected.normal, observation_completion=None)
            malformed_selected = _unsafe_set(selected, normal=malformed_normal)
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(
                    result, selected_successful_outer_receipt=malformed_selected
                ),
            )
        if self.mode == "verifier_observation_non_bool":
            assert selected.normal is not None
            observation = cast(
                ObservationCompletionReceipt,
                _unsafe_set(
                    selected.normal.observation_completion,
                    artifact_stored=1,
                ),
            )
            malformed_normal = _unsafe_set(
                selected.normal, observation_completion=observation
            )
            malformed_selected = _unsafe_set(selected, normal=malformed_normal)
            return cast(
                M5DirectExecutionReceipt,
                _unsafe_set(
                    result, selected_successful_outer_receipt=malformed_selected
                ),
            )
        if self.mode == "changed_held_replayed":
            opened = self.world.epoch(epoch_id).current_open_receipt
            assert opened is not None
            object.__setattr__(opened, "replayed", not opened.replayed)
            return result
        if self.mode == "changed_held_epoch":
            opened = self.world.epoch(epoch_id).current_open_receipt
            assert opened is not None
            object.__setattr__(opened, "epoch_id", opened.epoch_id + 1)
            return result
        raise AssertionError(f"unknown selected corruption mode: {self.mode}")


def _assert_no_r2d_action_after_rejection(harness: FakeHarness) -> None:
    assert harness.world.terminal_telemetry == {}
    assert harness.audit.calls == 0
    selected_indexes = [
        index
        for index, marker in enumerate(harness.world.operation_log)
        if marker.startswith("direct-selected-outer:")
    ]
    assert len(selected_indexes) == 1
    suffix = harness.world.operation_log[selected_indexes[0] + 1 :]
    assert not any(marker.startswith("measure-terminal:") for marker in suffix)
    assert not any(marker.startswith("terminal-telemetry:") for marker in suffix)
    assert not any(marker.startswith("external:") for marker in suffix)
    assert not any(marker.startswith("timing:") for marker in suffix)
    assert not any(marker.startswith("tx:") for marker in suffix)
    assert not any(marker.startswith("acquire:") for marker in suffix)
    assert not {
        "typed-seal",
        "typed-failed",
        "direct-complete",
        "race:direct-winning-complete",
        "discovery-staged",
        "root-barrier",
        "verifier-complete",
        "late-attempt-audit-only",
        "audit:python-reference",
    }.intersection(suffix)


def _assert_no_r2d_hydration_or_later_action(harness: FakeHarness) -> None:
    _assert_no_r2d_action_after_rejection(harness)
    selected_index = next(
        index
        for index, marker in enumerate(harness.world.operation_log)
        if marker.startswith("direct-selected-outer:")
    )
    assert (
        "read:terminal-result" not in harness.world.operation_log[selected_index + 1 :]
    )


@pytest.mark.parametrize(
    ("mode", "return_kind", "branch"),
    [
        ("receipt_only", M5TypedDirectReturnKind.DISCOVERY, "normal"),
        ("context_only", M5TypedDirectReturnKind.DISCOVERY, "normal"),
        ("missing_job_context", M5TypedDirectReturnKind.DISCOVERY, "normal"),
        ("missing_kind_context", M5TypedDirectReturnKind.DISCOVERY, "normal"),
        ("selected_with_blocked", M5TypedDirectReturnKind.DISCOVERY, "normal"),
        ("selected_with_failure", M5TypedDirectReturnKind.DISCOVERY, "normal"),
        ("selected_wrong_type", M5TypedDirectReturnKind.DISCOVERY, "normal"),
        ("selected_subclass", M5TypedDirectReturnKind.DISCOVERY, "normal"),
        ("selected_duck_type", M5TypedDirectReturnKind.VERIFIER, "late"),
        ("direct_revision_float", M5TypedDirectReturnKind.DISCOVERY, "normal"),
        ("cursor_local_receipt", M5TypedDirectReturnKind.DISCOVERY, "normal"),
        ("wrapper_both", M5TypedDirectReturnKind.DISCOVERY, "normal"),
        ("wrapper_neither", M5TypedDirectReturnKind.DISCOVERY, "normal"),
        ("wrong_invoked_kind", M5TypedDirectReturnKind.DISCOVERY, "late"),
        ("wrong_invoked_job", M5TypedDirectReturnKind.VERIFIER, "late"),
        ("empty_job", M5TypedDirectReturnKind.DISCOVERY, "late"),
        ("non_string_job", M5TypedDirectReturnKind.VERIFIER, "late"),
        ("wrong_epoch", M5TypedDirectReturnKind.DISCOVERY, "late"),
        ("wrong_revision", M5TypedDirectReturnKind.VERIFIER, "late"),
        ("branch_revision_float", M5TypedDirectReturnKind.DISCOVERY, "late"),
        ("missing_hash", M5TypedDirectReturnKind.DISCOVERY, "normal"),
        ("terminal_hash_mismatch", M5TypedDirectReturnKind.VERIFIER, "late"),
        ("discovery_observation", M5TypedDirectReturnKind.DISCOVERY, "normal"),
        (
            "verifier_missing_observation",
            M5TypedDirectReturnKind.VERIFIER,
            "normal",
        ),
        (
            "verifier_observation_non_bool",
            M5TypedDirectReturnKind.VERIFIER,
            "normal",
        ),
        ("changed_held_replayed", M5TypedDirectReturnKind.DISCOVERY, "normal"),
        ("changed_held_epoch", M5TypedDirectReturnKind.VERIFIER, "late"),
    ],
)
def test_r2d_selected_context_wrapper_branch_and_held_receipt_rejections(
    mode: str,
    return_kind: M5TypedDirectReturnKind,
    branch: str,
) -> None:
    harness, plan, _ = _direct_selected_case(
        f"reject:{mode}",
        return_kind=return_kind,
        branch=branch,
        outcome=M5ReplayedOutcome.FAILED,
        nonzero=True,
        resumed=False,
    )
    direct = _CorruptSelectedDirect(harness.world, mode=mode)
    harness.direct = direct
    harness.application.direct = direct

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    _assert_no_r2d_action_after_rejection(harness)
    suffix_start = next(
        index
        for index, marker in enumerate(harness.world.operation_log)
        if marker.startswith("direct-selected-outer:")
    )
    reads_after_selected = harness.world.operation_log[suffix_start + 1 :].count(
        "read:terminal-result"
    )
    assert reads_after_selected == int(mode == "terminal_hash_mismatch")


@pytest.mark.parametrize(
    ("mode", "return_kind", "branch"),
    [
        ("context_job_str_subclass", M5TypedDirectReturnKind.DISCOVERY, "late"),
        ("attempt_id_str_subclass", M5TypedDirectReturnKind.VERIFIER, "late"),
        ("terminal_hash_str_subclass", M5TypedDirectReturnKind.DISCOVERY, "normal"),
        (
            "transition_source_str_subclass",
            M5TypedDirectReturnKind.VERIFIER,
            "normal",
        ),
    ],
)
def test_r2d_selected_direct_rejects_nonexact_string_subclasses_before_hydration(
    mode: str,
    return_kind: M5TypedDirectReturnKind,
    branch: str,
) -> None:
    harness, plan, _ = _direct_selected_case(
        f"nonexact-string:{mode}",
        return_kind=return_kind,
        branch=branch,
        outcome=M5ReplayedOutcome.FAILED,
        nonzero=False,
        resumed=False,
    )
    direct = _CorruptSelectedDirect(harness.world, mode=mode)
    harness.direct = direct
    harness.application.direct = direct

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    _assert_no_r2d_hydration_or_later_action(harness)
    selected = harness.world.direct_selected_receipts_by_event[plan.structural_event_id]
    selected_branch = selected.normal if selected.normal is not None else selected.late
    assert selected_branch is not None
    if mode == "context_job_str_subclass":
        spoof = _EqualitySpoofingStr(_SPOOFED_SELECTED_JOB_ID)
        assert str(spoof) != selected_branch.job_id
        assert spoof == selected_branch.job_id
    elif mode == "terminal_hash_str_subclass":
        terminal = harness.world.epoch(selected_branch.epoch_id).terminal_result
        assert terminal is not None
        assert terminal.logical_result_hash is not None
        spoof = _EqualitySpoofingStr(_SPOOFED_TERMINAL_HASH)
        assert str(spoof) != terminal.logical_result_hash
        assert spoof == terminal.logical_result_hash


@pytest.mark.parametrize("mode", ["call_work_subclass", "call_work_counter_subclass"])
def test_r2d_selected_direct_rejects_unsafe_call_work_before_hydration(
    mode: str,
) -> None:
    harness, plan, _ = _direct_selected_case(
        "unsafe-call-work-subclass",
        return_kind=M5TypedDirectReturnKind.DISCOVERY,
        branch="normal",
        outcome=M5ReplayedOutcome.SEALED,
        nonzero=False,
        resumed=False,
    )
    direct = _CorruptSelectedDirect(harness.world, mode=mode)
    harness.direct = direct
    harness.application.direct = direct

    with pytest.raises(ValidationError, match="invalid call work|nonexact counter"):
        harness.application.run_event(plan)

    _assert_no_r2d_hydration_or_later_action(harness)
    assert direct.injected_call_work is not None
    if mode == "call_work_subclass":
        assert type(direct.injected_call_work) is _AlternatingRuntimeWork
        first = direct.injected_call_work.counter_values()
        second = direct.injected_call_work.counter_values()
        assert first != second
    else:
        counter = direct.injected_call_work.bytes_hashed
        assert type(counter) is _ArithmeticSpoofingInt
        assert 0 + counter == 777


@dataclass(slots=True)
class _CorruptDirectCanonicalRuntime(FakeRuntime):
    mode: str = "missing"

    def read_typed_event_result(
        self, event_id: str, payload_hash: str
    ) -> M5EventRunResult | None:
        canonical = FakeRuntime.read_typed_event_result(self, event_id, payload_hash)
        selected_returned = any(
            marker.startswith("direct-selected-outer:")
            for marker in self.world.operation_log
        )
        if canonical is None or not selected_returned:
            return canonical
        if self.mode == "missing":
            return None
        if self.mode == "wrong_event":
            return cast(
                M5EventRunResult,
                _unsafe_set(canonical, event_id="another-event"),
            )
        if self.mode == "event_id_str_subclass":
            return cast(
                M5EventRunResult,
                _unsafe_set(
                    canonical,
                    event_id=_EqualitySpoofingStr(_SPOOFED_CANONICAL_EVENT_ID),
                ),
            )
        if self.mode == "wrong_payload":
            return cast(
                M5EventRunResult,
                _unsafe_set(canonical, payload_hash=sha("another-payload")),
            )
        if self.mode == "payload_hash_str_subclass":
            return cast(
                M5EventRunResult,
                _unsafe_set(
                    canonical,
                    payload_hash=_EqualitySpoofingStr(_SPOOFED_CANONICAL_PAYLOAD_HASH),
                ),
            )
        if self.mode == "wrong_epoch":
            return cast(
                M5EventRunResult,
                _unsafe_set(canonical, epoch_id=canonical.epoch_id + 1),
            )
        if self.mode == "epoch_int_subclass":
            return cast(
                M5EventRunResult,
                _unsafe_set(
                    canonical,
                    epoch_id=_EqualitySpoofingInt(canonical.epoch_id + 1),
                ),
            )
        if self.mode == "epoch_bool":
            return cast(
                M5EventRunResult,
                _unsafe_set(canonical, epoch_id=True),
            )
        if self.mode == "wrong_logical_hash":
            return cast(
                M5EventRunResult,
                _unsafe_set(
                    canonical,
                    logical_result_hash=sha("another-logical-result"),
                ),
            )
        if self.mode == "logical_hash_str_subclass":
            return cast(
                M5EventRunResult,
                _unsafe_set(
                    canonical,
                    logical_result_hash=_EqualitySpoofingStr(_SPOOFED_TERMINAL_HASH),
                ),
            )
        if self.mode == "result_subclass_hash_bypass":
            changed_event_work = M5RuntimeWork(
                **{
                    name: (
                        getattr(canonical.event_work, name)
                        + int(name == "bytes_hashed")
                    )
                    for name in M5RuntimeWork.counter_names()
                }
            )
            return _HashBypassingEventRunResult(
                event_id=canonical.event_id,
                payload_hash=canonical.payload_hash,
                epoch_id=canonical.epoch_id,
                state=canonical.state,
                replayed_outcome=canonical.replayed_outcome,
                open_receipt=canonical.open_receipt,
                publication_receipt=canonical.publication_receipt,
                event_work=changed_event_work,
                call_work=canonical.call_work,
                event_timing=canonical.event_timing,
                call_timing=canonical.call_timing,
                combined_deltas=canonical.combined_deltas,
                changed_state_references=canonical.changed_state_references,
                failure_reason=canonical.failure_reason,
                logical_result_hash=canonical.logical_result_hash,
                event_timing_coverage=canonical.event_timing_coverage,
                call_timing_coverage=canonical.call_timing_coverage,
            )
        if self.mode == "open_failure_reason_str_subclass":
            assert canonical.failure_reason is not None
            malformed_open = cast(
                OpenEventReceipt,
                _unsafe_set(
                    canonical.open_receipt,
                    failure_reason=_EqualitySpoofingStr("another-failure"),
                ),
            )
            return cast(
                M5EventRunResult,
                _unsafe_set(canonical, open_receipt=malformed_open),
            )
        if self.mode == "call_work_subclass":
            return cast(
                M5EventRunResult,
                _unsafe_set(canonical, call_work=_RuntimeWorkSubclass()),
            )
        if self.mode == "combined_deltas_tuple_subclass":
            return cast(
                M5EventRunResult,
                _unsafe_set(
                    canonical,
                    combined_deltas=_IterationSpoofingTuple(canonical.combined_deltas),
                ),
            )
        if self.mode == "changed_refs_tuple_subclass":
            return cast(
                M5EventRunResult,
                _unsafe_set(
                    canonical,
                    changed_state_references=_IterationSpoofingTuple(
                        canonical.changed_state_references
                    ),
                ),
            )
        if self.mode == "wrong_state":
            return cast(
                M5EventRunResult,
                _unsafe_set(canonical, state=M5RunState.FAILED),
            )
        if self.mode == "wrong_outcome":
            other = (
                M5ReplayedOutcome.FAILED
                if canonical.replayed_outcome is M5ReplayedOutcome.SEALED
                else M5ReplayedOutcome.SEALED
            )
            return cast(
                M5EventRunResult,
                _unsafe_set(canonical, replayed_outcome=other),
            )
        if self.mode == "active_open_receipt":
            return cast(
                M5EventRunResult,
                _unsafe_set(
                    canonical,
                    open_receipt=OpenEventReceipt(canonical.epoch_id, False, False),
                ),
            )
        if self.mode == "wrong_terminal_open_receipt":
            wrong_open = _unsafe_set(
                canonical.open_receipt,
                epoch_id=canonical.epoch_id + 1,
            )
            return cast(
                M5EventRunResult,
                _unsafe_set(canonical, open_receipt=wrong_open),
            )
        if self.mode == "nonzero_call_work":
            return cast(
                M5EventRunResult,
                _unsafe_set(
                    canonical,
                    call_work=M5RuntimeWork(direct_discovery_call_count=1),
                ),
            )
        if self.mode == "changed_event_work":
            changed_work = M5RuntimeWork(
                **{
                    name: (
                        getattr(canonical.event_work, name)
                        + int(name == "bytes_hashed")
                    )
                    for name in M5RuntimeWork.counter_names()
                }
            )
            return cast(
                M5EventRunResult,
                _unsafe_set(canonical, event_work=changed_work),
            )
        if self.mode == "changed_event_timing":
            changed_timing = _unsafe_set(
                canonical.event_timing,
                coordinator_non_db_non_neural_ns=-1,
            )
            return cast(
                M5EventRunResult,
                _unsafe_set(canonical, event_timing=changed_timing),
            )
        if self.mode == "missing_coverage":
            return cast(
                M5EventRunResult,
                _unsafe_set(canonical, event_timing_coverage=None),
            )
        if self.mode == "wrong_publication":
            assert canonical.publication_receipt is not None
            publication = _unsafe_set(
                canonical.publication_receipt,
                publication_id=sha("another-publication"),
            )
            return cast(
                M5EventRunResult,
                _unsafe_set(canonical, publication_receipt=publication),
            )
        if self.mode == "wrong_failure":
            assert canonical.failure_reason is M5RunFailureReason.RETRIEVAL_ERROR
            return cast(
                M5EventRunResult,
                _unsafe_set(
                    canonical,
                    failure_reason=M5RunFailureReason.VERIFIER_ERROR,
                ),
            )
        if self.mode == "mutate_held_during_read":
            epoch = self.world.epoch(canonical.epoch_id)
            assert epoch.current_open_receipt is not None
            object.__setattr__(
                epoch.current_open_receipt,
                "replayed",
                not epoch.current_open_receipt.replayed,
            )
            return canonical
        raise AssertionError(f"unknown canonical corruption mode: {self.mode}")


@pytest.mark.parametrize(
    ("mode", "outcome"),
    [
        ("missing", M5ReplayedOutcome.FAILED),
        ("wrong_event", M5ReplayedOutcome.FAILED),
        ("wrong_payload", M5ReplayedOutcome.FAILED),
        ("wrong_epoch", M5ReplayedOutcome.FAILED),
        ("wrong_logical_hash", M5ReplayedOutcome.FAILED),
        ("wrong_state", M5ReplayedOutcome.FAILED),
        ("wrong_outcome", M5ReplayedOutcome.FAILED),
        ("wrong_outcome", M5ReplayedOutcome.SEALED),
        ("active_open_receipt", M5ReplayedOutcome.FAILED),
        ("wrong_terminal_open_receipt", M5ReplayedOutcome.SEALED),
        ("nonzero_call_work", M5ReplayedOutcome.FAILED),
        ("changed_event_work", M5ReplayedOutcome.SEALED),
        ("changed_event_timing", M5ReplayedOutcome.FAILED),
        ("missing_coverage", M5ReplayedOutcome.SEALED),
        ("wrong_publication", M5ReplayedOutcome.SEALED),
        ("wrong_failure", M5ReplayedOutcome.FAILED),
        ("mutate_held_during_read", M5ReplayedOutcome.FAILED),
    ],
)
def test_r2d_selected_direct_rejects_noncanonical_hydration_before_telemetry(
    mode: str,
    outcome: M5ReplayedOutcome,
) -> None:
    harness, plan, _ = _direct_selected_case(
        f"canonical:{mode}:{outcome.value}",
        return_kind=M5TypedDirectReturnKind.VERIFIER,
        branch="late",
        outcome=outcome,
        nonzero=True,
        resumed=False,
    )
    runtime = _CorruptDirectCanonicalRuntime(harness.world, mode=mode)
    _install_runtime(harness, runtime)

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    _assert_no_r2d_action_after_rejection(harness)


@pytest.mark.parametrize(
    "mode",
    [
        "logical_hash_str_subclass",
        "event_id_str_subclass",
        "payload_hash_str_subclass",
        "epoch_int_subclass",
        "epoch_bool",
        "result_subclass_hash_bypass",
        "open_failure_reason_str_subclass",
        "call_work_subclass",
        "combined_deltas_tuple_subclass",
        "changed_refs_tuple_subclass",
    ],
)
def test_r2d_rejects_unsafe_canonical_identity_subclasses_before_measurement(
    mode: str,
) -> None:
    harness, plan, _ = _direct_selected_case(
        f"unsafe-canonical-identity:{mode}",
        return_kind=M5TypedDirectReturnKind.VERIFIER,
        branch="late",
        outcome=M5ReplayedOutcome.FAILED,
        nonzero=False,
        resumed=False,
    )
    runtime = _CorruptDirectCanonicalRuntime(harness.world, mode=mode)
    _install_runtime(harness, runtime)

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    _assert_no_r2d_action_after_rejection(harness)
    selected_index = next(
        index
        for index, marker in enumerate(harness.world.operation_log)
        if marker.startswith("direct-selected-outer:")
    )
    suffix = harness.world.operation_log[selected_index + 1 :]
    assert suffix.count("read:terminal-result") == 1
    assert not any(marker.startswith("measure-terminal:") for marker in suffix)
    if mode == "logical_hash_str_subclass":
        selected = harness.world.direct_selected_receipts_by_event[
            plan.structural_event_id
        ]
        selected_branch = (
            selected.normal if selected.normal is not None else selected.late
        )
        assert selected_branch is not None
        assert selected_branch.current_terminal_logical_result_hash is not None
        spoof = _EqualitySpoofingStr(_SPOOFED_TERMINAL_HASH)
        assert str(spoof) != selected_branch.current_terminal_logical_result_hash
        assert spoof == selected_branch.current_terminal_logical_result_hash


@pytest.mark.parametrize("mode", ["open_receipt", "call_work", "event_id"])
def test_r2d_rejects_measurement_mutation_before_terminal_telemetry(
    mode: str,
) -> None:
    harness, plan, _ = _direct_selected_case(
        f"measurement-mutation:{mode}",
        return_kind=M5TypedDirectReturnKind.VERIFIER,
        branch="late",
        outcome=M5ReplayedOutcome.FAILED,
        nonzero=True,
        resumed=True,
    )
    measurements = _MutatingTerminalMeasurements(harness.world, mode=mode)
    harness.measurements = measurements
    harness.application.measurements = measurements

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert len(measurements.terminal_invocation_results) == 1
    assert (
        sum(
            marker.startswith("measure-terminal:")
            for marker in harness.world.operation_log
        )
        == 1
    )
    assert harness.world.terminal_telemetry == {}
    assert harness.audit.calls == 0
    measurement_index = next(
        index
        for index, marker in enumerate(harness.world.operation_log)
        if marker.startswith("measure-terminal:")
    )
    assert not any(
        marker.startswith(("terminal-telemetry:", "tx:", "acquire:"))
        for marker in harness.world.operation_log[measurement_index + 1 :]
    )


def test_r2d_selected_direct_rejects_changed_resumed_receipt_before_hydration() -> None:
    harness, plan, _ = _direct_selected_case(
        "changed-resumed-held-receipt",
        return_kind=M5TypedDirectReturnKind.VERIFIER,
        branch="normal",
        outcome=M5ReplayedOutcome.SEALED,
        nonzero=False,
        resumed=True,
    )
    direct = _CorruptSelectedDirect(harness.world, mode="changed_held_replayed")
    harness.direct = direct
    harness.application.direct = direct

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    _assert_no_r2d_action_after_rejection(harness)


@dataclass(slots=True)
class _UnselectedTerminalDirect(FakeDirect):
    def run_pending_direct(
        self, epoch_id: int, expected_revision: int, event: M5TypedEventPlan
    ) -> M5DirectExecutionReceipt:
        selected = FakeDirect.run_pending_direct(
            self, epoch_id, expected_revision, event
        )
        assert selected.selected_successful_outer_receipt is not None
        return M5DirectExecutionReceipt(
            resulting_revision=selected.resulting_revision,
            call_work=selected.call_work,
        )


@dataclass(slots=True)
class _StopAfterUnselectedDirectRuntime(FakeRuntime):
    def acquire_m5_job(
        self, epoch_id: int, expected_revision: int, job: M5LogicalJobSpec
    ) -> M5JobLease:
        self.world.operation_log.append("probe:unselected-direct-next-action")
        raise SyntheticCrash("unselected direct continued without terminal inference")


@pytest.mark.parametrize("nonzero", [False, True], ids=["zero", "nonzero"])
@pytest.mark.parametrize(
    "outcome",
    tuple(M5ReplayedOutcome),
    ids=lambda value: value.value,
)
def test_r2d_absent_selected_context_never_infers_terminal_from_work_or_state(
    nonzero: bool,
    outcome: M5ReplayedOutcome,
) -> None:
    harness, plan, expected_work = _direct_selected_case(
        f"unselected:{nonzero}:{outcome.value}",
        return_kind=M5TypedDirectReturnKind.DISCOVERY,
        branch="normal",
        outcome=outcome,
        nonzero=nonzero,
        resumed=False,
    )
    direct = _UnselectedTerminalDirect(harness.world)
    harness.direct = direct
    harness.application.direct = direct
    runtime = _StopAfterUnselectedDirectRuntime(harness.world)
    _install_runtime(harness, runtime)

    with pytest.raises(SyntheticCrash, match="without terminal inference"):
        harness.application.run_event(plan)

    assert harness.world.operation_log.count("read:terminal-result") == 1
    assert harness.world.operation_log[-1] == "probe:unselected-direct-next-action"
    assert harness.world.terminal_telemetry == {}
    assert expected_work.is_zero is (not nonzero)


def test_r2d_selected_direct_context_defaults_are_absent_and_joint() -> None:
    parameters = inspect.signature(M5DirectExecutionReceipt).parameters
    for name in (
        "selected_successful_outer_receipt",
        "selected_successful_outer_return_kind",
        "selected_successful_outer_job_id",
    ):
        assert parameters[name].default is None

    harness, plan, _ = _direct_selected_case(
        "context-constructor",
        return_kind=M5TypedDirectReturnKind.DISCOVERY,
        branch="late",
        outcome=M5ReplayedOutcome.FAILED,
        nonzero=False,
        resumed=False,
    )
    result = harness.application.run_event(plan)
    selected = harness.world.direct_selected_receipts_by_event[plan.structural_event_id]
    branch = selected.normal if selected.normal is not None else selected.late
    assert branch is not None

    with pytest.raises(ValidationError, match="jointly present"):
        M5DirectExecutionReceipt(
            result.epoch_id,
            selected_successful_outer_receipt=selected,
        )
    with pytest.raises(ValidationError, match="successful execution"):
        M5DirectExecutionReceipt(
            result.epoch_id,
            blocked_reason=M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
            selected_successful_outer_receipt=selected,
            selected_successful_outer_return_kind=selected.return_kind,
            selected_successful_outer_job_id=branch.job_id,
        )
