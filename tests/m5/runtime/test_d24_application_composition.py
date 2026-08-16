"""Pure application evidence for the activated M5-D24 R2b lane.

The tests deliberately use the transaction-aware fake ports.  They exercise
the application boundary only; they are not PostgreSQL or provider evidence.
"""

from __future__ import annotations

import inspect
from copy import copy, deepcopy
from dataclasses import dataclass, replace
from typing import cast

import pytest
from m5.runtime.fake_ports import (
    FAKE_LEASE_BASE,
    FAKE_OBSERVED_TIMING,
    FakeDirect,
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
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TerminalReason,
    M5TransitionTimingAnchor,
    M5TransitionTimingReceipt,
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


@pytest.mark.parametrize("mode", ["epoch_bool", "flag_non_bool"])
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
