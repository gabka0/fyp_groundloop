"""Internal PostgreSQL composition for the group/requirement pre-seal slice.

This facade is deliberately incomplete.  It admits only group lifecycle
events, delegates every transaction to :class:`PostgresM5RuntimeStore`, and
fails closed when the pure application reaches the unsupported production
seal boundary.
"""

from __future__ import annotations

from dataclasses import replace

from groundloop.errors import ValidationError
from groundloop.m4.application import OpenEventReceipt, StructuralWithdrawal
from groundloop.m4.contracts import DiscoveryScope as M4DiscoveryScope
from groundloop.m4.contracts import LogicalJobSpec as M4LogicalJobSpec
from groundloop.m4.pipeline import StructuralPayload
from groundloop.m5.domain import EvidenceGroupVersion, EvidenceRequirementVersion
from groundloop.m5.events import (
    RegisterGroupEvent,
    ReplaceGroupEvent,
    RetireGroupEvent,
)
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.application import (
    M5DirectExecutionReceipt,
    M5DirectOpenPlan,
    M5PostSealAuditPort,
    M5RequirementDiscoveryPort,
    M5RequirementRootDeclaration,
    M5RequirementVerifierPort,
    M5RuntimeMeasurementPort,
    M5TypedApplication,
)
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5AttemptCompletionReceipt,
    M5AttemptOutput,
    M5CandidatePolicyManifest,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5EventRunResult,
    M5ExecutionEvidenceDisposition,
    M5JobKind,
    M5JobLease,
    M5LogicalJobSpec,
    M5RequirementAttemptReturnReceipt,
    M5RequirementDiscoveryResult,
    M5RequirementFallbackKey,
    M5RequirementPairInput,
    M5RequirementVerifierArtifact,
    M5RequirementWithdrawalPlan,
    M5RootBarrierReceipt,
    M5RunFailureReason,
    M5RuntimeOperationalConfig,
    M5RuntimeTiming,
    M5RuntimeTimingCoverage,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TerminalReason,
    M5TransitionTimingReceipt,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
    RequirementRegistrySnapshotEntry,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore

_GROUP_EVENT_TYPES = (RegisterGroupEvent, ReplaceGroupEvent, RetireGroupEvent)
_PRE_SEAL_ERROR = (
    "production typed seal is unavailable in the group/requirement pre-seal facade"
)


def _require_positive_int(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValidationError(f"{name} must be a positive integer")


def _require_nonempty_text(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be nonempty")


def _validated_active_open_receipt(
    open_receipt: OpenEventReceipt, *, epoch_id: int
) -> OpenEventReceipt:
    if (
        type(open_receipt) is not OpenEventReceipt
        or type(open_receipt.epoch_id) is not int
        or open_receipt.epoch_id != epoch_id
        or open_receipt.epoch_id < 1
        or type(open_receipt.replayed) is not bool
        or type(open_receipt.already_sealed) is not bool
        or type(open_receipt.already_failed) is not bool
        or open_receipt.already_sealed
        or open_receipt.publication_id is not None
        or open_receipt.already_failed
        or open_receipt.failure_reason is not None
    ):
        raise ValidationError(
            "group facade requires the exact held nonterminal open receipt"
        )
    if replace(open_receipt) != open_receipt:
        raise ValidationError("held open receipt reconstruction changed")
    return open_receipt


def _validated_group(group: EvidenceGroupVersion) -> EvidenceGroupVersion:
    if type(group) is not EvidenceGroupVersion:
        raise ValidationError("group lifecycle event carries another group type")
    if type(group.requirements) is not tuple:
        raise ValidationError("group requirements must be an immutable tuple")
    requirements: list[EvidenceRequirementVersion] = []
    for requirement in group.requirements:
        if type(requirement) is not EvidenceRequirementVersion:
            raise ValidationError("group carries another requirement record type")
        requirements.append(replace(requirement))
    return replace(group, requirements=tuple(requirements))


def _validated_group_event_plan(event: M5TypedEventPlan) -> M5TypedEventPlan:
    if type(event) is not M5TypedEventPlan:
        raise ValidationError("event must be an M5TypedEventPlan")
    concrete = event.event
    if type(concrete) not in _GROUP_EVENT_TYPES:
        raise ValidationError(
            "group/requirement pre-seal facade admits only group lifecycle events"
        )
    if not isinstance(concrete, _GROUP_EVENT_TYPES):
        raise ValidationError("group event type validation failed")
    if event.direct_plan is not None:
        raise ValidationError("group lifecycle event cannot carry a direct plan")
    checked_event: RegisterGroupEvent | ReplaceGroupEvent | RetireGroupEvent
    if isinstance(concrete, RegisterGroupEvent):
        checked_event = replace(concrete, group=_validated_group(concrete.group))
        if checked_event.group.supersedes_group_version_id is not None:
            raise ValidationError("initial group version cannot name a predecessor")
    elif isinstance(concrete, ReplaceGroupEvent):
        _require_nonempty_text("old_group_version_id", concrete.old_group_version_id)
        checked_event = replace(
            concrete, successor=_validated_group(concrete.successor)
        )
        if (
            checked_event.successor.supersedes_group_version_id
            != checked_event.old_group_version_id
        ):
            raise ValidationError("group replacement must name its exact predecessor")
    else:
        _require_nonempty_text("group_version_id", concrete.group_version_id)
        checked_event = replace(concrete)
    if type(event.requirement_registry_snapshot) is not RequirementRegistrySnapshot:
        raise ValidationError("event carries another requirement snapshot type")
    if type(event.active_chunk_snapshot) is not ActiveChunkSnapshot:
        raise ValidationError("event carries another active-chunk snapshot type")
    if type(event.requirement_registry_snapshot.entries) is not tuple:
        raise ValidationError("requirement snapshot entries must be an immutable tuple")
    if type(event.active_chunk_snapshot.entries) is not tuple:
        raise ValidationError(
            "active-chunk snapshot entries must be an immutable tuple"
        )
    registry_entries: list[RequirementRegistrySnapshotEntry] = []
    for registry_entry in event.requirement_registry_snapshot.entries:
        if type(registry_entry) is not RequirementRegistrySnapshotEntry:
            raise ValidationError("requirement snapshot carries another entry type")
        registry_entries.append(replace(registry_entry))
    registry = replace(
        event.requirement_registry_snapshot, entries=tuple(registry_entries)
    )
    chunk_entries: list[ActiveChunkSnapshotEntry] = []
    for chunk_entry in event.active_chunk_snapshot.entries:
        if type(chunk_entry) is not ActiveChunkSnapshotEntry:
            raise ValidationError("active-chunk snapshot carries another entry type")
        chunk_entries.append(replace(chunk_entry))
    chunks = replace(event.active_chunk_snapshot, entries=tuple(chunk_entries))
    checked = replace(
        event,
        event=checked_event,
        requirement_registry_snapshot=registry,
        active_chunk_snapshot=chunks,
    )
    group: EvidenceGroupVersion | None
    if isinstance(checked_event, RegisterGroupEvent):
        group = checked_event.group
    elif isinstance(checked_event, ReplaceGroupEvent):
        group = checked_event.successor
    else:
        group = None
    if group is not None:
        for requirement in group.requirements:
            member = registry.member(requirement.requirement_version_id)
            if (
                member.group_version_id != group.group_version_id
                or member.group_family_id != group.group_family_id
                or member.owner_claim_id != group.owner_claim_id
                or member.normalized_requirement_text != requirement.requirement_text
                or member.requirement_text_hash != requirement.requirement_text_hash
            ):
                raise ValidationError(
                    "new group requirement disagrees with its frozen snapshot member"
                )
    return checked


def _canonical_group_withdrawal(event_id: str) -> M5RequirementWithdrawalPlan:
    empty: tuple[str, ...] = ()
    return M5RequirementWithdrawalPlan(
        event_id=event_id,
        deactivated_chunk_version_ids=empty,
        withdrawn_candidate_pair_digests=empty,
        withdrawn_observation_ids=empty,
        cancelled_job_ids=empty,
        fallback_keys=(),
        plan_digest=digests.requirement_withdrawal_plan_digest(
            event_id=event_id,
            deactivated_chunk_version_ids=empty,
            withdrawn_candidate_pair_digests=empty,
            withdrawn_observation_ids=empty,
            cancelled_job_ids=empty,
            fallback_keys=(),
        ),
    )


def _expected_group_roots(
    event: M5TypedEventPlan,
    manifest: M5CandidatePolicyManifest,
) -> tuple[M5RequirementRootDeclaration, ...]:
    if isinstance(event.event, RegisterGroupEvent):
        requirements = event.event.group.requirements
    elif isinstance(event.event, ReplaceGroupEvent):
        requirements = event.event.successor.requirements
    else:
        requirements = ()
    declarations: list[M5RequirementRootDeclaration] = []
    for requirement in requirements:
        scope = M5DiscoveryScopeContract.build(
            direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
            requirement_version_id=requirement.requirement_version_id,
            inserted_chunk_version_id=None,
            candidate_policy_id=manifest.candidate_policy_id,
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
        declarations.append(
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
    ordered = tuple(
        sorted(declarations, key=lambda declaration: declaration.job.logical_job_id)
    )
    if len({item.job.logical_job_id for item in ordered}) != len(ordered):
        raise ValidationError("group root declaration repeats a logical job")
    return ordered


class PostgresM5GroupRequirementPreSealPorts:
    """Exact group/requirement bridge that intentionally cannot seal."""

    def __init__(
        self,
        store: PostgresM5RuntimeStore,
        manifest: M5CandidatePolicyManifest,
        operational_config: M5RuntimeOperationalConfig,
    ) -> None:
        if type(store) is not PostgresM5RuntimeStore:
            raise ValidationError("store must be a PostgresM5RuntimeStore")
        if type(manifest) is not M5CandidatePolicyManifest:
            raise ValidationError("manifest must be an M5CandidatePolicyManifest")
        if type(operational_config) is not M5RuntimeOperationalConfig:
            raise ValidationError(
                "operational_config must be an M5RuntimeOperationalConfig"
            )
        self._store = store
        self._manifest = replace(manifest)
        self._operational_config = replace(operational_config)

    def candidate_policy(self, candidate_policy_id: str) -> M5CandidatePolicyManifest:
        if candidate_policy_id != self._manifest.candidate_policy_id:
            raise ValidationError("candidate policy is not configured by this facade")
        return replace(self._manifest)

    def plan_exact_requirement_withdrawal(
        self, event: M5TypedEventPlan
    ) -> M5RequirementWithdrawalPlan:
        checked = self._validate_bound_event(event)
        return _canonical_group_withdrawal(checked.structural_event_id)

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
        checked = self._validate_bound_event(event)
        if direct_payload is not None or direct_withdrawal is not None:
            raise ValidationError("group structural open rejects direct payloads")
        if type(direct_roots) is not tuple or direct_roots:
            raise ValidationError("group structural open requires empty direct roots")
        if type(direct_scopes) is not tuple or direct_scopes:
            raise ValidationError("group structural open requires empty direct scopes")
        canonical_withdrawal = _canonical_group_withdrawal(checked.structural_event_id)
        supplied_withdrawal = self._validate_withdrawal(requirement_withdrawal)
        if supplied_withdrawal != canonical_withdrawal:
            raise ValidationError(
                "group structural open requires canonical empty withdrawal"
            )
        supplied_roots = self._validate_roots(checked, requirement_roots)
        expected_roots = _expected_group_roots(checked, self._manifest)
        if supplied_roots != expected_roots:
            raise ValidationError(
                "requirement roots differ from the deterministic group declaration"
            )
        expected_root_set_hash = digests.requirement_root_set_digest(
            declaration.job.logical_job_id for declaration in expected_roots
        )
        if requirement_root_set_hash != expected_root_set_hash:
            raise ValidationError("requirement root-set hash disagrees with roots")
        fallback_keys = set(supplied_withdrawal.fallback_keys)
        fallback_map: dict[str, bool] = {}
        for declaration in supplied_roots:
            requirement_version_id = declaration.scope.requirement_version_id
            if requirement_version_id is None:
                raise ValidationError(
                    "group structural open cannot declare reverse roots"
                )
            key = M5RequirementFallbackKey(
                requirement_version_id=requirement_version_id,
                candidate_policy_id=declaration.job.candidate_policy_id,
            )
            fallback_map[declaration.job.logical_job_id] = key in fallback_keys
        if set(fallback_map) != {
            declaration.job.logical_job_id for declaration in supplied_roots
        }:
            raise ValidationError("fallback map does not exactly cover group roots")
        receipt = self._store.open_typed_event_atomically(
            checked,
            recovery_operational_config=self._operational_config,
            recovery_root_fallback_required=fallback_map,
        )
        if type(receipt) is not OpenEventReceipt:
            raise ValidationError("typed structural open returned another receipt type")
        return replace(receipt)

    def _validate_bound_event(self, event: M5TypedEventPlan) -> M5TypedEventPlan:
        checked = _validated_group_event_plan(event)
        if (
            checked.candidate_policy_id != self._manifest.candidate_policy_id
            or checked.candidate_policy_manifest_hash != self._manifest.manifest_hash
        ):
            raise ValidationError("typed event binds another candidate manifest")
        return checked

    @staticmethod
    def _validate_withdrawal(
        withdrawal: M5RequirementWithdrawalPlan,
    ) -> M5RequirementWithdrawalPlan:
        if type(withdrawal) is not M5RequirementWithdrawalPlan:
            raise ValidationError(
                "requirement withdrawal must be an M5RequirementWithdrawalPlan"
            )
        for name in (
            "deactivated_chunk_version_ids",
            "withdrawn_candidate_pair_digests",
            "withdrawn_observation_ids",
            "cancelled_job_ids",
            "fallback_keys",
        ):
            if type(getattr(withdrawal, name)) is not tuple:
                raise ValidationError(f"withdrawal {name} must be an immutable tuple")
        fallback_keys: list[M5RequirementFallbackKey] = []
        for key in withdrawal.fallback_keys:
            if type(key) is not M5RequirementFallbackKey:
                raise ValidationError("withdrawal carries another fallback-key type")
            fallback_keys.append(replace(key))
        return replace(withdrawal, fallback_keys=tuple(fallback_keys))

    def _validate_roots(
        self,
        event: M5TypedEventPlan,
        roots: tuple[M5RequirementRootDeclaration, ...],
    ) -> tuple[M5RequirementRootDeclaration, ...]:
        if type(roots) is not tuple:
            raise ValidationError("requirement roots must be an immutable tuple")
        checked: list[M5RequirementRootDeclaration] = []
        for declaration in roots:
            if type(declaration) is not M5RequirementRootDeclaration:
                raise ValidationError("requirement root has another declaration type")
            if type(declaration.scope) is not M5DiscoveryScopeContract:
                raise ValidationError("requirement root has another scope type")
            if type(declaration.job) is not M5LogicalJobSpec:
                raise ValidationError("requirement root has another job type")
            scope = replace(declaration.scope)
            job = replace(declaration.job)
            rebuilt = replace(declaration, scope=scope, job=job)
            scope.validate_snapshots(
                event.requirement_registry_snapshot, event.active_chunk_snapshot
            )
            job.validate_manifest_and_scope(self._manifest, scope)
            if (
                job.structural_event_id != event.structural_event_id
                or job.job_kind is not M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL
                or job.parent_job_id is not None
                or job.pair is not None
                or not job.expandable
            ):
                raise ValidationError("group event contains a malformed forward root")
            checked.append(rebuilt)
        ordered_ids = tuple(item.job.logical_job_id for item in checked)
        if ordered_ids != tuple(sorted(set(ordered_ids))):
            raise ValidationError("requirement roots must be ID-sorted and unique")
        return tuple(checked)

    def plan_direct_open(self, event: M5TypedEventPlan) -> M5DirectOpenPlan:
        self._validate_bound_event(event)
        raise ValidationError(
            "group/requirement pre-seal facade does not plan direct events"
        )

    def run_pending_direct(
        self,
        epoch_id: int,
        expected_revision: int,
        event: M5TypedEventPlan,
        open_receipt: OpenEventReceipt,
    ) -> M5DirectExecutionReceipt:
        _require_positive_int("epoch_id", epoch_id)
        _require_positive_int("expected_revision", expected_revision)
        self._validate_bound_event(event)
        _validated_active_open_receipt(open_receipt, epoch_id=epoch_id)
        return M5DirectExecutionReceipt(
            resulting_revision=expected_revision,
            call_work=M5RuntimeWork(),
        )

    def read_typed_event_result(
        self, event_id: str, payload_hash: str
    ) -> M5EventRunResult | None:
        return self._store.read_typed_event_result(event_id, payload_hash)

    def acquire_m5_job(
        self,
        epoch_id: int,
        expected_revision: int,
        job: M5LogicalJobSpec,
    ) -> M5JobLease:
        return self._store.acquire_m5_job(epoch_id, expected_revision, job)

    def mark_m5_retryable_failure(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5JobLease,
        error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> M5AttemptCompletionReceipt:
        return self._store.mark_m5_retryable_failure(
            epoch_id,
            expected_revision,
            lease,
            error_hash,
            attempt_work,
            attempt_timing,
        )

    def mark_m5_terminal_failure(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5JobLease,
        terminal_reason: M5TerminalReason,
        error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> M5AttemptCompletionReceipt:
        return self._store.mark_m5_terminal_failure(
            epoch_id,
            expected_revision,
            lease,
            terminal_reason,
            error_hash,
            attempt_work,
            attempt_timing,
        )

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
        return self._store.stage_m5_discovery_result_atomically(
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

    def close_m5_requirement_roots_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        requirement_root_set_hash: str,
    ) -> M5RootBarrierReceipt:
        return self._store.close_m5_requirement_roots_atomically(
            epoch_id, expected_revision, requirement_root_set_hash
        )

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
        return self._store.complete_m5_verifier_atomically(
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

    def fail_typed_epoch_with_open_receipt_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        failure_reason: M5RunFailureReason,
        open_receipt: OpenEventReceipt,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        _require_positive_int("epoch_id", epoch_id)
        _require_positive_int("expected_revision", expected_revision)
        _validated_active_open_receipt(open_receipt, epoch_id=epoch_id)
        return self._store.fail_typed_epoch_with_open_receipt_atomically(
            epoch_id,
            expected_revision,
            failure_reason,
            open_receipt,
            call_work,
        )

    def request_typed_seal_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        event: M5TypedEventPlan,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        _require_positive_int("epoch_id", epoch_id)
        _require_positive_int("expected_revision", expected_revision)
        self._validate_bound_event(event)
        if type(call_work) is not M5RuntimeWork:
            raise ValidationError("call_work must be an M5RuntimeWork")
        replace(call_work)
        raise ValidationError(_PRE_SEAL_ERROR)

    def append_transition_call_timing(
        self,
        epoch_id: int,
        contribution_kind: M5RuntimeWorkContributionKind,
        source_id: str,
        contribution_key_digest: str,
        anchor_revision: int,
        observed_timing: M5RuntimeTiming | None,
    ) -> M5TransitionTimingReceipt:
        return self._store.append_transition_call_timing(
            epoch_id,
            contribution_kind,
            source_id,
            contribution_key_digest,
            anchor_revision,
            observed_timing,
        )

    def append_terminal_invocation_telemetry(
        self,
        invocation_id: str,
        event_id: str,
        epoch_id: int,
        terminal_logical_result_hash: str,
        call_timing: M5RuntimeTiming | None,
        call_timing_coverage: M5RuntimeTimingCoverage,
    ) -> None:
        self._store.append_terminal_invocation_telemetry(
            invocation_id,
            event_id,
            epoch_id,
            terminal_logical_result_hash,
            call_timing,
            call_timing_coverage,
        )

    def current_revision(self, epoch_id: int) -> int:
        return self._store.current_revision(epoch_id)

    def verifier_jobs(self, epoch_id: int) -> tuple[M5LogicalJobSpec, ...]:
        return self._store.verifier_jobs(epoch_id)

    def current_event_work(self, epoch_id: int) -> M5RuntimeWork:
        return self._store.current_event_work(epoch_id)

    def current_event_timing(
        self, epoch_id: int
    ) -> tuple[M5RuntimeTiming, M5RuntimeTimingCoverage]:
        return self._store.current_event_timing(epoch_id)

    def build_application(
        self,
        *,
        discovery: M5RequirementDiscoveryPort,
        verifier: M5RequirementVerifierPort,
        measurements: M5RuntimeMeasurementPort,
        post_seal_audit: M5PostSealAuditPort | None,
    ) -> M5TypedApplication:
        """Wire explicit providers into the intentionally pre-seal coordinator."""

        return M5TypedApplication(
            policies=self,
            structural=self,
            direct=self,
            runtime=self,
            runtime_reads=self,
            discovery=discovery,
            verifier=verifier,
            measurements=measurements,
            post_seal_audit=post_seal_audit,
        )
