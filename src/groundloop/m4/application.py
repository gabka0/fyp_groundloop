"""Persistence-neutral orchestration for one deterministic M4 corpus event.

This module owns no database transaction, retrieval implementation, or neural
model.  It coordinates injected ports and makes the ordering constraints of
the M4.1 vertical slice executable: exact withdrawal precedes discovery,
expandable jobs close atomically with their complete child set, observation
archival/working-state installation/verifier completion share one transaction,
and publication is requested only after the runtime and all three equality
surfaces say that sealing is safe.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from groundloop.domain import SemanticObservation, SubjectKind
from groundloop.errors import GroundLoopError, ValidationError
from groundloop.m4.contracts import (
    AdmittedPair,
    ChildClosure,
    CorpusUpdateIdentity,
    DiscoveryScope,
    JobCompletion,
    JobKind,
    JobState,
    LogicalJobSpec,
    PairKey,
    UpdateKind,
    stable_m4_digest,
)
from groundloop.m4.runtime.withdrawal import WithdrawalPlan


def _require_text(name: str, value: str) -> None:
    if not value.strip():
        raise ValidationError(f"{name} must be non-empty")


def _require_canonical(name: str, values: tuple[str, ...]) -> None:
    if values != tuple(sorted(set(values))):
        raise ValidationError(f"{name} must be sorted and unique")
    if any(not value.strip() for value in values):
        raise ValidationError(f"{name} must contain non-empty identifiers")


@dataclass(frozen=True, slots=True)
class DynamicEventPlan:
    """Semantic inputs needed to open one serialized M4 structural epoch."""

    update: CorpusUpdateIdentity
    inserted_chunk_version_ids: tuple[str, ...]
    deactivated_chunk_version_ids: tuple[str, ...]
    registered_claim_ids: tuple[str, ...]
    claim_registry_snapshot_id: str

    def __post_init__(self) -> None:
        _require_canonical(
            "inserted_chunk_version_ids", self.inserted_chunk_version_ids
        )
        _require_canonical(
            "deactivated_chunk_version_ids", self.deactivated_chunk_version_ids
        )
        _require_canonical("registered_claim_ids", self.registered_claim_ids)
        _require_text("claim_registry_snapshot_id", self.claim_registry_snapshot_id)
        if set(self.inserted_chunk_version_ids) & set(
            self.deactivated_chunk_version_ids
        ):
            raise ValidationError("inserted and deactivated chunks must be disjoint")
        if self.update.update_kind is UpdateKind.INSERT:
            if not self.inserted_chunk_version_ids:
                raise ValidationError("insert requires at least one inserted chunk")
            if self.deactivated_chunk_version_ids:
                raise ValidationError("insert cannot deactivate chunks")
        elif self.update.update_kind is UpdateKind.DELETE:
            if not self.deactivated_chunk_version_ids:
                raise ValidationError("delete requires at least one deactivated chunk")
            if self.inserted_chunk_version_ids:
                raise ValidationError("delete cannot insert chunks")
        elif not (
            self.inserted_chunk_version_ids
            and self.deactivated_chunk_version_ids
        ):
            raise ValidationError(
                "replacement requires inserted and deactivated chunks"
            )


@dataclass(frozen=True, slots=True)
class ApplicationExecutionPolicy:
    """Execution identities used when constructing deterministic logical jobs."""

    impact_discovery_execution_spec_hash: str
    frontier_retrieval_execution_spec_hash: str
    verifier_execution_spec_hash: str

    def __post_init__(self) -> None:
        for name, value in (
            (
                "impact_discovery_execution_spec_hash",
                self.impact_discovery_execution_spec_hash,
            ),
            (
                "frontier_retrieval_execution_spec_hash",
                self.frontier_retrieval_execution_spec_hash,
            ),
            ("verifier_execution_spec_hash", self.verifier_execution_spec_hash),
        ):
            if len(value) != 64:
                raise ValidationError(f"{name} must be a SHA-256 digest")


@dataclass(frozen=True, slots=True)
class StructuralWithdrawal:
    """Exact reverse-edge withdrawal plus any mandatory frontier fallbacks."""

    plan: WithdrawalPlan
    fallback_claim_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_canonical("fallback_claim_ids", self.fallback_claim_ids)


@dataclass(frozen=True, slots=True)
class OpenEventReceipt:
    epoch_id: int
    replayed: bool
    already_sealed: bool
    publication_id: str | None = None

    def __post_init__(self) -> None:
        if self.epoch_id <= 0:
            raise ValidationError("epoch_id must be positive")
        if self.already_sealed != (self.publication_id is not None):
            raise ValidationError("sealed replay and publication identity disagree")


@dataclass(frozen=True, slots=True)
class JobLease:
    """Result of an idempotent runtime acquire-or-observe transition."""

    job_id: str
    should_execute: bool
    already_completed: bool

    def __post_init__(self) -> None:
        _require_text("job_id", self.job_id)
        if self.should_execute == self.already_completed:
            raise ValidationError(
                "a job lease must either execute or identify an existing completion"
            )


@dataclass(frozen=True, slots=True)
class DiscoveryResult:
    """Persisted admission result returned for a discovery logical job."""

    root_job_id: str
    result_artifact_id: str
    result_artifact_hash: str
    admitted_pairs: tuple[AdmittedPair, ...]
    fallback_satisfied: bool = True

    def __post_init__(self) -> None:
        _require_text("root_job_id", self.root_job_id)
        _require_text("result_artifact_id", self.result_artifact_id)
        if len(self.result_artifact_hash) != 64:
            raise ValidationError("result_artifact_hash must be a SHA-256 digest")
        pair_keys = tuple(admitted.pair for admitted in self.admitted_pairs)
        if len(set(pair_keys)) != len(pair_keys):
            raise ValidationError("one discovery result cannot repeat an admitted pair")


@dataclass(frozen=True, slots=True)
class VerificationResult:
    result_artifact_id: str
    result_artifact_hash: str
    observation: SemanticObservation

    def __post_init__(self) -> None:
        _require_text("result_artifact_id", self.result_artifact_id)
        if len(self.result_artifact_hash) != 64:
            raise ValidationError("result_artifact_hash must be a SHA-256 digest")


@dataclass(frozen=True, slots=True)
class ObservationCompletionReceipt:
    """Outcome of one atomic observation-artifact/job completion operation."""

    artifact_stored: bool
    made_effective: bool


@dataclass(frozen=True, slots=True)
class SealingSnapshot:
    epoch_id: int
    revision: int
    ready: bool
    failed: bool
    open_job_ids: tuple[str, ...] = ()
    open_discovery_root_ids: tuple[str, ...] = ()
    fallback_blocked_root_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.epoch_id <= 0 or self.revision <= 0:
            raise ValidationError("sealing snapshot identifiers must be positive")
        for name, values in (
            ("open_job_ids", self.open_job_ids),
            ("open_discovery_root_ids", self.open_discovery_root_ids),
            ("fallback_blocked_root_ids", self.fallback_blocked_root_ids),
        ):
            _require_canonical(name, values)
        if self.ready and (
            self.failed
            or self.open_job_ids
            or self.open_discovery_root_ids
            or self.fallback_blocked_root_ids
        ):
            raise ValidationError("ready snapshot cannot contain incomplete work")


@dataclass(frozen=True, slots=True)
class PublicationReceipt:
    epoch_id: int
    publication_id: str
    replayed: bool = False

    def __post_init__(self) -> None:
        if self.epoch_id <= 0:
            raise ValidationError("published epoch_id must be positive")
        _require_text("publication_id", self.publication_id)


class EventRunState(StrEnum):
    SEALED = "sealed"
    REPLAYED = "replayed"
    FAILED = "failed"
    BLOCKED = "blocked"


@dataclass(frozen=True, slots=True)
class EventRunResult:
    event_id: str
    epoch_id: int
    state: EventRunState
    publication_id: str | None
    discovery_call_count: int
    verifier_call_count: int
    observation_artifact_count: int
    effective_observation_count: int
    inactive_completion_count: int
    failure_reason: str | None = None


class ExternalWorkFailure(GroundLoopError):
    """Expected failure from an admission or verifier worker boundary."""


class StructuralMutationPort(Protocol):
    """Owns exact withdrawal and the atomic structural/open transaction."""

    def plan_exact_withdrawal(
        self, event: DynamicEventPlan
    ) -> StructuralWithdrawal: ...

    def open_event(
        self,
        event: DynamicEventPlan,
        withdrawal: StructuralWithdrawal,
        root_jobs: tuple[LogicalJobSpec, ...],
        discovery_scopes: tuple[DiscoveryScope, ...],
    ) -> OpenEventReceipt: ...

    def chunk_is_active(self, chunk_version_id: str) -> bool: ...


class RuntimeTransitionPort(Protocol):
    """Owns idempotent runtime transitions after the structural commit."""

    def pending_claim_ids(self, epoch_id: int) -> tuple[str, ...]: ...

    def acquire_job(self, epoch_id: int, spec: LogicalJobSpec) -> JobLease: ...

    def complete_expansion(
        self,
        epoch_id: int,
        lease: JobLease,
        completion: JobCompletion,
        child_jobs: tuple[LogicalJobSpec, ...],
    ) -> None: ...

    def children_of(
        self, epoch_id: int, root_job_id: str
    ) -> tuple[LogicalJobSpec, ...]: ...

    def mark_fallback_blocked(self, epoch_id: int, root_job_id: str) -> None: ...

    def fail_epoch(self, epoch_id: int, reason: str) -> None: ...

    def sealing_snapshot(self, epoch_id: int) -> SealingSnapshot: ...


class AdmissionPort(Protocol):
    def discover(
        self, epoch_id: int, root_job: LogicalJobSpec
    ) -> DiscoveryResult: ...


class VerificationPort(Protocol):
    def verify(
        self, epoch_id: int, verifier_job: LogicalJobSpec
    ) -> VerificationResult: ...


class ObservationApplicationPort(Protocol):
    """Atomically archive a result, update working currency, and finish its job.

    The immutable artifact is stored for active and inactive completions.  It
    becomes effective only for ``COMPLETED_ACTIVE``.  Job terminalization and
    any effective working-overlay update are one all-or-nothing operation.
    This port must never mutate published M2 ``observation_currency``;
    promotion belongs exclusively to ``PublicationPort`` at sealing.
    """

    def complete_verifier_atomically(
        self,
        epoch_id: int,
        lease: JobLease,
        verifier_job: LogicalJobSpec,
        completion: JobCompletion,
        observation: SemanticObservation,
        *,
        make_effective: bool,
    ) -> ObservationCompletionReceipt: ...


class EqualityGatePort(Protocol):
    def check_grounding(self, epoch_id: int) -> None: ...

    def check_coordination(self, epoch_id: int) -> None: ...

    def check_evaluation(self, epoch_id: int) -> None: ...


class PublicationPort(Protocol):
    """Atomically validate, promote the working overlay, and seal publication."""

    def request_seal(
        self,
        epoch_id: int,
        expected_revision: int,
        update: CorpusUpdateIdentity,
    ) -> PublicationReceipt: ...


@dataclass(slots=True)
class M4Application:
    """Coordinate one event without owning persistence or semantic models."""

    structural: StructuralMutationPort
    runtime: RuntimeTransitionPort
    admission: AdmissionPort
    verifier: VerificationPort
    observations: ObservationApplicationPort
    equality_gates: EqualityGatePort
    publication: PublicationPort
    execution_policy: ApplicationExecutionPolicy

    def run_event(self, event: DynamicEventPlan) -> EventRunResult:
        withdrawal = self.structural.plan_exact_withdrawal(event)
        if withdrawal.plan.deactivated_chunk_ids != (
            event.deactivated_chunk_version_ids
        ):
            raise ValidationError("withdrawal plan does not match event deactivation")
        roots = self._root_jobs(event, withdrawal.fallback_claim_ids)
        scopes = tuple(
            DiscoveryScope(
                root_job_id=root.job_id,
                registry_snapshot_id=event.claim_registry_snapshot_id,
                registered_claim_ids=event.registered_claim_ids,
            )
            for root in roots
            if root.kind is JobKind.IMPACT_DISCOVERY
        )
        opened = self.structural.open_event(event, withdrawal, roots, scopes)
        if opened.already_sealed:
            return EventRunResult(
                event_id=event.update.event_id,
                epoch_id=opened.epoch_id,
                state=EventRunState.REPLAYED,
                publication_id=opened.publication_id,
                discovery_call_count=0,
                verifier_call_count=0,
                observation_artifact_count=0,
                effective_observation_count=0,
                inactive_completion_count=0,
            )

        self._validate_initial_pending(opened.epoch_id, event, withdrawal)
        discovery_calls = 0
        verifier_calls = 0
        observation_artifacts = 0
        effective_observations = 0
        inactive_completions = 0
        try:
            executable: list[tuple[LogicalJobSpec, JobLease, DiscoveryResult]] = []
            existing_children: list[LogicalJobSpec] = []
            for root in roots:
                lease = self.runtime.acquire_job(opened.epoch_id, root)
                if not lease.should_execute:
                    existing_children.extend(
                        self.runtime.children_of(opened.epoch_id, root.job_id)
                    )
                    continue
                discovered = self.admission.discover(opened.epoch_id, root)
                discovery_calls += 1
                self._validate_discovery(opened.epoch_id, root, discovered)
                if root.kind is JobKind.FRONTIER_RETRIEVE and not (
                    discovered.fallback_satisfied
                ):
                    self.runtime.mark_fallback_blocked(opened.epoch_id, root.job_id)
                    return EventRunResult(
                        event_id=event.update.event_id,
                        epoch_id=opened.epoch_id,
                        state=EventRunState.BLOCKED,
                        publication_id=None,
                        discovery_call_count=discovery_calls,
                        verifier_call_count=0,
                        observation_artifact_count=0,
                        effective_observation_count=0,
                        inactive_completion_count=0,
                        failure_reason=(
                            "mandatory frontier fallback remains unsatisfied"
                        ),
                    )
                executable.append((root, lease, discovered))

            existing_pairs = {
                child.pair for child in existing_children if child.pair is not None
            }
            owner_by_pair: dict[PairKey, str] = {}
            for root, _lease, discovered in executable:
                for admitted in discovered.admitted_pairs:
                    if admitted.pair in existing_pairs:
                        continue
                    owner = owner_by_pair.get(admitted.pair)
                    if owner is None or root.job_id < owner:
                        owner_by_pair[admitted.pair] = root.job_id

            for root, lease, discovered in executable:
                owned = tuple(
                    admitted
                    for admitted in discovered.admitted_pairs
                    if owner_by_pair.get(admitted.pair) == root.job_id
                )
                active = self._root_target_active(root)
                children = (
                    tuple(
                        sorted(
                            (
                                self._verifier_job(event, root, item.pair)
                                for item in owned
                            ),
                            key=lambda spec: spec.job_id,
                        )
                    )
                    if active
                    else ()
                )
                terminal = (
                    JobState.COMPLETED_ACTIVE
                    if active
                    else JobState.COMPLETED_INACTIVE
                )
                if not active:
                    inactive_completions += 1
                closure = ChildClosure.build(
                    parent_job_id=root.job_id,
                    result_artifact_hash=discovered.result_artifact_hash,
                    child_job_ids=tuple(child.job_id for child in children),
                )
                completion = JobCompletion.build(
                    job_id=root.job_id,
                    payload_hash=root.payload_hash,
                    execution_spec_hash=root.execution_spec_hash,
                    result_artifact_id=discovered.result_artifact_id,
                    result_artifact_hash=discovered.result_artifact_hash,
                    terminal_state=terminal,
                    child_closure=closure,
                )
                self.runtime.complete_expansion(
                    opened.epoch_id, lease, completion, children
                )

            all_children = tuple(
                child
                for root in roots
                for child in self.runtime.children_of(opened.epoch_id, root.job_id)
            )
            self._validate_global_children(all_children)
            for child in sorted(all_children, key=lambda spec: spec.job_id):
                lease = self.runtime.acquire_job(opened.epoch_id, child)
                if not lease.should_execute:
                    continue
                verified = self.verifier.verify(opened.epoch_id, child)
                verifier_calls += 1
                pair = child.pair
                if pair is None:  # LogicalJobSpec already enforces this.
                    raise ValidationError("verifier job is missing its pair")
                self._validate_verification(pair, verified)
                active = self.structural.chunk_is_active(pair.chunk_version_id)
                terminal = (
                    JobState.COMPLETED_ACTIVE
                    if active
                    else JobState.COMPLETED_INACTIVE
                )
                if not active:
                    inactive_completions += 1
                completion = JobCompletion.build(
                    job_id=child.job_id,
                    payload_hash=child.payload_hash,
                    execution_spec_hash=child.execution_spec_hash,
                    result_artifact_id=verified.result_artifact_id,
                    result_artifact_hash=verified.result_artifact_hash,
                    terminal_state=terminal,
                )
                observation_completion = (
                    self.observations.complete_verifier_atomically(
                        opened.epoch_id,
                        lease,
                        child,
                        completion,
                        verified.observation,
                        make_effective=active,
                    )
                )
                if observation_completion.artifact_stored:
                    observation_artifacts += 1
                if observation_completion.made_effective:
                    effective_observations += 1
        except ExternalWorkFailure as error:
            reason = str(error).strip() or type(error).__name__
            self.runtime.fail_epoch(opened.epoch_id, reason)
            return EventRunResult(
                event_id=event.update.event_id,
                epoch_id=opened.epoch_id,
                state=EventRunState.FAILED,
                publication_id=None,
                discovery_call_count=discovery_calls,
                verifier_call_count=verifier_calls,
                observation_artifact_count=observation_artifacts,
                effective_observation_count=effective_observations,
                inactive_completion_count=inactive_completions,
                failure_reason=reason,
            )

        snapshot = self.runtime.sealing_snapshot(opened.epoch_id)
        if snapshot.failed:
            return EventRunResult(
                event_id=event.update.event_id,
                epoch_id=opened.epoch_id,
                state=EventRunState.FAILED,
                publication_id=None,
                discovery_call_count=discovery_calls,
                verifier_call_count=verifier_calls,
                observation_artifact_count=observation_artifacts,
                effective_observation_count=effective_observations,
                inactive_completion_count=inactive_completions,
                failure_reason="epoch failed while external work was running",
            )
        if not snapshot.ready:
            return EventRunResult(
                event_id=event.update.event_id,
                epoch_id=opened.epoch_id,
                state=EventRunState.BLOCKED,
                publication_id=None,
                discovery_call_count=discovery_calls,
                verifier_call_count=verifier_calls,
                observation_artifact_count=observation_artifacts,
                effective_observation_count=effective_observations,
                inactive_completion_count=inactive_completions,
                failure_reason="runtime reports open jobs, scopes, or fallback work",
            )
        try:
            self.equality_gates.check_grounding(opened.epoch_id)
            self.equality_gates.check_coordination(opened.epoch_id)
            self.equality_gates.check_evaluation(opened.epoch_id)
        except ExternalWorkFailure as error:
            reason = str(error).strip() or type(error).__name__
            self.runtime.fail_epoch(opened.epoch_id, reason)
            return EventRunResult(
                event_id=event.update.event_id,
                epoch_id=opened.epoch_id,
                state=EventRunState.FAILED,
                publication_id=None,
                discovery_call_count=discovery_calls,
                verifier_call_count=verifier_calls,
                observation_artifact_count=observation_artifacts,
                effective_observation_count=effective_observations,
                inactive_completion_count=inactive_completions,
                failure_reason=reason,
            )
        published = self.publication.request_seal(
            opened.epoch_id, snapshot.revision, event.update
        )
        if published.epoch_id != opened.epoch_id:
            raise ValidationError("publication receipt names another epoch")
        return EventRunResult(
            event_id=event.update.event_id,
            epoch_id=opened.epoch_id,
            state=EventRunState.SEALED,
            publication_id=published.publication_id,
            discovery_call_count=discovery_calls,
            verifier_call_count=verifier_calls,
            observation_artifact_count=observation_artifacts,
            effective_observation_count=effective_observations,
            inactive_completion_count=inactive_completions,
        )

    def _root_jobs(
        self, event: DynamicEventPlan, fallback_claim_ids: tuple[str, ...]
    ) -> tuple[LogicalJobSpec, ...]:
        roots: list[LogicalJobSpec] = []
        for chunk_id in event.inserted_chunk_version_ids:
            roots.append(
                self._job(
                    event,
                    kind=JobKind.IMPACT_DISCOVERY,
                    execution_hash=(
                        self.execution_policy.impact_discovery_execution_spec_hash
                    ),
                    chunk_id=chunk_id,
                )
            )
        for claim_id in fallback_claim_ids:
            roots.append(
                self._job(
                    event,
                    kind=JobKind.FRONTIER_RETRIEVE,
                    execution_hash=(
                        self.execution_policy.frontier_retrieval_execution_spec_hash
                    ),
                    claim_id=claim_id,
                )
            )
        return tuple(sorted(roots, key=lambda spec: spec.job_id))

    def _job(
        self,
        event: DynamicEventPlan,
        *,
        kind: JobKind,
        execution_hash: str,
        parent_job_id: str | None = None,
        claim_id: str = "",
        chunk_id: str = "",
    ) -> LogicalJobSpec:
        job_id = LogicalJobSpec.derive_job_id(
            event_id=event.update.event_id,
            kind=kind,
            candidate_policy_id=event.update.candidate_policy_id,
            execution_spec_hash=execution_hash,
            parent_job_id=parent_job_id or "",
            claim_id=claim_id,
            chunk_version_id=chunk_id,
        )
        payload_hash = stable_m4_digest(
            "m4-application-job-payload-v1",
            event.update.payload_hash,
            kind.value,
            parent_job_id or "",
            claim_id,
            chunk_id,
        )
        pair = PairKey(claim_id, chunk_id) if kind is JobKind.VERIFY_PAIR else None
        return LogicalJobSpec(
            job_id=job_id,
            event_id=event.update.event_id,
            kind=kind,
            candidate_policy_id=event.update.candidate_policy_id,
            payload_hash=payload_hash,
            execution_spec_hash=execution_hash,
            parent_job_id=parent_job_id,
            pair=pair,
            target_claim_id=claim_id if kind is JobKind.FRONTIER_RETRIEVE else None,
            target_chunk_version_id=(
                chunk_id if kind is JobKind.IMPACT_DISCOVERY else None
            ),
            expandable=kind is not JobKind.VERIFY_PAIR,
        )

    def _verifier_job(
        self, event: DynamicEventPlan, root: LogicalJobSpec, pair: PairKey
    ) -> LogicalJobSpec:
        return self._job(
            event,
            kind=JobKind.VERIFY_PAIR,
            execution_hash=self.execution_policy.verifier_execution_spec_hash,
            parent_job_id=root.job_id,
            claim_id=pair.claim_id,
            chunk_id=pair.chunk_version_id,
        )

    def _validate_initial_pending(
        self,
        epoch_id: int,
        event: DynamicEventPlan,
        withdrawal: StructuralWithdrawal,
    ) -> None:
        pending = set(self.runtime.pending_claim_ids(epoch_id))
        if event.inserted_chunk_version_ids and not set(
            event.registered_claim_ids
        ) <= pending:
            raise ValidationError(
                "open impact discovery must make the registry snapshot PENDING"
            )
        if not set(withdrawal.fallback_claim_ids) <= pending:
            raise ValidationError("open fallback roots must make their claims PENDING")

    @staticmethod
    def _validate_discovery(
        epoch_id: int, root: LogicalJobSpec, discovered: DiscoveryResult
    ) -> None:
        if discovered.root_job_id != root.job_id:
            raise ValidationError("admission result belongs to another root")
        for admitted in discovered.admitted_pairs:
            if admitted.epoch_id != epoch_id:
                raise ValidationError("admitted pair belongs to another epoch")
            if admitted.candidate_policy_id != root.candidate_policy_id:
                raise ValidationError("admitted pair uses another candidate policy")
            if root.kind is JobKind.IMPACT_DISCOVERY and (
                admitted.pair.chunk_version_id != root.target_chunk_version_id
            ):
                raise ValidationError("impact admission escaped its chunk scope")
            if root.kind is JobKind.FRONTIER_RETRIEVE and (
                admitted.pair.claim_id != root.target_claim_id
            ):
                raise ValidationError("frontier admission escaped its claim scope")

    def _root_target_active(self, root: LogicalJobSpec) -> bool:
        if root.target_chunk_version_id is None:
            return True
        return self.structural.chunk_is_active(root.target_chunk_version_id)

    @staticmethod
    def _validate_global_children(children: tuple[LogicalJobSpec, ...]) -> None:
        ids = tuple(child.job_id for child in children)
        pairs = tuple(child.pair for child in children)
        if len(set(ids)) != len(ids):
            raise ValidationError("runtime exposed duplicate verifier job IDs")
        if len(set(pairs)) != len(pairs):
            raise ValidationError("event-wide admitted-pair deduplication failed")

    @staticmethod
    def _validate_verification(
        pair: PairKey, verified: VerificationResult
    ) -> None:
        observation = verified.observation
        if observation.subject_kind is not SubjectKind.CLAIM:
            raise ValidationError("M4 verifier observation must target a claim")
        if observation.subject_id != pair.claim_id:
            raise ValidationError("verifier observation targets another claim")
        if observation.chunk_version_id != pair.chunk_version_id:
            raise ValidationError("verifier observation targets another chunk")
