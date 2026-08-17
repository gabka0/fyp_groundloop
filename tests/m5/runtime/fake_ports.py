"""Deterministic transaction-aware fake ports for the typed M5 coordinator.

These fakes deliberately keep strict published and working repositories
separate.  External discovery/verifier methods assert that no fake persistence
transaction is active, while every state transition owns one bounded fake
transaction.  The sealed state is recomputed through the independent M5
Python reference oracle.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta

from groundloop.domain import (
    AnswerVersion,
    ChunkVersion,
    Claim,
    DecisionPolicy,
    DocumentVersion,
    ModelStamp,
    Question,
    StatusDelta,
    SubjectKind,
)
from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.events import (
    DeleteDocumentVersionEvent,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    ReplaceDocumentVersionEvent,
    apply_event,
)
from groundloop.m4.application import (
    DynamicEventPlan,
    ObservationCompletionReceipt,
    OpenEventReceipt,
    PublicationReceipt,
    StructuralWithdrawal,
)
from groundloop.m4.contracts import (
    DiscoveryScope as M4DiscoveryScope,
)
from groundloop.m4.contracts import (
    JobKind,
    PairKey,
    VectorIndexKind,
    stable_m4_digest,
)
from groundloop.m4.contracts import (
    LogicalJobSpec as M4LogicalJobSpec,
)
from groundloop.m4.pipeline import InsertedDocument, StructuralPayload
from groundloop.m4.runtime.withdrawal import (
    ObservationDependency,
    ReverseDependencyIndex,
    plan_withdrawal,
)
from groundloop.m5.digests import stable_m5_digest, text_field
from groundloop.m5.domain import (
    ClaimCertificateArtifact,
    GroupMatchingCertificateArtifact,
)
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    ReplaceGroupEvent,
    RetireGroupEvent,
    legacy_event_payload_digest,
    m5_event_payload_digest,
)
from groundloop.m5.reference import (
    M5ReferenceStates,
    build_reference_claim_certificate,
    build_reference_group_certificate,
    compute_reference_states,
)
from groundloop.m5.repository import M5Repository
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.application import (
    M5DirectExecutionReceipt,
    M5DirectOpenPlan,
    M5DiscoveryExecution,
    M5ExternalWorkFailure,
    M5RequirementRootDeclaration,
    M5TerminalInvocationTelemetry,
    M5TypedApplication,
    M5VerifierExecution,
)
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5AcquisitionDisposition,
    M5AttemptCompletionReceipt,
    M5AttemptOutput,
    M5CandidatePolicyManifest,
    M5ChangedStateReference,
    M5DirectAttemptReturnReceipt,
    M5DirectLateReturnDisposition,
    M5DirectLateReturnReceipt,
    M5DirectNormalReturnReceipt,
    M5DiscoveryDirection,
    M5EventRunResult,
    M5ExecutionEvidenceDisposition,
    M5JobAttempt,
    M5JobCompletion,
    M5JobKind,
    M5JobLease,
    M5JobState,
    M5LeaseTerminalProjection,
    M5LogicalJobSpec,
    M5ReplayedOutcome,
    M5RequirementAdmissionChannel,
    M5RequirementAttemptReturnReceipt,
    M5RequirementChannelHit,
    M5RequirementDiscoveryResult,
    M5RequirementFallbackKey,
    M5RequirementPairInput,
    M5RequirementReturnDisposition,
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
    M5ScopeState,
    M5StateReferenceKind,
    M5TerminalReason,
    M5TransitionTimingAnchor,
    M5TransitionTimingReceipt,
    M5TypedDirectReturnKind,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
    RequirementRegistrySnapshotEntry,
    SemanticPairKey,
)
from groundloop.m5.runtime.frontier import (
    build_rank_interleaved_discovery_result,
    build_root_barrier_plan,
    deduplicate_discovery_results,
)
from groundloop.repository import InMemoryRepository

FAKE_STAMP = ModelStamp("fake-m5", "v1", "prompt-v1")
FAKE_LEASE_BASE = datetime(2099, 1, 1, tzinfo=UTC)
FAKE_OBSERVED_TIMING = M5RuntimeTiming(
    coordinator_non_db_non_neural_ns=1,
    neural_wall_ns=1,
    postgres_roundtrip_wall_ns=1,
    external_io_wall_ns=1,
    end_to_end_wall_ns=4,
)


def sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _add_work(*items: M5RuntimeWork) -> M5RuntimeWork:
    return M5RuntimeWork(
        **{
            name: sum(getattr(item, name) for item in items)
            for name in M5RuntimeWork.counter_names()
        }
    )


def _add_timing(*items: M5RuntimeTiming) -> M5RuntimeTiming:
    def optional_sum(name: str) -> int | None:
        values: tuple[int | None, ...] = tuple(getattr(item, name) for item in items)
        if not values or any(value is None for value in values):
            return None
        return sum(value for value in values if value is not None)

    return M5RuntimeTiming(
        coordinator_non_db_non_neural_ns=sum(
            item.coordinator_non_db_non_neural_ns for item in items
        ),
        neural_wall_ns=sum(item.neural_wall_ns for item in items),
        postgres_roundtrip_wall_ns=sum(
            item.postgres_roundtrip_wall_ns for item in items
        ),
        external_io_wall_ns=sum(item.external_io_wall_ns for item in items),
        end_to_end_wall_ns=sum(item.end_to_end_wall_ns for item in items),
        postgres_server_execution_ns=optional_sum("postgres_server_execution_ns"),
        postgres_lock_wait_ns=optional_sum("postgres_lock_wait_ns"),
        postgres_wal_bytes=optional_sum("postgres_wal_bytes"),
        postgres_shared_block_reads=optional_sum("postgres_shared_block_reads"),
    )


def _timing_image(
    points: tuple[M5RuntimeTiming | None, ...],
) -> tuple[M5RuntimeTiming, M5RuntimeTimingCoverage]:
    observed = tuple(point for point in points if point is not None)
    timing = _add_timing(*observed) if observed else M5RuntimeTiming()
    expected_count = len(points)
    required_observed = len(observed)

    def optional_counts(name: str) -> tuple[int, int, int]:
        optional_observed = sum(
            point is not None and getattr(point, name) is not None for point in points
        )
        return (expected_count, optional_observed, expected_count - optional_observed)

    return timing, M5RuntimeTimingCoverage(
        expected_count,
        required_observed,
        expected_count - required_observed,
        *optional_counts("postgres_server_execution_ns"),
        *optional_counts("postgres_lock_wait_ns"),
        *optional_counts("postgres_wal_bytes"),
        *optional_counts("postgres_shared_block_reads"),
        False,
    )


def make_manifest(policy_version: str = "policy-v1") -> M5CandidatePolicyManifest:
    return M5CandidatePolicyManifest.build(
        embedding_model_artifact_id="fake-embedding-artifact-v1",
        requirement_role_template_hash=sha("requirement-role"),
        chunk_role_template_hash=sha("chunk-role"),
        vector_method_version="fake-vector-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=sha("vector-build"),
        vector_search_config_hash=sha("vector-search"),
        lexical_method_version="fake-lexical-v1",
        lexical_config_hash=sha("lexical-config"),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="pg_catalog.english",
        fusion_version="rank-interleave-v1",
        reverse_budget_per_inserted_chunk=4,
        forward_budget_per_requirement=4,
        verifier_execution_spec_hash=sha("requirement-verifier-execution"),
        decision_policy_version=policy_version,
        lineage_safety_override=True,
    )


def make_repository(
    *,
    required_claims: tuple[str, ...] = ("claim-required",),
    optional_claims: tuple[str, ...] = ("claim-optional",),
) -> M5Repository:
    base = InMemoryRepository()
    apply_event(
        base,
        PolicyChangeEvent("bootstrap-policy", DecisionPolicy("policy-v1", 0.8, 0.8)),
    )
    base.register_question(Question("question-1", "Question?"))
    base.register_answer(
        AnswerVersion("answer-1", "question-1", "Answer.", FAKE_STAMP),
        tuple(
            Claim(claim_id, "answer-1", claim_id, FAKE_STAMP, True)
            for claim_id in required_claims
        )
        + tuple(
            Claim(claim_id, "answer-1", claim_id, FAKE_STAMP, False)
            for claim_id in optional_claims
        ),
    )
    return M5Repository(base)


def _stage_event(repository: M5Repository, plan: M5TypedEventPlan) -> None:
    event = plan.event
    if isinstance(
        event,
        (
            InsertDocumentEvent,
            DeleteDocumentVersionEvent,
            ReplaceDocumentVersionEvent,
            PolicyChangeEvent,
            ObserveEvent,
        ),
    ):
        apply_event(repository.base, event)
        _ = repository.current_point
        return
    point = repository.begin_event_epoch()
    if isinstance(event, RegisterGroupEvent):
        repository.register_group(event.group, point.epoch_id)
    elif isinstance(event, ReplaceGroupEvent):
        repository.replace_group(
            event.old_group_version_id, event.successor, point.epoch_id
        )
    elif isinstance(event, RetireGroupEvent):
        repository.retire_group(event.group_version_id, point.epoch_id, event.event_id)
    else:
        repository.register_requirement_observation(event.observation, point)


def requirement_snapshot(repository: M5Repository) -> RequirementRegistrySnapshot:
    entries = tuple(
        RequirementRegistrySnapshotEntry(
            requirement_version_id=requirement.requirement_version_id,
            group_version_id=requirement.group_version_id,
            group_family_id=repository.group(
                requirement.group_version_id
            ).group_family_id,
            owner_claim_id=repository.owner_claim_id(
                requirement.requirement_version_id
            ),
            normalized_requirement_text=requirement.requirement_text,
            requirement_text_hash=requirement.requirement_text_hash,
        )
        for requirement_id in sorted(repository.all_requirement_ids())
        if repository.is_requirement_active(requirement_id)
        for requirement in (repository.requirement(requirement_id),)
    )
    return RequirementRegistrySnapshot.build(entries)


def chunk_snapshot(repository: M5Repository) -> ActiveChunkSnapshot:
    return ActiveChunkSnapshot.build(
        tuple(
            ActiveChunkSnapshotEntry.build(
                chunk_version_id=chunk.chunk_version_id, chunk_text=chunk.text
            )
            for chunk, validity in repository.base.export_snapshot().chunks
            if validity[1] is None
        )
    )


def make_typed_plan(
    world: FakeTypedWorld,
    event: InsertDocumentEvent
    | DeleteDocumentVersionEvent
    | ReplaceDocumentVersionEvent
    | PolicyChangeEvent
    | ObserveEvent
    | RegisterGroupEvent
    | ReplaceGroupEvent
    | RetireGroupEvent
    | ObserveRequirementEvent,
) -> M5TypedEventPlan:
    staged = deepcopy(world.published)
    direct_plan: DynamicEventPlan | None = None
    if isinstance(event, InsertDocumentEvent):
        update_kind = "insert"
        inserted = tuple(sorted(chunk.chunk_version_id for chunk in event.chunks))
        deactivated: tuple[str, ...] = ()
    elif isinstance(event, DeleteDocumentVersionEvent):
        update_kind = "delete"
        inserted = ()
        deactivated = tuple(
            sorted(staged.base.chunk_ids_of_document_version(event.document_version_id))
        )
    elif isinstance(event, ReplaceDocumentVersionEvent):
        update_kind = "replace"
        inserted = tuple(sorted(chunk.chunk_version_id for chunk in event.chunks))
        deactivated = tuple(
            sorted(
                staged.base.chunk_ids_of_document_version(event.old_document_version_id)
            )
        )
    else:
        update_kind = ""
        inserted = ()
        deactivated = ()
    payload_hash = (
        m5_event_payload_digest(event)
        if isinstance(
            event,
            (
                RegisterGroupEvent,
                ReplaceGroupEvent,
                RetireGroupEvent,
                ObserveRequirementEvent,
            ),
        )
        else legacy_event_payload_digest(event)
    )
    if update_kind:
        from groundloop.m4.contracts import CorpusUpdateIdentity, UpdateKind

        direct_plan = DynamicEventPlan(
            update=CorpusUpdateIdentity(
                event_id=event.event_id,
                payload_hash=payload_hash,
                update_kind=UpdateKind(update_kind),
                previous_published_epoch_id=world.published.current_epoch,
                candidate_policy_id=world.manifest.candidate_policy_id,
            ),
            inserted_chunk_version_ids=inserted,
            deactivated_chunk_version_ids=deactivated,
            registered_claim_ids=tuple(sorted(staged.base.all_claim_ids())),
            claim_registry_snapshot_id="fake-claim-registry-v1",
        )
    provisional = M5TypedEventPlan(
        structural_event_id=event.event_id,
        event=event,
        payload_hash=payload_hash,
        direct_plan=direct_plan,
        candidate_policy_id=world.manifest.candidate_policy_id,
        candidate_policy_manifest_hash=world.manifest.manifest_hash,
        requirement_registry_snapshot=RequirementRegistrySnapshot.build(()),
        active_chunk_snapshot=ActiveChunkSnapshot.build(()),
        expected_previous_published_epoch_id=world.published.current_epoch,
    )
    _stage_event(staged, provisional)
    return M5TypedEventPlan(
        structural_event_id=event.event_id,
        event=event,
        payload_hash=payload_hash,
        direct_plan=direct_plan,
        candidate_policy_id=world.manifest.candidate_policy_id,
        candidate_policy_manifest_hash=world.manifest.manifest_hash,
        requirement_registry_snapshot=requirement_snapshot(staged),
        active_chunk_snapshot=chunk_snapshot(staged),
        expected_previous_published_epoch_id=world.published.current_epoch,
    )


@dataclass(frozen=True, slots=True)
class FakeDiscoveryOutcome:
    pairs: tuple[SemanticPairKey, ...] = ()
    failure_reason: M5RunFailureReason | None = None
    retryable: bool = False
    execution_disposition: M5ExecutionEvidenceDisposition = (
        M5ExecutionEvidenceDisposition.RETURNED
    )
    attempt_timing: M5RuntimeTiming | None = FAKE_OBSERVED_TIMING


@dataclass(frozen=True, slots=True)
class FakeVerifierOutcome:
    scores: tuple[float, float, float] = (0.9, 0.05, 0.05)
    failure_reason: M5RunFailureReason | None = None
    retryable: bool = False
    execution_disposition: M5ExecutionEvidenceDisposition = (
        M5ExecutionEvidenceDisposition.RETURNED
    )
    attempt_timing: M5RuntimeTiming | None = FAKE_OBSERVED_TIMING


class FakeDirectInterruption(RuntimeError):
    """A deterministic fake-only interruption before direct execution."""


@dataclass(frozen=True, slots=True)
class FakeDirectSelectedOutcome:
    """One fake-only successful outer settlement that loses terminal cutoff."""

    return_kind: M5TypedDirectReturnKind
    branch: str
    outcome: M5ReplayedOutcome
    call_work: M5RuntimeWork = M5RuntimeWork()

    def __post_init__(self) -> None:
        if not isinstance(self.return_kind, M5TypedDirectReturnKind):
            raise ValidationError("fake direct selected outcome has another kind")
        if self.branch not in {"normal", "late"}:
            raise ValidationError("fake direct selected branch must be normal or late")
        if not isinstance(self.outcome, M5ReplayedOutcome):
            raise ValidationError("fake direct selected outcome must be terminal")
        if not isinstance(self.call_work, M5RuntimeWork):
            raise ValidationError("fake direct selected outcome has invalid call work")
        replace(self.call_work)


@dataclass(slots=True)
class _FakeJob:
    spec: M5LogicalJobSpec
    state: M5JobState = M5JobState.DECLARED
    attempts: list[M5JobAttempt] = field(default_factory=list)
    dispatch_digests: dict[str, str] = field(default_factory=dict)
    completion: M5JobCompletion | None = None


@dataclass(slots=True)
class _FakeEpoch:
    plan: M5TypedEventPlan
    epoch_id: int
    working: M5Repository
    before_states: M5ReferenceStates
    declarations: dict[str, M5RequirementRootDeclaration]
    direct_roots: tuple[M4LogicalJobSpec, ...]
    jobs: dict[str, _FakeJob]
    scope_states: dict[str, M5ScopeState]
    current_open_receipt: OpenEventReceipt | None = None
    revision: int = 1
    root_results: dict[str, M5RequirementDiscoveryResult] = field(default_factory=dict)
    attempt_outputs: dict[str, M5AttemptOutput] = field(default_factory=dict)
    root_barrier: M5RootBarrierReceipt | None = None
    event_work: M5RuntimeWork = M5RuntimeWork()
    direct_done: bool = False
    failed: bool = False
    terminal_result: M5EventRunResult | None = None
    sealed_states: M5ReferenceStates | None = None
    pending_transition_anchor: M5TransitionTimingAnchor | None = None
    timing_points: list[M5RuntimeTiming | None] = field(default_factory=list)
    transition_observations: dict[
        tuple[M5RuntimeWorkContributionKind, str, int], M5RuntimeTimingObservation
    ] = field(default_factory=dict)
    attempt_failures: dict[
        str,
        tuple[
            M5AttemptCompletionReceipt,
            str,
            M5RuntimeWork,
            M5RuntimeTiming | None,
            M5TerminalReason | None,
        ],
    ] = field(default_factory=dict)
    requirement_returns: dict[
        str,
        tuple[
            M5RequirementAttemptReturnReceipt,
            M5AttemptOutput,
            M5ExecutionEvidenceDisposition,
            M5RuntimeWork,
            M5RuntimeTiming | None,
        ],
    ] = field(default_factory=dict)


@dataclass(slots=True)
class FakeTypedWorld:
    published: M5Repository
    manifest: M5CandidatePolicyManifest = field(default_factory=make_manifest)
    next_runtime_epoch_id: int = 0
    epochs_by_event: dict[str, _FakeEpoch] = field(default_factory=dict)
    operation_log: list[str] = field(default_factory=list)
    transaction_depth: int = 0
    external_call_count: int = 0
    discovery_outcomes: dict[
        tuple[str, M5DiscoveryDirection, str], list[FakeDiscoveryOutcome]
    ] = field(default_factory=dict)
    verifier_outcomes: dict[tuple[str, str], list[FakeVerifierOutcome]] = field(
        default_factory=dict
    )
    fallback_by_event: dict[str, tuple[M5RequirementFallbackKey, ...]] = field(
        default_factory=dict
    )
    direct_blocked_by_event: dict[str, M5RunFailureReason] = field(default_factory=dict)
    direct_failed_by_event: dict[str, M5RunFailureReason] = field(default_factory=dict)
    direct_interruptions_by_event: dict[str, int] = field(default_factory=dict)
    direct_selected_outcomes_by_event: dict[str, FakeDirectSelectedOutcome] = field(
        default_factory=dict
    )
    direct_selected_receipts_by_event: dict[str, M5DirectAttemptReturnReceipt] = field(
        default_factory=dict
    )
    late_attempt_artifacts: list[tuple[int, str]] = field(default_factory=list)
    takeover_jobs_once: set[str] = field(default_factory=set)
    return_dispositions_by_job: dict[str, list[M5RequirementReturnDisposition]] = field(
        default_factory=dict
    )
    terminal_acquisition_after_by_kind: dict[M5JobKind, int] = field(
        default_factory=dict
    )
    terminal_acquisition_failure_reason: M5RunFailureReason = (
        M5RunFailureReason.RETRIEVAL_ERROR
    )
    terminal_job_failure_after_by_kind: dict[M5JobKind, int] = field(
        default_factory=dict
    )
    terminal_job_failure_reason: M5TerminalReason = M5TerminalReason.RETRIEVAL_ERROR
    failure_mutator_race_reason: M5RunFailureReason | None = None
    seal_mutator_race_outcome: M5ReplayedOutcome | None = None
    seal_mutator_race_failure_reason: M5RunFailureReason = (
        M5RunFailureReason.RETRIEVAL_ERROR
    )
    transition_timing: M5RuntimeTiming | None = FAKE_OBSERVED_TIMING
    terminal_call_timing: M5RuntimeTiming | None = FAKE_OBSERVED_TIMING
    next_invocation_ordinal: int = 1
    terminal_telemetry: dict[
        str,
        tuple[str, int, str, M5RuntimeTiming | None, M5RuntimeTimingCoverage],
    ] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.next_runtime_epoch_id == 0:
            self.next_runtime_epoch_id = self.published.current_epoch + 1

    def allocate_runtime_epoch_id(self) -> int:
        epoch_id = self.next_runtime_epoch_id
        self.next_runtime_epoch_id += 1
        return epoch_id

    @contextmanager
    def transaction(self, name: str) -> Iterator[None]:
        if self.transaction_depth:
            raise AssertionError("fake persistence transactions cannot nest")
        self.transaction_depth += 1
        self.operation_log.append(f"tx:{name}:begin")
        try:
            yield
        finally:
            self.operation_log.append(f"tx:{name}:end")
            self.transaction_depth -= 1

    def assert_external_boundary(self, name: str) -> None:
        if self.transaction_depth:
            raise AssertionError("model work occurred inside a transaction")
        self.external_call_count += 1
        self.operation_log.append(f"external:{name}")

    def epoch(self, epoch_id: int) -> _FakeEpoch:
        for epoch in self.epochs_by_event.values():
            if epoch.epoch_id == epoch_id:
                return epoch
        raise InvalidEventError("fake typed epoch does not exist")

    @staticmethod
    def _anchor_key(
        anchor: M5TransitionTimingAnchor,
    ) -> tuple[M5RuntimeWorkContributionKind, str, int]:
        return (anchor.contribution_kind, anchor.source_id, anchor.anchor_revision)

    def install_transition_anchor(
        self, epoch: _FakeEpoch, anchor: M5TransitionTimingAnchor
    ) -> None:
        if anchor.epoch_id != epoch.epoch_id:
            raise ValidationError("fake transition anchor belongs to another epoch")
        pending = epoch.pending_transition_anchor
        if pending is not None:
            key = self._anchor_key(pending)
            if key in epoch.transition_observations:
                raise AssertionError("fake pending timing point was already resolved")
            epoch.transition_observations[key] = M5RuntimeTimingObservation.build(None)
            epoch.timing_points.append(None)
            self.operation_log.append(
                f"timing-missing:{pending.contribution_kind.value}"
            )
        epoch.pending_transition_anchor = anchor

    def terminalize_timing(self, epoch: _FakeEpoch) -> None:
        pending = epoch.pending_transition_anchor
        if pending is not None:
            key = self._anchor_key(pending)
            epoch.transition_observations[key] = M5RuntimeTimingObservation.build(None)
            epoch.timing_points.append(None)
            epoch.pending_transition_anchor = None
            self.operation_log.append(
                f"timing-missing:{pending.contribution_kind.value}"
            )
        epoch.timing_points.append(None)

    def timing_image(
        self, epoch: _FakeEpoch, *, project_pending: bool
    ) -> tuple[M5RuntimeTiming, M5RuntimeTimingCoverage]:
        points = tuple(epoch.timing_points)
        if project_pending and epoch.pending_transition_anchor is not None:
            points += (None,)
        return _timing_image(points)

    def root_target(
        self, epoch: _FakeEpoch, job: M5LogicalJobSpec
    ) -> tuple[M5DiscoveryDirection, str]:
        scope = epoch.declarations[job.logical_job_id].scope
        if scope.direction is M5DiscoveryDirection.FORWARD_REQUIREMENT:
            assert scope.requirement_version_id is not None
            return scope.direction, scope.requirement_version_id
        assert scope.inserted_chunk_version_id is not None
        return scope.direction, scope.inserted_chunk_version_id

    def pending_by_owner(self, epoch_id: int) -> dict[str, int]:
        epoch = self.epoch(epoch_id)
        counts: dict[str, int] = {}
        for root_id, declaration in epoch.declarations.items():
            state = epoch.scope_states[root_id]
            if state not in {M5ScopeState.OPEN, M5ScopeState.RESULT_STAGED}:
                continue
            if declaration.scope.direction is M5DiscoveryDirection.REVERSE_CHUNK:
                owners = {
                    entry.owner_claim_id
                    for entry in epoch.plan.requirement_registry_snapshot.entries
                }
            else:
                assert declaration.scope.requirement_version_id is not None
                owners = {
                    epoch.plan.requirement_registry_snapshot.member(
                        declaration.scope.requirement_version_id
                    ).owner_claim_id
                }
            for owner in owners:
                counts[owner] = counts.get(owner, 0) + 1
        for job in epoch.jobs.values():
            if job.spec.job_kind is not M5JobKind.VERIFY_REQUIREMENT_PAIR:
                continue
            if job.state in {
                M5JobState.DECLARED,
                M5JobState.RUNNING,
                M5JobState.RETRYABLE_FAILED,
                M5JobState.TERMINAL_FAILED,
            }:
                assert job.spec.pair is not None
                owner = epoch.plan.requirement_registry_snapshot.member(
                    job.spec.pair.subject_id
                ).owner_claim_id
                counts[owner] = counts.get(owner, 0) + 1
        return dict(sorted(counts.items()))


@dataclass(slots=True)
class FakePolicies:
    world: FakeTypedWorld

    def candidate_policy(self, candidate_policy_id: str) -> M5CandidatePolicyManifest:
        if candidate_policy_id != self.world.manifest.candidate_policy_id:
            raise InvalidEventError("unknown fake candidate policy")
        return self.world.manifest


@dataclass(slots=True)
class FakeStructural:
    world: FakeTypedWorld

    def plan_exact_requirement_withdrawal(
        self, event: M5TypedEventPlan
    ) -> M5RequirementWithdrawalPlan:
        self.world.operation_log.append("plan:requirement-withdrawal")
        deactivated = (
            ()
            if event.direct_plan is None
            else event.direct_plan.deactivated_chunk_version_ids
        )
        withdrawn_records = tuple(
            sorted(
                (
                    record
                    for record in (
                        self.world.published.current_requirement_observations()
                    )
                    if record.observation.chunk_version_id in deactivated
                ),
                key=lambda record: record.observation.observation_id,
            )
        )
        automatic_fallback = tuple(
            M5RequirementFallbackKey(requirement_id, event.candidate_policy_id)
            for requirement_id in sorted(
                {
                    record.observation.subject_id
                    for record in withdrawn_records
                    if event.requirement_registry_snapshot.contains(
                        record.observation.subject_id
                    )
                }
            )
        )
        fallback = tuple(
            sorted(
                set(automatic_fallback)
                | set(self.world.fallback_by_event.get(event.structural_event_id, ()))
            )
        )
        pair_digests = tuple(
            sorted(
                {
                    SemanticPairKey(
                        SubjectKind.REQUIREMENT,
                        record.observation.subject_id,
                        record.observation.chunk_version_id,
                    ).semantic_pair_digest
                    for record in withdrawn_records
                }
            )
        )
        observation_ids = tuple(
            record.observation.observation_id for record in withdrawn_records
        )
        plan_hash = digests.requirement_withdrawal_plan_digest(
            event_id=event.structural_event_id,
            deactivated_chunk_version_ids=deactivated,
            withdrawn_candidate_pair_digests=pair_digests,
            withdrawn_observation_ids=observation_ids,
            cancelled_job_ids=(),
            fallback_keys=(
                (key.requirement_version_id, key.candidate_policy_id)
                for key in fallback
            ),
        )
        return M5RequirementWithdrawalPlan(
            event.structural_event_id,
            deactivated,
            pair_digests,
            observation_ids,
            (),
            fallback,
            plan_hash,
        )

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
        if event.direct_plan is not None:
            if direct_payload is None or direct_withdrawal is None:
                raise ValidationError("fake direct event lacks payload/withdrawal")
            if (
                direct_withdrawal.plan.deactivated_chunk_ids
                != event.direct_plan.deactivated_chunk_version_ids
            ):
                raise ValidationError("fake direct withdrawal drift")
            if (
                direct_payload.inserted_chunk_ids
                != event.direct_plan.inserted_chunk_version_ids
            ):
                raise ValidationError("fake direct structural payload drift")
            impact_ids = tuple(
                root.job_id
                for root in direct_roots
                if root.kind is JobKind.IMPACT_DISCOVERY
            )
            if tuple(scope.root_job_id for scope in direct_scopes) != impact_ids:
                raise ValidationError("fake direct scopes drift")
        elif (
            direct_payload is not None
            or direct_withdrawal is not None
            or direct_roots
            or direct_scopes
        ):
            raise ValidationError("fake non-document event declared direct work")
        existing = self.world.epochs_by_event.get(event.structural_event_id)
        if existing is not None:
            if existing.plan != event:
                raise EventConflictError("fake typed event replay conflicts")
            if existing.terminal_result is not None:
                terminal = existing.terminal_result
                if terminal.state is M5RunState.SEALED:
                    assert terminal.publication_receipt is not None
                    return OpenEventReceipt(
                        existing.epoch_id,
                        True,
                        True,
                        terminal.publication_receipt.publication_id,
                    )
                return OpenEventReceipt(
                    existing.epoch_id,
                    True,
                    False,
                    already_failed=True,
                    failure_reason=terminal.failure_reason.value
                    if terminal.failure_reason is not None
                    else None,
                )
            opened = OpenEventReceipt(existing.epoch_id, True, False)
            existing.current_open_receipt = opened
            return opened
        if any(
            epoch.terminal_result is None
            for epoch in self.world.epochs_by_event.values()
        ):
            raise InvalidEventError("fake runtime permits only one open typed epoch")
        with self.world.transaction("typed-open"):
            if (
                event.expected_previous_published_epoch_id
                != self.world.published.current_epoch
            ):
                raise InvalidEventError("fake typed predecessor is stale")
            root_ids = tuple(root.job.logical_job_id for root in requirement_roots)
            if root_ids != tuple(sorted(set(root_ids))):
                raise ValidationError("fake root declaration order drift")
            if requirement_root_set_hash != digests.requirement_root_set_digest(
                root_ids
            ):
                raise ValidationError("fake root-set digest drift")
            epoch_id = self.world.allocate_runtime_epoch_id()
            working = deepcopy(self.world.published)
            before = compute_reference_states(self.world.published)
            while working.current_epoch < epoch_id - 1:
                working.base.advance_epoch()
                _ = working.current_point
            _stage_event(working, event)
            if working.current_epoch != epoch_id:
                raise AssertionError("fake structural epoch allocation drift")
            if requirement_snapshot(working) != event.requirement_registry_snapshot:
                raise ValidationError("fake requirement snapshot is not post-event")
            if chunk_snapshot(working) != event.active_chunk_snapshot:
                raise ValidationError("fake chunk snapshot is not post-event")
            declarations = {
                declaration.job.logical_job_id: declaration
                for declaration in requirement_roots
            }
            jobs = {
                job_id: _FakeJob(declaration.job)
                for job_id, declaration in declarations.items()
            }
            scopes = {job_id: M5ScopeState.OPEN for job_id in declarations}
            epoch = _FakeEpoch(
                event,
                epoch_id,
                working,
                before,
                declarations,
                direct_roots,
                jobs,
                scopes,
                event_work=M5RuntimeWork(
                    deactivated_chunk_count=len(
                        event.direct_plan.deactivated_chunk_version_ids
                        if event.direct_plan is not None
                        else ()
                    ),
                    withdrawn_candidate_edge_count=(
                        len(requirement_withdrawal.withdrawn_candidate_pair_digests)
                        + (
                            len(direct_withdrawal.plan.candidate_edge_ids)
                            if direct_withdrawal is not None
                            else 0
                        )
                    ),
                    withdrawn_current_observation_count=(
                        len(requirement_withdrawal.withdrawn_observation_ids)
                        + (
                            len(direct_withdrawal.plan.observation_ids)
                            if direct_withdrawal is not None
                            else 0
                        )
                    ),
                ),
                direct_done=not direct_roots,
            )
            self.world.epochs_by_event[event.structural_event_id] = epoch
            opened = OpenEventReceipt(epoch_id, False, False)
            epoch.current_open_receipt = opened
            self.world.install_transition_anchor(
                epoch,
                M5TransitionTimingAnchor.build(
                    epoch_id=epoch_id,
                    contribution_kind=M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
                    source_id=event.structural_event_id,
                    anchor_revision=1,
                    terminal_transition=False,
                ),
            )
            self.world.operation_log.append("typed-open")
            return opened


@dataclass(slots=True)
class FakeDirect:
    world: FakeTypedWorld

    def plan_direct_open(self, event: M5TypedEventPlan) -> M5DirectOpenPlan:
        self.world.operation_log.append("plan:direct")
        if event.direct_plan is None:
            raise ValidationError("direct planning requires a direct event")
        observations = tuple(
            observation
            for observation in self.world.published.base.current_observations()
            if observation.subject_kind is SubjectKind.CLAIM
            and observation.chunk_version_id
            in event.direct_plan.deactivated_chunk_version_ids
        )
        dependencies = tuple(
            ObservationDependency(
                observation.observation_id,
                PairKey(observation.subject_id, observation.chunk_version_id),
            )
            for observation in observations
        )
        withdrawal_plan = plan_withdrawal(
            ReverseDependencyIndex.build(dependencies, ()),
            event.direct_plan.deactivated_chunk_version_ids,
        )
        withdrawal = StructuralWithdrawal(
            withdrawal_plan, withdrawal_plan.affected_claim_ids
        )
        roots: list[M4LogicalJobSpec] = []
        for chunk_id in event.direct_plan.inserted_chunk_version_ids:
            roots.append(self._root(event, JobKind.IMPACT_DISCOVERY, chunk_id))
        for claim_id in withdrawal.fallback_claim_ids:
            roots.append(self._root(event, JobKind.FRONTIER_RETRIEVE, claim_id))
        ordered_roots = tuple(sorted(roots, key=lambda root: root.job_id))
        scopes = tuple(
            M4DiscoveryScope(
                root_job_id=root.job_id,
                registry_snapshot_id=event.direct_plan.claim_registry_snapshot_id,
                registered_claim_ids=event.direct_plan.registered_claim_ids,
            )
            for root in ordered_roots
            if root.kind is JobKind.IMPACT_DISCOVERY
        )
        return M5DirectOpenPlan(
            self._structural_payload(event),
            withdrawal,
            ordered_roots,
            scopes,
        )

    @staticmethod
    def _structural_payload(event: M5TypedEventPlan) -> StructuralPayload:
        typed_event = event.event
        if isinstance(typed_event, InsertDocumentEvent):
            document_id = typed_event.document_id
            version_id = typed_event.document_version_id
            content_hash = typed_event.content_hash
            deactivated_id = None
            chunks = typed_event.chunks
        elif isinstance(typed_event, ReplaceDocumentVersionEvent):
            document_id = typed_event.document_id
            version_id = typed_event.new_document_version_id
            content_hash = typed_event.content_hash
            deactivated_id = typed_event.old_document_version_id
            chunks = typed_event.chunks
        elif isinstance(typed_event, DeleteDocumentVersionEvent):
            return StructuralPayload(
                deactivated_document_version_id=typed_event.document_version_id
            )
        else:
            raise ValidationError("direct payload requires a document event")
        inserted_chunks = tuple(
            sorted(
                (
                    ChunkVersion(
                        chunk_version_id=chunk.chunk_version_id,
                        document_version_id=version_id,
                        chunk_index=chunk.chunk_index,
                        text=chunk.text,
                    )
                    for chunk in chunks
                ),
                key=lambda chunk: chunk.chunk_version_id,
            )
        )
        return StructuralPayload(
            inserted=InsertedDocument(
                version=DocumentVersion(version_id, document_id, content_hash),
                chunks=inserted_chunks,
            ),
            deactivated_document_version_id=deactivated_id,
        )

    @staticmethod
    def _root(event: M5TypedEventPlan, kind: JobKind, target: str) -> M4LogicalJobSpec:
        execution_hash = sha(f"fake-direct:{kind.value}")
        claim_id = target if kind is JobKind.FRONTIER_RETRIEVE else ""
        chunk_id = target if kind is JobKind.IMPACT_DISCOVERY else ""
        return M4LogicalJobSpec(
            job_id=M4LogicalJobSpec.derive_job_id(
                event_id=event.structural_event_id,
                kind=kind,
                candidate_policy_id=event.candidate_policy_id,
                execution_spec_hash=execution_hash,
                claim_id=claim_id,
                chunk_version_id=chunk_id,
            ),
            event_id=event.structural_event_id,
            kind=kind,
            candidate_policy_id=event.candidate_policy_id,
            payload_hash=stable_m4_digest(
                "m4-application-job-payload-v1",
                event.payload_hash,
                kind.value,
                "",
                claim_id,
                chunk_id,
            ),
            execution_spec_hash=execution_hash,
            target_claim_id=claim_id or None,
            target_chunk_version_id=chunk_id or None,
            expandable=True,
        )

    def _selected_job_id(
        self,
        epoch: _FakeEpoch,
        event: M5TypedEventPlan,
        return_kind: M5TypedDirectReturnKind,
    ) -> str:
        if len(epoch.direct_roots) != 1:
            raise ValidationError(
                "fake selected direct outcome requires one unambiguous root"
            )
        root = epoch.direct_roots[0]
        if return_kind is M5TypedDirectReturnKind.DISCOVERY:
            return root.job_id
        if return_kind is not M5TypedDirectReturnKind.VERIFIER:
            raise ValidationError("fake selected direct outcome has another kind")
        claim_ids = tuple(sorted(self.world.published.base.all_claim_ids()))
        chunk_id = root.target_chunk_version_id
        if not claim_ids or chunk_id is None:
            raise ValidationError(
                "fake selected verifier outcome requires a discovered pair"
            )
        claim_id = claim_ids[0]
        execution_hash = sha("fake-direct:verify_pair")
        job_id = M4LogicalJobSpec.derive_job_id(
            event_id=event.structural_event_id,
            kind=JobKind.VERIFY_PAIR,
            candidate_policy_id=event.candidate_policy_id,
            execution_spec_hash=execution_hash,
            parent_job_id=root.job_id,
            claim_id=claim_id,
            chunk_version_id=chunk_id,
        )
        child = M4LogicalJobSpec(
            job_id=job_id,
            event_id=event.structural_event_id,
            kind=JobKind.VERIFY_PAIR,
            candidate_policy_id=event.candidate_policy_id,
            payload_hash=stable_m4_digest(
                "m4-application-job-payload-v1",
                event.payload_hash,
                JobKind.VERIFY_PAIR.value,
                root.job_id,
                claim_id,
                chunk_id,
            ),
            execution_spec_hash=execution_hash,
            parent_job_id=root.job_id,
            pair=PairKey(claim_id, chunk_id),
            expandable=False,
        )
        return child.job_id

    @staticmethod
    def _selected_outer_receipt(
        *,
        epoch_id: int,
        resulting_revision: int,
        job_id: str,
        return_kind: M5TypedDirectReturnKind,
        branch: str,
        terminal_logical_result_hash: str,
    ) -> M5DirectAttemptReturnReceipt:
        attempt_id = sha(f"fake-direct-attempt:{epoch_id}:{job_id}:{return_kind.value}")
        if branch == "normal":
            source_id = sha(
                f"fake-direct-transition:{epoch_id}:{job_id}:{return_kind.value}"
            )
            normal = M5DirectNormalReturnReceipt(
                epoch_id=epoch_id,
                job_id=job_id,
                attempt_id=attempt_id,
                resulting_revision=resulting_revision,
                exact_replay=True,
                execution_evidence_digest=sha(
                    f"fake-direct-execution:{epoch_id}:{job_id}"
                ),
                return_artifact_digest=sha(f"fake-direct-artifact:{epoch_id}:{job_id}"),
                direct_transition_source_id=source_id,
                direct_transition_source_identity_hash=sha(
                    f"fake-direct-transition-identity:{source_id}"
                ),
                direct_transition_contribution_key_digest=(
                    digests.runtime_work_contribution_key_digest(
                        epoch_id=epoch_id,
                        contribution_kind=(
                            M5RuntimeWorkContributionKind.DIRECT_TRANSITION
                        ),
                        source_id=source_id,
                    )
                ),
                observation_completion=(
                    ObservationCompletionReceipt(True, True)
                    if return_kind is M5TypedDirectReturnKind.VERIFIER
                    else None
                ),
                current_terminal_logical_result_hash=(terminal_logical_result_hash),
                transition_anchor=None,
            )
            return M5DirectAttemptReturnReceipt(return_kind, normal, None)
        late = M5DirectLateReturnReceipt(
            disposition=(M5DirectLateReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL),
            epoch_id=epoch_id,
            job_id=job_id,
            attempt_id=attempt_id,
            resulting_revision=resulting_revision,
            exact_replay=False,
            envelope_digest=sha(f"fake-direct-envelope:{epoch_id}:{job_id}"),
            execution_evidence_digest=sha(f"fake-direct-execution:{epoch_id}:{job_id}"),
            expired_return_digest=None,
            current_terminal_logical_result_hash=terminal_logical_result_hash,
            transition_anchor=None,
        )
        return M5DirectAttemptReturnReceipt(return_kind, None, late)

    def _run_selected_terminal_outcome(
        self,
        epoch: _FakeEpoch,
        event: M5TypedEventPlan,
        selected: FakeDirectSelectedOutcome,
    ) -> M5DirectExecutionReceipt:
        job_id = self._selected_job_id(epoch, event, selected.return_kind)
        if not selected.call_work.is_zero:
            self.world.assert_external_boundary(f"direct-{selected.return_kind.value}")
        if selected.branch == "normal":
            epoch.event_work = _add_work(epoch.event_work, selected.call_work)
            transaction_name = "direct-runtime"
            completion_marker = "direct-complete"
        else:
            transaction_name = "direct-winning-runtime"
            completion_marker = "race:direct-winning-complete"
        with self.world.transaction(transaction_name):
            epoch.direct_done = True
            epoch.revision += 1
            self.world.operation_log.append(completion_marker)
        runtime = FakeRuntime(self.world)
        self.world.operation_log.append(
            "race:direct-selected-terminal:"
            f"{selected.outcome.value}:{selected.return_kind.value}:{selected.branch}"
        )
        if selected.outcome is M5ReplayedOutcome.SEALED:
            cancelled = 0
            for runtime_job in epoch.jobs.values():
                if runtime_job.state.terminal:
                    continue
                runtime_job.state = M5JobState.CANCELLED
                runtime_job.completion = M5JobCompletion.build(
                    job=runtime_job.spec,
                    terminal_state=M5JobState.CANCELLED,
                    archive_reason=M5TerminalReason.SCOPE_RETIRED,
                )
                cancelled += 1
            for root_id, scope_state in tuple(epoch.scope_states.items()):
                if scope_state in {M5ScopeState.OPEN, M5ScopeState.RESULT_STAGED}:
                    epoch.scope_states[root_id] = M5ScopeState.CANCELLED
            epoch.event_work = _add_work(
                epoch.event_work,
                M5RuntimeWork(requirement_cancelled_job_count=cancelled),
            )
            self.world.operation_log.append("race:direct-selected-seal-ready")
            terminal = runtime.request_typed_seal_atomically(
                epoch.epoch_id,
                epoch.revision,
                event,
                M5RuntimeWork(),
            )
        else:
            terminal = runtime.fail_typed_epoch_atomically(
                epoch.epoch_id,
                epoch.revision,
                M5RunFailureReason.RETRIEVAL_ERROR,
                M5RuntimeWork(),
            )
        logical_result_hash = terminal.logical_result_hash
        assert logical_result_hash is not None
        receipt = self._selected_outer_receipt(
            epoch_id=epoch.epoch_id,
            resulting_revision=epoch.revision,
            job_id=job_id,
            return_kind=selected.return_kind,
            branch=selected.branch,
            terminal_logical_result_hash=logical_result_hash,
        )
        self.world.direct_selected_receipts_by_event[event.structural_event_id] = (
            receipt
        )
        self.world.operation_log.append(
            f"direct-selected-outer:{selected.return_kind.value}:{selected.branch}"
        )
        return M5DirectExecutionReceipt(
            resulting_revision=epoch.revision,
            call_work=selected.call_work,
            selected_successful_outer_receipt=receipt,
            selected_successful_outer_return_kind=selected.return_kind,
            selected_successful_outer_job_id=job_id,
        )

    def run_pending_direct(
        self, epoch_id: int, expected_revision: int, event: M5TypedEventPlan
    ) -> M5DirectExecutionReceipt:
        epoch = self.world.epoch(epoch_id)
        if expected_revision != epoch.revision:
            raise EventConflictError("stale fake direct revision")
        interruptions = self.world.direct_interruptions_by_event.get(
            event.structural_event_id, 0
        )
        if interruptions:
            self.world.direct_interruptions_by_event[event.structural_event_id] = (
                interruptions - 1
            )
            self.world.operation_log.append("interrupt:direct-before-execution")
            raise FakeDirectInterruption("fake direct execution interrupted")
        selected = self.world.direct_selected_outcomes_by_event.get(
            event.structural_event_id
        )
        if selected is not None:
            return self._run_selected_terminal_outcome(epoch, event, selected)
        call_work = M5RuntimeWork()
        if not epoch.direct_done:
            call_work = M5RuntimeWork(
                direct_discovery_call_count=len(epoch.direct_roots),
                embedding_model_call_count=len(epoch.direct_roots),
            )
            for _root in epoch.direct_roots:
                self.world.assert_external_boundary("direct-discovery")
            epoch.event_work = _add_work(epoch.event_work, call_work)
            blocked = self.world.direct_blocked_by_event.get(event.structural_event_id)
            terminal = self.world.direct_failed_by_event.get(event.structural_event_id)
            if blocked is not None or terminal is not None:
                return M5DirectExecutionReceipt(
                    resulting_revision=epoch.revision,
                    call_work=call_work,
                    blocked_reason=blocked,
                    terminal_failure_reason=terminal,
                )
            with self.world.transaction("direct-runtime"):
                epoch.direct_done = True
                epoch.revision += 1
                self.world.operation_log.append("direct-complete")
        return M5DirectExecutionReceipt(
            resulting_revision=epoch.revision,
            call_work=call_work,
        )


@dataclass(slots=True)
class FakeDiscovery:
    world: FakeTypedWorld

    def discover_requirement_scope(
        self,
        epoch_id: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        manifest: M5CandidatePolicyManifest,
        event: M5TypedEventPlan,
    ) -> M5DiscoveryExecution:
        epoch = self.world.epoch(epoch_id)
        direction, target = self.world.root_target(epoch, job)
        key = (event.structural_event_id, direction, target)
        outcomes = self.world.discovery_outcomes.get(key, [])
        outcome = outcomes.pop(0) if outcomes else FakeDiscoveryOutcome()
        reused = (
            outcome.execution_disposition
            is M5ExecutionEvidenceDisposition.REUSED_ARTIFACT
        )
        if not reused:
            self.world.assert_external_boundary("requirement-discovery")
        call_work = (
            M5RuntimeWork()
            if reused
            else M5RuntimeWork(
                requirement_forward_retrieval_call_count=int(
                    direction is M5DiscoveryDirection.FORWARD_REQUIREMENT
                ),
                requirement_reverse_retrieval_call_count=int(
                    direction is M5DiscoveryDirection.REVERSE_CHUNK
                ),
                requirement_fallback_forward_call_count=int(
                    direction is M5DiscoveryDirection.FORWARD_REQUIREMENT
                    and not isinstance(
                        event.event, (RegisterGroupEvent, ReplaceGroupEvent)
                    )
                ),
                embedding_model_call_count=1,
            )
        )
        if outcome.failure_reason is not None:
            raise M5ExternalWorkFailure(
                outcome.failure_reason,
                retryable=outcome.retryable,
                call_work=call_work,
                attempt_timing=outcome.attempt_timing,
                error_hash=sha(
                    f"discovery-error:{event.structural_event_id}:"
                    f"{job.logical_job_id}:{outcome.failure_reason.value}"
                ),
            )
        hits = tuple(
            M5RequirementChannelHit.build(
                epoch_id=epoch_id,
                root_job_id=job.logical_job_id,
                scope_contract_digest=job.scope_contract_digest or "",
                pair=pair,
                candidate_policy_id=manifest.candidate_policy_id,
                channel=M5RequirementAdmissionChannel.VECTOR,
                rank=rank,
                score=1.0 - rank / 100.0,
                channel_artifact_hash=sha(
                    f"hit:{event.structural_event_id}:{job.logical_job_id}:{rank}"
                ),
            )
            for rank, pair in enumerate(outcome.pairs, start=1)
        )
        result = build_rank_interleaved_discovery_result(
            hits=hits,
            direction=direction,
            manifest=manifest,
            root_job_id=job.logical_job_id,
            scope_contract_digest=job.scope_contract_digest or "",
            eligible_snapshot_exhausted=True,
        )
        assert lease.attempt is not None
        attempt_output = M5AttemptOutput.build(
            attempt=lease.attempt,
            job_epoch_id=epoch_id,
            payload_hash=job.payload_hash,
            result_artifact_id=result.result_artifact_id,
            result_artifact_hash=result.result_artifact_hash,
        )
        return M5DiscoveryExecution(
            result,
            attempt_output,
            True,
            outcome.execution_disposition,
            call_work,
            outcome.attempt_timing,
        )


@dataclass(slots=True)
class FakeVerifier:
    world: FakeTypedWorld

    def verify_requirement_pair(
        self,
        epoch_id: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        manifest: M5CandidatePolicyManifest,
        event: M5TypedEventPlan,
    ) -> M5VerifierExecution:
        assert job.pair is not None
        epoch = self.world.epoch(epoch_id)
        key = (job.pair.subject_id, job.pair.chunk_version_id)
        outcomes = self.world.verifier_outcomes.get(key, [])
        outcome = outcomes.pop(0) if outcomes else FakeVerifierOutcome()
        reused = (
            outcome.execution_disposition
            is M5ExecutionEvidenceDisposition.REUSED_ARTIFACT
        )
        if not reused:
            self.world.assert_external_boundary("requirement-verifier")
        call_work = (
            M5RuntimeWork()
            if reused
            else M5RuntimeWork(
                requirement_verifier_call_count=1,
                verifier_model_call_count=1,
                verifier_input_token_count=8,
                verifier_output_token_count=3,
            )
        )
        if outcome.failure_reason is not None:
            raise M5ExternalWorkFailure(
                outcome.failure_reason,
                retryable=outcome.retryable,
                call_work=call_work,
                attempt_timing=outcome.attempt_timing,
                error_hash=sha(
                    f"verifier-error:{event.structural_event_id}:"
                    f"{job.logical_job_id}:{outcome.failure_reason.value}"
                ),
            )
        requirement = epoch.working.requirement(job.pair.subject_id)
        group = epoch.working.group(requirement.group_version_id)
        chunk = epoch.working.base.chunk_version(job.pair.chunk_version_id)
        pair_input = M5RequirementPairInput.build(
            pair=job.pair,
            scope_contract_digest=job.scope_contract_digest or "",
            candidate_policy_id=manifest.candidate_policy_id,
            owner_claim_id=group.owner_claim_id,
            group_version_id=group.group_version_id,
            group_family_id=group.group_family_id,
            requirement_ordinal=requirement.ordinal,
            requirement_text=requirement.requirement_text,
            document_version_id=chunk.document_version_id,
            chunk_index=chunk.chunk_index,
            chunk_text=chunk.text,
            stored_chunk_text_hash=chunk.text_hash,
            chunker_artifact_id="fake-chunker-v1",
        )
        support, refute, neutral = outcome.scores
        artifact = M5RequirementVerifierArtifact.build_checked(
            pair=job.pair,
            pair_input_hash=pair_input.pair_input_hash,
            execution_spec_hash=job.execution_spec_hash,
            model_artifact_id="fake-verifier-artifact-tree-v1",
            model_id="fake-verifier",
            model_revision="revision-v1",
            prompt_artifact_id="fake-prompt-artifact-v1",
            prompt_version="prompt-v1",
            calibration_version="calibration-v1",
            calibration_artifact_hash=None,
            temperature=1.0,
            decision_policy=epoch.working.base.current_policy(),
            support_score=support,
            refute_score=refute,
            neutral_score=neutral,
            raw_logits=(refute, support, neutral),
            raw_output_hash=sha(f"raw:{job.logical_job_id}:{outcome.scores}"),
        )
        assert lease.attempt is not None
        output = M5AttemptOutput.build(
            attempt=lease.attempt,
            job_epoch_id=epoch_id,
            payload_hash=job.payload_hash,
            result_artifact_id=artifact.artifact_id,
            result_artifact_hash=artifact.artifact_hash,
        )
        return M5VerifierExecution(
            pair_input,
            artifact,
            output,
            outcome.execution_disposition,
            call_work,
            outcome.attempt_timing,
        )


@dataclass(slots=True)
class FakeRuntime:
    world: FakeTypedWorld

    def read_typed_event_result(
        self, event_id: str, payload_hash: str
    ) -> M5EventRunResult | None:
        self.world.operation_log.append("read:terminal-result")
        epoch = self.world.epochs_by_event.get(event_id)
        if epoch is None:
            return None
        if epoch.plan.payload_hash != payload_hash:
            raise EventConflictError("fake typed replay payload conflict")
        stored = epoch.terminal_result
        if stored is None:
            return None
        if stored.state is M5RunState.SEALED:
            assert stored.publication_receipt is not None
            publication = PublicationReceipt(
                stored.epoch_id,
                stored.publication_receipt.publication_id,
                replayed=True,
            )
            opened = OpenEventReceipt(
                stored.epoch_id,
                True,
                True,
                publication.publication_id,
            )
            replayed_outcome = M5ReplayedOutcome.SEALED
        else:
            assert stored.failure_reason is not None
            publication = None
            opened = OpenEventReceipt(
                stored.epoch_id,
                True,
                False,
                already_failed=True,
                failure_reason=stored.failure_reason.value,
            )
            replayed_outcome = M5ReplayedOutcome.FAILED
        event_timing, event_coverage = self.world.timing_image(
            epoch, project_pending=False
        )
        call_coverage = M5RuntimeTimingCoverage.single_point(
            None, terminal_client_roundtrip_included=False
        )
        return M5EventRunResult.build(
            event_id=stored.event_id,
            payload_hash=stored.payload_hash,
            epoch_id=stored.epoch_id,
            state=M5RunState.REPLAYED,
            replayed_outcome=replayed_outcome,
            open_receipt=opened,
            publication_receipt=publication,
            event_work=stored.event_work,
            call_work=M5RuntimeWork(),
            event_timing=event_timing,
            call_timing=M5RuntimeTiming(),
            combined_deltas=stored.combined_deltas,
            changed_state_references=stored.changed_state_references,
            failure_reason=stored.failure_reason,
            event_timing_coverage=event_coverage,
            call_timing_coverage=call_coverage,
        )

    def acquire_m5_job(
        self, epoch_id: int, expected_revision: int, job: M5LogicalJobSpec
    ) -> M5JobLease:
        epoch = self.world.epoch(epoch_id)
        runtime_job = epoch.jobs.get(job.logical_job_id)
        if runtime_job is None or runtime_job.spec != job:
            raise EventConflictError("fake acquire names another job")
        remaining = self.world.terminal_acquisition_after_by_kind.get(job.job_kind)
        if remaining is not None and epoch.terminal_result is None:
            if remaining:
                self.world.terminal_acquisition_after_by_kind[job.job_kind] = (
                    remaining - 1
                )
            else:
                del self.world.terminal_acquisition_after_by_kind[job.job_kind]
                self.world.operation_log.append(
                    f"race:acquisition-epoch-failed:{job.job_kind.value}"
                )
                self.fail_typed_epoch_atomically(
                    epoch_id,
                    epoch.revision,
                    self.world.terminal_acquisition_failure_reason,
                    M5RuntimeWork(),
                )
        failure_remaining = self.world.terminal_job_failure_after_by_kind.get(
            job.job_kind
        )
        if failure_remaining is not None and not runtime_job.state.terminal:
            if failure_remaining:
                self.world.terminal_job_failure_after_by_kind[job.job_kind] = (
                    failure_remaining - 1
                )
            else:
                del self.world.terminal_job_failure_after_by_kind[job.job_kind]
                with self.world.transaction("terminal-job-race"):
                    runtime_job.state = M5JobState.TERMINAL_FAILED
                    runtime_job.completion = M5JobCompletion.build(
                        job=job,
                        terminal_state=M5JobState.TERMINAL_FAILED,
                        result_artifact_id=None,
                        result_artifact_hash=None,
                        archive_reason=self.world.terminal_job_failure_reason,
                    )
                    epoch.revision += 1
                    self.world.operation_log.append(
                        f"race:terminal-job-failed:{job.job_kind.value}"
                    )
        if job.logical_job_id in epoch.root_results:
            attempt = runtime_job.attempts[-1]
            return M5JobLease(
                job.logical_job_id,
                attempt,
                epoch.revision,
                False,
                True,
                lease_expires_at=attempt.lease_expires_at,
                dispatch_record_digest=runtime_job.dispatch_digests[attempt.attempt_id],
                disposition=M5AcquisitionDisposition.RESULT_RESERVED,
            )
        if runtime_job.state.terminal:
            if runtime_job.completion is None:
                raise AssertionError("fake terminal job lacks completion")
            terminal_attempt = (
                runtime_job.attempts[-1] if runtime_job.attempts else None
            )
            return M5JobLease(
                job.logical_job_id,
                terminal_attempt,
                epoch.revision,
                False,
                True,
                lease_expires_at=(
                    None
                    if terminal_attempt is None
                    else terminal_attempt.lease_expires_at
                ),
                dispatch_record_digest=(
                    None
                    if terminal_attempt is None
                    else runtime_job.dispatch_digests[terminal_attempt.attempt_id]
                ),
                disposition=M5AcquisitionDisposition.TERMINAL,
                terminal_projection=M5LeaseTerminalProjection.build(
                    logical_job_id=job.logical_job_id,
                    terminal_state=runtime_job.completion.terminal_state,
                    terminal_reason=runtime_job.completion.archive_reason,
                    completion_digest=runtime_job.completion.completion_digest,
                ),
            )
        if runtime_job.state is M5JobState.RUNNING and runtime_job.attempts:
            attempt = runtime_job.attempts[-1]
            if job.logical_job_id not in self.world.takeover_jobs_once:
                if expected_revision > epoch.revision:
                    raise EventConflictError("future fake live-lease revision")
                return M5JobLease(
                    job.logical_job_id,
                    attempt,
                    epoch.revision,
                    False,
                    True,
                    lease_expires_at=attempt.lease_expires_at,
                    dispatch_record_digest=runtime_job.dispatch_digests[
                        attempt.attempt_id
                    ],
                    disposition=M5AcquisitionDisposition.LIVE_LEASE,
                )
            self.world.takeover_jobs_once.remove(job.logical_job_id)
            disposition = M5AcquisitionDisposition.DISPATCH_TAKEOVER
        else:
            disposition = M5AcquisitionDisposition.DISPATCH_NEW
        if expected_revision != epoch.revision:
            raise EventConflictError("stale fake acquisition revision")
        with self.world.transaction("job-acquire"):
            ordinal = len(runtime_job.attempts) + 1
            attempt = M5JobAttempt.build(
                logical_job_id=job.logical_job_id,
                attempt_ordinal=ordinal,
                execution_spec_hash=job.execution_spec_hash,
                lease_token_hash=sha(f"lease:{job.logical_job_id}:{ordinal}"),
                lease_expires_at=FAKE_LEASE_BASE + timedelta(seconds=ordinal),
                attempt_work_digest=M5RuntimeWork().work_digest,
            )
            dispatch_digest = sha(f"dispatch:{epoch_id}:{job.logical_job_id}:{ordinal}")
            runtime_job.attempts.append(attempt)
            runtime_job.dispatch_digests[attempt.attempt_id] = dispatch_digest
            runtime_job.state = M5JobState.RUNNING
            epoch.revision += 1
            self.world.install_transition_anchor(
                epoch,
                M5TransitionTimingAnchor.build(
                    epoch_id=epoch_id,
                    contribution_kind=M5RuntimeWorkContributionKind.M5_ACQUISITION,
                    source_id=dispatch_digest,
                    anchor_revision=epoch.revision,
                    terminal_transition=False,
                ),
            )
            self.world.operation_log.append(f"acquire:{job.job_kind.value}")
            return M5JobLease(
                job.logical_job_id,
                attempt,
                epoch.revision,
                True,
                False,
                lease_expires_at=attempt.lease_expires_at,
                dispatch_record_digest=dispatch_digest,
                disposition=disposition,
            )

    def mark_m5_retryable_failure(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5JobLease,
        error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> M5AttemptCompletionReceipt:
        return self._mark_m5_failure(
            epoch_id,
            expected_revision,
            lease,
            error_hash,
            attempt_work,
            attempt_timing,
            None,
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
        return self._mark_m5_failure(
            epoch_id,
            expected_revision,
            lease,
            error_hash,
            attempt_work,
            attempt_timing,
            terminal_reason,
        )

    def _mark_m5_failure(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5JobLease,
        error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
        terminal_reason: M5TerminalReason | None,
    ) -> M5AttemptCompletionReceipt:
        epoch = self.world.epoch(epoch_id)
        job = epoch.jobs[lease.logical_job_id]
        if expected_revision != epoch.revision or lease.attempt is None:
            raise EventConflictError("stale fake attempt failure")
        existing = epoch.attempt_failures.get(lease.attempt.attempt_id)
        if existing is not None:
            receipt, stored_error, stored_work, stored_timing, stored_reason = existing
            if (
                stored_error,
                stored_work,
                stored_timing,
                stored_reason,
            ) != (error_hash, attempt_work, attempt_timing, terminal_reason):
                raise EventConflictError("fake attempt failure replay conflicts")
            return replace(
                receipt, resulting_revision=epoch.revision, exact_replay=True
            )
        name = "retryable-failure" if terminal_reason is None else "terminal-failure"
        with self.world.transaction(name):
            settled_attempt = replace(
                lease.attempt, attempt_work_digest=attempt_work.work_digest
            )
            job.attempts[-1] = settled_attempt
            epoch.event_work = _add_work(epoch.event_work, attempt_work)
            epoch.timing_points.append(attempt_timing)
            if terminal_reason is None:
                job.state = M5JobState.RETRYABLE_FAILED
            else:
                job.state = M5JobState.TERMINAL_FAILED
                job.completion = M5JobCompletion.build(
                    job=job.spec,
                    terminal_state=M5JobState.TERMINAL_FAILED,
                    result_artifact_id=None,
                    result_artifact_hash=None,
                    archive_reason=terminal_reason,
                )
            epoch.revision += 1
            receipt = M5AttemptCompletionReceipt(
                job.spec.logical_job_id,
                lease.attempt.attempt_id,
                epoch.revision,
                False,
            )
            epoch.attempt_failures[lease.attempt.attempt_id] = (
                receipt,
                error_hash,
                attempt_work,
                attempt_timing,
                terminal_reason,
            )
            self.world.install_transition_anchor(
                epoch,
                M5TransitionTimingAnchor.build(
                    epoch_id=epoch_id,
                    contribution_kind=(
                        M5RuntimeWorkContributionKind.M5_ATTEMPT_EXECUTION
                    ),
                    source_id=lease.attempt.attempt_id,
                    anchor_revision=epoch.revision,
                    terminal_transition=False,
                ),
            )
            self.world.operation_log.append(name)
            return receipt

    def _next_return_disposition(
        self, logical_job_id: str
    ) -> M5RequirementReturnDisposition:
        queued = self.world.return_dispositions_by_job.get(logical_job_id)
        if queued:
            return queued.pop(0)
        return M5RequirementReturnDisposition.APPLIED

    def _settle_requirement_return(
        self,
        *,
        epoch: _FakeEpoch,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        attempt_output: M5AttemptOutput,
        execution_disposition: M5ExecutionEvidenceDisposition,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
        disposition: M5RequirementReturnDisposition,
        applied_kind: M5RuntimeWorkContributionKind,
        persistence_work: M5RuntimeWork,
    ) -> M5RequirementAttemptReturnReceipt:
        if lease.attempt is None:
            raise ValidationError("fake successful return lacks an attempt")
        postterminal = disposition in {
            M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL,
            M5RequirementReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL,
        }
        if postterminal and epoch.terminal_result is None:
            raise EventConflictError("fake postterminal return lacks terminal result")
        anchor: M5TransitionTimingAnchor | None = None
        if not postterminal:
            settled_attempt = replace(
                lease.attempt, attempt_work_digest=attempt_work.work_digest
            )
            runtime_job = epoch.jobs[job.logical_job_id]
            runtime_job.attempts[-1] = settled_attempt
            epoch.event_work = _add_work(
                epoch.event_work,
                attempt_work,
                persistence_work
                if disposition is M5RequirementReturnDisposition.APPLIED
                else M5RuntimeWork(requirement_late_attempt_artifact_count=1),
            )
            epoch.timing_points.append(attempt_timing)
            if disposition is M5RequirementReturnDisposition.APPLIED:
                epoch.revision += 1
                anchor_kind = applied_kind
            else:
                anchor_kind = M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN
                if (
                    disposition
                    is M5RequirementReturnDisposition.TERMINAL_AUDIT_PRETERMINAL
                ):
                    runtime_job.state = M5JobState.CANCELLED
                    runtime_job.completion = M5JobCompletion.build(
                        job=runtime_job.spec,
                        terminal_state=M5JobState.CANCELLED,
                        result_artifact_id=None,
                        result_artifact_hash=None,
                        archive_reason=M5TerminalReason.SCOPE_RETIRED,
                    )
                    if job.logical_job_id in epoch.scope_states:
                        epoch.scope_states[job.logical_job_id] = M5ScopeState.CANCELLED
            anchor = M5TransitionTimingAnchor.build(
                epoch_id=epoch.epoch_id,
                contribution_kind=anchor_kind,
                source_id=lease.attempt.attempt_id,
                anchor_revision=epoch.revision,
                terminal_transition=False,
            )
            self.world.install_transition_anchor(epoch, anchor)
        evidence_digest = sha(
            f"evidence:{epoch.epoch_id}:{lease.attempt.attempt_id}:"
            f"{execution_disposition.value}:{attempt_work.work_digest}"
        )
        receipt = M5RequirementAttemptReturnReceipt(
            disposition,
            job.logical_job_id,
            lease.attempt.attempt_id,
            epoch.revision,
            False,
            evidence_digest,
            attempt_output.attempt_output_digest,
            (
                None
                if epoch.terminal_result is None
                else epoch.terminal_result.logical_result_hash
            ),
            anchor,
        )
        epoch.requirement_returns[lease.attempt.attempt_id] = (
            receipt,
            attempt_output,
            execution_disposition,
            attempt_work,
            attempt_timing,
        )
        return receipt

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
        epoch = self.world.epoch(epoch_id)
        if expected_revision != epoch.revision or lease.attempt is None:
            raise EventConflictError("stale fake discovery staging")
        if not isinstance(eligible_snapshot_exhausted, bool):
            raise ValidationError("fake discovery exhaustion evidence must be boolean")
        existing = epoch.requirement_returns.get(attempt_output.attempt_id)
        if existing is not None:
            receipt, stored_output, stored_disposition, stored_work, stored_timing = (
                existing
            )
            if (
                stored_output,
                stored_disposition,
                stored_work,
                stored_timing,
            ) != (
                attempt_output,
                execution_disposition,
                attempt_work,
                attempt_timing,
            ):
                raise EventConflictError("fake attempt output conflict")
            return replace(
                receipt,
                resulting_revision=epoch.revision,
                exact_replay=True,
                current_terminal_logical_result_hash=(
                    None
                    if epoch.terminal_result is None
                    else epoch.terminal_result.logical_result_hash
                ),
                transition_anchor=None,
            )
        with self.world.transaction("discovery-stage"):
            epoch.attempt_outputs[attempt_output.attempt_id] = attempt_output
            disposition = self._next_return_disposition(job.logical_job_id)
            receipt = self._settle_requirement_return(
                epoch=epoch,
                lease=lease,
                job=job,
                attempt_output=attempt_output,
                execution_disposition=execution_disposition,
                attempt_work=attempt_work,
                attempt_timing=attempt_timing,
                disposition=disposition,
                applied_kind=M5RuntimeWorkContributionKind.ROOT_RESULT_STAGE,
                persistence_work=M5RuntimeWork(
                    requirement_channel_hit_count=len(result.channel_hits),
                    requirement_pre_dedup_selection_count=len(result.selections),
                ),
            )
            if disposition is M5RequirementReturnDisposition.APPLIED:
                epoch.root_results[job.logical_job_id] = result
                epoch.scope_states[job.logical_job_id] = M5ScopeState.RESULT_STAGED
            self.world.operation_log.append("discovery-staged")
            return receipt

    def close_m5_requirement_roots_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        requirement_root_set_hash: str,
    ) -> M5RootBarrierReceipt:
        epoch = self.world.epoch(epoch_id)
        if epoch.root_barrier is not None:
            return M5RootBarrierReceipt(
                epoch.root_barrier.requirement_root_set_hash,
                epoch.root_barrier.barrier_completion_hash,
                epoch.revision,
                True,
            )
        if expected_revision != epoch.revision:
            raise EventConflictError("stale fake root barrier revision")
        if set(epoch.root_results) != set(epoch.declarations):
            raise InvalidEventError("fake root barrier is not complete")
        with self.world.transaction("root-barrier"):
            results = tuple(
                epoch.root_results[root_id] for root_id in sorted(epoch.root_results)
            )
            dedup = deduplicate_discovery_results(
                epoch_id=epoch_id,
                candidate_policy_id=epoch.plan.candidate_policy_id,
                results=results,
            )
            children: list[M5LogicalJobSpec] = []
            for admitted in dedup.admitted_pairs:
                declaration = epoch.declarations[admitted.owner_root_job_id]
                child = M5LogicalJobSpec.build(
                    structural_event_id=epoch.plan.structural_event_id,
                    job_kind=M5JobKind.VERIFY_REQUIREMENT_PAIR,
                    manifest=self.world.manifest,
                    scope=declaration.scope,
                    parent_job_id=admitted.owner_root_job_id,
                    pair=admitted.pair,
                )
                children.append(child)
            ordered_children = tuple(
                sorted(children, key=lambda child: child.logical_job_id)
            )
            barrier_plan = build_root_barrier_plan(
                structural_event_id=epoch.plan.structural_event_id,
                results=results,
                deduplication=dedup,
                child_jobs=ordered_children,
            )
            if barrier_plan.requirement_root_set_hash != requirement_root_set_hash:
                raise EventConflictError("fake barrier root-set mismatch")
            closure_by_root = {
                closure.root_job_id: closure for closure in barrier_plan.root_closures
            }
            for root_id, result in epoch.root_results.items():
                root = epoch.jobs[root_id]
                closure = closure_by_root[root_id]
                root.completion = M5JobCompletion.build(
                    job=root.spec,
                    terminal_state=M5JobState.COMPLETED_ACTIVE,
                    result_artifact_id=result.result_artifact_id,
                    result_artifact_hash=result.result_artifact_hash,
                    scope_closure_digest=closure.scope_closure_digest,
                    child_set_hash=closure.child_set_hash,
                )
                root.state = M5JobState.COMPLETED_ACTIVE
                epoch.scope_states[root_id] = M5ScopeState.CLOSED_ACTIVE
            for child in ordered_children:
                epoch.jobs[child.logical_job_id] = _FakeJob(child)
            epoch.event_work = _add_work(
                epoch.event_work,
                M5RuntimeWork(
                    requirement_admitted_pair_count=len(dedup.admitted_pairs)
                ),
            )
            epoch.revision += 1
            self.world.install_transition_anchor(
                epoch,
                M5TransitionTimingAnchor.build(
                    epoch_id=epoch_id,
                    contribution_kind=M5RuntimeWorkContributionKind.ROOT_BARRIER,
                    source_id=epoch.plan.structural_event_id,
                    anchor_revision=epoch.revision,
                    terminal_transition=False,
                ),
            )
            receipt = M5RootBarrierReceipt(
                requirement_root_set_hash,
                barrier_plan.barrier_completion_hash,
                epoch.revision,
                False,
            )
            epoch.root_barrier = receipt
            self.world.operation_log.append("root-barrier")
            return receipt

    def verifier_jobs(self, epoch_id: int) -> tuple[M5LogicalJobSpec, ...]:
        epoch = self.world.epoch(epoch_id)
        return tuple(
            sorted(
                (
                    job.spec
                    for job in epoch.jobs.values()
                    if job.spec.job_kind is M5JobKind.VERIFY_REQUIREMENT_PAIR
                ),
                key=lambda spec: spec.logical_job_id,
            )
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
        epoch = self.world.epoch(epoch_id)
        runtime_job = epoch.jobs[job.logical_job_id]
        if expected_revision != epoch.revision or lease.attempt is None:
            raise EventConflictError("stale fake verifier completion")
        existing = epoch.requirement_returns.get(attempt_output.attempt_id)
        if existing is not None:
            receipt, stored_output, stored_disposition, stored_work, stored_timing = (
                existing
            )
            if (
                stored_output,
                stored_disposition,
                stored_work,
                stored_timing,
            ) != (
                attempt_output,
                execution_disposition,
                attempt_work,
                attempt_timing,
            ):
                raise EventConflictError("fake verifier return replay conflicts")
            return replace(
                receipt,
                resulting_revision=epoch.revision,
                exact_replay=True,
                current_terminal_logical_result_hash=(
                    None
                    if epoch.terminal_result is None
                    else epoch.terminal_result.logical_result_hash
                ),
                transition_anchor=None,
            )
        with self.world.transaction("verifier-completion"):
            epoch.attempt_outputs[attempt_output.attempt_id] = attempt_output
            assert job.pair is not None
            active = epoch.working.is_requirement_active(
                job.pair.subject_id
            ) and epoch.working.base.is_chunk_active(job.pair.chunk_version_id)
            disposition = self._next_return_disposition(job.logical_job_id)
            receipt = self._settle_requirement_return(
                epoch=epoch,
                lease=lease,
                job=job,
                attempt_output=attempt_output,
                execution_disposition=execution_disposition,
                attempt_work=attempt_work,
                attempt_timing=attempt_timing,
                disposition=disposition,
                applied_kind=M5RuntimeWorkContributionKind.VERIFIER_COMPLETION,
                persistence_work=M5RuntimeWork(
                    requirement_observation_artifact_count=1,
                    requirement_effective_observation_count=int(active),
                    requirement_inactive_completion_count=int(not active),
                ),
            )
            if disposition is M5RequirementReturnDisposition.APPLIED:
                point = epoch.working.advance_semantic_revision()
                observation = verifier_artifact.to_semantic_observation()
                record = epoch.working.register_requirement_observation(
                    observation, point
                )
                if record.eligible_for_currency != active:
                    raise AssertionError("fake activity classification drift")
                terminal_state = (
                    M5JobState.COMPLETED_ACTIVE
                    if active
                    else M5JobState.COMPLETED_INACTIVE
                )
                runtime_job.state = terminal_state
                runtime_job.completion = M5JobCompletion.build(
                    job=job,
                    terminal_state=terminal_state,
                    result_artifact_id=verifier_artifact.artifact_id,
                    result_artifact_hash=verifier_artifact.artifact_hash,
                    archive_reason=(
                        None if active else M5TerminalReason.SUBJECT_INACTIVE
                    ),
                )
            self.world.operation_log.append("verifier-complete")
            return receipt

    def fail_typed_epoch_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        failure_reason: M5RunFailureReason,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        epoch = self.world.epoch(epoch_id)
        competing_reason = self.world.failure_mutator_race_reason
        if competing_reason is not None and epoch.terminal_result is None:
            self.world.failure_mutator_race_reason = None
            self.world.operation_log.append(
                f"race:failure-mutator:{competing_reason.value}"
            )
            self.fail_typed_epoch_atomically(
                epoch_id,
                expected_revision,
                competing_reason,
                M5RuntimeWork(),
            )
        if epoch.terminal_result is not None:
            if (
                epoch.terminal_result.state is not M5RunState.FAILED
                or epoch.terminal_result.failure_reason is not failure_reason
            ):
                raise EventConflictError(
                    "fake failure replay requires the same failed outcome and reason"
                )
            replay = self.read_typed_event_result(
                epoch.plan.structural_event_id, epoch.plan.payload_hash
            )
            if (
                replay is None
                or replay.state is not M5RunState.REPLAYED
                or replay.replayed_outcome is not M5ReplayedOutcome.FAILED
                or replay.failure_reason is not failure_reason
            ):
                raise EventConflictError("fake failure replay is not canonical")
            return replay
        if expected_revision != epoch.revision:
            raise EventConflictError("stale fake failure revision")
        if epoch.current_open_receipt is None:
            raise AssertionError("fake failure lacks the current invocation receipt")
        with self.world.transaction("typed-failure"):
            cancelled = 0
            for runtime_job in epoch.jobs.values():
                if not runtime_job.state.terminal:
                    runtime_job.state = M5JobState.CANCELLED
                    runtime_job.completion = M5JobCompletion.build(
                        job=runtime_job.spec,
                        terminal_state=M5JobState.CANCELLED,
                        result_artifact_id=None,
                        result_artifact_hash=None,
                        archive_reason=M5TerminalReason.EPOCH_FAILED,
                    )
                    cancelled += 1
            for root_id in epoch.scope_states:
                if epoch.scope_states[root_id] in {
                    M5ScopeState.OPEN,
                    M5ScopeState.RESULT_STAGED,
                }:
                    epoch.scope_states[root_id] = M5ScopeState.CANCELLED
            epoch.event_work = _add_work(
                epoch.event_work,
                M5RuntimeWork(requirement_cancelled_job_count=cancelled),
            )
            epoch.revision += 1
            epoch.failed = True
            self.world.terminalize_timing(epoch)
            event_timing, event_coverage = self.world.timing_image(
                epoch, project_pending=False
            )
            call_coverage = M5RuntimeTimingCoverage.single_point(
                None, terminal_client_roundtrip_included=False
            )
            result = M5EventRunResult.build(
                event_id=epoch.plan.structural_event_id,
                payload_hash=epoch.plan.payload_hash,
                epoch_id=epoch_id,
                state=M5RunState.FAILED,
                replayed_outcome=None,
                open_receipt=epoch.current_open_receipt,
                publication_receipt=None,
                event_work=epoch.event_work,
                call_work=call_work,
                event_timing=event_timing,
                call_timing=M5RuntimeTiming(),
                combined_deltas=(),
                changed_state_references=(),
                failure_reason=failure_reason,
                event_timing_coverage=event_coverage,
                call_timing_coverage=call_coverage,
            )
            epoch.terminal_result = result
            self.world.operation_log.append("typed-failed")
            return result

    def request_typed_seal_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        event: M5TypedEventPlan,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        epoch = self.world.epoch(epoch_id)
        competing_outcome = self.world.seal_mutator_race_outcome
        if competing_outcome is not None and epoch.terminal_result is None:
            self.world.seal_mutator_race_outcome = None
            self.world.operation_log.append(
                f"race:seal-mutator:{competing_outcome.value}"
            )
            if competing_outcome is M5ReplayedOutcome.FAILED:
                self.fail_typed_epoch_atomically(
                    epoch_id,
                    expected_revision,
                    self.world.seal_mutator_race_failure_reason,
                    M5RuntimeWork(),
                )
            else:
                self.request_typed_seal_atomically(
                    epoch_id,
                    expected_revision,
                    event,
                    M5RuntimeWork(),
                )
        if epoch.terminal_result is not None:
            replay = self.read_typed_event_result(
                event.structural_event_id, event.payload_hash
            )
            if replay is None:
                raise AssertionError("fake terminal event lost its replay")
            return replay
        if expected_revision != epoch.revision:
            raise EventConflictError("stale fake seal revision")
        if epoch.current_open_receipt is None:
            raise AssertionError("fake seal lacks the current invocation receipt")
        if (
            not epoch.direct_done
            or any(not job.state.terminal for job in epoch.jobs.values())
            or any(
                state in {M5ScopeState.OPEN, M5ScopeState.RESULT_STAGED}
                for state in epoch.scope_states.values()
            )
        ):
            raise InvalidEventError("fake typed epoch is not seal-ready")
        with self.world.transaction("typed-seal"):
            after = compute_reference_states(epoch.working)
            deltas = _status_deltas(
                event.structural_event_id, epoch.before_states, after
            )
            references = _changed_references(
                epoch_id,
                epoch.revision + 1,
                self.world.published,
                epoch.working,
                epoch.before_states,
                after,
            )
            certificate_write_count = sum(
                reference.kind
                in {
                    M5StateReferenceKind.GROUP_CERTIFICATE,
                    M5StateReferenceKind.CLAIM_CERTIFICATE,
                }
                for reference in references
            )
            epoch.event_work = _add_work(
                epoch.event_work,
                M5RuntimeWork(
                    group_state_write_count=sum(
                        epoch.before_states.groups.get(key) != after.groups.get(key)
                        for key in set(epoch.before_states.groups) | set(after.groups)
                    ),
                    claim_state_write_count=sum(
                        epoch.before_states.claims.get(key) != after.claims.get(key)
                        for key in set(epoch.before_states.claims) | set(after.claims)
                    ),
                    answer_state_write_count=sum(
                        epoch.before_states.answers.get(key) != after.answers.get(key)
                        for key in set(epoch.before_states.answers) | set(after.answers)
                    ),
                    certificate_binding_write_count=certificate_write_count,
                    public_delta_count=len(deltas),
                ),
            )
            epoch.revision += 1
            self.world.terminalize_timing(epoch)
            self.world.published = deepcopy(epoch.working)
            publication = PublicationReceipt(
                epoch_id,
                stable_m4_digest("m4-publication-v1", str(epoch_id)),
                False,
            )
            event_timing, event_coverage = self.world.timing_image(
                epoch, project_pending=False
            )
            call_coverage = M5RuntimeTimingCoverage.single_point(
                None, terminal_client_roundtrip_included=False
            )
            result = M5EventRunResult.build(
                event_id=event.structural_event_id,
                payload_hash=event.payload_hash,
                epoch_id=epoch_id,
                state=M5RunState.SEALED,
                replayed_outcome=None,
                open_receipt=epoch.current_open_receipt,
                publication_receipt=publication,
                event_work=epoch.event_work,
                call_work=call_work,
                event_timing=event_timing,
                call_timing=M5RuntimeTiming(),
                combined_deltas=deltas,
                changed_state_references=references,
                failure_reason=None,
                event_timing_coverage=event_coverage,
                call_timing_coverage=call_coverage,
            )
            epoch.terminal_result = result
            epoch.sealed_states = after
            self.world.operation_log.append("typed-seal")
            return result

    def append_transition_call_timing(
        self,
        epoch_id: int,
        contribution_kind: M5RuntimeWorkContributionKind,
        source_id: str,
        contribution_key_digest: str,
        anchor_revision: int,
        observed_timing: M5RuntimeTiming | None,
    ) -> M5TransitionTimingReceipt:
        epoch = self.world.epoch(epoch_id)
        anchor = M5TransitionTimingAnchor(
            epoch_id,
            contribution_kind,
            source_id,
            contribution_key_digest,
            anchor_revision,
            False,
        )
        key = self.world._anchor_key(anchor)
        observation = M5RuntimeTimingObservation.build(observed_timing)
        existing = epoch.transition_observations.get(key)
        if existing is not None:
            if existing != observation:
                raise EventConflictError("fake transition timing replay conflicts")
            event_timing, coverage = self.world.timing_image(
                epoch, project_pending=True
            )
            receipt = M5TransitionTimingReceipt(
                anchor,
                digests.transition_call_timing_digest(
                    epoch_id=epoch_id,
                    contribution_kind=contribution_kind,
                    source_id=source_id,
                    contribution_key_digest=contribution_key_digest,
                    anchor_revision=anchor_revision,
                    observation_digest=observation.observation_digest,
                ),
                event_timing,
                coverage,
                epoch.revision,
                True,
            )
            receipt.validate_observation(observation)
            return receipt
        if epoch.pending_transition_anchor != anchor:
            raise EventConflictError("fake transition timing names no pending anchor")
        with self.world.transaction("transition-timing"):
            epoch.transition_observations[key] = observation
            epoch.timing_points.append(observed_timing)
            epoch.pending_transition_anchor = None
            event_timing, coverage = self.world.timing_image(
                epoch, project_pending=False
            )
            receipt = M5TransitionTimingReceipt(
                anchor,
                digests.transition_call_timing_digest(
                    epoch_id=epoch_id,
                    contribution_kind=contribution_kind,
                    source_id=source_id,
                    contribution_key_digest=contribution_key_digest,
                    anchor_revision=anchor_revision,
                    observation_digest=observation.observation_digest,
                ),
                event_timing,
                coverage,
                epoch.revision,
                False,
            )
            self.world.operation_log.append(f"timing:{contribution_kind.value}")
            return receipt

    def current_event_timing(
        self, epoch_id: int
    ) -> tuple[M5RuntimeTiming, M5RuntimeTimingCoverage]:
        epoch = self.world.epoch(epoch_id)
        return self.world.timing_image(
            epoch, project_pending=epoch.terminal_result is None
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
        epoch = self.world.epoch(epoch_id)
        if (
            epoch.plan.structural_event_id != event_id
            or epoch.terminal_result is None
            or epoch.terminal_result.logical_result_hash != terminal_logical_result_hash
        ):
            raise EventConflictError("fake terminal telemetry binding conflicts")
        payload = (
            event_id,
            epoch_id,
            terminal_logical_result_hash,
            call_timing,
            call_timing_coverage,
        )
        existing = self.world.terminal_telemetry.get(invocation_id)
        if existing is not None and existing != payload:
            raise EventConflictError("fake terminal telemetry replay conflicts")
        if existing is None:
            self.world.terminal_telemetry[invocation_id] = payload
            self.world.operation_log.append(f"terminal-telemetry:{invocation_id}")

    def current_event_work(self, epoch_id: int) -> M5RuntimeWork:
        return self.world.epoch(epoch_id).event_work

    def current_revision(self, epoch_id: int) -> int:
        return self.world.epoch(epoch_id).revision

    def archive_terminal_late_attempt_for_test(
        self, epoch_id: int, logical_job_id: str
    ) -> None:
        """Exercise the audit-only late-return boundary without reopening work."""

        epoch = self.world.epoch(epoch_id)
        job = epoch.jobs[logical_job_id]
        if not job.state.terminal:
            raise InvalidEventError("late-attempt audit requires a terminal job")
        revision = epoch.revision
        published = self.world.published.export_snapshot()
        with self.world.transaction("late-attempt-audit"):
            self.world.late_attempt_artifacts.append((epoch_id, logical_job_id))
            self.world.operation_log.append("late-attempt-audit-only")
        if (
            epoch.revision != revision
            or self.world.published.export_snapshot() != published
        ):
            raise AssertionError("audit-only late attempt mutated semantic state")


def _status_deltas(
    event_id: str, before: M5ReferenceStates, after: M5ReferenceStates
) -> tuple[StatusDelta, ...]:
    rows: list[StatusDelta] = []
    for object_type, old_states, new_states in (
        ("claim", before.claims, after.claims),
        ("answer", before.answers, after.answers),
    ):
        for object_id in sorted(new_states):
            old = old_states[object_id]
            new = new_states[object_id]
            if old.status is not new.status:
                rows.append(
                    StatusDelta(
                        event_id,
                        object_type,
                        object_id,
                        old.status.value,
                        new.status.value,
                        f"typed-event={event_id}",
                    )
                )
    return tuple(sorted(rows, key=lambda row: (row.object_type, row.object_id)))


def _changed_references(
    epoch_id: int,
    revision: int,
    before_repository: M5Repository,
    after_repository: M5Repository,
    before: M5ReferenceStates,
    after: M5ReferenceStates,
) -> tuple[M5ChangedStateReference, ...]:
    references: list[M5ChangedStateReference] = []
    for kind, old_states, new_states in (
        (
            M5StateReferenceKind.REQUIREMENT_STATE,
            before.requirements,
            after.requirements,
        ),
        (M5StateReferenceKind.GROUP_STATE, before.groups, after.groups),
        (M5StateReferenceKind.CLAIM_STATE, before.claims, after.claims),
        (M5StateReferenceKind.ANSWER_STATE, before.answers, after.answers),
    ):
        for object_id in sorted(set(old_states) | set(new_states)):
            if old_states.get(object_id) == new_states.get(object_id):
                continue
            state_hash = stable_m5_digest(
                "m5-fake-state-artifact-v1", text_field(repr(new_states.get(object_id)))
            )
            references.append(
                M5ChangedStateReference.build(
                    kind=kind,
                    object_id=object_id,
                    epoch_id=epoch_id,
                    revision=revision,
                    state_artifact_hash=state_hash,
                )
            )
    before_groups, before_claims = _certificates(before_repository)
    after_groups, after_claims = _certificates(after_repository)
    for kind, old_certificates, new_certificates in (
        (M5StateReferenceKind.GROUP_CERTIFICATE, before_groups, after_groups),
        (M5StateReferenceKind.CLAIM_CERTIFICATE, before_claims, after_claims),
    ):
        for object_id in sorted(set(old_certificates) | set(new_certificates)):
            old = old_certificates.get(object_id)
            new = new_certificates.get(object_id)
            if old == new:
                continue
            artifact_hash = (
                new.certificate_digest
                if new is not None
                else stable_m5_digest(
                    "m5-fake-absent-certificate-v1", text_field(object_id)
                )
            )
            references.append(
                M5ChangedStateReference.build(
                    kind=kind,
                    object_id=object_id,
                    epoch_id=epoch_id,
                    revision=revision,
                    state_artifact_hash=artifact_hash,
                )
            )
    return tuple(
        sorted(
            references,
            key=lambda item: (item.kind.value, item.object_id, item.reference_digest),
        )
    )


def _certificates(
    repository: M5Repository,
) -> tuple[
    dict[str, GroupMatchingCertificateArtifact],
    dict[str, ClaimCertificateArtifact],
]:
    groups: dict[str, GroupMatchingCertificateArtifact] = {}
    for group_id in sorted(repository.active_group_ids()):
        certificate = build_reference_group_certificate(repository, group_id)
        if certificate is not None:
            groups[group_id] = certificate
    claims = {
        claim_id: build_reference_claim_certificate(repository, claim_id, groups)
        for claim_id in sorted(repository.base.all_claim_ids())
    }
    return groups, claims


@dataclass(slots=True)
class FakeMeasurements:
    world: FakeTypedWorld
    terminal_invocation_results: list[M5EventRunResult] = field(default_factory=list)

    def transition_call_timing(
        self, anchor: M5TransitionTimingAnchor
    ) -> M5RuntimeTiming | None:
        self.world.operation_log.append(
            f"measure-transition:{anchor.contribution_kind.value}"
        )
        return self.world.transition_timing

    def terminal_invocation(
        self, event: M5TypedEventPlan, result: M5EventRunResult
    ) -> M5TerminalInvocationTelemetry:
        self.terminal_invocation_results.append(result)
        ordinal = self.world.next_invocation_ordinal
        self.world.next_invocation_ordinal += 1
        invocation_id = sha(
            f"invocation:{event.structural_event_id}:{result.epoch_id}:{ordinal}"
        )
        self.world.operation_log.append(f"measure-terminal:{invocation_id}")
        return M5TerminalInvocationTelemetry(
            invocation_id, self.world.terminal_call_timing
        )


@dataclass(slots=True)
class FakeAudit:
    world: FakeTypedWorld
    calls: int = 0

    def audit_after_seal(
        self, event: M5TypedEventPlan, result: M5EventRunResult
    ) -> None:
        self.calls += 1
        self.world.operation_log.append("audit:python-reference")
        epoch = self.world.epoch(result.epoch_id)
        independently_recomputed = compute_reference_states(self.world.published)
        if epoch.sealed_states != independently_recomputed:
            raise AssertionError("fake post-seal Python-oracle mismatch")
        if event.structural_event_id != result.event_id:
            raise AssertionError("fake post-seal event binding drift")


@dataclass(slots=True)
class FakeHarness:
    world: FakeTypedWorld
    structural: FakeStructural
    direct: FakeDirect
    runtime: FakeRuntime
    discovery: FakeDiscovery
    verifier: FakeVerifier
    measurements: FakeMeasurements
    audit: FakeAudit
    application: M5TypedApplication


def make_harness(repository: M5Repository | None = None) -> FakeHarness:
    world = FakeTypedWorld(repository or make_repository())
    structural = FakeStructural(world)
    direct = FakeDirect(world)
    runtime = FakeRuntime(world)
    discovery = FakeDiscovery(world)
    verifier = FakeVerifier(world)
    measurements = FakeMeasurements(world)
    audit = FakeAudit(world)
    application = M5TypedApplication(
        policies=FakePolicies(world),
        structural=structural,
        direct=direct,
        runtime=runtime,
        runtime_reads=runtime,
        discovery=discovery,
        verifier=verifier,
        measurements=measurements,
        post_seal_audit=audit,
    )
    return FakeHarness(
        world,
        structural,
        direct,
        runtime,
        discovery,
        verifier,
        measurements,
        audit,
        application,
    )


__all__ = [
    "FakeDirectInterruption",
    "FakeDirectSelectedOutcome",
    "FakeDiscoveryOutcome",
    "FakeHarness",
    "FakeTypedWorld",
    "FakeVerifierOutcome",
    "chunk_snapshot",
    "make_harness",
    "make_manifest",
    "make_repository",
    "make_typed_plan",
    "requirement_snapshot",
    "sha",
]
