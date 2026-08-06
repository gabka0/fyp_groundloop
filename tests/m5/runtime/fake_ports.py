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
from dataclasses import dataclass, field

from groundloop.domain import (
    AnswerVersion,
    Claim,
    DecisionPolicy,
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
    OpenEventReceipt,
    PublicationReceipt,
    StructuralWithdrawal,
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
    M5TypedApplication,
    M5VerifierExecution,
)
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5AttemptCompletionReceipt,
    M5AttemptOutput,
    M5CandidatePolicyManifest,
    M5ChangedStateReference,
    M5DiscoveryDirection,
    M5EventRunResult,
    M5JobAttempt,
    M5JobCompletion,
    M5JobKind,
    M5JobLease,
    M5JobState,
    M5LogicalJobSpec,
    M5ReplayedOutcome,
    M5RequirementAdmissionChannel,
    M5RequirementChannelHit,
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
    M5ScopeState,
    M5StateReferenceKind,
    M5TerminalReason,
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


def sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _add_work(*items: M5RuntimeWork) -> M5RuntimeWork:
    return M5RuntimeWork(
        **{
            name: sum(getattr(item, name) for item in items)
            for name in M5RuntimeWork.counter_names()
        }
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


@dataclass(frozen=True, slots=True)
class FakeVerifierOutcome:
    scores: tuple[float, float, float] = (0.9, 0.05, 0.05)
    failure_reason: M5RunFailureReason | None = None
    retryable: bool = False


@dataclass(slots=True)
class _FakeJob:
    spec: M5LogicalJobSpec
    state: M5JobState = M5JobState.DECLARED
    attempts: list[M5JobAttempt] = field(default_factory=list)
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
    revision: int = 1
    root_results: dict[str, M5RequirementDiscoveryResult] = field(default_factory=dict)
    attempt_outputs: dict[str, M5AttemptOutput] = field(default_factory=dict)
    root_barrier: M5RootBarrierReceipt | None = None
    event_work: M5RuntimeWork = M5RuntimeWork()
    direct_done: bool = False
    failed: bool = False
    terminal_result: M5EventRunResult | None = None
    sealed_states: M5ReferenceStates | None = None


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
    late_attempt_artifacts: list[tuple[int, str]] = field(default_factory=list)

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
        direct_withdrawal: StructuralWithdrawal | None,
        requirement_withdrawal: M5RequirementWithdrawalPlan,
        direct_roots: tuple[M4LogicalJobSpec, ...],
        requirement_roots: tuple[M5RequirementRootDeclaration, ...],
        requirement_root_set_hash: str,
    ) -> OpenEventReceipt:
        if event.direct_plan is not None:
            if direct_withdrawal is None:
                raise ValidationError("fake direct event lacks its withdrawal")
            if (
                direct_withdrawal.plan.deactivated_chunk_ids
                != event.direct_plan.deactivated_chunk_version_ids
            ):
                raise ValidationError("fake direct withdrawal drift")
        elif direct_withdrawal is not None or direct_roots:
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
            return OpenEventReceipt(existing.epoch_id, True, False)
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
            )
            self.world.epochs_by_event[event.structural_event_id] = epoch
            self.world.operation_log.append("typed-open")
            return OpenEventReceipt(epoch_id, False, False)


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
        return M5DirectOpenPlan(
            withdrawal,
            tuple(sorted(roots, key=lambda root: root.job_id)),
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

    def run_pending_direct(
        self, epoch_id: int, expected_revision: int, event: M5TypedEventPlan
    ) -> M5DirectExecutionReceipt:
        epoch = self.world.epoch(epoch_id)
        if expected_revision != epoch.revision:
            raise EventConflictError("stale fake direct revision")
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
        self.world.assert_external_boundary("requirement-discovery")
        epoch = self.world.epoch(epoch_id)
        direction, target = self.world.root_target(epoch, job)
        key = (event.structural_event_id, direction, target)
        outcomes = self.world.discovery_outcomes.get(key, [])
        outcome = outcomes.pop(0) if outcomes else FakeDiscoveryOutcome()
        call_work = M5RuntimeWork(
            requirement_forward_retrieval_call_count=int(
                direction is M5DiscoveryDirection.FORWARD_REQUIREMENT
            ),
            requirement_reverse_retrieval_call_count=int(
                direction is M5DiscoveryDirection.REVERSE_CHUNK
            ),
            requirement_fallback_forward_call_count=int(
                direction is M5DiscoveryDirection.FORWARD_REQUIREMENT
                and not isinstance(event.event, (RegisterGroupEvent, ReplaceGroupEvent))
            ),
            embedding_model_call_count=1,
        )
        epoch.event_work = _add_work(epoch.event_work, call_work)
        if outcome.failure_reason is not None:
            raise M5ExternalWorkFailure(
                outcome.failure_reason,
                retryable=outcome.retryable,
                call_work=call_work,
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
        result_work = M5RuntimeWork(
            requirement_channel_hit_count=len(result.channel_hits),
            requirement_pre_dedup_selection_count=len(result.selections),
        )
        return M5DiscoveryExecution(
            result,
            attempt_output,
            True,
            _add_work(call_work, result_work),
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
        self.world.assert_external_boundary("requirement-verifier")
        assert job.pair is not None
        epoch = self.world.epoch(epoch_id)
        key = (job.pair.subject_id, job.pair.chunk_version_id)
        outcomes = self.world.verifier_outcomes.get(key, [])
        outcome = outcomes.pop(0) if outcomes else FakeVerifierOutcome()
        call_work = M5RuntimeWork(
            requirement_verifier_call_count=1,
            verifier_model_call_count=1,
            verifier_input_token_count=8,
            verifier_output_token_count=3,
        )
        epoch.event_work = _add_work(epoch.event_work, call_work)
        if outcome.failure_reason is not None:
            raise M5ExternalWorkFailure(
                outcome.failure_reason,
                retryable=outcome.retryable,
                call_work=call_work,
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
        return M5VerifierExecution(pair_input, artifact, output, call_work)


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
            event_timing=stored.event_timing,
            call_timing=M5RuntimeTiming(),
            combined_deltas=stored.combined_deltas,
            changed_state_references=stored.changed_state_references,
            failure_reason=stored.failure_reason,
        )

    def acquire_m5_job(
        self, epoch_id: int, expected_revision: int, job: M5LogicalJobSpec
    ) -> M5JobLease:
        epoch = self.world.epoch(epoch_id)
        runtime_job = epoch.jobs.get(job.logical_job_id)
        if runtime_job is None or runtime_job.spec != job:
            raise EventConflictError("fake acquire names another job")
        if job.logical_job_id in epoch.root_results:
            return M5JobLease(job.logical_job_id, None, epoch.revision, False, True)
        if runtime_job.state.terminal:
            return M5JobLease(job.logical_job_id, None, epoch.revision, False, True)
        if expected_revision != epoch.revision:
            raise EventConflictError("stale fake acquisition revision")
        with self.world.transaction("job-acquire"):
            ordinal = len(runtime_job.attempts) + 1
            attempt = M5JobAttempt.build(
                logical_job_id=job.logical_job_id,
                attempt_ordinal=ordinal,
                execution_spec_hash=job.execution_spec_hash,
                lease_token_hash=sha(f"lease:{job.logical_job_id}:{ordinal}"),
            )
            runtime_job.attempts.append(attempt)
            runtime_job.state = M5JobState.RUNNING
            epoch.revision += 1
            self.world.operation_log.append(f"acquire:{job.job_kind.value}")
            return M5JobLease(
                job.logical_job_id,
                attempt,
                epoch.revision,
                True,
                False,
            )

    def mark_m5_retryable_failure(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5JobLease,
        error_hash: str,
    ) -> M5AttemptCompletionReceipt:
        del error_hash
        epoch = self.world.epoch(epoch_id)
        job = epoch.jobs[lease.logical_job_id]
        if expected_revision != epoch.revision or lease.attempt is None:
            raise EventConflictError("stale fake retryable failure")
        with self.world.transaction("retryable-failure"):
            job.state = M5JobState.RETRYABLE_FAILED
            epoch.revision += 1
            self.world.operation_log.append("retryable-failure")
            return M5AttemptCompletionReceipt(
                job.spec.logical_job_id,
                lease.attempt.attempt_id,
                epoch.revision,
                False,
            )

    def stage_m5_discovery_result_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        result: M5RequirementDiscoveryResult,
        attempt_output: M5AttemptOutput,
    ) -> M5AttemptCompletionReceipt:
        epoch = self.world.epoch(epoch_id)
        if expected_revision != epoch.revision or lease.attempt is None:
            raise EventConflictError("stale fake discovery staging")
        existing = epoch.attempt_outputs.get(attempt_output.attempt_id)
        if existing is not None:
            if existing != attempt_output:
                raise EventConflictError("fake attempt output conflict")
            return M5AttemptCompletionReceipt(
                job.logical_job_id,
                attempt_output.attempt_id,
                epoch.revision,
                True,
            )
        with self.world.transaction("discovery-stage"):
            epoch.attempt_outputs[attempt_output.attempt_id] = attempt_output
            epoch.root_results[job.logical_job_id] = result
            epoch.scope_states[job.logical_job_id] = M5ScopeState.RESULT_STAGED
            epoch.event_work = _add_work(
                epoch.event_work,
                M5RuntimeWork(
                    requirement_channel_hit_count=len(result.channel_hits),
                    requirement_pre_dedup_selection_count=len(result.selections),
                ),
            )
            epoch.revision += 1
            self.world.operation_log.append("discovery-staged")
            return M5AttemptCompletionReceipt(
                job.logical_job_id,
                attempt_output.attempt_id,
                epoch.revision,
                False,
            )

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
    ) -> M5AttemptCompletionReceipt:
        epoch = self.world.epoch(epoch_id)
        runtime_job = epoch.jobs[job.logical_job_id]
        if runtime_job.state.terminal:
            assert lease.attempt is not None
            return M5AttemptCompletionReceipt(
                job.logical_job_id,
                lease.attempt.attempt_id,
                epoch.revision,
                True,
            )
        if expected_revision != epoch.revision or lease.attempt is None:
            raise EventConflictError("stale fake verifier completion")
        with self.world.transaction("verifier-completion"):
            epoch.attempt_outputs[attempt_output.attempt_id] = attempt_output
            assert job.pair is not None
            active = epoch.working.is_requirement_active(
                job.pair.subject_id
            ) and epoch.working.base.is_chunk_active(job.pair.chunk_version_id)
            point = epoch.working.advance_semantic_revision()
            observation = verifier_artifact.to_semantic_observation()
            record = epoch.working.register_requirement_observation(observation, point)
            if record.eligible_for_currency != active:
                raise AssertionError("fake activity classification drift")
            terminal_state = (
                M5JobState.COMPLETED_ACTIVE if active else M5JobState.COMPLETED_INACTIVE
            )
            runtime_job.state = terminal_state
            runtime_job.completion = M5JobCompletion.build(
                job=job,
                terminal_state=terminal_state,
                result_artifact_id=verifier_artifact.artifact_id,
                result_artifact_hash=verifier_artifact.artifact_hash,
                archive_reason=(None if active else M5TerminalReason.SUBJECT_INACTIVE),
            )
            epoch.event_work = _add_work(
                epoch.event_work,
                M5RuntimeWork(
                    requirement_observation_artifact_count=1,
                    requirement_effective_observation_count=int(active),
                    requirement_inactive_completion_count=int(not active),
                ),
            )
            epoch.revision += 1
            self.world.operation_log.append("verifier-complete")
            return M5AttemptCompletionReceipt(
                job.logical_job_id,
                lease.attempt.attempt_id,
                epoch.revision,
                False,
            )

    def fail_typed_epoch_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        failure_reason: M5RunFailureReason,
    ) -> M5EventRunResult:
        epoch = self.world.epoch(epoch_id)
        if epoch.terminal_result is not None:
            return (
                self.read_typed_event_result(
                    epoch.plan.structural_event_id, epoch.plan.payload_hash
                )
                or epoch.terminal_result
            )
        if expected_revision != epoch.revision:
            raise EventConflictError("stale fake failure revision")
        with self.world.transaction("typed-failure"):
            cancelled = 0
            for runtime_job in epoch.jobs.values():
                if not runtime_job.state.terminal:
                    runtime_job.state = M5JobState.CANCELLED
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
            result = M5EventRunResult.build(
                event_id=epoch.plan.structural_event_id,
                payload_hash=epoch.plan.payload_hash,
                epoch_id=epoch_id,
                state=M5RunState.FAILED,
                replayed_outcome=None,
                open_receipt=OpenEventReceipt(epoch_id, False, False),
                publication_receipt=None,
                event_work=epoch.event_work,
                call_work=M5RuntimeWork(),
                event_timing=M5RuntimeTiming(),
                call_timing=M5RuntimeTiming(),
                combined_deltas=(),
                changed_state_references=(),
                failure_reason=failure_reason,
            )
            epoch.terminal_result = result
            self.world.operation_log.append("typed-failed")
            return result

    def request_typed_seal_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        event: M5TypedEventPlan,
    ) -> M5EventRunResult:
        epoch = self.world.epoch(epoch_id)
        if expected_revision != epoch.revision:
            raise EventConflictError("stale fake seal revision")
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
            self.world.published = deepcopy(epoch.working)
            publication = PublicationReceipt(
                epoch_id,
                stable_m4_digest("m4-publication-v1", str(epoch_id)),
                False,
            )
            result = M5EventRunResult.build(
                event_id=event.structural_event_id,
                payload_hash=event.payload_hash,
                epoch_id=epoch_id,
                state=M5RunState.SEALED,
                replayed_outcome=None,
                open_receipt=OpenEventReceipt(epoch_id, False, False),
                publication_receipt=publication,
                event_work=epoch.event_work,
                call_work=M5RuntimeWork(),
                event_timing=M5RuntimeTiming(),
                call_timing=M5RuntimeTiming(),
                combined_deltas=deltas,
                changed_state_references=references,
                failure_reason=None,
            )
            epoch.terminal_result = result
            epoch.sealed_states = after
            self.world.operation_log.append("typed-seal")
            return result

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
    audit: FakeAudit
    application: M5TypedApplication


def make_harness(repository: M5Repository | None = None) -> FakeHarness:
    world = FakeTypedWorld(repository or make_repository())
    structural = FakeStructural(world)
    direct = FakeDirect(world)
    runtime = FakeRuntime(world)
    discovery = FakeDiscovery(world)
    verifier = FakeVerifier(world)
    audit = FakeAudit(world)
    application = M5TypedApplication(
        policies=FakePolicies(world),
        structural=structural,
        direct=direct,
        runtime=runtime,
        runtime_reads=runtime,
        discovery=discovery,
        verifier=verifier,
        post_seal_audit=audit,
    )
    return FakeHarness(
        world, structural, direct, runtime, discovery, verifier, audit, application
    )


__all__ = [
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
