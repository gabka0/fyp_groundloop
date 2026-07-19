from __future__ import annotations

from dataclasses import dataclass, field, replace

import pytest

from groundloop.domain import (
    ClaimStatus,
    DecisionPolicy,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
)
from groundloop.errors import EventConflictError, ValidationError
from groundloop.m4.application import (
    ApplicationExecutionPolicy,
    DiscoveryResult,
    DynamicEventPlan,
    EventRunState,
    ExternalWorkFailure,
    JobLease,
    M4Application,
    ObservationCompletionReceipt,
    OpenEventReceipt,
    PublicationReceipt,
    SealingSnapshot,
    StructuralWithdrawal,
    VerificationResult,
)
from groundloop.m4.contracts import (
    AdmissionChannel,
    AdmittedPair,
    CorpusUpdateIdentity,
    DiscoveryScope,
    JobAttempt,
    JobCompletion,
    JobKind,
    JobState,
    LogicalJobSpec,
    PairKey,
    UpdateKind,
    stable_m4_digest,
)
from groundloop.m4.runtime import (
    CompletionPlan,
    ObservationDependency,
    ReverseDependencyIndex,
    RuntimeBook,
    RuntimeEpoch,
    RuntimeEpochState,
    apply_completion,
    claim_evaluation_state,
    fail_epoch,
    open_epoch,
    plan_withdrawal,
    seal_epoch,
    start_attempt,
)
from groundloop.m4.runtime.epoch import EvaluationState


def _hash(value: str) -> str:
    return stable_m4_digest(value)


POLICY = DecisionPolicy("decision-v1", 0.7, 0.7)
PRODUCER = ModelStamp("fake-verifier", "1", "prompt-1")


def test_job_lease_requires_complete_attempt_binding() -> None:
    with pytest.raises(ValidationError, match="requires attempt"):
        JobLease("job-1", True, False)
    with pytest.raises(ValidationError, match="cannot carry"):
        JobLease(
            "job-1",
            False,
            True,
            attempt_id="attempt-1",
            lease_token_hash=_hash("lease-1"),
            expected_revision=2,
        )
    assert (
        JobLease(
            "job-1",
            True,
            False,
            attempt_id="attempt-1",
            lease_token_hash=_hash("lease-1"),
            expected_revision=2,
        ).expected_revision
        == 2
    )


def _scores(label: str) -> tuple[float, float, float]:
    if label == "support":
        return (0.9, 0.05, 0.05)
    if label == "refute":
        return (0.05, 0.9, 0.05)
    return (0.1, 0.1, 0.8)


def _observation(event_id: str, pair: PairKey, label: str) -> SemanticObservation:
    support, refute, neutral = _scores(label)
    return SemanticObservation(
        observation_id=_hash(
            f"observation:{event_id}:{pair.claim_id}:{pair.chunk_version_id}"
        ),
        subject_kind=SubjectKind.CLAIM,
        subject_id=pair.claim_id,
        chunk_version_id=pair.chunk_version_id,
        task_type="claim-verification-v1",
        support_score=support,
        refute_score=refute,
        neutral_score=neutral,
        producer=PRODUCER,
        input_hash=_hash(
            f"input:{event_id}:{pair.claim_id}:{pair.chunk_version_id}"
        ),
    )


def _derived_label(observation: SemanticObservation) -> str:
    support = observation.support_score >= POLICY.support_threshold
    refute = observation.refute_score >= POLICY.refute_threshold
    if support and not refute:
        return "support"
    if refute and not support:
        return "refute"
    return "neutral"


@dataclass
class FakeWorld:
    runtime_book: RuntimeBook = field(default_factory=RuntimeBook)
    active_chunk_ids: set[str] = field(default_factory=set)
    published: dict[PairKey, SemanticObservation] = field(default_factory=dict)
    working: dict[PairKey, SemanticObservation] = field(default_factory=dict)
    artifacts: dict[str, SemanticObservation] = field(default_factory=dict)
    publication_by_event: dict[str, str] = field(default_factory=dict)
    fallback_by_chunk: dict[str, tuple[str, ...]] = field(default_factory=dict)
    fallback_blocked: set[str] = field(default_factory=set)
    operation_log: list[str] = field(default_factory=list)
    fail_completion_once: bool = False

    def epoch(self, epoch_id: int) -> RuntimeEpoch:
        return next(
            epoch for epoch in self.runtime_book.epochs if epoch.epoch_id == epoch_id
        )

    def claim_status(self, claim_id: str) -> ClaimStatus:
        labels = {
            _derived_label(observation)
            for pair, observation in self.published.items()
            if pair.claim_id == claim_id
        }
        support = "support" in labels
        refute = "refute" in labels
        if support and refute:
            return ClaimStatus.CONFLICTED
        if refute:
            return ClaimStatus.REFUTED
        if support:
            return ClaimStatus.SUPPORTED
        return ClaimStatus.UNSUPPORTED


@dataclass
class FakeStructural:
    world: FakeWorld
    exact_withdrawal_calls: int = 0
    retrieval_calls: int = 0

    def plan_exact_withdrawal(
        self, event: DynamicEventPlan
    ) -> StructuralWithdrawal:
        self.exact_withdrawal_calls += 1
        self.world.operation_log.append("withdrawal")
        deactivated_chunk_version_ids = event.deactivated_chunk_version_ids
        dependencies = tuple(
            ObservationDependency(observation.observation_id, pair)
            for pair, observation in self.world.published.items()
        )
        index = ReverseDependencyIndex.build(dependencies, ())
        fallback = tuple(
            sorted(
                {
                    claim_id
                    for chunk_id in deactivated_chunk_version_ids
                    for claim_id in self.world.fallback_by_chunk.get(chunk_id, ())
                }
            )
        )
        return StructuralWithdrawal(
            plan_withdrawal(index, deactivated_chunk_version_ids), fallback
        )

    def open_event(
        self,
        event: DynamicEventPlan,
        withdrawal: StructuralWithdrawal,
        root_jobs: tuple[LogicalJobSpec, ...],
        discovery_scopes: tuple[DiscoveryScope, ...],
    ) -> OpenEventReceipt:
        transitioned = open_epoch(
            self.world.runtime_book,
            event.update,
            root_jobs,
            discovery_scopes,
        )
        self.world.runtime_book = transitioned.book
        epoch = next(
            known
            for known in transitioned.book.epochs
            if known.update.event_id == event.update.event_id
        )
        if transitioned.replayed:
            publication = self.world.publication_by_event.get(event.update.event_id)
            return OpenEventReceipt(
                epoch.epoch_id,
                replayed=True,
                already_sealed=epoch.state is RuntimeEpochState.SEALED,
                publication_id=publication,
            )
        self.world.operation_log.append("structural-open")
        self.world.working = dict(self.world.published)
        for chunk_id in withdrawal.plan.deactivated_chunk_ids:
            self.world.active_chunk_ids.discard(chunk_id)
            self.world.working = {
                pair: observation
                for pair, observation in self.world.working.items()
                if pair.chunk_version_id != chunk_id
            }
        self.world.active_chunk_ids.update(event.inserted_chunk_version_ids)
        return OpenEventReceipt(epoch.epoch_id, False, False)

    def chunk_is_active(self, chunk_version_id: str) -> bool:
        return chunk_version_id in self.world.active_chunk_ids


def _validate_fake_lease(epoch: RuntimeEpoch, lease: JobLease, job_id: str) -> None:
    job = next(item for item in epoch.jobs if item.spec.job_id == job_id)
    latest = job.attempts[-1]
    if (
        lease.job_id != job_id
        or lease.attempt_id != latest.attempt_id
        or lease.lease_token_hash != latest.lease_token_hash
        or lease.expected_revision is None
        or lease.expected_revision > epoch.revision
    ):
        raise EventConflictError("completion result belongs to a stale attempt")


@dataclass
class FakeRuntime:
    world: FakeWorld

    def pending_claim_ids(self, epoch_id: int) -> tuple[str, ...]:
        epoch = self.world.epoch(epoch_id)
        claims = {
            claim_id
            for scope in epoch.discovery_scopes
            for claim_id in scope.registered_claim_ids
        }
        claims.update(
            job.spec.target_claim_id
            for job in epoch.jobs
            if job.spec.target_claim_id is not None
        )
        return tuple(
            sorted(
                claim_id
                for claim_id in claims
                if claim_evaluation_state(epoch, claim_id)
                is EvaluationState.PENDING
            )
        )

    def acquire_job(self, epoch_id: int, spec: LogicalJobSpec) -> JobLease:
        epoch = self.world.epoch(epoch_id)
        runtime_job = next(job for job in epoch.jobs if job.spec.job_id == spec.job_id)
        if runtime_job.completion is not None:
            return JobLease(spec.job_id, False, True)
        if runtime_job.state is JobState.DECLARED:
            ordinal = len(runtime_job.attempts) + 1
            attempt = JobAttempt(
                attempt_id=f"attempt:{spec.job_id}:{ordinal}",
                job_id=spec.job_id,
                execution_spec_hash=spec.execution_spec_hash,
                attempt_ordinal=ordinal,
                lease_token_hash=_hash(f"lease:{spec.job_id}:{ordinal}"),
            )
            self.world.runtime_book = start_attempt(
                self.world.runtime_book, epoch_id, attempt
            ).book
            epoch = self.world.epoch(epoch_id)
        else:
            attempt = runtime_job.attempts[-1]
        self.world.operation_log.append(f"acquire:{spec.kind.value}")
        return JobLease(
            spec.job_id,
            True,
            False,
            attempt_id=attempt.attempt_id,
            lease_token_hash=attempt.lease_token_hash,
            expected_revision=epoch.revision,
        )

    def complete_expansion(
        self,
        epoch_id: int,
        lease: JobLease,
        completion: JobCompletion,
        child_jobs: tuple[LogicalJobSpec, ...],
    ) -> None:
        epoch = self.world.epoch(epoch_id)
        _validate_fake_lease(epoch, lease, completion.job_id)
        self.world.runtime_book = apply_completion(
            self.world.runtime_book,
            CompletionPlan(epoch_id, epoch.revision, completion, child_jobs),
            active_chunk_ids=frozenset(self.world.active_chunk_ids),
        ).book
        self.world.operation_log.append("expand")

    def children_of(
        self, epoch_id: int, root_job_id: str
    ) -> tuple[LogicalJobSpec, ...]:
        return tuple(
            job.spec
            for job in self.world.epoch(epoch_id).jobs
            if job.spec.parent_job_id == root_job_id
        )

    def mark_fallback_blocked(self, epoch_id: int, root_job_id: str) -> None:
        self.world.fallback_blocked.add(root_job_id)
        self.world.operation_log.append("fallback-blocked")

    def fail_epoch(self, epoch_id: int, reason: str) -> None:
        epoch = self.world.epoch(epoch_id)
        if epoch.state is not RuntimeEpochState.FAILED:
            self.world.runtime_book = fail_epoch(
                self.world.runtime_book, epoch_id, epoch.revision, reason
            ).book
        self.world.operation_log.append("failed")

    def sealing_snapshot(self, epoch_id: int) -> SealingSnapshot:
        epoch = self.world.epoch(epoch_id)
        open_jobs = tuple(sorted(job.spec.job_id for job in epoch.jobs if job.open))
        open_scopes = tuple(
            sorted(
                scope.root_job_id
                for scope in epoch.discovery_scopes
                if not scope.closed
            )
        )
        blocked = tuple(sorted(self.world.fallback_blocked))
        return SealingSnapshot(
            epoch_id=epoch_id,
            revision=epoch.revision,
            ready=(
                epoch.state is RuntimeEpochState.SEMANTIC_COMPLETE
                and not blocked
            ),
            failed=epoch.state is RuntimeEpochState.FAILED,
            open_job_ids=open_jobs,
            open_discovery_root_ids=open_scopes,
            fallback_blocked_root_ids=blocked,
        )


@dataclass
class FakeAdmission:
    world: FakeWorld
    claims_by_target: dict[tuple[JobKind, str], tuple[str, ...]] = field(
        default_factory=dict
    )
    unsatisfied_targets: set[tuple[JobKind, str]] = field(default_factory=set)
    fail_targets: set[tuple[JobKind, str]] = field(default_factory=set)
    calls: int = 0
    pending_seen: list[tuple[str, ...]] = field(default_factory=list)

    def discover(
        self, epoch_id: int, root_job: LogicalJobSpec
    ) -> DiscoveryResult:
        self.calls += 1
        target = root_job.target_chunk_version_id or root_job.target_claim_id or ""
        key = (root_job.kind, target)
        self.pending_seen.append(FakeRuntime(self.world).pending_claim_ids(epoch_id))
        self.world.operation_log.append(f"admission:{root_job.kind.value}")
        if key in self.fail_targets:
            raise ExternalWorkFailure("injected admission failure")
        claim_ids = self.claims_by_target.get(key, ())
        if root_job.kind is JobKind.IMPACT_DISCOVERY:
            pairs = tuple(PairKey(claim_id, target) for claim_id in claim_ids)
        else:
            pairs = tuple(PairKey(target, chunk_id) for chunk_id in claim_ids)
        admitted = tuple(
            AdmittedPair(
                epoch_id=epoch_id,
                pair=pair,
                candidate_policy_id=root_job.candidate_policy_id,
                fused_rank=rank,
                reasons=(AdmissionChannel.VECTOR,),
                mandatory_lineage=False,
            )
            for rank, pair in enumerate(pairs, start=1)
        )
        artifact_hash = _hash(f"admission:{root_job.job_id}:{pairs}")
        return DiscoveryResult(
            root_job_id=root_job.job_id,
            result_artifact_id=f"admission:{root_job.job_id}",
            result_artifact_hash=artifact_hash,
            admitted_pairs=admitted,
            fallback_satisfied=key not in self.unsatisfied_targets,
        )


@dataclass
class FakeVerifier:
    world: FakeWorld
    labels: dict[PairKey, str] = field(default_factory=dict)
    fail_pairs: set[PairKey] = field(default_factory=set)
    late_inactive_pairs: set[PairKey] = field(default_factory=set)
    calls: int = 0

    def verify(
        self, epoch_id: int, verifier_job: LogicalJobSpec
    ) -> VerificationResult:
        self.calls += 1
        pair = verifier_job.pair
        assert pair is not None
        self.world.operation_log.append("verifier")
        if pair in self.fail_pairs:
            raise ExternalWorkFailure("injected verifier failure")
        observation = _observation(
            verifier_job.event_id, pair, self.labels.get(pair, "neutral")
        )
        if pair in self.late_inactive_pairs:
            epoch = self.world.epoch(epoch_id)
            self.world.runtime_book = fail_epoch(
                self.world.runtime_book,
                epoch_id,
                epoch.revision,
                "concurrent operator failure",
            ).book
            self.world.active_chunk_ids.discard(pair.chunk_version_id)
        return VerificationResult(
            result_artifact_id=f"verification:{observation.observation_id}",
            result_artifact_hash=_hash(repr(observation)),
            observation=observation,
        )


@dataclass
class FakeObservations:
    world: FakeWorld
    calls: int = 0

    def complete_verifier_atomically(
        self,
        epoch_id: int,
        lease: JobLease,
        verifier_job: LogicalJobSpec,
        completion: JobCompletion,
        observation: SemanticObservation,
        *,
        make_effective: bool,
    ) -> ObservationCompletionReceipt:
        self.calls += 1
        if self.world.fail_completion_once:
            self.world.fail_completion_once = False
            raise ExternalWorkFailure("injected atomic completion crash")
        expected_effective = completion.terminal_state is JobState.COMPLETED_ACTIVE
        if make_effective != expected_effective:
            raise EventConflictError("effective flag disagrees with terminal state")
        existing = self.world.artifacts.get(observation.observation_id)
        if existing is not None:
            if existing != observation:
                raise EventConflictError("observation identity conflict")
        epoch = self.world.epoch(epoch_id)
        _validate_fake_lease(epoch, lease, completion.job_id)
        staged_book = apply_completion(
            self.world.runtime_book,
            CompletionPlan(epoch_id, epoch.revision, completion),
            active_chunk_ids=frozenset(self.world.active_chunk_ids),
        ).book
        self.world.runtime_book = staged_book
        is_new = existing is None
        if is_new:
            self.world.artifacts[observation.observation_id] = observation
        pair = PairKey(observation.subject_id, observation.chunk_version_id)
        if make_effective:
            self.world.working[pair] = observation
            self.world.operation_log.append("working-overlay-append")
        else:
            self.world.operation_log.append("inactive-artifact-archive")
        self.world.operation_log.append("verify-complete")
        return ObservationCompletionReceipt(
            artifact_stored=is_new,
            made_effective=make_effective,
        )


@dataclass
class FakeGates:
    world: FakeWorld
    calls: list[str] = field(default_factory=list)

    def check_grounding(self, epoch_id: int) -> None:
        self.calls.append("grounding")
        self.world.operation_log.append("gate-grounding")

    def check_coordination(self, epoch_id: int) -> None:
        self.calls.append("coordination")
        self.world.operation_log.append("gate-coordination")

    def check_evaluation(self, epoch_id: int) -> None:
        self.calls.append("evaluation")
        self.world.operation_log.append("gate-evaluation")


@dataclass
class FakePublication:
    world: FakeWorld
    calls: int = 0

    def request_seal(
        self,
        epoch_id: int,
        expected_revision: int,
        update: CorpusUpdateIdentity,
    ) -> PublicationReceipt:
        self.calls += 1
        self.world.operation_log.append("publish")
        self.world.runtime_book = seal_epoch(
            self.world.runtime_book, epoch_id, expected_revision
        ).book
        self.world.published = dict(self.world.working)
        publication_id = f"publication:{epoch_id}"
        self.world.publication_by_event[update.event_id] = publication_id
        return PublicationReceipt(epoch_id, publication_id)


@dataclass
class Harness:
    world: FakeWorld
    structural: FakeStructural
    runtime: FakeRuntime
    admission: FakeAdmission
    verifier: FakeVerifier
    observations: FakeObservations
    gates: FakeGates
    publication: FakePublication
    application: M4Application


def _harness() -> Harness:
    world = FakeWorld()
    structural = FakeStructural(world)
    runtime = FakeRuntime(world)
    admission = FakeAdmission(world)
    verifier = FakeVerifier(world)
    observations = FakeObservations(world)
    gates = FakeGates(world)
    publication = FakePublication(world)
    application = M4Application(
        structural=structural,
        runtime=runtime,
        admission=admission,
        verifier=verifier,
        observations=observations,
        equality_gates=gates,
        publication=publication,
        execution_policy=ApplicationExecutionPolicy(
            impact_discovery_execution_spec_hash=_hash("impact-execution"),
            frontier_retrieval_execution_spec_hash=_hash("frontier-execution"),
            verifier_execution_spec_hash=_hash("verifier-execution"),
        ),
    )
    return Harness(
        world,
        structural,
        runtime,
        admission,
        verifier,
        observations,
        gates,
        publication,
        application,
    )


def _event(
    event_id: str,
    kind: UpdateKind,
    *,
    inserted: tuple[str, ...] = (),
    deactivated: tuple[str, ...] = (),
    previous: int | None = None,
    payload_suffix: str = "",
) -> DynamicEventPlan:
    return DynamicEventPlan(
        update=CorpusUpdateIdentity(
            event_id=event_id,
            payload_hash=_hash(
                f"payload:{event_id}:{kind.value}:{inserted}:{deactivated}:"
                f"{payload_suffix}"
            ),
            update_kind=kind,
            previous_published_epoch_id=previous,
            candidate_policy_id="candidate-policy-1",
        ),
        inserted_chunk_version_ids=inserted,
        deactivated_chunk_version_ids=deactivated,
        registered_claim_ids=("claim-1", "claim-2"),
        claim_registry_snapshot_id="registry-1",
    )


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("support", ClaimStatus.SUPPORTED),
        ("neutral", ClaimStatus.UNSUPPORTED),
        ("refute", ClaimStatus.REFUTED),
    ],
)
def test_insertion_runs_pending_discovery_verification_gates_and_seal(
    label: str, expected: ClaimStatus
) -> None:
    harness = _harness()
    event = _event("insert-1", UpdateKind.INSERT, inserted=("new-chunk",))
    key = (JobKind.IMPACT_DISCOVERY, "new-chunk")
    pair = PairKey("claim-1", "new-chunk")
    harness.admission.claims_by_target[key] = ("claim-1",)
    harness.verifier.labels[pair] = label

    result = harness.application.run_event(event)

    assert result.state is EventRunState.SEALED
    assert result.discovery_call_count == 1
    assert result.verifier_call_count == 1
    assert result.observation_artifact_count == 1
    assert result.effective_observation_count == 1
    assert harness.world.claim_status("claim-1") is expected
    assert harness.admission.pending_seen == [("claim-1", "claim-2")]
    assert harness.gates.calls == ["grounding", "coordination", "evaluation"]
    assert harness.world.operation_log.index("working-overlay-append") < (
        harness.world.operation_log.index("publish")
    )


def _seed_support(harness: Harness, chunk_id: str = "old-chunk") -> None:
    pair = PairKey("claim-1", chunk_id)
    observation = _observation("seed", pair, "support")
    harness.world.active_chunk_ids.add(chunk_id)
    harness.world.published[pair] = observation
    harness.world.working[pair] = observation
    harness.world.artifacts[observation.observation_id] = observation
    harness.world.fallback_by_chunk[chunk_id] = ("claim-1",)


def test_delete_uses_exact_withdrawal_and_no_impact_retrieval() -> None:
    harness = _harness()
    _seed_support(harness)
    event = _event("delete-1", UpdateKind.DELETE, deactivated=("old-chunk",))

    result = harness.application.run_event(event)

    assert result.state is EventRunState.SEALED
    assert harness.structural.exact_withdrawal_calls == 1
    assert harness.structural.retrieval_calls == 0
    assert harness.world.claim_status("claim-1") is ClaimStatus.UNSUPPORTED
    assert result.verifier_call_count == 0
    assert "admission:impact_discovery" not in harness.world.operation_log


@pytest.mark.parametrize(
    ("new_label", "expected"),
    [
        ("neutral", ClaimStatus.UNSUPPORTED),
        ("refute", ClaimStatus.REFUTED),
    ],
)
def test_replacement_withdraws_support_then_applies_new_observation(
    new_label: str, expected: ClaimStatus
) -> None:
    harness = _harness()
    _seed_support(harness)
    event = _event(
        f"replace-{new_label}",
        UpdateKind.REPLACE,
        inserted=("new-chunk",),
        deactivated=("old-chunk",),
    )
    harness.admission.claims_by_target[
        (JobKind.IMPACT_DISCOVERY, "new-chunk")
    ] = ("claim-1",)
    harness.verifier.labels[PairKey("claim-1", "new-chunk")] = new_label

    result = harness.application.run_event(event)

    assert result.state is EventRunState.SEALED
    assert harness.world.claim_status("claim-1") is expected
    assert all(
        pair.chunk_version_id != "old-chunk" for pair in harness.world.published
    )


def test_empty_impact_discovery_closes_scope_and_seals_without_verifier() -> None:
    harness = _harness()
    event = _event("empty-1", UpdateKind.INSERT, inserted=("empty-chunk",))

    result = harness.application.run_event(event)

    assert result.state is EventRunState.SEALED
    assert result.discovery_call_count == 1
    assert result.verifier_call_count == 0
    assert harness.publication.calls == 1


def test_exact_replay_uses_existing_publication_and_conflict_is_rejected() -> None:
    harness = _harness()
    event = _event("replay-1", UpdateKind.INSERT, inserted=("new-chunk",))
    first = harness.application.run_event(event)
    admission_calls = harness.admission.calls
    publication_calls = harness.publication.calls

    replay = harness.application.run_event(event)

    assert first.state is EventRunState.SEALED
    assert replay.state is EventRunState.REPLAYED
    assert replay.publication_id == first.publication_id
    assert harness.admission.calls == admission_calls
    assert harness.publication.calls == publication_calls
    conflicting = replace(
        event,
        update=replace(event.update, payload_hash=_hash("different-payload")),
    )
    with pytest.raises(EventConflictError):
        harness.application.run_event(conflicting)


def test_failure_before_expansion_preserves_published_snapshot() -> None:
    harness = _harness()
    _seed_support(harness)
    before = dict(harness.world.published)
    event = _event("fail-admission", UpdateKind.INSERT, inserted=("new-chunk",))
    harness.admission.fail_targets.add(
        (JobKind.IMPACT_DISCOVERY, "new-chunk")
    )

    result = harness.application.run_event(event)

    assert result.state is EventRunState.FAILED
    assert harness.world.published == before
    assert harness.publication.calls == 0
    assert "expand" not in harness.world.operation_log


def test_failure_after_expansion_preserves_published_snapshot() -> None:
    harness = _harness()
    _seed_support(harness)
    before = dict(harness.world.published)
    event = _event("fail-verifier", UpdateKind.INSERT, inserted=("new-chunk",))
    pair = PairKey("claim-1", "new-chunk")
    harness.admission.claims_by_target[
        (JobKind.IMPACT_DISCOVERY, "new-chunk")
    ] = ("claim-1",)
    harness.verifier.fail_pairs.add(pair)

    result = harness.application.run_event(event)

    assert result.state is EventRunState.FAILED
    assert "expand" in harness.world.operation_log
    assert harness.world.published == before
    assert harness.publication.calls == 0


def test_atomic_completion_failure_exposes_no_effective_observation() -> None:
    harness = _harness()
    before = dict(harness.world.published)
    event = _event("fail-after-overlay", UpdateKind.INSERT, inserted=("new-chunk",))
    harness.admission.claims_by_target[
        (JobKind.IMPACT_DISCOVERY, "new-chunk")
    ] = ("claim-1",)
    harness.verifier.labels[PairKey("claim-1", "new-chunk")] = "support"
    harness.world.fail_completion_once = True

    result = harness.application.run_event(event)

    assert result.state is EventRunState.FAILED
    assert result.observation_artifact_count == 0
    assert result.effective_observation_count == 0
    assert harness.world.working == before
    assert harness.world.published == before
    assert harness.publication.calls == 0
    child = next(
        job
        for job in harness.world.epoch(1).jobs
        if job.spec.kind is JobKind.VERIFY_PAIR
    )
    assert child.state is JobState.RUNNING
    assert "working-overlay-append" not in harness.world.operation_log


def test_late_inactive_completion_skips_observation_and_publication() -> None:
    harness = _harness()
    event = _event("late-1", UpdateKind.INSERT, inserted=("new-chunk",))
    pair = PairKey("claim-1", "new-chunk")
    harness.admission.claims_by_target[
        (JobKind.IMPACT_DISCOVERY, "new-chunk")
    ] = ("claim-1",)
    harness.verifier.late_inactive_pairs.add(pair)

    result = harness.application.run_event(event)

    assert result.state is EventRunState.FAILED
    assert result.inactive_completion_count == 1
    assert result.observation_artifact_count == 1
    assert result.effective_observation_count == 0
    assert harness.world.published == {}
    assert harness.publication.calls == 0
    assert len(harness.world.artifacts) == 1
    assert "inactive-artifact-archive" in harness.world.operation_log
    child = next(
        job
        for job in harness.world.epoch(1).jobs
        if job.spec.kind is JobKind.VERIFY_PAIR
    )
    assert child.state is JobState.COMPLETED_INACTIVE


def test_unsatisfied_frontier_fallback_blocks_sealing() -> None:
    harness = _harness()
    _seed_support(harness)
    event = _event("blocked-1", UpdateKind.DELETE, deactivated=("old-chunk",))
    harness.admission.unsatisfied_targets.add(
        (JobKind.FRONTIER_RETRIEVE, "claim-1")
    )

    result = harness.application.run_event(event)

    assert result.state is EventRunState.BLOCKED
    assert harness.publication.calls == 0
    assert harness.world.claim_status("claim-1") is ClaimStatus.SUPPORTED
    assert harness.world.fallback_blocked


def test_event_wide_pair_dedup_assigns_one_verifier_child() -> None:
    harness = _harness()
    _seed_support(harness)
    event = _event(
        "dedup-1",
        UpdateKind.REPLACE,
        inserted=("new-chunk",),
        deactivated=("old-chunk",),
    )
    harness.admission.claims_by_target[
        (JobKind.IMPACT_DISCOVERY, "new-chunk")
    ] = ("claim-1",)
    harness.admission.claims_by_target[
        (JobKind.FRONTIER_RETRIEVE, "claim-1")
    ] = ("new-chunk",)

    result = harness.application.run_event(event)

    assert result.state is EventRunState.SEALED
    assert result.discovery_call_count == 2
    assert result.verifier_call_count == 1
    children = tuple(
        job for job in harness.world.epoch(1).jobs if job.spec.parent_job_id
    )
    assert len(children) == 1
