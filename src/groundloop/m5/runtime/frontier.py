"""Pure M5 retrieval fusion, withdrawal, deduplication, and frontier rules."""

from __future__ import annotations

from dataclasses import dataclass

from groundloop.errors import ValidationError
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    M5AttemptArchiveReason,
    M5CandidatePolicyManifest,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5JobCompletion,
    M5JobKind,
    M5JobState,
    M5LogicalJobSpec,
    M5RequirementAdmissionChannel,
    M5RequirementAdmittedPair,
    M5RequirementAdmittedPairSource,
    M5RequirementChannelHit,
    M5RequirementDiscoveryResult,
    M5RequirementFallbackKey,
    M5RequirementFrontierHead,
    M5RequirementScopeSelection,
    M5RequirementWithdrawalPlan,
    M5RetrievalTermination,
    SemanticPairKey,
    validate_requirement_channel_hits,
)


@dataclass(frozen=True, slots=True)
class M5WithdrawnCandidateEdge:
    semantic_pair_digest: str
    requirement_version_id: str
    chunk_version_id: str
    candidate_policy_id: str

    def __post_init__(self) -> None:
        if len(self.semantic_pair_digest) != 64 or any(
            character not in "0123456789abcdef"
            for character in self.semantic_pair_digest
        ):
            raise ValidationError("withdrawn candidate pair digest must be SHA-256")
        for name, value in (
            ("requirement_version_id", self.requirement_version_id),
            ("chunk_version_id", self.chunk_version_id),
            ("candidate_policy_id", self.candidate_policy_id),
        ):
            _require_nonempty(name, value)


@dataclass(frozen=True, slots=True)
class M5WithdrawnObservationEdge:
    observation_id: str
    requirement_version_id: str
    chunk_version_id: str
    candidate_policy_id: str

    def __post_init__(self) -> None:
        for name, value in (
            ("observation_id", self.observation_id),
            ("requirement_version_id", self.requirement_version_id),
            ("chunk_version_id", self.chunk_version_id),
            ("candidate_policy_id", self.candidate_policy_id),
        ):
            _require_nonempty(name, value)


@dataclass(frozen=True, slots=True)
class M5RootOwnership:
    root_job_id: str
    scope_contract_digest: str
    semantic_pair_digests: tuple[str, ...]
    scope_closure_digest: str


@dataclass(frozen=True, slots=True)
class M5DeduplicationPlan:
    epoch_id: int
    candidate_policy_id: str
    admitted_pairs: tuple[M5RequirementAdmittedPair, ...]
    root_ownership: tuple[M5RootOwnership, ...]


@dataclass(frozen=True, slots=True)
class M5RootClosurePlan:
    root_job_id: str
    scope_contract_digest: str
    semantic_pair_digests: tuple[str, ...]
    scope_closure_digest: str
    child_job_ids: tuple[str, ...]
    child_set_hash: str


@dataclass(frozen=True, slots=True)
class M5RootBarrierPlan:
    structural_event_id: str
    requirement_root_set_hash: str
    admitted_pairs: tuple[M5RequirementAdmittedPair, ...]
    root_closures: tuple[M5RootClosurePlan, ...]
    barrier_completion_hash: str


def _require_nonempty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be nonempty")


def _require_nonnegative(name: str, value: int, *, positive: bool = False) -> None:
    minimum = 1 if positive else 0
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        qualifier = "positive" if positive else "nonnegative"
        raise ValidationError(f"{name} must be a {qualifier} integer")


def validate_directional_hit_order(
    hits: tuple[M5RequirementChannelHit, ...],
    direction: M5DiscoveryDirection,
) -> None:
    """Validate score ordering and the frozen UTF-8 tie breaker."""

    validate_requirement_channel_hits(hits)
    for channel in (
        M5RequirementAdmissionChannel.VECTOR,
        M5RequirementAdmissionChannel.LEXICAL,
    ):
        channel_hits = tuple(hit for hit in hits if hit.channel is channel)

        def ordering(hit: M5RequirementChannelHit) -> tuple[float, bytes]:
            assert hit.score is not None
            target = (
                hit.pair.chunk_version_id
                if direction is M5DiscoveryDirection.FORWARD_REQUIREMENT
                else hit.pair.subject_id
            )
            return -hit.score, target.encode("utf-8")

        if channel_hits != tuple(sorted(channel_hits, key=ordering)):
            raise ValidationError(
                "vector/lexical ranks violate score and UTF-8 target ordering"
            )


def rank_interleave_scope_selections(
    *,
    hits: tuple[M5RequirementChannelHit, ...],
    direction: M5DiscoveryDirection,
    approximate_budget: int,
    lineage_safety_override: bool,
) -> tuple[M5RequirementScopeSelection, ...]:
    """Apply vector-first rank interleave and append lineage-only pairs."""

    _require_nonnegative("approximate_budget", approximate_budget, positive=True)
    validate_directional_hit_order(hits, direction)
    if not lineage_safety_override and any(
        hit.channel is M5RequirementAdmissionChannel.LINEAGE for hit in hits
    ):
        raise ValidationError("lineage hit supplied while safety override is off")
    if not hits:
        return ()
    root_ids = {hit.root_job_id for hit in hits}
    scope_ids = {hit.scope_contract_digest for hit in hits}
    if len(root_ids) != 1 or len(scope_ids) != 1:
        raise ValidationError("one fusion call requires exactly one root and scope")
    root_job_id = next(iter(root_ids))
    scope_contract_digest = next(iter(scope_ids))

    hit_by_channel = {
        channel: tuple(hit for hit in hits if hit.channel is channel)
        for channel in M5RequirementAdmissionChannel
    }
    pair_by_digest: dict[str, SemanticPairKey] = {}
    reasons_by_digest: dict[str, set[M5RequirementAdmissionChannel]] = {}
    for hit in hits:
        existing = pair_by_digest.setdefault(hit.semantic_pair_digest, hit.pair)
        if existing != hit.pair:
            raise ValidationError("one semantic pair digest names different pairs")
        reasons_by_digest.setdefault(hit.semantic_pair_digest, set()).add(hit.channel)

    selected: list[str] = []
    selected_set: set[str] = set()
    approximate_channels = (
        M5RequirementAdmissionChannel.VECTOR,
        M5RequirementAdmissionChannel.LEXICAL,
    )
    next_index = {channel: 0 for channel in approximate_channels}
    while len(selected) < approximate_budget:
        consumed = False
        for channel in approximate_channels:
            channel_hits = hit_by_channel[channel]
            index = next_index[channel]
            if index >= len(channel_hits):
                continue
            consumed = True
            hit = channel_hits[index]
            next_index[channel] = index + 1
            if hit.semantic_pair_digest not in selected_set:
                selected.append(hit.semantic_pair_digest)
                selected_set.add(hit.semantic_pair_digest)
                if len(selected) == approximate_budget:
                    break
        if not consumed:
            break

    if lineage_safety_override:
        lineage_only = sorted(
            {
                hit.semantic_pair_digest
                for hit in hit_by_channel[M5RequirementAdmissionChannel.LINEAGE]
            }
            - selected_set
        )
        selected.extend(lineage_only)

    return tuple(
        M5RequirementScopeSelection.build(
            root_job_id=root_job_id,
            scope_contract_digest=scope_contract_digest,
            pair=pair_by_digest[pair_digest],
            fused_rank=rank,
            reasons=tuple(
                sorted(reasons_by_digest[pair_digest], key=lambda item: item.value)
            ),
        )
        for rank, pair_digest in enumerate(selected, start=1)
    )


def build_rank_interleaved_discovery_result(
    *,
    hits: tuple[M5RequirementChannelHit, ...],
    direction: M5DiscoveryDirection,
    manifest: M5CandidatePolicyManifest,
    root_job_id: str,
    scope_contract_digest: str,
    eligible_snapshot_exhausted: bool,
) -> M5RequirementDiscoveryResult:
    """Build a successful result; unavailable/error states have no DTO path."""

    budget = (
        manifest.forward_budget_per_requirement
        if direction is M5DiscoveryDirection.FORWARD_REQUIREMENT
        else manifest.reverse_budget_per_inserted_chunk
    )
    if hits:
        if any(
            hit.root_job_id != root_job_id
            or hit.scope_contract_digest != scope_contract_digest
            or hit.candidate_policy_id != manifest.candidate_policy_id
            for hit in hits
        ):
            raise ValidationError(
                "discovery hit does not bind the requested root/policy"
            )
        selections = rank_interleave_scope_selections(
            hits=hits,
            direction=direction,
            approximate_budget=budget,
            lineage_safety_override=manifest.lineage_safety_override,
        )
    else:
        selections = ()
    approximate_count = sum(
        bool(
            set(selection.reasons)
            & {
                M5RequirementAdmissionChannel.VECTOR,
                M5RequirementAdmissionChannel.LEXICAL,
            }
        )
        for selection in selections
    )
    if approximate_count == budget:
        termination = M5RetrievalTermination.BUDGET_FILLED
    else:
        if not eligible_snapshot_exhausted:
            raise ValidationError(
                "a short successful result requires frozen-snapshot exhaustion"
            )
        termination = M5RetrievalTermination.SNAPSHOT_EXHAUSTED
    result = M5RequirementDiscoveryResult.build(
        root_job_id=root_job_id,
        scope_contract_digest=scope_contract_digest,
        termination=termination,
        channel_hits=hits,
        selections=selections,
    )
    result.validate_policy(
        direction=direction,
        manifest=manifest,
        eligible_snapshot_exhausted=eligible_snapshot_exhausted,
    )
    return result


def deduplicate_discovery_results(
    *,
    epoch_id: int,
    candidate_policy_id: str,
    results: tuple[M5RequirementDiscoveryResult, ...],
    inactive_root_job_ids: tuple[str, ...] = (),
) -> M5DeduplicationPlan:
    """Union staged selections and choose one least-root owner per pair."""

    _require_nonnegative("epoch_id", epoch_id, positive=True)
    _require_nonempty("candidate_policy_id", candidate_policy_id)
    root_ids = tuple(result.root_job_id for result in results)
    if len(set(root_ids)) != len(root_ids):
        raise ValidationError("event-wide staged results repeat a root")
    ordered_results = tuple(sorted(results, key=lambda result: result.root_job_id))
    inactive_roots = set(inactive_root_job_ids)
    if not inactive_roots <= set(root_ids):
        raise ValidationError("inactive root set contains an unknown root")
    selections_by_pair: dict[str, list[M5RequirementScopeSelection]] = {}
    pair_by_digest: dict[str, SemanticPairKey] = {}
    for result in ordered_results:
        if result.channel_hits and any(
            hit.epoch_id != epoch_id or hit.candidate_policy_id != candidate_policy_id
            for hit in result.channel_hits
        ):
            raise ValidationError("staged result epoch/policy binding mismatch")
        if result.root_job_id in inactive_roots:
            continue
        for selection in result.selections:
            pair_by_digest.setdefault(selection.semantic_pair_digest, selection.pair)
            if pair_by_digest[selection.semantic_pair_digest] != selection.pair:
                raise ValidationError("semantic digest collision across staged roots")
            selections_by_pair.setdefault(selection.semantic_pair_digest, []).append(
                selection
            )

    admitted: list[M5RequirementAdmittedPair] = []
    for pair_digest in sorted(selections_by_pair):
        selections = tuple(
            sorted(
                selections_by_pair[pair_digest],
                key=lambda selection: selection.root_job_id,
            )
        )
        sources = tuple(
            M5RequirementAdmittedPairSource(
                selection.root_job_id,
                selection.scope_contract_digest,
                selection.selection_digest,
            )
            for selection in selections
        )
        reasons = tuple(
            sorted(
                {reason for selection in selections for reason in selection.reasons},
                key=lambda reason: reason.value,
            )
        )
        admitted.append(
            M5RequirementAdmittedPair.build(
                epoch_id=epoch_id,
                pair=pair_by_digest[pair_digest],
                candidate_policy_id=candidate_policy_id,
                sources=sources,
                reasons=reasons,
            )
        )
    admitted_pairs = tuple(sorted(admitted, key=lambda pair: pair.semantic_pair_digest))
    owned_by_root: dict[str, list[str]] = {root_id: [] for root_id in root_ids}
    scope_by_root = {
        result.root_job_id: result.scope_contract_digest for result in ordered_results
    }
    for pair in admitted_pairs:
        owned_by_root[pair.owner_root_job_id].append(pair.semantic_pair_digest)
    ownership = tuple(
        M5RootOwnership(
            root_job_id=root_id,
            scope_contract_digest=scope_by_root[root_id],
            semantic_pair_digests=tuple(sorted(owned_by_root[root_id])),
            scope_closure_digest=digests.discovery_scope_closure_digest(
                scope_by_root[root_id], owned_by_root[root_id]
            ),
        )
        for root_id in sorted(root_ids)
    )
    return M5DeduplicationPlan(
        epoch_id=epoch_id,
        candidate_policy_id=candidate_policy_id,
        admitted_pairs=admitted_pairs,
        root_ownership=ownership,
    )


def bind_verifier_children(
    plan: M5DeduplicationPlan,
    child_jobs: tuple[M5LogicalJobSpec, ...],
) -> tuple[M5RootClosurePlan, ...]:
    """Validate the closure/child bijection and bind canonical child sets."""

    child_by_pair: dict[str, M5LogicalJobSpec] = {}
    for child in child_jobs:
        if (
            child.job_kind is not M5JobKind.VERIFY_REQUIREMENT_PAIR
            or child.semantic_pair_digest is None
            or child.parent_job_id is None
        ):
            raise ValidationError("root closure may bind only verifier child jobs")
        if child.semantic_pair_digest in child_by_pair:
            raise ValidationError("two verifier children bind the same pair")
        if child.candidate_policy_id != plan.candidate_policy_id:
            raise ValidationError("verifier child uses another candidate policy")
        child_by_pair[child.semantic_pair_digest] = child
    admitted_digests = {pair.semantic_pair_digest for pair in plan.admitted_pairs}
    if set(child_by_pair) != admitted_digests:
        raise ValidationError(
            "verifier children and admitted pairs are not a bijection"
        )
    owner_by_pair = {
        pair.semantic_pair_digest: pair.owner_root_job_id
        for pair in plan.admitted_pairs
    }
    for pair_digest, child in child_by_pair.items():
        if child.parent_job_id != owner_by_pair[pair_digest]:
            raise ValidationError("verifier child belongs to a nonowner root")
        admitted_pair = next(
            pair
            for pair in plan.admitted_pairs
            if pair.semantic_pair_digest == pair_digest
        )
        if child.pair != admitted_pair.pair:
            raise ValidationError(
                "verifier child pair payload disagrees with admission"
            )

    scope_by_root = {
        ownership.root_job_id: ownership.scope_contract_digest
        for ownership in plan.root_ownership
    }
    for child in child_jobs:
        assert child.parent_job_id is not None
        if child.scope_contract_digest != scope_by_root[child.parent_job_id]:
            raise ValidationError("verifier child uses another root scope")

    return tuple(
        M5RootClosurePlan(
            root_job_id=ownership.root_job_id,
            scope_contract_digest=ownership.scope_contract_digest,
            semantic_pair_digests=ownership.semantic_pair_digests,
            scope_closure_digest=ownership.scope_closure_digest,
            child_job_ids=tuple(
                sorted(
                    child_by_pair[pair_digest].logical_job_id
                    for pair_digest in ownership.semantic_pair_digests
                )
            ),
            child_set_hash=digests.child_set_digest(
                child_by_pair[pair_digest].logical_job_id
                for pair_digest in ownership.semantic_pair_digests
            ),
        )
        for ownership in plan.root_ownership
    )


def build_root_barrier_plan(
    *,
    structural_event_id: str,
    results: tuple[M5RequirementDiscoveryResult, ...],
    deduplication: M5DeduplicationPlan,
    child_jobs: tuple[M5LogicalJobSpec, ...],
) -> M5RootBarrierPlan:
    _require_nonempty("structural_event_id", structural_event_id)
    result_by_root = {result.root_job_id: result for result in results}
    roots = tuple(ownership.root_job_id for ownership in deduplication.root_ownership)
    if set(result_by_root) != set(roots) or len(result_by_root) != len(results):
        raise ValidationError("barrier result/root ownership sets disagree")
    if any(child.structural_event_id != structural_event_id for child in child_jobs):
        raise ValidationError("barrier child belongs to another structural event")
    root_set_hash = digests.requirement_root_set_digest(roots)
    closures = bind_verifier_children(deduplication, child_jobs)
    barrier_hash = digests.requirement_root_barrier_completion_digest(
        structural_event_id=structural_event_id,
        requirement_root_set_hash=root_set_hash,
        root_result_hashes=(
            (root_id, result_by_root[root_id].result_artifact_hash)
            for root_id in sorted(result_by_root)
        ),
        admitted_pair_digests=(
            (pair.semantic_pair_digest, pair.admitted_pair_digest)
            for pair in deduplication.admitted_pairs
        ),
    )
    return M5RootBarrierPlan(
        structural_event_id,
        root_set_hash,
        deduplication.admitted_pairs,
        closures,
        barrier_hash,
    )


def plan_requirement_withdrawal(
    *,
    event_id: str,
    deactivated_chunk_version_ids: tuple[str, ...],
    candidate_edges: tuple[M5WithdrawnCandidateEdge, ...],
    observation_edges: tuple[M5WithdrawnObservationEdge, ...],
    cancelled_job_ids: tuple[str, ...],
    active_requirement_version_ids: tuple[str, ...],
) -> M5RequirementWithdrawalPlan:
    """Visit exact reverse edges and coalesce fresh forward fallback keys."""

    _require_nonempty("event_id", event_id)
    deactivated = set(deactivated_chunk_version_ids)
    active_requirements = set(active_requirement_version_ids)
    affected_candidates = tuple(
        edge for edge in candidate_edges if edge.chunk_version_id in deactivated
    )
    affected_observations = tuple(
        edge for edge in observation_edges if edge.chunk_version_id in deactivated
    )
    candidate_fallback_keys = {
        M5RequirementFallbackKey(edge.requirement_version_id, edge.candidate_policy_id)
        for edge in affected_candidates
        if edge.requirement_version_id in active_requirements
    }
    observation_fallback_keys = {
        M5RequirementFallbackKey(edge.requirement_version_id, edge.candidate_policy_id)
        for edge in affected_observations
        if edge.requirement_version_id in active_requirements
    }
    chunks = tuple(sorted(deactivated))
    pairs = tuple(sorted({edge.semantic_pair_digest for edge in affected_candidates}))
    observations = tuple(
        sorted({edge.observation_id for edge in affected_observations})
    )
    jobs = tuple(sorted(set(cancelled_job_ids)))
    fallback = tuple(sorted(candidate_fallback_keys | observation_fallback_keys))
    plan_digest = digests.requirement_withdrawal_plan_digest(
        event_id=event_id,
        deactivated_chunk_version_ids=chunks,
        withdrawn_candidate_pair_digests=pairs,
        withdrawn_observation_ids=observations,
        cancelled_job_ids=jobs,
        fallback_keys=(
            (key.requirement_version_id, key.candidate_policy_id) for key in fallback
        ),
    )
    return M5RequirementWithdrawalPlan(
        event_id,
        chunks,
        pairs,
        observations,
        jobs,
        fallback,
        plan_digest,
    )


def coalesce_forward_root_keys(
    *,
    new_requirement_keys: tuple[M5RequirementFallbackKey, ...],
    fallback_keys: tuple[M5RequirementFallbackKey, ...],
) -> tuple[M5RequirementFallbackKey, ...]:
    """Use one forward root per event/requirement/policy key."""

    return tuple(sorted(set(new_requirement_keys) | set(fallback_keys)))


def classify_attempt_activity(
    *,
    epoch_active: bool,
    requirement_active: bool | None,
    group_active: bool | None,
    chunk_active: bool | None,
    job_already_terminal: bool,
) -> M5AttemptArchiveReason | None:
    """Apply EPOCH_FAILED > SUBJECT_INACTIVE > CHUNK_INACTIVE precedence."""

    if not epoch_active:
        return M5AttemptArchiveReason.EPOCH_FAILED
    if requirement_active is False or group_active is False:
        return M5AttemptArchiveReason.SUBJECT_INACTIVE
    if chunk_active is False:
        return M5AttemptArchiveReason.CHUNK_INACTIVE
    if job_already_terminal:
        return M5AttemptArchiveReason.JOB_ALREADY_TERMINAL
    return None


def build_forward_frontier_head(
    *,
    job: M5LogicalJobSpec,
    scope: M5DiscoveryScopeContract,
    discovery_result: M5RequirementDiscoveryResult,
    completion: M5JobCompletion,
    completed_epoch_id: int,
    completed_revision: int,
) -> M5RequirementFrontierHead:
    """Build a head only for a successful active forward root closure."""

    _require_nonnegative("completed_epoch_id", completed_epoch_id, positive=True)
    _require_nonnegative("completed_revision", completed_revision)
    if (
        job.job_kind is not M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL
        or scope.direction is not M5DiscoveryDirection.FORWARD_REQUIREMENT
        or scope.requirement_version_id is None
        or completion.terminal_state is not M5JobState.COMPLETED_ACTIVE
    ):
        raise ValidationError("only active forward completion may update a frontier")
    completion.validate_job(job)
    if (
        job.scope_contract_digest != scope.scope_contract_digest
        or discovery_result.root_job_id != job.logical_job_id
        or discovery_result.scope_contract_digest != scope.scope_contract_digest
        or discovery_result.result_artifact_id != completion.result_artifact_id
        or discovery_result.result_artifact_hash != completion.result_artifact_hash
        or completion.scope_closure_digest is None
    ):
        raise ValidationError("frontier head artifacts do not bind one forward root")
    return M5RequirementFrontierHead(
        requirement_version_id=scope.requirement_version_id,
        candidate_policy_id=job.candidate_policy_id,
        latest_root_job_id=job.logical_job_id,
        latest_scope_contract_digest=scope.scope_contract_digest,
        latest_active_chunk_snapshot_digest=job.active_chunk_snapshot_digest,
        latest_discovery_result_artifact_hash=(discovery_result.result_artifact_hash),
        latest_scope_closure_digest=completion.scope_closure_digest,
        latest_completion_digest=completion.completion_digest,
        completed_epoch_id=completed_epoch_id,
        completed_revision=completed_revision,
    )


def advance_requirement_frontier(
    current: M5RequirementFrontierHead | None,
    candidate: M5RequirementFrontierHead,
) -> M5RequirementFrontierHead:
    """Apply latest-successful-forward semantics with exact replay validation."""

    if current is None:
        return candidate
    if (
        current.requirement_version_id,
        current.candidate_policy_id,
    ) != (
        candidate.requirement_version_id,
        candidate.candidate_policy_id,
    ):
        raise ValidationError("frontier heads use different requirement/policy keys")
    current_point = current.completed_epoch_id, current.completed_revision
    candidate_point = candidate.completed_epoch_id, candidate.completed_revision
    if candidate_point > current_point:
        return candidate
    if candidate_point == current_point and candidate != current:
        raise ValidationError("same frontier completion point has another payload")
    return current


__all__ = [
    "M5DeduplicationPlan",
    "M5RootBarrierPlan",
    "M5RootClosurePlan",
    "M5RootOwnership",
    "M5WithdrawnCandidateEdge",
    "M5WithdrawnObservationEdge",
    "advance_requirement_frontier",
    "bind_verifier_children",
    "build_forward_frontier_head",
    "build_rank_interleaved_discovery_result",
    "build_root_barrier_plan",
    "classify_attempt_activity",
    "coalesce_forward_root_keys",
    "deduplicate_discovery_results",
    "plan_requirement_withdrawal",
    "rank_interleave_scope_selections",
    "validate_directional_hit_order",
]
