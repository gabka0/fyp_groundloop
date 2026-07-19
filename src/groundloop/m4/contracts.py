"""Frozen immutable contracts for GroundLoop M4.

These records separate empirical candidate/model products from exact runtime
coordination and structured maintenance. They contain identities and results;
they do not implement admission, job transitions, or semantic oracles.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from enum import StrEnum

from groundloop.domain import AnswerState, ClaimState, VerificationLabel
from groundloop.errors import ValidationError

_HEX = frozenset("0123456789abcdef")


def stable_m4_digest(*parts: str) -> str:
    """Hash ordered UTF-8 fields without concatenation ambiguity."""
    digest = hashlib.sha256()
    for part in parts:
        encoded = part.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _require_text(name: str, value: str) -> None:
    if not value.strip():
        raise ValidationError(f"{name} must be non-empty")


def _require_sha256(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in _HEX for character in value):
        raise ValidationError(f"{name} must be a lowercase SHA-256 hex digest")


def _require_sorted_unique(name: str, values: tuple[str, ...]) -> None:
    if values != tuple(sorted(set(values))):
        raise ValidationError(f"{name} must be sorted and unique")


class UpdateKind(StrEnum):
    INSERT = "insert"
    DELETE = "delete"
    REPLACE = "replace"


class AdmissionChannel(StrEnum):
    VECTOR = "vector"
    LEXICAL = "lexical"
    LINEAGE = "lineage"
    FRONTIER = "frontier"
    LEARNED = "learned"


class VectorIndexKind(StrEnum):
    EXACT = "exact"
    HNSW = "hnsw"


class JobKind(StrEnum):
    IMPACT_DISCOVERY = "impact_discovery"
    FRONTIER_RETRIEVE = "frontier_retrieve"
    VERIFY_PAIR = "verify_pair"


class JobState(StrEnum):
    DECLARED = "declared"
    RUNNING = "running"
    COMPLETED_ACTIVE = "completed_active"
    COMPLETED_INACTIVE = "completed_inactive"
    RETRYABLE_FAILED = "retryable_failed"
    TERMINAL_FAILED = "terminal_failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in {
            JobState.COMPLETED_ACTIVE,
            JobState.COMPLETED_INACTIVE,
            JobState.TERMINAL_FAILED,
            JobState.CANCELLED,
        }


class FrontierState(StrEnum):
    UNVERIFIED = "unverified"
    QUEUED = "queued"
    VERIFIED_CURRENT = "verified_current"
    INACTIVE = "inactive"
    FAILED = "failed"


class JudgmentSourceKind(StrEnum):
    MODEL = "model"
    HUMAN = "human"


class ImpactTargetKind(StrEnum):
    TEACHER_NONNEUTRAL = "teacher_nonneutral"
    HUMAN_NONNEUTRAL = "human_nonneutral"


class ComparisonOutcome(StrEnum):
    AGREE = "agree"
    DISAGREE = "disagree"
    NOT_APPLICABLE = "not_applicable"
    FAILED = "failed"


@dataclass(frozen=True, slots=True, order=True)
class PairKey:
    claim_id: str
    chunk_version_id: str

    def __post_init__(self) -> None:
        _require_text("claim_id", self.claim_id)
        _require_text("chunk_version_id", self.chunk_version_id)


@dataclass(frozen=True, slots=True)
class CorpusUpdateIdentity:
    event_id: str
    payload_hash: str
    update_kind: UpdateKind
    previous_published_epoch_id: int | None
    candidate_policy_id: str

    def __post_init__(self) -> None:
        _require_text("event_id", self.event_id)
        _require_sha256("payload_hash", self.payload_hash)
        _require_text("candidate_policy_id", self.candidate_policy_id)
        if (
            self.previous_published_epoch_id is not None
            and self.previous_published_epoch_id <= 0
        ):
            raise ValidationError("previous_published_epoch_id must be positive")


@dataclass(frozen=True, slots=True)
class CandidatePolicyManifest:
    policy_id: str
    policy_hash: str
    embedding_model_artifact_id: str
    claim_role_template_hash: str
    chunk_role_template_hash: str
    vector_method_version: str
    vector_index_kind: VectorIndexKind
    vector_index_build_config_hash: str
    vector_search_config_hash: str
    lexical_method_version: str
    lexical_config_hash: str
    lexical_postgres_version: str
    lexical_regconfig_identity: str
    claim_registry_snapshot_id: str
    claim_count: int
    fusion_version: str
    approximate_cap_per_inserted_chunk: int
    frontier_depth: int
    verifier_execution_spec_hash: str
    decision_policy_version: str
    lineage_safety_override: bool = True

    def __post_init__(self) -> None:
        for name, value in (
            ("policy_id", self.policy_id),
            ("embedding_model_artifact_id", self.embedding_model_artifact_id),
            ("vector_method_version", self.vector_method_version),
            ("lexical_method_version", self.lexical_method_version),
            ("lexical_postgres_version", self.lexical_postgres_version),
            ("lexical_regconfig_identity", self.lexical_regconfig_identity),
            ("claim_registry_snapshot_id", self.claim_registry_snapshot_id),
            ("fusion_version", self.fusion_version),
            ("decision_policy_version", self.decision_policy_version),
        ):
            _require_text(name, value)
        for name, value in (
            ("policy_hash", self.policy_hash),
            ("claim_role_template_hash", self.claim_role_template_hash),
            ("chunk_role_template_hash", self.chunk_role_template_hash),
            ("vector_index_build_config_hash", self.vector_index_build_config_hash),
            ("vector_search_config_hash", self.vector_search_config_hash),
            ("lexical_config_hash", self.lexical_config_hash),
            ("verifier_execution_spec_hash", self.verifier_execution_spec_hash),
        ):
            _require_sha256(name, value)
        if self.approximate_cap_per_inserted_chunk <= 0:
            raise ValidationError("approximate admission cap must be positive")
        if self.frontier_depth <= 0:
            raise ValidationError("frontier depth must be positive")
        if self.claim_count < 0:
            raise ValidationError("claim_count must be nonnegative")
        if self.policy_hash != self.expected_policy_hash():
            raise ValidationError("policy_hash does not match candidate policy fields")

    def expected_policy_hash(self) -> str:
        return stable_m4_digest(
            "m4-candidate-policy-v1",
            self.embedding_model_artifact_id,
            self.claim_role_template_hash,
            self.chunk_role_template_hash,
            self.vector_method_version,
            self.vector_index_kind.value,
            self.vector_index_build_config_hash,
            self.vector_search_config_hash,
            self.lexical_method_version,
            self.lexical_config_hash,
            self.lexical_postgres_version,
            self.lexical_regconfig_identity,
            self.claim_registry_snapshot_id,
            str(self.claim_count),
            self.fusion_version,
            str(self.approximate_cap_per_inserted_chunk),
            str(self.frontier_depth),
            self.verifier_execution_spec_hash,
            self.decision_policy_version,
            "lineage" if self.lineage_safety_override else "no-lineage",
        )

    @classmethod
    def build(
        cls,
        *,
        policy_id: str,
        embedding_model_artifact_id: str,
        claim_role_template_hash: str,
        chunk_role_template_hash: str,
        vector_method_version: str,
        vector_index_kind: VectorIndexKind,
        vector_index_build_config_hash: str,
        vector_search_config_hash: str,
        lexical_method_version: str,
        lexical_config_hash: str,
        lexical_postgres_version: str,
        lexical_regconfig_identity: str,
        claim_registry_snapshot_id: str,
        claim_count: int,
        fusion_version: str,
        approximate_cap_per_inserted_chunk: int,
        frontier_depth: int,
        verifier_execution_spec_hash: str,
        decision_policy_version: str,
        lineage_safety_override: bool = True,
    ) -> CandidatePolicyManifest:
        fields = (
            embedding_model_artifact_id,
            claim_role_template_hash,
            chunk_role_template_hash,
            vector_method_version,
            vector_index_kind.value,
            vector_index_build_config_hash,
            vector_search_config_hash,
            lexical_method_version,
            lexical_config_hash,
            lexical_postgres_version,
            lexical_regconfig_identity,
            claim_registry_snapshot_id,
            str(claim_count),
            fusion_version,
            str(approximate_cap_per_inserted_chunk),
            str(frontier_depth),
            verifier_execution_spec_hash,
            decision_policy_version,
            "lineage" if lineage_safety_override else "no-lineage",
        )
        return cls(
            policy_id=policy_id,
            policy_hash=stable_m4_digest("m4-candidate-policy-v1", *fields),
            embedding_model_artifact_id=embedding_model_artifact_id,
            claim_role_template_hash=claim_role_template_hash,
            chunk_role_template_hash=chunk_role_template_hash,
            vector_method_version=vector_method_version,
            vector_index_kind=vector_index_kind,
            vector_index_build_config_hash=vector_index_build_config_hash,
            vector_search_config_hash=vector_search_config_hash,
            lexical_method_version=lexical_method_version,
            lexical_config_hash=lexical_config_hash,
            lexical_postgres_version=lexical_postgres_version,
            lexical_regconfig_identity=lexical_regconfig_identity,
            claim_registry_snapshot_id=claim_registry_snapshot_id,
            claim_count=claim_count,
            fusion_version=fusion_version,
            approximate_cap_per_inserted_chunk=approximate_cap_per_inserted_chunk,
            frontier_depth=frontier_depth,
            verifier_execution_spec_hash=verifier_execution_spec_hash,
            decision_policy_version=decision_policy_version,
            lineage_safety_override=lineage_safety_override,
        )


@dataclass(frozen=True, slots=True)
class ChannelHit:
    epoch_id: int
    pair: PairKey
    candidate_policy_id: str
    channel: AdmissionChannel
    rank: int
    score: float | None
    channel_artifact_hash: str

    def __post_init__(self) -> None:
        if self.epoch_id <= 0:
            raise ValidationError("epoch_id must be positive")
        _require_text("candidate_policy_id", self.candidate_policy_id)
        _require_sha256("channel_artifact_hash", self.channel_artifact_hash)
        if self.rank <= 0:
            raise ValidationError("channel rank must be positive")
        if self.score is not None and not math.isfinite(self.score):
            raise ValidationError("channel score must be finite")


@dataclass(frozen=True, slots=True)
class AdmittedPair:
    epoch_id: int
    pair: PairKey
    candidate_policy_id: str
    fused_rank: int
    reasons: tuple[AdmissionChannel, ...]
    mandatory_lineage: bool

    def __post_init__(self) -> None:
        if self.epoch_id <= 0:
            raise ValidationError("epoch_id must be positive")
        _require_text("candidate_policy_id", self.candidate_policy_id)
        if self.fused_rank <= 0:
            raise ValidationError("fused_rank must be positive")
        if not self.reasons:
            raise ValidationError("admitted pair requires an admission reason")
        if self.reasons != tuple(sorted(set(self.reasons), key=str)):
            raise ValidationError("admission reasons must be sorted and unique")
        if self.mandatory_lineage != (AdmissionChannel.LINEAGE in self.reasons):
            raise ValidationError("mandatory_lineage must match LINEAGE reason")


@dataclass(frozen=True, slots=True)
class LogicalJobSpec:
    job_id: str
    event_id: str
    kind: JobKind
    candidate_policy_id: str
    payload_hash: str
    execution_spec_hash: str
    parent_job_id: str | None = None
    pair: PairKey | None = None
    target_claim_id: str | None = None
    target_chunk_version_id: str | None = None
    expandable: bool = False

    def __post_init__(self) -> None:
        for name, value in (
            ("job_id", self.job_id),
            ("event_id", self.event_id),
            ("candidate_policy_id", self.candidate_policy_id),
        ):
            _require_text(name, value)
        _require_sha256("payload_hash", self.payload_hash)
        _require_sha256("execution_spec_hash", self.execution_spec_hash)
        if self.parent_job_id is not None:
            _require_text("parent_job_id", self.parent_job_id)
            if self.parent_job_id == self.job_id:
                raise ValidationError("a job cannot be its own parent")
        if self.kind is JobKind.VERIFY_PAIR:
            if self.pair is None:
                raise ValidationError("VERIFY_PAIR requires a pair")
            if (
                self.target_claim_id is not None
                or self.target_chunk_version_id is not None
            ):
                raise ValidationError("VERIFY_PAIR must use pair, not target fields")
        elif self.kind is JobKind.IMPACT_DISCOVERY:
            if self.pair is not None or self.target_claim_id is not None:
                raise ValidationError("IMPACT_DISCOVERY is chunk-scoped only")
            if self.target_chunk_version_id is None:
                raise ValidationError("IMPACT_DISCOVERY requires a chunk target")
            _require_text("target_chunk_version_id", self.target_chunk_version_id)
        else:
            if self.pair is not None or self.target_chunk_version_id is not None:
                raise ValidationError("FRONTIER_RETRIEVE is claim-scoped only")
            if self.target_claim_id is None:
                raise ValidationError("FRONTIER_RETRIEVE requires a claim target")
            _require_text("target_claim_id", self.target_claim_id)
        expected_expandable = self.kind in {
            JobKind.IMPACT_DISCOVERY,
            JobKind.FRONTIER_RETRIEVE,
        }
        if self.expandable != expected_expandable:
            raise ValidationError("expandable flag conflicts with job kind")
        claim_id = self.pair.claim_id if self.pair is not None else self.target_claim_id
        chunk_id = (
            self.pair.chunk_version_id
            if self.pair is not None
            else self.target_chunk_version_id
        )
        expected_job_id = self.derive_job_id(
            event_id=self.event_id,
            kind=self.kind,
            candidate_policy_id=self.candidate_policy_id,
            execution_spec_hash=self.execution_spec_hash,
            parent_job_id=self.parent_job_id or "",
            claim_id=claim_id or "",
            chunk_version_id=chunk_id or "",
        )
        if self.job_id != expected_job_id:
            raise ValidationError("job_id does not match persisted logical targets")

    @staticmethod
    def derive_job_id(
        *,
        event_id: str,
        kind: JobKind,
        candidate_policy_id: str,
        execution_spec_hash: str,
        parent_job_id: str = "",
        claim_id: str = "",
        chunk_version_id: str = "",
    ) -> str:
        return stable_m4_digest(
            "m4-logical-job-v1",
            event_id,
            kind.value,
            candidate_policy_id,
            execution_spec_hash,
            parent_job_id,
            claim_id,
            chunk_version_id,
        )


@dataclass(frozen=True, slots=True)
class ChildClosure:
    parent_job_id: str
    completion_digest: str
    child_job_ids: tuple[str, ...]
    child_set_hash: str

    def __post_init__(self) -> None:
        _require_text("parent_job_id", self.parent_job_id)
        _require_sha256("completion_digest", self.completion_digest)
        _require_sha256("child_set_hash", self.child_set_hash)
        _require_sorted_unique("child_job_ids", self.child_job_ids)
        expected = stable_m4_digest("m4-child-set-v1", *self.child_job_ids)
        if self.child_set_hash != expected:
            raise ValidationError("child_set_hash does not match child_job_ids")

    @classmethod
    def build(
        cls,
        *,
        parent_job_id: str,
        result_artifact_hash: str,
        child_job_ids: tuple[str, ...],
    ) -> ChildClosure:
        canonical = tuple(sorted(set(child_job_ids)))
        child_set_hash = stable_m4_digest("m4-child-set-v1", *canonical)
        completion_digest = stable_m4_digest(
            "m4-expandable-completion-v1",
            parent_job_id,
            result_artifact_hash,
            child_set_hash,
        )
        return cls(
            parent_job_id=parent_job_id,
            completion_digest=completion_digest,
            child_job_ids=canonical,
            child_set_hash=child_set_hash,
        )


@dataclass(frozen=True, slots=True)
class JobAttempt:
    attempt_id: str
    job_id: str
    execution_spec_hash: str
    attempt_ordinal: int
    lease_token_hash: str

    def __post_init__(self) -> None:
        _require_text("attempt_id", self.attempt_id)
        _require_text("job_id", self.job_id)
        _require_sha256("execution_spec_hash", self.execution_spec_hash)
        _require_sha256("lease_token_hash", self.lease_token_hash)
        if self.attempt_ordinal <= 0:
            raise ValidationError("attempt_ordinal must be positive")


@dataclass(frozen=True, slots=True)
class JobCompletion:
    job_id: str
    payload_hash: str
    execution_spec_hash: str
    result_artifact_id: str
    result_artifact_hash: str
    terminal_state: JobState
    completion_digest: str
    child_closure: ChildClosure | None = None

    def __post_init__(self) -> None:
        _require_text("job_id", self.job_id)
        _require_text("result_artifact_id", self.result_artifact_id)
        for name, value in (
            ("payload_hash", self.payload_hash),
            ("execution_spec_hash", self.execution_spec_hash),
            ("result_artifact_hash", self.result_artifact_hash),
            ("completion_digest", self.completion_digest),
        ):
            _require_sha256(name, value)
        if self.terminal_state not in {
            JobState.COMPLETED_ACTIVE,
            JobState.COMPLETED_INACTIVE,
        }:
            raise ValidationError("successful completion requires a completed state")
        if (
            self.child_closure is not None
            and self.child_closure.parent_job_id != self.job_id
        ):
            raise ValidationError("child closure belongs to a different parent")
        closure_hash = (
            self.child_closure.child_set_hash if self.child_closure is not None else ""
        )
        expected = stable_m4_digest(
            "m4-job-completion-v1",
            self.job_id,
            self.payload_hash,
            self.execution_spec_hash,
            self.result_artifact_id,
            self.result_artifact_hash,
            self.terminal_state.value,
            closure_hash,
        )
        if self.completion_digest != expected:
            raise ValidationError("completion_digest does not match completion payload")

    @classmethod
    def build(
        cls,
        *,
        job_id: str,
        payload_hash: str,
        execution_spec_hash: str,
        result_artifact_id: str,
        result_artifact_hash: str,
        terminal_state: JobState,
        child_closure: ChildClosure | None = None,
    ) -> JobCompletion:
        closure_hash = child_closure.child_set_hash if child_closure is not None else ""
        completion_digest = stable_m4_digest(
            "m4-job-completion-v1",
            job_id,
            payload_hash,
            execution_spec_hash,
            result_artifact_id,
            result_artifact_hash,
            terminal_state.value,
            closure_hash,
        )
        return cls(
            job_id=job_id,
            payload_hash=payload_hash,
            execution_spec_hash=execution_spec_hash,
            result_artifact_id=result_artifact_id,
            result_artifact_hash=result_artifact_hash,
            terminal_state=terminal_state,
            completion_digest=completion_digest,
            child_closure=child_closure,
        )


@dataclass(frozen=True, slots=True)
class DiscoveryScope:
    root_job_id: str
    registry_snapshot_id: str
    registered_claim_ids: tuple[str, ...]
    closed: bool = False

    def __post_init__(self) -> None:
        _require_text("root_job_id", self.root_job_id)
        _require_text("registry_snapshot_id", self.registry_snapshot_id)
        _require_sorted_unique("registered_claim_ids", self.registered_claim_ids)

    def contains(self, claim_id: str) -> bool:
        return not self.closed and claim_id in self.registered_claim_ids


@dataclass(frozen=True, slots=True)
class FrontierEntry:
    claim_id: str
    chunk_version_id: str
    candidate_policy_id: str
    state: FrontierState
    rank: int
    retrieval_score: float
    candidate_artifact_hash: str

    def __post_init__(self) -> None:
        for name, value in (
            ("claim_id", self.claim_id),
            ("chunk_version_id", self.chunk_version_id),
            ("candidate_policy_id", self.candidate_policy_id),
        ):
            _require_text(name, value)
        if self.rank <= 0:
            raise ValidationError("frontier rank must be positive")
        if not math.isfinite(self.retrieval_score):
            raise ValidationError("frontier retrieval_score must be finite")
        _require_sha256("candidate_artifact_hash", self.candidate_artifact_hash)


@dataclass(frozen=True, slots=True)
class PairJudgment:
    pair: PairKey
    source_kind: JudgmentSourceKind
    source_artifact_id: str
    decision_policy_or_guideline_id: str
    derived_label: VerificationLabel
    input_hash: str
    split_id: str
    support_score: float | None = None
    refute_score: float | None = None
    neutral_score: float | None = None

    def __post_init__(self) -> None:
        for name, value in (
            ("source_artifact_id", self.source_artifact_id),
            (
                "decision_policy_or_guideline_id",
                self.decision_policy_or_guideline_id,
            ),
            ("split_id", self.split_id),
        ):
            _require_text(name, value)
        _require_sha256("input_hash", self.input_hash)
        scores = (self.support_score, self.refute_score, self.neutral_score)
        if self.source_kind is JudgmentSourceKind.MODEL:
            if any(score is None for score in scores):
                raise ValidationError("model judgment requires all three scores")
            numeric_scores = tuple(
                float(score) for score in scores if score is not None
            )
            if any(
                not math.isfinite(score) or not 0.0 <= score <= 1.0
                for score in numeric_scores
            ):
                raise ValidationError("model judgment scores must be finite in [0,1]")
            if not math.isclose(sum(numeric_scores), 1.0, abs_tol=1e-6):
                raise ValidationError("model judgment scores must sum to one")
        elif any(score is not None for score in scores):
            raise ValidationError("human judgment must not masquerade as model scores")


@dataclass(frozen=True, slots=True)
class ImpactTrainingExample:
    pair: PairKey
    target_kind: ImpactTargetKind
    target_source_id: str
    split_id: str
    split_component_id: str
    mining_policy_id: str
    nonneutral: bool

    def __post_init__(self) -> None:
        for name, value in (
            ("target_source_id", self.target_source_id),
            ("split_id", self.split_id),
            ("split_component_id", self.split_component_id),
            ("mining_policy_id", self.mining_policy_id),
        ):
            _require_text(name, value)


@dataclass(frozen=True, slots=True)
class FullPairAuditResult:
    event_id: str
    inserted_active_chunk_ids: tuple[str, ...]
    registered_claim_ids: tuple[str, ...]
    judgments: tuple[PairJudgment, ...]
    manifest_id: str

    def __post_init__(self) -> None:
        _require_text("event_id", self.event_id)
        _require_text("manifest_id", self.manifest_id)
        _require_sorted_unique(
            "inserted_active_chunk_ids", self.inserted_active_chunk_ids
        )
        _require_sorted_unique("registered_claim_ids", self.registered_claim_ids)
        expected = {
            PairKey(claim_id, chunk_id)
            for claim_id in self.registered_claim_ids
            for chunk_id in self.inserted_active_chunk_ids
        }
        actual = {judgment.pair for judgment in self.judgments}
        if len(actual) != len(self.judgments):
            raise ValidationError("full-pair audit contains duplicate pair judgments")
        if actual != expected:
            raise ValidationError("full-pair audit must cover the exact Cartesian set")

    @property
    def expected_pair_count(self) -> int:
        return len(self.registered_claim_ids) * len(self.inserted_active_chunk_ids)

    @property
    def positive_pairs(self) -> tuple[PairKey, ...]:
        return tuple(
            sorted(
                judgment.pair
                for judgment in self.judgments
                if judgment.derived_label
                in {VerificationLabel.SUPPORT, VerificationLabel.REFUTE}
            )
        )


@dataclass(frozen=True, slots=True)
class AffectedSets:
    baseline_id: str
    materialized_state_claim_ids: tuple[str, ...]
    decision_summary_claim_ids: tuple[str, ...]
    status_claim_ids: tuple[str, ...]
    answer_status_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_text("baseline_id", self.baseline_id)
        for name, values in (
            ("materialized_state_claim_ids", self.materialized_state_claim_ids),
            ("decision_summary_claim_ids", self.decision_summary_claim_ids),
            ("status_claim_ids", self.status_claim_ids),
            ("answer_status_ids", self.answer_status_ids),
        ):
            _require_sorted_unique(name, values)
        materialized = set(self.materialized_state_claim_ids)
        summary = set(self.decision_summary_claim_ids)
        status = set(self.status_claim_ids)
        if not status <= summary <= materialized:
            raise ValidationError(
                "same-baseline status/summary/materialized containment failed"
            )


@dataclass(frozen=True, slots=True)
class SnapshotRefreshResult:
    corpus_snapshot_hash: str
    refresh_policy_id: str
    depth_k: int
    retrieved_pairs: tuple[PairKey, ...]
    judgments: tuple[PairJudgment, ...]
    claim_states: tuple[ClaimState, ...]
    answer_states: tuple[AnswerState, ...]
    manifest_id: str

    def __post_init__(self) -> None:
        _require_sha256("corpus_snapshot_hash", self.corpus_snapshot_hash)
        _require_text("refresh_policy_id", self.refresh_policy_id)
        _require_text("manifest_id", self.manifest_id)
        if self.depth_k <= 0:
            raise ValidationError("snapshot refresh depth must be positive")
        if self.retrieved_pairs != tuple(sorted(set(self.retrieved_pairs))):
            raise ValidationError("retrieved_pairs must be sorted and unique")
        judgment_pairs = tuple(sorted(judgment.pair for judgment in self.judgments))
        if judgment_pairs != self.retrieved_pairs:
            raise ValidationError(
                "refresh judgments must match retrieved pairs exactly"
            )
        claim_ids = tuple(state.claim_id for state in self.claim_states)
        answer_ids = tuple(state.answer_version_id for state in self.answer_states)
        _require_sorted_unique("refresh claim state IDs", claim_ids)
        _require_sorted_unique("refresh answer state IDs", answer_ids)


@dataclass(frozen=True, slots=True)
class EventComparisonRecord:
    event_id: str
    treatment_manifest_id: str
    baseline_manifest_id: str
    grounding_outcome: ComparisonOutcome
    coordination_outcome: ComparisonOutcome
    completeness_outcome: ComparisonOutcome
    positive_pair_numerator: int
    positive_pair_denominator: int
    status_effect_numerator: int
    status_effect_denominator: int
    admitted_pair_count: int
    verifier_call_count: int
    failure_code: str | None = None

    def __post_init__(self) -> None:
        for name, value in (
            ("event_id", self.event_id),
            ("treatment_manifest_id", self.treatment_manifest_id),
            ("baseline_manifest_id", self.baseline_manifest_id),
        ):
            _require_text(name, value)
        counts = (
            self.positive_pair_numerator,
            self.positive_pair_denominator,
            self.status_effect_numerator,
            self.status_effect_denominator,
            self.admitted_pair_count,
            self.verifier_call_count,
        )
        if any(count < 0 for count in counts):
            raise ValidationError("event comparison counts must be nonnegative")
        if self.positive_pair_numerator > self.positive_pair_denominator:
            raise ValidationError("positive-pair numerator exceeds denominator")
        if self.status_effect_numerator > self.status_effect_denominator:
            raise ValidationError("status-effect numerator exceeds denominator")
        if self.failure_code is not None:
            _require_text("failure_code", self.failure_code)
