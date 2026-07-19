"""Exact reverse-vector reference and ANN measurement hooks for M4 admission."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from groundloop.errors import ValidationError
from groundloop.m4.contracts import (
    AdmissionChannel,
    ChannelHit,
    PairKey,
    stable_m4_digest,
)

_HEX = frozenset("0123456789abcdef")


def _validate_sha256(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in _HEX for character in value):
        raise ValidationError(f"{name} must be lowercase SHA-256")


def _validate_unit_vector(name: str, vector: tuple[float, ...]) -> None:
    if not vector:
        raise ValidationError(f"{name} vector must be non-empty")
    if any(not math.isfinite(value) for value in vector):
        raise ValidationError(f"{name} vector values must be finite")
    norm = math.sqrt(sum(value * value for value in vector))
    if not math.isclose(norm, 1.0, rel_tol=1e-6, abs_tol=1e-6):
        raise ValidationError(f"{name} vector must be L2-normalized")


@dataclass(frozen=True, slots=True)
class ClaimRoleVector:
    """A prefixed-claim embedding in the frozen BGE query role."""

    claim_id: str
    vector: tuple[float, ...]
    input_hash: str

    def __post_init__(self) -> None:
        if not self.claim_id.strip():
            raise ValidationError("claim_id must be non-empty")
        _validate_unit_vector("claim", self.vector)
        _validate_sha256("claim input_hash", self.input_hash)


@dataclass(frozen=True, slots=True)
class ChunkRoleVector:
    """An unprefixed inserted-chunk embedding in the BGE passage role."""

    chunk_version_id: str
    vector: tuple[float, ...]
    input_hash: str

    def __post_init__(self) -> None:
        if not self.chunk_version_id.strip():
            raise ValidationError("chunk_version_id must be non-empty")
        _validate_unit_vector("chunk", self.vector)
        _validate_sha256("chunk input_hash", self.input_hash)


@dataclass(frozen=True, slots=True)
class ReverseVectorSearch:
    """One persisted-ready exact or approximate reverse-vector result."""

    hits: tuple[ChannelHit, ...]
    query_artifact_hash: str


class ApproximateReverseVectorIndex(Protocol):
    """Adapter boundary for HNSW or another explicitly empirical index."""

    def search(
        self,
        *,
        epoch_id: int,
        candidate_policy_id: str,
        chunk: ChunkRoleVector,
        limit: int,
    ) -> ReverseVectorSearch: ...


@dataclass(frozen=True, slots=True)
class AnnRecallMeasurement:
    """Recall hook comparing an approximate index with exact pair scores."""

    limit: int
    exact_pair_count: int
    approximate_pair_count: int
    intersection_count: int
    recall_at_limit: float | None

    def __post_init__(self) -> None:
        if self.limit <= 0:
            raise ValidationError("measurement limit must be positive")
        counts = (
            self.exact_pair_count,
            self.approximate_pair_count,
            self.intersection_count,
        )
        if any(count < 0 for count in counts):
            raise ValidationError("ANN measurement counts must be nonnegative")
        if self.intersection_count > min(
            self.exact_pair_count, self.approximate_pair_count
        ):
            raise ValidationError("ANN intersection exceeds measured result sets")
        expected = (
            None
            if self.exact_pair_count == 0
            else self.intersection_count / self.exact_pair_count
        )
        if self.recall_at_limit != expected:
            raise ValidationError("ANN recall does not match measured counts")


class ExactReverseVectorIndex:
    """Brute-force role-specific dot-product reference over registered claims."""

    def __init__(self, claims: Sequence[ClaimRoleVector]) -> None:
        materialized = tuple(claims)
        claim_ids = tuple(item.claim_id for item in materialized)
        if len(set(claim_ids)) != len(claim_ids):
            raise ValidationError("exact claim index contains duplicate claim IDs")
        dimensions = {len(item.vector) for item in materialized}
        if len(dimensions) > 1:
            raise ValidationError("claim vectors must have one dimension")
        self._claims = tuple(sorted(materialized, key=lambda item: item.claim_id))
        self._dimension = next(iter(dimensions), None)

    @property
    def claim_count(self) -> int:
        return len(self._claims)

    def search(
        self,
        *,
        epoch_id: int,
        candidate_policy_id: str,
        chunk: ChunkRoleVector,
        limit: int,
    ) -> ReverseVectorSearch:
        if epoch_id <= 0:
            raise ValidationError("epoch_id must be positive")
        if not candidate_policy_id.strip():
            raise ValidationError("candidate_policy_id must be non-empty")
        if limit <= 0:
            raise ValidationError("reverse-vector limit must be positive")
        if self._dimension is not None and len(chunk.vector) != self._dimension:
            raise ValidationError("claim and chunk vector dimensions differ")

        scored: list[tuple[float, str, float, ClaimRoleVector]] = []
        for claim in self._claims:
            similarity = sum(
                left * right
                for left, right in zip(claim.vector, chunk.vector, strict=True)
            )
            similarity = min(1.0, max(-1.0, similarity))
            distance = 1.0 - similarity
            scored.append((distance, claim.claim_id, similarity, claim))
        scored.sort(key=lambda item: (item[0], item[1]))
        selected = scored[:limit]
        query_artifact_hash = stable_m4_digest(
            "m4-exact-reverse-vector-query-v1",
            candidate_policy_id,
            chunk.chunk_version_id,
            chunk.input_hash,
            str(limit),
            *(
                value
                for _, claim_id, _, claim in scored
                for value in (claim_id, claim.input_hash)
            ),
        )
        hits = tuple(
            ChannelHit(
                epoch_id=epoch_id,
                pair=PairKey(claim_id, chunk.chunk_version_id),
                candidate_policy_id=candidate_policy_id,
                channel=AdmissionChannel.VECTOR,
                rank=rank,
                score=similarity,
                channel_artifact_hash=stable_m4_digest(
                    "m4-vector-channel-hit-v1",
                    query_artifact_hash,
                    claim_id,
                    format(distance, ".17g"),
                    format(similarity, ".17g"),
                    str(rank),
                ),
            )
            for rank, (distance, claim_id, similarity, _claim) in enumerate(
                selected, start=1
            )
        )
        return ReverseVectorSearch(hits, query_artifact_hash)


def measure_ann_recall(
    exact: ReverseVectorSearch,
    approximate: ReverseVectorSearch,
    *,
    limit: int,
) -> AnnRecallMeasurement:
    """Measure pair-set recall; this hook makes no ANN exactness claim."""
    if limit <= 0:
        raise ValidationError("measurement limit must be positive")
    exact_slice = exact.hits[:limit]
    approximate_slice = approximate.hits[:limit]
    combined = (*exact_slice, *approximate_slice)
    if any(hit.channel is not AdmissionChannel.VECTOR for hit in combined):
        raise ValidationError("ANN recall accepts VECTOR channel hits only")
    universes = {
        (hit.epoch_id, hit.candidate_policy_id, hit.pair.chunk_version_id)
        for hit in combined
    }
    if len(universes) > 1:
        raise ValidationError(
            "ANN recall comparisons must share epoch, policy and chunk"
        )
    exact_pairs = {hit.pair for hit in exact_slice}
    approximate_pairs = {hit.pair for hit in approximate_slice}
    if len(exact_pairs) != len(exact_slice) or len(approximate_pairs) != len(
        approximate_slice
    ):
        raise ValidationError("ANN recall inputs contain duplicate pairs")
    intersection = len(exact_pairs & approximate_pairs)
    recall = None if not exact_pairs else intersection / len(exact_pairs)
    return AnnRecallMeasurement(
        limit=limit,
        exact_pair_count=len(exact_pairs),
        approximate_pair_count=len(approximate_pairs),
        intersection_count=intersection,
        recall_at_limit=recall,
    )
