"""Deterministic fake reverse-vector adapter for ordinary Wave 1 tests."""

from __future__ import annotations

from collections.abc import Mapping

from groundloop.errors import ValidationError
from groundloop.m4.admission.vector import (
    ApproximateReverseVectorIndex,
    ChunkRoleVector,
    ReverseVectorSearch,
)
from groundloop.m4.contracts import (
    AdmissionChannel,
    ChannelHit,
    PairKey,
    stable_m4_digest,
)


class DeterministicFakeApproximateIndex(ApproximateReverseVectorIndex):
    """Return a frozen empirical ranking without pretending to be ANN."""

    def __init__(self, rankings: Mapping[str, tuple[tuple[str, float], ...]]) -> None:
        self._rankings = dict(rankings)
        for chunk_id, ranking in self._rankings.items():
            if not chunk_id.strip():
                raise ValidationError("fake index chunk ID must be non-empty")
            claim_ids = tuple(claim_id for claim_id, _score in ranking)
            if len(set(claim_ids)) != len(claim_ids):
                raise ValidationError("fake ANN ranking contains duplicate claims")

    def search(
        self,
        *,
        epoch_id: int,
        candidate_policy_id: str,
        chunk: ChunkRoleVector,
        limit: int,
    ) -> ReverseVectorSearch:
        if limit <= 0:
            raise ValidationError("fake ANN limit must be positive")
        ranking = self._rankings.get(chunk.chunk_version_id, ())[:limit]
        artifact_hash = stable_m4_digest(
            "m4-fake-ann-query-v1",
            candidate_policy_id,
            chunk.chunk_version_id,
            chunk.input_hash,
            str(limit),
            *(
                value
                for claim_id, score in ranking
                for value in (claim_id, repr(score))
            ),
        )
        hits = tuple(
            ChannelHit(
                epoch_id=epoch_id,
                pair=PairKey(claim_id, chunk.chunk_version_id),
                candidate_policy_id=candidate_policy_id,
                channel=AdmissionChannel.VECTOR,
                rank=rank,
                score=score,
                channel_artifact_hash=stable_m4_digest(
                    "m4-fake-ann-hit-v1", artifact_hash, claim_id, str(rank)
                ),
            )
            for rank, (claim_id, score) in enumerate(ranking, start=1)
        )
        return ReverseVectorSearch(hits, artifact_hash)
