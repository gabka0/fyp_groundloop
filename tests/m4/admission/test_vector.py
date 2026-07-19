from __future__ import annotations

import math

import pytest

from groundloop.errors import ValidationError
from groundloop.m4.admission import (
    ChunkRoleVector,
    ClaimRoleVector,
    DeterministicFakeApproximateIndex,
    ExactReverseVectorIndex,
    measure_ann_recall,
)
from groundloop.m4.contracts import AdmissionChannel

HASH = "a" * 64


def test_exact_reverse_vector_orders_distance_then_claim_id() -> None:
    index = ExactReverseVectorIndex(
        (
            ClaimRoleVector("claim-b", (1.0, 0.0), HASH),
            ClaimRoleVector("claim-a", (1.0, 0.0), HASH),
            ClaimRoleVector("claim-c", (0.0, 1.0), HASH),
        )
    )
    chunk = ChunkRoleVector("chunk-1", (1.0, 0.0), HASH)

    first = index.search(
        epoch_id=1,
        candidate_policy_id="policy-1",
        chunk=chunk,
        limit=3,
    )
    replay = index.search(
        epoch_id=1,
        candidate_policy_id="policy-1",
        chunk=chunk,
        limit=3,
    )

    assert first == replay
    assert tuple(hit.pair.claim_id for hit in first.hits) == (
        "claim-a",
        "claim-b",
        "claim-c",
    )
    assert tuple(hit.score for hit in first.hits) == (1.0, 1.0, 0.0)
    assert tuple(hit.rank for hit in first.hits) == (1, 2, 3)


def test_reverse_pair_score_is_the_role_specific_dot_product() -> None:
    root = math.sqrt(0.5)
    claim = ClaimRoleVector("claim", (root, root), HASH)
    chunk = ChunkRoleVector("chunk", (0.0, 1.0), HASH)
    result = ExactReverseVectorIndex((claim,)).search(
        epoch_id=2,
        candidate_policy_id="policy",
        chunk=chunk,
        limit=1,
    )
    assert result.hits[0].score == pytest.approx(root)


@pytest.mark.parametrize(
    "vector",
    [(), (0.0, 0.0), (float("nan"), 0.0), (2.0, 0.0)],
)
def test_role_vectors_reject_invalid_normalization(vector: tuple[float, ...]) -> None:
    with pytest.raises(ValidationError):
        ClaimRoleVector("claim", vector, HASH)


def test_exact_index_rejects_duplicate_claims_and_dimension_mismatch() -> None:
    claim = ClaimRoleVector("claim", (1.0, 0.0), HASH)
    with pytest.raises(ValidationError, match="duplicate"):
        ExactReverseVectorIndex((claim, claim))

    index = ExactReverseVectorIndex((claim,))
    with pytest.raises(ValidationError, match="dimensions"):
        index.search(
            epoch_id=1,
            candidate_policy_id="policy",
            chunk=ChunkRoleVector("chunk", (1.0, 0.0, 0.0), HASH),
            limit=1,
        )


def test_fake_ann_hook_measures_recall_without_claiming_exactness() -> None:
    chunk = ChunkRoleVector("chunk", (1.0, 0.0), HASH)
    exact = ExactReverseVectorIndex(
        (
            ClaimRoleVector("claim-a", (1.0, 0.0), HASH),
            ClaimRoleVector("claim-b", (0.8, 0.6), HASH),
            ClaimRoleVector("claim-c", (0.0, 1.0), HASH),
        )
    ).search(
        epoch_id=1,
        candidate_policy_id="policy",
        chunk=chunk,
        limit=2,
    )
    approximate = DeterministicFakeApproximateIndex(
        {"chunk": (("claim-a", 1.0), ("claim-c", 0.0))}
    ).search(
        epoch_id=1,
        candidate_policy_id="policy",
        chunk=chunk,
        limit=2,
    )

    measurement = measure_ann_recall(exact, approximate, limit=2)
    assert measurement.exact_pair_count == 2
    assert measurement.approximate_pair_count == 2
    assert measurement.intersection_count == 1
    assert measurement.recall_at_limit == 0.5


def test_empty_exact_index_has_not_applicable_recall() -> None:
    chunk = ChunkRoleVector("chunk", (1.0, 0.0), HASH)
    exact = ExactReverseVectorIndex(()).search(
        epoch_id=1,
        candidate_policy_id="policy",
        chunk=chunk,
        limit=2,
    )
    approximate = DeterministicFakeApproximateIndex({}).search(
        epoch_id=1,
        candidate_policy_id="policy",
        chunk=chunk,
        limit=2,
    )
    assert measure_ann_recall(exact, approximate, limit=2).recall_at_limit is None


def test_ann_measurement_rejects_different_query_universes() -> None:
    first_chunk = ChunkRoleVector("chunk-a", (1.0, 0.0), HASH)
    second_chunk = ChunkRoleVector("chunk-b", (1.0, 0.0), HASH)
    index = DeterministicFakeApproximateIndex(
        {
            "chunk-a": (("claim", 1.0),),
            "chunk-b": (("claim", 1.0),),
        }
    )
    first = index.search(
        epoch_id=1,
        candidate_policy_id="policy",
        chunk=first_chunk,
        limit=1,
    )
    second = index.search(
        epoch_id=1,
        candidate_policy_id="policy",
        chunk=second_chunk,
        limit=1,
    )
    assert first.hits[0].channel is AdmissionChannel.VECTOR
    with pytest.raises(ValidationError, match="share epoch, policy and chunk"):
        measure_ann_recall(first, second, limit=1)
