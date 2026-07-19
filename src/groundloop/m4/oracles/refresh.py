"""Exact brute-force policy-relative snapshot refresh primitives."""

from __future__ import annotations

import math
from dataclasses import dataclass

from groundloop.errors import ValidationError
from groundloop.m4.contracts import (
    PairJudgment,
    PairKey,
    SnapshotRefreshResult,
    stable_m4_digest,
)
from groundloop.m4.oracles.grounding import recompute_grounding_states
from groundloop.m4.oracles.identity import judgment_digest
from groundloop.m4.oracles.testing import PairJudge

_HEX = frozenset("0123456789abcdef")


def _validate_vector(name: str, vector: tuple[float, ...]) -> None:
    if not vector:
        raise ValidationError(f"{name} vector must be non-empty")
    if any(not math.isfinite(value) for value in vector):
        raise ValidationError(f"{name} vector must contain only finite values")
    norm = math.sqrt(sum(value * value for value in vector))
    if not math.isclose(norm, 1.0, rel_tol=1e-6, abs_tol=1e-6):
        raise ValidationError(f"{name} vector must be L2-normalized")


@dataclass(frozen=True, slots=True)
class RefreshClaim:
    claim_id: str
    answer_version_id: str
    required: bool
    vector: tuple[float, ...]

    def __post_init__(self) -> None:
        if not self.claim_id.strip() or not self.answer_version_id.strip():
            raise ValidationError("refresh claim identities must be non-empty")
        _validate_vector("claim", self.vector)


@dataclass(frozen=True, slots=True)
class RefreshChunk:
    chunk_version_id: str
    text_hash: str
    vector: tuple[float, ...]

    def __post_init__(self) -> None:
        if not self.chunk_version_id.strip():
            raise ValidationError("refresh chunk identity must be non-empty")
        invalid_hash = len(self.text_hash) != 64 or any(
            value not in _HEX for value in self.text_hash
        )
        if invalid_hash:
            raise ValidationError("refresh chunk text_hash must be lowercase SHA-256")
        _validate_vector("chunk", self.vector)


def _canonical_claims(records: tuple[RefreshClaim, ...]) -> tuple[RefreshClaim, ...]:
    by_id: dict[str, RefreshClaim] = {}
    for record in records:
        if record.claim_id in by_id:
            raise ValidationError(f"claims contains duplicate ID {record.claim_id}")
        by_id[record.claim_id] = record
    return tuple(by_id[key] for key in sorted(by_id))


def _canonical_chunks(records: tuple[RefreshChunk, ...]) -> tuple[RefreshChunk, ...]:
    by_id: dict[str, RefreshChunk] = {}
    for record in records:
        if record.chunk_version_id in by_id:
            raise ValidationError(
                f"active_chunks contains duplicate ID {record.chunk_version_id}"
            )
        by_id[record.chunk_version_id] = record
    return tuple(by_id[key] for key in sorted(by_id))


def _dot(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    if len(left) != len(right):
        raise ValidationError("claim and chunk embedding dimensions differ")
    return sum(a * b for a, b in zip(left, right, strict=True))


def exact_brute_force_pairs(
    *,
    claims: tuple[RefreshClaim, ...],
    active_chunks: tuple[RefreshChunk, ...],
    depth_k: int,
) -> tuple[PairKey, ...]:
    """Return exact per-claim cosine top-k with `(distance, chunk_id)` ties."""
    if depth_k <= 0:
        raise ValidationError("snapshot refresh depth must be positive")
    ordered_claims = _canonical_claims(claims)
    ordered_chunks = _canonical_chunks(active_chunks)
    selected: list[PairKey] = []
    for claim in ordered_claims:
        ranked = sorted(
            ordered_chunks,
            key=lambda chunk: (
                1.0 - _dot(claim.vector, chunk.vector),
                chunk.chunk_version_id,
            ),
        )
        selected.extend(
            PairKey(claim.claim_id, chunk.chunk_version_id)
            for chunk in ranked[:depth_k]
        )
    return tuple(sorted(selected))


def _refresh_manifest_id(
    *,
    corpus_snapshot_hash: str,
    refresh_policy_id: str,
    depth_k: int,
    pairs: tuple[PairKey, ...],
    judgments: tuple[PairJudgment, ...],
) -> str:
    parts = [
        "m4-snapshot-refresh-v1",
        corpus_snapshot_hash,
        refresh_policy_id,
        str(depth_k),
    ]
    for pair, judgment in zip(pairs, judgments, strict=True):
        if pair != judgment.pair:
            raise ValidationError("refresh manifest received mismatched pair identity")
        parts.append(judgment_digest(judgment))
    return "snapshot-refresh-" + stable_m4_digest(*parts)


def run_snapshot_refresh(
    *,
    corpus_snapshot_hash: str,
    refresh_policy_id: str,
    depth_k: int,
    claims: tuple[RefreshClaim, ...],
    active_chunks: tuple[RefreshChunk, ...],
    judge: PairJudge,
) -> SnapshotRefreshResult:
    """Retrieve, judge, and recompute a fresh bounded snapshot independently."""
    ordered_claims = _canonical_claims(claims)
    ordered_chunks = _canonical_chunks(active_chunks)
    pairs = exact_brute_force_pairs(
        claims=ordered_claims,
        active_chunks=ordered_chunks,
        depth_k=depth_k,
    )
    judgments = judge.judge_pairs(pairs)
    if tuple(judgment.pair for judgment in judgments) != pairs:
        raise ValidationError("refresh judge changed pair identity or order")
    claim_states, answer_states = recompute_grounding_states(
        claim_to_answer={
            claim.claim_id: claim.answer_version_id for claim in ordered_claims
        },
        required_claim_ids=frozenset(
            claim.claim_id for claim in ordered_claims if claim.required
        ),
        chunk_text_hashes={
            chunk.chunk_version_id: chunk.text_hash for chunk in ordered_chunks
        },
        judgments=judgments,
    )
    return SnapshotRefreshResult(
        corpus_snapshot_hash=corpus_snapshot_hash,
        refresh_policy_id=refresh_policy_id,
        depth_k=depth_k,
        retrieved_pairs=pairs,
        judgments=judgments,
        claim_states=claim_states,
        answer_states=answer_states,
        manifest_id=_refresh_manifest_id(
            corpus_snapshot_hash=corpus_snapshot_hash,
            refresh_policy_id=refresh_policy_id,
            depth_k=depth_k,
            pairs=pairs,
            judgments=judgments,
        ),
    )
