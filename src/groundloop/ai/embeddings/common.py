"""Shared validation and provenance for M3 embeddings."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass

from groundloop.errors import ValidationError

EMBEDDING_DIMENSION = 384


@dataclass(frozen=True, slots=True)
class EmbeddingInputAudit:
    input_id: str
    token_count: int | None
    max_tokens: int
    truncated: bool


@dataclass(frozen=True, slots=True)
class EmbeddedQuery:
    vector: tuple[float, ...]
    input_hash: str
    audit: EmbeddingInputAudit


def input_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def normalized_vector(values: tuple[float, ...]) -> tuple[float, ...]:
    if len(values) != EMBEDDING_DIMENSION:
        raise ValidationError(
            f"embedding dimension must be {EMBEDDING_DIMENSION}, got {len(values)}"
        )
    if any(not math.isfinite(value) for value in values):
        raise ValidationError("embedding vector values must be finite")
    norm = math.sqrt(sum(value * value for value in values))
    if not math.isfinite(norm) or norm == 0.0:
        raise ValidationError("embedding vector must have a finite nonzero norm")
    normalized = tuple(value / norm for value in values)
    check = math.sqrt(sum(value * value for value in normalized))
    if not math.isclose(check, 1.0, rel_tol=1e-6, abs_tol=1e-6):
        raise ValidationError("embedding vector must be L2-normalized")
    return normalized


class ArtifactUnavailableError(RuntimeError):
    """A pinned real-model artifact is absent from the local cache."""
