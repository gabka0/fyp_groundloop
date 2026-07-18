"""Injected static cosine-search stores, including the pgvector boundary."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Protocol

from psycopg import Connection

from groundloop.ai.contracts import EmbeddingRecord
from groundloop.ai.embeddings.common import normalized_vector
from groundloop.errors import ArtifactConflictError, ValidationError


@dataclass(frozen=True, slots=True)
class StoredEmbeddingHit:
    chunk_version_id: str
    distance: float

    def __post_init__(self) -> None:
        if not self.chunk_version_id.strip():
            raise ValidationError("retrieval hit chunk_version_id must be non-empty")
        if not math.isfinite(self.distance):
            raise ValidationError("retrieval distance must be finite")


class EmbeddingStore(Protocol):
    def search_cosine(
        self,
        *,
        vector: tuple[float, ...],
        model_artifact_id: str,
        limit: int,
    ) -> tuple[StoredEmbeddingHit, ...]: ...


@dataclass(slots=True)
class InMemoryCosineStore:
    """Exact offline cosine store used by unit tests and fixture evaluation."""

    _records: dict[tuple[str, str], EmbeddingRecord] = field(default_factory=dict)

    def add(self, records: tuple[EmbeddingRecord, ...]) -> None:
        for record in records:
            key = (record.chunk_version_id, record.model_artifact_id)
            existing = self._records.get(key)
            if existing is not None and existing != record:
                raise ArtifactConflictError(
                    f"embedding {record.chunk_version_id}/{record.model_artifact_id} "
                    "has conflicting content"
                )
            normalized = normalized_vector(record.vector)
            if any(
                not math.isclose(left, right, rel_tol=1e-6, abs_tol=1e-6)
                for left, right in zip(normalized, record.vector, strict=True)
            ):
                raise ValidationError("stored embedding must already be L2-normalized")
            self._records[key] = record

    def search_cosine(
        self,
        *,
        vector: tuple[float, ...],
        model_artifact_id: str,
        limit: int,
    ) -> tuple[StoredEmbeddingHit, ...]:
        query = normalized_vector(vector)
        if limit <= 0:
            raise ValidationError("retrieval limit must be positive")
        hits = [
            StoredEmbeddingHit(
                chunk_version_id=record.chunk_version_id,
                distance=1.0
                - sum(
                    left * right
                    for left, right in zip(query, record.vector, strict=True)
                ),
            )
            for (chunk_id, artifact_id), record in self._records.items()
            if artifact_id == model_artifact_id and chunk_id == record.chunk_version_id
        ]
        hits.sort(key=lambda hit: (hit.distance, hit.chunk_version_id))
        return tuple(hits[:limit])

    @property
    def index_size_bytes(self) -> int:
        return sum(len(record.vector) * 8 for record in self._records.values())


@dataclass(slots=True)
class PgvectorCosineStore:
    """Static pgvector cosine query using the frozen M3 embedding table."""

    connection: Connection[Any]

    def search_cosine(
        self,
        *,
        vector: tuple[float, ...],
        model_artifact_id: str,
        limit: int,
    ) -> tuple[StoredEmbeddingHit, ...]:
        normalized = normalized_vector(vector)
        if limit <= 0:
            raise ValidationError("retrieval limit must be positive")
        vector_literal = (
            "[" + ",".join(format(value, ".17g") for value in normalized) + "]"
        )
        rows = self.connection.execute(
            """
            SELECT chunk_version_id, embedding <=> %s::vector AS distance
            FROM groundloop_chunk_embedding
            WHERE model_artifact_id = %s
            ORDER BY distance, chunk_version_id
            LIMIT %s
            """,
            (vector_literal, model_artifact_id, limit),
        ).fetchall()
        return tuple(
            StoredEmbeddingHit(str(chunk_version_id), float(distance))
            for chunk_version_id, distance in rows
        )
