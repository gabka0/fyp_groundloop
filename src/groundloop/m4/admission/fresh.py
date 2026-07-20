"""Exact PostgreSQL retrieval for mandatory M4 frontier repair.

This component ranks the active chunks for one registered claim from immutable
role-specific embedding artifacts.  It is deliberately a brute-force
pgvector reference path: it makes no ANN or semantic-quality claim.  A result
is complete only when every active chunk has exactly one compatible passage
artifact under the frozen candidate policy.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from psycopg import Connection

from groundloop.errors import EventConflictError, ValidationError
from groundloop.m4.contracts import (
    CandidatePolicyManifest,
    PairKey,
    stable_m4_digest,
)


def _require_text(name: str, value: str) -> None:
    if not value.strip():
        raise ValidationError(f"{name} must be non-empty")


def _hash_value(value: object) -> str:
    """Read fixed-width PostgreSQL hashes without CHAR display padding."""
    return str(value).strip()


@dataclass(frozen=True, slots=True)
class FreshFrontierCandidate:
    """One exact cosine-ranked active-chunk candidate."""

    pair: PairKey
    rank: int
    score: float
    distance: float
    chunk_artifact_id: str
    chunk_input_hash: str
    chunk_vector_hash: str

    def __post_init__(self) -> None:
        if self.rank <= 0:
            raise ValidationError("fresh frontier rank must be positive")
        if not math.isfinite(self.score) or not math.isfinite(self.distance):
            raise ValidationError("fresh frontier scores must be finite")
        for name, value in (
            ("chunk artifact_id", self.chunk_artifact_id),
            ("chunk input_hash", self.chunk_input_hash),
            ("chunk vector_hash", self.chunk_vector_hash),
        ):
            _require_text(name, value)


@dataclass(frozen=True, slots=True)
class FreshFrontierRetrieval:
    """Replay-bound output of one exact claim-to-active-chunk retrieval.

    ``complete`` is artifact completeness, not semantic completeness.  It is
    false when at least one active chunk lacks a compatible passage vector.
    Ambiguous claim or chunk artifacts are rejected instead of represented in
    this result.
    """

    epoch_id: int
    claim_id: str
    candidate_policy_id: str
    candidate_policy_hash: str
    embedding_model_artifact_id: str
    claim_role_template_hash: str
    chunk_role_template_hash: str
    claim_registry_snapshot_id: str
    claim_artifact_id: str
    requested_limit: int
    excluded_chunk_ids: tuple[str, ...]
    active_chunk_count: int
    compatible_chunk_count: int
    missing_active_chunk_ids: tuple[str, ...]
    complete: bool
    candidates: tuple[FreshFrontierCandidate, ...]
    artifact_hash: str

    def __post_init__(self) -> None:
        if self.epoch_id <= 0:
            raise ValidationError("fresh frontier epoch_id must be positive")
        if self.requested_limit <= 0:
            raise ValidationError("fresh frontier limit must be positive")
        for name, value in (
            ("claim_id", self.claim_id),
            ("candidate_policy_id", self.candidate_policy_id),
            ("claim registry snapshot", self.claim_registry_snapshot_id),
            ("claim artifact_id", self.claim_artifact_id),
            ("artifact_hash", self.artifact_hash),
        ):
            _require_text(name, value)
        if self.excluded_chunk_ids != tuple(sorted(set(self.excluded_chunk_ids))):
            raise ValidationError("excluded chunk IDs must be sorted and unique")
        if self.missing_active_chunk_ids != tuple(
            sorted(set(self.missing_active_chunk_ids))
        ):
            raise ValidationError("missing chunk IDs must be sorted and unique")
        if self.active_chunk_count < 0 or self.compatible_chunk_count < 0:
            raise ValidationError("fresh frontier artifact counts must be nonnegative")
        if self.compatible_chunk_count + len(self.missing_active_chunk_ids) != (
            self.active_chunk_count
        ):
            raise ValidationError("fresh frontier artifact counts are inconsistent")
        if self.complete != (not self.missing_active_chunk_ids):
            raise ValidationError("fresh frontier completeness flag is inconsistent")
        expected_ranks = tuple(range(1, len(self.candidates) + 1))
        if tuple(candidate.rank for candidate in self.candidates) != expected_ranks:
            raise ValidationError("fresh frontier candidate ranks are not contiguous")
        if len(self.candidates) > self.requested_limit:
            raise ValidationError("fresh frontier result exceeds requested limit")
        if any(
            candidate.pair.claim_id != self.claim_id
            for candidate in self.candidates
        ):
            raise ValidationError("fresh frontier candidate escaped claim scope")
        candidate_chunks = tuple(
            candidate.pair.chunk_version_id for candidate in self.candidates
        )
        if len(candidate_chunks) != len(set(candidate_chunks)):
            raise ValidationError("fresh frontier candidates contain duplicates")
        if set(candidate_chunks) & set(self.excluded_chunk_ids):
            raise ValidationError("fresh frontier result contains an excluded chunk")


class FreshFrontierRetriever(Protocol):
    """Typed service boundary for mandatory fresh frontier retrieval."""

    def retrieve(
        self,
        *,
        epoch_id: int,
        claim_id: str,
        manifest: CandidatePolicyManifest,
        excluded_chunk_ids: Sequence[str] = (),
        limit: int,
    ) -> FreshFrontierRetrieval: ...


@dataclass(slots=True)
class PostgresExactFreshFrontierRetriever:
    """Brute-force exact pgvector retrieval over an epoch's active chunks."""

    connection: Connection[Any]

    def __post_init__(self) -> None:
        if not self.connection.autocommit:
            raise ValidationError(
                "fresh frontier retrieval requires an autocommit connection"
            )

    def retrieve(
        self,
        *,
        epoch_id: int,
        claim_id: str,
        manifest: CandidatePolicyManifest,
        excluded_chunk_ids: Sequence[str] = (),
        limit: int,
    ) -> FreshFrontierRetrieval:
        """Rank compatible active chunks by exact cosine distance.

        The explicit repeatable-read transaction gives the policy check,
        artifact-completeness audit, and rank query one database snapshot.
        Model inference is not performed by this component.
        """
        if epoch_id <= 0:
            raise ValidationError("fresh frontier epoch_id must be positive")
        _require_text("fresh frontier claim_id", claim_id)
        if limit <= 0:
            raise ValidationError("fresh frontier limit must be positive")
        excluded = tuple(sorted(set(excluded_chunk_ids)))
        if any(not chunk_id.strip() for chunk_id in excluded):
            raise ValidationError("excluded chunk IDs must be non-empty")

        with self.connection.transaction():
            self.connection.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
            self._validate_policy_and_epoch(epoch_id, manifest)
            claim_artifact_id = self._claim_artifact_id(claim_id, manifest)
            artifact_counts = self._active_chunk_artifact_counts(
                epoch_id, manifest
            )
            ambiguous_chunks = tuple(
                chunk_id for chunk_id, count in artifact_counts if count > 1
            )
            if ambiguous_chunks:
                raise ValidationError(
                    "active chunks have ambiguous compatible passage artifacts: "
                    + ", ".join(ambiguous_chunks)
                )
            missing = tuple(
                chunk_id for chunk_id, count in artifact_counts if count == 0
            )
            rows = self.connection.execute(
                """
                WITH scored AS MATERIALIZED (
                    SELECT chunk.chunk_version_id,
                           passage.artifact_id,
                           passage.input_hash,
                           passage.vector_hash,
                           passage.embedding <=> claim.embedding AS distance
                    FROM groundloop_m4_effective_chunk_version AS chunk
                    JOIN groundloop_m4_role_embedding_artifact AS passage
                      ON passage.embedding_role = 'chunk_passage'
                     AND passage.chunk_version_id = chunk.chunk_version_id
                     AND passage.model_artifact_id = %s
                     AND passage.role_template_hash = %s
                    JOIN groundloop_m4_role_embedding_artifact AS claim
                      ON claim.artifact_id = %s
                    WHERE chunk.epoch_id = %s
                      AND NOT (chunk.chunk_version_id = ANY(%s::text[]))
                )
                SELECT chunk_version_id, artifact_id, input_hash, vector_hash,
                       distance
                FROM scored
                ORDER BY distance ASC, chunk_version_id ASC
                LIMIT %s
                """,
                (
                    manifest.embedding_model_artifact_id,
                    manifest.chunk_role_template_hash,
                    claim_artifact_id,
                    epoch_id,
                    list(excluded),
                    limit,
                ),
            ).fetchall()

        candidates = tuple(
            FreshFrontierCandidate(
                pair=PairKey(claim_id, str(row[0])),
                rank=rank,
                score=1.0 - float(row[4]),
                distance=float(row[4]),
                chunk_artifact_id=_hash_value(row[1]),
                chunk_input_hash=_hash_value(row[2]),
                chunk_vector_hash=_hash_value(row[3]),
            )
            for rank, row in enumerate(rows, start=1)
        )
        complete = not missing
        compatible_count = len(artifact_counts) - len(missing)
        artifact_hash = self._artifact_hash(
            epoch_id=epoch_id,
            claim_id=claim_id,
            manifest=manifest,
            claim_artifact_id=claim_artifact_id,
            limit=limit,
            excluded=excluded,
            active_chunk_count=len(artifact_counts),
            compatible_chunk_count=compatible_count,
            missing=missing,
            candidates=candidates,
        )
        return FreshFrontierRetrieval(
            epoch_id=epoch_id,
            claim_id=claim_id,
            candidate_policy_id=manifest.policy_id,
            candidate_policy_hash=manifest.policy_hash,
            embedding_model_artifact_id=manifest.embedding_model_artifact_id,
            claim_role_template_hash=manifest.claim_role_template_hash,
            chunk_role_template_hash=manifest.chunk_role_template_hash,
            claim_registry_snapshot_id=manifest.claim_registry_snapshot_id,
            claim_artifact_id=claim_artifact_id,
            requested_limit=limit,
            excluded_chunk_ids=excluded,
            active_chunk_count=len(artifact_counts),
            compatible_chunk_count=compatible_count,
            missing_active_chunk_ids=missing,
            complete=complete,
            candidates=candidates,
            artifact_hash=artifact_hash,
        )

    def _validate_policy_and_epoch(
        self, epoch_id: int, manifest: CandidatePolicyManifest
    ) -> None:
        row = self.connection.execute(
            """
            SELECT policy_hash, embedding_model_artifact_id,
                   claim_role_template_hash, chunk_role_template_hash
            FROM groundloop_candidate_policy
            WHERE candidate_policy_id = %s
            """,
            (manifest.policy_id,),
        ).fetchone()
        expected = (
            manifest.policy_hash,
            manifest.embedding_model_artifact_id,
            manifest.claim_role_template_hash,
            manifest.chunk_role_template_hash,
        )
        actual = None
        if row is not None:
            actual = (
                _hash_value(row[0]),
                str(row[1]),
                _hash_value(row[2]),
                _hash_value(row[3]),
            )
        if actual != expected:
            raise EventConflictError(
                "fresh frontier candidate policy differs from the registry"
            )
        update = self.connection.execute(
            """
            SELECT candidate_policy_id FROM groundloop_m4_update
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        if update is None:
            raise ValidationError("fresh frontier epoch is not an M4 update")
        if str(update[0]) != manifest.policy_id:
            raise EventConflictError(
                "fresh frontier epoch uses another candidate policy"
            )

    def _claim_artifact_id(
        self, claim_id: str, manifest: CandidatePolicyManifest
    ) -> str:
        member = self.connection.execute(
            """
            SELECT 1 FROM groundloop_m4_claim_registry_member
            WHERE claim_registry_snapshot_id = %s AND claim_id = %s
            """,
            (manifest.claim_registry_snapshot_id, claim_id),
        ).fetchone()
        if member is None:
            raise ValidationError(
                "fresh frontier claim is outside the policy registry snapshot"
            )
        rows = self.connection.execute(
            """
            SELECT artifact_id
            FROM groundloop_m4_role_embedding_artifact
            WHERE embedding_role = 'claim_query'
              AND claim_id = %s
              AND model_artifact_id = %s
              AND role_template_hash = %s
            ORDER BY artifact_id
            """,
            (
                claim_id,
                manifest.embedding_model_artifact_id,
                manifest.claim_role_template_hash,
            ),
        ).fetchall()
        if not rows:
            raise ValidationError(
                "fresh frontier claim lacks a compatible query artifact"
            )
        if len(rows) != 1:
            raise ValidationError(
                "fresh frontier claim has ambiguous compatible query artifacts"
            )
        return _hash_value(rows[0][0])

    def _active_chunk_artifact_counts(
        self, epoch_id: int, manifest: CandidatePolicyManifest
    ) -> tuple[tuple[str, int], ...]:
        rows = self.connection.execute(
            """
            SELECT chunk.chunk_version_id, count(passage.artifact_id)
            FROM groundloop_m4_effective_chunk_version AS chunk
            LEFT JOIN groundloop_m4_role_embedding_artifact AS passage
              ON passage.embedding_role = 'chunk_passage'
             AND passage.chunk_version_id = chunk.chunk_version_id
             AND passage.model_artifact_id = %s
             AND passage.role_template_hash = %s
            WHERE chunk.epoch_id = %s
            GROUP BY chunk.chunk_version_id
            ORDER BY chunk.chunk_version_id
            """,
            (
                manifest.embedding_model_artifact_id,
                manifest.chunk_role_template_hash,
                epoch_id,
            ),
        ).fetchall()
        return tuple((str(row[0]), int(row[1])) for row in rows)

    @staticmethod
    def _artifact_hash(
        *,
        epoch_id: int,
        claim_id: str,
        manifest: CandidatePolicyManifest,
        claim_artifact_id: str,
        limit: int,
        excluded: tuple[str, ...],
        active_chunk_count: int,
        compatible_chunk_count: int,
        missing: tuple[str, ...],
        candidates: tuple[FreshFrontierCandidate, ...],
    ) -> str:
        parts = [
            "m4-exact-fresh-frontier-retrieval-v1",
            str(epoch_id),
            claim_id,
            manifest.policy_id,
            manifest.policy_hash,
            manifest.embedding_model_artifact_id,
            manifest.claim_role_template_hash,
            manifest.chunk_role_template_hash,
            manifest.claim_registry_snapshot_id,
            claim_artifact_id,
            str(limit),
            str(len(excluded)),
            *excluded,
            str(active_chunk_count),
            str(compatible_chunk_count),
            "complete" if not missing else "incomplete",
            str(len(missing)),
            *missing,
        ]
        for candidate in candidates:
            parts.extend(
                (
                    str(candidate.rank),
                    candidate.pair.chunk_version_id,
                    candidate.distance.hex(),
                    candidate.score.hex(),
                    candidate.chunk_artifact_id,
                    candidate.chunk_input_hash,
                    candidate.chunk_vector_hash,
                )
            )
        return stable_m4_digest(*parts)
