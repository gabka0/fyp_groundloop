"""Production composition of M4 vector, lexical, lineage, and frontier work."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from typing import Any, Protocol

from psycopg import Connection

from groundloop.ai.contracts import ChunkDraft
from groundloop.errors import EventConflictError, ValidationError
from groundloop.m4.admission.fresh import FreshFrontierRetriever
from groundloop.m4.admission.fusion import (
    fuse_admission_channels,
    lineage_channel_hits,
)
from groundloop.m4.admission.lexical import LexicalV1Policy
from groundloop.m4.admission.vector import (
    ApproximateReverseVectorIndex,
    ChunkRoleVector,
)
from groundloop.m4.application import DiscoveryResult
from groundloop.m4.contracts import (
    AdmissionChannel,
    AdmittedPair,
    CandidatePolicyManifest,
    ChannelHit,
    FrontierEntry,
    FrontierState,
    JobKind,
    LogicalJobSpec,
    PairKey,
    stable_m4_digest,
)
from groundloop.m4.models.embedding import ClaimEmbeddingInput
from groundloop.m4.models.ports import AdmissionEmbeddingArtifacts
from groundloop.m4.runtime.frontier import plan_frontier_refill


class ChunkEmbeddingService(Protocol):
    def embed_for_admission(
        self,
        *,
        claims: Sequence[ClaimEmbeddingInput] = (),
        chunks: Sequence[ChunkDraft] = (),
    ) -> AdmissionEmbeddingArtifacts: ...


LineageProvider = Callable[[int, str], tuple[PairKey, ...]]


@dataclass(slots=True)
class PostgresHybridAdmissionPort:
    """Compute the frozen M4 CORE admission policy outside a DB transaction.

    Approximate impact discovery is chunk-to-claim vector plus lexical fusion.
    Frontier repair first uses the persisted reserve.  If the reserve cannot
    meet its frozen depth, an injected exact fresh retriever must establish
    complete active-chunk artifact coverage.  A complete empty result is a
    valid fallback; an unavailable or incomplete retrieval blocks sealing.
    The coordinator persists the returned result atomically with parent/child
    closure; this worker deliberately performs no result writes.
    """

    connection: Connection[Any]
    manifest: CandidatePolicyManifest
    embeddings: ChunkEmbeddingService
    vector_index: ApproximateReverseVectorIndex
    lexical_policy: LexicalV1Policy
    fresh_frontier_retriever: FreshFrontierRetriever | None = None
    lineage_provider: LineageProvider | None = None
    frontier_score_floor: float = -1.0

    def __post_init__(self) -> None:
        if not self.connection.autocommit:
            raise ValidationError(
                "M4 admission requires a psycopg autocommit connection so model "
                "inference cannot inherit an implicit database transaction"
            )

    def discover(
        self, epoch_id: int, root_job: LogicalJobSpec
    ) -> DiscoveryResult:
        if root_job.candidate_policy_id != self.manifest.policy_id:
            raise EventConflictError("admission job uses another candidate policy")
        if root_job.kind is JobKind.IMPACT_DISCOVERY:
            result, hits = self._discover_inserted_chunk(epoch_id, root_job)
        elif root_job.kind is JobKind.FRONTIER_RETRIEVE:
            result, hits = self._repair_frontier(epoch_id, root_job)
        else:
            raise ValidationError("admission accepts expandable root jobs only")
        return replace(result, channel_hits=hits)

    def _chunk_draft(self, chunk_id: str) -> ChunkDraft:
        row = self.connection.execute(
            """
            SELECT chunk.document_version_id, chunk.chunk_index, chunk.text,
                   chunk.text_hash, provenance.chunker_artifact_id
            FROM groundloop_chunk_version AS chunk
            JOIN groundloop_chunk_provenance AS provenance
              USING (chunk_version_id)
            WHERE chunk.chunk_version_id = %s
            """,
            (chunk_id,),
        ).fetchone()
        if row is None:
            raise ValidationError(
                "dynamic admission requires persisted chunk provenance"
            )
        return ChunkDraft(
            chunk_version_id=chunk_id,
            document_version_id=str(row[0]),
            chunk_index=int(row[1]),
            text=str(row[2]),
            text_hash=str(row[3]).strip(),
            chunker_artifact_id=str(row[4]),
        )

    def _discover_inserted_chunk(
        self, epoch_id: int, root_job: LogicalJobSpec
    ) -> tuple[DiscoveryResult, tuple[ChannelHit, ...]]:
        chunk_id = root_job.target_chunk_version_id
        if chunk_id is None:
            raise ValidationError("impact job has no chunk target")
        chunk = self._chunk_draft(chunk_id)
        embedded = self.embeddings.embed_for_admission(chunks=(chunk,))
        if len(embedded.chunk_vectors) != 1:
            raise ValidationError("embedding service returned the wrong chunk set")
        vector: ChunkRoleVector = embedded.chunk_vectors[0]
        vector_result = self.vector_index.search(
            epoch_id=epoch_id,
            candidate_policy_id=self.manifest.policy_id,
            chunk=vector,
            limit=self.manifest.approximate_cap_per_inserted_chunk,
        )
        lexical_hits = self.lexical_policy.search(
            epoch_id=epoch_id,
            manifest=self.manifest,
            chunk_version_id=chunk_id,
            chunk_text=chunk.text,
        )
        lineage_pairs = (
            ()
            if self.lineage_provider is None
            else self.lineage_provider(epoch_id, chunk_id)
        )
        lineage_hits = lineage_channel_hits(
            epoch_id=epoch_id,
            candidate_policy_id=self.manifest.policy_id,
            pairs=lineage_pairs,
        )
        fused = fuse_admission_channels(
            inserted_chunk_ids=(chunk_id,),
            manifest=self.manifest,
            vector_hits=vector_result.hits,
            lexical_hits=lexical_hits,
            lineage_hits=lineage_hits,
        )
        artifact_hash = stable_m4_digest(
            "m4-impact-discovery-result-v1",
            root_job.job_id,
            vector_result.query_artifact_hash,
            *(hit.channel_artifact_hash for hit in fused.channel_hits),
            *(self._admitted_id(item) for item in fused.admitted_pairs),
        )
        return (
            DiscoveryResult(
                root_job.job_id,
                f"m4-impact:{artifact_hash}",
                artifact_hash,
                fused.admitted_pairs,
            ),
            fused.channel_hits,
        )

    def _repair_frontier(
        self, epoch_id: int, root_job: LogicalJobSpec
    ) -> tuple[DiscoveryResult, tuple[ChannelHit, ...]]:
        claim_id = root_job.target_claim_id
        if claim_id is None:
            raise ValidationError("frontier job has no claim target")
        entry_rows = self.connection.execute(
            """
            SELECT claim_id, chunk_version_id, candidate_policy_id,
                   frontier_state, rank, retrieval_score,
                   candidate_artifact_hash
            FROM groundloop_m4_effective_candidate_frontier
            WHERE epoch_id = %s AND claim_id = %s AND candidate_policy_id = %s
            ORDER BY rank, chunk_version_id
            """,
            (epoch_id, claim_id, self.manifest.policy_id),
        ).fetchall()
        entries = tuple(
            FrontierEntry(
                str(row[0]),
                str(row[1]),
                str(row[2]),
                FrontierState(str(row[3])),
                int(row[4]),
                float(row[5]),
                str(row[6]).strip(),
            )
            for row in entry_rows
        )
        active_chunks = frozenset(
            str(row[0])
            for row in self.connection.execute(
                """
                SELECT chunk_version_id
                FROM groundloop_m4_effective_chunk_version
                WHERE epoch_id = %s
                """,
                (epoch_id,),
            ).fetchall()
        )
        current_rows = self.connection.execute(
            """
            SELECT currency.chunk_version_id
            FROM groundloop_m4_effective_observation_currency AS currency
            JOIN groundloop_m4_effective_chunk_version AS chunk
              ON chunk.epoch_id = currency.epoch_id
             AND chunk.chunk_version_id = currency.chunk_version_id
            WHERE currency.subject_kind = 'claim'
              AND currency.subject_id = %s
              AND currency.epoch_id = %s
            ORDER BY currency.chunk_version_id
            """,
            (claim_id, epoch_id),
        ).fetchall()
        current = tuple(PairKey(claim_id, str(row[0])) for row in current_rows)
        plan = plan_frontier_refill(
            claim_id=claim_id,
            candidate_policy_id=self.manifest.policy_id,
            entries=entries,
            current_verified_pairs=current,
            active_chunk_ids=active_chunks,
            target_depth=self.manifest.frontier_depth,
            retrieval_score_floor=self.frontier_score_floor,
        )
        reserve_hits = tuple(
            ChannelHit(
                epoch_id,
                pair,
                self.manifest.policy_id,
                AdmissionChannel.FRONTIER,
                rank,
                next(
                    entry.retrieval_score
                    for entry in entries
                    if entry.chunk_version_id == pair.chunk_version_id
                ),
                stable_m4_digest(
                    "m4-frontier-channel-hit-v1",
                    root_job.job_id,
                    pair.claim_id,
                    pair.chunk_version_id,
                    str(rank),
                ),
            )
            for rank, pair in enumerate(plan.verifier_pairs, start=1)
        )
        fresh_hash = "fresh-not-required"
        fresh_hits: tuple[ChannelHit, ...] = ()
        fallback_satisfied = not plan.fresh_retrieval_required
        if plan.fresh_retrieval_required:
            if self.fresh_frontier_retriever is None:
                fresh_hash = "fresh-retriever-unavailable"
            else:
                excluded = tuple(
                    sorted(
                        {
                            pair.chunk_version_id
                            for pair in (
                                *plan.current_pairs,
                                *plan.already_queued_pairs,
                                *plan.selected_reserve_pairs,
                            )
                        }
                    )
                )
                fresh = self.fresh_frontier_retriever.retrieve(
                    epoch_id=epoch_id,
                    claim_id=claim_id,
                    manifest=self.manifest,
                    excluded_chunk_ids=excluded,
                    limit=plan.remaining_deficit,
                )
                fresh_hash = fresh.artifact_hash
                fallback_satisfied = fresh.complete
                fresh_hits = tuple(
                    ChannelHit(
                        epoch_id,
                        candidate.pair,
                        self.manifest.policy_id,
                        AdmissionChannel.FRONTIER,
                        rank,
                        candidate.score,
                        stable_m4_digest(
                            "m4-fresh-frontier-channel-hit-v1",
                            root_job.job_id,
                            fresh.artifact_hash,
                            candidate.pair.claim_id,
                            candidate.pair.chunk_version_id,
                            str(candidate.rank),
                            candidate.distance.hex(),
                            candidate.score.hex(),
                            candidate.chunk_artifact_id,
                            candidate.chunk_input_hash,
                            candidate.chunk_vector_hash,
                        ),
                    )
                    for rank, candidate in enumerate(
                        fresh.candidates, start=len(reserve_hits) + 1
                    )
                )
        hits = reserve_hits + fresh_hits
        admitted = tuple(
            AdmittedPair(
                epoch_id,
                hit.pair,
                self.manifest.policy_id,
                hit.rank,
                (AdmissionChannel.FRONTIER,),
                False,
            )
            for hit in hits
        )
        artifact_hash = stable_m4_digest(
            "m4-frontier-discovery-result-v1",
            root_job.job_id,
            *(hit.channel_artifact_hash for hit in hits),
            fresh_hash,
            "fallback-satisfied" if fallback_satisfied else "fallback-blocked",
        )
        return (
            DiscoveryResult(
                root_job.job_id,
                f"m4-frontier:{artifact_hash}",
                artifact_hash,
                admitted,
                fallback_satisfied=fallback_satisfied,
            ),
            hits,
        )

    @staticmethod
    def _admitted_id(admitted: AdmittedPair) -> str:
        return stable_m4_digest(
            "m4-admitted-pair-v1",
            str(admitted.epoch_id),
            admitted.pair.claim_id,
            admitted.pair.chunk_version_id,
            admitted.candidate_policy_id,
        )
