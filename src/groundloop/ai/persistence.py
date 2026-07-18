"""Atomic PostgreSQL publication for the static M3 AI pipeline."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from groundloop.ai.contracts import (
    ComponentTiming,
    EmbeddingRecord,
    ModelArtifact,
    PipelineRunManifest,
    PipelineRunStatus,
    PromptArtifact,
)
from groundloop.ai.embeddings.common import normalized_vector
from groundloop.ai.manifest import manifest_from_dict, manifest_to_dict
from groundloop.ai.pipeline import CorpusDocument, StructuredGroundingResult
from groundloop.ai.retrieval.store import EmbeddingStore, StoredEmbeddingHit
from groundloop.domain import DecisionPolicy
from groundloop.errors import ArtifactConflictError, ValidationError
from groundloop.postgres import (
    MismatchCounts,
    apply_m2_schema,
    read_mismatch_counts,
    read_oracle_states,
)


@dataclass(frozen=True, slots=True)
class ChunkerArtifactRecord:
    artifact_id: str
    version: str
    normalization_version: str
    config_hash: str


@dataclass(frozen=True, slots=True)
class M3PublicationBundle:
    """Complete staged products required for one atomic publication."""

    manifest: PipelineRunManifest
    model_artifacts: tuple[ModelArtifact, ...]
    prompt_artifacts: tuple[PromptArtifact, ...]
    chunker_artifact: ChunkerArtifactRecord
    documents: tuple[CorpusDocument, ...]
    embeddings: tuple[EmbeddingRecord, ...]
    policy: DecisionPolicy
    structured: StructuredGroundingResult
    generation_model_artifact_id: str
    generation_prompt_artifact_id: str
    extraction_model_artifact_id: str
    extraction_prompt_artifact_id: str

    def __post_init__(self) -> None:
        if self.manifest.status is not PipelineRunStatus.STAGED:
            raise ValidationError("publication bundle must carry a staged manifest")
        if self.manifest.answer is None or self.manifest.extraction is None:
            raise ValidationError("publication bundle is missing model outputs")
        if self.manifest.claims != self.manifest.extraction.claims:
            raise ValidationError("manifest claims differ from extraction result")


@dataclass(slots=True)
class StagedPgvectorCosineStore:
    """Session-private pgvector index used before atomic final publication."""

    connection: Connection[Any]

    def search_cosine(
        self,
        *,
        vector: tuple[float, ...],
        model_artifact_id: str,
        limit: int,
    ) -> tuple[StoredEmbeddingHit, ...]:
        if limit <= 0:
            raise ValidationError("retrieval limit must be positive")
        values = normalized_vector(vector)
        literal = "[" + ",".join(format(item, ".17g") for item in values) + "]"
        rows = self.connection.execute(
            """
            SELECT chunk_version_id, embedding <=> %s::vector AS distance
            FROM pg_temp.groundloop_m3_staged_embedding
            WHERE model_artifact_id = %s
            ORDER BY distance, chunk_version_id
            LIMIT %s
            """,
            (literal, model_artifact_id, limit),
        ).fetchall()
        hits = tuple(
            StoredEmbeddingHit(str(chunk_id), float(distance))
            for chunk_id, distance in rows
        )
        if any(not math.isfinite(item.distance) for item in hits):
            raise ValidationError("staged pgvector returned non-finite distance")
        return hits


class PostgresArtifactStore:
    """Conflict-detecting M3 store with staged, terminal run transitions."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def initialize_schema(self) -> bool:
        """Initialize the current search path once; return whether it was new."""
        with self._connection.transaction():
            m3_exists = self._connection.execute(
                "SELECT to_regclass('groundloop_pipeline_run')"
            ).fetchone()
            if m3_exists is not None and m3_exists[0] is not None:
                return False
            m2_exists = self._connection.execute(
                "SELECT to_regclass('groundloop_epoch')"
            ).fetchone()
            if m2_exists is not None and m2_exists[0] is not None:
                root = Path(__file__).resolve().parents[3]
                self._connection.execute(
                    (root / "migrations/002_m3_static_ai.sql").read_text()
                )
            else:
                apply_m2_schema(self._connection)
            return True

    def lookup(self, run_id: str) -> PipelineRunManifest | None:
        row = self._connection.execute(
            "SELECT manifest FROM groundloop_pipeline_run WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        if row is None or not row[0]:
            return None
        return manifest_from_dict(row[0])

    def prepare_retrieval_store(
        self, embeddings: tuple[EmbeddingRecord, ...]
    ) -> EmbeddingStore:
        """Build a session-private pgvector index for pre-publication retrieval."""
        with self._connection.transaction():
            self._connection.execute(
                """
                DROP TABLE IF EXISTS pg_temp.groundloop_m3_staged_embedding
                """
            )
            self._connection.execute(
                """
                CREATE TEMP TABLE groundloop_m3_staged_embedding (
                    chunk_version_id text NOT NULL,
                    model_artifact_id text NOT NULL,
                    embedding vector(384) NOT NULL,
                    input_hash char(64) NOT NULL,
                    PRIMARY KEY (chunk_version_id, model_artifact_id)
                ) ON COMMIT PRESERVE ROWS
                """
            )
            for item in embeddings:
                self._connection.execute(
                    """
                    INSERT INTO groundloop_m3_staged_embedding VALUES
                        (%s, %s, %s, %s)
                    """,
                    (
                        item.chunk_version_id,
                        item.model_artifact_id,
                        list(item.vector),
                        item.input_hash,
                    ),
                )
            self._connection.execute(
                """
                CREATE INDEX groundloop_m3_staged_embedding_hnsw
                ON groundloop_m3_staged_embedding
                USING hnsw (embedding vector_cosine_ops)
                """
            )
        return StagedPgvectorCosineStore(self._connection)

    def stage(self, manifest: PipelineRunManifest) -> None:
        if manifest.status is not PipelineRunStatus.STAGED:
            raise ValidationError("only a staged manifest can start an M3 run")
        with self._connection.transaction():
            existing = self._connection.execute(
                """
                SELECT schema_version, config_hash, input_hash, corpus_hash,
                       question_id
                FROM groundloop_pipeline_run WHERE run_id = %s
                FOR UPDATE
                """,
                (manifest.run_id,),
            ).fetchone()
            identity = (
                manifest.schema_version,
                manifest.config_hash,
                manifest.input_hash,
                manifest.corpus_hash,
                manifest.question_id,
            )
            if existing is not None:
                normalized_existing = tuple(
                    item.strip() if isinstance(item, str) else item
                    for item in existing
                )
                if normalized_existing != identity:
                    raise ArtifactConflictError(
                        f"pipeline run {manifest.run_id} has conflicting identity"
                    )
                return
            self._connection.execute(
                """
                INSERT INTO groundloop_pipeline_run (
                    run_id, schema_version, status, config_hash, input_hash,
                    corpus_hash, question_id, manifest
                ) VALUES (%s, %s, 'staged', %s, %s, %s, %s, %s)
                """,
                (
                    manifest.run_id,
                    manifest.schema_version,
                    manifest.config_hash,
                    manifest.input_hash,
                    manifest.corpus_hash,
                    manifest.question_id,
                    Jsonb(manifest_to_dict(manifest)),
                ),
            )

    def fail(self, manifest: PipelineRunManifest) -> None:
        if manifest.status is not PipelineRunStatus.FAILED:
            raise ValidationError("fail requires a failed manifest")
        with self._connection.transaction():
            updated = self._connection.execute(
                """
                UPDATE groundloop_pipeline_run
                SET status = 'failed', failure_code = %s, completed_at = now(),
                    manifest = %s
                WHERE run_id = %s AND status = 'staged'
                RETURNING run_id
                """,
                (
                    manifest.failure_code,
                    Jsonb(manifest_to_dict(manifest)),
                    manifest.run_id,
                ),
            ).fetchone()
            if updated is None:
                raise ArtifactConflictError(
                    f"pipeline run {manifest.run_id} is not staged"
                )

    def publish(self, manifest: PipelineRunManifest) -> None:
        raise ValidationError("use publish_bundle for atomic structured publication")

    def publish_bundle(self, bundle: M3PublicationBundle) -> PipelineRunManifest:
        """Publish every structured record in one transaction or none."""
        publication_started = time.perf_counter()
        with self._connection.transaction():
            row = self._connection.execute(
                """
                SELECT status, config_hash, input_hash, corpus_hash, question_id,
                       manifest
                FROM groundloop_pipeline_run WHERE run_id = %s FOR UPDATE
                """,
                (bundle.manifest.run_id,),
            ).fetchone()
            if row is None:
                raise ArtifactConflictError(
                    f"pipeline run {bundle.manifest.run_id} was not staged"
                )
            expected_identity = (
                bundle.manifest.config_hash,
                bundle.manifest.input_hash,
                bundle.manifest.corpus_hash,
                bundle.manifest.question_id,
            )
            actual_identity = tuple(
                item.strip() if isinstance(item, str) else item for item in row[1:5]
            )
            if actual_identity != expected_identity:
                raise ArtifactConflictError("staged pipeline identity changed")
            if row[0] == PipelineRunStatus.PUBLISHED.value:
                return manifest_from_dict(row[5])
            if row[0] != PipelineRunStatus.STAGED.value:
                raise ArtifactConflictError(
                    f"pipeline run is terminal with status {row[0]}"
                )

            new_ids: list[str] = []
            reused_ids: list[str] = []
            self._register_artifacts(bundle, new_ids, reused_ids)
            epoch_id = self._insert_epoch(bundle.manifest)
            self._insert_policy(bundle.policy, epoch_id, new_ids, reused_ids)
            self._insert_corpus(bundle, epoch_id, new_ids, reused_ids)
            self._insert_answer(bundle, epoch_id, new_ids, reused_ids)
            self._insert_candidates(bundle, new_ids, reused_ids)
            self._insert_observations(bundle, epoch_id, new_ids, reused_ids)
            self._insert_materialized_state(bundle, epoch_id)
            self._connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

            oracle = read_oracle_states(self._connection)
            expected_claims = {
                state.claim_id: state for state in bundle.structured.claim_states
            }
            expected_answers = {
                state.answer_version_id: state
                for state in bundle.structured.answer_states
            }
            if oracle.claims != expected_claims or oracle.answers != expected_answers:
                raise ValidationError("PostgreSQL oracle disagrees before publication")
            if read_mismatch_counts(self._connection) != MismatchCounts(0, 0, 0):
                raise ValidationError("materialized M3 state fails SQL oracle checks")

            published = replace(
                bundle.manifest,
                status=PipelineRunStatus.PUBLISHED,
                semantic_epoch_id=epoch_id,
                confirmed_as_of_epoch=epoch_id,
                reused_artifact_ids=tuple(sorted(set(reused_ids))),
                new_artifact_ids=tuple(sorted(set(new_ids))),
                timings=bundle.manifest.timings
                + (
                    ComponentTiming(
                        "database_publication",
                        (time.perf_counter() - publication_started) * 1_000.0,
                        False,
                    ),
                ),
                failure_code=None,
            )
            self._insert_artifact_use_and_timings(published, bundle)
            self._connection.execute(
                """
                UPDATE groundloop_pipeline_run
                SET status = 'published', answer_version_id = %s,
                    semantic_epoch_id = %s, manifest = %s, completed_at = now()
                WHERE run_id = %s
                """,
                (
                    published.answer_version_id,
                    epoch_id,
                    Jsonb(manifest_to_dict(published)),
                    published.run_id,
                ),
            )
            return published

    def replay_manifest(self, manifest: PipelineRunManifest) -> PipelineRunManifest:
        """Return an attempt-local view showing that all artifacts were reused."""
        all_ids = tuple(
            sorted(set(manifest.reused_artifact_ids + manifest.new_artifact_ids))
        )
        return replace(
            manifest,
            reused_artifact_ids=all_ids,
            new_artifact_ids=(),
        )

    def _register_artifacts(
        self,
        bundle: M3PublicationBundle,
        new_ids: list[str],
        reused_ids: list[str],
    ) -> None:
        for model_artifact in bundle.model_artifacts:
            created = self._insert_model(model_artifact)
            (new_ids if created else reused_ids).append(model_artifact.artifact_id)
        for prompt_artifact in bundle.prompt_artifacts:
            created = self._insert_prompt(prompt_artifact)
            (new_ids if created else reused_ids).append(prompt_artifact.artifact_id)
        chunker = bundle.chunker_artifact
        inserted = self._connection.execute(
            """
            INSERT INTO groundloop_chunker_artifact (
                chunker_artifact_id, chunker_version, normalization_version,
                config_hash
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (chunker_artifact_id) DO NOTHING RETURNING true
            """,
            (
                chunker.artifact_id,
                chunker.version,
                chunker.normalization_version,
                chunker.config_hash,
            ),
        ).fetchone()
        if inserted is None:
            existing = self._connection.execute(
                """
                SELECT chunker_version, normalization_version, config_hash
                FROM groundloop_chunker_artifact
                WHERE chunker_artifact_id = %s
                """,
                (chunker.artifact_id,),
            ).fetchone()
            if existing is None or self._strip(existing) != (
                chunker.version,
                chunker.normalization_version,
                chunker.config_hash,
            ):
                raise ArtifactConflictError("chunker artifact payload conflict")
            reused_ids.append(chunker.artifact_id)
        else:
            new_ids.append(chunker.artifact_id)

    def _insert_model(self, artifact: ModelArtifact) -> bool:
        inserted = self._connection.execute(
            """
            INSERT INTO groundloop_model_artifact (
                model_artifact_id, task, provider, model_id, immutable_revision,
                tokenizer_revision, license_id, config_hash, artifact_sha256
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (model_artifact_id) DO NOTHING RETURNING true
            """,
            (
                artifact.artifact_id,
                artifact.task.value,
                artifact.provider,
                artifact.model_id,
                artifact.immutable_revision,
                artifact.tokenizer_revision,
                artifact.license_id,
                artifact.config_hash,
                artifact.artifact_sha256,
            ),
        ).fetchone()
        if inserted is not None:
            return True
        existing = self._connection.execute(
            """
            SELECT task, provider, model_id, immutable_revision,
                   tokenizer_revision, license_id, config_hash, artifact_sha256
            FROM groundloop_model_artifact WHERE model_artifact_id = %s
            """,
            (artifact.artifact_id,),
        ).fetchone()
        expected = (
            artifact.task.value,
            artifact.provider,
            artifact.model_id,
            artifact.immutable_revision,
            artifact.tokenizer_revision,
            artifact.license_id,
            artifact.config_hash,
            artifact.artifact_sha256,
        )
        if existing is None or self._strip(existing) != expected:
            raise ArtifactConflictError(
                f"model artifact {artifact.artifact_id} payload conflict"
            )
        return False

    def _insert_prompt(self, artifact: PromptArtifact) -> bool:
        inserted = self._connection.execute(
            """
            INSERT INTO groundloop_prompt_artifact (
                prompt_artifact_id, task, version, template, template_hash,
                decoding_config_hash
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (prompt_artifact_id) DO NOTHING RETURNING true
            """,
            (
                artifact.artifact_id,
                artifact.task.value,
                artifact.version,
                artifact.template,
                artifact.template_hash,
                artifact.decoding_config_hash,
            ),
        ).fetchone()
        if inserted is not None:
            return True
        existing = self._connection.execute(
            """
            SELECT task, version, template, template_hash, decoding_config_hash
            FROM groundloop_prompt_artifact WHERE prompt_artifact_id = %s
            """,
            (artifact.artifact_id,),
        ).fetchone()
        expected = (
            artifact.task.value,
            artifact.version,
            artifact.template,
            artifact.template_hash,
            artifact.decoding_config_hash,
        )
        if existing is None or self._strip(existing) != expected:
            raise ArtifactConflictError(
                f"prompt artifact {artifact.artifact_id} payload conflict"
            )
        return False

    def _insert_epoch(self, manifest: PipelineRunManifest) -> int:
        row = self._connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (%s, %s, 0, 'committed', 'sealed', 'complete',
                      'provisional', now())
            RETURNING epoch_id
            """,
            (f"m3-run:{manifest.run_id}", manifest.input_hash),
        ).fetchone()
        assert row is not None
        return int(row[0])

    def _insert_policy(
        self,
        policy: DecisionPolicy,
        epoch_id: int,
        new_ids: list[str],
        reused_ids: list[str],
    ) -> None:
        existing = self._connection.execute(
            """
            SELECT support_threshold, refute_threshold, tie_rule_version,
                   valid_to_epoch
            FROM groundloop_decision_policy WHERE policy_version = %s
            """,
            (policy.policy_version,),
        ).fetchone()
        if existing is not None:
            if existing[:3] != (
                policy.support_threshold,
                policy.refute_threshold,
                policy.tie_rule_version,
            ) or existing[3] is not None:
                raise ArtifactConflictError("decision-policy payload conflict")
            reused_ids.append(policy.policy_version)
            return
        self._connection.execute(
            """
            UPDATE groundloop_decision_policy SET valid_to_epoch = %s
            WHERE valid_to_epoch IS NULL
            """,
            (epoch_id,),
        )
        self._connection.execute(
            """
            INSERT INTO groundloop_decision_policy (
                policy_version, support_threshold, refute_threshold,
                tie_rule_version, valid_from_epoch
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (
                policy.policy_version,
                policy.support_threshold,
                policy.refute_threshold,
                policy.tie_rule_version,
                epoch_id,
            ),
        )
        new_ids.append(policy.policy_version)

    def _insert_corpus(
        self,
        bundle: M3PublicationBundle,
        epoch_id: int,
        new_ids: list[str],
        reused_ids: list[str],
    ) -> None:
        embedding_by_chunk = {
            (item.chunk_version_id, item.model_artifact_id): item
            for item in bundle.embeddings
        }
        for document in bundle.documents:
            document_created = self._connection.execute(
                """
                INSERT INTO groundloop_document (
                    document_id, source_uri, authority_class
                ) VALUES (%s, %s, 'local-m3')
                ON CONFLICT (document_id) DO NOTHING RETURNING true
                """,
                (document.document_id, document.source_uri),
            ).fetchone()
            if document_created is None:
                self._require_existing(
                    """
                    SELECT source_uri, authority_class FROM groundloop_document
                    WHERE document_id = %s
                    """,
                    (document.document_id,),
                    (document.source_uri, "local-m3"),
                    "document",
                )
            version = self._connection.execute(
                """
                INSERT INTO groundloop_document_version (
                    document_version_id, document_id, content_hash,
                    valid_from_epoch
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (document_version_id) DO NOTHING RETURNING true
                """,
                (
                    document.document_version_id,
                    document.document_id,
                    document.content_hash,
                    epoch_id,
                ),
            ).fetchone()
            if version is None:
                self._require_existing(
                    """
                    SELECT document_id, content_hash, valid_to_epoch
                    FROM groundloop_document_version
                    WHERE document_version_id = %s
                    """,
                    (document.document_version_id,),
                    (document.document_id, document.content_hash, None),
                    "document version",
                )
                reused_ids.append(document.document_version_id)
            else:
                new_ids.append(document.document_version_id)
            for chunk in document.chunks:
                created = self._connection.execute(
                    """
                    INSERT INTO groundloop_chunk_version (
                        chunk_version_id, document_version_id, chunk_index, text,
                        text_hash, chunker_version, valid_from_epoch
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (chunk_version_id) DO NOTHING RETURNING true
                    """,
                    (
                        chunk.chunk_version_id,
                        chunk.document_version_id,
                        chunk.chunk_index,
                        chunk.text,
                        chunk.text_hash,
                        bundle.chunker_artifact.version,
                        epoch_id,
                    ),
                ).fetchone()
                if created is None:
                    existing = self._connection.execute(
                        """
                        SELECT document_version_id, chunk_index, text, text_hash,
                               chunker_version
                        FROM groundloop_chunk_version WHERE chunk_version_id = %s
                        """,
                        (chunk.chunk_version_id,),
                    ).fetchone()
                    if existing is None or self._strip(existing) != (
                        chunk.document_version_id,
                        chunk.chunk_index,
                        chunk.text,
                        chunk.text_hash,
                        bundle.chunker_artifact.version,
                    ):
                        raise ArtifactConflictError("chunk payload conflict")
                    reused_ids.append(chunk.chunk_version_id)
                else:
                    new_ids.append(chunk.chunk_version_id)
                provenance_created = self._connection.execute(
                    """
                    INSERT INTO groundloop_chunk_provenance VALUES (%s, %s, %s)
                    ON CONFLICT (chunk_version_id) DO NOTHING RETURNING true
                    """,
                    (
                        chunk.chunk_version_id,
                        bundle.chunker_artifact.artifact_id,
                        document.content_hash,
                    ),
                ).fetchone()
                if provenance_created is None:
                    self._require_existing(
                        """
                        SELECT chunker_artifact_id, input_hash
                        FROM groundloop_chunk_provenance
                        WHERE chunk_version_id = %s
                        """,
                        (chunk.chunk_version_id,),
                        (
                            bundle.chunker_artifact.artifact_id,
                            document.content_hash,
                        ),
                        "chunk provenance",
                    )
                for key, embedding in embedding_by_chunk.items():
                    if key[0] != chunk.chunk_version_id:
                        continue
                    embedded = self._connection.execute(
                        """
                        INSERT INTO groundloop_chunk_embedding (
                            chunk_version_id, model_artifact_id, embedding,
                            input_hash
                        ) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (chunk_version_id, model_artifact_id)
                        DO NOTHING RETURNING true
                        """,
                        (
                            embedding.chunk_version_id,
                            embedding.model_artifact_id,
                            list(embedding.vector),
                            embedding.input_hash,
                        ),
                    ).fetchone()
                    embedding_id = (
                        f"embedding:{embedding.chunk_version_id}:"
                        f"{embedding.model_artifact_id}"
                    )
                    if embedded is None:
                        matches = self._connection.execute(
                            """
                            SELECT count(*) FROM groundloop_chunk_embedding
                            WHERE chunk_version_id = %s
                              AND model_artifact_id = %s
                              AND input_hash = %s
                              AND embedding = %s::vector
                            """,
                            (
                                embedding.chunk_version_id,
                                embedding.model_artifact_id,
                                embedding.input_hash,
                                list(embedding.vector),
                            ),
                        ).fetchone()
                        if matches != (1,):
                            raise ArtifactConflictError(
                                "embedding payload conflict"
                            )
                        reused_ids.append(embedding_id)
                    else:
                        new_ids.append(embedding_id)

    def _insert_answer(
        self,
        bundle: M3PublicationBundle,
        epoch_id: int,
        new_ids: list[str],
        reused_ids: list[str],
    ) -> None:
        structured = bundle.structured
        question_created = self._connection.execute(
            """
            INSERT INTO groundloop_question (question_id, text, created_epoch)
            VALUES (%s, %s, %s) ON CONFLICT (question_id) DO NOTHING
            RETURNING true
            """,
            (structured.question.question_id, structured.question.text, epoch_id),
        ).fetchone()
        if question_created is None:
            self._require_existing(
                "SELECT text FROM groundloop_question WHERE question_id = %s",
                (structured.question.question_id,),
                (structured.question.text,),
                "question",
            )
        answer_created = self._connection.execute(
            """
            INSERT INTO groundloop_answer_version (
                answer_version_id, question_id, text, generator_model_id,
                generator_model_version, prompt_version, created_epoch
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (answer_version_id) DO NOTHING RETURNING true
            """,
            (
                structured.answer.answer_version_id,
                structured.answer.question_id,
                structured.answer.text,
                structured.answer.producer.model_id,
                structured.answer.producer.model_version,
                structured.answer.producer.prompt_version,
                epoch_id,
            ),
        ).fetchone()
        if answer_created is None:
            self._require_existing(
                """
                SELECT question_id, text, generator_model_id,
                       generator_model_version, prompt_version
                FROM groundloop_answer_version WHERE answer_version_id = %s
                """,
                (structured.answer.answer_version_id,),
                (
                    structured.answer.question_id,
                    structured.answer.text,
                    structured.answer.producer.model_id,
                    structured.answer.producer.model_version,
                    structured.answer.producer.prompt_version,
                ),
                "answer version",
            )
            reused_ids.append(structured.answer.answer_version_id)
        else:
            new_ids.append(structured.answer.answer_version_id)
        for claim in structured.claims:
            created = self._connection.execute(
                """
                INSERT INTO groundloop_claim (
                    claim_id, answer_version_id, text, extractor_model_id,
                    extractor_model_version, extractor_prompt_version, required
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (claim_id) DO NOTHING RETURNING true
                """,
                (
                    claim.claim_id,
                    claim.answer_version_id,
                    claim.text,
                    claim.extractor.model_id,
                    claim.extractor.model_version,
                    claim.extractor.prompt_version,
                    claim.required,
                ),
            ).fetchone()
            if created is None:
                self._require_existing(
                    """
                    SELECT answer_version_id, text, extractor_model_id,
                           extractor_model_version, extractor_prompt_version,
                           required
                    FROM groundloop_claim WHERE claim_id = %s
                    """,
                    (claim.claim_id,),
                    (
                        claim.answer_version_id,
                        claim.text,
                        claim.extractor.model_id,
                        claim.extractor.model_version,
                        claim.extractor.prompt_version,
                        claim.required,
                    ),
                    "claim",
                )
                reused_ids.append(claim.claim_id)
            else:
                new_ids.append(claim.claim_id)
        assert bundle.manifest.answer is not None
        for ordinal, chunk_id in enumerate(
            bundle.manifest.answer.cited_chunk_version_ids, start=1
        ):
            citation_created = self._connection.execute(
                """
                INSERT INTO groundloop_answer_citation VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING RETURNING true
                """,
                (structured.answer.answer_version_id, ordinal, chunk_id),
            ).fetchone()
            if citation_created is None:
                self._require_existing(
                    """
                    SELECT chunk_version_id FROM groundloop_answer_citation
                    WHERE answer_version_id = %s AND citation_ordinal = %s
                    """,
                    (structured.answer.answer_version_id, ordinal),
                    (chunk_id,),
                    "answer citation",
                )
        answer = bundle.manifest.answer
        generation_created = self._connection.execute(
            """
            INSERT INTO groundloop_generation_execution VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (answer_version_id) DO NOTHING
            RETURNING true
            """,
            (
                structured.answer.answer_version_id,
                bundle.manifest.run_id,
                bundle.generation_model_artifact_id,
                bundle.generation_prompt_artifact_id,
                answer.input_hash,
                next(
                    item.decoding_config_hash
                    for item in bundle.prompt_artifacts
                    if item.artifact_id == bundle.generation_prompt_artifact_id
                ),
                answer.raw_output_hash,
                answer.repair_count,
            ),
        ).fetchone()
        generation_decoding_hash = next(
            item.decoding_config_hash
            for item in bundle.prompt_artifacts
            if item.artifact_id == bundle.generation_prompt_artifact_id
        )
        if generation_created is None:
            self._require_existing(
                """
                SELECT model_artifact_id, prompt_artifact_id, input_hash,
                       decoding_config_hash, raw_output_hash, repair_count
                FROM groundloop_generation_execution
                WHERE answer_version_id = %s
                """,
                (structured.answer.answer_version_id,),
                (
                    bundle.generation_model_artifact_id,
                    bundle.generation_prompt_artifact_id,
                    answer.input_hash,
                    generation_decoding_hash,
                    answer.raw_output_hash,
                    answer.repair_count,
                ),
                "generation execution",
            )
        assert bundle.manifest.extraction is not None
        extraction = bundle.manifest.extraction
        for claim in structured.claims:
            extraction_created = self._connection.execute(
                """
                INSERT INTO groundloop_claim_extraction_execution VALUES
                    (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (claim_id) DO NOTHING
                RETURNING true
                """,
                (
                    claim.claim_id,
                    bundle.manifest.run_id,
                    bundle.extraction_model_artifact_id,
                    bundle.extraction_prompt_artifact_id,
                    extraction.input_hash,
                    extraction.raw_output_hash,
                    extraction.repair_count,
                ),
            ).fetchone()
            if extraction_created is None:
                self._require_existing(
                    """
                    SELECT model_artifact_id, prompt_artifact_id, input_hash,
                           raw_output_hash, repair_count
                    FROM groundloop_claim_extraction_execution
                    WHERE claim_id = %s
                    """,
                    (claim.claim_id,),
                    (
                        bundle.extraction_model_artifact_id,
                        bundle.extraction_prompt_artifact_id,
                        extraction.input_hash,
                        extraction.raw_output_hash,
                        extraction.repair_count,
                    ),
                    "claim-extraction execution",
                )

    def _insert_candidates(
        self,
        bundle: M3PublicationBundle,
        new_ids: list[str],
        reused_ids: list[str],
    ) -> None:
        claim_ids = {claim.claim_id for claim in bundle.structured.claims}
        for candidate in bundle.manifest.retrieval_candidates:
            claim_id = candidate.query_id if candidate.query_id in claim_ids else None
            created = self._connection.execute(
                """
                INSERT INTO groundloop_retrieval_candidate (
                    candidate_id, run_id, query_kind, query_id, claim_id,
                    chunk_version_id, embedding_model_artifact_id,
                    method_version, score, rank
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (candidate_id) DO NOTHING RETURNING true
                """,
                (
                    candidate.candidate_id,
                    bundle.manifest.run_id,
                    candidate.query_kind.value,
                    candidate.query_id,
                    claim_id,
                    candidate.chunk_version_id,
                    candidate.embedding_model_artifact_id,
                    candidate.method_version,
                    candidate.score,
                    candidate.rank,
                ),
            ).fetchone()
            if created is None:
                self._require_existing(
                    """
                    SELECT query_kind, query_id, claim_id, chunk_version_id,
                           embedding_model_artifact_id, method_version, score,
                           rank
                    FROM groundloop_retrieval_candidate WHERE candidate_id = %s
                    """,
                    (candidate.candidate_id,),
                    (
                        candidate.query_kind.value,
                        candidate.query_id,
                        claim_id,
                        candidate.chunk_version_id,
                        candidate.embedding_model_artifact_id,
                        candidate.method_version,
                        candidate.score,
                        candidate.rank,
                    ),
                    "retrieval candidate",
                )
                reused_ids.append(candidate.candidate_id)
            else:
                new_ids.append(candidate.candidate_id)

    def _insert_observations(
        self,
        bundle: M3PublicationBundle,
        epoch_id: int,
        new_ids: list[str],
        reused_ids: list[str],
    ) -> None:
        for publication in bundle.structured.verification_publications:
            observation = publication.observation
            result = publication.result
            created = self._connection.execute(
                """
                INSERT INTO groundloop_semantic_observation (
                    observation_id, subject_kind, subject_id, chunk_version_id,
                    task_type, support_score, refute_score, neutral_score,
                    model_id, model_version, prompt_version, input_hash,
                    produced_epoch, raw_output_hash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s)
                ON CONFLICT (observation_id) DO NOTHING RETURNING true
                """,
                (
                    observation.observation_id,
                    observation.subject_kind.value,
                    observation.subject_id,
                    observation.chunk_version_id,
                    observation.task_type,
                    observation.support_score,
                    observation.refute_score,
                    observation.neutral_score,
                    observation.producer.model_id,
                    observation.producer.model_version,
                    observation.producer.prompt_version,
                    observation.input_hash,
                    epoch_id,
                    result.raw_output_hash,
                ),
            ).fetchone()
            if created is None:
                self._require_existing(
                    """
                    SELECT subject_kind::text, subject_id, chunk_version_id,
                           task_type, support_score, refute_score, neutral_score,
                           model_id, model_version, prompt_version, input_hash,
                           raw_output_hash
                    FROM groundloop_semantic_observation
                    WHERE observation_id = %s
                    """,
                    (observation.observation_id,),
                    (
                        observation.subject_kind.value,
                        observation.subject_id,
                        observation.chunk_version_id,
                        observation.task_type,
                        observation.support_score,
                        observation.refute_score,
                        observation.neutral_score,
                        observation.producer.model_id,
                        observation.producer.model_version,
                        observation.producer.prompt_version,
                        observation.input_hash,
                        result.raw_output_hash,
                    ),
                    "semantic observation",
                )
                reused_ids.append(observation.observation_id)
            else:
                new_ids.append(observation.observation_id)
            self._connection.execute(
                """
                INSERT INTO groundloop_observation_currency (
                    subject_kind, subject_id, chunk_version_id, task_type,
                    observation_id, installed_revision
                ) VALUES (%s, %s, %s, %s, %s, 0)
                ON CONFLICT (subject_kind, subject_id, chunk_version_id, task_type)
                DO UPDATE SET observation_id = EXCLUDED.observation_id,
                              installed_revision = EXCLUDED.installed_revision
                """,
                (
                    observation.subject_kind.value,
                    observation.subject_id,
                    observation.chunk_version_id,
                    observation.task_type,
                    observation.observation_id,
                ),
            )
            logits = result.raw_logits or (0.0, 0.0, 0.0)
            execution_created = self._connection.execute(
                """
                INSERT INTO groundloop_verification_execution (
                    observation_id, run_id, candidate_id, model_artifact_id,
                    prompt_artifact_id, calibration_version, temperature,
                    raw_logits, raw_output_hash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (observation_id) DO NOTHING
                RETURNING true
                """,
                (
                    observation.observation_id,
                    bundle.manifest.run_id,
                    result.candidate_id,
                    result.model_artifact_id,
                    result.prompt_artifact_id,
                    result.calibration_version,
                    result.temperature,
                    list(logits),
                    result.raw_output_hash,
                ),
            ).fetchone()
            if execution_created is None:
                self._require_existing(
                    """
                    SELECT candidate_id, model_artifact_id, prompt_artifact_id,
                           calibration_version, temperature, raw_logits,
                           raw_output_hash
                    FROM groundloop_verification_execution
                    WHERE observation_id = %s
                    """,
                    (observation.observation_id,),
                    (
                        result.candidate_id,
                        result.model_artifact_id,
                        result.prompt_artifact_id,
                        result.calibration_version,
                        result.temperature,
                        list(logits),
                        result.raw_output_hash,
                    ),
                    "verification execution",
                )

    def _insert_materialized_state(
        self, bundle: M3PublicationBundle, epoch_id: int
    ) -> None:
        certificate_by_claim = {
            item.claim_id: item for item in bundle.structured.certificates
        }
        for claim_state in bundle.structured.claim_states:
            self._connection.execute(
                """
                INSERT INTO groundloop_claim_state_materialized VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, 0
                ) ON CONFLICT (claim_id) DO UPDATE SET
                    support_count = EXCLUDED.support_count,
                    refute_count = EXCLUDED.refute_count,
                    best_support_score = EXCLUDED.best_support_score,
                    best_refute_score = EXCLUDED.best_refute_score,
                    supporting_observation_ids = EXCLUDED.supporting_observation_ids,
                    refuting_observation_ids = EXCLUDED.refuting_observation_ids,
                    status = EXCLUDED.status,
                    updated_epoch = EXCLUDED.updated_epoch,
                    updated_revision = EXCLUDED.updated_revision
                """,
                (
                    claim_state.claim_id,
                    claim_state.support_count,
                    claim_state.refute_count,
                    claim_state.best_support_score,
                    claim_state.best_refute_score,
                    list(claim_state.supporting_observation_ids),
                    list(claim_state.refuting_observation_ids),
                    claim_state.status.value,
                    epoch_id,
                ),
            )
            certificate = certificate_by_claim[claim_state.claim_id]
            self._connection.execute(
                """
                INSERT INTO groundloop_claim_certificate VALUES (%s, %s, %s, %s, 0)
                ON CONFLICT (claim_id) DO UPDATE SET
                    support_observation_id = EXCLUDED.support_observation_id,
                    refute_observation_id = EXCLUDED.refute_observation_id,
                    repaired_epoch = EXCLUDED.repaired_epoch,
                    repaired_revision = EXCLUDED.repaired_revision
                """,
                (
                    certificate.claim_id,
                    certificate.support_observation_id,
                    certificate.refute_observation_id,
                    epoch_id,
                ),
            )
        for answer_state in bundle.structured.answer_states:
            self._connection.execute(
                """
                INSERT INTO groundloop_answer_state_materialized VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, 0
                ) ON CONFLICT (answer_version_id) DO UPDATE SET
                    required_claim_count = EXCLUDED.required_claim_count,
                    supported_count = EXCLUDED.supported_count,
                    unsupported_count = EXCLUDED.unsupported_count,
                    refuted_count = EXCLUDED.refuted_count,
                    conflicted_count = EXCLUDED.conflicted_count,
                    status = EXCLUDED.status,
                    updated_epoch = EXCLUDED.updated_epoch,
                    updated_revision = EXCLUDED.updated_revision
                """,
                (
                    answer_state.answer_version_id,
                    answer_state.required_claim_count,
                    answer_state.supported_count,
                    answer_state.unsupported_count,
                    answer_state.refuted_count,
                    answer_state.conflicted_count,
                    answer_state.status.value,
                    epoch_id,
                ),
            )

    def _insert_artifact_use_and_timings(
        self,
        manifest: PipelineRunManifest,
        bundle: M3PublicationBundle,
    ) -> None:
        reused = set(manifest.reused_artifact_ids)
        kinds = {
            **{item.artifact_id: "model" for item in bundle.model_artifacts},
            **{item.artifact_id: "prompt" for item in bundle.prompt_artifacts},
            bundle.chunker_artifact.artifact_id: "chunker",
            **{
                f"embedding:{item.chunk_version_id}:{item.model_artifact_id}": (
                    "embedding"
                )
                for item in bundle.embeddings
            },
            **{
                item.candidate_id: "retrieval"
                for item in manifest.retrieval_candidates
            },
            bundle.structured.answer.answer_version_id: "generation",
            **{item.claim_id: "extraction" for item in bundle.structured.claims},
            **{
                item.observation_id: "verification"
                for item in bundle.structured.observations
            },
        }
        for artifact_id, artifact_kind in sorted(kinds.items()):
            self._connection.execute(
                """
                INSERT INTO groundloop_pipeline_artifact_use VALUES
                    (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    manifest.run_id,
                    artifact_kind,
                    artifact_id,
                    artifact_id in reused,
                ),
            )
        for timing in manifest.timings:
            self._connection.execute(
                """
                INSERT INTO groundloop_component_timing VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    manifest.run_id,
                    timing.component,
                    timing.elapsed_ms,
                    timing.cold_start,
                ),
            )

    @staticmethod
    def _strip(row: tuple[Any, ...]) -> tuple[Any, ...]:
        return tuple(item.strip() if isinstance(item, str) else item for item in row)

    def _require_existing(
        self,
        query: str,
        parameters: tuple[object, ...],
        expected: tuple[object, ...],
        label: str,
    ) -> None:
        row = self._connection.execute(query, parameters).fetchone()
        if row is None or self._strip(row) != expected:
            raise ArtifactConflictError(f"{label} payload conflict")
