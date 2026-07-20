"""Reproducible, bounded M4.5 PostgreSQL smoke with pinned local models.

This module is an opt-in integration harness, not a quality benchmark.  It
creates a disposable schema, registers one existing claim, executes one real
model-backed document insertion through the production M4 application, checks
the three exact state surfaces and durable empirical provenance, then rebuilds
all ports and proves that exact event replay performs no embedding or verifier
work.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from psycopg import Connection, sql

from groundloop.ai.chunking import FixedCharChunker
from groundloop.ai.contracts import ChunkDraft, stable_digest
from groundloop.ai.embeddings.common import EMBEDDING_DIMENSION
from groundloop.ai.persistence import ChunkerArtifactRecord
from groundloop.domain import (
    ChunkVersion,
    DocumentVersion,
    normalized_text_hash,
)
from groundloop.errors import ValidationError
from groundloop.m4.admission.lexical import (
    LexicalRegistrySnapshot,
    LexicalV1Policy,
    load_frozen_lexical_v1,
)
from groundloop.m4.admission.manifest import hash_config_pairs
from groundloop.m4.admission.postgres_common import (
    PostgresAdmissionServerIdentity,
)
from groundloop.m4.admission.postgres_lexical import (
    PostgresLexicalSearchBackend,
    PostgresSimpleLexemeAnalyzer,
)
from groundloop.m4.admission.postgres_vector import (
    ExactPgvectorConfig,
    PostgresExactReverseVectorIndex,
)
from groundloop.m4.admission.service import PostgresHybridAdmissionPort
from groundloop.m4.application import (
    ApplicationExecutionPolicy,
    DynamicEventPlan,
    EventRunResult,
    EventRunState,
    M4Application,
)
from groundloop.m4.artifacts import PostgresM4ArtifactRegistry
from groundloop.m4.contracts import (
    CandidatePolicyManifest,
    CorpusUpdateIdentity,
    UpdateKind,
    VectorIndexKind,
    sha256_text,
    stable_m4_digest,
)
from groundloop.m4.execution import M4ExecutionIdentity
from groundloop.m4.models.config import (
    PinnedM3AdapterBundle,
    PinnedM3ReuseConfig,
    build_pinned_m3_adapters,
    inspect_local_artifacts,
)
from groundloop.m4.models.contracts import (
    CHUNK_ROLE_TEMPLATE,
    CLAIM_ROLE_TEMPLATE,
    decision_policy_hash,
)
from groundloop.m4.models.embedding import ClaimEmbeddingInput
from groundloop.m4.models.ports import (
    AdmissionEmbeddingArtifacts,
    M4AdmissionEmbeddingService,
    M4VerificationApplicationPort,
)
from groundloop.m4.pipeline import (
    InsertedDocument,
    PersistingAdmissionPort,
    PostgresM4ApplicationPorts,
    PostgresM4VerificationExecutionWriter,
    PostgresPairInputResolver,
    StructuralPayload,
    bootstrap_m4_publication,
)
from groundloop.m4.runtime import RuntimeEpochState
from groundloop.postgres import apply_m2_schema, record_epoch

_SCHEMA_VERSION = "groundloop-m4-real-postgres-smoke-v1"
_CLAIM_ID = "m4-real-smoke-claim"
_ANSWER_ID = "m4-real-smoke-answer"
_QUESTION_ID = "m4-real-smoke-question"
_CLAIM_TEXT = (
    "GroundLoop exactly maintains structured grounding state relative to "
    "stored semantic observations."
)
_INSERTED_TEXT = (
    "GroundLoop does not establish objective truth. It exactly maintains "
    "structured grounding state only after versioned semantic observations "
    "have been stored."
)


class SmokeUnavailableError(RuntimeError):
    """Raised when an explicitly requested local-only smoke cannot start."""


@dataclass(frozen=True, slots=True)
class M4RealPostgresSmokeConfig:
    database_url: str
    repo_root: Path
    artifact_root: Path
    model_config_path: Path | None = None
    lexical_config_path: Path | None = None
    schema_prefix: str = "groundloop_m4_real_smoke"
    keep_schema: bool = False

    def __post_init__(self) -> None:
        if not self.database_url.strip():
            raise SmokeUnavailableError("PostgreSQL database URL is absent")
        if not self.schema_prefix or any(
            not (character.isalnum() or character == "_")
            for character in self.schema_prefix
        ):
            raise ValidationError(
                "smoke schema prefix must contain only letters, digits, underscores"
            )

    @property
    def resolved_model_config_path(self) -> Path:
        return self.model_config_path or (
            self.repo_root / "configs/m4/models/m3_reuse_v1.json"
        )

    @property
    def resolved_lexical_config_path(self) -> Path:
        return self.lexical_config_path or (
            self.repo_root / "configs/m4/impact/lexical_v1.json"
        )


@dataclass(frozen=True, slots=True)
class M4RealPostgresSmokeResult:
    manifest: Mapping[str, object]

    def to_json(self) -> str:
        return json.dumps(self.manifest, indent=2, sort_keys=True) + "\n"


@dataclass(slots=True)
class _PersistingEmbeddingService:
    """Store complete role artifacts immediately after each model call.

    The current production admission boundary returns role artifacts but does
    not itself own their registry.  This smoke composition makes that boundary
    explicit and leaves semantic job/discovery writes to the application.
    """

    delegate: M4AdmissionEmbeddingService
    registry: PostgresM4ArtifactRegistry
    inserted_role_artifacts: int = 0
    reused_role_artifacts: int = 0

    @property
    def request_count(self) -> int:
        return self.delegate.request_count

    def embed_for_admission(
        self,
        *,
        claims: Sequence[ClaimEmbeddingInput] = (),
        chunks: Sequence[ChunkDraft] = (),
    ) -> AdmissionEmbeddingArtifacts:
        result = self.delegate.embed_for_admission(claims=claims, chunks=chunks)
        for claim_artifact in result.claims:
            if self.registry.register_role_embedding(claim_artifact):
                self.inserted_role_artifacts += 1
            else:
                self.reused_role_artifacts += 1
        for chunk_artifact in result.chunks:
            if self.registry.register_role_embedding(chunk_artifact):
                self.inserted_role_artifacts += 1
            else:
                self.reused_role_artifacts += 1
        return result


@dataclass(frozen=True, slots=True)
class _Composition:
    application: M4Application
    ports: PostgresM4ApplicationPorts
    embeddings: _PersistingEmbeddingService
    verifier: M4VerificationApplicationPort
    identity: M4ExecutionIdentity


def _psycopg_url(value: str) -> str:
    return value.replace("postgresql+psycopg://", "postgresql://", 1)


def _chunker_artifact(chunker: FixedCharChunker) -> ChunkerArtifactRecord:
    return ChunkerArtifactRecord(
        artifact_id=chunker.artifact_id,
        version="fixed-char-v1",
        normalization_version="v1",
        config_hash=stable_digest(
            "fixed-char-v1",
            chunker.artifact_id,
            str(chunker.max_characters),
            "no-overlap",
        ),
    )


def _create_schema(connection: Connection[Any], schema_name: str) -> None:
    connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
    connection.execute(
        sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
    )
    apply_m2_schema(connection)


def _drop_schema(connection: Connection[Any], schema_name: str) -> None:
    connection.execute("SET search_path TO public")
    connection.execute(
        sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
    )


def _register_static_artifacts(
    registry: PostgresM4ArtifactRegistry,
    bundle: PinnedM3AdapterBundle,
    chunker: FixedCharChunker,
) -> None:
    registry.register_model(bundle.embeddings.spec.model_artifact)
    registry.register_model(bundle.verifier.spec.model_artifact)
    registry.register_prompt(bundle.verifier.spec.prompt_artifact)
    registry.register_chunker(_chunker_artifact(chunker))


def _seed_b0(
    connection: Connection[Any],
    *,
    config: PinnedM3ReuseConfig,
    chunker: FixedCharChunker,
) -> int:
    epoch_id, created = record_epoch(
        connection,
        event_id="m4-real-smoke-b0",
        payload_hash=stable_m4_digest("m4-real-smoke-b0"),
    )
    if not created:
        raise ValidationError("disposable smoke schema unexpectedly contains B0")
    connection.execute(
        """
        UPDATE groundloop_epoch
        SET revision = 1, structural_status = 'committed',
            semantic_status = 'sealed', evaluation_state = 'complete',
            publication_mode = 'strict', sealed_at = now()
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    )
    base_text = _CLAIM_TEXT
    base_text_hash = normalized_text_hash(base_text)
    connection.execute(
        """
        INSERT INTO groundloop_document(document_id, source_uri, authority_class)
        VALUES ('m4-real-smoke-base-document', 'smoke://base', 'smoke')
        """
    )
    connection.execute(
        """
        INSERT INTO groundloop_document_version(
            document_version_id, document_id, content_hash,
            valid_from_epoch, valid_to_epoch
        ) VALUES (
            'm4-real-smoke-base-version', 'm4-real-smoke-base-document',
            %s, %s, NULL
        )
        """,
        (base_text_hash, epoch_id),
    )
    connection.execute(
        """
        INSERT INTO groundloop_chunk_version(
            chunk_version_id, document_version_id, chunk_index, text,
            text_hash, chunker_version, valid_from_epoch, valid_to_epoch
        ) VALUES (
            'm4-real-smoke-base-chunk', 'm4-real-smoke-base-version', 0,
            %s, %s, 'fixed-char-v1', %s, NULL
        )
        """,
        (base_text, base_text_hash, epoch_id),
    )
    connection.execute(
        """
        INSERT INTO groundloop_chunk_provenance(
            chunk_version_id, chunker_artifact_id, input_hash
        ) VALUES ('m4-real-smoke-base-chunk', %s, %s)
        """,
        (chunker.artifact_id, base_text_hash),
    )
    connection.execute(
        """
        INSERT INTO groundloop_question(question_id, text, created_epoch)
        VALUES (%s, 'What does GroundLoop maintain exactly?', %s)
        """,
        (_QUESTION_ID, epoch_id),
    )
    with connection.transaction():
        connection.execute("SET CONSTRAINTS ALL DEFERRED")
        connection.execute(
            """
            INSERT INTO groundloop_answer_version(
                answer_version_id, question_id, text, generator_model_id,
                generator_model_version, prompt_version, created_epoch
            ) VALUES (%s, %s, %s, 'smoke-generator', 'v1', 'v1', %s)
            """,
            (_ANSWER_ID, _QUESTION_ID, _CLAIM_TEXT, epoch_id),
        )
        connection.execute(
            """
            INSERT INTO groundloop_claim(
                claim_id, answer_version_id, text, extractor_model_id,
                extractor_model_version, extractor_prompt_version, required)
            VALUES (%s, %s, %s, 'smoke-extractor', 'v1', 'v1', true)
            """,
            (_CLAIM_ID, _ANSWER_ID, _CLAIM_TEXT),
        )
    connection.execute(
        """
        INSERT INTO groundloop_decision_policy(
            policy_version, support_threshold, refute_threshold,
            tie_rule_version, calibration_version, source_policy_version,
            valid_from_epoch, valid_to_epoch
        ) VALUES (%s, %s, %s, %s, %s, NULL, %s, NULL)
        """,
        (
            config.decision_policy.policy_version,
            config.decision_policy.support_threshold,
            config.decision_policy.refute_threshold,
            config.decision_policy.tie_rule_version,
            config.calibration_version,
            epoch_id,
        ),
    )
    observation_id = stable_m4_digest("m4-real-smoke-base-observation")
    connection.execute(
        """
        INSERT INTO groundloop_semantic_observation(
            observation_id, subject_kind, subject_id, chunk_version_id,
            task_type, support_score, refute_score, neutral_score, model_id,
            model_version, prompt_version, input_hash, produced_epoch,
            raw_output_hash
        ) VALUES (
            %s, 'claim', %s, 'm4-real-smoke-base-chunk',
            'claim-verification-v1', 0.95, 0.02, 0.03,
            'smoke-b0-verifier', 'v1', 'v1', %s, %s, %s
        )
        """,
        (
            observation_id,
            _CLAIM_ID,
            stable_m4_digest("m4-real-smoke-base-input"),
            epoch_id,
            stable_m4_digest("m4-real-smoke-base-output"),
        ),
    )
    connection.execute(
        """
        INSERT INTO groundloop_observation_currency(
            subject_kind, subject_id, chunk_version_id, task_type,
            observation_id, installed_revision
        ) VALUES (
            'claim', %s, 'm4-real-smoke-base-chunk',
            'claim-verification-v1', %s, 1
        )
        """,
        (_CLAIM_ID, observation_id),
    )
    connection.execute(
        """
        INSERT INTO groundloop_claim_state_materialized(
            claim_id, support_count, refute_count, best_support_score,
            best_refute_score, supporting_observation_ids,
            refuting_observation_ids, status, updated_epoch, updated_revision
        ) VALUES (%s, 1, 0, 0.95, NULL, ARRAY[%s], ARRAY[]::text[],
                  'supported', %s, 1)
        """,
        (_CLAIM_ID, observation_id, epoch_id),
    )
    connection.execute(
        """
        INSERT INTO groundloop_answer_state_materialized(
            answer_version_id, required_claim_count, supported_count,
            unsupported_count, refuted_count, conflicted_count, status,
            updated_epoch, updated_revision
        ) VALUES (%s, 1, 1, 0, 0, 0, 'valid', %s, 1)
        """,
        (_ANSWER_ID, epoch_id),
    )
    connection.execute(
        """
        INSERT INTO groundloop_claim_certificate(
            claim_id, support_observation_id,
            refute_observation_id, repaired_epoch, repaired_revision
        ) VALUES (%s, %s, NULL, %s, 1)
        """,
        (_CLAIM_ID, observation_id, epoch_id),
    )
    bootstrap_m4_publication(connection, sealed_epoch_id=epoch_id)
    return epoch_id


def _build_manifest(
    *,
    bundle: PinnedM3AdapterBundle,
    server: PostgresAdmissionServerIdentity,
    registry_snapshot_id: str,
    lexical_config_hash: str,
) -> tuple[CandidatePolicyManifest, ExactPgvectorConfig]:
    vector_config = ExactPgvectorConfig(
        dimensions=EMBEDDING_DIMENSION,
        pgvector_version=server.pgvector_version,
    )
    manifest = CandidatePolicyManifest.build(
        policy_id="m4-real-postgres-smoke-policy-v1",
        embedding_model_artifact_id=bundle.embeddings.spec.model_artifact.artifact_id,
        claim_role_template_hash=sha256_text(CLAIM_ROLE_TEMPLATE),
        chunk_role_template_hash=sha256_text(CHUNK_ROLE_TEMPLATE),
        vector_method_version="exact-reverse-pgvector-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=hash_config_pairs(
            "m4-vector-index-build-config-v1", vector_config.build_config
        ),
        vector_search_config_hash=hash_config_pairs(
            "m4-vector-search-config-v1", vector_config.search_config
        ),
        lexical_method_version="postgres-lexical-v1",
        lexical_config_hash=lexical_config_hash,
        lexical_postgres_version=server.postgres_version,
        lexical_regconfig_identity="simple",
        claim_registry_snapshot_id=registry_snapshot_id,
        claim_count=1,
        fusion_version="rank-interleave-v1",
        approximate_cap_per_inserted_chunk=1,
        frontier_depth=1,
        verifier_execution_spec_hash=bundle.verifier.spec.execution_spec_hash,
        decision_policy_version=bundle.verifier.spec.decision_policy.policy_version,
        lineage_safety_override=True,
    )
    return manifest, vector_config


def _build_lexical_policy(
    connection: Connection[Any],
    *,
    server: PostgresAdmissionServerIdentity,
    manifest: CandidatePolicyManifest,
    lexical_config_path: Path,
) -> tuple[LexicalV1Policy, PostgresSimpleLexemeAnalyzer, PostgresLexicalSearchBackend]:
    lexical_config = load_frozen_lexical_v1(lexical_config_path.parents[3])
    analyzer = PostgresSimpleLexemeAnalyzer(connection, server)
    registry = LexicalRegistrySnapshot(
        manifest.claim_registry_snapshot_id,
        ((_CLAIM_ID, analyzer.analyze(_CLAIM_TEXT)),),
    )
    backend = PostgresLexicalSearchBackend(
        connection=connection,
        server=server,
        manifest=manifest,
    )
    backend.validate_registry(registry)
    return (
        LexicalV1Policy(
            config=lexical_config,
            registry=registry,
            analyzer=analyzer,
            backend=backend,
        ),
        analyzer,
        backend,
    )


def _compose(
    connection: Connection[Any],
    *,
    bundle: PinnedM3AdapterBundle,
    registry: PostgresM4ArtifactRegistry,
    manifest: CandidatePolicyManifest,
    vector_config: ExactPgvectorConfig,
    server: PostgresAdmissionServerIdentity,
    lexical_config_path: Path,
    payloads: Mapping[str, StructuralPayload],
) -> _Composition:
    embedding_delegate = M4AdmissionEmbeddingService(bundle.embeddings)
    embeddings = _PersistingEmbeddingService(embedding_delegate, registry)
    lexical, analyzer, lexical_backend = _build_lexical_policy(
        connection,
        server=server,
        manifest=manifest,
        lexical_config_path=lexical_config_path,
    )
    vector = PostgresExactReverseVectorIndex(
        connection=connection,
        server=server,
        manifest=manifest,
        config=vector_config,
    )
    vector.validate_registry()
    admission = PostgresHybridAdmissionPort(
        connection=connection,
        manifest=manifest,
        embeddings=embeddings,
        vector_index=vector,
        lexical_policy=lexical,
    )
    verifier = M4VerificationApplicationPort(
        PostgresPairInputResolver(connection), bundle.verifier
    )
    writer = PostgresM4VerificationExecutionWriter(verifier)
    ports = PostgresM4ApplicationPorts(
        connection,
        structural_payloads=payloads,
        verification_provenance_writer=writer,
    )
    identity = M4ExecutionIdentity.build(
        manifest=manifest,
        embedding_adapter_spec_hash=bundle.embeddings.spec.spec_hash,
        vector_adapter_artifact_id=vector.artifact_id,
        lexical_analyzer_artifact_id=analyzer.artifact_id,
        lexical_backend_artifact_id=lexical_backend.artifact_id,
        frontier_retriever_artifact_id="m4-real-smoke-frontier-not-exercised-v1",
    )
    application = M4Application(
        structural=ports,
        runtime=ports,
        admission=PersistingAdmissionPort(connection, admission),
        verifier=verifier,
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            identity.impact_discovery_hash,
            identity.frontier_retrieval_hash,
            identity.verifier_hash,
        ),
    )
    return _Composition(application, ports, embeddings, verifier, identity)


def _insert_payload(chunker: FixedCharChunker) -> tuple[StructuralPayload, str]:
    document_id = stable_m4_digest("m4-real-smoke-insert-document")
    document_version_id = stable_m4_digest(
        "m4-real-smoke-insert-version", normalized_text_hash(_INSERTED_TEXT)
    )
    drafts = chunker.chunk(document_version_id, _INSERTED_TEXT)
    if len(drafts) != 1:
        raise ValidationError("bounded M4 smoke input must produce exactly one chunk")
    draft = drafts[0]
    inserted = InsertedDocument(
        version=DocumentVersion(
            document_version_id,
            document_id,
            normalized_text_hash(_INSERTED_TEXT),
        ),
        chunks=(
            ChunkVersion(
                draft.chunk_version_id,
                draft.document_version_id,
                draft.chunk_index,
                draft.text,
                draft.text_hash,
            ),
        ),
        source_uri="smoke://inserted",
        authority_class="smoke",
        chunker_version="fixed-char-v1",
        chunker_artifact_id=chunker.artifact_id,
        chunker_input_hash=normalized_text_hash(_INSERTED_TEXT),
    )
    return StructuralPayload(inserted=inserted), draft.chunk_version_id


def _event(
    *,
    previous_epoch_id: int,
    manifest: CandidatePolicyManifest,
    chunk_id: str,
    payload: StructuralPayload,
) -> DynamicEventPlan:
    event_id = "m4-real-smoke-insert-event"
    update = CorpusUpdateIdentity(
        event_id=event_id,
        payload_hash=stable_m4_digest(
            "m4-real-smoke-event-payload-v1",
            json.dumps(payload.manifest, sort_keys=True, separators=(",", ":")),
            manifest.policy_hash,
            str(previous_epoch_id),
        ),
        update_kind=UpdateKind.INSERT,
        previous_published_epoch_id=previous_epoch_id,
        candidate_policy_id=manifest.policy_id,
    )
    return DynamicEventPlan(
        update=update,
        inserted_chunk_version_ids=(chunk_id,),
        deactivated_chunk_version_ids=(),
        registered_claim_ids=(_CLAIM_ID,),
        claim_registry_snapshot_id=manifest.claim_registry_snapshot_id,
    )


def _scalar(
    connection: Connection[Any],
    statement: str,
    values: tuple[object, ...] = (),
) -> int:
    row = connection.execute(statement, values).fetchone()
    if row is None:
        raise ValidationError("smoke SQL scalar query returned no row")
    return int(row[0])


def _row_counts(connection: Connection[Any], epoch_id: int) -> dict[str, int]:
    tables = {
        "jobs": "groundloop_semantic_job",
        "attempts": "groundloop_semantic_job_attempt",
        "channel_hits": "groundloop_impact_channel_hit",
        "admitted_pairs": "groundloop_admitted_pair",
        "discovery_results": "groundloop_m4_discovery_result",
        "working_observation_deltas": "groundloop_working_observation_delta",
        "verification_executions": "groundloop_m4_verification_execution",
    }
    return {
        name: _scalar(
            connection,
            f"SELECT count(*) FROM {table} WHERE epoch_id = %s"
            if name not in {"attempts", "verification_executions"}
            else (
                "SELECT count(*) FROM groundloop_semantic_job_attempt AS attempt "
                "JOIN groundloop_semantic_job AS job USING (job_id) "
                "WHERE job.epoch_id = %s"
                if name == "attempts"
                else "SELECT count(*) FROM groundloop_m4_verification_execution "
                "AS execution JOIN groundloop_semantic_job AS job "
                "ON job.job_id = execution.job_id WHERE job.epoch_id = %s"
            ),
            (epoch_id,),
        )
        for name, table in tables.items()
    }


def _artifact_counts(connection: Connection[Any]) -> dict[str, int]:
    tables = {
        "model_artifacts": "groundloop_model_artifact",
        "prompt_artifacts": "groundloop_prompt_artifact",
        "chunker_artifacts": "groundloop_chunker_artifact",
        "role_embedding_artifacts": "groundloop_m4_role_embedding_artifact",
        "channel_hits": "groundloop_impact_channel_hit",
        "admitted_pairs": "groundloop_admitted_pair",
        "discovery_results": "groundloop_m4_discovery_result",
        "semantic_observations": "groundloop_semantic_observation",
        "verification_executions": "groundloop_m4_verification_execution",
        "pair_judgments": "groundloop_pair_judgment",
    }
    return {
        name: _scalar(
            connection,
            f"SELECT count(*) FROM {table}",
        )
        for name, table in tables.items()
    }


def _assert_surface_a(connection: Connection[Any], epoch_id: int) -> dict[str, str]:
    claim_mismatch = _scalar(
        connection,
        """
        SELECT count(*) FROM (
            (SELECT claim_id, support_count, refute_count, best_support_score,
                    best_refute_score, supporting_observation_ids,
                    refuting_observation_ids, status::text
             FROM groundloop_m4_working_claim_state WHERE epoch_id = %s
             EXCEPT ALL
             SELECT claim_id, support_count, refute_count, best_support_score,
                    best_refute_score, supporting_observation_ids,
                    refuting_observation_ids, status::text
             FROM groundloop_m4_claim_state_oracle WHERE epoch_id = %s)
            UNION ALL
            (SELECT claim_id, support_count, refute_count, best_support_score,
                    best_refute_score, supporting_observation_ids,
                    refuting_observation_ids, status::text
             FROM groundloop_m4_claim_state_oracle WHERE epoch_id = %s
             EXCEPT ALL
             SELECT claim_id, support_count, refute_count, best_support_score,
                    best_refute_score, supporting_observation_ids,
                    refuting_observation_ids, status::text
             FROM groundloop_m4_working_claim_state WHERE epoch_id = %s)
        ) AS mismatch
        """,
        (epoch_id, epoch_id, epoch_id, epoch_id),
    )
    answer_mismatch = _scalar(
        connection,
        """
        SELECT count(*) FROM (
            (SELECT answer_version_id, required_claim_count, supported_count,
                    unsupported_count, refuted_count, conflicted_count, status::text
             FROM groundloop_m4_working_answer_state WHERE epoch_id = %s
             EXCEPT ALL
             SELECT answer_version_id, required_claim_count, supported_count,
                    unsupported_count, refuted_count, conflicted_count, status::text
             FROM groundloop_m4_answer_state_oracle WHERE epoch_id = %s)
            UNION ALL
            (SELECT answer_version_id, required_claim_count, supported_count,
                    unsupported_count, refuted_count, conflicted_count, status::text
             FROM groundloop_m4_answer_state_oracle WHERE epoch_id = %s
             EXCEPT ALL
             SELECT answer_version_id, required_claim_count, supported_count,
                    unsupported_count, refuted_count, conflicted_count, status::text
             FROM groundloop_m4_working_answer_state WHERE epoch_id = %s)
        ) AS mismatch
        """,
        (epoch_id, epoch_id, epoch_id, epoch_id),
    )
    if claim_mismatch or answer_mismatch:
        raise ValidationError("Surface A SQL oracle differs from working state")
    row = connection.execute(
        """
        SELECT claim.status::text, answer.status::text
        FROM groundloop_published_claim_state AS claim
        CROSS JOIN groundloop_published_answer_state AS answer
        WHERE claim.claim_id = %s AND claim.valid_to_epoch IS NULL
          AND answer.answer_version_id = %s AND answer.valid_to_epoch IS NULL
        """,
        (_CLAIM_ID, _ANSWER_ID),
    ).fetchone()
    if row is None:
        raise ValidationError("strict published state is absent after sealing")
    return {"claim_status": str(row[0]), "answer_status": str(row[1])}


def _assert_surfaces_b_c(
    connection: Connection[Any],
    *,
    ports: PostgresM4ApplicationPorts,
    epoch_id: int,
) -> dict[str, int]:
    epoch = ports.runtime_store.read_epoch(epoch_id)
    if epoch.state is not RuntimeEpochState.SEALED:
        raise ValidationError("Surface B runtime epoch is not SEALED")
    if any(job.open for job in epoch.jobs) or any(
        not scope.closed for scope in epoch.discovery_scopes
    ):
        raise ValidationError("Surface B contains open jobs or discovery scopes")
    if not epoch.jobs or any(len(job.attempts) != 1 for job in epoch.jobs):
        raise ValidationError("Surface B expected one attempt per bounded smoke job")
    evaluation_rows = connection.execute(
        """
        SELECT object_type, object_id, evaluation_state,
               confirmed_as_of_epoch, open_required_job_count,
               discovery_scope_open, updated_revision
        FROM groundloop_object_evaluation
        WHERE epoch_id = %s ORDER BY object_type, object_id
        """,
        (epoch_id,),
    ).fetchall()
    if len(evaluation_rows) != 2 or any(
        (
            str(row[2]),
            int(row[3]),
            int(row[4]),
            bool(row[5]),
            int(row[6]),
        )
        != ("complete", epoch_id, 0, False, epoch.revision)
        for row in evaluation_rows
    ):
        raise ValidationError("Surface C sealed evaluation state is incomplete")
    return {
        "epoch_revision": epoch.revision,
        "job_count": len(epoch.jobs),
        "discovery_scope_count": len(epoch.discovery_scopes),
        "evaluation_row_count": len(evaluation_rows),
    }


def _assert_provenance(
    connection: Connection[Any],
    *,
    epoch_id: int,
    manifest: CandidatePolicyManifest,
    bundle: PinnedM3AdapterBundle,
) -> dict[str, int]:
    role_rows = connection.execute(
        """
        SELECT embedding_role, count(*)
        FROM groundloop_m4_role_embedding_artifact
        GROUP BY embedding_role ORDER BY embedding_role
        """
    ).fetchall()
    role_counts = {str(role): int(count) for role, count in role_rows}
    if role_counts != {"chunk_passage": 1, "claim_query": 1}:
        raise ValidationError("durable role-embedding provenance is incomplete")
    execution = connection.execute(
        """
        SELECT execution.model_artifact_id, execution.prompt_artifact_id,
               execution.execution_spec_hash, cardinality(execution.raw_logits),
               judgment.source_kind, judgment.derived_label
        FROM groundloop_m4_verification_execution AS execution
        JOIN groundloop_semantic_observation AS observation
          USING (observation_id)
        JOIN groundloop_pair_judgment AS judgment
          ON judgment.claim_id = observation.subject_id
         AND judgment.chunk_version_id = observation.chunk_version_id
        JOIN groundloop_semantic_job AS job ON job.job_id = execution.job_id
         AND judgment.source_artifact_id = job.result_artifact_id
        WHERE job.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    if execution is None:
        raise ValidationError("durable verifier execution/judgment is absent")
    expected = (
        bundle.verifier.spec.model_artifact.artifact_id,
        bundle.verifier.spec.prompt_artifact.artifact_id,
        manifest.verifier_execution_spec_hash,
        3,
        "model",
    )
    actual = (
        str(execution[0]),
        str(execution[1]),
        str(execution[2]).strip(),
        int(execution[3]),
        str(execution[4]),
    )
    if actual != expected:
        raise ValidationError("durable verifier provenance differs from pinned spec")
    return {
        "claim_role_artifacts": role_counts["claim_query"],
        "chunk_role_artifacts": role_counts["chunk_passage"],
        "verification_execution_rows": 1,
        "pair_judgment_rows": 1,
    }


def _prepare_registry(
    connection: Connection[Any],
    *,
    bundle: PinnedM3AdapterBundle,
    registry: PostgresM4ArtifactRegistry,
    manifest: CandidatePolicyManifest,
) -> None:
    service = M4AdmissionEmbeddingService(bundle.embeddings)
    artifact = service.embed_for_admission(
        claims=(ClaimEmbeddingInput(_CLAIM_ID, _CLAIM_TEXT),)
    ).claims[0]
    seed = registry.seed_claim_admission_index(
        manifest=manifest,
        artifacts=(artifact,),
        claim_texts={_CLAIM_ID: _CLAIM_TEXT},
    )
    if (seed.inserted_role_artifacts, seed.inserted_index_rows) != (1, 1):
        raise ValidationError("fresh smoke claim registry was not installed once")


def _event_manifest(
    *,
    schema_name: str,
    server: PostgresAdmissionServerIdentity,
    manifest: CandidatePolicyManifest,
    first: EventRunResult,
    replay: EventRunResult,
    first_composition: _Composition,
    replay_composition: _Composition,
    counts: Mapping[str, int],
    artifact_counts: Mapping[str, int],
    exact_states: Mapping[str, str],
    surface: Mapping[str, int],
    provenance: Mapping[str, int],
    model_config: PinnedM3ReuseConfig,
) -> dict[str, object]:
    verifier_spec = first_composition.verifier.adapter.spec
    return {
        "schema_version": _SCHEMA_VERSION,
        "scope": "bounded-integration-smoke-not-model-quality",
        "downloads_allowed": False,
        "database_connection_autocommit": True,
        "reconnected_with_fresh_ports_for_replay": True,
        "temporary_schema": schema_name,
        "postgres_version": server.postgres_version,
        "pgvector_version": server.pgvector_version,
        "candidate_policy_id": manifest.policy_id,
        "candidate_policy_hash": manifest.policy_hash,
        "embedding_model_artifact_id": manifest.embedding_model_artifact_id,
        "embedding_model_tree_sha256": (model_config.embedding_snapshot_tree_sha256),
        "verifier_model_artifact_id": (verifier_spec.model_artifact.artifact_id),
        "verifier_checkpoint_tree_sha256": (
            model_config.verifier_checkpoint_tree_sha256
        ),
        "verifier_prompt_artifact_id": verifier_spec.prompt_artifact.artifact_id,
        "verifier_prompt_template_sha256": verifier_spec.prompt_artifact.template_hash,
        "calibration_version": verifier_spec.calibration_version,
        "calibration_artifact_sha256": verifier_spec.calibration_artifact_sha256,
        "decision_policy_version": verifier_spec.decision_policy.policy_version,
        "decision_policy_hash": decision_policy_hash(verifier_spec.decision_policy),
        "verifier_execution_spec_hash": manifest.verifier_execution_spec_hash,
        "impact_discovery_execution_spec_hash": (
            first_composition.identity.impact_discovery_hash
        ),
        "frontier_retrieval_execution_spec_hash": (
            first_composition.identity.frontier_retrieval_hash
        ),
        "first_execution": {
            "event_id": first.event_id,
            "epoch_id": first.epoch_id,
            "state": first.state.value,
            "publication_id": first.publication_id,
            "discovery_calls": first.discovery_call_count,
            "verifier_requests": first_composition.verifier.request_count,
            "verifier_backend_pair_calls": (
                first_composition.verifier.backend_pair_calls
            ),
            "admission_embedding_requests": (
                first_composition.embeddings.request_count
            ),
        },
        "fresh_process_replay": {
            "state": replay.state.value,
            "publication_id": replay.publication_id,
            "discovery_calls": replay.discovery_call_count,
            "verifier_requests": replay_composition.verifier.request_count,
            "verifier_backend_pair_calls": (
                replay_composition.verifier.backend_pair_calls
            ),
            "admission_embedding_requests": replay_composition.embeddings.request_count,
            "database_projection_unchanged": True,
        },
        "durable_epoch_rows": dict(sorted(counts.items())),
        "durable_artifact_counts": dict(sorted(artifact_counts.items())),
        "artifact_counts_unchanged_on_replay": True,
        "exact_grounding": dict(exact_states),
        "exact_surfaces": {
            "surface_a_grounding": True,
            "surface_b_coordination": True,
            "surface_c_evaluation": True,
            **dict(surface),
        },
        "durable_model_provenance": dict(provenance),
    }


def run_m4_real_postgres_smoke(
    config: M4RealPostgresSmokeConfig,
) -> M4RealPostgresSmokeResult:
    """Run the opt-in M4.5 smoke and return its machine-readable manifest."""
    model_config = PinnedM3ReuseConfig.load(config.resolved_model_config_path)
    availability = inspect_local_artifacts(
        model_config, artifact_root=config.artifact_root
    )
    if not availability.available:
        raise SmokeUnavailableError("; ".join(availability.problems))
    schema_name = f"{config.schema_prefix}_{uuid.uuid4().hex}"
    try:
        connection = psycopg.connect(_psycopg_url(config.database_url), autocommit=True)
    except psycopg.Error as error:
        raise SmokeUnavailableError(f"PostgreSQL is unavailable: {error}") from error

    with connection:
        _create_schema(connection, schema_name)
        try:
            server = PostgresAdmissionServerIdentity.inspect(connection)
            chunker = FixedCharChunker()
            bundle = build_pinned_m3_adapters(
                model_config, artifact_root=config.artifact_root
            )
            registry = PostgresM4ArtifactRegistry(connection)
            _register_static_artifacts(registry, bundle, chunker)
            base_epoch_id = _seed_b0(connection, config=model_config, chunker=chunker)
            lexical_config = load_frozen_lexical_v1(config.repo_root)
            registry_snapshot_id = stable_m4_digest(
                "m4-real-smoke-claim-registry-v1", _CLAIM_ID, _CLAIM_TEXT
            )
            manifest, vector_config = _build_manifest(
                bundle=bundle,
                server=server,
                registry_snapshot_id=registry_snapshot_id,
                lexical_config_hash=lexical_config.config_hash,
            )
            _prepare_registry(
                connection,
                bundle=bundle,
                registry=registry,
                manifest=manifest,
            )
            ports_for_policy = PostgresM4ApplicationPorts(
                connection, structural_payloads={}
            )
            if not ports_for_policy.runtime_store.register_candidate_policy(manifest):
                raise ValidationError("fresh smoke candidate policy was replayed")
            payload, chunk_id = _insert_payload(chunker)
            event = _event(
                previous_epoch_id=base_epoch_id,
                manifest=manifest,
                chunk_id=chunk_id,
                payload=payload,
            )
            payloads = {event.update.event_id: payload}
            first_composition = _compose(
                connection,
                bundle=bundle,
                registry=registry,
                manifest=manifest,
                vector_config=vector_config,
                server=server,
                lexical_config_path=config.resolved_lexical_config_path,
                payloads=payloads,
            )
            first = first_composition.application.run_event(event)
            if first.state is not EventRunState.SEALED:
                raise ValidationError(f"real M4 insertion did not seal: {first}")
            if first.verifier_call_count != 1:
                raise ValidationError("bounded real insertion must verify one pair")
            first_composition.ports.check_grounding(first.epoch_id)
            exact_states = _assert_surface_a(connection, first.epoch_id)
            surface = _assert_surfaces_b_c(
                connection,
                ports=first_composition.ports,
                epoch_id=first.epoch_id,
            )
            provenance = _assert_provenance(
                connection,
                epoch_id=first.epoch_id,
                manifest=manifest,
                bundle=bundle,
            )
            counts_before_replay = _row_counts(connection, first.epoch_id)
            artifacts_before_replay = _artifact_counts(connection)

            # A distinct PostgreSQL session plus newly loaded adapters/ports
            # models a process restart.  The sealed-event read must short-circuit
            # before either empirical model boundary receives a request.
            with psycopg.connect(
                _psycopg_url(config.database_url), autocommit=True
            ) as replay_connection:
                replay_connection.execute(
                    sql.SQL("SET search_path TO {}, public").format(
                        sql.Identifier(schema_name)
                    )
                )
                replay_server = PostgresAdmissionServerIdentity.inspect(
                    replay_connection
                )
                if replay_server != server:
                    raise ValidationError("replay PostgreSQL server identity drift")
                replay_bundle = build_pinned_m3_adapters(
                    model_config, artifact_root=config.artifact_root
                )
                replay_registry = PostgresM4ArtifactRegistry(replay_connection)
                replay_composition = _compose(
                    replay_connection,
                    bundle=replay_bundle,
                    registry=replay_registry,
                    manifest=manifest,
                    vector_config=vector_config,
                    server=replay_server,
                    lexical_config_path=config.resolved_lexical_config_path,
                    payloads=payloads,
                )
                replay = replay_composition.application.run_event(event)
                if replay.state is not EventRunState.REPLAYED:
                    raise ValidationError("sealed event did not return exact replay")
                if (
                    replay.discovery_call_count != 0
                    or replay.verifier_call_count != 0
                    or replay_composition.embeddings.request_count != 0
                    or replay_composition.verifier.request_count != 0
                    or replay_composition.verifier.backend_pair_calls != 0
                ):
                    raise ValidationError("exact replay performed semantic model work")
                counts_after_replay = _row_counts(replay_connection, first.epoch_id)
                if counts_after_replay != counts_before_replay:
                    raise ValidationError("exact replay changed durable event rows")
                artifacts_after_replay = _artifact_counts(replay_connection)
                if artifacts_after_replay != artifacts_before_replay:
                    raise ValidationError("exact replay changed durable artifacts")
                replay_epoch = replay_composition.ports.runtime_store.read_epoch(
                    first.epoch_id
                )
                if replay_epoch.revision != surface["epoch_revision"]:
                    raise ValidationError("exact replay changed the sealed revision")
                _assert_surface_a(replay_connection, replay.epoch_id)
                _assert_surfaces_b_c(
                    replay_connection,
                    ports=replay_composition.ports,
                    epoch_id=replay.epoch_id,
                )

            output = _event_manifest(
                schema_name=schema_name,
                server=server,
                manifest=manifest,
                first=first,
                replay=replay,
                first_composition=first_composition,
                replay_composition=replay_composition,
                counts=counts_after_replay,
                artifact_counts=artifacts_after_replay,
                exact_states=exact_states,
                surface=surface,
                provenance=provenance,
                model_config=model_config,
            )
            return M4RealPostgresSmokeResult(output)
        finally:
            if not config.keep_schema:
                _drop_schema(connection, schema_name)


def write_smoke_manifest(result: M4RealPostgresSmokeResult, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(result.to_json(), encoding="utf-8")
