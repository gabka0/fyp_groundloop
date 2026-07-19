"""Live gates for immutable M4 role embeddings and admission seeding."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from typing import Any

import pytest
from psycopg import Connection, errors

from groundloop.ai.contracts import (
    ChunkDraft,
    ModelTask,
    PromptArtifact,
)
from groundloop.ai.embeddings.fake import DeterministicFakeEmbedder
from groundloop.ai.persistence import ChunkerArtifactRecord
from groundloop.domain import normalized_text_hash
from groundloop.errors import ArtifactConflictError, ValidationError
from groundloop.m4.admission.vector import ClaimRoleVector
from groundloop.m4.artifacts import PostgresM4ArtifactRegistry
from groundloop.m4.contracts import CandidatePolicyManifest, VectorIndexKind
from groundloop.m4.models.contracts import (
    CLAIM_ROLE_TEMPLATE,
    ChunkVectorArtifact,
    ClaimVectorArtifact,
    EmbeddingAdapterSpec,
    EmbeddingRole,
    RoleEmbeddingProvenance,
    sha256_text,
    vector_sha256,
)
from groundloop.m4.models.embedding import ClaimEmbeddingInput, M4BgeRoleAdapter
from groundloop.postgres import record_epoch, temporary_m2_schema


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _vector_literal(vector: tuple[float, ...]) -> str:
    return "[" + ",".join(format(value, ".17g") for value in vector) + "]"


CLAIM_TEXTS = {
    "claim-a": "Nimbus supports exact incremental grounding.",
    "claim-b": "GroundLoop records immutable neural observations.",
    "claim-c": "Admission completeness is policy relative.",
}
CHUNK_TEXTS = {
    "chunk-a": "Nimbus documentation describes incremental grounding.",
    "chunk-b": "GroundLoop stores versioned observations for audit.",
}


def _seed_subjects(connection: Connection[Any]) -> int:
    epoch_id, created = record_epoch(
        connection,
        event_id="role-artifact-seed",
        payload_hash=_hash("role-artifact-seed"),
    )
    assert created
    connection.execute(
        """
        UPDATE groundloop_epoch
        SET structural_status = 'committed', semantic_status = 'sealed',
            evaluation_state = 'complete', publication_mode = 'strict',
            sealed_at = now()
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    )
    connection.execute(
        "INSERT INTO groundloop_document VALUES ('doc', 'fixture://doc', 'test')"
    )
    connection.execute(
        """
        INSERT INTO groundloop_document_version VALUES
          ('dv', 'doc', %s, %s, NULL)
        """,
        (_hash("document"), epoch_id),
    )
    for index, (chunk_id, text) in enumerate(CHUNK_TEXTS.items()):
        connection.execute(
            """
            INSERT INTO groundloop_chunk_version VALUES
              (%s, 'dv', %s, %s, %s, 'fixed-char-v1', %s, NULL)
            """,
            (chunk_id, index, text, normalized_text_hash(text), epoch_id),
        )
    connection.execute(
        "INSERT INTO groundloop_question VALUES ('question', 'What changed?', %s)",
        (epoch_id,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_answer_version VALUES
          ('answer', 'question', 'Grounded answer.', 'generator', 'rev',
           'prompt', %s)
        """,
        (epoch_id,),
    )
    for claim_id, text in CLAIM_TEXTS.items():
        connection.execute(
            """
            INSERT INTO groundloop_claim VALUES
              (%s, 'answer', %s, 'extractor', 'rev', 'prompt', true)
            """,
            (claim_id, text),
        )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return epoch_id


def _artifacts() -> tuple[
    DeterministicFakeEmbedder,
    tuple[ClaimVectorArtifact, ...],
    tuple[ChunkVectorArtifact, ...],
]:
    backend = DeterministicFakeEmbedder()
    adapter = M4BgeRoleAdapter(
        backend,
        EmbeddingAdapterSpec(backend.model_artifact, backend.dimension),
    )
    claim_artifacts = adapter.embed_claims(
        tuple(
            ClaimEmbeddingInput(claim_id, text)
            for claim_id, text in CLAIM_TEXTS.items()
        )
    )
    chunk_artifacts = adapter.embed_chunks(
        tuple(
            ChunkDraft(
                chunk_id,
                "dv",
                index,
                text,
                normalized_text_hash(text),
                "fixed-char-v1-1200-no-overlap",
            )
            for index, (chunk_id, text) in enumerate(CHUNK_TEXTS.items())
        )
    )
    return backend, claim_artifacts, chunk_artifacts


def _prompt(template: str = "premise={evidence}\nhypothesis={claim}") -> PromptArtifact:
    return PromptArtifact(
        artifact_id="m4-verification-prompt",
        task=ModelTask.VERIFICATION,
        version="v1",
        template=template,
        template_hash=_hash(template),
        decoding_config_hash=_hash("deterministic-verifier"),
    )


def _manifest(
    *,
    model_artifact_id: str,
    claim_role_template_hash: str,
    chunk_role_template_hash: str,
    snapshot_id: str,
    claim_count: int,
) -> CandidatePolicyManifest:
    return CandidatePolicyManifest.build(
        policy_id=f"candidate:{snapshot_id}",
        embedding_model_artifact_id=model_artifact_id,
        claim_role_template_hash=claim_role_template_hash,
        chunk_role_template_hash=chunk_role_template_hash,
        vector_method_version="reverse-bge-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=_hash("exact-vector-build"),
        vector_search_config_hash=_hash("exact-vector-search"),
        lexical_method_version="lexical-v1",
        lexical_config_hash=_hash("lexical-v1"),
        lexical_postgres_version="16",
        lexical_regconfig_identity="simple",
        claim_registry_snapshot_id=snapshot_id,
        claim_count=claim_count,
        fusion_version="rank-interleave-v1",
        approximate_cap_per_inserted_chunk=8,
        frontier_depth=4,
        verifier_execution_spec_hash=_hash("verifier-execution"),
        decision_policy_version="policy-v1",
    )


def test_registry_content_validates_base_and_full_role_artifacts(
    live_connection: Connection[Any],
) -> None:
    with temporary_m2_schema(live_connection):
        _seed_subjects(live_connection)
        backend, claim_artifacts, chunk_artifacts = _artifacts()
        registry = PostgresM4ArtifactRegistry(live_connection)
        chunker = ChunkerArtifactRecord(
            "fixed-char-v1-1200-no-overlap",
            "fixed-char-v1",
            "v1",
            _hash("fixed-char-v1:1200:no-overlap"),
        )
        prompt = _prompt()

        assert registry.register_model(backend.model_artifact)
        assert not registry.register_model(backend.model_artifact)
        assert registry.register_prompt(prompt)
        assert not registry.register_prompt(prompt)
        assert registry.register_chunker(chunker)
        assert not registry.register_chunker(chunker)
        assert registry.register_role_embedding(claim_artifacts[0])
        assert not registry.register_role_embedding(claim_artifacts[0])
        assert registry.register_role_embedding(chunk_artifacts[0])
        assert not registry.register_role_embedding(chunk_artifacts[0])

        claim = claim_artifacts[0]
        row = live_connection.execute(
            """
            SELECT subject_id, embedding_role, claim_id, chunk_version_id,
                   model_artifact_id, model_id, model_revision,
                   tokenizer_revision, role_template_hash, input_hash,
                   vector_hash, adapter_spec_hash, token_count, max_tokens,
                   truncated, format_type(attribute.atttypid, attribute.atttypmod)
            FROM groundloop_m4_role_embedding_artifact
            CROSS JOIN pg_attribute AS attribute
            WHERE artifact_id = %s
              AND attribute.attrelid =
                  'groundloop_m4_role_embedding_artifact'::regclass
              AND attribute.attname = 'embedding'
            """,
            (claim.provenance.artifact_id,),
        ).fetchone()
        assert row == (
            claim.provenance.subject_id,
            "claim_query",
            claim.provenance.subject_id,
            None,
            claim.provenance.model_artifact_id,
            claim.provenance.model_id,
            claim.provenance.model_revision,
            claim.provenance.tokenizer_revision,
            claim.provenance.role_template_hash,
            claim.provenance.input_hash,
            claim.provenance.vector_hash,
            claim.provenance.adapter_spec_hash,
            claim.provenance.token_count,
            claim.provenance.max_tokens,
            claim.provenance.truncated,
            "vector(384)",
        )

        with pytest.raises(ArtifactConflictError, match="model artifact"):
            registry.register_model(
                replace(backend.model_artifact, provider="drifted-provider")
            )
        changed_template = "premise={evidence}\nclaim={claim}"
        with pytest.raises(ArtifactConflictError, match="prompt artifact"):
            registry.register_prompt(
                replace(
                    prompt,
                    template=changed_template,
                    template_hash=_hash(changed_template),
                )
            )
        with pytest.raises(ArtifactConflictError, match="chunker artifact"):
            registry.register_chunker(replace(chunker, config_hash=_hash("drift")))

        conflicting = claim_artifacts[1]
        provenance = conflicting.provenance
        live_connection.execute(
            """
            INSERT INTO groundloop_m4_role_embedding_artifact (
                artifact_id, subject_id, embedding_role, claim_id,
                model_artifact_id, model_id, model_revision,
                tokenizer_revision, role_template_hash, input_hash,
                vector_hash, adapter_spec_hash, token_count, max_tokens,
                truncated, embedding
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                17, %s, %s, %s::vector
            )
            """,
            (
                provenance.artifact_id,
                provenance.subject_id,
                provenance.role.value,
                provenance.subject_id,
                provenance.model_artifact_id,
                provenance.model_id,
                provenance.model_revision,
                provenance.tokenizer_revision,
                provenance.role_template_hash,
                provenance.input_hash,
                provenance.vector_hash,
                provenance.adapter_spec_hash,
                provenance.max_tokens,
                provenance.truncated,
                _vector_literal(conflicting.vector.vector),
            ),
        )
        with pytest.raises(ArtifactConflictError, match="payload conflict"):
            registry.register_role_embedding(conflicting)

        with pytest.raises(errors.RaiseException, match="immutable M4 table"):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    UPDATE groundloop_m4_role_embedding_artifact
                    SET vector_hash = %s WHERE artifact_id = %s
                    """,
                    (_hash("changed"), claim.provenance.artifact_id),
                )

        short_vector = (1.0, 0.0)
        short_input_hash = sha256_text(
            CLAIM_ROLE_TEMPLATE.format(text=CLAIM_TEXTS["claim-c"])
        )
        short_vector_hash = vector_sha256(short_vector)
        short_spec_hash = _hash("short-adapter")
        short_id = RoleEmbeddingProvenance.build_artifact_id(
            subject_id="claim-c",
            role=EmbeddingRole.CLAIM_QUERY,
            model_artifact_id=backend.model_artifact.artifact_id,
            model_id=backend.model_artifact.model_id,
            model_revision=backend.model_artifact.immutable_revision,
            tokenizer_revision=backend.model_artifact.tokenizer_revision,
            role_template_hash=sha256_text(CLAIM_ROLE_TEMPLATE),
            input_hash=short_input_hash,
            vector_hash=short_vector_hash,
            adapter_spec_hash=short_spec_hash,
        )
        short_artifact = ClaimVectorArtifact(
            ClaimRoleVector("claim-c", short_vector, short_input_hash),
            RoleEmbeddingProvenance(
                artifact_id=short_id,
                subject_id="claim-c",
                role=EmbeddingRole.CLAIM_QUERY,
                model_artifact_id=backend.model_artifact.artifact_id,
                model_id=backend.model_artifact.model_id,
                model_revision=backend.model_artifact.immutable_revision,
                tokenizer_revision=backend.model_artifact.tokenizer_revision,
                role_template_hash=sha256_text(CLAIM_ROLE_TEMPLATE),
                input_hash=short_input_hash,
                vector_hash=short_vector_hash,
                adapter_spec_hash=short_spec_hash,
                token_count=None,
                max_tokens=512,
                truncated=False,
            ),
        )
        with pytest.raises(ValidationError, match="exactly 384 dimensions"):
            registry.register_role_embedding(short_artifact)


def test_claim_admission_seeding_is_atomic_and_exactly_replayable(
    live_connection: Connection[Any],
) -> None:
    with temporary_m2_schema(live_connection):
        _seed_subjects(live_connection)
        backend, claim_artifacts, chunk_artifacts = _artifacts()
        registry = PostgresM4ArtifactRegistry(live_connection)
        registry.register_model(backend.model_artifact)
        manifest = _manifest(
            model_artifact_id=backend.model_artifact.artifact_id,
            claim_role_template_hash=claim_artifacts[0].provenance.role_template_hash,
            chunk_role_template_hash=chunk_artifacts[0].provenance.role_template_hash,
            snapshot_id="registry-main",
            claim_count=2,
        )
        selected = claim_artifacts[:2]
        selected_texts = {
            artifact.vector.claim_id: CLAIM_TEXTS[artifact.vector.claim_id]
            for artifact in selected
        }

        first = registry.seed_claim_admission_index(
            manifest=manifest,
            artifacts=selected,
            claim_texts=selected_texts,
        )
        assert (
            first.inserted_role_artifacts,
            first.reused_role_artifacts,
            first.inserted_index_rows,
            first.reused_index_rows,
            first.claim_count,
        ) == (2, 0, 2, 0, 2)
        replay = registry.seed_claim_admission_index(
            manifest=manifest,
            artifacts=selected,
            claim_texts=selected_texts,
        )
        assert (
            replay.inserted_role_artifacts,
            replay.reused_role_artifacts,
            replay.inserted_index_rows,
            replay.reused_index_rows,
        ) == (0, 2, 0, 2)

        rows = live_connection.execute(
            """
            SELECT index.claim_id, index.embedding_model_artifact_id,
                   index.claim_role_template_hash,
                   index.embedding_input_hash,
                   index.lexical_tsv =
                       to_tsvector('simple'::regconfig, claim.text),
                   artifact.artifact_id IS NOT NULL
            FROM groundloop_m4_claim_admission_index AS index
            JOIN groundloop_claim AS claim USING (claim_id)
            LEFT JOIN groundloop_m4_role_embedding_artifact AS artifact
              ON artifact.claim_id = index.claim_id
             AND artifact.model_artifact_id = index.embedding_model_artifact_id
             AND artifact.role_template_hash = index.claim_role_template_hash
             AND artifact.input_hash = index.embedding_input_hash
            WHERE index.claim_registry_snapshot_id = 'registry-main'
            ORDER BY index.claim_id
            """
        ).fetchall()
        assert rows == [
            (
                artifact.vector.claim_id,
                backend.model_artifact.artifact_id,
                artifact.provenance.role_template_hash,
                artifact.provenance.input_hash,
                True,
                True,
            )
            for artifact in selected
        ]

        drifted_texts = dict(selected_texts)
        drifted_texts[selected[0].vector.claim_id] = "changed claim"
        with pytest.raises(ArtifactConflictError, match="claim text differs"):
            registry.seed_claim_admission_index(
                manifest=manifest,
                artifacts=selected,
                claim_texts=drifted_texts,
            )

        conflict_artifact = claim_artifacts[2]
        conflict_manifest = _manifest(
            model_artifact_id=backend.model_artifact.artifact_id,
            claim_role_template_hash=(
                conflict_artifact.provenance.role_template_hash
            ),
            chunk_role_template_hash=chunk_artifacts[0].provenance.role_template_hash,
            snapshot_id="registry-conflict",
            claim_count=1,
        )
        live_connection.execute(
            """
            INSERT INTO groundloop_m4_claim_admission_index (
                claim_registry_snapshot_id, claim_id,
                embedding_model_artifact_id, claim_role_template_hash,
                embedding_input_hash, embedding, lexical_tsv
            ) VALUES (
                'registry-conflict', %s, %s, %s, %s, %s::vector,
                to_tsvector('simple'::regconfig, %s)
            )
            """,
            (
                conflict_artifact.vector.claim_id,
                backend.model_artifact.artifact_id,
                conflict_artifact.provenance.role_template_hash,
                _hash("wrong input"),
                _vector_literal(conflict_artifact.vector.vector),
                CLAIM_TEXTS[conflict_artifact.vector.claim_id],
            ),
        )
        assert live_connection.execute(
            """
            SELECT count(*) FROM groundloop_m4_role_embedding_artifact
            WHERE artifact_id = %s
            """,
            (conflict_artifact.provenance.artifact_id,),
        ).fetchone() == (0,)
        with pytest.raises(ArtifactConflictError, match="index key"):
            registry.seed_claim_admission_index(
                manifest=conflict_manifest,
                artifacts=(conflict_artifact,),
                claim_texts={
                    conflict_artifact.vector.claim_id: CLAIM_TEXTS[
                        conflict_artifact.vector.claim_id
                    ]
                },
            )
        assert live_connection.execute(
            """
            SELECT count(*) FROM groundloop_m4_role_embedding_artifact
            WHERE artifact_id = %s
            """,
            (conflict_artifact.provenance.artifact_id,),
        ).fetchone() == (0,)


def test_database_rejects_role_model_drift_and_typed_subject_drift(
    live_connection: Connection[Any],
) -> None:
    with temporary_m2_schema(live_connection):
        _seed_subjects(live_connection)
        backend, claim_artifacts, _ = _artifacts()
        registry = PostgresM4ArtifactRegistry(live_connection)
        registry.register_model(backend.model_artifact)
        artifact = claim_artifacts[0]
        provenance = artifact.provenance

        with pytest.raises(errors.RaiseException, match="model provenance"):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    INSERT INTO groundloop_m4_role_embedding_artifact (
                        artifact_id, subject_id, embedding_role, claim_id,
                        model_artifact_id, model_id, model_revision,
                        tokenizer_revision, role_template_hash, input_hash,
                        vector_hash, adapter_spec_hash, token_count, max_tokens,
                        truncated, embedding
                    ) VALUES (
                        %s, %s, 'claim_query', %s, %s, 'wrong-model', %s,
                        %s, %s, %s, %s, %s, NULL, %s, false, %s::vector
                    )
                    """,
                    (
                        _hash("wrong-model-row"),
                        provenance.subject_id,
                        provenance.subject_id,
                        provenance.model_artifact_id,
                        provenance.model_revision,
                        provenance.tokenizer_revision,
                        provenance.role_template_hash,
                        provenance.input_hash,
                        provenance.vector_hash,
                        provenance.adapter_spec_hash,
                        provenance.max_tokens,
                        _vector_literal(artifact.vector.vector),
                    ),
                )
        with pytest.raises(errors.CheckViolation):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    INSERT INTO groundloop_m4_role_embedding_artifact (
                        artifact_id, subject_id, embedding_role, chunk_version_id,
                        model_artifact_id, model_id, model_revision,
                        tokenizer_revision, role_template_hash, input_hash,
                        vector_hash, adapter_spec_hash, token_count, max_tokens,
                        truncated, embedding
                    ) VALUES (
                        %s, %s, 'claim_query', 'chunk-a', %s, %s, %s, %s,
                        %s, %s, %s, %s, NULL, %s, false, %s::vector
                    )
                    """,
                    (
                        _hash("wrong-subject-row"),
                        provenance.subject_id,
                        provenance.model_artifact_id,
                        provenance.model_id,
                        provenance.model_revision,
                        provenance.tokenizer_revision,
                        provenance.role_template_hash,
                        provenance.input_hash,
                        provenance.vector_hash,
                        provenance.adapter_spec_hash,
                        provenance.max_tokens,
                        _vector_literal(artifact.vector.vector),
                    ),
                )
