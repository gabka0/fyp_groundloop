from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.errors import EventConflictError, ValidationError
from groundloop.m4.admission.fresh import PostgresExactFreshFrontierRetriever
from groundloop.m4.contracts import CandidatePolicyManifest, VectorIndexKind
from groundloop.m4.persistence import PostgresM4RuntimeStore
from groundloop.postgres import apply_m2_schema

MODEL_ARTIFACT_ID = "embedding-fresh-frontier-v1"
POLICY_ID = "fresh-frontier-policy-v1"
CLAIM_ID = "claim-fresh"
CLAIM_ROLE_HASH = hashlib.sha256(b"claim-role-v1").hexdigest()
CHUNK_ROLE_HASH = hashlib.sha256(b"chunk-role-v1").hexdigest()


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _database_url() -> str:
    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip("live PostgreSQL is required for fresh-frontier tests")
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


@pytest.fixture
def fresh_connection() -> Iterator[Connection[tuple[Any, ...]]]:
    connection = psycopg.connect(_database_url(), autocommit=True)
    schema = f"groundloop_m4_fresh_{uuid.uuid4().hex}"
    try:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
        )
        with connection.transaction():
            apply_m2_schema(connection)
        yield connection
    finally:
        connection.execute("SET search_path TO public")
        connection.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(schema)
            )
        )
        connection.close()


def _manifest() -> CandidatePolicyManifest:
    return CandidatePolicyManifest.build(
        policy_id=POLICY_ID,
        embedding_model_artifact_id=MODEL_ARTIFACT_ID,
        claim_role_template_hash=CLAIM_ROLE_HASH,
        chunk_role_template_hash=CHUNK_ROLE_HASH,
        vector_method_version="exact-fresh-frontier-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=_digest("brute-force-pgvector"),
        vector_search_config_hash=_digest("cosine-distance-chunk-id"),
        lexical_method_version="postgres-lexical-v1",
        lexical_config_hash=_digest("lexical-v1"),
        lexical_postgres_version="test-server",
        lexical_regconfig_identity="simple",
        claim_registry_snapshot_id="registry-fresh-v1",
        claim_count=1,
        fusion_version="rank-interleave-v1",
        approximate_cap_per_inserted_chunk=2,
        frontier_depth=2,
        verifier_execution_spec_hash=_digest("verifier-execution"),
        decision_policy_version="decision-fresh-v1",
    )


def _vector_literal(axis: int) -> str:
    values = [0.0] * 384
    values[axis] = 1.0
    return "[" + ",".join(format(value, ".17g") for value in values) + "]"


def _seed_world(
    connection: Connection[tuple[Any, ...]],
) -> tuple[CandidatePolicyManifest, int]:
    base_epoch = int(
        connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, structural_status, semantic_status,
                evaluation_state, publication_mode, sealed_at
            ) VALUES (
                'fresh-base', %s, 'committed', 'sealed', 'complete',
                'strict', now()
            ) RETURNING epoch_id
            """,
            (_digest("fresh-base"),),
        ).fetchone()[0]
    )
    working_epoch = int(
        connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, structural_status, semantic_status,
                evaluation_state, publication_mode
            ) VALUES (
                'fresh-working', %s, 'committed', 'pending', 'pending',
                'provisional'
            ) RETURNING epoch_id
            """,
            (_digest("fresh-working"),),
        ).fetchone()[0]
    )
    connection.execute(
        """
        INSERT INTO groundloop_model_artifact (
            model_artifact_id, task, provider, model_id, immutable_revision,
            tokenizer_revision, license_id, config_hash, metadata
        ) VALUES (%s, 'embedding', 'test', 'test-bge', 'revision-1',
                  'tokenizer-1', 'test-only', %s, '{}'::jsonb)
        """,
        (MODEL_ARTIFACT_ID, _digest("embedding-config")),
    )
    connection.execute(
        """
        INSERT INTO groundloop_decision_policy (
            policy_version, support_threshold, refute_threshold,
            tie_rule_version, valid_from_epoch
        ) VALUES ('decision-fresh-v1', 0.7, 0.7, 'v1', %s)
        """,
        (base_epoch,),
    )
    manifest = _manifest()
    PostgresM4RuntimeStore(connection).register_candidate_policy(manifest)
    connection.execute(
        """
        INSERT INTO groundloop_m4_update (
            epoch_id, update_kind, candidate_policy_id,
            previous_published_epoch_id, registry_snapshot_id, manifest
        ) VALUES (%s, 'insert', %s, %s, %s, '{}'::jsonb)
        """,
        (
            working_epoch,
            manifest.policy_id,
            base_epoch,
            manifest.claim_registry_snapshot_id,
        ),
    )
    with connection.transaction():
        connection.execute(
            """
            INSERT INTO groundloop_question (
                question_id, text, created_epoch
            ) VALUES ('question-fresh', 'Which chunks are relevant?', %s)
            """,
            (base_epoch,),
        )
        connection.execute(
            """
            INSERT INTO groundloop_answer_version (
                answer_version_id, question_id, text, generator_model_id,
                generator_model_version, prompt_version, created_epoch
            ) VALUES ('answer-fresh', 'question-fresh', 'An answer', 'generator',
                      'v1', 'prompt-v1', %s)
            """,
            (base_epoch,),
        )
        connection.execute(
            """
            INSERT INTO groundloop_claim (
                claim_id, answer_version_id, text, extractor_model_id,
                extractor_model_version, extractor_prompt_version, required
            ) VALUES (%s, 'answer-fresh', 'The relevant claim', 'extractor',
                      'v1', 'prompt-v1', true)
            """,
            (CLAIM_ID,),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m4_claim_registry_member (
                claim_registry_snapshot_id, claim_id, member_ordinal
            ) VALUES ('registry-fresh-v1', %s, 0)
            """,
            (CLAIM_ID,),
        )
    connection.execute(
        "INSERT INTO groundloop_document VALUES ('document-fresh', NULL, 'test')"
    )
    connection.execute(
        """
        INSERT INTO groundloop_document_version (
            document_version_id, document_id, content_hash, valid_from_epoch
        ) VALUES ('document-version-fresh', 'document-fresh', %s, %s)
        """,
        (_digest("document-content"), base_epoch),
    )
    for index, chunk_id in enumerate(
        ("chunk-a", "chunk-b", "chunk-c", "chunk-missing")
    ):
        connection.execute(
            """
            INSERT INTO groundloop_chunk_version (
                chunk_version_id, document_version_id, chunk_index, text,
                text_hash, chunker_version, valid_from_epoch
            ) VALUES (%s, 'document-version-fresh', %s, %s, %s, 'fixed-v1', %s)
            """,
            (
                chunk_id,
                index,
                f"Text for {chunk_id}",
                _digest(f"text-{chunk_id}"),
                base_epoch,
            ),
        )
    _insert_role_artifact(
        connection,
        subject_id=CLAIM_ID,
        role="claim_query",
        role_hash=CLAIM_ROLE_HASH,
        vector_axis=0,
    )
    _insert_role_artifact(
        connection,
        subject_id="chunk-a",
        role="chunk_passage",
        role_hash=CHUNK_ROLE_HASH,
        vector_axis=0,
    )
    _insert_role_artifact(
        connection,
        subject_id="chunk-b",
        role="chunk_passage",
        role_hash=CHUNK_ROLE_HASH,
        vector_axis=0,
    )
    _insert_role_artifact(
        connection,
        subject_id="chunk-c",
        role="chunk_passage",
        role_hash=CHUNK_ROLE_HASH,
        vector_axis=1,
    )
    return manifest, working_epoch


def _insert_role_artifact(
    connection: Connection[tuple[Any, ...]],
    *,
    subject_id: str,
    role: str,
    role_hash: str,
    vector_axis: int,
    adapter_suffix: str = "primary",
) -> str:
    artifact_id = _digest(
        f"artifact:{subject_id}:{role}:{role_hash}:{adapter_suffix}"
    )
    claim_id = subject_id if role == "claim_query" else None
    chunk_id = subject_id if role == "chunk_passage" else None
    connection.execute(
        """
        INSERT INTO groundloop_m4_role_embedding_artifact (
            artifact_id, subject_id, embedding_role, claim_id,
            chunk_version_id, model_artifact_id, model_id, model_revision,
            tokenizer_revision, role_template_hash, input_hash, vector_hash,
            adapter_spec_hash, token_count, max_tokens, truncated, embedding
        ) VALUES (
            %s, %s, %s, %s, %s, %s, 'test-bge', 'revision-1',
            'tokenizer-1', %s, %s, %s, %s, 4, 512, false, %s::vector
        )
        """,
        (
            artifact_id,
            subject_id,
            role,
            claim_id,
            chunk_id,
            MODEL_ARTIFACT_ID,
            role_hash,
            _digest(f"input:{subject_id}:{role}"),
            _digest(f"vector:{subject_id}:{role}:{vector_axis}"),
            _digest(f"adapter:{adapter_suffix}"),
            _vector_literal(vector_axis),
        ),
    )
    return artifact_id


def test_exact_retrieval_orders_ties_and_binds_complete_artifacts(
    fresh_connection: Connection[tuple[Any, ...]],
) -> None:
    manifest, epoch_id = _seed_world(fresh_connection)
    retriever = PostgresExactFreshFrontierRetriever(fresh_connection)

    incomplete = retriever.retrieve(
        epoch_id=epoch_id,
        claim_id=CLAIM_ID,
        manifest=manifest,
        excluded_chunk_ids=("chunk-a", "chunk-a"),
        limit=2,
    )
    assert incomplete.excluded_chunk_ids == ("chunk-a",)
    assert tuple(
        candidate.pair.chunk_version_id for candidate in incomplete.candidates
    ) == ("chunk-b", "chunk-c")
    assert incomplete.candidates[0].score == pytest.approx(1.0)
    assert incomplete.active_chunk_count == 4
    assert incomplete.compatible_chunk_count == 3
    assert incomplete.missing_active_chunk_ids == ("chunk-missing",)
    assert not incomplete.complete
    assert (
        retriever.retrieve(
            epoch_id=epoch_id,
            claim_id=CLAIM_ID,
            manifest=manifest,
            excluded_chunk_ids=("chunk-a",),
            limit=2,
        ).artifact_hash
        == incomplete.artifact_hash
    )

    tie_order = retriever.retrieve(
        epoch_id=epoch_id,
        claim_id=CLAIM_ID,
        manifest=manifest,
        limit=2,
    )
    assert tuple(
        candidate.pair.chunk_version_id for candidate in tie_order.candidates
    ) == ("chunk-a", "chunk-b")

    _insert_role_artifact(
        fresh_connection,
        subject_id="chunk-missing",
        role="chunk_passage",
        role_hash=CHUNK_ROLE_HASH,
        vector_axis=1,
    )
    complete = retriever.retrieve(
        epoch_id=epoch_id,
        claim_id=CLAIM_ID,
        manifest=manifest,
        excluded_chunk_ids=("chunk-a",),
        limit=2,
    )
    assert complete.complete
    assert complete.compatible_chunk_count == complete.active_chunk_count == 4
    assert complete.missing_active_chunk_ids == ()
    assert complete.artifact_hash != incomplete.artifact_hash


def test_retrieval_rejects_ambiguous_claim_artifacts(
    fresh_connection: Connection[tuple[Any, ...]],
) -> None:
    manifest, epoch_id = _seed_world(fresh_connection)
    _insert_role_artifact(
        fresh_connection,
        subject_id=CLAIM_ID,
        role="claim_query",
        role_hash=CLAIM_ROLE_HASH,
        vector_axis=0,
        adapter_suffix="ambiguous",
    )
    with pytest.raises(ValidationError, match="ambiguous compatible query"):
        PostgresExactFreshFrontierRetriever(fresh_connection).retrieve(
            epoch_id=epoch_id,
            claim_id=CLAIM_ID,
            manifest=manifest,
            limit=2,
        )


def test_retrieval_rejects_ambiguous_chunk_artifacts(
    fresh_connection: Connection[tuple[Any, ...]],
) -> None:
    manifest, epoch_id = _seed_world(fresh_connection)
    _insert_role_artifact(
        fresh_connection,
        subject_id="chunk-a",
        role="chunk_passage",
        role_hash=CHUNK_ROLE_HASH,
        vector_axis=0,
        adapter_suffix="ambiguous",
    )
    with pytest.raises(ValidationError, match="ambiguous compatible passage"):
        PostgresExactFreshFrontierRetriever(fresh_connection).retrieve(
            epoch_id=epoch_id,
            claim_id=CLAIM_ID,
            manifest=manifest,
            limit=2,
        )


def test_retrieval_rejects_unregistered_policy_content(
    fresh_connection: Connection[tuple[Any, ...]],
) -> None:
    manifest, epoch_id = _seed_world(fresh_connection)
    drifted = CandidatePolicyManifest.build(
        policy_id=manifest.policy_id,
        embedding_model_artifact_id=manifest.embedding_model_artifact_id,
        claim_role_template_hash=manifest.claim_role_template_hash,
        chunk_role_template_hash=_digest("drifted-chunk-role"),
        vector_method_version=manifest.vector_method_version,
        vector_index_kind=manifest.vector_index_kind,
        vector_index_build_config_hash=manifest.vector_index_build_config_hash,
        vector_search_config_hash=manifest.vector_search_config_hash,
        lexical_method_version=manifest.lexical_method_version,
        lexical_config_hash=manifest.lexical_config_hash,
        lexical_postgres_version=manifest.lexical_postgres_version,
        lexical_regconfig_identity=manifest.lexical_regconfig_identity,
        claim_registry_snapshot_id=manifest.claim_registry_snapshot_id,
        claim_count=manifest.claim_count,
        fusion_version=manifest.fusion_version,
        approximate_cap_per_inserted_chunk=(
            manifest.approximate_cap_per_inserted_chunk
        ),
        frontier_depth=manifest.frontier_depth,
        verifier_execution_spec_hash=manifest.verifier_execution_spec_hash,
        decision_policy_version=manifest.decision_policy_version,
    )
    with pytest.raises(EventConflictError, match="differs from the registry"):
        PostgresExactFreshFrontierRetriever(fresh_connection).retrieve(
            epoch_id=epoch_id,
            claim_id=CLAIM_ID,
            manifest=drifted,
            limit=2,
        )


def test_retrieval_rejects_an_ambient_transaction(
    fresh_connection: Connection[tuple[Any, ...]],
) -> None:
    manifest, epoch_id = _seed_world(fresh_connection)
    retriever = PostgresExactFreshFrontierRetriever(fresh_connection)
    with fresh_connection.transaction():
        with pytest.raises(ValidationError, match="outside an active transaction"):
            retriever.retrieve(
                epoch_id=epoch_id,
                claim_id=CLAIM_ID,
                manifest=manifest,
                limit=2,
            )
