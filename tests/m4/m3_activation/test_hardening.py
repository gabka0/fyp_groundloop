from __future__ import annotations

import hashlib
import os
import queue
import threading
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import cast

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.ai.application import M3Application, M3ApplicationConfig
from groundloop.ai.chunking import FixedCharChunker
from groundloop.ai.claim_extraction import DeterministicClaimExtractor
from groundloop.ai.contracts import (
    ChunkDraft,
    EmbeddingRecord,
    PipelineRunManifest,
    QueryKind,
    stable_digest,
)
from groundloop.ai.embeddings import DeterministicFakeEmbedder
from groundloop.ai.embeddings.common import EmbeddedQuery
from groundloop.ai.generation import DeterministicAnswerGenerator
from groundloop.ai.persistence import PostgresArtifactStore
from groundloop.ai.verification import DeterministicFakeVerifier
from groundloop.domain import DecisionPolicy
from groundloop.errors import ArtifactConflictError, GroundLoopError, ValidationError
from groundloop.m4.m3_activation import (
    M3M4ActivationReceipt,
    activate_published_m3_run,
    inspect_m3_m4_activation,
)
from groundloop.m4.models.contracts import (
    EmbeddingAdapterSpec,
    VerificationAdapterSpec,
)
from groundloop.m4.models.embedding import M4BgeRoleAdapter

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_HASH = hashlib.sha256(b"m3-m4-hardening-v1").hexdigest()


@dataclass(frozen=True, slots=True)
class _PublishedRun:
    manifest: PipelineRunManifest
    embeddings: M4BgeRoleAdapter
    verifier_spec: VerificationAdapterSpec


class _CountingEmbedder(DeterministicFakeEmbedder):
    """Count inference calls and optionally pause the first one."""

    def __init__(
        self,
        entered: threading.Event | None = None,
        release: threading.Event | None = None,
    ) -> None:
        super().__init__()
        self._entered = entered
        self._release = release
        self._paused = False
        self.call_count = 0

    def _record(self) -> None:
        self.call_count += 1
        if self._entered is None or self._release is None or self._paused:
            return
        self._paused = True
        self._entered.set()
        if not self._release.wait(timeout=10):
            raise AssertionError("activation inference pause was not released")

    def embed(self, chunks: tuple[ChunkDraft, ...]) -> tuple[EmbeddingRecord, ...]:
        self._record()
        return super().embed(chunks)

    def embed_query(self, text: str, query_kind: QueryKind) -> EmbeddedQuery:
        self._record()
        return super().embed_query(text, query_kind)


def _publish_m3_run(
    connection: Connection[tuple[object, ...]],
    tmp_path: Path,
    *,
    include_empty_document: bool = False,
    include_unicode_empty_documents: bool = False,
    include_symlink_alias: bool = False,
) -> _PublishedRun:
    corpus = tmp_path / "hardening-corpus"
    corpus.mkdir()
    for index, text in enumerate(
        (
            "GroundLoop maintains claims relative to versioned evidence.",
            "Semantic observations are immutable and versioned.",
            "Relational maintenance begins after observations are stored.",
        ),
        start=1,
    ):
        (corpus / f"source-{index}.txt").write_text(text + "\n", encoding="utf-8")
    if include_empty_document:
        (corpus / "source-empty.txt").write_text("", encoding="utf-8")
    if include_unicode_empty_documents:
        (corpus / "z.txt").write_text("", encoding="utf-8")
        (corpus / "é [1].txt").write_text("", encoding="utf-8")
    if include_symlink_alias:
        (corpus / "alias-first.txt").symlink_to("source-3.txt")

    policy = DecisionPolicy("m3-hardening-policy-v1", 0.8, 0.8)
    embedder = DeterministicFakeEmbedder()
    verifier = DeterministicFakeVerifier()
    application = M3Application(
        store=PostgresArtifactStore(connection),
        chunker=FixedCharChunker(),
        embedder=embedder,
        generator=DeterministicAnswerGenerator(),
        extractor=DeterministicClaimExtractor(),
        verifier=verifier,
        config=M3ApplicationConfig(
            config_hash=CONFIG_HASH,
            question_top_k=1,
            claim_top_k=1,
            policy=policy,
        ),
    )
    manifest = application.register(corpus, "What does GroundLoop maintain?")
    embedding_spec = EmbeddingAdapterSpec(
        model_artifact=embedder.model_artifact,
        dimension=embedder.dimension,
    )
    verifier_spec = VerificationAdapterSpec(
        model_artifact=verifier.model_artifact,
        prompt_artifact=verifier.prompt_artifact,
        calibration_version=verifier.calibration_version,
        calibration_artifact_sha256=None,
        temperature=verifier.temperature,
        max_length=verifier.max_length,
        decision_policy=policy,
    )
    return _PublishedRun(
        manifest=manifest,
        embeddings=M4BgeRoleAdapter(DeterministicFakeEmbedder(), embedding_spec),
        verifier_spec=verifier_spec,
    )


def test_activation_accepts_mixed_empty_and_nonempty_m3_documents(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(
        m3_m4_activation_connection,
        tmp_path,
        include_empty_document=True,
    )
    counts = m3_m4_activation_connection.execute(
        """
        SELECT (SELECT count(*) FROM groundloop_document_version),
               (SELECT count(*) FROM groundloop_chunk_version)
        """
    ).fetchone()
    assert counts is not None
    assert cast(int, counts[0]) == cast(int, counts[1]) + 1

    receipt = _activate(m3_m4_activation_connection, published)

    assert receipt.chunk_count == cast(int, counts[1])
    assert receipt.claim_count == len(published.manifest.claims)
    assert not receipt.replayed


def test_activation_reproduces_unicode_path_order_with_empty_documents(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(
        m3_m4_activation_connection,
        tmp_path,
        include_unicode_empty_documents=True,
    )
    counts = m3_m4_activation_connection.execute(
        """
        SELECT (SELECT count(*) FROM groundloop_document_version),
               (SELECT count(*) FROM groundloop_chunk_version)
        """
    ).fetchone()
    assert counts is not None
    assert cast(int, counts[0]) == cast(int, counts[1]) + 2

    receipt = _activate(m3_m4_activation_connection, published)

    assert receipt.chunk_count == cast(int, counts[1])
    assert not receipt.replayed


def test_source_rejects_coherent_empty_document_substitution(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(
        m3_m4_activation_connection,
        tmp_path,
        include_empty_document=True,
    )
    empty_row = m3_m4_activation_connection.execute(
        """
        SELECT version.document_id, version.document_version_id
        FROM groundloop_document_version AS version
        LEFT JOIN groundloop_chunk_version AS chunk
          USING (document_version_id)
        WHERE chunk.chunk_version_id IS NULL
        """
    ).fetchone()
    assert empty_row is not None
    document_id, old_version_id = str(empty_row[0]), str(empty_row[1])
    replacement_content_hash = hashlib.sha256(b"coherent substitution").hexdigest()
    replacement_version_id = stable_digest(
        "document-version-v1",
        document_id,
        replacement_content_hash,
        FixedCharChunker().artifact_id,
    )
    m3_m4_activation_connection.execute(
        "ALTER TABLE groundloop_document_version DISABLE TRIGGER USER"
    )
    m3_m4_activation_connection.execute(
        """
        UPDATE groundloop_document_version
        SET document_version_id = %s, content_hash = %s
        WHERE document_version_id = %s
        """,
        (replacement_version_id, replacement_content_hash, old_version_id),
    )
    m3_m4_activation_connection.execute(
        "ALTER TABLE groundloop_pipeline_run DISABLE TRIGGER USER"
    )
    m3_m4_activation_connection.execute(
        """
        UPDATE groundloop_pipeline_run
        SET manifest = jsonb_set(
            manifest,
            '{new_artifact_ids}',
            (
                SELECT jsonb_agg(
                    CASE WHEN value = %s THEN %s ELSE value END
                    ORDER BY CASE WHEN value = %s THEN %s ELSE value END
                )
                FROM jsonb_array_elements_text(manifest->'new_artifact_ids')
            )
        )
        """,
        (
            old_version_id,
            replacement_version_id,
            old_version_id,
            replacement_version_id,
        ),
    )

    with pytest.raises(
        ArtifactConflictError,
        match="M3 corpus identity/provenance drift",
    ):
        inspect_m3_m4_activation(
            m3_m4_activation_connection,
            run_id=published.manifest.run_id,
        )


def test_activation_accepts_nonempty_symlink_alias_source_uris(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(
        m3_m4_activation_connection,
        tmp_path,
        include_symlink_alias=True,
    )
    document_count, distinct_uri_count = cast(
        tuple[int, int],
        m3_m4_activation_connection.execute(
            """
            SELECT count(*), count(DISTINCT source_uri)
            FROM groundloop_document
            """
        ).fetchone(),
    )
    assert document_count == distinct_uri_count + 1

    receipt = _activate(m3_m4_activation_connection, published)

    assert receipt.chunk_count == document_count
    assert not receipt.replayed


def _activate(
    connection: Connection[tuple[object, ...]],
    published: _PublishedRun,
    *,
    embeddings: M4BgeRoleAdapter | None = None,
    verifier_spec: VerificationAdapterSpec | None = None,
    approximate_cap_per_inserted_chunk: int | None = None,
    frontier_depth: int = 1,
) -> M3M4ActivationReceipt:
    return activate_published_m3_run(
        connection,
        run_id=published.manifest.run_id,
        embeddings=embeddings or published.embeddings,
        verifier_spec=verifier_spec or published.verifier_spec,
        repo_root=REPO_ROOT,
        approximate_cap_per_inserted_chunk=approximate_cap_per_inserted_chunk,
        frontier_depth=frontier_depth,
    )


@pytest.mark.parametrize(
    "drift", ("calibration", "temperature", "max_length", "adapter_version")
)
def test_replay_rejects_verifier_calibration_or_temperature_drift(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
    drift: str,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    _activate(m3_m4_activation_connection, published)
    if drift == "calibration":
        changed = replace(
            published.verifier_spec,
            calibration_version=published.verifier_spec.calibration_version + "-drift",
        )
        message = "M4 verifier calibration differs from M3 run"
    elif drift == "temperature":
        changed = replace(
            published.verifier_spec,
            temperature=published.verifier_spec.temperature + 0.125,
        )
        message = "M4 verifier temperature differs from M3 run"
    elif drift == "max_length":
        changed = replace(
            published.verifier_spec,
            max_length=published.verifier_spec.max_length + 1,
        )
        message = "M4 verifier max_length/input identity differs from M3 run"
    else:
        assert drift == "adapter_version"
        changed = replace(
            published.verifier_spec,
            adapter_version="m4-m3-verifier-adapter-drift",
        )
        message = "M4 verifier adapter version is not frozen"

    with pytest.raises(ArtifactConflictError, match=message):
        _activate(
            m3_m4_activation_connection,
            published,
            verifier_spec=changed,
        )


def test_replay_rejects_embedding_adapter_version_drift(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    _activate(m3_m4_activation_connection, published)
    backend = DeterministicFakeEmbedder()
    changed = M4BgeRoleAdapter(
        backend,
        replace(
            published.embeddings.spec,
            adapter_version="m4-m3-bge-role-adapter-drift",
        ),
    )

    with pytest.raises(
        ArtifactConflictError,
        match="M4 embedding adapter version is not frozen",
    ):
        _activate(
            m3_m4_activation_connection,
            published,
            embeddings=changed,
        )


@pytest.mark.parametrize("drift", ("admission_cap", "frontier_depth"))
def test_replay_rejects_admission_policy_drift(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
    drift: str,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    first = _activate(m3_m4_activation_connection, published)
    assert first.claim_count == 1

    with pytest.raises(
        ArtifactConflictError,
        match="activation replay configuration differs from frozen policy",
    ):
        if drift == "admission_cap":
            _activate(
                m3_m4_activation_connection,
                published,
                approximate_cap_per_inserted_chunk=2,
            )
        else:
            _activate(
                m3_m4_activation_connection,
                published,
                frontier_depth=2,
            )


def test_activation_rejects_incomplete_local_m4_schema(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    m3_m4_activation_connection.execute(
        "DROP TABLE groundloop_m4_claim_admission_index CASCADE"
    )

    with pytest.raises(
        ValidationError,
        match="selected schema lacks required M3/M4 relations: "
        "groundloop_m4_claim_admission_index",
    ):
        _activate(m3_m4_activation_connection, published)


@pytest.mark.parametrize("corruption", ("candidate_chunk", "result_candidate"))
def test_activation_rejects_candidate_or_result_link_corruption(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
    corruption: str,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    if corruption == "candidate_chunk":
        candidate = m3_m4_activation_connection.execute(
            """
            SELECT candidate_id, chunk_version_id
            FROM groundloop_retrieval_candidate
            WHERE query_kind = 'claim'
            """
        ).fetchone()
        assert candidate is not None
        replacement_chunk = next(
            chunk_id
            for chunk_id in published.manifest.chunk_version_ids
            if chunk_id != str(candidate[1])
        )
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_retrieval_candidate DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_retrieval_candidate
            SET chunk_version_id = %s
            WHERE candidate_id = %s
            """,
            (replacement_chunk, str(candidate[0])),
        )
        message = "M3 retrieval candidate closure drift"
    else:
        question_candidate = m3_m4_activation_connection.execute(
            """
            SELECT candidate_id
            FROM groundloop_retrieval_candidate
            WHERE query_kind = 'question'
            """
        ).fetchone()
        assert question_candidate is not None
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_verification_execution DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_verification_execution
            SET candidate_id = %s
            """,
            (str(question_candidate[0]),),
        )
        message = "M3 verification references unknown candidate"

    with pytest.raises(ArtifactConflictError, match=message):
        inspect_m3_m4_activation(
            m3_m4_activation_connection,
            run_id=published.manifest.run_id,
        )


@pytest.mark.parametrize(
    "corruption",
    (
        "unknown_run",
        "staged_run",
        "failed_run",
        "wrong_schema_version",
        "run_manifest_identity",
        "run_input_recipe",
        "local_global_claim_mapping",
        "generation_model_payload",
        "generation_prompt_template",
        "generation_decoding_config",
        "missing_claim_execution",
        "extra_claim",
        "missing_candidate",
        "extra_candidate",
        "missing_chunk_embedding",
        "extra_active_chunk",
        "missing_verification_execution",
        "extra_semantic_observation",
        "missing_currency",
        "currency_revision",
        "missing_claim_state",
        "missing_certificate",
        "extra_epoch",
        "second_published_run",
    ),
)
def test_source_closure_falsifiers_fail_before_m4_writes(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
    corruption: str,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    run_id = published.manifest.run_id
    selected_run_id = run_id

    if corruption == "unknown_run":
        selected_run_id = "run-does-not-exist"
    elif corruption == "staged_run":
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_pipeline_run DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_pipeline_run
            SET status = 'staged', answer_version_id = NULL,
                semantic_epoch_id = NULL, completed_at = NULL
            """
        )
    elif corruption == "failed_run":
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_pipeline_run DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_pipeline_run
            SET status = 'failed', failure_code = 'injected', completed_at = now()
            """
        )
    elif corruption == "wrong_schema_version":
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_pipeline_run DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            """
            ALTER TABLE groundloop_pipeline_run
            DROP CONSTRAINT groundloop_pipeline_run_schema_version_check
            """
        )
        m3_m4_activation_connection.execute(
            "UPDATE groundloop_pipeline_run SET schema_version = 'm3-v0'"
        )
    elif corruption == "run_manifest_identity":
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_pipeline_run DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            "UPDATE groundloop_pipeline_run SET config_hash = %s",
            ("0" * 64,),
        )
    elif corruption == "run_input_recipe":
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_pipeline_run DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_pipeline_run
            SET config_hash = %s,
                manifest = jsonb_set(
                    manifest,
                    '{config_hash}',
                    to_jsonb(%s::text)
                )
            """,
            ("0" * 64, "0" * 64),
        )
    elif corruption == "local_global_claim_mapping":
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_pipeline_run DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_pipeline_run
            SET manifest = jsonb_set(
                jsonb_set(
                    manifest,
                    '{claims,0,local_claim_id}',
                    to_jsonb('claim-local-tampered'::text)
                ),
                '{extraction,claims,0,local_claim_id}',
                to_jsonb('claim-local-tampered'::text)
            )
            """
        )
    elif corruption == "generation_model_payload":
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_model_artifact DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_model_artifact
            SET provider = 'tampered-provider'
            WHERE task = 'generation'
            """
        )
    elif corruption == "generation_prompt_template":
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_prompt_artifact DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_prompt_artifact
            SET template = 'tampered prompt template'
            WHERE task = 'generation'
            """
        )
    elif corruption == "generation_decoding_config":
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_generation_execution DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_generation_execution
            SET decoding_config_hash = %s
            """,
            ("4" * 64,),
        )
    elif corruption == "missing_claim_execution":
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_claim_extraction_execution DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            "DELETE FROM groundloop_claim_extraction_execution"
        )
    elif corruption == "extra_claim":
        m3_m4_activation_connection.execute(
            """
            INSERT INTO groundloop_claim (
                claim_id, answer_version_id, text, extractor_model_id,
                extractor_model_version, extractor_prompt_version, required
            )
            SELECT 'claim-foreign', answer_version_id, 'foreign claim',
                   extractor_model_id, extractor_model_version,
                   extractor_prompt_version, false
            FROM groundloop_claim
            ORDER BY claim_id
            LIMIT 1
            """
        )
    elif corruption == "missing_candidate":
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_retrieval_candidate DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            "DELETE FROM groundloop_retrieval_candidate WHERE query_kind = 'question'"
        )
    elif corruption == "extra_candidate":
        m3_m4_activation_connection.execute(
            """
            INSERT INTO groundloop_retrieval_candidate (
                candidate_id, run_id, query_kind, query_id, claim_id,
                chunk_version_id, embedding_model_artifact_id,
                method_version, score, rank
            )
            SELECT candidate_id || '-foreign', run_id, query_kind,
                   query_id || '-foreign', NULL, chunk_version_id,
                   embedding_model_artifact_id, method_version, score, rank + 100
            FROM groundloop_retrieval_candidate
            WHERE query_kind = 'question'
            ORDER BY candidate_id
            LIMIT 1
            """
        )
    elif corruption == "missing_chunk_embedding":
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_chunk_embedding DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            """
            DELETE FROM groundloop_chunk_embedding
            WHERE chunk_version_id = (
                SELECT chunk_version_id
                FROM groundloop_chunk_embedding
                ORDER BY chunk_version_id
                LIMIT 1
            )
            """
        )
    elif corruption == "extra_active_chunk":
        epoch_row = m3_m4_activation_connection.execute(
            "SELECT epoch_id FROM groundloop_epoch"
        ).fetchone()
        assert epoch_row is not None
        epoch_id = cast(int, epoch_row[0])
        m3_m4_activation_connection.execute(
            """
            INSERT INTO groundloop_document (
                document_id, source_uri, authority_class
            ) VALUES ('document-foreign', 'file:///foreign.txt', 'local-m3')
            """
        )
        m3_m4_activation_connection.execute(
            """
            INSERT INTO groundloop_document_version (
                document_version_id, document_id, content_hash, valid_from_epoch
            ) VALUES ('document-version-foreign', 'document-foreign', %s, %s)
            """,
            ("1" * 64, epoch_id),
        )
        m3_m4_activation_connection.execute(
            """
            INSERT INTO groundloop_chunk_version (
                chunk_version_id, document_version_id, chunk_index, text,
                text_hash, chunker_version, valid_from_epoch
            ) VALUES (
                'chunk-foreign', 'document-version-foreign', 0, 'foreign',
                %s, 'fixed-char-v1', %s
            )
            """,
            ("2" * 64, epoch_id),
        )
    elif corruption == "missing_verification_execution":
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_verification_execution DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            "DELETE FROM groundloop_verification_execution"
        )
    elif corruption == "extra_semantic_observation":
        m3_m4_activation_connection.execute(
            """
            INSERT INTO groundloop_semantic_observation (
                observation_id, subject_kind, subject_id, chunk_version_id,
                task_type, support_score, refute_score, neutral_score,
                model_id, model_version, prompt_version, input_hash,
                produced_epoch, raw_output_hash
            )
            SELECT 'observation-foreign', subject_kind, subject_id,
                   chunk_version_id, task_type, support_score, refute_score,
                   neutral_score, model_id, model_version, prompt_version,
                   input_hash, produced_epoch, raw_output_hash
            FROM groundloop_semantic_observation
            ORDER BY observation_id
            LIMIT 1
            """
        )
    elif corruption == "missing_currency":
        m3_m4_activation_connection.execute(
            "DELETE FROM groundloop_observation_currency"
        )
    elif corruption == "currency_revision":
        m3_m4_activation_connection.execute(
            "UPDATE groundloop_observation_currency SET installed_revision = 1"
        )
    elif corruption == "missing_claim_state":
        m3_m4_activation_connection.execute(
            "DELETE FROM groundloop_claim_state_materialized"
        )
    elif corruption == "missing_certificate":
        m3_m4_activation_connection.execute("DELETE FROM groundloop_claim_certificate")
    elif corruption == "extra_epoch":
        m3_m4_activation_connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (
                'm3-foreign-epoch', %s, 0, 'committed', 'sealed',
                'complete', 'provisional', now()
            )
            """,
            ("3" * 64,),
        )
    else:
        assert corruption == "second_published_run"
        m3_m4_activation_connection.execute(
            """
            INSERT INTO groundloop_pipeline_run (
                run_id, schema_version, status, config_hash, input_hash,
                corpus_hash, question_id, answer_version_id,
                semantic_epoch_id, manifest, completed_at
            )
            SELECT 'run-foreign', schema_version, status, config_hash,
                   input_hash, corpus_hash, question_id, answer_version_id,
                   semantic_epoch_id,
                   jsonb_set(
                       manifest, '{run_id}', to_jsonb('run-foreign'::text)
                   ),
                   now()
            FROM groundloop_pipeline_run
            WHERE run_id = %s
            """,
            (run_id,),
        )

    with pytest.raises(GroundLoopError):
        inspect_m3_m4_activation(
            m3_m4_activation_connection,
            run_id=selected_run_id,
        )
    counts = m3_m4_activation_connection.execute(
        """
        SELECT
            (SELECT count(*) FROM groundloop_m4_publication_head),
            (SELECT count(*) FROM groundloop_candidate_policy),
            (SELECT count(*) FROM groundloop_m4_claim_registry_snapshot)
        """
    ).fetchone()
    assert counts == (0, 0, 0)


def test_source_rejects_claim_candidate_without_verification(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    m3_m4_activation_connection.execute(
        "ALTER TABLE groundloop_pipeline_run DISABLE TRIGGER USER"
    )
    m3_m4_activation_connection.execute(
        """
        UPDATE groundloop_pipeline_run
        SET manifest = jsonb_set(manifest, '{verifications}', '[]'::jsonb)
        """
    )
    m3_m4_activation_connection.execute(
        "ALTER TABLE groundloop_verification_execution DISABLE TRIGGER USER"
    )
    m3_m4_activation_connection.execute("DELETE FROM groundloop_verification_execution")

    with pytest.raises(
        ArtifactConflictError,
        match="claim candidates and verifications are not an exact bijection",
    ):
        inspect_m3_m4_activation(
            m3_m4_activation_connection,
            run_id=published.manifest.run_id,
        )


@pytest.mark.parametrize(
    ("corruption", "message"),
    (
        ("missing_question", "M3 question retrieval has no evidence"),
        ("question_id", "M3 question retrieval query identity drift"),
        ("method", "M3 retrieval method/model/chunk provenance drift"),
        ("model", "M3 retrieval method/model/chunk provenance drift"),
        ("rank", "M3 retrieval ranks are not contiguous"),
        (
            "citation",
            "M3 answer citations escape question retrieval evidence",
        ),
    ),
)
def test_source_independently_rejects_coherent_question_retrieval_drift(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
    corruption: str,
    message: str,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    m3_m4_activation_connection.execute(
        "ALTER TABLE groundloop_pipeline_run DISABLE TRIGGER USER"
    )
    m3_m4_activation_connection.execute(
        "ALTER TABLE groundloop_retrieval_candidate DISABLE TRIGGER USER"
    )

    if corruption == "missing_question":
        m3_m4_activation_connection.execute(
            "DELETE FROM groundloop_retrieval_candidate WHERE query_kind = 'question'"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_pipeline_run
            SET manifest = jsonb_set(
                manifest,
                '{retrieval_candidates}',
                (manifest->'retrieval_candidates') - 0
            )
            """
        )
    elif corruption == "question_id":
        m3_m4_activation_connection.execute(
            "UPDATE groundloop_retrieval_candidate "
            "SET query_id = 'question-tampered' WHERE query_kind = 'question'"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_pipeline_run
            SET manifest = jsonb_set(
                manifest,
                '{retrieval_candidates,0,query_id}',
                to_jsonb('question-tampered'::text)
            )
            """
        )
    elif corruption == "method":
        m3_m4_activation_connection.execute(
            "UPDATE groundloop_retrieval_candidate "
            "SET method_version = 'tampered-retrieval-v1' "
            "WHERE query_kind = 'question'"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_pipeline_run
            SET manifest = jsonb_set(
                manifest,
                '{retrieval_candidates,0,method_version}',
                to_jsonb('tampered-retrieval-v1'::text)
            )
            """
        )
    elif corruption == "model":
        model_row = m3_m4_activation_connection.execute(
            "SELECT model_artifact_id FROM groundloop_model_artifact "
            "WHERE task = 'generation'"
        ).fetchone()
        assert model_row is not None
        model_id = str(model_row[0])
        m3_m4_activation_connection.execute(
            "UPDATE groundloop_retrieval_candidate "
            "SET embedding_model_artifact_id = %s WHERE query_kind = 'question'",
            (model_id,),
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_pipeline_run
            SET manifest = jsonb_set(
                manifest,
                '{retrieval_candidates,0,embedding_model_artifact_id}',
                to_jsonb(%s::text)
            )
            """,
            (model_id,),
        )
    elif corruption == "rank":
        m3_m4_activation_connection.execute(
            "UPDATE groundloop_retrieval_candidate "
            "SET rank = 2 WHERE query_kind = 'question'"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_pipeline_run
            SET manifest = jsonb_set(
                manifest,
                '{retrieval_candidates,0,rank}',
                '2'::jsonb
            )
            """
        )
    else:
        assert corruption == "citation"
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_answer_citation DISABLE TRIGGER USER"
        )
        chunk_row = m3_m4_activation_connection.execute(
            """
            SELECT chunk_version_id FROM groundloop_chunk_version
            WHERE chunk_version_id <> (
                SELECT chunk_version_id FROM groundloop_retrieval_candidate
                WHERE query_kind = 'question'
            )
            ORDER BY chunk_version_id
            LIMIT 1
            """
        ).fetchone()
        assert chunk_row is not None
        chunk_id = str(chunk_row[0])
        m3_m4_activation_connection.execute(
            "UPDATE groundloop_answer_citation SET chunk_version_id = %s",
            (chunk_id,),
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_pipeline_run
            SET manifest = jsonb_set(
                manifest,
                '{answer,cited_chunk_version_ids,0}',
                to_jsonb(%s::text)
            )
            """,
            (chunk_id,),
        )

    with pytest.raises(ArtifactConflictError, match=message):
        inspect_m3_m4_activation(
            m3_m4_activation_connection,
            run_id=published.manifest.run_id,
        )


def test_source_rejects_incomplete_manifest_artifact_partition(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    m3_m4_activation_connection.execute(
        "ALTER TABLE groundloop_pipeline_run DISABLE TRIGGER USER"
    )
    m3_m4_activation_connection.execute(
        """
        UPDATE groundloop_pipeline_run
        SET manifest = jsonb_set(
            manifest,
            '{new_artifact_ids}',
            (manifest->'new_artifact_ids') - 0
        )
        """
    )

    with pytest.raises(
        ArtifactConflictError,
        match="M3 manifest artifact ledger closure drift",
    ):
        inspect_m3_m4_activation(
            m3_m4_activation_connection,
            run_id=published.manifest.run_id,
        )


@pytest.mark.parametrize(
    ("corruption", "message"),
    (
        ("claim_role_vector", "M4 claim admission index closure drift"),
        ("chunk_role_vector", "M4 role embedding closure drift"),
        ("admission_vector", "M4 claim admission index closure drift"),
        ("admission_lexical", "M4 claim admission index closure drift"),
    ),
)
def test_activation_rejects_role_or_admission_payload_corruption(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
    corruption: str,
    message: str,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    _activate(m3_m4_activation_connection, published)

    if corruption == "claim_role_vector":
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_m4_role_embedding_artifact DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_m4_role_embedding_artifact
            SET embedding = (
                SELECT embedding
                FROM groundloop_m4_role_embedding_artifact
                WHERE embedding_role = 'chunk_passage'
                ORDER BY subject_id
                LIMIT 1
            )
            WHERE embedding_role = 'claim_query'
            """
        )
    elif corruption == "chunk_role_vector":
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_m4_role_embedding_artifact DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_m4_role_embedding_artifact
            SET embedding = (
                SELECT embedding
                FROM groundloop_m4_role_embedding_artifact
                WHERE embedding_role = 'claim_query'
                ORDER BY subject_id
                LIMIT 1
            )
            WHERE artifact_id = (
                SELECT artifact_id
                FROM groundloop_m4_role_embedding_artifact
                WHERE embedding_role = 'chunk_passage'
                ORDER BY subject_id
                LIMIT 1
            )
            """
        )
    elif corruption == "admission_vector":
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_m4_claim_admission_index DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_m4_claim_admission_index
            SET embedding = (
                SELECT embedding
                FROM groundloop_m4_role_embedding_artifact
                WHERE embedding_role = 'chunk_passage'
                ORDER BY subject_id
                LIMIT 1
            )
            """
        )
    else:
        assert corruption == "admission_lexical"
        m3_m4_activation_connection.execute(
            "ALTER TABLE groundloop_m4_claim_admission_index DISABLE TRIGGER USER"
        )
        m3_m4_activation_connection.execute(
            """
            UPDATE groundloop_m4_claim_admission_index
            SET lexical_tsv = to_tsvector('simple'::regconfig, 'corrupt payload')
            """
        )

    with pytest.raises(ArtifactConflictError, match=message):
        inspect_m3_m4_activation(
            m3_m4_activation_connection,
            run_id=published.manifest.run_id,
        )


def _database_url() -> str:
    value = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    assert value is not None
    return value.replace("postgresql+psycopg://", "postgresql://", 1)


def _wait_for_advisory_lock_waiter(
    connection: Connection[tuple[object, ...]], backend_pid: int
) -> None:
    deadline = time.monotonic() + 8.0
    yielded = threading.Event()
    while time.monotonic() < deadline:
        row = connection.execute(
            """
            SELECT count(*)
            FROM pg_locks
            WHERE pid = %s
              AND locktype = 'advisory'
              AND NOT granted
            """,
            (backend_pid,),
        ).fetchone()
        assert row is not None
        if cast(int, row[0]) == 1:
            return
        yielded.wait(0.01)
    pytest.fail("second activation did not wait on the activation advisory lock")


def test_concurrent_identical_activation_serializes_to_one_fresh_and_one_replay(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    schema_row = m3_m4_activation_connection.execute(
        "SELECT current_schema()"
    ).fetchone()
    assert schema_row is not None
    schema_name = str(schema_row[0])
    inference_entered = threading.Event()
    release_inference = threading.Event()
    second_pid: queue.Queue[int] = queue.Queue()
    outcomes: queue.Queue[tuple[str, M3M4ActivationReceipt | BaseException]] = (
        queue.Queue()
    )
    first_backend = _CountingEmbedder(inference_entered, release_inference)
    second_backend = _CountingEmbedder()

    def worker(label: str, backend: _CountingEmbedder) -> None:
        try:
            with psycopg.connect(_database_url(), autocommit=True) as connection:
                connection.execute(
                    sql.SQL("SET search_path TO {}, public").format(
                        sql.Identifier(schema_name)
                    )
                )
                connection.execute("SET lock_timeout TO '8s'")
                if label == "second":
                    second_pid.put(connection.info.backend_pid)
                embeddings = M4BgeRoleAdapter(
                    backend,
                    EmbeddingAdapterSpec(
                        model_artifact=backend.model_artifact,
                        dimension=backend.dimension,
                    ),
                )
                receipt = _activate(
                    cast(Connection[tuple[object, ...]], connection),
                    published,
                    embeddings=embeddings,
                )
                outcomes.put((label, receipt))
        except BaseException as error:  # pragma: no cover - asserted below
            outcomes.put((label, error))

    first_thread = threading.Thread(
        target=worker, args=("first", first_backend), daemon=True
    )
    second_thread = threading.Thread(
        target=worker, args=("second", second_backend), daemon=True
    )
    first_thread.start()
    assert inference_entered.wait(timeout=10)
    second_thread.start()
    try:
        _wait_for_advisory_lock_waiter(
            m3_m4_activation_connection,
            second_pid.get(timeout=5),
        )
    finally:
        release_inference.set()

    for thread in (first_thread, second_thread):
        thread.join(timeout=15)
        assert not thread.is_alive()
    results = (outcomes.get_nowait(), outcomes.get_nowait())
    errors = tuple(
        value for _label, value in results if isinstance(value, BaseException)
    )
    assert not errors, repr(errors)
    receipts = {label: cast(M3M4ActivationReceipt, value) for label, value in results}
    first = receipts["first"]
    second = receipts["second"]
    assert not first.replayed
    assert second.replayed
    assert first_backend.call_count > 0
    assert first.activation_embedding_request_count == 1
    assert second_backend.call_count == 0
    assert second.activation_embedding_request_count == 0
    assert first.run_id == second.run_id == published.manifest.run_id
    assert first.candidate_policy_id == second.candidate_policy_id
    assert first.candidate_policy_hash == second.candidate_policy_hash
    counts = m3_m4_activation_connection.execute(
        """
        SELECT
            (SELECT count(*) FROM groundloop_m4_publication_head),
            (SELECT count(*) FROM groundloop_m4_claim_registry_snapshot),
            (SELECT count(*) FROM groundloop_candidate_policy)
        """
    ).fetchone()
    assert counts == (1, 1, 1)
