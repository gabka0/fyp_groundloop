from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from pathlib import Path
from typing import cast

import pytest
from psycopg import Connection
from psycopg.types.json import Jsonb

from groundloop.ai.application import (
    InMemoryPublicationStore,
    M3Application,
    M3ApplicationConfig,
    PublicationStore,
)
from groundloop.ai.chunking import FixedCharChunker
from groundloop.ai.claim_extraction import DeterministicClaimExtractor
from groundloop.ai.contracts import PipelineRunManifest
from groundloop.ai.embeddings import DeterministicFakeEmbedder
from groundloop.ai.generation import DeterministicAnswerGenerator
from groundloop.ai.manifest import manifest_to_dict
from groundloop.ai.persistence import M3PublicationBundle, PostgresArtifactStore
from groundloop.ai.verification import DeterministicFakeVerifier
from groundloop.domain import DecisionPolicy
from groundloop.errors import ArtifactConflictError
from groundloop.m4.m3_activation import activate_published_m3_run
from groundloop.m4.models.contracts import (
    EmbeddingAdapterSpec,
    VerificationAdapterSpec,
)
from groundloop.m4.models.embedding import M4BgeRoleAdapter

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_HASH = hashlib.sha256(b"m3-m4-publish-guard-v1").hexdigest()


@dataclass(frozen=True, slots=True)
class _PublishedRun:
    manifest: PipelineRunManifest
    embedding_spec: EmbeddingAdapterSpec
    verifier_spec: VerificationAdapterSpec


def _application(
    store: PublicationStore,
) -> tuple[M3Application, EmbeddingAdapterSpec, VerificationAdapterSpec]:
    embedder = DeterministicFakeEmbedder()
    verifier = DeterministicFakeVerifier()
    policy = DecisionPolicy("m3-publish-guard-policy-v1", 0.8, 0.8)
    application = M3Application(
        store=store,
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
    return (
        application,
        EmbeddingAdapterSpec(
            model_artifact=embedder.model_artifact,
            dimension=embedder.dimension,
        ),
        VerificationAdapterSpec(
            model_artifact=verifier.model_artifact,
            prompt_artifact=verifier.prompt_artifact,
            calibration_version=verifier.calibration_version,
            calibration_artifact_sha256=None,
            temperature=verifier.temperature,
            max_length=verifier.max_length,
            decision_policy=policy,
        ),
    )


def _publish_source(
    connection: Connection[tuple[object, ...]], tmp_path: Path
) -> _PublishedRun:
    corpus = tmp_path / "published-source"
    corpus.mkdir()
    (corpus / "source.txt").write_text(
        "GroundLoop maintains claims against versioned evidence.\n",
        encoding="utf-8",
    )
    application, embedding_spec, verifier_spec = _application(
        PostgresArtifactStore(connection)
    )
    manifest = application.register(corpus, "What does GroundLoop maintain?")
    return _PublishedRun(manifest, embedding_spec, verifier_spec)


def _complete_staged_bundle(tmp_path: Path) -> M3PublicationBundle:
    corpus = tmp_path / "competing-source"
    corpus.mkdir()
    (corpus / "other.txt").write_text(
        "A competing M3 run publishes another versioned evidence snapshot.\n",
        encoding="utf-8",
    )
    store = InMemoryPublicationStore()
    application, _embedding_spec, _verifier_spec = _application(store)
    application.generator.prompt_artifact = replace(
        application.generator.prompt_artifact,
        artifact_id="m3-generation-prompt-competing-v1",
        version="m3-generation-competing-v1",
    )
    application.extractor.prompt_artifact = replace(
        application.extractor.prompt_artifact,
        artifact_id="m3-claim-extraction-prompt-competing-v1",
        version="m3-claim-extraction-competing-v1",
    )
    application.verifier.prompt_artifact = replace(
        application.verifier.prompt_artifact,
        artifact_id="groundloop-verifier-pair-competing-v1",
        version="groundloop-verifier-pair-competing-v1",
    )
    manifest = application.register(corpus, "What did the competing run publish?")
    return store.bundles[manifest.run_id]


def _activate(
    connection: Connection[tuple[object, ...]],
    published: _PublishedRun,
    backend: DeterministicFakeEmbedder | None = None,
) -> None:
    selected_backend = backend or DeterministicFakeEmbedder()
    activate_published_m3_run(
        connection,
        run_id=published.manifest.run_id,
        embeddings=M4BgeRoleAdapter(selected_backend, published.embedding_spec),
        verifier_spec=published.verifier_spec,
        repo_root=REPO_ROOT,
    )


def _semantic_counts(
    connection: Connection[tuple[object, ...]],
) -> tuple[int, ...]:
    row = connection.execute(
        """
        SELECT
            (SELECT count(*) FROM groundloop_epoch),
            (SELECT count(*) FROM groundloop_document),
            (SELECT count(*) FROM groundloop_document_version),
            (SELECT count(*) FROM groundloop_chunk_version),
            (SELECT count(*) FROM groundloop_question),
            (SELECT count(*) FROM groundloop_answer_version),
            (SELECT count(*) FROM groundloop_claim),
            (SELECT count(*) FROM groundloop_semantic_observation),
            (SELECT count(*) FROM groundloop_observation_currency)
        """
    ).fetchone()
    assert row is not None
    return tuple(cast(int, value) for value in row)


def test_publish_bundle_rechecks_head_for_a_complete_staged_run(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_source(m3_m4_activation_connection, tmp_path)
    bundle = _complete_staged_bundle(tmp_path)
    _activate(m3_m4_activation_connection, published)

    m3_m4_activation_connection.execute(
        """
        INSERT INTO groundloop_pipeline_run (
            run_id, schema_version, status, config_hash, input_hash,
            corpus_hash, question_id, manifest
        ) VALUES (%s, %s, 'staged', %s, %s, %s, %s, %s)
        """,
        (
            bundle.manifest.run_id,
            bundle.manifest.schema_version,
            bundle.manifest.config_hash,
            bundle.manifest.input_hash,
            bundle.manifest.corpus_hash,
            bundle.manifest.question_id,
            Jsonb(manifest_to_dict(bundle.manifest)),
        ),
    )
    before = _semantic_counts(m3_m4_activation_connection)

    with pytest.raises(
        ArtifactConflictError,
        match="cannot publish a staged M3 run after M4 activation",
    ):
        PostgresArtifactStore(m3_m4_activation_connection).publish_bundle(bundle)

    assert _semantic_counts(m3_m4_activation_connection) == before
    status = m3_m4_activation_connection.execute(
        "SELECT status FROM groundloop_pipeline_run WHERE run_id = %s",
        (bundle.manifest.run_id,),
    ).fetchone()
    assert status == ("staged",)
