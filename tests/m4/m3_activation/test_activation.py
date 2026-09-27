from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import cast

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
)
from groundloop.ai.embeddings import DeterministicFakeEmbedder
from groundloop.ai.embeddings.common import EmbeddedQuery
from groundloop.ai.generation import DeterministicAnswerGenerator
from groundloop.ai.persistence import PostgresArtifactStore
from groundloop.ai.verification import DeterministicFakeVerifier
from groundloop.domain import DecisionPolicy
from groundloop.errors import ArtifactConflictError
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
CONFIG_HASH = hashlib.sha256(b"m3-m4-activation-test-v1").hexdigest()

_ACTIVATION_RELATIONS = (
    "groundloop_candidate_policy",
    "groundloop_m4_publication_head",
    "groundloop_published_observation_currency",
    "groundloop_published_claim_state",
    "groundloop_published_answer_state",
    "groundloop_m4_claim_registry_snapshot",
    "groundloop_m4_claim_registry_member",
    "groundloop_m4_claim_admission_index",
    "groundloop_m4_role_embedding_artifact",
)


@dataclass(frozen=True, slots=True)
class _PublishedRun:
    manifest: PipelineRunManifest
    application: M3Application
    corpus: Path
    embeddings: M4BgeRoleAdapter
    verifier_spec: VerificationAdapterSpec


class _FailOnCallEmbedder(DeterministicFakeEmbedder):
    def embed(self, chunks: tuple[ChunkDraft, ...]) -> tuple[EmbeddingRecord, ...]:
        del chunks
        raise AssertionError("activation replay called the passage embedder")

    def embed_query(self, text: str, query_kind: QueryKind) -> EmbeddedQuery:
        del text, query_kind
        raise AssertionError("activation replay called the query embedder")


def _publish_m3_run(
    connection: Connection[tuple[object, ...]], tmp_path: Path
) -> _PublishedRun:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "groundloop.txt").write_text(
        "GroundLoop maintains claim grounding relative to versioned evidence.\n",
        encoding="utf-8",
    )
    policy = DecisionPolicy("m3-policy-v1", 0.8, 0.8)
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
        application=application,
        corpus=corpus,
        embeddings=M4BgeRoleAdapter(
            DeterministicFakeEmbedder(),
            embedding_spec,
        ),
        verifier_spec=verifier_spec,
    )


def _scalar(connection: Connection[tuple[object, ...]], query: str) -> int:
    row = connection.execute(query).fetchone()
    assert row is not None
    return cast(int, row[0])


def _activate(
    connection: Connection[tuple[object, ...]], published: _PublishedRun
) -> M3M4ActivationReceipt:
    return activate_published_m3_run(
        connection,
        run_id=published.manifest.run_id,
        embeddings=published.embeddings,
        verifier_spec=published.verifier_spec,
        repo_root=REPO_ROOT,
    )


def _database_projection(
    connection: Connection[tuple[object, ...]],
) -> dict[str, str]:
    schema_row = connection.execute("SELECT current_schema()").fetchone()
    assert schema_row is not None and schema_row[0] is not None
    schema_name = str(schema_row[0])
    relations = tuple(
        str(row[0])
        for row in connection.execute(
            """
            SELECT class.relname
            FROM pg_class AS class
            JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
            WHERE namespace.nspname = %s
              AND class.relkind IN ('r', 'p')
              AND class.relname LIKE 'groundloop_%%'
            ORDER BY class.relname
            """,
            (schema_name,),
        ).fetchall()
    )
    projection: dict[str, str] = {}
    for relation in relations:
        row = connection.execute(
            sql.SQL(
                """
                SELECT COALESCE(
                    jsonb_agg(to_jsonb(projected)
                              ORDER BY to_jsonb(projected)::text),
                    '[]'::jsonb
                )::text
                FROM {}.{} AS projected
                """
            ).format(sql.Identifier(schema_name), sql.Identifier(relation))
        ).fetchone()
        assert row is not None
        projection[relation] = str(row[0])
    return projection


def test_fresh_activation_installs_the_complete_m3_baseline(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    before = inspect_m3_m4_activation(
        m3_m4_activation_connection,
        run_id=published.manifest.run_id,
    )

    assert before.replay_receipt is None
    assert before.source.run_id == published.manifest.run_id
    assert before.source.epoch_id == published.manifest.semantic_epoch_id
    assert before.source.answer_version_id == published.manifest.answer_version_id

    receipt = activate_published_m3_run(
        m3_m4_activation_connection,
        run_id=published.manifest.run_id,
        embeddings=published.embeddings,
        verifier_spec=published.verifier_spec,
        repo_root=REPO_ROOT,
    )

    assert not receipt.replayed
    assert receipt.run_id == published.manifest.run_id
    assert receipt.base_epoch_id == published.manifest.semantic_epoch_id
    assert receipt.answer_version_id == published.manifest.answer_version_id
    assert receipt.claim_count == len(published.manifest.claim_states)
    assert receipt.chunk_count == len(published.manifest.chunk_version_ids)
    assert receipt.observation_count == len(published.manifest.verifications)
    assert receipt.role_artifact_count == receipt.claim_count + receipt.chunk_count
    assert receipt.claim_index_count == receipt.claim_count

    interval_counts = m3_m4_activation_connection.execute(
        """
        SELECT
            (SELECT count(*) FROM groundloop_published_observation_currency),
            (SELECT count(*) FROM groundloop_published_claim_state),
            (SELECT count(*) FROM groundloop_published_answer_state),
            (SELECT count(*) FROM groundloop_published_observation_currency
             WHERE valid_from_epoch = %s AND valid_to_epoch IS NULL),
            (SELECT count(*) FROM groundloop_published_claim_state
             WHERE valid_from_epoch = %s AND valid_to_epoch IS NULL),
            (SELECT count(*) FROM groundloop_published_answer_state
             WHERE valid_from_epoch = %s AND valid_to_epoch IS NULL)
        """,
        (receipt.base_epoch_id,) * 3,
    ).fetchone()
    assert interval_counts == (
        receipt.observation_count,
        receipt.claim_count,
        1,
        receipt.observation_count,
        receipt.claim_count,
        1,
    )
    assert (
        _scalar(
            m3_m4_activation_connection,
            "SELECT count(*) FROM groundloop_m4_publication_head",
        )
        == 1
    )
    assert (
        _scalar(
            m3_m4_activation_connection,
            "SELECT count(*) FROM groundloop_m4_claim_registry_snapshot",
        )
        == 1
    )
    assert (
        _scalar(
            m3_m4_activation_connection,
            "SELECT count(*) FROM groundloop_m4_claim_registry_member",
        )
        == receipt.claim_count
    )
    assert (
        _scalar(
            m3_m4_activation_connection,
            "SELECT count(*) FROM groundloop_candidate_policy",
        )
        == 1
    )
    assert (
        _scalar(
            m3_m4_activation_connection,
            "SELECT count(*) FROM groundloop_m4_claim_admission_index",
        )
        == receipt.claim_count
    )
    head = m3_m4_activation_connection.execute(
        "SELECT singleton, epoch_id FROM groundloop_m4_publication_head"
    ).fetchone()
    assert head == (True, receipt.base_epoch_id)
    snapshot = m3_m4_activation_connection.execute(
        """
        SELECT claim_registry_snapshot_id, claim_count
        FROM groundloop_m4_claim_registry_snapshot
        """
    ).fetchone()
    assert snapshot == (receipt.claim_registry_snapshot_id, receipt.claim_count)
    members = tuple(
        (str(row[0]), cast(int, row[1]))
        for row in m3_m4_activation_connection.execute(
            """
            SELECT claim_id, member_ordinal
            FROM groundloop_m4_claim_registry_member
            ORDER BY member_ordinal
            """
        ).fetchall()
    )
    assert members == tuple(
        (claim_id, ordinal) for ordinal, claim_id in enumerate(before.source.claim_ids)
    )
    policy = m3_m4_activation_connection.execute(
        """
        SELECT candidate_policy_id, policy_hash,
               claim_registry_snapshot_id, claim_count
        FROM groundloop_candidate_policy
        """
    ).fetchone()
    assert policy is not None
    assert (str(policy[0]), str(policy[1]).strip()) == (
        receipt.candidate_policy_id,
        receipt.candidate_policy_hash,
    )
    assert (str(policy[2]), cast(int, policy[3])) == (
        receipt.claim_registry_snapshot_id,
        receipt.claim_count,
    )
    role_counts: dict[str, int] = {
        str(row[0]): cast(int, row[1])
        for row in m3_m4_activation_connection.execute(
            """
            SELECT embedding_role, count(*)
            FROM groundloop_m4_role_embedding_artifact
            GROUP BY embedding_role
            """
        ).fetchall()
    }
    assert role_counts == {
        "claim_query": receipt.claim_count,
        "chunk_passage": receipt.chunk_count,
    }

    after = inspect_m3_m4_activation(
        m3_m4_activation_connection,
        run_id=published.manifest.run_id,
    )
    assert after.replay_receipt is not None
    assert after.replay_receipt.replayed
    assert after.replay_receipt.claim_registry_snapshot_id == (
        receipt.claim_registry_snapshot_id
    )
    assert after.replay_receipt.candidate_policy_id == receipt.candidate_policy_id
    assert after.replay_receipt.candidate_policy_hash == receipt.candidate_policy_hash


def test_exact_replay_calls_no_embedding_backend_and_changes_no_table(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    first = _activate(m3_m4_activation_connection, published)
    before = _database_projection(m3_m4_activation_connection)
    fail_backend = _FailOnCallEmbedder()
    replay_embeddings = M4BgeRoleAdapter(
        fail_backend,
        EmbeddingAdapterSpec(
            model_artifact=fail_backend.model_artifact,
            dimension=fail_backend.dimension,
        ),
    )

    replay = activate_published_m3_run(
        m3_m4_activation_connection,
        run_id=published.manifest.run_id,
        embeddings=replay_embeddings,
        verifier_spec=published.verifier_spec,
        repo_root=REPO_ROOT,
    )

    assert replay.replayed
    assert replay.to_dict() == {
        **first.to_dict(),
        "created_role_artifact_count": 0,
        "reused_role_artifact_count": first.role_artifact_count,
        "activation_embedding_request_count": 0,
        "replayed": True,
    }
    assert _database_projection(m3_m4_activation_connection) == before


@pytest.mark.parametrize(
    "checkpoint",
    (
        "after_model_artifacts",
        "after_claim_registry",
        "after_candidate_policy",
        "after_claim_artifacts",
        "after_chunk_artifacts",
        "after_artifacts",
        "before_bootstrap",
        "after_bootstrap",
    ),
)
def test_activation_failure_cut_rolls_back_every_m4_row_and_can_retry(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
    checkpoint: str,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    before = _database_projection(m3_m4_activation_connection)

    visited: list[str] = []

    def inject(name: str) -> None:
        visited.append(name)
        if name == checkpoint:
            raise RuntimeError(f"injected activation failure at {checkpoint}")

    with pytest.raises(RuntimeError, match=f"failure at {checkpoint}"):
        activate_published_m3_run(
            m3_m4_activation_connection,
            run_id=published.manifest.run_id,
            embeddings=published.embeddings,
            verifier_spec=published.verifier_spec,
            repo_root=REPO_ROOT,
            failure_injector=inject,
        )

    assert visited[-1] == checkpoint
    assert _database_projection(m3_m4_activation_connection) == before
    assert {
        relation: _scalar(
            m3_m4_activation_connection,
            f"SELECT count(*) FROM {relation}",
        )
        for relation in _ACTIVATION_RELATIONS
    } == {relation: 0 for relation in _ACTIVATION_RELATIONS}
    inspection = inspect_m3_m4_activation(
        m3_m4_activation_connection,
        run_id=published.manifest.run_id,
    )
    assert inspection.replay_receipt is None
    assert not _activate(m3_m4_activation_connection, published).replayed


def test_new_m3_stage_is_rejected_after_activation(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    _activate(m3_m4_activation_connection, published)

    with pytest.raises(
        ArtifactConflictError,
        match="cannot stage a new M3 run after M4 activation",
    ):
        published.application.register(
            published.corpus,
            "Which evidence version grounds the answer?",
        )

    assert (
        _scalar(
            m3_m4_activation_connection,
            "SELECT count(*) FROM groundloop_pipeline_run",
        )
        == 1
    )


def test_inspection_rejects_corrupted_m3_state_coordinates(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    m3_m4_activation_connection.execute(
        "UPDATE groundloop_claim_state_materialized SET updated_revision = 1"
    )

    with pytest.raises(
        ArtifactConflictError,
        match="M3 materialized claim state drift",
    ):
        inspect_m3_m4_activation(
            m3_m4_activation_connection,
            run_id=published.manifest.run_id,
        )


def test_inspection_rejects_partial_m4_activation(
    m3_m4_activation_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
) -> None:
    published = _publish_m3_run(m3_m4_activation_connection, tmp_path)
    assert published.manifest.semantic_epoch_id is not None
    m3_m4_activation_connection.execute(
        """
        INSERT INTO groundloop_m4_publication_head (singleton, epoch_id)
        VALUES (true, %s)
        """,
        (published.manifest.semantic_epoch_id,),
    )

    with pytest.raises(
        ArtifactConflictError,
        match="M4 published observation baseline drift",
    ):
        inspect_m3_m4_activation(
            m3_m4_activation_connection,
            run_id=published.manifest.run_id,
        )
