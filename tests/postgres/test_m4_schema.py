"""Live PostgreSQL checks for the frozen M4 coordination schema."""

from __future__ import annotations

from typing import Any

import pytest
from psycopg import Connection, errors
from psycopg.types.json import Jsonb

from groundloop.postgres import record_epoch, temporary_m2_schema

HASH = "a" * 64


def _seed_m4(connection: Connection[tuple[object, ...]]) -> tuple[int, int]:
    sealed_epoch, _ = record_epoch(
        connection,
        event_id="seed-event",
        payload_hash="1" * 64,
    )
    connection.execute(
        "INSERT INTO groundloop_document VALUES ('doc', 'doc.txt', 'test')"
    )
    connection.execute(
        "INSERT INTO groundloop_document_version VALUES ('dv', 'doc', 'raw', %s, NULL)",
        (sealed_epoch,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_chunk_version VALUES
            ('chunk', 'dv', 0, 'evidence', %s, 'fixed-char-v1', %s, NULL)
        """,
        ("2" * 64, sealed_epoch),
    )
    connection.execute(
        "INSERT INTO groundloop_question VALUES ('question', 'question?', %s)",
        (sealed_epoch,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_answer_version VALUES
            ('answer', 'question', 'answer', 'generator', 'revision', 'prompt', %s)
        """,
        (sealed_epoch,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_claim VALUES
            ('claim', 'answer', 'claim', 'extractor', 'revision', 'prompt', true)
        """
    )
    connection.execute(
        """
        INSERT INTO groundloop_decision_policy VALUES
            ('policy-v1', 0.8, 0.8, 'v1', NULL, NULL, %s, NULL)
        """,
        (sealed_epoch,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_model_artifact (
            model_artifact_id, task, provider, model_id, immutable_revision,
            tokenizer_revision, license_id, config_hash
        ) VALUES ('embedder', 'embedding', 'test', 'embedder', 'rev', 'rev',
                  'MIT', %s)
        """,
        ("3" * 64,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_candidate_policy (
            candidate_policy_id, policy_hash, embedding_model_artifact_id,
            decision_policy_version, claim_role_template_hash,
            chunk_role_template_hash, vector_method_version, vector_index_kind,
            vector_index_build_config_hash, vector_search_config_hash,
            lexical_method_version, lexical_config_hash,
            lexical_postgres_version, lexical_regconfig_identity,
            claim_registry_snapshot_id, claim_count, fusion_version,
            approximate_cap_per_inserted_chunk, frontier_depth, manifest
        ) VALUES (
            'candidate-v1', %s, 'embedder', 'policy-v1', %s, %s,
            'reverse-bge-v1', 'exact', %s, %s, 'lexical-v1', %s,
            '16.14', 'simple', 'registry-1', 1, 'interleave-v1', 10, 4, %s
        )
        """,
        ("4" * 64, "5" * 64, "6" * 64, "7" * 64, "8" * 64, "9" * 64, Jsonb({})),
    )
    row = connection.execute(
        """
        INSERT INTO groundloop_epoch (
            event_id, payload_hash, revision, structural_status,
            semantic_status, evaluation_state, publication_mode
        ) VALUES ('m4-event', %s, 0, 'committed', 'pending', 'pending',
                  'provisional')
        RETURNING epoch_id
        """,
        ("b" * 64,),
    ).fetchone()
    assert row is not None
    m4_epoch = int(row[0])
    connection.execute(
        """
        INSERT INTO groundloop_m4_update VALUES
            (%s, 'insert', 'candidate-v1', %s, 'registry-1', %s, DEFAULT)
        """,
        (m4_epoch, sealed_epoch, Jsonb({})),
    )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return sealed_epoch, m4_epoch


def _insert_job(
    connection: Connection[tuple[object, ...]],
    *,
    epoch_id: int,
    job_id: str,
    kind: str,
    parent_job_id: str | None = None,
    claim_id: str | None = None,
    chunk_id: str | None = None,
) -> None:
    expandable = kind != "verify_pair"
    connection.execute(
        """
        INSERT INTO groundloop_semantic_job (
            job_id, epoch_id, parent_job_id, job_kind, candidate_policy_id,
            payload_hash, execution_spec_hash, claim_id, chunk_version_id,
            expandable, job_state, created_revision
        ) VALUES (%s, %s, %s, %s, 'candidate-v1', %s, %s, %s, %s,
                  %s, 'declared', 0)
        """,
        (
            job_id,
            epoch_id,
            parent_job_id,
            kind,
            HASH,
            HASH,
            claim_id,
            chunk_id,
            expandable,
        ),
    )


def test_m4_job_shapes_child_epoch_and_terminal_transitions(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    with temporary_m2_schema(live_connection):
        _, epoch_id = _seed_m4(live_connection)
        _insert_job(
            live_connection,
            epoch_id=epoch_id,
            job_id="discover",
            kind="impact_discovery",
            chunk_id="chunk",
        )
        _insert_job(
            live_connection,
            epoch_id=epoch_id,
            job_id="verify",
            kind="verify_pair",
            parent_job_id="discover",
            claim_id="claim",
            chunk_id="chunk",
        )
        live_connection.execute(
            """
            INSERT INTO groundloop_semantic_job_dependency VALUES
                (%s, 'discover', 'verify')
            """,
            (epoch_id,),
        )
        live_connection.execute(
            """
            UPDATE groundloop_semantic_job
            SET job_state = 'running' WHERE job_id = 'discover'
            """
        )
        live_connection.execute(
            """
            UPDATE groundloop_semantic_job
            SET job_state = 'completed_active', child_closed = true,
                child_set_hash = %s, completion_digest = %s,
                result_artifact_id = 'discovery-result', result_artifact_hash = %s,
                completed_revision = 1, completed_at = now()
            WHERE job_id = 'discover'
            """,
            ("c" * 64, "d" * 64, "e" * 64),
        )

        with pytest.raises(errors.RaiseException, match="terminal semantic job"):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    UPDATE groundloop_semantic_job
                    SET result_artifact_id = 'changed' WHERE job_id = 'discover'
                    """
                )

        with pytest.raises(errors.CheckViolation):
            with live_connection.transaction():
                _insert_job(
                    live_connection,
                    epoch_id=epoch_id,
                    job_id="bad-frontier",
                    kind="frontier_retrieve",
                    chunk_id="chunk",
                )


def test_m4_typed_judgments_do_not_mix_human_and_model_payloads(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    with temporary_m2_schema(live_connection):
        _seed_m4(live_connection)
        base: tuple[Any, ...] = (
            "model-judgment",
            "claim",
            "chunk",
            "model",
            "verifier-v1",
            "policy-v1",
            "support",
            0.9,
            0.05,
            0.05,
            HASH,
            "development",
            "audit-1",
        )
        live_connection.execute(
            """
            INSERT INTO groundloop_pair_judgment (
                judgment_id, claim_id, chunk_version_id, source_kind,
                source_artifact_id, decision_policy_or_guideline_id,
                derived_label, support_score, refute_score, neutral_score,
                input_hash, split_id, manifest_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            base,
        )
        with pytest.raises(errors.CheckViolation):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    INSERT INTO groundloop_pair_judgment (
                        judgment_id, claim_id, chunk_version_id, source_kind,
                        source_artifact_id, decision_policy_or_guideline_id,
                        derived_label, support_score, refute_score, neutral_score,
                        input_hash, split_id, manifest_id
                    ) VALUES ('bad-human', 'claim', 'chunk', 'human', 'annotator',
                              'guide', 'support', 1.0, NULL, NULL, %s, 'test', 'audit')
                    """,
                    (HASH,),
                )


def test_failed_working_state_cannot_overwrite_published_snapshot(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    with temporary_m2_schema(live_connection):
        sealed_epoch, m4_epoch = _seed_m4(live_connection)
        live_connection.execute(
            """
            INSERT INTO groundloop_published_claim_state VALUES
                ('claim', %s, NULL, 1, 0, 0.9, NULL, ARRAY['obs'], ARRAY[]::text[],
                 'supported', %s)
            """,
            (sealed_epoch, HASH),
        )
        live_connection.execute(
            """
            INSERT INTO groundloop_claim_state_materialized VALUES
                ('claim', 0, 0, NULL, NULL, ARRAY[]::text[], ARRAY[]::text[],
                 'unsupported', %s, 1)
            """,
            (m4_epoch,),
        )
        assert live_connection.execute(
            """
            SELECT support_count, status
            FROM groundloop_published_claim_state
            WHERE claim_id = 'claim' AND valid_to_epoch IS NULL
            """
        ).fetchone() == (1, "supported")

        with pytest.raises(errors.UniqueViolation):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    INSERT INTO groundloop_published_claim_state VALUES
                        ('claim', %s, NULL, 0, 0, NULL, NULL,
                         ARRAY[]::text[], ARRAY[]::text[], 'unsupported', %s)
                    """,
                    (m4_epoch, HASH),
                )
