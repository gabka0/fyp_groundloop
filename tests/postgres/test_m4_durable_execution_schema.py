"""Live schema gates for durable M4 state and neural provenance."""

from __future__ import annotations

from typing import Any

import pytest
from psycopg import Connection, errors

from groundloop.postgres import record_epoch, temporary_m2_schema

HASH = "a" * 64


def _unit_vector_literal() -> str:
    return "[1," + ",".join("0" for _ in range(383)) + "]"


def _seed(connection: Connection[Any]) -> tuple[int, int]:
    sealed_epoch, created = record_epoch(
        connection, event_id="sealed", payload_hash="1" * 64
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
        (sealed_epoch,),
    )
    connection.execute(
        "INSERT INTO groundloop_document VALUES ('doc', 'doc://source', 'test')"
    )
    connection.execute(
        """
        INSERT INTO groundloop_document_version VALUES
          ('dv', 'doc', 'content', %s, NULL)
        """,
        (sealed_epoch,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_chunk_version VALUES
          ('chunk', 'dv', 0, 'claim evidence', %s, 'fixed-char-v1', %s, NULL)
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
          ('claim', 'answer', 'claim evidence', 'extractor', 'revision',
           'prompt', true)
        """
    )
    connection.execute(
        """
        INSERT INTO groundloop_decision_policy VALUES
          ('policy-v1', 0.8, 0.8, 'v1', 'temperature-v1', NULL, %s, NULL)
        """,
        (sealed_epoch,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_model_artifact (
            model_artifact_id, task, provider, model_id, immutable_revision,
            tokenizer_revision, license_id, config_hash, artifact_sha256
        ) VALUES
          ('embedder', 'embedding', 'test', 'embedder', 'rev', 'rev', 'MIT',
           %s, %s),
          ('verifier', 'verification', 'test', 'verifier', 'rev', 'rev', 'MIT',
           %s, %s)
        """,
        ("3" * 64, "4" * 64, "5" * 64, "6" * 64),
    )
    connection.execute(
        """
        INSERT INTO groundloop_prompt_artifact (
            prompt_artifact_id, task, version, template, template_hash,
            decoding_config_hash
        ) VALUES ('verify-prompt', 'verification', 'v1',
                  'premise={evidence} hypothesis={claim}', %s, %s)
        """,
        ("7" * 64, "8" * 64),
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
            'reverse-bge-v1', 'hnsw', %s, %s, 'lexical-v1', %s,
            '16.14', 'simple', 'registry-1', 1, 'rank-interleave-v1', 8, 4,
            '{}'::jsonb
        )
        """,
        ("9" * 64, "a" * 64, "b" * 64, "c" * 64, "d" * 64, "e" * 64),
    )

    epoch_id, created = record_epoch(
        connection, event_id="m4-event", payload_hash="f" * 64
    )
    assert created
    connection.execute(
        """
        UPDATE groundloop_epoch
        SET structural_status = 'committed', revision = 1
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m4_update (
            epoch_id, update_kind, candidate_policy_id,
            previous_published_epoch_id, registry_snapshot_id
        ) VALUES (%s, 'insert', 'candidate-v1', %s, 'registry-1')
        """,
        (epoch_id, sealed_epoch),
    )
    connection.execute(
        """
        INSERT INTO groundloop_semantic_job (
            job_id, epoch_id, job_kind, candidate_policy_id, payload_hash,
            execution_spec_hash, chunk_version_id, expandable, job_state,
            created_revision
        ) VALUES ('discover', %s, 'impact_discovery', 'candidate-v1', %s, %s,
                  'chunk', true, 'declared', 1)
        """,
        (epoch_id, HASH, "1" * 64),
    )
    connection.execute(
        """
        INSERT INTO groundloop_semantic_job (
            job_id, epoch_id, parent_job_id, job_kind, candidate_policy_id,
            payload_hash, execution_spec_hash, claim_id, chunk_version_id,
            expandable, job_state, created_revision
        ) VALUES ('verify', %s, 'discover', 'verify_pair', 'candidate-v1', %s,
                  %s, 'claim', 'chunk', false, 'declared', 1)
        """,
        (epoch_id, HASH, HASH),
    )
    connection.execute(
        """
        INSERT INTO groundloop_admitted_pair VALUES
          ('admitted', %s, 'chunk', 'claim', 'candidate-v1', 1,
           ARRAY['vector'], false)
        """,
        (epoch_id,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_semantic_observation (
            observation_id, subject_kind, subject_id, chunk_version_id,
            task_type, support_score, refute_score, neutral_score, model_id,
            model_version, prompt_version, input_hash, produced_epoch,
            raw_output_hash
        ) VALUES ('observation', 'claim', 'claim', 'chunk',
                  'claim-verification-v1', 0.9, 0.05, 0.05, 'verifier', 'rev',
                  'v1', %s, %s, %s)
        """,
        ("2" * 64, epoch_id, "3" * 64),
    )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return sealed_epoch, epoch_id


def _column_names(connection: Connection[Any], table: str) -> tuple[str, ...]:
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table,),
    ).fetchall()
    return tuple(str(row[0]) for row in rows)


def test_m4_durable_tables_and_physical_indexes_have_frozen_shape(
    live_connection: Connection[Any],
) -> None:
    with temporary_m2_schema(live_connection):
        assert _column_names(live_connection, "groundloop_m4_working_claim_state") == (
            "epoch_id",
            "claim_id",
            "support_count",
            "refute_count",
            "best_support_score",
            "best_refute_score",
            "supporting_observation_ids",
            "refuting_observation_ids",
            "status",
            "certificate_digest",
            "updated_revision",
        )
        assert _column_names(live_connection, "groundloop_m4_working_answer_state") == (
            "epoch_id",
            "answer_version_id",
            "required_claim_count",
            "supported_count",
            "unsupported_count",
            "refuted_count",
            "conflicted_count",
            "status",
            "updated_revision",
        )
        assert _column_names(
            live_connection, "groundloop_m4_verification_execution"
        ) == (
            "observation_id",
            "job_id",
            "admitted_pair_id",
            "model_artifact_id",
            "prompt_artifact_id",
            "execution_spec_hash",
            "pair_input_hash",
            "calibration_version",
            "calibration_artifact_sha256",
            "temperature",
            "raw_logits",
            "raw_output_hash",
            "reused_from_observation_id",
            "created_at",
        )
        assert _column_names(
            live_connection, "groundloop_m4_claim_admission_index"
        ) == (
            "claim_registry_snapshot_id",
            "claim_id",
            "embedding_model_artifact_id",
            "claim_role_template_hash",
            "embedding_input_hash",
            "embedding",
            "lexical_tsv",
        )

        index_rows = live_connection.execute(
            """
            SELECT index_class.relname, access_method.amname,
                   index_class.reloptions, pg_get_indexdef(index_class.oid)
            FROM pg_class AS index_class
            JOIN pg_am AS access_method ON access_method.oid = index_class.relam
            JOIN pg_index AS index_record
              ON index_record.indexrelid = index_class.oid
            WHERE index_record.indrelid =
                  'groundloop_m4_claim_admission_index'::regclass
            ORDER BY index_class.relname
            """
        ).fetchall()
        by_name = {str(row[0]): row for row in index_rows}
        assert set(by_name) == {
            "groundloop_m4_claim_admission_hnsw",
            "groundloop_m4_claim_admission_index_pkey",
            "groundloop_m4_claim_admission_lexical_gin",
        }
        hnsw = by_name["groundloop_m4_claim_admission_hnsw"]
        assert hnsw[1] == "hnsw"
        assert set(hnsw[2]) == {"m=16", "ef_construction=64"}
        assert "embedding vector_cosine_ops" in hnsw[3]
        assert by_name["groundloop_m4_claim_admission_lexical_gin"][1] == "gin"

        foreign_keys = live_connection.execute(
            """
            SELECT count(*)
            FROM pg_constraint
            WHERE conrelid = 'groundloop_m4_verification_execution'::regclass
              AND contype = 'f'
            """
        ).fetchone()
        assert foreign_keys == (6,)


def test_working_complete_state_checks_and_terminal_epoch_guard(
    live_connection: Connection[Any],
) -> None:
    with temporary_m2_schema(live_connection):
        _, epoch_id = _seed(live_connection)
        live_connection.execute(
            """
            INSERT INTO groundloop_m4_working_claim_state VALUES
              (%s, 'claim', 1, 0, 0.9, NULL, ARRAY['observation'],
               ARRAY[]::text[], 'supported', %s, 1)
            """,
            (epoch_id, "4" * 64),
        )
        live_connection.execute(
            """
            INSERT INTO groundloop_m4_working_answer_state VALUES
              (%s, 'answer', 1, 1, 0, 0, 0, 'valid', 1)
            """,
            (epoch_id,),
        )
        live_connection.execute(
            """
            UPDATE groundloop_m4_working_claim_state
            SET best_support_score = 0.95, updated_revision = 2
            WHERE epoch_id = %s AND claim_id = 'claim'
            """,
            (epoch_id,),
        )
        assert live_connection.execute(
            """
            SELECT support_count, best_support_score, status::text,
                   updated_revision
            FROM groundloop_m4_working_claim_state
            WHERE epoch_id = %s AND claim_id = 'claim'
            """,
            (epoch_id,),
        ).fetchone() == (1, 0.95, "supported", 2)

        with pytest.raises(errors.RaiseException, match="without a revision"):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    UPDATE groundloop_m4_working_claim_state
                    SET best_support_score = 0.8
                    WHERE epoch_id = %s AND claim_id = 'claim'
                    """,
                    (epoch_id,),
                )
        with pytest.raises(errors.CheckViolation):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    INSERT INTO groundloop_m4_working_answer_state VALUES
                      (%s, 'answer', 1, 1, 1, 0, 0, 'valid', 2)
                    ON CONFLICT (epoch_id, answer_version_id) DO UPDATE SET
                      supported_count = EXCLUDED.supported_count,
                      unsupported_count = EXCLUDED.unsupported_count,
                      updated_revision = EXCLUDED.updated_revision
                    """,
                    (epoch_id,),
                )
        with pytest.raises(errors.CheckViolation):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    INSERT INTO groundloop_m4_working_claim_state VALUES
                      (%s, 'claim', 0, 0, NULL, NULL, ARRAY[]::text[],
                       ARRAY[]::text[], 'unsupported', 'short', 3)
                    ON CONFLICT (epoch_id, claim_id) DO UPDATE SET
                      certificate_digest = EXCLUDED.certificate_digest,
                      updated_revision = EXCLUDED.updated_revision
                    """,
                    (epoch_id,),
                )

        live_connection.execute(
            """
            UPDATE groundloop_epoch
            SET structural_status = 'failed', semantic_status = 'failed',
                evaluation_state = 'failed'
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        )
        with pytest.raises(errors.RaiseException, match="terminal M4 epoch"):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    UPDATE groundloop_m4_working_answer_state
                    SET updated_revision = 2 WHERE epoch_id = %s
                    """,
                    (epoch_id,),
                )


def test_claim_admission_population_is_role_safe_and_immutable(
    live_connection: Connection[Any],
) -> None:
    with temporary_m2_schema(live_connection):
        _seed(live_connection)
        vector = _unit_vector_literal()
        live_connection.execute(
            """
            INSERT INTO groundloop_m4_claim_admission_index (
                claim_registry_snapshot_id, claim_id,
                embedding_model_artifact_id, claim_role_template_hash,
                embedding_input_hash, embedding, lexical_tsv
            ) VALUES ('registry-1', 'claim', 'embedder', %s, %s, %s::vector,
                      to_tsvector('simple'::regconfig, 'claim evidence'))
            """,
            ("a" * 64, "b" * 64, vector),
        )
        assert live_connection.execute(
            """
            SELECT format_type(attribute.atttypid, attribute.atttypmod)
            FROM pg_attribute AS attribute
            WHERE attribute.attrelid =
                  'groundloop_m4_claim_admission_index'::regclass
              AND attribute.attname = 'embedding'
            """
        ).fetchone() == ("vector(384)",)

        with pytest.raises(errors.RaiseException, match="embedding model"):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    INSERT INTO groundloop_m4_claim_admission_index VALUES
                      ('registry-2', 'claim', 'verifier', %s, %s, %s::vector,
                       to_tsvector('simple', 'claim evidence'))
                    """,
                    ("a" * 64, "b" * 64, vector),
                )
        with pytest.raises(errors.ForeignKeyViolation):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    INSERT INTO groundloop_m4_claim_admission_index VALUES
                      ('registry-2', 'missing-claim', 'embedder', %s, %s,
                       %s::vector, to_tsvector('simple', 'missing'))
                    """,
                    ("a" * 64, "b" * 64, vector),
                )
        with pytest.raises(errors.RaiseException, match="immutable M4 table"):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    UPDATE groundloop_m4_claim_admission_index
                    SET embedding_input_hash = %s
                    WHERE claim_registry_snapshot_id = 'registry-1'
                    """,
                    ("c" * 64,),
                )


def test_verifier_execution_binds_dynamic_pair_and_is_immutable(
    live_connection: Connection[Any],
) -> None:
    with temporary_m2_schema(live_connection):
        _, epoch_id = _seed(live_connection)
        values = (
            "observation",
            "verify",
            "admitted",
            "verifier",
            "verify-prompt",
            HASH,
            "1" * 64,
            "temperature-v1",
            "2" * 64,
            1.1,
            "3" * 64,
        )
        with pytest.raises(errors.CheckViolation):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    INSERT INTO groundloop_m4_verification_execution (
                        observation_id, job_id, admitted_pair_id,
                        model_artifact_id, prompt_artifact_id,
                        execution_spec_hash, pair_input_hash,
                        calibration_version, calibration_artifact_sha256,
                        temperature, raw_logits, raw_output_hash
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              ARRAY[0.1, 0.2]::double precision[], %s)
                    """,
                    values,
                )
        with pytest.raises(errors.RaiseException, match="execution-spec"):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    INSERT INTO groundloop_m4_verification_execution (
                        observation_id, job_id, admitted_pair_id,
                        model_artifact_id, prompt_artifact_id,
                        execution_spec_hash, pair_input_hash,
                        calibration_version, calibration_artifact_sha256,
                        temperature, raw_logits, raw_output_hash
                    ) VALUES ('observation', 'verify', 'admitted', 'verifier',
                              'verify-prompt', %s, %s, 'temperature-v1', %s,
                              1.1, ARRAY[0.1, 0.2, 0.3], %s)
                    """,
                    ("f" * 64, "1" * 64, "2" * 64, "3" * 64),
                )
        live_connection.execute(
            """
            INSERT INTO groundloop_m4_verification_execution (
                observation_id, job_id, admitted_pair_id, model_artifact_id,
                prompt_artifact_id, execution_spec_hash, pair_input_hash,
                calibration_version, calibration_artifact_sha256, temperature,
                raw_logits, raw_output_hash
            ) VALUES ('observation', 'verify', 'admitted', 'verifier',
                      'verify-prompt', %s, %s, 'temperature-v1', %s, 1.1,
                      ARRAY[0.1, 0.2, 0.3], %s)
            """,
            (HASH, "1" * 64, "2" * 64, "3" * 64),
        )
        assert live_connection.execute(
            """
            SELECT job_id, admitted_pair_id, execution_spec_hash,
                   pair_input_hash, calibration_artifact_sha256,
                   cardinality(raw_logits)
            FROM groundloop_m4_verification_execution
            """
        ).fetchone() == (
            "verify",
            "admitted",
            HASH,
            "1" * 64,
            "2" * 64,
            3,
        )
        with pytest.raises(errors.RaiseException, match="immutable M4 table"):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    UPDATE groundloop_m4_verification_execution
                    SET temperature = 1.2 WHERE observation_id = 'observation'
                    """
                )

        connection_row = live_connection.execute(
            """
            INSERT INTO groundloop_semantic_job (
                job_id, epoch_id, job_kind, candidate_policy_id, payload_hash,
                execution_spec_hash, claim_id, expandable, job_state,
                created_revision
            ) VALUES ('frontier', %s, 'frontier_retrieve', 'candidate-v1', %s,
                      %s, 'claim', true, 'declared', 1)
            RETURNING job_id
            """,
            (epoch_id, HASH, HASH),
        ).fetchone()
        assert connection_row == ("frontier",)
        with pytest.raises(errors.RaiseException, match="VERIFY_PAIR"):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    INSERT INTO groundloop_m4_verification_execution (
                        observation_id, job_id, admitted_pair_id,
                        model_artifact_id, prompt_artifact_id,
                        execution_spec_hash, pair_input_hash,
                        calibration_version, calibration_artifact_sha256,
                        temperature, raw_logits, raw_output_hash
                    ) VALUES ('observation', 'frontier', 'admitted', 'verifier',
                              'verify-prompt', %s, %s, 'temperature-v1', %s,
                              1.1, ARRAY[0.1, 0.2, 0.3], %s)
                    """,
                    (HASH, "1" * 64, "2" * 64, "3" * 64),
                )
