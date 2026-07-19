"""Live failure-isolation tests for M4 working observation overlays."""

from __future__ import annotations

from typing import Any

import pytest
from psycopg import Connection, errors

from groundloop.postgres import record_epoch, temporary_m2_schema

HASH = "a" * 64


def _seed_registered_claim(connection: Connection[Any], epoch_id: int) -> None:
    connection.execute(
        "INSERT INTO groundloop_document VALUES ('doc', 'doc://source', 'test')"
    )
    connection.execute(
        "INSERT INTO groundloop_document_version VALUES "
        "('dv', 'doc', 'content', %s, NULL)",
        (epoch_id,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_chunk_version VALUES
          ('chunk', 'dv', 0, 'evidence', %s, 'fixed-char-v1', %s, NULL)
        """,
        ("b" * 64, epoch_id),
    )
    connection.execute(
        "INSERT INTO groundloop_question VALUES ('question', 'question?', %s)",
        (epoch_id,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_answer_version VALUES
          ('answer', 'question', 'answer', 'generator', 'revision', 'prompt', %s)
        """,
        (epoch_id,),
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
        (epoch_id,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_model_artifact (
            model_artifact_id, task, provider, model_id, immutable_revision,
            tokenizer_revision, license_id, config_hash
        ) VALUES ('embedder', 'embedding', 'test', 'embedder', 'rev', 'rev',
                  'MIT', %s)
        """,
        ("c" * 64,),
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
            '16.14', 'simple', 'registry-1', 1, 'rank-interleave-v1', 4, 2,
            '{}'::jsonb
        )
        """,
        ("d" * 64, "e" * 64, "f" * 64, "1" * 64, "2" * 64, "3" * 64),
    )


def _insert_observation(
    connection: Connection[Any], observation_id: str, epoch_id: int, score: float
) -> None:
    connection.execute(
        """
        INSERT INTO groundloop_semantic_observation (
            observation_id, subject_kind, subject_id, chunk_version_id,
            task_type, support_score, refute_score, neutral_score, model_id,
            model_version, prompt_version, input_hash, produced_epoch
        ) VALUES (%s, 'claim', 'claim', 'chunk', 'nli', %s, %s, 0.05,
                  'model', 'revision', 'prompt', %s, %s)
        """,
        (observation_id, score, 0.95 - score, HASH, epoch_id),
    )


def _open_update(
    connection: Connection[Any], *, event_id: str, previous: int
) -> int:
    epoch_id, created = record_epoch(
        connection, event_id=event_id, payload_hash=HASH
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
        (epoch_id, previous),
    )
    return epoch_id


def test_pending_and_failed_work_never_mutate_published_currency(
    live_connection: Connection[Any],
) -> None:
    with temporary_m2_schema(live_connection):
        published_epoch, _ = record_epoch(
            live_connection, event_id="published", payload_hash="4" * 64
        )
        live_connection.execute(
            """
            UPDATE groundloop_epoch
            SET structural_status = 'committed', semantic_status = 'sealed',
                evaluation_state = 'complete', publication_mode = 'strict',
                sealed_at = now()
            WHERE epoch_id = %s
            """,
            (published_epoch,),
        )
        _seed_registered_claim(live_connection, published_epoch)
        _insert_observation(live_connection, "old-observation", published_epoch, 0.9)
        live_connection.execute(
            """
            INSERT INTO groundloop_observation_currency VALUES
              ('claim', 'claim', 'chunk', 'nli', 'old-observation', 1)
            """
        )
        live_connection.execute(
            "INSERT INTO groundloop_m4_publication_head(epoch_id) VALUES (%s)",
            (published_epoch,),
        )
        live_connection.execute(
            """
            INSERT INTO groundloop_published_observation_currency VALUES
              ('claim', 'claim', 'chunk', 'nli', 'old-observation', %s, NULL)
            """,
            (published_epoch,),
        )

        failed_epoch = _open_update(
            live_connection, event_id="failed-update", previous=published_epoch
        )
        _insert_observation(live_connection, "failed-observation", failed_epoch, 0.1)
        live_connection.execute(
            """
            INSERT INTO groundloop_working_observation_delta VALUES
              (%s, 'claim', 'claim', 'chunk', 'nli', 'old-observation',
               'failed-observation', 2)
            """,
            (failed_epoch,),
        )

        assert live_connection.execute(
            "SELECT observation_id FROM groundloop_observation_currency"
        ).fetchone() == ("old-observation",)
        assert live_connection.execute(
            """
            SELECT observation_id
            FROM groundloop_m4_effective_observation_currency
            WHERE epoch_id = %s
            """,
            (failed_epoch,),
        ).fetchone() == ("failed-observation",)
        assert live_connection.execute(
            """
            SELECT support_count, refute_count, status
            FROM groundloop_m4_claim_state_oracle
            WHERE epoch_id = %s AND claim_id = 'claim'
            """,
            (failed_epoch,),
        ).fetchone() == (0, 1, "refuted")

        live_connection.execute(
            """
            UPDATE groundloop_epoch
            SET structural_status = 'failed', semantic_status = 'failed',
                evaluation_state = 'failed'
            WHERE epoch_id = %s
            """,
            (failed_epoch,),
        )
        next_epoch = _open_update(
            live_connection, event_id="next-update", previous=published_epoch
        )
        assert live_connection.execute(
            """
            SELECT observation_id
            FROM groundloop_m4_effective_observation_currency
            WHERE epoch_id = %s
            """,
            (next_epoch,),
        ).fetchone() == ("old-observation",)
        assert live_connection.execute(
            """
            SELECT support_count, refute_count, status
            FROM groundloop_m4_claim_state_oracle
            WHERE epoch_id = %s AND claim_id = 'claim'
            """,
            (next_epoch,),
        ).fetchone() == (1, 0, "supported")
        assert live_connection.execute(
            """
            SELECT supported_count, refuted_count, status
            FROM groundloop_m4_answer_state_oracle
            WHERE epoch_id = %s AND answer_version_id = 'answer'
            """,
            (next_epoch,),
        ).fetchone() == (1, 0, "valid")
        assert live_connection.execute(
            "SELECT epoch_id FROM groundloop_m4_publication_head"
        ).fetchone() == (published_epoch,)
        live_connection.execute("SET CONSTRAINTS ALL IMMEDIATE")


def test_working_delta_is_immutable_and_key_bound(
    live_connection: Connection[Any],
) -> None:
    with temporary_m2_schema(live_connection):
        published_epoch, _ = record_epoch(
            live_connection, event_id="published", payload_hash="4" * 64
        )
        live_connection.execute(
            """
            UPDATE groundloop_epoch
            SET structural_status = 'committed', semantic_status = 'sealed',
                evaluation_state = 'complete', publication_mode = 'strict',
                sealed_at = now()
            WHERE epoch_id = %s
            """,
            (published_epoch,),
        )
        _seed_registered_claim(live_connection, published_epoch)
        _insert_observation(live_connection, "old-observation", published_epoch, 0.9)
        live_connection.execute(
            "INSERT INTO groundloop_m4_publication_head(epoch_id) VALUES (%s)",
            (published_epoch,),
        )
        live_connection.execute(
            """
            INSERT INTO groundloop_published_observation_currency VALUES
              ('claim', 'claim', 'chunk', 'nli', 'old-observation', %s, NULL)
            """,
            (published_epoch,),
        )
        epoch_id = _open_update(
            live_connection, event_id="working", previous=published_epoch
        )
        _insert_observation(live_connection, "new-observation", epoch_id, 0.1)
        live_connection.execute(
            """
            INSERT INTO groundloop_working_observation_delta VALUES
              (%s, 'claim', 'claim', 'chunk', 'nli', 'old-observation',
               'new-observation', 2)
            """,
            (epoch_id,),
        )
        with pytest.raises(errors.RaiseException, match="immutable"):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    UPDATE groundloop_working_observation_delta
                    SET installed_revision = 3 WHERE epoch_id = %s
                    """,
                    (epoch_id,),
                )
        live_connection.execute("SET CONSTRAINTS ALL IMMEDIATE")


def test_database_rejects_two_open_structural_epochs(
    live_connection: Connection[Any],
) -> None:
    with temporary_m2_schema(live_connection):
        first, _ = record_epoch(
            live_connection, event_id="first-open", payload_hash="5" * 64
        )
        live_connection.execute(
            """
            UPDATE groundloop_epoch
            SET structural_status = 'committed'
            WHERE epoch_id = %s
            """,
            (first,),
        )
        second, _ = record_epoch(
            live_connection, event_id="second-open", payload_hash="6" * 64
        )
        with pytest.raises(errors.UniqueViolation):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    UPDATE groundloop_epoch
                    SET structural_status = 'committed'
                    WHERE epoch_id = %s
                    """,
                    (second,),
                )
