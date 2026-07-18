"""Live checks for the frozen M3 artifact and run-state schema."""

from __future__ import annotations

from typing import Any

import pytest
from psycopg import Connection, errors

from groundloop.postgres import record_epoch, temporary_m2_schema


def _seed_static_objects(
    connection: Connection[tuple[object, ...]],
) -> int:
    epoch_id, _ = record_epoch(
        connection,
        event_id="m3-event",
        payload_hash="a" * 64,
    )
    connection.execute(
        "INSERT INTO groundloop_document VALUES ('doc', 'local/doc.txt', 'test')"
    )
    connection.execute(
        "INSERT INTO groundloop_document_version VALUES ('dv', 'doc', 'raw', %s, NULL)",
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
    # Flush the M2 deferred required-claim trigger before the temporary schema
    # is dropped by its context manager.
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return epoch_id


def _register_artifacts(connection: Connection[tuple[object, ...]]) -> None:
    connection.execute(
        """
        INSERT INTO groundloop_model_artifact (
            model_artifact_id, task, provider, model_id, immutable_revision,
            tokenizer_revision, license_id, config_hash
        ) VALUES ('embedder', 'embedding', 'huggingface', 'bge', 'rev', 'rev',
                  'MIT', %s)
        """,
        ("c" * 64,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_prompt_artifact (
            prompt_artifact_id, task, version, template, template_hash,
            decoding_config_hash
        ) VALUES ('prompt', 'verification', 'v1', 'template', %s, %s)
        """,
        ("d" * 64, "e" * 64),
    )
    connection.execute(
        """
        INSERT INTO groundloop_chunker_artifact VALUES
            ('chunker', 'fixed-char-v1', 'v1', %s, DEFAULT)
        """,
        ("f" * 64,),
    )


def _insert_staged_run(
    connection: Connection[tuple[object, ...]],
    *,
    run_id: str = "run",
) -> None:
    connection.execute(
        """
        INSERT INTO groundloop_pipeline_run (
            run_id, schema_version, status, config_hash, input_hash,
            corpus_hash, question_id
        ) VALUES (%s, 'm3-v1', 'staged', %s, %s, %s, 'question')
        """,
        (run_id, "1" * 64, "2" * 64, "3" * 64),
    )


def test_artifact_identity_is_reusable_but_conflicts_and_updates_fail(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    with temporary_m2_schema(live_connection):
        _seed_static_objects(live_connection)
        _register_artifacts(live_connection)

        live_connection.execute(
            """
            INSERT INTO groundloop_model_artifact (
                model_artifact_id, task, provider, model_id, immutable_revision,
                tokenizer_revision, license_id, config_hash
            ) VALUES ('embedder', 'embedding', 'huggingface', 'bge', 'rev', 'rev',
                      'MIT', %s)
            ON CONFLICT DO NOTHING
            """,
            ("c" * 64,),
        )
        assert live_connection.execute(
            "SELECT count(*) FROM groundloop_model_artifact"
        ).fetchone() == (1,)

        with pytest.raises(errors.UniqueViolation):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    INSERT INTO groundloop_model_artifact (
                        model_artifact_id, task, provider, model_id,
                        immutable_revision, tokenizer_revision, license_id,
                        config_hash
                    ) VALUES ('embedder', 'embedding', 'huggingface', 'other',
                              'rev', 'rev', 'MIT', %s)
                    """,
                    ("c" * 64,),
                )

        with pytest.raises(errors.RaiseException, match="immutable M3"):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    UPDATE groundloop_model_artifact
                    SET license_id = 'changed' WHERE model_artifact_id = 'embedder'
                    """
                )

        with pytest.raises(errors.RaiseException, match="immutable M3"):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    DELETE FROM groundloop_prompt_artifact
                    WHERE prompt_artifact_id = 'prompt'
                    """
                )


def test_embedding_dimension_candidate_rank_and_foreign_keys_are_enforced(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    with temporary_m2_schema(live_connection):
        _seed_static_objects(live_connection)
        _register_artifacts(live_connection)
        _insert_staged_run(live_connection)

        vector = [0.0] * 384
        live_connection.execute(
            """
            INSERT INTO groundloop_chunk_embedding VALUES
                ('chunk', 'embedder', %s, %s, DEFAULT)
            """,
            (vector, "4" * 64),
        )
        with pytest.raises(errors.DataException):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    INSERT INTO groundloop_chunk_embedding VALUES
                        ('chunk', 'embedder', %s, %s, DEFAULT)
                    """,
                    ([0.0] * 383, "4" * 64),
                )

        candidate_values: tuple[Any, ...] = (
            "candidate",
            "run",
            "question",
            "question",
            "chunk",
            "embedder",
            "bge-cosine-v1",
            0.9,
            1,
        )
        live_connection.execute(
            """
            INSERT INTO groundloop_retrieval_candidate (
                candidate_id, run_id, query_kind, query_id, chunk_version_id,
                embedding_model_artifact_id, method_version, score, rank
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            candidate_values,
        )
        with pytest.raises(errors.CheckViolation):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    INSERT INTO groundloop_retrieval_candidate (
                        candidate_id, run_id, query_kind, query_id,
                        chunk_version_id, embedding_model_artifact_id,
                        method_version, score, rank
                    ) VALUES ('bad-rank', 'run', 'question', 'question',
                              'chunk', 'embedder', 'bge-cosine-v1', 0.8, 0)
                    """
                )


def test_pipeline_run_has_one_way_terminal_transitions_and_stable_identity(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    with temporary_m2_schema(live_connection):
        epoch_id = _seed_static_objects(live_connection)
        _insert_staged_run(live_connection)
        live_connection.execute(
            """
            UPDATE groundloop_pipeline_run
            SET status = 'published', answer_version_id = 'answer',
                semantic_epoch_id = %s, completed_at = now()
            WHERE run_id = 'run'
            """,
            (epoch_id,),
        )
        assert live_connection.execute(
            "SELECT status FROM groundloop_pipeline_run WHERE run_id = 'run'"
        ).fetchone() == ("published",)

        with pytest.raises(errors.RaiseException, match="invalid pipeline"):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    UPDATE groundloop_pipeline_run
                    SET status = 'failed', failure_code = 'late'
                    WHERE run_id = 'run'
                    """
                )

        _insert_staged_run(live_connection, run_id="identity-change")
        with pytest.raises(errors.RaiseException, match="identity fields"):
            with live_connection.transaction():
                live_connection.execute(
                    """
                    UPDATE groundloop_pipeline_run SET input_hash = %s,
                        status = 'failed', failure_code = 'bad', completed_at = now()
                    WHERE run_id = 'identity-change'
                    """,
                    ("9" * 64,),
                )
