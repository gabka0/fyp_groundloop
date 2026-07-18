#!/usr/bin/env python3
"""Apply M2 SQL in a temporary schema and validate a three-path fixture.

The transaction is always rolled back, so the target database is unchanged.
Requires the normal project dependency ``psycopg`` and a running PostgreSQL 16
instance. It intentionally fails rather than silently skipping unavailable DB
infrastructure.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def _normalized_text_hash(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text.strip())
    return hashlib.sha256(normalized.encode()).hexdigest()


def _psycopg_url(url: str) -> str:
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


def validate(database_url: str) -> dict[str, Any]:
    import psycopg
    from psycopg import sql

    migration = (ROOT / "migrations/001_m2_base.sql").read_text()
    oracle = (ROOT / "sql/m2_full_recompute_oracle.sql").read_text()
    schema = f"groundloop_m2_validation_{uuid.uuid4().hex}"

    with psycopg.connect(_psycopg_url(database_url)) as connection:
        try:
            connection.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
            )
            connection.execute(
                sql.SQL("SET LOCAL search_path TO {}, public").format(
                    sql.Identifier(schema)
                )
            )
            connection.execute(migration)
            connection.execute(oracle)

            epoch_id = connection.execute(
                """
                INSERT INTO groundloop_epoch (
                    event_id, payload_hash, structural_status,
                    semantic_status, evaluation_state, publication_mode
                ) VALUES (
                    'fixture-event', %s, 'committed', 'complete', 'complete',
                    'provisional'
                )
                RETURNING epoch_id
                """,
                ("0" * 64,),
            ).fetchone()[0]
            connection.execute(
                """
                INSERT INTO groundloop_document
                    (document_id, source_uri, authority_class)
                VALUES ('doc', 'fixture://doc', 'fixture')
                """
            )
            connection.execute(
                """
                INSERT INTO groundloop_document_version
                    (document_version_id, document_id, content_hash,
                     valid_from_epoch)
                VALUES ('dv', 'doc', 'content-dv', %s)
                """,
                (epoch_id,),
            )
            connection.execute(
                """
                INSERT INTO groundloop_chunk_version
                    (chunk_version_id, document_version_id, chunk_index, text,
                     text_hash, chunker_version, valid_from_epoch)
                VALUES ('p', 'dv', 0, 'fixture evidence', %s, 'fixed-v1', %s)
                """,
                (_normalized_text_hash("fixture evidence"), epoch_id),
            )
            connection.execute(
                """
                INSERT INTO groundloop_question (question_id, text, created_epoch)
                VALUES ('q', 'question', %s)
                """,
                (epoch_id,),
            )
            connection.execute(
                """
                INSERT INTO groundloop_answer_version (
                    answer_version_id, question_id, text, generator_model_id,
                    generator_model_version, prompt_version, created_epoch
                ) VALUES ('a', 'q', 'answer', 'fixture', 'v1', 'p1', %s)
                """,
                (epoch_id,),
            )
            connection.execute(
                """
                INSERT INTO groundloop_claim (
                    claim_id, answer_version_id, text, extractor_model_id,
                    extractor_model_version, extractor_prompt_version, required
                ) VALUES ('c', 'a', 'claim', 'fixture', 'v1', 'p1', true)
                """
            )
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
            connection.execute("SET CONSTRAINTS ALL DEFERRED")
            connection.execute(
                """
                INSERT INTO groundloop_decision_policy (
                    policy_version, support_threshold, refute_threshold,
                    tie_rule_version, valid_from_epoch
                ) VALUES ('k1', 0.8, 0.8, 'v1', %s)
                """,
                (epoch_id,),
            )
            connection.execute(
                """
                INSERT INTO groundloop_semantic_observation (
                    observation_id, subject_kind, subject_id, chunk_version_id,
                    task_type, support_score, refute_score, neutral_score,
                    model_id, model_version, prompt_version, input_hash,
                    produced_epoch
                ) VALUES (
                    'o', 'claim', 'c', 'p', 'verify', 0.9, 0.05, 0.05,
                    'fixture', 'v1', 'p1', 'input-o', %s
                )
                """,
                (epoch_id,),
            )
            connection.execute(
                """
                INSERT INTO groundloop_observation_currency (
                    subject_kind, subject_id, chunk_version_id, task_type,
                    observation_id, installed_revision
                ) VALUES ('claim', 'c', 'p', 'verify', 'o', 1)
                """
            )
            connection.execute(
                """
                INSERT INTO groundloop_claim_state_materialized VALUES (
                    'c', 1, 0, 0.9, NULL, ARRAY['o'], ARRAY[]::text[],
                    'supported', %s, 1
                )
                """,
                (epoch_id,),
            )
            connection.execute(
                """
                INSERT INTO groundloop_answer_state_materialized VALUES (
                    'a', 1, 1, 0, 0, 0, 'valid', %s, 1
                )
                """,
                (epoch_id,),
            )
            connection.execute(
                """
                INSERT INTO groundloop_claim_certificate VALUES (
                    'c', 'o', NULL, %s, 1
                )
                """,
                (epoch_id,),
            )

            claim = connection.execute(
                """
                SELECT support_count, refute_count, best_support_score,
                       supporting_observation_ids, status
                FROM groundloop_claim_state_oracle WHERE claim_id = 'c'
                """
            ).fetchone()
            answer = connection.execute(
                """
                SELECT required_claim_count, supported_count, status
                FROM groundloop_answer_state_oracle
                WHERE answer_version_id = 'a'
                """
            ).fetchone()
            claim_mismatches = connection.execute(
                "SELECT count(*) FROM groundloop_claim_state_mismatches"
            ).fetchone()[0]
            answer_mismatches = connection.execute(
                "SELECT count(*) FROM groundloop_answer_state_mismatches"
            ).fetchone()[0]
            invalid_certificates = connection.execute(
                """
                SELECT count(*)
                FROM groundloop_claim_certificate_validity_oracle
                WHERE NOT certificate_valid
                """
            ).fetchone()[0]

            assert tuple(claim) == (1, 0, 0.9, ["o"], "supported")
            assert tuple(answer) == (1, 1, "valid")
            assert claim_mismatches == 0
            assert answer_mismatches == 0
            assert invalid_certificates == 0
            return {
                "schema": schema,
                "claim_mismatches": claim_mismatches,
                "answer_mismatches": answer_mismatches,
                "invalid_certificates": invalid_certificates,
            }
        finally:
            connection.rollback()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--database-url",
        default=os.environ.get("GROUNDLOOP_DATABASE_URL"),
    )
    args = parser.parse_args()
    if not args.database_url:
        parser.error("set GROUNDLOOP_DATABASE_URL or pass --database-url")
    print(validate(args.database_url))


if __name__ == "__main__":
    main()
