#!/usr/bin/env python3
"""Run the live PostgreSQL M2 three-engine validation fixture.

The fixture uses the same typed snapshot adapter as database tests. Its unique
schema is created and removed transactionally; the target database retains no
fixture rows. Unavailable database infrastructure is a hard error here, not a
skip, because this script is the explicit live-runtime gate.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

import psycopg
from psycopg import Connection

from groundloop.domain import (
    AnswerVersion,
    Claim,
    DecisionPolicy,
    ModelStamp,
    Question,
    SemanticObservation,
    SubjectKind,
)
from groundloop.events import (
    ChunkInput,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    apply_event,
)
from groundloop.incremental import IncrementalMaintenanceEngine
from groundloop.postgres import (
    MismatchCounts,
    PostgresSnapshot,
    load_snapshot,
    read_mismatch_counts,
    read_oracle_states,
    read_server_metadata,
    temporary_m2_schema,
)
from groundloop.reference import compute_all_states
from groundloop.repository import InMemoryRepository


def _psycopg_url(url: str) -> str:
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


def _fixture() -> tuple[InMemoryRepository, IncrementalMaintenanceEngine]:
    stamp = ModelStamp("postgres-validator", "v1", "p1")
    repository = InMemoryRepository()
    apply_event(
        repository,
        PolicyChangeEvent("fixture-policy", DecisionPolicy("k1", 0.8, 0.8)),
    )
    repository.register_question(Question("q", "question"))
    repository.register_answer(
        AnswerVersion("a", "q", "answer", stamp),
        (Claim("c", "a", "claim", stamp, True),),
    )
    apply_event(
        repository,
        InsertDocumentEvent(
            "fixture-insert",
            "doc",
            "dv",
            "content-dv",
            (ChunkInput("p", 0, "fixture evidence"),),
        ),
    )
    apply_event(
        repository,
        ObserveEvent(
            "fixture-observe",
            SemanticObservation(
                "o",
                SubjectKind.CLAIM,
                "c",
                "p",
                "verify",
                0.9,
                0.05,
                0.05,
                stamp,
                "input-o",
            ),
        ),
    )
    return repository, IncrementalMaintenanceEngine.from_repository(repository)


def _plan(
    connection: Connection[tuple[object, ...]], query: str
) -> tuple[str, ...]:
    rows = connection.execute(f"EXPLAIN (ANALYZE, BUFFERS) {query}").fetchall()
    return tuple(str(row[0]) for row in rows)


def validate(database_url: str) -> dict[str, Any]:
    repository, engine = _fixture()
    expected_claims, expected_answers = compute_all_states(repository)
    snapshot = PostgresSnapshot.capture(repository, engine)

    with psycopg.connect(_psycopg_url(database_url)) as connection:
        with temporary_m2_schema(connection) as schema:
            metadata = read_server_metadata(connection)
            load_snapshot(connection, snapshot)
            oracle = read_oracle_states(connection)
            mismatches = read_mismatch_counts(connection)
            assert oracle.claims == expected_claims == engine.claim_states
            assert oracle.answers == expected_answers == engine.answer_states
            assert mismatches == MismatchCounts(0, 0, 0)

            current_query = """
                SELECT observation_id FROM groundloop_observation_currency
                WHERE chunk_version_id = 'p'
            """
            policy_query = """
                SELECT observation_id FROM groundloop_semantic_observation
                WHERE support_score >= 0.8 AND support_score < 0.95
            """
            natural_current_plan = _plan(connection, current_query)
            natural_policy_plan = _plan(connection, policy_query)
            connection.execute("SET LOCAL enable_seqscan = off")
            indexed_current_plan = _plan(connection, current_query)
            indexed_policy_plan = _plan(connection, policy_query)
            current_index_usable = any(
                "groundloop_current_observations_by_chunk" in line
                for line in indexed_current_plan
            )
            policy_index_usable = any(
                "groundloop_observation_support_scores" in line
                for line in indexed_policy_plan
            )
            assert current_index_usable
            assert policy_index_usable

            return {
                "schema": schema,
                "postgres_version": metadata.postgres_version,
                "postgres_version_num": metadata.postgres_version_num,
                "pgvector_available_version": (
                    metadata.pgvector_available_version
                ),
                "pgvector_installed_version": (
                    metadata.pgvector_installed_version
                ),
                "claim_mismatches": mismatches.claims,
                "answer_mismatches": mismatches.answers,
                "invalid_certificates": mismatches.invalid_certificates,
                "current_index_usable": current_index_usable,
                "policy_index_usable": policy_index_usable,
                "natural_current_plan": natural_current_plan,
                "natural_policy_plan": natural_policy_plan,
                "indexed_current_plan": indexed_current_plan,
                "indexed_policy_plan": indexed_policy_plan,
            }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--database-url",
        default=os.environ.get("GROUNDLOOP_DATABASE_URL"),
    )
    args = parser.parse_args()
    if not args.database_url:
        parser.error("set GROUNDLOOP_DATABASE_URL or pass --database-url")
    print(json.dumps(validate(args.database_url), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
