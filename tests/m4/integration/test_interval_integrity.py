from __future__ import annotations

from typing import Any

from psycopg import Connection


def test_published_interval_relations_have_exclusion_constraints(
    m4_pipeline_connection: Connection[Any],
) -> None:
    rows = m4_pipeline_connection.execute(
        """
        SELECT relation.relname, constraint_row.conname,
               pg_get_constraintdef(constraint_row.oid)
        FROM pg_constraint AS constraint_row
        JOIN pg_class AS relation
          ON relation.oid = constraint_row.conrelid
        JOIN pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = current_schema()
          AND constraint_row.contype = 'x'
          AND relation.relname = ANY(%s)
        ORDER BY relation.relname
        """,
        (
            [
                "groundloop_candidate_frontier",
                "groundloop_published_answer_state",
                "groundloop_published_claim_state",
                "groundloop_published_observation_currency",
            ],
        ),
    ).fetchall()

    assert [str(row[0]) for row in rows] == [
        "groundloop_candidate_frontier",
        "groundloop_published_answer_state",
        "groundloop_published_claim_state",
        "groundloop_published_observation_currency",
    ]
    assert all("EXCLUDE USING gist" in str(row[2]) for row in rows)
    assert all("int8range" in str(row[2]) and "&&" in str(row[2]) for row in rows)
