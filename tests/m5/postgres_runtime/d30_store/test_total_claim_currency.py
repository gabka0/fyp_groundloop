"""D30 falsifiers 1 and 12: total mixed-subject current-currency location."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from groundloop.m4.application import ApplicationExecutionPolicy
from groundloop.m5.runtime import postgres_withdrawal
from tests.m5.postgres.helpers import insert_observation


@contextmanager
def _savepoint(connection: Any, name: str) -> Any:
    connection.execute(f"SAVEPOINT {name}")
    try:
        yield
    finally:
        connection.execute(f"ROLLBACK TO SAVEPOINT {name}")
        connection.execute(f"RELEASE SAVEPOINT {name}")


def test_total_locator_reads_full_rows_once_then_partitions(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_gather_d29_locator_authority")
    assert source.count("FROM groundloop_observation_currency") == 1
    locator = source.split("FROM groundloop_observation_currency", maxsplit=1)[0]
    for column in (
        "subject_kind::text",
        "subject_id",
        "chunk_version_id",
        "task_type",
        "observation_id",
        "installed_revision",
    ):
        assert column in locator
    query_tail = source.split("FROM groundloop_observation_currency", maxsplit=1)[1]
    assert "WHERE chunk_version_id = %s" in query_tail
    assert "subject_kind = 'claim'" not in query_tail.split(")", maxsplit=1)[0]
    assert "subject_kind = 'requirement'" not in query_tail.split(")", maxsplit=1)[0]
    for ordering in (
        'subject_kind::text COLLATE "C"',
        'subject_id COLLATE "C"',
        'chunk_version_id COLLATE "C"',
        'task_type COLLATE "C"',
        'observation_id COLLATE "C"',
    ):
        assert ordering in query_tail
    assert "d30_claim_currency_rows.append(currency)" in source
    assert "requirement_currency_keys.add" in source


def test_one_live_chunk_range_returns_claim_and_requirement_without_conversion(
    d30_schema: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = d30_schema.connection
    event = d30_schema.document_plan("delete", tag="d30-mixed-total-locator")
    claim = connection.execute(
        """SELECT subject_id,chunk_version_id,observation_id,installed_revision
             FROM groundloop_observation_currency
            WHERE subject_kind='claim'
            ORDER BY observation_id COLLATE "C" LIMIT 1"""
    ).fetchone()
    assert claim is not None
    assert event.requirement_registry_snapshot.entries
    claim_id, chunk_id = str(claim[0]), str(claim[1])
    requirement_id = event.requirement_registry_snapshot.entries[
        0
    ].requirement_version_id
    observation_id = "d30-mixed-requirement-observation"
    task = "d30-mixed-requirement-task"

    with _savepoint(connection, "d30_mixed_total_locator"):
        insert_observation(
            connection,
            observation_id=observation_id,
            subject_kind="requirement",
            subject_id=requirement_id,
            chunk_id=chunk_id,
            produced_epoch=d30_schema.database.base.epoch_id,
            task_type=task,
        )
        connection.execute(
            """INSERT INTO groundloop_observation_currency (
                   subject_kind,subject_id,chunk_version_id,task_type,
                   observation_id,installed_revision
               ) VALUES ('requirement',%s,%s,%s,%s,%s)""",
            (requirement_id, chunk_id, task, observation_id, int(claim[3])),
        )
        expected_rows = tuple(
            tuple(row)
            for row in connection.execute(
                """SELECT subject_kind::text,subject_id,chunk_version_id,
                          task_type,observation_id,installed_revision
                     FROM groundloop_observation_currency
                    WHERE chunk_version_id=%s
                    ORDER BY subject_kind::text COLLATE "C",
                             subject_id COLLATE "C",
                             chunk_version_id COLLATE "C",
                             task_type COLLATE "C",
                             observation_id COLLATE "C"
                """,
                (chunk_id,),
            ).fetchall()
        )
        expected_claims = tuple(
            postgres_withdrawal._D30CurrencyRow(
                subject_kind=str(row[0]),
                subject_id=str(row[1]),
                chunk_version_id=str(row[2]),
                task_type=str(row[3]),
                observation_id=str(row[4]),
                installed_revision=int(row[5]),
            )
            for row in expected_rows
            if str(row[0]) == "claim"
        )
        expected_requirement_keys = tuple(
            (str(row[1]), str(row[2]), str(row[3]), str(row[4]))
            for row in expected_rows
            if str(row[0]) == "requirement"
        )
        assert expected_claims and expected_requirement_keys
        captured_claims: list[postgres_withdrawal._D30CurrencyRow] = []

        def capture_claims(
            _cursor: object,
            currency_rows: object,
        ) -> postgres_withdrawal._D30ClaimLocatorAuthority:
            rows = tuple(currency_rows)  # type: ignore[arg-type]
            captured_claims.extend(rows)
            return postgres_withdrawal._D30ClaimLocatorAuthority(
                currency_rows=rows,
                dynamic=(),
                bootstrap=(),
                owners=(),
                activation_row=None,
            )

        monkeypatch.setattr(
            postgres_withdrawal,
            "_gather_d30_claim_authority",
            capture_claims,
        )
        monkeypatch.setattr(
            postgres_withdrawal,
            "_gather_d29_direct_state_locators",
            lambda *_args, **_kwargs: ((), (), (), (), ()),
        )
        statements: list[str] = []

        class _TracingCursor:
            def __init__(self, wrapped: Any) -> None:
                self.wrapped = wrapped

            def execute(self, statement: object, parameters: object = None) -> Any:
                statements.append(" ".join(str(statement).split()))
                return self.wrapped.execute(statement, parameters)

        closure = SimpleNamespace(
            previous_epoch_id=d30_schema.database.base.epoch_id,
            candidate_policy=d30_schema.database.manifest,
        )
        execution_policy = ApplicationExecutionPolicy(
            "a" * 64,
            "b" * 64,
            d30_schema.database.manifest.verifier_execution_spec_hash,
        )
        with connection.cursor() as raw_cursor:
            locator = postgres_withdrawal._gather_d29_locator_authority(
                _TracingCursor(raw_cursor),  # type: ignore[arg-type]
                event,
                closure,  # type: ignore[arg-type]
                execution_policy=execution_policy,
                source_chunks=(chunk_id,),
            )

        total_ranges = tuple(
            statement
            for statement in statements
            if "FROM groundloop_observation_currency" in statement
            and "WHERE chunk_version_id = %s" in statement
        )
        assert len(total_ranges) == 1
        assert "subject_kind = 'claim'" not in total_ranges[0]
        assert "subject_kind = 'requirement'" not in total_ranges[0]
        assert tuple(captured_claims) == expected_claims
        assert locator.requirement_currency_keys == expected_requirement_keys
        assert len(captured_claims) + len(locator.requirement_currency_keys) == len(
            expected_rows
        )
        assert any(row.subject_id == claim_id for row in captured_claims)
        assert (requirement_id, chunk_id, task, observation_id) in (
            locator.requirement_currency_keys
        )
        assert all(row.subject_kind == "claim" for row in captured_claims)


def test_claim_and_requirement_outputs_remain_independent(
    function_source: Callable[[str], str],
) -> None:
    prepare = function_source("_prepare_locked_document_open")
    continuation = function_source("_continue_locked_document_open")
    gather = function_source("_gather_d29_locator_authority")
    assert "locator.requirement_currency_keys" in prepare
    assert "_gather_d30_claim_authority" in gather
    assert "plan_requirement_withdrawal" in continuation
    assert "_locked_direct_withdrawal" in continuation
    assert continuation.index("plan_requirement_withdrawal") < continuation.index(
        "_locked_direct_withdrawal"
    )
    dynamic = function_source("_validate_d30_dynamic_claim")
    assert "task_type" in dynamic
    assert 'task_type != "verify"' not in dynamic
    assert 'task_type == "verify"' not in dynamic
