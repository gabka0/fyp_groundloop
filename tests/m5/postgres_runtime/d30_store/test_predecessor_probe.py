"""D30 falsifier 4: bounded predecessor-currency discovery and rerange."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

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


def _nodes(node: dict[str, object]) -> tuple[dict[str, object], ...]:
    children = node.get("Plans", [])
    assert isinstance(children, list)
    return (
        node,
        *(
            child_node
            for child in children
            if isinstance(child, dict)
            for child_node in _nodes(child)
        ),
    )


class _OneRow:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self.row = row

    def fetchone(self) -> tuple[object, ...] | None:
        return self.row


class _ProbeCursor:
    def __init__(self, row: tuple[object, ...] | None = None) -> None:
        self.row = row
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, statement: object, parameters: tuple[object, ...]) -> _OneRow:
        self.calls.append((" ".join(str(statement).split()), parameters))
        return _OneRow(self.row)


def test_probe_is_one_backward_primary_key_candidate_then_memory_coverage(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_probe_d30_predecessor_currency")
    assert "FROM groundloop_published_observation_currency" in source
    for predicate in (
        "subject_kind = %s",
        "subject_id = %s",
        "chunk_version_id = %s",
        "task_type = %s",
        "valid_from_epoch <= %s",
    ):
        assert predicate in source
    assert "ORDER BY valid_from_epoch DESC" in source
    assert "LIMIT 1" in source
    query = source.split("FROM groundloop_published_observation_currency", maxsplit=1)[
        1
    ].split("LIMIT 1", maxsplit=1)[0]
    assert "valid_to_epoch" not in query
    assert "observation_id, valid_from_epoch, valid_to_epoch" in source
    assert source.count("ORDER BY valid_from_epoch DESC") == 1
    assert source.count("LIMIT 1") == 1
    for forbidden in ("WITH RECURSIVE", "ANY(", "while "):
        assert forbidden not in source


def test_null_predecessor_epoch_requires_null_base_without_interval_query(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_probe_d30_predecessor_currency")
    null_guard = min(
        position
        for literal in ("is None", "== None")
        if (position := source.find(literal)) >= 0
    )
    first_query = source.index("groundloop_published_observation_currency")
    assert null_guard < first_query


def test_null_predecessor_executes_no_probe() -> None:
    currency = postgres_withdrawal._D30CurrencyRow(
        "claim", "claim-a", "chunk-a", "task-a", "observation-a", 4
    )
    cursor = _ProbeCursor()
    assert (
        postgres_withdrawal._probe_d30_predecessor_currency(  # type: ignore[arg-type]
            cursor, currency, None
        )
        is None
    )
    assert cursor.calls == []


def test_preliminary_and_locked_probe_share_one_bounded_query() -> None:
    currency = postgres_withdrawal._D30CurrencyRow(
        "claim", "claim-a", "chunk-a", "task-a", "observation-a", 4
    )
    candidate = (
        "claim",
        "claim-a",
        "chunk-a",
        "task-a",
        "base-observation",
        2,
        9,
    )
    preliminary = _ProbeCursor(candidate)
    locked = _ProbeCursor(candidate)
    assert (
        postgres_withdrawal._probe_d30_predecessor_currency(  # type: ignore[arg-type]
            preliminary, currency, 7
        )
        == candidate
    )
    assert (
        postgres_withdrawal._probe_d30_predecessor_currency(  # type: ignore[arg-type]
            locked, currency, 7, lock_authority=True
        )
        == candidate
    )
    assert len(preliminary.calls) == len(locked.calls) == 1
    unlocked_sql, parameters = preliminary.calls[0]
    locked_sql, locked_parameters = locked.calls[0]
    assert parameters == locked_parameters == (*currency.full_key, 7)
    assert locked_sql == unlocked_sql + " FOR UPDATE"
    assert "ORDER BY valid_from_epoch DESC LIMIT 1" in unlocked_sql


def test_long_history_plan_visits_only_latest_candidate(d30_schema: Any) -> None:
    connection = d30_schema.connection
    source = connection.execute(
        """SELECT subject_id,chunk_version_id,produced_epoch
             FROM groundloop_semantic_observation
            WHERE subject_kind='claim'
            ORDER BY octet_length(subject_id),octet_length(chunk_version_id),
                     observation_id COLLATE "C" LIMIT 1"""
    ).fetchone()
    assert source is not None
    claim_id, chunk_id, produced_epoch = str(source[0]), str(source[1]), int(source[2])
    task = "d30-long-predecessor-task"
    observation_id = "d30-long-predecessor-observation"

    with _savepoint(connection, "d30_long_predecessor_history"):
        insert_observation(
            connection,
            observation_id=observation_id,
            subject_kind="claim",
            subject_id=claim_id,
            chunk_id=chunk_id,
            produced_epoch=produced_epoch,
            task_type=task,
        )
        epochs: list[int] = []
        for ordinal in range(42):
            event_id = f"d30-long-predecessor-{ordinal:02d}"
            row = connection.execute(
                """INSERT INTO groundloop_epoch (
                       event_id,payload_hash,revision,structural_status,
                       semantic_status,evaluation_state,publication_mode,sealed_at
                   ) VALUES (%s,%s,0,'committed','sealed','complete','strict',now())
                   RETURNING epoch_id""",
                (event_id, hashlib.sha256(event_id.encode()).hexdigest()),
            ).fetchone()
            assert row is not None
            epochs.append(int(row[0]))
        for ordinal in range(40):
            connection.execute(
                """INSERT INTO groundloop_published_observation_currency (
                       subject_kind,subject_id,chunk_version_id,task_type,
                       observation_id,valid_from_epoch,valid_to_epoch
                   ) VALUES ('claim',%s,%s,%s,%s,%s,%s)""",
                (
                    claim_id,
                    chunk_id,
                    task,
                    observation_id,
                    epochs[ordinal],
                    epochs[ordinal + 1],
                ),
            )
        predecessor = epochs[30]
        connection.execute("ANALYZE groundloop_published_observation_currency")
        explained = connection.execute(
            """EXPLAIN (ANALYZE,COSTS OFF,FORMAT JSON)
               SELECT observation_id,valid_from_epoch,valid_to_epoch
                 FROM groundloop_published_observation_currency
                WHERE subject_kind='claim' AND subject_id=%s
                  AND chunk_version_id=%s AND task_type=%s
                  AND valid_from_epoch<=%s
                ORDER BY valid_from_epoch DESC LIMIT 1""",
            (claim_id, chunk_id, task, predecessor),
        ).fetchone()
        assert explained is not None
        root = explained[0][0]["Plan"]
        assert isinstance(root, dict)
        nodes = _nodes(root)
        assert all(node.get("Node Type") != "Seq Scan" for node in nodes)
        index_nodes = tuple(
            node
            for node in nodes
            if node.get("Index Name")
            == "groundloop_published_observation_currency_pkey"
        )
        assert len(index_nodes) == 1, tuple(
            (
                node.get("Node Type"),
                node.get("Index Name"),
                node.get("Scan Direction"),
                node.get("Index Cond"),
                node.get("Rows Removed by Filter"),
            )
            for node in nodes
        )
        index = index_nodes[0]
        assert str(index.get("Scan Direction")) == "Backward"
        assert int(index.get("Actual Rows", 0)) == 1
        assert int(root.get("Actual Rows", 0)) == 1
        assert int(index.get("Rows Removed by Filter", 0)) == 0
        assert all(
            int(node.get("Rows Removed by Index Recheck", 0)) == 0 for node in nodes
        )
        condition = str(index.get("Index Cond"))
        for column in (
            "subject_kind",
            "subject_id",
            "chunk_version_id",
            "task_type",
            "valid_from_epoch",
        ):
            assert column in condition
        row = connection.execute(
            """SELECT valid_from_epoch,valid_to_epoch
                 FROM groundloop_published_observation_currency
                WHERE subject_kind='claim' AND subject_id=%s
                  AND chunk_version_id=%s AND task_type=%s
                  AND valid_from_epoch<=%s
                ORDER BY valid_from_epoch DESC LIMIT 1""",
            (claim_id, chunk_id, task, predecessor),
        ).fetchone()
        assert row == (epochs[30], epochs[31])
