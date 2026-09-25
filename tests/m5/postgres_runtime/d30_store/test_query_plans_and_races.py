"""D30 falsifiers 13--14: bounded plans, isolation and race reranges."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Event
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest
from psycopg import sql

from groundloop.errors import EventConflictError
from groundloop.m4.contracts import (
    CandidatePolicyManifest,
    ChildClosure,
    JobCompletion,
    JobState,
    VectorIndexKind,
    stable_m4_digest,
)
from groundloop.m5.runtime import digests as runtime_digests
from groundloop.m5.runtime import postgres_withdrawal
from groundloop.m5.runtime.contracts import M5RuntimeWork
from tests.m5.postgres.helpers import insert_observation


def _plan_nodes(node: dict[str, object]) -> tuple[dict[str, object], ...]:
    children = node.get("Plans", [])
    assert isinstance(children, list)
    return (
        node,
        *(
            nested
            for child in children
            if isinstance(child, dict)
            for nested in _plan_nodes(child)
        ),
    )


@contextmanager
def _savepoint(connection: Any, name: str) -> Any:
    connection.execute(f"SAVEPOINT {name}")
    try:
        yield
    finally:
        connection.execute(f"ROLLBACK TO SAVEPOINT {name}")
        connection.execute(f"RELEASE SAVEPOINT {name}")


@contextmanager
def _reconnect(d30_schema: Any) -> Any:
    dsn = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    assert dsn is not None
    normalized_dsn = dsn.replace("postgresql+psycopg://", "postgresql://", 1)
    with psycopg.connect(normalized_dsn) as connection:
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(d30_schema.schema_name)
            )
        )
        connection.commit()
        yield connection


def _analyze(
    connection: Any, statement: str, parameters: tuple[object, ...]
) -> tuple[dict[str, object], ...]:
    row = connection.execute(
        "EXPLAIN (ANALYZE,COSTS OFF,FORMAT JSON) " + statement,
        parameters,
    ).fetchone()
    assert row is not None
    root = row[0][0]["Plan"]
    assert isinstance(root, dict)
    return _plan_nodes(root)


def _assert_exact_index_route(
    nodes: tuple[dict[str, object], ...],
    *,
    index_name: str,
    expected_rows: int,
) -> None:
    indexes = tuple(node for node in nodes if node.get("Index Name") == index_name)
    assert len(indexes) == 1
    assert int(indexes[0].get("Actual Rows", 0)) == expected_rows
    assert all(int(node.get("Rows Removed by Filter", 0)) == 0 for node in nodes), (
        index_name,
        tuple(
            (
                node.get("Node Type"),
                node.get("Relation Name"),
                node.get("Index Name"),
                node.get("Rows Removed by Filter"),
            )
            for node in nodes
        ),
    )
    assert all(
        int(node.get("Rows Removed by Index Recheck", 0)) == 0 for node in nodes
    ), (
        index_name,
        tuple(
            (
                node.get("Node Type"),
                node.get("Relation Name"),
                node.get("Index Name"),
                node.get("Rows Removed by Index Recheck"),
            )
            for node in nodes
        ),
    )


@dataclass(frozen=True, slots=True)
class _F13Route:
    """One exact SQL route in the executable D30 physical-route manifest."""

    label: str
    cardinality_term: str
    statement: str
    parameters: tuple[object, ...]
    index_name: str | tuple[str, ...]
    expected_rows: int


def _assert_default_point_plan(connection: Any, route: _F13Route) -> None:
    """Prove a manifest point/range under the unmodified PostgreSQL planner."""

    nodes = _analyze(connection, route.statement, route.parameters)
    expected_indexes = (
        (route.index_name,) if isinstance(route.index_name, str) else route.index_name
    )
    try:
        matching_indexes = tuple(
            node for node in nodes if node.get("Index Name") in expected_indexes
        )
        assert matching_indexes
        assert any(
            int(node.get("Actual Rows", 0)) == route.expected_rows
            for node in matching_indexes
        )
        assert all(int(node.get("Rows Removed by Filter", 0)) == 0 for node in nodes)
        assert all(
            int(node.get("Rows Removed by Index Recheck", 0)) == 0 for node in nodes
        )
    except AssertionError as error:
        raise AssertionError(
            (
                route.label,
                expected_indexes,
                tuple(
                    (
                        node.get("Node Type"),
                        node.get("Index Name"),
                        node.get("Actual Rows"),
                        node.get("Index Cond"),
                        node.get("Filter"),
                    )
                    for node in nodes
                ),
            )
        ) from error
    assert all(node.get("Node Type") != "Seq Scan" for node in nodes), route.label


def _assert_captured_default_plan(
    connection: Any,
    entry: dict[str, object],
    *,
    expected_indexes: tuple[str, ...] | None = None,
    expected_rows: int,
    expected_any_indexes: tuple[str, ...] = (),
    allowed_seq_relations: frozenset[str] = frozenset(),
    maximum_index_rows: int | None = 1,
) -> tuple[dict[str, object], ...]:
    """Plan the exact SQL bytes previously executed by a production helper."""

    statement = str(entry["sql"])
    parameters = tuple(entry["parameters"])  # type: ignore[arg-type]
    nodes = _analyze(connection, statement, parameters)
    assert int(nodes[0].get("Actual Rows", 0)) == expected_rows
    assert all(int(node.get("Rows Removed by Filter", 0)) == 0 for node in nodes), (
        statement,
        tuple(
            (
                node.get("Node Type"),
                node.get("Relation Name"),
                node.get("Index Name"),
                node.get("Rows Removed by Filter"),
            )
            for node in nodes
        ),
    )
    assert all(
        int(node.get("Rows Removed by Index Recheck", 0)) == 0 for node in nodes
    ), (
        statement,
        tuple(
            (
                node.get("Node Type"),
                node.get("Relation Name"),
                node.get("Index Name"),
                node.get("Rows Removed by Index Recheck"),
            )
            for node in nodes
        ),
    )
    for index_name in expected_indexes or ():
        matches = tuple(node for node in nodes if node.get("Index Name") == index_name)
        assert matches, (
            statement,
            index_name,
            tuple(
                (
                    node.get("Node Type"),
                    node.get("Relation Name"),
                    node.get("Index Name"),
                )
                for node in nodes
            ),
        )
        if maximum_index_rows is not None:
            assert all(
                int(node.get("Actual Rows", 0)) <= maximum_index_rows
                for node in matches
            ), (
                statement,
                index_name,
                maximum_index_rows,
                tuple(node.get("Actual Rows") for node in matches),
            )
    if expected_any_indexes:
        alternatives = tuple(
            node for node in nodes if node.get("Index Name") in expected_any_indexes
        )
        assert alternatives, (statement, expected_any_indexes)
        if maximum_index_rows is not None:
            assert all(
                int(node.get("Actual Rows", 0)) <= maximum_index_rows
                for node in alternatives
            ), (
                statement,
                expected_any_indexes,
                maximum_index_rows,
                tuple(node.get("Actual Rows") for node in alternatives),
            )
    unexpected_sequential = tuple(
        str(node.get("Relation Name"))
        for node in nodes
        if node.get("Node Type") == "Seq Scan"
        and str(node.get("Relation Name")) not in allowed_seq_relations
    )
    assert not unexpected_sequential, (statement, unexpected_sequential)
    return nodes


def _hash_expression(prefix: str) -> str:
    return (
        f"md5('{prefix}:'||series.value::text)"
        f"||md5('{prefix}:tail:'||series.value::text)"
    )


def _inflate_for_default_planner(
    connection: Any,
    relation: str,
    *,
    overrides: dict[str, str],
    count: int = 384,
    disable_triggers: bool = True,
) -> None:
    """Add unrelated physical-plan ballast inside the caller's savepoint.

    The rows are deliberately unrelated to every asserted coordinate.  Triggers
    are disabled only for these cloned ballast rows; CHECK and UNIQUE indexes
    remain active, and the surrounding savepoint restores both data and trigger
    state.  This makes a default-planner index choice observable without the
    forbidden ``enable_seqscan=off`` shortcut.
    """

    assert relation.startswith("groundloop_")
    columns = tuple(
        str(row[0])
        for row in connection.execute(
            """SELECT column_name
                 FROM information_schema.columns
                WHERE table_schema=current_schema() AND table_name=%s
                  AND is_generated='NEVER'
                  AND is_identity='NO'
                ORDER BY ordinal_position""",
            (relation,),
        ).fetchall()
    )
    assert columns
    quoted_columns = ",".join(f'"{column}"' for column in columns)
    expressions = ",".join(
        overrides.get(column, f'source."{column}"') for column in columns
    )
    if disable_triggers:
        connection.execute(f"ALTER TABLE {relation} DISABLE TRIGGER ALL")
    inserted = connection.execute(
        f"""INSERT INTO {relation} ({quoted_columns})
            SELECT {expressions}
              FROM (SELECT * FROM {relation} LIMIT 1) AS source
              CROSS JOIN generate_series(1,{count}) AS series(value)"""
    ).rowcount
    assert inserted == count
    connection.execute(f"ANALYZE {relation}")


class _RecordedResult:
    def __init__(self, wrapped: Any, entry: dict[str, object]) -> None:
        self._wrapped = wrapped
        self._entry = entry

    def fetchone(self) -> Any:
        row = self._wrapped.fetchone()
        self._entry["returned_rows"] = 0 if row is None else 1
        return row

    def fetchall(self) -> Any:
        rows = self._wrapped.fetchall()
        self._entry["returned_rows"] = len(rows)
        return rows

    def __iter__(self) -> Any:
        rows = tuple(self._wrapped)
        self._entry["returned_rows"] = len(rows)
        return iter(rows)


class _RecordingCursor:
    """Transparent cursor proxy that records executed SQL and returned width."""

    def __init__(self, wrapped: Any) -> None:
        self._wrapped = wrapped
        self.entries: list[dict[str, object]] = []

    def execute(self, statement: object, parameters: object = None) -> Any:
        entry: dict[str, object] = {
            "sql": " ".join(str(statement).split()),
            "parameters": tuple(parameters or ()),
            "returned_rows": None,
        }
        self.entries.append(entry)
        return _RecordedResult(self._wrapped.execute(statement, parameters), entry)


def _d24_hash(label: str) -> str:
    return hashlib.sha256(f"d30-f13-d24:{label}".encode()).hexdigest()


def _seed_d24_physical_owner(d30_schema: Any) -> dict[str, object]:
    """Seed one internally coherent sealed D24 owner for planner evidence only.

    This is deliberately not public-writer evidence.  Every row is scoped by the
    caller's rollback savepoint and the production locator remains the reader
    under test.
    """

    connection = d30_schema.connection
    source_epoch = int(d30_schema.first_m5["epoch_id"])
    source_m4_epoch = int(d30_schema.first_m4["epoch_id"])
    event_id = "d30-f13-d24-sealed-event"
    payload_hash = _d24_hash("payload")
    zero = M5RuntimeWork()
    counters = M5RuntimeWork.counter_names()
    relations = (
        "groundloop_m5_update",
        "groundloop_m4_update",
        "groundloop_m5_runtime_epoch",
        "groundloop_semantic_job",
        "groundloop_semantic_job_attempt",
        "groundloop_m5_direct_terminal_projection",
        "groundloop_m5_dispatch_record",
        "groundloop_m5_attempt_execution_evidence",
        "groundloop_m5_runtime_timing_contribution",
        "groundloop_m5_typed_direct_late_return_envelope",
        "groundloop_m5_expired_attempt_return",
        "groundloop_m5_post_terminal_attempt_timing",
        "groundloop_m5_post_terminal_attempt_audit",
        "groundloop_m5_runtime_work_contribution",
        "groundloop_m5_transition_call_timing",
        "groundloop_m4_evaluation_counter_transition",
        "groundloop_m5_runtime_work_accumulator",
        "groundloop_m5_runtime_timing_accumulator",
        "groundloop_m5_runtime_work",
        "groundloop_m5_event_result",
        "groundloop_m5_event_timing_coverage",
    )
    for relation in relations:
        connection.execute(f"ALTER TABLE {relation} DISABLE TRIGGER ALL")

    epoch_row = connection.execute(
        """INSERT INTO groundloop_epoch (
               event_id,payload_hash,revision,structural_status,semantic_status,
               evaluation_state,publication_mode,sealed_at
           ) VALUES (%s,%s,4,'committed','sealed','complete','strict',now())
           RETURNING epoch_id""",
        (event_id, payload_hash),
    ).fetchone()
    assert epoch_row is not None
    epoch_id = int(epoch_row[0])
    connection.execute(
        """INSERT INTO groundloop_m5_update
           SELECT %s,update_kind,previous_published_epoch_id,
                  decision_policy_version,manifest,now()
             FROM groundloop_m5_update WHERE epoch_id=%s""",
        (epoch_id, source_epoch),
    )
    connection.execute(
        """INSERT INTO groundloop_m4_update
           SELECT %s,'delete',candidate_policy_id,%s,%s,'{}'::jsonb,now()
             FROM groundloop_semantic_job
            WHERE epoch_id=%s LIMIT 1""",
        (
            epoch_id,
            int(d30_schema.database.base.epoch_id),
            str(d30_schema.first_m4["registry_snapshot_id"]),
            source_m4_epoch,
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_runtime_epoch (
               epoch_id,structural_event_id,candidate_policy_id,
               candidate_policy_manifest_hash,
               requirement_registry_snapshot_digest,active_chunk_snapshot_digest,
               expected_previous_published_epoch_id,requirement_root_set_hash,
               runtime_state,revision,open_work_count,open_scope_count,
               blocking_failure_count,opened_at,terminal_at)
           SELECT %s,%s,candidate_policy_id,candidate_policy_manifest_hash,
                  requirement_registry_snapshot_digest,active_chunk_snapshot_digest,
                  expected_previous_published_epoch_id,requirement_root_set_hash,
                  'sealed',4,0,0,0,now(),now()
             FROM groundloop_m5_runtime_epoch WHERE epoch_id=%s""",
        (epoch_id, event_id, source_epoch),
    )
    m4_identity = connection.execute(
        """SELECT candidate_policy_id,execution_spec_hash,claim_id,chunk_version_id
             FROM groundloop_semantic_job
            WHERE epoch_id=%s AND job_kind='verify_pair' LIMIT 1""",
        (source_m4_epoch,),
    ).fetchone()
    assert m4_identity is not None
    candidate_policy_id = str(m4_identity[0])
    execution_hash = str(m4_identity[1]).strip()
    claim_id = str(m4_identity[2])
    chunk_id = str(m4_identity[3])
    completed_job = "d30-f13-d24-completed-job"
    empty_job = "d30-f13-d24-empty-job"
    failed_job = "d30-f13-d24-failed-job"
    transition_id = _d24_hash("transition")
    for job_id, state, completed_revision in (
        (completed_job, "completed_active", 2),
        (empty_job, "terminal_failed", 4),
        (failed_job, "terminal_failed", 3),
    ):
        successful = state == "completed_active"
        connection.execute(
            """INSERT INTO groundloop_semantic_job (
                   job_id,epoch_id,parent_job_id,job_kind,candidate_policy_id,
                   payload_hash,execution_spec_hash,claim_id,chunk_version_id,
                   expandable,job_state,child_closed,child_set_hash,
                   completion_digest,result_artifact_id,result_artifact_hash,
                   created_revision,completed_revision,created_at,completed_at)
               VALUES (%s,%s,NULL,'verify_pair',%s,%s,%s,%s,%s,false,%s,
                       false,NULL,%s,%s,%s,1,%s,now(),now())""",
            (
                job_id,
                epoch_id,
                candidate_policy_id,
                _d24_hash(f"payload:{job_id}"),
                execution_hash,
                claim_id,
                chunk_id,
                state,
                transition_id if successful else None,
                f"{job_id}-result" if successful else None,
                _d24_hash(f"result:{job_id}") if successful else None,
                completed_revision,
            ),
        )
    completed_attempt = "d30-f13-d24-completed-attempt"
    failed_attempt = "d30-f13-d24-failed-attempt"
    for attempt_id, job_id, state, ordinal in (
        (completed_attempt, completed_job, "completed", 1),
        (failed_attempt, failed_job, "failed", 1),
    ):
        connection.execute(
            """INSERT INTO groundloop_semantic_job_attempt (
                   attempt_id,job_id,execution_spec_hash,attempt_ordinal,
                   lease_token_hash,attempt_state,lease_expires_at,started_at,
                   finished_at)
               VALUES (%s,%s,%s,%s,%s,%s,now()+interval '1 hour',now(),now())""",
            (
                attempt_id,
                job_id,
                execution_hash,
                ordinal,
                _d24_hash(f"lease:{attempt_id}"),
                state,
            ),
        )
    connection.execute(
        """INSERT INTO groundloop_m5_direct_terminal_projection (
               epoch_id,job_id,terminal_state,terminal_reason,
               m4_completion_digest,completed_revision,terminal_identity_hash)
           VALUES (%s,%s,'completed_active',NULL,%s,2,%s)""",
        (epoch_id, completed_job, transition_id, _d24_hash("projection")),
    )

    counter_columns = ",".join(counters)
    maximum_columns = ",".join(f"maximum_{name}" for name in counters)
    attempt_columns = ",".join(f"attempt_{name}" for name in counters)
    zero_placeholders = ",".join("%s" for _ in counters)
    dispatch_digests: dict[str, str] = {}
    evidence_digests: dict[str, str] = {}
    timing_digests: dict[str, str] = {}
    for ordinal, (attempt_id, job_id) in enumerate(
        ((completed_attempt, completed_job), (failed_attempt, failed_job)), start=1
    ):
        dispatch_digest = _d24_hash(f"dispatch:{attempt_id}")
        evidence_digest = _d24_hash(f"evidence:{attempt_id}")
        timing_digest = _d24_hash(f"attempt-timing:{attempt_id}")
        dispatch_digests[attempt_id] = dispatch_digest
        evidence_digests[attempt_id] = evidence_digest
        timing_digests[attempt_id] = timing_digest
        connection.execute(
            f"""INSERT INTO groundloop_m5_dispatch_record (
                   epoch_id,{maximum_columns},maximum_work_digest,subgraph,
                   attempt_id,logical_job_id,attempt_ordinal,job_kind,
                   fallback_required,dispatched_revision,lease_expires_at,
                   record_digest)
               VALUES (%s,{zero_placeholders},%s,'direct',%s,%s,1,'verify_pair',
                       false,%s,now()+interval '1 hour',%s)""",
            (
                epoch_id,
                *zero.counter_values(),
                zero.work_digest,
                attempt_id,
                job_id,
                ordinal,
                dispatch_digest,
            ),
        )
        connection.execute(
            f"""INSERT INTO groundloop_m5_attempt_execution_evidence (
                   epoch_id,{attempt_columns},attempt_work_digest,subgraph,
                   attempt_id,disposition,result_or_error_hash,
                   attempt_timing_digest,evidence_digest)
               VALUES (%s,{zero_placeholders},%s,'direct',%s,'returned',%s,%s,%s)""",
            (
                epoch_id,
                *zero.counter_values(),
                zero.work_digest,
                attempt_id,
                _d24_hash(f"output:{attempt_id}"),
                timing_digest,
                evidence_digest,
            ),
        )
        connection.execute(
            """INSERT INTO groundloop_m5_runtime_timing_contribution (
                   epoch_id,subgraph,attempt_id,execution_evidence_digest,
                   required_interval_observed,observation_digest,
                   attempt_timing_digest)
               VALUES (%s,'direct',%s,%s,false,%s,%s)""",
            (
                epoch_id,
                attempt_id,
                evidence_digest,
                _d24_hash(f"observation:{attempt_id}"),
                timing_digest,
            ),
        )

    def insert_contribution(kind: str, source_id: str, revision: int) -> str:
        key = _d24_hash(f"key:{kind}:{source_id}")
        connection.execute(
            f"""INSERT INTO groundloop_m5_runtime_work_contribution (
                   epoch_id,{counter_columns},work_digest,contribution_kind,
                   source_id,source_identity_hash,contribution_key_digest,
                   applied_revision)
               VALUES (%s,{zero_placeholders},%s,%s,%s,%s,%s,%s)""",
            (
                epoch_id,
                *zero.counter_values(),
                zero.work_digest,
                kind,
                source_id,
                _d24_hash(f"identity:{kind}:{source_id}"),
                key,
                revision,
            ),
        )
        return key

    contribution_keys: dict[tuple[str, str], str] = {}
    for attempt_id, revision in ((completed_attempt, 1), (failed_attempt, 2)):
        dispatch_digest = dispatch_digests[attempt_id]
        contribution_keys[("direct_acquisition", dispatch_digest)] = (
            insert_contribution("direct_acquisition", dispatch_digest, revision)
        )
        contribution_keys[("direct_attempt_execution", attempt_id)] = (
            insert_contribution("direct_attempt_execution", attempt_id, revision)
        )
    contribution_keys[("preterminal_late_return", failed_attempt)] = (
        insert_contribution("preterminal_late_return", failed_attempt, 3)
    )
    contribution_keys[("direct_transition", transition_id)] = insert_contribution(
        "direct_transition", transition_id, 2
    )
    publication_id = stable_m4_digest("m4-publication-v1", str(epoch_id))
    combined_hash = runtime_digests.combined_status_delta_set_digest(())
    changed_hash = runtime_digests.changed_state_set_digest(())
    seal_key = insert_contribution("seal", event_id, 4)

    def insert_transition_timing(kind: str, source_id: str, revision: int) -> None:
        connection.execute(
            """INSERT INTO groundloop_m5_transition_call_timing (
                   epoch_id,contribution_kind,source_id,contribution_key_digest,
                   anchor_revision,required_interval_observed,
                   observation_digest,transition_timing_digest)
               VALUES (%s,%s,%s,%s,%s,false,%s,%s)""",
            (
                epoch_id,
                kind,
                source_id,
                contribution_keys[(kind, source_id)],
                revision,
                _d24_hash(f"transition-observation:{kind}:{source_id}"),
                _d24_hash(f"transition-timing:{kind}:{source_id}"),
            ),
        )

    insert_transition_timing(
        "direct_acquisition", dispatch_digests[completed_attempt], 1
    )
    insert_transition_timing("direct_acquisition", dispatch_digests[failed_attempt], 2)
    insert_transition_timing("direct_transition", transition_id, 2)
    insert_transition_timing("direct_attempt_execution", failed_attempt, 2)
    connection.execute(
        """INSERT INTO groundloop_m4_evaluation_counter_transition (
               epoch_id,transition_id,payload_hash,transition_kind,
               from_revision,to_revision,override_rows_written)
           VALUES (%s,%s,%s,'delta',1,2,0)""",
        (epoch_id, transition_id, _d24_hash("transition-payload")),
    )
    connection.execute(
        f"""INSERT INTO groundloop_m5_runtime_work_accumulator (
               epoch_id,{counter_columns},work_digest,updated_revision,terminalized)
           VALUES (%s,{zero_placeholders},%s,4,true)""",
        (epoch_id, *zero.counter_values(), zero.work_digest),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_runtime_timing_accumulator (
               epoch_id,required_expected_count,required_observed_count,
               required_missing_count,postgres_server_execution_expected_count,
               postgres_server_execution_observed_count,
               postgres_server_execution_missing_count,
               postgres_lock_wait_expected_count,postgres_lock_wait_observed_count,
               postgres_lock_wait_missing_count,postgres_wal_bytes_expected_count,
               postgres_wal_bytes_observed_count,postgres_wal_bytes_missing_count,
               postgres_shared_block_reads_expected_count,
               postgres_shared_block_reads_observed_count,
               postgres_shared_block_reads_missing_count,updated_revision,terminalized)
           VALUES (%s,1,0,1,1,0,1,1,0,1,1,0,1,1,0,1,4,true)""",
        (epoch_id,),
    )
    for work_kind in ("event", "call"):
        connection.execute(
            f"""INSERT INTO groundloop_m5_runtime_work (
                   work_digest,structural_event_id,epoch_id,work_kind,{counter_columns})
               VALUES (%s,%s,%s,%s,{zero_placeholders})""",
            (zero.work_digest, event_id, epoch_id, work_kind, *zero.counter_values()),
        )
    open_binding = runtime_digests.open_event_receipt_binding_digest(
        epoch_id=epoch_id,
        replayed=False,
        already_sealed=False,
        publication_id=None,
        already_failed=False,
        failure_reason=None,
    )
    publication_binding = runtime_digests.publication_receipt_binding_digest(
        epoch_id=epoch_id, publication_id=publication_id, replayed=False
    )
    logical_hash = runtime_digests.event_run_logical_result_digest(
        event_id=event_id,
        payload_hash=payload_hash,
        epoch_id=epoch_id,
        sealed_or_failed_outcome="sealed",
        original_open_receipt_binding_hash=open_binding,
        original_publication_receipt_binding_hash=publication_binding,
        event_work_digest=zero.work_digest,
        combined_status_delta_set_hash=combined_hash,
        changed_state_set_hash=changed_hash,
        failure_reason=None,
    )
    connection.execute(
        """INSERT INTO groundloop_m5_event_result (
               structural_event_id,payload_hash,epoch_id,outcome,
               original_open_receipt_binding_hash,publication_id,
               original_publication_receipt_binding_hash,event_work_kind,
               event_work_digest,combined_status_delta_set_hash,
               changed_state_set_hash,failure_reason,logical_result_hash,
               delta_count,state_reference_count,
               coordinator_non_db_non_neural_ns,neural_wall_ns,
               postgres_roundtrip_wall_ns,external_io_wall_ns,end_to_end_wall_ns)
           VALUES (%s,%s,%s,'sealed',%s,%s,%s,'event',%s,%s,%s,NULL,%s,
                   0,0,0,0,0,0,0)""",
        (
            event_id,
            payload_hash,
            epoch_id,
            open_binding,
            publication_id,
            publication_binding,
            zero.work_digest,
            combined_hash,
            changed_hash,
            logical_hash,
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_event_timing_coverage (
               structural_event_id,epoch_id,required_expected_count,
               required_observed_count,required_missing_count,
               postgres_server_execution_expected_count,
               postgres_server_execution_observed_count,
               postgres_server_execution_missing_count,
               postgres_lock_wait_expected_count,postgres_lock_wait_observed_count,
               postgres_lock_wait_missing_count,postgres_wal_bytes_expected_count,
               postgres_wal_bytes_observed_count,postgres_wal_bytes_missing_count,
               postgres_shared_block_reads_expected_count,
               postgres_shared_block_reads_observed_count,
               postgres_shared_block_reads_missing_count,
               terminal_client_roundtrip_included)
           VALUES (%s,%s,1,0,1,1,0,1,1,0,1,1,0,1,1,0,1,false)""",
        (event_id, epoch_id),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_typed_direct_late_return_envelope (
               epoch_id,return_kind,job_id,attempt_id,result_artifact_id,
               result_artifact_hash,verification_execution_present,
               observation_eligible_for_currency,requested_make_effective,
               job_binding,attempt_binding,completion_binding,discovery_binding,
               scope_binding,verifier_binding,job_binding_digest,
               attempt_binding_digest,completion_binding_digest,
               discovery_binding_digest,scope_binding_digest,
               verifier_binding_digest,envelope_digest)
           VALUES (%s,'verifier',%s,%s,%s,%s,false,true,false,
                   '{}'::jsonb,'{}'::jsonb,'{}'::jsonb,NULL,NULL,'{}'::jsonb,
                   %s,%s,%s,NULL,NULL,%s,%s)""",
        (
            epoch_id,
            completed_job,
            completed_attempt,
            f"{completed_job}-late-result",
            _d24_hash("late-result"),
            _d24_hash("late-job-binding"),
            _d24_hash("late-attempt-binding"),
            _d24_hash("late-completion-binding"),
            _d24_hash("late-verifier-binding"),
            _d24_hash("late-envelope"),
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_expired_attempt_return (
               epoch_id,subgraph,attempt_id,logical_job_id,
               original_lease_token_hash,original_lease_expires_at,
               worker_output_digest,worker_artifact_hash,
               activity_snapshot_epoch_id,activity_snapshot_revision,
               cancellation_attribution,archive_reason,
               execution_evidence_digest,received_after_terminal,
               expired_return_digest)
           VALUES (%s,'direct',%s,%s,%s,now(),%s,%s,%s,4,
                   '{"cancelled_by_event_id":"x","cancelled_by_epoch_id":1,
                     "cancellation_reason":"x"}'::jsonb,'attempt_expired',
                   %s,true,%s)""",
        (
            epoch_id,
            completed_attempt,
            completed_job,
            _d24_hash("expired-lease"),
            _d24_hash("expired-output"),
            _d24_hash("expired-artifact"),
            epoch_id,
            evidence_digests[completed_attempt],
            _d24_hash("expired-return"),
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_post_terminal_attempt_timing (
               epoch_id,subgraph,attempt_id,required_interval_observed,
               observation_digest,attempt_timing_digest)
           VALUES (%s,'direct',%s,false,%s,%s)""",
        (
            epoch_id,
            completed_attempt,
            _d24_hash("postterminal-observation"),
            timing_digests[completed_attempt],
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_post_terminal_attempt_audit (
               epoch_id,subgraph,attempt_id,return_kind,
               return_artifact_digest,execution_evidence_digest,work_digest,
               timing_digest,terminal_logical_result_hash)
           VALUES (%s,'direct',%s,'terminal_audit_only',%s,%s,%s,%s,%s)""",
        (
            epoch_id,
            completed_attempt,
            _d24_hash("late-envelope"),
            evidence_digests[completed_attempt],
            zero.work_digest,
            timing_digests[completed_attempt],
            logical_hash,
        ),
    )
    assert seal_key
    return {
        "epoch_id": epoch_id,
        "event_id": event_id,
        "completed_attempt": completed_attempt,
        "failed_attempt": failed_attempt,
    }


def _inflate_d24_physical_routes(connection: Any, *, epoch_id: int) -> None:
    """Add independent decoys for every exact D24 production read route."""

    for relation in (
        "groundloop_m4_discovery_result",
        "groundloop_m5_event_result_delta",
        "groundloop_m5_event_result_state_reference",
    ):
        connection.execute(f"ALTER TABLE {relation} DISABLE TRIGGER ALL")
    connection.execute(
        """INSERT INTO groundloop_m4_discovery_result (
               root_job_id,epoch_id,result_artifact_id,result_artifact_hash,
               fallback_satisfied,channel_hit_count,admitted_pair_count,
               channel_set_hash,admitted_pair_set_hash)
           VALUES ('d30-f13-d24-decoy-result',%s,
                   'd30-f13-d24-decoy-result-artifact',%s,true,0,0,%s,%s)""",
        (
            epoch_id,
            _d24_hash("decoy-result"),
            _d24_hash("decoy-channel-set"),
            _d24_hash("decoy-admission-set"),
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_event_result_delta
           VALUES ('d30-f13-d24-decoy-terminal',0,'claim',
                   'd30-f13-d24-decoy-object','unsupported','supported','decoy')"""
    )
    connection.execute(
        """INSERT INTO groundloop_m5_event_result_state_reference
           VALUES ('d30-f13-d24-decoy-terminal',0,'claim_state',
                   'd30-f13-d24-decoy-object',%s,4,%s,%s)""",
        (epoch_id, _d24_hash("decoy-state"), _d24_hash("decoy-reference")),
    )
    specs: tuple[tuple[str, dict[str, str]], ...] = (
        (
            "groundloop_epoch",
            {"event_id": "'d30-f13-d24-decoy-epoch-'||series.value::text"},
        ),
        ("groundloop_m4_update", {"epoch_id": "30000000+series.value"}),
        (
            "groundloop_semantic_job",
            {
                "job_id": "'d30-f13-d24-decoy-job-'||series.value::text",
                "epoch_id": "30100000+series.value",
            },
        ),
        (
            "groundloop_semantic_job_dependency",
            {
                "epoch_id": "30200000+series.value",
                "parent_job_id": "'d30-f13-d24-decoy-parent-'||series.value::text",
                "child_job_id": "'d30-f13-d24-decoy-child-'||series.value::text",
            },
        ),
        (
            "groundloop_discovery_scope",
            {
                "root_job_id": "'d30-f13-d24-decoy-scope-'||series.value::text",
                "epoch_id": "30300000+series.value",
            },
        ),
        (
            "groundloop_semantic_job_attempt",
            {
                "attempt_id": "'d30-f13-d24-decoy-attempt-'||series.value::text",
                "job_id": "'d30-f13-d24-decoy-attempt-job-'||series.value::text",
            },
        ),
        (
            "groundloop_m4_discovery_result",
            {
                "root_job_id": "'d30-f13-d24-decoy-result-'||series.value::text",
                "result_artifact_id": (
                    "'d30-f13-d24-decoy-result-artifact-'||series.value::text"
                ),
            },
        ),
        (
            "groundloop_m5_direct_terminal_projection",
            {
                "job_id": "'d30-f13-d24-decoy-projection-'||series.value::text",
                "terminal_identity_hash": _hash_expression("d24-projection"),
            },
        ),
        (
            "groundloop_m5_runtime_epoch",
            {
                "epoch_id": "30400000+series.value",
                "structural_event_id": (
                    "'d30-f13-d24-decoy-runtime-'||series.value::text"
                ),
                "runtime_state": "'sealed'",
                "terminal_at": "clock_timestamp()",
            },
        ),
        (
            "groundloop_m5_dispatch_record",
            {
                "attempt_id": "'d30-f13-d24-decoy-dispatch-'||series.value::text",
                "logical_job_id": (
                    "'d30-f13-d24-decoy-dispatch-job-'||series.value::text"
                ),
                "record_digest": _hash_expression("d24-dispatch"),
            },
        ),
        (
            "groundloop_m5_attempt_execution_evidence",
            {
                "attempt_id": "'d30-f13-d24-decoy-evidence-'||series.value::text",
                "result_or_error_hash": _hash_expression("d24-result"),
                "attempt_timing_digest": _hash_expression("d24-attempt-timing"),
                "evidence_digest": _hash_expression("d24-evidence"),
            },
        ),
        (
            "groundloop_m5_runtime_timing_contribution",
            {
                "attempt_id": "'d30-f13-d24-decoy-runtime-timing-'||series.value::text",
                "execution_evidence_digest": _hash_expression("d24-timing-evidence"),
                "observation_digest": _hash_expression("d24-timing-observation"),
                "attempt_timing_digest": _hash_expression("d24-timing-digest"),
            },
        ),
        (
            "groundloop_m5_typed_direct_late_return_envelope",
            {
                "attempt_id": "'d30-f13-d24-decoy-envelope-'||series.value::text",
                "job_id": "'d30-f13-d24-decoy-envelope-job-'||series.value::text",
                "result_artifact_id": (
                    "'d30-f13-d24-decoy-envelope-result-'||series.value::text"
                ),
                "envelope_digest": _hash_expression("d24-envelope"),
            },
        ),
        (
            "groundloop_m5_expired_attempt_return",
            {
                "attempt_id": "'d30-f13-d24-decoy-expired-'||series.value::text",
                "logical_job_id": (
                    "'d30-f13-d24-decoy-expired-job-'||series.value::text"
                ),
                "expired_return_digest": _hash_expression("d24-expired"),
            },
        ),
        (
            "groundloop_m5_post_terminal_attempt_timing",
            {
                "attempt_id": ("'d30-f13-d24-decoy-post-timing-'||series.value::text"),
                "attempt_timing_digest": _hash_expression("d24-post-timing"),
            },
        ),
        (
            "groundloop_m5_post_terminal_attempt_audit",
            {
                "attempt_id": ("'d30-f13-d24-decoy-post-audit-'||series.value::text"),
                "return_artifact_digest": _hash_expression("d24-post-artifact"),
            },
        ),
        (
            "groundloop_m5_runtime_work_contribution",
            {
                "source_id": "'d30-f13-d24-decoy-work-'||series.value::text",
                "source_identity_hash": _hash_expression("d24-work-identity"),
                "contribution_key_digest": _hash_expression("d24-work-key"),
            },
        ),
        (
            "groundloop_m5_transition_call_timing",
            {
                "source_id": (
                    "'d30-f13-d24-decoy-transition-timing-'||series.value::text"
                ),
                "contribution_key_digest": _hash_expression("d24-transition-key"),
                "observation_digest": _hash_expression("d24-transition-observation"),
                "transition_timing_digest": _hash_expression("d24-transition-digest"),
            },
        ),
        (
            "groundloop_m4_evaluation_counter_transition",
            {
                "epoch_id": "30500000+series.value",
                "transition_id": "'d30-f13-d24-decoy-transition-'||series.value::text",
                "payload_hash": _hash_expression("d24-transition-payload"),
                "from_revision": "1000+series.value",
                "to_revision": "1001+series.value",
            },
        ),
        (
            "groundloop_m5_runtime_work_accumulator",
            {"epoch_id": "30600000+series.value"},
        ),
        (
            "groundloop_m5_runtime_timing_accumulator",
            {"epoch_id": "30700000+series.value"},
        ),
        (
            "groundloop_m5_runtime_work",
            {
                "structural_event_id": (
                    "'d30-f13-d24-decoy-terminal-work-'||series.value::text"
                ),
                "epoch_id": "30800000+series.value",
            },
        ),
        (
            "groundloop_m5_event_result",
            {
                "structural_event_id": (
                    "'d30-f13-d24-decoy-result-event-'||series.value::text"
                ),
                "epoch_id": "30900000+series.value",
                "logical_result_hash": _hash_expression("d24-logical-result"),
            },
        ),
        (
            "groundloop_m5_event_timing_coverage",
            {
                "structural_event_id": (
                    "'d30-f13-d24-decoy-coverage-'||series.value::text"
                ),
                "epoch_id": "31000000+series.value",
            },
        ),
        (
            "groundloop_m5_event_result_delta",
            {
                "structural_event_id": (
                    "'d30-f13-d24-decoy-delta-'||series.value::text"
                ),
                "object_id": "'d30-f13-d24-decoy-object-'||series.value::text",
            },
        ),
        (
            "groundloop_m5_event_result_state_reference",
            {
                "structural_event_id": (
                    "'d30-f13-d24-decoy-reference-'||series.value::text"
                ),
                "object_id": "'d30-f13-d24-decoy-object-'||series.value::text",
                "reference_digest": _hash_expression("d24-reference"),
            },
        ),
    )
    for relation, overrides in specs:
        _inflate_for_default_planner(
            connection,
            relation,
            overrides=overrides,
            # The cloned epoch is independently valid.  Keeping its triggers
            # enabled also avoids ALTER TABLE colliding with the seed epoch's
            # still-pending deferred referential events.
            disable_triggers=relation != "groundloop_epoch",
            count=4096 if relation == "groundloop_m5_runtime_work" else 384,
        )
    _inflate_for_default_planner(
        connection,
        "groundloop_m4_evaluation_counter_transition",
        overrides={
            "epoch_id": str(epoch_id),
            "transition_id": (
                "'d30-f13-d24-same-epoch-transition-'||series.value::text"
            ),
            "payload_hash": _hash_expression("d24-same-epoch-transition"),
            "from_revision": "20000+2*series.value",
            "to_revision": "20001+2*series.value",
        },
    )


def _seed_f13_route_rows(d30_schema: Any) -> dict[str, object]:
    """Install one real, isolated row for every otherwise-empty F13 route."""

    connection = d30_schema.connection
    epoch_id = int(d30_schema.first_m4["epoch_id"])
    root_job_id = str(d30_schema.first_m4["root_job_id"])
    frontier_job_id = str(d30_schema.first_m4["frontier_job_id"])
    child_job_id = str(d30_schema.first_m4["child_job_id"])
    connection.execute("ALTER TABLE groundloop_epoch DISABLE TRIGGER USER")
    connection.execute(
        "UPDATE groundloop_epoch SET revision=3 WHERE epoch_id=%s", (epoch_id,)
    )
    connection.execute("ALTER TABLE groundloop_epoch ENABLE TRIGGER USER")
    claim = connection.execute(
        "SELECT claim_id FROM groundloop_claim "
        'ORDER BY octet_length(claim_id),claim_id COLLATE "C" LIMIT 1'
    ).fetchone()
    document = connection.execute(
        "SELECT document_version_id FROM groundloop_document_version "
        "ORDER BY octet_length(document_version_id),"
        'document_version_id COLLATE "C" LIMIT 1'
    ).fetchone()
    assert claim is not None and document is not None
    claim_id = str(claim[0])
    chunk_id = "d30-f13-route-chunk"
    chunk_text = "D30 F13 physical-route fixture"
    connection.execute(
        """INSERT INTO groundloop_chunk_version (
               chunk_version_id,document_version_id,chunk_index,text,text_hash,
               chunker_version,valid_from_epoch,valid_to_epoch
           ) SELECT %s,%s,COALESCE(max(chunk_index)+1,0),%s,%s,
                    'd30-f13-v1',%s,NULL
               FROM groundloop_chunk_version WHERE document_version_id=%s""",
        (
            chunk_id,
            str(document[0]),
            chunk_text,
            hashlib.sha256(chunk_text.encode()).hexdigest(),
            int(d30_schema.database.base.epoch_id),
            str(document[0]),
        ),
    )
    job = connection.execute(
        "SELECT candidate_policy_id,execution_spec_hash FROM "
        "groundloop_semantic_job WHERE job_id=%s",
        (child_job_id,),
    ).fetchone()
    assert job is not None
    policy_id, execution_spec_hash = str(job[0]), str(job[1]).strip()
    policy_row = connection.execute(
        """SELECT embedding_model_artifact_id,decision_policy_version,
                  claim_role_template_hash,chunk_role_template_hash,
                  vector_method_version,vector_index_kind,
                  vector_index_build_config_hash,vector_search_config_hash,
                  lexical_method_version,lexical_config_hash,
                  lexical_postgres_version,lexical_regconfig_identity,
                  claim_registry_snapshot_id,claim_count,fusion_version,
                  approximate_cap_per_inserted_chunk,frontier_depth
             FROM groundloop_candidate_policy WHERE candidate_policy_id=%s""",
        (policy_id,),
    ).fetchone()
    assert policy_row is not None
    direct_policy = CandidatePolicyManifest.build(
        policy_id=policy_id,
        embedding_model_artifact_id=str(policy_row[0]),
        decision_policy_version=str(policy_row[1]),
        claim_role_template_hash=str(policy_row[2]).strip(),
        chunk_role_template_hash=str(policy_row[3]).strip(),
        vector_method_version=str(policy_row[4]),
        vector_index_kind=VectorIndexKind(str(policy_row[5])),
        vector_index_build_config_hash=str(policy_row[6]).strip(),
        vector_search_config_hash=str(policy_row[7]).strip(),
        lexical_method_version=str(policy_row[8]),
        lexical_config_hash=str(policy_row[9]).strip(),
        lexical_postgres_version=str(policy_row[10]),
        lexical_regconfig_identity=str(policy_row[11]),
        claim_registry_snapshot_id=str(policy_row[12]),
        claim_count=int(policy_row[13]),
        fusion_version=str(policy_row[14]),
        approximate_cap_per_inserted_chunk=int(policy_row[15]),
        frontier_depth=int(policy_row[16]),
        verifier_execution_spec_hash=(
            d30_schema.database.manifest.verifier_execution_spec_hash
        ),
        lineage_safety_override=(d30_schema.database.manifest.lineage_safety_override),
    )
    execution_spec_hash = direct_policy.verifier_execution_spec_hash
    policy_manifest = {
        "policy_id": direct_policy.policy_id,
        "policy_hash": direct_policy.policy_hash,
        "embedding_model_artifact_id": direct_policy.embedding_model_artifact_id,
        "claim_role_template_hash": direct_policy.claim_role_template_hash,
        "chunk_role_template_hash": direct_policy.chunk_role_template_hash,
        "vector_method_version": direct_policy.vector_method_version,
        "vector_index_kind": direct_policy.vector_index_kind.value,
        "vector_index_build_config_hash": (
            direct_policy.vector_index_build_config_hash
        ),
        "vector_search_config_hash": direct_policy.vector_search_config_hash,
        "lexical_method_version": direct_policy.lexical_method_version,
        "lexical_config_hash": direct_policy.lexical_config_hash,
        "lexical_postgres_version": direct_policy.lexical_postgres_version,
        "lexical_regconfig_identity": direct_policy.lexical_regconfig_identity,
        "claim_registry_snapshot_id": direct_policy.claim_registry_snapshot_id,
        "claim_count": direct_policy.claim_count,
        "fusion_version": direct_policy.fusion_version,
        "approximate_cap_per_inserted_chunk": (
            direct_policy.approximate_cap_per_inserted_chunk
        ),
        "frontier_depth": direct_policy.frontier_depth,
        "verifier_execution_spec_hash": direct_policy.verifier_execution_spec_hash,
        "decision_policy_version": direct_policy.decision_policy_version,
        "lineage_safety_override": direct_policy.lineage_safety_override,
    }
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.execute("SET CONSTRAINTS ALL DEFERRED")
    connection.execute("ALTER TABLE groundloop_candidate_policy DISABLE TRIGGER ALL")
    connection.execute(
        """UPDATE groundloop_candidate_policy
              SET policy_hash=%s,manifest=%s::jsonb
            WHERE candidate_policy_id=%s""",
        (direct_policy.policy_hash, json.dumps(policy_manifest), policy_id),
    )
    connection.execute("ALTER TABLE groundloop_candidate_policy ENABLE TRIGGER ALL")
    epoch_identity = connection.execute(
        "SELECT event_id,payload_hash FROM groundloop_epoch WHERE epoch_id=%s",
        (epoch_id,),
    ).fetchone()
    assert epoch_identity is not None
    _owner_event_id, owner_payload_hash = (
        str(epoch_identity[0]),
        str(epoch_identity[1]).strip(),
    )
    connection.execute(
        "ALTER TABLE groundloop_m4_update "
        "DISABLE TRIGGER groundloop_m4_update_runtime_mode_guard"
    )
    try:
        connection.execute(
            """INSERT INTO groundloop_m4_update (
                   epoch_id,update_kind,candidate_policy_id,
                   previous_published_epoch_id,registry_snapshot_id,manifest
               ) VALUES (%s,'delete',%s,%s,%s,'{}'::jsonb)""",
            (
                epoch_id,
                policy_id,
                int(d30_schema.database.base.epoch_id),
                str(d30_schema.first_m4["registry_snapshot_id"]),
            ),
        )
    finally:
        connection.execute(
            "ALTER TABLE groundloop_m4_update "
            "ENABLE TRIGGER groundloop_m4_update_runtime_mode_guard"
        )
    previous = connection.execute(
        "SELECT previous_published_epoch_id FROM groundloop_m4_update "
        "WHERE epoch_id=%s",
        (epoch_id,),
    ).fetchone()
    assert previous is not None and previous[0] is not None
    predecessor_epoch_id = int(previous[0])

    dynamic_observation = "d30-f13-dynamic-observation"
    dynamic_task = "d30-f13-dynamic-task"
    dynamic_raw_hash = hashlib.sha256(dynamic_observation.encode()).hexdigest()
    connection.execute(
        """INSERT INTO groundloop_semantic_observation (
               observation_id,subject_kind,subject_id,chunk_version_id,
               task_type,support_score,refute_score,neutral_score,
               model_id,model_version,prompt_version,input_hash,
               produced_epoch,raw_output_hash,eligible_for_currency
           ) VALUES (%s,'claim',%s,%s,%s,0.8,0.1,0.1,
                     'd30-f13-verifier','v1','v1',%s,%s,%s,true)""",
        (
            dynamic_observation,
            claim_id,
            chunk_id,
            dynamic_task,
            hashlib.sha256(f"input:{dynamic_observation}".encode()).hexdigest(),
            epoch_id,
            dynamic_raw_hash,
        ),
    )
    dynamic_predecessor_observation = "d30-f13-dynamic-predecessor-observation"
    dynamic_predecessor_raw_hash = hashlib.sha256(
        dynamic_predecessor_observation.encode()
    ).hexdigest()
    connection.execute(
        """INSERT INTO groundloop_semantic_observation (
               observation_id,subject_kind,subject_id,chunk_version_id,
               task_type,support_score,refute_score,neutral_score,
               model_id,model_version,prompt_version,input_hash,
               produced_epoch,raw_output_hash,eligible_for_currency
           ) VALUES (%s,'claim',%s,%s,%s,0.7,0.2,0.1,
                     'd30-f13-verifier','v1','v1',%s,%s,%s,true)""",
        (
            dynamic_predecessor_observation,
            claim_id,
            chunk_id,
            dynamic_task,
            hashlib.sha256(
                f"input:{dynamic_predecessor_observation}".encode()
            ).hexdigest(),
            int(d30_schema.database.base.epoch_id),
            dynamic_predecessor_raw_hash,
        ),
    )
    # Install a long, non-overlapping history before the positive owner epoch.
    # These rollback-scoped negative identity coordinates exist solely to make
    # the production <= predecessor probe choose its required backward PK
    # route under the default planner; no public/runtime writer accepts them.
    connection.execute(
        """INSERT INTO groundloop_epoch (
               epoch_id,event_id,payload_hash,revision,structural_status,
               semantic_status,evaluation_state,publication_mode,sealed_at)
           OVERRIDING SYSTEM VALUE
           SELECT -series.value,
                  'd30-f13-dynamic-predecessor-epoch-'||series.value::text,
                  md5('d30-f13-dynamic-predecessor:'||series.value::text)
                    ||md5('d30-f13-dynamic-predecessor-tail:'||series.value::text),
                  0,'committed','sealed','complete','strict',now()
             FROM generate_series(1,130) AS series(value)"""
    )
    connection.execute(
        """INSERT INTO groundloop_published_observation_currency (
               subject_kind,subject_id,chunk_version_id,task_type,
               observation_id,valid_from_epoch,valid_to_epoch)
           SELECT 'claim',%s,%s,%s,%s,point.epoch_id,
                  CASE WHEN point.epoch_id=-1 THEN %s ELSE point.epoch_id+1 END
             FROM generate_series(-130,-1) AS point(epoch_id)""",
        (
            claim_id,
            chunk_id,
            dynamic_task,
            dynamic_predecessor_observation,
            epoch_id,
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_working_observation_delta (
               epoch_id,subject_kind,subject_id,chunk_version_id,task_type,
               base_observation_id,working_observation_id,installed_revision
           ) VALUES (%s,'claim',%s,%s,%s,%s,%s,1)""",
        (
            epoch_id,
            claim_id,
            chunk_id,
            dynamic_task,
            dynamic_predecessor_observation,
            dynamic_observation,
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_observation_currency (
               subject_kind,subject_id,chunk_version_id,task_type,
               observation_id,installed_revision
           ) VALUES ('claim',%s,%s,%s,%s,%s)""",
        (claim_id, chunk_id, dynamic_task, dynamic_observation, epoch_id),
    )
    current_point_chunk = "d30-f13-current-point-chunk"
    current_point_text = "D30 F13 exact current-currency point fixture"
    connection.execute(
        """INSERT INTO groundloop_chunk_version (
               chunk_version_id,document_version_id,chunk_index,text,text_hash,
               chunker_version,valid_from_epoch,valid_to_epoch
           ) SELECT %s,%s,COALESCE(max(chunk_index)+1,0),%s,%s,
                    'd30-f13-v1',%s,NULL
               FROM groundloop_chunk_version WHERE document_version_id=%s""",
        (
            current_point_chunk,
            str(document[0]),
            current_point_text,
            hashlib.sha256(current_point_text.encode()).hexdigest(),
            int(d30_schema.database.base.epoch_id),
            str(document[0]),
        ),
    )
    current_point_observation = "d30-f13-current-point-observation"
    current_point_task = "d30-f13-current-point-task"
    connection.execute(
        """INSERT INTO groundloop_semantic_observation (
               observation_id,subject_kind,subject_id,chunk_version_id,
               task_type,support_score,refute_score,neutral_score,
               model_id,model_version,prompt_version,input_hash,
               produced_epoch,raw_output_hash,eligible_for_currency
           ) VALUES (%s,'claim',%s,%s,%s,0.8,0.1,0.1,
                     'd30-f13-model','v1','d30-f13-prompt',%s,%s,%s,true)""",
        (
            current_point_observation,
            claim_id,
            current_point_chunk,
            current_point_task,
            hashlib.sha256(f"input:{current_point_observation}".encode()).hexdigest(),
            epoch_id,
            hashlib.sha256(current_point_observation.encode()).hexdigest(),
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_observation_currency (
               subject_kind,subject_id,chunk_version_id,task_type,
               observation_id,installed_revision
           ) VALUES ('claim',%s,%s,%s,%s,%s)""",
        (
            claim_id,
            current_point_chunk,
            current_point_task,
            current_point_observation,
            epoch_id,
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_published_observation_currency (
               subject_kind,subject_id,chunk_version_id,task_type,
               observation_id,valid_from_epoch,valid_to_epoch
           ) VALUES ('claim',%s,%s,%s,%s,%s,NULL)""",
        (
            claim_id,
            chunk_id,
            dynamic_task,
            dynamic_observation,
            epoch_id,
        ),
    )
    history_observation = "d30-f13-predecessor-observation"
    history_task = "d30-f13-predecessor-task"
    connection.execute(
        """INSERT INTO groundloop_semantic_observation (
               observation_id,subject_kind,subject_id,chunk_version_id,
               task_type,support_score,refute_score,neutral_score,
               model_id,model_version,prompt_version,input_hash,
               produced_epoch,raw_output_hash,eligible_for_currency
           ) VALUES (%s,'claim',%s,%s,%s,0.8,0.1,0.1,
                     'd30-f13-model','v1','d30-f13-prompt',%s,%s,%s,true)""",
        (
            history_observation,
            claim_id,
            chunk_id,
            history_task,
            hashlib.sha256(f"input:{history_observation}".encode()).hexdigest(),
            epoch_id,
            hashlib.sha256(history_observation.encode()).hexdigest(),
        ),
    )
    history_epochs: list[int] = []
    for ordinal in range(130):
        event_id = f"d30-f13-predecessor-epoch-{ordinal:02d}"
        inserted_epoch = connection.execute(
            """INSERT INTO groundloop_epoch (
                   event_id,payload_hash,revision,structural_status,
                   semantic_status,evaluation_state,publication_mode,sealed_at
               ) VALUES (%s,%s,0,'committed','sealed','complete','strict',now())
               RETURNING epoch_id""",
            (event_id, hashlib.sha256(event_id.encode()).hexdigest()),
        ).fetchone()
        assert inserted_epoch is not None
        history_epochs.append(int(inserted_epoch[0]))
    with connection.cursor() as history_cursor:
        history_cursor.executemany(
            """INSERT INTO groundloop_published_observation_currency (
                   subject_kind,subject_id,chunk_version_id,task_type,
                   observation_id,valid_from_epoch,valid_to_epoch
               ) VALUES ('claim',%s,%s,%s,%s,%s,%s)""",
            (
                (
                    claim_id,
                    chunk_id,
                    history_task,
                    history_observation,
                    history_epochs[ordinal],
                    history_epochs[ordinal + 1],
                )
                for ordinal in range(len(history_epochs) - 1)
            ),
        )
    open_observation = "d30-f13-open-published-observation"
    open_task = "d30-f13-open-published-task"
    connection.execute(
        """INSERT INTO groundloop_semantic_observation (
               observation_id,subject_kind,subject_id,chunk_version_id,
               task_type,support_score,refute_score,neutral_score,
               model_id,model_version,prompt_version,input_hash,
               produced_epoch,raw_output_hash,eligible_for_currency
           ) VALUES (%s,'claim',%s,%s,%s,0.8,0.1,0.1,
                     'd30-f13-model','v1','d30-f13-prompt',%s,%s,%s,true)""",
        (
            open_observation,
            claim_id,
            chunk_id,
            open_task,
            hashlib.sha256(f"input:{open_observation}".encode()).hexdigest(),
            epoch_id,
            hashlib.sha256(open_observation.encode()).hexdigest(),
        ),
    )
    with connection.cursor() as open_history_cursor:
        open_history_cursor.executemany(
            """INSERT INTO groundloop_published_observation_currency (
                   subject_kind,subject_id,chunk_version_id,task_type,
                   observation_id,valid_from_epoch,valid_to_epoch
               ) VALUES ('claim',%s,%s,%s,%s,%s,%s)""",
            (
                (
                    claim_id,
                    chunk_id,
                    open_task,
                    open_observation,
                    history_epochs[ordinal],
                    history_epochs[ordinal + 1],
                )
                for ordinal in range(len(history_epochs) - 1)
            ),
        )
    connection.execute(
        """INSERT INTO groundloop_published_observation_currency (
               subject_kind,subject_id,chunk_version_id,task_type,
               observation_id,valid_from_epoch,valid_to_epoch
           ) VALUES ('claim',%s,%s,%s,%s,%s,NULL)""",
        (claim_id, chunk_id, open_task, open_observation, history_epochs[-1]),
    )

    # Give the real D30 activation validator a closed image whose predecessor
    # is later than this historical legacy owner while its activation base is
    # exactly the owner.  All three singleton changes roll back with the test.
    for relation in (
        "groundloop_m5_activation",
        "groundloop_m4_publication_head",
        "groundloop_m5_publication_head",
    ):
        connection.execute(f"ALTER TABLE {relation} DISABLE TRIGGER ALL")
    connection.execute(
        "UPDATE groundloop_m5_activation SET base_m4_epoch_id=%s WHERE singleton",
        (epoch_id,),
    )
    connection.execute(
        "UPDATE groundloop_m4_publication_head SET epoch_id=%s WHERE singleton",
        (history_epochs[-1],),
    )
    connection.execute(
        """UPDATE groundloop_m5_publication_head
              SET epoch_id=%s,sealed_revision=0 WHERE singleton""",
        (history_epochs[-1],),
    )
    for relation in (
        "groundloop_m5_activation",
        "groundloop_m4_publication_head",
        "groundloop_m5_publication_head",
    ):
        connection.execute(f"ALTER TABLE {relation} ENABLE TRIGGER ALL")

    # Turn the otherwise width-only M4 graph into one coherent, sealed legacy
    # owner.  The real D30 gather/lock/rerun helpers below can therefore execute
    # their complete production call graph rather than a handwritten SQL model.
    original_root_job_id = root_job_id
    original_child_job_id = child_job_id
    original_frontier_job_id = frontier_job_id
    job_rows = {
        str(row[0]): (str(row[1]), str(row[2]).strip())
        for row in connection.execute(
            """SELECT job_id,job_kind,execution_spec_hash
                 FROM groundloop_semantic_job WHERE epoch_id=%s""",
            (epoch_id,),
        ).fetchall()
    }
    assert set(job_rows) == {root_job_id, child_job_id, frontier_job_id}
    root_execution_spec_hash = job_rows[original_root_job_id][1]
    frontier_execution_spec_hash = job_rows[original_frontier_job_id][1]
    root_job_id = stable_m4_digest(
        "m4-logical-job-v1",
        _owner_event_id,
        "impact_discovery",
        policy_id,
        root_execution_spec_hash,
        "",
        "",
        chunk_id,
    )
    child_job_id = stable_m4_digest(
        "m4-logical-job-v1",
        _owner_event_id,
        "verify_pair",
        policy_id,
        execution_spec_hash,
        root_job_id,
        claim_id,
        chunk_id,
    )
    frontier_job_id = stable_m4_digest(
        "m4-logical-job-v1",
        _owner_event_id,
        "frontier_retrieve",
        policy_id,
        frontier_execution_spec_hash,
        "",
        claim_id,
        "",
    )
    for relation in (
        "groundloop_semantic_job",
        "groundloop_semantic_job_dependency",
        "groundloop_discovery_scope",
    ):
        connection.execute(f"ALTER TABLE {relation} DISABLE TRIGGER ALL")
    connection.execute(
        """UPDATE groundloop_semantic_job
              SET job_id=CASE job_id
                    WHEN %s THEN %s WHEN %s THEN %s WHEN %s THEN %s END,
                  parent_job_id=CASE
                    WHEN parent_job_id=%s THEN %s ELSE parent_job_id END
            WHERE epoch_id=%s""",
        (
            original_root_job_id,
            root_job_id,
            original_child_job_id,
            child_job_id,
            original_frontier_job_id,
            frontier_job_id,
            original_root_job_id,
            root_job_id,
            epoch_id,
        ),
    )
    connection.execute(
        """UPDATE groundloop_semantic_job_dependency
              SET parent_job_id=%s,child_job_id=%s
            WHERE epoch_id=%s AND parent_job_id=%s AND child_job_id=%s""",
        (
            root_job_id,
            child_job_id,
            epoch_id,
            original_root_job_id,
            original_child_job_id,
        ),
    )
    connection.execute(
        "UPDATE groundloop_discovery_scope SET root_job_id=%s WHERE root_job_id=%s",
        (root_job_id, original_root_job_id),
    )
    for relation in (
        "groundloop_semantic_job",
        "groundloop_semantic_job_dependency",
        "groundloop_discovery_scope",
    ):
        connection.execute(f"ALTER TABLE {relation} ENABLE TRIGGER ALL")
    job_rows = {
        root_job_id: (job_rows[original_root_job_id][0], root_execution_spec_hash),
        child_job_id: (job_rows[original_child_job_id][0], execution_spec_hash),
        frontier_job_id: (
            job_rows[original_frontier_job_id][0],
            frontier_execution_spec_hash,
        ),
    }
    root_result_id = "d30-f13-impact-result"
    root_result_hash = hashlib.sha256(root_result_id.encode()).hexdigest()
    frontier_result_id = "d30-f13-frontier-result"
    frontier_result_hash = hashlib.sha256(frontier_result_id.encode()).hexdigest()
    child_result_id = "d30-f13-verifier-result"
    child_result_hash = dynamic_raw_hash

    def complete_job(
        *,
        job_id: str,
        kind: str,
        parent_id: str | None,
        job_claim_id: str | None,
        job_chunk_id: str | None,
        result_id: str,
        result_hash: str,
        completed_revision: int,
        child_ids: tuple[str, ...],
    ) -> None:
        execution_hash = job_rows[job_id][1]
        payload_hash = stable_m4_digest(
            "m4-application-job-payload-v1",
            owner_payload_hash,
            kind,
            parent_id or "",
            job_claim_id or "",
            job_chunk_id or "",
        )
        closure = (
            ChildClosure.build(
                parent_job_id=job_id,
                result_artifact_hash=result_hash,
                child_job_ids=child_ids,
            )
            if parent_id is None
            else None
        )
        completion = JobCompletion.build(
            job_id=job_id,
            payload_hash=payload_hash,
            execution_spec_hash=execution_hash,
            result_artifact_id=result_id,
            result_artifact_hash=result_hash,
            terminal_state=JobState.COMPLETED_ACTIVE,
            child_closure=closure,
        )
        connection.execute(
            """UPDATE groundloop_semantic_job
                  SET parent_job_id=%s,payload_hash=%s,execution_spec_hash=%s,
                      claim_id=%s,
                      chunk_version_id=%s,job_state='completed_active',
                      child_closed=%s,child_set_hash=%s,completion_digest=%s,
                      result_artifact_id=%s,result_artifact_hash=%s,
                      completed_revision=%s,completed_at=now()
                WHERE job_id=%s""",
            (
                parent_id,
                payload_hash,
                execution_hash,
                job_claim_id,
                job_chunk_id,
                closure is not None,
                None if closure is None else closure.child_set_hash,
                completion.completion_digest,
                result_id,
                result_hash,
                completed_revision,
                job_id,
            ),
        )

    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.execute("SET CONSTRAINTS ALL DEFERRED")
    for relation in (
        "groundloop_m4_update",
        "groundloop_semantic_job",
        "groundloop_semantic_job_attempt",
        "groundloop_discovery_scope",
    ):
        connection.execute(f"ALTER TABLE {relation} DISABLE TRIGGER ALL")
    complete_job(
        job_id=root_job_id,
        kind="impact_discovery",
        parent_id=None,
        job_claim_id=None,
        job_chunk_id=chunk_id,
        result_id=root_result_id,
        result_hash=root_result_hash,
        completed_revision=2,
        child_ids=(child_job_id,),
    )
    complete_job(
        job_id=child_job_id,
        kind="verify_pair",
        parent_id=root_job_id,
        job_claim_id=claim_id,
        job_chunk_id=chunk_id,
        result_id=child_result_id,
        result_hash=child_result_hash,
        completed_revision=1,
        child_ids=(),
    )
    complete_job(
        job_id=frontier_job_id,
        kind="frontier_retrieve",
        parent_id=None,
        job_claim_id=claim_id,
        job_chunk_id=None,
        result_id=frontier_result_id,
        result_hash=frontier_result_hash,
        completed_revision=3,
        child_ids=(),
    )
    connection.execute(
        """UPDATE groundloop_discovery_scope
              SET closed_revision=2 WHERE root_job_id=%s""",
        (root_job_id,),
    )
    connection.execute(
        """UPDATE groundloop_m4_update
              SET manifest=jsonb_build_object(
                    '_groundloop_m4_runtime_v1',jsonb_build_object(
                        'event_manifest','{}'::jsonb,
                        'scope_claim_ids',jsonb_build_object(
                            %s::text,jsonb_build_array(%s::text)),
                        'failure_reason',NULL))
            WHERE epoch_id=%s""",
        (root_job_id, claim_id, epoch_id),
    )
    attempts: dict[str, str] = {}
    for job_id in (root_job_id, child_job_id, frontier_job_id):
        attempt_id = stable_m4_digest("m4-job-attempt-v1", job_id, "1")
        attempts[job_id] = attempt_id
        connection.execute(
            """INSERT INTO groundloop_semantic_job_attempt (
                   attempt_id,job_id,execution_spec_hash,attempt_ordinal,
                   lease_token_hash,attempt_state,lease_expires_at,
                   started_at,finished_at)
               VALUES (%s,%s,%s,1,%s,'completed',now()+interval '1 hour',
                       now(),now())""",
            (
                attempt_id,
                job_id,
                job_rows[job_id][1],
                stable_m4_digest("m4-lease-token-v1", job_id, "1"),
            ),
        )
    for relation in (
        "groundloop_m4_update",
        "groundloop_semantic_job",
        "groundloop_semantic_job_attempt",
        "groundloop_discovery_scope",
    ):
        connection.execute(f"ALTER TABLE {relation} ENABLE TRIGGER ALL")

    for relation in (
        "groundloop_m4_discovery_result",
        "groundloop_admitted_pair",
        "groundloop_m5_direct_terminal_projection",
        "groundloop_m4_verification_execution",
    ):
        connection.execute(f"ALTER TABLE {relation} DISABLE TRIGGER ALL")
    admitted_pair_id = stable_m4_digest(
        "m4-admitted-pair-v1", str(epoch_id), claim_id, chunk_id, policy_id
    )
    for (
        result_root_id,
        result_id,
        result_hash,
        admitted_count,
    ) in (
        (root_job_id, root_result_id, root_result_hash, 1),
        (frontier_job_id, frontier_result_id, frontier_result_hash, 1),
    ):
        connection.execute(
            """INSERT INTO groundloop_m4_discovery_result (
                   root_job_id,epoch_id,result_artifact_id,result_artifact_hash,
                   fallback_satisfied,channel_hit_count,admitted_pair_count,
                   channel_set_hash,admitted_pair_set_hash
               ) VALUES (%s,%s,%s,%s,true,0,%s,%s,%s)""",
            (
                result_root_id,
                epoch_id,
                result_id,
                result_hash,
                admitted_count,
                hashlib.sha256(f"channel:{result_root_id}".encode()).hexdigest(),
                hashlib.sha256(f"admitted:{result_root_id}".encode()).hexdigest(),
            ),
        )
    connection.execute(
        """INSERT INTO groundloop_admitted_pair (
               admitted_pair_id,epoch_id,chunk_version_id,claim_id,
               candidate_policy_id,fused_rank,reasons,mandatory_lineage
           ) VALUES (%s,%s,%s,%s,%s,1,ARRAY['vector'],false)""",
        (admitted_pair_id, epoch_id, chunk_id, claim_id, policy_id),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_direct_terminal_projection (
               epoch_id,job_id,terminal_state,terminal_reason,
               m4_completion_digest,completed_revision,terminal_identity_hash)
           VALUES (%s,'d30-f13-projection-seed','completed_active',NULL,%s,1,%s)""",
        (
            history_epochs[-1],
            hashlib.sha256(b"d30-f13-projection-completion").hexdigest(),
            hashlib.sha256(b"d30-f13-projection-identity").hexdigest(),
        ),
    )

    model_id = "d30-f13-verification-model"
    prompt_id = "d30-f13-verification-prompt"
    connection.execute(
        """INSERT INTO groundloop_model_artifact (
               model_artifact_id,task,provider,model_id,immutable_revision,
               tokenizer_revision,license_id,config_hash,artifact_sha256
           ) VALUES (%s,'verification','fixture','d30-f13-verifier','v1',
                     'v1','MIT',%s,%s)""",
        (model_id, "1" * 64, "2" * 64),
    )
    connection.execute(
        """INSERT INTO groundloop_prompt_artifact (
               prompt_artifact_id,task,version,template,template_hash,
               decoding_config_hash
           ) VALUES (%s,'verification','v1','d30 f13 prompt',%s,%s)""",
        (prompt_id, "3" * 64, "4" * 64),
    )
    connection.execute(
        """INSERT INTO groundloop_m4_verification_execution (
               observation_id,job_id,admitted_pair_id,model_artifact_id,
               prompt_artifact_id,execution_spec_hash,pair_input_hash,
               calibration_version,calibration_artifact_sha256,temperature,
               raw_logits,raw_output_hash,reused_from_observation_id
           ) VALUES (%s,%s,%s,%s,%s,%s,%s,'d30-f13-calibration',%s,1.0,
                     ARRAY[1.0,0.0,-1.0],%s,NULL)""",
        (
            dynamic_observation,
            child_job_id,
            admitted_pair_id,
            model_id,
            prompt_id,
            execution_spec_hash,
            "5" * 64,
            "6" * 64,
            dynamic_raw_hash,
        ),
    )

    activation_image = connection.execute(
        """SELECT activation.base_m4_epoch_id,m4_head.epoch_id,m5_head.epoch_id
             FROM groundloop_m5_activation AS activation
             JOIN groundloop_m4_publication_head AS m4_head ON m4_head.singleton
             JOIN groundloop_m5_publication_head AS m5_head ON m5_head.singleton
            WHERE activation.singleton"""
    ).fetchone()
    assert activation_image is not None
    assert int(activation_image[1]) == int(activation_image[2])
    image_epoch_ids = tuple(
        dict.fromkeys((int(activation_image[0]), int(activation_image[1])))
    )
    bootstrap_claim_id, bootstrap_chunk_id = claim_id, chunk_id
    connection.execute(
        "ALTER TABLE groundloop_m4_update "
        "DISABLE TRIGGER groundloop_m4_update_runtime_mode_guard"
    )
    try:
        for image_ordinal, image_epoch_id in enumerate(image_epoch_ids):
            image_update = connection.execute(
                "SELECT registry_snapshot_id FROM groundloop_m4_update "
                "WHERE epoch_id=%s",
                (image_epoch_id,),
            ).fetchone()
            if image_update is None:
                registry_id = f"d30-f13-image-registry-{image_ordinal}"
                connection.execute(
                    """INSERT INTO groundloop_m4_update (
                           epoch_id,update_kind,candidate_policy_id,
                           previous_published_epoch_id,registry_snapshot_id,manifest
                       ) VALUES (%s,'insert',%s,NULL,%s,'{}'::jsonb)""",
                    (image_epoch_id, policy_id, registry_id),
                )
            else:
                registry_id = str(image_update[0])
            connection.execute(
                """INSERT INTO groundloop_m4_claim_registry_snapshot (
                       claim_registry_snapshot_id,claim_count,claim_set_hash
                   ) VALUES (%s,1,%s) ON CONFLICT DO NOTHING""",
                (registry_id, hashlib.sha256(registry_id.encode()).hexdigest()),
            )
            connection.execute(
                """INSERT INTO groundloop_m4_claim_registry_member (
                       claim_registry_snapshot_id,claim_id,member_ordinal
                   ) VALUES (
                       %s,%s,
                       COALESCE((SELECT max(member_ordinal)+1
                                   FROM groundloop_m4_claim_registry_member
                                  WHERE claim_registry_snapshot_id=%s),0)
                   ) ON CONFLICT DO NOTHING""",
                (registry_id, bootstrap_claim_id, registry_id),
            )
    finally:
        connection.execute(
            "ALTER TABLE groundloop_m4_update "
            "ENABLE TRIGGER groundloop_m4_update_runtime_mode_guard"
        )
    image_epoch_id = image_epoch_ids[0]
    bootstrap_observation = "d30-f13-bootstrap-observation"
    bootstrap_task = "d30-f13-bootstrap-task"
    bootstrap_raw_hash = hashlib.sha256(bootstrap_observation.encode()).hexdigest()
    connection.execute(
        """INSERT INTO groundloop_semantic_observation (
               observation_id,subject_kind,subject_id,chunk_version_id,
               task_type,support_score,refute_score,neutral_score,
               model_id,model_version,prompt_version,input_hash,
               produced_epoch,raw_output_hash,eligible_for_currency
           ) VALUES (%s,'claim',%s,%s,%s,0.8,0.1,0.1,
                     'd30-f13-model','v1','d30-f13-prompt',%s,%s,%s,true)""",
        (
            bootstrap_observation,
            bootstrap_claim_id,
            bootstrap_chunk_id,
            bootstrap_task,
            hashlib.sha256(f"input:{bootstrap_observation}".encode()).hexdigest(),
            image_epoch_id,
            bootstrap_raw_hash,
        ),
    )
    run_id = "d30-f13-run"
    candidate_id = "d30-f13-candidate"
    embedding_model_id = connection.execute(
        "SELECT embedding_model_artifact_id FROM groundloop_candidate_policy "
        "WHERE candidate_policy_id=%s",
        (policy_id,),
    ).fetchone()
    assert embedding_model_id is not None
    embedding_model = str(embedding_model_id[0])
    base = d30_schema.database.base
    connection.execute(
        """INSERT INTO groundloop_pipeline_run (
               run_id,schema_version,status,config_hash,input_hash,corpus_hash,
               question_id,answer_version_id,semantic_epoch_id,manifest,
               completed_at
           ) VALUES (%s,'m3-v1','published',%s,%s,%s,%s,%s,%s,'{}'::jsonb,now())""",
        (
            run_id,
            "7" * 64,
            "8" * 64,
            "9" * 64,
            str(base.question_id),
            str(base.answer_id),
            image_epoch_id,
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_retrieval_candidate (
               candidate_id,run_id,query_kind,query_id,claim_id,
               chunk_version_id,embedding_model_artifact_id,method_version,
               score,rank
           ) VALUES (%s,%s,'claim',%s,%s,%s,%s,'d30-f13-retrieval',0.9,1)""",
        (
            candidate_id,
            run_id,
            bootstrap_claim_id,
            bootstrap_claim_id,
            bootstrap_chunk_id,
            embedding_model,
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_verification_execution (
               observation_id,run_id,candidate_id,model_artifact_id,
               prompt_artifact_id,calibration_version,temperature,
               raw_logits,raw_output_hash,reused_from_observation_id
           ) VALUES (%s,%s,%s,%s,%s,'d30-f13-calibration',1.0,
                     ARRAY[1.0,0.0,-1.0],%s,NULL)""",
        (
            bootstrap_observation,
            run_id,
            candidate_id,
            model_id,
            prompt_id,
            bootstrap_raw_hash,
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_chunk_embedding (
               chunk_version_id,model_artifact_id,embedding,input_hash
           ) VALUES (%s,%s,array_fill(0.0::real,ARRAY[384])::vector,%s)""",
        (bootstrap_chunk_id, embedding_model, "a" * 64),
    )
    embedding_use_id = f"embedding:{bootstrap_chunk_id}:{embedding_model}"
    artifact_uses = (
        ("embedding", embedding_use_id),
        ("model", embedding_model),
        ("model", model_id),
        ("prompt", prompt_id),
        ("retrieval", candidate_id),
        ("verification", bootstrap_observation),
    )
    with connection.cursor() as artifact_cursor:
        artifact_cursor.executemany(
            """INSERT INTO groundloop_pipeline_artifact_use (
                   run_id,artifact_kind,artifact_id,reused
               ) VALUES (%s,%s,%s,false)""",
            ((run_id, kind, artifact_id) for kind, artifact_id in artifact_uses),
        )
    return {
        "epoch_id": epoch_id,
        "predecessor_epoch_id": predecessor_epoch_id,
        "authority_predecessor_epoch_id": history_epochs[-1],
        "root_job_id": root_job_id,
        "frontier_job_id": frontier_job_id,
        "child_job_id": child_job_id,
        "claim_id": claim_id,
        "chunk_id": chunk_id,
        "dynamic_observation": dynamic_observation,
        "dynamic_task": dynamic_task,
        "current_point_chunk": current_point_chunk,
        "current_point_observation": current_point_observation,
        "current_point_task": current_point_task,
        "history_observation": history_observation,
        "history_task": history_task,
        "open_observation": open_observation,
        "open_task": open_task,
        "history_predecessor_epoch_id": history_epochs[-2],
        "history_valid_from_epoch": history_epochs[-2],
        "history_valid_to_epoch": history_epochs[-1],
        "attempt_id": attempts[child_job_id],
        "attempts": attempts,
        "result_artifact_id": root_result_id,
        "admitted_pair_id": admitted_pair_id,
        "candidate_policy_id": policy_id,
        "verifier_execution_spec_hash": execution_spec_hash,
        "model_id": model_id,
        "prompt_id": prompt_id,
        "image_epoch_id": image_epoch_id,
        "image_epoch_ids": image_epoch_ids,
        "bootstrap_claim_id": bootstrap_claim_id,
        "bootstrap_chunk_id": bootstrap_chunk_id,
        "bootstrap_observation": bootstrap_observation,
        "bootstrap_task": bootstrap_task,
        "run_id": run_id,
        "candidate_id": candidate_id,
        "embedding_model_id": embedding_model,
        "embedding_use_id": embedding_use_id,
        "artifact_uses": artifact_uses,
    }


def _inflate_f13_relations(connection: Any) -> None:
    """Populate independent coordinates so the default planner is meaningful."""

    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.execute("SET CONSTRAINTS ALL DEFERRED")
    clone_specs: tuple[tuple[str, dict[str, str]], ...] = (
        (
            "groundloop_epoch",
            {
                "epoch_id": "9000000+series.value",
                "event_id": "'d30-f13-decoy-epoch-'||series.value::text",
            },
        ),
        (
            "groundloop_m4_update",
            {
                "epoch_id": "9100000+series.value",
                "registry_snapshot_id": (
                    "'d30-f13-decoy-update-registry-'||series.value::text"
                ),
            },
        ),
        (
            "groundloop_semantic_observation",
            {
                "observation_id": ("'d30-f13-decoy-observation-'||series.value::text"),
            },
        ),
        (
            "groundloop_working_observation_delta",
            {"task_type": "'d30-f13-decoy-delta-'||series.value::text"},
        ),
        (
            "groundloop_observation_currency",
            {
                "task_type": "'d30-f13-decoy-current-'||series.value::text",
                "chunk_version_id": (
                    "'d30-f13-decoy-current-chunk-'||series.value::text"
                ),
                "observation_id": (
                    "'d30-f13-decoy-current-observation-'||series.value::text"
                ),
            },
        ),
        (
            "groundloop_semantic_job",
            {
                "job_id": "'d30-f13-decoy-job-'||series.value::text",
                "epoch_id": "9200000+series.value",
            },
        ),
        (
            "groundloop_semantic_job_dependency",
            {
                "epoch_id": "9300000+series.value",
                "parent_job_id": ("'d30-f13-decoy-parent-'||series.value::text"),
                "child_job_id": "'d30-f13-decoy-child-'||series.value::text",
            },
        ),
        (
            "groundloop_discovery_scope",
            {"root_job_id": "'d30-f13-decoy-scope-'||series.value::text"},
        ),
        (
            "groundloop_semantic_job_attempt",
            {
                "attempt_id": "'d30-f13-decoy-attempt-'||series.value::text",
                "job_id": "'d30-f13-decoy-attempt-job-'||series.value::text",
                "attempt_ordinal": "1000+series.value",
            },
        ),
        (
            "groundloop_m4_discovery_result",
            {
                "root_job_id": "'d30-f13-decoy-result-'||series.value::text",
                "result_artifact_id": (
                    "'d30-f13-decoy-result-artifact-'||series.value::text"
                ),
            },
        ),
        (
            "groundloop_m5_direct_terminal_projection",
            {
                "job_id": "'d30-f13-decoy-projection-'||series.value::text",
                "terminal_identity_hash": _hash_expression("projection"),
            },
        ),
        (
            "groundloop_m5_runtime_epoch",
            {
                "epoch_id": "9400000+series.value",
                "structural_event_id": ("'d30-f13-decoy-runtime-'||series.value::text"),
                "runtime_state": "'sealed'",
                "terminal_at": "clock_timestamp()",
            },
        ),
        (
            "groundloop_admitted_pair",
            {
                "admitted_pair_id": ("'d30-f13-decoy-admission-'||series.value::text"),
                "chunk_version_id": "'d30-f13-decoy-chunk-'||series.value::text",
            },
        ),
        (
            "groundloop_m4_verification_execution",
            {
                "observation_id": (
                    "'d30-f13-decoy-execution-observation-'||series.value::text"
                ),
                "job_id": "'d30-f13-decoy-execution-job-'||series.value::text",
                "admitted_pair_id": (
                    "'d30-f13-decoy-execution-pair-'||series.value::text"
                ),
            },
        ),
        (
            "groundloop_model_artifact",
            {
                "model_artifact_id": "'d30-f13-decoy-model-'||series.value::text",
                "model_id": "'d30-f13-decoy-model-id-'||series.value::text",
            },
        ),
        (
            "groundloop_prompt_artifact",
            {
                "prompt_artifact_id": ("'d30-f13-decoy-prompt-'||series.value::text"),
                "version": "'d30-f13-decoy-version-'||series.value::text",
            },
        ),
        (
            "groundloop_pipeline_run",
            {"run_id": "'d30-f13-decoy-run-'||series.value::text"},
        ),
        (
            "groundloop_retrieval_candidate",
            {
                "candidate_id": "'d30-f13-decoy-candidate-'||series.value::text",
                "run_id": "'d30-f13-decoy-candidate-run-'||series.value::text",
            },
        ),
        (
            "groundloop_verification_execution",
            {
                "observation_id": ("'d30-f13-decoy-m3-execution-'||series.value::text"),
            },
        ),
        (
            "groundloop_chunk_embedding",
            {
                "chunk_version_id": (
                    "'d30-f13-decoy-embedding-chunk-'||series.value::text"
                ),
            },
        ),
        (
            "groundloop_pipeline_artifact_use",
            {"run_id": "'d30-f13-decoy-use-run-'||series.value::text"},
        ),
        (
            "groundloop_m4_claim_registry_snapshot",
            {
                "claim_registry_snapshot_id": (
                    "'d30-f13-decoy-registry-'||series.value::text"
                ),
            },
        ),
        (
            "groundloop_m4_claim_registry_member",
            {
                "claim_registry_snapshot_id": (
                    "'d30-f13-decoy-member-registry-'||series.value::text"
                ),
            },
        ),
        (
            "groundloop_claim",
            {"claim_id": "'d30-f13-decoy-claim-'||series.value::text"},
        ),
        (
            "groundloop_document_version",
            {
                "document_version_id": (
                    "'d30-f13-decoy-document-version-'||series.value::text"
                ),
                "document_id": "'d30-f13-decoy-document-'||series.value::text",
            },
        ),
        (
            "groundloop_chunk_version",
            {
                "chunk_version_id": (
                    "'d30-f13-decoy-image-chunk-'||series.value::text"
                ),
                "document_version_id": (
                    "'d30-f13-decoy-image-version-'||series.value::text"
                ),
            },
        ),
    )
    for relation, overrides in clone_specs:
        _inflate_for_default_planner(
            connection,
            relation,
            overrides=overrides,
            # The activation closure joins four singleton rows to one epoch.
            # A wider epoch relation makes its exact base-epoch PK route win
            # under the default planner as well.
            count=(
                4096
                if relation == "groundloop_epoch"
                else 2048
                if relation == "groundloop_claim"
                else 384
            ),
        )
    for singleton_relation in (
        "groundloop_m5_activation",
        "groundloop_runtime_mode",
        "groundloop_m4_publication_head",
        "groundloop_m5_publication_head",
    ):
        connection.execute(f"ANALYZE {singleton_relation}")
    _inflate_for_default_planner(
        connection,
        "groundloop_observation_currency",
        overrides={
            "task_type": "'d30-f13-current-point-decoy-'||series.value::text",
            "chunk_version_id": "'d30-f13-current-point-chunk'",
            "observation_id": (
                "'d30-f13-current-point-decoy-observation-'||series.value::text"
            ),
        },
        count=384,
    )
    _inflate_for_default_planner(
        connection,
        "groundloop_published_observation_currency",
        overrides={
            "task_type": "'d30-f13-decoy-published-'||series.value::text",
            "chunk_version_id": (
                "'d30-f13-decoy-published-chunk-'||series.value::text"
            ),
            "valid_from_epoch": (
                "(SELECT max(epoch_id)-1 FROM groundloop_epoch "
                "WHERE event_id LIKE 'd30-f13-predecessor-epoch-%')"
            ),
            "valid_to_epoch": (
                "(SELECT max(epoch_id) FROM groundloop_epoch "
                "WHERE event_id LIKE 'd30-f13-predecessor-epoch-%')"
            ),
        },
        count=384,
    )


def _inflate_f13_exact_currency_points(
    connection: Any,
    *,
    claim_id: str,
    chunk_version_id: str,
    valid_from_epoch: int,
) -> None:
    """Make the tier-11a full-key point routes selective on their full keys."""

    task_expression = "'d30-f13-exact-point-task-'||series.value::text"
    observation_expression = "'d30-f13-exact-point-observation-'||series.value::text"
    shared_overrides = {
        "subject_kind": "'claim'::groundloop_subject_kind",
        "subject_id": f"'{claim_id}'",
        "chunk_version_id": f"'{chunk_version_id}'",
        "task_type": task_expression,
        "observation_id": observation_expression,
    }
    _inflate_for_default_planner(
        connection,
        "groundloop_semantic_observation",
        overrides={
            **shared_overrides,
            "input_hash": _hash_expression("exact-point-input"),
            "produced_epoch": str(valid_from_epoch),
            "raw_output_hash": _hash_expression("exact-point-output"),
        },
    )
    _inflate_for_default_planner(
        connection,
        "groundloop_observation_currency",
        overrides={
            **shared_overrides,
            "installed_revision": str(valid_from_epoch),
        },
    )
    _inflate_for_default_planner(
        connection,
        "groundloop_published_observation_currency",
        overrides={
            **shared_overrides,
            "valid_from_epoch": str(valid_from_epoch),
            "valid_to_epoch": "NULL::bigint",
        },
    )


class _OneRow:
    def __init__(self, row: tuple[object, ...]) -> None:
        self.row = row

    def fetchone(self) -> tuple[object, ...]:
        return self.row


class _IsolationCursor:
    def __init__(self, setting: str) -> None:
        self.setting = setting
        self.statements: list[str] = []

    def execute(self, statement: object, _parameters: object = None) -> _OneRow:
        rendered = " ".join(str(statement).split())
        self.statements.append(rendered)
        assert "transaction_isolation" in rendered
        return _OneRow((self.setting,))


@pytest.mark.parametrize(
    "setting",
    ("repeatable read", "serializable", "read uncommitted", "READ COMMITTED "),
)
def test_nonexact_read_committed_rejected_before_any_locator(setting: str) -> None:
    cursor = _IsolationCursor(setting)
    with pytest.raises(EventConflictError, match="READ COMMITTED|read committed"):
        postgres_withdrawal._require_d30_read_committed(cursor)  # type: ignore[arg-type]
    assert len(cursor.statements) == 1


def test_exact_read_committed_is_accepted_by_one_setting_point() -> None:
    cursor = _IsolationCursor("read committed")
    assert (
        postgres_withdrawal._require_d30_read_committed(cursor)  # type: ignore[arg-type]
        is None
    )
    assert len(cursor.statements) == 1


def test_isolation_guard_precedes_first_total_currency_locator(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_prepare_locked_document_open")
    assert source.index("_require_d30_read_committed") < source.index(
        "_gather_d29_locator_authority"
    )


def test_preview_isolation_guard_precedes_first_withdrawal_planning_read(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_preview_document_direct_open")
    assert source.index("_require_d30_read_committed") < source.index(
        "_direct_withdrawal_preview"
    )


def test_existing_indexes_bound_currency_job_and_dependency_routes(
    d30_schema: Any,
    function_source: Callable[[str], str],
) -> None:
    connection = d30_schema.connection
    with _savepoint(connection, "d30_existing_index_plans"):
        chunk = connection.execute(
            """SELECT chunk_version_id,count(*)
                 FROM groundloop_observation_currency
                GROUP BY chunk_version_id
                ORDER BY count(*) DESC,chunk_version_id COLLATE "C" LIMIT 1"""
        ).fetchone()
        assert chunk is not None and int(chunk[1]) > 0
        decoy_chunk = connection.execute(
            """SELECT chunk_version_id FROM groundloop_chunk_version
                WHERE chunk_version_id<>%s
                ORDER BY octet_length(chunk_version_id),
                         chunk_version_id COLLATE "C" LIMIT 1""",
            (str(chunk[0]),),
        ).fetchone()
        assert decoy_chunk is not None
        connection.execute(
            """INSERT INTO groundloop_semantic_observation (
                   observation_id,subject_kind,subject_id,chunk_version_id,
                   task_type,support_score,refute_score,neutral_score,
                   model_id,model_version,prompt_version,input_hash,
                   produced_epoch,raw_output_hash,eligible_for_currency
               )
               SELECT 'd30-plan-decoy-observation-'||series.value::text,
                      'claim',%s,%s,
                      'd30-plan-decoy-task-'||series.value::text,
                      0.8,0.1,0.1,'fixture-model','v1','p1',repeat('a',64),
                      %s,NULL,true
               FROM generate_series(1,1024) AS series(value)""",
            (
                str(d30_schema.database.base.claim_ids[0]),
                str(decoy_chunk[0]),
                int(d30_schema.database.base.epoch_id),
            ),
        )
        connection.execute(
            """INSERT INTO groundloop_observation_currency (
                   subject_kind,subject_id,chunk_version_id,task_type,
                   observation_id,installed_revision
               )
               SELECT subject_kind,subject_id,chunk_version_id,task_type,
                      observation_id,0
               FROM groundloop_semantic_observation
               WHERE observation_id LIKE 'd30-plan-decoy-observation-%'"""
        )
        epoch_id = int(d30_schema.first_m4["epoch_id"])
        unrelated_epoch = int(d30_schema.first_m5["epoch_id"])
        assert unrelated_epoch != epoch_id
        inserted_jobs = connection.execute(
            """INSERT INTO groundloop_semantic_job (
                   job_id,epoch_id,parent_job_id,job_kind,candidate_policy_id,
                   payload_hash,execution_spec_hash,claim_id,chunk_version_id,
                   expandable,job_state,child_closed,created_revision
               )
               SELECT 'd30-plan-unrelated-job-'||series.value::text,
                      %s,NULL,'frontier_retrieve',source.candidate_policy_id,
                      source.payload_hash,source.execution_spec_hash,
                      source.claim_id,NULL,true,'declared',false,0
               FROM generate_series(1,1024) AS series(value)
               CROSS JOIN LATERAL (
                   SELECT candidate_policy_id,payload_hash,
                          execution_spec_hash,claim_id
                   FROM groundloop_semantic_job
                   WHERE epoch_id=%s AND job_kind='frontier_retrieve'
                   ORDER BY job_id COLLATE "C" LIMIT 1
               ) AS source""",
            (unrelated_epoch, epoch_id),
        ).rowcount
        assert inserted_jobs == 1024
        inserted_dependencies = connection.execute(
            """INSERT INTO groundloop_semantic_job_dependency (
                   epoch_id,parent_job_id,child_job_id
               )
               SELECT %s,
                      'd30-plan-unrelated-job-'||(2*series.value-1)::text,
                      'd30-plan-unrelated-job-'||(2*series.value)::text
               FROM generate_series(1,512) AS series(value)""",
            (unrelated_epoch,),
        ).rowcount
        assert inserted_dependencies == 512
        for relation in (
            "groundloop_observation_currency",
            "groundloop_semantic_job",
            "groundloop_semantic_job_dependency",
        ):
            connection.execute(f"ANALYZE {relation}")
        currency_nodes = _analyze(
            connection,
            """SELECT subject_kind::text,subject_id,chunk_version_id,task_type,
                      observation_id,installed_revision
                 FROM groundloop_observation_currency
                WHERE chunk_version_id=%s
                ORDER BY subject_kind::text COLLATE "C",subject_id COLLATE "C",
                         chunk_version_id COLLATE "C",task_type COLLATE "C",
                         observation_id COLLATE "C"
            """,
            (str(chunk[0]),),
        )
        _assert_exact_index_route(
            currency_nodes,
            index_name="groundloop_current_observations_by_chunk",
            expected_rows=int(chunk[1]),
        )

        job_count = connection.execute(
            "SELECT count(*) FROM groundloop_semantic_job WHERE epoch_id=%s",
            (epoch_id,),
        ).fetchone()
        dependency_count = connection.execute(
            "SELECT count(*) FROM groundloop_semantic_job_dependency WHERE epoch_id=%s",
            (epoch_id,),
        ).fetchone()
        assert job_count is not None and dependency_count is not None
        job_statement = """
            SELECT job_id, epoch_id, parent_job_id, job_kind,
                   candidate_policy_id, payload_hash, execution_spec_hash,
                   claim_id, chunk_version_id, expandable, job_state,
                   child_closed, child_set_hash, completion_digest,
                   result_artifact_id, result_artifact_hash,
                   created_revision, completed_revision, created_at, completed_at
            FROM groundloop_semantic_job
            WHERE epoch_id = %s
            ORDER BY job_id COLLATE "C"
        """
        dependency_statement = """
            SELECT epoch_id, parent_job_id, child_job_id
            FROM groundloop_semantic_job_dependency
            WHERE epoch_id = %s
            ORDER BY parent_job_id COLLATE "C", child_job_id COLLATE "C"
        """

        def compact(value: str) -> str:
            return "".join(value.lower().split())

        owner_source = compact(function_source("_d30_owner_locator"))
        assert compact(job_statement) in owner_source
        assert compact(dependency_statement) in owner_source
        job_nodes = _analyze(connection, job_statement, (epoch_id,))
        dependency_nodes = _analyze(
            connection,
            dependency_statement,
            (epoch_id,),
        )
        _assert_exact_index_route(
            job_nodes,
            index_name="groundloop_m4_job_by_epoch",
            expected_rows=int(job_count[0]),
        )
        _assert_exact_index_route(
            dependency_nodes,
            index_name="groundloop_semantic_job_dependency_pkey",
            expected_rows=int(dependency_count[0]),
        )
        assert all(node.get("Node Type") != "Seq Scan" for node in job_nodes)
        assert all(node.get("Node Type") != "Seq Scan" for node in dependency_nodes)
        assert int(job_nodes[0].get("Actual Rows", 0)) == int(job_count[0])
        assert int(dependency_nodes[0].get("Actual Rows", 0)) == int(
            dependency_count[0]
        )


def test_dynamic_f13_default_plans_trace_and_cardinality_ledger(
    d30_schema: Any,
    function_source: Callable[[str], str],
) -> None:
    """Execute every dynamic F13 route that exists in the Lane-P planner."""

    connection = d30_schema.connection
    with _savepoint(connection, "d30_f13_dynamic_routes"):
        target = _seed_f13_route_rows(d30_schema)
        epoch_id = int(target["epoch_id"])
        root_job_id = str(target["root_job_id"])
        child_job_id = str(target["child_job_id"])
        claim_id = str(target["claim_id"])
        chunk_id = str(target["chunk_id"])
        task = str(target["dynamic_task"])
        observation_id = str(target["dynamic_observation"])
        current_point_chunk = str(target["current_point_chunk"])
        current_point_task = str(target["current_point_task"])
        admitted_pair_id = str(target["admitted_pair_id"])
        history_task = str(target["history_task"])
        open_task = str(target["open_task"])
        history_predecessor_epoch_id = int(target["history_predecessor_epoch_id"])
        history_valid_from_epoch = int(target["history_valid_from_epoch"])

        _inflate_f13_relations(connection)
        representative_shape_rows = {
            "K": int(
                connection.execute(
                    "SELECT count(*) FROM groundloop_observation_currency "
                    "WHERE chunk_version_id=%s",
                    (chunk_id,),
                ).fetchone()[0]
            ),
            "J_e": int(
                connection.execute(
                    "SELECT count(*) FROM groundloop_semantic_job WHERE epoch_id=%s",
                    (epoch_id,),
                ).fetchone()[0]
            ),
            "D_e": int(
                connection.execute(
                    "SELECT count(*) FROM groundloop_semantic_job_dependency "
                    "WHERE epoch_id=%s",
                    (epoch_id,),
                ).fetchone()[0]
            ),
            "S_e": 1,
            "A_e": 1,
            "O_e": 1,
            "B": 1,
        }
        assert (
            representative_shape_rows["K"] == 1
            and representative_shape_rows["J_e"] == 3
            and representative_shape_rows["D_e"] == 1
        )

        routes = (
            _F13Route(
                "current-currency chunk range",
                "K",
                '''SELECT subject_kind::text,subject_id,chunk_version_id,
                          task_type,observation_id,installed_revision
                     FROM groundloop_observation_currency
                    WHERE chunk_version_id=%s
                    ORDER BY subject_kind::text COLLATE "C",subject_id COLLATE "C",
                             chunk_version_id COLLATE "C",task_type COLLATE "C",
                             observation_id COLLATE "C"''',
                (chunk_id,),
                "groundloop_current_observations_by_chunk",
                representative_shape_rows["K"],
            ),
            _F13Route(
                "source-epoch jobs",
                "J_e",
                '''SELECT job_id,epoch_id,parent_job_id,job_kind,
                          candidate_policy_id,payload_hash,execution_spec_hash,
                          claim_id,chunk_version_id,expandable,job_state,
                          child_closed,child_set_hash,completion_digest,
                          result_artifact_id,result_artifact_hash,
                          created_revision,completed_revision,created_at,completed_at
                     FROM groundloop_semantic_job WHERE epoch_id=%s
                    ORDER BY job_id COLLATE "C"''',
                (epoch_id,),
                "groundloop_m4_job_by_epoch",
                representative_shape_rows["J_e"],
            ),
            _F13Route(
                "source-epoch dependencies",
                "D_e",
                '''SELECT epoch_id,parent_job_id,child_job_id
                     FROM groundloop_semantic_job_dependency WHERE epoch_id=%s
                    ORDER BY parent_job_id COLLATE "C",child_job_id COLLATE "C"''',
                (epoch_id,),
                "groundloop_semantic_job_dependency_pkey",
                representative_shape_rows["D_e"],
            ),
            _F13Route(
                "owner epoch",
                "constant",
                """SELECT epoch_id, event_id, payload_hash, revision,
                          structural_status, semantic_status, evaluation_state,
                          publication_mode, sealed_at
                     FROM groundloop_epoch WHERE epoch_id = %s""",
                (epoch_id,),
                "groundloop_epoch_pkey",
                1,
            ),
            _F13Route(
                "owner update",
                "constant",
                """SELECT epoch_id, update_kind, candidate_policy_id,
                          previous_published_epoch_id, registry_snapshot_id,
                          manifest
                     FROM groundloop_m4_update WHERE epoch_id = %s""",
                (epoch_id,),
                "groundloop_m4_update_pkey",
                1,
            ),
            _F13Route(
                "job point",
                "constant",
                """SELECT job_id, epoch_id, parent_job_id, job_kind,
                          candidate_policy_id, payload_hash, execution_spec_hash,
                          claim_id, chunk_version_id, expandable, job_state,
                          child_closed, child_set_hash, completion_digest,
                          result_artifact_id, result_artifact_hash,
                          created_revision, completed_revision, created_at,
                          completed_at
                     FROM groundloop_semantic_job
                    WHERE job_id = %s
                    FOR UPDATE""",
                (child_job_id,),
                "groundloop_semantic_job_job_id_epoch_id_key",
                1,
            ),
            _F13Route(
                "scope present",
                "S_e",
                """SELECT root_job_id, epoch_id, registry_snapshot_id,
                          scope_kind, explicit_claim_ids, closed_revision
                     FROM groundloop_discovery_scope WHERE root_job_id = %s""",
                (root_job_id,),
                "groundloop_discovery_scope_pkey",
                1,
            ),
            _F13Route(
                "scope absence",
                "constant",
                """SELECT root_job_id, epoch_id, registry_snapshot_id,
                          scope_kind, explicit_claim_ids, closed_revision
                     FROM groundloop_discovery_scope WHERE root_job_id = %s""",
                ("d30-f13-absent-scope",),
                "groundloop_discovery_scope_pkey",
                0,
            ),
            _F13Route(
                "attempt prefix",
                "A_e",
                """SELECT attempt_id,job_id,execution_spec_hash,attempt_ordinal,
                          lease_token_hash,attempt_state,lease_expires_at,
                          started_at,finished_at
                     FROM groundloop_semantic_job_attempt WHERE job_id=%s
                    ORDER BY attempt_ordinal""",
                (child_job_id,),
                "groundloop_semantic_job_attempt_job_id_attempt_ordinal_key",
                1,
            ),
            _F13Route(
                "discovery result",
                "O_e",
                """SELECT root_job_id, epoch_id, result_artifact_id,
                          result_artifact_hash, fallback_satisfied,
                          channel_hit_count, admitted_pair_count,
                          channel_set_hash, admitted_pair_set_hash
                     FROM groundloop_m4_discovery_result
                    WHERE root_job_id = %s""",
                (root_job_id,),
                "groundloop_m4_discovery_result_pkey",
                1,
            ),
            _F13Route(
                "composite terminal projection",
                "O_e",
                """SELECT epoch_id,job_id,terminal_state,terminal_reason,
                          m4_completion_digest,completed_revision,
                          terminal_identity_hash
                     FROM groundloop_m5_direct_terminal_projection
                    WHERE epoch_id=%s AND job_id=%s""",
                (epoch_id, child_job_id),
                "groundloop_m5_direct_terminal_projection_pkey",
                0,
            ),
            _F13Route(
                "cited admission",
                "constant",
                """SELECT admitted_pair_id, epoch_id, claim_id,
                          chunk_version_id, candidate_policy_id, fused_rank,
                          reasons, mandatory_lineage
                     FROM groundloop_admitted_pair WHERE admitted_pair_id = %s""",
                (admitted_pair_id,),
                "groundloop_admitted_pair_pkey",
                1,
            ),
            _F13Route(
                "execution by observation",
                "constant",
                """SELECT observation_id, job_id, btrim(admitted_pair_id),
                          model_artifact_id, prompt_artifact_id,
                          btrim(execution_spec_hash), btrim(pair_input_hash),
                          calibration_version,
                          btrim(calibration_artifact_sha256), temperature,
                          raw_logits, btrim(raw_output_hash),
                          reused_from_observation_id
                     FROM groundloop_m4_verification_execution
                    WHERE observation_id = %s""",
                (observation_id,),
                "groundloop_m4_verification_execution_pkey",
                1,
            ),
            _F13Route(
                "execution by job",
                "constant",
                """SELECT observation_id, job_id, btrim(admitted_pair_id),
                          model_artifact_id, prompt_artifact_id,
                          btrim(execution_spec_hash), btrim(pair_input_hash),
                          calibration_version,
                          btrim(calibration_artifact_sha256), temperature,
                          raw_logits, btrim(raw_output_hash),
                          reused_from_observation_id
                     FROM groundloop_m4_verification_execution
                    WHERE job_id = %s""",
                (child_job_id,),
                "groundloop_m4_verification_execution_job_id_key",
                1,
            ),
            _F13Route(
                "execution by admission",
                "constant",
                """SELECT observation_id, job_id, btrim(admitted_pair_id),
                          model_artifact_id, prompt_artifact_id,
                          btrim(execution_spec_hash), btrim(pair_input_hash),
                          calibration_version,
                          btrim(calibration_artifact_sha256), temperature,
                          raw_logits, btrim(raw_output_hash),
                          reused_from_observation_id
                     FROM groundloop_m4_verification_execution
                    WHERE admitted_pair_id = %s""",
                (admitted_pair_id,),
                "groundloop_m4_verification_execution_admitted_pair_id_key",
                1,
            ),
            _F13Route(
                "observation point",
                "constant",
                """SELECT observation_id, subject_kind::text, subject_id,
                          chunk_version_id, task_type, support_score,
                          refute_score, neutral_score, model_id, model_version,
                          prompt_version, input_hash, produced_epoch,
                          raw_output_hash, eligible_for_currency
                     FROM groundloop_semantic_observation
                    WHERE observation_id = %s""",
                (observation_id,),
                (
                    "groundloop_semantic_observation_pkey",
                    "groundloop_semantic_observati_observation_id_subject_kind_s_key",
                ),
                1,
            ),
            _F13Route(
                "working delta point",
                "constant",
                """SELECT epoch_id, subject_kind::text, subject_id,
                          chunk_version_id, task_type, base_observation_id,
                          working_observation_id, installed_revision
                     FROM groundloop_working_observation_delta
                    WHERE epoch_id = %s AND subject_kind = %s
                      AND subject_id = %s AND chunk_version_id = %s
                      AND task_type = %s
                    FOR UPDATE""",
                (epoch_id, "claim", claim_id, chunk_id, task),
                "groundloop_working_observation_delta_pkey",
                1,
            ),
            _F13Route(
                "current currency point",
                "constant",
                """SELECT subject_kind::text, subject_id, chunk_version_id,
                          task_type, observation_id, installed_revision
                     FROM groundloop_observation_currency
                    WHERE subject_kind = %s AND subject_id = %s
                      AND chunk_version_id = %s AND task_type = %s
                    FOR UPDATE""",
                ("claim", claim_id, current_point_chunk, current_point_task),
                "groundloop_observation_currency_pkey",
                1,
            ),
            _F13Route(
                "open-published currency point",
                "constant",
                """SELECT subject_kind::text, subject_id, chunk_version_id,
                          task_type, observation_id, valid_from_epoch,
                          valid_to_epoch
                     FROM groundloop_published_observation_currency
                    WHERE subject_kind = %s AND subject_id = %s
                      AND chunk_version_id = %s AND task_type = %s
                      AND valid_to_epoch IS NULL
                    FOR UPDATE""",
                ("claim", claim_id, chunk_id, open_task),
                "groundloop_one_current_published_observation",
                1,
            ),
            _F13Route(
                "predecessor backward candidate",
                "B",
                """SELECT subject_kind::text,subject_id,chunk_version_id,
                          task_type,observation_id,valid_from_epoch,valid_to_epoch
                     FROM groundloop_published_observation_currency
                    WHERE subject_kind=%s AND subject_id=%s
                      AND chunk_version_id=%s AND task_type=%s
                      AND valid_from_epoch <= %s
                    ORDER BY valid_from_epoch DESC
                    LIMIT 1""",
                (
                    "claim",
                    claim_id,
                    chunk_id,
                    history_task,
                    history_predecessor_epoch_id,
                ),
                "groundloop_published_observation_currency_pkey",
                1,
            ),
            _F13Route(
                "predecessor exact point lock route",
                "constant",
                """SELECT subject_kind::text,subject_id,chunk_version_id,
                          task_type,observation_id,valid_from_epoch,valid_to_epoch
                     FROM groundloop_published_observation_currency
                    WHERE subject_kind=%s AND subject_id=%s
                      AND chunk_version_id=%s AND task_type=%s
                      AND valid_from_epoch = %s
                    FOR UPDATE""",
                (
                    "claim",
                    claim_id,
                    chunk_id,
                    history_task,
                    history_valid_from_epoch,
                ),
                "groundloop_published_observation_currency_pkey",
                1,
            ),
            _F13Route(
                "verification model",
                "constant",
                """SELECT model_artifact_id, task, provider, model_id,
                          immutable_revision, tokenizer_revision, license_id,
                          config_hash, artifact_sha256
                     FROM groundloop_model_artifact WHERE model_artifact_id = %s""",
                (str(target["model_id"]),),
                "groundloop_model_artifact_pkey",
                1,
            ),
            _F13Route(
                "verification prompt",
                "constant",
                """SELECT prompt_artifact_id, task, version, template,
                          template_hash, decoding_config_hash
                     FROM groundloop_prompt_artifact
                    WHERE prompt_artifact_id = %s""",
                (str(target["prompt_id"]),),
                "groundloop_prompt_artifact_pkey",
                1,
            ),
        )
        assert len({route.label for route in routes}) == len(routes)
        assert "enable_seqscan" not in " ".join(route.statement for route in routes)
        route_source = {
            "current-currency chunk range": "_gather_d29_locator_authority",
            "source-epoch jobs": "_d30_owner_locator",
            "source-epoch dependencies": "_d30_owner_locator",
            "owner epoch": "_d30_owner_locator",
            "owner update": "_d30_owner_locator",
            "job point": "_lock_d29_jobs_and_reserve",
            "scope present": "_d30_owner_locator",
            "scope absence": "_d30_owner_locator",
            "attempt prefix": "_d30_owner_locator",
            "discovery result": "_d30_owner_locator",
            "composite terminal projection": "_d30_owner_locator",
            "cited admission": "_gather_d30_claim_authority",
            "observation point": "_d30_observation_row",
            "working delta point": "_lock_d29_observation_authority",
            "current currency point": "_lock_d29_observation_authority",
            "open-published currency point": "_lock_d29_observation_authority",
            "predecessor backward candidate": "_probe_d30_predecessor_currency",
            "predecessor exact point lock route": ("_lock_d29_observation_authority"),
            "verification model": "_gather_d30_claim_authority",
            "verification prompt": "_gather_d30_claim_authority",
        }

        def compact_sql(value: str) -> str:
            return "".join(value.lower().split())

        source_cache = {
            name: compact_sql(function_source(name))
            for name in set(route_source.values())
        }
        for route in routes:
            if route.label in route_source:
                assert (
                    compact_sql(route.statement)
                    in source_cache[route_source[route.label]]
                ), route.label

        execution_routes = tuple(
            route for route in routes if route.label.startswith("execution by ")
        )
        with connection.cursor() as raw_cursor:
            execution_trace = _RecordingCursor(raw_cursor)
            execution_rows = postgres_withdrawal._d30_execution_coordinates(
                execution_trace,  # type: ignore[arg-type]
                observation_id=observation_id,
                job_id=child_job_id,
                admitted_pair_id=admitted_pair_id,
            )
        assert all(row is not None for row in execution_rows)
        assert len(execution_trace.entries) == len(execution_routes) == 3
        assert tuple(compact_sql(route.statement) for route in execution_routes) == (
            tuple(compact_sql(str(entry["sql"])) for entry in execution_trace.entries)
        )
        assert set(route_source) | {route.label for route in execution_routes} == {
            route.label for route in routes
        }
        plan_failures: list[object] = []
        for route in routes:
            try:
                _assert_default_point_plan(connection, route)
            except AssertionError as error:
                plan_failures.append(error.args[0])
        assert not plan_failures, "\n".join(repr(item) for item in plan_failures)

        with connection.cursor() as raw_cursor:
            trace = _RecordingCursor(raw_cursor)
            for route in routes:
                rows = trace.execute(route.statement, route.parameters).fetchall()
                assert len(rows) == route.expected_rows, route.label
        assert len(trace.entries) == len(routes)
        assert tuple(entry["returned_rows"] for entry in trace.entries) == tuple(
            route.expected_rows for route in routes
        )
        trace_text = "\n".join(str(entry["sql"]) for entry in trace.entries)
        for forbidden in (
            "groundloop_impact_channel_hit",
            "pairverificationinput",
            "pairverificationartifact",
            "pairjudgment",
            "migration_019",
        ):
            assert forbidden not in trace_text.lower()
        assert all(
            "d30-f13-decoy" not in str(parameter)
            for entry in trace.entries
            for parameter in entry["parameters"]  # type: ignore[union-attr]
        )
        observed_ledger = {
            term: sum(
                int(entry["returned_rows"])
                for route, entry in zip(routes, trace.entries, strict=True)
                if route.cardinality_term == term
            )
            for term in ("K", "J_e", "D_e", "S_e", "A_e", "O_e", "B")
        }
        # This first ledger counts one representative executable SQL shape per
        # term.  It is not the owner formula: the complete production sequence
        # below has all per-job point/absence coordinates and repeated guards.
        assert observed_ledger == representative_shape_rows
        assert sum(observed_ledger.values()) == (
            representative_shape_rows["K"]
            + representative_shape_rows["B"]
            + representative_shape_rows["J_e"]
            + representative_shape_rows["D_e"]
            + representative_shape_rows["S_e"]
            + representative_shape_rows["A_e"]
            + representative_shape_rows["O_e"]
        )

        # The earlier K route must stay selective on one chunk.  Only after it
        # has executed do we add same-chunk, different-full-key ballast for the
        # production tier-11a PK/unique point plans below.
        _inflate_f13_exact_currency_points(
            connection,
            claim_id=claim_id,
            chunk_version_id=chunk_id,
            valid_from_epoch=epoch_id,
        )

        # Execute the actual production call chain, in its frozen order, over
        # the coherent legacy owner installed above.  This is the executable
        # lock/rerun evidence; the route table is only its planner ledger.
        currency = postgres_withdrawal._D30CurrencyRow(
            subject_kind="claim",
            subject_id=claim_id,
            chunk_version_id=chunk_id,
            task_type=task,
            observation_id=observation_id,
            installed_revision=epoch_id,
        )
        authority_predecessor_epoch_id = int(target["authority_predecessor_epoch_id"])
        candidate_policy_id = str(target["candidate_policy_id"])
        verifier_execution_spec_hash = str(target["verifier_execution_spec_hash"])
        with connection.cursor() as raw_cursor:
            sequence_trace = _RecordingCursor(raw_cursor)
            d30_claims = postgres_withdrawal._gather_d30_claim_authority(
                sequence_trace,  # type: ignore[arg-type]
                (currency,),
            )
            (
                direct_claim_before_images,
                direct_answer_before_images,
                direct_remaining_observation_rows,
                direct_observation_source_rows,
                direct_remaining_currency_rows,
            ) = postgres_withdrawal._gather_d29_direct_state_locators(
                sequence_trace,  # type: ignore[arg-type]
                d30_claims,
                predecessor_epoch_id=authority_predecessor_epoch_id,
            )
            assert direct_remaining_observation_rows == ()
            assert direct_remaining_currency_rows == ()
            gather_boundary = len(sequence_trace.entries)
            assert len(d30_claims.dynamic) == len(d30_claims.owners) == 1
            gathered_owner = d30_claims.owners[0]
            direct_job_ids = tuple(sorted(str(row[0]) for row in gathered_owner.jobs))
            direct_job_coordinates = tuple(
                sorted((gathered_owner.epoch_id, job_id) for job_id in direct_job_ids)
            )
            direct_dependency_coordinates = tuple(
                (int(str(row[0])), str(row[1]), str(row[2]))
                for row in gathered_owner.dependencies
            )
            locator = postgres_withdrawal._D29LocatorAuthority(
                predecessor_snapshots=None,
                admitted_locator_keys=(),
                qualifying_admitted_pair_digests=(),
                candidate_root_job_ids=(),
                candidate_root_job_coordinates=(),
                candidate_dependency_coordinates=(),
                direct_job_ids=direct_job_ids,
                direct_job_coordinates=direct_job_coordinates,
                direct_dependency_coordinates=direct_dependency_coordinates,
                direct_admitted_pair_ids=(admitted_pair_id,),
                direct_scope_root_job_ids=(root_job_id,),
                direct_verifier_observation_ids=(observation_id,),
                verifier_job_ids=(),
                verifier_root_job_ids=(),
                verifier_observation_ids=(),
                verifier_dependency_coordinates=(),
                verifier_artifact_ids=(),
                verifier_pair_input_hashes=(),
                bootstrap_observation_coordinates=(),
                direct_bootstrap_observation_coordinates=(),
                requirement_currency_keys=(),
                direct_currency_keys=((claim_id, chunk_id, task, observation_id),),
                direct_frontier_keys=(),
                touched_requirement_ids=(),
                prospective_coordinates=(
                    postgres_withdrawal._DocumentDeclarationCoordinates(
                        direct_scope_root_job_ids=(),
                        requirement_scope_root_job_ids=(),
                        direct_job_ids=(),
                        requirement_job_ids=(),
                    )
                ),
                d30_claims=d30_claims,
                direct_claim_before_images=direct_claim_before_images,
                direct_answer_before_images=direct_answer_before_images,
                direct_remaining_observation_rows=(direct_remaining_observation_rows),
                direct_observation_source_rows=direct_observation_source_rows,
            )
            direct_scopes = postgres_withdrawal._lock_d29_scopes_and_reserve(
                sequence_trace,  # type: ignore[arg-type]
                locator,
            )
            scope_boundary = len(sequence_trace.entries)
            postgres_withdrawal._lock_d29_jobs_and_reserve(
                sequence_trace,  # type: ignore[arg-type]
                locator,
            )
            job_boundary = len(sequence_trace.entries)
            direct_attempts, direct_candidates = (
                postgres_withdrawal._lock_d29_tier_10_authority(
                    sequence_trace,  # type: ignore[arg-type]
                    locator,
                    predecessor_epoch_id=authority_predecessor_epoch_id,
                    candidate_policy_id=candidate_policy_id,
                )
            )
            tier10_boundary = len(sequence_trace.entries)
            assert direct_candidates == ()
            dynamic_authority = postgres_withdrawal._lock_d30_dynamic_owner_topology(
                sequence_trace,  # type: ignore[arg-type]
                locator,
                direct_attempts,
                direct_scopes,
                predecessor_epoch_id=authority_predecessor_epoch_id,
                candidate_policy_id=candidate_policy_id,
                verifier_execution_spec_hash=verifier_execution_spec_hash,
            )
            topology_boundary = len(sequence_trace.entries)
            observation_edges, direct_observations = (
                postgres_withdrawal._lock_d29_observation_authority(
                    sequence_trace,  # type: ignore[arg-type]
                    locator,
                    predecessor_epoch_id=authority_predecessor_epoch_id,
                    candidate_policy_id=candidate_policy_id,
                    verifier_authority={},
                    bootstrap_authority={},
                    d30_dynamic_authority=dynamic_authority,
                )
            )
            observation_boundary = len(sequence_trace.entries)
        assert observation_edges == ()
        assert len(direct_observations) == 1
        assert set(direct_attempts) == set(direct_job_ids)
        assert set(direct_scopes) == {root_job_id}
        assert set(dynamic_authority) == {observation_id}
        assert (
            gather_boundary,
            scope_boundary,
            job_boundary,
            tier10_boundary,
            topology_boundary,
            observation_boundary,
        ) == (34, 38, 45, 64, 67, 75)
        assert all(
            entry["returned_rows"] is not None for entry in sequence_trace.entries
        )
        production_sequence_sql = "\n".join(
            str(entry["sql"]) for entry in sequence_trace.entries
        ).lower()
        for required_relation in (
            "groundloop_m5_activation",
            "groundloop_discovery_scope",
            "groundloop_semantic_job",
            "groundloop_semantic_job_attempt",
            "groundloop_m5_direct_terminal_projection",
            "groundloop_semantic_job_dependency",
            "groundloop_m4_discovery_result",
            "groundloop_admitted_pair",
            "groundloop_m4_verification_execution",
            "groundloop_semantic_observation",
            "groundloop_claim",
            "groundloop_published_claim_state",
            "groundloop_claim_state_materialized",
            "groundloop_claim_certificate",
            "groundloop_published_answer_state",
            "groundloop_chunk_version",
            "groundloop_document_version",
            "groundloop_working_observation_delta",
            "groundloop_observation_currency",
            "groundloop_published_observation_currency",
        ):
            assert required_relation in production_sequence_sql
        assert production_sequence_sql.count("for update") >= 20

        # This is a source-complete manifest for the actual production call
        # chain above.  Each tuple pins returned rows, the only touched
        # relations, every required named route, and any explicit alternative
        # unique/range route.  No entry is accepted merely because it used
        # some index.
        sequence_manifest: dict[
            str,
            tuple[
                tuple[int, ...],
                frozenset[str],
                tuple[str, ...],
                tuple[str, ...],
                int | None,
            ],
        ] = {
            "activation closure": (
                (1,),
                frozenset(
                    {
                        "groundloop_m5_activation",
                        "groundloop_runtime_mode",
                        "groundloop_m4_publication_head",
                        "groundloop_m5_publication_head",
                        "groundloop_epoch",
                    }
                ),
                ("groundloop_epoch_pkey",),
                (),
                1,
            ),
            "observation point": (
                (1, 1),
                frozenset({"groundloop_semantic_observation"}),
                (),
                (
                    "groundloop_semantic_observation_pkey",
                    "groundloop_semantic_observati_observation_id_subject_kind_s_key",
                ),
                1,
            ),
            "direct claim owner point": (
                (1,),
                frozenset({"groundloop_claim"}),
                ("groundloop_claim_pkey",),
                (),
                1,
            ),
            "direct published claim point": (
                (1,),
                frozenset({"groundloop_published_claim_state"}),
                ("groundloop_published_claim_state_no_overlap",),
                (),
                1,
            ),
            "direct materialized claim point": (
                (1,),
                frozenset({"groundloop_claim_state_materialized"}),
                ("groundloop_claim_state_materialized_pkey",),
                (),
                1,
            ),
            "direct claim certificate point": (
                (1,),
                frozenset({"groundloop_claim_certificate"}),
                ("groundloop_claim_certificate_pkey",),
                (),
                1,
            ),
            "direct published answer point": (
                (1,),
                frozenset({"groundloop_published_answer_state"}),
                ("groundloop_published_answer_state_pkey",),
                (),
                1,
            ),
            "direct source closure point": (
                (1,),
                frozenset(
                    {
                        "groundloop_chunk_version",
                        "groundloop_document_version",
                    }
                ),
                (
                    "groundloop_chunk_version_pkey",
                    "groundloop_document_version_pkey",
                ),
                (),
                1,
            ),
            "source chunk point": (
                (1,),
                frozenset({"groundloop_chunk_version"}),
                ("groundloop_chunk_version_pkey",),
                (),
                1,
            ),
            "working delta point": (
                (1, 1, 1),
                frozenset({"groundloop_working_observation_delta"}),
                ("groundloop_working_observation_delta_pkey",),
                (),
                1,
            ),
            "preliminary previous epoch": (
                (1,),
                frozenset({"groundloop_m4_update"}),
                ("groundloop_m4_update_pkey",),
                (),
                1,
            ),
            "predecessor backward probe": (
                (1, 1),
                frozenset({"groundloop_published_observation_currency"}),
                ("groundloop_published_observation_currency_pkey",),
                (),
                1,
            ),
            "predecessor exact lock point": (
                (1,),
                frozenset({"groundloop_published_observation_currency"}),
                ("groundloop_published_observation_currency_pkey",),
                (),
                1,
            ),
            "owner epoch point": (
                (1, 1),
                frozenset({"groundloop_epoch"}),
                ("groundloop_epoch_pkey",),
                (),
                1,
            ),
            "owner update point": (
                (1, 1),
                frozenset({"groundloop_m4_update"}),
                ("groundloop_m4_update_pkey",),
                (),
                1,
            ),
            "owner job range": (
                (3, 3),
                frozenset({"groundloop_semantic_job"}),
                ("groundloop_m4_job_by_epoch",),
                (),
                3,
            ),
            "owner dependency range": (
                (1, 1),
                frozenset({"groundloop_semantic_job_dependency"}),
                ("groundloop_semantic_job_dependency_pkey",),
                (),
                1,
            ),
            "owner scope points": (
                (1, 0, 0, 1, 0, 0),
                frozenset({"groundloop_discovery_scope"}),
                ("groundloop_discovery_scope_pkey",),
                (),
                1,
            ),
            "owner attempt prefixes": (
                (1, 1, 1, 1, 1, 1),
                frozenset({"groundloop_semantic_job_attempt"}),
                ("groundloop_semantic_job_attempt_job_id_attempt_ordinal_key",),
                (),
                1,
            ),
            "owner discovery-result points": (
                (1, 1, 0, 1, 1),
                frozenset({"groundloop_m4_discovery_result"}),
                ("groundloop_m4_discovery_result_pkey",),
                (),
                1,
            ),
            "owner projection points": (
                (0, 0, 0, 0, 0, 0),
                frozenset({"groundloop_m5_direct_terminal_projection"}),
                ("groundloop_m5_direct_terminal_projection_pkey",),
                (),
                1,
            ),
            "legacy runtime-header absence": (
                (0, 0),
                frozenset({"groundloop_m5_runtime_epoch"}),
                ("groundloop_m5_runtime_epoch_pkey",),
                (),
                1,
            ),
            "admitted-pair point": (
                (1, 1),
                frozenset({"groundloop_admitted_pair"}),
                ("groundloop_admitted_pair_pkey",),
                (),
                1,
            ),
            "execution observation point": (
                (1, 1),
                frozenset({"groundloop_m4_verification_execution"}),
                ("groundloop_m4_verification_execution_pkey",),
                (),
                1,
            ),
            "execution job point": (
                (1, 1),
                frozenset({"groundloop_m4_verification_execution"}),
                ("groundloop_m4_verification_execution_job_id_key",),
                (),
                1,
            ),
            "execution admission point": (
                (1, 1),
                frozenset({"groundloop_m4_verification_execution"}),
                ("groundloop_m4_verification_execution_admitted_pair_id_key",),
                (),
                1,
            ),
            "verification model point": (
                (1, 1),
                frozenset({"groundloop_model_artifact"}),
                ("groundloop_model_artifact_pkey",),
                (),
                1,
            ),
            "verification prompt point": (
                (1, 1),
                frozenset({"groundloop_prompt_artifact"}),
                ("groundloop_prompt_artifact_pkey",),
                (),
                1,
            ),
            "scope lock points": (
                (1, 0, 0),
                frozenset({"groundloop_discovery_scope", "groundloop_m4_update"}),
                (
                    "groundloop_discovery_scope_pkey",
                    "groundloop_m4_update_pkey",
                ),
                (),
                None,
            ),
            "candidate-policy point": (
                (1,),
                frozenset({"groundloop_candidate_policy"}),
                ("groundloop_candidate_policy_pkey",),
                (),
                1,
            ),
            "job lock points": (
                (1, 1, 1),
                frozenset({"groundloop_semantic_job"}),
                ("groundloop_semantic_job_job_id_epoch_id_key",),
                (),
                1,
            ),
            "job-state guards": (
                (1, 1, 1),
                frozenset({"groundloop_semantic_job"}),
                ("groundloop_semantic_job_job_id_epoch_id_key",),
                (),
                1,
            ),
            "dependency lock point": (
                (1,),
                frozenset({"groundloop_semantic_job_dependency"}),
                ("groundloop_semantic_job_dependency_pkey",),
                (),
                1,
            ),
            "current-currency lock point": (
                (1,),
                frozenset({"groundloop_observation_currency"}),
                ("groundloop_observation_currency_pkey",),
                (),
                1,
            ),
            "open-published lock point": (
                (1,),
                frozenset({"groundloop_published_observation_currency"}),
                ("groundloop_one_current_published_observation",),
                (),
                1,
            ),
        }

        def sequence_family(statement: str) -> str:
            compact = " ".join(statement.lower().split())
            if "from groundloop_m5_activation as activation" in compact:
                return "activation closure"
            if "from groundloop_discovery_scope as scope" in compact:
                return "scope lock points"
            if "from groundloop_semantic_observation" in compact:
                return "observation point"
            if "from groundloop_claim where" in compact:
                return "direct claim owner point"
            if "from groundloop_published_claim_state" in compact:
                return "direct published claim point"
            if "from groundloop_claim_state_materialized" in compact:
                return "direct materialized claim point"
            if "from groundloop_claim_certificate" in compact:
                return "direct claim certificate point"
            if "from groundloop_published_answer_state" in compact:
                return "direct published answer point"
            if "from groundloop_chunk_version as chunk" in compact:
                return "direct source closure point"
            if "from groundloop_chunk_version" in compact:
                return "source chunk point"
            if "from groundloop_working_observation_delta" in compact:
                return "working delta point"
            if "from groundloop_m4_update" in compact:
                if compact.startswith("select previous_published_epoch_id"):
                    return "preliminary previous epoch"
                return "owner update point"
            if "from groundloop_published_observation_currency" in compact:
                if "valid_to_epoch is null" in compact:
                    return "open-published lock point"
                if "valid_from_epoch = %s" in compact:
                    return "predecessor exact lock point"
                return "predecessor backward probe"
            if "from groundloop_epoch where" in compact:
                return "owner epoch point"
            if "from groundloop_semantic_job_dependency" in compact:
                if "and parent_job_id = %s" in compact:
                    return "dependency lock point"
                return "owner dependency range"
            if "from groundloop_discovery_scope where" in compact:
                return "owner scope points"
            if "from groundloop_semantic_job_attempt" in compact:
                return "owner attempt prefixes"
            if "from groundloop_m4_discovery_result" in compact:
                return "owner discovery-result points"
            if "from groundloop_m5_direct_terminal_projection" in compact:
                return "owner projection points"
            if "from groundloop_m5_runtime_epoch" in compact:
                return "legacy runtime-header absence"
            if "from groundloop_admitted_pair" in compact:
                return "admitted-pair point"
            if "from groundloop_m4_verification_execution" in compact:
                if "where observation_id" in compact:
                    return "execution observation point"
                if "where job_id" in compact:
                    return "execution job point"
                return "execution admission point"
            if "from groundloop_model_artifact" in compact:
                return "verification model point"
            if "from groundloop_prompt_artifact" in compact:
                return "verification prompt point"
            if "from groundloop_candidate_policy" in compact:
                return "candidate-policy point"
            if "from groundloop_semantic_job where" in compact:
                if compact.startswith("select execution_spec_hash"):
                    return "job-state guards"
                if "where job_id = %s" in compact:
                    return "job lock points"
                return "owner job range"
            if "from groundloop_observation_currency" in compact:
                return "current-currency lock point"
            raise AssertionError(("unclassified production SQL", statement))

        entries_by_family: dict[str, list[dict[str, object]]] = {
            label: [] for label in sequence_manifest
        }
        for sequence_entry in sequence_trace.entries:
            label = sequence_family(str(sequence_entry["sql"]))
            entries_by_family[label].append(sequence_entry)
            (
                _expected_rows,
                expected_relations,
                expected_indexes,
                expected_any_indexes,
                maximum_index_rows,
            ) = sequence_manifest[label]
            nodes = _assert_captured_default_plan(
                connection,
                sequence_entry,
                expected_indexes=expected_indexes,
                expected_rows=int(sequence_entry["returned_rows"]),
                expected_any_indexes=expected_any_indexes,
                allowed_seq_relations=(
                    frozenset(
                        {
                            "groundloop_m5_activation",
                            "groundloop_runtime_mode",
                            "groundloop_m4_publication_head",
                            "groundloop_m5_publication_head",
                        }
                    )
                    if label == "activation closure"
                    else frozenset()
                ),
                maximum_index_rows=maximum_index_rows,
            )
            relation_names = frozenset(
                str(node["Relation Name"])
                for node in nodes
                if node.get("Relation Name") is not None
            )
            assert relation_names == expected_relations, (
                label,
                relation_names,
                expected_relations,
            )
            if label == "predecessor backward probe":
                pkey_nodes = tuple(
                    node
                    for node in nodes
                    if node.get("Index Name")
                    == "groundloop_published_observation_currency_pkey"
                )
                assert len(pkey_nodes) == 1
                assert pkey_nodes[0].get("Scan Direction") == "Backward"
        for label, (
            expected_rows,
            _relations,
            _indexes,
            _any_indexes,
            _maximum_index_rows,
        ) in sequence_manifest.items():
            family_entries = entries_by_family[label]
            assert tuple(entry["returned_rows"] for entry in family_entries) == (
                expected_rows
            ), label
        assert sum(len(entries) for entries in entries_by_family.values()) == 75

        actual_owner_formula = {
            "K": 1,
            "J_e": len(gathered_owner.jobs),
            "D_e": len(gathered_owner.dependencies),
            "S_e": sum(row is not None for _job_id, row in gathered_owner.scopes),
            "A_e": sum(len(rows) for _job_id, rows in gathered_owner.attempts),
            "O_e": sum(
                row is not None
                for points in (
                    gathered_owner.discovery_results,
                    gathered_owner.projections,
                )
                for _job_id, row in points
            ),
            "B": sum(
                dynamic.predecessor_candidate is not None
                for dynamic in d30_claims.dynamic
            ),
        }
        assert actual_owner_formula == {
            "K": 1,
            "J_e": 3,
            "D_e": 1,
            "S_e": 1,
            "A_e": 3,
            "O_e": 2,
            "B": 1,
        }
        # Expose the exact private ordering inputs separately from both the
        # returned-row formula and SQL query-coordinate counts.  These are the
        # coordinates consumed by the production currency/owner/topology,
        # tier-10, and tier-11a ordering steps.
        owner_job_ids = tuple(str(row[0]) for row in gathered_owner.jobs)
        owner_dependency_coordinates = tuple(
            (int(str(row[0])), str(row[1]), str(row[2]))
            for row in gathered_owner.dependencies
        )
        owner_attempt_coordinates = tuple(
            (job_id, str(row[0]), int(str(row[3])))
            for job_id, rows in gathered_owner.attempts
            for row in rows
        )
        owner_projection_coordinates = tuple(
            (gathered_owner.epoch_id, job_id)
            for job_id, _row in gathered_owner.projections
        )
        owner_root_coordinates = tuple(
            (gathered_owner.epoch_id, str(row[0]))
            for row in gathered_owner.jobs
            if row[2] is None
        )
        dynamic_private_order_inputs = {
            "currency": tuple(
                (*row.full_key, row.observation_id) for row in d30_claims.currency_rows
            ),
            "source_epochs": tuple(owner.epoch_id for owner in d30_claims.owners),
            "topology_owners": tuple(owner.epoch_id for owner in d30_claims.owners),
            "owner_jobs": owner_job_ids,
            "owner_dependencies": owner_dependency_coordinates,
            "direct_job_ids": locator.direct_job_ids,
            "direct_job_coordinates": locator.direct_job_coordinates,
            "direct_dependency_coordinates": locator.direct_dependency_coordinates,
            "attempt_coordinates": owner_attempt_coordinates,
            "projection_coordinates": owner_projection_coordinates,
            "direct_root_coordinates": owner_root_coordinates,
            "tier11_typed_keys": tuple(
                (*row.full_key, row.observation_id) for row in d30_claims.currency_rows
            ),
            "dynamic_claims": tuple(
                row.currency.observation_id for row in d30_claims.dynamic
            ),
        }
        assert dynamic_private_order_inputs == {
            "currency": (("claim", claim_id, chunk_id, task, observation_id),),
            "source_epochs": (epoch_id,),
            "topology_owners": (epoch_id,),
            "owner_jobs": direct_job_ids,
            "owner_dependencies": direct_dependency_coordinates,
            "direct_job_ids": direct_job_ids,
            "direct_job_coordinates": direct_job_coordinates,
            "direct_dependency_coordinates": direct_dependency_coordinates,
            "attempt_coordinates": tuple(
                (job_id, str(rows[0][0]), 1) for job_id, rows in gathered_owner.attempts
            ),
            "projection_coordinates": direct_job_coordinates,
            "direct_root_coordinates": tuple(
                (epoch_id, str(row[0])) for row in gathered_owner.jobs if row[2] is None
            ),
            "tier11_typed_keys": (("claim", claim_id, chunk_id, task, observation_id),),
            "dynamic_claims": (observation_id,),
        }
        # Query coordinates are deliberately separate from returned-row terms:
        # present and absent points are all acquired, and guards rerun them.
        assert {
            "job_range_queries": len(entries_by_family["owner job range"]),
            "job_lock_coordinates": len(entries_by_family["job lock points"]),
            "dependency_range_queries": len(
                entries_by_family["owner dependency range"]
            ),
            "dependency_lock_coordinates": len(
                entries_by_family["dependency lock point"]
            ),
            "scope_point_queries": len(entries_by_family["owner scope points"])
            + len(entries_by_family["scope lock points"]),
            "attempt_prefix_queries": len(entries_by_family["owner attempt prefixes"]),
            "discovery_point_queries": len(
                entries_by_family["owner discovery-result points"]
            ),
            "projection_point_queries": len(
                entries_by_family["owner projection points"]
            ),
            "predecessor_probe_queries": len(
                entries_by_family["predecessor backward probe"]
            ),
        } == {
            "job_range_queries": 2,
            "job_lock_coordinates": 3,
            "dependency_range_queries": 2,
            "dependency_lock_coordinates": 1,
            "scope_point_queries": 9,
            "attempt_prefix_queries": 6,
            "discovery_point_queries": 5,
            "projection_point_queries": 6,
            "predecessor_probe_queries": 2,
        }
        current_route = next(
            route for route in routes if route.label == "current currency point"
        )
        current_sequence_entry = entries_by_family["current-currency lock point"]
        assert len(current_sequence_entry) == 1
        assert compact_sql(str(current_sequence_entry[0]["sql"])) == compact_sql(
            current_route.statement
        )
        # The earlier K route used its distinct chunk-range coordinate; this
        # source-identical tier-11a statement is proved independently on its
        # dense same-chunk coordinate and must use the composite primary key.

        # Bind the legacy owner points to the production locator as well.  In
        # particular this owner has no typed runtime row, while its per-job
        # prefixes exercise both present and absent rows.
        with connection.cursor() as raw_cursor:
            owner_trace = _RecordingCursor(raw_cursor)
            owner = postgres_withdrawal._d30_owner_locator(  # type: ignore[arg-type]
                owner_trace, epoch_id
            )
        assert owner.runtime_row is None and owner.d24 is None
        assert len(owner.jobs) == representative_shape_rows["J_e"]
        assert len(owner.dependencies) == representative_shape_rows["D_e"]
        assert sorted(row is not None for _job_id, row in owner.scopes) == [
            False,
            False,
            True,
        ]
        assert sorted(len(rows) for _job_id, rows in owner.attempts) == [1, 1, 1]
        assert sorted(row is not None for _job_id, row in owner.discovery_results) == [
            False,
            True,
            True,
        ]
        assert sorted(row is not None for _job_id, row in owner.projections) == [
            False,
            False,
            False,
        ]

        legacy_manifest = (
            (
                "from groundloop_epoch where",
                1,
                (1,),
                frozenset({"groundloop_epoch"}),
                ("groundloop_epoch_pkey",),
            ),
            (
                "from groundloop_m4_update where",
                1,
                (1,),
                frozenset({"groundloop_m4_update"}),
                ("groundloop_m4_update_pkey",),
            ),
            (
                "from groundloop_semantic_job where",
                1,
                (representative_shape_rows["J_e"],),
                frozenset({"groundloop_semantic_job"}),
                ("groundloop_m4_job_by_epoch",),
            ),
            (
                "from groundloop_semantic_job_dependency where",
                1,
                (representative_shape_rows["D_e"],),
                frozenset({"groundloop_semantic_job_dependency"}),
                ("groundloop_semantic_job_dependency_pkey",),
            ),
            (
                "from groundloop_discovery_scope where",
                representative_shape_rows["J_e"],
                tuple(1 if row is not None else 0 for _job_id, row in owner.scopes),
                frozenset({"groundloop_discovery_scope"}),
                ("groundloop_discovery_scope_pkey",),
            ),
            (
                "from groundloop_semantic_job_attempt where",
                representative_shape_rows["J_e"],
                tuple(len(rows) for _job_id, rows in owner.attempts),
                frozenset({"groundloop_semantic_job_attempt"}),
                ("groundloop_semantic_job_attempt_job_id_attempt_ordinal_key",),
            ),
            (
                "from groundloop_m4_discovery_result where",
                representative_shape_rows["J_e"],
                tuple(
                    1 if row is not None else 0
                    for _job_id, row in owner.discovery_results
                ),
                frozenset({"groundloop_m4_discovery_result"}),
                ("groundloop_m4_discovery_result_pkey",),
            ),
            (
                "from groundloop_m5_direct_terminal_projection where",
                representative_shape_rows["J_e"],
                tuple(
                    1 if row is not None else 0 for _job_id, row in owner.projections
                ),
                frozenset({"groundloop_m5_direct_terminal_projection"}),
                ("groundloop_m5_direct_terminal_projection_pkey",),
            ),
            (
                "from groundloop_m5_runtime_epoch",
                1,
                (0,),
                frozenset({"groundloop_m5_runtime_epoch"}),
                ("groundloop_m5_runtime_epoch_pkey",),
            ),
        )
        owner_statements = tuple(
            str(entry["sql"]).lower() for entry in owner_trace.entries
        )
        assert sum(route[1] for route in legacy_manifest) == len(owner_trace.entries)
        for (
            marker,
            expected_calls,
            expected_rows,
            _relations,
            _indexes,
        ) in legacy_manifest:
            matching_entries = tuple(
                entry
                for entry, statement in zip(
                    owner_trace.entries, owner_statements, strict=True
                )
                if marker in statement
            )
            assert len(matching_entries) == expected_calls
            assert tuple(entry["returned_rows"] for entry in matching_entries) == (
                expected_rows
            )
        for entry, statement in zip(owner_trace.entries, owner_statements, strict=True):
            matching_routes = tuple(
                route for route in legacy_manifest if route[0] in statement
            )
            assert len(matching_routes) == 1, statement
            expected_relations = matching_routes[0][3]
            expected_indexes = matching_routes[0][4]
            nodes = _assert_captured_default_plan(
                connection,
                entry,
                expected_indexes=expected_indexes,
                expected_rows=int(entry["returned_rows"]),
                maximum_index_rows=max(1, int(entry["returned_rows"])),
            )
            relation_names = frozenset(
                str(node["Relation Name"])
                for node in nodes
                if node.get("Relation Name") is not None
            )
            assert relation_names == expected_relations


def test_bootstrap_f13_uses_exact_production_sql_plans_and_trace(
    d30_schema: Any,
    function_source: Callable[[str], str],
) -> None:
    """Execute and plan the production M3 locator and locked image reruns."""

    connection = d30_schema.connection
    with _savepoint(connection, "d30_f13_bootstrap_routes"):
        target = _seed_f13_route_rows(d30_schema)
        _inflate_f13_relations(connection)
        # Give each exact image snapshot a dense same-snapshot member set so
        # the default planner must use the composite member PK for the target
        # claim.  These rollback-scoped rows are physical-plan ballast only.
        for image_epoch_id in tuple(target["image_epoch_ids"]):  # type: ignore[arg-type]
            registry = connection.execute(
                "SELECT registry_snapshot_id FROM groundloop_m4_update "
                "WHERE epoch_id=%s",
                (int(image_epoch_id),),
            ).fetchone()
            assert registry is not None
            inserted = connection.execute(
                """
                INSERT INTO groundloop_m4_claim_registry_member (
                    claim_registry_snapshot_id, claim_id, member_ordinal
                )
                SELECT %s, claim_id,
                       10000 + row_number() OVER (ORDER BY claim_id COLLATE "C")
                FROM groundloop_claim
                WHERE claim_id LIKE 'd30-f13-decoy-claim-%%'
                """,
                (str(registry[0]),),
            ).rowcount
            assert inserted == 2048
        connection.execute("ANALYZE groundloop_m4_claim_registry_member")
        currency = postgres_withdrawal._D30CurrencyRow(
            subject_kind="claim",
            subject_id=str(target["bootstrap_claim_id"]),
            chunk_version_id=str(target["bootstrap_chunk_id"]),
            task_type=str(target["bootstrap_task"]),
            observation_id=str(target["bootstrap_observation"]),
            installed_revision=0,
        )
        with connection.cursor() as raw_cursor:
            trace = _RecordingCursor(raw_cursor)
            observation = postgres_withdrawal._d30_observation_row(
                trace,
                currency.observation_id,  # type: ignore[arg-type]
            )
            locator = postgres_withdrawal._d30_m3_claim_locator(
                trace,  # type: ignore[arg-type]
                currency=currency,
                observation_row=observation,
            )
            preliminary_count = len(trace.entries)
            for image_row in locator.image_rows:
                locked = postgres_withdrawal._d30_m3_image_point(
                    trace,  # type: ignore[arg-type]
                    image_epoch_id=int(str(image_row[0])),
                    claim_id=str(image_row[6]),
                    chunk_version_id=str(image_row[15]),
                    lock_registry=True,
                )
                assert locked == image_row

        model_points = {
            str(row[0]): row for row in (locator.model_row, locator.embedding_model_row)
        }
        tier10_routes: list[_F13Route] = [
            _F13Route(
                "locked M3 epoch reread",
                "constant",
                """SELECT epoch_id, event_id, payload_hash, revision,
                          structural_status, semantic_status, evaluation_state,
                          publication_mode, sealed_at
                     FROM groundloop_epoch WHERE epoch_id = %s""",
                (int(str(locator.epoch_row[0])),),
                "groundloop_epoch_pkey",
                1,
            ),
            _F13Route(
                "locked M3 verification execution",
                "M",
                """SELECT observation_id, run_id, candidate_id,
                          model_artifact_id, prompt_artifact_id,
                          calibration_version, temperature, raw_logits,
                          raw_output_hash, reused_from_observation_id
                     FROM groundloop_verification_execution
                    WHERE observation_id = %s
                    FOR UPDATE""",
                (currency.observation_id,),
                "groundloop_verification_execution_pkey",
                1,
            ),
            _F13Route(
                "locked M3 run",
                "M",
                """SELECT run_id, schema_version, status, config_hash,
                          input_hash, corpus_hash, question_id,
                          answer_version_id, semantic_epoch_id, manifest,
                          failure_code, started_at, completed_at
                     FROM groundloop_pipeline_run WHERE run_id = %s
                    FOR UPDATE""",
                (str(locator.run_row[0]),),
                "groundloop_pipeline_run_pkey",
                1,
            ),
            _F13Route(
                "locked M3 candidate",
                "M",
                """SELECT candidate_id, run_id, query_kind, query_id,
                          claim_id, chunk_version_id,
                          embedding_model_artifact_id, method_version,
                          score, rank
                     FROM groundloop_retrieval_candidate
                    WHERE candidate_id = %s
                    FOR UPDATE""",
                (str(locator.candidate_row[0]),),
                "groundloop_retrieval_candidate_pkey",
                1,
            ),
        ]
        tier10_expected: list[tuple[object, ...]] = [
            locator.epoch_row,
            locator.execution_row,
            locator.run_row,
            locator.candidate_row,
        ]
        for artifact_id in sorted(model_points):
            tier10_routes.append(
                _F13Route(
                    f"locked M3 model {artifact_id}",
                    "M",
                    """SELECT model_artifact_id, task, provider, model_id,
                              immutable_revision, tokenizer_revision,
                              license_id, config_hash, artifact_sha256
                         FROM groundloop_model_artifact
                        WHERE model_artifact_id = %s
                        FOR UPDATE""",
                    (artifact_id,),
                    "groundloop_model_artifact_pkey",
                    1,
                )
            )
            tier10_expected.append(tuple(model_points[artifact_id]))
        tier10_routes.extend(
            (
                _F13Route(
                    "locked M3 prompt",
                    "M",
                    """SELECT prompt_artifact_id, task, version, template,
                              template_hash, decoding_config_hash
                         FROM groundloop_prompt_artifact
                        WHERE prompt_artifact_id = %s
                        FOR UPDATE""",
                    (str(locator.prompt_row[0]),),
                    "groundloop_prompt_artifact_pkey",
                    1,
                ),
                _F13Route(
                    "locked M3 embedding",
                    "M",
                    """SELECT chunk_version_id, model_artifact_id,
                              embedding::text, input_hash
                         FROM groundloop_chunk_embedding
                        WHERE chunk_version_id = %s AND model_artifact_id = %s
                        FOR UPDATE""",
                    (
                        currency.chunk_version_id,
                        str(locator.embedding_model_row[0]),
                    ),
                    "groundloop_chunk_embedding_pkey",
                    1,
                ),
            )
        )
        tier10_expected.extend(
            (tuple(locator.prompt_row), tuple(locator.chunk_embedding_row))
        )
        artifact_uses = tuple(
            sorted(
                locator.artifact_use_rows,
                key=lambda row: tuple(str(value) for value in row[:3]),
            )
        )
        for use in artifact_uses:
            coordinate = tuple(str(value) for value in use[:3])
            tier10_routes.append(
                _F13Route(
                    f"locked M3 artifact use {coordinate!r}",
                    "M",
                    """SELECT run_id, artifact_kind, artifact_id, reused
                         FROM groundloop_pipeline_artifact_use
                        WHERE run_id = %s AND artifact_kind = %s
                          AND artifact_id = %s
                        FOR UPDATE""",
                    coordinate,
                    "groundloop_pipeline_artifact_use_pkey",
                    1,
                )
            )
            tier10_expected.append(tuple(use))

        tier10_source = "".join(
            function_source("_lock_d29_tier_10_authority").lower().split()
        )
        assert all(
            "".join(route.statement.lower().split()) in tier10_source
            for route in tier10_routes
        )
        with connection.cursor() as raw_cursor:
            tier10_trace = _RecordingCursor(raw_cursor)
            for route, expected in zip(tier10_routes, tier10_expected, strict=True):
                row = tier10_trace.execute(route.statement, route.parameters).fetchone()
                assert row is not None and tuple(row) == tuple(expected)

        image_count = len(tuple(target["image_epoch_ids"]))  # type: ignore[arg-type]
        expected_relation_counts = {
            "groundloop_semantic_observation": 1,
            "groundloop_verification_execution": 1,
            "groundloop_pipeline_run": 1,
            "groundloop_retrieval_candidate": 1,
            "groundloop_epoch": 1,
            "groundloop_model_artifact": 2,
            "groundloop_prompt_artifact": 1,
            "groundloop_chunk_embedding": 1,
            "groundloop_m5_activation": 1,
            "groundloop_m4_update as update_row": image_count,
            "groundloop_pipeline_artifact_use": 6,
        }
        preliminary_entries = trace.entries[:preliminary_count]
        normalized_preliminary = tuple(
            str(entry["sql"]).lower() for entry in preliminary_entries
        )
        for relation, expected_count in expected_relation_counts.items():
            assert sum(
                f"from {relation}" in statement for statement in normalized_preliminary
            ) == (expected_count), relation
        locked_entries = trace.entries[preliminary_count:]
        assert len(locked_entries) == image_count
        assert all(
            "for key share of update_row, snapshot, member, claim_row"
            in str(entry["sql"]).lower()
            for entry in locked_entries
        )

        simple_indexes = {
            "from groundloop_semantic_observation": (
                "groundloop_semantic_observation_pkey",
            ),
            "from groundloop_verification_execution": (
                "groundloop_verification_execution_pkey",
            ),
            "from groundloop_pipeline_run": ("groundloop_pipeline_run_pkey",),
            "from groundloop_retrieval_candidate": (
                "groundloop_retrieval_candidate_pkey",
            ),
            "from groundloop_epoch": ("groundloop_epoch_pkey",),
            "from groundloop_model_artifact": ("groundloop_model_artifact_pkey",),
            "from groundloop_prompt_artifact": ("groundloop_prompt_artifact_pkey",),
            "from groundloop_chunk_embedding": ("groundloop_chunk_embedding_pkey",),
            "from groundloop_pipeline_artifact_use": (
                "groundloop_pipeline_artifact_use_pkey",
            ),
        }
        image_indexes = (
            "groundloop_m4_update_pkey",
            "groundloop_m4_claim_registry_snapshot_pkey",
            "groundloop_m4_claim_registry_member_pkey",
            "groundloop_claim_pkey",
            "groundloop_chunk_version_pkey",
            "groundloop_document_version_pkey",
        )
        plan_nodes: list[tuple[dict[str, object], ...]] = []
        for entry in trace.entries:
            statement = str(entry["sql"]).lower()
            if "from groundloop_m5_activation as activation" in statement:
                nodes = _analyze(
                    connection,
                    str(entry["sql"]),
                    tuple(entry["parameters"]),  # type: ignore[arg-type]
                )
                singleton_relations = frozenset(
                    {
                        "groundloop_m5_activation",
                        "groundloop_m4_publication_head",
                        "groundloop_m5_publication_head",
                    }
                )
                assert int(nodes[0].get("Actual Rows", 0)) == 1
                assert (
                    frozenset(
                        str(node["Relation Name"])
                        for node in nodes
                        if node.get("Relation Name") is not None
                    )
                    == singleton_relations
                )
                assert all(
                    node.get("Node Type") == "Seq Scan"
                    for node in nodes
                    if node.get("Relation Name") in singleton_relations
                )
                assert all(
                    int(node.get("Rows Removed by Filter", 0)) == 0 for node in nodes
                )
                assert all(
                    int(node.get("Rows Removed by Index Recheck", 0)) == 0
                    for node in nodes
                )
            elif "from groundloop_m4_update as update_row" in statement:
                nodes = _assert_captured_default_plan(
                    connection,
                    entry,
                    expected_indexes=image_indexes,
                    expected_rows=1,
                )
            else:
                matches = tuple(
                    indexes
                    for marker, indexes in simple_indexes.items()
                    if marker in statement
                )
                assert len(matches) == 1, statement
                if "from groundloop_semantic_observation" in statement:
                    # Both named indexes are exact UNIQUE observation-id
                    # points; neither alternative is a prefix/range waiver.
                    nodes = _assert_captured_default_plan(
                        connection,
                        entry,
                        expected_any_indexes=(
                            "groundloop_semantic_observation_pkey",
                            "groundloop_semantic_observati_"
                            "observation_id_subject_kind_s_key",
                        ),
                        expected_rows=1,
                    )
                else:
                    nodes = _assert_captured_default_plan(
                        connection,
                        entry,
                        expected_indexes=matches[0],
                        expected_rows=1,
                    )
            plan_nodes.append(nodes)

        for route, entry in zip(tier10_routes, tier10_trace.entries, strict=True):
            expected_indexes = (
                (route.index_name,)
                if isinstance(route.index_name, str)
                else route.index_name
            )
            nodes = _assert_captured_default_plan(
                connection,
                entry,
                expected_indexes=expected_indexes,
                expected_rows=1,
            )
            plan_nodes.append(nodes)

        assert all(entry["returned_rows"] == 1 for entry in trace.entries)
        assert all(entry["returned_rows"] == 1 for entry in tier10_trace.entries)
        assert all(
            "d30-f13-decoy" not in str(parameter)
            for entry in trace.entries
            for parameter in entry["parameters"]  # type: ignore[union-attr]
        )
        trace_text = "\n".join(str(entry["sql"]).lower() for entry in trace.entries)
        for forbidden in (
            "groundloop_impact_channel_hit",
            "pairverificationinput",
            "pairverificationartifact",
            "pairjudgment",
        ):
            assert forbidden not in trace_text
        m_count = (
            1  # produced M3 epoch
            + 1  # verification execution
            + 1  # run
            + 1  # candidate
            + 2  # verifier and embedding models
            + 1  # prompt
            + 1  # chunk embedding
            + 6  # immutable artifact-use points
        )
        assert sum(int(entry["returned_rows"]) for entry in preliminary_entries) == (
            m_count
            + 1  # semantic observation is one selected-claim constant
            + 1  # fixed activation/base/head coordinate query
            + image_count  # activation/base-head image coordinates are constants
        )
        assert (
            sum(int(entry["returned_rows"]) for entry in locked_entries) == image_count
        )
        assert (
            sum(
                route.expected_rows
                for route in tier10_routes
                if route.cardinality_term == "M"
            )
            == m_count - 1
        )  # immutable epoch reread is the M epoch already counted
        # These are the exact private sort inputs used by the production M3
        # tier-10 ordering.  Keep them separate from M and from the fixed
        # observation/activation/image constants above.
        bootstrap_private_sort_inputs = {
            "epoch_points": (int(str(locator.epoch_row[0])),),
            "execution_points": (str(locator.execution_row[0]),),
            "run_points": (str(locator.run_row[0]),),
            "candidate_points": (str(locator.candidate_row[0]),),
            "model_points": tuple(sorted(model_points)),
            "prompt_points": (str(locator.prompt_row[0]),),
            "embedding_points": (
                (
                    currency.chunk_version_id,
                    str(locator.embedding_model_row[0]),
                ),
            ),
            "artifact_use_points": tuple(
                tuple(str(value) for value in row[:3]) for row in artifact_uses
            ),
            "image_epoch_points": tuple(
                sorted(int(str(row[0])) for row in locator.image_rows)
            ),
        }
        assert bootstrap_private_sort_inputs == {
            "epoch_points": (int(str(locator.epoch_row[0])),),
            "execution_points": (currency.observation_id,),
            "run_points": (str(locator.run_row[0]),),
            "candidate_points": (str(locator.candidate_row[0]),),
            "model_points": tuple(
                sorted(
                    {
                        str(locator.model_row[0]),
                        str(locator.embedding_model_row[0]),
                    }
                )
            ),
            "prompt_points": (str(locator.prompt_row[0]),),
            "embedding_points": (
                (
                    currency.chunk_version_id,
                    str(locator.embedding_model_row[0]),
                ),
            ),
            "artifact_use_points": tuple(
                sorted(
                    tuple(str(value) for value in row[:3])
                    for row in locator.artifact_use_rows
                )
            ),
            "image_epoch_points": tuple(
                sorted(int(value) for value in target["image_epoch_ids"])  # type: ignore[arg-type]
            ),
        }
        assert len(plan_nodes) == len(trace.entries) + len(tier10_trace.entries)


def test_predecessor_f13_executes_production_probe_lock_and_guarded_rerun(
    d30_schema: Any,
    function_source: Callable[[str], str],
) -> None:
    """Bind the B route and both authority guards to production SQL bytes."""

    connection = d30_schema.connection
    with _savepoint(connection, "d30_f13_predecessor_sequence"):
        target = _seed_f13_route_rows(d30_schema)
        _inflate_f13_relations(connection)
        currency = postgres_withdrawal._D30CurrencyRow(
            subject_kind="claim",
            subject_id=str(target["claim_id"]),
            chunk_version_id=str(target["chunk_id"]),
            task_type=str(target["history_task"]),
            observation_id=str(target["history_observation"]),
            installed_revision=1,
        )
        predecessor_epoch_id = int(target["history_predecessor_epoch_id"])
        lock_statement = """
            SELECT subject_kind::text, subject_id, chunk_version_id,
                   task_type, observation_id, valid_from_epoch,
                   valid_to_epoch
            FROM groundloop_published_observation_currency
            WHERE subject_kind = %s AND subject_id = %s
              AND chunk_version_id = %s AND task_type = %s
              AND valid_from_epoch = %s
            FOR UPDATE
        """
        normalized_lock = " ".join(lock_statement.split()).lower()
        assert (
            normalized_lock
            in " ".join(
                function_source("_lock_d29_observation_authority").split()
            ).lower()
        )
        with connection.cursor() as raw_cursor:
            trace = _RecordingCursor(raw_cursor)
            preliminary = postgres_withdrawal._probe_d30_predecessor_currency(
                trace,  # type: ignore[arg-type]
                currency,
                predecessor_epoch_id,
            )
            assert preliminary is not None
            locked = trace.execute(
                lock_statement,
                (*currency.full_key, int(str(preliminary[5]))),
            ).fetchone()
            rerun = postgres_withdrawal._probe_d30_predecessor_currency(
                trace,  # type: ignore[arg-type]
                currency,
                predecessor_epoch_id,
            )
        assert preliminary == locked == rerun
        assert len(trace.entries) == 3
        assert trace.entries[0]["sql"] == trace.entries[2]["sql"]
        assert trace.entries[0]["parameters"] == trace.entries[2]["parameters"]
        assert "for update" not in str(trace.entries[0]["sql"]).lower()
        assert "for update" in str(trace.entries[1]["sql"]).lower()
        assert tuple(entry["returned_rows"] for entry in trace.entries) == (1, 1, 1)

        planned: list[tuple[dict[str, object], ...]] = []
        for ordinal, entry in enumerate(trace.entries):
            nodes = _assert_captured_default_plan(
                connection,
                entry,
                expected_indexes=("groundloop_published_observation_currency_pkey",),
                expected_rows=1,
            )
            pkey_nodes = tuple(
                node
                for node in nodes
                if node.get("Index Name")
                == "groundloop_published_observation_currency_pkey"
            )
            assert len(pkey_nodes) == 1
            if ordinal in (0, 2):
                assert pkey_nodes[0].get("Scan Direction") == "Backward"
            assert int(nodes[0].get("Actual Rows", 0)) <= 1
            planned.append(nodes)
        assert len(planned) == 3
        assert sum(int(entry["returned_rows"]) for entry in trace.entries[::2]) == 2
        assert int(trace.entries[1]["returned_rows"]) == 1


def test_typed_d24_f13_executes_exact_production_locator_trace(
    d30_schema: Any,
) -> None:
    """Execute every conditional D24 locator route on one physical owner."""

    connection = d30_schema.connection
    with _savepoint(connection, "d30_f13_typed_d24_trace"):
        target = _seed_d24_physical_owner(d30_schema)
        _inflate_d24_physical_routes(connection, epoch_id=int(target["epoch_id"]))
        with connection.cursor() as raw_cursor:
            trace = _RecordingCursor(raw_cursor)
            owner = postgres_withdrawal._d30_owner_locator(  # type: ignore[arg-type]
                trace, int(target["epoch_id"])
            )
        assert owner.d24 is not None
        assert len(owner.jobs) == 3
        assert sum(len(rows) for _, rows in owner.attempts) == 2
        assert len(owner.d24.attempts) == 2

        typed_claims = postgres_withdrawal._D30ClaimLocatorAuthority(
            currency_rows=(),
            dynamic=(),
            bootstrap=(),
            owners=(owner,),
            activation_row=None,
        )
        typed_locator = postgres_withdrawal._D29LocatorAuthority(
            predecessor_snapshots=None,
            admitted_locator_keys=(),
            qualifying_admitted_pair_digests=(),
            candidate_root_job_ids=(),
            candidate_root_job_coordinates=(),
            candidate_dependency_coordinates=(),
            direct_job_ids=(),
            direct_job_coordinates=(),
            direct_dependency_coordinates=(),
            direct_admitted_pair_ids=(),
            direct_scope_root_job_ids=(),
            direct_verifier_observation_ids=(),
            verifier_job_ids=(),
            verifier_root_job_ids=(),
            verifier_observation_ids=(),
            verifier_dependency_coordinates=(),
            verifier_artifact_ids=(),
            verifier_pair_input_hashes=(),
            bootstrap_observation_coordinates=(),
            direct_bootstrap_observation_coordinates=(),
            requirement_currency_keys=(),
            direct_currency_keys=(),
            direct_frontier_keys=(),
            touched_requirement_ids=(),
            prospective_coordinates=(
                postgres_withdrawal._DocumentDeclarationCoordinates(
                    direct_scope_root_job_ids=(),
                    requirement_scope_root_job_ids=(),
                    direct_job_ids=(),
                    requirement_job_ids=(),
                )
            ),
            d30_claims=typed_claims,
        )
        with connection.cursor() as raw_cursor:
            typed_lock_trace = _RecordingCursor(raw_cursor)
            postgres_withdrawal._lock_d30_typed_owner_evidence(
                typed_lock_trace,  # type: ignore[arg-type]
                typed_locator,
            )
        assert all(
            entry["returned_rows"] is not None for entry in typed_lock_trace.entries
        )

        statements = tuple(str(entry["sql"]).lower() for entry in trace.entries)

        def route_count(marker: str) -> int:
            return sum(marker in statement for statement in statements)

        # Every production statement is assigned to exactly one route family.
        # The tuple records executable call count, returned cardinalities, the
        # F13 term, and the only relations that the physical plan may touch.
        route_manifest: tuple[
            tuple[
                str,
                str,
                int,
                tuple[int, ...],
                str,
                frozenset[str],
            ],
            ...,
        ] = (
            (
                "owner epoch point",
                "from groundloop_epoch where",
                1,
                (1,),
                "constant",
                frozenset({"groundloop_epoch"}),
            ),
            (
                "owner M4 update point",
                "from groundloop_m4_update where",
                1,
                (1,),
                "constant",
                frozenset({"groundloop_m4_update"}),
            ),
            (
                "owner job range",
                "from groundloop_semantic_job where",
                1,
                (3,),
                "J_e",
                frozenset({"groundloop_semantic_job"}),
            ),
            (
                "owner dependency range",
                "from groundloop_semantic_job_dependency where",
                1,
                (0,),
                "D_e",
                frozenset({"groundloop_semantic_job_dependency"}),
            ),
            (
                "owner scope points",
                "from groundloop_discovery_scope where",
                3,
                (0, 0, 0),
                "S_e",
                frozenset({"groundloop_discovery_scope"}),
            ),
            (
                "owner attempt prefixes",
                "from groundloop_semantic_job_attempt where",
                3,
                (1, 0, 1),
                "A_e",
                frozenset({"groundloop_semantic_job_attempt"}),
            ),
            (
                "owner discovery-result points",
                "from groundloop_m4_discovery_result where",
                3,
                (0, 0, 0),
                "O_e",
                frozenset({"groundloop_m4_discovery_result"}),
            ),
            (
                "owner projection points",
                "from groundloop_m5_direct_terminal_projection where",
                3,
                (1, 0, 0),
                "O_e",
                frozenset({"groundloop_m5_direct_terminal_projection"}),
            ),
            (
                "typed runtime epoch point",
                "from groundloop_m5_runtime_epoch",
                1,
                (1,),
                "constant",
                frozenset({"groundloop_m5_runtime_epoch"}),
            ),
            (
                "dispatch points",
                "from groundloop_m5_dispatch_record",
                2,
                (1, 1),
                "O_e",
                frozenset({"groundloop_m5_dispatch_record"}),
            ),
            (
                "execution-evidence points",
                "from groundloop_m5_attempt_execution_evidence",
                2,
                (1, 1),
                "O_e",
                frozenset({"groundloop_m5_attempt_execution_evidence"}),
            ),
            (
                "attempt-timing points",
                "from groundloop_m5_runtime_timing_contribution",
                2,
                (1, 1),
                "O_e",
                frozenset({"groundloop_m5_runtime_timing_contribution"}),
            ),
            (
                "late-envelope points",
                "from groundloop_m5_typed_direct_late_return_envelope",
                2,
                (1, 0),
                "O_e",
                frozenset({"groundloop_m5_typed_direct_late_return_envelope"}),
            ),
            (
                "expired-return points",
                "from groundloop_m5_expired_attempt_return",
                2,
                (1, 0),
                "O_e",
                frozenset({"groundloop_m5_expired_attempt_return"}),
            ),
            (
                "postterminal-timing points",
                "from groundloop_m5_post_terminal_attempt_timing",
                2,
                (1, 0),
                "O_e",
                frozenset({"groundloop_m5_post_terminal_attempt_timing"}),
            ),
            (
                "postterminal-audit points",
                "from groundloop_m5_post_terminal_attempt_audit",
                2,
                (1, 0),
                "O_e",
                frozenset({"groundloop_m5_post_terminal_attempt_audit"}),
            ),
            (
                "work-contribution points",
                "from groundloop_m5_runtime_work_contribution",
                8,
                (1, 1, 0, 1, 1, 1, 1, 1),
                "O_e",
                frozenset({"groundloop_m5_runtime_work_contribution"}),
            ),
            (
                "transition-call-timing points",
                "from groundloop_m5_transition_call_timing",
                4,
                (1, 1, 1, 1),
                "O_e",
                frozenset({"groundloop_m5_transition_call_timing"}),
            ),
            (
                "M4 counter-transition point",
                "from groundloop_m4_evaluation_counter_transition",
                1,
                (1,),
                "O_e",
                frozenset({"groundloop_m4_evaluation_counter_transition"}),
            ),
            (
                "work accumulator point",
                "from groundloop_m5_runtime_work_accumulator",
                1,
                (1,),
                "constant",
                frozenset({"groundloop_m5_runtime_work_accumulator"}),
            ),
            (
                "timing accumulator point",
                "from groundloop_m5_runtime_timing_accumulator",
                1,
                (1,),
                "constant",
                frozenset({"groundloop_m5_runtime_timing_accumulator"}),
            ),
            (
                "terminal event-result point",
                "from groundloop_m5_event_result where",
                1,
                (1,),
                "constant",
                frozenset({"groundloop_m5_event_result"}),
            ),
            (
                "terminal work range and exact points",
                "from groundloop_m5_runtime_work where",
                4,
                (2, 1, 1, 1),
                "O_e",
                frozenset({"groundloop_m5_runtime_work"}),
            ),
            (
                "event timing-coverage points",
                "from groundloop_m5_event_timing_coverage",
                2,
                (1, 1),
                "constant",
                frozenset({"groundloop_m5_event_timing_coverage"}),
            ),
            (
                "event-result delta ranges",
                "from groundloop_m5_event_result_delta",
                2,
                (0, 0),
                "O_e",
                frozenset({"groundloop_m5_event_result_delta"}),
            ),
            (
                "event-result reference ranges",
                "from groundloop_m5_event_result_state_reference",
                2,
                (0, 0),
                "O_e",
                frozenset({"groundloop_m5_event_result_state_reference"}),
            ),
            (
                "canonical terminal projection",
                "from groundloop_epoch as base",
                1,
                (1,),
                "constant",
                frozenset(
                    {
                        "groundloop_epoch",
                        "groundloop_m5_runtime_epoch",
                        "groundloop_m5_event_result",
                    }
                ),
            ),
        )
        typed_index_sequences: dict[str, tuple[tuple[str, ...], ...]] = {
            "owner epoch point": (("groundloop_epoch_pkey",),),
            "owner M4 update point": (("groundloop_m4_update_pkey",),),
            "owner job range": (("groundloop_m4_job_by_epoch",),),
            "owner dependency range": (("groundloop_semantic_job_dependency_pkey",),),
            "owner scope points": 3 * (("groundloop_discovery_scope_pkey",),),
            "owner attempt prefixes": 3
            * (("groundloop_semantic_job_attempt_job_id_attempt_ordinal_key",),),
            "owner discovery-result points": 3
            * (("groundloop_m4_discovery_result_pkey",),),
            "owner projection points": 3
            * (("groundloop_m5_direct_terminal_projection_pkey",),),
            # The predicate is the exact epoch PK coordinate.  PostgreSQL may
            # choose this named UNIQUE (epoch_id, revision) index because the
            # table PK independently makes its epoch_id prefix singleton.
            "typed runtime epoch point": (
                ("groundloop_m5_runtime_epoch_epoch_id_revision_key",),
            ),
            "dispatch points": 2
            * (("groundloop_m5_dispatch_record_epoch_id_subgraph_attempt_id_key",),),
            "execution-evidence points": 2
            * (("groundloop_m5_attempt_executi_epoch_id_subgraph_attempt_id_key2",),),
            "attempt-timing points": 2
            * (("groundloop_m5_runtime_timing_contribution_pkey",),),
            "late-envelope points": 2
            * (("groundloop_m5_typed_direct_late_return_envelope_pkey",),),
            "expired-return points": 2
            * (("groundloop_m5_expired_attempt_return_pkey",),),
            "postterminal-timing points": 2
            * (("groundloop_m5_post_terminal_attempt_timing_pkey",),),
            "postterminal-audit points": 2
            * (("groundloop_m5_post_terminal_attempt_audit_pkey",),),
            "work-contribution points": 8
            * (("groundloop_m5_runtime_work_co_epoch_id_contribution_kind_so_key",),),
            "transition-call-timing points": 4
            * (("groundloop_m5_transition_call_timing_pkey",),),
            "M4 counter-transition point": (
                ("groundloop_m4_evaluation_counter_transition_pkey",),
            ),
            "work accumulator point": (
                ("groundloop_m5_runtime_work_accumulator_pkey",),
            ),
            "timing accumulator point": (
                ("groundloop_m5_runtime_timing_accumulator_pkey",),
            ),
            "terminal event-result point": (
                ("groundloop_m5_event_result_epoch_id_key",),
            ),
            "terminal work range and exact points": (
                ("groundloop_m5_runtime_work_epoch_id_work_kind_key",),
                ("groundloop_m5_runtime_work_pkey",),
                ("groundloop_m5_runtime_work_by_event",),
                ("groundloop_m5_runtime_work_by_event",),
            ),
            "event timing-coverage points": 2
            * (("groundloop_m5_event_timing_coverage_epoch_id_key",),),
            "event-result delta ranges": 2
            * (("groundloop_m5_event_result_delta_pkey",),),
            "event-result reference ranges": 2
            * (("groundloop_m5_event_result_state_reference_pkey",),),
            "canonical terminal projection": (
                (
                    "groundloop_epoch_event_id_key",
                    "groundloop_m5_runtime_epoch_pkey",
                    "groundloop_m5_event_result_epoch_id_key",
                ),
            ),
        }
        assert set(typed_index_sequences) == {route[0] for route in route_manifest}
        assert all(
            len(typed_index_sequences[label]) == expected_calls
            for label, _marker, expected_calls, *_rest in route_manifest
        )
        assert sum(route[2] for route in route_manifest) == len(trace.entries)
        for (
            label,
            marker,
            expected_calls,
            expected_rows,
            _term,
            _relations,
        ) in route_manifest:
            matching_entries = tuple(
                entry
                for entry, statement in zip(trace.entries, statements, strict=True)
                if marker in statement
            )
            assert route_count(marker) == expected_calls, label
            assert tuple(entry["returned_rows"] for entry in matching_entries) == (
                expected_rows
            ), label
        for statement in statements:
            matched_labels = tuple(
                label for label, marker, *_rest in route_manifest if marker in statement
            )
            assert len(matched_labels) == 1, (statement, matched_labels)

        plan_signatures: dict[str, list[tuple[str, ...]]] = {}
        plan_occurrences = {label: 0 for label in typed_index_sequences}
        for entry, statement in zip(trace.entries, statements, strict=True):
            route = next(route for route in route_manifest if route[1] in statement)
            label, _marker, _calls, _rows, _term, expected_relations = route
            expected_rows = int(entry["returned_rows"])
            occurrence = plan_occurrences[label]
            expected_indexes = typed_index_sequences[label][occurrence]
            plan_occurrences[label] += 1
            nodes = _assert_captured_default_plan(
                connection,
                entry,
                expected_indexes=expected_indexes,
                expected_rows=expected_rows,
                maximum_index_rows=(
                    None
                    if label == "canonical terminal projection"
                    else max(1, expected_rows)
                ),
            )
            relation_names = frozenset(
                str(node["Relation Name"])
                for node in nodes
                if node.get("Relation Name") is not None
            )
            assert relation_names == expected_relations, (
                label,
                relation_names,
                expected_relations,
            )
            index_names = tuple(
                sorted(
                    str(node["Index Name"])
                    for node in nodes
                    if node.get("Index Name") is not None
                )
            )
            assert index_names == tuple(sorted(expected_indexes)), (
                label,
                statement,
                index_names,
                expected_indexes,
            )
            plan_signatures.setdefault(label, []).append(index_names)
        assert set(plan_signatures) == {route[0] for route in route_manifest}
        assert plan_occurrences == {
            label: len(indexes) for label, indexes in typed_index_sequences.items()
        }

        typed_returned_ledger = {
            term: sum(
                int(entry["returned_rows"])
                for route in route_manifest
                if route[4] == term
                for entry, statement in zip(trace.entries, statements, strict=True)
                if route[1] in statement
            )
            for term in ("J_e", "D_e", "S_e", "A_e", "O_e")
        }
        assert typed_returned_ledger == {
            "J_e": 3,
            "D_e": 0,
            "S_e": 0,
            "A_e": 2,
            "O_e": 28,
        }
        typed_query_coordinate_ledger = {
            term: sum(route[2] for route in route_manifest if route[4] == term)
            for term in ("J_e", "D_e", "S_e", "A_e", "O_e")
        }
        assert typed_query_coordinate_ledger == {
            "J_e": 1,
            "D_e": 1,
            "S_e": 3,
            "A_e": 3,
            "O_e": 41,
        }

        typed_lock_expected_rows = {
            "dispatch points": (1, 1),
            "execution-evidence points": (1, 1),
            "attempt-timing points": (1, 1),
            "late-envelope points": (1, 0),
            "expired-return points": (1, 0),
            "postterminal-timing points": (1, 0),
            "postterminal-audit points": (1, 0),
            "work-contribution points": (1, 1, 1, 1, 1, 1, 1),
            "transition-call-timing points": (1, 1, 1, 1),
            "M4 counter-transition point": (1,),
            "work accumulator point": (1,),
            "timing accumulator point": (1,),
            "terminal event-result point": (1,),
            "terminal work range and exact points": (2, 1, 1, 1),
            "event timing-coverage points": (1, 1),
            "event-result delta ranges": (0, 0),
            "event-result reference ranges": (0, 0),
            "canonical terminal projection": (1,),
        }
        typed_lock_index_sequences = {
            label: typed_index_sequences[label] for label in typed_lock_expected_rows
        }
        typed_lock_index_sequences["work-contribution points"] = 7 * (
            ("groundloop_m5_runtime_work_co_epoch_id_contribution_kind_so_key",),
        )
        typed_lock_index_sequences["terminal work range and exact points"] = (
            ("groundloop_m5_runtime_work_epoch_id_work_kind_key",),
            ("groundloop_m5_runtime_work_pkey",),
            ("groundloop_m5_runtime_work_by_event",),
            ("groundloop_m5_runtime_work_by_event",),
        )
        runtime_work_index_shapes = {
            str(row[0]): (bool(row[1]), tuple(str(value) for value in row[2]))
            for row in connection.execute(
                """
                SELECT index_relation.relname, definition.indisunique,
                       array_agg(attribute.attname ORDER BY key.ordinality)
                FROM pg_index AS definition
                JOIN pg_class AS table_relation
                  ON table_relation.oid = definition.indrelid
                JOIN pg_namespace AS namespace
                  ON namespace.oid = table_relation.relnamespace
                JOIN pg_class AS index_relation
                  ON index_relation.oid = definition.indexrelid
                JOIN LATERAL unnest(definition.indkey)
                     WITH ORDINALITY AS key(attnum, ordinality)
                  ON key.ordinality <= definition.indnkeyatts
                JOIN pg_attribute AS attribute
                  ON attribute.attrelid = table_relation.oid
                 AND attribute.attnum = key.attnum
                WHERE namespace.nspname = current_schema()
                  AND table_relation.relname = 'groundloop_m5_runtime_work'
                  AND index_relation.relname IN (
                      'groundloop_m5_runtime_work_pkey',
                      'groundloop_m5_runtime_work_by_event'
                  )
                GROUP BY index_relation.relname, definition.indisunique
                """
            ).fetchall()
        }
        assert runtime_work_index_shapes == {
            "groundloop_m5_runtime_work_pkey": (
                True,
                ("structural_event_id", "work_kind"),
            ),
            # This index is not claimed unique.  It is an exact logical-PK
            # point because its leading columns are the complete table PK.
            "groundloop_m5_runtime_work_by_event": (
                False,
                ("structural_event_id", "work_kind", "work_digest"),
            ),
        }
        lock_statements = tuple(
            str(entry["sql"]).lower() for entry in typed_lock_trace.entries
        )
        lock_entries_by_label: dict[str, list[dict[str, object]]] = {
            label: [] for label in typed_lock_expected_rows
        }
        lock_occurrences = {label: 0 for label in typed_lock_expected_rows}
        for entry, statement in zip(
            typed_lock_trace.entries, lock_statements, strict=True
        ):
            matching_routes = tuple(
                route for route in route_manifest if route[1] in statement
            )
            assert len(matching_routes) == 1, statement
            route = matching_routes[0]
            label = route[0]
            assert label in typed_lock_expected_rows, (label, statement)
            lock_entries_by_label[label].append(entry)
            occurrence = lock_occurrences[label]
            expected_indexes = typed_lock_index_sequences[label][occurrence]
            lock_occurrences[label] += 1
            expected_rows = int(entry["returned_rows"])
            nodes = _assert_captured_default_plan(
                connection,
                entry,
                expected_indexes=expected_indexes,
                expected_rows=expected_rows,
                maximum_index_rows=(
                    None
                    if label == "canonical terminal projection"
                    else max(1, expected_rows)
                ),
            )
            relation_names = frozenset(
                str(node["Relation Name"])
                for node in nodes
                if node.get("Relation Name") is not None
            )
            assert relation_names == route[5], (label, relation_names, route[5])
            index_names = tuple(
                sorted(
                    str(node["Index Name"])
                    for node in nodes
                    if node.get("Index Name") is not None
                )
            )
            assert index_names == tuple(sorted(expected_indexes)), (
                label,
                statement,
                index_names,
                expected_indexes,
            )
        assert len(typed_lock_trace.entries) == 40
        assert lock_occurrences == {
            label: len(indexes) for label, indexes in typed_lock_index_sequences.items()
        }
        assert {
            label: tuple(entry["returned_rows"] for entry in entries)
            for label, entries in lock_entries_by_label.items()
        } == typed_lock_expected_rows

        # The production Python sorts expose these exact private inputs; the
        # values are neither SQL call counts nor returned-row formula terms.
        assert {
            "typed_owner_sort": 1,
            "attempt_locator_sort": len(owner.d24.attempts),
            "contribution_key_sort": len(
                lock_entries_by_label["work-contribution points"]
            ),
            "timing_key_sort": len(
                lock_entries_by_label["transition-call-timing points"]
            ),
            "transition_key_sort": len(
                lock_entries_by_label["M4 counter-transition point"]
            ),
        } == {
            "typed_owner_sort": 1,
            "attempt_locator_sort": 2,
            "contribution_key_sort": 7,
            "timing_key_sort": 4,
            "transition_key_sort": 1,
        }
        assert all(entry["returned_rows"] is not None for entry in trace.entries)
        trace_text = "\n".join(statements)
        for forbidden in (
            "pairverificationinput",
            "pairverificationartifact",
            "pairjudgment",
            "migration_019",
        ):
            assert forbidden not in trace_text


def test_requirement_bootstrap_rejects_typed_rootless_provenance(
    d30_schema: Any,
) -> None:
    """Keep the retained requirement bootstrap path fail-closed on typed roots."""

    connection = d30_schema.connection
    with _savepoint(connection, "d30_typed_rootless_bootstrap"):
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.execute("SET CONSTRAINTS ALL DEFERRED")
        requirement = connection.execute(
            """
            SELECT currency.subject_id, currency.chunk_version_id,
                   currency.task_type, currency.observation_id,
                   observation.produced_epoch, activation.base_m4_epoch_id
            FROM groundloop_observation_currency AS currency
            JOIN groundloop_semantic_observation AS observation
              ON observation.observation_id = currency.observation_id
            JOIN groundloop_m5_activation AS activation ON activation.singleton
            WHERE currency.subject_kind = 'requirement'
              AND observation.produced_epoch <= activation.base_m4_epoch_id
              AND NOT EXISTS (
                    SELECT 1 FROM groundloop_m5_runtime_epoch
                    WHERE epoch_id = observation.produced_epoch
              )
              AND NOT EXISTS (
                    SELECT 1 FROM groundloop_m5_update
                    WHERE epoch_id = observation.produced_epoch
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM groundloop_m5_requirement_verifier_execution
                    WHERE observation_id = currency.observation_id
              )
              AND NOT EXISTS (
                    SELECT 1 FROM groundloop_m4_verification_execution
                    WHERE observation_id = currency.observation_id
              )
            ORDER BY currency.observation_id COLLATE "C"
            LIMIT 1
            """
        ).fetchone()
        assert requirement is not None
        produced_epoch_id = int(requirement[4])
        base_epoch_id = int(requirement[5])
        previous = connection.execute(
            "SELECT epoch_id FROM groundloop_epoch WHERE epoch_id < %s "
            "ORDER BY epoch_id DESC LIMIT 1",
            (produced_epoch_id,),
        ).fetchone()
        if previous is None:
            previous = connection.execute(
                """
                INSERT INTO groundloop_epoch (
                    epoch_id, event_id, payload_hash, revision,
                    structural_status, semantic_status, evaluation_state,
                    publication_mode, sealed_at
                ) OVERRIDING SYSTEM VALUE
                VALUES (%s, 'd30-typed-rootless-prior', %s, 0,
                        'committed', 'sealed', 'complete', 'strict', now())
                RETURNING epoch_id
                """,
                (produced_epoch_id - 1, "e" * 64),
            ).fetchone()
        policy = connection.execute(
            "SELECT policy_version FROM groundloop_decision_policy "
            'ORDER BY policy_version COLLATE "C" LIMIT 1'
        ).fetchone()
        assert previous is not None and policy is not None
        previous_epoch_id = int(previous[0])
        assert previous_epoch_id < produced_epoch_id <= base_epoch_id
        locator = SimpleNamespace(
            bootstrap_observation_coordinates=(
                (str(requirement[3]), produced_epoch_id),
            ),
            requirement_currency_keys=(
                (
                    str(requirement[0]),
                    str(requirement[1]),
                    str(requirement[2]),
                    str(requirement[3]),
                ),
            ),
        )

        with connection.cursor() as cursor:
            baseline = postgres_withdrawal._lock_d29_bootstrap_provenance(  # type: ignore[arg-type]
                cursor,
                locator,
                predecessor_epoch_id=base_epoch_id,
            )
        assert set(baseline) == {str(requirement[3])}

        connection.execute(
            "ALTER TABLE groundloop_m5_update "
            "DISABLE TRIGGER groundloop_m5_update_runtime_mode_guard"
        )
        try:
            connection.execute(
                """
                INSERT INTO groundloop_m5_update (
                    epoch_id, update_kind, previous_published_epoch_id,
                    decision_policy_version, manifest
                ) VALUES (%s, 'observe_requirement', %s, %s, '{}'::jsonb)
                """,
                (produced_epoch_id, previous_epoch_id, str(policy[0])),
            )
        finally:
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
            connection.execute(
                "ALTER TABLE groundloop_m5_update "
                "ENABLE TRIGGER groundloop_m5_update_runtime_mode_guard"
            )
            connection.execute("SET CONSTRAINTS ALL DEFERRED")

        typed_bits = connection.execute(
            """
            SELECT EXISTS (
                       SELECT 1 FROM groundloop_m5_runtime_epoch
                       WHERE epoch_id = %s
                   ), EXISTS (
                       SELECT 1 FROM groundloop_m5_update WHERE epoch_id = %s
                   ), EXISTS (
                       SELECT 1
                       FROM groundloop_m5_requirement_verifier_execution
                       WHERE observation_id = %s
                   ), EXISTS (
                       SELECT 1 FROM groundloop_m4_verification_execution
                       WHERE observation_id = %s
                   )
            """,
            (
                produced_epoch_id,
                produced_epoch_id,
                str(requirement[3]),
                str(requirement[3]),
            ),
        ).fetchone()
        assert typed_bits == (False, True, False, False)
        with connection.cursor() as cursor:
            with pytest.raises(
                EventConflictError,
                match="typed or post-activation provenance",
            ):
                postgres_withdrawal._lock_d29_bootstrap_provenance(  # type: ignore[arg-type]
                    cursor,
                    locator,
                    predecessor_epoch_id=base_epoch_id,
                )


def test_predecessor_publication_race_serializes_in_both_orders(
    d30_schema: Any,
) -> None:
    setup = d30_schema.connection
    current = setup.execute(
        """SELECT subject_kind::text,subject_id,chunk_version_id,task_type,
                  observation_id,valid_from_epoch
             FROM groundloop_published_observation_currency
            WHERE valid_to_epoch IS NULL
            ORDER BY subject_kind::text COLLATE "C",subject_id COLLATE "C",
                     chunk_version_id COLLATE "C",task_type COLLATE "C"
            LIMIT 1"""
    ).fetchone()
    assert current is not None
    base_epoch = int(current[5])
    later = setup.execute(
        "SELECT max(epoch_id) FROM groundloop_epoch WHERE epoch_id > %s",
        (base_epoch,),
    ).fetchone()
    assert later is not None and later[0] is not None
    later_epoch = int(later[0])
    subject_kind = str(current[0])
    subject_id = str(current[1])
    chunk_id = str(current[2])
    task = str(current[3])
    observation_id = str(current[4])
    currency = postgres_withdrawal._D30CurrencyRow(
        subject_kind,
        subject_id,
        chunk_id,
        task,
        observation_id,
        base_epoch,
    )
    setup.commit()

    update_sql = """UPDATE groundloop_published_observation_currency
                       SET valid_to_epoch=%s
                     WHERE subject_kind=%s AND subject_id=%s
                       AND chunk_version_id=%s AND task_type=%s
                       AND valid_from_epoch=%s"""
    update_parameters = (
        later_epoch,
        subject_kind,
        subject_id,
        chunk_id,
        task,
        base_epoch,
    )
    try:
        with (
            _reconnect(d30_schema) as first,
            _reconnect(d30_schema) as second,
        ):
            preliminary = postgres_withdrawal._probe_d30_predecessor_currency(
                first,
                currency,
                base_epoch,
            )
            locked = postgres_withdrawal._probe_d30_predecessor_currency(
                first,
                currency,
                base_epoch,
                lock_authority=True,
            )
            assert locked == preliminary
            started = Event()

            def close_interval() -> int:
                started.set()
                result = second.execute(update_sql, update_parameters)
                return int(result.rowcount or 0)

            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(close_interval)
                assert started.wait(timeout=2)
                with pytest.raises(FutureTimeoutError):
                    future.result(timeout=0.2)
                first.commit()
                assert future.result(timeout=5) == 1
            second.rollback()

            preliminary = postgres_withdrawal._probe_d30_predecessor_currency(
                first,
                currency,
                base_epoch,
            )
            assert second.execute(update_sql, update_parameters).rowcount == 1
            second.commit()
            changed = postgres_withdrawal._probe_d30_predecessor_currency(
                first,
                currency,
                base_epoch,
                lock_authority=True,
            )
            assert preliminary is not None and changed is not None
            assert changed != preliminary
            assert int(changed[6]) == later_epoch
            first.rollback()
    finally:
        setup.execute(
            """UPDATE groundloop_published_observation_currency
                  SET valid_to_epoch=NULL
                WHERE subject_kind=%s AND subject_id=%s
                   AND chunk_version_id=%s AND task_type=%s""",
            (subject_kind, subject_id, chunk_id, task),
        )
        setup.commit()


def _exercise_point_race(
    d30_schema: Any,
    *,
    select_sql: str,
    select_parameters: tuple[object, ...],
    update_sql: str,
    update_parameters: tuple[object, ...],
    restore_sql: str,
    restore_parameters: tuple[object, ...],
) -> None:
    setup = d30_schema.connection
    setup.commit()
    try:
        with _reconnect(d30_schema) as first, _reconnect(d30_schema) as second:
            preliminary = first.execute(select_sql, select_parameters).fetchone()
            assert preliminary is not None
            locked = first.execute(
                select_sql + " FOR UPDATE", select_parameters
            ).fetchone()
            assert locked == preliminary
            started = Event()

            def mutate() -> int:
                started.set()
                result = second.execute(update_sql, update_parameters)
                return int(result.rowcount or 0)

            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(mutate)
                assert started.wait(timeout=2)
                with pytest.raises(FutureTimeoutError):
                    future.result(timeout=0.5)
                first.commit()
                assert future.result(timeout=5) == 1
            second.rollback()

            preliminary = first.execute(select_sql, select_parameters).fetchone()
            assert preliminary is not None
            assert second.execute(update_sql, update_parameters).rowcount == 1
            second.commit()
            changed = first.execute(
                select_sql + " FOR UPDATE", select_parameters
            ).fetchone()
            assert changed is not None and changed != preliminary
            first.rollback()
    finally:
        setup.execute(restore_sql, restore_parameters)
        setup.commit()


def test_job_range_race_detects_phantom_in_both_orders(
    d30_schema: Any,
    function_source: Callable[[str], str],
) -> None:
    """Exercise the production JOB point-lock/rerange order with raw DML.

    The insert is deliberately an adversarial low-level writer, not evidence
    for a conforming public writer.  A planner-first committed phantom must be
    rejected by the guarded rerange; a writer-first row may be accepted only
    when the preliminary bytes, point locks, and rerange are identical.
    """

    setup = d30_schema.connection
    epoch_id = int(d30_schema.first_m4["epoch_id"])
    source_job_id = str(d30_schema.first_m4["root_job_id"])
    range_sql = """
        SELECT job_id, epoch_id, parent_job_id, job_kind,
               candidate_policy_id, payload_hash, execution_spec_hash,
               claim_id, chunk_version_id, expandable, job_state,
               child_closed, child_set_hash, completion_digest,
               result_artifact_id, result_artifact_hash,
               created_revision, completed_revision, created_at, completed_at
        FROM groundloop_semantic_job
        WHERE epoch_id = %s
        ORDER BY job_id COLLATE "C"
    """
    point_lock_sql = """
        SELECT job_id, epoch_id, parent_job_id, job_kind,
               candidate_policy_id, payload_hash, execution_spec_hash,
               claim_id, chunk_version_id, expandable, job_state,
               child_closed, child_set_hash, completion_digest,
               result_artifact_id, result_artifact_hash,
               created_revision, completed_revision, created_at, completed_at
        FROM groundloop_semantic_job
        WHERE job_id = %s
        FOR UPDATE
    """
    adversarial_insert_sql = """
        INSERT INTO groundloop_semantic_job (
            job_id, epoch_id, parent_job_id, job_kind,
            candidate_policy_id, payload_hash, execution_spec_hash,
            claim_id, chunk_version_id, expandable, job_state,
            child_closed, child_set_hash, completion_digest,
            result_artifact_id, result_artifact_hash,
            created_revision, completed_revision, created_at, completed_at
        )
        SELECT %s, epoch_id, parent_job_id, job_kind,
               candidate_policy_id, payload_hash, execution_spec_hash,
               claim_id, chunk_version_id, expandable, job_state,
               child_closed, child_set_hash, completion_digest,
               result_artifact_id, result_artifact_hash,
               created_revision, completed_revision, created_at, completed_at
        FROM groundloop_semantic_job
        WHERE job_id = %s
    """

    def compact(value: str) -> str:
        return "".join(value.lower().split())

    locator_source = compact(function_source("_d30_owner_locator"))
    lock_source = compact(function_source("_lock_d29_jobs_and_reserve"))
    assert compact(range_sql) in locator_source
    assert compact(range_sql) in lock_source
    assert compact(point_lock_sql) in lock_source
    assert lock_source.index(compact(point_lock_sql)) < lock_source.index(
        compact(range_sql)
    )

    job_ids = {
        "planner-first": "d30-job-range-race-planner-first",
        "writer-first": "d30-job-range-race-writer-first",
    }
    setup.commit()
    try:
        for order, inserted_job_id in job_ids.items():
            with _reconnect(d30_schema) as planner, _reconnect(d30_schema) as writer:
                chronology: list[str] = []
                if order == "writer-first":
                    assert (
                        writer.execute(
                            adversarial_insert_sql,
                            (inserted_job_id, source_job_id),
                        ).rowcount
                        == 1
                    )
                    writer.commit()
                    chronology.append("adversarial insert")

                preliminary = tuple(
                    tuple(row)
                    for row in planner.execute(range_sql, (epoch_id,)).fetchall()
                )
                chronology.append("preliminary range")
                assert preliminary
                locked = tuple(
                    tuple(
                        planner.execute(point_lock_sql, (str(row[0]),)).fetchone() or ()
                    )
                    for row in preliminary
                )
                chronology.append("point locks")
                assert locked == preliminary

                if order == "planner-first":
                    assert inserted_job_id not in {str(row[0]) for row in preliminary}
                    assert (
                        writer.execute(
                            adversarial_insert_sql,
                            (inserted_job_id, source_job_id),
                        ).rowcount
                        == 1
                    )
                    writer.commit()
                    chronology.append("adversarial insert")

                rerun = tuple(
                    tuple(row)
                    for row in planner.execute(range_sql, (epoch_id,)).fetchall()
                )
                chronology.append("guarded rerange")
                if order == "planner-first":
                    assert rerun != preliminary
                    assert inserted_job_id in {str(row[0]) for row in rerun}
                    chronology.append("conflict")
                    assert chronology == [
                        "preliminary range",
                        "point locks",
                        "adversarial insert",
                        "guarded rerange",
                        "conflict",
                    ]
                else:
                    assert rerun == preliminary
                    assert inserted_job_id in {str(row[0]) for row in rerun}
                    chronology.append("stable")
                    assert chronology == [
                        "adversarial insert",
                        "preliminary range",
                        "point locks",
                        "guarded rerange",
                        "stable",
                    ]
                planner.rollback()
            setup.execute(
                "DELETE FROM groundloop_semantic_job WHERE job_id=%s",
                (inserted_job_id,),
            )
            setup.commit()
    finally:
        setup.rollback()
        setup.execute(
            "DELETE FROM groundloop_semantic_job WHERE job_id = ANY(%s)",
            (list(job_ids.values()),),
        )
        setup.commit()


def test_scope_race_serializes_or_reranges_in_both_orders(d30_schema: Any) -> None:
    root_job_id = str(d30_schema.first_m4["root_job_id"])
    original = d30_schema.connection.execute(
        "SELECT closed_revision FROM groundloop_discovery_scope WHERE root_job_id=%s",
        (root_job_id,),
    ).fetchone()
    assert original is not None
    _exercise_point_race(
        d30_schema,
        select_sql=(
            "SELECT root_job_id,epoch_id,registry_snapshot_id,scope_kind,"
            "explicit_claim_ids,closed_revision FROM groundloop_discovery_scope "
            "WHERE root_job_id=%s"
        ),
        select_parameters=(root_job_id,),
        update_sql=(
            "UPDATE groundloop_discovery_scope "
            "SET closed_revision=COALESCE(closed_revision,0)+1 "
            "WHERE root_job_id=%s"
        ),
        update_parameters=(root_job_id,),
        restore_sql=(
            "UPDATE groundloop_discovery_scope SET closed_revision=%s "
            "WHERE root_job_id=%s"
        ),
        restore_parameters=(original[0], root_job_id),
    )


def test_working_delta_race_serializes_or_reranges_in_both_orders(
    d30_schema: Any,
) -> None:
    setup = d30_schema.connection
    source = setup.execute(
        """SELECT job.candidate_policy_id,policy.claim_registry_snapshot_id
             FROM groundloop_semantic_job AS job
             JOIN groundloop_candidate_policy AS policy
               USING (candidate_policy_id)
            WHERE job.epoch_id=%s
            ORDER BY job.job_id COLLATE "C" LIMIT 1""",
        (int(d30_schema.first_m4["epoch_id"]),),
    ).fetchone()
    assert source is not None
    epoch = setup.execute(
        """INSERT INTO groundloop_epoch (
               event_id,payload_hash,revision,structural_status,
               semantic_status,evaluation_state,publication_mode,sealed_at
           ) VALUES (
               'd30-working-delta-race-epoch',%s,1,'committed','sealed',
               'complete','strict',now()
           ) RETURNING epoch_id""",
        ("a" * 64,),
    ).fetchone()
    assert epoch is not None
    epoch_id = int(epoch[0])
    setup.execute(
        "ALTER TABLE groundloop_m4_update "
        "DISABLE TRIGGER groundloop_m4_update_runtime_mode_guard"
    )
    try:
        setup.execute(
            """INSERT INTO groundloop_m4_update (
                   epoch_id,update_kind,candidate_policy_id,
                   previous_published_epoch_id,registry_snapshot_id,manifest
               ) VALUES (%s,'insert',%s,%s,%s,'{}'::jsonb)""",
            (
                epoch_id,
                str(source[0]),
                int(d30_schema.database.base.epoch_id),
                str(source[1]),
            ),
        )
    finally:
        setup.execute(
            "ALTER TABLE groundloop_m4_update "
            "ENABLE TRIGGER groundloop_m4_update_runtime_mode_guard"
        )
    claim_id = str(d30_schema.database.base.claim_ids[0])
    chunk_id = str(d30_schema.database.base.chunk_ids[0])
    installed_revision = 1
    coordinates = tuple(
        (
            f"d30-working-delta-race-task-{order}",
            f"d30-working-delta-race-observation-{order}",
        )
        for order in ("planner-first", "writer-first")
    )
    for task_type, observation_id in coordinates:
        insert_observation(
            setup,
            observation_id=observation_id,
            subject_kind="claim",
            subject_id=claim_id,
            chunk_id=chunk_id,
            produced_epoch=epoch_id,
            task_type=task_type,
        )
    setup.commit()
    select_sql = (
        "SELECT epoch_id,subject_kind::text,subject_id,chunk_version_id,"
        "task_type,base_observation_id,working_observation_id,"
        "installed_revision FROM groundloop_working_observation_delta "
        "WHERE epoch_id=%s AND subject_kind=%s AND subject_id=%s "
        "AND chunk_version_id=%s AND task_type=%s"
    )
    insert_sql = """INSERT INTO groundloop_working_observation_delta (
                           epoch_id,subject_kind,subject_id,chunk_version_id,
                           task_type,base_observation_id,
                           working_observation_id,installed_revision
                       ) VALUES (%s,%s,%s,%s,%s,NULL,%s,%s)"""
    try:
        with _reconnect(d30_schema) as first, _reconnect(d30_schema) as second:
            planner_task, planner_observation = coordinates[0]
            planner_key = (epoch_id, "claim", claim_id, chunk_id, planner_task)
            assert first.execute(select_sql, planner_key).fetchone() is None
            assert (
                second.execute(
                    insert_sql,
                    (*planner_key, planner_observation, installed_revision),
                ).rowcount
                == 1
            )
            second.commit()
            appeared = first.execute(select_sql + " FOR UPDATE", planner_key).fetchone()
            assert appeared is not None
            first.rollback()

            writer_task, writer_observation = coordinates[1]
            writer_key = (epoch_id, "claim", claim_id, chunk_id, writer_task)
            assert (
                second.execute(
                    insert_sql,
                    (*writer_key, writer_observation, installed_revision),
                ).rowcount
                == 1
            )
            second.commit()
            preliminary = first.execute(select_sql, writer_key).fetchone()
            locked = first.execute(select_sql + " FOR UPDATE", writer_key).fetchone()
            assert preliminary is not None and locked == preliminary
            first.rollback()
    finally:
        setup.rollback()
        setup.execute(
            "ALTER TABLE groundloop_semantic_observation "
            "DISABLE TRIGGER groundloop_semantic_observation_immutable"
        )
        setup.execute(
            "ALTER TABLE groundloop_working_observation_delta "
            "DISABLE TRIGGER groundloop_working_observation_delta_immutable"
        )
        try:
            setup.execute(
                "DELETE FROM groundloop_working_observation_delta WHERE epoch_id=%s",
                (epoch_id,),
            )
            setup.execute(
                "DELETE FROM groundloop_semantic_observation "
                "WHERE observation_id = ANY(%s)",
                ([observation_id for _task, observation_id in coordinates],),
            )
            setup.commit()
        finally:
            setup.rollback()
            setup.execute(
                "ALTER TABLE groundloop_working_observation_delta "
                "ENABLE TRIGGER groundloop_working_observation_delta_immutable"
            )
            setup.execute(
                "ALTER TABLE groundloop_semantic_observation "
                "ENABLE TRIGGER groundloop_semantic_observation_immutable"
            )
            setup.commit()
        setup.execute(
            "DELETE FROM groundloop_m4_update WHERE epoch_id=%s",
            (epoch_id,),
        )
        setup.execute("DELETE FROM groundloop_epoch WHERE epoch_id=%s", (epoch_id,))
        setup.commit()


def test_current_currency_race_serializes_or_reranges_in_both_orders(
    d30_schema: Any,
) -> None:
    original = d30_schema.connection.execute(
        """SELECT subject_kind::text,subject_id,chunk_version_id,task_type,
                  installed_revision
             FROM groundloop_observation_currency
            ORDER BY subject_kind::text COLLATE "C",subject_id COLLATE "C",
                     chunk_version_id COLLATE "C",task_type COLLATE "C"
            LIMIT 1"""
    ).fetchone()
    assert original is not None
    key = tuple(original[:4])
    _exercise_point_race(
        d30_schema,
        select_sql=(
            "SELECT subject_kind::text,subject_id,chunk_version_id,task_type,"
            "observation_id,installed_revision "
            "FROM groundloop_observation_currency WHERE subject_kind=%s "
            "AND subject_id=%s AND chunk_version_id=%s AND task_type=%s"
        ),
        select_parameters=key,
        update_sql=(
            "UPDATE groundloop_observation_currency "
            "SET installed_revision=installed_revision+1 "
            "WHERE subject_kind=%s AND subject_id=%s "
            "AND chunk_version_id=%s AND task_type=%s"
        ),
        update_parameters=key,
        restore_sql=(
            "UPDATE groundloop_observation_currency SET installed_revision=%s "
            "WHERE subject_kind=%s AND subject_id=%s "
            "AND chunk_version_id=%s AND task_type=%s"
        ),
        restore_parameters=(original[4], *key),
    )


def test_authority_trace_has_no_forbidden_root_or_classic_reconstruction(
    function_source: Callable[[str], str],
) -> None:
    source = "\n".join(
        function_source(name)
        for name in (
            "_gather_d30_claim_authority",
            "_d30_owner_locator",
            "_d30_execution_coordinates",
            "_lock_d29_scopes_and_reserve",
            "_lock_d29_jobs_and_reserve",
            "_lock_d29_direct_attempts",
            "_lock_d29_tier_10_authority",
            "_validate_d30_m3_bootstrap",
            "_validate_d30_dynamic_claim",
            "_probe_d30_predecessor_currency",
        )
    )
    for forbidden in (
        "groundloop_impact_channel_hit",
        "PairVerificationInput",
        "PairVerificationArtifact",
        "PairJudgment",
        "migration_019",
        "migrations/019",
        "repository.",
        "model_provider",
    ):
        assert forbidden not in source


def test_publication_scope_delta_and_currency_have_locked_guarded_reranges(
    function_source: Callable[[str], str],
) -> None:
    topology = "\n".join(
        function_source(name)
        for name in (
            "_lock_d29_scopes_and_reserve",
            "_lock_d29_jobs_and_reserve",
            "_lock_d29_tier_10_authority",
        )
    )
    claim = "\n".join(
        function_source(name)
        for name in (
            "_lock_d29_observation_authority",
            "_validate_d30_dynamic_claim",
        )
    )
    locator = function_source("_gather_d29_locator_authority")
    assert topology.count("FROM groundloop_semantic_job") >= 2
    assert topology.count("FROM groundloop_semantic_job_dependency") >= 2
    assert topology.count("groundloop_discovery_scope") >= 2
    assert "FOR UPDATE" in topology or "FOR KEY SHARE" in topology
    assert claim.count("groundloop_working_observation_delta") >= 2
    assert locator.count("groundloop_observation_currency") == 1
    assert claim.count("groundloop_observation_currency") >= 1
    assert "FOR UPDATE" in claim or "FOR KEY SHARE" in claim
    assert "changed" in topology.lower() or "rerun" in topology.lower()
    assert "changed" in claim.lower() or "rerun" in claim.lower()
