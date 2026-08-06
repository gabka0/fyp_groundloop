"""Adversarial live acceptance tests for M5.3-07 failure and replay."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import Any

import pytest
from psycopg import Connection, sql

from groundloop.errors import EventConflictError, InvalidEventError
from groundloop.m4.application import OpenEventReceipt
from groundloop.m5.events import RegisterGroupEvent, ReplaceGroupEvent
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    M5CandidatePolicyManifest,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5EventRunResult,
    M5JobCompletion,
    M5JobKind,
    M5JobState,
    M5LogicalJobSpec,
    M5ReplayedOutcome,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeWork,
    M5TerminalReason,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
    RequirementRegistrySnapshotEntry,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore


class InjectedRuntimeFailure(RuntimeError):
    """Deterministic test-only failure raised at one production injection point."""


OPEN_FAILURE_CASES = (
    ("retire", "typed_open_epoch_inserted"),
    ("retire", "typed_open_update_inserted"),
    ("retire", "typed_open_runtime_header_inserted"),
    ("register", "typed_open_family_staged"),
    ("register", "typed_open_group_staged"),
    ("replace", "typed_open_deactivation_staged"),
    ("register", "typed_open_roots_persisted"),
    ("register", "typed_open_snapshots_persisted"),
    ("register", "typed_open_before_commit"),
)

FAILURE_TRANSITION_POINTS = (
    "typed_fail_authorized",
    "typed_fail_jobs_cancelled",
    "typed_fail_counters_updated",
    "typed_fail_structure_failed",
    "typed_fail_work_inserted",
    "typed_fail_result_inserted",
    "typed_fail_base_updated",
    "typed_fail_runtime_updated",
    "typed_fail_before_constraints",
    "typed_fail_after_constraints",
)

PUBLISHED_SURFACE_TABLES = (
    "groundloop_m4_publication_head",
    "groundloop_m5_publication_head",
    "groundloop_m5_group_family",
    "groundloop_m5_group_version",
    "groundloop_m5_requirement_version",
    "groundloop_m5_group_validity",
    "groundloop_m5_group_family_retirement",
    "groundloop_observation_currency",
    "groundloop_published_observation_currency",
    "groundloop_status_delta",
    "groundloop_claim_state_materialized",
    "groundloop_answer_state_materialized",
    "groundloop_claim_certificate",
    "groundloop_published_claim_state",
    "groundloop_published_answer_state",
    "groundloop_m5_requirement_state_materialized",
    "groundloop_m5_group_state_materialized",
    "groundloop_m5_claim_state_materialized",
    "groundloop_m5_answer_state_materialized",
    "groundloop_m5_published_requirement_state",
    "groundloop_m5_published_group_state",
    "groundloop_m5_published_claim_state",
    "groundloop_m5_published_answer_state",
    "groundloop_m5_group_certificate_artifact",
    "groundloop_m5_group_certificate_artifact_row",
    "groundloop_m5_claim_certificate_artifact",
    "groundloop_m5_published_group_certificate_binding",
    "groundloop_m5_published_claim_certificate_binding",
)

STRICT_NONSTRUCTURAL_TABLES = tuple(
    table_name
    for table_name in PUBLISHED_SURFACE_TABLES
    if table_name
    not in {
        "groundloop_m5_group_family",
        "groundloop_m5_group_version",
        "groundloop_m5_requirement_version",
    }
)


def _table_names(connection: Connection[Any]) -> tuple[str, ...]:
    return tuple(
        str(row[0])
        for row in connection.execute(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = current_schema()
            ORDER BY tablename COLLATE "C"
            """
        ).fetchall()
    )


def _snapshot_tables(
    connection: Connection[Any], table_names: tuple[str, ...]
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    """Capture logical rows plus xmin so same-value rewrites are observable."""

    connection.commit()
    snapshot: list[tuple[str, tuple[tuple[str, str], ...]]] = []
    for table_name in table_names:
        statement = sql.SQL(
            "SELECT xmin::text, to_jsonb(snapshot_row)::text "
            "FROM {} AS snapshot_row "
            'ORDER BY to_jsonb(snapshot_row)::text COLLATE "C", xmin::text'
        ).format(sql.Identifier(table_name))
        rows = tuple(
            (str(row[0]), str(row[1]))
            for row in connection.execute(statement).fetchall()
        )
        snapshot.append((table_name, rows))
    connection.commit()
    return tuple(snapshot)


def _database_snapshot(
    connection: Connection[Any],
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    return _snapshot_tables(connection, _table_names(connection))


def _published_snapshot(
    connection: Connection[Any],
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    return _snapshot_tables(connection, PUBLISHED_SURFACE_TABLES)


def _published_structure_rows(
    connection: Connection[Any], table_name: str
) -> tuple[tuple[str, str], ...]:
    statement = sql.SQL(
        "SELECT xmin::text, to_jsonb(snapshot_row)::text "
        "FROM {} AS snapshot_row WHERE lifecycle_state = 'PUBLISHED' "
        'ORDER BY to_jsonb(snapshot_row)::text COLLATE "C", xmin::text'
    ).format(sql.Identifier(table_name))
    return tuple(
        (str(row[0]), str(row[1])) for row in connection.execute(statement).fetchall()
    )


def _strict_published_truth_snapshot(
    connection: Connection[Any],
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    snapshot = list(_snapshot_tables(connection, STRICT_NONSTRUCTURAL_TABLES))
    for table_name in (
        "groundloop_m5_group_family",
        "groundloop_m5_group_version",
        "groundloop_m5_requirement_version",
    ):
        snapshot.append(
            (
                f"{table_name}:PUBLISHED",
                _published_structure_rows(connection, table_name),
            )
        )
    connection.commit()
    return tuple(snapshot)


def _plan_for_kind(m5_runtime_db: Any, event_kind: str, event_id: str) -> Any:
    if event_kind == "register":
        return m5_runtime_db.register_plan(event_id=event_id)
    if event_kind == "replace":
        return m5_runtime_db.replace_plan(event_id=event_id)
    return m5_runtime_db.retire_plan(event_id=event_id)


def _root_expectations(
    plan: M5TypedEventPlan, manifest: M5CandidatePolicyManifest
) -> tuple[tuple[M5DiscoveryScopeContract, M5LogicalJobSpec, M5JobCompletion], ...]:
    if isinstance(plan.event, RegisterGroupEvent):
        requirements = plan.event.group.requirements
    elif isinstance(plan.event, ReplaceGroupEvent):
        requirements = plan.event.successor.requirements
    else:
        requirements = ()
    expected = []
    for requirement in requirements:
        scope = M5DiscoveryScopeContract.build(
            direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
            requirement_version_id=requirement.requirement_version_id,
            inserted_chunk_version_id=None,
            candidate_policy_id=manifest.candidate_policy_id,
            requirement_registry_snapshot_digest=(
                plan.requirement_registry_snapshot.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest=(
                plan.active_chunk_snapshot.active_chunk_snapshot_digest
            ),
        )
        job = M5LogicalJobSpec.build(
            structural_event_id=plan.structural_event_id,
            job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
            manifest=manifest,
            scope=scope,
        )
        completion = M5JobCompletion.build(
            job=job,
            terminal_state=M5JobState.CANCELLED,
            archive_reason=M5TerminalReason.EPOCH_FAILED,
        )
        expected.append((scope, job, completion))
    return tuple(sorted(expected, key=lambda item: item[1].logical_job_id))


def _raise_at(target: str, seen: list[str]) -> Callable[[str], None]:
    def inject(point: str) -> None:
        seen.append(point)
        if point == target:
            raise InjectedRuntimeFailure(target)

    return inject


def _terminal_fail(
    m5_runtime_db: Any,
) -> tuple[M5TypedEventPlan, OpenEventReceipt, M5EventRunResult]:
    plan = m5_runtime_db.retire_plan()
    store = PostgresM5RuntimeStore(m5_runtime_db.connection)
    receipt = store.open_typed_event_atomically(plan)
    result = store.fail_typed_epoch_atomically(
        receipt.epoch_id,
        failure_reason=M5RunFailureReason.INVARIANT_FAILURE,
    )
    return plan, receipt, result


def _assert_open_roots(
    connection: Connection[Any],
    *,
    m5_runtime_db: Any,
    plan: M5TypedEventPlan,
    epoch_id: int,
) -> tuple[tuple[M5DiscoveryScopeContract, M5LogicalJobSpec, M5JobCompletion], ...]:
    roots = _root_expectations(plan, m5_runtime_db.manifest)
    root_ids = tuple(root[1].logical_job_id for root in roots)
    assert connection.execute(
        """
        SELECT requirement_root_set_hash, runtime_state, revision,
               open_work_count, open_scope_count, blocking_failure_count
        FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone() == (
        digests.requirement_root_set_digest(root_ids),
        "structural_committed",
        1,
        len(roots),
        len(roots),
        0,
    )
    assert connection.execute(
        """
        SELECT broad_reverse_scope_count, forward_scope_count,
               verifier_job_count, blocking_failure_count, updated_revision
        FROM groundloop_m5_owner_pending_counter
        WHERE epoch_id = %s AND owner_claim_id = %s
        """,
        (epoch_id, m5_runtime_db.base.claim_ids[0]),
    ).fetchone() == (0, len(roots), 0, 0, 1)
    assert connection.execute(
        """
        SELECT broad_reverse_scope_count, forward_scope_count,
               verifier_job_count, blocking_failure_count, updated_revision
        FROM groundloop_m5_answer_pending_counter
        WHERE epoch_id = %s AND answer_version_id = %s
        """,
        (epoch_id, m5_runtime_db.base.answer_id),
    ).fetchone() == (0, len(roots), 0, 0, 1)
    for scope, job, _ in roots:
        assert connection.execute(
            """
            SELECT direction, requirement_version_id,
                   inserted_chunk_version_id, candidate_policy_id,
                   requirement_registry_snapshot_digest,
                   active_chunk_snapshot_digest, scope_contract_digest,
                   scope_state, completion_digest, created_revision,
                   staged_revision, closed_revision, closed_at
            FROM groundloop_m5_discovery_scope WHERE root_job_id = %s
            """,
            (job.logical_job_id,),
        ).fetchone() == (
            scope.direction.value,
            scope.requirement_version_id,
            None,
            scope.candidate_policy_id,
            scope.requirement_registry_snapshot_digest,
            scope.active_chunk_snapshot_digest,
            scope.scope_contract_digest,
            "open",
            None,
            1,
            None,
            None,
            None,
        )
        assert connection.execute(
            """
            SELECT structural_event_id, job_kind, candidate_policy_id,
                   candidate_policy_manifest_hash, parent_job_id,
                   scope_contract_digest,
                   requirement_registry_snapshot_digest,
                   active_chunk_snapshot_digest, role_template_hash,
                   execution_spec_hash, expandable, payload_hash,
                   job_state, archive_reason, completion_digest,
                   created_revision, completed_revision, completed_at
            FROM groundloop_m5_semantic_job WHERE logical_job_id = %s
            """,
            (job.logical_job_id,),
        ).fetchone() == (
            plan.structural_event_id,
            job.job_kind.value,
            job.candidate_policy_id,
            job.candidate_policy_manifest_hash,
            None,
            job.scope_contract_digest,
            job.requirement_registry_snapshot_digest,
            job.active_chunk_snapshot_digest,
            job.role_template_hash,
            job.execution_spec_hash,
            True,
            job.payload_hash,
            "declared",
            None,
            None,
            1,
            None,
            None,
        )
    connection.commit()
    return roots


def _assert_failed_roots(
    connection: Connection[Any],
    *,
    m5_runtime_db: Any,
    plan: M5TypedEventPlan,
    epoch_id: int,
    result: M5EventRunResult,
    roots: tuple[
        tuple[M5DiscoveryScopeContract, M5LogicalJobSpec, M5JobCompletion], ...
    ],
) -> None:
    assert connection.execute(
        """
        SELECT runtime_state, revision, open_work_count, open_scope_count,
               blocking_failure_count, terminal_at IS NOT NULL
        FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone() == ("failed", 2, 0, 0, 0, True)
    for _, job, completion in roots:
        assert connection.execute(
            """
            SELECT job_state, result_artifact_id, result_artifact_hash,
                   scope_closure_digest, child_set_hash, archive_reason,
                   completion_digest, cancelled_by_event_id,
                   cancelled_by_epoch_id, cancellation_reason,
                   completed_revision, completed_at IS NOT NULL
            FROM groundloop_m5_semantic_job WHERE logical_job_id = %s
            """,
            (job.logical_job_id,),
        ).fetchone() == (
            "cancelled",
            None,
            None,
            None,
            None,
            M5TerminalReason.EPOCH_FAILED.value,
            completion.completion_digest,
            plan.structural_event_id,
            epoch_id,
            M5TerminalReason.EPOCH_FAILED.value,
            2,
            True,
        )
        assert connection.execute(
            """
            SELECT scope_state, staged_result_artifact_hash,
                   scope_closure_digest, child_set_hash, completion_digest,
                   staged_revision, closed_revision, closed_at IS NOT NULL
            FROM groundloop_m5_discovery_scope WHERE root_job_id = %s
            """,
            (job.logical_job_id,),
        ).fetchone() == (
            "cancelled",
            None,
            None,
            None,
            completion.completion_digest,
            None,
            2,
            True,
        )
    for table_name, key_name, key_value in (
        (
            "groundloop_m5_owner_pending_counter",
            "owner_claim_id",
            m5_runtime_db.base.claim_ids[0],
        ),
        (
            "groundloop_m5_answer_pending_counter",
            "answer_version_id",
            m5_runtime_db.base.answer_id,
        ),
    ):
        statement = sql.SQL(
            "SELECT broad_reverse_scope_count, forward_scope_count, "
            "verifier_job_count, blocking_failure_count, updated_revision "
            "FROM {} WHERE epoch_id = %s AND {} = %s"
        ).format(sql.Identifier(table_name), sql.Identifier(key_name))
        assert connection.execute(statement, (epoch_id, key_value)).fetchone() == (
            0,
            0,
            0,
            0,
            2,
        )

    expected_work = M5RuntimeWork(requirement_cancelled_job_count=len(roots))
    assert result.event_work == expected_work
    assert result.call_work == expected_work
    assert connection.execute(
        """
        SELECT work_kind, work_digest, requirement_cancelled_job_count
        FROM groundloop_m5_runtime_work
        WHERE structural_event_id = %s ORDER BY work_kind COLLATE "C"
        """,
        (plan.structural_event_id,),
    ).fetchall() == [
        ("call", expected_work.work_digest, len(roots)),
        ("event", expected_work.work_digest, len(roots)),
    ]
    assert connection.execute(
        """
        SELECT event_work_digest, failure_reason, logical_result_hash,
               delta_count, state_reference_count
        FROM groundloop_m5_event_result WHERE structural_event_id = %s
        """,
        (plan.structural_event_id,),
    ).fetchone() == (
        expected_work.work_digest,
        M5RunFailureReason.INVARIANT_FAILURE.value,
        result.logical_result_hash,
        0,
        0,
    )
    connection.commit()


@pytest.mark.parametrize("rejection", ("missing_group", "wrong_snapshot"))
def test_rejected_declaration_consumes_no_event_or_epoch(
    m5_runtime_db: Any, rejection: str
) -> None:
    if rejection == "missing_group":
        plan = m5_runtime_db.retire_plan(
            event_id="rejected-retire-missing",
            group_version_id="absent-group-version",
        )
    else:
        requirement = m5_runtime_db.group.requirements[0]
        wrong_snapshot = RequirementRegistrySnapshot.build(
            (
                RequirementRegistrySnapshotEntry.build(
                    requirement_version_id=requirement.requirement_version_id,
                    group_version_id=m5_runtime_db.group.group_version_id,
                    group_family_id=m5_runtime_db.group.group_family_id,
                    owner_claim_id=m5_runtime_db.group.owner_claim_id,
                    requirement_text=requirement.requirement_text,
                ),
            )
        )
        plan = m5_runtime_db.retire_plan(
            event_id="rejected-retire-snapshot",
            requirement_snapshot=wrong_snapshot,
        )

    before = _database_snapshot(m5_runtime_db.connection)
    with pytest.raises(InvalidEventError):
        PostgresM5RuntimeStore(m5_runtime_db.connection).open_typed_event_atomically(
            plan
        )

    with m5_runtime_db.reconnect() as reconnect:
        assert _database_snapshot(reconnect) == before
        assert reconnect.execute(
            "SELECT count(*) FROM groundloop_epoch WHERE event_id = %s",
            (plan.structural_event_id,),
        ).fetchone() == (0,)


@pytest.mark.parametrize(
    ("event_kind", "failure_point"),
    OPEN_FAILURE_CASES,
    ids=lambda value: str(value),
)
def test_every_open_failure_point_rolls_back_and_allows_clean_retry(
    m5_runtime_db: Any, event_kind: str, failure_point: str
) -> None:
    plan = _plan_for_kind(
        m5_runtime_db,
        event_kind,
        f"open-rollback-{failure_point}",
    )
    store = PostgresM5RuntimeStore(m5_runtime_db.connection)
    before = _database_snapshot(m5_runtime_db.connection)
    seen: list[str] = []

    with pytest.raises(InjectedRuntimeFailure, match=failure_point):
        store.open_typed_event_atomically(
            plan,
            failure_injector=_raise_at(failure_point, seen),
        )
    assert failure_point in seen

    with m5_runtime_db.reconnect() as reconnect:
        assert _database_snapshot(reconnect) == before
        assert reconnect.execute(
            "SELECT count(*) FROM groundloop_epoch WHERE event_id = %s",
            (plan.structural_event_id,),
        ).fetchone() == (0,)

    retry = store.open_typed_event_atomically(plan)
    assert not retry.replayed
    assert not retry.already_failed
    assert not retry.already_sealed


def test_durable_retire_failure_keeps_exact_audit_and_published_truth(
    m5_runtime_db: Any,
) -> None:
    published_before = _published_snapshot(m5_runtime_db.connection)
    plan, receipt, result = _terminal_fail(m5_runtime_db)
    zero_work = M5RuntimeWork()

    assert result.state is M5RunState.FAILED
    assert result.replayed_outcome is None
    assert result.failure_reason is M5RunFailureReason.INVARIANT_FAILURE
    assert result.event_work == zero_work
    assert result.call_work == zero_work
    assert result.combined_deltas == ()
    assert result.changed_state_references == ()
    assert result.open_receipt == receipt
    assert result.logical_result_hash is not None

    with m5_runtime_db.reconnect() as reconnect:
        assert _published_snapshot(reconnect) == published_before

        base_row = reconnect.execute(
            """
            SELECT revision, structural_status, semantic_status,
                   evaluation_state, publication_mode, sealed_at,
                   payload_hash
            FROM groundloop_epoch
            WHERE epoch_id = %s AND event_id = %s
            """,
            (receipt.epoch_id, plan.structural_event_id),
        ).fetchone()
        assert base_row == (
            2,
            "failed",
            "failed",
            "failed",
            "provisional",
            None,
            plan.payload_hash,
        )

        update_row = reconnect.execute(
            """
            SELECT update_kind, previous_published_epoch_id,
                   decision_policy_version, manifest
            FROM groundloop_m5_update WHERE epoch_id = %s
            """,
            (receipt.epoch_id,),
        ).fetchone()
        assert update_row == (
            "retire_group",
            m5_runtime_db.base.epoch_id,
            m5_runtime_db.base.policy_version,
            {},
        )

        runtime_row = reconnect.execute(
            """
            SELECT structural_event_id, candidate_policy_id,
                   candidate_policy_manifest_hash,
                   requirement_registry_snapshot_digest,
                   active_chunk_snapshot_digest,
                   expected_previous_published_epoch_id,
                   requirement_root_set_hash, runtime_state, revision,
                   open_work_count, open_scope_count, blocking_failure_count,
                   terminal_at IS NOT NULL
            FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
            """,
            (receipt.epoch_id,),
        ).fetchone()
        assert runtime_row == (
            plan.structural_event_id,
            m5_runtime_db.manifest.candidate_policy_id,
            m5_runtime_db.manifest.manifest_hash,
            m5_runtime_db.requirement_snapshot.requirement_registry_snapshot_digest,
            m5_runtime_db.chunk_snapshot.active_chunk_snapshot_digest,
            m5_runtime_db.base.epoch_id,
            digests.requirement_root_set_digest(()),
            "failed",
            2,
            0,
            0,
            0,
            True,
        )

        assert reconnect.execute(
            """
            SELECT group_version_id, action, successor_group_version_id, event_id
            FROM groundloop_m5_group_deactivation WHERE epoch_id = %s
            """,
            (receipt.epoch_id,),
        ).fetchone() == (
            m5_runtime_db.group.group_version_id,
            "RETIRE",
            None,
            plan.structural_event_id,
        )
        assert reconnect.execute(
            """
            SELECT lifecycle_state FROM groundloop_m5_group_version
            WHERE group_version_id = %s
            """,
            (m5_runtime_db.group.group_version_id,),
        ).fetchone() == ("PUBLISHED",)
        assert reconnect.execute(
            """
            SELECT valid_from_epoch, valid_to_epoch
            FROM groundloop_m5_group_validity WHERE group_version_id = %s
            """,
            (m5_runtime_db.group.group_version_id,),
        ).fetchone() == (m5_runtime_db.base.epoch_id, None)
        assert reconnect.execute(
            "SELECT count(*) FROM groundloop_m5_group_family_retirement"
        ).fetchone() == (0,)

        work_rows = reconnect.execute(
            """
            SELECT work_kind, work_digest
            FROM groundloop_m5_runtime_work
            WHERE structural_event_id = %s
            ORDER BY work_kind COLLATE "C"
            """,
            (plan.structural_event_id,),
        ).fetchall()
        assert work_rows == [
            ("call", zero_work.work_digest),
            ("event", zero_work.work_digest),
        ]
        for work_kind in ("event", "call"):
            work_json = reconnect.execute(
                """
                SELECT to_jsonb(work_row)
                FROM groundloop_m5_runtime_work AS work_row
                WHERE structural_event_id = %s AND work_kind = %s
                """,
                (plan.structural_event_id, work_kind),
            ).fetchone()
            assert work_json is not None
            for counter_name in M5RuntimeWork.counter_names():
                assert work_json[0][counter_name] == 0

        event_result = reconnect.execute(
            """
            SELECT outcome, original_open_receipt_binding_hash,
                   publication_id, original_publication_receipt_binding_hash,
                   event_work_kind, event_work_digest,
                   combined_status_delta_set_hash, changed_state_set_hash,
                   failure_reason, logical_result_hash, delta_count,
                   state_reference_count
            FROM groundloop_m5_event_result
            WHERE structural_event_id = %s
            """,
            (plan.structural_event_id,),
        ).fetchone()
        assert event_result == (
            "failed",
            digests.open_event_receipt_binding_digest(
                epoch_id=receipt.epoch_id,
                replayed=False,
                already_sealed=False,
                publication_id=None,
                already_failed=False,
                failure_reason=None,
            ),
            None,
            None,
            "event",
            zero_work.work_digest,
            digests.combined_status_delta_set_digest(()),
            digests.changed_state_set_digest(()),
            M5RunFailureReason.INVARIANT_FAILURE.value,
            result.logical_result_hash,
            0,
            0,
        )
        assert reconnect.execute(
            """
            SELECT
              (SELECT count(*) FROM groundloop_m5_event_result_delta
               WHERE structural_event_id = %s),
              (SELECT count(*) FROM groundloop_m5_event_result_state_reference
               WHERE structural_event_id = %s)
            """,
            (plan.structural_event_id, plan.structural_event_id),
        ).fetchone() == (0, 0)


@pytest.mark.parametrize("event_kind", ("register", "replace"))
def test_failed_staged_group_keeps_audit_cancels_roots_and_preserves_strict_truth(
    m5_runtime_db: Any, event_kind: str
) -> None:
    strict_before = _strict_published_truth_snapshot(m5_runtime_db.connection)
    plan = _plan_for_kind(
        m5_runtime_db,
        event_kind,
        f"durable-staged-{event_kind}",
    )
    store = PostgresM5RuntimeStore(m5_runtime_db.connection)
    receipt = store.open_typed_event_atomically(plan)
    roots = _assert_open_roots(
        m5_runtime_db.connection,
        m5_runtime_db=m5_runtime_db,
        plan=plan,
        epoch_id=receipt.epoch_id,
    )
    result = store.fail_typed_epoch_atomically(receipt.epoch_id)

    assert result.state is M5RunState.FAILED
    assert result.failure_reason is M5RunFailureReason.INVARIANT_FAILURE
    with m5_runtime_db.reconnect() as reconnect:
        _assert_failed_roots(
            reconnect,
            m5_runtime_db=m5_runtime_db,
            plan=plan,
            epoch_id=receipt.epoch_id,
            result=result,
            roots=roots,
        )
        assert _strict_published_truth_snapshot(reconnect) == strict_before

        if isinstance(plan.event, RegisterGroupEvent):
            staged_group = plan.event.group
            assert reconnect.execute(
                """
                SELECT lifecycle_state, creator_epoch_id
                FROM groundloop_m5_group_family WHERE group_family_id = %s
                """,
                (staged_group.group_family_id,),
            ).fetchone() == ("FAILED", receipt.epoch_id)
        else:
            assert isinstance(plan.event, ReplaceGroupEvent)
            staged_group = plan.event.successor
            assert reconnect.execute(
                """
                SELECT lifecycle_state FROM groundloop_m5_group_family
                WHERE group_family_id = %s
                """,
                (staged_group.group_family_id,),
            ).fetchone() == ("PUBLISHED",)
            assert reconnect.execute(
                """
                SELECT group_version_id, action, successor_group_version_id,
                       event_id
                FROM groundloop_m5_group_deactivation WHERE epoch_id = %s
                """,
                (receipt.epoch_id,),
            ).fetchone() == (
                m5_runtime_db.group.group_version_id,
                "REPLACE",
                staged_group.group_version_id,
                plan.structural_event_id,
            )
            assert reconnect.execute(
                """
                SELECT lifecycle_state FROM groundloop_m5_group_version
                WHERE group_version_id = %s
                """,
                (m5_runtime_db.group.group_version_id,),
            ).fetchone() == ("PUBLISHED",)
            assert reconnect.execute(
                """
                SELECT valid_to_epoch FROM groundloop_m5_group_validity
                WHERE group_version_id = %s
                """,
                (m5_runtime_db.group.group_version_id,),
            ).fetchone() == (None,)

        assert reconnect.execute(
            """
            SELECT lifecycle_state, creator_epoch_id
            FROM groundloop_m5_group_version WHERE group_version_id = %s
            """,
            (staged_group.group_version_id,),
        ).fetchone() == ("FAILED", receipt.epoch_id)
        assert reconnect.execute(
            """
            SELECT requirement_version_id, lifecycle_state, creator_epoch_id
            FROM groundloop_m5_requirement_version
            WHERE group_version_id = %s
            ORDER BY requirement_version_id COLLATE "C"
            """,
            (staged_group.group_version_id,),
        ).fetchall() == [
            (requirement.requirement_version_id, "FAILED", receipt.epoch_id)
            for requirement in sorted(
                staged_group.requirements,
                key=lambda item: item.requirement_version_id,
            )
        ]
        assert reconnect.execute(
            """
            SELECT count(*) FROM groundloop_m5_group_validity
            WHERE group_version_id = %s
            """,
            (staged_group.group_version_id,),
        ).fetchone() == (0,)

        before_replay = _database_snapshot(reconnect)
        replay = PostgresM5RuntimeStore(reconnect).read_typed_event_result(
            plan.structural_event_id, plan.payload_hash
        )
        assert replay is not None
        after_replay = _database_snapshot(reconnect)

    assert after_replay == before_replay
    assert replay.state is M5RunState.REPLAYED
    assert replay.replayed_outcome is M5ReplayedOutcome.FAILED
    assert replay.event_work == result.event_work
    assert replay.call_work.is_zero
    assert replay.logical_result_hash == result.logical_result_hash


@pytest.mark.parametrize("failure_point", FAILURE_TRANSITION_POINTS)
def test_every_terminal_failure_point_rolls_back_then_retries_exactly_once(
    m5_runtime_db: Any, failure_point: str
) -> None:
    plan = m5_runtime_db.register_plan(event_id=f"fail-rollback-{failure_point}")
    store = PostgresM5RuntimeStore(m5_runtime_db.connection)
    receipt = store.open_typed_event_atomically(plan)
    before = _database_snapshot(m5_runtime_db.connection)
    seen: list[str] = []

    with pytest.raises(InjectedRuntimeFailure, match=failure_point):
        store.fail_typed_epoch_atomically(
            receipt.epoch_id,
            failure_reason=M5RunFailureReason.INVARIANT_FAILURE,
            failure_injector=_raise_at(failure_point, seen),
        )
    assert failure_point in seen

    with m5_runtime_db.reconnect() as reconnect:
        assert _database_snapshot(reconnect) == before
        assert reconnect.execute(
            """
            SELECT revision, structural_status, semantic_status, evaluation_state
            FROM groundloop_epoch WHERE epoch_id = %s
            """,
            (receipt.epoch_id,),
        ).fetchone() == (1, "committed", "pending", "pending")
        assert reconnect.execute(
            """
            SELECT runtime_state, revision, terminal_at
            FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
            """,
            (receipt.epoch_id,),
        ).fetchone() == ("structural_committed", 1, None)

    retry = store.fail_typed_epoch_atomically(receipt.epoch_id)
    assert retry.state is M5RunState.FAILED
    assert retry.failure_reason is M5RunFailureReason.INVARIANT_FAILURE
    assert retry.event_id == plan.structural_event_id
    assert retry.event_work.requirement_cancelled_job_count == 2
    assert retry.call_work.requirement_cancelled_job_count == 2


def test_fresh_connection_failed_replay_is_exact_and_read_only(
    m5_runtime_db: Any,
) -> None:
    plan, receipt, original = _terminal_fail(m5_runtime_db)

    with m5_runtime_db.reconnect() as reconnect:
        before = _database_snapshot(reconnect)
        store = PostgresM5RuntimeStore(reconnect)
        read_result = store.read_typed_event_result(
            plan.structural_event_id, plan.payload_hash
        )
        assert read_result is not None
        open_replay = store.open_typed_event_atomically(plan)
        fail_replay = store.fail_typed_epoch_atomically(
            receipt.epoch_id,
            failure_reason=M5RunFailureReason.INVARIANT_FAILURE,
        )
        after = _database_snapshot(reconnect)

    assert after == before
    assert open_replay.replayed
    assert open_replay.already_failed
    assert not open_replay.already_sealed
    assert open_replay.failure_reason == M5RunFailureReason.INVARIANT_FAILURE.value
    for replay in (read_result, fail_replay):
        assert replay.state is M5RunState.REPLAYED
        assert replay.replayed_outcome is M5ReplayedOutcome.FAILED
        assert replay.failure_reason is M5RunFailureReason.INVARIANT_FAILURE
        assert replay.epoch_id == original.epoch_id
        assert replay.event_id == original.event_id
        assert replay.payload_hash == original.payload_hash
        assert replay.event_work == original.event_work
        assert replay.call_work.is_zero
        assert replay.event_timing == original.event_timing
        assert replay.combined_deltas == original.combined_deltas
        assert replay.changed_state_references == original.changed_state_references
        assert replay.logical_result_hash == original.logical_result_hash


def test_conflicting_payload_and_declaration_replay_change_no_row(
    m5_runtime_db: Any,
) -> None:
    plan, _, _ = _terminal_fail(m5_runtime_db)
    alternate = m5_runtime_db.register_manifest(m5_runtime_db.alternate_manifest())
    conflicting_payload = m5_runtime_db.retire_plan(
        event_id=plan.structural_event_id,
        group_version_id="another-group-version",
    )
    conflicting_declaration = m5_runtime_db.retire_plan(
        event_id=plan.structural_event_id,
        manifest=alternate,
    )
    before = _database_snapshot(m5_runtime_db.connection)
    store = PostgresM5RuntimeStore(m5_runtime_db.connection)

    with pytest.raises(EventConflictError, match="another payload"):
        store.open_typed_event_atomically(conflicting_payload)
    with pytest.raises(EventConflictError, match="declaration differs"):
        store.open_typed_event_atomically(conflicting_declaration)
    with pytest.raises(EventConflictError, match="another payload"):
        store.read_typed_event_result(
            plan.structural_event_id, conflicting_payload.payload_hash
        )

    with m5_runtime_db.reconnect() as reconnect:
        assert _database_snapshot(reconnect) == before


def test_concurrent_exact_open_commits_one_epoch_and_replays_the_other(
    m5_runtime_db: Any,
) -> None:
    plan = m5_runtime_db.retire_plan(event_id="concurrent-exact-retire")
    first_inserted = Event()
    release_first = Event()

    def first_open() -> OpenEventReceipt:
        def hold_after_insert(point: str) -> None:
            if point == "typed_open_epoch_inserted":
                first_inserted.set()
                if not release_first.wait(timeout=10):
                    raise TimeoutError("test did not release first typed opener")

        with m5_runtime_db.reconnect() as connection:
            return PostgresM5RuntimeStore(connection).open_typed_event_atomically(
                plan, failure_injector=hold_after_insert
            )

    def second_open() -> OpenEventReceipt:
        with m5_runtime_db.reconnect() as connection:
            return PostgresM5RuntimeStore(connection).open_typed_event_atomically(plan)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(first_open)
        assert first_inserted.wait(timeout=10)
        second_future = executor.submit(second_open)
        release_first.set()
        receipts = (first_future.result(timeout=20), second_future.result(timeout=20))

    assert {receipt.replayed for receipt in receipts} == {False, True}
    assert receipts[0].epoch_id == receipts[1].epoch_id
    with m5_runtime_db.reconnect() as reconnect:
        assert reconnect.execute(
            "SELECT count(*), min(epoch_id), max(epoch_id) "
            "FROM groundloop_epoch WHERE event_id = %s",
            (plan.structural_event_id,),
        ).fetchone() == (1, receipts[0].epoch_id, receipts[0].epoch_id)
