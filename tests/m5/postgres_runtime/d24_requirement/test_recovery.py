"""Live acceptance tests for checked D24 requirement acquisition."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest
from psycopg import Connection, sql

from groundloop.domain import SubjectKind
from groundloop.errors import (
    EventConflictError,
    InvalidEventError,
    ValidationError,
)
from groundloop.m5.events import m5_event_payload_digest
from groundloop.m5.runtime import digests
from groundloop.m5.runtime import persistence as persistence_module
from groundloop.m5.runtime import postgres_roots as postgres_roots_module
from groundloop.m5.runtime.contracts import (
    M5AcquisitionDisposition,
    M5AttemptOutput,
    M5CancellationPlan,
    M5ExecutionEvidenceDisposition,
    M5JobCompletion,
    M5JobState,
    M5RequirementAdmissionChannel,
    M5RequirementChannelHit,
    M5RequirementDiscoveryResult,
    M5RequirementPairInput,
    M5RequirementReturnDisposition,
    M5RequirementRootProvenance,
    M5RequirementScopeSelection,
    M5RequirementVerifierArtifact,
    M5RetrievalTermination,
    M5RuntimeOperationalConfig,
    M5RuntimeTiming,
    M5RuntimeTimingCoverage,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TerminalReason,
    SemanticPairKey,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from groundloop.m5.runtime.postgres_recovery import AcquisitionClock
from tests.m5.postgres_runtime.d24_requirement.conftest import (
    build_d24_verifier_fixture,
    terminalize_recovered_epoch_for_storage_fixture,
)

_ACQUISITION_TABLES = (
    "groundloop_epoch",
    "groundloop_m5_runtime_epoch",
    "groundloop_m5_runtime_operational_config",
    "groundloop_m5_requirement_root_provenance",
    "groundloop_m5_semantic_job",
    "groundloop_m5_job_dependency",
    "groundloop_m5_discovery_scope",
    "groundloop_m5_job_attempt",
    "groundloop_m5_attempt_result_artifact",
    "groundloop_m5_requirement_channel_hit",
    "groundloop_m5_requirement_scope_selection",
    "groundloop_m5_requirement_discovery_result",
    "groundloop_m5_dispatch_record",
    "groundloop_m5_attempt_execution_evidence",
    "groundloop_m5_expired_attempt_return",
    "groundloop_m5_post_terminal_attempt_timing",
    "groundloop_m5_post_terminal_attempt_audit",
    "groundloop_m5_runtime_work_contribution",
    "groundloop_m5_runtime_work_accumulator",
    "groundloop_m5_runtime_timing_contribution",
    "groundloop_m5_transition_call_timing",
    "groundloop_m5_runtime_timing_accumulator",
    "groundloop_m5_owner_pending_counter",
    "groundloop_m5_answer_pending_counter",
)

_TERMINAL_STATE_TABLES = (
    "groundloop_epoch",
    "groundloop_m4_publication_head",
    "groundloop_m5_publication_head",
    "groundloop_m5_activation",
    "groundloop_m5_runtime_epoch",
    "groundloop_m5_semantic_job",
    "groundloop_m5_discovery_scope",
    "groundloop_m5_requirement_admitted_pair",
    "groundloop_m5_requirement_admitted_pair_source",
    "groundloop_m5_requirement_pair_input",
    "groundloop_m5_requirement_verifier_artifact",
    "groundloop_m5_requirement_verifier_execution",
    "groundloop_semantic_observation",
    "groundloop_m5_requirement_frontier_head",
    "groundloop_admitted_pair",
    "groundloop_observation_currency",
    "groundloop_published_observation_currency",
    "groundloop_working_observation_delta",
    "groundloop_claim_state_materialized",
    "groundloop_answer_state_materialized",
    "groundloop_published_claim_state",
    "groundloop_published_answer_state",
    "groundloop_claim_certificate",
    "groundloop_m5_working_currency_history",
    "groundloop_m4_working_claim_state",
    "groundloop_m4_working_answer_state",
    "groundloop_m5_working_requirement_state",
    "groundloop_m5_working_group_state",
    "groundloop_m5_working_claim_state",
    "groundloop_m5_working_answer_state",
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
    "groundloop_m5_working_group_certificate_binding",
    "groundloop_m5_claim_certificate_artifact",
    "groundloop_m5_working_claim_certificate_binding",
    "groundloop_m5_published_group_certificate_binding",
    "groundloop_m5_published_claim_certificate_binding",
    "groundloop_m5_runtime_work_accumulator",
    "groundloop_m5_runtime_timing_accumulator",
    "groundloop_m5_runtime_work",
    "groundloop_m5_event_result",
    "groundloop_m5_event_result_delta",
    "groundloop_m5_event_result_state_reference",
    "groundloop_status_delta",
    "groundloop_m5_event_timing_coverage",
    "groundloop_m5_postcommit_invocation_telemetry",
    "groundloop_m5_owner_pending_counter",
    "groundloop_m5_answer_pending_counter",
)

_LATE_SEMANTIC_TABLES = (
    "groundloop_m5_requirement_channel_hit",
    "groundloop_m5_requirement_scope_selection",
    "groundloop_m5_requirement_discovery_result",
    "groundloop_m5_requirement_admitted_pair",
    "groundloop_m5_requirement_admitted_pair_source",
    "groundloop_m5_requirement_pair_input",
    "groundloop_m5_requirement_verifier_artifact",
    "groundloop_semantic_observation",
    "groundloop_m5_requirement_verifier_execution",
    "groundloop_observation_currency",
    "groundloop_published_observation_currency",
    "groundloop_working_observation_delta",
    "groundloop_m5_working_currency_history",
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _forward_attempt_work() -> M5RuntimeWork:
    return M5RuntimeWork(
        requirement_forward_retrieval_call_count=1,
        bytes_hashed=17,
        bytes_serialized=19,
        embedding_model_call_count=1,
        embedding_input_token_count=7,
    )


def _attempt_timing() -> M5RuntimeTiming:
    return M5RuntimeTiming(
        coordinator_non_db_non_neural_ns=11,
        neural_wall_ns=13,
        postgres_roundtrip_wall_ns=17,
        external_io_wall_ns=19,
        end_to_end_wall_ns=23,
        postgres_server_execution_ns=29,
        postgres_lock_wait_ns=None,
        postgres_wal_bytes=31,
        postgres_shared_block_reads=None,
    )


def _sum_work(*items: M5RuntimeWork) -> M5RuntimeWork:
    return M5RuntimeWork(
        **{
            name: sum(getattr(item, name) for item in items)
            for name in M5RuntimeWork.counter_names()
        }
    )


def _root_barrier_projection_byte_count(
    connection: Connection[Any], *, epoch_id: int, barrier_completion_hash: str
) -> int:
    """Independently frame the immutable barrier persistence projection."""

    header = connection.execute(
        """
        SELECT structural_event_id, requirement_root_set_hash
        FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert header is not None
    payloads: list[tuple[Any, ...]] = [
        ("plan", str(header[0]), str(header[1]), barrier_completion_hash)
    ]
    for row in connection.execute(
        """
        SELECT epoch_id, subject_kind, subject_id, chunk_version_id,
               semantic_pair_digest, candidate_policy_id, owner_root_job_id,
               reasons, mandatory_lineage, admitted_pair_digest
        FROM groundloop_m5_requirement_admitted_pair
        WHERE epoch_id = %s
        ORDER BY semantic_pair_digest COLLATE "C"
        """,
        (epoch_id,),
    ).fetchall():
        payloads.append(
            (
                "admitted_pair",
                int(row[0]),
                str(row[1]),
                str(row[2]),
                str(row[3]),
                str(row[4]),
                str(row[5]),
                str(row[6]),
                tuple(row[7]),
                bool(row[8]),
                str(row[9]),
            )
        )
    payloads.extend(
        (
            "admitted_source",
            str(row[0]),
            str(row[1]),
            str(row[2]),
            str(row[3]),
        )
        for row in connection.execute(
            """
            SELECT source.admitted_pair_digest, source.root_job_id,
                   source.scope_contract_digest, source.selection_digest
            FROM groundloop_m5_requirement_admitted_pair_source AS source
            JOIN groundloop_m5_requirement_admitted_pair AS admitted
              ON admitted.admitted_pair_digest = source.admitted_pair_digest
            WHERE admitted.epoch_id = %s
            ORDER BY source.admitted_pair_digest COLLATE "C",
                     source.root_job_id COLLATE "C"
            """,
            (epoch_id,),
        ).fetchall()
    )
    for row in connection.execute(
        """
        SELECT scope.root_job_id, scope.scope_contract_digest,
               ARRAY(
                   SELECT admitted.semantic_pair_digest
                   FROM groundloop_m5_requirement_admitted_pair AS admitted
                   WHERE admitted.epoch_id = scope.epoch_id
                     AND admitted.owner_root_job_id = scope.root_job_id
                   ORDER BY admitted.semantic_pair_digest COLLATE "C"
               ),
               scope.scope_closure_digest,
               ARRAY(
                   SELECT child.logical_job_id
                   FROM groundloop_m5_semantic_job AS child
                   WHERE child.epoch_id = scope.epoch_id
                     AND child.parent_job_id = scope.root_job_id
                   ORDER BY child.logical_job_id COLLATE "C"
               ),
               scope.child_set_hash
        FROM groundloop_m5_discovery_scope AS scope
        WHERE scope.epoch_id = %s
        ORDER BY scope.root_job_id COLLATE "C"
        """,
        (epoch_id,),
    ).fetchall():
        payloads.append(
            (
                "root_closure",
                str(row[0]),
                str(row[1]),
                tuple(row[2]),
                str(row[3]),
                tuple(row[4]),
                str(row[5]),
            )
        )
    payloads.extend(
        (
            "child_spec",
            str(row[0]),
            str(row[1]),
            str(row[2]),
            str(row[3]),
            str(row[4]),
            str(row[5]),
            str(row[6]),
            str(row[7]),
            str(row[8]),
            str(row[9]),
            str(row[10]),
            str(row[11]),
            str(row[12]),
            str(row[13]),
            str(row[14]),
            bool(row[15]),
            str(row[16]),
        )
        for row in connection.execute(
            """
            SELECT logical_job_id, structural_event_id, job_kind,
                   candidate_policy_id, candidate_policy_manifest_hash,
                   parent_job_id, subject_kind, subject_id, chunk_version_id,
                   semantic_pair_digest, scope_contract_digest,
                   requirement_registry_snapshot_digest,
                   active_chunk_snapshot_digest, role_template_hash,
                   execution_spec_hash, expandable, payload_hash
            FROM groundloop_m5_semantic_job
            WHERE epoch_id = %s AND parent_job_id IS NOT NULL
            ORDER BY logical_job_id COLLATE "C"
            """,
            (epoch_id,),
        ).fetchall()
    )
    encoded = sorted(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        for payload in payloads
    )
    return sum(8 + len(row) for row in encoded)


def _root_result(database: Any, *, job: Any) -> M5RequirementDiscoveryResult:
    subject_row = database.connection.execute(
        """
        SELECT requirement_version_id
        FROM groundloop_m5_discovery_scope
        WHERE epoch_id = %s AND root_job_id = %s
        """,
        (database.epoch_id, job.logical_job_id),
    ).fetchone()
    assert subject_row is not None
    pair = SemanticPairKey(
        SubjectKind.REQUIREMENT,
        str(subject_row[0]),
        database.plan.active_chunk_snapshot.entries[0].chunk_version_id,
    )
    hit = M5RequirementChannelHit.build(
        epoch_id=database.epoch_id,
        root_job_id=job.logical_job_id,
        scope_contract_digest=job.scope_contract_digest,
        pair=pair,
        candidate_policy_id=job.candidate_policy_id,
        channel=M5RequirementAdmissionChannel.VECTOR,
        rank=1,
        score=0.75,
        channel_artifact_hash=_sha("d24-root-hit"),
    )
    selection = M5RequirementScopeSelection.build(
        root_job_id=job.logical_job_id,
        scope_contract_digest=job.scope_contract_digest,
        pair=pair,
        fused_rank=1,
        reasons=(M5RequirementAdmissionChannel.VECTOR,),
    )
    return M5RequirementDiscoveryResult.build(
        root_job_id=job.logical_job_id,
        scope_contract_digest=job.scope_contract_digest,
        termination=M5RetrievalTermination.SNAPSHOT_EXHAUSTED,
        channel_hits=(hit,),
        selections=(selection,),
    )


def _root_result_and_output(
    database: Any, *, job: Any, attempt: Any
) -> tuple[M5RequirementDiscoveryResult, M5AttemptOutput]:
    result = _root_result(database, job=job)
    return result, M5AttemptOutput.build(
        attempt=attempt,
        job_epoch_id=database.epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=result.result_artifact_id,
        result_artifact_hash=result.result_artifact_hash,
    )


def _budget_filled_root_result_and_output(
    database: Any, *, job: Any, attempt: Any
) -> tuple[M5RequirementDiscoveryResult, M5AttemptOutput]:
    """Build an exact two-selection result for the fixture's frozen budget."""

    subject_row = database.connection.execute(
        """
        SELECT requirement_version_id
        FROM groundloop_m5_discovery_scope
        WHERE epoch_id = %s AND root_job_id = %s
        """,
        (database.epoch_id, job.logical_job_id),
    ).fetchone()
    chunk_rows = database.connection.execute(
        """
        SELECT chunk_version_id
        FROM groundloop_m5_active_chunk_snapshot_member
        WHERE active_chunk_snapshot_digest = %s
        ORDER BY member_ordinal
        LIMIT 2
        """,
        (job.active_chunk_snapshot_digest,),
    ).fetchall()
    assert subject_row is not None and len(chunk_rows) == 2
    hits = tuple(
        M5RequirementChannelHit.build(
            epoch_id=database.epoch_id,
            root_job_id=job.logical_job_id,
            scope_contract_digest=job.scope_contract_digest,
            pair=SemanticPairKey(
                SubjectKind.REQUIREMENT,
                str(subject_row[0]),
                str(chunk_row[0]),
            ),
            candidate_policy_id=job.candidate_policy_id,
            channel=M5RequirementAdmissionChannel.VECTOR,
            rank=rank,
            score=1.0 - rank / 10,
            channel_artifact_hash=_sha(f"d24-budget-hit-{rank}"),
        )
        for rank, chunk_row in enumerate(chunk_rows, start=1)
    )
    selections = tuple(
        M5RequirementScopeSelection.build(
            root_job_id=job.logical_job_id,
            scope_contract_digest=job.scope_contract_digest,
            pair=hit.pair,
            fused_rank=rank,
            reasons=(M5RequirementAdmissionChannel.VECTOR,),
        )
        for rank, hit in enumerate(hits, start=1)
    )
    result = M5RequirementDiscoveryResult.build(
        root_job_id=job.logical_job_id,
        scope_contract_digest=job.scope_contract_digest,
        termination=M5RetrievalTermination.BUDGET_FILLED,
        channel_hits=hits,
        selections=selections,
    )
    return result, M5AttemptOutput.build(
        attempt=attempt,
        job_epoch_id=database.epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=result.result_artifact_id,
        result_artifact_hash=result.result_artifact_hash,
    )


def _acquire_and_stage_root(
    database: Any,
    *,
    store: PostgresM5RuntimeStore,
    expected_revision: int,
    job: Any,
) -> tuple[int, Any, M5RequirementDiscoveryResult, M5AttemptOutput]:
    lease = store.acquire_m5_job(database.epoch_id, expected_revision, job)
    assert lease.attempt is not None and lease.should_execute
    result, output = _root_result_and_output(database, job=job, attempt=lease.attempt)

    receipt = store.stage_m5_discovery_result_atomically(
        database.epoch_id,
        lease.resulting_revision,
        lease,
        job,
        result,
        output,
        M5ExecutionEvidenceDisposition.RETURNED,
        _forward_attempt_work(),
        _attempt_timing(),
        eligible_snapshot_exhausted=True,
    )
    assert receipt.disposition is M5RequirementReturnDisposition.APPLIED
    return receipt.resulting_revision, lease, result, output


def _snapshot_tables(
    connection: Connection[Any], table_names: tuple[str, ...]
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    connection.commit()
    captured = []
    for table_name in table_names:
        rows = connection.execute(
            sql.SQL(
                "SELECT xmin::text, to_jsonb(snapshot_row)::text "
                "FROM {} AS snapshot_row "
                'ORDER BY to_jsonb(snapshot_row)::text COLLATE "C", xmin::text'
            ).format(sql.Identifier(table_name))
        ).fetchall()
        captured.append((table_name, tuple((str(row[0]), str(row[1])) for row in rows)))
    connection.commit()
    return tuple(captured)


def _snapshot(
    connection: Connection[Any],
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    return _snapshot_tables(connection, _ACQUISITION_TABLES)


def _full_snapshot(
    connection: Connection[Any],
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    table_names = tuple(dict.fromkeys((*_ACQUISITION_TABLES, *_TERMINAL_STATE_TABLES)))
    return _snapshot_tables(connection, table_names)


def _wait_until_expired(connection: Connection[Any], deadline: Any) -> None:
    connection.execute(
        """
        SELECT pg_sleep(
            greatest(extract(epoch FROM (%s::timestamptz - clock_timestamp())), 0)
            + 0.02
        )
        """,
        (deadline,),
    )
    connection.commit()


def _cancel_requirement_job_for_setup(
    database: Any,
    *,
    job: Any,
    reason: M5TerminalReason,
) -> int:
    store = PostgresM5RuntimeStore(database.connection)
    expected_revision = store.current_revision(database.epoch_id)
    plan = M5CancellationPlan.build(
        structural_event_id=database.plan.structural_event_id,
        epoch_id=database.epoch_id,
        cancelled_job_ids=(job.logical_job_id,),
        reason=reason,
    )
    receipt = store.cancel_m5_work_atomically(
        database.epoch_id,
        expected_revision,
        plan,
    )
    assert receipt.cancelled_job_ids == (job.logical_job_id,)
    assert not receipt.exact_replay
    return receipt.resulting_revision


def _late_requirement_row_counts(
    connection: Connection[Any], *, epoch_id: int, attempt_id: str
) -> tuple[int, ...]:
    """Count only the five migration-016 postterminal audit members."""

    row = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_attempt_result_artifact
           WHERE attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_expired_attempt_return
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_attempt_execution_evidence
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_post_terminal_attempt_timing
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_post_terminal_attempt_audit
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s)
        """,
        (
            attempt_id,
            epoch_id,
            attempt_id,
            epoch_id,
            attempt_id,
            epoch_id,
            attempt_id,
            epoch_id,
            attempt_id,
        ),
    ).fetchone()
    assert row is not None
    return tuple(map(int, row))


def _assert_expired_successor_rejections(
    database: Any,
    *,
    call: Any,
    stale_revision: int,
    current_revision: int,
) -> None:
    """C4 stale/current calls both conflict against a byte-identical cut."""

    for rejected_revision in (stale_revision, current_revision):
        before = _full_snapshot(database.connection)
        with pytest.raises(
            EventConflictError,
            match="nonrunning successor requires terminal event",
        ):
            call(rejected_revision)
        assert _full_snapshot(database.connection) == before


@pytest.mark.parametrize("observed_timing", [_attempt_timing(), None])
def test_transition_timing_append_replay_and_conflict(
    d24_requirement_db: Any,
    observed_timing: M5RuntimeTiming | None,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    source_id = d24_requirement_db.plan.structural_event_id
    contribution_kind = M5RuntimeWorkContributionKind.STRUCTURAL_OPEN
    contribution_key = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=contribution_kind,
        source_id=source_id,
    )

    receipt = store.append_transition_call_timing(
        epoch_id,
        contribution_kind,
        source_id,
        contribution_key,
        1,
        observed_timing,
    )
    expected_timing = observed_timing or M5RuntimeTiming()
    expected_coverage = M5RuntimeTimingCoverage.single_point(
        observed_timing,
        terminal_client_roundtrip_included=False,
    )
    assert not receipt.exact_replay and receipt.resulting_revision == 1
    assert receipt.event_timing == expected_timing
    assert receipt.event_timing_coverage == expected_coverage
    assert store.current_revision(epoch_id) == 1
    assert store.current_event_timing(epoch_id) == (
        expected_timing,
        expected_coverage,
    )

    before_replay = _full_snapshot(d24_requirement_db.connection)
    with d24_requirement_db.reconnect() as reconnected:
        replay = PostgresM5RuntimeStore(reconnected).append_transition_call_timing(
            epoch_id,
            contribution_kind,
            source_id,
            contribution_key,
            1,
            observed_timing,
        )
    assert replay.exact_replay and replay.resulting_revision == 1
    assert replay.event_timing == expected_timing
    assert replay.event_timing_coverage == expected_coverage
    assert _full_snapshot(d24_requirement_db.connection) == before_replay

    conflicting = None if observed_timing is not None else _attempt_timing()
    with pytest.raises(EventConflictError, match="immutable observation"):
        store.append_transition_call_timing(
            epoch_id,
            contribution_kind,
            source_id,
            contribution_key,
            1,
            conflicting,
        )
    assert _full_snapshot(d24_requirement_db.connection) == before_replay


def test_transition_timing_late_missing_replays_and_current_projection_is_read_only(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    source_id = d24_requirement_db.plan.structural_event_id
    contribution_kind = M5RuntimeWorkContributionKind.STRUCTURAL_OPEN
    contribution_key = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=contribution_kind,
        source_id=source_id,
    )
    lease = store.acquire_m5_job(epoch_id, 1, d24_requirement_db.jobs[0])
    assert lease.resulting_revision == 2

    before_read = _full_snapshot(d24_requirement_db.connection)
    timing, coverage = store.current_event_timing(epoch_id)
    assert timing == M5RuntimeTiming()
    assert coverage.required_expected_count == 2
    assert coverage.required_observed_count == 0
    assert coverage.required_missing_count == 2
    assert _full_snapshot(d24_requirement_db.connection) == before_read

    replay = store.append_transition_call_timing(
        epoch_id,
        contribution_kind,
        source_id,
        contribution_key,
        1,
        None,
    )
    assert replay.exact_replay and replay.resulting_revision == 2
    assert replay.event_timing_coverage == coverage
    assert _full_snapshot(d24_requirement_db.connection) == before_read
    with pytest.raises(EventConflictError, match="immutable observation"):
        store.append_transition_call_timing(
            epoch_id,
            contribution_kind,
            source_id,
            contribution_key,
            1,
            _attempt_timing(),
        )


@pytest.mark.parametrize(
    "forgery",
    ("contribution_kind", "source_id", "contribution_key", "anchor_revision"),
)
def test_transition_timing_forged_pending_identity_is_zero_write(
    d24_requirement_db: Any,
    forgery: str,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    contribution_kind = M5RuntimeWorkContributionKind.STRUCTURAL_OPEN
    source_id = d24_requirement_db.plan.structural_event_id
    contribution_key = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=contribution_kind,
        source_id=source_id,
    )
    anchor_revision = 1
    if forgery == "contribution_kind":
        contribution_kind = M5RuntimeWorkContributionKind.M5_ACQUISITION
        contribution_key = digests.runtime_work_contribution_key_digest(
            epoch_id=epoch_id,
            contribution_kind=contribution_kind,
            source_id=source_id,
        )
    elif forgery == "source_id":
        source_id = "forged-transition-source"
        contribution_key = digests.runtime_work_contribution_key_digest(
            epoch_id=epoch_id,
            contribution_kind=contribution_kind,
            source_id=source_id,
        )
    elif forgery == "contribution_key":
        contribution_key = _sha("forged-transition-key")
    else:
        assert forgery == "anchor_revision"
        anchor_revision = 2

    expected_error: type[Exception] = EventConflictError
    expected_message = "sole pending anchor"
    if forgery == "contribution_key":
        expected_error = ValidationError
        expected_message = "frozen digest recipe"
    before = _full_snapshot(d24_requirement_db.connection)
    with pytest.raises(expected_error, match=expected_message):
        store.append_transition_call_timing(
            epoch_id,
            contribution_kind,
            source_id,
            contribution_key,
            anchor_revision,
            _attempt_timing(),
        )
    assert _full_snapshot(d24_requirement_db.connection) == before


def test_transition_timing_partial_optional_coverage_is_explicit(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    event_id = d24_requirement_db.plan.structural_event_id
    open_key = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
        source_id=event_id,
    )
    first = _attempt_timing()
    store.append_transition_call_timing(
        epoch_id,
        M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
        event_id,
        open_key,
        1,
        first,
    )

    lease = store.acquire_m5_job(epoch_id, 1, d24_requirement_db.jobs[0])
    assert lease.attempt is not None and lease.resulting_revision == 2
    assert lease.dispatch_record_digest is not None
    source_id = lease.dispatch_record_digest
    acquisition_key = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.M5_ACQUISITION,
        source_id=source_id,
    )
    second = M5RuntimeTiming(
        coordinator_non_db_non_neural_ns=2,
        neural_wall_ns=3,
        postgres_roundtrip_wall_ns=5,
        external_io_wall_ns=7,
        end_to_end_wall_ns=11,
        postgres_server_execution_ns=41,
        postgres_lock_wait_ns=43,
        postgres_wal_bytes=None,
        postgres_shared_block_reads=47,
    )
    receipt = store.append_transition_call_timing(
        epoch_id,
        M5RuntimeWorkContributionKind.M5_ACQUISITION,
        source_id,
        acquisition_key,
        2,
        second,
    )

    assert not receipt.exact_replay and receipt.resulting_revision == 2
    assert receipt.event_timing == M5RuntimeTiming(
        coordinator_non_db_non_neural_ns=13,
        neural_wall_ns=16,
        postgres_roundtrip_wall_ns=22,
        external_io_wall_ns=26,
        end_to_end_wall_ns=34,
        postgres_server_execution_ns=70,
        postgres_lock_wait_ns=None,
        postgres_wal_bytes=None,
        postgres_shared_block_reads=None,
    )
    coverage = receipt.event_timing_coverage
    assert (
        coverage.required_expected_count,
        coverage.required_observed_count,
        coverage.required_missing_count,
    ) == (2, 2, 0)
    assert (
        coverage.postgres_server_execution_expected_count,
        coverage.postgres_server_execution_observed_count,
        coverage.postgres_server_execution_missing_count,
    ) == (2, 2, 0)
    assert (
        coverage.postgres_lock_wait_expected_count,
        coverage.postgres_lock_wait_observed_count,
        coverage.postgres_lock_wait_missing_count,
    ) == (2, 1, 1)
    assert (
        coverage.postgres_wal_bytes_expected_count,
        coverage.postgres_wal_bytes_observed_count,
        coverage.postgres_wal_bytes_missing_count,
    ) == (2, 1, 1)
    assert (
        coverage.postgres_shared_block_reads_expected_count,
        coverage.postgres_shared_block_reads_observed_count,
        coverage.postgres_shared_block_reads_missing_count,
    ) == (2, 1, 1)
    assert not coverage.terminal_client_roundtrip_included
    assert store.current_event_timing(epoch_id) == (
        receipt.event_timing,
        coverage,
    )


@pytest.mark.parametrize(
    "cutoff",
    ["transition_timing_inserted", "transition_timing_accumulator_updated"],
)
def test_transition_timing_append_cutoffs_roll_back_and_reconnect(
    d24_requirement_db: Any,
    monkeypatch: pytest.MonkeyPatch,
    cutoff: str,
) -> None:
    epoch_id = d24_requirement_db.epoch_id
    source_id = d24_requirement_db.plan.structural_event_id
    contribution_kind = M5RuntimeWorkContributionKind.STRUCTURAL_OPEN
    contribution_key = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=contribution_kind,
        source_id=source_id,
    )
    original: Any = persistence_module.__dict__["append_transition_call_timing_checked"]

    def fail_checked(*args: Any, **kwargs: Any) -> Any:
        def inject(point: str) -> None:
            if point == cutoff:
                raise RuntimeError(f"rollback {cutoff}")

        return original(*args, **kwargs, failure_injector=inject)

    monkeypatch.setattr(
        persistence_module,
        "append_transition_call_timing_checked",
        fail_checked,
    )
    before = _full_snapshot(d24_requirement_db.connection)
    with pytest.raises(RuntimeError, match=f"rollback {cutoff}"):
        PostgresM5RuntimeStore(
            d24_requirement_db.connection
        ).append_transition_call_timing(
            epoch_id,
            contribution_kind,
            source_id,
            contribution_key,
            1,
            _attempt_timing(),
        )
    assert _full_snapshot(d24_requirement_db.connection) == before

    monkeypatch.setattr(
        persistence_module,
        "append_transition_call_timing_checked",
        original,
    )
    with d24_requirement_db.reconnect() as reconnected:
        receipt = PostgresM5RuntimeStore(reconnected).append_transition_call_timing(
            epoch_id,
            contribution_kind,
            source_id,
            contribution_key,
            1,
            _attempt_timing(),
        )
    assert not receipt.exact_replay


def test_terminal_timing_hydration_and_invocation_telemetry(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    event_id = d24_requirement_db.plan.structural_event_id
    _cancel_requirement_job_for_setup(
        d24_requirement_db,
        job=d24_requirement_db.jobs[0],
        reason=M5TerminalReason.EPOCH_FAILED,
    )
    terminal_hash = terminalize_recovered_epoch_for_storage_fixture(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
    )

    before_read = _full_snapshot(d24_requirement_db.connection)
    result = store.read_typed_event_result(
        event_id,
        d24_requirement_db.plan.payload_hash,
    )
    assert result is not None and result.logical_result_hash == terminal_hash
    assert result.event_timing_coverage is not None
    assert result.call_timing == M5RuntimeTiming()
    assert result.call_timing_coverage == M5RuntimeTimingCoverage.single_point(
        None,
        terminal_client_roundtrip_included=False,
    )
    assert store.current_event_timing(epoch_id) == (
        result.event_timing,
        result.event_timing_coverage,
    )
    assert _full_snapshot(d24_requirement_db.connection) == before_read

    terminal_tables = tuple(
        table
        for table in _TERMINAL_STATE_TABLES
        if table != "groundloop_m5_postcommit_invocation_telemetry"
    )
    terminal_before = _snapshot_tables(
        d24_requirement_db.connection,
        terminal_tables,
    )
    observed = _attempt_timing()
    observed_coverage = M5RuntimeTimingCoverage.single_point(
        observed,
        terminal_client_roundtrip_included=True,
    )
    store.append_terminal_invocation_telemetry(
        "terminal-call-observed",
        event_id,
        epoch_id,
        terminal_hash,
        observed,
        observed_coverage,
    )
    assert _snapshot_tables(d24_requirement_db.connection, terminal_tables) == (
        terminal_before
    )
    replay_before = _full_snapshot(d24_requirement_db.connection)
    with d24_requirement_db.reconnect() as reconnected:
        PostgresM5RuntimeStore(reconnected).append_terminal_invocation_telemetry(
            "terminal-call-observed",
            event_id,
            epoch_id,
            terminal_hash,
            observed,
            observed_coverage,
        )
    assert _full_snapshot(d24_requirement_db.connection) == replay_before

    with pytest.raises(EventConflictError, match="conflicting immutable telemetry"):
        store.append_terminal_invocation_telemetry(
            "terminal-call-observed",
            event_id,
            epoch_id,
            terminal_hash,
            replace(observed, end_to_end_wall_ns=observed.end_to_end_wall_ns + 1),
            M5RuntimeTimingCoverage.single_point(
                replace(
                    observed,
                    end_to_end_wall_ns=observed.end_to_end_wall_ns + 1,
                ),
                terminal_client_roundtrip_included=True,
            ),
        )
    with pytest.raises(ValidationError, match="coverage is inconsistent"):
        store.append_terminal_invocation_telemetry(
            "terminal-call-bad-coverage",
            event_id,
            epoch_id,
            terminal_hash,
            None,
            observed_coverage,
        )
    store.append_terminal_invocation_telemetry(
        "terminal-call-missing",
        event_id,
        epoch_id,
        terminal_hash,
        None,
        M5RuntimeTimingCoverage.single_point(
            None,
            terminal_client_roundtrip_included=False,
        ),
    )
    telemetry = d24_requirement_db.connection.execute(
        """
        SELECT invocation_id, required_interval_observed,
               required_expected_count, required_observed_count,
               required_missing_count, terminal_client_roundtrip_included
        FROM groundloop_m5_postcommit_invocation_telemetry
        ORDER BY invocation_id COLLATE "C"
        """
    ).fetchall()
    assert tuple(telemetry) == (
        ("terminal-call-missing", False, 1, 0, 1, False),
        ("terminal-call-observed", True, 1, 1, 0, True),
    )


def test_terminal_invocation_telemetry_cutoff_rolls_back(
    d24_requirement_db: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    event_id = d24_requirement_db.plan.structural_event_id
    _cancel_requirement_job_for_setup(
        d24_requirement_db,
        job=d24_requirement_db.jobs[0],
        reason=M5TerminalReason.EPOCH_FAILED,
    )
    terminal_hash = terminalize_recovered_epoch_for_storage_fixture(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
    )
    timing = _attempt_timing()
    coverage = M5RuntimeTimingCoverage.single_point(
        timing,
        terminal_client_roundtrip_included=True,
    )
    original: Any = persistence_module.__dict__["persist_terminal_invocation_telemetry"]

    def fail_after_insert(*args: Any, **kwargs: Any) -> None:
        def inject(point: str) -> None:
            if point == "terminal_invocation_telemetry_inserted":
                raise RuntimeError("rollback terminal telemetry")

        original(*args, **kwargs, failure_injector=inject)

    monkeypatch.setattr(
        persistence_module,
        "persist_terminal_invocation_telemetry",
        fail_after_insert,
    )
    before = _full_snapshot(d24_requirement_db.connection)
    with pytest.raises(RuntimeError, match="rollback terminal telemetry"):
        store.append_terminal_invocation_telemetry(
            "terminal-call-cutoff",
            event_id,
            epoch_id,
            terminal_hash,
            timing,
            coverage,
        )
    assert _full_snapshot(d24_requirement_db.connection) == before

    monkeypatch.setattr(
        persistence_module,
        "persist_terminal_invocation_telemetry",
        original,
    )
    with d24_requirement_db.reconnect() as reconnected:
        PostgresM5RuntimeStore(reconnected).append_terminal_invocation_telemetry(
            "terminal-call-cutoff",
            event_id,
            epoch_id,
            terminal_hash,
            timing,
            coverage,
        )


@pytest.mark.parametrize("d24_requirement_db", ["two_roots"], indirect=True)
def test_cancellation_batch_is_point_accounted_and_exactly_replayable(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job_ids = tuple(job.logical_job_id for job in d24_requirement_db.jobs)
    plan = M5CancellationPlan.build(
        structural_event_id=d24_requirement_db.plan.structural_event_id,
        epoch_id=epoch_id,
        cancelled_job_ids=job_ids,
        reason=M5TerminalReason.SCOPE_RETIRED,
    )
    before = _full_snapshot(d24_requirement_db.connection)
    work_before = store.current_event_work(epoch_id)

    def fail_after_contribution(point: str) -> None:
        if point == "cancellation_contribution_inserted":
            raise RuntimeError("rollback cancellation")

    with pytest.raises(RuntimeError, match="rollback cancellation"):
        store.cancel_m5_work_atomically(
            epoch_id,
            1,
            plan,
            failure_injector=fail_after_contribution,
        )
    assert _full_snapshot(d24_requirement_db.connection) == before

    receipt = store.cancel_m5_work_atomically(epoch_id, 1, plan)
    assert receipt.cancelled_job_ids == job_ids
    assert receipt.resulting_revision == 2 and not receipt.exact_replay
    runtime = d24_requirement_db.connection.execute(
        """
        SELECT base.revision, runtime.revision, runtime.runtime_state,
               runtime.open_work_count, runtime.open_scope_count,
               runtime.blocking_failure_count
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        WHERE base.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert runtime == (2, 2, "semantic_pending", 0, 0, 0)

    job_rows = d24_requirement_db.connection.execute(
        """
        SELECT logical_job_id, job_state, archive_reason, completion_digest,
               cancelled_by_event_id, cancelled_by_epoch_id,
               cancellation_reason, completed_revision,
               result_artifact_id, result_artifact_hash,
               scope_closure_digest, child_set_hash
        FROM groundloop_m5_semantic_job
        WHERE epoch_id = %s
        ORDER BY logical_job_id COLLATE "C"
        """,
        (epoch_id,),
    ).fetchall()
    scope_rows = d24_requirement_db.connection.execute(
        """
        SELECT root_job_id, scope_state, completion_digest, closed_revision,
               scope_closure_digest, child_set_hash
        FROM groundloop_m5_discovery_scope
        WHERE epoch_id = %s
        ORDER BY root_job_id COLLATE "C"
        """,
        (epoch_id,),
    ).fetchall()
    for job, job_row, scope_row in zip(
        d24_requirement_db.jobs, job_rows, scope_rows, strict=True
    ):
        completion = M5JobCompletion.build(
            job=job,
            terminal_state=M5JobState.CANCELLED,
            archive_reason=M5TerminalReason.SCOPE_RETIRED,
        )
        assert tuple(job_row) == (
            job.logical_job_id,
            "cancelled",
            "scope_retired",
            completion.completion_digest,
            plan.structural_event_id,
            epoch_id,
            "scope_retired",
            2,
            None,
            None,
            None,
            None,
        )
        assert tuple(scope_row) == (
            job.logical_job_id,
            "cancelled",
            completion.completion_digest,
            2,
            None,
            None,
        )

    pending_rows = d24_requirement_db.connection.execute(
        """
        SELECT broad_reverse_scope_count, forward_scope_count,
               verifier_job_count, blocking_failure_count, updated_revision
        FROM groundloop_m5_owner_pending_counter
        WHERE epoch_id = %s
        UNION ALL
        SELECT broad_reverse_scope_count, forward_scope_count,
               verifier_job_count, blocking_failure_count, updated_revision
        FROM groundloop_m5_answer_pending_counter
        WHERE epoch_id = %s
        """,
        (epoch_id, epoch_id),
    ).fetchall()
    assert pending_rows and all(tuple(row) == (0, 0, 0, 0, 2) for row in pending_rows)

    work_names = M5RuntimeWork.counter_names()
    contribution = d24_requirement_db.connection.execute(
        sql.SQL(
            "SELECT {}, work_digest, source_identity_hash, "
            "contribution_key_digest, applied_revision "
            "FROM groundloop_m5_runtime_work_contribution "
            "WHERE epoch_id = %s AND contribution_kind = 'cancellation' "
            "AND source_id = %s"
        ).format(sql.SQL(", ").join(map(sql.Identifier, work_names))),
        (epoch_id, plan.plan_digest),
    ).fetchone()
    assert contribution is not None
    work_end = len(work_names)
    cancellation_work = M5RuntimeWork(requirement_cancelled_job_count=2)
    stored_work = M5RuntimeWork(
        **dict(zip(work_names, map(int, contribution[:work_end]), strict=True)),
        work_digest=str(contribution[work_end]).strip(),
    )
    contribution_key = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.CANCELLATION,
        source_id=plan.plan_digest,
    )
    assert stored_work == cancellation_work
    assert tuple(contribution[work_end + 1 :]) == (
        plan.plan_digest,
        contribution_key,
        2,
    )
    assert store.current_event_work(epoch_id) == _sum_work(
        work_before, cancellation_work
    )

    timing = d24_requirement_db.connection.execute(
        """
        SELECT required_expected_count, required_observed_count,
               required_missing_count,
               postgres_server_execution_expected_count,
               postgres_server_execution_observed_count,
               postgres_server_execution_missing_count,
               postgres_lock_wait_expected_count,
               postgres_lock_wait_observed_count,
               postgres_lock_wait_missing_count,
               postgres_wal_bytes_expected_count,
               postgres_wal_bytes_observed_count,
               postgres_wal_bytes_missing_count,
               postgres_shared_block_reads_expected_count,
               postgres_shared_block_reads_observed_count,
               postgres_shared_block_reads_missing_count,
               pending_contribution_kind, pending_source_id,
               pending_contribution_key_digest, pending_anchor_revision,
               updated_revision
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert timing == (
        2,
        0,
        1,
        2,
        0,
        1,
        2,
        0,
        1,
        2,
        0,
        1,
        2,
        0,
        1,
        "cancellation",
        plan.plan_digest,
        contribution_key,
        2,
        2,
    )

    before_replay = _full_snapshot(d24_requirement_db.connection)
    with pytest.raises(EventConflictError, match="newer than durable"):
        store.cancel_m5_work_atomically(epoch_id, 3, plan)
    assert _full_snapshot(d24_requirement_db.connection) == before_replay
    with d24_requirement_db.reconnect() as reconnected:
        replay = PostgresM5RuntimeStore(reconnected).cancel_m5_work_atomically(
            epoch_id, 1, plan
        )
    assert replay.cancelled_job_ids == job_ids
    assert replay.resulting_revision == 2 and replay.exact_replay
    assert _full_snapshot(d24_requirement_db.connection) == before_replay

    for conflicting_plan in (
        M5CancellationPlan.build(
            structural_event_id=plan.structural_event_id,
            epoch_id=epoch_id,
            cancelled_job_ids=job_ids,
            reason=M5TerminalReason.SUBJECT_INACTIVE,
        ),
        M5CancellationPlan.build(
            structural_event_id=plan.structural_event_id,
            epoch_id=epoch_id,
            cancelled_job_ids=(job_ids[0],),
            reason=plan.reason,
        ),
    ):
        with pytest.raises(EventConflictError, match="only open jobs"):
            store.cancel_m5_work_atomically(epoch_id, 2, conflicting_plan)
        assert _full_snapshot(d24_requirement_db.connection) == before_replay


def test_cancellation_clears_result_projection_and_retains_staged_sidecars(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    revision, _lease, result, _output = _acquire_and_stage_root(
        d24_requirement_db,
        store=store,
        expected_revision=1,
        job=job,
    )
    assert revision == 3
    immutable_tables = (
        "groundloop_m5_job_attempt",
        "groundloop_m5_attempt_result_artifact",
        "groundloop_m5_requirement_channel_hit",
        "groundloop_m5_requirement_scope_selection",
        "groundloop_m5_requirement_discovery_result",
    )
    sidecars_before = _snapshot_tables(d24_requirement_db.connection, immutable_tables)
    plan = M5CancellationPlan.build(
        structural_event_id=d24_requirement_db.plan.structural_event_id,
        epoch_id=epoch_id,
        cancelled_job_ids=(job.logical_job_id,),
        reason=M5TerminalReason.SCOPE_RETIRED,
    )
    receipt = store.cancel_m5_work_atomically(epoch_id, revision, plan)
    assert receipt.resulting_revision == 4 and not receipt.exact_replay

    completion = M5JobCompletion.build(
        job=job,
        terminal_state=M5JobState.CANCELLED,
        archive_reason=plan.reason,
    )
    projection = d24_requirement_db.connection.execute(
        """
        SELECT job.job_state, job.result_artifact_id, job.result_artifact_hash,
               job.scope_closure_digest, job.child_set_hash,
               job.archive_reason, job.completion_digest,
               job.cancelled_by_event_id, job.cancelled_by_epoch_id,
               job.cancellation_reason, job.completed_revision,
               scope.scope_state, scope.staged_result_artifact_hash,
               scope.staged_revision, scope.scope_closure_digest,
               scope.child_set_hash, scope.completion_digest,
               scope.closed_revision
        FROM groundloop_m5_semantic_job AS job
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.epoch_id = job.epoch_id
         AND scope.root_job_id = job.logical_job_id
        WHERE job.epoch_id = %s AND job.logical_job_id = %s
        """,
        (epoch_id, job.logical_job_id),
    ).fetchone()
    assert projection == (
        "cancelled",
        None,
        None,
        None,
        None,
        "scope_retired",
        completion.completion_digest,
        plan.structural_event_id,
        epoch_id,
        "scope_retired",
        4,
        "cancelled",
        result.result_artifact_hash,
        3,
        None,
        None,
        completion.completion_digest,
        4,
    )
    assert (
        _snapshot_tables(d24_requirement_db.connection, immutable_tables)
        == sidecars_before
    )

    before_reconnect = _full_snapshot(d24_requirement_db.connection)
    with d24_requirement_db.reconnect() as reconnected:
        reconnect_store = PostgresM5RuntimeStore(reconnected)
        terminal = reconnect_store.acquire_m5_job(epoch_id, revision, job)
        replay = reconnect_store.cancel_m5_work_atomically(epoch_id, revision, plan)
    assert terminal.disposition is M5AcquisitionDisposition.TERMINAL
    assert terminal.terminal_projection is not None
    assert terminal.terminal_projection.terminal_reason is plan.reason
    assert replay.resulting_revision == 4 and replay.exact_replay
    assert _full_snapshot(d24_requirement_db.connection) == before_reconnect


@pytest.mark.parametrize("d24_requirement_db", ["two_roots"], indirect=True)
def test_cancellation_replay_survives_later_open_job_progress(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    cancelled_job, successor_job = d24_requirement_db.jobs
    plan = M5CancellationPlan.build(
        structural_event_id=d24_requirement_db.plan.structural_event_id,
        epoch_id=epoch_id,
        cancelled_job_ids=(cancelled_job.logical_job_id,),
        reason=M5TerminalReason.SUBJECT_INACTIVE,
    )
    cancelled = store.cancel_m5_work_atomically(epoch_id, 1, plan)
    assert cancelled.resulting_revision == 2 and not cancelled.exact_replay
    successor = store.acquire_m5_job(epoch_id, 2, successor_job)
    assert (
        successor.disposition is M5AcquisitionDisposition.DISPATCH_NEW
        and successor.resulting_revision == 3
    )

    before_replay = _full_snapshot(d24_requirement_db.connection)
    replay = store.cancel_m5_work_atomically(epoch_id, 1, plan)
    assert replay.cancelled_job_ids == (cancelled_job.logical_job_id,)
    assert replay.resulting_revision == 3 and replay.exact_replay
    assert _full_snapshot(d24_requirement_db.connection) == before_replay
    assert d24_requirement_db.connection.execute(
        """
        SELECT count(*), min(applied_revision), max(applied_revision)
        FROM groundloop_m5_runtime_work_contribution
        WHERE epoch_id = %s AND contribution_kind = 'cancellation'
          AND source_id = %s
        """,
        (epoch_id, plan.plan_digest),
    ).fetchone() == (1, 2, 2)


def test_cancellation_rejects_pending_underflow_without_durable_writes(
    d24_requirement_db: Any,
) -> None:
    connection = d24_requirement_db.connection
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    plan = M5CancellationPlan.build(
        structural_event_id=d24_requirement_db.plan.structural_event_id,
        epoch_id=epoch_id,
        cancelled_job_ids=(job.logical_job_id,),
        reason=M5TerminalReason.SUBJECT_INACTIVE,
    )
    before = _full_snapshot(connection)
    connection.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
        (epoch_id, 1),
    )
    drifted = connection.execute(
        """
        UPDATE groundloop_m5_owner_pending_counter
        SET forward_scope_count = forward_scope_count - 1
        WHERE epoch_id = %s AND forward_scope_count > 0
        """,
        (epoch_id,),
    ).rowcount
    assert drifted == 1
    with pytest.raises(
        ValidationError, match="owner PENDING cancellation lost multiplicity"
    ):
        PostgresM5RuntimeStore(connection).cancel_m5_work_atomically(epoch_id, 1, plan)
    connection.rollback()
    assert _full_snapshot(connection) == before


def test_acquisition_restores_deferred_constraints_inside_ambient_transaction(
    d24_requirement_db: Any,
) -> None:
    """A nested acquisition must not make the following settlement immediate."""

    connection = d24_requirement_db.connection
    store = PostgresM5RuntimeStore(connection)
    job = d24_requirement_db.jobs[0]
    connection.execute("SELECT 1")
    lease = store.acquire_m5_job(d24_requirement_db.epoch_id, 1, job)
    assert lease.attempt is not None and lease.resulting_revision == 2
    result, output = _root_result_and_output(
        d24_requirement_db, job=job, attempt=lease.attempt
    )
    receipt = store.stage_m5_discovery_result_atomically(
        d24_requirement_db.epoch_id,
        lease.resulting_revision,
        lease,
        job,
        result,
        output,
        M5ExecutionEvidenceDisposition.RETURNED,
        _forward_attempt_work(),
        _attempt_timing(),
        eligible_snapshot_exhausted=True,
    )
    assert (
        receipt.disposition is M5RequirementReturnDisposition.APPLIED
        and receipt.resulting_revision == 3
    )
    connection.commit()
    assert connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_attempt_execution_evidence
           WHERE epoch_id = %s AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_runtime_timing_contribution
           WHERE epoch_id = %s AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_runtime_work_contribution
           WHERE epoch_id = %s AND contribution_kind = 'm5_attempt_execution'
             AND source_id = %s)
        """,
        (
            d24_requirement_db.epoch_id,
            lease.attempt.attempt_id,
            d24_requirement_db.epoch_id,
            lease.attempt.attempt_id,
            d24_requirement_db.epoch_id,
            lease.attempt.attempt_id,
        ),
    ).fetchone() == (1, 1, 1)


@pytest.mark.parametrize("d24_requirement_db", [False], indirect=True)
def test_pre016_open_rejects_before_event_or_epoch_id_consumption(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    before_sequence = d24_requirement_db.connection.execute(
        "SELECT last_value, is_called FROM groundloop_epoch_epoch_id_seq"
    ).fetchone()
    assert before_sequence is not None
    assert (
        store.read_typed_event_result(
            d24_requirement_db.plan.structural_event_id,
            d24_requirement_db.plan.payload_hash,
        )
        is None
    )

    with pytest.raises(InvalidEventError, match="exact accepted migration-016 bundle"):
        store.open_typed_event_atomically(
            d24_requirement_db.plan,
            recovery_operational_config=d24_requirement_db.operational_config,
            recovery_root_fallback_required={
                d24_requirement_db.jobs[0].logical_job_id: False
            },
        )
    with pytest.raises(InvalidEventError, match="exact accepted migration-016 bundle"):
        store.open_typed_event_atomically(d24_requirement_db.plan)

    assert d24_requirement_db.connection.execute(
        "SELECT count(*) FROM groundloop_epoch WHERE event_id = %s",
        (d24_requirement_db.plan.structural_event_id,),
    ).fetchone() == (0,)
    assert (
        d24_requirement_db.connection.execute(
            "SELECT last_value, is_called FROM groundloop_epoch_epoch_id_seq"
        ).fetchone()
        == before_sequence
    )
    assert (
        store.read_typed_event_result(
            d24_requirement_db.plan.structural_event_id,
            d24_requirement_db.plan.payload_hash,
        )
        is None
    )


def test_new_no_envelope_open_rejects_without_writes(
    d24_requirement_db: Any,
) -> None:
    event_id = f"{d24_requirement_db.plan.structural_event_id}-no-envelope"
    event = replace(d24_requirement_db.plan.event, event_id=event_id)
    plan = replace(
        d24_requirement_db.plan,
        structural_event_id=event_id,
        event=event,
        payload_hash=m5_event_payload_digest(event),
    )
    before = _snapshot(d24_requirement_db.connection)
    with pytest.raises(ValidationError, match="requires config and root provenance"):
        PostgresM5RuntimeStore(
            d24_requirement_db.connection
        ).open_typed_event_atomically(plan)
    assert _snapshot(d24_requirement_db.connection) == before
    assert d24_requirement_db.connection.execute(
        "SELECT count(*) FROM groundloop_epoch WHERE event_id = %s", (event_id,)
    ).fetchone() == (0,)


def test_read_only_audit_does_not_cross_the_recovery_tuple_barrier(
    d24_requirement_db: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden_guard(_cursor: Any) -> None:
        raise AssertionError("read-only audit crossed the recovery tuple barrier")

    monkeypatch.setattr(
        persistence_module, "require_runtime_recovery_bundle", forbidden_guard
    )
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    assert store.current_revision(d24_requirement_db.epoch_id) == 1
    assert store.verifier_jobs(d24_requirement_db.epoch_id) == ()
    assert not store.current_event_work(d24_requirement_db.epoch_id).is_zero
    assert (
        store.read_typed_event_result(
            d24_requirement_db.plan.structural_event_id,
            d24_requirement_db.plan.payload_hash,
        )
        is None
    )


def test_recovery_open_persists_exact_identity_nonzero_work_and_replays(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    work = store.current_event_work(epoch_id)
    assert work.bytes_hashed > 0
    assert work.bytes_serialized == work.bytes_hashed
    assert work == M5RuntimeWork(
        bytes_hashed=work.bytes_hashed,
        bytes_serialized=work.bytes_serialized,
    )

    expected_provenance = M5RequirementRootProvenance.build(
        epoch_id=epoch_id,
        root_job_id=job.logical_job_id,
        fallback_required=False,
    )
    rows = d24_requirement_db.connection.execute(
        """
        SELECT
          (SELECT lease_duration_ms
           FROM groundloop_m5_runtime_operational_config WHERE epoch_id = %s),
          (SELECT config_digest
           FROM groundloop_m5_runtime_operational_config WHERE epoch_id = %s),
          (SELECT root_job_id
           FROM groundloop_m5_requirement_root_provenance WHERE epoch_id = %s),
          (SELECT fallback_required
           FROM groundloop_m5_requirement_root_provenance WHERE epoch_id = %s),
          (SELECT provenance_digest
           FROM groundloop_m5_requirement_root_provenance WHERE epoch_id = %s),
          (SELECT source_id
           FROM groundloop_m5_runtime_work_contribution
           WHERE epoch_id = %s AND contribution_kind = 'structural_open'),
          (SELECT source_identity_hash
           FROM groundloop_m5_runtime_work_contribution
           WHERE epoch_id = %s AND contribution_kind = 'structural_open'),
          (SELECT work_digest
           FROM groundloop_m5_runtime_work_contribution
           WHERE epoch_id = %s AND contribution_kind = 'structural_open'),
          (SELECT applied_revision
           FROM groundloop_m5_runtime_work_contribution
           WHERE epoch_id = %s AND contribution_kind = 'structural_open'),
          (SELECT work_digest
           FROM groundloop_m5_runtime_work_accumulator WHERE epoch_id = %s),
          (SELECT updated_revision
           FROM groundloop_m5_runtime_work_accumulator WHERE epoch_id = %s),
          (SELECT terminalized
           FROM groundloop_m5_runtime_work_accumulator WHERE epoch_id = %s),
          (SELECT pending_contribution_kind
           FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id = %s),
          (SELECT pending_source_id
           FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id = %s),
          (SELECT pending_anchor_revision
           FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id = %s),
          (SELECT updated_revision
           FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id = %s),
          (SELECT terminalized
           FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id = %s)
        """,
        (epoch_id,) * 17,
    ).fetchone()
    assert rows is not None
    assert tuple(rows[:2]) == (
        d24_requirement_db.operational_config.lease_duration_ms,
        d24_requirement_db.operational_config.config_digest,
    )
    assert tuple(rows[2:5]) == (
        expected_provenance.root_job_id,
        expected_provenance.fallback_required,
        expected_provenance.provenance_digest,
    )
    assert tuple(rows[5:9]) == (
        d24_requirement_db.plan.structural_event_id,
        d24_requirement_db.plan.payload_hash,
        work.work_digest,
        1,
    )
    assert tuple(rows[9:12]) == (work.work_digest, 1, False)
    assert tuple(rows[12:]) == (
        "structural_open",
        d24_requirement_db.plan.structural_event_id,
        1,
        1,
        False,
    )

    before = _snapshot(d24_requirement_db.connection)
    replay = store.open_typed_event_atomically(
        d24_requirement_db.plan,
        recovery_operational_config=d24_requirement_db.operational_config,
        recovery_root_fallback_required={job.logical_job_id: False},
    )
    assert replay.replayed and replay.epoch_id == epoch_id
    assert _snapshot(d24_requirement_db.connection) == before


@pytest.mark.parametrize("d24_requirement_db", ["recovery_unopened"], indirect=True)
def test_recovery_open_initialization_cutoff_rolls_back_then_reconnects(
    d24_requirement_db: Any,
) -> None:
    """The recovery identity/accounting group is atomic with typed open."""

    connection = d24_requirement_db.connection
    store = PostgresM5RuntimeStore(connection)
    fallback = {job.logical_job_id: False for job in d24_requirement_db.jobs}
    before = _snapshot(connection)

    def fail(point: str) -> None:
        if point == "typed_open_recovery_initialized":
            raise RuntimeError("rollback typed_open_recovery_initialized")

    with pytest.raises(RuntimeError, match="rollback typed_open_recovery_initialized"):
        store.open_typed_event_atomically(
            d24_requirement_db.plan,
            recovery_operational_config=d24_requirement_db.operational_config,
            recovery_root_fallback_required=fallback,
            failure_injector=fail,
        )
    assert _snapshot(connection) == before

    with d24_requirement_db.reconnect() as reconnected:
        receipt = PostgresM5RuntimeStore(reconnected).open_typed_event_atomically(
            d24_requirement_db.plan,
            recovery_operational_config=d24_requirement_db.operational_config,
            recovery_root_fallback_required=fallback,
        )
        assert not receipt.replayed and receipt.epoch_id > 0

    after = _snapshot(connection)
    assert after != before
    replay = store.open_typed_event_atomically(
        d24_requirement_db.plan,
        recovery_operational_config=d24_requirement_db.operational_config,
        recovery_root_fallback_required=fallback,
    )
    assert replay.replayed and replay.epoch_id == receipt.epoch_id
    assert _snapshot(connection) == after


def test_recovery_open_rejects_partial_or_mismatched_replay_without_writes(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    job_id = d24_requirement_db.jobs[0].logical_job_id

    with pytest.raises(ValidationError, match="config and root provenance together"):
        store.open_typed_event_atomically(
            d24_requirement_db.plan,
            recovery_operational_config=d24_requirement_db.operational_config,
        )

    before = _snapshot(d24_requirement_db.connection)
    with pytest.raises(EventConflictError, match="differs on replay"):
        store.open_typed_event_atomically(
            d24_requirement_db.plan,
            recovery_operational_config=M5RuntimeOperationalConfig.build(301),
            recovery_root_fallback_required={job_id: False},
        )
    assert _snapshot(d24_requirement_db.connection) == before

    with pytest.raises(ValidationError, match="exactly cover the frozen root set"):
        store.open_typed_event_atomically(
            d24_requirement_db.plan,
            recovery_operational_config=d24_requirement_db.operational_config,
            recovery_root_fallback_required={job_id: False, "0" * 64: False},
        )
    assert _snapshot(d24_requirement_db.connection) == before

    with pytest.raises(EventConflictError, match="differs on replay"):
        store.open_typed_event_atomically(
            d24_requirement_db.plan,
            recovery_operational_config=d24_requirement_db.operational_config,
            recovery_root_fallback_required={job_id: True},
        )
    assert _snapshot(d24_requirement_db.connection) == before


def test_dispatch_live_and_takeover_are_total_and_point_maintained(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    job = d24_requirement_db.jobs[0]

    assert store.current_revision(d24_requirement_db.epoch_id) == 1
    dispatched = store.acquire_m5_job(d24_requirement_db.epoch_id, 1, job)
    assert dispatched.disposition is M5AcquisitionDisposition.DISPATCH_NEW
    assert dispatched.should_execute
    assert not dispatched.exact_replay
    assert dispatched.resulting_revision == 2
    assert dispatched.attempt is not None
    assert dispatched.attempt.attempt_ordinal == 1
    assert dispatched.attempt.attempt_work_digest == M5RuntimeWork().work_digest
    assert dispatched.lease_expires_at == dispatched.attempt.lease_expires_at
    assert dispatched.dispatch_record_digest is not None

    before_live = _snapshot(d24_requirement_db.connection)
    live = store.acquire_m5_job(d24_requirement_db.epoch_id, 1, job)
    assert live.disposition is M5AcquisitionDisposition.LIVE_LEASE
    assert not live.should_execute
    assert live.exact_replay
    assert live.resulting_revision == 2
    assert live.attempt == dispatched.attempt
    assert live.dispatch_record_digest == dispatched.dispatch_record_digest
    assert _snapshot(d24_requirement_db.connection) == before_live

    _wait_until_expired(d24_requirement_db.connection, dispatched.lease_expires_at)
    takeover = store.acquire_m5_job(d24_requirement_db.epoch_id, 2, job)
    assert takeover.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert takeover.should_execute
    assert not takeover.exact_replay
    assert takeover.resulting_revision == 3
    assert takeover.attempt is not None
    assert takeover.attempt.attempt_ordinal == 2
    assert takeover.attempt.attempt_id != dispatched.attempt.attempt_id
    assert takeover.attempt.lease_token_hash != dispatched.attempt.lease_token_hash

    attempt_rows = d24_requirement_db.connection.execute(
        """
        SELECT attempt_ordinal, attempt_state, attempt_work_digest,
               lease_expires_at > dispatched_at
        FROM groundloop_m5_job_attempt
        WHERE logical_job_id = %s
        ORDER BY attempt_ordinal
        """,
        (job.logical_job_id,),
    ).fetchall()
    assert attempt_rows == [
        (1, "expired", M5RuntimeWork().work_digest, True),
        (2, "dispatched", M5RuntimeWork().work_digest, True),
    ]
    accounting = d24_requirement_db.connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_dispatch_record
           WHERE epoch_id = %s AND subgraph = 'requirement'),
          (SELECT count(*) FROM groundloop_m5_runtime_work_contribution
           WHERE epoch_id = %s AND contribution_kind = 'm5_acquisition'),
          work.updated_revision,
          timing.updated_revision,
          timing.required_expected_count,
          timing.required_observed_count,
          timing.required_missing_count,
          timing.pending_anchor_revision,
          timing.pending_contribution_kind,
          (SELECT count(*) FROM groundloop_m5_transition_call_timing
           WHERE epoch_id = %s AND NOT required_interval_observed)
        FROM groundloop_m5_runtime_work_accumulator AS work
        JOIN groundloop_m5_runtime_timing_accumulator AS timing
          ON timing.epoch_id = work.epoch_id
        WHERE work.epoch_id = %s
        """,
        (
            d24_requirement_db.epoch_id,
            d24_requirement_db.epoch_id,
            d24_requirement_db.epoch_id,
            d24_requirement_db.epoch_id,
        ),
    ).fetchone()
    assert accounting == (2, 2, 3, 3, 3, 0, 2, 3, "m5_acquisition", 2)

    before_replay = _snapshot(d24_requirement_db.connection)
    replay = store.open_typed_event_atomically(
        d24_requirement_db.plan,
        recovery_operational_config=d24_requirement_db.operational_config,
        recovery_root_fallback_required={job.logical_job_id: False},
    )
    assert replay.replayed and replay.epoch_id == d24_requirement_db.epoch_id
    assert _snapshot(d24_requirement_db.connection) == before_replay


def test_deadline_equality_is_takeover_eligible(
    d24_requirement_db: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    job = d24_requirement_db.jobs[0]
    first = store.acquire_m5_job(d24_requirement_db.epoch_id, 1, job)
    assert first.attempt is not None
    assert first.lease_expires_at is not None
    config = M5RuntimeOperationalConfig.build(300)
    equality_clock = AcquisitionClock(
        decision_time=first.lease_expires_at,
        lease_expires_at=first.lease_expires_at
        + timedelta(milliseconds=config.lease_duration_ms),
        config=config,
    )
    monkeypatch.setattr(
        persistence_module,
        "read_acquisition_clock",
        lambda _cursor, *, epoch_id: equality_clock,
    )

    successor = store.acquire_m5_job(d24_requirement_db.epoch_id, 2, job)
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert successor.attempt is not None
    assert successor.attempt.attempt_ordinal == 2


def test_committed_result_reserved_attempt_is_an_invariant_failure_with_no_writes(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    job = d24_requirement_db.jobs[0]
    lease = store.acquire_m5_job(d24_requirement_db.epoch_id, 1, job)
    assert lease.attempt is not None
    with d24_requirement_db.connection.transaction():
        d24_requirement_db.connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (d24_requirement_db.epoch_id, 2),
        )
        d24_requirement_db.connection.execute(
            """
            UPDATE groundloop_m5_job_attempt
            SET attempt_state = 'result_reserved', attempt_output_digest = %s
            WHERE attempt_id = %s AND attempt_state = 'dispatched'
            """,
            (_sha("committed-reservation"), lease.attempt.attempt_id),
        )
        d24_requirement_db.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

    before = _snapshot(d24_requirement_db.connection)
    with pytest.raises(
        ValidationError, match="committed result_reserved M5 attempt is invalid"
    ):
        store.acquire_m5_job(d24_requirement_db.epoch_id, 2, job)
    assert _snapshot(d24_requirement_db.connection) == before


@pytest.mark.parametrize("d24_requirement_db", ["two_roots"], indirect=True)
@pytest.mark.parametrize(
    "cutoff",
    (
        "root_stage_authorized",
        "root_stage_output_reserved",
        "root_stage_attempt_artifact_inserted",
        "root_stage_hits_inserted",
        "root_stage_selections_inserted",
        "root_stage_result_inserted",
        "root_stage_scope_transitioned",
        "root_stage_attempt_completed",
        "root_stage_revision_advanced",
        "root_stage_constraints_validated",
    ),
)
def test_root_result_stage_all_cutoffs_roll_back_then_retry_on_fresh_connection(
    d24_requirement_db: Any,
    cutoff: str,
) -> None:
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    lease = store.acquire_m5_job(epoch_id, 1, job)
    assert lease.attempt is not None and lease.should_execute
    result, output = _root_result_and_output(
        d24_requirement_db,
        job=job,
        attempt=lease.attempt,
    )
    before = _full_snapshot(d24_requirement_db.connection)

    def fail_at_cutoff(point: str) -> None:
        if point == cutoff:
            raise RuntimeError(f"rollback {cutoff}")

    with pytest.raises(RuntimeError, match=f"rollback {cutoff}"):
        store.stage_m5_discovery_result_atomically(
            epoch_id,
            lease.resulting_revision,
            lease,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
            failure_injector=fail_at_cutoff,
        )

    with d24_requirement_db.reconnect() as reconnected:
        assert _full_snapshot(reconnected) == before
        fresh_store = PostgresM5RuntimeStore(reconnected)
        receipt = fresh_store.stage_m5_discovery_result_atomically(
            epoch_id,
            lease.resulting_revision,
            lease,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )
        assert receipt.disposition is M5RequirementReturnDisposition.APPLIED
        assert receipt.resulting_revision == 3 and not receipt.exact_replay
        assert fresh_store.current_revision(epoch_id) == 3


@pytest.mark.parametrize("d24_requirement_db", ["two_roots"], indirect=True)
@pytest.mark.parametrize(
    "cutoff",
    (
        "root_barrier_authorized",
        "root_barrier_admitted_pairs_inserted",
        "root_barrier_children_inserted",
        "root_barrier_jobs_completed",
        "root_barrier_scopes_closed",
        "root_barrier_frontiers_updated",
        "root_barrier_pending_recomputed",
        "root_barrier_revision_advanced",
        "root_barrier_contribution_inserted",
        "root_barrier_accounting_finished",
        "root_barrier_constraints_validated",
    ),
)
def test_root_barrier_all_cutoffs_roll_back_then_retry_on_fresh_connection(
    d24_requirement_db: Any,
    cutoff: str,
) -> None:
    epoch_id = d24_requirement_db.epoch_id
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    revision = 1
    for job in d24_requirement_db.jobs:
        revision, _lease, _result, _output = _acquire_and_stage_root(
            d24_requirement_db,
            store=store,
            expected_revision=revision,
            job=job,
        )
    assert revision == 5
    root_set_row = d24_requirement_db.connection.execute(
        """
        SELECT requirement_root_set_hash
        FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert root_set_row is not None
    root_set_hash = str(root_set_row[0]).strip()
    before = _full_snapshot(d24_requirement_db.connection)

    def fail_at_cutoff(point: str) -> None:
        if point == cutoff:
            raise RuntimeError(f"rollback {cutoff}")

    with pytest.raises(RuntimeError, match=f"rollback {cutoff}"):
        store.close_m5_requirement_roots_atomically(
            epoch_id,
            revision,
            root_set_hash,
            failure_injector=fail_at_cutoff,
        )

    with d24_requirement_db.reconnect() as reconnected:
        assert _full_snapshot(reconnected) == before
        fresh_store = PostgresM5RuntimeStore(reconnected)
        receipt = fresh_store.close_m5_requirement_roots_atomically(
            epoch_id,
            revision,
            root_set_hash,
        )
        assert receipt.resulting_revision == 6 and not receipt.exact_replay
        assert fresh_store.current_revision(epoch_id) == 6


@pytest.mark.parametrize("d24_requirement_db", ["two_roots"], indirect=True)
@pytest.mark.parametrize(
    "cutoff",
    (
        "cancellation_authorized",
        "cancellation_jobs_closed",
        "cancellation_scopes_closed",
        "cancellation_pending_recomputed",
        "cancellation_revision_advanced",
        "cancellation_contribution_inserted",
        "cancellation_accounting_finished",
        "cancellation_constraints_validated",
    ),
)
def test_cancellation_all_cutoffs_roll_back_then_retry_on_fresh_connection(
    d24_requirement_db: Any,
    cutoff: str,
) -> None:
    epoch_id = d24_requirement_db.epoch_id
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    plan = M5CancellationPlan.build(
        structural_event_id=d24_requirement_db.plan.structural_event_id,
        epoch_id=epoch_id,
        cancelled_job_ids=tuple(job.logical_job_id for job in d24_requirement_db.jobs),
        reason=M5TerminalReason.SCOPE_RETIRED,
    )
    before = _full_snapshot(d24_requirement_db.connection)

    def fail_at_cutoff(point: str) -> None:
        if point == cutoff:
            raise RuntimeError(f"rollback {cutoff}")

    with pytest.raises(RuntimeError, match=f"rollback {cutoff}"):
        store.cancel_m5_work_atomically(
            epoch_id,
            1,
            plan,
            failure_injector=fail_at_cutoff,
        )

    with d24_requirement_db.reconnect() as reconnected:
        assert _full_snapshot(reconnected) == before
        fresh_store = PostgresM5RuntimeStore(reconnected)
        receipt = fresh_store.cancel_m5_work_atomically(epoch_id, 1, plan)
        assert receipt.cancelled_job_ids == plan.cancelled_job_ids
        assert receipt.resulting_revision == 2 and not receipt.exact_replay
        assert fresh_store.current_revision(epoch_id) == 2


@pytest.mark.parametrize("d24_requirement_db", ["two_roots"], indirect=True)
@pytest.mark.parametrize(
    "execution_disposition",
    (
        M5ExecutionEvidenceDisposition.RETURNED,
        M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
    ),
)
def test_root_result_stage_has_explicit_evidence_one_anchor_and_exact_replay(
    d24_requirement_db: Any,
    execution_disposition: M5ExecutionEvidenceDisposition,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    lease = store.acquire_m5_job(epoch_id, 1, job)
    assert lease.attempt is not None
    result = _root_result(d24_requirement_db, job=job)
    output = M5AttemptOutput.build(
        attempt=lease.attempt,
        job_epoch_id=epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=result.result_artifact_id,
        result_artifact_hash=result.result_artifact_hash,
    )
    attempt_work = (
        _forward_attempt_work()
        if execution_disposition is M5ExecutionEvidenceDisposition.RETURNED
        else M5RuntimeWork()
    )
    attempt_timing = _attempt_timing()
    for tampered_lease in (
        replace(lease, dispatch_record_digest=_sha("forged-root-dispatch")),
        replace(lease, resulting_revision=1),
    ):
        before_dispatch_conflict = _full_snapshot(d24_requirement_db.connection)
        with pytest.raises(EventConflictError, match="dispatch identity"):
            store.stage_m5_discovery_result_atomically(
                epoch_id,
                2,
                tampered_lease,
                job,
                result,
                output,
                execution_disposition,
                attempt_work,
                attempt_timing,
                eligible_snapshot_exhausted=True,
            )
        assert _full_snapshot(d24_requirement_db.connection) == before_dispatch_conflict
    if execution_disposition is M5ExecutionEvidenceDisposition.REUSED_ARTIFACT:
        before_rejection = _snapshot(d24_requirement_db.connection)
        with pytest.raises(
            ValidationError, match="reused-artifact evidence cannot charge"
        ):
            store.stage_m5_discovery_result_atomically(
                epoch_id,
                2,
                lease,
                job,
                result,
                output,
                execution_disposition,
                _forward_attempt_work(),
                attempt_timing,
                eligible_snapshot_exhausted=True,
            )
        assert _snapshot(d24_requirement_db.connection) == before_rejection

    work_before_stage = store.current_event_work(epoch_id)
    receipt = store.stage_m5_discovery_result_atomically(
        epoch_id,
        2,
        lease,
        job,
        result,
        output,
        execution_disposition,
        attempt_work,
        attempt_timing,
        eligible_snapshot_exhausted=True,
    )
    assert receipt.disposition is M5RequirementReturnDisposition.APPLIED
    assert receipt.resulting_revision == 3 and not receipt.exact_replay
    assert receipt.current_terminal_logical_result_hash is None
    assert receipt.transition_anchor is not None
    assert receipt.transition_anchor.contribution_kind.value == "root_result_stage"
    assert receipt.transition_anchor.source_id == lease.attempt.attempt_id
    assert receipt.transition_anchor.anchor_revision == 3

    row = d24_requirement_db.connection.execute(
        """
        SELECT evidence.disposition, attempt.attempt_state,
               attempt.attempt_output_digest, attempt.attempt_work_digest,
               scope.scope_state, root_work.requirement_channel_hit_count,
               root_work.requirement_pre_dedup_selection_count,
               root_work.bytes_hashed, root_work.bytes_serialized,
               root_work.source_identity_hash,
               timing.pending_contribution_kind, timing.pending_source_id,
               timing.pending_anchor_revision
        FROM groundloop_m5_attempt_execution_evidence AS evidence
        JOIN groundloop_m5_job_attempt AS attempt
          ON attempt.attempt_id = evidence.attempt_id
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.root_job_id = attempt.logical_job_id
        JOIN groundloop_m5_runtime_work_contribution AS root_work
          ON root_work.epoch_id = evidence.epoch_id
         AND root_work.contribution_kind = 'root_result_stage'
         AND root_work.source_id = evidence.attempt_id
        JOIN groundloop_m5_runtime_timing_accumulator AS timing
          ON timing.epoch_id = evidence.epoch_id
        WHERE evidence.epoch_id = %s AND evidence.attempt_id = %s
        """,
        (epoch_id, lease.attempt.attempt_id),
    ).fetchone()
    assert row is not None
    assert row[:7] == (
        execution_disposition.value,
        "completed",
        output.attempt_output_digest,
        attempt_work.work_digest,
        "result_staged",
        1,
        1,
    )
    assert row[7] > 0 and row[8] == row[7]
    assert row[9:] == (
        output.attempt_output_digest,
        "root_result_stage",
        lease.attempt.attempt_id,
        3,
    )
    stage_work = M5RuntimeWork(
        requirement_channel_hit_count=1,
        requirement_pre_dedup_selection_count=1,
        bytes_hashed=row[7],
        bytes_serialized=row[8],
    )
    expected_event_work = _sum_work(work_before_stage, attempt_work, stage_work)
    assert store.current_event_work(epoch_id) == expected_event_work
    work_accumulator = d24_requirement_db.connection.execute(
        """
        SELECT work_digest, updated_revision, terminalized
        FROM groundloop_m5_runtime_work_accumulator
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert work_accumulator == (expected_event_work.work_digest, 3, False)

    timing_accumulator = d24_requirement_db.connection.execute(
        """
        SELECT required_expected_count, required_observed_count,
               required_missing_count,
               postgres_server_execution_ns,
               postgres_server_execution_expected_count,
               postgres_server_execution_observed_count,
               postgres_server_execution_missing_count,
               postgres_lock_wait_ns,
               postgres_lock_wait_expected_count,
               postgres_lock_wait_observed_count,
               postgres_lock_wait_missing_count,
               postgres_wal_bytes,
               postgres_wal_bytes_expected_count,
               postgres_wal_bytes_observed_count,
               postgres_wal_bytes_missing_count,
               postgres_shared_block_reads,
               postgres_shared_block_reads_expected_count,
               postgres_shared_block_reads_observed_count,
               postgres_shared_block_reads_missing_count,
               pending_contribution_kind, pending_source_id,
               pending_anchor_revision, updated_revision
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert timing_accumulator == (
        4,
        1,
        2,
        attempt_timing.postgres_server_execution_ns,
        4,
        1,
        2,
        0,
        4,
        0,
        3,
        attempt_timing.postgres_wal_bytes,
        4,
        1,
        2,
        0,
        4,
        0,
        3,
        "root_result_stage",
        lease.attempt.attempt_id,
        3,
        3,
    )

    before_reserved = _snapshot(d24_requirement_db.connection)
    reserved = store.acquire_m5_job(epoch_id, 3, job)
    assert reserved.disposition is M5AcquisitionDisposition.RESULT_RESERVED
    assert _snapshot(d24_requirement_db.connection) == before_reserved
    other_job = next(
        candidate
        for candidate in d24_requirement_db.jobs
        if candidate.logical_job_id != job.logical_job_id
    )
    successor = store.acquire_m5_job(epoch_id, 3, other_job)
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_NEW
    assert successor.resulting_revision == 4
    before_replay = _snapshot(d24_requirement_db.connection)
    with d24_requirement_db.reconnect() as reconnected:
        replay = PostgresM5RuntimeStore(
            reconnected
        ).stage_m5_discovery_result_atomically(
            epoch_id,
            4,
            lease,
            job,
            result,
            output,
            execution_disposition,
            attempt_work,
            attempt_timing,
            eligible_snapshot_exhausted=True,
        )
    assert replay.disposition is M5RequirementReturnDisposition.APPLIED
    assert replay.exact_replay and replay.resulting_revision == 4
    assert replay.transition_anchor is None
    assert _snapshot(d24_requirement_db.connection) == before_replay

    other_disposition = (
        M5ExecutionEvidenceDisposition.REUSED_ARTIFACT
        if execution_disposition is M5ExecutionEvidenceDisposition.RETURNED
        else M5ExecutionEvidenceDisposition.RETURNED
    )
    with pytest.raises(EventConflictError, match="immutable evidence"):
        store.stage_m5_discovery_result_atomically(
            epoch_id,
            4,
            lease,
            job,
            result,
            output,
            other_disposition,
            M5RuntimeWork(),
            attempt_timing,
            eligible_snapshot_exhausted=True,
        )
    assert _snapshot(d24_requirement_db.connection) == before_replay


@pytest.mark.parametrize("d24_requirement_db", ["two_roots"], indirect=True)
def test_root_barrier_is_point_accounted_and_replays_after_verifier_acquisition(
    d24_requirement_db: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    revision = 1
    for job in d24_requirement_db.jobs:
        revision, _lease, _result, _output = _acquire_and_stage_root(
            d24_requirement_db,
            store=store,
            expected_revision=revision,
            job=job,
        )
    assert revision == 5
    root_set_row = d24_requirement_db.connection.execute(
        """
        SELECT requirement_root_set_hash
        FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert root_set_row is not None
    root_set_hash = str(root_set_row[0]).strip()

    before_wrong_hash = _full_snapshot(d24_requirement_db.connection)
    with pytest.raises(EventConflictError, match="root-set hash"):
        store.close_m5_requirement_roots_atomically(
            epoch_id, revision, _sha("wrong-root-set")
        )
    assert _full_snapshot(d24_requirement_db.connection) == before_wrong_hash

    work_before = store.current_event_work(epoch_id)
    timing_before = d24_requirement_db.connection.execute(
        """
        SELECT required_expected_count, required_observed_count,
               required_missing_count,
               postgres_server_execution_expected_count,
               postgres_server_execution_observed_count,
               postgres_server_execution_missing_count,
               postgres_lock_wait_expected_count,
               postgres_lock_wait_observed_count,
               postgres_lock_wait_missing_count,
               postgres_wal_bytes_expected_count,
               postgres_wal_bytes_observed_count,
               postgres_wal_bytes_missing_count,
               postgres_shared_block_reads_expected_count,
               postgres_shared_block_reads_observed_count,
               postgres_shared_block_reads_missing_count
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert timing_before is not None

    def fail_after_contribution(point: str) -> None:
        if point == "root_barrier_contribution_inserted":
            raise RuntimeError("rollback root barrier")

    with pytest.raises(RuntimeError, match="rollback root barrier"):
        store.close_m5_requirement_roots_atomically(
            epoch_id,
            revision,
            root_set_hash,
            failure_injector=fail_after_contribution,
        )
    assert _full_snapshot(d24_requirement_db.connection) == before_wrong_hash

    with d24_requirement_db.reconnect() as reconnected:
        barrier = PostgresM5RuntimeStore(
            reconnected
        ).close_m5_requirement_roots_atomically(epoch_id, revision, root_set_hash)
    assert not barrier.exact_replay and barrier.resulting_revision == 6
    work_names = M5RuntimeWork.counter_names()
    work_row = d24_requirement_db.connection.execute(
        sql.SQL(
            """
        SELECT {}, contribution.work_digest, contribution.source_id,
               contribution.source_identity_hash,
               contribution.contribution_key_digest,
               contribution.applied_revision, accumulator.work_digest,
               accumulator.updated_revision, accumulator.terminalized
        FROM groundloop_m5_runtime_work_contribution AS contribution
        JOIN groundloop_m5_runtime_work_accumulator AS accumulator
          USING (epoch_id)
        WHERE contribution.epoch_id = %s
          AND contribution.contribution_kind = 'root_barrier'
        """
        ).format(
            sql.SQL(", ").join(
                sql.SQL("contribution.{}").format(sql.Identifier(name))
                for name in work_names
            )
        ),
        (epoch_id,),
    ).fetchone()
    assert work_row is not None
    work_end = len(work_names)
    stored_barrier_work = M5RuntimeWork(
        **dict(zip(work_names, map(int, work_row[:work_end]), strict=True)),
        work_digest=str(work_row[work_end]),
    )
    expected_byte_count = _root_barrier_projection_byte_count(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        barrier_completion_hash=barrier.barrier_completion_hash,
    )
    barrier_work = M5RuntimeWork(
        requirement_admitted_pair_count=2,
        bytes_hashed=expected_byte_count,
        bytes_serialized=expected_byte_count,
    )
    assert stored_barrier_work == barrier_work
    barrier_owned = {
        "requirement_admitted_pair_count",
        "bytes_hashed",
        "bytes_serialized",
    }
    assert all(
        getattr(stored_barrier_work, name) == 0
        for name in work_names
        if name not in barrier_owned
    )
    expected_key = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.ROOT_BARRIER,
        source_id=d24_requirement_db.plan.structural_event_id,
    )
    assert work_row[work_end + 1 : work_end + 5] == (
        d24_requirement_db.plan.structural_event_id,
        barrier.barrier_completion_hash,
        expected_key,
        6,
    )
    expected_work = _sum_work(work_before, barrier_work)
    assert work_row[work_end + 5 :] == (expected_work.work_digest, 6, False)
    assert store.current_event_work(epoch_id) == expected_work

    timing_after = d24_requirement_db.connection.execute(
        """
        SELECT required_expected_count, required_observed_count,
               required_missing_count,
               postgres_server_execution_expected_count,
               postgres_server_execution_observed_count,
               postgres_server_execution_missing_count,
               postgres_lock_wait_expected_count,
               postgres_lock_wait_observed_count,
               postgres_lock_wait_missing_count,
               postgres_wal_bytes_expected_count,
               postgres_wal_bytes_observed_count,
               postgres_wal_bytes_missing_count,
               postgres_shared_block_reads_expected_count,
               postgres_shared_block_reads_observed_count,
               postgres_shared_block_reads_missing_count,
               pending_contribution_kind, pending_source_id,
               pending_contribution_key_digest, pending_anchor_revision,
               updated_revision
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert timing_after is not None
    assert (
        tuple(
            int(after) - int(before)
            for before, after in zip(timing_before, timing_after[:15], strict=True)
        )
        == (1, 0, 1) * 5
    )
    assert timing_after[15:] == (
        "root_barrier",
        d24_requirement_db.plan.structural_event_id,
        expected_key,
        6,
        6,
    )
    assert d24_requirement_db.connection.execute(
        """
        SELECT contribution_kind, required_interval_observed
        FROM groundloop_m5_transition_call_timing
        WHERE epoch_id = %s AND contribution_kind = 'root_result_stage'
          AND anchor_revision = 5
        """,
        (epoch_id,),
    ).fetchone() == ("root_result_stage", False)
    closures = d24_requirement_db.connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_semantic_job
           WHERE epoch_id = %s AND parent_job_id IS NULL
             AND job_state = 'completed_active' AND completed_revision = 6),
          (SELECT count(*) FROM groundloop_m5_discovery_scope
           WHERE epoch_id = %s AND scope_state = 'closed_active'
             AND closed_revision = 6),
          (SELECT count(*) FROM groundloop_m5_semantic_job
           WHERE epoch_id = %s AND parent_job_id IS NOT NULL
             AND job_state = 'declared' AND created_revision = 6),
          (SELECT count(*) FROM groundloop_m5_job_dependency
           WHERE epoch_id = %s)
        """,
        (epoch_id, epoch_id, epoch_id, epoch_id),
    ).fetchone()
    assert closures == (2, 2, 2, 2)
    owner_pending = d24_requirement_db.connection.execute(
        """
        SELECT broad_reverse_scope_count, forward_scope_count,
               verifier_job_count, blocking_failure_count, updated_revision
        FROM groundloop_m5_owner_pending_counter WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    answer_pending = d24_requirement_db.connection.execute(
        """
        SELECT broad_reverse_scope_count, forward_scope_count,
               verifier_job_count, blocking_failure_count, updated_revision
        FROM groundloop_m5_answer_pending_counter WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert owner_pending == answer_pending == (0, 0, 2, 0, 6)

    before_future = _full_snapshot(d24_requirement_db.connection)
    with pytest.raises(EventConflictError, match="newer than durable runtime"):
        store.close_m5_requirement_roots_atomically(
            epoch_id, barrier.resulting_revision + 1, root_set_hash
        )
    assert _full_snapshot(d24_requirement_db.connection) == before_future

    before_replay = _full_snapshot(d24_requirement_db.connection)
    replay = store.close_m5_requirement_roots_atomically(
        epoch_id, revision, root_set_hash
    )
    assert replay.exact_replay and replay.resulting_revision == 6
    assert replay.barrier_completion_hash == barrier.barrier_completion_hash
    assert _full_snapshot(d24_requirement_db.connection) == before_replay

    verifier_job = store.verifier_jobs(epoch_id)[0]
    verifier_lease = store.acquire_m5_job(epoch_id, 6, verifier_job)
    assert (
        verifier_lease.disposition is M5AcquisitionDisposition.DISPATCH_NEW
        and verifier_lease.resulting_revision == 7
    )
    before_later_replay = _full_snapshot(d24_requirement_db.connection)
    later_replay = store.close_m5_requirement_roots_atomically(
        epoch_id, revision, root_set_hash
    )
    assert later_replay.exact_replay and later_replay.resulting_revision == 7
    assert _full_snapshot(d24_requirement_db.connection) == before_later_replay
    before_later_future = _full_snapshot(d24_requirement_db.connection)
    with pytest.raises(EventConflictError, match="newer than durable runtime"):
        store.close_m5_requirement_roots_atomically(epoch_id, 8, root_set_hash)
    assert _full_snapshot(d24_requirement_db.connection) == before_later_future

    original_load = postgres_roots_module._load_frontier_head

    def load_regressed_frontier(*args: Any, **kwargs: Any) -> Any:
        current = original_load(*args, **kwargs)
        assert current is not None
        return replace(current, completed_revision=current.completed_revision - 1)

    monkeypatch.setattr(
        postgres_roots_module, "_load_frontier_head", load_regressed_frontier
    )
    before_regressed = _full_snapshot(d24_requirement_db.connection)
    with pytest.raises(EventConflictError, match="frontier regressed"):
        store.close_m5_requirement_roots_atomically(epoch_id, revision, root_set_hash)
    assert _full_snapshot(d24_requirement_db.connection) == before_regressed


def test_inactive_root_stage_remains_result_reserved_on_fresh_reconnect(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    lease = store.acquire_m5_job(epoch_id, 1, job)
    assert lease.attempt is not None

    group = d24_requirement_db.plan.event.group
    d24_requirement_db.connection.execute(
        """
        UPDATE groundloop_m5_requirement_version
        SET lifecycle_state = 'FAILED'
        WHERE group_version_id = %s AND lifecycle_state = 'STAGED'
        """,
        (group.group_version_id,),
    )
    d24_requirement_db.connection.execute(
        """
        UPDATE groundloop_m5_group_version
        SET lifecycle_state = 'FAILED'
        WHERE group_version_id = %s AND lifecycle_state = 'STAGED'
        """,
        (group.group_version_id,),
    )
    d24_requirement_db.connection.execute(
        """
        UPDATE groundloop_m5_group_family
        SET lifecycle_state = 'FAILED'
        WHERE group_family_id = %s AND lifecycle_state = 'STAGED'
        """,
        (group.group_family_id,),
    )
    d24_requirement_db.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    d24_requirement_db.connection.commit()

    result = _root_result(d24_requirement_db, job=job)
    output = M5AttemptOutput.build(
        attempt=lease.attempt,
        job_epoch_id=epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=result.result_artifact_id,
        result_artifact_hash=result.result_artifact_hash,
    )
    receipt = store.stage_m5_discovery_result_atomically(
        epoch_id,
        2,
        lease,
        job,
        result,
        output,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        _attempt_timing(),
        eligible_snapshot_exhausted=True,
    )
    assert receipt.resulting_revision == 3
    artifact_activity = d24_requirement_db.connection.execute(
        """
        SELECT archive_reason, epoch_active, chunk_active,
               requirement_active, group_active
        FROM groundloop_m5_attempt_result_artifact
        WHERE attempt_id = %s
        """,
        (lease.attempt.attempt_id,),
    ).fetchone()
    assert artifact_activity == ("subject_inactive", True, None, False, False)

    before_reserved = _snapshot(d24_requirement_db.connection)
    with d24_requirement_db.reconnect() as reconnected:
        reserved = PostgresM5RuntimeStore(reconnected).acquire_m5_job(epoch_id, 3, job)
    assert reserved.disposition is M5AcquisitionDisposition.RESULT_RESERVED
    assert reserved.resulting_revision == 3 and reserved.exact_replay
    assert _snapshot(d24_requirement_db.connection) == before_reserved


def test_retryable_failure_is_exact_accounted_and_replays_after_successor(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    structural_work = store.current_event_work(epoch_id)
    lease = store.acquire_m5_job(epoch_id, 1, job)
    assert lease.attempt is not None
    error_hash = _sha("retryable-provider-error")
    attempt_work = _forward_attempt_work()
    attempt_timing = _attempt_timing()

    for tampered_lease in (
        replace(lease, dispatch_record_digest=_sha("forged-failure-dispatch")),
        replace(lease, resulting_revision=1),
    ):
        before_dispatch_conflict = _full_snapshot(d24_requirement_db.connection)
        with pytest.raises(EventConflictError, match="dispatch identity"):
            store.mark_m5_retryable_failure(
                epoch_id,
                2,
                tampered_lease,
                error_hash,
                attempt_work,
                attempt_timing,
            )
        assert _full_snapshot(d24_requirement_db.connection) == before_dispatch_conflict

    before_rejection = _snapshot(d24_requirement_db.connection)
    with pytest.raises(ValidationError, match="belongs to persistence"):
        store.mark_m5_retryable_failure(
            epoch_id,
            2,
            lease,
            error_hash,
            M5RuntimeWork(group_state_write_count=1),
            attempt_timing,
        )
    assert _snapshot(d24_requirement_db.connection) == before_rejection

    # Keep the failure, successor acquisition, and exact replay on one ambient
    # transaction until the snapshot below commits it.
    d24_requirement_db.connection.execute("SELECT 1")
    receipt = store.mark_m5_retryable_failure(
        epoch_id,
        2,
        lease,
        error_hash,
        attempt_work,
        attempt_timing,
    )
    assert receipt.resulting_revision == 3
    assert not receipt.exact_replay
    row = d24_requirement_db.connection.execute(
        """
        SELECT runtime.revision, job.job_state, attempt.attempt_state,
               attempt.error_hash, attempt.attempt_work_digest,
               evidence.disposition, evidence.result_or_error_hash,
               evidence.attempt_work_digest,
               contribution.applied_revision, contribution.work_digest,
               timing.required_interval_observed,
               timing.postgres_server_execution_ns,
               accumulator.required_expected_count,
               accumulator.required_observed_count,
               accumulator.required_missing_count,
               accumulator.pending_contribution_kind,
               accumulator.pending_source_id,
               accumulator.pending_anchor_revision
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
        JOIN groundloop_m5_job_attempt AS attempt
          ON attempt.logical_job_id = job.logical_job_id
        JOIN groundloop_m5_attempt_execution_evidence AS evidence
          ON evidence.epoch_id = runtime.epoch_id
         AND evidence.attempt_id = attempt.attempt_id
        JOIN groundloop_m5_runtime_work_contribution AS contribution
          ON contribution.epoch_id = evidence.epoch_id
         AND contribution.contribution_kind = 'm5_attempt_execution'
         AND contribution.source_id = evidence.attempt_id
        JOIN groundloop_m5_runtime_timing_contribution AS timing
          ON timing.epoch_id = evidence.epoch_id
         AND timing.attempt_id = evidence.attempt_id
        JOIN groundloop_m5_runtime_timing_accumulator AS accumulator
          ON accumulator.epoch_id = runtime.epoch_id
        WHERE runtime.epoch_id = %s AND attempt.attempt_id = %s
        """,
        (epoch_id, lease.attempt.attempt_id),
    ).fetchone()
    assert row == (
        3,
        "retryable_failed",
        "failed",
        error_hash,
        attempt_work.work_digest,
        "retryable_failure",
        error_hash,
        attempt_work.work_digest,
        3,
        attempt_work.work_digest,
        True,
        attempt_timing.postgres_server_execution_ns,
        4,
        1,
        2,
        "m5_attempt_execution",
        lease.attempt.attempt_id,
        3,
    )
    event_work = store.current_event_work(epoch_id)
    assert event_work.bytes_hashed == structural_work.bytes_hashed + 17
    assert event_work.bytes_serialized == structural_work.bytes_serialized + 19
    assert event_work.requirement_forward_retrieval_call_count == 1
    assert event_work.embedding_model_call_count == 1

    successor = store.acquire_m5_job(epoch_id, 3, job)
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_NEW
    assert successor.resulting_revision == 4
    assert successor.attempt is not None
    ambient_replay = store.mark_m5_retryable_failure(
        epoch_id,
        4,
        lease,
        error_hash,
        attempt_work,
        attempt_timing,
    )
    assert ambient_replay.exact_replay and ambient_replay.resulting_revision == 4
    before_replay = _snapshot(d24_requirement_db.connection)
    with d24_requirement_db.reconnect() as reconnected:
        replay = PostgresM5RuntimeStore(reconnected).mark_m5_retryable_failure(
            epoch_id,
            4,
            lease,
            error_hash,
            attempt_work,
            attempt_timing,
        )
        assert replay.exact_replay and replay.resulting_revision == 4
    assert _snapshot(d24_requirement_db.connection) == before_replay
    with pytest.raises(EventConflictError, match="immutable evidence"):
        store.mark_m5_retryable_failure(
            epoch_id,
            4,
            lease,
            _sha("different-retryable-error"),
            attempt_work,
            attempt_timing,
        )
    assert _snapshot(d24_requirement_db.connection) == before_replay
    with pytest.raises(EventConflictError, match="immutable evidence"):
        store.mark_m5_retryable_failure(
            epoch_id,
            4,
            lease,
            error_hash,
            attempt_work,
            replace(attempt_timing, neural_wall_ns=37),
        )
    assert _snapshot(d24_requirement_db.connection) == before_replay


def test_terminal_failure_moves_exact_blocking_unit_and_replays_on_reconnect(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    lease = store.acquire_m5_job(epoch_id, 1, job)
    assert lease.attempt is not None
    error_hash = _sha("terminal-retrieval-error")
    attempt_work = _forward_attempt_work()
    attempt_timing = _attempt_timing()
    d24_requirement_db.connection.execute("SELECT 1")
    receipt = store.mark_m5_terminal_failure(
        epoch_id,
        2,
        lease,
        M5TerminalReason.RETRIEVAL_ERROR,
        error_hash,
        attempt_work,
        attempt_timing,
    )
    assert receipt.resulting_revision == 3
    assert not receipt.exact_replay

    row = d24_requirement_db.connection.execute(
        """
        SELECT runtime.revision, runtime.runtime_state,
               runtime.open_work_count, runtime.open_scope_count,
               runtime.blocking_failure_count,
               job.job_state, job.archive_reason, job.completed_revision,
               scope.scope_state, scope.completion_digest,
               scope.closed_revision,
               owner.forward_scope_count, owner.blocking_failure_count,
               answer.forward_scope_count, answer.blocking_failure_count,
               evidence.disposition, evidence.attempt_work_digest,
               terminal.work_digest, terminal.applied_revision,
               terminal.source_identity_hash,
               terminal.contribution_key_digest,
               timing.pending_contribution_kind,
               timing.pending_source_id, timing.pending_anchor_revision
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.epoch_id = job.epoch_id
         AND scope.root_job_id = job.logical_job_id
        JOIN groundloop_m5_owner_pending_counter AS owner
          ON owner.epoch_id = runtime.epoch_id
        JOIN groundloop_m5_answer_pending_counter AS answer
          ON answer.epoch_id = runtime.epoch_id
        JOIN groundloop_m5_attempt_execution_evidence AS evidence
          ON evidence.epoch_id = runtime.epoch_id
         AND evidence.attempt_id = %s
        JOIN groundloop_m5_runtime_work_contribution AS terminal
          ON terminal.epoch_id = runtime.epoch_id
         AND terminal.contribution_kind = 'terminal_job_failure'
         AND terminal.source_id = job.logical_job_id
        JOIN groundloop_m5_runtime_timing_accumulator AS timing
          ON timing.epoch_id = runtime.epoch_id
        WHERE runtime.epoch_id = %s
        """,
        (lease.attempt.attempt_id, epoch_id),
    ).fetchone()
    assert row is not None
    assert row[:9] == (
        3,
        "semantic_pending",
        0,
        0,
        1,
        "terminal_failed",
        M5TerminalReason.RETRIEVAL_ERROR.value,
        3,
        "terminal_failed",
    )
    assert isinstance(row[9], str) and len(row[9].strip()) == 64
    assert row[10:] == (
        3,
        0,
        1,
        0,
        1,
        "terminal_failure",
        attempt_work.work_digest,
        M5RuntimeWork().work_digest,
        3,
        digests.terminal_job_failure_contribution_source_digest(
            logical_job_id=job.logical_job_id,
            terminal_reason=M5TerminalReason.RETRIEVAL_ERROR,
            error_hash=error_hash,
        ),
        digests.runtime_work_contribution_key_digest(
            epoch_id=epoch_id,
            contribution_kind="terminal_job_failure",
            source_id=job.logical_job_id,
        ),
        "m5_attempt_execution",
        lease.attempt.attempt_id,
        3,
    )
    terminal_projection = store.acquire_m5_job(epoch_id, 3, job)
    assert terminal_projection.disposition is M5AcquisitionDisposition.TERMINAL
    assert terminal_projection.terminal_projection is not None
    assert (
        terminal_projection.terminal_projection.terminal_reason
        is M5TerminalReason.RETRIEVAL_ERROR
    )
    ambient_replay = store.mark_m5_terminal_failure(
        epoch_id,
        3,
        lease,
        M5TerminalReason.RETRIEVAL_ERROR,
        error_hash,
        attempt_work,
        attempt_timing,
    )
    assert ambient_replay.exact_replay and ambient_replay.resulting_revision == 3

    before_replay = _snapshot(d24_requirement_db.connection)
    with d24_requirement_db.reconnect() as reconnected:
        replay = PostgresM5RuntimeStore(reconnected).mark_m5_terminal_failure(
            epoch_id,
            3,
            lease,
            M5TerminalReason.RETRIEVAL_ERROR,
            error_hash,
            attempt_work,
            attempt_timing,
        )
        assert replay.exact_replay and replay.resulting_revision == 3
    assert _snapshot(d24_requirement_db.connection) == before_replay
    with pytest.raises(EventConflictError, match="changed its terminal reason"):
        store.mark_m5_terminal_failure(
            epoch_id,
            3,
            lease,
            M5TerminalReason.INVALID_ARTIFACT,
            error_hash,
            attempt_work,
            attempt_timing,
        )
    assert _snapshot(d24_requirement_db.connection) == before_replay
    with pytest.raises(EventConflictError, match="immutable evidence"):
        store.mark_m5_terminal_failure(
            epoch_id,
            3,
            lease,
            M5TerminalReason.RETRIEVAL_ERROR,
            error_hash,
            attempt_work,
            replace(attempt_timing, end_to_end_wall_ns=41),
        )
    assert _snapshot(d24_requirement_db.connection) == before_replay


def test_takeover_first_rejects_both_old_failure_settlements_without_writes(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    original = store.acquire_m5_job(epoch_id, 1, job)
    assert original.attempt is not None
    assert original.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db.connection, original.lease_expires_at)
    takeover = store.acquire_m5_job(epoch_id, 2, job)
    assert takeover.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert takeover.attempt is not None
    assert takeover.attempt.attempt_id != original.attempt.attempt_id
    before = _snapshot(d24_requirement_db.connection)

    with pytest.raises(EventConflictError, match="no longer the latest"):
        store.mark_m5_retryable_failure(
            epoch_id,
            3,
            original,
            _sha("old-retry-after-takeover"),
            _forward_attempt_work(),
            _attempt_timing(),
        )
    assert _snapshot(d24_requirement_db.connection) == before
    with pytest.raises(EventConflictError, match="no longer the latest"):
        store.mark_m5_terminal_failure(
            epoch_id,
            3,
            original,
            M5TerminalReason.RETRY_EXHAUSTED,
            _sha("old-terminal-after-takeover"),
            _forward_attempt_work(),
            _attempt_timing(),
        )
    assert _snapshot(d24_requirement_db.connection) == before


def test_expired_output_waits_for_retryable_successor_then_archives_when_running(
    d24_requirement_db: Any,
) -> None:
    """C4 delays the old output until a checked retry restores running."""

    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    original = store.acquire_m5_job(epoch_id, 1, job)
    assert original.attempt is not None and original.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db.connection, original.lease_expires_at)
    takeover = store.acquire_m5_job(epoch_id, 2, job)
    assert takeover.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert takeover.attempt is not None and takeover.resulting_revision == 3
    assert takeover.attempt.attempt_ordinal == 2
    result, output = _root_result_and_output(
        d24_requirement_db, job=job, attempt=original.attempt
    )
    failed = store.mark_m5_retryable_failure(
        epoch_id,
        3,
        takeover,
        _sha("c4-retryable-successor"),
        _forward_attempt_work(),
        _attempt_timing(),
    )
    assert failed.resulting_revision == 4 and not failed.exact_replay

    def old_output(expected_revision: int) -> Any:
        return store.stage_m5_discovery_result_atomically(
            epoch_id,
            expected_revision,
            original,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )

    _assert_expired_successor_rejections(
        d24_requirement_db,
        call=old_output,
        stale_revision=3,
        current_revision=4,
    )
    assert d24_requirement_db.connection.execute(
        """
        SELECT runtime.runtime_state, runtime.revision, job.job_state,
               latest.attempt_ordinal, latest.attempt_state
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
        JOIN groundloop_m5_job_attempt AS latest
          ON latest.logical_job_id = job.logical_job_id
         AND latest.attempt_ordinal = 2
        WHERE runtime.epoch_id = %s AND job.logical_job_id = %s
        """,
        (epoch_id, job.logical_job_id),
    ).fetchone() == ("semantic_pending", 4, "retryable_failed", 2, "failed")

    retry = store.acquire_m5_job(epoch_id, 4, job)
    assert retry.disposition is M5AcquisitionDisposition.DISPATCH_NEW
    assert retry.resulting_revision == 5 and retry.attempt is not None
    assert retry.attempt.attempt_ordinal == 3
    receipt = old_output(5)
    assert receipt.disposition is M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
    assert receipt.resulting_revision == 5 and not receipt.exact_replay
    assert d24_requirement_db.connection.execute(
        """
        SELECT job.job_state, latest.attempt_state,
               artifact.job_state_at_receipt, artifact.job_state_after,
               artifact.archive_reason, expired.received_after_terminal,
               (SELECT count(*)
                FROM groundloop_m5_requirement_discovery_result
                WHERE root_job_id = %s),
               (SELECT count(*)
                FROM groundloop_m5_requirement_channel_hit
                WHERE root_job_id = %s),
               (SELECT count(*)
                FROM groundloop_m5_requirement_scope_selection
                WHERE root_job_id = %s)
        FROM groundloop_m5_semantic_job AS job
        JOIN groundloop_m5_job_attempt AS latest
          ON latest.logical_job_id = job.logical_job_id
         AND latest.attempt_ordinal = 3
        JOIN groundloop_m5_attempt_result_artifact AS artifact
          ON artifact.attempt_id = %s
        JOIN groundloop_m5_expired_attempt_return AS expired
          ON expired.epoch_id = job.epoch_id
         AND expired.attempt_id = artifact.attempt_id
         AND expired.subgraph = 'requirement'
        WHERE job.epoch_id = %s AND job.logical_job_id = %s
        """,
        (
            job.logical_job_id,
            job.logical_job_id,
            job.logical_job_id,
            original.attempt.attempt_id,
            epoch_id,
            job.logical_job_id,
        ),
    ).fetchone() == (
        "running",
        "dispatched",
        "running",
        "running",
        "attempt_expired",
        False,
        0,
        0,
        0,
    )


def test_retryable_successor_archives_only_after_fixture_terminal_resolution(
    d24_requirement_db: Any,
) -> None:
    """C4 case 5 uses a labelled SQL-only terminal storage fixture."""

    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    original = store.acquire_m5_job(epoch_id, 1, job)
    assert original.attempt is not None and original.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db.connection, original.lease_expires_at)
    takeover = store.acquire_m5_job(epoch_id, 2, job)
    assert takeover.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert takeover.attempt is not None and takeover.resulting_revision == 3
    result, output = _root_result_and_output(
        d24_requirement_db, job=job, attempt=original.attempt
    )
    failed = store.mark_m5_retryable_failure(
        epoch_id,
        3,
        takeover,
        _sha("c4-retryable-terminal-resolution"),
        _forward_attempt_work(),
        _attempt_timing(),
    )
    assert failed.resulting_revision == 4 and not failed.exact_replay

    def old_output(expected_revision: int) -> Any:
        return store.stage_m5_discovery_result_atomically(
            epoch_id,
            expected_revision,
            original,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )

    _assert_expired_successor_rejections(
        d24_requirement_db,
        call=old_output,
        stale_revision=3,
        current_revision=4,
    )
    assert _late_requirement_row_counts(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        attempt_id=original.attempt.attempt_id,
    ) == (0, 0, 0, 0, 0)

    terminal_hash = terminalize_recovered_epoch_for_storage_fixture(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        resolve_retryable_job_id=job.logical_job_id,
    )
    d24_requirement_db.connection.commit()
    assert d24_requirement_db.connection.execute(
        """
        SELECT base.revision, runtime.runtime_state, runtime.revision,
               runtime.open_work_count, runtime.open_scope_count,
               runtime.blocking_failure_count,
               job.job_state, job.archive_reason, job.completed_revision,
               scope.scope_state, scope.closed_revision,
               (SELECT count(*)
                FROM groundloop_m5_owner_pending_counter
                WHERE epoch_id = runtime.epoch_id),
               (SELECT count(*)
                FROM groundloop_m5_owner_pending_counter
                WHERE epoch_id = runtime.epoch_id
                  AND forward_scope_count = 0
                  AND blocking_failure_count = 1
                  AND updated_revision = 5),
               (SELECT count(*)
                FROM groundloop_m5_answer_pending_counter
                WHERE epoch_id = runtime.epoch_id),
               (SELECT count(*)
                FROM groundloop_m5_answer_pending_counter
                WHERE epoch_id = runtime.epoch_id
                  AND forward_scope_count = 0
                  AND blocking_failure_count = 1
                  AND updated_revision = 5),
               (SELECT count(*)
                FROM groundloop_m5_runtime_work_contribution
                WHERE epoch_id = runtime.epoch_id
                  AND contribution_kind = 'terminal_job_failure'
                  AND source_id = job.logical_job_id
                  AND applied_revision = 5)
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.epoch_id = job.epoch_id
         AND scope.root_job_id = job.logical_job_id
        WHERE runtime.epoch_id = %s AND job.logical_job_id = %s
        """,
        (epoch_id, job.logical_job_id),
    ).fetchone() == (
        5,
        "failed",
        5,
        0,
        0,
        1,
        "terminal_failed",
        "retry_exhausted",
        5,
        "terminal_failed",
        5,
        1,
        1,
        1,
        1,
        1,
    )

    terminal_before = _snapshot_tables(
        d24_requirement_db.connection, _TERMINAL_STATE_TABLES
    )
    semantic_before = _snapshot_tables(
        d24_requirement_db.connection, _LATE_SEMANTIC_TABLES
    )
    receipt = old_output(5)
    assert receipt.disposition is M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
    assert receipt.resulting_revision == 5 and not receipt.exact_replay
    assert receipt.transition_anchor is None
    assert receipt.current_terminal_logical_result_hash == terminal_hash
    assert (
        _snapshot_tables(d24_requirement_db.connection, _TERMINAL_STATE_TABLES)
        == terminal_before
    )
    assert (
        _snapshot_tables(d24_requirement_db.connection, _LATE_SEMANTIC_TABLES)
        == semantic_before
    )
    assert _late_requirement_row_counts(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        attempt_id=original.attempt.attempt_id,
    ) == (1, 1, 1, 1, 1)
    assert d24_requirement_db.connection.execute(
        """
        SELECT attempt.attempt_state,
               artifact.job_state_at_receipt, artifact.job_state_after,
               artifact.disposition, artifact.archive_reason,
               artifact.cancelled_by_event_id,
               artifact.cancelled_by_epoch_id,
               artifact.cancellation_reason,
               expired.received_after_terminal,
               (SELECT count(*)
                FROM groundloop_m5_runtime_work_contribution
                WHERE epoch_id = %s AND source_id = %s
                  AND contribution_kind IN (
                      'm5_attempt_execution', 'preterminal_late_return')),
               (SELECT count(*)
                FROM groundloop_m5_runtime_timing_contribution
                WHERE epoch_id = %s AND subgraph = 'requirement'
                  AND attempt_id = %s)
        FROM groundloop_m5_job_attempt AS attempt
        JOIN groundloop_m5_attempt_result_artifact AS artifact
          ON artifact.attempt_id = attempt.attempt_id
        JOIN groundloop_m5_expired_attempt_return AS expired
          ON expired.epoch_id = artifact.job_epoch_id
         AND expired.attempt_id = artifact.attempt_id
         AND expired.subgraph = 'requirement'
        WHERE attempt.attempt_id = %s
        """,
        (
            epoch_id,
            original.attempt.attempt_id,
            epoch_id,
            original.attempt.attempt_id,
            original.attempt.attempt_id,
        ),
    ).fetchone() == (
        "expired",
        "terminal_failed",
        "terminal_failed",
        "terminal_audit_only",
        "attempt_expired",
        None,
        None,
        None,
        True,
        0,
        0,
    )

    before_replay = _full_snapshot(d24_requirement_db.connection)
    replay = old_output(5)
    assert replay.exact_replay and replay.transition_anchor is None
    assert replay.disposition is M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
    assert replay.current_terminal_logical_result_hash == terminal_hash
    assert _full_snapshot(d24_requirement_db.connection) == before_replay


def test_expired_output_archives_while_successor_result_remains_staged(
    d24_requirement_db: Any,
) -> None:
    """A staged successor result does not stop the unchanged running branch."""

    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    original = store.acquire_m5_job(epoch_id, 1, job)
    assert original.attempt is not None and original.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db.connection, original.lease_expires_at)
    successor = store.acquire_m5_job(epoch_id, 2, job)
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert successor.attempt is not None and successor.resulting_revision == 3

    result, successor_output = _root_result_and_output(
        d24_requirement_db, job=job, attempt=successor.attempt
    )
    staged = store.stage_m5_discovery_result_atomically(
        epoch_id,
        3,
        successor,
        job,
        result,
        successor_output,
        M5ExecutionEvidenceDisposition.RETURNED,
        _forward_attempt_work(),
        _attempt_timing(),
        eligible_snapshot_exhausted=True,
    )
    assert staged.disposition is M5RequirementReturnDisposition.APPLIED
    assert staged.resulting_revision == 4 and not staged.exact_replay

    old_output = M5AttemptOutput.build(
        attempt=original.attempt,
        job_epoch_id=epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=result.result_artifact_id,
        result_artifact_hash=result.result_artifact_hash,
    )
    normal_before = _snapshot_tables(
        d24_requirement_db.connection, _LATE_SEMANTIC_TABLES
    )
    receipt = store.stage_m5_discovery_result_atomically(
        epoch_id,
        4,
        original,
        job,
        result,
        old_output,
        M5ExecutionEvidenceDisposition.RETURNED,
        _forward_attempt_work(),
        _attempt_timing(),
        eligible_snapshot_exhausted=True,
    )
    assert receipt.disposition is M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
    assert receipt.resulting_revision == 4 and not receipt.exact_replay
    assert (
        _snapshot_tables(d24_requirement_db.connection, _LATE_SEMANTIC_TABLES)
        == normal_before
    )
    assert d24_requirement_db.connection.execute(
        """
        SELECT runtime.revision, runtime.runtime_state,
               job.job_state, scope.scope_state,
               successor_attempt.attempt_state,
               successor_artifact.disposition,
               old_artifact.disposition, old_artifact.archive_reason,
               expired.received_after_terminal,
               (SELECT count(*)
                FROM groundloop_m5_attempt_result_artifact
                WHERE attempt_id = %s AND disposition = 'root_result_staged'),
               result.staged_revision
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.epoch_id = job.epoch_id
         AND scope.root_job_id = job.logical_job_id
        JOIN groundloop_m5_job_attempt AS successor_attempt
          ON successor_attempt.attempt_id = %s
        JOIN groundloop_m5_attempt_result_artifact AS successor_artifact
          ON successor_artifact.attempt_id = successor_attempt.attempt_id
        JOIN groundloop_m5_attempt_result_artifact AS old_artifact
          ON old_artifact.attempt_id = %s
        JOIN groundloop_m5_expired_attempt_return AS expired
          ON expired.epoch_id = job.epoch_id
         AND expired.attempt_id = old_artifact.attempt_id
         AND expired.subgraph = 'requirement'
        JOIN groundloop_m5_requirement_discovery_result AS result
          ON result.root_job_id = job.logical_job_id
        WHERE runtime.epoch_id = %s AND job.logical_job_id = %s
        """,
        (
            original.attempt.attempt_id,
            successor.attempt.attempt_id,
            original.attempt.attempt_id,
            epoch_id,
            job.logical_job_id,
        ),
    ).fetchone() == (
        4,
        "semantic_pending",
        "running",
        "result_staged",
        "completed",
        "root_result_staged",
        "terminal_audit_only",
        "attempt_expired",
        False,
        0,
        4,
    )

    before_replay = _full_snapshot(d24_requirement_db.connection)
    replay = store.stage_m5_discovery_result_atomically(
        epoch_id,
        4,
        original,
        job,
        result,
        old_output,
        M5ExecutionEvidenceDisposition.RETURNED,
        _forward_attempt_work(),
        _attempt_timing(),
        eligible_snapshot_exhausted=True,
    )
    assert replay.exact_replay
    assert replay.disposition is M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
    assert _full_snapshot(d24_requirement_db.connection) == before_replay


def test_completed_active_successor_delays_expired_output_until_terminal_fixture(
    d24_requirement_db: Any,
) -> None:
    """C4 terminal-successor proof; terminalization remains an SQL-only fixture."""

    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    original = store.acquire_m5_job(epoch_id, 1, job)
    assert original.attempt is not None and original.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db.connection, original.lease_expires_at)
    successor = store.acquire_m5_job(epoch_id, 2, job)
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert successor.attempt is not None and successor.resulting_revision == 3

    result, successor_output = _root_result_and_output(
        d24_requirement_db, job=job, attempt=successor.attempt
    )
    staged = store.stage_m5_discovery_result_atomically(
        epoch_id,
        3,
        successor,
        job,
        result,
        successor_output,
        M5ExecutionEvidenceDisposition.RETURNED,
        _forward_attempt_work(),
        _attempt_timing(),
        eligible_snapshot_exhausted=True,
    )
    assert staged.resulting_revision == 4
    root_set_row = d24_requirement_db.connection.execute(
        """
        SELECT requirement_root_set_hash
        FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert root_set_row is not None
    barrier = store.close_m5_requirement_roots_atomically(
        epoch_id,
        4,
        str(root_set_row[0]).strip(),
    )
    assert barrier.resulting_revision == 5 and not barrier.exact_replay
    assert d24_requirement_db.connection.execute(
        """
        SELECT runtime.runtime_state, runtime.revision,
               job.job_state, scope.scope_state
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.epoch_id = job.epoch_id
         AND scope.root_job_id = job.logical_job_id
        WHERE runtime.epoch_id = %s AND job.logical_job_id = %s
        """,
        (epoch_id, job.logical_job_id),
    ).fetchone() == ("semantic_pending", 5, "completed_active", "closed_active")

    old_output = M5AttemptOutput.build(
        attempt=original.attempt,
        job_epoch_id=epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=result.result_artifact_id,
        result_artifact_hash=result.result_artifact_hash,
    )

    def old_return(expected_revision: int) -> Any:
        return store.stage_m5_discovery_result_atomically(
            epoch_id,
            expected_revision,
            original,
            job,
            result,
            old_output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )

    _assert_expired_successor_rejections(
        d24_requirement_db,
        call=old_return,
        stale_revision=4,
        current_revision=5,
    )
    assert _late_requirement_row_counts(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        attempt_id=original.attempt.attempt_id,
    ) == (0, 0, 0, 0, 0)

    normal_tables = (
        "groundloop_m5_semantic_job",
        "groundloop_m5_job_attempt",
        "groundloop_m5_discovery_scope",
        "groundloop_m5_requirement_channel_hit",
        "groundloop_m5_requirement_scope_selection",
        "groundloop_m5_requirement_discovery_result",
        "groundloop_m5_requirement_admitted_pair",
        "groundloop_m5_requirement_admitted_pair_source",
        "groundloop_m5_requirement_frontier_head",
    )
    normal_before = _snapshot_tables(d24_requirement_db.connection, normal_tables)
    successor_before = d24_requirement_db.connection.execute(
        """
        SELECT artifact.xmin::text, to_jsonb(artifact)::text,
               evidence.xmin::text, to_jsonb(evidence)::text
        FROM groundloop_m5_attempt_result_artifact AS artifact
        JOIN groundloop_m5_attempt_execution_evidence AS evidence
          ON evidence.epoch_id = artifact.job_epoch_id
         AND evidence.attempt_id = artifact.attempt_id
         AND evidence.subgraph = 'requirement'
        WHERE artifact.attempt_id = %s
        """,
        (successor.attempt.attempt_id,),
    ).fetchone()
    assert successor_before is not None

    terminal_hash = terminalize_recovered_epoch_for_storage_fixture(
        d24_requirement_db.connection, epoch_id=epoch_id
    )
    d24_requirement_db.connection.commit()
    terminal_revision = store.current_revision(epoch_id)
    assert terminal_revision == 6
    terminal_before = _snapshot_tables(
        d24_requirement_db.connection, _TERMINAL_STATE_TABLES
    )
    receipt = old_return(terminal_revision)
    assert receipt.disposition is M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
    assert receipt.resulting_revision == terminal_revision
    assert not receipt.exact_replay and receipt.transition_anchor is None
    assert receipt.current_terminal_logical_result_hash == terminal_hash
    assert (
        _snapshot_tables(d24_requirement_db.connection, _TERMINAL_STATE_TABLES)
        == terminal_before
    )
    assert (
        _snapshot_tables(d24_requirement_db.connection, normal_tables) == normal_before
    )
    assert (
        d24_requirement_db.connection.execute(
            """
        SELECT artifact.xmin::text, to_jsonb(artifact)::text,
               evidence.xmin::text, to_jsonb(evidence)::text
        FROM groundloop_m5_attempt_result_artifact AS artifact
        JOIN groundloop_m5_attempt_execution_evidence AS evidence
          ON evidence.epoch_id = artifact.job_epoch_id
         AND evidence.attempt_id = artifact.attempt_id
         AND evidence.subgraph = 'requirement'
        WHERE artifact.attempt_id = %s
        """,
            (successor.attempt.attempt_id,),
        ).fetchone()
        == successor_before
    )
    assert _late_requirement_row_counts(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        attempt_id=original.attempt.attempt_id,
    ) == (1, 1, 1, 1, 1)
    assert d24_requirement_db.connection.execute(
        """
        SELECT artifact.job_state_at_receipt, artifact.job_state_after,
               artifact.archive_reason, artifact.cancelled_by_event_id,
               artifact.cancelled_by_epoch_id, artifact.cancellation_reason,
               expired.received_after_terminal
        FROM groundloop_m5_attempt_result_artifact AS artifact
        JOIN groundloop_m5_expired_attempt_return AS expired
          ON expired.epoch_id = artifact.job_epoch_id
         AND expired.attempt_id = artifact.attempt_id
         AND expired.subgraph = 'requirement'
        WHERE artifact.attempt_id = %s
        """,
        (original.attempt.attempt_id,),
    ).fetchone() == (
        "completed_active",
        "completed_active",
        "attempt_expired",
        None,
        None,
        None,
        True,
    )


def test_terminal_failed_successor_delays_expired_output_while_event_is_open(
    d24_requirement_db: Any,
) -> None:
    """C4 rejects stale and current output after a terminal job failure."""

    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    original = store.acquire_m5_job(epoch_id, 1, job)
    assert original.attempt is not None and original.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db.connection, original.lease_expires_at)
    successor = store.acquire_m5_job(epoch_id, 2, job)
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert successor.attempt is not None and successor.resulting_revision == 3

    result, old_output = _root_result_and_output(
        d24_requirement_db, job=job, attempt=original.attempt
    )
    failure = store.mark_m5_terminal_failure(
        epoch_id,
        3,
        successor,
        M5TerminalReason.RETRIEVAL_ERROR,
        _sha("c4-terminal-successor"),
        _forward_attempt_work(),
        _attempt_timing(),
    )
    assert failure.resulting_revision == 4 and not failure.exact_replay
    assert d24_requirement_db.connection.execute(
        """
        SELECT runtime.runtime_state, runtime.revision,
               job.job_state, scope.scope_state
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.epoch_id = job.epoch_id
         AND scope.root_job_id = job.logical_job_id
        WHERE runtime.epoch_id = %s AND job.logical_job_id = %s
        """,
        (epoch_id, job.logical_job_id),
    ).fetchone() == ("semantic_pending", 4, "terminal_failed", "terminal_failed")

    def old_return(expected_revision: int) -> Any:
        return store.stage_m5_discovery_result_atomically(
            epoch_id,
            expected_revision,
            original,
            job,
            result,
            old_output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )

    _assert_expired_successor_rejections(
        d24_requirement_db,
        call=old_return,
        stale_revision=3,
        current_revision=4,
    )
    assert _late_requirement_row_counts(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        attempt_id=original.attempt.attempt_id,
    ) == (0, 0, 0, 0, 0)


def test_completed_inactive_successor_delays_expired_output_until_terminal(
    d24_requirement_db: Any,
) -> None:
    """C4 rejects old output after an inactive root closes nonterminally."""

    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    original = store.acquire_m5_job(epoch_id, 1, job)
    assert original.attempt is not None and original.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db.connection, original.lease_expires_at)
    successor = store.acquire_m5_job(epoch_id, 2, job)
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert successor.attempt is not None and successor.resulting_revision == 3

    group = d24_requirement_db.plan.event.group
    d24_requirement_db.connection.execute(
        """
        UPDATE groundloop_m5_requirement_version
        SET lifecycle_state = 'FAILED'
        WHERE group_version_id = %s AND lifecycle_state = 'STAGED'
        """,
        (group.group_version_id,),
    )
    d24_requirement_db.connection.execute(
        """
        UPDATE groundloop_m5_group_version
        SET lifecycle_state = 'FAILED'
        WHERE group_version_id = %s AND lifecycle_state = 'STAGED'
        """,
        (group.group_version_id,),
    )
    d24_requirement_db.connection.execute(
        """
        UPDATE groundloop_m5_group_family
        SET lifecycle_state = 'FAILED'
        WHERE group_family_id = %s AND lifecycle_state = 'STAGED'
        """,
        (group.group_family_id,),
    )
    d24_requirement_db.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    d24_requirement_db.connection.commit()

    result, successor_output = _root_result_and_output(
        d24_requirement_db, job=job, attempt=successor.attempt
    )
    staged = store.stage_m5_discovery_result_atomically(
        epoch_id,
        3,
        successor,
        job,
        result,
        successor_output,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        _attempt_timing(),
        eligible_snapshot_exhausted=True,
    )
    assert staged.resulting_revision == 4 and not staged.exact_replay
    root_set_row = d24_requirement_db.connection.execute(
        """
        SELECT requirement_root_set_hash
        FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert root_set_row is not None
    barrier = store.close_m5_requirement_roots_atomically(
        epoch_id,
        4,
        str(root_set_row[0]).strip(),
    )
    assert barrier.resulting_revision == 5 and not barrier.exact_replay
    assert d24_requirement_db.connection.execute(
        """
        SELECT runtime.runtime_state, runtime.revision,
               job.job_state, scope.scope_state
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.epoch_id = job.epoch_id
         AND scope.root_job_id = job.logical_job_id
        WHERE runtime.epoch_id = %s AND job.logical_job_id = %s
        """,
        (epoch_id, job.logical_job_id),
    ).fetchone() == (
        "semantic_pending",
        5,
        "completed_inactive",
        "closed_inactive",
    )

    old_output = M5AttemptOutput.build(
        attempt=original.attempt,
        job_epoch_id=epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=result.result_artifact_id,
        result_artifact_hash=result.result_artifact_hash,
    )

    def old_return(expected_revision: int) -> Any:
        return store.stage_m5_discovery_result_atomically(
            epoch_id,
            expected_revision,
            original,
            job,
            result,
            old_output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )

    _assert_expired_successor_rejections(
        d24_requirement_db,
        call=old_return,
        stale_revision=4,
        current_revision=5,
    )
    assert _late_requirement_row_counts(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        attempt_id=original.attempt.attempt_id,
    ) == (0, 0, 0, 0, 0)


def test_snapshot_exhaustion_evidence_is_required_on_first_write_and_replay(
    d24_requirement_db: Any,
) -> None:
    """C4 requires explicit true evidence for snapshot-exhausted results."""

    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    lease = store.acquire_m5_job(epoch_id, 1, job)
    assert lease.attempt is not None
    result, output = _root_result_and_output(
        d24_requirement_db, job=job, attempt=lease.attempt
    )
    assert result.termination is M5RetrievalTermination.SNAPSHOT_EXHAUSTED

    before_first = _full_snapshot(d24_requirement_db.connection)
    with pytest.raises(
        ValidationError,
        match="snapshot_exhausted requires a short result and exhaustion evidence",
    ):
        store.stage_m5_discovery_result_atomically(
            epoch_id,
            2,
            lease,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=False,
        )
    assert _full_snapshot(d24_requirement_db.connection) == before_first

    receipt = store.stage_m5_discovery_result_atomically(
        epoch_id,
        2,
        lease,
        job,
        result,
        output,
        M5ExecutionEvidenceDisposition.RETURNED,
        _forward_attempt_work(),
        _attempt_timing(),
        eligible_snapshot_exhausted=True,
    )
    assert receipt.disposition is M5RequirementReturnDisposition.APPLIED
    assert receipt.resulting_revision == 3 and not receipt.exact_replay

    before_replay = _full_snapshot(d24_requirement_db.connection)
    with pytest.raises(
        ValidationError,
        match="snapshot_exhausted requires a short result and exhaustion evidence",
    ):
        store.stage_m5_discovery_result_atomically(
            epoch_id,
            3,
            lease,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=False,
        )
    assert _full_snapshot(d24_requirement_db.connection) == before_replay

    replay = store.stage_m5_discovery_result_atomically(
        epoch_id,
        3,
        lease,
        job,
        result,
        output,
        M5ExecutionEvidenceDisposition.RETURNED,
        _forward_attempt_work(),
        _attempt_timing(),
        eligible_snapshot_exhausted=True,
    )
    assert replay.exact_replay and replay.resulting_revision == 3
    assert _full_snapshot(d24_requirement_db.connection) == before_replay


@pytest.mark.parametrize("first_evidence", [False, True])
def test_budget_filled_exhaustion_boolean_is_not_replay_identity(
    d24_requirement_db: Any,
    first_evidence: bool,
) -> None:
    """C4 keeps the unstored boolean non-material for a full budget."""

    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    lease = store.acquire_m5_job(epoch_id, 1, job)
    assert lease.attempt is not None and lease.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db.connection, lease.lease_expires_at)
    takeover = store.acquire_m5_job(epoch_id, 2, job)
    assert takeover.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert takeover.resulting_revision == 3
    result, output = _budget_filled_root_result_and_output(
        d24_requirement_db, job=job, attempt=lease.attempt
    )
    receipt = store.stage_m5_discovery_result_atomically(
        epoch_id,
        3,
        lease,
        job,
        result,
        output,
        M5ExecutionEvidenceDisposition.RETURNED,
        _forward_attempt_work(),
        _attempt_timing(),
        eligible_snapshot_exhausted=first_evidence,
    )
    assert receipt.disposition is M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
    assert receipt.resulting_revision == 3 and not receipt.exact_replay

    before_replay = _full_snapshot(d24_requirement_db.connection)
    replay = store.stage_m5_discovery_result_atomically(
        epoch_id,
        3,
        lease,
        job,
        result,
        output,
        M5ExecutionEvidenceDisposition.RETURNED,
        _forward_attempt_work(),
        _attempt_timing(),
        eligible_snapshot_exhausted=not first_evidence,
    )
    assert replay.exact_replay and replay.resulting_revision == 3
    assert _full_snapshot(d24_requirement_db.connection) == before_replay
    assert d24_requirement_db.connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_requirement_channel_hit
           WHERE root_job_id = %s),
          (SELECT count(*) FROM groundloop_m5_requirement_scope_selection
           WHERE root_job_id = %s),
          (SELECT count(*) FROM groundloop_m5_requirement_discovery_result
           WHERE root_job_id = %s)
        """,
        (job.logical_job_id, job.logical_job_id, job.logical_job_id),
    ).fetchone() == (0, 0, 0)


@pytest.mark.parametrize(
    "tamper",
    ("epoch", "policy", "scope", "direction_target", "snapshot_member"),
)
def test_late_discovery_revalidates_every_nested_context(
    d24_requirement_db: Any,
    tamper: str,
) -> None:
    """Self-digested late DTOs still bind the frozen scope and snapshots."""

    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    original = store.acquire_m5_job(epoch_id, 1, job)
    assert original.attempt is not None and original.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db.connection, original.lease_expires_at)
    takeover = store.acquire_m5_job(epoch_id, 2, job)
    assert takeover.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert takeover.resulting_revision == 3
    result, _output = _root_result_and_output(
        d24_requirement_db, job=job, attempt=original.attempt
    )
    base_hit = result.channel_hits[0]
    pair = base_hit.pair
    scope_digest = result.scope_contract_digest
    hit_epoch = epoch_id
    policy_id = job.candidate_policy_id
    if tamper == "epoch":
        hit_epoch += 1
    elif tamper == "policy":
        policy_id = "foreign-candidate-policy"
    elif tamper == "scope":
        scope_digest = _sha("foreign-discovery-scope")
    elif tamper == "direction_target":
        other = d24_requirement_db.connection.execute(
            """
            SELECT member.requirement_version_id
            FROM groundloop_m5_requirement_registry_snapshot_member AS member
            JOIN groundloop_m5_discovery_scope AS scope
              ON scope.epoch_id = %s AND scope.root_job_id = %s
            WHERE member.requirement_registry_snapshot_digest = %s
              AND member.requirement_version_id <> scope.requirement_version_id
            ORDER BY member.member_ordinal
            LIMIT 1
            """,
            (
                epoch_id,
                job.logical_job_id,
                job.requirement_registry_snapshot_digest,
            ),
        ).fetchone()
        assert other is not None
        pair = SemanticPairKey(
            SubjectKind.REQUIREMENT,
            str(other[0]),
            pair.chunk_version_id,
        )
    elif tamper == "snapshot_member":
        pair = SemanticPairKey(
            SubjectKind.REQUIREMENT,
            pair.subject_id,
            "chunk-outside-frozen-snapshot",
        )
    else:  # pragma: no cover - the parameter tuple is exhaustive
        raise AssertionError(tamper)

    changed_hit = M5RequirementChannelHit.build(
        epoch_id=hit_epoch,
        root_job_id=job.logical_job_id,
        scope_contract_digest=scope_digest,
        pair=pair,
        candidate_policy_id=policy_id,
        channel=base_hit.channel,
        rank=1,
        score=base_hit.score,
        channel_artifact_hash=_sha(f"c4-context-{tamper}"),
    )
    changed_selection = M5RequirementScopeSelection.build(
        root_job_id=job.logical_job_id,
        scope_contract_digest=scope_digest,
        pair=pair,
        fused_rank=1,
        reasons=(base_hit.channel,),
    )
    changed_result = M5RequirementDiscoveryResult.build(
        root_job_id=job.logical_job_id,
        scope_contract_digest=scope_digest,
        termination=M5RetrievalTermination.SNAPSHOT_EXHAUSTED,
        channel_hits=(changed_hit,),
        selections=(changed_selection,),
    )
    changed_output = M5AttemptOutput.build(
        attempt=original.attempt,
        job_epoch_id=epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=changed_result.result_artifact_id,
        result_artifact_hash=changed_result.result_artifact_hash,
    )

    before = _full_snapshot(d24_requirement_db.connection)
    with pytest.raises(ValidationError):
        store.stage_m5_discovery_result_atomically(
            epoch_id,
            3,
            original,
            job,
            changed_result,
            changed_output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )
    assert _full_snapshot(d24_requirement_db.connection) == before


def test_expired_preterminal_return_is_same_revision_and_replays_after_terminal(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    original = store.acquire_m5_job(epoch_id, 1, job)
    assert original.attempt is not None and original.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db.connection, original.lease_expires_at)
    takeover = store.acquire_m5_job(epoch_id, 2, job)
    assert takeover.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert takeover.attempt is not None
    result, output = _root_result_and_output(
        d24_requirement_db, job=job, attempt=original.attempt
    )
    attempt_work = _forward_attempt_work()
    attempt_timing = _attempt_timing()

    before_future = _full_snapshot(d24_requirement_db.connection)
    with pytest.raises(EventConflictError, match="newer than durable runtime"):
        store.stage_m5_discovery_result_atomically(
            epoch_id,
            4,
            original,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            attempt_work,
            attempt_timing,
            eligible_snapshot_exhausted=True,
        )
    assert _full_snapshot(d24_requirement_db.connection) == before_future

    def fail_after_sidecar(point: str) -> None:
        if point == "late_return_expired_sidecar_inserted":
            raise RuntimeError("rollback late expired return")

    with pytest.raises(RuntimeError, match="rollback late expired return"):
        store.stage_m5_discovery_result_atomically(
            epoch_id,
            3,
            original,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            attempt_work,
            attempt_timing,
            eligible_snapshot_exhausted=True,
            failure_injector=fail_after_sidecar,
        )
    assert _full_snapshot(d24_requirement_db.connection) == before_future

    work_before = store.current_event_work(epoch_id)
    timing_before = d24_requirement_db.connection.execute(
        """
        SELECT required_expected_count, required_observed_count,
               required_missing_count,
               postgres_server_execution_expected_count,
               postgres_server_execution_observed_count,
               postgres_server_execution_missing_count,
               postgres_lock_wait_expected_count,
               postgres_lock_wait_observed_count,
               postgres_lock_wait_missing_count,
               postgres_wal_bytes_expected_count,
               postgres_wal_bytes_observed_count,
               postgres_wal_bytes_missing_count,
               postgres_shared_block_reads_expected_count,
               postgres_shared_block_reads_observed_count,
               postgres_shared_block_reads_missing_count
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert timing_before is not None
    with d24_requirement_db.reconnect() as reconnected:
        receipt = PostgresM5RuntimeStore(
            reconnected
        ).stage_m5_discovery_result_atomically(
            epoch_id,
            3,
            original,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            attempt_work,
            attempt_timing,
            eligible_snapshot_exhausted=True,
        )
    assert receipt.disposition is M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
    assert receipt.resulting_revision == 3 and not receipt.exact_replay
    assert receipt.current_terminal_logical_result_hash is None
    assert receipt.transition_anchor is not None
    assert (
        receipt.transition_anchor.contribution_kind
        is M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN
    )
    assert receipt.transition_anchor.anchor_revision == 3

    row = d24_requirement_db.connection.execute(
        """
        SELECT attempt.attempt_state, attempt.attempt_output_digest,
               artifact.disposition, artifact.archive_reason,
               artifact.job_state_at_receipt, expired.received_after_terminal,
               evidence.disposition, execution.applied_revision,
               late.applied_revision,
               late.requirement_late_attempt_artifact_count,
               late.bytes_hashed, late.bytes_serialized,
               timing.pending_contribution_kind, timing.pending_source_id,
               timing.pending_anchor_revision, timing.updated_revision
        FROM groundloop_m5_job_attempt AS attempt
        JOIN groundloop_m5_attempt_result_artifact AS artifact
          ON artifact.attempt_id = attempt.attempt_id
        JOIN groundloop_m5_expired_attempt_return AS expired
          ON expired.attempt_id = attempt.attempt_id
         AND expired.subgraph = 'requirement'
        JOIN groundloop_m5_attempt_execution_evidence AS evidence
          ON evidence.attempt_id = attempt.attempt_id
         AND evidence.subgraph = 'requirement'
        JOIN groundloop_m5_runtime_work_contribution AS execution
          ON execution.epoch_id = evidence.epoch_id
         AND execution.contribution_kind = 'm5_attempt_execution'
         AND execution.source_id = attempt.attempt_id
        JOIN groundloop_m5_runtime_work_contribution AS late
          ON late.epoch_id = evidence.epoch_id
         AND late.contribution_kind = 'preterminal_late_return'
         AND late.source_id = attempt.attempt_id
        JOIN groundloop_m5_runtime_timing_accumulator AS timing
          ON timing.epoch_id = evidence.epoch_id
        WHERE attempt.attempt_id = %s
        """,
        (original.attempt.attempt_id,),
    ).fetchone()
    assert row is not None
    assert row[:10] == (
        "expired",
        None,
        "terminal_audit_only",
        "attempt_expired",
        "running",
        False,
        "returned",
        3,
        3,
        1,
    )
    assert row[10] > 0 and row[11] == row[10]
    assert row[12:] == (
        "preterminal_late_return",
        original.attempt.attempt_id,
        3,
        3,
    )
    late_work = M5RuntimeWork(
        requirement_late_attempt_artifact_count=1,
        bytes_hashed=int(row[10]),
        bytes_serialized=int(row[11]),
    )
    assert store.current_event_work(epoch_id) == _sum_work(
        work_before, attempt_work, late_work
    )
    timing_after = d24_requirement_db.connection.execute(
        """
        SELECT required_expected_count, required_observed_count,
               required_missing_count,
               postgres_server_execution_expected_count,
               postgres_server_execution_observed_count,
               postgres_server_execution_missing_count,
               postgres_lock_wait_expected_count,
               postgres_lock_wait_observed_count,
               postgres_lock_wait_missing_count,
               postgres_wal_bytes_expected_count,
               postgres_wal_bytes_observed_count,
               postgres_wal_bytes_missing_count,
               postgres_shared_block_reads_expected_count,
               postgres_shared_block_reads_observed_count,
               postgres_shared_block_reads_missing_count
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert timing_after is not None
    expected_timing_deltas = (
        2,
        1,
        1,
        2,
        1,
        1,
        2,
        0,
        2,
        2,
        1,
        1,
        2,
        0,
        2,
    )
    assert (
        tuple(
            int(after) - int(before)
            for before, after in zip(timing_before, timing_after, strict=True)
        )
        == expected_timing_deltas
    )

    cancelled_revision = _cancel_requirement_job_for_setup(
        d24_requirement_db, job=job, reason=M5TerminalReason.EPOCH_FAILED
    )
    terminal_hash = terminalize_recovered_epoch_for_storage_fixture(
        d24_requirement_db.connection, epoch_id=epoch_id
    )
    d24_requirement_db.connection.commit()
    before_replay = _full_snapshot(d24_requirement_db.connection)
    with d24_requirement_db.reconnect() as reconnected:
        replay = PostgresM5RuntimeStore(
            reconnected
        ).stage_m5_discovery_result_atomically(
            epoch_id,
            cancelled_revision + 1,
            original,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            attempt_work,
            attempt_timing,
            eligible_snapshot_exhausted=True,
        )
    assert replay.disposition is M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
    assert replay.exact_replay and replay.transition_anchor is None
    assert replay.current_terminal_logical_result_hash == terminal_hash
    assert _full_snapshot(d24_requirement_db.connection) == before_replay
    with pytest.raises(EventConflictError, match="immutable evidence"):
        store.stage_m5_discovery_result_atomically(
            epoch_id,
            cancelled_revision + 1,
            original,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
            M5RuntimeWork(),
            attempt_timing,
            eligible_snapshot_exhausted=True,
        )
    assert _full_snapshot(d24_requirement_db.connection) == before_replay


def test_still_current_cancellation_return_is_preterminal_audit_with_one_anchor(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    lease = store.acquire_m5_job(epoch_id, 1, job)
    assert lease.attempt is not None
    cancelled_revision = _cancel_requirement_job_for_setup(
        d24_requirement_db, job=job, reason=M5TerminalReason.SCOPE_RETIRED
    )
    assert cancelled_revision == 3
    result, output = _root_result_and_output(
        d24_requirement_db, job=job, attempt=lease.attempt
    )
    work_before = store.current_event_work(epoch_id)
    receipt = store.stage_m5_discovery_result_atomically(
        epoch_id,
        cancelled_revision,
        lease,
        job,
        result,
        output,
        M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
        M5RuntimeWork(),
        _attempt_timing(),
        eligible_snapshot_exhausted=True,
    )
    assert (
        receipt.disposition is M5RequirementReturnDisposition.TERMINAL_AUDIT_PRETERMINAL
    )
    assert receipt.resulting_revision == cancelled_revision
    assert not receipt.exact_replay and receipt.transition_anchor is not None
    assert (
        receipt.transition_anchor.contribution_kind
        is M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN
    )

    row = d24_requirement_db.connection.execute(
        """
        SELECT artifact.disposition, artifact.archive_reason,
               artifact.job_state_at_receipt,
               artifact.cancelled_by_event_id,
               artifact.cancelled_by_epoch_id, artifact.cancellation_reason,
               evidence.disposition,
               (SELECT count(*) FROM groundloop_m5_expired_attempt_return
                WHERE epoch_id = %s AND subgraph = 'requirement'
                  AND attempt_id = %s),
               execution.applied_revision, late.applied_revision,
               late.requirement_late_attempt_artifact_count,
               late.bytes_hashed, late.bytes_serialized,
               late.source_identity_hash,
               timing.pending_contribution_kind,
               timing.pending_source_id, timing.pending_anchor_revision
        FROM groundloop_m5_attempt_result_artifact AS artifact
        JOIN groundloop_m5_attempt_execution_evidence AS evidence
          ON evidence.attempt_id = artifact.attempt_id
         AND evidence.subgraph = 'requirement'
        JOIN groundloop_m5_runtime_work_contribution AS execution
          ON execution.epoch_id = evidence.epoch_id
         AND execution.contribution_kind = 'm5_attempt_execution'
         AND execution.source_id = evidence.attempt_id
        JOIN groundloop_m5_runtime_work_contribution AS late
          ON late.epoch_id = evidence.epoch_id
         AND late.contribution_kind = 'preterminal_late_return'
         AND late.source_id = evidence.attempt_id
        JOIN groundloop_m5_runtime_timing_accumulator AS timing
          ON timing.epoch_id = evidence.epoch_id
        WHERE artifact.attempt_id = %s
        """,
        (epoch_id, lease.attempt.attempt_id, lease.attempt.attempt_id),
    ).fetchone()
    assert row is not None
    assert row[:11] == (
        "terminal_audit_only",
        "job_already_terminal",
        "cancelled",
        d24_requirement_db.plan.structural_event_id,
        epoch_id,
        "scope_retired",
        "reused_artifact",
        0,
        cancelled_revision,
        cancelled_revision,
        1,
    )
    assert row[11] > 0 and row[12] == row[11]
    assert row[13] == receipt.return_artifact_digest
    assert row[14:] == (
        "preterminal_late_return",
        lease.attempt.attempt_id,
        cancelled_revision,
    )
    late_work = M5RuntimeWork(
        requirement_late_attempt_artifact_count=1,
        bytes_hashed=int(row[11]),
        bytes_serialized=int(row[12]),
    )
    assert store.current_event_work(epoch_id) == _sum_work(work_before, late_work)

    before_replay = _full_snapshot(d24_requirement_db.connection)
    with d24_requirement_db.reconnect() as reconnected:
        replay = PostgresM5RuntimeStore(
            reconnected
        ).stage_m5_discovery_result_atomically(
            epoch_id,
            cancelled_revision,
            lease,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
            M5RuntimeWork(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )
    assert replay.exact_replay and replay.transition_anchor is None
    assert (
        replay.disposition is M5RequirementReturnDisposition.TERMINAL_AUDIT_PRETERMINAL
    )
    assert _full_snapshot(d24_requirement_db.connection) == before_replay


@pytest.mark.parametrize(
    "cutoff",
    (
        "late_return_artifact_inserted",
        "late_return_expired_sidecar_inserted",
        "late_return_execution_evidence_inserted",
        "late_return_postterminal_timing_inserted",
        "late_return_postterminal_audit_inserted",
        "late_return_accounting_inserted",
        "late_return_constraints_validated",
    ),
)
def test_expired_postterminal_member_cutoffs_roll_back_and_reconnect(
    d24_requirement_db: Any,
    cutoff: str,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    original = store.acquire_m5_job(epoch_id, 1, job)
    assert original.attempt is not None and original.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db.connection, original.lease_expires_at)
    successor = store.acquire_m5_job(epoch_id, 2, job)
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    result, output = _root_result_and_output(
        d24_requirement_db,
        job=job,
        attempt=original.attempt,
    )
    cancelled_revision = _cancel_requirement_job_for_setup(
        d24_requirement_db,
        job=job,
        reason=M5TerminalReason.EPOCH_FAILED,
    )
    terminal_hash = terminalize_recovered_epoch_for_storage_fixture(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
    )
    d24_requirement_db.connection.commit()
    before = _full_snapshot(d24_requirement_db.connection)

    def fail_at_member(point: str) -> None:
        if point == cutoff:
            raise RuntimeError(f"rollback {cutoff}")

    with pytest.raises(RuntimeError, match=f"rollback {cutoff}"):
        store.stage_m5_discovery_result_atomically(
            epoch_id,
            cancelled_revision,
            original,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
            failure_injector=fail_at_member,
        )
    assert _full_snapshot(d24_requirement_db.connection) == before

    with d24_requirement_db.reconnect() as reconnected:
        receipt = PostgresM5RuntimeStore(
            reconnected
        ).stage_m5_discovery_result_atomically(
            epoch_id,
            cancelled_revision,
            original,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )
    assert not receipt.exact_replay
    assert receipt.disposition is M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
    assert receipt.current_terminal_logical_result_hash == terminal_hash
    counts = d24_requirement_db.connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_attempt_result_artifact
           WHERE attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_expired_attempt_return
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_attempt_execution_evidence
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_post_terminal_attempt_timing
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_post_terminal_attempt_audit
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_runtime_work_contribution
           WHERE epoch_id = %s AND source_id = %s
             AND contribution_kind IN (
                 'm5_attempt_execution', 'preterminal_late_return')),
          (SELECT count(*) FROM groundloop_m5_runtime_timing_contribution
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s)
        """,
        (
            original.attempt.attempt_id,
            epoch_id,
            original.attempt.attempt_id,
            epoch_id,
            original.attempt.attempt_id,
            epoch_id,
            original.attempt.attempt_id,
            epoch_id,
            original.attempt.attempt_id,
            epoch_id,
            original.attempt.attempt_id,
            epoch_id,
            original.attempt.attempt_id,
        ),
    ).fetchone()
    assert counts == (1, 1, 1, 1, 1, 0, 0)


@pytest.mark.parametrize(
    "execution_disposition",
    (
        M5ExecutionEvidenceDisposition.RETURNED,
        M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
    ),
)
def test_postterminal_terminal_audit_isolated_and_equal_zero_kinds_stay_distinct(
    d24_requirement_db: Any,
    execution_disposition: M5ExecutionEvidenceDisposition,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    lease = store.acquire_m5_job(epoch_id, 1, job)
    assert lease.attempt is not None
    cancelled_revision = _cancel_requirement_job_for_setup(
        d24_requirement_db, job=job, reason=M5TerminalReason.EPOCH_FAILED
    )
    result, output = _root_result_and_output(
        d24_requirement_db, job=job, attempt=lease.attempt
    )
    before_preterminal = _full_snapshot(d24_requirement_db.connection)
    with pytest.raises(
        EventConflictError, match="must terminalize before return audit"
    ):
        store.stage_m5_discovery_result_atomically(
            epoch_id,
            cancelled_revision,
            lease,
            job,
            result,
            output,
            execution_disposition,
            M5RuntimeWork(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )
    assert _full_snapshot(d24_requirement_db.connection) == before_preterminal

    terminal_hash = terminalize_recovered_epoch_for_storage_fixture(
        d24_requirement_db.connection, epoch_id=epoch_id
    )
    d24_requirement_db.connection.commit()
    terminal_before = _snapshot_tables(
        d24_requirement_db.connection, _TERMINAL_STATE_TABLES
    )
    full_before = _full_snapshot(d24_requirement_db.connection)

    def fail_after_artifact(point: str) -> None:
        if point == "late_return_artifact_inserted":
            raise RuntimeError("rollback postterminal audit")

    with pytest.raises(RuntimeError, match="rollback postterminal audit"):
        store.stage_m5_discovery_result_atomically(
            epoch_id,
            cancelled_revision,
            lease,
            job,
            result,
            output,
            execution_disposition,
            M5RuntimeWork(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
            failure_injector=fail_after_artifact,
        )
    assert _full_snapshot(d24_requirement_db.connection) == full_before

    with d24_requirement_db.reconnect() as reconnected:
        receipt = PostgresM5RuntimeStore(
            reconnected
        ).stage_m5_discovery_result_atomically(
            epoch_id,
            cancelled_revision,
            lease,
            job,
            result,
            output,
            execution_disposition,
            M5RuntimeWork(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )
    assert (
        receipt.disposition
        is M5RequirementReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL
    )
    assert receipt.resulting_revision == cancelled_revision + 1
    assert not receipt.exact_replay and receipt.transition_anchor is None
    assert receipt.current_terminal_logical_result_hash == terminal_hash
    assert (
        _snapshot_tables(d24_requirement_db.connection, _TERMINAL_STATE_TABLES)
        == terminal_before
    )

    row = d24_requirement_db.connection.execute(
        """
        SELECT attempt.attempt_state, artifact.disposition,
               artifact.archive_reason, artifact.cancellation_reason,
               evidence.disposition, evidence.attempt_work_digest,
               post_timing.required_interval_observed,
               audit.return_kind, audit.return_artifact_digest,
               audit.terminal_logical_result_hash,
               (SELECT count(*) FROM groundloop_m5_runtime_work_contribution
                WHERE epoch_id = %s AND source_id = %s
                  AND contribution_kind IN (
                      'm5_attempt_execution', 'preterminal_late_return')),
               (SELECT count(*) FROM groundloop_m5_runtime_timing_contribution
                WHERE epoch_id = %s AND subgraph = 'requirement'
                  AND attempt_id = %s),
               (SELECT count(*) FROM groundloop_m5_expired_attempt_return
                WHERE epoch_id = %s AND subgraph = 'requirement'
                  AND attempt_id = %s)
        FROM groundloop_m5_job_attempt AS attempt
        JOIN groundloop_m5_attempt_result_artifact AS artifact
          ON artifact.attempt_id = attempt.attempt_id
        JOIN groundloop_m5_attempt_execution_evidence AS evidence
          ON evidence.attempt_id = attempt.attempt_id
         AND evidence.subgraph = 'requirement'
        JOIN groundloop_m5_post_terminal_attempt_timing AS post_timing
          ON post_timing.epoch_id = evidence.epoch_id
         AND post_timing.attempt_id = evidence.attempt_id
         AND post_timing.subgraph = evidence.subgraph
        JOIN groundloop_m5_post_terminal_attempt_audit AS audit
          ON audit.epoch_id = evidence.epoch_id
         AND audit.attempt_id = evidence.attempt_id
         AND audit.subgraph = evidence.subgraph
        WHERE attempt.attempt_id = %s
        """,
        (
            epoch_id,
            lease.attempt.attempt_id,
            epoch_id,
            lease.attempt.attempt_id,
            epoch_id,
            lease.attempt.attempt_id,
            lease.attempt.attempt_id,
        ),
    ).fetchone()
    assert row == (
        "dispatched",
        "terminal_audit_only",
        "epoch_failed",
        "epoch_failed",
        execution_disposition.value,
        M5RuntimeWork().work_digest,
        True,
        "terminal_audit_only",
        receipt.return_artifact_digest,
        terminal_hash,
        0,
        0,
        0,
    )

    before_replay = _full_snapshot(d24_requirement_db.connection)
    replay = store.stage_m5_discovery_result_atomically(
        epoch_id,
        cancelled_revision + 1,
        lease,
        job,
        result,
        output,
        execution_disposition,
        M5RuntimeWork(),
        _attempt_timing(),
        eligible_snapshot_exhausted=True,
    )
    assert replay.exact_replay and replay.transition_anchor is None
    assert replay.current_terminal_logical_result_hash == terminal_hash
    assert _full_snapshot(d24_requirement_db.connection) == before_replay
    other_disposition = (
        M5ExecutionEvidenceDisposition.REUSED_ARTIFACT
        if execution_disposition is M5ExecutionEvidenceDisposition.RETURNED
        else M5ExecutionEvidenceDisposition.RETURNED
    )
    with pytest.raises(EventConflictError, match="immutable evidence"):
        store.stage_m5_discovery_result_atomically(
            epoch_id,
            cancelled_revision + 1,
            lease,
            job,
            result,
            output,
            other_disposition,
            M5RuntimeWork(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )
    assert _full_snapshot(d24_requirement_db.connection) == before_replay


def test_expired_postterminal_return_uses_only_audit_sidecars(
    d24_requirement_db: Any,
) -> None:
    """Prove C3 storage shape with a labelled non-production terminal fixture."""

    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    original = store.acquire_m5_job(epoch_id, 1, job)
    assert original.attempt is not None and original.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db.connection, original.lease_expires_at)
    takeover = store.acquire_m5_job(epoch_id, 2, job)
    assert takeover.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert takeover.attempt is not None
    result, output = _root_result_and_output(
        d24_requirement_db, job=job, attempt=original.attempt
    )
    cancelled_revision = _cancel_requirement_job_for_setup(
        d24_requirement_db, job=job, reason=M5TerminalReason.EPOCH_FAILED
    )
    for rejected_revision in (cancelled_revision - 1, cancelled_revision):
        before_preterminal_conflict = _full_snapshot(d24_requirement_db.connection)
        with pytest.raises(
            EventConflictError,
            match="nonrunning successor requires terminal event",
        ):
            store.stage_m5_discovery_result_atomically(
                epoch_id,
                rejected_revision,
                original,
                job,
                result,
                output,
                M5ExecutionEvidenceDisposition.RETURNED,
                _forward_attempt_work(),
                _attempt_timing(),
                eligible_snapshot_exhausted=True,
            )
        assert (
            _full_snapshot(d24_requirement_db.connection) == before_preterminal_conflict
        )

    # This SQL-only helper labels a storage-shape fixture. It is not evidence
    # for the R2 production failure/seal composition.
    terminal_hash = terminalize_recovered_epoch_for_storage_fixture(
        d24_requirement_db.connection, epoch_id=epoch_id
    )
    d24_requirement_db.connection.commit()
    terminal_before = _snapshot_tables(
        d24_requirement_db.connection, _TERMINAL_STATE_TABLES
    )
    receipt = store.stage_m5_discovery_result_atomically(
        epoch_id,
        cancelled_revision,
        original,
        job,
        result,
        output,
        M5ExecutionEvidenceDisposition.RETURNED,
        _forward_attempt_work(),
        _attempt_timing(),
        eligible_snapshot_exhausted=True,
    )
    assert receipt.disposition is M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
    assert receipt.resulting_revision == cancelled_revision + 1
    assert not receipt.exact_replay and receipt.transition_anchor is None
    assert receipt.current_terminal_logical_result_hash == terminal_hash
    assert (
        _snapshot_tables(d24_requirement_db.connection, _TERMINAL_STATE_TABLES)
        == terminal_before
    )

    row = d24_requirement_db.connection.execute(
        """
        SELECT attempt.attempt_state, attempt.attempt_output_digest,
               artifact.disposition, artifact.archive_reason,
               artifact.job_state_at_receipt, artifact.job_state_after,
               artifact.cancelled_by_event_id,
               artifact.cancelled_by_epoch_id,
               artifact.cancellation_reason,
               expired.received_after_terminal,
               expired.worker_output_digest, expired.worker_artifact_hash,
               evidence.disposition, evidence.attempt_work_digest,
               evidence.attempt_timing_digest,
               timing.attempt_timing_digest,
               audit.return_kind,
               audit.return_artifact_digest,
               audit.work_digest, audit.timing_digest,
               audit.terminal_logical_result_hash,
               (SELECT count(*) FROM groundloop_m5_runtime_work_contribution
                WHERE epoch_id = %s AND source_id = %s
                  AND contribution_kind IN (
                      'm5_attempt_execution', 'preterminal_late_return')),
               (SELECT count(*) FROM groundloop_m5_runtime_timing_contribution
                WHERE epoch_id = %s AND subgraph = 'requirement'
                  AND attempt_id = %s)
        FROM groundloop_m5_job_attempt AS attempt
        JOIN groundloop_m5_attempt_result_artifact AS artifact
          ON artifact.attempt_id = attempt.attempt_id
        JOIN groundloop_m5_expired_attempt_return AS expired
          ON expired.epoch_id = artifact.job_epoch_id
         AND expired.attempt_id = artifact.attempt_id
         AND expired.subgraph = 'requirement'
        JOIN groundloop_m5_attempt_execution_evidence AS evidence
          ON evidence.epoch_id = artifact.job_epoch_id
         AND evidence.attempt_id = artifact.attempt_id
         AND evidence.subgraph = 'requirement'
        JOIN groundloop_m5_post_terminal_attempt_timing AS timing
          ON timing.epoch_id = evidence.epoch_id
         AND timing.attempt_id = evidence.attempt_id
         AND timing.subgraph = evidence.subgraph
        JOIN groundloop_m5_post_terminal_attempt_audit AS audit
          ON audit.epoch_id = evidence.epoch_id
         AND audit.attempt_id = evidence.attempt_id
         AND audit.subgraph = evidence.subgraph
        WHERE attempt.attempt_id = %s
        """,
        (
            epoch_id,
            original.attempt.attempt_id,
            epoch_id,
            original.attempt.attempt_id,
            original.attempt.attempt_id,
        ),
    ).fetchone()
    assert row is not None
    assert row[:15] == (
        "expired",
        None,
        "terminal_audit_only",
        "attempt_expired",
        "cancelled",
        "cancelled",
        d24_requirement_db.plan.structural_event_id,
        epoch_id,
        "epoch_failed",
        True,
        output.attempt_output_digest,
        output.result_artifact_hash,
        "returned",
        _forward_attempt_work().work_digest,
        row[14],
    )
    assert row[14] == row[15] == row[19]
    assert row[16:] == (
        "expired_return",
        receipt.return_artifact_digest,
        _forward_attempt_work().work_digest,
        row[19],
        terminal_hash,
        0,
        0,
    )
    closure_counts = d24_requirement_db.connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_attempt_result_artifact
           WHERE attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_expired_attempt_return
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_attempt_execution_evidence
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_post_terminal_attempt_timing
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_post_terminal_attempt_audit
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s)
        """,
        (
            original.attempt.attempt_id,
            epoch_id,
            original.attempt.attempt_id,
            epoch_id,
            original.attempt.attempt_id,
            epoch_id,
            original.attempt.attempt_id,
            epoch_id,
            original.attempt.attempt_id,
        ),
    ).fetchone()
    assert closure_counts == (1, 1, 1, 1, 1)

    before_replay = _full_snapshot(d24_requirement_db.connection)
    with d24_requirement_db.reconnect() as reconnected:
        replay = PostgresM5RuntimeStore(
            reconnected
        ).stage_m5_discovery_result_atomically(
            epoch_id,
            cancelled_revision + 1,
            original,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )
    assert replay.exact_replay and replay.transition_anchor is None
    assert replay.disposition is M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
    assert replay.current_terminal_logical_result_hash == terminal_hash
    assert _full_snapshot(d24_requirement_db.connection) == before_replay

    changed_hit = M5RequirementChannelHit.build(
        epoch_id=result.channel_hits[0].epoch_id,
        root_job_id=result.channel_hits[0].root_job_id,
        scope_contract_digest=result.channel_hits[0].scope_contract_digest,
        pair=result.channel_hits[0].pair,
        candidate_policy_id=result.channel_hits[0].candidate_policy_id,
        channel=result.channel_hits[0].channel,
        rank=result.channel_hits[0].rank,
        score=result.channel_hits[0].score,
        channel_artifact_hash=_sha("changed-postterminal-expired-hit"),
    )
    changed_result = M5RequirementDiscoveryResult.build(
        root_job_id=result.root_job_id,
        scope_contract_digest=result.scope_contract_digest,
        termination=result.termination,
        channel_hits=(changed_hit,),
        selections=result.selections,
    )
    changed_output = M5AttemptOutput.build(
        attempt=original.attempt,
        job_epoch_id=epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=changed_result.result_artifact_id,
        result_artifact_hash=changed_result.result_artifact_hash,
    )
    conflicting_calls = (
        (
            result,
            output,
            M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
            M5RuntimeWork(),
            _attempt_timing(),
        ),
        (
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            M5RuntimeWork(
                requirement_forward_retrieval_call_count=1,
                bytes_hashed=18,
                bytes_serialized=19,
                embedding_model_call_count=1,
                embedding_input_token_count=7,
            ),
            _attempt_timing(),
        ),
        (
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            replace(_attempt_timing(), neural_wall_ns=37),
        ),
        (
            changed_result,
            changed_output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
        ),
    )
    for (
        conflicting_result,
        conflicting_output,
        conflicting_disposition,
        conflicting_work,
        conflicting_timing,
    ) in conflicting_calls:
        with pytest.raises(EventConflictError, match="immutable evidence"):
            store.stage_m5_discovery_result_atomically(
                epoch_id,
                cancelled_revision + 1,
                original,
                job,
                conflicting_result,
                conflicting_output,
                conflicting_disposition,
                conflicting_work,
                conflicting_timing,
                eligible_snapshot_exhausted=True,
            )
        assert _full_snapshot(d24_requirement_db.connection) == before_replay


def _prepare_verifier_attempt(database: Any) -> tuple[PostgresM5RuntimeStore, Any]:
    """Stage the root, close the barrier, and dispatch its durable verifier."""

    store = PostgresM5RuntimeStore(database.connection)
    revision, _root_lease, _result, _output = _acquire_and_stage_root(
        database,
        store=store,
        expected_revision=1,
        job=database.jobs[0],
    )
    assert revision == 3
    root_row = database.connection.execute(
        """
        SELECT requirement_root_set_hash
        FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
        """,
        (database.epoch_id,),
    ).fetchone()
    assert root_row is not None
    barrier = store.close_m5_requirement_roots_atomically(
        database.epoch_id,
        revision,
        str(root_row[0]).strip(),
    )
    assert barrier.resulting_revision == 4 and not barrier.exact_replay
    jobs = store.verifier_jobs(database.epoch_id)
    assert len(jobs) == 1
    verifier_job = jobs[0]
    lease = store.acquire_m5_job(database.epoch_id, 4, verifier_job)
    assert lease.attempt is not None and lease.should_execute
    assert lease.resulting_revision == 5
    fixture = build_d24_verifier_fixture(
        database,
        lease=lease,
        job=verifier_job,
    )
    return store, fixture


def _verifier_attempt_work(
    disposition: M5ExecutionEvidenceDisposition,
) -> M5RuntimeWork:
    if disposition is M5ExecutionEvidenceDisposition.REUSED_ARTIFACT:
        return M5RuntimeWork(bytes_hashed=41, bytes_serialized=43)
    return M5RuntimeWork(
        requirement_verifier_call_count=1,
        verifier_model_call_count=1,
        verifier_input_token_count=11,
        verifier_output_token_count=7,
        bytes_hashed=41,
        bytes_serialized=43,
    )


def _inactivate_verifier_subject(database: Any, fixture: Any) -> None:
    """Use the accepted staged-lifecycle failure transition as test setup."""

    connection = database.connection
    connection.execute(
        """
        UPDATE groundloop_m5_requirement_version
        SET lifecycle_state = 'FAILED'
        WHERE requirement_version_id = %s AND lifecycle_state = 'STAGED'
        """,
        (fixture.pair_input.pair.subject_id,),
    )
    connection.execute(
        """
        UPDATE groundloop_m5_group_version
        SET lifecycle_state = 'FAILED'
        WHERE group_version_id = %s AND lifecycle_state = 'STAGED'
        """,
        (fixture.pair_input.group_version_id,),
    )
    connection.execute(
        """
        UPDATE groundloop_m5_group_family
        SET lifecycle_state = 'FAILED'
        WHERE group_family_id = %s AND lifecycle_state = 'STAGED'
        """,
        (fixture.pair_input.group_family_id,),
    )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()


def _complete_verifier(
    store: PostgresM5RuntimeStore,
    database: Any,
    fixture: Any,
    *,
    expected_revision: int,
    disposition: M5ExecutionEvidenceDisposition,
    timing: M5RuntimeTiming | None,
    failure_injector: Any | None = None,
) -> Any:
    return store.complete_m5_verifier_atomically(
        database.epoch_id,
        expected_revision,
        fixture.lease,
        fixture.job,
        fixture.pair_input,
        fixture.verifier_artifact,
        fixture.attempt_output,
        disposition,
        _verifier_attempt_work(disposition),
        timing,
        failure_injector=failure_injector,
    )


def _verifier_fixture_with_changed_immutable_core(
    database: Any, fixture: Any
) -> tuple[M5RequirementPairInput, M5RequirementVerifierArtifact, M5AttemptOutput]:
    """Re-digest a wrong ordinal through the complete verifier envelope."""

    pair_input = fixture.pair_input
    changed_ordinal = pair_input.requirement_ordinal + 1
    changed_input_hash = digests.requirement_pair_input_digest(
        semantic_pair_digest_value=pair_input.semantic_pair_digest,
        scope_contract_digest=pair_input.scope_contract_digest,
        candidate_policy_id=pair_input.candidate_policy_id,
        owner_claim_id=pair_input.owner_claim_id,
        group_version_id=pair_input.group_version_id,
        group_family_id=pair_input.group_family_id,
        requirement_ordinal=changed_ordinal,
        normalized_requirement_text=pair_input.normalized_requirement_text,
        requirement_text_hash=pair_input.requirement_text_hash,
        document_version_id=pair_input.document_version_id,
        chunk_index=pair_input.chunk_index,
        chunk_text=pair_input.chunk_text,
        stored_chunk_text_hash=pair_input.stored_chunk_text_hash,
        m5_chunk_text_hash=pair_input.m5_chunk_text_hash,
        chunker_artifact_id=pair_input.chunker_artifact_id,
        normalizer_id=pair_input.normalizer_id,
        normalizer_provenance_hash=pair_input.normalizer_provenance_hash,
    )
    changed_input = replace(
        pair_input,
        requirement_ordinal=changed_ordinal,
        pair_input_hash=changed_input_hash,
    )
    artifact = fixture.verifier_artifact
    changed_artifact_hash = digests.requirement_verifier_result_digest(
        semantic_pair_digest_value=artifact.semantic_pair_digest,
        pair_input_hash=changed_input_hash,
        execution_spec_hash=artifact.execution_spec_hash,
        model_artifact_id=artifact.model_artifact_id,
        model_id=artifact.model_id,
        model_revision=artifact.model_revision,
        prompt_artifact_id=artifact.prompt_artifact_id,
        prompt_version=artifact.prompt_version,
        calibration_version=artifact.calibration_version,
        calibration_artifact_hash=artifact.calibration_artifact_hash,
        temperature=artifact.temperature,
        decision_policy_version=artifact.decision_policy_version,
        decision_policy_hash=artifact.decision_policy_hash,
        support_score=artifact.support_score,
        refute_score=artifact.refute_score,
        neutral_score=artifact.neutral_score,
        raw_logits=artifact.raw_logits,
        raw_output_hash=artifact.raw_output_hash,
        operational_label=artifact.operational_label,
    )
    changed_artifact = replace(
        artifact,
        pair_input_hash=changed_input_hash,
        artifact_hash=changed_artifact_hash,
        artifact_id=digests.requirement_verifier_artifact_id(
            artifact.semantic_pair_digest,
            changed_input_hash,
            changed_artifact_hash,
        ),
    )
    assert fixture.lease.attempt is not None
    changed_output = M5AttemptOutput.build(
        attempt=fixture.lease.attempt,
        job_epoch_id=database.epoch_id,
        payload_hash=fixture.job.payload_hash,
        result_artifact_id=changed_artifact.artifact_id,
        result_artifact_hash=changed_artifact.artifact_hash,
    )
    return changed_input, changed_artifact, changed_output


def test_verifier_all_active_first_write_is_d25_blocked_without_mutation(
    d24_requirement_db: Any,
) -> None:
    store, fixture = _prepare_verifier_attempt(d24_requirement_db)
    before = _full_snapshot(d24_requirement_db.connection)

    with pytest.raises(ValidationError, match="M5-D25 persisted matching"):
        _complete_verifier(
            store,
            d24_requirement_db,
            fixture,
            expected_revision=5,
            disposition=M5ExecutionEvidenceDisposition.RETURNED,
            timing=_attempt_timing(),
        )

    assert _full_snapshot(d24_requirement_db.connection) == before
    assert d24_requirement_db.connection.execute(
        """
        SELECT count(*) FROM groundloop_m5_requirement_verifier_execution
        WHERE logical_job_id = %s AND attempt_id = %s
        """,
        (fixture.job.logical_job_id, fixture.lease.attempt.attempt_id),
    ).fetchone() == (0,)


@pytest.mark.parametrize(
    ("disposition", "timing"),
    (
        (M5ExecutionEvidenceDisposition.RETURNED, _attempt_timing()),
        (M5ExecutionEvidenceDisposition.REUSED_ARTIFACT, None),
    ),
)
def test_inactive_verifier_persists_exact_closure_and_replays(
    d24_requirement_db: Any,
    disposition: M5ExecutionEvidenceDisposition,
    timing: M5RuntimeTiming | None,
) -> None:
    store, fixture = _prepare_verifier_attempt(d24_requirement_db)
    assert fixture.lease.attempt is not None
    _inactivate_verifier_subject(d24_requirement_db, fixture)
    prior_work = store.current_event_work(d24_requirement_db.epoch_id)

    receipt = _complete_verifier(
        store,
        d24_requirement_db,
        fixture,
        expected_revision=5,
        disposition=disposition,
        timing=timing,
    )
    assert receipt.disposition is M5RequirementReturnDisposition.APPLIED
    assert not receipt.exact_replay and receipt.resulting_revision == 6
    assert receipt.transition_anchor is not None
    assert (
        receipt.transition_anchor.contribution_kind
        is M5RuntimeWorkContributionKind.VERIFIER_COMPLETION
    )

    closure = d24_requirement_db.connection.execute(
        """
        SELECT job.job_state, job.archive_reason, job.completed_revision,
               attempt.attempt_state, attempt.attempt_output_digest,
               result.disposition, result.archive_reason,
               result.requirement_active, result.group_active,
               execution.eligible_for_currency,
               observation.eligible_for_currency,
               evidence.disposition, evidence.attempt_work_digest,
               contribution.contribution_kind,
               contribution.source_id, contribution.source_identity_hash,
               contribution.applied_revision,
               owner.verifier_job_count, owner.updated_revision,
               answer.verifier_job_count, answer.updated_revision,
               timing.pending_contribution_kind,
               timing.pending_source_id, timing.pending_anchor_revision,
               timing.updated_revision
        FROM groundloop_m5_semantic_job AS job
        JOIN groundloop_m5_job_attempt AS attempt
          ON attempt.logical_job_id = job.logical_job_id
        JOIN groundloop_m5_attempt_result_artifact AS result
          ON result.attempt_id = attempt.attempt_id
        JOIN groundloop_m5_requirement_verifier_execution AS execution
          ON execution.logical_job_id = job.logical_job_id
         AND execution.attempt_id = attempt.attempt_id
        JOIN groundloop_semantic_observation AS observation
          ON observation.observation_id = execution.observation_id
        JOIN groundloop_m5_attempt_execution_evidence AS evidence
          ON evidence.epoch_id = job.epoch_id
         AND evidence.attempt_id = attempt.attempt_id
        JOIN groundloop_m5_runtime_work_contribution AS contribution
          ON contribution.epoch_id = job.epoch_id
         AND contribution.contribution_kind = 'verifier_completion'
         AND contribution.source_id = attempt.attempt_id
        JOIN groundloop_m5_owner_pending_counter AS owner
          ON owner.epoch_id = job.epoch_id
        JOIN groundloop_m5_answer_pending_counter AS answer
          ON answer.epoch_id = job.epoch_id
        JOIN groundloop_m5_runtime_timing_accumulator AS timing
          ON timing.epoch_id = job.epoch_id
        WHERE job.epoch_id = %s AND job.logical_job_id = %s
        """,
        (d24_requirement_db.epoch_id, fixture.job.logical_job_id),
    ).fetchone()
    assert closure is not None
    assert tuple(closure[:14]) == (
        "completed_inactive",
        "subject_inactive",
        6,
        "completed",
        fixture.attempt_output.attempt_output_digest,
        "verifier_completed_inactive",
        "subject_inactive",
        False,
        False,
        False,
        False,
        disposition.value,
        _verifier_attempt_work(disposition).work_digest,
        "verifier_completion",
    )
    assert closure[14] == fixture.lease.attempt.attempt_id
    assert closure[15] == receipt.return_artifact_digest
    assert tuple(closure[16:]) == (
        6,
        0,
        6,
        0,
        6,
        "verifier_completion",
        fixture.lease.attempt.attempt_id,
        6,
        6,
    )
    event_work = store.current_event_work(d24_requirement_db.epoch_id)
    assert event_work.requirement_observation_artifact_count == (
        prior_work.requirement_observation_artifact_count + 1
    )
    assert event_work.requirement_effective_observation_count == (
        prior_work.requirement_effective_observation_count
    )
    assert event_work.requirement_inactive_completion_count == (
        prior_work.requirement_inactive_completion_count + 1
    )

    before_replay = _full_snapshot(d24_requirement_db.connection)
    immediate = _complete_verifier(
        store,
        d24_requirement_db,
        fixture,
        expected_revision=5,
        disposition=disposition,
        timing=timing,
    )
    assert immediate.exact_replay and immediate.resulting_revision == 6
    assert _full_snapshot(d24_requirement_db.connection) == before_replay

    with d24_requirement_db.reconnect() as reconnected:
        later = _complete_verifier(
            PostgresM5RuntimeStore(reconnected),
            d24_requirement_db,
            fixture,
            expected_revision=6,
            disposition=disposition,
            timing=timing,
        )
    assert later.exact_replay and later.resulting_revision == 6
    assert _full_snapshot(d24_requirement_db.connection) == before_replay

    conflicts = (
        (
            M5ExecutionEvidenceDisposition.REUSED_ARTIFACT
            if disposition is M5ExecutionEvidenceDisposition.RETURNED
            else M5ExecutionEvidenceDisposition.RETURNED,
            _verifier_attempt_work(
                M5ExecutionEvidenceDisposition.REUSED_ARTIFACT
                if disposition is M5ExecutionEvidenceDisposition.RETURNED
                else M5ExecutionEvidenceDisposition.RETURNED
            ),
            timing,
        ),
        (
            disposition,
            replace(
                _verifier_attempt_work(disposition),
                bytes_hashed=47,
                work_digest="",
            ),
            timing,
        ),
        (
            disposition,
            _verifier_attempt_work(disposition),
            _attempt_timing() if timing is None else None,
        ),
    )
    for changed_disposition, changed_work, changed_timing in conflicts:
        with pytest.raises(EventConflictError, match="immutable evidence"):
            store.complete_m5_verifier_atomically(
                d24_requirement_db.epoch_id,
                6,
                fixture.lease,
                fixture.job,
                fixture.pair_input,
                fixture.verifier_artifact,
                fixture.attempt_output,
                changed_disposition,
                changed_work,
                changed_timing,
            )
        assert _full_snapshot(d24_requirement_db.connection) == before_replay

    changed_fixture = build_d24_verifier_fixture(
        d24_requirement_db,
        lease=fixture.lease,
        job=fixture.job,
        raw_output_tag="changed",
    )
    with pytest.raises((EventConflictError, ValidationError)):
        store.complete_m5_verifier_atomically(
            d24_requirement_db.epoch_id,
            6,
            fixture.lease,
            fixture.job,
            changed_fixture.pair_input,
            changed_fixture.verifier_artifact,
            changed_fixture.attempt_output,
            disposition,
            _verifier_attempt_work(disposition),
            timing,
        )
    assert _full_snapshot(d24_requirement_db.connection) == before_replay


@pytest.mark.parametrize(
    "cutoff",
    (
        "verifier_accounting_inserted",
        "verifier_output_reserved",
        "verifier_pair_input_inserted",
        "verifier_artifact_inserted",
        "verifier_observation_inserted",
        "verifier_execution_inserted",
        "verifier_attempt_artifact_inserted",
        "verifier_attempt_completed",
        "verifier_job_completed",
        "verifier_pending_updated",
        "verifier_contribution_inserted",
        "verifier_epoch_revision_advanced",
        "verifier_revision_advanced",
        "verifier_constraints_validated",
        "requirement_work_accumulator_updated",
        "requirement_timing_accumulator_updated",
    ),
)
def test_inactive_verifier_cutoffs_roll_back_every_owned_row(
    d24_requirement_db: Any,
    cutoff: str,
) -> None:
    store, fixture = _prepare_verifier_attempt(d24_requirement_db)
    _inactivate_verifier_subject(d24_requirement_db, fixture)
    before = _full_snapshot(d24_requirement_db.connection)

    def fail(point: str) -> None:
        if point == cutoff:
            raise RuntimeError(f"rollback {cutoff}")

    with pytest.raises(RuntimeError, match=f"rollback {cutoff}"):
        _complete_verifier(
            store,
            d24_requirement_db,
            fixture,
            expected_revision=5,
            disposition=M5ExecutionEvidenceDisposition.RETURNED,
            timing=_attempt_timing(),
            failure_injector=fail,
        )
    assert _full_snapshot(d24_requirement_db.connection) == before

    with d24_requirement_db.reconnect() as reconnected:
        receipt = _complete_verifier(
            PostgresM5RuntimeStore(reconnected),
            d24_requirement_db,
            fixture,
            expected_revision=5,
            disposition=M5ExecutionEvidenceDisposition.RETURNED,
            timing=_attempt_timing(),
        )
    assert not receipt.exact_replay and receipt.resulting_revision == 6


def _verifier_semantic_closure_counts(
    database: Any,
    fixture: Any,
) -> tuple[int, int, int, int]:
    row = database.connection.execute(
        """
        SELECT
          (SELECT count(*)
           FROM groundloop_m5_requirement_pair_input
           WHERE pair_input_hash = %s),
          (SELECT count(*)
           FROM groundloop_m5_requirement_verifier_artifact
           WHERE artifact_id = %s),
          (SELECT count(*)
           FROM groundloop_m5_requirement_verifier_execution
           WHERE logical_job_id = %s AND attempt_id = %s),
          (SELECT count(*)
           FROM groundloop_semantic_observation
           WHERE observation_id = %s)
        """,
        (
            fixture.pair_input.pair_input_hash,
            fixture.verifier_artifact.artifact_id,
            fixture.job.logical_job_id,
            fixture.lease.attempt.attempt_id,
            fixture.verifier_artifact.to_semantic_observation().observation_id,
        ),
    ).fetchone()
    assert row is not None
    return (int(row[0]), int(row[1]), int(row[2]), int(row[3]))


def test_expired_verifier_preterminal_is_same_revision_audit_without_semantics(
    d24_requirement_db: Any,
) -> None:
    store, fixture = _prepare_verifier_attempt(d24_requirement_db)
    epoch_id = d24_requirement_db.epoch_id
    assert fixture.lease.attempt is not None
    assert fixture.lease.lease_expires_at is not None
    _wait_until_expired(
        d24_requirement_db.connection,
        fixture.lease.lease_expires_at,
    )
    takeover = store.acquire_m5_job(epoch_id, 5, fixture.job)
    assert takeover.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert takeover.attempt is not None and takeover.resulting_revision == 6
    assert takeover.attempt.attempt_ordinal == 2
    assert takeover.attempt.attempt_id != fixture.lease.attempt.attempt_id
    work = _verifier_attempt_work(M5ExecutionEvidenceDisposition.RETURNED)
    timing = _attempt_timing()
    semantic_before = _snapshot_tables(
        d24_requirement_db.connection, _LATE_SEMANTIC_TABLES
    )

    receipt = _complete_verifier(
        store,
        d24_requirement_db,
        fixture,
        expected_revision=6,
        disposition=M5ExecutionEvidenceDisposition.RETURNED,
        timing=timing,
    )
    assert receipt.disposition is M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
    assert receipt.resulting_revision == 6 and not receipt.exact_replay
    assert receipt.transition_anchor is not None
    assert (
        receipt.transition_anchor.contribution_kind
        is M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN
    )
    assert receipt.transition_anchor.anchor_revision == 6
    assert _verifier_semantic_closure_counts(d24_requirement_db, fixture) == (
        0,
        0,
        0,
        0,
    )
    assert (
        _snapshot_tables(d24_requirement_db.connection, _LATE_SEMANTIC_TABLES)
        == semantic_before
    )

    closure = d24_requirement_db.connection.execute(
        """
        SELECT base.revision, runtime.revision,
               attempt.attempt_state, attempt.attempt_output_digest,
               artifact.disposition, artifact.archive_reason,
               expired.received_after_terminal,
               evidence.disposition, evidence.attempt_work_digest,
               execution.applied_revision, late.applied_revision,
               timing.pending_contribution_kind,
               timing.pending_source_id, timing.pending_anchor_revision,
               timing.updated_revision,
               (SELECT count(*)
                FROM groundloop_m5_transition_call_timing
                WHERE epoch_id = %s
                  AND contribution_kind = 'preterminal_late_return'
                  AND source_id = %s)
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_job_attempt AS attempt
          ON attempt.logical_job_id = %s AND attempt.attempt_id = %s
        JOIN groundloop_m5_attempt_result_artifact AS artifact
          ON artifact.attempt_id = attempt.attempt_id
        JOIN groundloop_m5_expired_attempt_return AS expired
          ON expired.epoch_id = runtime.epoch_id
         AND expired.attempt_id = attempt.attempt_id
         AND expired.subgraph = 'requirement'
        JOIN groundloop_m5_attempt_execution_evidence AS evidence
          ON evidence.epoch_id = runtime.epoch_id
         AND evidence.attempt_id = attempt.attempt_id
         AND evidence.subgraph = 'requirement'
        JOIN groundloop_m5_runtime_work_contribution AS execution
          ON execution.epoch_id = runtime.epoch_id
         AND execution.contribution_kind = 'm5_attempt_execution'
         AND execution.source_id = attempt.attempt_id
        JOIN groundloop_m5_runtime_work_contribution AS late
          ON late.epoch_id = runtime.epoch_id
         AND late.contribution_kind = 'preterminal_late_return'
         AND late.source_id = attempt.attempt_id
        JOIN groundloop_m5_runtime_timing_accumulator AS timing
          ON timing.epoch_id = runtime.epoch_id
        WHERE base.epoch_id = %s
        """,
        (
            epoch_id,
            fixture.lease.attempt.attempt_id,
            fixture.job.logical_job_id,
            fixture.lease.attempt.attempt_id,
            epoch_id,
        ),
    ).fetchone()
    assert closure == (
        6,
        6,
        "expired",
        None,
        "terminal_audit_only",
        "attempt_expired",
        False,
        "returned",
        work.work_digest,
        6,
        6,
        "preterminal_late_return",
        fixture.lease.attempt.attempt_id,
        6,
        6,
        0,
    )

    before_replay = _full_snapshot(d24_requirement_db.connection)
    replay = _complete_verifier(
        store,
        d24_requirement_db,
        fixture,
        expected_revision=6,
        disposition=M5ExecutionEvidenceDisposition.RETURNED,
        timing=timing,
    )
    assert replay.exact_replay
    assert replay.disposition is M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
    assert replay.transition_anchor is None and replay.resulting_revision == 6
    assert _full_snapshot(d24_requirement_db.connection) == before_replay

    conflicts = (
        (
            M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
            _verifier_attempt_work(M5ExecutionEvidenceDisposition.REUSED_ARTIFACT),
            timing,
        ),
        (
            M5ExecutionEvidenceDisposition.RETURNED,
            replace(work, bytes_serialized=47, work_digest=""),
            timing,
        ),
        (M5ExecutionEvidenceDisposition.RETURNED, work, None),
    )
    for disposition, changed_work, changed_timing in conflicts:
        with pytest.raises(EventConflictError, match="immutable evidence"):
            store.complete_m5_verifier_atomically(
                epoch_id,
                6,
                fixture.lease,
                fixture.job,
                fixture.pair_input,
                fixture.verifier_artifact,
                fixture.attempt_output,
                disposition,
                changed_work,
                changed_timing,
            )
        assert _full_snapshot(d24_requirement_db.connection) == before_replay

    changed_input, changed_artifact, changed_output = (
        _verifier_fixture_with_changed_immutable_core(d24_requirement_db, fixture)
    )
    with pytest.raises(EventConflictError, match="immutable core rows"):
        store.complete_m5_verifier_atomically(
            epoch_id,
            6,
            fixture.lease,
            fixture.job,
            changed_input,
            changed_artifact,
            changed_output,
            M5ExecutionEvidenceDisposition.RETURNED,
            work,
            timing,
        )
    assert _full_snapshot(d24_requirement_db.connection) == before_replay
    assert _verifier_semantic_closure_counts(d24_requirement_db, fixture) == (
        0,
        0,
        0,
        0,
    )


def test_retryable_verifier_successor_rejects_then_reacquires_running(
    d24_requirement_db: Any,
) -> None:
    """C4 case 3 applies to an expired verifier output as well."""

    store, fixture = _prepare_verifier_attempt(d24_requirement_db)
    epoch_id = d24_requirement_db.epoch_id
    assert fixture.lease.attempt is not None
    assert fixture.lease.lease_expires_at is not None
    _wait_until_expired(
        d24_requirement_db.connection,
        fixture.lease.lease_expires_at,
    )
    takeover = store.acquire_m5_job(epoch_id, 5, fixture.job)
    assert takeover.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert takeover.attempt is not None and takeover.resulting_revision == 6
    failed = store.mark_m5_retryable_failure(
        epoch_id,
        6,
        takeover,
        _sha("c4-retryable-verifier-successor"),
        _verifier_attempt_work(M5ExecutionEvidenceDisposition.RETURNED),
        _attempt_timing(),
    )
    assert failed.resulting_revision == 7 and not failed.exact_replay

    def old_completion(expected_revision: int) -> Any:
        return _complete_verifier(
            store,
            d24_requirement_db,
            fixture,
            expected_revision=expected_revision,
            disposition=M5ExecutionEvidenceDisposition.RETURNED,
            timing=_attempt_timing(),
        )

    _assert_expired_successor_rejections(
        d24_requirement_db,
        call=old_completion,
        stale_revision=6,
        current_revision=7,
    )
    assert _late_requirement_row_counts(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        attempt_id=fixture.lease.attempt.attempt_id,
    ) == (0, 0, 0, 0, 0)

    retry = store.acquire_m5_job(epoch_id, 7, fixture.job)
    assert retry.disposition is M5AcquisitionDisposition.DISPATCH_NEW
    assert retry.attempt is not None and retry.resulting_revision == 8
    assert retry.attempt.attempt_ordinal == 3
    semantic_before = _snapshot_tables(
        d24_requirement_db.connection, _LATE_SEMANTIC_TABLES
    )
    receipt = old_completion(8)
    assert receipt.disposition is M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
    assert receipt.resulting_revision == 8 and not receipt.exact_replay
    assert receipt.transition_anchor is not None
    assert (
        _snapshot_tables(d24_requirement_db.connection, _LATE_SEMANTIC_TABLES)
        == semantic_before
    )
    assert d24_requirement_db.connection.execute(
        """
        SELECT job.job_state, latest.attempt_ordinal, latest.attempt_state,
               artifact.job_state_at_receipt, artifact.job_state_after,
               artifact.archive_reason, expired.received_after_terminal
        FROM groundloop_m5_semantic_job AS job
        JOIN groundloop_m5_job_attempt AS latest
          ON latest.logical_job_id = job.logical_job_id
         AND latest.attempt_ordinal = 3
        JOIN groundloop_m5_attempt_result_artifact AS artifact
          ON artifact.attempt_id = %s
        JOIN groundloop_m5_expired_attempt_return AS expired
          ON expired.epoch_id = job.epoch_id
         AND expired.attempt_id = artifact.attempt_id
         AND expired.subgraph = 'requirement'
        WHERE job.epoch_id = %s AND job.logical_job_id = %s
        """,
        (
            fixture.lease.attempt.attempt_id,
            epoch_id,
            fixture.job.logical_job_id,
        ),
    ).fetchone() == (
        "running",
        3,
        "dispatched",
        "running",
        "running",
        "attempt_expired",
        False,
    )

    before_replay = _full_snapshot(d24_requirement_db.connection)
    with d24_requirement_db.reconnect() as reconnected:
        replay = _complete_verifier(
            PostgresM5RuntimeStore(reconnected),
            d24_requirement_db,
            fixture,
            expected_revision=8,
            disposition=M5ExecutionEvidenceDisposition.RETURNED,
            timing=_attempt_timing(),
        )
    assert replay.exact_replay and replay.transition_anchor is None
    assert replay.disposition is M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
    assert _full_snapshot(d24_requirement_db.connection) == before_replay


@pytest.mark.parametrize(
    "successor_state",
    ("completed_inactive", "terminal_failed"),
)
def test_nonrunning_verifier_successor_delays_expired_output_until_terminal(
    d24_requirement_db: Any,
    successor_state: str,
) -> None:
    """C4 case 4 rejects every verifier state currently reachable before D25."""

    store, fixture = _prepare_verifier_attempt(d24_requirement_db)
    epoch_id = d24_requirement_db.epoch_id
    assert fixture.lease.attempt is not None
    assert fixture.lease.lease_expires_at is not None
    _wait_until_expired(
        d24_requirement_db.connection,
        fixture.lease.lease_expires_at,
    )
    takeover = store.acquire_m5_job(epoch_id, 5, fixture.job)
    assert takeover.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert takeover.attempt is not None and takeover.resulting_revision == 6

    if successor_state == "completed_inactive":
        successor_fixture = build_d24_verifier_fixture(
            d24_requirement_db,
            lease=takeover,
            job=fixture.job,
            raw_output_tag="c4-inactive-successor",
        )
        _inactivate_verifier_subject(d24_requirement_db, successor_fixture)
        settled = _complete_verifier(
            store,
            d24_requirement_db,
            successor_fixture,
            expected_revision=6,
            disposition=M5ExecutionEvidenceDisposition.RETURNED,
            timing=_attempt_timing(),
        )
        assert settled.disposition is M5RequirementReturnDisposition.APPLIED
        assert settled.resulting_revision == 7 and not settled.exact_replay
    else:
        settled = store.mark_m5_terminal_failure(
            epoch_id,
            6,
            takeover,
            M5TerminalReason.VERIFIER_ERROR,
            _sha("c4-terminal-verifier-successor"),
            _verifier_attempt_work(M5ExecutionEvidenceDisposition.RETURNED),
            _attempt_timing(),
        )
        assert settled.resulting_revision == 7 and not settled.exact_replay

    assert d24_requirement_db.connection.execute(
        """
        SELECT runtime.runtime_state, runtime.revision, job.job_state,
               parent.scope_state
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
        JOIN groundloop_m5_discovery_scope AS parent
          ON parent.epoch_id = job.epoch_id
         AND parent.root_job_id = job.parent_job_id
        WHERE runtime.epoch_id = %s AND job.logical_job_id = %s
        """,
        (epoch_id, fixture.job.logical_job_id),
    ).fetchone() == ("semantic_pending", 7, successor_state, "closed_active")

    def old_completion(expected_revision: int) -> Any:
        return _complete_verifier(
            store,
            d24_requirement_db,
            fixture,
            expected_revision=expected_revision,
            disposition=M5ExecutionEvidenceDisposition.RETURNED,
            timing=_attempt_timing(),
        )

    semantic_before = _snapshot_tables(
        d24_requirement_db.connection, _LATE_SEMANTIC_TABLES
    )
    _assert_expired_successor_rejections(
        d24_requirement_db,
        call=old_completion,
        stale_revision=6,
        current_revision=7,
    )
    assert (
        _snapshot_tables(d24_requirement_db.connection, _LATE_SEMANTIC_TABLES)
        == semantic_before
    )
    assert _late_requirement_row_counts(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        attempt_id=fixture.lease.attempt.attempt_id,
    ) == (0, 0, 0, 0, 0)


def test_retryable_verifier_archives_only_after_fixture_terminal_resolution(
    d24_requirement_db: Any,
) -> None:
    """C4 case 5 resolves a verifier only in the labelled SQL fixture."""

    store, fixture = _prepare_verifier_attempt(d24_requirement_db)
    epoch_id = d24_requirement_db.epoch_id
    assert fixture.lease.attempt is not None
    assert fixture.lease.lease_expires_at is not None
    _wait_until_expired(
        d24_requirement_db.connection,
        fixture.lease.lease_expires_at,
    )
    takeover = store.acquire_m5_job(epoch_id, 5, fixture.job)
    assert takeover.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert takeover.attempt is not None and takeover.resulting_revision == 6
    failed = store.mark_m5_retryable_failure(
        epoch_id,
        6,
        takeover,
        _sha("c4-retryable-verifier-terminal-resolution"),
        _verifier_attempt_work(M5ExecutionEvidenceDisposition.RETURNED),
        _attempt_timing(),
    )
    assert failed.resulting_revision == 7 and not failed.exact_replay

    def old_completion(expected_revision: int) -> Any:
        return _complete_verifier(
            store,
            d24_requirement_db,
            fixture,
            expected_revision=expected_revision,
            disposition=M5ExecutionEvidenceDisposition.RETURNED,
            timing=_attempt_timing(),
        )

    _assert_expired_successor_rejections(
        d24_requirement_db,
        call=old_completion,
        stale_revision=6,
        current_revision=7,
    )
    assert _late_requirement_row_counts(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        attempt_id=fixture.lease.attempt.attempt_id,
    ) == (0, 0, 0, 0, 0)

    terminal_hash = terminalize_recovered_epoch_for_storage_fixture(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        resolve_retryable_job_id=fixture.job.logical_job_id,
    )
    d24_requirement_db.connection.commit()
    assert d24_requirement_db.connection.execute(
        """
        SELECT base.revision, runtime.runtime_state, runtime.revision,
               runtime.open_work_count, runtime.open_scope_count,
               runtime.blocking_failure_count,
               job.job_state, job.archive_reason, job.completed_revision,
               parent.scope_state, parent.closed_revision,
               (SELECT count(*)
                FROM groundloop_m5_owner_pending_counter
                WHERE epoch_id = runtime.epoch_id),
               (SELECT count(*)
                FROM groundloop_m5_owner_pending_counter
                WHERE epoch_id = runtime.epoch_id
                  AND verifier_job_count = 0
                  AND blocking_failure_count = 1
                  AND updated_revision = 8),
               (SELECT count(*)
                FROM groundloop_m5_answer_pending_counter
                WHERE epoch_id = runtime.epoch_id),
               (SELECT count(*)
                FROM groundloop_m5_answer_pending_counter
                WHERE epoch_id = runtime.epoch_id
                  AND verifier_job_count = 0
                  AND blocking_failure_count = 1
                  AND updated_revision = 8),
               (SELECT count(*)
                FROM groundloop_m5_runtime_work_contribution
                WHERE epoch_id = runtime.epoch_id
                  AND contribution_kind = 'terminal_job_failure'
                  AND source_id = job.logical_job_id
                  AND applied_revision = 8)
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
        JOIN groundloop_m5_discovery_scope AS parent
          ON parent.epoch_id = job.epoch_id
         AND parent.root_job_id = job.parent_job_id
        WHERE runtime.epoch_id = %s AND job.logical_job_id = %s
        """,
        (epoch_id, fixture.job.logical_job_id),
    ).fetchone() == (
        8,
        "failed",
        8,
        0,
        0,
        1,
        "terminal_failed",
        "retry_exhausted",
        8,
        "closed_active",
        4,
        1,
        1,
        1,
        1,
        1,
    )

    terminal_before = _snapshot_tables(
        d24_requirement_db.connection, _TERMINAL_STATE_TABLES
    )
    semantic_before = _snapshot_tables(
        d24_requirement_db.connection, _LATE_SEMANTIC_TABLES
    )
    receipt = old_completion(8)
    assert receipt.disposition is M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
    assert receipt.resulting_revision == 8 and not receipt.exact_replay
    assert receipt.transition_anchor is None
    assert receipt.current_terminal_logical_result_hash == terminal_hash
    assert (
        _snapshot_tables(d24_requirement_db.connection, _TERMINAL_STATE_TABLES)
        == terminal_before
    )
    assert (
        _snapshot_tables(d24_requirement_db.connection, _LATE_SEMANTIC_TABLES)
        == semantic_before
    )
    assert _verifier_semantic_closure_counts(d24_requirement_db, fixture) == (
        0,
        0,
        0,
        0,
    )
    assert _late_requirement_row_counts(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        attempt_id=fixture.lease.attempt.attempt_id,
    ) == (1, 1, 1, 1, 1)
    assert d24_requirement_db.connection.execute(
        """
        SELECT artifact.job_state_at_receipt, artifact.job_state_after,
               artifact.disposition, artifact.archive_reason,
               artifact.cancelled_by_event_id,
               artifact.cancelled_by_epoch_id,
               artifact.cancellation_reason,
               expired.received_after_terminal,
               (SELECT count(*)
                FROM groundloop_m5_runtime_work_contribution
                WHERE epoch_id = %s AND source_id = %s
                  AND contribution_kind IN (
                      'm5_attempt_execution', 'preterminal_late_return')),
               (SELECT count(*)
                FROM groundloop_m5_runtime_timing_contribution
                WHERE epoch_id = %s AND subgraph = 'requirement'
                  AND attempt_id = %s)
        FROM groundloop_m5_attempt_result_artifact AS artifact
        JOIN groundloop_m5_expired_attempt_return AS expired
          ON expired.epoch_id = artifact.job_epoch_id
         AND expired.attempt_id = artifact.attempt_id
         AND expired.subgraph = 'requirement'
        WHERE artifact.attempt_id = %s
        """,
        (
            epoch_id,
            fixture.lease.attempt.attempt_id,
            epoch_id,
            fixture.lease.attempt.attempt_id,
            fixture.lease.attempt.attempt_id,
        ),
    ).fetchone() == (
        "terminal_failed",
        "terminal_failed",
        "terminal_audit_only",
        "attempt_expired",
        None,
        None,
        None,
        True,
        0,
        0,
    )

    before_replay = _full_snapshot(d24_requirement_db.connection)
    with d24_requirement_db.reconnect() as reconnected:
        replay = _complete_verifier(
            PostgresM5RuntimeStore(reconnected),
            d24_requirement_db,
            fixture,
            expected_revision=8,
            disposition=M5ExecutionEvidenceDisposition.RETURNED,
            timing=_attempt_timing(),
        )
    assert replay.exact_replay and replay.transition_anchor is None
    assert replay.disposition is M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
    assert replay.current_terminal_logical_result_hash == terminal_hash
    assert _full_snapshot(d24_requirement_db.connection) == before_replay


def test_cancelled_current_verifier_is_preterminal_audit_without_semantics(
    d24_requirement_db: Any,
) -> None:
    store, fixture = _prepare_verifier_attempt(d24_requirement_db)
    epoch_id = d24_requirement_db.epoch_id
    assert fixture.lease.attempt is not None
    cancelled_revision = _cancel_requirement_job_for_setup(
        d24_requirement_db,
        job=fixture.job,
        reason=M5TerminalReason.SCOPE_RETIRED,
    )
    assert cancelled_revision == 6

    receipt = _complete_verifier(
        store,
        d24_requirement_db,
        fixture,
        expected_revision=cancelled_revision,
        disposition=M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
        timing=None,
    )
    assert (
        receipt.disposition is M5RequirementReturnDisposition.TERMINAL_AUDIT_PRETERMINAL
    )
    assert receipt.resulting_revision == cancelled_revision
    assert not receipt.exact_replay and receipt.transition_anchor is not None
    assert (
        receipt.transition_anchor.contribution_kind
        is M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN
    )
    assert _verifier_semantic_closure_counts(d24_requirement_db, fixture) == (
        0,
        0,
        0,
        0,
    )
    row = d24_requirement_db.connection.execute(
        """
        SELECT artifact.disposition, artifact.archive_reason,
               artifact.job_state_at_receipt, artifact.job_state_after,
               artifact.cancelled_by_event_id,
               artifact.cancelled_by_epoch_id, artifact.cancellation_reason,
               evidence.disposition,
               (SELECT count(*)
                FROM groundloop_m5_expired_attempt_return
                WHERE epoch_id = %s AND subgraph = 'requirement'
                  AND attempt_id = %s),
               execution.applied_revision, late.applied_revision,
               timing.pending_contribution_kind,
               timing.pending_source_id, timing.pending_anchor_revision,
               timing.updated_revision
        FROM groundloop_m5_attempt_result_artifact AS artifact
        JOIN groundloop_m5_attempt_execution_evidence AS evidence
          ON evidence.epoch_id = artifact.job_epoch_id
         AND evidence.attempt_id = artifact.attempt_id
         AND evidence.subgraph = 'requirement'
        JOIN groundloop_m5_runtime_work_contribution AS execution
          ON execution.epoch_id = evidence.epoch_id
         AND execution.contribution_kind = 'm5_attempt_execution'
         AND execution.source_id = evidence.attempt_id
        JOIN groundloop_m5_runtime_work_contribution AS late
          ON late.epoch_id = evidence.epoch_id
         AND late.contribution_kind = 'preterminal_late_return'
         AND late.source_id = evidence.attempt_id
        JOIN groundloop_m5_runtime_timing_accumulator AS timing
          ON timing.epoch_id = evidence.epoch_id
        WHERE artifact.attempt_id = %s
        """,
        (
            epoch_id,
            fixture.lease.attempt.attempt_id,
            fixture.lease.attempt.attempt_id,
        ),
    ).fetchone()
    assert row == (
        "terminal_audit_only",
        "job_already_terminal",
        "cancelled",
        "cancelled",
        d24_requirement_db.plan.structural_event_id,
        epoch_id,
        "scope_retired",
        "reused_artifact",
        0,
        cancelled_revision,
        cancelled_revision,
        "preterminal_late_return",
        fixture.lease.attempt.attempt_id,
        cancelled_revision,
        cancelled_revision,
    )

    before_replay = _full_snapshot(d24_requirement_db.connection)
    with d24_requirement_db.reconnect() as reconnected:
        replay = _complete_verifier(
            PostgresM5RuntimeStore(reconnected),
            d24_requirement_db,
            fixture,
            expected_revision=cancelled_revision,
            disposition=M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
            timing=None,
        )
    assert replay.exact_replay and replay.transition_anchor is None
    assert (
        replay.disposition is M5RequirementReturnDisposition.TERMINAL_AUDIT_PRETERMINAL
    )
    assert _full_snapshot(d24_requirement_db.connection) == before_replay


def test_cancelled_expired_verifier_requires_terminal_then_writes_five_audit_rows(
    d24_requirement_db: Any,
) -> None:
    """Exercise C3 with the labelled SQL-only terminal storage fixture."""

    store, fixture = _prepare_verifier_attempt(d24_requirement_db)
    epoch_id = d24_requirement_db.epoch_id
    assert fixture.lease.attempt is not None
    assert fixture.lease.lease_expires_at is not None
    _wait_until_expired(
        d24_requirement_db.connection,
        fixture.lease.lease_expires_at,
    )
    takeover = store.acquire_m5_job(epoch_id, 5, fixture.job)
    assert takeover.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert takeover.attempt is not None and takeover.resulting_revision == 6
    cancelled_revision = _cancel_requirement_job_for_setup(
        d24_requirement_db,
        job=fixture.job,
        reason=M5TerminalReason.EPOCH_FAILED,
    )
    assert cancelled_revision == 7

    for rejected_revision in (6, cancelled_revision):
        before_conflict = _full_snapshot(d24_requirement_db.connection)
        with pytest.raises(
            EventConflictError,
            match="nonrunning successor requires terminal event",
        ):
            _complete_verifier(
                store,
                d24_requirement_db,
                fixture,
                expected_revision=rejected_revision,
                disposition=M5ExecutionEvidenceDisposition.RETURNED,
                timing=_attempt_timing(),
            )
        assert _full_snapshot(d24_requirement_db.connection) == before_conflict
        assert _verifier_semantic_closure_counts(d24_requirement_db, fixture) == (
            0,
            0,
            0,
            0,
        )

    terminal_hash = terminalize_recovered_epoch_for_storage_fixture(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
    )
    d24_requirement_db.connection.commit()
    terminal_before = _snapshot_tables(
        d24_requirement_db.connection,
        _TERMINAL_STATE_TABLES,
    )
    semantic_before = _snapshot_tables(
        d24_requirement_db.connection,
        _LATE_SEMANTIC_TABLES,
    )
    receipt = _complete_verifier(
        store,
        d24_requirement_db,
        fixture,
        expected_revision=cancelled_revision,
        disposition=M5ExecutionEvidenceDisposition.RETURNED,
        timing=_attempt_timing(),
    )
    assert receipt.disposition is M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
    assert receipt.resulting_revision == cancelled_revision + 1
    assert not receipt.exact_replay and receipt.transition_anchor is None
    assert receipt.current_terminal_logical_result_hash == terminal_hash
    assert (
        _snapshot_tables(d24_requirement_db.connection, _TERMINAL_STATE_TABLES)
        == terminal_before
    )
    assert (
        _snapshot_tables(d24_requirement_db.connection, _LATE_SEMANTIC_TABLES)
        == semantic_before
    )
    assert _verifier_semantic_closure_counts(d24_requirement_db, fixture) == (
        0,
        0,
        0,
        0,
    )

    closure = d24_requirement_db.connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_attempt_result_artifact
           WHERE attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_expired_attempt_return
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_attempt_execution_evidence
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_post_terminal_attempt_timing
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_post_terminal_attempt_audit
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_runtime_work_contribution
           WHERE epoch_id = %s AND source_id = %s
             AND contribution_kind IN (
                 'm5_attempt_execution', 'preterminal_late_return')),
          (SELECT count(*) FROM groundloop_m5_runtime_timing_contribution
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s)
        """,
        (
            fixture.lease.attempt.attempt_id,
            epoch_id,
            fixture.lease.attempt.attempt_id,
            epoch_id,
            fixture.lease.attempt.attempt_id,
            epoch_id,
            fixture.lease.attempt.attempt_id,
            epoch_id,
            fixture.lease.attempt.attempt_id,
            epoch_id,
            fixture.lease.attempt.attempt_id,
            epoch_id,
            fixture.lease.attempt.attempt_id,
        ),
    ).fetchone()
    assert closure == (1, 1, 1, 1, 1, 0, 0)

    before_replay = _full_snapshot(d24_requirement_db.connection)
    with d24_requirement_db.reconnect() as reconnected:
        replay = _complete_verifier(
            PostgresM5RuntimeStore(reconnected),
            d24_requirement_db,
            fixture,
            expected_revision=cancelled_revision + 1,
            disposition=M5ExecutionEvidenceDisposition.RETURNED,
            timing=_attempt_timing(),
        )
    assert replay.exact_replay and replay.transition_anchor is None
    assert replay.disposition is M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
    assert replay.current_terminal_logical_result_hash == terminal_hash
    assert _full_snapshot(d24_requirement_db.connection) == before_replay

    work = _verifier_attempt_work(M5ExecutionEvidenceDisposition.RETURNED)
    conflicts = (
        (
            M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
            _verifier_attempt_work(M5ExecutionEvidenceDisposition.REUSED_ARTIFACT),
            _attempt_timing(),
        ),
        (
            M5ExecutionEvidenceDisposition.RETURNED,
            replace(work, bytes_hashed=53, work_digest=""),
            _attempt_timing(),
        ),
        (M5ExecutionEvidenceDisposition.RETURNED, work, None),
    )
    for disposition, changed_work, changed_timing in conflicts:
        with pytest.raises(EventConflictError, match="immutable evidence"):
            store.complete_m5_verifier_atomically(
                epoch_id,
                cancelled_revision + 1,
                fixture.lease,
                fixture.job,
                fixture.pair_input,
                fixture.verifier_artifact,
                fixture.attempt_output,
                disposition,
                changed_work,
                changed_timing,
            )
        assert _full_snapshot(d24_requirement_db.connection) == before_replay

    changed_input, changed_artifact, changed_output = (
        _verifier_fixture_with_changed_immutable_core(d24_requirement_db, fixture)
    )
    with pytest.raises(EventConflictError, match="immutable core rows"):
        store.complete_m5_verifier_atomically(
            epoch_id,
            cancelled_revision + 1,
            fixture.lease,
            fixture.job,
            changed_input,
            changed_artifact,
            changed_output,
            M5ExecutionEvidenceDisposition.RETURNED,
            work,
            _attempt_timing(),
        )
    assert _full_snapshot(d24_requirement_db.connection) == before_replay
