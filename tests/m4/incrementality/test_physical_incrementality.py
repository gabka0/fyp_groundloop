from __future__ import annotations

from typing import Any

import pytest
from harness import (
    CANDIDATE_POLICY_ID,
    DECOY_CHUNK_COUNT,
    FRONTIER_EXECUTION_HASH,
    IMPACT_EXECUTION_HASH,
    REGISTRY_SIZE,
    REGISTRY_SNAPSHOT_ID,
    TARGET_ANSWER_ID,
    TARGET_CLAIM_ID,
    VERIFIER_EXECUTION_HASH,
    CompactPendingAdmission,
    ExecutionAccounting,
    SupportingVerifier,
    candidate_policy,
    claim_ids,
    digest,
    inserted_support_document,
    published_interval,
    seed_large_unsupported_registry,
)
from psycopg import Connection

from groundloop.m4.application import (
    ApplicationExecutionPolicy,
    DynamicEventPlan,
    EventRunState,
    M4Application,
)
from groundloop.m4.contracts import CorpusUpdateIdentity, UpdateKind
from groundloop.m4.pipeline import (
    PersistingAdmissionPort,
    PostgresM4ApplicationPorts,
    StructuralPayload,
)


def _measured_mode() -> Any:
    try:
        from groundloop.m4.pipeline import M4ExecutionMode
    except ImportError:
        pytest.xfail("coordinator has not exposed M4ExecutionMode yet")
    return M4ExecutionMode.MEASURED


def test_measured_mode_contract_is_exposed() -> None:
    mode = _measured_mode()
    assert mode.value == "measured"
    assert hasattr(PostgresM4ApplicationPorts, "audit_grounding_exactness")


def test_measured_event_is_physically_incremental_and_separately_auditable(
    incrementality_connection: Connection[Any],
) -> None:
    mode = _measured_mode()
    base_epoch = seed_large_unsupported_registry(incrementality_connection)
    inserted = inserted_support_document()
    event = DynamicEventPlan(
        CorpusUpdateIdentity(
            "incrementality-insert",
            digest("incrementality-insert-payload"),
            UpdateKind.INSERT,
            base_epoch,
            CANDIDATE_POLICY_ID,
        ),
        ("incrementality-chunk",),
        (),
        claim_ids(),
        REGISTRY_SNAPSHOT_ID,
    )
    admission_delegate = CompactPendingAdmission(incrementality_connection)
    verifier = SupportingVerifier()
    ports = PostgresM4ApplicationPorts(
        incrementality_connection,
        structural_payloads={
            event.update.event_id: StructuralPayload(inserted=inserted)
        },
        execution_mode=mode,
    )
    ports.runtime_store.register_candidate_policy(candidate_policy())
    application = M4Application(
        structural=ports,
        runtime=ports,
        admission=PersistingAdmissionPort(
            incrementality_connection, admission_delegate
        ),
        verifier=verifier,
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_EXECUTION_HASH,
            FRONTIER_EXECUTION_HASH,
            VERIFIER_EXECUTION_HASH,
        ),
    )

    result = application.run_event(event)

    assert result.state is EventRunState.SEALED
    assert result.discovery_call_count == 1
    assert result.verifier_call_count == 1
    assert admission_delegate.checked_pending
    assert verifier.calls == 1

    accounting = ExecutionAccounting.read(
        incrementality_connection, result.epoch_id
    )
    assert accounting.execution_mode == "measured"
    assert accounting.inline_grounding_oracle_calls == 0
    assert accounting.working_claim_rows_written == 1
    assert accounting.working_answer_rows_written == 1
    assert accounting.published_claim_versions_written == 1
    assert accounting.published_answer_versions_written == 1

    # Evaluation accounting is proportional to the one targeted child job,
    # not to the 64-object discovery scope.
    logical_jobs = result.discovery_call_count + result.verifier_call_count
    assert 0 < accounting.evaluation_default_rows_written <= 3 * logical_jobs
    assert 0 <= accounting.evaluation_override_rows_written <= 2 * logical_jobs
    assert accounting.evaluation_default_rows_written < REGISTRY_SIZE
    assert accounting.evaluation_override_rows_written < REGISTRY_SIZE
    assert incrementality_connection.execute(
        """
        SELECT count(*) FROM groundloop_m4_evaluation_default
        WHERE epoch_id = %s
        """,
        (result.epoch_id,),
    ).fetchone() == (1,)
    assert incrementality_connection.execute(
        """
        SELECT count(*) FROM groundloop_object_evaluation
        WHERE epoch_id = %s
        """,
        (result.epoch_id,),
    ).fetchone() == (0,)

    # Point checks may occur at root and verifier completion boundaries, but
    # their work must scale with jobs, not with the active corpus cardinality.
    assert 0 < accounting.active_chunk_rows_examined <= 2 * logical_jobs
    assert accounting.active_chunk_rows_examined < DECOY_CHUNK_COUNT

    assert incrementality_connection.execute(
        """
        SELECT claim_id FROM groundloop_m4_working_claim_state
        WHERE epoch_id = %s ORDER BY claim_id
        """,
        (result.epoch_id,),
    ).fetchall() == [(TARGET_CLAIM_ID,)]
    assert incrementality_connection.execute(
        """
        SELECT answer_version_id FROM groundloop_m4_working_answer_state
        WHERE epoch_id = %s ORDER BY answer_version_id
        """,
        (result.epoch_id,),
    ).fetchall() == [(TARGET_ANSWER_ID,)]

    # The changed objects receive one new interval.  Every untouched object
    # keeps the original open interval; no close-and-copy snapshot rewrite is
    # permitted.
    assert published_interval(
        incrementality_connection,
        "groundloop_published_claim_state",
        "claim_id",
        TARGET_CLAIM_ID,
    ) == ((base_epoch, result.epoch_id), (result.epoch_id, None))
    assert published_interval(
        incrementality_connection,
        "groundloop_published_answer_state",
        "answer_version_id",
        TARGET_ANSWER_ID,
    ) == ((base_epoch, result.epoch_id), (result.epoch_id, None))
    untouched_claim = "claim-063"
    untouched_answer = "answer-063"
    assert published_interval(
        incrementality_connection,
        "groundloop_published_claim_state",
        "claim_id",
        untouched_claim,
    ) == ((base_epoch, None),)
    assert published_interval(
        incrementality_connection,
        "groundloop_published_answer_state",
        "answer_version_id",
        untouched_answer,
    ) == ((base_epoch, None),)
    assert incrementality_connection.execute(
        "SELECT count(*) FROM groundloop_published_claim_state"
    ).fetchone() == (REGISTRY_SIZE + 1,)
    assert incrementality_connection.execute(
        "SELECT count(*) FROM groundloop_published_answer_state"
    ).fetchone() == (REGISTRY_SIZE + 1,)

    # Exactness is a distinct, explicit audit operation.  It is deliberately
    # not charged to measured event execution and it must not mutate any
    # physical-work counter.
    before_audit = ExecutionAccounting.read(
        incrementality_connection, result.epoch_id
    )
    ports.audit_grounding_exactness(result.epoch_id)
    after_audit = ExecutionAccounting.read(
        incrementality_connection, result.epoch_id
    )
    assert after_audit == before_audit
