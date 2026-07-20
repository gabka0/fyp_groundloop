from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any

import pytest
from physical_gate_harness import (
    CANDIDATE_POLICY_ID,
    CHILD_COUNT,
    FRONTIER_EXECUTION_HASH,
    IMPACT_EXECUTION_HASH,
    INSERTED_CHUNK_ID,
    REGISTRY_SNAPSHOT_ID,
    VERIFIER_EXECUTION_HASH,
    ExecutionAccounting,
    SupportingVerifier,
    ThreePairAdmission,
    candidate_policy,
    claim_ids,
    digest,
    inserted_document,
    isolated_connection,
    seed_published_base,
    seed_unrelated_runtime_jobs,
)

import groundloop.m4.pipeline as pipeline_module
from groundloop.m4.application import (
    ApplicationExecutionPolicy,
    DynamicEventPlan,
    EventRunState,
    M4Application,
)
from groundloop.m4.contracts import CorpusUpdateIdentity, UpdateKind
from groundloop.m4.pipeline import (
    M4ExecutionMode,
    PersistingAdmissionPort,
    PostgresM4ApplicationPorts,
    StructuralPayload,
)


@dataclass(frozen=True, slots=True)
class ScaleResult:
    registry_size: int
    unrelated_job_count: int
    kernel_sql: tuple[str, ...]
    accounting: ExecutionAccounting
    audit_sql_count: int
    event_job_count: int
    event_child_count: int


def _forbidden_global_path(*_args: object, **_kwargs: object) -> Any:
    raise AssertionError(
        "measured event used deepcopy or reconstructed a full runtime surface"
    )


def _run_scale(
    database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    registry_size: int,
    unrelated_job_count: int,
) -> ScaleResult:
    with isolated_connection(
        database_url, label=f"{registry_size}_{unrelated_job_count}"
    ) as connection:
        base_epoch = seed_published_base(
            connection, registry_size=registry_size
        )
        inserted = inserted_document()
        event = DynamicEventPlan(
            CorpusUpdateIdentity(
                "m4-physical-gate-insert",
                digest("m4-physical-gate-insert-payload"),
                UpdateKind.INSERT,
                base_epoch,
                CANDIDATE_POLICY_ID,
            ),
            (INSERTED_CHUNK_ID,),
            (),
            (),
            REGISTRY_SNAPSHOT_ID,
        )
        ports = PostgresM4ApplicationPorts(
            connection,
            structural_payloads={
                event.update.event_id: StructuralPayload(inserted=inserted)
            },
            execution_mode=M4ExecutionMode.MEASURED,
        )
        registry = claim_ids(registry_size)
        ports.runtime_store.register_claim_registry_snapshot(
            REGISTRY_SNAPSHOT_ID, registry
        )
        ports.runtime_store.register_candidate_policy(
            candidate_policy(registry_size)
        )
        seed_unrelated_runtime_jobs(
            connection,
            base_epoch=base_epoch,
            job_count=unrelated_job_count,
        )
        setup_book = ports.runtime_store.read_book()
        setup_predecessor = next(
            epoch for epoch in setup_book.epochs if epoch.epoch_id == base_epoch
        )
        assert len(setup_predecessor.jobs) == unrelated_job_count
        admission = ThreePairAdmission(registry[:CHILD_COUNT])
        verifier = SupportingVerifier()
        application = M4Application(
            structural=ports,
            runtime=ports,
            admission=PersistingAdmissionPort(connection, admission),
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

        connection.reset_statement_trace()
        with monkeypatch.context() as measured_guard:
            measured_guard.setattr(copy, "deepcopy", _forbidden_global_path)
            measured_guard.setattr(
                pipeline_module, "deepcopy", _forbidden_global_path
            )
            measured_guard.setattr(
                ports.runtime_store, "read_epoch", _forbidden_global_path
            )
            measured_guard.setattr(
                ports.runtime_store, "read_book", _forbidden_global_path
            )
            result = application.run_event(event)
        kernel_sql = tuple(connection.statement_fingerprints)

        assert not any(
            "groundloop_m4_claim_registry_member" in statement
            for statement in kernel_sql
        )
        assert not any(
            "FROM groundloop_semantic_job WHERE epoch_id = %s ORDER BY job_id"
            in statement
            for statement in kernel_sql
        )
        assert not any(
            "JOIN groundloop_m4_update u USING (epoch_id) ORDER BY e.epoch_id"
            in statement
            for statement in kernel_sql
        )

        assert result.state is EventRunState.SEALED
        assert result.discovery_call_count == 1
        assert result.verifier_call_count == CHILD_COUNT
        assert result.effective_observation_count == CHILD_COUNT
        assert admission.calls == 1
        assert verifier.calls == CHILD_COUNT
        event_counts = connection.execute(
            """
            SELECT count(*), count(*) FILTER (WHERE parent_job_id IS NOT NULL)
            FROM groundloop_semantic_job WHERE epoch_id = %s
            """,
            (result.epoch_id,),
        ).fetchone()
        assert event_counts is not None
        event_job_count, event_child_count = map(int, event_counts)
        assert (event_job_count, event_child_count) == (1 + CHILD_COUNT, CHILD_COUNT)

        accounting = ExecutionAccounting.read(connection, result.epoch_id)
        assert accounting.execution_mode == "measured"
        assert accounting.inline_grounding_oracle_calls == 0
        assert accounting.working_claim_rows_written <= 2 * CHILD_COUNT
        assert accounting.working_answer_rows_written <= 2 * CHILD_COUNT
        assert accounting.evaluation_default_rows_written <= 2 * event_job_count + 2
        assert accounting.evaluation_override_rows_written <= 4 * event_job_count
        assert accounting.active_chunk_rows_examined <= 2 * event_job_count
        assert accounting.published_claim_versions_written == CHILD_COUNT
        assert accounting.published_answer_versions_written == CHILD_COUNT

        accounting_before_audit = ExecutionAccounting.read(
            connection, result.epoch_id
        )
        audit_start = len(connection.statement_fingerprints)
        ports.audit_grounding_exactness(result.epoch_id)
        ports.check_evaluation(result.epoch_id)
        runtime_book = ports.runtime_store.read_book()
        audit_sql_count = len(connection.statement_fingerprints) - audit_start
        accounting_after_audit = ExecutionAccounting.read(
            connection, result.epoch_id
        )
        assert accounting_after_audit == accounting_before_audit
        assert audit_sql_count > 0
        predecessor = next(
            epoch for epoch in runtime_book.epochs if epoch.epoch_id == base_epoch
        )
        assert len(predecessor.jobs) == unrelated_job_count

        return ScaleResult(
            registry_size=registry_size,
            unrelated_job_count=unrelated_job_count,
            kernel_sql=kernel_sql,
            accounting=accounting,
            audit_sql_count=audit_sql_count,
            event_job_count=event_job_count,
            event_child_count=event_child_count,
        )


def test_measured_multi_child_kernel_is_independent_of_unrelated_scale(
    physical_gate_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    small = _run_scale(
        physical_gate_database_url,
        monkeypatch,
        registry_size=8,
        unrelated_job_count=2,
    )
    large = _run_scale(
        physical_gate_database_url,
        monkeypatch,
        registry_size=256,
        unrelated_job_count=256,
    )

    assert small.event_job_count == large.event_job_count == 1 + CHILD_COUNT
    assert small.event_child_count == large.event_child_count == CHILD_COUNT
    assert small.accounting == large.accounting
    assert small.kernel_sql == large.kernel_sql
    assert len(small.kernel_sql) <= 64 + 48 * small.event_job_count


def test_gate_uses_compact_registry_identity_not_event_member_payload() -> None:
    event = DynamicEventPlan(
        CorpusUpdateIdentity(
            "m4-physical-gate-contract",
            digest("m4-physical-gate-contract-payload"),
            UpdateKind.INSERT,
            1,
            CANDIDATE_POLICY_ID,
        ),
        (INSERTED_CHUNK_ID,),
        (),
        (),
        REGISTRY_SNAPSHOT_ID,
    )

    assert event.registered_claim_ids == ()
    assert len(claim_ids(256)) == 256
