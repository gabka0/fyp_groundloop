"""D30 falsifiers 2--4, 6--8 and 11 for generic direct-M4 claims."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from groundloop.errors import EventConflictError
from groundloop.m4.contracts import (
    ChildClosure,
    JobCompletion,
    JobKind,
    JobState,
    LogicalJobSpec,
    stable_m4_digest,
)
from groundloop.m5.runtime import postgres_withdrawal


class _CoordinateResult:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self.row = row

    def fetchone(self) -> tuple[object, ...] | None:
        return self.row


class _RowsResult:
    def __init__(self, rows: tuple[tuple[object, ...], ...]) -> None:
        self.rows = rows

    def fetchall(self) -> tuple[tuple[object, ...], ...]:
        return self.rows


class _CoordinateCursor:
    def __init__(self, rows: dict[str, tuple[object, ...] | None]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, object]] = []

    def execute(
        self, statement: object, parameters: tuple[object, ...]
    ) -> _CoordinateResult:
        sql = " ".join(str(statement).split())
        self.calls.append((sql, parameters[0]))
        column = next(
            name
            for name in ("observation_id", "job_id", "admitted_pair_id")
            if f"WHERE {name} = %s" in sql
        )
        return _CoordinateResult(self.rows[column])


_DEFAULT_CITED_ADMISSION = object()


class _DynamicGatherCursor:
    def __init__(
        self,
        claim: Any,
        *,
        cited_admission_row: object = _DEFAULT_CITED_ADMISSION,
        unrelated_hit_rows: tuple[tuple[object, ...], ...] = (),
        raw_root_hit_memberships: tuple[tuple[str, tuple[object, ...]], ...] = (),
        other_root_admissions: tuple[tuple[str, tuple[object, ...]], ...] = (),
        raw_root_admission_memberships: tuple[tuple[str, str], ...] = (),
    ) -> None:
        self.claim = claim
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.unrelated_hit_rows = unrelated_hit_rows
        self.raw_root_hit_memberships = raw_root_hit_memberships
        self.other_root_admissions = other_root_admissions
        self.raw_root_admission_memberships = raw_root_admission_memberships
        if cited_admission_row is _DEFAULT_CITED_ADMISSION:
            selected_admission: tuple[object, ...] | None = claim.admitted_pair_row
        elif cited_admission_row is None or isinstance(cited_admission_row, tuple):
            selected_admission = cited_admission_row
        else:
            raise TypeError("cited admission must be a tuple or None")
        self.admission_rows = {
            str(row[0]): row for _root_job_id, row in other_root_admissions
        }
        if selected_admission is not None:
            self.admission_rows[str(selected_admission[0])] = selected_admission

    def execute(
        self,
        statement: object,
        parameters: tuple[object, ...] = (),
    ) -> _CoordinateResult:
        sql = " ".join(str(statement).split())
        self.calls.append((sql, tuple(parameters)))
        if "FROM groundloop_m5_activation AS activation" in sql:
            return _CoordinateResult(("activation",))
        if "FROM groundloop_working_observation_delta" in sql:
            return _CoordinateResult(self.claim.delta_row)
        if "FROM groundloop_m4_update" in sql:
            return _CoordinateResult((None,))
        if "FROM groundloop_m4_verification_execution" in sql:
            return _CoordinateResult(None)
        if "FROM groundloop_admitted_pair" in sql:
            if "WHERE admitted_pair_id = %s" not in sql or len(parameters) != 1:
                raise AssertionError(f"non-point D30 admission query: {sql!r}")
            return _CoordinateResult(self.admission_rows.get(str(parameters[0])))
        raise AssertionError(f"unexpected D30 gather query: {sql!r} {parameters!r}")


class _F8Tier10Cursor(_DynamicGatherCursor):
    def __init__(self, claim: Any, owner: Any, **kwargs: Any) -> None:
        super().__init__(claim, **kwargs)
        self.owner = owner

    def execute(
        self,
        statement: object,
        parameters: tuple[object, ...] = (),
    ) -> _CoordinateResult | _RowsResult:
        sql = " ".join(str(statement).split())
        if "FROM groundloop_m5_direct_terminal_projection" in sql:
            self.calls.append((sql, tuple(parameters)))
            if len(parameters) != 2:
                raise AssertionError(f"non-composite projection lookup: {parameters!r}")
            coordinate = (int(str(parameters[0])), str(parameters[1]))
            projections = {
                (self.owner.epoch_id, job_id): row
                for job_id, row in self.owner.projections
            }
            return _CoordinateResult(projections.get(coordinate))
        if "FROM groundloop_semantic_job_dependency" in sql:
            self.calls.append((sql, tuple(parameters)))
            if "AND parent_job_id = %s" in sql:
                coordinate = (
                    int(str(parameters[0])),
                    str(parameters[1]),
                    str(parameters[2]),
                )
                row = next(
                    (row for row in self.owner.dependencies if row == coordinate),
                    None,
                )
                return _CoordinateResult(row)
            if parameters != (self.owner.epoch_id,):
                raise AssertionError(f"wrong dependency epoch: {parameters!r}")
            return _RowsResult(self.owner.dependencies)
        if "FROM groundloop_m4_discovery_result" in sql:
            self.calls.append((sql, tuple(parameters)))
            if len(parameters) != 1:
                raise AssertionError(f"non-point result lookup: {parameters!r}")
            results = dict(self.owner.discovery_results)
            return _CoordinateResult(results.get(str(parameters[0])))
        return super().execute(statement, parameters)


class _AfterProjectionRelock(RuntimeError):
    pass


class _ProjectionRelockCursor:
    def __init__(
        self,
        projection_rows: dict[tuple[int, str], tuple[object, ...]],
    ) -> None:
        self.projection_rows = projection_rows
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(
        self,
        statement: object,
        parameters: tuple[object, ...] = (),
    ) -> _CoordinateResult:
        sql = " ".join(str(statement).split())
        if "FROM groundloop_m5_direct_terminal_projection" not in sql:
            raise _AfterProjectionRelock
        self.calls.append((sql, tuple(parameters)))
        if len(parameters) != 2:
            raise AssertionError(f"non-composite projection lookup: {parameters!r}")
        coordinate = (int(str(parameters[0])), str(parameters[1]))
        return _CoordinateResult(self.projection_rows.get(coordinate))


def _dynamic_sources(function_source: Callable[[str], str]) -> str:
    return "\n".join(
        function_source(name)
        for name in (
            "_gather_d30_claim_authority",
            "_d30_observation_row",
            "_d30_owner_locator",
            "_d30_execution_coordinates",
            "_validate_d30_owner_topology_from_held_rows",
            "_lock_d30_dynamic_owner_topology",
            "_validate_d30_dynamic_claim",
        )
    )


def _gather_dynamic_claim_authority(
    monkeypatch: pytest.MonkeyPatch,
    *,
    claim: Any,
    owner: Any,
    cursor: _DynamicGatherCursor,
) -> Any:
    monkeypatch.setattr(
        postgres_withdrawal,
        "_d30_observation_row",
        lambda _cursor, _observation_id: claim.observation_row,
    )
    monkeypatch.setattr(
        postgres_withdrawal,
        "_d30_owner_locator",
        lambda _cursor, epoch_id: (
            owner
            if epoch_id == owner.epoch_id
            else pytest.fail("gather requested the wrong source epoch")
        ),
    )
    monkeypatch.setattr(
        postgres_withdrawal,
        "_probe_d30_predecessor_currency",
        lambda *_args, **_kwargs: None,
    )
    return postgres_withdrawal._gather_d30_claim_authority(
        cursor,  # type: ignore[arg-type]
        (claim.currency,),
    )


def _cell(row: tuple[object, ...], index: int, value: object) -> tuple[object, ...]:
    values = list(row)
    values[index] = value
    return tuple(values)


def _assert_f8_exact_admission_trace(
    cursor: _DynamicGatherCursor,
    claim: Any,
    *,
    locked: bool,
) -> None:
    admission_calls = tuple(
        (sql, parameters)
        for sql, parameters in cursor.calls
        if "FROM groundloop_admitted_pair" in sql
    )
    assert len(admission_calls) == (2 if locked else 1)
    for index, (admission_sql, admission_parameters) in enumerate(admission_calls):
        assert "WHERE admitted_pair_id = %s" in admission_sql
        assert admission_parameters == (claim.admitted_pair_id,)
        assert "root_job_id" not in admission_sql
        assert "ORDER BY" not in admission_sql
        assert ("FOR UPDATE" in admission_sql) is (index == 1)
    for sql, _parameters in cursor.calls:
        assert "groundloop_impact_channel_hit" not in sql
        assert "reason_hit" not in sql.lower()
        if "FROM groundloop_admitted_pair" in sql:
            assert "WHERE admitted_pair_id = %s" in sql


def _valid_dynamic_authority(
    *,
    execution: bool = False,
    reused_from: str | None = None,
    parent_kind: JobKind = JobKind.IMPACT_DISCOVERY,
    task_type: str = "arbitrary-task",
    parent_execution_spec_hash: str | None = None,
    parent_target_claim_id: str = "claim-a",
    parent_target_chunk_version_id: str = "chunk-a",
) -> tuple[Any, Any]:
    epoch_id = 7
    epoch_revision = 20
    completion_revision = 11
    event_id = "event-owner"
    policy_id = "policy-a"
    epoch_payload = "0" * 64
    parent_execution = (
        parent_execution_spec_hash
        if parent_execution_spec_hash is not None
        else "1" * 64
        if parent_kind is JobKind.IMPACT_DISCOVERY
        else "f" * 64
    )
    child_execution = "2" * 64
    parent_result_hash = "3" * 64
    child_result_hash = "4" * 64
    parent_id = LogicalJobSpec.derive_job_id(
        event_id=event_id,
        kind=parent_kind,
        candidate_policy_id=policy_id,
        execution_spec_hash=parent_execution,
        claim_id=(
            parent_target_claim_id if parent_kind is JobKind.FRONTIER_RETRIEVE else ""
        ),
        chunk_version_id=(
            parent_target_chunk_version_id
            if parent_kind is JobKind.IMPACT_DISCOVERY
            else ""
        ),
    )
    child_id = LogicalJobSpec.derive_job_id(
        event_id=event_id,
        kind=JobKind.VERIFY_PAIR,
        candidate_policy_id=policy_id,
        execution_spec_hash=child_execution,
        parent_job_id=parent_id,
        claim_id="claim-a",
        chunk_version_id="chunk-a",
    )
    parent_payload = stable_m4_digest(
        "m4-application-job-payload-v1",
        epoch_payload,
        parent_kind.value,
        "",
        (parent_target_claim_id if parent_kind is JobKind.FRONTIER_RETRIEVE else ""),
        (
            parent_target_chunk_version_id
            if parent_kind is JobKind.IMPACT_DISCOVERY
            else ""
        ),
    )
    child_payload = stable_m4_digest(
        "m4-application-job-payload-v1",
        epoch_payload,
        JobKind.VERIFY_PAIR.value,
        parent_id,
        "claim-a",
        "chunk-a",
    )
    closure = ChildClosure.build(
        parent_job_id=parent_id,
        result_artifact_hash=parent_result_hash,
        child_job_ids=(child_id,),
    )
    parent_completion = JobCompletion.build(
        job_id=parent_id,
        payload_hash=parent_payload,
        execution_spec_hash=parent_execution,
        result_artifact_id="opaque-parent-result",
        result_artifact_hash=parent_result_hash,
        terminal_state=JobState.COMPLETED_ACTIVE,
        child_closure=closure,
    )
    child_completion = JobCompletion.build(
        job_id=child_id,
        payload_hash=child_payload,
        execution_spec_hash=child_execution,
        result_artifact_id="opaque-child-result",
        result_artifact_hash=child_result_hash,
        terminal_state=JobState.COMPLETED_ACTIVE,
    )
    parent = (
        parent_id,
        epoch_id,
        None,
        parent_kind.value,
        policy_id,
        parent_payload,
        parent_execution,
        (parent_target_claim_id if parent_kind is JobKind.FRONTIER_RETRIEVE else None),
        (
            parent_target_chunk_version_id
            if parent_kind is JobKind.IMPACT_DISCOVERY
            else None
        ),
        True,
        JobState.COMPLETED_ACTIVE.value,
        True,
        closure.child_set_hash,
        parent_completion.completion_digest,
        "opaque-parent-result",
        parent_result_hash,
        1,
        6,
        object(),
        object(),
    )
    child = (
        child_id,
        epoch_id,
        parent_id,
        JobKind.VERIFY_PAIR.value,
        policy_id,
        child_payload,
        child_execution,
        "claim-a",
        "chunk-a",
        False,
        JobState.COMPLETED_ACTIVE.value,
        False,
        None,
        child_completion.completion_digest,
        "opaque-child-result",
        child_result_hash,
        2,
        completion_revision,
        object(),
        object(),
    )
    admitted_pair_id = stable_m4_digest(
        "m4-admitted-pair-v1",
        str(epoch_id),
        "claim-a",
        "chunk-a",
        policy_id,
    )
    observation = (
        "observation-a",
        "claim",
        "claim-a",
        "chunk-a",
        task_type,
        0.8,
        0.1,
        0.1,
        "model-id",
        "model-revision",
        "prompt-version",
        "5" * 64,
        epoch_id,
        child_result_hash,
        True,
    )
    execution_row = (
        "observation-a",
        child_id,
        admitted_pair_id,
        "model-artifact",
        "prompt-artifact",
        child_execution,
        "6" * 64,
        "calibration-v1",
        "7" * 64,
        1.0,
        (2.0, 0.0, -1.0),
        child_result_hash,
        reused_from,
    )
    executions = (
        (execution_row, execution_row, execution_row)
        if execution
        else (None, None, None)
    )
    currency = postgres_withdrawal._D30CurrencyRow(
        "claim",
        "claim-a",
        "chunk-a",
        task_type,
        "observation-a",
        epoch_id,
    )
    claim = postgres_withdrawal._D30DynamicClaimLocator(
        currency=currency,
        observation_row=observation,
        delta_row=(
            epoch_id,
            "claim",
            "claim-a",
            "chunk-a",
            task_type,
            None,
            "observation-a",
            completion_revision,
        ),
        predecessor_epoch_id=None,
        predecessor_candidate=None,
        owner_epoch_id=epoch_id,
        child_job_id=child_id,
        parent_job_id=parent_id,
        admitted_pair_id=admitted_pair_id,
        admitted_pair_row=(
            admitted_pair_id,
            epoch_id,
            "claim-a",
            "chunk-a",
            policy_id,
            1,
            ("impact",),
            False,
        ),
        parent_result_row=(
            parent_id,
            epoch_id,
            "opaque-parent-result",
            parent_result_hash,
            True,
            1,
            1,
            "8" * 64,
            "9" * 64,
        ),
        execution_rows=executions,
        model_row=("model-artifact", "verification") if execution else None,
        prompt_row=("prompt-artifact", "verification") if execution else None,
    )
    owner = postgres_withdrawal._D30OwnerLocator(
        epoch_id=epoch_id,
        epoch_row=(
            epoch_id,
            event_id,
            epoch_payload,
            epoch_revision,
            "committed",
            "sealed",
            "complete",
            "strict",
            object(),
        ),
        update_row=(epoch_id, "replace", policy_id, None, "snapshot", {}),
        runtime_row=None,
        jobs=tuple(sorted((parent, child), key=lambda row: str(row[0]).encode())),
        dependencies=((epoch_id, parent_id, child_id),),
        scopes=tuple(
            sorted(
                (
                    (
                        parent_id,
                        (
                            parent_id,
                            epoch_id,
                            "snapshot",
                            "all_registered_claims",
                            None,
                            parent[17],
                        )
                        if parent_kind is JobKind.IMPACT_DISCOVERY
                        else None,
                    ),
                    (child_id, None),
                ),
                key=lambda item: item[0].encode(),
            )
        ),
        attempts=tuple(
            sorted(
                (
                    (
                        parent_id,
                        (
                            (
                                "attempt-parent",
                                parent_id,
                                parent_execution,
                                1,
                                "a" * 64,
                                "completed",
                                object(),
                                object(),
                                object(),
                            ),
                        ),
                    ),
                    (
                        child_id,
                        (
                            (
                                "attempt-child",
                                child_id,
                                child_execution,
                                1,
                                "b" * 64,
                                "completed",
                                object(),
                                object(),
                                object(),
                            ),
                        ),
                    ),
                ),
                key=lambda item: item[0].encode(),
            )
        ),
        discovery_results=tuple(
            sorted(
                ((parent_id, claim.parent_result_row), (child_id, None)),
                key=lambda item: item[0].encode(),
            )
        ),
        projections=tuple(
            sorted(
                ((parent_id, None), (child_id, None)),
                key=lambda item: item[0].encode(),
            )
        ),
    )
    return claim, owner


def _empty_f8_noise() -> SimpleNamespace:
    # Membership witnesses are test-retained context kept outside the exact
    # persisted hit/admission row shapes; they do not add a synthetic root column.
    return SimpleNamespace(
        unrelated_hit_rows=(),
        raw_root_hit_memberships=(),
        other_root_admissions=(),
        raw_root_admission_memberships=(),
    )


def _expanded_f8_authority(noise_kind: str) -> tuple[Any, Any, SimpleNamespace]:
    if noise_kind not in {
        "unrelated_hit",
        "other_root_admission",
        "impact_frontier_overlap",
    }:
        raise AssertionError(f"unsupported F8 noise kind: {noise_kind}")
    claim, owner = _valid_dynamic_authority()
    epoch_id = owner.epoch_id
    event_id = str(owner.epoch_row[1])
    epoch_payload = str(owner.epoch_row[2])
    policy_id = str(owner.update_row[2])
    frontier_target_claim = (
        "claim-a" if noise_kind == "impact_frontier_overlap" else "claim-b"
    )
    frontier_execution = stable_m4_digest("d30-f8-frontier-execution-v1", noise_kind)
    frontier_id = LogicalJobSpec.derive_job_id(
        event_id=event_id,
        kind=JobKind.FRONTIER_RETRIEVE,
        candidate_policy_id=policy_id,
        execution_spec_hash=frontier_execution,
        claim_id=frontier_target_claim,
    )
    frontier_payload = stable_m4_digest(
        "m4-application-job-payload-v1",
        epoch_payload,
        JobKind.FRONTIER_RETRIEVE.value,
        "",
        frontier_target_claim,
        "",
    )
    frontier_result_id = f"f8-frontier-result:{noise_kind}"
    frontier_result_hash = stable_m4_digest("d30-f8-frontier-result-v1", noise_kind)

    other_child: tuple[object, ...] | None = None
    other_admission: tuple[object, ...] | None = None
    child_ids: tuple[str, ...] = ()
    if noise_kind == "other_root_admission":
        other_execution = "2" * 64
        other_child_id = LogicalJobSpec.derive_job_id(
            event_id=event_id,
            kind=JobKind.VERIFY_PAIR,
            candidate_policy_id=policy_id,
            execution_spec_hash=other_execution,
            parent_job_id=frontier_id,
            claim_id="claim-b",
            chunk_version_id="chunk-b",
        )
        other_payload = stable_m4_digest(
            "m4-application-job-payload-v1",
            epoch_payload,
            JobKind.VERIFY_PAIR.value,
            frontier_id,
            "claim-b",
            "chunk-b",
        )
        other_result_id = "f8-other-child-result"
        other_result_hash = stable_m4_digest("d30-f8-other-child-result-v1")
        other_completion = JobCompletion.build(
            job_id=other_child_id,
            payload_hash=other_payload,
            execution_spec_hash=other_execution,
            result_artifact_id=other_result_id,
            result_artifact_hash=other_result_hash,
            terminal_state=JobState.COMPLETED_ACTIVE,
        )
        other_child = (
            other_child_id,
            epoch_id,
            frontier_id,
            JobKind.VERIFY_PAIR.value,
            policy_id,
            other_payload,
            other_execution,
            "claim-b",
            "chunk-b",
            False,
            JobState.COMPLETED_ACTIVE.value,
            False,
            None,
            other_completion.completion_digest,
            other_result_id,
            other_result_hash,
            3,
            13,
            object(),
            object(),
        )
        child_ids = (other_child_id,)
        other_admission_id = stable_m4_digest(
            "m4-admitted-pair-v1",
            str(epoch_id),
            "claim-b",
            "chunk-b",
            policy_id,
        )
        other_admission = (
            other_admission_id,
            epoch_id,
            "claim-b",
            "chunk-b",
            policy_id,
            2,
            ("frontier",),
            False,
        )

    frontier_closure = ChildClosure.build(
        parent_job_id=frontier_id,
        result_artifact_hash=frontier_result_hash,
        child_job_ids=child_ids,
    )
    frontier_completion = JobCompletion.build(
        job_id=frontier_id,
        payload_hash=frontier_payload,
        execution_spec_hash=frontier_execution,
        result_artifact_id=frontier_result_id,
        result_artifact_hash=frontier_result_hash,
        terminal_state=JobState.COMPLETED_ACTIVE,
        child_closure=frontier_closure,
    )
    frontier = (
        frontier_id,
        epoch_id,
        None,
        JobKind.FRONTIER_RETRIEVE.value,
        policy_id,
        frontier_payload,
        frontier_execution,
        frontier_target_claim,
        None,
        True,
        JobState.COMPLETED_ACTIVE.value,
        True,
        frontier_closure.child_set_hash,
        frontier_completion.completion_digest,
        frontier_result_id,
        frontier_result_hash,
        2,
        8,
        object(),
        object(),
    )
    admitted_count = 0 if noise_kind == "unrelated_hit" else 1
    frontier_result = (
        frontier_id,
        epoch_id,
        frontier_result_id,
        frontier_result_hash,
        True,
        1,
        admitted_count,
        stable_m4_digest("d30-f8-frontier-channel-set-v1", noise_kind),
        stable_m4_digest("d30-f8-frontier-admitted-set-v1", noise_kind),
    )
    frontier_attempt = (
        f"attempt-frontier:{noise_kind}",
        frontier_id,
        frontier_execution,
        1,
        stable_m4_digest("d30-f8-frontier-lease-v1", noise_kind),
        "completed",
        object(),
        object(),
        object(),
    )

    jobs = [*owner.jobs, frontier]
    dependencies = [*owner.dependencies]
    scopes = [*owner.scopes, (frontier_id, None)]
    attempts = [*owner.attempts, (frontier_id, (frontier_attempt,))]
    results = [*owner.discovery_results, (frontier_id, frontier_result)]
    projections = [*owner.projections, (frontier_id, None)]
    if other_child is not None:
        other_child_id = str(other_child[0])
        other_attempt = (
            "attempt-other-child",
            other_child_id,
            str(other_child[6]),
            1,
            stable_m4_digest("d30-f8-other-child-lease-v1"),
            "completed",
            object(),
            object(),
            object(),
        )
        jobs.append(other_child)
        dependencies.append((epoch_id, frontier_id, other_child_id))
        scopes.append((other_child_id, None))
        attempts.append((other_child_id, (other_attempt,)))
        results.append((other_child_id, None))
        projections.append((other_child_id, None))
    expanded_owner = replace(
        owner,
        jobs=tuple(sorted(jobs, key=lambda row: str(row[0]).encode())),
        dependencies=tuple(
            sorted(
                dependencies,
                key=lambda row: (
                    int(str(row[0])),
                    str(row[1]).encode(),
                    str(row[2]).encode(),
                ),
            )
        ),
        scopes=tuple(sorted(scopes, key=lambda item: item[0].encode())),
        attempts=tuple(sorted(attempts, key=lambda item: item[0].encode())),
        discovery_results=tuple(sorted(results, key=lambda item: item[0].encode())),
        projections=tuple(sorted(projections, key=lambda item: item[0].encode())),
    )

    unrelated_hit = (
        epoch_id,
        "chunk-b",
        "claim-b",
        policy_id,
        "vector",
        1,
        0.75,
        stable_m4_digest("d30-f8-unrelated-hit-v1"),
    )
    noise = _empty_f8_noise()
    if noise_kind == "unrelated_hit":
        noise.unrelated_hit_rows = (unrelated_hit,)
        noise.raw_root_hit_memberships = ((frontier_id, unrelated_hit),)
    elif noise_kind == "other_root_admission":
        assert other_admission is not None
        noise.other_root_admissions = ((frontier_id, other_admission),)
        noise.raw_root_admission_memberships = ((frontier_id, str(other_admission[0])),)
    else:
        # The same retained raw pair belongs to both real roots, while the
        # authoritative owner contains exactly one deduplicated verifier child.
        noise.raw_root_admission_memberships = (
            (claim.parent_job_id, claim.admitted_pair_id),
            (frontier_id, claim.admitted_pair_id),
        )
    return claim, expanded_owner, noise


def _f8_cursor(
    claim: Any,
    owner: Any,
    noise: SimpleNamespace,
    *,
    cited_admission_row: object = _DEFAULT_CITED_ADMISSION,
) -> _F8Tier10Cursor:
    return _F8Tier10Cursor(
        claim,
        owner,
        cited_admission_row=cited_admission_row,
        unrelated_hit_rows=noise.unrelated_hit_rows,
        raw_root_hit_memberships=noise.raw_root_hit_memberships,
        other_root_admissions=noise.other_root_admissions,
        raw_root_admission_memberships=noise.raw_root_admission_memberships,
    )


def _f8_tier10_locator(claim: Any, owner: Any) -> SimpleNamespace:
    return SimpleNamespace(
        d30_claims=SimpleNamespace(
            owners=(owner,),
            dynamic=(claim,),
            bootstrap=(),
        ),
        direct_job_coordinates=tuple(
            (owner.epoch_id, str(row[0])) for row in owner.jobs
        ),
        direct_dependency_coordinates=owner.dependencies,
        candidate_root_job_ids=(),
        candidate_root_job_coordinates=(),
        candidate_dependency_coordinates=(),
        verifier_job_ids=(),
        verifier_root_job_ids=(),
        verifier_observation_ids=(),
        verifier_dependency_coordinates=(),
        direct_admitted_pair_ids=(claim.admitted_pair_id,),
        qualifying_admitted_pair_digests=(),
        verifier_artifact_ids=(),
        verifier_pair_input_hashes=(),
        direct_frontier_keys=(),
    )


def _lock_f8_tier10(
    monkeypatch: pytest.MonkeyPatch,
    *,
    claim: Any,
    owner: Any,
    cursor: _F8Tier10Cursor,
) -> None:
    monkeypatch.setattr(
        postgres_withdrawal,
        "_lock_d29_direct_attempts",
        lambda *_args, **_kwargs: {},
    )
    locked = postgres_withdrawal._lock_d29_tier_10_authority(
        cursor,  # type: ignore[arg-type]
        _f8_tier10_locator(claim, owner),
        predecessor_epoch_id=10,
        candidate_policy_id="policy-a",
    )
    assert locked == ({}, ())


def _validate_f8_owner(owner: Any) -> dict[str, Any]:
    return postgres_withdrawal._validate_d30_owner_topology_from_held_rows(
        owner,
        candidate_policy_id="policy-a",
        verifier_execution_spec_hash="2" * 64,
        activation_base_epoch_id=10,
        predecessor_epoch_id=10,
    )


def test_working_delta_not_optional_execution_classifies_dynamic_claim(
    function_source: Callable[[str], str],
) -> None:
    gather = function_source("_gather_d30_claim_authority")
    assert "groundloop_working_observation_delta" in gather
    assert "_d30_execution_coordinates" in gather
    delta_position = gather.index("groundloop_working_observation_delta")
    execution_position = gather.index("_d30_execution_coordinates")
    assert delta_position < execution_position
    assert "if currency.installed_revision == 0" in gather


def test_dynamic_full_key_revision_and_injective_child_equalities_are_explicit(
    function_source: Callable[[str], str],
) -> None:
    source = _dynamic_sources(function_source)
    gather = function_source("_gather_d30_claim_authority")
    for field in (
        "subject_kind",
        "subject_id",
        "chunk_version_id",
        "task_type",
        "observation_id",
        "installed_revision",
        "working_observation_id",
        "base_observation_id",
        "completed_revision",
    ):
        assert field in source
    assert "source_epoch" in gather
    assert "delta_installed_revision = delta[7]" in gather
    assert "int(row[17]) == delta_installed_revision" in gather
    revision_selector = gather[
        gather.index("revision_matches") : gather.index("child =")
    ]
    assert "currency.installed_revision" not in revision_selector
    assert "selected_children" in source
    assert "reuse a child revision" in source


def test_both_parent_kinds_and_arbitrary_retained_identifiers_are_supported(
    function_source: Callable[[str], str],
) -> None:
    source = _dynamic_sources(function_source)
    lowered = source.lower()
    assert "impact_discovery" in lowered
    assert "frontier_retrieve" in lowered
    for value in (
        "result_artifact_id",
        "result_artifact_hash",
        "observation_id",
        "model_id",
        "model_version",
        "prompt_version",
        "task_type",
    ):
        assert value in source
    for forbidden in ("PairVerificationInput(", "PairVerificationArtifact("):
        assert forbidden not in source


def test_optional_execution_uses_all_unique_coordinates_and_keeps_reuse_scalar(
    function_source: Callable[[str], str],
) -> None:
    source = _dynamic_sources(function_source)
    execution_select = postgres_withdrawal._D30_M4_EXECUTION_SELECT
    assert "observation_id" in execution_select
    assert "job_id" in execution_select
    assert "admitted_pair_id" in execution_select
    for field in (
        "execution_spec_hash",
        "pair_input_hash",
        "calibration_version",
        "calibration_artifact_sha256",
        "temperature",
        "raw_logits",
        "raw_output_hash",
        "reused_from_observation_id",
    ):
        assert field in source
    assert "groundloop_model_artifact" in source
    assert "groundloop_prompt_artifact" in source
    assert "verification" in source
    assert "reused_from_observation_id = observation_id" not in source


def test_execution_absence_is_all_three_absences_plus_raw_result_hash_equality(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_validate_d30_dynamic_claim")
    assert "if not present" in source
    assert "raw_output_hash" in source
    assert "child[15]" in source
    assert "optional execution coordinates disagree" in source


def test_exact_admission_and_parent_result_do_not_reconstruct_root_preimages(
    function_source: Callable[[str], str],
) -> None:
    source = _dynamic_sources(function_source)
    assert "groundloop_admitted_pair" in source
    assert "admitted_pair_id = %s" in source
    assert "groundloop_m4_discovery_result" in source
    assert "root_job_id = %s" in source
    for field in (
        "fused_rank",
        "reasons",
        "mandatory_lineage",
        "channel_hit_count",
        "channel_set_hash",
        "admitted_pair_count",
        "admitted_pair_set_hash",
    ):
        assert field in source
    for forbidden in (
        "FROM groundloop_impact_channel_hit",
        "JOIN groundloop_impact_channel_hit",
        "_validate_direct_discovery_closure",
        "_direct_channel_set_hash",
    ):
        assert forbidden not in source


def test_cross_policy_and_every_cited_child_binding_fail_closed(
    function_source: Callable[[str], str],
) -> None:
    source = _dynamic_sources(function_source)
    for field in (
        "candidate_policy_id",
        "parent_job_id",
        "payload_hash",
        "execution_spec_hash",
        "job_state",
        "completion_digest",
        "result_artifact_id",
        "result_artifact_hash",
        "completed_revision",
    ):
        assert field in source


def test_valid_dynamic_claim_accepts_absent_present_and_reused_execution() -> None:
    for execution, reused_from in (
        (False, None),
        (True, None),
        (True, "older-observation"),
    ):
        claim, owner = _valid_dynamic_authority(
            execution=execution,
            reused_from=reused_from,
        )
        postgres_withdrawal._validate_d30_dynamic_claim(
            claim,
            owner=owner,
            candidate_policy_id="policy-a",
        )


def test_source_epoch_and_child_completion_revision_are_independent() -> None:
    claim, owner = _valid_dynamic_authority()
    child = next(row for row in owner.jobs if row[0] == claim.child_job_id)
    assert claim.currency.installed_revision == claim.observation_row[12]
    assert claim.observation_row[12] == owner.epoch_id
    assert claim.delta_row[7] == child[17]
    assert claim.currency.installed_revision != claim.delta_row[7]
    postgres_withdrawal._validate_d30_dynamic_claim(
        claim,
        owner=owner,
        candidate_policy_id="policy-a",
    )


def test_gather_selects_child_by_delta_completion_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claim, owner = _valid_dynamic_authority()
    assert claim.currency.installed_revision != claim.delta_row[7]
    authority = _gather_dynamic_claim_authority(
        monkeypatch,
        claim=claim,
        owner=owner,
        cursor=_DynamicGatherCursor(claim),
    )
    assert len(authority.dynamic) == 1
    selected = authority.dynamic[0]
    assert selected.child_job_id == claim.child_job_id
    child = next(row for row in owner.jobs if row[0] == selected.child_job_id)
    assert child[17] == claim.delta_row[7]
    assert child[17] != claim.currency.installed_revision


def test_terminal_projection_relock_uses_exact_owner_epoch_and_job_coordinate(
    monkeypatch: pytest.MonkeyPatch,
    function_source: Callable[[str], str],
) -> None:
    projection = (
        7,
        "job-a",
        "completed_active",
        None,
        "a" * 64,
        11,
        "b" * 64,
    )
    wrong_epoch_projection = (
        8,
        "job-a",
        "completed_inactive",
        "inactive_at_completion",
        "c" * 64,
        12,
        "d" * 64,
    )
    owner = SimpleNamespace(epoch_id=7, projections=(("job-a", projection),))
    locator = SimpleNamespace(
        d30_claims=SimpleNamespace(owners=(owner,)),
        direct_job_coordinates=((7, "job-a"),),
        direct_dependency_coordinates=(),
    )
    monkeypatch.setattr(
        postgres_withdrawal,
        "_lock_d29_direct_attempts",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        postgres_withdrawal,
        "_lock_d29_m5_attempt_and_output_rows",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        postgres_withdrawal,
        "_lock_d30_typed_owner_evidence",
        lambda *_args, **_kwargs: None,
    )

    source = function_source("_lock_d29_tier_10_authority")
    projection_section = source[
        source.index("preliminary_projections") : source.index("# 10.2")
    ]
    assert "coordinate = (owner.epoch_id, job_id)" in projection_section
    assert "projection_coordinates != locator.direct_job_coordinates" in (
        projection_section
    )
    assert "for projection_epoch_id, job_id in locator.direct_job_coordinates" in (
        projection_section
    )
    assert "WHERE epoch_id = %s AND job_id = %s" in projection_section
    assert "(projection_epoch_id, job_id)" in projection_section
    assert "WHERE job_id = %s" not in projection_section

    exact = _ProjectionRelockCursor(
        {
            (7, "job-a"): projection,
            (8, "job-a"): wrong_epoch_projection,
        }
    )
    with pytest.raises(_AfterProjectionRelock):
        postgres_withdrawal._lock_d29_tier_10_authority(  # type: ignore[arg-type]
            exact,
            locator,
            predecessor_epoch_id=7,
            candidate_policy_id="policy-a",
        )
    assert len(exact.calls) == 1
    exact_sql, exact_parameters = exact.calls[0]
    assert "WHERE epoch_id = %s AND job_id = %s" in exact_sql
    assert exact_parameters == (7, "job-a")

    for rows in (
        {(8, "job-a"): wrong_epoch_projection},
        {
            (7, "job-a"): _cell(projection, 2, "completed_inactive"),
            (8, "job-a"): wrong_epoch_projection,
        },
    ):
        changed = _ProjectionRelockCursor(rows)
        with pytest.raises(EventConflictError, match="terminal projection changed"):
            postgres_withdrawal._lock_d29_tier_10_authority(  # type: ignore[arg-type]
                changed,
                locator,
                predecessor_epoch_id=7,
                candidate_policy_id="policy-a",
            )
        assert len(changed.calls) == 1
        assert changed.calls[0][1] == (7, "job-a")

    duplicate_owner = SimpleNamespace(
        epoch_id=7,
        projections=(("job-a", projection), ("job-a", projection)),
    )
    duplicate_locator = SimpleNamespace(
        d30_claims=SimpleNamespace(owners=(duplicate_owner,)),
        direct_job_coordinates=((7, "job-a"),),
    )
    duplicate_cursor = _ProjectionRelockCursor({(7, "job-a"): projection})
    with pytest.raises(EventConflictError, match="terminal projection changed"):
        postgres_withdrawal._lock_d29_tier_10_authority(  # type: ignore[arg-type]
            duplicate_cursor,
            duplicate_locator,
            predecessor_epoch_id=7,
            candidate_policy_id="policy-a",
        )
    assert duplicate_cursor.calls == []

    mismatched_locator = SimpleNamespace(
        d30_claims=SimpleNamespace(owners=(owner,)),
        direct_job_coordinates=((8, "job-a"),),
    )
    mismatched_cursor = _ProjectionRelockCursor(
        {
            (7, "job-a"): projection,
            (8, "job-a"): wrong_epoch_projection,
        }
    )
    with pytest.raises(EventConflictError, match="terminal projection changed"):
        postgres_withdrawal._lock_d29_tier_10_authority(  # type: ignore[arg-type]
            mismatched_cursor,
            mismatched_locator,
            predecessor_epoch_id=7,
            candidate_policy_id="policy-a",
        )
    assert mismatched_cursor.calls == []


@pytest.mark.parametrize(
    "noise_kind",
    ("unrelated_hit", "other_root_admission", "impact_frontier_overlap"),
)
def test_f8_unrelated_raw_history_neither_authorizes_nor_poisons_exact_admission(
    monkeypatch: pytest.MonkeyPatch,
    noise_kind: str,
) -> None:
    clean_claim, clean_owner = _valid_dynamic_authority()
    assert (
        clean_claim.currency.installed_revision,
        clean_claim.delta_row[7],
        clean_owner.epoch_row[3],
    ) == (7, 11, 20)
    clean_cursor = _f8_cursor(
        clean_claim,
        clean_owner,
        _empty_f8_noise(),
    )
    clean = _gather_dynamic_claim_authority(
        monkeypatch,
        claim=clean_claim,
        owner=clean_owner,
        cursor=clean_cursor,
    )
    assert clean.dynamic == (clean_claim,)
    _lock_f8_tier10(
        monkeypatch,
        claim=clean.dynamic[0],
        owner=clean_owner,
        cursor=clean_cursor,
    )
    _validate_f8_owner(clean_owner)
    postgres_withdrawal._validate_d30_dynamic_claim(
        clean.dynamic[0],
        owner=clean_owner,
        candidate_policy_id="policy-a",
    )
    _assert_f8_exact_admission_trace(clean_cursor, clean_claim, locked=True)

    claim, noisy_owner, noise = _expanded_f8_authority(noise_kind)
    assert (
        claim.currency.installed_revision,
        claim.delta_row[7],
        noisy_owner.epoch_row[3],
    ) == (7, 11, 20)
    noisy_cursor = _f8_cursor(claim, noisy_owner, noise)
    noisy = _gather_dynamic_claim_authority(
        monkeypatch,
        claim=claim,
        owner=noisy_owner,
        cursor=noisy_cursor,
    )
    assert noisy.dynamic == clean.dynamic
    _lock_f8_tier10(
        monkeypatch,
        claim=noisy.dynamic[0],
        owner=noisy_owner,
        cursor=noisy_cursor,
    )
    _validate_f8_owner(noisy_owner)
    postgres_withdrawal._validate_d30_dynamic_claim(
        noisy.dynamic[0],
        owner=noisy_owner,
        candidate_policy_id="policy-a",
    )
    _assert_f8_exact_admission_trace(noisy_cursor, claim, locked=True)
    roots = {str(row[0]): str(row[3]) for row in noisy_owner.jobs if row[2] is None}
    assert set(roots.values()) == {
        JobKind.IMPACT_DISCOVERY.value,
        JobKind.FRONTIER_RETRIEVE.value,
    }
    selected_children = tuple(
        row
        for row in noisy_owner.jobs
        if str(row[3]) == JobKind.VERIFY_PAIR.value
        and tuple(str(value) for value in row[7:9]) == ("claim-a", "chunk-a")
    )
    assert len(selected_children) == 1
    if noise_kind == "unrelated_hit":
        assert len(noisy_cursor.unrelated_hit_rows) == 1
        assert len(noisy_cursor.raw_root_hit_memberships) == 1
        hit_root_id, hit_row = noisy_cursor.raw_root_hit_memberships[0]
        assert hit_root_id in roots
        assert roots[hit_root_id] == JobKind.FRONTIER_RETRIEVE.value
        assert hit_row == noisy_cursor.unrelated_hit_rows[0]
        assert noisy_cursor.other_root_admissions == ()
        assert noisy_cursor.raw_root_admission_memberships == ()
    elif noise_kind == "other_root_admission":
        assert len(noisy_cursor.other_root_admissions) == 1
        assert len(noisy_cursor.raw_root_admission_memberships) == 1
        root_id, admission = noisy_cursor.other_root_admissions[0]
        assert root_id in roots
        assert roots[root_id] == JobKind.FRONTIER_RETRIEVE.value
        assert len(admission) == 8
        assert root_id not in admission
        assert noisy_cursor.raw_root_admission_memberships == (
            (root_id, str(admission[0])),
        )
        other_children = tuple(
            row
            for row in noisy_owner.jobs
            if row[2] == root_id
            and tuple(str(value) for value in row[7:9])
            == (str(admission[2]), str(admission[3]))
        )
        assert len(other_children) == 1
        assert str(admission[0]) != claim.admitted_pair_id
    else:
        assert noisy_cursor.other_root_admissions == ()
        memberships = noisy_cursor.raw_root_admission_memberships
        assert len(memberships) == 2
        assert {root_id for root_id, _admission_id in memberships} == set(roots)
        assert {admission_id for _root_id, admission_id in memberships} == {
            claim.admitted_pair_id
        }


@pytest.mark.parametrize(
    "noise_kind",
    ("unrelated_hit", "other_root_admission", "impact_frontier_overlap"),
)
@pytest.mark.parametrize("cited_state", ("missing", "corrupt"))
def test_f8_noise_cannot_replace_missing_or_corrupt_exact_admission(
    monkeypatch: pytest.MonkeyPatch,
    noise_kind: str,
    cited_state: str,
) -> None:
    claim, owner, noise = _expanded_f8_authority(noise_kind)
    _validate_f8_owner(owner)
    cited_admission = (
        None
        if cited_state == "missing"
        else _cell(claim.admitted_pair_row, 2, "claim-corrupt")
    )
    cursor = _f8_cursor(
        claim,
        owner,
        noise,
        cited_admission_row=cited_admission,
    )
    if cited_state == "missing":
        with pytest.raises(EventConflictError, match="child closure is incomplete"):
            _gather_dynamic_claim_authority(
                monkeypatch,
                claim=claim,
                owner=owner,
                cursor=cursor,
            )
        _assert_f8_exact_admission_trace(cursor, claim, locked=False)
    else:
        authority = _gather_dynamic_claim_authority(
            monkeypatch,
            claim=claim,
            owner=owner,
            cursor=cursor,
        )
        _lock_f8_tier10(
            monkeypatch,
            claim=authority.dynamic[0],
            owner=owner,
            cursor=cursor,
        )
        with pytest.raises(EventConflictError, match="retained authority changed"):
            postgres_withdrawal._validate_d30_dynamic_claim(
                authority.dynamic[0],
                owner=owner,
                candidate_policy_id="policy-a",
            )
        _assert_f8_exact_admission_trace(cursor, claim, locked=True)


def test_source_epoch_and_child_completion_revision_cross_links_fail_closed() -> None:
    claim, owner = _valid_dynamic_authority()
    child = next(row for row in owner.jobs if row[0] == claim.child_job_id)
    changed_jobs = tuple(
        _cell(row, 17, int(child[17]) + 1) if row[0] == claim.child_job_id else row
        for row in owner.jobs
    )
    malformed = (
        replace(
            claim,
            currency=replace(
                claim.currency,
                installed_revision=claim.currency.installed_revision + 1,
            ),
        ),
        replace(claim, observation_row=_cell(claim.observation_row, 12, 8)),
    )
    for changed_claim in malformed:
        with pytest.raises(EventConflictError):
            postgres_withdrawal._validate_d30_dynamic_claim(
                changed_claim,
                owner=owner,
                candidate_policy_id="policy-a",
            )
    with pytest.raises(EventConflictError):
        postgres_withdrawal._validate_d30_dynamic_claim(
            claim,
            owner=replace(owner, jobs=changed_jobs),
            candidate_policy_id="policy-a",
        )


@pytest.mark.parametrize(
    "parent_kind",
    (JobKind.IMPACT_DISCOVERY, JobKind.FRONTIER_RETRIEVE),
)
def test_valid_dynamic_claim_accepts_both_parent_kinds(
    parent_kind: JobKind,
) -> None:
    claim, owner = _valid_dynamic_authority(parent_kind=parent_kind)
    postgres_withdrawal._validate_d30_owner_topology_from_held_rows(
        owner,
        candidate_policy_id="policy-a",
        verifier_execution_spec_hash="2" * 64,
        activation_base_epoch_id=10,
        predecessor_epoch_id=10,
    )
    postgres_withdrawal._validate_d30_dynamic_claim(
        claim,
        owner=owner,
        candidate_policy_id="policy-a",
    )


def test_historical_root_execution_hash_is_self_authenticated() -> None:
    historical_root_hash = "9" * 64
    claim, owner = _valid_dynamic_authority(
        parent_execution_spec_hash=historical_root_hash,
    )
    parent = next(job for job in owner.jobs if job[2] is None)
    child = next(job for job in owner.jobs if job[2] is not None)
    assert parent[6] == historical_root_hash
    assert child[6] == "2" * 64
    postgres_withdrawal._validate_d30_owner_topology_from_held_rows(
        owner,
        candidate_policy_id="policy-a",
        verifier_execution_spec_hash="2" * 64,
        activation_base_epoch_id=10,
        predecessor_epoch_id=10,
    )
    postgres_withdrawal._validate_d30_dynamic_claim(
        claim,
        owner=owner,
        candidate_policy_id="policy-a",
    )


@pytest.mark.parametrize(
    ("field", "index", "bad"),
    (
        ("delta_row", 0, 8),
        ("delta_row", 1, "requirement"),
        ("delta_row", 2, "claim-b"),
        ("delta_row", 3, "chunk-b"),
        ("delta_row", 4, "other-task"),
        ("delta_row", 5, "unexpected-base-observation"),
        ("delta_row", 6, "observation-b"),
        ("delta_row", 7, 8),
        ("observation_row", 12, 8),
    ),
)
def test_dynamic_delta_and_revision_corruptions_fail_closed(
    field: str,
    index: int,
    bad: object,
) -> None:
    claim, owner = _valid_dynamic_authority()
    claim = replace(claim, **{field: _cell(getattr(claim, field), index, bad)})
    with pytest.raises(EventConflictError):
        postgres_withdrawal._validate_d30_dynamic_claim(
            claim,
            owner=owner,
            candidate_policy_id="policy-a",
        )


def test_predecessor_candidate_coverage_controls_exact_delta_base() -> None:
    claim, owner = _valid_dynamic_authority()
    owner = replace(owner, update_row=_cell(owner.update_row, 3, 5))
    covering = (
        "claim",
        "claim-a",
        "chunk-a",
        "arbitrary-task",
        "base-observation",
        3,
        6,
    )
    claim = replace(
        claim,
        predecessor_epoch_id=5,
        predecessor_candidate=covering,
        delta_row=_cell(claim.delta_row, 5, "base-observation"),
    )
    postgres_withdrawal._validate_d30_dynamic_claim(
        claim,
        owner=owner,
        candidate_policy_id="policy-a",
    )

    noncovering = replace(
        claim,
        predecessor_candidate=_cell(covering, 6, 5),
        delta_row=_cell(claim.delta_row, 5, None),
    )
    postgres_withdrawal._validate_d30_dynamic_claim(
        noncovering,
        owner=owner,
        candidate_policy_id="policy-a",
    )

    for malformed in (
        replace(claim, delta_row=_cell(claim.delta_row, 5, None)),
        replace(
            noncovering, delta_row=_cell(noncovering.delta_row, 5, "base-observation")
        ),
        replace(claim, predecessor_candidate=_cell(covering, 2, "chunk-b")),
    ):
        with pytest.raises(EventConflictError):
            postgres_withdrawal._validate_d30_dynamic_claim(
                malformed,
                owner=owner,
                candidate_policy_id="policy-a",
            )


def test_two_observations_cannot_reuse_one_child_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claim, owner = _valid_dynamic_authority()
    other_currency = replace(claim.currency, observation_id="observation-b")
    other = replace(
        claim,
        currency=other_currency,
        observation_row=_cell(claim.observation_row, 0, "observation-b"),
        delta_row=_cell(claim.delta_row, 6, "observation-b"),
    )
    activation = (
        "activation-a",
        "a" * 64,
        10,
        object(),
        "m5_active",
        1,
        10,
        10,
        1,
        "committed",
        "sealed",
        "complete",
        "strict",
        object(),
    )
    locator = SimpleNamespace(
        d30_claims=SimpleNamespace(
            activation_row=activation,
            currency_rows=(claim.currency, other.currency),
            owners=(owner,),
            dynamic=(claim, other),
            bootstrap=(),
        )
    )
    monkeypatch.setattr(
        postgres_withdrawal,
        "_is_sealed_direct_owner",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        postgres_withdrawal,
        "_reread_d30_owner_headers",
        lambda *_args, **_kwargs: None,
    )
    with pytest.raises(EventConflictError, match="reuse a child revision"):
        postgres_withdrawal._lock_d30_dynamic_owner_topology(
            object(),  # type: ignore[arg-type]
            locator,  # type: ignore[arg-type]
            {str(job[0]): object() for job in owner.jobs},
            {
                str(job[0]): object()
                for job in owner.jobs
                if job[2] is None and str(job[3]) == JobKind.IMPACT_DISCOVERY.value
            },
            predecessor_epoch_id=10,
            candidate_policy_id="policy-a",
            verifier_execution_spec_hash="2" * 64,
        )


@pytest.mark.parametrize(
    ("field", "index", "bad"),
    (
        ("admitted_pair_row", 0, "f" * 64),
        ("admitted_pair_row", 2, "claim-b"),
        ("admitted_pair_row", 3, "chunk-b"),
        ("admitted_pair_row", 4, "policy-b"),
        ("parent_result_row", 0, "other-parent"),
        ("parent_result_row", 1, 8),
        ("parent_result_row", 2, "different-result"),
        ("parent_result_row", 3, "a" * 64),
    ),
)
def test_dynamic_admission_and_parent_result_corruptions_fail_closed(
    field: str,
    index: int,
    bad: object,
) -> None:
    claim, owner = _valid_dynamic_authority()
    claim = replace(claim, **{field: _cell(getattr(claim, field), index, bad)})
    with pytest.raises(EventConflictError):
        postgres_withdrawal._validate_d30_dynamic_claim(
            claim,
            owner=owner,
            candidate_policy_id="policy-a",
        )


@pytest.mark.parametrize("coordinate", (0, 1, 2))
def test_partial_optional_execution_is_rejected(coordinate: int) -> None:
    claim, owner = _valid_dynamic_authority(execution=True)
    rows = list(claim.execution_rows)
    rows[coordinate] = None
    claim = replace(claim, execution_rows=tuple(rows))
    with pytest.raises(EventConflictError, match="coordinates disagree"):
        postgres_withdrawal._validate_d30_dynamic_claim(
            claim,
            owner=owner,
            candidate_policy_id="policy-a",
        )


@pytest.mark.parametrize(
    ("index", "bad"),
    (
        (0, "other-observation"),
        (1, "other-job"),
        (2, "f" * 64),
        (5, "f" * 64),
        (10, (None, 0.0, 0.0)),
        (11, "f" * 64),
    ),
)
def test_execution_binding_corruptions_fail_closed(index: int, bad: object) -> None:
    claim, owner = _valid_dynamic_authority(execution=True)
    execution = _cell(claim.execution_rows[0], index, bad)  # type: ignore[arg-type]
    claim = replace(claim, execution_rows=(execution, execution, execution))
    with pytest.raises(EventConflictError):
        postgres_withdrawal._validate_d30_dynamic_claim(
            claim,
            owner=owner,
            candidate_policy_id="policy-a",
        )


def test_execution_model_prompt_task_and_self_reuse_fail_closed() -> None:
    claim, owner = _valid_dynamic_authority(execution=True)
    for malformed in (
        replace(claim, model_row=("model-artifact", "embedding")),
        replace(claim, prompt_row=("prompt-artifact", "generation")),
    ):
        with pytest.raises(EventConflictError):
            postgres_withdrawal._validate_d30_dynamic_claim(
                malformed,
                owner=owner,
                candidate_policy_id="policy-a",
            )

    self_reused, owner = _valid_dynamic_authority(
        execution=True,
        reused_from="observation-a",
    )
    with pytest.raises(EventConflictError):
        postgres_withdrawal._validate_d30_dynamic_claim(
            self_reused,
            owner=owner,
            candidate_policy_id="policy-a",
        )


def test_execution_coordinate_probe_preserves_three_independent_results() -> None:
    execution = tuple(range(13))
    cursor = _CoordinateCursor(
        {
            "observation_id": execution,
            "job_id": execution,
            "admitted_pair_id": execution,
        }
    )
    rows = postgres_withdrawal._d30_execution_coordinates(  # type: ignore[arg-type]
        cursor,
        observation_id="observation-a",
        job_id="job-a",
        admitted_pair_id="pair-a",
    )
    assert rows == (execution, execution, execution)
    assert tuple((sql.split(" WHERE ")[-1], value) for sql, value in cursor.calls) == (
        ("observation_id = %s", "observation-a"),
        ("job_id = %s", "job-a"),
        ("admitted_pair_id = %s", "pair-a"),
    )


def test_execution_coordinate_probe_retains_partial_and_total_absence() -> None:
    execution = tuple(range(13))
    partial = _CoordinateCursor(
        {
            "observation_id": execution,
            "job_id": None,
            "admitted_pair_id": execution,
        }
    )
    assert postgres_withdrawal._d30_execution_coordinates(  # type: ignore[arg-type]
        partial,
        observation_id="observation-a",
        job_id="job-a",
        admitted_pair_id="pair-a",
    ) == (execution, None, execution)
    absent = _CoordinateCursor(
        {"observation_id": None, "job_id": None, "admitted_pair_id": None}
    )
    assert postgres_withdrawal._d30_execution_coordinates(  # type: ignore[arg-type]
        absent,
        observation_id="observation-a",
        job_id="job-a",
        admitted_pair_id="pair-a",
    ) == (None, None, None)


def _arbitrary_retained_execution_row() -> tuple[object, ...]:
    return (
        "observation-arbitrary",
        "job-arbitrary",
        "pair-arbitrary",
        "model-arbitrary",
        "prompt-arbitrary",
        "a" * 64,
        "b" * 64,
        "calibration-arbitrary",
        "c" * 64,
        0.8125,
        (7.25, 3.5, 0.125),
        "d" * 64,
        "older-observation-arbitrary",
    )


@pytest.mark.parametrize(
    ("index", "changed"),
    (
        (6, "e" * 64),
        (7, "different-calibration"),
        (8, "f" * 64),
        (9, 1.375),
        (10, (0.25, 8.5, 4.0)),
        (12, "different-older-observation"),
    ),
)
def test_locked_execution_revalidation_rejects_changed_retained_field(
    index: int,
    changed: object,
) -> None:
    preliminary = _arbitrary_retained_execution_row()
    locked = _cell(preliminary, index, changed)
    with pytest.raises(EventConflictError, match="coordinates changed"):
        postgres_withdrawal._validate_d30_locked_execution_coordinates(
            preliminary_rows=(preliminary, preliminary, preliminary),
            locked_rows=(locked, locked, locked),
        )


def test_locked_execution_accepts_stable_arbitrary_values_and_reuse() -> None:
    execution = _arbitrary_retained_execution_row()
    postgres_withdrawal._validate_d30_locked_execution_coordinates(
        preliminary_rows=(execution, execution, execution),
        locked_rows=(execution, execution, execution),
    )
