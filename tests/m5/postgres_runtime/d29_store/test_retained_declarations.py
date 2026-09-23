"""Retained all-state declaration hydration contract tests."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest

from groundloop.errors import EventConflictError
from groundloop.m5.runtime import digests, postgres_withdrawal
from groundloop.m5.runtime.postgres_withdrawal import (
    _DocumentClosure,
    _lock_d29_candidate_source_closure,
    _stored_direct_open,
    _stored_requirement_declarations,
    _validate_retained_direct_verifier_closure,
)

ALL_STATES = (
    "declared",
    "running",
    "completed_active",
    "completed_inactive",
    "retryable_failed",
    "terminal_failed",
    "cancelled",
)


def _first_m5_retained_projection(d29_database: Any) -> tuple[Any, _DocumentClosure]:
    connection = d29_database.connection
    epoch_id = int(d29_database.first_m5["epoch_id"])
    runtime = connection.execute(
        """
        SELECT epoch.revision, update.update_kind,
               update.previous_published_epoch_id, runtime.revision,
               runtime.runtime_state, runtime.open_work_count,
               runtime.open_scope_count, runtime.blocking_failure_count,
               runtime.requirement_registry_snapshot_digest,
               runtime.active_chunk_snapshot_digest,
               runtime.requirement_root_set_hash, epoch.event_id
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_update AS update USING (epoch_id)
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        WHERE epoch.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert runtime is not None
    reverse_chunks = tuple(
        str(row[0])
        for row in connection.execute(
            """
            SELECT inserted_chunk_version_id
            FROM groundloop_m5_discovery_scope
            WHERE epoch_id = %s AND direction = 'reverse_chunk'
            ORDER BY inserted_chunk_version_id COLLATE "C"
            """,
            (epoch_id,),
        ).fetchall()
    )
    event = SimpleNamespace(
        structural_event_id=str(runtime[11]),
        direct_plan=SimpleNamespace(
            inserted_chunk_version_ids=reverse_chunks,
            deactivated_chunk_version_ids=(),
        ),
    )
    closure = _DocumentClosure(
        epoch_id=epoch_id,
        epoch_revision=int(runtime[0]),
        update_kind=str(runtime[1]),
        previous_epoch_id=int(runtime[2]),
        runtime_revision=int(runtime[3]),
        runtime_state=str(runtime[4]),
        open_work_count=int(runtime[5]),
        open_scope_count=int(runtime[6]),
        blocking_failure_count=int(runtime[7]),
        candidate_policy=d29_database.database.manifest,
        requirement_snapshot_digest=str(runtime[8]).strip(),
        chunk_snapshot_digest=str(runtime[9]).strip(),
        requirement_root_set_hash=str(runtime[10]).strip(),
        direct_root_job_ids=(),
        direct_scope_root_job_ids=(),
        direct_fallback_claim_ids=(),
    )
    return event, closure


def test_retained_m5_reader_uses_all_state_epoch_range_and_explicit_sort(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_stored_requirement_declarations")
    assert "WHERE epoch_id = %s" in source
    assert "job_state = ANY" not in source
    assert "WITH hydrated AS MATERIALIZED" in source
    assert "ORDER BY job_state, logical_job_id" in source
    assert 'ORDER BY logical_job_id COLLATE "C"' in source
    assert "groundloop_m5_requirement_root_provenance" in source
    assert "_validate_m5_job_state" in source
    assert "_validate_m5_scope_state" in source
    assert "validate_manifest_and_scope" in source
    assert "for child_id in children_by_parent.get(job_id, ())" in source
    assert "str(row[19]) not in _ALL_JOB_STATES" in source
    assert len(ALL_STATES) == 7


def test_live_retained_m5_reader_hydrates_all_seven_states(
    d29_database: Any,
) -> None:
    event, closure = _first_m5_retained_projection(d29_database)
    connection = d29_database.connection
    stored_states = tuple(
        str(row[0])
        for row in connection.execute(
            """
            SELECT DISTINCT job_state
            FROM groundloop_m5_semantic_job
            WHERE epoch_id = %s
            ORDER BY job_state
            """,
            (closure.epoch_id,),
        ).fetchall()
    )
    assert set(stored_states) == set(ALL_STATES)
    savepoint = "d29_retained_all_states_document_provenance"
    connection.execute(f"SAVEPOINT {savepoint}")
    try:
        forward_root_ids = tuple(
            str(row[0]).strip()
            for row in connection.execute(
                """
                SELECT logical_job_id
                FROM groundloop_m5_semantic_job
                WHERE epoch_id = %s
                  AND parent_job_id IS NULL
                  AND job_kind = 'forward_requirement_retrieval'
                ORDER BY logical_job_id COLLATE "C"
                """,
                (closure.epoch_id,),
            ).fetchall()
        )
        assert forward_root_ids
        connection.execute(
            """
            ALTER TABLE groundloop_m5_requirement_root_provenance
            DISABLE TRIGGER groundloop_m5_requirement_root_provenance_immutable
            """
        )
        for root_id in forward_root_ids:
            assert (
                connection.execute(
                    """
                    UPDATE groundloop_m5_requirement_root_provenance
                    SET fallback_required = true, provenance_digest = %s
                    WHERE epoch_id = %s AND root_job_id = %s
                    """,
                    (
                        digests.requirement_root_provenance_digest(
                            epoch_id=closure.epoch_id,
                            root_job_id=root_id,
                            fallback_required=True,
                        ),
                        closure.epoch_id,
                        root_id,
                    ),
                ).rowcount
                == 1
            )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.execute(
            """
            ALTER TABLE groundloop_m5_requirement_root_provenance
            ENABLE TRIGGER groundloop_m5_requirement_root_provenance_immutable
            """
        )
        connection.execute("SET CONSTRAINTS ALL DEFERRED")
        with connection.cursor() as cursor:
            plan, roots = _stored_requirement_declarations(  # type: ignore[arg-type]
                cursor, event, closure
            )
        assert plan.event_id == event.structural_event_id
        assert roots
        assert {
            str(row[0])
            for row in connection.execute(
                """
                SELECT job_state FROM groundloop_m5_semantic_job
                WHERE epoch_id = %s
                """,
                (closure.epoch_id,),
            ).fetchall()
        } == set(ALL_STATES)
    finally:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")


@pytest.mark.parametrize("extra", ("scope", "provenance"))
def test_live_retained_m5_reader_rejects_malformed_extra_rows(
    d29_database: Any,
    extra: str,
) -> None:
    event, closure = _first_m5_retained_projection(d29_database)
    connection = d29_database.connection
    child = connection.execute(
        """
        SELECT child.logical_job_id, child.parent_job_id
        FROM groundloop_m5_semantic_job AS child
        WHERE child.epoch_id = %s AND child.parent_job_id IS NOT NULL
        ORDER BY child.logical_job_id COLLATE "C"
        LIMIT 1
        """,
        (closure.epoch_id,),
    ).fetchone()
    assert child is not None and child[1] is not None
    child_id = str(child[0]).strip()
    parent_id = str(child[1]).strip()
    savepoint = f"d29_retained_extra_{extra}"
    connection.execute(f"SAVEPOINT {savepoint}")
    try:
        if extra == "scope":
            connection.execute(
                """
                INSERT INTO groundloop_m5_discovery_scope (
                    root_job_id, epoch_id, direction,
                    requirement_version_id, inserted_chunk_version_id,
                    candidate_policy_id,
                    requirement_registry_snapshot_digest,
                    active_chunk_snapshot_digest, scope_contract_digest,
                    scope_state, staged_result_artifact_hash,
                    scope_closure_digest, child_set_hash, completion_digest,
                    created_revision, staged_revision, closed_revision,
                    created_at, closed_at
                )
                SELECT %s, epoch_id, direction,
                       requirement_version_id, inserted_chunk_version_id,
                       candidate_policy_id,
                       requirement_registry_snapshot_digest,
                       active_chunk_snapshot_digest, %s,
                       scope_state, staged_result_artifact_hash,
                       scope_closure_digest, child_set_hash, completion_digest,
                       created_revision, staged_revision, closed_revision,
                       created_at, closed_at
                FROM groundloop_m5_discovery_scope
                WHERE root_job_id = %s
                """,
                (
                    child_id,
                    hashlib.sha256(
                        f"d29-extra-child-scope:{child_id}".encode()
                    ).hexdigest(),
                    parent_id,
                ),
            )
        else:
            connection.execute(
                """
                INSERT INTO groundloop_m5_requirement_root_provenance (
                    epoch_id, root_job_id, fallback_required,
                    provenance_digest
                ) VALUES (%s, %s, true, %s)
                """,
                (
                    closure.epoch_id,
                    child_id,
                    digests.requirement_root_provenance_digest(
                        epoch_id=closure.epoch_id,
                        root_job_id=child_id,
                        fallback_required=True,
                    ),
                ),
            )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="child closure"):
                _stored_requirement_declarations(  # type: ignore[arg-type]
                    cursor, event, closure
                )
    finally:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")


def test_retained_direct_reader_uses_all_state_index_and_dependency_order(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_stored_direct_open")
    assert "job_state = ANY(%s)" in source
    assert 'ORDER BY job_id COLLATE "C"' in source
    assert "FROM groundloop_semantic_job_dependency" in source
    assert 'ORDER BY parent_job_id COLLATE "C", child_job_id COLLATE "C"' in source
    assert "_validate_m4_job_state" in source
    assert "_validate_direct_discovery_closure" in source
    assert "_validate_retained_direct_verifier_closure" in source
    assert "lock_authority=False" in source
    assert "expected_dependencies" in source


def test_live_retained_direct_reader_validates_all_seven_full_row_states(
    d29_direct_all_states: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validated_states: list[str] = []
    original = postgres_withdrawal._validate_m4_job_state

    def record_validation(
        spec: Any, row: tuple[object, ...], child_job_ids: tuple[str, ...]
    ) -> None:
        validated_states.append(str(row[10]))
        original(spec, row, child_job_ids)

    monkeypatch.setattr(
        postgres_withdrawal,
        "_validate_m4_job_state",
        record_validation,
    )
    with d29_direct_all_states.connection.cursor() as cursor:
        direct_open = _stored_direct_open(
            cursor,
            d29_direct_all_states.event,
            d29_direct_all_states.closure,
        )

    expected = dict(d29_direct_all_states.state_job_ids)
    assert len(validated_states) == len(ALL_STATES)
    assert set(validated_states) == set(ALL_STATES)
    assert {job.job_id for job in direct_open.root_jobs} == set(expected.values())
    assert direct_open.discovery_scopes == ()
    assert direct_open.withdrawal is not None
    assert direct_open.withdrawal.fallback_claim_ids == (
        d29_direct_all_states.closure.direct_fallback_claim_ids
    )


def test_retained_direct_verifier_reconstructs_exact_result_provenance(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_validate_retained_direct_verifier_closure")
    for relation in (
        "groundloop_m4_verification_execution",
        "groundloop_admitted_pair",
        "groundloop_semantic_observation",
        "groundloop_model_artifact",
        "groundloop_prompt_artifact",
        "groundloop_answer_citation",
    ):
        assert relation in source
    assert "PairVerificationInput(" in source
    assert "M5TypedDirectVerificationExecution(" in source
    assert "PairVerificationArtifact(" in source
    assert "verification_artifact_payload_hash(artifact)" in source
    assert "completed retained verifier lacks one execution provenance row" in source


def test_live_completed_direct_verifier_cannot_omit_result_provenance(
    d29_database: Any,
) -> None:
    connection = d29_database.connection
    child_job_id = str(d29_database.first_m4["child_job_id"])
    row = connection.execute(
        """
        SELECT job_id, epoch_id, parent_job_id, job_kind,
               candidate_policy_id, payload_hash, execution_spec_hash,
               claim_id, chunk_version_id, expandable, job_state,
               child_closed, child_set_hash, completion_digest,
               result_artifact_id, result_artifact_hash,
               created_revision, completed_revision, created_at, completed_at
        FROM groundloop_semantic_job WHERE job_id = %s
        """,
        (child_job_id,),
    ).fetchone()
    assert row is not None
    completed_row = list(row)
    completed_row[10] = "completed_active"
    spec = SimpleNamespace(
        job_id=child_job_id,
        pair=SimpleNamespace(
            claim_id=str(row[7]),
            chunk_version_id=str(row[8]),
        ),
    )
    with connection.cursor() as cursor:
        with pytest.raises(EventConflictError, match="execution provenance"):
            _validate_retained_direct_verifier_closure(  # type: ignore[arg-type]
                cursor,
                spec,
                tuple(completed_row),
            )


def test_retained_direct_manifest_is_exactly_three_typed_arrays(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_document_declaration_manifest")
    assert '"direct_root_job_ids"' in source
    assert '"direct_scope_root_job_ids"' in source
    assert '"direct_fallback_claim_ids"' in source
    assert "repr(" not in source
    assert "json.dumps" not in source
    validation = function_source("_validate_document_manifest")
    assert "tuple(value)" in validation
    assert "set(nested)" in validation


def test_existing_hydration_validates_declarations_before_terminal_cutoff(
    function_source: Callable[[str], str],
) -> None:
    for name in (
        "_plan_document_requirement_withdrawal",
        "_preview_document_direct_open",
        "_hydrate_retained_direct_open",
        "_load_retained_document_open",
    ):
        source = function_source(name)
        terminal = source.rindex("_raise_if_terminal")
        if "_stored_requirement_declarations" in source:
            assert source.index("_stored_requirement_declarations") < terminal
        if "_stored_direct_open" in source:
            assert source.index("_stored_direct_open") < terminal
        assert "_direct_withdrawal_preview" not in source[:terminal]


def test_live_every_m5_child_revalidates_manifest_and_root_scope(
    d29_database: Any,
) -> None:
    connection = d29_database.connection
    epoch_id = int(d29_database.first_m5["epoch_id"])
    row = connection.execute(
        """
        SELECT logical_job_id, structural_event_id, job_kind,
               candidate_policy_id, candidate_policy_manifest_hash,
               parent_job_id, semantic_pair_digest, scope_contract_digest,
               requirement_registry_snapshot_digest,
               active_chunk_snapshot_digest, execution_spec_hash, expandable
        FROM groundloop_m5_semantic_job
        WHERE epoch_id = %s
          AND job_kind = 'verify_requirement_pair'
          AND job_state = 'declared'
        ORDER BY logical_job_id COLLATE "C"
        LIMIT 1
        """,
        (epoch_id,),
    ).fetchone()
    assert row is not None and row[5] is not None
    original_id = str(row[0]).strip()
    parent_id = str(row[5]).strip()
    wrong_role = hashlib.sha256(b"d29-wrong-child-role").hexdigest()
    wrong_payload = digests.job_payload_digest(
        job_kind=str(row[2]),
        candidate_policy_id=str(row[3]).strip(),
        candidate_policy_manifest_hash=str(row[4]).strip(),
        parent_job_id=parent_id,
        semantic_pair_digest_value=str(row[6]).strip(),
        scope_contract_digest=str(row[7]).strip(),
        requirement_registry_snapshot_digest_value=str(row[8]).strip(),
        active_chunk_snapshot_digest_value=str(row[9]).strip(),
        role_template_hash=wrong_role,
        execution_spec_hash=str(row[10]).strip(),
        expandable=bool(row[11]),
    )
    wrong_id = digests.logical_job_id(str(row[1]), wrong_payload)
    verifier_job_ids = tuple(
        wrong_id if str(item[0]).strip() == original_id else str(item[0]).strip()
        for item in connection.execute(
            """
            SELECT child_job_id
            FROM groundloop_m5_job_dependency
            WHERE parent_job_id = %s
            ORDER BY child_job_id COLLATE "C"
            """,
            (parent_id,),
        ).fetchall()
    )
    root = connection.execute(
        """
        SELECT payload_hash, execution_spec_hash, job_state,
               result_artifact_id, result_artifact_hash,
               scope_closure_digest, archive_reason
        FROM groundloop_m5_semantic_job
        WHERE logical_job_id = %s
        """,
        (parent_id,),
    ).fetchone()
    assert root is not None
    child_set_hash = digests.child_set_digest(verifier_job_ids)
    completion_digest = digests.job_completion_digest(
        logical_job_id_value=parent_id,
        payload_hash=str(root[0]).strip(),
        execution_spec_hash=str(root[1]).strip(),
        terminal_state=str(root[2]),
        result_artifact_id=None if root[3] is None else str(root[3]).strip(),
        result_artifact_hash=None if root[4] is None else str(root[4]).strip(),
        scope_closure_digest=None if root[5] is None else str(root[5]).strip(),
        child_set_hash=child_set_hash,
        archive_reason=None if root[6] is None else str(root[6]),
    )
    runtime = connection.execute(
        """
        SELECT epoch.revision, update.update_kind,
               update.previous_published_epoch_id, runtime.revision,
               runtime.runtime_state, runtime.open_work_count,
               runtime.open_scope_count, runtime.blocking_failure_count,
               runtime.requirement_registry_snapshot_digest,
               runtime.active_chunk_snapshot_digest,
               runtime.requirement_root_set_hash, epoch.event_id
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_update AS update USING (epoch_id)
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        WHERE epoch.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert runtime is not None
    reverse_chunks = tuple(
        str(item[0])
        for item in connection.execute(
            """
            SELECT inserted_chunk_version_id
            FROM groundloop_m5_discovery_scope
            WHERE epoch_id = %s AND direction = 'reverse_chunk'
            ORDER BY inserted_chunk_version_id COLLATE "C"
            """,
            (epoch_id,),
        ).fetchall()
    )
    event = SimpleNamespace(
        structural_event_id=str(runtime[11]),
        direct_plan=SimpleNamespace(
            inserted_chunk_version_ids=reverse_chunks,
            deactivated_chunk_version_ids=(),
        ),
    )
    closure = _DocumentClosure(
        epoch_id=epoch_id,
        epoch_revision=int(runtime[0]),
        update_kind=str(runtime[1]),
        previous_epoch_id=int(runtime[2]),
        runtime_revision=int(runtime[3]),
        runtime_state=str(runtime[4]),
        open_work_count=int(runtime[5]),
        open_scope_count=int(runtime[6]),
        blocking_failure_count=int(runtime[7]),
        candidate_policy=d29_database.database.manifest,
        requirement_snapshot_digest=str(runtime[8]).strip(),
        chunk_snapshot_digest=str(runtime[9]).strip(),
        requirement_root_set_hash=str(runtime[10]).strip(),
        direct_root_job_ids=(),
        direct_scope_root_job_ids=(),
        direct_fallback_claim_ids=(),
    )
    locator = SimpleNamespace(
        candidate_root_job_ids=(parent_id,),
        candidate_root_job_coordinates=((epoch_id, parent_id),),
        verifier_job_ids=verifier_job_ids,
    )

    connection.execute("SAVEPOINT d29_wrong_child_manifest_scope")
    try:
        connection.execute(
            "ALTER TABLE groundloop_m5_semantic_job DISABLE TRIGGER USER"
        )
        connection.execute(
            "ALTER TABLE groundloop_m5_job_dependency DISABLE TRIGGER USER"
        )
        connection.execute(
            """
            UPDATE groundloop_m5_semantic_job
            SET logical_job_id = %s, role_template_hash = %s, payload_hash = %s
            WHERE logical_job_id = %s
            """,
            (wrong_id, wrong_role, wrong_payload, original_id),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_job_dependency
            SET child_job_id = %s
            WHERE parent_job_id = %s AND child_job_id = %s
            """,
            (wrong_id, parent_id, original_id),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_semantic_job
            SET child_set_hash = %s, completion_digest = %s
            WHERE logical_job_id = %s
            """,
            (child_set_hash, completion_digest, parent_id),
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="manifest/scope binding"):
                _lock_d29_candidate_source_closure(  # type: ignore[arg-type]
                    cursor, locator
                )
            with pytest.raises(EventConflictError, match="manifest/scope binding"):
                _stored_requirement_declarations(  # type: ignore[arg-type]
                    cursor, event, closure
                )
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_wrong_child_manifest_scope")
        connection.execute("RELEASE SAVEPOINT d29_wrong_child_manifest_scope")
