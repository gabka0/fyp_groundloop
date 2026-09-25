"""Executable structural tripwires for bounded D29 withdrawal planning."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from groundloop.domain import SubjectKind
from groundloop.errors import EventConflictError, ValidationError
from groundloop.m4.application import ApplicationExecutionPolicy
from groundloop.m4.contracts import stable_m4_digest
from groundloop.m5.runtime import digests as runtime_digests
from groundloop.m5.runtime import postgres_withdrawal
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    M5AttemptDisposition,
    M5AttemptOutput,
    M5AttemptResultArtifact,
    M5JobAttempt,
    M5JobState,
    M5ReplayedOutcome,
    M5RequirementAdmissionChannel,
    M5RequirementAdmittedPair,
    M5RequirementAdmittedPairSource,
    M5RequirementFallbackKey,
    M5TypedEventPlan,
    SemanticPairKey,
)
from groundloop.m5.runtime.frontier import plan_requirement_withdrawal
from groundloop.m5.runtime.persistence import (
    _persist_active_chunk_snapshot,
    _persist_requirement_snapshot,
)
from groundloop.m5.runtime.postgres_withdrawal import (
    _assert_zero_cancellation_authority,
    _candidate_edges,
    _is_sealed_lineage_owner,
    _lock_d29_attempt_outputs,
    _lock_d29_bootstrap_provenance,
    _lock_d29_direct_attempts,
    _lock_d29_direct_frontier,
    _lock_d29_touched_membership,
    _lock_d29_verifier_provenance,
    _observation_edges,
    _plan_document_requirement_withdrawal,
    _preview_document_direct_open,
)


class _SingleRowResult:
    def __init__(self, row: tuple[object, ...]) -> None:
        self._row = row

    def fetchone(self) -> tuple[object, ...]:
        return self._row


class _LineageCursor:
    def __init__(self, row: tuple[object, ...]) -> None:
        self.row = row

    def execute(self, _statement: str, _parameters: object) -> _SingleRowResult:
        return _SingleRowResult(self.row)


def test_candidate_locator_has_the_exact_migration_018_shape(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_candidate_edges")
    assert 'WHERE chunk_version_id COLLATE "C" = %s' in source
    assert "SELECT chunk_version_id, admitted_pair_digest" in source
    assert 'ORDER BY admitted_pair_digest COLLATE "C"' in source
    locator = source.split("locator_rows", maxsplit=1)[0]
    assert "ANY(" not in locator


def test_candidate_validation_orders_lineage_activity_then_policy(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_candidate_edges")
    lineage = source.index("_is_sealed_lineage_owner")
    activity = source.index("_require_active_membership")
    validation = source.index("_validate_candidate_locator_admitted_row")
    source_closure = source.index("groundloop_m5_requirement_admitted_pair_source")
    policy = source.index("cross-policy candidate withdrawal")
    assert lineage < activity < validation < source_closure
    assert source_closure < policy
    assert "M5RequirementScopeSelection(" in source
    assert "M5RequirementAdmittedPair(" in source
    gather = function_source("_gather_d29_locator_authority")
    complete_row = gather.index("subject_kind::text")
    coordinates = gather.index("_candidate_locator_classification_coordinates")
    row_validation = gather.index("_validate_candidate_locator_admitted_row")
    gather_lineage = gather.index("_is_sealed_lineage_owner")
    gather_activity = gather.index("_require_active_membership")
    source_closure = gather.index("groundloop_m5_requirement_admitted_pair_source")
    assert complete_row < coordinates < gather_lineage < gather_activity
    assert gather_activity < row_validation < source_closure


def test_lineage_owner_classification_is_state_shape_total(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_is_sealed_lineage_owner")
    for field in (
        "epoch.publication_mode",
        "runtime.revision",
        "runtime.open_work_count",
        "runtime.open_scope_count",
        "runtime.blocking_failure_count",
        "runtime.terminal_at",
    ):
        assert field in source
    assert '"sealed": ("committed", "sealed", "complete", "strict", True)' in source
    assert '"failed": ("failed", "failed", "failed", "provisional", False)' in source
    assert "_load_canonical_terminal_result" in source
    assert "if not terminal:" in source


def test_lineage_owner_excludes_valid_open_and_failed_but_rejects_malformed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event_id = "d29-lineage-owner"
    payload_hash = "a" * 64
    owner_epoch_id = 7

    def terminal_result(
        cursor: _LineageCursor,
        *,
        structural_event_id: str,
        payload_hash: str,
    ) -> SimpleNamespace:
        assert structural_event_id == event_id
        assert payload_hash == "a" * 64
        return SimpleNamespace(
            epoch_id=owner_epoch_id,
            replayed_outcome=M5ReplayedOutcome(str(cursor.row[18])),
        )

    monkeypatch.setattr(
        postgres_withdrawal,
        "_load_canonical_terminal_result",
        terminal_result,
    )
    open_row: tuple[object, ...] = (
        event_id,
        payload_hash,
        3,
        "committed",
        "pending",
        "pending",
        "provisional",
        None,
        event_id,
        "semantic_pending",
        3,
        1,
        1,
        0,
        None,
        None,
        None,
        None,
        None,
    )
    assert not _is_sealed_lineage_owner(  # type: ignore[arg-type]
        _LineageCursor(open_row), owner_epoch_id, owner_epoch_id
    )
    complete_row = (
        event_id,
        payload_hash,
        4,
        "committed",
        "complete",
        "complete",
        "provisional",
        None,
        event_id,
        "semantic_complete",
        4,
        0,
        0,
        0,
        None,
        None,
        None,
        None,
        None,
    )
    assert not _is_sealed_lineage_owner(  # type: ignore[arg-type]
        _LineageCursor(complete_row), owner_epoch_id, owner_epoch_id
    )
    failed_row = (
        event_id,
        payload_hash,
        4,
        "failed",
        "failed",
        "failed",
        "provisional",
        None,
        event_id,
        "failed",
        4,
        0,
        0,
        1,
        object(),
        event_id,
        payload_hash,
        owner_epoch_id,
        "failed",
    )
    assert not _is_sealed_lineage_owner(  # type: ignore[arg-type]
        _LineageCursor(failed_row), owner_epoch_id, owner_epoch_id
    )
    sealed_row = (
        event_id,
        payload_hash,
        5,
        "committed",
        "sealed",
        "complete",
        "strict",
        object(),
        event_id,
        "sealed",
        5,
        0,
        0,
        0,
        object(),
        event_id,
        payload_hash,
        owner_epoch_id,
        "sealed",
    )
    assert _is_sealed_lineage_owner(  # type: ignore[arg-type]
        _LineageCursor(sealed_row), owner_epoch_id, owner_epoch_id
    )
    malformed_rows: list[tuple[object, ...]] = []
    for index, value in (
        (6, "provisional"),
        (10, 6),
        (11, 1),
        (14, None),
    ):
        changed = list(sealed_row)
        changed[index] = value
        malformed_rows.append(tuple(changed))
    malformed_open = list(open_row)
    malformed_open[15] = event_id
    malformed_rows.append(tuple(malformed_open))
    malformed_failed = list(failed_row)
    malformed_failed[7] = object()
    malformed_rows.append(tuple(malformed_failed))
    malformed_complete = list(complete_row)
    malformed_complete[11] = 1
    malformed_rows.append(tuple(malformed_complete))
    malformed_failed_counter = list(failed_row)
    malformed_failed_counter[12] = 1
    malformed_rows.append(tuple(malformed_failed_counter))
    for malformed in malformed_rows:
        with pytest.raises(EventConflictError, match="owner"):
            _is_sealed_lineage_owner(  # type: ignore[arg-type]
                _LineageCursor(malformed), owner_epoch_id, owner_epoch_id
            )


def test_locked_derivation_uses_frozen_tier_sequence(
    function_source: Callable[[str], str],
) -> None:
    prepare = function_source("_prepare_locked_document_open")
    prepare_calls = (
        "_validated_structural_source_chunks",
        "_require_d30_read_committed",
        "_gather_d29_locator_authority",
        "_assert_zero_cancellation_authority",
        "_lock_d29_bootstrap_provenance",
    )
    prepare_positions = tuple(prepare.index(call) for call in prepare_calls)
    assert prepare_positions == tuple(sorted(prepare_positions))
    continuation = function_source("_continue_locked_document_open")
    continuation_calls = (
        "_validate_d29_event_snapshot_image",
        "_lock_d29_touched_membership",
        "_lock_d29_scopes_and_reserve",
        "_lock_d29_jobs_and_reserve",
        "_lock_d29_tier_10_authority",
        "_lock_d29_attempt_outputs",
        "_lock_d30_dynamic_owner_topology",
        "_lock_d29_candidate_source_closure",
        "_candidate_edges",
        "_lock_d29_verifier_provenance",
        "_lock_d29_observation_authority",
        "plan_requirement_withdrawal",
        "_locked_direct_withdrawal",
        "_require_coordinate_subset",
    )
    continuation_positions = tuple(
        continuation.index(call) for call in continuation_calls
    )
    assert continuation_positions == tuple(sorted(continuation_positions))
    wrapper = function_source("_derive_locked_document_open")
    assert wrapper.index("_prepare_locked_document_open") < wrapper.index(
        "_continue_locked_document_open"
    )
    for source in (prepare, continuation, wrapper):
        assert "_direct_withdrawal_preview" not in source
        assert "_bounded_requirement_plan" not in source
        assert "_observation_edges(" not in source
    assert "locator.predecessor_snapshots" in continuation
    assert "locator.requirement_currency_keys" in prepare
    locator_source = function_source("_gather_d29_locator_authority")
    assert locator_source.count("FROM groundloop_observation_currency") == 1
    observation_locator = locator_source.split("current_rows =", maxsplit=1)[1].split(
        "for subject_kind", maxsplit=1
    )[0]
    assert "WHERE chunk_version_id = %s" in observation_locator
    assert 'COLLATE "C" = %s' not in observation_locator
    tier_10 = function_source("_lock_d29_tier_10_authority")
    tier_10_relations = (
        "_lock_d29_direct_attempts",
        "_lock_d29_m5_attempt_and_output_rows",
        "groundloop_semantic_job_dependency",
        "_locate_d29_m5_dependency_edges",
        "_lock_d29_m5_dependency_edge",
        "groundloop_m4_discovery_result",
        "groundloop_m5_requirement_discovery_result",
        "groundloop_m5_requirement_channel_hit",
        "groundloop_m5_requirement_scope_selection",
        "groundloop_admitted_pair",
        "groundloop_m5_requirement_admitted_pair",
        "groundloop_m5_requirement_admitted_pair_source",
        "_d30_execution_coordinates",
        "groundloop_m5_requirement_verifier_execution",
        "groundloop_m5_requirement_verifier_artifact",
        "groundloop_m5_requirement_pair_input",
        "_lock_d29_direct_frontier",
    )
    relation_positions = tuple(tier_10.index(item) for item in tier_10_relations)
    assert relation_positions == tuple(sorted(relation_positions))


def test_document_locator_authority_rejects_copy_reuse_and_changed_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = object()
    event = SimpleNamespace(structural_event_id="event-1", payload_hash="a" * 64)
    execution_policy = ApplicationExecutionPolicy("b" * 64, "c" * 64, "d" * 64)
    binding = postgres_withdrawal._D29DocumentOpenBinding(
        cursor_object_identity=id(cursor),
        backend_identity=7,
        transaction_identity=11,
        session_role="groundloop",
        epoch_id=13,
        structural_event_id=event.structural_event_id,
        source_identity_hash=event.payload_hash,
        predecessor_epoch_id=12,
        source_chunks=("chunk-1",),
    )

    monkeypatch.setattr(postgres_withdrawal, "_document_event", lambda value: value)
    monkeypatch.setattr(
        postgres_withdrawal,
        "_capture_d29_document_open_binding",
        lambda current_cursor, *_args: replace(
            binding, cursor_object_identity=id(current_cursor)
        ),
    )

    def prepared_authority() -> postgres_withdrawal._PreparedDocumentOpen:
        prepared = postgres_withdrawal._PreparedDocumentOpen(
            binding=binding,
            prepared_identity=0,
            event=event,  # type: ignore[arg-type]
            execution_policy=execution_policy,
            closure=SimpleNamespace(),  # type: ignore[arg-type]
            source_chunks=("chunk-1",),
            locator=SimpleNamespace(),  # type: ignore[arg-type]
            located_observation_edges=(),
            bootstrap_authority=(),
        )
        prepared.prepared_identity = id(prepared)
        postgres_withdrawal._seal_prepared_document_open(prepared)
        return prepared

    prepared = prepared_authority()
    postgres_withdrawal._validate_prepared_document_open(
        cursor,  # type: ignore[arg-type]
        event,  # type: ignore[arg-type]
        prepared,
        execution_policy=execution_policy,
        phase=postgres_withdrawal._DocumentOpenPhase.LOCATORS_GATHERED,
    )

    with pytest.raises(ValidationError, match="copied|replaced"):
        postgres_withdrawal._validate_prepared_document_open(
            cursor,  # type: ignore[arg-type]
            event,  # type: ignore[arg-type]
            deepcopy(prepared),
            execution_policy=execution_policy,
            phase=postgres_withdrawal._DocumentOpenPhase.LOCATORS_GATHERED,
        )

    copied_snapshot = prepared_authority()
    copied_snapshot.authority_snapshot = deepcopy(copied_snapshot.authority_snapshot)
    with pytest.raises(ValidationError, match="snapshot was replaced"):
        postgres_withdrawal._validate_prepared_document_open(
            cursor,  # type: ignore[arg-type]
            event,  # type: ignore[arg-type]
            copied_snapshot,
            execution_policy=execution_policy,
            phase=postgres_withdrawal._DocumentOpenPhase.LOCATORS_GATHERED,
        )

    changed = prepared_authority()
    changed.source_chunks = ("chunk-2",)
    with pytest.raises(EventConflictError, match="locator authority changed"):
        postgres_withdrawal._validate_prepared_document_open(
            cursor,  # type: ignore[arg-type]
            event,  # type: ignore[arg-type]
            changed,
            execution_policy=execution_policy,
            phase=postgres_withdrawal._DocumentOpenPhase.LOCATORS_GATHERED,
        )

    wrong_event = SimpleNamespace(
        structural_event_id="event-2", payload_hash=event.payload_hash
    )
    with pytest.raises(ValidationError, match="phase or input changed"):
        postgres_withdrawal._validate_prepared_document_open(
            cursor,  # type: ignore[arg-type]
            wrong_event,  # type: ignore[arg-type]
            prepared_authority(),
            execution_policy=execution_policy,
            phase=postgres_withdrawal._DocumentOpenPhase.LOCATORS_GATHERED,
        )

    for field_name, changed_value in (
        ("backend_identity", binding.backend_identity + 1),
        ("transaction_identity", binding.transaction_identity + 1),
        ("session_role", f"{binding.session_role}-changed"),
    ):
        monkeypatch.setattr(
            postgres_withdrawal,
            "_capture_d29_document_open_binding",
            lambda *_args, field_name=field_name, changed_value=changed_value: replace(
                binding, **{field_name: changed_value}
            ),
        )
        with pytest.raises(ValidationError, match="changed transaction context"):
            postgres_withdrawal._validate_prepared_document_open(
                cursor,  # type: ignore[arg-type]
                event,  # type: ignore[arg-type]
                prepared_authority(),
                execution_policy=execution_policy,
                phase=postgres_withdrawal._DocumentOpenPhase.LOCATORS_GATHERED,
            )

    consumed = prepared_authority()
    consumed.phase = postgres_withdrawal._DocumentOpenPhase.CONSUMED
    with pytest.raises(ValidationError, match="phase or input changed"):
        postgres_withdrawal._validate_prepared_document_open(
            cursor,  # type: ignore[arg-type]
            event,  # type: ignore[arg-type]
            consumed,
            execution_policy=execution_policy,
            phase=postgres_withdrawal._DocumentOpenPhase.LOCATORS_GATHERED,
        )

    with pytest.raises(ValidationError, match="changed transaction context"):
        postgres_withdrawal._validate_prepared_document_open(
            object(),  # type: ignore[arg-type]
            event,  # type: ignore[arg-type]
            prepared_authority(),
            execution_policy=execution_policy,
            phase=postgres_withdrawal._DocumentOpenPhase.LOCATORS_GATHERED,
        )


def test_document_phase_cut_requires_exact_snapshot_before_tier_8_locks(
    function_source: Callable[[str], str],
) -> None:
    prepare = function_source("_prepare_locked_document_open")
    continuation = function_source("_continue_locked_document_open")
    snapshot = function_source("_validate_d29_event_snapshot_image")
    closure = function_source("_read_existing_document_closure")

    assert "allow_missing_event_snapshots=True" in prepare
    assert "_validate_d29_event_snapshot_image" not in prepare
    assert continuation.index("_validate_d29_event_snapshot_image") < (
        continuation.index("_lock_d29_touched_membership")
    )
    assert "touched_direct_chunk_ids" in continuation
    assert "locator.direct_observation_source_rows" in continuation
    assert "_gather_d29_locator_authority" not in continuation
    for relation in (
        "groundloop_m5_requirement_registry_snapshot",
        "groundloop_m5_requirement_registry_snapshot_member",
        "groundloop_m5_active_chunk_snapshot",
        "groundloop_m5_active_chunk_snapshot_member",
    ):
        assert relation in snapshot
    assert "allow_missing_event_snapshots: bool = False" in closure
    assert "not allow_missing_event_snapshots" in closure


def _fresh_active_member_snapshot(database: Any) -> ActiveChunkSnapshot:
    """Build one absent snapshot whose member names a real predecessor chunk."""

    connection = database.connection
    for entry in database.database.chunk_snapshot.entries:
        snapshot = ActiveChunkSnapshot.build((entry,))
        existing = connection.execute(
            """
            SELECT 1
            FROM groundloop_m5_active_chunk_snapshot
            WHERE active_chunk_snapshot_digest = %s
            """,
            (snapshot.active_chunk_snapshot_digest,),
        ).fetchone()
        if existing is None:
            return snapshot
    raise AssertionError("reservation fixture has no fresh active-member snapshot")


def _persist_d29_event_snapshots(
    cursor: Any,
    event: M5TypedEventPlan,
    *,
    epoch_id: int,
) -> None:
    _persist_requirement_snapshot(
        cursor,
        snapshot=event.requirement_registry_snapshot,
        epoch_id=epoch_id,
    )
    _persist_active_chunk_snapshot(
        cursor,
        snapshot=event.active_chunk_snapshot,
        epoch_id=epoch_id,
    )


def _assert_d29_event_snapshots_absent(
    cursor: Any,
    event: M5TypedEventPlan,
) -> None:
    assert (
        cursor.execute(
            """
            SELECT 1
            FROM groundloop_m5_requirement_registry_snapshot
            WHERE requirement_registry_snapshot_digest = %s
            """,
            (event.requirement_registry_snapshot.requirement_registry_snapshot_digest,),
        ).fetchone()
        is None
    )
    assert (
        cursor.execute(
            """
            SELECT 1
            FROM groundloop_m5_active_chunk_snapshot
            WHERE active_chunk_snapshot_digest = %s
            """,
            (event.active_chunk_snapshot.active_chunk_snapshot_digest,),
        ).fetchone()
        is None
    )


def _reservation_phase_case(
    d29_database: Any,
    monkeypatch: pytest.MonkeyPatch,
    *,
    tag: str,
    nonempty_active_snapshot: bool,
) -> tuple[
    M5TypedEventPlan,
    ApplicationExecutionPolicy,
    tuple[Any, Any, tuple[Any, ...], str],
    postgres_withdrawal._DocumentClosure,
    tuple[str, ...],
]:
    """Prepare the retained reservation fixture for the real two-phase route."""

    connection = d29_database.connection
    base = d29_database.database.base
    manifest = d29_database.database.manifest
    event = d29_database.document_plan("delete", tag=tag)
    if nonempty_active_snapshot:
        event = replace(
            event,
            active_chunk_snapshot=_fresh_active_member_snapshot(d29_database),
        )
    direct = event.direct_plan
    assert direct is not None
    source_chunks = tuple(sorted(direct.deactivated_chunk_version_ids))

    # The retained fixture's synthetic claim currents predate both valid D30
    # provenance branches.  This is the same narrow cleanup used by the
    # prospective-reservation test; D30 covers valid mixed claim/requirement
    # location separately.
    connection.execute(
        """
        DELETE FROM groundloop_observation_currency
        WHERE subject_kind = 'claim' AND chunk_version_id = ANY(%s)
        """,
        (list(source_chunks),),
    )
    execution_policy = ApplicationExecutionPolicy(
        "a" * 64,
        "b" * 64,
        manifest.verifier_execution_spec_hash,
    )
    qualifying_owner = int(str(d29_database.first_m5["epoch_id"]))
    excluded_owner = int(str(d29_database.excluded_m5["epoch_id"]))
    assert qualifying_owner != excluded_owner
    monkeypatch.setattr(
        postgres_withdrawal,
        "_is_sealed_lineage_owner",
        lambda _cursor, owner, _predecessor: owner == qualifying_owner,
    )

    with connection.cursor() as cursor:
        requirement_withdrawal = _plan_document_requirement_withdrawal(cursor, event)
        direct_open = _preview_document_direct_open(
            cursor,
            event,
            execution_policy=execution_policy,
        )
    requirement_roots = postgres_withdrawal._document_requirement_roots(
        event,
        manifest,
        requirement_withdrawal,
    )
    root_set_hash = runtime_digests.requirement_root_set_digest(
        root.job.logical_job_id for root in requirement_roots
    )
    proposal = (
        direct_open,
        requirement_withdrawal,
        requirement_roots,
        root_set_hash,
    )
    manifest_arrays = postgres_withdrawal._validate_document_manifest(
        postgres_withdrawal._document_declaration_manifest(direct_open)
    )
    closure = postgres_withdrawal._DocumentClosure(
        epoch_id=qualifying_owner,
        epoch_revision=1,
        update_kind="document_delete",
        previous_epoch_id=base.epoch_id,
        runtime_revision=1,
        runtime_state="structural_committed",
        open_work_count=0,
        open_scope_count=0,
        blocking_failure_count=0,
        candidate_policy=manifest,
        requirement_snapshot_digest=(
            event.requirement_registry_snapshot.requirement_registry_snapshot_digest
        ),
        chunk_snapshot_digest=(
            event.active_chunk_snapshot.active_chunk_snapshot_digest
        ),
        requirement_root_set_hash=root_set_hash,
        direct_root_job_ids=manifest_arrays[0],
        direct_scope_root_job_ids=manifest_arrays[1],
        direct_fallback_claim_ids=manifest_arrays[2],
    )
    monkeypatch.setattr(
        postgres_withdrawal,
        "_read_existing_document_closure",
        lambda *_args, **_kwargs: closure,
    )
    monkeypatch.setattr(
        postgres_withdrawal,
        "_validated_structural_source_chunks",
        lambda *_args, **_kwargs: source_chunks,
    )
    return event, execution_policy, proposal, closure, source_chunks


def _d29_reservation_row_image(
    cursor: Any, proposal: tuple[Any, ...]
) -> tuple[Any, ...]:
    coordinates = postgres_withdrawal._document_declaration_coordinates(
        proposal[0], proposal[2]
    )
    images: list[tuple[str, tuple[Any, ...]]] = []
    for label, relation, identifier, values in (
        (
            "direct-job",
            "groundloop_semantic_job",
            "job_id",
            coordinates.direct_job_ids,
        ),
        (
            "direct-scope",
            "groundloop_discovery_scope",
            "root_job_id",
            coordinates.direct_scope_root_job_ids,
        ),
        (
            "requirement-job",
            "groundloop_m5_semantic_job",
            "logical_job_id",
            coordinates.requirement_job_ids,
        ),
        (
            "requirement-scope",
            "groundloop_m5_discovery_scope",
            "root_job_id",
            coordinates.requirement_scope_root_job_ids,
        ),
    ):
        rows = cursor.execute(
            f"SELECT to_jsonb(item) FROM {relation} AS item "
            f'WHERE {identifier} = ANY(%s) ORDER BY {identifier} COLLATE "C"',
            (list(values),),
        ).fetchall()
        images.append((label, tuple(row[0] for row in rows)))
    return tuple(images)


def test_live_split_allowed_snapshot_step_and_wrapper_are_byte_equivalent(
    d29_reservation_database: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = d29_reservation_database
    connection = database.connection
    event, execution_policy, proposal, closure, _source_chunks = (
        _reservation_phase_case(
            database,
            monkeypatch,
            tag="real-phase-equivalence",
            nonempty_active_snapshot=False,
        )
    )

    def run_route(*, split: bool) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
        name = "d29_live_split" if split else "d29_live_wrapper"
        connection.execute(f"SAVEPOINT {name}")
        try:
            with connection.cursor() as cursor:
                _assert_d29_event_snapshots_absent(cursor, event)
                before_rows = _d29_reservation_row_image(cursor, proposal)
                if split:
                    prepared = postgres_withdrawal._prepare_locked_document_open(
                        cursor,
                        event,
                        execution_policy=execution_policy,
                    )
                    assert (
                        prepared.phase
                        is postgres_withdrawal._DocumentOpenPhase.LOCATORS_GATHERED
                    )
                    _assert_d29_event_snapshots_absent(cursor, event)
                    _persist_d29_event_snapshots(
                        cursor,
                        event,
                        epoch_id=closure.epoch_id,
                    )
                    result = postgres_withdrawal._continue_locked_document_open(
                        cursor,
                        event,
                        prepared,
                        execution_policy=execution_policy,
                        supplied_direct_open=proposal[0],
                        supplied_requirement_withdrawal=proposal[1],
                        supplied_requirement_roots=proposal[2],
                        supplied_requirement_root_set_hash=proposal[3],
                    )
                    output = (
                        result.direct_open,
                        result.requirement_withdrawal,
                        result.requirement_roots,
                        result.requirement_root_set_hash,
                    )
                else:
                    _persist_d29_event_snapshots(
                        cursor,
                        event,
                        epoch_id=closure.epoch_id,
                    )
                    output = postgres_withdrawal._derive_locked_document_open(
                        cursor,
                        event,
                        execution_policy=execution_policy,
                        supplied_direct_open=proposal[0],
                        supplied_requirement_withdrawal=proposal[1],
                        supplied_requirement_roots=proposal[2],
                        supplied_requirement_root_set_hash=proposal[3],
                    )
                after_rows = _d29_reservation_row_image(cursor, proposal)
            assert after_rows == before_rows
            return output, after_rows
        finally:
            connection.execute(f"ROLLBACK TO SAVEPOINT {name}")
            connection.execute(f"RELEASE SAVEPOINT {name}")

    wrapper_output, wrapper_rows = run_route(split=False)
    split_output, split_rows = run_route(split=True)
    assert wrapper_output == split_output == proposal
    assert wrapper_rows == split_rows
    with connection.cursor() as cursor:
        _assert_d29_event_snapshots_absent(cursor, event)


class _Tier8QueryProbe:
    """Record the first tier-8 membership query without replacing the phase."""

    def __init__(self, cursor: Any) -> None:
        self.cursor = cursor
        self.tier_8_queries: list[str] = []

    def execute(self, statement: Any, *args: Any, **kwargs: Any) -> Any:
        source = str(statement)
        if (
            "WITH member_point AS MATERIALIZED" in source
            and "FOR KEY SHARE OF member" in source
        ):
            self.tier_8_queries.append(source)
        return self.cursor.execute(statement, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.cursor, name)


def test_live_prepared_authority_cannot_cross_a_real_rollback(
    d29_database: Any,
) -> None:
    connection = d29_database.connection
    event = d29_database.document_plan("delete", tag="real-rollback-reuse")
    execution_policy = ApplicationExecutionPolicy(
        "a" * 64,
        "b" * 64,
        d29_database.database.manifest.verifier_execution_spec_hash,
    )
    epoch_row = connection.execute(
        "SELECT max(epoch_id) FROM groundloop_epoch"
    ).fetchone()
    assert epoch_row is not None and epoch_row[0] is not None
    closure = SimpleNamespace(
        epoch_id=int(epoch_row[0]),
        previous_epoch_id=d29_database.database.base.epoch_id,
    )
    source_chunks = tuple(sorted(d29_database.database.base.chunk_ids))
    with connection.cursor() as cursor:
        binding = postgres_withdrawal._capture_d29_document_open_binding(
            cursor,
            event,
            closure,  # type: ignore[arg-type]
            source_chunks,
        )
        prepared = postgres_withdrawal._PreparedDocumentOpen(
            binding=binding,
            prepared_identity=0,
            event=event,
            execution_policy=execution_policy,
            closure=closure,  # type: ignore[arg-type]
            source_chunks=source_chunks,
            locator=SimpleNamespace(),  # type: ignore[arg-type]
            located_observation_edges=(),
            bootstrap_authority=(),
        )
        prepared.prepared_identity = id(prepared)
        postgres_withdrawal._seal_prepared_document_open(prepared)
        postgres_withdrawal._validate_prepared_document_open(
            cursor,
            event,
            prepared,
            execution_policy=execution_policy,
            phase=postgres_withdrawal._DocumentOpenPhase.LOCATORS_GATHERED,
        )

        connection.rollback()
        cursor.execute("SELECT 1")
        with pytest.raises(ValidationError, match="changed transaction context"):
            postgres_withdrawal._validate_prepared_document_open(
                cursor,
                event,
                prepared,
                execution_policy=execution_policy,
                phase=postgres_withdrawal._DocumentOpenPhase.LOCATORS_GATHERED,
            )
    connection.rollback()


@pytest.mark.parametrize(
    "corruption",
    (
        "requirement-header",
        "active-chunk-header",
        "requirement-member",
        "active-chunk-member",
    ),
)
def test_live_new_event_snapshot_conflicts_before_tier_8(
    d29_reservation_database: Any,
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
) -> None:
    database = d29_reservation_database
    connection = database.connection
    event, execution_policy, proposal, closure, _source_chunks = (
        _reservation_phase_case(
            database,
            monkeypatch,
            tag=f"real-snapshot-{corruption}",
            nonempty_active_snapshot=True,
        )
    )
    later_owner = int(str(database.excluded_m5["epoch_id"]))
    assert later_owner > closure.epoch_id
    savepoint = f"d29_snapshot_{corruption.replace('-', '_')}"
    connection.execute(f"SAVEPOINT {savepoint}")
    try:
        with connection.cursor() as raw_cursor:
            cursor = _Tier8QueryProbe(raw_cursor)
            _assert_d29_event_snapshots_absent(cursor, event)
            prepared = postgres_withdrawal._prepare_locked_document_open(
                cursor,  # type: ignore[arg-type]
                event,
                execution_policy=execution_policy,
            )
            assert (
                prepared.phase
                is postgres_withdrawal._DocumentOpenPhase.LOCATORS_GATHERED
            )
            _assert_d29_event_snapshots_absent(cursor, event)
            _persist_d29_event_snapshots(
                cursor,
                event,
                epoch_id=closure.epoch_id,
            )
            cursor.execute("SET LOCAL session_replication_role = 'replica'")
            try:
                if corruption == "requirement-header":
                    changed = cursor.execute(
                        """
                        UPDATE groundloop_m5_requirement_registry_snapshot
                        SET created_epoch_id = %s
                        WHERE requirement_registry_snapshot_digest = %s
                        """,
                        (
                            later_owner,
                            event.requirement_registry_snapshot.requirement_registry_snapshot_digest,
                        ),
                    ).rowcount
                elif corruption == "active-chunk-header":
                    changed = cursor.execute(
                        """
                        UPDATE groundloop_m5_active_chunk_snapshot
                        SET created_epoch_id = %s
                        WHERE active_chunk_snapshot_digest = %s
                        """,
                        (
                            later_owner,
                            event.active_chunk_snapshot.active_chunk_snapshot_digest,
                        ),
                    ).rowcount
                elif corruption == "requirement-member":
                    changed = cursor.execute(
                        """
                        UPDATE groundloop_m5_requirement_registry_snapshot_member
                        SET normalized_requirement_text =
                                normalized_requirement_text || ' corrupted',
                            requirement_text_hash = encode(
                                digest(
                                    convert_to(
                                        normalized_requirement_text || ' corrupted',
                                        'UTF8'
                                    ),
                                    'sha256'
                                ),
                                'hex'
                            )
                        WHERE requirement_registry_snapshot_digest = %s
                          AND member_ordinal = 0
                        """,
                        (
                            event.requirement_registry_snapshot.requirement_registry_snapshot_digest,
                        ),
                    ).rowcount
                else:
                    assert corruption == "active-chunk-member"
                    entry = event.active_chunk_snapshot.entries[0]
                    changed_hash = "0" * 64 if entry.text_hash != "0" * 64 else "1" * 64
                    changed = cursor.execute(
                        """
                        UPDATE groundloop_m5_active_chunk_snapshot_member
                        SET text_hash = %s
                        WHERE active_chunk_snapshot_digest = %s
                          AND member_ordinal = 0
                        """,
                        (
                            changed_hash,
                            event.active_chunk_snapshot.active_chunk_snapshot_digest,
                        ),
                    ).rowcount
            finally:
                cursor.execute("SET LOCAL session_replication_role = 'origin'")
            assert changed == 1

            cursor.tier_8_queries.clear()
            message = {
                "requirement-header": "D29 requirement snapshot header changed",
                "active-chunk-header": "D29 active-chunk snapshot header changed",
                "requirement-member": "D29 requirement snapshot members changed",
                "active-chunk-member": "D29 active-chunk snapshot members changed",
            }[corruption]
            with pytest.raises(EventConflictError, match=message):
                postgres_withdrawal._continue_locked_document_open(
                    cursor,  # type: ignore[arg-type]
                    event,
                    prepared,
                    execution_policy=execution_policy,
                    supplied_direct_open=proposal[0],
                    supplied_requirement_withdrawal=proposal[1],
                    supplied_requirement_roots=proposal[2],
                    supplied_requirement_root_set_hash=proposal[3],
                )
            assert cursor.tier_8_queries == []
            assert (
                prepared.phase
                is postgres_withdrawal._DocumentOpenPhase.LOCATORS_GATHERED
            )
    finally:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")


def test_direct_state_locator_uses_m4_distinct_text_hash_authority(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_gather_d29_direct_state_locators")

    for relation in (
        "groundloop_published_claim_state",
        "groundloop_claim_state_materialized",
        "groundloop_claim_certificate",
        "groundloop_published_answer_state",
        "groundloop_observation_currency",
        "groundloop_chunk_version",
        "groundloop_document_version",
    ):
        assert relation in source
    assert "WHERE subject_kind = %s AND subject_id = %s" in source
    assert "D29 remaining direct observation is not current" in source
    assert "normalized_text_hash(str(row[1]))" in source
    assert "normalized_text_hash_v1" not in source
    assert "text_hash_by_observation" in source
    assert "D29 direct claim distinct-evidence count changed" in source

    locked = function_source("_lock_d29_direct_claim_before_images")
    for relation in (
        "groundloop_published_claim_state",
        "groundloop_claim_state_materialized",
        "groundloop_claim_certificate",
    ):
        assert relation in locked
    assert locked.count("FOR UPDATE") == 3
    assert "materialized_updated_epoch != image.state_valid_from_epoch" in locked
    assert (
        "image.certificate_repaired_revision"
        "\n            != image.materialized_updated_revision"
    ) in locked
    tier_11a = function_source("_lock_d29_observation_authority")
    assert "_lock_d29_direct_claim_before_images" not in tier_11a
    for relation in (
        "groundloop_published_claim_state",
        "groundloop_claim_state_materialized",
        "groundloop_claim_certificate",
    ):
        assert relation not in tier_11a


def _one_direct_claim_before_image(
    d29_database: Any,
) -> tuple[postgres_withdrawal._D29DirectClaimBeforeImage, ...]:
    connection = d29_database.connection
    row = connection.execute(
        """
        SELECT currency.subject_kind::text, currency.subject_id,
               currency.chunk_version_id, currency.task_type,
               currency.observation_id, currency.installed_revision
        FROM groundloop_observation_currency AS currency
        WHERE currency.subject_kind = 'claim'
        ORDER BY currency.installed_revision DESC,
                 currency.subject_id COLLATE "C",
                 currency.chunk_version_id COLLATE "C",
                 currency.task_type COLLATE "C",
                 currency.observation_id COLLATE "C"
        LIMIT 1
        """
    ).fetchone()
    assert row is not None
    currency = postgres_withdrawal._D30CurrencyRow(
        subject_kind=str(row[0]),
        subject_id=str(row[1]),
        chunk_version_id=str(row[2]),
        task_type=str(row[3]),
        observation_id=str(row[4]),
        installed_revision=int(row[5]),
    )
    with connection.cursor() as cursor:
        observation_row = postgres_withdrawal._d30_observation_row(
            cursor, currency.observation_id
        )
        authority = SimpleNamespace(
            currency_rows=(currency,),
            dynamic=(),
            bootstrap=(
                SimpleNamespace(
                    currency=currency,
                    observation_row=observation_row,
                ),
            ),
        )
        claim_images, _answers, _remaining, _sources, _currency = (
            postgres_withdrawal._gather_d29_direct_state_locators(
                cursor,
                authority,  # type: ignore[arg-type]
                predecessor_epoch_id=d29_database.database.base.epoch_id,
            )
        )
    assert len(claim_images) == 1
    return claim_images


def _one_direct_answer_before_image(
    d29_database: Any,
) -> tuple[postgres_withdrawal._D29DirectAnswerBeforeImage, ...]:
    claim = _one_direct_claim_before_image(d29_database)[0]
    row = d29_database.connection.execute(
        """
        SELECT required_claim_count, supported_count, unsupported_count,
               refuted_count, conflicted_count, status
        FROM groundloop_published_answer_state
        WHERE answer_version_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (
            claim.answer_version_id,
            d29_database.database.base.epoch_id,
            d29_database.database.base.epoch_id,
        ),
    ).fetchone()
    assert row is not None
    return (
        postgres_withdrawal._D29DirectAnswerBeforeImage(
            answer_version_id=claim.answer_version_id,
            required_claim_count=int(row[0]),
            supported_count=int(row[1]),
            unsupported_count=int(row[2]),
            refuted_count=int(row[3]),
            conflicted_count=int(row[4]),
            status=str(row[5]),
        ),
    )


def test_live_direct_answer_complete_before_image_fails_closed(
    d29_database: Any,
) -> None:
    connection = d29_database.connection
    answer_images = _one_direct_answer_before_image(d29_database)
    image = answer_images[0]
    savepoint = "d29_direct_answer_complete_before_image"
    connection.execute(f"SAVEPOINT {savepoint}")
    try:
        if image.supported_count > 0:
            supported_delta, unsupported_delta = -1, 1
        else:
            assert image.unsupported_count > 0
            supported_delta, unsupported_delta = 1, -1
        assert (
            connection.execute(
                """
                UPDATE groundloop_published_answer_state
                SET supported_count = supported_count + %s,
                    unsupported_count = unsupported_count + %s
                WHERE answer_version_id = %s AND valid_from_epoch <= %s
                  AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
                """,
                (
                    supported_delta,
                    unsupported_delta,
                    image.answer_version_id,
                    d29_database.database.base.epoch_id,
                    d29_database.database.base.epoch_id,
                ),
            ).rowcount
            == 1
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="answer point changed"):
                postgres_withdrawal._lock_d29_direct_answer_before_images(
                    cursor,
                    answer_images,
                    predecessor_epoch_id=d29_database.database.base.epoch_id,
                )
    finally:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")


@pytest.mark.parametrize(
    ("corruption", "revalidate_existing"),
    (
        ("missing", False),
        ("wrong-support-id", False),
        ("wrong-refute-id", True),
        ("wrong-repaired-epoch", False),
        ("wrong-repaired-revision", True),
        ("stale-materialized-coordinate", True),
    ),
)
def test_live_direct_claim_certificate_closure_fails_closed(
    d29_database: Any,
    corruption: str,
    revalidate_existing: bool,
) -> None:
    connection = d29_database.connection
    claim_images = _one_direct_claim_before_image(d29_database)
    image = claim_images[0]
    savepoint = f"d29_claim_certificate_{corruption.replace('-', '_')}"
    connection.execute(f"SAVEPOINT {savepoint}")
    try:
        if corruption == "missing":
            assert (
                connection.execute(
                    "DELETE FROM groundloop_claim_certificate WHERE claim_id = %s",
                    (image.claim_id,),
                ).rowcount
                == 1
            )
        elif corruption in {"wrong-support-id", "wrong-refute-id"}:
            named = (
                image.certificate_support_observation_id
                if corruption == "wrong-support-id"
                else image.certificate_refute_observation_id
            )
            alternate = connection.execute(
                """
                SELECT observation_id FROM groundloop_semantic_observation
                WHERE observation_id <> COALESCE(%s, '')
                ORDER BY observation_id COLLATE "C"
                LIMIT 1
                """,
                (named,),
            ).fetchone()
            assert alternate is not None
            column = (
                "support_observation_id"
                if corruption == "wrong-support-id"
                else "refute_observation_id"
            )
            connection.execute(
                f"UPDATE groundloop_claim_certificate SET {column} = %s "
                "WHERE claim_id = %s",
                (str(alternate[0]), image.claim_id),
            )
        elif corruption == "wrong-repaired-epoch":
            alternate_epoch = connection.execute(
                """
                SELECT epoch_id FROM groundloop_epoch
                WHERE epoch_id <> %s
                ORDER BY epoch_id
                LIMIT 1
                """,
                (image.certificate_repaired_epoch,),
            ).fetchone()
            assert alternate_epoch is not None
            connection.execute(
                """
                UPDATE groundloop_claim_certificate SET repaired_epoch = %s
                WHERE claim_id = %s
                """,
                (int(alternate_epoch[0]), image.claim_id),
            )
        elif corruption == "wrong-repaired-revision":
            connection.execute(
                """
                UPDATE groundloop_claim_certificate
                SET repaired_revision = repaired_revision + 1
                WHERE claim_id = %s
                """,
                (image.claim_id,),
            )
        else:
            assert corruption == "stale-materialized-coordinate"
            connection.execute(
                """
                UPDATE groundloop_claim_state_materialized
                SET updated_revision = updated_revision + 1
                WHERE claim_id = %s
                """,
                (image.claim_id,),
            )

        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="certificate"):
                if revalidate_existing:
                    postgres_withdrawal._lock_d29_direct_claim_before_images(
                        cursor,
                        claim_images,
                        predecessor_epoch_id=d29_database.database.base.epoch_id,
                    )
                else:
                    _one_direct_claim_before_image(d29_database)
    finally:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")


def test_direct_verifier_requires_exact_parent_scope_topology(
    function_source: Callable[[str], str],
) -> None:
    topology = function_source("_validate_d30_owner_topology_from_held_rows")
    assert "D30 direct owner has an orphan or grandchild" in topology
    assert "spec.kind is not M4JobKind.VERIFY_PAIR" in topology
    assert "parent.kind" in topology
    scopes = function_source("_lock_d29_scopes_and_reserve")
    for field in (
        "scope.epoch_id",
        "scope.registry_snapshot_id",
        "scope.scope_kind",
        "scope.explicit_claim_ids",
        "scope.closed_revision",
        "update.manifest",
        'runtime["scope_claim_ids"]',
    ):
        assert field in scopes
    assert "direct child/frontier job owns a scope" in scopes
    closure = function_source("_lock_d29_direct_job_closure")
    assert "direct locator has a parentless verifier" in closure
    assert "direct verifier parent topology changed" in closure
    assert "direct root/scope bijection changed" in closure
    assert "direct verifier escaped root scope" in closure


def test_absent_preview_cannot_be_reused_as_an_authoritative_lock_route(
    function_source: Callable[[str], str],
) -> None:
    assert not hasattr(postgres_withdrawal, "_recompute_locked_requirement_withdrawal")
    source = function_source("_bounded_requirement_plan")
    assert "lock_authority:" not in source.split('"""', maxsplit=1)[0]
    assert "lock_authority=True" not in source
    assert "source_closure" not in source
    assert "_validated_structural_source_chunks" not in source
    assert source.count("lock_authority=False") == 2


def test_tier_11a_locks_all_observations_before_currency(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_lock_d29_observation_authority")
    semantic_lock = source.index("FROM groundloop_semantic_observation")
    requirement_currency = source.index("FROM groundloop_observation_currency")
    published_currency = source.index("FROM groundloop_published_observation_currency")
    assert semantic_lock < requirement_currency < published_currency
    assert "valid_to_epoch IS NULL" in source
    assert "not set(remaining_typed_keys) <= set(currency_typed_keys)" in source
    assert "D29 direct observation currency partition changed" in source


def test_tier_11a_preserves_the_retained_empty_partition_authority(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_lock_d29_observation_authority")
    retained = source.index("retained_unpartitioned_direct_state = not any(")
    fallback = source.index(
        "direct_currency_typed_keys\n        if retained_unpartitioned_direct_state"
    )
    source_closure = source.index("not retained_unpartitioned_direct_state")
    assert retained < fallback < source_closure
    for field in (
        "locator.direct_currency_keys",
        "locator.direct_claim_before_images",
        "locator.direct_answer_before_images",
        "locator.direct_remaining_observation_rows",
        "locator.direct_observation_source_rows",
    ):
        assert field in source[retained:fallback]


@pytest.mark.parametrize("corruption", ("claim", "chunk", "task", "duplicate"))
def test_tier_11a_rejects_nonexact_withdrawn_coordinates_before_sql(
    corruption: str,
) -> None:
    currency = postgres_withdrawal._D30CurrencyRow(
        subject_kind="claim",
        subject_id="claim-a",
        chunk_version_id="chunk-a",
        task_type="verify-pair",
        observation_id="observation-a",
        installed_revision=0,
    )
    direct_key = (
        currency.subject_id,
        currency.chunk_version_id,
        currency.task_type,
        currency.observation_id,
    )
    if corruption == "claim":
        direct_keys = (("claim-b", *direct_key[1:]),)
    elif corruption == "chunk":
        direct_keys = ((direct_key[0], "chunk-b", *direct_key[2:]),)
    elif corruption == "task":
        direct_keys = ((*direct_key[:2], "other-task", direct_key[3]),)
    else:
        assert corruption == "duplicate"
        direct_keys = (direct_key, direct_key)
    locator = SimpleNamespace(
        requirement_currency_keys=(),
        direct_currency_keys=direct_keys,
        direct_claim_before_images=(),
        direct_answer_before_images=(),
        direct_remaining_observation_rows=(),
        direct_observation_source_rows=(),
        d30_claims=SimpleNamespace(currency_rows=(currency,)),
    )
    with pytest.raises(
        EventConflictError,
        match="D29 direct observation currency partition changed",
    ):
        postgres_withdrawal._lock_d29_observation_authority(
            object(),  # type: ignore[arg-type]
            locator,  # type: ignore[arg-type]
            predecessor_epoch_id=1,
            candidate_policy_id="policy-a",
            verifier_authority={},
            bootstrap_authority={},
            d30_dynamic_authority={},
        )


def test_tier_8_membership_point_validates_immutable_core_rows(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_lock_d29_touched_membership")
    for relation in (
        "groundloop_m5_requirement_registry_snapshot_member",
        "groundloop_m5_requirement_version",
        "groundloop_m5_group_version",
        "groundloop_m5_group_family",
        "groundloop_m5_group_validity",
        "groundloop_m5_active_chunk_snapshot_member",
        "groundloop_chunk_version",
        "groundloop_document_version",
        "groundloop_epoch AS creator",
    ):
        assert relation in source
    assert "member.requirement_text_hash" in source
    assert "requirement.requirement_text_hash" in source
    assert "member.text_hash" in source
    assert "chunk.text_hash" in source
    assert "chunk.text" in source
    assert "chunk.chunk_index" in source
    assert "normalized_text_hash_v1" in source
    assert "normalize_text_v1" in source
    assert "ChunkVersion(" in source
    assert "FOR KEY SHARE OF member" in source
    assert "FOR KEY SHARE OF requirement, group_version" in source
    assert "FOR KEY SHARE OF member" in source
    assert "FOR KEY SHARE OF requirement, group_version, family, validity" in source
    assert source.count("FOR KEY SHARE OF chunk, version") == 2


def test_zero_cancellation_probes_cover_direct_and_m5_authority(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_assert_zero_cancellation_authority")
    for relation in (
        "groundloop_semantic_job",
        "groundloop_discovery_scope",
        "groundloop_m5_semantic_job",
        "groundloop_m5_discovery_scope",
        "groundloop_m5_owner_pending_counter",
        "groundloop_m5_answer_pending_counter",
    ):
        assert relation in source
    assert source.count("LIMIT 1") >= 6


def test_live_zero_cancellation_accepts_sealed_base_and_rejects_open_authority(
    d29_database: Any,
) -> None:
    with d29_database.connection.cursor() as cursor:
        _assert_zero_cancellation_authority(cursor, d29_database.database.base.epoch_id)
        with pytest.raises(EventConflictError, match="nonterminal authority"):
            _assert_zero_cancellation_authority(
                cursor, int(d29_database.first_m4["epoch_id"])
            )
        with pytest.raises(EventConflictError, match="nonterminal authority"):
            _assert_zero_cancellation_authority(
                cursor, int(d29_database.first_m5["epoch_id"])
            )


def test_live_verifier_observation_requires_complete_provenance_and_attempt_result(
    d29_verifier_database: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = d29_verifier_database.connection
    locator = SimpleNamespace(
        verifier_observation_ids=(d29_verifier_database.observation_id,),
        verifier_job_ids=(d29_verifier_database.job_id,),
    )
    with connection.cursor() as cursor:
        _lock_d29_attempt_outputs(cursor, locator)  # type: ignore[arg-type]
        authority = _lock_d29_verifier_provenance(  # type: ignore[arg-type]
            cursor, locator
        )
    assert tuple(authority) == (d29_verifier_database.observation_id,)
    assert (
        authority[d29_verifier_database.observation_id].candidate_policy_id
        == d29_verifier_database.candidate_policy_id
    )

    monkeypatch.setattr(
        postgres_withdrawal,
        "_is_sealed_lineage_owner",
        lambda *_args, **_kwargs: True,
    )
    with connection.cursor() as cursor:
        edges = _observation_edges(
            cursor,
            chunks=(d29_verifier_database.chunk_id,),
            predecessor_epoch_id=d29_verifier_database.epoch_id,
            snapshots=(
                d29_verifier_database.requirement_snapshot_digest,
                d29_verifier_database.chunk_snapshot_digest,
            ),
            candidate_policy_id=d29_verifier_database.candidate_policy_id,
            lock_authority=False,
        )
    assert len(edges) == 1
    assert edges[0].observation_id == d29_verifier_database.observation_id

    connection.execute("SAVEPOINT d29_corrupt_verifier_pair_input")
    try:
        connection.execute(
            "ALTER TABLE groundloop_m5_requirement_pair_input "
            "DISABLE TRIGGER groundloop_m5_pair_input_immutable"
        )
        connection.execute(
            """
            UPDATE groundloop_m5_requirement_pair_input AS pair_input
            SET requirement_ordinal = requirement_ordinal + 1
            FROM groundloop_m5_requirement_verifier_execution AS execution
            WHERE execution.observation_id = %s
              AND pair_input.pair_input_hash = execution.pair_input_hash
            """,
            (d29_verifier_database.observation_id,),
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="artifact/input digest"):
                _lock_d29_verifier_provenance(  # type: ignore[arg-type]
                    cursor, locator
                )
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_corrupt_verifier_pair_input")
        connection.execute("RELEASE SAVEPOINT d29_corrupt_verifier_pair_input")

    connection.execute("SAVEPOINT d29_corrupt_verifier_attempt_result")
    try:
        connection.execute(
            "ALTER TABLE groundloop_m5_attempt_result_artifact "
            "DISABLE TRIGGER groundloop_m5_attempt_result_immutable"
        )
        connection.execute(
            """
            UPDATE groundloop_m5_attempt_result_artifact AS result
            SET result_artifact_hash = %s
            FROM groundloop_m5_requirement_verifier_execution AS execution
            WHERE execution.observation_id = %s
              AND result.attempt_id = execution.attempt_id
            """,
            ("f" * 64, d29_verifier_database.observation_id),
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="attempt/result history"):
                _lock_d29_attempt_outputs(cursor, locator)  # type: ignore[arg-type]
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_corrupt_verifier_attempt_result")
        connection.execute("RELEASE SAVEPOINT d29_corrupt_verifier_attempt_result")

    connection.execute("SAVEPOINT d29_corrupt_verifier_attempt_identity")
    try:
        connection.execute("ALTER TABLE groundloop_m5_job_attempt DISABLE TRIGGER USER")
        connection.execute(
            """
            UPDATE groundloop_m5_job_attempt AS attempt
            SET attempt_ordinal = attempt_ordinal + 100
            FROM groundloop_m5_requirement_verifier_execution AS execution
            WHERE execution.observation_id = %s
              AND attempt.attempt_id = execution.attempt_id
            """,
            (d29_verifier_database.observation_id,),
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="attempt/result history"):
                _lock_d29_attempt_outputs(cursor, locator)  # type: ignore[arg-type]
    finally:
        connection.execute(
            "ROLLBACK TO SAVEPOINT d29_corrupt_verifier_attempt_identity"
        )
        connection.execute("RELEASE SAVEPOINT d29_corrupt_verifier_attempt_identity")

    output_source = connection.execute(
        """
        SELECT attempt.attempt_id, attempt.logical_job_id,
               attempt.attempt_ordinal, attempt.execution_spec_hash,
               attempt.lease_token_hash, attempt.lease_expires_at,
               attempt.attempt_work_digest, result.job_epoch_id,
               result.result_artifact_id, result.result_artifact_hash,
               result.activity_snapshot_epoch_id,
               result.activity_snapshot_revision
        FROM groundloop_m5_requirement_verifier_execution AS execution
        JOIN groundloop_m5_job_attempt AS attempt
          ON attempt.attempt_id = execution.attempt_id
        JOIN groundloop_m5_attempt_result_artifact AS result
          ON result.attempt_id = attempt.attempt_id
        WHERE execution.observation_id = %s
        """,
        (d29_verifier_database.observation_id,),
    ).fetchone()
    assert output_source is not None
    checked_attempt = M5JobAttempt(
        attempt_id=str(output_source[0]).strip(),
        logical_job_id=str(output_source[1]).strip(),
        attempt_ordinal=int(output_source[2]),
        execution_spec_hash=str(output_source[3]).strip(),
        lease_token_hash=str(output_source[4]).strip(),
        lease_expires_at=output_source[5],
        attempt_work_digest=str(output_source[6]).strip(),
    )
    altered_output = M5AttemptOutput.build(
        attempt=checked_attempt,
        job_epoch_id=int(output_source[7]),
        payload_hash="e" * 64,
        result_artifact_id=str(output_source[8]).strip(),
        result_artifact_hash=str(output_source[9]).strip(),
    )
    altered_result = M5AttemptResultArtifact.build(
        attempt_output=altered_output,
        job_state_at_receipt=M5JobState.RUNNING,
        job_state_after=M5JobState.COMPLETED_ACTIVE,
        disposition=M5AttemptDisposition.VERIFIER_COMPLETED_ACTIVE,
        activity_snapshot_epoch_id=int(output_source[10]),
        activity_snapshot_revision=int(output_source[11]),
        epoch_active=True,
        chunk_active=True,
        requirement_active=True,
        group_active=True,
        archive_reason=None,
    )
    connection.execute("SAVEPOINT d29_corrupt_verifier_output_payload")
    try:
        connection.execute("ALTER TABLE groundloop_m5_job_attempt DISABLE TRIGGER USER")
        connection.execute(
            "ALTER TABLE groundloop_m5_attempt_result_artifact DISABLE TRIGGER USER"
        )
        connection.execute(
            """
            UPDATE groundloop_m5_job_attempt
            SET attempt_output_digest = %s
            WHERE attempt_id = %s
            """,
            (altered_output.attempt_output_digest, checked_attempt.attempt_id),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_attempt_result_artifact
            SET attempt_result_artifact_id = %s,
                attempt_result_artifact_hash = %s,
                attempt_output_digest = %s,
                payload_hash = %s
            WHERE attempt_id = %s
            """,
            (
                altered_result.attempt_result_artifact_id,
                altered_result.attempt_result_artifact_hash,
                altered_output.attempt_output_digest,
                altered_output.payload_hash,
                checked_attempt.attempt_id,
            ),
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="attempt/result closure"):
                _lock_d29_attempt_outputs(cursor, locator)  # type: ignore[arg-type]
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_corrupt_verifier_output_payload")
        connection.execute("RELEASE SAVEPOINT d29_corrupt_verifier_output_payload")


def test_bootstrap_provenance_is_strict_and_point_bounded(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_lock_d29_bootstrap_provenance")
    assert "typed != (False, False, False, False)" in source
    assert '("committed", "sealed", "complete", "strict")' in source
    assert "produced_epoch_id > base_epoch_id" in source
    assert "FOR KEY SHARE OF activation, base" not in source
    assert "FOR UPDATE" not in source
    assert "_require_active_membership(" in source
    membership = function_source("_require_active_membership")
    assert "groundloop_m5_group_version AS group_version" in membership
    assert "group_version.lifecycle_state = 'PUBLISHED'" in membership
    assert "version.valid_from_epoch <= %s" in membership
    assert "version.valid_to_epoch IS NULL" in membership
    assert "creator.structural_status = 'committed'" in membership
    assert "creator.semantic_status = 'sealed'" in membership
    assert "creator.evaluation_state = 'complete'" in membership
    assert "creator.publication_mode IN ('provisional', 'strict')" in membership
    assert "creator.sealed_at IS NOT NULL" in membership


def test_direct_attempts_are_locked_and_byte_validated_not_discarded(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_lock_d29_direct_attempts")
    assert "M4JobAttempt(" in source
    assert "ORDER BY attempt_ordinal" in source
    assert "FOR UPDATE" in source
    assert 'state not in {"leased", "completed", "failed", "expired"}' in source
    assert "attempt.attempt_ordinal != expected_ordinal" in source
    assert '"m4-job-attempt-v1"' in source
    assert '"m4-lease-token-v1"' in source
    assert 'states.count("leased") > 1' in source
    assert '"leased" in states[:-1]' in source
    assert 'job_state is M4JobState.CANCELLED and "completed" in states' in source
    assert "direct attempt state disagrees with its job" in source
    derivation = function_source("_continue_locked_document_open")
    assert (
        "direct_attempts, direct_candidates = _lock_d29_tier_10_authority" in derivation
    )
    assert "_lock_d30_dynamic_owner_topology(" in derivation
    assert "direct_attempts," in derivation
    tier_10 = function_source("_lock_d29_tier_10_authority")
    assert "direct_attempts = _lock_d29_direct_attempts" in tier_10


def test_direct_discovery_closure_recomputes_rows_counts_and_set_hashes(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_validate_direct_discovery_closure")
    for relation in (
        "groundloop_m4_discovery_result",
        "groundloop_impact_channel_hit",
        "groundloop_admitted_pair",
    ):
        assert relation in source
    assert 'lock_clause = " FOR UPDATE" if lock_authority else ""' in source
    assert "M4ChannelHit(" in source
    assert "AdmittedPair(" in source
    assert "M4DiscoveryResult(" in source
    assert '"m4-discovery-channel-set-v1"' in function_source(
        "_direct_channel_set_hash"
    )
    assert '"m4-discovery-admitted-set-v1"' in source
    assert "expected_child_pairs != admitted_pairs" in source


def test_direct_frontier_authority_is_a_complete_strict_dto(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_lock_d29_direct_frontier")
    assert "M4FrontierEntry(" in source
    assert "M4FrontierState(" in source
    for field in (
        "frontier.frontier_state",
        "frontier.rank",
        "frontier.retrieval_score",
        "frontier.candidate_artifact_hash",
        "creator.publication_mode",
        "creator_update.candidate_policy_id",
    ):
        assert field in source
    assert '("committed", "sealed", "complete", "strict")' in source
    assert "entry.candidate_policy_id != candidate_policy_id" in source
    preview = function_source("_direct_withdrawal_preview")
    assert "entry.candidate_policy_id != checked.candidate_policy_id" in preview


def test_candidate_frontier_locator_does_not_infer_historical_job_association(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_gather_d29_locator_authority")
    frontier_block = source.split("direct_candidate_rows =", maxsplit=1)[1].split(
        "pending_direct_parents =", maxsplit=1
    )[0]
    assert "groundloop_candidate_frontier" in frontier_block
    assert "groundloop_semantic_job" not in frontier_block


def test_live_absent_insert_delete_replace_are_bounded_store_previews(
    d29_database: Any,
) -> None:
    plans = {
        kind: d29_database.document_plan(kind, tag="live-preview")
        for kind in ("insert", "delete", "replace")
    }
    with d29_database.connection.cursor() as cursor:
        previews = {
            kind: _plan_document_requirement_withdrawal(cursor, plan)
            for kind, plan in plans.items()
        }
    insert = previews["insert"]
    assert insert.deactivated_chunk_version_ids == ()
    assert insert.withdrawn_candidate_pair_digests == ()
    assert insert.withdrawn_observation_ids == ()
    assert insert.cancelled_job_ids == ()
    assert insert.fallback_keys == ()

    expected_chunks = tuple(
        sorted(d29_database.database.base.chunk_ids, key=lambda value: value.encode())
    )
    for kind in ("delete", "replace"):
        preview = previews[kind]
        assert preview.deactivated_chunk_version_ids == expected_chunks
        assert preview.cancelled_job_ids == ()
        assert preview.withdrawn_observation_ids
        assert preview.fallback_keys
    assert previews["delete"].withdrawn_candidate_pair_digests == (
        previews["replace"].withdrawn_candidate_pair_digests
    )
    assert previews["delete"].withdrawn_observation_ids == (
        previews["replace"].withdrawn_observation_ids
    )
    assert previews["delete"].fallback_keys == previews["replace"].fallback_keys


def test_live_candidate_observation_projections_are_independent_and_deduplicated(
    d29_database: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = d29_database.connection
    base = d29_database.database.base
    manifest = d29_database.database.manifest
    requirement_snapshot = d29_database.database.requirement_snapshot
    chunk_snapshot = d29_database.database.chunk_snapshot
    snapshots = (
        requirement_snapshot.requirement_registry_snapshot_digest,
        chunk_snapshot.active_chunk_snapshot_digest,
    )
    admitted_digest = str(d29_database.first_m5["admitted_pair_digest"])
    candidate_chunk = str(d29_database.first_m5["chunk_version_id"])
    observation_chunk = base.chunk_ids[7]
    both_chunk = base.chunk_ids[6]
    neither_chunk = base.chunk_ids[1]
    candidate_row = connection.execute(
        """
        SELECT epoch_id, subject_id, semantic_pair_digest, candidate_policy_id
        FROM groundloop_m5_requirement_admitted_pair
        WHERE admitted_pair_digest = %s
        """,
        (admitted_digest,),
    ).fetchone()
    both_digest_row = connection.execute(
        """
        SELECT admitted_pair_digest, epoch_id, subject_id,
               semantic_pair_digest, candidate_policy_id
        FROM groundloop_m5_requirement_admitted_pair
        WHERE epoch_id = %s AND chunk_version_id = %s
        """,
        (int(d29_database.first_m5["epoch_id"]), both_chunk),
    ).fetchone()
    candidate_currency = connection.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM groundloop_observation_currency
            WHERE subject_kind = 'requirement' AND chunk_version_id = %s
        )
        """,
        (candidate_chunk,),
    ).fetchone()
    observation_candidate = connection.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM groundloop_m5_requirement_admitted_pair
            WHERE chunk_version_id = %s AND epoch_id <= %s
        )
        """,
        (observation_chunk, base.epoch_id),
    ).fetchone()
    observation_only_rows = connection.execute(
        """
        SELECT subject_id, observation_id
        FROM groundloop_observation_currency
        WHERE subject_kind = 'requirement' AND chunk_version_id = %s
        ORDER BY observation_id COLLATE "C", subject_id COLLATE "C"
        """,
        (observation_chunk,),
    ).fetchall()
    both_observation_rows = connection.execute(
        """
        SELECT subject_id, observation_id
        FROM groundloop_observation_currency
        WHERE subject_kind = 'requirement' AND chunk_version_id = %s
        ORDER BY observation_id COLLATE "C", subject_id COLLATE "C"
        """,
        (both_chunk,),
    ).fetchall()
    assert candidate_row is not None
    assert both_digest_row is not None
    assert candidate_currency is not None
    assert observation_candidate is not None
    assert observation_only_rows
    assert both_observation_rows
    both_digest = str(both_digest_row[0]).strip()
    candidate_requirement = str(candidate_row[1])
    candidate_pair_digest = str(candidate_row[2]).strip()
    both_requirement = str(both_digest_row[2])
    both_pair_digest = str(both_digest_row[3]).strip()
    assert str(candidate_row[3]).strip() == manifest.candidate_policy_id
    assert str(both_digest_row[4]).strip() == manifest.candidate_policy_id
    assert (
        candidate_pair_digest
        == SemanticPairKey(
            SubjectKind.REQUIREMENT,
            candidate_requirement,
            candidate_chunk,
        ).semantic_pair_digest
    )
    assert (
        both_pair_digest
        == SemanticPairKey(
            SubjectKind.REQUIREMENT,
            both_requirement,
            both_chunk,
        ).semantic_pair_digest
    )

    lineage_calls: list[tuple[int, int]] = []
    activity_calls: list[tuple[str, str, bool]] = []

    def install_classification(*, lineage: bool, active: bool) -> None:
        def classify_lineage(
            _cursor: object, owner_epoch_id: int, predecessor_epoch_id: int
        ) -> bool:
            lineage_calls.append((owner_epoch_id, predecessor_epoch_id))
            return lineage

        def classify_activity(
            _cursor: object,
            *,
            predecessor_epoch_id: int,
            snapshots: tuple[str, str] | None,
            requirement_version_id: str,
            chunk_version_id: str,
        ) -> bool:
            assert predecessor_epoch_id <= base.epoch_id
            if snapshots is not None:
                assert snapshots == (
                    requirement_snapshot.requirement_registry_snapshot_digest,
                    chunk_snapshot.active_chunk_snapshot_digest,
                )
            activity_calls.append(
                (requirement_version_id, chunk_version_id, snapshots is None)
            )
            return active

        monkeypatch.setattr(
            postgres_withdrawal, "_is_sealed_lineage_owner", classify_lineage
        )
        monkeypatch.setattr(
            postgres_withdrawal, "_require_active_membership", classify_activity
        )

    with connection.cursor() as cursor:
        install_classification(lineage=True, active=True)
        candidate_only = _candidate_edges(
            cursor,
            chunks=(candidate_chunk,),
            predecessor_epoch_id=base.epoch_id,
            snapshots=snapshots,
            candidate_policy_id=manifest.candidate_policy_id,
            lock_authority=False,
            located_rows=(
                (candidate_chunk, admitted_digest),
                (candidate_chunk, admitted_digest),
            ),
        )
        install_classification(lineage=False, active=True)
        excluded_by_lineage = _candidate_edges(
            cursor,
            chunks=(candidate_chunk,),
            predecessor_epoch_id=base.epoch_id,
            snapshots=snapshots,
            candidate_policy_id=manifest.candidate_policy_id,
            lock_authority=False,
            located_rows=((candidate_chunk, admitted_digest),),
        )
        install_classification(lineage=True, active=False)
        excluded_by_activity = _candidate_edges(
            cursor,
            chunks=(candidate_chunk,),
            predecessor_epoch_id=base.epoch_id,
            snapshots=snapshots,
            candidate_policy_id=manifest.candidate_policy_id,
            lock_authority=False,
            located_rows=((candidate_chunk, admitted_digest),),
        )
        install_classification(lineage=True, active=True)
        observation_only = _observation_edges(
            cursor,
            chunks=(observation_chunk,),
            predecessor_epoch_id=base.epoch_id,
            snapshots=snapshots,
            candidate_policy_id=manifest.candidate_policy_id,
            lock_authority=False,
        )
        both_candidates = _candidate_edges(
            cursor,
            chunks=(both_chunk,),
            predecessor_epoch_id=base.epoch_id,
            snapshots=snapshots,
            candidate_policy_id=manifest.candidate_policy_id,
            lock_authority=False,
            located_rows=((both_chunk, both_digest),),
        )
        both_observations = _observation_edges(
            cursor,
            chunks=(both_chunk,),
            predecessor_epoch_id=base.epoch_id,
            snapshots=snapshots,
            candidate_policy_id=manifest.candidate_policy_id,
            lock_authority=False,
        )

    # The duplicate locator is byte-validated twice but collapses only in the
    # final operational projection.  Lineage and activity remain independent
    # classification decisions rather than being inferred from one another.
    assert tuple(edge.semantic_pair_digest for edge in candidate_only) == (
        candidate_pair_digest,
    )
    assert tuple(edge.requirement_version_id for edge in candidate_only) == (
        candidate_requirement,
    )
    assert excluded_by_lineage == ()
    assert excluded_by_activity == ()
    assert lineage_calls[:4] == [
        (int(candidate_row[0]), base.epoch_id),
        (int(candidate_row[0]), base.epoch_id),
        (int(candidate_row[0]), base.epoch_id),
        (int(candidate_row[0]), base.epoch_id),
    ]
    assert activity_calls[:4] == [
        (candidate_requirement, candidate_chunk, False),
        (candidate_requirement, candidate_chunk, False),
        (candidate_requirement, candidate_chunk, False),
        (candidate_requirement, candidate_chunk, False),
    ]
    assert candidate_currency == (False,)
    assert observation_only and observation_candidate == (False,)
    assert tuple(edge.semantic_pair_digest for edge in both_candidates) == (
        both_pair_digest,
    )
    expected_observation_only_ids = tuple(str(row[1]) for row in observation_only_rows)
    expected_both_observation_ids = tuple(str(row[1]) for row in both_observation_rows)
    assert tuple(edge.observation_id for edge in observation_only) == (
        expected_observation_only_ids
    )
    assert tuple(edge.observation_id for edge in both_observations) == (
        expected_both_observation_ids
    )

    def planned(
        *,
        tag: str,
        chunks: tuple[str, ...],
        candidates: tuple[Any, ...],
        observations: tuple[Any, ...],
    ) -> Any:
        requirements = tuple(
            sorted(
                {edge.requirement_version_id for edge in (*candidates, *observations)}
            )
        )
        return plan_requirement_withdrawal(
            event_id=f"d29-independent-{tag}",
            deactivated_chunk_version_ids=chunks,
            candidate_edges=candidates,
            observation_edges=observations,
            cancelled_job_ids=(),
            active_requirement_version_ids=requirements,
        )

    candidate_plan = planned(
        tag="candidate-only",
        chunks=(candidate_chunk,),
        candidates=candidate_only,
        observations=(),
    )
    observation_plan = planned(
        tag="observation-only",
        chunks=(observation_chunk,),
        candidates=(),
        observations=observation_only,
    )
    both_plan = planned(
        tag="both",
        chunks=(both_chunk,),
        candidates=both_candidates,
        observations=both_observations,
    )
    neither_plan = planned(
        tag="neither",
        chunks=(neither_chunk,),
        candidates=(),
        observations=(),
    )
    expected_candidate_fallback = (
        M5RequirementFallbackKey(
            candidate_requirement,
            manifest.candidate_policy_id,
        ),
    )
    expected_observation_fallback = tuple(
        sorted(
            M5RequirementFallbackKey(str(row[0]), manifest.candidate_policy_id)
            for row in observation_only_rows
        )
    )
    expected_both_fallback = tuple(
        sorted(
            {
                M5RequirementFallbackKey(
                    both_requirement,
                    manifest.candidate_policy_id,
                ),
                *(
                    M5RequirementFallbackKey(str(row[0]), manifest.candidate_policy_id)
                    for row in both_observation_rows
                ),
            }
        )
    )
    assert candidate_plan.withdrawn_candidate_pair_digests == (candidate_pair_digest,)
    assert candidate_plan.deactivated_chunk_version_ids == (candidate_chunk,)
    assert candidate_plan.withdrawn_observation_ids == ()
    assert candidate_plan.cancelled_job_ids == ()
    assert candidate_plan.fallback_keys == expected_candidate_fallback
    assert observation_plan.withdrawn_candidate_pair_digests == ()
    assert observation_plan.deactivated_chunk_version_ids == (observation_chunk,)
    assert observation_plan.withdrawn_observation_ids == expected_observation_only_ids
    assert observation_plan.cancelled_job_ids == ()
    assert observation_plan.fallback_keys == expected_observation_fallback
    assert both_plan.withdrawn_candidate_pair_digests == (both_pair_digest,)
    assert both_plan.deactivated_chunk_version_ids == (both_chunk,)
    assert both_plan.withdrawn_observation_ids == expected_both_observation_ids
    assert both_plan.cancelled_job_ids == ()
    assert both_plan.fallback_keys == expected_both_fallback
    assert neither_plan.withdrawn_candidate_pair_digests == ()
    assert neither_plan.deactivated_chunk_version_ids == (neither_chunk,)
    assert neither_plan.withdrawn_observation_ids == ()
    assert neither_plan.cancelled_job_ids == ()
    assert neither_plan.fallback_keys == ()


def test_live_qualifying_cross_policy_candidate_fails_closed(
    d29_database: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = d29_database.connection
    base = d29_database.database.base
    manifest = d29_database.database.manifest
    old_digest = str(d29_database.first_m5["admitted_pair_digest"])
    chunk_id = str(d29_database.first_m5["chunk_version_id"])
    admitted = connection.execute(
        """
        SELECT epoch_id, subject_id, chunk_version_id, reasons,
               mandatory_lineage
        FROM groundloop_m5_requirement_admitted_pair
        WHERE admitted_pair_digest = %s
        """,
        (old_digest,),
    ).fetchone()
    source_rows = connection.execute(
        """
        SELECT root_job_id, scope_contract_digest, selection_digest
        FROM groundloop_m5_requirement_admitted_pair_source
        WHERE admitted_pair_digest = %s
        ORDER BY root_job_id COLLATE "C"
        """,
        (old_digest,),
    ).fetchall()
    assert admitted is not None and source_rows
    alternate_budget = manifest.reverse_budget_per_inserted_chunk + 1
    alternate_policy_id = runtime_digests.candidate_policy_manifest_digest(
        embedding_model_artifact_id=manifest.embedding_model_artifact_id,
        requirement_role_template_hash=manifest.requirement_role_template_hash,
        chunk_role_template_hash=manifest.chunk_role_template_hash,
        vector_method_version=manifest.vector_method_version,
        vector_index_kind=manifest.vector_index_kind,
        vector_index_build_config_hash=manifest.vector_index_build_config_hash,
        vector_search_config_hash=manifest.vector_search_config_hash,
        lexical_method_version=manifest.lexical_method_version,
        lexical_config_hash=manifest.lexical_config_hash,
        lexical_postgres_version=manifest.lexical_postgres_version,
        lexical_regconfig_identity=manifest.lexical_regconfig_identity,
        fusion_version=manifest.fusion_version,
        reverse_budget_per_inserted_chunk=alternate_budget,
        forward_budget_per_requirement=manifest.forward_budget_per_requirement,
        verifier_execution_spec_hash=manifest.verifier_execution_spec_hash,
        decision_policy_version=manifest.decision_policy_version,
        lineage_safety_override=manifest.lineage_safety_override,
    )
    sources = tuple(
        M5RequirementAdmittedPairSource(
            str(row[0]).strip(), str(row[1]).strip(), str(row[2]).strip()
        )
        for row in source_rows
    )
    altered = M5RequirementAdmittedPair.build(
        epoch_id=int(admitted[0]),
        pair=SemanticPairKey(
            SubjectKind.REQUIREMENT,
            str(admitted[1]),
            str(admitted[2]),
        ),
        candidate_policy_id=alternate_policy_id,
        sources=sources,
        reasons=tuple(
            M5RequirementAdmissionChannel(str(value)) for value in admitted[3]
        ),
    )
    root_ids = [source.root_job_id for source in sources]
    connection.execute("SAVEPOINT d29_cross_policy_candidate")
    try:
        connection.execute(
            """
            INSERT INTO groundloop_m5_candidate_policy (
                candidate_policy_id, candidate_policy_manifest_hash,
                embedding_model_artifact_id, requirement_role_template_hash,
                chunk_role_template_hash, vector_method_version,
                vector_index_kind, vector_index_build_config_hash,
                vector_search_config_hash, lexical_method_version,
                lexical_config_hash, lexical_postgres_version,
                lexical_regconfig_identity, fusion_version,
                reverse_budget_per_inserted_chunk,
                forward_budget_per_requirement,
                verifier_execution_spec_hash, decision_policy_version,
                lineage_safety_override
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                alternate_policy_id,
                alternate_policy_id,
                manifest.embedding_model_artifact_id,
                manifest.requirement_role_template_hash,
                manifest.chunk_role_template_hash,
                manifest.vector_method_version,
                manifest.vector_index_kind.value,
                manifest.vector_index_build_config_hash,
                manifest.vector_search_config_hash,
                manifest.lexical_method_version,
                manifest.lexical_config_hash,
                manifest.lexical_postgres_version,
                manifest.lexical_regconfig_identity,
                manifest.fusion_version,
                alternate_budget,
                manifest.forward_budget_per_requirement,
                manifest.verifier_execution_spec_hash,
                manifest.decision_policy_version,
                manifest.lineage_safety_override,
            ),
        )
        for statement in (
            "ALTER TABLE groundloop_m5_requirement_admitted_pair "
            "DISABLE TRIGGER groundloop_m5_admitted_pair_immutable",
            "ALTER TABLE groundloop_m5_requirement_admitted_pair_source "
            "DISABLE TRIGGER groundloop_m5_admitted_pair_source_immutable",
            "ALTER TABLE groundloop_m5_discovery_scope "
            "DISABLE TRIGGER groundloop_m5_discovery_scope_transition",
            "ALTER TABLE groundloop_m5_semantic_job "
            "DISABLE TRIGGER groundloop_m5_semantic_job_transition",
        ):
            connection.execute(statement)
        connection.execute(
            """
            UPDATE groundloop_m5_requirement_admitted_pair
            SET candidate_policy_id = %s, admitted_pair_digest = %s
            WHERE admitted_pair_digest = %s
            """,
            (alternate_policy_id, altered.admitted_pair_digest, old_digest),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_requirement_admitted_pair_source
            SET admitted_pair_digest = %s
            WHERE admitted_pair_digest = %s
            """,
            (altered.admitted_pair_digest, old_digest),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_discovery_scope
            SET candidate_policy_id = %s
            WHERE root_job_id = ANY(%s)
            """,
            (alternate_policy_id, root_ids),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_semantic_job
            SET candidate_policy_id = %s,
                candidate_policy_manifest_hash = %s
            WHERE logical_job_id = ANY(%s)
            """,
            (alternate_policy_id, alternate_policy_id, root_ids),
        )
        monkeypatch.setattr(
            postgres_withdrawal,
            "_is_sealed_lineage_owner",
            lambda *_args, **_kwargs: True,
        )
        monkeypatch.setattr(
            postgres_withdrawal,
            "_require_active_membership",
            lambda *_args, **_kwargs: True,
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="cross-policy"):
                _candidate_edges(
                    cursor,
                    chunks=(chunk_id,),
                    predecessor_epoch_id=base.epoch_id,
                    snapshots=None,
                    candidate_policy_id=manifest.candidate_policy_id,
                    lock_authority=False,
                    located_rows=((chunk_id, altered.admitted_pair_digest),),
                    qualifying_digests=frozenset((altered.admitted_pair_digest,)),
                )
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_cross_policy_candidate")
        connection.execute("RELEASE SAVEPOINT d29_cross_policy_candidate")


def test_live_qualifying_candidate_rejects_semantic_pair_digest_only_corruption(
    d29_database: Any,
) -> None:
    connection = d29_database.connection
    admitted_digest = str(d29_database.first_m5["admitted_pair_digest"])
    chunk_id = str(d29_database.first_m5["chunk_version_id"])
    corrupted = "f" * 64
    connection.execute("SAVEPOINT d29_qualifying_semantic_digest_only")
    try:
        connection.execute(
            """
            ALTER TABLE groundloop_m5_requirement_admitted_pair
            DISABLE TRIGGER groundloop_m5_admitted_pair_immutable
            """
        )
        connection.execute(
            """
            ALTER TABLE groundloop_m5_requirement_admitted_pair
            DROP CONSTRAINT groundloop_m5_admitted_semantic_pair_digest
            """
        )
        assert (
            connection.execute(
                """
                UPDATE groundloop_m5_requirement_admitted_pair
                SET semantic_pair_digest = %s
                WHERE admitted_pair_digest = %s
                """,
                (corrupted, admitted_digest),
            ).rowcount
            == 1
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="candidate locator row"):
                _candidate_edges(
                    cursor,
                    chunks=(chunk_id,),
                    predecessor_epoch_id=d29_database.database.base.epoch_id,
                    snapshots=None,
                    candidate_policy_id=(
                        d29_database.database.manifest.candidate_policy_id
                    ),
                    lock_authority=False,
                    located_rows=((chunk_id, admitted_digest),),
                    qualifying_digests=frozenset((admitted_digest,)),
                )
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_qualifying_semantic_digest_only")
        connection.execute("RELEASE SAVEPOINT d29_qualifying_semantic_digest_only")


def test_live_qualifying_candidate_rejects_admitted_digest_only_corruption(
    d29_database: Any,
) -> None:
    connection = d29_database.connection
    admitted_digest = str(d29_database.first_m5["admitted_pair_digest"])
    chunk_id = str(d29_database.first_m5["chunk_version_id"])
    corrupted = "e" * 64
    connection.execute("SAVEPOINT d29_qualifying_admitted_digest_only")
    try:
        for statement in (
            "ALTER TABLE groundloop_m5_requirement_admitted_pair "
            "DISABLE TRIGGER groundloop_m5_admitted_pair_immutable",
            "ALTER TABLE groundloop_m5_requirement_admitted_pair_source "
            "DISABLE TRIGGER groundloop_m5_admitted_pair_source_immutable",
        ):
            connection.execute(statement)
        assert (
            connection.execute(
                """
                UPDATE groundloop_m5_requirement_admitted_pair
                SET admitted_pair_digest = %s
                WHERE admitted_pair_digest = %s
                """,
                (corrupted, admitted_digest),
            ).rowcount
            == 1
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_requirement_admitted_pair_source
            SET admitted_pair_digest = %s
            WHERE admitted_pair_digest = %s
            """,
                (corrupted, admitted_digest),
            ).rowcount
            > 0
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="candidate admission digest"):
                _candidate_edges(
                    cursor,
                    chunks=(chunk_id,),
                    predecessor_epoch_id=d29_database.database.base.epoch_id,
                    snapshots=None,
                    candidate_policy_id=(
                        d29_database.database.manifest.candidate_policy_id
                    ),
                    lock_authority=False,
                    located_rows=((chunk_id, corrupted),),
                    qualifying_digests=frozenset((corrupted,)),
                )
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_qualifying_admitted_digest_only")
        connection.execute("RELEASE SAVEPOINT d29_qualifying_admitted_digest_only")


def test_live_malformed_nonlineage_admission_is_ignored_before_full_validation(
    d29_database: Any,
) -> None:
    connection = d29_database.connection
    owner_epoch_id = int(d29_database.first_m5["epoch_id"])
    owner_state = connection.execute(
        """
        SELECT runtime_state FROM groundloop_m5_runtime_epoch
        WHERE epoch_id = %s
        """,
        (owner_epoch_id,),
    ).fetchone()
    assert owner_state is not None
    assert str(owner_state[0]) != "sealed"
    admitted_digest = str(d29_database.first_m5["admitted_pair_digest"])
    connection.execute("SAVEPOINT d29_nonlineage_malformed_admission")
    try:
        connection.execute(
            """
            ALTER TABLE groundloop_m5_requirement_admitted_pair
            DISABLE TRIGGER groundloop_m5_admitted_pair_immutable
            """
        )
        connection.execute(
            """
            ALTER TABLE groundloop_m5_requirement_admitted_pair
            DROP CONSTRAINT groundloop_m5_admitted_semantic_pair_digest
            """
        )
        connection.execute(
            """
            UPDATE groundloop_m5_requirement_admitted_pair
            SET semantic_pair_digest = %s
            WHERE admitted_pair_digest = %s
            """,
            ("f" * 64, admitted_digest),
        )
        event = d29_database.document_plan("delete", tag="ignore-malformed-nonlineage")
        with connection.cursor() as cursor:
            preview = _plan_document_requirement_withdrawal(cursor, event)
        assert admitted_digest not in preview.withdrawn_candidate_pair_digests
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_nonlineage_malformed_admission")
        connection.execute("RELEASE SAVEPOINT d29_nonlineage_malformed_admission")


def test_live_same_event_id_with_another_payload_conflicts_before_planning(
    d29_database: Any,
) -> None:
    event = d29_database.document_plan("delete", tag="wrong-payload")
    different_hash = "0" * 64 if event.payload_hash != "0" * 64 else "1" * 64
    d29_database.connection.execute(
        """
        INSERT INTO groundloop_epoch (
            event_id, payload_hash, revision, structural_status,
            semantic_status, evaluation_state, publication_mode, sealed_at
        ) VALUES (%s, %s, 0, 'committed', 'sealed', 'complete', 'strict', now())
        """,
        (event.structural_event_id, different_hash),
    )
    try:
        with d29_database.connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="payload hash"):
                _plan_document_requirement_withdrawal(cursor, event)
    finally:
        d29_database.connection.rollback()


def test_live_direct_preview_composes_with_store_derived_withdrawal(
    d29_database: Any,
) -> None:
    execution_policy = ApplicationExecutionPolicy(
        "a" * 64,
        "b" * 64,
        d29_database.database.manifest.verifier_execution_spec_hash,
    )
    plans = {
        kind: d29_database.document_plan(kind, tag="direct-preview")
        for kind in ("insert", "delete", "replace")
    }
    with d29_database.connection.cursor() as cursor:
        previews = {
            kind: _preview_document_direct_open(
                cursor,
                plan,
                execution_policy=execution_policy,
            )
            for kind, plan in plans.items()
        }
    insert = previews["insert"]
    assert insert.withdrawal is not None
    assert insert.withdrawal.plan.deactivated_chunk_ids == ()
    assert len(insert.root_jobs) == 1
    assert len(insert.discovery_scopes) == 1
    assert plans["insert"].direct_plan is not None
    assert (
        insert.discovery_scopes[0].registered_claim_ids
        == plans["insert"].direct_plan.registered_claim_ids
    )

    delete = previews["delete"]
    assert delete.withdrawal is not None
    assert delete.withdrawal.plan.deactivated_chunk_ids
    assert delete.withdrawal.plan.observation_ids
    assert delete.withdrawal.fallback_claim_ids

    replace = previews["replace"]
    assert replace.withdrawal is not None
    assert replace.withdrawal.plan == delete.withdrawal.plan
    assert replace.withdrawal.fallback_claim_ids == delete.withdrawal.fallback_claim_ids
    assert len(replace.root_jobs) == len(delete.root_jobs) + 1
    assert plans["replace"].direct_plan is not None
    impact_scopes = tuple(
        scope.registered_claim_ids for scope in replace.discovery_scopes
    )
    assert impact_scopes
    assert all(
        members == plans["replace"].direct_plan.registered_claim_ids
        for members in impact_scopes
    )


def test_live_direct_frontier_requires_complete_dto_and_strict_creator(
    d29_database: Any,
) -> None:
    connection = d29_database.connection
    predecessor_epoch_id = d29_database.database.base.epoch_id
    frontier = connection.execute(
        """
        SELECT claim_id, chunk_version_id, candidate_policy_id, valid_from_epoch
        FROM groundloop_candidate_frontier
        WHERE valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        ORDER BY claim_id COLLATE "C", chunk_version_id COLLATE "C",
                 candidate_policy_id COLLATE "C", valid_from_epoch
        LIMIT 1
        """,
        (predecessor_epoch_id, predecessor_epoch_id),
    ).fetchone()
    seeded_frontier = False
    if frontier is None:
        creator = connection.execute(
            """
            SELECT job.candidate_policy_id, policy.claim_registry_snapshot_id
            FROM groundloop_semantic_job AS job
            JOIN groundloop_candidate_policy AS policy
              ON policy.candidate_policy_id = job.candidate_policy_id
            WHERE job.job_id = %s
            """,
            (str(d29_database.first_m4["root_job_id"]),),
        ).fetchone()
        assert creator is not None
        creator_epoch = connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (
                'd29-frontier-authority-event', %s, 1, 'committed',
                'sealed', 'complete', 'strict', clock_timestamp()
            ) RETURNING epoch_id
            """,
            ("c" * 64,),
        ).fetchone()
        assert creator_epoch is not None
        predecessor_epoch_id = int(creator_epoch[0])
        connection.execute(
            "ALTER TABLE groundloop_m4_update "
            "DISABLE TRIGGER groundloop_m4_update_runtime_mode_guard"
        )
        connection.execute(
            """
            INSERT INTO groundloop_m4_update (
                epoch_id, update_kind, candidate_policy_id,
                previous_published_epoch_id, registry_snapshot_id, manifest
            ) VALUES (%s, 'insert', %s, %s, %s, '{}'::jsonb)
            """,
            (
                predecessor_epoch_id,
                str(creator[0]),
                d29_database.database.base.epoch_id,
                str(creator[1]),
            ),
        )
        connection.execute(
            "ALTER TABLE groundloop_m4_update "
            "ENABLE TRIGGER groundloop_m4_update_runtime_mode_guard"
        )
        key = (
            d29_database.database.base.claim_ids[0],
            d29_database.database.base.chunk_ids[0],
            str(creator[0]),
            predecessor_epoch_id,
        )
        connection.execute(
            """
            INSERT INTO groundloop_candidate_frontier (
                claim_id, chunk_version_id, candidate_policy_id,
                frontier_state, rank, retrieval_score,
                candidate_artifact_hash, valid_from_epoch, valid_to_epoch
            ) VALUES (%s, %s, %s, 'verified_current', 1, 1.0, %s, %s, NULL)
            """,
            (*key[:3], "d" * 64, key[3]),
        )
        frontier = key
        seeded_frontier = True
    assert frontier is not None
    key = (str(frontier[0]), str(frontier[1]), str(frontier[2]), int(frontier[3]))
    locator = SimpleNamespace(direct_frontier_keys=(key,))

    with connection.cursor() as cursor:
        dependencies = _lock_d29_direct_frontier(  # type: ignore[arg-type]
            cursor,
            locator,
            predecessor_epoch_id=predecessor_epoch_id,
            candidate_policy_id=key[2],
        )
    assert len(dependencies) == 1

    with connection.cursor() as cursor:
        with pytest.raises(EventConflictError, match="cross-policy direct candidate"):
            _lock_d29_direct_frontier(  # type: ignore[arg-type]
                cursor,
                locator,
                predecessor_epoch_id=predecessor_epoch_id,
                candidate_policy_id=("0" * 64 if key[2] != "0" * 64 else "1" * 64),
            )

    connection.execute("SAVEPOINT d29_frontier_bad_artifact")
    try:
        connection.execute(
            """
            UPDATE groundloop_candidate_frontier
            SET candidate_artifact_hash = 'bad'
            WHERE claim_id = %s AND chunk_version_id = %s
              AND candidate_policy_id = %s AND valid_from_epoch = %s
            """,
            key,
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="frontier DTO"):
                _lock_d29_direct_frontier(  # type: ignore[arg-type]
                    cursor,
                    locator,
                    predecessor_epoch_id=predecessor_epoch_id,
                    candidate_policy_id=key[2],
                )
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_frontier_bad_artifact")
        connection.execute("RELEASE SAVEPOINT d29_frontier_bad_artifact")

    connection.execute("SAVEPOINT d29_frontier_provisional_creator")
    try:
        connection.execute(
            """
            UPDATE groundloop_epoch SET publication_mode = 'provisional'
            WHERE epoch_id = %s
            """,
            (key[3],),
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="frontier authority"):
                _lock_d29_direct_frontier(  # type: ignore[arg-type]
                    cursor,
                    locator,
                    predecessor_epoch_id=predecessor_epoch_id,
                    candidate_policy_id=key[2],
                )
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_frontier_provisional_creator")
        connection.execute("RELEASE SAVEPOINT d29_frontier_provisional_creator")
    if seeded_frontier:
        connection.execute(
            """
            DELETE FROM groundloop_candidate_frontier
            WHERE claim_id = %s AND chunk_version_id = %s
              AND candidate_policy_id = %s AND valid_from_epoch = %s
            """,
            key,
        )
        connection.execute(
            "DELETE FROM groundloop_m4_update WHERE epoch_id = %s",
            (key[3],),
        )
        connection.execute(
            "DELETE FROM groundloop_epoch WHERE epoch_id = %s",
            (key[3],),
        )


def test_live_cancelled_direct_job_rejects_completed_attempt_history(
    d29_database: Any,
) -> None:
    connection = d29_database.connection
    root_job_id = str(d29_database.first_m4["root_job_id"])
    connection.execute("SAVEPOINT d29_cancelled_completed_attempt")
    try:
        row = connection.execute(
            """
            UPDATE groundloop_semantic_job
            SET job_state = 'cancelled'
            WHERE job_id = %s
            RETURNING execution_spec_hash
            """,
            (root_job_id,),
        ).fetchone()
        assert row is not None
        connection.execute(
            """
            INSERT INTO groundloop_semantic_job_attempt (
                attempt_id, job_id, execution_spec_hash, attempt_ordinal,
                lease_token_hash, attempt_state, lease_expires_at,
                started_at, finished_at
            ) VALUES (%s, %s, %s, 1, %s, 'completed',
                      now() + interval '1 hour', now(), now())
            """,
            (
                stable_m4_digest("m4-job-attempt-v1", root_job_id, "1"),
                root_job_id,
                str(row[0]),
                stable_m4_digest("m4-lease-token-v1", root_job_id, "1"),
            ),
        )
        locator = _direct_attempt_locator(connection, root_job_id)
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="state disagrees"):
                _lock_d29_direct_attempts(cursor, locator)  # type: ignore[arg-type]
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_cancelled_completed_attempt")
        connection.execute("RELEASE SAVEPOINT d29_cancelled_completed_attempt")


@pytest.mark.parametrize("corruption", ("wrong-token", "multiple-leased"))
def test_live_direct_attempt_history_rejects_noncanonical_or_ambiguous_leases(
    d29_database: Any,
    corruption: str,
) -> None:
    connection = d29_database.connection
    root_job_id = str(d29_database.first_m4["root_job_id"])
    savepoint = f"d29_attempt_{corruption.replace('-', '_')}"
    connection.execute(f"SAVEPOINT {savepoint}")
    try:
        job_state = "running" if corruption == "wrong-token" else "cancelled"
        row = connection.execute(
            """
            UPDATE groundloop_semantic_job
            SET job_state = %s
            WHERE job_id = %s
            RETURNING execution_spec_hash
            """,
            (job_state, root_job_id),
        ).fetchone()
        assert row is not None
        attempt_count = 1 if corruption == "wrong-token" else 2
        for ordinal in range(1, attempt_count + 1):
            token_hash = stable_m4_digest(
                "m4-lease-token-v1", root_job_id, str(ordinal)
            )
            if corruption == "wrong-token":
                token_hash = "f" * 64
            connection.execute(
                """
                INSERT INTO groundloop_semantic_job_attempt (
                    attempt_id, job_id, execution_spec_hash, attempt_ordinal,
                    lease_token_hash, attempt_state, lease_expires_at,
                    started_at, finished_at
                ) VALUES (%s, %s, %s, %s, %s, 'leased',
                          now() + interval '1 hour', now(), NULL)
                """,
                (
                    stable_m4_digest("m4-job-attempt-v1", root_job_id, str(ordinal)),
                    root_job_id,
                    str(row[0]),
                    ordinal,
                    token_hash,
                ),
            )
        locator = _direct_attempt_locator(connection, root_job_id)
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="attempt"):
                _lock_d29_direct_attempts(cursor, locator)  # type: ignore[arg-type]
    finally:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")


def test_live_bootstrap_currency_revision_is_independent_of_base_revision(
    d29_database: Any,
) -> None:
    connection = d29_database.connection
    base_epoch_id = d29_database.database.base.epoch_id
    connection.execute("SAVEPOINT d29_bootstrap_revision_independence")
    try:
        connection.execute(
            "UPDATE groundloop_epoch SET revision = 0 WHERE epoch_id = %s",
            (base_epoch_id,),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_publication_head
            SET sealed_revision = 0
            WHERE singleton AND epoch_id = %s
            """,
            (base_epoch_id,),
        )
        installed = connection.execute(
            """
            SELECT min(installed_revision), max(installed_revision)
            FROM groundloop_observation_currency
            WHERE subject_kind = 'requirement'
            """
        ).fetchone()
        assert installed is not None
        assert int(installed[0]) > 0
        assert int(installed[1]) > 0
        plan = d29_database.document_plan(
            "delete", tag="bootstrap-revision-independence"
        )
        with connection.cursor() as cursor:
            preview = _plan_document_requirement_withdrawal(cursor, plan)
        assert preview.withdrawn_observation_ids
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_bootstrap_revision_independence")
        connection.execute("RELEASE SAVEPOINT d29_bootstrap_revision_independence")


def _direct_attempt_locator(connection: Any, job_id: str) -> Any:
    rows = tuple(
        tuple(row)
        for row in connection.execute(
            """
            SELECT attempt_id, job_id, execution_spec_hash, attempt_ordinal,
                   lease_token_hash, attempt_state, lease_expires_at,
                   started_at, finished_at
            FROM groundloop_semantic_job_attempt
            WHERE job_id = %s
            ORDER BY attempt_ordinal
            """,
            (job_id,),
        ).fetchall()
    )
    owner = SimpleNamespace(attempts=((job_id, rows),))
    return SimpleNamespace(
        direct_job_ids=(job_id,),
        d30_claims=SimpleNamespace(owners=(owner,)),
    )


def test_live_requirement_bootstrap_membership_rejects_lifecycle_and_creator_drift(
    d29_database: Any,
) -> None:
    connection = d29_database.connection
    base_epoch_id = d29_database.database.base.epoch_id
    requirement = connection.execute(
        """
        SELECT currency.subject_id, currency.chunk_version_id,
               currency.task_type, currency.observation_id,
               observation.produced_epoch
        FROM groundloop_observation_currency AS currency
        JOIN groundloop_semantic_observation AS observation
          ON observation.observation_id = currency.observation_id
        WHERE currency.subject_kind = 'requirement'
        ORDER BY currency.observation_id COLLATE "C"
        LIMIT 1
        """
    ).fetchone()
    assert requirement is not None
    locator = SimpleNamespace(
        bootstrap_observation_coordinates=((str(requirement[3]), int(requirement[4])),),
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
        authority = _lock_d29_bootstrap_provenance(  # type: ignore[arg-type]
            cursor,
            locator,
            predecessor_epoch_id=base_epoch_id,
        )
    assert set(authority) == {str(requirement[3])}

    group_version = connection.execute(
        """
        SELECT group_version_id
        FROM groundloop_m5_requirement_version
        WHERE requirement_version_id = %s
        """,
        (str(requirement[0]),),
    ).fetchone()
    assert group_version is not None
    connection.execute("SAVEPOINT d29_bootstrap_group_lifecycle")
    try:
        connection.execute(
            """
            ALTER TABLE groundloop_m5_group_version
            DISABLE TRIGGER groundloop_m5_group_version_lifecycle_guard
            """
        )
        connection.execute(
            """
            UPDATE groundloop_m5_group_version SET lifecycle_state = 'FAILED'
            WHERE group_version_id = %s
            """,
            (str(group_version[0]),),
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="activation image"):
                _lock_d29_bootstrap_provenance(  # type: ignore[arg-type]
                    cursor,
                    locator,
                    predecessor_epoch_id=base_epoch_id,
                )
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_bootstrap_group_lifecycle")
        connection.execute("RELEASE SAVEPOINT d29_bootstrap_group_lifecycle")

    document_version = connection.execute(
        """
        SELECT chunk.document_version_id
        FROM groundloop_chunk_version AS chunk
        WHERE chunk.chunk_version_id = %s
        """,
        (str(requirement[1]),),
    ).fetchone()
    assert document_version is not None
    connection.execute("SAVEPOINT d29_bootstrap_document_interval")
    try:
        future = connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (%s, %s, 0, 'committed', 'sealed', 'complete', 'strict', now())
            RETURNING epoch_id
            """,
            ("d29-bootstrap-future-version", "f" * 64),
        ).fetchone()
        assert future is not None
        assert int(future[0]) > base_epoch_id
        connection.execute(
            """
            UPDATE groundloop_document_version SET valid_from_epoch = %s
            WHERE document_version_id = %s
            """,
            (int(future[0]), str(document_version[0])),
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="activation image"):
                _lock_d29_bootstrap_provenance(  # type: ignore[arg-type]
                    cursor,
                    locator,
                    predecessor_epoch_id=base_epoch_id,
                )
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_bootstrap_document_interval")
        connection.execute("RELEASE SAVEPOINT d29_bootstrap_document_interval")

    creator = connection.execute(
        """
        SELECT valid_from_epoch FROM groundloop_chunk_version
        WHERE chunk_version_id = %s
        """,
        (str(requirement[1]),),
    ).fetchone()
    assert creator is not None
    connection.execute("SAVEPOINT d29_bootstrap_creator_publication")
    try:
        connection.execute(
            "UPDATE groundloop_epoch SET publication_mode = 'provisional' "
            "WHERE epoch_id = %s",
            (int(creator[0]),),
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="bootstrap|activation image"):
                _lock_d29_bootstrap_provenance(  # type: ignore[arg-type]
                    cursor,
                    locator,
                    predecessor_epoch_id=base_epoch_id,
                )
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_bootstrap_creator_publication")
        connection.execute("RELEASE SAVEPOINT d29_bootstrap_creator_publication")


def test_live_touched_snapshot_members_reject_core_field_corruption(
    d29_database: Any,
) -> None:
    connection = d29_database.connection
    predecessor_epoch_id = d29_database.database.base.epoch_id
    requirement_snapshot = d29_database.database.requirement_snapshot
    chunk_snapshot = d29_database.database.chunk_snapshot
    requirement_id = requirement_snapshot.entries[0].requirement_version_id
    chunk_id = chunk_snapshot.entries[0].chunk_version_id
    snapshots = (
        requirement_snapshot.requirement_registry_snapshot_digest,
        chunk_snapshot.active_chunk_snapshot_digest,
    )
    locator = SimpleNamespace(touched_requirement_ids=(requirement_id,))

    connection.execute("SAVEPOINT d29_corrupt_requirement_member")
    try:
        owner = connection.execute(
            """
            SELECT owner_claim_id
            FROM groundloop_m5_requirement_registry_snapshot_member
            WHERE requirement_registry_snapshot_digest = %s
              AND requirement_version_id = %s
            """,
            (snapshots[0], requirement_id),
        ).fetchone()
        assert owner is not None
        other_claim = next(
            claim_id
            for claim_id in d29_database.database.base.claim_ids
            if claim_id != str(owner[0])
        )
        connection.execute(
            "ALTER TABLE groundloop_m5_requirement_registry_snapshot_member "
            "DISABLE TRIGGER groundloop_m5_requirement_snapshot_member_immutable"
        )
        connection.execute(
            """
            UPDATE groundloop_m5_requirement_registry_snapshot_member
            SET owner_claim_id = %s
            WHERE requirement_registry_snapshot_digest = %s
              AND requirement_version_id = %s
            """,
            (other_claim, snapshots[0], requirement_id),
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="snapshot authority"):
                _lock_d29_touched_membership(  # type: ignore[arg-type]
                    cursor,
                    locator,
                    predecessor_epoch_id=predecessor_epoch_id,
                    snapshots=snapshots,
                    source_chunks=(chunk_id,),
                )
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_corrupt_requirement_member")
        connection.execute("RELEASE SAVEPOINT d29_corrupt_requirement_member")

    connection.execute("SAVEPOINT d29_corrupt_chunk_member")
    try:
        connection.execute(
            "ALTER TABLE groundloop_m5_active_chunk_snapshot_member "
            "DISABLE TRIGGER groundloop_m5_chunk_snapshot_member_immutable"
        )
        connection.execute(
            """
            UPDATE groundloop_m5_active_chunk_snapshot_member
            SET text_hash = %s
            WHERE active_chunk_snapshot_digest = %s
              AND chunk_version_id = %s
            """,
            ("f" * 64, snapshots[1], chunk_id),
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="snapshot authority"):
                _lock_d29_touched_membership(  # type: ignore[arg-type]
                    cursor,
                    locator,
                    predecessor_epoch_id=predecessor_epoch_id,
                    snapshots=snapshots,
                    source_chunks=(chunk_id,),
                )
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_corrupt_chunk_member")
        connection.execute("RELEASE SAVEPOINT d29_corrupt_chunk_member")
