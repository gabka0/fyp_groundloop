"""D29 source-identity and dependency-direction tripwires."""

from __future__ import annotations

import ast
import hashlib
import inspect
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from psycopg import sql

from groundloop.errors import EventConflictError, ValidationError
from groundloop.m4.application import ApplicationExecutionPolicy
from groundloop.m4.contracts import UpdateKind
from groundloop.m5.runtime import postgres_withdrawal
from groundloop.m5.runtime.postgres_withdrawal import (
    _derive_locked_document_open,
    _document_event,
    _hydrate_retained_direct_open,
    _hydrate_retained_requirement_open,
    _load_retained_document_open,
    _plan_document_requirement_withdrawal,
    _predecessor_snapshot_ids,
    _preview_document_direct_open,
    _structural_payload_from_event,
    _validate_direct_update_manifest,
)
from groundloop.postgres.migrations import (
    M5BoundedDocumentWithdrawalBundleError,
    m5_bounded_document_withdrawal_bundle_identity,
)


def _install_test_typed_predecessor(
    d29_database: Any,
    *,
    label: str,
    registry_digest: str,
    chunk_digest: str,
) -> int:
    connection = d29_database.connection
    base_epoch_id = d29_database.database.base.epoch_id
    manifest = d29_database.database.manifest
    event_id = f"d29-store-predecessor-{label}"
    row = connection.execute(
        """
        INSERT INTO groundloop_epoch (
            event_id, payload_hash, revision, structural_status,
            semantic_status, evaluation_state, publication_mode, sealed_at
        ) VALUES (%s, %s, 1, 'committed', 'sealed', 'complete', 'strict', now())
        RETURNING epoch_id
        """,
        (event_id, hashlib.sha256(event_id.encode()).hexdigest()),
    ).fetchone()
    assert row is not None
    epoch_id = int(row[0])
    connection.execute(
        """
        INSERT INTO groundloop_m5_update (
            epoch_id, update_kind, previous_published_epoch_id,
            decision_policy_version, manifest
        ) VALUES (%s, 'policy_change', %s, %s, '{}'::jsonb)
        """,
        (epoch_id, base_epoch_id, manifest.decision_policy_version),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_runtime_epoch (
            epoch_id, structural_event_id, candidate_policy_id,
            candidate_policy_manifest_hash,
            requirement_registry_snapshot_digest,
            active_chunk_snapshot_digest,
            expected_previous_published_epoch_id,
            requirement_root_set_hash, runtime_state, revision,
            open_work_count, open_scope_count, blocking_failure_count,
            terminal_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                  'sealed', 1, 0, 0, 0, now())
        """,
        (
            epoch_id,
            event_id,
            manifest.candidate_policy_id,
            manifest.manifest_hash,
            registry_digest,
            chunk_digest,
            base_epoch_id,
            "0" * 64,
        ),
    )
    connection.execute(
        "UPDATE groundloop_m4_publication_head SET epoch_id = %s WHERE singleton",
        (epoch_id,),
    )
    connection.execute(
        """
        UPDATE groundloop_m5_publication_head
        SET epoch_id = %s, sealed_revision = 1
        WHERE singleton
        """,
        (epoch_id,),
    )
    return epoch_id


def test_withdrawal_has_no_forbidden_runtime_facade_imports() -> None:
    source = Path(postgres_withdrawal.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    }
    forbidden = {
        "groundloop.m5.runtime.persistence",
        "groundloop.m5.runtime.postgres_direct_application",
        "groundloop.m5.runtime.postgres_matching",
        "groundloop.m5.runtime.postgres_recovery",
        "groundloop.m5.runtime.verifier",
    }
    assert imported.isdisjoint(forbidden)
    assert "_structural_payload" not in imported


def test_structural_source_is_derived_from_exact_typed_event(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_structural_payload_from_event")
    assert "InsertDocumentEvent" in source
    assert "DeleteDocumentVersionEvent" in source
    assert "ReplaceDocumentVersionEvent" in source
    assert "StructuralPayload" in source
    assert "repr(" not in source
    assert "json.dumps" not in source


@pytest.mark.parametrize("event_kind", ("insert", "replace"))
def test_live_direct_inserted_chunk_ids_are_compare_only_event_derivatives(
    d29_database: Any,
    event_kind: str,
) -> None:
    plan = d29_database.document_plan(event_kind, tag="alter-inserted")
    assert plan.direct_plan is not None
    altered_direct = replace(
        plan.direct_plan,
        inserted_chunk_version_ids=(f"d29-altered-{event_kind}-chunk",),
    )
    with pytest.raises(ValidationError, match="differs from its concrete event"):
        _document_event(replace(plan, direct_plan=altered_direct))


def test_live_direct_update_kind_is_compare_only_concrete_event_derivative(
    d29_database: Any,
) -> None:
    plan = d29_database.document_plan("insert", tag="wrong-update-kind")
    assert plan.direct_plan is not None
    altered_update = replace(
        plan.direct_plan.update,
        update_kind=UpdateKind.REPLACE,
    )
    altered_direct = replace(
        plan.direct_plan,
        update=altered_update,
        deactivated_chunk_version_ids=("d29-wrong-kind-old-chunk",),
    )
    with pytest.raises(ValidationError, match="differs from its concrete event"):
        _document_event(replace(plan, direct_plan=altered_direct))


def test_live_direct_update_manifest_is_exact_typed_metadata(
    d29_database: Any,
) -> None:
    plan = d29_database.document_plan("insert", tag="direct-manifest")
    assert plan.direct_plan is not None
    scope_id = "d29-direct-manifest-scope"
    runtime = {
        "event_manifest": _structural_payload_from_event(plan).manifest,
        "scope_claim_ids": {
            scope_id: list(plan.direct_plan.registered_claim_ids),
        },
        "failure_reason": None,
    }
    manifest = {"_groundloop_m4_runtime_v1": runtime}
    _validate_direct_update_manifest(
        manifest,
        plan,
        direct_scope_root_job_ids=(scope_id,),
        runtime_state="semantic_pending",
    )
    corruptions = (
        {**manifest, "extra": {}},
        {
            "_groundloop_m4_runtime_v1": {
                **runtime,
                "event_manifest": {**runtime["event_manifest"], "schema": "wrong"},
            }
        },
        {
            "_groundloop_m4_runtime_v1": {
                **runtime,
                "scope_claim_ids": {scope_id: []},
            }
        },
        {
            "_groundloop_m4_runtime_v1": {
                **runtime,
                "failure_reason": "unexpected failure",
            }
        },
    )
    for corrupted in corruptions:
        with pytest.raises(EventConflictError, match="manifest|metadata"):
            _validate_direct_update_manifest(
                corrupted,
                plan,
                direct_scope_root_job_ids=(scope_id,),
                runtime_state="semantic_pending",
            )
    source = inspect.getsource(_validate_direct_update_manifest)
    assert "json.dumps" not in source
    assert "repr(" not in source


def test_event_local_existing_closure_never_joins_predecessor_state(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_read_existing_document_closure")
    assert "WHERE epoch.event_id = %s" in source
    assert "groundloop_m4_publication_head" not in source
    assert "groundloop_m5_publication_head" not in source
    assert "groundloop_observation_currency" not in source
    assert "groundloop_m5_requirement_admitted_pair" not in source
    assert "legacy" in source


def test_every_document_entrypoint_checks_migration_018_first(
    function_source: Callable[[str], str],
) -> None:
    for name in (
        "_plan_document_requirement_withdrawal",
        "_hydrate_retained_requirement_open",
        "_preview_document_direct_open",
        "_derive_locked_document_open",
        "_hydrate_retained_direct_open",
        "_load_retained_document_open",
    ):
        source = function_source(name)
        barrier = source.index("_require_d29_document_route_authority(cursor)")
        event_reads = tuple(
            position
            for marker in (
                "_document_event(event)",
                "_read_existing_document_closure(cursor, event)",
                "_read_existing_document_closure(cursor, checked)",
            )
            if (position := source.find(marker)) >= 0
        )
        assert event_reads
        event_read = min(event_reads)
        assert barrier < event_read


def test_live_every_document_entrypoint_rejects_missing_or_changed_018_ledger(
    d29_database: Any,
) -> None:
    connection = d29_database.connection
    event = d29_database.document_plan("delete", tag="route-barrier")
    execution_policy = ApplicationExecutionPolicy(
        "a" * 64,
        "b" * 64,
        d29_database.database.manifest.verifier_execution_spec_hash,
    )
    identity = m5_bounded_document_withdrawal_bundle_identity()

    def entrypoints(cursor: Any) -> tuple[Callable[[], object], ...]:
        return (
            lambda: _plan_document_requirement_withdrawal(cursor, event),
            lambda: _hydrate_retained_requirement_open(
                cursor, event, expected_epoch_id=0
            ),
            lambda: _preview_document_direct_open(
                cursor,
                event,
                execution_policy=execution_policy,
            ),
            lambda: _derive_locked_document_open(
                cursor,
                event,
                execution_policy=execution_policy,
                supplied_direct_open=None,  # type: ignore[arg-type]
                supplied_requirement_withdrawal=None,  # type: ignore[arg-type]
                supplied_requirement_roots=(),
                supplied_requirement_root_set_hash="",
            ),
            lambda: _hydrate_retained_direct_open(cursor, event, expected_epoch_id=0),
            lambda: _load_retained_document_open(cursor, event, expected_epoch_id=0),
        )

    exact = {
        "bundle_id": identity.bundle_id,
        "bundle_sha256": identity.bundle_sha256,
        "migration_sha256": identity.migration_sha256,
        "oracle_sha256": identity.oracle_sha256,
        "prerequisite_sha256": identity.prerequisite_sha256,
    }
    for mutation in ("missing", *exact):
        connection.execute(f"SAVEPOINT d29_route_barrier_{mutation}")
        try:
            connection.execute(
                "ALTER TABLE groundloop_m5_schema_bundle "
                "DISABLE TRIGGER groundloop_m5_schema_bundle_immutable"
            )
            if mutation == "missing":
                connection.execute(
                    "DELETE FROM groundloop_m5_schema_bundle WHERE bundle_id = %s",
                    (identity.bundle_id,),
                )
            else:
                accepted = exact[mutation]
                changed = (
                    f"{accepted}-different"
                    if mutation == "bundle_id"
                    else ("0" * 64 if accepted != "0" * 64 else "1" * 64)
                )
                connection.execute(
                    sql.SQL(
                        "UPDATE groundloop_m5_schema_bundle SET {} = %s "
                        "WHERE bundle_id = %s"
                    ).format(sql.Identifier(mutation)),
                    (changed, identity.bundle_id),
                )
            with connection.cursor() as cursor:
                for invoke in entrypoints(cursor):
                    with pytest.raises(
                        M5BoundedDocumentWithdrawalBundleError,
                        match="exact accepted five-field migration-018 ledger row",
                    ):
                        invoke()
        finally:
            connection.execute(f"ROLLBACK TO SAVEPOINT d29_route_barrier_{mutation}")
            connection.execute(f"RELEASE SAVEPOINT d29_route_barrier_{mutation}")


def test_structural_sidecars_are_locked_and_exact_compared(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_validated_structural_source_chunks")
    for relation in (
        "groundloop_document",
        "groundloop_document_version",
        "groundloop_chunk_version",
        "groundloop_chunk_provenance",
        "groundloop_m4_document_metadata_overlay",
        "groundloop_m4_structural_deactivation",
    ):
        assert relation in source
    assert source.count("FOR UPDATE") >= 6
    assert "inserted.source_uri" in source
    assert "inserted.chunker_artifact_id" in source
    assert "inserted.chunker_input_hash" in source
    assert "DocumentVersion(" in source
    assert "ChunkVersion(" in source
    assert "InsertedDocument(" in source
    assert "deactivated document content hash" in source
    assert "deactivated chunker input hash" in source
    assert "<= closure.previous_epoch_id" in source
    assert "deactivated document chunks have inconsistent provenance" in source


def test_predecessor_authority_binds_heads_epoch_runtime_and_snapshots(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_predecessor_snapshot_ids")
    for relation in (
        "groundloop_m4_publication_head",
        "groundloop_m5_publication_head",
        "groundloop_epoch",
        "groundloop_m5_runtime_epoch",
        "groundloop_m5_requirement_registry_snapshot",
        "groundloop_m5_active_chunk_snapshot",
        "groundloop_m5_activation",
    ):
        assert relation in source
    assert "m5_head.sealed_revision" in source
    assert '("committed", "sealed", "complete", "strict")' in source
    assert "terminal_at" in source
    assert "structural_event_id" in source
    assert "epoch.event_id" in source
    assert "epoch.payload_hash" in source
    assert "_load_canonical_terminal_result" in source
    assert "M5ReplayedOutcome.SEALED" in source
    assert "1 <= int(registry[2]) <= predecessor_epoch_id" in source
    assert "1 <= int(chunks[2]) <= predecessor_epoch_id" in source
    assert "int(registry[2]) == predecessor_epoch_id" not in source
    assert "int(chunks[2]) == predecessor_epoch_id" not in source
    assert "FOR UPDATE" not in source
    assert "FOR KEY SHARE" not in source


def test_bootstrap_provenance_never_reacquires_an_earlier_tier_lock(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_lock_d29_bootstrap_provenance")
    assert "groundloop_m5_activation" in source
    assert "groundloop_epoch AS base" in source
    assert "FOR UPDATE" not in source
    assert "FOR KEY SHARE" not in source


def test_currency_interval_start_is_not_equated_with_production_epoch(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_lock_d29_observation_authority")
    preview = function_source("_observation_edges")
    assert "published[1])) != verifier.produced_epoch_id" not in source
    assert "published[1])) != direct_verifier.produced_epoch_id" not in source
    assert "current[1])) != verifier.installed_revision" not in source
    assert "current[1])) != direct_verifier.installed_revision" not in source
    assert "current[1])) > bootstrap.base_revision" not in source
    assert "current[1])) > direct_bootstrap.base_revision" not in source
    assert source.count("type(current[5]) is not int") == 1
    assert source.count("int(current[5]) < 0") == 1
    locator = function_source("_gather_d29_locator_authority")
    assert "type(installed_revision) is not int" in locator
    assert "currency.installed_revision" in source
    assert "int(row[1]) > int(activation[1])" not in preview
    assert "int(row[1]) < 0" in preview
    assert "int(str(observation[12])) != verifier.produced_epoch_id" in source
    dynamic = function_source("_validate_d30_dynamic_claim")
    assert "int(str(observation[12])) != currency.installed_revision" in dynamic


def test_live_predecessor_accepts_reused_snapshots_and_rejects_bad_headers(
    d29_database: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = d29_database.connection
    registry_digest = (
        d29_database.database.requirement_snapshot.requirement_registry_snapshot_digest
    )
    chunk_digest = d29_database.database.chunk_snapshot.active_chunk_snapshot_digest

    connection.execute("SAVEPOINT d29_missing_predecessor_result")
    try:
        predecessor_epoch_id = _install_test_typed_predecessor(
            d29_database,
            label="missing-result",
            registry_digest=registry_digest,
            chunk_digest=chunk_digest,
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="canonical sealed result"):
                _predecessor_snapshot_ids(cursor, predecessor_epoch_id)
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_missing_predecessor_result")
        connection.execute("RELEASE SAVEPOINT d29_missing_predecessor_result")

    expected_epoch = {"value": 0}

    def canonical_result(
        _cursor: object, *, structural_event_id: str, payload_hash: str
    ) -> SimpleNamespace:
        assert structural_event_id.startswith("d29-store-predecessor-")
        assert len(payload_hash) == 64
        return SimpleNamespace(
            epoch_id=expected_epoch["value"],
            replayed_outcome=postgres_withdrawal.M5ReplayedOutcome.SEALED,
        )

    monkeypatch.setattr(
        postgres_withdrawal,
        "_load_canonical_terminal_result",
        canonical_result,
    )

    connection.execute("SAVEPOINT d29_reused_snapshot")
    try:
        predecessor_epoch_id = _install_test_typed_predecessor(
            d29_database,
            label="reused-snapshot",
            registry_digest=registry_digest,
            chunk_digest=chunk_digest,
        )
        expected_epoch["value"] = predecessor_epoch_id
        with connection.cursor() as cursor:
            assert _predecessor_snapshot_ids(cursor, predecessor_epoch_id) == (
                registry_digest,
                chunk_digest,
            )
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_reused_snapshot")
        connection.execute("RELEASE SAVEPOINT d29_reused_snapshot")

    connection.execute("SAVEPOINT d29_future_snapshot")
    try:
        base_epoch_id = d29_database.database.base.epoch_id
        predecessor_epoch_id = _install_test_typed_predecessor(
            d29_database,
            label=f"future-snapshot-{base_epoch_id}",
            registry_digest=registry_digest,
            chunk_digest=chunk_digest,
        )
        expected_epoch["value"] = predecessor_epoch_id
        future_event = "d29-store-future-snapshot-creator"
        future = connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode
            ) VALUES (%s, %s, 0, 'received', 'pending', 'pending', 'provisional')
            RETURNING epoch_id
            """,
            (future_event, hashlib.sha256(future_event.encode()).hexdigest()),
        ).fetchone()
        assert future is not None
        future_epoch_id = int(future[0])
        assert future_epoch_id > predecessor_epoch_id
        # Move the immutable creator coordinate into the future while keeping
        # the row otherwise schema-valid; the store must reject it itself.
        connection.execute(
            "ALTER TABLE groundloop_m5_requirement_registry_snapshot "
            "DISABLE TRIGGER groundloop_m5_requirement_snapshot_immutable"
        )
        connection.execute(
            """
            UPDATE groundloop_m5_requirement_registry_snapshot
            SET created_epoch_id = %s
            WHERE requirement_registry_snapshot_digest = %s
            """,
            (future_epoch_id, registry_digest),
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="snapshots changed"):
                _predecessor_snapshot_ids(cursor, predecessor_epoch_id)
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_future_snapshot")
        connection.execute("RELEASE SAVEPOINT d29_future_snapshot")

    connection.execute("SAVEPOINT d29_corrupt_snapshot_header")
    try:
        predecessor_epoch_id = _install_test_typed_predecessor(
            d29_database,
            label="corrupt-snapshot-header",
            registry_digest=registry_digest,
            chunk_digest=chunk_digest,
        )
        expected_epoch["value"] = predecessor_epoch_id
        constraint = connection.execute(
            """
            SELECT constraint_row.conname
            FROM pg_constraint AS constraint_row
            WHERE constraint_row.conrelid =
                  'groundloop_m5_active_chunk_snapshot'::regclass
              AND pg_get_constraintdef(constraint_row.oid)
                  LIKE 'CHECK ((normalizer_id =%'
            """
        ).fetchone()
        assert constraint is not None
        connection.execute(
            "ALTER TABLE groundloop_m5_active_chunk_snapshot "
            "DISABLE TRIGGER groundloop_m5_chunk_snapshot_immutable"
        )
        connection.execute(
            sql.SQL(
                "ALTER TABLE groundloop_m5_active_chunk_snapshot DROP CONSTRAINT {}"
            ).format(sql.Identifier(str(constraint[0])))
        )
        connection.execute(
            """
            UPDATE groundloop_m5_active_chunk_snapshot
            SET normalizer_id = 'corrupt-normalizer'
            WHERE active_chunk_snapshot_digest = %s
            """,
            (chunk_digest,),
        )
        with connection.cursor() as cursor:
            with pytest.raises(EventConflictError, match="snapshots changed"):
                _predecessor_snapshot_ids(cursor, predecessor_epoch_id)
    finally:
        connection.execute("ROLLBACK TO SAVEPOINT d29_corrupt_snapshot_header")
        connection.execute("RELEASE SAVEPOINT d29_corrupt_snapshot_header")
