"""D29 retained replay, cutoff, and prospective-reservation tests."""

from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest

from groundloop.errors import EventConflictError
from groundloop.m4.application import ApplicationExecutionPolicy
from groundloop.m5.runtime import digests, postgres_withdrawal
from groundloop.m5.runtime.application import _M5D29HydrationTerminalCutoff
from groundloop.m5.runtime.postgres_withdrawal import (
    _derive_locked_document_open,
    _document_declaration_coordinates,
    _document_declaration_manifest,
    _document_requirement_roots,
    _DocumentClosure,
    _DocumentDeclarationCoordinates,
    _gather_d29_locator_authority,
    _plan_document_requirement_withdrawal,
    _preview_document_direct_open,
    _prospective_requirement_withdrawal,
    _require_coordinate_subset,
    _validate_document_manifest,
)


def _coordinates(
    *, direct_jobs: tuple[str, ...] = (), requirement_jobs: tuple[str, ...] = ()
) -> _DocumentDeclarationCoordinates:
    return _DocumentDeclarationCoordinates(
        direct_job_ids=direct_jobs,
        direct_scope_root_job_ids=(),
        requirement_job_ids=requirement_jobs,
        requirement_scope_root_job_ids=(),
    )


def test_cutoff_signal_is_exact_empty_private_type() -> None:
    signal = _M5D29HydrationTerminalCutoff()
    assert type(signal) is _M5D29HydrationTerminalCutoff
    assert signal.args == ()


def test_exact_coordinates_must_be_subset_of_reserved_superset() -> None:
    _require_coordinate_subset(
        _coordinates(direct_jobs=("direct-a",), requirement_jobs=("m5-a",)),
        _coordinates(
            direct_jobs=("direct-a", "direct-b"),
            requirement_jobs=("m5-a", "m5-b"),
        ),
    )
    with pytest.raises(EventConflictError, match="prospective reservations"):
        _require_coordinate_subset(
            _coordinates(direct_jobs=("unreserved",)),
            _coordinates(direct_jobs=("reserved",)),
        )


def test_reservation_order_is_direct_jobs_then_m5_jobs(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_lock_d29_jobs_and_reserve")
    direct = source.index("prospective_coordinates.direct_job_ids")
    existing_m5 = source.index("existing_m5_ids")
    requirement = source.index("prospective_coordinates.requirement_job_ids")
    assert direct < existing_m5 < requirement
    assert "pg_advisory_xact_lock" in function_source("_reserve_d29_coordinate")


def test_prospective_coordinates_are_store_locator_derived(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_gather_d29_locator_authority")
    assert "event.direct_plan" not in source
    assert "groundloop_m5_requirement_admitted_pair" in source
    assert "groundloop_observation_currency" in source
    assert "groundloop_candidate_frontier" in source
    assert "groundloop_published_observation_currency" not in source
    assert "valid_to_epoch IS NULL" in source
    assert "_document_declaration_coordinates" in source
    admitted = source.index("_candidate_locator_classification_coordinates")
    prospective = source.index("prospective_requirement_ids.add(requirement_id)")
    lineage = source.index("_is_sealed_lineage_owner")
    assert admitted < prospective < lineage


@pytest.mark.parametrize("classification", ("nonlineage", "inactive"))
def test_live_filtered_admitted_locators_reserve_unused_requirement_coordinates(
    d29_reservation_database: Any,
    monkeypatch: pytest.MonkeyPatch,
    classification: str,
) -> None:
    d29_database = d29_reservation_database
    connection = d29_database.connection
    base = d29_database.database.base
    manifest = d29_database.database.manifest
    event = d29_database.document_plan("delete", tag=f"prospective-{classification}")
    direct = event.direct_plan
    assert direct is not None
    source_chunks = tuple(sorted(direct.deactivated_chunk_version_ids))
    # This retained test isolates prospective requirement-coordinate
    # reservation.  Its old synthetic claim currents predate both positive
    # D30 provenance branches, so remove that unrelated invalid authority;
    # mixed valid claim/requirement location is covered by d30_store.
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
    locator_closure = SimpleNamespace(
        previous_epoch_id=base.epoch_id,
        candidate_policy=manifest,
    )
    try:
        qualifying_owner = int(str(d29_database.first_m5["epoch_id"]))
        qualifying_requirement_row = connection.execute(
            """
            SELECT subject_id, admitted_pair_digest, semantic_pair_digest
            FROM groundloop_m5_requirement_admitted_pair
            WHERE admitted_pair_digest = %s
            """,
            (str(d29_database.first_m5["admitted_pair_digest"]),),
        ).fetchone()
        assert qualifying_requirement_row is not None
        qualifying_requirement = str(qualifying_requirement_row[0])
        qualifying_digest = str(qualifying_requirement_row[1]).strip()
        qualifying_pair_digest = str(qualifying_requirement_row[2]).strip()
        excluded_fixture = d29_database.excluded_m5
        excluded_owner = int(str(excluded_fixture["epoch_id"]))
        excluded_requirement_row = connection.execute(
            """
            SELECT subject_id, admitted_pair_digest, semantic_pair_digest
            FROM groundloop_m5_requirement_admitted_pair
            WHERE admitted_pair_digest = %s
            """,
            (str(excluded_fixture["admitted_pair_digest"]),),
        ).fetchone()
        assert excluded_requirement_row is not None
        excluded_requirement = str(excluded_requirement_row[0])
        excluded_digest = str(excluded_requirement_row[1]).strip()
        excluded_pair_digest = str(excluded_requirement_row[2]).strip()
        assert excluded_owner != qualifying_owner
        assert excluded_requirement != qualifying_requirement
        assert excluded_digest != qualifying_digest
        assert str(d29_database.first_m5["chunk_version_id"]) == str(
            excluded_fixture["chunk_version_id"]
        )
        assert connection.execute(
            """
            SELECT epoch.structural_status, epoch.semantic_status,
                   epoch.evaluation_state, epoch.publication_mode,
                   runtime.runtime_state, runtime.terminal_at IS NULL
            FROM groundloop_epoch AS epoch
            JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
            WHERE epoch.epoch_id = %s
            """,
            (excluded_owner,),
        ).fetchone() == (
            "committed",
            "pending",
            "pending",
            "provisional",
            "semantic_pending",
            True,
        )
        monkeypatch.setattr(
            postgres_withdrawal,
            "_is_sealed_lineage_owner",
            (
                (lambda _cursor, owner, _predecessor: owner == qualifying_owner)
                if classification == "nonlineage"
                else (lambda *_args, **_kwargs: True)
            ),
        )
        if classification == "inactive":
            connection.execute(
                "ALTER TABLE groundloop_m5_requirement_version DISABLE TRIGGER USER"
            )
            connection.execute(
                """
                UPDATE groundloop_m5_requirement_version
                SET lifecycle_state = 'FAILED'
                WHERE requirement_version_id = %s
                """,
                (excluded_requirement,),
            )
            connection.execute(
                "ALTER TABLE groundloop_m5_requirement_version ENABLE TRIGGER USER"
            )

        restored_triggers = dict(
            connection.execute(
                """
                SELECT relation.relname, bool_and(trigger.tgenabled <> 'D')
                FROM pg_trigger AS trigger
                JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
                JOIN pg_namespace AS namespace
                  ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = current_schema()
                  AND NOT trigger.tgisinternal
                  AND relation.relname = ANY(%s)
                GROUP BY relation.relname
                ORDER BY relation.relname COLLATE "C"
                """,
                (
                    [
                        "groundloop_epoch",
                        "groundloop_m5_runtime_epoch",
                        "groundloop_m5_requirement_version",
                    ],
                ),
            ).fetchall()
        )
        assert restored_triggers == {
            "groundloop_epoch": True,
            "groundloop_m5_requirement_version": True,
            "groundloop_m5_runtime_epoch": True,
        }

        with connection.cursor() as raw_cursor:
            locator = _gather_d29_locator_authority(  # type: ignore[arg-type]
                raw_cursor,
                event,
                locator_closure,
                execution_policy=execution_policy,
                source_chunks=source_chunks,
            )

        excluded_plan = _prospective_requirement_withdrawal(
            event,
            source_chunks=source_chunks,
            requirement_ids=(excluded_requirement,),
        )
        qualifying_plan = _prospective_requirement_withdrawal(
            event,
            source_chunks=source_chunks,
            requirement_ids=(qualifying_requirement,),
        )
        excluded_roots = _document_requirement_roots(event, manifest, excluded_plan)
        qualifying_roots = _document_requirement_roots(event, manifest, qualifying_plan)
        assert len(excluded_roots) == len(qualifying_roots) == 1
        excluded_id = excluded_roots[0].job.logical_job_id
        qualifying_id = qualifying_roots[0].job.logical_job_id
        located_digests = {
            digest for _chunk_id, digest in locator.admitted_locator_keys
        }
        assert {excluded_digest, qualifying_digest} <= located_digests
        assert excluded_digest not in locator.qualifying_admitted_pair_digests
        assert qualifying_digest in locator.qualifying_admitted_pair_digests
        assert excluded_requirement not in locator.touched_requirement_ids
        assert qualifying_requirement in locator.touched_requirement_ids
        assert excluded_id in locator.prospective_coordinates.requirement_job_ids
        assert excluded_id in (
            locator.prospective_coordinates.requirement_scope_root_job_ids
        )
        assert qualifying_id in locator.prospective_coordinates.requirement_job_ids

        with connection.cursor() as cursor:
            exact_requirement = _plan_document_requirement_withdrawal(cursor, event)
            exact_direct = _preview_document_direct_open(
                cursor,
                event,
                execution_policy=execution_policy,
            )
        exact_roots = _document_requirement_roots(event, manifest, exact_requirement)
        exact_root_ids = {root.job.logical_job_id for root in exact_roots}
        assert (
            qualifying_pair_digest in exact_requirement.withdrawn_candidate_pair_digests
        )
        assert excluded_pair_digest not in (
            exact_requirement.withdrawn_candidate_pair_digests
        )
        assert qualifying_id in exact_root_ids
        assert excluded_id not in exact_root_ids
        exact_root_set_hash = digests.requirement_root_set_digest(
            root.job.logical_job_id for root in exact_roots
        )

        raw_requirement_ids = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT subject_id
                FROM groundloop_m5_requirement_admitted_pair
                WHERE chunk_version_id = ANY(%s)
                UNION
                SELECT subject_id
                FROM groundloop_observation_currency
                WHERE subject_kind = 'requirement'
                  AND chunk_version_id = ANY(%s)
                """,
                (list(source_chunks), list(source_chunks)),
            ).fetchall()
        }
        independent_prospective_plan = _prospective_requirement_withdrawal(
            event,
            source_chunks=source_chunks,
            requirement_ids=raw_requirement_ids,
        )
        independent_prospective_roots = _document_requirement_roots(
            event, manifest, independent_prospective_plan
        )
        expected_coordinates = _document_declaration_coordinates(
            exact_direct, independent_prospective_roots
        )
        assert locator.prospective_coordinates == expected_coordinates
        expected_reservations = [
            *(
                ("direct-scope", root_id)
                for root_id in expected_coordinates.direct_scope_root_job_ids
            ),
            *(
                ("requirement-scope", root_id)
                for root_id in expected_coordinates.requirement_scope_root_job_ids
            ),
            *(("direct-job", job_id) for job_id in expected_coordinates.direct_job_ids),
            *(
                ("requirement-job", job_id)
                for job_id in expected_coordinates.requirement_job_ids
            ),
        ]
        manifest_arrays = _validate_document_manifest(
            _document_declaration_manifest(exact_direct)
        )
        locked_closure = _DocumentClosure(
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
            requirement_root_set_hash=exact_root_set_hash,
            direct_root_job_ids=manifest_arrays[0],
            direct_scope_root_job_ids=manifest_arrays[1],
            direct_fallback_claim_ids=manifest_arrays[2],
        )
        timeline: list[tuple[str, str]] = []
        snapshot_validation_calls: list[None] = []
        original_reserve = postgres_withdrawal._reserve_d29_coordinate
        original_tier_10 = postgres_withdrawal._lock_d29_direct_attempts

        def validate_synthetic_event_snapshot(
            _cursor: Any, checked_event: Any, closure: Any
        ) -> None:
            assert checked_event is event
            assert closure is locked_closure
            assert closure.epoch_id == qualifying_owner
            assert closure.requirement_snapshot_digest == (
                event.requirement_registry_snapshot.requirement_registry_snapshot_digest
            )
            assert closure.chunk_snapshot_digest == (
                event.active_chunk_snapshot.active_chunk_snapshot_digest
            )
            assert timeline == []
            snapshot_validation_calls.append(None)

        def record_reservation(
            cursor: Any, *, namespace: int, kind: str, object_id: str
        ) -> None:
            assert snapshot_validation_calls == [None]
            timeline.append((kind, object_id))
            original_reserve(
                cursor,
                namespace=namespace,
                kind=kind,
                object_id=object_id,
            )

        def record_tier_10(cursor: Any, authority: Any) -> Any:
            timeline.append(("tier-10", ""))
            return original_tier_10(cursor, authority)

        monkeypatch.setattr(
            postgres_withdrawal,
            "_read_existing_document_closure",
            lambda *_args, **_kwargs: locked_closure,
        )
        monkeypatch.setattr(
            postgres_withdrawal,
            "_validated_structural_source_chunks",
            lambda *_args, **_kwargs: source_chunks,
        )
        monkeypatch.setattr(
            postgres_withdrawal,
            "_validate_d29_event_snapshot_image",
            validate_synthetic_event_snapshot,
        )
        monkeypatch.setattr(
            postgres_withdrawal,
            "_reserve_d29_coordinate",
            record_reservation,
        )
        monkeypatch.setattr(
            postgres_withdrawal,
            "_lock_d29_direct_attempts",
            record_tier_10,
        )
        with connection.cursor() as cursor:
            derived = _derive_locked_document_open(
                cursor,
                event,
                execution_policy=execution_policy,
                supplied_direct_open=exact_direct,
                supplied_requirement_withdrawal=exact_requirement,
                supplied_requirement_roots=exact_roots,
                supplied_requirement_root_set_hash=exact_root_set_hash,
            )
        assert derived == (
            exact_direct,
            exact_requirement,
            exact_roots,
            exact_root_set_hash,
        )
        assert snapshot_validation_calls == [None]
        tier_10_index = timeline.index(("tier-10", ""))
        assert timeline[:tier_10_index] == expected_reservations
        assert len(timeline[:tier_10_index]) == len(expected_reservations)
        assert qualifying_id in {root.job.logical_job_id for root in derived[2]}
        assert excluded_id not in {root.job.logical_job_id for root in derived[2]}
        assert connection.execute(
            """
            SELECT
              EXISTS (SELECT 1 FROM groundloop_m5_semantic_job
                      WHERE logical_job_id = %s),
              EXISTS (SELECT 1 FROM groundloop_m5_discovery_scope
                      WHERE root_job_id = %s)
            """,
            (excluded_id, excluded_id),
        ).fetchone() == (False, False)
    finally:
        connection.rollback()


def test_terminal_reader_reconstructs_complete_canonical_result(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_load_canonical_terminal_result")
    delegated = source + function_source("_load_terminal_work")
    for relation in (
        "groundloop_m5_event_result",
        "groundloop_m5_runtime_work",
        "groundloop_m5_event_result_delta",
        "groundloop_m5_event_result_state_reference",
        "groundloop_m5_event_timing_coverage",
    ):
        assert relation in delegated
    assert "open_event_receipt_binding_digest" in source
    assert "publication_receipt_binding_digest" in source
    assert "combined_status_delta_set_digest" in source
    assert "changed_state_set_digest" in source
    assert "logical_result_hash" in source
