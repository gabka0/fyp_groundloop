"""Genuine cursor-local J33 staging followed by rollback, not C1 publication."""

from __future__ import annotations

import copy
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.m4.contracts import UpdateKind, stable_m4_digest
from groundloop.m4.evaluation_overlay import (
    EvaluationTransition,
    EvaluationTransitionKind,
)
from groundloop.m4.pipeline import M4ExecutionMode, PostgresM4ApplicationPorts
from groundloop.m4.runtime.epoch import RuntimeEpochState
from groundloop.m5.runtime import postgres_document_seal as seal
from tests.m5.postgres_runtime import test_semantic_readiness as r33
from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot
from tests.m5.postgres_runtime.d33_document_completion import (
    test_document_open_prerequisite as o33,
)

OpenDatabase = o33.OpenDatabase
open_db = o33.open_db


class _RollbackAdapter(RuntimeError):
    pass


def _ready(db: OpenDatabase) -> tuple[Any, int, int]:
    plan, opened, revision = r33._r33_prepare(db, "semantic_complete")
    completed = r33._edge(
        db.connection,
        opened.epoch_id,
        plan.structural_event_id,
        "semantic_complete",
        revision,
    )
    assert not completed.replay and completed.anchor.anchor_revision == revision + 1
    assert r33._point(db.connection, opened.epoch_id) == (
        revision + 1,
        "complete",
        "complete",
        "semantic_complete",
        revision + 1,
        0,
        0,
        0,
    )
    return plan, opened.epoch_id, revision + 1


def _cache(ports: PostgresM4ApplicationPorts) -> Any:
    def engine(value: Any) -> Any:
        return (
            value.claim_states,
            value.answer_states,
            value.certificates,
            value._state_revision,
            value.last_stats,
        )

    return copy.deepcopy(
        (
            ports._working_repository.export_snapshot(),
            engine(ports._working_engine),
            ports._published_repository.export_snapshot(),
            engine(ports._published_engine),
            ports._active_epoch_id,
            tuple(
                id(x)
                for x in (
                    ports._working_repository,
                    ports._working_engine,
                    ports._published_repository,
                    ports._published_engine,
                )
            ),
        )
    )


@contextmanager
def _principal(
    db: OpenDatabase, nonowner: bool
) -> Iterator[PostgresM4ApplicationPorts]:
    if not nonowner:
        yield db.ports
        return
    address = SimpleNamespace(
        connection=db.connection, dsn=db.dsn, schema_name=db.schema
    )
    with r33._runtime_login(address) as connection:
        connection.autocommit = True
        ports = PostgresM4ApplicationPorts(
            connection, structural_payloads={}, execution_mode=M4ExecutionMode.MEASURED
        )
        ports._hydrate_direct_working_cache(db.ports._active_epoch_id)
        assert (
            connection.execute("SELECT session_user,current_user").fetchone()[0]
            != (db.connection.execute("SELECT current_user").fetchone()[0])
        )
        yield ports


def _sql_text(query: Any, connection: Connection[Any]) -> str:
    return (
        query.as_string(connection) if isinstance(query, sql.Composable) else str(query)
    )


def _mutation_cursor(control: dict[str, Any]) -> Any:
    class MutationCursor(psycopg.Cursor):
        def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
            result = super().execute(query, params, **kwargs)
            source = _sql_text(query, self.connection).lstrip()
            if source.upper().startswith(("INSERT ", "UPDATE ", "DELETE ")):
                control["records"].append((source, self.rowcount, params))
                if control.get("target") == len(control["records"]):
                    raise _RollbackAdapter("actual adapter mutation cut")
            return result

    return MutationCursor


@pytest.mark.parametrize("open_db", (False, True), indirect=True)
@pytest.mark.parametrize("nonowner", (False, True))
def test_genuine_adapter_stages_native_two_clock_seal_and_rolls_back(
    open_db: OpenDatabase,
    nonowner: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = open_db
    plan, epoch, revision = _ready(db)
    with _principal(db, nonowner) as ports:
        connection = ports.connection
        before, cache = database_snapshot(connection), _cache(ports)
        calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        point_calls: list[tuple[Any, ...]] = []
        native_point = ports.runtime_store.validate_seal_point_local

        def observe_point(*args: Any, **kwargs: Any) -> Any:
            point_calls.append(args)
            return native_point(*args, **kwargs)

        monkeypatch.setattr(
            ports.runtime_store, "validate_seal_point_local", observe_point
        )
        for name in (
            "_promote_structural_overlay",
            "_promote_observation_currency",
            "_publish_grounding_states",
        ):
            native = getattr(ports, name)

            def observe(
                *args: Any, _name: str = name, _native: Any = native, **kwargs: Any
            ) -> Any:
                calls.append((_name, args, kwargs))
                return _native(*args, **kwargs)

            monkeypatch.setattr(ports, name, observe)
        native_transition = ports.evaluation_store.apply_transition_local

        def observe_transition(*args: Any, **kwargs: Any) -> Any:
            calls.append(("apply_transition_local", args, kwargs))
            return native_transition(*args, **kwargs)

        monkeypatch.setattr(
            ports.evaluation_store, "apply_transition_local", observe_transition
        )
        control: dict[str, Any] = {"records": []}
        with pytest.raises(
            _RollbackAdapter, match="intentional complete adapter rollback"
        ):
            with (
                connection.transaction(),
                _mutation_cursor(control)(connection) as cursor,
            ):
                publication = seal._stage_job_free_document_seal_local(
                    ports, cursor, epoch, revision, plan.direct_plan.update
                )
                assert publication.epoch_id == epoch
                assert publication.publication_id == stable_m4_digest(
                    "m4-publication-v1", str(epoch)
                )
                assert [x[0] for x in calls] == [
                    "_promote_structural_overlay",
                    "_promote_observation_currency",
                    "_publish_grounding_states",
                    "apply_transition_local",
                ]
                assert all(args[0] is cursor for _, args, _ in calls)
                assert point_calls == [(cursor, epoch, revision)]
                assert calls[2][1][1:] == (epoch, revision + 1, plan.direct_plan.update)
                assert calls[2][2] == {"emit_public_deltas": False}
                transition = calls[3][1][2]
                assert transition == EvaluationTransition(
                    stable_m4_digest("m4-evaluation-seal-v1", str(epoch)),
                    1,
                    kind=EvaluationTransitionKind.SEAL,
                )
                assert cursor.execute(
                    "SELECT lifecycle_state,default_evaluation_state,"
                    "confirmed_as_of_epoch,open_discovery_scope_count,revision "
                    "FROM groundloop_m4_evaluation_epoch_counter WHERE epoch_id=%s",
                    (epoch,),
                ).fetchone() == ("sealed", "complete", epoch, 0, 2)
                assert cursor.execute(
                    "SELECT from_revision,to_revision,override_rows_written "
                    "FROM groundloop_m4_evaluation_counter_transition "
                    "WHERE epoch_id=%s",
                    (epoch,),
                ).fetchall() == [(1, 2, 0)]
                assert cursor.execute(
                    "SELECT revision,semantic_status,evaluation_state,publication_mode "
                    "FROM groundloop_epoch WHERE epoch_id=%s",
                    (epoch,),
                ).fetchone() == (revision, "complete", "complete", "provisional")
                after = database_snapshot(connection)
                changed = {name for name, rows in after if dict(before)[name] != rows}
                assert changed == {
                    "groundloop_document_version",
                    "groundloop_chunk_version",
                    "groundloop_m4_execution_accounting",
                    "groundloop_m4_evaluation_epoch_counter",
                    "groundloop_m4_evaluation_counter_transition",
                }
                assert (
                    dict(before)["groundloop_status_delta"]
                    == dict(after)["groundloop_status_delta"]
                )
                assert _cache(ports) == cache
                writes = control["records"]
                assert (
                    sum(
                        "UPDATE groundloop_document_version" in q and n == 1
                        for q, n, _ in writes
                    )
                    == 1
                )
                assert (
                    sum(
                        "UPDATE groundloop_chunk_version" in q and n == 2
                        for q, n, _ in writes
                    )
                    == 1
                )
                assert all(n >= 0 for _, n, _ in writes)
                cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
                raise _RollbackAdapter("intentional complete adapter rollback")
        assert database_snapshot(connection) == before
        assert _cache(ports) == cache


@pytest.mark.parametrize("open_db", (False, True), indirect=True)
def test_unchanged_old_wrapper_reproduces_revision_incompatibility(
    open_db: OpenDatabase,
) -> None:
    db = open_db
    plan, epoch, revision = _ready(db)
    before, cache = database_snapshot(db.connection), _cache(db.ports)
    with pytest.raises(ValidationError, match="revisions are not synchronized"):
        with db.connection.transaction(), db.connection.cursor() as cursor:
            db.ports._stage_direct_seal_local(
                cursor, epoch, revision, plan.direct_plan.update
            )
    assert database_snapshot(db.connection) == before and _cache(db.ports) == cache


@pytest.mark.parametrize("open_db", (False, True), indirect=True)
@pytest.mark.parametrize("nonowner", (False, True))
def test_every_actual_adapter_mutation_and_native_named_cut_rolls_back_then_retries(
    open_db: OpenDatabase,
    nonowner: bool,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    db = open_db
    plan, epoch, revision = _ready(db)
    with _principal(db, nonowner) as ports:
        connection = ports.connection
        before, cache = database_snapshot(connection), _cache(ports)
        points: list[str] = []
        monkeypatch.setattr(ports, "_failure_injector", points.append)
        discovery: dict[str, Any] = {"records": []}
        with pytest.raises(_RollbackAdapter):
            with (
                connection.transaction(),
                _mutation_cursor(discovery)(connection) as cursor,
            ):
                seal._stage_job_free_document_seal_local(
                    ports, cursor, epoch, revision, plan.direct_plan.update
                )
                raise _RollbackAdapter("discover actual mutation positions")
        assert points == [
            "publication_store_seal_checked",
            "publication_structure_promoted",
            "publication_currency_promoted",
            "publication_states_written",
            "publication_evaluation_promoted",
        ]
        assert len(discovery["records"]) == 10
        assert database_snapshot(connection) == before and _cache(ports) == cache
        for target in range(1, len(discovery["records"]) + 1):
            control: dict[str, Any] = {"records": [], "target": target}
            with pytest.raises(_RollbackAdapter, match="actual adapter mutation cut"):
                with (
                    connection.transaction(),
                    _mutation_cursor(control)(connection) as cursor,
                ):
                    seal._stage_job_free_document_seal_local(
                        ports, cursor, epoch, revision, plan.direct_plan.update
                    )
            assert len(control["records"]) == target
            assert database_snapshot(connection) == before and _cache(ports) == cache
        for target in tuple(points[:5]):

            def cut(point: str, _target: str = target) -> None:
                if point == _target:
                    raise _RollbackAdapter("native named adapter cut")

            monkeypatch.setattr(ports, "_failure_injector", cut)
            with pytest.raises(_RollbackAdapter, match="native named adapter cut"):
                with connection.transaction(), connection.cursor() as cursor:
                    seal._stage_job_free_document_seal_local(
                        ports, cursor, epoch, revision, plan.direct_plan.update
                    )
            assert database_snapshot(connection) == before and _cache(ports) == cache
        monkeypatch.setattr(ports, "_failure_injector", None)
        with pytest.raises(_RollbackAdapter, match="genuine retry completed"):
            with connection.transaction(), connection.cursor() as cursor:
                seal._stage_job_free_document_seal_local(
                    ports, cursor, epoch, revision, plan.direct_plan.update
                )
                cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
                raise _RollbackAdapter("genuine retry completed")
        assert database_snapshot(connection) == before and _cache(ports) == cache
        request.node.user_properties.extend(
            (
                ("actual_mutation_cuts", len(discovery["records"])),
                ("native_named_cuts", 5),
            )
        )


@pytest.mark.parametrize("open_db", (False, True), indirect=True)
def test_duplicate_adapter_does_not_repromote_and_native_seal_replay_is_zero_dml(
    open_db: OpenDatabase,
) -> None:
    db = open_db
    plan, epoch, revision = _ready(db)
    before, cache = database_snapshot(db.connection), _cache(db.ports)
    control: dict[str, Any] = {"records": []}
    with pytest.raises(_RollbackAdapter):
        with (
            db.connection.transaction(),
            _mutation_cursor(control)(db.connection) as cursor,
        ):
            seal._stage_job_free_document_seal_local(
                db.ports, cursor, epoch, revision, plan.direct_plan.update
            )
            staged = database_snapshot(db.connection)
            writes = len(control["records"])
            with pytest.raises(EventConflictError, match="original active counter"):
                seal._stage_job_free_document_seal_local(
                    db.ports, cursor, epoch, revision, plan.direct_plan.update
                )
            assert len(control["records"]) == writes
            assert database_snapshot(db.connection) == staged
            canonical = EvaluationTransition(
                stable_m4_digest("m4-evaluation-seal-v1", str(epoch)),
                1,
                kind=EvaluationTransitionKind.SEAL,
            )
            replay = db.ports.evaluation_store.apply_transition_local(
                cursor, epoch, canonical
            )
            assert replay.replayed and (replay.from_revision, replay.to_revision) == (
                1,
                2,
            )
            for transition in (
                replace(canonical, expected_revision=revision),
                replace(canonical, kind=EvaluationTransitionKind.FAIL),
                replace(canonical, transition_id="another-seal"),
            ):
                with pytest.raises((EventConflictError, InvalidEventError)):
                    db.ports.evaluation_store.apply_transition_local(
                        cursor, epoch, transition
                    )
                assert len(control["records"]) == writes
                assert database_snapshot(db.connection) == staged
            assert _cache(db.ports) == cache
            raise _RollbackAdapter("no duplicate promotion")
    assert database_snapshot(db.connection) == before and _cache(db.ports) == cache


@pytest.mark.parametrize("open_db", (False, True), indirect=True)
def test_each_caller_identity_cache_and_context_conflict_rejects_before_dml(
    open_db: OpenDatabase,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    db = open_db
    plan, epoch, revision = _ready(db)
    connection, ports = db.connection, db.ports
    before, cache = database_snapshot(connection), _cache(ports)
    updates = [
        replace(plan.direct_plan.update, **{name: value})
        for name, value in (
            ("event_id", "another-event"),
            ("payload_hash", "f" * 64),
            ("previous_published_epoch_id", db.base.epoch_id + 100),
            ("candidate_policy_id", "another-policy"),
            ("update_kind", UpdateKind.REPLACE),
        )
    ]
    cases = [(epoch, revision, u) for u in updates] + [
        (epoch, revision + 1, plan.direct_plan.update),
        (epoch + 100000, revision, plan.direct_plan.update),
    ]
    for e, n, update in cases:
        control: dict[str, Any] = {"records": []}
        with pytest.raises((EventConflictError, InvalidEventError, ValidationError)):
            with (
                connection.transaction(),
                _mutation_cursor(control)(connection) as cursor,
            ):
                seal._stage_job_free_document_seal_local(ports, cursor, e, n, update)
        assert not control["records"]
        assert database_snapshot(connection) == before and _cache(ports) == cache
    for name, value in (
        ("_active_epoch_id", None),
        ("_active_epoch_id", epoch + 1),
        ("_working_repository", object()),
        ("_working_engine", object()),
        ("_working_repository", ports._published_repository),
        ("execution_mode", M4ExecutionMode.AUDIT),
    ):
        control = {"records": []}
        with monkeypatch.context() as patch:
            patch.setattr(ports, name, value)
            with pytest.raises(
                (EventConflictError, InvalidEventError, ValidationError)
            ):
                with (
                    connection.transaction(),
                    _mutation_cursor(control)(connection) as cursor,
                ):
                    seal._stage_job_free_document_seal_local(
                        ports, cursor, epoch, revision, plan.direct_plan.update
                    )
            assert not control["records"]
        assert database_snapshot(connection) == before and _cache(ports) == cache
    with connection.cursor() as cursor:
        with pytest.raises(InvalidEventError, match="active transaction"):
            seal._stage_job_free_document_seal_local(
                ports, cursor, epoch, revision, plan.direct_plan.update
            )
    for settings in (
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "SET TRANSACTION READ ONLY",
    ):
        with pytest.raises(InvalidEventError):
            with connection.transaction(), connection.cursor() as cursor:
                cursor.execute(settings)
                seal._stage_job_free_document_seal_local(
                    ports, cursor, epoch, revision, plan.direct_plan.update
                )
    with (
        r33._r33_connection(db) as other,
        other.transaction(),
        other.cursor() as cursor,
    ):
        with pytest.raises(ValidationError, match="connection differs"):
            seal._stage_job_free_document_seal_local(
                ports, cursor, epoch, revision, plan.direct_plan.update
            )
    assert database_snapshot(connection) == before and _cache(ports) == cache
    request.node.user_properties.append(("identity_cache_context_faults", 17))


@pytest.mark.parametrize("open_db", (False, True), indirect=True)
def test_document_retained_wire_faults_reject_without_mutations(
    open_db: OpenDatabase,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    """Read-boundary negatives only; immutable guards are never relaxed."""
    db = open_db
    plan, epoch, revision = _ready(db)
    before, cache = database_snapshot(db.connection), _cache(db.ports)
    cases: list[tuple[str, str, Any]] = [
        ("groundloop_m5_update", "previous_published_epoch_id", 0),
        ("groundloop_m4_update", "update_kind", "replace"),
        ("groundloop_m4_update", "previous_published_epoch_id", 0),
        ("groundloop_m4_update", "candidate_policy_id", "other-policy"),
        ("groundloop_m4_update", "registry_snapshot_id", "absent"),
        (
            "groundloop_m4_update",
            "manifest._groundloop_m4_runtime_v1.failure_reason",
            "bad",
        ),
        (
            "groundloop_m4_update",
            "manifest._groundloop_m4_runtime_v1.event_manifest.deactivated_document_version_id",
            "absent",
        ),
        ("groundloop_m4_claim_registry_snapshot", "claim_count", 999),
        ("groundloop_m4_claim_registry_snapshot", "claim_set_hash", "f" * 64),
        ("groundloop_m4_claim_registry_member", "member_ordinal", 999),
        ("groundloop_m5_requirement_registry_snapshot", "requirement_count", 999),
        ("groundloop_m5_active_chunk_snapshot", "chunk_count", 999),
        ("groundloop_m5_active_chunk_snapshot", "normalizer_id", "other"),
        ("groundloop_m5_runtime_work_contribution", "work_digest", "f" * 64),
        ("groundloop_m5_runtime_work_contribution", "public_delta_count", 1),
        ("groundloop_chunk_version", "chunk_to", None),
        ("groundloop_chunk_version", "input_hash", None),
        ("groundloop_m4_structural_deactivation", "missing_row", None),
        ("groundloop_m4_structural_deactivation", "extra_row", None),
        ("groundloop_m4_document_metadata_overlay", "phantom", None),
    ]
    for name in (
        "direct_root_job_ids",
        "direct_scope_root_job_ids",
        "direct_fallback_claim_ids",
    ):
        for value in (None, ["extra"], [None], ["extra", "extra"], "REMOVE_KEY"):
            cases.append(
                (
                    "groundloop_m5_update",
                    "manifest.m5_d29_document_declaration_v1." + name,
                    value,
                )
            )
    if db.group is not None:
        cases.extend(
            [
                ("groundloop_m5_semantic_job", "job_state", "running"),
                ("groundloop_m5_semantic_job", "completed_revision", None),
                ("groundloop_m5_semantic_job", "cancellation_reason", "epoch_failed"),
                ("groundloop_m5_discovery_scope", "scope_state", "open"),
                (
                    "groundloop_m5_requirement_root_provenance",
                    "provenance_digest",
                    "f" * 64,
                ),
            ]
        )
    for relation, field, value in cases:
        with monkeypatch.context() as patch:
            touched = r33._r33_read_fault(patch, relation, field, value)
            control: dict[str, Any] = {"records": []}
            with pytest.raises(
                (EventConflictError, InvalidEventError, ValidationError)
            ):
                with (
                    db.connection.transaction(),
                    _mutation_cursor(control)(db.connection) as cursor,
                ):
                    seal._stage_job_free_document_seal_local(
                        db.ports, cursor, epoch, revision, plan.direct_plan.update
                    )
            assert touched[0] > 0, (relation, field)
            assert not control["records"], (relation, field)
        assert database_snapshot(db.connection) == before and _cache(db.ports) == cache
    request.node.user_properties.append(("retained_wire_faults", len(cases)))


@pytest.mark.parametrize("open_db", (False, True), indirect=True)
def test_all_direct_counter_history_and_header_wire_faults_reject(
    open_db: OpenDatabase,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    db = open_db
    plan, epoch, revision = _ready(db)
    before, cache = database_snapshot(db.connection), _cache(db.ports)
    original = r33.readiness._read_header_pair
    cases: list[tuple[str, str, Any]] = [
        ("counter", "epoch_id", epoch + 1),
        ("counter", "declaration_hash", "f" * 64),
        ("counter", "lifecycle_state", "sealed"),
        ("counter", "default_evaluation_state", "pending"),
        ("counter", "confirmed_as_of_epoch", epoch),
        ("counter", "open_discovery_scope_count", 1),
        ("counter", "revision", revision),
        ("point", "transition_count", 1),
        ("point", "direct_history", True),
        ("point", "owner_counter_bad", True),
        ("point", "answer_counter_bad", True),
        ("seal", "transition_id", "another-transition"),
        ("runtime", "runtime_state", "semantic_pending"),
        ("runtime", "open_work_count", 1),
        ("runtime", "open_scope_count", 1),
        ("runtime", "blocking_failure_count", 1),
        ("base", "semantic_status", "pending"),
        ("base", "publication_mode", "strict"),
    ]
    for surface, field, value in cases:
        touched = [0]

        def fault(
            *args: Any,
            _surface: str = surface,
            _field: str = field,
            _value: Any = value,
            _touched: list[int] = touched,
            **kwargs: Any,
        ) -> Any:
            base, runtime = original(*args, **kwargs)
            base, runtime = copy.deepcopy(base), copy.deepcopy(runtime)
            point = runtime["_r33_document_point"]
            target = {
                "counter": point["counter"],
                "point": point,
                "seal": point["seal"],
                "runtime": runtime,
                "base": base,
            }[_surface]
            target[_field] = _value
            _touched[0] += 1
            return base, runtime

        control: dict[str, Any] = {"records": []}
        with monkeypatch.context() as patch:
            patch.setattr(r33.readiness, "_read_header_pair", fault)
            with pytest.raises(
                (EventConflictError, InvalidEventError, ValidationError)
            ):
                with (
                    db.connection.transaction(),
                    _mutation_cursor(control)(db.connection) as cursor,
                ):
                    seal._stage_job_free_document_seal_local(
                        db.ports, cursor, epoch, revision, plan.direct_plan.update
                    )
            assert touched[0] == 1 and not control["records"]
        assert database_snapshot(db.connection) == before and _cache(db.ports) == cache
    request.node.user_properties.append(("header_counter_wire_faults", len(cases)))


@pytest.mark.parametrize("open_db", (False,), indirect=True)
def test_catalog_and_lookup_authority_fail_before_promotion(
    open_db: OpenDatabase,
) -> None:
    from groundloop.postgres.migrations import M5DocumentUpdateCompletionBundleError

    db = open_db
    plan, epoch, revision = _ready(db)
    before, cache = database_snapshot(db.connection), _cache(db.ports)
    with pytest.raises(M5DocumentUpdateCompletionBundleError):
        with db.connection.transaction(), db.connection.cursor() as cursor:
            cursor.execute(
                "CREATE OR REPLACE FUNCTION "
                "groundloop_m5_validate_event_result_children() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                "RAISE EXCEPTION 'owned catalog negative'; END; $$"
            )
            seal._stage_job_free_document_seal_local(
                db.ports, cursor, epoch, revision, plan.direct_plan.update
            )
    assert database_snapshot(db.connection) == before and _cache(db.ports) == cache


@pytest.mark.parametrize("open_db", (False,), indirect=True)
def test_native_prior_commit_probe_rejects_top_and_subcommitted_child(
    open_db: OpenDatabase,
) -> None:
    """Native MVCC predicate probe ONLY, not an authored readiness positive."""
    db = open_db
    connection = db.connection
    # A fixture-owned temporary probe never shadows a GroundLoop relation and
    # is not used as adapter/source authority. It tests the exact native XID
    # scalar predicate against own and separately committed inserts.
    connection.execute(
        "CREATE TEMP TABLE j33_native_transaction_probe(probe_id integer)"
    )
    connection.execute("INSERT INTO j33_native_transaction_probe VALUES (0)")
    connection.execute(
        "CREATE TABLE j33_native_later_transaction_probe(probe_id integer)"
    )
    query = (
        "SELECT xmin::pg_catalog.text, "
        "pg_catalog.pg_current_xact_id_if_assigned()::pg_catalog.text, "
        "pg_catalog.pg_snapshot_xmax(pg_catalog.pg_current_snapshot())"
        "::pg_catalog.text "
        "FROM j33_native_transaction_probe WHERE probe_id=%s"
    )
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute("INSERT INTO j33_native_transaction_probe VALUES (1)")
        own = cursor.execute(query, (1,)).fetchone()
        assert own[0] == own[1]
        assert not seal._prior_committed_xmin(cursor, own)
        with connection.transaction():
            cursor.execute("INSERT INTO j33_native_transaction_probe VALUES (2)")
            sub = cursor.execute(query, (2,)).fetchone()
            assert sub[0] != sub[1]
            assert not seal._prior_committed_xmin(cursor, sub)
        # Release the savepoint, then another real transaction advances xmax.
        # This forces own-sub detection through native pg_xact_status, not just
        # the quiet-database xmax cutoff.
        with psycopg.connect(db.dsn, autocommit=True) as other:
            other.execute(
                sql.SQL("INSERT INTO {} VALUES (3)").format(
                    sql.Identifier(db.schema, "j33_native_later_transaction_probe")
                )
            )
        released = cursor.execute(query, (2,)).fetchone()
        assert int(released[0]) < int(released[2])
        assert not seal._prior_committed_xmin(cursor, released)
        older = cursor.execute(query, (0,)).fetchone()
        assert seal._prior_committed_xmin(cursor, older)
        later = cursor.execute(
            query.replace(
                "j33_native_transaction_probe", "j33_native_later_transaction_probe"
            ),
            (3,),
        ).fetchone()
        assert int(later[0]) > int(later[1])
        assert seal._prior_committed_xmin(cursor, later)
    # Both top and child insertions are now genuinely committed before the new
    # transaction. Readiness is NOT manufactured by this independent probe.
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute("SELECT pg_catalog.pg_current_xact_id()")
        assert all(
            seal._prior_committed_xmin(cursor, cursor.execute(query, (i,)).fetchone())
            for i in (0, 1, 2)
        )


@pytest.mark.parametrize("open_db", (False,), indirect=True)
def test_wrong_namespace_and_temporary_shadow_reject_before_promotion(
    open_db: OpenDatabase,
) -> None:
    from groundloop.postgres.migrations import M5DocumentUpdateCompletionBundleError

    db = open_db
    plan, epoch, revision = _ready(db)
    before, cache = database_snapshot(db.connection), _cache(db.ports)
    for setting in (
        sql.SQL("SET LOCAL search_path TO {},public,pg_catalog").format(
            sql.Identifier(db.schema)
        ),
        sql.SQL("SET LOCAL search_path TO public,{} ").format(
            sql.Identifier(db.schema)
        ),
    ):
        with pytest.raises(
            (
                ValidationError,
                M5DocumentUpdateCompletionBundleError,
                psycopg.errors.UndefinedTable,
            )
        ):
            with db.connection.transaction(), db.connection.cursor() as cursor:
                cursor.execute(setting)
                seal._stage_job_free_document_seal_local(
                    db.ports, cursor, epoch, revision, plan.direct_plan.update
                )
        assert database_snapshot(db.connection) == before and _cache(db.ports) == cache
    with pytest.raises(ValidationError, match="colliding temporary"):
        with db.connection.transaction(), db.connection.cursor() as cursor:
            cursor.execute("CREATE TEMP TABLE groundloop_m4_update(dummy integer)")
            seal._stage_job_free_document_seal_local(
                db.ports, cursor, epoch, revision, plan.direct_plan.update
            )
    assert database_snapshot(db.connection) == before and _cache(db.ports) == cache


@pytest.mark.parametrize("open_db", (False, True), indirect=True)
def test_unchanged_native_m4_point_checks_still_reject_every_coordination_fault(
    open_db: OpenDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = open_db
    plan, epoch, revision = _ready(db)
    before, cache = database_snapshot(db.connection), _cache(db.ports)
    native = db.ports.runtime_store.read_epoch_header_point
    for field, value in (
        ("revision", revision + 1),
        ("state", RuntimeEpochState.SEMANTIC_PENDING),
        ("open_job_count", 1),
        ("open_scope_count", 1),
    ):
        touched = [0]

        def fault(
            *args: Any,
            _field: str = field,
            _value: Any = value,
            _touched: list[int] = touched,
            **kwargs: Any,
        ) -> Any:
            point = native(*args, **kwargs)
            _touched[0] += 1
            return replace(point, **{_field: _value})

        control: dict[str, Any] = {"records": []}
        with monkeypatch.context() as patch:
            patch.setattr(db.ports.runtime_store, "read_epoch_header_point", fault)
            with pytest.raises((EventConflictError, InvalidEventError)):
                with (
                    db.connection.transaction(),
                    _mutation_cursor(control)(db.connection) as cursor,
                ):
                    seal._stage_job_free_document_seal_local(
                        db.ports, cursor, epoch, revision, plan.direct_plan.update
                    )
            assert touched[0] == 1 and not control["records"]
        assert database_snapshot(db.connection) == before and _cache(db.ports) == cache
