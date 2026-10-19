"""Retained native-byte component checks, not committed/reconnect C2 gates."""

from typing import Any

import pytest

from groundloop.errors import EventConflictError
from groundloop.m5.runtime import postgres_matching as matching
from groundloop.m5.runtime.contracts import M5PersistedMatchingSourceKind
from tests.m5.postgres_runtime.d25_store_core.conftest import (
    authorize_and_derive_requirement_completion,
    install_prepared_matching_headers,
    retained_matching_replay_intent,
    terminalize_requirement_completion_source,
)
from tests.m5.postgres_runtime.d34_requirement_completion import (
    test_active_completion,
    test_archive_order,
)


def test_latest_older_reconnect_replays_are_zero_write_and_never_rederive(
    c2_db: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from groundloop.m5.runtime import postgres_verifier as verifier
    from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot
    from tests.m5.postgres_runtime.d34_requirement_completion.conftest import (
        capture_sql,
    )

    db = c2_db
    sources = [
        db.build(chunk, label)
        for chunk, label in zip(
            db.chunks, ("SUPPORT", "NEUTRAL", "REFUTE"), strict=True
        )
    ]
    for source in sources:
        assert not db.complete(source).exact_replay
    current = db.revision()
    before = database_snapshot(db.connection)

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail("retained C2 replay entered fresh matching/write validation")

    for name in (
        "derive_matching_transition_intent",
        "_prepare_matching_transition",
        "_authorize_matching_transition",
        "_stage_prepared_matching_transition",
        "_finalize_prepared_matching_transition",
        "_d35_transition_reader_route",
        "_d35_read_transition_projection",
    ):
        monkeypatch.setattr(matching, name, forbidden)
    monkeypatch.setattr(verifier, "_force_deferred_validation", forbidden)
    for source in reversed(sources):
        with db.runtime.reconnect() as connection:
            with capture_sql(connection) as trace:
                receipt = db.complete(
                    source,
                    connection=connection,
                    expected_revision=source.lease.resulting_revision,
                )
        assert receipt.exact_replay and receipt.resulting_revision == current
        assert receipt.transition_anchor is None
        statements = [s for s, _, _ in trace]
        assert not any(
            s.lstrip()
            .upper()
            .startswith(("INSERT", "UPDATE", "DELETE", "MERGE", "SET CONSTRAINTS"))
            for s in statements
        )
        assert not any(
            "FROM groundloop_semantic_observation" in s and "FOR UPDATE" in s
            for s in statements
        )
        assert not any(
            params
            and isinstance(params, tuple)
            and params[0] == matching._d34_ARCHIVE_NAMESPACE
            for s, params, _ in trace
            if "pg_advisory_xact_lock" in s
        )
        assert any(
            "groundloop_m5_matching_work_accumulator" in s and "FOR SHARE" in s
            for s in statements
        )
        assert database_snapshot(db.connection) == before


@pytest.mark.parametrize("c2_db", [None, "short-lease"], indirect=True)
def test_private_cancelled_or_expired_late_return_has_no_semantic_effect(
    c2_db: Any,
) -> None:
    from groundloop.m5.runtime.contracts import M5CancellationPlan, M5TerminalReason
    from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
    from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot
    from tests.m5.postgres_runtime.d34_requirement_completion.conftest import (
        capture_sql,
    )

    db = c2_db
    source = db.build(db.chunks[0])
    store = PostgresM5RuntimeStore(db.connection)
    if db.runtime.operational_config.lease_duration_ms == 5:
        successor = store.acquire_m5_job(db.runtime.epoch_id, db.revision(), source.job)
        assert successor.attempt.attempt_ordinal == 2
    else:
        cancellation = M5CancellationPlan.build(
            structural_event_id=db.runtime.plan.structural_event_id,
            epoch_id=db.runtime.epoch_id,
            cancelled_job_ids=(source.job.logical_job_id,),
            reason=M5TerminalReason.SCOPE_RETIRED,
        )
        store.cancel_m5_work_atomically(
            db.runtime.epoch_id, db.revision(), cancellation
        )
    revision = db.revision()
    semantic = db.connection.execute(
        "SELECT count(*) FROM groundloop_semantic_observation"
    ).fetchone()
    matching = db.connection.execute(
        "SELECT count(*) FROM groundloop_m5_matching_work_contribution"
    ).fetchone()
    with capture_sql(db.connection) as trace:
        receipt = db.complete(source)
    assert not receipt.exact_replay and receipt.resulting_revision == revision
    assert (
        db.connection.execute(
            "SELECT count(*) FROM groundloop_semantic_observation"
        ).fetchone()
        == semantic
    )
    assert (
        db.connection.execute(
            "SELECT count(*) FROM groundloop_m5_matching_work_contribution"
        ).fetchone()
        == matching
    )
    assert not any(
        s.lstrip().startswith(
            "SELECT groundloop_m5_authorize_persisted_matching_transition("
        )
        for s, _, _ in trace
    )
    before = database_snapshot(db.connection)
    with db.runtime.reconnect() as reader:
        assert db.complete(source, connection=reader).exact_replay
    assert database_snapshot(db.connection) == before


def test_changed_immutable_return_is_zero_write_conflict(c2_db: Any) -> None:
    from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot

    db = c2_db
    db.complete(db.build(db.chunks[0]))
    changed = db.build(db.chunks[0], "NEUTRAL", "different-return")
    before = database_snapshot(db.connection)
    with pytest.raises(EventConflictError):
        db.complete(changed)
    assert database_snapshot(db.connection) == before


@pytest.mark.parametrize("c2_db", ["short-lease"], indirect=True)
@pytest.mark.parametrize(
    "successor_state",
    [
        "running",
        "retryable_then_running",
        "completed_active",
        "completed_inactive",
        "failed_terminal",
        "cancelled",
    ],
)
def test_private_expired_successor_and_genuine_failed_epoch_disposition_matrix(
    c2_db: Any,
    successor_state: str,
) -> None:
    from groundloop.m5.runtime.contracts import (
        M5CancellationPlan,
        M5RunFailureReason,
        M5RuntimeWork,
        M5TerminalReason,
    )
    from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
    from tests.m5.postgres.helpers import sha
    from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot
    from tests.m5.postgres_runtime.d34_requirement_completion.conftest import (
        _build_c2_verifier_fixture,
        capture_sql,
    )

    db = c2_db
    source = db.build(db.chunks[0])
    store = PostgresM5RuntimeStore(db.connection)
    successor = store.acquire_m5_job(db.runtime.epoch_id, db.revision(), source.job)
    assert successor.attempt.attempt_ordinal == 2
    terminal = successor_state in {
        "completed_active",
        "completed_inactive",
        "failed_terminal",
        "cancelled",
    }
    if successor_state == "retryable_then_running":
        store.mark_m5_retryable_failure(
            db.runtime.epoch_id,
            db.revision(),
            successor,
            sha("c2-retryable-successor"),
            M5RuntimeWork(),
            None,
        )
        before = database_snapshot(db.connection)
        with pytest.raises(EventConflictError, match="nonrunning successor"):
            db.complete(source)
        assert database_snapshot(db.connection) == before
        successor = store.acquire_m5_job(db.runtime.epoch_id, db.revision(), source.job)
        assert successor.attempt.attempt_ordinal == 3
    elif successor_state in {"completed_active", "completed_inactive"}:
        current = _build_c2_verifier_fixture(
            db.runtime, lease=successor, job=source.job
        )
        if successor_state == "completed_inactive":
            with db.connection.transaction():
                db.connection.execute(
                    "UPDATE groundloop_chunk_version SET valid_to_epoch=%s "
                    "WHERE chunk_version_id=%s",
                    (db.runtime.epoch_id, db.chunks[0]),
                )
                db.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        db.complete(current)
    elif successor_state == "failed_terminal":
        store.mark_m5_terminal_failure(
            db.runtime.epoch_id,
            db.revision(),
            successor,
            M5TerminalReason.VERIFIER_ERROR,
            sha("c2-terminal-successor"),
            M5RuntimeWork(),
            None,
        )
    elif successor_state == "cancelled":
        cancellation = M5CancellationPlan.build(
            structural_event_id=db.runtime.plan.structural_event_id,
            epoch_id=db.runtime.epoch_id,
            cancelled_job_ids=(source.job.logical_job_id,),
            reason=M5TerminalReason.SCOPE_RETIRED,
        )
        store.cancel_m5_work_atomically(
            db.runtime.epoch_id, db.revision(), cancellation
        )
    if terminal:
        before = database_snapshot(db.connection)
        with pytest.raises(EventConflictError, match="nonrunning successor"):
            db.complete(source)
        assert database_snapshot(db.connection) == before
        outcome = store.fail_typed_epoch_atomically(
            db.runtime.epoch_id,
            db.revision(),
            M5RunFailureReason.VERIFIER_UNAVAILABLE,
            M5RuntimeWork(),
        )
        assert outcome.state.value == "failed"
    semantic_before = db.connection.execute(
        "SELECT observation_id,xmin::text FROM groundloop_semantic_observation "
        "ORDER BY observation_id"
    ).fetchall()
    matching_before = db.connection.execute(
        "SELECT source_id,xmin::text FROM groundloop_m5_matching_work_contribution "
        "ORDER BY epoch_id,source_kind,source_id"
    ).fetchall()
    revision = db.revision()
    with capture_sql(db.connection) as trace:
        receipt = db.complete(source)
    assert not receipt.exact_replay and receipt.resulting_revision == revision
    assert (receipt.current_terminal_logical_result_hash is not None) == terminal
    assert (receipt.transition_anchor is None) == terminal
    assert not any(
        "matching_read_transition_" in s
        or "SELECT groundloop_m5_authorize_persisted_matching_transition(" in s
        for s, _, _ in trace
    )
    assert (
        db.connection.execute(
            "SELECT observation_id,xmin::text FROM groundloop_semantic_observation "
            "ORDER BY observation_id"
        ).fetchall()
        == semantic_before
    )
    assert (
        db.connection.execute(
            "SELECT source_id,xmin::text FROM groundloop_m5_matching_work_contribution "
            "ORDER BY epoch_id,source_kind,source_id"
        ).fetchall()
        == matching_before
    )
    before = database_snapshot(db.connection)
    with db.runtime.reconnect() as reader:
        assert db.complete(source, connection=reader).exact_replay
    assert database_snapshot(db.connection) == before
    changed = db.build(db.chunks[0], "REFUTE", "changed-late-return")
    with pytest.raises(EventConflictError):
        db.complete(changed)
    assert database_snapshot(db.connection) == before


@pytest.mark.parametrize("terminal", [False, True])
def test_failed_current_attempt_cannot_be_reinterpreted_as_success(
    c2_db: Any,
    terminal: bool,
) -> None:
    from groundloop.m5.runtime.contracts import M5RuntimeWork, M5TerminalReason
    from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
    from tests.m5.postgres.helpers import sha
    from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot

    db = c2_db
    source = db.build(db.chunks[0])
    store = PostgresM5RuntimeStore(db.connection)
    args = (db.runtime.epoch_id, db.revision(), source.lease)
    if terminal:
        store.mark_m5_terminal_failure(
            *args,
            M5TerminalReason.VERIFIER_ERROR,
            sha("failed-return"),
            M5RuntimeWork(),
            None,
        )
    else:
        store.mark_m5_retryable_failure(
            *args, sha("failed-return"), M5RuntimeWork(), None
        )
    before = database_snapshot(db.connection)
    with pytest.raises(EventConflictError):
        db.complete(source)
    assert database_snapshot(db.connection) == before


@pytest.mark.parametrize("terminal", [False, True])
def test_private_late_return_every_client_mutation_and_named_phase_rolls_back(
    c2_db: Any,
    terminal: bool,
) -> None:
    from groundloop.m5.runtime.contracts import (
        M5CancellationPlan,
        M5RunFailureReason,
        M5RuntimeWork,
        M5TerminalReason,
    )
    from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
    from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot
    from tests.m5.postgres_runtime.d34_requirement_completion.conftest import (
        capture_sql,
    )

    db = c2_db
    source = db.build(db.chunks[0])
    store = PostgresM5RuntimeStore(db.connection)
    if terminal:
        store.fail_typed_epoch_atomically(
            db.runtime.epoch_id,
            db.revision(),
            M5RunFailureReason.VERIFIER_UNAVAILABLE,
            M5RuntimeWork(),
        )
    else:
        plan = M5CancellationPlan.build(
            structural_event_id=db.runtime.plan.structural_event_id,
            epoch_id=db.runtime.epoch_id,
            cancelled_job_ids=(source.job.logical_job_id,),
            reason=M5TerminalReason.SCOPE_RETIRED,
        )
        store.cancel_m5_work_atomically(db.runtime.epoch_id, db.revision(), plan)
    before = database_snapshot(db.connection)
    phases: list[str] = []

    def discover(stage: str) -> None:
        phases.append(stage)
        if stage == "late_return_constraints_validated":
            raise RuntimeError("late return phase discovery")

    with capture_sql(db.connection) as trace:
        with pytest.raises(RuntimeError, match="phase discovery"):
            db.complete(source, failure_injector=discover)
    assert database_snapshot(db.connection) == before
    mutations = [
        r
        for r in trace
        if r[0].lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "MERGE"))
        and r[2] > 0
    ]
    assert mutations and phases[-1] == "late_return_constraints_validated"
    for number in range(1, len(mutations) + 1):
        with capture_sql(db.connection, fail_mutation=number):
            with pytest.raises(RuntimeError, match="actual mutation cut"):
                db.complete(source)
        with db.runtime.reconnect() as reader:
            assert database_snapshot(reader) == before
    for phase in phases:

        def cut(stage: str, selected: str = phase) -> None:
            if stage == selected:
                raise RuntimeError("late return named phase cut")

        with pytest.raises(RuntimeError, match="named phase cut"):
            db.complete(source, failure_injector=cut)
        assert database_snapshot(db.connection) == before
    assert not db.complete(source).exact_replay
    with db.runtime.reconnect() as reader:
        assert db.complete(source, connection=reader).exact_replay


@pytest.mark.parametrize("c2_db", ["legacy-021"], indirect=True)
@pytest.mark.parametrize("kind", ["inactive", "late"])
def test_legacy_021_nonmatching_returns_never_enter_022_prerequisite(
    c2_db: Any,
    kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from groundloop.m5.runtime.contracts import M5CancellationPlan, M5TerminalReason
    from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore

    db = c2_db
    source = db.build(db.chunks[0])
    if kind == "inactive":
        with db.connection.transaction():
            db.connection.execute(
                "UPDATE groundloop_chunk_version SET valid_to_epoch=%s "
                "WHERE chunk_version_id=%s",
                (db.runtime.epoch_id, db.chunks[0]),
            )
            db.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    else:
        plan = M5CancellationPlan.build(
            structural_event_id=db.runtime.plan.structural_event_id,
            epoch_id=db.runtime.epoch_id,
            cancelled_job_ids=(source.job.logical_job_id,),
            reason=M5TerminalReason.SCOPE_RETIRED,
        )
        PostgresM5RuntimeStore(db.connection).cancel_m5_work_atomically(
            db.runtime.epoch_id,
            db.revision(),
            plan,
        )

    def forbidden(*_args: Any, **_kw: Any) -> Any:
        pytest.fail("nonmatching return attempted a fresh022 reader/prerequisite")

    monkeypatch.setattr(matching, "_d35_transition_reader_route", forbidden)
    monkeypatch.setattr(matching, "_d35_read_transition_projection", forbidden)
    assert not db.complete(source).exact_replay
    with db.runtime.reconnect() as reader:
        assert db.complete(source, connection=reader).exact_replay
    assert db.connection.execute(
        "SELECT count(*) FROM groundloop_m5_matching_work_contribution "
        "WHERE epoch_id=%s AND source_kind='requirement_completion'",
        (db.runtime.epoch_id,),
    ).fetchone() == (0,)


def test_retained_partial_closure_read_projection_is_never_repaired(c2_db: Any) -> None:
    """Missing authoritative closure reads, without storing guard-invalid rows."""
    import inspect

    import psycopg

    from groundloop.errors import ValidationError
    from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot

    db = c2_db
    source = db.build(db.chunks[0])
    db.complete(source)
    before = database_snapshot(db.connection)
    target = [""]
    hits: list[str] = []
    statements: list[str] = []
    original = db.connection.cursor_factory

    class PartialRead(psycopg.Cursor):
        statement = ""

        def execute(self, query: Any, params: Any = None, **kw: Any) -> Any:
            self.statement = (
                query if isinstance(query, str) else query.as_string(self.connection)
            )
            statements.append(self.statement)
            return super().execute(query, params, **kw)

        def fetchone(self) -> Any:
            row = super().fetchone()
            if (
                row is not None
                and "FROM " + target[0] in self.statement
                and any(
                    f.function == "_validate_verifier_closure_replay"
                    for f in inspect.stack(context=0)
                )
            ):
                hits.append(target[0])
                return None
            return row

    tables = (
        "groundloop_m5_requirement_pair_input",
        "groundloop_m5_requirement_verifier_artifact",
        "groundloop_semantic_observation",
        "groundloop_m5_requirement_verifier_execution",
    )
    for table in tables:
        target[0] = table
        statements.clear()
        db.connection.cursor_factory = PartialRead
        try:
            with pytest.raises((ValidationError, EventConflictError)):
                db.complete(source)
        finally:
            db.connection.cursor_factory = original
        assert not any(
            s.lstrip()
            .upper()
            .startswith(("INSERT", "UPDATE", "DELETE", "SET CONSTRAINTS"))
            for s in statements
        )
        with db.runtime.reconnect() as reader:
            assert database_snapshot(reader) == before
    assert set(hits) == set(tables)


def test_retained_native_replay_preserves_bytes_without_first_application(
    requirement_completion_database: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = requirement_completion_database
    with database.connection.cursor() as cursor:
        intent, _ = authorize_and_derive_requirement_completion(cursor, database)
        prepared = matching._prepare_matching_transition(cursor, intent)
        install_prepared_matching_headers(
            cursor, prepared, expected_revision=database.expected_revision
        )
        matching._stage_prepared_matching_transition(cursor, intent, prepared)
        terminalize_requirement_completion_source(cursor, database)
        first = matching._finalize_prepared_matching_transition(
            cursor, intent, prepared
        )
        # Component-level exact tier16 rows: use the native output unchanged,
        # exercising the immediate event FK rather than remapping at INSERT.
        logical = prepared.prewrite_patch_artifact.logical_patch
        deltas = tuple(
            value for kind, _, value in logical.output_records if kind == "status_delta"
        )
        assert len(deltas) == 2
        for delta in deltas:
            cursor.execute(
                """INSERT INTO groundloop_status_delta (
                     event_id,epoch_id,revision,object_type,object_id,
                     old_status,new_status,reason
                   ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    delta.event_id,
                    database.epoch_id,
                    database.resulting_revision,
                    delta.object_type,
                    delta.object_id,
                    delta.old_status,
                    delta.new_status,
                    delta.reason,
                ),
            )
        stored_deltas = cursor.execute(
            """SELECT event_id,object_type,object_id,old_status,new_status,reason
               FROM groundloop_status_delta WHERE epoch_id=%s AND revision=%s
               ORDER BY object_type COLLATE "C",object_id COLLATE "C"
            """,
            (database.epoch_id, database.resulting_revision),
        ).fetchall()
        assert stored_deltas == sorted(
            (
                delta.event_id,
                delta.object_type,
                delta.object_id,
                delta.old_status,
                delta.new_status,
                delta.reason,
            )
            for delta in deltas
        )
        retained = retained_matching_replay_intent(
            cursor,
            epoch_id=database.epoch_id,
            source_kind=M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION,
            source_id=database.attempt_id,
        )
        before = cursor.execute(
            """SELECT canonical_patch_preimage,logical_output_preimage,
                      matching_work_digest
               FROM groundloop_m5_matching_patch_artifact WHERE patch_digest=%s""",
            (first.patch.patch_digest,),
        ).fetchone()

        def forbidden_first_application(**kwargs: Any) -> Any:
            raise AssertionError("retained replay re-entered first application")

        monkeypatch.setattr(
            matching, "_requirement_logical_transition", forbidden_first_application
        )
        trace = test_archive_order._ArchiveTraceCursor(cursor)
        replay = matching.apply_matching_transition(
            trace,  # type: ignore[arg-type]
            retained,
            expected_patch_digest=first.patch.patch_digest,
            expected_work=prepared.d25_contribution_work,
        )
        assert replay.exact_replay
        assert replay.patch == first.patch
        assert replay.contribution_digest == first.contribution_digest
        assert replay.accumulated_work == first.accumulated_work
        assert (
            cursor.execute(
                """SELECT canonical_patch_preimage,logical_output_preimage,
                      matching_work_digest
               FROM groundloop_m5_matching_patch_artifact WHERE patch_digest=%s""",
                (first.patch.patch_digest,),
            ).fetchone()
            == before
        )
        assert not any(
            statement.lstrip()
            .upper()
            .startswith(("INSERT", "UPDATE", "DELETE", "MERGE", "SET CONSTRAINTS"))
            for statement in trace.statements
        )
        advisory_namespaces = tuple(
            parameters[0]
            for statement, parameters in zip(
                trace.statements, trace.parameters, strict=True
            )
            if "pg_advisory_xact_lock" in statement
        )
        assert advisory_namespaces == (matching._MATCHING_ARTIFACT_LOCK_NAMESPACE,)
        assert matching._d34_ARCHIVE_NAMESPACE not in advisory_namespaces
        assert not any(
            "groundloop_m5_authorize_persisted_matching_transition" in statement
            for statement in trace.statements
        )
        assert not any(
            "FOR UPDATE" in statement.upper()
            and (
                "FROM groundloop_semantic_observation" in statement
                or "FROM groundloop_m5_requirement_pair_input" in statement
            )
            for statement in trace.statements
        )


def test_retained_source_checks_observation_without_archive_repair(
    requirement_completion_database: Any,
) -> None:
    database = requirement_completion_database
    with database.connection.cursor() as cursor:
        intent, _ = authorize_and_derive_requirement_completion(cursor, database)
        prepared = matching._prepare_matching_transition(cursor, intent)
        install_prepared_matching_headers(
            cursor, prepared, expected_revision=database.expected_revision
        )
        matching._stage_prepared_matching_transition(cursor, intent, prepared)
        terminalize_requirement_completion_source(cursor, database)
        matching._finalize_prepared_matching_transition(cursor, intent, prepared)
        matching._lock_requirement_replay_source(cursor, intent)
        proxy = test_active_completion._ProjectionMutationCursor(
            cursor,
            "FROM groundloop_semantic_observation WHERE observation_id=%s",
            12,
            database.epoch_id + 1,
        )
        with pytest.raises(EventConflictError, match="full observation"):
            matching._lock_requirement_replay_source(proxy, intent)
        assert proxy.hits == 1
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            or "pg_advisory_xact_lock" in statement
            for statement in proxy.statements
        )
        wrong_hash = "f" * 64
        if wrong_hash == database.verifier.verifier_artifact.decision_policy_hash:
            wrong_hash = "0" * 64
        execution_proxy = test_active_completion._ProjectionMutationCursor(
            cursor,
            "FROM groundloop_m5_requirement_verifier_execution",
            9,
            wrong_hash,
        )
        with pytest.raises(EventConflictError, match="replay full source changed"):
            matching._lock_requirement_replay_source(execution_proxy, intent)
        assert execution_proxy.hits == 1
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            or "pg_advisory_xact_lock" in statement
            for statement in execution_proxy.statements
        )
