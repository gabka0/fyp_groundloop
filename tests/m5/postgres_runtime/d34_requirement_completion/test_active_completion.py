"""Native producer prerequisites for the private C2 completion path.

This controlled component fixture does not claim genuine committed C2 history.
The event-ID regression is a normal test, never xfail/skip. Projection mutation
tests corrupt only a returned read projection, never immutable database rows.
"""

from typing import Any

import pytest

from groundloop.errors import EventConflictError
from groundloop.m5.runtime.postgres_matching import (
    _d34_validate_source_observation,
    _prepare_matching_transition,
    _requirement_completion_source,
)
from tests.m5.postgres_runtime.d25_store_core.conftest import (
    authorize_and_derive_requirement_completion,
)


@pytest.mark.parametrize("principal", ["owner", "login"])
@pytest.mark.parametrize(
    ("damage", "c2_db"),
    [
        (name, "legacy-021" if name in {"absent", "orphan", "ledger"} else None)
        for name in (
            "absent",
            "orphan",
            "partial",
            "ledger",
            "body",
            "volatility",
            "acl",
        )
    ],
    indirect=["c2_db"],
)
def test_d35_fresh_prerequisite_fails_before_authorizer_or_source_mutation(
    c2_db: Any, principal: str, damage: str
) -> None:
    from contextlib import nullcontext
    from types import SimpleNamespace

    from psycopg import sql

    from groundloop.postgres import migrations as m
    from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot
    from tests.m5.postgres_runtime.d34_requirement_completion.conftest import (
        capture_sql,
    )
    from tests.m5.postgres_runtime.test_semantic_readiness import _runtime_login

    db = c2_db
    source = db.build(db.chunks[0])
    signature = "(bigint,bigint,bigint,text,text,oid,oid,oid)"
    function = sql.Identifier(db.schema, m._m5_transition_access_names[0])
    with db.connection.transaction():
        if damage == "partial":
            db.connection.execute(
                sql.SQL("DROP FUNCTION {}" + signature).format(function)
            )
        if damage in {"orphan", "ledger"}:
            for _, statement in m._m5_transition_access_statements(
                m.M5_TRANSITION_CONTEXT_ACCESS_MIGRATION_PATH.read_bytes()
            ):
                db.connection.execute(statement)
            if damage == "ledger":
                expected = list(m._m5_transition_access_expected_ledger())
                expected[2] = "0" * 64
                db.connection.execute(
                    "INSERT INTO groundloop_m5_schema_bundle "
                    "(bundle_id,bundle_sha256,migration_sha256,oracle_sha256,"
                    "prerequisite_sha256) VALUES (%s,%s,%s,%s,%s)",
                    tuple(expected),
                )
        elif damage == "body":
            definition = db.connection.execute(
                "SELECT pg_catalog.pg_get_functiondef(p.oid) FROM pg_catalog.pg_proc p "
                "JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace "
                "WHERE n.nspname=%s AND p.proname=%s",
                (db.schema, m._m5_transition_access_names[0]),
            ).fetchone()[0]
            assert definition.count("AS $function$") == 1
            db.connection.execute(
                definition.replace(
                    "AS $function$", "AS $function$\n-- D35 exact-body falsifier\n", 1
                )
            )
        elif damage == "volatility":
            db.connection.execute(
                sql.SQL("ALTER FUNCTION {}" + signature + " VOLATILE").format(function)
            )
        elif damage == "acl":
            db.connection.execute(
                sql.SQL(
                    "REVOKE EXECUTE ON FUNCTION {}" + signature + " FROM PUBLIC"
                ).format(function)
            )
    before = database_snapshot(db.connection)
    adapter = SimpleNamespace(
        connection=db.connection, schema_name=db.schema, dsn=db.dsn
    )
    context = (
        _runtime_login(adapter) if principal == "login" else nullcontext(db.connection)
    )
    with context as connection:
        with capture_sql(connection) as trace:
            with pytest.raises(EventConflictError, match="D35"):
                db.complete(source, connection=connection)
    statements = [s for s, _, _ in trace]
    assert not any(
        "SELECT groundloop_m5_authorize_persisted_matching_transition(" in s
        for s in statements
    )
    assert not any(
        s.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
        for s in statements
    )
    assert database_snapshot(db.connection) == before


@pytest.mark.parametrize("projection", ["context", "journal"])
@pytest.mark.parametrize("damage", ["missing", "duplicate", *range(10)])
@pytest.mark.parametrize("c2_db", ["complete"], indirect=True)
def test_d35_native_comparison_rejects_corrupt_bridge_projection_and_rolls_back(
    c2_db: Any, projection: str, damage: Any
) -> None:
    """Alter only real cursor read results; retain native trigger/guard writes."""
    import psycopg

    from groundloop.errors import ValidationError
    from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot

    db = c2_db
    source = db.build(db.chunks[0])
    before = database_snapshot(db.connection)
    target = (
        "groundloop_m5_matching_read_transition_context"
        if projection == "context"
        else "groundloop_m5_matching_read_transition_stage_journal"
    )
    hit = []
    original = db.connection.cursor_factory

    class CorruptRead(psycopg.Cursor):
        statement = ""

        def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
            self.statement = (
                query if isinstance(query, str) else query.as_string(self.connection)
            )
            return super().execute(query, params, **kwargs)

        def fetchall(self) -> Any:
            rows = super().fetchall()
            if (
                self.statement.startswith('SELECT * FROM "')
                and target in self.statement
            ):
                assert rows
                hit.append((self.statement, rows))
                if damage == "missing":
                    return rows[1:]
                if damage == "duplicate":
                    return [*rows, rows[0]]
                row = list(rows[0])
                value = row[damage]
                row[damage] = (
                    not value
                    if isinstance(value, bool)
                    else value + 1
                    if isinstance(value, int)
                    else b"bad-key"
                    if isinstance(value, bytes)
                    else {"corrupt": True}
                    if isinstance(value, dict) or value is None
                    else str(value) + "wrong"
                )
                return [tuple(row), *rows[1:]]
            return rows

    db.connection.cursor_factory = CorruptRead
    try:
        with pytest.raises((EventConflictError, ValidationError)):
            db.complete(source)
    finally:
        db.connection.cursor_factory = original
    assert hit
    with db.runtime.reconnect() as reader:
        assert database_snapshot(reader) == before
    assert not db.complete(source).exact_replay


@pytest.mark.parametrize("second_envelope", [False, True])
def test_d35_phase_route_cannot_follow_another_genuine_same_owner_installation(
    c2_db: Any, second_envelope: bool
) -> None:
    from contextlib import contextmanager
    from types import SimpleNamespace

    from psycopg import sql

    from groundloop.m5.runtime import postgres_matching as matching
    from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot
    from tests.m5.postgres_runtime.d34_requirement_completion.conftest import (
        c2_db as fixture,
    )

    db = c2_db
    source = db.build(db.chunks[0])
    before = database_snapshot(db.connection)
    second_fixture = contextmanager(fixture.__wrapped__)
    with second_fixture(SimpleNamespace(param=None)) as other:
        reached: list[str] = []

        def drift(stage: str) -> None:
            if stage != "d34_matching_prepared":
                return
            db.connection.execute(
                sql.SQL("SET LOCAL search_path TO {},public").format(
                    sql.Identifier(other.schema)
                )
            )
            if second_envelope:
                # A real second permanent runtime envelope, not invented GUCs.
                db.connection.execute(
                    "SELECT epoch_id FROM groundloop_m5_runtime_epoch FOR SHARE"
                )
            with db.connection.cursor() as cursor:
                with pytest.raises(EventConflictError, match="D35.*runtime envelope"):
                    matching._d35_transition_reader_route(cursor)
            reached.append(stage)

        with pytest.raises(
            EventConflictError, match="D34 owned transaction or schema changed"
        ):
            db.complete(source, failure_injector=drift)
        assert reached == ["d34_matching_prepared"]
        assert database_snapshot(db.connection) == before
    assert not db.complete(source).exact_replay


@pytest.mark.parametrize("retained", [False, True])
def test_integrated_fifteen_observation_read_fields_fail_closed_and_preserve_rows(
    c2_db: Any,
    retained: bool,
) -> None:
    import psycopg

    from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot

    db = c2_db
    source = db.build(db.chunks[0])
    if retained:
        db.complete(source)
    before = database_snapshot(db.connection)
    original = db.connection.cursor_factory
    target = [0]
    hits: list[int] = []
    statements: list[str] = []

    class ObservationReadFault(psycopg.Cursor):
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
                and len(row) == 15
                and "FROM groundloop_semantic_observation" in self.statement
            ):
                index = target[0]
                changed = list(row)
                value = row[index]
                changed[index] = (
                    not value
                    if index == 14
                    else int(value) + 1
                    if index == 12
                    else float(value) + 0.125
                    if index in (5, 6, 7)
                    else "f" * 64
                    if index in (11, 13) and str(value).strip() != "f" * 64
                    else str(value) + "-fault"
                )
                hits.append(index)
                return tuple(changed)
            return row

    for index in range(15):
        target[0] = index
        statements.clear()
        db.connection.cursor_factory = ObservationReadFault
        try:
            with pytest.raises(EventConflictError, match="observation"):
                db.complete(source)
        finally:
            db.connection.cursor_factory = original
        if retained:
            assert not any(
                s.lstrip()
                .upper()
                .startswith(("INSERT", "UPDATE", "DELETE", "SET CONSTRAINTS"))
                for s in statements
            )
        with db.runtime.reconnect() as reader:
            assert database_snapshot(reader) == before
    assert set(hits) == set(range(15))
    assert db.complete(source).exact_replay == retained


@pytest.mark.parametrize(
    "fault",
    [
        "missing-reservation",
        "absent-row",
        "old-xmin",
        "wrong-full-xid",
        "same-low32-wrong-full-epoch",
    ],
)
def test_native_source_freshness_and_actual_reserved_prefix_read_proofs(
    requirement_completion_database: Any,
    fault: str,
) -> None:
    """Native preterminal source plus read-projection faults; no guard changes.

    Changing the observed top epoch while retaining its low32 is a negative,
    not an actual server wrap. Forward-wrap arithmetic has separate pure cases.
    Real row, reservation, full-XID and running/reserved closure are prerequisites.
    """
    import psycopg

    from groundloop.m5.runtime import postgres_matching as matching
    from groundloop.m5.runtime.contracts import M5PersistedMatchingSourceKind
    from tests.m5.postgres_runtime.d25_store_core.conftest import (
        _lock_requirement_completion_source_prefix,
    )

    db = requirement_completion_database
    with db.connection.cursor() as cursor:
        matching._authorize_matching_transition(
            cursor,
            epoch_id=db.epoch_id,
            expected_runtime_revision=db.expected_revision,
            resulting_revision=db.resulting_revision,
            source_kind=M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION,
            source_id=db.attempt_id,
        )
        result = _lock_requirement_completion_source_prefix(cursor, db)
        arguments = dict(
            epoch_id=db.epoch_id,
            expected_runtime_revision=db.expected_revision,
            resulting_revision=db.resulting_revision,
            proposed_attempt_id=db.attempt_id,
            expected_source_identity_hash=result.attempt_result_artifact_hash,
        )
        assert (
            matching._requirement_completion_source(cursor, **arguments).attempt_id
            == db.attempt_id
        )
    original = db.connection.cursor_factory
    hits: list[Any] = []
    statements: list[str] = []

    class FreshnessReadFault(psycopg.Cursor):
        statement = ""
        arguments: Any = None

        def execute(self, query: Any, params: Any = None, **kw: Any) -> Any:
            self.statement = (
                query if isinstance(query, str) else query.as_string(self.connection)
            )
            self.arguments = params
            statements.append(self.statement)
            return super().execute(query, params, **kw)

        def fetchall(self) -> Any:
            rows = super().fetchall()
            if (
                fault == "missing-reservation"
                and "FROM pg_catalog.pg_locks" in self.statement
                and self.arguments[0] == matching._d34_ARCHIVE_NAMESPACE
            ):
                assert rows == [(1,)]
                hits.append(rows)
                return []
            return rows

        def fetchone(self) -> Any:
            row = super().fetchone()
            if "SELECT xmin::text::bigint" in self.statement:
                assert row is not None and int(row[0]) == int(row[1]) % 2**32
                hits.append(row)
                if fault == "absent-row":
                    return None
                if fault == "old-xmin":
                    return (int(row[0]) - 1, row[1])
                if fault == "wrong-full-xid":
                    return (row[0], int(row[1]) + 1)
                if fault == "same-low32-wrong-full-epoch":
                    return (row[0], int(row[1]) + 2**32)
            return row

    db.connection.cursor_factory = FreshnessReadFault
    try:
        with db.connection.cursor() as cursor:
            with pytest.raises(EventConflictError):
                matching.derive_matching_transition_intent(
                    cursor,
                    epoch_id=db.epoch_id,
                    expected_runtime_revision=db.expected_revision,
                    resulting_revision=db.resulting_revision,
                    source_kind=M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION,
                    source_id=db.attempt_id,
                    expected_source_identity_hash=result.attempt_result_artifact_hash,
                )
    finally:
        db.connection.cursor_factory = original
    assert hits
    assert not any(
        s.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
        or "FROM groundloop_semantic_observation" in s
        and "FOR UPDATE" in s
        for s in statements
    )


def test_genuine_active_completion_commits_and_replays(c2_db: Any) -> None:
    db = c2_db
    fixture = db.build(db.chunks[0])
    revision = db.revision()
    receipt = db.complete(fixture)
    assert not receipt.exact_replay and receipt.resulting_revision == revision + 1
    row = db.connection.execute(
        "SELECT job_state,completed_revision FROM groundloop_m5_semantic_job "
        "WHERE epoch_id=%s AND logical_job_id=%s",
        (db.runtime.epoch_id, fixture.job.logical_job_id),
    ).fetchone()
    assert row == ("completed_active", receipt.resulting_revision)
    with db.runtime.reconnect() as connection:
        replay = db.complete(
            fixture,
            connection=connection,
            expected_revision=fixture.lease.resulting_revision,
        )
    assert (
        replay.exact_replay and replay.resulting_revision == receipt.resulting_revision
    )


def test_private_inactive_completion_has_no_currency_or_matching_effect(
    c2_db: Any,
) -> None:
    """Controlled activity transition, not a new document-event history claim."""
    from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot

    db = c2_db
    fixture = db.build(db.chunks[2])
    with db.connection.transaction():
        db.connection.execute(
            "UPDATE groundloop_chunk_version SET valid_to_epoch=%s WHERE "
            "chunk_version_id=%s",
            (db.runtime.epoch_id, db.chunks[2]),
        )
        db.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    receipt = db.complete(fixture)
    assert not receipt.exact_replay
    assert db.connection.execute(
        "SELECT job_state FROM groundloop_m5_semantic_job WHERE logical_job_id=%s",
        (fixture.job.logical_job_id,),
    ).fetchone() == ("completed_inactive",)
    assert db.connection.execute(
        "SELECT eligible_for_currency FROM groundloop_semantic_observation WHERE "
        "observation_id=%s",
        (fixture.verifier_artifact.to_semantic_observation().observation_id,),
    ).fetchone() == (False,)
    assert db.connection.execute(
        "SELECT count(*) FROM groundloop_m5_working_currency_history WHERE epoch_id=%s",
        (db.runtime.epoch_id,),
    ).fetchone() == (0,)
    assert db.connection.execute(
        "SELECT count(*) FROM groundloop_m5_matching_work_contribution WHERE "
        "epoch_id=%s AND source_kind='requirement_completion'",
        (db.runtime.epoch_id,),
    ).fetchone() == (0,)
    before = database_snapshot(db.connection)
    with db.runtime.reconnect() as connection:
        assert db.complete(fixture, connection=connection).exact_replay
    assert database_snapshot(db.connection) == before


@pytest.mark.parametrize(
    "changes",
    [
        {"model_artifact_id": "missing-model"},
        {"prompt_artifact_id": "missing-prompt"},
        {"model_id": "wrong-model"},
        {"model_revision": "wrong-revision"},
        {"prompt_version": "wrong-prompt"},
        {"execution_spec_hash": "f" * 64},
        {"pair_input_hash": "e" * 64},
    ],
)
def test_missing_or_wrong_native_fk_and_artifact_bindings_fail_before_archive(
    c2_db: Any,
    changes: dict[str, Any],
) -> None:
    from groundloop.errors import ValidationError
    from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot
    from tests.m5.postgres_runtime.d34_requirement_completion.conftest import (
        capture_sql,
    )

    db = c2_db
    source = db.build(db.chunks[0], artifact_changes=changes)
    before = database_snapshot(db.connection)
    with capture_sql(db.connection) as trace:
        with pytest.raises((EventConflictError, ValidationError)):
            db.complete(source)
    assert not any(
        s.lstrip().startswith("INSERT")
        and any(
            name in s
            for name in (
                "groundloop_m5_attempt_result_artifact (",
                "groundloop_m5_requirement_pair_input (",
                "groundloop_m5_requirement_verifier_artifact (",
                "groundloop_semantic_observation (",
                "groundloop_m5_requirement_verifier_execution (",
            )
        )
        for s, _, _ in trace
    )
    assert database_snapshot(db.connection) == before


def test_internal_commit_is_rejected_and_rolls_back_owned_outer_transaction(
    c2_db: Any,
) -> None:
    import psycopg

    from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot

    db = c2_db
    source = db.build(db.chunks[0])
    before = database_snapshot(db.connection)

    def bad_commit(stage: str) -> None:
        if stage == "verifier_attempt_artifact_inserted":
            db.connection.commit()

    with pytest.raises(psycopg.ProgrammingError, match="commit"):
        db.complete(source, failure_injector=bad_commit)
    assert database_snapshot(db.connection) == before
    assert not db.complete(source).exact_replay


@pytest.mark.parametrize("label", ["SUPPORT", "REFUTE", "NEUTRAL"])
def test_genuine_labels_and_exact_pending_header_after_images(
    c2_db: Any, label: str
) -> None:
    db = c2_db
    fixture = db.build(db.chunks[0], label)
    old = db.connection.execute(
        "SELECT revision,runtime_state,open_work_count,open_scope_count,blocking_"
        "failure_count "
        "FROM groundloop_m5_runtime_epoch WHERE epoch_id=%s",
        (db.runtime.epoch_id,),
    ).fetchone()
    receipt = db.complete(fixture)
    assert db.connection.execute(
        "SELECT revision,runtime_state,open_work_count,open_scope_count,blocking_"
        "failure_count "
        "FROM groundloop_m5_runtime_epoch WHERE epoch_id=%s",
        (db.runtime.epoch_id,),
    ).fetchone() == (old[0] + 1, "semantic_pending", old[2] - 1, old[3], old[4])
    assert db.connection.execute(
        "SELECT revision,structural_status,semantic_status,evaluation_state,publi"
        "cation_mode "
        "FROM groundloop_epoch WHERE epoch_id=%s",
        (db.runtime.epoch_id,),
    ).fetchone() == (
        receipt.resulting_revision,
        "committed",
        "pending",
        "pending",
        "provisional",
    )
    observation = fixture.verifier_artifact.to_semantic_observation()
    assert db.connection.execute(
        "SELECT observation_id FROM groundloop_m5_currency_at(%s,%s) "
        "WHERE subject_kind='requirement' AND subject_id=%s AND chunk_version_id=%s",
        (
            db.runtime.epoch_id,
            receipt.resulting_revision,
            fixture.job.pair.subject_id,
            db.chunks[0],
        ),
    ).fetchone() == (observation.observation_id,)
    assert db.connection.execute(
        "SELECT count(*) FROM groundloop_m5_matching_observation_working "
        "WHERE epoch_id=%s AND present",
        (db.runtime.epoch_id,),
    ).fetchone() == (int(label == "SUPPORT"),)
    assert db.connection.execute(
        "SELECT status,refute_count FROM groundloop_m5_working_claim_state "
        "WHERE epoch_id=%s AND claim_id=%s",
        (db.runtime.epoch_id, db.base.claim_ids[0]),
    ).fetchone() in (None, ("unsupported", 0))
    for family in ("owner", "answer"):
        assert db.connection.execute(
            "SELECT verifier_job_count,updated_revision "
            f"FROM groundloop_m5_{family}_pending_counter "
            "WHERE epoch_id=%s",
            (db.runtime.epoch_id,),
        ).fetchall() == [(2, receipt.resulting_revision)]
    assert db.connection.execute(
        "SELECT count(*) FROM groundloop_status_delta WHERE epoch_id=%s",
        (db.runtime.epoch_id,),
    ).fetchone() == (0,)


@pytest.mark.parametrize("c2_db", ["complete"], indirect=True)
def test_genuine_nonzero_status_deltas_keep_native_event_and_seal_owned_counter(
    c2_db: Any,
) -> None:
    db = c2_db
    fixture = db.build(db.chunks[0])
    receipt = db.complete(fixture)
    deltas = db.connection.execute(
        "SELECT event_id,object_type,object_id,old_status,new_status,reason "
        "FROM groundloop_status_delta WHERE epoch_id=%s AND revision=%s "
        'ORDER BY object_type COLLATE "C",object_id COLLATE "C"',
        (db.runtime.epoch_id, receipt.resulting_revision),
    ).fetchall()
    assert len(deltas) == 2 and {r[0] for r in deltas} == {
        db.runtime.plan.structural_event_id
    }
    assert {(r[1], r[4]) for r in deltas} == {
        ("claim", "supported"),
        ("answer", "valid"),
    }
    assert {r[5] for r in deltas} == {
        f"attempt={fixture.lease.attempt.attempt_id} op=RequirementCompletion"
    }
    assert db.connection.execute(
        "SELECT public_delta_count FROM groundloop_m5_runtime_work_contribution "
        "WHERE epoch_id=%s AND contribution_kind='verifier_completion' AND "
        "source_id=%s",
        (db.runtime.epoch_id, fixture.lease.attempt.attempt_id),
    ).fetchone() == (0,)
    assert db.connection.execute(
        "SELECT public_status_deltas FROM groundloop_m5_matching_work_contribution "
        "WHERE epoch_id=%s AND source_kind='requirement_completion' AND source_id=%s",
        (db.runtime.epoch_id, fixture.lease.attempt.attempt_id),
    ).fetchone() == (2,)


def test_nonempty_requirement_deltas_reference_the_actual_epoch_event(
    requirement_completion_database: Any,
) -> None:
    database = requirement_completion_database
    with database.connection.cursor() as cursor:
        intent, _ = authorize_and_derive_requirement_completion(cursor, database)
        prepared = _prepare_matching_transition(cursor, intent)
        logical = prepared.prewrite_patch_artifact.logical_patch
        deltas = tuple(
            value for kind, _, value in logical.output_records if kind == "status_delta"
        )
        assert len(deltas) == 2
        assert prepared.d24_owned_planned_write_counts.public_delta_write_count == 2
        assert prepared.d25_contribution_work.public_status_deltas == 2
        actual_event = cursor.execute(
            "SELECT event_id FROM groundloop_epoch WHERE epoch_id=%s",
            (database.epoch_id,),
        ).fetchone()
        assert actual_event is not None
        referenced_events = tuple(
            cursor.execute(
                "SELECT event_id FROM groundloop_epoch WHERE event_id=%s",
                (delta.event_id,),
            ).fetchone()
            for delta in deltas
        )
        assert {delta.event_id for delta in deltas} == {actual_event[0]}, (
            "Native status deltas use an attempt ID instead of the epoch event ID",
            tuple(delta.event_id for delta in deltas),
            actual_event[0],
            referenced_events,
        )
        assert all(row == actual_event for row in referenced_events)


class _ProjectionMutationCursor:
    """Return one deliberately corrupted ordinary-read field without DML."""

    def __init__(
        self, cursor: Any, needle: str, index: int, replacement: object
    ) -> None:
        self.cursor = cursor
        self.needle = needle
        self.index = index
        self.replacement = replacement
        self.hits = 0
        self.statements: list[str] = []
        self.row: tuple[object, ...] | None = None

    def execute(self, statement: Any, parameters: Any = None) -> Any:
        text = statement if isinstance(statement, str) else str(statement)
        self.statements.append(text)
        result = self.cursor.execute(statement, parameters)
        if self.needle not in text:
            return result
        row = result.fetchone()
        assert row is not None
        values = list(row)
        values[self.index] = self.replacement
        self.row = tuple(values)
        self.hits += 1
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self.row

    def assert_one_read_only_mutation(self) -> None:
        assert self.hits == 1
        for statement in self.statements:
            upper = statement.upper()
            assert not upper.lstrip().startswith(
                ("INSERT", "UPDATE", "DELETE", "MERGE", "CALL", "SET")
            )
            assert "FOR UPDATE" not in upper
            assert "PG_ADVISORY" not in upper


def test_event_authority_is_checked_without_changing_attempt_identity(
    requirement_completion_database: Any,
) -> None:
    database = requirement_completion_database
    with database.connection.cursor() as cursor:
        intent, result = authorize_and_derive_requirement_completion(cursor, database)
        source_arguments = dict(
            epoch_id=database.epoch_id,
            expected_runtime_revision=database.expected_revision,
            resulting_revision=database.resulting_revision,
            proposed_attempt_id=database.attempt_id,
            expected_source_identity_hash=result.attempt_result_artifact_hash,
        )
        source = _requirement_completion_source(cursor, **source_arguments)
        authority = cursor.execute(
            """SELECT epoch.event_id,runtime.structural_event_id
               FROM groundloop_epoch epoch
               JOIN groundloop_m5_runtime_epoch runtime USING(epoch_id)
               WHERE epoch_id=%s""",
            (database.epoch_id,),
        ).fetchone()
        assert authority is not None and authority[0] == authority[1]
        assert source.structural_event_id == authority[0]
        assert source.attempt_id == database.attempt_id
        assert (
            source.attempt_result_artifact_hash == result.attempt_result_artifact_hash
        )
        assert source.logical_job_id == database.verifier.job.logical_job_id
        assert source.structural_event_id != source.attempt_id

        prepared = _prepare_matching_transition(cursor, intent)
        logical = prepared.prewrite_patch_artifact.logical_patch
        deltas = tuple(
            value for kind, _, value in logical.output_records if kind == "status_delta"
        )
        assert len(deltas) == 2
        assert {delta.reason for delta in deltas} == {
            f"attempt={database.attempt_id} op=RequirementCompletion"
        }
        assert {delta.event_id for delta in deltas} == {source.structural_event_id}

        for index in (6, 7):
            for replacement in (None, "", "wrong-structural-event", 42):
                proxy = _ProjectionMutationCursor(
                    cursor,
                    "epoch.event_id, runtime.structural_event_id",
                    index,
                    replacement,
                )
                with pytest.raises(EventConflictError, match="source closure changed"):
                    _requirement_completion_source(proxy, **source_arguments)
                proxy.assert_one_read_only_mutation()
        assert (
            cursor.execute(
                """SELECT epoch.event_id,runtime.structural_event_id
               FROM groundloop_epoch epoch
               JOIN groundloop_m5_runtime_epoch runtime USING(epoch_id)
               WHERE epoch_id=%s""",
                (database.epoch_id,),
            ).fetchone()
            == authority
        )


def test_every_source_observation_field_is_independently_validated(
    requirement_completion_database: Any,
) -> None:
    database = requirement_completion_database
    fixture = database.verifier
    with database.connection.cursor() as cursor:
        authorize_and_derive_requirement_completion(cursor, database)
        observation = fixture.verifier_artifact.to_semantic_observation()
        arguments = dict(
            observation_id=observation.observation_id,
            epoch_id=database.epoch_id,
            artifact_id=fixture.verifier_artifact.artifact_id,
            pair_input_hash=fixture.pair_input.pair_input_hash,
            eligible=True,
        )
        assert _d34_validate_source_observation(cursor, **arguments) == (
            fixture.pair_input,
            fixture.verifier_artifact,
        )
        needle = "FROM groundloop_semantic_observation WHERE observation_id=%s"
        original = cursor.execute(
            """SELECT observation_id,subject_kind,subject_id,chunk_version_id,
                      task_type,support_score,refute_score,neutral_score,
                      model_id,model_version,prompt_version,input_hash,
                      produced_epoch,raw_output_hash,eligible_for_currency
               FROM groundloop_semantic_observation WHERE observation_id=%s""",
            (arguments["observation_id"],),
        ).fetchone()
        assert original is not None and len(original) == 15
        tested = set()
        for index, value in enumerate(original):
            if index == 14:
                replacement: object = not value
            elif index == 12:
                replacement = int(value) + 1
            elif index in (5, 6, 7):
                replacement = float(value) + 0.125
            elif index in (11, 13):
                replacement = "f" * 64 if str(value).strip() != "f" * 64 else "0" * 64
            else:
                replacement = str(value) + "-mutated"
            proxy = _ProjectionMutationCursor(cursor, needle, index, replacement)
            with pytest.raises(EventConflictError, match="observation"):
                _d34_validate_source_observation(proxy, **arguments)
            proxy.assert_one_read_only_mutation()
            tested.add(index)
        assert tested == set(range(15))
        assert _d34_validate_source_observation(cursor, **arguments) == (
            fixture.pair_input,
            fixture.verifier_artifact,
        )
