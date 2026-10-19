"""Live common-archive component checks, not a committed C2 history gate."""

from typing import Any

import pytest

from groundloop.errors import EventConflictError
from groundloop.m5.runtime.contracts import M5PersistedMatchingSourceKind
from groundloop.m5.runtime.postgres_matching import _authorize_matching_transition
from groundloop.m5.runtime.postgres_verifier import _persist_verifier_closure
from tests.m5.postgres_runtime.d25_store_core.conftest import (
    _lock_requirement_completion_source_prefix,
)


def _native_archive_map(source: Any, result: Any) -> Any:
    from groundloop.m5.runtime import postgres_matching as matching

    pair, artifact = source.pair_input, source.verifier_artifact
    observation = artifact.to_semantic_observation()
    return matching._d34_archive_coordinates(
        result=(
            result.attempt_result_artifact_id,
            result.attempt_result_artifact_hash,
            source.attempt_output.attempt_output_digest,
            result.attempt_id,
        ),
        pair=(
            pair.pair_input_hash,
            pair.semantic_pair_digest,
            pair.scope_contract_digest,
            pair.candidate_policy_id,
        ),
        verifier=(artifact.artifact_id, artifact.artifact_hash),
        observation=(
            observation.observation_id,
            "requirement",
            observation.subject_id,
            observation.chunk_version_id,
            observation.task_type,
        ),
        execution=(
            observation.observation_id,
            artifact.artifact_id,
            source.job.logical_job_id,
            result.attempt_id,
            pair.pair_input_hash,
        ),
    )


def _old_and_fresh_component_maps(db: Any) -> Any:
    """Actual older committed closure and new typed DTO, never a persisted orphan."""
    from groundloop.m5.runtime import postgres_verifier as verifier
    from groundloop.m5.runtime.contracts import (
        M5AttemptDisposition,
        M5AttemptResultArtifact,
        M5JobState,
    )
    from tests.m5.postgres_runtime.d34_requirement_completion import test_history as h

    h._seal_genuine_baseline(db)
    old_source, old_args, _ = next(
        call for call in db.calls if call[0].job.pair.chunk_version_id == db.chunks[0]
    )
    with db.connection.cursor() as cursor:
        old_result = verifier._load_attempt_result(
            cursor,
            attempt_id=old_source.lease.attempt.attempt_id,
        ).artifact
    old_map = _native_archive_map(old_source, old_result)
    h._open_genuine_delete_fallback(db)
    fresh = db.build(db.chunks[0])
    fresh_result = M5AttemptResultArtifact.build(
        attempt_output=fresh.attempt_output,
        job_state_at_receipt=M5JobState.RUNNING,
        job_state_after=M5JobState.COMPLETED_ACTIVE,
        disposition=M5AttemptDisposition.VERIFIER_COMPLETED_ACTIVE,
        activity_snapshot_epoch_id=db.runtime.epoch_id,
        activity_snapshot_revision=db.revision(),
        epoch_active=True,
        chunk_active=True,
        requirement_active=True,
        group_active=True,
        archive_reason=None,
    )
    fresh_map = _native_archive_map(fresh, fresh_result)
    assert old_args[0] != db.runtime.epoch_id
    assert old_source.lease.attempt.attempt_id != fresh.lease.attempt.attempt_id
    return old_source, old_args, old_map, fresh, fresh_map


def test_fresh_archive_map_rejects_older_epoch_foreign_and_partial_aliases(
    c2_db: Any,
) -> None:
    """Labelled negative maps, not successful fabricated completion histories.

    R/P come from a real new child. One V/O/E coordinate deliberately names an
    older real closure. A V-only partial projection is tested without persisting
    an orphan forbidden by the existing deferred guards.
    """
    from groundloop.m5.runtime import postgres_matching as matching

    db = c2_db
    old, old_args, old_map, fresh, fresh_map = _old_and_fresh_component_maps(db)
    observation_id = old.verifier_artifact.to_semantic_observation().observation_id
    assert db.connection.execute(
        "SELECT produced_epoch,eligible_for_currency "
        "FROM groundloop_semantic_observation "
        "WHERE observation_id=%s",
        (observation_id,),
    ).fetchone() == (old_args[0], True)
    assert db.connection.execute(
        "SELECT attempt_id FROM groundloop_m5_requirement_verifier_execution "
        "WHERE artifact_id=%s",
        (old.verifier_artifact.artifact_id,),
    ).fetchone() == (old.lease.attempt.attempt_id,)
    with db.connection.transaction(), db.connection.cursor() as cursor:
        for target in old_map:
            if target.relation in {
                "groundloop_m5_attempt_result_artifact",
                "groundloop_m5_requirement_pair_input",
            }:
                continue
            combined = tuple(
                target
                if (c.relation, c.columns) == (target.relation, target.columns)
                else c
                for c in fresh_map
            )
            assert len(combined) == 15
            assert not any("epoch" in column for c in combined for column in c.columns)
            trace = _ArchiveTraceCursor(cursor)
            matching._d34_reserve_archive(trace, combined)
            with pytest.raises(EventConflictError, match="occupied"):
                matching._d34_preflight_archive(trace, combined)
            assert not any(
                "FOR UPDATE" in s
                or "FOR KEY SHARE" in s
                or s.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
                for s in trace.statements
            )
            assert any(target.relation in s for s in trace.statements)
            if target.relation == "groundloop_m5_requirement_verifier_artifact":
                pair = next(c for c in fresh_map if c.columns == ("pair_input_hash",))
                with pytest.raises(EventConflictError, match="occupied"):
                    matching._d34_preflight_archive(trace, (pair, target))
    # No candidate source/archive/state was installed by these negative maps.
    assert db.connection.execute(
        "SELECT count(*) FROM groundloop_m5_attempt_result_artifact "
        "WHERE attempt_id=%s",
        (fresh.lease.attempt.attempt_id,),
    ).fetchone() == (0,)


def test_committed_native_closure_every_global_alias_probe(c2_db: Any) -> None:
    """DB-backed component probes; not a fabricated cross-epoch completion."""
    from dataclasses import replace

    from groundloop.m5.runtime import postgres_matching as matching
    from groundloop.m5.runtime import postgres_verifier as verifier

    db = c2_db
    source = db.build(db.chunks[0])
    db.complete(source)
    with db.connection.transaction(), db.connection.cursor() as cursor:
        result = verifier._load_attempt_result(
            cursor, attempt_id=source.lease.attempt.attempt_id
        ).artifact
        pair = source.pair_input
        artifact = source.verifier_artifact
        observation = artifact.to_semantic_observation()
        coordinates = matching._d34_archive_coordinates(
            result=(
                result.attempt_result_artifact_id,
                result.attempt_result_artifact_hash,
                source.attempt_output.attempt_output_digest,
                result.attempt_id,
            ),
            pair=(
                pair.pair_input_hash,
                pair.semantic_pair_digest,
                pair.scope_contract_digest,
                pair.candidate_policy_id,
            ),
            verifier=(artifact.artifact_id, artifact.artifact_hash),
            observation=(
                observation.observation_id,
                "requirement",
                observation.subject_id,
                observation.chunk_version_id,
                observation.task_type,
            ),
            execution=(
                observation.observation_id,
                artifact.artifact_id,
                source.job.logical_job_id,
                result.attempt_id,
                pair.pair_input_hash,
            ),
        )
        assert len(coordinates) == 15
        pair_key = next(c for c in coordinates if c.columns == ("pair_input_hash",))
        for coordinate in coordinates:
            trace = _ArchiveTraceCursor(cursor)
            if coordinate.relation == "groundloop_m5_requirement_pair_input":
                matching._d34_preflight_archive(trace, (pair_key, coordinate))
                # A foreign expected P identity must reject the occupied global
                # semantic/scope/policy alias, without inventing an invalid row.
                if coordinate.columns == (
                    "semantic_pair_digest",
                    "scope_contract_digest",
                    "candidate_policy_id",
                ):
                    foreign = replace(pair_key, values=("f" * 64,))
                    with pytest.raises(EventConflictError, match="occupied"):
                        matching._d34_preflight_archive(trace, (foreign, coordinate))
            else:
                with pytest.raises(EventConflictError, match="occupied"):
                    matching._d34_preflight_archive(trace, (pair_key, coordinate))
            assert not any(
                "FOR UPDATE" in s
                or "FOR KEY SHARE" in s
                or s.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
                for s in trace.statements
            )


@pytest.mark.parametrize("buckets", [1, 2])
def test_actual_archive_physical_hash_collisions_do_not_merge_logical_aliases(
    c2_db: Any, monkeypatch: pytest.MonkeyPatch, buckets: int
) -> None:
    import inspect

    from groundloop.m5.runtime import postgres_matching as matching
    from tests.m5.postgres_runtime.d34_requirement_completion.conftest import (
        capture_sql,
    )

    original_sha = matching.hashlib.sha256
    aliases: list[bytes] = []

    class CollidingHash:
        def __init__(self, original: Any, physical: int) -> None:
            self.original, self.physical = original, physical

        def digest(self) -> bytes:
            return (
                self.physical.to_bytes(4, "big", signed=True)
                + self.original.digest()[4:]
            )

    def collision_sha(data: bytes = b"", *args: Any, **kw: Any) -> Any:
        original = original_sha(data, *args, **kw)
        if inspect.currentframe().f_back.f_code.co_name == "_d34_archive_coordinates":
            aliases.append(data)
            return CollidingHash(
                original, -7 if original.digest()[4] % buckets == 0 else 3
            )
        return original

    db = c2_db
    source = db.build(db.chunks[0])
    with monkeypatch.context() as patch:
        patch.setattr(matching.hashlib, "sha256", collision_sha)
        with capture_sql(db.connection) as trace:
            db.complete(source)
    assert len(set(aliases)) == 15
    locks = [
        params[1]
        for statement, params, _ in trace
        if "pg_advisory_xact_lock" in statement
        and params
        and params[0] == matching._d34_ARCHIVE_NAMESPACE
    ]
    assert locks == ([-7] if buckets == 1 else [-7, 3])
    with db.runtime.reconnect() as reader:
        assert db.complete(source, connection=reader).exact_replay


@pytest.mark.parametrize("label", ["SUPPORT", "REFUTE", "NEUTRAL"])
def test_actual_archive_prefix_tail_native_phases_and_guards_last(
    c2_db: Any, label: str
) -> None:
    from groundloop.m5.runtime import postgres_matching as matching
    from tests.m5.postgres_runtime.d34_requirement_completion.conftest import (
        capture_sql,
    )

    db = c2_db
    source = db.build(db.chunks[0], label)
    with capture_sql(db.connection) as trace:
        db.complete(source)

    def locate(needle: str, mode: str = "") -> int:
        return next(
            i
            for i, (s, _, _) in enumerate(trace)
            if needle in s and (not mode or s.lstrip().upper().startswith(mode))
        )

    archives = (
        "groundloop_m5_attempt_result_artifact (",
        "groundloop_m5_requirement_pair_input (",
        "groundloop_m5_requirement_verifier_artifact (",
        "groundloop_semantic_observation (",
        "groundloop_m5_requirement_verifier_execution (",
    )
    positions = [locate(name, "INSERT") for name in archives]
    assert positions == sorted(positions)
    import re

    # These are the actual common writer's fifteen bounded global probes, not
    # a replay of a hand-authored alias list against a mock cursor.
    probes = [
        (i, re.search(r'FROM "([a-z0-9_]+)" WHERE', s)[1])
        for i, (s, _, _) in enumerate(trace)
        if re.match(r'SELECT (?:1|"pair_input_hash") FROM "', s)
        and re.search(r'FROM "([a-z0-9_]+)" WHERE', s)
    ]
    relation_order = [name.split(" (")[0] for name in archives]
    assert [
        sum(name == relation for _, name in probes) for relation in relation_order
    ] == [4, 3, 2, 2, 4]
    assert len(probes) == 15 and max(i for i, _ in probes) < positions[0]
    assert [relation_order.index(name) for _, name in probes] == sorted(
        relation_order.index(name) for _, name in probes
    )
    tail_locks = [
        i
        for i, (s, _, _) in enumerate(trace)
        if any(
            "FROM " + relation in s or 'FROM "' + relation + '"' in s
            for relation in relation_order
        )
        and ("FOR UPDATE" in s or "FOR KEY SHARE" in s)
    ]
    if tail_locks:
        assert max(i for i, _ in probes) < min(tail_locks)
    locks = [
        (i, params[1])
        for i, (s, params, _) in enumerate(trace)
        if "pg_advisory_xact_lock" in s
        and params
        and params[0] == matching._d34_ARCHIVE_NAMESPACE
    ]
    assert locks and [key for _, key in locks] == sorted({key for _, key in locks})
    assert max(i for i, _ in locks) < positions[0]
    assert locate("groundloop_m5_attempt_execution_evidence (", "INSERT") < min(
        i for i, _ in locks
    )
    chunk_lock = next(
        i
        for i, (s, _, _) in enumerate(trace)
        if "FROM groundloop_chunk_version" in s and "FOR UPDATE" in s
    )
    assert (
        locate("SELECT groundloop_m5_authorize_persisted_matching_transition(")
        < chunk_lock
    )
    assert locate("groundloop_m5_matching_image_current", "SELECT") > positions[-1]
    assert not any(
        "FROM groundloop_semantic_observation" in s and "FOR UPDATE" in s
        for s, _, _ in trace
    )
    assert (
        sum(
            "groundloop_semantic_observation (" in s and s.lstrip().startswith("INSERT")
            for s, _, _ in trace
        )
        == 1
    )
    assert not any(
        "COUNT(" in s.upper()
        and any(
            name in s
            for name in (
                "groundloop_m5_semantic_job",
                "groundloop_m5_discovery_scope",
                "groundloop_m5_runtime_work_contribution",
                "groundloop_m5_matching_work_contribution",
            )
        )
        and "pg_catalog" not in s
        for s, _, _ in trace
    )
    assert not any(
        re.search(r"\b(?:FROM|JOIN)\s+groundloop_m5_\w*oracle\b", s, re.I)
        for s, _, _ in trace
    )
    assert [s for s, _, _ in trace][-2:] == [
        "SET CONSTRAINTS ALL IMMEDIATE",
        "SET CONSTRAINTS ALL DEFERRED",
    ]


def test_exact_pair_only_reuse_preserves_row_and_omits_second_insert(
    c2_db: Any,
) -> None:
    from psycopg import sql

    from groundloop.m5.runtime import postgres_verifier as verifier
    from groundloop.m5.runtime.postgres_roots import _lock_epoch
    from tests.m5.postgres_runtime.d34_requirement_completion.conftest import (
        capture_sql,
    )

    db = c2_db
    source = db.build(db.chunks[0])
    values = verifier._pair_input_values(source.pair_input)
    columns = (
        "pair_input_hash",
        "subject_kind",
        "subject_id",
        "chunk_version_id",
        "semantic_pair_digest",
        "scope_contract_digest",
        "candidate_policy_id",
        "owner_claim_id",
        "group_version_id",
        "group_family_id",
        "requirement_ordinal",
        "normalized_requirement_text",
        "requirement_text_hash",
        "document_version_id",
        "chunk_index",
        "chunk_text",
        "stored_chunk_text_hash",
        "m5_chunk_text_hash",
        "chunker_artifact_id",
        "normalizer_id",
        "normalizer_provenance_hash",
    )
    with db.connection.transaction(), db.connection.cursor() as cursor:
        header = _lock_epoch(cursor, db.runtime.epoch_id)
        verifier._d34_matching._authorize_checked_prefix(
            cursor, epoch_id=header.epoch_id, expected_revision=header.revision
        )
        verifier._d34_lock_document_parents(cursor, source.pair_input)
        verifier._d34_lock_provenance_parents(
            cursor, source.pair_input, source.verifier_artifact
        )
        cursor.execute(
            sql.SQL(
                "INSERT INTO groundloop_m5_requirement_pair_input ({}) VALUES ({})"
            ).format(
                sql.SQL(",").join(map(sql.Identifier, columns)),
                sql.SQL(",").join(sql.Placeholder() for _ in values),
            ),
            values,
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    before = db.connection.execute(
        "SELECT xmin::text FROM groundloop_m5_requirement_pair_input WHERE "
        "pair_input_hash=%s",
        (values[0],),
    ).fetchone()
    with capture_sql(db.connection) as trace:
        db.complete(source)
    assert not any(
        "groundloop_m5_requirement_pair_input (" in s
        and s.lstrip().startswith("INSERT")
        for s, _, _ in trace
    )
    assert (
        db.connection.execute(
            "SELECT xmin::text FROM groundloop_m5_requirement_pair_input WHERE "
            "pair_input_hash=%s",
            (values[0],),
        ).fetchone()
        == before
    )


@pytest.mark.parametrize(
    "setting",
    [
        "ambient",
        "savepoint",
        "read-only",
        "repeatable-read",
        "temp-shadow",
        "builtin-shadow",
        "digest-shadow",
    ],
)
def test_private_entry_rejects_ambient_settings_and_shadowing_without_writes(
    c2_db: Any, setting: str
) -> None:
    from psycopg import sql

    from groundloop.errors import ValidationError
    from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot

    db = c2_db
    source = db.build(db.chunks[0])
    before = database_snapshot(db.connection)
    if setting in {"ambient", "savepoint"}:
        with db.connection.transaction():
            if setting == "savepoint":
                with db.connection.transaction():
                    with pytest.raises(ValidationError):
                        db.complete(source)
            else:
                with pytest.raises(ValidationError):
                    db.complete(source)
    elif setting in {"read-only", "repeatable-read"}:
        command = (
            "READ ONLY" if setting == "read-only" else "ISOLATION LEVEL REPEATABLE READ"
        )
        db.connection.execute("SET SESSION CHARACTERISTICS AS TRANSACTION " + command)
        try:
            with pytest.raises(ValidationError):
                db.complete(source)
        finally:
            db.connection.execute(
                "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE, "
                "ISOLATION LEVEL READ COMMITTED"
            )
    else:
        with db.connection.transaction():
            if setting == "temp-shadow":
                db.connection.execute(
                    "CREATE TEMP TABLE groundloop_epoch (epoch_id bigint)"
                )
            elif setting == "builtin-shadow":
                db.connection.execute(
                    sql.SQL(
                        "CREATE FUNCTION {}() RETURNS int LANGUAGE sql AS 'SELECT 0'"
                    ).format(sql.Identifier(db.schema, "pg_backend_pid"))
                )
            else:
                db.connection.execute(
                    sql.SQL(
                        "CREATE FUNCTION {}(bytea,text) RETURNS bytea LANGUAGE "
                        "sql AS 'SELECT $1'"
                    ).format(sql.Identifier(db.schema, "digest"))
                )
            # The fixture-only shadow is committed before the owned entrypoint.
        try:
            with pytest.raises(ValidationError):
                db.complete(source)
        finally:
            if setting == "temp-shadow":
                db.connection.execute("DROP TABLE pg_temp.groundloop_epoch")
            elif setting == "builtin-shadow":
                db.connection.execute(
                    sql.SQL("DROP FUNCTION {}()").format(
                        sql.Identifier(db.schema, "pg_backend_pid")
                    )
                )
            else:
                db.connection.execute(
                    sql.SQL("DROP FUNCTION {}(bytea,text)").format(
                        sql.Identifier(db.schema, "digest")
                    )
                )
    assert database_snapshot(db.connection) == before


class _ArchiveTraceCursor:
    def __init__(self, cursor: Any) -> None:
        self.cursor = cursor
        self.statements: list[str] = []
        self.parameters: list[Any] = []

    def execute(self, statement: Any, parameters: Any = None) -> Any:
        self.parameters.append(parameters)
        self.statements.append(
            statement
            if isinstance(statement, str)
            else statement.as_string(self.cursor.connection)
        )
        return self.cursor.execute(statement, parameters)


def test_common_writer_rejects_existing_source_before_any_tail_lock_or_write(
    requirement_completion_database: Any,
) -> None:
    database = requirement_completion_database
    fixture = database.verifier
    with database.connection.cursor() as cursor:
        _authorize_matching_transition(
            cursor,
            epoch_id=database.epoch_id,
            expected_runtime_revision=database.expected_revision,
            resulting_revision=database.resulting_revision,
            source_kind=M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION,
            source_id=database.attempt_id,
        )
        result = _lock_requirement_completion_source_prefix(cursor, database)
        # The real common writer has already installed the checked preterminal
        # closure, but no matching derive/tail or terminalization has occurred.
        # Re-entry is NOT replay and cannot adopt even its identical R/V/O/E.
        trace = _ArchiveTraceCursor(cursor)
        with pytest.raises(EventConflictError, match="occupied"):
            _persist_verifier_closure(
                trace,  # type: ignore[arg-type]
                epoch_id=database.epoch_id,
                job=fixture.job,
                attempt_id=database.attempt_id,
                pair_input=fixture.pair_input,
                artifact=fixture.verifier_artifact,
                attempt_result=result,
                attempt_output=fixture.attempt_output,
                eligible_for_currency=True,
            )
        assert any("pg_advisory_xact_lock" in sql for sql in trace.statements)
        assert any(
            "groundloop_m5_attempt_result_artifact" in sql for sql in trace.statements
        )
        assert not any(
            "FOR UPDATE" in sql.upper()
            or "FOR KEY SHARE" in sql.upper()
            or sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for sql in trace.statements
        )
        assert cursor.execute(
            """SELECT attempt_state,attempt_output_digest
               FROM groundloop_m5_job_attempt WHERE attempt_id=%s""",
            (database.attempt_id,),
        ).fetchone() == (
            "result_reserved",
            fixture.attempt_output.attempt_output_digest,
        )


@pytest.mark.parametrize(
    "phase",
    (
        "top",
        "active-child",
        "released-child",
        "rolled-back-child",
        "released-descendant-parent-abort",
    ),
)
def test_real_archive_source_tracks_native_transaction_family(
    requirement_completion_database: Any, phase: str
) -> None:
    """Real immutable source writes; no orphan, altered xmin or fake SQL closure."""
    import psycopg

    from groundloop.m5.runtime import postgres_matching as matching

    db = requirement_completion_database
    with db.connection.transaction(), db.connection.cursor() as cursor:
        _authorize_matching_transition(
            cursor,
            epoch_id=db.epoch_id,
            expected_runtime_revision=db.expected_revision,
            resulting_revision=db.resulting_revision,
            source_kind=M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION,
            source_id=db.attempt_id,
        )
        if phase != "top":
            cursor.execute("SAVEPOINT d34_native_parent")
        if phase == "released-descendant-parent-abort":
            cursor.execute("SAVEPOINT d34_native_descendant")
        result = _lock_requirement_completion_source_prefix(cursor, db)
        coordinates = _native_archive_map(db.verifier, result)
        before = cursor.execute(
            "SELECT xmin::text::bigint, pg_catalog.pg_current_xact_id()::text "
            "FROM groundloop_m5_attempt_result_artifact "
            "WHERE attempt_result_artifact_id=%s",
            (result.attempt_result_artifact_id,),
        ).fetchone()
        assert before is not None
        assert (before[0] == int(before[1]) % 2**32) is (phase == "top")
        if phase == "released-child":
            cursor.execute("RELEASE SAVEPOINT d34_native_parent")
            assert (
                cursor.execute(
                    "SELECT 1 FROM pg_catalog.pg_locks "
                    "WHERE pid=pg_catalog.pg_backend_pid() "
                    "AND locktype='transactionid' "
                    "AND transactionid::text=%s AND mode='ExclusiveLock' AND granted",
                    (str(before[0]),),
                ).fetchall()
                == []
            )
        if phase == "released-descendant-parent-abort":
            cursor.execute("RELEASE SAVEPOINT d34_native_descendant")
        if phase in {"rolled-back-child", "released-descendant-parent-abort"}:
            cursor.execute("ROLLBACK TO SAVEPOINT d34_native_parent")
            cursor.execute("RELEASE SAVEPOINT d34_native_parent")
            # Reacquire only reservations lost on abort; never restore source rows.
            matching._d34_reserve_archive(cursor, coordinates)
            with pytest.raises(EventConflictError, match="not owned by the current"):
                matching._d34_require_current_archive_rows(cursor, coordinates)
            assert (
                cursor.execute(
                    "SELECT 1 FROM groundloop_m5_attempt_result_artifact "
                    "WHERE attempt_result_artifact_id=%s",
                    (result.attempt_result_artifact_id,),
                ).fetchall()
                == []
            )
        else:
            matching._d34_require_current_archive_rows(cursor, coordinates)
        # This is a source-prefix probe, never commit its preterminal closure.
        raise psycopg.Rollback()


def test_real_committed_archive_is_not_a_fresh_family_source(c2_db: Any) -> None:
    from groundloop.m5.runtime import postgres_matching as matching
    from groundloop.m5.runtime import postgres_verifier as verifier

    db = c2_db
    source = db.build(db.chunks[0])
    db.complete(source)
    with db.connection.transaction(), db.connection.cursor() as cursor:
        result = verifier._load_attempt_result(
            cursor, attempt_id=source.lease.attempt.attempt_id
        ).artifact
        coordinates = _native_archive_map(source, result)
        matching._d34_reserve_archive(cursor, coordinates)
        with pytest.raises(EventConflictError, match="not owned by the current"):
            matching._d34_require_current_archive_rows(cursor, coordinates)
