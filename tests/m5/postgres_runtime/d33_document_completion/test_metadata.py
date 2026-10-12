"""M33 source helpers over genuine M4 history, NOT public M5/C1 completion.

No authored M5 epoch/update/terminal headers, held C1 code, model requests or
disabled guards. M5 stays v1_only. Later progress is actual legacy-M4 history;
read-only closure projections below are not persisted M5 runtime authority.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, replace
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.errors import EventConflictError
from groundloop.events import ChunkInput, ReplaceDocumentVersionEvent
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
    bootstrap_m4_publication,
)
from groundloop.m5.events import legacy_event_payload_digest
from groundloop.m5.runtime import postgres_withdrawal as withdrawal
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
)
from groundloop.postgres.migrations import (
    apply_legacy_migrations,
    install_m5_bounded_document_withdrawal_bundle,
    install_m5_core_bundle,
    install_m5_persisted_matching_bundle,
    install_m5_runtime_bundle,
    install_m5_runtime_recovery_bundle,
)
from tests.m4.integration.test_postgres_pipeline import (
    ScriptedAdmission,
    ScriptedVerifier,
)
from tests.m5.postgres.helpers import SeededBase, seed_base, sha
from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot
from tests.m5.postgres_runtime.test_direct_m4_composition import (
    REGISTRY_ID,
    _m4_manifest,
    _m5_manifest,
)


@dataclass
class SourceDatabase:
    connection: Connection[Any]
    schema: str
    dsn: str
    base: SeededBase
    policy: Any

    @property
    def execution(self) -> ApplicationExecutionPolicy:
        return ApplicationExecutionPolicy(
            sha("m33-impact"),
            sha("m33-frontier"),
            self.policy.verifier_execution_spec_hash,
        )

    def plan(
        self,
        label: str = "first",
        *,
        old: str | None = None,
        previous: int | None = None,
    ) -> M5TypedEventPlan:
        connection = self.connection
        version = old or "m33-document-v1"
        previous_epoch = self.base.epoch_id if previous is None else previous
        old_chunks = tuple(
            row[0]
            for row in connection.execute(
                "SELECT chunk_version_id FROM groundloop_chunk_version "
                "WHERE document_version_id = %s "
                'ORDER BY chunk_version_id COLLATE "C"',
                (version,),
            ).fetchall()
        )
        chunks = tuple(
            ChunkInput(
                f"m33-{label}-chunk-{index}",
                index,
                f"replacement {label} evidence {index}",
            )
            for index in range(2)
        )
        event = ReplaceDocumentVersionEvent(
            f"m33-{label}",
            "m33-document",
            version,
            f"m33-{label}-version",
            sha(f"m33-{label}-content"),
            chunks,
        )
        payload_hash = legacy_event_payload_digest(event)
        direct = DynamicEventPlan(
            CorpusUpdateIdentity(
                event.event_id,
                payload_hash,
                UpdateKind.REPLACE,
                previous_epoch,
                self.policy.candidate_policy_id,
            ),
            tuple(chunk.chunk_version_id for chunk in chunks),
            old_chunks,
            (),
            REGISTRY_ID,
        )
        return M5TypedEventPlan(
            event.event_id,
            event,
            payload_hash,
            direct,
            self.policy.candidate_policy_id,
            self.policy.manifest_hash,
            RequirementRegistrySnapshot.build(()),
            ActiveChunkSnapshot.build(
                tuple(
                    ActiveChunkSnapshotEntry.build(
                        chunk_version_id=chunk.chunk_version_id, chunk_text=chunk.text
                    )
                    for chunk in chunks
                )
            ),
            previous_epoch,
        )

    def publish(self, event: M5TypedEventPlan) -> int:
        with self.connection.cursor() as cursor:
            payload = withdrawal._document_structural_payload(cursor, event)
        ports = PostgresM4ApplicationPorts(
            self.connection,
            structural_payloads={event.structural_event_id: payload},
            execution_mode=M4ExecutionMode.MEASURED,
        )
        admission = ScriptedAdmission({})
        verifier = ScriptedVerifier({})
        application = M4Application(
            structural=ports,
            runtime=ports,
            admission=PersistingAdmissionPort(self.connection, admission),
            verifier=verifier,
            observations=ports,
            equality_gates=ports,
            publication=ports,
            execution_policy=self.execution,
        )
        assert event.direct_plan is not None
        result = application.run_event(event.direct_plan)
        assert result.state is EventRunState.SEALED
        assert admission.calls == len(event.event.chunks)
        assert verifier.calls == 0
        return result.epoch_id

    def source_coordinates(self, event: M5TypedEventPlan, epoch: int) -> Any:
        roots = tuple(
            row[0]
            for row in self.connection.execute(
                "SELECT job_id FROM groundloop_semantic_job WHERE epoch_id = %s "
                'AND parent_job_id IS NULL ORDER BY job_id COLLATE "C"',
                (epoch,),
            ).fetchall()
        )
        scopes = tuple(
            row[0]
            for row in self.connection.execute(
                "SELECT root_job_id FROM groundloop_discovery_scope "
                'WHERE epoch_id = %s ORDER BY root_job_id COLLATE "C"',
                (epoch,),
            ).fetchall()
        )
        return SimpleNamespace(
            epoch_id=epoch,
            previous_epoch_id=event.expected_previous_published_epoch_id,
            runtime_state="sealed",
            candidate_policy=self.policy,
            direct_root_job_ids=roots,
            direct_scope_root_job_ids=scopes,
            direct_fallback_claim_ids=(),
        )


@pytest.fixture
def source_db(request: pytest.FixtureRequest) -> Iterator[SourceDatabase]:
    dsn = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("M33 live source-helper gate requires owned test PostgreSQL DSN")
    schema = f"groundloop_m33_source_{uuid.uuid4().hex}"
    pair = getattr(request, "param", ("https://例子.hk/α?x=é", "journal"))
    with psycopg.connect(dsn, autocommit=True) as connection:
        try:
            connection.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
            )
            connection.execute(
                sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
            )
            with connection.transaction():
                apply_legacy_migrations(connection)
            for installer in (
                install_m5_core_bundle,
                install_m5_runtime_bundle,
                install_m5_runtime_recovery_bundle,
                install_m5_persisted_matching_bundle,
                install_m5_bounded_document_withdrawal_bundle,
            ):
                installer(connection)
            with connection.transaction():
                base = seed_base(
                    connection,
                    prefix="m33",
                    chunk_texts=("old source evidence",),
                    epoch_revision=1,
                )
                connection.execute(
                    "UPDATE groundloop_document SET source_uri = %s, "
                    "authority_class = %s WHERE document_id = 'm33-document'",
                    pair,
                )
                connection.execute(
                    "INSERT INTO groundloop_model_artifact (model_artifact_id, "
                    "task, provider, model_id, immutable_revision, "
                    "tokenizer_revision, license_id, config_hash) VALUES "
                    "('direct-m4-embedding', 'embedding', 'fixture', "
                    "'direct-m4-embedding', 'v1', 'v1', 'MIT', %s)",
                    (sha("m33-embedding"),),
                )
                connection.execute(
                    "INSERT INTO groundloop_chunker_artifact "
                    "(chunker_artifact_id, chunker_version, normalization_version, "
                    "config_hash) VALUES "
                    "('m33-source-chunker', 'fixture-v1', 'v1', %s)",
                    (sha("m33-chunker"),),
                )
                for chunk in base.chunk_ids:
                    connection.execute(
                        "INSERT INTO groundloop_chunk_provenance "
                        "(chunk_version_id, chunker_artifact_id, input_hash) "
                        "VALUES (%s, 'm33-source-chunker', %s)",
                        (chunk, sha("m33-source-input")),
                    )
            bootstrap_m4_publication(connection, sealed_epoch_id=base.epoch_id)
            policy = _m5_manifest(base)
            ports = PostgresM4ApplicationPorts(
                connection,
                structural_payloads={},
                execution_mode=M4ExecutionMode.MEASURED,
            )
            ports.runtime_store.register_candidate_policy(_m4_manifest(base, policy))
            ports.register_claim_registry_snapshot(REGISTRY_ID, base.claim_ids)
            yield SourceDatabase(connection, schema, dsn, base, policy)
        finally:
            connection.execute("SET search_path TO public")
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )


class SelectOnlyCursor:
    def __init__(self, cursor: Any) -> None:
        self.cursor = cursor
        self.statements: list[str] = []

    def execute(self, statement: Any, *args: Any, **kwargs: Any) -> Any:
        source = str(statement)
        assert source.lstrip().startswith("SELECT"), source
        assert "FOR UPDATE" not in source
        assert "publication_head" not in source
        self.statements.append(source)
        return self.cursor.execute(statement, *args, **kwargs)


class MetadataFaultCursor(SelectOnlyCursor):
    """Alter one returned field only; real immutable SQL rows stay untouched."""

    def __init__(self, cursor: Any, location: str, field: str) -> None:
        super().__init__(cursor)
        self.location = location
        self.field = field
        self.rows: list[Any] = []

    def execute(self, statement: Any, *args: Any, **kwargs: Any) -> Any:
        result = super().execute(statement, *args, **kwargs)
        self.rows = result.fetchall()
        source = str(statement)
        if self.location == "manifest" and "SELECT manifest" in source:
            self.rows[0][0][withdrawal._M4_RUNTIME_MANIFEST_KEY]["event_manifest"][
                self.field
            ] = "wrong"
        elif (
            self.location == "overlay"
            and "FROM groundloop_m4_document_metadata_overlay" in source
        ) or (
            self.location == "public"
            and "JOIN groundloop_document AS document" in source
        ):
            row = list(self.rows[0])
            offset = 2 if self.location == "overlay" else 1
            row[offset + (0 if self.field == "source_uri" else 1)] = "wrong"
            self.rows = [tuple(row)]
        return self

    def fetchone(self) -> Any:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[Any]:
        return self.rows


@pytest.mark.parametrize(
    "source_db",
    [(None, "journal"), ("https://例子.hk/α?x=é", " 政府 "), ("", "archival")],
    indirect=True,
)
def test_genuine_replacement_preview_publication_and_retained_source(
    source_db: SourceDatabase,
) -> None:
    database = source_db
    connection = database.connection
    event = database.plan()
    before = database_snapshot(connection)
    with connection.transaction(), connection.cursor() as cursor:
        preview = withdrawal._preview_document_direct_open(
            cursor, event, execution_policy=database.execution
        )
        cursor.execute(
            "SELECT document_id FROM groundloop_document "
            "WHERE document_id = 'm33-document' FOR UPDATE"
        )
        locked = withdrawal._document_structural_payload(cursor, event)
        assert preview.structural_payload == locked
    assert database_snapshot(connection) == before
    epoch = database.publish(event)
    closure = database.source_coordinates(event, epoch)
    before_replay = database_snapshot(connection)
    with connection.transaction(), connection.cursor() as cursor:
        assert (
            withdrawal._validated_structural_source_chunks(
                cursor, event, closure, lock_authority=True
            )
            == database.base.chunk_ids
        )
    assert database_snapshot(connection) == before_replay
    with connection.cursor() as raw_cursor:
        cursor = SelectOnlyCursor(raw_cursor)
        retained = withdrawal._document_structural_payload(
            cursor, event, epoch_id=epoch
        )
        assert retained == locked
        assert (
            withdrawal._validated_structural_source_chunks(
                cursor, event, closure, lock_authority=False, event_local_only=True
            )
            == database.base.chunk_ids
        )
        assert (
            withdrawal._stored_direct_open(cursor, event, closure).structural_payload
            == retained
        )
        assert cursor.statements
    assert database_snapshot(connection) == before_replay
    assert connection.execute(
        "SELECT source_uri, authority_class FROM groundloop_document "
        "WHERE document_id = 'm33-document'"
    ).fetchone() == (locked.inserted.source_uri, locked.inserted.authority_class)


def test_original_source_reconstructs_after_later_legal_m4_replacement_and_reconnect(
    source_db: SourceDatabase,
) -> None:
    database = source_db
    first = database.plan()
    epoch = database.publish(first)
    original_overlay = database.connection.execute(
        "SELECT * FROM groundloop_m4_document_metadata_overlay WHERE epoch_id = %s",
        (epoch,),
    ).fetchall()
    original_manifest = database.connection.execute(
        "SELECT manifest FROM groundloop_m4_update WHERE epoch_id = %s", (epoch,)
    ).fetchone()
    later = database.plan(
        "later", old=first.event.new_document_version_id, previous=epoch
    )
    later_epoch = database.publish(later)
    assert database.connection.execute(
        "SELECT document_version_id, valid_to_epoch "
        "FROM groundloop_document_version WHERE document_id = 'm33-document' "
        "ORDER BY valid_from_epoch"
    ).fetchall() == [
        ("m33-document-v1", epoch),
        (first.event.new_document_version_id, later_epoch),
        (later.event.new_document_version_id, None),
    ]
    with psycopg.connect(database.dsn, autocommit=True) as restarted:
        restarted.execute(
            sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(database.schema)
            )
        )
        reconnected = replace(database, connection=restarted)
        before = database_snapshot(restarted)
        closure = reconnected.source_coordinates(first, epoch)
        with restarted.cursor() as raw:
            cursor = SelectOnlyCursor(raw)
            payload = withdrawal._document_structural_payload(
                cursor, first, epoch_id=epoch
            )
            assert (
                withdrawal._validated_structural_source_chunks(
                    cursor, first, closure, lock_authority=False, event_local_only=True
                )
                == database.base.chunk_ids
            )
            assert (
                withdrawal._stored_direct_open(
                    cursor, first, closure
                ).structural_payload
                == payload
            )
        assert database_snapshot(restarted) == before
        assert (
            restarted.execute(
                "SELECT * FROM groundloop_m4_document_metadata_overlay "
                "WHERE epoch_id = %s",
                (epoch,),
            ).fetchall()
            == original_overlay
        )
        assert (
            restarted.execute(
                "SELECT manifest FROM groundloop_m4_update WHERE epoch_id = %s",
                (epoch,),
            ).fetchone()
            == original_manifest
        )


def test_historical_interval_allowance_rejects_mixed_or_invalid_closures(
    source_db: SourceDatabase,
) -> None:
    database = source_db
    first = database.plan()
    epoch = database.publish(first)
    later = database.plan(
        "later", old=first.event.new_document_version_id, previous=epoch
    )
    later_epoch = database.publish(later)
    closure = database.source_coordinates(first, epoch)
    before = database_snapshot(database.connection)

    class IntervalFaultCursor(SelectOnlyCursor):
        def __init__(self, cursor: Any, mode: str) -> None:
            super().__init__(cursor)
            self.mode = mode
            self.rows: list[Any] = []

        def execute(self, statement: Any, *args: Any, **kwargs: Any) -> Any:
            result = super().execute(statement, *args, **kwargs)
            self.rows = result.fetchall()
            if "WHERE version.valid_from_epoch = %s" in str(statement):
                rows = [list(row) for row in self.rows]
                assert len(rows) == 2
                assert all(row[4] == row[11] == later_epoch for row in rows)
                if self.mode in {"at-event", "before-event", "bool"}:
                    bad = {"at-event": epoch, "before-event": epoch - 1, "bool": True}[
                        self.mode
                    ]
                    for row in rows:
                        row[4] = row[11] = bad
                elif self.mode == "mixed-version":
                    rows[0][4] = None
                elif self.mode == "mixed-chunk":
                    rows[0][11] = None
                elif self.mode == "different-version":
                    rows[1][4] = later_epoch + 1
                else:
                    rows[1][11] = later_epoch + 1
                self.rows = [tuple(row) for row in rows]
            return self

        def fetchone(self) -> Any:
            return self.rows[0] if self.rows else None

        def fetchall(self) -> list[Any]:
            return self.rows

    for mode in (
        "at-event",
        "before-event",
        "bool",
        "mixed-version",
        "mixed-chunk",
        "different-version",
        "different-chunk",
    ):
        with database.connection.cursor() as raw:
            with pytest.raises(EventConflictError, match="interval|sidecar"):
                withdrawal._validated_structural_source_chunks(
                    IntervalFaultCursor(raw, mode),
                    first,
                    closure,
                    lock_authority=False,
                    event_local_only=True,
                )
    with database.connection.cursor() as raw:
        with pytest.raises(EventConflictError, match="historical interval"):
            withdrawal._validated_structural_source_chunks(
                SelectOnlyCursor(raw), first, closure, lock_authority=False
            )
    assert database_snapshot(database.connection) == before


@pytest.mark.parametrize("location", ["manifest", "overlay", "public"])
@pytest.mark.parametrize("field", ["source_uri", "authority_class"])
def test_live_each_metadata_conflict_rejects_without_additional_writes(
    source_db: SourceDatabase, location: str, field: str
) -> None:
    database = source_db
    connection = database.connection
    event = database.plan()
    epoch = database.publish(event)
    clean = database_snapshot(connection)
    with connection.cursor() as raw:
        with pytest.raises(EventConflictError, match="metadata|manifest"):
            withdrawal._document_structural_payload(
                MetadataFaultCursor(raw, location, field), event, epoch_id=epoch
            )
    assert database_snapshot(connection) == clean


def test_retained_overlay_really_is_immutable(source_db: SourceDatabase) -> None:
    database = source_db
    event = database.plan()
    epoch = database.publish(event)
    before = database_snapshot(database.connection)
    with pytest.raises(psycopg.errors.RaiseException, match="immutable M4 table"):
        with database.connection.transaction():
            database.connection.execute(
                "UPDATE groundloop_m4_document_metadata_overlay "
                "SET source_uri = 'wrong' WHERE epoch_id = %s",
                (epoch,),
            )
    assert database_snapshot(database.connection) == before


@pytest.mark.parametrize("field", ["source_uri", "authority_class"])
def test_changed_preview_cannot_satisfy_genuine_m4_promotion_guard(
    source_db: SourceDatabase, field: str
) -> None:
    database = source_db
    connection = database.connection
    event = database.plan()
    with connection.cursor() as cursor:
        preview = withdrawal._preview_document_direct_open(
            cursor, event, execution_policy=database.execution
        )
    assert preview.structural_payload is not None
    assert preview.structural_payload.inserted is not None
    changed = replace(
        preview.structural_payload,
        inserted=replace(preview.structural_payload.inserted, **{field: "caller-lie"}),
    )
    with connection.transaction(force_rollback=True):
        ports = PostgresM4ApplicationPorts(
            connection,
            structural_payloads={event.structural_event_id: changed},
            execution_mode=M4ExecutionMode.MEASURED,
        )
        admission = ScriptedAdmission({})
        verifier = ScriptedVerifier({})
        app = M4Application(
            structural=ports,
            runtime=ports,
            admission=PersistingAdmissionPort(connection, admission),
            verifier=verifier,
            observations=ports,
            equality_gates=ports,
            publication=ports,
            execution_policy=database.execution,
        )
        with pytest.raises(EventConflictError, match="metadata"):
            app.run_event(event.direct_plan)
    assert connection.execute(
        "SELECT count(*) FROM groundloop_epoch WHERE event_id = %s",
        (event.structural_event_id,),
    ).fetchone() == (0,)


@pytest.mark.parametrize("field", ["source_uri", "authority_class"])
def test_source_changed_after_preview_is_independently_rederived_under_lock(
    source_db: SourceDatabase, field: str
) -> None:
    database = source_db
    connection = database.connection
    event = database.plan()
    with connection.cursor() as cursor:
        preview = withdrawal._preview_document_direct_open(
            cursor, event, execution_policy=database.execution
        )
    # A second committed connection changes genuine public source, not a DTO.
    # This is an adversary in isolated v1_only, not authorized metadata mutation
    # in public M5. No immutable overlay or guard is modified/disabled.
    with psycopg.connect(database.dsn, autocommit=True) as other:
        other.execute(
            sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(database.schema)
            )
        )
        other.execute(
            sql.SQL(
                "UPDATE groundloop_document SET {} = 'changed-after-preview' "
                "WHERE document_id = 'm33-document'"
            ).format(sql.Identifier(field))
        )
    before = database_snapshot(connection)
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            "SELECT document_id FROM groundloop_document "
            "WHERE document_id = 'm33-document' FOR UPDATE"
        )
        cursor.execute(
            "SELECT document_version_id FROM groundloop_document_version "
            "WHERE document_version_id = %s FOR UPDATE",
            (event.event.old_document_version_id,),
        )
        locked_payload = withdrawal._document_structural_payload(cursor, event)
        assert getattr(locked_payload.inserted, field) == "changed-after-preview"
        assert preview.structural_payload != locked_payload
        exact = withdrawal._direct_open_from_withdrawal(
            event,
            database.execution,
            preview.withdrawal,
            structural_payload=locked_payload,
        )
        assert exact != preview
    assert database_snapshot(connection) == before


@pytest.mark.parametrize("mode", ["wrong-parent", "missing-old"])
def test_live_wrong_parent_or_missing_old_source_rejects_with_zero_write(
    source_db: SourceDatabase, mode: str
) -> None:
    database = source_db
    event = database.plan()
    changes = (
        {"document_id": "another-document"}
        if mode == "wrong-parent"
        else {"old_document_version_id": "missing-version"}
    )
    concrete = replace(event.event, **changes)
    payload_hash = legacy_event_payload_digest(concrete)
    changed = replace(
        event,
        event=concrete,
        payload_hash=payload_hash,
        direct_plan=replace(
            event.direct_plan,
            update=replace(event.direct_plan.update, payload_hash=payload_hash),
        ),
    )
    before = database_snapshot(database.connection)
    with database.connection.cursor() as raw:
        with pytest.raises(EventConflictError, match="owning document"):
            withdrawal._document_structural_payload(SelectOnlyCursor(raw), changed)
    assert database_snapshot(database.connection) == before


@pytest.mark.parametrize(
    "cut",
    [
        "structural_registry_written",
        "structural_versions_written",
        "structural_withdrawal_written",
        "structural_working_states_written",
        "structural_evaluation_written",
        "open_structural_written",
        "open_rows_written",
    ],
)
def test_genuine_m4_open_metadata_rolls_back_at_each_named_source_cut(
    source_db: SourceDatabase, cut: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = source_db
    connection = database.connection
    event = database.plan()
    with connection.cursor() as cursor:
        payload = withdrawal._document_structural_payload(cursor, event)
        direct = withdrawal._preview_document_direct_open(
            cursor, event, execution_policy=database.execution
        )
    before = database_snapshot(connection)
    reached: list[str] = []

    def fail(point: str) -> None:
        reached.append(point)
        if point == cut:
            raise RuntimeError("M33 named source cut")

    ports = PostgresM4ApplicationPorts(
        connection,
        structural_payloads={event.structural_event_id: payload},
        execution_mode=M4ExecutionMode.MEASURED,
        failure_injector=fail,
    )
    actual_open = ports.runtime_store.open_epoch

    def instrumented_open(*args: Any, **kwargs: Any) -> Any:
        return actual_open(*args, **kwargs, failure_injector=fail)

    monkeypatch.setattr(ports.runtime_store, "open_epoch", instrumented_open)
    with pytest.raises(RuntimeError, match="M33 named source cut"):
        ports.open_event(
            event.direct_plan,
            direct.withdrawal,
            direct.root_jobs,
            direct.discovery_scopes,
        )
    assert cut in reached
    # PostgreSQL sequences are intentionally nontransactional. Every table row
    # and xmin is identical; failed allocation is not claimed to roll back.
    after = database_snapshot(connection)
    assert tuple(row for row in after if not row[0].startswith("sequence:")) == tuple(
        row for row in before if not row[0].startswith("sequence:")
    )
    # A fresh real application proves the rolled-back metadata does not poison retry.
    database.publish(event)


def test_every_actual_m4_open_dml_cut_rolls_back_metadata_and_retry(
    source_db: SourceDatabase,
    record_testsuite_property: Any,
) -> None:
    database = source_db
    connection = database.connection
    event = database.plan()
    with connection.cursor() as cursor:
        payload = withdrawal._document_structural_payload(cursor, event)
        direct = withdrawal._preview_document_direct_open(
            cursor, event, execution_policy=database.execution
        )
    original_factory = connection.cursor_factory
    statements: list[str] = []
    cut: int | None = None

    class CutCursor(psycopg.Cursor[Any]):
        def execute(self, query: Any, *args: Any, **kwargs: Any) -> Any:
            result = super().execute(query, *args, **kwargs)
            if self.statusmessage and self.statusmessage.split()[0] in {
                "INSERT",
                "UPDATE",
                "DELETE",
            }:
                statements.append(
                    query
                    if isinstance(query, str)
                    else query.as_string(self.connection)
                )
                if cut is not None and len(statements) == cut:
                    raise RuntimeError("M33 per-DML cut")
            return result

    def open_real() -> None:
        ports = PostgresM4ApplicationPorts(
            connection,
            structural_payloads={event.structural_event_id: payload},
            execution_mode=M4ExecutionMode.MEASURED,
        )
        ports.open_event(
            event.direct_plan,
            direct.withdrawal,
            direct.root_jobs,
            direct.discovery_scopes,
        )

    def table_image() -> Any:
        return tuple(
            row
            for row in database_snapshot(connection)
            if not row[0].startswith("sequence:")
        )

    before = table_image()
    try:
        connection.cursor_factory = CutCursor
        with connection.transaction(force_rollback=True):
            open_real()
        count = len(statements)
        record_testsuite_property("m33_actual_open_dml_cuts", count)
        successful_trace = tuple(statements)
        assert count > 10
        assert sum(
            "INSERT INTO groundloop_chunk_version" in item for item in statements
        ) == len(event.event.chunks)
        assert any(
            "groundloop_m4_document_metadata_overlay" in item for item in statements
        )
        assert any("groundloop_m4_update" in item for item in statements)
        assert any("groundloop_discovery_scope" in item for item in statements)
        connection.cursor_factory = original_factory
        assert table_image() == before
        for cut in range(1, count + 1):
            statements.clear()
            connection.cursor_factory = CutCursor
            with pytest.raises(RuntimeError, match="M33 per-DML cut"):
                open_real()
            assert len(statements) == cut
            assert tuple(statements) == successful_trace[:cut]
            connection.cursor_factory = original_factory
            assert table_image() == before
    finally:
        connection.cursor_factory = original_factory
    database.publish(event)
