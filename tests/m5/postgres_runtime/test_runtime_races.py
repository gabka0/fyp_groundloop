"""Live concurrent acceptance tests for M5 activation and route races."""

from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Event
from typing import Any, cast

import psycopg
import pytest
from psycopg import Connection, sql
from psycopg import errors as pg_errors

from groundloop.errors import EventConflictError, InvalidEventError
from groundloop.m4.application import OpenEventReceipt
from groundloop.m4.contracts import (
    CandidatePolicyManifest as M4CandidatePolicyManifest,
)
from groundloop.m4.contracts import (
    CorpusUpdateIdentity,
    UpdateKind,
    VectorIndexKind,
)
from groundloop.m4.persistence import PostgresM4RuntimeStore
from groundloop.m5.domain import EvidenceGroupVersion
from groundloop.m5.events import RetireGroupEvent, m5_event_payload_digest
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5ActivationReceipt,
    M5ActivationRequest,
    M5CandidatePolicyManifest,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from groundloop.postgres.migrations import (
    apply_legacy_migrations,
    install_m5_core_bundle,
    install_m5_runtime_bundle,
)
from tests.m5.postgres.helpers import (
    SeededBase,
    insert_published_group,
    make_group,
    seed_base,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _database_url() -> str:
    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip(
            "live PostgreSQL test: set GROUNDLOOP_TEST_DATABASE_URL or "
            "GROUNDLOOP_DATABASE_URL"
        )
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


def _select_schema(connection: Connection[Any], schema_name: str) -> None:
    connection.execute(
        sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
    )
    connection.commit()


def _candidate_manifest(base: SeededBase) -> M5CandidatePolicyManifest:
    return M5CandidatePolicyManifest.build(
        embedding_model_artifact_id="runtime-race-embedding",
        requirement_role_template_hash=_sha("runtime-race:requirement-role"),
        chunk_role_template_hash=_sha("runtime-race:chunk-role"),
        vector_method_version="runtime-race-vector-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=_sha("runtime-race:vector-build"),
        vector_search_config_hash=_sha("runtime-race:vector-search"),
        lexical_method_version="runtime-race-lexical-v1",
        lexical_config_hash=_sha("runtime-race:lexical-config"),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        fusion_version="rank-interleave-v1",
        reverse_budget_per_inserted_chunk=2,
        forward_budget_per_requirement=2,
        verifier_execution_spec_hash=_sha("runtime-race:verifier-execution"),
        decision_policy_version=base.policy_version,
        lineage_safety_override=True,
    )


def _legacy_candidate_manifest(base: SeededBase) -> M4CandidatePolicyManifest:
    return M4CandidatePolicyManifest.build(
        policy_id="runtime-race-v1-candidate",
        embedding_model_artifact_id="runtime-race-embedding",
        claim_role_template_hash=_sha("runtime-race-v1:claim-role"),
        chunk_role_template_hash=_sha("runtime-race-v1:chunk-role"),
        vector_method_version="runtime-race-v1-vector",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=_sha("runtime-race-v1:vector-build"),
        vector_search_config_hash=_sha("runtime-race-v1:vector-search"),
        lexical_method_version="runtime-race-v1-lexical",
        lexical_config_hash=_sha("runtime-race-v1:lexical-config"),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        claim_registry_snapshot_id="runtime-race-v1-registry",
        claim_count=len(base.claim_ids),
        fusion_version="interleave-v1",
        approximate_cap_per_inserted_chunk=2,
        frontier_depth=2,
        verifier_execution_spec_hash=_sha("runtime-race-v1:verifier"),
        decision_policy_version=base.policy_version,
        lineage_safety_override=True,
    )


@dataclass(frozen=True, slots=True)
class RuntimeRaceDatabase:
    dsn: str
    schema_name: str
    connection: Connection[Any]
    base: SeededBase
    group: EvidenceGroupVersion
    legacy_candidate_policy_id: str
    activation_request: M5ActivationRequest
    retire_plan: M5TypedEventPlan

    @contextmanager
    def reconnect(self) -> Iterator[Connection[Any]]:
        with psycopg.connect(self.dsn) as connection:
            _select_schema(connection, self.schema_name)
            yield connection


@pytest.fixture
def runtime_race_db() -> Iterator[RuntimeRaceDatabase]:
    """Build one sealed M4 head with installed, inactive M5 runtime bundles."""

    dsn = _database_url()
    schema_name = f"groundloop_m5_runtime_race_{uuid.uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))

    try:
        with psycopg.connect(dsn) as connection:
            _select_schema(connection, schema_name)
            with connection.transaction():
                apply_legacy_migrations(connection)
            install_m5_core_bundle(connection)
            install_m5_runtime_bundle(connection)

            chunk_texts = ("alpha", "beta")
            with connection.transaction():
                base = seed_base(
                    connection,
                    prefix="runtime-race",
                    claim_count=1,
                    chunk_texts=chunk_texts,
                )
                group = make_group(
                    group_id="runtime-race-group-v1",
                    family_id="runtime-race-family",
                    claim_id=base.claim_ids[0],
                    texts=("required runtime-race fact",),
                    source_id="runtime-race-fixture",
                )
                insert_published_group(
                    connection,
                    group=group,
                    epoch_id=base.epoch_id,
                )
                connection.execute(
                    """
                    INSERT INTO groundloop_model_artifact (
                        model_artifact_id, task, provider, model_id,
                        immutable_revision, tokenizer_revision, license_id,
                        config_hash
                    ) VALUES (
                        'runtime-race-embedding', 'embedding', 'fixture',
                        'runtime-race-embedding', 'v1', 'v1', 'MIT', %s
                    )
                    """,
                    (_sha("runtime-race:embedding-config"),),
                )

            m4_manifest = _legacy_candidate_manifest(base)
            m4_store = PostgresM4RuntimeStore(connection)
            m4_store.register_claim_registry_snapshot(
                m4_manifest.claim_registry_snapshot_id,
                base.claim_ids,
            )
            m4_store.register_candidate_policy(m4_manifest)
            manifest = _candidate_manifest(base)
            runtime_store = PostgresM5RuntimeStore(connection)
            runtime_store.register_candidate_policy(manifest)
            activation_request = runtime_store.prepare_activation_request(
                "runtime-race-activation"
            )
            retire_event = RetireGroupEvent(
                event_id="runtime-race-typed-retire",
                group_version_id=group.group_version_id,
            )
            retire_plan = M5TypedEventPlan(
                structural_event_id=retire_event.event_id,
                event=retire_event,
                payload_hash=m5_event_payload_digest(retire_event),
                direct_plan=None,
                candidate_policy_id=manifest.candidate_policy_id,
                candidate_policy_manifest_hash=manifest.manifest_hash,
                requirement_registry_snapshot=RequirementRegistrySnapshot.build(()),
                active_chunk_snapshot=ActiveChunkSnapshot.build(
                    tuple(
                        ActiveChunkSnapshotEntry.build(
                            chunk_version_id=chunk_id,
                            chunk_text=chunk_text,
                        )
                        for chunk_id, chunk_text in zip(
                            base.chunk_ids, chunk_texts, strict=True
                        )
                    )
                ),
                expected_previous_published_epoch_id=base.epoch_id,
            )
            yield RuntimeRaceDatabase(
                dsn=dsn,
                schema_name=schema_name,
                connection=connection,
                base=base,
                group=group,
                legacy_candidate_policy_id=m4_manifest.policy_id,
                activation_request=activation_request,
                retire_plan=retire_plan,
            )
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )


def _table_names(connection: Connection[Any]) -> tuple[str, ...]:
    return tuple(
        str(row[0])
        for row in connection.execute(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = current_schema()
            ORDER BY tablename COLLATE "C"
            """
        ).fetchall()
    )


def _snapshot_tables(
    connection: Connection[Any], table_names: tuple[str, ...]
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    """Capture logical rows and xmin so same-value rewrites remain visible."""

    connection.commit()
    snapshot: list[tuple[str, tuple[tuple[str, str], ...]]] = []
    for table_name in table_names:
        statement = sql.SQL(
            "SELECT xmin::text, to_jsonb(snapshot_row)::text "
            "FROM {} AS snapshot_row "
            'ORDER BY to_jsonb(snapshot_row)::text COLLATE "C", xmin::text'
        ).format(sql.Identifier(table_name))
        rows = tuple(
            (str(row[0]), str(row[1]))
            for row in connection.execute(statement).fetchall()
        )
        snapshot.append((table_name, rows))
    connection.commit()
    return tuple(snapshot)


def _database_snapshot(
    connection: Connection[Any],
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    return _snapshot_tables(connection, _table_names(connection))


ACTIVATION_SURFACE_TABLES = (
    "groundloop_runtime_mode",
    "groundloop_m4_publication_head",
    "groundloop_m5_publication_head",
    "groundloop_m5_activation",
    "groundloop_m5_requirement_state_materialized",
    "groundloop_m5_group_state_materialized",
    "groundloop_m5_claim_state_materialized",
    "groundloop_m5_answer_state_materialized",
    "groundloop_m5_published_requirement_state",
    "groundloop_m5_published_group_state",
    "groundloop_m5_published_claim_state",
    "groundloop_m5_published_answer_state",
    "groundloop_m5_group_certificate_artifact",
    "groundloop_m5_group_certificate_artifact_row",
    "groundloop_m5_claim_certificate_artifact",
    "groundloop_m5_published_group_certificate_binding",
    "groundloop_m5_published_claim_certificate_binding",
)

RouteHeadSnapshot = tuple[
    tuple[str, int, str] | None,
    tuple[int, str] | None,
    tuple[int, int, str] | None,
    tuple[str, str, int, str] | None,
]


def _activation_surface_snapshot(
    connection: Connection[Any],
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    return _snapshot_tables(connection, ACTIVATION_SURFACE_TABLES)


def _route_head_snapshot(connection: Connection[Any]) -> RouteHeadSnapshot:
    connection.commit()
    mode = connection.execute(
        "SELECT mode, mode_revision, xmin::text FROM groundloop_runtime_mode"
    ).fetchone()
    m4_head = connection.execute(
        "SELECT epoch_id, xmin::text FROM groundloop_m4_publication_head"
    ).fetchone()
    m5_head = connection.execute(
        "SELECT epoch_id, sealed_revision, xmin::text "
        "FROM groundloop_m5_publication_head"
    ).fetchone()
    activation = connection.execute(
        "SELECT activation_id, payload_hash, base_m4_epoch_id, xmin::text "
        "FROM groundloop_m5_activation"
    ).fetchone()
    connection.commit()
    return (
        cast(tuple[str, int, str] | None, mode),
        cast(tuple[int, str] | None, m4_head),
        cast(tuple[int, int, str] | None, m5_head),
        cast(tuple[str, str, int, str] | None, activation),
    )


def _assert_activated_route(
    connection: Connection[Any], database: RuntimeRaceDatabase
) -> None:
    mode, m4_head, m5_head, activation = _route_head_snapshot(connection)
    assert mode is not None and tuple(mode[:2]) == ("m5_active", 1)
    assert m4_head is not None and int(m4_head[0]) == database.base.epoch_id
    assert m5_head is not None and int(m5_head[0]) == database.base.epoch_id
    assert activation is not None
    assert tuple(activation[:3]) == (
        database.activation_request.activation_id,
        database.activation_request.payload_hash,
        database.base.epoch_id,
    )


def _legacy_update(
    database: RuntimeRaceDatabase, event_id: str
) -> CorpusUpdateIdentity:
    return CorpusUpdateIdentity(
        event_id=event_id,
        payload_hash=_sha(f"payload:{event_id}"),
        update_kind=UpdateKind.INSERT,
        previous_published_epoch_id=database.base.epoch_id,
        candidate_policy_id=database.legacy_candidate_policy_id,
    )


def _open_v1_event(
    database: RuntimeRaceDatabase,
    *,
    event_id: str,
    hold_after_write: Event | None = None,
    release: Event | None = None,
) -> int:
    with database.reconnect() as connection:
        store = PostgresM4RuntimeStore(connection, audit_transitions=False)

        def inject(point: str) -> None:
            if point == "open_structural_written" and hold_after_write is not None:
                hold_after_write.set()
                if release is None or not release.wait(timeout=10):
                    raise TimeoutError("test did not release the v1 opener")

        result = store.open_epoch(
            _legacy_update(database, event_id),
            (),
            registry_snapshot_id="runtime-race-v1-registry",
            structural_action=lambda _cursor, _epoch_id: None,
            failure_injector=inject,
        )
        return result.epoch.epoch_id


def test_concurrent_exact_activation_commits_once_and_replays_read_only(
    runtime_race_db: RuntimeRaceDatabase,
) -> None:
    first_locked = Event()
    release_first = Event()
    second_started = Event()
    second_finished = Event()

    def first_activation() -> M5ActivationReceipt:
        def hold(point: str) -> None:
            if point == "activation_before_bootstrap":
                first_locked.set()
                if not release_first.wait(timeout=10):
                    raise TimeoutError("test did not release first activation")

        with runtime_race_db.reconnect() as connection:
            return PostgresM5RuntimeStore(connection).activate(
                runtime_race_db.activation_request,
                failure_injector=hold,
            )

    def exact_replay() -> M5ActivationReceipt:
        assert first_locked.wait(timeout=10)
        second_started.set()
        try:
            with runtime_race_db.reconnect() as connection:
                return PostgresM5RuntimeStore(connection).activate(
                    runtime_race_db.activation_request
                )
        finally:
            second_finished.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(first_activation)
        assert first_locked.wait(timeout=10)
        second_future = executor.submit(exact_replay)
        assert second_started.wait(timeout=10)
        try:
            assert not second_finished.wait(timeout=0.25)
        finally:
            release_first.set()
        receipts = (
            first_future.result(timeout=20),
            second_future.result(timeout=20),
        )

    assert {receipt.replayed for receipt in receipts} == {False, True}
    assert receipts[0].receipt_hash == receipts[1].receipt_hash
    _assert_activated_route(runtime_race_db.connection, runtime_race_db)
    assert runtime_race_db.connection.execute(
        "SELECT count(*) FROM groundloop_m5_activation"
    ).fetchone() == (1,)
    assert runtime_race_db.connection.execute(
        "SELECT count(*) FROM groundloop_epoch"
    ).fetchone() == (1,)
    before_replay = _database_snapshot(runtime_race_db.connection)
    replay = PostgresM5RuntimeStore(runtime_race_db.connection).activate(
        runtime_race_db.activation_request
    )
    assert replay.replayed
    assert replay.receipt_hash == receipts[0].receipt_hash
    assert _database_snapshot(runtime_race_db.connection) == before_replay


def test_concurrent_conflicting_activation_loser_rolls_back_without_consumption(
    runtime_race_db: RuntimeRaceDatabase,
) -> None:
    winner_locked = Event()
    release_winner = Event()
    loser_started = Event()
    loser_finished = Event()
    request = runtime_race_db.activation_request
    conflict = M5ActivationRequest.build(
        activation_id=request.activation_id,
        expected_mode_revision=request.expected_mode_revision,
        expected_base_m4_epoch_id=request.expected_base_m4_epoch_id,
        core_schema_bundle_sha256=request.core_schema_bundle_sha256,
        bootstrap_state_hash=_sha("runtime-race:conflicting-bootstrap"),
    )

    def winning_activation() -> M5ActivationReceipt:
        def hold(point: str) -> None:
            if point == "activation_before_bootstrap":
                winner_locked.set()
                if not release_winner.wait(timeout=10):
                    raise TimeoutError("test did not release winning activation")

        with runtime_race_db.reconnect() as connection:
            return PostgresM5RuntimeStore(connection).activate(
                request,
                failure_injector=hold,
            )

    def conflicting_activation() -> M5ActivationReceipt:
        assert winner_locked.wait(timeout=10)
        loser_started.set()
        try:
            with runtime_race_db.reconnect() as connection:
                return PostgresM5RuntimeStore(connection).activate(conflict)
        finally:
            loser_finished.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        winner_future = executor.submit(winning_activation)
        assert winner_locked.wait(timeout=10)
        loser_future = executor.submit(conflicting_activation)
        assert loser_started.wait(timeout=10)
        try:
            assert not loser_finished.wait(timeout=0.25)
        finally:
            release_winner.set()
        winner = winner_future.result(timeout=20)
        with pytest.raises(
            EventConflictError,
            match="already activated by another request",
        ):
            loser_future.result(timeout=20)

    assert not winner.replayed
    _assert_activated_route(runtime_race_db.connection, runtime_race_db)
    assert runtime_race_db.connection.execute(
        "SELECT count(*) FROM groundloop_m5_activation"
    ).fetchone() == (1,)
    assert runtime_race_db.connection.execute(
        "SELECT count(*) FROM groundloop_epoch"
    ).fetchone() == (1,)
    after_winner = _database_snapshot(runtime_race_db.connection)
    with runtime_race_db.reconnect() as reconnect:
        with pytest.raises(
            EventConflictError,
            match="already activated by another request",
        ):
            PostgresM5RuntimeStore(reconnect).activate(conflict)
        assert _database_snapshot(reconnect) == after_winner


def test_v1_open_wins_then_public_activation_rejects_without_partial_state(
    runtime_race_db: RuntimeRaceDatabase,
) -> None:
    v1_holds_route = Event()
    release_v1 = Event()
    activation_started = Event()
    activation_finished = Event()
    event_id = "runtime-race-v1-wins"
    before_activation = _activation_surface_snapshot(runtime_race_db.connection)

    def v1_open() -> int:
        return _open_v1_event(
            runtime_race_db,
            event_id=event_id,
            hold_after_write=v1_holds_route,
            release=release_v1,
        )

    def activation_attempt() -> M5ActivationReceipt:
        assert v1_holds_route.wait(timeout=10)
        activation_started.set()
        try:
            with runtime_race_db.reconnect() as connection:
                return PostgresM5RuntimeStore(connection).activate(
                    runtime_race_db.activation_request
                )
        finally:
            activation_finished.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        v1_future = executor.submit(v1_open)
        assert v1_holds_route.wait(timeout=10)
        activation_future = executor.submit(activation_attempt)
        assert activation_started.wait(timeout=10)
        try:
            assert not activation_finished.wait(timeout=0.25)
        finally:
            release_v1.set()
        epoch_id = v1_future.result(timeout=20)
        with pytest.raises(EventConflictError, match="rejects a live mutation epoch"):
            activation_future.result(timeout=20)

    assert _activation_surface_snapshot(runtime_race_db.connection) == before_activation
    assert runtime_race_db.connection.execute(
        "SELECT epoch_id, semantic_status FROM groundloop_epoch WHERE event_id = %s",
        (event_id,),
    ).fetchone() == (epoch_id, "complete")
    assert runtime_race_db.connection.execute(
        "SELECT count(*) FROM groundloop_epoch WHERE event_id = %s",
        (runtime_race_db.activation_request.activation_id,),
    ).fetchone() == (0,)


def test_activation_wins_then_public_v1_open_rolls_back_before_event_consumption(
    runtime_race_db: RuntimeRaceDatabase,
) -> None:
    activation_locked = Event()
    release_activation = Event()
    v1_started = Event()
    v1_finished = Event()
    event_id = "runtime-race-activation-wins-v1-loser"

    def activation() -> M5ActivationReceipt:
        def hold(point: str) -> None:
            if point == "activation_before_bootstrap":
                activation_locked.set()
                if not release_activation.wait(timeout=10):
                    raise TimeoutError("test did not release activation")

        with runtime_race_db.reconnect() as connection:
            return PostgresM5RuntimeStore(connection).activate(
                runtime_race_db.activation_request,
                failure_injector=hold,
            )

    def losing_v1_open() -> int:
        assert activation_locked.wait(timeout=10)
        v1_started.set()
        try:
            return _open_v1_event(runtime_race_db, event_id=event_id)
        finally:
            v1_finished.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        activation_future = executor.submit(activation)
        assert activation_locked.wait(timeout=10)
        v1_future = executor.submit(losing_v1_open)
        assert v1_started.wait(timeout=10)
        try:
            assert not v1_finished.wait(timeout=0.25)
        finally:
            release_activation.set()
        receipt = activation_future.result(timeout=20)
        with pytest.raises(
            pg_errors.RaiseException, match="disabled after M5 activation"
        ):
            v1_future.result(timeout=20)

    assert not receipt.replayed
    _assert_activated_route(runtime_race_db.connection, runtime_race_db)
    assert runtime_race_db.connection.execute(
        "SELECT count(*) FROM groundloop_epoch WHERE event_id = %s",
        (event_id,),
    ).fetchone() == (0,)
    assert runtime_race_db.connection.execute(
        "SELECT count(*) FROM groundloop_epoch"
    ).fetchone() == (1,)
    stable = _database_snapshot(runtime_race_db.connection)
    replay = PostgresM5RuntimeStore(runtime_race_db.connection).activate(
        runtime_race_db.activation_request
    )
    assert replay.replayed
    assert _database_snapshot(runtime_race_db.connection) == stable


def test_typed_open_arrives_first_and_rejects_before_activation_consumes_no_event(
    runtime_race_db: RuntimeRaceDatabase,
) -> None:
    typed_rejected = Event()
    activation_finished = Event()

    def typed_before_activation() -> str:
        try:
            with runtime_race_db.reconnect() as connection:
                PostgresM5RuntimeStore(connection).open_typed_event_atomically(
                    runtime_race_db.retire_plan
                )
        except InvalidEventError as error:
            typed_rejected.set()
            if not activation_finished.wait(timeout=10):
                raise TimeoutError(
                    "activation did not finish after typed rejection"
                ) from error
            return str(error)
        raise AssertionError("typed event unexpectedly crossed inactive routing")

    def activation_after_rejection() -> M5ActivationReceipt:
        assert typed_rejected.wait(timeout=10)
        try:
            with runtime_race_db.reconnect() as connection:
                return PostgresM5RuntimeStore(connection).activate(
                    runtime_race_db.activation_request
                )
        finally:
            activation_finished.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        typed_future = executor.submit(typed_before_activation)
        assert typed_rejected.wait(timeout=10)
        activation_future = executor.submit(activation_after_rejection)
        receipt = activation_future.result(timeout=20)
        message = typed_future.result(timeout=20)

    assert "requires activated mode" in message
    assert not receipt.replayed
    _assert_activated_route(runtime_race_db.connection, runtime_race_db)
    assert runtime_race_db.connection.execute(
        "SELECT count(*) FROM groundloop_epoch WHERE event_id = %s",
        (runtime_race_db.retire_plan.structural_event_id,),
    ).fetchone() == (0,)
    assert runtime_race_db.connection.execute(
        "SELECT count(*) FROM groundloop_epoch"
    ).fetchone() == (1,)


def test_activation_arrives_first_then_typed_open_uses_head_equal_route(
    runtime_race_db: RuntimeRaceDatabase,
) -> None:
    activation_locked = Event()
    release_activation = Event()
    typed_started = Event()
    typed_inserted = Event()
    release_typed = Event()

    def activation() -> M5ActivationReceipt:
        def hold(point: str) -> None:
            if point == "activation_before_bootstrap":
                activation_locked.set()
                if not release_activation.wait(timeout=10):
                    raise TimeoutError("test did not release activation")

        with runtime_race_db.reconnect() as connection:
            return PostgresM5RuntimeStore(connection).activate(
                runtime_race_db.activation_request,
                failure_injector=hold,
            )

    def typed_after_activation() -> OpenEventReceipt:
        assert activation_locked.wait(timeout=10)
        typed_started.set()

        def hold(point: str) -> None:
            if point == "typed_open_epoch_inserted":
                typed_inserted.set()
                if not release_typed.wait(timeout=10):
                    raise TimeoutError("test did not release typed opener")

        with runtime_race_db.reconnect() as connection:
            return PostgresM5RuntimeStore(connection).open_typed_event_atomically(
                runtime_race_db.retire_plan,
                failure_injector=hold,
            )

    with ThreadPoolExecutor(max_workers=2) as executor:
        activation_future = executor.submit(activation)
        assert activation_locked.wait(timeout=10)
        typed_future = executor.submit(typed_after_activation)
        assert typed_started.wait(timeout=10)
        try:
            assert not typed_inserted.wait(timeout=0.25)
        finally:
            release_activation.set()
        activation_receipt = activation_future.result(timeout=20)
        assert typed_inserted.wait(timeout=10)
        activated_heads = _route_head_snapshot(runtime_race_db.connection)
        release_typed.set()
        typed_receipt = typed_future.result(timeout=20)

    assert not activation_receipt.replayed
    assert not typed_receipt.replayed
    assert typed_receipt.epoch_id != runtime_race_db.base.epoch_id
    assert _route_head_snapshot(runtime_race_db.connection) == activated_heads
    _assert_activated_route(runtime_race_db.connection, runtime_race_db)
    assert runtime_race_db.connection.execute(
        "SELECT epoch_id FROM groundloop_epoch WHERE event_id = %s",
        (runtime_race_db.retire_plan.structural_event_id,),
    ).fetchone() == (typed_receipt.epoch_id,)
    assert runtime_race_db.connection.execute(
        "SELECT count(*) FROM groundloop_m5_update WHERE epoch_id = %s",
        (typed_receipt.epoch_id,),
    ).fetchone() == (1,)
    assert runtime_race_db.connection.execute(
        "SELECT count(*) FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s",
        (typed_receipt.epoch_id,),
    ).fetchone() == (1,)
