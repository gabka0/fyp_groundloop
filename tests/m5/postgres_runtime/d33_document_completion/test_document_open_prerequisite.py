"""O33 actual private activation/OPEN, not public readiness/seal acceptance."""

from __future__ import annotations

import hashlib
import inspect
import os
import re
import time
import uuid
from collections import Counter
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from copy import copy, deepcopy
from dataclasses import asdict, dataclass, replace
from enum import Enum
from pathlib import Path
from threading import Event
from typing import Any

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.ai.persistence import PostgresArtifactStore
from groundloop.domain import (
    AnswerVersion,
    ChunkVersion,
    Claim,
    DecisionPolicy,
    DocumentVersion,
    ModelStamp,
    Question,
    SemanticObservation,
    StatusDelta,
    SubjectKind,
)
from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.events import (
    ChunkInput,
    DeleteDocumentVersionEvent,
    InsertDocumentEvent,
    ReplaceDocumentVersionEvent,
)
from groundloop.m4 import evaluation_overlay
from groundloop.m4.application import (
    ApplicationExecutionPolicy,
    DynamicEventPlan,
    EventRunState,
    M4Application,
)
from groundloop.m4.contracts import CorpusUpdateIdentity, UpdateKind
from groundloop.m4.persistence import PostgresM4RuntimeStore
from groundloop.m4.pipeline import (
    M4ExecutionMode,
    PersistingAdmissionPort,
    PostgresM4ApplicationPorts,
    StructuralPayload,
    bootstrap_m4_publication,
)
from groundloop.m5 import digests as base_digests
from groundloop.m5 import domain as m5_domain
from groundloop.m5.claim_certificates import WorkingClaimCertificateBinding
from groundloop.m5.events import legacy_event_payload_digest
from groundloop.m5.matching import WorkingGroupCertificateBinding
from groundloop.m5.reference import (
    build_reference_claim_certificate,
    build_reference_group_certificate,
    compute_reference_states,
    derive_active_requirement_witnesses,
)
from groundloop.m5.repository import M5Repository
from groundloop.m5.runtime import contracts as runtime_contracts
from groundloop.m5.runtime import digests
from groundloop.m5.runtime import persistence as persistence_module
from groundloop.m5.runtime import postgres_matching as matching_store
from groundloop.m5.runtime import postgres_withdrawal as withdrawal
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5ActivationRequest,
    M5CancellationPlan,
    M5RuntimeOperationalConfig,
    M5RuntimeWork,
    M5TerminalReason,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
    RequirementRegistrySnapshotEntry,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from groundloop.m5.runtime.postgres_recovery import validate_structural_open_recovery
from groundloop.postgres import migrations
from groundloop.postgres.m5 import persist_claim_certificate, persist_group_certificate
from groundloop.repository import InMemoryRepository
from tests.m4.integration.test_postgres_pipeline import (
    ScriptedAdmission,
    ScriptedVerifier,
)
from tests.m5.postgres.helpers import (
    SeededBase,
    insert_observation,
    insert_published_group,
    install_current_currency,
    make_group,
    seed_base,
    sha,
)
from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot
from tests.m5.postgres_runtime.test_direct_m4_composition import (
    REGISTRY_ID,
    _m4_manifest,
    _m5_manifest,
)
from tests.postgres.test_m3_persistence import _bundle


@dataclass
class OpenDatabase:
    connection: Connection[Any]
    dsn: str
    schema: str
    base: SeededBase
    group: Any
    policy: Any
    ports: PostgresM4ApplicationPorts
    store: PostgresM5RuntimeStore
    request: Any
    remaining_chunks: tuple[ActiveChunkSnapshotEntry, ...] = ()
    retained_fixture_certificates: tuple[str, ...] = ()

    @property
    def execution(self) -> ApplicationExecutionPolicy:
        return ApplicationExecutionPolicy(
            sha("o33-impact"),
            sha("o33-frontier"),
            self.policy.verifier_execution_spec_hash,
        )

    def plan(self, label: str = "delete") -> M5TypedEventPlan:
        event = DeleteDocumentVersionEvent(f"o33-{label}", "o33-document-v1")
        payload = legacy_event_payload_digest(event)
        entries = (
            ()
            if self.group is None
            else tuple(
                RequirementRegistrySnapshotEntry.build(
                    requirement_version_id=r.requirement_version_id,
                    group_version_id=self.group.group_version_id,
                    group_family_id=self.group.group_family_id,
                    owner_claim_id=self.group.owner_claim_id,
                    requirement_text=r.requirement_text,
                )
                for r in self.group.requirements
            )
        )
        direct = DynamicEventPlan(
            CorpusUpdateIdentity(
                event.event_id,
                payload,
                UpdateKind.DELETE,
                self.base.epoch_id,
                self.policy.candidate_policy_id,
            ),
            (),
            self.base.chunk_ids,
            (),
            REGISTRY_ID,
        )
        return M5TypedEventPlan(
            event.event_id,
            event,
            payload,
            direct,
            self.policy.candidate_policy_id,
            self.policy.manifest_hash,
            RequirementRegistrySnapshot.build(entries),
            ActiveChunkSnapshot.build(self.remaining_chunks),
            self.base.epoch_id,
        )

    def proposals(self, plan: M5TypedEventPlan) -> tuple[Any, ...]:
        with self.connection.transaction(), self.connection.cursor() as cursor:
            direct = withdrawal._preview_document_direct_open(
                cursor,
                plan,
                execution_policy=self.execution,
            )
            required = withdrawal._plan_document_requirement_withdrawal(cursor, plan)
        roots = withdrawal._document_requirement_roots(plan, self.policy, required)
        root_hash = digests.requirement_root_set_digest(
            r.job.logical_job_id for r in roots
        )
        return direct, required, roots, root_hash


class _ControlledCut(RuntimeError):
    pass


def _table_image(connection: Connection[Any]) -> Any:
    # PostgreSQL sequences are nontransactional. Only rollback comparisons
    # exclude them; exact read replay uses database_snapshot including them.
    return tuple(
        row
        for row in database_snapshot(connection)
        if not row[0].startswith("sequence:")
    )


def _cache_image(ports: PostgresM4ApplicationPorts) -> Any:
    engine = ports._published_engine
    return (
        ports._published_repository.export_snapshot(),
        engine.claim_states,
        engine.answer_states,
        engine.certificates,
        engine._state_revision,
        engine.last_stats,
    )


def _mutation_cursor(connection: Connection[Any], control: dict[str, Any]) -> Any:
    class MutationCursor(psycopg.Cursor):
        def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
            result = super().execute(query, params, **kwargs)
            source = (
                query.as_string(self.connection)
                if isinstance(query, sql.Composable)
                else str(query)
            )
            source = source.lstrip()
            if source.upper().startswith(
                ("INSERT ", "UPDATE ", "DELETE ", "SET CONSTRAINTS ")
            ) or re.search(
                r"\bSELECT\s+(?:pg_catalog\.)?(?:groundloop_m5_authorize_\w+|set_config|pg_advisory_xact_lock)\s*\(",
                source,
                re.I,
            ):
                control["records"].append((source, self.rowcount))
                if "timeline" in control:
                    control["timeline"].append(("sql", source))
                control["count"] += 1
                if control["target"] == control["count"]:
                    raise _ControlledCut(f"actual mutation {control['target']}")
            return result

    return MutationCursor


@pytest.mark.parametrize("operation", ["activation", "open"])
@pytest.mark.parametrize(
    "open_db",
    [
        False,
        True,
        "complete-changed",
        "complete-unchanged",
        "complete-reused",
        "partial",
    ],
    indirect=True,
)
def test_failure_after_every_actual_mutation_rolls_back_and_retries(
    open_db: OpenDatabase, operation: str, request: pytest.FixtureRequest
) -> None:
    db = open_db
    if operation == "open":
        db.store._activate_document_open_prerequisite(db.request)
        plan = db.plan()
        arguments = _open_arguments(db, plan)
    before = _table_image(db.connection)
    cache_before = _cache_image(db.ports)
    original_repository = db.ports._published_repository
    original_engine = db.ports._published_engine
    control: dict[str, Any] = {"records": [], "count": 0, "target": None}
    original_factory = db.connection.cursor_factory
    db.connection.cursor_factory = _mutation_cursor(db.connection, control)

    def invoke(*, discover: bool = False) -> Any:
        def final_cut(stage: str) -> None:
            expected = (
                "o33_activation_before_commit"
                if operation == "activation"
                else "o33_before_commit"
            )
            if discover and stage == expected:
                raise _ControlledCut("complete real trace before commit")

        if operation == "activation":
            return db.store._activate_document_open_prerequisite(
                db.request, failure_injector=final_cut
            )
        return db.store._open_document_open_prerequisite(
            plan, **arguments, failure_injector=final_cut
        )

    try:
        with pytest.raises(_ControlledCut, match="complete real trace"):
            invoke(discover=True)
        discovered = tuple(control["records"])
        request.node.user_properties.append(("actual_mutation_cuts", len(discovered)))
        assert discovered and discovered[-1][0].startswith("SET CONSTRAINTS")
        assert _table_image(db.connection) == before
        for position in range(1, len(discovered) + 1):
            control.update(records=[], count=0, target=position)
            with pytest.raises(_ControlledCut, match=f"actual mutation {position}$"):
                invoke()
            assert control["count"] == position
            assert _table_image(db.connection) == before
            assert _cache_image(db.ports) == cache_before
            assert db.ports._working_repository is original_repository
            assert db.ports._working_engine is original_engine
            assert db.ports._active_epoch_id is None
        control.update(records=[], count=0, target=None)
        receipt = invoke()
        assert not receipt.replayed
        assert len(control["records"]) == len(discovered)
        if operation == "open":
            assert db.ports._active_epoch_id == receipt.epoch_id
            assert db.ports._working_repository is not original_repository
            assert db.ports._working_engine is not original_engine
    finally:
        db.connection.cursor_factory = original_factory


def _stage_row_image(connection: Connection[Any]) -> dict[tuple[str, bytes], Any]:
    """Independent catalog-PK projection, not a prepared-plan/journal oracle."""
    result = {}
    relations = (
        "matching_image_working",
        "matching_observation_working",
        "matching_edge_working",
        "matching_hash_mask_working",
        "matching_hall_working",
        "working_requirement_state",
        "working_group_state",
        "working_claim_state",
        "working_answer_state",
        "group_certificate_artifact",
        "group_certificate_artifact_row",
        "claim_certificate_artifact",
        "working_group_certificate_binding",
        "working_claim_certificate_binding",
        "matching_patch_artifact",
        "matching_work_contribution",
        "matching_work_accumulator",
    )
    for suffix in relations:
        relation = "groundloop_m5_" + suffix
        columns = connection.execute(
            "SELECT a.attname FROM pg_index i CROSS JOIN LATERAL "
            "unnest(i.indkey) WITH ORDINALITY k(attnum,position) JOIN pg_attribute a "
            "ON a.attrelid=i.indrelid AND a.attnum=k.attnum "
            "WHERE i.indrelid=%s::regclass AND i.indisprimary ORDER BY k.position",
            (relation,),
        ).fetchall()
        assert columns
        for (row,) in connection.execute(
            sql.SQL("SELECT to_jsonb(t) FROM {} t").format(sql.Identifier(relation))
        ).fetchall():
            # Migration017's contribution journal includes its revision in
            # addition to the SQL primary key; all other families use the PK.
            names = [c[0] for c in columns]
            if suffix == "matching_work_contribution":
                names.append("resulting_revision")
            parts = (relation, *(str(row[name]) for name in names))
            key = b"".join(
                len(part.encode()).to_bytes(8, "big", signed=True) + part.encode()
                for part in parts
            )
            result[relation, key] = row
    return result


@pytest.mark.parametrize(
    "open_db",
    [
        False,
        True,
        "complete-changed",
        "complete-unchanged",
        "complete-reused",
        "partial",
    ],
    indirect=True,
)
def test_actual_source_first_order_and_exact_journal(
    open_db: OpenDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = open_db
    db.store._activate_document_open_prerequisite(db.request)
    plan = db.plan()
    arguments = _open_arguments(db, plan)
    before = _stage_row_image(db.connection)
    timeline: list[tuple[str, str]] = []
    ordered = (
        (matching_store, "_authorize_matching_transition"),
        (persistence_module, "_o33_stage_delete_source"),
        (withdrawal, "_prepare_locked_document_open"),
        (persistence_module, "_validate_effective_snapshots"),
        (persistence_module, "_persist_requirement_snapshot"),
        (persistence_module, "_persist_active_chunk_snapshot"),
        (withdrawal, "_continue_locked_document_open"),
        (matching_store, "_derive_matching_transition_intent_with_document_authority"),
        (matching_store, "_prepare_matching_transition_with_document_authority"),
        (persistence_module, "_o33_persist_scopes"),
        (persistence_module, "_o33_persist_jobs"),
        (matching_store, "_stage_prepared_matching_transition_through_tier_12"),
        (matching_store, "_stage_prepared_matching_transition_through_tier_13"),
        (matching_store, "_stage_prepared_matching_transition_through_tier_14"),
        (persistence_module, "_persist_initial_pending_counters"),
        (PostgresM4ApplicationPorts, "_register_execution_accounting"),
        (evaluation_overlay.PostgresEvaluationOverlayStore, "declare_epoch_local"),
        (persistence_module, "persist_structural_open_identity"),
        (persistence_module, "_o33_complete_structural_work"),
        (persistence_module, "persist_structural_open_accounting"),
        (matching_store, "_finalize_prepared_matching_transition"),
        (persistence_module, "_o33_validate_consumed_cache"),
        (PostgresM4ApplicationPorts, "_stage_structural_detached"),
        (persistence_module, "_force_deferred_validation"),
        (PostgresM4ApplicationPorts, "_adopt_direct_working_cache"),
    )
    for module, name in ordered:
        original = getattr(module, name)

        def traced(
            *args: Any, _name: str = name, _original: Any = original, **kwargs: Any
        ) -> Any:
            timeline.append(("call", _name))
            return _original(*args, **kwargs)

        monkeypatch.setattr(module, name, traced)
    control: dict[str, Any] = dict(records=[], count=0, target=None, timeline=timeline)
    original_factory = db.connection.cursor_factory
    db.connection.cursor_factory = _mutation_cursor(db.connection, control)
    journals = []

    def inspect_final(stage: str) -> None:
        if stage != "o33_before_commit":
            return
        after = _stage_row_image(db.connection)
        expected = {
            key: value for key, value in after.items() if before.get(key) != value
        }
        rows = db.connection.execute(
            "SELECT relation_name,key_preimage,first_old,final_new,first_operation,"
            "last_operation,mutation_count,saw_insert,saw_update,saw_delete "
            "FROM pg_temp.groundloop_m5_matching_change_journal"
        ).fetchall()
        journals.extend(rows)
        assert len(rows) == len(expected)
        assert {(r[0], bytes(r[1])) for r in rows} == set(expected)
        for row in rows:
            key = row[0], bytes(row[1])
            operation = "UPDATE" if key in before else "INSERT"
            assert row[2:] == (
                before.get(key),
                expected[key],
                operation,
                operation,
                1,
                operation == "INSERT",
                operation == "UPDATE",
                False,
            )

    try:
        receipt = db.store._open_document_open_prerequisite(
            plan, **arguments, failure_injector=inspect_final
        )
    finally:
        db.connection.cursor_factory = original_factory
    assert journals and not receipt.replayed
    calls = [name for kind, name in timeline if kind == "call"]
    assert calls == [name for _, name in ordered]
    scope_positions = [
        i
        for i, (kind, value) in enumerate(timeline)
        if kind == "sql"
        and value.startswith("INSERT INTO groundloop_m5_discovery_scope ")
    ]
    job_positions = [
        i
        for i, (kind, value) in enumerate(timeline)
        if kind == "sql" and value.startswith("INSERT INTO groundloop_m5_semantic_job ")
    ]
    assert (
        len(scope_positions)
        == len(job_positions)
        == len(arguments["requirement_roots"])
    )
    if scope_positions:
        assert max(scope_positions) < min(job_positions)
        assert timeline.index(
            ("call", "_prepare_matching_transition_with_document_authority")
        ) < min(scope_positions)
        assert max(job_positions) < timeline.index(
            ("call", "_stage_prepared_matching_transition_through_tier_12")
        )
    mutations = [value for kind, value in timeline if kind == "sql"]
    assert mutations[-2:] == [
        "SET CONSTRAINTS ALL IMMEDIATE",
        "SET CONSTRAINTS ALL DEFERRED",
    ]
    assert timeline[-1] == ("call", "_adopt_direct_working_cache")


def test_reconnected_owner_and_distinct_login_read_actual_retained_closure(
    open_db: OpenDatabase,
) -> None:
    db = open_db
    db.store._activate_document_open_prerequisite(db.request)
    plan = db.plan()
    arguments = _open_arguments(db, plan)
    receipt = db.store._open_document_open_prerequisite(plan, **arguments)
    before = database_snapshot(db.connection)
    expected_work = db.store.current_event_work(receipt.epoch_id)
    role = "o33_reader_" + uuid.uuid4().hex
    password = uuid.uuid4().hex
    db.connection.execute(
        sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
            sql.Identifier(role), sql.Literal(password)
        )
    )
    try:
        db.connection.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(db.schema), sql.Identifier(role)
            )
        )
        db.connection.execute(
            sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                sql.Identifier(db.schema), sql.Identifier(role)
            )
        )
        for credentials in ({}, {"user": role, "password": password}):
            with psycopg.connect(db.dsn, autocommit=True, **credentials) as reader:
                reader.execute(
                    sql.SQL("SET search_path TO {}, public").format(
                        sql.Identifier(db.schema)
                    )
                )
                identity = reader.execute("SELECT session_user,current_user").fetchone()
                assert identity[0] == identity[1]
                if credentials:
                    assert identity == (role, role)
                store = PostgresM5RuntimeStore(reader)
                assert store.current_revision(receipt.epoch_id) == 1
                assert store.current_event_work(receipt.epoch_id) == expected_work
                with reader.transaction(), reader.cursor() as cursor:
                    cursor.execute("SET TRANSACTION READ ONLY")
                    direct, _, roots, root_hash = (
                        withdrawal._load_retained_document_open(
                            cursor, plan, expected_epoch_id=receipt.epoch_id
                        )
                    )
                    assert not direct.root_jobs and not direct.discovery_scopes
                    assert tuple(r.job.logical_job_id for r in roots) == tuple(
                        r.job.logical_job_id for r in arguments["requirement_roots"]
                    )
                    assert root_hash == arguments["requirement_root_set_hash"]
                    validate_structural_open_recovery(
                        cursor,
                        epoch_id=receipt.epoch_id,
                        structural_event_id=plan.structural_event_id,
                        payload_hash=plan.payload_hash,
                        config=arguments["recovery_operational_config"],
                        root_fallback_required=arguments[
                            "recovery_root_fallback_required"
                        ],
                        expected_structural_work=expected_work,
                    )
                    candidate = matching_store._candidate_retained_replay_intent(
                        cursor,
                        epoch_id=receipt.epoch_id,
                        expected_runtime_revision=1,
                        resulting_revision=1,
                        source_kind=runtime_contracts.M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
                        source_id=plan.structural_event_id,
                        expected_source_identity_hash=plan.payload_hash,
                    )
                    assert (
                        candidate is not None
                        and candidate.source_identity_hash == plan.payload_hash
                    )
        assert database_snapshot(db.connection) == before
    finally:
        db.connection.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
        db.connection.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


@pytest.mark.parametrize("operation", ["activation", "open"])
@pytest.mark.parametrize("leader_index", [0, 1], ids=["A-first", "B-first"])
@pytest.mark.parametrize("open_db", [True], indirect=True)
def test_both_actual_serializer_orders_and_zero_write_locked_loser(
    open_db: OpenDatabase,
    operation: str,
    leader_index: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = open_db
    if operation == "open":
        db.store._activate_document_open_prerequisite(db.request)
    plan = db.plan()
    with ExitStack() as stack:
        connections = [
            stack.enter_context(psycopg.connect(db.dsn, autocommit=True))
            for _ in range(2)
        ]
        arguments = []
        for connection in connections:
            connection.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(db.schema)
                )
            )
            connection.execute("SET statement_timeout='25s'")
            ports = PostgresM4ApplicationPorts(
                connection,
                structural_payloads={},
                execution_mode=M4ExecutionMode.MEASURED,
            )
            args = _open_arguments(db, plan) if operation == "open" else {}
            if args:
                args["ports"] = ports
            arguments.append(args)
        leader, follower = connections[leader_index], connections[1 - leader_index]
        leader_pid = leader.info.backend_pid
        follower_pid = follower.info.backend_pid
        assert leader_pid != follower_pid != db.connection.info.backend_pid
        held, commit_leader, loser_acquired, finish_loser = (Event() for _ in range(4))
        follower_control: dict[str, Any] = dict(records=[], count=0, target=None)
        mutation_type = _mutation_cursor(follower, follower_control)

        class FollowerCursor(mutation_type):
            def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
                result = super().execute(query, params, **kwargs)
                source = (
                    query.as_string(self.connection)
                    if isinstance(query, sql.Composable)
                    else str(query)
                )
                if "FROM groundloop_runtime_mode" in source and "FOR UPDATE" in source:
                    loser_acquired.set()
                    assert finish_loser.wait(20), (
                        "coordinator did not release locked loser"
                    )
                return result

        follower.cursor_factory = FollowerCursor

        def invoke(index: int) -> Any:
            store = PostgresM5RuntimeStore(connections[index])

            def hold(stage: str) -> None:
                target = (
                    "o33_activation_before_bootstrap"
                    if operation == "activation"
                    else "o33_before_commit"
                )
                if index == leader_index and stage == target:
                    held.set()
                    assert commit_leader.wait(20), "coordinator did not release winner"

            if operation == "activation":
                return store._activate_document_open_prerequisite(
                    db.request, failure_injector=hold
                )
            return store._open_document_open_prerequisite(
                plan, **arguments[index], failure_injector=hold
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(invoke, leader_index)
            try:
                assert held.wait(15), "leader did not acquire native serializer"
                second = pool.submit(invoke, 1 - leader_index)
                deadline = time.monotonic() + 10
                while True:
                    row = db.connection.execute(
                        "SELECT pg_blocking_pids(pid),wait_event_type,query "
                        "FROM pg_stat_activity WHERE pid=%s",
                        (follower_pid,),
                    ).fetchone()
                    locks = db.connection.execute(
                        "SELECT locktype FROM pg_locks WHERE pid=%s AND NOT granted",
                        (follower_pid,),
                    ).fetchall()
                    if row and leader_pid in row[0] and row[1] == "Lock" and locks:
                        assert "groundloop_runtime_mode" in row[2]
                        break
                    assert time.monotonic() < deadline, (
                        "native follower blocking was not observed"
                    )
                    time.sleep(0.01)
                commit_leader.set()
                winner = first.result(timeout=15)
                assert not winner.replayed
                assert loser_acquired.wait(10)
                before = database_snapshot(db.connection)
                if operation == "open":
                    with db.connection.transaction(), db.connection.cursor() as cursor:
                        retained = withdrawal._load_retained_document_open(
                            cursor, plan, expected_epoch_id=winner.epoch_id
                        )
                    original = arguments[1 - leader_index]
                    assert retained[1] != original["supplied_requirement_withdrawal"]
                    assert retained[3] == original["requirement_root_set_hash"]

                    def forbidden(*_a: Any, **_kw: Any) -> Any:
                        pytest.fail("locked exact loser entered a fresh producer")

                    monkeypatch.setattr(
                        withdrawal, "_prepare_locked_document_open", forbidden
                    )
                    monkeypatch.setattr(
                        withdrawal, "_continue_locked_document_open", forbidden
                    )
                    for name in (
                        "_authorize_matching_transition",
                        "_derive_matching_transition_intent_with_document_authority",
                        "_prepare_matching_transition_with_document_authority",
                    ):
                        monkeypatch.setattr(matching_store, name, forbidden)
                    monkeypatch.setattr(
                        arguments[1 - leader_index]["ports"].runtime_store,
                        "stage_open_epoch_local",
                        forbidden,
                    )
                finish_loser.set()
                replay = second.result(timeout=15)
                assert replay.replayed
                identity = "receipt_hash" if operation == "activation" else "epoch_id"
                assert getattr(replay, identity) == getattr(winner, identity)
                if operation == "activation":
                    assert follower_control["records"] == []
                else:
                    # Accepted D25 retained replay preserves its checked/CAS
                    # reader scope and artifact lock. These three transient
                    # SELECTs are rollback cut points, NOT durable row DML.
                    records = follower_control["records"]
                    assert len(records) == 3
                    assert (
                        records[0][0]
                        == "SELECT groundloop_m5_authorize_checked_transition(%s, %s)"
                    )
                    assert records[1][0].startswith("SELECT set_config(")
                    assert re.findall(r"'(groundloop\.\w+)'", records[1][0]) == [
                        "groundloop.m5_matching_reader_epoch_id",
                        "groundloop.m5_matching_reader_revision",
                        "groundloop.m5_matching_reader_backend_pid",
                        "groundloop.m5_matching_reader_transaction_id",
                    ]
                    assert records[2][0] == "SELECT pg_advisory_xact_lock(%s, %s)"
                    ports = arguments[1 - leader_index]["ports"]
                    assert ports._active_epoch_id is None
                    assert ports._working_repository is ports._published_repository
                    assert ports._working_engine is ports._published_engine
                assert database_snapshot(db.connection) == before
            finally:
                commit_leader.set()
                finish_loser.set()


def _m3_direct_foundation(
    connection: Connection[Any], dsn: str, schema: str
) -> OpenDatabase:
    """Real deterministic M3 publication and real strict M4 successor; no AI calls."""
    # Legacy publishers need their exact SQL diagnostics. Closed020/021 forbids
    # adding permanent GroundLoop objects, so only VIEW lifetime changes here.
    # Both accepted SELECT bodies remain byte-identical and are removed before
    # the ordinary M5 catalog/authority checks. No guard/header is authored.
    root = Path(__file__).resolve().parents[4]
    diagnostic_names: list[str] = []
    for name, count in (
        ("m2_full_recompute_oracle.sql", 6),
        ("m4_working_full_recompute_oracle.sql", 3),
    ):
        original = (root / "sql" / name).read_text()
        names = re.findall(
            r"^CREATE OR REPLACE VIEW (groundloop_\w+) AS", original, re.M
        )
        assert len(names) == count
        temporary = original.replace(
            "CREATE OR REPLACE VIEW ", "CREATE OR REPLACE TEMP VIEW "
        )
        assert (
            temporary.replace("CREATE OR REPLACE TEMP VIEW ", "CREATE OR REPLACE VIEW ")
            == original
        )
        connection.execute(temporary)
        diagnostic_names.extend(names)
    bundle = _bundle(run_id="o33-direct-m3")
    texts = {c.chunk_version_id: c.text for d in bundle.documents for c in d.chunks}
    bundle = replace(
        bundle,
        embeddings=tuple(
            replace(e, input_hash=sha(texts[e.chunk_version_id]))
            for e in bundle.embeddings
        ),
    )
    artifact_store = PostgresArtifactStore(connection)
    artifact_store.stage(bundle.manifest)
    published = artifact_store.publish_bundle(bundle)
    assert published.semantic_epoch_id is not None
    initial = published.semantic_epoch_id
    claim = bundle.structured.claims[0].claim_id
    with connection.transaction():
        connection.execute(
            "INSERT INTO groundloop_document VALUES ('o33-unused','fixture://unused','test')"
        )
        connection.execute(
            "INSERT INTO groundloop_document_version VALUES "
            "('o33-unused-v1','o33-unused',%s,%s,NULL)",
            (sha("unused-content"), initial),
        )
        connection.execute(
            "INSERT INTO groundloop_chunk_version VALUES "
            "('o33-unused-chunk','o33-unused-v1',0,'unused',%s,'fixed-char-v1',%s,NULL)",
            (sha("unused"), initial),
        )
        connection.execute(
            "INSERT INTO groundloop_chunk_provenance "
            "VALUES ('o33-unused-chunk','chunker',%s)",
            (sha("unused-content"),),
        )
        connection.execute(
            "INSERT INTO groundloop_model_artifact "
            "(model_artifact_id,task,provider,model_id,immutable_revision,"
            "tokenizer_revision,license_id,config_hash) VALUES "
            "('direct-m4-embedding','embedding','fixture','embedding','v1','v1','MIT',%s)",
            (sha("o33-embedding"),),
        )
    bootstrap_m4_publication(connection, sealed_epoch_id=initial)
    base = SeededBase(
        initial,
        bundle.policy.policy_version,
        "question",
        "answer",
        (claim,),
        ("chunk",),
    )
    policy = _m5_manifest(base)
    event = DeleteDocumentVersionEvent("o33-m3-predecessor", "o33-unused-v1")
    legacy = DynamicEventPlan(
        CorpusUpdateIdentity(
            event.event_id,
            legacy_event_payload_digest(event),
            UpdateKind.DELETE,
            initial,
            policy.candidate_policy_id,
        ),
        (),
        ("o33-unused-chunk",),
        (),
        REGISTRY_ID,
    )
    ports = PostgresM4ApplicationPorts(
        connection,
        structural_payloads={
            event.event_id: StructuralPayload(
                deactivated_document_version_id="o33-unused-v1"
            )
        },
        execution_mode=M4ExecutionMode.MEASURED,
    )
    ports.runtime_store.register_candidate_policy(_m4_manifest(base, policy))
    ports.register_claim_registry_snapshot(REGISTRY_ID, (claim,))
    admission, verifier = ScriptedAdmission({}), ScriptedVerifier({})
    application = M4Application(
        structural=ports,
        runtime=ports,
        admission=PersistingAdmissionPort(connection, admission),
        verifier=verifier,
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            sha("o33-impact"), sha("o33-frontier"), policy.verifier_execution_spec_hash
        ),
    )
    predecessor = application.run_event(legacy)
    assert predecessor.state is EventRunState.SEALED
    assert admission.calls == verifier.calls == 0
    assert connection.execute(
        "SELECT semantic_status,publication_mode FROM groundloop_epoch "
        "WHERE epoch_id=%s",
        (predecessor.epoch_id,),
    ).fetchone() == ("sealed", "strict")
    for name in reversed(diagnostic_names):
        connection.execute(
            sql.SQL("DROP VIEW {}.{} RESTRICT").format(
                sql.Identifier("pg_temp"), sql.Identifier(name)
            )
        )
    assert connection.execute(
        "SELECT count(*) FROM pg_class c JOIN pg_namespace n "
        "ON n.oid=c.relnamespace WHERE c.relname=ANY(%s) "
        "AND (n.nspname=%s OR n.oid=pg_my_temp_schema())",
        (diagnostic_names, schema),
    ).fetchone() == (0,)
    migrations.require_m5_document_update_completion_bundle(
        connection, schema_name=schema
    )
    ports = PostgresM4ApplicationPorts(
        connection, structural_payloads={}, execution_mode=M4ExecutionMode.MEASURED
    )
    store = PostgresM5RuntimeStore(connection)
    store.register_candidate_policy(policy)
    return OpenDatabase(
        connection,
        dsn,
        schema,
        replace(base, epoch_id=predecessor.epoch_id),
        None,
        policy,
        ports,
        store,
        store.prepare_activation_request("o33-direct-m3-activation"),
    )


@pytest.fixture(params=[False, True], ids=["rootless", "requirement-roots"])
def open_db(request: pytest.FixtureRequest) -> Iterator[OpenDatabase]:
    dsn = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("O33 requires its owned PostgreSQL test database")
    schema = f"groundloop_o33_{uuid.uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as connection:
        try:
            connection.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
            )
            connection.execute(
                sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
            )
            with connection.transaction():
                migrations.apply_legacy_migrations(connection)
            for installer in (
                migrations.install_m5_core_bundle,
                migrations.install_m5_runtime_bundle,
                migrations.install_m5_runtime_recovery_bundle,
                migrations.install_m5_persisted_matching_bundle,
                migrations.install_m5_bounded_document_withdrawal_bundle,
                migrations.install_m5_preterminal_seal_context_bundle,
                migrations.install_m5_semantic_readiness_bundle,
                migrations.install_m5_document_update_completion_bundle,
            ):
                if (
                    request.param == "missing-021"
                    and installer
                    is migrations.install_m5_document_update_completion_bundle
                ):
                    continue
                installer(connection)
            if request.param == "direct-m3":
                yield _m3_direct_foundation(connection, dsn, schema)
                return
            with connection.transaction():
                base = seed_base(
                    connection,
                    prefix="o33",
                    chunk_texts=("alpha", "beta"),
                    epoch_revision=1,
                )
                connection.execute(
                    "INSERT INTO groundloop_model_artifact "
                    "(model_artifact_id,task,provider,model_id,immutable_revision,"
                    "tokenizer_revision,license_id,config_hash) VALUES "
                    "('direct-m4-embedding','embedding','fixture','embedding','v1','v1','MIT',%s)",
                    (sha("o33-embedding"),),
                )
                connection.execute(
                    "INSERT INTO groundloop_chunker_artifact "
                    "(chunker_artifact_id,chunker_version,"
                    "normalization_version,config_hash) "
                    "VALUES ('o33-chunker','fixture-v1','v1',%s)",
                    (sha("o33-chunker"),),
                )
                for chunk in base.chunk_ids:
                    if (
                        request.param == "missing-provenance"
                        and chunk == base.chunk_ids[0]
                    ):
                        continue
                    connection.execute(
                        "INSERT INTO groundloop_chunk_provenance "
                        "(chunk_version_id,chunker_artifact_id,input_hash) "
                        "VALUES (%s,'o33-chunker',%s)",
                        (chunk, sha("o33-input")),
                    )
                remaining_chunks: list[ActiveChunkSnapshotEntry] = []
                retained_certificates: list[str] = []
                group = None
                if request.param and request.param not in {
                    "missing-provenance",
                    "missing-021",
                }:
                    group = make_group(
                        group_id="o33-group",
                        family_id="o33-family",
                        claim_id=base.claim_ids[0],
                        texts=("alpha", "beta"),
                    )
                    insert_published_group(
                        connection, group=group, epoch_id=base.epoch_id
                    )
                    for ordinal, requirement in enumerate(group.requirements):
                        if request.param == "partial" and ordinal == 1:
                            continue
                        observation = f"o33-observation-{ordinal}"
                        insert_observation(
                            connection,
                            observation_id=observation,
                            subject_kind="requirement",
                            subject_id=requirement.requirement_version_id,
                            chunk_id=base.chunk_ids[ordinal],
                            produced_epoch=base.epoch_id,
                        )
                        install_current_currency(
                            connection,
                            observation_id=observation,
                            subject_kind="requirement",
                            subject_id=requirement.requirement_version_id,
                            chunk_id=base.chunk_ids[ordinal],
                            task_type="verify_requirement_v1",
                            epoch_id=base.epoch_id,
                            revision=1,
                        )
                    if request.param in {
                        "complete-changed",
                        "complete-unchanged",
                        "complete-reused",
                    }:
                        connection.execute(
                            "INSERT INTO groundloop_document VALUES "
                            "('o33-alternate','fixture://alternate','test')"
                        )
                        connection.execute(
                            "INSERT INTO groundloop_document_version VALUES "
                            "('o33-alternate-v1','o33-alternate',%s,%s,NULL)",
                            (sha("alternate-source"), base.epoch_id),
                        )
                        for ordinal, text in enumerate(("alpha", "beta")):
                            chunk = f"o33-alternate-chunk-{ordinal}"
                            connection.execute(
                                "INSERT INTO groundloop_chunk_version VALUES "
                                "(%s,'o33-alternate-v1',%s,%s,%s,'fixture-v1',%s,NULL)",
                                (chunk, ordinal, text, sha(text), base.epoch_id),
                            )
                            connection.execute(
                                "INSERT INTO groundloop_chunk_provenance "
                                "(chunk_version_id,chunker_artifact_id,input_hash) "
                                "VALUES (%s,'o33-chunker',%s)",
                                (chunk, sha("alternate-input")),
                            )
                            prefix = (
                                "a" if request.param == "complete-unchanged" else "z"
                            )
                            observation = (
                                f"{prefix}-o33-alternate-observation-{ordinal}"
                            )
                            requirement = group.requirements[
                                ordinal
                            ].requirement_version_id
                            insert_observation(
                                connection,
                                observation_id=observation,
                                subject_kind="requirement",
                                subject_id=requirement,
                                chunk_id=chunk,
                                produced_epoch=base.epoch_id,
                            )
                            install_current_currency(
                                connection,
                                observation_id=observation,
                                subject_kind="requirement",
                                subject_id=requirement,
                                chunk_id=chunk,
                                task_type="verify_requirement_v1",
                                epoch_id=base.epoch_id,
                                revision=1,
                            )
                            remaining_chunks.append(
                                ActiveChunkSnapshotEntry(chunk, sha(text))
                            )
                        if request.param == "complete-reused":
                            # Retained immutable fixture artifacts only. No
                            # lifecycle/binding/matching/runtime header is
                            # seeded; genuine activation selects primary IDs.
                            artifact = m5_domain.GroupMatchingCertificateArtifact(
                                base.policy_version,
                                group.group_version_id,
                                tuple(
                                    m5_domain.GroupCertificateRow(
                                        ordinal,
                                        requirement.requirement_version_id,
                                        sha(("alpha", "beta")[ordinal]),
                                        f"z-o33-alternate-observation-{ordinal}",
                                    )
                                    for ordinal, requirement in enumerate(
                                        group.requirements
                                    )
                                ),
                            )
                            claim_artifact = m5_domain.ClaimCertificateArtifact(
                                claim_id=group.owner_claim_id,
                                decision_policy_version=base.policy_version,
                                support_kind=m5_domain.ClaimSupportKind.GROUP,
                                group_version_id=group.group_version_id,
                                group_certificate_digest=artifact.certificate_digest,
                            )
                            persist_group_certificate(connection, artifact)
                            persist_claim_certificate(connection, claim_artifact)
                            retained_certificates.extend(
                                (
                                    artifact.certificate_digest,
                                    claim_artifact.certificate_digest,
                                )
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
            store = PostgresM5RuntimeStore(connection)
            store.register_candidate_policy(policy)
            activation_request = store.prepare_activation_request("o33-activation")
            yield OpenDatabase(
                connection,
                dsn,
                schema,
                base,
                group,
                policy,
                ports,
                store,
                activation_request,
                tuple(remaining_chunks),
                tuple(retained_certificates),
            )
        finally:
            connection.execute("SET search_path TO public")
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )


def test_genuine_matching_activation_and_zero_write_replay(
    open_db: OpenDatabase,
) -> None:
    db = open_db
    assert db.connection.execute(
        "SELECT count(*) FROM groundloop_m5_activation"
    ).fetchone() == (0,)
    result = db.store._activate_document_open_prerequisite(db.request)
    assert not result.replayed and result.base_m4_epoch_id == db.base.epoch_id
    assert db.connection.execute(
        "SELECT installed_epoch_id,installed_revision "
        "FROM groundloop_m5_matching_image_current"
    ).fetchone() == (db.base.epoch_id, 1)
    before = database_snapshot(db.connection)
    replay = db.store._activate_document_open_prerequisite(db.request)
    assert replay.replayed and replay.receipt_hash == result.receipt_hash
    assert database_snapshot(db.connection) == before


_ACTIVATION_CUTS = (
    "o33_activation_before_bootstrap",
    "o33_activation_bootstrap_written",
    "o33_activation_matching_installed",
    "o33_activation_head_written",
    "o33_activation_record_written",
    "o33_activation_mode_written",
    "o33_activation_before_commit",
)


@pytest.mark.parametrize("cut", _ACTIVATION_CUTS)
def test_activation_named_cut_rollback_and_genuine_retry(
    open_db: OpenDatabase, cut: str
) -> None:
    """Named-cut coverage only; not the mandatory every-DML acceptance gate."""

    before = database_snapshot(open_db.connection)
    visited: list[str] = []

    def fail_at(stage: str) -> None:
        visited.append(stage)
        if stage == cut:
            raise RuntimeError("controlled O33 activation cut")

    with pytest.raises(RuntimeError, match="controlled O33 activation cut"):
        open_db.store._activate_document_open_prerequisite(
            open_db.request, failure_injector=fail_at
        )
    assert cut in visited
    assert database_snapshot(open_db.connection) == before
    assert not open_db.store._activate_document_open_prerequisite(
        open_db.request
    ).replayed


def test_activation_rejects_caller_transaction_without_writes(
    open_db: OpenDatabase,
) -> None:
    before = database_snapshot(open_db.connection)
    with open_db.connection.transaction():
        with pytest.raises(ValidationError, match="idle owned outer transaction"):
            open_db.store._activate_document_open_prerequisite(open_db.request)
    assert database_snapshot(open_db.connection) == before


@pytest.mark.parametrize("setting", ["ISOLATION LEVEL REPEATABLE READ", "READ ONLY"])
def test_activation_rejects_wrong_transaction_settings_without_writes(
    open_db: OpenDatabase, setting: str
) -> None:
    connection = open_db.connection
    before = database_snapshot(connection)
    connection.execute(f"SET SESSION CHARACTERISTICS AS TRANSACTION {setting}")
    try:
        with pytest.raises(ValidationError, match="READ COMMITTED|read-write"):
            open_db.store._activate_document_open_prerequisite(open_db.request)
    finally:
        connection.execute(
            "SET SESSION CHARACTERISTICS AS TRANSACTION "
            "ISOLATION LEVEL READ COMMITTED, READ WRITE"
        )
    assert database_snapshot(connection) == before


def test_activation_conflicting_identity_is_zero_write(open_db: OpenDatabase) -> None:
    open_db.store._activate_document_open_prerequisite(open_db.request)
    before = database_snapshot(open_db.connection)
    original = open_db.request
    conflict = M5ActivationRequest.build(
        activation_id="o33-conflicting-activation",
        expected_mode_revision=original.expected_mode_revision,
        expected_base_m4_epoch_id=original.expected_base_m4_epoch_id,
        core_schema_bundle_sha256=original.core_schema_bundle_sha256,
        bootstrap_state_hash=original.bootstrap_state_hash,
    )
    with pytest.raises(EventConflictError, match="retained identity changed"):
        open_db.store._activate_document_open_prerequisite(conflict)
    assert database_snapshot(open_db.connection) == before


def test_activation_rejects_temporary_shadow_without_writes(
    open_db: OpenDatabase,
) -> None:
    connection = open_db.connection
    connection.execute("CREATE TEMP TABLE groundloop_runtime_mode (singleton bool)")
    try:
        before = database_snapshot(connection)
        with pytest.raises(ValidationError, match="permanent relation is shadowed"):
            open_db.store._activate_document_open_prerequisite(open_db.request)
        assert database_snapshot(connection) == before
    finally:
        connection.execute("DROP TABLE pg_temp.groundloop_runtime_mode")


def _open(db: OpenDatabase, plan: M5TypedEventPlan, **kwargs: Any) -> Any:
    direct, required, roots, root_hash = db.proposals(plan)
    arguments = dict(
        ports=db.ports,
        execution_policy=db.execution,
        supplied_direct_open=direct,
        supplied_requirement_withdrawal=required,
        requirement_roots=roots,
        requirement_root_set_hash=root_hash,
        recovery_operational_config=M5RuntimeOperationalConfig.build(60_000),
        recovery_root_fallback_required={r.job.logical_job_id: True for r in roots},
    )
    arguments.update(kwargs)
    return db.store._open_document_open_prerequisite(plan, **arguments)


def _open_arguments(db: OpenDatabase, plan: M5TypedEventPlan) -> dict[str, Any]:
    direct, required, roots, root_hash = db.proposals(plan)
    return dict(
        ports=db.ports,
        execution_policy=db.execution,
        supplied_direct_open=direct,
        supplied_requirement_withdrawal=required,
        requirement_roots=roots,
        requirement_root_set_hash=root_hash,
        recovery_operational_config=M5RuntimeOperationalConfig.build(60_000),
        recovery_root_fallback_required={r.job.logical_job_id: True for r in roots},
    )


def test_exact_replay_ignores_absent_event_proposals_and_writes_nothing(
    open_db: OpenDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = open_db
    db.store._activate_document_open_prerequisite(db.request)
    plan = db.plan()
    arguments = _open_arguments(db, plan)
    receipt = db.store._open_document_open_prerequisite(plan, **arguments)
    before = database_snapshot(db.connection)
    # A consumed/malformed absent-event proposal is not retained authority.
    arguments.update(
        supplied_direct_open=None,
        supplied_requirement_withdrawal=None,
        requirement_roots=(),
        requirement_root_set_hash="0" * 64,
    )

    def no_fresh_phase(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail("retained OPEN entered a first-application/cache phase")

    for name in (
        "_prepare_locked_document_open",
        "_continue_locked_document_open",
        "_preview_document_direct_open",
        "_plan_document_requirement_withdrawal",
    ):
        monkeypatch.setattr(withdrawal, name, no_fresh_phase)
    monkeypatch.setattr(db.ports, "_stage_structural_detached", no_fresh_phase)
    replay = db.store._open_document_open_prerequisite(plan, **arguments)
    assert replay.replayed and replay.epoch_id == receipt.epoch_id
    assert database_snapshot(db.connection) == before


def test_wrong_retained_recovery_config_is_zero_write(open_db: OpenDatabase) -> None:
    db = open_db
    db.store._activate_document_open_prerequisite(db.request)
    plan = db.plan()
    arguments = _open_arguments(db, plan)
    db.store._open_document_open_prerequisite(plan, **arguments)
    before = database_snapshot(db.connection)
    arguments["recovery_operational_config"] = M5RuntimeOperationalConfig.build(60_001)
    with pytest.raises(EventConflictError, match="recovery image differs"):
        db.store._open_document_open_prerequisite(plan, **arguments)
    assert database_snapshot(db.connection) == before


@pytest.mark.parametrize(
    "conflict", ["valid-new-payload", "different-retained-predecessor"]
)
def test_coherent_retained_identity_conflict_has_zero_dml_and_sequence_change(
    open_db: OpenDatabase,
    conflict: str,
) -> None:
    db = open_db
    db.store._activate_document_open_prerequisite(db.request)
    plan = db.plan()
    arguments = _open_arguments(db, plan)
    db.store._open_document_open_prerequisite(plan, **arguments)
    changed = (
        _retarget_delete(plan, old="different-valid-source")
        if conflict == "valid-new-payload"
        else _retarget_delete(plan, previous=db.base.epoch_id + 1)
    )
    before = database_snapshot(db.connection)
    with pytest.raises(EventConflictError):
        db.store._open_document_open_prerequisite(changed, **arguments)
    assert database_snapshot(db.connection) == before


@pytest.mark.parametrize("operation", ["activation", "open"])
def test_present_but_corrupt_021_fails_closed_before_writes(
    open_db: OpenDatabase, operation: str
) -> None:
    """Owned negative catalog corruption only, never relaxed positive authority."""
    db = open_db
    plan = db.plan()
    arguments = {}
    if operation == "open":
        db.store._activate_document_open_prerequisite(db.request)
        arguments = _open_arguments(db, plan)
    db.connection.execute(
        "CREATE OR REPLACE FUNCTION groundloop_m5_validate_event_result_children() "
        "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
        "RAISE EXCEPTION 'controlled fail-closed negative021'; END; $$"
    )
    before = database_snapshot(db.connection)
    with pytest.raises(migrations.M5DocumentUpdateCompletionBundleError):
        if operation == "activation":
            db.store._activate_document_open_prerequisite(db.request)
        else:
            db.store._open_document_open_prerequisite(plan, **arguments)
    assert database_snapshot(db.connection) == before


def _retarget_delete(
    plan: M5TypedEventPlan, *, old: str | None = None, previous: int | None = None
) -> M5TypedEventPlan:
    event = replace(
        plan.event, document_version_id=old or plan.event.document_version_id
    )
    payload = legacy_event_payload_digest(event)
    point = (
        previous if previous is not None else plan.expected_previous_published_epoch_id
    )
    direct = replace(
        plan.direct_plan,
        update=replace(
            plan.direct_plan.update,
            payload_hash=payload,
            previous_published_epoch_id=point,
        ),
    )
    return replace(
        plan,
        event=event,
        payload_hash=payload,
        direct_plan=direct,
        expected_previous_published_epoch_id=point,
    )


@pytest.mark.parametrize(
    "case",
    [
        "audit",
        "foreign-port",
        "foreign-runtime",
        "foreign-evaluation",
        "ports-type",
        "execution-type",
        "config-type",
        "wrong-verifier",
        "wrong-policy-manifest",
        "old-predecessor",
        "missing-source",
        "incomplete-deactivation",
        "root-list",
        "root-missing-coherent",
        "root-hash",
        "root-digest",
        "wrong-registry-snapshot",
        "wrong-active-snapshot",
        "withdrawal-digest",
        "direct-fallback",
        "payload-tamper",
        "cache-active",
        "cache-alias",
        "cache-document-hash",
        "cache-parent",
        "cache-chunk-set",
        "cache-chunk-text",
        "cache-chunk-inactive",
        "cache-policy",
    ],
)
@pytest.mark.parametrize("open_db", [True], indirect=True)
def test_open_rejects_exact_boundary_matrix_without_material_writes(
    open_db: OpenDatabase,
    case: str,
) -> None:
    db = open_db
    db.store._activate_document_open_prerequisite(db.request)
    plan = db.plan()
    args = _open_arguments(db, plan)
    repository = db.ports._published_repository
    with psycopg.connect(db.dsn, autocommit=True) as other:
        other.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(db.schema))
        )
        if case == "audit":
            db.ports.execution_mode = M4ExecutionMode.AUDIT
        elif case == "foreign-port":
            db.ports.connection = other
        elif case == "foreign-runtime":
            db.ports.runtime_store = PostgresM4RuntimeStore(
                other, audit_transitions=False
            )
        elif case == "foreign-evaluation":
            db.ports.evaluation_store = (
                evaluation_overlay.PostgresEvaluationOverlayStore(other)
            )
        elif case == "ports-type":
            args["ports"] = object()
        elif case == "execution-type":
            args["execution_policy"] = object()
        elif case == "config-type":
            args["recovery_operational_config"] = object()
        elif case == "wrong-verifier":
            args["execution_policy"] = replace(
                db.execution, verifier_execution_spec_hash=sha("wrong")
            )
        elif case == "wrong-policy-manifest":
            plan = copy(plan)
            object.__setattr__(plan, "candidate_policy_manifest_hash", sha("wrong"))
        elif case == "old-predecessor":
            plan = _retarget_delete(plan, previous=db.base.epoch_id + 1)
        elif case == "missing-source":
            plan = _retarget_delete(plan, old="missing-o33-version")
        elif case == "incomplete-deactivation":
            plan = replace(
                plan,
                direct_plan=replace(
                    plan.direct_plan,
                    deactivated_chunk_version_ids=(db.base.chunk_ids[0],),
                ),
            )
        elif case == "root-list":
            args["requirement_roots"] = list(args["requirement_roots"])
        elif case == "root-missing-coherent":
            roots = args["requirement_roots"][:-1]
            args.update(
                requirement_roots=roots,
                requirement_root_set_hash=digests.requirement_root_set_digest(
                    r.job.logical_job_id for r in roots
                ),
                recovery_root_fallback_required={
                    r.job.logical_job_id: True for r in roots
                },
            )
        elif case == "root-hash":
            args["requirement_root_set_hash"] = sha("wrong")
        elif case == "root-digest":
            root = copy(args["requirement_roots"][0])
            object.__setattr__(root, "job", copy(root.job))
            object.__setattr__(root.job, "execution_spec_hash", sha("wrong"))
            args["requirement_roots"] = (root, *args["requirement_roots"][1:])
        elif case == "wrong-registry-snapshot":
            plan = replace(
                plan,
                requirement_registry_snapshot=RequirementRegistrySnapshot.build(()),
            )
            args.update(
                requirement_roots=(),
                requirement_root_set_hash=digests.requirement_root_set_digest(()),
                recovery_root_fallback_required={},
            )
        elif case == "wrong-active-snapshot":
            plan = replace(
                plan,
                active_chunk_snapshot=ActiveChunkSnapshot.build(
                    (ActiveChunkSnapshotEntry(db.base.chunk_ids[0], sha("alpha")),)
                ),
            )
        elif case == "withdrawal-digest":
            value = copy(args["supplied_requirement_withdrawal"])
            object.__setattr__(value, "plan_digest", sha("wrong"))
            args["supplied_requirement_withdrawal"] = value
        elif case == "direct-fallback":
            direct = args["supplied_direct_open"]
            args["supplied_direct_open"] = replace(
                direct,
                withdrawal=replace(
                    direct.withdrawal, fallback_claim_ids=db.base.claim_ids
                ),
            )
        elif case == "payload-tamper":
            plan = copy(plan)
            object.__setattr__(plan, "payload_hash", sha("wrong"))
        elif case == "cache-active":
            db.ports._active_epoch_id = db.base.epoch_id
        elif case == "cache-alias":
            db.ports._working_repository = deepcopy(repository)
        elif case == "cache-document-hash":
            old = repository._document_versions["o33-document-v1"]
            repository._document_versions[old.document_version_id] = replace(
                old, content_hash=sha("wrong")
            )
        elif case == "cache-parent":
            old = repository._document_versions["o33-document-v1"]
            repository._document_versions[old.document_version_id] = replace(
                old, document_id="wrong-parent"
            )
        elif case == "cache-chunk-set":
            repository._chunks_by_document_version["o33-document-v1"] = list(
                db.base.chunk_ids[:1]
            )
        elif case == "cache-chunk-text":
            old = repository._chunk_versions[db.base.chunk_ids[0]]
            repository._chunk_versions[old.chunk_version_id] = replace(
                old, text="wrong", text_hash=""
            )
        elif case == "cache-chunk-inactive":
            interval = repository._chunk_validity[db.base.chunk_ids[0]]
            repository._chunk_validity[db.base.chunk_ids[0]] = (
                interval[0],
                repository.current_epoch,
            )
        elif case == "cache-policy":
            old = repository.current_policy()
            repository._policies[old.policy_version] = replace(
                old, support_threshold=0.99
            )
        else:
            raise AssertionError(case)
        full_before = database_snapshot(db.connection)
        before = _table_image(db.connection)
        cache = _cache_image(db.ports)
        with pytest.raises((ValidationError, EventConflictError, InvalidEventError)):
            db.store._open_document_open_prerequisite(plan, **args)
        assert _table_image(db.connection) == before
        assert _cache_image(db.ports) == cache
        if case in {
            "audit",
            "foreign-port",
            "foreign-runtime",
            "foreign-evaluation",
            "ports-type",
            "execution-type",
            "config-type",
            "wrong-verifier",
            "wrong-policy-manifest",
            "old-predecessor",
            "root-list",
            "root-hash",
            "root-digest",
            "payload-tamper",
            "direct-fallback",
        }:
            assert database_snapshot(db.connection) == full_before


@pytest.mark.parametrize("kind", ["insert", "replace"])
def test_coherent_insert_and_replace_are_excluded_at_kind_guard(
    open_db: OpenDatabase, kind: str
) -> None:
    db = open_db
    db.store._activate_document_open_prerequisite(db.request)
    original = db.plan()
    args = _open_arguments(db, original)
    chunks = (ChunkInput("new-o33-chunk", 0, "new source"),)
    if kind == "insert":
        event = InsertDocumentEvent(
            "o33-insert", "new-o33-doc", "new-o33-v1", sha("new"), chunks
        )
    else:
        event = ReplaceDocumentVersionEvent(
            "o33-replace",
            "o33-document",
            "o33-document-v1",
            "new-o33-v1",
            sha("new"),
            chunks,
        )
    payload = legacy_event_payload_digest(event)
    direct = DynamicEventPlan(
        CorpusUpdateIdentity(
            event.event_id,
            payload,
            UpdateKind.INSERT if kind == "insert" else UpdateKind.REPLACE,
            db.base.epoch_id,
            db.policy.candidate_policy_id,
        ),
        ("new-o33-chunk",),
        () if kind == "insert" else db.base.chunk_ids,
        (),
        REGISTRY_ID,
    )
    plan = replace(
        original,
        structural_event_id=event.event_id,
        event=event,
        payload_hash=payload,
        direct_plan=direct,
    )
    before = database_snapshot(db.connection)
    with pytest.raises(InvalidEventError, match="job-free DELETE only"):
        db.store._open_document_open_prerequisite(plan, **args)
    assert database_snapshot(db.connection) == before


@pytest.mark.parametrize(
    "setting",
    [
        "ISOLATION LEVEL REPEATABLE READ",
        "READ ONLY",
        "caller-transaction",
        "temporary-shadow",
    ],
)
def test_open_rejects_connection_context_without_writes(
    open_db: OpenDatabase, setting: str
) -> None:
    db = open_db
    db.store._activate_document_open_prerequisite(db.request)
    plan = db.plan()
    args = _open_arguments(db, plan)
    if setting == "temporary-shadow":
        db.connection.execute(
            "CREATE TEMP TABLE groundloop_runtime_mode(singleton bool)"
        )
    elif setting != "caller-transaction":
        db.connection.execute(f"SET SESSION CHARACTERISTICS AS TRANSACTION {setting}")
    before = database_snapshot(db.connection)
    try:
        with pytest.raises(ValidationError):
            if setting == "caller-transaction":
                with db.connection.transaction():
                    db.store._open_document_open_prerequisite(plan, **args)
            else:
                db.store._open_document_open_prerequisite(plan, **args)
        assert database_snapshot(db.connection) == before
    finally:
        if setting == "temporary-shadow":
            db.connection.execute("DROP TABLE pg_temp.groundloop_runtime_mode")
        else:
            db.connection.execute(
                "SET SESSION CHARACTERISTICS AS TRANSACTION "
                "ISOLATION LEVEL READ COMMITTED, READ WRITE"
            )


@pytest.mark.parametrize("open_db", ["missing-provenance"], indirect=True)
def test_missing_pre_activation_provenance_rejects_genuine_open(
    open_db: OpenDatabase,
) -> None:
    db = open_db
    db.store._activate_document_open_prerequisite(db.request)
    # Preview is compare-only and may succeed; locked source authority must
    # still reject the deliberately incomplete pre-activation foundation.
    plan = db.plan()
    arguments = _open_arguments(db, plan)
    before = _table_image(db.connection)
    with pytest.raises((ValidationError, EventConflictError)):
        db.store._open_document_open_prerequisite(plan, **arguments)
    assert _table_image(db.connection) == before


@pytest.mark.parametrize("open_db", ["missing-021"], indirect=True)
def test_missing_021_has_no_activation_or_open_fallback(open_db: OpenDatabase) -> None:
    db = open_db
    before = database_snapshot(db.connection)
    with pytest.raises(migrations.M5DocumentUpdateCompletionBundleError):
        db.store._activate_document_open_prerequisite(db.request)
    with pytest.raises(migrations.M5DocumentUpdateCompletionBundleError):
        db.store._open_document_open_prerequisite(
            db.plan(),
            ports=db.ports,
            execution_policy=db.execution,
            supplied_direct_open=None,
            supplied_requirement_withdrawal=None,
            requirement_roots=(),
            requirement_root_set_hash=digests.requirement_root_set_digest(()),
            recovery_operational_config=M5RuntimeOperationalConfig.build(60_000),
            recovery_root_fallback_required={},
        )
    assert database_snapshot(db.connection) == before


@pytest.mark.parametrize(
    "column,value",
    [
        ("chunker_version", "wrong-o33-version"),
        ("content_hash", sha("changed-after-preview")),
    ],
)
def test_changed_source_after_absent_preview_is_locked_rejected(
    open_db: OpenDatabase, column: str, value: str
) -> None:
    """Isolated negative raw-source corruption, never a production mutation API."""
    db = open_db
    db.store._activate_document_open_prerequisite(db.request)
    plan = db.plan()
    args = _open_arguments(db, plan)
    with psycopg.connect(db.dsn, autocommit=True) as adversary:
        adversary.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(db.schema))
        )
        if column == "chunker_version":
            adversary.execute(
                "UPDATE groundloop_chunk_version SET chunker_version=%s "
                "WHERE chunk_version_id=%s",
                (value, db.base.chunk_ids[0]),
            )
        else:
            adversary.execute(
                "UPDATE groundloop_document_version SET content_hash=%s "
                "WHERE document_version_id='o33-document-v1'",
                (value,),
            )
    before, cache = _table_image(db.connection), _cache_image(db.ports)
    with pytest.raises((ValidationError, EventConflictError)):
        db.store._open_document_open_prerequisite(plan, **args)
    assert _table_image(db.connection) == before
    assert _cache_image(db.ports) == cache


@pytest.mark.parametrize("open_db", ["direct-m3"], indirect=True)
def test_genuine_direct_history_delete_is_rejected_without_writes(
    open_db: OpenDatabase,
) -> None:
    db = open_db
    db.store._activate_document_open_prerequisite(db.request)
    event = DeleteDocumentVersionEvent("o33-direct-history-delete", "document-version")
    payload = legacy_event_payload_digest(event)
    plan = M5TypedEventPlan(
        event.event_id,
        event,
        payload,
        DynamicEventPlan(
            CorpusUpdateIdentity(
                event.event_id,
                payload,
                UpdateKind.DELETE,
                db.base.epoch_id,
                db.policy.candidate_policy_id,
            ),
            (),
            ("chunk",),
            (),
            REGISTRY_ID,
        ),
        db.policy.candidate_policy_id,
        db.policy.manifest_hash,
        RequirementRegistrySnapshot.build(()),
        ActiveChunkSnapshot.build(()),
        db.base.epoch_id,
    )
    with db.connection.transaction(), db.connection.cursor() as cursor:
        row = cursor.execute(
            "SELECT subject_kind::text,subject_id,chunk_version_id,task_type,"
            "observation_id,installed_revision FROM groundloop_observation_currency "
            "WHERE subject_kind='claim'"
        ).fetchone()
        assert row is not None and row[-1] == 0
        authority = withdrawal._gather_d30_claim_authority(
            cursor, (withdrawal._D30CurrencyRow(*row),)
        )
        assert len(authority.bootstrap) == 1 and authority.dynamic == ()
        withdrawal._validate_d30_m3_bootstrap(
            authority.bootstrap[0],
            activation_base_epoch_id=db.base.epoch_id,
            predecessor_epoch_id=db.base.epoch_id,
        )
    arguments = _open_arguments(db, plan)
    direct = arguments["supplied_direct_open"]
    assert direct.withdrawal.plan.observation_ids == (row[4],)
    assert direct.withdrawal.plan.affected_claim_ids == db.base.claim_ids
    assert direct.withdrawal.fallback_claim_ids == db.base.claim_ids
    assert (
        len(direct.root_jobs) == 1
        and direct.root_jobs[0].kind.value == "frontier_retrieve"
    )
    assert direct.discovery_scopes == ()
    before = database_snapshot(db.connection)
    cache = _cache_image(db.ports)
    with pytest.raises(
        InvalidEventError, match="all three direct declaration arrays empty"
    ):
        db.store._open_document_open_prerequisite(plan, **arguments)
    assert database_snapshot(db.connection) == before
    assert _cache_image(db.ports) == cache


@pytest.mark.parametrize("open_db", [True], indirect=True, ids=["requirement-roots"])
def test_retained_replay_after_genuine_requirement_cancellation(
    open_db: OpenDatabase,
) -> None:
    db = open_db
    db.store._activate_document_open_prerequisite(db.request)
    plan = db.plan()
    arguments = _open_arguments(db, plan)
    receipt = db.store._open_document_open_prerequisite(plan, **arguments)
    cancellation = M5CancellationPlan.build(
        epoch_id=receipt.epoch_id,
        structural_event_id=plan.structural_event_id,
        cancelled_job_ids=tuple(
            r.job.logical_job_id for r in arguments["requirement_roots"]
        ),
        reason=M5TerminalReason.SCOPE_RETIRED,
    )
    db.store.cancel_m5_work_atomically(receipt.epoch_id, 1, cancellation)
    assert db.connection.execute(
        "SELECT revision,runtime_state,open_work_count,open_scope_count "
        "FROM groundloop_m5_runtime_epoch WHERE epoch_id=%s",
        (receipt.epoch_id,),
    ).fetchone() == (2, "semantic_pending", 0, 0)
    before = database_snapshot(db.connection)
    replay = db.store._open_document_open_prerequisite(plan, **arguments)
    assert replay.replayed and replay.epoch_id == receipt.epoch_id
    assert database_snapshot(db.connection) == before


def test_genuine_job_free_delete_open_draft(open_db: OpenDatabase) -> None:
    db = open_db
    db.store._activate_document_open_prerequisite(db.request)
    receipt = _open(db, db.plan())
    assert not receipt.replayed
    assert db.connection.execute(
        "SELECT revision,runtime_state,open_work_count,open_scope_count "
        "FROM groundloop_m5_runtime_epoch WHERE epoch_id=%s",
        (receipt.epoch_id,),
    ).fetchone() == (
        1,
        "structural_committed",
        0 if db.group is None else 2,
        0 if db.group is None else 2,
    )


def _wire(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {k: _wire(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [_wire(v) for v in value]
    return value


def _raw_python_point(db: OpenDatabase, epoch: int) -> M5Repository:
    """Only raw fixture/domain rows and independently resolved raw currency."""
    connection = db.connection
    base = InMemoryRepository(current_epoch=epoch)
    for row in connection.execute("SELECT question_id,text FROM groundloop_question"):
        base.register_question(Question(*row))
    for row in connection.execute(
        "SELECT answer_version_id,question_id,text,generator_model_id,"
        "generator_model_version,prompt_version FROM groundloop_answer_version"
    ):
        claims = tuple(
            Claim(str(c[0]), str(c[1]), str(c[2]), ModelStamp(*c[3:6]), bool(c[6]))
            for c in connection.execute(
                "SELECT claim_id,answer_version_id,text,extractor_model_id,"
                "extractor_model_version,extractor_prompt_version,required "
                "FROM groundloop_claim WHERE answer_version_id=%s",
                (row[0],),
            ).fetchall()
        )
        base.register_answer(AnswerVersion(*row[:3], ModelStamp(*row[3:])), claims)
    for row in connection.execute(
        "SELECT document_version_id,document_id,btrim(content_hash),valid_from_epoch "
        "FROM groundloop_document_version WHERE valid_from_epoch<=%s "
        "AND (valid_to_epoch IS NULL OR %s<valid_to_epoch)",
        (epoch, epoch),
    ).fetchall():
        chunks = tuple(
            ChunkVersion(*c)
            for c in connection.execute(
                "SELECT chunk_version_id,document_version_id,chunk_index,text,"
                "btrim(text_hash) "
                "FROM groundloop_chunk_version WHERE document_version_id=%s "
                "AND valid_from_epoch<=%s AND "
                "(valid_to_epoch IS NULL OR %s<valid_to_epoch) "
                "ORDER BY chunk_index",
                (row[0], epoch, epoch),
            ).fetchall()
        )
        base.register_document_version(DocumentVersion(*row[:3]), chunks, int(row[3]))
    policy = connection.execute(
        "SELECT policy_version,support_threshold,refute_threshold,tie_rule_version,"
        "valid_from_epoch FROM groundloop_decision_policy WHERE valid_from_epoch<=%s "
        "AND (valid_to_epoch IS NULL OR %s<valid_to_epoch)",
        (epoch, epoch),
    ).fetchone()
    assert policy is not None
    base.activate_policy(DecisionPolicy(*policy[:4]), int(policy[4]))
    repo = M5Repository(base, semantic_revision=1)
    if db.group is not None:
        # Original immutable fixture source, unchanged by DELETE; never a
        # materialized/matching/certificate/patch target as oracle input.
        repo.register_group(db.group, db.base.epoch_id)
    currency = {
        tuple(row[:4]): row[4]
        for row in connection.execute(
            "SELECT subject_kind,subject_id,chunk_version_id,task_type,observation_id "
            "FROM groundloop_published_observation_currency "
            "WHERE valid_from_epoch<=%s "
            "AND (valid_to_epoch IS NULL OR %s<valid_to_epoch)",
            (db.base.epoch_id, db.base.epoch_id),
        ).fetchall()
    }
    if epoch != db.base.epoch_id:
        for row in connection.execute(
            "SELECT subject_kind,subject_id,chunk_version_id,task_type,observation_id "
            "FROM groundloop_m5_working_currency_history WHERE epoch_id=%s "
            "AND valid_from_revision<=1 AND "
            "(valid_to_revision IS NULL OR 1<valid_to_revision)",
            (epoch,),
        ).fetchall():
            currency[tuple(row[:4])] = row[4]  # Preserve NULL suppression.
        effective = {
            tuple(row[:4]): row[4]
            for row in connection.execute(
                "SELECT subject_kind,subject_id,chunk_version_id,task_type,"
                "observation_id "
                "FROM groundloop_m5_currency_at(%s,1)",
                (epoch,),
            ).fetchall()
        }
        assert currency == effective
    for observation_id in sorted(v for v in currency.values() if v is not None):
        row = connection.execute(
            "SELECT observation_id,subject_kind,subject_id,chunk_version_id,task_type,"
            "support_score,refute_score,neutral_score,model_id,model_version,"
            "prompt_version,input_hash FROM groundloop_semantic_observation "
            "WHERE observation_id=%s",
            (observation_id,),
        ).fetchone()
        assert row is not None
        if not base.has_chunk_version(str(row[3])):
            continue
        observation = SemanticObservation(
            str(row[0]),
            SubjectKind(str(row[1])),
            str(row[2]),
            str(row[3]),
            str(row[4]),
            *row[5:8],
            ModelStamp(*row[8:11]),
            str(row[11]),
        )
        if observation.subject_kind is SubjectKind.REQUIREMENT:
            repo.register_requirement_observation(observation, repo.current_point)
        else:
            base.register_observation(observation)
    return repo


def _independent_sql_point(
    db: OpenDatabase, epoch: int
) -> dict[str, list[dict[str, Any]]]:
    """Inline accepted SQL view bodies, with explicit point/currency CTEs.

    Stored view dependencies cannot be shadowed by a caller CTE. Every actual
    dependency is inlined; no head mutation, DDL or target-state input.
    """
    root = Path(__file__).resolve().parents[4]
    source = (root / "sql/m5/full_recompute_oracle.sql").read_text()
    bodies = dict(
        re.findall(r"CREATE OR REPLACE VIEW (\w+) AS\n(.*?);\n", source, re.S)
    )
    names = [
        "groundloop_m5_oracle_snapshot",
        "groundloop_m5_observation_decision_oracle",
        "groundloop_m5_active_requirement_edge_oracle",
        "groundloop_m5_requirement_state_oracle",
        "groundloop_m5_group_subset_hall_oracle",
        "groundloop_m5_group_state_oracle",
        "groundloop_m5_direct_claim_state_oracle",
        "groundloop_m5_claim_state_oracle",
        "groundloop_m5_answer_state_oracle",
    ]
    bodies[names[0]] = (
        "SELECT %(e)s::bigint AS epoch_id,policy.policy_version,"
        "policy.support_threshold,"
        "policy.refute_threshold FROM groundloop_decision_policy policy "
        "WHERE policy.valid_from_epoch<=%(e)s "
        "AND (policy.valid_to_epoch IS NULL OR %(e)s<policy.valid_to_epoch)"
    )
    bodies[names[1]] = bodies[names[1]].replace(
        "FROM groundloop_observation_currency AS currency",
        "FROM currency_at AS currency",
    )
    currency = (
        "SELECT subject_kind,subject_id,chunk_version_id,task_type,observation_id "
        "FROM groundloop_published_observation_currency WHERE valid_from_epoch<=%(e)s "
        "AND (valid_to_epoch IS NULL OR %(e)s<valid_to_epoch)"
        if epoch == db.base.epoch_id
        else "SELECT * FROM groundloop_m5_currency_at(%(e)s,1)"
    )
    ctes = [
        f"currency_at AS ({currency})",
        *(f"{name} AS ({bodies[name]})" for name in names),
    ]
    selections = {
        "requirements": names[3],
        "groups": names[5],
        "claims": names[7],
        "answers": names[8],
        "edges": names[2],
        "hall": names[4],
    }
    query = (
        "WITH "
        + ",".join(ctes)
        + " "
        + " UNION ALL ".join(
            f"SELECT '{family}',to_jsonb(value) FROM {view} value"
            for family, view in selections.items()
        )
    )
    result: dict[str, list[dict[str, Any]]] = {family: [] for family in selections}
    for family, row in db.connection.execute(query, {"e": epoch}).fetchall():
        result[family].append(row)
    return result


def _reference_artifacts(repo: M5Repository) -> dict[tuple[str, str], str]:
    states = compute_reference_states(repo)
    policy = repo.base.current_policy().policy_version
    group_certificates = {
        key: artifact
        for key in states.groups
        if (artifact := build_reference_group_certificate(repo, key)) is not None
    }
    result = {
        ("requirement_state", key): digests.requirement_state_artifact_digest(
            **asdict(state), decision_policy_version=policy
        )
        for key, state in states.requirements.items()
    }
    for key, state in states.groups.items():
        certificate = group_certificates.get(key)
        result["group_state", key] = digests.group_state_artifact_digest(
            **asdict(state),
            decision_policy_version=policy,
            certificate_digest=None
            if certificate is None
            else certificate.certificate_digest,
        )
        if certificate is not None:
            result["group_certificate", key] = certificate.certificate_digest
    for key, state in states.claims.items():
        certificate = build_reference_claim_certificate(repo, key, group_certificates)
        result["claim_state", key] = digests.claim_state_artifact_digest(
            **asdict(state),
            decision_policy_version=policy,
            certificate_digest=certificate.certificate_digest,
        )
        result["claim_certificate", key] = certificate.certificate_digest
    result.update(
        {
            ("answer_state", key): digests.answer_state_artifact_digest(**asdict(state))
            for key, state in states.answers.items()
        }
    )
    return result


def _assert_complete_point(db: OpenDatabase, epoch: int) -> M5Repository:
    repo = _raw_python_point(db, epoch)
    states = compute_reference_states(repo)
    independent_sql = _independent_sql_point(db, epoch)
    for family in ("requirements", "groups", "claims", "answers"):
        expected = getattr(states, family)
        key = {
            "requirements": "requirement_version_id",
            "groups": "group_version_id",
            "claims": "claim_id",
            "answers": "answer_version_id",
        }[family]
        sql_rows = {row[key]: row for row in independent_sql[family]}
        assert sql_rows == {
            identity: _wire(asdict(value)) for identity, value in expected.items()
        }
        kind = family[:-1] if family != "requirements" else "requirement"
        published = db.connection.execute(
            sql.SQL(
                "SELECT to_jsonb(value) FROM {} value WHERE valid_from_epoch<=%s "
                "AND (valid_to_epoch IS NULL OR %s<valid_to_epoch)"
            ).format(sql.Identifier(f"groundloop_m5_published_{kind}_state")),
            (db.base.epoch_id, db.base.epoch_id),
        ).fetchall()
        effective = {row[0][key]: row[0] for row in published}
        if epoch != db.base.epoch_id:
            for (row,) in db.connection.execute(
                sql.SQL(
                    "SELECT to_jsonb(value) FROM {} value WHERE epoch_id=%s"
                ).format(sql.Identifier(f"groundloop_m5_working_{kind}_state")),
                (epoch,),
            ).fetchall():
                effective[row[key]] = row
        assert {
            identity: {name: row[name] for name in asdict(expected[identity])}
            for identity, row in effective.items()
        } == sql_rows
    witnesses = derive_active_requirement_witnesses(repo)
    edges = {
        (w.requirement_version_id, w.text_hash): len(w.active_observation_ids)
        for w in witnesses
    }
    edge_payloads = {
        (w.requirement_version_id, w.text_hash): (
            repo.requirement(w.requirement_version_id).group_version_id,
            w.requirement_ordinal,
            len(w.active_observation_ids),
        )
        for w in witnesses
    }
    observations = {
        oid: (
            w.requirement_version_id,
            repo.requirement(w.requirement_version_id).group_version_id,
            w.requirement_ordinal,
            w.text_hash,
        )
        for w in witnesses
        for oid in w.active_observation_ids
    }
    masks: dict[tuple[str, str], int] = {}
    for w in witnesses:
        group_id = repo.requirement(w.requirement_version_id).group_version_id
        masks[group_id, w.text_hash] = (
            masks.get((group_id, w.text_hash), 0) | 1 << w.requirement_ordinal
        )
    assert {
        (r["requirement_version_id"], r["text_hash"]): len(r["active_observation_ids"])
        for r in independent_sql["edges"]
    } == edges
    for family, columns, keys, positive, expected in (
        (
            "observation",
            "observation_id,requirement_version_id,group_version_id,requirement_ordinal,text_hash",
            1,
            "present",
            observations,
        ),
        (
            "edge",
            "requirement_version_id,text_hash,group_version_id,requirement_ordinal,refcount",
            2,
            "refcount",
            edge_payloads,
        ),
        ("hash_mask", "group_version_id,text_hash,mask", 2, "mask", masks),
    ):
        current = db.connection.execute(
            f"SELECT {columns} FROM groundloop_m5_matching_{family}_current"
        ).fetchall()
        effective = {
            (r[0] if keys == 1 else tuple(r[:keys])): (
                tuple(r[keys:]) if family in {"observation", "edge"} else r[-1]
            )
            for r in current
        }
        if epoch != db.base.epoch_id:
            rows = db.connection.execute(
                f"SELECT {columns},{positive} "
                f"FROM groundloop_m5_matching_{family}_working WHERE epoch_id=%s",
                (epoch,),
            ).fetchall()
            for r in rows:
                key = r[0] if keys == 1 else tuple(r[:keys])
                if not r[-1]:
                    effective.pop(key, None)
                else:
                    effective[key] = (
                        tuple(r[keys:-1])
                        if family in {"observation", "edge"}
                        else r[-2]
                    )
        assert effective == expected
    hall_ids = {
        row[0]
        for row in db.connection.execute(
            "SELECT group_version_id FROM groundloop_m5_matching_hall_current"
        ).fetchall()
    }
    if epoch != db.base.epoch_id:
        for group_id, present in db.connection.execute(
            "SELECT group_version_id,present "
            "FROM groundloop_m5_matching_hall_working WHERE epoch_id=%s",
            (epoch,),
        ).fetchall():
            assert present is True  # DELETE does not retire a group.
            hall_ids.add(group_id)
    assert hall_ids == set(states.groups)
    for group_id, state in states.groups.items():
        n = state.requirement_count
        histogram = [0] * (1 << n)
        for (gid, _), mask in masks.items():
            if gid == group_id:
                histogram[mask] += 1
        neighbors = [
            sum(count for mask, count in enumerate(histogram) if mask & subset)
            for subset in range(1 << n)
        ]
        deficiencies = [
            subset.bit_count() - neighbors[subset] for subset in range(1 << n)
        ]
        expected = (
            n,
            histogram,
            neighbors,
            deficiencies,
            max(deficiencies),
            n - max(deficiencies),
            sum(histogram),
        )
        relation = "working" if epoch != db.base.epoch_id else "current"
        parameters = (group_id, epoch) if relation == "working" else (group_id,)
        row = db.connection.execute(
            "SELECT requirement_count,mask_histogram,neighbor_counts,deficiencies,"
            "maximum_deficiency,matching_size,distinct_hash_count "
            f"FROM groundloop_m5_matching_hall_{relation} "
            "WHERE group_version_id=%s"
            + (" AND epoch_id=%s" if relation == "working" else ""),
            parameters,
        ).fetchone()
        if row is None and relation == "working":
            row = db.connection.execute(
                "SELECT requirement_count,mask_histogram,neighbor_counts,deficiencies,"
                "maximum_deficiency,matching_size,distinct_hash_count "
                "FROM groundloop_m5_matching_hall_current WHERE group_version_id=%s",
                (group_id,),
            ).fetchone()
        assert row == expected
        assert {
            r["subset_mask"]: (r["neighbor_count"], r["deficiency"])
            for r in independent_sql["hall"]
            if r["group_version_id"] == group_id
        } == {s: (neighbors[s], deficiencies[s]) for s in range(1, 1 << n)}
    return repo


def _reference_outputs(
    before: M5Repository, after: M5Repository, event_id: str, epoch: int
) -> tuple[tuple[str, str, object], ...]:
    """Construct full output from independent exhaustive states/certificates."""
    prior = compute_reference_states(before)
    result = compute_reference_states(after)
    before_hashes, after_hashes = (
        _reference_artifacts(before),
        _reference_artifacts(after),
    )
    records: list[tuple[str, str, object]] = []
    for family, kind in (
        ("requirements", "requirement_state"),
        ("groups", "group_state"),
        ("claims", "claim_state"),
        ("answers", "answer_state"),
    ):
        values = getattr(result, family)
        for key, value in values.items():
            if before_hashes.get((kind, key)) != after_hashes.get((kind, key)):
                records.append((kind, key, value))
    groups = {
        gid: build_reference_group_certificate(after, gid) for gid in result.groups
    }
    for gid, certificate in groups.items():
        if before_hashes.get(("group_certificate", gid)) != after_hashes.get(
            ("group_certificate", gid)
        ):
            records.append(("group_certificate", gid, certificate))
        if (
            before_hashes["group_state", gid] != after_hashes["group_state", gid]
            and certificate is not None
        ):
            records.append(
                (
                    "group_binding",
                    gid,
                    WorkingGroupCertificateBinding(
                        epoch, gid, 1, None, certificate.certificate_digest
                    ),
                )
            )
    present_groups = {key: value for key, value in groups.items() if value is not None}
    for cid in result.claims:
        certificate = build_reference_claim_certificate(after, cid, present_groups)
        if (
            before_hashes["claim_certificate", cid]
            != after_hashes["claim_certificate", cid]
        ):
            records.append(("claim_certificate", cid, certificate))
        if before_hashes["claim_state", cid] != after_hashes["claim_state", cid]:
            records.append(
                (
                    "claim_binding",
                    cid,
                    WorkingClaimCertificateBinding(
                        epoch, cid, 1, None, certificate.certificate_digest
                    ),
                )
            )
    for family, kind in (("claims", "claim"), ("answers", "answer")):
        for key, state in getattr(result, family).items():
            old = getattr(prior, family)[key]
            if old.status is not state.status:
                records.append(
                    (
                        "status_delta",
                        key,
                        StatusDelta(
                            event_id,
                            kind,
                            key,
                            old.status.value,
                            state.status.value,
                            f"event={event_id} op=DeleteDocumentVersionEvent",
                        ),
                    )
                )
    order = {
        kind: rank
        for rank, kind in enumerate(
            (
                "requirement_state",
                "group_state",
                "claim_state",
                "answer_state",
                "group_certificate",
                "claim_certificate",
                "group_binding",
                "claim_binding",
                "status_delta",
            )
        )
    }
    return tuple(sorted(records, key=lambda row: (order[row[0]], row[1])))


def _independent_work_vector(
    before: M5Repository,
    after: M5Repository,
    changes: list[Any],
    outputs: tuple[Any, ...],
) -> tuple[int, ...]:
    """Count fixture primitives from exhaustive witnesses, not native work."""

    def indexes(repo: M5Repository) -> tuple[Any, Any, Any]:
        observations, edges, masks = {}, {}, {}
        for witness in derive_active_requirement_witnesses(repo):
            requirement = repo.requirement(witness.requirement_version_id)
            key = witness.requirement_version_id, witness.text_hash
            edges[key] = len(witness.active_observation_ids)
            group_key = requirement.group_version_id, witness.text_hash
            masks[group_key] = (
                masks.get(group_key, 0) | 1 << witness.requirement_ordinal
            )
            for identity in witness.active_observation_ids:
                observations[identity] = key
        return observations, edges, masks

    old_obs, old_edges, old_masks = indexes(before)
    new_obs, new_edges, new_masks = indexes(after)
    old_states, new_states = (
        compute_reference_states(before),
        compute_reference_states(after),
    )
    values = dict.fromkeys(runtime_contracts.MATCHING_WORK_COUNTER_NAMES, 0)
    removed = set(old_obs) - set(new_obs)
    values["contribution_removals"] = len(removed)
    values["requirement_observation_changes_processed"] = len(removed)
    values["edge_refcount_keys_updated"] = sum(
        old_edges.get(key, 0) != new_edges.get(key, 0)
        for key in set(old_edges) | set(new_edges)
    )
    transitions = [
        (key, old_masks.get(key, 0), new_masks.get(key, 0))
        for key in set(old_masks) | set(new_masks)
        if old_masks.get(key, 0) != new_masks.get(key, 0)
    ]
    values["ordered_index_operations"] = 3 * len(removed) + sum(
        bool(a) + bool(b) for _, a, b in transitions
    )
    values["distinct_edge_crossings"] = sum(
        (a ^ b).bit_count() for _, a, b in transitions
    )
    values["hash_mask_transitions"] = len(transitions)
    hall_groups = set()
    for (group_id, _), old, new in transitions:
        hall_groups.add(group_id)
        n = new_states.groups[group_id].requirement_count
        values["hall_subset_entries_examined"] += (1 << n) - 1
        values["hall_deficiency_entries_examined"] += (1 << n) - 1
        values["hall_neighbor_entries_changed"] += sum(
            bool(old & subset) != bool(new & subset) for subset in range(1, 1 << n)
        )
    values["group_local_state_operations"] = len(hall_groups) + sum(
        kind == "group_binding" for kind, _, _ in outputs
    )
    changed = {(kind, key) for kind, key, _, _ in changes}
    for kind, family in (
        ("requirement", "requirements"),
        ("group", "groups"),
        ("claim", "claims"),
    ):
        old, new = getattr(old_states, family), getattr(new_states, family)
        attribute = {
            "requirement": "satisfied",
            "group": "complete",
            "claim": "status",
        }[kind]
        values[f"{kind}_state_only_changes"] = sum(
            (f"{kind}_state", key) in changed
            and getattr(old[key], attribute) == getattr(new[key], attribute)
            for key in new
        )
    for kind in ("group", "claim"):
        state_keys = {key for family, key in changed if family == f"{kind}_state"}
        cert_keys = {key for family, key in changed if family == f"{kind}_certificate"}
        values[f"{kind}s_touched"] = len(state_keys | cert_keys)
        values[f"{kind}_certificate_only_changes"] = len(cert_keys - state_keys)
    values["answers_touched"] = sum(kind == "answer_state" for kind, _ in changed)
    for kind, _, output in outputs:
        if kind == "status_delta":
            values[f"{output.object_type}_status_changes"] += 1
            values["public_status_deltas"] += 1
        if kind == "group_certificate" and output is not None:
            # Exact fixtures have two singleton masks and distinct witnesses.
            # Every augment visits its sole edge once, without native kernel.
            n = len(output.rows)
            assert n == 2
            masks = {
                h: m for (g, h), m in new_masks.items() if g == output.group_version_id
            }
            assert sorted(masks.values()) == [1, 2]
            for name in (
                "canonical_sort_items",
                "augmenting_searches",
                "augmenting_requirement_visits",
                "augmenting_edge_visits",
                "representative_hashes_read",
                "representative_observations_read",
            ):
                values[name] += 2
            values["certificate_reconstructions"] += 1
            values["ordered_index_operations"] += 4
            values["certificate_digest_input_bytes"] += len(
                digests.stable_m5_preimage(
                    "m5-group-certificate-v1",
                    base_digests.text_field(output.decision_policy_version),
                    base_digests.text_field(output.group_version_id),
                    base_digests.int_field(n),
                    base_digests.sequence_field(
                        base_digests.sequence_field(
                            (
                                base_digests.int_field(row.requirement_ordinal),
                                base_digests.text_field(row.requirement_version_id),
                                base_digests.hash_field(row.text_hash),
                                base_digests.text_field(row.selected_observation_id),
                            )
                        )
                        for row in output.rows
                    ),
                )
            )
    values["output_bytes"] = len(digests.logical_output_preimage(outputs))
    return tuple(values.values())


def _assert_physical_patch(db: OpenDatabase, epoch: int) -> None:
    """Rebuild every physical child from raw independently checked row images."""
    for family, relation, type_prefix, keys in (
        ("observation", "observation", "Observation", ("observation_id",)),
        ("edge", "edge", "Edge", ("requirement_version_id", "text_hash")),
        ("mask", "hash_mask", "Mask", ("group_version_id", "text_hash")),
        ("hall", "hall", "Hall", ("group_version_id",)),
    ):
        current = db.connection.execute(
            f"SELECT to_jsonb(t) FROM groundloop_m5_matching_{relation}_current t"
        ).fetchall()
        before = {tuple(row[0][k] for k in keys): row[0] for row in current}
        working = db.connection.execute(
            f"SELECT to_jsonb(t) FROM groundloop_m5_matching_{relation}_working t "
            "WHERE epoch_id=%s",
            (epoch,),
        ).fetchall()
        expected = []
        for (row,) in sorted(working, key=lambda r: tuple(r[0][k] for k in keys)):
            key = tuple(row[k] for k in keys)
            points = []
            for layer, value in (("Current", before.get(key)), ("Working", row)):
                if value is None:
                    points.append(None)
                    continue
                value = dict(value)
                value["layer"] = runtime_contracts.M5MatchingLayer(layer.lower())
                for name in ("mask_histogram", "neighbor_counts", "deficiencies"):
                    if value.get(name) is not None:
                        value[name] = tuple(value[name])
                points.append(
                    getattr(runtime_contracts, f"M5Matching{type_prefix}{layer}")(
                        **value
                    )
                )
            fields = tuple(
                (
                    base_digests.hash_field
                    if name == "text_hash"
                    else base_digests.text_field
                )(v)
                for name, v in zip(keys, key, strict=True)
            )
            preimage = digests.matching_change_preimage(
                f"m5-persisted-matching-{family}-change-v1", fields, *points
            )
            expected.append((hashlib.sha256(preimage).hexdigest(), preimage))
        stored = db.connection.execute(
            f"SELECT {family}_change_digests,{family}_change_preimages "
            "FROM groundloop_m5_matching_patch_artifact WHERE resulting_epoch_id=%s",
            (epoch,),
        ).fetchone()
        assert tuple(zip(stored[0], map(bytes, stored[1]), strict=True)) == tuple(
            expected
        )


@pytest.mark.parametrize(
    "open_db",
    [
        False,
        True,
        "complete-changed",
        "complete-unchanged",
        "complete-reused",
        "partial",
    ],
    indirect=True,
)
def test_complete_independent_before_and_working_oracles(open_db: OpenDatabase) -> None:
    db = open_db
    db.store._activate_document_open_prerequisite(db.request)
    before_repo = _assert_complete_point(db, db.base.epoch_id)
    before_artifacts = _reference_artifacts(before_repo)
    plan = db.plan()
    receipt = _open(db, plan)
    after_repo = _assert_complete_point(db, receipt.epoch_id)
    after_artifacts = _reference_artifacts(after_repo)
    expected_changes = sorted(
        (
            kind,
            identity,
            before_artifacts.get((kind, identity)),
            after_artifacts.get((kind, identity)),
        )
        for kind, identity in set(before_artifacts) | set(after_artifacts)
        if before_artifacts.get((kind, identity))
        != after_artifacts.get((kind, identity))
    )
    rows = db.connection.execute(
        "SELECT logical_overlay_patch_preimage,logical_output_preimage,patch_digest,"
        "matching_work_digest FROM groundloop_m5_matching_patch_artifact "
        "WHERE resulting_epoch_id=%s",
        (receipt.epoch_id,),
    ).fetchall()
    assert len(rows) == 1
    with db.connection.transaction(), db.connection.cursor() as cursor:
        nodes = cursor.execute(
            "SELECT groundloop_m5_matching_parse_typed_preimage(%s,%s)",
            (rows[0][0], "m5-persisted-logical-overlay-patch-v1"),
        ).fetchone()[0]
        logical_nodes = nodes["children"]
        changes = []
        for node in logical_nodes[0]["children"]:
            children = node["children"]
            changes.append(
                (
                    matching_store._node_value(children[0], tag="enum"),
                    matching_store._node_value(children[1], tag="text"),
                    matching_store._optional_hash_node(children[2]),
                    matching_store._optional_hash_node(children[3]),
                )
            )
    assert sorted(changes) == expected_changes
    expected_outputs = _reference_outputs(
        before_repo, after_repo, plan.structural_event_id, receipt.epoch_id
    )
    output_preimage = digests.logical_output_preimage(expected_outputs)
    assert bytes(rows[0][1]) == output_preimage
    binding_digests = tuple(
        digests.certificate_binding_row_digest(
            "group" if kind == "group_binding" else "claim",
            receipt.epoch_id,
            key,
            value.valid_from_revision,
            value.valid_to_revision,
            value.certificate_digest,
        )
        for kind, key, value in expected_outputs
        if kind.endswith("_binding")
    )
    logical_preimage = digests.logical_overlay_patch_preimage(
        expected_changes,
        binding_digests,
        hashlib.sha256(output_preimage).hexdigest(),
        len(output_preimage),
    )
    assert bytes(rows[0][0]) == logical_preimage
    stored_deltas = db.connection.execute(
        "SELECT event_id,object_type,object_id,old_status,new_status,reason "
        "FROM groundloop_status_delta WHERE epoch_id=%s ORDER BY object_type,object_id",
        (receipt.epoch_id,),
    ).fetchall()
    assert stored_deltas == sorted(
        [
            (
                value.event_id,
                value.object_type,
                value.object_id,
                value.old_status,
                value.new_status,
                value.reason,
            )
            for kind, _, value in expected_outputs
            if kind == "status_delta"
        ],
        key=lambda row: (row[1], row[2]),
    )
    assert db.connection.execute(
        "SELECT valid_to_epoch FROM groundloop_document_version "
        "WHERE document_version_id='o33-document-v1'"
    ).fetchone() == (receipt.epoch_id,)
    assert db.connection.execute(
        "SELECT chunk_version_id,valid_to_epoch FROM groundloop_chunk_version "
        "WHERE document_version_id='o33-document-v1' ORDER BY chunk_version_id"
    ).fetchall() == [(key, receipt.epoch_id) for key in db.base.chunk_ids]
    for suffix, snapshot, digest_name in (
        (
            "requirement_registry",
            plan.requirement_registry_snapshot,
            "requirement_registry_snapshot_digest",
        ),
        ("active_chunk", plan.active_chunk_snapshot, "active_chunk_snapshot_digest"),
    ):
        assert db.connection.execute(
            sql.SQL("SELECT count(*) FROM {} WHERE {}=%s").format(
                sql.Identifier(f"groundloop_m5_{suffix}_snapshot_member"),
                sql.Identifier(digest_name),
            ),
            (getattr(snapshot, digest_name),),
        ).fetchone() == (len(snapshot.entries),)
    _assert_physical_patch(db, receipt.epoch_id)
    complete_loss = (
        db.group is not None
        and not db.remaining_chunks
        and len(before_repo.export_snapshot().observations) == 2
    )
    expected_values = _independent_work_vector(
        before_repo, after_repo, expected_changes, expected_outputs
    )
    expected_digest = digests.matching_work_digest(expected_values)
    for table in ("contribution", "accumulator"):
        row = db.connection.execute(
            sql.SQL("SELECT {},matching_work_digest FROM {} WHERE epoch_id=%s").format(
                sql.SQL(",").join(
                    map(sql.Identifier, runtime_contracts.MATCHING_WORK_COUNTER_NAMES)
                ),
                sql.Identifier(f"groundloop_m5_matching_work_{table}"),
            ),
            (receipt.epoch_id,),
        ).fetchone()
        assert row == (*expected_values, expected_digest)
    assert rows[0][3] == expected_digest
    assert db.connection.execute(
        "SELECT epoch_id,base_epoch_id,base_revision,decision_policy_version,"
        "updated_revision FROM groundloop_m5_matching_image_working WHERE epoch_id=%s",
        (receipt.epoch_id,),
    ).fetchone() == (receipt.epoch_id, db.base.epoch_id, 1, db.base.policy_version, 1)
    contribution = db.connection.execute(
        "SELECT source_kind,source_id,source_identity_hash,before_epoch_id,"
        "before_revision,"
        "resulting_revision,patch_digest,matching_work_digest,contribution_digest "
        "FROM groundloop_m5_matching_work_contribution WHERE epoch_id=%s",
        (receipt.epoch_id,),
    ).fetchall()
    assert len(contribution) == 1
    assert contribution[0][:7] == (
        "structural_open",
        plan.structural_event_id,
        plan.payload_hash,
        db.base.epoch_id,
        1,
        1,
        rows[0][2],
    )
    assert contribution[0][8] == digests.matching_work_contribution_digest(
        epoch_id=receipt.epoch_id,
        source_kind="structural_open",
        source_id=plan.structural_event_id,
        source_identity_hash=plan.payload_hash,
        before_epoch_id=db.base.epoch_id,
        before_revision=1,
        resulting_revision=1,
        patch_digest=rows[0][2],
        matching_work_digest_value=rows[0][3],
    )
    assert db.connection.execute(
        "SELECT epoch_id FROM groundloop_m5_publication_head"
    ).fetchone() == (db.base.epoch_id,)
    assert db.connection.execute(
        "SELECT epoch_id FROM groundloop_m4_publication_head"
    ).fetchone() == (db.base.epoch_id,)
    assert db.connection.execute(
        "SELECT revision,lifecycle_state,default_evaluation_state,"
        "confirmed_as_of_epoch,"
        "open_discovery_scope_count FROM groundloop_m4_evaluation_epoch_counter "
        "WHERE epoch_id=%s",
        (receipt.epoch_id,),
    ).fetchone() == (1, "active", "complete", db.base.epoch_id, 0)
    if complete_loss:
        old = before_artifacts["group_certificate", db.group.group_version_id]
        assert db.connection.execute(
            "SELECT groundloop_m5_group_certificate_valid_at(%s,%s,1)",
            (old, receipt.epoch_id),
        ).fetchone() == (False,)
        assert db.connection.execute(
            "SELECT count(*) FROM groundloop_m5_working_group_certificate_binding "
            "WHERE epoch_id=%s",
            (receipt.epoch_id,),
        ).fetchone() == (0,)


@pytest.mark.parametrize(
    "open_db",
    [
        False,
        True,
        "complete-changed",
        "complete-unchanged",
        "complete-reused",
        "partial",
    ],
    indirect=True,
    ids=[
        "rootless",
        "loss",
        "new-certificate",
        "unchanged-certificate",
        "reused-artifacts",
        "already-incomplete",
    ],
)
def test_native_producer_bytes_independently_match_durable_work(
    open_db: OpenDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Observe native first producers; never use the local ledger as oracle.

    Test-only spies wrap unchanged native recipes. Validation, DTO checking,
    retained hydration, diagnostic sizing and explicitly neutral D29 private
    locator/reservation work are excluded by their real call boundaries.
    """
    db = open_db
    db.store._activate_document_open_prerequisite(db.request)

    def retained_image() -> Any:
        return tuple(
            (
                table,
                db.connection.execute(
                    sql.SQL(
                        "SELECT to_jsonb(t),xmin::text FROM {} t "
                        "WHERE certificate_digest=ANY(%s) ORDER BY to_jsonb(t)::text"
                    ).format(sql.Identifier(table)),
                    (list(db.retained_fixture_certificates),),
                ).fetchall(),
            )
            for table in (
                "groundloop_m5_group_certificate_artifact",
                "groundloop_m5_group_certificate_artifact_row",
                "groundloop_m5_claim_certificate_artifact",
            )
        )

    immutable_before = retained_image()
    plan = db.plan()
    direct, required, roots, root_hash = db.proposals(plan)
    sizes: Counter[str] = Counter()
    executions: Counter[str] = Counter()
    original_digest = base_digests.stable_m5_digest

    def context() -> tuple[list[Any], list[str]]:
        frames = inspect.stack(context=0)
        return frames, [frame.function for frame in frames]

    def native_digest(domain: str, frames: list[Any], names: list[str]) -> bool:
        if "_validate_prepared_matching_write_plan" in names:
            return False
        if "_gather_d29_locator_authority" in names:
            return False
        constructors = [
            f.frame.f_locals.get("self")
            for f in frames
            if f.function == "__post_init__"
        ]
        final_work = (
            domain == "m5-runtime-work-v2"
            and any(
                type(value) is M5RuntimeWork and value.work_digest == ""
                for value in constructors
            )
            and "_o33_complete_structural_work" in names
        )
        if final_work:
            return True
        if any(name.startswith("_o33_") for name in names):
            return False
        if constructors:
            first_group = (
                domain == "m5-group-certificate-v1"
                and any(
                    type(value) is m5_domain.GroupMatchingCertificateArtifact
                    and value.certificate_digest == ""
                    for value in constructors
                )
                and "_document_withdrawal_preview" in names
            )
            first_claim = (
                domain == "m5-claim-certificate-v2"
                and any(
                    type(value) is m5_domain.ClaimCertificateArtifact
                    and value.certificate_digest == ""
                    for value in constructors
                )
                and "_document_claim_artifact" in names
            )
            return first_group or first_claim
        if "validate_manifest_and_scope" in names:
            return False
        if domain == "m5-requirement-root-set-v2":
            return frames[2].function == "_continue_locked_document_open"
        if domain == "m5-requirement-withdrawal-plan-v2":
            return (
                "plan_requirement_withdrawal" in names
                and "_continue_locked_document_open" in names
            )
        if domain in {
            "m5-candidate-policy-v2",
            "m5-text-normalizer-provenance-v1",
            "m5-forward-requirement-retrieval-execution-v2",
            "m5-job-payload-v2",
            "m5-logical-job-v2",
            "m5-discovery-scope-contract-v2",
        }:
            return (
                "build" in names
                and "_document_requirement_roots" in names
                and "_continue_locked_document_open" in names
            )
        if domain == "m5-requirement-root-provenance-v1":
            return "build" in names and "persist_structural_open_identity" in names
        if domain == "m5-runtime-work-contribution-key-v1":
            return "build" in names and "persist_structural_open_accounting" in names
        if domain == "m5-persisted-matching-transition-intent-v1":
            return "_derive_matching_transition_intent_core" in names
        if domain == "m5-matching-work-contribution-v1":
            return "_matching_contribution" in names
        if domain == "m5-matching-work-v1":
            return any(
                name in names
                for name in (
                    "_build_patch_artifact",
                    "_insert_contribution",
                    "_insert_or_advance_matching_accumulator",
                )
            )
        return "_document_first_application_plan" in names and domain.startswith(
            (
                "m5-persisted-matching-",
                "m5-persisted-logical-overlay-",
                "m5-requirement-state-",
                "m5-group-state-",
                "m5-claim-state-",
                "m5-answer-state-",
                "m5-persisted-certificate-binding-",
            )
        )

    def digest_spy(domain: str, *fields: tuple[str, ...]) -> str:
        frames, names = context()
        # context() -> digest_spy -> frozen recipe -> native caller.
        if native_digest(domain, frames[1:], names[1:]):
            size = len(digests.stable_m5_preimage(domain, *fields))
            sizes["hashed"] += size
            sizes["serialized"] += size
            executions[domain] += 1
        return original_digest(domain, *fields)

    monkeypatch.setattr(base_digests, "stable_m5_digest", digest_spy)
    monkeypatch.setattr(digests, "stable_m5_digest", digest_spy)
    monkeypatch.setattr(m5_domain, "stable_m5_digest", digest_spy)
    for name in (
        "matching_change_preimage",
        "matching_group_shape_set_preimage",
        "logical_overlay_patch_preimage",
        "logical_output_preimage",
        "persisted_matching_patch_preimage",
    ):
        original = getattr(digests, name)

        def preimage_spy(
            *args: Any, _original: Any = original, _name: str = name, **kwargs: Any
        ) -> bytes:
            result = _original(*args, **kwargs)
            frames, names = context()
            if (
                "_validate_prepared_matching_write_plan" not in names
                and "__post_init__" not in names
                and not any(n.startswith("_o33_") for n in names)
                and any(
                    name in names
                    for name in (
                        "_build_patch_artifact",
                        "_document_first_application_plan",
                    )
                )
            ):
                sizes["serialized"] += len(result)
                executions[_name] += 1
                if (
                    _name == "logical_output_preimage"
                    and frames[2].function == "_build_patch_artifact"
                ):
                    sizes["hashed"] += len(result)
            return result

        monkeypatch.setattr(digests, name, preimage_spy)
    original_absence = matching_store._matching_absence_lock_key

    def absence_spy(relation: str, key: Any) -> int:
        result = original_absence(relation, key)
        if inspect.currentframe().f_back.f_code.co_name == "_reserve_matching_absence":
            size = len(b"m5-d28-matching-absence-v1") + sum(
                8 + len(str(value).encode("utf-8")) for value in (relation, *key)
            )
            sizes["hashed"] += size
            sizes["serialized"] += size
            executions["d25.native_absence"] += 1
        return result

    monkeypatch.setattr(matching_store, "_matching_absence_lock_key", absence_spy)
    original_m4 = evaluation_overlay._sha256_json

    def m4_spy(payload: Any) -> str:
        result = original_m4(payload)
        _, names = context()
        if "declare_epoch_local" in names:
            import json

            size = len(
                json.dumps(
                    payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
                ).encode("utf-8")
            )
            sizes["hashed"] += size
            sizes["serialized"] += size
            executions["m4.native_declaration"] += 1
        return result

    monkeypatch.setattr(evaluation_overlay, "_sha256_json", m4_spy)
    control: dict[str, Any] = {"records": [], "count": 0, "target": None}
    original_factory = db.connection.cursor_factory
    db.connection.cursor_factory = _mutation_cursor(db.connection, control)
    try:
        receipt = db.store._open_document_open_prerequisite(
            plan,
            ports=db.ports,
            execution_policy=db.execution,
            supplied_direct_open=direct,
            supplied_requirement_withdrawal=required,
            requirement_roots=roots,
            requirement_root_set_hash=root_hash,
            recovery_operational_config=M5RuntimeOperationalConfig.build(60_000),
            recovery_root_fallback_required={r.job.logical_job_id: True for r in roots},
        )
    finally:
        db.connection.cursor_factory = original_factory
    writes: Counter[str] = Counter()
    for statement, rowcount in control["records"]:
        matched = re.match(r'(?:INSERT\s+INTO|UPDATE)\s+"?(\w+)"?', statement, re.I)
        if matched:
            writes[matched.group(1)] += rowcount
    actual_counts = db.connection.execute(
        "SELECT group_state_write_count,claim_state_write_count,"
        "answer_state_write_count,"
        "certificate_binding_write_count,public_delta_count "
        "FROM groundloop_m5_runtime_work_contribution WHERE epoch_id=%s "
        "AND contribution_kind='structural_open'",
        (receipt.epoch_id,),
    ).fetchone()
    assert actual_counts == (
        writes["groundloop_m5_working_group_state"],
        writes["groundloop_m5_working_claim_state"]
        + writes["groundloop_m4_working_claim_state"],
        writes["groundloop_m5_working_answer_state"]
        + writes["groundloop_m4_working_answer_state"],
        writes["groundloop_m5_working_group_certificate_binding"]
        + writes["groundloop_m5_working_claim_certificate_binding"],
        0,  # Provisional status rows are not seal-owned public-delta work.
    )
    assert "requirement_state_write_count" not in M5RuntimeWork.counter_names()
    assert writes["groundloop_m5_runtime_work_contribution"] == 1
    assert writes["groundloop_m5_runtime_work_accumulator"] == 1
    assert writes["groundloop_m5_runtime_timing_accumulator"] == 1
    assert writes["groundloop_m5_matching_work_contribution"] == 1
    assert writes["groundloop_m5_matching_work_accumulator"] == 1
    if db.retained_fixture_certificates:
        assert retained_image() == immutable_before
        assert writes["groundloop_m5_group_certificate_artifact"] == 0
        assert writes["groundloop_m5_group_certificate_artifact_row"] == 0
        assert writes["groundloop_m5_claim_certificate_artifact"] == 0
    stored = db.connection.execute(
        "SELECT bytes_hashed,bytes_serialized "
        "FROM groundloop_m5_runtime_work_contribution "
        "WHERE epoch_id=%s AND contribution_kind='structural_open'",
        (receipt.epoch_id,),
    ).fetchone()
    assert stored == (sizes["hashed"], sizes["serialized"]), dict(executions)
    assert executions["m5-persisted-matching-transition-intent-v1"] == 1
    assert executions["m5-matching-work-v1"] == 4
    assert executions["m5-runtime-work-v2"] == 1
    assert executions["m4.native_declaration"] == 1
    if db.remaining_chunks:
        assert executions["m5-group-certificate-v1"] == 3
        assert executions["m5-claim-certificate-v2"] == 2
        state = db.connection.execute(
            "SELECT complete FROM groundloop_m5_working_group_state WHERE epoch_id=%s",
            (receipt.epoch_id,),
        ).fetchone()
        # Both variants stay complete; an unchanged certificate may need no
        # group row. Distinguish the actual accepted branch via raw source IDs.
        assert state is None or state == (True,)
        first = db.connection.execute(
            "SELECT min(observation_id) FROM groundloop_semantic_observation "
            "WHERE chunk_version_id LIKE 'o33-alternate-chunk-%%'"
        ).fetchone()[0]
        writes = db.connection.execute(
            "SELECT count(*) FROM groundloop_m5_working_group_certificate_binding "
            "WHERE epoch_id=%s",
            (receipt.epoch_id,),
        ).fetchone()[0]
        assert writes == (0 if first.startswith("a-") else 1)
