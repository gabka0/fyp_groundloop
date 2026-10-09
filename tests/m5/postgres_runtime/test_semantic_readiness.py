"""Genuine readiness-only histories; no public C1 or matching-seal claim."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any

import psycopg
import pytest
from psycopg import Connection, Cursor, IsolationLevel, sql

from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    M5CancellationPlan,
    M5RunFailureReason,
    M5RuntimeTiming,
    M5RuntimeTimingCoverage,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TerminalReason,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from groundloop.m5.runtime.postgres_readiness import (
    _advance_semantic_readiness_atomically,
)
from groundloop.postgres.migrations import (
    M5SemanticReadinessBundleError,
    _m5_readiness_catalog_fingerprint,
    apply_legacy_migrations,
    install_m5_bounded_document_withdrawal_bundle,
    install_m5_core_bundle,
    install_m5_persisted_matching_bundle,
    install_m5_preterminal_seal_context_bundle,
    install_m5_runtime_bundle,
    install_m5_runtime_recovery_bundle,
    install_m5_semantic_readiness_bundle,
)
from tests.m5.postgres.helpers import (
    insert_published_group,
    install_test_activation_barrier,
    make_group,
    seed_base,
)
from tests.m5.postgres_runtime.conftest import (
    M5RuntimeDatabase,
    _database_url,
    _manifest,
    _sha,
)
from tests.m5.postgres_runtime.test_direct_m4_composition import (
    _open_direct_epoch,
)
from tests.m5.postgres_runtime.test_direct_m4_composition import (
    direct_runtime_db as direct_runtime_db,
)
from tests.m5.postgres_runtime.test_migration_017 import _seed_b3_direct_m4_snapshot
from tests.m5.postgres_runtime.test_root_transitions import (
    _acquire,
    _open_recovery_event,
    _result,
    _root_set_hash,
    _roots,
    _stage,
)


@pytest.fixture
def readiness_db(m5_runtime_db: M5RuntimeDatabase) -> Iterator[M5RuntimeDatabase]:
    db = m5_runtime_db
    row = db.connection.execute(
        "SELECT revision FROM groundloop_epoch WHERE epoch_id=%s", (db.base.epoch_id,)
    ).fetchone()
    assert row is not None
    _seed_b3_direct_m4_snapshot(
        db.connection, epoch_id=db.base.epoch_id, revision=row[0]
    )
    db.connection.commit()
    for installer in (
        install_m5_persisted_matching_bundle,
        install_m5_bounded_document_withdrawal_bundle,
        install_m5_preterminal_seal_context_bundle,
        install_m5_semantic_readiness_bundle,
    ):
        assert installer(db.connection).applied
    yield db


def _edge(
    connection: Connection[Any],
    epoch: int,
    event: str,
    target: str,
    rev: int,
    **kwargs: Any,
) -> Any:
    return _advance_semantic_readiness_atomically(
        connection,
        epoch_id=epoch,
        structural_event_id=event,
        target=target,
        expected_revision=rev,
        **kwargs,
    )


def _point(connection: Connection[Any], epoch: int) -> tuple[Any, ...]:
    row = connection.execute(
        "SELECT b.revision,b.semantic_status,b.evaluation_state,r.runtime_state,"
        "r.revision,r.open_work_count,r.open_scope_count,r.blocking_failure_count "
        "FROM groundloop_epoch b JOIN groundloop_m5_runtime_epoch r USING(epoch_id) "
        "WHERE epoch_id=%s",
        (epoch,),
    ).fetchone()
    connection.commit()
    assert row is not None
    return row


def _snapshot(connection: Connection[Any], schema: str) -> tuple[Any, ...]:
    """All fixture tables plus xmin: same-value rewrites are also observable.

    JSON is display-only snapshot serialization, never a GroundLoop identity.
    """
    values = []
    names = connection.execute(
        "SELECT tablename FROM pg_catalog.pg_tables "
        "WHERE schemaname OPERATOR(pg_catalog.=) %s "
        'ORDER BY tablename COLLATE pg_catalog."C"',
        (schema,),
    ).fetchall()
    assert names
    for (name,) in names:
        rows = connection.execute(
            sql.SQL(
                "SELECT xmin::pg_catalog.text,"
                "pg_catalog.row_to_json(t)::pg_catalog.text "
                "FROM {} t ORDER BY pg_catalog.row_to_json(t)::pg_catalog.text "
                'COLLATE pg_catalog."C",xmin::pg_catalog.text'
            ).format(sql.Identifier(schema, name))
        ).fetchall()
        values.append((name, rows))
    connection.commit()
    return tuple(values)


def test_retire_two_distinct_outer_commits_and_historical_replay(
    readiness_db: M5RuntimeDatabase,
) -> None:
    db = readiness_db
    plan = db.retire_plan(event_id="d32-runtime-retire")
    opened = _open_recovery_event(PostgresM5RuntimeStore(db.connection), db, plan)
    assert _point(db.connection, opened.epoch_id) == (
        1,
        "pending",
        "pending",
        "structural_committed",
        1,
        0,
        0,
        0,
    )
    pending = _edge(
        db.connection, opened.epoch_id, plan.structural_event_id, "semantic_pending", 1
    )
    assert not pending.replay
    with db.reconnect() as observer:
        assert _point(observer, opened.epoch_id) == (
            2,
            "pending",
            "pending",
            "semantic_pending",
            2,
            0,
            0,
            0,
        )
    complete = _edge(
        db.connection, opened.epoch_id, plan.structural_event_id, "semantic_complete", 2
    )
    assert not complete.replay
    assert _point(db.connection, opened.epoch_id) == (
        3,
        "complete",
        "complete",
        "semantic_complete",
        3,
        0,
        0,
        0,
    )
    before = _snapshot(db.connection, db.schema_name)
    assert (
        _edge(
            db.connection,
            opened.epoch_id,
            plan.structural_event_id,
            "semantic_pending",
            1,
        ).anchor
        == pending.anchor
    )
    assert (
        _edge(
            db.connection,
            opened.epoch_id,
            plan.structural_event_id,
            "semantic_complete",
            2,
        ).anchor
        == complete.anchor
    )
    assert _snapshot(db.connection, db.schema_name) == before
    with pytest.raises(EventConflictError):
        _edge(
            db.connection,
            opened.epoch_id,
            plan.structural_event_id,
            "semantic_complete",
            3,
        )
    assert _snapshot(db.connection, db.schema_name) == before


def test_public_catalog_boundary_rejects_before_readiness_without_writes() -> None:
    """Real public installation is a retained catalog negative, NOT a positive.

    Retain this isolated database as a diagnostic artifact; do not move or
    replace any existing schema, table, function or user's database.
    S32's one pinned catalog encodes public extension bindings separately from
    the installation namespace. Do not mutate that pin to invent portability.
    """
    from psycopg.conninfo import make_conninfo

    database = "groundloop_d32_public_" + uuid.uuid4().hex
    source_dsn = _database_url()
    with psycopg.connect(source_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    dsn = make_conninfo(source_dsn, dbname=database)
    with psycopg.connect(dsn) as connection:
        connection.execute("SET search_path TO public")
        connection.commit()
        with connection.transaction():
            apply_legacy_migrations(connection)
        install_m5_core_bundle(connection)
        install_m5_runtime_bundle(connection)
        install_m5_runtime_recovery_bundle(connection)
        with connection.transaction():
            base = seed_base(
                connection,
                prefix="d32-public",
                claim_count=1,
                chunk_texts=("alpha", "beta"),
            )
            group = make_group(
                group_id="d32-public-group-v1",
                family_id="d32-public-family",
                claim_id=base.claim_ids[0],
                texts=("required fact",),
                source_id="d32-public-fixture",
            )
            insert_published_group(connection, group=group, epoch_id=base.epoch_id)
            connection.execute(
                "INSERT INTO groundloop_model_artifact "
                "(model_artifact_id,task,provider,model_id,immutable_revision,"
                "tokenizer_revision,license_id,config_hash) VALUES "
                "('failure-replay-embedding','embedding','fixture','fixture-embedding',"
                "'v1','v1','MIT',%s)",
                (_sha("embedding-config"),),
            )
        with connection.transaction():
            install_test_activation_barrier(
                connection, base, activation_id="d32-public-activation"
            )
        manifest = _manifest(base)
        PostgresM5RuntimeStore(connection).register_candidate_policy(manifest)
        revision = connection.execute(
            "SELECT revision FROM groundloop_epoch WHERE epoch_id=%s", (base.epoch_id,)
        ).fetchone()[0]
        _seed_b3_direct_m4_snapshot(
            connection, epoch_id=base.epoch_id, revision=revision
        )
        connection.commit()
        for installer in (
            install_m5_persisted_matching_bundle,
            install_m5_bounded_document_withdrawal_bundle,
            install_m5_preterminal_seal_context_bundle,
        ):
            assert installer(connection).applied
        effective = connection.execute(
            "SELECT pg_catalog.current_schemas(true)"
        ).fetchone()[0]
        assert [name for name in effective if not name.startswith("pg_temp_")] == [
            "pg_catalog",
            "public",
        ]
        connection.commit()
        catalog = _m5_readiness_catalog_fingerprint(connection, schema_name="public")
        connection.commit()
        before = _snapshot(connection, "public")
        with pytest.raises(M5SemanticReadinessBundleError, match="catalog"):
            install_m5_semantic_readiness_bundle(connection)
        assert _snapshot(connection, "public") == before
        assert (
            _m5_readiness_catalog_fingerprint(connection, schema_name="public")
            == catalog
        )
        connection.commit()
        with pytest.raises(M5SemanticReadinessBundleError):
            _edge(
                connection,
                base.epoch_id,
                "not-a-readiness-event",
                "semantic_pending",
                1,
            )
        assert _snapshot(connection, "public") == before


@pytest.mark.parametrize("kind", ("register", "replace"))
def test_actual_roots_must_close_before_complete(
    readiness_db: M5RuntimeDatabase,
    kind: str,
) -> None:
    db = readiness_db
    plan = getattr(db, kind + "_plan")(event_id="d32-runtime-" + kind)
    store = PostgresM5RuntimeStore(db.connection)
    opened = _open_recovery_event(store, db, plan)
    roots = _roots(plan, db.manifest)
    with pytest.raises(InvalidEventError):
        _edge(
            db.connection,
            opened.epoch_id,
            plan.structural_event_id,
            "semantic_pending",
            1,
        )
    revision = 1
    for _, job in roots:
        lease = _acquire(
            db.connection, epoch_id=opened.epoch_id, expected_revision=revision, job=job
        )
        revision += 1
        result = _result(epoch_id=opened.epoch_id, root=job, pairs=())
        _stage(
            db.connection,
            epoch_id=opened.epoch_id,
            expected_revision=revision,
            lease=lease,
            job=job,
            result=result,
        )
        revision += 1
    with pytest.raises(InvalidEventError):
        _edge(
            db.connection,
            opened.epoch_id,
            plan.structural_event_id,
            "semantic_complete",
            revision,
        )
    barrier = store.close_m5_requirement_roots_atomically(
        opened.epoch_id, revision, _root_set_hash(roots)
    )
    complete = _edge(
        db.connection,
        opened.epoch_id,
        plan.structural_event_id,
        "semantic_complete",
        barrier.resulting_revision,
    )
    assert complete.anchor.anchor_revision == barrier.resulting_revision + 1
    assert _point(db.connection, opened.epoch_id)[3] == "semantic_complete"


@contextmanager
def _runtime_login(db: M5RuntimeDatabase) -> Iterator[Connection[Any]]:
    """Separate database principal; fixture-owned role only, never SET ROLE."""
    role = "d32_runtime_" + uuid.uuid4().hex
    password = uuid.uuid4().hex
    owner = db.connection
    owner.execute(
        sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
            sql.Identifier(role), sql.Literal(password)
        )
    )
    owner.execute(
        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
            sql.Identifier(db.schema_name), sql.Identifier(role)
        )
    )
    owner.execute(
        sql.SQL("GRANT ALL ON ALL TABLES IN SCHEMA {} TO {}").format(
            sql.Identifier(db.schema_name), sql.Identifier(role)
        )
    )
    owner.execute(
        sql.SQL("GRANT USAGE ON ALL SEQUENCES IN SCHEMA {} TO {}").format(
            sql.Identifier(db.schema_name), sql.Identifier(role)
        )
    )
    owner.commit()
    try:
        with psycopg.connect(db.dsn, user=role, password=password) as connection:
            connection.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(db.schema_name)
                )
            )
            connection.commit()
            identity = connection.execute("SELECT session_user,current_user").fetchone()
            assert identity == (role, role)
            connection.commit()
            yield connection
    finally:
        owner.rollback()
        owner.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
        owner.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
        owner.commit()


@pytest.mark.parametrize("kind", ("retire", "replace"))
def test_genuine_nonowner_complete_history(
    readiness_db: M5RuntimeDatabase, kind: str
) -> None:
    db = readiness_db
    plan = getattr(db, kind + "_plan")(event_id="d32-nonowner-" + kind)
    with _runtime_login(db) as runtime:
        store = PostgresM5RuntimeStore(runtime)
        opened = _open_recovery_event(store, db, plan)
        revision = 1
        roots = _roots(plan, db.manifest)
        if not roots:
            _edge(
                runtime,
                opened.epoch_id,
                plan.structural_event_id,
                "semantic_pending",
                1,
            )
            revision = 2
        else:
            for _, job in roots:
                lease = _acquire(
                    runtime,
                    epoch_id=opened.epoch_id,
                    expected_revision=revision,
                    job=job,
                )
                revision += 1
                _stage(
                    runtime,
                    epoch_id=opened.epoch_id,
                    expected_revision=revision,
                    lease=lease,
                    job=job,
                    result=_result(epoch_id=opened.epoch_id, root=job, pairs=()),
                )
                revision += 1
            barrier = store.close_m5_requirement_roots_atomically(
                opened.epoch_id, revision, _root_set_hash(roots)
            )
            revision = barrier.resulting_revision
        receipt = _edge(
            runtime,
            opened.epoch_id,
            plan.structural_event_id,
            "semantic_complete",
            revision,
        )
        assert not receipt.replay
        assert _point(db.connection, opened.epoch_id)[0] == revision + 1
        before = _snapshot(runtime, db.schema_name)
        assert _edge(
            runtime,
            opened.epoch_id,
            plan.structural_event_id,
            "semantic_complete",
            revision,
        ).replay
        assert _snapshot(runtime, db.schema_name) == before


@pytest.mark.parametrize("observed", (False, True))
def test_exact_increment_and_prior_anchor_classification(
    readiness_db: M5RuntimeDatabase,
    observed: bool,
) -> None:
    db = readiness_db
    plan = db.retire_plan(event_id="d32-accounting")
    store = PostgresM5RuntimeStore(db.connection)
    opened = _open_recovery_event(store, db, plan)
    original = db.connection.execute(
        "SELECT contribution_key_digest FROM groundloop_m5_runtime_work_contribution "
        "WHERE epoch_id=%s AND contribution_kind='structural_open'",
        (opened.epoch_id,),
    ).fetchone()
    assert original is not None
    db.connection.commit()
    if observed:
        timing = M5RuntimeTiming(11, 0, 23, 0, 34, 7, 3, 90, 2)
        store.append_transition_call_timing(
            opened.epoch_id,
            M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
            plan.structural_event_id,
            original[0],
            1,
            timing,
        )
    prior = store.current_event_work(opened.epoch_id)
    pending = _edge(
        db.connection, opened.epoch_id, plan.structural_event_id, "semantic_pending", 1
    )
    complete = _edge(
        db.connection, opened.epoch_id, plan.structural_event_id, "semantic_complete", 2
    )
    rows = db.connection.execute(
        "SELECT contribution_kind,source_id,applied_revision,source_identity_hash,"
        "contribution_key_digest,"
        + ",".join(M5RuntimeWork.counter_names())
        + " FROM groundloop_m5_runtime_work_contribution WHERE epoch_id=%s "
        "AND contribution_kind='semantic_readiness' ORDER BY applied_revision",
        (opened.epoch_id,),
    ).fetchall()
    assert len(rows) == 2
    added = [0] * 32
    root_hash = digests.requirement_root_set_digest(())
    for row, receipt, predecessor in zip(
        rows,
        (pending, complete),
        ("structural_committed", "semantic_pending"),
        strict=True,
    ):
        source = digests.semantic_readiness_transition_preimage(
            epoch_id=opened.epoch_id,
            structural_event_id=plan.structural_event_id,
            event_payload_hash=plan.payload_hash,
            requirement_root_set_hash=root_hash,
            from_runtime_state=predecessor,
            to_runtime_state=receipt.anchor.source_id,
            expected_revision=receipt.anchor.anchor_revision - 1,
            resulting_revision=receipt.anchor.anchor_revision,
        )
        from tests.m5.runtime.test_semantic_readiness import _frame

        key = _frame(
            "m5-runtime-work-contribution-key-v1",
            "int",
            str(opened.epoch_id),
            "enum",
            "semantic_readiness",
            "text",
            receipt.anchor.source_id,
        )
        counts = list(row[5:])
        assert counts[:25] == [0] * 25 and counts[27:] == [0] * 5
        assert counts[25:27] == [len(source) + len(key)] * 2
        added = [a + b for a, b in zip(added, counts, strict=True)]
    point = db.connection.execute(
        "SELECT "
        + ",".join(M5RuntimeWork.counter_names())
        + " FROM groundloop_m5_runtime_work_accumulator WHERE epoch_id=%s",
        (opened.epoch_id,),
    ).fetchone()
    assert point == tuple(
        a + b for a, b in zip(prior.counter_values(), added, strict=True)
    )
    coverage = db.connection.execute(
        "SELECT required_expected_count,required_observed_count,required_missing_count,"
        "pending_contribution_kind,pending_source_id,pending_anchor_revision,"
        "coordinator_non_db_non_neural_ns,postgres_server_execution_ns "
        "FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id=%s",
        (opened.epoch_id,),
    ).fetchone()
    assert coverage == (
        3,
        int(observed),
        int(not observed) + 1,
        "semantic_readiness",
        "semantic_complete",
        3,
        11 if observed else 0,
        7 if observed else 0,
    )
    samples = db.connection.execute(
        "SELECT contribution_kind,required_interval_observed FROM "
        "groundloop_m5_transition_call_timing WHERE epoch_id=%s "
        "ORDER BY anchor_revision",
        (opened.epoch_id,),
    ).fetchall()
    assert samples == [("structural_open", observed), ("semantic_readiness", False)]
    db.connection.commit()


_CUTS = (
    "after_authority",
    "after_base_lock",
    "after_runtime_lock",
    "after_counter_locks",
    "after_accounting_locks",
    "after_pending_resolved",
    "after_contribution",
    "after_authorized_transition",
    "after_base_advance",
    "after_runtime_advance",
    "after_owner_advance",
    "after_answer_advance",
    "after_work_cas",
    "after_timing_cas",
    "after_deferred_validation",
)


@pytest.mark.parametrize(
    "target,revision", (("semantic_pending", 1), ("semantic_complete", 2))
)
def test_every_cut_rolls_back_whole_edge(
    readiness_db: M5RuntimeDatabase, target: str, revision: int
) -> None:
    db = readiness_db
    plan = db.retire_plan(event_id="d32-cuts")
    opened = _open_recovery_event(PostgresM5RuntimeStore(db.connection), db, plan)
    if revision == 2:
        _edge(
            db.connection,
            opened.epoch_id,
            plan.structural_event_id,
            "semantic_pending",
            1,
        )
    before = _snapshot(db.connection, db.schema_name)
    for boundary in _CUTS:
        reached = []

        def fail(
            point: str, *, boundary: str = boundary, reached: list[str] = reached
        ) -> None:
            reached.append(point)
            if point == boundary:
                raise RuntimeError("injected " + point)

        with pytest.raises(RuntimeError, match=boundary):
            _edge(
                db.connection,
                opened.epoch_id,
                plan.structural_event_id,
                target,
                revision,
                failure_injector=fail,
            )
        assert boundary in reached
        assert _snapshot(db.connection, db.schema_name) == before
    assert not _edge(
        db.connection, opened.epoch_id, plan.structural_event_id, target, revision
    ).replay


@pytest.mark.parametrize("state", ("open", "acquired", "staged", "cancelled"))
def test_real_job_states_block_or_allow_completion(
    readiness_db: M5RuntimeDatabase, state: str
) -> None:
    db = readiness_db
    plan = db.replace_plan(event_id="d32-job-state")
    store = PostgresM5RuntimeStore(db.connection)
    opened = _open_recovery_event(store, db, plan)
    roots = _roots(plan, db.manifest)
    revision = 1
    job = roots[0][1]
    if state in {"acquired", "staged"}:
        lease = _acquire(
            db.connection, epoch_id=opened.epoch_id, expected_revision=revision, job=job
        )
        revision += 1
        if state == "staged":
            _stage(
                db.connection,
                epoch_id=opened.epoch_id,
                expected_revision=revision,
                lease=lease,
                job=job,
                result=_result(epoch_id=opened.epoch_id, root=job, pairs=()),
            )
            revision += 1
    if state == "cancelled":
        plan_cancel = M5CancellationPlan.build(
            epoch_id=opened.epoch_id,
            structural_event_id=plan.structural_event_id,
            reason=M5TerminalReason.SCOPE_RETIRED,
            cancelled_job_ids=(job.logical_job_id,),
        )
        cancelled = store.cancel_m5_work_atomically(
            opened.epoch_id, revision, plan_cancel
        )
        revision = cancelled.resulting_revision
        assert not _edge(
            db.connection,
            opened.epoch_id,
            plan.structural_event_id,
            "semantic_complete",
            revision,
        ).replay
    else:
        before = _snapshot(db.connection, db.schema_name)
        with pytest.raises(InvalidEventError):
            _edge(
                db.connection,
                opened.epoch_id,
                plan.structural_event_id,
                "semantic_complete",
                revision,
            )
        assert _snapshot(db.connection, db.schema_name) == before


def test_idle_read_write_read_committed_required(
    readiness_db: M5RuntimeDatabase,
) -> None:
    db = readiness_db
    plan = db.retire_plan(event_id="d32-isolation")
    opened = _open_recovery_event(PostgresM5RuntimeStore(db.connection), db, plan)
    db.connection.execute("SELECT 1")
    with pytest.raises(InvalidEventError, match="idle"):
        _edge(
            db.connection,
            opened.epoch_id,
            plan.structural_event_id,
            "semantic_pending",
            1,
        )
    db.connection.rollback()
    for isolation, readonly in (
        (IsolationLevel.REPEATABLE_READ, False),
        (IsolationLevel.READ_COMMITTED, True),
    ):
        db.connection.isolation_level = isolation
        db.connection.read_only = readonly
        with pytest.raises(InvalidEventError, match="read-write"):
            _edge(
                db.connection,
                opened.epoch_id,
                plan.structural_event_id,
                "semantic_pending",
                1,
            )
    db.connection.isolation_level = IsolationLevel.READ_COMMITTED
    db.connection.read_only = False


@pytest.mark.parametrize("first", (0, 1))
def test_competing_readiness_serial_orders(
    readiness_db: M5RuntimeDatabase, first: int
) -> None:
    import threading

    db = readiness_db
    plan = db.retire_plan(event_id="d32-race")
    opened = _open_recovery_event(PostgresM5RuntimeStore(db.connection), db, plan)
    locked = threading.Event()
    release = threading.Event()
    second_started = threading.Event()

    def run(index: int) -> Any:
        with db.reconnect() as connection:

            def cut(point: str) -> None:
                if index == first and point == "after_base_lock":
                    locked.set()
                    assert release.wait(15)
                if index != first and point == "after_authority":
                    second_started.set()

            return _edge(
                connection,
                opened.epoch_id,
                plan.structural_event_id,
                "semantic_pending",
                1,
                failure_injector=cut,
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        leading = pool.submit(run, first)
        assert locked.wait(15)
        following = pool.submit(run, 1 - first)
        assert second_started.wait(15)
        release.set()
        result = leading.result(timeout=20)
        replay = following.result(timeout=20)
    assert not result.replay and replay.replay
    assert result.anchor == replay.anchor
    assert _point(db.connection, opened.epoch_id)[0] == 2


@pytest.mark.parametrize("terminal", (False, True))
def test_failed_job_is_never_readiness(
    readiness_db: M5RuntimeDatabase, terminal: bool
) -> None:
    db = readiness_db
    plan = db.replace_plan(event_id="d32-failure")
    store = PostgresM5RuntimeStore(db.connection)
    opened = _open_recovery_event(store, db, plan)
    job = _roots(plan, db.manifest)[0][1]
    lease = _acquire(
        db.connection, epoch_id=opened.epoch_id, expected_revision=1, job=job
    )
    import hashlib

    error = hashlib.sha256(b"fixture error").hexdigest()
    work = M5RuntimeWork(
        requirement_forward_retrieval_call_count=1, embedding_model_call_count=1
    )
    if terminal:
        store.mark_m5_terminal_failure(
            opened.epoch_id,
            2,
            lease,
            M5TerminalReason.RETRIEVAL_ERROR,
            error,
            work,
            None,
        )
    else:
        store.mark_m5_retryable_failure(opened.epoch_id, 2, lease, error, work, None)
    before = _snapshot(db.connection, db.schema_name)
    with pytest.raises(InvalidEventError):
        _edge(
            db.connection,
            opened.epoch_id,
            plan.structural_event_id,
            "semantic_complete",
            3,
        )
    assert _snapshot(db.connection, db.schema_name) == before


def test_replay_after_genuine_terminal_failure(readiness_db: M5RuntimeDatabase) -> None:
    db = readiness_db
    plan = db.retire_plan(event_id="d32-terminal-failure")
    store = PostgresM5RuntimeStore(db.connection)
    opened = _open_recovery_event(store, db, plan)
    pending = _edge(
        db.connection, opened.epoch_id, plan.structural_event_id, "semantic_pending", 1
    )
    complete = _edge(
        db.connection, opened.epoch_id, plan.structural_event_id, "semantic_complete", 2
    )
    store.fail_typed_epoch_atomically(
        opened.epoch_id, 3, M5RunFailureReason.INVARIANT_FAILURE, M5RuntimeWork()
    )
    before = _snapshot(db.connection, db.schema_name)
    for receipt, revision in ((pending, 1), (complete, 2)):
        replay = _edge(
            db.connection,
            opened.epoch_id,
            plan.structural_event_id,
            receipt.anchor.source_id,
            revision,
        )
        assert replay.replay and replay.anchor == receipt.anchor
    assert _snapshot(db.connection, db.schema_name) == before


def test_trace_qualifies_schema_locks_and_single_final_cas(
    readiness_db: M5RuntimeDatabase,
) -> None:
    db = readiness_db
    plan = db.retire_plan(event_id="d32-trace")
    opened = _open_recovery_event(PostgresM5RuntimeStore(db.connection), db, plan)
    statements: list[str] = []

    class RecordingCursor(Cursor[Any]):
        def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
            text = query if isinstance(query, str) else query.as_string(self.connection)
            statements.append(" ".join(text.split()))
            return super().execute(query, params, **kwargs)

    factory = db.connection.cursor_factory
    db.connection.cursor_factory = RecordingCursor
    try:
        _edge(
            db.connection,
            opened.epoch_id,
            plan.structural_event_id,
            "semantic_pending",
            1,
        )
    finally:
        db.connection.cursor_factory = factory
    prefix = '"' + db.schema_name + '".'
    writes = [q for q in statements if q.startswith(("INSERT ", "UPDATE ", "DELETE "))]
    assert len(writes) == 8  # old missing timing + contribution + six point UPDATEs
    assert all(prefix in query for query in writes)
    updates = [q for q in writes if q.startswith("UPDATE ")]
    expected = [
        "groundloop_epoch",
        "groundloop_m5_runtime_epoch",
        "groundloop_m5_owner_pending_counter",
        "groundloop_m5_answer_pending_counter",
        "groundloop_m5_runtime_work_accumulator",
        "groundloop_m5_runtime_timing_accumulator",
    ]
    assert [
        next(name for name in expected if prefix + '"' + name + '"' in q)
        for q in updates
    ] == expected
    assert writes[1].startswith(
        "INSERT INTO " + prefix + '"groundloop_m5_runtime_work_contribution"'
    )
    locked = [q for q in statements if "FOR UPDATE" in q]
    assert [
        next(name for name in expected if prefix + '"' + name + '"' in q)
        for q in locked
    ] == expected
    assert 'COLLATE pg_catalog."C"' in locked[2]
    assert 'COLLATE pg_catalog."C"' in locked[3]
    assert not any("SUM(" in q.upper() for q in statements)
    assert not any("SET SEARCH_PATH" in q.upper() for q in statements)
    assert statements[-1] == "SET CONSTRAINTS ALL IMMEDIATE"
    work_reads = [q for q in statements if "SELECT" in q and prefix in q]
    assert all(
        "epoch_id OPERATOR(pg_catalog.=)" in q
        or "candidate_policy_id OPERATOR(pg_catalog.=)" in q
        or "groundloop_m5_authorize_checked_transition" in q
        or "groundloop_m5_schema_bundle" in q
        for q in work_reads
    )
    statements.clear()
    db.connection.cursor_factory = RecordingCursor
    try:
        assert _edge(
            db.connection,
            opened.epoch_id,
            plan.structural_event_id,
            "semantic_pending",
            1,
        ).replay
    finally:
        db.connection.cursor_factory = factory
    assert not any(q.startswith(("INSERT ", "UPDATE ", "DELETE ")) for q in statements)
    assert not any("FOR UPDATE" in q for q in statements)
    db.connection.execute("SET enable_seqscan TO off")
    for relation in (
        "groundloop_m5_runtime_work_accumulator",
        "groundloop_m5_runtime_timing_accumulator",
    ):
        rows = db.connection.execute(
            sql.SQL("EXPLAIN (COSTS OFF) SELECT * FROM {} WHERE epoch_id=%s").format(
                sql.Identifier(db.schema_name, relation)
            ),
            (opened.epoch_id,),
        ).fetchall()
        assert any("Index Scan" in row[0] for row in rows)
        assert any("Index Cond:" in row[0] and "epoch_id" in row[0] for row in rows)
    db.connection.rollback()


def test_temp_shadows_cannot_supply_runtime_authority(
    readiness_db: M5RuntimeDatabase,
) -> None:
    db = readiness_db
    plan = db.retire_plan(event_id="d32-shadow")
    opened = _open_recovery_event(PostgresM5RuntimeStore(db.connection), db, plan)
    for relation in (
        "groundloop_epoch",
        "groundloop_m5_runtime_epoch",
        "groundloop_m5_runtime_work_contribution",
        "groundloop_m5_runtime_work_accumulator",
        "groundloop_m5_runtime_timing_accumulator",
    ):
        db.connection.execute(
            sql.SQL("CREATE TEMP TABLE {} (epoch_id bigint)").format(
                sql.Identifier(relation)
            )
        )
    db.connection.commit()
    before = _snapshot(db.connection, db.schema_name)
    with pytest.raises(ValidationError, match="temporary lookup"):
        _edge(
            db.connection,
            opened.epoch_id,
            plan.structural_event_id,
            "semantic_pending",
            1,
        )
    assert _snapshot(db.connection, db.schema_name) == before
    for relation in ("groundloop_epoch", "groundloop_m5_runtime_epoch"):
        assert db.connection.execute(
            sql.SQL("SELECT count(*) FROM pg_temp.{}").format(sql.Identifier(relation))
        ).fetchone() == (0,)
    db.connection.commit()


@pytest.mark.parametrize(
    "field,value",
    (
        ("epoch_id", True),
        ("epoch_id", -1),
        ("structural_event_id", ""),
        ("expected_revision", True),
        ("expected_revision", 0),
        ("target", "sealed"),
    ),
)
def test_invalid_call_is_no_write(
    readiness_db: M5RuntimeDatabase, field: str, value: Any
) -> None:
    db = readiness_db
    before = _snapshot(db.connection, db.schema_name)
    args = dict(
        epoch_id=2,
        structural_event_id="absent",
        target="semantic_pending",
        expected_revision=1,
    )
    args[field] = value
    with pytest.raises(ValidationError):
        _advance_semantic_readiness_atomically(db.connection, **args)
    assert _snapshot(db.connection, db.schema_name) == before


def test_foreign_operator_shadows_cannot_change_points_or_arithmetic(
    readiness_db: M5RuntimeDatabase,
) -> None:
    db = readiness_db
    plan = db.retire_plan(event_id="d32-operator-shadow")
    opened = _open_recovery_event(PostgresM5RuntimeStore(db.connection), db, plan)
    foreign = "d32_foreign_" + uuid.uuid4().hex
    connection = db.connection
    connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(foreign)))
    for name, left, right, result, body, operator in (
        ("eq8", "int8", "int8", "bool", "true", "="),
        ("eq2", "int8", "int2", "bool", "true", "="),
        ("eqtext", "text", "text", "bool", "true", "="),
        ("add4", "int8", "int4", "int8", "99999::pg_catalog.int8", "+"),
        ("add2", "int8", "int2", "int8", "99999::pg_catalog.int8", "+"),
    ):
        connection.execute(
            sql.SQL(
                "CREATE FUNCTION {}(pg_catalog.{},pg_catalog.{}) RETURNS pg_catalog.{} "
                "LANGUAGE SQL IMMUTABLE AS {}"
            ).format(
                sql.Identifier(foreign, name),
                sql.Identifier(left),
                sql.Identifier(right),
                sql.Identifier(result),
                sql.Literal("SELECT " + body),
            )
        )
        connection.execute(
            sql.SQL(
                "CREATE OPERATOR {}.{} (FUNCTION={},LEFTARG=pg_catalog.{},"
                "RIGHTARG=pg_catalog.{})"
            ).format(
                sql.Identifier(foreign),
                sql.SQL(operator),
                sql.Identifier(foreign, name),
                sql.Identifier(left),
                sql.Identifier(right),
            )
        )
    connection.commit()
    try:
        connection.execute(
            sql.SQL("SET search_path TO {}, {}, pg_catalog").format(
                sql.Identifier(db.schema_name), sql.Identifier(foreign)
            )
        )
        connection.commit()
        before = _snapshot(connection, db.schema_name)
        for nonexistent in (opened.epoch_id + 999, opened.epoch_id + 1000):
            with pytest.raises(ValidationError, match="mixed legacy"):
                _edge(
                    connection,
                    nonexistent,
                    plan.structural_event_id,
                    "semantic_pending",
                    1,
                )
        with pytest.raises(ValidationError, match="mixed legacy"):
            _edge(
                connection,
                opened.epoch_id,
                plan.structural_event_id,
                "semantic_pending",
                1,
            )
        assert _snapshot(connection, db.schema_name) == before
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(db.schema_name)
            )
        )
        connection.commit()
        pending = _edge(
            connection, opened.epoch_id, plan.structural_event_id, "semantic_pending", 1
        )
        connection.execute(
            sql.SQL("SET search_path TO {}, {}, pg_catalog").format(
                sql.Identifier(db.schema_name), sql.Identifier(foreign)
            )
        )
        connection.commit()
        before = _snapshot(connection, db.schema_name)
        with pytest.raises(ValidationError, match="mixed legacy"):
            _edge(
                connection,
                opened.epoch_id,
                plan.structural_event_id,
                pending.anchor.source_id,
                1,
            )
        assert _snapshot(connection, db.schema_name) == before
    finally:
        connection.rollback()
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(db.schema_name)
            )
        )
        connection.execute(
            sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(foreign))
        )
        connection.commit()


def test_genuine_direct_document_surface_is_not_group_readiness(
    direct_runtime_db: Any,
) -> None:
    db = direct_runtime_db
    row = db.connection.execute(
        "SELECT revision FROM groundloop_epoch WHERE epoch_id=%s", (db.base.epoch_id,)
    ).fetchone()
    assert row is not None
    _seed_b3_direct_m4_snapshot(
        db.connection, epoch_id=db.base.epoch_id, revision=row[0]
    )
    db.connection.commit()
    for installer in (
        install_m5_persisted_matching_bundle,
        install_m5_bounded_document_withdrawal_bundle,
        install_m5_preterminal_seal_context_bundle,
        install_m5_semantic_readiness_bundle,
    ):
        assert installer(db.connection).applied
    opened = _open_direct_epoch(db)
    schema = db.connection.execute("SELECT pg_catalog.current_schema()").fetchone()[0]
    db.connection.commit()
    before = _snapshot(db.connection, schema)
    for target in ("semantic_pending", "semantic_complete"):
        with pytest.raises(InvalidEventError, match="group declaration"):
            _edge(
                db.connection, opened.epoch_id, opened.event.update.event_id, target, 1
            )
        assert _snapshot(db.connection, schema) == before


def _matching_registration(db: M5RuntimeDatabase) -> tuple[Any, int, int]:
    """Accepted-helper foundation in rev-1 open, not a production C1 fixture."""
    from tests.m5.postgres_runtime.d25_store_core.conftest import (
        _open_requirement_registration_with_matching,
    )

    plan = db.register_plan(event_id="d32-sql-boundary-register")
    epoch = _open_requirement_registration_with_matching(
        db.connection,
        plan=plan,
        operational_config=db.operational_config,
    )
    roots = _roots(plan, db.manifest)
    cancelled = PostgresM5RuntimeStore(db.connection).cancel_m5_work_atomically(
        epoch,
        1,
        M5CancellationPlan.build(
            epoch_id=epoch,
            structural_event_id=plan.structural_event_id,
            reason=M5TerminalReason.SCOPE_RETIRED,
            cancelled_job_ids=tuple(sorted(job.logical_job_id for _, job in roots)),
        ),
    )
    return plan, epoch, cancelled.resulting_revision


def _matching_ready_registration(db: M5RuntimeDatabase) -> tuple[Any, int, Any]:
    plan, epoch, revision = _matching_registration(db)
    ready = _edge(
        db.connection,
        epoch,
        plan.structural_event_id,
        "semantic_complete",
        revision,
    )
    # This is a genuine separately committed timing append. Pending-new-kind
    # reader acceptance belongs to R-T; do not bypass it by clearing a point.
    PostgresM5RuntimeStore(db.connection).append_transition_call_timing(
        epoch,
        ready.anchor.contribution_kind,
        ready.anchor.source_id,
        ready.anchor.contribution_key_digest,
        ready.anchor.anchor_revision,
        None,
    )
    return plan, epoch, ready


def _sql_seal_registration_boundary(
    connection: Connection[Any],
    plan: Any,
    epoch: int,
    revision: int,
    cut: Any = lambda _: None,
) -> Any:
    """Bounded SQL terminal adapter, never a production/public seal route.

    Uses the reported real readiness point; preserves cumulative work and
    coverage. Records actual state-row writes and explicit S/K producer bytes.
    Other Python child validation/SQL hashing is not benchmarked or promoted
    as comprehensive C1 producer instrumentation by this diagnostic fixture.
    """
    import hashlib

    from groundloop.m4.application import OpenEventReceipt, PublicationReceipt
    from groundloop.m4.contracts import stable_m4_digest
    from groundloop.m5.digests import enum_field, hash_field, int_field, text_field
    from groundloop.m5.runtime.contracts import M5EventRunResult, M5RunState
    from groundloop.m5.runtime.persistence import _insert_runtime_work
    from groundloop.m5.runtime.postgres_matching_publication import (
        _prepare_preterminal_matching_publication_children,
        build_matching_publication_children,
        promote_matching_overlay,
    )
    from groundloop.m5.runtime.postgres_readiness import _lock_accounting
    from groundloop.m5.runtime.postgres_recovery import _TIMING_COVERAGE_COLUMNS

    sealed = revision + 1
    with connection.transaction(), connection.cursor() as cursor:
        schema = cursor.execute("SELECT pg_catalog.current_schema()").fetchone()[0]
        for table in (
            "groundloop_runtime_mode",
            "groundloop_m4_publication_head",
            "groundloop_m5_publication_head",
        ):
            cursor.execute(
                sql.SQL("SELECT * FROM {} WHERE singleton FOR UPDATE").format(
                    sql.Identifier(schema, table)
                )
            ).fetchone()
        base = cursor.execute(
            "SELECT revision,semantic_status FROM groundloop_epoch "
            "WHERE epoch_id=%s FOR UPDATE",
            (epoch,),
        ).fetchone()
        cut("after_base_lock")
        runtime = cursor.execute(
            "SELECT revision,runtime_state FROM "
            "groundloop_m5_runtime_epoch WHERE epoch_id=%s FOR UPDATE",
            (epoch,),
        ).fetchone()
        if base != (revision, "complete") or runtime != (revision, "semantic_complete"):
            raise InvalidEventError("SQL adapter requires genuine semantic readiness")
        for table, key in (
            ("groundloop_m5_owner_pending_counter", "owner_claim_id"),
            ("groundloop_m5_answer_pending_counter", "answer_version_id"),
        ):
            cursor.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE epoch_id=%s "
                    'ORDER BY {} COLLATE "C" FOR UPDATE'
                ).format(sql.Identifier(schema, table), sql.Identifier(key)),
                (epoch,),
            ).fetchall()
        prior, timing = _lock_accounting(cursor, schema, epoch, revision)
        if timing.has_pending_anchor:
            raise InvalidEventError(
                "SQL fixture requires separately reported readiness"
            )
        promote_matching_overlay(
            cursor, epoch_id=epoch, expected_revision=revision, sealed_revision=sealed
        )
        group = plan.event.group
        cursor.execute(
            "UPDATE groundloop_m5_group_family SET lifecycle_state='PUBLISHED' "
            "WHERE group_family_id=%s",
            (group.group_family_id,),
        )
        cursor.execute(
            "UPDATE groundloop_m5_group_version SET lifecycle_state='PUBLISHED' "
            "WHERE group_version_id=%s",
            (group.group_version_id,),
        )
        cursor.execute(
            "UPDATE groundloop_m5_requirement_version SET "
            "lifecycle_state='PUBLISHED' WHERE group_version_id=%s",
            (group.group_version_id,),
        )
        cursor.execute(
            "INSERT INTO groundloop_m5_group_validity "
            "(group_version_id,group_family_id,claim_id,semantic_structure_hash,"
            "supersedes_group_version_id,valid_from_epoch,valid_to_epoch) "
            "VALUES (%s,%s,%s,%s,NULL,%s,NULL)",
            (
                group.group_version_id,
                group.group_family_id,
                group.owner_claim_id,
                group.semantic_structure_hash,
                epoch,
            ),
        )
        state_write_counts = {}
        for kind, _key in (
            ("requirement", "requirement_version_id"),
            ("group", "group_version_id"),
        ):
            working = "groundloop_m5_working_" + kind + "_state"
            columns = [
                item[0]
                for item in cursor.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema=%s AND table_name=%s "
                    "AND column_name NOT IN ('epoch_id','updated_revision') "
                    "ORDER BY ordinal_position",
                    (schema, working),
                ).fetchall()
            ]
            col = sql.SQL(",").join(map(sql.Identifier, columns))
            count = cursor.execute(
                sql.SQL(
                    "INSERT INTO {} "
                    "({},valid_from_epoch,valid_to_epoch,sealed_revision) "
                    "SELECT {},%s,NULL,%s FROM {} WHERE epoch_id=%s"
                ).format(
                    sql.Identifier(
                        schema, "groundloop_m5_published_" + kind + "_state"
                    ),
                    col,
                    col,
                    sql.Identifier(schema, working),
                ),
                (epoch, sealed, epoch),
            ).rowcount
            count += cursor.execute(
                sql.SQL(
                    "INSERT INTO {} ({},updated_epoch,updated_revision) "
                    "SELECT {},%s,%s FROM {} WHERE epoch_id=%s"
                ).format(
                    sql.Identifier(
                        schema, "groundloop_m5_" + kind + "_state_materialized"
                    ),
                    col,
                    col,
                    sql.Identifier(schema, working),
                ),
                (epoch, sealed, epoch),
            ).rowcount
            state_write_counts[kind] = count
        cursor.execute(
            "UPDATE groundloop_m4_publication_head SET epoch_id=%s WHERE singleton",
            (epoch,),
        )
        cursor.execute(
            "UPDATE groundloop_m5_publication_head SET epoch_id=%s,"
            "sealed_revision=%s WHERE singleton",
            (epoch, sealed),
        )
        cursor.execute(
            "UPDATE groundloop_epoch SET revision=%s,semantic_status='sealed',"
            "evaluation_state='complete',publication_mode='strict',"
            "sealed_at=clock_timestamp() WHERE epoch_id=%s AND revision=%s",
            (sealed, epoch, revision),
        )
        children = _prepare_preterminal_matching_publication_children(
            cursor,
            epoch_id=epoch,
            expected_revision=revision,
            sealed_revision=sealed,
        )
        assert not children.combined_deltas
        publication = stable_m4_digest("m4-publication-v1", str(epoch))
        combined = digests.combined_status_delta_set_digest(children.combined_deltas)
        changed = digests.changed_state_set_digest(
            ref.reference_digest for ref in children.changed_state_references
        )
        source_bytes = digests.stable_m5_preimage(
            "m5-seal-contribution-source-v1",
            text_field(plan.structural_event_id),
            hash_field(combined),
            hash_field(changed),
            text_field(publication),
        )
        source = hashlib.sha256(source_bytes).hexdigest()
        key_bytes = digests.stable_m5_preimage(
            "m5-runtime-work-contribution-key-v1",
            int_field(epoch),
            enum_field("seal"),
            text_field(plan.structural_event_id),
        )
        key = hashlib.sha256(key_bytes).hexdigest()
        work = M5RuntimeWork(
            group_state_write_count=state_write_counts["group"],
            bytes_hashed=len(source_bytes) + len(key_bytes),
            bytes_serialized=len(source_bytes) + len(key_bytes),
        )
        counts: dict[str, Any] = dict(
            zip(
                prior.counter_names(),
                (
                    a + b
                    for a, b in zip(
                        prior.counter_values(), work.counter_values(), strict=True
                    )
                ),
                strict=True,
            )
        )
        total = M5RuntimeWork(**counts)
        names = (
            "epoch_id",
            "contribution_kind",
            "source_id",
            "source_identity_hash",
            "contribution_key_digest",
            "applied_revision",
            *work.counter_names(),
            "work_digest",
        )
        cursor.execute(
            sql.SQL(
                "INSERT INTO groundloop_m5_runtime_work_contribution ({}) VALUES ({})"
            ).format(
                sql.SQL(",").join(map(sql.Identifier, names)),
                sql.SQL(",").join(sql.Placeholder() for _ in names),
            ),
            (
                epoch,
                "seal",
                plan.structural_event_id,
                source,
                key,
                sealed,
                *work.counter_values(),
                work.work_digest,
            ),
        )
        cut("after_contribution")
        cursor.execute(
            "UPDATE groundloop_m5_runtime_epoch SET revision=%s,"
            "runtime_state='sealed',terminal_at=clock_timestamp() WHERE epoch_id=%s "
            "AND revision=%s",
            (sealed, epoch, revision),
        )
        for table in (
            "groundloop_m5_owner_pending_counter",
            "groundloop_m5_answer_pending_counter",
        ):
            cursor.execute(
                sql.SQL("UPDATE {} SET updated_revision=%s WHERE epoch_id=%s").format(
                    sql.Identifier(schema, table)
                ),
                (sealed, epoch),
            )
        assignments = sql.SQL(",").join(
            sql.SQL("{}=%s").format(sql.Identifier(name))
            for name in total.counter_names()
        )
        assert (
            cursor.execute(
                sql.SQL(
                    "UPDATE groundloop_m5_runtime_work_accumulator SET {},"
                    "work_digest=%s,updated_revision=%s,terminalized=true "
                    "WHERE epoch_id=%s "
                    "AND updated_revision=%s AND work_digest=%s AND NOT terminalized"
                ).format(assignments),
                (
                    *total.counter_values(),
                    total.work_digest,
                    sealed,
                    epoch,
                    revision,
                    prior.work_digest,
                ),
            ).rowcount
            == 1
        )
        coverage_updates = []
        for prefix in (
            "required",
            "postgres_server_execution",
            "postgres_lock_wait",
            "postgres_wal_bytes",
            "postgres_shared_block_reads",
        ):
            coverage_updates.extend(
                [
                    sql.SQL("{}={}+1").format(
                        sql.Identifier(prefix + "_expected_count"),
                        sql.Identifier(prefix + "_expected_count"),
                    ),
                    sql.SQL("{}={}+1").format(
                        sql.Identifier(prefix + "_missing_count"),
                        sql.Identifier(prefix + "_missing_count"),
                    ),
                ]
            )
        assert (
            cursor.execute(
                sql.SQL(
                    "UPDATE groundloop_m5_runtime_timing_accumulator SET {},"
                    "terminalized=true,updated_revision=%s,pending_contribution_kind=NULL,"
                    "pending_source_id=NULL,pending_contribution_key_digest=NULL,"
                    "pending_anchor_revision=NULL "
                    "WHERE epoch_id=%s AND updated_revision=%s AND NOT terminalized"
                ).format(sql.SQL(",").join(coverage_updates)),
                (sealed, epoch, revision),
            ).rowcount
            == 1
        )
        # current_event_timing owns a nested transaction and requests READ ONLY:
        # read the just-frozen point directly instead of relying on that wrapper.
        from groundloop.m5.runtime.postgres_recovery import (
            _TIMING_SUM_COLUMNS,
            _timing_accumulator_from_row,
        )

        returned = cursor.execute(
            "SELECT "
            + ",".join((*_TIMING_SUM_COLUMNS, *_TIMING_COVERAGE_COLUMNS))
            + ",updated_revision,terminalized,"
            "pending_contribution_kind,pending_source_id,pending_contribution_key_digest,"
            "pending_anchor_revision FROM groundloop_m5_runtime_timing_accumulator "
            "WHERE epoch_id=%s",
            (epoch,),
        ).fetchone()
        event_timing, coverage = _timing_accumulator_from_row(returned).project(
            pending_as_missing=False
        )
        for kind, vector in (("event", total), ("call", work)):
            _insert_runtime_work(
                cursor,
                structural_event_id=plan.structural_event_id,
                epoch_id=epoch,
                work_kind=kind,
                work=vector,
            )
        result = M5EventRunResult.build(
            event_id=plan.structural_event_id,
            payload_hash=plan.payload_hash,
            epoch_id=epoch,
            state=M5RunState.SEALED,
            replayed_outcome=None,
            open_receipt=OpenEventReceipt(epoch, False, False),
            publication_receipt=PublicationReceipt(epoch, publication, False),
            event_work=total,
            call_work=work,
            event_timing=event_timing,
            call_timing=M5RuntimeTiming(),
            combined_deltas=children.combined_deltas,
            changed_state_references=children.changed_state_references,
            failure_reason=None,
            event_timing_coverage=coverage,
            call_timing_coverage=M5RuntimeTimingCoverage.single_point(
                timing=None, terminal_client_roundtrip_included=False
            ),
        )
        cursor.execute(
            "INSERT INTO groundloop_m5_event_result "
            "(structural_event_id,payload_hash,epoch_id,outcome,original_open_receipt_binding_hash,"
            "publication_id,original_publication_receipt_binding_hash,event_work_kind,event_work_digest,"
            "combined_status_delta_set_hash,changed_state_set_hash,failure_reason,logical_result_hash,"
            "delta_count,state_reference_count,coordinator_non_db_non_neural_ns,neural_wall_ns,"
            "postgres_roundtrip_wall_ns,external_io_wall_ns,end_to_end_wall_ns,"
            "postgres_server_execution_ns,postgres_lock_wait_ns,postgres_wal_bytes,"
            "postgres_shared_block_reads) "
            "VALUES (%s,%s,%s,'sealed',%s,%s,%s,'event',%s,%s,%s,NULL,%s,%s,%s,"
            + ",".join(["%s"] * 9)
            + ")",
            (
                plan.structural_event_id,
                plan.payload_hash,
                epoch,
                digests.open_event_receipt_binding_digest(
                    epoch_id=epoch,
                    replayed=False,
                    already_sealed=False,
                    publication_id=None,
                    already_failed=False,
                    failure_reason=None,
                ),
                publication,
                digests.publication_receipt_binding_digest(
                    epoch_id=epoch, publication_id=publication, replayed=False
                ),
                total.work_digest,
                combined,
                changed,
                result.logical_result_hash,
                len(children.combined_deltas),
                len(children.changed_state_references),
                *(getattr(event_timing, name) for name in _TIMING_SUM_COLUMNS),
            ),
        )
        assert (
            build_matching_publication_children(
                cursor, epoch_id=epoch, sealed_revision=sealed
            )
            == children
        )
        for ordinal, ref in enumerate(children.changed_state_references):
            cursor.execute(
                "INSERT INTO groundloop_m5_event_result_state_reference VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    plan.structural_event_id,
                    ordinal,
                    ref.kind.value,
                    ref.object_id,
                    ref.epoch_id,
                    ref.revision,
                    ref.state_artifact_hash,
                    ref.reference_digest,
                ),
            )
        cursor.execute(
            sql.SQL(
                "INSERT INTO groundloop_m5_event_timing_coverage "
                "(structural_event_id,epoch_id,{},terminal_client_roundtrip_included) "
                "VALUES "
                "(%s,%s,{},false)"
            ).format(
                sql.SQL(",").join(map(sql.Identifier, _TIMING_COVERAGE_COLUMNS)),
                sql.SQL(",").join(sql.Placeholder() for _ in _TIMING_COVERAGE_COLUMNS),
            ),
            (
                plan.structural_event_id,
                epoch,
                *(getattr(coverage, name) for name in _TIMING_COVERAGE_COLUMNS),
            ),
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
        cut("after_constraints")
        return result


@pytest.mark.parametrize("nonowner", (False, True))
def test_sql_boundary_after_seal_retains_original_readiness_anchor(
    readiness_db: M5RuntimeDatabase,
    nonowner: bool,
) -> None:
    db = readiness_db
    plan, epoch, ready = _matching_ready_registration(db)
    context = _runtime_login(db) if nonowner else db.reconnect()
    with context as connection:
        principal = connection.execute("SELECT current_user").fetchone()[0]
        owner = db.connection.execute("SELECT current_user").fetchone()[0]
        db.connection.commit()
        assert (principal != owner) is nonowner
        connection.commit()
        result = _sql_seal_registration_boundary(
            connection, plan, epoch, ready.anchor.anchor_revision
        )
        assert result.event_work.bytes_hashed > result.call_work.bytes_hashed > 0
        before = _snapshot(connection, db.schema_name)
        replay = _edge(
            connection,
            epoch,
            plan.structural_event_id,
            "semantic_complete",
            ready.anchor.anchor_revision - 1,
        )
        assert replay.replay and replay.anchor == ready.anchor
        assert _snapshot(connection, db.schema_name) == before


@pytest.mark.parametrize("first", ("readiness", "seal"))
def test_fresh_readiness_and_sql_seal_lock_orders(
    readiness_db: M5RuntimeDatabase, first: str
) -> None:
    import threading

    db = readiness_db
    plan, epoch, revision = _matching_registration(db)
    locked = threading.Event()
    following_started = threading.Event()
    release = threading.Event()

    def run_ready() -> Any:
        with db.reconnect() as connection:

            def cut(point: str) -> None:
                if first == "readiness" and point == "after_base_lock":
                    locked.set()
                    assert release.wait(20)
                if first == "seal" and point == "after_authority":
                    following_started.set()

            return _edge(
                connection,
                epoch,
                plan.structural_event_id,
                "semantic_complete",
                revision,
                failure_injector=cut,
            )

    def run_seal() -> Any:
        with db.reconnect() as connection:

            def cut(point: str) -> None:
                if first == "seal" and point == "after_base_lock":
                    locked.set()
                    assert release.wait(20)

            if first == "readiness":
                following_started.set()
            return _sql_seal_registration_boundary(
                connection,
                plan,
                epoch,
                revision if first == "seal" else revision + 1,
                cut,
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        leading = pool.submit(run_ready if first == "readiness" else run_seal)
        assert locked.wait(20)
        following = pool.submit(run_seal if first == "readiness" else run_ready)
        assert following_started.wait(20)
        release.set()
        ready_future = leading if first == "readiness" else following
        seal_future = leading if first == "seal" else following
        with pytest.raises(InvalidEventError):
            seal_future.result(timeout=30)
        ready = ready_future.result(timeout=30)
    assert not ready.replay
    assert _point(db.connection, epoch)[0] == revision + 1
    # The supplemental SQL adapter does not prove unreported-kind reader
    # acceptance; report the real point, then retry under the legal new phase.
    PostgresM5RuntimeStore(db.connection).append_transition_call_timing(
        epoch,
        ready.anchor.contribution_kind,
        ready.anchor.source_id,
        ready.anchor.contribution_key_digest,
        ready.anchor.anchor_revision,
        None,
    )
    _sql_seal_registration_boundary(db.connection, plan, epoch, revision + 1)
    assert _point(db.connection, epoch)[3] == "sealed"
    before = _snapshot(db.connection, db.schema_name)
    assert (
        _edge(
            db.connection,
            epoch,
            plan.structural_event_id,
            "semantic_complete",
            revision,
        ).anchor
        == ready.anchor
    )
    assert _snapshot(db.connection, db.schema_name) == before


@pytest.mark.parametrize("first", ("replay_snapshot", "seal_commit"))
def test_concurrent_historical_replay_keeps_one_snapshot(
    readiness_db: M5RuntimeDatabase,
    first: str,
) -> None:
    import threading

    db = readiness_db
    plan, epoch, ready = _matching_ready_registration(db)
    replay_paused = threading.Event()
    resume_replay = threading.Event()

    def run_replay() -> Any:
        with db.reconnect() as connection:

            def cut(point: str) -> None:
                boundary = (
                    "after_replay_read"
                    if first == "replay_snapshot"
                    else "after_authority"
                )
                if point == boundary:
                    replay_paused.set()
                    assert resume_replay.wait(20)

            return _edge(
                connection,
                epoch,
                plan.structural_event_id,
                "semantic_complete",
                ready.anchor.anchor_revision - 1,
                failure_injector=cut,
            )

    def run_seal() -> Any:
        with db.reconnect() as connection:
            return _sql_seal_registration_boundary(
                connection, plan, epoch, ready.anchor.anchor_revision
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        replay_future = pool.submit(run_replay)
        assert replay_paused.wait(20)
        sealed = pool.submit(run_seal).result(timeout=30)
        resume_replay.set()
        replay = replay_future.result(timeout=30)
    assert sealed.epoch_id == epoch
    assert replay.replay and replay.anchor == ready.anchor
    assert _point(db.connection, epoch)[3] == "sealed"
