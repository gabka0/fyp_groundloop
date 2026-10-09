"""M5-D32 schema diagnostics; manual SQL edges are not runtime acceptance."""

from __future__ import annotations

import hashlib
import re
import threading
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import Connection, IsolationLevel, sql
from psycopg.pq import TransactionStatus

from groundloop.domain import SubjectKind
from groundloop.m5.digests import enum_field, int_field, text_field
from groundloop.m5.runtime.contracts import (
    M5CancellationPlan,
    M5JobCompletion,
    M5JobState,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TerminalReason,
    M5TransitionTimingAnchor,
    SemanticPairKey,
)
from groundloop.m5.runtime.digests import (
    semantic_readiness_transition_preimage,
    stable_m5_preimage,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from groundloop.m5.runtime.postgres_recovery import (
    persist_structural_open_accounting,
    start_event_accounting,
)
from groundloop.postgres.migrations import (
    _M5_READINESS_CATALOG_SHA256,
    M5_ACCEPTED_SEMANTIC_READINESS_BUNDLE_SHA256,
    M5_ACCEPTED_SEMANTIC_READINESS_MIGRATION_SHA256,
    M5_SEMANTIC_READINESS_BUNDLE_ID,
    M5BundleHashConflictError,
    M5PrerequisiteError,
    M5SemanticReadinessBundleError,
    _m5_readiness_catalog_fingerprint,
    _m5_readiness_catalog_sql,
    _m5_readiness_verify_catalog,
    install_m5_bounded_document_withdrawal_bundle,
    install_m5_persisted_matching_bundle,
    install_m5_preterminal_seal_context_bundle,
    install_m5_semantic_readiness_bundle,
    m5_semantic_readiness_bundle_identity,
    require_m5_semantic_readiness_bundle,
)
from tests.m5.postgres_runtime.conftest import M5RuntimeDatabase
from tests.m5.postgres_runtime.test_migration_017 import (
    _open_b2_runtime_epoch,
    _seed_b2_runtime_base,
    _seed_b3_direct_m4_snapshot,
)
from tests.m5.postgres_runtime.test_migration_019 import (
    FROZEN_MIGRATION_HASHES,
    _all_ledger_rows,
    _database_url,
    _pre019_schema,
    _RecordingConnection,
    _select_schema,
)
from tests.m5.postgres_runtime.test_root_transitions import (
    _acquire,
    _open_recovery_event,
    _result,
    _root_set_hash,
    _roots,
    _stage,
)

ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "migrations/020_m5_semantic_readiness.sql"
MARKER = "-- groundloop:m5-semantic-readiness-statement:"
FUNCTIONS = (
    "groundloop_m5_validate_semantic_readiness_predecessor",
    "groundloop_m5_validate_work_contribution",
    "groundloop_m5_validate_timing_accumulator",
)


def _statements() -> tuple[tuple[str, str], ...]:
    return tuple(
        (part.split("\n", 1)[0], part.split("\n", 1)[1].strip())
        for part in MIGRATION.read_text().split(MARKER)[1:]
    )


def _raw_install(connection: Connection[Any], schema: str) -> None:
    """Diagnostic raw SQL only, before the separate H32 installer hash barrier."""
    with connection.transaction():
        connection.execute(
            "SELECT set_config('search_path',quote_ident(%s)||', pg_catalog',true)",
            (schema,),
        )
        for _, statement in _statements():
            connection.execute(statement)


@pytest.fixture
def raw_database() -> Iterator[tuple[Connection[Any], str]]:
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        _raw_install(connection, schema)
        yield connection, schema


def _manual_open(connection: Connection[Any]) -> tuple[int, str, str]:
    """Schema-only locked-row fixture, not the production structural opener."""
    base, policy = _seed_b2_runtime_base(connection, "d32-schema")
    epoch, event, payload = _open_b2_runtime_epoch(
        connection, "d32-schema-更新", base, policy, update_kind="retire_group"
    )
    with connection.cursor() as cursor:
        persist_structural_open_accounting(
            cursor,
            epoch_id=epoch,
            structural_event_id=event,
            payload_hash=payload,
            structural_work=M5RuntimeWork(),
        )
    connection.commit()
    return epoch, event, payload


def _edge_values(
    connection: Connection[Any], epoch: int, target: str, revision: int
) -> tuple[M5RuntimeWork, M5TransitionTimingAnchor, str]:
    event, payload, roots = connection.execute(
        "SELECT base.event_id,base.payload_hash,runtime.requirement_root_set_hash "
        "FROM groundloop_epoch base JOIN groundloop_m5_runtime_epoch runtime "
        "USING(epoch_id) "
        "WHERE epoch_id=%s",
        (epoch,),
    ).fetchone() or (None, None, None)
    assert event is not None
    fields = dict(
        epoch_id=epoch,
        structural_event_id=str(event),
        event_payload_hash=str(payload),
        requirement_root_set_hash=str(roots),
        from_runtime_state=(
            "structural_committed"
            if target == "semantic_pending"
            else "semantic_pending"
        ),
        to_runtime_state=target,
        expected_revision=revision,
        resulting_revision=revision + 1,
    )
    source_preimage = semantic_readiness_transition_preimage(**fields)
    source = hashlib.sha256(source_preimage).hexdigest()
    key_preimage = stable_m5_preimage(
        "m5-runtime-work-contribution-key-v1",
        int_field(epoch),
        enum_field("semantic_readiness"),
        text_field(target),
    )
    count = len(source_preimage) + len(key_preimage)
    work = M5RuntimeWork(bytes_hashed=count, bytes_serialized=count)
    anchor = M5TransitionTimingAnchor(
        epoch_id=epoch,
        contribution_kind=M5RuntimeWorkContributionKind.SEMANTIC_READINESS,
        source_id=target,
        contribution_key_digest=hashlib.sha256(key_preimage).hexdigest(),
        anchor_revision=revision + 1,
        terminal_transition=False,
    )
    return work, anchor, source


def _insert(
    connection: Connection[Any],
    work: M5RuntimeWork,
    anchor: M5TransitionTimingAnchor,
    source: str,
) -> None:
    columns = (
        "epoch_id",
        *work.counter_names(),
        "work_digest",
        "contribution_kind",
        "source_id",
        "source_identity_hash",
        "contribution_key_digest",
        "applied_revision",
    )
    connection.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_contribution ({}) VALUES ({})"
        ).format(
            sql.SQL(",").join(map(sql.Identifier, columns)),
            sql.SQL(",").join(sql.Placeholder() for _ in columns),
        ),
        (
            anchor.epoch_id,
            *work.counter_values(),
            work.work_digest,
            anchor.contribution_kind.value,
            anchor.source_id,
            source,
            anchor.contribution_key_digest,
            anchor.anchor_revision,
        ),
    )


def _advance(
    connection: Connection[Any], epoch: int, target: str, revision: int
) -> None:
    connection.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s,%s)", (epoch, revision)
    )
    status = "complete" if target == "semantic_complete" else "pending"
    connection.execute(
        "UPDATE groundloop_epoch SET "
        "revision=revision+1,semantic_status=%s,evaluation_state=%s "
        "WHERE epoch_id=%s",
        (status, status, epoch),
    )
    connection.execute(
        "UPDATE groundloop_m5_runtime_epoch SET revision=revision+1,runtime_state=%s "
        "WHERE epoch_id=%s",
        (target, epoch),
    )


def _manual_edge(
    connection: Connection[Any], epoch: int, target: str, revision: int
) -> None:
    """One intact SQL boundary diagnostic, explicitly not a private runtime call."""
    work, anchor, source = _edge_values(connection, epoch, target, revision)
    with connection.cursor() as cursor:
        start = start_event_accounting(
            cursor, epoch_id=epoch, expected_revision=revision
        )
    _insert(connection, work, anchor, source)
    _advance(connection, epoch, target, revision)
    for relation in (
        "groundloop_m5_owner_pending_counter",
        "groundloop_m5_answer_pending_counter",
    ):
        connection.execute(
            sql.SQL("UPDATE {} SET updated_revision=%s WHERE epoch_id=%s").format(
                sql.Identifier(relation)
            ),
            (revision + 1, epoch),
        )
    total = M5RuntimeWork(
        **dict(
            zip(
                work.counter_names(),
                (
                    a + b
                    for a, b in zip(
                        start.work.counter_values(), work.counter_values(), strict=True
                    )
                ),
                strict=True,
            )
        )
    )
    connection.execute(
        sql.SQL(
            "UPDATE groundloop_m5_runtime_work_accumulator SET "
            "{},work_digest=%s,updated_revision=%s WHERE epoch_id=%s"
        ).format(
            sql.SQL(",").join(
                sql.SQL("{}=%s").format(sql.Identifier(name))
                for name in work.counter_names()
            )
        ),
        (*total.counter_values(), total.work_digest, revision + 1, epoch),
    )
    counts = (
        "required",
        "postgres_server_execution",
        "postgres_lock_wait",
        "postgres_wal_bytes",
        "postgres_shared_block_reads",
    )
    assignments = []
    for prefix in counts:
        assignments.extend(
            (
                sql.SQL("{}={}+1").format(
                    sql.Identifier(prefix + "_expected_count"),
                    sql.Identifier(prefix + "_expected_count"),
                ),
                sql.SQL("{}={}+%s").format(
                    sql.Identifier(prefix + "_missing_count"),
                    sql.Identifier(prefix + "_missing_count"),
                ),
            )
        )
    connection.execute(
        sql.SQL(
            "UPDATE groundloop_m5_runtime_timing_accumulator SET "
            "{},pending_contribution_kind=%s,pending_source_id=%s,"
            "pending_contribution_key_digest=%s,pending_anchor_revision=%s,"
            "updated_revision=%s WHERE epoch_id=%s"
        ).format(sql.SQL(",").join(assignments)),
        (
            *([int(start.timing.has_pending_anchor)] * 5),
            "semantic_readiness",
            target,
            anchor.contribution_key_digest,
            revision + 1,
            revision + 1,
            epoch,
        ),
    )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()


def test_prior_migration_hashes_are_unchanged() -> None:
    expected = dict(FROZEN_MIGRATION_HASHES)
    expected["migrations/000_extensions.sql"] = (
        "14230448e66ee34c4f05847cdc777b8eea14d7ee24d58dac8dac22d5c1317774"
    )
    expected["migrations/019_m5_preterminal_seal_context.sql"] = (
        "f92c02a365ac43a26f3291718866436b19f69924eb7a8c2b77af0312f82b536e"
    )
    for label, digest in expected.items():
        assert hashlib.sha256((ROOT / label).read_bytes()).hexdigest() == digest


def test_exact_sql_inventory_and_old_branches() -> None:
    statements = dict(_statements())
    assert len(statements) == 11
    source = MIGRATION.read_text()
    assert len(re.findall(r"^CREATE FUNCTION", source, re.M)) == 1
    assert len(re.findall(r"^CREATE OR REPLACE FUNCTION", source, re.M)) == 2
    assert len(re.findall(r"^CREATE TRIGGER", source, re.M)) == 1
    assert len(re.findall(r"^ALTER TABLE", source, re.M)) == 6
    assert not re.search(r"^(CREATE TABLE|CREATE INDEX|GRANT|REVOKE)", source, re.M)
    old = (ROOT / "migrations/016_m5_runtime_recovery.sql").read_text()
    old_timing = re.search(
        r"CREATE FUNCTION groundloop_m5_validate_timing_accumulator\(\).*?\n\$\$;",
        old,
        re.S,
    )
    assert old_timing is not None
    assert statements["timing_validator"] == old_timing.group().replace(
        "CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1
    ).replace(
        "'verifier_completion', 'cancellation', 'preterminal_late_return'\n          )",
        "'verifier_completion', 'cancellation', 'preterminal_late_return',\n        "
        "      'semantic_readiness'\n          )",
    )
    old_work = re.search(
        r"CREATE FUNCTION groundloop_m5_validate_work_contribution\(\).*?\n\$\$;",
        old,
        re.S,
    )
    assert old_work is not None
    candidate = statements["work_validator"].replace(
        "CREATE OR REPLACE FUNCTION", "CREATE FUNCTION", 1
    )
    start = candidate.index("    readiness_row record;")
    end = candidate.index("BEGIN\n", start)
    candidate = candidate[:start] + candidate[end:]
    candidate = candidate.replace(
        "        WHEN 'semantic_readiness' THEN ARRAY[26, 27]\n", ""
    )
    start = candidate.index("    IF NEW.contribution_kind = 'semantic_readiness' THEN")
    end = candidate.index("    RETURN NULL;\nEND;\n$$;", start)
    candidate = candidate[:start] + candidate[end:]
    assert candidate == old_work.group()


def test_raw_catalog_flags_and_two_manual_edges(
    raw_database: tuple[Connection[Any], str],
) -> None:
    connection, schema = raw_database
    rows = connection.execute(
        "SELECT p.proname,p.proconfig,p.prosecdef FROM pg_proc p JOIN pg_namespace "
        "n ON n.oid=p.pronamespace WHERE n.nspname=%s AND p.proname=ANY(%s) ORDER "
        "BY p.proname",
        (schema, list(FUNCTIONS)),
    ).fetchall()
    assert rows == [
        (FUNCTIONS[0], [f"search_path={schema}, pg_catalog"], False),
        (FUNCTIONS[2], None, False),
        (FUNCTIONS[1], None, False),
    ]
    connection.commit()
    epoch, _, _ = _manual_open(connection)
    _manual_edge(connection, epoch, "semantic_pending", 1)
    _manual_edge(connection, epoch, "semantic_complete", 2)
    assert connection.execute(
        "SELECT revision,runtime_state FROM groundloop_m5_runtime_epoch WHERE "
        "epoch_id=%s",
        (epoch,),
    ).fetchone() == (3, "semantic_complete")
    assert connection.execute(
        "SELECT required_expected_count,required_missing_count,pending_source_id "
        "FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id=%s",
        (epoch,),
    ).fetchone() == (3, 2, "semantic_complete")


@pytest.mark.parametrize(
    "mutation",
    (
        "source",
        "key",
        "work_bytes",
        "foreign_counter",
        "revision",
        "target",
        "after_advance",
        "wrong_kind",
        "half_terminal",
    ),
)
def test_predecessor_rejects_invalid_images(
    raw_database: tuple[Connection[Any], str], mutation: str
) -> None:
    connection, _ = raw_database
    epoch, _, _ = _manual_open(connection)
    work, anchor, source = _edge_values(connection, epoch, "semantic_pending", 1)
    if mutation == "source":
        source = "0" * 64
    elif mutation == "key":
        object.__setattr__(anchor, "contribution_key_digest", "0" * 64)
    elif mutation == "work_bytes":
        work = M5RuntimeWork(
            bytes_hashed=work.bytes_hashed + 1, bytes_serialized=work.bytes_serialized
        )
    elif mutation == "foreign_counter":
        work = M5RuntimeWork(
            bytes_hashed=work.bytes_hashed,
            bytes_serialized=work.bytes_serialized,
            public_delta_count=1,
        )
    elif mutation == "revision":
        object.__setattr__(anchor, "anchor_revision", 3)
    elif mutation == "target":
        object.__setattr__(anchor, "source_id", "semantic_complete")
    elif mutation == "after_advance":
        _advance(connection, epoch, "semantic_pending", 1)
    elif mutation == "wrong_kind":
        connection.execute(
            "UPDATE groundloop_m5_update SET update_kind='policy_change' WHERE "
            "epoch_id=%s",
            (epoch,),
        )
    elif mutation == "half_terminal":
        connection.execute(
            "UPDATE groundloop_epoch SET "
            "semantic_status='complete',evaluation_state='complete' WHERE "
            "epoch_id=%s",
            (epoch,),
        )
    with pytest.raises(psycopg.Error, match="M5 readiness"):
        _insert(connection, work, anchor, source)
    connection.rollback()
    assert connection.execute(
        "SELECT revision FROM groundloop_m5_runtime_epoch WHERE epoch_id=%s", (epoch,)
    ).fetchone() == (1,)


def test_guard_does_not_read_shadow_temp_epoch(
    raw_database: tuple[Connection[Any], str],
) -> None:
    connection, _ = raw_database
    epoch, _, _ = _manual_open(connection)
    work, anchor, source = _edge_values(connection, epoch, "semantic_pending", 1)
    connection.execute("CREATE TEMP TABLE groundloop_epoch (epoch_id bigint)")
    connection.execute(
        "CREATE TEMP TABLE groundloop_m5_runtime_epoch (epoch_id bigint)"
    )
    _insert(connection, work, anchor, source)
    # Guarded insertion succeeds; no authored full history is committed.
    connection.rollback()


def test_deferred_validator_requires_real_result(
    raw_database: tuple[Connection[Any], str],
) -> None:
    connection, _ = raw_database
    epoch, _, _ = _manual_open(connection)
    work, anchor, source = _edge_values(connection, epoch, "semantic_pending", 1)
    _insert(connection, work, anchor, source)
    with pytest.raises(psycopg.Error, match="exact current revision"):
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.rollback()


def _install_runtime_schema(database: M5RuntimeDatabase) -> None:
    # The old recovery fixture lacks the independently derived direct-v1 image
    # required by migration 017. Complete that baseline, not its readiness.
    revision = database.connection.execute(
        "SELECT revision FROM groundloop_epoch WHERE epoch_id=%s",
        (database.base.epoch_id,),
    ).fetchone()
    assert revision is not None
    _seed_b3_direct_m4_snapshot(
        database.connection,
        epoch_id=database.base.epoch_id,
        revision=int(revision[0]),
    )
    database.connection.commit()
    for installer in (
        install_m5_persisted_matching_bundle,
        install_m5_bounded_document_withdrawal_bundle,
        install_m5_preterminal_seal_context_bundle,
    ):
        installer(database.connection)
    _raw_install(database.connection, database.schema_name)


def test_guard_accepts_actual_root_closure(m5_runtime_db: M5RuntimeDatabase) -> None:
    _install_runtime_schema(m5_runtime_db)
    plan = m5_runtime_db.register_plan(event_id="d32-real-root-close")
    store = PostgresM5RuntimeStore(m5_runtime_db.connection)
    opened = _open_recovery_event(store, m5_runtime_db, plan)
    roots = _roots(plan, m5_runtime_db.manifest)
    revision = 1
    for _, job in roots:
        lease = _acquire(
            m5_runtime_db.connection,
            epoch_id=opened.epoch_id,
            expected_revision=revision,
            job=job,
        )
        revision += 1
        result = _result(epoch_id=opened.epoch_id, root=job, pairs=())
        _stage(
            m5_runtime_db.connection,
            epoch_id=opened.epoch_id,
            expected_revision=revision,
            lease=lease,
            job=job,
            result=result,
        )
        revision += 1
    barrier = store.close_m5_requirement_roots_atomically(
        opened.epoch_id, revision, _root_set_hash(roots)
    )
    _manual_edge(
        m5_runtime_db.connection,
        opened.epoch_id,
        "semantic_complete",
        barrier.resulting_revision,
    )


@pytest.mark.parametrize("corrupt_scope", (False, True))
def test_guard_checks_actual_cancellation_artifact(
    m5_runtime_db: M5RuntimeDatabase,
    monkeypatch: pytest.MonkeyPatch,
    corrupt_scope: bool,
) -> None:
    from groundloop.m5.runtime import postgres_roots

    _install_runtime_schema(m5_runtime_db)
    plan = m5_runtime_db.register_plan(event_id="d32-real-cancel")
    store = PostgresM5RuntimeStore(m5_runtime_db.connection)
    opened = _open_recovery_event(store, m5_runtime_db, plan)
    jobs = tuple(job.logical_job_id for _, job in _roots(plan, m5_runtime_db.manifest))
    cancellation = M5CancellationPlan.build(
        epoch_id=opened.epoch_id,
        structural_event_id=plan.structural_event_id,
        reason=M5TerminalReason.SCOPE_RETIRED,
        cancelled_job_ids=jobs,
    )
    if corrupt_scope:
        # Fault-inject the scope output during the real cancellation transaction;
        # job artifacts remain correct. No trigger/constraint is disabled.
        original = postgres_roots.M5JobCompletion.build
        completions: list[Any] = []

        def capture(**kwargs: Any) -> Any:
            completion = original(**kwargs)
            completions.append(completion)
            return completion

        monkeypatch.setattr(postgres_roots.M5JobCompletion, "build", capture)

        def inject(point: str) -> None:
            if point == "cancellation_jobs_closed":
                for completion in completions:
                    object.__setattr__(completion, "completion_digest", "0" * 64)

        store.cancel_m5_work_atomically(
            opened.epoch_id, 1, cancellation, failure_injector=inject
        )
        work, anchor, source = _edge_values(
            m5_runtime_db.connection, opened.epoch_id, "semantic_complete", 2
        )
        with pytest.raises(psycopg.Error, match="unclosed work"):
            _insert(m5_runtime_db.connection, work, anchor, source)
        m5_runtime_db.connection.rollback()
    else:
        cancelled = store.cancel_m5_work_atomically(opened.epoch_id, 1, cancellation)
        _manual_edge(
            m5_runtime_db.connection,
            opened.epoch_id,
            "semantic_complete",
            cancelled.resulting_revision,
        )


def test_guard_rejects_cross_reason_cancellation_witness(
    m5_runtime_db: M5RuntimeDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fault-inject two reason groups with a valid artifact for only one."""
    from groundloop.m5.runtime import postgres_roots

    _install_runtime_schema(m5_runtime_db)
    plan = m5_runtime_db.register_plan(event_id="d32-cross-reason")
    store = PostgresM5RuntimeStore(m5_runtime_db.connection)
    opened = _open_recovery_event(store, m5_runtime_db, plan)
    jobs = tuple(job for _, job in _roots(plan, m5_runtime_db.manifest))
    assert len(jobs) == 2
    full_plan = M5CancellationPlan.build(
        epoch_id=opened.epoch_id,
        structural_event_id=plan.structural_event_id,
        reason=M5TerminalReason.SCOPE_RETIRED,
        cancelled_job_ids=tuple(job.logical_job_id for job in jobs),
    )
    first_plan = M5CancellationPlan.build(
        epoch_id=opened.epoch_id,
        structural_event_id=plan.structural_event_id,
        reason=M5TerminalReason.SCOPE_RETIRED,
        cancelled_job_ids=(jobs[0].logical_job_id,),
    )
    second_completion = M5JobCompletion.build(
        job=jobs[1],
        terminal_state=M5JobState.CANCELLED,
        archive_reason=M5TerminalReason.SUBJECT_INACTIVE,
    )
    original_persist = postgres_roots._persist_new_cancellation
    original_contribution = postgres_roots.persist_cancellation_contribution
    original_finish = postgres_roots.finish_cancellation_accounting

    class FaultCursor:
        def __init__(self, cursor: Any) -> None:
            self.cursor = cursor

        def execute(self, query: Any, params: Any = None) -> Any:
            rendered = query if isinstance(query, str) else query.as_string(self.cursor)
            selected = params
            if (
                "UPDATE groundloop_m5_semantic_job" in rendered
                and params[8] == jobs[1].logical_job_id
            ):
                selected = list(params)
                selected[0] = selected[4] = "subject_inactive"
                selected[1] = second_completion.completion_digest
            elif (
                "UPDATE groundloop_m5_discovery_scope" in rendered
                and params[4] == jobs[1].logical_job_id
            ):
                selected = list(params)
                selected[0] = second_completion.completion_digest
            return self.cursor.execute(query, selected)

        def __getattr__(self, name: str) -> Any:
            return getattr(self.cursor, name)

    def persist(cursor: Any, **kwargs: Any) -> Any:
        return original_persist(FaultCursor(cursor), **kwargs)

    def contribution(cursor: Any, **kwargs: Any) -> Any:
        kwargs["plan_digest"] = first_plan.plan_digest
        kwargs["cancellation_work"] = M5RuntimeWork(requirement_cancelled_job_count=1)
        return original_contribution(cursor, **kwargs)

    def finish(cursor: Any, **kwargs: Any) -> Any:
        kwargs["cancellation_work"] = M5RuntimeWork(requirement_cancelled_job_count=1)
        return original_finish(cursor, **kwargs)

    monkeypatch.setattr(postgres_roots, "_persist_new_cancellation", persist)
    monkeypatch.setattr(
        postgres_roots, "persist_cancellation_contribution", contribution
    )
    monkeypatch.setattr(postgres_roots, "finish_cancellation_accounting", finish)
    # The unchanged old validators admit the reason-A artifact without proving
    # reason-B reverse coverage. The new guard must reject that predecessor.
    store.cancel_m5_work_atomically(opened.epoch_id, 1, full_plan)
    work, anchor, source = _edge_values(
        m5_runtime_db.connection, opened.epoch_id, "semantic_complete", 2
    )
    with pytest.raises(psycopg.Error, match="cancellation group lacks its exact plan"):
        _insert(m5_runtime_db.connection, work, anchor, source)
    m5_runtime_db.connection.rollback()


@pytest.mark.parametrize("fault", ("null_reason", "future_revision"))
def test_guard_checks_terminal_verifier_predecessor(
    m5_runtime_db: M5RuntimeDatabase,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    from groundloop.m5.runtime import postgres_roots

    _install_runtime_schema(m5_runtime_db)
    plan = m5_runtime_db.register_plan(event_id="d32-child-cutoff")
    store = PostgresM5RuntimeStore(m5_runtime_db.connection)
    opened = _open_recovery_event(store, m5_runtime_db, plan)
    roots = _roots(plan, m5_runtime_db.manifest)
    revision = 1
    for index, (scope, job) in enumerate(roots):
        lease = _acquire(
            m5_runtime_db.connection,
            epoch_id=opened.epoch_id,
            expected_revision=revision,
            job=job,
        )
        revision += 1
        pairs = (
            ()
            if index
            else (
                SemanticPairKey(
                    SubjectKind.REQUIREMENT,
                    scope.requirement_version_id or "",
                    m5_runtime_db.base.chunk_ids[0],
                ),
            )
        )
        _stage(
            m5_runtime_db.connection,
            epoch_id=opened.epoch_id,
            expected_revision=revision,
            lease=lease,
            job=job,
            result=_result(epoch_id=opened.epoch_id, root=job, pairs=pairs),
        )
        revision += 1
    barrier = store.close_m5_requirement_roots_atomically(
        opened.epoch_id, revision, _root_set_hash(roots)
    )
    revision = barrier.resulting_revision
    child = m5_runtime_db.connection.execute(
        "SELECT logical_job_id FROM groundloop_m5_semantic_job WHERE epoch_id=%s "
        "AND parent_job_id IS NOT NULL",
        (opened.epoch_id,),
    ).fetchone()
    assert child is not None
    m5_runtime_db.connection.commit()
    cancellation = M5CancellationPlan.build(
        epoch_id=opened.epoch_id,
        structural_event_id=plan.structural_event_id,
        reason=M5TerminalReason.SCOPE_RETIRED,
        cancelled_job_ids=(str(child[0]),),
    )
    original = postgres_roots._persist_new_cancellation

    class FaultCursor:
        def __init__(self, cursor: Any) -> None:
            self.cursor = cursor

        def execute(self, query: Any, params: Any = None) -> Any:
            rendered = query if isinstance(query, str) else query.as_string(self.cursor)
            selected = params
            if "UPDATE groundloop_m5_semantic_job" in rendered:
                selected = list(params)
                if fault == "null_reason":
                    selected[4] = None
                else:
                    selected[5] = revision + 2
            return self.cursor.execute(query, selected)

        def __getattr__(self, name: str) -> Any:
            return getattr(self.cursor, name)

    def persist(cursor: Any, **kwargs: Any) -> Any:
        return original(FaultCursor(cursor), **kwargs)

    monkeypatch.setattr(postgres_roots, "_persist_new_cancellation", persist)

    def inspect_predecessor(point: str) -> None:
        if point == "cancellation_accounting_finished":
            work, anchor, source = _edge_values(
                m5_runtime_db.connection,
                opened.epoch_id,
                "semantic_complete",
                revision + 1,
            )
            _insert(m5_runtime_db.connection, work, anchor, source)

    with pytest.raises(psycopg.Error, match="unclosed work"):
        store.cancel_m5_work_atomically(
            opened.epoch_id,
            revision,
            cancellation,
            failure_injector=inspect_predecessor,
        )
    assert m5_runtime_db.connection.execute(
        "SELECT job_state FROM groundloop_m5_semantic_job WHERE logical_job_id=%s",
        (str(child[0]),),
    ).fetchone() == ("declared",)


def _catalog_images(
    connection: Connection[Any], schema: str
) -> dict[str, dict[tuple[Any, ...], tuple[Any, ...]]]:
    queries = {
        "relations": (
            "SELECT "
            "c.relname,c.relkind,c.relpersistence,c.relowner,c.relacl,"
            "c.reloptions FROM pg_class c JOIN pg_namespace n ON "
            "n.oid=c.relnamespace WHERE n.nspname=%s"
        ),
        "columns": (
            "SELECT "
            "c.relname,a.attname,a.atttypid,a.atttypmod,a.attnotnull,"
            "a.attidentity,a.attgenerated,a.attnum,pg_get_expr(d.adbin,"
            "d.adrelid) FROM pg_attribute a JOIN pg_class c ON "
            "c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace "
            "LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum "
            "WHERE n.nspname=%s AND a.attnum>0 AND NOT a.attisdropped"
        ),
        "functions": (
            "SELECT "
            "p.proname,pg_get_function_identity_arguments(p.oid),p.proowner,"
            "p.prosecdef,p.proconfig,p.proacl,p.provolatile,p.proisstrict,"
            "p.proparallel,p.proleakproof,p.prosrc,p.prorettype,p.proretset,"
            "p.prokind,l.lanname FROM pg_proc p JOIN pg_namespace n ON "
            "n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang WHERE "
            "n.nspname=%s"
        ),
        "constraints": (
            "SELECT "
            "c.relname,k.conname,k.contype,k.convalidated,k.condeferrable,"
            "k.condeferred,pg_get_constraintdef(k.oid) FROM pg_constraint k "
            "JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON "
            "n.oid=c.relnamespace WHERE n.nspname=%s"
        ),
        "triggers": (
            "SELECT "
            "c.relname,t.tgname,t.tgtype,t.tgenabled,t.tgdeferrable,"
            "t.tginitdeferred,t.tgisinternal,t.tgattr::text,t.tgqual,t.tgargs,"
            "p.proname FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON "
            "p.oid=t.tgfoid WHERE n.nspname=%s AND NOT t.tgisinternal"
        ),
        "indices": (
            "SELECT c.relname,pg_get_indexdef(i.indexrelid) FROM pg_index i "
            "JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON "
            "n.oid=c.relnamespace WHERE n.nspname=%s"
        ),
    }
    result = {}
    for name, query in queries.items():
        width = 1 if name in {"relations", "indices"} else 2
        result[name] = {
            tuple(row[:width]): tuple(row[width:])
            for row in connection.execute(query, (schema,)).fetchall()
        }
    return result


def test_raw_catalog_delta_is_exact() -> None:
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        before = _catalog_images(connection, schema)
        connection.commit()
        _raw_install(connection, schema)
        after = _catalog_images(connection, schema)
        for category in ("relations", "columns", "indices"):
            assert after[category] == before[category]
        for category in ("functions", "constraints", "triggers"):
            changed = {
                key
                for key in before[category]
                if after[category].get(key) != before[category][key]
            }
            added = set(after[category]) - set(before[category])
            removed = set(before[category]) - set(after[category])
            assert not removed
            if category == "functions":
                assert changed == {(FUNCTIONS[1], ""), (FUNCTIONS[2], "")}
                assert added == {(FUNCTIONS[0], "")}
                for key in changed:
                    old = list(before[category][key])
                    new = list(after[category][key])
                    # prosrc is the sole changed metadata field.
                    old[8] = new[8]
                    assert old == new
            elif category == "constraints":
                assert len(changed) == 3 and not added
                for key in changed:
                    old = list(before[category][key])
                    new = list(after[category][key])
                    assert (
                        new[-1].replace(", 'semantic_readiness'::text", "") == old[-1]
                    )
                    old[-1] = new[-1]
                    assert old == new
            else:
                assert not changed
                assert added == {
                    (
                        "groundloop_m5_runtime_work_contribution",
                        "groundloop_m5_semantic_readiness_predecessor",
                    )
                }
        _m5_readiness_verify_catalog(connection, schema_name=schema, installed=True)
        connection.rollback()


def test_production_installer_and_readonly_replay() -> None:
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        before = _catalog_images(connection, schema)
        old_ledger = _all_ledger_rows(connection)
        mode = connection.execute("SELECT * FROM groundloop_runtime_mode").fetchall()
        connection.commit()
        identity = m5_semantic_readiness_bundle_identity()
        assert (
            identity.migration_sha256
            == ("df0a3c0c9b228a4a22903479896326d27fbd6f98f5878e34d73182ba007bf837")
            == M5_ACCEPTED_SEMANTIC_READINESS_MIGRATION_SHA256
        )
        assert (
            identity.bundle_sha256
            == ("b7706feb7d54fcf9fdb4f9f38350a32967e0b460229b6f264493fb428be8ddfc")
            == M5_ACCEPTED_SEMANTIC_READINESS_BUNDLE_SHA256
        )
        assert install_m5_semantic_readiness_bundle(connection).applied
        after = _catalog_images(connection, schema)
        new_ledger = _all_ledger_rows(connection)
        assert (
            tuple(row for row in new_ledger if row[0] != identity.bundle_id)
            == old_ledger
        )
        assert len(new_ledger) == len(old_ledger) + 1
        assert (
            connection.execute("SELECT * FROM groundloop_runtime_mode").fetchall()
            == mode
        )
        for category in ("relations", "columns", "indices"):
            assert after[category] == before[category]
        connection.commit()
        assert not install_m5_semantic_readiness_bundle(connection).applied
        assert _catalog_images(connection, schema) == after
        assert _all_ledger_rows(connection) == new_ledger
        connection.commit()
        with connection.transaction():
            connection.execute("SET TRANSACTION READ ONLY")
            require_m5_semantic_readiness_bundle(connection, schema_name=schema)
        assert _all_ledger_rows(connection) == new_ledger
        assert M5_SEMANTIC_READINESS_BUNDLE_ID == identity.bundle_id


def test_installer_every_cut_rolls_back_catalog_and_ledger() -> None:
    points = (
        "after_initial_ledger",
        "after_prerequisite",
        "after_install_lock",
        *(
            point
            for name, _ in _statements()
            for point in (f"before_{name}", f"after_{name}")
        ),
        "before_ledger",
        "after_ledger",
    )
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        before = _catalog_images(connection, schema)
        ledger = _all_ledger_rows(connection)
        connection.commit()
        for index, selected in enumerate(points):
            observed: list[str] = []

            def fail(
                point: str, seen: list[str] = observed, expected: str = selected
            ) -> None:
                seen.append(point)
                if point == expected:
                    raise RuntimeError(f"020 injected {point}")

            with pytest.raises(RuntimeError, match=re.escape(selected)):
                install_m5_semantic_readiness_bundle(connection, failure_injector=fail)
            assert connection.info.transaction_status == TransactionStatus.IDLE
            assert observed == list(points[: index + 1])
            assert _catalog_images(connection, schema) == before
            assert _all_ledger_rows(connection) == ledger
            connection.commit()
        assert install_m5_semantic_readiness_bundle(connection).applied


def test_installer_lock_order_replay_without_dml_and_ledger_first_conflict() -> None:
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        recorded = _RecordingConnection(connection)
        assert install_m5_semantic_readiness_bundle(recorded).applied  # type: ignore[arg-type]
        assert [q for q in recorded.statements if q.startswith("LOCK TABLE")] == [
            f'LOCK TABLE "{schema}"."{name}" IN SHARE ROW EXCLUSIVE MODE'
            for name in (
                "groundloop_m5_schema_bundle",
                "groundloop_epoch",
                "groundloop_m5_runtime_epoch",
                "groundloop_m5_runtime_work_contribution",
                "groundloop_m5_transition_call_timing",
                "groundloop_m5_runtime_timing_accumulator",
            )
        ]
        ledger = _all_ledger_rows(connection)
        connection.commit()
        with psycopg.connect(_database_url()) as blocker:
            blocker.execute(
                sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(
                    sql.Identifier(schema, "groundloop_m5_schema_bundle")
                )
            )
            connection.execute("SET lock_timeout='1s'")
            connection.commit()
            replay = _RecordingConnection(connection)
            assert not install_m5_semantic_readiness_bundle(replay).applied  # type: ignore[arg-type]
            assert not any(
                re.match(r"(?:LOCK|INSERT|UPDATE|DELETE|ALTER|CREATE|DROP)\b", q)
                for q in replay.statements
            )
            assert _all_ledger_rows(connection) == ledger
            connection.commit()
            conflict_points: list[str] = []
            with pytest.raises(M5BundleHashConflictError):
                install_m5_semantic_readiness_bundle(
                    connection,
                    migration_bytes=MIGRATION.read_bytes() + b"-- altered\n",
                    failure_injector=conflict_points.append,
                )
            assert not conflict_points
            assert _all_ledger_rows(connection) == ledger
            connection.commit()


def test_installer_wrong_bytes_and_connection_gates() -> None:
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        before = _catalog_images(connection, schema)
        connection.commit()
        points: list[str] = []
        with pytest.raises(M5SemanticReadinessBundleError, match="H32 authority"):
            install_m5_semantic_readiness_bundle(
                connection,
                migration_bytes=b"-- altered\n",
                failure_injector=points.append,
            )
        assert points == ["after_initial_ledger"]
        assert _catalog_images(connection, schema) == before
        connection.commit()
        connection.execute("SELECT 1")
        with pytest.raises(M5SemanticReadinessBundleError, match="idle"):
            install_m5_semantic_readiness_bundle(connection)
        connection.rollback()
        for level in (IsolationLevel.REPEATABLE_READ, IsolationLevel.SERIALIZABLE):
            connection.isolation_level = level
            with pytest.raises(M5SemanticReadinessBundleError, match="READ COMMITTED"):
                install_m5_semantic_readiness_bundle(connection)
        connection.isolation_level = None
        connection.read_only = True
        with pytest.raises(M5SemanticReadinessBundleError, match="read-write"):
            install_m5_semantic_readiness_bundle(connection)
        connection.read_only = None
        assert connection.info.transaction_status == TransactionStatus.IDLE


@pytest.mark.parametrize(
    "alteration",
    [
        "ALTER FUNCTION groundloop_m5_validate_work_contribution() SECURITY DEFINER",
        "ALTER FUNCTION groundloop_m5_matching_private_temp_triplet"
        "(regclass,regclass,regclass) SECURITY INVOKER",
        "DROP TRIGGER groundloop_m5_runtime_epoch_transition "
        "ON groundloop_m5_runtime_epoch",
        "DROP TRIGGER groundloop_m5_runtime_epoch_counter_integrity "
        "ON groundloop_m5_runtime_epoch",
        "CREATE FUNCTION groundloop_m5_validate_semantic_readiness_predecessor() "
        "RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'",
        "ALTER TABLE groundloop_m5_runtime_work_contribution DROP CONSTRAINT "
        "groundloop_m5_runtime_work_contribution_contribution_kind_check",
    ],
)
def test_installer_rejects_partial_or_altered_prior_catalog(alteration: str) -> None:
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        connection.execute(alteration)
        connection.commit()
        before = _catalog_images(connection, schema)
        ledger = _all_ledger_rows(connection)
        connection.commit()
        with pytest.raises(M5SemanticReadinessBundleError):
            install_m5_semantic_readiness_bundle(connection)
        assert _catalog_images(connection, schema) == before
        assert _all_ledger_rows(connection) == ledger


@pytest.mark.parametrize(
    "alteration",
    [
        "ALTER FUNCTION groundloop_m5_validate_semantic_readiness_predecessor() "
        "SECURITY DEFINER",
        "ALTER FUNCTION groundloop_m5_validate_semantic_readiness_predecessor() "
        "SET search_path TO public",
        "REVOKE EXECUTE ON FUNCTION "
        "groundloop_m5_validate_semantic_readiness_predecessor() "
        "FROM PUBLIC",
        "DROP TRIGGER groundloop_m5_semantic_readiness_predecessor "
        "ON groundloop_m5_runtime_work_contribution",
        "DROP TRIGGER groundloop_m5_runtime_epoch_transition "
        "ON groundloop_m5_runtime_epoch",
        "ALTER FUNCTION groundloop_m5_matching_read_preterminal_seal_context"
        "(bigint,bigint,bigint) SECURITY INVOKER",
    ],
)
def test_replay_rejects_altered_authority(alteration: str) -> None:
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        install_m5_semantic_readiness_bundle(connection)
        connection.execute(alteration)
        connection.commit()
        before = _catalog_images(connection, schema)
        ledger = _all_ledger_rows(connection)
        connection.commit()
        with pytest.raises(M5SemanticReadinessBundleError):
            install_m5_semantic_readiness_bundle(connection)
        assert _catalog_images(connection, schema) == before
        assert _all_ledger_rows(connection) == ledger


def test_all_five_fields_of_every_prerequisite_are_exact() -> None:
    """Bad permanent ledger fixtures are negatives, never readiness histories."""
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        rows = _all_ledger_rows(connection)
        assert len(rows) == 6
        connection.commit()
        bad_schema = "d32_bad_ledger_" + uuid.uuid4().hex
        with psycopg.connect(_database_url(), autocommit=True) as admin:
            admin.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(bad_schema))
            )
            try:
                admin.execute(
                    sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING ALL)").format(
                        sql.Identifier(bad_schema, "groundloop_m5_schema_bundle"),
                        sql.Identifier(schema, "groundloop_m5_schema_bundle"),
                    )
                )
                with psycopg.connect(_database_url()) as bad:
                    _select_schema(bad, bad_schema)
                    for selected in range(len(rows)):
                        for field in range(5):
                            for index, row in enumerate(rows):
                                values = list(row[:5])
                                if index == selected:
                                    values[field] = (
                                        values[field] + "-missing"
                                        if field == 0
                                        else "f" * 64
                                    )
                                bad.execute(
                                    "INSERT INTO groundloop_m5_schema_bundle "
                                    "(bundle_id,bundle_sha256,migration_sha256,"
                                    "oracle_sha256,prerequisite_sha256) "
                                    "VALUES(%s,%s,%s,%s,%s)",
                                    values,
                                )
                            bad.commit()
                            with pytest.raises(M5PrerequisiteError):
                                install_m5_semantic_readiness_bundle(bad)
                            assert bad.info.transaction_status == TransactionStatus.IDLE
                            bad.execute("DELETE FROM groundloop_m5_schema_bundle")
                            bad.commit()
            finally:
                admin.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(bad_schema))
                )


def test_installer_captures_schema_and_rejects_fallback_or_temp_authority() -> None:
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        other = "d32_empty_" + uuid.uuid4().hex
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(other)))
        connection.execute(
            sql.SQL("SET search_path TO {}, {}, public").format(
                sql.Identifier(other), sql.Identifier(schema)
            )
        )
        connection.commit()
        try:
            with pytest.raises(psycopg.errors.UndefinedTable):
                install_m5_semantic_readiness_bundle(connection)
            connection.execute(
                "CREATE TEMP TABLE groundloop_m5_schema_bundle (bundle_id text)"
            )
            connection.execute(
                sql.SQL("SET search_path TO pg_temp, {}, public").format(
                    sql.Identifier(schema)
                )
            )
            connection.commit()
            with pytest.raises(
                M5SemanticReadinessBundleError, match="trusted permanent"
            ):
                install_m5_semantic_readiness_bundle(connection)
            _select_schema(connection, schema)
            assert install_m5_semantic_readiness_bundle(connection).applied
            assert connection.execute(
                "SELECT count(*) FROM pg_temp.groundloop_m5_schema_bundle"
            ).fetchone() == (0,)
            with pytest.raises(M5SemanticReadinessBundleError):
                require_m5_semantic_readiness_bundle(connection, schema_name=other)
            connection.rollback()
        finally:
            connection.rollback()
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(other))
            )
            connection.commit()


@pytest.mark.parametrize("pause_at", ["after_initial_ledger", "after_install_lock"])
def test_two_installers_serialize_and_replay_in_both_orders(pause_at: str) -> None:
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        reached = threading.Event()
        resume = threading.Event()
        second_started = threading.Event()
        results: list[bool] = []
        errors: list[BaseException] = []
        second_pid: list[int] = []

        def run(paused: bool) -> None:
            try:
                with psycopg.connect(_database_url()) as worker:
                    _select_schema(worker, schema)
                    if not paused:
                        second_pid.append(worker.info.backend_pid)

                    def cut(point: str) -> None:
                        if paused and point == pause_at:
                            reached.set()
                            if not resume.wait(30):
                                raise TimeoutError("020 installer resume timed out")
                        if not paused and point == "after_prerequisite":
                            second_started.set()

                    results.append(
                        install_m5_semantic_readiness_bundle(
                            worker, failure_injector=cut
                        ).applied
                    )
            except BaseException as error:
                errors.append(error)

        first = threading.Thread(target=run, args=(True,), daemon=True)
        second = threading.Thread(target=run, args=(False,), daemon=True)
        first.start()
        try:
            assert reached.wait(30)
            second.start()
            assert second_started.wait(30)
            if pause_at == "after_initial_ledger":
                second.join(30)
                assert not second.is_alive() and results == [True]
            else:
                # Observe the actual conflicting permanent ledger lock request,
                # not merely thread scheduling or a sleep-based race assumption.
                with psycopg.connect(_database_url(), autocommit=True) as observer:
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        waiting = observer.execute(
                            "SELECT count(*) FROM pg_catalog.pg_locks "
                            "WHERE pid=%s AND mode='ShareRowExclusiveLock' "
                            "AND NOT granted",
                            (second_pid[0],),
                        ).fetchone()
                        if waiting == (1,):
                            break
                        time.sleep(0.01)
                    assert waiting == (1,)
                assert first.is_alive() and second.is_alive() and not results
            with pytest.raises(
                (M5SemanticReadinessBundleError, M5BundleHashConflictError)
            ):
                install_m5_semantic_readiness_bundle(
                    connection, migration_bytes=b"-- conflicting\n"
                )
        finally:
            resume.set()
            first.join(30)
            if second.ident is not None:
                second.join(30)
        assert not first.is_alive() and not second.is_alive()
        assert not errors
        assert sorted(results) == [False, True]
        assert not install_m5_semantic_readiness_bundle(connection).applied


def test_unledgered_raw_020_is_not_adopted() -> None:
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        _raw_install(connection, schema)
        before = _catalog_images(connection, schema)
        ledger = _all_ledger_rows(connection)
        connection.commit()
        with pytest.raises(M5SemanticReadinessBundleError):
            install_m5_semantic_readiness_bundle(connection)
        assert _catalog_images(connection, schema) == before
        assert _all_ledger_rows(connection) == ledger


@pytest.mark.parametrize("installed", [False, True])
def test_same_body_wrong_signature_is_rejected(installed: bool) -> None:
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        if installed:
            install_m5_semantic_readiness_bundle(connection)
        row = connection.execute(
            "SELECT pg_catalog.pg_get_functiondef(p.oid) FROM pg_catalog.pg_proc p "
            "JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname=%s AND p.proname='groundloop_m5_matching_u64'",
            (schema,),
        ).fetchone()
        assert row is not None
        connection.execute("DROP FUNCTION groundloop_m5_matching_u64(bytea,integer)")
        altered = str(row[0]).replace("RETURNS bigint", "RETURNS text")
        assert altered != row[0]
        connection.execute(altered)
        connection.commit()
        before = _catalog_images(connection, schema)
        ledger = _all_ledger_rows(connection)
        connection.commit()
        with pytest.raises(M5SemanticReadinessBundleError, match="signatures"):
            install_m5_semantic_readiness_bundle(connection)
        assert _catalog_images(connection, schema) == before
        assert _all_ledger_rows(connection) == ledger


def test_raw_preflight_is_schema_bounded_during_foreign_constraint_churn() -> None:
    """Diagnostic only: raw candidate SQL, not acceptance of an unpushed pin."""
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        other = "d32_constraint_churn_" + uuid.uuid4().hex
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(other)))
        connection.commit()
        ready = threading.Event()
        finish = threading.Event()
        errors: list[BaseException] = []
        iterations: list[int] = []

        def churn() -> None:
            try:
                with psycopg.connect(_database_url(), autocommit=True) as worker:
                    while not finish.is_set():
                        worker.execute(
                            sql.SQL(
                                "CREATE TABLE {} (value integer CHECK (value>0))"
                            ).format(sql.Identifier(other, "volatile_check"))
                        )
                        ready.set()
                        worker.execute(
                            sql.SQL("DROP TABLE {}").format(
                                sql.Identifier(other, "volatile_check")
                            )
                        )
                        iterations.append(1)
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=churn, daemon=True)
        thread.start()
        try:
            assert ready.wait(10)
            with connection.transaction():
                connection.execute(
                    "SELECT set_config('search_path',"
                    "quote_ident(%s)||', pg_catalog',true)",
                    (schema,),
                )
                for _ in range(40):
                    connection.execute(_statements()[0][1])
            _raw_install(connection, schema)
            _m5_readiness_verify_catalog(connection, schema_name=schema, installed=True)
            connection.rollback()
        finally:
            finish.set()
            thread.join(10)
            connection.rollback()
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(other))
            )
            connection.commit()
        assert not thread.is_alive() and not errors
        assert iterations


@pytest.mark.parametrize("fault", ["duplicate", "renamed", "widened", "unvalidated"])
def test_closed_check_uniqueness_and_validation_fail_closed(fault: str) -> None:
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        table = "groundloop_m5_runtime_work_contribution"
        name = table + "_contribution_kind_check"
        row = connection.execute(
            "SELECT pg_catalog.pg_get_constraintdef(k.oid) "
            "FROM pg_catalog.pg_constraint k "
            "WHERE k.conrelid=%s::pg_catalog.regclass AND k.conname=%s",
            (schema + "." + table, name),
        ).fetchone()
        assert row is not None
        definition = str(row[0])
        if fault == "duplicate":
            connection.execute(
                sql.SQL("ALTER TABLE {} ADD CONSTRAINT ambiguous_kind {}").format(
                    sql.Identifier(table), sql.SQL(definition)
                )
            )
        elif fault == "renamed":
            connection.execute(
                sql.SQL("ALTER TABLE {} RENAME CONSTRAINT {} TO renamed_kind").format(
                    sql.Identifier(table), sql.Identifier(name)
                )
            )
        else:
            connection.execute(
                sql.SQL("ALTER TABLE {} DROP CONSTRAINT {}").format(
                    sql.Identifier(table), sql.Identifier(name)
                )
            )
            if fault == "widened":
                definition = definition.replace(
                    "'seal'::text", "'seal'::text, 'unapproved_kind'::text"
                )
            else:
                definition += " NOT VALID"
            connection.execute(
                sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} {}").format(
                    sql.Identifier(table), sql.Identifier(name), sql.SQL(definition)
                )
            )
        connection.commit()
        before = _catalog_images(connection, schema)
        ledger = _all_ledger_rows(connection)
        connection.commit()
        with pytest.raises(M5SemanticReadinessBundleError, match="CHECK"):
            install_m5_semantic_readiness_bundle(connection)
        assert _catalog_images(connection, schema) == before
        assert _all_ledger_rows(connection) == ledger


@pytest.mark.parametrize("installed", [False, True])
@pytest.mark.parametrize(
    "alteration",
    [
        "ALTER TABLE groundloop_m5_runtime_epoch DROP CONSTRAINT "
        "groundloop_m5_runtime_epoch_runtime_state_check",
        "ALTER TABLE groundloop_m5_schema_bundle DROP CONSTRAINT "
        "groundloop_m5_schema_bundle_pkey",
        "ALTER TABLE groundloop_m5_runtime_epoch DROP CONSTRAINT "
        "groundloop_m5_runtime_epoch_pkey CASCADE",
        "ALTER TABLE groundloop_m5_runtime_epoch DROP CONSTRAINT "
        "groundloop_m5_runtime_epoch_epoch_id_fkey",
        "ALTER TABLE groundloop_m5_runtime_epoch RENAME COLUMN runtime_state "
        "TO renamed_runtime_state",
        "DROP INDEX groundloop_m5_admitted_pair_by_chunk_edge",
        "CREATE TRIGGER unapproved_extra_trigger BEFORE INSERT "
        "ON groundloop_m5_runtime_work_contribution FOR EACH ROW EXECUTE FUNCTION "
        "groundloop_m5_validate_work_contribution()",
        "CREATE TABLE groundloop_unapproved_relation(id bigint)",
        "CREATE FUNCTION groundloop_unapproved_function() RETURNS integer "
        "LANGUAGE sql AS 'SELECT 1'",
        "CREATE UNIQUE INDEX unapproved_attached_index "
        "ON groundloop_m5_runtime_epoch(epoch_id)",
        "CREATE TABLE unapproved_inherited_child () "
        "INHERITS (groundloop_m5_schema_bundle)",
        "internal_disabled",
    ],
)
def test_retained_structural_catalog_fails_closed(
    installed: bool, alteration: str
) -> None:
    """All tampering is isolated negative evidence, never a runtime bypass."""
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        if installed:
            install_m5_semantic_readiness_bundle(connection)
        if alteration == "internal_disabled":
            row = connection.execute(
                "SELECT t.tgname FROM pg_catalog.pg_trigger t "
                "WHERE t.tgrelid=%s::pg_catalog.regclass AND t.tgisinternal "
                "ORDER BY t.tgname LIMIT 1",
                (schema + ".groundloop_m5_runtime_epoch",),
            ).fetchone()
            assert row is not None
            connection.execute(
                sql.SQL("ALTER TABLE {} DISABLE TRIGGER {}").format(
                    sql.Identifier(schema, "groundloop_m5_runtime_epoch"),
                    sql.Identifier(row[0]),
                )
            )
        else:
            connection.execute(alteration)
        if "groundloop_m5_schema_bundle_pkey" in alteration:
            # Without a PK fetchone() could silently select an authentic row
            # while a second, conflicting immutable authority row exists.
            connection.execute(
                "INSERT INTO groundloop_m5_schema_bundle "
                "SELECT bundle_id,repeat('f',64),migration_sha256,oracle_sha256,"
                "prerequisite_sha256,applied_at FROM groundloop_m5_schema_bundle "
                "WHERE bundle_id=%s",
                ("m5-preterminal-seal-context-schema-bundle-v1",),
            )
        connection.commit()
        before = _m5_readiness_catalog_fingerprint(connection, schema_name=schema)
        ledger = _all_ledger_rows(connection)
        connection.commit()
        with pytest.raises(M5SemanticReadinessBundleError, match="structural catalog"):
            install_m5_semantic_readiness_bundle(connection)
        assert (
            _m5_readiness_catalog_fingerprint(connection, schema_name=schema) == before
        )
        assert _all_ledger_rows(connection) == ledger
        connection.commit()
        if installed:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                with pytest.raises(
                    M5SemanticReadinessBundleError, match="structural catalog"
                ):
                    require_m5_semantic_readiness_bundle(connection, schema_name=schema)


@pytest.mark.parametrize(
    "source,expected",
    [
        ("public.example('public.example')", "example ( 'public.example' )"),
        ('"test schema"."value"', '"value"'),
        ("other.example('test schema')", "other . example ( 'test schema' )"),
        ("'public.''example'::pg_catalog.text", "'public.''example' : : text"),
        (r"E'escaped\'public.example'", r"E'escaped\'public.example'"),
        ("a OPERATOR(pg_catalog.=) b", "a = b"),
        ("a OPERATOR(other.=) b", "a OPERATOR ( other . = ) b"),
        ("'OPERATOR ( = )'", "'OPERATOR ( = )'"),
    ],
)
def test_catalog_sql_normalizes_qualifiers_not_data(source: str, expected: str) -> None:
    assert _m5_readiness_catalog_sql(source, "test schema") == expected


def test_catalog_checksums_reproduce_across_schemas_paths_and_nonowner() -> None:
    images: list[tuple[str, str]] = []
    for _ in range(2):
        with _pre019_schema() as (owner, schema):
            install_m5_preterminal_seal_context_bundle(owner)
            prior = _m5_readiness_catalog_fingerprint(owner, schema_name=schema)
            owner.commit()
            _raw_install(owner, schema)
            current = _m5_readiness_catalog_fingerprint(owner, schema_name=schema)
            assert prior == _M5_READINESS_CATALOG_SHA256[False]
            assert current == _M5_READINESS_CATALOG_SHA256[True]
            owner.execute(
                sql.SQL("SET search_path TO {},pg_catalog").format(
                    sql.Identifier(schema)
                )
            )
            assert (
                _m5_readiness_catalog_fingerprint(owner, schema_name=schema) == current
            )
            owner.rollback()
            images.append((prior, current))
    assert images[0] == images[1]

    role = "d32_catalog_reader_" + uuid.uuid4().hex
    password = uuid.uuid4().hex
    with _pre019_schema() as (owner, schema):
        install_m5_preterminal_seal_context_bundle(owner)
        install_m5_semantic_readiness_bundle(owner)
        owner.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(role), sql.Literal(password)
            )
        )
        owner.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(schema), sql.Identifier(role)
            )
        )
        owner.execute(
            sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                sql.Identifier(schema), sql.Identifier(role)
            )
        )
        owner.commit()
        try:
            assert (
                _m5_readiness_catalog_fingerprint(owner, schema_name=schema)
                == _M5_READINESS_CATALOG_SHA256[True]
            )
            owner.rollback()
            with psycopg.connect(
                _database_url(), user=role, password=password
            ) as reader:
                _select_schema(reader, schema)
                reader.execute(
                    "CREATE TEMP TABLE groundloop_m5_schema_bundle(fake int)"
                )
                reader.commit()
                with reader.transaction():
                    reader.execute("SET TRANSACTION READ ONLY")
                    assert (
                        _m5_readiness_catalog_fingerprint(reader, schema_name=schema)
                        == _M5_READINESS_CATALOG_SHA256[True]
                    )
                    require_m5_semantic_readiness_bundle(reader, schema_name=schema)
        finally:
            owner.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            owner.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
            owner.commit()


def test_catalog_authority_ignores_caller_builtin_and_operator_shadows() -> None:
    hostile = "d32_catalog_shadow_" + uuid.uuid4().hex
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        install_m5_semantic_readiness_bundle(connection)
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(hostile)))
        try:
            connection.execute(
                sql.SQL(
                    "CREATE FUNCTION {}.left(text,integer) RETURNS text "
                    "LANGUAGE sql IMMUTABLE AS 'SELECT ''not_groundloop'''"
                ).format(sql.Identifier(hostile))
            )
            connection.execute(
                sql.SQL(
                    "CREATE FUNCTION {}.false_equal(text,text) RETURNS boolean "
                    "LANGUAGE sql IMMUTABLE AS 'SELECT false'"
                ).format(sql.Identifier(hostile))
            )
            connection.execute(
                sql.SQL(
                    "CREATE OPERATOR {}.= (FUNCTION={}.false_equal,"
                    "LEFTARG=text,RIGHTARG=text)"
                ).format(sql.Identifier(hostile), sql.Identifier(hostile))
            )
            connection.commit()
            connection.execute(
                sql.SQL("SET search_path TO {},{},pg_catalog").format(
                    sql.Identifier(hostile), sql.Identifier(schema)
                )
            )
            assert connection.execute(
                "SELECT left('groundloop_test',11)"
            ).fetchone() == ("not_groundloop",)
            assert connection.execute(
                "SELECT 'same'::text='same'::text"
            ).fetchone() == (False,)
            before = connection.execute("SHOW search_path").fetchone()
            connection.commit()
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                require_m5_semantic_readiness_bundle(connection, schema_name=schema)
                assert (
                    _m5_readiness_catalog_fingerprint(connection, schema_name=schema)
                    == _M5_READINESS_CATALOG_SHA256[True]
                )
                assert connection.execute("SHOW search_path").fetchone() == before
        finally:
            connection.rollback()
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(hostile))
            )
            connection.commit()


def test_installer_ledger_timestamp_does_not_call_application_now() -> None:
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        connection.execute(
            sql.SQL(
                "CREATE FUNCTION {}.now() RETURNS pg_catalog.timestamptz "
                "LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION "
                "'unapproved application now called'; END$$"
            ).format(sql.Identifier(schema))
        )
        connection.commit()
        assert install_m5_semantic_readiness_bundle(connection).applied
        assert not install_m5_semantic_readiness_bundle(connection).applied
