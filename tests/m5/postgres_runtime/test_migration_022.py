"""D35 schema/guard fixtures. Raw diagnostics are NOT authoritative installation.

The separately audited H35 pins are required before installer qualification.
No held C2/B33/C1 WIP is imported and no ready/completed runtime is fabricated.
"""

from __future__ import annotations

import hashlib
import os
import re
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, replace
from threading import Event
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.m5.runtime.postgres_matching import _STAGE_JOURNAL_RELATIONS
from groundloop.postgres import migrations as m
from tests.m5.postgres_runtime.d33_document_completion import (
    test_document_open_prerequisite as o33,
)

_RUNTIME_ROLE = "groundloop_s35_runtime_20261015"
_ACCESS_SETTINGS = (
    "groundloop.m5_checked_transition",
    "groundloop.m5_matching_mode",
    "groundloop.m5_matching_epoch_id",
    "groundloop.m5_matching_expected_revision",
    "groundloop.m5_matching_resulting_revision",
    "groundloop.m5_matching_source_kind",
    "groundloop.m5_matching_source_id",
    "groundloop.m5_matching_context_oid",
    "groundloop.m5_matching_journal_oid",
    "groundloop.m5_matching_expected_oid",
)


def _dsn() -> str:
    value = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL")
    if not value:
        pytest.skip("S35 live qualification requires its explicitly owned fixture DB")
    return value


@dataclass(frozen=True)
class _Schema:
    connection: Connection[Any]
    name: str


@contextmanager
def _schema(*, raw022: bool = False) -> Iterator[_Schema]:
    """Empty private fixture schema; exact old installers, raw022 only on request."""
    name = f"groundloop_s35_{uuid.uuid4().hex}"
    with psycopg.connect(_dsn(), autocommit=True) as connection:
        assert connection.execute("SELECT current_database()").fetchone() == (
            "groundloop_s35_20261015",
        )
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))
        try:
            connection.execute(
                sql.SQL("SET search_path TO {}, public").format(sql.Identifier(name))
            )
            with connection.transaction():
                m.apply_legacy_migrations(connection)
            for install in (
                m.install_m5_core_bundle,
                m.install_m5_runtime_bundle,
                m.install_m5_runtime_recovery_bundle,
                m.install_m5_persisted_matching_bundle,
                m.install_m5_bounded_document_withdrawal_bundle,
                m.install_m5_preterminal_seal_context_bundle,
                m.install_m5_semantic_readiness_bundle,
                m.install_m5_document_update_completion_bundle,
            ):
                install(connection)
            if raw022:
                with connection.transaction():
                    connection.execute(
                        sql.SQL("SET LOCAL search_path TO {}, pg_catalog").format(
                            sql.Identifier(name)
                        )
                    )
                    connection.execute(
                        m.M5_TRANSITION_CONTEXT_ACCESS_MIGRATION_PATH.read_text()
                    )
            yield _Schema(connection, name)
        finally:
            connection.execute("SET search_path TO public")
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name))
            )


def _bridge_call(db: _Schema, *, journal: bool = False) -> sql.Composed:
    return sql.SQL(
        "SELECT * FROM {}(%s::pg_catalog.int8,%s::pg_catalog.int8,"
        "%s::pg_catalog.int8,%s::pg_catalog.text,%s::pg_catalog.text,"
        "%s::pg_catalog.oid,%s::pg_catalog.oid,%s::pg_catalog.oid)"
    ).format(sql.Identifier(db.name, m._m5_transition_access_names[int(journal)]))


def test_raw_sql_six_statement_catalog_diagnostic() -> None:
    """Pre-H35 raw SQL catalog diagnostic, not022 installer acceptance."""
    with _schema(raw022=True) as db:
        rows = db.connection.execute(
            "SELECT p.proname,p.prosecdef,p.provolatile,p.proisstrict,"
            "p.proleakproof,p.proparallel,p.pronargs,p.prosrc "
            "FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n "
            "ON n.oid=p.pronamespace WHERE n.nspname=%s "
            "AND p.proname=ANY(%s) ORDER BY p.proname",
            (db.name, list(m._m5_transition_access_names)),
        ).fetchall()
        assert len(rows) == 2
        for row in rows:
            assert tuple(row[1:7]) == (True, "s", False, False, "u", 8)
            assert hashlib.sha256(str(row[7]).encode()).hexdigest()
        assert db.connection.execute(
            "SELECT count(*) FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n "
            "ON n.oid=p.pronamespace WHERE n.nspname=%s "
            "AND left(p.proname,11)='groundloop_'",
            (db.name,),
        ).fetchone() == (174,)
        assert (
            m._m5_readiness_bundle_row(
                db.connection,
                m.M5_TRANSITION_CONTEXT_ACCESS_BUNDLE_ID,
                schema_name=db.name,
            )
            is None
        )


@pytest.mark.parametrize("journal", [False, True])
def test_raw_rejected_unassigned_xid_call_does_not_assign_xid(journal: bool) -> None:
    with _schema(raw022=True) as db, db.connection.transaction():
        assert db.connection.execute(
            "SELECT pg_catalog.pg_current_xact_id_if_assigned()"
        ).fetchone() == (None,)
        with pytest.raises(psycopg.errors.RaiseException), db.connection.transaction():
            db.connection.execute(
                _bridge_call(db, journal=journal),
                (1, 1, 2, "requirement_completion", "absent", 1, 2, 3),
            )
        assert db.connection.execute(
            "SELECT pg_catalog.pg_current_xact_id_if_assigned()"
        ).fetchone() == (None,)


def test_raw_legacy_strict_checker_rejects_successor_orphan() -> None:
    with _schema(raw022=True) as db:
        with pytest.raises(m.M5DocumentUpdateCompletionBundleError):
            m._m5_document_completion_verify_catalog(
                db.connection, schema_name=db.name, installed=True
            )
        with pytest.raises(m.M5TransitionContextAccessBundleError, match="orphan"):
            m.require_m5_document_update_completion_bundle(
                db.connection, schema_name=db.name
            )


@dataclass(frozen=True)
class _RawEpoch:
    db: _Schema
    epoch: int
    event_id: str


@pytest.fixture(scope="module")
def raw_epoch() -> Iterator[_RawEpoch]:
    """Real legacy O33 OPEN first; only then isolated raw022 guard diagnostics.

    The initial bootstrap snapshot is labelled fixture input. Actual activation
    and OPEN are native producers, not later authored ready/completed headers.
    Raw022 has no ledger; this fixture does NOT claim022 installation or C2.
    """
    with psycopg.connect(_dsn(), autocommit=True) as check:
        assert check.execute("SELECT current_database()").fetchone() == (
            "groundloop_s35_20261015",
        )
    generator = o33.open_db.__wrapped__(SimpleNamespace(param=None))
    foundation = next(generator)
    try:
        foundation.store._activate_document_open_prerequisite(foundation.request)
        plan = foundation.plan("s35-raw-foundation")
        receipt = o33._open(foundation, plan)
        epoch_row = foundation.connection.execute(
            "SELECT epoch_id FROM groundloop_epoch WHERE event_id=%s",
            (plan.structural_event_id,),
        ).fetchone()
        assert epoch_row is not None and receipt is not None
        connection = foundation.connection
        with connection.transaction():
            connection.execute(
                sql.SQL("SET LOCAL search_path TO {}, pg_catalog").format(
                    sql.Identifier(foundation.schema)
                )
            )
            connection.execute(
                m.M5_TRANSITION_CONTEXT_ACCESS_MIGRATION_PATH.read_text()
            )
        connection.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(foundation.schema), sql.Identifier(_RUNTIME_ROLE)
            )
        )
        connection.execute(
            sql.SQL("GRANT ALL ON ALL TABLES IN SCHEMA {} TO {}").format(
                sql.Identifier(foundation.schema), sql.Identifier(_RUNTIME_ROLE)
            )
        )
        connection.execute(
            sql.SQL("GRANT USAGE ON ALL SEQUENCES IN SCHEMA {} TO {}").format(
                sql.Identifier(foundation.schema), sql.Identifier(_RUNTIME_ROLE)
            )
        )
        yield _RawEpoch(
            _Schema(connection, foundation.schema),
            int(epoch_row[0]),
            plan.structural_event_id,
        )
    finally:
        generator.close()


@contextmanager
def _context(
    fixture: _RawEpoch,
    *,
    runtime: bool = False,
    kind: str = "requirement_completion",
    source_id: str | None = None,
) -> Iterator[tuple[_Schema, tuple[Any, ...]]]:
    """Genuine native SQL context; rollback-only shape evidence, not completion."""
    with psycopg.connect(
        _dsn(), user=_RUNTIME_ROLE if runtime else "groundloop", autocommit=True
    ) as connection:
        db = _Schema(connection, fixture.db.name)
        connection.execute(
            sql.SQL("SET search_path TO {}, pg_catalog").format(sql.Identifier(db.name))
        )
        with connection.transaction(force_rollback=True):
            connection.execute(
                sql.SQL("SELECT {}(%s::bigint,%s::bigint)").format(
                    sql.Identifier(
                        db.name, "groundloop_m5_authorize_checked_transition"
                    )
                ),
                (fixture.epoch, 1),
            )
            result = 1 if kind == "structural_open" else 2
            source = fixture.event_id if kind == "structural_open" else f"s35-{kind}"
            if source_id is not None:
                source = source_id
            connection.execute(
                sql.SQL(
                    "SELECT {}(%s::bigint,%s::bigint,%s::bigint,%s::text,%s::text)"
                ).format(
                    sql.Identifier(
                        db.name, "groundloop_m5_authorize_persisted_matching_transition"
                    )
                ),
                (fixture.epoch, 1, result, kind, source),
            )
            oids = connection.execute(
                "SELECT pg_catalog.current_setting("
                "'groundloop.m5_matching_context_oid')"
                "::pg_catalog.oid,"
                "pg_catalog.current_setting('groundloop.m5_matching_journal_oid')"
                "::pg_catalog.oid,"
                "pg_catalog.current_setting('groundloop.m5_matching_expected_oid')"
                "::pg_catalog.oid"
            ).fetchone()
            assert oids is not None
            # No runtime source/header mutation or deferred-guard forcing.
            # force_rollback preserves any test exception without masking it.
            yield db, (fixture.epoch, 1, result, kind, source, *oids)


@pytest.mark.parametrize("runtime", [False, True])
@pytest.mark.parametrize(
    "kind", ["structural_open", "requirement_completion", "direct_transition"]
)
def test_raw_genuine_owner_and_login_context_shapes(
    raw_epoch: _RawEpoch,
    runtime: bool,
    kind: str,
) -> None:
    with _context(raw_epoch, runtime=runtime, kind=kind) as (db, args):
        rows = db.connection.execute(_bridge_call(db), args).fetchall()
        identity = db.connection.execute(
            "SELECT pg_catalog.pg_backend_pid(),"
            "pg_catalog.pg_current_xact_id_if_assigned()::text,session_user::text"
        ).fetchone()
        assert len(rows) == 1 and tuple(rows[0][:3]) == identity
        assert tuple(rows[0][3:]) == (*args[:5], False, False)
        assert db.connection.execute(_bridge_call(db), args).fetchall() == rows
        assert (
            db.connection.execute(_bridge_call(db, journal=True), args).fetchall() == []
        )
        if runtime:
            for relation in (
                "groundloop_m5_matching_transition_context",
                "groundloop_m5_matching_change_journal",
                "groundloop_m5_matching_expected_changes",
            ):
                with (
                    pytest.raises(psycopg.errors.InsufficientPrivilege),
                    db.connection.transaction(),
                ):
                    db.connection.execute(
                        sql.SQL("SELECT * FROM pg_temp.{}").format(
                            sql.Identifier(relation)
                        )
                    )


@pytest.mark.parametrize("journal", [False, True])
@pytest.mark.parametrize("index", range(8))
def test_raw_all_nullable_positions_fail_closed(
    raw_epoch: _RawEpoch,
    journal: bool,
    index: int,
) -> None:
    with _context(raw_epoch) as (db, original):
        args = list(original)
        args[index] = None
        with pytest.raises(psycopg.errors.RaiseException), db.connection.transaction():
            db.connection.execute(_bridge_call(db, journal=journal), args)


@pytest.mark.parametrize("journal", [False, True])
@pytest.mark.parametrize(
    "index,value",
    [
        (0, 0),
        (0, -1),
        (0, 999999),
        (1, 0),
        (1, 2),
        (2, 0),
        (2, 1),
        (2, 3),
        (2, 9223372036854775807),
        (3, "unknown"),
        (4, ""),
        (4, "  "),
        (4, "mismatch"),
        (5, 0),
        (6, 0),
        (7, 0),
        (5, 1),
        (6, 1),
        (7, 1),
    ],
)
def test_raw_invalid_argument_matrix(
    raw_epoch: _RawEpoch,
    journal: bool,
    index: int,
    value: Any,
) -> None:
    with _context(raw_epoch) as (db, original):
        args = list(original)
        args[index] = value
        with pytest.raises(psycopg.errors.RaiseException), db.connection.transaction():
            db.connection.execute(_bridge_call(db, journal=journal), args)


def _negative_triplet(db: _Schema) -> tuple[int, int, int]:
    """Same-shape forged tables for NEGATIVES only; never native positive authority."""
    db.connection.execute(
        "CREATE TEMP TABLE groundloop_m5_matching_transition_context ("
        "backend_pid integer NOT NULL,transaction_id bigint NOT NULL,"
        "session_role text NOT NULL,epoch_id bigint NOT NULL,"
        "expected_revision bigint NOT NULL,resulting_revision bigint NOT NULL,"
        "source_kind text NOT NULL,source_id text NOT NULL,"
        "anchor_count integer NOT NULL DEFAULT 0,runtime_first_old jsonb,"
        "runtime_final_new jsonb,runtime_saw_insert boolean NOT NULL DEFAULT false,"
        "runtime_saw_update boolean NOT NULL DEFAULT false,"
        "validation_started boolean NOT NULL DEFAULT false,"
        "validation_done boolean NOT NULL DEFAULT false,"
        "PRIMARY KEY(backend_pid,transaction_id,session_role)) ON COMMIT PRESERVE ROWS"
    )
    db.connection.execute(
        "CREATE TEMP TABLE groundloop_m5_matching_change_journal ("
        "relation_name text NOT NULL,key_preimage bytea NOT NULL,first_old jsonb,"
        "final_new jsonb,first_operation text NOT NULL,last_operation text NOT NULL,"
        "mutation_count integer NOT NULL,saw_insert boolean NOT NULL,"
        "saw_update boolean NOT NULL,saw_delete boolean NOT NULL,"
        "PRIMARY KEY(relation_name,key_preimage)) ON COMMIT PRESERVE ROWS"
    )
    db.connection.execute(
        "CREATE TEMP TABLE groundloop_m5_matching_expected_changes ("
        "relation_name text NOT NULL,key_preimage bytea NOT NULL,"
        "PRIMARY KEY(relation_name,key_preimage)) ON COMMIT PRESERVE ROWS"
    )
    row = db.connection.execute(
        "SELECT 'pg_temp.groundloop_m5_matching_transition_context'::regclass::oid,"
        "'pg_temp.groundloop_m5_matching_change_journal'::regclass::oid,"
        "'pg_temp.groundloop_m5_matching_expected_changes'::regclass::oid"
    ).fetchone()
    assert row is not None
    return int(row[0]), int(row[1]), int(row[2])


def _negative_settings(db: _Schema, args: tuple[Any, ...], *, oid_mode: str) -> None:
    names = (
        "epoch_id",
        "expected_revision",
        "resulting_revision",
        "source_kind",
        "source_id",
        "context_oid",
        "journal_oid",
        "expected_oid",
    )
    db.connection.execute(
        "SELECT pg_catalog.set_config('groundloop.m5_checked_transition','on',true),"
        "pg_catalog.set_config('groundloop.m5_matching_mode','transition',true)"
    )
    for index, (name, value) in enumerate(zip(names, args, strict=True)):
        if index >= 5 and oid_mode == "absent":
            continue
        if index >= 5 and oid_mode == "invented":
            value = 1
        db.connection.execute(
            "SELECT pg_catalog.set_config(%s,%s,true)",
            (f"groundloop.m5_matching_{name}", str(value)),
        )


@pytest.mark.parametrize("oid_mode", ["absent", "invented", "exact"])
def test_raw_untrusted_actual_login_forged_triplet_is_not_authority(
    raw_epoch: _RawEpoch,
    oid_mode: str,
) -> None:
    with psycopg.connect(_dsn(), user=_RUNTIME_ROLE, autocommit=True) as connection:
        db = _Schema(connection, raw_epoch.db.name)
        connection.execute(
            sql.SQL("SET search_path TO {}, pg_catalog").format(sql.Identifier(db.name))
        )
        with connection.transaction(force_rollback=True):
            oids = _negative_triplet(db)
            args = (raw_epoch.epoch, 1, 2, "requirement_completion", "forged", *oids)
            connection.execute(
                "INSERT INTO pg_temp.groundloop_m5_matching_transition_context "
                "(backend_pid,transaction_id,session_role,epoch_id,expected_revision,"
                "resulting_revision,source_kind,source_id) VALUES "
                "(pg_backend_pid(),pg_current_xact_id()::text::bigint,session_user,"
                "%s,1,2,'requirement_completion','forged')",
                (raw_epoch.epoch,),
            )
            _negative_settings(db, args, oid_mode=oid_mode)
            for journal in (False, True):
                with (
                    pytest.raises(psycopg.errors.RaiseException) as error,
                    connection.transaction(),
                ):
                    connection.execute(_bridge_call(db, journal=journal), args)
                if oid_mode == "exact":
                    assert "untrusted owner" in str(error.value)


@pytest.mark.parametrize("journal", [False, True])
def test_raw_actual_unassigned_xid_guard_reached(journal: bool) -> None:
    """Controlled trusted-owner NEGATIVE, not a fabricated native positive.

    Preserved same-shape temp tables satisfy the preceding metadata checks.
    A fresh read-only transaction reaches the explicit unassigned-XID guard.
    """
    with _schema(raw022=True) as db:
        with db.connection.transaction():
            oids = _negative_triplet(db)
        args = (1, 1, 2, "requirement_completion", "unassigned-negative", *oids)
        with db.connection.transaction(force_rollback=True):
            _negative_settings(db, args, oid_mode="exact")
            assert db.connection.execute(
                "SELECT pg_catalog.pg_current_xact_id_if_assigned()"
            ).fetchone() == (None,)
            with (
                pytest.raises(psycopg.errors.RaiseException, match="unassigned"),
                db.connection.transaction(),
            ):
                db.connection.execute(_bridge_call(db, journal=journal), args)
            assert db.connection.execute(
                "SELECT pg_catalog.pg_current_xact_id_if_assigned()"
            ).fetchone() == (None,)


def test_raw_complete_metadata_matches_reviewed_sql() -> None:
    with _schema(raw022=True) as db:
        rows = m._m5_transition_access_function_rows(db.connection, schema_name=db.name)
        bodies = dict(
            re.findall(
                r"CREATE FUNCTION (\w+)\(.*?\bAS \$\$(.*?)\$\$;",
                m.M5_TRANSITION_CONTEXT_ACCESS_MIGRATION_PATH.read_text(),
                re.S,
            )
        )
        inputs = ["int8", "int8", "int8", "text", "text", "oid", "oid", "oid"]
        input_names = [
            "selected_epoch_id",
            "selected_expected_revision",
            "selected_resulting_revision",
            "selected_source_kind",
            "selected_source_id",
            "selected_context_oid",
            "selected_journal_oid",
            "selected_expected_oid",
        ]
        outputs = [
            [
                ("backend_pid", "int4"),
                ("transaction_id", "text"),
                ("session_role", "text"),
                ("epoch_id", "int8"),
                ("expected_revision", "int8"),
                ("resulting_revision", "int8"),
                ("source_kind", "text"),
                ("source_id", "text"),
                ("validation_started", "bool"),
                ("validation_done", "bool"),
            ],
            [
                ("relation_name", "text"),
                ("key_preimage", "bytea"),
                ("first_old", "jsonb"),
                ("final_new", "jsonb"),
                ("first_operation", "text"),
                ("last_operation", "text"),
                ("mutation_count", "int4"),
                ("saw_insert", "bool"),
                ("saw_update", "bool"),
                ("saw_delete", "bool"),
            ],
        ]
        owner = db.connection.execute(
            "SELECT relowner FROM pg_catalog.pg_class WHERE oid=%s::regclass",
            (f"{db.name}.groundloop_m5_schema_bundle",),
        ).fetchone()
        assert owner is not None and len(rows) == 2
        for row, columns in zip(rows, outputs, strict=True):
            assert tuple(row[2:-2]) == (
                True,
                "plpgsql",
                True,
                [f"search_path={db.name}, pg_catalog"],
                "s",
                False,
                False,
                "u",
                "f",
                0,
                0,
                True,
                "pg_catalog",
                "record",
                8,
                input_names + [name for name, _ in columns],
                ["i"] * 8 + ["t"] * 10,
                [
                    "pg_catalog:" + kind
                    for kind in inputs + [kind for _, kind in columns]
                ],
                ["pg_catalog:" + kind for kind in inputs],
                ["0:EXECUTE:false", f"{owner[0]}:EXECUTE:false"],
                None,
                0,
                100,
                1000,
                None,
            )
            assert row[-2] == bodies[row[1]]


@pytest.mark.parametrize(
    "field,expression",
    [
        ("backend_pid", "backend_pid+1"),
        ("transaction_id", "transaction_id+4294967296"),
        ("session_role", "'foreign-session'"),
        ("epoch_id", "epoch_id+1"),
        ("expected_revision", "expected_revision+1"),
        ("resulting_revision", "resulting_revision+1"),
        ("source_kind", "'direct_transition'"),
        ("source_id", "'foreign-source'"),
        ("validation_started", "true"),
        ("validation_done", "true"),
    ],
)
def test_raw_mutated_context_field_rejected(
    raw_epoch: _RawEpoch,
    field: str,
    expression: str,
) -> None:
    """Trusted-owner negative tampering, never a manufactured positive."""
    with _context(raw_epoch) as (db, args):
        db.connection.execute(
            sql.SQL(
                "UPDATE pg_temp.groundloop_m5_matching_transition_context SET {}={}"
            ).format(sql.Identifier(field), sql.SQL(expression))
        )
        for journal in (False, True):
            with pytest.raises(psycopg.Error), db.connection.transaction():
                db.connection.execute(_bridge_call(db, journal=journal), args)


@pytest.mark.parametrize("count", [0, 2])
def test_raw_context_total_cardinality_rejected(
    raw_epoch: _RawEpoch, count: int
) -> None:
    with _context(raw_epoch) as (db, args):
        if count == 0:
            db.connection.execute(
                "DELETE FROM pg_temp.groundloop_m5_matching_transition_context"
            )
        else:
            db.connection.execute(
                "INSERT INTO pg_temp.groundloop_m5_matching_transition_context "
                "(backend_pid,transaction_id,session_role,epoch_id,expected_revision,"
                "resulting_revision,source_kind,source_id) SELECT backend_pid+1,"
                "transaction_id,session_role,epoch_id,expected_revision,"
                "resulting_revision,source_kind,source_id "
                "FROM pg_temp.groundloop_m5_matching_transition_context"
            )
        for journal in (False, True):
            with pytest.raises(psycopg.Error), db.connection.transaction():
                db.connection.execute(_bridge_call(db, journal=journal), args)


@pytest.mark.parametrize(
    "field", ["validation_started", "validation_done", "source_id"]
)
def test_raw_sql_null_context_field_is_not_proof(
    raw_epoch: _RawEpoch, field: str
) -> None:
    with _context(raw_epoch) as (db, args):
        db.connection.execute(
            sql.SQL(
                "ALTER TABLE pg_temp.groundloop_m5_matching_transition_context "
                "ALTER COLUMN {} DROP NOT NULL"
            ).format(sql.Identifier(field))
        )
        db.connection.execute(
            sql.SQL(
                "UPDATE pg_temp.groundloop_m5_matching_transition_context SET {}=NULL"
            ).format(sql.Identifier(field))
        )
        for journal in (False, True):
            with pytest.raises(psycopg.Error), db.connection.transaction():
                db.connection.execute(_bridge_call(db, journal=journal), args)


@pytest.mark.parametrize(
    "relation",
    [
        "groundloop_m5_matching_transition_context",
        "groundloop_m5_matching_change_journal",
        "groundloop_m5_matching_expected_changes",
    ],
)
def test_raw_dropped_or_replaced_triplet_oid_rejected(
    raw_epoch: _RawEpoch,
    relation: str,
) -> None:
    with _context(raw_epoch) as (db, args):
        db.connection.execute(
            sql.SQL("DROP TABLE pg_temp.{}").format(sql.Identifier(relation))
        )
        for journal in (False, True):
            with pytest.raises(psycopg.Error), db.connection.transaction():
                db.connection.execute(_bridge_call(db, journal=journal), args)
        db.connection.execute(
            sql.SQL("CREATE TEMP TABLE pg_temp.{} (decoy int)").format(
                sql.Identifier(relation)
            )
        )
        for journal in (False, True):
            with pytest.raises(psycopg.Error), db.connection.transaction():
                db.connection.execute(_bridge_call(db, journal=journal), args)


@pytest.mark.parametrize(
    "relation",
    [
        "groundloop_m5_matching_promotion_context",
        "groundloop_m5_matching_promotion_journal",
        "groundloop_m5_matching_promotion_expected",
    ],
)
def test_raw_promotion_coexistence_rejected(
    raw_epoch: _RawEpoch, relation: str
) -> None:
    with _context(raw_epoch) as (db, args):
        db.connection.execute(
            sql.SQL("CREATE TEMP TABLE pg_temp.{} (decoy int)").format(
                sql.Identifier(relation)
            )
        )
        for journal in (False, True):
            with pytest.raises(psycopg.Error), db.connection.transaction():
                db.connection.execute(_bridge_call(db, journal=journal), args)


def test_raw_private_helpers_execute_denied_to_actual_login(
    raw_epoch: _RawEpoch,
) -> None:
    with _context(raw_epoch, runtime=True) as (db, args):
        for signature in (
            "groundloop_m5_matching_begin_transition_context(bigint,bigint,bigint,text,text)",
            "groundloop_m5_matching_private_temp_triplet(regclass,regclass,regclass)",
            "groundloop_m5_matching_journal_change(text,text,jsonb,jsonb)",
            "groundloop_m5_matching_deferred_validate()",
            "groundloop_m5_matching_capture_transition_anchor()",
        ):
            assert db.connection.execute(
                "SELECT pg_catalog.has_function_privilege(session_user,%s,'EXECUTE')",
                (f"{db.name}.{signature}",),
            ).fetchone() == (False,)
        with (
            pytest.raises(psycopg.errors.InsufficientPrivilege),
            db.connection.transaction(),
        ):
            db.connection.execute(
                sql.SQL("SELECT {}(%s::regclass,%s::regclass,%s::regclass)").format(
                    sql.Identifier(
                        db.name, "groundloop_m5_matching_private_temp_triplet"
                    )
                ),
                args[5:],
            )


def test_raw_repeatability_preserves_complete_temp_images_and_settings(
    raw_epoch: _RawEpoch,
) -> None:
    with _context(raw_epoch) as (db, args):
        # An unresolved unrelated deferred FK must survive both readers. This
        # probe is not a transition row and bypasses no production guard.
        db.connection.execute("CREATE TEMP TABLE s35_fk_parent (id int PRIMARY KEY)")
        db.connection.execute(
            "CREATE TEMP TABLE s35_fk_child (id int REFERENCES "
            "s35_fk_parent(id) DEFERRABLE INITIALLY DEFERRED)"
        )
        db.connection.execute("INSERT INTO s35_fk_child VALUES (1)")
        relations = (
            "groundloop_m5_matching_transition_context",
            "groundloop_m5_matching_change_journal",
            "groundloop_m5_matching_expected_changes",
        )

        def snapshot() -> tuple[Any, ...]:
            images = tuple(
                db.connection.execute(
                    sql.SQL("SELECT * FROM pg_temp.{}").format(sql.Identifier(relation))
                ).fetchall()
                for relation in relations
            )
            identity = db.connection.execute(
                "SELECT pg_catalog.pg_current_xact_id_if_assigned()::text,"
                "pg_catalog.current_setting('search_path'),"
                "pg_catalog.current_setting('groundloop.m5_checked_transition'),"
                "pg_catalog.current_setting('groundloop.m5_matching_mode')"
            ).fetchone()
            settings = db.connection.execute(
                "SELECT name,current_setting(name,true) "
                "FROM unnest(%s::text[]) AS names(name) ORDER BY name",
                (list(_ACCESS_SETTINGS),),
            ).fetchall()
            return images, identity, settings, o33.database_snapshot(db.connection)

        before = snapshot()
        for _ in range(3):
            assert db.connection.execute(_bridge_call(db), args).fetchall()
            assert not db.connection.execute(
                _bridge_call(db, journal=True), args
            ).fetchall()
        assert snapshot() == before
        db.connection.execute("INSERT INTO s35_fk_child VALUES (2)")
        assert db.connection.execute(
            "SELECT count(*) FROM s35_fk_child"
        ).fetchone() == (2,)


@pytest.mark.parametrize(
    "setting,value",
    [
        ("groundloop.m5_checked_transition", "off"),
        ("groundloop.m5_matching_mode", "seal"),
        ("groundloop.m5_matching_epoch_id", ""),
        ("groundloop.m5_matching_epoch_id", "01"),
        ("groundloop.m5_matching_expected_revision", "01"),
        ("groundloop.m5_matching_resulting_revision", "02"),
        ("groundloop.m5_matching_source_kind", "direct_transition"),
        ("groundloop.m5_matching_source_id", "foreign"),
        ("groundloop.m5_matching_context_oid", "0"),
        ("groundloop.m5_matching_journal_oid", "0"),
        ("groundloop.m5_matching_expected_oid", "0"),
    ],
)
def test_raw_noncanonical_or_wrong_guc_is_not_authority(
    raw_epoch: _RawEpoch,
    setting: str,
    value: str,
) -> None:
    with _context(raw_epoch, runtime=True) as (db, args):
        db.connection.execute(
            "SELECT pg_catalog.set_config(%s,%s,true)", (setting, value)
        )
        for journal in (False, True):
            with (
                pytest.raises(psycopg.errors.RaiseException),
                db.connection.transaction(),
            ):
                db.connection.execute(_bridge_call(db, journal=journal), args)


def _installed_image(db: _Schema) -> tuple[Any, ...]:
    """Raw OIDs/ledger timestamps matter for no-write replay and rollback."""
    return (
        o33.database_snapshot(db.connection),
        db.connection.execute(
            "SELECT p.oid,p.proname,p.prosrc,p.proacl,p.proconfig "
            "FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n "
            "ON n.oid=p.pronamespace WHERE n.nspname=%s ORDER BY p.oid",
            (db.name,),
        ).fetchall(),
    )


def _negative_ledger_damage(db: _Schema, query: Any, params: Any) -> None:
    """Owner-only negative corruption; immutable triggers restored before check.

    Never used to qualify installation or runtime positives. The table belongs
    solely to this disposable fixture, not an existing user database/schema.
    """
    with db.connection.transaction():
        db.connection.execute(
            "ALTER TABLE groundloop_m5_schema_bundle DISABLE TRIGGER USER"
        )
        db.connection.execute(query, params)
        db.connection.execute(
            "ALTER TABLE groundloop_m5_schema_bundle ENABLE TRIGGER USER"
        )


def test_authoritative_first_install_exact_replay_and_strict_successor() -> None:
    with _schema() as db:
        before_ledgers = db.connection.execute(
            "SELECT * FROM groundloop_m5_schema_bundle ORDER BY bundle_id"
        ).fetchall()
        result = m.install_m5_transition_context_access_bundle(db.connection)
        assert result.applied
        assert m._m5_readiness_bundle_row(
            db.connection, result.identity.bundle_id, schema_name=db.name
        ) == m._m5_transition_access_ledger(result.identity)
        assert (
            db.connection.execute(
                "SELECT * FROM groundloop_m5_schema_bundle WHERE bundle_id<>%s "
                "ORDER BY bundle_id",
                (result.identity.bundle_id,),
            ).fetchall()
            == before_ledgers
        )
        m.require_m5_transition_context_access_bundle(
            db.connection, schema_name=db.name
        )
        m.require_m5_document_update_completion_bundle(
            db.connection, schema_name=db.name
        )
        with pytest.raises(m.M5DocumentUpdateCompletionBundleError):
            m._m5_document_completion_verify_catalog(
                db.connection, schema_name=db.name, installed=True
            )
        with pytest.raises(m.M5RuntimeBundleError):
            m._m5_readiness_verify_catalog(
                db.connection, schema_name=db.name, installed=True
            )
        before = _installed_image(db)
        cuts: list[str] = []
        replay = m.install_m5_transition_context_access_bundle(
            db.connection, failure_injector=cuts.append
        )
        assert not replay.applied and replay.identity == result.identity
        assert cuts == [] and _installed_image(db) == before


@pytest.mark.parametrize("source", [b"\xff", b"SELECT 1;", b"", b"-- corrupt\n"])
@pytest.mark.parametrize("installed", [False, True])
def test_caller_bytes_are_ledger_first_without_partial_objects(
    source: bytes, installed: bool
) -> None:
    with _schema() as db:
        if installed:
            m.install_m5_transition_context_access_bundle(db.connection)
            # Even prerequisite rejection must not precede a caller conflict.
            _negative_ledger_damage(
                db,
                "DELETE FROM groundloop_m5_schema_bundle WHERE bundle_id=%s",
                (m.M5_DOCUMENT_UPDATE_COMPLETION_BUNDLE_ID,),
            )
        before = _installed_image(db)
        cuts: list[str] = []
        expected = (
            m.M5BundleHashConflictError
            if installed
            else (m.M5TransitionContextAccessBundleError)
        )
        with pytest.raises(expected):
            m.install_m5_transition_context_access_bundle(
                db.connection, migration_bytes=source, failure_injector=cuts.append
            )
        assert cuts == ([] if installed else ["after_initial_ledger"])
        assert _installed_image(db) == before


_INSTALL_CUTS = (
    "after_initial_ledger",
    "after_prerequisite",
    "after_ledger_lock",
    "after_runtime_lock",
    *(
        f"{side}_{operation}_{name}"
        for name in m._m5_transition_access_names
        for operation in ("create", "revoke", "grant")
        for side in ("before", "after")
    ),
    "before_ledger",
    "after_ledger",
)


@pytest.mark.parametrize("target", _INSTALL_CUTS)
def test_every_install_ddl_acl_ledger_cut_rolls_back_and_retry(target: str) -> None:
    with _schema() as db:
        before = _installed_image(db)
        visited: list[str] = []

        def cut(stage: str) -> None:
            visited.append(stage)
            if stage == target:
                raise o33._ControlledCut(target)

        with pytest.raises(o33._ControlledCut, match=target):
            m.install_m5_transition_context_access_bundle(
                db.connection, failure_injector=cut
            )
        assert target in visited and _installed_image(db) == before
        assert not m._m5_transition_access_new_names(db.connection, schema_name=db.name)
        assert m.install_m5_transition_context_access_bundle(db.connection).applied


@pytest.mark.parametrize(
    "field",
    [
        "bundle_id",
        "bundle_sha256",
        "migration_sha256",
        "oracle_sha256",
        "prerequisite_sha256",
    ],
)
@pytest.mark.parametrize(
    "bundle",
    [
        m.M5_DOCUMENT_UPDATE_COMPLETION_BUNDLE_ID,
        m.M5_SEMANTIC_READINESS_BUNDLE_ID,
        m.M5_RUNTIME_RECOVERY_BUNDLE_ID,
    ],
)
def test_each_prerequisite_field_and_transitive_ledger_rejected(
    field: str, bundle: str
) -> None:
    with _schema() as db:
        _negative_ledger_damage(
            db,
            sql.SQL(
                "UPDATE groundloop_m5_schema_bundle SET {}=%s WHERE bundle_id=%s"
            ).format(sql.Identifier(field)),
            ("not-authority" if field == "bundle_id" else "0" * 64, bundle),
        )
        before = _installed_image(db)
        with pytest.raises(m.M5PrerequisiteError):
            m.install_m5_transition_context_access_bundle(db.connection)
        assert _installed_image(db) == before


@pytest.mark.parametrize("mode", ["orphan", "partial", "missing", "conflicting"])
def test_dispatch_never_falls_back_from_bad_successor(mode: str) -> None:
    with _schema() as db:
        if mode in ("missing", "conflicting"):
            m.install_m5_transition_context_access_bundle(db.connection)
        if mode in ("orphan", "partial"):
            statements = m._m5_transition_access_statements(
                m.M5_TRANSITION_CONTEXT_ACCESS_MIGRATION_PATH.read_bytes()
            )
            with db.connection.transaction():
                db.connection.execute(
                    sql.SQL("SET LOCAL search_path TO {}, pg_catalog").format(
                        sql.Identifier(db.name)
                    )
                )
                for _, statement in statements[: 3 if mode == "partial" else 6]:
                    db.connection.execute(statement)
        elif mode == "missing":
            db.connection.execute(
                sql.SQL(
                    "DROP FUNCTION {}(bigint,bigint,bigint,text,text,oid,oid,oid)"
                ).format(sql.Identifier(db.name, m._m5_transition_access_names[0]))
            )
        else:
            _negative_ledger_damage(
                db,
                "UPDATE groundloop_m5_schema_bundle "
                "SET migration_sha256=%s WHERE bundle_id=%s",
                ("0" * 64, m.M5_TRANSITION_CONTEXT_ACCESS_BUNDLE_ID),
            )
        before = _installed_image(db)
        with pytest.raises(m.M5RuntimeBundleError):
            m.require_m5_document_update_completion_bundle(
                db.connection, schema_name=db.name
            )
        assert _installed_image(db) == before


@pytest.mark.parametrize("target", ["old", "new"])
@pytest.mark.parametrize(
    "tamper",
    [
        "body",
        "owner",
        "volatility",
        "strict",
        "parallel",
        "leakproof",
        "security",
        "path",
        "acl",
        "cost",
        "rows",
        "default",
        "overload",
        "dependency",
        "support",
        "signature",
    ],
)
def test_closed_catalog_rejects_old_and_new_function_drift(
    target: str, tamper: str
) -> None:
    with _schema() as db:
        m.install_m5_transition_context_access_bundle(db.connection)
        name = (
            m._m5_transition_access_names[0]
            if target == "new"
            else "groundloop_m5_matching_begin_transition_context"
        )
        args = (
            "bigint,bigint,bigint,text,text,oid,oid,oid"
            if target == "new"
            else "bigint,bigint,bigint,text,text"
        )
        if target == "old" and tamper == "rows":
            name, args = "groundloop_m5_group_certificate_bindings_at", "bigint,bigint"
        signature = sql.SQL("{}(" + args + ")").format(sql.Identifier(db.name, name))
        if tamper == "body":
            body = db.connection.execute(
                "SELECT prosrc FROM pg_proc WHERE oid=%s::regprocedure",
                (signature.as_string(db.connection),),
            ).fetchone()[0]
            source = db.connection.execute(
                "SELECT pg_get_functiondef(%s::regprocedure)",
                (signature.as_string(db.connection),),
            ).fetchone()[0]
            assert body in source
            db.connection.execute(source.replace(body, body + "\n-- altered body\n"))
        elif tamper == "acl":
            db.connection.execute(
                sql.SQL("GRANT EXECUTE ON FUNCTION {} TO {}").format(
                    signature, sql.Identifier(_RUNTIME_ROLE)
                )
            )
        elif tamper == "overload":
            db.connection.execute(
                sql.SQL(
                    "CREATE FUNCTION {}(integer) RETURNS integer "
                    "LANGUAGE SQL AS 'SELECT $1'"
                ).format(sql.Identifier(db.name, name))
            )
        elif tamper == "signature":
            db.connection.execute(sql.SQL("DROP FUNCTION {}").format(signature))
            db.connection.execute(
                sql.SQL(
                    "CREATE FUNCTION {}(" + args + ") RETURNS integer "
                    "LANGUAGE SQL AS 'SELECT 1'"
                ).format(sql.Identifier(db.name, name))
            )
        elif tamper == "default":
            source = db.connection.execute(
                "SELECT pg_get_functiondef(%s::regprocedure)",
                (signature.as_string(db.connection),),
            ).fetchone()[0]
            # Last input gains a default; result columns/signature stay otherwise exact.
            token = (
                "selected_expected_oid oid"
                if target == "new"
                else "selected_source_id text"
            )
            assert token in source
            db.connection.execute(source.replace(token, token + (" DEFAULT NULL")))
        elif tamper == "dependency":
            db.connection.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS hstore"))
            db.connection.execute(
                sql.SQL("ALTER FUNCTION {} DEPENDS ON EXTENSION hstore").format(
                    signature
                )
            )
        else:
            operation = {
                "owner": "OWNER TO " + _RUNTIME_ROLE,
                "volatility": "VOLATILE" if target == "new" else "STABLE",
                "strict": "STRICT",
                "parallel": "PARALLEL SAFE",
                "leakproof": "LEAKPROOF",
                "security": "SECURITY INVOKER",
                "path": "SET search_path TO public",
                "cost": "COST 101",
                "rows": "ROWS 999",
                "support": "SUPPORT pg_catalog.textlike_support",
            }[tamper]
            db.connection.execute(
                sql.SQL("ALTER FUNCTION {} " + operation).format(signature)
            )
        with pytest.raises(m.M5RuntimeBundleError):
            m.require_m5_transition_context_access_bundle(
                db.connection, schema_name=db.name
            )


@pytest.mark.parametrize(
    "ddl",
    [
        "CREATE FUNCTION groundloop_extra() RETURNS int LANGUAGE SQL AS 'SELECT 1'",
        "CREATE TYPE groundloop_extra AS ENUM ('bad')",
        "CREATE INDEX extra_attached_index ON groundloop_m5_runtime_epoch(epoch_id)",
        "CREATE TRIGGER extra_attached_trigger BEFORE INSERT "
        "ON groundloop_m5_runtime_epoch "
        "FOR EACH ROW EXECUTE FUNCTION groundloop_m5_matching_deferred_validate()",
    ],
)
def test_closed_catalog_rejects_extra_function_type_index_trigger(ddl: str) -> None:
    with _schema() as db:
        m.install_m5_transition_context_access_bundle(db.connection)
        db.connection.execute(ddl)
        with pytest.raises(m.M5RuntimeBundleError):
            m.require_m5_transition_context_access_bundle(
                db.connection, schema_name=db.name
            )


def test_noncanonical_default_acl_causes_atomic_install_rejection() -> None:
    with _schema() as db:
        db.connection.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT EXECUTE ON FUNCTIONS TO {}"
            ).format(sql.Identifier(db.name), sql.Identifier(_RUNTIME_ROLE))
        )
        before = _installed_image(db)
        with pytest.raises(m.M5TransitionContextAccessBundleError, match="ACL"):
            m.install_m5_transition_context_access_bundle(db.connection)
        assert _installed_image(db) == before


@pytest.mark.parametrize("setting", ["READ ONLY", "ISOLATION LEVEL REPEATABLE READ"])
def test_installer_checks_actual_server_transaction_defaults(setting: str) -> None:
    with _schema() as db:
        db.connection.execute("SET SESSION CHARACTERISTICS AS TRANSACTION " + setting)
        try:
            with pytest.raises(m.M5TransitionContextAccessBundleError, match="server"):
                m.install_m5_transition_context_access_bundle(db.connection)
        finally:
            db.connection.execute(
                "SET SESSION CHARACTERISTICS AS TRANSACTION "
                "ISOLATION LEVEL READ COMMITTED, READ WRITE"
            )
        assert not m._m5_transition_access_new_names(db.connection, schema_name=db.name)


def test_installer_rejects_nested_and_closed_connections() -> None:
    with _schema() as db:
        before = _installed_image(db)
        with db.connection.transaction():
            with pytest.raises(m.M5TransitionContextAccessBundleError, match="idle"):
                m.install_m5_transition_context_access_bundle(db.connection)
        assert _installed_image(db) == before
    connection = psycopg.connect(_dsn(), autocommit=True)
    connection.close()
    with pytest.raises(m.M5TransitionContextAccessBundleError, match="idle"):
        m.install_m5_transition_context_access_bundle(connection)


@pytest.mark.parametrize("winner", ["first", "second"])
def test_two_installers_nowait_conflict_and_exact_retry(winner: str) -> None:
    with _schema() as db:
        held, release = Event(), Event()

        def install(label: str, pause: bool) -> Any:
            with psycopg.connect(_dsn(), autocommit=True) as connection:
                connection.execute(
                    sql.SQL("SET search_path TO {}, public").format(
                        sql.Identifier(db.name)
                    )
                )

                def cut(stage: str) -> None:
                    if pause and stage == "after_runtime_lock":
                        held.set()
                        assert release.wait(30), label

                return m.install_m5_transition_context_access_bundle(
                    connection, failure_injector=cut
                )

        with ThreadPoolExecutor(max_workers=2) as pool:
            leader = pool.submit(install, winner, True)
            try:
                assert held.wait(30)
                loser = pool.submit(
                    install, "second" if winner == "first" else "first", False
                )
                with pytest.raises(psycopg.errors.LockNotAvailable):
                    loser.result(timeout=30)
            finally:
                release.set()
            assert leader.result(timeout=30).applied
        assert not install("retry", False).applied


def test_absent_initial_ledger_then_competing_commit_rereads_under_lock() -> None:
    with _schema() as db:
        absent, release = Event(), Event()

        def late() -> Any:
            with psycopg.connect(_dsn(), autocommit=True) as connection:
                connection.execute(
                    sql.SQL("SET search_path TO {}, public").format(
                        sql.Identifier(db.name)
                    )
                )

                def cut(stage: str) -> None:
                    if stage == "after_initial_ledger":
                        absent.set()
                        assert release.wait(30)

                return m.install_m5_transition_context_access_bundle(
                    connection, failure_injector=cut
                )

        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(late)
            try:
                assert absent.wait(30)
                assert m.install_m5_transition_context_access_bundle(
                    db.connection
                ).applied
                before = _installed_image(db)
            finally:
                release.set()
            assert not pending.result(timeout=30).applied
        assert _installed_image(db) == before


@contextmanager
def _native_foundation(param: Any = None) -> Iterator[o33.OpenDatabase]:
    """Own accepted-parent fixture, never any held C2 checkout or authored successor."""
    with psycopg.connect(_dsn(), autocommit=True) as check:
        assert check.execute("SELECT current_database()").fetchone() == (
            "groundloop_s35_20261015",
        )
    generator = o33.open_db.__wrapped__(SimpleNamespace(param=param))
    foundation = next(generator)
    try:
        yield foundation
    finally:
        generator.close()


def _native_args(connection: Connection[Any]) -> tuple[Any, ...]:
    row = connection.execute(
        "SELECT current_setting('groundloop.m5_matching_epoch_id')::bigint,"
        "current_setting('groundloop.m5_matching_expected_revision')::bigint,"
        "current_setting('groundloop.m5_matching_resulting_revision')::bigint,"
        "current_setting('groundloop.m5_matching_source_kind'),"
        "current_setting('groundloop.m5_matching_source_id'),"
        "current_setting('groundloop.m5_matching_context_oid')::oid,"
        "current_setting('groundloop.m5_matching_journal_oid')::oid,"
        "current_setting('groundloop.m5_matching_expected_oid')::oid"
    ).fetchone()
    assert row is not None
    return row


def _grant_runtime(db: o33.OpenDatabase) -> None:
    for statement in (
        "GRANT USAGE ON SCHEMA {} TO {}",
        "GRANT ALL ON ALL TABLES IN SCHEMA {} TO {}",
        "GRANT USAGE ON ALL SEQUENCES IN SCHEMA {} TO {}",
    ):
        db.connection.execute(
            sql.SQL(statement).format(
                sql.Identifier(db.schema), sql.Identifier(_RUNTIME_ROLE)
            )
        )


def test_actual_native_all_fourteen_journal_projection_and_physical_images() -> None:
    """Real owner OPEN producers; full projection, no fabricated journal rows."""
    seen: set[str] = set()
    for variant in (True, "complete-changed"):
        with _native_foundation(variant) as foundation:
            m.install_m5_transition_context_access_bundle(foundation.connection)
            foundation.store._activate_document_open_prerequisite(foundation.request)
            db = _Schema(foundation.connection, foundation.schema)
            before = o33._stage_row_image(db.connection)
            visits: list[tuple[Any, ...]] = []

            def observe(
                stage: str, db: _Schema = db, before: Any = before, visits: Any = visits
            ) -> None:
                if stage != "o33_recovery_accounted":
                    return
                args = _native_args(db.connection)
                assert args[1:4] == (1, 1, "structural_open")
                context = db.connection.execute(_bridge_call(db), args).fetchall()
                assert (
                    context
                    == db.connection.execute(
                        "SELECT backend_pid,transaction_id::text,session_role::text,"
                        "epoch_id,expected_revision,resulting_revision,source_kind,"
                        "source_id,validation_started,validation_done "
                        "FROM pg_temp.groundloop_m5_matching_transition_context"
                    ).fetchall()
                )
                assert len(context) == 1 and context[0][-2:] == (False, False)
                rows = db.connection.execute(
                    _bridge_call(db, journal=True), args
                ).fetchall()
                raw = db.connection.execute(
                    "SELECT relation_name,key_preimage,first_old,final_new,"
                    "first_operation,"
                    "last_operation,mutation_count,saw_insert,saw_update,saw_delete "
                    "FROM pg_temp.groundloop_m5_matching_change_journal "
                    "WHERE relation_name=ANY(%s)",
                    (list(_STAGE_JOURNAL_RELATIONS),),
                ).fetchall()

                def canonical(row: Any) -> Any:
                    return row[0], bytes(row[1])

                assert sorted(rows, key=canonical) == sorted(raw, key=canonical)
                assert len({canonical(row) for row in rows}) == len(rows)
                after = o33._stage_row_image(db.connection)
                changed = {
                    key
                    for key in before.keys() | after.keys()
                    if key[0] in _STAGE_JOURNAL_RELATIONS
                    and before.get(key) != after.get(key)
                }
                assert {canonical(row) for row in rows} == changed
                for row in rows:
                    key = canonical(row)
                    assert row[2:4] == (before.get(key), after.get(key))
                    operation = "UPDATE" if key in before else "INSERT"
                    assert row[4:] == (
                        operation,
                        operation,
                        1,
                        operation == "INSERT",
                        operation == "UPDATE",
                        False,
                    )
                seen.update(row[0] for row in rows)
                # Projection is repeatable and does not start validation.
                assert (
                    db.connection.execute(
                        _bridge_call(db, journal=True), args
                    ).fetchall()
                    == rows
                )
                assert (
                    db.connection.execute(_bridge_call(db), args).fetchall() == context
                )
                visits.extend(rows)

            receipt = o33._open(
                foundation,
                foundation.plan("s35-full-journal-" + str(variant)),
                failure_injector=observe,
            )
            assert visits and not receipt.replayed
    assert seen == set(_STAGE_JOURNAL_RELATIONS)


def test_actual_fresh_login_open_context_empty_journal_and_rollback() -> None:
    """Actual LOGIN O33 source callback, NOT completed C2/nonowner matching."""
    with _native_foundation() as foundation:
        m.install_m5_transition_context_access_bundle(foundation.connection)
        foundation.store._activate_document_open_prerequisite(foundation.request)
        _grant_runtime(foundation)
        plan = foundation.plan("s35-login-structural")
        before = o33._table_image(foundation.connection)
        with psycopg.connect(_dsn(), user=_RUNTIME_ROLE, autocommit=True) as connection:
            connection.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(foundation.schema)
                )
            )
            ports = o33.PostgresM4ApplicationPorts(
                connection,
                structural_payloads={},
                execution_mode=o33.M4ExecutionMode.MEASURED,
            )
            store = o33.PostgresM5RuntimeStore(connection)
            fresh = replace(foundation, connection=connection, ports=ports, store=store)
            arguments = o33._open_arguments(fresh, plan)
            visits: list[Any] = []

            def observe(stage: str) -> None:
                if stage != "o33_source_deactivation_written":
                    return
                db = _Schema(connection, foundation.schema)
                args = _native_args(connection)
                assert args[1:5] == (1, 1, "structural_open", plan.structural_event_id)
                context = connection.execute(_bridge_call(db), args).fetchall()
                identity = connection.execute(
                    "SELECT pg_backend_pid(),"
                    "pg_current_xact_id_if_assigned()::text,session_user::text"
                ).fetchone()
                assert len(context) == 1 and context[0][:3] == identity
                assert context[0][3:] == (*args[:5], False, False)
                assert not connection.execute(
                    _bridge_call(db, journal=True), args
                ).fetchall()
                for suffix in (
                    "transition_context",
                    "change_journal",
                    "expected_changes",
                ):
                    with (
                        pytest.raises(psycopg.errors.InsufficientPrivilege),
                        connection.transaction(),
                    ):
                        connection.execute(
                            sql.SQL("SELECT * FROM pg_temp.{}").format(
                                sql.Identifier("groundloop_m5_matching_" + suffix)
                            )
                        )
                visits.append(context)
                raise o33._ControlledCut("S35 observed genuine LOGIN context")

            with pytest.raises(o33._ControlledCut, match="genuine LOGIN"):
                store._open_document_open_prerequisite(
                    plan, **arguments, failure_injector=observe
                )
            assert visits
        assert o33._table_image(foundation.connection) == before


def test_actual_held_checked_transition_blocks_first_install_then_retry() -> None:
    """Real native authorizer holds ROW SHARE, not a manually substituted lock."""
    with _native_foundation() as foundation:
        foundation.store._activate_document_open_prerequisite(foundation.request)
        before = o33._table_image(foundation.connection)
        cuts: list[str] = []

        def observe(stage: str) -> None:
            if stage != "o33_source_deactivation_written":
                return
            assert _native_args(foundation.connection)[3] == "structural_open"
            with psycopg.connect(_dsn(), autocommit=True) as installer:
                installer.execute(
                    sql.SQL("SET search_path TO {}, public").format(
                        sql.Identifier(foundation.schema)
                    )
                )
                with pytest.raises(psycopg.errors.LockNotAvailable):
                    m.install_m5_transition_context_access_bundle(
                        installer, failure_injector=cuts.append
                    )
                assert not m._m5_transition_access_new_names(
                    installer, schema_name=foundation.schema
                )
                assert (
                    m._m5_readiness_bundle_row(
                        installer,
                        m.M5_TRANSITION_CONTEXT_ACCESS_BUNDLE_ID,
                        schema_name=foundation.schema,
                    )
                    is None
                )
            raise o33._ControlledCut("held transition blocks022")

        with pytest.raises(o33._ControlledCut, match="blocks022"):
            o33._open(
                foundation,
                foundation.plan("s35-install-race"),
                failure_injector=observe,
            )
        assert cuts == [
            "after_initial_ledger",
            "after_prerequisite",
            "after_ledger_lock",
        ]
        assert o33._table_image(foundation.connection) == before
        assert m.install_m5_transition_context_access_bundle(
            foundation.connection
        ).applied


def _native_m4_predecessor(foundation: o33.OpenDatabase) -> o33.OpenDatabase:
    """Native M4 zero-job deletion/seal creates revision>1, never authored headers.

    The second document is initial bootstrap fixture input, not a later state.
    Accepted oracle SELECT bodies change only temporary VIEW lifetime, matching
    the accepted-parent diagnostic pattern, then are removed before strict checks.
    """
    connection = foundation.connection
    with connection.transaction():
        connection.execute(
            "INSERT INTO groundloop_document VALUES "
            "('s35-disposable','fixture://s35-disposable','test')"
        )
        connection.execute(
            "INSERT INTO groundloop_document_version VALUES "
            "('s35-disposable-v1','s35-disposable',%s,%s,NULL)",
            (o33.sha("s35-disposable"), foundation.base.epoch_id),
        )
        connection.execute(
            "INSERT INTO groundloop_chunk_version VALUES "
            "('s35-disposable-chunk','s35-disposable-v1',0,'disposable',%s,"
            "'fixture-v1',%s,NULL)",
            (o33.sha("disposable"), foundation.base.epoch_id),
        )
        connection.execute(
            "INSERT INTO groundloop_chunk_provenance VALUES "
            "('s35-disposable-chunk','o33-chunker',%s)",
            (o33.sha("s35-disposable"),),
        )
    views: list[str] = []
    for filename, count in (
        ("m2_full_recompute_oracle.sql", 6),
        ("m4_working_full_recompute_oracle.sql", 3),
    ):
        original = (m.ROOT / "sql" / filename).read_text()
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
        views.extend(names)
    try:
        event = o33.DeleteDocumentVersionEvent(
            "s35-native-m4-predecessor", "s35-disposable-v1"
        )
        plan = o33.DynamicEventPlan(
            o33.CorpusUpdateIdentity(
                event.event_id,
                o33.legacy_event_payload_digest(event),
                o33.UpdateKind.DELETE,
                foundation.base.epoch_id,
                foundation.policy.candidate_policy_id,
            ),
            (),
            ("s35-disposable-chunk",),
            (),
            o33.REGISTRY_ID,
        )
        ports = o33.PostgresM4ApplicationPorts(
            connection,
            structural_payloads={
                event.event_id: o33.StructuralPayload(
                    deactivated_document_version_id="s35-disposable-v1"
                )
            },
            execution_mode=o33.M4ExecutionMode.MEASURED,
        )
        admission, verifier = o33.ScriptedAdmission({}), o33.ScriptedVerifier({})
        app = o33.M4Application(
            structural=ports,
            runtime=ports,
            admission=o33.PersistingAdmissionPort(connection, admission),
            verifier=verifier,
            observations=ports,
            equality_gates=ports,
            publication=ports,
            execution_policy=foundation.execution,
        )
        result = app.run_event(plan)
        assert result.state is o33.EventRunState.SEALED
        assert admission.calls == verifier.calls == 0
        header = connection.execute(
            "SELECT revision,semantic_status,publication_mode "
            "FROM groundloop_epoch WHERE epoch_id=%s",
            (result.epoch_id,),
        ).fetchone()
        assert (
            header is not None and header[0] > 1 and header[1:] == ("sealed", "strict")
        )
    finally:
        for name in reversed(views):
            connection.execute(
                sql.SQL("DROP VIEW {}.{} RESTRICT").format(
                    sql.Identifier("pg_temp"), sql.Identifier(name)
                )
            )
    m.require_m5_document_update_completion_bundle(
        connection, schema_name=foundation.schema
    )
    ports = o33.PostgresM4ApplicationPorts(
        connection, structural_payloads={}, execution_mode=o33.M4ExecutionMode.MEASURED
    )
    store = o33.PostgresM5RuntimeStore(connection)
    return replace(
        foundation,
        base=replace(foundation.base, epoch_id=result.epoch_id),
        ports=ports,
        store=store,
        request=store.prepare_activation_request("s35-native-predecessor"),
    )


@pytest.mark.parametrize("runtime", [False, True])
def test_native_sealed_predecessor_gt_one_still_structural_context_one_one(
    runtime: bool,
) -> None:
    with _native_foundation() as initial:
        foundation = _native_m4_predecessor(initial)
        m.install_m5_transition_context_access_bundle(foundation.connection)
        foundation.store._activate_document_open_prerequisite(foundation.request)
        _grant_runtime(foundation)
        before = o33._table_image(foundation.connection)
        visits: list[Any] = []
        with psycopg.connect(
            _dsn(), user=_RUNTIME_ROLE if runtime else "groundloop", autocommit=True
        ) as connection:
            connection.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(foundation.schema)
                )
            )
            ports = o33.PostgresM4ApplicationPorts(
                connection,
                structural_payloads={},
                execution_mode=o33.M4ExecutionMode.MEASURED,
            )
            fresh = replace(
                foundation,
                connection=connection,
                ports=ports,
                store=o33.PostgresM5RuntimeStore(connection),
            )

            def observe(stage: str) -> None:
                if stage != "o33_source_deactivation_written":
                    return
                args = _native_args(connection)
                assert args[1:4] == (1, 1, "structural_open")
                db = _Schema(connection, foundation.schema)
                rows = connection.execute(_bridge_call(db), args).fetchall()
                assert len(rows) == 1 and rows[0][4:6] == (1, 1)
                assert rows[0][2] == (_RUNTIME_ROLE if runtime else "groundloop")
                visits.append(rows)
                raise o33._ControlledCut("native sealed predecessor inspected")

            with pytest.raises(o33._ControlledCut, match="predecessor inspected"):
                o33._open(
                    fresh,
                    fresh.plan("s35-native-predecessor-" + str(runtime)),
                    failure_injector=observe,
                )
            assert visits
        assert o33._table_image(foundation.connection) == before


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "extra",
        "duplicate",
        "first_old",
        "final_new",
        "first_operation",
        "last_operation",
        "mutation_count",
        "saw_insert",
        "saw_update",
        "saw_delete",
    ],
)
def test_actual_journal_damage_is_not_concealed_and_native_comparison_rejects(
    damage: str,
) -> None:
    """Owner NEGATIVE corruption only; real production comparison stays enabled."""
    with _native_foundation(True) as foundation:
        connection = foundation.connection
        m.install_m5_transition_context_access_bundle(connection)
        foundation.store._activate_document_open_prerequisite(foundation.request)
        before = o33._table_image(connection)
        observed: list[Any] = []

        def damage_at(stage: str) -> None:
            if stage != "o33_recovery_accounted":
                return
            db = _Schema(connection, foundation.schema)
            args = _native_args(connection)
            original = connection.execute(
                _bridge_call(db, journal=True), args
            ).fetchall()
            assert original
            row = original[0]
            key = (row[0], bytes(row[1]))
            table = "pg_temp.groundloop_m5_matching_change_journal"
            if damage == "missing":
                connection.execute(
                    "DELETE FROM "
                    + table
                    + " WHERE relation_name=%s AND key_preimage=%s",
                    key,
                )
            elif damage in ("extra", "duplicate"):
                if damage == "duplicate":
                    constraint = connection.execute(
                        "SELECT conname FROM pg_constraint "
                        "WHERE conrelid=%s::regclass AND contype='p'",
                        (table,),
                    ).fetchone()
                    assert constraint is not None
                    connection.execute(
                        sql.SQL("ALTER TABLE " + table + " DROP CONSTRAINT {}").format(
                            sql.Identifier(constraint[0])
                        )
                    )
                columns = (
                    "relation_name,key_preimage,first_old,final_new,first_operation,"
                    "last_operation,mutation_count,saw_insert,saw_update,saw_delete"
                )
                key_expression = (
                    "key_preimage" if damage == "duplicate" else "'extra-key'::bytea"
                )
                connection.execute(
                    "INSERT INTO " + table + " (" + columns + ") SELECT "
                    "relation_name,"
                    + key_expression
                    + ",first_old,final_new,first_operation,"
                    "last_operation,mutation_count,saw_insert,saw_update,saw_delete "
                    "FROM " + table + " WHERE relation_name=%s AND key_preimage=%s",
                    key,
                )
            else:
                expression = {
                    "first_old": "'{}'::jsonb",
                    "final_new": "'{}'::jsonb",
                    "first_operation": "'DELETE'",
                    "last_operation": "'DELETE'",
                    "mutation_count": "mutation_count+1",
                    "saw_insert": "NOT saw_insert",
                    "saw_update": "NOT saw_update",
                    "saw_delete": "true",
                }[damage]
                connection.execute(
                    sql.SQL(
                        "UPDATE "
                        + table
                        + " SET {}={}"
                        + " WHERE relation_name=%s AND key_preimage=%s"
                    ).format(sql.Identifier(damage), sql.SQL(expression)),
                    key,
                )
            mutated = connection.execute(
                _bridge_call(db, journal=True), args
            ).fetchall()
            assert mutated != original
            if damage == "duplicate":
                assert len(mutated) > len({(r[0], bytes(r[1])) for r in mutated})
            if damage == "extra":
                assert any(bytes(r[1]) == b"extra-key" for r in mutated)
            observed.append(mutated)

        with pytest.raises(o33.EventConflictError, match="matching stage journal"):
            o33._open(
                foundation,
                foundation.plan("s35-journal-negative-" + damage),
                failure_injector=damage_at,
            )
        assert observed and o33._table_image(connection) == before


def test_exact_installer_replay_does_not_take_install_locks() -> None:
    with _schema() as db:
        m.install_m5_transition_context_access_bundle(db.connection)
        before = _installed_image(db)
        with psycopg.connect(_dsn(), autocommit=True) as blocker, blocker.transaction():
            for name in ("groundloop_m5_schema_bundle", "groundloop_m5_runtime_epoch"):
                blocker.execute(
                    sql.SQL("LOCK TABLE {} IN EXCLUSIVE MODE NOWAIT").format(
                        sql.Identifier(db.name, name)
                    )
                )
            db.connection.execute("SET lock_timeout TO '2s'")
            assert not m.install_m5_transition_context_access_bundle(
                db.connection
            ).applied
        assert _installed_image(db) == before


def test_installer_first_native_runtime_waits_then_uses_new_authority() -> None:
    with _native_foundation() as foundation:
        foundation.store._activate_document_open_prerequisite(foundation.request)
        before = o33._table_image(foundation.connection)
        held, release, started = Event(), Event(), Event()
        pids: list[int] = []
        observed: list[Any] = []

        def install() -> Any:
            with psycopg.connect(_dsn(), autocommit=True) as connection:
                connection.execute(
                    sql.SQL("SET search_path TO {}, public").format(
                        sql.Identifier(foundation.schema)
                    )
                )

                def cut(stage: str) -> None:
                    if stage == "after_runtime_lock":
                        held.set()
                        assert release.wait(30)

                return m.install_m5_transition_context_access_bundle(
                    connection, failure_injector=cut
                )

        def transition() -> None:
            with psycopg.connect(_dsn(), autocommit=True) as connection:
                connection.execute(
                    sql.SQL("SET search_path TO {}, public").format(
                        sql.Identifier(foundation.schema)
                    )
                )
                pids.append(connection.info.backend_pid)
                fresh = replace(
                    foundation,
                    connection=connection,
                    store=o33.PostgresM5RuntimeStore(connection),
                    ports=o33.PostgresM4ApplicationPorts(
                        connection,
                        structural_payloads={},
                        execution_mode=o33.M4ExecutionMode.MEASURED,
                    ),
                )

                def observe(stage: str) -> None:
                    if stage == "o33_source_deactivation_written":
                        db = _Schema(connection, foundation.schema)
                        rows = connection.execute(
                            _bridge_call(db), _native_args(connection)
                        ).fetchall()
                        assert len(rows) == 1
                        observed.append(rows)
                        raise o33._ControlledCut("post-install native context")

                started.set()
                with pytest.raises(o33._ControlledCut, match="post-install"):
                    o33._open(
                        fresh,
                        fresh.plan("s35-installer-first"),
                        failure_injector=observe,
                    )

        with ThreadPoolExecutor(max_workers=2) as pool:
            installer = pool.submit(install)
            try:
                assert held.wait(30)
                runtime = pool.submit(transition)
                assert started.wait(30)
                # Fresh OPEN first requests RowExclusive for its runtime INSERT;
                # a retained checked transition holds at least native RowShare.
                # Observe the actual pre-phase wait, not a timer inference.
                deadline = o33.time.monotonic() + 20
                waiting = False
                while o33.time.monotonic() < deadline:
                    wait_modes = foundation.connection.execute(
                        "SELECT mode "
                        "FROM pg_locks WHERE pid=%s AND relation=%s::regclass "
                        "AND mode IN ('RowShareLock','RowExclusiveLock') "
                        "AND NOT granted",
                        (pids[0], foundation.schema + ".groundloop_m5_runtime_epoch"),
                    ).fetchall()
                    waiting = bool(wait_modes)
                    if waiting:
                        break
                    Event().wait(0.02)
                assert waiting and not runtime.done()
                assert wait_modes == [("RowExclusiveLock",)]
            finally:
                release.set()
            assert installer.result(timeout=30).applied
            runtime.result(timeout=30)
        assert observed
        after = o33._table_image(foundation.connection)
        ledger_name = "groundloop_m5_schema_bundle"
        assert tuple(row for row in after if row[0] != ledger_name) == tuple(
            row for row in before if row[0] != ledger_name
        )
        before_ledger, after_ledger = (
            dict(before)[ledger_name],
            dict(after)[ledger_name],
        )
        assert set(before_ledger) < set(after_ledger)
        assert len(after_ledger) == len(before_ledger) + 1
        m.require_m5_transition_context_access_bundle(
            foundation.connection, schema_name=foundation.schema
        )


@pytest.mark.parametrize(
    "field",
    [
        "bundle_id",
        "bundle_sha256",
        "migration_sha256",
        "oracle_sha256",
        "prerequisite_sha256",
    ],
)
def test_every_successor_ledger_field_is_strict(field: str) -> None:
    with _schema() as db:
        m.install_m5_transition_context_access_bundle(db.connection)
        _negative_ledger_damage(
            db,
            sql.SQL(
                "UPDATE groundloop_m5_schema_bundle SET {}=%s WHERE bundle_id=%s"
            ).format(sql.Identifier(field)),
            (
                "foreign-successor" if field == "bundle_id" else "0" * 64,
                m.M5_TRANSITION_CONTEXT_ACCESS_BUNDLE_ID,
            ),
        )
        before = _installed_image(db)
        with pytest.raises(m.M5TransitionContextAccessBundleError):
            m.require_m5_document_update_completion_bundle(
                db.connection, schema_name=db.name
            )
        assert _installed_image(db) == before


@pytest.mark.parametrize(
    "attribute,value",
    [
        ("read_only", True),
        ("isolation_level", psycopg.IsolationLevel.REPEATABLE_READ),
        ("isolation_level", psycopg.IsolationLevel.SERIALIZABLE),
    ],
)
def test_explicit_client_transaction_settings_rejected(
    attribute: str, value: Any
) -> None:
    with _schema() as db:
        setattr(db.connection, attribute, value)
        try:
            with pytest.raises(
                m.M5TransitionContextAccessBundleError, match="read-write"
            ):
                m.install_m5_transition_context_access_bundle(db.connection)
        finally:
            setattr(db.connection, attribute, None)
        assert not m._m5_transition_access_new_names(db.connection, schema_name=db.name)


@pytest.mark.parametrize("kind", ["requirement_completion", "direct_transition"])
def test_actual_owner_and_login_nonempty_native_trigger_journal_agree(
    raw_epoch: _RawEpoch,
    kind: str,
) -> None:
    """Rollback-only SQL business-stage shape, not a completed C2/direct source.

    Both independent sessions use public native authorizers and ordinary
    permanent matching DML. Unchanged017 triggers produce every journal value;
    no temporary row is inserted, transferred, granted or fabricated by test code.
    """
    projections: list[Any] = []
    persistent_before = o33._table_image(raw_epoch.db.connection)
    for runtime in (False, True):
        with _context(raw_epoch, runtime=runtime, kind=kind) as (db, args):
            before = db.connection.execute(
                "SELECT to_jsonb(t) FROM "
                "groundloop_m5_matching_image_working t WHERE epoch_id=%s",
                (raw_epoch.epoch,),
            ).fetchone()[0]
            for _ in range(2):
                assert (
                    db.connection.execute(
                        "UPDATE groundloop_m5_matching_image_working "
                        "SET updated_revision=%s WHERE epoch_id=%s",
                        (args[2], raw_epoch.epoch),
                    ).rowcount
                    == 1
                )
            final = db.connection.execute(
                "SELECT to_jsonb(t) FROM "
                "groundloop_m5_matching_image_working t WHERE epoch_id=%s",
                (raw_epoch.epoch,),
            ).fetchone()[0]
            relation = "groundloop_m5_matching_image_working"
            key = b"".join(
                len(v.encode()).to_bytes(8, "big") + v.encode()
                for v in (relation, str(raw_epoch.epoch))
            )
            expected = [
                (
                    relation,
                    key,
                    before,
                    final,
                    "UPDATE",
                    "UPDATE",
                    2,
                    False,
                    True,
                    False,
                )
            ]
            rows = db.connection.execute(
                _bridge_call(db, journal=True), args
            ).fetchall()
            assert rows == expected
            image = o33.database_snapshot(db.connection)
            context = db.connection.execute(_bridge_call(db), args).fetchall()
            for _ in range(2):
                assert (
                    db.connection.execute(
                        _bridge_call(db, journal=True), args
                    ).fetchall()
                    == rows
                )
                assert (
                    db.connection.execute(_bridge_call(db), args).fetchall() == context
                )
            assert o33.database_snapshot(db.connection) == image
            if runtime:
                with (
                    pytest.raises(psycopg.errors.InsufficientPrivilege),
                    db.connection.transaction(),
                ):
                    db.connection.execute(
                        "SELECT * FROM pg_temp.groundloop_m5_matching_change_journal"
                    )
            projections.append(rows)
    assert projections[0] == projections[1]
    assert o33._table_image(raw_epoch.db.connection) == persistent_before


@pytest.mark.parametrize("pair", [(5, 6), (5, 7), (6, 7)])
@pytest.mark.parametrize("journal", [False, True])
def test_equal_oid_inputs_fail_at_argument_boundary(
    raw_epoch: _RawEpoch, pair: tuple[int, int], journal: bool
) -> None:
    with _context(raw_epoch) as (db, original):
        args = list(original)
        args[pair[1]] = args[pair[0]]
        with (
            pytest.raises(psycopg.errors.RaiseException, match="arguments"),
            db.connection.transaction(),
        ):
            db.connection.execute(_bridge_call(db, journal=journal), args)


@pytest.mark.parametrize("setting", _ACCESS_SETTINGS)
@pytest.mark.parametrize("empty", [False, True], ids=["absent", "empty"])
def test_every_coordinate_guc_absent_or_empty_fails_before_temp_lookup(
    raw_epoch: _RawEpoch,
    setting: str,
    empty: bool,
) -> None:
    # Controlled NEGATIVE settings envelope, never a fabricated positive triplet.
    with _context(raw_epoch) as (native, args):
        settings = dict(
            native.connection.execute(
                "SELECT name,current_setting(name) "
                "FROM unnest(%s::text[]) AS names(name)",
                (list(_ACCESS_SETTINGS),),
            ).fetchall()
        )
    with psycopg.connect(_dsn(), user=_RUNTIME_ROLE, autocommit=True) as connection:
        db = _Schema(connection, raw_epoch.db.name)
        assert connection.execute(
            "SELECT current_setting(%s,true)", (setting,)
        ).fetchone() == (None,)
        with connection.transaction(force_rollback=True):
            for name, value in settings.items():
                if name != setting or empty:
                    connection.execute(
                        "SELECT set_config(%s,%s,true)",
                        (name, "" if name == setting else value),
                    )
            for journal in (False, True):
                with (
                    pytest.raises(psycopg.errors.RaiseException, match="settings"),
                    connection.transaction(),
                ):
                    connection.execute(_bridge_call(db, journal=journal), args)


@pytest.mark.parametrize(
    "offset,label", [(5, "context"), (6, "journal"), (7, "expected")]
)
@pytest.mark.parametrize("prefix", ["0", "+", " "])
def test_noncanonical_oid_guc_rejected(
    raw_epoch: _RawEpoch, offset: int, label: str, prefix: str
) -> None:
    with _context(raw_epoch) as (db, args):
        db.connection.execute(
            "SELECT set_config(%s,%s,true)",
            ("groundloop.m5_matching_" + label + "_oid", prefix + str(args[offset])),
        )
        for journal in (False, True):
            with (
                pytest.raises(psycopg.errors.RaiseException, match="settings"),
                db.connection.transaction(),
            ):
                db.connection.execute(_bridge_call(db, journal=journal), args)


@pytest.mark.parametrize(
    "offset,label,relation",
    [
        (5, "context", "transition_context"),
        (6, "journal", "change_journal"),
        (7, "expected", "expected_changes"),
    ],
)
@pytest.mark.parametrize("replacement", ["view", "permanent"])
def test_rebound_true_oid_wrong_kind_or_namespace_is_not_genuine(
    raw_epoch: _RawEpoch, offset: int, label: str, relation: str, replacement: str
) -> None:
    with _context(raw_epoch) as (db, original):
        name = "groundloop_m5_matching_" + relation
        args = list(original)
        if replacement == "view":
            db.connection.execute(
                sql.SQL("DROP TABLE pg_temp.{}").format(sql.Identifier(name))
            )
            db.connection.execute(
                sql.SQL("CREATE TEMP VIEW {} AS SELECT 1 AS decoy").format(
                    sql.Identifier(name)
                )
            )
            qualified = "pg_temp." + name
        else:
            db.connection.execute(
                sql.SQL("CREATE TABLE {}.{} (decoy int)").format(
                    sql.Identifier(db.name), sql.Identifier(name)
                )
            )
            qualified = db.name + "." + name
        args[offset] = db.connection.execute(
            "SELECT %s::regclass::oid", (qualified,)
        ).fetchone()[0]
        db.connection.execute(
            "SELECT set_config(%s,%s,true)",
            ("groundloop.m5_matching_" + label + "_oid", str(args[offset])),
        )
        expected = "not genuine" if replacement == "view" else "absent or replaced"
        for journal in (False, True):
            with (
                pytest.raises(psycopg.errors.RaiseException, match=expected),
                db.connection.transaction(),
            ):
                db.connection.execute(_bridge_call(db, journal=journal), args)


def test_pinned_bridge_ignores_poisoned_caller_path_functions_operators_and_types(
    raw_epoch: _RawEpoch,
) -> None:
    with _context(raw_epoch) as (db, args):
        namespace = "s35_poison_" + uuid.uuid4().hex
        db.connection.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(namespace))
        )
        for name, signature, result in (
            ("btrim", "text", "text"),
            ("current_setting", "text,boolean", "text"),
            (
                "groundloop_m5_matching_private_temp_triplet",
                "regclass,regclass,regclass",
                "boolean",
            ),
            ("equal_int8", "bigint,bigint", "boolean"),
        ):
            db.connection.execute(
                sql.SQL(
                    "CREATE FUNCTION {}("
                    + signature
                    + ") RETURNS "
                    + result
                    + " LANGUAGE plpgsql AS $$ BEGIN "
                    "RAISE EXCEPTION 'poison invoked'; END $$"
                ).format(sql.Identifier(namespace, name))
            )
        db.connection.execute(
            sql.SQL(
                "CREATE OPERATOR {}.=(FUNCTION={}.equal_int8,"
                "LEFTARG=pg_catalog.int8,RIGHTARG=pg_catalog.int8)"
            ).format(sql.Identifier(namespace), sql.Identifier(namespace))
        )
        db.connection.execute(
            sql.SQL("CREATE DOMAIN {}.text AS pg_catalog.text").format(
                sql.Identifier(namespace)
            )
        )
        db.connection.execute(
            sql.SQL("CREATE TEMP TABLE groundloop_m5_schema_bundle (decoy int)")
        )
        db.connection.execute(
            sql.SQL("SET LOCAL search_path TO {}, pg_temp, {}, pg_catalog").format(
                sql.Identifier(namespace), sql.Identifier(db.name)
            )
        )
        # All casts/calls use the captured permanent schema and pg_catalog.
        assert len(db.connection.execute(_bridge_call(db), args).fetchall()) == 1
        assert not db.connection.execute(
            _bridge_call(db, journal=True), args
        ).fetchall()


def test_admin_never_combines_foreign_ledger_with_local_decoy_bridges() -> None:
    with _schema() as real, _schema() as decoy:
        m.install_m5_transition_context_access_bundle(real.connection)
        # Decoy schema has real021 but no022 authority; same names cannot borrow
        # the other schema's exact022 ledger. No forged ledger is authored.
        for name in m._m5_transition_access_names:
            decoy.connection.execute(
                sql.SQL(
                    "CREATE FUNCTION {}(bigint,bigint,bigint,"
                    "text,text,oid,oid,oid) RETURNS int LANGUAGE SQL AS 'SELECT 1'"
                ).format(sql.Identifier(decoy.name, name))
            )
        decoy.connection.execute(
            sql.SQL("SET search_path TO {}, {}, public").format(
                sql.Identifier(decoy.name), sql.Identifier(real.name)
            )
        )
        for require in (
            m.require_m5_transition_context_access_bundle,
            m.require_m5_document_update_completion_bundle,
        ):
            with pytest.raises(m.M5TransitionContextAccessBundleError):
                require(decoy.connection, schema_name=decoy.name)
        m.require_m5_transition_context_access_bundle(
            real.connection, schema_name=real.name
        )


@pytest.mark.parametrize("runtime", [False, True])
def test_context_preserves_nonblank_unicode_source_identity_without_trimming(
    raw_epoch: _RawEpoch,
    runtime: bool,
) -> None:
    source = "  α/source identity\t  "
    with _context(raw_epoch, runtime=runtime, source_id=source) as (db, args):
        rows = db.connection.execute(_bridge_call(db), args).fetchall()
        assert len(rows) == 1 and rows[0][7] == source and args[4] == source
        wrong = list(args)
        wrong[4] = source.strip()
        with (
            pytest.raises(psycopg.errors.RaiseException, match="settings"),
            db.connection.transaction(),
        ):
            db.connection.execute(_bridge_call(db), wrong)


def test_settings_falsifiers_cover_exact_sql_setting_inventory() -> None:
    source = m.M5_TRANSITION_CONTEXT_ACCESS_MIGRATION_PATH.read_text()
    assert set(re.findall(r"current_setting\('([^']+)'", source)) == set(
        _ACCESS_SETTINGS
    )
