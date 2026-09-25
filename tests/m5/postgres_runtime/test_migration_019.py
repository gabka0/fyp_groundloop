"""Contract and live PostgreSQL tests for M5-D31 migration 019."""

from __future__ import annotations

import hashlib
import importlib
import os
import re
import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import Connection, IsolationLevel, sql
from psycopg.pq import TransactionStatus

from groundloop.m5.digests import hash_field, stable_m5_digest, text_field
from groundloop.postgres.migrations import (
    M5_ACCEPTED_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_SHA256,
    M5_ACCEPTED_BOUNDED_DOCUMENT_WITHDRAWAL_MIGRATION_SHA256,
    M5_ACCEPTED_PRETERMINAL_SEAL_CONTEXT_BUNDLE_SHA256,
    M5_ACCEPTED_PRETERMINAL_SEAL_CONTEXT_MIGRATION_SHA256,
    M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID,
    M5_PRETERMINAL_SEAL_CONTEXT_BUNDLE_ID,
    M5_PRETERMINAL_SEAL_CONTEXT_MIGRATION_LABEL,
    M5_PRETERMINAL_SEAL_CONTEXT_MIGRATION_PATH,
    M5_PRETERMINAL_SEAL_CONTEXT_ORACLE_SHA256,
    M5BundleHashConflictError,
    M5PrerequisiteError,
    M5PreterminalSealContextBundleError,
    M5PreterminalSealContextBundleIdentity,
    M5PreterminalSealContextBundleInstallResult,
    apply_legacy_migrations,
    install_m5_bounded_document_withdrawal_bundle,
    install_m5_core_bundle,
    install_m5_persisted_matching_bundle,
    install_m5_preterminal_seal_context_bundle,
    install_m5_runtime_bundle,
    install_m5_runtime_recovery_bundle,
    m5_preterminal_seal_context_bundle_identity,
)

EXPECTED_MIGRATION_SHA256 = (
    "f92c02a365ac43a26f3291718866436b19f69924eb7a8c2b77af0312f82b536e"
)
EXPECTED_BUNDLE_SHA256 = (
    "e12d4abd95a9b2ef49010a43d6b708824462a80dfed2548107e175920dabe481"
)
EXPECTED_ORACLE_SHA256 = hashlib.sha256(b"").hexdigest()
EXPECTED_018_LEDGER = (
    "m5-bounded-document-withdrawal-schema-bundle-v1",
    "9c45e58fb5c61156d4d07aa0c9b767112bf285452664f39731d893445e7a9e4f",
    "941bba975c12e9fb5ba4b4f75a82e59fa518b23eac34468ed2f8b15d1cd9ed90",
    EXPECTED_ORACLE_SHA256,
    "52240e19968926d0c051fe6146b3c7d877cf582014341efbfcc78637f3ff5761",
)
FROZEN_MIGRATION_HASHES = {
    "migrations/001_m2_base.sql": (
        "ddbfdc1cedec89d9ffd852b66ece4192763099ee34429d5fbb85e54a3a196e37"
    ),
    "migrations/002_m3_static_ai.sql": (
        "8812d0438ac9048a38a001711e8b575a6fb7ed7917e49664c8d8f3a0573ee56d"
    ),
    "migrations/003_m4_dynamic_impact.sql": (
        "fcbe9eaa2ef281cfdc03ddaf25fec3b5dbef73ce6ee957a76b7a3e68b729862c"
    ),
    "migrations/004_m4_working_publication.sql": (
        "db73d14810f819f17239a0f3e6d687175af17e7fc17fd7fb0d95bb594e7fdaf6"
    ),
    "migrations/005_m4_durable_execution.sql": (
        "f2ef9471fc29a9da4974545f282d12a9a6e92eaa71e096b0a3ab122035622b89"
    ),
    "migrations/006_m4_role_embedding_artifacts.sql": (
        "9cc1b23b8adeac087d827a00488e531045f89e8264fcfd6539a11b4a6abf1e5e"
    ),
    "migrations/007_m4_structural_overlay.sql": (
        "5852e9ac4cf56d068c3583c227ed6ff8f9185a97c91d904b9109bffc0fb9e46d"
    ),
    "migrations/008_m4_atomic_discovery.sql": (
        "20d7b8f3d9de96d93312ebb74747febf5a11ca7ad23482ad9590f636624afddb"
    ),
    "migrations/009_m4_incremental_execution.sql": (
        "f345de0034e979e3b7ee5df42cb5b6745c081bbb545590124cab4c3adafe9f84"
    ),
    "migrations/010_m4_event_audit.sql": (
        "26be7f1fc65f934804c22f022b6f1139bc00ae50a4b973458e1d486c8a9fa493"
    ),
    "migrations/011_m4_interval_exclusion.sql": (
        "9200f962fd5a8a631b33dc43e01fd4f795f633ad72f9d20a9fac58e68cc7c18d"
    ),
    "migrations/012_m4_point_runtime_counters.sql": (
        "b754a3915d734db4b7af28f72655633af9cf693cdac37832c1d5e48ea495d6c4"
    ),
    "migrations/013_m4_evaluation_overlay.sql": (
        "ffe1a403039b78006aef6d8da005f77d9d8ca103f52822d81e9242b669a328fb"
    ),
    "migrations/014_m5_evidence_groups.sql": (
        "4c37626f24524316c7990b2f3d213573bbe0f805a2c0f884585bf6aa325f0330"
    ),
    "migrations/015_m5_runtime.sql": (
        "85cb7f8e6a33273ce67fc6b4160e74a3aff314cd084df7ac3647cadae930185c"
    ),
    "migrations/016_m5_runtime_recovery.sql": (
        "a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7"
    ),
    "migrations/017_m5_persisted_matching.sql": (
        "e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c"
    ),
    "migrations/018_m5_bounded_document_withdrawal.sql": (
        "941bba975c12e9fb5ba4b4f75a82e59fa518b23eac34468ed2f8b15d1cd9ed90"
    ),
}
INSTALL_POINTS = (
    "after_initial_ledger",
    "after_prerequisite",
    "after_install_lock",
    "before_function",
    "after_function",
    "after_revoke",
    "after_grant",
    "before_ledger",
    "after_ledger",
)
FUNCTION_NAME = "groundloop_m5_matching_read_preterminal_seal_context"
FUNCTION_IDENTITY_ARGUMENTS = "bigint, bigint, bigint"
SEAL_EXPECTED_REVISION = 3
SEAL_RESULTING_REVISION = 4
PRIVATE_TRIPLET_SIGNATURE = (
    "groundloop_m5_matching_private_temp_triplet(regclass,regclass,regclass)"
)
SEAL_AUTHORIZER_SIGNATURE = (
    "groundloop_m5_authorize_persisted_matching_seal(bigint,bigint,bigint)"
)
EXPECTED_ARGUMENT_ROWS = (
    (1, "selected_epoch_id", "i", "bigint"),
    (2, "selected_expected_revision", "i", "bigint"),
    (3, "selected_sealed_revision", "i", "bigint"),
    (4, "policy_version", "t", "text"),
    (5, "anchor_m4_epoch_id", "t", "bigint"),
    (6, "anchor_m5_epoch_id", "t", "bigint"),
    (7, "anchor_m5_revision", "t", "bigint"),
    (8, "anchor_activation_count", "t", "integer"),
    (9, "anchor_predecessor_revision", "t", "bigint"),
    (10, "anchor_predecessor_sealed_at", "t", "timestamp with time zone"),
    (11, "anchor_current_policy", "t", "text"),
)


def _database_url() -> str:
    value = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if value is None:
        pytest.skip("live PostgreSQL test database is not configured")
    return value.replace("postgresql+psycopg://", "postgresql://", 1)


def _select_schema(connection: Connection[Any], schema_name: str) -> None:
    connection.execute(
        sql.SQL("SET search_path TO {}, public, pg_catalog").format(
            sql.Identifier(schema_name)
        )
    )
    connection.commit()


def _five_field_ledger(
    connection: Connection[Any], bundle_id: str
) -> tuple[str, ...] | None:
    row = connection.execute(
        """SELECT bundle_id,bundle_sha256,migration_sha256,oracle_sha256,
                  prerequisite_sha256
             FROM groundloop_m5_schema_bundle WHERE bundle_id=%s""",
        (bundle_id,),
    ).fetchone()
    return None if row is None else tuple(str(value).strip() for value in row)


def _ledger_with_storage(
    connection: Connection[Any], bundle_id: str
) -> tuple[str, ...] | None:
    row = connection.execute(
        """SELECT bundle_id,bundle_sha256,migration_sha256,oracle_sha256,
                  prerequisite_sha256,applied_at::text,xmin::text
             FROM groundloop_m5_schema_bundle WHERE bundle_id=%s""",
        (bundle_id,),
    ).fetchone()
    return None if row is None else tuple(str(value).strip() for value in row)


def _all_ledger_rows(connection: Connection[Any]) -> tuple[tuple[str, ...], ...]:
    return tuple(
        tuple(str(value).strip() for value in row)
        for row in connection.execute(
            '''SELECT bundle_id,bundle_sha256,migration_sha256,oracle_sha256,
                      prerequisite_sha256,applied_at::text,xmin::text
                 FROM groundloop_m5_schema_bundle
                ORDER BY bundle_id COLLATE "C"'''
        ).fetchall()
    )


def _function_oid(connection: Connection[Any], schema_name: str) -> int | None:
    row = connection.execute(
        "SELECT to_regprocedure(%s)::oid",
        (f"{schema_name}.{FUNCTION_NAME}(bigint,bigint,bigint)",),
    ).fetchone()
    return None if row is None or row[0] is None else int(row[0])


def _critical_function_snapshot(
    connection: Connection[Any], schema_name: str
) -> tuple[tuple[Any, ...], ...]:
    signatures = (PRIVATE_TRIPLET_SIGNATURE, SEAL_AUTHORIZER_SIGNATURE)
    return tuple(
        tuple(row)
        for row in connection.execute(
            '''SELECT procedure_row.oid::regprocedure::text,
                      procedure_row.proowner,procedure_row.prosecdef,
                      procedure_row.provolatile,procedure_row.proisstrict,
                      procedure_row.proleakproof,procedure_row.proparallel,
                      procedure_row.proconfig,procedure_row.proacl,
                      pg_get_functiondef(procedure_row.oid)
                 FROM pg_proc AS procedure_row
                 JOIN pg_namespace AS namespace_row
                   ON namespace_row.oid=procedure_row.pronamespace
                WHERE namespace_row.nspname=%s
                  AND procedure_row.oid=ANY(%s::regprocedure[])
                ORDER BY procedure_row.proname COLLATE "C"'''.replace(
                "%s::regprocedure[]", "ARRAY[%s,%s]::regprocedure[]"
            ),
            (schema_name, *[f"{schema_name}.{item}" for item in signatures]),
        ).fetchall()
    )


def _catalog_row(connection: Connection[Any], schema_name: str) -> tuple[Any, ...]:
    row = connection.execute(
        """SELECT language_row.lanname,procedure_row.provolatile,
                  procedure_row.prosecdef,procedure_row.proisstrict,
                  procedure_row.proleakproof,procedure_row.proparallel,
                  procedure_row.prokind,procedure_row.proretset,
                  procedure_row.pronargs,procedure_row.pronargdefaults,
                  procedure_row.proowner,owner_row.rolname,
                  procedure_row.proconfig,procedure_row.proacl,
                  pg_get_function_identity_arguments(procedure_row.oid)
             FROM pg_proc AS procedure_row
             JOIN pg_namespace AS namespace_row
               ON namespace_row.oid=procedure_row.pronamespace
             JOIN pg_language AS language_row
               ON language_row.oid=procedure_row.prolang
             JOIN pg_roles AS owner_row ON owner_row.oid=procedure_row.proowner
            WHERE namespace_row.nspname=%s AND procedure_row.proname=%s""",
        (schema_name, FUNCTION_NAME),
    ).fetchone()
    assert row is not None
    return tuple(row)


def _argument_rows(
    connection: Connection[Any], schema_name: str
) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        tuple(row)
        for row in connection.execute(
            """SELECT argument.ordinality,argument.argument_name,
                      argument.argument_mode,
                      format_type(argument.argument_type,NULL)
                 FROM pg_proc AS procedure_row
                 JOIN pg_namespace AS namespace_row
                   ON namespace_row.oid=procedure_row.pronamespace
                 CROSS JOIN LATERAL unnest(
                     procedure_row.proallargtypes,
                     procedure_row.proargnames,
                     procedure_row.proargmodes
                 ) WITH ORDINALITY AS argument(
                     argument_type,argument_name,argument_mode,ordinality
                 )
                WHERE namespace_row.nspname=%s AND procedure_row.proname=%s
                ORDER BY argument.ordinality""",
            (schema_name, FUNCTION_NAME),
        ).fetchall()
    )


def _schema_relation_inventory(
    connection: Connection[Any], schema_name: str
) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        tuple(row)
        for row in connection.execute(
            '''SELECT relation_row.relname,relation_row.relkind,
                      relation_row.relpersistence,relation_row.relowner,
                      relation_row.relacl
                 FROM pg_class AS relation_row
                 JOIN pg_namespace AS namespace_row
                   ON namespace_row.oid=relation_row.relnamespace
                WHERE namespace_row.nspname=%s
                ORDER BY relation_row.relname COLLATE "C"''',
            (schema_name,),
        ).fetchall()
    )


def _install_through_018(connection: Connection[Any]) -> None:
    apply_legacy_migrations(connection)
    connection.commit()
    for installer in (
        install_m5_core_bundle,
        install_m5_runtime_bundle,
        install_m5_runtime_recovery_bundle,
        install_m5_persisted_matching_bundle,
        install_m5_bounded_document_withdrawal_bundle,
    ):
        installer(connection)
        connection.commit()
    assert (
        _five_field_ledger(connection, M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID)
        == EXPECTED_018_LEDGER
    )
    schema_row = connection.execute("SELECT current_schema()").fetchone()
    assert schema_row is not None and schema_row[0] is not None
    assert _function_oid(connection, str(schema_row[0])) is None
    connection.commit()


@contextmanager
def _pre019_schema() -> Iterator[tuple[Connection[Any], str]]:
    schema_name = f"d31_migration_019_{uuid.uuid4().hex}"
    dsn = _database_url()
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
    connection: Connection[Any] = psycopg.connect(dsn)
    try:
        _select_schema(connection, schema_name)
        _install_through_018(connection)
        yield connection, schema_name
    finally:
        connection.close()
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )


def _assert_not_installed(connection: Connection[Any], schema_name: str) -> None:
    assert _function_oid(connection, schema_name) is None
    assert _five_field_ledger(connection, M5_PRETERMINAL_SEAL_CONTEXT_BUNDLE_ID) is None


class _RecordingConnection:
    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection
        self.statements: list[str] = []

    def execute(self, query: Any, *args: Any, **kwargs: Any) -> Any:
        rendered = (
            query if isinstance(query, str) else query.as_string(self._connection)
        )
        self.statements.append(" ".join(rendered.split()))
        return self._connection.execute(query, *args, **kwargs)

    def transaction(self, *args: Any, **kwargs: Any) -> Any:
        return self._connection.transaction(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


@dataclass
class _PausedInstaller:
    thread: threading.Thread
    reached_pause: threading.Event
    resume: threading.Event
    points: list[str]
    results: list[M5PreterminalSealContextBundleInstallResult]
    errors: list[BaseException]


def _start_paused_installer(schema_name: str, pause_at: str) -> _PausedInstaller:
    reached_pause = threading.Event()
    resume = threading.Event()
    points: list[str] = []
    results: list[M5PreterminalSealContextBundleInstallResult] = []
    errors: list[BaseException] = []

    def run() -> None:
        try:
            with psycopg.connect(_database_url()) as connection:
                _select_schema(connection, schema_name)

                def pause(point: str) -> None:
                    points.append(point)
                    if point == pause_at:
                        reached_pause.set()
                        if not resume.wait(timeout=30):
                            raise TimeoutError(
                                f"migration-019 installer not resumed at {point}"
                            )

                results.append(
                    install_m5_preterminal_seal_context_bundle(
                        connection, failure_injector=pause
                    )
                )
        except BaseException as error:  # retained for assertion by test thread
            errors.append(error)

    worker = _PausedInstaller(
        thread=threading.Thread(target=run, daemon=True),
        reached_pause=reached_pause,
        resume=resume,
        points=points,
        results=results,
        errors=errors,
    )
    worker.thread.start()
    return worker


def _resume_and_join(worker: _PausedInstaller) -> None:
    worker.resume.set()
    worker.thread.join(timeout=60)
    assert not worker.thread.is_alive(), "migration-019 installer did not terminate"


@contextmanager
def _block_install_lock(schema_name: str) -> Iterator[None]:
    with psycopg.connect(_database_url()) as blocker:
        _select_schema(blocker, schema_name)
        blocker.execute("LOCK TABLE groundloop_m5_schema_bundle IN ROW EXCLUSIVE MODE")
        try:
            yield
        finally:
            blocker.rollback()


def _seed_seal_ready(connection: Connection[Any], prefix: str) -> tuple[int, int, str]:
    migration_017_tests = importlib.import_module(
        "tests.m5.postgres_runtime.test_migration_017"
    )

    base, policy = migration_017_tests._seed_b2_runtime_base(connection, prefix)
    epoch, event, payload = migration_017_tests._open_b2_runtime_epoch(
        connection, f"{prefix}-open", base, policy
    )
    migration_017_tests._apply_empty_b2_structural(
        connection,
        epoch=epoch,
        event=event,
        payload=payload,
        base=base,
        policy=policy,
    )
    connection.commit()

    for before, after, runtime_state, semantic_state, evaluation_state in (
        (1, 2, "semantic_pending", "pending", "pending"),
        (2, 3, "semantic_complete", "complete", "complete"),
    ):
        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s,%s)",
            (epoch, before),
        )
        connection.execute(
            "UPDATE groundloop_epoch SET revision=%s,semantic_status=%s,"
            "evaluation_state=%s WHERE epoch_id=%s AND revision=%s",
            (after, semantic_state, evaluation_state, epoch, before),
        )
        connection.execute(
            "UPDATE groundloop_m5_runtime_epoch SET runtime_state=%s,revision=%s "
            "WHERE epoch_id=%s AND revision=%s",
            (runtime_state, after, epoch, before),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()
    return base, epoch, policy


def _authorize_seal(connection: Connection[Any], epoch: int) -> None:
    connection.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s,%s)",
        (epoch, SEAL_EXPECTED_REVISION),
    )
    connection.execute(
        "SELECT groundloop_m5_authorize_persisted_matching_seal(%s,%s,%s)",
        (epoch, SEAL_EXPECTED_REVISION, SEAL_RESULTING_REVISION),
    )


def _create_structurally_identical_promotion_triplet(
    connection: Connection[Any], *, schema_name: str | None = None
) -> None:
    create = (
        sql.SQL("CREATE TEMP TABLE") if schema_name is None else sql.SQL("CREATE TABLE")
    )
    namespace = "pg_temp" if schema_name is None else schema_name
    suffix = sql.SQL("ON COMMIT DROP") if schema_name is None else sql.SQL("")
    connection.execute(
        sql.SQL(
            """{} {} (
                backend_pid integer NOT NULL,
                transaction_id bigint NOT NULL,
                session_role text NOT NULL,
                mode text NOT NULL,
                epoch_id bigint NOT NULL,
                expected_revision bigint NOT NULL,
                resulting_revision bigint NOT NULL,
                policy_version text NOT NULL,
                anchor_m4_epoch_id bigint NOT NULL,
                anchor_m5_epoch_id bigint,
                anchor_m5_revision bigint,
                anchor_activation_count integer NOT NULL,
                anchor_predecessor_revision bigint NOT NULL,
                anchor_predecessor_sealed_at timestamptz NOT NULL,
                anchor_current_policy text,
                validation_started boolean NOT NULL DEFAULT false,
                validation_done boolean NOT NULL DEFAULT false,
                PRIMARY KEY(backend_pid,transaction_id,session_role)
            ) {}"""
        ).format(
            create,
            sql.Identifier(namespace, "groundloop_m5_matching_promotion_context"),
            suffix,
        )
    )
    connection.execute(
        sql.SQL(
            """{} {} (
                relation_name text NOT NULL,
                key_preimage bytea NOT NULL,
                first_old jsonb,
                final_new jsonb,
                first_operation text NOT NULL,
                last_operation text NOT NULL,
                mutation_count integer NOT NULL,
                saw_insert boolean NOT NULL,
                saw_update boolean NOT NULL,
                saw_delete boolean NOT NULL,
                PRIMARY KEY(relation_name,key_preimage)
            ) {}"""
        ).format(
            create,
            sql.Identifier(namespace, "groundloop_m5_matching_promotion_journal"),
            suffix,
        )
    )
    connection.execute(
        sql.SQL(
            """{} {} (
                relation_name text NOT NULL,
                key_preimage bytea NOT NULL,
                PRIMARY KEY(relation_name,key_preimage)
            ) {}"""
        ).format(
            create,
            sql.Identifier(namespace, "groundloop_m5_matching_promotion_expected"),
            suffix,
        )
    )


def _accessor_row(
    connection: Connection[Any], schema_name: str, epoch: int
) -> tuple[Any, ...]:
    row = connection.execute(
        sql.SQL("SELECT * FROM {}.{}(%s,%s,%s)").format(
            sql.Identifier(schema_name), sql.Identifier(FUNCTION_NAME)
        ),
        (epoch, SEAL_EXPECTED_REVISION, SEAL_RESULTING_REVISION),
    ).fetchone()
    assert row is not None
    return tuple(row)


def _expect_database_error(
    connection: Connection[Any], action: Callable[[], object], match: str | None = None
) -> None:
    with pytest.raises(psycopg.Error, match=match):
        with connection.transaction():
            action()


def _accessor_state_snapshot(connection: Connection[Any]) -> tuple[Any, ...]:
    settings = connection.execute(
        """SELECT current_setting('groundloop.m5_checked_transition',true),
                  current_setting('groundloop.m5_matching_mode',true),
                  current_setting('groundloop.m5_matching_epoch_id',true),
                  current_setting('groundloop.m5_matching_expected_revision',true),
                  current_setting('groundloop.m5_matching_resulting_revision',true),
                  current_setting('groundloop.m5_matching_policy',true),
                  current_setting('groundloop.m5_matching_context_oid',true),
                  current_setting('groundloop.m5_matching_journal_oid',true),
                  current_setting('groundloop.m5_matching_expected_oid',true)"""
    ).fetchone()
    context = connection.execute(
        """SELECT row_to_json(context_row)::text,xmin::text
             FROM pg_temp.groundloop_m5_matching_promotion_context AS context_row"""
    ).fetchall()
    journal = connection.execute(
        """SELECT row_to_json(journal_row)::text,xmin::text
             FROM pg_temp.groundloop_m5_matching_promotion_journal AS journal_row
            ORDER BY relation_name COLLATE "C",key_preimage"""
    ).fetchall()
    expected = connection.execute(
        """SELECT row_to_json(expected_row)::text,xmin::text
             FROM pg_temp.groundloop_m5_matching_promotion_expected AS expected_row
            ORDER BY relation_name COLLATE "C",key_preimage"""
    ).fetchall()
    persistent = connection.execute(
        """SELECT
             (SELECT xmin::text FROM groundloop_m5_schema_bundle
               WHERE bundle_id=%s),
             (SELECT xmin::text FROM groundloop_m4_publication_head
               WHERE singleton),
             (SELECT xmin::text FROM groundloop_m5_publication_head
               WHERE singleton),
             (SELECT count(*) FROM groundloop_epoch),
             (SELECT count(*) FROM groundloop_m5_runtime_epoch)""",
        (M5_PRETERMINAL_SEAL_CONTEXT_BUNDLE_ID,),
    ).fetchone()
    locks = connection.execute(
        """SELECT locktype,coalesce(relation::text,''),mode,granted
             FROM pg_locks WHERE pid=pg_backend_pid()
            ORDER BY locktype COLLATE "C",coalesce(relation::text,'') COLLATE "C",
                     mode COLLATE "C",granted"""
    ).fetchall()
    return (
        settings,
        tuple(context),
        tuple(journal),
        tuple(expected),
        persistent,
        tuple(locks),
    )


def test_exact_sql_hash_surface_and_no_forbidden_object_or_mutation() -> None:
    source = M5_PRETERMINAL_SEAL_CONTEXT_MIGRATION_PATH.read_bytes()
    assert hashlib.sha256(source).hexdigest() == EXPECTED_MIGRATION_SHA256
    assert source.endswith(b"\n") and b"\r" not in source
    text = source.decode("utf-8")
    assert text.count(f"CREATE FUNCTION {FUNCTION_NAME}(") == 1
    assert text.count(f"REVOKE ALL ON FUNCTION {FUNCTION_NAME}(") == 1
    assert text.count(f"GRANT EXECUTE ON FUNCTION {FUNCTION_NAME}(") == 1
    assert "LANGUAGE plpgsql" in text
    assert "STABLE\nCALLED ON NULL INPUT\nSECURITY DEFINER" in text
    assert "PARALLEL UNSAFE\nNOT LEAKPROOF\nSET search_path FROM CURRENT" in text
    assert "INTO STRICT" in text and "count(*) OVER ()" in text
    assert "groundloop_m5_matching_private_temp_triplet(" in text
    assert "validation_started IS DISTINCT FROM false" in text
    assert "validation_done IS DISTINCT FROM false" in text
    for forbidden in (
        r"\bCREATE\s+(?:TABLE|TYPE|INDEX|TRIGGER|VIEW|SEQUENCE)\b",
        r"\bALTER\b",
        r"\bDROP\b",
        r"\bINSERT\b",
        r"\bUPDATE\b",
        r"\bDELETE\b",
        r"\bMERGE\b",
        r"\bTRUNCATE\b",
        r"\bLOCK\b",
        r"\bSET\s+CONSTRAINTS\b",
        r"\bset_config\s*\(",
    ):
        assert re.search(forbidden, text, re.IGNORECASE) is None


def test_identity_formula_pins_exact_018_and_frozen_migrations() -> None:
    source = M5_PRETERMINAL_SEAL_CONTEXT_MIGRATION_PATH.read_bytes()
    identity = m5_preterminal_seal_context_bundle_identity()
    expected_bundle = stable_m5_digest(
        "m5-preterminal-seal-context-schema-bundle-v1",
        text_field("migrations/019_m5_preterminal_seal_context.sql"),
        hash_field(hashlib.sha256(source).hexdigest()),
        hash_field(EXPECTED_018_LEDGER[1]),
    )
    assert identity == M5PreterminalSealContextBundleIdentity(
        bundle_id="m5-preterminal-seal-context-schema-bundle-v1",
        bundle_sha256=expected_bundle,
        migration_sha256=EXPECTED_MIGRATION_SHA256,
        oracle_sha256=EXPECTED_ORACLE_SHA256,
        prerequisite_sha256=EXPECTED_018_LEDGER[1],
    )
    assert M5_PRETERMINAL_SEAL_CONTEXT_BUNDLE_ID == identity.bundle_id
    assert M5_PRETERMINAL_SEAL_CONTEXT_MIGRATION_LABEL == (
        "migrations/019_m5_preterminal_seal_context.sql"
    )
    assert M5_PRETERMINAL_SEAL_CONTEXT_ORACLE_SHA256 == EXPECTED_ORACLE_SHA256
    assert M5_ACCEPTED_PRETERMINAL_SEAL_CONTEXT_MIGRATION_SHA256 == (
        EXPECTED_MIGRATION_SHA256
    )
    assert M5_ACCEPTED_PRETERMINAL_SEAL_CONTEXT_BUNDLE_SHA256 == expected_bundle
    assert expected_bundle == EXPECTED_BUNDLE_SHA256
    assert (
        M5_ACCEPTED_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_SHA256
        == (EXPECTED_018_LEDGER[1])
    )
    assert (
        M5_ACCEPTED_BOUNDED_DOCUMENT_WITHDRAWAL_MIGRATION_SHA256
        == (EXPECTED_018_LEDGER[2])
    )
    root = M5_PRETERMINAL_SEAL_CONTEXT_MIGRATION_PATH.parents[1]
    assert {
        relative: hashlib.sha256((root / relative).read_bytes()).hexdigest()
        for relative in FROZEN_MIGRATION_HASHES
    } == FROZEN_MIGRATION_HASHES


def test_first_install_catalog_acl_owner_and_ledger_are_exact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _pre019_schema() as (connection, schema_name):
        before_relations = _schema_relation_inventory(connection, schema_name)
        before_critical = _critical_function_snapshot(connection, schema_name)
        before_ledgers = _all_ledger_rows(connection)
        connection.commit()
        original_read_bytes = Path.read_bytes
        migration_reads: list[Path] = []

        def record_read_bytes(path: Path) -> bytes:
            if path == M5_PRETERMINAL_SEAL_CONTEXT_MIGRATION_PATH:
                migration_reads.append(path)
            return original_read_bytes(path)

        monkeypatch.setattr(Path, "read_bytes", record_read_bytes)
        result = install_m5_preterminal_seal_context_bundle(connection)
        assert migration_reads == [M5_PRETERMINAL_SEAL_CONTEXT_MIGRATION_PATH]
        monkeypatch.undo()
        assert isinstance(result, M5PreterminalSealContextBundleInstallResult)
        assert result.applied
        assert result.identity == m5_preterminal_seal_context_bundle_identity()
        assert _schema_relation_inventory(connection, schema_name) == before_relations
        assert _critical_function_snapshot(connection, schema_name) == before_critical
        assert (
            tuple(
                row
                for row in _all_ledger_rows(connection)
                if row[0] != M5_PRETERMINAL_SEAL_CONTEXT_BUNDLE_ID
            )
            == before_ledgers
        )
        row = _catalog_row(connection, schema_name)
        assert row[:10] == ("plpgsql", "s", True, False, False, "u", "f", True, 3, 0)
        owner_oid = int(row[10])
        assert row[12] == [f"search_path={schema_name}, pg_catalog"]
        assert row[14] == (
            "selected_epoch_id bigint, selected_expected_revision bigint, "
            "selected_sealed_revision bigint"
        )
        assert _argument_rows(connection, schema_name) == EXPECTED_ARGUMENT_ROWS
        owner_rows = connection.execute(
            '''SELECT procedure_row.proname,procedure_row.proowner
                 FROM pg_proc AS procedure_row
                 JOIN pg_namespace AS namespace_row
                   ON namespace_row.oid=procedure_row.pronamespace
                WHERE namespace_row.nspname=%s
                  AND procedure_row.proname IN (
                    'groundloop_m5_matching_private_temp_triplet',
                    'groundloop_m5_authorize_persisted_matching_seal')
                ORDER BY procedure_row.proname COLLATE "C"''',
            (schema_name,),
        ).fetchall()
        assert len(owner_rows) == 2
        assert {int(item[1]) for item in owner_rows} == {owner_oid}
        acl = connection.execute(
            """SELECT expanded.grantee,expanded.privilege_type,
                      expanded.is_grantable
                 FROM pg_proc AS procedure_row
                 JOIN pg_namespace AS namespace_row
                   ON namespace_row.oid=procedure_row.pronamespace
                 CROSS JOIN LATERAL aclexplode(procedure_row.proacl) AS expanded
                WHERE namespace_row.nspname=%s AND procedure_row.proname=%s
                ORDER BY expanded.grantee,expanded.privilege_type""",
            (schema_name, FUNCTION_NAME),
        ).fetchall()
        assert (0, "EXECUTE", False) in acl
        assert (owner_oid, "EXECUTE", False) in acl
        ledger = _five_field_ledger(connection, M5_PRETERMINAL_SEAL_CONTEXT_BUNDLE_ID)
        assert ledger == (
            result.identity.bundle_id,
            result.identity.bundle_sha256,
            result.identity.migration_sha256,
            result.identity.oracle_sha256,
            result.identity.prerequisite_sha256,
        )
        assert (
            _five_field_ledger(connection, M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID)
            == EXPECTED_018_LEDGER
        )
        connection.commit()


def test_replay_is_install_lock_free_and_conflicting_ledger_is_ledger_first() -> None:
    with _pre019_schema() as (connection, schema_name):
        assert install_m5_preterminal_seal_context_bundle(connection).applied
        connection.commit()
        before = _ledger_with_storage(connection, M5_PRETERMINAL_SEAL_CONTEXT_BUNDLE_ID)
        before_oid = _function_oid(connection, schema_name)
        connection.commit()
        with _block_install_lock(schema_name):
            replay = install_m5_preterminal_seal_context_bundle(connection)
        assert not replay.applied
        assert (
            _ledger_with_storage(connection, M5_PRETERMINAL_SEAL_CONTEXT_BUNDLE_ID)
            == before
        )
        assert _function_oid(connection, schema_name) == before_oid
        connection.commit()

    with _pre019_schema() as (connection, schema_name):
        identity = m5_preterminal_seal_context_bundle_identity()
        connection.execute(
            """INSERT INTO groundloop_m5_schema_bundle
               (bundle_id,bundle_sha256,migration_sha256,oracle_sha256,
                prerequisite_sha256) VALUES (%s,%s,%s,%s,%s)""",
            (
                identity.bundle_id,
                "f" * 64,
                identity.migration_sha256,
                identity.oracle_sha256,
                identity.prerequisite_sha256,
            ),
        )
        connection.commit()
        with _block_install_lock(schema_name), pytest.raises(M5BundleHashConflictError):
            install_m5_preterminal_seal_context_bundle(connection)
        assert _function_oid(connection, schema_name) is None
        connection.commit()


def test_wrong_bytes_prerequisite_partial_object_and_connection_state_fails() -> None:
    with _pre019_schema() as (connection, schema_name):
        points: list[str] = []
        with pytest.raises(M5PreterminalSealContextBundleError, match="frozen M5-D31"):
            install_m5_preterminal_seal_context_bundle(
                connection,
                migration_bytes=b"-- unauthorized migration 019\n",
                failure_injector=points.append,
            )
        assert points == ["after_initial_ledger"]
        _assert_not_installed(connection, schema_name)
        connection.commit()

        connection.execute("SELECT 1")
        assert connection.info.transaction_status == TransactionStatus.INTRANS
        with pytest.raises(
            M5PreterminalSealContextBundleError, match="idle connection"
        ):
            install_m5_preterminal_seal_context_bundle(connection)
        connection.rollback()
        for isolation in (IsolationLevel.REPEATABLE_READ, IsolationLevel.SERIALIZABLE):
            connection.isolation_level = isolation
            try:
                with pytest.raises(
                    M5PreterminalSealContextBundleError,
                    match="read-write READ COMMITTED",
                ):
                    install_m5_preterminal_seal_context_bundle(connection)
            finally:
                connection.isolation_level = None
        connection.read_only = True
        try:
            with pytest.raises(
                M5PreterminalSealContextBundleError,
                match="read-write READ COMMITTED",
            ):
                install_m5_preterminal_seal_context_bundle(connection)
        finally:
            connection.read_only = None

    with _pre019_schema() as (connection, schema_name):
        prerequisite_fields = {
            "bundle_id": EXPECTED_018_LEDGER[0],
            "bundle_sha256": EXPECTED_018_LEDGER[1],
            "migration_sha256": EXPECTED_018_LEDGER[2],
            "oracle_sha256": EXPECTED_018_LEDGER[3],
            "prerequisite_sha256": EXPECTED_018_LEDGER[4],
        }
        for field, accepted in prerequisite_fields.items():
            mismatch = accepted + "-mismatch" if field == "bundle_id" else "f" * 64
            connection.execute(
                "ALTER TABLE groundloop_m5_schema_bundle "
                "DISABLE TRIGGER groundloop_m5_schema_bundle_immutable"
            )
            connection.execute(
                sql.SQL(
                    "UPDATE groundloop_m5_schema_bundle SET {}=%s WHERE bundle_id=%s"
                ).format(sql.Identifier(field)),
                (mismatch, M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID),
            )
            connection.execute(
                "ALTER TABLE groundloop_m5_schema_bundle "
                "ENABLE TRIGGER groundloop_m5_schema_bundle_immutable"
            )
            connection.commit()
            with pytest.raises(M5PrerequisiteError, match="migration-018"):
                install_m5_preterminal_seal_context_bundle(connection)
            _assert_not_installed(connection, schema_name)
            connection.commit()

            connection.execute(
                "ALTER TABLE groundloop_m5_schema_bundle "
                "DISABLE TRIGGER groundloop_m5_schema_bundle_immutable"
            )
            current_bundle_id = (
                mismatch
                if field == "bundle_id"
                else M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID
            )
            connection.execute(
                sql.SQL(
                    "UPDATE groundloop_m5_schema_bundle SET {}=%s WHERE bundle_id=%s"
                ).format(sql.Identifier(field)),
                (accepted, current_bundle_id),
            )
            connection.execute(
                "ALTER TABLE groundloop_m5_schema_bundle "
                "ENABLE TRIGGER groundloop_m5_schema_bundle_immutable"
            )
            connection.commit()
            assert (
                _five_field_ledger(connection, M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID)
                == EXPECTED_018_LEDGER
            )
            connection.commit()

    with _pre019_schema() as (connection, schema_name):
        connection.execute(
            f"""CREATE FUNCTION {FUNCTION_NAME}(bigint,bigint,bigint)
                RETURNS TABLE (
                    policy_version text,anchor_m4_epoch_id bigint,
                    anchor_m5_epoch_id bigint,anchor_m5_revision bigint,
                    anchor_activation_count integer,
                    anchor_predecessor_revision bigint,
                    anchor_predecessor_sealed_at timestamptz,
                    anchor_current_policy text)
                LANGUAGE sql AS 'SELECT NULL::text,NULL::bigint,NULL::bigint,
                    NULL::bigint,NULL::integer,NULL::bigint,NULL::timestamptz,
                    NULL::text'"""
        )
        connection.commit()
        with pytest.raises(M5PreterminalSealContextBundleError, match="pre-existing"):
            install_m5_preterminal_seal_context_bundle(connection)
        assert (
            _five_field_ledger(connection, M5_PRETERMINAL_SEAL_CONTEXT_BUNDLE_ID)
            is None
        )
        assert _function_oid(connection, schema_name) is not None
        connection.commit()


def test_installer_exact_lock_statement_and_every_failure_cut_is_atomic() -> None:
    with _pre019_schema() as (connection, schema_name):
        recording = _RecordingConnection(connection)
        result = install_m5_preterminal_seal_context_bundle(recording)  # type: ignore[arg-type]
        assert result.applied
        lock_statements = [
            item for item in recording.statements if item.startswith("LOCK TABLE")
        ]
        assert lock_statements == [
            f'LOCK TABLE "{schema_name}"."groundloop_m5_schema_bundle" '
            "IN SHARE ROW EXCLUSIVE MODE NOWAIT"
        ]
        connection.commit()

    with _pre019_schema() as (connection, schema_name):
        for index, failure_point in enumerate(INSTALL_POINTS):
            observed: list[str] = []

            def fail(
                point: str,
                expected: str = failure_point,
                seen: list[str] = observed,
            ) -> None:
                seen.append(point)
                if point == expected:
                    raise RuntimeError(f"injected migration-019 failure at {point}")

            with pytest.raises(RuntimeError, match=re.escape(failure_point)):
                install_m5_preterminal_seal_context_bundle(
                    connection, failure_injector=fail
                )
            assert observed == list(INSTALL_POINTS[: index + 1])
            _assert_not_installed(connection, schema_name)
            assert (
                _five_field_ledger(connection, M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID)
                == EXPECTED_018_LEDGER
            )
            connection.commit()
        assert install_m5_preterminal_seal_context_bundle(connection).applied
        connection.commit()


def test_concurrent_installers_fail_fast_or_replay_in_both_orders() -> None:
    with _pre019_schema() as (connection, schema_name):
        worker = _start_paused_installer(schema_name, "after_install_lock")
        try:
            assert worker.reached_pause.wait(timeout=30)
            with pytest.raises(psycopg.errors.LockNotAvailable):
                install_m5_preterminal_seal_context_bundle(connection)
        finally:
            _resume_and_join(worker)
        assert worker.errors == []
        assert len(worker.results) == 1 and worker.results[0].applied
        replay = install_m5_preterminal_seal_context_bundle(connection)
        assert not replay.applied
        connection.commit()

    with _pre019_schema() as (connection, schema_name):
        worker = _start_paused_installer(schema_name, "after_initial_ledger")
        try:
            assert worker.reached_pause.wait(timeout=30)
            winner = install_m5_preterminal_seal_context_bundle(connection)
            assert winner.applied
            connection.commit()
        finally:
            _resume_and_join(worker)
        assert worker.errors == []
        assert len(worker.results) == 1 and not worker.results[0].applied
        assert _function_oid(connection, schema_name) is not None
        connection.commit()


def test_installer_selected_schema_never_falls_through_search_path() -> None:
    valid_schema = f"d31_selected_valid_{uuid.uuid4().hex}"
    earlier_schema = f"d31_selected_earlier_{uuid.uuid4().hex}"
    absent_schema = f"d31_selected_absent_{uuid.uuid4().hex}"
    dsn = _database_url()
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(valid_schema)))
        admin.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(earlier_schema))
        )
    connection: Connection[Any] = psycopg.connect(dsn)
    try:
        _select_schema(connection, valid_schema)
        _install_through_018(connection)
        installed = install_m5_preterminal_seal_context_bundle(connection)
        assert installed.applied
        connection.commit()
        valid_oid = _function_oid(connection, valid_schema)
        valid_ledger = _ledger_with_storage(
            connection, M5_PRETERMINAL_SEAL_CONTEXT_BUNDLE_ID
        )
        assert valid_oid is not None and valid_ledger is not None
        connection.commit()

        connection.execute(
            sql.SQL("SET search_path TO {}, {}, public, pg_catalog").format(
                sql.Identifier(earlier_schema), sql.Identifier(valid_schema)
            )
        )
        connection.commit()
        assert connection.execute("SELECT current_schema()").fetchone() == (
            earlier_schema,
        )
        connection.commit()
        with pytest.raises(
            M5PreterminalSealContextBundleError,
            match="selected schema's exact bundle ledger",
        ):
            install_m5_preterminal_seal_context_bundle(connection)
        assert _function_oid(connection, earlier_schema) is None
        connection.commit()

        connection.execute(
            sql.SQL("SET search_path TO {}, {}, public, pg_catalog").format(
                sql.Identifier(valid_schema), sql.Identifier(earlier_schema)
            )
        )
        connection.commit()
        replay = install_m5_preterminal_seal_context_bundle(connection)
        assert not replay.applied
        assert _function_oid(connection, valid_schema) == valid_oid
        assert (
            _ledger_with_storage(connection, M5_PRETERMINAL_SEAL_CONTEXT_BUNDLE_ID)
            == valid_ledger
        )
        connection.commit()

        connection.execute(
            sql.SQL("SET search_path TO {}").format(sql.Identifier(absent_schema))
        )
        connection.commit()
        assert connection.execute("SELECT current_schema()").fetchone() == (None,)
        connection.commit()
        with pytest.raises(
            M5PreterminalSealContextBundleError,
            match="selected installation schema",
        ):
            install_m5_preterminal_seal_context_bundle(connection)
        connection.execute(
            sql.SQL("SET search_path TO {}, public, pg_catalog").format(
                sql.Identifier(valid_schema)
            )
        )
        connection.commit()
        assert _function_oid(connection, valid_schema) == valid_oid
        assert (
            _ledger_with_storage(connection, M5_PRETERMINAL_SEAL_CONTEXT_BUNDLE_ID)
            == valid_ledger
        )
        connection.commit()
    finally:
        connection.close()
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(earlier_schema))
            )
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(valid_schema))
            )


def test_owner_and_genuine_nonowner_accessor_match_and_raw_denied() -> None:
    role = f"d31_runtime_{uuid.uuid4().hex}"
    password = uuid.uuid4().hex
    with _pre019_schema() as (owner, schema_name):
        assert install_m5_preterminal_seal_context_bundle(owner).applied
        owner.commit()
        base, epoch, policy = _seed_seal_ready(owner, "d31-access")
        _authorize_seal(owner, epoch)
        owner_row = _accessor_row(owner, schema_name, epoch)
        assert owner_row[0] == policy
        assert owner_row[1:6] == (base, base, 0, 1, 0)
        assert owner_row[6] is not None and owner_row[7] == policy
        owner.rollback()

        try:
            owner.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(role), sql.Literal(password)
                )
            )
        except psycopg.errors.InsufficientPrivilege:
            owner.rollback()
            pytest.skip("database role cannot create a genuine non-owner login")
        owner.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(schema_name), sql.Identifier(role)
            )
        )
        owner.execute(
            sql.SQL("GRANT ALL ON ALL TABLES IN SCHEMA {} TO {}").format(
                sql.Identifier(schema_name), sql.Identifier(role)
            )
        )
        owner.execute(
            sql.SQL("GRANT USAGE ON ALL SEQUENCES IN SCHEMA {} TO {}").format(
                sql.Identifier(schema_name), sql.Identifier(role)
            )
        )
        owner.commit()
        try:
            with psycopg.connect(
                _database_url(), user=role, password=password
            ) as runtime:
                _select_schema(runtime, schema_name)
                _authorize_seal(runtime, epoch)
                nonowner_row = _accessor_row(runtime, schema_name, epoch)
                assert nonowner_row == owner_row
                _expect_database_error(
                    runtime,
                    lambda: runtime.execute(
                        "SELECT * FROM pg_temp.groundloop_m5_matching_promotion_context"
                    ),
                    "permission denied",
                )
                _expect_database_error(
                    runtime,
                    lambda: runtime.execute(
                        sql.SQL(
                            "SELECT {}.groundloop_m5_matching_private_temp_triplet("
                            "to_regclass('pg_temp.groundloop_m5_matching_"
                            "promotion_context'),to_regclass('pg_temp."
                            "groundloop_m5_matching_promotion_journal'),"
                            "to_regclass('pg_temp.groundloop_m5_matching_"
                            "promotion_expected'))"
                        ).format(sql.Identifier(schema_name))
                    ),
                    "permission denied",
                )
                assert _accessor_row(runtime, schema_name, epoch) == owner_row
                runtime.rollback()

                _create_structurally_identical_promotion_triplet(runtime)
                for setting, value in (
                    ("groundloop.m5_checked_transition", "on"),
                    ("groundloop.m5_matching_mode", "seal"),
                    ("groundloop.m5_matching_epoch_id", str(epoch)),
                    (
                        "groundloop.m5_matching_expected_revision",
                        str(SEAL_EXPECTED_REVISION),
                    ),
                    (
                        "groundloop.m5_matching_resulting_revision",
                        str(SEAL_RESULTING_REVISION),
                    ),
                    ("groundloop.m5_matching_policy", policy),
                ):
                    runtime.execute("SELECT set_config(%s,%s,true)", (setting, value))

                def forged_accessor_call() -> object:
                    return _accessor_row(runtime, schema_name, epoch)

                _expect_database_error(runtime, forged_accessor_call, "not genuine")
                for setting in (
                    "groundloop.m5_matching_context_oid",
                    "groundloop.m5_matching_journal_oid",
                    "groundloop.m5_matching_expected_oid",
                ):
                    runtime.execute("SELECT set_config(%s,'1',true)", (setting,))
                _expect_database_error(runtime, forged_accessor_call, "not genuine")
                for setting, relation_name in (
                    (
                        "groundloop.m5_matching_context_oid",
                        "groundloop_m5_matching_promotion_context",
                    ),
                    (
                        "groundloop.m5_matching_journal_oid",
                        "groundloop_m5_matching_promotion_journal",
                    ),
                    (
                        "groundloop.m5_matching_expected_oid",
                        "groundloop_m5_matching_promotion_expected",
                    ),
                ):
                    runtime.execute(
                        "SELECT set_config(%s,to_regclass(%s)::oid::text,true)",
                        (setting, f"pg_temp.{relation_name}"),
                    )
                _expect_database_error(runtime, forged_accessor_call, "not genuine")
                runtime.rollback()
        finally:
            owner.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            owner.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
            owner.commit()


def test_permanent_wrong_namespace_triplet_never_substitutes_for_pg_temp() -> None:
    with _pre019_schema() as (connection, schema_name):
        assert install_m5_preterminal_seal_context_bundle(connection).applied
        connection.commit()
        _create_structurally_identical_promotion_triplet(
            connection, schema_name=schema_name
        )
        relation_rows = connection.execute(
            """SELECT relation_row.relname,relation_row.relpersistence,
                      relation_row.relnamespace=pg_my_temp_schema()
                 FROM pg_class AS relation_row
                 JOIN pg_namespace AS namespace_row
                   ON namespace_row.oid=relation_row.relnamespace
                WHERE namespace_row.nspname=%s
                  AND relation_row.relkind='r'
                  AND relation_row.relname LIKE
                      'groundloop_m5_matching_promotion_%%'
                ORDER BY relation_row.relname COLLATE "C"
            """,
            (schema_name,),
        ).fetchall()
        assert relation_rows == [
            ("groundloop_m5_matching_promotion_context", "p", False),
            ("groundloop_m5_matching_promotion_expected", "p", False),
            ("groundloop_m5_matching_promotion_journal", "p", False),
        ]
        connection.commit()

        for setting, value in (
            ("groundloop.m5_checked_transition", "on"),
            ("groundloop.m5_matching_mode", "seal"),
            ("groundloop.m5_matching_epoch_id", "1"),
            (
                "groundloop.m5_matching_expected_revision",
                str(SEAL_EXPECTED_REVISION),
            ),
            (
                "groundloop.m5_matching_resulting_revision",
                str(SEAL_RESULTING_REVISION),
            ),
            ("groundloop.m5_matching_policy", "wrong-namespace-policy"),
        ):
            connection.execute("SELECT set_config(%s,%s,true)", (setting, value))
        for setting, relation_name in (
            (
                "groundloop.m5_matching_context_oid",
                "groundloop_m5_matching_promotion_context",
            ),
            (
                "groundloop.m5_matching_journal_oid",
                "groundloop_m5_matching_promotion_journal",
            ),
            (
                "groundloop.m5_matching_expected_oid",
                "groundloop_m5_matching_promotion_expected",
            ),
        ):
            oid_row = connection.execute(
                sql.SQL("SELECT {}::regclass::oid::text").format(
                    sql.Literal(f"{schema_name}.{relation_name}")
                )
            ).fetchone()
            assert oid_row is not None
            connection.execute(
                "SELECT set_config(%s,%s,true)", (setting, str(oid_row[0]))
            )

        _expect_database_error(
            connection,
            lambda: _accessor_row(connection, schema_name, 1),
            "preterminal promotion context is incomplete",
        )
        connection.rollback()


def test_null_coordinate_guc_row_relation_flag_and_cardinality_falsifiers() -> None:
    with _pre019_schema() as (connection, schema_name):
        assert install_m5_preterminal_seal_context_bundle(connection).applied
        connection.commit()
        _, epoch, _ = _seed_seal_ready(connection, "d31-falsifiers")
        _authorize_seal(connection, epoch)

        for null_arguments in (
            (None, SEAL_EXPECTED_REVISION, SEAL_RESULTING_REVISION),
            (epoch, None, SEAL_RESULTING_REVISION),
            (epoch, SEAL_EXPECTED_REVISION, None),
        ):

            def call_with_null(
                arguments: tuple[int | None, int | None, int | None] = null_arguments,
            ) -> object:
                return connection.execute(
                    sql.SQL("SELECT * FROM {}.{}(%s,%s,%s)").format(
                        sql.Identifier(schema_name), sql.Identifier(FUNCTION_NAME)
                    ),
                    arguments,
                )

            _expect_database_error(
                connection,
                call_with_null,
                "invalid preterminal seal-context coordinates",
            )
        for invalid_arguments in (
            (0, 1, 2),
            (epoch, 0, 1),
            (epoch, SEAL_EXPECTED_REVISION, SEAL_EXPECTED_REVISION),
            (epoch, SEAL_EXPECTED_REVISION, SEAL_RESULTING_REVISION + 1),
            (epoch, 9223372036854775807, -9223372036854775808),
        ):

            def call_with_invalid(
                arguments: tuple[int, int, int] = invalid_arguments,
            ) -> object:
                return connection.execute(
                    sql.SQL("SELECT * FROM {}.{}(%s,%s,%s)").format(
                        sql.Identifier(schema_name), sql.Identifier(FUNCTION_NAME)
                    ),
                    arguments,
                )

            _expect_database_error(
                connection,
                call_with_invalid,
            )

        setting_mutations = (
            ("groundloop.m5_checked_transition", ""),
            ("groundloop.m5_checked_transition", "off"),
            ("groundloop.m5_matching_mode", ""),
            ("groundloop.m5_matching_mode", "activation"),
            ("groundloop.m5_matching_epoch_id", ""),
            ("groundloop.m5_matching_epoch_id", str(epoch + 1)),
            ("groundloop.m5_matching_epoch_id", "not-a-bigint"),
            ("groundloop.m5_matching_expected_revision", ""),
            ("groundloop.m5_matching_expected_revision", "4"),
            ("groundloop.m5_matching_resulting_revision", ""),
            ("groundloop.m5_matching_resulting_revision", "5"),
            ("groundloop.m5_matching_policy", ""),
            ("groundloop.m5_matching_policy", "wrong-policy"),
            ("groundloop.m5_matching_context_oid", "1"),
            ("groundloop.m5_matching_journal_oid", "1"),
            ("groundloop.m5_matching_expected_oid", "1"),
        )
        for setting, value in setting_mutations:

            def change_setting(name: str = setting, changed: str = value) -> object:
                connection.execute("SELECT set_config(%s,%s,true)", (name, changed))
                return _accessor_row(connection, schema_name, epoch)

            _expect_database_error(
                connection,
                change_setting,
            )

        row_mutations = (
            "UPDATE pg_temp.groundloop_m5_matching_promotion_context "
            "SET backend_pid=backend_pid+100000",
            "UPDATE pg_temp.groundloop_m5_matching_promotion_context "
            "SET transaction_id=transaction_id+1",
            "UPDATE pg_temp.groundloop_m5_matching_promotion_context "
            "SET session_role=session_role||'-wrong'",
            "UPDATE pg_temp.groundloop_m5_matching_promotion_context "
            "SET mode='activation'",
            "UPDATE pg_temp.groundloop_m5_matching_promotion_context "
            "SET epoch_id=epoch_id+1",
            "UPDATE pg_temp.groundloop_m5_matching_promotion_context "
            "SET expected_revision=expected_revision+1",
            "UPDATE pg_temp.groundloop_m5_matching_promotion_context "
            "SET resulting_revision=resulting_revision+1",
            "UPDATE pg_temp.groundloop_m5_matching_promotion_context "
            "SET policy_version=policy_version||'-wrong'",
            "UPDATE pg_temp.groundloop_m5_matching_promotion_context "
            "SET validation_started=true",
            "UPDATE pg_temp.groundloop_m5_matching_promotion_context "
            "SET validation_done=true",
            "DELETE FROM pg_temp.groundloop_m5_matching_promotion_context",
            "INSERT INTO pg_temp.groundloop_m5_matching_promotion_context "
            "SELECT backend_pid+100000,transaction_id,session_role||'-duplicate',"
            "mode,epoch_id,expected_revision,resulting_revision,policy_version,"
            "anchor_m4_epoch_id,anchor_m5_epoch_id,anchor_m5_revision,"
            "anchor_activation_count,anchor_predecessor_revision,"
            "anchor_predecessor_sealed_at,anchor_current_policy,"
            "validation_started,validation_done FROM pg_temp."
            "groundloop_m5_matching_promotion_context",
        )
        for mutation in row_mutations:

            def change_context_row(statement: str = mutation) -> object:
                connection.execute(statement)
                return _accessor_row(connection, schema_name, epoch)

            _expect_database_error(
                connection,
                change_context_row,
            )

        for relation in (
            "groundloop_m5_matching_transition_context",
            "groundloop_m5_matching_change_journal",
            "groundloop_m5_matching_expected_changes",
        ):

            def create_transition_triplet_member(name: str = relation) -> object:
                connection.execute(
                    sql.SQL("CREATE TEMP TABLE pg_temp.{} (value integer)").format(
                        sql.Identifier(name)
                    )
                )
                return _accessor_row(connection, schema_name, epoch)

            _expect_database_error(
                connection,
                create_transition_triplet_member,
                "transition context conflicts",
            )
        for relation in (
            "groundloop_m5_matching_promotion_context",
            "groundloop_m5_matching_promotion_journal",
            "groundloop_m5_matching_promotion_expected",
        ):

            def drop_relation(name: str = relation) -> object:
                connection.execute(
                    sql.SQL("DROP TABLE pg_temp.{}").format(sql.Identifier(name))
                )
                return _accessor_row(connection, schema_name, epoch)

            _expect_database_error(
                connection,
                drop_relation,
                "incomplete",
            )

        def replace_relation_with_view() -> object:
            connection.execute(
                "DROP TABLE pg_temp.groundloop_m5_matching_promotion_expected"
            )
            connection.execute(
                "CREATE TEMP VIEW "
                "pg_temp.groundloop_m5_matching_promotion_expected AS SELECT 1"
            )
            connection.execute(
                "SELECT set_config('groundloop.m5_matching_expected_oid',"
                "to_regclass('pg_temp.groundloop_m5_matching_promotion_expected')"
                "::oid::text,true)"
            )
            return _accessor_row(connection, schema_name, epoch)

        _expect_database_error(
            connection,
            replace_relation_with_view,
            "not genuine",
        )
        connection.rollback()


def test_two_reads_are_deterministic_and_do_not_mutate_state() -> None:
    with _pre019_schema() as (connection, schema_name):
        assert install_m5_preterminal_seal_context_bundle(connection).applied
        connection.commit()
        _, epoch, _ = _seed_seal_ready(connection, "d31-nonmutation")
        _authorize_seal(connection, epoch)
        connection.execute(
            "CREATE TEMP TABLE d31_deferred_parent(id integer PRIMARY KEY) "
            "ON COMMIT DROP"
        )
        connection.execute(
            "CREATE TEMP TABLE d31_deferred_child("
            "parent_id integer REFERENCES d31_deferred_parent(id) "
            "DEFERRABLE INITIALLY IMMEDIATE) ON COMMIT DROP"
        )
        connection.execute("SET CONSTRAINTS ALL DEFERRED")
        first = _accessor_row(connection, schema_name, epoch)
        before = _accessor_state_snapshot(connection)
        second = _accessor_row(connection, schema_name, epoch)
        after = _accessor_state_snapshot(connection)
        assert second == first
        assert after == before
        assert connection.execute(
            """SELECT validation_started,validation_done
                 FROM pg_temp.groundloop_m5_matching_promotion_context"""
        ).fetchone() == (False, False)
        connection.execute("INSERT INTO d31_deferred_child(parent_id) VALUES (1)")
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()
