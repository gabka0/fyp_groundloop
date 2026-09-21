"""Contract and live PostgreSQL tests for M5-D29 migration 018."""

from __future__ import annotations

import hashlib
import os
import re
import threading
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Any

import psycopg
import pytest
from psycopg import Connection, IsolationLevel, sql
from psycopg.pq import TransactionStatus

from groundloop.m5.digests import hash_field, stable_m5_digest, text_field
from groundloop.postgres import migrations as postgres_migrations
from groundloop.postgres.migrations import (
    M5_ACCEPTED_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_SHA256,
    M5_ACCEPTED_BOUNDED_DOCUMENT_WITHDRAWAL_MIGRATION_SHA256,
    M5_ACCEPTED_PERSISTED_MATCHING_BUNDLE_SHA256,
    M5_ACCEPTED_PERSISTED_MATCHING_MIGRATION_SHA256,
    M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID,
    M5_BOUNDED_DOCUMENT_WITHDRAWAL_INSTALL_LOCK_RELATIONS,
    M5_BOUNDED_DOCUMENT_WITHDRAWAL_MIGRATION_LABEL,
    M5_BOUNDED_DOCUMENT_WITHDRAWAL_MIGRATION_PATH,
    M5_BOUNDED_DOCUMENT_WITHDRAWAL_ORACLE_SHA256,
    M5_PERSISTED_MATCHING_BUNDLE_ID,
    M5BoundedDocumentWithdrawalBundleError,
    M5BoundedDocumentWithdrawalBundleIdentity,
    M5BoundedDocumentWithdrawalBundleInstallResult,
    M5BundleHashConflictError,
    M5PrerequisiteError,
    apply_legacy_migrations,
    install_m5_bounded_document_withdrawal_bundle,
    install_m5_core_bundle,
    install_m5_persisted_matching_bundle,
    install_m5_runtime_bundle,
    install_m5_runtime_recovery_bundle,
    m5_bounded_document_withdrawal_bundle_identity,
)

EXPECTED_SQL = (
    b"CREATE INDEX groundloop_m5_admitted_pair_by_chunk_edge\n"
    b"    ON groundloop_m5_requirement_admitted_pair (\n"
    b'        chunk_version_id COLLATE "C"\n'
    b"    );\n\n"
    b"CREATE INDEX groundloop_m4_job_by_epoch\n"
    b"    ON groundloop_semantic_job (epoch_id);\n"
)

EXPECTED_LOCKS = (
    "groundloop_m5_requirement_admitted_pair",
    "groundloop_semantic_job",
)

EXPECTED_017_LEDGER = (
    "m5-persisted-matching-schema-bundle-v1",
    "52240e19968926d0c051fe6146b3c7d877cf582014341efbfcc78637f3ff5761",
    "e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c",
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565",
)

FROZEN_MIGRATION_HASHES = {
    "migrations/001_m2_base.sql": (
        "ddbfdc1cedec89d9ffd852b66ece4192763099ee34429d5fbb85e54a3a196e37"
    ),
    "migrations/003_m4_dynamic_impact.sql": (
        "fcbe9eaa2ef281cfdc03ddaf25fec3b5dbef73ce6ee957a76b7a3e68b729862c"
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
}

INSTALL_POINTS = (
    "after_initial_ledger",
    "after_prerequisite",
    "after_install_locks",
    "before_first_index",
    "after_admitted_pair_index",
    "after_m4_job_index",
    "before_ledger",
    "after_ledger",
)

INDEX_NAMES = (
    "groundloop_m5_admitted_pair_by_chunk_edge",
    "groundloop_m4_job_by_epoch",
)

M5_JOB_STATES = (
    "declared",
    "running",
    "completed_active",
    "completed_inactive",
    "retryable_failed",
    "terminal_failed",
    "cancelled",
)

LONG_CLAIM_ID_LENGTH = 512
LONG_REQUIREMENT_ID_LENGTH = 256
LONG_CHUNK_ID_LENGTH = 1_900
LONG_M4_ID_LENGTH = 2_000

M5_SCOPE_EQUIVALENT_POINT_INDEX = (
    "groundloop_m5_discovery_scope_root_job_id_scope_contract_di_key"
)

ADMITTED_PAIR_FULL_ROW_SELECT = """SELECT admitted_pair_digest,epoch_id,
       subject_kind,subject_id,chunk_version_id,semantic_pair_digest,
       candidate_policy_id,owner_root_job_id,reasons,mandatory_lineage
  FROM groundloop_m5_requirement_admitted_pair
 WHERE admitted_pair_digest=%s"""

M5_JOB_HYDRATION_SELECT = '''WITH hydrated AS MATERIALIZED (
    SELECT job.*
      FROM groundloop_m5_semantic_job AS job
     WHERE job.epoch_id=%s
     ORDER BY job.job_state,job.logical_job_id
)
SELECT *
  FROM hydrated
 ORDER BY logical_job_id COLLATE "C"'''

M4_JOB_HYDRATION_SELECT = '''SELECT job.*
  FROM groundloop_semantic_job AS job
 WHERE job.epoch_id=%s
 ORDER BY job.job_id COLLATE "C"'''

CURRENT_REQUIREMENT_CURRENCY_SELECT = '''WITH located AS MATERIALIZED (
    SELECT subject_kind,subject_id,chunk_version_id,task_type,
           observation_id,installed_revision
      FROM groundloop_observation_currency
     WHERE chunk_version_id=%s
)
SELECT subject_kind,subject_id,chunk_version_id,task_type,
       observation_id,installed_revision
  FROM located
 WHERE subject_kind='requirement'
 ORDER BY subject_id COLLATE "C",task_type COLLATE "C",
          observation_id COLLATE "C"'''


def _database_url() -> str:
    value = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if value is None:
        pytest.skip("live PostgreSQL test database is not configured")
    return value.replace("postgresql+psycopg://", "postgresql://", 1)


def _select_schema(connection: Connection[Any], schema_name: str) -> None:
    connection.execute(
        sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
    )
    connection.commit()


def _ledger(
    connection: Connection[Any], bundle_id: str
) -> tuple[str, str, str, str, str, str, str] | None:
    row = connection.execute(
        """SELECT bundle_id,bundle_sha256,migration_sha256,oracle_sha256,
                  prerequisite_sha256,applied_at::text,xmin::text
             FROM groundloop_m5_schema_bundle WHERE bundle_id=%s""",
        (bundle_id,),
    ).fetchone()
    if row is None:
        return None
    return tuple(str(value).strip() for value in row)  # type: ignore[return-value]


def _accepted_017_ledger(connection: Connection[Any]) -> tuple[str, ...] | None:
    row = connection.execute(
        """SELECT bundle_id,bundle_sha256,migration_sha256,oracle_sha256,
                  prerequisite_sha256
             FROM groundloop_m5_schema_bundle WHERE bundle_id=%s""",
        (M5_PERSISTED_MATCHING_BUNDLE_ID,),
    ).fetchone()
    return None if row is None else tuple(str(value).strip() for value in row)


def _index_inventory(connection: Connection[Any]) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        tuple(row)
        for row in connection.execute(
            """SELECT index_relation.relname,table_relation.relname,
                      index_row.indisunique,index_row.indisprimary,
                      index_row.indisvalid,index_row.indisready,
                      index_row.indnatts,index_row.indnkeyatts,
                      pg_get_indexdef(index_relation.oid),
                      pg_get_expr(index_row.indpred,index_row.indrelid),
                      pg_get_expr(index_row.indexprs,index_row.indrelid),
                      attribute.attname,collation_row.collname,
                      operator_class.opcname,
                      index_relation.oid::text,index_relation.relfilenode::text
                 FROM pg_index AS index_row
                 JOIN pg_class AS index_relation
                   ON index_relation.oid=index_row.indexrelid
                 JOIN pg_class AS table_relation
                   ON table_relation.oid=index_row.indrelid
                 JOIN pg_namespace AS namespace
                   ON namespace.oid=index_relation.relnamespace
                 LEFT JOIN pg_attribute AS attribute
                   ON attribute.attrelid=table_relation.oid
                  AND attribute.attnum=index_row.indkey[0]
                 LEFT JOIN pg_collation AS collation_row
                   ON collation_row.oid=index_row.indcollation[0]
                 LEFT JOIN pg_opclass AS operator_class
                   ON operator_class.oid=index_row.indclass[0]
                WHERE namespace.nspname=current_schema()
                  AND index_relation.relname=ANY(%s)
                ORDER BY index_relation.relname COLLATE \"C\"""",
            (list(INDEX_NAMES),),
        ).fetchall()
    )


def _assert_not_installed(connection: Connection[Any]) -> None:
    assert _index_inventory(connection) == ()
    assert _ledger(connection, M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID) is None


@contextmanager
def _empty_schema() -> Iterator[tuple[Connection[Any], str]]:
    schema_name = f"d29_migration_018_empty_{uuid.uuid4().hex}"
    dsn = _database_url()
    with psycopg.connect(dsn, autocommit=True) as admin:
        before = tuple(
            str(row[0])
            for row in admin.execute(
                """SELECT schema_name FROM information_schema.schemata
                    WHERE schema_name NOT LIKE 'pg_temp_%'
                      AND schema_name NOT LIKE 'pg_toast_temp_%'
                    ORDER BY schema_name COLLATE \"C\""""
            ).fetchall()
        )
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))

    connection: Connection[Any] = psycopg.connect(dsn)
    try:
        _select_schema(connection, schema_name)
        yield connection, schema_name
    finally:
        connection.close()
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )
            after = tuple(
                str(row[0])
                for row in admin.execute(
                    """SELECT schema_name FROM information_schema.schemata
                        WHERE schema_name NOT LIKE 'pg_temp_%'
                          AND schema_name NOT LIKE 'pg_toast_temp_%'
                        ORDER BY schema_name COLLATE \"C\""""
                ).fetchall()
            )
            assert after == before


@contextmanager
def _pre018_schema() -> Iterator[tuple[Connection[Any], str]]:
    schema_name = f"d29_migration_018_{uuid.uuid4().hex}"
    dsn = _database_url()
    with psycopg.connect(dsn, autocommit=True) as admin:
        before = tuple(
            str(row[0])
            for row in admin.execute(
                """SELECT schema_name FROM information_schema.schemata
                    WHERE schema_name NOT LIKE 'pg_temp_%'
                      AND schema_name NOT LIKE 'pg_toast_temp_%'
                    ORDER BY schema_name COLLATE \"C\""""
            ).fetchall()
        )
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))

    connection: Connection[Any] = psycopg.connect(dsn)
    try:
        _select_schema(connection, schema_name)
        apply_legacy_migrations(connection)
        connection.commit()
        install_m5_core_bundle(connection)
        connection.commit()
        install_m5_runtime_bundle(connection)
        connection.commit()
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        install_m5_persisted_matching_bundle(connection)
        connection.commit()
        assert _accepted_017_ledger(connection) == EXPECTED_017_LEDGER
        _assert_not_installed(connection)
        connection.commit()
        yield connection, schema_name
    finally:
        connection.close()
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )
            after = tuple(
                str(row[0])
                for row in admin.execute(
                    """SELECT schema_name FROM information_schema.schemata
                        WHERE schema_name NOT LIKE 'pg_temp_%'
                          AND schema_name NOT LIKE 'pg_toast_temp_%'
                        ORDER BY schema_name COLLATE \"C\""""
                ).fetchall()
            )
            assert after == before


def _low_compressibility_identifier(label: str, length: int = 1900) -> str:
    blocks = (
        hashlib.sha256(f"{label}:{ordinal}".encode()).hexdigest()
        for ordinal in range((length + 63) // 64)
    )
    return "".join(blocks)[:length]


def _assert_low_compressibility_widths(
    row: Sequence[object] | None, *expected_lengths: int
) -> None:
    assert row is not None
    assert len(row) == 2 * len(expected_lengths)
    for ordinal, expected_length in enumerate(expected_lengths):
        assert row[2 * ordinal] == expected_length
        assert int(row[2 * ordinal + 1]) >= expected_length - 100


@contextmanager
def _pre018_populated_long_schema() -> Iterator[tuple[Any, ...]]:
    from m5.postgres import helpers as b3_helpers

    from tests.m5.postgres_runtime import test_migration_016 as migration_016_tests
    from tests.m5.postgres_runtime import test_migration_017 as migration_017_tests

    schema_name = f"d29_migration_018_populated_{uuid.uuid4().hex}"
    dsn = _database_url()
    with psycopg.connect(dsn, autocommit=True) as admin:
        before = tuple(
            str(row[0])
            for row in admin.execute(
                """SELECT schema_name FROM information_schema.schemata
                    WHERE schema_name NOT LIKE 'pg_temp_%'
                      AND schema_name NOT LIKE 'pg_toast_temp_%'
                    ORDER BY schema_name COLLATE \"C\""""
            ).fetchall()
        )
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))

    connection: Connection[Any] = psycopg.connect(dsn)
    try:
        _select_schema(connection, schema_name)
        apply_legacy_migrations(connection)
        connection.commit()
        install_m5_core_bundle(connection)
        connection.commit()
        install_m5_runtime_bundle(connection)
        connection.commit()
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()

        original_seed_base = b3_helpers.seed_base
        original_make_group = b3_helpers.make_group
        seeded_bases: list[tuple[Any, tuple[str, ...]]] = []
        long_claim_id = _low_compressibility_identifier(
            "d29-width-claim", LONG_CLAIM_ID_LENGTH
        )
        runtime_group_id = _low_compressibility_identifier(
            "d29-width-runtime-group", LONG_CLAIM_ID_LENGTH
        )
        runtime_family_id = _low_compressibility_identifier(
            "d29-width-runtime-family", LONG_CLAIM_ID_LENGTH
        )
        runtime_requirement_ids = (
            _low_compressibility_identifier(
                "d29-width-runtime-requirement", LONG_REQUIREMENT_ID_LENGTH
            ),
        )

        def seed_long_base(selected_connection: Connection[Any], **kwargs: Any) -> Any:
            chunk_texts = tuple(kwargs["chunk_texts"])
            kwargs["chunk_texts"] = ()
            base = original_seed_base(selected_connection, **kwargs)
            selected_connection.execute(
                """INSERT INTO groundloop_claim (
                     claim_id,answer_version_id,text,extractor_model_id,
                     extractor_model_version,extractor_prompt_version,required
                   ) VALUES (%s,%s,'long claim','extractor','v1','p1',true)""",
                (long_claim_id, base.answer_id),
            )
            selected_connection.execute(
                "UPDATE groundloop_claim SET required=false WHERE claim_id=%s",
                (base.claim_ids[0],),
            )
            long_chunk_ids = tuple(
                _low_compressibility_identifier(
                    f"d29-width-chunk-{index}",
                    LONG_CHUNK_ID_LENGTH,
                )
                for index in range(len(chunk_texts))
            )
            document_version = selected_connection.execute(
                "SELECT document_version_id FROM groundloop_document_version"
            ).fetchone()
            assert document_version is not None
            for chunk_index, (chunk_id, chunk_text) in enumerate(
                zip(long_chunk_ids, chunk_texts, strict=True)
            ):
                selected_connection.execute(
                    """INSERT INTO groundloop_chunk_version (
                         chunk_version_id,document_version_id,chunk_index,text,
                         text_hash,chunker_version,valid_from_epoch,valid_to_epoch
                       ) VALUES (%s,%s,%s,%s,%s,'fixture-v1',%s,NULL)""",
                    (
                        chunk_id,
                        document_version[0],
                        chunk_index,
                        chunk_text,
                        hashlib.sha256(chunk_text.encode()).hexdigest(),
                        base.epoch_id,
                    ),
                )
            long_base = replace(
                base,
                claim_ids=(long_claim_id, *base.claim_ids[1:]),
                chunk_ids=long_chunk_ids,
            )
            seeded_bases.append((long_base, chunk_texts))
            return long_base

        def make_long_runtime_group(**kwargs: Any) -> Any:
            if kwargs["group_id"] == "d29-width-zero-hash-group":
                kwargs["group_id"] = runtime_group_id
                kwargs["family_id"] = runtime_family_id
                kwargs["requirement_ids"] = runtime_requirement_ids
            return original_make_group(**kwargs)

        with pytest.MonkeyPatch.context() as preparation_patch:
            preparation_patch.setattr(
                b3_helpers,
                "seed_base",
                seed_long_base,
            )
            preparation_patch.setattr(
                b3_helpers,
                "make_group",
                make_long_runtime_group,
            )
            b3_snapshot = migration_017_tests._seed_b3_activated_snapshot(
                connection,
                "d29-width",
                complete_owner_survivor=True,
            )
        assert len(seeded_bases) == 1
        base, chunk_texts = seeded_bases[0]
        install_m5_persisted_matching_bundle(connection)
        connection.commit()
        assert _accepted_017_ledger(connection) == EXPECTED_017_LEDGER

        connection.execute(
            """INSERT INTO groundloop_model_artifact (
                 model_artifact_id,task,provider,model_id,immutable_revision,
                 tokenizer_revision,license_id,config_hash
               ) VALUES (
                 'failure-replay-embedding','embedding','fixture',
                 'fixture-embedding','v1','v1','MIT',%s
               )""",
            (hashlib.sha256(b"d29-width-embedding").hexdigest(),),
        )
        runtime_group = migration_016_tests.runtime_support.make_group(
            group_id=runtime_group_id,
            family_id=runtime_family_id,
            claim_id=base.claim_ids[5],
            texts=("zero hash",),
            requirement_ids=runtime_requirement_ids,
            source_id="d29-width-zero-hash-source",
        )
        baseline_manifest = migration_016_tests.runtime_support._manifest(base)
        manifest = type(baseline_manifest).build(
            embedding_model_artifact_id=baseline_manifest.embedding_model_artifact_id,
            requirement_role_template_hash=(
                baseline_manifest.requirement_role_template_hash
            ),
            chunk_role_template_hash=baseline_manifest.chunk_role_template_hash,
            vector_method_version=baseline_manifest.vector_method_version,
            vector_index_kind=baseline_manifest.vector_index_kind,
            vector_index_build_config_hash=(
                baseline_manifest.vector_index_build_config_hash
            ),
            vector_search_config_hash=baseline_manifest.vector_search_config_hash,
            lexical_method_version=baseline_manifest.lexical_method_version,
            lexical_config_hash=baseline_manifest.lexical_config_hash,
            lexical_postgres_version=baseline_manifest.lexical_postgres_version,
            lexical_regconfig_identity=baseline_manifest.lexical_regconfig_identity,
            fusion_version=baseline_manifest.fusion_version,
            reverse_budget_per_inserted_chunk=8,
            forward_budget_per_requirement=8,
            verifier_execution_spec_hash=(
                baseline_manifest.verifier_execution_spec_hash
            ),
            decision_policy_version=baseline_manifest.decision_policy_version,
            lineage_safety_override=baseline_manifest.lineage_safety_override,
        )
        migration_016_tests.runtime_support.PostgresM5RuntimeStore(
            connection
        ).register_candidate_policy(manifest)
        requirement_snapshot = (
            migration_016_tests.runtime_support._requirement_snapshot((runtime_group,))
        )
        chunk_snapshot = migration_016_tests.runtime_support.ActiveChunkSnapshot.build(
            tuple(
                migration_016_tests.runtime_support.ActiveChunkSnapshotEntry.build(
                    chunk_version_id=chunk_id,
                    chunk_text=chunk_text,
                )
                for chunk_id, chunk_text in zip(
                    base.chunk_ids,
                    chunk_texts,
                    strict=True,
                )
            )
        )
        database = migration_016_tests.runtime_support.M5RuntimeDatabase(
            dsn="",
            schema_name="",
            connection=connection,
            base=replace(base, answer_id=b3_snapshot.second_answer_id),
            group=runtime_group,
            manifest=manifest,
            requirement_snapshot=requirement_snapshot,
            chunk_snapshot=chunk_snapshot,
            operational_config=(
                migration_016_tests.M5RuntimeOperationalConfig.build(3_600_000)
            ),
        )
        connection.commit()
        _assert_not_installed(connection)
        connection.commit()
        yield (
            connection,
            schema_name,
            database,
            b3_snapshot,
            migration_016_tests,
        )
    finally:
        connection.close()
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )
            after = tuple(
                str(row[0])
                for row in admin.execute(
                    """SELECT schema_name FROM information_schema.schemata
                        WHERE schema_name NOT LIKE 'pg_temp_%'
                          AND schema_name NOT LIKE 'pg_toast_temp_%'
                        ORDER BY schema_name COLLATE \"C\""""
                ).fetchall()
            )
            assert after == before


class _RecordingConnection:
    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection
        self.statements: list[str] = []

    def execute(self, query: Any, *args: Any, **kwargs: Any) -> Any:
        if isinstance(query, str):
            rendered = query
        else:
            rendered = query.as_string(self._connection)
        self.statements.append(" ".join(rendered.split()))
        return self._connection.execute(query, *args, **kwargs)

    def transaction(self, *args: Any, **kwargs: Any) -> Any:
        return self._connection.transaction(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


class _StaticRowResult:
    def __init__(self, row: tuple[object, ...]) -> None:
        self._row = row

    def fetchone(self) -> tuple[object, ...]:
        return self._row


class _EnvironmentOverrideConnection(_RecordingConnection):
    def __init__(self, connection: Connection[Any], row: tuple[object, ...]) -> None:
        super().__init__(connection)
        self._row = row

    def execute(self, query: Any, *args: Any, **kwargs: Any) -> Any:
        rendered = (
            query if isinstance(query, str) else query.as_string(self._connection)
        )
        if "current_setting('server_encoding')" in rendered:
            self.statements.append(" ".join(rendered.split()))
            return _StaticRowResult(self._row)
        return super().execute(query, *args, **kwargs)


@dataclass
class _PausedInstaller:
    thread: threading.Thread
    reached_pause: threading.Event
    resume: threading.Event
    backend_pids: list[int]
    points: list[str]
    results: list[M5BoundedDocumentWithdrawalBundleInstallResult]
    errors: list[BaseException]


def _start_paused_installer(
    schema_name: str,
    *,
    pause_at: str,
    migration_bytes: bytes | None = None,
) -> _PausedInstaller:
    reached_pause = threading.Event()
    resume = threading.Event()
    backend_pids: list[int] = []
    points: list[str] = []
    results: list[M5BoundedDocumentWithdrawalBundleInstallResult] = []
    errors: list[BaseException] = []

    def run() -> None:
        try:
            with psycopg.connect(_database_url()) as connection:
                _select_schema(connection, schema_name)
                backend_pids.append(connection.info.backend_pid)

                def pause(point: str) -> None:
                    points.append(point)
                    if point == pause_at:
                        reached_pause.set()
                        if not resume.wait(timeout=30):
                            raise TimeoutError(
                                f"migration-018 installer was not resumed at {point}"
                            )

                results.append(
                    install_m5_bounded_document_withdrawal_bundle(
                        connection,
                        failure_injector=pause,
                        migration_bytes=migration_bytes,
                    )
                )
        except BaseException as error:  # retained for assertion by the test thread
            errors.append(error)

    thread = threading.Thread(
        target=run,
        name=f"migration-018-{pause_at}",
        daemon=True,
    )
    worker = _PausedInstaller(
        thread=thread,
        reached_pause=reached_pause,
        resume=resume,
        backend_pids=backend_pids,
        points=points,
        results=results,
        errors=errors,
    )
    thread.start()
    return worker


def _resume_and_join(worker: _PausedInstaller) -> None:
    worker.resume.set()
    worker.thread.join(timeout=60)
    assert not worker.thread.is_alive(), "migration-018 installer did not terminate"


@contextmanager
def _blocked_targets(schema_name: str) -> Iterator[None]:
    with psycopg.connect(_database_url()) as blocker:
        _select_schema(blocker, schema_name)
        blocker.execute(
            """LOCK TABLE groundloop_m5_requirement_admitted_pair,
                          groundloop_semantic_job
                   IN ACCESS EXCLUSIVE MODE"""
        )
        try:
            yield
        finally:
            blocker.rollback()


def _replace_017_ledger_field(
    connection: Connection[Any], field: str, value: str
) -> None:
    if field not in {
        "bundle_id",
        "bundle_sha256",
        "migration_sha256",
        "oracle_sha256",
        "prerequisite_sha256",
    }:
        raise AssertionError(f"unexpected ledger field: {field}")
    connection.execute(
        "ALTER TABLE groundloop_m5_schema_bundle "
        "DISABLE TRIGGER groundloop_m5_schema_bundle_immutable"
    )
    current_bundle_id = (
        M5_PERSISTED_MATCHING_BUNDLE_ID
        if field != "bundle_id"
        else EXPECTED_017_LEDGER[0]
    )
    connection.execute(
        sql.SQL(
            "UPDATE groundloop_m5_schema_bundle SET {}=%s WHERE bundle_id=%s"
        ).format(sql.Identifier(field)),
        (value, current_bundle_id),
    )
    connection.execute(
        "ALTER TABLE groundloop_m5_schema_bundle "
        "ENABLE TRIGGER groundloop_m5_schema_bundle_immutable"
    )
    connection.commit()


def _restore_017_ledger(connection: Connection[Any], changed_field: str) -> None:
    field_index = {
        "bundle_id": 0,
        "bundle_sha256": 1,
        "migration_sha256": 2,
        "oracle_sha256": 3,
        "prerequisite_sha256": 4,
    }[changed_field]
    current_bundle_id = (
        "m5-persisted-matching-schema-bundle-v1-mismatch"
        if changed_field == "bundle_id"
        else M5_PERSISTED_MATCHING_BUNDLE_ID
    )
    connection.execute(
        "ALTER TABLE groundloop_m5_schema_bundle "
        "DISABLE TRIGGER groundloop_m5_schema_bundle_immutable"
    )
    connection.execute(
        sql.SQL(
            "UPDATE groundloop_m5_schema_bundle SET {}=%s WHERE bundle_id=%s"
        ).format(sql.Identifier(changed_field)),
        (EXPECTED_017_LEDGER[field_index], current_bundle_id),
    )
    connection.execute(
        "ALTER TABLE groundloop_m5_schema_bundle "
        "ENABLE TRIGGER groundloop_m5_schema_bundle_immutable"
    )
    connection.commit()
    assert _accepted_017_ledger(connection) == EXPECTED_017_LEDGER
    connection.commit()


def _explain_text(
    connection: Connection[Any], statement: str, parameters: Sequence[object]
) -> str:
    rows = connection.execute(
        "EXPLAIN (COSTS OFF, FORMAT TEXT) " + statement,
        parameters,
    ).fetchall()
    return "\n".join(str(row[0]) for row in rows)


def _unique_leading_point_indexes(
    connection: Connection[Any], *, table_name: str, leading_column: str
) -> dict[str, tuple[Any, ...]]:
    rows = connection.execute(
        '''SELECT index_relation.relname,
                  index_row.indisprimary,index_row.indisunique,
                  index_row.indisvalid,index_row.indisready,
                  index_row.indnatts,index_row.indnkeyatts,
                  ARRAY(
                      SELECT attribute.attname
                        FROM unnest(index_row.indkey)
                             WITH ORDINALITY AS key(attnum,ordinality)
                        JOIN pg_attribute AS attribute
                          ON attribute.attrelid=index_row.indrelid
                         AND attribute.attnum=key.attnum
                       WHERE key.ordinality <= index_row.indnkeyatts
                       ORDER BY key.ordinality
                  )
             FROM pg_index AS index_row
             JOIN pg_class AS index_relation
               ON index_relation.oid=index_row.indexrelid
             JOIN pg_class AS table_relation
               ON table_relation.oid=index_row.indrelid
             JOIN pg_namespace AS namespace
               ON namespace.oid=table_relation.relnamespace
             JOIN pg_attribute AS leading_attribute
               ON leading_attribute.attrelid=index_row.indrelid
              AND leading_attribute.attnum=index_row.indkey[0]
            WHERE namespace.nspname=current_schema()
              AND table_relation.relname=%s
              AND index_row.indisunique
              AND leading_attribute.attname=%s
            ORDER BY index_relation.relname COLLATE "C"''',
        (table_name, leading_column),
    ).fetchall()
    return {str(row[0]): tuple(row[1:]) for row in rows}


def _insert_valid_admitted_pair_fixture(
    connection: Connection[Any],
    *,
    database: Any,
    migration_016_tests: Any,
    event_id: str,
    chunk_index: int,
    include_all_job_states: bool = False,
) -> dict[str, object]:
    epoch_id, roots = migration_016_tests._legacy_open_overlap_event(
        database,
        event_id=event_id,
    )
    chunk_version_id = database.base.chunk_ids[chunk_index]
    pair_chunk_ids = (
        database.base.chunk_ids[:7] if include_all_job_states else (chunk_version_id,)
    )
    assert len(pair_chunk_ids) == (7 if include_all_job_states else 1)
    pairs = tuple(
        migration_016_tests.SemanticPairKey(
            migration_016_tests.SubjectKind.REQUIREMENT,
            database.group.requirements[0].requirement_version_id,
            pair_chunk_id,
        )
        for pair_chunk_id in pair_chunk_ids
    )
    revision = 1
    results = []
    for scope, root_job in roots:
        _, lease = migration_016_tests._acquire_requirement_job_for_recovery_test(
            connection,
            epoch_id=epoch_id,
            expected_revision=revision,
            job=root_job,
        )
        revision += 1
        result_pairs = (
            pairs
            if root_job.job_kind.value == "forward_requirement_retrieval"
            else tuple(
                pair
                for pair in pairs
                if pair.chunk_version_id == scope.inserted_chunk_version_id
            )
        )
        assert result_pairs
        result = migration_016_tests._legacy_requirement_result(
            epoch_id=epoch_id,
            root=root_job,
            pairs=result_pairs,
        )
        staged, _ = migration_016_tests._stage_legacy_requirement_root(
            connection,
            epoch_id=epoch_id,
            expected_revision=revision,
            lease=lease,
            job=root_job,
            result=result,
        )
        results.append(result)
        revision = int(staged.resulting_revision)
    barrier = migration_016_tests._close_legacy_requirement_roots(
        connection,
        epoch_id=epoch_id,
        expected_revision=revision,
        manifest=database.manifest,
        roots=roots,
        results=tuple(results),
    )
    connection.commit()
    final_revision = int(barrier.resulting_revision)
    if include_all_job_states:
        final_revision = _install_all_m5_job_states(
            connection,
            database=database,
            migration_016_tests=migration_016_tests,
            epoch_id=epoch_id,
            expected_revision=final_revision,
        )
    admitted = connection.execute(
        """SELECT admitted_pair_digest,owner_root_job_id
             FROM groundloop_m5_requirement_admitted_pair
            WHERE epoch_id=%s AND chunk_version_id=%s""",
        (epoch_id, chunk_version_id),
    ).fetchone()
    assert admitted is not None
    forward_root = connection.execute(
        """SELECT scope.root_job_id
             FROM groundloop_m5_discovery_scope AS scope
             JOIN groundloop_m5_requirement_root_provenance AS provenance
               ON provenance.epoch_id=scope.epoch_id
              AND provenance.root_job_id=scope.root_job_id
            WHERE scope.epoch_id=%s AND scope.direction='forward_requirement'""",
        (epoch_id,),
    ).fetchone()
    assert forward_root is not None
    connection.commit()
    return {
        "epoch_id": epoch_id,
        "revision": final_revision,
        "chunk_version_id": chunk_version_id,
        "semantic_pair_digest": next(
            pair.semantic_pair_digest
            for pair in pairs
            if pair.chunk_version_id == chunk_version_id
        ),
        "candidate_policy_id": database.manifest.candidate_policy_id,
        "admitted_pair_digest": str(admitted[0]).strip(),
        "owner_root_job_id": str(admitted[1]).strip(),
        "forward_root_job_id": str(forward_root[0]).strip(),
    }


def _install_all_m5_job_states(
    connection: Connection[Any],
    *,
    database: Any,
    migration_016_tests: Any,
    epoch_id: int,
    expected_revision: int,
) -> int:
    jobs = tuple(
        sorted(
            migration_016_tests.runtime_support.PostgresM5RuntimeStore(
                connection
            ).verifier_jobs(epoch_id),
            key=lambda job: job.logical_job_id,
        )
    )
    assert len(jobs) == len(M5_JOB_STATES)
    revision = expected_revision
    for job in jobs[1:]:
        row, _ = migration_016_tests._acquire_requirement_job_for_recovery_test(
            connection,
            epoch_id=epoch_id,
            expected_revision=revision,
            job=job,
        )
        revision = int(row["dispatched_revision"])

    state_jobs = dict(zip(M5_JOB_STATES, jobs, strict=True))
    completion_arguments = {
        "completed_active": {
            "result_artifact_id": hashlib.sha256(
                b"d29-all-states-completed-active-id"
            ).hexdigest(),
            "result_artifact_hash": hashlib.sha256(
                b"d29-all-states-completed-active-hash"
            ).hexdigest(),
            "archive_reason": None,
        },
        "completed_inactive": {
            "result_artifact_id": hashlib.sha256(
                b"d29-all-states-completed-inactive-id"
            ).hexdigest(),
            "result_artifact_hash": hashlib.sha256(
                b"d29-all-states-completed-inactive-hash"
            ).hexdigest(),
            "archive_reason": migration_016_tests.M5TerminalReason.CHUNK_INACTIVE,
        },
        "terminal_failed": {
            "result_artifact_id": None,
            "result_artifact_hash": None,
            "archive_reason": migration_016_tests.M5TerminalReason.VERIFIER_ERROR,
        },
        "cancelled": {
            "result_artifact_id": None,
            "result_artifact_hash": None,
            "archive_reason": migration_016_tests.M5TerminalReason.SUBJECT_INACTIVE,
        },
    }
    completions = {
        state: migration_016_tests.M5JobCompletion.build(
            job=state_jobs[state],
            terminal_state=migration_016_tests.M5JobState(state),
            **arguments,
        )
        for state, arguments in completion_arguments.items()
    }
    resulting_revision = revision + 1
    with connection.transaction():
        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s,%s)",
            (epoch_id, revision),
        )
        assert (
            connection.execute(
                """UPDATE groundloop_m5_semantic_job
                      SET job_state='retryable_failed'
                    WHERE epoch_id=%s AND logical_job_id=%s
                      AND job_state='running'""",
                (epoch_id, state_jobs["retryable_failed"].logical_job_id),
            ).rowcount
            == 1
        )
        for state in (
            "completed_active",
            "completed_inactive",
            "terminal_failed",
            "cancelled",
        ):
            completion = completions[state]
            cancelled = state == "cancelled"
            assert (
                connection.execute(
                    """UPDATE groundloop_m5_semantic_job
                          SET job_state=%s,result_artifact_id=%s,
                              result_artifact_hash=%s,archive_reason=%s,
                              completion_digest=%s,completed_revision=%s,
                              completed_at=clock_timestamp(),
                              cancelled_by_event_id=%s,cancelled_by_epoch_id=%s,
                              cancellation_reason=%s
                        WHERE epoch_id=%s AND logical_job_id=%s
                          AND job_state='running'""",
                    (
                        state,
                        completion.result_artifact_id,
                        completion.result_artifact_hash,
                        (
                            completion.archive_reason.value
                            if completion.archive_reason is not None
                            else None
                        ),
                        completion.completion_digest,
                        resulting_revision,
                        (state_jobs[state].structural_event_id if cancelled else None),
                        epoch_id if cancelled else None,
                        (
                            completion.archive_reason.value
                            if cancelled and completion.archive_reason is not None
                            else None
                        ),
                        epoch_id,
                        state_jobs[state].logical_job_id,
                    ),
                ).rowcount
                == 1
            )
        for relation in (
            "groundloop_m5_owner_pending_counter",
            "groundloop_m5_answer_pending_counter",
        ):
            connection.execute(
                sql.SQL(
                    "UPDATE {} SET verifier_job_count=3,"
                    " blocking_failure_count=1,updated_revision=%s"
                    " WHERE epoch_id=%s"
                ).format(sql.Identifier(relation)),
                (resulting_revision, epoch_id),
            )
        assert (
            connection.execute(
                """UPDATE groundloop_epoch SET revision=%s
                    WHERE epoch_id=%s AND revision=%s""",
                (resulting_revision, epoch_id, revision),
            ).rowcount
            == 1
        )
        assert (
            connection.execute(
                """UPDATE groundloop_m5_runtime_epoch
                      SET revision=%s,open_work_count=3,
                          blocking_failure_count=1
                    WHERE epoch_id=%s AND revision=%s""",
                (resulting_revision, epoch_id, revision),
            ).rowcount
            == 1
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.execute("SET CONSTRAINTS ALL DEFERRED")
    connection.commit()
    return resulting_revision


def _insert_m4_job_graph(
    connection: Connection[Any],
    *,
    database: Any,
    label: str,
    long_job_id: str,
    chunk_version_id: str,
) -> dict[str, object]:
    digest = hashlib.sha256(label.encode()).hexdigest()
    model_id = f"d29-width-{label}-model"
    candidate_id = f"d29-width-{label}-candidate"
    registry_snapshot_id = _low_compressibility_identifier(
        f"d29-width-{label}-registry", LONG_M4_ID_LENGTH
    )
    frontier_job_id = _low_compressibility_identifier(
        f"d29-width-{label}-frontier-job", LONG_M4_ID_LENGTH
    )
    connection.execute(
        """INSERT INTO groundloop_model_artifact
           (model_artifact_id,task,provider,model_id,immutable_revision,
            tokenizer_revision,license_id,config_hash)
           VALUES (%s,'embedding','test','test','v1','v1','test',%s)""",
        (model_id, digest),
    )
    connection.execute(
        """INSERT INTO groundloop_candidate_policy
           (candidate_policy_id,policy_hash,embedding_model_artifact_id,
            decision_policy_version,claim_role_template_hash,
            chunk_role_template_hash,vector_method_version,vector_index_kind,
            vector_index_build_config_hash,vector_search_config_hash,
            lexical_method_version,lexical_config_hash,
            lexical_postgres_version,lexical_regconfig_identity,
            claim_registry_snapshot_id,claim_count,fusion_version,
            approximate_cap_per_inserted_chunk,frontier_depth,manifest)
           VALUES (%s,%s,%s,%s,%s,%s,'v1','exact',%s,%s,'v1',%s,
                   '16','simple',%s,1,'v1',1,1,'{}')""",
        (
            candidate_id,
            hashlib.sha256(f"{label}:policy".encode()).hexdigest(),
            model_id,
            database.base.policy_version,
            digest,
            digest,
            digest,
            digest,
            digest,
            registry_snapshot_id,
        ),
    )
    epoch_row = connection.execute(
        """INSERT INTO groundloop_epoch
           (event_id,payload_hash,revision,structural_status,semantic_status,
            evaluation_state,publication_mode,sealed_at)
           VALUES (%s,%s,0,'committed','sealed','complete','strict',now())
           RETURNING epoch_id""",
        (f"d29-width-{label}-event", digest),
    ).fetchone()
    assert epoch_row is not None
    epoch_id = int(epoch_row[0])
    child_job_id = hashlib.sha256(f"{label}:child".encode()).hexdigest()
    connection.execute(
        """INSERT INTO groundloop_semantic_job
           (job_id,epoch_id,parent_job_id,job_kind,candidate_policy_id,
            payload_hash,execution_spec_hash,claim_id,chunk_version_id,
            expandable,job_state,child_closed,created_revision)
           VALUES (%s,%s,NULL,'impact_discovery',%s,%s,%s,NULL,%s,
                   true,'declared',false,0)""",
        (long_job_id, epoch_id, candidate_id, digest, digest, chunk_version_id),
    )
    connection.execute(
        """INSERT INTO groundloop_semantic_job
           (job_id,epoch_id,parent_job_id,job_kind,candidate_policy_id,
            payload_hash,execution_spec_hash,claim_id,chunk_version_id,
            expandable,job_state,child_closed,created_revision)
           VALUES (%s,%s,%s,'verify_pair',%s,%s,%s,%s,%s,
                   false,'declared',false,0)""",
        (
            child_job_id,
            epoch_id,
            long_job_id,
            candidate_id,
            digest,
            digest,
            database.base.claim_ids[0],
            chunk_version_id,
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_semantic_job
           (job_id,epoch_id,parent_job_id,job_kind,candidate_policy_id,
            payload_hash,execution_spec_hash,claim_id,chunk_version_id,
            expandable,job_state,child_closed,created_revision)
           VALUES (%s,%s,NULL,'frontier_retrieve',%s,%s,%s,%s,NULL,
                   true,'declared',false,0)""",
        (
            frontier_job_id,
            epoch_id,
            candidate_id,
            digest,
            digest,
            database.base.claim_ids[0],
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_semantic_job_dependency
           (epoch_id,parent_job_id,child_job_id) VALUES (%s,%s,%s)""",
        (epoch_id, long_job_id, child_job_id),
    )
    connection.execute(
        """INSERT INTO groundloop_discovery_scope
           (root_job_id,epoch_id,registry_snapshot_id,scope_kind,
            explicit_claim_ids,closed_revision)
           VALUES (%s,%s,%s,'all_registered_claims',NULL,NULL)""",
        (long_job_id, epoch_id, registry_snapshot_id),
    )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()
    return {
        "epoch_id": epoch_id,
        "root_job_id": long_job_id,
        "frontier_job_id": frontier_job_id,
        "child_job_id": child_job_id,
        "chunk_version_id": chunk_version_id,
        "claim_id": database.base.claim_ids[0],
        "registry_snapshot_id": registry_snapshot_id,
    }


def test_exact_two_statement_bytes_order_and_no_forbidden_sql() -> None:
    source = M5_BOUNDED_DOCUMENT_WITHDRAWAL_MIGRATION_PATH.read_bytes()
    assert source == EXPECTED_SQL
    text = source.decode("utf-8")
    assert text.count("CREATE INDEX ") == 2
    assert text.count(";") == 2
    assert re.findall(r"CREATE INDEX ([a-z0-9_]+)", text) == list(INDEX_NAMES)
    assert "groundloop_m5_requirement_admitted_pair" in text
    assert 'chunk_version_id COLLATE "C"' in text
    assert "groundloop_semantic_job (epoch_id)" in text
    for forbidden in (
        "CONCURRENTLY",
        "CREATE UNIQUE",
        "INCLUDE",
        " WHERE ",
        "ALTER ",
        "DROP ",
        "INSERT ",
        "UPDATE ",
        "DELETE ",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "LOCK TABLE",
        "CREATE TABLE",
        "CREATE FUNCTION",
        "CREATE TRIGGER",
        "GRANT ",
        "REVOKE ",
    ):
        assert forbidden not in text.upper()


def test_identity_formula_literal_pins_and_exact_017_binding() -> None:
    identity = m5_bounded_document_withdrawal_bundle_identity()
    migration_hash = hashlib.sha256(EXPECTED_SQL).hexdigest()
    expected_bundle_hash = stable_m5_digest(
        "m5-bounded-document-withdrawal-schema-bundle-v1",
        text_field("migrations/018_m5_bounded_document_withdrawal.sql"),
        hash_field(migration_hash),
        hash_field(EXPECTED_017_LEDGER[1]),
    )
    assert isinstance(identity, M5BoundedDocumentWithdrawalBundleIdentity)
    assert identity == M5BoundedDocumentWithdrawalBundleIdentity(
        bundle_id="m5-bounded-document-withdrawal-schema-bundle-v1",
        bundle_sha256=expected_bundle_hash,
        migration_sha256=migration_hash,
        oracle_sha256=hashlib.sha256(b"").hexdigest(),
        prerequisite_sha256=EXPECTED_017_LEDGER[1],
    )
    assert M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID == identity.bundle_id
    assert M5_BOUNDED_DOCUMENT_WITHDRAWAL_MIGRATION_LABEL == (
        "migrations/018_m5_bounded_document_withdrawal.sql"
    )
    assert M5_BOUNDED_DOCUMENT_WITHDRAWAL_ORACLE_SHA256 == identity.oracle_sha256
    assert M5_ACCEPTED_BOUNDED_DOCUMENT_WITHDRAWAL_MIGRATION_SHA256 == migration_hash
    assert M5_ACCEPTED_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_SHA256 == (
        expected_bundle_hash
    )
    assert migration_hash == (
        "941bba975c12e9fb5ba4b4f75a82e59fa518b23eac34468ed2f8b15d1cd9ed90"
    )
    assert expected_bundle_hash == (
        "9c45e58fb5c61156d4d07aa0c9b767112bf285452664f39731d893445e7a9e4f"
    )
    assert M5_ACCEPTED_PERSISTED_MATCHING_MIGRATION_SHA256 == EXPECTED_017_LEDGER[2]
    assert M5_ACCEPTED_PERSISTED_MATCHING_BUNDLE_SHA256 == EXPECTED_017_LEDGER[1]
    assert M5_BOUNDED_DOCUMENT_WITHDRAWAL_INSTALL_LOCK_RELATIONS == EXPECTED_LOCKS


def test_frozen_migrations_001_003_and_013_through_017_are_unchanged() -> None:
    root = M5_BOUNDED_DOCUMENT_WITHDRAWAL_MIGRATION_PATH.parents[1]
    assert {
        relative: hashlib.sha256((root / relative).read_bytes()).hexdigest()
        for relative in FROZEN_MIGRATION_HASHES
    } == FROZEN_MIGRATION_HASHES


def test_first_install_result_ledger_and_exact_catalog_shapes() -> None:
    with _pre018_schema() as (connection, _):
        result = install_m5_bounded_document_withdrawal_bundle(connection)
        assert isinstance(result, M5BoundedDocumentWithdrawalBundleInstallResult)
        assert result.applied
        assert result.identity == m5_bounded_document_withdrawal_bundle_identity()
        inventory = _index_inventory(connection)
        assert len(inventory) == 2
        by_name = {str(row[0]): row for row in inventory}

        admitted = by_name["groundloop_m5_admitted_pair_by_chunk_edge"]
        assert admitted[1:8] == (
            "groundloop_m5_requirement_admitted_pair",
            False,
            False,
            True,
            True,
            1,
            1,
        )
        assert admitted[9:14] == (
            None,
            None,
            "chunk_version_id",
            "C",
            "text_ops",
        )
        assert '(chunk_version_id COLLATE "C")' in str(admitted[8])

        jobs = by_name["groundloop_m4_job_by_epoch"]
        assert jobs[1:8] == (
            "groundloop_semantic_job",
            False,
            False,
            True,
            True,
            1,
            1,
        )
        assert jobs[9:14] == (None, None, "epoch_id", None, "int8_ops")
        assert "(epoch_id)" in str(jobs[8])

        ledger = _ledger(connection, M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID)
        assert ledger is not None
        assert ledger[:5] == (
            result.identity.bundle_id,
            result.identity.bundle_sha256,
            result.identity.migration_sha256,
            result.identity.oracle_sha256,
            result.identity.prerequisite_sha256,
        )
        assert _accepted_017_ledger(connection) == EXPECTED_017_LEDGER
        connection.commit()


def test_exact_rerun_is_lock_free_and_catalog_stable() -> None:
    with _pre018_schema() as (connection, schema_name):
        assert install_m5_bounded_document_withdrawal_bundle(connection).applied
        connection.commit()
        before_catalog = _index_inventory(connection)
        before_ledger = _ledger(connection, M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID)
        before_017 = _accepted_017_ledger(connection)
        connection.commit()
        with _blocked_targets(schema_name):
            replay = install_m5_bounded_document_withdrawal_bundle(connection)
        assert not replay.applied
        assert replay.identity == m5_bounded_document_withdrawal_bundle_identity()
        assert _index_inventory(connection) == before_catalog
        assert (
            _ledger(connection, M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID)
            == before_ledger
        )
        assert _accepted_017_ledger(connection) == before_017
        connection.commit()


def test_arbitrary_first_install_bytes_reject_before_prerequisite_and_ddl() -> None:
    with _pre018_schema() as (connection, _):
        points: list[str] = []
        with pytest.raises(
            M5BoundedDocumentWithdrawalBundleError,
            match="frozen M5-D29 authority",
        ):
            install_m5_bounded_document_withdrawal_bundle(
                connection,
                migration_bytes=b"-- unauthorized migration-018 bytes\n",
                failure_injector=points.append,
            )
        assert points == ["after_initial_ledger"]
        _assert_not_installed(connection)
        assert _accepted_017_ledger(connection) == EXPECTED_017_LEDGER
        connection.commit()


def test_same_id_differing_ledger_is_ledger_first_and_lock_free() -> None:
    with _pre018_schema() as (connection, schema_name):
        identity = m5_bounded_document_withdrawal_bundle_identity()
        connection.execute(
            """INSERT INTO groundloop_m5_schema_bundle
               (bundle_id,bundle_sha256,migration_sha256,oracle_sha256,
                prerequisite_sha256)
               VALUES (%s,%s,%s,%s,%s)""",
            (
                identity.bundle_id,
                "f" * 64,
                identity.migration_sha256,
                identity.oracle_sha256,
                identity.prerequisite_sha256,
            ),
        )
        connection.commit()
        with _blocked_targets(schema_name), pytest.raises(M5BundleHashConflictError):
            install_m5_bounded_document_withdrawal_bundle(connection)
        assert _index_inventory(connection) == ()
        connection.commit()


@pytest.mark.parametrize(
    "field",
    (
        "bundle_id",
        "bundle_sha256",
        "migration_sha256",
        "oracle_sha256",
        "prerequisite_sha256",
    ),
)
def test_each_017_field_mismatch_rejects_before_target_lock_and_ddl(field: str) -> None:
    with _pre018_schema() as (connection, schema_name):
        mismatch = (
            "m5-persisted-matching-schema-bundle-v1-mismatch"
            if field == "bundle_id"
            else "f" * 64
        )
        _replace_017_ledger_field(connection, field, mismatch)
        with (
            _blocked_targets(schema_name),
            pytest.raises(M5PrerequisiteError, match="migration-017"),
        ):
            install_m5_bounded_document_withdrawal_bundle(connection)
        _assert_not_installed(connection)
        connection.commit()
        _restore_017_ledger(connection, field)


def test_ambient_transaction_and_savepoint_reject_before_ledger() -> None:
    with _pre018_schema() as (connection, _):
        connection.execute("SELECT 1")
        assert connection.info.transaction_status == TransactionStatus.INTRANS
        with pytest.raises(
            M5BoundedDocumentWithdrawalBundleError, match="idle connection"
        ):
            install_m5_bounded_document_withdrawal_bundle(connection)
        connection.rollback()

        with connection.transaction():
            with connection.transaction():
                with pytest.raises(
                    M5BoundedDocumentWithdrawalBundleError,
                    match="idle connection",
                ):
                    install_m5_bounded_document_withdrawal_bundle(connection)
        _assert_not_installed(connection)
        connection.commit()


@pytest.mark.parametrize(
    ("setting", "value"),
    (
        ("isolation_level", IsolationLevel.REPEATABLE_READ),
        ("isolation_level", IsolationLevel.SERIALIZABLE),
        ("read_only", True),
    ),
)
def test_non_read_committed_or_read_only_rejects_before_ledger(
    setting: str, value: object
) -> None:
    with _pre018_schema() as (connection, _):
        setattr(connection, setting, value)
        try:
            with pytest.raises(
                M5BoundedDocumentWithdrawalBundleError,
                match="read-write READ COMMITTED",
            ):
                install_m5_bounded_document_withdrawal_bundle(connection)
        finally:
            setattr(connection, setting, None)
        _assert_not_installed(connection)
        connection.commit()


def test_utf8_and_c_collation_environment_is_explicitly_accepted() -> None:
    with _pre018_schema() as (connection, _):
        assert connection.execute("SHOW server_encoding").fetchone() == ("UTF8",)
        assert connection.execute(
            "SELECT collname FROM pg_collation WHERE oid='\"C\"'::regcollation"
        ).fetchone() == ("C",)
        assert connection.execute(
            "SELECT ('A' COLLATE \"C\") < ('a' COLLATE \"C\")"
        ).fetchone() == (True,)
        connection.commit()
        assert install_m5_bounded_document_withdrawal_bundle(connection).applied
        connection.commit()


@pytest.mark.parametrize(
    "environment_row",
    (
        ("LATIN1", "pg_catalog", "C", "c", True, -1, True, True),
        ("UTF8", "pg_catalog", "C", "c", True, -1, True, False),
    ),
)
def test_invalid_encoding_or_c_order_rejects_before_lock_and_ddl(
    environment_row: tuple[object, ...],
) -> None:
    with _pre018_schema() as (connection, schema_name):
        overridden = _EnvironmentOverrideConnection(connection, environment_row)
        with (
            _blocked_targets(schema_name),
            pytest.raises(
                M5BoundedDocumentWithdrawalBundleError,
                match="UTF-8.*C collation",
            ),
        ):
            install_m5_bounded_document_withdrawal_bundle(overridden)  # type: ignore[arg-type]
        _assert_not_installed(connection)
        connection.commit()


def test_installer_uses_one_exact_two_table_nowait_lock_statement() -> None:
    with _pre018_schema() as (connection, _):
        recording = _RecordingConnection(connection)
        result = install_m5_bounded_document_withdrawal_bundle(recording)  # type: ignore[arg-type]
        assert result.applied
        lock_statements = [
            statement
            for statement in recording.statements
            if statement.startswith("LOCK TABLE")
        ]
        assert lock_statements == [
            "LOCK TABLE groundloop_m5_requirement_admitted_pair, "
            "groundloop_semantic_job IN SHARE ROW EXCLUSIVE MODE NOWAIT"
        ]
        assert (
            sum(
                statement.startswith(
                    "CREATE INDEX groundloop_m5_admitted_pair_by_chunk_edge"
                )
                for statement in recording.statements
            )
            == 1
        )
        assert (
            sum(
                statement.startswith("CREATE INDEX groundloop_m4_job_by_epoch")
                for statement in recording.statements
            )
            == 1
        )
        assert recording.statements.index(lock_statements[0]) < (
            recording.statements.index(
                "CREATE INDEX groundloop_m5_admitted_pair_by_chunk_edge "
                "ON groundloop_m5_requirement_admitted_pair "
                '( chunk_version_id COLLATE "C" );'
            )
        )
        connection.commit()


def test_paused_installer_holds_exact_two_granted_target_relation_locks() -> None:
    with _pre018_schema() as (connection, schema_name):
        worker = _start_paused_installer(
            schema_name,
            pause_at="after_install_locks",
        )
        try:
            assert worker.reached_pause.wait(timeout=30)
            assert len(worker.backend_pids) == 1
            locks = connection.execute(
                """SELECT relation.relname,lock_row.mode,lock_row.granted
                     FROM pg_locks AS lock_row
                     JOIN pg_class AS relation ON relation.oid=lock_row.relation
                     JOIN pg_namespace AS namespace
                       ON namespace.oid=relation.relnamespace
                    WHERE lock_row.pid=%s
                      AND lock_row.locktype='relation'
                      AND lock_row.mode='ShareRowExclusiveLock'
                      AND namespace.nspname=%s
                    ORDER BY relation.relname COLLATE \"C\"""",
                (worker.backend_pids[0], schema_name),
            ).fetchall()
            assert locks == [
                (EXPECTED_LOCKS[0], "ShareRowExclusiveLock", True),
                (EXPECTED_LOCKS[1], "ShareRowExclusiveLock", True),
            ]
            connection.commit()
        finally:
            _resume_and_join(worker)

        assert worker.errors == []
        assert len(worker.results) == 1 and worker.results[0].applied
        assert worker.points == list(INSTALL_POINTS)


def test_private_route_authority_normalizes_missing_pre_m5_ledger_table() -> None:
    with _empty_schema() as (connection, _):
        verifier = (
            postgres_migrations._verify_m5_bounded_document_withdrawal_route_authority
        )
        with pytest.raises(
            M5BoundedDocumentWithdrawalBundleError,
            match="exact accepted five-field migration-018 ledger row",
        ):
            verifier(connection)
        connection.rollback()


def test_private_route_authority_rejects_exact_fallback_schema_ledger() -> None:
    with _empty_schema() as (connection, current_schema):
        fallback_schema = f"d29_migration_018_fallback_{uuid.uuid4().hex}"
        connection.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(fallback_schema))
        )
        connection.execute(
            sql.SQL(
                """CREATE TABLE {}.groundloop_m5_schema_bundle (
                       bundle_id text PRIMARY KEY,
                       bundle_sha256 text NOT NULL,
                       migration_sha256 text NOT NULL,
                       oracle_sha256 text NOT NULL,
                       prerequisite_sha256 text NOT NULL
                   )"""
            ).format(sql.Identifier(fallback_schema))
        )
        identity = m5_bounded_document_withdrawal_bundle_identity()
        connection.execute(
            sql.SQL(
                """INSERT INTO {}.groundloop_m5_schema_bundle
                   (bundle_id,bundle_sha256,migration_sha256,oracle_sha256,
                    prerequisite_sha256) VALUES (%s,%s,%s,%s,%s)"""
            ).format(sql.Identifier(fallback_schema)),
            (
                identity.bundle_id,
                identity.bundle_sha256,
                identity.migration_sha256,
                identity.oracle_sha256,
                identity.prerequisite_sha256,
            ),
        )
        connection.execute(
            sql.SQL("SET search_path TO {}, {}, public").format(
                sql.Identifier(current_schema),
                sql.Identifier(fallback_schema),
            )
        )
        assert connection.execute("SELECT current_schema()").fetchone() == (
            current_schema,
        )
        assert connection.execute(
            """SELECT namespace_row.nspname
                 FROM pg_catalog.pg_class AS relation_row
                 JOIN pg_catalog.pg_namespace AS namespace_row
                   ON namespace_row.oid=relation_row.relnamespace
                WHERE relation_row.oid=
                      to_regclass('groundloop_m5_schema_bundle')"""
        ).fetchone() == (fallback_schema,)
        assert connection.execute(
            """SELECT count(*)
                 FROM pg_catalog.pg_class AS relation_row
                 JOIN pg_catalog.pg_namespace AS namespace_row
                   ON namespace_row.oid=relation_row.relnamespace
                WHERE namespace_row.nspname=%s
                  AND relation_row.relname IN (
                      'groundloop_m5_schema_bundle',
                      'groundloop_m5_admitted_pair_by_chunk_edge',
                      'groundloop_m4_job_by_epoch'
                  )""",
            (current_schema,),
        ).fetchone() == (0,)
        verifier = (
            postgres_migrations._verify_m5_bounded_document_withdrawal_route_authority
        )
        with pytest.raises(
            M5BoundedDocumentWithdrawalBundleError,
            match="exact accepted five-field migration-018 ledger row",
        ):
            verifier(connection)
        connection.rollback()


def test_private_route_authority_is_exact_lock_free_and_five_field_complete() -> None:
    with _pre018_schema() as (connection, schema_name):
        verifier = (
            postgres_migrations._verify_m5_bounded_document_withdrawal_route_authority
        )
        with pytest.raises(
            M5BoundedDocumentWithdrawalBundleError,
            match="exact accepted five-field migration-018 ledger row",
        ):
            verifier(connection)
        connection.rollback()

        assert install_m5_bounded_document_withdrawal_bundle(connection).applied
        connection.commit()
        with _blocked_targets(schema_name):
            verifier(connection)
        connection.commit()

        exact = m5_bounded_document_withdrawal_bundle_identity()
        expected = {
            "bundle_id": exact.bundle_id,
            "bundle_sha256": exact.bundle_sha256,
            "migration_sha256": exact.migration_sha256,
            "oracle_sha256": exact.oracle_sha256,
            "prerequisite_sha256": exact.prerequisite_sha256,
        }
        for field, accepted_value in expected.items():
            mismatch = (
                exact.bundle_id + "-mismatch" if field == "bundle_id" else "f" * 64
            )
            connection.execute(
                "ALTER TABLE groundloop_m5_schema_bundle "
                "DISABLE TRIGGER groundloop_m5_schema_bundle_immutable"
            )
            connection.execute(
                sql.SQL(
                    "UPDATE groundloop_m5_schema_bundle SET {}=%s WHERE bundle_id=%s"
                ).format(sql.Identifier(field)),
                (mismatch, exact.bundle_id),
            )
            connection.execute(
                "ALTER TABLE groundloop_m5_schema_bundle "
                "ENABLE TRIGGER groundloop_m5_schema_bundle_immutable"
            )
            connection.commit()
            with pytest.raises(
                M5BoundedDocumentWithdrawalBundleError,
                match="exact accepted five-field migration-018 ledger row",
            ):
                verifier(connection)
            connection.rollback()

            connection.execute(
                "ALTER TABLE groundloop_m5_schema_bundle "
                "DISABLE TRIGGER groundloop_m5_schema_bundle_immutable"
            )
            current_bundle_id = mismatch if field == "bundle_id" else exact.bundle_id
            connection.execute(
                sql.SQL(
                    "UPDATE groundloop_m5_schema_bundle SET {}=%s WHERE bundle_id=%s"
                ).format(sql.Identifier(field)),
                (accepted_value, current_bundle_id),
            )
            connection.execute(
                "ALTER TABLE groundloop_m5_schema_bundle "
                "ENABLE TRIGGER groundloop_m5_schema_bundle_immutable"
            )
            connection.commit()
            verifier(connection)
            connection.commit()


def test_failure_at_every_checkpoint_is_atomic_and_fresh_retry_succeeds() -> None:
    with _pre018_schema() as (connection, _):
        for index, failure_point in enumerate(INSTALL_POINTS):
            observed: list[str] = []

            def fail(
                point: str,
                *,
                _observed: list[str] = observed,
                _failure_point: str = failure_point,
            ) -> None:
                _observed.append(point)
                if point == _failure_point:
                    raise RuntimeError(f"injected migration-018 failure at {point}")

            with pytest.raises(RuntimeError, match=re.escape(failure_point)):
                install_m5_bounded_document_withdrawal_bundle(
                    connection, failure_injector=fail
                )
            assert observed == list(INSTALL_POINTS[: index + 1])
            _assert_not_installed(connection)
            assert _accepted_017_ledger(connection) == EXPECTED_017_LEDGER
            connection.commit()

        retry = install_m5_bounded_document_withdrawal_bundle(connection)
        assert retry.applied
        assert len(_index_inventory(connection)) == 2
        connection.commit()


@pytest.mark.parametrize("conflicting_relation", INDEX_NAMES)
def test_each_create_index_execute_failure_rolls_back_and_fresh_retry_succeeds(
    conflicting_relation: str,
) -> None:
    with _pre018_schema() as (connection, _):
        connection.execute(
            sql.SQL("CREATE SEQUENCE {}").format(sql.Identifier(conflicting_relation))
        )
        connection.commit()

        with pytest.raises(psycopg.errors.DuplicateTable):
            install_m5_bounded_document_withdrawal_bundle(connection)
        _assert_not_installed(connection)
        assert connection.execute(
            "SELECT to_regclass(%s)::text",
            (conflicting_relation,),
        ).fetchone() == (conflicting_relation,)
        assert _accepted_017_ledger(connection) == EXPECTED_017_LEDGER
        connection.commit()

        connection.execute(
            sql.SQL("DROP SEQUENCE {}").format(sql.Identifier(conflicting_relation))
        )
        connection.commit()
        retry = install_m5_bounded_document_withdrawal_bundle(connection)
        assert retry.applied
        assert len(_index_inventory(connection)) == 2
        connection.commit()


def test_unavailable_first_target_is_lock_free_and_new_tx_retries() -> None:
    with _pre018_schema() as (connection, schema_name):
        with psycopg.connect(_database_url()) as blocker:
            _select_schema(blocker, schema_name)
            blocker.execute(
                "LOCK TABLE groundloop_m5_requirement_admitted_pair "
                "IN ACCESS EXCLUSIVE MODE"
            )
            with pytest.raises(psycopg.errors.LockNotAvailable):
                install_m5_bounded_document_withdrawal_bundle(connection)
            _assert_not_installed(connection)
            connection.commit()
            blocker.rollback()

        assert install_m5_bounded_document_withdrawal_bundle(connection).applied
        connection.commit()


def test_unavailable_second_target_rolls_back_lock_prefix_and_new_tx_retries() -> None:
    with _pre018_schema() as (connection, schema_name):
        with psycopg.connect(_database_url()) as blocker:
            _select_schema(blocker, schema_name)
            blocker.execute(
                "LOCK TABLE groundloop_semantic_job IN ACCESS EXCLUSIVE MODE"
            )
            with pytest.raises(psycopg.errors.LockNotAvailable):
                install_m5_bounded_document_withdrawal_bundle(connection)
            _assert_not_installed(connection)
            connection.commit()

            with psycopg.connect(_database_url()) as probe:
                _select_schema(probe, schema_name)
                probe.execute(
                    "LOCK TABLE groundloop_m5_requirement_admitted_pair "
                    "IN ACCESS EXCLUSIVE MODE NOWAIT"
                )
                probe.rollback()
            blocker.rollback()

        assert install_m5_bounded_document_withdrawal_bundle(connection).applied
        connection.commit()


def test_two_same_byte_installers_fail_fast_then_replay_exactly() -> None:
    with _pre018_schema() as (connection, schema_name):
        first = _start_paused_installer(schema_name, pause_at="after_install_locks")
        try:
            assert first.reached_pause.wait(timeout=30)
            with pytest.raises(psycopg.errors.LockNotAvailable):
                install_m5_bounded_document_withdrawal_bundle(connection)
        finally:
            _resume_and_join(first)

        assert first.errors == []
        assert len(first.results) == 1 and first.results[0].applied
        assert first.points == list(INSTALL_POINTS)
        replay = install_m5_bounded_document_withdrawal_bundle(connection)
        assert not replay.applied
        assert len(_index_inventory(connection)) == 2
        connection.commit()


def test_commit_between_ledger_reads_becomes_exact_noop() -> None:
    with _pre018_schema() as (connection, schema_name):
        late = _start_paused_installer(schema_name, pause_at="after_initial_ledger")
        try:
            assert late.reached_pause.wait(timeout=30)
            winner = install_m5_bounded_document_withdrawal_bundle(connection)
            assert winner.applied
            connection.commit()
            winner_inventory = _index_inventory(connection)
            winner_ledger = _ledger(
                connection, M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID
            )
            connection.commit()
        finally:
            _resume_and_join(late)

        assert late.errors == []
        assert len(late.results) == 1 and not late.results[0].applied
        assert late.points == [
            "after_initial_ledger",
            "after_prerequisite",
            "after_install_locks",
        ]
        assert _index_inventory(connection) == winner_inventory
        assert (
            _ledger(connection, M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID)
            == winner_ledger
        )
        connection.commit()


def test_conflicting_concurrent_installer_detects_post_lock_ledger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alternate_bytes = EXPECTED_SQL + b"\n"
    alternate_identity = m5_bounded_document_withdrawal_bundle_identity(
        migration_bytes=alternate_bytes
    )
    assert alternate_identity.migration_sha256 != (
        M5_ACCEPTED_BOUNDED_DOCUMENT_WITHDRAWAL_MIGRATION_SHA256
    )
    assert alternate_identity.bundle_sha256 != (
        M5_ACCEPTED_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_SHA256
    )

    with _pre018_schema() as (connection, schema_name):
        current = _start_paused_installer(schema_name, pause_at="after_initial_ledger")
        try:
            assert current.reached_pause.wait(timeout=30)
            with monkeypatch.context() as alternate_authority:
                alternate_authority.setattr(
                    postgres_migrations,
                    "M5_ACCEPTED_BOUNDED_DOCUMENT_WITHDRAWAL_MIGRATION_SHA256",
                    alternate_identity.migration_sha256,
                )
                alternate_authority.setattr(
                    postgres_migrations,
                    "M5_ACCEPTED_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_SHA256",
                    alternate_identity.bundle_sha256,
                )
                winner = install_m5_bounded_document_withdrawal_bundle(
                    connection, migration_bytes=alternate_bytes
                )
                assert winner.applied
                connection.commit()
                winner_inventory = _index_inventory(connection)
                winner_ledger = _ledger(
                    connection, M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID
                )
                connection.commit()
        finally:
            _resume_and_join(current)

        assert current.results == []
        assert len(current.errors) == 1
        assert isinstance(current.errors[0], M5BundleHashConflictError)
        assert current.points == [
            "after_initial_ledger",
            "after_prerequisite",
            "after_install_locks",
        ]
        assert _index_inventory(connection) == winner_inventory
        assert (
            _ledger(connection, M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID)
            == winner_ledger
        )
        connection.commit()


def test_populated_long_width_and_named_routes_before_and_after_install() -> None:
    with _pre018_populated_long_schema() as (
        connection,
        _,
        database,
        b3_snapshot,
        migration_016_tests,
    ):
        first_m4 = _insert_m4_job_graph(
            connection,
            database=database,
            label="first",
            long_job_id=_low_compressibility_identifier("d29-width-first-job", 2000),
            chunk_version_id=database.base.chunk_ids[0],
        )
        first_m5 = _insert_valid_admitted_pair_fixture(
            connection,
            database=database,
            migration_016_tests=migration_016_tests,
            event_id="d29-width-first-requirement-event",
            chunk_index=0,
            include_all_job_states=True,
        )
        _assert_low_compressibility_widths(
            connection.execute(
                """SELECT length(chunk_version_id),pg_column_size(chunk_version_id)
                 FROM groundloop_m5_requirement_admitted_pair
                WHERE admitted_pair_digest=%s""",
                (first_m5["admitted_pair_digest"],),
            ).fetchone(),
            LONG_CHUNK_ID_LENGTH,
        )
        _assert_low_compressibility_widths(
            connection.execute(
                """SELECT length(subject_id),pg_column_size(subject_id)
                 FROM groundloop_m5_requirement_admitted_pair
                WHERE admitted_pair_digest=%s""",
                (first_m5["admitted_pair_digest"],),
            ).fetchone(),
            LONG_REQUIREMENT_ID_LENGTH,
        )
        _assert_low_compressibility_widths(
            connection.execute(
                """SELECT length(requirement.requirement_version_id),
                      pg_column_size(requirement.requirement_version_id),
                      length(subject.subject_id),pg_column_size(subject.subject_id)
                 FROM groundloop_m5_requirement_version AS requirement
                 JOIN groundloop_semantic_subject AS subject
                   ON subject.subject_kind='requirement'
                  AND subject.subject_id=requirement.requirement_version_id
                WHERE requirement.requirement_version_id=%s""",
                (database.group.requirements[0].requirement_version_id,),
            ).fetchone(),
            LONG_REQUIREMENT_ID_LENGTH,
            LONG_REQUIREMENT_ID_LENGTH,
        )
        _assert_low_compressibility_widths(
            connection.execute(
                """SELECT length(claim_id),pg_column_size(claim_id)
                 FROM groundloop_claim WHERE claim_id=%s""",
                (database.base.claim_ids[0],),
            ).fetchone(),
            LONG_CLAIM_ID_LENGTH,
        )
        _assert_low_compressibility_widths(
            connection.execute(
                "SELECT length(job_id),pg_column_size(job_id) "
                "FROM groundloop_semantic_job WHERE job_id=%s",
                (first_m4["root_job_id"],),
            ).fetchone(),
            LONG_M4_ID_LENGTH,
        )
        _assert_low_compressibility_widths(
            connection.execute(
                """SELECT length(scope.root_job_id),pg_column_size(scope.root_job_id),
                      length(scope.registry_snapshot_id),
                      pg_column_size(scope.registry_snapshot_id)
                 FROM groundloop_discovery_scope AS scope
                WHERE scope.root_job_id=%s""",
                (first_m4["root_job_id"],),
            ).fetchone(),
            LONG_M4_ID_LENGTH,
            LONG_M4_ID_LENGTH,
        )
        frontier_width = connection.execute(
            """SELECT length(job_id),pg_column_size(job_id),
                      length(claim_id),pg_column_size(claim_id)
                 FROM groundloop_semantic_job WHERE job_id=%s""",
            (first_m4["frontier_job_id"],),
        ).fetchone()
        _assert_low_compressibility_widths(
            frontier_width,
            LONG_M4_ID_LENGTH,
            LONG_CLAIM_ID_LENGTH,
        )
        _assert_low_compressibility_widths(
            connection.execute(
                """SELECT length(claim_id),pg_column_size(claim_id),
                      length(chunk_version_id),pg_column_size(chunk_version_id)
                 FROM groundloop_semantic_job WHERE job_id=%s""",
                (first_m4["child_job_id"],),
            ).fetchone(),
            LONG_CLAIM_ID_LENGTH,
            LONG_CHUNK_ID_LENGTH,
        )
        connection.commit()

        assert install_m5_bounded_document_withdrawal_bundle(connection).applied
        connection.commit()
        connection.execute("SET LOCAL enable_seqscan=off")

        admitted_plan = _explain_text(
            connection,
            """SELECT chunk_version_id,admitted_pair_digest
                 FROM groundloop_m5_requirement_admitted_pair
                WHERE chunk_version_id COLLATE \"C\"=%s
                ORDER BY admitted_pair_digest COLLATE \"C\"""",
            (str(first_m5["chunk_version_id"]),),
        )
        admitted_point_plan = _explain_text(
            connection,
            ADMITTED_PAIR_FULL_ROW_SELECT,
            (str(first_m5["admitted_pair_digest"]),),
        )
        currency_chunk_id = database.base.chunk_ids[6]
        current_requirement_plan = _explain_text(
            connection,
            CURRENT_REQUIREMENT_CURRENCY_SELECT,
            (currency_chunk_id,),
        )
        m4_job_plan = _explain_text(
            connection,
            M4_JOB_HYDRATION_SELECT,
            (int(first_m4["epoch_id"]),),
        )
        m4_scope_plan = _explain_text(
            connection,
            "SELECT * FROM groundloop_discovery_scope WHERE root_job_id=%s",
            (str(first_m4["root_job_id"]),),
        )
        dependency_plan = _explain_text(
            connection,
            """SELECT epoch_id,parent_job_id,child_job_id
                 FROM groundloop_semantic_job_dependency
                WHERE epoch_id=%s
                ORDER BY parent_job_id COLLATE \"C\",child_job_id COLLATE \"C\"""",
            (int(first_m4["epoch_id"]),),
        )
        m5_job_plan = _explain_text(
            connection,
            M5_JOB_HYDRATION_SELECT,
            (int(first_m5["epoch_id"]),),
        )
        scope_plan = _explain_text(
            connection,
            """SELECT scope.* FROM groundloop_m5_discovery_scope AS scope
                WHERE root_job_id=%s""",
            (str(first_m5["forward_root_job_id"]),),
        )
        provenance_plan = _explain_text(
            connection,
            """SELECT * FROM groundloop_m5_requirement_root_provenance
                WHERE epoch_id=%s AND root_job_id=%s""",
            (
                int(first_m5["epoch_id"]),
                str(first_m5["forward_root_job_id"]),
            ),
        )

        assert "groundloop_m5_admitted_pair_by_chunk_edge" in admitted_plan
        assert "groundloop_current_observations_by_chunk" in current_requirement_plan
        assert "groundloop_m4_job_by_epoch" in m4_job_plan
        assert "groundloop_discovery_scope_pkey" in m4_scope_plan
        assert "groundloop_semantic_job_dependency_pkey" in dependency_plan
        assert "groundloop_m5_job_by_epoch_state" in m5_job_plan
        # The table also has two UNIQUE indexes beginning with root_job_id.  On
        # this tiny populated fixture PostgreSQL deterministically selects the
        # equivalent root_job_id/scope_contract_digest unique point path; the
        # catalog assertion below separately pins the required one-column PK.
        assert M5_SCOPE_EQUIVALENT_POINT_INDEX in scope_plan
        assert "groundloop_m5_requirement_root_provenance_pkey" in provenance_plan
        assert all(
            "Seq Scan" not in plan
            for plan in (
                admitted_plan,
                admitted_point_plan,
                current_requirement_plan,
                m4_job_plan,
                m4_scope_plan,
                dependency_plan,
                m5_job_plan,
                scope_plan,
                provenance_plan,
            )
        )

        admitted_point_indexes = _unique_leading_point_indexes(
            connection,
            table_name="groundloop_m5_requirement_admitted_pair",
            leading_column="admitted_pair_digest",
        )
        assert admitted_point_indexes[
            "groundloop_m5_requirement_admitted_pair_pkey"
        ] == (True, True, True, True, 1, 1, ["admitted_pair_digest"])
        admitted_equivalent_indexes = {
            name: shape
            for name, shape in admitted_point_indexes.items()
            if name != "groundloop_m5_requirement_admitted_pair_pkey"
        }
        assert len(admitted_equivalent_indexes) == 1
        admitted_equivalent_name, admitted_equivalent_shape = next(
            iter(admitted_equivalent_indexes.items())
        )
        assert admitted_equivalent_shape == (
            False,
            True,
            True,
            True,
            2,
            2,
            ["admitted_pair_digest", "semantic_pair_digest"],
        )
        admitted_selected_indexes = {
            name for name in admitted_point_indexes if name in admitted_point_plan
        }
        # Both accepted unique indexes have the complete point key as their
        # leading attribute.  PostgreSQL may pick either equivalent path; the
        # catalog assertions pin the required one-column primary key separately.
        assert admitted_selected_indexes in (
            {"groundloop_m5_requirement_admitted_pair_pkey"},
            {admitted_equivalent_name},
        )

        scope_indexes = _unique_leading_point_indexes(
            connection,
            table_name="groundloop_m5_discovery_scope",
            leading_column="root_job_id",
        )
        assert scope_indexes["groundloop_m5_discovery_scope_pkey"] == (
            True,
            True,
            True,
            True,
            1,
            1,
            ["root_job_id"],
        )
        assert scope_indexes[M5_SCOPE_EQUIVALENT_POINT_INDEX] == (
            False,
            True,
            True,
            True,
            2,
            2,
            ["root_job_id", "scope_contract_digest"],
        )
        assert len(scope_indexes) == 3

        admitted_row = connection.execute(
            ADMITTED_PAIR_FULL_ROW_SELECT,
            (str(first_m5["admitted_pair_digest"]),),
        ).fetchone()
        assert admitted_row is not None and len(admitted_row) == 10
        assert tuple(admitted_row[:8]) == (
            first_m5["admitted_pair_digest"],
            first_m5["epoch_id"],
            "requirement",
            database.group.requirements[0].requirement_version_id,
            first_m5["chunk_version_id"],
            first_m5["semantic_pair_digest"],
            first_m5["candidate_policy_id"],
            first_m5["owner_root_job_id"],
        )
        assert tuple(admitted_row[8]) == tuple(sorted(admitted_row[8]))
        assert admitted_row[9] is ("lineage" in admitted_row[8])

        currency_rows = connection.execute(
            CURRENT_REQUIREMENT_CURRENCY_SELECT,
            (currency_chunk_id,),
        ).fetchall()
        assert len(currency_rows) == 2
        assert tuple(currency_rows) == tuple(
            sorted(currency_rows, key=lambda row: (row[1], row[3], row[4]))
        )
        assert any(
            tuple(row[:4])
            == (
                "requirement",
                b3_snapshot.complete_requirement_ids[0],
                currency_chunk_id,
                "verify_requirement_v1",
            )
            and row[4] == "d29-width-complete-a-observation"
            and row[5] == b3_snapshot.revision
            for row in currency_rows
        )

        m4_job_cursor = connection.execute(
            M4_JOB_HYDRATION_SELECT,
            (int(first_m4["epoch_id"]),),
        )
        assert m4_job_cursor.description is not None
        m4_job_columns = tuple(column.name for column in m4_job_cursor.description)
        m4_job_rows = tuple(m4_job_cursor.fetchall())
        m4_job_id_index = m4_job_columns.index("job_id")
        m4_job_kind_index = m4_job_columns.index("job_kind")
        m4_parent_index = m4_job_columns.index("parent_job_id")
        assert len(m4_job_rows) == 3
        assert tuple(row[m4_job_id_index] for row in m4_job_rows) == tuple(
            sorted(row[m4_job_id_index] for row in m4_job_rows)
        )
        assert {
            row[m4_job_id_index]: row[m4_job_kind_index] for row in m4_job_rows
        } == {
            first_m4["root_job_id"]: "impact_discovery",
            first_m4["frontier_job_id"]: "frontier_retrieve",
            first_m4["child_job_id"]: "verify_pair",
        }
        assert (
            next(
                row[m4_parent_index]
                for row in m4_job_rows
                if row[m4_job_id_index] == first_m4["child_job_id"]
            )
            == first_m4["root_job_id"]
        )
        for row in m4_job_rows:
            scope = connection.execute(
                "SELECT * FROM groundloop_discovery_scope WHERE root_job_id=%s",
                (row[m4_job_id_index],),
            ).fetchone()
            assert (scope is not None) is (
                row[m4_job_kind_index] == "impact_discovery"
                and row[m4_parent_index] is None
            )

        assert connection.execute(
            """SELECT parent_job_id,child_job_id
                 FROM groundloop_semantic_job_dependency
                WHERE epoch_id=%s""",
            (int(first_m4["epoch_id"]),),
        ).fetchone() == (first_m4["root_job_id"], first_m4["child_job_id"])

        m5_job_cursor = connection.execute(
            M5_JOB_HYDRATION_SELECT,
            (int(first_m5["epoch_id"]),),
        )
        assert m5_job_cursor.description is not None
        m5_job_columns = tuple(column.name for column in m5_job_cursor.description)
        m5_job_rows = tuple(m5_job_cursor.fetchall())
        assert (
            len(m5_job_columns)
            == connection.execute(
                """SELECT count(*)
                 FROM pg_attribute
                WHERE attrelid='groundloop_m5_semantic_job'::regclass
                  AND attnum>0 AND NOT attisdropped"""
            ).fetchone()[0]
        )
        logical_job_index = m5_job_columns.index("logical_job_id")
        epoch_index = m5_job_columns.index("epoch_id")
        job_state_index = m5_job_columns.index("job_state")
        parent_job_index = m5_job_columns.index("parent_job_id")
        assert len(m5_job_rows) == 9
        assert tuple(row[logical_job_index] for row in m5_job_rows) == tuple(
            sorted(row[logical_job_index] for row in m5_job_rows)
        )
        assert {row[epoch_index] for row in m5_job_rows} == {first_m5["epoch_id"]}
        assert {row[job_state_index] for row in m5_job_rows} == set(M5_JOB_STATES)
        root_job_ids = {
            row[logical_job_index]
            for row in m5_job_rows
            if row[parent_job_index] is None
        }
        assert len(root_job_ids) == 2
        for row in m5_job_rows:
            logical_job_id = row[logical_job_index]
            scope = connection.execute(
                "SELECT * FROM groundloop_m5_discovery_scope WHERE root_job_id=%s",
                (logical_job_id,),
            ).fetchone()
            provenance = connection.execute(
                """SELECT * FROM groundloop_m5_requirement_root_provenance
                    WHERE epoch_id=%s AND root_job_id=%s""",
                (first_m5["epoch_id"], logical_job_id),
            ).fetchone()
            assert (scope is not None) is (logical_job_id in root_job_ids)
            assert (provenance is not None) is (
                logical_job_id == first_m5["forward_root_job_id"]
            )
        connection.commit()

    with _pre018_populated_long_schema() as (
        connection,
        _,
        database,
        _b3_snapshot,
        migration_016_tests,
    ):
        assert install_m5_bounded_document_withdrawal_bundle(connection).applied
        connection.commit()
        post_m5 = _insert_valid_admitted_pair_fixture(
            connection,
            database=database,
            migration_016_tests=migration_016_tests,
            event_id="d29-width-post-install-requirement-event",
            chunk_index=0,
        )
        post_m4 = _insert_m4_job_graph(
            connection,
            database=database,
            label="post-install",
            long_job_id=_low_compressibility_identifier(
                "d29-width-post-install-job", 2000
            ),
            chunk_version_id=str(post_m5["chunk_version_id"]),
        )
        _assert_low_compressibility_widths(
            connection.execute(
                """SELECT length(chunk_version_id),pg_column_size(chunk_version_id)
                 FROM groundloop_m5_requirement_admitted_pair
                WHERE admitted_pair_digest=%s""",
                (post_m5["admitted_pair_digest"],),
            ).fetchone(),
            LONG_CHUNK_ID_LENGTH,
        )
        _assert_low_compressibility_widths(
            connection.execute(
                """SELECT length(subject_id),pg_column_size(subject_id)
                 FROM groundloop_m5_requirement_admitted_pair
                WHERE admitted_pair_digest=%s""",
                (post_m5["admitted_pair_digest"],),
            ).fetchone(),
            LONG_REQUIREMENT_ID_LENGTH,
        )
        _assert_low_compressibility_widths(
            connection.execute(
                """SELECT length(requirement.requirement_version_id),
                      pg_column_size(requirement.requirement_version_id),
                      length(subject.subject_id),pg_column_size(subject.subject_id)
                 FROM groundloop_m5_requirement_version AS requirement
                 JOIN groundloop_semantic_subject AS subject
                   ON subject.subject_kind='requirement'
                  AND subject.subject_id=requirement.requirement_version_id
                WHERE requirement.requirement_version_id=%s""",
                (database.group.requirements[0].requirement_version_id,),
            ).fetchone(),
            LONG_REQUIREMENT_ID_LENGTH,
            LONG_REQUIREMENT_ID_LENGTH,
        )
        _assert_low_compressibility_widths(
            connection.execute(
                """SELECT length(claim_id),pg_column_size(claim_id)
                 FROM groundloop_claim WHERE claim_id=%s""",
                (database.base.claim_ids[0],),
            ).fetchone(),
            LONG_CLAIM_ID_LENGTH,
        )
        _assert_low_compressibility_widths(
            connection.execute(
                "SELECT length(job_id),pg_column_size(job_id) "
                "FROM groundloop_semantic_job WHERE job_id=%s",
                (post_m4["root_job_id"],),
            ).fetchone(),
            LONG_M4_ID_LENGTH,
        )
        _assert_low_compressibility_widths(
            connection.execute(
                """SELECT length(root_job_id),pg_column_size(root_job_id),
                      length(registry_snapshot_id),
                      pg_column_size(registry_snapshot_id)
                 FROM groundloop_discovery_scope WHERE root_job_id=%s""",
                (post_m4["root_job_id"],),
            ).fetchone(),
            LONG_M4_ID_LENGTH,
            LONG_M4_ID_LENGTH,
        )
        _assert_low_compressibility_widths(
            connection.execute(
                """SELECT length(job_id),pg_column_size(job_id),
                      length(claim_id),pg_column_size(claim_id)
                 FROM groundloop_semantic_job WHERE job_id=%s""",
                (post_m4["frontier_job_id"],),
            ).fetchone(),
            LONG_M4_ID_LENGTH,
            LONG_CLAIM_ID_LENGTH,
        )
        _assert_low_compressibility_widths(
            connection.execute(
                """SELECT length(claim_id),pg_column_size(claim_id),
                      length(chunk_version_id),pg_column_size(chunk_version_id)
                 FROM groundloop_semantic_job WHERE job_id=%s""",
                (post_m4["child_job_id"],),
            ).fetchone(),
            LONG_CLAIM_ID_LENGTH,
            LONG_CHUNK_ID_LENGTH,
        )
        connection.execute("SET LOCAL enable_seqscan=off")
        post_admitted_plan = _explain_text(
            connection,
            """SELECT chunk_version_id,admitted_pair_digest
                 FROM groundloop_m5_requirement_admitted_pair
                WHERE chunk_version_id COLLATE \"C\"=%s
                ORDER BY admitted_pair_digest COLLATE \"C\"""",
            (str(post_m5["chunk_version_id"]),),
        )
        post_admitted_point_plan = _explain_text(
            connection,
            ADMITTED_PAIR_FULL_ROW_SELECT,
            (str(post_m5["admitted_pair_digest"]),),
        )
        post_m4_plan = _explain_text(
            connection,
            M4_JOB_HYDRATION_SELECT,
            (int(post_m4["epoch_id"]),),
        )
        assert "groundloop_m5_admitted_pair_by_chunk_edge" in post_admitted_plan
        assert "groundloop_m4_job_by_epoch" in post_m4_plan
        assert "Seq Scan" not in post_admitted_plan
        assert "Seq Scan" not in post_admitted_point_plan
        assert "Seq Scan" not in post_m4_plan
        post_admitted_point_indexes = _unique_leading_point_indexes(
            connection,
            table_name="groundloop_m5_requirement_admitted_pair",
            leading_column="admitted_pair_digest",
        )
        assert (
            len(
                {
                    name
                    for name in post_admitted_point_indexes
                    if name in post_admitted_point_plan
                }
            )
            == 1
        )
        post_admitted_row = connection.execute(
            ADMITTED_PAIR_FULL_ROW_SELECT,
            (str(post_m5["admitted_pair_digest"]),),
        ).fetchone()
        assert post_admitted_row is not None and len(post_admitted_row) == 10
        assert tuple(post_admitted_row[:8]) == (
            post_m5["admitted_pair_digest"],
            post_m5["epoch_id"],
            "requirement",
            database.group.requirements[0].requirement_version_id,
            post_m5["chunk_version_id"],
            post_m5["semantic_pair_digest"],
            post_m5["candidate_policy_id"],
            post_m5["owner_root_job_id"],
        )
        post_m4_rows = connection.execute(
            M4_JOB_HYDRATION_SELECT,
            (int(post_m4["epoch_id"]),),
        ).fetchall()
        assert len(post_m4_rows) == 3
        connection.commit()


def test_migration_017_exact_replay_remains_accepted_after_018() -> None:
    with _pre018_schema() as (connection, _):
        assert install_m5_bounded_document_withdrawal_bundle(connection).applied
        connection.commit()
        before_018 = _ledger(connection, M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID)
        connection.commit()
        replay_017 = install_m5_persisted_matching_bundle(connection)
        assert not replay_017.applied
        assert _accepted_017_ledger(connection) == EXPECTED_017_LEDGER
        assert (
            _ledger(connection, M5_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_ID) == before_018
        )
        connection.commit()
