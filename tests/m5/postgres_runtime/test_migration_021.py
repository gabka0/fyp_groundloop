"""D33 SQL guard diagnostics, not production document runtime acceptance.

Raw SQL fixtures are allowed before the separate H33 literal hash freeze.
Installer tests must be run only after that authority is accepted.
"""

from __future__ import annotations

import hashlib
import re
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, fields, replace
from pathlib import Path
from typing import Any

import psycopg
import pytest
from m5.postgres_runtime import conftest as runtime_fixtures
from psycopg import Connection, IsolationLevel, sql
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

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
from groundloop.errors import EventConflictError, ValidationError
from groundloop.events import (
    ChunkInput,
    DeleteDocumentVersionEvent,
    ReplaceDocumentVersionEvent,
)
from groundloop.m4.application import DynamicEventPlan
from groundloop.m4.contracts import (
    CandidatePolicyManifest,
    CorpusUpdateIdentity,
    UpdateKind,
    stable_m4_digest,
)
from groundloop.m4.evaluation_overlay import (
    EvaluationTransition,
    EvaluationTransitionKind,
    PostgresEvaluationOverlayStore,
)
from groundloop.m4.persistence import PostgresM4RuntimeStore, _policy_payload
from groundloop.m4.pipeline import InsertedDocument, StructuralPayload
from groundloop.m5.digests import normalized_text_hash_v1
from groundloop.m5.events import legacy_event_payload_digest
from groundloop.m5.reference import (
    build_reference_claim_certificate,
    build_reference_group_certificate,
    compute_reference_states,
)
from groundloop.m5.repository import M5Repository
from groundloop.m5.runtime import digests as runtime_digests
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5AttemptOutput,
    M5CancellationPlan,
    M5ExecutionEvidenceDisposition,
    M5PersistedMatchingSourceKind,
    M5RuntimeTiming,
    M5RuntimeWork,
    M5TerminalReason,
    M5TypedEventPlan,
)
from groundloop.m5.runtime.persistence import (
    PostgresM5RuntimeStore,
    _persist_active_chunk_snapshot,
    _persist_initial_pending_counters,
    _persist_requirement_snapshot,
    _persist_root_declarations,
    _root_declarations_for_open,
    _validate_effective_snapshots,
)
from groundloop.m5.runtime.postgres_matching import (
    _empty_patch_artifact,
    _insert_matching_artifact,
    _prepare_matching_transition,
)
from groundloop.m5.runtime.postgres_matching_publication import (
    _present_artifact_hash,
    promote_matching_overlay,
)
from groundloop.m5.runtime.postgres_recovery import (
    persist_structural_open_accounting,
    persist_structural_open_identity,
    start_event_accounting,
)
from groundloop.m5.runtime.postgres_withdrawal import (
    _document_requirement_roots,
    _plan_document_requirement_withdrawal,
)
from groundloop.postgres.m5 import read_m5_mismatch_counts, read_m5_oracle_states
from groundloop.postgres.migrations import (
    M5_ACCEPTED_DOCUMENT_UPDATE_COMPLETION_BUNDLE_SHA256,
    M5_ACCEPTED_DOCUMENT_UPDATE_COMPLETION_MIGRATION_SHA256,
    M5_DOCUMENT_UPDATE_COMPLETION_BUNDLE_ID,
    M5_DOCUMENT_UPDATE_COMPLETION_INSTALL_LOCK_RELATIONS,
    M5BundleHashConflictError,
    M5DocumentUpdateCompletionBundleError,
    M5PrerequisiteError,
    M5SemanticReadinessBundleError,
    _m5_readiness_catalog_fingerprint,
    _m5_readiness_verify_catalog,
    install_m5_document_update_completion_bundle,
    install_m5_preterminal_seal_context_bundle,
    install_m5_semantic_readiness_bundle,
    m5_document_update_completion_bundle_identity,
    require_m5_document_update_completion_bundle,
    require_m5_semantic_readiness_bundle,
)
from groundloop.repository import InMemoryRepository
from tests.m5.postgres.helpers import insert_observation, install_current_currency
from tests.m5.postgres_runtime.conftest import M5RuntimeDatabase
from tests.m5.postgres_runtime.d25_store_core.conftest import (
    authorize_and_derive_matching_transition,
    install_document_matching_foundation_retained_bytes,
)
from tests.m5.postgres_runtime.test_migration_017 import (
    _insert_d26_runtime_work_row,
    _open_b2_runtime_epoch,
)
from tests.m5.postgres_runtime.test_migration_019 import (
    FROZEN_MIGRATION_HASHES,
    _all_ledger_rows,
    _database_url,
    _pre019_schema,
    _RecordingConnection,
    _select_schema,
)
from tests.m5.postgres_runtime.test_migration_020 import (
    _catalog_images,
    _edge_values,
    _insert,
    _install_runtime_schema,
    _manual_edge,
    _manual_open,
)
from tests.m5.postgres_runtime.test_migration_020 import (
    test_deferred_validator_requires_real_result as _old_group_deferred_negative,
)
from tests.m5.postgres_runtime.test_migration_020 import (
    test_guard_does_not_read_shadow_temp_epoch as _old_group_shadow_boundary,
)
from tests.m5.postgres_runtime.test_migration_020 import (
    test_predecessor_rejects_invalid_images as _old_group_predecessor_negative,
)
from tests.m5.postgres_runtime.test_root_transitions import _acquire, _result

ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "migrations/021_m5_document_update_completion.sql"
MARKER = "-- groundloop:m5-document-completion-statement:"
FUNCTIONS = (
    "groundloop_m5_validate_semantic_readiness_predecessor",
    "groundloop_m5_validate_work_contribution",
    "groundloop_m5_validate_event_result_children",
)


@contextmanager
def _pre021_schema() -> Iterator[tuple[Connection[Any], str]]:
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        install_m5_semantic_readiness_bundle(connection)
        yield connection, schema


def test_installer_h33_identity_replay_readonly_and_exact_delta() -> None:
    with _pre021_schema() as (connection, schema):
        before = _catalog_images(connection, schema)
        ledger = _all_ledger_rows(connection)
        mode = connection.execute("SELECT * FROM groundloop_runtime_mode").fetchall()
        connection.commit()
        identity = m5_document_update_completion_bundle_identity()
        assert (
            identity.migration_sha256
            == (M5_ACCEPTED_DOCUMENT_UPDATE_COMPLETION_MIGRATION_SHA256)
            == "198646f9a4b2f965d9ca89df684eb4f4287f3753aeff1d255c848bcb0352681c"
        )
        assert (
            identity.bundle_sha256
            == (M5_ACCEPTED_DOCUMENT_UPDATE_COMPLETION_BUNDLE_SHA256)
            == "893baf9d50cbb84c27b356cbc329f97bc714ca8c280778ece920b5a4dc728eb5"
        )
        assert identity.bundle_id == M5_DOCUMENT_UPDATE_COMPLETION_BUNDLE_ID
        assert install_m5_document_update_completion_bundle(connection).applied
        after = _catalog_images(connection, schema)
        installed_ledger = _all_ledger_rows(connection)
        assert (
            tuple(row for row in installed_ledger if row[0] != identity.bundle_id)
            == ledger
        )
        assert len(installed_ledger) == len(ledger) + 1
        assert (
            connection.execute("SELECT * FROM groundloop_runtime_mode").fetchall()
            == mode
        )
        for category in before:
            if category != "functions":
                assert before[category] == after[category]
                continue
            assert set(before[category]) == set(after[category])
            changed = {
                key
                for key in before[category]
                if before[category][key] != after[category][key]
            }
            assert changed == {(name, "") for name in FUNCTIONS}
            for key in changed:
                old, new = list(before[category][key]), list(after[category][key])
                old[8] = new[8]  # ONLY prosrc may change.
                assert old == new
        connection.commit()
        recorded = _RecordingConnection(connection)
        assert not install_m5_document_update_completion_bundle(recorded).applied  # type: ignore[arg-type]
        assert not any(
            re.match(r"(?:LOCK|INSERT|UPDATE|DELETE|ALTER|CREATE|DROP)\b", q)
            for q in recorded.statements
        )
        assert _catalog_images(connection, schema) == after
        assert _all_ledger_rows(connection) == installed_ledger
        connection.commit()
        with connection.transaction():
            connection.execute("SET TRANSACTION READ ONLY")
            require_m5_document_update_completion_bundle(connection, schema_name=schema)
        with pytest.raises(M5SemanticReadinessBundleError, match="source catalog"):
            require_m5_semantic_readiness_bundle(connection, schema_name=schema)
        connection.rollback()
        points: list[str] = []
        with pytest.raises(M5BundleHashConflictError):
            install_m5_document_update_completion_bundle(
                connection,
                migration_bytes=MIGRATION.read_bytes() + b"-- changed\n",
                failure_injector=points.append,
            )
        assert points == []
        assert _all_ledger_rows(connection) == installed_ledger


def test_installer_every_cut_and_fixed_lock_order() -> None:
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
    with _pre021_schema() as (connection, schema):
        before, ledger = (
            _catalog_images(connection, schema),
            _all_ledger_rows(connection),
        )
        connection.commit()
        for index, selected in enumerate(points):
            observed: list[str] = []

            def fail(
                point: str, expected: str = selected, seen: list[str] = observed
            ) -> None:
                seen.append(point)
                if point == expected:
                    raise RuntimeError("021 injected " + point)

            with pytest.raises(RuntimeError, match=re.escape(selected)):
                install_m5_document_update_completion_bundle(
                    connection, failure_injector=fail
                )
            assert observed == list(points[: index + 1])
            assert connection.info.transaction_status == TransactionStatus.IDLE
            assert _catalog_images(connection, schema) == before
            assert _all_ledger_rows(connection) == ledger
            connection.commit()
        recorded = _RecordingConnection(connection)
        assert install_m5_document_update_completion_bundle(recorded).applied  # type: ignore[arg-type]
        assert [q for q in recorded.statements if q.startswith("LOCK TABLE")] == [
            f'LOCK TABLE "{schema}"."{name}" IN SHARE ROW EXCLUSIVE MODE'
            for name in M5_DOCUMENT_UPDATE_COMPLETION_INSTALL_LOCK_RELATIONS
        ]


def test_installer_wrong_bytes_connections_and_partial_raw_install() -> None:
    with _pre021_schema() as (connection, schema):
        before, ledger = (
            _catalog_images(connection, schema),
            _all_ledger_rows(connection),
        )
        connection.commit()
        observed: list[str] = []
        with pytest.raises(
            M5DocumentUpdateCompletionBundleError, match="H33 authority"
        ):
            install_m5_document_update_completion_bundle(
                connection,
                migration_bytes=b"-- wrong\n",
                failure_injector=observed.append,
            )
        assert observed == ["after_initial_ledger"]
        assert _catalog_images(connection, schema) == before
        assert _all_ledger_rows(connection) == ledger
        connection.commit()
        connection.execute("SELECT 1")
        with pytest.raises(M5DocumentUpdateCompletionBundleError, match="idle"):
            install_m5_document_update_completion_bundle(connection)
        connection.rollback()
        for level in (IsolationLevel.REPEATABLE_READ, IsolationLevel.SERIALIZABLE):
            connection.isolation_level = level
            with pytest.raises(
                M5DocumentUpdateCompletionBundleError, match="READ COMMITTED"
            ):
                install_m5_document_update_completion_bundle(connection)
        connection.isolation_level = None
        connection.read_only = True
        with pytest.raises(M5DocumentUpdateCompletionBundleError, match="read-write"):
            install_m5_document_update_completion_bundle(connection)
        connection.read_only = None
        # Partial or completely raw-applied bodies must NEVER be adopted.
        for _, statement in _statements():
            connection.execute(statement)
            connection.commit()
            raw = _catalog_images(connection, schema)
            connection.commit()
            with pytest.raises(M5SemanticReadinessBundleError, match="source catalog"):
                install_m5_document_update_completion_bundle(connection)
            assert _catalog_images(connection, schema) == raw
            assert _all_ledger_rows(connection) == ledger
            connection.commit()


def test_installer_all_five_fields_of_all_prerequisites_and_own_ledger() -> None:
    """Malformed ledger-only schemas are NEGATIVES, never installable authority."""
    with _pre021_schema() as (connection, schema):
        assert install_m5_document_update_completion_bundle(connection).applied
        rows = _all_ledger_rows(connection)
        assert len(rows) == 8
        connection.commit()
        bad_schema = "d33_bad_ledger_" + uuid.uuid4().hex
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
                                if selected == index:
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
                            errors = (
                                M5PrerequisiteError,
                                M5DocumentUpdateCompletionBundleError,
                                M5BundleHashConflictError,
                            )
                            # Missing021 id correctly follows first-install
                            # through valid prerequisites, then fails on this
                            # deliberately ledger-only schema's absent tables.
                            install_errors = errors + (
                                (psycopg.errors.UndefinedTable,)
                                if rows[selected][0]
                                == M5_DOCUMENT_UPDATE_COMPLETION_BUNDLE_ID
                                and field == 0
                                else ()
                            )
                            with pytest.raises(install_errors):
                                install_m5_document_update_completion_bundle(bad)
                            with pytest.raises(errors):
                                require_m5_document_update_completion_bundle(
                                    bad, schema_name=bad_schema
                                )
                            bad.rollback()
                            bad.execute("DELETE FROM groundloop_m5_schema_bundle")
                            bad.commit()
            finally:
                admin.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(bad_schema))
                )


@pytest.mark.parametrize("pause_at", ["after_initial_ledger", "after_install_lock"])
def test_installer_two_forced_race_orders(pause_at: str) -> None:
    with _pre021_schema() as (connection, schema):
        reached, resume, second_started = (
            threading.Event(),
            threading.Event(),
            threading.Event(),
        )
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
                                raise TimeoutError("021 installer resume timed out")
                        if not paused and point == "after_prerequisite":
                            second_started.set()

                    results.append(
                        install_m5_document_update_completion_bundle(
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
                with psycopg.connect(_database_url(), autocommit=True) as observer:
                    deadline = time.monotonic() + 10
                    waiting = None
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
                (M5DocumentUpdateCompletionBundleError, M5BundleHashConflictError)
            ):
                install_m5_document_update_completion_bundle(
                    connection, migration_bytes=b"-- conflict\n"
                )
        finally:
            resume.set()
            first.join(30)
            if second.ident is not None:
                second.join(30)
        assert not first.is_alive() and not second.is_alive()
        assert not errors
        assert sorted(results) == [False, True]
        assert not install_m5_document_update_completion_bundle(connection).applied


def _statements() -> tuple[tuple[str, str], ...]:
    return tuple(
        (part.split("\n", 1)[0], part.split("\n", 1)[1].strip())
        for part in MIGRATION.read_text().split(MARKER)[1:]
    )


def test_installer_caller_concat_and_like_shadows_cannot_run_or_hide_functions() -> (
    None
):
    with _pre021_schema() as (connection, schema):
        hostile = "d33_operator_shadow_" + uuid.uuid4().hex
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(hostile)))
        try:
            connection.execute(
                sql.SQL("CREATE TABLE {}.canary (operation text)").format(
                    sql.Identifier(hostile)
                )
            )
            connection.execute(
                sql.SQL(
                    "CREATE FUNCTION {}.concat(text,text) RETURNS text "
                    "LANGUAGE plpgsql AS $$"
                    "BEGIN INSERT INTO {}.canary VALUES ('concat'); "
                    "RETURN $1 OPERATOR(pg_catalog.||) $2; END$$"
                ).format(sql.Identifier(hostile), sql.Identifier(hostile))
            )
            connection.execute(
                sql.SQL(
                    "CREATE OPERATOR {}.|| "
                    "(FUNCTION={}.concat,LEFTARG=text,RIGHTARG=text)"
                ).format(sql.Identifier(hostile), sql.Identifier(hostile))
            )
            connection.execute(
                sql.SQL(
                    "CREATE FUNCTION {}.hidden_like(name,text) RETURNS boolean "
                    "LANGUAGE plpgsql AS $$BEGIN "
                    "INSERT INTO {}.canary VALUES ('like'); "
                    "RETURN $1::pg_catalog.text OPERATOR(pg_catalog.~~) $2 "
                    "AND $1 OPERATOR(pg_catalog.<>) 'groundloopxunapproved'; END$$"
                ).format(sql.Identifier(hostile), sql.Identifier(hostile))
            )
            connection.execute(
                sql.SQL(
                    "CREATE OPERATOR {}.~~ "
                    "(FUNCTION={}.hidden_like,LEFTARG=name,RIGHTARG=text)"
                ).format(sql.Identifier(hostile), sql.Identifier(hostile))
            )
            connection.execute(
                sql.SQL("SET search_path TO {},{},pg_catalog").format(
                    sql.Identifier(schema), sql.Identifier(hostile)
                )
            )
            assert connection.execute("SELECT 'a'::text||'b'::text").fetchone() == (
                "ab",
            )
            assert connection.execute(
                "SELECT 'groundloopxunapproved'::name LIKE 'groundloop_%'"
            ).fetchone() == (False,)
            assert connection.execute(
                sql.SQL("SELECT count(*) FROM {}.canary").format(
                    sql.Identifier(hostile)
                )
            ).fetchone() == (2,)
            connection.execute(
                sql.SQL("TRUNCATE {}.canary").format(sql.Identifier(hostile))
            )
            connection.commit()
            assert install_m5_document_update_completion_bundle(connection).applied
            connection.execute(
                "CREATE FUNCTION groundloopxunapproved() RETURNS integer "
                "LANGUAGE sql AS 'SELECT 1'"
            )
            connection.commit()
            with pytest.raises(
                M5DocumentUpdateCompletionBundleError,
                match="source catalog|structural catalog",
            ):
                install_m5_document_update_completion_bundle(connection)
            with pytest.raises(
                M5DocumentUpdateCompletionBundleError,
                match="source catalog|structural catalog",
            ):
                require_m5_document_update_completion_bundle(
                    connection, schema_name=schema
                )
            connection.rollback()
            assert connection.execute(
                sql.SQL("SELECT count(*) FROM {}.canary").format(
                    sql.Identifier(hostile)
                )
            ).fetchone() == (0,)
            connection.execute("DROP FUNCTION groundloopxunapproved()")
            connection.commit()
            assert not install_m5_document_update_completion_bundle(connection).applied
        finally:
            connection.rollback()
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(hostile))
            )
            connection.commit()


def test_installer_pre_and_post_catalog_tamper_matrix() -> None:
    """Intact catalog restoration between independent isolated negatives."""
    alterations = (
        (
            "ALTER FUNCTION groundloop_m5_validate_semantic_readiness_predecessor() "
            "SECURITY DEFINER",
            "ALTER FUNCTION groundloop_m5_validate_semantic_readiness_predecessor() "
            "SECURITY INVOKER",
        ),
        (
            "ALTER FUNCTION groundloop_m5_validate_semantic_readiness_predecessor() "
            "SET search_path TO public",
            "ALTER FUNCTION groundloop_m5_validate_semantic_readiness_predecessor() "
            "SET search_path FROM CURRENT",
        ),
        (
            "ALTER FUNCTION groundloop_m5_matching_private_temp_triplet"
            "(regclass,regclass,regclass) SECURITY INVOKER",
            "ALTER FUNCTION groundloop_m5_matching_private_temp_triplet"
            "(regclass,regclass,regclass) SECURITY DEFINER",
        ),
        (
            "ALTER FUNCTION groundloop_m5_binomial(integer,integer) SECURITY DEFINER",
            "ALTER FUNCTION groundloop_m5_binomial(integer,integer) SECURITY INVOKER",
        ),
        (
            "ALTER FUNCTION groundloop_m5_binomial(integer,integer) STABLE",
            "ALTER FUNCTION groundloop_m5_binomial(integer,integer) IMMUTABLE",
        ),
        (
            "ALTER FUNCTION groundloop_m5_assignment_preflight_bound(integer,integer) "
            "CALLED ON NULL INPUT",
            "ALTER FUNCTION groundloop_m5_assignment_preflight_bound(integer,integer) "
            "STRICT",
        ),
        (
            "ALTER FUNCTION groundloop_m5_group_certificate_valid_at"
            "(character,bigint,bigint) SET search_path TO public",
            "ALTER FUNCTION groundloop_m5_group_certificate_valid_at"
            "(character,bigint,bigint) RESET search_path",
        ),
        (
            "REVOKE EXECUTE ON FUNCTION groundloop_m5_claim_certificate_valid_at"
            "(character,bigint,bigint) FROM PUBLIC",
            "GRANT EXECUTE ON FUNCTION groundloop_m5_claim_certificate_valid_at"
            "(character,bigint,bigint) TO PUBLIC",
        ),
        (
            "ALTER FUNCTION groundloop_m5_group_certificate_bindings_at(bigint,bigint) "
            "PARALLEL SAFE",
            "ALTER FUNCTION groundloop_m5_group_certificate_bindings_at(bigint,bigint) "
            "PARALLEL UNSAFE",
        ),
        (
            "CREATE FUNCTION groundloop_unapproved() RETURNS integer "
            "LANGUAGE sql AS 'SELECT 1'",
            "DROP FUNCTION groundloop_unapproved()",
        ),
        (
            "ALTER TABLE groundloop_m5_runtime_epoch "
            "ALTER COLUMN revision DROP NOT NULL",
            "ALTER TABLE groundloop_m5_runtime_epoch "
            "ALTER COLUMN revision SET NOT NULL",
        ),
    )
    for installed in (False, True):
        with _pre021_schema() as (connection, schema):
            if installed:
                assert install_m5_document_update_completion_bundle(connection).applied
            # Match the source-pinned path for the predecessor's original SET.
            connection.execute(
                sql.SQL("SET search_path TO {},pg_catalog").format(
                    sql.Identifier(schema)
                )
            )
            connection.commit()
            ledger = _all_ledger_rows(connection)
            connection.commit()
            for alteration, undo in alterations:
                connection.execute(alteration)
                connection.commit()
                with pytest.raises(
                    (
                        M5DocumentUpdateCompletionBundleError,
                        M5SemanticReadinessBundleError,
                    )
                ):
                    install_m5_document_update_completion_bundle(connection)
                assert _all_ledger_rows(connection) == ledger
                connection.commit()
                if installed:
                    with pytest.raises(
                        (
                            M5DocumentUpdateCompletionBundleError,
                            M5SemanticReadinessBundleError,
                        )
                    ):
                        require_m5_document_update_completion_bundle(
                            connection, schema_name=schema
                        )
                    connection.rollback()
                connection.execute(undo)
                connection.commit()
            if installed:
                assert not install_m5_document_update_completion_bundle(
                    connection
                ).applied
            else:
                assert install_m5_document_update_completion_bundle(connection).applied


def test_installer_same_body_wrong_signature_including_oracle() -> None:
    for installed in (False, True):
        with _pre021_schema() as (connection, schema):
            if installed:
                assert install_m5_document_update_completion_bundle(connection).applied
            for name, signature in (
                ("groundloop_m5_matching_u64", "bytea,integer"),
                ("groundloop_m5_binomial", "integer,integer"),
            ):
                row = connection.execute(
                    "SELECT pg_catalog.pg_get_functiondef(p.oid) "
                    "FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n "
                    "ON n.oid=p.pronamespace WHERE n.nspname=%s AND p.proname=%s",
                    (schema, name),
                ).fetchone()
                assert row is not None
                original = str(row[0])
                altered = original.replace("RETURNS bigint", "RETURNS text")
                assert altered != original
                connection.execute(f"DROP FUNCTION {name}({signature})")
                connection.execute(altered)
                connection.commit()
                with pytest.raises(M5SemanticReadinessBundleError, match="signatures"):
                    install_m5_document_update_completion_bundle(connection)
                connection.execute(f"DROP FUNCTION {name}({signature})")
                connection.execute(original)
                connection.commit()
            assert install_m5_document_update_completion_bundle(connection).applied == (
                not installed
            )


def test_installer_predecessor_acl_negative_has_its_own_catalog() -> None:
    # REVOKE/GRANT does not restore PostgreSQL's original NULL proacl. Never
    # reuse a nonidentically restored catalog for an unrelated negative.
    for installed in (False, True):
        with _pre021_schema() as (connection, schema):
            if installed:
                install_m5_document_update_completion_bundle(connection)
            connection.execute(
                "REVOKE EXECUTE ON FUNCTION "
                "groundloop_m5_validate_semantic_readiness_predecessor() FROM PUBLIC"
            )
            connection.commit()
            with pytest.raises(M5SemanticReadinessBundleError, match="ACL|flags"):
                install_m5_document_update_completion_bundle(connection)
            if installed:
                with pytest.raises(M5SemanticReadinessBundleError, match="ACL|flags"):
                    require_m5_document_update_completion_bundle(
                        connection, schema_name=schema
                    )
                connection.rollback()


def test_installer_captured_schema_temp_and_fallback_rejection() -> None:
    with _pre021_schema() as (connection, schema):
        other = "d33_empty_" + uuid.uuid4().hex
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(other)))
        try:
            connection.execute(
                sql.SQL("SET search_path TO {},{},public").format(
                    sql.Identifier(other), sql.Identifier(schema)
                )
            )
            connection.commit()
            with pytest.raises(psycopg.errors.UndefinedTable):
                install_m5_document_update_completion_bundle(connection)
            connection.execute(
                "CREATE TEMP TABLE groundloop_m5_schema_bundle(fake integer)"
            )
            connection.execute(
                sql.SQL("SET search_path TO pg_temp,{},public").format(
                    sql.Identifier(schema)
                )
            )
            connection.commit()
            with pytest.raises(
                M5SemanticReadinessBundleError, match="trusted permanent"
            ):
                install_m5_document_update_completion_bundle(connection)
            _select_schema(connection, schema)
            assert install_m5_document_update_completion_bundle(connection).applied
            assert connection.execute(
                "SELECT count(*) FROM pg_temp.groundloop_m5_schema_bundle"
            ).fetchone() == (0,)
            with pytest.raises(psycopg.errors.UndefinedTable):
                require_m5_document_update_completion_bundle(
                    connection, schema_name=other
                )
            connection.rollback()
            with pytest.raises(
                M5DocumentUpdateCompletionBundleError, match="absent or temporary"
            ):
                require_m5_document_update_completion_bundle(
                    connection, schema_name="pg_temp"
                )
            connection.rollback()
        finally:
            connection.rollback()
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(other))
            )
            connection.commit()


def _raw_install(connection: Connection[Any], schema: str) -> None:
    """Controlled schema diagnostic; no production installer acceptance."""
    with connection.transaction():
        connection.execute(
            "SELECT pg_catalog.set_config('search_path',"
            "pg_catalog.quote_ident(%s)||', pg_catalog',true)",
            (schema,),
        )
        for _, statement in _statements():
            connection.execute(statement)


@pytest.fixture
def raw_database() -> Iterator[tuple[Connection[Any], str]]:
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        install_m5_semantic_readiness_bundle(connection)
        _raw_install(connection, schema)
        yield connection, schema


def test_all_000_through_020_source_pins() -> None:
    pins = {
        **FROZEN_MIGRATION_HASHES,
        "migrations/000_extensions.sql": (
            "14230448e66ee34c4f05847cdc777b8eea14d7ee24d58dac8dac22d5c1317774"
        ),
        "migrations/019_m5_preterminal_seal_context.sql": (
            "f92c02a365ac43a26f3291718866436b19f69924eb7a8c2b77af0312f82b536e"
        ),
        "migrations/020_m5_semantic_readiness.sql": (
            "df0a3c0c9b228a4a22903479896326d27fbd6f98f5878e34d73182ba007bf837"
        ),
    }
    assert len(pins) == 21
    for label, digest in pins.items():
        assert hashlib.sha256((ROOT / label).read_bytes()).hexdigest() == digest


def test_exact_three_statement_inventory() -> None:
    statements = _statements()
    assert tuple(name for name, _ in statements) == (
        "predecessor_function",
        "work_validator",
        "result_children",
    )
    assert (
        tuple(
            re.match(r"CREATE OR REPLACE FUNCTION (\w+)\(\)", statement)[1]
            for _, statement in statements
        )
        == FUNCTIONS
    )
    assert not re.search(
        r"^(ALTER|DROP|GRANT|REVOKE|INSERT|UPDATE|DELETE|"
        r"CREATE (?!OR REPLACE FUNCTION))",
        MIGRATION.read_text(),
        re.M,
    )


def test_raw_catalog_delta_is_only_three_bodies() -> None:
    with _pre019_schema() as (connection, schema):
        install_m5_preterminal_seal_context_bundle(connection)
        install_m5_semantic_readiness_bundle(connection)
        before = _catalog_images(connection, schema)
        ledgers = _all_ledger_rows(connection)
        structural = _m5_readiness_catalog_fingerprint(connection, schema_name=schema)
        connection.commit()
        _raw_install(connection, schema)
        after = _catalog_images(connection, schema)
        assert _all_ledger_rows(connection) == ledgers
        assert (
            _m5_readiness_catalog_fingerprint(connection, schema_name=schema)
            == structural
        )
        for category in before:
            if category != "functions":
                assert after[category] == before[category]
                continue
            assert set(after[category]) == set(before[category])
            changed = {
                key
                for key in before[category]
                if before[category][key] != after[category][key]
            }
            assert changed == {(name, "") for name in FUNCTIONS}
            for key in changed:
                old = list(before[category][key])
                new = list(after[category][key])
                old[8] = new[8]  # prosrc only; flags/owner/ACL/config unchanged.
                assert old == new
        with pytest.raises(M5SemanticReadinessBundleError, match="source catalog"):
            _m5_readiness_verify_catalog(connection, schema_name=schema, installed=True)
        connection.rollback()


def test_old_group_edges_still_pass_raw_021(
    raw_database: tuple[Connection[Any], str],
) -> None:
    connection, _ = raw_database
    epoch, _, _ = _manual_open(connection)
    _manual_edge(connection, epoch, "semantic_pending", 1)
    _manual_edge(connection, epoch, "semantic_complete", 2)
    assert connection.execute(
        "SELECT runtime_state,revision FROM groundloop_m5_runtime_epoch "
        "WHERE epoch_id=%s",
        (epoch,),
    ).fetchone() == ("semantic_complete", 3)


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
def test_old_group_predecessor_negatives_on_raw_021(
    raw_database: tuple[Connection[Any], str], mutation: str
) -> None:
    """Run accepted020 assertions on the owned fixture AFTER raw021."""
    _old_group_predecessor_negative(raw_database, mutation)


def test_old_group_shadow_boundary_on_raw_021(
    raw_database: tuple[Connection[Any], str],
) -> None:
    _old_group_shadow_boundary(raw_database)


def test_old_group_deferred_result_negative_on_raw_021(
    raw_database: tuple[Connection[Any], str],
) -> None:
    _old_group_deferred_negative(raw_database)


def _seed_negative_direct_authority(
    database: M5RuntimeDatabase, policy: CandidatePolicyManifest, fault: str
) -> None:
    """Initial malformed legacy rows ONLY for negatives; never disable a guard.

    The genuine producers reject malformed inputs themselves. These retained
    SQL source negatives model legacy corruption BEFORE immutable insertion,
    so they can test021's independent source validation, not an UPDATE bypass.
    """
    connection = database.connection
    claims = tuple(sorted(database.base.claim_ids))
    registry_hash = stable_m4_digest("m4-claim-registry-snapshot-v1", *claims)
    connection.execute(
        "INSERT INTO groundloop_m4_claim_registry_snapshot "
        "(claim_registry_snapshot_id,claim_count,claim_set_hash) VALUES (%s,%s,%s)",
        (
            policy.claim_registry_snapshot_id,
            len(claims) + int(fault == "registry_count"),
            "f" * 64 if fault == "registry_hash" else registry_hash,
        ),
    )
    for ordinal, claim in enumerate(claims):
        connection.execute(
            "INSERT INTO groundloop_m4_claim_registry_member "
            "(claim_registry_snapshot_id,claim_id,member_ordinal) VALUES (%s,%s,%s)",
            (
                policy.claim_registry_snapshot_id,
                claim,
                ordinal + int(fault == "registry_ordinal"),
            ),
        )
    payload = _policy_payload(policy)
    if fault == "policy_json_extra":
        payload["unapproved"] = True
    elif fault == "policy_json_hash":
        payload["policy_hash"] = "f" * 64
    elif fault == "policy_rehashed_wrong_hash":
        payload["policy_hash"] = "f" * 64
    elif fault == "policy_version_manifest":
        payload["decision_policy_version"] = "different-policy-version"
    elif fault == "policy_spec":
        payload["verifier_execution_spec_hash"] = "f" * 64
    columns = (
        "candidate_policy_id",
        "policy_hash",
        "embedding_model_artifact_id",
        "decision_policy_version",
        "claim_role_template_hash",
        "chunk_role_template_hash",
        "vector_method_version",
        "vector_index_kind",
        "vector_index_build_config_hash",
        "vector_search_config_hash",
        "lexical_method_version",
        "lexical_config_hash",
        "lexical_postgres_version",
        "lexical_regconfig_identity",
        "claim_registry_snapshot_id",
        "claim_count",
        "fusion_version",
        "approximate_cap_per_inserted_chunk",
        "frontier_depth",
        "manifest",
    )
    connection.execute(
        sql.SQL("INSERT INTO groundloop_candidate_policy ({}) VALUES ({})").format(
            sql.SQL(",").join(map(sql.Identifier, columns)),
            sql.SQL(",").join(sql.Placeholder() for _ in columns),
        ),
        (
            policy.policy_id,
            "f" * 64 if fault == "policy_rehashed_wrong_hash" else policy.policy_hash,
            policy.embedding_model_artifact_id,
            policy.decision_policy_version,
            policy.claim_role_template_hash,
            policy.chunk_role_template_hash,
            policy.vector_method_version,
            policy.vector_index_kind.value,
            policy.vector_index_build_config_hash,
            policy.vector_search_config_hash,
            policy.lexical_method_version,
            policy.lexical_config_hash,
            policy.lexical_postgres_version,
            policy.lexical_regconfig_identity,
            policy.claim_registry_snapshot_id,
            policy.claim_count,
            policy.fusion_version,
            policy.approximate_cap_per_inserted_chunk,
            policy.frontier_depth,
            Jsonb(payload),
        ),
    )


def _manual_document_delete(
    database: M5RuntimeDatabase,
    *,
    fault: str | None = None,
    defer_open_commit: bool = False,
    replacement_fault: str | None = None,
    direct_authority_fault: str | None = None,
    root_capture: list[Any] | None = None,
    direct_dependency: str | None = None,
) -> tuple[int, str, str]:
    """Controlled SQL header/source fixture, NOT a production document opener.

    Uses genuine immutable policy/manifest recipes and the real M4 declaration
    producer. Only the SQL boundaries are under test; C1 is not imported.
    """
    connection = database.connection
    manifest = database.manifest
    predecessor_row = connection.execute(
        "SELECT epoch_id FROM groundloop_m5_publication_head WHERE singleton"
    ).fetchone()
    assert predecessor_row is not None
    predecessor_epoch = int(predecessor_row[0])
    direct_policy = CandidatePolicyManifest.build(
        policy_id=manifest.candidate_policy_id,
        embedding_model_artifact_id=manifest.embedding_model_artifact_id,
        claim_role_template_hash=manifest.requirement_role_template_hash,
        chunk_role_template_hash=manifest.chunk_role_template_hash,
        vector_method_version=manifest.vector_method_version,
        vector_index_kind=manifest.vector_index_kind,
        vector_index_build_config_hash=manifest.vector_index_build_config_hash,
        vector_search_config_hash=manifest.vector_search_config_hash,
        lexical_method_version=manifest.lexical_method_version,
        lexical_config_hash=manifest.lexical_config_hash,
        lexical_postgres_version=manifest.lexical_postgres_version,
        lexical_regconfig_identity=manifest.lexical_regconfig_identity,
        claim_registry_snapshot_id="d33-schema-registry",
        claim_count=len(database.base.claim_ids),
        fusion_version=manifest.fusion_version,
        approximate_cap_per_inserted_chunk=manifest.reverse_budget_per_inserted_chunk,
        frontier_depth=1,
        verifier_execution_spec_hash=manifest.verifier_execution_spec_hash,
        decision_policy_version=manifest.decision_policy_version,
        lineage_safety_override=manifest.lineage_safety_override,
    )
    direct_store = PostgresM4RuntimeStore(connection)
    if direct_authority_fault is None:
        direct_store.register_claim_registry_snapshot(
            direct_policy.claim_registry_snapshot_id,
            tuple(sorted(database.base.claim_ids)),
        )
        direct_store.register_candidate_policy(direct_policy)
    else:
        _seed_negative_direct_authority(database, direct_policy, direct_authority_fault)
    version = connection.execute(
        "SELECT document_version_id FROM groundloop_document_version "
        'WHERE valid_to_epoch IS NULL ORDER BY document_version_id COLLATE "C"'
    ).fetchone()
    assert version is not None
    source_version = str(version[0])
    source_chunks = tuple(
        str(row[0])
        for row in connection.execute(
            "SELECT chunk_version_id FROM groundloop_chunk_version "
            "WHERE document_version_id=%s ORDER BY chunk_index",
            (source_version,),
        ).fetchall()
    )
    if direct_dependency == "published_claim_currency":
        observation_id = "d33-negative-direct-currency"
        insert_observation(
            connection,
            observation_id=observation_id,
            subject_kind="claim",
            subject_id=database.base.claim_ids[0],
            chunk_id=source_chunks[0],
            produced_epoch=predecessor_epoch,
            task_type="verify_claim_v1",
            scores=(0.05, 0.05, 0.9),
        )
        install_current_currency(
            connection,
            observation_id=observation_id,
            subject_kind="claim",
            subject_id=database.base.claim_ids[0],
            chunk_id=source_chunks[0],
            task_type="verify_claim_v1",
            epoch_id=predecessor_epoch,
        )
    elif direct_dependency in ("frontier_missing_creator", "frontier_wrong_policy"):
        frontier_policy = direct_policy
        if direct_dependency == "frontier_wrong_policy":
            frontier_policy = CandidatePolicyManifest.build(
                **{
                    field.name: getattr(direct_policy, field.name)
                    for field in fields(direct_policy)
                    if field.name not in ("policy_id", "policy_hash", "frontier_depth")
                },
                policy_id="d33-other-frontier-policy",
                frontier_depth=direct_policy.frontier_depth + 1,
            )
            direct_store.register_candidate_policy(frontier_policy)
        # Retained legacy source NEGATIVE, not a claimed valid producer/history.
        # Real existing sealed P has no matching M4 creator; do not invent one.
        connection.execute(
            "INSERT INTO groundloop_candidate_frontier VALUES "
            "(%s,%s,%s,'unverified',1,0.5,%s,%s,NULL)",
            (
                database.base.claim_ids[0],
                source_chunks[0],
                frontier_policy.policy_id,
                hashlib.sha256(b"d33-frontier").hexdigest(),
                predecessor_epoch,
            ),
        )
    prefix = f"d33-schema-delete-更新-P{predecessor_epoch}"
    event: DeleteDocumentVersionEvent | ReplaceDocumentVersionEvent = (
        DeleteDocumentVersionEvent(f"{prefix}-event", source_version)
    )
    declaration: dict[str, Any] = {
        "m5_d29_document_declaration_v1": {
            "direct_root_job_ids": [],
            "direct_scope_root_job_ids": [],
            "direct_fallback_claim_ids": [],
        }
    }
    if fault in declaration["m5_d29_document_declaration_v1"]:
        declaration["m5_d29_document_declaration_v1"][fault] = [
            "retained-nonempty-root"
        ]
    if fault == "missing_declaration_key":
        del declaration["m5_d29_document_declaration_v1"]["direct_fallback_claim_ids"]
    if fault == "null_declaration_array":
        declaration["m5_d29_document_declaration_v1"]["direct_root_job_ids"] = None
    if fault == "nontext_declaration_id":
        declaration["m5_d29_document_declaration_v1"]["direct_root_job_ids"] = [1]
    if fault == "duplicate_declaration_id":
        declaration["m5_d29_document_declaration_v1"]["direct_root_job_ids"] = [
            "duplicate",
            "duplicate",
        ]
    payload = StructuralPayload(deactivated_document_version_id=source_version).manifest
    if replacement_fault is not None:
        owner = connection.execute(
            "SELECT d.document_id,d.source_uri,d.authority_class "
            "FROM groundloop_document d JOIN groundloop_document_version v "
            "USING(document_id) WHERE v.document_version_id=%s",
            (source_version,),
        ).fetchone()
        assert owner is not None
        new_version, new_chunk = f"{prefix}-new-version", f"{prefix}-new-chunk"
        new_hash = hashlib.sha256(b"replacement").hexdigest()
        event = ReplaceDocumentVersionEvent(
            event.event_id,
            str(owner[0]),
            source_version,
            new_version,
            new_hash,
            (ChunkInput(new_chunk, 0, "replacement"),),
        )
        payload = StructuralPayload(
            InsertedDocument(
                DocumentVersion(new_version, str(owner[0]), new_hash),
                (ChunkVersion(new_chunk, new_version, 0, "replacement"),),
                source_uri=owner[1],
                authority_class=str(owner[2]),
                chunker_version="fixture-v1",
            ),
            source_version,
        ).manifest
        if replacement_fault == "bad_inserted_hash":
            payload["inserted_content_hash"] = "not-a-sha256"
        elif replacement_fault == "blank_inserted_version":
            payload["inserted_document_version_id"] = "\x1c"
            payload["inserted_chunks"][0]["document_version_id"] = "\x1c"
        elif replacement_fault == "blank_inserted_chunk":
            payload["inserted_chunk_ids"] = ["\x1c"]
            payload["inserted_chunks"][0]["chunk_version_id"] = "\x1c"
        elif replacement_fault == "unpaired_provenance":
            payload["chunker_artifact_id"] = "unexpected-artifact"
        elif replacement_fault == "changed_metadata":
            payload["source_uri"] = "test://changed-更新"
        elif replacement_fault == "blank_chunker":
            payload["chunker_version"] = "\x1c"
    if fault == "wrong_manifest_source":
        payload["deactivated_document_version_id"] = "another-version"
    if fault == "extra_payload_key":
        payload["unexpected"] = None
    if fault == "missing_payload_uri":
        del payload["source_uri"]
    direct_manifest = {
        "_groundloop_m4_runtime_v1": {
            "event_manifest": payload,
            "scope_claim_ids": {},
            "failure_reason": None,
        }
    }
    if fault == "extra_manifest_key":
        direct_manifest["extra"] = {}
    if fault == "nonempty_scope_manifest":
        direct_manifest["_groundloop_m4_runtime_v1"]["scope_claim_ids"] = {
            "invented-scope": []
        }

    roots: tuple[Any, ...] = ()
    typed_plan = None
    root_hash = runtime_digests.requirement_root_set_digest(())
    if root_capture is not None:
        assert replacement_fault is None and fault is None
        snapshot = ActiveChunkSnapshot.build(
            tuple(
                ActiveChunkSnapshotEntry.build(
                    chunk_version_id=str(row[0]), chunk_text=str(row[1])
                )
                for row in connection.execute(
                    "SELECT chunk_version_id,text FROM groundloop_chunk_version "
                    "WHERE valid_to_epoch IS NULL AND document_version_id<>%s "
                    'ORDER BY chunk_version_id COLLATE "C"',
                    (source_version,),
                ).fetchall()
            )
        )
        typed_plan = M5TypedEventPlan(
            event.event_id,
            event,
            legacy_event_payload_digest(event),
            DynamicEventPlan(
                update=CorpusUpdateIdentity(
                    event.event_id,
                    legacy_event_payload_digest(event),
                    UpdateKind.DELETE,
                    predecessor_epoch,
                    manifest.candidate_policy_id,
                ),
                inserted_chunk_version_ids=(),
                deactivated_chunk_version_ids=tuple(sorted(source_chunks)),
                registered_claim_ids=tuple(sorted(database.base.claim_ids)),
                claim_registry_snapshot_id=direct_policy.claim_registry_snapshot_id,
            ),
            manifest.candidate_policy_id,
            manifest.manifest_hash,
            runtime_fixtures._requirement_snapshot((database.group,)),
            snapshot,
            predecessor_epoch,
        )
        with connection.cursor() as cursor:
            withdrawal = _plan_document_requirement_withdrawal(cursor, typed_plan)
        supplied = _document_requirement_roots(typed_plan, manifest, withdrawal)
        assert withdrawal.fallback_keys and supplied
        root_hash = runtime_digests.requirement_root_set_digest(
            root.job.logical_job_id for root in supplied
        )
        roots, root_hash = _root_declarations_for_open(
            typed_plan, manifest, supplied, root_hash
        )
        root_capture.extend(roots)

    class SchemaSeedConnection:
        """Adapt only initial schema-test INSERT values, never existing rows."""

        def execute(self, query: Any, parameters: Any = None) -> Any:
            if (
                isinstance(query, str)
                and "INSERT INTO groundloop_m5_runtime_epoch" in query
                and roots
            ):
                assert typed_plan is not None
                parameters = (
                    *parameters[:4],
                    typed_plan.requirement_registry_snapshot.requirement_registry_snapshot_digest,
                    typed_plan.active_chunk_snapshot.active_chunk_snapshot_digest,
                    parameters[6],
                    root_hash,
                )
                query = query.replace("1,0,0,0)", f"1,{len(roots)},{len(roots)},0)")
            if (
                isinstance(query, str)
                and "INSERT INTO groundloop_m5_update VALUES" in query
            ):
                query = query.replace(",'{}'::jsonb,DEFAULT)", ",%s,DEFAULT)")
                parameters = (*parameters, Jsonb(declaration))
            if isinstance(query, str) and "INSERT INTO groundloop_m4_update" in query:
                query = query.replace(",%s,'{}')", ",%s,%s)")
                parameters = (*parameters, Jsonb(direct_manifest))
            return connection.execute(query, parameters)

    epoch, event_id, payload_hash = _open_b2_runtime_epoch(
        SchemaSeedConnection(),
        prefix,
        predecessor_epoch,
        manifest.decision_policy_version,
        direct_bridge=True,
        update_kind="document_replace"
        if replacement_fault is not None
        else "document_delete",
        payload_override=legacy_event_payload_digest(event),
    )
    connection.execute(
        "INSERT INTO groundloop_m4_structural_deactivation VALUES (%s,%s)",
        (epoch, source_version),
    )
    connection.execute(
        "UPDATE groundloop_document_version SET valid_to_epoch=%s "
        "WHERE document_version_id=%s",
        (epoch, source_version),
    )
    connection.execute(
        "UPDATE groundloop_chunk_version SET valid_to_epoch=%s "
        "WHERE document_version_id=%s",
        (epoch, source_version),
    )
    if replacement_fault is not None:
        connection.execute(
            "INSERT INTO groundloop_document_version VALUES (%s,%s,%s,%s,NULL)",
            (
                payload["inserted_document_version_id"],
                payload["inserted_document_id"],
                payload["inserted_content_hash"],
                epoch,
            ),
        )
        for chunk in payload["inserted_chunks"]:
            connection.execute(
                "INSERT INTO groundloop_chunk_version "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,NULL)",
                (
                    chunk["chunk_version_id"],
                    chunk["document_version_id"],
                    chunk["chunk_index"],
                    chunk["text"],
                    chunk["text_hash"],
                    payload["chunker_version"],
                    epoch,
                ),
            )
        connection.execute(
            "INSERT INTO groundloop_m4_document_metadata_overlay VALUES (%s,%s,%s,%s)",
            (
                epoch,
                payload["inserted_document_id"],
                payload["source_uri"],
                payload["authority_class"],
            ),
        )
    chunker = (
        "\x1c"
        if fault == "strip_empty_chunker_artifact"
        else f"d33-schema-chunker-P{predecessor_epoch}"
    )
    chunker_version_row = connection.execute(
        "SELECT chunker_version FROM groundloop_chunk_version "
        "WHERE chunk_version_id=%s",
        (source_chunks[0],),
    ).fetchone()
    assert chunker_version_row is not None
    connection.execute(
        "INSERT INTO groundloop_chunker_artifact VALUES (%s,%s,'v1',%s,DEFAULT)",
        (
            chunker,
            str(chunker_version_row[0]),
            hashlib.sha256(chunker.encode()).hexdigest(),
        ),
    )
    for chunk in source_chunks:
        connection.execute(
            "INSERT INTO groundloop_chunk_provenance VALUES (%s,%s,%s)",
            (
                chunk,
                chunker,
                "x" * 64
                if fault == "malformed_provenance_hash"
                else hashlib.sha256(b"d33-schema-chunker-input").hexdigest(),
            ),
        )
    PostgresEvaluationOverlayStore(connection).declare_epoch_local(
        connection,
        epoch,
        revision=0 if fault == "counter_zero" else 1,
        confirmed_as_of_epoch=None
        if fault == "counter_null_predecessor"
        else predecessor_epoch,
        open_discovery_scope_count=0,
    )
    with connection.cursor() as cursor:
        if roots:
            assert typed_plan is not None
            _validate_effective_snapshots(
                cursor, plan=typed_plan, epoch_id=epoch, direct_open=True
            )
            _persist_requirement_snapshot(
                cursor,
                snapshot=typed_plan.requirement_registry_snapshot,
                epoch_id=epoch,
            )
            _persist_active_chunk_snapshot(
                cursor, snapshot=typed_plan.active_chunk_snapshot, epoch_id=epoch
            )
            _persist_root_declarations(
                cursor, structural_event_id=event_id, epoch_id=epoch, roots=roots
            )
            _persist_initial_pending_counters(
                cursor,
                snapshot=typed_plan.requirement_registry_snapshot,
                epoch_id=epoch,
                roots=roots,
            )
            persist_structural_open_identity(
                cursor,
                epoch_id=epoch,
                config=database.operational_config,
                root_fallback_required={
                    root.job.logical_job_id: True for root in roots
                },
            )
        persist_structural_open_accounting(
            cursor,
            epoch_id=epoch,
            structural_event_id=event_id,
            payload_hash=payload_hash,
            structural_work=M5RuntimeWork(),
        )
    if not defer_open_commit:
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()
    return epoch, event_id, payload_hash


@pytest.fixture
def malformed_old_source_database(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> Iterator[M5RuntimeDatabase]:
    """Malformed legacy source is seeded before activation; guards stay intact."""
    original = runtime_fixtures.install_test_activation_barrier

    def activate(connection: Connection[Any], base: Any, **kwargs: Any) -> None:
        if request.param in ("bad_content_hash", "uppercase_content_hash"):
            connection.execute(
                "UPDATE groundloop_document_version SET content_hash=%s",
                ("not-a-sha256" if request.param == "bad_content_hash" else "A" * 64,),
            )
        elif request.param == "strip_empty_chunker_version":
            connection.execute(
                "UPDATE groundloop_chunk_version SET chunker_version=%s", ("\x1c",)
            )
        original(connection, base, **kwargs)

    monkeypatch.setattr(runtime_fixtures, "install_test_activation_barrier", activate)
    with contextmanager(runtime_fixtures.m5_runtime_db.__wrapped__)() as database:
        _install_runtime_schema(database)
        _raw_install(database.connection, database.schema_name)
        try:
            yield database
        finally:
            database.connection.rollback()


@pytest.mark.parametrize(
    "malformed_old_source_database",
    ("bad_content_hash", "uppercase_content_hash", "strip_empty_chunker_version"),
    indirect=True,
)
def test_predecessor_rejects_preactivation_malformed_old_source(
    malformed_old_source_database: M5RuntimeDatabase,
) -> None:
    database = malformed_old_source_database
    epoch, _, _ = _manual_document_delete(database)
    work, anchor, source = _edge_values(
        database.connection, epoch, "semantic_pending", 1
    )
    with pytest.raises(psycopg.Error, match="withdrawn version/chunk/provenance"):
        _insert(database.connection, work, anchor, source)
    database.connection.rollback()


@pytest.mark.parametrize(
    "fault", ("malformed_provenance_hash", "strip_empty_chunker_artifact")
)
def test_predecessor_rejects_malformed_retained_provenance(
    raw_document_database: M5RuntimeDatabase, fault: str
) -> None:
    database = raw_document_database
    epoch, _, _ = _manual_document_delete(database, fault=fault)
    work, anchor, source = _edge_values(
        database.connection, epoch, "semantic_pending", 1
    )
    with pytest.raises(psycopg.Error, match="withdrawn version/chunk/provenance"):
        _insert(database.connection, work, anchor, source)
    database.connection.rollback()


@pytest.fixture
def raw_document_database(
    m5_runtime_db: M5RuntimeDatabase,
) -> Iterator[M5RuntimeDatabase]:
    _install_runtime_schema(m5_runtime_db)
    # _install_runtime_schema deliberately performs raw020, not ledger acceptance.
    _raw_install(m5_runtime_db.connection, m5_runtime_db.schema_name)
    try:
        yield m5_runtime_db
    finally:
        m5_runtime_db.connection.rollback()


def test_raw_job_free_delete_both_edges_keep_m4_counter_unchanged(
    raw_document_database: M5RuntimeDatabase,
) -> None:
    connection = raw_document_database.connection
    epoch, _, _ = _manual_document_delete(raw_document_database)
    before = connection.execute(
        "SELECT *,xmin::text FROM groundloop_m4_evaluation_epoch_counter "
        "WHERE epoch_id=%s",
        (epoch,),
    ).fetchone()
    connection.commit()
    _manual_edge(connection, epoch, "semantic_pending", 1)
    _manual_edge(connection, epoch, "semantic_complete", 2)
    assert (
        connection.execute(
            "SELECT *,xmin::text FROM groundloop_m4_evaluation_epoch_counter "
            "WHERE epoch_id=%s",
            (epoch,),
        ).fetchone()
        == before
    )


def _controlled_guarded_matching_publication(
    connection: Connection[Any], epoch: int, revision: int
) -> None:
    """Schema fixture only: one guarded mutation per key, never an upsert.

    The production helper's two-journal-operation failure is retained in a
    separate diagnostic. This is NOT its repair or production seal evidence.
    No guard, journal, authorization or constraint is disabled or rewritten.
    """
    connection.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s,%s)",
        (epoch, revision - 1),
    )
    connection.execute(
        "SELECT groundloop_m5_authorize_persisted_matching_seal(%s,%s,%s)",
        (epoch, revision - 1, revision),
    )
    assert (
        connection.execute(
            "UPDATE groundloop_m5_matching_image_current c "
            "SET decision_policy_version=w.decision_policy_version,"
            "installed_epoch_id=%s,installed_revision=%s "
            "FROM groundloop_m5_matching_image_working w "
            "WHERE c.singleton AND w.epoch_id=%s",
            (epoch, revision, epoch),
        ).rowcount
        == 1
    )
    families = (
        ("observation", ("observation_id",), "w.present"),
        ("edge", ("requirement_version_id", "text_hash"), "w.refcount>0"),
        ("hash_mask", ("group_version_id", "text_hash"), "w.mask>0"),
        ("hall", ("group_version_id",), "w.present"),
    )
    for family, keys, positive in families:
        current = f"groundloop_m5_matching_{family}_current"
        working = f"groundloop_m5_matching_{family}_working"
        columns = tuple(
            str(row[0])
            for row in connection.execute(
                "SELECT attname FROM pg_catalog.pg_attribute "
                "WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped "
                "AND attname NOT IN ('installed_epoch_id','installed_revision') "
                "ORDER BY attnum",
                (current,),
            ).fetchall()
        )
        equality = sql.SQL(" AND ").join(
            sql.SQL("c.{k}=w.{k}").format(k=sql.Identifier(key)) for key in keys
        )
        connection.execute(
            sql.SQL(
                "DELETE FROM {c} c USING {w} w "
                "WHERE w.epoch_id=%s AND NOT ({positive}) AND {equality}"
            ).format(
                c=sql.Identifier(current),
                w=sql.Identifier(working),
                positive=sql.SQL(positive),
                equality=equality,
            ),
            (epoch,),
        )
        assignments = sql.SQL(",").join(
            sql.SQL("{c}=w.{c}").format(c=sql.Identifier(column))
            for column in columns
            if column not in keys
        )
        connection.execute(
            sql.SQL(
                "UPDATE {c} c SET {assignments},installed_epoch_id=%s,"
                "installed_revision=%s FROM {w} w "
                "WHERE w.epoch_id=%s AND ({positive}) AND {equality}"
            ).format(
                c=sql.Identifier(current),
                w=sql.Identifier(working),
                assignments=assignments,
                positive=sql.SQL(positive),
                equality=equality,
            ),
            (epoch, revision, epoch),
        )
        connection.execute(
            sql.SQL(
                "INSERT INTO {c} ({columns},installed_epoch_id,installed_revision) "
                "SELECT {values},%s,%s FROM {w} w WHERE w.epoch_id=%s "
                "AND ({positive}) AND NOT EXISTS "
                "(SELECT 1 FROM {c} c WHERE {equality})"
            ).format(
                c=sql.Identifier(current),
                w=sql.Identifier(working),
                columns=sql.SQL(",").join(map(sql.Identifier, columns)),
                values=sql.SQL(",").join(
                    sql.SQL("w.{}").format(sql.Identifier(column)) for column in columns
                ),
                positive=sql.SQL(positive),
                equality=equality,
            ),
            (epoch, revision, epoch),
        )


def _controlled_document_seal(
    database: M5RuntimeDatabase,
    epoch: int,
    event: str,
    payload: str,
    *,
    reference_fault: str | None = None,
    production_promotion_diagnostic: bool = False,
    before_checks: Callable[[Connection[Any]], None] | None = None,
    binding_fault: str | None = None,
    result_fault: str | None = None,
    expected_revision: int = 3,
    validator_only_probe: bool = False,
    absent_group_certificate: bool = True,
    reference_keys: tuple[tuple[str, str], ...] | None = None,
    status_deltas: tuple[StatusDelta, ...] | None = None,
) -> None:
    """SQL boundary fixture, NOT the future J33 adapter or C1 coordinator.

    Genuine D25 structural patch, controlled guarded publication, M4 SEAL.
    SQL-authored publication/accounting is never latency/work/runtime evidence.
    """
    connection = database.connection
    revision = expected_revision + 1
    with connection.cursor() as cursor:
        start = start_event_accounting(
            cursor, epoch_id=epoch, expected_revision=expected_revision
        )
        if production_promotion_diagnostic:
            promote_matching_overlay(
                cursor,
                epoch_id=epoch,
                expected_revision=expected_revision,
                sealed_revision=revision,
            )
        else:
            _controlled_guarded_matching_publication(connection, epoch, revision)
        journal = cursor.execute(
            "SELECT first_operation,last_operation,mutation_count,saw_insert,"
            "saw_update FROM pg_temp.groundloop_m5_matching_promotion_journal "
            "WHERE relation_name='groundloop_m5_matching_hall_current'"
        ).fetchall()
        expected_journal = (
            ("INSERT", "UPDATE", 2, True, True)
            if production_promotion_diagnostic
            else ("UPDATE", "UPDATE", 1, False, True)
        )
        assert journal == [expected_journal], journal
    chunks = list(database.base.chunk_ids)
    connection.execute(
        "UPDATE groundloop_published_observation_currency SET valid_to_epoch=%s "
        "WHERE chunk_version_id=ANY(%s) AND valid_to_epoch IS NULL",
        (epoch, chunks),
    )
    connection.execute(
        "DELETE FROM groundloop_observation_currency WHERE chunk_version_id=ANY(%s)",
        (chunks,),
    )
    families = (
        ("requirement", "requirement_version_id"),
        ("group", "group_version_id"),
        ("claim", "claim_id"),
        ("answer", "answer_version_id"),
    )
    for family, key in families:
        if reference_fault == "omit_publication_and_absence" and family == "group":
            continue
        published = f"groundloop_m5_published_{family}_state"
        working = f"groundloop_m5_working_{family}_state"
        materialized = f"groundloop_m5_{family}_state_materialized"
        columns = tuple(
            str(row[0])
            for row in connection.execute(
                "SELECT attname FROM pg_catalog.pg_attribute "
                "WHERE attrelid=%s::regclass "
                "AND attnum>0 AND NOT attisdropped AND attname NOT IN "
                "('valid_from_epoch','valid_to_epoch','sealed_revision') "
                "ORDER BY attnum",
                (published,),
            ).fetchall()
        )
        connection.execute(
            sql.SQL(
                "UPDATE {p} p SET valid_to_epoch=%s FROM {w} w "
                "WHERE w.epoch_id=%s AND p.{k}=w.{k} AND p.valid_to_epoch IS NULL"
            ).format(
                p=sql.Identifier(published),
                w=sql.Identifier(working),
                k=sql.Identifier(key),
            ),
            (epoch, epoch),
        )
        connection.execute(
            sql.SQL(
                "INSERT INTO {p} ({columns},valid_from_epoch,valid_to_epoch,"
                "sealed_revision) "
                "SELECT {columns},%s,NULL,%s FROM {w} WHERE epoch_id=%s"
            ).format(
                p=sql.Identifier(published),
                w=sql.Identifier(working),
                columns=sql.SQL(",").join(map(sql.Identifier, columns)),
            ),
            (epoch, revision, epoch),
        )
        assignments = sql.SQL(",").join(
            sql.SQL("{c}=w.{c}").format(c=sql.Identifier(column))
            for column in columns
            if column != key
        )
        connection.execute(
            sql.SQL(
                "UPDATE {m} m SET {assignments},updated_epoch=%s,updated_revision=%s "
                "FROM {w} w WHERE w.epoch_id=%s AND m.{k}=w.{k}"
            ).format(
                m=sql.Identifier(materialized),
                w=sql.Identifier(working),
                assignments=assignments,
                k=sql.Identifier(key),
            ),
            (epoch, revision, epoch),
        )
    for family, key in (("group", "group_version_id"), ("claim", "claim_id")):
        if reference_fault == "omit_publication_and_absence" and family == "group":
            continue
        binding = f"groundloop_m5_published_{family}_certificate_binding"
        state = f"groundloop_m5_working_{family}_state"
        working = f"groundloop_m5_working_{family}_certificate_binding"
        if family == "group" and binding_fault == "unclosed":
            continue
        predecessor_binding = connection.execute(
            sql.SQL(
                "SELECT certificate_digest FROM {b} WHERE {k}=%s "
                "AND valid_to_epoch IS NULL"
            ).format(b=sql.Identifier(binding), k=sql.Identifier(key)),
            (
                database.group.group_version_id
                if family == "group"
                else database.base.claim_ids[0],
            ),
        ).fetchone()
        connection.execute(
            sql.SQL(
                "UPDATE {b} b SET valid_to_epoch=%s FROM {s} s "
                "WHERE s.epoch_id=%s AND b.{k}=s.{k} AND b.valid_to_epoch IS NULL"
            ).format(
                b=sql.Identifier(binding),
                s=sql.Identifier(state),
                k=sql.Identifier(key),
            ),
            (
                database.base.epoch_id
                if family == "group" and binding_fault == "wrong_close"
                else epoch,
                epoch,
            ),
        )
        connection.execute(
            sql.SQL(
                "INSERT INTO {b} ({k},valid_from_epoch,valid_to_epoch,"
                "sealed_revision,certificate_digest) "
                "SELECT {k},%s,NULL,%s,certificate_digest FROM {w} "
                "WHERE epoch_id=%s AND valid_to_revision IS NULL"
            ).format(
                b=sql.Identifier(binding),
                w=sql.Identifier(working),
                k=sql.Identifier(key),
            ),
            (epoch, revision, epoch),
        )
        if family == "group" and binding_fault == "same_k_successor":
            assert predecessor_binding is not None
            connection.execute(
                "INSERT INTO groundloop_m5_published_group_certificate_binding "
                "VALUES (%s,%s,NULL,%s,%s)",
                (
                    database.group.group_version_id,
                    epoch,
                    revision,
                    predecessor_binding[0],
                ),
            )
    evaluation = PostgresEvaluationOverlayStore(connection).apply_transition_local(
        connection,
        epoch,
        EvaluationTransition(
            stable_m4_digest("m4-evaluation-seal-v1", str(epoch)),
            1,
            kind=EvaluationTransitionKind.SEAL,
        ),
    )
    assert (evaluation.from_revision, evaluation.to_revision) == (1, 2)
    connection.execute(
        "UPDATE groundloop_m4_publication_head SET epoch_id=%s WHERE singleton",
        (epoch,),
    )
    connection.execute(
        "UPDATE groundloop_m5_publication_head SET epoch_id=%s,sealed_revision=%s "
        "WHERE singleton",
        (epoch, revision),
    )
    references: list[tuple[str, str, int, int, str, str]] = []
    keys = (
        reference_keys
        if reference_keys is not None
        else (
            (
                "requirement_state",
                database.group.requirements[0].requirement_version_id,
            ),
            ("group_state", database.group.group_version_id),
            ("group_certificate", database.group.group_version_id),
            ("claim_state", database.base.claim_ids[0]),
            ("claim_certificate", database.base.claim_ids[0]),
            ("answer_state", database.base.answer_id),
        )
    )
    with connection.cursor() as cursor:
        for kind, object_id in sorted(keys):
            if reference_fault == "omit_publication_and_absence" and kind in (
                "group_state",
                "group_certificate",
            ):
                continue
            if reference_fault == "omit_absence" and kind == "group_certificate":
                continue
            if (
                reference_fault == "wrong_absence_object"
                and kind == "group_certificate"
            ):
                object_id = "not-the-lost-group"
            artifact = (
                runtime_digests.changed_state_absence_artifact_digest(kind, object_id)
                if kind == "group_certificate" and absent_group_certificate
                else _present_artifact_hash(
                    cursor,
                    kind=kind,
                    object_id=object_id,
                    epoch_id=epoch,
                    revision=revision,
                )
            )
            if reference_fault == f"{kind}_absence":
                # Attack framing is intentional even for kinds forbidden by
                # the Python helper; do not weaken that helper to build it.
                artifact = runtime_digests.stable_m5_digest(
                    "m5-changed-state-absence-artifact-v1",
                    runtime_digests.enum_field(kind),
                    runtime_digests.text_field(object_id),
                )
            wire_epoch, wire_revision = epoch, revision
            if kind == "group_certificate":
                if reference_fault == "wrong_absence_epoch":
                    wire_epoch = database.base.epoch_id
                if reference_fault == "wrong_absence_revision":
                    wire_revision -= 1
                if reference_fault == "wrong_absence_hash":
                    artifact = "a" * 64
            wire = (kind, object_id, wire_epoch, wire_revision, artifact)
            references.append(
                (*wire, runtime_digests.changed_state_reference_digest(wire))
            )
    deltas = (
        status_deltas
        if status_deltas is not None
        else tuple(
            StatusDelta(event, kind, object_id, before, after, f"typed-event={event}")
            for kind, object_id, before, after in (
                ("answer", database.base.answer_id, "valid", "unsupported"),
                ("claim", database.base.claim_ids[0], "supported", "unsupported"),
            )
        )
    )
    combined = runtime_digests.combined_status_delta_set_digest(deltas)
    changed = runtime_digests.changed_state_set_digest(row[-1] for row in references)
    publication = stable_m4_digest("m4-publication-v1", str(epoch))
    opened = runtime_digests.open_event_receipt_binding_digest(
        epoch_id=database.base.epoch_id if result_fault == "open_receipt" else epoch,
        replayed=False,
        already_sealed=False,
        publication_id=None,
        already_failed=False,
        failure_reason=None,
    )
    published = runtime_digests.publication_receipt_binding_digest(
        epoch_id=epoch,
        publication_id=publication,
        replayed=result_fault == "publication_receipt",
    )
    seal_work = M5RuntimeWork(public_delta_count=len(deltas))
    event_work = M5RuntimeWork(
        **{
            name: getattr(start.work, name) + getattr(seal_work, name)
            for name in M5RuntimeWork.counter_names()
        }
    )
    if result_fault == "event_work":
        event_work = replace(
            event_work,
            public_delta_count=event_work.public_delta_count + 1,
            work_digest="",
        )
    source = runtime_digests.seal_contribution_source_digest(
        structural_event_id=event,
        combined_status_delta_set_hash=combined,
        changed_state_set_hash=changed,
        publication_id=publication,
    )
    if result_fault == "seal_source":
        source = "f" * 64
    key = runtime_digests.runtime_work_contribution_key_digest(
        epoch_id=epoch,
        contribution_kind="seal",
        source_id=event,
    )
    columns = (
        "epoch_id",
        *M5RuntimeWork.counter_names(),
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
            epoch,
            *seal_work.counter_values(),
            seal_work.work_digest,
            "seal",
            event,
            source,
            key,
            revision,
        ),
    )
    for kind, work in (("event", event_work), ("call", seal_work)):
        _insert_d26_runtime_work_row(
            connection, work=work, epoch=epoch, event=event, kind=kind
        )
    logical = runtime_digests.event_run_logical_result_digest(
        event_id=event,
        payload_hash="f" * 64 if result_fault == "payload" else payload,
        epoch_id=epoch,
        sealed_or_failed_outcome="sealed",
        original_open_receipt_binding_hash=opened,
        original_publication_receipt_binding_hash=published,
        event_work_digest=event_work.work_digest,
        combined_status_delta_set_hash=combined,
        changed_state_set_hash=changed,
        failure_reason=None,
    )
    connection.execute(
        """INSERT INTO groundloop_m5_event_result
        (structural_event_id,payload_hash,epoch_id,outcome,original_open_receipt_binding_hash,
         publication_id,original_publication_receipt_binding_hash,event_work_kind,event_work_digest,
         combined_status_delta_set_hash,changed_state_set_hash,failure_reason,logical_result_hash,
         delta_count,state_reference_count,coordinator_non_db_non_neural_ns,neural_wall_ns,
         postgres_roundtrip_wall_ns,external_io_wall_ns,end_to_end_wall_ns)
        VALUES (%s,%s,%s,'sealed',%s,%s,%s,'event',%s,%s,%s,NULL,%s,%s,%s,0,0,0,0,0)""",
        (
            event,
            "f" * 64 if result_fault == "payload" else payload,
            epoch,
            opened,
            publication,
            published,
            event_work.work_digest,
            combined,
            changed,
            logical,
            len(deltas),
            len(references),
        ),
    )
    for ordinal, delta in enumerate(deltas):
        connection.execute(
            "INSERT INTO groundloop_m5_event_result_delta VALUES "
            "(%s,%s,%s,%s,%s,%s,%s)",
            (
                event,
                ordinal,
                delta.object_type,
                delta.object_id,
                delta.old_status,
                delta.new_status,
                delta.reason,
            ),
        )
    for ordinal, reference in enumerate(references):
        connection.execute(
            "INSERT INTO groundloop_m5_event_result_state_reference VALUES "
            "(%s,%s,%s,%s,%s,%s,%s,%s)",
            (event, ordinal + int(result_fault == "child_order"), *reference),
        )
    connection.execute(
        "UPDATE groundloop_epoch SET revision=%s,semantic_status='sealed',"
        "evaluation_state='complete',"
        "publication_mode='strict',sealed_at=clock_timestamp() WHERE epoch_id=%s",
        (revision, epoch),
    )
    connection.execute(
        "UPDATE groundloop_m5_runtime_epoch SET revision=%s,runtime_state='sealed',"
        "terminal_at=clock_timestamp() "
        "WHERE epoch_id=%s",
        (revision, epoch),
    )
    assignments = sql.SQL(",").join(
        sql.SQL("{}=%s").format(sql.Identifier(name))
        for name in M5RuntimeWork.counter_names()
    )
    connection.execute(
        sql.SQL(
            "UPDATE groundloop_m5_runtime_work_accumulator SET {},work_digest=%s,"
            "updated_revision=%s,terminalized=true WHERE epoch_id=%s"
        ).format(assignments),
        (*event_work.counter_values(), event_work.work_digest, revision, epoch),
    )
    prefixes = (
        "required",
        "postgres_server_execution",
        "postgres_lock_wait",
        "postgres_wal_bytes",
        "postgres_shared_block_reads",
    )
    timing_assignments = []
    for prefix in prefixes:
        timing_assignments.extend(
            (
                sql.SQL("{c}={c}+1").format(
                    c=sql.Identifier(prefix + "_expected_count")
                ),
                sql.SQL("{c}={c}+%s").format(
                    c=sql.Identifier(prefix + "_missing_count")
                ),
            )
        )
    connection.execute(
        sql.SQL(
            "UPDATE groundloop_m5_runtime_timing_accumulator SET {},"
            "pending_contribution_kind=NULL,pending_source_id=NULL,"
            "pending_contribution_key_digest=NULL,pending_anchor_revision=NULL,"
            "updated_revision=%s,terminalized=true WHERE epoch_id=%s"
        ).format(sql.SQL(",").join(timing_assignments)),
        (*([1 + int(start.timing.has_pending_anchor)] * 5), revision, epoch),
    )
    coverage = tuple(
        f"{prefix}_{suffix}_count"
        for prefix in prefixes
        for suffix in ("expected", "observed", "missing")
    )
    connection.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_event_timing_coverage "
            "(structural_event_id,epoch_id,{},"
            "terminal_client_roundtrip_included) SELECT %s,epoch_id,{},false "
            "FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id=%s"
        ).format(
            sql.SQL(",").join(map(sql.Identifier, coverage)),
            sql.SQL(",").join(map(sql.Identifier, coverage)),
        ),
        (event, epoch),
    )
    if before_checks is not None:
        before_checks(connection)
    if validator_only_probe:
        # Probe only this INSERT-trigger boundary, then roll the ENTIRE image
        # back. Never claim a legal later publication or suppress other guards.
        connection.execute(
            "SET CONSTRAINTS groundloop_m5_event_result_children IMMEDIATE"
        )
        connection.rollback()
        return
    if not absent_group_certificate and reference_fault == "group_certificate_absence":
        connection.execute(
            "SET CONSTRAINTS groundloop_m5_event_result_children IMMEDIATE"
        )
    if binding_fault in ("unclosed", "same_k_successor"):
        connection.execute(
            "SET CONSTRAINTS groundloop_m5_event_result_children IMMEDIATE"
        )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()
    oracle = read_m5_oracle_states(connection)
    assert oracle.groups[database.group.group_version_id].complete == (
        not absent_group_certificate
    )


def _prepare_loss_edges(database: M5RuntimeDatabase) -> tuple[int, str, str]:
    connection = database.connection
    epoch, event, payload = _manual_document_delete(database, defer_open_commit=True)
    with connection.cursor() as cursor:
        intent = authorize_and_derive_matching_transition(
            cursor,
            epoch_id=epoch,
            expected_runtime_revision=1,
            resulting_revision=1,
            source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            source_id=event,
            expected_source_identity_hash=payload,
        )
        install_document_matching_foundation_retained_bytes(cursor, intent)
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()
    _manual_edge(connection, epoch, "semantic_pending", 1)
    _manual_edge(connection, epoch, "semantic_complete", 2)
    return epoch, event, payload


def test_raw_document_certificate_absence_seals_with_intact_guards(
    raw_loss_database: M5RuntimeDatabase,
) -> None:
    database = raw_loss_database
    epoch, event, payload = _prepare_loss_edges(database)
    _controlled_document_seal(database, epoch, event, payload)


def _independent_python_inputs(database: M5RuntimeDatabase) -> M5Repository:
    """Hydrate ONLY raw domain inputs, never SQL state/certificate/patch output."""
    connection = database.connection
    base = InMemoryRepository(current_epoch=database.base.epoch_id)
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
        "SELECT document_version_id,document_id,content_hash,valid_from_epoch "
        "FROM groundloop_document_version WHERE valid_to_epoch IS NULL"
    ):
        chunks = tuple(
            ChunkVersion(*c)
            for c in connection.execute(
                "SELECT chunk_version_id,document_version_id,chunk_index,"
                "text,text_hash "
                "FROM groundloop_chunk_version WHERE document_version_id=%s "
                "ORDER BY chunk_index",
                (row[0],),
            ).fetchall()
        )
        base.register_document_version(DocumentVersion(*row[:3]), chunks, int(row[3]))
    policy = connection.execute(
        "SELECT policy_version,support_threshold,refute_threshold,tie_rule_version,"
        "valid_from_epoch FROM groundloop_decision_policy WHERE valid_to_epoch IS NULL"
    ).fetchone()
    assert policy is not None
    base.activate_policy(DecisionPolicy(*policy[:4]), int(policy[4]))
    repo = M5Repository(base)
    repo.register_group(database.group, database.base.epoch_id)
    for row in connection.execute(
        "SELECT o.observation_id,o.subject_kind,o.subject_id,o.chunk_version_id,"
        "o.task_type,o.support_score,o.refute_score,o.neutral_score,"
        "o.model_id,o.model_version,o.prompt_version,o.input_hash "
        "FROM groundloop_semantic_observation o "
        "JOIN groundloop_observation_currency c USING(observation_id)"
    ).fetchall():
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


def _independent_present_artifacts(repo: M5Repository) -> dict[tuple[str, str], str]:
    """Reference bytes from raw Python inputs, never published/working SQL rows."""
    states = compute_reference_states(repo)
    policy = repo.base.current_policy().policy_version
    certificates = {
        group_id: certificate
        for group_id in states.groups
        if (certificate := build_reference_group_certificate(repo, group_id))
        is not None
    }
    artifacts = {
        ("requirement_state", key): runtime_digests.requirement_state_artifact_digest(
            **asdict(state), decision_policy_version=policy
        )
        for key, state in states.requirements.items()
    }
    for key, state in states.groups.items():
        certificate = certificates.get(key)
        artifacts["group_state", key] = runtime_digests.group_state_artifact_digest(
            **asdict(state),
            decision_policy_version=policy,
            certificate_digest=(
                certificate.certificate_digest if certificate is not None else None
            ),
        )
        if certificate is not None:
            artifacts["group_certificate", key] = certificate.certificate_digest
    for key, state in states.claims.items():
        certificate = build_reference_claim_certificate(repo, key, certificates)
        artifacts["claim_state", key] = runtime_digests.claim_state_artifact_digest(
            **asdict(state),
            decision_policy_version=policy,
            certificate_digest=certificate.certificate_digest,
        )
        artifacts["claim_certificate", key] = certificate.certificate_digest
    for key, state in states.answers.items():
        artifacts["answer_state", key] = runtime_digests.answer_state_artifact_digest(
            **asdict(state)
        )
    return artifacts


@pytest.mark.parametrize("raw_loss_database", ["alternate_support"], indirect=True)
@pytest.mark.parametrize("malicious_absence", [False, True])
def test_document_complete_to_complete_present_certificate_change(
    raw_loss_database: M5RuntimeDatabase, malicious_absence: bool
) -> None:
    """Reachable D25 structural replacement, no C2 active completion required."""
    database = raw_loss_database
    connection = database.connection
    repo = _independent_python_inputs(database)
    group_id = database.group.group_version_id
    before = compute_reference_states(repo)
    sql_before = read_m5_oracle_states(connection)
    for name in ("requirements", "groups", "claims", "answers"):
        assert getattr(before, name) == getattr(sql_before, name)
    old_artifacts = _independent_present_artifacts(repo)
    old_certificate = build_reference_group_certificate(repo, group_id)
    assert old_certificate is not None
    assert old_certificate.rows[0].selected_observation_id == (
        "d33-predecessor-selected-observation"
    )
    source = connection.execute(
        "SELECT document_version_id FROM groundloop_chunk_version "
        "WHERE chunk_version_id=%s",
        (database.base.chunk_ids[0],),
    ).fetchone()
    assert source is not None
    epoch, event, payload = _prepare_loss_edges(database)
    assert repo.begin_event_epoch().epoch_id == epoch
    repo.base.deactivate_document_version(str(source[0]), epoch)
    for _ in range(4):
        repo.advance_semantic_revision()
    expected = compute_reference_states(repo)
    new_artifacts = _independent_present_artifacts(repo)
    new_certificate = build_reference_group_certificate(repo, group_id)
    assert new_certificate is not None
    assert before.groups[group_id].complete and expected.groups[group_id].complete
    assert new_certificate.rows[0].selected_observation_id == (
        "d33-predecessor-surviving-observation"
    )
    assert old_certificate.certificate_digest != new_certificate.certificate_digest
    assert old_artifacts.keys() == new_artifacts.keys()
    changed_keys = tuple(
        sorted(
            key for key in new_artifacts if old_artifacts.get(key) != new_artifacts[key]
        )
    )
    assert ("group_certificate", group_id) in changed_keys
    assert ("claim_certificate", database.base.claim_ids[0]) in changed_keys
    # Certificate-only changes belong to the set even with no status delta.
    for family in ("claims", "answers"):
        for key, state in getattr(expected, family).items():
            assert state.status == getattr(before, family)[key].status
    if malicious_absence:
        with pytest.raises(
            psycopg.Error, match="absence|state artifact|reference subtype"
        ):
            _controlled_document_seal(
                database,
                epoch,
                event,
                payload,
                absent_group_certificate=False,
                reference_keys=changed_keys,
                status_deltas=(),
                reference_fault="group_certificate_absence",
            )
        connection.rollback()
        assert connection.execute(
            "SELECT runtime_state,revision FROM groundloop_m5_runtime_epoch "
            "WHERE epoch_id=%s",
            (epoch,),
        ).fetchone() == ("semantic_complete", 3)
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_event_result "
            "WHERE structural_event_id=%s",
            (event,),
        ).fetchone() == (0,)
        return
    _controlled_document_seal(
        database,
        epoch,
        event,
        payload,
        absent_group_certificate=False,
        reference_keys=changed_keys,
        status_deltas=(),
    )
    actual = read_m5_oracle_states(connection)
    for name in ("requirements", "groups", "claims", "answers"):
        assert getattr(expected, name) == getattr(actual, name)
    assert all(
        value == 0 for value in asdict(read_m5_mismatch_counts(connection)).values()
    )
    references = tuple(
        (*wire, runtime_digests.changed_state_reference_digest(wire))
        for kind, key in changed_keys
        for wire in ((kind, key, epoch, 4, new_artifacts[kind, key]),)
    )
    assert (
        tuple(
            connection.execute(
                "SELECT kind,object_id,epoch_id,revision,"
                "state_artifact_hash,reference_digest "
                "FROM groundloop_m5_event_result_state_reference "
                "WHERE structural_event_id=%s ORDER BY reference_ordinal",
                (event,),
            ).fetchall()
        )
        == references
    )
    assert connection.execute(
        "SELECT changed_state_set_hash,delta_count FROM groundloop_m5_event_result "
        "WHERE structural_event_id=%s",
        (event,),
    ).fetchone() == (
        runtime_digests.changed_state_set_digest(row[-1] for row in references),
        0,
    )
    assert connection.execute(
        "SELECT certificate_digest,valid_from_epoch,valid_to_epoch,sealed_revision "
        "FROM groundloop_m5_published_group_certificate_binding "
        "WHERE group_version_id=%s ORDER BY valid_from_epoch",
        (group_id,),
    ).fetchall() == [
        (old_certificate.certificate_digest, database.base.epoch_id, epoch, 0),
        (new_certificate.certificate_digest, epoch, None, 4),
    ]
    assert connection.execute(
        "SELECT certificate_digest FROM groundloop_m5_group_certificate_artifact "
        "WHERE certificate_digest=ANY(%s) ORDER BY certificate_digest",
        ([old_certificate.certificate_digest, new_certificate.certificate_digest],),
    ).fetchall() == [
        (digest,)
        for digest in sorted(
            (
                old_certificate.certificate_digest,
                new_certificate.certificate_digest,
            )
        )
    ]


def test_document_loss_full_python_sql_states_and_all_reference_bytes(
    raw_loss_database: M5RuntimeDatabase,
) -> None:
    database = raw_loss_database
    connection = database.connection
    repo = _independent_python_inputs(database)
    before = compute_reference_states(repo)
    sql_before = read_m5_oracle_states(connection)
    for name in ("requirements", "groups", "claims", "answers"):
        assert getattr(before, name) == getattr(sql_before, name)
    source = connection.execute(
        "SELECT document_version_id FROM groundloop_document_version "
        'WHERE valid_to_epoch IS NULL ORDER BY document_version_id COLLATE "C"'
    ).fetchone()
    assert source is not None
    epoch, event, payload = _prepare_loss_edges(database)
    assert repo.begin_event_epoch().epoch_id == epoch
    repo.base.deactivate_document_version(str(source[0]), epoch)
    for _ in range(4):
        repo.advance_semantic_revision()
    _controlled_document_seal(database, epoch, event, payload)
    expected = compute_reference_states(repo)
    actual = read_m5_oracle_states(connection)
    for name in ("requirements", "groups", "claims", "answers"):
        assert getattr(expected, name) == getattr(actual, name)
    assert all(
        value == 0 for value in asdict(read_m5_mismatch_counts(connection)).values()
    )
    policy = repo.base.current_policy().policy_version
    claim = repo.base.all_claim_ids()[0]
    claim_certificate = build_reference_claim_certificate(repo, claim, {})
    artifacts: list[tuple[str, str, str]] = []
    for object_id, state in expected.requirements.items():
        artifacts.append(
            (
                "requirement_state",
                object_id,
                runtime_digests.requirement_state_artifact_digest(
                    **asdict(state), decision_policy_version=policy
                ),
            )
        )
    for object_id, state in expected.groups.items():
        artifacts.append(
            (
                "group_state",
                object_id,
                runtime_digests.group_state_artifact_digest(
                    **asdict(state),
                    decision_policy_version=policy,
                    certificate_digest=None,
                ),
            )
        )
        assert before.groups[object_id].complete and not state.complete
        artifacts.append(
            (
                "group_certificate",
                object_id,
                runtime_digests.changed_state_absence_artifact_digest(
                    "group_certificate", object_id
                ),
            )
        )
    for object_id, state in expected.claims.items():
        artifacts.append(
            (
                "claim_state",
                object_id,
                runtime_digests.claim_state_artifact_digest(
                    **asdict(state),
                    decision_policy_version=policy,
                    certificate_digest=claim_certificate.certificate_digest,
                ),
            )
        )
        artifacts.append(
            ("claim_certificate", object_id, claim_certificate.certificate_digest)
        )
    for object_id, state in expected.answers.items():
        artifacts.append(
            (
                "answer_state",
                object_id,
                runtime_digests.answer_state_artifact_digest(**asdict(state)),
            )
        )
    references = tuple(
        (*wire, runtime_digests.changed_state_reference_digest(wire))
        for kind, object_id, artifact in sorted(artifacts)
        for wire in ((kind, object_id, epoch, 4, artifact),)
    )
    assert (
        tuple(
            connection.execute(
                "SELECT kind,object_id,epoch_id,revision,"
                "state_artifact_hash,reference_digest "
                "FROM groundloop_m5_event_result_state_reference "
                "WHERE structural_event_id=%s ORDER BY reference_ordinal",
                (event,),
            ).fetchall()
        )
        == references
    )
    expected_set = runtime_digests.changed_state_set_digest(
        row[-1] for row in references
    )
    assert connection.execute(
        "SELECT changed_state_set_hash FROM groundloop_m5_event_result "
        "WHERE structural_event_id=%s",
        (event,),
    ).fetchone() == (expected_set,)


@pytest.mark.parametrize(
    "fault",
    (
        "omit_absence",
        "omit_publication_and_absence",
        "wrong_absence_object",
        "wrong_absence_epoch",
        "wrong_absence_revision",
        "wrong_absence_hash",
        "requirement_state_absence",
        "group_state_absence",
        "claim_state_absence",
        "answer_state_absence",
        "claim_certificate_absence",
    ),
)
def test_controlled_result_rejects_rehashed_absence_counterexample(
    raw_loss_database: M5RuntimeDatabase,
    fault: str,
) -> None:
    """Recompute child/set/result/seal identities; structural proof still wins."""
    database = raw_loss_database
    epoch, event, payload = _prepare_loss_edges(database)
    with pytest.raises(
        psycopg.Error,
        match="absence|state artifact|published|reference subtype|historical state",
    ):
        _controlled_document_seal(
            database,
            epoch,
            event,
            payload,
            reference_fault=fault,
        )
    database.connection.rollback()
    assert database.connection.execute(
        "SELECT runtime_state,revision FROM groundloop_m5_runtime_epoch "
        "WHERE epoch_id=%s",
        (epoch,),
    ).fetchone() == ("semantic_complete", 3)
    assert database.connection.execute(
        "SELECT count(*) FROM groundloop_m5_event_result WHERE epoch_id=%s",
        (epoch,),
    ).fetchone() == (0,)


@pytest.mark.parametrize("fault", ("unclosed", "wrong_close", "same_k_successor"))
def test_document_absence_binding_interval_negatives(
    raw_loss_database: M5RuntimeDatabase,
    fault: str,
) -> None:
    """Fault FIRST close/insert; never reopen immutable published history."""
    database = raw_loss_database
    epoch, event, payload = _prepare_loss_edges(database)
    # Equal-to-start closure is rejected by retained014 BEFORE its CHECK.
    match = "may close exactly once" if fault == "wrong_close" else "M5-D33|absence"
    with pytest.raises(psycopg.Error, match=match):
        _controlled_document_seal(database, epoch, event, payload, binding_fault=fault)
    database.connection.rollback()
    assert database.connection.execute(
        "SELECT count(*) FROM groundloop_m5_event_result WHERE epoch_id=%s", (epoch,)
    ).fetchone() == (0,)


@pytest.mark.parametrize("future_closed", (False, True))
def test_absence_validator_is_as_of_k_not_timeless_future_binding_exclusion(
    raw_loss_database: M5RuntimeDatabase,
    future_closed: bool,
) -> None:
    """UNCOMMITTED controlled interval probe, not legal restoration/P33 replay.

    A future interval is an adversarial unrelated input. Its old artifact is
    NOT asserted valid at L and its epoch is NOT authored semantic-complete.
    Only021's K child boundary is forced; the complete transaction rolls back.
    Genuine valid restored publication and historical replay still wait C2/P33.
    """
    database = raw_loss_database
    epoch, event, payload = _prepare_loss_edges(database)
    prior_bindings = database.connection.execute(
        "SELECT * FROM groundloop_m5_published_group_certificate_binding "
        "ORDER BY group_version_id,valid_from_epoch"
    ).fetchall()
    prior_m4_head = database.connection.execute(
        "SELECT * FROM groundloop_m4_publication_head WHERE singleton"
    ).fetchone()
    prior_m5_head = database.connection.execute(
        "SELECT * FROM groundloop_m5_publication_head WHERE singleton"
    ).fetchone()
    database.connection.rollback()
    future_points: list[int] = []

    def probe(connection: Connection[Any]) -> None:
        for ordinal in range(1 + int(future_closed)):
            row = connection.execute(
                "INSERT INTO groundloop_epoch (event_id,payload_hash,revision,"
                "structural_status,semantic_status,evaluation_state,publication_mode) "
                "VALUES (%s,%s,0,'received','pending','pending','provisional') "
                "RETURNING epoch_id",
                (f"d33-uncommitted-future-interval-{ordinal}", "a" * 64),
            ).fetchone()
            assert row is not None and int(row[0]) > epoch
            future_points.append(int(row[0]))
        predecessor = connection.execute(
            "SELECT certificate_digest "
            "FROM groundloop_m5_published_group_certificate_binding "
            "WHERE group_version_id=%s AND valid_to_epoch=%s",
            (database.group.group_version_id, epoch),
        ).fetchone()
        assert predecessor is not None
        connection.execute(
            "INSERT INTO groundloop_m5_published_group_certificate_binding "
            "VALUES (%s,%s,%s,1,%s)",
            (
                database.group.group_version_id,
                future_points[0],
                future_points[1] if future_closed else None,
                predecessor[0],
            ),
        )
        assert connection.execute(
            "SELECT epoch_id,sealed_revision FROM groundloop_m5_publication_head "
            "WHERE singleton"
        ).fetchone() == (epoch, 4)

    _controlled_document_seal(
        database, epoch, event, payload, before_checks=probe, validator_only_probe=True
    )
    assert len(future_points) == 1 + int(future_closed)
    # Sequence values may advance on rollback; all relational effects must not.
    assert database.connection.execute(
        "SELECT count(*) FROM groundloop_epoch WHERE epoch_id=ANY(%s)",
        (future_points,),
    ).fetchone() == (0,)
    assert (
        database.connection.execute(
            "SELECT * FROM groundloop_m5_published_group_certificate_binding "
            "ORDER BY group_version_id,valid_from_epoch"
        ).fetchall()
        == prior_bindings
    )
    assert (
        database.connection.execute(
            "SELECT * FROM groundloop_m4_publication_head WHERE singleton"
        ).fetchone()
        == prior_m4_head
    )
    assert (
        database.connection.execute(
            "SELECT * FROM groundloop_m5_publication_head WHERE singleton"
        ).fetchone()
        == prior_m5_head
    )
    assert database.connection.execute(
        "SELECT runtime_state,revision FROM groundloop_m5_runtime_epoch "
        "WHERE epoch_id=%s",
        (epoch,),
    ).fetchone() == ("semantic_complete", 3)
    assert database.connection.execute(
        "SELECT count(*) FROM groundloop_m5_event_result WHERE epoch_id=%s", (epoch,)
    ).fetchone() == (0,)


@pytest.mark.parametrize(
    "fault",
    (
        "payload",
        "open_receipt",
        "publication_receipt",
        "event_work",
        "child_order",
        "seal_source",
    ),
)
def test_typed_predecessor_corruption_is_rejected_before_immutable_insertion(
    raw_loss_database: M5RuntimeDatabase,
    fault: str,
) -> None:
    """Self-rehashed result/children; retained earlier guards may preempt D33.

    Immutable P cannot legally be corrupted later, so fault the initial output.
    This is total validation-chain evidence, not a claim every case reaches021.
    """
    database = raw_loss_database
    epoch, event, payload = _prepare_loss_edges(database)
    with pytest.raises(
        psycopg.Error,
        match="result|receipt|work|source|ordinal|payload|seal final envelope mismatch",
    ):
        _controlled_document_seal(database, epoch, event, payload, result_fault=fault)
    database.connection.rollback()
    assert database.connection.execute(
        "SELECT count(*) FROM groundloop_m5_event_result WHERE epoch_id=%s", (epoch,)
    ).fetchone() == (0,)


def test_diagnostic_unmodified_production_helper_double_journal_rejected(
    raw_loss_database: M5RuntimeDatabase,
) -> None:
    """OPEN production blocker, not a positive or a production acceptance gate.

    The originally failed seal/probe XML is retained outside the repository.
    This passing diagnostic means ONLY that the current defect is reproducible
    and intact017 guards reject it. A new activation must repair the helper.
    """
    database = raw_loss_database
    epoch, event, payload = _prepare_loss_edges(database)
    with pytest.raises(psycopg.Error, match="current upsert mismatch"):
        _controlled_document_seal(
            database,
            epoch,
            event,
            payload,
            production_promotion_diagnostic=True,
        )
    database.connection.rollback()


@pytest.mark.parametrize(
    "fault",
    (
        "direct_root_job_ids",
        "direct_scope_root_job_ids",
        "direct_fallback_claim_ids",
        "missing_declaration_key",
        "null_declaration_array",
        "nontext_declaration_id",
        "duplicate_declaration_id",
        "extra_manifest_key",
        "extra_payload_key",
        "missing_payload_uri",
        "nonempty_scope_manifest",
        "wrong_manifest_source",
        "counter_zero",
        "counter_null_predecessor",
    ),
)
def test_raw_document_predecessor_rejects_bad_source_or_declaration(
    raw_document_database: M5RuntimeDatabase,
    fault: str,
) -> None:
    connection = raw_document_database.connection
    epoch, _, _ = _manual_document_delete(raw_document_database, fault=fault)
    work, anchor, source = _edge_values(connection, epoch, "semantic_pending", 1)
    with pytest.raises(psycopg.Error):
        _insert(connection, work, anchor, source)
    connection.rollback()
    assert connection.execute(
        "SELECT runtime_state,revision FROM groundloop_m5_runtime_epoch "
        "WHERE epoch_id=%s",
        (epoch,),
    ).fetchone() == ("structural_committed", 1)


@pytest.fixture
def raw_loss_database(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> Iterator[M5RuntimeDatabase]:
    """Seed a genuine complete group BEFORE activation and migration017.

    No trigger disabling or held C1 code. The subsequent document header and
    seal remain explicitly controlled SQL-validator diagnostics.
    """
    original = runtime_fixtures.install_test_activation_barrier
    activated: list[bool] = []
    incomplete = getattr(request, "param", None) == "incomplete"
    alternate_support = getattr(request, "param", None) == "alternate_support"

    def activate(connection: Connection[Any], base: Any, **kwargs: Any) -> None:
        # A second unused source allows a genuine guarded terminal P followed
        # by another document update. All source rows precede activation.
        auxiliary = "zz-d33-auxiliary"
        auxiliary_text = "auxiliary"
        if alternate_support:
            primary_text = connection.execute(
                "SELECT text FROM groundloop_chunk_version WHERE chunk_version_id=%s",
                (base.chunk_ids[0],),
            ).fetchone()
            assert primary_text is not None
            primary_hash = normalized_text_hash_v1(str(primary_text[0]))
            auxiliary_text = next(
                text
                for number in range(10000)
                for text in (f"surviving support {number}",)
                if normalized_text_hash_v1(text) > primary_hash
            )
        auxiliary_hash = hashlib.sha256(auxiliary_text.encode()).hexdigest()
        connection.execute(
            "INSERT INTO groundloop_document VALUES (%s,NULL,'fixture')",
            (auxiliary,),
        )
        connection.execute(
            "INSERT INTO groundloop_document_version VALUES (%s,%s,%s,%s,NULL)",
            (
                auxiliary,
                auxiliary,
                auxiliary_hash,
                base.epoch_id,
            ),
        )
        connection.execute(
            "INSERT INTO groundloop_chunk_version VALUES "
            "(%s,%s,0,%s,%s,'fixture-v1',%s,NULL)",
            (
                auxiliary,
                auxiliary,
                auxiliary_text,
                auxiliary_hash,
                base.epoch_id,
            ),
        )
        requirement = connection.execute(
            "SELECT requirement_version_id FROM groundloop_m5_requirement_version"
        ).fetchone()
        assert requirement is not None
        observation = "d33-predecessor-selected-observation"
        insert_observation(
            connection,
            observation_id=observation,
            subject_kind="requirement",
            subject_id=str(requirement[0]),
            chunk_id=base.chunk_ids[0],
            produced_epoch=base.epoch_id,
            scores=(0.05, 0.05, 0.9) if incomplete else (0.9, 0.05, 0.05),
        )
        install_current_currency(
            connection,
            observation_id=observation,
            subject_kind="requirement",
            subject_id=str(requirement[0]),
            chunk_id=base.chunk_ids[0],
            task_type="verify_requirement_v1",
            epoch_id=base.epoch_id,
            revision=0,
        )
        if alternate_support:
            survivor = "d33-predecessor-surviving-observation"
            insert_observation(
                connection,
                observation_id=survivor,
                subject_kind="requirement",
                subject_id=str(requirement[0]),
                chunk_id=auxiliary,
                produced_epoch=base.epoch_id,
                scores=(0.9, 0.05, 0.05),
            )
            install_current_currency(
                connection,
                observation_id=survivor,
                subject_kind="requirement",
                subject_id=str(requirement[0]),
                chunk_id=auxiliary,
                task_type="verify_requirement_v1",
                epoch_id=base.epoch_id,
                revision=0,
            )
        original(connection, base, **kwargs)
        activated.append(True)

    monkeypatch.setattr(runtime_fixtures, "install_test_activation_barrier", activate)
    with contextmanager(runtime_fixtures.m5_runtime_db.__wrapped__)() as database:
        assert activated == [True]
        _install_runtime_schema(database)
        _raw_install(database.connection, database.schema_name)
        try:
            yield database
        finally:
            database.connection.rollback()


def test_raw_document_loss_uses_genuine_d25_initial_patch(
    raw_loss_database: M5RuntimeDatabase,
) -> None:
    database = raw_loss_database
    connection = database.connection
    before = connection.execute(
        "SELECT complete,certificate_digest FROM groundloop_m5_published_group_state"
    ).fetchone()
    assert before is not None and before[0] and before[1] is not None
    epoch, event, payload = _manual_document_delete(database, defer_open_commit=True)
    with connection.cursor() as cursor:
        intent = authorize_and_derive_matching_transition(
            cursor,
            epoch_id=epoch,
            expected_runtime_revision=1,
            resulting_revision=1,
            source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            source_id=event,
            expected_source_identity_hash=payload,
        )
        receipt = install_document_matching_foundation_retained_bytes(cursor, intent)
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()
    assert not receipt.exact_replay
    assert database.group.group_version_id in intent.group_certificate_ids
    assert connection.execute(
        "SELECT complete,certificate_digest FROM groundloop_m5_working_group_state "
        "WHERE epoch_id=%s",
        (epoch,),
    ).fetchone() == (False, None)
    _manual_edge(connection, epoch, "semantic_pending", 1)
    _manual_edge(connection, epoch, "semantic_complete", 2)


@pytest.mark.parametrize("raw_loss_database", ("incomplete",), indirect=True)
def test_incomplete_predecessor_withdrawal_has_no_certificate_loss_change(
    raw_loss_database: M5RuntimeDatabase,
) -> None:
    """Actual D25 foundation/no-loss proof, not a fabricated restored seal."""
    database = raw_loss_database
    connection = database.connection
    assert connection.execute(
        "SELECT complete,certificate_digest FROM groundloop_m5_published_group_state "
        "WHERE group_version_id=%s",
        (database.group.group_version_id,),
    ).fetchone() == (False, None)
    assert connection.execute(
        "SELECT count(*) FROM groundloop_m5_published_group_certificate_binding "
        "WHERE group_version_id=%s",
        (database.group.group_version_id,),
    ).fetchone() == (0,)
    epoch, event, payload = _manual_document_delete(database, defer_open_commit=True)
    with connection.cursor() as cursor:
        intent = authorize_and_derive_matching_transition(
            cursor,
            epoch_id=epoch,
            expected_runtime_revision=1,
            resulting_revision=1,
            source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            source_id=event,
            expected_source_identity_hash=payload,
        )
        prepared = _prepare_matching_transition(cursor, intent)
        assert not any(
            change.kind.value == "group_certificate"
            for change in prepared.prewrite_patch_artifact.logical_patch.changes
        )
        install_document_matching_foundation_retained_bytes(cursor, intent)
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()
    _manual_edge(connection, epoch, "semantic_pending", 1)
    _manual_edge(connection, epoch, "semantic_complete", 2)
    assert connection.execute(
        "SELECT count(*) FROM groundloop_m5_working_group_certificate_binding "
        "WHERE epoch_id=%s AND valid_to_revision IS NULL",
        (epoch,),
    ).fetchone() == (0,)


@pytest.mark.parametrize("cancel", (False, True))
def test_document_nonempty_roots_close_through_actual_job_progress(
    raw_loss_database: M5RuntimeDatabase,
    cancel: bool,
) -> None:
    """Controlled opener; genuine dispatch/staging/barrier, not C1 acceptance."""
    database = raw_loss_database
    connection = database.connection
    roots: list[Any] = []
    epoch, event, payload = _manual_document_delete(
        database, defer_open_commit=True, root_capture=roots
    )
    with connection.cursor() as cursor:
        intent = authorize_and_derive_matching_transition(
            cursor,
            epoch_id=epoch,
            expected_runtime_revision=1,
            resulting_revision=1,
            source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            source_id=event,
            expected_source_identity_hash=payload,
        )
        install_document_matching_foundation_retained_bytes(cursor, intent)
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()
    root_hash = runtime_digests.requirement_root_set_digest(
        root.job.logical_job_id for root in roots
    )
    assert root_hash != runtime_digests.requirement_root_set_digest(())
    assert connection.execute(
        "SELECT requirement_root_set_hash,open_work_count,open_scope_count "
        "FROM groundloop_m5_runtime_epoch WHERE epoch_id=%s",
        (epoch,),
    ).fetchone() == (root_hash, len(roots), len(roots))
    counter_before = connection.execute(
        "SELECT revision,confirmed_as_of_epoch "
        "FROM groundloop_m4_evaluation_epoch_counter "
        "WHERE epoch_id=%s",
        (epoch,),
    ).fetchone()
    assert counter_before == (1, database.base.epoch_id)
    connection.commit()
    store = PostgresM5RuntimeStore(connection)
    revision = 1
    for root in () if cancel else roots:
        job = root.job
        lease = _acquire(
            connection, epoch_id=epoch, expected_revision=revision, job=job
        )
        assert lease.should_execute and lease.resulting_revision == revision + 1
        revision += 1
        assert lease.attempt is not None
        result = _result(epoch_id=epoch, root=job, pairs=())
        output = M5AttemptOutput.build(
            attempt=lease.attempt,
            job_epoch_id=epoch,
            payload_hash=job.payload_hash,
            result_artifact_id=result.result_artifact_id,
            result_artifact_hash=result.result_artifact_hash,
        )
        staged = store.stage_m5_discovery_result_atomically(
            epoch,
            revision,
            lease,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            M5RuntimeWork(
                requirement_forward_retrieval_call_count=1,
                requirement_fallback_forward_call_count=1,
                embedding_model_call_count=1,
            ),
            M5RuntimeTiming(),
            eligible_snapshot_exhausted=True,
        )
        assert staged.resulting_revision == revision + 1 and not staged.exact_replay
        revision += 1
    if cancel:
        cancelled = store.cancel_m5_work_atomically(
            epoch,
            revision,
            M5CancellationPlan.build(
                epoch_id=epoch,
                structural_event_id=event,
                reason=M5TerminalReason.SCOPE_RETIRED,
                cancelled_job_ids=tuple(root.job.logical_job_id for root in roots),
            ),
        )
        completed_revision = cancelled.resulting_revision
    else:
        barrier = store.close_m5_requirement_roots_atomically(
            epoch, revision, root_hash
        )
        completed_revision = barrier.resulting_revision
    _manual_edge(connection, epoch, "semantic_complete", completed_revision)
    assert connection.execute(
        "SELECT runtime_state,open_work_count,open_scope_count,blocking_failure_count "
        "FROM groundloop_m5_runtime_epoch WHERE epoch_id=%s",
        (epoch,),
    ).fetchone() == ("semantic_complete", 0, 0, 0)
    assert connection.execute(
        "SELECT count(*) FROM groundloop_m5_semantic_job WHERE epoch_id=%s "
        "AND job_state=%s AND completion_digest IS NOT NULL",
        (epoch, "cancelled" if cancel else "completed_active"),
    ).fetchone() == (len(roots),)
    assert connection.execute(
        "SELECT count(*) FROM groundloop_m5_runtime_work_contribution "
        "WHERE epoch_id=%s "
        "AND contribution_kind=%s",
        (epoch, "cancellation" if cancel else "root_barrier"),
    ).fetchone() == (1,)
    assert (
        connection.execute(
            "SELECT revision,confirmed_as_of_epoch "
            "FROM groundloop_m4_evaluation_epoch_counter WHERE epoch_id=%s",
            (epoch,),
        ).fetchone()
        == counter_before
    )
    assert connection.execute(
        "SELECT count(*) FROM groundloop_m5_runtime_work_contribution "
        "WHERE epoch_id=%s "
        "AND contribution_kind='semantic_readiness'",
        (epoch,),
    ).fetchone() == (1,)  # complete only; no forbidden rootless readiness-start


def test_controlled_document_readiness_accepts_canonical_typed_predecessor(
    raw_loss_database: M5RuntimeDatabase,
) -> None:
    """Two guarded document images are SQL evidence, not production C1."""
    database = raw_loss_database
    connection = database.connection
    previous, event, payload = _prepare_loss_edges(database)
    _controlled_document_seal(database, previous, event, payload)
    epoch, _, _ = _manual_document_delete(database)
    _manual_edge(connection, epoch, "semantic_pending", 1)
    _manual_edge(connection, epoch, "semantic_complete", 2)


def _source_mutations(
    database: M5RuntimeDatabase, epoch: int
) -> dict[str, tuple[str, tuple[Any, ...]]]:
    """Mutations of otherwise valid, controlled SQL-boundary inputs."""
    return {
        "counter_revision_two": (
            "UPDATE groundloop_m4_evaluation_epoch_counter SET revision=2 "
            "WHERE epoch_id=%s",
            (epoch,),
        ),
        "counter_declaration_hash": (
            "UPDATE groundloop_m4_evaluation_epoch_counter SET declaration_hash=%s "
            "WHERE epoch_id=%s",
            ("0" * 64, epoch),
        ),
        "counter_wrong_confirmation": (
            "UPDATE groundloop_m4_evaluation_epoch_counter "
            "SET confirmed_as_of_epoch=%s,revision=revision+1 "
            "WHERE epoch_id=%s",
            (epoch, epoch),
        ),
        "missing_counter": (
            "DELETE FROM groundloop_m4_evaluation_epoch_counter WHERE epoch_id=%s",
            (epoch,),
        ),
        "override": (
            "INSERT INTO groundloop_m4_evaluation_override_counter VALUES "
            "(%s,'claim',%s,1,1)",
            (epoch, database.base.claim_ids[0]),
        ),
        "prior_transition": (
            "INSERT INTO groundloop_m4_evaluation_counter_transition "
            "(epoch_id,transition_id,payload_hash,transition_kind,from_revision,"
            "to_revision,override_rows_written) "
            "VALUES (%s,'unexpected',%s,'delta',0,1,0)",
            (epoch, "0" * 64),
        ),
        "discovery_scope": (
            "INSERT INTO groundloop_discovery_scope VALUES "
            "('unexpected-scope',%s,'d33-schema-registry','all_registered_claims',NULL,NULL)",
            (epoch,),
        ),
        "wrong_direct_action": (
            "UPDATE groundloop_m4_update SET update_kind='insert' WHERE epoch_id=%s",
            (epoch,),
        ),
        "retained_manifest": (
            "UPDATE groundloop_m4_update SET manifest=jsonb_set(manifest,"
            "'{_groundloop_m4_runtime_v1,event_manifest,source_uri}',"
            "'\"unexpected\"'::jsonb) WHERE epoch_id=%s",
            (epoch,),
        ),
        "old_content_hash": (
            "UPDATE groundloop_document_version SET content_hash='not-a-sha256' "
            "WHERE valid_to_epoch=%s",
            (epoch,),
        ),
    }


def test_document_retained_policy_and_registry_source_matrix(
    raw_document_database: M5RuntimeDatabase,
) -> None:
    """Eight initial legacy-source falsifiers; enclosing hashes are not authority."""
    database = raw_document_database
    connection = database.connection
    for fault in (
        "policy_json_extra",
        "policy_json_hash",
        "policy_rehashed_wrong_hash",
        "policy_version_manifest",
        "policy_spec",
        "registry_count",
        "registry_ordinal",
        "registry_hash",
    ):
        try:
            epoch, _, _ = _manual_document_delete(
                database, direct_authority_fault=fault, defer_open_commit=True
            )
            work, anchor, source = _edge_values(
                connection, epoch, "semantic_pending", 1
            )
            with pytest.raises(psycopg.Error, match="M5-D33.*policy|M5-D33.*registry"):
                _insert(connection, work, anchor, source)
        finally:
            connection.rollback()
    epoch, _, _ = _manual_document_delete(database)
    _manual_edge(connection, epoch, "semantic_pending", 1)


def test_document_readiness_identity_and_exact_byte_work_matrix(
    raw_document_database: M5RuntimeDatabase,
) -> None:
    connection = raw_document_database.connection
    epoch, _, _ = _manual_document_delete(raw_document_database)
    work, anchor, source = _edge_values(connection, epoch, "semantic_pending", 1)
    cases = [
        (work, anchor, "f" * 64),
        (
            replace(work, bytes_hashed=work.bytes_hashed - 1, work_digest=""),
            anchor,
            source,
        ),
        (
            replace(work, bytes_hashed=work.bytes_hashed + 1, work_digest=""),
            anchor,
            source,
        ),
        (
            replace(work, bytes_serialized=work.bytes_serialized - 1, work_digest=""),
            anchor,
            source,
        ),
        (
            replace(work, bytes_serialized=work.bytes_serialized + 1, work_digest=""),
            anchor,
            source,
        ),
        (replace(work, verifier_model_call_count=1, work_digest=""), anchor, source),
    ]
    for bad_work, bad_anchor, bad_source in cases:
        connection.execute("SAVEPOINT readiness_vector")
        try:
            with pytest.raises(
                psycopg.Error, match="readiness|contribution key|source|work"
            ):
                _insert(connection, bad_work, bad_anchor, bad_source)
        finally:
            connection.execute("ROLLBACK TO SAVEPOINT readiness_vector")
            connection.execute("RELEASE SAVEPOINT readiness_vector")

    class BadKeyWire:
        """Fault the negative INSERT wire, not a self-validating immutable DTO."""

        def execute(self, query: Any, parameters: Any) -> Any:
            return connection.execute(
                query, (*parameters[:-2], "f" * 64, parameters[-1])
            )

    connection.execute("SAVEPOINT readiness_bad_key")
    with pytest.raises(
        psycopg.Error, match=r"readiness identity/key/exact S\+K work is incorrect"
    ):
        _insert(BadKeyWire(), work, anchor, source)
    connection.execute("ROLLBACK TO SAVEPOINT readiness_bad_key")
    connection.execute("RELEASE SAVEPOINT readiness_bad_key")
    connection.rollback()
    _manual_edge(connection, epoch, "semantic_pending", 1)


@pytest.mark.parametrize(
    "dependency",
    (
        "published_claim_currency",
        "frontier_missing_creator",
        "frontier_wrong_policy",
    ),
)
def test_empty_direct_declaration_cannot_hide_independent_retained_sources(
    raw_document_database: M5RuntimeDatabase,
    dependency: str,
) -> None:
    """Separate predecessor source families, not oracle-labelled selection.

    Frontier negatives deliberately have no valid sealed M4 creator. They do
    not isolate the wrong-policy predicate from the fallback/declaration check.
    """
    database = raw_document_database
    epoch, _, _ = _manual_document_delete(
        database, direct_dependency=dependency, defer_open_commit=True
    )
    work, anchor, source = _edge_values(
        database.connection, epoch, "semantic_pending", 1
    )
    with pytest.raises(
        psycopg.Error, match="M5-D33.*declaration/source closure|M5-D33.*direct"
    ):
        _insert(database.connection, work, anchor, source)
    database.connection.rollback()


def test_document_predecessor_independent_eligibility_matrix(
    raw_document_database: M5RuntimeDatabase,
) -> None:
    """Ten separately rolled-back cases; one pytest case, no pooled count."""
    database = raw_document_database
    connection = database.connection
    epoch, _, _ = _manual_document_delete(database)
    work, anchor, source = _edge_values(connection, epoch, "semantic_pending", 1)
    for name, (query, params) in _source_mutations(database, epoch).items():
        connection.execute("SAVEPOINT eligibility_case")
        try:
            if name in ("counter_declaration_hash", "missing_counter"):
                # These attacks cannot reach readiness: unchanged M4 guards
                # already reject them. Not counted as a D33 validator proof.
                with pytest.raises(psycopg.Error, match="immutable|cannot be deleted"):
                    connection.execute(query, params)
                continue
            connection.execute(query, params)
            with pytest.raises(psycopg.Error, match="M5-D33") as rejected:
                _insert(connection, work, anchor, source)
            assert rejected.value is not None, name
        finally:
            connection.execute("ROLLBACK TO SAVEPOINT eligibility_case")
            connection.execute("RELEASE SAVEPOINT eligibility_case")
    connection.rollback()
    # Prove that each negative left the original valid predecessor usable.
    _manual_edge(connection, epoch, "semantic_pending", 1)


def test_document_deferred_work_revalidates_result_and_source(
    raw_document_database: M5RuntimeDatabase,
) -> None:
    database = raw_document_database
    connection = database.connection
    epoch, _, _ = _manual_document_delete(database)

    class BeforeChecksConnection:
        def __init__(self, query: str, params: tuple[Any, ...]) -> None:
            self.query = query
            self.params = params
            self.called = False

        def execute(self, query: Any, params: Any = None) -> Any:
            if query == "SET CONSTRAINTS ALL IMMEDIATE":
                self.called = True
                connection.execute(self.query, self.params)
            return connection.execute(query, params)

        def __getattr__(self, name: str) -> Any:
            return getattr(connection, name)

    # Mutate AFTER the valid BEFORE guard and header/accounting writes. This
    # specifically exercises the deferred resulting-state/source validator.
    for name in (
        "counter_revision_two",
        "counter_wrong_confirmation",
        "retained_manifest",
        "old_content_hash",
    ):
        query, params = _source_mutations(database, epoch)[name]
        wrapper = BeforeChecksConnection(query, params)
        with pytest.raises(psycopg.Error, match="M5-D33"):
            _manual_edge(wrapper, epoch, "semantic_pending", 1)
        assert wrapper.called, name
        connection.rollback()
    work, anchor, source = _edge_values(connection, epoch, "semantic_pending", 1)
    _insert(connection, work, anchor, source)
    with pytest.raises(psycopg.Error, match="exact current revision"):
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.rollback()
    _manual_edge(connection, epoch, "semantic_pending", 1)


def test_controlled_result_revalidates_withdrawn_source_and_seal_counter(
    raw_loss_database: M5RuntimeDatabase,
) -> None:
    """Source revalidation plus retained terminal immutability boundaries."""
    database = raw_loss_database
    connection = database.connection
    epoch, event, payload = _prepare_loss_edges(database)
    mutations = _source_mutations(database, epoch)
    mutations["counter_wrong_confirmation"] = (
        "UPDATE groundloop_m4_evaluation_epoch_counter SET revision=3 "
        "WHERE epoch_id=%s",
        (epoch,),
    )
    mutations["counter_revision_two"] = (
        "UPDATE groundloop_m4_evaluation_epoch_counter SET lifecycle_state='active',"
        "confirmed_as_of_epoch=%s,revision=1 WHERE epoch_id=%s",
        (database.base.epoch_id, epoch),
    )
    for name in (
        "old_content_hash",
        "retained_manifest",
        "counter_declaration_hash",
        "counter_revision_two",
        "counter_wrong_confirmation",
    ):
        query, params = mutations[name]

        def mutate(
            executor: Connection[Any],
            query: str = query,
            params: tuple[Any, ...] = params,
        ) -> None:
            executor.execute(query, params)

        expected = "immutable" if name.startswith("counter_") else "M5-D33"
        with pytest.raises(psycopg.Error, match=expected):
            _controlled_document_seal(
                database, epoch, event, payload, before_checks=mutate
            )
        connection.rollback()
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_event_result WHERE epoch_id=%s",
            (epoch,),
        ).fetchone() == (0,)
        connection.rollback()
    _controlled_document_seal(database, epoch, event, payload)


def test_guard_rejects_coherently_rehashed_omitted_group_shape(
    raw_loss_database: M5RuntimeDatabase,
) -> None:
    """Fault only the initial artifact INSERT; do not rewrite immutable rows."""
    database = raw_loss_database
    connection = database.connection
    epoch, event, payload = _manual_document_delete(database, defer_open_commit=True)
    with connection.cursor() as cursor:
        intent = authorize_and_derive_matching_transition(
            cursor,
            epoch_id=epoch,
            expected_runtime_revision=1,
            resulting_revision=1,
            source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            source_id=event,
            expected_source_identity_hash=payload,
        )
        prepared = _prepare_matching_transition(cursor, intent)
        artifact = prepared.prewrite_patch_artifact
        patch = artifact.patch
        assert artifact.group_shapes
        values = {
            "source_kind": patch.source_kind,
            "source_id": patch.source_id,
            "source_identity_hash": patch.source_identity_hash,
            "before_epoch_id": patch.before_epoch_id,
            "before_revision": patch.before_revision,
            "resulting_epoch_id": patch.resulting_epoch_id,
            "resulting_revision": patch.resulting_revision,
            "decision_policy_version": patch.decision_policy_version,
            "group_shape_set_digest": runtime_digests.matching_group_shape_set_digest(
                ()
            ),
            "observation_change_digests": patch.observation_change_digests,
            "edge_change_digests": patch.edge_change_digests,
            "mask_change_digests": patch.mask_change_digests,
            "hall_change_digests": patch.hall_change_digests,
            "logical_overlay_patch_digest_value": patch.logical_overlay_patch_digest,
            "matching_work_digest_value": patch.matching_work_digest,
        }

        class OmittedShapeCursor:
            def execute(self, query: Any, parameters: Any = None) -> Any:
                assert isinstance(query, str)
                assert "INSERT INTO groundloop_m5_matching_patch_artifact" in query
                changed = list(parameters)
                changed[0] = runtime_digests.persisted_matching_patch_digest(**values)
                changed[9] = values["group_shape_set_digest"]
                changed[10] = runtime_digests.matching_group_shape_set_preimage(())
                changed[-1] = runtime_digests.persisted_matching_patch_preimage(
                    **values
                )
                return cursor.execute(query, changed)

        with pytest.raises(psycopg.Error, match="group|shape"):
            _insert_matching_artifact(OmittedShapeCursor(), artifact)
    connection.rollback()


def test_replace_coherent_source_corruption_matrix(
    raw_document_database: M5RuntimeDatabase,
) -> None:
    """Row plus manifest attacks; no fake positive job-free REPLACE history.

    The canonical inserted image reaches the later direct-root closure check:
    actual replacement chunks require direct discovery jobs and are ineligible
    for this empty-declaration readiness fixture. Earlier source checks must
    reject malformed coherent sidecars first. This is seven matrix subcases,
    not seven separately counted pytest passes or M33/C1 acceptance.
    """
    database = raw_document_database
    connection = database.connection
    for fault in (
        "bad_inserted_hash",
        "blank_inserted_version",
        "blank_inserted_chunk",
        "unpaired_provenance",
        "changed_metadata",
        "blank_chunker",
        "canonical",
    ):
        # OPEN's exact xmin law requires the real outer transaction, not a
        # SAVEPOINT subtransaction. Roll back the complete authored case.
        connection.rollback()
        try:
            epoch, _, _ = _manual_document_delete(
                database, defer_open_commit=True, replacement_fault=fault
            )
            work, anchor, source = _edge_values(
                connection, epoch, "semantic_pending", 1
            )
            expected = (
                "retained direct declaration/source closure"
                if fault == "canonical"
                else "inserted document/metadata closure|inserted chunk source"
            )
            with pytest.raises(psycopg.Error, match=expected):
                _insert(connection, work, anchor, source)
        finally:
            connection.rollback()
    connection.rollback()


def test_source_guard_rejects_fully_coherent_group_coomission(
    raw_loss_database: M5RuntimeDatabase,
) -> None:
    """Omit EVERY affected family, not merely its shape; real source wins."""
    database = raw_loss_database
    connection = database.connection
    epoch, event, payload = _manual_document_delete(database, defer_open_commit=True)
    with connection.cursor() as cursor:
        intent = authorize_and_derive_matching_transition(
            cursor,
            epoch_id=epoch,
            expected_runtime_revision=1,
            resulting_revision=1,
            source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            source_id=event,
            expected_source_identity_hash=payload,
        )
        assert intent.group_shapes and intent.group_certificate_ids
        omitted = dict.fromkeys(
            (
                "group_shapes",
                "observation_ids",
                "edge_keys",
                "mask_keys",
                "hall_group_ids",
                "requirement_state_ids",
                "group_state_ids",
                "claim_state_ids",
                "answer_state_ids",
                "group_certificate_ids",
                "claim_certificate_ids",
            ),
            (),
        )
        values = {
            field.name: getattr(intent, field.name)
            for field in fields(intent)
            if field.name != "intent_digest"
        } | omitted
        empty_intent = replace(
            intent,
            **omitted,
            intent_digest=runtime_digests.persisted_matching_transition_intent_digest(
                **values
            ),
        )
        empty = _empty_patch_artifact(empty_intent)
        for requested_intent in (empty_intent, intent):
            connection.execute("SAVEPOINT coherent_omission")
            try:
                with pytest.raises(
                    (EventConflictError, ValidationError),
                    match="projection|intent|patch|work",
                ):
                    _prepare_matching_transition(
                        cursor,
                        requested_intent,
                        expected_patch_digest=empty.patch.patch_digest,
                        expected_work=empty.work,
                    )
            finally:
                connection.execute("ROLLBACK TO SAVEPOINT coherent_omission")
                connection.execute("RELEASE SAVEPOINT coherent_omission")
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_matching_patch_artifact "
            "WHERE resulting_epoch_id=%s",
            (epoch,),
        ).fetchone() == (0,)
    connection.rollback()
