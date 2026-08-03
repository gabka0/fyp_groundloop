"""Ordered GroundLoop PostgreSQL schema installation.

The historical ``apply_m2_schema`` name predates M3/M4 and is used widely by
tests and runtime smoke harnesses.  Its legacy contract is now deliberately
pinned to migrations 000--013.  M5 is an explicit two-member, content-ledgered
bundle and is never discovered by globbing the migrations directory.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from psycopg import Connection

from groundloop.m5.digests import hash_field, stable_m5_digest, text_field

ROOT = Path(__file__).resolve().parents[3]

LEGACY_MIGRATION_NAMES = (
    "000_extensions.sql",
    "001_m2_base.sql",
    "002_m3_static_ai.sql",
    "003_m4_dynamic_impact.sql",
    "004_m4_working_publication.sql",
    "005_m4_durable_execution.sql",
    "006_m4_role_embedding_artifacts.sql",
    "007_m4_structural_overlay.sql",
    "008_m4_atomic_discovery.sql",
    "009_m4_incremental_execution.sql",
    "010_m4_event_audit.sql",
    "011_m4_interval_exclusion.sql",
    "012_m4_point_runtime_counters.sql",
    "013_m4_evaluation_overlay.sql",
)
LEGACY_MIGRATION_PATHS = tuple(
    ROOT / "migrations" / name for name in LEGACY_MIGRATION_NAMES
)

M5_BUNDLE_ID = "m5-core-schema-bundle-v1"
M5_MIGRATION_LABEL = "migrations/014_m5_evidence_groups.sql"
M5_ORACLE_LABEL = "sql/m5/full_recompute_oracle.sql"
M5_MIGRATION_PATH = ROOT / M5_MIGRATION_LABEL
M5_ORACLE_PATH = ROOT / M5_ORACLE_LABEL

_PREREQUISITE_RELATIONS = (
    "groundloop_epoch",
    "groundloop_semantic_observation",
    "groundloop_observation_currency",
    "groundloop_m4_update",
    "groundloop_m4_publication_head",
    "groundloop_published_observation_currency",
    "groundloop_working_observation_delta",
    "groundloop_m4_working_claim_state",
    "groundloop_m4_role_embedding_artifact",
    "groundloop_m4_structural_deactivation",
    "groundloop_m4_discovery_result",
    "groundloop_m4_execution_accounting",
    "groundloop_m4_event_audit_run",
    "groundloop_m4_evaluation_epoch_counter",
)

M5_INSTALL_LOCK_RELATIONS = (
    "groundloop_epoch",
    "groundloop_m4_update",
    "groundloop_claim",
    "groundloop_semantic_observation",
    "groundloop_observation_currency",
    "groundloop_published_observation_currency",
    "groundloop_working_observation_delta",
)


class M5BundleError(RuntimeError):
    """The M5 schema bundle cannot be installed or replayed safely."""


class M5BundleHashConflictError(M5BundleError):
    """A bundle ID is already ledgered with different immutable content."""


class M5PrerequisiteError(M5BundleError):
    """The target search path is not an intact migration-013 schema."""


@dataclass(frozen=True, slots=True)
class M5BundleIdentity:
    bundle_id: str
    bundle_sha256: str
    migration_sha256: str
    oracle_sha256: str
    prerequisite_source_sha256: str


@dataclass(frozen=True, slots=True)
class M5BundleInstallResult:
    identity: M5BundleIdentity
    applied: bool


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def legacy_prerequisite_source_sha256() -> str:
    """Hash the exact ordered local 000--013 migration source set.

    GroundLoop did not historically ledger migrations 000--013. This is a
    source-set identity used for exact replay of this installer, not proof that
    an arbitrary populated database was created from byte-identical files.
    ``_verify_013_catalog_preflight`` supplies the separate live shape check.
    """

    fields: list[tuple[str, ...]] = []
    for path in LEGACY_MIGRATION_PATHS:
        relative = path.relative_to(ROOT).as_posix()
        fields.extend((text_field(relative), hash_field(_sha256(path.read_bytes()))))
    return stable_m5_digest("groundloop-legacy-schema-013-v1", *fields)


def m5_bundle_identity(
    *,
    migration_bytes: bytes | None = None,
    oracle_bytes: bytes | None = None,
) -> M5BundleIdentity:
    """Return the exact frozen identity of the ordered schema/oracle pair.

    Byte overrides exist for failure/hash-conflict tests but retain the frozen
    logical path labels.  Production callers read the checked-in files.
    """

    migration = (
        M5_MIGRATION_PATH.read_bytes() if migration_bytes is None else migration_bytes
    )
    oracle = M5_ORACLE_PATH.read_bytes() if oracle_bytes is None else oracle_bytes
    migration_hash = _sha256(migration)
    oracle_hash = _sha256(oracle)
    bundle_hash = stable_m5_digest(
        M5_BUNDLE_ID,
        text_field(M5_MIGRATION_LABEL),
        hash_field(migration_hash),
        text_field(M5_ORACLE_LABEL),
        hash_field(oracle_hash),
    )
    return M5BundleIdentity(
        bundle_id=M5_BUNDLE_ID,
        bundle_sha256=bundle_hash,
        migration_sha256=migration_hash,
        oracle_sha256=oracle_hash,
        prerequisite_source_sha256=legacy_prerequisite_source_sha256(),
    )


def apply_legacy_migrations(connection: Connection[Any]) -> None:
    """Apply exactly migrations 000--013 in their frozen order."""

    missing_files = [str(path) for path in LEGACY_MIGRATION_PATHS if not path.is_file()]
    if missing_files:
        raise FileNotFoundError(f"missing frozen legacy migrations: {missing_files}")
    for path in LEGACY_MIGRATION_PATHS:
        connection.execute(path.read_text(encoding="utf-8"))


def _relation_exists(connection: Connection[Any], relation: str) -> bool:
    row = connection.execute("SELECT to_regclass(%s)", (relation,)).fetchone()
    return row is not None and row[0] is not None


def _verify_013_catalog_preflight(connection: Connection[Any]) -> None:
    """Fail closed on missing migration-013 catalog surfaces.

    This deliberately does not claim byte provenance: no 000--013 ledger was
    present in the historical schema. The exact local source identity is
    recorded separately in the M5 bundle row.
    """

    missing = [
        relation
        for relation in _PREREQUISITE_RELATIONS
        if not _relation_exists(connection, relation)
    ]
    if missing:
        raise M5PrerequisiteError(
            "M5 requires an intact migration-013 schema; missing relations: "
            + ", ".join(missing)
        )
    extension = connection.execute(
        "SELECT extversion FROM pg_extension WHERE extname = 'btree_gist'"
    ).fetchone()
    if extension is None:
        raise M5PrerequisiteError("migration 011 btree_gist extension is missing")
    epoch_columns = connection.execute(
        """
        SELECT attname
        FROM pg_attribute
        WHERE attrelid = 'groundloop_epoch'::regclass
          AND attnum > 0
          AND NOT attisdropped
          AND attname IN ('open_job_count', 'open_scope_count')
        ORDER BY attname
        """
    ).fetchall()
    if tuple(row[0] for row in epoch_columns) != ("open_job_count", "open_scope_count"):
        raise M5PrerequisiteError("migration 012 epoch counters are missing")
    counter_columns = connection.execute(
        """
        SELECT attname
        FROM pg_attribute
        WHERE attrelid = 'groundloop_m4_evaluation_epoch_counter'::regclass
          AND attnum > 0
          AND NOT attisdropped
          AND attname IN ('declaration_hash', 'lifecycle_state', 'revision')
        ORDER BY attname
        """
    ).fetchall()
    if tuple(row[0] for row in counter_columns) != (
        "declaration_hash",
        "lifecycle_state",
        "revision",
    ):
        raise M5PrerequisiteError(
            "migration 013 evaluation-counter shape is incomplete"
        )


def _acquire_m5_install_locks(connection: Connection[Any]) -> None:
    """Serialize installers and legacy writers in the frozen migration order."""

    for relation in M5_INSTALL_LOCK_RELATIONS:
        # Names are frozen local constants, never caller input.
        connection.execute(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE")


def _read_ledger(
    connection: Connection[Any], bundle_id: str
) -> tuple[str, str, str, str] | None:
    if not _relation_exists(connection, "groundloop_m5_schema_bundle"):
        return None
    row = connection.execute(
        """
        SELECT bundle_sha256, migration_sha256, oracle_sha256,
        prerequisite_sha256
        FROM groundloop_m5_schema_bundle
        WHERE bundle_id = %s
        """,
        (bundle_id,),
    ).fetchone()
    if row is None:
        return None
    return tuple(str(value).strip() for value in row)  # type: ignore[return-value]


def install_m5_core_bundle(
    connection: Connection[Any],
    *,
    failure_injector: Callable[[str], None] | None = None,
    migration_bytes: bytes | None = None,
    oracle_bytes: bytes | None = None,
) -> M5BundleInstallResult:
    """Atomically install or exactly replay the immutable M5 core bundle."""

    identity = m5_bundle_identity(
        migration_bytes=migration_bytes,
        oracle_bytes=oracle_bytes,
    )
    migration = (
        M5_MIGRATION_PATH.read_bytes() if migration_bytes is None else migration_bytes
    )
    oracle = M5_ORACLE_PATH.read_bytes() if oracle_bytes is None else oracle_bytes

    with connection.transaction():
        _verify_013_catalog_preflight(connection)
        # This must precede the first ledger read. Otherwise two installers can
        # both observe an empty ledger before migration 014 takes its own
        # (reentrant) locks, and the loser attempts duplicate CREATE statements.
        _acquire_m5_install_locks(connection)
        ledger = _read_ledger(connection, identity.bundle_id)
        if ledger is not None:
            expected = (
                identity.bundle_sha256,
                identity.migration_sha256,
                identity.oracle_sha256,
                identity.prerequisite_source_sha256,
            )
            if ledger != expected:
                raise M5BundleHashConflictError(
                    f"bundle {identity.bundle_id} is already ledgered with "
                    "different content"
                )
            return M5BundleInstallResult(identity=identity, applied=False)

        connection.execute(migration.decode("utf-8"))
        if failure_injector is not None:
            failure_injector("after_schema")
        connection.execute(oracle.decode("utf-8"))
        if failure_injector is not None:
            failure_injector("after_oracle")
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        if failure_injector is not None:
            failure_injector("before_ledger")
        connection.execute(
            """
            INSERT INTO groundloop_m5_schema_bundle (
                bundle_id, bundle_sha256, migration_sha256, oracle_sha256,
                prerequisite_sha256, applied_at
            ) VALUES (%s, %s, %s, %s, %s, now())
            """,
            (
                identity.bundle_id,
                identity.bundle_sha256,
                identity.migration_sha256,
                identity.oracle_sha256,
                identity.prerequisite_source_sha256,
            ),
        )
        return M5BundleInstallResult(identity=identity, applied=True)


__all__ = [
    "LEGACY_MIGRATION_NAMES",
    "LEGACY_MIGRATION_PATHS",
    "M5_BUNDLE_ID",
    "M5_INSTALL_LOCK_RELATIONS",
    "M5BundleError",
    "M5BundleHashConflictError",
    "M5BundleIdentity",
    "M5BundleInstallResult",
    "M5PrerequisiteError",
    "apply_legacy_migrations",
    "install_m5_core_bundle",
    "legacy_prerequisite_source_sha256",
    "m5_bundle_identity",
]
