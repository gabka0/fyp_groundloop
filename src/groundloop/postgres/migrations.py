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

from psycopg import Connection, IsolationLevel
from psycopg.pq import TransactionStatus

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

M5_RUNTIME_BUNDLE_ID = "m5-runtime-schema-bundle-v2"
M5_RUNTIME_MIGRATION_LABEL = "migrations/015_m5_runtime.sql"
M5_RUNTIME_MIGRATION_PATH = ROOT / M5_RUNTIME_MIGRATION_LABEL
M5_RUNTIME_ORACLE_SHA256 = hashlib.sha256(b"").hexdigest()

# M5-D24 deliberately pins the accepted migration-015 ledger bytes.  Do not
# derive these values from the local 015 files: doing so would turn a changed
# historical prerequisite into a new accepted prerequisite.
M5_ACCEPTED_RUNTIME_BUNDLE_ID = "m5-runtime-schema-bundle-v2"
M5_ACCEPTED_RUNTIME_MIGRATION_SHA256 = (
    "85cb7f8e6a33273ce67fc6b4160e74a3aff314cd084df7ac3647cadae930185c"
)
M5_ACCEPTED_RUNTIME_BUNDLE_SHA256 = (
    "b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd"
)
M5_ACCEPTED_RUNTIME_ORACLE_SHA256 = (
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)
M5_ACCEPTED_RUNTIME_PREREQUISITE_SHA256 = (
    "9bce4ae68a434aefbc196b6367b77454f4ec65d531f4ff206f37fb4d69e9167a"
)

M5_RUNTIME_RECOVERY_BUNDLE_ID = "m5-runtime-recovery-schema-bundle-v1"
M5_RUNTIME_RECOVERY_MIGRATION_LABEL = "migrations/016_m5_runtime_recovery.sql"
M5_RUNTIME_RECOVERY_MIGRATION_PATH = ROOT / M5_RUNTIME_RECOVERY_MIGRATION_LABEL
M5_RUNTIME_RECOVERY_ORACLE_SHA256 = hashlib.sha256(b"").hexdigest()

# M5-D25 deliberately pins the accepted migration-016 ledger bytes.  These
# literals are authority: never derive them from whichever 016 happens to be
# present in the checkout.
M5_ACCEPTED_RECOVERY_BUNDLE_ID = "m5-runtime-recovery-schema-bundle-v1"
M5_ACCEPTED_RECOVERY_MIGRATION_SHA256 = (
    "a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7"
)
M5_ACCEPTED_RECOVERY_BUNDLE_SHA256 = (
    "28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565"
)
M5_ACCEPTED_RECOVERY_ORACLE_SHA256 = (
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)
M5_ACCEPTED_RECOVERY_PREREQUISITE_SHA256 = (
    "b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd"
)

M5_PERSISTED_MATCHING_BUNDLE_ID = "m5-persisted-matching-schema-bundle-v1"
M5_PERSISTED_MATCHING_MIGRATION_LABEL = "migrations/017_m5_persisted_matching.sql"
M5_PERSISTED_MATCHING_MIGRATION_PATH = ROOT / M5_PERSISTED_MATCHING_MIGRATION_LABEL
M5_PERSISTED_MATCHING_ORACLE_SHA256 = hashlib.sha256(b"").hexdigest()

M5_PERSISTED_MATCHING_INSTALL_LOCK_RELATIONS = (
    "groundloop_runtime_mode",
    "groundloop_m4_publication_head",
    "groundloop_m5_publication_head",
    "groundloop_m5_activation",
    "groundloop_epoch",
    "groundloop_m5_runtime_epoch",
    "groundloop_m5_update",
    "groundloop_decision_policy",
    "groundloop_document",
    "groundloop_document_version",
    "groundloop_chunk_version",
    "groundloop_answer_version",
    "groundloop_claim",
    "groundloop_m5_group_family",
    "groundloop_m5_group_version",
    "groundloop_m5_requirement_version",
    "groundloop_m5_group_validity",
    "groundloop_m5_group_deactivation",
    "groundloop_m5_group_family_retirement",
    "groundloop_semantic_subject",
    "groundloop_semantic_observation",
    "groundloop_observation_currency",
    "groundloop_published_observation_currency",
    "groundloop_claim_state_materialized",
    "groundloop_answer_state_materialized",
    "groundloop_claim_certificate",
    "groundloop_published_claim_state",
    "groundloop_published_answer_state",
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


@dataclass(frozen=True, slots=True)
class _ExpectedLegacyTrigger:
    relation: str
    trigger: str
    function: str
    type_bits: int
    update_columns: tuple[str, ...] = ()
    is_constraint: bool = False
    deferrable: bool = False
    initially_deferred: bool = False


# Exact non-internal trigger sets for legacy surfaces whose mutation semantics
# protect historical M4 truth, fields extended by migration 014, or invariants
# consumed by the M5 oracle. This is deliberately a critical-surface manifest,
# not a claim that every trigger in migrations 000--013 is byte-proven.
# PostgreSQL's tgtype bit mask captures ROW/BEFORE/AFTER and
# INSERT/UPDATE/DELETE shape; tgattr separately captures UPDATE OF column
# restrictions.
_CRITICAL_013_TRIGGERS: tuple[_ExpectedLegacyTrigger, ...] = (
    _ExpectedLegacyTrigger(
        "groundloop_answer_version",
        "groundloop_answer_requires_claim",
        "groundloop_required_claim_constraint_trigger",
        21,
        is_constraint=True,
        deferrable=True,
        initially_deferred=True,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_claim",
        "groundloop_claim_preserves_required_claim",
        "groundloop_required_claim_constraint_trigger",
        29,
        is_constraint=True,
        deferrable=True,
        initially_deferred=True,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_semantic_observation",
        "groundloop_semantic_observation_immutable",
        "groundloop_reject_immutable_ai_update",
        27,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_working_observation_delta",
        "groundloop_working_observation_delta_immutable",
        "groundloop_reject_immutable_working_delta",
        27,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_m4_working_claim_state",
        "groundloop_m4_working_claim_state_transition",
        "groundloop_validate_m4_working_state_mutation",
        31,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_m4_working_answer_state",
        "groundloop_m4_working_answer_state_transition",
        "groundloop_validate_m4_working_state_mutation",
        31,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_m4_claim_admission_index",
        "groundloop_m4_claim_admission_validate",
        "groundloop_validate_m4_claim_admission_row",
        7,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_m4_claim_admission_index",
        "groundloop_m4_claim_admission_immutable",
        "groundloop_reject_immutable_m4_update",
        27,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_m4_verification_execution",
        "groundloop_m4_verification_execution_validate",
        "groundloop_validate_m4_verification_execution",
        7,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_m4_verification_execution",
        "groundloop_m4_verification_execution_immutable",
        "groundloop_reject_immutable_m4_update",
        27,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_m4_role_embedding_artifact",
        "groundloop_m4_role_embedding_validate",
        "groundloop_validate_m4_role_embedding_artifact",
        7,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_m4_role_embedding_artifact",
        "groundloop_m4_role_embedding_immutable",
        "groundloop_reject_immutable_m4_update",
        27,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_m4_document_metadata_overlay",
        "groundloop_m4_document_metadata_overlay_immutable",
        "groundloop_reject_immutable_m4_update",
        27,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_m4_claim_registry_member",
        "groundloop_m4_claim_registry_member_immutable",
        "groundloop_reject_immutable_m4_update",
        27,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_m4_structural_deactivation",
        "groundloop_m4_structural_deactivation_immutable",
        "groundloop_reject_immutable_structural_deactivation",
        27,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_m4_discovery_result",
        "groundloop_m4_discovery_result_immutable",
        "groundloop_reject_immutable_m4_update",
        27,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_m4_claim_registry_snapshot",
        "groundloop_m4_claim_registry_snapshot_immutable",
        "groundloop_reject_immutable_m4_update",
        27,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_m4_event_audit_run",
        "groundloop_m4_event_audit_run_immutable",
        "groundloop_reject_immutable_m4_update",
        27,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_impact_evaluation_run",
        "groundloop_completed_event_audit_envelope_immutable",
        "groundloop_reject_completed_event_audit_envelope_update",
        27,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_semantic_job",
        "groundloop_semantic_job_transition",
        "groundloop_validate_semantic_job_transition",
        19,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_semantic_job",
        "groundloop_semantic_job_open_count",
        "groundloop_adjust_open_job_count",
        29,
        ("job_state",),
    ),
    _ExpectedLegacyTrigger(
        "groundloop_discovery_scope",
        "groundloop_discovery_scope_open_count",
        "groundloop_adjust_open_scope_count",
        29,
        ("closed_revision",),
    ),
    _ExpectedLegacyTrigger(
        "groundloop_m4_evaluation_epoch_counter",
        "groundloop_m4_evaluation_epoch_counter_guard",
        "groundloop_validate_m4_evaluation_epoch_counter_change",
        27,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_m4_evaluation_counter_transition",
        "groundloop_m4_evaluation_counter_transition_immutable",
        "groundloop_reject_m4_evaluation_transition_change",
        27,
    ),
)
_CRITICAL_013_TRIGGER_RELATIONS = tuple(
    dict.fromkeys(trigger.relation for trigger in _CRITICAL_013_TRIGGERS)
)
_M5_TRIGGER_EXTENSIONS_ON_CRITICAL_RELATIONS: tuple[_ExpectedLegacyTrigger, ...] = (
    _ExpectedLegacyTrigger(
        "groundloop_claim",
        "groundloop_claim_registers_semantic_subject",
        "groundloop_m5_register_claim_subject",
        5,
    ),
    _ExpectedLegacyTrigger(
        "groundloop_working_observation_delta",
        "groundloop_working_observation_delta_eligible",
        "groundloop_m5_validate_currency_holder",
        23,
    ),
)
M5_CATALOG_PREFLIGHT_ROW_EXCLUSIVE_RELATIONS = tuple(
    relation
    for relation in _CRITICAL_013_TRIGGER_RELATIONS
    if relation not in M5_INSTALL_LOCK_RELATIONS
)

M5_RUNTIME_INSTALL_LOCK_RELATIONS = (
    "groundloop_runtime_mode",
    "groundloop_m4_publication_head",
    "groundloop_m5_publication_head",
    "groundloop_epoch",
)

M5_RUNTIME_RECOVERY_INSTALL_LOCK_RELATIONS = (
    "groundloop_runtime_mode",
    "groundloop_m4_publication_head",
    "groundloop_m5_publication_head",
    "groundloop_epoch",
    "groundloop_m5_runtime_epoch",
    "groundloop_semantic_job_attempt",
    "groundloop_m5_job_attempt",
)

_M5_RUNTIME_RECOVERY_PREREQUISITE_RELATIONS = (
    "groundloop_m5_schema_bundle",
    "groundloop_runtime_mode",
    "groundloop_m4_publication_head",
    "groundloop_m5_publication_head",
    "groundloop_epoch",
    "groundloop_m5_runtime_epoch",
    "groundloop_semantic_job",
    "groundloop_semantic_job_attempt",
    "groundloop_m5_semantic_job",
    "groundloop_m5_job_attempt",
    "groundloop_m5_attempt_result_artifact",
    "groundloop_m5_runtime_work",
    "groundloop_m5_event_result",
)

_M5_RUNTIME_PREREQUISITE_RELATIONS = (
    "groundloop_m5_schema_bundle",
    "groundloop_runtime_mode",
    "groundloop_m4_publication_head",
    "groundloop_m5_publication_head",
    "groundloop_m5_activation",
    "groundloop_semantic_subject",
    "groundloop_m5_update",
    "groundloop_m5_group_family",
    "groundloop_m5_group_version",
    "groundloop_m5_requirement_version",
    "groundloop_m5_working_currency_history",
    "groundloop_m5_working_requirement_state",
    "groundloop_m5_working_group_state",
    "groundloop_m5_working_claim_state",
    "groundloop_m5_working_answer_state",
    "groundloop_m5_published_requirement_state",
    "groundloop_m5_published_group_state",
    "groundloop_m5_published_claim_state",
    "groundloop_m5_published_answer_state",
    "groundloop_m5_group_certificate_artifact",
    "groundloop_m5_claim_certificate_artifact",
)


class M5BundleError(RuntimeError):
    """The M5 schema bundle cannot be installed or replayed safely."""


class M5BundleHashConflictError(M5BundleError):
    """A bundle ID is already ledgered with different immutable content."""


class M5PrerequisiteError(M5BundleError):
    """The target search path is not an intact migration-013 schema."""


class M5RuntimeBundleError(M5BundleError):
    """The M5 runtime schema bundle cannot be installed or replayed safely."""


class M5RuntimeRecoveryBundleError(M5RuntimeBundleError):
    """Migration 016 cannot be installed or replayed safely."""


class M5PersistedMatchingBundleError(M5RuntimeBundleError):
    """Migration 017 cannot be installed or replayed safely."""


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


@dataclass(frozen=True, slots=True)
class M5RuntimeBundleIdentity:
    bundle_id: str
    bundle_sha256: str
    migration_sha256: str
    oracle_sha256: str
    prerequisite_sha256: str


@dataclass(frozen=True, slots=True)
class M5RuntimeBundleInstallResult:
    identity: M5RuntimeBundleIdentity
    applied: bool


@dataclass(frozen=True, slots=True)
class M5RuntimeRecoveryBundleIdentity:
    bundle_id: str
    bundle_sha256: str
    migration_sha256: str
    oracle_sha256: str
    prerequisite_sha256: str


@dataclass(frozen=True, slots=True)
class M5RuntimeRecoveryBundleInstallResult:
    identity: M5RuntimeRecoveryBundleIdentity
    applied: bool


@dataclass(frozen=True, slots=True)
class M5PersistedMatchingBundleIdentity:
    bundle_id: str
    bundle_sha256: str
    migration_sha256: str
    oracle_sha256: str
    prerequisite_sha256: str


@dataclass(frozen=True, slots=True)
class M5PersistedMatchingBundleInstallResult:
    identity: M5PersistedMatchingBundleIdentity
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


def m5_runtime_bundle_identity(
    *,
    migration_bytes: bytes | None = None,
    core_identity: M5BundleIdentity | None = None,
) -> M5RuntimeBundleIdentity:
    """Return the exact migration-015 identity bound to the local 014 bundle."""

    migration = (
        M5_RUNTIME_MIGRATION_PATH.read_bytes()
        if migration_bytes is None
        else migration_bytes
    )
    accepted_core = m5_bundle_identity() if core_identity is None else core_identity
    migration_hash = _sha256(migration)
    bundle_hash = stable_m5_digest(
        M5_RUNTIME_BUNDLE_ID,
        text_field(M5_RUNTIME_MIGRATION_LABEL),
        hash_field(migration_hash),
        hash_field(accepted_core.bundle_sha256),
    )
    return M5RuntimeBundleIdentity(
        bundle_id=M5_RUNTIME_BUNDLE_ID,
        bundle_sha256=bundle_hash,
        migration_sha256=migration_hash,
        oracle_sha256=M5_RUNTIME_ORACLE_SHA256,
        prerequisite_sha256=accepted_core.bundle_sha256,
    )


def m5_runtime_recovery_bundle_identity(
    *,
    migration_bytes: bytes | None = None,
) -> M5RuntimeRecoveryBundleIdentity:
    """Return the migration-016 identity bound to the pinned accepted 015.

    The prerequisite is intentionally a frozen constant rather than the local
    migration-015 identity.  M5-D24 forbids silently accepting changed 015
    source or ledger bytes.
    """

    migration = (
        M5_RUNTIME_RECOVERY_MIGRATION_PATH.read_bytes()
        if migration_bytes is None
        else migration_bytes
    )
    migration_hash = _sha256(migration)
    bundle_hash = stable_m5_digest(
        M5_RUNTIME_RECOVERY_BUNDLE_ID,
        text_field(M5_RUNTIME_RECOVERY_MIGRATION_LABEL),
        hash_field(migration_hash),
        hash_field(M5_ACCEPTED_RUNTIME_BUNDLE_SHA256),
    )
    return M5RuntimeRecoveryBundleIdentity(
        bundle_id=M5_RUNTIME_RECOVERY_BUNDLE_ID,
        bundle_sha256=bundle_hash,
        migration_sha256=migration_hash,
        oracle_sha256=M5_RUNTIME_RECOVERY_ORACLE_SHA256,
        prerequisite_sha256=M5_ACCEPTED_RUNTIME_BUNDLE_SHA256,
    )


def m5_persisted_matching_bundle_identity(
    *, migration_bytes: bytes | None = None
) -> M5PersistedMatchingBundleIdentity:
    """Return migration 017's identity bound to the accepted migration 016."""

    migration = (
        M5_PERSISTED_MATCHING_MIGRATION_PATH.read_bytes()
        if migration_bytes is None
        else migration_bytes
    )
    migration_hash = _sha256(migration)
    bundle_hash = stable_m5_digest(
        M5_PERSISTED_MATCHING_BUNDLE_ID,
        text_field(M5_PERSISTED_MATCHING_MIGRATION_LABEL),
        hash_field(migration_hash),
        hash_field(M5_ACCEPTED_RECOVERY_BUNDLE_SHA256),
    )
    return M5PersistedMatchingBundleIdentity(
        bundle_id=M5_PERSISTED_MATCHING_BUNDLE_ID,
        bundle_sha256=bundle_hash,
        migration_sha256=migration_hash,
        oracle_sha256=M5_PERSISTED_MATCHING_ORACLE_SHA256,
        prerequisite_sha256=M5_ACCEPTED_RECOVERY_BUNDLE_SHA256,
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


def _verify_013_relations(connection: Connection[Any]) -> None:
    """Reject missing required relations before issuing the frozen install locks."""

    missing = tuple(
        relation
        for relation in dict.fromkeys(
            _PREREQUISITE_RELATIONS + _CRITICAL_013_TRIGGER_RELATIONS
        )
        if not _relation_exists(connection, relation)
    )
    if missing:
        raise M5PrerequisiteError(
            "M5 requires its migration-013 prerequisite relations; missing: "
            + ", ".join(missing)
        )


def _verify_013_critical_triggers(connection: Connection[Any]) -> None:
    current_schema_row = connection.execute("SELECT current_schema()").fetchone()
    if current_schema_row is None or current_schema_row[0] is None:
        raise M5PrerequisiteError("migration-013 preflight has no current schema")
    current_schema = str(current_schema_row[0])
    expected_triggers = _CRITICAL_013_TRIGGERS
    if _relation_exists(connection, "groundloop_m5_schema_bundle"):
        # Exact replay sees migration 014's two deliberate trigger extensions
        # on legacy critical relations. No other extra trigger is accepted.
        expected_triggers += _M5_TRIGGER_EXTENSIONS_ON_CRITICAL_RELATIONS
    expected = {
        (
            current_schema,
            trigger.relation,
            trigger.trigger,
            "O",
            trigger.is_constraint,
            trigger.deferrable,
            trigger.initially_deferred,
            current_schema,
            trigger.function,
            trigger.type_bits,
            trigger.update_columns,
            "",
            None,
            None,
            None,
        )
        for trigger in expected_triggers
    }
    rows = connection.execute(
        """
        SELECT relation_namespace.nspname,
               relation.relname,
               trigger_row.tgname,
               trigger_row.tgenabled,
               trigger_row.tgconstraint <> 0,
               trigger_row.tgdeferrable,
               trigger_row.tginitdeferred,
               function_namespace.nspname,
               function_row.proname,
               trigger_row.tgtype::integer,
               COALESCE(
                   (
                       SELECT array_agg(
                           attribute_row.attname ORDER BY position.ordinality
                       )
                       FROM unnest(trigger_row.tgattr::smallint[])
                            WITH ORDINALITY AS position(attnum, ordinality)
                       JOIN pg_attribute AS attribute_row
                         ON attribute_row.attrelid = trigger_row.tgrelid
                        AND attribute_row.attnum = position.attnum
                   ),
                   ARRAY[]::name[]
               ),
               encode(trigger_row.tgargs, 'hex'),
               pg_get_expr(trigger_row.tgqual, trigger_row.tgrelid, true),
               trigger_row.tgoldtable,
               trigger_row.tgnewtable
        FROM pg_trigger AS trigger_row
        JOIN pg_class AS relation
          ON relation.oid = trigger_row.tgrelid
        JOIN pg_namespace AS relation_namespace
          ON relation_namespace.oid = relation.relnamespace
        JOIN pg_proc AS function_row
          ON function_row.oid = trigger_row.tgfoid
        JOIN pg_namespace AS function_namespace
          ON function_namespace.oid = function_row.pronamespace
        WHERE NOT trigger_row.tgisinternal
          AND trigger_row.tgrelid IN (
              SELECT to_regclass(requested.relation_name)
              FROM unnest(%s::text[]) AS requested(relation_name)
          )
        ORDER BY relation_namespace.nspname, relation.relname, trigger_row.tgname
        """,
        (list(_CRITICAL_013_TRIGGER_RELATIONS),),
    ).fetchall()
    actual = {
        (
            str(row[0]),
            str(row[1]),
            str(row[2]),
            str(row[3]),
            bool(row[4]),
            bool(row[5]),
            bool(row[6]),
            str(row[7]),
            str(row[8]),
            int(row[9]),
            tuple(str(value) for value in row[10]),
            str(row[11]),
            None if row[12] is None else str(row[12]),
            None if row[13] is None else str(row[13]),
            None if row[14] is None else str(row[14]),
        )
        for row in rows
    }
    if actual != expected:
        missing = sorted(expected - actual)
        changed_or_unexpected = sorted(actual - expected)
        raise M5PrerequisiteError(
            "migration-013 critical trigger catalog is not exact; "
            f"missing_or_changed={missing!r}; unexpected_or_changed="
            f"{changed_or_unexpected!r}"
        )


def _verify_013_catalog_preflight(connection: Connection[Any]) -> None:
    """Fail closed on migration-013 critical semantic catalog drift.

    This deliberately does not claim byte provenance: no 000--013 ledger was
    present in the historical schema. The exact local source identity is
    recorded separately in the M5 bundle row.
    """

    _verify_013_relations(connection)
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
    _verify_013_critical_triggers(connection)


def _acquire_m5_install_locks(connection: Connection[Any]) -> None:
    """Serialize install and critical-catalog checks in deterministic order."""

    for relation in M5_INSTALL_LOCK_RELATIONS:
        # Names are frozen local constants, never caller input.
        connection.execute(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE")
    # ROW EXCLUSIVE is compatible with ordinary DML's own ROW EXCLUSIVE lock,
    # but conflicts with trigger-changing SHARE ROW EXCLUSIVE (and stronger)
    # DDL. This stabilizes the extra catalog-only surfaces without expanding
    # the frozen seven-table writer freeze above.
    for relation in M5_CATALOG_PREFLIGHT_ROW_EXCLUSIVE_RELATIONS:
        connection.execute(f"LOCK TABLE {relation} IN ROW EXCLUSIVE MODE")


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
        _verify_013_relations(connection)
        # This must precede the first ledger read. Otherwise two installers can
        # both observe an empty ledger before migration 014 takes its own
        # (reentrant) locks, and the loser attempts duplicate CREATE statements.
        _acquire_m5_install_locks(connection)
        _verify_013_catalog_preflight(connection)
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


def _verify_m5_core_bundle_prerequisite(connection: Connection[Any]) -> None:
    missing = tuple(
        relation
        for relation in _M5_RUNTIME_PREREQUISITE_RELATIONS
        if not _relation_exists(connection, relation)
    )
    if missing:
        raise M5PrerequisiteError(
            "M5 runtime bundle requires migration-014 relations; missing: "
            + ", ".join(missing)
        )

    expected_core = m5_bundle_identity()
    actual_core = _read_ledger(connection, expected_core.bundle_id)
    expected_row = (
        expected_core.bundle_sha256,
        expected_core.migration_sha256,
        expected_core.oracle_sha256,
        expected_core.prerequisite_source_sha256,
    )
    if actual_core is None:
        raise M5PrerequisiteError(
            "M5 runtime bundle requires the accepted migration-014 ledger row"
        )
    if actual_core != expected_row:
        raise M5PrerequisiteError(
            "M5 runtime bundle requires the exact accepted migration-014 bundle"
        )

    immutable_trigger = connection.execute(
        """
        SELECT trigger_row.tgenabled, function_row.proname
        FROM pg_trigger AS trigger_row
        JOIN pg_proc AS function_row ON function_row.oid = trigger_row.tgfoid
        WHERE trigger_row.tgrelid = 'groundloop_m5_schema_bundle'::regclass
          AND trigger_row.tgname = 'groundloop_m5_schema_bundle_immutable'
          AND NOT trigger_row.tgisinternal
        """
    ).fetchone()
    if immutable_trigger != ("O", "groundloop_m5_reject_immutable_row"):
        raise M5PrerequisiteError(
            "migration-014 bundle ledger immutability trigger is missing or changed"
        )

    mode_rows = connection.execute(
        """
        SELECT singleton, mode, mode_revision
        FROM groundloop_runtime_mode
        ORDER BY singleton
        """
    ).fetchall()
    if len(mode_rows) != 1 or tuple(mode_rows[0][:2]) not in {
        (True, "v1_only"),
        (True, "m5_active"),
    }:
        raise M5PrerequisiteError("migration-014 runtime-mode singleton is invalid")


def _acquire_m5_runtime_install_locks(connection: Connection[Any]) -> None:
    """Serialize 015 installation in the frozen runtime lock order."""

    for relation in M5_RUNTIME_INSTALL_LOCK_RELATIONS:
        # Names are frozen local constants, never caller input.
        connection.execute(f"LOCK TABLE {relation} IN SHARE ROW EXCLUSIVE MODE")


def _m5_runtime_install_guard_snapshot(
    connection: Connection[Any],
) -> tuple[tuple[object, ...], tuple[tuple[object, ...], ...]]:
    mode = connection.execute(
        """
        SELECT singleton, mode, mode_revision, updated_at
        FROM groundloop_runtime_mode
        WHERE singleton
        """
    ).fetchone()
    if mode is None:
        raise M5PrerequisiteError("migration-014 runtime-mode singleton is missing")
    singleton_rows = connection.execute(
        """
        SELECT surface, singleton, identity, revision, audit_time
        FROM (
            SELECT 'm4_head'::text AS surface,
                   head.singleton,
                   head.epoch_id::text AS identity,
                   epoch.revision AS revision,
                   head.updated_at AS audit_time
            FROM groundloop_m4_publication_head AS head
            JOIN groundloop_epoch AS epoch ON epoch.epoch_id = head.epoch_id
            UNION ALL
            SELECT 'm5_head'::text,
                   singleton,
                   epoch_id::text,
                   sealed_revision,
                   updated_at
            FROM groundloop_m5_publication_head
            UNION ALL
            SELECT 'activation'::text,
                   singleton,
                   activation_id,
                   base_m4_epoch_id,
                   activated_at
            FROM groundloop_m5_activation
        ) AS protected_surface
        ORDER BY surface, identity
        """
    ).fetchall()
    return tuple(mode), tuple(tuple(row) for row in singleton_rows)


def install_m5_runtime_bundle(
    connection: Connection[Any],
    *,
    failure_injector: Callable[[str], None] | None = None,
    migration_bytes: bytes | None = None,
) -> M5RuntimeBundleInstallResult:
    """Atomically install or exactly replay migration 015.

    The runtime bundle is content-bound to the exact local 014 schema/oracle
    identity.  It neither changes runtime mode nor activates M5.
    """

    identity = m5_runtime_bundle_identity(migration_bytes=migration_bytes)
    migration = (
        M5_RUNTIME_MIGRATION_PATH.read_bytes()
        if migration_bytes is None
        else migration_bytes
    )

    with connection.transaction():
        # Validate the accepted 014 ledger and its consumed relations before
        # the first statement from migration 015 can execute.
        _verify_m5_core_bundle_prerequisite(connection)
        _acquire_m5_runtime_install_locks(connection)
        _verify_m5_core_bundle_prerequisite(connection)

        ledger = _read_ledger(connection, identity.bundle_id)
        if ledger is not None:
            expected = (
                identity.bundle_sha256,
                identity.migration_sha256,
                identity.oracle_sha256,
                identity.prerequisite_sha256,
            )
            if ledger != expected:
                raise M5BundleHashConflictError(
                    f"bundle {identity.bundle_id} is already ledgered with "
                    "different content"
                )
            return M5RuntimeBundleInstallResult(identity=identity, applied=False)

        live_epoch_row = connection.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM groundloop_epoch
                WHERE structural_status = 'committed'
                  AND semantic_status IN ('pending', 'complete')
            )
            """
        ).fetchone()
        if live_epoch_row is None:
            raise M5RuntimeBundleError("live-epoch guard returned no row")
        if bool(live_epoch_row[0]):
            raise M5RuntimeBundleError(
                "M5 runtime bundle requires no committed pending/complete epoch"
            )

        protected_before = _m5_runtime_install_guard_snapshot(connection)
        connection.execute(migration.decode("utf-8"))
        if failure_injector is not None:
            failure_injector("after_schema")
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        if failure_injector is not None:
            failure_injector("after_constraints")
        if _m5_runtime_install_guard_snapshot(connection) != protected_before:
            raise M5RuntimeBundleError(
                "migration 015 changed runtime mode, activation, or a publication head"
            )
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
                identity.prerequisite_sha256,
            ),
        )
        if failure_injector is not None:
            failure_injector("after_ledger")
        return M5RuntimeBundleInstallResult(identity=identity, applied=True)


def _verify_accepted_m5_runtime_bundle(connection: Connection[Any]) -> None:
    """Require the exact five-field migration-015 ledger prerequisite."""

    missing = tuple(
        relation
        for relation in _M5_RUNTIME_RECOVERY_PREREQUISITE_RELATIONS
        if not _relation_exists(connection, relation)
    )
    if missing:
        raise M5PrerequisiteError(
            "M5 runtime recovery bundle requires migration-015 relations; missing: "
            + ", ".join(missing)
        )

    row = connection.execute(
        """
        SELECT bundle_id, migration_sha256, bundle_sha256, oracle_sha256,
               prerequisite_sha256
        FROM groundloop_m5_schema_bundle
        WHERE bundle_id = %s
        """,
        (M5_ACCEPTED_RUNTIME_BUNDLE_ID,),
    ).fetchone()
    expected = (
        M5_ACCEPTED_RUNTIME_BUNDLE_ID,
        M5_ACCEPTED_RUNTIME_MIGRATION_SHA256,
        M5_ACCEPTED_RUNTIME_BUNDLE_SHA256,
        M5_ACCEPTED_RUNTIME_ORACLE_SHA256,
        M5_ACCEPTED_RUNTIME_PREREQUISITE_SHA256,
    )
    actual = None if row is None else tuple(str(value).strip() for value in row)
    if actual != expected:
        raise M5PrerequisiteError(
            "M5 runtime recovery bundle requires the exact accepted "
            "five-field migration-015 ledger row"
        )


def _acquire_m5_runtime_recovery_install_locks(
    connection: Connection[Any],
) -> None:
    """Serialize migration 016 in the frozen M5-D24 lock order."""

    for relation in M5_RUNTIME_RECOVERY_INSTALL_LOCK_RELATIONS:
        # Names are frozen local constants, never caller input.
        connection.execute(f"LOCK TABLE {relation} IN SHARE ROW EXCLUSIVE MODE")


_M5_RUNTIME_RECOVERY_GROUP_PREFIX = "-- groundloop:m5-runtime-recovery-group:"


def _m5_runtime_recovery_migration_groups(
    migration: bytes,
) -> tuple[tuple[str, str], ...]:
    """Split the audited 016 source at explicit transaction test barriers."""

    source = migration.decode("utf-8")
    groups: list[tuple[str, str]] = []
    current_name: str | None = None
    current_lines: list[str] = []
    preamble: list[str] = []
    for line in source.splitlines(keepends=True):
        if line.startswith(_M5_RUNTIME_RECOVERY_GROUP_PREFIX):
            if current_name is not None:
                groups.append((current_name, "".join(current_lines)))
            elif any(
                value.strip() and not value.lstrip().startswith("--")
                for value in preamble
            ):
                raise M5RuntimeRecoveryBundleError(
                    "migration 016 contains executable SQL before its first group"
                )
            current_name = line[len(_M5_RUNTIME_RECOVERY_GROUP_PREFIX) :].strip()
            if not current_name or not current_name.replace("_", "").isalnum():
                raise M5RuntimeRecoveryBundleError(
                    "migration 016 has an invalid group marker"
                )
            current_lines = [line]
        elif current_name is None:
            preamble.append(line)
        else:
            current_lines.append(line)
    if current_name is not None:
        groups.append((current_name, "".join(current_lines)))
    if not groups or any(not sql_source.strip() for _, sql_source in groups):
        raise M5RuntimeRecoveryBundleError(
            "migration 016 has no executable migration groups"
        )
    names = tuple(name for name, _ in groups)
    if len(set(names)) != len(names):
        raise M5RuntimeRecoveryBundleError("migration 016 group markers must be unique")
    return tuple(groups)


def _m5_runtime_recovery_first_install_guard(
    connection: Connection[Any],
) -> None:
    live_epoch = connection.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM groundloop_epoch
            WHERE structural_status = 'committed'
              AND semantic_status IN ('pending', 'complete')
        )
        """
    ).fetchone()
    if live_epoch is None:
        raise M5RuntimeRecoveryBundleError(
            "migration-016 live-epoch guard returned no row"
        )
    if bool(live_epoch[0]):
        raise M5RuntimeRecoveryBundleError(
            "M5 runtime recovery bundle requires no committed pending/complete epoch"
        )

    if connection.execute(
        "SELECT EXISTS (SELECT 1 FROM groundloop_m5_job_attempt)"
    ).fetchone() != (False,):
        raise M5RuntimeRecoveryBundleError(
            "M5 runtime recovery bundle forbids an existing M5 attempt"
        )
    if connection.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM groundloop_semantic_job_attempt AS attempt
            JOIN groundloop_semantic_job AS job ON job.job_id = attempt.job_id
            JOIN groundloop_m5_runtime_epoch AS runtime_epoch
              ON runtime_epoch.epoch_id = job.epoch_id
        )
        """
    ).fetchone() != (False,):
        raise M5RuntimeRecoveryBundleError(
            "M5 runtime recovery bundle forbids an existing typed-direct attempt"
        )

    terminal_history = connection.execute(
        """
        SELECT
            EXISTS (
                SELECT 1
                FROM groundloop_m5_runtime_epoch
                WHERE runtime_state IN ('sealed', 'failed')
            ),
            EXISTS (SELECT 1 FROM groundloop_m5_event_result)
        """
    ).fetchone()
    if terminal_history is None:
        raise M5RuntimeRecoveryBundleError(
            "migration-016 terminal-history guard returned no row"
        )
    if bool(terminal_history[0]) or bool(terminal_history[1]):
        raise M5RuntimeRecoveryBundleError(
            "M5 runtime recovery bundle forbids legacy terminal M5 history"
        )


def install_m5_runtime_recovery_bundle(
    connection: Connection[Any],
    *,
    failure_injector: Callable[[str], None] | None = None,
    migration_bytes: bytes | None = None,
) -> M5RuntimeRecoveryBundleInstallResult:
    """Atomically install or ledger-first replay frozen migration 016."""

    identity = m5_runtime_recovery_bundle_identity(
        migration_bytes=migration_bytes,
    )
    migration = (
        M5_RUNTIME_RECOVERY_MIGRATION_PATH.read_bytes()
        if migration_bytes is None
        else migration_bytes
    )

    with connection.transaction():
        # D24 requires bundle-ledger replay before the zero-attempt first-install
        # guard.  This keeps exact rerun legal after production attempts exist.
        ledger = _read_ledger(connection, identity.bundle_id)
        if ledger is not None:
            expected = (
                identity.bundle_sha256,
                identity.migration_sha256,
                identity.oracle_sha256,
                identity.prerequisite_sha256,
            )
            if ledger != expected:
                raise M5BundleHashConflictError(
                    f"bundle {identity.bundle_id} is already ledgered with "
                    "different content"
                )
            _verify_accepted_m5_runtime_bundle(connection)
            return M5RuntimeRecoveryBundleInstallResult(
                identity=identity,
                applied=False,
            )

        _verify_accepted_m5_runtime_bundle(connection)
        _acquire_m5_runtime_recovery_install_locks(connection)
        _verify_accepted_m5_runtime_bundle(connection)

        # A concurrent exact installer can only become visible before these
        # locks are acquired.  Recheck the ledger before the strict guard.
        ledger = _read_ledger(connection, identity.bundle_id)
        if ledger is not None:
            expected = (
                identity.bundle_sha256,
                identity.migration_sha256,
                identity.oracle_sha256,
                identity.prerequisite_sha256,
            )
            if ledger != expected:
                raise M5BundleHashConflictError(
                    f"bundle {identity.bundle_id} is already ledgered with "
                    "different content"
                )
            return M5RuntimeRecoveryBundleInstallResult(
                identity=identity,
                applied=False,
            )

        _m5_runtime_recovery_first_install_guard(connection)
        for group_name, group_sql in _m5_runtime_recovery_migration_groups(migration):
            connection.execute(group_sql)
            if failure_injector is not None:
                failure_injector(f"after_{group_name}")

        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        if failure_injector is not None:
            failure_injector("after_constraints")
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
                identity.prerequisite_sha256,
            ),
        )
        if failure_injector is not None:
            failure_injector("after_ledger")
        return M5RuntimeRecoveryBundleInstallResult(identity=identity, applied=True)


def _verify_accepted_m5_recovery_bundle(connection: Connection[Any]) -> None:
    row = connection.execute(
        """
        SELECT bundle_id, migration_sha256, bundle_sha256, oracle_sha256,
               prerequisite_sha256
        FROM groundloop_m5_schema_bundle WHERE bundle_id = %s
        """,
        (M5_ACCEPTED_RECOVERY_BUNDLE_ID,),
    ).fetchone()
    expected = (
        M5_ACCEPTED_RECOVERY_BUNDLE_ID,
        M5_ACCEPTED_RECOVERY_MIGRATION_SHA256,
        M5_ACCEPTED_RECOVERY_BUNDLE_SHA256,
        M5_ACCEPTED_RECOVERY_ORACLE_SHA256,
        M5_ACCEPTED_RECOVERY_PREREQUISITE_SHA256,
    )
    actual = None if row is None else tuple(str(value).strip() for value in row)
    if actual != expected:
        raise M5PrerequisiteError(
            "migration 017 requires the exact accepted five-field "
            "migration-016 ledger row"
        )


def _acquire_m5_persisted_matching_install_locks(
    connection: Connection[Any],
) -> None:
    for relation in M5_PERSISTED_MATCHING_INSTALL_LOCK_RELATIONS:
        connection.execute(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE NOWAIT")


def _m5_persisted_matching_singletons(connection: Connection[Any]) -> str:
    mode_row = connection.execute(
        "SELECT mode FROM groundloop_runtime_mode WHERE singleton FOR UPDATE"
    ).fetchone()
    if mode_row is None or str(mode_row[0]) not in {"v1_only", "m5_active"}:
        raise M5PersistedMatchingBundleError("migration-017 runtime mode is invalid")
    connection.execute(
        "SELECT epoch_id FROM groundloop_m4_publication_head WHERE singleton FOR UPDATE"
    ).fetchone()
    connection.execute(
        "SELECT epoch_id, sealed_revision FROM groundloop_m5_publication_head "
        "WHERE singleton FOR UPDATE"
    ).fetchone()
    connection.execute(
        "SELECT activation_id FROM groundloop_m5_activation WHERE singleton FOR UPDATE"
    ).fetchone()
    return str(mode_row[0])


def _m5_persisted_matching_first_install_guard(
    connection: Connection[Any], mode: str
) -> None:
    if connection.execute(
        "SELECT EXISTS (SELECT 1 FROM groundloop_m5_runtime_epoch)"
    ).fetchone() != (False,) or connection.execute(
        "SELECT EXISTS (SELECT 1 FROM groundloop_m5_update)"
    ).fetchone() != (False,):
        raise M5PersistedMatchingBundleError(
            "migration 017 forbids pre-D25 typed runtime history"
        )
    if mode == "m5_active":
        shape = connection.execute(
            """
            SELECT count(*) FILTER (WHERE activation.singleton),
                   count(*) FILTER (WHERE m5_head.singleton),
                   min(m4_head.epoch_id), min(m5_head.epoch_id),
                   min(m5_head.sealed_revision), min(epoch.revision),
                   bool_and(epoch.semantic_status = 'sealed')
            FROM groundloop_runtime_mode mode
            LEFT JOIN groundloop_m5_activation activation ON activation.singleton
            LEFT JOIN groundloop_m4_publication_head m4_head ON m4_head.singleton
            LEFT JOIN groundloop_m5_publication_head m5_head ON m5_head.singleton
            LEFT JOIN groundloop_epoch epoch ON epoch.epoch_id=m5_head.epoch_id
            WHERE mode.singleton
            """
        ).fetchone()
        if (
            shape is None
            or shape[0] != 1
            or shape[1] != 1
            or shape[2] != shape[3]
            or shape[4] != shape[5]
            or shape[6] is not True
        ):
            raise M5PersistedMatchingBundleError(
                "migration-017 activated no-history heads are inconsistent"
            )
        mismatches = connection.execute(
            "SELECT * FROM groundloop_m5_oracle_mismatch_counts"
        ).fetchone()
        if mismatches is None or any(int(value) != 0 for value in mismatches):
            raise M5PersistedMatchingBundleError(
                "migration-017 activated no-history semantic snapshot is inconsistent"
            )


def _m5_persisted_matching_backfill(connection: Connection[Any], mode: str) -> None:
    if mode == "v1_only":
        return
    connection.execute(
        "SELECT set_config('groundloop.m5_checked_transition','on',true)"
    )
    connection.execute(
        "SELECT set_config('groundloop.m5_matching_mode','migration',true)"
    )
    connection.execute(
        """  # noqa: E501
        INSERT INTO groundloop_m5_matching_image_current
        SELECT true, snapshot.policy_version, head.epoch_id, head.sealed_revision
        FROM groundloop_m5_oracle_snapshot snapshot
        JOIN groundloop_m5_publication_head head ON head.singleton
        """  # noqa: E501
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_matching_observation_current
        SELECT observation_id, edge.requirement_version_id, edge.group_version_id,
               edge.requirement_ordinal, edge.text_hash, head.epoch_id,
               head.sealed_revision
        FROM groundloop_m5_active_requirement_edge_oracle edge
        CROSS JOIN LATERAL unnest(edge.active_observation_ids) observation_id
        JOIN groundloop_m5_publication_head head ON head.singleton
        """
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_matching_edge_current
        SELECT requirement_version_id, text_hash, group_version_id,
               requirement_ordinal, cardinality(active_observation_ids),
               head.epoch_id, head.sealed_revision
        FROM groundloop_m5_active_requirement_edge_oracle
        JOIN groundloop_m5_publication_head head ON head.singleton
        """
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_matching_hash_mask_current
        SELECT edge.group_version_id, edge.text_hash,
               sum(1 << edge.requirement_ordinal)::integer,
               head.epoch_id, head.sealed_revision
        FROM groundloop_m5_active_requirement_edge_oracle edge
        JOIN groundloop_m5_publication_head head ON head.singleton
        GROUP BY edge.group_version_id, edge.text_hash, head.epoch_id,
                 head.sealed_revision
        """
    )
    connection.execute(
        """  # noqa: E501
        WITH groups AS (
          SELECT group_version_id, max(requirement_count) requirement_count
          FROM groundloop_m5_group_subset_hall_oracle GROUP BY group_version_id
        ), arrays AS (
          SELECT g.group_version_id, g.requirement_count,
            ARRAY[0::bigint] || ARRAY(
              SELECT count(*)::bigint
              FROM generate_series(1,(1<<g.requirement_count)-1) mask
              LEFT JOIN groundloop_m5_matching_hash_mask_current h
                ON h.group_version_id=g.group_version_id AND h.mask=mask
              GROUP BY mask ORDER BY mask
            ) histogram,
            ARRAY[0::bigint] || ARRAY(
              SELECT neighbor_count::bigint
              FROM groundloop_m5_group_subset_hall_oracle h
              WHERE h.group_version_id=g.group_version_id ORDER BY subset_mask
            ) neighbors,
            ARRAY[0::bigint] || ARRAY(
              SELECT deficiency::bigint
              FROM groundloop_m5_group_subset_hall_oracle h
              WHERE h.group_version_id=g.group_version_id ORDER BY subset_mask
            ) deficiencies
          FROM groups g)
        INSERT INTO groundloop_m5_matching_hall_current
        SELECT a.group_version_id, a.requirement_count, a.histogram, a.neighbors,
          a.deficiencies,
          greatest(0,(SELECT max(v) FROM unnest(a.deficiencies) v))::integer,
          a.requirement_count - greatest(
            0,(SELECT max(v) FROM unnest(a.deficiencies) v)
          )::integer,
          (SELECT sum(v) FROM unnest(a.histogram) v), head.epoch_id,
          head.sealed_revision
        FROM arrays a JOIN groundloop_m5_publication_head head ON head.singleton
        """
    )


def install_m5_persisted_matching_bundle(
    connection: Connection[Any],
    *,
    failure_injector: Callable[[str], None] | None = None,
    migration_bytes: bytes | None = None,
) -> M5PersistedMatchingBundleInstallResult:
    """Atomically install or ledger-first replay frozen migration 017."""

    if connection.info.transaction_status != TransactionStatus.IDLE:
        raise M5PersistedMatchingBundleError(
            "migration 017 requires an idle connection and owns its "
            "top-level transaction"
        )
    if connection.read_only is True or connection.isolation_level not in (
        None,
        IsolationLevel.READ_COMMITTED,
    ):
        raise M5PersistedMatchingBundleError(
            "migration 017 requires a read-write READ COMMITTED connection"
        )
    identity = m5_persisted_matching_bundle_identity(migration_bytes=migration_bytes)
    migration = (
        M5_PERSISTED_MATCHING_MIGRATION_PATH.read_bytes()
        if migration_bytes is None
        else migration_bytes
    )
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE")
        ledger = _read_ledger(connection, identity.bundle_id)
        expected = (
            identity.bundle_sha256,
            identity.migration_sha256,
            identity.oracle_sha256,
            identity.prerequisite_sha256,
        )
        if ledger is not None:
            if ledger != expected:
                raise M5BundleHashConflictError(
                    f"bundle {identity.bundle_id} is already ledgered with "
                    "different content"
                )
            return M5PersistedMatchingBundleInstallResult(identity, False)
        if failure_injector is not None:
            failure_injector("after_initial_ledger")
        _verify_accepted_m5_recovery_bundle(connection)
        _acquire_m5_persisted_matching_install_locks(connection)
        if failure_injector is not None:
            failure_injector("after_install_locks")
        ledger = _read_ledger(connection, identity.bundle_id)
        if ledger is not None:
            if ledger != expected:
                raise M5BundleHashConflictError(
                    f"bundle {identity.bundle_id} is already ledgered with "
                    "different content"
                )
            return M5PersistedMatchingBundleInstallResult(identity, False)
        mode = _m5_persisted_matching_singletons(connection)
        _m5_persisted_matching_first_install_guard(connection, mode)
        for name, sql_source in _m5_runtime_recovery_migration_groups(
            migration.replace(
                b"groundloop:m5-persisted-matching-group:",
                b"groundloop:m5-runtime-recovery-group:",
            )
        ):
            connection.execute(sql_source)
            if failure_injector is not None:
                failure_injector(f"after_{name}")
        _m5_persisted_matching_backfill(connection, mode)
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        if failure_injector is not None:
            failure_injector("before_ledger")
        connection.execute(
            """INSERT INTO groundloop_m5_schema_bundle
               (bundle_id,bundle_sha256,migration_sha256,oracle_sha256,
                prerequisite_sha256,applied_at) VALUES (%s,%s,%s,%s,%s,now())""",
            (
                identity.bundle_id,
                identity.bundle_sha256,
                identity.migration_sha256,
                identity.oracle_sha256,
                identity.prerequisite_sha256,
            ),
        )
        if failure_injector is not None:
            failure_injector("after_ledger")
        return M5PersistedMatchingBundleInstallResult(identity, True)


__all__ = [
    "LEGACY_MIGRATION_NAMES",
    "LEGACY_MIGRATION_PATHS",
    "M5_BUNDLE_ID",
    "M5_CATALOG_PREFLIGHT_ROW_EXCLUSIVE_RELATIONS",
    "M5_INSTALL_LOCK_RELATIONS",
    "M5_RUNTIME_BUNDLE_ID",
    "M5_ACCEPTED_RUNTIME_BUNDLE_ID",
    "M5_ACCEPTED_RUNTIME_BUNDLE_SHA256",
    "M5_ACCEPTED_RUNTIME_MIGRATION_SHA256",
    "M5_ACCEPTED_RUNTIME_ORACLE_SHA256",
    "M5_ACCEPTED_RUNTIME_PREREQUISITE_SHA256",
    "M5_ACCEPTED_RECOVERY_BUNDLE_ID",
    "M5_ACCEPTED_RECOVERY_BUNDLE_SHA256",
    "M5_ACCEPTED_RECOVERY_MIGRATION_SHA256",
    "M5_ACCEPTED_RECOVERY_ORACLE_SHA256",
    "M5_ACCEPTED_RECOVERY_PREREQUISITE_SHA256",
    "M5_PERSISTED_MATCHING_BUNDLE_ID",
    "M5_PERSISTED_MATCHING_INSTALL_LOCK_RELATIONS",
    "M5_PERSISTED_MATCHING_MIGRATION_LABEL",
    "M5_PERSISTED_MATCHING_MIGRATION_PATH",
    "M5_PERSISTED_MATCHING_ORACLE_SHA256",
    "M5_RUNTIME_INSTALL_LOCK_RELATIONS",
    "M5_RUNTIME_MIGRATION_LABEL",
    "M5_RUNTIME_MIGRATION_PATH",
    "M5_RUNTIME_ORACLE_SHA256",
    "M5_RUNTIME_RECOVERY_BUNDLE_ID",
    "M5_RUNTIME_RECOVERY_INSTALL_LOCK_RELATIONS",
    "M5_RUNTIME_RECOVERY_MIGRATION_LABEL",
    "M5_RUNTIME_RECOVERY_MIGRATION_PATH",
    "M5_RUNTIME_RECOVERY_ORACLE_SHA256",
    "M5BundleError",
    "M5BundleHashConflictError",
    "M5BundleIdentity",
    "M5BundleInstallResult",
    "M5PrerequisiteError",
    "M5RuntimeBundleError",
    "M5RuntimeBundleIdentity",
    "M5RuntimeBundleInstallResult",
    "M5RuntimeRecoveryBundleError",
    "M5RuntimeRecoveryBundleIdentity",
    "M5RuntimeRecoveryBundleInstallResult",
    "M5PersistedMatchingBundleError",
    "M5PersistedMatchingBundleIdentity",
    "M5PersistedMatchingBundleInstallResult",
    "apply_legacy_migrations",
    "install_m5_core_bundle",
    "install_m5_runtime_bundle",
    "install_m5_runtime_recovery_bundle",
    "install_m5_persisted_matching_bundle",
    "legacy_prerequisite_source_sha256",
    "m5_bundle_identity",
    "m5_runtime_bundle_identity",
    "m5_runtime_recovery_bundle_identity",
    "m5_persisted_matching_bundle_identity",
]
