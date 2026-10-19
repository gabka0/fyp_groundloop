"""Cursor-local checked persistence for M5 requirement-verifier returns.

This module owns the R1-P verifier settlement boundary.  It deliberately does
not own a transaction and it deliberately does not implement the active
maintained-matching transition reserved for M5-D25.  Exact durable replay and
late audit branches remain available; a first all-active completion is
rejected before checked-write authorization or any database mutation.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import asdict, dataclass, fields
from typing import Any

from psycopg import Connection, Cursor, IsolationLevel, sql
from psycopg.pq import TransactionStatus

from groundloop.domain import DecisionPolicy, StatusDelta, SubjectKind
from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.m5.digests import (
    bool_field as _d34_bool_field,
)
from groundloop.m5.digests import (
    enum_field as _d34_enum_field,
)
from groundloop.m5.digests import (
    f64_field as _d34_f64_field,
)
from groundloop.m5.digests import (
    hash_field as _d34_hash_field,
)
from groundloop.m5.digests import (
    int_field as _d34_int_field,
)
from groundloop.m5.digests import (
    option_field as _d34_option_field,
)
from groundloop.m5.digests import (
    sequence_field as _d34_sequence_field,
)
from groundloop.m5.digests import (
    text_field as _d34_text_field,
)
from groundloop.m5.domain import (
    GroupMatchingCertificateArtifact as _d34_GroupMatchingCertificateArtifact,
)
from groundloop.m5.runtime import digests
from groundloop.m5.runtime import postgres_matching as _d34_matching
from groundloop.m5.runtime import postgres_recovery as _d34_recovery
from groundloop.m5.runtime.contracts import (
    MATCHING_WORK_COUNTER_NAMES as _d34_MATCHING_WORK_COUNTER_NAMES,
)
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshotEntry,
    M5AttemptArchiveReason,
    M5AttemptDisposition,
    M5AttemptOutput,
    M5AttemptResultArtifact,
    M5ExecutionEvidenceDisposition,
    M5JobCompletion,
    M5JobKind,
    M5JobLease,
    M5JobState,
    M5LogicalJobSpec,
    M5PersistedMatchingSourceKind,
    M5PersistedMatchingTransitionIntent,
    M5RequirementAttemptReturnReceipt,
    M5RequirementPairInput,
    M5RequirementReturnDisposition,
    M5RequirementVerifierArtifact,
    M5RuntimeTiming,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5ScopeState,
    M5TerminalReason,
    RequirementRegistrySnapshotEntry,
)
from groundloop.m5.runtime.contracts import (
    m5_overlay_work_values as _d34_m5_overlay_work_values,
)
from groundloop.m5.runtime.postgres_matching import (
    _d34_archive_coordinates,
    _d34_ArchiveCoordinate,
    _d34_preflight_archive,
    _d34_reserve_archive,
    _d34_validate_source_observation,
)
from groundloop.m5.runtime.postgres_recovery import (
    RequirementExecutionAccounting,
    build_requirement_execution_accounting,
    finish_requirement_execution_accounting,
    load_requirement_dispatch,
    persist_requirement_execution_accounting,
    read_latest_requirement_attempt,
    read_requirement_attempt,
    read_requirement_execution_replay,
    read_requirement_postterminal_replay,
    require_runtime_recovery_bundle,
    start_event_accounting,
)
from groundloop.m5.runtime.postgres_roots import (
    _JOB_SELECT,
    _advance_revision,
    _authorize,
    _build_late_return_plan,
    _current_terminal_logical_result_hash,
    _EpochHeader,
    _force_deferred_validation,
    _job_from_row,
    _load_attempt_result,
    _load_manifest,
    _lock_and_validate_pending_counter_revisions,
    _lock_epoch,
    _lock_snapshot_headers,
    _persist_late_return,
    _read_scope,
    _require_pending_epoch,
    _same_attempt_identity,
    _stored_completion,
    _StoredJob,
    _validate_executable_lease,
    _validate_header_bindings,
    _validate_late_replay,
)

RuntimeVerifierFailureInjector = Callable[[str], None]

_d34_OWNER_SEAL = object()


@dataclass(frozen=True, slots=True)
class _d34_ReturnProbe:
    job: _StoredJob
    attempt: _d34_recovery.StoredRequirementAttempt
    receipt: M5RequirementAttemptReturnReceipt | None
    matching_intent: M5PersistedMatchingTransitionIntent | None


def _d34_read_activity(
    cursor: Cursor[Any], header: _EpochHeader, pair_input: M5RequirementPairInput
) -> tuple[bool, bool, bool, bool]:
    """Non-authoritative discovery; compare with the later locked predicates."""

    row = cursor.execute(
        """SELECT chunk.valid_to_epoch,
                  EXISTS (SELECT 1 FROM groundloop_m5_active_chunk_snapshot_member
                    WHERE active_chunk_snapshot_digest=%s
                      AND chunk_version_id=chunk.chunk_version_id),
                  family.lifecycle_state,group_version.lifecycle_state,
                  requirement.lifecycle_state,
                  EXISTS (SELECT 1
                    FROM groundloop_m5_requirement_registry_snapshot_member
                    WHERE requirement_registry_snapshot_digest=%s
                      AND requirement_version_id=requirement.requirement_version_id)
           FROM groundloop_chunk_version chunk
           JOIN groundloop_m5_requirement_version requirement
             ON requirement.requirement_version_id=%s
           JOIN groundloop_m5_group_version group_version
             ON group_version.group_version_id=requirement.group_version_id
            AND group_version.group_version_id=%s
           JOIN groundloop_m5_group_family family
             ON family.group_family_id=group_version.group_family_id
            AND family.group_family_id=%s
           WHERE chunk.chunk_version_id=%s""",
        (
            header.active_chunk_snapshot_digest,
            header.requirement_registry_snapshot_digest,
            pair_input.pair.subject_id,
            pair_input.group_version_id,
            pair_input.group_family_id,
            pair_input.pair.chunk_version_id,
        ),
    ).fetchone()
    if row is None:
        raise ValidationError("D34 activity locator lacks its native structure")
    return (
        (header.structural_status, header.semantic_status, header.evaluation_state)
        == ("committed", "pending", "pending")
        and header.runtime_state in {"structural_committed", "semantic_pending"},
        row[0] is None and bool(row[1]),
        bool(row[5]) and str(row[4]) in {"STAGED", "PUBLISHED"},
        bool(row[5])
        and str(row[2]) in {"STAGED", "PUBLISHED"}
        and str(row[3]) in {"STAGED", "PUBLISHED"},
    )


def _d34_probe_retained_return(
    cursor: Cursor[Any],
    *,
    owner: _d34_CompletionOwner,
    lease: M5JobLease,
    job: M5LogicalJobSpec,
    pair_input: M5RequirementPairInput,
    artifact: M5RequirementVerifierArtifact,
    output: M5AttemptOutput,
    disposition: M5ExecutionEvidenceDisposition,
    attempt_work: M5RuntimeWork,
    attempt_timing: M5RuntimeTiming | None,
) -> _d34_ReturnProbe:
    """Classify the complete durable tuple at5/6, before fresh authorization."""

    _d34_validate_owner(cursor, owner)
    header = owner.header
    row = cursor.execute(
        _JOB_SELECT + " WHERE epoch_id=%s AND logical_job_id=%s",
        (header.epoch_id, job.logical_job_id),
    ).fetchone()
    if row is None:
        raise EventConflictError("D34 verifier job is not durable")
    stored_job = _job_from_row(tuple(row))
    attempt = read_requirement_attempt(
        cursor,
        logical_job_id=job.logical_job_id,
        attempt_id=output.attempt_id,
    )
    if (
        attempt is None
        or lease.attempt is None
        or stored_job.spec != job
        or not _same_attempt_identity(attempt.attempt, lease.attempt)
    ):
        raise EventConflictError("D34 retained locator changed job/attempt identity")
    dispatch = load_requirement_dispatch(
        cursor, epoch_id=header.epoch_id, attempt=attempt.attempt
    )
    if (
        dispatch.record_digest != lease.dispatch_record_digest
        or dispatch.dispatched_revision != lease.resulting_revision
    ):
        raise EventConflictError("D34 retained dispatch identity changed")
    matching = cursor.execute(
        """SELECT resulting_revision,source_identity_hash
           FROM groundloop_m5_matching_work_contribution
           WHERE epoch_id=%s AND source_kind='requirement_completion'
             AND source_id=%s""",
        (header.epoch_id, output.attempt_id),
    ).fetchone()
    result = _load_attempt_result(cursor, attempt_id=output.attempt_id)
    evidence = cursor.execute(
        """SELECT evidence_digest FROM groundloop_m5_attempt_execution_evidence
           WHERE epoch_id=%s AND subgraph='requirement' AND attempt_id=%s""",
        (header.epoch_id, output.attempt_id),
    ).fetchone()
    contributions = cursor.execute(
        """SELECT contribution_kind FROM groundloop_m5_runtime_work_contribution
           WHERE epoch_id=%s AND source_id=%s
             AND contribution_kind IN ('m5_attempt_execution','verifier_completion',
                                       'preterminal_late_return')""",
        (header.epoch_id, output.attempt_id),
    ).fetchall()
    timing = cursor.execute(
        """SELECT attempt_timing_digest FROM groundloop_m5_runtime_timing_contribution
           WHERE epoch_id=%s AND subgraph='requirement' AND attempt_id=%s""",
        (header.epoch_id, output.attempt_id),
    ).fetchone()
    if (
        matching is None
        and result is None
        and evidence is None
        and not contributions
        and timing is None
    ):
        return _d34_ReturnProbe(stored_job, attempt, None, None)
    standard = build_requirement_execution_accounting(
        epoch_id=header.epoch_id,
        expected_revision=header.revision,
        attempt=lease.attempt,
        dispatch=dispatch,
        disposition=disposition,
        result_or_error_hash=output.attempt_output_digest,
        attempt_work=attempt_work,
        attempt_timing=attempt_timing,
        anchor_kind=M5RuntimeWorkContributionKind.VERIFIER_COMPLETION,
    )
    late = build_requirement_execution_accounting(
        epoch_id=header.epoch_id,
        expected_revision=header.revision,
        attempt=lease.attempt,
        dispatch=dispatch,
        disposition=disposition,
        result_or_error_hash=output.attempt_output_digest,
        attempt_work=attempt_work,
        attempt_timing=attempt_timing,
        anchor_kind=M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
        anchor_revision=header.revision,
    )
    replay_revision = read_requirement_execution_replay(
        cursor,
        expected_evidence=standard.evidence,
        expected_observation=standard.observation,
    )
    postterminal = (
        None
        if replay_revision is not None
        else read_requirement_postterminal_replay(
            cursor,
            expected_evidence=standard.evidence,
            expected_observation=standard.observation,
        )
    )
    receipt = _validate_late_replay(
        cursor,
        header=header,
        lease=lease,
        supplied_job=job,
        supplied_output=output,
        job=stored_job,
        attempt=attempt,
        accounting=late,
        event_replay_revision=replay_revision,
        postterminal_replay=postterminal,
    )
    if receipt is None:
        receipt = _validate_applied_replay(
            cursor,
            header=header,
            lease=lease,
            supplied_job=job,
            pair_input=pair_input,
            verifier_artifact=artifact,
            attempt_output=output,
            stored_job=stored_job,
            attempt=attempt,
            accounting=standard,
            replay_revision=replay_revision,
        )
    active = (
        result is not None
        and result.artifact.disposition
        is M5AttemptDisposition.VERIFIER_COMPLETED_ACTIVE
    )
    intent = None
    if receipt is not None and active:
        if matching is None:
            raise EventConflictError("D34 active replay lacks its matching tuple")
        assert result is not None
        result_revision = int(matching[0])
        intent = _d34_matching._candidate_retained_replay_intent(
            cursor,
            epoch_id=header.epoch_id,
            expected_runtime_revision=result_revision - 1,
            resulting_revision=result_revision,
            source_kind=M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION,
            source_id=output.attempt_id,
            expected_source_identity_hash=result.artifact.attempt_result_artifact_hash,
        )
        if intent is None or result_revision != replay_revision:
            raise EventConflictError("D34 retained D24/D25 revisions disagree")
    elif matching is not None or active:
        raise EventConflictError(
            "D34 verifier/matching tuple is only partially durable"
        )
    return _d34_ReturnProbe(stored_job, attempt, receipt, intent)


@dataclass(frozen=True, slots=True)
class _d34_CompletionOwner:
    cursor: Cursor[Any]
    connection: Connection[Any]
    backend: int
    transaction: int
    schema: str
    header: _EpochHeader
    epoch_id: int
    revision: int
    header_snapshot: tuple[object, ...]
    owner_identity: int
    seal: object


def _d34_require_idle(connection: Connection[Any]) -> None:
    if (
        connection.closed
        or connection.broken
        or (connection.info.transaction_status is not TransactionStatus.IDLE)
    ):
        raise ValidationError("D34 completion requires an idle owned connection")
    if connection.isolation_level not in {None, IsolationLevel.READ_COMMITTED} or (
        connection.read_only is True
    ):
        raise ValidationError("D34 completion requires read-write READ COMMITTED")


def _d34_schema_authority(cursor: Cursor[Any]) -> str:
    if cursor.execute("SHOW transaction_isolation").fetchone() != ("read committed",):
        raise ValidationError("D34 completion requires READ COMMITTED")
    if cursor.execute("SHOW transaction_read_only").fetchone() != ("off",):
        raise ValidationError("D34 completion requires a read-write transaction")
    row = cursor.execute("SELECT pg_catalog.current_schema()").fetchone()
    if row is None or type(row[0]) is not str or row[0].startswith("pg_"):
        raise ValidationError("D34 permanent schema is absent")
    schema = row[0]
    shadow = cursor.execute(
        """SELECT relation.relname FROM pg_catalog.pg_class AS relation
           JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relnamespace
           WHERE namespace.nspname=%s AND relation.relkind IN ('r','p','v','m','S')
             AND pg_catalog.to_regclass(pg_catalog.quote_ident(relname))
                 IS DISTINCT FROM relation.oid LIMIT 1""",
        (schema,),
    ).fetchone()
    temporary = cursor.execute(
        """SELECT relname FROM pg_catalog.pg_class
           WHERE relnamespace=pg_catalog.pg_my_temp_schema()
             AND relname LIKE 'groundloop%%'
             AND relname NOT IN ('groundloop_m5_matching_transition_context',
                 'groundloop_m5_matching_change_journal',
                 'groundloop_m5_matching_expected_changes') LIMIT 1"""
    ).fetchone()
    builtin_shadow = cursor.execute(
        """SELECT p.proname FROM pg_catalog.pg_proc p
           JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
           WHERE n.nspname=%s AND EXISTS (
             SELECT 1 FROM pg_catalog.pg_proc builtin
             WHERE builtin.pronamespace='pg_catalog'::regnamespace
               AND builtin.proname=p.proname)
             AND NOT (
               p.proname='gen_random_uuid' AND p.pronargs=0
               AND p.prosrc='pg_random_uuid' AND p.probin='$libdir/pgcrypto'
               AND p.prorettype='uuid'::regtype AND p.proconfig IS NULL
               AND NOT p.prosecdef AND p.provariadic=0 AND p.pronargdefaults=0
               AND EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                   JOIN pg_catalog.pg_extension e ON e.oid=d.refobjid
                   JOIN pg_catalog.pg_language l ON l.oid=p.prolang
                   WHERE d.classid='pg_catalog.pg_proc'::regclass
                     AND d.objid=p.oid AND d.deptype='e'
                     AND d.refclassid='pg_catalog.pg_extension'::regclass
                     AND e.extname='pgcrypto' AND l.lanname='c')) LIMIT 1""",
        (schema,),
    ).fetchone()
    if shadow is not None or temporary is not None or builtin_shadow is not None:
        raise ValidationError("D34 permanent authority is shadowed")
    crypto = cursor.execute(
        """SELECT namespace.nspname,extension.oid,namespace.oid,
                  extension.extowner,namespace.nspowner
           FROM pg_catalog.pg_extension extension
           JOIN pg_catalog.pg_namespace namespace
             ON namespace.oid=extension.extnamespace
           WHERE extension.extname='pgcrypto'"""
    ).fetchone()
    if crypto is None:
        raise ValidationError("D34 native crypto extension is absent")
    crypto_schema = str(crypto[0])
    unsafe = cursor.execute(
        """SELECT 1 FROM pg_catalog.pg_namespace namespace,
                LATERAL pg_catalog.aclexplode(COALESCE(namespace.nspacl,
                    pg_catalog.acldefault('n',namespace.nspowner))) acl
           WHERE namespace.oid=%s AND acl.privilege_type='CREATE'
             AND acl.grantee NOT IN (%s,%s) LIMIT 1""",
        (crypto[2], crypto[3], crypto[4]),
    ).fetchone()
    functions = cursor.execute(
        """SELECT proc.oid,pg_catalog.pg_get_function_identity_arguments(proc.oid),
                  proc.prosrc,proc.proisstrict,proc.provolatile,proc.prosecdef,
                  proc.probin,language.lanname,proc.pronargdefaults,
                  proc.provariadic,proc.proconfig,proc.prorettype='bytea'::regtype,
                  EXISTS (SELECT 1 FROM pg_catalog.pg_depend dependency
                    WHERE dependency.classid='pg_catalog.pg_proc'::regclass
                      AND dependency.objid=proc.oid AND dependency.deptype='e'
                      AND dependency.refclassid='pg_catalog.pg_extension'::regclass
                      AND dependency.refobjid=%s)
           FROM pg_catalog.pg_proc proc
           JOIN pg_catalog.pg_language language ON language.oid=proc.prolang
           WHERE proc.pronamespace=%s AND proc.proname='digest'
           ORDER BY pg_catalog.pg_get_function_identity_arguments(proc.oid)""",
        (crypto[1], crypto[2]),
    ).fetchall()
    crypto_shadow = cursor.execute(
        "SELECT 1 FROM pg_catalog.pg_proc proc "
        "JOIN pg_catalog.pg_namespace namespace ON namespace.oid=proc.pronamespace "
        "WHERE namespace.nspname=%s AND proc.proname='digest' "
        "AND namespace.oid<>%s LIMIT 1",
        (schema, crypto[2]),
    ).fetchone()
    crypto_fallback = cursor.execute(
        """SELECT 1 WHERE %s<>%s AND (
             EXISTS (SELECT 1 FROM pg_catalog.pg_class
                     WHERE relnamespace=%s AND relname LIKE 'groundloop%%')
             OR EXISTS (SELECT 1 FROM pg_catalog.pg_proc
                        WHERE pronamespace=%s AND proname LIKE 'groundloop%%'))""",
        (schema, crypto_schema, crypto[2], crypto[2]),
    ).fetchone()
    if (
        unsafe is not None
        or crypto_shadow is not None
        or len(functions) != 2
        or crypto_fallback is not None
        or {str(r[1]) for r in functions} != {"bytea, text", "text, text"}
        or any(
            tuple(r[2:])
            != (
                "pg_digest",
                True,
                "i",
                False,
                "$libdir/pgcrypto",
                "c",
                0,
                0,
                None,
                True,
                True,
            )
            for r in functions
        )
    ):
        raise ValidationError("D34 native crypto authority is shadowed")
    cursor.execute(
        sql.SQL("SET LOCAL search_path TO {}, pg_catalog, {}, pg_temp").format(
            sql.Identifier(schema), sql.Identifier(crypto_schema)
        )
    )
    resolved = cursor.execute(
        "SELECT 'digest(bytea,text)'::regprocedure::oid"
    ).fetchone()
    if resolved is None or resolved[0] != next(
        r[0] for r in functions if r[1] == "bytea, text"
    ):
        raise ValidationError("D34 pinned crypto resolution changed")
    require_runtime_recovery_bundle(cursor)
    _d34_matching.require_persisted_matching_bundle(cursor)
    return schema


def _d34_header_authority(header: _EpochHeader) -> tuple[object, ...]:
    if type(header) is not _EpochHeader:
        raise ValidationError("D34 requires the exact held native epoch header")
    if (
        type(header.epoch_id) is not int
        or header.epoch_id < 1
        or type(header.revision) is not int
        or header.revision < 1
    ):
        raise ValidationError("D34 held epoch/revision is invalid")
    return tuple(getattr(header, field.name) for field in fields(_EpochHeader))


def _d34_issue_owner(
    cursor: Cursor[Any], schema: str, header: _EpochHeader
) -> _d34_CompletionOwner:
    header_snapshot = _d34_header_authority(header)
    row = cursor.execute(
        "SELECT pg_catalog.pg_backend_pid(),pg_catalog.pg_current_xact_id()::text"
    ).fetchone()
    if row is None:
        raise ValidationError("D34 owned transaction identity is absent")
    owner = _d34_CompletionOwner(
        cursor=cursor,
        connection=cursor.connection,
        backend=int(row[0]),
        transaction=int(row[1]),
        schema=schema,
        header=header,
        epoch_id=header.epoch_id,
        revision=header.revision,
        header_snapshot=header_snapshot,
        owner_identity=0,
        seal=_d34_OWNER_SEAL,
    )
    object.__setattr__(owner, "owner_identity", id(owner))
    return owner


def _d34_validate_owner(cursor: Cursor[Any], owner: _d34_CompletionOwner) -> None:
    if (
        type(owner) is not _d34_CompletionOwner
        or owner.seal is not _d34_OWNER_SEAL
        or owner.owner_identity != id(owner)
        or owner.cursor is not cursor
        or owner.connection is not cursor.connection
        or owner.connection.info.transaction_status is not TransactionStatus.INTRANS
    ):
        raise ValidationError("D34 phase belongs to another cursor or transaction")
    if (
        owner.header_snapshot != _d34_header_authority(owner.header)
        or owner.epoch_id != owner.header.epoch_id
        or owner.revision != owner.header.revision
    ):
        raise EventConflictError("D34 owned header/epoch/revision changed")
    row = cursor.execute(
        "SELECT pg_catalog.pg_backend_pid(),pg_catalog.pg_current_xact_id()::text,"
        "pg_catalog.current_schema()"
    ).fetchone()
    if row is None or (int(row[0]), int(row[1]), row[2]) != (
        owner.backend,
        owner.transaction,
        owner.schema,
    ):
        raise EventConflictError("D34 owned transaction or schema changed")


def _d34_lock_document_parents(
    cursor: Cursor[Any],
    pair_input: M5RequirementPairInput,
) -> None:
    locator = cursor.execute(
        "SELECT document_id FROM groundloop_document_version "
        "WHERE document_version_id=%s",
        (pair_input.document_version_id,),
    ).fetchone()
    if locator is None:
        raise EventConflictError("D34 source document version is absent")
    document = cursor.execute(
        "SELECT document_id FROM groundloop_document "
        "WHERE document_id=%s FOR KEY SHARE",
        (locator[0],),
    ).fetchone()
    version = cursor.execute(
        "SELECT document_id FROM groundloop_document_version "
        "WHERE document_version_id=%s FOR KEY SHARE",
        (pair_input.document_version_id,),
    ).fetchone()
    if document != locator or version != locator:
        raise EventConflictError("D34 source document parent changed")


def _d34_lock_provenance_parents(
    cursor: Cursor[Any],
    pair_input: M5RequirementPairInput,
    artifact: M5RequirementVerifierArtifact,
) -> None:
    parents = (
        (
            "groundloop_m5_candidate_policy",
            ("candidate_policy_id",),
            (pair_input.candidate_policy_id,),
        ),
        ("groundloop_claim", ("claim_id",), (pair_input.owner_claim_id,)),
        (
            "groundloop_chunker_artifact",
            ("chunker_artifact_id",),
            (pair_input.chunker_artifact_id,),
        ),
        (
            "groundloop_semantic_subject",
            ("subject_kind", "subject_id"),
            ("requirement", pair_input.pair.subject_id),
        ),
        (
            "groundloop_model_artifact",
            ("model_artifact_id",),
            (artifact.model_artifact_id,),
        ),
        (
            "groundloop_prompt_artifact",
            ("prompt_artifact_id",),
            (artifact.prompt_artifact_id,),
        ),
        (
            "groundloop_decision_policy",
            ("policy_version",),
            (artifact.decision_policy_version,),
        ),
    )
    for relation, columns, values in sorted(parents):
        row = cursor.execute(
            sql.SQL("SELECT {} FROM {} WHERE {} FOR KEY SHARE").format(
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.Identifier(relation),
                sql.SQL(" AND ").join(
                    sql.SQL("{}=%s").format(sql.Identifier(c)) for c in columns
                ),
            ),
            values,
        ).fetchone()
        if row != values:
            raise EventConflictError("D34 required provenance parent is absent")
    model = cursor.execute(
        "SELECT task,model_id,immutable_revision FROM groundloop_model_artifact "
        "WHERE model_artifact_id=%s",
        (artifact.model_artifact_id,),
    ).fetchone()
    prompt = cursor.execute(
        "SELECT task,version FROM groundloop_prompt_artifact "
        "WHERE prompt_artifact_id=%s",
        (artifact.prompt_artifact_id,),
    ).fetchone()
    if model != ("verification", artifact.model_id, artifact.model_revision) or (
        prompt != ("verification", artifact.prompt_version)
    ):
        raise EventConflictError("D34 protected model/prompt binding changed")


def _inject(
    injector: RuntimeVerifierFailureInjector | None,
    point: str,
) -> None:
    if injector is not None:
        injector(point)


def _text(value: Any) -> str:
    return str(value).strip()


def _optional_text(value: Any) -> str | None:
    return None if value is None else _text(value)


def _validate_success_disposition(
    disposition: M5ExecutionEvidenceDisposition,
) -> None:
    if disposition not in {
        M5ExecutionEvidenceDisposition.RETURNED,
        M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
    }:
        raise ValidationError(
            "successful verifier return requires returned or reused_artifact"
        )


def _lock_verifier_activity(
    cursor: Cursor[Any],
    *,
    header: Any,
    pair_input: M5RequirementPairInput,
) -> tuple[bool, bool, bool, bool]:
    """Lock and classify every applicable verifier activity predicate.

    The statements follow frozen tier 7: chunk first, then family, group, and
    requirement.  Snapshot membership is included in the classification but
    the immutable snapshot members need no row lock.
    """

    chunk_row = cursor.execute(
        """
        SELECT chunk.valid_to_epoch,
               EXISTS (
                   SELECT 1
                   FROM groundloop_m5_active_chunk_snapshot_member
                   WHERE active_chunk_snapshot_digest = %s
                     AND chunk_version_id = chunk.chunk_version_id
               )
        FROM groundloop_chunk_version AS chunk
        WHERE chunk.chunk_version_id = %s
        FOR UPDATE OF chunk
        """,
        (
            header.active_chunk_snapshot_digest,
            pair_input.pair.chunk_version_id,
        ),
    ).fetchone()
    if chunk_row is None:
        raise ValidationError("verifier pair names an unknown chunk version")

    family_row = cursor.execute(
        """
        SELECT family.lifecycle_state
        FROM groundloop_m5_group_family AS family
        WHERE family.group_family_id = %s
        FOR UPDATE
        """,
        (pair_input.group_family_id,),
    ).fetchone()
    if family_row is None:
        raise ValidationError("verifier pair names an unknown group family")

    group_row = cursor.execute(
        """
        SELECT group_version.lifecycle_state, group_version.group_family_id
        FROM groundloop_m5_group_version AS group_version
        WHERE group_version.group_version_id = %s
        FOR UPDATE
        """,
        (pair_input.group_version_id,),
    ).fetchone()
    if group_row is None or str(group_row[1]) != pair_input.group_family_id:
        raise ValidationError("verifier pair group binding is corrupt")

    requirement_row = cursor.execute(
        """
        SELECT requirement.lifecycle_state,
               requirement.group_version_id,
               EXISTS (
                   SELECT 1
                   FROM groundloop_m5_requirement_registry_snapshot_member
                   WHERE requirement_registry_snapshot_digest = %s
                     AND requirement_version_id =
                         requirement.requirement_version_id
               )
        FROM groundloop_m5_requirement_version AS requirement
        WHERE requirement.requirement_version_id = %s
        FOR UPDATE
        """,
        (
            header.requirement_registry_snapshot_digest,
            pair_input.pair.subject_id,
        ),
    ).fetchone()
    if (
        requirement_row is None
        or str(requirement_row[1]) != pair_input.group_version_id
    ):
        raise ValidationError("verifier pair names an unknown requirement")

    epoch_active = (
        header.structural_status == "committed"
        and header.semantic_status == "pending"
        and header.evaluation_state == "pending"
        and header.runtime_state in {"structural_committed", "semantic_pending"}
    )
    chunk_active = bool(chunk_row[1]) and chunk_row[0] is None
    in_requirement_snapshot = bool(requirement_row[2])
    requirement_active = in_requirement_snapshot and str(requirement_row[0]) in {
        "STAGED",
        "PUBLISHED",
    }
    group_active = (
        in_requirement_snapshot
        and str(group_row[0]) in {"STAGED", "PUBLISHED"}
        and str(family_row[0]) in {"STAGED", "PUBLISHED"}
    )
    return epoch_active, chunk_active, requirement_active, group_active


def _lock_verifier_jobs(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    parent_job_id: str,
    verifier_job_id: str,
) -> tuple[Any, Any]:
    """Lock the root and child together in tier-9 C order."""

    expected_ids = {parent_job_id, verifier_job_id}
    rows = cursor.execute(
        _JOB_SELECT
        + " WHERE epoch_id = %s AND logical_job_id = ANY(%s)"
        + ' ORDER BY logical_job_id COLLATE "C" FOR UPDATE',
        (epoch_id, list(expected_ids)),
    ).fetchall()
    jobs = tuple(_job_from_row(tuple(row)) for row in rows)
    if {item.spec.logical_job_id for item in jobs} != expected_ids:
        raise InvalidEventError("verifier completion lacks its root/child job pair")
    by_id = {item.spec.logical_job_id: item for item in jobs}
    return by_id[parent_job_id], by_id[verifier_job_id]


def _load_decision_policy(
    cursor: Cursor[Any], *, policy_version: str
) -> DecisionPolicy:
    row = cursor.execute(
        """
        SELECT policy_version, support_threshold, refute_threshold,
               tie_rule_version
        FROM groundloop_decision_policy
        WHERE policy_version = %s
        """,
        (policy_version,),
    ).fetchone()
    if row is None:
        raise ValidationError("verifier artifact names an unknown decision policy")
    return DecisionPolicy(str(row[0]), float(row[1]), float(row[2]), str(row[3]))


def _validate_pair_input_core(
    cursor: Cursor[Any],
    *,
    header: Any,
    pair_input: M5RequirementPairInput,
) -> None:
    requirement_row = cursor.execute(
        """
        SELECT requirement_version_id, group_version_id, group_family_id,
               owner_claim_id, normalized_requirement_text,
               requirement_text_hash
        FROM groundloop_m5_requirement_registry_snapshot_member
        WHERE requirement_registry_snapshot_digest = %s
          AND requirement_version_id = %s
        """,
        (
            header.requirement_registry_snapshot_digest,
            pair_input.pair.subject_id,
        ),
    ).fetchone()
    chunk_snapshot_row = cursor.execute(
        """
        SELECT chunk_version_id, text_hash
        FROM groundloop_m5_active_chunk_snapshot_member
        WHERE active_chunk_snapshot_digest = %s AND chunk_version_id = %s
        """,
        (header.active_chunk_snapshot_digest, pair_input.pair.chunk_version_id),
    ).fetchone()
    if requirement_row is None or chunk_snapshot_row is None:
        raise ValidationError("verifier pair is outside its frozen snapshots")
    requirement_entry = RequirementRegistrySnapshotEntry(
        requirement_version_id=str(requirement_row[0]),
        group_version_id=str(requirement_row[1]),
        group_family_id=str(requirement_row[2]),
        owner_claim_id=str(requirement_row[3]),
        normalized_requirement_text=str(requirement_row[4]),
        requirement_text_hash=_text(requirement_row[5]),
    )
    chunk_entry = ActiveChunkSnapshotEntry(
        chunk_version_id=str(chunk_snapshot_row[0]),
        text_hash=_text(chunk_snapshot_row[1]),
    )
    pair_input.validate_bound_rows(
        requirement_entry=requirement_entry,
        chunk_entry=chunk_entry,
    )

    core_row = cursor.execute(
        """
        SELECT requirement.ordinal, requirement.requirement_text,
               requirement.requirement_text_hash,
               group_version.group_version_id,
               group_version.group_family_id, family.claim_id,
               chunk.document_version_id, chunk.chunk_index, chunk.text,
               chunk.text_hash, provenance.chunker_artifact_id
        FROM groundloop_m5_requirement_version AS requirement
        JOIN groundloop_m5_group_version AS group_version
          ON group_version.group_version_id = requirement.group_version_id
        JOIN groundloop_m5_group_family AS family
          ON family.group_family_id = group_version.group_family_id
        JOIN groundloop_chunk_version AS chunk
          ON chunk.chunk_version_id = %s
        JOIN groundloop_chunk_provenance AS provenance
          ON provenance.chunk_version_id = chunk.chunk_version_id
        WHERE requirement.requirement_version_id = %s
        """,
        (pair_input.pair.chunk_version_id, pair_input.pair.subject_id),
    ).fetchone()
    expected_core = (
        pair_input.requirement_ordinal,
        pair_input.normalized_requirement_text,
        pair_input.requirement_text_hash,
        pair_input.group_version_id,
        pair_input.group_family_id,
        pair_input.owner_claim_id,
        pair_input.document_version_id,
        pair_input.chunk_index,
        pair_input.chunk_text,
        pair_input.stored_chunk_text_hash,
        pair_input.chunker_artifact_id,
    )
    if (
        core_row is None
        or tuple(
            _text(value) if index in {2, 9} else value
            for index, value in enumerate(core_row)
        )
        != expected_core
    ):
        raise EventConflictError("pair input differs from immutable core rows")


def _validate_verifier_bindings(
    cursor: Cursor[Any],
    *,
    header: Any,
    scope: Any,
    parent_job: Any,
    stored_job: Any,
    supplied_job: M5LogicalJobSpec,
    pair_input: M5RequirementPairInput,
    verifier_artifact: M5RequirementVerifierArtifact,
    attempt_output: M5AttemptOutput,
    manifest: Any,
) -> None:
    if supplied_job != stored_job.spec:
        raise EventConflictError("supplied verifier job differs from durable identity")
    if (
        supplied_job.job_kind is not M5JobKind.VERIFY_REQUIREMENT_PAIR
        or supplied_job.expandable
        or supplied_job.parent_job_id is None
        or supplied_job.pair is None
        or supplied_job.scope_contract_digest is None
    ):
        raise ValidationError("verifier completion requires one pair child job")
    _validate_header_bindings(header, scope, parent_job, manifest)
    supplied_job.validate_manifest_and_scope(manifest, scope.contract)
    if (
        parent_job.spec.logical_job_id != supplied_job.parent_job_id
        or parent_job.state
        not in {M5JobState.COMPLETED_ACTIVE, M5JobState.COMPLETED_INACTIVE}
        or scope.state not in {M5ScopeState.CLOSED_ACTIVE, M5ScopeState.CLOSED_INACTIVE}
    ):
        raise ValidationError("verifier job lacks its closed root scope")

    dependency = cursor.execute(
        """
        SELECT 1
        FROM groundloop_m5_job_dependency
        WHERE epoch_id = %s AND parent_job_id = %s AND child_job_id = %s
        """,
        (header.epoch_id, supplied_job.parent_job_id, supplied_job.logical_job_id),
    ).fetchone()
    admitted = cursor.execute(
        """
        SELECT admitted_pair_digest, subject_kind, subject_id,
               chunk_version_id, semantic_pair_digest, candidate_policy_id
        FROM groundloop_m5_requirement_admitted_pair
        WHERE epoch_id = %s AND admitted_pair_digest = %s
        """,
        (header.epoch_id, stored_job.admitted_pair_digest),
    ).fetchone()
    expected_admitted = (
        stored_job.admitted_pair_digest,
        SubjectKind.REQUIREMENT.value,
        supplied_job.pair.subject_id,
        supplied_job.pair.chunk_version_id,
        supplied_job.semantic_pair_digest,
        supplied_job.candidate_policy_id,
    )
    if (
        dependency is None
        or admitted is None
        or tuple(map(str, admitted)) != tuple(map(str, expected_admitted))
    ):
        raise ValidationError("verifier job lacks its exact admitted-pair closure")

    _validate_pair_input_core(cursor, header=header, pair_input=pair_input)
    policy = _load_decision_policy(
        cursor, policy_version=manifest.decision_policy_version
    )
    verifier_artifact.validate_decision_policy(policy)
    if (
        pair_input.pair != supplied_job.pair
        or pair_input.scope_contract_digest != supplied_job.scope_contract_digest
        or pair_input.candidate_policy_id != supplied_job.candidate_policy_id
        or verifier_artifact.pair != supplied_job.pair
        or verifier_artifact.pair_input_hash != pair_input.pair_input_hash
        or verifier_artifact.execution_spec_hash != supplied_job.execution_spec_hash
        or verifier_artifact.decision_policy_version != manifest.decision_policy_version
        or attempt_output.logical_job_id != supplied_job.logical_job_id
        or attempt_output.job_epoch_id != header.epoch_id
        or attempt_output.payload_hash != supplied_job.payload_hash
        or attempt_output.execution_spec_hash != supplied_job.execution_spec_hash
        or attempt_output.result_artifact_id != verifier_artifact.artifact_id
        or attempt_output.result_artifact_hash != verifier_artifact.artifact_hash
    ):
        raise ValidationError("verifier return does not bind its job and artifact")


def _pair_input_values(pair_input: M5RequirementPairInput) -> tuple[Any, ...]:
    return (
        pair_input.pair_input_hash,
        pair_input.pair.subject_kind.value,
        pair_input.pair.subject_id,
        pair_input.pair.chunk_version_id,
        pair_input.semantic_pair_digest,
        pair_input.scope_contract_digest,
        pair_input.candidate_policy_id,
        pair_input.owner_claim_id,
        pair_input.group_version_id,
        pair_input.group_family_id,
        pair_input.requirement_ordinal,
        pair_input.normalized_requirement_text,
        pair_input.requirement_text_hash,
        pair_input.document_version_id,
        pair_input.chunk_index,
        pair_input.chunk_text,
        pair_input.stored_chunk_text_hash,
        pair_input.m5_chunk_text_hash,
        pair_input.chunker_artifact_id,
        pair_input.normalizer_id,
        pair_input.normalizer_provenance_hash,
    )


def _artifact_values(
    artifact: M5RequirementVerifierArtifact,
) -> tuple[Any, ...]:
    return (
        artifact.artifact_id,
        artifact.artifact_hash,
        artifact.pair.subject_kind.value,
        artifact.pair.subject_id,
        artifact.pair.chunk_version_id,
        artifact.semantic_pair_digest,
        artifact.pair_input_hash,
        artifact.execution_spec_hash,
        artifact.model_artifact_id,
        artifact.model_id,
        artifact.model_revision,
        artifact.prompt_artifact_id,
        artifact.prompt_version,
        artifact.calibration_version,
        artifact.calibration_artifact_hash,
        artifact.temperature,
        artifact.decision_policy_version,
        artifact.decision_policy_hash,
        artifact.support_score,
        artifact.refute_score,
        artifact.neutral_score,
        *artifact.raw_logits,
        artifact.raw_output_hash,
        artifact.operational_label.value,
    )


def _observation_values(
    *,
    epoch_id: int,
    artifact: M5RequirementVerifierArtifact,
    eligible_for_currency: bool,
) -> tuple[Any, ...]:
    observation = artifact.to_semantic_observation()
    return (
        observation.observation_id,
        observation.subject_kind.value,
        observation.subject_id,
        observation.chunk_version_id,
        observation.task_type,
        observation.support_score,
        observation.refute_score,
        observation.neutral_score,
        observation.producer.model_id,
        observation.producer.model_version,
        observation.producer.prompt_version,
        observation.input_hash,
        epoch_id,
        artifact.raw_output_hash,
        eligible_for_currency,
    )


def _execution_values(
    *,
    epoch_id: int,
    job: M5LogicalJobSpec,
    attempt_id: str,
    pair_input: M5RequirementPairInput,
    artifact: M5RequirementVerifierArtifact,
    eligible_for_currency: bool,
) -> tuple[Any, ...]:
    return (
        artifact.to_semantic_observation().observation_id,
        artifact.artifact_id,
        artifact.artifact_hash,
        job.logical_job_id,
        attempt_id,
        pair_input.pair_input_hash,
        artifact.decision_policy_version,
        artifact.decision_policy_hash,
        eligible_for_currency,
        epoch_id,
    )


def _validate_existing_row(
    row: Any,
    expected: tuple[Any, ...],
    *,
    label: str,
) -> bool:
    if row is None:
        return False
    values = tuple(row)
    normalized = tuple(
        _text(value) if isinstance(expected_value, str) else value
        for value, expected_value in zip(values, expected, strict=True)
    )
    if normalized != expected:
        raise EventConflictError(f"existing {label} differs from verifier return")
    return True


def _persist_verifier_closure(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    job: M5LogicalJobSpec,
    attempt_id: str,
    pair_input: M5RequirementPairInput,
    artifact: M5RequirementVerifierArtifact,
    attempt_result: M5AttemptResultArtifact,
    attempt_output: M5AttemptOutput,
    eligible_for_currency: bool,
    failure_injector: RuntimeVerifierFailureInjector | None = None,
) -> tuple[_d34_ArchiveCoordinate, ...]:
    observation_id = artifact.to_semantic_observation().observation_id
    if (
        attempt_result.attempt_id != attempt_id
        or attempt_result.attempt_output_digest != attempt_output.attempt_output_digest
        or attempt_result.logical_job_id != job.logical_job_id
        or attempt_result.job_epoch_id != epoch_id
        or pair_input.scope_contract_digest != job.scope_contract_digest
        or pair_input.candidate_policy_id != job.candidate_policy_id
        or pair_input.pair != job.pair
        or artifact.pair != pair_input.pair
        or artifact.pair_input_hash != pair_input.pair_input_hash
        or artifact.execution_spec_hash != job.execution_spec_hash
        or attempt_output.result_artifact_id != artifact.artifact_id
        or attempt_output.result_artifact_hash != artifact.artifact_hash
        or attempt_result.epoch_active is not True
        or attempt_result.disposition
        not in {
            M5AttemptDisposition.VERIFIER_COMPLETED_ACTIVE,
            M5AttemptDisposition.VERIFIER_COMPLETED_INACTIVE,
        }
        or eligible_for_currency
        is not (
            attempt_result.disposition is M5AttemptDisposition.VERIFIER_COMPLETED_ACTIVE
        )
    ):
        raise EventConflictError("D34 archive result/output binding changed")
    coordinates = _d34_archive_coordinates(
        result=(
            attempt_result.attempt_result_artifact_id,
            attempt_result.attempt_result_artifact_hash,
            attempt_output.attempt_output_digest,
            attempt_id,
        ),
        pair=(
            pair_input.pair_input_hash,
            pair_input.semantic_pair_digest,
            pair_input.scope_contract_digest,
            pair_input.candidate_policy_id,
        ),
        verifier=(artifact.artifact_id, artifact.artifact_hash),
        observation=(
            observation_id,
            "requirement",
            pair_input.pair.subject_id,
            pair_input.pair.chunk_version_id,
            "verify_requirement_v1",
        ),
        execution=(
            observation_id,
            artifact.artifact_id,
            job.logical_job_id,
            attempt_id,
            pair_input.pair_input_hash,
        ),
    )
    _d34_reserve_archive(cursor, coordinates)
    _d34_preflight_archive(cursor, coordinates)
    _insert_attempt_result(cursor, artifact=attempt_result, output=attempt_output)
    _inject(failure_injector, "verifier_attempt_artifact_inserted")
    pair_values = _pair_input_values(pair_input)
    pair_row = cursor.execute(
        """
        SELECT pair_input_hash, subject_kind, subject_id, chunk_version_id,
               semantic_pair_digest, scope_contract_digest,
               candidate_policy_id, owner_claim_id, group_version_id,
               group_family_id, requirement_ordinal,
               normalized_requirement_text, requirement_text_hash,
               document_version_id, chunk_index, chunk_text,
               stored_chunk_text_hash, m5_chunk_text_hash,
               chunker_artifact_id, normalizer_id,
               normalizer_provenance_hash
        FROM groundloop_m5_requirement_pair_input
        WHERE pair_input_hash = %s
        FOR UPDATE
        """,
        (pair_input.pair_input_hash,),
    ).fetchone()
    if not _validate_existing_row(pair_row, pair_values, label="pair input"):
        cursor.execute(
            """
            INSERT INTO groundloop_m5_requirement_pair_input (
                pair_input_hash, subject_kind, subject_id, chunk_version_id,
                semantic_pair_digest, scope_contract_digest,
                candidate_policy_id, owner_claim_id, group_version_id,
                group_family_id, requirement_ordinal,
                normalized_requirement_text, requirement_text_hash,
                document_version_id, chunk_index, chunk_text,
                stored_chunk_text_hash, m5_chunk_text_hash,
                chunker_artifact_id, normalizer_id,
                normalizer_provenance_hash
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            pair_values,
        )
        _inject(failure_injector, "verifier_pair_input_inserted")

    artifact_values = _artifact_values(artifact)
    artifact_row = cursor.execute(
        """
        SELECT artifact_id, artifact_hash, subject_kind, subject_id,
               chunk_version_id, semantic_pair_digest, pair_input_hash,
               execution_spec_hash, model_artifact_id, model_id,
               model_revision, prompt_artifact_id, prompt_version,
               calibration_version, calibration_artifact_hash, temperature,
               decision_policy_version, decision_policy_hash,
               support_score, refute_score, neutral_score,
               raw_logit_contradiction, raw_logit_entailment,
               raw_logit_neutral, raw_output_hash, operational_label
        FROM groundloop_m5_requirement_verifier_artifact
        WHERE artifact_id = %s
        """,
        (artifact.artifact_id,),
    ).fetchone()
    if artifact_row is not None:
        raise EventConflictError("D34 fresh verifier artifact appeared")
    if artifact_row is None:
        cursor.execute(
            """
            INSERT INTO groundloop_m5_requirement_verifier_artifact (
                artifact_id, artifact_hash, subject_kind, subject_id,
                chunk_version_id, semantic_pair_digest, pair_input_hash,
                execution_spec_hash, model_artifact_id, model_id,
                model_revision, prompt_artifact_id, prompt_version,
                calibration_version, calibration_artifact_hash, temperature,
                decision_policy_version, decision_policy_hash,
                support_score, refute_score, neutral_score,
                raw_logit_contradiction, raw_logit_entailment,
                raw_logit_neutral, raw_output_hash, operational_label
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            artifact_values,
        )
        _inject(failure_injector, "verifier_artifact_inserted")

    observation_values = _observation_values(
        epoch_id=epoch_id,
        artifact=artifact,
        eligible_for_currency=eligible_for_currency,
    )
    observation_id = str(observation_values[0])
    observation_row = cursor.execute(
        """
        SELECT observation_id, subject_kind, subject_id, chunk_version_id,
               task_type, support_score, refute_score, neutral_score,
               model_id, model_version, prompt_version, input_hash,
               produced_epoch, raw_output_hash, eligible_for_currency
        FROM groundloop_semantic_observation
        WHERE observation_id = %s
        """,
        (observation_id,),
    ).fetchone()
    if observation_row is not None:
        raise EventConflictError("D34 fresh source observation appeared")
    if observation_row is None:
        cursor.execute(
            """
            INSERT INTO groundloop_semantic_observation (
                observation_id, subject_kind, subject_id, chunk_version_id,
                task_type, support_score, refute_score, neutral_score,
                model_id, model_version, prompt_version, input_hash,
                produced_epoch, raw_output_hash, eligible_for_currency
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s
            )
            """,
            observation_values,
        )
        _inject(failure_injector, "verifier_observation_inserted")

    execution_values = _execution_values(
        epoch_id=epoch_id,
        job=job,
        attempt_id=attempt_id,
        pair_input=pair_input,
        artifact=artifact,
        eligible_for_currency=eligible_for_currency,
    )
    execution_row = cursor.execute(
        """
        SELECT observation_id, artifact_id, artifact_hash, logical_job_id,
               attempt_id, pair_input_hash, decision_policy_version,
               decision_policy_hash, eligible_for_currency, produced_epoch_id
        FROM groundloop_m5_requirement_verifier_execution
        WHERE artifact_id = %s OR (logical_job_id = %s AND attempt_id = %s)
        ORDER BY artifact_id COLLATE "C"
        """,
        (artifact.artifact_id, job.logical_job_id, attempt_id),
    ).fetchall()
    if not execution_row:
        cursor.execute(
            """
            INSERT INTO groundloop_m5_requirement_verifier_execution (
                observation_id, artifact_id, artifact_hash, logical_job_id,
                attempt_id, pair_input_hash, decision_policy_version,
                decision_policy_hash, eligible_for_currency, produced_epoch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            execution_values,
        )
        _inject(failure_injector, "verifier_execution_inserted")
    elif len(execution_row) != 1:
        raise ValidationError("verifier execution unique identities diverged")
    else:
        raise EventConflictError("D34 fresh verifier execution appeared")
    _d34_validate_source_observation(
        cursor,
        observation_id=observation_id,
        epoch_id=epoch_id,
        artifact_id=artifact.artifact_id,
        pair_input_hash=pair_input.pair_input_hash,
        eligible=eligible_for_currency,
    )
    return coordinates


def _validate_verifier_closure_replay(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    job: M5LogicalJobSpec,
    attempt_id: str,
    pair_input: M5RequirementPairInput,
    artifact: M5RequirementVerifierArtifact,
    eligible_for_currency: bool,
) -> bool:
    """Validate the complete attempt-linked closure without writing."""

    execution = cursor.execute(
        """
        SELECT observation_id, artifact_id, artifact_hash, logical_job_id,
               attempt_id, pair_input_hash, decision_policy_version,
               decision_policy_hash, eligible_for_currency, produced_epoch_id
        FROM groundloop_m5_requirement_verifier_execution
        WHERE logical_job_id = %s AND attempt_id = %s
        """,
        (job.logical_job_id, attempt_id),
    ).fetchone()
    if execution is None:
        return False
    _validate_existing_row(
        execution,
        _execution_values(
            epoch_id=epoch_id,
            job=job,
            attempt_id=attempt_id,
            pair_input=pair_input,
            artifact=artifact,
            eligible_for_currency=eligible_for_currency,
        ),
        label="verifier execution",
    )
    pair_row = cursor.execute(
        """
        SELECT pair_input_hash, subject_kind, subject_id, chunk_version_id,
               semantic_pair_digest, scope_contract_digest,
               candidate_policy_id, owner_claim_id, group_version_id,
               group_family_id, requirement_ordinal,
               normalized_requirement_text, requirement_text_hash,
               document_version_id, chunk_index, chunk_text,
               stored_chunk_text_hash, m5_chunk_text_hash,
               chunker_artifact_id, normalizer_id,
               normalizer_provenance_hash
        FROM groundloop_m5_requirement_pair_input
        WHERE pair_input_hash = %s
        """,
        (pair_input.pair_input_hash,),
    ).fetchone()
    artifact_row = cursor.execute(
        """
        SELECT artifact_id, artifact_hash, subject_kind, subject_id,
               chunk_version_id, semantic_pair_digest, pair_input_hash,
               execution_spec_hash, model_artifact_id, model_id,
               model_revision, prompt_artifact_id, prompt_version,
               calibration_version, calibration_artifact_hash, temperature,
               decision_policy_version, decision_policy_hash,
               support_score, refute_score, neutral_score,
               raw_logit_contradiction, raw_logit_entailment,
               raw_logit_neutral, raw_output_hash, operational_label
        FROM groundloop_m5_requirement_verifier_artifact
        WHERE artifact_id = %s
        """,
        (artifact.artifact_id,),
    ).fetchone()
    observation_row = cursor.execute(
        """
        SELECT observation_id, subject_kind, subject_id, chunk_version_id,
               task_type, support_score, refute_score, neutral_score,
               model_id, model_version, prompt_version, input_hash,
               produced_epoch, raw_output_hash, eligible_for_currency
        FROM groundloop_semantic_observation
        WHERE observation_id = %s
        """,
        (artifact.to_semantic_observation().observation_id,),
    ).fetchone()
    if pair_row is None or artifact_row is None or observation_row is None:
        raise ValidationError("verifier execution closure is only partially durable")
    _validate_existing_row(pair_row, _pair_input_values(pair_input), label="pair input")
    _validate_existing_row(
        artifact_row, _artifact_values(artifact), label="verifier artifact"
    )
    _validate_existing_row(
        observation_row,
        _observation_values(
            epoch_id=epoch_id,
            artifact=artifact,
            eligible_for_currency=eligible_for_currency,
        ),
        label="semantic observation",
    )
    return True


def _derive_verifier_completion_work(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    logical_job_id: str,
    attempt_id: str,
    active: bool,
    archive_coordinates: tuple[_d34_ArchiveCoordinate, ...] = (),
) -> M5RuntimeWork:
    rows = cursor.execute(
        """
        SELECT serialized FROM (
            SELECT to_jsonb(item)::text AS serialized
            FROM groundloop_m5_requirement_pair_input AS item
            JOIN groundloop_m5_requirement_verifier_execution AS execution
              ON execution.pair_input_hash = item.pair_input_hash
            WHERE execution.logical_job_id = %s AND execution.attempt_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_m5_requirement_verifier_artifact AS item
            JOIN groundloop_m5_requirement_verifier_execution AS execution
              ON execution.artifact_id = item.artifact_id
            WHERE execution.logical_job_id = %s AND execution.attempt_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_semantic_observation AS item
            JOIN groundloop_m5_requirement_verifier_execution AS execution
              ON execution.observation_id = item.observation_id
            WHERE execution.logical_job_id = %s AND execution.attempt_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_m5_requirement_verifier_execution AS item
            WHERE item.logical_job_id = %s AND item.attempt_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_m5_attempt_result_artifact AS item
            WHERE item.logical_job_id = %s AND item.attempt_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_m5_job_attempt AS item
            WHERE item.logical_job_id = %s AND item.attempt_id = %s
            UNION ALL
            SELECT to_jsonb(item)::text
            FROM groundloop_m5_semantic_job AS item
            WHERE item.epoch_id = %s AND item.logical_job_id = %s
        ) AS verifier_rows
        ORDER BY serialized COLLATE "C"
        """,
        (
            logical_job_id,
            attempt_id,
            logical_job_id,
            attempt_id,
            logical_job_id,
            attempt_id,
            logical_job_id,
            attempt_id,
            logical_job_id,
            attempt_id,
            logical_job_id,
            attempt_id,
            epoch_id,
            logical_job_id,
        ),
    ).fetchall()
    if len(rows) != 7:
        raise ValidationError("verifier completion lacks its exact persistence rows")
    byte_count = sum(len(item.preimage) for item in archive_coordinates)
    hasher = hashlib.sha256()
    for row in rows:
        encoded = str(row[0]).encode("utf-8")
        frame = len(encoded).to_bytes(8, byteorder="big", signed=False)
        hasher.update(frame)
        hasher.update(encoded)
        byte_count += len(frame) + len(encoded)
    if len(hasher.digest()) != hashlib.sha256().digest_size:
        raise AssertionError("SHA-256 verifier instrumentation drift")
    return M5RuntimeWork(
        requirement_observation_artifact_count=1,
        requirement_effective_observation_count=int(active),
        requirement_inactive_completion_count=int(not active),
        bytes_hashed=byte_count,
        bytes_serialized=byte_count,
    )


def _insert_verifier_completion_contribution(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    attempt_id: str,
    artifact_hash: str,
    resulting_revision: int,
    work: M5RuntimeWork,
) -> None:
    kind = M5RuntimeWorkContributionKind.VERIFIER_COMPLETION
    key = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=kind,
        source_id=attempt_id,
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
    cursor.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_contribution ({}) VALUES ({})"
        ).format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        (
            epoch_id,
            *work.counter_values(),
            work.work_digest,
            kind.value,
            attempt_id,
            artifact_hash,
            key,
            resulting_revision,
        ),
    )


def _load_verifier_completion_contribution(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    attempt_id: str,
) -> tuple[M5RuntimeWork, str, str, int] | None:
    names = M5RuntimeWork.counter_names()
    row = cursor.execute(
        sql.SQL(
            "SELECT {}, work_digest, source_identity_hash, "
            "contribution_key_digest, applied_revision "
            "FROM groundloop_m5_runtime_work_contribution "
            "WHERE epoch_id = %s AND contribution_kind = "
            "'verifier_completion' AND source_id = %s"
        ).format(sql.SQL(", ").join(map(sql.Identifier, names))),
        (epoch_id, attempt_id),
    ).fetchone()
    if row is None:
        return None
    values = tuple(row)
    end = len(names)
    work = M5RuntimeWork(
        **dict(zip(names, map(int, values[:end]), strict=True)),
        work_digest=_text(values[end]),
    )
    return (
        work,
        _text(values[end + 1]),
        _text(values[end + 2]),
        int(values[end + 3]),
    )


def _settle_verifier_pending(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    owner_claim_id: str,
    expected_revision: int,
    resulting_revision: int,
) -> None:
    owner = cursor.execute(
        """
        SELECT required, answer_version_id
        FROM groundloop_claim
        WHERE claim_id = %s
        """,
        (owner_claim_id,),
    ).fetchone()
    if owner is None:
        raise ValidationError("verifier owner claim is not durable")
    owner_count, answer_count = _lock_and_validate_pending_counter_revisions(
        cursor, epoch_id=epoch_id, expected_revision=expected_revision
    )
    updated_owner = cursor.execute(
        """
        UPDATE groundloop_m5_owner_pending_counter
        SET verifier_job_count = verifier_job_count - 1
        WHERE epoch_id = %s AND owner_claim_id = %s
          AND updated_revision = %s AND verifier_job_count >= 1
        """,
        (epoch_id, owner_claim_id, expected_revision),
    ).rowcount
    if updated_owner != 1:
        raise ValidationError("verifier owner PENDING unit is absent")
    if bool(owner[0]):
        updated_answer = cursor.execute(
            """
            UPDATE groundloop_m5_answer_pending_counter
            SET verifier_job_count = verifier_job_count - 1
            WHERE epoch_id = %s AND answer_version_id = %s
              AND updated_revision = %s AND verifier_job_count >= 1
            """,
            (epoch_id, str(owner[1]), expected_revision),
        ).rowcount
        if updated_answer != 1:
            raise ValidationError("verifier answer PENDING unit is absent")

    owners_advanced = cursor.execute(
        """
        UPDATE groundloop_m5_owner_pending_counter
        SET updated_revision = %s
        WHERE epoch_id = %s AND updated_revision = %s
        """,
        (resulting_revision, epoch_id, expected_revision),
    ).rowcount
    answers_advanced = cursor.execute(
        """
        UPDATE groundloop_m5_answer_pending_counter
        SET updated_revision = %s
        WHERE epoch_id = %s AND updated_revision = %s
        """,
        (resulting_revision, epoch_id, expected_revision),
    ).rowcount
    if owners_advanced != owner_count or answers_advanced != answer_count:
        raise ValidationError("verifier PENDING cutoff lost rows")


def _validate_applied_replay(
    cursor: Cursor[Any],
    *,
    header: Any,
    lease: M5JobLease,
    supplied_job: M5LogicalJobSpec,
    pair_input: M5RequirementPairInput,
    verifier_artifact: M5RequirementVerifierArtifact,
    attempt_output: M5AttemptOutput,
    stored_job: Any,
    attempt: Any,
    accounting: RequirementExecutionAccounting,
    replay_revision: int | None,
) -> M5RequirementAttemptReturnReceipt | None:
    stored_result = _load_attempt_result(cursor, attempt_id=attempt.attempt.attempt_id)
    contribution = _load_verifier_completion_contribution(
        cursor,
        epoch_id=header.epoch_id,
        attempt_id=attempt.attempt.attempt_id,
    )
    verifier_dispositions = {
        M5AttemptDisposition.VERIFIER_COMPLETED_ACTIVE,
        M5AttemptDisposition.VERIFIER_COMPLETED_INACTIVE,
    }
    has_result = (
        stored_result is not None
        and stored_result.artifact.disposition in verifier_dispositions
    )
    if replay_revision is None and not has_result and contribution is None:
        return None
    if (
        replay_revision is None
        or not has_result
        or stored_result is None
        or contribution is None
    ):
        raise EventConflictError("verifier completion is only partially durable")
    active = (
        stored_result.artifact.disposition
        is M5AttemptDisposition.VERIFIER_COMPLETED_ACTIVE
    )
    if not _validate_verifier_closure_replay(
        cursor,
        epoch_id=header.epoch_id,
        job=supplied_job,
        attempt_id=attempt.attempt.attempt_id,
        pair_input=pair_input,
        artifact=verifier_artifact,
        eligible_for_currency=active,
    ):
        raise ValidationError("verifier completion lacks its immutable closure")
    stored_result.artifact.validate_job_shape(M5JobKind.VERIFY_REQUIREMENT_PAIR)
    completion = _stored_completion(stored_job)
    archive_reason: M5TerminalReason | None = None
    if not active:
        stored_archive_reason = stored_result.artifact.archive_reason
        if stored_archive_reason not in {
            M5AttemptArchiveReason.SUBJECT_INACTIVE,
            M5AttemptArchiveReason.CHUNK_INACTIVE,
        }:
            raise ValidationError(
                "inactive verifier completion lacks an inactivity reason"
            )
        assert stored_archive_reason is not None
        archive_reason = M5TerminalReason(stored_archive_reason.value)
    expected_completion = M5JobCompletion.build(
        job=supplied_job,
        terminal_state=(
            M5JobState.COMPLETED_ACTIVE if active else M5JobState.COMPLETED_INACTIVE
        ),
        result_artifact_id=verifier_artifact.artifact_id,
        result_artifact_hash=verifier_artifact.artifact_hash,
        archive_reason=archive_reason,
    )
    work, source_identity, contribution_key, applied_revision = contribution
    expected_key = digests.runtime_work_contribution_key_digest(
        epoch_id=header.epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.VERIFIER_COMPLETION,
        source_id=attempt.attempt.attempt_id,
    )
    expected_counts = (
        1,
        int(active),
        int(not active),
    )
    actual_counts = (
        work.requirement_observation_artifact_count,
        work.requirement_effective_observation_count,
        work.requirement_inactive_completion_count,
    )
    if (
        supplied_job != stored_job.spec
        or lease.attempt is None
        or not _same_attempt_identity(attempt.attempt, lease.attempt)
        or stored_result.output != attempt_output
        or stored_result.artifact.logical_job_id != supplied_job.logical_job_id
        or attempt.state != "completed"
        or attempt.attempt_output_digest != attempt_output.attempt_output_digest
        or attempt.attempt.attempt_work_digest
        != accounting.evidence.attempt_work.work_digest
        or stored_job.state
        is not (
            M5JobState.COMPLETED_ACTIVE if active else M5JobState.COMPLETED_INACTIVE
        )
        or completion != expected_completion
        or source_identity != stored_result.artifact.attempt_result_artifact_hash
        or contribution_key != expected_key
        or applied_revision != replay_revision
        or stored_job.completed_revision != replay_revision
        or actual_counts != expected_counts
    ):
        raise EventConflictError("verifier replay changed immutable completion")
    return M5RequirementAttemptReturnReceipt(
        disposition=M5RequirementReturnDisposition.APPLIED,
        logical_job_id=supplied_job.logical_job_id,
        attempt_id=attempt.attempt.attempt_id,
        resulting_revision=header.revision,
        exact_replay=True,
        execution_evidence_digest=accounting.evidence.evidence_digest,
        return_artifact_digest=(stored_result.artifact.attempt_result_artifact_hash),
        current_terminal_logical_result_hash=(
            _current_terminal_logical_result_hash(cursor, header=header)
        ),
        transition_anchor=None,
    )


def _insert_attempt_result(
    cursor: Cursor[Any],
    *,
    artifact: M5AttemptResultArtifact,
    output: M5AttemptOutput,
) -> None:
    cursor.execute(
        """
        INSERT INTO groundloop_m5_attempt_result_artifact (
            attempt_result_artifact_id, attempt_result_artifact_hash,
            attempt_output_digest, attempt_id, logical_job_id, job_epoch_id,
            payload_hash, execution_spec_hash, result_artifact_id,
            result_artifact_hash, job_state_at_receipt, job_state_after,
            disposition, activity_snapshot_epoch_id,
            activity_snapshot_revision, epoch_active, chunk_active,
            requirement_active, group_active, archive_reason,
            cancelled_by_event_id, cancelled_by_epoch_id, cancellation_reason
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, NULL, NULL
        )
        """,
        (
            artifact.attempt_result_artifact_id,
            artifact.attempt_result_artifact_hash,
            artifact.attempt_output_digest,
            artifact.attempt_id,
            artifact.logical_job_id,
            artifact.job_epoch_id,
            output.payload_hash,
            output.execution_spec_hash,
            output.result_artifact_id,
            output.result_artifact_hash,
            artifact.job_state_at_receipt.value,
            artifact.job_state_after.value,
            artifact.disposition.value,
            artifact.activity_snapshot_epoch_id,
            artifact.activity_snapshot_revision,
            artifact.epoch_active,
            artifact.chunk_active,
            artifact.requirement_active,
            artifact.group_active,
            None if artifact.archive_reason is None else artifact.archive_reason.value,
        ),
    )


def complete_m5_verifier(
    cursor: Cursor[Any],
    epoch_id: int,
    expected_revision: int,
    lease: M5JobLease,
    job: M5LogicalJobSpec,
    pair_input: M5RequirementPairInput,
    verifier_artifact: M5RequirementVerifierArtifact,
    attempt_output: M5AttemptOutput,
    execution_disposition: M5ExecutionEvidenceDisposition,
    attempt_work: M5RuntimeWork,
    attempt_timing: M5RuntimeTiming | None,
    *,
    failure_injector: RuntimeVerifierFailureInjector | None = None,
) -> M5RequirementAttemptReturnReceipt:
    """Settle one successful requirement-verifier return atomically.

    The caller owns the transaction.  R1 supports exact replay, every D24 late
    branch, and first inactive completion.  First all-active completion is a
    zero-write D25 blocker because no persisted matching implementation is yet
    authorized on this branch.
    """

    return _d34_complete_verifier_cursor(
        cursor,
        epoch_id,
        expected_revision,
        lease,
        job,
        pair_input,
        verifier_artifact,
        attempt_output,
        execution_disposition,
        attempt_work,
        attempt_timing,
        owner=None,
        failure_injector=failure_injector,
    )


@dataclass(frozen=True, slots=True)
class _d34_PendingPlan:
    owners: tuple[tuple[str, int, int, int, int], ...]
    answers: tuple[tuple[str, int, int, int, int], ...]
    owner_claim_id: str
    required_answer_id: str | None


def _d34_plan_pending(
    cursor: Cursor[Any],
    owner: _d34_CompletionOwner,
    pair_input: M5RequirementPairInput,
) -> _d34_PendingPlan:
    _d34_validate_owner(cursor, owner)
    claim = cursor.execute(
        "SELECT required,answer_version_id FROM groundloop_claim WHERE claim_id=%s",
        (pair_input.owner_claim_id,),
    ).fetchone()
    if claim is None:
        raise EventConflictError("D34 pending owner disappeared")
    projections = []
    for relation, key in (
        ("groundloop_m5_owner_pending_counter", "owner_claim_id"),
        ("groundloop_m5_answer_pending_counter", "answer_version_id"),
    ):
        rows = cursor.execute(
            sql.SQL(
                "SELECT {},broad_reverse_scope_count,forward_scope_count,"
                "verifier_job_count,updated_revision FROM {} WHERE epoch_id=%s "
                'ORDER BY {} COLLATE "C"'
            ).format(
                sql.Identifier(key), sql.Identifier(relation), sql.Identifier(key)
            ),
            (owner.epoch_id,),
        ).fetchall()
        values = tuple(
            (str(r[0]), int(r[1]), int(r[2]), int(r[3]), int(r[4])) for r in rows
        )
        if any(r[4] != owner.revision for r in values):
            raise EventConflictError("D34 pending plan revision diverged")
        projections.append(values)
    required_answer = str(claim[1]) if claim[0] else None
    if not any(r[0] == pair_input.owner_claim_id and r[3] >= 1 for r in projections[0]):
        raise EventConflictError("D34 owner verifier pending unit is absent")
    if required_answer is not None and not any(
        r[0] == required_answer and r[3] >= 1 for r in projections[1]
    ):
        raise EventConflictError("D34 answer verifier pending unit is absent")
    return _d34_PendingPlan(
        projections[0], projections[1], pair_input.owner_claim_id, required_answer
    )


def _d34_settle_pending(
    cursor: Cursor[Any],
    owner: _d34_CompletionOwner,
    plan: _d34_PendingPlan,
) -> None:
    for relation, key, rows, target in (
        (
            "groundloop_m5_owner_pending_counter",
            "owner_claim_id",
            plan.owners,
            plan.owner_claim_id,
        ),
        (
            "groundloop_m5_answer_pending_counter",
            "answer_version_id",
            plan.answers,
            plan.required_answer_id,
        ),
    ):
        for before in rows:
            actual = cursor.execute(
                sql.SQL(
                    "SELECT {},broad_reverse_scope_count,forward_scope_count,"
                    "verifier_job_count,updated_revision FROM {} "
                    "WHERE epoch_id=%s AND {}=%s FOR UPDATE"
                ).format(
                    sql.Identifier(key), sql.Identifier(relation), sql.Identifier(key)
                ),
                (owner.epoch_id, before[0]),
            ).fetchone()
            if actual is None or tuple(actual) != before:
                raise EventConflictError("D34 held pending before-image changed")
            updated = cursor.execute(
                sql.SQL(
                    "UPDATE {} SET verifier_job_count=%s,updated_revision=%s "
                    "WHERE epoch_id=%s AND {}=%s AND updated_revision=%s "
                    "AND broad_reverse_scope_count=%s AND forward_scope_count=%s "
                    "AND verifier_job_count=%s"
                ).format(sql.Identifier(relation), sql.Identifier(key)),
                (
                    before[3] - int(before[0] == target),
                    owner.revision + 1,
                    owner.epoch_id,
                    before[0],
                    owner.revision,
                    *before[1:4],
                ),
            ).rowcount
            if updated != 1:
                raise EventConflictError("D34 pending counter lost its exact CAS")


def _d34_read_accounting_before(
    cursor: Cursor[Any],
    owner: _d34_CompletionOwner,
) -> _d34_recovery.EventAccountingStart:
    names = M5RuntimeWork.counter_names()
    row = cursor.execute(
        sql.SQL(
            "SELECT {},work_digest,updated_revision,terminalized "
            "FROM groundloop_m5_runtime_work_accumulator WHERE epoch_id=%s"
        ).format(sql.SQL(",").join(map(sql.Identifier, names))),
        (owner.epoch_id,),
    ).fetchone()
    if row is None or int(row[-2]) != owner.revision or row[-1]:
        raise EventConflictError("D34 work before-point changed")
    work = _d34_recovery._work_from_row(tuple(row), digest_index=len(names))
    timing_names = (
        *_d34_recovery._TIMING_SUM_COLUMNS,
        *_d34_recovery._TIMING_COVERAGE_COLUMNS,
    )
    timing_row = cursor.execute(
        sql.SQL(
            "SELECT {},updated_revision,terminalized,pending_contribution_kind,"
            "pending_source_id,pending_contribution_key_digest,pending_anchor_revision "
            "FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id=%s"
        ).format(sql.SQL(",").join(map(sql.Identifier, timing_names))),
        (owner.epoch_id,),
    ).fetchone()
    if timing_row is None:
        raise EventConflictError("D34 timing before-point is absent")
    timing = _d34_recovery._timing_accumulator_from_row(tuple(timing_row))
    if timing.updated_revision != owner.revision or timing.terminalized:
        raise EventConflictError("D34 timing before-point changed")
    _ = timing.has_pending_anchor
    return _d34_recovery.EventAccountingStart(work, timing)


def _d34_install_matching_headers(
    cursor: Cursor[Any],
    owner: _d34_CompletionOwner,
    prepared: _d34_matching._PreparedMatchingTransition,
    failure_injector: RuntimeVerifierFailureInjector | None,
) -> None:
    _d34_validate_owner(cursor, owner)
    intent = prepared.official_intent
    _d34_matching._validate_prepared_identity(
        cursor, intent, prepared, phase=_d34_matching._MatchingPhase.READY_TO_ADVANCE
    )
    base, runtime = (
        prepared.base_header_after_image,
        prepared.runtime_header_after_image,
    )
    if (
        (base.epoch_id, runtime.epoch_id, base.revision, runtime.revision)
        != (owner.epoch_id, owner.epoch_id, owner.revision + 1, owner.revision + 1)
        or base.sealed
        or runtime.terminal
    ):
        raise EventConflictError("D34 prepared complete header coordinates changed")
    updated = cursor.execute(
        """UPDATE groundloop_epoch SET revision=%s,structural_status=%s,
             semantic_status=%s,evaluation_state=%s,publication_mode=%s,sealed_at=NULL
           WHERE epoch_id=%s AND revision=%s AND structural_status=%s
             AND semantic_status=%s AND evaluation_state=%s AND sealed_at IS NULL""",
        (
            base.revision,
            base.structural_status,
            base.semantic_status,
            base.evaluation_state,
            base.publication_mode,
            owner.epoch_id,
            owner.revision,
            owner.header.structural_status,
            owner.header.semantic_status,
            owner.header.evaluation_state,
        ),
    ).rowcount
    if updated != 1:
        raise EventConflictError("D34 base complete header lost its CAS")
    _inject(failure_injector, "d34_base_header_advanced")
    updated = cursor.execute(
        """UPDATE groundloop_m5_runtime_epoch SET revision=%s,runtime_state=%s,
             open_work_count=%s,open_scope_count=%s,blocking_failure_count=%s,
             terminal_at=NULL
           WHERE epoch_id=%s AND revision=%s AND runtime_state=%s
             AND open_work_count=%s AND open_scope_count=%s
             AND blocking_failure_count=%s AND terminal_at IS NULL""",
        (
            runtime.revision,
            runtime.runtime_state,
            runtime.open_work_count,
            runtime.open_scope_count,
            runtime.blocking_failure_count,
            owner.epoch_id,
            owner.revision,
            owner.header.runtime_state,
            owner.header.open_work_count,
            owner.header.open_scope_count,
            owner.header.blocking_failure_count,
        ),
    ).rowcount
    if updated != 1:
        raise EventConflictError("D34 runtime complete header lost its CAS")
    _inject(failure_injector, "d34_runtime_header_advanced")


def _d34_install_inactive_headers(
    cursor: Cursor[Any],
    owner: _d34_CompletionOwner,
    failure_injector: RuntimeVerifierFailureInjector | None,
) -> None:
    """One current inactive child: checked complete point after-images only."""
    _d34_validate_owner(cursor, owner)
    h = owner.header
    if h.open_work_count < 1:
        raise EventConflictError("D34 inactive child lacks its pending work unit")
    updated = cursor.execute(
        """UPDATE groundloop_epoch SET revision=%s,structural_status='committed',
             semantic_status='pending',evaluation_state='pending',
             publication_mode='provisional',sealed_at=NULL
           WHERE epoch_id=%s AND revision=%s AND structural_status=%s
             AND semantic_status=%s AND evaluation_state=%s AND sealed_at IS NULL""",
        (
            h.revision + 1,
            h.epoch_id,
            h.revision,
            h.structural_status,
            h.semantic_status,
            h.evaluation_state,
        ),
    ).rowcount
    if updated != 1:
        raise EventConflictError("D34 inactive base header lost its CAS")
    _inject(failure_injector, "d34_base_header_advanced")
    updated = cursor.execute(
        """UPDATE groundloop_m5_runtime_epoch SET revision=%s,
             runtime_state='semantic_pending',open_work_count=%s,
             open_scope_count=%s,blocking_failure_count=%s,terminal_at=NULL
           WHERE epoch_id=%s AND revision=%s AND runtime_state=%s
             AND open_work_count=%s AND open_scope_count=%s
             AND blocking_failure_count=%s AND terminal_at IS NULL""",
        (
            h.revision + 1,
            h.open_work_count - 1,
            h.open_scope_count,
            h.blocking_failure_count,
            h.epoch_id,
            h.revision,
            h.runtime_state,
            h.open_work_count,
            h.open_scope_count,
            h.blocking_failure_count,
        ),
    ).rowcount
    if updated != 1:
        raise EventConflictError("D34 inactive runtime header lost its CAS")
    _inject(failure_injector, "d34_runtime_header_advanced")


def _d34_insert_execution_contribution(
    cursor: Cursor[Any],
    accounting: RequirementExecutionAccounting,
) -> None:
    evidence = accounting.evidence
    kind = M5RuntimeWorkContributionKind.M5_ATTEMPT_EXECUTION
    key = digests.runtime_work_contribution_key_digest(
        epoch_id=evidence.epoch_id,
        contribution_kind=kind,
        source_id=evidence.attempt_id,
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
    cursor.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_contribution ({}) VALUES ({})"
        ).format(
            sql.SQL(",").join(map(sql.Identifier, columns)),
            sql.SQL(",").join(sql.Placeholder() for _ in columns),
        ),
        (
            evidence.epoch_id,
            *evidence.attempt_work.counter_values(),
            evidence.attempt_work.work_digest,
            kind.value,
            evidence.attempt_id,
            evidence.evidence_digest,
            key,
            accounting.anchor.anchor_revision,
        ),
    )


def _d34_finish_ordered_accounting(
    cursor: Cursor[Any],
    owner: _d34_CompletionOwner,
    accounting: RequirementExecutionAccounting,
    before: _d34_recovery.EventAccountingStart,
    completion_work: M5RuntimeWork,
    cumulative_work: M5RuntimeWork,
    result: M5AttemptResultArtifact,
    failure_injector: RuntimeVerifierFailureInjector | None,
) -> None:
    # 15d keys are sorted by (kind, source): m5_attempt_execution before verifier.
    _d34_insert_execution_contribution(cursor, accounting)
    _inject(failure_injector, "d34_execution_contribution_inserted")
    _insert_verifier_completion_contribution(
        cursor,
        epoch_id=owner.epoch_id,
        attempt_id=result.attempt_id,
        artifact_hash=result.attempt_result_artifact_hash,
        resulting_revision=owner.revision + 1,
        work=completion_work,
    )
    _inject(failure_injector, "verifier_contribution_inserted")
    locked = _d34_recovery._lock_work_accumulator(
        cursor,
        epoch_id=owner.epoch_id,
        expected_revision=owner.revision,
    )
    if locked != before.work:
        raise EventConflictError(
            "D34 work accumulator differs from planned before-point"
        )
    names = M5RuntimeWork.counter_names()
    updated = cursor.execute(
        sql.SQL(
            "UPDATE groundloop_m5_runtime_work_accumulator SET {},work_digest=%s,"
            "updated_revision=%s,updated_at=clock_timestamp() "
            "WHERE epoch_id=%s AND updated_revision=%s AND work_digest=%s "
            "AND NOT terminalized"
        ).format(
            sql.SQL(",").join(sql.SQL("{}=%s").format(sql.Identifier(n)) for n in names)
        ),
        (
            *cumulative_work.counter_values(),
            cumulative_work.work_digest,
            owner.revision + 1,
            owner.epoch_id,
            owner.revision,
            before.work.work_digest,
        ),
    ).rowcount
    if updated != 1:
        raise EventConflictError("D34 work accumulator lost its single CAS")
    _inject(failure_injector, "requirement_work_accumulator_updated")
    evidence, observation = accounting.evidence, accounting.observation
    cursor.execute(
        """INSERT INTO groundloop_m5_runtime_timing_contribution (
             epoch_id,subgraph,attempt_id,execution_evidence_digest,
             required_interval_observed,coordinator_non_db_non_neural_ns,
             neural_wall_ns,postgres_roundtrip_wall_ns,external_io_wall_ns,
             end_to_end_wall_ns,postgres_server_execution_ns,postgres_lock_wait_ns,
             postgres_wal_bytes,postgres_shared_block_reads,observation_digest,
             attempt_timing_digest
           ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (
            owner.epoch_id,
            evidence.subgraph.value,
            evidence.attempt_id,
            evidence.evidence_digest,
            observation.required_interval_observed,
            *_d34_recovery._timing_values(observation),
            observation.observation_digest,
            evidence.attempt_timing_digest,
        ),
    )
    _inject(failure_injector, "d34_attempt_timing_inserted")
    _d34_recovery._resolve_pending_anchor(
        cursor,
        epoch_id=owner.epoch_id,
        accumulator=before.timing,
    )
    _inject(failure_injector, "d34_prior_anchor_resolved")
    timing = _d34_recovery._lock_timing_accumulator(
        cursor,
        epoch_id=owner.epoch_id,
        expected_revision=owner.revision,
    )
    if timing != before.timing:
        raise EventConflictError(
            "D34 timing accumulator differs from planned before-point"
        )
    _d34_recovery._finish_requirement_execution_timing(
        cursor,
        epoch_id=owner.epoch_id,
        expected_revision=owner.revision,
        start=before,
        accounting=accounting,
        same_revision=False,
    )
    _inject(failure_injector, "requirement_timing_accumulator_updated")


@dataclass(frozen=True, slots=True)
class _d34_ByteRegion:
    name: str
    bytes_hashed: int
    bytes_serialized: int


def _d34_state_point(value: object) -> dict[str, Any]:
    point = asdict(value)  # type: ignore[call-overload]
    if "state" in point:
        return {**point.pop("state"), **point}
    return dict(point)


def _d34_state_preimage(kind: str, point: dict[str, Any]) -> bytes:
    """Size unchanged frozen state recipes; this is diagnostic, not production.

    No new state identity is created, nor can this choose an after-state. Inputs
    are already-held effective points or the accepted prepared after payloads.
    """
    t, i, h, q = (
        _d34_text_field,
        _d34_int_field,
        _d34_hash_field,
        _d34_sequence_field,
    )
    p = digests.stable_m5_preimage
    if kind == "requirement":
        return p(
            "m5-requirement-state-artifact-v2",
            t(point["requirement_version_id"]),
            q(h(v) for v in point["witness_hashes"]),
            q(t(v) for v in point["supporting_observation_ids"]),
            i(point["witness_count"]),
            _d34_bool_field(point["satisfied"]),
            t(point["decision_policy_version"]),
        )
    if kind == "group":
        return p(
            "m5-group-state-artifact-v2",
            t(point["group_version_id"]),
            i(point["requirement_count"]),
            i(point["satisfied_count"]),
            i(point["matching_size"]),
            _d34_bool_field(point["complete"]),
            t(point["decision_policy_version"]),
            _d34_option_field(
                None
                if point["certificate_digest"] is None
                else h(point["certificate_digest"])
            ),
        )
    if kind == "claim":
        return p(
            "m5-claim-state-artifact-v2",
            t(point["claim_id"]),
            i(point["support_count"]),
            i(point["refute_count"]),
            _d34_option_field(
                None
                if point["best_support_score"] is None
                else _d34_f64_field(float(point["best_support_score"]))
            ),
            _d34_option_field(
                None
                if point["best_refute_score"] is None
                else _d34_f64_field(float(point["best_refute_score"]))
            ),
            q(t(v) for v in point["supporting_observation_ids"]),
            q(t(v) for v in point["refuting_observation_ids"]),
            i(point["complete_group_count"]),
            q(t(v) for v in point["complete_group_ids"]),
            _d34_enum_field(point["status"]),
            t(point["decision_policy_version"]),
            h(point["certificate_digest"]),
        )
    if kind == "answer":
        return p(
            "m5-answer-state-artifact-v2",
            t(point["answer_version_id"]),
            i(point["required_claim_count"]),
            i(point["supported_count"]),
            i(point["unsupported_count"]),
            i(point["refuted_count"]),
            i(point["conflicted_count"]),
            _d34_enum_field(point["status"]),
        )
    raise AssertionError("unknown D34 state family")


def _d34_native_matching_regions(
    cursor: Cursor[Any], matching: _d34_matching._PreparedMatchingTransition
) -> tuple[_d34_ByteRegion, ...]:
    """Size actual native requirement producers, excluding validation rebuilds."""
    intent = matching.official_intent
    artifact = matching.prewrite_patch_artifact
    plan = matching.physical_and_logical_write_plan
    source = _d34_matching._requirement_completion_source(
        cursor,
        epoch_id=intent.resulting_epoch_id,
        expected_runtime_revision=intent.before_revision,
        resulting_revision=intent.resulting_revision,
        proposed_attempt_id=intent.source_id,
        expected_source_identity_hash=intent.source_identity_hash,
    )
    regions: list[_d34_ByteRegion] = []

    def add(name: str, size: int, hashes: int = 1, encodes: int = 1) -> None:
        regions.append(_d34_ByteRegion(name, size * hashes, size * encodes))

    t, i, h, q = (
        _d34_text_field,
        _d34_int_field,
        _d34_hash_field,
        _d34_sequence_field,
    )
    p = digests.stable_m5_preimage
    intent_bytes = p(
        "m5-persisted-matching-transition-intent-v1",
        _d34_enum_field(intent.source_kind),
        t(intent.source_id),
        h(intent.source_identity_hash),
        i(intent.before_epoch_id),
        i(intent.before_revision),
        i(intent.resulting_epoch_id),
        i(intent.resulting_revision),
        t(intent.decision_policy_version),
        q(
            q(
                (
                    t(s.group_version_id),
                    i(s.requirement_count),
                    q(q((i(o), t(r))) for o, r in s.requirements),
                )
            )
            for s in intent.group_shapes
        ),
        q(t(v) for v in intent.observation_ids),
        q(q((t(g), i(o), h(v), t(r))) for g, o, v, r in intent.edge_keys),
        q(q((t(g), h(v))) for g, v in intent.mask_keys),
        *(
            q(t(v) for v in values)
            for values in (
                intent.hall_group_ids,
                intent.requirement_state_ids,
                intent.group_state_ids,
                intent.claim_state_ids,
                intent.answer_state_ids,
                intent.group_certificate_ids,
                intent.claim_certificate_ids,
            )
        ),
    )
    add("d25.intent", len(intent_bytes))
    before: dict[str, dict[str, dict[str, Any]]] = {}
    after: dict[str, dict[str, dict[str, Any]]] = {}
    for kind, ids, writes in (
        ("requirement", intent.requirement_state_ids, plan.requirement_state_rows),
        ("group", intent.group_state_ids, plan.group_state_rows),
        ("claim", intent.claim_state_ids, plan.claim_state_rows),
        ("answer", intent.answer_state_ids, plan.answer_state_rows),
    ):
        read = {
            "requirement": _d34_matching._effective_requirement_state,
            "group": _d34_matching._effective_group_state,
            "claim": _d34_matching._effective_claim_state,
            "answer": _d34_matching._effective_answer_state,
        }[kind]
        point = _d34_state_point(read(cursor, source, lock=False))
        key = {
            "requirement": "requirement_version_id",
            "group": "group_version_id",
            "claim": "claim_id",
            "answer": "answer_version_id",
        }[kind]
        before[kind] = {str(point[key]): point}
        if any(identity not in before[kind] for identity in ids):
            raise EventConflictError("D34 ledger contains an unowned logical state")
        after[kind] = dict(before[kind])
        key = {
            "requirement": "requirement_version_id",
            "group": "group_version_id",
            "claim": "claim_id",
            "answer": "answer_version_id",
        }[kind]
        for write in writes:
            write_fields = asdict(write)
            point = (
                write_fields
                if kind == "answer"
                else {**write_fields.pop("state"), **write_fields}
            )
            identity = point[key]
            after[kind][identity] = point
            add(
                f"d25.state.{kind}.{identity}.before",
                len(_d34_state_preimage(kind, before[kind][identity])),
                1,
                1,
            )
            add(
                f"d25.state.{kind}.{identity}.after",
                len(_d34_state_preimage(kind, point)),
                1,
                1,
            )
    outputs = {
        (kind, identity): value
        for kind, identity, value in artifact.logical_patch.output_records
    }
    for identity, point in after["group"].items():
        if not point["complete"]:
            continue
        desired = outputs.get(("group_certificate", identity))
        if desired is None:
            desired = _d34_matching._stored_group_certificate_artifact(
                cursor, str(point["certificate_digest"]).strip(), lock=False
            )
        if (
            not isinstance(desired, _d34_GroupMatchingCertificateArtifact)
            or desired.group_version_id != identity
        ):
            raise EventConflictError("D34 byte ledger desired group artifact absent")
        encoded = p(
            "m5-group-certificate-v1",
            t(desired.decision_policy_version),
            t(desired.group_version_id),
            i(len(desired.rows)),
            q(
                q(
                    (
                        i(r.requirement_ordinal),
                        t(r.requirement_version_id),
                        h(r.text_hash),
                        t(r.selected_observation_id),
                    )
                )
                for r in desired.rows
            ),
        )
        add(
            f"d25.desired_group_certificate.{identity}",
            len(encoded),
            2
            + int(
                before["group"][identity]["certificate_digest"]
                != desired.certificate_digest
            ),
            2
            + int(
                before["group"][identity]["certificate_digest"]
                != desired.certificate_digest
            ),
        )
    for identity, point in after["claim"].items():
        supports, refutes, groups = (
            point["supporting_observation_ids"],
            point["refuting_observation_ids"],
            point["complete_group_ids"],
        )
        # Only the fixed HASH field width is needed here. This is diagnostic
        # sizing, not a fabricated certificate or an expected-state input.
        encoded = p(
            "m5-claim-certificate-v2",
            t(identity),
            t(intent.decision_policy_version),
            _d34_enum_field("direct" if supports else "group" if groups else "none"),
            _d34_option_field(t(supports[0]) if supports else None),
            _d34_option_field(t(groups[0]) if groups and not supports else None),
            _d34_option_field(h("0" * 64) if groups and not supports else None),
            _d34_option_field(t(refutes[0]) if refutes else None),
        )
        add(f"d25.desired_claim_certificate.{identity}", len(encoded), 2, 2)
    for binding in artifact.logical_patch.binding_rows:
        encoded = p(
            "m5-persisted-certificate-binding-row-v1",
            _d34_enum_field(binding.kind),
            i(binding.epoch_id),
            t(binding.object_id),
            i(binding.valid_from_revision),
            _d34_option_field(
                None
                if binding.valid_to_revision is None
                else i(binding.valid_to_revision)
            ),
            h(binding.certificate_digest),
        )
        add(f"d25.binding.{binding.kind}.{binding.object_id}", len(encoded), 1, 1)
    for family, preimages in (
        ("observation", artifact.observation_change_preimages),
        ("edge", artifact.edge_change_preimages),
        ("mask", artifact.mask_change_preimages),
        ("hall", artifact.hall_change_preimages),
    ):
        add(f"d25.changes.{family}", sum(map(len, preimages)), 1, 2)
    add("d25.shape_set", len(artifact.group_shape_set_preimage), 1, 2)
    add("d25.logical_patch", len(artifact.logical_patch.patch_preimage), 1, 2)
    add("d25.logical_output", len(artifact.logical_patch.logical_output_preimage), 1, 2)
    work_bytes = p(
        "m5-matching-work-v1",
        *(i(v) for v in _d34_m5_overlay_work_values(artifact.work)),
    )
    add("d25.matching_work.patch_and_contribution", len(work_bytes), 2, 2)
    row = cursor.execute(
        sql.SQL(
            "SELECT {},matching_work_digest,updated_revision "
            "FROM groundloop_m5_matching_work_accumulator WHERE epoch_id=%s"
        ).format(
            sql.SQL(",").join(map(sql.Identifier, _d34_MATCHING_WORK_COUNTER_NAMES))
        ),
        (intent.resulting_epoch_id,),
    ).fetchone()
    if row is None or int(row[-1]) != matching.prewrite_matching_revision:
        raise EventConflictError(
            "D34 matching ledger lacks its exact accumulator point"
        )
    prior = tuple(int(v) for v in row[:-2])
    if str(row[-2]).strip() != digests.matching_work_digest(prior):
        raise EventConflictError("D34 matching ledger accumulator digest changed")
    total = tuple(
        a + b
        for a, b in zip(prior, _d34_m5_overlay_work_values(artifact.work), strict=True)
    )
    add(
        "d25.matching_work.accumulated",
        len(p("m5-matching-work-v1", *(i(v) for v in total))),
    )
    add("d25.outer_patch", len(artifact.patch_preimage), 1, 2)
    patch = artifact.patch
    contribution_bytes = p(
        "m5-matching-work-contribution-v1",
        i(patch.resulting_epoch_id),
        _d34_enum_field(patch.source_kind),
        t(patch.source_id),
        h(patch.source_identity_hash),
        i(patch.before_epoch_id),
        i(patch.before_revision),
        i(patch.resulting_revision),
        h(patch.patch_digest),
        h(patch.matching_work_digest),
    )
    add("d25.finalizer_contribution", len(contribution_bytes))
    for index, (relation, coordinate_key) in enumerate(
        _d34_matching_reservation_coordinates(cursor, matching)
    ):
        # This is the unchanged signed length-framed advisory-key producer,
        # not typed M5 digest framing. Full coordinates, never truncated keys,
        # determine multiplicity. Reservation-proof rehashes are excluded.
        size = len(b"m5-d28-matching-absence-v1") + sum(
            8 + len(str(part).encode("utf-8")) for part in (relation, *coordinate_key)
        )
        add(f"d25.absence_reservation.{index}.{relation}", size)
    return tuple(regions)


def _d34_matching_reservation_coordinates(
    cursor: Cursor[Any],
    matching: _d34_matching._PreparedMatchingTransition,
) -> tuple[tuple[str, tuple[object, ...]], ...]:
    """Enumerate actual requirement derive reservations, not extra authority."""
    intent = matching.official_intent
    k, s = intent.resulting_epoch_id, intent.resulting_revision
    currency = matching.observation_currency_before_images[0]
    source = _d34_matching._requirement_completion_source(
        cursor,
        epoch_id=k,
        expected_runtime_revision=intent.before_revision,
        resulting_revision=s,
        proposed_attempt_id=intent.source_id,
        expected_source_identity_hash=intent.source_identity_hash,
    )
    coordinates: list[tuple[str, tuple[object, ...]]] = [
        ("groundloop_m5_working_currency_history", (*currency.key, s)),
        ("groundloop_m5_working_currency_history#open", currency.key),
    ]
    for relation, keys in (
        ("matching_observation_working", ((k, v) for v in intent.observation_ids)),
        ("matching_edge_working", ((k, r, h) for _, _, h, r in intent.edge_keys)),
        ("matching_hash_mask_working", ((k, *v) for v in intent.mask_keys)),
        ("matching_hall_working", ((k, v) for v in intent.hall_group_ids)),
        ("working_requirement_state", ((k, source.requirement_version_id),)),
        ("working_group_state", ((k, source.group_version_id),)),
        ("working_claim_state", ((k, source.owner_claim_id),)),
        ("working_answer_state", ((k, source.answer_version_id),)),
    ):
        coordinates.extend(("groundloop_m5_" + relation, key) for key in keys)
    plan = matching.physical_and_logical_write_plan
    group = _d34_matching._effective_group_state(cursor, source, lock=False)
    claim = _d34_matching._effective_claim_state(cursor, source, lock=False)
    group_after = plan.group_state_rows[0] if plan.group_state_rows else group
    claim_after = plan.claim_state_rows[0] if plan.claim_state_rows else claim
    if group_after.certificate_digest is not None:
        digest = group_after.certificate_digest
        coordinates.append(("groundloop_m5_group_certificate_artifact", (digest,)))
        if (
            cursor.execute(
                "SELECT 1 FROM groundloop_m5_group_certificate_artifact "
                "WHERE certificate_digest=%s",
                (digest,),
            ).fetchone()
            is None
        ):
            desired = next(
                (
                    v
                    for kind, _, v in (
                        matching.prewrite_patch_artifact.logical_patch.output_records
                    )
                    if kind == "group_certificate"
                ),
                None,
            )
            if not isinstance(desired, _d34_GroupMatchingCertificateArtifact):
                raise EventConflictError("D34 missing prospective dense certificate")
            coordinates.extend(
                (
                    "groundloop_m5_group_certificate_artifact_row",
                    (digest, row.requirement_ordinal),
                )
                for row in desired.rows
            )
    coordinates.append(
        ("groundloop_m5_claim_certificate_artifact", (claim_after.certificate_digest,))
    )
    if group.certificate_digest != group_after.certificate_digest:
        coordinates.extend(
            (
                (
                    "groundloop_m5_working_group_certificate_binding",
                    (k, source.group_version_id, s),
                ),
                (
                    "groundloop_m5_working_group_certificate_binding#open",
                    (k, source.group_version_id),
                ),
            )
        )
    claim_binding = _d34_matching._effective_claim_binding(cursor, source, lock=False)
    if claim_binding is None:
        raise EventConflictError("D34 missing held claim binding")
    if claim.certificate_digest != claim_after.certificate_digest or (
        bool(plan.claim_state_rows) and not claim_binding.working
    ):
        coordinates.extend(
            (
                (
                    "groundloop_m5_working_claim_certificate_binding",
                    (k, source.owner_claim_id, s),
                ),
                (
                    "groundloop_m5_working_claim_certificate_binding#open",
                    (k, source.owner_claim_id),
                ),
            )
        )
    return tuple(coordinates)


def _d34_source_regions(
    result: M5AttemptResultArtifact,
    completion: M5JobCompletion,
    accounting: RequirementExecutionAccounting,
    archive: tuple[_d34_ArchiveCoordinate, ...],
    artifact: M5RequirementVerifierArtifact,
    before: _d34_recovery.EventAccountingStart,
) -> tuple[_d34_ByteRegion, ...]:
    """Size only native store first producers; external model DTOs are excluded."""
    p = digests.stable_m5_preimage
    t, i, h, e, b, o = (
        _d34_text_field,
        _d34_int_field,
        _d34_hash_field,
        _d34_enum_field,
        _d34_bool_field,
        _d34_option_field,
    )
    regions = [
        _d34_ByteRegion(f"d34.alias.{index}", len(c.preimage), len(c.preimage))
        for index, c in enumerate(archive)
    ]

    def add(name: str, encoded: bytes, count: int = 1) -> None:
        regions.append(
            _d34_ByteRegion(name, len(encoded) * count, len(encoded) * count)
        )

    add(
        "result.hash",
        p(
            "m5-attempt-result-artifact-v2",
            h(result.attempt_output_digest),
            t(result.attempt_id),
            t(result.logical_job_id),
            i(result.job_epoch_id),
            e(result.job_state_at_receipt),
            e(result.job_state_after),
            e(result.disposition),
            i(result.activity_snapshot_epoch_id),
            i(result.activity_snapshot_revision),
            b(result.epoch_active),
            o(None if result.chunk_active is None else b(result.chunk_active)),
            o(
                None
                if result.requirement_active is None
                else b(result.requirement_active)
            ),
            o(None if result.group_active is None else b(result.group_active)),
            o(None if result.archive_reason is None else e(result.archive_reason)),
            o(None),
            o(None),
            o(None),
        ),
    )
    add(
        "result.id",
        p(
            "m5-attempt-result-id-v2",
            t(result.attempt_id),
            h(result.attempt_result_artifact_hash),
        ),
    )
    add(
        "job.completion",
        p(
            "m5-job-completion-v2",
            t(completion.logical_job_id),
            h(completion.payload_hash),
            h(completion.execution_spec_hash),
            e(completion.terminal_state),
            o(t(artifact.artifact_id)),
            o(h(artifact.artifact_hash)),
            o(None),
            o(None),
            o(
                None
                if completion.archive_reason is None
                else e(completion.archive_reason)
            ),
        ),
    )
    add(
        "semantic.observation.id",
        p(
            "m5-requirement-semantic-observation-v2",
            h(artifact.artifact_id),
            t("verify_requirement_v1"),
        ),
        3,
    )
    evidence, obs, anchor = (
        accounting.evidence,
        accounting.observation,
        accounting.anchor,
    )
    values = _d34_recovery._timing_values(obs)
    add(
        "timing.observation",
        p(
            "m5-runtime-timing-observation-v1",
            b(obs.required_interval_observed),
            *(o(None if v is None else i(v)) for v in values),
        ),
    )
    add(
        "timing.attempt",
        p(
            "m5-attempt-runtime-timing-v1",
            i(evidence.epoch_id),
            e(evidence.subgraph),
            t(evidence.attempt_id),
            h(obs.observation_digest),
        ),
    )
    add(
        "execution.evidence",
        p(
            "m5-attempt-execution-evidence-v1",
            i(evidence.epoch_id),
            e(evidence.subgraph),
            t(evidence.attempt_id),
            e(evidence.disposition),
            h(evidence.result_or_error_hash),
            h(evidence.attempt_work.work_digest),
            h(evidence.attempt_timing_digest),
        ),
    )
    add(
        "completion.contribution.key",
        p(
            "m5-runtime-work-contribution-key-v1",
            i(evidence.epoch_id),
            e(anchor.contribution_kind),
            t(evidence.attempt_id),
        ),
        2,
    )
    add(
        "execution.contribution.key",
        p(
            "m5-runtime-work-contribution-key-v1",
            i(evidence.epoch_id),
            e(M5RuntimeWorkContributionKind.M5_ATTEMPT_EXECUTION),
            t(evidence.attempt_id),
        ),
    )
    if before.timing.has_pending_anchor:
        prior = before.timing
        assert prior.pending_contribution_kind is not None
        assert prior.pending_source_id is not None
        assert prior.pending_contribution_key_digest is not None
        assert prior.pending_anchor_revision is not None
        add(
            "prior.missing.timing",
            p(
                "m5-runtime-timing-observation-v1",
                b(False),
                *(o(None) for _ in range(9)),
            ),
        )
        # Only the invariant HASH field width is needed; no timing DTO/hash is built.
        add(
            "prior.transition.timing",
            p(
                "m5-transition-call-timing-v1",
                i(evidence.epoch_id),
                e(prior.pending_contribution_kind),
                t(prior.pending_source_id),
                h(prior.pending_contribution_key_digest),
                i(prior.pending_anchor_revision),
                h("0" * 64),
            ),
        )
    return tuple(regions)


def _d34_complete_work(
    cursor: Cursor[Any],
    prepared: _d34_matching._PreparedMatchingTransition | None,
    result: M5AttemptResultArtifact,
    completion: M5JobCompletion,
    accounting: RequirementExecutionAccounting,
    archive: tuple[_d34_ArchiveCoordinate, ...],
    artifact: M5RequirementVerifierArtifact,
    before: _d34_recovery.EventAccountingStart,
) -> tuple[M5RuntimeWork, M5RuntimeWork]:
    counts = _d34_matching._D24OwnedWriteCounts()
    native_regions: tuple[_d34_ByteRegion, ...] = ()
    if prepared is not None:
        counts = prepared.d24_owned_planned_write_counts
        plan = prepared.physical_and_logical_write_plan
        if (
            counts.group_state_write_count,
            counts.claim_state_write_count,
            counts.answer_state_write_count,
            counts.certificate_binding_write_count,
            counts.public_delta_write_count,
            prepared.requirement_state_write_count_diagnostic,
        ) != (
            len(plan.group_state_rows),
            len(plan.claim_state_rows),
            len(plan.answer_state_rows),
            len(plan.group_binding_rows) + len(plan.claim_binding_rows),
            sum(
                int(kind == "status_delta")
                for kind, _, _ in (
                    prepared.prewrite_patch_artifact.logical_patch.output_records
                )
            ),
            len(plan.requirement_state_rows),
        ):
            raise EventConflictError("D34 prepared physical work diagnostic changed")
        native_regions = _d34_native_matching_regions(cursor, prepared)
    regions = (
        *native_regions,
        *_d34_source_regions(result, completion, accounting, archive, artifact, before),
    )
    names = M5RuntimeWork.counter_names()
    values: dict[str, int] = dict.fromkeys(names, 0)
    values.update(
        requirement_observation_artifact_count=1,
        requirement_effective_observation_count=int(prepared is not None),
        requirement_inactive_completion_count=int(prepared is None),
        group_state_write_count=counts.group_state_write_count,
        claim_state_write_count=counts.claim_state_write_count,
        answer_state_write_count=counts.answer_state_write_count,
        certificate_binding_write_count=counts.certificate_binding_write_count,
        bytes_hashed=sum(r.bytes_hashed for r in regions),
        bytes_serialized=sum(r.bytes_serialized for r in regions),
    )
    base_h, base_s = values["bytes_hashed"], values["bytes_serialized"]
    prior = tuple(
        a + b
        for a, b in zip(
            before.work.counter_values(),
            accounting.evidence.attempt_work.counter_values(),
            strict=True,
        )
    )
    for _ in range(16):
        total = tuple(a + values[n] for a, n in zip(prior, names, strict=True))
        size = sum(
            len(
                digests.stable_m5_preimage(
                    "m5-runtime-work-v2", *(_d34_int_field(v) for v in row)
                )
            )
            for row in (tuple(values[n] for n in names), total)
        )
        if (values["bytes_hashed"], values["bytes_serialized"]) == (
            base_h + size,
            base_s + size,
        ):
            return M5RuntimeWork(**values, work_digest=""), M5RuntimeWork(
                **dict(zip(names, total, strict=True)), work_digest=""
            )
        values["bytes_hashed"], values["bytes_serialized"] = (
            base_h + size,
            base_s + size,
        )
    raise EventConflictError("D34 byte ledger did not reach its bounded fixed point")


def _d34_apply_current_completion(
    cursor: Cursor[Any],
    *,
    owner: _d34_CompletionOwner,
    lease: M5JobLease,
    job: M5LogicalJobSpec,
    pair_input: M5RequirementPairInput,
    verifier_artifact: M5RequirementVerifierArtifact,
    attempt_output: M5AttemptOutput,
    accounting: RequirementExecutionAccounting,
    failure_injector: RuntimeVerifierFailureInjector | None,
    activity: tuple[bool, bool, bool, bool] = (True, True, True, True),
) -> M5RequirementAttemptReturnReceipt:
    _d34_validate_owner(cursor, owner)
    assert lease.attempt is not None
    attempt = lease.attempt
    active = all(activity)
    archive_reason = (
        None
        if active
        else M5AttemptArchiveReason.SUBJECT_INACTIVE
        if not activity[2] or not activity[3]
        else M5AttemptArchiveReason.CHUNK_INACTIVE
    )
    terminal_state = (
        M5JobState.COMPLETED_ACTIVE if active else M5JobState.COMPLETED_INACTIVE
    )
    result = M5AttemptResultArtifact.build(
        attempt_output=attempt_output,
        job_state_at_receipt=M5JobState.RUNNING,
        job_state_after=terminal_state,
        disposition=(
            M5AttemptDisposition.VERIFIER_COMPLETED_ACTIVE
            if active
            else M5AttemptDisposition.VERIFIER_COMPLETED_INACTIVE
        ),
        activity_snapshot_epoch_id=owner.epoch_id,
        activity_snapshot_revision=owner.revision,
        archive_reason=archive_reason,
        epoch_active=activity[0],
        chunk_active=activity[1],
        requirement_active=activity[2],
        group_active=activity[3],
    )
    result.validate_job_shape(job.job_kind)
    completion = M5JobCompletion.build(
        job=job,
        terminal_state=terminal_state,
        archive_reason=None
        if archive_reason is None
        else M5TerminalReason(archive_reason.value),
        result_artifact_id=verifier_artifact.artifact_id,
        result_artifact_hash=verifier_artifact.artifact_hash,
    )
    reserved = cursor.execute(
        """UPDATE groundloop_m5_job_attempt
             SET attempt_state='result_reserved',attempt_output_digest=%s
           WHERE attempt_id=%s AND logical_job_id=%s AND attempt_state='dispatched'
             AND attempt_output_digest IS NULL AND lease_token_hash=%s
             AND lease_expires_at=%s AND execution_spec_hash=%s""",
        (
            attempt_output.attempt_output_digest,
            attempt.attempt_id,
            job.logical_job_id,
            attempt.lease_token_hash,
            attempt.lease_expires_at,
            attempt.execution_spec_hash,
        ),
    ).rowcount
    if reserved != 1:
        raise EventConflictError("D34 output reservation lost its exact CAS")
    _inject(failure_injector, "verifier_output_reserved")
    _d34_lock_provenance_parents(cursor, pair_input, verifier_artifact)
    dispatch = cursor.execute(
        "SELECT record_digest FROM groundloop_m5_dispatch_record "
        "WHERE epoch_id=%s AND attempt_id=%s FOR KEY SHARE",
        (owner.epoch_id, attempt.attempt_id),
    ).fetchone()
    if dispatch is None or _text(dispatch[0]) != lease.dispatch_record_digest:
        raise EventConflictError("D34 immutable dispatch parent changed")
    _d34_recovery._insert_requirement_execution_evidence(cursor, accounting)
    _inject(failure_injector, "d34_execution_evidence_inserted")
    archive = _persist_verifier_closure(
        cursor,
        epoch_id=owner.epoch_id,
        job=job,
        attempt_id=attempt.attempt_id,
        pair_input=pair_input,
        artifact=verifier_artifact,
        attempt_result=result,
        attempt_output=attempt_output,
        eligible_for_currency=active,
        failure_injector=failure_injector,
    )
    intent = None
    prepared = None
    if active:
        intent = _d34_matching.derive_matching_transition_intent(
            cursor,
            epoch_id=owner.epoch_id,
            expected_runtime_revision=owner.revision,
            resulting_revision=owner.revision + 1,
            source_kind=M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION,
            source_id=attempt.attempt_id,
            expected_source_identity_hash=result.attempt_result_artifact_hash,
        )
        _inject(failure_injector, "d34_matching_derived")
        prepared = _d34_matching._prepare_matching_transition(cursor, intent)
        _inject(failure_injector, "d34_matching_prepared")
    pending = _d34_plan_pending(cursor, owner, pair_input)
    before = _d34_read_accounting_before(cursor, owner)
    work, total = _d34_complete_work(
        cursor,
        prepared,
        result,
        completion,
        accounting,
        archive,
        verifier_artifact,
        before,
    )
    if prepared is not None and intent is not None:
        _d34_install_matching_headers(cursor, owner, prepared, failure_injector)
        _d34_matching._stage_prepared_matching_transition(cursor, intent, prepared)
        _inject(failure_injector, "d34_matching_staged")
    else:
        _d34_install_inactive_headers(cursor, owner, failure_injector)
    completed = cursor.execute(
        """UPDATE groundloop_m5_job_attempt SET attempt_state='completed',
             finished_at=clock_timestamp(),attempt_work_digest=%s
           WHERE attempt_id=%s AND logical_job_id=%s AND attempt_state='result_reserved'
             AND attempt_output_digest=%s AND lease_token_hash=%s
             AND lease_expires_at=%s AND execution_spec_hash=%s""",
        (
            accounting.evidence.attempt_work.work_digest,
            attempt.attempt_id,
            job.logical_job_id,
            attempt_output.attempt_output_digest,
            attempt.lease_token_hash,
            attempt.lease_expires_at,
            attempt.execution_spec_hash,
        ),
    ).rowcount
    if completed != 1:
        raise EventConflictError("D34 attempt terminalization lost its exact CAS")
    _inject(failure_injector, "verifier_attempt_completed")
    completed = cursor.execute(
        """UPDATE groundloop_m5_semantic_job SET job_state=%s,
             result_artifact_id=%s,result_artifact_hash=%s,archive_reason=%s,
             completion_digest=%s,completed_revision=%s,completed_at=clock_timestamp()
           WHERE epoch_id=%s AND logical_job_id=%s AND job_state='running'
             AND result_artifact_id IS NULL AND result_artifact_hash IS NULL
             AND completion_digest IS NULL AND completed_revision IS NULL""",
        (
            terminal_state.value,
            verifier_artifact.artifact_id,
            verifier_artifact.artifact_hash,
            None if archive_reason is None else archive_reason.value,
            completion.completion_digest,
            owner.revision + 1,
            owner.epoch_id,
            job.logical_job_id,
        ),
    ).rowcount
    if completed != 1:
        raise EventConflictError("D34 job terminalization lost its exact CAS")
    _inject(failure_injector, "verifier_job_completed")
    _d34_settle_pending(cursor, owner, pending)
    _inject(failure_injector, "verifier_pending_updated")
    _d34_finish_ordered_accounting(
        cursor, owner, accounting, before, work, total, result, failure_injector
    )
    if prepared is not None and intent is not None:
        _d34_matching._finalize_prepared_matching_transition(cursor, intent, prepared)
        _inject(failure_injector, "d34_matching_finalized")
        for (
            kind,
            _,
            delta,
        ) in prepared.prewrite_patch_artifact.logical_patch.output_records:
            if kind != "status_delta":
                continue
            if (
                type(delta) is not StatusDelta
                or delta.event_id != owner.header.structural_event_id
            ):
                raise EventConflictError("D34 native status output changed its event")
            cursor.execute(
                "INSERT INTO groundloop_status_delta "
                "(event_id,epoch_id,revision,object_type,object_id,"
                "old_status,new_status,reason) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    delta.event_id,
                    owner.epoch_id,
                    owner.revision + 1,
                    delta.object_type,
                    delta.object_id,
                    delta.old_status,
                    delta.new_status,
                    delta.reason,
                ),
            )
        _inject(failure_injector, "d34_status_deltas_inserted")
    _force_deferred_validation(cursor)
    _inject(failure_injector, "verifier_constraints_validated")
    receipt = M5RequirementAttemptReturnReceipt(
        disposition=M5RequirementReturnDisposition.APPLIED,
        logical_job_id=job.logical_job_id,
        attempt_id=attempt.attempt_id,
        resulting_revision=owner.revision + 1,
        exact_replay=False,
        execution_evidence_digest=accounting.evidence.evidence_digest,
        return_artifact_digest=result.attempt_result_artifact_hash,
        current_terminal_logical_result_hash=None,
        transition_anchor=accounting.anchor,
    )
    receipt.validate_anchor_context(
        epoch_id=owner.epoch_id,
        expected_kind=M5RuntimeWorkContributionKind.VERIFIER_COMPLETION,
    )
    return receipt


def _d34_complete_m5_verifier_atomically(
    connection: Connection[Any],
    epoch_id: int,
    expected_revision: int,
    lease: M5JobLease,
    job: M5LogicalJobSpec,
    pair_input: M5RequirementPairInput,
    verifier_artifact: M5RequirementVerifierArtifact,
    attempt_output: M5AttemptOutput,
    execution_disposition: M5ExecutionEvidenceDisposition,
    attempt_work: M5RuntimeWork,
    attempt_timing: M5RuntimeTiming | None,
    *,
    failure_injector: RuntimeVerifierFailureInjector | None = None,
) -> M5RequirementAttemptReturnReceipt:
    """Private C2 boundary owning one top-level transaction, never a savepoint."""

    _d34_require_idle(connection)
    with connection.transaction():
        with connection.cursor() as cursor:
            schema = _d34_schema_authority(cursor)
            header = _lock_epoch(cursor, epoch_id)
            owner = _d34_issue_owner(cursor, schema, header)
            return _d34_complete_verifier_cursor(
                cursor,
                epoch_id,
                expected_revision,
                lease,
                job,
                pair_input,
                verifier_artifact,
                attempt_output,
                execution_disposition,
                attempt_work,
                attempt_timing,
                owner=owner,
                failure_injector=failure_injector,
            )


def _d34_complete_verifier_cursor(
    cursor: Cursor[Any],
    epoch_id: int,
    expected_revision: int,
    lease: M5JobLease,
    job: M5LogicalJobSpec,
    pair_input: M5RequirementPairInput,
    verifier_artifact: M5RequirementVerifierArtifact,
    attempt_output: M5AttemptOutput,
    execution_disposition: M5ExecutionEvidenceDisposition,
    attempt_work: M5RuntimeWork,
    attempt_timing: M5RuntimeTiming | None,
    *,
    owner: _d34_CompletionOwner | None,
    failure_injector: RuntimeVerifierFailureInjector | None = None,
) -> M5RequirementAttemptReturnReceipt:
    """Shared checked return path; only an exact internal owner enables C2."""

    if (
        isinstance(expected_revision, bool)
        or not isinstance(expected_revision, int)
        or expected_revision < 1
    ):
        raise InvalidEventError("expected runtime revision must be positive")
    _validate_success_disposition(execution_disposition)
    require_runtime_recovery_bundle(cursor)
    leased_attempt = _validate_executable_lease(
        lease=lease, job=job, expected_revision=expected_revision
    )
    if (
        job.job_kind is not M5JobKind.VERIFY_REQUIREMENT_PAIR
        or job.parent_job_id is None
        or job.pair is None
        or job.expandable
    ):
        raise ValidationError("verifier completion accepts only pair child jobs")
    if attempt_output.attempt_id != leased_attempt.attempt_id:
        raise ValidationError("attempt output belongs to another verifier lease")

    if owner is None:
        header = _lock_epoch(cursor, epoch_id)
    else:
        _d34_validate_owner(cursor, owner)
        header = owner.header
        if header.epoch_id != epoch_id:
            raise EventConflictError("D34 owner names another epoch")
    if expected_revision > header.revision:
        raise EventConflictError("expected revision is newer than durable runtime")
    activity_hint = None
    fresh_active = False
    fresh_return = False
    if owner is not None:
        # The after-wait held header, never the caller's stale hint, is the CAS.
        expected_revision = header.revision
        probe = _d34_probe_retained_return(
            cursor,
            owner=owner,
            lease=lease,
            job=job,
            pair_input=pair_input,
            artifact=verifier_artifact,
            output=attempt_output,
            disposition=execution_disposition,
            attempt_work=attempt_work,
            attempt_timing=attempt_timing,
        )
        if probe.matching_intent is not None:
            assert probe.receipt is not None
            _inject(failure_injector, "d34_retained_classified")
            _d34_matching._authorize_checked_prefix(
                cursor, epoch_id=epoch_id, expected_revision=header.revision
            )
            _d34_matching._d34_read_matching_replay_with_held_prefix(
                cursor,
                probe.matching_intent,
                owner,
                failure_injector=failure_injector,
            )
            return probe.receipt
        if probe.receipt is None:
            activity_hint = _d34_read_activity(cursor, header, pair_input)
            fresh_return = (
                activity_hint[0]
                and probe.job.state is M5JobState.RUNNING
                and probe.attempt.state == "dispatched"
                and probe.attempt.attempt_output_digest is None
            )
            fresh_active = fresh_return and all(activity_hint)
            if fresh_active:
                _d34_matching._d35_transition_reader_route(
                    cursor, schema_name=owner.schema, require_022=True
                )
                _d34_matching._authorize_matching_transition(
                    cursor,
                    epoch_id=epoch_id,
                    expected_runtime_revision=header.revision,
                    resulting_revision=header.revision + 1,
                    source_kind=M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION,
                    source_id=attempt_output.attempt_id,
                )
            else:
                _d34_matching._authorize_checked_prefix(
                    cursor, epoch_id=epoch_id, expected_revision=header.revision
                )
    manifest = _load_manifest(cursor, header.candidate_policy_id)
    _d34_lock_document_parents(cursor, pair_input)
    activity = _lock_verifier_activity(cursor, header=header, pair_input=pair_input)
    if activity_hint is not None and activity != activity_hint:
        raise EventConflictError(
            "D34 activity changed while acquiring its native locks"
        )
    _lock_snapshot_headers(cursor, header=header)
    scope = _read_scope(
        cursor,
        epoch_id=epoch_id,
        root_job_id=job.parent_job_id,
        for_update=True,
    )
    parent_job, stored_job = _lock_verifier_jobs(
        cursor,
        epoch_id=epoch_id,
        parent_job_id=job.parent_job_id,
        verifier_job_id=job.logical_job_id,
    )
    _validate_verifier_bindings(
        cursor,
        header=header,
        scope=scope,
        parent_job=parent_job,
        stored_job=stored_job,
        supplied_job=job,
        pair_input=pair_input,
        verifier_artifact=verifier_artifact,
        attempt_output=attempt_output,
        manifest=manifest,
    )
    attempt = read_requirement_attempt(
        cursor,
        logical_job_id=job.logical_job_id,
        attempt_id=leased_attempt.attempt_id,
    )
    if attempt is None or not _same_attempt_identity(attempt.attempt, leased_attempt):
        raise EventConflictError("verifier return lease identity is not durable")
    dispatch = load_requirement_dispatch(
        cursor, epoch_id=epoch_id, attempt=attempt.attempt
    )
    if (
        dispatch.record_digest != lease.dispatch_record_digest
        or dispatch.dispatched_revision != lease.resulting_revision
    ):
        raise EventConflictError("verifier lease dispatch identity is not durable")

    terminal_logical_result_hash = _current_terminal_logical_result_hash(
        cursor, header=header
    )
    standard_accounting = build_requirement_execution_accounting(
        epoch_id=epoch_id,
        expected_revision=expected_revision,
        attempt=leased_attempt,
        dispatch=dispatch,
        disposition=execution_disposition,
        result_or_error_hash=attempt_output.attempt_output_digest,
        attempt_work=attempt_work,
        attempt_timing=attempt_timing,
        anchor_kind=M5RuntimeWorkContributionKind.VERIFIER_COMPLETION,
    )
    late_accounting: RequirementExecutionAccounting | None = None
    if owner is None or not fresh_return:
        late_accounting = build_requirement_execution_accounting(
            epoch_id=epoch_id,
            expected_revision=expected_revision,
            attempt=leased_attempt,
            dispatch=dispatch,
            disposition=execution_disposition,
            result_or_error_hash=attempt_output.attempt_output_digest,
            attempt_work=attempt_work,
            attempt_timing=attempt_timing,
            anchor_kind=M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
            anchor_revision=header.revision,
        )
        replay_revision = read_requirement_execution_replay(
            cursor,
            expected_evidence=standard_accounting.evidence,
            expected_observation=standard_accounting.observation,
        )
        postterminal_replay = (
            None
            if replay_revision is not None
            else read_requirement_postterminal_replay(
                cursor,
                expected_evidence=standard_accounting.evidence,
                expected_observation=standard_accounting.observation,
            )
        )

        # Immutable replay lookup precedes latest-attempt enforcement.  A late
        # return does not install semantic verifier rows; its output digest binds
        # the supplied, self-validating artifact bytes.
        late_replay = _validate_late_replay(
            cursor,
            header=header,
            lease=lease,
            supplied_job=job,
            supplied_output=attempt_output,
            job=stored_job,
            attempt=attempt,
            accounting=late_accounting,
            event_replay_revision=replay_revision,
            postterminal_replay=postterminal_replay,
        )
        if late_replay is not None:
            return late_replay
        applied_replay = _validate_applied_replay(
            cursor,
            header=header,
            lease=lease,
            supplied_job=job,
            pair_input=pair_input,
            verifier_artifact=verifier_artifact,
            attempt_output=attempt_output,
            stored_job=stored_job,
            attempt=attempt,
            accounting=standard_accounting,
            replay_revision=replay_revision,
        )
        if applied_replay is not None:
            return applied_replay

    latest = read_latest_requirement_attempt(cursor, logical_job_id=job.logical_job_id)
    if latest is None:
        raise ValidationError("verifier job lost its durable attempt history")
    late_plan = _build_late_return_plan(
        header=header,
        job=stored_job,
        attempt=attempt,
        output=attempt_output,
        activity=activity,
        terminal_logical_result_hash=terminal_logical_result_hash,
    )
    if late_plan is not None:
        if late_accounting is None:
            raise EventConflictError("D34 fresh active source changed to late return")
        postterminal = late_plan.terminal_logical_result_hash is not None
        if attempt.state == "expired":
            successor = cursor.execute(
                """
                SELECT 1
                FROM groundloop_m5_job_attempt
                WHERE logical_job_id = %s AND attempt_ordinal = %s
                """,
                (job.logical_job_id, attempt.attempt.attempt_ordinal + 1),
            ).fetchone()
            if (
                successor is None
                or latest.attempt.attempt_ordinal <= attempt.attempt.attempt_ordinal
            ):
                raise ValidationError("expired verifier return lacks dense successor")
        elif not _same_attempt_identity(latest.attempt, leased_attempt):
            raise EventConflictError(
                "still-current verifier audit has a later durable attempt"
            )
        if postterminal:
            if not stored_job.state.terminal:
                raise ValidationError("postterminal verifier return lacks terminal job")
        else:
            if header.revision != expected_revision:
                raise EventConflictError("stale preterminal verifier return revision")
            _require_pending_epoch(header)
            if attempt.state == "expired":
                if stored_job.state is not M5JobState.RUNNING:
                    raise EventConflictError(
                        "preterminal expired verifier lacks running successor job"
                    )
            elif stored_job.state is not M5JobState.CANCELLED:
                raise EventConflictError(
                    "preterminal verifier audit lacks cancelled closure"
                )
        return _persist_late_return(
            cursor,
            header=header,
            plan=late_plan,
            attempt=attempt,
            output=attempt_output,
            accounting=late_accounting,
            failure_injector=failure_injector,
        )

    if header.revision != expected_revision:
        raise EventConflictError("stale typed runtime revision")
    _require_pending_epoch(header)
    if not _same_attempt_identity(latest.attempt, leased_attempt):
        raise EventConflictError("verifier lease is no longer the latest attempt")
    if attempt.state != "dispatched" or attempt.attempt_output_digest is not None:
        raise EventConflictError("verifier attempt is not an unreserved dispatch")
    if stored_job.state is not M5JobState.RUNNING:
        raise EventConflictError("verifier job is not running")

    active = all(activity)
    if active:
        # This must remain before authorization, accounting inserts, output
        # reservation, or any semantic row write.  Replays were handled above.
        if owner is None:
            raise ValidationError(
                "active verifier completion requires M5-D25 persisted matching"
            )
        if not fresh_active:
            raise EventConflictError("D34 active return lacks its fresh classification")
        return _d34_apply_current_completion(
            cursor,
            owner=owner,
            lease=lease,
            job=job,
            pair_input=pair_input,
            verifier_artifact=verifier_artifact,
            attempt_output=attempt_output,
            accounting=standard_accounting,
            failure_injector=failure_injector,
        )

    if owner is not None:
        if not fresh_return:
            raise EventConflictError(
                "D34 inactive return lacks its fresh classification"
            )
        return _d34_apply_current_completion(
            cursor,
            owner=owner,
            lease=lease,
            job=job,
            pair_input=pair_input,
            verifier_artifact=verifier_artifact,
            attempt_output=attempt_output,
            accounting=standard_accounting,
            failure_injector=failure_injector,
            activity=activity,
        )

    archive_reason = (
        M5AttemptArchiveReason.EPOCH_FAILED
        if not activity[0]
        else (
            M5AttemptArchiveReason.SUBJECT_INACTIVE
            if not activity[2] or not activity[3]
            else M5AttemptArchiveReason.CHUNK_INACTIVE
        )
    )
    terminal_reason = M5TerminalReason(archive_reason.value)
    attempt_result = M5AttemptResultArtifact.build(
        attempt_output=attempt_output,
        job_state_at_receipt=M5JobState.RUNNING,
        job_state_after=M5JobState.COMPLETED_INACTIVE,
        disposition=M5AttemptDisposition.VERIFIER_COMPLETED_INACTIVE,
        activity_snapshot_epoch_id=epoch_id,
        activity_snapshot_revision=header.revision,
        epoch_active=activity[0],
        chunk_active=activity[1],
        requirement_active=activity[2],
        group_active=activity[3],
        archive_reason=archive_reason,
    )
    attempt_result.validate_job_shape(job.job_kind)
    completion = M5JobCompletion.build(
        job=job,
        terminal_state=M5JobState.COMPLETED_INACTIVE,
        result_artifact_id=verifier_artifact.artifact_id,
        result_artifact_hash=verifier_artifact.artifact_hash,
        archive_reason=terminal_reason,
    )
    resulting_revision = header.revision + 1

    if owner is None:
        _authorize(cursor, header)

    reserved = cursor.execute(
        """
        UPDATE groundloop_m5_job_attempt
        SET attempt_state = 'result_reserved', attempt_output_digest = %s
        WHERE attempt_id = %s AND logical_job_id = %s
          AND attempt_state = 'dispatched' AND attempt_output_digest IS NULL
          AND lease_token_hash = %s AND lease_expires_at = %s
          AND execution_spec_hash = %s
        """,
        (
            attempt_output.attempt_output_digest,
            attempt_output.attempt_id,
            job.logical_job_id,
            leased_attempt.lease_token_hash,
            leased_attempt.lease_expires_at,
            leased_attempt.execution_spec_hash,
        ),
    ).rowcount
    if reserved != 1:
        raise EventConflictError("verifier output reservation lost its race")
    _inject(failure_injector, "verifier_output_reserved")

    _d34_lock_provenance_parents(cursor, pair_input, verifier_artifact)
    archive_coordinates = _persist_verifier_closure(
        cursor,
        epoch_id=epoch_id,
        job=job,
        attempt_id=attempt_output.attempt_id,
        pair_input=pair_input,
        artifact=verifier_artifact,
        attempt_result=attempt_result,
        attempt_output=attempt_output,
        eligible_for_currency=False,
        failure_injector=failure_injector,
    )
    accounting_start = start_event_accounting(
        cursor, epoch_id=epoch_id, expected_revision=expected_revision
    )
    persist_requirement_execution_accounting(cursor, accounting=standard_accounting)
    _inject(failure_injector, "verifier_accounting_inserted")

    completed_attempt = cursor.execute(
        """
        UPDATE groundloop_m5_job_attempt
        SET attempt_state = 'completed', finished_at = clock_timestamp(),
            attempt_work_digest = %s
        WHERE attempt_id = %s AND logical_job_id = %s
          AND attempt_state = 'result_reserved'
          AND attempt_output_digest = %s
          AND lease_token_hash = %s AND lease_expires_at = %s
          AND execution_spec_hash = %s
        """,
        (
            standard_accounting.evidence.attempt_work.work_digest,
            attempt_output.attempt_id,
            job.logical_job_id,
            attempt_output.attempt_output_digest,
            leased_attempt.lease_token_hash,
            leased_attempt.lease_expires_at,
            leased_attempt.execution_spec_hash,
        ),
    ).rowcount
    if completed_attempt != 1:
        raise EventConflictError("verifier attempt completion lost its reservation")
    _inject(failure_injector, "verifier_attempt_completed")

    completed_job = cursor.execute(
        """
        UPDATE groundloop_m5_semantic_job
        SET job_state = 'completed_inactive', result_artifact_id = %s,
            result_artifact_hash = %s, archive_reason = %s,
            completion_digest = %s, completed_revision = %s,
            completed_at = clock_timestamp()
        WHERE epoch_id = %s AND logical_job_id = %s AND job_state = 'running'
          AND result_artifact_id IS NULL AND result_artifact_hash IS NULL
          AND completion_digest IS NULL AND completed_revision IS NULL
        """,
        (
            verifier_artifact.artifact_id,
            verifier_artifact.artifact_hash,
            terminal_reason.value,
            completion.completion_digest,
            resulting_revision,
            epoch_id,
            job.logical_job_id,
        ),
    ).rowcount
    if completed_job != 1:
        raise EventConflictError("verifier job completion lost its race")
    _inject(failure_injector, "verifier_job_completed")

    _settle_verifier_pending(
        cursor,
        epoch_id=epoch_id,
        owner_claim_id=pair_input.owner_claim_id,
        expected_revision=expected_revision,
        resulting_revision=resulting_revision,
    )
    _inject(failure_injector, "verifier_pending_updated")

    verifier_work = _derive_verifier_completion_work(
        cursor,
        epoch_id=epoch_id,
        logical_job_id=job.logical_job_id,
        attempt_id=attempt_output.attempt_id,
        active=False,
        archive_coordinates=archive_coordinates,
    )
    _insert_verifier_completion_contribution(
        cursor,
        epoch_id=epoch_id,
        attempt_id=attempt_output.attempt_id,
        artifact_hash=attempt_result.attempt_result_artifact_hash,
        resulting_revision=resulting_revision,
        work=verifier_work,
    )
    _inject(failure_injector, "verifier_contribution_inserted")

    _advance_revision(
        cursor,
        header=header,
        resulting_revision=resulting_revision,
        pending_revision_already_updated=True,
    )
    _inject(failure_injector, "verifier_epoch_revision_advanced")
    finish_requirement_execution_accounting(
        cursor,
        epoch_id=epoch_id,
        expected_revision=expected_revision,
        start=accounting_start,
        accounting=standard_accounting,
        additional_work=verifier_work,
        failure_injector=failure_injector,
    )
    _inject(failure_injector, "verifier_revision_advanced")
    _force_deferred_validation(cursor)
    _inject(failure_injector, "verifier_constraints_validated")
    receipt = M5RequirementAttemptReturnReceipt(
        disposition=M5RequirementReturnDisposition.APPLIED,
        logical_job_id=job.logical_job_id,
        attempt_id=attempt_output.attempt_id,
        resulting_revision=resulting_revision,
        exact_replay=False,
        execution_evidence_digest=standard_accounting.evidence.evidence_digest,
        return_artifact_digest=attempt_result.attempt_result_artifact_hash,
        current_terminal_logical_result_hash=None,
        transition_anchor=standard_accounting.anchor,
    )
    receipt.validate_anchor_context(
        epoch_id=epoch_id,
        expected_kind=M5RuntimeWorkContributionKind.VERIFIER_COMPLETION,
    )
    return receipt


__all__ = ["RuntimeVerifierFailureInjector", "complete_m5_verifier"]
