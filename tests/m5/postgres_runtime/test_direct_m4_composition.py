"""Live composition tests for the cursor-local M4-v1 typed bridge."""

from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import psycopg
import pytest
from psycopg import Connection, Cursor, sql

from groundloop.domain import (
    ChunkVersion,
    DocumentVersion,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
)
from groundloop.errors import EventConflictError, ValidationError
from groundloop.m4.application import (
    DiscoveryResult,
    DynamicEventPlan,
    JobLease,
    OpenEventReceipt,
    StructuralWithdrawal,
)
from groundloop.m4.contracts import (
    AdmissionChannel,
    AdmittedPair,
    CandidatePolicyManifest,
    ChannelHit,
    ChildClosure,
    CorpusUpdateIdentity,
    DiscoveryScope,
    JobAttempt,
    JobCompletion,
    JobKind,
    JobState,
    LogicalJobSpec,
    PairKey,
    UpdateKind,
    VectorIndexKind,
    stable_m4_digest,
)
from groundloop.m4.evaluation_overlay import EvaluationLifecycle
from groundloop.m4.pipeline import (
    InsertedDocument,
    M4ExecutionMode,
    PostgresM4ApplicationPorts,
    StructuralPayload,
    bootstrap_m4_publication,
)
from groundloop.m5.runtime import digests as runtime_digests
from groundloop.m5.runtime import postgres_direct_recovery as direct_recovery_module
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5AcquisitionDisposition,
    M5CandidatePolicyManifest,
    M5ExecutionEvidenceDisposition,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeOperationalConfig,
    M5RuntimeTiming,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TransitionTimingAnchor,
    M5TypedDirectJobLease,
    M5TypedDirectLateReturnEnvelope,
    M5TypedDirectReturnKind,
    M5TypedDirectScopeKind,
    RequirementRegistrySnapshot,
)
from groundloop.m5.runtime.direct_m4 import PostgresM5DirectM4Adapter
from groundloop.m5.runtime.persistence import (
    PostgresM5RuntimeStore,
    _persist_active_chunk_snapshot,
    _persist_requirement_snapshot,
)
from groundloop.postgres.migrations import (
    apply_legacy_migrations,
    install_m5_core_bundle,
    install_m5_runtime_bundle,
    install_m5_runtime_recovery_bundle,
)
from tests.m5.postgres.helpers import (
    SeededBase,
    install_test_activation_barrier,
    seed_base,
)
from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


IMPACT_EXECUTION_HASH = _sha("direct-m4-impact-execution")
REGISTRY_ID = "direct-m4-claim-registry"
NEW_DOCUMENT_ID = "direct-m4-inserted-document"
NEW_DOCUMENT_VERSION_ID = "direct-m4-inserted-document-v1"
NEW_CHUNK_ID = "direct-m4-chunk-v1"
NEW_CHUNK_TEXT = "Nimbus is an atmospheric probe."


@dataclass(frozen=True, slots=True)
class DirectRuntimeDatabase:
    connection: Connection[Any]
    base: SeededBase
    manifest: M5CandidatePolicyManifest
    ports: PostgresM4ApplicationPorts


def _database_url() -> str:
    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip(
            "live PostgreSQL test: set GROUNDLOOP_TEST_DATABASE_URL or "
            "GROUNDLOOP_DATABASE_URL"
        )
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


def _m5_manifest(base: SeededBase) -> M5CandidatePolicyManifest:
    return M5CandidatePolicyManifest.build(
        embedding_model_artifact_id="direct-m4-embedding",
        requirement_role_template_hash=_sha("direct-m4-requirement-role"),
        chunk_role_template_hash=_sha("direct-m4-chunk-role"),
        vector_method_version="direct-m4-vector-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=_sha("direct-m4-vector-build"),
        vector_search_config_hash=_sha("direct-m4-vector-search"),
        lexical_method_version="direct-m4-lexical-v1",
        lexical_config_hash=_sha("direct-m4-lexical-config"),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        fusion_version="rank-interleave-v1",
        reverse_budget_per_inserted_chunk=2,
        forward_budget_per_requirement=2,
        verifier_execution_spec_hash=_sha("direct-m4-verifier-execution"),
        decision_policy_version=base.policy_version,
        lineage_safety_override=True,
    )


def _m4_manifest(
    base: SeededBase, typed: M5CandidatePolicyManifest
) -> CandidatePolicyManifest:
    return CandidatePolicyManifest.build(
        policy_id=typed.candidate_policy_id,
        embedding_model_artifact_id=typed.embedding_model_artifact_id,
        claim_role_template_hash=typed.requirement_role_template_hash,
        chunk_role_template_hash=typed.chunk_role_template_hash,
        vector_method_version=typed.vector_method_version,
        vector_index_kind=typed.vector_index_kind,
        vector_index_build_config_hash=typed.vector_index_build_config_hash,
        vector_search_config_hash=typed.vector_search_config_hash,
        lexical_method_version=typed.lexical_method_version,
        lexical_config_hash=typed.lexical_config_hash,
        lexical_postgres_version=typed.lexical_postgres_version,
        lexical_regconfig_identity=typed.lexical_regconfig_identity,
        claim_registry_snapshot_id=REGISTRY_ID,
        claim_count=len(base.claim_ids),
        fusion_version=typed.fusion_version,
        approximate_cap_per_inserted_chunk=typed.reverse_budget_per_inserted_chunk,
        frontier_depth=typed.forward_budget_per_requirement,
        verifier_execution_spec_hash=typed.verifier_execution_spec_hash,
        decision_policy_version=typed.decision_policy_version,
        lineage_safety_override=typed.lineage_safety_override,
    )


@pytest.fixture
def direct_runtime_db() -> Iterator[DirectRuntimeDatabase]:
    dsn = _database_url()
    schema_name = f"groundloop_m5_direct_m4_{uuid.uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))

    try:
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(schema_name)
                )
            )
            with connection.transaction():
                apply_legacy_migrations(connection)
            install_m5_core_bundle(connection)
            install_m5_runtime_bundle(connection)
            install_m5_runtime_recovery_bundle(connection)

            with connection.transaction():
                base = seed_base(
                    connection,
                    prefix="direct-m4",
                    claim_count=1,
                    chunk_texts=("Nimbus was the prior evidence.",),
                )
                connection.execute(
                    """
                    INSERT INTO groundloop_model_artifact (
                        model_artifact_id, task, provider, model_id,
                        immutable_revision, tokenizer_revision, license_id,
                        config_hash
                    ) VALUES (
                        'direct-m4-embedding', 'embedding', 'fixture',
                        'direct-m4-embedding', 'v1', 'v1', 'MIT', %s
                    )
                    """,
                    (_sha("direct-m4-embedding-config"),),
                )

            bootstrap_m4_publication(connection, sealed_epoch_id=base.epoch_id)
            manifest = _m5_manifest(base)
            ports = PostgresM4ApplicationPorts(
                connection,
                structural_payloads={},
                execution_mode=M4ExecutionMode.MEASURED,
            )
            ports.runtime_store.register_candidate_policy(_m4_manifest(base, manifest))
            ports.register_claim_registry_snapshot(REGISTRY_ID, base.claim_ids)
            PostgresM5RuntimeStore(connection).register_candidate_policy(manifest)
            with connection.transaction():
                install_test_activation_barrier(
                    connection,
                    base,
                    activation_id="direct-m4-activation",
                )

            yield DirectRuntimeDatabase(connection, base, manifest, ports)
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )


def _event_and_payload(
    database: DirectRuntimeDatabase,
) -> tuple[DynamicEventPlan, StructuralPayload]:
    inserted = InsertedDocument(
        version=DocumentVersion(
            NEW_DOCUMENT_VERSION_ID,
            NEW_DOCUMENT_ID,
            _sha("direct-m4-new-content"),
        ),
        chunks=(
            ChunkVersion(
                NEW_CHUNK_ID,
                NEW_DOCUMENT_VERSION_ID,
                0,
                NEW_CHUNK_TEXT,
            ),
        ),
        source_uri="fixture://direct-m4-document",
    )
    event_id = "direct-m4-document-insert"
    update = CorpusUpdateIdentity(
        event_id=event_id,
        payload_hash=_sha("direct-m4-document-payload"),
        update_kind=UpdateKind.INSERT,
        previous_published_epoch_id=database.base.epoch_id,
        candidate_policy_id=database.manifest.candidate_policy_id,
    )
    return (
        DynamicEventPlan(
            update=update,
            inserted_chunk_version_ids=(NEW_CHUNK_ID,),
            deactivated_chunk_version_ids=(),
            registered_claim_ids=(),
            claim_registry_snapshot_id=REGISTRY_ID,
        ),
        StructuralPayload(inserted=inserted),
    )


def _job(
    event: DynamicEventPlan,
    *,
    kind: JobKind,
    execution_hash: str,
    parent_job_id: str | None = None,
    claim_id: str = "",
    chunk_id: str = "",
) -> LogicalJobSpec:
    job_id = LogicalJobSpec.derive_job_id(
        event_id=event.update.event_id,
        kind=kind,
        candidate_policy_id=event.update.candidate_policy_id,
        execution_spec_hash=execution_hash,
        parent_job_id=parent_job_id or "",
        claim_id=claim_id,
        chunk_version_id=chunk_id,
    )
    return LogicalJobSpec(
        job_id=job_id,
        event_id=event.update.event_id,
        kind=kind,
        candidate_policy_id=event.update.candidate_policy_id,
        payload_hash=stable_m4_digest(
            "m4-application-job-payload-v1",
            event.update.payload_hash,
            kind.value,
            parent_job_id or "",
            claim_id,
            chunk_id,
        ),
        execution_spec_hash=execution_hash,
        parent_job_id=parent_job_id,
        pair=(PairKey(claim_id, chunk_id) if kind is JobKind.VERIFY_PAIR else None),
        target_claim_id=(claim_id if kind is JobKind.FRONTIER_RETRIEVE else None),
        target_chunk_version_id=(
            chunk_id if kind is JobKind.IMPACT_DISCOVERY else None
        ),
        expandable=kind is not JobKind.VERIFY_PAIR,
    )


def _active_chunk_snapshot(
    cursor: Cursor[Any], database: DirectRuntimeDatabase
) -> ActiveChunkSnapshot:
    base_entries = tuple(
        ActiveChunkSnapshotEntry.build(
            chunk_version_id=str(row[0]), chunk_text=str(row[1])
        )
        for row in cursor.execute(
            """
            SELECT chunk_version_id, text
            FROM groundloop_chunk_version
            WHERE valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            ORDER BY chunk_version_id
            """,
            (database.base.epoch_id, database.base.epoch_id),
        ).fetchall()
    )
    return ActiveChunkSnapshot.build(
        base_entries
        + (
            ActiveChunkSnapshotEntry.build(
                chunk_version_id=NEW_CHUNK_ID,
                chunk_text=NEW_CHUNK_TEXT,
            ),
        )
    )


def _insert_typed_outer_declaration(
    cursor: Cursor[Any],
    database: DirectRuntimeDatabase,
    event: DynamicEventPlan,
) -> tuple[int, RequirementRegistrySnapshot, ActiveChunkSnapshot]:
    mode = cursor.execute(
        "SELECT mode FROM groundloop_runtime_mode WHERE singleton FOR UPDATE"
    ).fetchone()
    m4_head = cursor.execute(
        "SELECT epoch_id FROM groundloop_m4_publication_head WHERE singleton FOR UPDATE"
    ).fetchone()
    m5_head = cursor.execute(
        "SELECT epoch_id FROM groundloop_m5_publication_head WHERE singleton FOR UPDATE"
    ).fetchone()
    assert mode == ("m5_active",)
    assert m4_head == m5_head == (database.base.epoch_id,)

    row = cursor.execute(
        """
        INSERT INTO groundloop_epoch (
            event_id, payload_hash, revision, structural_status,
            semantic_status, evaluation_state, publication_mode, sealed_at
        ) VALUES (
            %s, %s, 1, 'committed', 'pending', 'pending',
            'provisional', NULL
        ) RETURNING epoch_id
        """,
        (event.update.event_id, event.update.payload_hash),
    ).fetchone()
    assert row is not None
    epoch_id = int(row[0])
    cursor.execute(
        """
        INSERT INTO groundloop_m5_update (
            epoch_id, update_kind, previous_published_epoch_id,
            decision_policy_version, manifest
        ) VALUES (%s, 'document_insert', %s, %s, '{}'::jsonb)
        """,
        (
            epoch_id,
            database.base.epoch_id,
            database.base.policy_version,
        ),
    )

    requirements = RequirementRegistrySnapshot.build(())
    active_chunks = _active_chunk_snapshot(cursor, database)
    cursor.execute(
        """
        INSERT INTO groundloop_m5_runtime_epoch (
            epoch_id, structural_event_id, candidate_policy_id,
            candidate_policy_manifest_hash,
            requirement_registry_snapshot_digest,
            active_chunk_snapshot_digest,
            expected_previous_published_epoch_id,
            requirement_root_set_hash, runtime_state, revision,
            open_work_count, open_scope_count,
            blocking_failure_count, terminal_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            'structural_committed', 1, 0, 0, 0, NULL
        )
        """,
        (
            epoch_id,
            event.update.event_id,
            database.manifest.candidate_policy_id,
            database.manifest.manifest_hash,
            requirements.requirement_registry_snapshot_digest,
            active_chunks.active_chunk_snapshot_digest,
            database.base.epoch_id,
            runtime_digests.requirement_root_set_digest(()),
        ),
    )
    return epoch_id, requirements, active_chunks


def _persist_typed_outer_snapshots(
    cursor: Cursor[Any],
    epoch_id: int,
    requirements: RequirementRegistrySnapshot,
    active_chunks: ActiveChunkSnapshot,
    *,
    lease_duration_ms: int = 60_000,
) -> None:
    _persist_requirement_snapshot(
        cursor,
        snapshot=requirements,
        epoch_id=epoch_id,
    )
    _persist_active_chunk_snapshot(
        cursor,
        snapshot=active_chunks,
        epoch_id=epoch_id,
    )
    _persist_d24_direct_accounting(
        cursor, epoch_id, lease_duration_ms=lease_duration_ms
    )


def _length_frame(value: bytes) -> bytes:
    return len(value).to_bytes(8, byteorder="big", signed=False) + value


def _canonical_db_scalar(value: object) -> bytes:
    if value is None:
        return b"n"
    if isinstance(value, bool):
        return b"b1" if value else b"b0"
    if isinstance(value, int):
        return b"i" + str(value).encode("ascii")
    if isinstance(value, str):
        return b"s" + value.encode("utf-8")
    raise AssertionError(f"unsupported direct declaration scalar: {type(value)!r}")


def _direct_structural_open_work(
    cursor: Cursor[Any], *, epoch_id: int
) -> M5RuntimeWork:
    """Measure the exact immutable direct declaration with length framing."""

    queries = (
        (
            "m4_update",
            """
            SELECT update_kind, previous_published_epoch_id,
                   candidate_policy_id, registry_snapshot_id, manifest::text
            FROM groundloop_m4_update WHERE epoch_id = %s
            """,
        ),
        (
            "semantic_job",
            """
            SELECT btrim(job_id), parent_job_id, job_kind,
                   candidate_policy_id, btrim(payload_hash),
                   btrim(execution_spec_hash), claim_id, chunk_version_id,
                   expandable, created_revision
            FROM groundloop_semantic_job
            WHERE epoch_id = %s AND parent_job_id IS NULL
            ORDER BY job_id
            """,
        ),
        (
            "discovery_scope",
            """
            SELECT btrim(root_job_id), registry_snapshot_id, scope_kind,
                   explicit_claim_ids::text
            FROM groundloop_discovery_scope
            WHERE epoch_id = %s ORDER BY root_job_id
            """,
        ),
    )
    framed = [_length_frame(b"m5-d24-direct-declaration-v1")]
    for relation_name, query in queries:
        rows = cursor.execute(query, (epoch_id,)).fetchall()
        assert rows
        relation = [_length_frame(relation_name.encode("ascii"))]
        for row in rows:
            row_bytes = b"".join(
                _length_frame(_canonical_db_scalar(value)) for value in row
            )
            relation.append(_length_frame(row_bytes))
        framed.append(_length_frame(b"".join(relation)))
    canonical = b"".join(framed)
    assert canonical and hashlib.sha256(canonical).digest()
    return M5RuntimeWork(
        bytes_hashed=len(canonical),
        bytes_serialized=len(canonical),
    )


def _persist_d24_direct_accounting(
    cursor: Cursor[Any], epoch_id: int, *, lease_duration_ms: int
) -> None:
    """Install the revision-1 D24 point rows owned by structural open."""

    event_row = cursor.execute(
        "SELECT event_id, payload_hash FROM groundloop_epoch WHERE epoch_id = %s",
        (epoch_id,),
    ).fetchone()
    assert event_row is not None
    event_id = str(event_row[0])
    payload_hash = str(event_row[1]).strip()
    config = M5RuntimeOperationalConfig.build(lease_duration_ms)
    structural_work = _direct_structural_open_work(cursor, epoch_id=epoch_id)
    assert not structural_work.is_zero
    contribution_kind = M5RuntimeWorkContributionKind.STRUCTURAL_OPEN
    contribution_key = runtime_digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=contribution_kind,
        source_id=event_id,
    )
    cursor.execute(
        """
        INSERT INTO groundloop_m5_runtime_operational_config (
            epoch_id, lease_duration_ms, config_digest
        ) VALUES (%s, %s, %s)
        """,
        (epoch_id, config.lease_duration_ms, config.config_digest),
    )
    counter_columns = M5RuntimeWork.counter_names()
    cursor.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_contribution "
            "(epoch_id, {}, work_digest, contribution_kind, source_id, "
            "source_identity_hash, contribution_key_digest, applied_revision) "
            "VALUES (%s, {}, %s, %s, %s, %s, %s, 1)"
        ).format(
            sql.SQL(", ").join(sql.Identifier(name) for name in counter_columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in counter_columns),
        ),
        (
            epoch_id,
            *structural_work.counter_values(),
            structural_work.work_digest,
            contribution_kind.value,
            event_id,
            payload_hash,
            contribution_key,
        ),
    )
    cursor.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_accumulator "
            "(epoch_id, {}, work_digest, updated_revision, terminalized) "
            "VALUES (%s, {}, %s, 1, false)"
        ).format(
            sql.SQL(", ").join(sql.Identifier(name) for name in counter_columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in counter_columns),
        ),
        (
            epoch_id,
            *structural_work.counter_values(),
            structural_work.work_digest,
        ),
    )
    cursor.execute(
        """
        INSERT INTO groundloop_m5_runtime_timing_accumulator (
            epoch_id,
            required_expected_count,
            postgres_server_execution_expected_count,
            postgres_lock_wait_expected_count,
            postgres_wal_bytes_expected_count,
            postgres_shared_block_reads_expected_count,
            pending_contribution_kind, pending_source_id,
            pending_contribution_key_digest, pending_anchor_revision,
            updated_revision, terminalized
        ) VALUES (
            %s, 1, 1, 1, 1, 1, %s, %s, %s, 1, 1, false
        )
        """,
        (epoch_id, contribution_kind.value, event_id, contribution_key),
    )


def _authorize(cursor: Cursor[Any], epoch_id: int, revision: int) -> None:
    cursor.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
        (epoch_id, revision),
    )


def _force_accumulator_revision_for_test(
    cursor: Cursor[Any], *, table_name: str, epoch_id: int, revision: int
) -> None:
    if table_name not in {
        "groundloop_m5_runtime_work_accumulator",
        "groundloop_m5_runtime_timing_accumulator",
    }:
        raise AssertionError("test drift helper received an unexpected table")
    table = sql.Identifier(table_name)
    cursor.execute(sql.SQL("ALTER TABLE {} DISABLE TRIGGER USER").format(table))
    try:
        cursor.execute(
            sql.SQL("UPDATE {} SET updated_revision = %s WHERE epoch_id = %s").format(
                table
            ),
            (revision, epoch_id),
        )
    finally:
        cursor.execute(sql.SQL("ALTER TABLE {} ENABLE TRIGGER USER").format(table))


def _deterministic_lease_token(job: LogicalJobSpec, ordinal: int = 1) -> str:
    return stable_m4_digest("m4-lease-token-v1", job.job_id, str(ordinal))


def _m4_job_lease(lease: M5TypedDirectJobLease) -> JobLease:
    return JobLease(
        job_id=lease.job_id,
        should_execute=lease.should_execute,
        already_completed=lease.already_completed,
        attempt_id=lease.attempt_id,
        lease_token_hash=lease.lease_token_hash,
        expected_revision=lease.resulting_revision,
    )


def _typed_attempt(job: LogicalJobSpec, lease: M5TypedDirectJobLease) -> JobAttempt:
    assert lease.attempt_id is not None and lease.lease_token_hash is not None
    return JobAttempt(
        attempt_id=lease.attempt_id,
        job_id=job.job_id,
        execution_spec_hash=job.execution_spec_hash,
        attempt_ordinal=1,
        lease_token_hash=lease.lease_token_hash,
    )


def _discovery_return_envelope(
    *,
    epoch_id: int,
    job: LogicalJobSpec,
    lease: M5TypedDirectJobLease,
    discovery: DiscoveryResult,
    completion: JobCompletion,
    registered_claim_ids: tuple[str, ...],
) -> M5TypedDirectLateReturnEnvelope:
    return M5TypedDirectLateReturnEnvelope.build_discovery(
        epoch_id=epoch_id,
        job=job,
        attempt=_typed_attempt(job, lease),
        completion=completion,
        discovery=discovery,
        scope=DiscoveryScope(job.job_id, REGISTRY_ID, registered_claim_ids),
        persisted_scope_kind=M5TypedDirectScopeKind.ALL_REGISTERED_CLAIMS,
        explicit_claim_ids=None,
        closed_revision=None,
    )


def _verifier_return_envelope(
    *,
    epoch_id: int,
    job: LogicalJobSpec,
    lease: M5TypedDirectJobLease,
    completion: JobCompletion,
    observation: SemanticObservation,
    make_effective: bool,
) -> M5TypedDirectLateReturnEnvelope:
    return M5TypedDirectLateReturnEnvelope.build_verifier(
        epoch_id=epoch_id,
        job=job,
        attempt=_typed_attempt(job, lease),
        completion=completion,
        verification_execution=None,
        observation=observation,
        observation_produced_epoch=epoch_id,
        observation_raw_output_hash=completion.result_artifact_hash,
        observation_eligible_for_currency=True,
        requested_make_effective=make_effective,
    )


def _complete_expansion(
    epoch_id: int,
    event: DynamicEventPlan,
    root: LogicalJobSpec,
    claim_id: str,
) -> tuple[DiscoveryResult, JobCompletion, LogicalJobSpec]:
    pair = PairKey(claim_id, NEW_CHUNK_ID)
    child = _job(
        event,
        kind=JobKind.VERIFY_PAIR,
        execution_hash=_sha("direct-m4-verifier-execution"),
        parent_job_id=root.job_id,
        claim_id=pair.claim_id,
        chunk_id=pair.chunk_version_id,
    )
    admitted = AdmittedPair(
        epoch_id=epoch_id,
        pair=pair,
        candidate_policy_id=event.update.candidate_policy_id,
        fused_rank=1,
        reasons=(AdmissionChannel.VECTOR,),
        mandatory_lineage=False,
    )
    hit = ChannelHit(
        epoch_id=epoch_id,
        pair=pair,
        candidate_policy_id=event.update.candidate_policy_id,
        channel=AdmissionChannel.VECTOR,
        rank=1,
        score=0.91,
        channel_artifact_hash=_sha("direct-m4-channel-hit"),
    )
    discovery = DiscoveryResult(
        root_job_id=root.job_id,
        result_artifact_id="direct-m4-discovery-artifact",
        result_artifact_hash=_sha("direct-m4-discovery-result"),
        admitted_pairs=(admitted,),
        channel_hits=(hit,),
    )
    closure = ChildClosure.build(
        parent_job_id=root.job_id,
        result_artifact_hash=discovery.result_artifact_hash,
        child_job_ids=(child.job_id,),
    )
    completion = JobCompletion.build(
        job_id=root.job_id,
        payload_hash=root.payload_hash,
        execution_spec_hash=root.execution_spec_hash,
        result_artifact_id=discovery.result_artifact_id,
        result_artifact_hash=discovery.result_artifact_hash,
        terminal_state=JobState.COMPLETED_ACTIVE,
        child_closure=closure,
    )
    return discovery, completion, child


def _verifier_completion(
    child: LogicalJobSpec,
) -> tuple[JobCompletion, SemanticObservation]:
    result_hash = _sha("direct-m4-verification-result")
    completion = JobCompletion.build(
        job_id=child.job_id,
        payload_hash=child.payload_hash,
        execution_spec_hash=child.execution_spec_hash,
        result_artifact_id="direct-m4-verification-artifact",
        result_artifact_hash=result_hash,
        terminal_state=JobState.COMPLETED_ACTIVE,
    )
    observation = SemanticObservation(
        observation_id=_sha("direct-m4-observation"),
        subject_kind=SubjectKind.CLAIM,
        subject_id=child.pair.claim_id if child.pair is not None else "",
        chunk_version_id=(
            child.pair.chunk_version_id if child.pair is not None else ""
        ),
        task_type="claim-verification-v1",
        support_score=0.95,
        refute_score=0.025,
        neutral_score=0.025,
        producer=ModelStamp("direct-m4-verifier", "v1", "prompt-v1"),
        input_hash=_sha("direct-m4-verifier-input"),
    )
    return completion, observation


class _RollbackOuterTransaction(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _OpenedDirectEpoch:
    event: DynamicEventPlan
    payload: StructuralPayload
    withdrawal: StructuralWithdrawal
    root: LogicalJobSpec
    scope: DiscoveryScope
    epoch_id: int
    adapter: PostgresM5DirectM4Adapter
    open_receipt: OpenEventReceipt | None = None


def _open_direct_epoch(
    database: DirectRuntimeDatabase, *, lease_duration_ms: int = 60_000
) -> _OpenedDirectEpoch:
    event, payload = _event_and_payload(database)
    withdrawal = database.ports.plan_exact_withdrawal(event)
    root = _job(
        event,
        kind=JobKind.IMPACT_DISCOVERY,
        execution_hash=IMPACT_EXECUTION_HASH,
        chunk_id=NEW_CHUNK_ID,
    )
    scope = DiscoveryScope(root.job_id, REGISTRY_ID, ())
    adapter = PostgresM5DirectM4Adapter(database.ports)
    with database.connection.transaction(), database.connection.cursor() as cursor:
        epoch_id, requirements, active_chunks = _insert_typed_outer_declaration(
            cursor, database, event
        )
        opened = adapter.stage_direct_open(
            cursor,
            event,
            payload,
            withdrawal,
            (root,),
            (scope,),
        )
        assert opened.epoch_id == epoch_id
        assert not opened.replayed
        _persist_typed_outer_snapshots(
            cursor,
            epoch_id,
            requirements,
            active_chunks,
            lease_duration_ms=lease_duration_ms,
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    adapter._after_outer_commit()
    return _OpenedDirectEpoch(
        event=event,
        payload=payload,
        withdrawal=withdrawal,
        root=root,
        scope=scope,
        epoch_id=epoch_id,
        adapter=adapter,
        open_receipt=opened,
    )


def test_d24_direct_acquisition_is_db_clock_total(
    direct_runtime_db: DirectRuntimeDatabase,
) -> None:
    opened = _open_direct_epoch(direct_runtime_db)
    connection = direct_runtime_db.connection
    token = _deterministic_lease_token(opened.root)
    with connection.transaction(), connection.cursor() as cursor:
        lease = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            1,
            opened.root,
            token,
        )
        assert lease.disposition is M5AcquisitionDisposition.DISPATCH_NEW
        assert lease.should_execute
        assert lease.resulting_revision == 2
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

    with connection.transaction(), connection.cursor() as cursor:
        live = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            1,
            opened.root,
            token,
        )
        assert live.disposition is M5AcquisitionDisposition.LIVE_LEASE
        assert not live.should_execute
        assert live.exact_replay
        assert live.resulting_revision == 2
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

    assert connection.execute(
        """
        SELECT base.revision, runtime.revision,
               count(dispatch.*), count(contribution.*),
               work.updated_revision, timing.updated_revision,
               timing.pending_contribution_kind
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_dispatch_record AS dispatch
          ON dispatch.epoch_id = base.epoch_id AND dispatch.subgraph = 'direct'
        JOIN groundloop_m5_runtime_work_contribution AS contribution
          ON contribution.epoch_id = base.epoch_id
         AND contribution.contribution_kind = 'direct_acquisition'
        JOIN groundloop_m5_runtime_work_accumulator AS work
          ON work.epoch_id = base.epoch_id
        JOIN groundloop_m5_runtime_timing_accumulator AS timing
          ON timing.epoch_id = base.epoch_id
        WHERE base.epoch_id = %s
        GROUP BY base.revision, runtime.revision, work.updated_revision,
                 timing.updated_revision, timing.pending_contribution_kind
        """,
        (opened.epoch_id,),
    ).fetchone() == (2, 2, 1, 1, 2, 2, "direct_acquisition")


def test_d24_tokenless_acquisition_is_total_and_attempt_bound(
    direct_runtime_db: DirectRuntimeDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = _open_direct_epoch(direct_runtime_db, lease_duration_ms=100)
    connection = direct_runtime_db.connection
    before = connection.execute("SELECT clock_timestamp()").fetchone()
    assert before is not None and isinstance(before[0], datetime)
    first = opened.adapter.acquire_direct_job_atomically(
        opened.epoch_id,
        1,
        opened.root,
    )
    after = connection.execute("SELECT clock_timestamp()").fetchone()
    assert after is not None and isinstance(after[0], datetime)
    assert first.lease.disposition is M5AcquisitionDisposition.DISPATCH_NEW
    assert first.attempt is not None
    assert first.attempt.attempt_ordinal == 1
    assert first.attempt.attempt_id == stable_m4_digest(
        "m4-job-attempt-v1", opened.root.job_id, "1"
    )
    assert first.attempt.lease_token_hash == stable_m4_digest(
        "m4-lease-token-v1", opened.root.job_id, "1"
    )
    assert first.lease.lease_expires_at is not None
    sampled_at = first.lease.lease_expires_at - timedelta(milliseconds=100)
    assert before[0] <= sampled_at <= after[0]

    snapshot = _direct_replay_snapshot(connection, opened.epoch_id)
    live = opened.adapter.acquire_direct_job_atomically(
        opened.epoch_id,
        1,
        opened.root,
    )
    assert live.lease.disposition is M5AcquisitionDisposition.LIVE_LEASE
    assert live.attempt == first.attempt
    assert _direct_replay_snapshot(connection, opened.epoch_id) == snapshot

    equality_time = first.lease.lease_expires_at
    successor_expiry = equality_time + timedelta(milliseconds=100)
    monkeypatch.setattr(
        direct_recovery_module,
        "_sample_database_deadline",
        lambda _cursor, _epoch_id: (equality_time, successor_expiry),
    )
    takeover = opened.adapter.acquire_direct_job_atomically(
        opened.epoch_id,
        2,
        opened.root,
    )
    assert takeover.lease.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert takeover.attempt is not None
    assert takeover.attempt.attempt_ordinal == 2
    assert takeover.attempt.attempt_id == stable_m4_digest(
        "m4-job-attempt-v1", opened.root.job_id, "2"
    )
    assert takeover.attempt.lease_token_hash == stable_m4_digest(
        "m4-lease-token-v1", opened.root.job_id, "2"
    )
    assert takeover.lease.lease_expires_at == successor_expiry
    assert connection.execute(
        """
        SELECT attempt_ordinal, attempt_state, finished_at
        FROM groundloop_semantic_job_attempt
        WHERE job_id = %s ORDER BY attempt_ordinal
        """,
        (opened.root.job_id,),
    ).fetchall() == [
        (1, "expired", equality_time),
        (2, "leased", None),
    ]

    discovery = DiscoveryResult(
        root_job_id=opened.root.job_id,
        result_artifact_id="tokenless-empty-discovery",
        result_artifact_hash=_sha("tokenless-empty-discovery"),
        admitted_pairs=(),
    )
    closure = ChildClosure.build(
        parent_job_id=opened.root.job_id,
        result_artifact_hash=discovery.result_artifact_hash,
        child_job_ids=(),
    )
    completion = JobCompletion.build(
        job_id=opened.root.job_id,
        payload_hash=opened.root.payload_hash,
        execution_spec_hash=opened.root.execution_spec_hash,
        result_artifact_id=discovery.result_artifact_id,
        result_artifact_hash=discovery.result_artifact_hash,
        terminal_state=JobState.COMPLETED_ACTIVE,
        child_closure=closure,
    )
    envelope = M5TypedDirectLateReturnEnvelope.build_discovery(
        epoch_id=opened.epoch_id,
        job=opened.root,
        attempt=takeover.attempt,
        completion=completion,
        discovery=discovery,
        scope=DiscoveryScope(
            opened.root.job_id,
            REGISTRY_ID,
            direct_runtime_db.base.claim_ids,
        ),
        persisted_scope_kind=M5TypedDirectScopeKind.ALL_REGISTERED_CLAIMS,
        explicit_claim_ids=None,
        closed_revision=None,
    )
    settled = opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        3,
        takeover.lease,
        envelope,
        (),
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(direct_discovery_call_count=1),
        None,
    )
    assert settled.normal is not None
    assert settled.normal.resulting_revision == 4
    terminal = opened.adapter.acquire_direct_job_atomically(
        opened.epoch_id,
        3,
        opened.root,
    )
    assert terminal.lease.disposition is M5AcquisitionDisposition.TERMINAL
    assert terminal.lease.already_completed
    assert terminal.attempt == takeover.attempt
    assert terminal.lease.resulting_revision == 4


@pytest.mark.parametrize("with_attempt", (False, True))
def test_token_compatibility_checks_bounded_terminal_candidates_on_reconnect(
    direct_runtime_db: DirectRuntimeDatabase,
    with_attempt: bool,
) -> None:
    database = direct_runtime_db
    opened = _open_direct_epoch(database)
    expected_revision = 1
    persisted_token: str | None = None
    if with_attempt:
        acquisition = opened.adapter.acquire_direct_job_atomically(
            opened.epoch_id,
            expected_revision,
            opened.root,
        )
        assert acquisition.attempt is not None
        persisted_token = acquisition.attempt.lease_token_hash
        expected_revision = acquisition.lease.resulting_revision

    assert opened.open_receipt is not None
    terminal = opened.adapter.fail_typed_epoch_with_open_receipt_atomically(
        opened.epoch_id,
        expected_revision,
        M5RunFailureReason.INVALID_ARTIFACT,
        opened.open_receipt,
        M5RuntimeWork(),
    )
    assert terminal.state is M5RunState.FAILED
    schema_row = database.connection.execute("SELECT current_schema()").fetchone()
    assert schema_row is not None and type(schema_row[0]) is str
    schema_name = schema_row[0]
    before = database_snapshot(database.connection)

    with psycopg.connect(_database_url(), autocommit=True) as connection:
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
        )
        ports = PostgresM4ApplicationPorts(
            connection,
            structural_payloads={},
            execution_mode=M4ExecutionMode.MEASURED,
        )
        adapter = PostgresM5DirectM4Adapter(ports)
        with pytest.raises(
            EventConflictError,
            match=(
                "terminal direct lease token is not a bounded deterministic candidate"
            ),
        ):
            with connection.transaction(), connection.cursor() as cursor:
                adapter.acquire_direct_job(
                    cursor,
                    opened.epoch_id,
                    expected_revision,
                    opened.root,
                    _sha("wrong-terminal-compatibility-token"),
                )
        assert database_snapshot(connection) == before

        accepted_tokens = (
            (_deterministic_lease_token(opened.root, 1),)
            if persisted_token is None
            else (
                persisted_token,
                _deterministic_lease_token(opened.root, 2),
            )
        )
        for accepted_token in accepted_tokens:
            with connection.transaction(), connection.cursor() as cursor:
                replay_lease = adapter.acquire_direct_job(
                    cursor,
                    opened.epoch_id,
                    expected_revision,
                    opened.root,
                    accepted_token,
                )
            assert replay_lease.disposition is M5AcquisitionDisposition.TERMINAL
            assert replay_lease.lease_token_hash == persisted_token
            assert database_snapshot(connection) == before

        if persisted_token is None:
            replay = adapter.acquire_direct_job_atomically(
                opened.epoch_id,
                expected_revision,
                opened.root,
            )
            assert replay.attempt is None
            assert replay.lease.disposition is M5AcquisitionDisposition.TERMINAL
        assert database_snapshot(connection) == before


def test_token_compatibility_rejects_runtime_none_before_database_action(
    direct_runtime_db: DirectRuntimeDatabase,
) -> None:
    opened = _open_direct_epoch(direct_runtime_db)
    before = database_snapshot(direct_runtime_db.connection)
    with (
        direct_runtime_db.connection.transaction(),
        direct_runtime_db.connection.cursor() as cursor,
    ):
        with pytest.raises(
            ValidationError,
            match="compatibility token must be exact text",
        ):
            opened.adapter.acquire_direct_job(
                cursor,
                opened.epoch_id,
                1,
                opened.root,
                cast(str, None),
            )
    assert database_snapshot(direct_runtime_db.connection) == before


def test_d24_direct_expansion_is_one_lock_normal_and_exact_replay(
    direct_runtime_db: DirectRuntimeDatabase,
) -> None:
    opened = _open_direct_epoch(direct_runtime_db)
    connection = direct_runtime_db.connection
    with connection.transaction(), connection.cursor() as cursor:
        lease = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            1,
            opened.root,
            _deterministic_lease_token(opened.root),
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    assert lease.attempt_id is not None
    assert lease.lease_token_hash is not None
    discovery, completion, child = _complete_expansion(
        opened.epoch_id,
        opened.event,
        opened.root,
        direct_runtime_db.base.claim_ids[0],
    )
    attempt = JobAttempt(
        attempt_id=lease.attempt_id,
        job_id=opened.root.job_id,
        execution_spec_hash=opened.root.execution_spec_hash,
        attempt_ordinal=1,
        lease_token_hash=lease.lease_token_hash,
    )
    open_scope = DiscoveryScope(
        opened.root.job_id,
        REGISTRY_ID,
        direct_runtime_db.base.claim_ids,
    )
    envelope = M5TypedDirectLateReturnEnvelope.build_discovery(
        epoch_id=opened.epoch_id,
        job=opened.root,
        attempt=attempt,
        completion=completion,
        discovery=discovery,
        scope=open_scope,
        persisted_scope_kind=M5TypedDirectScopeKind.ALL_REGISTERED_CLAIMS,
        explicit_claim_ids=None,
        closed_revision=None,
    )
    work = M5RuntimeWork(direct_discovery_call_count=1)

    first = opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        2,
        lease,
        envelope,
        (child,),
        M5ExecutionEvidenceDisposition.RETURNED,
        work,
        None,
    )
    assert first.normal is not None and first.late is None
    assert not first.normal.exact_replay
    assert first.normal.resulting_revision == 3
    assert first.normal.return_artifact_digest == discovery.result_artifact_hash
    assert first.normal.transition_anchor is not None

    replay = opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        2,
        lease,
        envelope,
        (child,),
        M5ExecutionEvidenceDisposition.RETURNED,
        work,
        None,
    )
    assert replay.normal is not None and replay.late is None
    assert replay.normal.exact_replay
    assert replay.normal.resulting_revision == 3
    assert replay.normal.transition_anchor is None
    assert connection.execute(
        """
        SELECT count(*) FILTER (WHERE contribution_kind = 'direct_transition'),
               count(*) FILTER (
                   WHERE contribution_kind = 'direct_attempt_execution'
               )
        FROM groundloop_m5_runtime_work_contribution
        WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ).fetchone() == (1, 1)


def test_d24_direct_verifier_is_one_lock_normal_and_exact_replay(
    direct_runtime_db: DirectRuntimeDatabase,
) -> None:
    make_effective = True
    opened = _open_direct_epoch(direct_runtime_db)
    connection = direct_runtime_db.connection
    with connection.transaction(), connection.cursor() as cursor:
        root_lease = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            1,
            opened.root,
            _deterministic_lease_token(opened.root),
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    assert root_lease.attempt_id is not None
    assert root_lease.lease_token_hash is not None
    discovery, expansion_completion, child = _complete_expansion(
        opened.epoch_id,
        opened.event,
        opened.root,
        direct_runtime_db.base.claim_ids[0],
    )
    root_attempt = JobAttempt(
        attempt_id=root_lease.attempt_id,
        job_id=opened.root.job_id,
        execution_spec_hash=opened.root.execution_spec_hash,
        attempt_ordinal=1,
        lease_token_hash=root_lease.lease_token_hash,
    )
    expansion_envelope = M5TypedDirectLateReturnEnvelope.build_discovery(
        epoch_id=opened.epoch_id,
        job=opened.root,
        attempt=root_attempt,
        completion=expansion_completion,
        discovery=discovery,
        scope=DiscoveryScope(
            opened.root.job_id,
            REGISTRY_ID,
            direct_runtime_db.base.claim_ids,
        ),
        persisted_scope_kind=M5TypedDirectScopeKind.ALL_REGISTERED_CLAIMS,
        explicit_claim_ids=None,
        closed_revision=None,
    )
    opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        2,
        root_lease,
        expansion_envelope,
        (child,),
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(direct_discovery_call_count=1),
        None,
    )
    with connection.transaction(), connection.cursor() as cursor:
        child_lease = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            3,
            child,
            _deterministic_lease_token(child),
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    assert child_lease.attempt_id is not None
    assert child_lease.lease_token_hash is not None
    completion, observation = _verifier_completion(child)
    child_attempt = JobAttempt(
        attempt_id=child_lease.attempt_id,
        job_id=child.job_id,
        execution_spec_hash=child.execution_spec_hash,
        attempt_ordinal=1,
        lease_token_hash=child_lease.lease_token_hash,
    )
    envelope = M5TypedDirectLateReturnEnvelope.build_verifier(
        epoch_id=opened.epoch_id,
        job=child,
        attempt=child_attempt,
        completion=completion,
        verification_execution=None,
        observation=observation,
        observation_produced_epoch=opened.epoch_id,
        observation_raw_output_hash=completion.result_artifact_hash,
        observation_eligible_for_currency=True,
        requested_make_effective=make_effective,
    )
    work = M5RuntimeWork()

    first = opened.adapter.settle_direct_verifier_atomically(
        opened.epoch_id,
        4,
        child_lease,
        envelope,
        M5ExecutionEvidenceDisposition.RETURNED,
        work,
        None,
    )
    assert first.return_kind is M5TypedDirectReturnKind.VERIFIER
    assert first.normal is not None and first.late is None
    assert not first.normal.exact_replay
    assert first.normal.resulting_revision == 5
    assert first.normal.return_artifact_digest == completion.result_artifact_hash
    assert first.normal.observation_completion is not None
    assert first.normal.observation_completion.artifact_stored
    assert first.normal.observation_completion.made_effective is make_effective
    assert first.normal.transition_anchor is not None

    replay = opened.adapter.settle_direct_verifier_atomically(
        opened.epoch_id,
        4,
        child_lease,
        envelope,
        M5ExecutionEvidenceDisposition.RETURNED,
        work,
        None,
    )
    assert replay.return_kind is M5TypedDirectReturnKind.VERIFIER
    assert replay.normal is not None and replay.late is None
    assert replay.normal.exact_replay
    assert replay.normal.resulting_revision == 5
    assert replay.normal.observation_completion == first.normal.observation_completion
    assert replay.normal.transition_anchor is None
    with pytest.raises(EventConflictError):
        opened.adapter.settle_direct_verifier_atomically(
            opened.epoch_id,
            4,
            child_lease,
            envelope,
            M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
            work,
            None,
        )
    assert connection.execute(
        """
        SELECT runtime.revision,
               count(*) FILTER (WHERE contribution_kind = 'direct_transition'),
               count(*) FILTER (
                   WHERE contribution_kind = 'direct_attempt_execution'
               ),
               (SELECT count(*)
                FROM groundloop_m5_typed_direct_late_return_envelope
                WHERE epoch_id = %s)
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_m5_runtime_work_contribution AS contribution
          ON contribution.epoch_id = runtime.epoch_id
        WHERE runtime.epoch_id = %s
        GROUP BY runtime.revision
        """,
        (opened.epoch_id, opened.epoch_id),
    ).fetchone() == (5, 2, 2, 0)


def test_public_m4_start_attempt_rejects_past_explicit_deadline(
    direct_runtime_db: DirectRuntimeDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = _open_direct_epoch(direct_runtime_db)
    store = direct_runtime_db.ports.runtime_store
    monkeypatch.setattr(store, "_reject_typed_epoch_mutation", lambda _epoch_id: None)
    attempt = JobAttempt(
        attempt_id=stable_m4_digest("m4-job-attempt-v1", opened.root.job_id, "1"),
        job_id=opened.root.job_id,
        execution_spec_hash=opened.root.execution_spec_hash,
        attempt_ordinal=1,
        lease_token_hash=_deterministic_lease_token(opened.root),
    )

    with pytest.raises(
        ValidationError, match="attempt lease must expire in the future"
    ):
        store.start_attempt_point(
            opened.epoch_id,
            1,
            attempt,
            lease_expires_at=datetime.now(UTC) - timedelta(seconds=1),
        )

    assert direct_runtime_db.connection.execute(
        "SELECT revision FROM groundloop_epoch WHERE epoch_id = %s",
        (opened.epoch_id,),
    ).fetchone() == (1,)
    assert direct_runtime_db.connection.execute(
        "SELECT count(*) FROM groundloop_semantic_job_attempt WHERE job_id = %s",
        (opened.root.job_id,),
    ).fetchone() == (0,)


@pytest.mark.parametrize(
    ("table", "message"),
    (
        (
            "groundloop_m5_runtime_work_accumulator",
            "work accumulator revision differs from locked cutoff",
        ),
        (
            "groundloop_m5_runtime_timing_accumulator",
            "anchor input revision differs from timing accumulator",
        ),
    ),
)
def test_d24_direct_acquisition_rejects_accumulator_revision_drift(
    direct_runtime_db: DirectRuntimeDatabase,
    table: str,
    message: str,
) -> None:
    opened = _open_direct_epoch(direct_runtime_db)
    connection = direct_runtime_db.connection

    with pytest.raises(ValidationError, match=message):
        with connection.transaction(), connection.cursor() as cursor:
            _authorize(cursor, opened.epoch_id, 1)
            _force_accumulator_revision_for_test(
                cursor,
                table_name=table,
                epoch_id=opened.epoch_id,
                revision=2,
            )
            opened.adapter.acquire_direct_job(
                cursor,
                opened.epoch_id,
                1,
                opened.root,
                _deterministic_lease_token(opened.root),
            )

    assert connection.execute(
        "SELECT revision FROM groundloop_epoch WHERE epoch_id = %s",
        (opened.epoch_id,),
    ).fetchone() == (1,)
    assert connection.execute(
        "SELECT count(*) FROM groundloop_m5_dispatch_record WHERE epoch_id = %s",
        (opened.epoch_id,),
    ).fetchone() == (0,)
    assert connection.execute(
        "SELECT count(*) FROM groundloop_semantic_job_attempt WHERE job_id = %s",
        (opened.root.job_id,),
    ).fetchone() == (0,)


def test_d24_direct_takeover_is_dense_and_second_acquirer_observes_live(
    direct_runtime_db: DirectRuntimeDatabase,
) -> None:
    opened = _open_direct_epoch(direct_runtime_db, lease_duration_ms=100)
    connection = direct_runtime_db.connection
    with connection.transaction(), connection.cursor() as cursor:
        first = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            1,
            opened.root,
            _deterministic_lease_token(opened.root, 1),
        )
        assert first.disposition is M5AcquisitionDisposition.DISPATCH_NEW
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

    connection.execute("SELECT pg_sleep(0.15)")
    with connection.transaction(), connection.cursor() as cursor:
        successor = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            2,
            opened.root,
            _deterministic_lease_token(opened.root, 2),
        )
        assert successor.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
        assert successor.resulting_revision == 3
        assert successor.attempt_id != first.attempt_id
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

    with connection.transaction(), connection.cursor() as cursor:
        live = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            2,
            opened.root,
            _deterministic_lease_token(opened.root, 2),
        )
        assert live.disposition is M5AcquisitionDisposition.LIVE_LEASE
        assert live.attempt_id == successor.attempt_id
        assert live.resulting_revision == 3
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

    assert tuple(
        connection.execute(
            """
            SELECT attempt_ordinal, attempt_state
            FROM groundloop_semantic_job_attempt
            WHERE job_id = %s
            ORDER BY attempt_ordinal
            """,
            (opened.root.job_id,),
        ).fetchall()
    ) == ((1, "expired"), (2, "leased"))
    assert connection.execute(
        """
        SELECT count(*), count(*) FILTER (
                   WHERE contribution_kind = 'direct_acquisition'
               )
        FROM groundloop_m5_runtime_work_contribution
        WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ).fetchone() == (3, 2)


def test_d24_direct_retryable_failure_persists_exact_point_evidence(
    direct_runtime_db: DirectRuntimeDatabase,
) -> None:
    opened = _open_direct_epoch(direct_runtime_db)
    connection = direct_runtime_db.connection
    with connection.transaction(), connection.cursor() as cursor:
        lease = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            1,
            opened.root,
            _deterministic_lease_token(opened.root),
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

    work = M5RuntimeWork(
        direct_discovery_call_count=1,
        embedding_model_call_count=1,
        embedding_input_token_count=7,
    )
    timing = M5RuntimeTiming(
        coordinator_non_db_non_neural_ns=11,
        neural_wall_ns=12,
        postgres_roundtrip_wall_ns=13,
        external_io_wall_ns=14,
        end_to_end_wall_ns=15,
    )
    with connection.transaction(), connection.cursor() as cursor:
        receipt = opened.adapter.mark_direct_retryable_failure(
            cursor,
            opened.epoch_id,
            2,
            lease,
            _sha("direct-retryable-error"),
            work,
            timing,
        )
        anchor = M5TransitionTimingAnchor.build(
            epoch_id=opened.epoch_id,
            contribution_kind=(M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION),
            source_id=receipt.attempt_id,
            anchor_revision=3,
            terminal_transition=False,
        )
        opened.adapter.install_outer_transition_anchor(cursor, anchor)
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

    with connection.transaction(), connection.cursor() as cursor:
        replay = opened.adapter.mark_direct_retryable_failure(
            cursor,
            opened.epoch_id,
            2,
            lease,
            _sha("direct-retryable-error"),
            work,
            timing,
        )
        assert replay == receipt
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

    assert connection.execute(
        """
        SELECT base.revision, runtime.revision, job.job_state,
               evidence.disposition, contribution.work_digest,
               timing.required_interval_observed,
               accumulator.pending_contribution_kind,
               accumulator.pending_anchor_revision
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_semantic_job AS job USING (epoch_id)
        JOIN groundloop_m5_attempt_execution_evidence AS evidence
          ON evidence.epoch_id = base.epoch_id AND evidence.subgraph = 'direct'
        JOIN groundloop_m5_runtime_work_contribution AS contribution
          ON contribution.epoch_id = base.epoch_id
         AND contribution.contribution_kind = 'direct_attempt_execution'
        JOIN groundloop_m5_runtime_timing_contribution AS timing
          ON timing.epoch_id = base.epoch_id AND timing.subgraph = 'direct'
        JOIN groundloop_m5_runtime_timing_accumulator AS accumulator
          ON accumulator.epoch_id = base.epoch_id
        WHERE base.epoch_id = %s AND job.job_id = %s
        """,
        (opened.epoch_id, opened.root.job_id),
    ).fetchone() == (
        3,
        3,
        "retryable_failed",
        "retryable_failure",
        work.work_digest,
        True,
        "direct_attempt_execution",
        3,
    )


def test_d24_atomic_retryable_failure_replay_has_no_second_anchor(
    direct_runtime_db: DirectRuntimeDatabase,
) -> None:
    opened = _open_direct_epoch(direct_runtime_db)
    acquisition = opened.adapter.acquire_direct_job_atomically(
        opened.epoch_id,
        1,
        opened.root,
    )
    work = M5RuntimeWork(
        direct_discovery_call_count=1,
        embedding_model_call_count=1,
    )
    timing = M5RuntimeTiming(
        coordinator_non_db_non_neural_ns=2,
        neural_wall_ns=3,
        postgres_roundtrip_wall_ns=5,
        external_io_wall_ns=7,
        end_to_end_wall_ns=11,
    )

    first = opened.adapter.mark_direct_retryable_failure_atomically(
        opened.epoch_id,
        acquisition.lease.resulting_revision,
        acquisition.lease,
        _sha("atomic-retryable-replay-error"),
        work,
        timing,
    )
    assert not first.exact_replay
    assert first.resulting_revision == acquisition.lease.resulting_revision + 1
    assert first.transition_anchor is not None
    anchor = first.transition_anchor
    store = PostgresM5RuntimeStore(direct_runtime_db.connection)
    appended = store.append_transition_call_timing(
        anchor.epoch_id,
        anchor.contribution_kind,
        anchor.source_id,
        anchor.contribution_key_digest,
        anchor.anchor_revision,
        timing,
    )
    assert not appended.exact_replay
    assert appended.resulting_revision == first.resulting_revision

    successor = opened.adapter.acquire_direct_job_atomically(
        opened.epoch_id,
        first.resulting_revision,
        opened.root,
    )
    before_replay = database_snapshot(direct_runtime_db.connection)
    pending_before = direct_runtime_db.connection.execute(
        """
        SELECT pending_contribution_kind, pending_source_id,
               pending_anchor_revision, required_expected_count
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        """,
        (opened.epoch_id,),
    ).fetchone()
    replay = opened.adapter.mark_direct_retryable_failure_atomically(
        opened.epoch_id,
        acquisition.lease.resulting_revision,
        acquisition.lease,
        _sha("atomic-retryable-replay-error"),
        work,
        timing,
    )

    assert replay.receipt == first.receipt
    assert replay.resulting_revision == successor.lease.resulting_revision
    assert replay.exact_replay
    assert replay.transition_anchor is None
    assert database_snapshot(direct_runtime_db.connection) == before_replay
    assert (
        direct_runtime_db.connection.execute(
            """
        SELECT pending_contribution_kind, pending_source_id,
               pending_anchor_revision, required_expected_count
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        """,
            (opened.epoch_id,),
        ).fetchone()
        == pending_before
    )


def test_d24_direct_failure_rejects_timing_accumulator_revision_drift(
    direct_runtime_db: DirectRuntimeDatabase,
) -> None:
    opened = _open_direct_epoch(direct_runtime_db)
    connection = direct_runtime_db.connection
    with connection.transaction(), connection.cursor() as cursor:
        lease = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            1,
            opened.root,
            _deterministic_lease_token(opened.root),
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

    with pytest.raises(
        ValidationError,
        match="timing accumulator revision differs from locked cutoff",
    ):
        with connection.transaction(), connection.cursor() as cursor:
            _authorize(cursor, opened.epoch_id, 2)
            _force_accumulator_revision_for_test(
                cursor,
                table_name="groundloop_m5_runtime_timing_accumulator",
                epoch_id=opened.epoch_id,
                revision=1,
            )
            opened.adapter.mark_direct_retryable_failure(
                cursor,
                opened.epoch_id,
                2,
                lease,
                _sha("drifted-failure"),
                M5RuntimeWork(direct_discovery_call_count=1),
                None,
            )

    assert connection.execute(
        """
        SELECT base.revision, runtime.revision, job.job_state,
               work.updated_revision, timing.updated_revision
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_semantic_job AS job USING (epoch_id)
        JOIN groundloop_m5_runtime_work_accumulator AS work USING (epoch_id)
        JOIN groundloop_m5_runtime_timing_accumulator AS timing USING (epoch_id)
        WHERE base.epoch_id = %s AND job.job_id = %s
        """,
        (opened.epoch_id, opened.root.job_id),
    ).fetchone() == (2, 2, "running", 2, 2)
    assert connection.execute(
        """
        SELECT count(*) FROM groundloop_m5_attempt_execution_evidence
        WHERE epoch_id = %s AND subgraph = 'direct'
        """,
        (opened.epoch_id,),
    ).fetchone() == (0,)


def test_d24_standalone_terminal_failure_paths_reject_without_writes(
    direct_runtime_db: DirectRuntimeDatabase,
) -> None:
    opened = _open_direct_epoch(direct_runtime_db)
    connection = direct_runtime_db.connection
    with connection.transaction(), connection.cursor() as cursor:
        lease = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            1,
            opened.root,
            _deterministic_lease_token(opened.root),
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

    work = M5RuntimeWork(direct_discovery_call_count=1)
    before = _direct_replay_snapshot(connection, opened.epoch_id)
    for settle in (
        opened.adapter.mark_direct_terminal_failure,
        opened.adapter._recovery.mark_direct_terminal_failure,
    ):
        with pytest.raises(
            EventConflictError,
            match="requires the checked combined operation",
        ):
            with connection.transaction(), connection.cursor() as cursor:
                settle(
                    cursor,
                    opened.epoch_id,
                    2,
                    lease,
                    "opaque-provider-terminal-wire",
                    _sha("direct-terminal-error"),
                    work,
                    None,
                )
    assert _direct_replay_snapshot(connection, opened.epoch_id) == before
    assert connection.execute(
        """
        SELECT revision, job_state, completion_digest
        FROM groundloop_epoch
        JOIN groundloop_semantic_job USING (epoch_id)
        WHERE epoch_id = %s AND job_id = %s
        """,
        (opened.epoch_id, opened.root.job_id),
    ).fetchone() == (2, "running", None)


def _direct_replay_snapshot(
    connection: Connection[Any], epoch_id: int
) -> tuple[object, ...]:
    return (
        connection.execute(
            """
            SELECT revision, structural_status, semantic_status,
                   evaluation_state, open_job_count, open_scope_count
            FROM groundloop_epoch WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone(),
        connection.execute(
            """
            SELECT revision, runtime_state, open_work_count, open_scope_count
            FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone(),
        tuple(
            connection.execute(
                """
                SELECT job_id, job_state, parent_job_id, completed_revision
                FROM groundloop_semantic_job
                WHERE epoch_id = %s ORDER BY job_id
                """,
                (epoch_id,),
            ).fetchall()
        ),
        tuple(
            connection.execute(
                """
                SELECT root_job_id, registry_snapshot_id, closed_revision
                FROM groundloop_discovery_scope
                WHERE epoch_id = %s ORDER BY root_job_id
                """,
                (epoch_id,),
            ).fetchall()
        ),
        connection.execute(
            """
            SELECT lifecycle_state, default_evaluation_state,
                   open_discovery_scope_count, revision
            FROM groundloop_m4_evaluation_epoch_counter
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone(),
        connection.execute(
            "SELECT count(*) FROM groundloop_m4_update WHERE epoch_id = %s",
            (epoch_id,),
        ).fetchone(),
    )


def _reconnect_and_replay_open(
    database: DirectRuntimeDatabase,
    opened: _OpenedDirectEpoch,
    *,
    expected_revision: int,
) -> tuple[PostgresM4ApplicationPorts, PostgresM5DirectM4Adapter]:
    ports = PostgresM4ApplicationPorts(
        database.connection,
        structural_payloads={},
        execution_mode=M4ExecutionMode.MEASURED,
    )
    adapter = PostgresM5DirectM4Adapter(ports)
    before = _direct_replay_snapshot(database.connection, opened.epoch_id)

    with database.connection.transaction(), database.connection.cursor() as cursor:
        with pytest.raises(
            ValidationError,
            match="open declaration cannot supply a closed scope",
        ):
            adapter.stage_direct_open(
                cursor,
                opened.event,
                opened.payload,
                opened.withdrawal,
                (opened.root,),
                (replace(opened.scope, closed=True),),
            )

        replay = adapter.stage_direct_open(
            cursor,
            opened.event,
            opened.payload,
            opened.withdrawal,
            (opened.root,),
            (opened.scope,),
        )
        assert replay.replayed
        assert replay.epoch_id == opened.epoch_id
        header = ports.runtime_store.read_epoch_header_point(
            opened.epoch_id, cursor=cursor
        )
        assert header.revision == expected_revision
    adapter._after_outer_commit()

    assert _direct_replay_snapshot(database.connection, opened.epoch_id) == before
    assert ports._active_epoch_id == opened.epoch_id
    inserted = opened.payload.inserted
    assert inserted is not None
    assert (
        ports._working_repository.document_version(NEW_DOCUMENT_VERSION_ID)
        == inserted.version
    )
    assert ports._working_repository.is_chunk_active(NEW_CHUNK_ID)
    return ports, adapter


def test_first_direct_declaration_still_requires_revision_one(
    direct_runtime_db: DirectRuntimeDatabase,
) -> None:
    database = direct_runtime_db
    event, payload = _event_and_payload(database)
    withdrawal = database.ports.plan_exact_withdrawal(event)
    root = _job(
        event,
        kind=JobKind.IMPACT_DISCOVERY,
        execution_hash=IMPACT_EXECUTION_HASH,
        chunk_id=NEW_CHUNK_ID,
    )
    scope = DiscoveryScope(root.job_id, REGISTRY_ID, ())
    adapter = PostgresM5DirectM4Adapter(database.ports)

    with pytest.raises(_RollbackOuterTransaction):
        try:
            with (
                database.connection.transaction(),
                database.connection.cursor() as cursor,
            ):
                epoch_id, _, _ = _insert_typed_outer_declaration(
                    cursor, database, event
                )
                _authorize(cursor, epoch_id, 1)
                cursor.execute(
                    "UPDATE groundloop_epoch SET revision = 2 WHERE epoch_id = %s",
                    (epoch_id,),
                )
                cursor.execute(
                    """
                    UPDATE groundloop_m5_runtime_epoch
                    SET runtime_state = 'semantic_pending', revision = 2
                    WHERE epoch_id = %s
                    """,
                    (epoch_id,),
                )
                with pytest.raises(
                    EventConflictError,
                    match="requires the revision-1 structural epoch",
                ):
                    adapter.stage_direct_open(
                        cursor,
                        event,
                        payload,
                        withdrawal,
                        (root,),
                        (scope,),
                    )
                raise _RollbackOuterTransaction
        finally:
            adapter._after_outer_rollback()

    assert database.connection.execute(
        "SELECT count(*) FROM groundloop_epoch WHERE event_id = %s",
        (event.update.event_id,),
    ).fetchone() == (0,)


def test_exact_open_replay_rehydrates_after_acquire_and_scope_closure(
    direct_runtime_db: DirectRuntimeDatabase,
) -> None:
    database = direct_runtime_db
    opened = _open_direct_epoch(database)

    with database.connection.transaction(), database.connection.cursor() as cursor:
        root_typed_lease = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            1,
            opened.root,
            _deterministic_lease_token(opened.root),
        )
        assert root_typed_lease.resulting_revision == 2
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

    _, acquisition_adapter = _reconnect_and_replay_open(
        database,
        opened,
        expected_revision=2,
    )
    discovery, root_completion, child = _complete_expansion(
        opened.epoch_id,
        opened.event,
        opened.root,
        database.base.claim_ids[0],
    )
    expansion_envelope = _discovery_return_envelope(
        epoch_id=opened.epoch_id,
        job=opened.root,
        lease=root_typed_lease,
        discovery=discovery,
        completion=root_completion,
        registered_claim_ids=database.base.claim_ids,
    )
    expanded = acquisition_adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        2,
        root_typed_lease,
        expansion_envelope,
        (child,),
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert expanded.normal is not None and expanded.normal.resulting_revision == 3

    assert database.connection.execute(
        """
        SELECT closed_revision FROM groundloop_discovery_scope
        WHERE root_job_id = %s
        """,
        (opened.root.job_id,),
    ).fetchone() == (3,)
    _, closed_scope_adapter = _reconnect_and_replay_open(
        database,
        opened,
        expected_revision=3,
    )

    with database.connection.transaction(), database.connection.cursor() as cursor:
        child_typed_lease = closed_scope_adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            3,
            child,
            _deterministic_lease_token(child),
        )
        assert child_typed_lease.resulting_revision == 4
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

    completion, observation = _verifier_completion(child)
    verifier_envelope = _verifier_return_envelope(
        epoch_id=opened.epoch_id,
        job=child,
        lease=child_typed_lease,
        completion=completion,
        observation=observation,
        make_effective=True,
    )
    verified = closed_scope_adapter.settle_direct_verifier_atomically(
        opened.epoch_id,
        4,
        child_typed_lease,
        verifier_envelope,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert verified.normal is not None
    assert verified.normal.observation_completion is not None
    assert verified.normal.observation_completion.made_effective

    assert database.connection.execute(
        """
        SELECT base.revision, base.semantic_status,
               runtime.revision, runtime.runtime_state
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        WHERE base.epoch_id = %s
        """,
        (opened.epoch_id,),
    ).fetchone() == (5, "complete", 5, "semantic_complete")


def test_cursor_local_direct_subgraph_is_atomic_and_outer_authoritative(
    direct_runtime_db: DirectRuntimeDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = direct_runtime_db
    connection = database.connection
    event, payload = _event_and_payload(database)
    withdrawal: StructuralWithdrawal = database.ports.plan_exact_withdrawal(event)
    root = _job(
        event,
        kind=JobKind.IMPACT_DISCOVERY,
        execution_hash=IMPACT_EXECUTION_HASH,
        chunk_id=NEW_CHUNK_ID,
    )
    scope = DiscoveryScope(root.job_id, REGISTRY_ID, ())
    adapter = PostgresM5DirectM4Adapter(database.ports)
    rolled_back_requirements: RequirementRegistrySnapshot | None = None
    rolled_back_active_chunks: ActiveChunkSnapshot | None = None

    with pytest.raises(_RollbackOuterTransaction):
        try:
            with connection.transaction(), connection.cursor() as cursor:
                (
                    rolled_back_epoch,
                    rolled_back_requirements,
                    rolled_back_active_chunks,
                ) = _insert_typed_outer_declaration(cursor, database, event)
                opened = adapter.stage_direct_open(
                    cursor,
                    event,
                    payload,
                    withdrawal,
                    (root,),
                    (scope,),
                )
                assert opened.epoch_id == rolled_back_epoch
                _persist_typed_outer_snapshots(
                    cursor,
                    rolled_back_epoch,
                    rolled_back_requirements,
                    rolled_back_active_chunks,
                )
                cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
                raise _RollbackOuterTransaction
        finally:
            adapter._after_outer_rollback()

    assert connection.execute(
        "SELECT count(*) FROM groundloop_epoch WHERE event_id = %s",
        (event.update.event_id,),
    ).fetchone() == (0,)
    assert connection.execute(
        "SELECT count(*) FROM groundloop_m4_update WHERE epoch_id = %s",
        (rolled_back_epoch,),
    ).fetchone() == (0,)
    assert connection.execute(
        "SELECT count(*) FROM groundloop_document_version "
        "WHERE document_version_id = %s",
        (NEW_DOCUMENT_VERSION_ID,),
    ).fetchone() == (0,)
    assert rolled_back_requirements is not None
    assert rolled_back_active_chunks is not None
    assert connection.execute(
        """
        SELECT count(*) FROM groundloop_m5_requirement_registry_snapshot
        WHERE requirement_registry_snapshot_digest = %s
        """,
        (rolled_back_requirements.requirement_registry_snapshot_digest,),
    ).fetchone() == (0,)
    assert connection.execute(
        """
        SELECT count(*) FROM groundloop_m5_active_chunk_snapshot
        WHERE active_chunk_snapshot_digest = %s
        """,
        (rolled_back_active_chunks.active_chunk_snapshot_digest,),
    ).fetchone() == (0,)
    assert database.ports._active_epoch_id is None

    with connection.transaction(), connection.cursor() as cursor:
        epoch_id, requirements, active_chunks = _insert_typed_outer_declaration(
            cursor, database, event
        )
        opened = adapter.stage_direct_open(
            cursor,
            event,
            payload,
            withdrawal,
            (root,),
            (scope,),
        )
        assert opened.epoch_id == epoch_id
        assert not opened.replayed
        _persist_typed_outer_snapshots(
            cursor,
            epoch_id,
            requirements,
            active_chunks,
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

    original_adopt = database.ports._adopt_direct_working_cache

    def fail_cache_adoption(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("injected cache adoption failure")

    monkeypatch.setattr(
        database.ports,
        "_adopt_direct_working_cache",
        fail_cache_adoption,
    )
    with pytest.raises(RuntimeError, match="injected cache adoption failure"):
        adapter._after_outer_commit()
    assert database.ports._active_epoch_id is None
    assert connection.execute(
        "SELECT count(*) FROM groundloop_m4_update WHERE epoch_id = %s",
        (epoch_id,),
    ).fetchone() == (1,)

    monkeypatch.setattr(
        database.ports,
        "_adopt_direct_working_cache",
        original_adopt,
    )
    with connection.transaction(), connection.cursor() as cursor:
        replay = adapter.stage_direct_open(
            cursor,
            event,
            payload,
            withdrawal,
            (root,),
            (scope,),
        )
        assert replay.replayed
        assert replay.epoch_id == epoch_id
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    adapter._after_outer_commit()
    assert database.ports._active_epoch_id == epoch_id

    assert connection.execute(
        "SELECT epoch_id FROM groundloop_m4_publication_head WHERE singleton"
    ).fetchone() == (database.base.epoch_id,)
    assert connection.execute(
        "SELECT epoch_id FROM groundloop_m5_publication_head WHERE singleton"
    ).fetchone() == (database.base.epoch_id,)
    assert connection.execute(
        """
        SELECT base.revision, base.semantic_status,
               runtime.revision, runtime.runtime_state,
               base.open_job_count, base.open_scope_count
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        WHERE base.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone() == (1, "pending", 1, "structural_committed", 1, 1)

    with pytest.raises(_RollbackOuterTransaction):
        try:
            with connection.transaction(), connection.cursor() as cursor:
                adapter.stage_direct_failure(
                    cursor,
                    epoch_id,
                    1,
                    "injected direct failure",
                )
                assert cursor.execute(
                    "SELECT revision, semantic_status FROM groundloop_epoch "
                    "WHERE epoch_id = %s",
                    (epoch_id,),
                ).fetchone() == (1, "pending")
                assert cursor.execute(
                    """
                    SELECT lifecycle_state, revision
                    FROM groundloop_m4_evaluation_epoch_counter
                    WHERE epoch_id = %s
                    """,
                    (epoch_id,),
                ).fetchone() == (EvaluationLifecycle.FAILED.value, 2)
                raise _RollbackOuterTransaction
        finally:
            adapter._after_outer_rollback()

    assert database.ports._active_epoch_id == epoch_id
    assert connection.execute(
        """
        SELECT manifest #>>
               '{_groundloop_m4_runtime_v1,failure_reason}'
        FROM groundloop_m4_update WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone() == (None,)

    with connection.transaction(), connection.cursor() as cursor:
        root_typed_lease = adapter.acquire_direct_job(
            cursor,
            epoch_id,
            1,
            root,
            _deterministic_lease_token(root),
        )
        assert root_typed_lease.resulting_revision == 2
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    root_lease = _m4_job_lease(root_typed_lease)

    discovery, root_completion, child = _complete_expansion(
        epoch_id,
        event,
        root,
        database.base.claim_ids[0],
    )
    with pytest.raises(_RollbackOuterTransaction):
        try:
            with connection.transaction(), connection.cursor() as cursor:
                _authorize(cursor, epoch_id, 2)
                adapter.stage_direct_expansion(
                    cursor,
                    epoch_id,
                    2,
                    root_lease,
                    discovery,
                    root_completion,
                    (child,),
                )
                assert cursor.execute(
                    "SELECT revision FROM groundloop_epoch WHERE epoch_id = %s",
                    (epoch_id,),
                ).fetchone() == (3,)
                raise _RollbackOuterTransaction
        finally:
            adapter._after_outer_rollback()

    expansion_envelope = _discovery_return_envelope(
        epoch_id=epoch_id,
        job=root,
        lease=root_typed_lease,
        discovery=discovery,
        completion=root_completion,
        registered_claim_ids=database.base.claim_ids,
    )
    expansion = adapter.settle_direct_expansion_atomically(
        epoch_id,
        2,
        root_typed_lease,
        expansion_envelope,
        (child,),
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert expansion.normal is not None
    assert expansion.normal.resulting_revision == 3

    with connection.transaction(), connection.cursor() as cursor:
        child_typed_lease = adapter.acquire_direct_job(
            cursor,
            epoch_id,
            3,
            child,
            _deterministic_lease_token(child),
        )
        assert child_typed_lease.resulting_revision == 4
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    child_lease = _m4_job_lease(child_typed_lease)

    verifier_completion, observation = _verifier_completion(child)
    with pytest.raises(_RollbackOuterTransaction):
        try:
            with connection.transaction(), connection.cursor() as cursor:
                _authorize(cursor, epoch_id, 4)
                observation_receipt = adapter.stage_direct_verifier_completion(
                    cursor,
                    epoch_id,
                    4,
                    child_lease,
                    child,
                    verifier_completion,
                    observation,
                    True,
                )
                assert observation_receipt.artifact_stored
                assert observation_receipt.made_effective
                raise _RollbackOuterTransaction
        finally:
            adapter._after_outer_rollback()

    verifier_envelope = _verifier_return_envelope(
        epoch_id=epoch_id,
        job=child,
        lease=child_typed_lease,
        completion=verifier_completion,
        observation=observation,
        make_effective=True,
    )
    verified = adapter.settle_direct_verifier_atomically(
        epoch_id,
        4,
        child_typed_lease,
        verifier_envelope,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert verified.normal is not None
    assert verified.normal.observation_completion is not None
    assert verified.normal.observation_completion.made_effective

    assert connection.execute(
        """
        SELECT base.revision, base.semantic_status, base.evaluation_state,
               runtime.revision, runtime.runtime_state,
               base.open_job_count, base.open_scope_count
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        WHERE base.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone() == (
        5,
        "complete",
        "complete",
        5,
        "semantic_complete",
        0,
        0,
    )
    assert connection.execute(
        """
        SELECT lifecycle_state, default_evaluation_state,
               open_discovery_scope_count, revision
        FROM groundloop_m4_evaluation_epoch_counter
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone() == ("active", "complete", 0, 5)

    base_before_seal = connection.execute(
        """
        SELECT revision, structural_status, semantic_status,
               evaluation_state, publication_mode, sealed_at
        FROM groundloop_epoch WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert base_before_seal is not None
    with pytest.raises(_RollbackOuterTransaction):
        try:
            with connection.transaction(), connection.cursor() as cursor:
                publication = adapter.stage_direct_seal(
                    cursor,
                    epoch_id,
                    5,
                    event.update,
                )
                assert publication.epoch_id == epoch_id
                assert cursor.execute(
                    "SELECT epoch_id FROM groundloop_m4_publication_head "
                    "WHERE singleton"
                ).fetchone() == (database.base.epoch_id,)
                assert cursor.execute(
                    "SELECT epoch_id FROM groundloop_m5_publication_head "
                    "WHERE singleton"
                ).fetchone() == (database.base.epoch_id,)
                assert (
                    cursor.execute(
                        """
                    SELECT revision, structural_status, semantic_status,
                           evaluation_state, publication_mode, sealed_at
                    FROM groundloop_epoch WHERE epoch_id = %s
                    """,
                        (epoch_id,),
                    ).fetchone()
                    == base_before_seal
                )
                assert cursor.execute(
                    "SELECT count(*) FROM groundloop_status_delta WHERE epoch_id = %s",
                    (epoch_id,),
                ).fetchone() == (0,)
                assert cursor.execute(
                    """
                    SELECT lifecycle_state, confirmed_as_of_epoch, revision
                    FROM groundloop_m4_evaluation_epoch_counter
                    WHERE epoch_id = %s
                    """,
                    (epoch_id,),
                ).fetchone() == ("sealed", epoch_id, 6)
                assert cursor.execute(
                    """
                    SELECT status FROM groundloop_published_claim_state
                    WHERE claim_id = %s AND valid_from_epoch = %s
                    """,
                    (database.base.claim_ids[0], epoch_id),
                ).fetchone() == ("supported",)
                raise _RollbackOuterTransaction
        finally:
            adapter._after_outer_rollback()

    assert connection.execute(
        """
        SELECT lifecycle_state, confirmed_as_of_epoch, revision
        FROM groundloop_m4_evaluation_epoch_counter
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone() == ("active", database.base.epoch_id, 5)
    assert connection.execute(
        "SELECT count(*) FROM groundloop_published_claim_state "
        "WHERE valid_from_epoch = %s",
        (epoch_id,),
    ).fetchone() == (0,)
    assert connection.execute(
        "SELECT count(*) FROM groundloop_status_delta WHERE epoch_id = %s",
        (epoch_id,),
    ).fetchone() == (0,)
