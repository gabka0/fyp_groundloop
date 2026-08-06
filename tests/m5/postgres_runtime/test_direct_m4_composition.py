"""Live composition tests for the cursor-local M4-v1 typed bridge."""

from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, replace
from typing import Any

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
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5CandidatePolicyManifest,
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
)
from tests.m5.postgres.helpers import (
    SeededBase,
    install_test_activation_barrier,
    seed_base,
)


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


def _authorize(cursor: Cursor[Any], epoch_id: int, revision: int) -> None:
    cursor.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
        (epoch_id, revision),
    )


def _advance_typed_runtime(
    cursor: Cursor[Any],
    epoch_id: int,
    expected_revision: int,
    runtime_state: str,
) -> None:
    changed = cursor.execute(
        """
        UPDATE groundloop_m5_runtime_epoch
        SET runtime_state = %s, revision = revision + 1
        WHERE epoch_id = %s AND revision = %s
        """,
        (runtime_state, epoch_id, expected_revision),
    ).rowcount
    assert changed == 1
    cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")


def _deterministic_lease_token(job: LogicalJobSpec, ordinal: int = 1) -> str:
    return stable_m4_digest("m4-lease-token-v1", job.job_id, str(ordinal))


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


def _open_direct_epoch(database: DirectRuntimeDatabase) -> _OpenedDirectEpoch:
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
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    adapter._after_outer_commit()
    return _OpenedDirectEpoch(
        event,
        payload,
        withdrawal,
        root,
        scope,
        epoch_id,
        adapter,
    )


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
        _authorize(cursor, opened.epoch_id, 1)
        root_lease = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            1,
            opened.root,
            _deterministic_lease_token(opened.root),
        )
        _advance_typed_runtime(cursor, opened.epoch_id, 1, "semantic_pending")

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
    with database.connection.transaction(), database.connection.cursor() as cursor:
        _authorize(cursor, opened.epoch_id, 2)
        acquisition_adapter.stage_direct_expansion(
            cursor,
            opened.epoch_id,
            2,
            root_lease,
            discovery,
            root_completion,
            (child,),
        )
        _advance_typed_runtime(cursor, opened.epoch_id, 2, "semantic_pending")

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
        _authorize(cursor, opened.epoch_id, 3)
        child_lease = closed_scope_adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            3,
            child,
            _deterministic_lease_token(child),
        )
        _advance_typed_runtime(cursor, opened.epoch_id, 3, "semantic_pending")

    completion, observation = _verifier_completion(child)
    with database.connection.transaction(), database.connection.cursor() as cursor:
        _authorize(cursor, opened.epoch_id, 4)
        receipt = closed_scope_adapter.stage_direct_verifier_completion(
            cursor,
            opened.epoch_id,
            4,
            child_lease,
            child,
            completion,
            observation,
            True,
        )
        assert receipt.artifact_stored
        assert receipt.made_effective
        _advance_typed_runtime(cursor, opened.epoch_id, 4, "semantic_complete")
    closed_scope_adapter._after_outer_commit()

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

    with pytest.raises(_RollbackOuterTransaction):
        try:
            with connection.transaction(), connection.cursor() as cursor:
                (
                    rolled_back_epoch,
                    requirements,
                    active_chunks,
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
                    requirements,
                    active_chunks,
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
    assert connection.execute(
        """
        SELECT count(*) FROM groundloop_m5_requirement_registry_snapshot
        WHERE requirement_registry_snapshot_digest = %s
        """,
        (requirements.requirement_registry_snapshot_digest,),
    ).fetchone() == (0,)
    assert connection.execute(
        """
        SELECT count(*) FROM groundloop_m5_active_chunk_snapshot
        WHERE active_chunk_snapshot_digest = %s
        """,
        (active_chunks.active_chunk_snapshot_digest,),
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
        _authorize(cursor, epoch_id, 1)
        root_lease: JobLease = adapter.acquire_direct_job(
            cursor,
            epoch_id,
            1,
            root,
            _deterministic_lease_token(root),
        )
        assert root_lease.expected_revision == 2
        _advance_typed_runtime(cursor, epoch_id, 1, "semantic_pending")

    discovery, root_completion, child = _complete_expansion(
        epoch_id,
        event,
        root,
        database.base.claim_ids[0],
    )
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
        _advance_typed_runtime(cursor, epoch_id, 2, "semantic_pending")

    with connection.transaction(), connection.cursor() as cursor:
        _authorize(cursor, epoch_id, 3)
        child_lease = adapter.acquire_direct_job(
            cursor,
            epoch_id,
            3,
            child,
            _deterministic_lease_token(child),
        )
        assert child_lease.expected_revision == 4
        _advance_typed_runtime(cursor, epoch_id, 3, "semantic_pending")

    verifier_completion, observation = _verifier_completion(child)
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
        _advance_typed_runtime(cursor, epoch_id, 4, "semantic_complete")
    adapter._after_outer_commit()

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
