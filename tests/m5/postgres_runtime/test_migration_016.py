"""Live PostgreSQL acceptance tests for frozen M5-D24 migration 016."""

from __future__ import annotations

import hashlib
import os
import struct
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import contextmanager
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from threading import Event
from typing import Any

import psycopg
import pytest
from psycopg import Connection, sql
from psycopg.types.json import Jsonb

from groundloop.domain import DecisionPolicy, SubjectKind
from groundloop.m4.contracts import stable_m4_digest
from groundloop.m5.digests import (
    bool_field,
    enum_field,
    f64_field,
    hash_field,
    int_field,
    option_field,
    sequence_field,
    stable_m5_digest,
    text_field,
)
from groundloop.m5.runtime import digests as runtime_digests
from groundloop.m5.runtime.contracts import (
    M5AttemptArchiveReason,
    M5AttemptCompletionReceipt,
    M5AttemptDisposition,
    M5AttemptOutput,
    M5AttemptResultArtifact,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5JobAttempt,
    M5JobCompletion,
    M5JobKind,
    M5JobLease,
    M5JobState,
    M5LogicalJobSpec,
    M5RequirementAdmissionChannel,
    M5RequirementChannelHit,
    M5RequirementDiscoveryResult,
    M5RequirementPairInput,
    M5RequirementScopeSelection,
    M5RequirementVerifierArtifact,
    M5RetrievalTermination,
    M5RootBarrierReceipt,
    M5RuntimeOperationalConfig,
    M5TerminalReason,
    RequirementRegistrySnapshot,
    RequirementRegistrySnapshotEntry,
    SemanticPairKey,
)
from groundloop.m5.runtime.frontier import (
    build_forward_frontier_head,
    build_root_barrier_plan,
    deduplicate_discovery_results,
)
from groundloop.postgres.migrations import (
    M5_ACCEPTED_RUNTIME_BUNDLE_ID,
    M5_ACCEPTED_RUNTIME_BUNDLE_SHA256,
    M5_ACCEPTED_RUNTIME_MIGRATION_SHA256,
    M5_ACCEPTED_RUNTIME_ORACLE_SHA256,
    M5_ACCEPTED_RUNTIME_PREREQUISITE_SHA256,
    M5_RUNTIME_RECOVERY_BUNDLE_ID,
    M5_RUNTIME_RECOVERY_INSTALL_LOCK_RELATIONS,
    M5_RUNTIME_RECOVERY_MIGRATION_PATH,
    M5BundleHashConflictError,
    M5PrerequisiteError,
    M5RuntimeRecoveryBundleError,
    apply_legacy_migrations,
    install_m5_core_bundle,
    install_m5_runtime_bundle,
    install_m5_runtime_recovery_bundle,
    m5_runtime_recovery_bundle_identity,
)
from tests.m5.postgres_runtime import conftest as runtime_support
from tests.m5.postgres_runtime import test_direct_m4_composition as direct_support
from tests.m5.postgres_runtime import test_job_lifecycle as lifecycle_support

RECOVERY_RELATIONS = (
    "groundloop_m5_runtime_operational_config",
    "groundloop_m5_requirement_root_provenance",
    "groundloop_m5_dispatch_record",
    "groundloop_m5_direct_terminal_projection",
    "groundloop_m5_attempt_execution_evidence",
    "groundloop_m5_runtime_work_contribution",
    "groundloop_m5_runtime_work_accumulator",
    "groundloop_m5_runtime_timing_contribution",
    "groundloop_m5_transition_call_timing",
    "groundloop_m5_runtime_timing_accumulator",
    "groundloop_m5_expired_attempt_return",
    "groundloop_m5_typed_direct_late_return_envelope",
    "groundloop_m5_post_terminal_attempt_timing",
    "groundloop_m5_post_terminal_attempt_audit",
    "groundloop_m5_postcommit_invocation_telemetry",
    "groundloop_m5_event_timing_coverage",
)

WORK_COUNTER_COLUMNS = (
    "deactivated_chunk_count",
    "withdrawn_candidate_edge_count",
    "withdrawn_current_observation_count",
    "direct_discovery_call_count",
    "direct_verifier_call_count",
    "direct_observation_artifact_count",
    "direct_effective_observation_count",
    "direct_inactive_completion_count",
    "requirement_forward_retrieval_call_count",
    "requirement_reverse_retrieval_call_count",
    "requirement_fallback_forward_call_count",
    "requirement_verifier_call_count",
    "requirement_observation_artifact_count",
    "requirement_effective_observation_count",
    "requirement_inactive_completion_count",
    "requirement_cancelled_job_count",
    "requirement_late_attempt_artifact_count",
    "requirement_channel_hit_count",
    "requirement_pre_dedup_selection_count",
    "requirement_admitted_pair_count",
    "group_state_write_count",
    "claim_state_write_count",
    "answer_state_write_count",
    "certificate_binding_write_count",
    "public_delta_count",
    "bytes_hashed",
    "bytes_serialized",
    "embedding_model_call_count",
    "verifier_model_call_count",
    "embedding_input_token_count",
    "verifier_input_token_count",
    "verifier_output_token_count",
)

FAILURE_POINTS = (
    "after_additive_attempt_columns",
    "after_recovery_helpers",
    "after_dispatch_schema",
    "after_work_schema",
    "after_timing_schema",
    "after_audit_schema",
    "after_attempt_result_replacements",
    "after_constraints",
    "before_ledger",
    "after_ledger",
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _f64_hex(value: float) -> str:
    return struct.pack(">d", value).hex()


def _f64_from_hex(value: str) -> float:
    return struct.unpack(">d", bytes.fromhex(value))[0]


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


def _select_schema(connection: Connection[Any], schema_name: str) -> None:
    connection.execute(
        sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
    )
    connection.commit()


@contextmanager
def _isolated_015_schema() -> Iterator[Connection[Any]]:
    dsn = _database_url()
    schema_name = f"groundloop_m5_recovery_{uuid.uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
    try:
        with psycopg.connect(dsn) as connection:
            _select_schema(connection, schema_name)
            with connection.transaction():
                apply_legacy_migrations(connection)
            install_m5_core_bundle(connection)
            connection.commit()
            install_m5_runtime_bundle(connection)
            connection.commit()
            yield connection
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )


def _current_relations(connection: Connection[Any]) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            """
            SELECT relation.relname
            FROM pg_class AS relation
            JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = current_schema()
              AND relation.relkind = 'r'
            """
        ).fetchall()
    }


def _ledger_row(
    connection: Connection[Any], bundle_id: str
) -> tuple[object, ...] | None:
    row = connection.execute(
        """
        SELECT bundle_id, bundle_sha256, migration_sha256, oracle_sha256,
               prerequisite_sha256, applied_at
        FROM groundloop_m5_schema_bundle
        WHERE bundle_id = %s
        """,
        (bundle_id,),
    ).fetchone()
    return None if row is None else tuple(row)


def _attempt_result_catalog(
    connection: Connection[Any],
) -> tuple[dict[str, str], tuple[object, ...], str]:
    constraints = {
        str(row[0]): str(row[1])
        for row in connection.execute(
            """
            SELECT conname, pg_get_constraintdef(oid, true)
            FROM pg_constraint
            WHERE conrelid = 'groundloop_m5_attempt_result_artifact'::regclass
            ORDER BY conname
            """
        ).fetchall()
    }
    trigger = connection.execute(
        """
        SELECT trigger_row.oid, trigger_row.tgname, trigger_row.tgenabled,
               trigger_row.tgdeferrable, trigger_row.tginitdeferred,
               function_row.oid, function_row.proname,
               pg_get_triggerdef(trigger_row.oid, true)
        FROM pg_trigger AS trigger_row
        JOIN pg_proc AS function_row ON function_row.oid = trigger_row.tgfoid
        WHERE trigger_row.tgrelid =
              'groundloop_m5_attempt_result_artifact'::regclass
          AND trigger_row.tgname = 'groundloop_m5_attempt_result_shape'
        """
    ).fetchone()
    assert trigger is not None
    function_row = connection.execute(
        """
        SELECT pg_get_functiondef(
            'groundloop_m5_validate_attempt_result_shape()'::regprocedure
        )
        """
    ).fetchone()
    assert function_row is not None
    function_definition = str(function_row[0])
    return constraints, tuple(trigger), function_definition


def _seed_legacy_requirement_attempt(
    connection: Connection[Any], *, acquire: bool = True
) -> int:
    database = _prepare_requirement_runtime_for_recovery_test(connection)
    epoch_id, roots = _legacy_open_overlap_event(
        database, event_id="recovery-install-guard-event"
    )
    if acquire:
        _scope, job = roots[0]
        _acquire_legacy_requirement_job_pre016(
            connection,
            epoch_id=epoch_id,
            expected_revision=1,
            job=job,
        )
    return epoch_id


def _prepare_requirement_runtime_for_recovery_test(
    connection: Connection[Any],
) -> runtime_support.M5RuntimeDatabase:
    with connection.transaction():
        base = runtime_support.seed_base(
            connection,
            prefix="recovery-requirement-kind",
            claim_count=1,
            chunk_texts=("alpha", "beta"),
        )
        group = runtime_support.make_group(
            group_id="recovery-requirement-kind-group-v1",
            family_id="recovery-requirement-kind-family",
            claim_id=base.claim_ids[0],
            texts=("required fact",),
            source_id="recovery-requirement-kind-fixture",
        )
        runtime_support.insert_published_group(
            connection,
            group=group,
            epoch_id=base.epoch_id,
        )
        connection.execute(
            """
            INSERT INTO groundloop_model_artifact (
                model_artifact_id, task, provider, model_id,
                immutable_revision, tokenizer_revision, license_id,
                config_hash
            ) VALUES (
                'failure-replay-embedding', 'embedding', 'fixture',
                'fixture-embedding', 'v1', 'v1', 'MIT', %s
            )
            """,
            (_sha("recovery-kind-embedding-config"),),
        )
    with connection.transaction():
        runtime_support.install_test_activation_barrier(
            connection,
            base,
            activation_id="recovery-requirement-kind-activation",
        )
    manifest = runtime_support._manifest(base)
    runtime_support.PostgresM5RuntimeStore(connection).register_candidate_policy(
        manifest
    )
    requirement_snapshot = runtime_support.RequirementRegistrySnapshot.build(
        (
            runtime_support.RequirementRegistrySnapshotEntry.build(
                requirement_version_id=group.requirements[0].requirement_version_id,
                group_version_id=group.group_version_id,
                group_family_id=group.group_family_id,
                owner_claim_id=group.owner_claim_id,
                requirement_text=group.requirements[0].requirement_text,
            ),
        )
    )
    chunk_snapshot = runtime_support.ActiveChunkSnapshot.build(
        tuple(
            runtime_support.ActiveChunkSnapshotEntry.build(
                chunk_version_id=chunk_id,
                chunk_text=chunk_text,
            )
            for chunk_id, chunk_text in zip(
                base.chunk_ids,
                ("alpha", "beta"),
                strict=True,
            )
        )
    )
    return runtime_support.M5RuntimeDatabase(
        dsn="",
        schema_name="",
        connection=connection,
        base=base,
        group=group,
        manifest=manifest,
        requirement_snapshot=requirement_snapshot,
        chunk_snapshot=chunk_snapshot,
        operational_config=M5RuntimeOperationalConfig.build(3_600_000),
    )


def _legacy_root_set_hash(
    roots: tuple[tuple[M5DiscoveryScopeContract, M5LogicalJobSpec], ...],
) -> str:
    return runtime_digests.requirement_root_set_digest(
        job.logical_job_id for _scope, job in roots
    )


def _legacy_open_overlap_event(
    database: runtime_support.M5RuntimeDatabase,
    *,
    event_id: str = "recovery-root-overlap-event",
) -> tuple[int, tuple[tuple[M5DiscoveryScopeContract, M5LogicalJobSpec], ...]]:
    """Create a migration-015 root topology without D24 accounting rows."""

    requirement = database.group.requirements[0]
    requirement_snapshot = RequirementRegistrySnapshot.build(
        (
            RequirementRegistrySnapshotEntry.build(
                requirement_version_id=requirement.requirement_version_id,
                group_version_id=database.group.group_version_id,
                group_family_id=database.group.group_family_id,
                owner_claim_id=database.group.owner_claim_id,
                requirement_text=requirement.requirement_text,
            ),
        )
    )
    forward_scope = M5DiscoveryScopeContract.build(
        direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
        requirement_version_id=requirement.requirement_version_id,
        inserted_chunk_version_id=None,
        candidate_policy_id=database.manifest.candidate_policy_id,
        requirement_registry_snapshot_digest=(
            requirement_snapshot.requirement_registry_snapshot_digest
        ),
        active_chunk_snapshot_digest=(
            database.chunk_snapshot.active_chunk_snapshot_digest
        ),
    )
    reverse_scope = M5DiscoveryScopeContract.build(
        direction=M5DiscoveryDirection.REVERSE_CHUNK,
        requirement_version_id=None,
        inserted_chunk_version_id=database.base.chunk_ids[0],
        candidate_policy_id=database.manifest.candidate_policy_id,
        requirement_registry_snapshot_digest=(
            requirement_snapshot.requirement_registry_snapshot_digest
        ),
        active_chunk_snapshot_digest=(
            database.chunk_snapshot.active_chunk_snapshot_digest
        ),
    )
    roots = tuple(
        sorted(
            (
                (
                    forward_scope,
                    M5LogicalJobSpec.build(
                        structural_event_id=event_id,
                        job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
                        manifest=database.manifest,
                        scope=forward_scope,
                    ),
                ),
                (
                    reverse_scope,
                    M5LogicalJobSpec.build(
                        structural_event_id=event_id,
                        job_kind=M5JobKind.REVERSE_REQUIREMENT_DISCOVERY,
                        manifest=database.manifest,
                        scope=reverse_scope,
                    ),
                ),
            ),
            key=lambda item: item[1].logical_job_id,
        )
    )
    connection = database.connection
    with connection.transaction():
        row = connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (%s, %s, 1, 'committed', 'pending', 'pending',
                      'provisional', NULL)
            RETURNING epoch_id
            """,
            (event_id, _sha(f"{event_id}:payload")),
        ).fetchone()
        assert row is not None
        epoch_id = int(row[0])
        connection.execute(
            """
            INSERT INTO groundloop_m5_update (
                epoch_id, update_kind, previous_published_epoch_id,
                decision_policy_version, manifest
            ) VALUES (%s, 'observe_requirement', %s, %s, '{}'::jsonb)
            """,
            (
                epoch_id,
                database.base.epoch_id,
                database.manifest.decision_policy_version,
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_runtime_epoch (
                epoch_id, structural_event_id, candidate_policy_id,
                candidate_policy_manifest_hash,
                requirement_registry_snapshot_digest,
                active_chunk_snapshot_digest,
                expected_previous_published_epoch_id,
                requirement_root_set_hash, runtime_state, revision,
                open_work_count, open_scope_count, blocking_failure_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                      'structural_committed', 1, 2, 2, 0)
            """,
            (
                epoch_id,
                event_id,
                database.manifest.candidate_policy_id,
                database.manifest.manifest_hash,
                requirement_snapshot.requirement_registry_snapshot_digest,
                database.chunk_snapshot.active_chunk_snapshot_digest,
                database.base.epoch_id,
                _legacy_root_set_hash(roots),
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_requirement_registry_snapshot (
                requirement_registry_snapshot_digest, requirement_count,
                created_epoch_id
            ) VALUES (%s, 1, %s)
            """,
            (
                requirement_snapshot.requirement_registry_snapshot_digest,
                epoch_id,
            ),
        )
        entry = requirement_snapshot.entries[0]
        connection.execute(
            """
            INSERT INTO groundloop_m5_requirement_registry_snapshot_member (
                requirement_registry_snapshot_digest, member_ordinal,
                requirement_version_id, group_version_id, group_family_id,
                owner_claim_id, normalized_requirement_text,
                requirement_text_hash
            ) VALUES (%s, 0, %s, %s, %s, %s, %s, %s)
            """,
            (
                requirement_snapshot.requirement_registry_snapshot_digest,
                entry.requirement_version_id,
                entry.group_version_id,
                entry.group_family_id,
                entry.owner_claim_id,
                entry.normalized_requirement_text,
                entry.requirement_text_hash,
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_active_chunk_snapshot (
                active_chunk_snapshot_digest, chunk_count, created_epoch_id,
                normalizer_id, normalizer_provenance_hash
            ) VALUES (%s, %s, %s, 'm5-normalize-text-v1', %s)
            ON CONFLICT (active_chunk_snapshot_digest) DO NOTHING
            """,
            (
                database.chunk_snapshot.active_chunk_snapshot_digest,
                database.chunk_snapshot.chunk_count,
                epoch_id,
                "d91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb",
            ),
        )
        for ordinal, chunk in enumerate(database.chunk_snapshot.entries):
            connection.execute(
                """
                INSERT INTO groundloop_m5_active_chunk_snapshot_member (
                    active_chunk_snapshot_digest, member_ordinal,
                    chunk_version_id, text_hash
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (active_chunk_snapshot_digest, chunk_version_id)
                DO NOTHING
                """,
                (
                    database.chunk_snapshot.active_chunk_snapshot_digest,
                    ordinal,
                    chunk.chunk_version_id,
                    chunk.text_hash,
                ),
            )
        for scope, job in roots:
            connection.execute(
                """
                INSERT INTO groundloop_m5_discovery_scope (
                    root_job_id, epoch_id, direction,
                    requirement_version_id, inserted_chunk_version_id,
                    candidate_policy_id,
                    requirement_registry_snapshot_digest,
                    active_chunk_snapshot_digest, scope_contract_digest,
                    scope_state, created_revision
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'open', 1)
                """,
                (
                    job.logical_job_id,
                    epoch_id,
                    scope.direction.value,
                    scope.requirement_version_id,
                    scope.inserted_chunk_version_id,
                    scope.candidate_policy_id,
                    scope.requirement_registry_snapshot_digest,
                    scope.active_chunk_snapshot_digest,
                    scope.scope_contract_digest,
                ),
            )
            connection.execute(
                """
                INSERT INTO groundloop_m5_semantic_job (
                    logical_job_id, epoch_id, structural_event_id, job_kind,
                    candidate_policy_id, candidate_policy_manifest_hash,
                    scope_contract_digest,
                    requirement_registry_snapshot_digest,
                    active_chunk_snapshot_digest, role_template_hash,
                    execution_spec_hash, expandable, payload_hash,
                    job_state, created_revision
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, TRUE, %s, 'declared', 1)
                """,
                (
                    job.logical_job_id,
                    epoch_id,
                    job.structural_event_id,
                    job.job_kind.value,
                    job.candidate_policy_id,
                    job.candidate_policy_manifest_hash,
                    job.scope_contract_digest,
                    job.requirement_registry_snapshot_digest,
                    job.active_chunk_snapshot_digest,
                    job.role_template_hash,
                    job.execution_spec_hash,
                    job.payload_hash,
                ),
            )
        connection.execute(
            """
            INSERT INTO groundloop_m5_owner_pending_counter (
                epoch_id, owner_claim_id, broad_reverse_scope_count,
                forward_scope_count, verifier_job_count,
                blocking_failure_count, updated_revision
            ) VALUES (%s, %s, 1, 1, 0, 0, 1)
            """,
            (epoch_id, database.group.owner_claim_id),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_answer_pending_counter (
                epoch_id, answer_version_id, broad_reverse_scope_count,
                forward_scope_count, verifier_job_count,
                blocking_failure_count, updated_revision
            ) VALUES (%s, %s, 1, 1, 0, 0, 1)
            """,
            (epoch_id, database.base.answer_id),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.execute("SET CONSTRAINTS ALL DEFERRED")
    return epoch_id, roots


def _legacy_requirement_result(
    *,
    epoch_id: int,
    root: M5LogicalJobSpec,
    pairs: tuple[SemanticPairKey, ...],
) -> M5RequirementDiscoveryResult:
    hits = tuple(
        M5RequirementChannelHit.build(
            epoch_id=epoch_id,
            root_job_id=root.logical_job_id,
            scope_contract_digest=root.scope_contract_digest or "",
            pair=pair,
            candidate_policy_id=root.candidate_policy_id,
            channel=M5RequirementAdmissionChannel.VECTOR,
            rank=rank,
            score=1.0 - rank / 10.0,
            channel_artifact_hash=_sha(
                f"hit:{root.logical_job_id}:{pair.semantic_pair_digest}"
            ),
        )
        for rank, pair in enumerate(pairs, start=1)
    )
    selections = tuple(
        M5RequirementScopeSelection.build(
            root_job_id=root.logical_job_id,
            scope_contract_digest=root.scope_contract_digest or "",
            pair=pair,
            fused_rank=rank,
            reasons=(M5RequirementAdmissionChannel.VECTOR,),
        )
        for rank, pair in enumerate(pairs, start=1)
    )
    return M5RequirementDiscoveryResult.build(
        root_job_id=root.logical_job_id,
        scope_contract_digest=root.scope_contract_digest or "",
        termination=M5RetrievalTermination.SNAPSHOT_EXHAUSTED,
        channel_hits=hits,
        selections=selections,
    )


def _acquire_legacy_requirement_job_pre016(
    connection: Connection[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    job: M5LogicalJobSpec,
) -> M5JobLease:
    attempt = M5JobAttempt.build(
        logical_job_id=job.logical_job_id,
        attempt_ordinal=1,
        execution_spec_hash=job.execution_spec_hash,
        lease_token_hash=_sha(f"legacy-lease:{job.logical_job_id}:1"),
    )
    resulting_revision = expected_revision + 1
    with connection.transaction():
        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (epoch_id, expected_revision),
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_semantic_job
            SET job_state = 'running'
            WHERE epoch_id = %s AND logical_job_id = %s
              AND job_state = 'declared'
            """,
                (epoch_id, job.logical_job_id),
            ).rowcount
            == 1
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_job_attempt (
                attempt_id, logical_job_id, attempt_ordinal,
                execution_spec_hash, lease_token_hash, attempt_state
            ) VALUES (%s, %s, 1, %s, %s, 'dispatched')
            """,
            (
                attempt.attempt_id,
                attempt.logical_job_id,
                attempt.execution_spec_hash,
                attempt.lease_token_hash,
            ),
        )
        for relation in (
            "groundloop_m5_owner_pending_counter",
            "groundloop_m5_answer_pending_counter",
        ):
            connection.execute(
                sql.SQL(
                    "UPDATE {} SET updated_revision = %s WHERE epoch_id = %s"
                ).format(sql.Identifier(relation)),
                (resulting_revision, epoch_id),
            )
        connection.execute(
            "UPDATE groundloop_epoch SET revision = %s WHERE epoch_id = %s",
            (resulting_revision, epoch_id),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_runtime_epoch
            SET revision = %s, runtime_state = 'semantic_pending'
            WHERE epoch_id = %s
            """,
            (resulting_revision, epoch_id),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.execute("SET CONSTRAINTS ALL DEFERRED")
    return M5JobLease(job.logical_job_id, attempt, resulting_revision, True, False)


def _stage_legacy_requirement_root(
    connection: Connection[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    lease: M5JobLease,
    job: M5LogicalJobSpec,
    result: M5RequirementDiscoveryResult,
) -> tuple[M5AttemptCompletionReceipt, M5AttemptOutput]:
    """Install legacy root-result rows without D24 work contributions."""

    assert lease.attempt is not None
    attempt = lease.attempt
    output = M5AttemptOutput.build(
        attempt=attempt,
        job_epoch_id=epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=result.result_artifact_id,
        result_artifact_hash=result.result_artifact_hash,
    )
    if job.job_kind is M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL:
        chunk_active = None
        requirement_active = True
        group_active = True
    else:
        assert job.job_kind is M5JobKind.REVERSE_REQUIREMENT_DISCOVERY
        chunk_active = True
        requirement_active = None
        group_active = None
    artifact = M5AttemptResultArtifact.build(
        attempt_output=output,
        job_state_at_receipt=M5JobState.RUNNING,
        job_state_after=M5JobState.RUNNING,
        disposition=M5AttemptDisposition.ROOT_RESULT_STAGED,
        activity_snapshot_epoch_id=epoch_id,
        activity_snapshot_revision=expected_revision,
        epoch_active=True,
        chunk_active=chunk_active,
        requirement_active=requirement_active,
        group_active=group_active,
        archive_reason=None,
    )
    resulting_revision = expected_revision + 1
    with connection.transaction():
        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (epoch_id, expected_revision),
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_job_attempt
            SET attempt_state = 'result_reserved', attempt_output_digest = %s
            WHERE attempt_id = %s AND logical_job_id = %s
              AND attempt_state = 'dispatched' AND attempt_output_digest IS NULL
            """,
                (output.attempt_output_digest, attempt.attempt_id, job.logical_job_id),
            ).rowcount
            == 1
        )
        connection.execute(
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
                %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, NULL, NULL, NULL
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
            ),
        )
        for hit in result.channel_hits:
            connection.execute(
                """
                INSERT INTO groundloop_m5_requirement_channel_hit (
                    hit_digest, epoch_id, root_job_id, scope_contract_digest,
                    subject_kind, subject_id, chunk_version_id,
                    semantic_pair_digest, candidate_policy_id, channel, rank,
                    score, channel_artifact_hash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    hit.hit_digest,
                    hit.epoch_id,
                    hit.root_job_id,
                    hit.scope_contract_digest,
                    hit.pair.subject_kind.value,
                    hit.pair.subject_id,
                    hit.pair.chunk_version_id,
                    hit.semantic_pair_digest,
                    hit.candidate_policy_id,
                    hit.channel.value,
                    hit.rank,
                    hit.score,
                    hit.channel_artifact_hash,
                ),
            )
        for selection in result.selections:
            connection.execute(
                """
                INSERT INTO groundloop_m5_requirement_scope_selection (
                    selection_digest, root_job_id, scope_contract_digest,
                    subject_kind, subject_id, chunk_version_id,
                    semantic_pair_digest, fused_rank, reasons, mandatory_lineage
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    selection.selection_digest,
                    selection.root_job_id,
                    selection.scope_contract_digest,
                    selection.pair.subject_kind.value,
                    selection.pair.subject_id,
                    selection.pair.chunk_version_id,
                    selection.semantic_pair_digest,
                    selection.fused_rank,
                    [reason.value for reason in selection.reasons],
                    selection.mandatory_lineage,
                ),
            )
        connection.execute(
            """
            INSERT INTO groundloop_m5_requirement_discovery_result (
                result_artifact_id, result_artifact_hash, root_job_id,
                scope_contract_digest, termination, channel_hit_count,
                selection_count, approximate_selection_count,
                mandatory_lineage_only_count, staged_epoch_id, staged_revision
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                result.result_artifact_id,
                result.result_artifact_hash,
                result.root_job_id,
                result.scope_contract_digest,
                result.termination.value,
                len(result.channel_hits),
                len(result.selections),
                result.approximate_selection_count,
                result.mandatory_lineage_only_count,
                epoch_id,
                resulting_revision,
            ),
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_discovery_scope
            SET scope_state = 'result_staged', staged_result_artifact_hash = %s,
                staged_revision = %s
            WHERE epoch_id = %s AND root_job_id = %s AND scope_state = 'open'
            """,
                (
                    result.result_artifact_hash,
                    resulting_revision,
                    epoch_id,
                    job.logical_job_id,
                ),
            ).rowcount
            == 1
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_job_attempt
            SET attempt_state = 'completed', finished_at = now()
            WHERE attempt_id = %s AND logical_job_id = %s
              AND attempt_state = 'result_reserved'
              AND attempt_output_digest = %s
            """,
                (attempt.attempt_id, job.logical_job_id, output.attempt_output_digest),
            ).rowcount
            == 1
        )
        for relation in (
            "groundloop_m5_owner_pending_counter",
            "groundloop_m5_answer_pending_counter",
        ):
            connection.execute(
                sql.SQL(
                    "UPDATE {} SET updated_revision = %s WHERE epoch_id = %s"
                ).format(sql.Identifier(relation)),
                (resulting_revision, epoch_id),
            )
        connection.execute(
            "UPDATE groundloop_epoch SET revision = %s WHERE epoch_id = %s",
            (resulting_revision, epoch_id),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_runtime_epoch
            SET revision = %s, runtime_state = 'semantic_pending'
            WHERE epoch_id = %s
            """,
            (resulting_revision, epoch_id),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.execute("SET CONSTRAINTS ALL DEFERRED")
    return (
        M5AttemptCompletionReceipt(
            logical_job_id=job.logical_job_id,
            attempt_id=attempt.attempt_id,
            resulting_revision=resulting_revision,
            exact_replay=False,
        ),
        output,
    )


def _close_legacy_requirement_roots(
    connection: Connection[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    manifest: Any,
    roots: tuple[tuple[M5DiscoveryScopeContract, M5LogicalJobSpec], ...],
    results: tuple[M5RequirementDiscoveryResult, ...],
) -> M5RootBarrierReceipt:
    """Install the migration-015 barrier image without D24 contributions."""

    structural_event_id = roots[0][1].structural_event_id
    candidate_policy_id = roots[0][1].candidate_policy_id
    deduplication = deduplicate_discovery_results(
        epoch_id=epoch_id,
        candidate_policy_id=candidate_policy_id,
        results=results,
        inactive_root_job_ids=(),
    )
    scope_by_root = {job.logical_job_id: scope for scope, job in roots}
    child_jobs = tuple(
        sorted(
            (
                M5LogicalJobSpec.build(
                    structural_event_id=structural_event_id,
                    job_kind=M5JobKind.VERIFY_REQUIREMENT_PAIR,
                    manifest=manifest,
                    scope=scope_by_root[pair.owner_root_job_id],
                    parent_job_id=pair.owner_root_job_id,
                    pair=pair.pair,
                )
                for pair in deduplication.admitted_pairs
            ),
            key=lambda child: child.logical_job_id,
        )
    )
    plan = build_root_barrier_plan(
        structural_event_id=structural_event_id,
        results=results,
        deduplication=deduplication,
        child_jobs=child_jobs,
    )
    closure_by_root = {closure.root_job_id: closure for closure in plan.root_closures}
    result_by_root = {result.root_job_id: result for result in results}
    completion_by_root = {
        job.logical_job_id: M5JobCompletion.build(
            job=job,
            terminal_state=M5JobState.COMPLETED_ACTIVE,
            result_artifact_id=result_by_root[job.logical_job_id].result_artifact_id,
            result_artifact_hash=result_by_root[
                job.logical_job_id
            ].result_artifact_hash,
            scope_closure_digest=closure_by_root[
                job.logical_job_id
            ].scope_closure_digest,
            child_set_hash=closure_by_root[job.logical_job_id].child_set_hash,
            archive_reason=None,
        )
        for _scope, job in roots
    }
    resulting_revision = expected_revision + 1
    with connection.transaction():
        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (epoch_id, expected_revision),
        )
        for admitted in plan.admitted_pairs:
            connection.execute(
                """
                INSERT INTO groundloop_m5_requirement_admitted_pair (
                    admitted_pair_digest, epoch_id, subject_kind, subject_id,
                    chunk_version_id, semantic_pair_digest, candidate_policy_id,
                    owner_root_job_id, reasons, mandatory_lineage
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    admitted.admitted_pair_digest,
                    admitted.epoch_id,
                    admitted.pair.subject_kind.value,
                    admitted.pair.subject_id,
                    admitted.pair.chunk_version_id,
                    admitted.semantic_pair_digest,
                    admitted.candidate_policy_id,
                    admitted.owner_root_job_id,
                    [reason.value for reason in admitted.reasons],
                    admitted.mandatory_lineage,
                ),
            )
            for source in admitted.sources:
                connection.execute(
                    """
                    INSERT INTO groundloop_m5_requirement_admitted_pair_source (
                        admitted_pair_digest, root_job_id,
                        scope_contract_digest, selection_digest
                    ) VALUES (%s, %s, %s, %s)
                    """,
                    (
                        admitted.admitted_pair_digest,
                        source.root_job_id,
                        source.scope_contract_digest,
                        source.selection_digest,
                    ),
                )
        admitted_by_pair = {
            admitted.semantic_pair_digest: admitted for admitted in plan.admitted_pairs
        }
        for child in child_jobs:
            assert child.pair is not None
            assert child.semantic_pair_digest is not None
            admitted = admitted_by_pair[child.semantic_pair_digest]
            connection.execute(
                """
                INSERT INTO groundloop_m5_semantic_job (
                    logical_job_id, epoch_id, structural_event_id, job_kind,
                    candidate_policy_id, candidate_policy_manifest_hash,
                    parent_job_id, subject_kind, subject_id, chunk_version_id,
                    semantic_pair_digest, admitted_pair_digest,
                    scope_contract_digest, requirement_registry_snapshot_digest,
                    active_chunk_snapshot_digest, role_template_hash,
                    execution_spec_hash, expandable, payload_hash, job_state,
                    created_revision
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, FALSE, %s, 'declared', %s
                )
                """,
                (
                    child.logical_job_id,
                    epoch_id,
                    child.structural_event_id,
                    child.job_kind.value,
                    child.candidate_policy_id,
                    child.candidate_policy_manifest_hash,
                    child.parent_job_id,
                    child.pair.subject_kind.value,
                    child.pair.subject_id,
                    child.pair.chunk_version_id,
                    child.semantic_pair_digest,
                    admitted.admitted_pair_digest,
                    child.scope_contract_digest,
                    child.requirement_registry_snapshot_digest,
                    child.active_chunk_snapshot_digest,
                    child.role_template_hash,
                    child.execution_spec_hash,
                    child.payload_hash,
                    resulting_revision,
                ),
            )
            connection.execute(
                """
                INSERT INTO groundloop_m5_job_dependency (
                    epoch_id, parent_job_id, child_job_id
                ) VALUES (%s, %s, %s)
                """,
                (epoch_id, child.parent_job_id, child.logical_job_id),
            )
        for scope, job in roots:
            completion = completion_by_root[job.logical_job_id]
            assert (
                connection.execute(
                    """
                UPDATE groundloop_m5_semantic_job
                SET job_state = 'completed_active', result_artifact_id = %s,
                    result_artifact_hash = %s, scope_closure_digest = %s,
                    child_set_hash = %s, archive_reason = NULL,
                    completion_digest = %s, completed_revision = %s,
                    completed_at = now()
                WHERE epoch_id = %s AND logical_job_id = %s
                  AND job_state = 'running'
                """,
                    (
                        completion.result_artifact_id,
                        completion.result_artifact_hash,
                        completion.scope_closure_digest,
                        completion.child_set_hash,
                        completion.completion_digest,
                        resulting_revision,
                        epoch_id,
                        job.logical_job_id,
                    ),
                ).rowcount
                == 1
            )
            assert (
                connection.execute(
                    """
                UPDATE groundloop_m5_discovery_scope
                SET scope_state = 'closed_active', scope_closure_digest = %s,
                    child_set_hash = %s, completion_digest = %s,
                    closed_revision = %s, closed_at = now()
                WHERE epoch_id = %s AND root_job_id = %s
                  AND scope_state = 'result_staged'
                """,
                    (
                        completion.scope_closure_digest,
                        completion.child_set_hash,
                        completion.completion_digest,
                        resulting_revision,
                        epoch_id,
                        job.logical_job_id,
                    ),
                ).rowcount
                == 1
            )
            if scope.direction is M5DiscoveryDirection.FORWARD_REQUIREMENT:
                assert scope.requirement_version_id is not None
                head = build_forward_frontier_head(
                    job=job,
                    scope=scope,
                    discovery_result=result_by_root[job.logical_job_id],
                    completion=completion,
                    completed_epoch_id=epoch_id,
                    completed_revision=resulting_revision,
                )
                connection.execute(
                    """
                    INSERT INTO groundloop_m5_requirement_frontier_head (
                        requirement_version_id, candidate_policy_id,
                        latest_root_job_id, latest_scope_contract_digest,
                        latest_active_chunk_snapshot_digest,
                        latest_discovery_result_artifact_hash,
                        latest_scope_closure_digest, latest_completion_digest,
                        completed_epoch_id, completed_revision
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        head.requirement_version_id,
                        head.candidate_policy_id,
                        head.latest_root_job_id,
                        head.latest_scope_contract_digest,
                        head.latest_active_chunk_snapshot_digest,
                        head.latest_discovery_result_artifact_hash,
                        head.latest_scope_closure_digest,
                        head.latest_completion_digest,
                        head.completed_epoch_id,
                        head.completed_revision,
                    ),
                )
        for relation in (
            "groundloop_m5_owner_pending_counter",
            "groundloop_m5_answer_pending_counter",
        ):
            connection.execute(
                sql.SQL(
                    "UPDATE {} SET broad_reverse_scope_count = 0, "
                    "forward_scope_count = 0, verifier_job_count = %s, "
                    "updated_revision = %s WHERE epoch_id = %s"
                ).format(sql.Identifier(relation)),
                (len(child_jobs), resulting_revision, epoch_id),
            )
        connection.execute(
            "UPDATE groundloop_epoch SET revision = %s WHERE epoch_id = %s",
            (resulting_revision, epoch_id),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_runtime_epoch
            SET revision = %s, runtime_state = 'semantic_pending',
                open_work_count = %s, open_scope_count = 0
            WHERE epoch_id = %s
            """,
            (resulting_revision, len(child_jobs), epoch_id),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.execute("SET CONSTRAINTS ALL DEFERRED")
    return M5RootBarrierReceipt(
        requirement_root_set_hash=plan.requirement_root_set_hash,
        barrier_completion_hash=plan.barrier_completion_hash,
        resulting_revision=resulting_revision,
        exact_replay=False,
    )


def _open_requirement_event_after_016(
    connection: Connection[Any],
    *,
    event_id: str,
) -> tuple[
    runtime_support.M5RuntimeDatabase,
    int,
    tuple[M5LogicalJobSpec, ...],
]:
    database = _prepare_requirement_runtime_for_recovery_test(connection)
    plan = database.register_plan(event_id=event_id)
    jobs = lifecycle_support._root_jobs(plan, database.manifest)
    opened = runtime_support.PostgresM5RuntimeStore(
        connection
    ).open_typed_event_atomically(
        plan,
        recovery_operational_config=database.operational_config,
        recovery_root_fallback_required={job.logical_job_id: False for job in jobs},
    )
    return database, opened.epoch_id, jobs


def _acquire_requirement_job_for_recovery_test(
    connection: Connection[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    job: Any,
) -> tuple[dict[str, Any], M5JobLease]:
    attempt = M5JobAttempt.build(
        logical_job_id=job.logical_job_id,
        attempt_ordinal=1,
        execution_spec_hash=job.execution_spec_hash,
        lease_token_hash=_sha(f"recovery-kind-lease:{job.logical_job_id}:1"),
    )
    config_digest = stable_m5_digest(
        "m5-runtime-operational-config-v1", int_field(3_600_000)
    )
    provenance_digest = stable_m5_digest(
        "m5-requirement-root-provenance-v1",
        int_field(epoch_id),
        text_field(job.logical_job_id),
        bool_field(False),
    )
    resulting_revision = expected_revision + 1
    with connection.transaction():
        connection.execute(
            """
            INSERT INTO groundloop_m5_runtime_operational_config (
                epoch_id, lease_duration_ms, config_digest
            ) VALUES (%s, 3600000, %s)
            ON CONFLICT (epoch_id) DO NOTHING
            """,
            (epoch_id, config_digest),
        )
        if job.job_kind.value == "forward_requirement_retrieval":
            connection.execute(
                """
                INSERT INTO groundloop_m5_requirement_root_provenance (
                    epoch_id, root_job_id, fallback_required, provenance_digest
                ) VALUES (%s, %s, false, %s)
                ON CONFLICT (epoch_id, root_job_id) DO NOTHING
                """,
                (epoch_id, job.logical_job_id, provenance_digest),
            )
        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (epoch_id, expected_revision),
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_semantic_job
            SET job_state = 'running'
            WHERE epoch_id = %s AND logical_job_id = %s
              AND job_state = 'declared'
            """,
                (epoch_id, job.logical_job_id),
            ).rowcount
            == 1
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_job_attempt (
                attempt_id, logical_job_id, attempt_ordinal,
                execution_spec_hash, lease_token_hash, attempt_state,
                lease_expires_at
            ) VALUES (%s, %s, 1, %s, %s, 'dispatched',
                      clock_timestamp() + interval '1 hour')
            """,
            (
                attempt.attempt_id,
                attempt.logical_job_id,
                attempt.execution_spec_hash,
                attempt.lease_token_hash,
            ),
        )
        connection.execute(
            "UPDATE groundloop_m5_owner_pending_counter "
            "SET updated_revision = %s WHERE epoch_id = %s",
            (resulting_revision, epoch_id),
        )
        connection.execute(
            "UPDATE groundloop_m5_answer_pending_counter "
            "SET updated_revision = %s WHERE epoch_id = %s",
            (resulting_revision, epoch_id),
        )
        assert (
            connection.execute(
                "UPDATE groundloop_epoch SET revision = %s "
                "WHERE epoch_id = %s AND revision = %s",
                (resulting_revision, epoch_id, expected_revision),
            ).rowcount
            == 1
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_runtime_epoch
            SET revision = %s, runtime_state = 'semantic_pending'
            WHERE epoch_id = %s AND revision = %s
            """,
                (resulting_revision, epoch_id, expected_revision),
            ).rowcount
            == 1
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    lease_row = connection.execute(
        "SELECT lease_expires_at FROM groundloop_m5_job_attempt WHERE attempt_id = %s",
        (attempt.attempt_id,),
    ).fetchone()
    assert lease_row is not None
    connection.commit()
    row = {
        "epoch_id": epoch_id,
        "logical_job_id": job.logical_job_id,
        "job_kind": job.job_kind.value,
        "attempt": attempt,
        "lease_expires_at": lease_row[0],
        "dispatched_revision": resulting_revision,
    }
    return row, M5JobLease(
        job.logical_job_id,
        attempt,
        resulting_revision,
        True,
        False,
    )


def _seed_requirement_job_kind_for_recovery_test(
    connection: Connection[Any], *, job_kind: str
) -> dict[str, Any]:
    assert job_kind in {
        "forward_requirement_retrieval",
        "reverse_requirement_discovery",
        "verify_requirement_pair",
    }
    if job_kind == "forward_requirement_retrieval":
        _database, epoch_id, jobs = _open_requirement_event_after_016(
            connection,
            event_id="recovery-kind-forward",
        )
        job = jobs[0]
        row, _ = _acquire_requirement_job_for_recovery_test(
            connection,
            epoch_id=epoch_id,
            expected_revision=1,
            job=job,
        )
        return row

    database = _prepare_requirement_runtime_for_recovery_test(connection)
    epoch_id, roots = _legacy_open_overlap_event(database)
    if job_kind == "reverse_requirement_discovery":
        job = next(
            job
            for _scope, job in roots
            if job.job_kind.value == "reverse_requirement_discovery"
        )
        row, _ = _acquire_requirement_job_for_recovery_test(
            connection,
            epoch_id=epoch_id,
            expected_revision=1,
            job=job,
        )
        return row

    pair = SemanticPairKey(
        SubjectKind.REQUIREMENT,
        database.group.requirements[0].requirement_version_id,
        database.base.chunk_ids[0],
    )
    revision = 1
    results = []
    for _scope, root_job in roots:
        _, lease = _acquire_requirement_job_for_recovery_test(
            connection,
            epoch_id=epoch_id,
            expected_revision=revision,
            job=root_job,
        )
        revision += 1
        result = _legacy_requirement_result(
            epoch_id=epoch_id,
            root=root_job,
            pairs=(pair,),
        )
        staged, _ = _stage_legacy_requirement_root(
            connection,
            epoch_id=epoch_id,
            expected_revision=revision,
            lease=lease,
            job=root_job,
            result=result,
        )
        results.append(result)
        revision = int(staged.resulting_revision)
    barrier = _close_legacy_requirement_roots(
        connection,
        epoch_id=epoch_id,
        expected_revision=revision,
        manifest=database.manifest,
        roots=roots,
        results=tuple(results),
    )
    revision = int(barrier.resulting_revision)
    verifier_jobs = runtime_support.PostgresM5RuntimeStore(connection).verifier_jobs(
        epoch_id
    )
    assert len(verifier_jobs) == 1
    row, _ = _acquire_requirement_job_for_recovery_test(
        connection,
        epoch_id=epoch_id,
        expected_revision=revision,
        job=verifier_jobs[0],
    )
    return row


def _close_requirement_roots_for_recovery_test(
    connection: Connection[Any],
) -> dict[str, Any]:
    database = _prepare_requirement_runtime_for_recovery_test(connection)
    epoch_id, roots = _legacy_open_overlap_event(database)
    pair = SemanticPairKey(
        SubjectKind.REQUIREMENT,
        database.group.requirements[0].requirement_version_id,
        database.base.chunk_ids[0],
    )
    revision = 1
    results = []
    for _scope, root_job in roots:
        _, lease = _acquire_requirement_job_for_recovery_test(
            connection,
            epoch_id=epoch_id,
            expected_revision=revision,
            job=root_job,
        )
        revision += 1
        result = _legacy_requirement_result(
            epoch_id=epoch_id,
            root=root_job,
            pairs=(pair,),
        )
        staged, _ = _stage_legacy_requirement_root(
            connection,
            epoch_id=epoch_id,
            expected_revision=revision,
            lease=lease,
            job=root_job,
            result=result,
        )
        results.append(result)
        revision = int(staged.resulting_revision)
    barrier = _close_legacy_requirement_roots(
        connection,
        epoch_id=epoch_id,
        expected_revision=revision,
        manifest=database.manifest,
        roots=roots,
        results=tuple(results),
    )
    event = connection.execute(
        "SELECT structural_event_id FROM groundloop_m5_runtime_epoch "
        "WHERE epoch_id = %s",
        (epoch_id,),
    ).fetchone()
    assert event is not None
    connection.commit()
    return {
        "database": database,
        "epoch_id": epoch_id,
        "roots": roots,
        "barrier": barrier,
        "revision": int(barrier.resulting_revision),
        "structural_event_id": str(event[0]),
        "admitted_pair_count": 1,
    }


def _stage_requirement_root_for_recovery_test(
    connection: Connection[Any],
) -> dict[str, Any]:
    database = _prepare_requirement_runtime_for_recovery_test(connection)
    epoch_id, roots = _legacy_open_overlap_event(database)
    _scope, root = roots[0]
    _, lease = _acquire_requirement_job_for_recovery_test(
        connection,
        epoch_id=epoch_id,
        expected_revision=1,
        job=root,
    )
    pair = SemanticPairKey(
        SubjectKind.REQUIREMENT,
        database.group.requirements[0].requirement_version_id,
        database.base.chunk_ids[0],
    )
    result = _legacy_requirement_result(epoch_id=epoch_id, root=root, pairs=(pair,))
    staged, output = _stage_legacy_requirement_root(
        connection,
        epoch_id=epoch_id,
        expected_revision=2,
        lease=lease,
        job=root,
        result=result,
    )
    assert lease.attempt is not None
    connection.commit()
    return {
        "epoch_id": epoch_id,
        "revision": int(staged.resulting_revision),
        "attempt_id": lease.attempt.attempt_id,
        "attempt_output_digest": output.attempt_output_digest,
        "channel_hit_count": len(result.channel_hits),
        "selection_count": len(result.selections),
    }


def _complete_requirement_verifier_for_recovery_test(
    connection: Connection[Any],
) -> dict[str, Any]:
    closed = _close_requirement_roots_for_recovery_test(connection)
    database = closed["database"]
    epoch_id = int(closed["epoch_id"])
    jobs = runtime_support.PostgresM5RuntimeStore(connection).verifier_jobs(epoch_id)
    assert len(jobs) == 1
    job = jobs[0]
    assert job.pair is not None
    assert job.scope_contract_digest is not None
    row, _lease = _acquire_requirement_job_for_recovery_test(
        connection,
        epoch_id=epoch_id,
        expected_revision=int(closed["revision"]),
        job=job,
    )
    attempt = row["attempt"]
    assert isinstance(attempt, M5JobAttempt)
    current_revision = int(row["dispatched_revision"])
    resulting_revision = current_revision + 1

    chunk = connection.execute(
        """
        SELECT document_version_id, chunk_index, text, text_hash
        FROM groundloop_chunk_version WHERE chunk_version_id = %s
        """,
        (job.pair.chunk_version_id,),
    ).fetchone()
    assert chunk is not None
    requirement = database.group.requirements[0]
    chunker_artifact_id = "recovery-verifier-chunker"
    pair_input = M5RequirementPairInput.build(
        pair=job.pair,
        scope_contract_digest=job.scope_contract_digest,
        candidate_policy_id=job.candidate_policy_id,
        owner_claim_id=database.group.owner_claim_id,
        group_version_id=database.group.group_version_id,
        group_family_id=database.group.group_family_id,
        requirement_ordinal=requirement.ordinal,
        requirement_text=requirement.requirement_text,
        document_version_id=str(chunk[0]),
        chunk_index=int(chunk[1]),
        chunk_text=str(chunk[2]),
        stored_chunk_text_hash=str(chunk[3]).strip(),
        chunker_artifact_id=chunker_artifact_id,
    )
    policy = DecisionPolicy(database.base.policy_version, 0.8, 0.8)
    verifier_artifact = M5RequirementVerifierArtifact.build_checked(
        pair=job.pair,
        pair_input_hash=pair_input.pair_input_hash,
        execution_spec_hash=job.execution_spec_hash,
        model_artifact_id="recovery-verifier-model-artifact",
        model_id="recovery-verifier-model",
        model_revision="v1",
        prompt_artifact_id="recovery-verifier-prompt-artifact",
        prompt_version="v1",
        calibration_version="uncalibrated-v1",
        calibration_artifact_hash=None,
        temperature=1.0,
        decision_policy=policy,
        support_score=0.9,
        refute_score=0.05,
        neutral_score=0.05,
        raw_logits=(-1.0, 2.0, 0.0),
        raw_output_hash=_sha("recovery-verifier-raw-output"),
    )
    observation = verifier_artifact.to_semantic_observation()
    attempt_output = M5AttemptOutput.build(
        attempt=attempt,
        job_epoch_id=epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=verifier_artifact.artifact_id,
        result_artifact_hash=verifier_artifact.artifact_hash,
    )
    attempt_result = M5AttemptResultArtifact.build(
        attempt_output=attempt_output,
        job_state_at_receipt=M5JobState.RUNNING,
        job_state_after=M5JobState.COMPLETED_ACTIVE,
        disposition=M5AttemptDisposition.VERIFIER_COMPLETED_ACTIVE,
        activity_snapshot_epoch_id=epoch_id,
        activity_snapshot_revision=current_revision,
        epoch_active=True,
        chunk_active=True,
        requirement_active=True,
        group_active=True,
        archive_reason=None,
    )
    completion = M5JobCompletion.build(
        job=job,
        terminal_state=M5JobState.COMPLETED_ACTIVE,
        result_artifact_id=verifier_artifact.artifact_id,
        result_artifact_hash=verifier_artifact.artifact_hash,
    )

    with connection.transaction():
        connection.execute(
            """
            INSERT INTO groundloop_chunker_artifact (
                chunker_artifact_id, chunker_version,
                normalization_version, config_hash
            ) VALUES (%s, 'fixture-v1', 'v1', %s)
            """,
            (chunker_artifact_id, _sha("recovery-verifier-chunker-config")),
        )
        connection.execute(
            """
            INSERT INTO groundloop_chunk_provenance (
                chunk_version_id, chunker_artifact_id, input_hash
            ) VALUES (%s, %s, %s)
            """,
            (
                job.pair.chunk_version_id,
                chunker_artifact_id,
                _sha("recovery-verifier-chunker-input"),
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_model_artifact (
                model_artifact_id, task, provider, model_id,
                immutable_revision, tokenizer_revision, license_id,
                config_hash
            ) VALUES (
                'recovery-verifier-model-artifact', 'verification', 'fixture',
                'recovery-verifier-model', 'v1', 'v1', 'MIT', %s
            )
            """,
            (_sha("recovery-verifier-model-config"),),
        )
        connection.execute(
            """
            INSERT INTO groundloop_prompt_artifact (
                prompt_artifact_id, task, version, template,
                template_hash, decoding_config_hash
            ) VALUES (
                'recovery-verifier-prompt-artifact', 'verification', 'v1',
                'recovery verifier prompt', %s, %s
            )
            """,
            (
                _sha("recovery-verifier-prompt"),
                _sha("recovery-verifier-decoding"),
            ),
        )
        connection.execute(
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
                %s, 'requirement', %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                pair_input.pair_input_hash,
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
            ),
        )
        connection.execute(
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
                %s, %s, 'requirement', %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s
            )
            """,
            (
                verifier_artifact.artifact_id,
                verifier_artifact.artifact_hash,
                verifier_artifact.pair.subject_id,
                verifier_artifact.pair.chunk_version_id,
                verifier_artifact.semantic_pair_digest,
                verifier_artifact.pair_input_hash,
                verifier_artifact.execution_spec_hash,
                verifier_artifact.model_artifact_id,
                verifier_artifact.model_id,
                verifier_artifact.model_revision,
                verifier_artifact.prompt_artifact_id,
                verifier_artifact.prompt_version,
                verifier_artifact.calibration_version,
                verifier_artifact.calibration_artifact_hash,
                verifier_artifact.temperature,
                verifier_artifact.decision_policy_version,
                verifier_artifact.decision_policy_hash,
                verifier_artifact.support_score,
                verifier_artifact.refute_score,
                verifier_artifact.neutral_score,
                *verifier_artifact.raw_logits,
                verifier_artifact.raw_output_hash,
                verifier_artifact.operational_label.value,
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_semantic_observation (
                observation_id, subject_kind, subject_id, chunk_version_id,
                task_type, support_score, refute_score, neutral_score,
                model_id, model_version, prompt_version, input_hash,
                produced_epoch, raw_output_hash, eligible_for_currency
            ) VALUES (
                %s, 'requirement', %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, true
            )
            """,
            (
                observation.observation_id,
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
                verifier_artifact.raw_output_hash,
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_requirement_verifier_execution (
                observation_id, artifact_id, artifact_hash, logical_job_id,
                attempt_id, pair_input_hash, decision_policy_version,
                decision_policy_hash, eligible_for_currency, produced_epoch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, true, %s)
            """,
            (
                observation.observation_id,
                verifier_artifact.artifact_id,
                verifier_artifact.artifact_hash,
                job.logical_job_id,
                attempt.attempt_id,
                pair_input.pair_input_hash,
                verifier_artifact.decision_policy_version,
                verifier_artifact.decision_policy_hash,
                epoch_id,
            ),
        )
        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (epoch_id, current_revision),
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_job_attempt
            SET attempt_state = 'result_reserved', attempt_output_digest = %s
            WHERE attempt_id = %s AND attempt_state = 'dispatched'
            """,
                (attempt_output.attempt_output_digest, attempt.attempt_id),
            ).rowcount
            == 1
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_job_attempt
            SET attempt_state = 'completed', finished_at = clock_timestamp()
            WHERE attempt_id = %s AND attempt_state = 'result_reserved'
            """,
                (attempt.attempt_id,),
            ).rowcount
            == 1
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_semantic_job
            SET job_state = 'completed_active', result_artifact_id = %s,
                result_artifact_hash = %s, completion_digest = %s,
                completed_revision = %s, completed_at = clock_timestamp()
            WHERE logical_job_id = %s AND job_state = 'running'
            """,
                (
                    verifier_artifact.artifact_id,
                    verifier_artifact.artifact_hash,
                    completion.completion_digest,
                    resulting_revision,
                    job.logical_job_id,
                ),
            ).rowcount
            == 1
        )
        _insert_requirement_attempt_artifact(
            connection,
            {
                "attempt_result_artifact_id": (
                    attempt_result.attempt_result_artifact_id
                ),
                "attempt_result_artifact_hash": (
                    attempt_result.attempt_result_artifact_hash
                ),
                "attempt_output_digest": attempt_output.attempt_output_digest,
                "attempt_id": attempt.attempt_id,
                "logical_job_id": job.logical_job_id,
                "job_epoch_id": epoch_id,
                "payload_hash": job.payload_hash,
                "execution_spec_hash": job.execution_spec_hash,
                "result_artifact_id": verifier_artifact.artifact_id,
                "result_artifact_hash": verifier_artifact.artifact_hash,
                "job_state_at_receipt": M5JobState.RUNNING.value,
                "job_state_after": M5JobState.COMPLETED_ACTIVE.value,
                "disposition": (M5AttemptDisposition.VERIFIER_COMPLETED_ACTIVE.value),
                "activity_snapshot_epoch_id": epoch_id,
                "activity_snapshot_revision": current_revision,
                "epoch_active": True,
                "chunk_active": True,
                "requirement_active": True,
                "group_active": True,
                "archive_reason": None,
                "cancelled_by_event_id": None,
                "cancelled_by_epoch_id": None,
                "cancellation_reason": None,
            },
        )
        for relation in (
            "groundloop_m5_owner_pending_counter",
            "groundloop_m5_answer_pending_counter",
        ):
            connection.execute(
                sql.SQL(
                    "UPDATE {} SET verifier_job_count = 0, "
                    "updated_revision = %s WHERE epoch_id = %s"
                ).format(sql.Identifier(relation)),
                (resulting_revision, epoch_id),
            )
        assert (
            connection.execute(
                "UPDATE groundloop_epoch SET revision = %s "
                "WHERE epoch_id = %s AND revision = %s",
                (resulting_revision, epoch_id, current_revision),
            ).rowcount
            == 1
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_runtime_epoch
            SET revision = %s, open_work_count = 0,
                runtime_state = 'semantic_pending'
            WHERE epoch_id = %s AND revision = %s
            """,
                (resulting_revision, epoch_id, current_revision),
            ).rowcount
            == 1
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

    root_artifact = connection.execute(
        """
        SELECT artifact.attempt_id, artifact.attempt_result_artifact_hash
        FROM groundloop_m5_attempt_result_artifact AS artifact
        JOIN groundloop_m5_semantic_job AS job
          ON job.logical_job_id = artifact.logical_job_id
        WHERE artifact.job_epoch_id = %s
          AND job.job_kind <> 'verify_requirement_pair'
        ORDER BY artifact.attempt_id COLLATE "C" LIMIT 1
        """,
        (epoch_id,),
    ).fetchone()
    assert root_artifact is not None
    connection.commit()
    return {
        "epoch_id": epoch_id,
        "revision": resulting_revision,
        "attempt_id": attempt.attempt_id,
        "attempt_result_hash": attempt_result.attempt_result_artifact_hash,
        "root_attempt_id": str(root_artifact[0]).strip(),
        "root_attempt_result_hash": str(root_artifact[1]).strip(),
    }


def _seed_requirement_audit_attempt(
    connection: Connection[Any],
) -> dict[str, Any]:
    database = _prepare_requirement_runtime_for_recovery_test(connection)
    epoch_id, _roots = _legacy_open_overlap_event(
        database,
        event_id="recovery-requirement-audit",
    )
    job = connection.execute(
        """
        SELECT logical_job_id, structural_event_id, job_kind, payload_hash,
               execution_spec_hash
        FROM groundloop_m5_semantic_job
        WHERE epoch_id = %s AND parent_job_id IS NULL
          AND job_kind = 'forward_requirement_retrieval'
        ORDER BY logical_job_id COLLATE "C"
        LIMIT 1
        """,
        (epoch_id,),
    ).fetchone()
    assert job is not None
    logical_job_id = str(job[0]).strip()
    execution_spec_hash = str(job[4]).strip()
    attempt = M5JobAttempt.build(
        logical_job_id=logical_job_id,
        attempt_ordinal=1,
        execution_spec_hash=execution_spec_hash,
        lease_token_hash=_sha(f"requirement-audit-lease:{logical_job_id}:1"),
    )
    provenance_digest = stable_m5_digest(
        "m5-requirement-root-provenance-v1",
        int_field(epoch_id),
        text_field(logical_job_id),
        bool_field(False),
    )
    with connection.transaction():
        connection.execute(
            """
            INSERT INTO groundloop_m5_runtime_operational_config (
                epoch_id, lease_duration_ms, config_digest
            ) VALUES (%s, %s, %s)
            """,
            (
                epoch_id,
                database.operational_config.lease_duration_ms,
                database.operational_config.config_digest,
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_requirement_root_provenance (
                epoch_id, root_job_id, fallback_required, provenance_digest
            ) VALUES (%s, %s, false, %s)
            """,
            (epoch_id, logical_job_id, provenance_digest),
        )
        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, 1)",
            (epoch_id,),
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_semantic_job
            SET job_state = 'running'
            WHERE epoch_id = %s AND logical_job_id = %s
              AND job_state = 'declared'
            """,
                (epoch_id, logical_job_id),
            ).rowcount
            == 1
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_job_attempt (
                attempt_id, logical_job_id, attempt_ordinal,
                execution_spec_hash, lease_token_hash, attempt_state,
                lease_expires_at
            ) VALUES (%s, %s, 1, %s, %s, 'dispatched',
                      clock_timestamp() + interval '1 hour')
            """,
            (
                attempt.attempt_id,
                logical_job_id,
                execution_spec_hash,
                attempt.lease_token_hash,
            ),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_owner_pending_counter
            SET updated_revision = 2 WHERE epoch_id = %s
            """,
            (epoch_id,),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_answer_pending_counter
            SET updated_revision = 2 WHERE epoch_id = %s
            """,
            (epoch_id,),
        )
        assert (
            connection.execute(
                "UPDATE groundloop_epoch SET revision = 2 "
                "WHERE epoch_id = %s AND revision = 1",
                (epoch_id,),
            ).rowcount
            == 1
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_runtime_epoch
            SET revision = 2, runtime_state = 'semantic_pending'
            WHERE epoch_id = %s AND revision = 1
            """,
                (epoch_id,),
            ).rowcount
            == 1
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    with connection.transaction():
        lease = connection.execute(
            """
            SELECT lease_expires_at
            FROM groundloop_m5_job_attempt
            WHERE attempt_id = %s
            """,
            (attempt.attempt_id,),
        ).fetchone()
    assert lease is not None
    return {
        "epoch_id": epoch_id,
        "logical_job_id": logical_job_id,
        "structural_event_id": str(job[1]),
        "job_kind": str(job[2]),
        "payload_hash": str(job[3]).strip(),
        "execution_spec_hash": execution_spec_hash,
        "attempt": attempt,
        "lease_expires_at": lease[0],
        "dispatched_revision": 2,
    }


def _cancel_requirement_job_for_audit(
    connection: Connection[Any],
    row: dict[str, Any],
    *,
    cancellation_reason: M5TerminalReason = M5TerminalReason.SUBJECT_INACTIVE,
) -> None:
    epoch_id = int(row["epoch_id"])
    identity = connection.execute(
        """
        SELECT runtime.revision, job.payload_hash, job.execution_spec_hash
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
        WHERE runtime.epoch_id = %s AND job.logical_job_id = %s
        """,
        (epoch_id, row["logical_job_id"]),
    ).fetchone()
    assert identity is not None
    connection.commit()
    expected_revision = int(identity[0])
    resulting_revision = expected_revision + 1
    completion_digest = stable_m5_digest(
        "m5-job-completion-v2",
        text_field(str(row["logical_job_id"])),
        hash_field(str(identity[1]).strip()),
        hash_field(str(identity[2]).strip()),
        enum_field(M5JobState.CANCELLED),
        option_field(None),
        option_field(None),
        option_field(None),
        option_field(None),
        option_field(enum_field(cancellation_reason)),
    )
    with connection.transaction():
        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (epoch_id, expected_revision),
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_semantic_job
            SET job_state = 'cancelled', archive_reason = %s,
                completion_digest = %s, cancelled_by_event_id = %s,
                cancelled_by_epoch_id = %s, cancellation_reason = %s,
                completed_revision = %s, completed_at = clock_timestamp()
            WHERE epoch_id = %s AND logical_job_id = %s
              AND job_state = 'running'
            """,
                (
                    cancellation_reason.value,
                    completion_digest,
                    row["structural_event_id"],
                    epoch_id,
                    cancellation_reason.value,
                    resulting_revision,
                    epoch_id,
                    row["logical_job_id"],
                ),
            ).rowcount
            == 1
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_discovery_scope
            SET scope_state = 'cancelled', completion_digest = %s,
                closed_revision = %s, closed_at = clock_timestamp()
            WHERE epoch_id = %s AND root_job_id = %s
              AND scope_state = 'open'
            """,
                (
                    completion_digest,
                    resulting_revision,
                    epoch_id,
                    row["logical_job_id"],
                ),
            ).rowcount
            == 1
        )
        connection.execute(
            """
            UPDATE groundloop_m5_owner_pending_counter
            SET forward_scope_count = forward_scope_count - 1,
                updated_revision = %s
            WHERE epoch_id = %s
            """,
            (resulting_revision, epoch_id),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_answer_pending_counter
            SET forward_scope_count = forward_scope_count - 1,
                updated_revision = %s
            WHERE epoch_id = %s
            """,
            (resulting_revision, epoch_id),
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_epoch SET revision = %s
            WHERE epoch_id = %s AND revision = %s
            """,
                (resulting_revision, epoch_id, expected_revision),
            ).rowcount
            == 1
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_runtime_epoch
            SET revision = %s, open_work_count = open_work_count - 1,
                open_scope_count = open_scope_count - 1
            WHERE epoch_id = %s AND revision = %s
            """,
                (resulting_revision, epoch_id, expected_revision),
            ).rowcount
            == 1
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    row.update(
        {
            "cancelled_by_event_id": row["structural_event_id"],
            "cancelled_by_epoch_id": epoch_id,
            "cancellation_reason": cancellation_reason.value,
        }
    )


def _recompute_requirement_artifact_identity(artifact: dict[str, Any]) -> None:
    artifact["attempt_result_artifact_hash"] = stable_m5_digest(
        "m5-attempt-result-artifact-v2",
        hash_field(str(artifact["attempt_output_digest"])),
        text_field(str(artifact["attempt_id"])),
        text_field(str(artifact["logical_job_id"])),
        int_field(int(artifact["job_epoch_id"])),
        enum_field(str(artifact["job_state_at_receipt"])),
        enum_field(str(artifact["job_state_after"])),
        enum_field(str(artifact["disposition"])),
        int_field(int(artifact["activity_snapshot_epoch_id"])),
        int_field(int(artifact["activity_snapshot_revision"])),
        bool_field(bool(artifact["epoch_active"])),
        option_field(
            bool_field(bool(artifact["chunk_active"]))
            if artifact["chunk_active"] is not None
            else None
        ),
        option_field(
            bool_field(bool(artifact["requirement_active"]))
            if artifact["requirement_active"] is not None
            else None
        ),
        option_field(
            bool_field(bool(artifact["group_active"]))
            if artifact["group_active"] is not None
            else None
        ),
        option_field(
            enum_field(str(artifact["archive_reason"]))
            if artifact["archive_reason"] is not None
            else None
        ),
        option_field(
            text_field(str(artifact["cancelled_by_event_id"]))
            if artifact["cancelled_by_event_id"] is not None
            else None
        ),
        option_field(
            int_field(int(artifact["cancelled_by_epoch_id"]))
            if artifact["cancelled_by_epoch_id"] is not None
            else None
        ),
        option_field(
            enum_field(str(artifact["cancellation_reason"]))
            if artifact["cancellation_reason"] is not None
            else None
        ),
    )
    artifact["attempt_result_artifact_id"] = stable_m5_digest(
        "m5-attempt-result-id-v2",
        text_field(str(artifact["attempt_id"])),
        hash_field(str(artifact["attempt_result_artifact_hash"])),
    )


def _build_requirement_audit_artifact(
    connection: Connection[Any],
    row: dict[str, Any],
    *,
    receipt_state: M5JobState = M5JobState.CANCELLED,
    epoch_active: bool = True,
) -> dict[str, Any]:
    attempt = row["attempt"]
    assert isinstance(attempt, M5JobAttempt)
    activity = connection.execute(
        "SELECT revision FROM groundloop_epoch WHERE epoch_id = %s",
        (row["epoch_id"],),
    ).fetchone()
    assert activity is not None
    result_artifact_id = _sha(
        f"requirement-audit-result-id:{attempt.attempt_id}:{receipt_state.value}"
    )
    result_artifact_hash = _sha(
        f"requirement-audit-result-hash:{attempt.attempt_id}:{receipt_state.value}"
    )
    output = M5AttemptOutput.build(
        attempt=attempt,
        job_epoch_id=int(row["epoch_id"]),
        payload_hash=str(row["payload_hash"]),
        result_artifact_id=result_artifact_id,
        result_artifact_hash=result_artifact_hash,
    )
    cancelled = receipt_state is M5JobState.CANCELLED
    artifact = M5AttemptResultArtifact.build(
        attempt_output=output,
        job_state_at_receipt=receipt_state,
        job_state_after=receipt_state,
        disposition=M5AttemptDisposition.TERMINAL_AUDIT_ONLY,
        activity_snapshot_epoch_id=int(row["epoch_id"]),
        activity_snapshot_revision=int(activity[0]),
        epoch_active=epoch_active,
        chunk_active=None,
        requirement_active=True,
        group_active=True,
        archive_reason=(
            M5AttemptArchiveReason.JOB_ALREADY_TERMINAL
            if epoch_active
            else M5AttemptArchiveReason.EPOCH_FAILED
        ),
        cancelled_by_event_id=(
            str(row["cancelled_by_event_id"]) if cancelled else None
        ),
        cancelled_by_epoch_id=(
            int(row["cancelled_by_epoch_id"]) if cancelled else None
        ),
        cancellation_reason=(
            M5TerminalReason(str(row["cancellation_reason"])) if cancelled else None
        ),
    )
    return {
        "attempt_result_artifact_id": artifact.attempt_result_artifact_id,
        "attempt_result_artifact_hash": artifact.attempt_result_artifact_hash,
        "attempt_output_digest": output.attempt_output_digest,
        "attempt_id": output.attempt_id,
        "logical_job_id": output.logical_job_id,
        "job_epoch_id": output.job_epoch_id,
        "payload_hash": output.payload_hash,
        "execution_spec_hash": output.execution_spec_hash,
        "result_artifact_id": output.result_artifact_id,
        "result_artifact_hash": output.result_artifact_hash,
        "job_state_at_receipt": artifact.job_state_at_receipt.value,
        "job_state_after": artifact.job_state_after.value,
        "disposition": artifact.disposition.value,
        "activity_snapshot_epoch_id": artifact.activity_snapshot_epoch_id,
        "activity_snapshot_revision": artifact.activity_snapshot_revision,
        "epoch_active": artifact.epoch_active,
        "chunk_active": artifact.chunk_active,
        "requirement_active": artifact.requirement_active,
        "group_active": artifact.group_active,
        "archive_reason": artifact.archive_reason.value,
        "cancelled_by_event_id": artifact.cancelled_by_event_id,
        "cancelled_by_epoch_id": artifact.cancelled_by_epoch_id,
        "cancellation_reason": (
            artifact.cancellation_reason.value
            if artifact.cancellation_reason is not None
            else None
        ),
    }


def _insert_requirement_attempt_artifact(
    connection: Connection[Any], artifact: dict[str, Any]
) -> None:
    connection.execute(
        """
        INSERT INTO groundloop_m5_attempt_result_artifact (
            attempt_result_artifact_id, attempt_result_artifact_hash,
            attempt_output_digest, attempt_id, logical_job_id, job_epoch_id,
            payload_hash, execution_spec_hash, result_artifact_id,
            result_artifact_hash, job_state_at_receipt, job_state_after,
            disposition, activity_snapshot_epoch_id,
            activity_snapshot_revision, epoch_active, chunk_active,
            requirement_active, group_active, archive_reason,
            cancelled_by_event_id, cancelled_by_epoch_id,
            cancellation_reason
        ) VALUES (
            %(attempt_result_artifact_id)s,
            %(attempt_result_artifact_hash)s,
            %(attempt_output_digest)s, %(attempt_id)s, %(logical_job_id)s,
            %(job_epoch_id)s, %(payload_hash)s, %(execution_spec_hash)s,
            %(result_artifact_id)s, %(result_artifact_hash)s,
            %(job_state_at_receipt)s, %(job_state_after)s, %(disposition)s,
            %(activity_snapshot_epoch_id)s, %(activity_snapshot_revision)s,
            %(epoch_active)s, %(chunk_active)s, %(requirement_active)s,
            %(group_active)s, %(archive_reason)s,
            %(cancelled_by_event_id)s, %(cancelled_by_epoch_id)s,
            %(cancellation_reason)s
        )
        """,
        artifact,
    )


def _insert_requirement_dispatch(
    connection: Connection[Any], row: dict[str, Any]
) -> str:
    attempt = row["attempt"]
    assert isinstance(attempt, M5JobAttempt)
    maximum_values = [0 for _ in WORK_COUNTER_COLUMNS]
    job_kind = str(row["job_kind"])
    if job_kind == "forward_requirement_retrieval":
        maximum_values[8] = 1
        maximum_values[27] = 1
    elif job_kind == "reverse_requirement_discovery":
        maximum_values[9] = 1
        maximum_values[27] = 1
    else:
        assert job_kind == "verify_requirement_pair"
        maximum_values[11] = 1
        maximum_values[28] = 1
    maximum_work_digest = _runtime_work_digest(tuple(maximum_values))
    record_digest = stable_m5_digest(
        "m5-dispatch-record-v1",
        int_field(int(row["epoch_id"])),
        enum_field("requirement"),
        text_field(attempt.attempt_id),
        text_field(str(row["logical_job_id"])),
        int_field(1),
        text_field(job_kind),
        bool_field(False),
        int_field(int(row["dispatched_revision"])),
        hash_field(maximum_work_digest),
    )
    columns = (
        "epoch_id",
        *(f"maximum_{name}" for name in WORK_COUNTER_COLUMNS),
        "maximum_work_digest",
        "subgraph",
        "attempt_id",
        "logical_job_id",
        "attempt_ordinal",
        "job_kind",
        "fallback_required",
        "dispatched_revision",
        "lease_expires_at",
        "record_digest",
    )
    connection.execute(
        sql.SQL("INSERT INTO groundloop_m5_dispatch_record ({}) VALUES ({})").format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        (
            row["epoch_id"],
            *maximum_values,
            maximum_work_digest,
            "requirement",
            attempt.attempt_id,
            row["logical_job_id"],
            1,
            job_kind,
            False,
            row["dispatched_revision"],
            row["lease_expires_at"],
            record_digest,
        ),
    )
    return record_digest


def _insert_requirement_execution_closure(
    connection: Connection[Any],
    *,
    row: dict[str, Any],
    attempt_values: tuple[int, ...],
) -> None:
    attempt = row["attempt"]
    assert isinstance(attempt, M5JobAttempt)
    assert len(attempt_values) == len(WORK_COUNTER_COLUMNS)
    _insert_requirement_dispatch(connection, row)
    attempt_work_digest = _runtime_work_digest(attempt_values)
    observation_digest = _timing_observation_digest(False, (None,) * 9)
    attempt_timing_digest = stable_m5_digest(
        "m5-attempt-runtime-timing-v1",
        int_field(int(row["epoch_id"])),
        enum_field("requirement"),
        text_field(attempt.attempt_id),
        hash_field(observation_digest),
    )
    result_or_error_hash = _sha(
        f"recovery-kind-result:{row['job_kind']}:{attempt.attempt_id}"
    )
    evidence_digest = stable_m5_digest(
        "m5-attempt-execution-evidence-v1",
        int_field(int(row["epoch_id"])),
        enum_field("requirement"),
        text_field(attempt.attempt_id),
        enum_field("returned"),
        hash_field(result_or_error_hash),
        hash_field(attempt_work_digest),
        hash_field(attempt_timing_digest),
    )
    columns = (
        "epoch_id",
        *(f"attempt_{name}" for name in WORK_COUNTER_COLUMNS),
        "attempt_work_digest",
        "subgraph",
        "attempt_id",
        "disposition",
        "result_or_error_hash",
        "attempt_timing_digest",
        "evidence_digest",
    )
    connection.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_attempt_execution_evidence ({}) VALUES ({})"
        ).format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        (
            row["epoch_id"],
            *attempt_values,
            attempt_work_digest,
            "requirement",
            attempt.attempt_id,
            "returned",
            result_or_error_hash,
            attempt_timing_digest,
            evidence_digest,
        ),
    )
    _insert_attempt_timing_contribution(
        connection,
        epoch_id=int(row["epoch_id"]),
        attempt_id=attempt.attempt_id,
        evidence_digest=evidence_digest,
        attempt_timing_digest=attempt_timing_digest,
        subgraph="requirement",
    )
    _insert_work_contribution(
        connection,
        epoch_id=int(row["epoch_id"]),
        contribution_kind="m5_attempt_execution",
        source_id=attempt.attempt_id,
        source_identity_hash=evidence_digest,
        applied_revision=int(row["dispatched_revision"]),
        values=attempt_values,
    )


def _insert_requirement_evidence(
    connection: Connection[Any],
    row: dict[str, Any],
    artifact: dict[str, Any],
    *,
    disposition: str = "returned",
) -> tuple[str, str, str]:
    attempt = row["attempt"]
    assert isinstance(attempt, M5JobAttempt)
    values = tuple(0 for _ in WORK_COUNTER_COLUMNS)
    attempt_work_digest = _runtime_work_digest(values)
    observation_digest = stable_m5_digest(
        "m5-runtime-timing-observation-v1",
        bool_field(False),
        *(option_field(None) for _ in range(9)),
    )
    timing_digest = stable_m5_digest(
        "m5-attempt-runtime-timing-v1",
        int_field(int(row["epoch_id"])),
        enum_field("requirement"),
        text_field(attempt.attempt_id),
        hash_field(observation_digest),
    )
    evidence_digest = stable_m5_digest(
        "m5-attempt-execution-evidence-v1",
        int_field(int(row["epoch_id"])),
        enum_field("requirement"),
        text_field(attempt.attempt_id),
        enum_field(disposition),
        hash_field(str(artifact["attempt_output_digest"])),
        hash_field(attempt_work_digest),
        hash_field(timing_digest),
    )
    columns = (
        "epoch_id",
        *(f"attempt_{name}" for name in WORK_COUNTER_COLUMNS),
        "attempt_work_digest",
        "subgraph",
        "attempt_id",
        "disposition",
        "result_or_error_hash",
        "attempt_timing_digest",
        "evidence_digest",
    )
    connection.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_attempt_execution_evidence ({}) VALUES ({})"
        ).format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        (
            row["epoch_id"],
            *values,
            attempt_work_digest,
            "requirement",
            attempt.attempt_id,
            disposition,
            artifact["attempt_output_digest"],
            timing_digest,
            evidence_digest,
        ),
    )
    return evidence_digest, attempt_work_digest, timing_digest


def _seed_fake_attempt(
    connection: Connection[Any], *, subgraph: str, after_016: bool
) -> int:
    """Seed one production-valid attempt for the install/rerun guard."""

    if subgraph == "requirement":
        assert not after_016
        return _seed_legacy_requirement_attempt(connection)

    database = _prepare_direct_runtime(connection)
    opened = _open_legacy_direct_epoch(database)
    with connection.transaction(), connection.cursor() as cursor:
        direct_support._authorize(cursor, opened.epoch_id, 1)
        _acquire_legacy_direct_job(
            cursor,
            opened=opened,
            expected_revision=1,
            job=opened.root,
            after_016=after_016,
        )
        _advance_legacy_direct_runtime(cursor, opened.epoch_id, 1, "semantic_pending")
    connection.autocommit = False
    return opened.epoch_id


def _typed_direct_completion_digest(
    *,
    job_id: str,
    payload_hash: str,
    execution_spec_hash: str,
    result_artifact_id: str,
    result_artifact_hash: str,
    completion_digest: str,
    child_parent_job_id: str | None,
    child_completion_digest: str | None,
    child_set_hash: str | None,
    child_job_ids: tuple[str, ...] = (),
) -> str:
    return stable_m5_digest(
        "m5-typed-direct-late-completion-binding-v1",
        text_field(job_id),
        hash_field(payload_hash),
        hash_field(execution_spec_hash),
        text_field(result_artifact_id),
        hash_field(result_artifact_hash),
        enum_field("completed_active"),
        hash_field(completion_digest),
        option_field(
            text_field(child_parent_job_id) if child_parent_job_id is not None else None
        ),
        option_field(
            hash_field(child_completion_digest)
            if child_completion_digest is not None
            else None
        ),
        option_field(
            hash_field(child_set_hash) if child_set_hash is not None else None
        ),
        sequence_field(text_field(child_id) for child_id in child_job_ids),
    )


def _typed_direct_envelope_digest(
    *,
    epoch_id: int,
    return_kind: str,
    job_binding_digest: str,
    attempt_binding_digest: str,
    completion_binding_digest: str,
    discovery_binding_digest: str | None,
    scope_binding_digest: str | None,
    verifier_binding_digest: str | None,
) -> str:
    return stable_m5_digest(
        "m5-typed-direct-late-return-envelope-v1",
        int_field(epoch_id),
        enum_field(return_kind),
        hash_field(job_binding_digest),
        hash_field(attempt_binding_digest),
        hash_field(completion_binding_digest),
        option_field(
            hash_field(discovery_binding_digest)
            if discovery_binding_digest is not None
            else None
        ),
        option_field(
            hash_field(scope_binding_digest)
            if scope_binding_digest is not None
            else None
        ),
        option_field(
            hash_field(verifier_binding_digest)
            if verifier_binding_digest is not None
            else None
        ),
    )


def _recompute_verifier_envelope_digests(row: dict[str, Any]) -> None:
    verifier = row["verifier_binding"]
    assert isinstance(verifier, dict)
    observation = verifier["observation"]
    execution = verifier["verification_execution"]
    assert isinstance(observation, dict)
    execution_present = bool(row["verification_execution_present"])
    if execution_present:
        assert isinstance(execution, dict)
        reused_from = execution["reused_from_observation_id"]
        execution_fields = sequence_field(
            (
                text_field(str(execution["observation_id"])),
                text_field(str(execution["job_id"])),
                hash_field(str(execution["admitted_pair_id"])),
                text_field(str(execution["model_artifact_id"])),
                text_field(str(execution["prompt_artifact_id"])),
                hash_field(str(execution["execution_spec_hash"])),
                hash_field(str(execution["pair_input_hash"])),
                text_field(str(execution["calibration_version"])),
                hash_field(str(execution["calibration_artifact_sha256"])),
                f64_field(_f64_from_hex(str(execution["temperature"]))),
                sequence_field(
                    f64_field(_f64_from_hex(str(value)))
                    for value in execution["raw_logits"]
                ),
                hash_field(str(execution["raw_output_hash"])),
                option_field(
                    text_field(str(reused_from)) if reused_from is not None else None
                ),
            )
        )
    else:
        assert execution is None
        execution_fields = None
    row["verifier_binding_digest"] = stable_m5_digest(
        "m5-typed-direct-late-verifier-binding-v1",
        text_field(str(verifier["result_artifact_id"])),
        hash_field(str(verifier["result_artifact_hash"])),
        bool_field(execution_present),
        option_field(execution_fields),
        text_field(str(observation["observation_id"])),
        enum_field(str(observation["subject_kind"])),
        text_field(str(observation["subject_id"])),
        text_field(str(observation["chunk_version_id"])),
        text_field(str(observation["task_type"])),
        f64_field(_f64_from_hex(str(observation["support_score"]))),
        f64_field(_f64_from_hex(str(observation["refute_score"]))),
        f64_field(_f64_from_hex(str(observation["neutral_score"]))),
        text_field(str(observation["model_id"])),
        text_field(str(observation["model_version"])),
        text_field(str(observation["prompt_version"])),
        text_field(str(observation["input_hash"])),
        int_field(int(observation["produced_epoch"])),
        hash_field(str(observation["raw_output_hash"])),
        bool_field(bool(row["observation_eligible_for_currency"])),
        bool_field(bool(row["requested_make_effective"])),
    )
    row["envelope_digest"] = _typed_direct_envelope_digest(
        epoch_id=int(row["epoch_id"]),
        return_kind=str(row["return_kind"]),
        job_binding_digest=str(row["job_binding_digest"]),
        attempt_binding_digest=str(row["attempt_binding_digest"]),
        completion_binding_digest=str(row["completion_binding_digest"]),
        discovery_binding_digest=None,
        scope_binding_digest=None,
        verifier_binding_digest=str(row["verifier_binding_digest"]),
    )


def _prepare_direct_runtime(
    connection: Connection[Any],
) -> direct_support.DirectRuntimeDatabase:
    connection.autocommit = True
    with connection.transaction():
        base = direct_support.seed_base(
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

    direct_support.bootstrap_m4_publication(connection, sealed_epoch_id=base.epoch_id)
    manifest = direct_support._m5_manifest(base)
    ports = direct_support.PostgresM4ApplicationPorts(
        connection,
        structural_payloads={},
        execution_mode=direct_support.M4ExecutionMode.MEASURED,
    )
    ports.runtime_store.register_candidate_policy(
        direct_support._m4_manifest(base, manifest)
    )
    ports.register_claim_registry_snapshot(direct_support.REGISTRY_ID, base.claim_ids)
    direct_support.PostgresM5RuntimeStore(connection).register_candidate_policy(
        manifest
    )
    with connection.transaction():
        direct_support.install_test_activation_barrier(
            connection,
            base,
            activation_id="direct-m4-activation",
        )
    return direct_support.DirectRuntimeDatabase(
        connection=connection,
        base=base,
        manifest=manifest,
        ports=ports,
    )


def _persist_legacy_direct_snapshots(
    cursor: Any,
    epoch_id: int,
    requirements: Any,
    active_chunks: Any,
) -> None:
    """Persist migration-015 snapshots without D24 structural accounting."""

    direct_support._persist_requirement_snapshot(
        cursor,
        snapshot=requirements,
        epoch_id=epoch_id,
    )
    direct_support._persist_active_chunk_snapshot(
        cursor,
        snapshot=active_chunks,
        epoch_id=epoch_id,
    )


def _advance_legacy_direct_runtime(
    cursor: Any,
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
    cursor.execute("SET CONSTRAINTS ALL DEFERRED")


def _open_legacy_direct_epoch(
    database: direct_support.DirectRuntimeDatabase,
) -> Any:
    """Open an accounting-free typed-direct image for schema-oracle tests."""

    event, payload = direct_support._event_and_payload(database)
    withdrawal = database.ports.plan_exact_withdrawal(event)
    root = direct_support._job(
        event,
        kind=direct_support.JobKind.IMPACT_DISCOVERY,
        execution_hash=direct_support.IMPACT_EXECUTION_HASH,
        chunk_id=direct_support.NEW_CHUNK_ID,
    )
    scope = direct_support.DiscoveryScope(root.job_id, direct_support.REGISTRY_ID, ())
    adapter = direct_support.PostgresM5DirectM4Adapter(database.ports)
    with database.connection.transaction(), database.connection.cursor() as cursor:
        epoch_id, requirements, active_chunks = (
            direct_support._insert_typed_outer_declaration(cursor, database, event)
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
        _persist_legacy_direct_snapshots(
            cursor,
            epoch_id,
            requirements,
            active_chunks,
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
        cursor.execute("SET CONSTRAINTS ALL DEFERRED")
    adapter._after_outer_commit()
    return direct_support._OpenedDirectEpoch(
        event,
        payload,
        withdrawal,
        root,
        scope,
        epoch_id,
        adapter,
    )


def _acquire_legacy_direct_job(
    cursor: Any,
    *,
    opened: Any,
    expected_revision: int,
    job: Any,
    after_016: bool,
) -> Any:
    epoch_id = opened.epoch_id
    if after_016:
        config = M5RuntimeOperationalConfig.build(3_600_000)
        cursor.execute(
            """
            INSERT INTO groundloop_m5_runtime_operational_config (
                epoch_id, lease_duration_ms, config_digest
            ) VALUES (%s, %s, %s)
            ON CONFLICT (epoch_id) DO NOTHING
            """,
            (
                epoch_id,
                config.lease_duration_ms,
                config.config_digest,
            ),
        )
    lease_expires_at = (
        None
        if not after_016
        else cursor.execute("SELECT clock_timestamp() + interval '1 hour'").fetchone()[
            0
        ]
    )
    return opened.adapter._ports._acquire_direct_job_local(
        cursor,
        epoch_id,
        expected_revision,
        job,
        direct_support._deterministic_lease_token(job),
        lease_expires_at=lease_expires_at,
    )


def _open_alternate_runtime_header(
    connection: Connection[Any],
    database: direct_support.DirectRuntimeDatabase,
) -> int:
    update = direct_support.CorpusUpdateIdentity(
        event_id="recovery-config-second-event",
        payload_hash=_sha("recovery-config-second-payload"),
        update_kind=direct_support.UpdateKind.INSERT,
        previous_published_epoch_id=database.base.epoch_id,
        candidate_policy_id=database.manifest.candidate_policy_id,
    )
    event = direct_support.DynamicEventPlan(
        update=update,
        inserted_chunk_version_ids=(direct_support.NEW_CHUNK_ID,),
        deactivated_chunk_version_ids=(),
        registered_claim_ids=(),
        claim_registry_snapshot_id=direct_support.REGISTRY_ID,
    )
    with connection.transaction(), connection.cursor() as cursor:
        epoch_row = cursor.execute(
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
        assert epoch_row is not None
        epoch_id = int(epoch_row[0])
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
        requirements = direct_support.RequirementRegistrySnapshot.build(())
        active_chunks = direct_support.ActiveChunkSnapshot.build(
            tuple(
                direct_support.ActiveChunkSnapshotEntry.build(
                    chunk_version_id=str(row[0]),
                    chunk_text=str(row[1]),
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
        )
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
                direct_support.runtime_digests.requirement_root_set_digest(()),
            ),
        )
        _persist_legacy_direct_snapshots(
            cursor,
            epoch_id,
            requirements,
            active_chunks,
        )
        cursor.execute(
            """
            INSERT INTO groundloop_m4_update (
                epoch_id, update_kind, candidate_policy_id,
                previous_published_epoch_id, registry_snapshot_id, manifest
            ) VALUES (%s, 'insert', %s, %s, %s, '{}'::jsonb)
            """,
            (
                epoch_id,
                database.manifest.candidate_policy_id,
                database.base.epoch_id,
                direct_support.REGISTRY_ID,
            ),
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return epoch_id


def _expire_direct_attempt(
    connection: Connection[Any],
    *,
    attempt_id: str,
    successor_ordinal: int | None = 2,
) -> None:
    with connection.transaction():
        row = connection.execute(
            """
            SELECT job_id, execution_spec_hash, attempt_ordinal
            FROM groundloop_semantic_job_attempt
            WHERE attempt_id = %s AND attempt_state = 'leased'
            """,
            (attempt_id,),
        ).fetchone()
        assert row is not None
        job_id, execution_spec_hash, attempt_ordinal = row
        changed = connection.execute(
            """
            UPDATE groundloop_semantic_job_attempt
            SET attempt_state = 'expired', finished_at = clock_timestamp()
            WHERE attempt_id = %s AND attempt_state = 'leased'
            """,
            (attempt_id,),
        ).rowcount
        assert changed == 1
        if successor_ordinal is not None:
            successor_attempt_id = stable_m4_digest(
                "m4-job-attempt-v1", str(job_id), str(successor_ordinal)
            )
            successor_lease_token = stable_m4_digest(
                "m4-lease-token-v1", str(job_id), str(successor_ordinal)
            )
            connection.execute(
                """
                INSERT INTO groundloop_semantic_job_attempt (
                    attempt_id, job_id, execution_spec_hash,
                    attempt_ordinal, lease_token_hash, attempt_state,
                    lease_expires_at
                ) VALUES (%s, %s, %s, %s, %s, 'leased',
                          clock_timestamp() + interval '1 hour')
                """,
                (
                    successor_attempt_id,
                    job_id,
                    execution_spec_hash,
                    successor_ordinal,
                    successor_lease_token,
                ),
            )
        assert int(attempt_ordinal) == 1


def _cancel_direct_job_for_audit(
    connection: Connection[Any],
    *,
    epoch_id: int,
    job_id: str,
    terminal_reason: str = "subject_inactive",
) -> str:
    """Install the exact typed-only terminal projection for an in-flight return."""

    with connection.transaction():
        row = connection.execute(
            """
            SELECT runtime.revision, job.job_state, job.completion_digest
            FROM groundloop_m5_runtime_epoch AS runtime
            JOIN groundloop_semantic_job AS job
              ON job.epoch_id = runtime.epoch_id
            WHERE runtime.epoch_id = %s AND job.job_id = %s
            """,
            (epoch_id, job_id),
        ).fetchone()
        assert row is not None
        revision = int(row[0])
        assert str(row[1]) == "running"
        assert row[2] is None
        terminal_identity_hash = stable_m5_digest(
            "m5-typed-direct-terminal-projection-v1",
            enum_field("direct"),
            text_field(job_id),
            enum_field("cancelled"),
            option_field(text_field(terminal_reason)),
            option_field(None),
            int_field(revision),
        )
        changed = connection.execute(
            """
            UPDATE groundloop_semantic_job
            SET job_state = 'cancelled', completed_revision = %s,
                completed_at = clock_timestamp()
            WHERE epoch_id = %s AND job_id = %s AND job_state = 'running'
            """,
            (revision, epoch_id, job_id),
        ).rowcount
        assert changed == 1
        connection.execute(
            """
            INSERT INTO groundloop_m5_direct_terminal_projection (
                epoch_id, job_id, terminal_state, terminal_reason,
                m4_completion_digest, completed_revision,
                terminal_identity_hash
            ) VALUES (%s, %s, 'cancelled', %s, NULL, %s, %s)
            """,
            (
                epoch_id,
                job_id,
                terminal_reason,
                revision,
                terminal_identity_hash,
            ),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return terminal_identity_hash


def _seed_direct_envelope_support(
    connection: Connection[Any],
    *,
    return_kind: str,
    verification_execution_present: bool = True,
    reused_execution: bool = False,
    requested_make_effective: bool = False,
    expire_attempt: bool = True,
    expired_successor_ordinal: int | None = 2,
) -> dict[str, Any]:
    database = _prepare_direct_runtime(connection)
    opened = _open_legacy_direct_epoch(database)
    with connection.transaction():
        connection.execute(
            """
            UPDATE groundloop_discovery_scope
            SET scope_kind = 'all_registered_claims', explicit_claim_ids = NULL
            WHERE root_job_id = %s AND closed_revision IS NULL
            """,
            (opened.root.job_id,),
        )

    with connection.transaction(), connection.cursor() as cursor:
        direct_support._authorize(cursor, opened.epoch_id, 1)
        root_lease = _acquire_legacy_direct_job(
            cursor,
            opened=opened,
            expected_revision=1,
            job=opened.root,
            after_016=True,
        )
        _advance_legacy_direct_runtime(cursor, opened.epoch_id, 1, "semantic_pending")
    assert root_lease.attempt_id is not None
    assert root_lease.lease_token_hash is not None

    discovery, discovery_completion, child = direct_support._complete_expansion(
        opened.epoch_id,
        opened.event,
        opened.root,
        database.base.claim_ids[0],
    )
    if return_kind == "discovery":
        job = opened.root
        attempt_id = root_lease.attempt_id
        lease_token_hash = root_lease.lease_token_hash
        completion = discovery_completion
        if expire_attempt:
            _expire_direct_attempt(
                connection,
                attempt_id=attempt_id,
                successor_ordinal=expired_successor_ordinal,
            )
    else:
        with connection.transaction(), connection.cursor() as cursor:
            direct_support._authorize(cursor, opened.epoch_id, 2)
            opened.adapter.stage_direct_expansion(
                cursor,
                opened.epoch_id,
                2,
                root_lease,
                discovery,
                discovery_completion,
                (child,),
            )
            _advance_legacy_direct_runtime(
                cursor, opened.epoch_id, 2, "semantic_pending"
            )
        with connection.transaction(), connection.cursor() as cursor:
            direct_support._authorize(cursor, opened.epoch_id, 3)
            child_lease = _acquire_legacy_direct_job(
                cursor,
                opened=opened,
                expected_revision=3,
                job=child,
                after_016=True,
            )
            _advance_legacy_direct_runtime(
                cursor, opened.epoch_id, 3, "semantic_pending"
            )
        assert child_lease.attempt_id is not None
        assert child_lease.lease_token_hash is not None
        job = child
        attempt_id = child_lease.attempt_id
        lease_token_hash = child_lease.lease_token_hash
        completion, _ = direct_support._verifier_completion(child)
        if expire_attempt:
            _expire_direct_attempt(
                connection,
                attempt_id=attempt_id,
                successor_ordinal=expired_successor_ordinal,
            )

    job_binding = {
        "job_id": job.job_id,
        "event_id": job.event_id,
        "job_kind": job.kind.value,
        "candidate_policy_id": job.candidate_policy_id,
        "payload_hash": job.payload_hash,
        "execution_spec_hash": job.execution_spec_hash,
        "parent_job_id": job.parent_job_id,
        "pair_claim_id": job.pair.claim_id if job.pair is not None else None,
        "pair_chunk_version_id": (
            job.pair.chunk_version_id if job.pair is not None else None
        ),
        "target_claim_id": job.target_claim_id,
        "target_chunk_version_id": job.target_chunk_version_id,
        "expandable": job.expandable,
    }
    job_binding_digest = stable_m5_digest(
        "m5-typed-direct-late-job-binding-v1",
        text_field(job.job_id),
        text_field(job.event_id),
        enum_field(job.kind),
        text_field(job.candidate_policy_id),
        hash_field(job.payload_hash),
        hash_field(job.execution_spec_hash),
        option_field(
            text_field(job.parent_job_id) if job.parent_job_id is not None else None
        ),
        option_field(text_field(job.pair.claim_id) if job.pair is not None else None),
        option_field(
            text_field(job.pair.chunk_version_id) if job.pair is not None else None
        ),
        option_field(
            text_field(job.target_claim_id) if job.target_claim_id is not None else None
        ),
        option_field(
            text_field(job.target_chunk_version_id)
            if job.target_chunk_version_id is not None
            else None
        ),
        bool_field(job.expandable),
    )
    attempt_binding = {
        "attempt_id": attempt_id,
        "job_id": job.job_id,
        "execution_spec_hash": job.execution_spec_hash,
        "attempt_ordinal": 1,
        "lease_token_hash": lease_token_hash,
    }
    attempt_binding_digest = stable_m5_digest(
        "m5-typed-direct-late-attempt-binding-v1",
        text_field(attempt_id),
        text_field(job.job_id),
        hash_field(job.execution_spec_hash),
        int_field(1),
        hash_field(lease_token_hash),
    )
    child_closure = completion.child_closure
    child_job_ids = () if child_closure is None else child_closure.child_job_ids
    completion_binding = {
        "job_id": completion.job_id,
        "payload_hash": completion.payload_hash,
        "execution_spec_hash": completion.execution_spec_hash,
        "result_artifact_id": completion.result_artifact_id,
        "result_artifact_hash": completion.result_artifact_hash,
        "terminal_state": completion.terminal_state.value,
        "completion_digest": completion.completion_digest,
        "child_parent_job_id": (
            None if child_closure is None else child_closure.parent_job_id
        ),
        "child_completion_digest": (
            None if child_closure is None else child_closure.completion_digest
        ),
        "child_set_hash": (
            None if child_closure is None else child_closure.child_set_hash
        ),
        "child_job_ids": list(child_job_ids),
    }
    completion_binding_digest = _typed_direct_completion_digest(
        job_id=job.job_id,
        payload_hash=job.payload_hash,
        execution_spec_hash=job.execution_spec_hash,
        result_artifact_id=completion.result_artifact_id,
        result_artifact_hash=completion.result_artifact_hash,
        completion_digest=completion.completion_digest,
        child_parent_job_id=(
            None if child_closure is None else child_closure.parent_job_id
        ),
        child_completion_digest=(
            None if child_closure is None else child_closure.completion_digest
        ),
        child_set_hash=(
            None if child_closure is None else child_closure.child_set_hash
        ),
        child_job_ids=child_job_ids,
    )
    row: dict[str, Any] = {
        "epoch_id": opened.epoch_id,
        "return_kind": return_kind,
        "job_id": job.job_id,
        "attempt_id": attempt_id,
        "result_artifact_id": completion.result_artifact_id,
        "result_artifact_hash": completion.result_artifact_hash,
        "job_binding": job_binding,
        "attempt_binding": attempt_binding,
        "completion_binding": completion_binding,
        "job_binding_digest": job_binding_digest,
        "attempt_binding_digest": attempt_binding_digest,
        "completion_binding_digest": completion_binding_digest,
    }

    if return_kind == "discovery":
        hits = tuple(
            sorted(
                discovery.channel_hits,
                key=lambda hit: (
                    hit.channel.value,
                    hit.rank,
                    hit.pair.claim_id,
                    hit.pair.chunk_version_id,
                    hit.candidate_policy_id,
                    hit.channel_artifact_hash,
                ),
            )
        )
        admitted_pairs = tuple(
            sorted(
                discovery.admitted_pairs,
                key=lambda pair: (
                    pair.fused_rank,
                    pair.pair.claim_id,
                    pair.pair.chunk_version_id,
                    pair.candidate_policy_id,
                ),
            )
        )
        channel_identities = tuple(
            sorted(
                stable_m4_digest(
                    "m4-discovery-channel-v1",
                    str(hit.epoch_id),
                    hit.pair.claim_id,
                    hit.pair.chunk_version_id,
                    hit.candidate_policy_id,
                    hit.channel.value,
                    str(hit.rank),
                    "" if hit.score is None else format(hit.score, ".17g"),
                    hit.channel_artifact_hash,
                )
                for hit in hits
            )
        )
        admitted_identities = tuple(
            sorted(
                stable_m4_digest(
                    "m4-admitted-pair-v1",
                    str(pair.epoch_id),
                    pair.pair.claim_id,
                    pair.pair.chunk_version_id,
                    pair.candidate_policy_id,
                )
                for pair in admitted_pairs
            )
        )
        channel_set_hash = stable_m4_digest(
            "m4-discovery-channel-set-v1", *channel_identities
        )
        admitted_pair_set_hash = stable_m4_digest(
            "m4-discovery-admitted-set-v1", *admitted_identities
        )
        discovery_binding = {
            "root_job_id": job.job_id,
            "result_artifact_id": completion.result_artifact_id,
            "result_artifact_hash": completion.result_artifact_hash,
            "fallback_satisfied": discovery.fallback_satisfied,
            "channel_hit_count": len(hits),
            "admitted_pair_count": len(admitted_pairs),
            "channel_set_hash": channel_set_hash,
            "admitted_pair_set_hash": admitted_pair_set_hash,
            "channel_hits": [
                {
                    "epoch_id": hit.epoch_id,
                    "claim_id": hit.pair.claim_id,
                    "chunk_version_id": hit.pair.chunk_version_id,
                    "candidate_policy_id": hit.candidate_policy_id,
                    "channel": hit.channel.value,
                    "rank": hit.rank,
                    "score": None if hit.score is None else _f64_hex(hit.score),
                    "channel_artifact_hash": hit.channel_artifact_hash,
                }
                for hit in hits
            ],
            "admitted_pairs": [
                {
                    "epoch_id": pair.epoch_id,
                    "claim_id": pair.pair.claim_id,
                    "chunk_version_id": pair.pair.chunk_version_id,
                    "candidate_policy_id": pair.candidate_policy_id,
                    "fused_rank": pair.fused_rank,
                    "reasons": [reason.value for reason in pair.reasons],
                    "mandatory_lineage": pair.mandatory_lineage,
                }
                for pair in admitted_pairs
            ],
        }
        discovery_binding_digest = stable_m5_digest(
            "m5-typed-direct-late-discovery-binding-v1",
            text_field(job.job_id),
            text_field(completion.result_artifact_id),
            hash_field(completion.result_artifact_hash),
            bool_field(discovery.fallback_satisfied),
            int_field(len(hits)),
            int_field(len(admitted_pairs)),
            hash_field(channel_set_hash),
            hash_field(admitted_pair_set_hash),
            sequence_field(
                sequence_field(
                    (
                        int_field(hit.epoch_id),
                        text_field(hit.pair.claim_id),
                        text_field(hit.pair.chunk_version_id),
                        text_field(hit.candidate_policy_id),
                        enum_field(hit.channel),
                        int_field(hit.rank),
                        option_field(
                            f64_field(hit.score) if hit.score is not None else None
                        ),
                        hash_field(hit.channel_artifact_hash),
                    )
                )
                for hit in hits
            ),
            sequence_field(
                sequence_field(
                    (
                        int_field(pair.epoch_id),
                        text_field(pair.pair.claim_id),
                        text_field(pair.pair.chunk_version_id),
                        text_field(pair.candidate_policy_id),
                        int_field(pair.fused_rank),
                        sequence_field(enum_field(reason) for reason in pair.reasons),
                        bool_field(pair.mandatory_lineage),
                    )
                )
                for pair in admitted_pairs
            ),
        )
        registered_claim_ids = tuple(database.base.claim_ids)
        scope_binding = {
            "root_job_id": job.job_id,
            "epoch_id": opened.epoch_id,
            "registry_snapshot_id": direct_support.REGISTRY_ID,
            "registered_claim_ids": list(registered_claim_ids),
            "closed": False,
            "persisted_scope_kind": "all_registered_claims",
            "explicit_claim_ids": None,
            "closed_revision": None,
        }
        scope_binding_digest = stable_m5_digest(
            "m5-typed-direct-late-scope-binding-v1",
            text_field(job.job_id),
            int_field(opened.epoch_id),
            text_field(direct_support.REGISTRY_ID),
            sequence_field(text_field(value) for value in registered_claim_ids),
            bool_field(False),
            enum_field("all_registered_claims"),
            option_field(None),
            option_field(None),
        )
        row.update(
            {
                "verification_execution_present": None,
                "observation_eligible_for_currency": None,
                "requested_make_effective": None,
                "discovery_binding": discovery_binding,
                "scope_binding": scope_binding,
                "verifier_binding": None,
                "discovery_binding_digest": discovery_binding_digest,
                "scope_binding_digest": scope_binding_digest,
                "verifier_binding_digest": None,
            }
        )
    else:
        assert job.pair is not None
        observation_id = _sha("late-verifier-observation")
        admitted_pair_id = stable_m4_digest(
            "m4-admitted-pair-v1",
            str(opened.epoch_id),
            job.pair.claim_id,
            job.pair.chunk_version_id,
            job.candidate_policy_id,
        )
        pair_input_hash = _sha("late-verifier-pair-input")
        calibration_hash = _sha("late-verifier-calibration")
        observation_scores = (0.7, 0.2, -0.0)
        raw_logits = (0.7, -0.0, 0.1)
        reused_observation_id = (
            _sha("late-verifier-reused-observation") if reused_execution else None
        )
        with connection.transaction():
            connection.execute(
                """
                INSERT INTO groundloop_model_artifact (
                    model_artifact_id, task, provider, model_id,
                    immutable_revision, tokenizer_revision, license_id,
                    config_hash
                ) VALUES (
                    'late-model-artifact', 'verification', 'fixture',
                    'late-model', 'v1', 'v1', 'MIT', %s
                )
                """,
                (_sha("late-verifier-model-config"),),
            )
            connection.execute(
                """
                INSERT INTO groundloop_prompt_artifact (
                    prompt_artifact_id, task, version, template,
                    template_hash, decoding_config_hash
                ) VALUES (
                    'late-prompt-artifact', 'verification', 'v1',
                    'late prompt', %s, %s
                )
                """,
                (_sha("late-verifier-prompt"), _sha("late-verifier-decoding")),
            )
            if reused_observation_id is not None:
                connection.execute(
                    """
                    INSERT INTO groundloop_semantic_observation (
                        observation_id, subject_kind, subject_id,
                        chunk_version_id, task_type, support_score,
                        refute_score, neutral_score, model_id, model_version,
                        prompt_version, input_hash, produced_epoch,
                        raw_output_hash, eligible_for_currency
                    ) VALUES (
                        %s, 'claim', %s, %s, 'verification',
                        0.6, 0.3, 0.1, 'late-model', 'late-model-v0',
                        'late-prompt-v0', %s, %s, %s, true
                    )
                    """,
                    (
                        reused_observation_id,
                        job.pair.claim_id,
                        job.pair.chunk_version_id,
                        _sha("late-verifier-reused-input"),
                        opened.epoch_id,
                        _sha("late-verifier-reused-output"),
                    ),
                )
            connection.execute(
                """
                INSERT INTO groundloop_semantic_observation (
                    observation_id, subject_kind, subject_id,
                    chunk_version_id, task_type, support_score, refute_score,
                    neutral_score, model_id, model_version, prompt_version,
                    input_hash, produced_epoch, raw_output_hash,
                    eligible_for_currency
                ) VALUES (
                    %s, 'claim', %s, %s, 'verification', %s, %s, %s,
                    'late-model', 'late-model-v1', 'late-prompt-v1',
                    %s, %s, %s, true
                )
                """,
                (
                    observation_id,
                    job.pair.claim_id,
                    job.pair.chunk_version_id,
                    *observation_scores,
                    pair_input_hash,
                    opened.epoch_id,
                    completion.result_artifact_hash,
                ),
            )
            if verification_execution_present:
                connection.execute(
                    """
                    INSERT INTO groundloop_m4_verification_execution (
                        observation_id, job_id, admitted_pair_id,
                        model_artifact_id, prompt_artifact_id,
                        execution_spec_hash, pair_input_hash,
                        calibration_version, calibration_artifact_sha256,
                        temperature, raw_logits, raw_output_hash,
                        reused_from_observation_id
                    ) VALUES (
                        %s, %s, %s, 'late-model-artifact',
                        'late-prompt-artifact', %s, %s, 'late-calibration-v1',
                        %s, 1.0, %s::double precision[], %s, %s
                    )
                    """,
                    (
                        observation_id,
                        job.job_id,
                        admitted_pair_id,
                        job.execution_spec_hash,
                        pair_input_hash,
                        calibration_hash,
                        list(raw_logits),
                        completion.result_artifact_hash,
                        reused_observation_id,
                    ),
                )
        observation = {
            "observation_id": observation_id,
            "subject_kind": "claim",
            "subject_id": job.pair.claim_id,
            "chunk_version_id": job.pair.chunk_version_id,
            "task_type": "verification",
            "support_score": _f64_hex(observation_scores[0]),
            "refute_score": _f64_hex(observation_scores[1]),
            "neutral_score": _f64_hex(observation_scores[2]),
            "model_id": "late-model",
            "model_version": "late-model-v1",
            "prompt_version": "late-prompt-v1",
            "input_hash": pair_input_hash,
            "produced_epoch": opened.epoch_id,
            "raw_output_hash": completion.result_artifact_hash,
            "eligible_for_currency": True,
            "requested_make_effective": requested_make_effective,
        }
        execution = (
            {
                "observation_id": observation_id,
                "job_id": job.job_id,
                "admitted_pair_id": admitted_pair_id,
                "model_artifact_id": "late-model-artifact",
                "prompt_artifact_id": "late-prompt-artifact",
                "execution_spec_hash": job.execution_spec_hash,
                "pair_input_hash": pair_input_hash,
                "calibration_version": "late-calibration-v1",
                "calibration_artifact_sha256": calibration_hash,
                "temperature": _f64_hex(1.0),
                "raw_logits": [_f64_hex(value) for value in raw_logits],
                "raw_output_hash": completion.result_artifact_hash,
                "reused_from_observation_id": reused_observation_id,
            }
            if verification_execution_present
            else None
        )
        execution_fields = (
            sequence_field(
                (
                    text_field(observation_id),
                    text_field(job.job_id),
                    hash_field(admitted_pair_id),
                    text_field("late-model-artifact"),
                    text_field("late-prompt-artifact"),
                    hash_field(job.execution_spec_hash),
                    hash_field(pair_input_hash),
                    text_field("late-calibration-v1"),
                    hash_field(calibration_hash),
                    f64_field(1.0),
                    sequence_field(f64_field(value) for value in raw_logits),
                    hash_field(completion.result_artifact_hash),
                    option_field(
                        text_field(reused_observation_id)
                        if reused_observation_id is not None
                        else None
                    ),
                )
            )
            if verification_execution_present
            else None
        )
        verifier_binding = {
            "result_artifact_id": completion.result_artifact_id,
            "result_artifact_hash": completion.result_artifact_hash,
            "verification_execution": execution,
            "observation": observation,
        }
        verifier_binding_digest = stable_m5_digest(
            "m5-typed-direct-late-verifier-binding-v1",
            text_field(completion.result_artifact_id),
            hash_field(completion.result_artifact_hash),
            bool_field(verification_execution_present),
            option_field(execution_fields),
            text_field(observation_id),
            enum_field("claim"),
            text_field(job.pair.claim_id),
            text_field(job.pair.chunk_version_id),
            text_field("verification"),
            *(f64_field(value) for value in observation_scores),
            text_field("late-model"),
            text_field("late-model-v1"),
            text_field("late-prompt-v1"),
            text_field(pair_input_hash),
            int_field(opened.epoch_id),
            hash_field(completion.result_artifact_hash),
            bool_field(True),
            bool_field(requested_make_effective),
        )
        row.update(
            {
                "verification_execution_present": verification_execution_present,
                "observation_eligible_for_currency": True,
                "requested_make_effective": requested_make_effective,
                "discovery_binding": None,
                "scope_binding": None,
                "verifier_binding": verifier_binding,
                "discovery_binding_digest": None,
                "scope_binding_digest": None,
                "verifier_binding_digest": verifier_binding_digest,
            }
        )

    row["envelope_digest"] = _typed_direct_envelope_digest(
        epoch_id=opened.epoch_id,
        return_kind=return_kind,
        job_binding_digest=job_binding_digest,
        attempt_binding_digest=attempt_binding_digest,
        completion_binding_digest=completion_binding_digest,
        discovery_binding_digest=row["discovery_binding_digest"],
        scope_binding_digest=row["scope_binding_digest"],
        verifier_binding_digest=row["verifier_binding_digest"],
    )
    connection.autocommit = False
    return row


def _insert_direct_envelope(connection: Connection[Any], row: dict[str, Any]) -> None:
    parameters = dict(row)
    for key in (
        "job_binding",
        "attempt_binding",
        "completion_binding",
        "discovery_binding",
        "scope_binding",
        "verifier_binding",
    ):
        if parameters[key] is not None:
            parameters[key] = Jsonb(parameters[key])
    connection.execute(
        """
        INSERT INTO groundloop_m5_typed_direct_late_return_envelope (
            epoch_id, return_kind, job_id, attempt_id,
            result_artifact_id, result_artifact_hash,
            verification_execution_present,
            observation_eligible_for_currency, requested_make_effective,
            job_binding, attempt_binding, completion_binding,
            discovery_binding, scope_binding, verifier_binding,
            job_binding_digest, attempt_binding_digest,
            completion_binding_digest, discovery_binding_digest,
            scope_binding_digest, verifier_binding_digest, envelope_digest
        ) VALUES (
            %(epoch_id)s, %(return_kind)s, %(job_id)s, %(attempt_id)s,
            %(result_artifact_id)s, %(result_artifact_hash)s,
            %(verification_execution_present)s,
            %(observation_eligible_for_currency)s, %(requested_make_effective)s,
            %(job_binding)s, %(attempt_binding)s, %(completion_binding)s,
            %(discovery_binding)s, %(scope_binding)s, %(verifier_binding)s,
            %(job_binding_digest)s, %(attempt_binding_digest)s,
            %(completion_binding_digest)s, %(discovery_binding_digest)s,
            %(scope_binding_digest)s, %(verifier_binding_digest)s,
            %(envelope_digest)s
        )
        """,
        parameters,
    )


def _runtime_work_digest(values: tuple[int, ...]) -> str:
    assert len(values) == len(WORK_COUNTER_COLUMNS)
    return stable_m5_digest(
        "m5-runtime-work-v2", *(int_field(value) for value in values)
    )


def _timing_observation_digest(
    required_interval_observed: bool,
    values: tuple[int | None, ...],
) -> str:
    assert len(values) == 9
    return stable_m5_digest(
        "m5-runtime-timing-observation-v1",
        bool_field(required_interval_observed),
        *(
            option_field(int_field(value) if value is not None else None)
            for value in values
        ),
    )


def _insert_runtime_work_row(
    connection: Connection[Any],
    *,
    epoch_id: int,
    structural_event_id: str,
    work_kind: str,
    values: tuple[int, ...],
) -> str:
    work_digest = _runtime_work_digest(values)
    columns = (
        "work_digest",
        "structural_event_id",
        "epoch_id",
        "work_kind",
        *WORK_COUNTER_COLUMNS,
    )
    connection.execute(
        sql.SQL("INSERT INTO groundloop_m5_runtime_work ({}) VALUES ({})").format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        (work_digest, structural_event_id, epoch_id, work_kind, *values),
    )
    return work_digest


def _terminalize_recovered_epoch_for_audit(
    connection: Connection[Any],
    *,
    epoch_id: int,
    runtime_locked: Event | None = None,
    continue_after_runtime_lock: Event | None = None,
    terminal_attempt_row: dict[str, Any] | None = None,
    terminal_attempt_values: tuple[int, ...] | None = None,
    terminal_attempt_timing_observed: bool = False,
    terminal_attempt_timing_values: tuple[int | None, ...] | None = None,
    include_terminal_attempt_timing: bool = True,
    include_terminal_transition_timing: bool = False,
) -> str:
    identity = connection.execute(
        """
        SELECT base.event_id, base.payload_hash, base.revision,
               runtime.runtime_state
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        WHERE base.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert identity is not None
    structural_event_id = str(identity[0])
    payload_hash = str(identity[1]).strip()
    revision = int(identity[2])
    assert str(identity[3]) in {"structural_committed", "semantic_pending"}
    zero_work = tuple(0 for _ in WORK_COUNTER_COLUMNS)
    if terminal_attempt_values is None:
        terminal_attempt_values = zero_work
    assert len(terminal_attempt_values) == len(WORK_COUNTER_COLUMNS)
    if terminal_attempt_timing_values is None:
        terminal_attempt_timing_values = (None,) * 9
    assert len(terminal_attempt_timing_values) == 9

    work_columns = (
        "work_digest",
        "epoch_id",
        *WORK_COUNTER_COLUMNS,
        "updated_revision",
        "terminalized",
    )
    existing_work = connection.execute(
        sql.SQL(
            "SELECT {} FROM groundloop_m5_runtime_work_accumulator WHERE epoch_id = %s"
        ).format(sql.SQL(", ").join(map(sql.Identifier, WORK_COUNTER_COLUMNS))),
        (epoch_id,),
    ).fetchone()
    if existing_work is None:
        connection.execute(
            sql.SQL(
                "INSERT INTO groundloop_m5_runtime_work_accumulator ({}) VALUES ({})"
            ).format(
                sql.SQL(", ").join(map(sql.Identifier, work_columns)),
                sql.SQL(", ").join(sql.Placeholder() for _ in work_columns),
            ),
            (_runtime_work_digest(zero_work), epoch_id, *zero_work, revision, False),
        )
    if (
        connection.execute(
            "SELECT 1 FROM groundloop_m5_runtime_timing_accumulator "
            "WHERE epoch_id = %s",
            (epoch_id,),
        ).fetchone()
        is None
    ):
        connection.execute(
            """
            INSERT INTO groundloop_m5_runtime_timing_accumulator (
                epoch_id, updated_revision, terminalized
            ) VALUES (%s, %s, false)
            """,
            (epoch_id, revision),
        )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()

    work_row = connection.execute(
        sql.SQL(
            "SELECT {}, work_digest FROM groundloop_m5_runtime_work_accumulator "
            "WHERE epoch_id = %s"
        ).format(sql.SQL(", ").join(map(sql.Identifier, WORK_COUNTER_COLUMNS))),
        (epoch_id,),
    ).fetchone()
    assert work_row is not None
    previous_work_values = tuple(int(value) for value in work_row[:-1])
    event_work_values = tuple(
        previous + current
        for previous, current in zip(
            previous_work_values,
            terminal_attempt_values,
            strict=True,
        )
    )
    event_work_digest = _runtime_work_digest(event_work_values)
    open_receipt_hash = stable_m5_digest(
        "m5-open-event-receipt-binding-v2",
        int_field(epoch_id),
        bool_field(False),
        bool_field(False),
        option_field(None),
        bool_field(False),
        option_field(None),
    )
    combined_hash = stable_m5_digest(
        "m5-combined-status-delta-set-v2", sequence_field(())
    )
    changed_hash = stable_m5_digest("m5-changed-state-set-v2", sequence_field(()))
    logical_result_hash = stable_m5_digest(
        "m5-event-run-logical-result-v2",
        text_field(structural_event_id),
        hash_field(payload_hash),
        int_field(epoch_id),
        enum_field("failed"),
        hash_field(open_receipt_hash),
        option_field(None),
        hash_field(event_work_digest),
        hash_field(combined_hash),
        hash_field(changed_hash),
        option_field(enum_field("invariant_failure")),
    )
    failure_source_hash = stable_m5_digest(
        "m5-epoch-failure-contribution-source-v1",
        text_field(structural_event_id),
        enum_field("invariant_failure"),
    )
    terminal_attempt_count = int(terminal_attempt_row is not None)
    included_attempt_timing = bool(
        terminal_attempt_row is not None and include_terminal_attempt_timing
    )
    attempt_required_observed = int(
        included_attempt_timing and terminal_attempt_timing_observed
    )
    attempt_required_missing = terminal_attempt_count - attempt_required_observed
    attempt_timing_sums = tuple(
        int(value) if included_attempt_timing and value is not None else 0
        for value in terminal_attempt_timing_values
    )
    attempt_optional_observed = tuple(
        int(included_attempt_timing and value is not None)
        for value in terminal_attempt_timing_values[5:]
    )

    with connection.transaction():
        connection.execute(
            "SELECT 1 FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s FOR UPDATE",
            (epoch_id,),
        )
        if runtime_locked is not None:
            assert continue_after_runtime_lock is not None
            runtime_locked.set()
            assert continue_after_runtime_lock.wait(timeout=10)
        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (epoch_id, revision),
        )
        if terminal_attempt_row is not None:
            evidence_digest, _, attempt_timing_digest = (
                _insert_direct_dispatch_and_evidence(
                    connection,
                    envelope_row=terminal_attempt_row,
                    disposition="returned",
                    force_constraints=False,
                    insert_dispatch=False,
                    attempt_values=terminal_attempt_values,
                    attempt_timing_observed=terminal_attempt_timing_observed,
                    attempt_timing_values=terminal_attempt_timing_values,
                )
            )
            if include_terminal_attempt_timing:
                _insert_attempt_timing_contribution(
                    connection,
                    epoch_id=epoch_id,
                    attempt_id=str(terminal_attempt_row["attempt_id"]),
                    evidence_digest=evidence_digest,
                    attempt_timing_digest=attempt_timing_digest,
                    required_interval_observed=terminal_attempt_timing_observed,
                    timing_values=terminal_attempt_timing_values,
                )
            attempt_contribution_key = _insert_work_contribution(
                connection,
                epoch_id=epoch_id,
                contribution_kind="direct_attempt_execution",
                source_id=str(terminal_attempt_row["attempt_id"]),
                source_identity_hash=evidence_digest,
                applied_revision=revision + 1,
                values=terminal_attempt_values,
            )
            if include_terminal_transition_timing:
                observation_digest = stable_m5_digest(
                    "m5-runtime-timing-observation-v1",
                    bool_field(False),
                    *(option_field(None) for _ in range(9)),
                )
                transition_timing_digest = stable_m5_digest(
                    "m5-transition-call-timing-v1",
                    int_field(epoch_id),
                    enum_field("direct_attempt_execution"),
                    text_field(str(terminal_attempt_row["attempt_id"])),
                    hash_field(attempt_contribution_key),
                    int_field(revision + 1),
                    hash_field(observation_digest),
                )
                connection.execute(
                    """
                    INSERT INTO groundloop_m5_transition_call_timing (
                        epoch_id, contribution_kind, source_id,
                        contribution_key_digest, anchor_revision,
                        required_interval_observed,
                        coordinator_non_db_non_neural_ns, neural_wall_ns,
                        postgres_roundtrip_wall_ns, external_io_wall_ns,
                        end_to_end_wall_ns, postgres_server_execution_ns,
                        postgres_lock_wait_ns, postgres_wal_bytes,
                        postgres_shared_block_reads, observation_digest,
                        transition_timing_digest
                    ) VALUES (
                        %s, 'direct_attempt_execution', %s, %s, %s, false,
                        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                        %s, %s
                    )
                    """,
                    (
                        epoch_id,
                        terminal_attempt_row["attempt_id"],
                        attempt_contribution_key,
                        revision + 1,
                        observation_digest,
                        transition_timing_digest,
                    ),
                )
        _insert_work_contribution(
            connection,
            epoch_id=epoch_id,
            contribution_kind="epoch_failure",
            source_id=structural_event_id,
            source_identity_hash=failure_source_hash,
            applied_revision=revision + 1,
            values=zero_work,
        )
        pending = connection.execute(
            """
            SELECT pending_contribution_kind, pending_source_id,
                   pending_contribution_key_digest, pending_anchor_revision
            FROM groundloop_m5_runtime_timing_accumulator
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        assert pending is not None
        pending_missing = int(pending[3] is not None)
        if pending_missing:
            observation_digest = stable_m5_digest(
                "m5-runtime-timing-observation-v1",
                bool_field(False),
                *(option_field(None) for _ in range(9)),
            )
            transition_timing_digest = stable_m5_digest(
                "m5-transition-call-timing-v1",
                int_field(epoch_id),
                enum_field(str(pending[0])),
                text_field(str(pending[1])),
                hash_field(str(pending[2]).strip()),
                int_field(int(pending[3])),
                hash_field(observation_digest),
            )
            connection.execute(
                """
                INSERT INTO groundloop_m5_transition_call_timing (
                    epoch_id, contribution_kind, source_id,
                    contribution_key_digest, anchor_revision,
                    required_interval_observed,
                    coordinator_non_db_non_neural_ns, neural_wall_ns,
                    postgres_roundtrip_wall_ns, external_io_wall_ns,
                    end_to_end_wall_ns, postgres_server_execution_ns,
                    postgres_lock_wait_ns, postgres_wal_bytes,
                    postgres_shared_block_reads, observation_digest,
                    transition_timing_digest
                ) VALUES (
                    %s, %s, %s, %s, %s, false,
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    %s, %s
                )
                """,
                (
                    epoch_id,
                    pending[0],
                    pending[1],
                    pending[2],
                    pending[3],
                    observation_digest,
                    transition_timing_digest,
                ),
            )
        assert (
            connection.execute(
                """
            UPDATE groundloop_epoch
            SET revision = revision + 1, structural_status = 'failed',
                semantic_status = 'failed', evaluation_state = 'failed',
                publication_mode = 'provisional', sealed_at = NULL
            WHERE epoch_id = %s AND revision = %s
            """,
                (epoch_id, revision),
            ).rowcount
            == 1
        )
        connection.execute(
            """
            UPDATE groundloop_m5_owner_pending_counter
            SET updated_revision = %s
            WHERE epoch_id = %s
            """,
            (revision + 1, epoch_id),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_answer_pending_counter
            SET updated_revision = %s
            WHERE epoch_id = %s
            """,
            (revision + 1, epoch_id),
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_runtime_epoch
            SET runtime_state = 'failed', revision = revision + 1,
                terminal_at = clock_timestamp()
            WHERE epoch_id = %s AND revision = %s
              AND runtime_state IN ('structural_committed', 'semantic_pending')
            """,
                (epoch_id, revision),
            ).rowcount
            == 1
        )
        work_accumulator_columns = (
            "work_digest",
            *WORK_COUNTER_COLUMNS,
            "updated_revision",
            "terminalized",
        )
        work_accumulator_values: tuple[object, ...] = (
            event_work_digest,
            *event_work_values,
            revision + 1,
            True,
        )
        assert (
            connection.execute(
                sql.SQL(
                    "UPDATE groundloop_m5_runtime_work_accumulator SET {} "
                    "WHERE epoch_id = %s AND updated_revision = %s "
                    "AND NOT terminalized"
                ).format(
                    sql.SQL(", ").join(
                        sql.SQL("{} = {}").format(
                            sql.Identifier(column_name), sql.Placeholder()
                        )
                        for column_name in work_accumulator_columns
                    )
                ),
                (*work_accumulator_values, epoch_id, revision),
            ).rowcount
            == 1
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_runtime_timing_accumulator
            SET coordinator_non_db_non_neural_ns =
                    coordinator_non_db_non_neural_ns + %s,
                neural_wall_ns = neural_wall_ns + %s,
                postgres_roundtrip_wall_ns = postgres_roundtrip_wall_ns + %s,
                external_io_wall_ns = external_io_wall_ns + %s,
                end_to_end_wall_ns = end_to_end_wall_ns + %s,
                postgres_server_execution_ns =
                    postgres_server_execution_ns + %s,
                postgres_lock_wait_ns = postgres_lock_wait_ns + %s,
                postgres_wal_bytes = postgres_wal_bytes + %s,
                postgres_shared_block_reads =
                    postgres_shared_block_reads + %s,
                required_expected_count = required_expected_count + %s,
                required_observed_count = required_observed_count + %s,
                required_missing_count = required_missing_count + %s,
                postgres_server_execution_expected_count =
                    postgres_server_execution_expected_count + %s,
                postgres_server_execution_observed_count =
                    postgres_server_execution_observed_count + %s,
                postgres_server_execution_missing_count =
                    postgres_server_execution_missing_count + %s,
                postgres_lock_wait_expected_count =
                    postgres_lock_wait_expected_count + %s,
                postgres_lock_wait_observed_count =
                    postgres_lock_wait_observed_count + %s,
                postgres_lock_wait_missing_count =
                    postgres_lock_wait_missing_count + %s,
                postgres_wal_bytes_expected_count =
                    postgres_wal_bytes_expected_count + %s,
                postgres_wal_bytes_observed_count =
                    postgres_wal_bytes_observed_count + %s,
                postgres_wal_bytes_missing_count =
                    postgres_wal_bytes_missing_count + %s,
                postgres_shared_block_reads_expected_count =
                    postgres_shared_block_reads_expected_count + %s,
                postgres_shared_block_reads_observed_count =
                    postgres_shared_block_reads_observed_count + %s,
                postgres_shared_block_reads_missing_count =
                    postgres_shared_block_reads_missing_count + %s,
                pending_contribution_kind = NULL,
                pending_source_id = NULL,
                pending_contribution_key_digest = NULL,
                pending_anchor_revision = NULL,
                updated_revision = %s, terminalized = true
            WHERE epoch_id = %s AND updated_revision = %s
              AND NOT terminalized
            """,
                (
                    *attempt_timing_sums,
                    terminal_attempt_count + 1,
                    attempt_required_observed,
                    attempt_required_missing + pending_missing + 1,
                    terminal_attempt_count + 1,
                    attempt_optional_observed[0],
                    terminal_attempt_count
                    - attempt_optional_observed[0]
                    + pending_missing
                    + 1,
                    terminal_attempt_count + 1,
                    attempt_optional_observed[1],
                    terminal_attempt_count
                    - attempt_optional_observed[1]
                    + pending_missing
                    + 1,
                    terminal_attempt_count + 1,
                    attempt_optional_observed[2],
                    terminal_attempt_count
                    - attempt_optional_observed[2]
                    + pending_missing
                    + 1,
                    terminal_attempt_count + 1,
                    attempt_optional_observed[3],
                    terminal_attempt_count
                    - attempt_optional_observed[3]
                    + pending_missing
                    + 1,
                    revision + 1,
                    epoch_id,
                    revision,
                ),
            ).rowcount
            == 1
        )
        timing = connection.execute(
            """
            SELECT coordinator_non_db_non_neural_ns, neural_wall_ns,
                   postgres_roundtrip_wall_ns, external_io_wall_ns,
                   end_to_end_wall_ns,
                   required_expected_count, required_observed_count,
                   required_missing_count,
                   postgres_server_execution_expected_count,
                   postgres_server_execution_observed_count,
                   postgres_server_execution_missing_count,
                   postgres_lock_wait_expected_count,
                   postgres_lock_wait_observed_count,
                   postgres_lock_wait_missing_count,
                   postgres_wal_bytes_expected_count,
                   postgres_wal_bytes_observed_count,
                   postgres_wal_bytes_missing_count,
                   postgres_shared_block_reads_expected_count,
                   postgres_shared_block_reads_observed_count,
                   postgres_shared_block_reads_missing_count
            FROM groundloop_m5_runtime_timing_accumulator
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        assert timing is not None
        _insert_runtime_work_row(
            connection,
            epoch_id=epoch_id,
            structural_event_id=structural_event_id,
            work_kind="event",
            values=event_work_values,
        )
        _insert_runtime_work_row(
            connection,
            epoch_id=epoch_id,
            structural_event_id=structural_event_id,
            work_kind="call",
            values=zero_work,
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_event_result (
                structural_event_id, payload_hash, epoch_id, outcome,
                original_open_receipt_binding_hash, publication_id,
                original_publication_receipt_binding_hash,
                event_work_kind, event_work_digest,
                combined_status_delta_set_hash, changed_state_set_hash,
                failure_reason, logical_result_hash, delta_count,
                state_reference_count, coordinator_non_db_non_neural_ns,
                neural_wall_ns, postgres_roundtrip_wall_ns,
                external_io_wall_ns, end_to_end_wall_ns,
                postgres_server_execution_ns, postgres_lock_wait_ns,
                postgres_wal_bytes, postgres_shared_block_reads
            ) VALUES (
                %s, %s, %s, 'failed', %s, NULL, NULL, 'event', %s,
                %s, %s, 'invariant_failure', %s, 0, 0,
                %s, %s, %s, %s, %s, NULL, NULL, NULL, NULL
            )
            """,
            (
                structural_event_id,
                payload_hash,
                epoch_id,
                open_receipt_hash,
                event_work_digest,
                combined_hash,
                changed_hash,
                logical_result_hash,
                *timing[:5],
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_event_timing_coverage (
                structural_event_id, epoch_id,
                required_expected_count, required_observed_count,
                required_missing_count,
                postgres_server_execution_expected_count,
                postgres_server_execution_observed_count,
                postgres_server_execution_missing_count,
                postgres_lock_wait_expected_count,
                postgres_lock_wait_observed_count,
                postgres_lock_wait_missing_count,
                postgres_wal_bytes_expected_count,
                postgres_wal_bytes_observed_count,
                postgres_wal_bytes_missing_count,
                postgres_shared_block_reads_expected_count,
                postgres_shared_block_reads_observed_count,
                postgres_shared_block_reads_missing_count,
                terminal_client_roundtrip_included
            ) VALUES (
                %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, false
            )
            """,
            (structural_event_id, epoch_id, *timing[5:]),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return logical_result_hash


def _terminalize_direct_epoch_for_audit(
    connection: Connection[Any], *, epoch_id: int, outcome: str = "failed"
) -> str:
    assert outcome in {"failed", "sealed"}
    recovery_installed = connection.execute(
        "SELECT to_regclass('groundloop_m5_runtime_work_accumulator') IS NOT NULL"
    ).fetchone()
    assert recovery_installed is not None
    if bool(recovery_installed[0]):
        assert outcome == "failed"
        return _terminalize_recovered_epoch_for_audit(connection, epoch_id=epoch_id)
    identity = connection.execute(
        """
        SELECT base.event_id, base.payload_hash, base.revision,
               runtime.runtime_state
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        WHERE base.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert identity is not None
    structural_event_id = str(identity[0])
    payload_hash = str(identity[1]).strip()
    revision = int(identity[2])
    assert str(identity[3]) in {
        "structural_committed",
        "semantic_pending",
        "semantic_complete",
    }
    zero_work = tuple(0 for _ in WORK_COUNTER_COLUMNS)
    open_receipt_hash = stable_m5_digest(
        "m5-open-event-receipt-binding-v2",
        int_field(epoch_id),
        bool_field(False),
        bool_field(False),
        option_field(None),
        bool_field(False),
        option_field(None),
    )
    combined_hash = stable_m5_digest(
        "m5-combined-status-delta-set-v2", sequence_field(())
    )
    changed_hash = stable_m5_digest("m5-changed-state-set-v2", sequence_field(()))
    publication_id = (
        stable_m4_digest("m4-publication-v1", str(epoch_id))
        if outcome == "sealed"
        else None
    )
    publication_receipt_hash = (
        stable_m5_digest(
            "m5-publication-receipt-binding-v2",
            int_field(epoch_id),
            text_field(str(publication_id)),
            bool_field(False),
        )
        if publication_id is not None
        else None
    )
    event_work_digest = _runtime_work_digest(zero_work)
    logical_result_hash = stable_m5_digest(
        "m5-event-run-logical-result-v2",
        text_field(structural_event_id),
        hash_field(payload_hash),
        int_field(epoch_id),
        enum_field(outcome),
        hash_field(open_receipt_hash),
        option_field(
            hash_field(publication_receipt_hash)
            if publication_receipt_hash is not None
            else None
        ),
        hash_field(event_work_digest),
        hash_field(combined_hash),
        hash_field(changed_hash),
        option_field(enum_field("invariant_failure") if outcome == "failed" else None),
    )
    with connection.transaction():
        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (epoch_id, revision),
        )
        _insert_runtime_work_row(
            connection,
            epoch_id=epoch_id,
            structural_event_id=structural_event_id,
            work_kind="event",
            values=zero_work,
        )
        _insert_runtime_work_row(
            connection,
            epoch_id=epoch_id,
            structural_event_id=structural_event_id,
            work_kind="call",
            values=zero_work,
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_event_result (
                structural_event_id, payload_hash, epoch_id, outcome,
                original_open_receipt_binding_hash, publication_id,
                original_publication_receipt_binding_hash,
                event_work_kind, event_work_digest,
                combined_status_delta_set_hash, changed_state_set_hash,
                failure_reason, logical_result_hash, delta_count,
                state_reference_count, coordinator_non_db_non_neural_ns,
                neural_wall_ns, postgres_roundtrip_wall_ns,
                external_io_wall_ns, end_to_end_wall_ns,
                postgres_server_execution_ns, postgres_lock_wait_ns,
                postgres_wal_bytes, postgres_shared_block_reads
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, 'event', %s,
                %s, %s, %s, %s, 0, 0,
                0, 0, 0, 0, 0, NULL, NULL, NULL, NULL
            )
            """,
            (
                structural_event_id,
                payload_hash,
                epoch_id,
                outcome,
                open_receipt_hash,
                publication_id,
                publication_receipt_hash,
                event_work_digest,
                combined_hash,
                changed_hash,
                "invariant_failure" if outcome == "failed" else None,
                logical_result_hash,
            ),
        )
        if outcome == "failed":
            base_transition = """
                UPDATE groundloop_epoch
                SET revision = revision + 1, structural_status = 'failed',
                    semantic_status = 'failed', evaluation_state = 'failed',
                    publication_mode = 'provisional', sealed_at = NULL
                WHERE epoch_id = %s AND revision = %s
            """
        else:
            base_transition = """
                UPDATE groundloop_epoch
                SET revision = revision + 1, structural_status = 'committed',
                    semantic_status = 'sealed', evaluation_state = 'complete',
                    publication_mode = 'strict', sealed_at = clock_timestamp()
                WHERE epoch_id = %s AND revision = %s
            """
        assert connection.execute(base_transition, (epoch_id, revision)).rowcount == 1
        connection.execute(
            """
            UPDATE groundloop_m5_owner_pending_counter
            SET updated_revision = %s
            WHERE epoch_id = %s
            """,
            (revision + 1, epoch_id),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_answer_pending_counter
            SET updated_revision = %s
            WHERE epoch_id = %s
            """,
            (revision + 1, epoch_id),
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_runtime_epoch
            SET runtime_state = %s, revision = revision + 1,
                terminal_at = clock_timestamp()
            WHERE epoch_id = %s AND revision = %s
              AND (
                  (%s = 'sealed' AND runtime_state = 'semantic_complete')
                  OR (%s = 'failed' AND runtime_state IN (
                      'structural_committed', 'semantic_pending',
                      'semantic_complete'
                  ))
              )
            """,
                (outcome, epoch_id, revision, outcome, outcome),
            ).rowcount
            == 1
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return logical_result_hash


def _insert_direct_dispatch_and_evidence(
    connection: Connection[Any],
    *,
    envelope_row: dict[str, Any],
    disposition: str,
    attempt_values: tuple[int, ...] | None = None,
    force_constraints: bool = True,
    insert_dispatch: bool = True,
    insert_evidence: bool = True,
    attempt_timing_observed: bool = False,
    attempt_timing_values: tuple[int | None, ...] | None = None,
) -> tuple[str, str, str]:
    epoch_id = int(envelope_row["epoch_id"])
    attempt_id = str(envelope_row["attempt_id"])
    job_id = str(envelope_row["job_id"])
    attempt = connection.execute(
        """
        SELECT attempt.attempt_ordinal, attempt.lease_expires_at, job.job_kind,
               runtime.revision
        FROM groundloop_semantic_job_attempt AS attempt
        JOIN groundloop_semantic_job AS job ON job.job_id = attempt.job_id
        JOIN groundloop_m5_runtime_epoch AS runtime
          ON runtime.epoch_id = job.epoch_id
        WHERE attempt.attempt_id = %s AND attempt.job_id = %s
          AND job.epoch_id = %s
        """,
        (attempt_id, job_id, epoch_id),
    ).fetchone()
    assert attempt is not None
    attempt_ordinal = int(attempt[0])
    lease_expires_at = attempt[1]
    job_kind = str(attempt[2])
    dispatched_revision = int(attempt[3])
    maximum_values = [0 for _ in WORK_COUNTER_COLUMNS]
    if job_kind in {"impact_discovery", "frontier_retrieve"}:
        maximum_values[3] = 1
        maximum_values[27] = 1
    else:
        assert job_kind == "verify_pair"
        maximum_values[4] = 1
        maximum_values[28] = 1
    maximum_work_digest = _runtime_work_digest(tuple(maximum_values))
    record_digest = stable_m5_digest(
        "m5-dispatch-record-v1",
        int_field(epoch_id),
        enum_field("direct"),
        text_field(attempt_id),
        text_field(job_id),
        int_field(attempt_ordinal),
        text_field(job_kind),
        bool_field(False),
        int_field(dispatched_revision),
        hash_field(maximum_work_digest),
    )
    dispatch_columns = (
        "epoch_id",
        *(f"maximum_{name}" for name in WORK_COUNTER_COLUMNS),
        "maximum_work_digest",
        "subgraph",
        "attempt_id",
        "logical_job_id",
        "attempt_ordinal",
        "job_kind",
        "fallback_required",
        "dispatched_revision",
        "lease_expires_at",
        "record_digest",
    )
    if insert_dispatch:
        connection.execute(
            sql.SQL(
                "INSERT INTO groundloop_m5_dispatch_record ({}) VALUES ({})"
            ).format(
                sql.SQL(", ").join(map(sql.Identifier, dispatch_columns)),
                sql.SQL(", ").join(sql.Placeholder() for _ in dispatch_columns),
            ),
            (
                epoch_id,
                *maximum_values,
                maximum_work_digest,
                "direct",
                attempt_id,
                job_id,
                attempt_ordinal,
                job_kind,
                False,
                dispatched_revision,
                lease_expires_at,
                record_digest,
            ),
        )
    if force_constraints and insert_dispatch:
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

    if attempt_timing_values is None:
        attempt_timing_values = (None,) * 9
    observation_digest = _timing_observation_digest(
        attempt_timing_observed,
        attempt_timing_values,
    )
    attempt_timing_digest = stable_m5_digest(
        "m5-attempt-runtime-timing-v1",
        int_field(epoch_id),
        enum_field("direct"),
        text_field(attempt_id),
        hash_field(observation_digest),
    )
    if attempt_values is None:
        attempt_values = tuple(0 for _ in WORK_COUNTER_COLUMNS)
    assert len(attempt_values) == len(WORK_COUNTER_COLUMNS)
    attempt_work_digest = _runtime_work_digest(attempt_values)
    evidence_digest = stable_m5_digest(
        "m5-attempt-execution-evidence-v1",
        int_field(epoch_id),
        enum_field("direct"),
        text_field(attempt_id),
        enum_field(disposition),
        hash_field(str(envelope_row["envelope_digest"])),
        hash_field(attempt_work_digest),
        hash_field(attempt_timing_digest),
    )
    evidence_columns = (
        "epoch_id",
        *(f"attempt_{name}" for name in WORK_COUNTER_COLUMNS),
        "attempt_work_digest",
        "subgraph",
        "attempt_id",
        "disposition",
        "result_or_error_hash",
        "attempt_timing_digest",
        "evidence_digest",
    )
    if insert_evidence:
        connection.execute(
            sql.SQL(
                "INSERT INTO groundloop_m5_attempt_execution_evidence ({}) VALUES ({})"
            ).format(
                sql.SQL(", ").join(map(sql.Identifier, evidence_columns)),
                sql.SQL(", ").join(sql.Placeholder() for _ in evidence_columns),
            ),
            (
                epoch_id,
                *attempt_values,
                attempt_work_digest,
                "direct",
                attempt_id,
                disposition,
                envelope_row["envelope_digest"],
                attempt_timing_digest,
                evidence_digest,
            ),
        )
    if force_constraints and insert_evidence:
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return evidence_digest, attempt_work_digest, attempt_timing_digest


def _work_contribution_key(
    *, epoch_id: int, contribution_kind: str, source_id: str
) -> str:
    return stable_m5_digest(
        "m5-runtime-work-contribution-key-v1",
        int_field(epoch_id),
        enum_field(contribution_kind),
        text_field(source_id),
    )


def _insert_work_contribution(
    connection: Connection[Any],
    *,
    epoch_id: int,
    contribution_kind: str,
    source_id: str,
    source_identity_hash: str,
    applied_revision: int,
    values: tuple[int, ...],
) -> str:
    work_digest = _runtime_work_digest(values)
    contribution_key_digest = _work_contribution_key(
        epoch_id=epoch_id,
        contribution_kind=contribution_kind,
        source_id=source_id,
    )
    columns = (
        "work_digest",
        "epoch_id",
        *WORK_COUNTER_COLUMNS,
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
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        (
            work_digest,
            epoch_id,
            *values,
            contribution_kind,
            source_id,
            source_identity_hash,
            contribution_key_digest,
            applied_revision,
        ),
    )
    return contribution_key_digest


def _insert_attempt_timing_contribution(
    connection: Connection[Any],
    *,
    epoch_id: int,
    attempt_id: str,
    evidence_digest: str,
    attempt_timing_digest: str,
    subgraph: str = "direct",
    required_interval_observed: bool = False,
    timing_values: tuple[int | None, ...] | None = None,
) -> None:
    if timing_values is None:
        timing_values = (None,) * 9
    observation_digest = _timing_observation_digest(
        required_interval_observed,
        timing_values,
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_runtime_timing_contribution (
            epoch_id, subgraph, attempt_id, execution_evidence_digest,
            required_interval_observed, coordinator_non_db_non_neural_ns,
            neural_wall_ns, postgres_roundtrip_wall_ns, external_io_wall_ns,
            end_to_end_wall_ns, postgres_server_execution_ns,
            postgres_lock_wait_ns, postgres_wal_bytes,
            postgres_shared_block_reads, observation_digest,
            attempt_timing_digest
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            epoch_id,
            subgraph,
            attempt_id,
            evidence_digest,
            required_interval_observed,
            *timing_values,
            observation_digest,
            attempt_timing_digest,
        ),
    )


def _insert_preterminal_accumulators(
    connection: Connection[Any],
    *,
    epoch_id: int,
    revision: int,
    attempt_values: tuple[int, ...],
    late_contribution_key: str,
    attempt_id: str,
    late_artifact_count: int = 1,
) -> None:
    accumulated_values = list(attempt_values)
    accumulated_values[16] += late_artifact_count
    work_digest = _runtime_work_digest(tuple(accumulated_values))
    work_columns = (
        "work_digest",
        "epoch_id",
        *WORK_COUNTER_COLUMNS,
        "updated_revision",
        "terminalized",
    )
    connection.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_accumulator ({}) VALUES ({})"
        ).format(
            sql.SQL(", ").join(map(sql.Identifier, work_columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in work_columns),
        ),
        (
            work_digest,
            epoch_id,
            *accumulated_values,
            revision,
            False,
        ),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_runtime_timing_accumulator (
            epoch_id,
            coordinator_non_db_non_neural_ns, neural_wall_ns,
            postgres_roundtrip_wall_ns, external_io_wall_ns,
            end_to_end_wall_ns, postgres_server_execution_ns,
            postgres_lock_wait_ns, postgres_wal_bytes,
            postgres_shared_block_reads,
            required_expected_count, required_observed_count,
            required_missing_count,
            postgres_server_execution_expected_count,
            postgres_server_execution_observed_count,
            postgres_server_execution_missing_count,
            postgres_lock_wait_expected_count,
            postgres_lock_wait_observed_count,
            postgres_lock_wait_missing_count,
            postgres_wal_bytes_expected_count,
            postgres_wal_bytes_observed_count,
            postgres_wal_bytes_missing_count,
            postgres_shared_block_reads_expected_count,
            postgres_shared_block_reads_observed_count,
            postgres_shared_block_reads_missing_count,
            pending_contribution_kind, pending_source_id,
            pending_contribution_key_digest, pending_anchor_revision,
            updated_revision, terminalized
        ) VALUES (
            %s,
            0, 0, 0, 0, 0, 0, 0, 0, 0,
            2, 0, 1,
            2, 0, 1,
            2, 0, 1,
            2, 0, 1,
            2, 0, 1,
            'preterminal_late_return', %s, %s, %s, %s, false
        )
        """,
        (epoch_id, attempt_id, late_contribution_key, revision, revision),
    )


def _insert_pending_timing_accumulator(
    connection: Connection[Any],
    *,
    epoch_id: int,
    revision: int,
    contribution_kind: str,
    source_id: str,
    contribution_key_digest: str,
) -> None:
    connection.execute(
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
            updated_revision
        ) VALUES (%s, 1, 1, 1, 1, 1, %s, %s, %s, %s, %s)
        """,
        (
            epoch_id,
            contribution_kind,
            source_id,
            contribution_key_digest,
            revision,
            revision,
        ),
    )


def _insert_preterminal_direct_return_closure(
    connection: Connection[Any],
    *,
    envelope_row: dict[str, Any],
    disposition: str = "returned",
    attempt_values: tuple[int, ...] | None = None,
    forged_late_identity: str | None = None,
    late_artifact_count: int = 1,
) -> tuple[str, str, str]:
    epoch_id = int(envelope_row["epoch_id"])
    attempt_id = str(envelope_row["attempt_id"])
    if attempt_values is None:
        attempt_values = tuple(0 for _ in WORK_COUNTER_COLUMNS)
    _insert_direct_envelope(connection, envelope_row)
    evidence_digest, work_digest, timing_digest = _insert_direct_dispatch_and_evidence(
        connection,
        envelope_row=envelope_row,
        disposition=disposition,
        attempt_values=attempt_values,
        force_constraints=False,
    )
    _insert_attempt_timing_contribution(
        connection,
        epoch_id=epoch_id,
        attempt_id=attempt_id,
        evidence_digest=evidence_digest,
        attempt_timing_digest=timing_digest,
    )
    revision_row = connection.execute(
        "SELECT revision FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s",
        (epoch_id,),
    ).fetchone()
    assert revision_row is not None
    revision = int(revision_row[0])
    _insert_work_contribution(
        connection,
        epoch_id=epoch_id,
        contribution_kind="direct_attempt_execution",
        source_id=attempt_id,
        source_identity_hash=evidence_digest,
        applied_revision=revision,
        values=attempt_values,
    )
    late_values = [0 for _ in WORK_COUNTER_COLUMNS]
    late_values[16] = late_artifact_count
    late_contribution_key = _insert_work_contribution(
        connection,
        epoch_id=epoch_id,
        contribution_kind="preterminal_late_return",
        source_id=attempt_id,
        source_identity_hash=(
            str(envelope_row["envelope_digest"])
            if forged_late_identity is None
            else forged_late_identity
        ),
        applied_revision=revision,
        values=tuple(late_values),
    )
    _insert_preterminal_accumulators(
        connection,
        epoch_id=epoch_id,
        revision=revision,
        attempt_values=attempt_values,
        late_contribution_key=late_contribution_key,
        attempt_id=attempt_id,
        late_artifact_count=late_artifact_count,
    )
    return evidence_digest, work_digest, timing_digest


def _insert_postterminal_timing(
    connection: Connection[Any],
    *,
    epoch_id: int,
    attempt_id: str,
    attempt_timing_digest: str,
    subgraph: str = "direct",
) -> None:
    observation_digest = stable_m5_digest(
        "m5-runtime-timing-observation-v1",
        bool_field(False),
        *(option_field(None) for _ in range(9)),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_post_terminal_attempt_timing (
            epoch_id, subgraph, attempt_id, required_interval_observed,
            coordinator_non_db_non_neural_ns, neural_wall_ns,
            postgres_roundtrip_wall_ns, external_io_wall_ns,
            end_to_end_wall_ns, postgres_server_execution_ns,
            postgres_lock_wait_ns, postgres_wal_bytes,
            postgres_shared_block_reads, observation_digest,
            attempt_timing_digest
        ) VALUES (
            %s, %s, %s, false,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, %s, %s
        )
        """,
        (
            epoch_id,
            subgraph,
            attempt_id,
            observation_digest,
            attempt_timing_digest,
        ),
    )


def _insert_direct_expired_return(
    connection: Connection[Any],
    *,
    envelope_row: dict[str, Any],
    execution_evidence_digest: str,
) -> str:
    epoch_id = int(envelope_row["epoch_id"])
    attempt_id = str(envelope_row["attempt_id"])
    attempt = connection.execute(
        """
        SELECT lease_token_hash, lease_expires_at
        FROM groundloop_semantic_job_attempt
        WHERE attempt_id = %s AND job_id = %s
        """,
        (attempt_id, envelope_row["job_id"]),
    ).fetchone()
    activity = connection.execute(
        "SELECT revision FROM groundloop_epoch WHERE epoch_id = %s",
        (epoch_id,),
    ).fetchone()
    assert attempt is not None
    assert activity is not None
    activity_revision = int(activity[0])
    expired_return_digest = stable_m5_digest(
        "m5-expired-attempt-return-v1",
        enum_field("direct"),
        int_field(epoch_id),
        text_field(attempt_id),
        text_field(str(envelope_row["job_id"])),
        hash_field(str(envelope_row["envelope_digest"])),
        hash_field(str(envelope_row["result_artifact_hash"])),
        int_field(epoch_id),
        int_field(activity_revision),
        enum_field("attempt_expired"),
        bool_field(True),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_expired_attempt_return (
            epoch_id, subgraph, attempt_id, logical_job_id,
            original_lease_token_hash, original_lease_expires_at,
            worker_output_digest, worker_artifact_hash,
            activity_snapshot_epoch_id, activity_snapshot_revision,
            cancellation_attribution, execution_evidence_digest,
            received_after_terminal, expired_return_digest
        ) VALUES (
            %s, 'direct', %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, true, %s
        )
        """,
        (
            epoch_id,
            attempt_id,
            envelope_row["job_id"],
            attempt[0],
            attempt[1],
            envelope_row["envelope_digest"],
            envelope_row["result_artifact_hash"],
            epoch_id,
            activity_revision,
            Jsonb(
                {
                    "cancelled_by_event_id": None,
                    "cancelled_by_epoch_id": None,
                    "cancellation_reason": None,
                }
            ),
            execution_evidence_digest,
            expired_return_digest,
        ),
    )
    return expired_return_digest


def _seed_postterminal_audit_support(
    connection: Connection[Any],
    *,
    return_kind: str,
    evidence_disposition: str = "returned",
    include_envelope: bool = False,
    include_expired_sidecar: bool = False,
    expired_successor_ordinal: int | None = 2,
    terminal_reason: str = "subject_inactive",
) -> dict[str, Any]:
    envelope_row = _seed_direct_envelope_support(
        connection,
        return_kind="discovery",
        expire_attempt=return_kind == "expired_return",
        expired_successor_ordinal=expired_successor_ordinal,
    )
    if return_kind == "terminal_audit_only":
        _cancel_direct_job_for_audit(
            connection,
            epoch_id=int(envelope_row["epoch_id"]),
            job_id=str(envelope_row["job_id"]),
            terminal_reason=terminal_reason,
        )
    _insert_direct_dispatch_and_evidence(
        connection,
        envelope_row=envelope_row,
        disposition=evidence_disposition,
        insert_evidence=False,
    )
    connection.commit()
    logical_result_hash = _terminalize_direct_epoch_for_audit(
        connection, epoch_id=int(envelope_row["epoch_id"])
    )
    connection.commit()
    evidence_digest, work_digest, timing_digest = _insert_direct_dispatch_and_evidence(
        connection,
        envelope_row=envelope_row,
        disposition=evidence_disposition,
        force_constraints=False,
        insert_dispatch=False,
    )
    if include_envelope or include_expired_sidecar:
        _insert_direct_envelope(connection, envelope_row)
    return_artifact_digest = str(envelope_row["envelope_digest"])
    if include_expired_sidecar:
        assert return_kind == "expired_return"
        return_artifact_digest = _insert_direct_expired_return(
            connection,
            envelope_row=envelope_row,
            execution_evidence_digest=evidence_digest,
        )
    _insert_postterminal_timing(
        connection,
        epoch_id=int(envelope_row["epoch_id"]),
        attempt_id=str(envelope_row["attempt_id"]),
        attempt_timing_digest=timing_digest,
    )
    return {
        "epoch_id": envelope_row["epoch_id"],
        "subgraph": "direct",
        "attempt_id": envelope_row["attempt_id"],
        "return_kind": return_kind,
        "return_artifact_digest": return_artifact_digest,
        "execution_evidence_digest": evidence_digest,
        "work_digest": work_digest,
        "timing_digest": timing_digest,
        "terminal_logical_result_hash": logical_result_hash,
    }


def _insert_postterminal_audit(
    connection: Connection[Any], row: dict[str, Any]
) -> None:
    connection.execute(
        """
        INSERT INTO groundloop_m5_post_terminal_attempt_audit (
            epoch_id, subgraph, attempt_id, return_kind,
            return_artifact_digest, execution_evidence_digest,
            work_digest, timing_digest, terminal_logical_result_hash
        ) VALUES (
            %(epoch_id)s, %(subgraph)s, %(attempt_id)s, %(return_kind)s,
            %(return_artifact_digest)s, %(execution_evidence_digest)s,
            %(work_digest)s, %(timing_digest)s,
            %(terminal_logical_result_hash)s
        )
        """,
        row,
    )


def _current_schema_name(connection: Connection[Any]) -> str:
    row = connection.execute("SELECT current_schema()").fetchone()
    assert row is not None
    schema_name = str(row[0])
    connection.commit()
    return schema_name


def _mutate_direct_attempt_identity(
    connection: Connection[Any], *, attempt_id: str, mutation: str
) -> str:
    if mutation == "delete":
        assert (
            connection.execute(
                "DELETE FROM groundloop_semantic_job_attempt WHERE attempt_id = %s",
                (attempt_id,),
            ).rowcount
            == 1
        )
        return attempt_id
    if mutation == "attempt_ordinal":
        value: object = 2
    elif mutation == "lease_expires_at":
        assert (
            connection.execute(
                """
            UPDATE groundloop_semantic_job_attempt
            SET lease_expires_at = lease_expires_at + interval '1 minute'
            WHERE attempt_id = %s
            """,
                (attempt_id,),
            ).rowcount
            == 1
        )
        return attempt_id
    else:
        assert mutation in {
            "attempt_id",
            "execution_spec_hash",
            "lease_token_hash",
        }
        value = _sha(f"forged-direct-attempt-{mutation}")
    assert (
        connection.execute(
            sql.SQL(
                "UPDATE groundloop_semantic_job_attempt "
                "SET {} = %s WHERE attempt_id = %s"
            ).format(sql.Identifier(mutation)),
            (value, attempt_id),
        ).rowcount
        == 1
    )
    return str(value) if mutation == "attempt_id" else attempt_id


def _run_direct_dispatch_insert(
    *, schema_name: str, envelope_row: dict[str, Any], started: Event
) -> Exception | None:
    try:
        with psycopg.connect(_database_url()) as connection:
            _select_schema(connection, schema_name)
            with connection.transaction():
                started.set()
                _insert_direct_dispatch_and_evidence(
                    connection,
                    envelope_row=envelope_row,
                    disposition="returned",
                    force_constraints=False,
                    insert_evidence=False,
                )
                connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    except Exception as error:
        return error
    return None


def _run_direct_attempt_identity_mutation(
    *,
    schema_name: str,
    attempt_id: str,
    column_name: str,
    new_value: object,
    started: Event,
) -> Exception | None:
    assert column_name in {
        "execution_spec_hash",
        "lease_token_hash",
        "lease_expires_at",
    }
    try:
        with psycopg.connect(_database_url()) as connection:
            _select_schema(connection, schema_name)
            with connection.transaction():
                started.set()
                changed = connection.execute(
                    sql.SQL(
                        "UPDATE groundloop_semantic_job_attempt "
                        "SET {} = %s WHERE attempt_id = %s"
                    ).format(sql.Identifier(column_name)),
                    (new_value, attempt_id),
                ).rowcount
                assert changed == 1
                connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    except Exception as error:
        return error
    return None


def _run_terminalization(
    *,
    schema_name: str,
    epoch_id: int,
    runtime_locked: Event,
    continue_after_runtime_lock: Event,
) -> Exception | None:
    try:
        with psycopg.connect(_database_url()) as connection:
            _select_schema(connection, schema_name)
            _terminalize_recovered_epoch_for_audit(
                connection,
                epoch_id=epoch_id,
                runtime_locked=runtime_locked,
                continue_after_runtime_lock=continue_after_runtime_lock,
            )
    except Exception as error:
        return error
    return None


def _run_event_accounting_insert(
    *, schema_name: str, epoch_id: int, surface: str, started: Event
) -> Exception | None:
    assert surface in {"work", "timing"}
    try:
        with psycopg.connect(_database_url()) as connection:
            _select_schema(connection, schema_name)
            with connection.transaction():
                started.set()
                if surface == "work":
                    _insert_work_contribution(
                        connection,
                        epoch_id=epoch_id,
                        contribution_kind="direct_acquisition",
                        source_id=_sha("concurrent-postterminal-dispatch"),
                        source_identity_hash=_sha("concurrent-postterminal-dispatch"),
                        applied_revision=2,
                        values=tuple(0 for _ in WORK_COUNTER_COLUMNS),
                    )
                else:
                    _insert_attempt_timing_contribution(
                        connection,
                        epoch_id=epoch_id,
                        attempt_id="concurrent-postterminal-attempt",
                        evidence_digest=_sha("concurrent-postterminal-evidence"),
                        attempt_timing_digest=_sha("concurrent-postterminal-timing"),
                    )
                connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    except Exception as error:
        return error
    return None


def _run_direct_envelope_closure_insert(
    *, schema_name: str, envelope_row: dict[str, Any], started: Event
) -> Exception | None:
    try:
        with psycopg.connect(_database_url()) as connection:
            _select_schema(connection, schema_name)
            with connection.transaction():
                started.set()
                _insert_preterminal_direct_return_closure(
                    connection,
                    envelope_row=envelope_row,
                )
                connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    except Exception as error:
        return error
    return None


def _run_base_epoch_source_mutation(
    *,
    schema_name: str,
    epoch_id: int,
    column_name: str,
    new_value: str,
    started: Event,
) -> Exception | None:
    assert column_name in {"event_id", "payload_hash"}
    try:
        with psycopg.connect(_database_url()) as connection:
            _select_schema(connection, schema_name)
            with connection.transaction():
                started.set()
                assert (
                    connection.execute(
                        sql.SQL(
                            "UPDATE groundloop_epoch SET {} = %s WHERE epoch_id = %s"
                        ).format(sql.Identifier(column_name)),
                        (new_value, epoch_id),
                    ).rowcount
                    == 1
                )
                connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    except Exception as error:
        return error
    return None


def _run_structural_open_contribution_insert(
    *,
    schema_name: str,
    epoch_id: int,
    event_id: str,
    payload_hash: str,
    started: Event,
) -> Exception | None:
    try:
        with psycopg.connect(_database_url()) as connection:
            _select_schema(connection, schema_name)
            with connection.transaction():
                started.set()
                _insert_work_contribution(
                    connection,
                    epoch_id=epoch_id,
                    contribution_kind="structural_open",
                    source_id=event_id,
                    source_identity_hash=payload_hash,
                    applied_revision=1,
                    values=tuple(0 for _ in WORK_COUNTER_COLUMNS),
                )
                connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    except Exception as error:
        return error
    return None


def _insert_requirement_successor_attempt(
    connection: Connection[Any], row: dict[str, Any]
) -> None:
    first = row["attempt"]
    assert isinstance(first, M5JobAttempt)
    successor = M5JobAttempt.build(
        logical_job_id=first.logical_job_id,
        attempt_ordinal=2,
        execution_spec_hash=first.execution_spec_hash,
        lease_token_hash=_sha(f"requirement-audit-lease:{first.logical_job_id}:2"),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_job_attempt (
            attempt_id, logical_job_id, attempt_ordinal,
            execution_spec_hash, lease_token_hash, attempt_state,
            lease_expires_at
        ) VALUES (%s, %s, 2, %s, %s, 'dispatched',
                  clock_timestamp() + interval '1 hour')
        """,
        (
            successor.attempt_id,
            successor.logical_job_id,
            successor.execution_spec_hash,
            successor.lease_token_hash,
        ),
    )


def _expire_requirement_audit_attempt(
    connection: Connection[Any], row: dict[str, Any]
) -> None:
    attempt = row["attempt"]
    assert isinstance(attempt, M5JobAttempt)
    revision = connection.execute(
        "SELECT revision FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s",
        (row["epoch_id"],),
    ).fetchone()
    assert revision is not None
    connection.commit()
    with connection.transaction():
        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (row["epoch_id"], int(revision[0])),
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_job_attempt
            SET attempt_state = 'expired', finished_at = clock_timestamp()
            WHERE attempt_id = %s AND attempt_state = 'dispatched'
            """,
                (attempt.attempt_id,),
            ).rowcount
            == 1
        )
        _insert_requirement_successor_attempt(connection, row)
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")


def _insert_requirement_preterminal_closure(
    connection: Connection[Any],
    row: dict[str, Any],
    artifact: dict[str, Any],
    *,
    late_artifact_count: int = 1,
) -> tuple[str, str, str]:
    attempt = row["attempt"]
    assert isinstance(attempt, M5JobAttempt)
    _insert_requirement_dispatch(connection, row)
    _insert_requirement_attempt_artifact(connection, artifact)
    evidence_digest, work_digest, timing_digest = _insert_requirement_evidence(
        connection, row, artifact
    )
    _insert_attempt_timing_contribution(
        connection,
        epoch_id=int(row["epoch_id"]),
        attempt_id=attempt.attempt_id,
        evidence_digest=evidence_digest,
        attempt_timing_digest=timing_digest,
        subgraph="requirement",
    )
    current = connection.execute(
        "SELECT revision FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s",
        (row["epoch_id"],),
    ).fetchone()
    assert current is not None
    revision = int(current[0])
    zero_values = tuple(0 for _ in WORK_COUNTER_COLUMNS)
    _insert_work_contribution(
        connection,
        epoch_id=int(row["epoch_id"]),
        contribution_kind="m5_attempt_execution",
        source_id=attempt.attempt_id,
        source_identity_hash=evidence_digest,
        applied_revision=revision,
        values=zero_values,
    )
    late_values = [0 for _ in WORK_COUNTER_COLUMNS]
    late_values[16] = late_artifact_count
    late_key = _insert_work_contribution(
        connection,
        epoch_id=int(row["epoch_id"]),
        contribution_kind="preterminal_late_return",
        source_id=attempt.attempt_id,
        source_identity_hash=str(artifact["attempt_result_artifact_hash"]),
        applied_revision=revision,
        values=tuple(late_values),
    )
    _insert_preterminal_accumulators(
        connection,
        epoch_id=int(row["epoch_id"]),
        revision=revision,
        attempt_values=zero_values,
        late_contribution_key=late_key,
        attempt_id=attempt.attempt_id,
        late_artifact_count=late_artifact_count,
    )
    return evidence_digest, work_digest, timing_digest


def _seed_requirement_postterminal_audit_support(
    connection: Connection[Any],
    *,
    cancellation_reason: M5TerminalReason,
    mutation: str | None = None,
) -> dict[str, Any]:
    row = _seed_requirement_audit_attempt(connection)
    _cancel_requirement_job_for_audit(
        connection, row, cancellation_reason=cancellation_reason
    )
    _insert_requirement_dispatch(connection, row)
    if mutation == "later_attempt":
        _insert_requirement_successor_attempt(connection, row)
    elif mutation == "expired_without_sidecar":
        _expire_requirement_audit_attempt(connection, row)
    else:
        assert mutation is None
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()
    logical_result_hash = _terminalize_direct_epoch_for_audit(
        connection, epoch_id=int(row["epoch_id"])
    )
    connection.commit()
    artifact = _build_requirement_audit_artifact(connection, row, epoch_active=False)
    _insert_requirement_attempt_artifact(connection, artifact)
    evidence_digest, work_digest, timing_digest = _insert_requirement_evidence(
        connection, row, artifact
    )
    attempt = row["attempt"]
    assert isinstance(attempt, M5JobAttempt)
    _insert_postterminal_timing(
        connection,
        epoch_id=int(row["epoch_id"]),
        attempt_id=attempt.attempt_id,
        attempt_timing_digest=timing_digest,
        subgraph="requirement",
    )
    return {
        "epoch_id": row["epoch_id"],
        "subgraph": "requirement",
        "attempt_id": attempt.attempt_id,
        "return_kind": "terminal_audit_only",
        "return_artifact_digest": artifact["attempt_result_artifact_hash"],
        "execution_evidence_digest": evidence_digest,
        "work_digest": work_digest,
        "timing_digest": timing_digest,
        "terminal_logical_result_hash": logical_result_hash,
    }


def test_fresh_install_has_exact_bundle_identity_and_is_atomic() -> None:
    with _isolated_015_schema() as connection:
        identity = m5_runtime_recovery_bundle_identity()
        result = install_m5_runtime_recovery_bundle(connection)
        connection.commit()

        assert result.applied
        assert result.identity == identity
        assert identity.bundle_id == M5_RUNTIME_RECOVERY_BUNDLE_ID
        assert (
            identity.migration_sha256
            == hashlib.sha256(
                M5_RUNTIME_RECOVERY_MIGRATION_PATH.read_bytes()
            ).hexdigest()
        )
        assert identity.prerequisite_sha256 == M5_ACCEPTED_RUNTIME_BUNDLE_SHA256
        assert set(RECOVERY_RELATIONS).issubset(_current_relations(connection))
        ledger = _ledger_row(connection, M5_RUNTIME_RECOVERY_BUNDLE_ID)
        assert ledger is not None
        assert ledger[:5] == (
            identity.bundle_id,
            identity.bundle_sha256,
            identity.migration_sha256,
            identity.oracle_sha256,
            identity.prerequisite_sha256,
        )


def test_populated_zero_attempt_activation_metadata_installs() -> None:
    # Importing this established fixture helper keeps the migration test about
    # the upgrade boundary rather than restating migration-014 activation DDL.
    from tests.m5.postgres_runtime.test_migration_015 import _seed_bridge_fixture

    with _isolated_015_schema() as connection:
        fixture = _seed_bridge_fixture(
            connection,
            activate=True,
            prefix="populated-zero-attempt-016",
            seed_snapshots=True,
        )
        connection.commit()
        protected_before = connection.execute(
            """
            SELECT to_jsonb(activation), to_jsonb(mode)
            FROM groundloop_m5_activation AS activation
            CROSS JOIN groundloop_runtime_mode AS mode
            """
        ).fetchall()

        result = install_m5_runtime_recovery_bundle(connection)
        connection.commit()

        assert result.applied
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_job_attempt"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT count(*) FROM groundloop_semantic_job_attempt"
        ).fetchone() == (0,)
        assert (
            connection.execute(
                """
            SELECT to_jsonb(activation), to_jsonb(mode)
            FROM groundloop_m5_activation AS activation
            CROSS JOIN groundloop_runtime_mode AS mode
            """
            ).fetchall()
            == protected_before
        )
        assert fixture.base_epoch_id > 0


def test_exact_rerun_is_ledger_first_even_after_attempt_rows_exist() -> None:
    with _isolated_015_schema() as connection:
        first = install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        _seed_fake_attempt(connection, subgraph="direct", after_016=True)
        connection.commit()

        replay = install_m5_runtime_recovery_bundle(connection)
        connection.commit()

        assert first.applied
        assert not replay.applied
        assert replay.identity == first.identity
        assert connection.execute(
            "SELECT count(*) FROM groundloop_semantic_job_attempt"
        ).fetchone() == (1,)


def test_exact_rerun_is_ledger_first_after_post_016_terminal_history() -> None:
    with _isolated_015_schema() as connection:
        first = install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        database = _prepare_direct_runtime(connection)
        epoch_id = _open_alternate_runtime_header(connection, database)
        connection.commit()
        _terminalize_direct_epoch_for_audit(connection, epoch_id=epoch_id)
        connection.commit()

        replay = install_m5_runtime_recovery_bundle(connection)
        connection.commit()

        assert first.applied
        assert not replay.applied
        assert replay.identity == first.identity
        assert connection.execute(
            "SELECT runtime_state FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s",
            (epoch_id,),
        ).fetchone() == ("failed",)


def test_root_result_contribution_accepts_exact_persisted_counts() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        staged = _stage_requirement_root_for_recovery_test(connection)
        values = [0 for _ in WORK_COUNTER_COLUMNS]
        values[17] = int(staged["channel_hit_count"])
        values[18] = int(staged["selection_count"])

        with connection.transaction():
            _insert_work_contribution(
                connection,
                epoch_id=int(staged["epoch_id"]),
                contribution_kind="root_result_stage",
                source_id=str(staged["attempt_id"]),
                source_identity_hash=str(staged["attempt_output_digest"]),
                applied_revision=int(staged["revision"]),
                values=tuple(values),
            )
            connection.execute(
                "SET CONSTRAINTS groundloop_m5_work_contribution_shape IMMEDIATE"
            )


@pytest.mark.parametrize("counter_index", (17, 18))
def test_root_result_contribution_rejects_wrong_persisted_count(
    counter_index: int,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        staged = _stage_requirement_root_for_recovery_test(connection)
        values = [0 for _ in WORK_COUNTER_COLUMNS]
        values[17] = int(staged["channel_hit_count"])
        values[18] = int(staged["selection_count"])
        values[counter_index] += 1

        _insert_work_contribution(
            connection,
            epoch_id=int(staged["epoch_id"]),
            contribution_kind="root_result_stage",
            source_id=str(staged["attempt_id"]),
            source_identity_hash=str(staged["attempt_output_digest"]),
            applied_revision=int(staged["revision"]),
            values=tuple(values),
        )
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="root-result contribution lacks its attempt output",
        ):
            connection.execute(
                "SET CONSTRAINTS groundloop_m5_work_contribution_shape IMMEDIATE"
            )
        connection.rollback()


def test_root_barrier_contribution_accepts_exact_completed_root_set() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        closed = _close_requirement_roots_for_recovery_test(connection)
        values = [0 for _ in WORK_COUNTER_COLUMNS]
        values[19] = int(closed["admitted_pair_count"])

        with connection.transaction():
            _insert_work_contribution(
                connection,
                epoch_id=int(closed["epoch_id"]),
                contribution_kind="root_barrier",
                source_id=str(closed["structural_event_id"]),
                source_identity_hash=str(closed["barrier"].barrier_completion_hash),
                applied_revision=int(closed["revision"]),
                values=tuple(values),
            )
            connection.execute(
                "SET CONSTRAINTS groundloop_m5_work_contribution_shape IMMEDIATE"
            )


def test_root_barrier_contribution_rejects_wrong_admitted_pair_count() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        closed = _close_requirement_roots_for_recovery_test(connection)
        values = [0 for _ in WORK_COUNTER_COLUMNS]
        values[19] = int(closed["admitted_pair_count"]) + 1

        _insert_work_contribution(
            connection,
            epoch_id=int(closed["epoch_id"]),
            contribution_kind="root_barrier",
            source_id=str(closed["structural_event_id"]),
            source_identity_hash=str(closed["barrier"].barrier_completion_hash),
            applied_revision=int(closed["revision"]),
            values=tuple(values),
        )
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="root-barrier contribution lacks its exact completion",
        ):
            connection.execute(
                "SET CONSTRAINTS groundloop_m5_work_contribution_shape IMMEDIATE"
            )
        connection.rollback()


def test_root_barrier_contribution_rejects_zero_root_runtime() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        database = _prepare_direct_runtime(connection)
        epoch_id = _open_alternate_runtime_header(connection, database)
        source = connection.execute(
            "SELECT structural_event_id FROM groundloop_m5_runtime_epoch "
            "WHERE epoch_id = %s",
            (epoch_id,),
        ).fetchone()
        assert source is not None
        digest = connection.execute(
            "SELECT groundloop_m5_recovery_root_barrier_digest(%s, %s)",
            (epoch_id, source[0]),
        ).fetchone()
        assert digest is not None

        with pytest.raises(
            psycopg.errors.RaiseException,
            match="root-barrier contribution lacks its exact completion",
        ):
            _insert_work_contribution(
                connection,
                epoch_id=epoch_id,
                contribution_kind="root_barrier",
                source_id=str(source[0]),
                source_identity_hash=str(digest[0]).strip(),
                applied_revision=1,
                values=tuple(0 for _ in WORK_COUNTER_COLUMNS),
            )
            connection.execute(
                "SET CONSTRAINTS groundloop_m5_work_contribution_shape IMMEDIATE"
            )
        connection.rollback()


def test_root_barrier_contribution_rejects_partial_root_set() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        database = _prepare_requirement_runtime_for_recovery_test(connection)
        epoch_id, roots = _legacy_open_overlap_event(database)
        _scope, root = roots[0]
        _, lease = _acquire_requirement_job_for_recovery_test(
            connection,
            epoch_id=epoch_id,
            expected_revision=1,
            job=root,
        )
        pair = SemanticPairKey(
            SubjectKind.REQUIREMENT,
            database.group.requirements[0].requirement_version_id,
            database.base.chunk_ids[0],
        )
        staged, _ = _stage_legacy_requirement_root(
            connection,
            epoch_id=epoch_id,
            expected_revision=2,
            lease=lease,
            job=root,
            result=_legacy_requirement_result(
                epoch_id=epoch_id, root=root, pairs=(pair,)
            ),
        )
        source = connection.execute(
            "SELECT structural_event_id FROM groundloop_m5_runtime_epoch "
            "WHERE epoch_id = %s",
            (epoch_id,),
        ).fetchone()
        assert source is not None
        digest = connection.execute(
            "SELECT groundloop_m5_recovery_root_barrier_digest(%s, %s)",
            (epoch_id, source[0]),
        ).fetchone()
        assert digest is not None

        _insert_work_contribution(
            connection,
            epoch_id=epoch_id,
            contribution_kind="root_barrier",
            source_id=str(source[0]),
            source_identity_hash=str(digest[0]).strip(),
            applied_revision=int(staged.resulting_revision),
            values=tuple(0 for _ in WORK_COUNTER_COLUMNS),
        )
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="root-barrier contribution lacks its exact completion",
        ):
            connection.execute(
                "SET CONSTRAINTS groundloop_m5_work_contribution_shape IMMEDIATE"
            )
        connection.rollback()


def test_root_barrier_contribution_rejects_orphan_root_job() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        closed = _close_requirement_roots_for_recovery_test(connection)
        database = closed["database"]
        runtime = connection.execute(
            """
            SELECT structural_event_id, requirement_registry_snapshot_digest,
                   active_chunk_snapshot_digest
            FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
            """,
            (closed["epoch_id"],),
        ).fetchone()
        assert runtime is not None
        orphan_scope = M5DiscoveryScopeContract.build(
            direction=M5DiscoveryDirection.REVERSE_CHUNK,
            requirement_version_id=None,
            inserted_chunk_version_id=database.base.chunk_ids[1],
            candidate_policy_id=database.manifest.candidate_policy_id,
            requirement_registry_snapshot_digest=str(runtime[1]).strip(),
            active_chunk_snapshot_digest=str(runtime[2]).strip(),
        )
        orphan = M5LogicalJobSpec.build(
            structural_event_id=str(runtime[0]),
            job_kind=M5JobKind.REVERSE_REQUIREMENT_DISCOVERY,
            manifest=database.manifest,
            scope=orphan_scope,
        )
        values = [0 for _ in WORK_COUNTER_COLUMNS]
        values[19] = int(closed["admitted_pair_count"])

        connection.execute(
            """
            INSERT INTO groundloop_m5_semantic_job (
                logical_job_id, epoch_id, structural_event_id, job_kind,
                candidate_policy_id, candidate_policy_manifest_hash,
                scope_contract_digest, requirement_registry_snapshot_digest,
                active_chunk_snapshot_digest, role_template_hash,
                execution_spec_hash, expandable, payload_hash, job_state,
                created_revision
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      true, %s, 'declared', 1)
            """,
            (
                orphan.logical_job_id,
                closed["epoch_id"],
                orphan.structural_event_id,
                orphan.job_kind.value,
                orphan.candidate_policy_id,
                orphan.candidate_policy_manifest_hash,
                orphan.scope_contract_digest,
                orphan.requirement_registry_snapshot_digest,
                orphan.active_chunk_snapshot_digest,
                orphan.role_template_hash,
                orphan.execution_spec_hash,
                orphan.payload_hash,
            ),
        )
        _insert_work_contribution(
            connection,
            epoch_id=int(closed["epoch_id"]),
            contribution_kind="root_barrier",
            source_id=str(closed["structural_event_id"]),
            source_identity_hash=str(closed["barrier"].barrier_completion_hash),
            applied_revision=int(closed["revision"]),
            values=tuple(values),
        )
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="root-barrier contribution lacks its exact completion",
        ):
            connection.execute(
                "SET CONSTRAINTS groundloop_m5_work_contribution_shape IMMEDIATE"
            )
        connection.rollback()


def test_root_barrier_contribution_rejects_applied_revision_drift() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        closed = _close_requirement_roots_for_recovery_test(connection)
        old_revision = int(closed["revision"])
        new_revision = old_revision + 1
        values = [0 for _ in WORK_COUNTER_COLUMNS]
        values[19] = int(closed["admitted_pair_count"])

        with connection.transaction():
            connection.execute(
                "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
                (closed["epoch_id"], old_revision),
            )
            for relation in (
                "groundloop_m5_owner_pending_counter",
                "groundloop_m5_answer_pending_counter",
            ):
                connection.execute(
                    sql.SQL(
                        "UPDATE {} SET updated_revision = %s WHERE epoch_id = %s"
                    ).format(sql.Identifier(relation)),
                    (new_revision, closed["epoch_id"]),
                )
            connection.execute(
                "UPDATE groundloop_epoch SET revision = %s "
                "WHERE epoch_id = %s AND revision = %s",
                (new_revision, closed["epoch_id"], old_revision),
            )
            connection.execute(
                "UPDATE groundloop_m5_runtime_epoch SET revision = %s "
                "WHERE epoch_id = %s AND revision = %s",
                (new_revision, closed["epoch_id"], old_revision),
            )
        _insert_work_contribution(
            connection,
            epoch_id=int(closed["epoch_id"]),
            contribution_kind="root_barrier",
            source_id=str(closed["structural_event_id"]),
            source_identity_hash=str(closed["barrier"].barrier_completion_hash),
            applied_revision=new_revision,
            values=tuple(values),
        )
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="root-barrier contribution lacks its exact completion",
        ):
            connection.execute(
                "SET CONSTRAINTS groundloop_m5_work_contribution_shape IMMEDIATE"
            )
        connection.rollback()


def test_verifier_completion_contribution_accepts_exact_result_artifact() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        completed = _complete_requirement_verifier_for_recovery_test(connection)
        values = [0 for _ in WORK_COUNTER_COLUMNS]
        values[12] = 1
        values[13] = 1

        with connection.transaction():
            _insert_work_contribution(
                connection,
                epoch_id=int(completed["epoch_id"]),
                contribution_kind="verifier_completion",
                source_id=str(completed["attempt_id"]),
                source_identity_hash=str(completed["attempt_result_hash"]),
                applied_revision=int(completed["revision"]),
                values=tuple(values),
            )
            connection.execute(
                "SET CONSTRAINTS groundloop_m5_work_contribution_shape IMMEDIATE"
            )


@pytest.mark.parametrize(
    "mutation",
    (
        "missing",
        "mismatch",
        "wrong_job_kind",
        "missing_artifact_count",
        "missing_effective_count",
        "wrong_inactive_count",
    ),
)
def test_verifier_completion_contribution_rejects_inexact_source_or_work(
    mutation: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        completed = _complete_requirement_verifier_for_recovery_test(connection)
        values = [0 for _ in WORK_COUNTER_COLUMNS]
        values[12] = 1
        values[13] = 1
        if mutation == "missing":
            source_id = _sha("missing-verifier-completion-attempt")
            source_identity_hash = _sha("missing-verifier-completion-result")
        elif mutation == "mismatch":
            source_id = str(completed["attempt_id"])
            source_identity_hash = _sha("mismatched-verifier-completion-result")
        else:
            source_id = str(completed["attempt_id"])
            source_identity_hash = str(completed["attempt_result_hash"])
            if mutation == "wrong_job_kind":
                source_id = str(completed["root_attempt_id"])
                source_identity_hash = str(completed["root_attempt_result_hash"])
            elif mutation == "missing_artifact_count":
                values[12] = 0
            elif mutation == "missing_effective_count":
                values[13] = 0
            else:
                assert mutation == "wrong_inactive_count"
                values[14] = 1

        _insert_work_contribution(
            connection,
            epoch_id=int(completed["epoch_id"]),
            contribution_kind="verifier_completion",
            source_id=source_id,
            source_identity_hash=source_identity_hash,
            applied_revision=int(completed["revision"]),
            values=tuple(values),
        )
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="verifier contribution lacks its result artifact",
        ):
            connection.execute(
                "SET CONSTRAINTS groundloop_m5_work_contribution_shape IMMEDIATE"
            )
        connection.rollback()


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("bundle_id", "wrong-m5-runtime-schema-bundle-v2"),
        ("migration_sha256", _sha("wrong-015-migration")),
        ("bundle_sha256", _sha("wrong-015-bundle")),
        ("oracle_sha256", _sha("wrong-015-oracle")),
        ("prerequisite_sha256", _sha("wrong-015-prerequisite")),
    ),
)
def test_every_accepted_015_ledger_field_is_pinned(
    field_name: str, replacement: str
) -> None:
    with _isolated_015_schema() as connection:
        connection.execute(
            "ALTER TABLE groundloop_m5_schema_bundle DISABLE TRIGGER "
            "groundloop_m5_schema_bundle_immutable"
        )
        connection.execute(
            sql.SQL(
                "UPDATE groundloop_m5_schema_bundle SET {} = %s WHERE bundle_id = %s"
            ).format(sql.Identifier(field_name)),
            (replacement, M5_ACCEPTED_RUNTIME_BUNDLE_ID),
        )
        connection.execute(
            "ALTER TABLE groundloop_m5_schema_bundle ENABLE TRIGGER "
            "groundloop_m5_schema_bundle_immutable"
        )
        connection.commit()

        with pytest.raises(M5PrerequisiteError, match="five-field"):
            install_m5_runtime_recovery_bundle(connection)
        connection.rollback()

        assert set(RECOVERY_RELATIONS).isdisjoint(_current_relations(connection))
        assert _ledger_row(connection, M5_RUNTIME_RECOVERY_BUNDLE_ID) is None


def test_same_bundle_id_with_different_content_conflicts_without_writes() -> None:
    with _isolated_015_schema() as connection:
        installed = install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        ledger_before = _ledger_row(connection, M5_RUNTIME_RECOVERY_BUNDLE_ID)
        relations_before = _current_relations(connection)

        conflicting_source = M5_RUNTIME_RECOVERY_MIGRATION_PATH.read_bytes() + (
            b"\n-- same-ID different-content probe\n"
        )
        with pytest.raises(M5BundleHashConflictError, match="different content"):
            install_m5_runtime_recovery_bundle(
                connection,
                migration_bytes=conflicting_source,
            )
        connection.rollback()

        assert installed.applied
        assert _ledger_row(connection, M5_RUNTIME_RECOVERY_BUNDLE_ID) == ledger_before
        assert _current_relations(connection) == relations_before


@pytest.mark.parametrize("failure_point", FAILURE_POINTS)
def test_every_migration_group_failure_rolls_back_schema_and_replacements(
    failure_point: str,
) -> None:
    with _isolated_015_schema() as connection:
        catalog_before = _attempt_result_catalog(connection)

        def inject(point: str) -> None:
            if point == failure_point:
                raise RuntimeError(f"injected-{failure_point}")

        with pytest.raises(RuntimeError, match=f"injected-{failure_point}"):
            install_m5_runtime_recovery_bundle(
                connection,
                failure_injector=inject,
            )
        connection.rollback()

        assert set(RECOVERY_RELATIONS).isdisjoint(_current_relations(connection))
        assert (
            connection.execute(
                """
            SELECT attname
            FROM pg_attribute
            WHERE attrelid = 'groundloop_m5_job_attempt'::regclass
              AND attname IN ('lease_expires_at', 'attempt_work_digest')
              AND NOT attisdropped
            """
            ).fetchall()
            == []
        )
        assert _ledger_row(connection, M5_RUNTIME_RECOVERY_BUNDLE_ID) is None
        assert _attempt_result_catalog(connection) == catalog_before


@pytest.mark.parametrize("subgraph", ("requirement", "direct"))
def test_first_install_attempt_family_rejection_precedes_terminal_history(
    subgraph: str,
) -> None:
    with _isolated_015_schema() as connection:
        epoch_id = _seed_fake_attempt(connection, subgraph=subgraph, after_016=False)
        connection.commit()
        _terminalize_direct_epoch_for_audit(connection, epoch_id=epoch_id)
        connection.commit()

        expected = (
            "existing M5 attempt" if subgraph == "requirement" else "typed-direct"
        )
        with pytest.raises(M5RuntimeRecoveryBundleError, match=expected):
            install_m5_runtime_recovery_bundle(connection)
        connection.rollback()

        assert set(RECOVERY_RELATIONS).isdisjoint(_current_relations(connection))
        assert _ledger_row(connection, M5_RUNTIME_RECOVERY_BUNDLE_ID) is None


@pytest.mark.parametrize(
    ("semantic_status", "evaluation_state"),
    (("pending", "pending"), ("complete", "complete")),
)
def test_first_install_rejects_committed_live_semantic_statuses(
    semantic_status: str,
    evaluation_state: str,
) -> None:
    with _isolated_015_schema() as connection:
        connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (
                %s, %s, 0, 'committed', %s, %s, 'provisional', NULL
            )
            """,
            (
                f"live-{semantic_status}-before-016",
                _sha(f"live-{semantic_status}-before-016"),
                semantic_status,
                evaluation_state,
            ),
        )
        connection.commit()

        with pytest.raises(M5RuntimeRecoveryBundleError, match="pending/complete"):
            install_m5_runtime_recovery_bundle(connection)
        connection.rollback()

        assert set(RECOVERY_RELATIONS).isdisjoint(_current_relations(connection))
        assert _ledger_row(connection, M5_RUNTIME_RECOVERY_BUNDLE_ID) is None


@pytest.mark.parametrize(
    ("semantic_status", "evaluation_state", "sealed"),
    (
        ("degraded", "degraded", False),
        ("failed", "failed", False),
        ("sealed", "complete", True),
    ),
)
def test_first_install_accepts_terminal_base_epoch_without_m5_history(
    semantic_status: str,
    evaluation_state: str,
    sealed: bool,
) -> None:
    with _isolated_015_schema() as connection:
        connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (
                %s, %s, 0, 'committed', %s, %s, 'strict',
                CASE WHEN %s THEN clock_timestamp() ELSE NULL END
            )
            """,
            (
                f"terminal-{semantic_status}-before-016",
                _sha(f"terminal-{semantic_status}-before-016"),
                semantic_status,
                evaluation_state,
                sealed,
            ),
        )
        connection.commit()

        result = install_m5_runtime_recovery_bundle(connection)
        connection.commit()

        assert result.applied
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_job_attempt"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT count(*) FROM groundloop_semantic_job_attempt"
        ).fetchone() == (0,)


@pytest.mark.parametrize("outcome", ("failed", "sealed"))
def test_first_install_rejects_legacy_terminal_m5_history_atomically(
    outcome: str,
) -> None:
    with _isolated_015_schema() as connection:
        database = _prepare_direct_runtime(connection)
        epoch_id = _open_alternate_runtime_header(connection, database)
        connection.commit()
        if outcome == "sealed":
            for expected_revision, runtime_state, semantic_status in (
                (1, "semantic_pending", "pending"),
                (2, "semantic_complete", "complete"),
            ):
                resulting_revision = expected_revision + 1
                with connection.transaction():
                    connection.execute(
                        "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
                        (epoch_id, expected_revision),
                    )
                    assert (
                        connection.execute(
                            """
                        UPDATE groundloop_epoch
                        SET revision = %s, semantic_status = %s,
                            evaluation_state = %s
                        WHERE epoch_id = %s AND revision = %s
                        """,
                            (
                                resulting_revision,
                                semantic_status,
                                semantic_status,
                                epoch_id,
                                expected_revision,
                            ),
                        ).rowcount
                        == 1
                    )
                    connection.execute(
                        """
                        UPDATE groundloop_m5_owner_pending_counter
                        SET updated_revision = %s WHERE epoch_id = %s
                        """,
                        (resulting_revision, epoch_id),
                    )
                    connection.execute(
                        """
                        UPDATE groundloop_m5_answer_pending_counter
                        SET updated_revision = %s WHERE epoch_id = %s
                        """,
                        (resulting_revision, epoch_id),
                    )
                    assert (
                        connection.execute(
                            """
                        UPDATE groundloop_m5_runtime_epoch
                        SET revision = %s, runtime_state = %s
                        WHERE epoch_id = %s AND revision = %s
                        """,
                            (
                                resulting_revision,
                                runtime_state,
                                epoch_id,
                                expected_revision,
                            ),
                        ).rowcount
                        == 1
                    )
                    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        _terminalize_direct_epoch_for_audit(
            connection,
            epoch_id=epoch_id,
            outcome=outcome,
        )
        connection.commit()
        legacy_before = connection.execute(
            """
            SELECT to_jsonb(runtime), to_jsonb(result), to_jsonb(event_work)
            FROM groundloop_m5_runtime_epoch AS runtime
            JOIN groundloop_m5_event_result AS result USING (epoch_id)
            JOIN groundloop_m5_runtime_work AS event_work
              ON event_work.epoch_id = runtime.epoch_id
             AND event_work.work_kind = 'event'
            WHERE runtime.epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        assert legacy_before is not None
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_job_attempt"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT count(*) FROM groundloop_semantic_job_attempt"
        ).fetchone() == (0,)

        with pytest.raises(
            M5RuntimeRecoveryBundleError,
            match="legacy terminal M5 history",
        ):
            install_m5_runtime_recovery_bundle(connection)
        connection.rollback()

        assert set(RECOVERY_RELATIONS).isdisjoint(_current_relations(connection))
        assert _ledger_row(connection, M5_RUNTIME_RECOVERY_BUNDLE_ID) is None
        assert (
            connection.execute(
                """
            SELECT to_jsonb(runtime), to_jsonb(result), to_jsonb(event_work)
            FROM groundloop_m5_runtime_epoch AS runtime
            JOIN groundloop_m5_event_result AS result USING (epoch_id)
            JOIN groundloop_m5_runtime_work AS event_work
              ON event_work.epoch_id = runtime.epoch_id
             AND event_work.work_kind = 'event'
            WHERE runtime.epoch_id = %s
            """,
                (epoch_id,),
            ).fetchone()
            == legacy_before
        )


def test_operational_config_digest_is_reusable_across_epochs() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        database = _prepare_direct_runtime(connection)
        first = _open_legacy_direct_epoch(database)
        _terminalize_direct_epoch_for_audit(
            connection,
            epoch_id=first.epoch_id,
        )
        connection.commit()
        second_epoch_id = _open_alternate_runtime_header(connection, database)
        connection.commit()
        config_digest = stable_m5_digest(
            "m5-runtime-operational-config-v1",
            int_field(30_000),
        )

        connection.execute(
            """
            INSERT INTO groundloop_m5_runtime_operational_config (
                epoch_id, lease_duration_ms, config_digest
            ) VALUES (%s, 30000, %s), (%s, 30000, %s)
            """,
            (first.epoch_id, config_digest, second_epoch_id, config_digest),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()

        assert connection.execute(
            """
            SELECT count(*), count(DISTINCT config_digest)
            FROM groundloop_m5_runtime_operational_config
            WHERE epoch_id IN (%s, %s)
            """,
            (first.epoch_id, second_epoch_id),
        ).fetchone() == (2, 1)


def test_migration_016_preserves_original_durable_m5_attempt_delete_rejection() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_requirement_audit_attempt(connection)
        attempt = row["attempt"]
        assert isinstance(attempt, M5JobAttempt)

        with pytest.raises(
            psycopg.errors.RaiseException,
            match="durable M5 job attempt cannot be deleted",
        ):
            with connection.transaction():
                connection.execute(
                    "DELETE FROM groundloop_m5_job_attempt WHERE attempt_id = %s",
                    (attempt.attempt_id,),
                )

        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_job_attempt WHERE attempt_id = %s",
            (attempt.attempt_id,),
        ).fetchone() == (1,)


@pytest.mark.parametrize(
    "mutation",
    (
        "attempt_id",
        "attempt_ordinal",
        "execution_spec_hash",
        "lease_token_hash",
        "lease_expires_at",
        "delete",
    ),
)
def test_dispatch_bound_direct_attempt_rejects_identity_or_deadline_mutation(
    mutation: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="discovery",
            expire_attempt=False,
        )
        connection.commit()
        attempt_id = str(row["attempt_id"])
        original = connection.execute(
            """
            SELECT attempt_id, job_id, execution_spec_hash,
                   attempt_ordinal, lease_token_hash, lease_expires_at
            FROM groundloop_semantic_job_attempt
            WHERE attempt_id = %s
            """,
            (attempt_id,),
        ).fetchone()
        assert original is not None
        connection.commit()
        with connection.transaction():
            _insert_direct_dispatch_and_evidence(
                connection,
                envelope_row=row,
                disposition="returned",
                insert_evidence=False,
            )

        with pytest.raises(
            (psycopg.errors.RaiseException, psycopg.errors.ForeignKeyViolation),
            match=(
                "M5 dispatch lost its exact applicable attempt binding"
                "|violates foreign key constraint"
            ),
        ):
            with connection.transaction():
                _mutate_direct_attempt_identity(
                    connection,
                    attempt_id=attempt_id,
                    mutation=mutation,
                )
                connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

        assert (
            connection.execute(
                """
            SELECT attempt_id, job_id, execution_spec_hash,
                   attempt_ordinal, lease_token_hash, lease_expires_at
            FROM groundloop_semantic_job_attempt
            WHERE attempt_id = %s
            """,
                (attempt_id,),
            ).fetchone()
            == original
        )


def test_dispatch_bound_direct_attempt_cannot_move_to_another_valid_job_identity() -> (
    None
):
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="verifier",
            expire_attempt=False,
        )
        connection.commit()
        attempt_id = str(row["attempt_id"])
        with connection.transaction():
            _insert_direct_dispatch_and_evidence(
                connection,
                envelope_row=row,
                disposition="returned",
                insert_evidence=False,
            )
        other_job = connection.execute(
            """
            SELECT job_id, execution_spec_hash
            FROM groundloop_semantic_job
            WHERE epoch_id = %s AND parent_job_id IS NULL
              AND job_id <> %s
            """,
            (row["epoch_id"], row["job_id"]),
        ).fetchone()
        assert other_job is not None
        other_job_id = str(other_job[0])
        other_execution_spec_hash = str(other_job[1]).strip()
        moved_attempt_id = stable_m4_digest("m4-job-attempt-v1", other_job_id, "2")
        moved_lease_token = stable_m4_digest("m4-lease-token-v1", other_job_id, "2")
        connection.commit()

        with pytest.raises(
            psycopg.errors.RaiseException,
            match="M5 dispatch lost its exact applicable attempt binding",
        ):
            with connection.transaction():
                assert (
                    connection.execute(
                        """
                    UPDATE groundloop_semantic_job_attempt
                    SET attempt_id = %s, job_id = %s,
                        execution_spec_hash = %s, attempt_ordinal = 2,
                        lease_token_hash = %s
                    WHERE attempt_id = %s
                    """,
                        (
                            moved_attempt_id,
                            other_job_id,
                            other_execution_spec_hash,
                            moved_lease_token,
                            attempt_id,
                        ),
                    ).rowcount
                    == 1
                )
                connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

        assert connection.execute(
            "SELECT job_id FROM groundloop_semantic_job_attempt WHERE attempt_id = %s",
            (attempt_id,),
        ).fetchone() == (row["job_id"],)


@pytest.mark.parametrize(
    "mutation",
    ("attempt_id", "attempt_ordinal", "execution_spec_hash", "lease_token_hash"),
)
def test_direct_dispatch_rejects_preexisting_noncanonical_attempt_identity(
    mutation: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="discovery",
            expire_attempt=False,
        )
        connection.commit()
        with connection.transaction():
            current_attempt_id = _mutate_direct_attempt_identity(
                connection,
                attempt_id=str(row["attempt_id"]),
                mutation=mutation,
            )
        mutated_row = deepcopy(row)
        mutated_row["attempt_id"] = current_attempt_id

        with pytest.raises(
            psycopg.errors.RaiseException,
            match="M5 dispatch does not lock its exact typed-direct attempt",
        ):
            with connection.transaction():
                _insert_direct_dispatch_and_evidence(
                    connection,
                    envelope_row=mutated_row,
                    disposition="returned",
                    insert_evidence=False,
                )

        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_dispatch_record"
        ).fetchone() == (0,)


@pytest.mark.parametrize(
    "column_name",
    ("execution_spec_hash", "lease_token_hash", "lease_expires_at"),
)
@pytest.mark.parametrize("first_writer", ("dispatch", "mutation"))
def test_direct_dispatch_and_identity_mutation_serialize_in_both_lock_orders(
    column_name: str,
    first_writer: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="discovery",
            expire_attempt=False,
        )
        schema_name = _current_schema_name(connection)
        attempt_id = str(row["attempt_id"])
        if column_name == "lease_expires_at":
            deadline = connection.execute(
                "SELECT lease_expires_at FROM groundloop_semantic_job_attempt "
                "WHERE attempt_id = %s",
                (attempt_id,),
            ).fetchone()
            assert deadline is not None
            original_value: object = deadline[0]
            forged_value: object = deadline[0] + timedelta(minutes=2)
            connection.commit()
        else:
            original_value = str(row["attempt_binding"][column_name])
            forged_value = _sha(f"concurrent-direct-{column_name}")
        started = Event()
        executor = ThreadPoolExecutor(max_workers=1)
        try:
            if first_writer == "dispatch":
                with connection.transaction():
                    _insert_direct_dispatch_and_evidence(
                        connection,
                        envelope_row=row,
                        disposition="returned",
                        force_constraints=False,
                        insert_evidence=False,
                    )
                    future = executor.submit(
                        _run_direct_attempt_identity_mutation,
                        schema_name=schema_name,
                        attempt_id=attempt_id,
                        column_name=column_name,
                        new_value=forged_value,
                        started=started,
                    )
                    assert started.wait(timeout=5)
                    with pytest.raises(FutureTimeoutError):
                        future.result(timeout=0.25)
                    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
                result = future.result(timeout=10)
                assert isinstance(result, psycopg.errors.RaiseException)
                assert "M5 dispatch lost its exact applicable attempt binding" in str(
                    result
                )
                expected_value = original_value
                expected_dispatch_count = 1
            else:
                with connection.transaction():
                    assert (
                        connection.execute(
                            sql.SQL(
                                "UPDATE groundloop_semantic_job_attempt "
                                "SET {} = %s WHERE attempt_id = %s"
                            ).format(sql.Identifier(column_name)),
                            (forged_value, attempt_id),
                        ).rowcount
                        == 1
                    )
                    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
                    future = executor.submit(
                        _run_direct_dispatch_insert,
                        schema_name=schema_name,
                        envelope_row=row,
                        started=started,
                    )
                    assert started.wait(timeout=5)
                    with pytest.raises(FutureTimeoutError):
                        future.result(timeout=0.25)
                result = future.result(timeout=10)
                assert isinstance(result, psycopg.errors.RaiseException)
                assert (
                    "M5 dispatch does not lock its exact typed-direct attempt"
                    in str(result)
                )
                expected_value = forged_value
                expected_dispatch_count = 0
        finally:
            executor.shutdown(wait=True, cancel_futures=True)

        assert connection.execute(
            sql.SQL(
                "SELECT {} FROM groundloop_semantic_job_attempt WHERE attempt_id = %s"
            ).format(
                sql.SQL("btrim({})").format(sql.Identifier(column_name))
                if column_name != "lease_expires_at"
                else sql.Identifier(column_name)
            ),
            (attempt_id,),
        ).fetchone() == (expected_value,)
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_dispatch_record "
            "WHERE subgraph = 'direct' AND attempt_id = %s",
            (attempt_id,),
        ).fetchone() == (expected_dispatch_count,)


@pytest.mark.parametrize("return_kind", ("discovery", "verifier"))
def test_typed_direct_late_envelope_accepts_exact_branch_closure(
    return_kind: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind=return_kind,
            expire_attempt=False,
        )
        _cancel_direct_job_for_audit(
            connection,
            epoch_id=int(row["epoch_id"]),
            job_id=str(row["job_id"]),
        )

        _insert_preterminal_direct_return_closure(
            connection,
            envelope_row=row,
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()

        assert connection.execute(
            """
            SELECT return_kind, envelope_digest
            FROM groundloop_m5_typed_direct_late_return_envelope
            WHERE epoch_id = %s AND attempt_id = %s
            """,
            (row["epoch_id"], row["attempt_id"]),
        ).fetchone() == (return_kind, row["envelope_digest"])
        assert connection.execute(
            """
            SELECT requirement_late_attempt_artifact_count
            FROM groundloop_m5_runtime_work_contribution
            WHERE epoch_id = %s
              AND contribution_kind = 'preterminal_late_return'
              AND source_id = %s
            """,
            (row["epoch_id"], row["attempt_id"]),
        ).fetchone() == (1,)
        if return_kind == "discovery":
            discovery = row["discovery_binding"]
            assert isinstance(discovery, dict)
            assert discovery["channel_hit_count"] > 0
            assert discovery["admitted_pair_count"] > 0
            assert discovery["channel_hits"][0]["score"] == _f64_hex(0.91)
        else:
            verifier = row["verifier_binding"]
            assert isinstance(verifier, dict)
            execution = verifier["verification_execution"]
            observation = verifier["observation"]
            assert isinstance(execution, dict)
            assert execution["raw_logits"][1] == _f64_hex(-0.0)
            assert observation["neutral_score"] == _f64_hex(-0.0)


@pytest.mark.parametrize(
    ("subgraph", "late_artifact_count"),
    (("direct", 0), ("direct", 98), ("requirement", 0), ("requirement", 98)),
)
def test_preterminal_late_return_rejects_inexact_artifact_count(
    subgraph: str,
    late_artifact_count: int,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        if subgraph == "direct":
            row = _seed_direct_envelope_support(
                connection,
                return_kind="discovery",
                expire_attempt=False,
            )
            _cancel_direct_job_for_audit(
                connection,
                epoch_id=int(row["epoch_id"]),
                job_id=str(row["job_id"]),
            )
            with pytest.raises(
                psycopg.errors.RaiseException,
                match="late-return contribution lacks its exact return artifact",
            ):
                _insert_preterminal_direct_return_closure(
                    connection,
                    envelope_row=row,
                    late_artifact_count=late_artifact_count,
                )
                connection.execute(
                    "SET CONSTRAINTS groundloop_m5_work_contribution_shape IMMEDIATE"
                )
        else:
            row = _seed_requirement_audit_attempt(connection)
            _cancel_requirement_job_for_audit(connection, row)
            artifact = _build_requirement_audit_artifact(connection, row)
            with pytest.raises(
                psycopg.errors.RaiseException,
                match="late-return contribution lacks its exact return artifact",
            ):
                _insert_requirement_preterminal_closure(
                    connection,
                    row,
                    artifact,
                    late_artifact_count=late_artifact_count,
                )
                connection.execute(
                    "SET CONSTRAINTS groundloop_m5_work_contribution_shape IMMEDIATE"
                )
        connection.rollback()


def test_typed_direct_envelope_prevents_postcommit_event_source_mutation() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="discovery",
            expire_attempt=False,
        )
        _cancel_direct_job_for_audit(
            connection,
            epoch_id=int(row["epoch_id"]),
            job_id=str(row["job_id"]),
        )
        with connection.transaction():
            _insert_preterminal_direct_return_closure(
                connection,
                envelope_row=row,
            )
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

        original_event_id = str(row["job_binding"]["event_id"])
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="typed-direct envelope lost its exact event source binding",
        ):
            with connection.transaction():
                connection.execute(
                    "UPDATE groundloop_epoch SET event_id = %s WHERE epoch_id = %s",
                    (f"{original_event_id}-mutated", row["epoch_id"]),
                )
                connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

        assert connection.execute(
            "SELECT event_id FROM groundloop_epoch WHERE epoch_id = %s",
            (row["epoch_id"],),
        ).fetchone() == (original_event_id,)


@pytest.mark.parametrize("first_writer", ("envelope", "event"))
def test_typed_direct_envelope_and_event_source_serialize_in_both_lock_orders(
    first_writer: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="discovery",
            expire_attempt=False,
        )
        _cancel_direct_job_for_audit(
            connection,
            epoch_id=int(row["epoch_id"]),
            job_id=str(row["job_id"]),
        )
        schema_name = _current_schema_name(connection)
        original_event_id = str(row["job_binding"]["event_id"])
        mutated_event_id = f"{original_event_id}-concurrent"
        started = Event()
        executor = ThreadPoolExecutor(max_workers=1)
        try:
            if first_writer == "envelope":
                with connection.transaction():
                    _insert_preterminal_direct_return_closure(
                        connection,
                        envelope_row=row,
                    )
                    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
                    future = executor.submit(
                        _run_base_epoch_source_mutation,
                        schema_name=schema_name,
                        epoch_id=int(row["epoch_id"]),
                        column_name="event_id",
                        new_value=mutated_event_id,
                        started=started,
                    )
                    assert started.wait(timeout=5)
                    with pytest.raises(FutureTimeoutError):
                        future.result(timeout=0.25)
                result = future.result(timeout=10)
                assert isinstance(result, psycopg.errors.RaiseException)
                assert (
                    "typed-direct envelope lost its exact event source binding"
                    in str(result)
                )
                expected_event_id = original_event_id
                expected_envelope_count = 1
            else:
                with connection.transaction():
                    assert (
                        connection.execute(
                            "UPDATE groundloop_epoch SET event_id = %s "
                            "WHERE epoch_id = %s",
                            (mutated_event_id, row["epoch_id"]),
                        ).rowcount
                        == 1
                    )
                    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
                    future = executor.submit(
                        _run_direct_envelope_closure_insert,
                        schema_name=schema_name,
                        envelope_row=row,
                        started=started,
                    )
                    assert started.wait(timeout=5)
                    with pytest.raises(FutureTimeoutError):
                        future.result(timeout=0.25)
                result = future.result(timeout=10)
                assert isinstance(result, psycopg.errors.RaiseException)
                assert (
                    "typed-direct envelope does not lock its exact event source"
                    in str(result)
                )
                expected_event_id = mutated_event_id
                expected_envelope_count = 0
        finally:
            executor.shutdown(wait=True, cancel_futures=True)

        assert connection.execute(
            "SELECT event_id FROM groundloop_epoch WHERE epoch_id = %s",
            (row["epoch_id"],),
        ).fetchone() == (expected_event_id,)
        assert connection.execute(
            "SELECT count(*) "
            "FROM groundloop_m5_typed_direct_late_return_envelope "
            "WHERE epoch_id = %s",
            (row["epoch_id"],),
        ).fetchone() == (expected_envelope_count,)


def test_typed_direct_event_binding_allows_unrelated_terminal_status_transition() -> (
    None
):
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="discovery",
            expire_attempt=False,
        )
        _cancel_direct_job_for_audit(
            connection,
            epoch_id=int(row["epoch_id"]),
            job_id=str(row["job_id"]),
        )
        with connection.transaction():
            _insert_preterminal_direct_return_closure(
                connection,
                envelope_row=row,
            )
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

        _terminalize_recovered_epoch_for_audit(
            connection,
            epoch_id=int(row["epoch_id"]),
        )
        connection.commit()

        terminal = connection.execute(
            """
            SELECT base.event_id, base.payload_hash, base.structural_status,
                   runtime.runtime_state, runtime.revision
            FROM groundloop_epoch AS base
            JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
            WHERE base.epoch_id = %s
            """,
            (row["epoch_id"],),
        ).fetchone()
        assert terminal is not None
        assert terminal[0] == row["job_binding"]["event_id"]
        assert str(terminal[1]).strip() != row["job_binding"]["payload_hash"]
        assert len(str(terminal[1]).strip()) == 64
        assert terminal[2:] == ("failed", "failed", 3)


@pytest.mark.parametrize(
    (
        "verification_execution_present",
        "reused_execution",
        "requested_make_effective",
    ),
    (
        (False, False, False),
        (True, True, False),
        (True, False, True),
    ),
)
def test_verifier_envelope_accepts_every_optional_branch_combination(
    verification_execution_present: bool,
    reused_execution: bool,
    requested_make_effective: bool,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="verifier",
            verification_execution_present=verification_execution_present,
            reused_execution=reused_execution,
            requested_make_effective=requested_make_effective,
            expire_attempt=False,
        )
        _cancel_direct_job_for_audit(
            connection,
            epoch_id=int(row["epoch_id"]),
            job_id=str(row["job_id"]),
        )

        _insert_preterminal_direct_return_closure(
            connection,
            envelope_row=row,
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()

        verifier = row["verifier_binding"]
        assert isinstance(verifier, dict)
        execution = verifier["verification_execution"]
        assert (execution is not None) is verification_execution_present
        if isinstance(execution, dict):
            assert (
                execution["reused_from_observation_id"] is not None
            ) is reused_execution
        assert row["requested_make_effective"] is requested_make_effective


@pytest.mark.parametrize(
    "missing_key",
    (
        "observation_id",
        "job_id",
        "admitted_pair_id",
        "model_artifact_id",
        "prompt_artifact_id",
        "execution_spec_hash",
        "pair_input_hash",
        "calibration_version",
        "calibration_artifact_sha256",
        "temperature",
        "raw_logits",
        "raw_output_hash",
    ),
)
def test_verifier_envelope_rejects_each_missing_required_execution_field(
    missing_key: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = deepcopy(
            _seed_direct_envelope_support(connection, return_kind="verifier")
        )
        verifier = row["verifier_binding"]
        assert isinstance(verifier, dict)
        execution = verifier["verification_execution"]
        assert isinstance(execution, dict)
        del execution[missing_key]

        _insert_direct_envelope(connection, row)
        with pytest.raises(psycopg.errors.RaiseException, match="execution is partial"):
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    (
        ("false_flag_present_tuple", "wrong fallback hash"),
        ("true_flag_absent_tuple", "execution is partial"),
        ("wrong_logit_length", "execution is partial"),
        ("nonfinite_logit", "M5 F64 wire value must be finite"),
        ("absent_fallback_mismatch", "wrong fallback hash"),
    ),
)
def test_verifier_envelope_rejects_flag_option_and_f64_shape_mismatches(
    mutation: str,
    expected_error: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        present = mutation != "absent_fallback_mismatch"
        row = deepcopy(
            _seed_direct_envelope_support(
                connection,
                return_kind="verifier",
                verification_execution_present=present,
            )
        )
        verifier = row["verifier_binding"]
        assert isinstance(verifier, dict)
        if mutation == "false_flag_present_tuple":
            row["verification_execution_present"] = False
        elif mutation == "true_flag_absent_tuple":
            verifier["verification_execution"] = None
        elif mutation == "wrong_logit_length":
            execution = verifier["verification_execution"]
            assert isinstance(execution, dict)
            execution["raw_logits"] = execution["raw_logits"][:2]
        elif mutation == "nonfinite_logit":
            execution = verifier["verification_execution"]
            assert isinstance(execution, dict)
            execution["raw_logits"][0] = "7ff0000000000000"
        else:
            observation = verifier["observation"]
            assert isinstance(observation, dict)
            observation["raw_output_hash"] = _sha("forged-absent-fallback")

        _insert_direct_envelope(connection, row)
        with pytest.raises(psycopg.errors.RaiseException, match=expected_error):
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()


def test_verifier_envelope_rejects_ineligible_observation() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = deepcopy(
            _seed_direct_envelope_support(connection, return_kind="verifier")
        )
        verifier = row["verifier_binding"]
        assert isinstance(verifier, dict)
        observation = verifier["observation"]
        assert isinstance(observation, dict)
        row["observation_eligible_for_currency"] = False
        observation["eligible_for_currency"] = False

        with pytest.raises(psycopg.errors.CheckViolation):
            _insert_direct_envelope(connection, row)
        connection.rollback()


def test_m4_recovery_digest_helpers_match_frozen_python_wires() -> None:
    values = (
        0.1,
        1.0,
        1.2345678901234567,
        1e20,
        1e-5,
        1e-10,
        -0.0,
        12345.6789,
        2.2250738585072014e-308,
        1.7976931348623157e308,
    )
    digest_parts = (
        "m4-discovery-channel-v1",
        "987650001",
        "claim",
        "chunk",
        "policy",
        "vector",
        "1",
        format(0.1, ".17g"),
        _sha("channel-artifact"),
    )
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()

        rendered_values: list[str] = []
        for value in values:
            rendered_row = connection.execute(
                "SELECT groundloop_m5_recovery_m4_float_text(%s)",
                (value,),
            ).fetchone()
            assert rendered_row is not None
            rendered_values.append(str(rendered_row[0]))
        rendered = tuple(rendered_values)
        digest_row = connection.execute(
            "SELECT groundloop_m5_recovery_stable_m4_digest(%s::text[])",
            (list(digest_parts),),
        ).fetchone()
        assert digest_row is not None
        sql_digest = str(digest_row[0])

        assert rendered == tuple(format(value, ".17g") for value in values)
        assert sql_digest == stable_m4_digest(*digest_parts)


@pytest.mark.parametrize(
    "value",
    (
        0.0,
        -0.0,
        5e-324,
        -5e-324,
        2.2250738585072014e-308,
        1.7976931348623157e308,
    ),
)
def test_exact_f64_hex_helper_round_trips_ieee_754_bits(value: float) -> None:
    expected_hex = _f64_hex(value)
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()

        assert connection.execute(
            """
            SELECT encode(
                float8send(groundloop_m5_recovery_f64_from_hex(%s)),
                'hex'
            )
            """,
            (expected_hex,),
        ).fetchone() == (expected_hex,)
        assert connection.execute(
            "SELECT groundloop_m5_recovery_f64_fields_from_hex(%s)",
            (expected_hex,),
        ).fetchone() == (["f64", expected_hex],)


@pytest.mark.parametrize(
    "invalid_hex",
    (
        "000000000000000",
        "00000000000000000",
        "3FF0000000000000",
        "7ff0000000000000",
        "fff0000000000000",
        "7ff8000000000000",
    ),
)
def test_exact_f64_hex_helper_rejects_noncanonical_or_nonfinite_wires(
    invalid_hex: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()

        with pytest.raises(psycopg.errors.RaiseException, match="M5 F64 wire"):
            connection.execute(
                "SELECT groundloop_m5_recovery_f64_from_hex(%s)",
                (invalid_hex,),
            )
        connection.rollback()


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    (
        ("unknown_scope_key", "scope binding is incomplete"),
        ("forged_channel_set", "channel-set hash is incorrect"),
        ("duplicate_channel_hit", "channel hits contain duplicate identities"),
        ("duplicate_admitted_pair", "admitted pairs contain duplicate identities"),
        ("registry_mismatch", "scope registry closure is inconsistent"),
    ),
)
def test_discovery_late_envelope_commit_time_falsifiers(
    mutation: str, expected_error: str
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(connection, return_kind="discovery")
        row = deepcopy(row)
        discovery = row["discovery_binding"]
        scope = row["scope_binding"]
        assert isinstance(discovery, dict)
        assert isinstance(scope, dict)

        if mutation == "unknown_scope_key":
            scope["unknown"] = "forbidden"
        elif mutation == "forged_channel_set":
            discovery["channel_set_hash"] = _sha("forged-channel-set")
        elif mutation == "duplicate_channel_hit":
            hit = {
                "epoch_id": row["epoch_id"],
                "claim_id": scope["registered_claim_ids"][0],
                "chunk_version_id": row["job_binding"]["target_chunk_version_id"],
                "candidate_policy_id": row["job_binding"]["candidate_policy_id"],
                "channel": "vector",
                "rank": 1,
                "score": None,
                "channel_artifact_hash": _sha("duplicate-channel-hit"),
            }
            discovery["channel_hits"] = [hit, hit]
            discovery["channel_hit_count"] = 2
        elif mutation == "duplicate_admitted_pair":
            admitted = {
                "epoch_id": row["epoch_id"],
                "claim_id": scope["registered_claim_ids"][0],
                "chunk_version_id": row["job_binding"]["target_chunk_version_id"],
                "candidate_policy_id": row["job_binding"]["candidate_policy_id"],
                "fused_rank": 1,
                "reasons": ["vector"],
                "mandatory_lineage": False,
            }
            discovery["admitted_pairs"] = [admitted, admitted]
            discovery["admitted_pair_count"] = 2
        else:
            scope["registered_claim_ids"] = ["not-the-registry-member"]

        _insert_direct_envelope(connection, row)
        with pytest.raises(psycopg.errors.RaiseException, match=expected_error):
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    (
        ("unknown_observation_key", "observation is inconsistent"),
        ("wrong_admitted_pair", "admitted-pair identity is incorrect"),
        (
            "persisted_execution_mismatch",
            "verification execution differs from persisted M4",
        ),
        (
            "signed_zero_raw_logit",
            "verification execution differs from persisted M4",
        ),
        (
            "signed_zero_observation",
            "existing M4 observation differs from late envelope",
        ),
    ),
)
def test_verifier_late_envelope_commit_time_falsifiers(
    mutation: str, expected_error: str
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(connection, return_kind="verifier")
        row = deepcopy(row)
        verifier = row["verifier_binding"]
        assert isinstance(verifier, dict)
        observation = verifier["observation"]
        execution = verifier["verification_execution"]
        assert isinstance(observation, dict)
        assert isinstance(execution, dict)

        if mutation == "unknown_observation_key":
            observation["unknown"] = "forbidden"
        elif mutation == "wrong_admitted_pair":
            execution["admitted_pair_id"] = _sha("wrong-admitted-pair")
        elif mutation == "persisted_execution_mismatch":
            execution["model_artifact_id"] = "forged-model-artifact"
        elif mutation == "signed_zero_raw_logit":
            assert execution["raw_logits"][1] == _f64_hex(-0.0)
            execution["raw_logits"][1] = _f64_hex(0.0)
            _recompute_verifier_envelope_digests(row)
        else:
            assert mutation == "signed_zero_observation"
            assert observation["neutral_score"] == _f64_hex(-0.0)
            observation["neutral_score"] = _f64_hex(0.0)
            _recompute_verifier_envelope_digests(row)

        _insert_direct_envelope(connection, row)
        with pytest.raises(psycopg.errors.RaiseException, match=expected_error):
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()


@pytest.mark.parametrize(
    ("return_kind", "expected_error"),
    (
        ("expired_return", "lacks expired-return sidecar"),
        (
            "terminal_audit_only",
            "lacks exclusive direct envelope",
        ),
    ),
)
def test_both_postterminal_return_kinds_have_commit_time_falsifiers(
    return_kind: str, expected_error: str
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_postterminal_audit_support(connection, return_kind=return_kind)

        _insert_postterminal_audit(connection, row)
        with pytest.raises(psycopg.errors.RaiseException, match=expected_error):
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()


@pytest.mark.parametrize("evidence_disposition", ("returned", "reused_artifact"))
def test_postterminal_direct_audit_accepts_both_successful_dispositions(
    evidence_disposition: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_postterminal_audit_support(
            connection,
            return_kind="terminal_audit_only",
            evidence_disposition=evidence_disposition,
            include_envelope=True,
        )

        _insert_postterminal_audit(connection, row)
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()

        assert connection.execute(
            """
            SELECT audit.return_kind, evidence.disposition
            FROM groundloop_m5_post_terminal_attempt_audit AS audit
            JOIN groundloop_m5_attempt_execution_evidence AS evidence
              ON evidence.epoch_id = audit.epoch_id
             AND evidence.subgraph = audit.subgraph
             AND evidence.attempt_id = audit.attempt_id
            WHERE audit.epoch_id = %s AND audit.attempt_id = %s
            """,
            (row["epoch_id"], row["attempt_id"]),
        ).fetchone() == ("terminal_audit_only", evidence_disposition)
        assert connection.execute(
            """
            SELECT
                count(*) FILTER (
                    WHERE contribution.source_id = %s
                ),
                (SELECT count(*)
                 FROM groundloop_m5_runtime_timing_contribution AS timing
                 WHERE timing.epoch_id = %s AND timing.attempt_id = %s)
            FROM groundloop_m5_runtime_work_contribution AS contribution
            WHERE contribution.epoch_id = %s
            """,
            (
                row["attempt_id"],
                row["epoch_id"],
                row["attempt_id"],
                row["epoch_id"],
            ),
        ).fetchone() == (0, 0)


@pytest.mark.parametrize(
    ("successor_ordinal", "accepted"),
    ((2, True), (None, False), (3, False)),
)
def test_postterminal_direct_expired_return_requires_immediate_successor(
    successor_ordinal: int | None, accepted: bool
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_postterminal_audit_support(
            connection,
            return_kind="expired_return",
            include_expired_sidecar=True,
            expired_successor_ordinal=successor_ordinal,
        )

        _insert_postterminal_audit(connection, row)
        if accepted:
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
            connection.commit()
            assert connection.execute(
                """
                SELECT return_kind
                FROM groundloop_m5_post_terminal_attempt_audit
                WHERE epoch_id = %s AND subgraph = 'direct'
                  AND attempt_id = %s
                """,
                (row["epoch_id"], row["attempt_id"]),
            ).fetchone() == ("expired_return",)
        else:
            with pytest.raises(
                psycopg.errors.RaiseException,
                match="expired return lacks its exact envelope",
            ):
                connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
            connection.rollback()


@pytest.mark.parametrize(
    "cancellation_reason",
    (M5TerminalReason.SUBJECT_INACTIVE, M5TerminalReason.EPOCH_FAILED),
)
def test_postterminal_requirement_terminal_audit_accepts_exact_cancelled_attempt(
    cancellation_reason: M5TerminalReason,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        audit = _seed_requirement_postterminal_audit_support(
            connection,
            cancellation_reason=cancellation_reason,
        )

        _insert_postterminal_audit(connection, audit)
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()

        assert connection.execute(
            """
            SELECT audit.return_kind, job.job_state,
                   job.cancellation_reason, attempt.attempt_state
            FROM groundloop_m5_post_terminal_attempt_audit AS audit
            JOIN groundloop_m5_job_attempt AS attempt
              ON attempt.attempt_id = audit.attempt_id
            JOIN groundloop_m5_semantic_job AS job
              ON job.logical_job_id = attempt.logical_job_id
            WHERE audit.epoch_id = %s AND audit.subgraph = 'requirement'
              AND audit.attempt_id = %s
            """,
            (audit["epoch_id"], audit["attempt_id"]),
        ).fetchone() == (
            "terminal_audit_only",
            "cancelled",
            cancellation_reason.value,
            "dispatched",
        )


@pytest.mark.parametrize("mutation", ("later_attempt", "expired_without_sidecar"))
def test_postterminal_requirement_terminal_audit_rejects_replaced_attempt(
    mutation: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        audit = _seed_requirement_postterminal_audit_support(
            connection,
            cancellation_reason=M5TerminalReason.SUBJECT_INACTIVE,
            mutation=mutation,
        )

        _insert_postterminal_audit(connection, audit)
        with pytest.raises(
            psycopg.errors.RaiseException,
            match=(
                "terminal-audit-only M5 result lacks current terminal job closure"
                "|lacks exclusive requirement artifact"
            ),
        ):
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()


def test_preterminal_requirement_terminal_audit_updates_exact_current_accounting() -> (
    None
):
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_requirement_audit_attempt(connection)
        _cancel_requirement_job_for_audit(connection, row)
        artifact = _build_requirement_audit_artifact(connection, row)

        _insert_requirement_preterminal_closure(connection, row, artifact)
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()

        attempt = row["attempt"]
        assert isinstance(attempt, M5JobAttempt)
        assert connection.execute(
            """
            SELECT runtime.revision, job.job_state,
                   job.cancelled_by_event_id, job.cancelled_by_epoch_id,
                   job.cancellation_reason, attempt.attempt_state,
                   execution.applied_revision, late.applied_revision,
                   late.requirement_late_attempt_artifact_count,
                   work.updated_revision, timing.updated_revision
            FROM groundloop_m5_runtime_epoch AS runtime
            JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
            JOIN groundloop_m5_job_attempt AS attempt
              ON attempt.logical_job_id = job.logical_job_id
            JOIN groundloop_m5_runtime_work_contribution AS execution
              ON execution.epoch_id = runtime.epoch_id
             AND execution.contribution_kind = 'm5_attempt_execution'
             AND execution.source_id = attempt.attempt_id
            JOIN groundloop_m5_runtime_work_contribution AS late
              ON late.epoch_id = runtime.epoch_id
             AND late.contribution_kind = 'preterminal_late_return'
             AND late.source_id = attempt.attempt_id
            JOIN groundloop_m5_runtime_work_accumulator AS work
              ON work.epoch_id = runtime.epoch_id
            JOIN groundloop_m5_runtime_timing_accumulator AS timing
              ON timing.epoch_id = runtime.epoch_id
            WHERE runtime.epoch_id = %s AND attempt.attempt_id = %s
            """,
            (row["epoch_id"], attempt.attempt_id),
        ).fetchone() == (
            3,
            "cancelled",
            row["structural_event_id"],
            row["epoch_id"],
            "subject_inactive",
            "dispatched",
            3,
            3,
            1,
            3,
            3,
        )


@pytest.mark.parametrize(
    "mutation",
    (
        "ordinary_running",
        "completed_active",
        "completed_inactive",
        "terminal_failed",
        "epoch_failed",
        "later_attempt",
        "expired_attempt",
    ),
)
def test_preterminal_requirement_terminal_audit_rejects_noncurrent_shape(
    mutation: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_requirement_audit_attempt(connection)
        if mutation == "ordinary_running":
            artifact = _build_requirement_audit_artifact(
                connection, row, receipt_state=M5JobState.COMPLETED_ACTIVE
            )
            artifact.update(
                {
                    "job_state_at_receipt": "running",
                    "job_state_after": "running",
                }
            )
            _recompute_requirement_artifact_identity(artifact)
        else:
            _cancel_requirement_job_for_audit(
                connection,
                row,
                cancellation_reason=(
                    M5TerminalReason.EPOCH_FAILED
                    if mutation == "epoch_failed"
                    else M5TerminalReason.SUBJECT_INACTIVE
                ),
            )
            receipt_state = {
                "completed_active": M5JobState.COMPLETED_ACTIVE,
                "completed_inactive": M5JobState.COMPLETED_INACTIVE,
                "terminal_failed": M5JobState.TERMINAL_FAILED,
            }.get(mutation, M5JobState.CANCELLED)
            artifact = _build_requirement_audit_artifact(
                connection, row, receipt_state=receipt_state
            )
            if mutation == "later_attempt":
                _insert_requirement_successor_attempt(connection, row)
            elif mutation == "expired_attempt":
                _expire_requirement_audit_attempt(connection, row)

        with pytest.raises(
            (psycopg.errors.CheckViolation, psycopg.errors.RaiseException)
        ):
            _insert_requirement_preterminal_closure(connection, row, artifact)
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()


def test_postterminal_direct_terminal_audit_accepts_epoch_failed_cancellation() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        audit = _seed_postterminal_audit_support(
            connection,
            return_kind="terminal_audit_only",
            include_envelope=True,
            terminal_reason="epoch_failed",
        )

        _insert_postterminal_audit(connection, audit)
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()

        assert connection.execute(
            """
            SELECT projection.terminal_reason
            FROM groundloop_m5_post_terminal_attempt_audit AS audit
            JOIN groundloop_m5_typed_direct_late_return_envelope AS envelope
              ON envelope.epoch_id = audit.epoch_id
             AND envelope.attempt_id = audit.attempt_id
            JOIN groundloop_m5_direct_terminal_projection AS projection
              ON projection.epoch_id = envelope.epoch_id
             AND projection.job_id = envelope.job_id
            WHERE audit.epoch_id = %s AND audit.attempt_id = %s
            """,
            (audit["epoch_id"], audit["attempt_id"]),
        ).fetchone() == ("epoch_failed",)


def test_preterminal_direct_terminal_audit_rejects_epoch_failed_cancellation() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="discovery",
            expire_attempt=False,
        )
        _cancel_direct_job_for_audit(
            connection,
            epoch_id=int(row["epoch_id"]),
            job_id=str(row["job_id"]),
            terminal_reason="epoch_failed",
        )

        _insert_preterminal_direct_return_closure(connection, envelope_row=row)
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="late-return envelope lacks exact audit/accounting closure",
        ):
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()


def test_nonexpired_preterminal_return_updates_exact_current_work_and_timing() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="discovery",
            expire_attempt=False,
        )
        _cancel_direct_job_for_audit(
            connection,
            epoch_id=int(row["epoch_id"]),
            job_id=str(row["job_id"]),
        )

        _insert_preterminal_direct_return_closure(
            connection,
            envelope_row=row,
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()

        assert connection.execute(
            """
            SELECT runtime.revision,
                   execution.applied_revision,
                   late.applied_revision,
                   work.updated_revision,
                   timing.updated_revision,
                   timing.pending_contribution_kind,
                   timing.pending_source_id,
                   timing.pending_anchor_revision
            FROM groundloop_m5_runtime_epoch AS runtime
            JOIN groundloop_m5_runtime_work_contribution AS execution
              ON execution.epoch_id = runtime.epoch_id
             AND execution.contribution_kind = 'direct_attempt_execution'
             AND execution.source_id = %s
            JOIN groundloop_m5_runtime_work_contribution AS late
              ON late.epoch_id = runtime.epoch_id
             AND late.contribution_kind = 'preterminal_late_return'
             AND late.source_id = %s
            JOIN groundloop_m5_runtime_work_accumulator AS work
              ON work.epoch_id = runtime.epoch_id
            JOIN groundloop_m5_runtime_timing_accumulator AS timing
              ON timing.epoch_id = runtime.epoch_id
            WHERE runtime.epoch_id = %s
            """,
            (row["attempt_id"], row["attempt_id"], row["epoch_id"]),
        ).fetchone() == (
            2,
            2,
            2,
            2,
            2,
            "preterminal_late_return",
            row["attempt_id"],
            2,
        )


@pytest.mark.parametrize(
    "mutation",
    ("ordinary_running", "forged_identity", "replaced_attempt", "terminal_epoch"),
)
def test_nonexpired_preterminal_return_rejects_wrong_identity_or_cutoff(
    mutation: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="discovery",
            expire_attempt=False,
        )
        if mutation != "ordinary_running":
            _cancel_direct_job_for_audit(
                connection,
                epoch_id=int(row["epoch_id"]),
                job_id=str(row["job_id"]),
            )
        if mutation == "replaced_attempt":
            successor_attempt_id = stable_m4_digest(
                "m4-job-attempt-v1", str(row["job_id"]), "2"
            )
            successor_lease_token = stable_m4_digest(
                "m4-lease-token-v1", str(row["job_id"]), "2"
            )
            connection.execute(
                """
                INSERT INTO groundloop_semantic_job_attempt (
                    attempt_id, job_id, execution_spec_hash,
                    attempt_ordinal, lease_token_hash, attempt_state,
                    lease_expires_at
                )
                SELECT %s, attempt.job_id, attempt.execution_spec_hash,
                       2, %s, 'leased', clock_timestamp() + interval '1 hour'
                FROM groundloop_semantic_job_attempt AS attempt
                WHERE attempt.attempt_id = %s
                """,
                (successor_attempt_id, successor_lease_token, row["attempt_id"]),
            )
        if mutation == "terminal_epoch":
            _terminalize_direct_epoch_for_audit(
                connection,
                epoch_id=int(row["epoch_id"]),
            )
            connection.commit()

        with pytest.raises(
            psycopg.errors.RaiseException,
            match=(
                "late-return envelope lacks exact audit/accounting closure"
                "|terminal M5 epoch rejects event-accounted work or timing"
            ),
        ):
            _insert_preterminal_direct_return_closure(
                connection,
                envelope_row=row,
                forged_late_identity=(
                    _sha("forged-preterminal-return")
                    if mutation == "forged_identity"
                    else None
                ),
            )
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()


def test_attempt_evidence_allows_external_byte_counters() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="discovery",
            expire_attempt=False,
        )
        attempt_values = [0 for _ in WORK_COUNTER_COLUMNS]
        attempt_values[25] = 17
        attempt_values[26] = 19
        _cancel_direct_job_for_audit(
            connection,
            epoch_id=int(row["epoch_id"]),
            job_id=str(row["job_id"]),
        )

        _insert_preterminal_direct_return_closure(
            connection,
            envelope_row=row,
            attempt_values=tuple(attempt_values),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()

        assert connection.execute(
            """
            SELECT attempt_bytes_hashed, attempt_bytes_serialized
            FROM groundloop_m5_attempt_execution_evidence
            WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
            """,
            (row["epoch_id"], row["attempt_id"]),
        ).fetchone() == (17, 19)


@pytest.mark.parametrize(
    ("return_kind", "with_tokens"),
    (
        ("discovery", False),
        ("discovery", True),
        ("verifier", False),
        ("verifier", True),
    ),
)
def test_attempt_evidence_accepts_job_owned_model_calls_and_optional_tokens(
    return_kind: str,
    with_tokens: bool,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind=return_kind,
            expire_attempt=False,
        )
        attempt_values = [0 for _ in WORK_COUNTER_COLUMNS]
        if return_kind == "discovery":
            attempt_values[3] = 1
            attempt_values[27] = 1
            attempt_values[29] = 23 if with_tokens else 0
        else:
            attempt_values[4] = 1
            attempt_values[28] = 1
            attempt_values[30] = 17 if with_tokens else 0
            attempt_values[31] = 11 if with_tokens else 0
        _cancel_direct_job_for_audit(
            connection,
            epoch_id=int(row["epoch_id"]),
            job_id=str(row["job_id"]),
        )

        _insert_preterminal_direct_return_closure(
            connection,
            envelope_row=row,
            attempt_values=tuple(attempt_values),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()

        assert connection.execute(
            """
            SELECT attempt_embedding_model_call_count,
                   attempt_verifier_model_call_count,
                   attempt_embedding_input_token_count,
                   attempt_verifier_input_token_count,
                   attempt_verifier_output_token_count
            FROM groundloop_m5_attempt_execution_evidence
            WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
            """,
            (row["epoch_id"], row["attempt_id"]),
        ).fetchone() == tuple(attempt_values[index] for index in range(27, 32))


@pytest.mark.parametrize(
    ("return_kind", "mutation"),
    (
        ("discovery", "foreign_model"),
        ("discovery", "token_without_model"),
        ("discovery", "model_without_job_call"),
        ("verifier", "foreign_model"),
        ("verifier", "token_without_model"),
        ("verifier", "model_without_job_call"),
    ),
)
def test_attempt_evidence_rejects_cross_kind_or_unowned_model_token_work(
    return_kind: str,
    mutation: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind=return_kind,
            expire_attempt=False,
        )
        attempt_values = [0 for _ in WORK_COUNTER_COLUMNS]
        if return_kind == "discovery":
            if mutation == "foreign_model":
                attempt_values[4] = 1
                attempt_values[28] = 1
            elif mutation == "token_without_model":
                attempt_values[3] = 1
                attempt_values[29] = 1
            else:
                attempt_values[27] = 1
        else:
            if mutation == "foreign_model":
                attempt_values[3] = 1
                attempt_values[27] = 1
            elif mutation == "token_without_model":
                attempt_values[4] = 1
                attempt_values[30] = 1
            else:
                attempt_values[28] = 1
        _cancel_direct_job_for_audit(
            connection,
            epoch_id=int(row["epoch_id"]),
            job_id=str(row["job_id"]),
        )

        with pytest.raises(
            psycopg.errors.RaiseException,
            match=(
                "confirmed attempt work exceeds its dispatch maximum"
                "|confirmed attempt model/token work disagrees with its job call"
            ),
        ):
            _insert_preterminal_direct_return_closure(
                connection,
                envelope_row=row,
                attempt_values=tuple(attempt_values),
            )
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()


@pytest.mark.parametrize(
    "job_kind",
    (
        "forward_requirement_retrieval",
        "reverse_requirement_discovery",
        "verify_requirement_pair",
    ),
)
def test_requirement_attempt_evidence_accepts_each_job_owned_model_token_vector(
    job_kind: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_requirement_job_kind_for_recovery_test(
            connection,
            job_kind=job_kind,
        )
        attempt_values = [0 for _ in WORK_COUNTER_COLUMNS]
        if job_kind == "forward_requirement_retrieval":
            attempt_values[8] = 1
            attempt_values[27] = 1
            attempt_values[29] = 31
        elif job_kind == "reverse_requirement_discovery":
            attempt_values[9] = 1
            attempt_values[27] = 1
            attempt_values[29] = 37
        else:
            attempt_values[11] = 1
            attempt_values[28] = 1
            attempt_values[30] = 41
            attempt_values[31] = 43

        with connection.transaction():
            _insert_requirement_execution_closure(
                connection,
                row=row,
                attempt_values=tuple(attempt_values),
            )
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

        attempt = row["attempt"]
        assert isinstance(attempt, M5JobAttempt)
        assert connection.execute(
            """
            SELECT attempt_requirement_forward_retrieval_call_count,
                   attempt_requirement_reverse_retrieval_call_count,
                   attempt_requirement_verifier_call_count,
                   attempt_embedding_model_call_count,
                   attempt_verifier_model_call_count,
                   attempt_embedding_input_token_count,
                   attempt_verifier_input_token_count,
                   attempt_verifier_output_token_count
            FROM groundloop_m5_attempt_execution_evidence
            WHERE subgraph = 'requirement' AND attempt_id = %s
            """,
            (attempt.attempt_id,),
        ).fetchone() == (
            attempt_values[8],
            attempt_values[9],
            attempt_values[11],
            attempt_values[27],
            attempt_values[28],
            attempt_values[29],
            attempt_values[30],
            attempt_values[31],
        )


@pytest.mark.parametrize(
    "job_kind",
    (
        "forward_requirement_retrieval",
        "reverse_requirement_discovery",
        "verify_requirement_pair",
    ),
)
def test_requirement_attempt_evidence_rejects_unowned_model_token_vectors(
    job_kind: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_requirement_job_kind_for_recovery_test(
            connection,
            job_kind=job_kind,
        )
        for mutation in ("foreign_model", "token_without_model", "model_without_call"):
            attempt_values = [0 for _ in WORK_COUNTER_COLUMNS]
            if job_kind in {
                "forward_requirement_retrieval",
                "reverse_requirement_discovery",
            }:
                job_call_index = 8 if job_kind == "forward_requirement_retrieval" else 9
                if mutation == "foreign_model":
                    attempt_values[11] = 1
                    attempt_values[28] = 1
                elif mutation == "token_without_model":
                    attempt_values[job_call_index] = 1
                    attempt_values[29] = 1
                else:
                    attempt_values[27] = 1
            else:
                if mutation == "foreign_model":
                    attempt_values[8] = 1
                    attempt_values[27] = 1
                elif mutation == "token_without_model":
                    attempt_values[11] = 1
                    attempt_values[30] = 1
                else:
                    attempt_values[28] = 1

            with pytest.raises(
                psycopg.errors.RaiseException,
                match=(
                    "confirmed attempt work exceeds its dispatch maximum"
                    "|confirmed attempt model/token work disagrees with its job call"
                ),
            ):
                with connection.transaction():
                    _insert_requirement_execution_closure(
                        connection,
                        row=row,
                        attempt_values=tuple(attempt_values),
                    )
                    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")


@pytest.mark.parametrize("mutation", ("evidence_digest", "timing_digest"))
def test_attempt_timing_rejects_composite_binding_mix_and_match(
    mutation: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(connection, return_kind="discovery")
        evidence_digest, _, timing_digest = _insert_direct_dispatch_and_evidence(
            connection,
            envelope_row=row,
            disposition="returned",
            force_constraints=False,
        )

        _insert_attempt_timing_contribution(
            connection,
            epoch_id=int(row["epoch_id"]),
            attempt_id=str(row["attempt_id"]),
            evidence_digest=(
                _sha("forged-evidence")
                if mutation == "evidence_digest"
                else evidence_digest
            ),
            attempt_timing_digest=(
                _sha("forged-timing") if mutation == "timing_digest" else timing_digest
            ),
        )
        with pytest.raises(
            (psycopg.errors.RaiseException, psycopg.errors.ForeignKeyViolation),
            match="attempt timing binding|foreign key constraint",
        ):
            connection.execute(
                "SET CONSTRAINTS "
                "groundloop_m5_runtime_timing_contribution_shape IMMEDIATE"
            )
        connection.rollback()


@pytest.mark.parametrize("mutation", ("contribution_key", "anchor_revision"))
def test_transition_timing_rejects_composite_anchor_mix_and_match(
    mutation: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="discovery",
            expire_attempt=False,
        )
        _cancel_direct_job_for_audit(
            connection,
            epoch_id=int(row["epoch_id"]),
            job_id=str(row["job_id"]),
        )
        _insert_preterminal_direct_return_closure(
            connection,
            envelope_row=row,
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()
        anchor = connection.execute(
            """
            SELECT contribution_key_digest, applied_revision
            FROM groundloop_m5_runtime_work_contribution
            WHERE epoch_id = %s
              AND contribution_kind = 'preterminal_late_return'
              AND source_id = %s
            """,
            (row["epoch_id"], row["attempt_id"]),
        ).fetchone()
        assert anchor is not None
        contribution_key = (
            _sha("forged-contribution-key")
            if mutation == "contribution_key"
            else str(anchor[0])
        )
        anchor_revision = (
            int(anchor[1]) + 1 if mutation == "anchor_revision" else int(anchor[1])
        )
        observation_digest = stable_m5_digest(
            "m5-runtime-timing-observation-v1",
            bool_field(False),
            *(option_field(None) for _ in range(9)),
        )
        transition_digest = stable_m5_digest(
            "m5-transition-call-timing-v1",
            int_field(int(row["epoch_id"])),
            enum_field("preterminal_late_return"),
            text_field(str(row["attempt_id"])),
            hash_field(contribution_key),
            int_field(anchor_revision),
            hash_field(observation_digest),
        )

        connection.execute(
            """
            INSERT INTO groundloop_m5_transition_call_timing (
                epoch_id, contribution_kind, source_id,
                contribution_key_digest, anchor_revision,
                required_interval_observed,
                coordinator_non_db_non_neural_ns, neural_wall_ns,
                postgres_roundtrip_wall_ns, external_io_wall_ns,
                end_to_end_wall_ns, postgres_server_execution_ns,
                postgres_lock_wait_ns, postgres_wal_bytes,
                postgres_shared_block_reads, observation_digest,
                transition_timing_digest
            ) VALUES (
                %s, 'preterminal_late_return', %s, %s, %s, false,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                %s, %s
            )
            """,
            (
                row["epoch_id"],
                row["attempt_id"],
                contribution_key,
                anchor_revision,
                observation_digest,
                transition_digest,
            ),
        )
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()


@pytest.mark.parametrize("terminal_kind", ("epoch_failure", "seal"))
def test_terminal_contribution_kinds_cannot_be_transition_timing_anchors(
    terminal_kind: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(connection, return_kind="discovery")

        with pytest.raises(psycopg.errors.CheckViolation):
            connection.execute(
                """
                INSERT INTO groundloop_m5_transition_call_timing (
                    epoch_id, contribution_kind, source_id,
                    contribution_key_digest, anchor_revision,
                    required_interval_observed,
                    coordinator_non_db_non_neural_ns, neural_wall_ns,
                    postgres_roundtrip_wall_ns, external_io_wall_ns,
                    end_to_end_wall_ns, postgres_server_execution_ns,
                    postgres_lock_wait_ns, postgres_wal_bytes,
                    postgres_shared_block_reads, observation_digest,
                    transition_timing_digest
                ) VALUES (
                    %s, %s, %s, %s, 2, false,
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    %s, %s
                )
                """,
                (
                    row["epoch_id"],
                    terminal_kind,
                    row["attempt_id"],
                    _sha("terminal-anchor-key"),
                    _sha("terminal-anchor-observation"),
                    _sha("terminal-anchor-timing"),
                ),
            )
        connection.rollback()


@pytest.mark.parametrize("insert_contribution", (True, False))
def test_timing_accumulator_insert_defers_exact_pending_anchor_closure(
    insert_contribution: bool,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="discovery",
            expire_attempt=False,
        )
        _insert_direct_dispatch_and_evidence(
            connection,
            envelope_row=row,
            disposition="returned",
            force_constraints=False,
            insert_evidence=False,
        )
        dispatch = connection.execute(
            """
            SELECT record_digest, dispatched_revision
            FROM groundloop_m5_dispatch_record
            WHERE subgraph = 'direct' AND attempt_id = %s
            """,
            (row["attempt_id"],),
        ).fetchone()
        assert dispatch is not None
        record_digest = str(dispatch[0]).strip()
        revision = int(dispatch[1])
        contribution_key = stable_m5_digest(
            "m5-runtime-work-contribution-key-v1",
            int_field(int(row["epoch_id"])),
            enum_field("direct_acquisition"),
            text_field(record_digest),
        )
        _insert_pending_timing_accumulator(
            connection,
            epoch_id=int(row["epoch_id"]),
            revision=revision,
            contribution_kind="direct_acquisition",
            source_id=record_digest,
            contribution_key_digest=contribution_key,
        )
        if insert_contribution:
            assert (
                _insert_work_contribution(
                    connection,
                    epoch_id=int(row["epoch_id"]),
                    contribution_kind="direct_acquisition",
                    source_id=record_digest,
                    source_identity_hash=record_digest,
                    applied_revision=revision,
                    values=tuple(0 for _ in WORK_COUNTER_COLUMNS),
                )
                == contribution_key
            )
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
            connection.commit()
            with pytest.raises(
                psycopg.errors.RaiseException,
                match="requires expected-revision authorization",
            ):
                connection.execute(
                    """
                    UPDATE groundloop_m5_runtime_timing_accumulator
                    SET updated_at = clock_timestamp()
                    WHERE epoch_id = %s
                    """,
                    (row["epoch_id"],),
                )
            connection.rollback()
        else:
            with pytest.raises(
                psycopg.errors.RaiseException,
                match="pending timing anchor lacks exact work contribution",
            ):
                connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
            connection.rollback()


@pytest.mark.parametrize("mutation", ("optional_expected", "terminal_pending"))
def test_timing_accumulator_rejects_count_or_pending_shape_mismatch(
    mutation: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(connection, return_kind="discovery")

        if mutation == "optional_expected":
            statement = """
                INSERT INTO groundloop_m5_runtime_timing_accumulator (
                    epoch_id, required_expected_count,
                    postgres_server_execution_expected_count,
                    postgres_server_execution_missing_count,
                    updated_revision, terminalized
                ) VALUES (%s, 0, 1, 1, 2, false)
            """
            parameters: tuple[object, ...] = (row["epoch_id"],)
        else:
            statement = """
                INSERT INTO groundloop_m5_runtime_timing_accumulator (
                    epoch_id,
                    required_expected_count, required_missing_count,
                    postgres_server_execution_expected_count,
                    postgres_server_execution_missing_count,
                    postgres_lock_wait_expected_count,
                    postgres_lock_wait_missing_count,
                    postgres_wal_bytes_expected_count,
                    postgres_wal_bytes_missing_count,
                    postgres_shared_block_reads_expected_count,
                    postgres_shared_block_reads_missing_count,
                    pending_contribution_kind, pending_source_id,
                    pending_contribution_key_digest, pending_anchor_revision,
                    updated_revision, terminalized
                ) VALUES (
                    %s,
                    1, 0, 1, 0, 1, 0, 1, 0, 1, 0,
                    'seal', %s, %s, 2, 2, false
                )
            """
            parameters = (
                row["epoch_id"],
                row["attempt_id"],
                _sha("terminal-pending-anchor"),
            )
        with pytest.raises(psycopg.errors.CheckViolation):
            connection.execute(statement, parameters)
        connection.rollback()


def test_terminal_telemetry_binds_independent_optional_field_presence() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(connection, return_kind="discovery")
        logical_result_hash = _terminalize_direct_epoch_for_audit(
            connection,
            epoch_id=int(row["epoch_id"]),
        )
        connection.commit()
        event_row = connection.execute(
            "SELECT structural_event_id FROM groundloop_m5_runtime_epoch "
            "WHERE epoch_id = %s",
            (row["epoch_id"],),
        ).fetchone()
        assert event_row is not None

        connection.execute(
            """
            INSERT INTO groundloop_m5_postcommit_invocation_telemetry (
                invocation_id, structural_event_id, epoch_id,
                terminal_logical_result_hash, required_interval_observed,
                coordinator_non_db_non_neural_ns, neural_wall_ns,
                postgres_roundtrip_wall_ns, external_io_wall_ns,
                end_to_end_wall_ns, postgres_server_execution_ns,
                postgres_lock_wait_ns, postgres_wal_bytes,
                postgres_shared_block_reads,
                required_expected_count, required_observed_count,
                required_missing_count,
                postgres_server_execution_expected_count,
                postgres_server_execution_observed_count,
                postgres_server_execution_missing_count,
                postgres_lock_wait_expected_count,
                postgres_lock_wait_observed_count,
                postgres_lock_wait_missing_count,
                postgres_wal_bytes_expected_count,
                postgres_wal_bytes_observed_count,
                postgres_wal_bytes_missing_count,
                postgres_shared_block_reads_expected_count,
                postgres_shared_block_reads_observed_count,
                postgres_shared_block_reads_missing_count,
                terminal_client_roundtrip_included
            ) VALUES (
                'terminal-telemetry-mixed-optionals', %s, %s, %s, true,
                11, 12, 13, 14, 15, NULL, 16, NULL, 17,
                1, 1, 0,
                1, 0, 1,
                1, 1, 0,
                1, 0, 1,
                1, 1, 0,
                true
            )
            """,
            (event_row[0], row["epoch_id"], logical_result_hash),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()

        assert connection.execute(
            """
            SELECT postgres_server_execution_ns, postgres_lock_wait_ns,
                   postgres_wal_bytes, postgres_shared_block_reads,
                   postgres_server_execution_observed_count,
                   postgres_lock_wait_observed_count,
                   postgres_wal_bytes_observed_count,
                   postgres_shared_block_reads_observed_count
            FROM groundloop_m5_postcommit_invocation_telemetry
            WHERE invocation_id = 'terminal-telemetry-mixed-optionals'
            """
        ).fetchone() == (None, 16, None, 17, 0, 1, 0, 1)


def test_terminal_telemetry_rejects_optional_expected_count_mismatch() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(connection, return_kind="discovery")
        logical_result_hash = _terminalize_direct_epoch_for_audit(
            connection,
            epoch_id=int(row["epoch_id"]),
        )
        connection.commit()
        event_row = connection.execute(
            "SELECT structural_event_id FROM groundloop_m5_runtime_epoch "
            "WHERE epoch_id = %s",
            (row["epoch_id"],),
        ).fetchone()
        assert event_row is not None

        with pytest.raises(psycopg.errors.CheckViolation):
            connection.execute(
                """
                INSERT INTO groundloop_m5_postcommit_invocation_telemetry (
                    invocation_id, structural_event_id, epoch_id,
                    terminal_logical_result_hash,
                    required_interval_observed,
                    required_expected_count, required_observed_count,
                    required_missing_count,
                    postgres_server_execution_expected_count,
                    postgres_server_execution_observed_count,
                    postgres_server_execution_missing_count,
                    postgres_lock_wait_expected_count,
                    postgres_lock_wait_observed_count,
                    postgres_lock_wait_missing_count,
                    postgres_wal_bytes_expected_count,
                    postgres_wal_bytes_observed_count,
                    postgres_wal_bytes_missing_count,
                    postgres_shared_block_reads_expected_count,
                    postgres_shared_block_reads_observed_count,
                    postgres_shared_block_reads_missing_count,
                    terminal_client_roundtrip_included
                ) VALUES (
                    'terminal-telemetry-bad-count', %s, %s, %s, false,
                    1, 0, 1,
                    0, 0, 0,
                    1, 0, 1,
                    1, 0, 1,
                    1, 0, 1,
                    false
                )
                """,
                (event_row[0], row["epoch_id"], logical_result_hash),
            )
        connection.rollback()


def test_work_counter_ownership_is_checked_on_the_inserted_row() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        database = _prepare_direct_runtime(connection)
        epoch_id = _open_alternate_runtime_header(connection, database)
        identity = connection.execute(
            """
            SELECT base.event_id, base.payload_hash, runtime.revision
            FROM groundloop_epoch AS base
            JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
            WHERE base.epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        assert identity is not None
        values = [0 for _ in WORK_COUNTER_COLUMNS]
        values[3] = 1

        with pytest.raises(
            psycopg.errors.RaiseException,
            match="work contribution uses a counter owned by another surface",
        ):
            _insert_work_contribution(
                connection,
                epoch_id=epoch_id,
                contribution_kind="structural_open",
                source_id=str(identity[0]),
                source_identity_hash=str(identity[1]).strip(),
                applied_revision=int(identity[2]),
                values=tuple(values),
            )
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()


@pytest.mark.parametrize("column_name", ("event_id", "payload_hash"))
def test_structural_open_contribution_prevents_postcommit_source_mutation(
    column_name: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        database = _prepare_direct_runtime(connection)
        epoch_id = _open_alternate_runtime_header(connection, database)
        identity = connection.execute(
            "SELECT event_id, payload_hash FROM groundloop_epoch WHERE epoch_id = %s",
            (epoch_id,),
        ).fetchone()
        assert identity is not None
        event_id = str(identity[0])
        payload_hash = str(identity[1]).strip()
        connection.commit()
        with connection.transaction():
            _insert_work_contribution(
                connection,
                epoch_id=epoch_id,
                contribution_kind="structural_open",
                source_id=event_id,
                source_identity_hash=payload_hash,
                applied_revision=1,
                values=tuple(0 for _ in WORK_COUNTER_COLUMNS),
            )
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        new_value = (
            f"{event_id}-mutated"
            if column_name == "event_id"
            else _sha("mutated-structural-open-payload")
        )

        with pytest.raises(
            psycopg.errors.RaiseException,
            match="structural-open contribution lost its exact event source",
        ):
            with connection.transaction():
                connection.execute(
                    sql.SQL(
                        "UPDATE groundloop_epoch SET {} = %s WHERE epoch_id = %s"
                    ).format(sql.Identifier(column_name)),
                    (new_value, epoch_id),
                )
                connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

        assert connection.execute(
            sql.SQL("SELECT {} FROM groundloop_epoch WHERE epoch_id = %s").format(
                sql.Identifier(column_name)
            ),
            (epoch_id,),
        ).fetchone() == (identity[0 if column_name == "event_id" else 1],)


@pytest.mark.parametrize("column_name", ("event_id", "payload_hash"))
@pytest.mark.parametrize("first_writer", ("contribution", "event"))
def test_structural_open_contribution_and_source_serialize_in_both_lock_orders(
    column_name: str,
    first_writer: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        database = _prepare_direct_runtime(connection)
        epoch_id = _open_alternate_runtime_header(connection, database)
        identity = connection.execute(
            "SELECT event_id, payload_hash FROM groundloop_epoch WHERE epoch_id = %s",
            (epoch_id,),
        ).fetchone()
        assert identity is not None
        event_id = str(identity[0])
        payload_hash = str(identity[1]).strip()
        original_value = event_id if column_name == "event_id" else payload_hash
        new_value = (
            f"{event_id}-concurrent"
            if column_name == "event_id"
            else _sha("concurrent-structural-open-payload")
        )
        schema_name = _current_schema_name(connection)
        started = Event()
        executor = ThreadPoolExecutor(max_workers=1)
        try:
            if first_writer == "contribution":
                with connection.transaction():
                    _insert_work_contribution(
                        connection,
                        epoch_id=epoch_id,
                        contribution_kind="structural_open",
                        source_id=event_id,
                        source_identity_hash=payload_hash,
                        applied_revision=1,
                        values=tuple(0 for _ in WORK_COUNTER_COLUMNS),
                    )
                    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
                    future = executor.submit(
                        _run_base_epoch_source_mutation,
                        schema_name=schema_name,
                        epoch_id=epoch_id,
                        column_name=column_name,
                        new_value=new_value,
                        started=started,
                    )
                    assert started.wait(timeout=5)
                    with pytest.raises(FutureTimeoutError):
                        future.result(timeout=0.25)
                result = future.result(timeout=10)
                assert isinstance(result, psycopg.errors.RaiseException)
                assert (
                    "structural-open contribution lost its exact event source"
                    in str(result)
                )
                expected_value = original_value
                expected_contribution_count = 1
            else:
                with connection.transaction():
                    assert (
                        connection.execute(
                            sql.SQL(
                                "UPDATE groundloop_epoch SET {} = %s "
                                "WHERE epoch_id = %s"
                            ).format(sql.Identifier(column_name)),
                            (new_value, epoch_id),
                        ).rowcount
                        == 1
                    )
                    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
                    future = executor.submit(
                        _run_structural_open_contribution_insert,
                        schema_name=schema_name,
                        epoch_id=epoch_id,
                        event_id=event_id,
                        payload_hash=payload_hash,
                        started=started,
                    )
                    assert started.wait(timeout=5)
                    with pytest.raises(FutureTimeoutError):
                        future.result(timeout=0.25)
                result = future.result(timeout=10)
                assert isinstance(result, psycopg.errors.RaiseException)
                assert "structural-open contribution lacks its exact event" in str(
                    result
                )
                expected_value = new_value
                expected_contribution_count = 0
        finally:
            executor.shutdown(wait=True, cancel_futures=True)

        assert connection.execute(
            sql.SQL("SELECT {} FROM groundloop_epoch WHERE epoch_id = %s").format(
                sql.Identifier(column_name)
            ),
            (epoch_id,),
        ).fetchone() == (expected_value,)
        assert connection.execute(
            """
            SELECT count(*) FROM groundloop_m5_runtime_work_contribution
            WHERE epoch_id = %s AND contribution_kind = 'structural_open'
            """,
            (epoch_id,),
        ).fetchone() == (expected_contribution_count,)


def test_structural_source_allows_unrelated_revision_and_status() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        database = _prepare_direct_runtime(connection)
        epoch_id = _open_alternate_runtime_header(connection, database)
        identity = connection.execute(
            "SELECT event_id, payload_hash FROM groundloop_epoch WHERE epoch_id = %s",
            (epoch_id,),
        ).fetchone()
        assert identity is not None
        connection.commit()
        with connection.transaction():
            _insert_work_contribution(
                connection,
                epoch_id=epoch_id,
                contribution_kind="structural_open",
                source_id=str(identity[0]),
                source_identity_hash=str(identity[1]).strip(),
                applied_revision=1,
                values=tuple(0 for _ in WORK_COUNTER_COLUMNS),
            )
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        with connection.transaction():
            connection.execute(
                "SELECT groundloop_m5_authorize_checked_transition(%s, 1)",
                (epoch_id,),
            )
            assert (
                connection.execute(
                    "UPDATE groundloop_epoch SET revision = 2 WHERE epoch_id = %s",
                    (epoch_id,),
                ).rowcount
                == 1
            )
            assert (
                connection.execute(
                    """
                UPDATE groundloop_m5_runtime_epoch
                SET revision = 2, runtime_state = 'semantic_pending'
                WHERE epoch_id = %s
                """,
                    (epoch_id,),
                ).rowcount
                == 1
            )
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

        assert connection.execute(
            """
            SELECT base.revision, base.structural_status, runtime.revision,
                   runtime.runtime_state
            FROM groundloop_epoch AS base
            JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
            WHERE base.epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone() == (2, "committed", 2, "semantic_pending")


def test_terminal_cutoff_rejects_late_event_work_and_attempt_timing_without_drift() -> (
    None
):
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        database = _prepare_direct_runtime(connection)
        epoch_id = _open_alternate_runtime_header(connection, database)
        logical_result_hash = _terminalize_recovered_epoch_for_audit(
            connection,
            epoch_id=epoch_id,
        )
        connection.commit()
        before = connection.execute(
            """
            SELECT to_jsonb(work), to_jsonb(timing), to_jsonb(result)
            FROM groundloop_m5_runtime_work_accumulator AS work
            JOIN groundloop_m5_runtime_timing_accumulator AS timing USING (epoch_id)
            JOIN groundloop_m5_event_result AS result USING (epoch_id)
            WHERE work.epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        assert before is not None
        connection.commit()

        with pytest.raises(
            psycopg.errors.RaiseException,
            match="terminal M5 epoch rejects event-accounted work or timing",
        ):
            _insert_work_contribution(
                connection,
                epoch_id=epoch_id,
                contribution_kind="direct_acquisition",
                source_id=_sha("postterminal-dispatch"),
                source_identity_hash=_sha("postterminal-dispatch"),
                applied_revision=2,
                values=tuple(0 for _ in WORK_COUNTER_COLUMNS),
            )
        connection.rollback()

        with pytest.raises(
            psycopg.errors.RaiseException,
            match="terminal M5 epoch rejects event-accounted work or timing",
        ):
            _insert_attempt_timing_contribution(
                connection,
                epoch_id=epoch_id,
                attempt_id="postterminal-attempt",
                evidence_digest=_sha("postterminal-evidence"),
                attempt_timing_digest=_sha("postterminal-timing"),
            )
        connection.rollback()

        assert (
            connection.execute(
                """
            SELECT to_jsonb(work), to_jsonb(timing), to_jsonb(result)
            FROM groundloop_m5_runtime_work_accumulator AS work
            JOIN groundloop_m5_runtime_timing_accumulator AS timing USING (epoch_id)
            JOIN groundloop_m5_event_result AS result USING (epoch_id)
            WHERE work.epoch_id = %s
              AND result.logical_result_hash = %s
            """,
                (epoch_id, logical_result_hash),
            ).fetchone()
            == before
        )


@pytest.mark.parametrize("surface", ("work", "timing"))
def test_terminalization_serializes_against_concurrent_event_accounting_append(
    surface: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        database = _prepare_direct_runtime(connection)
        epoch_id = _open_alternate_runtime_header(connection, database)
        schema_name = _current_schema_name(connection)
        runtime_locked = Event()
        continue_after_runtime_lock = Event()
        append_started = Event()
        executor = ThreadPoolExecutor(max_workers=2)
        try:
            terminal_future = executor.submit(
                _run_terminalization,
                schema_name=schema_name,
                epoch_id=epoch_id,
                runtime_locked=runtime_locked,
                continue_after_runtime_lock=continue_after_runtime_lock,
            )
            assert runtime_locked.wait(timeout=10)
            append_future = executor.submit(
                _run_event_accounting_insert,
                schema_name=schema_name,
                epoch_id=epoch_id,
                surface=surface,
                started=append_started,
            )
            assert append_started.wait(timeout=5)
            with pytest.raises(FutureTimeoutError):
                append_future.result(timeout=0.25)
            continue_after_runtime_lock.set()
            assert terminal_future.result(timeout=10) is None
            append_result = append_future.result(timeout=10)
            assert isinstance(append_result, psycopg.errors.RaiseException)
            assert "terminal M5 epoch rejects event-accounted work or timing" in str(
                append_result
            )
        finally:
            continue_after_runtime_lock.set()
            executor.shutdown(wait=True, cancel_futures=True)

        assert connection.execute(
            "SELECT runtime_state FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s",
            (epoch_id,),
        ).fetchone() == ("failed",)
        assert connection.execute(
            """
            SELECT count(*)
            FROM groundloop_m5_runtime_work_contribution
            WHERE epoch_id = %s AND source_id = %s
            """,
            (epoch_id, _sha("concurrent-postterminal-dispatch")),
        ).fetchone() == (0,)
        assert connection.execute(
            """
            SELECT count(*)
            FROM groundloop_m5_runtime_timing_contribution
            WHERE epoch_id = %s AND attempt_id = 'concurrent-postterminal-attempt'
            """,
            (epoch_id,),
        ).fetchone() == (0,)


def test_terminal_revision_includes_same_transaction_work_and_timing() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="discovery",
            expire_attempt=False,
        )
        connection.commit()
        with connection.transaction():
            _insert_direct_dispatch_and_evidence(
                connection,
                envelope_row=row,
                disposition="returned",
                insert_evidence=False,
            )

        attempt_values = [0 for _ in WORK_COUNTER_COLUMNS]
        attempt_values[3] = 1
        attempt_values[27] = 1
        attempt_values[29] = 23
        attempt_timing_values: tuple[int | None, ...] = (
            3,
            5,
            7,
            11,
            13,
            17,
            19,
            23,
            29,
        )
        _terminalize_recovered_epoch_for_audit(
            connection,
            epoch_id=int(row["epoch_id"]),
            terminal_attempt_row=row,
            terminal_attempt_values=tuple(attempt_values),
            terminal_attempt_timing_observed=True,
            terminal_attempt_timing_values=attempt_timing_values,
        )
        connection.commit()

        assert connection.execute(
            """
            SELECT runtime.runtime_state,
                   count(DISTINCT contribution.source_id),
                   count(DISTINCT timing.attempt_id),
                   work.direct_discovery_call_count,
                   work.embedding_model_call_count,
                   work.embedding_input_token_count,
                   work.work_digest,
                   event_work.direct_discovery_call_count,
                   event_work.embedding_model_call_count,
                   event_work.embedding_input_token_count,
                   result.coordinator_non_db_non_neural_ns,
                   result.neural_wall_ns,
                   result.postgres_roundtrip_wall_ns,
                   result.external_io_wall_ns,
                   result.end_to_end_wall_ns,
                   accumulator.required_expected_count,
                   accumulator.required_observed_count,
                   accumulator.required_missing_count,
                   coverage.required_expected_count,
                   coverage.required_observed_count,
                   coverage.required_missing_count
            FROM groundloop_m5_runtime_epoch AS runtime
            JOIN groundloop_m5_runtime_work_contribution AS contribution
              ON contribution.epoch_id = runtime.epoch_id
             AND contribution.contribution_kind = 'direct_attempt_execution'
             AND contribution.applied_revision = runtime.revision
            JOIN groundloop_m5_runtime_timing_contribution AS timing
              ON timing.epoch_id = contribution.epoch_id
             AND timing.attempt_id = contribution.source_id
            JOIN groundloop_m5_runtime_timing_accumulator AS accumulator
              ON accumulator.epoch_id = runtime.epoch_id
            JOIN groundloop_m5_event_timing_coverage AS coverage
              ON coverage.epoch_id = runtime.epoch_id
            JOIN groundloop_m5_runtime_work_accumulator AS work
              ON work.epoch_id = runtime.epoch_id
            JOIN groundloop_m5_runtime_work AS event_work
              ON event_work.epoch_id = runtime.epoch_id
             AND event_work.work_kind = 'event'
            JOIN groundloop_m5_event_result AS result
              ON result.epoch_id = runtime.epoch_id
            WHERE runtime.epoch_id = %s
            GROUP BY runtime.runtime_state,
                     work.direct_discovery_call_count,
                     work.embedding_model_call_count,
                     work.embedding_input_token_count,
                     work.work_digest,
                     event_work.direct_discovery_call_count,
                     event_work.embedding_model_call_count,
                     event_work.embedding_input_token_count,
                     result.coordinator_non_db_non_neural_ns,
                     result.neural_wall_ns,
                     result.postgres_roundtrip_wall_ns,
                     result.external_io_wall_ns,
                     result.end_to_end_wall_ns,
                     accumulator.required_expected_count,
                     accumulator.required_observed_count,
                     accumulator.required_missing_count,
                     coverage.required_expected_count,
                     coverage.required_observed_count,
                     coverage.required_missing_count
            """,
            (row["epoch_id"],),
        ).fetchone() == (
            "failed",
            1,
            1,
            1,
            1,
            23,
            _runtime_work_digest(tuple(attempt_values)),
            1,
            1,
            23,
            *attempt_timing_values[:5],
            2,
            1,
            1,
            2,
            1,
            1,
        )


def test_terminal_revision_rejects_attempt_contribution_without_attempt_timing() -> (
    None
):
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="discovery",
            expire_attempt=False,
        )
        connection.commit()
        with connection.transaction():
            _insert_direct_dispatch_and_evidence(
                connection,
                envelope_row=row,
                disposition="returned",
                insert_evidence=False,
            )

        with pytest.raises(
            psycopg.errors.RaiseException,
            match=(
                "terminal timing accumulator does not freeze exact revision slice"
                "|M5 execution evidence lacks one exclusive accounting branch"
                "|M5 attempt contribution lacks exact evidence"
            ),
        ):
            _terminalize_recovered_epoch_for_audit(
                connection,
                epoch_id=int(row["epoch_id"]),
                terminal_attempt_row=row,
                include_terminal_attempt_timing=False,
            )
        connection.rollback()

        assert connection.execute(
            "SELECT runtime_state FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s",
            (row["epoch_id"],),
        ).fetchone() != ("failed",)


def test_terminal_revision_rejects_attempt_kind_transition_call_timing_anchor() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="discovery",
            expire_attempt=False,
        )
        connection.commit()
        with connection.transaction():
            _insert_direct_dispatch_and_evidence(
                connection,
                envelope_row=row,
                disposition="returned",
                insert_evidence=False,
            )

        with pytest.raises(
            psycopg.errors.RaiseException,
            match=(
                "terminal revision cannot have a transition-call timing row"
                "|terminal timing accumulator does not freeze exact revision slice"
            ),
        ):
            _terminalize_recovered_epoch_for_audit(
                connection,
                epoch_id=int(row["epoch_id"]),
                terminal_attempt_row=row,
                include_terminal_transition_timing=True,
            )
        connection.rollback()

        assert connection.execute(
            "SELECT runtime_state FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s",
            (row["epoch_id"],),
        ).fetchone() != ("failed",)


@pytest.mark.parametrize(
    ("contribution_kind", "expected_error"),
    (
        ("structural_open", "structural-open contribution lacks"),
        ("root_barrier", "root-barrier contribution lacks"),
        ("terminal_job_failure", "terminal-job-failure contribution lacks"),
        ("direct_transition", "direct-transition contribution lacks"),
        ("epoch_failure", "epoch-failure contribution lacks"),
        ("seal", "seal contribution lacks"),
    ),
)
def test_work_contribution_rejects_floating_or_forged_source_closure(
    contribution_kind: str,
    expected_error: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(connection, return_kind="discovery")
        runtime_row = connection.execute(
            """
            SELECT structural_event_id, revision
            FROM groundloop_m5_runtime_epoch
            WHERE epoch_id = %s
            """,
            (row["epoch_id"],),
        ).fetchone()
        assert runtime_row is not None
        source_id = (
            str(runtime_row[0])
            if contribution_kind
            in {"structural_open", "root_barrier", "epoch_failure", "seal"}
            else _sha(f"missing-{contribution_kind}-source")
        )

        _insert_work_contribution(
            connection,
            epoch_id=int(row["epoch_id"]),
            contribution_kind=contribution_kind,
            source_id=source_id,
            source_identity_hash=_sha(f"forged-{contribution_kind}-identity"),
            applied_revision=int(runtime_row[1]),
            values=tuple(0 for _ in WORK_COUNTER_COLUMNS),
        )
        with pytest.raises(psycopg.errors.RaiseException, match=expected_error):
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()


@pytest.mark.parametrize("mutation", (None, "digest", "count"))
def test_cancellation_contribution_binds_exact_plan_and_cancelled_job_count(
    mutation: str | None,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_requirement_audit_attempt(connection)
        _cancel_requirement_job_for_audit(connection, row)
        plan = connection.execute(
            """
            SELECT groundloop_m5_recovery_cancellation_plan_digest(
                %s, %s, 'subject_inactive', %s
            )
            """,
            (
                row["epoch_id"],
                row["structural_event_id"],
                row["dispatched_revision"] + 1,
            ),
        ).fetchone()
        assert plan is not None
        plan_digest = str(plan[0]).strip()
        values = [0 for _ in WORK_COUNTER_COLUMNS]
        values[15] = 2 if mutation == "count" else 1
        source_digest = (
            _sha("forged-cancellation-plan") if mutation == "digest" else plan_digest
        )

        _insert_work_contribution(
            connection,
            epoch_id=int(row["epoch_id"]),
            contribution_kind="cancellation",
            source_id=source_digest,
            source_identity_hash=source_digest,
            applied_revision=int(row["dispatched_revision"]) + 1,
            values=tuple(values),
        )
        if mutation is None:
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
            connection.commit()
            assert connection.execute(
                """
                SELECT requirement_cancelled_job_count
                FROM groundloop_m5_runtime_work_contribution
                WHERE epoch_id = %s AND contribution_kind = 'cancellation'
                """,
                (row["epoch_id"],),
            ).fetchone() == (1,)
        else:
            with pytest.raises(
                psycopg.errors.RaiseException,
                match=(
                    "cancellation contribution lacks exact plan and cancelled-job count"
                ),
            ):
                connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
            connection.rollback()


def test_attempt_expired_check_retains_both_frozen_same_state_branches() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = connection.execute(
            """
            SELECT pg_get_constraintdef(oid, true)
            FROM pg_constraint
            WHERE conrelid =
                  'groundloop_m5_attempt_result_artifact'::regclass
              AND conname = 'groundloop_m5_attempt_result_artifact_check1'
            """
        ).fetchone()
        assert row is not None
        definition = str(row[0])
        assert "disposition = 'terminal_audit_only'::text" in definition
        assert "job_state_at_receipt = 'running'::text" in definition
        assert "job_state_after = 'running'::text" in definition
        assert "archive_reason = 'attempt_expired'::text" in definition
        for terminal_state in (
            "completed_active",
            "completed_inactive",
            "terminal_failed",
            "cancelled",
        ):
            assert terminal_state in definition
        assert "job_state_after = job_state_at_receipt" in definition


def test_catalog_inventory_definitions_indexes_constraints_and_triggers() -> None:
    with _isolated_015_schema() as connection:
        constraints_before, trigger_before, function_before = _attempt_result_catalog(
            connection
        )
        preexisting_triggers = {
            int(row[0]): str(row[1])
            for row in connection.execute(
                """
                SELECT oid, pg_get_triggerdef(oid, true)
                FROM pg_trigger
                WHERE tgrelid IN (
                    'groundloop_m5_job_attempt'::regclass,
                    'groundloop_m5_attempt_result_artifact'::regclass
                ) AND NOT tgisinternal
                """
            ).fetchall()
        }

        install_m5_runtime_recovery_bundle(connection)
        connection.commit()

        assert set(RECOVERY_RELATIONS).issubset(_current_relations(connection))
        attempt_columns = {
            str(row[0]): (str(row[1]), bool(row[2]))
            for row in connection.execute(
                """
                SELECT attname, format_type(atttypid, atttypmod), attnotnull
                FROM pg_attribute
                WHERE attrelid = 'groundloop_m5_job_attempt'::regclass
                  AND attnum > 0 AND NOT attisdropped
                  AND attname IN ('lease_expires_at', 'attempt_work_digest')
                ORDER BY attname
                """
            ).fetchall()
        }
        assert attempt_columns == {
            "attempt_work_digest": ("character(64)", True),
            "lease_expires_at": ("timestamp with time zone", True),
        }

        for relation in (
            "groundloop_m5_runtime_work_contribution",
            "groundloop_m5_runtime_work_accumulator",
        ):
            actual = tuple(
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT attname FROM pg_attribute
                    WHERE attrelid = to_regclass(%s)
                      AND attnum > 0 AND NOT attisdropped
                      AND attname = ANY(%s::text[])
                    ORDER BY attnum
                    """,
                    (relation, list(WORK_COUNTER_COLUMNS)),
                ).fetchall()
            )
            assert actual == WORK_COUNTER_COLUMNS

        dispatch_columns = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT attname FROM pg_attribute
                WHERE attrelid = 'groundloop_m5_dispatch_record'::regclass
                  AND attnum > 0 AND NOT attisdropped
                """
            ).fetchall()
        }
        assert {f"maximum_{name}" for name in WORK_COUNTER_COLUMNS}.issubset(
            dispatch_columns
        )
        evidence_columns = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT attname FROM pg_attribute
                WHERE attrelid =
                      'groundloop_m5_attempt_execution_evidence'::regclass
                  AND attnum > 0 AND NOT attisdropped
                """
            ).fetchall()
        }
        assert {f"attempt_{name}" for name in WORK_COUNTER_COLUMNS}.issubset(
            evidence_columns
        )

        indexed = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT DISTINCT table_row.relname
                FROM pg_index AS index_row
                JOIN pg_class AS table_row ON table_row.oid = index_row.indrelid
                JOIN pg_namespace AS namespace ON namespace.oid = table_row.relnamespace
                WHERE namespace.nspname = current_schema()
                  AND table_row.relname = ANY(%s::text[])
                  AND index_row.indisvalid
                """,
                (
                    [
                        "groundloop_m5_dispatch_record",
                        "groundloop_m5_runtime_work_contribution",
                        "groundloop_m5_transition_call_timing",
                        "groundloop_m5_expired_attempt_return",
                        "groundloop_m5_post_terminal_attempt_audit",
                    ],
                ),
            ).fetchall()
        }
        assert indexed == {
            "groundloop_m5_dispatch_record",
            "groundloop_m5_runtime_work_contribution",
            "groundloop_m5_transition_call_timing",
            "groundloop_m5_expired_attempt_return",
            "groundloop_m5_post_terminal_attempt_audit",
        }

        immutable_expected = set(RECOVERY_RELATIONS) - {
            "groundloop_m5_runtime_work_accumulator",
            "groundloop_m5_runtime_timing_accumulator",
        }
        immutable_actual = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT relation.relname
                FROM pg_trigger AS trigger_row
                JOIN pg_class AS relation ON relation.oid = trigger_row.tgrelid
                JOIN pg_proc AS function_row ON function_row.oid = trigger_row.tgfoid
                WHERE NOT trigger_row.tgisinternal
                  AND function_row.proname = 'groundloop_m5_reject_immutable_row'
                  AND relation.relname = ANY(%s::text[])
                """,
                (list(RECOVERY_RELATIONS),),
            ).fetchall()
        }
        assert immutable_actual == immutable_expected

        constraints_after, trigger_after, function_after = _attempt_result_catalog(
            connection
        )
        changed_constraints = {
            name
            for name in constraints_before
            if constraints_before[name] != constraints_after[name]
        }
        assert changed_constraints == {
            "groundloop_m5_attempt_result_artifact_archive_reason_check",
            "groundloop_m5_attempt_result_artifact_check1",
        }
        assert constraints_before.keys() == constraints_after.keys()
        assert (
            "attempt_expired"
            in constraints_after[
                "groundloop_m5_attempt_result_artifact_archive_reason_check"
            ]
        )
        assert (
            "attempt_expired"
            in constraints_after["groundloop_m5_attempt_result_artifact_check1"]
        )
        assert function_before != function_after
        assert "groundloop_m5_expired_attempt_return" in function_after
        assert "attempt_expired" in function_after
        # CREATE OR REPLACE keeps both the trigger and function OIDs; migration
        # 016 must not drop/recreate the constraint trigger.
        assert trigger_after == trigger_before

        existing_after = {
            int(row[0]): str(row[1])
            for row in connection.execute(
                """
                SELECT oid, pg_get_triggerdef(oid, true)
                FROM pg_trigger
                WHERE oid = ANY(%s::oid[])
                """,
                (list(preexisting_triggers),),
            ).fetchall()
        }
        assert existing_after == preexisting_triggers

        trigger_functions = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT function_row.proname
                FROM pg_trigger AS trigger_row
                JOIN pg_proc AS function_row ON function_row.oid = trigger_row.tgfoid
                JOIN pg_class AS relation ON relation.oid = trigger_row.tgrelid
                WHERE NOT trigger_row.tgisinternal
                  AND relation.relname = ANY(%s::text[])
                """,
                (list(RECOVERY_RELATIONS),),
            ).fetchall()
        }
        assert {
            "groundloop_m5_validate_dispatch_record",
            "groundloop_m5_validate_work_contribution",
            "groundloop_m5_validate_timing_row",
            "groundloop_m5_validate_late_return_rows",
            "groundloop_m5_reject_immutable_row",
        }.issubset(trigger_functions)


def test_migration_015_bytes_and_ledger_row_remain_unchanged() -> None:
    migration_015_path = Path(__file__).resolve().parents[3] / (
        "migrations/015_m5_runtime.sql"
    )
    bytes_before = migration_015_path.read_bytes()
    assert hashlib.sha256(bytes_before).hexdigest() == (
        M5_ACCEPTED_RUNTIME_MIGRATION_SHA256
    )

    with _isolated_015_schema() as connection:
        accepted_before = _ledger_row(connection, M5_ACCEPTED_RUNTIME_BUNDLE_ID)
        assert accepted_before is not None
        assert accepted_before[:5] == (
            M5_ACCEPTED_RUNTIME_BUNDLE_ID,
            M5_ACCEPTED_RUNTIME_BUNDLE_SHA256,
            M5_ACCEPTED_RUNTIME_MIGRATION_SHA256,
            M5_ACCEPTED_RUNTIME_ORACLE_SHA256,
            M5_ACCEPTED_RUNTIME_PREREQUISITE_SHA256,
        )

        install_m5_runtime_recovery_bundle(connection)
        connection.commit()

        assert _ledger_row(connection, M5_ACCEPTED_RUNTIME_BUNDLE_ID) == (
            accepted_before
        )
        assert migration_015_path.read_bytes() == bytes_before


def test_frozen_install_lock_order_is_exact() -> None:
    assert M5_RUNTIME_RECOVERY_INSTALL_LOCK_RELATIONS == (
        "groundloop_runtime_mode",
        "groundloop_m4_publication_head",
        "groundloop_m5_publication_head",
        "groundloop_epoch",
        "groundloop_m5_runtime_epoch",
        "groundloop_semantic_job_attempt",
        "groundloop_m5_job_attempt",
    )
