"""Live PostgreSQL acceptance tests for frozen M5-D24 migration 016."""

from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import Connection, sql
from psycopg.types.json import Jsonb

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


def _seed_fake_attempt(
    connection: Connection[Any], *, subgraph: str, after_016: bool
) -> None:
    """Seed a check-valid row while bypassing FKs for install-guard tests."""

    connection.execute("SET session_replication_role = replica")
    try:
        if subgraph == "requirement":
            columns = """
                attempt_id, logical_job_id, attempt_ordinal,
                execution_spec_hash, lease_token_hash, attempt_state,
                attempt_output_digest, error_hash, dispatched_at, finished_at
            """
            values = """
                %s, %s, 1, %s, %s, 'dispatched', NULL, NULL,
                clock_timestamp(), NULL
            """
            parameters: tuple[object, ...] = (
                _sha("fake-m5-attempt"),
                _sha("fake-m5-job"),
                _sha("fake-execution"),
                _sha("fake-lease"),
            )
            if after_016:
                columns += ", lease_expires_at, attempt_work_digest"
                values += ", clock_timestamp() + interval '1 minute', %s"
                parameters += (
                    "5304dcf5375d76796e45635d349cf1ddcc00218049f70859c17649bc8ab46bd2",
                )
            connection.execute(
                f"INSERT INTO groundloop_m5_job_attempt ({columns}) VALUES ({values})",
                parameters,
            )
            return

        epoch_id = 987654321
        connection.execute(
            """
            INSERT INTO groundloop_epoch (
                epoch_id, event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) OVERRIDING SYSTEM VALUE VALUES (
                %s, 'fake-direct-event', %s, 1, 'failed', 'failed', 'failed',
                'provisional', NULL
            )
            """,
            (epoch_id, _sha("fake-direct-payload")),
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
                open_work_count, open_scope_count, blocking_failure_count,
                terminal_at
            ) VALUES (
                %s, 'fake-direct-event', 'fake-policy', %s, %s, %s, %s, %s,
                'failed', 1, 0, 0, 0, clock_timestamp()
            )
            """,
            (
                epoch_id,
                _sha("fake-policy-manifest"),
                _sha("fake-requirements"),
                _sha("fake-chunks"),
                epoch_id - 1,
                _sha("fake-roots"),
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_semantic_job (
                job_id, epoch_id, parent_job_id, job_kind,
                candidate_policy_id, payload_hash, execution_spec_hash,
                claim_id, chunk_version_id, expandable, job_state,
                child_closed, created_revision
            ) VALUES (
                'fake-direct-job', %s, NULL, 'impact_discovery', 'fake-policy',
                %s, %s, NULL, 'fake-chunk', true, 'running', false, 1
            )
            """,
            (epoch_id, _sha("fake-direct-job-payload"), _sha("fake-execution")),
        )
        connection.execute(
            """
            INSERT INTO groundloop_semantic_job_attempt (
                attempt_id, job_id, execution_spec_hash, attempt_ordinal,
                lease_token_hash, attempt_state, lease_expires_at,
                started_at, finished_at
            ) VALUES (
                'fake-direct-attempt', 'fake-direct-job', %s, 1, %s,
                'leased', clock_timestamp() + interval '1 minute',
                clock_timestamp(), NULL
            )
            """,
            (_sha("fake-execution"), _sha("fake-direct-lease")),
        )
    finally:
        connection.execute("SET session_replication_role = origin")


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
        sequence_field(()),
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


def _seed_direct_envelope_support(
    connection: Connection[Any], *, return_kind: str
) -> dict[str, Any]:
    epoch_id = 987650001 if return_kind == "discovery" else 987650002
    event_id = f"late-{return_kind}-event"
    job_id = _sha(f"late-{return_kind}-job")
    policy_id = f"late-{return_kind}-policy"
    payload_hash = _sha(f"late-{return_kind}-payload")
    execution_spec_hash = _sha(f"late-{return_kind}-execution")
    claim_id = f"late-{return_kind}-claim"
    chunk_version_id = f"late-{return_kind}-chunk"
    job_kind = "impact_discovery" if return_kind == "discovery" else "verify_pair"
    attempt_id = stable_m4_digest("m4-job-attempt-v1", job_id, "1")
    lease_token_hash = stable_m4_digest("m4-lease-token-v1", job_id, "1")
    result_artifact_id = _sha(f"late-{return_kind}-artifact-id")
    result_artifact_hash = _sha(f"late-{return_kind}-artifact")

    job_binding = {
        "job_id": job_id,
        "event_id": event_id,
        "job_kind": job_kind,
        "candidate_policy_id": policy_id,
        "payload_hash": payload_hash,
        "execution_spec_hash": execution_spec_hash,
        "parent_job_id": None,
        "pair_claim_id": claim_id if return_kind == "verifier" else None,
        "pair_chunk_version_id": (
            chunk_version_id if return_kind == "verifier" else None
        ),
        "target_claim_id": None,
        "target_chunk_version_id": (
            chunk_version_id if return_kind == "discovery" else None
        ),
        "expandable": return_kind == "discovery",
    }
    job_binding_digest = stable_m5_digest(
        "m5-typed-direct-late-job-binding-v1",
        text_field(job_id),
        text_field(event_id),
        enum_field(job_kind),
        text_field(policy_id),
        hash_field(payload_hash),
        hash_field(execution_spec_hash),
        option_field(None),
        option_field(text_field(claim_id) if return_kind == "verifier" else None),
        option_field(
            text_field(chunk_version_id) if return_kind == "verifier" else None
        ),
        option_field(None),
        option_field(
            text_field(chunk_version_id) if return_kind == "discovery" else None
        ),
        bool_field(return_kind == "discovery"),
    )
    attempt_binding = {
        "attempt_id": attempt_id,
        "job_id": job_id,
        "execution_spec_hash": execution_spec_hash,
        "attempt_ordinal": 1,
        "lease_token_hash": lease_token_hash,
    }
    attempt_binding_digest = stable_m5_digest(
        "m5-typed-direct-late-attempt-binding-v1",
        text_field(attempt_id),
        text_field(job_id),
        hash_field(execution_spec_hash),
        int_field(1),
        hash_field(lease_token_hash),
    )

    child_set_hash = (
        stable_m4_digest("m4-child-set-v1") if return_kind == "discovery" else None
    )
    child_completion_digest = (
        stable_m4_digest(
            "m4-expandable-completion-v1",
            job_id,
            result_artifact_hash,
            child_set_hash,
        )
        if child_set_hash is not None
        else None
    )
    m4_completion_digest = stable_m4_digest(
        "m4-job-completion-v1",
        job_id,
        payload_hash,
        execution_spec_hash,
        result_artifact_id,
        result_artifact_hash,
        "completed_active",
        child_set_hash or "",
    )
    completion_binding: dict[str, Any] = {
        "job_id": job_id,
        "payload_hash": payload_hash,
        "execution_spec_hash": execution_spec_hash,
        "result_artifact_id": result_artifact_id,
        "result_artifact_hash": result_artifact_hash,
        "terminal_state": "completed_active",
        "completion_digest": m4_completion_digest,
        "child_parent_job_id": job_id if return_kind == "discovery" else None,
        "child_completion_digest": child_completion_digest,
        "child_set_hash": child_set_hash,
        "child_job_ids": [],
    }
    completion_binding_digest = _typed_direct_completion_digest(
        job_id=job_id,
        payload_hash=payload_hash,
        execution_spec_hash=execution_spec_hash,
        result_artifact_id=result_artifact_id,
        result_artifact_hash=result_artifact_hash,
        completion_digest=m4_completion_digest,
        child_parent_job_id=(job_id if return_kind == "discovery" else None),
        child_completion_digest=child_completion_digest,
        child_set_hash=child_set_hash,
    )

    row: dict[str, Any] = {
        "epoch_id": epoch_id,
        "return_kind": return_kind,
        "job_id": job_id,
        "attempt_id": attempt_id,
        "result_artifact_id": result_artifact_id,
        "result_artifact_hash": result_artifact_hash,
        "job_binding": job_binding,
        "attempt_binding": attempt_binding,
        "completion_binding": completion_binding,
        "job_binding_digest": job_binding_digest,
        "attempt_binding_digest": attempt_binding_digest,
        "completion_binding_digest": completion_binding_digest,
    }

    connection.execute("SET session_replication_role = replica")
    try:
        connection.execute(
            """
            INSERT INTO groundloop_epoch (
                epoch_id, event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) OVERRIDING SYSTEM VALUE VALUES (
                %s, %s, %s, 1, 'failed', 'failed', 'failed',
                'provisional', NULL
            )
            """,
            (epoch_id, event_id, payload_hash),
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
                open_work_count, open_scope_count, blocking_failure_count,
                terminal_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                'failed', 1, 0, 0, 0, clock_timestamp()
            )
            """,
            (
                epoch_id,
                event_id,
                policy_id,
                _sha(f"{return_kind}-manifest"),
                _sha(f"{return_kind}-requirements"),
                _sha(f"{return_kind}-chunks"),
                epoch_id - 1,
                _sha(f"{return_kind}-roots"),
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_semantic_job (
                job_id, epoch_id, parent_job_id, job_kind,
                candidate_policy_id, payload_hash, execution_spec_hash,
                claim_id, chunk_version_id, expandable, job_state,
                child_closed, created_revision
            ) VALUES (
                %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s,
                'running', false, 1
            )
            """,
            (
                job_id,
                epoch_id,
                job_kind,
                policy_id,
                payload_hash,
                execution_spec_hash,
                claim_id if return_kind == "verifier" else None,
                chunk_version_id,
                return_kind == "discovery",
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_semantic_job_attempt (
                attempt_id, job_id, execution_spec_hash, attempt_ordinal,
                lease_token_hash, attempt_state, lease_expires_at,
                started_at, finished_at
            ) VALUES (
                %s, %s, %s, 1, %s, 'expired',
                clock_timestamp() - interval '1 minute',
                clock_timestamp() - interval '2 minutes', clock_timestamp()
            )
            """,
            (attempt_id, job_id, execution_spec_hash, lease_token_hash),
        )

        if return_kind == "discovery":
            registry_snapshot_id = f"late-{return_kind}-registry"
            claim_set_hash = stable_m4_digest("m4-claim-registry-snapshot-v1", claim_id)
            connection.execute(
                """
                INSERT INTO groundloop_m4_claim_registry_snapshot (
                    claim_registry_snapshot_id, claim_count, claim_set_hash
                ) VALUES (%s, 1, %s)
                """,
                (registry_snapshot_id, claim_set_hash),
            )
            connection.execute(
                """
                INSERT INTO groundloop_m4_claim_registry_member (
                    claim_registry_snapshot_id, claim_id, member_ordinal
                ) VALUES (%s, %s, 0)
                """,
                (registry_snapshot_id, claim_id),
            )
            connection.execute(
                """
                INSERT INTO groundloop_discovery_scope (
                    root_job_id, epoch_id, registry_snapshot_id, scope_kind,
                    explicit_claim_ids, closed_revision
                ) VALUES (%s, %s, %s, 'all_registered_claims', NULL, 1)
                """,
                (job_id, epoch_id, registry_snapshot_id),
            )
            channel_set_hash = stable_m4_digest("m4-discovery-channel-set-v1")
            admitted_pair_set_hash = stable_m4_digest("m4-discovery-admitted-set-v1")
            discovery_binding = {
                "root_job_id": job_id,
                "result_artifact_id": result_artifact_id,
                "result_artifact_hash": result_artifact_hash,
                "fallback_satisfied": False,
                "channel_hit_count": 0,
                "admitted_pair_count": 0,
                "channel_set_hash": channel_set_hash,
                "admitted_pair_set_hash": admitted_pair_set_hash,
                "channel_hits": [],
                "admitted_pairs": [],
            }
            discovery_binding_digest = stable_m5_digest(
                "m5-typed-direct-late-discovery-binding-v1",
                text_field(job_id),
                text_field(result_artifact_id),
                hash_field(result_artifact_hash),
                bool_field(False),
                int_field(0),
                int_field(0),
                hash_field(channel_set_hash),
                hash_field(admitted_pair_set_hash),
                sequence_field(()),
                sequence_field(()),
            )
            scope_binding = {
                "root_job_id": job_id,
                "epoch_id": epoch_id,
                "registry_snapshot_id": registry_snapshot_id,
                "registered_claim_ids": [claim_id],
                "closed": True,
                "persisted_scope_kind": "all_registered_claims",
                "explicit_claim_ids": None,
                "closed_revision": 1,
            }
            scope_binding_digest = stable_m5_digest(
                "m5-typed-direct-late-scope-binding-v1",
                text_field(job_id),
                int_field(epoch_id),
                text_field(registry_snapshot_id),
                sequence_field((text_field(claim_id),)),
                bool_field(True),
                enum_field("all_registered_claims"),
                option_field(None),
                option_field(int_field(1)),
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
            observation_id = _sha("late-verifier-observation")
            admitted_pair_id = stable_m4_digest(
                "m4-admitted-pair-v1",
                str(epoch_id),
                claim_id,
                chunk_version_id,
                policy_id,
            )
            pair_input_hash = _sha("late-verifier-pair-input")
            calibration_hash = _sha("late-verifier-calibration")
            observation = {
                "observation_id": observation_id,
                "subject_kind": "claim",
                "subject_id": claim_id,
                "chunk_version_id": chunk_version_id,
                "task_type": "verification",
                "support_score": 0.7,
                "refute_score": 0.2,
                "neutral_score": 0.1,
                "model_id": "late-model",
                "model_version": "late-model-v1",
                "prompt_version": "late-prompt-v1",
                "input_hash": pair_input_hash,
                "produced_epoch": epoch_id,
                "raw_output_hash": result_artifact_hash,
                "eligible_for_currency": True,
                "requested_make_effective": False,
            }
            execution = {
                "observation_id": observation_id,
                "job_id": job_id,
                "admitted_pair_id": admitted_pair_id,
                "model_artifact_id": "late-model-artifact",
                "prompt_artifact_id": "late-prompt-artifact",
                "execution_spec_hash": execution_spec_hash,
                "pair_input_hash": pair_input_hash,
                "calibration_version": "late-calibration-v1",
                "calibration_artifact_sha256": calibration_hash,
                "temperature": 1.0,
                "raw_logits": [0.7, 0.2, 0.1],
                "raw_output_hash": result_artifact_hash,
                "reused_from_observation_id": None,
            }
            connection.execute(
                """
                INSERT INTO groundloop_semantic_observation (
                    observation_id, subject_kind, subject_id,
                    chunk_version_id, task_type, support_score, refute_score,
                    neutral_score, model_id, model_version, prompt_version,
                    input_hash, produced_epoch, raw_output_hash,
                    eligible_for_currency
                ) VALUES (
                    %s, 'claim', %s, %s, 'verification', 0.7, 0.2, 0.1,
                    'late-model', 'late-model-v1', 'late-prompt-v1',
                    %s, %s, %s, true
                )
                """,
                (
                    observation_id,
                    claim_id,
                    chunk_version_id,
                    pair_input_hash,
                    epoch_id,
                    result_artifact_hash,
                ),
            )
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
                    %s, 1.0, ARRAY[0.7, 0.2, 0.1]::double precision[],
                    %s, NULL
                )
                """,
                (
                    observation_id,
                    job_id,
                    admitted_pair_id,
                    execution_spec_hash,
                    pair_input_hash,
                    calibration_hash,
                    result_artifact_hash,
                ),
            )
            verifier_binding = {
                "result_artifact_id": result_artifact_id,
                "result_artifact_hash": result_artifact_hash,
                "verification_execution": execution,
                "observation": observation,
            }
            execution_fields = sequence_field(
                (
                    text_field(observation_id),
                    text_field(job_id),
                    hash_field(admitted_pair_id),
                    text_field("late-model-artifact"),
                    text_field("late-prompt-artifact"),
                    hash_field(execution_spec_hash),
                    hash_field(pair_input_hash),
                    text_field("late-calibration-v1"),
                    hash_field(calibration_hash),
                    f64_field(1.0),
                    sequence_field(f64_field(value) for value in (0.7, 0.2, 0.1)),
                    hash_field(result_artifact_hash),
                    option_field(None),
                )
            )
            verifier_binding_digest = stable_m5_digest(
                "m5-typed-direct-late-verifier-binding-v1",
                text_field(result_artifact_id),
                hash_field(result_artifact_hash),
                bool_field(True),
                option_field(execution_fields),
                text_field(observation_id),
                enum_field("claim"),
                text_field(claim_id),
                text_field(chunk_version_id),
                text_field("verification"),
                f64_field(0.7),
                f64_field(0.2),
                f64_field(0.1),
                text_field("late-model"),
                text_field("late-model-v1"),
                text_field("late-prompt-v1"),
                text_field(pair_input_hash),
                int_field(epoch_id),
                hash_field(result_artifact_hash),
                bool_field(True),
                bool_field(False),
            )
            row.update(
                {
                    "verification_execution_present": True,
                    "observation_eligible_for_currency": True,
                    "requested_make_effective": False,
                    "discovery_binding": None,
                    "scope_binding": None,
                    "verifier_binding": verifier_binding,
                    "discovery_binding_digest": None,
                    "scope_binding_digest": None,
                    "verifier_binding_digest": verifier_binding_digest,
                }
            )
    finally:
        connection.execute("SET session_replication_role = origin")

    row["envelope_digest"] = _typed_direct_envelope_digest(
        epoch_id=epoch_id,
        return_kind=return_kind,
        job_binding_digest=job_binding_digest,
        attempt_binding_digest=attempt_binding_digest,
        completion_binding_digest=completion_binding_digest,
        discovery_binding_digest=row["discovery_binding_digest"],
        scope_binding_digest=row["scope_binding_digest"],
        verifier_binding_digest=row["verifier_binding_digest"],
    )
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


def _seed_postterminal_audit_support(
    connection: Connection[Any], *, return_kind: str
) -> dict[str, Any]:
    epoch_id = 987650010 if return_kind == "expired_return" else 987650011
    structural_event_id = f"postterminal-{return_kind}-event"
    attempt_id = _sha(f"postterminal-{return_kind}-attempt")
    evidence_digest = _sha(f"postterminal-{return_kind}-evidence")
    work_digest = _sha(f"postterminal-{return_kind}-work")
    timing_digest = _sha(f"postterminal-{return_kind}-timing")
    logical_result_hash = _sha(f"postterminal-{return_kind}-result")
    result_or_error_hash = _sha(f"postterminal-{return_kind}-output")

    connection.execute("SET session_replication_role = replica")
    try:
        connection.execute(
            """
            INSERT INTO groundloop_epoch (
                epoch_id, event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) OVERRIDING SYSTEM VALUE VALUES (
                %s, %s, %s, 1, 'failed', 'failed', 'failed',
                'provisional', NULL
            )
            """,
            (epoch_id, structural_event_id, _sha(structural_event_id)),
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
                open_work_count, open_scope_count, blocking_failure_count,
                terminal_at
            ) VALUES (
                %s, %s, 'postterminal-policy', %s, %s, %s, %s, %s,
                'failed', 1, 0, 0, 0, clock_timestamp()
            )
            """,
            (
                epoch_id,
                structural_event_id,
                _sha("postterminal-policy-manifest"),
                _sha("postterminal-requirements"),
                _sha("postterminal-chunks"),
                epoch_id - 1,
                _sha("postterminal-roots"),
            ),
        )
        evidence_columns = [
            "epoch_id",
            *(f"attempt_{name}" for name in WORK_COUNTER_COLUMNS),
            "subgraph",
            "attempt_id",
            "disposition",
            "result_or_error_hash",
            "attempt_timing_digest",
            "attempt_work_digest",
            "evidence_digest",
        ]
        evidence_values: list[object] = [
            epoch_id,
            *(0 for _ in WORK_COUNTER_COLUMNS),
            "requirement",
            attempt_id,
            "returned",
            result_or_error_hash,
            timing_digest,
            work_digest,
            evidence_digest,
        ]
        connection.execute(
            sql.SQL(
                "INSERT INTO groundloop_m5_attempt_execution_evidence ({}) VALUES ({})"
            ).format(
                sql.SQL(", ").join(map(sql.Identifier, evidence_columns)),
                sql.SQL(", ").join(sql.Placeholder() for _ in evidence_columns),
            ),
            evidence_values,
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
                %s, 'requirement', %s, false,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                %s, %s
            )
            """,
            (
                epoch_id,
                attempt_id,
                _sha(f"postterminal-{return_kind}-observation"),
                timing_digest,
            ),
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
                0, 0, 0, 0, 0, NULL, NULL, NULL, NULL
            )
            """,
            (
                structural_event_id,
                _sha(structural_event_id),
                epoch_id,
                _sha(f"postterminal-{return_kind}-open-receipt"),
                _sha(f"postterminal-{return_kind}-event-work"),
                _sha(f"postterminal-{return_kind}-delta-set"),
                _sha(f"postterminal-{return_kind}-state-set"),
                logical_result_hash,
            ),
        )
    finally:
        connection.execute("SET session_replication_role = origin")

    return {
        "epoch_id": epoch_id,
        "subgraph": "requirement",
        "attempt_id": attempt_id,
        "return_kind": return_kind,
        "return_artifact_digest": _sha(f"postterminal-{return_kind}-artifact"),
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
        _seed_fake_attempt(connection, subgraph="requirement", after_016=True)
        connection.commit()

        replay = install_m5_runtime_recovery_bundle(connection)
        connection.commit()

        assert first.applied
        assert not replay.applied
        assert replay.identity == first.identity
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_job_attempt"
        ).fetchone() == (1,)


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
def test_first_install_rejects_every_existing_typed_attempt_family(
    subgraph: str,
) -> None:
    with _isolated_015_schema() as connection:
        _seed_fake_attempt(connection, subgraph=subgraph, after_016=False)
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
def test_first_install_accepts_terminal_zero_attempt_epochs(
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


@pytest.mark.parametrize("return_kind", ("discovery", "verifier"))
def test_typed_direct_late_envelope_accepts_exact_branch_closure(
    return_kind: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(connection, return_kind=return_kind)

        _insert_direct_envelope(connection, row)
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
        else:
            execution["model_artifact_id"] = "forged-model-artifact"

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
            "lacks exclusive requirement artifact",
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
        with pytest.raises(psycopg.errors.RaiseException, match=expected_error):
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()


@pytest.mark.parametrize(
    "job_state_at_receipt",
    (
        "running",
        "completed_active",
        "completed_inactive",
        "terminal_failed",
        "cancelled",
    ),
)
def test_attempt_expired_artifact_check_accepts_pre_and_postterminal_same_state(
    job_state_at_receipt: str,
) -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        connection.execute("SET session_replication_role = replica")
        try:
            suffix = job_state_at_receipt
            connection.execute(
                """
                INSERT INTO groundloop_m5_attempt_result_artifact (
                    attempt_result_artifact_id,
                    attempt_result_artifact_hash, attempt_output_digest,
                    attempt_id, logical_job_id, job_epoch_id, payload_hash,
                    execution_spec_hash, result_artifact_id,
                    result_artifact_hash, job_state_at_receipt,
                    job_state_after, disposition, activity_snapshot_epoch_id,
                    activity_snapshot_revision, epoch_active, chunk_active,
                    requirement_active, group_active, archive_reason,
                    cancelled_by_event_id, cancelled_by_epoch_id,
                    cancellation_reason
                ) VALUES (
                    %s, %s, %s, %s, %s, 987659999, %s, %s, %s, %s,
                    %s, %s, 'terminal_audit_only', 987659999, 1,
                    false, false, NULL, NULL, 'attempt_expired',
                    %s, %s, %s
                )
                """,
                (
                    _sha(f"expired-id-{suffix}"),
                    _sha(f"expired-hash-{suffix}"),
                    _sha(f"expired-output-{suffix}"),
                    _sha(f"expired-attempt-{suffix}"),
                    _sha(f"expired-job-{suffix}"),
                    _sha(f"expired-payload-{suffix}"),
                    _sha(f"expired-execution-{suffix}"),
                    _sha(f"expired-result-id-{suffix}"),
                    _sha(f"expired-result-hash-{suffix}"),
                    job_state_at_receipt,
                    job_state_at_receipt,
                    (
                        f"cancel-event-{suffix}"
                        if job_state_at_receipt == "cancelled"
                        else None
                    ),
                    987659999 if job_state_at_receipt == "cancelled" else None,
                    "epoch_failed" if job_state_at_receipt == "cancelled" else None,
                ),
            )
        finally:
            connection.execute("SET session_replication_role = origin")
        connection.rollback()


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
