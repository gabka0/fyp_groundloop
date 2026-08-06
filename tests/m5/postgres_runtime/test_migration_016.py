"""Live PostgreSQL acceptance tests for frozen M5-D24 migration 016."""

from __future__ import annotations

import hashlib
import os
import struct
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


def _seed_requirement_attempt(
    connection: Connection[Any], *, acquire: bool = True
) -> int:
    with connection.transaction():
        base = runtime_support.seed_base(
            connection,
            prefix="recovery-install-guard",
            claim_count=1,
            chunk_texts=("alpha", "beta"),
        )
        group = runtime_support.make_group(
            group_id="recovery-install-guard-group-v1",
            family_id="recovery-install-guard-family",
            claim_id=base.claim_ids[0],
            texts=("required fact",),
            source_id="recovery-install-guard-fixture",
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
            (_sha("embedding-config"),),
        )
    with connection.transaction():
        runtime_support.install_test_activation_barrier(
            connection,
            base,
            activation_id="recovery-install-guard-activation",
        )
    manifest = runtime_support._manifest(base)
    runtime_support.PostgresM5RuntimeStore(connection).register_candidate_policy(
        manifest
    )
    requirement_snapshot = runtime_support.RequirementRegistrySnapshot.build(())
    chunk_snapshot = runtime_support.ActiveChunkSnapshot.build(
        tuple(
            runtime_support.ActiveChunkSnapshotEntry.build(
                chunk_version_id=chunk_id,
                chunk_text=chunk_text,
            )
            for chunk_id, chunk_text in zip(
                base.chunk_ids, ("alpha", "beta"), strict=True
            )
        )
    )
    database = runtime_support.M5RuntimeDatabase(
        dsn="",
        schema_name="",
        connection=connection,
        base=base,
        group=group,
        manifest=manifest,
        requirement_snapshot=requirement_snapshot,
        chunk_snapshot=chunk_snapshot,
    )
    plan = database.register_plan(event_id="recovery-install-guard-register")
    store = runtime_support.PostgresM5RuntimeStore(connection)
    opened = store.open_typed_event_atomically(plan)
    if acquire:
        jobs = lifecycle_support._root_jobs(plan, manifest)
        assert jobs
        store.acquire_m5_job(opened.epoch_id, 1, jobs[0])
    return opened.epoch_id


def _seed_fake_attempt(
    connection: Connection[Any], *, subgraph: str, after_016: bool
) -> int:
    """Seed one production-valid attempt for the install/rerun guard."""

    if subgraph == "requirement":
        assert not after_016
        return _seed_requirement_attempt(connection)

    database = _prepare_direct_runtime(connection)
    opened = direct_support._open_direct_epoch(database)
    with connection.transaction(), connection.cursor() as cursor:
        direct_support._authorize(cursor, opened.epoch_id, 1)
        opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            1,
            opened.root,
            direct_support._deterministic_lease_token(opened.root),
        )
        direct_support._advance_typed_runtime(
            cursor, opened.epoch_id, 1, "semantic_pending"
        )
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
        epoch_id, requirements, active_chunks = (
            direct_support._insert_typed_outer_declaration(cursor, database, event)
        )
        direct_support._persist_typed_outer_snapshots(
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


def _expire_direct_attempt(connection: Connection[Any], *, attempt_id: str) -> None:
    with connection.transaction():
        changed = connection.execute(
            """
            UPDATE groundloop_semantic_job_attempt
            SET attempt_state = 'expired', finished_at = clock_timestamp()
            WHERE attempt_id = %s AND attempt_state = 'leased'
            """,
            (attempt_id,),
        ).rowcount
        assert changed == 1


def _seed_direct_envelope_support(
    connection: Connection[Any],
    *,
    return_kind: str,
    verification_execution_present: bool = True,
    reused_execution: bool = False,
    requested_make_effective: bool = False,
    expire_attempt: bool = True,
) -> dict[str, Any]:
    database = _prepare_direct_runtime(connection)
    opened = direct_support._open_direct_epoch(database)
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
        root_lease = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            1,
            opened.root,
            direct_support._deterministic_lease_token(opened.root),
        )
        direct_support._advance_typed_runtime(
            cursor, opened.epoch_id, 1, "semantic_pending"
        )
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
            _expire_direct_attempt(connection, attempt_id=attempt_id)
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
            direct_support._advance_typed_runtime(
                cursor, opened.epoch_id, 2, "semantic_pending"
            )
        with connection.transaction(), connection.cursor() as cursor:
            direct_support._authorize(cursor, opened.epoch_id, 3)
            child_lease = opened.adapter.acquire_direct_job(
                cursor,
                opened.epoch_id,
                3,
                child,
                direct_support._deterministic_lease_token(child),
            )
            direct_support._advance_typed_runtime(
                cursor, opened.epoch_id, 3, "semantic_pending"
            )
        assert child_lease.attempt_id is not None
        assert child_lease.lease_token_hash is not None
        job = child
        attempt_id = child_lease.attempt_id
        lease_token_hash = child_lease.lease_token_hash
        completion, _ = direct_support._verifier_completion(child)
        if expire_attempt:
            _expire_direct_attempt(connection, attempt_id=attempt_id)

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


def _terminalize_direct_epoch_for_audit(
    connection: Connection[Any], *, epoch_id: int
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
    event_work_digest = _runtime_work_digest(zero_work)
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
                %s, %s, %s, 'failed', %s, NULL, NULL, 'event', %s,
                %s, %s, 'invariant_failure', %s, 0, 0,
                0, 0, 0, 0, 0, NULL, NULL, NULL, NULL
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
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return logical_result_hash


def _insert_direct_dispatch_and_evidence(
    connection: Connection[Any],
    *,
    envelope_row: dict[str, Any],
    disposition: str,
    attempt_values: tuple[int, ...] | None = None,
    force_constraints: bool = True,
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
    connection.execute(
        sql.SQL("INSERT INTO groundloop_m5_dispatch_record ({}) VALUES ({})").format(
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
    if force_constraints:
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

    observation_digest = stable_m5_digest(
        "m5-runtime-timing-observation-v1",
        bool_field(False),
        *(option_field(None) for _ in range(9)),
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
    if force_constraints:
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
) -> None:
    observation_digest = stable_m5_digest(
        "m5-runtime-timing-observation-v1",
        bool_field(False),
        *(option_field(None) for _ in range(9)),
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
            %s, 'direct', %s, %s, false,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, %s, %s
        )
        """,
        (
            epoch_id,
            attempt_id,
            evidence_digest,
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
) -> None:
    accumulated_values = list(attempt_values)
    accumulated_values[16] += 1
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


def _insert_preterminal_direct_return_closure(
    connection: Connection[Any],
    *,
    envelope_row: dict[str, Any],
    disposition: str = "returned",
    attempt_values: tuple[int, ...] | None = None,
    forged_late_identity: str | None = None,
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
    late_values[16] = 1
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
    )
    return evidence_digest, work_digest, timing_digest


def _insert_postterminal_timing(
    connection: Connection[Any],
    *,
    epoch_id: int,
    attempt_id: str,
    attempt_timing_digest: str,
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
            %s, 'direct', %s, false,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, %s, %s
        )
        """,
        (epoch_id, attempt_id, observation_digest, attempt_timing_digest),
    )


def _seed_postterminal_audit_support(
    connection: Connection[Any],
    *,
    return_kind: str,
    evidence_disposition: str = "returned",
    include_envelope: bool = False,
) -> dict[str, Any]:
    envelope_row = _seed_direct_envelope_support(
        connection,
        return_kind="discovery",
    )
    evidence_digest, work_digest, timing_digest = _insert_direct_dispatch_and_evidence(
        connection,
        envelope_row=envelope_row,
        disposition=evidence_disposition,
    )
    connection.commit()
    logical_result_hash = _terminalize_direct_epoch_for_audit(
        connection, epoch_id=int(envelope_row["epoch_id"])
    )
    connection.commit()
    if include_envelope:
        _insert_direct_envelope(connection, envelope_row)
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
        "return_artifact_digest": envelope_row["envelope_digest"],
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


def test_operational_config_digest_is_reusable_across_epochs() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        database = _prepare_direct_runtime(connection)
        first = direct_support._open_direct_epoch(database)
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


def test_nonexpired_preterminal_return_updates_exact_current_work_and_timing() -> None:
    with _isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        row = _seed_direct_envelope_support(
            connection,
            return_kind="discovery",
            expire_attempt=False,
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


@pytest.mark.parametrize("mutation", ("forged_identity", "terminal_epoch"))
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
        if mutation == "terminal_epoch":
            _terminalize_direct_epoch_for_audit(
                connection,
                epoch_id=int(row["epoch_id"]),
            )
            connection.commit()

        _insert_preterminal_direct_return_closure(
            connection,
            envelope_row=row,
            forged_late_identity=(
                _sha("forged-preterminal-return")
                if mutation == "forged_identity"
                else None
            ),
        )
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="late-return envelope lacks exact audit/accounting closure",
        ):
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
        )
        connection.commit()

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
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
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
        if contribution_kind in {"epoch_failure", "seal"}:
            _terminalize_direct_epoch_for_audit(
                connection,
                epoch_id=int(row["epoch_id"]),
            )
            connection.commit()
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
