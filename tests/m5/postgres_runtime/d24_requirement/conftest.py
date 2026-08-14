"""Independent live PostgreSQL fixture for the D24 requirement checkpoint."""

from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from typing import Any

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.domain import DecisionPolicy
from groundloop.m4.contracts import VectorIndexKind
from groundloop.m5.domain import EvidenceGroupVersion
from groundloop.m5.events import RegisterGroupEvent, m5_event_payload_digest
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5AttemptOutput,
    M5CandidatePolicyManifest,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5JobKind,
    M5JobLease,
    M5LogicalJobSpec,
    M5RequirementPairInput,
    M5RequirementVerifierArtifact,
    M5RuntimeOperationalConfig,
    M5RuntimeTiming,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
    RequirementRegistrySnapshotEntry,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from groundloop.postgres.migrations import (
    apply_legacy_migrations,
    install_m5_core_bundle,
    install_m5_runtime_bundle,
    install_m5_runtime_recovery_bundle,
)
from tests.m5.postgres.helpers import (
    SeededBase,
    insert_published_group,
    install_test_activation_barrier,
    make_group,
    seed_base,
)

ACCEPTED_016_LEDGER = (
    "m5-runtime-recovery-schema-bundle-v1",
    "28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565",
    "a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7",
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd",
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


def _manifest(base: SeededBase) -> M5CandidatePolicyManifest:
    return M5CandidatePolicyManifest.build(
        embedding_model_artifact_id="d24-requirement-embedding",
        requirement_role_template_hash=_sha("d24-requirement-role"),
        chunk_role_template_hash=_sha("d24-chunk-role"),
        vector_method_version="d24-vector-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=_sha("d24-vector-build"),
        vector_search_config_hash=_sha("d24-vector-search"),
        lexical_method_version="d24-lexical-v1",
        lexical_config_hash=_sha("d24-lexical-config"),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        fusion_version="rank-interleave-v1",
        reverse_budget_per_inserted_chunk=2,
        forward_budget_per_requirement=2,
        verifier_execution_spec_hash=_sha("d24-verifier-execution"),
        decision_policy_version=base.policy_version,
        lineage_safety_override=True,
    )


def _requirement_snapshot(
    groups: tuple[EvidenceGroupVersion, ...],
) -> RequirementRegistrySnapshot:
    return RequirementRegistrySnapshot.build(
        tuple(
            RequirementRegistrySnapshotEntry.build(
                requirement_version_id=requirement.requirement_version_id,
                group_version_id=group.group_version_id,
                group_family_id=group.group_family_id,
                owner_claim_id=group.owner_claim_id,
                requirement_text=requirement.requirement_text,
            )
            for group in groups
            for requirement in group.requirements
        )
    )


def _root_jobs(
    plan: M5TypedEventPlan, manifest: M5CandidatePolicyManifest
) -> tuple[M5LogicalJobSpec, ...]:
    assert isinstance(plan.event, RegisterGroupEvent)
    jobs = []
    for requirement in plan.event.group.requirements:
        scope = M5DiscoveryScopeContract.build(
            direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
            requirement_version_id=requirement.requirement_version_id,
            inserted_chunk_version_id=None,
            candidate_policy_id=manifest.candidate_policy_id,
            requirement_registry_snapshot_digest=(
                plan.requirement_registry_snapshot.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest=(
                plan.active_chunk_snapshot.active_chunk_snapshot_digest
            ),
        )
        jobs.append(
            M5LogicalJobSpec.build(
                structural_event_id=plan.structural_event_id,
                job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
                manifest=manifest,
                scope=scope,
            )
        )
    return tuple(sorted(jobs, key=lambda item: item.logical_job_id))


def _insert_runtime_work_fixture(
    connection: Connection[Any],
    *,
    structural_event_id: str,
    epoch_id: int,
    work_kind: str,
    work: M5RuntimeWork,
) -> None:
    columns = (
        "work_digest",
        "structural_event_id",
        "epoch_id",
        "work_kind",
        *M5RuntimeWork.counter_names(),
    )
    connection.execute(
        sql.SQL("INSERT INTO groundloop_m5_runtime_work ({}) VALUES ({})").format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        (
            work.work_digest,
            structural_event_id,
            epoch_id,
            work_kind,
            *work.counter_values(),
        ),
    )


@dataclass(frozen=True, slots=True)
class _RetryableJobTerminalFixtureResolution:
    logical_job_id: str
    pending_counter_column: str
    completion_digest: str
    error_hash: str
    owner_claim_ids: tuple[str, ...]
    answer_multiplicities: tuple[tuple[str, int], ...]
    closes_root_scope: bool


def _lock_retryable_job_terminal_fixture_resolution(
    connection: Connection[Any],
    *,
    epoch_id: int,
    logical_job_id: str,
) -> _RetryableJobTerminalFixtureResolution:
    """Lock and validate the exact test-local retryable job closure."""

    job_row = connection.execute(
        """
        SELECT logical_job_id, job_kind, payload_hash, execution_spec_hash,
               scope_contract_digest, parent_job_id, job_state,
               result_artifact_id, result_artifact_hash,
               scope_closure_digest, child_set_hash, archive_reason,
               completion_digest, completed_revision, completed_at
        FROM groundloop_m5_semantic_job
        WHERE epoch_id = %s AND logical_job_id = %s
        FOR UPDATE
        """,
        (epoch_id, logical_job_id),
    ).fetchone()
    assert job_row is not None
    stored_job_id = str(job_row[0]).strip()
    job_kind = str(job_row[1])
    assert stored_job_id == logical_job_id
    assert job_kind in {
        M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL.value,
        M5JobKind.REVERSE_REQUIREMENT_DISCOVERY.value,
        M5JobKind.VERIFY_REQUIREMENT_PAIR.value,
    }
    closes_root_scope = job_kind != M5JobKind.VERIFY_REQUIREMENT_PAIR.value
    if closes_root_scope:
        assert job_row[5] is None
        scope_root_job_id = stored_job_id
    else:
        assert job_row[5] is not None
        scope_root_job_id = str(job_row[5]).strip()
        assert scope_root_job_id
    assert str(job_row[6]) == "retryable_failed"
    assert all(value is None for value in job_row[7:])

    scope_row = connection.execute(
        """
        SELECT scope_state, scope_closure_digest, child_set_hash,
               completion_digest, closed_revision, closed_at
        FROM groundloop_m5_discovery_scope
        WHERE epoch_id = %s AND root_job_id = %s
          AND scope_contract_digest = %s
        FOR UPDATE
        """,
        (epoch_id, scope_root_job_id, str(job_row[4]).strip()),
    ).fetchone()
    assert scope_row is not None
    if closes_root_scope:
        assert str(scope_row[0]) in {"open", "result_staged"}
        assert all(value is None for value in scope_row[1:])
    else:
        assert str(scope_row[0]) == "closed_active"
        assert all(value is not None for value in scope_row[1:])

    latest_attempt = connection.execute(
        """
        SELECT attempt_state, error_hash
        FROM groundloop_m5_job_attempt
        WHERE logical_job_id = %s
        ORDER BY attempt_ordinal DESC
        LIMIT 1
        FOR UPDATE
        """,
        (stored_job_id,),
    ).fetchone()
    assert latest_attempt is not None
    assert str(latest_attempt[0]) == "failed"
    assert latest_attempt[1] is not None
    error_hash = str(latest_attempt[1]).strip()

    owner_rows = connection.execute(
        """
        SELECT DISTINCT member.owner_claim_id COLLATE "C" AS owner_claim_id
        FROM groundloop_m5_semantic_job AS job
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.scope_contract_digest = job.scope_contract_digest
        JOIN groundloop_m5_requirement_registry_snapshot_member AS member
          ON member.requirement_registry_snapshot_digest =
             job.requirement_registry_snapshot_digest
         AND (
             job.job_kind = 'reverse_requirement_discovery'
             OR (job.job_kind = 'forward_requirement_retrieval'
                 AND member.requirement_version_id = scope.requirement_version_id)
             OR (job.job_kind = 'verify_requirement_pair'
                 AND member.requirement_version_id = job.subject_id)
         )
        WHERE job.epoch_id = %s AND job.logical_job_id = %s
        ORDER BY owner_claim_id
        """,
        (epoch_id, stored_job_id),
    ).fetchall()
    owner_claim_ids = tuple(str(row[0]) for row in owner_rows)
    assert owner_claim_ids
    answer_rows = connection.execute(
        """
        SELECT answer_version_id, count(*)::bigint
        FROM groundloop_claim
        WHERE claim_id = ANY(%s) AND required
        GROUP BY answer_version_id
        ORDER BY answer_version_id COLLATE "C"
        """,
        (list(owner_claim_ids),),
    ).fetchall()
    answer_multiplicities = tuple(
        (str(answer_id), int(multiplicity)) for answer_id, multiplicity in answer_rows
    )
    pending_counter_column = {
        M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL.value: "forward_scope_count",
        M5JobKind.REVERSE_REQUIREMENT_DISCOVERY.value: "broad_reverse_scope_count",
        M5JobKind.VERIFY_REQUIREMENT_PAIR.value: "verifier_job_count",
    }[job_kind]
    completion_digest = digests.job_completion_digest(
        logical_job_id_value=stored_job_id,
        payload_hash=str(job_row[2]).strip(),
        execution_spec_hash=str(job_row[3]).strip(),
        terminal_state="terminal_failed",
        result_artifact_id=None,
        result_artifact_hash=None,
        scope_closure_digest=None,
        child_set_hash=None,
        archive_reason="retry_exhausted",
    )
    return _RetryableJobTerminalFixtureResolution(
        logical_job_id=stored_job_id,
        pending_counter_column=pending_counter_column,
        completion_digest=completion_digest,
        error_hash=error_hash,
        owner_claim_ids=owner_claim_ids,
        answer_multiplicities=answer_multiplicities,
        closes_root_scope=closes_root_scope,
    )


def _apply_retryable_job_terminal_fixture_resolution(
    connection: Connection[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    resolution: _RetryableJobTerminalFixtureResolution,
) -> None:
    """Resolve one locked retryable job to the exact SQL terminal shape."""

    resulting_revision = expected_revision + 1
    settled_row = connection.execute("SELECT clock_timestamp()").fetchone()
    assert settled_row is not None
    settled_at = settled_row[0]
    assert (
        connection.execute(
            """
            UPDATE groundloop_m5_semantic_job
            SET job_state = 'terminal_failed', archive_reason = 'retry_exhausted',
                completion_digest = %s, completed_revision = %s,
                completed_at = %s
            WHERE epoch_id = %s AND logical_job_id = %s
              AND ((%s AND parent_job_id IS NULL)
                   OR (NOT %s AND parent_job_id IS NOT NULL))
              AND job_state = 'retryable_failed'
              AND result_artifact_id IS NULL
              AND result_artifact_hash IS NULL
              AND scope_closure_digest IS NULL AND child_set_hash IS NULL
              AND archive_reason IS NULL AND completion_digest IS NULL
              AND completed_revision IS NULL AND completed_at IS NULL
            """,
            (
                resolution.completion_digest,
                resulting_revision,
                settled_at,
                epoch_id,
                resolution.logical_job_id,
                resolution.closes_root_scope,
                resolution.closes_root_scope,
            ),
        ).rowcount
        == 1
    )
    if resolution.closes_root_scope:
        assert (
            connection.execute(
                """
                UPDATE groundloop_m5_discovery_scope
                SET scope_state = 'terminal_failed', completion_digest = %s,
                    closed_revision = %s, closed_at = %s
                WHERE epoch_id = %s AND root_job_id = %s
                  AND scope_state IN ('open', 'result_staged')
                  AND scope_closure_digest IS NULL AND child_set_hash IS NULL
                  AND completion_digest IS NULL AND closed_revision IS NULL
                  AND closed_at IS NULL
                """,
                (
                    resolution.completion_digest,
                    resulting_revision,
                    settled_at,
                    epoch_id,
                    resolution.logical_job_id,
                ),
            ).rowcount
            == 1
        )
    owner_updated = connection.execute(
        sql.SQL(
            "UPDATE groundloop_m5_owner_pending_counter "
            "SET {counter} = {counter} - 1, "
            "blocking_failure_count = blocking_failure_count + 1 "
            "WHERE epoch_id = %s AND owner_claim_id = ANY(%s) "
            "AND updated_revision = %s AND {counter} > 0"
        ).format(counter=sql.Identifier(resolution.pending_counter_column)),
        (
            epoch_id,
            list(resolution.owner_claim_ids),
            expected_revision,
        ),
    ).rowcount
    assert owner_updated == len(resolution.owner_claim_ids)
    for answer_id, multiplicity in resolution.answer_multiplicities:
        assert (
            connection.execute(
                sql.SQL(
                    "UPDATE groundloop_m5_answer_pending_counter "
                    "SET {counter} = {counter} - %s, "
                    "blocking_failure_count = blocking_failure_count + %s "
                    "WHERE epoch_id = %s AND answer_version_id = %s "
                    "AND updated_revision = %s AND {counter} >= %s"
                ).format(counter=sql.Identifier(resolution.pending_counter_column)),
                (
                    multiplicity,
                    multiplicity,
                    epoch_id,
                    answer_id,
                    expected_revision,
                    multiplicity,
                ),
            ).rowcount
            == 1
        )

    zero_work = M5RuntimeWork()
    work_names = M5RuntimeWork.counter_names()
    columns = (
        "epoch_id",
        *work_names,
        "work_digest",
        "contribution_kind",
        "source_id",
        "source_identity_hash",
        "contribution_key_digest",
        "applied_revision",
    )
    contribution_kind = M5RuntimeWorkContributionKind.TERMINAL_JOB_FAILURE
    connection.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_contribution ({}) VALUES ({})"
        ).format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        (
            epoch_id,
            *zero_work.counter_values(),
            zero_work.work_digest,
            contribution_kind.value,
            resolution.logical_job_id,
            digests.terminal_job_failure_contribution_source_digest(
                logical_job_id=resolution.logical_job_id,
                terminal_reason="retry_exhausted",
                error_hash=resolution.error_hash,
            ),
            digests.runtime_work_contribution_key_digest(
                epoch_id=epoch_id,
                contribution_kind=contribution_kind,
                source_id=resolution.logical_job_id,
            ),
            resulting_revision,
        ),
    )


def terminalize_recovered_epoch_for_storage_fixture(
    connection: Connection[Any],
    *,
    epoch_id: int,
    resolve_retryable_job_id: str | None = None,
) -> str:
    """Create a labelled SQL-only terminal closure for storage-path tests.

    When ``resolve_retryable_job_id`` is supplied, the same fixture transaction
    first resolves that exact retryable requirement job to ``terminal_failed`` with
    ``retry_exhausted`` at the event's terminal revision.

    This is fixture evidence only. It does not exercise or claim the R2
    production failure/seal composition.
    """

    work_names = M5RuntimeWork.counter_names()
    timing_names = (
        "coordinator_non_db_non_neural_ns",
        "neural_wall_ns",
        "postgres_roundtrip_wall_ns",
        "external_io_wall_ns",
        "end_to_end_wall_ns",
        "postgres_server_execution_ns",
        "postgres_lock_wait_ns",
        "postgres_wal_bytes",
        "postgres_shared_block_reads",
    )
    coverage_names = (
        "required_expected_count",
        "required_observed_count",
        "required_missing_count",
        "postgres_server_execution_expected_count",
        "postgres_server_execution_observed_count",
        "postgres_server_execution_missing_count",
        "postgres_lock_wait_expected_count",
        "postgres_lock_wait_observed_count",
        "postgres_lock_wait_missing_count",
        "postgres_wal_bytes_expected_count",
        "postgres_wal_bytes_observed_count",
        "postgres_wal_bytes_missing_count",
        "postgres_shared_block_reads_expected_count",
        "postgres_shared_block_reads_observed_count",
        "postgres_shared_block_reads_missing_count",
    )
    with connection.transaction():
        identity = connection.execute(
            """
            SELECT base.event_id, base.payload_hash, base.revision,
                   runtime.runtime_state
            FROM groundloop_epoch AS base
            JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
            WHERE base.epoch_id = %s
            FOR UPDATE OF base, runtime
            """,
            (epoch_id,),
        ).fetchone()
        assert identity is not None
        structural_event_id = str(identity[0])
        payload_hash = str(identity[1]).strip()
        revision = int(identity[2])
        assert str(identity[3]) in {"structural_committed", "semantic_pending"}
        retryable_resolution = (
            None
            if resolve_retryable_job_id is None
            else _lock_retryable_job_terminal_fixture_resolution(
                connection,
                epoch_id=epoch_id,
                logical_job_id=resolve_retryable_job_id,
            )
        )

        work_row = connection.execute(
            sql.SQL(
                "SELECT {}, work_digest, updated_revision, terminalized "
                "FROM groundloop_m5_runtime_work_accumulator "
                "WHERE epoch_id = %s FOR UPDATE"
            ).format(sql.SQL(", ").join(map(sql.Identifier, work_names))),
            (epoch_id,),
        ).fetchone()
        assert work_row is not None
        work_values = tuple(map(int, work_row[: len(work_names)]))
        event_work = M5RuntimeWork(
            **dict(zip(work_names, work_values, strict=True)),
            work_digest=str(work_row[len(work_names)]).strip(),
        )
        assert int(work_row[len(work_names) + 1]) == revision
        assert not bool(work_row[len(work_names) + 2])

        pending = connection.execute(
            """
            SELECT pending_contribution_kind, pending_source_id,
                   pending_contribution_key_digest, pending_anchor_revision
            FROM groundloop_m5_runtime_timing_accumulator
            WHERE epoch_id = %s FOR UPDATE
            """,
            (epoch_id,),
        ).fetchone()
        assert pending is not None
        pending_missing = int(pending[3] is not None)
        if pending_missing:
            observation = digests.runtime_timing_observation_digest(
                required_interval_observed=False,
                coordinator_non_db_non_neural_ns=None,
                neural_wall_ns=None,
                postgres_roundtrip_wall_ns=None,
                external_io_wall_ns=None,
                end_to_end_wall_ns=None,
                postgres_server_execution_ns=None,
                postgres_lock_wait_ns=None,
                postgres_wal_bytes=None,
                postgres_shared_block_reads=None,
            )
            transition = digests.transition_call_timing_digest(
                epoch_id=epoch_id,
                contribution_kind=str(pending[0]),
                source_id=str(pending[1]),
                contribution_key_digest=str(pending[2]).strip(),
                anchor_revision=int(pending[3]),
                observation_digest=observation,
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
                (epoch_id, *pending, observation, transition),
            )

        failure_source = digests.epoch_failure_contribution_source_digest(
            structural_event_id=structural_event_id,
            failure_reason="invariant_failure",
        )
        failure_key = digests.runtime_work_contribution_key_digest(
            epoch_id=epoch_id,
            contribution_kind=M5RuntimeWorkContributionKind.EPOCH_FAILURE,
            source_id=structural_event_id,
        )
        zero_work = M5RuntimeWork()
        contribution_columns = (
            "epoch_id",
            *work_names,
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
                sql.SQL(", ").join(map(sql.Identifier, contribution_columns)),
                sql.SQL(", ").join(sql.Placeholder() for _ in contribution_columns),
            ),
            (
                epoch_id,
                *zero_work.counter_values(),
                zero_work.work_digest,
                M5RuntimeWorkContributionKind.EPOCH_FAILURE.value,
                structural_event_id,
                failure_source,
                failure_key,
                revision + 1,
            ),
        )
        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (epoch_id, revision),
        )
        if retryable_resolution is not None:
            _apply_retryable_job_terminal_fixture_resolution(
                connection,
                epoch_id=epoch_id,
                expected_revision=revision,
                resolution=retryable_resolution,
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
        for table in (
            "groundloop_m5_owner_pending_counter",
            "groundloop_m5_answer_pending_counter",
        ):
            connection.execute(
                sql.SQL(
                    "UPDATE {} SET updated_revision = %s WHERE epoch_id = %s"
                ).format(sql.Identifier(table)),
                (revision + 1, epoch_id),
            )
        if retryable_resolution is None:
            assert (
                connection.execute(
                    """
                UPDATE groundloop_m5_runtime_epoch
                SET runtime_state = 'failed', revision = revision + 1,
                    terminal_at = clock_timestamp()
                WHERE epoch_id = %s AND revision = %s
                """,
                    (epoch_id, revision),
                ).rowcount
                == 1
            )
        else:
            scope_decrement = int(retryable_resolution.closes_root_scope)
            assert (
                connection.execute(
                    """
                UPDATE groundloop_m5_runtime_epoch
                SET runtime_state = 'failed', revision = revision + 1,
                    open_work_count = open_work_count - 1,
                    open_scope_count = open_scope_count - %s,
                    blocking_failure_count = blocking_failure_count + 1,
                    terminal_at = clock_timestamp()
                WHERE epoch_id = %s AND revision = %s
                  AND open_work_count > 0 AND open_scope_count >= %s
                """,
                    (scope_decrement, epoch_id, revision, scope_decrement),
                ).rowcount
                == 1
            )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_runtime_work_accumulator
            SET updated_revision = %s, terminalized = true,
                updated_at = clock_timestamp()
            WHERE epoch_id = %s AND updated_revision = %s AND NOT terminalized
            """,
                (revision + 1, epoch_id, revision),
            ).rowcount
            == 1
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_runtime_timing_accumulator
            SET required_expected_count = required_expected_count + 1,
                required_missing_count = required_missing_count + %s + 1,
                postgres_server_execution_expected_count =
                    postgres_server_execution_expected_count + 1,
                postgres_server_execution_missing_count =
                    postgres_server_execution_missing_count + %s + 1,
                postgres_lock_wait_expected_count =
                    postgres_lock_wait_expected_count + 1,
                postgres_lock_wait_missing_count =
                    postgres_lock_wait_missing_count + %s + 1,
                postgres_wal_bytes_expected_count =
                    postgres_wal_bytes_expected_count + 1,
                postgres_wal_bytes_missing_count =
                    postgres_wal_bytes_missing_count + %s + 1,
                postgres_shared_block_reads_expected_count =
                    postgres_shared_block_reads_expected_count + 1,
                postgres_shared_block_reads_missing_count =
                    postgres_shared_block_reads_missing_count + %s + 1,
                pending_contribution_kind = NULL, pending_source_id = NULL,
                pending_contribution_key_digest = NULL,
                pending_anchor_revision = NULL, updated_revision = %s,
                terminalized = true, updated_at = clock_timestamp()
            WHERE epoch_id = %s AND updated_revision = %s AND NOT terminalized
            """,
                (
                    pending_missing,
                    pending_missing,
                    pending_missing,
                    pending_missing,
                    pending_missing,
                    revision + 1,
                    epoch_id,
                    revision,
                ),
            ).rowcount
            == 1
        )

        timing_row = connection.execute(
            sql.SQL(
                "SELECT {} FROM groundloop_m5_runtime_timing_accumulator "
                "WHERE epoch_id = %s"
            ).format(
                sql.SQL(", ").join(
                    map(sql.Identifier, (*timing_names, *coverage_names))
                )
            ),
            (epoch_id,),
        ).fetchone()
        assert timing_row is not None
        timing_values = tuple(map(int, timing_row[: len(timing_names)]))
        coverage_values = tuple(map(int, timing_row[len(timing_names) :]))
        optional_values = tuple(
            timing_values[index]
            if coverage_values[5 + (index - 5) * 3] == 0
            and coverage_values[4 + (index - 5) * 3] > 0
            else None
            for index in range(5, 9)
        )
        event_timing = M5RuntimeTiming(*timing_values[:5], *optional_values)
        _insert_runtime_work_fixture(
            connection,
            structural_event_id=structural_event_id,
            epoch_id=epoch_id,
            work_kind="event",
            work=event_work,
        )
        _insert_runtime_work_fixture(
            connection,
            structural_event_id=structural_event_id,
            epoch_id=epoch_id,
            work_kind="call",
            work=zero_work,
        )
        open_hash = digests.open_event_receipt_binding_digest(
            epoch_id=epoch_id,
            replayed=False,
            already_sealed=False,
            publication_id=None,
            already_failed=False,
            failure_reason=None,
        )
        combined_hash = digests.combined_status_delta_set_digest(())
        changed_hash = digests.changed_state_set_digest(())
        logical_hash = digests.event_run_logical_result_digest(
            event_id=structural_event_id,
            payload_hash=payload_hash,
            epoch_id=epoch_id,
            sealed_or_failed_outcome="failed",
            original_open_receipt_binding_hash=open_hash,
            original_publication_receipt_binding_hash=None,
            event_work_digest=event_work.work_digest,
            combined_status_delta_set_hash=combined_hash,
            changed_state_set_hash=changed_hash,
            failure_reason="invariant_failure",
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
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                structural_event_id,
                payload_hash,
                epoch_id,
                open_hash,
                event_work.work_digest,
                combined_hash,
                changed_hash,
                logical_hash,
                *(getattr(event_timing, name) for name in timing_names),
            ),
        )
        connection.execute(
            sql.SQL(
                "INSERT INTO groundloop_m5_event_timing_coverage "
                "(structural_event_id, epoch_id, {}, "
                "terminal_client_roundtrip_included) VALUES (%s, %s, {}, false)"
            ).format(
                sql.SQL(", ").join(map(sql.Identifier, coverage_names)),
                sql.SQL(", ").join(sql.Placeholder() for _ in coverage_names),
            ),
            (structural_event_id, epoch_id, *coverage_values),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return logical_hash


@dataclass(frozen=True, slots=True)
class D24RequirementDatabase:
    dsn: str
    schema_name: str
    connection: Connection[Any]
    epoch_id: int
    plan: M5TypedEventPlan
    manifest: M5CandidatePolicyManifest
    jobs: tuple[M5LogicalJobSpec, ...]
    operational_config: M5RuntimeOperationalConfig

    @contextmanager
    def reconnect(self) -> Iterator[Connection[Any]]:
        with psycopg.connect(self.dsn) as connection:
            _select_schema(connection, self.schema_name)
            yield connection

    @contextmanager
    def two_connections(
        self,
    ) -> Iterator[tuple[Connection[Any], Connection[Any]]]:
        with ExitStack() as stack:
            first = stack.enter_context(psycopg.connect(self.dsn))
            second = stack.enter_context(psycopg.connect(self.dsn))
            _select_schema(first, self.schema_name)
            _select_schema(second, self.schema_name)
            yield first, second


@dataclass(frozen=True, slots=True)
class D24VerifierFixture:
    """Self-validating verifier inputs derived from durable R1-P rows."""

    lease: M5JobLease
    job: M5LogicalJobSpec
    pair_input: M5RequirementPairInput
    verifier_artifact: M5RequirementVerifierArtifact
    attempt_output: M5AttemptOutput


def build_d24_verifier_fixture(
    database: D24RequirementDatabase,
    *,
    lease: M5JobLease,
    job: M5LogicalJobSpec,
    raw_output_tag: str = "primary",
) -> D24VerifierFixture:
    """Build one verifier return from the exact durable child-job closure."""

    assert lease.attempt is not None
    assert job.pair is not None
    assert job.scope_contract_digest is not None
    identity_suffix = job.logical_job_id[:16]
    chunker_artifact_id = f"d24-verifier-chunker-{identity_suffix}"
    with database.connection.transaction():
        database.connection.execute(
            """
            INSERT INTO groundloop_chunker_artifact (
                chunker_artifact_id, chunker_version,
                normalization_version, config_hash
            ) VALUES (%s, 'fixture-v1', 'v1', %s)
            ON CONFLICT (chunker_artifact_id) DO NOTHING
            """,
            (chunker_artifact_id, _sha(f"{chunker_artifact_id}:config")),
        )
        database.connection.execute(
            """
            INSERT INTO groundloop_chunk_provenance (
                chunk_version_id, chunker_artifact_id, input_hash
            ) VALUES (%s, %s, %s)
            ON CONFLICT (chunk_version_id) DO NOTHING
            """,
            (
                job.pair.chunk_version_id,
                chunker_artifact_id,
                _sha(f"{job.pair.chunk_version_id}:input"),
            ),
        )
    row = database.connection.execute(
        """
        SELECT requirement.ordinal, requirement.requirement_text,
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
        ORDER BY provenance.chunker_artifact_id COLLATE "C"
        LIMIT 1
        """,
        (job.pair.chunk_version_id, job.pair.subject_id),
    ).fetchone()
    assert row is not None
    pair_input = M5RequirementPairInput.build(
        pair=job.pair,
        scope_contract_digest=job.scope_contract_digest,
        candidate_policy_id=job.candidate_policy_id,
        owner_claim_id=str(row[4]),
        group_version_id=str(row[2]),
        group_family_id=str(row[3]),
        requirement_ordinal=int(row[0]),
        requirement_text=str(row[1]),
        document_version_id=str(row[5]),
        chunk_index=int(row[6]),
        chunk_text=str(row[7]),
        stored_chunk_text_hash=str(row[8]).strip(),
        chunker_artifact_id=str(row[9]),
    )
    policy_row = database.connection.execute(
        """
        SELECT policy_version, support_threshold, refute_threshold,
               tie_rule_version
        FROM groundloop_decision_policy WHERE policy_version = %s
        """,
        (database.manifest.decision_policy_version,),
    ).fetchone()
    assert policy_row is not None
    policy = DecisionPolicy(
        str(policy_row[0]),
        float(policy_row[1]),
        float(policy_row[2]),
        str(policy_row[3]),
    )
    model_artifact_id = f"d24-verifier-model-{identity_suffix}"
    model_id = f"d24-verifier-{identity_suffix}"
    prompt_artifact_id = f"d24-verifier-prompt-{identity_suffix}"
    with database.connection.transaction():
        database.connection.execute(
            """
            INSERT INTO groundloop_model_artifact (
                model_artifact_id, task, provider, model_id,
                immutable_revision, tokenizer_revision, license_id,
                config_hash
            ) VALUES (%s, 'verification', 'fixture', %s, 'v1', 'v1', 'MIT', %s)
            ON CONFLICT (model_artifact_id) DO NOTHING
            """,
            (model_artifact_id, model_id, _sha(f"{model_id}:config")),
        )
        database.connection.execute(
            """
            INSERT INTO groundloop_prompt_artifact (
                prompt_artifact_id, task, version, template,
                template_hash, decoding_config_hash
            ) VALUES (%s, 'verification', 'v1', %s, %s, %s)
            ON CONFLICT (prompt_artifact_id) DO NOTHING
            """,
            (
                prompt_artifact_id,
                "d24 verifier fixture prompt",
                _sha("d24 verifier fixture prompt"),
                _sha("d24 verifier fixture decoding"),
            ),
        )
    verifier_artifact = M5RequirementVerifierArtifact.build_checked(
        pair=job.pair,
        pair_input_hash=pair_input.pair_input_hash,
        execution_spec_hash=job.execution_spec_hash,
        model_artifact_id=model_artifact_id,
        model_id=model_id,
        model_revision="v1",
        prompt_artifact_id=prompt_artifact_id,
        prompt_version="v1",
        calibration_version="uncalibrated-v1",
        calibration_artifact_hash=None,
        temperature=1.0,
        decision_policy=policy,
        support_score=0.9,
        refute_score=0.05,
        neutral_score=0.05,
        raw_logits=(-1.0, 2.0, 0.0),
        raw_output_hash=_sha(f"d24-verifier-output:{raw_output_tag}"),
    )
    attempt_output = M5AttemptOutput.build(
        attempt=lease.attempt,
        job_epoch_id=database.epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=verifier_artifact.artifact_id,
        result_artifact_hash=verifier_artifact.artifact_hash,
    )
    return D24VerifierFixture(
        lease=lease,
        job=job,
        pair_input=pair_input,
        verifier_artifact=verifier_artifact,
        attempt_output=attempt_output,
    )


@pytest.fixture
def d24_requirement_db(
    request: pytest.FixtureRequest,
) -> Iterator[D24RequirementDatabase]:
    """Create one independent runtime schema, accepted-016 by default."""

    fixture_mode = getattr(request, "param", "recovery")
    install_recovery = fixture_mode in {
        "recovery",
        "recovery_unopened",
        "two_roots",
    }
    root_count = 2 if fixture_mode == "two_roots" else 1
    dsn = _database_url()
    schema_name = f"groundloop_d24_requirement_{uuid.uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))

    try:
        with psycopg.connect(dsn) as connection:
            _select_schema(connection, schema_name)
            with connection.transaction():
                apply_legacy_migrations(connection)
            install_m5_core_bundle(connection)
            install_m5_runtime_bundle(connection)
            if install_recovery:
                install_m5_runtime_recovery_bundle(connection)

            ledger = connection.execute(
                """
                SELECT bundle_id, bundle_sha256, migration_sha256,
                       oracle_sha256, prerequisite_sha256
                FROM groundloop_m5_schema_bundle
                WHERE bundle_id = %s
                """,
                (ACCEPTED_016_LEDGER[0],),
            ).fetchone()
            if install_recovery:
                assert ledger is not None
                assert tuple(str(value).strip() for value in ledger) == (
                    ACCEPTED_016_LEDGER
                )
            else:
                assert ledger is None
            connection.commit()

            with connection.transaction():
                base = seed_base(
                    connection,
                    prefix=f"d24-requirement-{uuid.uuid4().hex[:8]}",
                    claim_count=1,
                    chunk_texts=("alpha", "beta"),
                )
                existing_group = make_group(
                    group_id=f"d24-existing-{uuid.uuid4().hex}",
                    family_id=f"d24-existing-family-{uuid.uuid4().hex}",
                    claim_id=base.claim_ids[0],
                    texts=("existing fact",),
                    source_id="d24-requirement-existing",
                )
                insert_published_group(
                    connection,
                    group=existing_group,
                    epoch_id=base.epoch_id,
                )
                connection.execute(
                    """
                    INSERT INTO groundloop_model_artifact (
                        model_artifact_id, task, provider, model_id,
                        immutable_revision, tokenizer_revision, license_id,
                        config_hash
                    ) VALUES (
                        'd24-requirement-embedding', 'embedding', 'fixture',
                        'fixture-embedding', 'v1', 'v1', 'MIT', %s
                    )
                    """,
                    (_sha("d24-embedding-config"),),
                )
            with connection.transaction():
                install_test_activation_barrier(
                    connection,
                    base,
                    activation_id=f"d24-activation-{uuid.uuid4().hex}",
                )

            manifest = _manifest(base)
            store = PostgresM5RuntimeStore(connection)
            store.register_candidate_policy(manifest)
            new_group = make_group(
                group_id=f"d24-new-{uuid.uuid4().hex}",
                family_id=f"d24-new-family-{uuid.uuid4().hex}",
                claim_id=base.claim_ids[0],
                texts=tuple(
                    f"new required fact {ordinal}" for ordinal in range(root_count)
                ),
                source_id="d24-requirement-new",
            )
            event_id = f"d24-register-{uuid.uuid4().hex}"
            event = RegisterGroupEvent(event_id=event_id, group=new_group)
            chunk_snapshot = ActiveChunkSnapshot.build(
                tuple(
                    ActiveChunkSnapshotEntry.build(
                        chunk_version_id=chunk_id,
                        chunk_text=chunk_text,
                    )
                    for chunk_id, chunk_text in zip(
                        base.chunk_ids, ("alpha", "beta"), strict=True
                    )
                )
            )
            plan = M5TypedEventPlan(
                structural_event_id=event_id,
                event=event,
                payload_hash=m5_event_payload_digest(event),
                direct_plan=None,
                candidate_policy_id=manifest.candidate_policy_id,
                candidate_policy_manifest_hash=manifest.manifest_hash,
                requirement_registry_snapshot=_requirement_snapshot(
                    (existing_group, new_group)
                ),
                active_chunk_snapshot=chunk_snapshot,
                expected_previous_published_epoch_id=base.epoch_id,
            )
            jobs = _root_jobs(plan, manifest)
            assert len(jobs) == root_count
            operational_config = M5RuntimeOperationalConfig.build(300)
            opened_epoch_id = -1
            if install_recovery and fixture_mode != "recovery_unopened":
                opened = store.open_typed_event_atomically(
                    plan,
                    recovery_operational_config=operational_config,
                    recovery_root_fallback_required={
                        job.logical_job_id: False for job in jobs
                    },
                )
                opened_epoch_id = opened.epoch_id
            yield D24RequirementDatabase(
                dsn=dsn,
                schema_name=schema_name,
                connection=connection,
                epoch_id=opened_epoch_id,
                plan=plan,
                manifest=manifest,
                jobs=jobs,
                operational_config=operational_config,
            )
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )


__all__ = [
    "ACCEPTED_016_LEDGER",
    "D24RequirementDatabase",
    "D24VerifierFixture",
    "build_d24_verifier_fixture",
    "terminalize_recovered_epoch_for_storage_fixture",
]
