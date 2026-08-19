"""Checked PostgreSQL helpers for M5-D24 requirement recovery.

This module contains only recovery-schema operations owned by the requirement
runtime.  Transaction ownership and the frozen epoch/job lock order remain in
``PostgresM5RuntimeStore``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg import Cursor, sql

from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    M5AttemptExecutionEvidence,
    M5DispatchRecord,
    M5ExecutionEvidenceDisposition,
    M5JobAttempt,
    M5JobKind,
    M5RequirementRootProvenance,
    M5RunFailureReason,
    M5RuntimeOperationalConfig,
    M5RuntimeSubgraph,
    M5RuntimeTiming,
    M5RuntimeTimingCoverage,
    M5RuntimeTimingObservation,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TerminalReason,
    M5TransitionTimingAnchor,
    M5TransitionTimingReceipt,
)

_RECOVERY_BUNDLE_ROW = (
    "m5-runtime-recovery-schema-bundle-v1",
    "28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565",
    "a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7",
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd",
)
_WORK_COUNTER_COLUMNS = M5RuntimeWork.counter_names()
_STRUCTURAL_OPEN_COUNTERS = frozenset(
    {
        "deactivated_chunk_count",
        "withdrawn_candidate_edge_count",
        "withdrawn_current_observation_count",
        "requirement_cancelled_job_count",
        "group_state_write_count",
        "claim_state_write_count",
        "answer_state_write_count",
        "certificate_binding_write_count",
        "bytes_hashed",
        "bytes_serialized",
    }
)
_ROOT_RESULT_STAGE_COUNTERS = frozenset(
    {
        "requirement_channel_hit_count",
        "requirement_pre_dedup_selection_count",
        "bytes_hashed",
        "bytes_serialized",
    }
)
_ROOT_BARRIER_COUNTERS = frozenset(
    {
        "requirement_admitted_pair_count",
        "bytes_hashed",
        "bytes_serialized",
    }
)
_CANCELLATION_COUNTERS = frozenset({"requirement_cancelled_job_count"})
_PRETERMINAL_LATE_COUNTERS = frozenset(
    {
        "requirement_late_attempt_artifact_count",
        "bytes_hashed",
        "bytes_serialized",
    }
)
_TIMING_SUM_COLUMNS = (
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
_TIMING_COVERAGE_COLUMNS = (
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
RecoveryFailureInjector = Callable[[str], None]


@dataclass(frozen=True, slots=True)
class StoredRequirementAttempt:
    """One complete migration-016 requirement-attempt projection."""

    attempt: M5JobAttempt
    state: str
    attempt_output_digest: str | None
    error_hash: str | None
    finished_at: datetime | None


@dataclass(frozen=True, slots=True)
class AcquisitionClock:
    """The single database-clock decision and its database-computed deadline."""

    decision_time: datetime
    lease_expires_at: datetime
    config: M5RuntimeOperationalConfig


@dataclass(frozen=True, slots=True)
class RequirementExecutionAccounting:
    """Immutable execution rows prepared for one checked requirement return."""

    evidence: M5AttemptExecutionEvidence
    observation: M5RuntimeTimingObservation
    anchor: M5TransitionTimingAnchor


@dataclass(frozen=True, slots=True)
class RequirementPostterminalReplay:
    """Exact immutable post-terminal branch projected for receipt replay."""

    return_kind: str
    return_artifact_digest: str
    terminal_logical_result_hash: str


@dataclass(frozen=True, slots=True)
class EventAccountingStart:
    """Locked nonterminal accumulator images at one exact input revision."""

    work: M5RuntimeWork
    timing: _TimingAccumulator


@dataclass(frozen=True, slots=True)
class _TimingAccumulator:
    sums: M5RuntimeTiming
    coverage_counts: tuple[int, ...]
    updated_revision: int
    terminalized: bool
    pending_contribution_kind: str | None
    pending_source_id: str | None
    pending_contribution_key_digest: str | None
    pending_anchor_revision: int | None

    @property
    def has_pending_anchor(self) -> bool:
        values = (
            self.pending_contribution_kind,
            self.pending_source_id,
            self.pending_contribution_key_digest,
            self.pending_anchor_revision,
        )
        if all(value is None for value in values):
            return False
        if any(value is None for value in values):
            raise ValidationError("stored M5 timing accumulator has a partial anchor")
        return True

    def project(
        self, *, pending_as_missing: bool
    ) -> tuple[M5RuntimeTiming, M5RuntimeTimingCoverage]:
        """Project physical sums/counts into the frozen public aggregate shape."""

        if len(self.coverage_counts) != len(_TIMING_COVERAGE_COLUMNS):
            raise ValidationError("stored M5 timing accumulator coverage is malformed")
        pending = int(pending_as_missing and self.has_pending_anchor)
        counts = list(self.coverage_counts)
        for missing_index in (2, 5, 8, 11, 14):
            counts[missing_index] += pending
        coverage = M5RuntimeTimingCoverage(
            counts[0],
            counts[1],
            counts[2],
            counts[3],
            counts[4],
            counts[5],
            counts[6],
            counts[7],
            counts[8],
            counts[9],
            counts[10],
            counts[11],
            counts[12],
            counts[13],
            counts[14],
            False,
        )
        sums = self.sums
        timing = M5RuntimeTiming(
            coordinator_non_db_non_neural_ns=(sums.coordinator_non_db_non_neural_ns),
            neural_wall_ns=sums.neural_wall_ns,
            postgres_roundtrip_wall_ns=sums.postgres_roundtrip_wall_ns,
            external_io_wall_ns=sums.external_io_wall_ns,
            end_to_end_wall_ns=sums.end_to_end_wall_ns,
            postgres_server_execution_ns=(
                sums.postgres_server_execution_ns
                if coverage.postgres_server_execution_observed_count > 0
                and coverage.postgres_server_execution_missing_count == 0
                else None
            ),
            postgres_lock_wait_ns=(
                sums.postgres_lock_wait_ns
                if coverage.postgres_lock_wait_observed_count > 0
                and coverage.postgres_lock_wait_missing_count == 0
                else None
            ),
            postgres_wal_bytes=(
                sums.postgres_wal_bytes
                if coverage.postgres_wal_bytes_observed_count > 0
                and coverage.postgres_wal_bytes_missing_count == 0
                else None
            ),
            postgres_shared_block_reads=(
                sums.postgres_shared_block_reads
                if coverage.postgres_shared_block_reads_observed_count > 0
                and coverage.postgres_shared_block_reads_missing_count == 0
                else None
            ),
        )
        coverage.validate_aggregate(timing)
        return timing, coverage


def _inject_recovery(
    failure_injector: RecoveryFailureInjector | None, point: str
) -> None:
    if failure_injector is not None:
        failure_injector(point)


def require_runtime_recovery_bundle(cursor: Cursor[Any]) -> None:
    """Require the literal accepted five-field migration-016 ledger tuple."""

    row = cursor.execute(
        """
        SELECT bundle_id, bundle_sha256, migration_sha256,
               oracle_sha256, prerequisite_sha256
        FROM groundloop_m5_schema_bundle
        WHERE bundle_id = %s
        """,
        (_RECOVERY_BUNDLE_ROW[0],),
    ).fetchone()
    if (
        row is None
        or tuple(str(value).strip() for value in row) != _RECOVERY_BUNDLE_ROW
    ):
        raise InvalidEventError(
            "typed M5 recovery requires the exact accepted migration-016 bundle"
        )


def persist_structural_open_identity(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    config: M5RuntimeOperationalConfig,
    root_fallback_required: dict[str, bool],
) -> None:
    """Persist immutable operational and forward-root provenance at open."""

    if not isinstance(config, M5RuntimeOperationalConfig):
        raise ValidationError("config must be an M5RuntimeOperationalConfig")
    cursor.execute(
        """
        INSERT INTO groundloop_m5_runtime_operational_config (
            epoch_id, lease_duration_ms, config_digest
        ) VALUES (%s, %s, %s)
        """,
        (epoch_id, config.lease_duration_ms, config.config_digest),
    )
    for root_job_id in sorted(root_fallback_required):
        provenance = M5RequirementRootProvenance.build(
            epoch_id=epoch_id,
            root_job_id=root_job_id,
            fallback_required=root_fallback_required[root_job_id],
        )
        cursor.execute(
            """
            INSERT INTO groundloop_m5_requirement_root_provenance (
                epoch_id, root_job_id, fallback_required, provenance_digest
            ) VALUES (%s, %s, %s, %s)
            """,
            (
                provenance.epoch_id,
                provenance.root_job_id,
                provenance.fallback_required,
                provenance.provenance_digest,
            ),
        )


def persist_structural_open_accounting(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    structural_event_id: str,
    payload_hash: str,
    structural_work: M5RuntimeWork,
) -> M5TransitionTimingAnchor:
    """Apply exact persistence-owned open work and install its pending anchor."""

    if not isinstance(structural_work, M5RuntimeWork):
        raise ValidationError("structural_work must be an M5RuntimeWork")
    foreign_counters = tuple(
        name
        for name, value in zip(
            structural_work.counter_names(),
            structural_work.counter_values(),
            strict=True,
        )
        if value and name not in _STRUCTURAL_OPEN_COUNTERS
    )
    if foreign_counters:
        raise ValidationError(
            "structural-open work uses counters owned by another surface: "
            f"{foreign_counters!r}"
        )
    anchor = M5TransitionTimingAnchor.build(
        epoch_id=epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
        source_id=structural_event_id,
        anchor_revision=1,
        terminal_transition=False,
    )
    contribution_columns = (
        "epoch_id",
        *_WORK_COUNTER_COLUMNS,
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
            sql.SQL(", ").join(map(sql.Identifier, contribution_columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in contribution_columns),
        ),
        (
            epoch_id,
            *structural_work.counter_values(),
            structural_work.work_digest,
            anchor.contribution_kind.value,
            structural_event_id,
            payload_hash,
            anchor.contribution_key_digest,
            1,
        ),
    )
    accumulator_columns = (
        "epoch_id",
        *_WORK_COUNTER_COLUMNS,
        "work_digest",
        "updated_revision",
        "terminalized",
    )
    cursor.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_accumulator ({}) VALUES ({})"
        ).format(
            sql.SQL(", ").join(map(sql.Identifier, accumulator_columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in accumulator_columns),
        ),
        (
            epoch_id,
            *structural_work.counter_values(),
            structural_work.work_digest,
            1,
            False,
        ),
    )
    cursor.execute(
        """
        INSERT INTO groundloop_m5_runtime_timing_accumulator (
            epoch_id, required_expected_count,
            postgres_server_execution_expected_count,
            postgres_lock_wait_expected_count,
            postgres_wal_bytes_expected_count,
            postgres_shared_block_reads_expected_count,
            pending_contribution_kind, pending_source_id,
            pending_contribution_key_digest, pending_anchor_revision,
            updated_revision, terminalized
        ) VALUES (%s, 1, 1, 1, 1, 1, %s, %s, %s, 1, 1, false)
        """,
        (
            epoch_id,
            anchor.contribution_kind.value,
            anchor.source_id,
            anchor.contribution_key_digest,
        ),
    )
    return anchor


def validate_structural_open_recovery(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    structural_event_id: str,
    payload_hash: str,
    config: M5RuntimeOperationalConfig,
    root_fallback_required: dict[str, bool],
    expected_structural_work: M5RuntimeWork | None = None,
) -> None:
    """Validate immutable open rows plus the current accumulator hydration."""

    config_row = cursor.execute(
        """
        SELECT lease_duration_ms, config_digest
        FROM groundloop_m5_runtime_operational_config WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    provenance_rows = tuple(
        tuple(row)
        for row in cursor.execute(
            """
            SELECT root_job_id, fallback_required, provenance_digest
            FROM groundloop_m5_requirement_root_provenance
            WHERE epoch_id = %s ORDER BY root_job_id COLLATE "C"
            """,
            (epoch_id,),
        ).fetchall()
    )
    expected_provenance = tuple(
        (
            provenance.root_job_id,
            provenance.fallback_required,
            provenance.provenance_digest,
        )
        for root_job_id in sorted(root_fallback_required)
        for provenance in (
            M5RequirementRootProvenance.build(
                epoch_id=epoch_id,
                root_job_id=root_job_id,
                fallback_required=root_fallback_required[root_job_id],
            ),
        )
    )
    anchor = M5TransitionTimingAnchor.build(
        epoch_id=epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
        source_id=structural_event_id,
        anchor_revision=1,
        terminal_transition=False,
    )
    closure = cursor.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_runtime_work_contribution AS c
           WHERE c.epoch_id = %s AND c.contribution_kind = 'structural_open'
             AND c.source_id = %s AND c.source_identity_hash = %s
             AND c.contribution_key_digest = %s AND c.applied_revision = 1),
          (SELECT count(*) FROM groundloop_m5_runtime_work_accumulator AS w
           JOIN groundloop_m5_runtime_epoch AS r USING (epoch_id)
           JOIN groundloop_m5_runtime_work_contribution AS c
             ON c.epoch_id = w.epoch_id
            AND c.contribution_kind = 'structural_open'
           WHERE w.epoch_id = %s AND w.updated_revision = r.revision
             AND w.terminalized = (r.runtime_state IN ('sealed', 'failed'))
             AND (r.revision > 1 OR (
                 NOT w.terminalized AND w.work_digest = c.work_digest
                 AND groundloop_m5_recovery_work_values(w) =
                     groundloop_m5_recovery_work_values(c)))),
          (SELECT count(*) FROM groundloop_m5_runtime_timing_accumulator AS t
           JOIN groundloop_m5_runtime_epoch AS r USING (epoch_id)
           WHERE t.epoch_id = %s AND t.updated_revision = r.revision
             AND t.terminalized = (r.runtime_state IN ('sealed', 'failed'))
             AND (r.revision > 1 OR (
                 NOT t.terminalized AND t.required_expected_count = 1
                 AND t.required_observed_count = 0
                 AND t.required_missing_count = 0
                 AND t.pending_contribution_kind = 'structural_open'
                 AND t.pending_source_id = %s
                 AND t.pending_contribution_key_digest = %s
                 AND t.pending_anchor_revision = 1)))
        """,
        (
            epoch_id,
            structural_event_id,
            payload_hash,
            anchor.contribution_key_digest,
            epoch_id,
            epoch_id,
            structural_event_id,
            anchor.contribution_key_digest,
        ),
    ).fetchone()
    if (
        config_row != (config.lease_duration_ms, config.config_digest)
        or provenance_rows != expected_provenance
        or closure != (1, 1, 1)
    ):
        raise EventConflictError("structural-open recovery image differs on replay")
    if expected_structural_work is not None:
        if (
            type(expected_structural_work) is not M5RuntimeWork
            or any(
                type(value) is not int
                for value in expected_structural_work.counter_values()
            )
            or type(expected_structural_work.work_digest) is not str
        ):
            raise ValidationError("expected structural work is not exact")
        contribution = cursor.execute(
            sql.SQL(
                "SELECT {}, work_digest "
                "FROM groundloop_m5_runtime_work_contribution "
                "WHERE epoch_id = %s AND contribution_kind = 'structural_open' "
                "AND source_id = %s AND source_identity_hash = %s "
                "AND contribution_key_digest = %s AND applied_revision = 1"
            ).format(sql.SQL(", ").join(map(sql.Identifier, _WORK_COUNTER_COLUMNS))),
            (
                epoch_id,
                structural_event_id,
                payload_hash,
                anchor.contribution_key_digest,
            ),
        ).fetchone()
        if contribution is None:
            raise EventConflictError("structural-open work contribution is absent")
        stored_work = M5RuntimeWork(
            **dict(
                zip(
                    _WORK_COUNTER_COLUMNS,
                    map(int, contribution[:-1]),
                    strict=True,
                )
            ),
            work_digest=str(contribution[-1]).strip(),
        )
        if stored_work != expected_structural_work:
            raise EventConflictError("structural-open work changed on replay")


def _stored_requirement_attempt(row: tuple[Any, ...]) -> StoredRequirementAttempt:
    output_digest = None if row[6] is None else str(row[6]).strip()
    error_hash = None if row[7] is None else str(row[7]).strip()
    finished_at = row[8]
    lease_expires_at = row[9]
    if not isinstance(lease_expires_at, datetime):
        raise ValidationError("stored M5 attempt lacks its operational deadline")
    attempt = M5JobAttempt(
        attempt_id=str(row[0]).strip(),
        logical_job_id=str(row[1]).strip(),
        attempt_ordinal=int(row[2]),
        execution_spec_hash=str(row[3]).strip(),
        lease_token_hash=str(row[4]).strip(),
        lease_expires_at=lease_expires_at,
        attempt_work_digest=str(row[10]).strip(),
    )
    state = str(row[5])
    if state == "dispatched":
        valid_shape = (
            output_digest is None and error_hash is None and finished_at is None
        )
    elif state == "result_reserved":
        valid_shape = (
            output_digest is not None and error_hash is None and finished_at is None
        )
    elif state == "completed":
        valid_shape = (
            output_digest is not None and error_hash is None and finished_at is not None
        )
    elif state == "failed":
        valid_shape = (
            output_digest is None and error_hash is not None and finished_at is not None
        )
    elif state == "expired":
        valid_shape = (
            output_digest is None and error_hash is None and finished_at is not None
        )
    else:
        valid_shape = False
    if not valid_shape:
        raise ValidationError("stored M5 attempt has an invalid recovery shape")
    return StoredRequirementAttempt(
        attempt=attempt,
        state=state,
        attempt_output_digest=output_digest,
        error_hash=error_hash,
        finished_at=finished_at,
    )


def read_latest_requirement_attempt(
    cursor: Cursor[Any], *, logical_job_id: str
) -> StoredRequirementAttempt | None:
    """Lock and hydrate only the latest dense requirement attempt."""

    row = cursor.execute(
        """
        SELECT attempt_id, logical_job_id, attempt_ordinal,
               execution_spec_hash, lease_token_hash, attempt_state,
               attempt_output_digest, error_hash, finished_at,
               lease_expires_at, attempt_work_digest
        FROM groundloop_m5_job_attempt
        WHERE logical_job_id = %s
        ORDER BY attempt_ordinal DESC
        LIMIT 1
        FOR UPDATE
        """,
        (logical_job_id,),
    ).fetchone()
    return None if row is None else _stored_requirement_attempt(tuple(row))


def read_requirement_attempt(
    cursor: Cursor[Any], *, logical_job_id: str, attempt_id: str
) -> StoredRequirementAttempt | None:
    """Hydrate one immutable attempt identity before latest-attempt enforcement."""

    row = cursor.execute(
        """
        SELECT attempt_id, logical_job_id, attempt_ordinal,
               execution_spec_hash, lease_token_hash, attempt_state,
               attempt_output_digest, error_hash, finished_at,
               lease_expires_at, attempt_work_digest
        FROM groundloop_m5_job_attempt
        WHERE logical_job_id = %s AND attempt_id = %s
        """,
        (logical_job_id, attempt_id),
    ).fetchone()
    return None if row is None else _stored_requirement_attempt(tuple(row))


def read_acquisition_clock(cursor: Cursor[Any], *, epoch_id: int) -> AcquisitionClock:
    """Read immutable lease configuration, then sample ``clock_timestamp`` once."""

    config_row = cursor.execute(
        """
        SELECT lease_duration_ms, config_digest
        FROM groundloop_m5_runtime_operational_config
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    if config_row is None:
        raise ValidationError("typed M5 epoch lacks its operational configuration")
    config = M5RuntimeOperationalConfig(
        lease_duration_ms=int(config_row[0]),
        config_digest=str(config_row[1]).strip(),
    )
    clock_row = cursor.execute(
        """
        WITH decision AS MATERIALIZED (
            SELECT clock_timestamp() AS decision_time
        )
        SELECT decision_time,
               decision_time + %s * interval '1 millisecond'
        FROM decision
        """,
        (config.lease_duration_ms,),
    ).fetchone()
    if clock_row is None or not all(isinstance(value, datetime) for value in clock_row):
        raise ValidationError("PostgreSQL did not return a valid acquisition clock")
    decision_time, lease_expires_at = clock_row
    if lease_expires_at <= decision_time:
        raise ValidationError("database-computed M5 lease deadline is not later")
    return AcquisitionClock(decision_time, lease_expires_at, config)


def requirement_fallback_required(
    cursor: Cursor[Any], *, epoch_id: int, job_kind: M5JobKind, logical_job_id: str
) -> bool:
    """Hydrate and validate the immutable forward-root fallback provenance."""

    row = cursor.execute(
        """
        SELECT fallback_required, provenance_digest
        FROM groundloop_m5_requirement_root_provenance
        WHERE epoch_id = %s AND root_job_id = %s
        """,
        (epoch_id, logical_job_id),
    ).fetchone()
    if job_kind is not M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL:
        if row is not None:
            raise ValidationError("non-forward M5 job has fallback provenance")
        return False
    if row is None:
        raise ValidationError("forward M5 job lacks fallback provenance")
    provenance = M5RequirementRootProvenance(
        epoch_id=epoch_id,
        root_job_id=logical_job_id,
        fallback_required=bool(row[0]),
        provenance_digest=str(row[1]).strip(),
    )
    return provenance.fallback_required


def load_requirement_dispatch(
    cursor: Cursor[Any], *, epoch_id: int, attempt: M5JobAttempt
) -> M5DispatchRecord:
    """Hydrate the immutable dispatch bound to one requirement attempt."""

    maximum_columns = ", ".join(f"maximum_{column}" for column in _WORK_COUNTER_COLUMNS)
    row = cursor.execute(
        f"""
        SELECT logical_job_id, attempt_ordinal, job_kind, fallback_required,
               dispatched_revision, lease_expires_at,
               {maximum_columns}, maximum_work_digest, record_digest
        FROM groundloop_m5_dispatch_record
        WHERE epoch_id = %s AND subgraph = 'requirement' AND attempt_id = %s
        """,
        (epoch_id, attempt.attempt_id),
    ).fetchone()
    if row is None:
        raise ValidationError("M5 attempt lacks its immutable dispatch record")
    work_start = 6
    work_end = work_start + len(_WORK_COUNTER_COLUMNS)
    maximum = M5RuntimeWork(
        **dict(
            zip(_WORK_COUNTER_COLUMNS, map(int, row[work_start:work_end]), strict=True)
        ),
        work_digest=str(row[work_end]).strip(),
    )
    dispatch = M5DispatchRecord(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=attempt.attempt_id,
        logical_job_id=str(row[0]).strip(),
        attempt_ordinal=int(row[1]),
        job_kind=str(row[2]),
        fallback_required=bool(row[3]),
        dispatched_revision=int(row[4]),
        lease_expires_at=row[5],
        maximum_ambiguous_call_work=maximum,
        record_digest=str(row[work_end + 1]).strip(),
    )
    if (
        dispatch.attempt_id != attempt.attempt_id
        or dispatch.logical_job_id != attempt.logical_job_id
        or dispatch.attempt_ordinal != attempt.attempt_ordinal
        or dispatch.lease_expires_at != attempt.lease_expires_at
    ):
        raise ValidationError("M5 dispatch disagrees with its durable attempt")
    return dispatch


def build_requirement_execution_accounting(
    *,
    epoch_id: int,
    expected_revision: int,
    attempt: M5JobAttempt,
    dispatch: M5DispatchRecord,
    disposition: M5ExecutionEvidenceDisposition,
    result_or_error_hash: str,
    attempt_work: M5RuntimeWork,
    attempt_timing: M5RuntimeTiming | None,
    anchor_kind: M5RuntimeWorkContributionKind = (
        M5RuntimeWorkContributionKind.M5_ATTEMPT_EXECUTION
    ),
    anchor_revision: int | None = None,
) -> RequirementExecutionAccounting:
    """Build and cross-check the immutable settlement evidence tuple."""

    observation = M5RuntimeTimingObservation.build(attempt_timing)
    attempt_timing_digest = digests.attempt_runtime_timing_digest(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=attempt.attempt_id,
        observation_digest=observation.observation_digest,
    )
    evidence = M5AttemptExecutionEvidence.build(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=attempt.attempt_id,
        disposition=disposition,
        result_or_error_hash=result_or_error_hash,
        attempt_work=attempt_work,
        attempt_timing_digest=attempt_timing_digest,
    )
    evidence.validate_dispatch(dispatch)
    evidence.validate_timing(observation)
    anchor = M5TransitionTimingAnchor.build(
        epoch_id=epoch_id,
        contribution_kind=anchor_kind,
        source_id=attempt.attempt_id,
        anchor_revision=(
            expected_revision + 1 if anchor_revision is None else anchor_revision
        ),
        terminal_transition=False,
    )
    return RequirementExecutionAccounting(evidence, observation, anchor)


def read_requirement_execution_replay(
    cursor: Cursor[Any],
    *,
    expected_evidence: M5AttemptExecutionEvidence,
    expected_observation: M5RuntimeTimingObservation,
) -> int | None:
    """Validate a complete immutable evidence branch, or report its absence."""

    attempt_columns = ", ".join(
        f"evidence.attempt_{column}" for column in _WORK_COUNTER_COLUMNS
    )
    row = cursor.execute(
        f"""
        SELECT {attempt_columns}, evidence.attempt_work_digest,
               evidence.disposition, evidence.result_or_error_hash,
               evidence.attempt_timing_digest, evidence.evidence_digest,
               timing.required_interval_observed,
               timing.coordinator_non_db_non_neural_ns, timing.neural_wall_ns,
               timing.postgres_roundtrip_wall_ns, timing.external_io_wall_ns,
               timing.end_to_end_wall_ns, timing.postgres_server_execution_ns,
               timing.postgres_lock_wait_ns, timing.postgres_wal_bytes,
               timing.postgres_shared_block_reads, timing.observation_digest,
               timing.attempt_timing_digest,
               contribution.work_digest, contribution.source_identity_hash,
               contribution.contribution_key_digest,
               contribution.applied_revision
        FROM groundloop_m5_attempt_execution_evidence AS evidence
        LEFT JOIN groundloop_m5_runtime_timing_contribution AS timing
          ON timing.epoch_id = evidence.epoch_id
         AND timing.subgraph = evidence.subgraph
         AND timing.attempt_id = evidence.attempt_id
        LEFT JOIN groundloop_m5_runtime_work_contribution AS contribution
          ON contribution.epoch_id = evidence.epoch_id
         AND contribution.contribution_kind = 'm5_attempt_execution'
         AND contribution.source_id = evidence.attempt_id
        WHERE evidence.epoch_id = %s AND evidence.subgraph = 'requirement'
          AND evidence.attempt_id = %s
        """,
        (expected_evidence.epoch_id, expected_evidence.attempt_id),
    ).fetchone()
    if row is None:
        return None
    values = tuple(row)
    work_end = len(_WORK_COUNTER_COLUMNS)
    work = _work_from_row(values, digest_index=work_end)
    timing_start = work_end + 5
    if all(value is None for value in values[timing_start:]):
        postterminal = cursor.execute(
            """
            SELECT 1
            FROM groundloop_m5_post_terminal_attempt_audit AS audit
            JOIN groundloop_m5_post_terminal_attempt_timing AS timing
              ON timing.epoch_id = audit.epoch_id
             AND timing.subgraph = audit.subgraph
             AND timing.attempt_id = audit.attempt_id
            WHERE audit.epoch_id = %s AND audit.subgraph = 'requirement'
              AND audit.attempt_id = %s
            """,
            (expected_evidence.epoch_id, expected_evidence.attempt_id),
        ).fetchone()
        if postterminal is not None:
            return None
        raise ValidationError("M5 execution evidence lacks complete accounting")
    if any(value is None for value in values[-4:]):
        raise ValidationError("M5 execution evidence lacks complete event accounting")
    timing_values = values[timing_start + 1 : timing_start + 10]
    observed = bool(values[timing_start])
    timing = None
    if observed:
        timing = M5RuntimeTiming(*timing_values)
    elif any(value is not None for value in timing_values):
        raise ValidationError("stored missing M5 attempt timing has values")
    observation = M5RuntimeTimingObservation(
        observed,
        timing,
        str(values[timing_start + 10]).strip(),
    )
    evidence = M5AttemptExecutionEvidence(
        epoch_id=expected_evidence.epoch_id,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=expected_evidence.attempt_id,
        disposition=M5ExecutionEvidenceDisposition(str(values[work_end + 1])),
        result_or_error_hash=str(values[work_end + 2]).strip(),
        attempt_work=work,
        attempt_timing_digest=str(values[work_end + 3]).strip(),
        evidence_digest=str(values[work_end + 4]).strip(),
    )
    anchor = M5TransitionTimingAnchor.build(
        epoch_id=evidence.epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.M5_ATTEMPT_EXECUTION,
        source_id=evidence.attempt_id,
        anchor_revision=int(values[-1]),
        terminal_transition=False,
    )
    evidence.validate_timing(observation)
    expected_key = digests.runtime_work_contribution_key_digest(
        epoch_id=evidence.epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.M5_ATTEMPT_EXECUTION,
        source_id=evidence.attempt_id,
    )
    if (
        evidence != expected_evidence
        or observation != expected_observation
        or anchor.contribution_kind
        is not M5RuntimeWorkContributionKind.M5_ATTEMPT_EXECUTION
        or anchor.source_id != evidence.attempt_id
        or anchor.contribution_key_digest != expected_key
        or str(values[-4]).strip() != evidence.attempt_work.work_digest
        or str(values[-3]).strip() != evidence.evidence_digest
        or str(values[-2]).strip() != anchor.contribution_key_digest
        or str(values[timing_start + 11]).strip() != evidence.attempt_timing_digest
    ):
        raise EventConflictError("M5 failure replay changed immutable evidence")
    return anchor.anchor_revision


def read_requirement_postterminal_replay(
    cursor: Cursor[Any],
    *,
    expected_evidence: M5AttemptExecutionEvidence,
    expected_observation: M5RuntimeTimingObservation,
) -> RequirementPostterminalReplay | None:
    """Validate an exact audit-only evidence branch, or report its absence."""

    attempt_columns = ", ".join(f"attempt_{column}" for column in _WORK_COUNTER_COLUMNS)
    evidence_row = cursor.execute(
        f"""
        SELECT {attempt_columns}, attempt_work_digest, disposition,
               result_or_error_hash, attempt_timing_digest, evidence_digest
        FROM groundloop_m5_attempt_execution_evidence
        WHERE epoch_id = %s AND subgraph = 'requirement' AND attempt_id = %s
        """,
        (expected_evidence.epoch_id, expected_evidence.attempt_id),
    ).fetchone()
    timing_row = cursor.execute(
        """
        SELECT required_interval_observed,
               coordinator_non_db_non_neural_ns, neural_wall_ns,
               postgres_roundtrip_wall_ns, external_io_wall_ns,
               end_to_end_wall_ns, postgres_server_execution_ns,
               postgres_lock_wait_ns, postgres_wal_bytes,
               postgres_shared_block_reads, observation_digest,
               attempt_timing_digest
        FROM groundloop_m5_post_terminal_attempt_timing
        WHERE epoch_id = %s AND subgraph = 'requirement' AND attempt_id = %s
        """,
        (expected_evidence.epoch_id, expected_evidence.attempt_id),
    ).fetchone()
    audit_row = cursor.execute(
        """
        SELECT return_kind, return_artifact_digest,
               execution_evidence_digest, work_digest, timing_digest,
               terminal_logical_result_hash
        FROM groundloop_m5_post_terminal_attempt_audit
        WHERE epoch_id = %s AND subgraph = 'requirement' AND attempt_id = %s
        """,
        (expected_evidence.epoch_id, expected_evidence.attempt_id),
    ).fetchone()
    if evidence_row is None and timing_row is None and audit_row is None:
        return None
    if evidence_row is None or timing_row is None or audit_row is None:
        raise ValidationError("postterminal requirement evidence is only partial")
    evidence_values = tuple(evidence_row)
    work_end = len(_WORK_COUNTER_COLUMNS)
    stored_work = _work_from_row(evidence_values, digest_index=work_end)
    evidence = M5AttemptExecutionEvidence(
        epoch_id=expected_evidence.epoch_id,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=expected_evidence.attempt_id,
        disposition=M5ExecutionEvidenceDisposition(str(evidence_values[work_end + 1])),
        result_or_error_hash=str(evidence_values[work_end + 2]).strip(),
        attempt_work=stored_work,
        attempt_timing_digest=str(evidence_values[work_end + 3]).strip(),
        evidence_digest=str(evidence_values[work_end + 4]).strip(),
    )
    timing_values = tuple(timing_row)
    observed = bool(timing_values[0])
    raw_timing = timing_values[1:10]
    timing = M5RuntimeTiming(*raw_timing) if observed else None
    if not observed and any(value is not None for value in raw_timing):
        raise ValidationError("stored missing postterminal timing has values")
    observation = M5RuntimeTimingObservation(
        observed, timing, str(timing_values[10]).strip()
    )
    evidence.validate_timing(observation)
    audit = tuple(str(value).strip() for value in audit_row)
    if (
        evidence != expected_evidence
        or observation != expected_observation
        or str(timing_values[11]).strip() != evidence.attempt_timing_digest
        or audit[0] not in {"expired_return", "terminal_audit_only"}
        or audit[2] != evidence.evidence_digest
        or audit[3] != evidence.attempt_work.work_digest
        or audit[4] != evidence.attempt_timing_digest
    ):
        raise EventConflictError("postterminal replay changed immutable evidence")
    return RequirementPostterminalReplay(audit[0], audit[1], audit[5])


def root_result_is_reserved(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    logical_job_id: str,
    attempt: StoredRequirementAttempt,
) -> bool:
    """Check the complete committed root-result reservation closure."""

    if attempt.state != "completed" or attempt.attempt_output_digest is None:
        return False
    row = cursor.execute(
        """
        SELECT 1
        FROM groundloop_m5_semantic_job AS job
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.epoch_id = job.epoch_id
         AND scope.root_job_id = job.logical_job_id
        JOIN groundloop_m5_job_attempt AS attempt
          ON attempt.logical_job_id = job.logical_job_id
        JOIN groundloop_m5_attempt_result_artifact AS artifact
          ON artifact.attempt_id = attempt.attempt_id
         AND artifact.logical_job_id = attempt.logical_job_id
         AND artifact.job_epoch_id = job.epoch_id
        JOIN groundloop_m5_requirement_discovery_result AS result
          ON result.root_job_id = scope.root_job_id
         AND result.staged_epoch_id = scope.epoch_id
         AND result.scope_contract_digest = scope.scope_contract_digest
        WHERE job.epoch_id = %s
          AND job.logical_job_id = %s
          AND job.parent_job_id IS NULL
          AND job.job_kind IN (
              'forward_requirement_retrieval', 'reverse_requirement_discovery'
          )
          AND job.job_state = 'running'
          AND attempt.attempt_id = %s
          AND attempt.attempt_state = 'completed'
          AND attempt.attempt_output_digest = %s
          AND attempt.execution_spec_hash = job.execution_spec_hash
          AND artifact.disposition = 'root_result_staged'
          AND artifact.attempt_output_digest = attempt.attempt_output_digest
          AND artifact.payload_hash = job.payload_hash
          AND artifact.execution_spec_hash = job.execution_spec_hash
          AND artifact.result_artifact_id = result.result_artifact_id
          AND artifact.result_artifact_hash = result.result_artifact_hash
          AND artifact.job_state_at_receipt = 'running'
          AND artifact.job_state_after = 'running'
          AND artifact.archive_reason IS NOT DISTINCT FROM CASE
              WHEN NOT artifact.epoch_active THEN 'epoch_failed'
              WHEN artifact.requirement_active IS FALSE
                OR artifact.group_active IS FALSE THEN 'subject_inactive'
              WHEN artifact.chunk_active IS FALSE THEN 'chunk_inactive'
              ELSE NULL
          END
          AND artifact.cancelled_by_event_id IS NULL
          AND artifact.cancelled_by_epoch_id IS NULL
          AND artifact.cancellation_reason IS NULL
          AND scope.scope_state = 'result_staged'
          AND scope.staged_result_artifact_hash = result.result_artifact_hash
          AND scope.staged_revision = result.staged_revision
        """,
        (
            epoch_id,
            logical_job_id,
            attempt.attempt.attempt_id,
            attempt.attempt_output_digest,
        ),
    ).fetchone()
    return row is not None


def persist_requirement_dispatch(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    attempt: M5JobAttempt,
    job_kind: M5JobKind,
    fallback_required: bool,
    takeover_attempt_id: str | None,
    decision_time: datetime,
) -> tuple[M5DispatchRecord, M5TransitionTimingAnchor]:
    """Persist one new/takeover dispatch and its point-maintained accounting."""

    if attempt.lease_expires_at is None or attempt.attempt_work_digest is None:
        raise ValidationError("recovery dispatch requires operational attempt fields")
    if attempt.attempt_work_digest != M5RuntimeWork().work_digest:
        raise ValidationError("new M5 dispatch must start with canonical-zero work")
    resulting_revision = expected_revision + 1
    dispatch = M5DispatchRecord.build(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=attempt.attempt_id,
        logical_job_id=attempt.logical_job_id,
        attempt_ordinal=attempt.attempt_ordinal,
        job_kind=job_kind.value,
        fallback_required=fallback_required,
        dispatched_revision=resulting_revision,
        lease_expires_at=attempt.lease_expires_at,
    )
    anchor = M5TransitionTimingAnchor.build(
        epoch_id=epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.M5_ACQUISITION,
        source_id=dispatch.record_digest,
        anchor_revision=resulting_revision,
        terminal_transition=False,
    )
    _lock_work_accumulator(
        cursor, epoch_id=epoch_id, expected_revision=expected_revision
    )
    timing_accumulator = _lock_timing_accumulator(
        cursor, epoch_id=epoch_id, expected_revision=expected_revision
    )
    _resolve_pending_anchor(cursor, epoch_id=epoch_id, accumulator=timing_accumulator)

    cursor.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
        (epoch_id, expected_revision),
    )
    if takeover_attempt_id is not None:
        expired = cursor.execute(
            """
            UPDATE groundloop_m5_job_attempt
            SET attempt_state = 'expired', finished_at = %s
            WHERE attempt_id = %s AND logical_job_id = %s
              AND attempt_state = 'dispatched'
            """,
            (decision_time, takeover_attempt_id, attempt.logical_job_id),
        ).rowcount
        if expired != 1:
            raise EventConflictError("M5 takeover lost its current attempt")

    cursor.execute(
        """
        INSERT INTO groundloop_m5_job_attempt (
            attempt_id, logical_job_id, attempt_ordinal,
            execution_spec_hash, lease_token_hash, attempt_state,
            attempt_output_digest, error_hash, dispatched_at, finished_at,
            lease_expires_at, attempt_work_digest
        ) VALUES (
            %s, %s, %s, %s, %s, 'dispatched',
            NULL, NULL, %s, NULL, %s, %s
        )
        """,
        (
            attempt.attempt_id,
            attempt.logical_job_id,
            attempt.attempt_ordinal,
            attempt.execution_spec_hash,
            attempt.lease_token_hash,
            decision_time,
            attempt.lease_expires_at,
            attempt.attempt_work_digest,
        ),
    )
    _insert_dispatch(cursor, dispatch)
    _insert_zero_acquisition_contribution(cursor, dispatch, anchor)
    return dispatch, anchor


def finish_requirement_dispatch_accounting(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    anchor: M5TransitionTimingAnchor,
) -> None:
    """Advance both point accumulators after the caller advances epoch revision."""

    resulting_revision = expected_revision + 1
    if anchor.anchor_revision != resulting_revision:
        raise ValidationError("acquisition anchor revision is inconsistent")
    work_updated = cursor.execute(
        """
        UPDATE groundloop_m5_runtime_work_accumulator
        SET updated_revision = %s, updated_at = clock_timestamp()
        WHERE epoch_id = %s AND updated_revision = %s AND NOT terminalized
        """,
        (resulting_revision, epoch_id, expected_revision),
    ).rowcount
    if work_updated != 1:
        raise ValidationError("M5 work accumulator is not at acquisition input")

    timing_updated = cursor.execute(
        """
        UPDATE groundloop_m5_runtime_timing_accumulator
        SET required_expected_count = required_expected_count + 1,
            required_missing_count = required_missing_count
                + CASE WHEN pending_anchor_revision IS NULL THEN 0 ELSE 1 END,
            postgres_server_execution_expected_count =
                postgres_server_execution_expected_count + 1,
            postgres_server_execution_missing_count =
                postgres_server_execution_missing_count
                + CASE WHEN pending_anchor_revision IS NULL THEN 0 ELSE 1 END,
            postgres_lock_wait_expected_count =
                postgres_lock_wait_expected_count + 1,
            postgres_lock_wait_missing_count =
                postgres_lock_wait_missing_count
                + CASE WHEN pending_anchor_revision IS NULL THEN 0 ELSE 1 END,
            postgres_wal_bytes_expected_count =
                postgres_wal_bytes_expected_count + 1,
            postgres_wal_bytes_missing_count =
                postgres_wal_bytes_missing_count
                + CASE WHEN pending_anchor_revision IS NULL THEN 0 ELSE 1 END,
            postgres_shared_block_reads_expected_count =
                postgres_shared_block_reads_expected_count + 1,
            postgres_shared_block_reads_missing_count =
                postgres_shared_block_reads_missing_count
                + CASE WHEN pending_anchor_revision IS NULL THEN 0 ELSE 1 END,
            pending_contribution_kind = %s,
            pending_source_id = %s,
            pending_contribution_key_digest = %s,
            pending_anchor_revision = %s,
            updated_revision = %s,
            updated_at = clock_timestamp()
        WHERE epoch_id = %s AND updated_revision = %s AND NOT terminalized
        """,
        (
            anchor.contribution_kind.value,
            anchor.source_id,
            anchor.contribution_key_digest,
            anchor.anchor_revision,
            resulting_revision,
            epoch_id,
            expected_revision,
        ),
    ).rowcount
    if timing_updated != 1:
        raise ValidationError("M5 timing accumulator is not at acquisition input")


def _work_from_row(row: tuple[Any, ...], *, digest_index: int) -> M5RuntimeWork:
    return M5RuntimeWork(
        **dict(
            zip(
                _WORK_COUNTER_COLUMNS,
                map(int, row[:digest_index]),
                strict=True,
            )
        ),
        work_digest=str(row[digest_index]).strip(),
    )


def _lock_work_accumulator(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    terminalized: bool = False,
) -> M5RuntimeWork:
    columns = ", ".join(_WORK_COUNTER_COLUMNS)
    row = cursor.execute(
        f"""
        SELECT {columns}, work_digest, updated_revision, terminalized
        FROM groundloop_m5_runtime_work_accumulator
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise ValidationError("typed M5 epoch lacks its work accumulator")
    work_end = len(_WORK_COUNTER_COLUMNS)
    work = _work_from_row(tuple(row), digest_index=work_end)
    if (
        int(row[work_end + 1]) != expected_revision
        or bool(row[work_end + 2]) is not terminalized
    ):
        raise ValidationError("M5 work accumulator is not at the required revision")
    return work


def _lock_timing_accumulator(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    allow_terminal: bool = False,
) -> _TimingAccumulator:
    columns = ", ".join((*_TIMING_SUM_COLUMNS, *_TIMING_COVERAGE_COLUMNS))
    row = cursor.execute(
        f"""
        SELECT {columns}, updated_revision, terminalized, pending_contribution_kind,
               pending_source_id, pending_contribution_key_digest,
               pending_anchor_revision
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise ValidationError("typed M5 epoch lacks its timing accumulator")
    values = tuple(row)
    sums_end = len(_TIMING_SUM_COLUMNS)
    coverage_end = sums_end + len(_TIMING_COVERAGE_COLUMNS)
    accumulator = _TimingAccumulator(
        sums=M5RuntimeTiming(*map(int, values[:sums_end])),
        coverage_counts=tuple(map(int, values[sums_end:coverage_end])),
        updated_revision=int(values[coverage_end]),
        terminalized=bool(values[coverage_end + 1]),
        pending_contribution_kind=(
            None if values[coverage_end + 2] is None else str(values[coverage_end + 2])
        ),
        pending_source_id=(
            None if values[coverage_end + 3] is None else str(values[coverage_end + 3])
        ),
        pending_contribution_key_digest=(
            None
            if values[coverage_end + 4] is None
            else str(values[coverage_end + 4]).strip()
        ),
        pending_anchor_revision=(
            None if values[coverage_end + 5] is None else int(values[coverage_end + 5])
        ),
    )
    if accumulator.updated_revision != expected_revision or (
        accumulator.terminalized and not allow_terminal
    ):
        raise ValidationError("M5 timing accumulator is not at the active revision")
    _ = accumulator.has_pending_anchor
    return accumulator


def _stored_transition_observation(
    row: tuple[Any, ...],
) -> tuple[str, M5RuntimeTimingObservation, str]:
    contribution_key_digest = str(row[0]).strip()
    required_observed = bool(row[1])
    raw_timing = tuple(row[2:11])
    if required_observed:
        if any(value is None for value in raw_timing[:5]):
            raise ValidationError("stored transition timing lacks required values")
        timing = M5RuntimeTiming(*raw_timing)
    else:
        if any(value is not None for value in raw_timing):
            raise ValidationError("stored missing transition timing has values")
        timing = None
    observation = M5RuntimeTimingObservation(
        required_observed,
        timing,
        str(row[11]).strip(),
    )
    return contribution_key_digest, observation, str(row[12]).strip()


def append_transition_call_timing_checked(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    contribution_kind: M5RuntimeWorkContributionKind,
    source_id: str,
    contribution_key_digest: str,
    anchor_revision: int,
    current_revision: int,
    observed_timing: M5RuntimeTiming | None,
    failure_injector: RecoveryFailureInjector | None = None,
) -> M5TransitionTimingReceipt:
    """Append or replay one nonterminal transition point under the timing lock."""

    anchor = M5TransitionTimingAnchor(
        epoch_id=epoch_id,
        contribution_kind=contribution_kind,
        source_id=source_id,
        contribution_key_digest=contribution_key_digest,
        anchor_revision=anchor_revision,
        terminal_transition=False,
    )
    observation = M5RuntimeTimingObservation.build(observed_timing)
    expected_digest = digests.transition_call_timing_digest(
        epoch_id=epoch_id,
        contribution_kind=contribution_kind,
        source_id=source_id,
        contribution_key_digest=contribution_key_digest,
        anchor_revision=anchor_revision,
        observation_digest=observation.observation_digest,
    )
    accumulator = _lock_timing_accumulator(
        cursor,
        epoch_id=epoch_id,
        expected_revision=current_revision,
        allow_terminal=True,
    )
    existing_row = cursor.execute(
        """
        SELECT contribution_key_digest, required_interval_observed,
               coordinator_non_db_non_neural_ns, neural_wall_ns,
               postgres_roundtrip_wall_ns, external_io_wall_ns,
               end_to_end_wall_ns, postgres_server_execution_ns,
               postgres_lock_wait_ns, postgres_wal_bytes,
               postgres_shared_block_reads, observation_digest,
               transition_timing_digest
        FROM groundloop_m5_transition_call_timing
        WHERE epoch_id = %s AND contribution_kind = %s
          AND source_id = %s AND anchor_revision = %s
        """,
        (epoch_id, contribution_kind.value, source_id, anchor_revision),
    ).fetchone()
    if existing_row is not None:
        stored_key, stored_observation, stored_digest = _stored_transition_observation(
            tuple(existing_row)
        )
        if (
            stored_key != contribution_key_digest
            or stored_observation != observation
            or stored_digest != expected_digest
        ):
            raise EventConflictError(
                "transition timing append changed an immutable observation"
            )
        event_timing, coverage = accumulator.project(pending_as_missing=True)
        receipt = M5TransitionTimingReceipt(
            anchor,
            expected_digest,
            event_timing,
            coverage,
            current_revision,
            True,
        )
        receipt.validate_observation(observation)
        return receipt

    if accumulator.terminalized:
        raise EventConflictError("terminal M5 timing cannot accept a new observation")
    if (
        not accumulator.has_pending_anchor
        or accumulator.pending_contribution_kind != contribution_kind.value
        or accumulator.pending_source_id != source_id
        or accumulator.pending_contribution_key_digest != contribution_key_digest
        or accumulator.pending_anchor_revision != anchor_revision
    ):
        raise EventConflictError(
            "transition timing does not match the sole pending anchor"
        )

    cursor.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
        (epoch_id, current_revision),
    )
    cursor.execute(
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
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            epoch_id,
            contribution_kind.value,
            source_id,
            contribution_key_digest,
            anchor_revision,
            observation.required_interval_observed,
            *_timing_values(observation),
            observation.observation_digest,
            expected_digest,
        ),
    )
    _inject_recovery(failure_injector, "transition_timing_inserted")

    timing = observation.timing
    timing_values = (0,) * len(_TIMING_SUM_COLUMNS)
    if timing is not None:
        timing_values = tuple(int(value or 0) for value in _timing_values(observation))
    required_observed = int(timing is not None)
    optional_observed = tuple(
        int(timing is not None and value is not None)
        for value in (
            None if timing is None else timing.postgres_server_execution_ns,
            None if timing is None else timing.postgres_lock_wait_ns,
            None if timing is None else timing.postgres_wal_bytes,
            None if timing is None else timing.postgres_shared_block_reads,
        )
    )
    returned_columns = ", ".join((*_TIMING_SUM_COLUMNS, *_TIMING_COVERAGE_COLUMNS))
    updated_row = cursor.execute(
        f"""
        UPDATE groundloop_m5_runtime_timing_accumulator
        SET coordinator_non_db_non_neural_ns =
                coordinator_non_db_non_neural_ns + %s,
            neural_wall_ns = neural_wall_ns + %s,
            postgres_roundtrip_wall_ns = postgres_roundtrip_wall_ns + %s,
            external_io_wall_ns = external_io_wall_ns + %s,
            end_to_end_wall_ns = end_to_end_wall_ns + %s,
            postgres_server_execution_ns = postgres_server_execution_ns + %s,
            postgres_lock_wait_ns = postgres_lock_wait_ns + %s,
            postgres_wal_bytes = postgres_wal_bytes + %s,
            postgres_shared_block_reads = postgres_shared_block_reads + %s,
            required_observed_count = required_observed_count + %s,
            required_missing_count = required_missing_count + %s,
            postgres_server_execution_observed_count =
                postgres_server_execution_observed_count + %s,
            postgres_server_execution_missing_count =
                postgres_server_execution_missing_count + %s,
            postgres_lock_wait_observed_count =
                postgres_lock_wait_observed_count + %s,
            postgres_lock_wait_missing_count =
                postgres_lock_wait_missing_count + %s,
            postgres_wal_bytes_observed_count =
                postgres_wal_bytes_observed_count + %s,
            postgres_wal_bytes_missing_count =
                postgres_wal_bytes_missing_count + %s,
            postgres_shared_block_reads_observed_count =
                postgres_shared_block_reads_observed_count + %s,
            postgres_shared_block_reads_missing_count =
                postgres_shared_block_reads_missing_count + %s,
            pending_contribution_kind = NULL,
            pending_source_id = NULL,
            pending_contribution_key_digest = NULL,
            pending_anchor_revision = NULL,
            updated_at = clock_timestamp()
        WHERE epoch_id = %s AND updated_revision = %s AND NOT terminalized
          AND pending_contribution_kind = %s AND pending_source_id = %s
          AND pending_contribution_key_digest = %s
          AND pending_anchor_revision = %s
        RETURNING {returned_columns}, updated_revision, terminalized,
                  pending_contribution_kind, pending_source_id,
                  pending_contribution_key_digest, pending_anchor_revision
        """,
        (
            *timing_values,
            required_observed,
            1 - required_observed,
            optional_observed[0],
            1 - optional_observed[0],
            optional_observed[1],
            1 - optional_observed[1],
            optional_observed[2],
            1 - optional_observed[2],
            optional_observed[3],
            1 - optional_observed[3],
            epoch_id,
            current_revision,
            contribution_kind.value,
            source_id,
            contribution_key_digest,
            anchor_revision,
        ),
    ).fetchone()
    if updated_row is None:
        raise EventConflictError("transition timing accumulator CAS failed")
    _inject_recovery(failure_injector, "transition_timing_accumulator_updated")
    updated = _timing_accumulator_from_row(tuple(updated_row))
    event_timing, coverage = updated.project(pending_as_missing=False)
    receipt = M5TransitionTimingReceipt(
        anchor,
        expected_digest,
        event_timing,
        coverage,
        current_revision,
        False,
    )
    receipt.validate_observation(observation)
    return receipt


def _timing_accumulator_from_row(row: tuple[Any, ...]) -> _TimingAccumulator:
    sums_end = len(_TIMING_SUM_COLUMNS)
    coverage_end = sums_end + len(_TIMING_COVERAGE_COLUMNS)
    return _TimingAccumulator(
        sums=M5RuntimeTiming(*map(int, row[:sums_end])),
        coverage_counts=tuple(map(int, row[sums_end:coverage_end])),
        updated_revision=int(row[coverage_end]),
        terminalized=bool(row[coverage_end + 1]),
        pending_contribution_kind=(
            None if row[coverage_end + 2] is None else str(row[coverage_end + 2])
        ),
        pending_source_id=(
            None if row[coverage_end + 3] is None else str(row[coverage_end + 3])
        ),
        pending_contribution_key_digest=(
            None
            if row[coverage_end + 4] is None
            else str(row[coverage_end + 4]).strip()
        ),
        pending_anchor_revision=(
            None if row[coverage_end + 5] is None else int(row[coverage_end + 5])
        ),
    )


def read_current_event_timing(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    structural_event_id: str,
    current_revision: int,
    terminal: bool,
) -> tuple[M5RuntimeTiming, M5RuntimeTimingCoverage]:
    """Read the frozen terminal image or project a live pending point missing."""

    if terminal:
        timing_columns = ", ".join(f"result.{column}" for column in _TIMING_SUM_COLUMNS)
        coverage_columns = ", ".join(
            f"coverage.{column}" for column in _TIMING_COVERAGE_COLUMNS
        )
        row = cursor.execute(
            f"""
            SELECT {timing_columns}, {coverage_columns},
                   coverage.terminal_client_roundtrip_included
            FROM groundloop_m5_event_result AS result
            JOIN groundloop_m5_event_timing_coverage AS coverage
              ON coverage.structural_event_id = result.structural_event_id
             AND coverage.epoch_id = result.epoch_id
            WHERE result.epoch_id = %s AND result.structural_event_id = %s
            """,
            (epoch_id, structural_event_id),
        ).fetchone()
        if row is None:
            raise ValidationError(
                "terminal M5 epoch lacks frozen event timing coverage"
            )
        values = tuple(row)
        sums_end = len(_TIMING_SUM_COLUMNS)
        coverage_end = sums_end + len(_TIMING_COVERAGE_COLUMNS)
        timing = M5RuntimeTiming(
            *map(int, values[:5]),
            *(None if value is None else int(value) for value in values[5:sums_end]),
        )
        counts = tuple(map(int, values[sums_end:coverage_end]))
        coverage = M5RuntimeTimingCoverage(
            counts[0],
            counts[1],
            counts[2],
            counts[3],
            counts[4],
            counts[5],
            counts[6],
            counts[7],
            counts[8],
            counts[9],
            counts[10],
            counts[11],
            counts[12],
            counts[13],
            counts[14],
            bool(values[coverage_end]),
        )
        coverage.validate_aggregate(timing)
        return timing, coverage

    columns = ", ".join((*_TIMING_SUM_COLUMNS, *_TIMING_COVERAGE_COLUMNS))
    row = cursor.execute(
        f"""
        SELECT {columns}, updated_revision, terminalized,
               pending_contribution_kind, pending_source_id,
               pending_contribution_key_digest, pending_anchor_revision
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise ValidationError("typed M5 epoch lacks its timing accumulator")
    accumulator = _timing_accumulator_from_row(tuple(row))
    if accumulator.updated_revision != current_revision or accumulator.terminalized:
        raise ValidationError("M5 timing accumulator is not at the active revision")
    return accumulator.project(pending_as_missing=True)


def _terminal_telemetry_values(
    *,
    structural_event_id: str,
    epoch_id: int,
    terminal_logical_result_hash: str,
    call_timing: M5RuntimeTiming | None,
    call_timing_coverage: M5RuntimeTimingCoverage,
) -> tuple[Any, ...]:
    observation = M5RuntimeTimingObservation.build(call_timing)
    return (
        structural_event_id,
        epoch_id,
        terminal_logical_result_hash,
        observation.required_interval_observed,
        *_timing_values(observation),
        *(getattr(call_timing_coverage, name) for name in _TIMING_COVERAGE_COLUMNS),
        call_timing_coverage.terminal_client_roundtrip_included,
    )


def _stored_terminal_telemetry_values(row: tuple[Any, ...]) -> tuple[Any, ...]:
    timing_start = 4
    timing_end = timing_start + len(_TIMING_SUM_COLUMNS)
    coverage_end = timing_end + len(_TIMING_COVERAGE_COLUMNS)
    return (
        str(row[0]),
        int(row[1]),
        str(row[2]).strip(),
        bool(row[3]),
        *(
            None if value is None else int(value)
            for value in row[timing_start:timing_end]
        ),
        *(int(value) for value in row[timing_end:coverage_end]),
        bool(row[coverage_end]),
    )


def persist_terminal_invocation_telemetry(
    cursor: Cursor[Any],
    *,
    invocation_id: str,
    structural_event_id: str,
    epoch_id: int,
    terminal_logical_result_hash: str,
    call_timing: M5RuntimeTiming | None,
    call_timing_coverage: M5RuntimeTimingCoverage,
    failure_injector: RecoveryFailureInjector | None = None,
) -> None:
    """Append or exactly replay one postcommit terminal-call observation."""

    if not isinstance(invocation_id, str) or not invocation_id.strip():
        raise ValidationError("terminal invocation ID must be nonempty")
    if not isinstance(structural_event_id, str) or not structural_event_id.strip():
        raise ValidationError("terminal telemetry event ID must be nonempty")
    if isinstance(epoch_id, bool) or not isinstance(epoch_id, int) or epoch_id < 1:
        raise ValidationError("terminal telemetry epoch ID must be positive")
    if (
        not isinstance(terminal_logical_result_hash, str)
        or len(terminal_logical_result_hash) != 64
        or terminal_logical_result_hash != terminal_logical_result_hash.lower()
        or any(
            character not in "0123456789abcdef"
            for character in terminal_logical_result_hash
        )
    ):
        raise ValidationError("terminal telemetry requires a SHA-256 result hash")
    expected_coverage = M5RuntimeTimingCoverage.single_point(
        call_timing,
        terminal_client_roundtrip_included=call_timing is not None,
    )
    if call_timing_coverage != expected_coverage:
        raise ValidationError("terminal invocation timing coverage is inconsistent")

    semantic_columns = (
        "structural_event_id",
        "epoch_id",
        "terminal_logical_result_hash",
        "required_interval_observed",
        *_TIMING_SUM_COLUMNS,
        *_TIMING_COVERAGE_COLUMNS,
        "terminal_client_roundtrip_included",
    )
    expected = _terminal_telemetry_values(
        structural_event_id=structural_event_id,
        epoch_id=epoch_id,
        terminal_logical_result_hash=terminal_logical_result_hash,
        call_timing=call_timing,
        call_timing_coverage=call_timing_coverage,
    )

    def read_existing() -> tuple[Any, ...] | None:
        row = cursor.execute(
            sql.SQL(
                "SELECT {} FROM groundloop_m5_postcommit_invocation_telemetry "
                "WHERE invocation_id = %s"
            ).format(sql.SQL(", ").join(map(sql.Identifier, semantic_columns))),
            (invocation_id,),
        ).fetchone()
        return None if row is None else _stored_terminal_telemetry_values(tuple(row))

    existing = read_existing()
    if existing is not None:
        if existing != expected:
            raise EventConflictError(
                "terminal invocation ID has conflicting immutable telemetry"
            )
        return

    terminal = cursor.execute(
        """
        SELECT 1
        FROM groundloop_m5_event_result
        WHERE structural_event_id = %s AND epoch_id = %s
          AND logical_result_hash = %s
        """,
        (structural_event_id, epoch_id, terminal_logical_result_hash),
    ).fetchone()
    if terminal is None:
        raise EventConflictError("terminal telemetry binds no exact terminal result")

    columns = ("invocation_id", *semantic_columns)
    inserted = cursor.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_postcommit_invocation_telemetry ({}) "
            "VALUES ({}) ON CONFLICT (invocation_id) DO NOTHING"
        ).format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        (invocation_id, *expected),
    ).rowcount
    if inserted == 1:
        _inject_recovery(failure_injector, "terminal_invocation_telemetry_inserted")
        return
    concurrent = read_existing()
    if concurrent != expected:
        raise EventConflictError(
            "terminal invocation ID has conflicting immutable telemetry"
        )


def _resolve_pending_anchor(
    cursor: Cursor[Any], *, epoch_id: int, accumulator: _TimingAccumulator
) -> None:
    if not accumulator.has_pending_anchor:
        return
    assert accumulator.pending_contribution_kind is not None
    assert accumulator.pending_source_id is not None
    assert accumulator.pending_contribution_key_digest is not None
    assert accumulator.pending_anchor_revision is not None
    observation = M5RuntimeTimingObservation.build(None)
    transition_digest = digests.transition_call_timing_digest(
        epoch_id=epoch_id,
        contribution_kind=accumulator.pending_contribution_kind,
        source_id=accumulator.pending_source_id,
        contribution_key_digest=accumulator.pending_contribution_key_digest,
        anchor_revision=accumulator.pending_anchor_revision,
        observation_digest=observation.observation_digest,
    )
    cursor.execute(
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
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, %s, %s
        )
        """,
        (
            epoch_id,
            accumulator.pending_contribution_kind,
            accumulator.pending_source_id,
            accumulator.pending_contribution_key_digest,
            accumulator.pending_anchor_revision,
            observation.observation_digest,
            transition_digest,
        ),
    )


def start_event_accounting(
    cursor: Cursor[Any], *, epoch_id: int, expected_revision: int
) -> EventAccountingStart:
    """Lock exact point images and classify the prior pending anchor missing."""

    work = _lock_work_accumulator(
        cursor, epoch_id=epoch_id, expected_revision=expected_revision
    )
    timing = _lock_timing_accumulator(
        cursor, epoch_id=epoch_id, expected_revision=expected_revision
    )
    _resolve_pending_anchor(cursor, epoch_id=epoch_id, accumulator=timing)
    return EventAccountingStart(work, timing)


def lock_epoch_failure_accounting(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    terminal_replay: bool = False,
) -> EventAccountingStart:
    """Lock failure accumulators without resolving the pending anchor yet.

    C7's cooperative failure transaction must acquire every M4/M5 detail lock
    before its first write. ``start_event_accounting`` remains the ordinary
    transition helper and may materialize a missing prior timing point. This
    failure-only variant deliberately returns the exact locked images without
    performing that insertion; the fused terminal finalizer resolves the prior
    point after the complete lock plan has validated.
    """

    if type(terminal_replay) is not bool:
        raise ValidationError("terminal_replay must be an exact boolean")
    work = _lock_work_accumulator(
        cursor,
        epoch_id=epoch_id,
        expected_revision=expected_revision,
        terminalized=terminal_replay,
    )
    timing = _lock_timing_accumulator(
        cursor,
        epoch_id=epoch_id,
        expected_revision=expected_revision,
        allow_terminal=terminal_replay,
    )
    if timing.terminalized is not terminal_replay:
        raise ValidationError("M5 timing accumulator terminal shape is inconsistent")
    if terminal_replay and timing.has_pending_anchor:
        raise ValidationError("terminal timing accumulator retains a pending anchor")
    return EventAccountingStart(work, timing)


def _insert_dispatch(cursor: Cursor[Any], dispatch: M5DispatchRecord) -> None:
    maximum_columns = tuple(f"maximum_{name}" for name in _WORK_COUNTER_COLUMNS)
    columns = (
        "epoch_id",
        *maximum_columns,
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
    values = (
        dispatch.epoch_id,
        *dispatch.maximum_ambiguous_call_work.counter_values(),
        dispatch.maximum_ambiguous_call_work.work_digest,
        dispatch.subgraph.value,
        dispatch.attempt_id,
        dispatch.logical_job_id,
        dispatch.attempt_ordinal,
        dispatch.job_kind,
        dispatch.fallback_required,
        dispatch.dispatched_revision,
        dispatch.lease_expires_at,
        dispatch.record_digest,
    )
    cursor.execute(
        sql.SQL("INSERT INTO groundloop_m5_dispatch_record ({}) VALUES ({})").format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        values,
    )


def _insert_zero_acquisition_contribution(
    cursor: Cursor[Any],
    dispatch: M5DispatchRecord,
    anchor: M5TransitionTimingAnchor,
) -> None:
    zero = M5RuntimeWork()
    columns = (
        "epoch_id",
        *_WORK_COUNTER_COLUMNS,
        "work_digest",
        "contribution_kind",
        "source_id",
        "source_identity_hash",
        "contribution_key_digest",
        "applied_revision",
    )
    values = (
        dispatch.epoch_id,
        *zero.counter_values(),
        zero.work_digest,
        anchor.contribution_kind.value,
        anchor.source_id,
        dispatch.record_digest,
        anchor.contribution_key_digest,
        anchor.anchor_revision,
    )
    cursor.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_contribution ({}) VALUES ({})"
        ).format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        values,
    )


def _insert_requirement_execution_evidence(
    cursor: Cursor[Any], accounting: RequirementExecutionAccounting
) -> None:
    evidence = accounting.evidence
    columns = (
        "epoch_id",
        *(f"attempt_{name}" for name in _WORK_COUNTER_COLUMNS),
        "attempt_work_digest",
        "subgraph",
        "attempt_id",
        "disposition",
        "result_or_error_hash",
        "attempt_timing_digest",
        "evidence_digest",
    )
    values = (
        evidence.epoch_id,
        *evidence.attempt_work.counter_values(),
        evidence.attempt_work.work_digest,
        evidence.subgraph.value,
        evidence.attempt_id,
        evidence.disposition.value,
        evidence.result_or_error_hash,
        evidence.attempt_timing_digest,
        evidence.evidence_digest,
    )
    cursor.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_attempt_execution_evidence ({}) VALUES ({})"
        ).format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        values,
    )


def _timing_values(
    observation: M5RuntimeTimingObservation,
) -> tuple[int | None, ...]:
    timing = observation.timing
    if timing is None:
        return (None,) * 9
    return (
        timing.coordinator_non_db_non_neural_ns,
        timing.neural_wall_ns,
        timing.postgres_roundtrip_wall_ns,
        timing.external_io_wall_ns,
        timing.end_to_end_wall_ns,
        timing.postgres_server_execution_ns,
        timing.postgres_lock_wait_ns,
        timing.postgres_wal_bytes,
        timing.postgres_shared_block_reads,
    )


def persist_postterminal_requirement_accounting(
    cursor: Cursor[Any],
    *,
    accounting: RequirementExecutionAccounting,
    return_kind: str,
    return_artifact_digest: str,
    terminal_logical_result_hash: str,
    failure_injector: RecoveryFailureInjector | None = None,
) -> None:
    """Insert evidence plus audit-only timing without touching event totals."""

    if return_kind not in {"expired_return", "terminal_audit_only"}:
        raise ValidationError("postterminal requirement return kind is invalid")
    _insert_requirement_execution_evidence(cursor, accounting)
    _inject_recovery(failure_injector, "late_return_execution_evidence_inserted")
    evidence = accounting.evidence
    observation = accounting.observation
    cursor.execute(
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
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            evidence.epoch_id,
            evidence.subgraph.value,
            evidence.attempt_id,
            observation.required_interval_observed,
            *_timing_values(observation),
            observation.observation_digest,
            evidence.attempt_timing_digest,
        ),
    )
    _inject_recovery(failure_injector, "late_return_postterminal_timing_inserted")
    cursor.execute(
        """
        INSERT INTO groundloop_m5_post_terminal_attempt_audit (
            epoch_id, subgraph, attempt_id, return_kind,
            return_artifact_digest, execution_evidence_digest,
            work_digest, timing_digest, terminal_logical_result_hash
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            evidence.epoch_id,
            evidence.subgraph.value,
            evidence.attempt_id,
            return_kind,
            return_artifact_digest,
            evidence.evidence_digest,
            evidence.attempt_work.work_digest,
            evidence.attempt_timing_digest,
            terminal_logical_result_hash,
        ),
    )
    _inject_recovery(failure_injector, "late_return_postterminal_audit_inserted")


def persist_requirement_execution_accounting(
    cursor: Cursor[Any], *, accounting: RequirementExecutionAccounting
) -> None:
    """Insert the evidence, attempt point, and sole anchored work contribution."""

    _insert_requirement_execution_evidence(cursor, accounting)
    evidence = accounting.evidence
    observation = accounting.observation
    cursor.execute(
        """
        INSERT INTO groundloop_m5_runtime_timing_contribution (
            epoch_id, subgraph, attempt_id, execution_evidence_digest,
            required_interval_observed,
            coordinator_non_db_non_neural_ns, neural_wall_ns,
            postgres_roundtrip_wall_ns, external_io_wall_ns,
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
            evidence.epoch_id,
            evidence.subgraph.value,
            evidence.attempt_id,
            evidence.evidence_digest,
            observation.required_interval_observed,
            *_timing_values(observation),
            observation.observation_digest,
            evidence.attempt_timing_digest,
        ),
    )
    contribution_kind = M5RuntimeWorkContributionKind.M5_ATTEMPT_EXECUTION
    contribution_key = digests.runtime_work_contribution_key_digest(
        epoch_id=evidence.epoch_id,
        contribution_kind=contribution_kind,
        source_id=evidence.attempt_id,
    )
    columns = (
        "epoch_id",
        *_WORK_COUNTER_COLUMNS,
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
            evidence.epoch_id,
            *evidence.attempt_work.counter_values(),
            evidence.attempt_work.work_digest,
            contribution_kind.value,
            evidence.attempt_id,
            evidence.evidence_digest,
            contribution_key,
            accounting.anchor.anchor_revision,
        ),
    )


def persist_terminal_job_failure_contribution(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    resulting_revision: int,
    logical_job_id: str,
    terminal_reason: M5TerminalReason,
    error_hash: str,
) -> None:
    """Insert the separate canonical-zero terminal-job closure contribution."""

    kind = M5RuntimeWorkContributionKind.TERMINAL_JOB_FAILURE
    source_identity_hash = digests.terminal_job_failure_contribution_source_digest(
        logical_job_id=logical_job_id,
        terminal_reason=terminal_reason,
        error_hash=error_hash,
    )
    key = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=kind,
        source_id=logical_job_id,
    )
    zero = M5RuntimeWork()
    columns = (
        "epoch_id",
        *_WORK_COUNTER_COLUMNS,
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
            *zero.counter_values(),
            zero.work_digest,
            kind.value,
            logical_job_id,
            source_identity_hash,
            key,
            resulting_revision,
        ),
    )


def persist_epoch_failure_contribution(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    structural_event_id: str,
    failure_reason: M5RunFailureReason,
    resulting_revision: int,
) -> M5TransitionTimingAnchor:
    """Insert the canonical-zero epoch-failure contribution and terminal anchor."""

    if not isinstance(failure_reason, M5RunFailureReason):
        raise ValidationError("failure_reason must be an M5RunFailureReason")
    kind = M5RuntimeWorkContributionKind.EPOCH_FAILURE
    source_identity_hash = digests.epoch_failure_contribution_source_digest(
        structural_event_id=structural_event_id,
        failure_reason=failure_reason,
    )
    key = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=kind,
        source_id=structural_event_id,
    )
    anchor = M5TransitionTimingAnchor(
        epoch_id=epoch_id,
        contribution_kind=kind,
        source_id=structural_event_id,
        contribution_key_digest=key,
        anchor_revision=resulting_revision,
        terminal_transition=True,
    )
    zero = M5RuntimeWork()
    columns = (
        "epoch_id",
        *_WORK_COUNTER_COLUMNS,
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
            *zero.counter_values(),
            zero.work_digest,
            kind.value,
            structural_event_id,
            source_identity_hash,
            key,
            resulting_revision,
        ),
    )
    return anchor


def persist_root_result_stage_contribution(
    cursor: Cursor[Any],
    *,
    accounting: RequirementExecutionAccounting,
    attempt_output_digest: str,
    stage_work: M5RuntimeWork,
) -> None:
    """Insert persistence-owned root staging work for the outer anchor."""

    anchor = accounting.anchor
    if anchor.contribution_kind is not M5RuntimeWorkContributionKind.ROOT_RESULT_STAGE:
        raise ValidationError("root staging requires a root-result anchor")
    foreign_counters = tuple(
        name
        for name, value in zip(
            stage_work.counter_names(), stage_work.counter_values(), strict=True
        )
        if value and name not in _ROOT_RESULT_STAGE_COUNTERS
    )
    if foreign_counters:
        raise ValidationError(
            "root-result work uses counters owned by another surface: "
            f"{foreign_counters!r}"
        )
    columns = (
        "epoch_id",
        *_WORK_COUNTER_COLUMNS,
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
            anchor.epoch_id,
            *stage_work.counter_values(),
            stage_work.work_digest,
            anchor.contribution_kind.value,
            anchor.source_id,
            attempt_output_digest,
            anchor.contribution_key_digest,
            anchor.anchor_revision,
        ),
    )


def persist_root_barrier_contribution(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    structural_event_id: str,
    barrier_completion_hash: str,
    resulting_revision: int,
    barrier_work: M5RuntimeWork,
) -> M5TransitionTimingAnchor:
    """Insert the barrier-owned work and return its sole timing anchor."""

    foreign_counters = tuple(
        name
        for name, value in zip(
            barrier_work.counter_names(), barrier_work.counter_values(), strict=True
        )
        if value and name not in _ROOT_BARRIER_COUNTERS
    )
    if foreign_counters:
        raise ValidationError(
            "root-barrier work uses counters owned by another surface: "
            f"{foreign_counters!r}"
        )
    anchor = M5TransitionTimingAnchor.build(
        epoch_id=epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.ROOT_BARRIER,
        source_id=structural_event_id,
        anchor_revision=resulting_revision,
        terminal_transition=False,
    )
    columns = (
        "epoch_id",
        *_WORK_COUNTER_COLUMNS,
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
            *barrier_work.counter_values(),
            barrier_work.work_digest,
            anchor.contribution_kind.value,
            anchor.source_id,
            barrier_completion_hash,
            anchor.contribution_key_digest,
            anchor.anchor_revision,
        ),
    )
    return anchor


def persist_cancellation_contribution(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    plan_digest: str,
    resulting_revision: int,
    cancellation_work: M5RuntimeWork,
) -> M5TransitionTimingAnchor:
    """Insert one exact cancellation-plan contribution and timing anchor."""

    foreign_counters = tuple(
        name
        for name, value in zip(
            cancellation_work.counter_names(),
            cancellation_work.counter_values(),
            strict=True,
        )
        if value and name not in _CANCELLATION_COUNTERS
    )
    if foreign_counters or cancellation_work.requirement_cancelled_job_count < 1:
        raise ValidationError("cancellation work is not canonical")
    anchor = M5TransitionTimingAnchor.build(
        epoch_id=epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.CANCELLATION,
        source_id=plan_digest,
        anchor_revision=resulting_revision,
        terminal_transition=False,
    )
    columns = (
        "epoch_id",
        *_WORK_COUNTER_COLUMNS,
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
            *cancellation_work.counter_values(),
            cancellation_work.work_digest,
            anchor.contribution_kind.value,
            anchor.source_id,
            plan_digest,
            anchor.contribution_key_digest,
            anchor.anchor_revision,
        ),
    )
    return anchor


def persist_preterminal_late_return_contribution(
    cursor: Cursor[Any],
    *,
    accounting: RequirementExecutionAccounting,
    return_artifact_digest: str,
    late_work: M5RuntimeWork,
) -> None:
    """Insert the persistence-owned same-revision late-return contribution."""

    anchor = accounting.anchor
    if (
        anchor.contribution_kind
        is not M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN
    ):
        raise ValidationError("late return requires its designated outer anchor")
    foreign_counters = tuple(
        name
        for name, value in zip(
            late_work.counter_names(), late_work.counter_values(), strict=True
        )
        if value and name not in _PRETERMINAL_LATE_COUNTERS
    )
    if foreign_counters or late_work.requirement_late_attempt_artifact_count != 1:
        raise ValidationError("preterminal late-return work is not canonical")
    columns = (
        "epoch_id",
        *_WORK_COUNTER_COLUMNS,
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
            anchor.epoch_id,
            *late_work.counter_values(),
            late_work.work_digest,
            anchor.contribution_kind.value,
            anchor.source_id,
            return_artifact_digest,
            anchor.contribution_key_digest,
            anchor.anchor_revision,
        ),
    )


def _sum_work(left: M5RuntimeWork, right: M5RuntimeWork) -> M5RuntimeWork:
    return M5RuntimeWork(
        **{
            name: getattr(left, name) + getattr(right, name)
            for name in _WORK_COUNTER_COLUMNS
        }
    )


def finish_epoch_failure_work_accounting(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    start: EventAccountingStart,
    terminal_work: M5RuntimeWork,
) -> M5RuntimeWork:
    """Point-CAS and freeze the exact work image at the failure cutoff."""

    if not isinstance(terminal_work, M5RuntimeWork):
        raise ValidationError("terminal_work must be an M5RuntimeWork")
    resulting_revision = expected_revision + 1
    work = _sum_work(start.work, terminal_work)
    assignments = tuple(
        sql.SQL("{} = {}").format(sql.Identifier(name), sql.Placeholder())
        for name in _WORK_COUNTER_COLUMNS
    )
    updated = cursor.execute(
        sql.SQL(
            "UPDATE groundloop_m5_runtime_work_accumulator SET {}, "
            "work_digest = %s, updated_revision = %s, terminalized = true, "
            "updated_at = clock_timestamp() "
            "WHERE epoch_id = %s AND updated_revision = %s "
            "AND work_digest = %s AND NOT terminalized"
        ).format(sql.SQL(", ").join(assignments)),
        (
            *work.counter_values(),
            work.work_digest,
            resulting_revision,
            epoch_id,
            expected_revision,
            start.work.work_digest,
        ),
    ).rowcount
    if updated != 1:
        raise ValidationError("M5 work accumulator changed before epoch failure")
    return work


def finish_epoch_failure_timing_accounting(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    start: EventAccountingStart,
    anchor: M5TransitionTimingAnchor,
    attempt_observation: M5RuntimeTimingObservation | None = None,
) -> tuple[M5RuntimeTiming, M5RuntimeTimingCoverage]:
    """Fuse an optional direct attempt and terminal missing point in one CAS."""

    resulting_revision = expected_revision + 1
    if type(start) is not EventAccountingStart:
        raise ValidationError("epoch-failure accounting start has another type")
    if (
        anchor.epoch_id != epoch_id
        or anchor.contribution_kind is not M5RuntimeWorkContributionKind.EPOCH_FAILURE
        or anchor.anchor_revision != resulting_revision
        or not anchor.terminal_transition
    ):
        raise ValidationError("epoch-failure timing anchor is inconsistent")

    if attempt_observation is not None:
        if type(attempt_observation) is not M5RuntimeTimingObservation:
            raise ValidationError("direct attempt timing has another observation type")
        supplied_timing = attempt_observation.timing
        if supplied_timing is not None and type(supplied_timing) is not M5RuntimeTiming:
            raise ValidationError("direct attempt timing has another timing type")
        rebuilt_timing = (
            None
            if supplied_timing is None
            else M5RuntimeTiming(
                coordinator_non_db_non_neural_ns=(
                    supplied_timing.coordinator_non_db_non_neural_ns
                ),
                neural_wall_ns=supplied_timing.neural_wall_ns,
                postgres_roundtrip_wall_ns=supplied_timing.postgres_roundtrip_wall_ns,
                external_io_wall_ns=supplied_timing.external_io_wall_ns,
                end_to_end_wall_ns=supplied_timing.end_to_end_wall_ns,
                postgres_server_execution_ns=(
                    supplied_timing.postgres_server_execution_ns
                ),
                postgres_lock_wait_ns=supplied_timing.postgres_lock_wait_ns,
                postgres_wal_bytes=supplied_timing.postgres_wal_bytes,
                postgres_shared_block_reads=(
                    supplied_timing.postgres_shared_block_reads
                ),
            )
        )
        rebuilt_observation = M5RuntimeTimingObservation(
            required_interval_observed=(attempt_observation.required_interval_observed),
            timing=rebuilt_timing,
            observation_digest=attempt_observation.observation_digest,
        )
        if rebuilt_observation != attempt_observation:
            raise ValidationError("direct attempt timing reconstruction changed")
    else:
        rebuilt_observation = None

    # The failure-only planning path already holds the timing row. Resolve the
    # prior pending point here, after all cooperative lock plans have validated.
    _resolve_pending_anchor(cursor, epoch_id=epoch_id, accumulator=start.timing)
    prior_missing = int(start.timing.has_pending_anchor)
    attempt_count = int(rebuilt_observation is not None)
    attempt_timing = None if rebuilt_observation is None else rebuilt_observation.timing
    required_observed = int(attempt_timing is not None)
    required_missing = attempt_count - required_observed

    def optional(value: int | None) -> tuple[int, int]:
        observed = int(attempt_timing is not None and value is not None)
        return observed, attempt_count - observed

    server_observed, server_missing = optional(
        None if attempt_timing is None else attempt_timing.postgres_server_execution_ns
    )
    lock_observed, lock_missing = optional(
        None if attempt_timing is None else attempt_timing.postgres_lock_wait_ns
    )
    wal_observed, wal_missing = optional(
        None if attempt_timing is None else attempt_timing.postgres_wal_bytes
    )
    blocks_observed, blocks_missing = optional(
        None if attempt_timing is None else attempt_timing.postgres_shared_block_reads
    )
    timing_values = (
        (0,) * len(_TIMING_SUM_COLUMNS)
        if attempt_timing is None
        else tuple(
            0 if value is None else value
            for value in (
                attempt_timing.coordinator_non_db_non_neural_ns,
                attempt_timing.neural_wall_ns,
                attempt_timing.postgres_roundtrip_wall_ns,
                attempt_timing.external_io_wall_ns,
                attempt_timing.end_to_end_wall_ns,
                attempt_timing.postgres_server_execution_ns,
                attempt_timing.postgres_lock_wait_ns,
                attempt_timing.postgres_wal_bytes,
                attempt_timing.postgres_shared_block_reads,
            )
        )
    )
    returned_columns = ", ".join((*_TIMING_SUM_COLUMNS, *_TIMING_COVERAGE_COLUMNS))
    row = cursor.execute(
        f"""
        UPDATE groundloop_m5_runtime_timing_accumulator
        SET coordinator_non_db_non_neural_ns =
                coordinator_non_db_non_neural_ns + %s,
            neural_wall_ns = neural_wall_ns + %s,
            postgres_roundtrip_wall_ns = postgres_roundtrip_wall_ns + %s,
            external_io_wall_ns = external_io_wall_ns + %s,
            end_to_end_wall_ns = end_to_end_wall_ns + %s,
            postgres_server_execution_ns = postgres_server_execution_ns + %s,
            postgres_lock_wait_ns = postgres_lock_wait_ns + %s,
            postgres_wal_bytes = postgres_wal_bytes + %s,
            postgres_shared_block_reads = postgres_shared_block_reads + %s,
            required_expected_count = required_expected_count + %s + 1,
            required_observed_count = required_observed_count + %s,
            required_missing_count = required_missing_count + %s + %s + 1,
            postgres_server_execution_expected_count =
                postgres_server_execution_expected_count + %s + 1,
            postgres_server_execution_observed_count =
                postgres_server_execution_observed_count + %s,
            postgres_server_execution_missing_count =
                postgres_server_execution_missing_count + %s + %s + 1,
            postgres_lock_wait_expected_count =
                postgres_lock_wait_expected_count + %s + 1,
            postgres_lock_wait_observed_count =
                postgres_lock_wait_observed_count + %s,
            postgres_lock_wait_missing_count =
                postgres_lock_wait_missing_count + %s + %s + 1,
            postgres_wal_bytes_expected_count =
                postgres_wal_bytes_expected_count + %s + 1,
            postgres_wal_bytes_observed_count =
                postgres_wal_bytes_observed_count + %s,
            postgres_wal_bytes_missing_count =
                postgres_wal_bytes_missing_count + %s + %s + 1,
            postgres_shared_block_reads_expected_count =
                postgres_shared_block_reads_expected_count + %s + 1,
            postgres_shared_block_reads_observed_count =
                postgres_shared_block_reads_observed_count + %s,
            postgres_shared_block_reads_missing_count =
                postgres_shared_block_reads_missing_count + %s + %s + 1,
            pending_contribution_kind = NULL,
            pending_source_id = NULL,
            pending_contribution_key_digest = NULL,
            pending_anchor_revision = NULL,
            updated_revision = %s,
            terminalized = true,
            updated_at = clock_timestamp()
        WHERE epoch_id = %s AND updated_revision = %s AND NOT terminalized
        RETURNING {returned_columns}, updated_revision, terminalized,
                  pending_contribution_kind, pending_source_id,
                  pending_contribution_key_digest, pending_anchor_revision
        """,
        (
            *timing_values,
            attempt_count,
            required_observed,
            required_missing,
            prior_missing,
            attempt_count,
            server_observed,
            server_missing,
            prior_missing,
            attempt_count,
            lock_observed,
            lock_missing,
            prior_missing,
            attempt_count,
            wal_observed,
            wal_missing,
            prior_missing,
            attempt_count,
            blocks_observed,
            blocks_missing,
            prior_missing,
            resulting_revision,
            epoch_id,
            expected_revision,
        ),
    ).fetchone()
    if row is None:
        raise ValidationError("M5 timing accumulator changed before epoch failure")
    updated = _timing_accumulator_from_row(tuple(row))
    if not updated.terminalized or updated.has_pending_anchor:
        raise ValidationError("terminal M5 timing cutoff did not freeze exactly")
    return updated.project(pending_as_missing=False)


def finish_root_barrier_accounting(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    start: EventAccountingStart,
    anchor: M5TransitionTimingAnchor,
    barrier_work: M5RuntimeWork,
) -> None:
    """CAS barrier work and install its sole pending transition point."""

    resulting_revision = expected_revision + 1
    if (
        anchor.contribution_kind is not M5RuntimeWorkContributionKind.ROOT_BARRIER
        or anchor.anchor_revision != resulting_revision
    ):
        raise ValidationError("root-barrier anchor is inconsistent")
    work = _sum_work(start.work, barrier_work)
    assignments = tuple(
        sql.SQL("{} = {}").format(sql.Identifier(name), sql.Placeholder())
        for name in _WORK_COUNTER_COLUMNS
    )
    work_updated = cursor.execute(
        sql.SQL(
            "UPDATE groundloop_m5_runtime_work_accumulator SET {}, "
            "work_digest = %s, updated_revision = %s, "
            "updated_at = clock_timestamp() "
            "WHERE epoch_id = %s AND updated_revision = %s "
            "AND work_digest = %s AND NOT terminalized"
        ).format(sql.SQL(", ").join(assignments)),
        (
            *work.counter_values(),
            work.work_digest,
            resulting_revision,
            epoch_id,
            expected_revision,
            start.work.work_digest,
        ),
    ).rowcount
    if work_updated != 1:
        raise ValidationError("M5 work accumulator changed before root barrier")
    prior_missing = int(start.timing.has_pending_anchor)
    timing_updated = cursor.execute(
        """
        UPDATE groundloop_m5_runtime_timing_accumulator
        SET required_expected_count = required_expected_count + 1,
            required_missing_count = required_missing_count + %s,
            postgres_server_execution_expected_count =
                postgres_server_execution_expected_count + 1,
            postgres_server_execution_missing_count =
                postgres_server_execution_missing_count + %s,
            postgres_lock_wait_expected_count =
                postgres_lock_wait_expected_count + 1,
            postgres_lock_wait_missing_count =
                postgres_lock_wait_missing_count + %s,
            postgres_wal_bytes_expected_count =
                postgres_wal_bytes_expected_count + 1,
            postgres_wal_bytes_missing_count =
                postgres_wal_bytes_missing_count + %s,
            postgres_shared_block_reads_expected_count =
                postgres_shared_block_reads_expected_count + 1,
            postgres_shared_block_reads_missing_count =
                postgres_shared_block_reads_missing_count + %s,
            pending_contribution_kind = %s, pending_source_id = %s,
            pending_contribution_key_digest = %s, pending_anchor_revision = %s,
            updated_revision = %s, updated_at = clock_timestamp()
        WHERE epoch_id = %s AND updated_revision = %s AND NOT terminalized
        """,
        (
            prior_missing,
            prior_missing,
            prior_missing,
            prior_missing,
            prior_missing,
            anchor.contribution_kind.value,
            anchor.source_id,
            anchor.contribution_key_digest,
            anchor.anchor_revision,
            resulting_revision,
            epoch_id,
            expected_revision,
        ),
    ).rowcount
    if timing_updated != 1:
        raise ValidationError("M5 timing accumulator changed before root barrier")


def finish_cancellation_accounting(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    start: EventAccountingStart,
    anchor: M5TransitionTimingAnchor,
    cancellation_work: M5RuntimeWork,
) -> None:
    """CAS cancellation work and install its sole pending transition point."""

    resulting_revision = expected_revision + 1
    if (
        anchor.contribution_kind is not M5RuntimeWorkContributionKind.CANCELLATION
        or anchor.anchor_revision != resulting_revision
    ):
        raise ValidationError("cancellation anchor is inconsistent")
    work = _sum_work(start.work, cancellation_work)
    assignments = tuple(
        sql.SQL("{} = {}").format(sql.Identifier(name), sql.Placeholder())
        for name in _WORK_COUNTER_COLUMNS
    )
    work_updated = cursor.execute(
        sql.SQL(
            "UPDATE groundloop_m5_runtime_work_accumulator SET {}, "
            "work_digest = %s, updated_revision = %s, "
            "updated_at = clock_timestamp() "
            "WHERE epoch_id = %s AND updated_revision = %s "
            "AND work_digest = %s AND NOT terminalized"
        ).format(sql.SQL(", ").join(assignments)),
        (
            *work.counter_values(),
            work.work_digest,
            resulting_revision,
            epoch_id,
            expected_revision,
            start.work.work_digest,
        ),
    ).rowcount
    if work_updated != 1:
        raise ValidationError("M5 work accumulator changed before cancellation")
    prior_missing = int(start.timing.has_pending_anchor)
    timing_updated = cursor.execute(
        """
        UPDATE groundloop_m5_runtime_timing_accumulator
        SET required_expected_count = required_expected_count + 1,
            required_missing_count = required_missing_count + %s,
            postgres_server_execution_expected_count =
                postgres_server_execution_expected_count + 1,
            postgres_server_execution_missing_count =
                postgres_server_execution_missing_count + %s,
            postgres_lock_wait_expected_count =
                postgres_lock_wait_expected_count + 1,
            postgres_lock_wait_missing_count =
                postgres_lock_wait_missing_count + %s,
            postgres_wal_bytes_expected_count =
                postgres_wal_bytes_expected_count + 1,
            postgres_wal_bytes_missing_count =
                postgres_wal_bytes_missing_count + %s,
            postgres_shared_block_reads_expected_count =
                postgres_shared_block_reads_expected_count + 1,
            postgres_shared_block_reads_missing_count =
                postgres_shared_block_reads_missing_count + %s,
            pending_contribution_kind = %s, pending_source_id = %s,
            pending_contribution_key_digest = %s, pending_anchor_revision = %s,
            updated_revision = %s, updated_at = clock_timestamp()
        WHERE epoch_id = %s AND updated_revision = %s AND NOT terminalized
        """,
        (
            prior_missing,
            prior_missing,
            prior_missing,
            prior_missing,
            prior_missing,
            anchor.contribution_kind.value,
            anchor.source_id,
            anchor.contribution_key_digest,
            anchor.anchor_revision,
            resulting_revision,
            epoch_id,
            expected_revision,
        ),
    ).rowcount
    if timing_updated != 1:
        raise ValidationError("M5 timing accumulator changed before cancellation")


def finish_requirement_execution_accounting(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    start: EventAccountingStart,
    accounting: RequirementExecutionAccounting,
    additional_work: M5RuntimeWork | None = None,
    same_revision: bool = False,
    failure_injector: RecoveryFailureInjector | None = None,
) -> None:
    """CAS both point accumulators after the caller advances one revision."""

    resulting_revision = expected_revision if same_revision else expected_revision + 1
    if accounting.anchor.anchor_revision != resulting_revision:
        raise ValidationError("attempt settlement anchor revision is inconsistent")
    work = _sum_work(start.work, accounting.evidence.attempt_work)
    if additional_work is not None:
        work = _sum_work(work, additional_work)
    assignments = tuple(
        sql.SQL("{} = {}").format(sql.Identifier(name), sql.Placeholder())
        for name in _WORK_COUNTER_COLUMNS
    )
    work_updated = cursor.execute(
        sql.SQL(
            "UPDATE groundloop_m5_runtime_work_accumulator SET {}, "
            "work_digest = %s, updated_revision = %s, "
            "updated_at = clock_timestamp() "
            "WHERE epoch_id = %s AND updated_revision = %s "
            "AND work_digest = %s AND NOT terminalized"
        ).format(sql.SQL(", ").join(assignments)),
        (
            *work.counter_values(),
            work.work_digest,
            resulting_revision,
            epoch_id,
            expected_revision,
            start.work.work_digest,
        ),
    ).rowcount
    if work_updated != 1:
        raise ValidationError("M5 work accumulator changed before settlement")
    _inject_recovery(failure_injector, "requirement_work_accumulator_updated")
    _finish_requirement_execution_timing(
        cursor,
        epoch_id=epoch_id,
        expected_revision=expected_revision,
        start=start,
        accounting=accounting,
        same_revision=same_revision,
    )
    _inject_recovery(failure_injector, "requirement_timing_accumulator_updated")


def _finish_requirement_execution_timing(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    start: EventAccountingStart,
    accounting: RequirementExecutionAccounting,
    same_revision: bool,
) -> None:
    observation = accounting.observation
    raw_timing = _timing_values(observation)
    timing_values = tuple(0 if value is None else value for value in raw_timing)
    required_observed = int(observation.required_interval_observed)
    physical_observed = tuple(int(value is not None) for value in raw_timing[5:])
    prior_missing = int(start.timing.has_pending_anchor)
    anchor = accounting.anchor
    resulting_revision = expected_revision if same_revision else expected_revision + 1
    timing_updated = cursor.execute(
        """
        UPDATE groundloop_m5_runtime_timing_accumulator
        SET coordinator_non_db_non_neural_ns =
                coordinator_non_db_non_neural_ns + %s,
            neural_wall_ns = neural_wall_ns + %s,
            postgres_roundtrip_wall_ns = postgres_roundtrip_wall_ns + %s,
            external_io_wall_ns = external_io_wall_ns + %s,
            end_to_end_wall_ns = end_to_end_wall_ns + %s,
            postgres_server_execution_ns = postgres_server_execution_ns + %s,
            postgres_lock_wait_ns = postgres_lock_wait_ns + %s,
            postgres_wal_bytes = postgres_wal_bytes + %s,
            postgres_shared_block_reads = postgres_shared_block_reads + %s,
            required_expected_count = required_expected_count + 2,
            required_observed_count = required_observed_count + %s,
            required_missing_count = required_missing_count + %s,
            postgres_server_execution_expected_count =
                postgres_server_execution_expected_count + 2,
            postgres_server_execution_observed_count =
                postgres_server_execution_observed_count + %s,
            postgres_server_execution_missing_count =
                postgres_server_execution_missing_count + %s,
            postgres_lock_wait_expected_count =
                postgres_lock_wait_expected_count + 2,
            postgres_lock_wait_observed_count =
                postgres_lock_wait_observed_count + %s,
            postgres_lock_wait_missing_count =
                postgres_lock_wait_missing_count + %s,
            postgres_wal_bytes_expected_count =
                postgres_wal_bytes_expected_count + 2,
            postgres_wal_bytes_observed_count =
                postgres_wal_bytes_observed_count + %s,
            postgres_wal_bytes_missing_count =
                postgres_wal_bytes_missing_count + %s,
            postgres_shared_block_reads_expected_count =
                postgres_shared_block_reads_expected_count + 2,
            postgres_shared_block_reads_observed_count =
                postgres_shared_block_reads_observed_count + %s,
            postgres_shared_block_reads_missing_count =
                postgres_shared_block_reads_missing_count + %s,
            pending_contribution_kind = %s, pending_source_id = %s,
            pending_contribution_key_digest = %s, pending_anchor_revision = %s,
            updated_revision = %s, updated_at = clock_timestamp()
        WHERE epoch_id = %s AND updated_revision = %s AND NOT terminalized
        """,
        (
            *timing_values,
            required_observed,
            1 - required_observed + prior_missing,
            physical_observed[0],
            1 - physical_observed[0] + prior_missing,
            physical_observed[1],
            1 - physical_observed[1] + prior_missing,
            physical_observed[2],
            1 - physical_observed[2] + prior_missing,
            physical_observed[3],
            1 - physical_observed[3] + prior_missing,
            anchor.contribution_kind.value,
            anchor.source_id,
            anchor.contribution_key_digest,
            anchor.anchor_revision,
            resulting_revision,
            epoch_id,
            expected_revision,
        ),
    ).rowcount
    if timing_updated != 1:
        raise ValidationError("M5 timing accumulator changed before settlement")


__all__ = [
    "AcquisitionClock",
    "RequirementExecutionAccounting",
    "RequirementPostterminalReplay",
    "StoredRequirementAttempt",
    "build_requirement_execution_accounting",
    "append_transition_call_timing_checked",
    "finish_cancellation_accounting",
    "finish_requirement_dispatch_accounting",
    "finish_requirement_execution_accounting",
    "finish_root_barrier_accounting",
    "load_requirement_dispatch",
    "persist_cancellation_contribution",
    "persist_requirement_dispatch",
    "persist_requirement_execution_accounting",
    "persist_postterminal_requirement_accounting",
    "persist_preterminal_late_return_contribution",
    "persist_root_result_stage_contribution",
    "persist_root_barrier_contribution",
    "persist_structural_open_accounting",
    "persist_structural_open_identity",
    "persist_terminal_invocation_telemetry",
    "persist_terminal_job_failure_contribution",
    "read_acquisition_clock",
    "read_current_event_timing",
    "read_latest_requirement_attempt",
    "read_requirement_attempt",
    "read_requirement_execution_replay",
    "read_requirement_postterminal_replay",
    "require_runtime_recovery_bundle",
    "requirement_fallback_required",
    "root_result_is_reserved",
    "start_event_accounting",
    "validate_structural_open_recovery",
]
