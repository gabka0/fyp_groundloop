"""Checked adapter for the frozen M4-v1 direct subgraph.

The typed M5 persistence coordinator owns the surrounding transaction and the
shared epoch/head authority. Cursor methods stage only the preserved M4-v1
subgraph. Successful worker returns use the explicit atomic methods here so
normal versus late classification shares one locked transaction.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Any

from psycopg import Cursor

from groundloop.domain import SemanticObservation
from groundloop.errors import EventConflictError, ValidationError
from groundloop.incremental import IncrementalMaintenanceEngine
from groundloop.m4.application import (
    DiscoveryResult,
    DynamicEventPlan,
    JobLease,
    ObservationCompletionReceipt,
    OpenEventReceipt,
    PublicationReceipt,
    StructuralWithdrawal,
)
from groundloop.m4.contracts import (
    CorpusUpdateIdentity,
    DiscoveryScope,
    JobCompletion,
    LogicalJobSpec,
)
from groundloop.m4.pipeline import PostgresM4ApplicationPorts, StructuralPayload
from groundloop.m5.runtime.contracts import (
    M5AcquisitionDisposition,
    M5CheckedDirectTerminalFailureReceipt,
    M5DirectAttemptReturnReceipt,
    M5DirectCursorContributionReceipt,
    M5DirectLateReturnDisposition,
    M5DirectLateReturnReceipt,
    M5DirectNormalReturnReceipt,
    M5EventRunResult,
    M5ExecutionEvidenceDisposition,
    M5RunFailureReason,
    M5RuntimeTiming,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TransitionTimingAnchor,
    M5TypedDirectAcquisitionReceipt,
    M5TypedDirectJobLease,
    M5TypedDirectLateReturnEnvelope,
    M5TypedDirectReturnKind,
)
from groundloop.m5.runtime.persistence import (
    _finalize_typed_epoch_failure_locked,
    _lock_typed_epoch_failure_preconditions,
    _require_typed_epoch_failure_prelock,
)
from groundloop.m5.runtime.postgres_direct_recovery import (
    PostgresM5DirectRecoveryStore,
    _DirectEpochFailureLockedPlan,
    _DirectRetryableFailureOutcome,
    _DirectReturnSettlement,
    _public_m4_lease,
)
from groundloop.m5.runtime.postgres_roots import (
    apply_m5_requirement_epoch_failure,
    lock_m5_requirement_details_for_epoch_failure,
    lock_m5_requirement_jobs_for_epoch_failure,
)
from groundloop.repository import InMemoryRepository


class _CacheAction(StrEnum):
    ADOPT_WORKING = "adopt_working"
    HYDRATE_WORKING = "hydrate_working"
    RESET_FAILED = "reset_failed"
    ADOPT_SEALED = "adopt_sealed"


@dataclass(frozen=True, slots=True)
class _PendingCacheAdoption:
    action: _CacheAction
    epoch_id: int
    repository: InMemoryRepository | None = None
    engine: IncrementalMaintenanceEngine | None = None


@dataclass(frozen=True, slots=True)
class _DirectRetryableFailureOuterReceipt:
    receipt: M5DirectCursorContributionReceipt
    resulting_revision: int
    exact_replay: bool
    transition_anchor: M5TransitionTimingAnchor | None

    def __post_init__(self) -> None:
        receipt = self.receipt
        if (
            type(receipt) is not M5DirectCursorContributionReceipt
            or type(receipt.epoch_id) is not int
            or type(receipt.job_id) is not str
            or type(receipt.attempt_id) is not str
            or type(receipt.execution_evidence_digest) is not str
            or type(receipt.attempt_execution_contribution_key_digest) is not str
            or receipt.direct_transition_source_id is not None
            or receipt.direct_transition_source_identity_hash is not None
            or receipt.direct_transition_contribution_key_digest is not None
            or receipt.observation_completion is not None
            or replace(receipt) != receipt
            or type(self.resulting_revision) is not int
            or self.resulting_revision < 1
            or type(self.exact_replay) is not bool
        ):
            raise ValidationError("retryable outer receipt must be exact")
        anchor = self.transition_anchor
        if (anchor is None) is not self.exact_replay:
            raise ValidationError("retryable replay and anchor presence disagree")
        if anchor is None:
            return
        if type(anchor) is not M5TransitionTimingAnchor or replace(anchor) != anchor:
            raise ValidationError("retryable outer anchor must be exact")
        if (
            type(anchor.epoch_id) is not int
            or type(anchor.contribution_kind) is not M5RuntimeWorkContributionKind
            or type(anchor.source_id) is not str
            or type(anchor.contribution_key_digest) is not str
            or type(anchor.anchor_revision) is not int
            or type(anchor.terminal_transition) is not bool
            or anchor.epoch_id != receipt.epoch_id
            or anchor.contribution_kind
            is not M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION
            or anchor.source_id != receipt.attempt_id
            or anchor.contribution_key_digest
            != receipt.attempt_execution_contribution_key_digest
            or anchor.anchor_revision != self.resulting_revision
        ):
            raise ValidationError("retryable outer anchor changed its receipt")


def _sum_runtime_work(*items: M5RuntimeWork) -> M5RuntimeWork:
    names = M5RuntimeWork.counter_names()
    return M5RuntimeWork(
        **{name: sum(getattr(item, name) for item in items) for name in names}
    )


class PostgresM5DirectM4Adapter:
    """Stage the six M5-D23 direct operations under an outer transaction."""

    def __init__(self, ports: PostgresM4ApplicationPorts) -> None:
        self._ports = ports
        self._recovery = PostgresM5DirectRecoveryStore(ports)
        self._pending_cache: _PendingCacheAdoption | None = None

    def stage_direct_open(
        self,
        cursor: Cursor[Any],
        event: DynamicEventPlan,
        payload: StructuralPayload,
        withdrawal: StructuralWithdrawal,
        roots: tuple[LogicalJobSpec, ...],
        scopes: tuple[DiscoveryScope, ...],
    ) -> OpenEventReceipt:
        """Stage an exact payload-bound M4 declaration and structural overlay."""
        self._require_cursor(cursor)
        self._require_no_pending_cache()
        receipt, repository, engine = self._ports._stage_direct_open_local(
            cursor,
            event,
            payload,
            withdrawal,
            roots,
            scopes,
        )
        if repository is not None and engine is not None:
            self._pending_cache = _PendingCacheAdoption(
                _CacheAction.ADOPT_WORKING,
                receipt.epoch_id,
                repository,
                engine,
            )
        elif receipt.already_failed:
            self._pending_cache = _PendingCacheAdoption(
                _CacheAction.RESET_FAILED, receipt.epoch_id
            )
        elif not receipt.already_sealed:
            self._pending_cache = _PendingCacheAdoption(
                _CacheAction.HYDRATE_WORKING, receipt.epoch_id
            )
        return receipt

    def acquire_direct_job(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        job: LogicalJobSpec,
        lease_token_hash: str,
    ) -> M5TypedDirectJobLease:
        """Persist one total migration-016 direct acquisition outcome."""
        self._require_cursor(cursor)
        self._require_no_pending_cache()
        return self._recovery.acquire_direct_job(
            cursor,
            epoch_id,
            expected_revision,
            job,
            lease_token_hash,
        )

    def acquire_direct_job_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        job: LogicalJobSpec,
    ) -> M5TypedDirectAcquisitionReceipt:
        """Own one tokenless total typed-direct acquisition transaction."""

        self._require_no_pending_cache()
        try:
            with self._ports.connection.transaction():
                with self._ports.connection.cursor() as cursor:
                    receipt = self._recovery.acquire_direct_job_tokenless(
                        cursor,
                        epoch_id,
                        expected_revision,
                        job,
                    )
                    cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
        except Exception:
            self._after_outer_rollback()
            raise
        self._after_outer_commit()
        return receipt

    def mark_direct_retryable_failure_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5TypedDirectJobLease,
        error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> _DirectRetryableFailureOuterReceipt:
        """Own one retryable attempt settlement and its outer timing anchor."""

        self._require_no_pending_cache()
        try:
            with self._ports.connection.transaction():
                with self._ports.connection.cursor() as cursor:
                    outcome = (
                        self._recovery._mark_direct_retryable_failure_with_outcome(
                            cursor,
                            epoch_id,
                            expected_revision,
                            lease,
                            error_hash,
                            attempt_work,
                            attempt_timing,
                        )
                    )
                    if (
                        type(outcome) is not _DirectRetryableFailureOutcome
                        or replace(outcome) != outcome
                    ):
                        raise ValidationError(
                            "retryable failure returned another private outcome"
                        )
                    receipt = outcome.receipt
                    resulting_revision = outcome.resulting_revision
                    anchor = None
                    if not outcome.exact_replay:
                        anchor = M5TransitionTimingAnchor(
                            epoch_id=epoch_id,
                            contribution_kind=(
                                M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION
                            ),
                            source_id=receipt.attempt_id,
                            contribution_key_digest=(
                                receipt.attempt_execution_contribution_key_digest
                            ),
                            anchor_revision=resulting_revision,
                            terminal_transition=False,
                        )
                        self._recovery.install_outer_transition_anchor(cursor, anchor)
                    cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
        except Exception:
            self._after_outer_rollback()
            raise
        self._after_outer_commit()
        return _DirectRetryableFailureOuterReceipt(
            receipt=receipt,
            resulting_revision=resulting_revision,
            exact_replay=outcome.exact_replay,
            transition_anchor=anchor,
        )

    def direct_children_of(
        self, epoch_id: int, root_job_id: str
    ) -> tuple[LogicalJobSpec, ...]:
        """Read the exact durable child set through the preserved M4 port."""

        self._require_no_pending_cache()
        return self._ports.children_of(epoch_id, root_job_id)

    def direct_chunk_is_active(self, chunk_version_id: str) -> bool:
        """Read current M4 chunk activity outside a worker transaction."""

        self._require_no_pending_cache()
        return self._ports.chunk_is_active(chunk_version_id)

    def direct_claim_registry_members(self, snapshot_id: str) -> tuple[str, ...]:
        """Read immutable measured scope members through the M4 owner."""

        self._require_no_pending_cache()
        return self._ports._direct_claim_registry_members(snapshot_id)

    def measure_direct_open_work(
        self, cursor: Cursor[Any], epoch_id: int
    ) -> M5RuntimeWork:
        """Return the byte-total M4 declaration contribution for typed open."""

        self._require_cursor(cursor)
        hashed, serialized = self._ports._measure_typed_direct_open_local(
            cursor, epoch_id
        )
        return M5RuntimeWork(bytes_hashed=hashed, bytes_serialized=serialized)

    def fail_typed_epoch_after_direct_terminal_failure_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        job: LogicalJobSpec,
        lease: M5TypedDirectJobLease,
        direct_terminal_reason: str,
        error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
        requested_failure_reason: M5RunFailureReason,
        open_receipt: OpenEventReceipt,
        call_work: M5RuntimeWork,
    ) -> M5CheckedDirectTerminalFailureReceipt | M5TypedDirectAcquisitionReceipt:
        """Fuse one direct nonretryable attempt with total typed failure."""

        self._validate_checked_terminal_failure_inputs(
            epoch_id=epoch_id,
            expected_revision=expected_revision,
            job=job,
            lease=lease,
            direct_terminal_reason=direct_terminal_reason,
            error_hash=error_hash,
            attempt_work=attempt_work,
            attempt_timing=attempt_timing,
            requested_failure_reason=requested_failure_reason,
            open_receipt=open_receipt,
            call_work=call_work,
        )
        self._require_no_pending_cache()
        direct_plan: _DirectEpochFailureLockedPlan | None = None
        try:
            with self._ports.connection.transaction():
                with self._ports.connection.cursor() as cursor:
                    prelock = _lock_typed_epoch_failure_preconditions(
                        cursor,
                        epoch_id=epoch_id,
                        expected_revision=expected_revision,
                        failure_reason=requested_failure_reason,
                        open_receipt=open_receipt,
                        call_work=call_work,
                        allow_terminal_reason_mismatch_for_direct_loser=True,
                    )
                    _require_typed_epoch_failure_prelock(cursor, prelock)
                    m4_job_locks = (
                        self._ports._lock_typed_direct_epoch_failure_jobs_local(
                            cursor, epoch_id, expected_revision
                        )
                    )
                    _require_typed_epoch_failure_prelock(cursor, prelock)
                    m5_job_locks = lock_m5_requirement_jobs_for_epoch_failure(
                        cursor, prelock.root_scope_locks
                    )
                    _require_typed_epoch_failure_prelock(cursor, prelock)
                    m4_plan = (
                        self._ports._lock_typed_direct_epoch_failure_details_local(
                            cursor, m4_job_locks, _public_m4_lease(lease)
                        )
                    )
                    _require_typed_epoch_failure_prelock(cursor, prelock)
                    m5_plan = lock_m5_requirement_details_for_epoch_failure(
                        cursor, m5_job_locks
                    )
                    _require_typed_epoch_failure_prelock(cursor, prelock)
                    direct_plan = self._recovery.lock_direct_epoch_failure_details(
                        cursor,
                        epoch_id=epoch_id,
                        expected_revision=expected_revision,
                        m4_plan=m4_plan,
                        job=job,
                        lease=lease,
                        direct_terminal_reason=direct_terminal_reason,
                        error_hash=error_hash,
                        attempt_work=attempt_work,
                        attempt_timing=attempt_timing,
                    )
                    _require_typed_epoch_failure_prelock(cursor, prelock)

                    if prelock.terminal_result is not None:
                        if not m5_plan.terminal_replay:
                            raise ValidationError(
                                "terminal direct failure lacks terminal M5 locks"
                            )
                        terminal_failure_reason = prelock.terminal_result.failure_reason
                        if type(terminal_failure_reason) is not M5RunFailureReason:
                            raise ValidationError(
                                "terminal direct failure lacks its exact M5 reason"
                            )
                        self._ports._discard_typed_direct_epoch_failure_plan_local(
                            cursor,
                            m4_plan,
                            terminal_failure_reason.value,
                        )
                        if direct_plan.branch == "loser":
                            loser = direct_plan.terminal_acquisition
                            if loser is None:
                                raise ValidationError(
                                    "direct loser lacks terminal acquisition"
                                )
                            cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
                            result: (
                                M5CheckedDirectTerminalFailureReceipt
                                | M5TypedDirectAcquisitionReceipt
                            ) = loser
                        elif direct_plan.branch == "replay":
                            if (
                                prelock.terminal_result.failure_reason
                                is not requested_failure_reason
                                or direct_plan.direct_failure is None
                            ):
                                raise EventConflictError(
                                    "direct checked replay changed its M4 or M5 "
                                    "failure reason"
                                )
                            result = M5CheckedDirectTerminalFailureReceipt(
                                direct_failure=direct_plan.direct_failure,
                                requested_failure_reason=requested_failure_reason,
                                resulting_revision=prelock.header.revision,
                                terminal_result=prelock.terminal_result,
                            )
                            cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
                        else:
                            raise EventConflictError(
                                "terminal direct failure has a mixed outcome"
                            )
                    else:
                        if m5_plan.terminal_replay or direct_plan.branch != "first":
                            raise EventConflictError(
                                "active direct failure has a mixed replay image"
                            )
                        m4_stage = self._ports._stage_typed_direct_epoch_failure_local(
                            cursor,
                            m4_plan,
                            requested_failure_reason.value,
                            direct_terminal_reason,
                        )
                        _require_typed_epoch_failure_prelock(cursor, prelock)
                        direct_applied = self._recovery.apply_direct_epoch_failure(
                            cursor, plan=direct_plan, m4_stage=m4_stage
                        )
                        _require_typed_epoch_failure_prelock(cursor, prelock)
                        closure = apply_m5_requirement_epoch_failure(cursor, m5_plan)
                        terminal_work = _sum_runtime_work(
                            attempt_work, closure.cancellation_work
                        )
                        terminal_result = _finalize_typed_epoch_failure_locked(
                            cursor,
                            prelock=prelock,
                            closure=closure,
                            terminal_work=terminal_work,
                            attempt_observation=direct_applied.attempt_observation,
                            m4_resulting_revision=m4_stage.resulting_revision,
                        )
                        self._recovery.validate_applied_direct_epoch_failure(
                            cursor, plan=direct_plan, m4_stage=m4_stage
                        )
                        if direct_applied.direct_failure is None:
                            raise ValidationError(
                                "checked direct failure lacks its cursor receipt"
                            )
                        result = M5CheckedDirectTerminalFailureReceipt(
                            direct_failure=direct_applied.direct_failure,
                            requested_failure_reason=requested_failure_reason,
                            resulting_revision=m4_stage.resulting_revision,
                            terminal_result=terminal_result,
                        )
                        self._pending_cache = _PendingCacheAdoption(
                            _CacheAction.RESET_FAILED, epoch_id
                        )
                        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
        except Exception:
            self._after_outer_rollback()
            self._recovery.discard_epoch_failure_authority()
            self._ports._typed_failure_job_lock_authority.clear()
            self._ports._typed_failure_plan_authority.clear()
            raise
        self._after_outer_commit()
        return result

    def fail_typed_epoch_with_open_receipt_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        failure_reason: M5RunFailureReason,
        open_receipt: OpenEventReceipt,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        """Fail a direct-capable epoch while closing every open direct job."""

        self._validate_generic_failure_inputs(
            epoch_id=epoch_id,
            expected_revision=expected_revision,
            failure_reason=failure_reason,
            open_receipt=open_receipt,
            call_work=call_work,
        )
        self._require_no_pending_cache()
        try:
            with self._ports.connection.transaction():
                with self._ports.connection.cursor() as cursor:
                    prelock = _lock_typed_epoch_failure_preconditions(
                        cursor,
                        epoch_id=epoch_id,
                        expected_revision=expected_revision,
                        failure_reason=failure_reason,
                        open_receipt=open_receipt,
                        call_work=call_work,
                    )
                    _require_typed_epoch_failure_prelock(cursor, prelock)
                    m4_job_locks = (
                        self._ports._lock_typed_direct_epoch_failure_jobs_local(
                            cursor, epoch_id, expected_revision
                        )
                    )
                    _require_typed_epoch_failure_prelock(cursor, prelock)
                    m5_job_locks = lock_m5_requirement_jobs_for_epoch_failure(
                        cursor, prelock.root_scope_locks
                    )
                    _require_typed_epoch_failure_prelock(cursor, prelock)
                    m4_plan = (
                        self._ports._lock_typed_direct_epoch_failure_details_local(
                            cursor, m4_job_locks, None
                        )
                    )
                    _require_typed_epoch_failure_prelock(cursor, prelock)
                    m5_plan = lock_m5_requirement_details_for_epoch_failure(
                        cursor, m5_job_locks
                    )
                    _require_typed_epoch_failure_prelock(cursor, prelock)
                    direct_plan = self._recovery.lock_direct_epoch_failure_details(
                        cursor,
                        epoch_id=epoch_id,
                        expected_revision=expected_revision,
                        m4_plan=m4_plan,
                        job=None,
                        lease=None,
                        direct_terminal_reason=None,
                        error_hash=None,
                        attempt_work=None,
                        attempt_timing=None,
                    )
                    _require_typed_epoch_failure_prelock(cursor, prelock)
                    if prelock.terminal_result is not None:
                        if (
                            not m5_plan.terminal_replay
                            or direct_plan.branch != "generic_replay"
                        ):
                            raise EventConflictError(
                                "generic direct failure has a mixed replay image"
                            )
                        terminal_failure_reason = prelock.terminal_result.failure_reason
                        if type(terminal_failure_reason) is not M5RunFailureReason:
                            raise ValidationError(
                                "terminal direct failure lacks its exact M5 reason"
                            )
                        self._ports._discard_typed_direct_epoch_failure_plan_local(
                            cursor,
                            m4_plan,
                            terminal_failure_reason.value,
                        )
                        result = prelock.terminal_result
                    else:
                        if (
                            m5_plan.terminal_replay
                            or direct_plan.branch != "generic_first"
                        ):
                            raise EventConflictError(
                                "generic direct failure has a mixed first image"
                            )
                        m4_stage = self._ports._stage_typed_direct_epoch_failure_local(
                            cursor,
                            m4_plan,
                            failure_reason.value,
                            None,
                        )
                        _require_typed_epoch_failure_prelock(cursor, prelock)
                        direct_applied = self._recovery.apply_direct_epoch_failure(
                            cursor, plan=direct_plan, m4_stage=m4_stage
                        )
                        _require_typed_epoch_failure_prelock(cursor, prelock)
                        closure = apply_m5_requirement_epoch_failure(cursor, m5_plan)
                        result = _finalize_typed_epoch_failure_locked(
                            cursor,
                            prelock=prelock,
                            closure=closure,
                            terminal_work=closure.cancellation_work,
                            attempt_observation=None,
                            m4_resulting_revision=m4_stage.resulting_revision,
                        )
                        if (
                            direct_applied.direct_failure is not None
                            or direct_applied.attempt_observation is not None
                        ):
                            raise ValidationError(
                                "generic direct failure produced attempt evidence"
                            )
                        self._recovery.validate_applied_direct_epoch_failure(
                            cursor, plan=direct_plan, m4_stage=m4_stage
                        )
                        self._pending_cache = _PendingCacheAdoption(
                            _CacheAction.RESET_FAILED, epoch_id
                        )
                    cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
        except Exception:
            self._after_outer_rollback()
            self._recovery.discard_epoch_failure_authority()
            self._ports._typed_failure_job_lock_authority.clear()
            self._ports._typed_failure_plan_authority.clear()
            raise
        self._after_outer_commit()
        return result

    @staticmethod
    def _validate_checked_terminal_failure_inputs(
        *,
        epoch_id: int,
        expected_revision: int,
        job: LogicalJobSpec,
        lease: M5TypedDirectJobLease,
        direct_terminal_reason: str,
        error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
        requested_failure_reason: M5RunFailureReason,
        open_receipt: OpenEventReceipt,
        call_work: M5RuntimeWork,
    ) -> None:
        if type(epoch_id) is not int or epoch_id < 1:
            raise ValidationError("direct failure epoch ID must be positive")
        if type(expected_revision) is not int or expected_revision < 1:
            raise ValidationError("direct failure revision must be positive")
        if type(job) is not LogicalJobSpec or replace(job) != job:
            raise ValidationError("direct failure job must be exact")
        if type(lease) is not M5TypedDirectJobLease or replace(lease) != lease:
            raise ValidationError("direct failure lease must be exact")
        if (
            job.job_id != lease.job_id
            or lease.disposition
            not in {
                M5AcquisitionDisposition.DISPATCH_NEW,
                M5AcquisitionDisposition.DISPATCH_TAKEOVER,
            }
            or not lease.should_execute
            or lease.exact_replay
            or lease.already_completed
            or lease.attempt_id is None
            or lease.lease_token_hash is None
            or lease.lease_expires_at is None
            or lease.dispatch_record_digest is None
        ):
            raise ValidationError(
                "direct failure requires its exact executable acquisition"
            )
        if (
            type(direct_terminal_reason) is not str
            or not direct_terminal_reason.strip()
        ):
            raise ValidationError("direct terminal reason must be exact nonempty text")
        if (
            type(error_hash) is not str
            or len(error_hash) != 64
            or error_hash != error_hash.lower()
            or any(character not in "0123456789abcdef" for character in error_hash)
        ):
            raise ValidationError(
                "direct terminal error hash must be lowercase SHA-256"
            )
        for name, work in (("attempt_work", attempt_work), ("call_work", call_work)):
            if type(work) is not M5RuntimeWork or replace(work) != work:
                raise ValidationError(f"direct failure {name} must be exact")
        if attempt_timing is not None and (
            type(attempt_timing) is not M5RuntimeTiming
            or replace(attempt_timing) != attempt_timing
        ):
            raise ValidationError("direct failure attempt timing must be exact")
        if type(requested_failure_reason) is not M5RunFailureReason:
            raise ValidationError("requested direct failure reason must be exact")
        if requested_failure_reason in {
            M5RunFailureReason.WORK_IN_PROGRESS,
            M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
            M5RunFailureReason.VERIFIER_UNAVAILABLE,
        }:
            raise ValidationError("requested direct failure reason must be terminal")
        if (
            type(open_receipt) is not OpenEventReceipt
            or replace(open_receipt) != open_receipt
            or open_receipt.epoch_id != epoch_id
            or open_receipt.already_sealed
            or open_receipt.publication_id is not None
            or open_receipt.already_failed
            or open_receipt.failure_reason is not None
        ):
            raise ValidationError(
                "direct failure requires the exact held nonterminal receipt"
            )

    @staticmethod
    def _validate_generic_failure_inputs(
        *,
        epoch_id: int,
        expected_revision: int,
        failure_reason: M5RunFailureReason,
        open_receipt: OpenEventReceipt,
        call_work: M5RuntimeWork,
    ) -> None:
        if type(epoch_id) is not int or epoch_id < 1:
            raise ValidationError("direct-aware failure epoch ID must be positive")
        if type(expected_revision) is not int or expected_revision < 1:
            raise ValidationError("direct-aware failure revision must be positive")
        if type(failure_reason) is not M5RunFailureReason or failure_reason in {
            M5RunFailureReason.WORK_IN_PROGRESS,
            M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
            M5RunFailureReason.VERIFIER_UNAVAILABLE,
        }:
            raise ValidationError("direct-aware failure reason must be terminal")
        if type(call_work) is not M5RuntimeWork or replace(call_work) != call_work:
            raise ValidationError("direct-aware failure call work must be exact")
        if (
            type(open_receipt) is not OpenEventReceipt
            or replace(open_receipt) != open_receipt
            or open_receipt.epoch_id != epoch_id
            or open_receipt.already_sealed
            or open_receipt.publication_id is not None
            or open_receipt.already_failed
            or open_receipt.failure_reason is not None
        ):
            raise ValidationError(
                "direct-aware failure requires the held nonterminal receipt"
            )

    def stage_direct_expansion(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        lease: JobLease,
        discovery: DiscoveryResult,
        completion: JobCompletion,
        children: tuple[LogicalJobSpec, ...],
    ) -> None:
        """Stage exact discovery artifacts, children, completion, and counters."""
        self._require_cursor(cursor)
        self._require_no_pending_cache()
        self._ports._stage_direct_expansion_local(
            cursor,
            epoch_id,
            expected_revision,
            lease,
            discovery,
            completion,
            children,
        )

    def settle_direct_expansion_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5TypedDirectJobLease,
        envelope: M5TypedDirectLateReturnEnvelope,
        children: tuple[LogicalJobSpec, ...],
        execution_disposition: M5ExecutionEvidenceDisposition,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> M5DirectAttemptReturnReceipt:
        """Commit one discovery return with one normal-versus-late decision."""

        self._require_no_pending_cache()
        try:
            with self._ports.connection.transaction():
                with self._ports.connection.cursor() as cursor:
                    settlement = self._recovery.settle_direct_expansion_cursor(
                        cursor,
                        epoch_id,
                        expected_revision,
                        lease,
                        envelope,
                        children,
                        execution_disposition,
                        attempt_work,
                        attempt_timing,
                    )
                    anchor: M5TransitionTimingAnchor | None = None
                    if not settlement.exact_replay and settlement.normal is not None:
                        normal_cursor = settlement.normal
                        assert normal_cursor.direct_transition_source_id is not None
                        anchor = M5TransitionTimingAnchor.build(
                            epoch_id=epoch_id,
                            contribution_kind=(
                                M5RuntimeWorkContributionKind.DIRECT_TRANSITION
                            ),
                            source_id=normal_cursor.direct_transition_source_id,
                            anchor_revision=settlement.resulting_revision,
                            terminal_transition=False,
                        )
                    elif (
                        not settlement.exact_replay
                        and settlement.late is not None
                        and settlement.late.disposition
                        in {
                            M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL,
                            M5DirectLateReturnDisposition.TERMINAL_AUDIT_PRETERMINAL,
                        }
                    ):
                        anchor = M5TransitionTimingAnchor.build(
                            epoch_id=epoch_id,
                            contribution_kind=(
                                M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN
                            ),
                            source_id=settlement.late.attempt_id,
                            anchor_revision=settlement.resulting_revision,
                            terminal_transition=False,
                        )
                    if anchor is not None:
                        self._recovery.install_outer_transition_anchor(cursor, anchor)
                    receipt = self._build_direct_return_receipt(
                        epoch_id=epoch_id,
                        return_kind=M5TypedDirectReturnKind.DISCOVERY,
                        envelope=envelope,
                        settlement=settlement,
                        anchor=anchor,
                    )
                    cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
        except Exception:
            self._after_outer_rollback()
            raise
        self._after_outer_commit()
        return receipt

    def settle_direct_verifier_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        lease: M5TypedDirectJobLease,
        envelope: M5TypedDirectLateReturnEnvelope,
        execution_disposition: M5ExecutionEvidenceDisposition,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> M5DirectAttemptReturnReceipt:
        """Commit one verifier return with one normal-versus-late decision."""

        self._require_no_pending_cache()
        try:
            with self._ports.connection.transaction():
                with self._ports.connection.cursor() as cursor:
                    verifier = self._recovery.settle_direct_verifier_cursor(
                        cursor,
                        epoch_id,
                        expected_revision,
                        lease,
                        envelope,
                        execution_disposition,
                        attempt_work,
                        attempt_timing,
                    )
                    if verifier.repository is not None:
                        assert verifier.engine is not None
                        self._pending_cache = _PendingCacheAdoption(
                            _CacheAction.ADOPT_WORKING,
                            epoch_id,
                            verifier.repository,
                            verifier.engine,
                        )
                    settlement = verifier.settlement
                    anchor: M5TransitionTimingAnchor | None = None
                    if not settlement.exact_replay and settlement.normal is not None:
                        normal_cursor = settlement.normal
                        assert normal_cursor.direct_transition_source_id is not None
                        anchor = M5TransitionTimingAnchor.build(
                            epoch_id=epoch_id,
                            contribution_kind=(
                                M5RuntimeWorkContributionKind.DIRECT_TRANSITION
                            ),
                            source_id=normal_cursor.direct_transition_source_id,
                            anchor_revision=settlement.resulting_revision,
                            terminal_transition=False,
                        )
                    elif (
                        not settlement.exact_replay
                        and settlement.late is not None
                        and settlement.late.disposition
                        in {
                            M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL,
                            M5DirectLateReturnDisposition.TERMINAL_AUDIT_PRETERMINAL,
                        }
                    ):
                        anchor = M5TransitionTimingAnchor.build(
                            epoch_id=epoch_id,
                            contribution_kind=(
                                M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN
                            ),
                            source_id=settlement.late.attempt_id,
                            anchor_revision=settlement.resulting_revision,
                            terminal_transition=False,
                        )
                    if anchor is not None:
                        self._recovery.install_outer_transition_anchor(cursor, anchor)
                    receipt = self._build_direct_return_receipt(
                        epoch_id=epoch_id,
                        return_kind=M5TypedDirectReturnKind.VERIFIER,
                        envelope=envelope,
                        settlement=settlement,
                        anchor=anchor,
                    )
                    cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
        except Exception:
            self._after_outer_rollback()
            raise
        self._after_outer_commit()
        return receipt

    @staticmethod
    def _build_direct_return_receipt(
        *,
        epoch_id: int,
        return_kind: M5TypedDirectReturnKind,
        envelope: M5TypedDirectLateReturnEnvelope,
        settlement: _DirectReturnSettlement,
        anchor: M5TransitionTimingAnchor | None,
    ) -> M5DirectAttemptReturnReceipt:
        normal: M5DirectNormalReturnReceipt | None
        late: M5DirectLateReturnReceipt | None
        if settlement.normal is not None:
            normal_cursor = settlement.normal
            assert normal_cursor.direct_transition_source_id is not None
            assert normal_cursor.direct_transition_source_identity_hash is not None
            assert normal_cursor.direct_transition_contribution_key_digest is not None
            normal = M5DirectNormalReturnReceipt(
                epoch_id=epoch_id,
                job_id=normal_cursor.job_id,
                attempt_id=normal_cursor.attempt_id,
                resulting_revision=settlement.resulting_revision,
                exact_replay=settlement.exact_replay,
                execution_evidence_digest=(normal_cursor.execution_evidence_digest),
                return_artifact_digest=envelope.result_artifact_hash,
                direct_transition_source_id=(normal_cursor.direct_transition_source_id),
                direct_transition_source_identity_hash=(
                    normal_cursor.direct_transition_source_identity_hash
                ),
                direct_transition_contribution_key_digest=(
                    normal_cursor.direct_transition_contribution_key_digest
                ),
                observation_completion=normal_cursor.observation_completion,
                current_terminal_logical_result_hash=(
                    settlement.current_terminal_logical_result_hash
                ),
                transition_anchor=anchor,
            )
            late = None
        else:
            assert settlement.late is not None
            late_cursor = settlement.late
            normal = None
            late = M5DirectLateReturnReceipt(
                disposition=late_cursor.disposition,
                epoch_id=epoch_id,
                job_id=late_cursor.job_id,
                attempt_id=late_cursor.attempt_id,
                resulting_revision=settlement.resulting_revision,
                exact_replay=settlement.exact_replay,
                envelope_digest=late_cursor.envelope_digest,
                execution_evidence_digest=late_cursor.execution_evidence_digest,
                expired_return_digest=late_cursor.expired_return_digest,
                current_terminal_logical_result_hash=(
                    settlement.current_terminal_logical_result_hash
                ),
                transition_anchor=anchor,
            )
        return M5DirectAttemptReturnReceipt(
            return_kind=return_kind,
            normal=normal,
            late=late,
        )

    def mark_direct_retryable_failure(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        lease: M5TypedDirectJobLease,
        error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> M5DirectCursorContributionReceipt:
        """Stage retryable M4 failure plus exact D24 point evidence."""

        self._require_cursor(cursor)
        self._require_no_pending_cache()
        return self._recovery.mark_direct_retryable_failure(
            cursor,
            epoch_id,
            expected_revision,
            lease,
            error_hash,
            attempt_work,
            attempt_timing,
        )

    def mark_direct_terminal_failure(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        lease: M5TypedDirectJobLease,
        terminal_reason: str,
        error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
    ) -> M5DirectCursorContributionReceipt:
        """Reject the superseded standalone terminal-attempt settlement."""

        del (
            cursor,
            epoch_id,
            expected_revision,
            lease,
            terminal_reason,
            error_hash,
            attempt_work,
            attempt_timing,
        )
        raise EventConflictError(
            "direct terminal failure requires the checked combined operation"
        )

    def install_outer_transition_anchor(
        self, cursor: Cursor[Any], anchor: M5TransitionTimingAnchor
    ) -> None:
        """Install exactly the anchor chosen by a surrounding typed transaction."""

        self._require_cursor(cursor)
        self._require_no_pending_cache()
        self._recovery.install_outer_transition_anchor(cursor, anchor)

    def stage_direct_verifier_completion(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        lease: JobLease,
        verifier_job: LogicalJobSpec,
        completion: JobCompletion,
        observation: SemanticObservation,
        make_effective: bool,
    ) -> ObservationCompletionReceipt:
        """Stage one direct verifier result and affected working-state patch."""
        self._require_cursor(cursor)
        self._require_no_pending_cache()
        receipt, repository, engine = (
            self._ports._stage_direct_verifier_completion_local(
                cursor,
                epoch_id,
                expected_revision,
                lease,
                verifier_job,
                completion,
                observation,
                make_effective=make_effective,
            )
        )
        if repository is not None and engine is not None:
            self._pending_cache = _PendingCacheAdoption(
                _CacheAction.ADOPT_WORKING,
                epoch_id,
                repository,
                engine,
            )
        return receipt

    def stage_direct_failure(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        reason: str,
    ) -> None:
        """Stage direct failure metadata/evaluation, leaving base state to M5."""
        self._require_cursor(cursor)
        self._require_no_pending_cache()
        self._ports._stage_direct_failure_local(
            cursor, epoch_id, expected_revision, reason
        )
        self._pending_cache = _PendingCacheAdoption(_CacheAction.RESET_FAILED, epoch_id)

    def stage_direct_seal(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        update: CorpusUpdateIdentity,
    ) -> PublicationReceipt:
        """Stage direct publication without advancing a head or sealing base."""
        self._require_cursor(cursor)
        self._require_no_pending_cache()
        receipt = self._ports._stage_direct_seal_local(
            cursor, epoch_id, expected_revision, update
        )
        self._pending_cache = _PendingCacheAdoption(_CacheAction.ADOPT_SEALED, epoch_id)
        return receipt

    def _after_outer_commit(self) -> None:
        """Adopt non-authoritative process caches after durable outer commit."""
        pending = self._pending_cache
        if pending is None:
            return
        self._pending_cache = None
        try:
            if pending.action is _CacheAction.ADOPT_WORKING:
                assert pending.repository is not None and pending.engine is not None
                self._ports._adopt_direct_working_cache(
                    pending.epoch_id, pending.repository, pending.engine
                )
            elif pending.action is _CacheAction.HYDRATE_WORKING:
                self._ports._hydrate_direct_working_cache(pending.epoch_id)
            elif pending.action is _CacheAction.RESET_FAILED:
                self._ports._adopt_direct_failure_cache()
            else:
                self._ports._adopt_direct_seal_cache()
        except Exception:
            self._ports._active_epoch_id = None
            raise

    def _after_outer_rollback(self) -> None:
        """Discard detached cache candidates after rollback."""
        self._pending_cache = None

    def _require_cursor(self, cursor: Cursor[Any]) -> None:
        if cursor.connection is not self._ports.connection:
            raise ValidationError("direct M4 cursor belongs to another connection")

    def _require_no_pending_cache(self) -> None:
        if self._pending_cache is not None:
            raise ValidationError(
                "direct M4 cache outcome must follow the prior outer transaction"
            )


__all__ = ["PostgresM5DirectM4Adapter"]
