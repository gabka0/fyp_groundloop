"""Checked adapter for the frozen M4-v1 direct subgraph.

The typed M5 persistence coordinator owns the surrounding transaction and the
shared epoch/head authority. Cursor methods stage only the preserved M4-v1
subgraph. Successful worker returns use the explicit atomic methods here so
normal versus late classification shares one locked transaction.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from psycopg import Cursor

from groundloop.domain import SemanticObservation
from groundloop.errors import ValidationError
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
    M5DirectAttemptReturnReceipt,
    M5DirectCursorContributionReceipt,
    M5DirectLateReturnDisposition,
    M5DirectLateReturnReceipt,
    M5DirectNormalReturnReceipt,
    M5ExecutionEvidenceDisposition,
    M5RuntimeTiming,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TransitionTimingAnchor,
    M5TypedDirectJobLease,
    M5TypedDirectLateReturnEnvelope,
    M5TypedDirectReturnKind,
)
from groundloop.m5.runtime.postgres_direct_recovery import (
    PostgresM5DirectRecoveryStore,
    _DirectReturnSettlement,
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
        """Stage terminal M4 failure plus its typed projection and evidence."""

        self._require_cursor(cursor)
        self._require_no_pending_cache()
        return self._recovery.mark_direct_terminal_failure(
            cursor,
            epoch_id,
            expected_revision,
            lease,
            terminal_reason,
            error_hash,
            attempt_work,
            attempt_timing,
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
