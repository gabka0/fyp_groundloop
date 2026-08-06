"""Cursor-local adapter for the frozen M4-v1 direct subgraph.

The typed M5 persistence coordinator owns the surrounding transaction and the
shared epoch/head authority.  This adapter stages only the preserved M4-v1
subgraph using the caller's cursor; it never commits, rolls back, starts a
nested transaction, or invokes an external model.
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
    ) -> JobLease:
        """Persist deterministic attempt/dispatch state before external work."""
        self._require_cursor(cursor)
        self._require_no_pending_cache()
        return self._ports._acquire_direct_job_local(
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
        self._pending_cache = _PendingCacheAdoption(
            _CacheAction.RESET_FAILED, epoch_id
        )

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
        self._pending_cache = _PendingCacheAdoption(
            _CacheAction.ADOPT_SEALED, epoch_id
        )
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
