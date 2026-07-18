"""M2 semantic-epoch coordinator oracle (frozen decision D-20).

This state machine is intentionally separate from the M1 repository revision
counter and the grounding-state oracles. A structurally committed corpus event
owns one stable epoch while semantic jobs complete in microtransactions.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, replace
from enum import StrEnum

from groundloop.domain import EvaluationState
from groundloop.errors import EventConflictError, ValidationError


class SemanticEpochStatus(StrEnum):
    PENDING = "pending"
    COMPLETE = "complete"
    DEGRADED = "degraded"
    FAILED = "failed"
    SEALED = "sealed"


class PublicationMode(StrEnum):
    STRICT = "strict"
    PROVISIONAL = "provisional"


@dataclass(frozen=True, slots=True)
class SemanticEpoch:
    epoch_id: int
    event_id: str
    payload_hash: str
    revision: int
    semantic_status: SemanticEpochStatus
    evaluation_state: EvaluationState
    publication_mode: PublicationMode
    required_job_ids: frozenset[str]
    completed_job_ids: frozenset[str]

    @property
    def pending_job_ids(self) -> frozenset[str]:
        return self.required_job_ids - self.completed_job_ids


@dataclass(frozen=True, slots=True)
class PublicationSnapshot:
    visible_epoch_id: int | None
    evaluation_state: EvaluationState
    confirmed_as_of_epoch: int | None


@dataclass(slots=True)
class EpochCoordinator:
    """Single-writer state machine and independent publication oracle."""

    _next_epoch_id: int = 1
    _epochs: dict[int, SemanticEpoch] = field(default_factory=dict)
    _epoch_by_event: dict[str, int] = field(default_factory=dict)
    _active_epoch_id: int | None = None
    _latest_epoch_id: int | None = None
    _last_sealed_epoch_id: int | None = None

    @staticmethod
    def payload_hash(payload: str) -> str:
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def open_epoch(
        self,
        *,
        event_id: str,
        payload_hash: str,
        required_job_ids: frozenset[str],
        publication_mode: PublicationMode = PublicationMode.PROVISIONAL,
    ) -> SemanticEpoch:
        """Commit a validated structural event and create its stable epoch.

        Callers validate the structural payload before this method. Thus a
        rejected structural event consumes neither an epoch nor an event ID.
        """
        recorded_epoch_id = self._epoch_by_event.get(event_id)
        if recorded_epoch_id is not None:
            recorded = self._epochs[recorded_epoch_id]
            if (
                recorded.payload_hash != payload_hash
                or recorded.required_job_ids != required_job_ids
                or recorded.publication_mode is not publication_mode
            ):
                raise EventConflictError(
                    f"event {event_id} already owns epoch {recorded_epoch_id} "
                    "with a different payload or execution declaration"
                )
            return recorded
        if self._active_epoch_id is not None:
            raise ValidationError(
                f"epoch {self._active_epoch_id} is not sealed; "
                "single-writer mode rejects a second open epoch"
            )
        if not event_id or not payload_hash:
            raise ValidationError("event_id and payload_hash must be nonempty")

        epoch_id = self._next_epoch_id
        status = (
            SemanticEpochStatus.PENDING
            if required_job_ids
            else SemanticEpochStatus.COMPLETE
        )
        epoch = SemanticEpoch(
            epoch_id=epoch_id,
            event_id=event_id,
            payload_hash=payload_hash,
            revision=0,
            semantic_status=status,
            evaluation_state=(
                EvaluationState.PENDING
                if required_job_ids
                else EvaluationState.COMPLETE
            ),
            publication_mode=publication_mode,
            required_job_ids=required_job_ids,
            completed_job_ids=frozenset(),
        )
        self._epochs[epoch_id] = epoch
        self._epoch_by_event[event_id] = epoch_id
        self._active_epoch_id = epoch_id
        self._latest_epoch_id = epoch_id
        self._next_epoch_id += 1
        return epoch

    def complete_job(self, epoch_id: int, job_id: str) -> SemanticEpoch:
        """Commit one idempotent semantic completion without changing EpochId."""
        epoch = self.epoch(epoch_id)
        if epoch.semantic_status not in (
            SemanticEpochStatus.PENDING,
            SemanticEpochStatus.COMPLETE,
        ):
            raise ValidationError(
                f"epoch {epoch_id} cannot accept jobs in {epoch.semantic_status}"
            )
        if job_id not in epoch.required_job_ids:
            raise ValidationError(f"job {job_id} is not required by epoch {epoch_id}")
        if job_id in epoch.completed_job_ids:
            return epoch

        completed = epoch.completed_job_ids | {job_id}
        status = (
            SemanticEpochStatus.COMPLETE
            if completed == epoch.required_job_ids
            else SemanticEpochStatus.PENDING
        )
        updated = replace(
            epoch,
            revision=epoch.revision + 1,
            semantic_status=status,
            evaluation_state=(
                EvaluationState.COMPLETE
                if status is SemanticEpochStatus.COMPLETE
                else EvaluationState.PENDING
            ),
            completed_job_ids=completed,
        )
        self._epochs[epoch_id] = updated
        return updated

    def mark_degraded(self, epoch_id: int) -> SemanticEpoch:
        epoch = self.epoch(epoch_id)
        if epoch.semantic_status not in (
            SemanticEpochStatus.PENDING,
            SemanticEpochStatus.COMPLETE,
        ):
            raise ValidationError(
                f"epoch {epoch_id} cannot degrade from {epoch.semantic_status}"
            )
        updated = replace(
            epoch,
            revision=epoch.revision + 1,
            semantic_status=SemanticEpochStatus.DEGRADED,
            evaluation_state=EvaluationState.DEGRADED,
        )
        self._epochs[epoch_id] = updated
        return updated

    def mark_failed(self, epoch_id: int) -> SemanticEpoch:
        epoch = self.epoch(epoch_id)
        if epoch.semantic_status is SemanticEpochStatus.SEALED:
            raise ValidationError(f"sealed epoch {epoch_id} cannot fail")
        updated = replace(
            epoch,
            revision=epoch.revision + 1,
            semantic_status=SemanticEpochStatus.FAILED,
            evaluation_state=EvaluationState.FAILED,
        )
        self._epochs[epoch_id] = updated
        self._active_epoch_id = None
        return updated

    def seal(self, epoch_id: int) -> SemanticEpoch:
        epoch = self.epoch(epoch_id)
        if epoch.semantic_status not in (
            SemanticEpochStatus.COMPLETE,
            SemanticEpochStatus.DEGRADED,
        ):
            raise ValidationError(
                f"epoch {epoch_id} cannot seal from {epoch.semantic_status}"
            )
        updated = replace(
            epoch,
            revision=epoch.revision + 1,
            semantic_status=SemanticEpochStatus.SEALED,
        )
        self._epochs[epoch_id] = updated
        self._active_epoch_id = None
        self._last_sealed_epoch_id = epoch_id
        return updated

    def epoch(self, epoch_id: int) -> SemanticEpoch:
        try:
            return self._epochs[epoch_id]
        except KeyError as exc:
            raise ValidationError(f"epoch {epoch_id} does not exist") from exc

    def publication_snapshot(self) -> PublicationSnapshot:
        """Return strict/provisional visibility without grounding-state data."""
        if self._latest_epoch_id is None:
            return PublicationSnapshot(
                visible_epoch_id=None,
                evaluation_state=EvaluationState.PENDING,
                confirmed_as_of_epoch=None,
            )

        latest = self._epochs[self._latest_epoch_id]
        if latest.semantic_status is SemanticEpochStatus.SEALED:
            return PublicationSnapshot(
                visible_epoch_id=latest.epoch_id,
                evaluation_state=latest.evaluation_state,
                confirmed_as_of_epoch=latest.epoch_id,
            )
        if latest.publication_mode is PublicationMode.STRICT:
            return PublicationSnapshot(
                visible_epoch_id=self._last_sealed_epoch_id,
                evaluation_state=latest.evaluation_state,
                confirmed_as_of_epoch=self._last_sealed_epoch_id,
            )
        return PublicationSnapshot(
            visible_epoch_id=latest.epoch_id,
            evaluation_state=latest.evaluation_state,
            confirmed_as_of_epoch=self._last_sealed_epoch_id,
        )
