"""Atomic differential execution of the M1 oracle and M2 delta engine."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass

from groundloop.domain import StatusDelta
from groundloop.events import Event, apply_event
from groundloop.incremental import IncrementalMaintenanceEngine
from groundloop.reference import compute_all_states
from groundloop.repository import InMemoryRepository

FailureInjector = Callable[[str], None]


class DifferentialMismatchError(AssertionError):
    """The incremental state diverged from the independent reference oracle."""


@dataclass(slots=True)
class DifferentialRunner:
    """Own and atomically advance a reference repository and delta engine."""

    repository: InMemoryRepository
    engine: IncrementalMaintenanceEngine

    @classmethod
    def from_repository(cls, repository: InMemoryRepository) -> DifferentialRunner:
        engine = IncrementalMaintenanceEngine.from_repository(repository)
        runner = cls(repository=repository, engine=engine)
        runner.assert_equivalent()
        return runner

    def assert_equivalent(self) -> None:
        reference_claims, reference_answers = compute_all_states(self.repository)
        if self.engine.claim_states != reference_claims:
            raise DifferentialMismatchError(
                "incremental claim states differ from full recomputation"
            )
        if self.engine.answer_states != reference_answers:
            raise DifferentialMismatchError(
                "incremental answer states differ from full recomputation"
            )
        self.engine.validate_certificates()

    def apply(
        self,
        event: Event,
        failure_injector: FailureInjector | None = None,
    ) -> tuple[StatusDelta, ...]:
        """Stage both paths, compare, then atomically publish the pair."""
        if self.engine.sync_registry(self.repository):
            self.assert_equivalent()
        staged_repository = deepcopy(self.repository)
        staged_engine = deepcopy(self.engine)

        result = apply_event(staged_repository, event)
        if failure_injector is not None:
            failure_injector("reference_applied")
        # Exact event replay is a semantic no-op. ``apply_event`` returns the
        # recorded deltas without advancing the repository epoch; applying the
        # signed delta a second time would corrupt counted state.
        if staged_repository.current_epoch == self.repository.current_epoch:
            return result
        staged_engine.apply_committed_event(event, self.repository, staged_repository)
        if failure_injector is not None:
            failure_injector("incremental_applied")

        reference_claims, reference_answers = compute_all_states(staged_repository)
        if staged_engine.claim_states != reference_claims:
            raise DifferentialMismatchError(
                "incremental claim states differ from full recomputation"
            )
        if staged_engine.answer_states != reference_answers:
            raise DifferentialMismatchError(
                "incremental answer states differ from full recomputation"
            )
        staged_engine.validate_certificates()

        self.repository.replace_with(staged_repository)
        self.engine = staged_engine
        return result
