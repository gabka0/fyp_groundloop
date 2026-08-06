"""Physically incremental evaluation-state counters for measured M4 execution.

The store represents the evaluation surface as one epoch-wide default plus
strictly-positive per-object open-job counters.  It does not enumerate the
claim registry when a global discovery scope opens or closes.  PostgreSQL row
locking serializes transitions for one epoch; a revision compare-and-swap
rejects stale writers, and an immutable transition ledger makes exact replay a
no-op while detecting identifier reuse with different content.

This module owns no schema installation.  The exact migration contract is
recorded in ``docs/workstreams/m4_evaluation_overlay/HANDOFF.md``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from psycopg import Connection, Cursor

from groundloop.errors import (
    DanglingReferenceError,
    EventConflictError,
    InvalidEventError,
    ValidationError,
)
from groundloop.m4.runtime.epoch import EvaluationState

SqlExecutor = Connection[Any] | Cursor[Any]


class EvaluationObjectType(StrEnum):
    """Objects whose provisional visibility is maintained by the overlay."""

    CLAIM = "claim"
    ANSWER = "answer"


class EvaluationLifecycle(StrEnum):
    """Terminal policy for an epoch's evaluation surface."""

    ACTIVE = "active"
    FAILED = "failed"
    SEALED = "sealed"


class EvaluationTransitionKind(StrEnum):
    """Permitted evaluation-overlay transition classes."""

    DELTA = "delta"
    FAIL = "fail"
    SEAL = "seal"


class EvaluationRevisionConflict(InvalidEventError):
    """A transition expected an obsolete epoch revision."""


@dataclass(frozen=True, slots=True)
class ClaimJobDelta:
    """A signed change to a claim's number of open required semantic jobs."""

    claim_id: str
    delta: int

    def __post_init__(self) -> None:
        if not self.claim_id.strip():
            raise ValidationError("claim_id must be non-empty")
        if isinstance(self.delta, bool) or not isinstance(self.delta, int):
            raise ValidationError("claim-job delta must be an integer")
        if self.delta == 0:
            raise ValidationError("claim-job delta must be non-zero")


@dataclass(frozen=True, slots=True)
class EvaluationTransition:
    """One atomic signed-counter transition at an expected epoch revision."""

    transition_id: str
    expected_revision: int
    scope_delta: int = 0
    claim_job_deltas: tuple[ClaimJobDelta, ...] = ()
    kind: EvaluationTransitionKind = EvaluationTransitionKind.DELTA

    def __post_init__(self) -> None:
        if not self.transition_id.strip():
            raise ValidationError("transition_id must be non-empty")
        if (
            isinstance(self.expected_revision, bool)
            or not isinstance(self.expected_revision, int)
            or self.expected_revision < 0
        ):
            raise ValidationError("expected_revision must be a non-negative integer")
        if isinstance(self.scope_delta, bool) or not isinstance(self.scope_delta, int):
            raise ValidationError("scope_delta must be an integer")
        if not isinstance(self.kind, EvaluationTransitionKind):
            raise ValidationError("kind must be an EvaluationTransitionKind")
        if self.kind is not EvaluationTransitionKind.DELTA and (
            self.scope_delta != 0 or self.claim_job_deltas
        ):
            raise ValidationError(
                "fail and seal transitions cannot also mutate evaluation counters"
            )


@dataclass(frozen=True, slots=True)
class EpochEvaluationDefault:
    """The persisted default and exact global discovery-scope counter."""

    epoch_id: int
    lifecycle: EvaluationLifecycle
    default_state: EvaluationState
    confirmed_as_of_epoch: int | None
    open_discovery_scope_count: int
    revision: int


@dataclass(frozen=True, slots=True)
class EffectiveEvaluation:
    """Effective point result after applying an optional positive override."""

    epoch_id: int
    object_type: EvaluationObjectType
    object_id: str
    state: EvaluationState
    confirmed_as_of_epoch: int | None
    open_required_job_count: int
    open_discovery_scope_count: int
    inherited_default: bool
    revision: int


@dataclass(frozen=True, slots=True)
class DeclarationReceipt:
    """Result of declaring or exactly replaying an epoch default."""

    default: EpochEvaluationDefault
    replayed: bool


@dataclass(frozen=True, slots=True)
class TransitionReceipt:
    """Durable transition result and physical override-write accounting."""

    epoch_id: int
    transition_id: str
    from_revision: int
    to_revision: int
    override_rows_written: int
    replayed: bool


@dataclass(frozen=True, slots=True)
class _EpochRow:
    epoch_id: int
    declaration_hash: str
    lifecycle: EvaluationLifecycle
    default_state: EvaluationState
    confirmed_as_of_epoch: int | None
    open_discovery_scope_count: int
    revision: int


@dataclass(frozen=True, slots=True)
class _ClaimBinding:
    answer_version_id: str
    required: bool


_EFFECTIVE_POINT_SQL = """
    SELECT counter.lifecycle_state, counter.default_evaluation_state,
           counter.confirmed_as_of_epoch,
           counter.open_discovery_scope_count, counter.revision,
           override_row.open_required_job_count
    FROM groundloop_m4_evaluation_epoch_counter AS counter
    LEFT JOIN groundloop_m4_evaluation_override_counter AS override_row
      ON override_row.epoch_id = counter.epoch_id
     AND override_row.object_type = %s
     AND override_row.object_id = %s
    WHERE counter.epoch_id = %s
"""


def _sha256_json(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_claim_deltas(
    deltas: tuple[ClaimJobDelta, ...],
) -> tuple[ClaimJobDelta, ...]:
    totals: dict[str, int] = {}
    for item in deltas:
        totals[item.claim_id] = totals.get(item.claim_id, 0) + item.delta
    return tuple(
        ClaimJobDelta(claim_id, delta)
        for claim_id, delta in sorted(totals.items())
        if delta != 0
    )


def _transition_payload(
    transition: EvaluationTransition,
    canonical_deltas: tuple[ClaimJobDelta, ...],
) -> dict[str, object]:
    return {
        "kind": transition.kind.value,
        "expected_revision": transition.expected_revision,
        "scope_delta": transition.scope_delta,
        "claim_job_deltas": [
            {"claim_id": item.claim_id, "delta": item.delta}
            for item in canonical_deltas
        ],
    }


def _declaration_hash(
    epoch_id: int,
    revision: int,
    confirmed_as_of_epoch: int | None,
    open_discovery_scope_count: int,
) -> str:
    return _sha256_json(
        {
            "epoch_id": epoch_id,
            "revision": revision,
            "confirmed_as_of_epoch": confirmed_as_of_epoch,
            "open_discovery_scope_count": open_discovery_scope_count,
        }
    )


class PostgresEvaluationOverlayStore:
    """Maintain exact M4 evaluation counters without registry-wide rewrites."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def declare_epoch(
        self,
        epoch_id: int,
        *,
        revision: int,
        confirmed_as_of_epoch: int | None,
        open_discovery_scope_count: int,
    ) -> DeclarationReceipt:
        """Insert one default row; exact declaration replay is a no-op."""
        with self._connection.transaction():
            return self.declare_epoch_local(
                self._connection,
                epoch_id,
                revision=revision,
                confirmed_as_of_epoch=confirmed_as_of_epoch,
                open_discovery_scope_count=open_discovery_scope_count,
            )

    def declare_epoch_local(
        self,
        cursor: SqlExecutor,
        epoch_id: int,
        *,
        revision: int,
        confirmed_as_of_epoch: int | None,
        open_discovery_scope_count: int,
    ) -> DeclarationReceipt:
        """Declare an evaluation epoch in the caller's current transaction."""
        self._validate_non_negative("epoch_id", epoch_id)
        self._validate_non_negative("revision", revision)
        self._validate_optional_non_negative(
            "confirmed_as_of_epoch", confirmed_as_of_epoch
        )
        self._validate_non_negative(
            "open_discovery_scope_count", open_discovery_scope_count
        )
        declaration_hash = _declaration_hash(
            epoch_id,
            revision,
            confirmed_as_of_epoch,
            open_discovery_scope_count,
        )
        default_state = (
            EvaluationState.PENDING
            if open_discovery_scope_count > 0
            else EvaluationState.COMPLETE
        )
        inserted = cursor.execute(
            """
            INSERT INTO groundloop_m4_evaluation_epoch_counter (
                epoch_id, declaration_hash, lifecycle_state,
                default_evaluation_state, confirmed_as_of_epoch,
                open_discovery_scope_count, revision
            ) VALUES (%s, %s, 'active', %s, %s, %s, %s)
            ON CONFLICT (epoch_id) DO NOTHING
            """,
            (
                epoch_id,
                declaration_hash,
                default_state.value,
                confirmed_as_of_epoch,
                open_discovery_scope_count,
                revision,
            ),
        ).rowcount
        row = self._read_epoch_row(epoch_id, for_update=True, cursor=cursor)
        if row.declaration_hash != declaration_hash:
            raise EventConflictError(
                "epoch evaluation declaration was replayed with different content"
            )
        return DeclarationReceipt(self._public_default(row), inserted == 0)

    def apply_transition(
        self, epoch_id: int, transition: EvaluationTransition
    ) -> TransitionReceipt:
        """Apply one exact-replay-safe signed transition atomically.

        The local implementation retains the per-key ``_apply_override_delta``
        path protected by the measured-kernel complexity contract.
        """
        with self._connection.transaction():
            return self.apply_transition_local(
                self._connection, epoch_id, transition
            )

    def apply_transition_local(
        self,
        cursor: SqlExecutor,
        epoch_id: int,
        transition: EvaluationTransition,
    ) -> TransitionReceipt:
        """Apply one transition in the caller's current transaction."""
        self._validate_non_negative("epoch_id", epoch_id)
        canonical_deltas = _canonical_claim_deltas(transition.claim_job_deltas)
        payload_hash = _sha256_json(
            _transition_payload(transition, canonical_deltas)
        )
        epoch = self._read_epoch_row(
            epoch_id, for_update=True, cursor=cursor
        )
        replay = self._read_transition(
            epoch_id, transition.transition_id, cursor=cursor
        )
        if replay is not None:
            stored_hash, from_revision, to_revision, override_writes = replay
            if stored_hash != payload_hash:
                raise EventConflictError(
                    "evaluation transition ID was reused with different content"
                )
            return TransitionReceipt(
                epoch_id,
                transition.transition_id,
                from_revision,
                to_revision,
                override_writes,
                True,
            )
        if epoch.lifecycle is not EvaluationLifecycle.ACTIVE:
            raise InvalidEventError("evaluation transitions require an active epoch")
        if epoch.revision != transition.expected_revision:
            raise EvaluationRevisionConflict(
                "evaluation transition expected revision "
                f"{transition.expected_revision}, found {epoch.revision}"
            )
        next_revision = epoch.revision + 1
        override_writes = 0
        next_scope_count = epoch.open_discovery_scope_count
        next_lifecycle: EvaluationLifecycle = epoch.lifecycle
        next_confirmed = epoch.confirmed_as_of_epoch
        next_default = epoch.default_state

        if transition.kind is EvaluationTransitionKind.DELTA:
            next_scope_count += transition.scope_delta
            if next_scope_count < 0:
                raise ValidationError(
                    "open discovery-scope count cannot become negative"
                )
            bindings = self._claim_bindings(
                epoch_id, canonical_deltas, cursor=cursor
            )
            answer_deltas: dict[str, int] = {}
            for item in canonical_deltas:
                override_writes += self._apply_override_delta(
                    epoch_id,
                    EvaluationObjectType.CLAIM,
                    item.claim_id,
                    item.delta,
                    next_revision,
                    cursor=cursor,
                )
                binding = bindings[item.claim_id]
                if binding.required:
                    answer_deltas[binding.answer_version_id] = (
                        answer_deltas.get(binding.answer_version_id, 0) + item.delta
                    )
            for answer_id, delta in sorted(answer_deltas.items()):
                if delta == 0:
                    continue
                override_writes += self._apply_override_delta(
                    epoch_id,
                    EvaluationObjectType.ANSWER,
                    answer_id,
                    delta,
                    next_revision,
                    cursor=cursor,
                )
            next_default = (
                EvaluationState.PENDING
                if next_scope_count > 0
                else EvaluationState.COMPLETE
            )
        elif transition.kind is EvaluationTransitionKind.FAIL:
            next_lifecycle = EvaluationLifecycle.FAILED
            next_default = EvaluationState.FAILED
        else:
            self._assert_sealable(
                epoch_id, epoch.open_discovery_scope_count, cursor=cursor
            )
            next_lifecycle = EvaluationLifecycle.SEALED
            next_default = EvaluationState.COMPLETE
            next_confirmed = epoch_id

        updated = cursor.execute(
            """
            UPDATE groundloop_m4_evaluation_epoch_counter
            SET lifecycle_state = %s,
                default_evaluation_state = %s,
                confirmed_as_of_epoch = %s,
                open_discovery_scope_count = %s,
                revision = %s
            WHERE epoch_id = %s AND revision = %s
              AND lifecycle_state = 'active'
            """,
            (
                next_lifecycle.value,
                next_default.value,
                next_confirmed,
                next_scope_count,
                next_revision,
                epoch_id,
                transition.expected_revision,
            ),
        ).rowcount
        if updated != 1:
            raise EvaluationRevisionConflict(
                "evaluation revision compare-and-swap failed"
            )
        cursor.execute(
            """
            INSERT INTO groundloop_m4_evaluation_counter_transition (
                epoch_id, transition_id, payload_hash, transition_kind,
                from_revision, to_revision, override_rows_written
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                epoch_id,
                transition.transition_id,
                payload_hash,
                transition.kind.value,
                transition.expected_revision,
                next_revision,
                override_writes,
            ),
        )
        return TransitionReceipt(
            epoch_id,
            transition.transition_id,
            transition.expected_revision,
            next_revision,
            override_writes,
            False,
        )

    def read_default(self, epoch_id: int) -> EpochEvaluationDefault:
        """Return the exact epoch-wide default and scope count."""
        self._validate_non_negative("epoch_id", epoch_id)
        return self._public_default(self._read_epoch_row(epoch_id, for_update=False))

    def read_effective(
        self,
        epoch_id: int,
        object_type: EvaluationObjectType,
        object_id: str,
    ) -> EffectiveEvaluation:
        """Resolve one object through the primary-key-indexed override lookup."""
        self._validate_non_negative("epoch_id", epoch_id)
        if not isinstance(object_type, EvaluationObjectType):
            raise ValidationError("object_type must be an EvaluationObjectType")
        if not object_id.strip():
            raise ValidationError("object_id must be non-empty")
        row = self._connection.execute(
            _EFFECTIVE_POINT_SQL,
            (object_type.value, object_id, epoch_id),
        ).fetchone()
        if row is None:
            raise DanglingReferenceError("evaluation epoch does not exist")
        self._assert_registered_object(epoch_id, object_type, object_id)
        lifecycle = EvaluationLifecycle(str(row[0]))
        default_state = EvaluationState(str(row[1]))
        confirmed = None if row[2] is None else int(row[2])
        open_scope_count = int(row[3])
        revision = int(row[4])
        override_count = None if row[5] is None else int(row[5])
        state = default_state
        if lifecycle is EvaluationLifecycle.ACTIVE and open_scope_count == 0:
            state = (
                EvaluationState.PENDING
                if override_count is not None
                else default_state
            )
        return EffectiveEvaluation(
            epoch_id=epoch_id,
            object_type=object_type,
            object_id=object_id,
            state=state,
            confirmed_as_of_epoch=confirmed,
            open_required_job_count=override_count or 0,
            open_discovery_scope_count=open_scope_count,
            inherited_default=override_count is None,
            revision=revision,
        )

    def _assert_registered_object(
        self,
        epoch_id: int,
        object_type: EvaluationObjectType,
        object_id: str,
    ) -> None:
        if object_type is EvaluationObjectType.CLAIM:
            predicate = "claim.claim_id = %s"
        else:
            predicate = "claim.answer_version_id = %s"
        row = self._connection.execute(
            """
            SELECT 1
            FROM groundloop_m4_update AS update_row
            JOIN groundloop_m4_claim_registry_member AS member
              ON member.claim_registry_snapshot_id = update_row.registry_snapshot_id
            JOIN groundloop_claim AS claim USING (claim_id)
            WHERE update_row.epoch_id = %s AND
            """
            + predicate
            + " LIMIT 1",
            (epoch_id, object_id),
        ).fetchone()
        if row is None:
            raise DanglingReferenceError(
                "evaluation object is not in the epoch registry snapshot"
            )

    def _read_epoch_row(
        self,
        epoch_id: int,
        *,
        for_update: bool,
        cursor: SqlExecutor | None = None,
    ) -> _EpochRow:
        suffix = " FOR UPDATE" if for_update else ""
        executor = self._connection if cursor is None else cursor
        row = executor.execute(
            """
            SELECT epoch_id, declaration_hash, lifecycle_state,
                   default_evaluation_state, confirmed_as_of_epoch,
                   open_discovery_scope_count, revision
            FROM groundloop_m4_evaluation_epoch_counter
            WHERE epoch_id = %s
            """
            + suffix,
            (epoch_id,),
        ).fetchone()
        if row is None:
            raise DanglingReferenceError("evaluation epoch does not exist")
        return _EpochRow(
            epoch_id=int(row[0]),
            declaration_hash=str(row[1]),
            lifecycle=EvaluationLifecycle(str(row[2])),
            default_state=EvaluationState(str(row[3])),
            confirmed_as_of_epoch=None if row[4] is None else int(row[4]),
            open_discovery_scope_count=int(row[5]),
            revision=int(row[6]),
        )

    def _read_transition(
        self,
        epoch_id: int,
        transition_id: str,
        *,
        cursor: SqlExecutor | None = None,
    ) -> tuple[str, int, int, int] | None:
        executor = self._connection if cursor is None else cursor
        row = executor.execute(
            """
            SELECT payload_hash, from_revision, to_revision,
                   override_rows_written
            FROM groundloop_m4_evaluation_counter_transition
            WHERE epoch_id = %s AND transition_id = %s
            """,
            (epoch_id, transition_id),
        ).fetchone()
        if row is None:
            return None
        return str(row[0]), int(row[1]), int(row[2]), int(row[3])

    def _claim_bindings(
        self,
        epoch_id: int,
        deltas: tuple[ClaimJobDelta, ...],
        *,
        cursor: SqlExecutor | None = None,
    ) -> dict[str, _ClaimBinding]:
        if not deltas:
            return {}
        claim_ids = [item.claim_id for item in deltas]
        executor = self._connection if cursor is None else cursor
        rows = executor.execute(
            """
            SELECT claim.claim_id, claim.answer_version_id, claim.required
            FROM groundloop_m4_update AS update_row
            JOIN groundloop_m4_claim_registry_member AS member
              ON member.claim_registry_snapshot_id = update_row.registry_snapshot_id
            JOIN groundloop_claim AS claim USING (claim_id)
            WHERE update_row.epoch_id = %s AND claim.claim_id = ANY(%s)
            """,
            (epoch_id, claim_ids),
        ).fetchall()
        bindings = {
            str(row[0]): _ClaimBinding(str(row[1]), bool(row[2])) for row in rows
        }
        missing = sorted(set(claim_ids) - bindings.keys())
        if missing:
            raise DanglingReferenceError(
                "claim-job delta references missing claims: " + ", ".join(missing)
            )
        return bindings

    def _apply_override_delta(
        self,
        epoch_id: int,
        object_type: EvaluationObjectType,
        object_id: str,
        delta: int,
        revision: int,
        *,
        cursor: SqlExecutor | None = None,
    ) -> int:
        executor = self._connection if cursor is None else cursor
        row = executor.execute(
            """
            SELECT open_required_job_count
            FROM groundloop_m4_evaluation_override_counter
            WHERE epoch_id = %s AND object_type = %s AND object_id = %s
            FOR UPDATE
            """,
            (epoch_id, object_type.value, object_id),
        ).fetchone()
        old_count = 0 if row is None else int(row[0])
        new_count = old_count + delta
        if new_count < 0:
            raise ValidationError(
                f"open-job count cannot become negative for {object_type.value} "
                f"{object_id}"
            )
        if new_count == 0:
            if row is None:
                return 0
            executor.execute(
                """
                DELETE FROM groundloop_m4_evaluation_override_counter
                WHERE epoch_id = %s AND object_type = %s AND object_id = %s
                """,
                (epoch_id, object_type.value, object_id),
            )
            return 1
        if row is None:
            executor.execute(
                """
                INSERT INTO groundloop_m4_evaluation_override_counter (
                    epoch_id, object_type, object_id,
                    open_required_job_count, counter_updated_revision
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    epoch_id,
                    object_type.value,
                    object_id,
                    new_count,
                    revision,
                ),
            )
        else:
            executor.execute(
                """
                UPDATE groundloop_m4_evaluation_override_counter
                SET open_required_job_count = %s,
                    counter_updated_revision = %s
                WHERE epoch_id = %s AND object_type = %s AND object_id = %s
                """,
                (
                    new_count,
                    revision,
                    epoch_id,
                    object_type.value,
                    object_id,
                ),
            )
        return 1

    def _assert_sealable(
        self,
        epoch_id: int,
        open_scope_count: int,
        *,
        cursor: SqlExecutor | None = None,
    ) -> None:
        if open_scope_count != 0:
            raise ValidationError("cannot seal with an open discovery scope")
        executor = self._connection if cursor is None else cursor
        open_override = executor.execute(
            """
            SELECT 1 FROM groundloop_m4_evaluation_override_counter
            WHERE epoch_id = %s LIMIT 1
            """,
            (epoch_id,),
        ).fetchone()
        if open_override is not None:
            raise ValidationError("cannot seal with an open required job")

    @staticmethod
    def _public_default(row: _EpochRow) -> EpochEvaluationDefault:
        return EpochEvaluationDefault(
            epoch_id=row.epoch_id,
            lifecycle=row.lifecycle,
            default_state=row.default_state,
            confirmed_as_of_epoch=row.confirmed_as_of_epoch,
            open_discovery_scope_count=row.open_discovery_scope_count,
            revision=row.revision,
        )

    @staticmethod
    def _validate_non_negative(name: str, value: int) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValidationError(f"{name} must be a non-negative integer")

    @classmethod
    def _validate_optional_non_negative(cls, name: str, value: int | None) -> None:
        if value is not None:
            cls._validate_non_negative(name, value)
