"""Pure falsifiers for the canonical D34 global archive protocol."""

from __future__ import annotations

import hashlib
from copy import copy
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest
from psycopg import IsolationLevel
from psycopg.pq import TransactionStatus

from groundloop.errors import EventConflictError, ValidationError
from groundloop.m5.runtime import postgres_matching as matching
from groundloop.m5.runtime.postgres_roots import _EpochHeader
from groundloop.m5.runtime.postgres_verifier import (
    _d34_issue_owner,
    _d34_require_idle,
    _d34_validate_owner,
)


def archive_coordinates() -> tuple[matching._d34_ArchiveCoordinate, ...]:
    hashes = tuple(hashlib.sha256(str(n).encode()).hexdigest() for n in range(11))
    return matching._d34_archive_coordinates(
        result=(hashes[8], hashes[0], hashes[1], hashes[2]),
        pair=(hashes[3], hashes[4], hashes[5], "policy"),
        verifier=(hashes[9], hashes[6]),
        observation=(
            hashes[10],
            "requirement",
            "requirement-id",
            "chunk-id",
            "verify_requirement_v1",
        ),
        execution=(hashes[10], hashes[9], hashes[7], hashes[2], hashes[3]),
    )


class PointCursor:
    def __init__(self, occupied: int | None = None, *, held: bool = True) -> None:
        self.statements: list[tuple[str, tuple[Any, ...]]] = []
        self.occupied = occupied
        self.held = held
        self.probes = 0
        self.rows: list[tuple[Any, ...]] = []

    def execute(self, query: Any, params: tuple[Any, ...]) -> PointCursor:
        statement = query if isinstance(query, str) else query.as_string()
        self.statements.append((statement, params))
        self.rows = []
        if "pg_catalog.pg_locks" in statement:
            self.rows = [(1,)] if self.held else []
        elif statement.startswith("SELECT") and "pg_advisory" not in statement:
            if self.probes == self.occupied:
                self.rows = [("foreign-row",)]
            self.probes += 1
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


def test_all_fifteen_aliases_use_native_framing_and_namespace() -> None:
    coordinates = archive_coordinates()
    assert len(coordinates) == 15
    assert [
        sum(c.relation == name for c in coordinates)
        for name in dict.fromkeys(c.relation for c in coordinates)
    ] == [4, 3, 2, 2, 4]
    assert matching._d34_ARCHIVE_NAMESPACE not in {
        matching._MATCHING_ARTIFACT_LOCK_NAMESPACE,
        matching._MATCHING_ABSENCE_LOCK_NAMESPACE,
        1_146_242_387,
        1_146_242_378,
    }
    for coordinate in coordinates:
        assert coordinate.physical_id == matching._matching_absence_lock_key(
            coordinate.relation, (",".join(coordinate.columns), *coordinate.values)
        )
        assert (
            coordinate.preimage
            == b"m5-d28-matching-absence-v1"
            + matching._matching_journal_key_preimage(
                coordinate.relation, (",".join(coordinate.columns), *coordinate.values)
            )
        )
        assert not any("epoch" in column for column in coordinate.columns)


def test_physical_ids_are_sorted_and_deduplicated_not_logical_order() -> None:
    coordinates = archive_coordinates()
    cursor = PointCursor()
    matching._d34_reserve_archive(cursor, tuple(reversed(coordinates)))  # type: ignore[arg-type]
    assert [params for _, params in cursor.statements] == [
        (matching._d34_ARCHIVE_NAMESPACE, key)
        for key in sorted({coordinate.physical_id for coordinate in coordinates})
    ]


def test_hash_collision_only_overserializes_and_retains_all_producer_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CollisionDigest:
        def digest(self) -> bytes:
            return b"\xff" * 32

    monkeypatch.setattr(matching.hashlib, "sha256", lambda _: CollisionDigest())
    # Do not use the patched module to produce the input digests themselves.
    coordinates = matching._d34_archive_coordinates(
        result=("8" * 64, "0" * 64, "1" * 64, "2" * 64),
        pair=("3" * 64, "4" * 64, "5" * 64, "p"),
        verifier=("9" * 64, "6" * 64),
        observation=("a" * 64, "requirement", "r", "c", "verify_requirement_v1"),
        execution=("a" * 64, "9" * 64, "7" * 64, "2" * 64, "3" * 64),
    )
    assert len(coordinates) == 15
    assert len({coordinate.preimage for coordinate in coordinates}) == 15
    assert {coordinate.physical_id for coordinate in coordinates} == {-1}
    cursor = PointCursor()
    matching._d34_reserve_archive(cursor, coordinates)  # type: ignore[arg-type]
    assert len(cursor.statements) == 1
    assert sum(len(coordinate.preimage) for coordinate in coordinates) > 15


@pytest.mark.parametrize("alias", range(15))
def test_every_occupied_global_alias_fails_preflight_before_row_lock(
    alias: int,
) -> None:
    cursor = PointCursor(occupied=alias)
    with pytest.raises((EventConflictError, ValidationError)):
        matching._d34_preflight_archive(cursor, archive_coordinates())  # type: ignore[arg-type]
    assert not any(
        "FOR UPDATE" in statement or statement.startswith("INSERT")
        for statement, _ in cursor.statements
    )


def test_pair_input_is_the_only_reusable_relation() -> None:
    coordinates = archive_coordinates()
    pair_hash = coordinates[4].values[0]

    class ReusedPairCursor(PointCursor):
        def execute(self, query: Any, params: tuple[Any, ...]) -> ReusedPairCursor:
            super().execute(query, params)
            if "groundloop_m5_requirement_pair_input" in self.statements[-1][0]:
                self.rows = [(pair_hash,)]
            return self

    cursor = ReusedPairCursor()
    matching._d34_preflight_archive(cursor, coordinates)  # type: ignore[arg-type]
    assert cursor.probes == 15


def test_missing_actual_reservation_fails_closed() -> None:
    cursor = PointCursor(held=False)
    with pytest.raises(EventConflictError, match="reservation is not held"):
        matching._d34_require_archive_reservations(cursor, archive_coordinates())  # type: ignore[arg-type]
    assert len(cursor.statements) == 1


@pytest.mark.parametrize(
    ("closed", "broken", "status"),
    (
        (True, False, TransactionStatus.IDLE),
        (False, True, TransactionStatus.IDLE),
        (False, False, TransactionStatus.ACTIVE),
        (False, False, TransactionStatus.INTRANS),
        (False, False, TransactionStatus.INERROR),
        (False, False, TransactionStatus.UNKNOWN),
    ),
)
def test_owned_entry_idle_precondition_rejects_unavailable_or_ambient_connection(
    closed: bool, broken: bool, status: TransactionStatus
) -> None:
    connection = SimpleNamespace(
        closed=closed,
        broken=broken,
        info=SimpleNamespace(transaction_status=status),
        isolation_level=None,
        read_only=None,
    )
    with pytest.raises(ValidationError, match="idle owned connection"):
        _d34_require_idle(connection)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("isolation", "read_only"),
    (
        (IsolationLevel.READ_UNCOMMITTED, None),
        (IsolationLevel.REPEATABLE_READ, None),
        (IsolationLevel.SERIALIZABLE, None),
        (None, True),
        (IsolationLevel.READ_COMMITTED, True),
    ),
)
def test_owned_entry_rejects_incompatible_settings_without_overriding_them(
    isolation: IsolationLevel | None, read_only: bool | None
) -> None:
    connection = SimpleNamespace(
        closed=False,
        broken=False,
        info=SimpleNamespace(transaction_status=TransactionStatus.IDLE),
        isolation_level=isolation,
        read_only=read_only,
    )
    with pytest.raises(ValidationError, match="read-write READ COMMITTED"):
        _d34_require_idle(connection)  # type: ignore[arg-type]
    assert connection.isolation_level is isolation
    assert connection.read_only is read_only


@pytest.mark.parametrize("isolation", (None, IsolationLevel.READ_COMMITTED))
def test_owned_entry_idle_precondition_accepts_only_compatible_idle_settings(
    isolation: IsolationLevel | None,
) -> None:
    connection = SimpleNamespace(
        closed=False,
        broken=False,
        info=SimpleNamespace(transaction_status=TransactionStatus.IDLE),
        isolation_level=isolation,
        read_only=False,
    )
    _d34_require_idle(connection)  # type: ignore[arg-type]


class _OwnerCursor:
    """Pure primitive harness; it does not stand in for real SQL qualification."""

    def __init__(self) -> None:
        self.connection = SimpleNamespace(
            info=SimpleNamespace(transaction_status=TransactionStatus.INTRANS)
        )
        self.backend = 17
        self.transaction = 2**33 + 37
        self.schema = "owned_fixture"
        self.statements: list[str] = []

    def execute(self, statement: str) -> _OwnerCursor:
        self.statements.append(statement)
        return self

    def fetchone(self) -> tuple[object, ...]:
        if "current_schema()" in self.statements[-1]:
            return self.backend, str(self.transaction), self.schema
        return self.backend, str(self.transaction)


def _owner_header() -> _EpochHeader:
    return _EpochHeader(
        epoch_id=2,
        structural_event_id="real-header-primitive",
        revision=7,
        structural_status="committed",
        semantic_status="pending",
        evaluation_state="pending",
        runtime_state="semantic_pending",
        candidate_policy_id="policy",
        candidate_policy_manifest_hash="a" * 64,
        requirement_registry_snapshot_digest="b" * 64,
        active_chunk_snapshot_digest="c" * 64,
        requirement_root_set_hash="d" * 64,
        open_work_count=1,
        open_scope_count=0,
        blocking_failure_count=0,
    )


def test_owned_frame_binds_full_xid_header_epoch_revision_and_exact_cursor() -> None:
    cursor = _OwnerCursor()
    header = _owner_header()
    owner = _d34_issue_owner(cursor, cursor.schema, header)  # type: ignore[arg-type]
    assert owner.owner_identity == id(owner)
    assert owner.transaction == 2**33 + 37
    assert (owner.epoch_id, owner.revision) == (header.epoch_id, header.revision)
    _d34_validate_owner(cursor, owner)  # type: ignore[arg-type]
    other_cursor = _OwnerCursor()
    other_cursor.connection = cursor.connection
    with pytest.raises(ValidationError, match="another cursor"):
        _d34_validate_owner(other_cursor, owner)  # type: ignore[arg-type]
    assert not other_cursor.statements


@pytest.mark.parametrize("changed_header", (False, True))
def test_owned_frame_rejects_copies_even_with_matching_backend_and_transaction(
    changed_header: bool,
) -> None:
    cursor = _OwnerCursor()
    owner = _d34_issue_owner(cursor, cursor.schema, _owner_header())  # type: ignore[arg-type]
    copied = (
        replace(owner, header=replace(owner.header, epoch_id=99, revision=99))
        if changed_header
        else copy(owner)
    )
    before = len(cursor.statements)
    with pytest.raises(ValidationError, match="another cursor or transaction"):
        _d34_validate_owner(cursor, copied)  # type: ignore[arg-type]
    assert len(cursor.statements) == before


@pytest.mark.parametrize("field", ("header", "epoch_id", "revision"))
def test_owned_frame_rejects_changed_original_coordinates(field: str) -> None:
    cursor = _OwnerCursor()
    owner = _d34_issue_owner(cursor, cursor.schema, _owner_header())  # type: ignore[arg-type]
    # Deliberate pure fault injection bypasses frozen assignment; production
    # issues this frame internally and never accepts a caller-authored one.
    value: object = replace(owner.header, revision=99) if field == "header" else 99
    object.__setattr__(owner, field, value)
    before = len(cursor.statements)
    with pytest.raises(EventConflictError, match="header/epoch/revision changed"):
        _d34_validate_owner(cursor, owner)  # type: ignore[arg-type]
    assert len(cursor.statements) == before


@pytest.mark.parametrize("field", ("backend", "transaction", "schema"))
def test_owned_frame_rejects_changed_actual_transaction_or_schema(field: str) -> None:
    cursor = _OwnerCursor()
    owner = _d34_issue_owner(cursor, cursor.schema, _owner_header())  # type: ignore[arg-type]
    setattr(cursor, field, "other" if field == "schema" else 99)
    with pytest.raises(EventConflictError, match="transaction or schema changed"):
        _d34_validate_owner(cursor, owner)  # type: ignore[arg-type]


class _ArchiveOwnershipCursor:
    """Labelled scalar/status faults, not a simulated successful SQL history."""

    def __init__(
        self,
        row: tuple[int, int] | None,
        status: str | None = "in progress",
        *,
        held: bool = True,
        observed_top: int | None = None,
        future: bool = False,
    ) -> None:
        self.row, self.status, self.held = row, status, held
        self.observed_top, self.future = observed_top, future
        self.status_parameters: list[tuple[str, ...]] = []
        self.rows: list[tuple[Any, ...]] = []

    def execute(self, query: Any, params: tuple[Any, ...]) -> _ArchiveOwnershipCursor:
        from psycopg.errors import InvalidParameterValue

        statement = query if isinstance(query, str) else query.as_string()
        if "pg_catalog.pg_locks" in statement:
            assert "pid=pg_catalog.pg_backend_pid()" in statement
            assert "mode='ExclusiveLock' AND granted" in statement
            self.rows = [(1,)] if self.held else []
        elif "pg_catalog.pg_xact_status" in statement:
            self.status_parameters.append(params)
            if self.future:
                raise InvalidParameterValue("transaction ID is in the future")
            assert self.row is not None
            top = self.row[1] if self.observed_top is None else self.observed_top
            self.rows = [(str(top), self.status)]
        else:
            assert "xmin::text::bigint" in statement
            self.rows = [self.row] if self.row is not None else []
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


@pytest.mark.parametrize(
    ("top", "xmin", "lifted"),
    [
        (2**33 + 37, 37, 2**33 + 37),
        (2**33 + 37, 38, 2**33 + 38),
        (2**33 - 2, 3, 2**33 + 3),
    ],
    ids=("top-full-xid", "child-full-xid", "synthetic-low32-wrap-not-live-wrap"),
)
def test_archive_family_status_uses_full_forward_lift(
    top: int, xmin: int, lifted: int
) -> None:
    cursor = _ArchiveOwnershipCursor((xmin, top))
    matching._d34_require_current_archive_rows(cursor, archive_coordinates())  # type: ignore[arg-type]
    assert cursor.status_parameters
    assert set(cursor.status_parameters) == {(str(lifted),)}


@pytest.mark.parametrize("status", (None, "committed", "aborted", "IN PROGRESS"))
def test_archive_family_requires_exact_native_in_progress(status: str | None) -> None:
    cursor = _ArchiveOwnershipCursor((38, 2**33 + 37), status)
    with pytest.raises(EventConflictError, match="not owned by the current"):
        matching._d34_require_current_archive_rows(cursor, archive_coordinates())  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "row",
    (
        None,
        (0, 37),
        (1, 37),
        (2, 37),
        (2**32, 37),
        (38, 2),
        (38, 2**64),
        (36, 37),
        (37 + 2**31, 37),
        (3, 2**64 - 2),
        (3, 2**32),
    ),
)
def test_archive_family_rejects_missing_nonnormal_old_and_overflow_coordinates(
    row: tuple[int, int] | None,
) -> None:
    cursor = _ArchiveOwnershipCursor(row)
    with pytest.raises(EventConflictError, match="not owned by the current"):
        matching._d34_require_current_archive_rows(cursor, archive_coordinates())  # type: ignore[arg-type]
    assert cursor.status_parameters == []


def test_archive_family_rejects_same_low32_but_different_full_top() -> None:
    cursor = _ArchiveOwnershipCursor((38, 2**33 + 37), observed_top=2**34 + 37)
    with pytest.raises(EventConflictError, match="not owned by the current"):
        matching._d34_require_current_archive_rows(cursor, archive_coordinates())  # type: ignore[arg-type]


def test_archive_family_native_future_error_fails_closed() -> None:
    cursor = _ArchiveOwnershipCursor((38, 2**33 + 37), future=True)
    with pytest.raises(EventConflictError, match="not owned by the current"):
        matching._d34_require_current_archive_rows(cursor, archive_coordinates())  # type: ignore[arg-type]


def test_archive_family_never_replaces_held_complete_reservations() -> None:
    cursor = _ArchiveOwnershipCursor((38, 2**33 + 37), held=False)
    with pytest.raises(EventConflictError, match="reservation is not held"):
        matching._d34_require_current_archive_rows(cursor, archive_coordinates())  # type: ignore[arg-type]
    assert cursor.status_parameters == []
