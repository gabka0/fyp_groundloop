"""Store-derived event-result reference boundary tests."""

from __future__ import annotations

import hashlib
from typing import Any, cast

import pytest

from groundloop.errors import ValidationError
from groundloop.m5.runtime.postgres_matching_publication import (
    build_matching_publication_children,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class _RowsCursor:
    def __init__(self, envelope: tuple[object, ...], logical: list[tuple[object, ...]]):
        self.envelope = envelope
        self.logical = logical
        self.rows: list[tuple[object, ...]] = []

    def execute(self, query: str, params: object = None) -> _RowsCursor:
        del params
        if "FROM groundloop_epoch AS epoch" in query:
            self.rows = [self.envelope]
        elif "FROM groundloop_m5_matching_work_contribution" in query:
            self.rows = list(self.logical)
        else:  # pragma: no cover - a new query is an intentional test failure.
            raise AssertionError(query)
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows


def _envelope(*, update_kind: str = "observe_requirement") -> tuple[object, ...]:
    return (
        "event-9",
        _sha("payload"),
        9,
        4,
        "committed",
        "sealed",
        "complete",
        "strict",
        True,
        "event-9",
        "sealed",
        True,
        4,
        8,
        8,
        update_kind,
        9,
        9,
        4,
        None,
        None,
        None,
        None,
        0,
        3,
    )


def _absence(kind: str) -> tuple[object, ...]:
    preimage = f"{kind}-patch".encode()
    return (
        1,
        "structural_open",
        "event-9",
        _sha("payload"),
        8,
        3,
        kind,
        "object-1",
        _sha("before"),
        None,
        True,
        hashlib.sha256(preimage).hexdigest(),
        preimage,
    )


def test_empty_net_change_builds_empty_deterministic_children() -> None:
    cursor = _RowsCursor(_envelope(), [])
    result = build_matching_publication_children(
        cast(Any, cursor), epoch_id=9, sealed_revision=4
    )
    assert result.combined_deltas == ()
    assert result.changed_state_references == ()


def test_claim_state_absence_is_never_a_d26_reference() -> None:
    cursor = _RowsCursor(_envelope(), [_absence("claim_state")])
    with pytest.raises(ValidationError, match="nonqualifying logical absence"):
        build_matching_publication_children(
            cast(Any, cursor), epoch_id=9, sealed_revision=4
        )


def test_d26_kind_still_requires_exact_replace_or_retire_envelope() -> None:
    cursor = _RowsCursor(_envelope(), [_absence("requirement_state")])
    with pytest.raises(ValidationError, match="replace_group or retire_group"):
        build_matching_publication_children(
            cast(Any, cursor), epoch_id=9, sealed_revision=4
        )
