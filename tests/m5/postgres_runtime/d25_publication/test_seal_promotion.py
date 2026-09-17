"""One-epoch promotion ownership and ordering tests."""

from __future__ import annotations

from typing import Any, cast

import pytest

from groundloop.errors import ValidationError
from groundloop.m5.runtime.postgres_matching_publication import (
    promote_matching_overlay,
)


class _RecordingCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.rowcount = 0

    def execute(self, query: str, params: object = None) -> _RecordingCursor:
        normalized = " ".join(query.split())
        self.calls.append((normalized, params))
        if normalized.startswith("UPDATE groundloop_m5_matching_image_current"):
            self.rowcount = 1
        elif normalized.startswith("DELETE FROM"):
            self.rowcount = 2
        elif normalized.startswith("INSERT INTO"):
            self.rowcount = 3
        else:
            self.rowcount = 0
        return self


def test_seal_promotion_is_one_epoch_and_does_not_own_outer_transaction() -> None:
    cursor = _RecordingCursor()
    receipt = promote_matching_overlay(
        cast(Any, cursor), epoch_id=17, expected_revision=4, sealed_revision=5
    )
    assert receipt.mode == "seal"
    assert receipt.observation_deletes == 2
    assert receipt.observation_writes == 3
    assert receipt.edge_deletes == 2
    assert receipt.edge_writes == 3
    assert receipt.mask_deletes == 2
    assert receipt.mask_writes == 3
    assert receipt.hall_deletes == 2
    assert receipt.hall_writes == 3
    sql = "\n".join(query for query, _ in cursor.calls).lower()
    assert "groundloop_m5_authorize_persisted_matching_seal" in sql
    assert "where epoch_id = %s" in sql
    assert "groundloop_m5_publication_head" not in sql
    assert "groundloop_m5_event_result" not in sql
    assert "commit" not in sql
    assert "rollback" not in sql


def test_seal_promotion_rejects_nonadjacent_seal_before_sql() -> None:
    cursor = _RecordingCursor()
    with pytest.raises(ValidationError, match=r"expected_revision \+ 1"):
        promote_matching_overlay(
            cast(Any, cursor), epoch_id=17, expected_revision=4, sealed_revision=6
        )
    assert cursor.calls == []
