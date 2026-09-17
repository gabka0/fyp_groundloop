"""Canonical projection and failed-artifact audit tests."""

from __future__ import annotations

import hashlib
from typing import Any, cast

import pytest

from groundloop.m5.runtime import postgres_matching_audit as audit_module
from groundloop.m5.runtime.contracts import (
    M5MatchingAuditError,
    M5MatchingAuditFamily,
    M5PersistedMatchingPhysicalMismatch,
)
from groundloop.m5.runtime.postgres_matching_audit import (
    M5MatchingAuditInvalidError,
    M5MatchingAuditProjection,
    audit_matching_actual_image,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _projection() -> M5MatchingAuditProjection:
    return M5MatchingAuditProjection(
        head_epoch_id=7,
        head_revision=3,
        decision_policy_version="policy-v1",
        observations=(("o1", "r1", "g1", 0, _sha("text")),),
        edges=(("r1", _sha("text"), "g1", 0, 1),),
        masks=(("g1", _sha("text"), 1),),
        halls=(("g1", 1, (0, 1), (0, 1), (0, 0), 0, 1, 1),),
    )


def _empty_points() -> dict[M5MatchingAuditFamily, dict[tuple[str, ...], Any]]:
    return {family: {} for family in audit_module._FAMILIES}


def test_projection_digest_is_stable_and_payload_changes_are_keyed() -> None:
    expected = _projection()
    actual = M5MatchingAuditProjection(
        head_epoch_id=7,
        head_revision=3,
        decision_policy_version="policy-v1",
        observations=expected.observations,
        edges=(("r1", _sha("text"), "g1", 0, 2),),
        masks=expected.masks,
        halls=expected.halls,
    )
    assert expected.projection_digest != actual.projection_digest
    mismatches = audit_module._physical_mismatches(expected, actual)
    assert len(mismatches) == 1
    assert mismatches[0].family is M5MatchingAuditFamily.EDGE
    assert mismatches[0].key == ("r1", _sha("text"))


def test_expected_projection_rejects_duplicate_or_unsorted_outer_keys() -> None:
    with pytest.raises(M5MatchingAuditInvalidError, match="key-sorted unique"):
        M5MatchingAuditProjection(
            head_epoch_id=7,
            head_revision=3,
            decision_policy_version="policy-v1",
            observations=(
                ("o2", "r1", "g1", 0, _sha("two")),
                ("o1", "r1", "g1", 0, _sha("one")),
            ),
            edges=(),
            masks=(),
            halls=(),
        )


def test_canonical_equal_projection_and_provenance_build_pass_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = _projection()
    points = _empty_points()
    monkeypatch.setattr(
        audit_module,
        "_read_actual_image",
        lambda cursor, expected: audit_module._ActualImage(  # noqa: ARG005
            expected, points, ()
        ),
    )
    pair = _sha("pair")
    monkeypatch.setattr(
        audit_module,
        "_audit_provenance",
        lambda cursor, expected, actual_points: audit_module._ProvenanceResult(  # noqa: ARG005
            True,
            _sha("replay"),
            pair,
            pair,
            pair,
            pair,
            (),
        ),
    )
    artifact = audit_matching_actual_image(
        cast(Any, object()), python_expected=expected, sql_expected=expected
    )
    assert artifact.passed


def test_keyed_malformed_current_skips_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = M5MatchingAuditProjection(7, 3, "policy-v1", (), (), (), ())
    mismatch = M5PersistedMatchingPhysicalMismatch(
        family=M5MatchingAuditFamily.OBSERVATION,
        key=("o-bad",),
        expected_row_digest=None,
        actual_row_digest=None,
        actual_error=M5MatchingAuditError.MALFORMED_PAYLOAD,
    )
    monkeypatch.setattr(
        audit_module,
        "_read_actual_image",
        lambda cursor, expected: audit_module._ActualImage(  # noqa: ARG005
            None, _empty_points(), (mismatch,)
        ),
    )

    def _unexpected(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("malformed current must skip provenance")

    monkeypatch.setattr(audit_module, "_audit_provenance", _unexpected)
    artifact = audit_matching_actual_image(
        cast(Any, object()), python_expected=expected, sql_expected=expected
    )
    assert not artifact.passed
    assert artifact.actual_error is M5MatchingAuditError.MALFORMED_PAYLOAD
    assert artifact.provenance_replay_digest is None
    assert artifact.mismatches == (mismatch,)
