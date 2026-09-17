"""Canonical projection and failed-artifact audit tests."""

from __future__ import annotations

import hashlib
import importlib
from typing import Any, cast

import pytest

from groundloop.m5.runtime import digests
from groundloop.m5.runtime import postgres_matching_audit as audit_module
from groundloop.m5.runtime.contracts import (
    MATCHING_WORK_COUNTER_NAMES,
    M5MatchingAuditError,
    M5MatchingAuditFamily,
    M5MatchingEpochStatus,
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


def _projection_from_current(
    connection: Any,
    *,
    epoch_id: int,
    revision: int,
    policy_version: str,
) -> M5MatchingAuditProjection:
    observations = tuple(
        (
            str(row[0]),
            str(row[1]),
            str(row[2]),
            int(row[3]),
            str(row[4]),
        )
        for row in connection.execute(
            """SELECT observation_id,requirement_version_id,group_version_id,
                      requirement_ordinal,text_hash::text
                 FROM groundloop_m5_matching_observation_current
                ORDER BY observation_id COLLATE "C"
            """
        ).fetchall()
    )
    edges = tuple(
        (
            str(row[0]),
            str(row[1]),
            str(row[2]),
            int(row[3]),
            int(row[4]),
        )
        for row in connection.execute(
            """SELECT requirement_version_id,text_hash::text,group_version_id,
                      requirement_ordinal,refcount
                 FROM groundloop_m5_matching_edge_current
                ORDER BY requirement_version_id COLLATE "C",text_hash
            """
        ).fetchall()
    )
    masks = tuple(
        (str(row[0]), str(row[1]), int(row[2]))
        for row in connection.execute(
            """SELECT group_version_id,text_hash::text,mask
                 FROM groundloop_m5_matching_hash_mask_current
                ORDER BY group_version_id COLLATE "C",text_hash
            """
        ).fetchall()
    )
    halls = tuple(
        (
            str(row[0]),
            int(row[1]),
            tuple(int(value) for value in row[2]),
            tuple(int(value) for value in row[3]),
            tuple(int(value) for value in row[4]),
            int(row[5]),
            int(row[6]),
            int(row[7]),
        )
        for row in connection.execute(
            """SELECT group_version_id,requirement_count,mask_histogram,
                      neighbor_counts,deficiencies,maximum_deficiency,
                      matching_size,distinct_hash_count
                 FROM groundloop_m5_matching_hall_current
                ORDER BY group_version_id COLLATE "C"
            """
        ).fetchall()
    )
    return M5MatchingAuditProjection(
        epoch_id,
        revision,
        policy_version,
        observations,
        edges,
        masks,
        halls,
    )


class _AuditRowsCursor:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows

    def execute(self, query: str) -> _AuditRowsCursor:
        assert "FROM groundloop_m5_runtime_epoch" in query
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows


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


def test_runtime_authority_accepts_coordination_gaps_and_binds_update_policy() -> None:
    timestamp = object()
    cursor = _AuditRowsCursor(
        [
            (
                11,
                "sealed",
                9,
                timestamp,
                7,
                9,
                "committed",
                "sealed",
                "complete",
                "strict",
                timestamp,
                7,
                "policy-authority",
                3,
                "committed",
                "sealed",
                "complete",
                "strict",
                timestamp,
            )
        ]
    )
    authority = audit_module._runtime_rows(cast(Any, cursor))[11]
    assert authority.decision_policy_version == "policy-authority"
    assert audit_module._revision_sequence_matches_authority(authority, (1, 4, 7))
    assert not audit_module._revision_sequence_matches_authority(authority, (1, 4, 10))


def test_structural_patch_policy_comes_from_update_not_patch_or_header() -> None:
    authority = audit_module._RuntimeAuthority(
        M5MatchingEpochStatus.SEALED,
        9,
        7,
        3,
        "policy-authority",
    )
    patch: dict[str, Any] = {
        "source_kind": "structural_open",
        "before_epoch_id": 7,
        "before_revision": 3,
        "decision_policy_version": "policy-self-consistent-but-wrong",
    }
    assert not audit_module._structural_patch_matches_authority(
        patch,
        authority,
        (7, 3, "policy-self-consistent-but-wrong"),
    )


def test_patch_and_contribution_work_digests_must_match() -> None:
    contribution: dict[str, Any] = {name: 0 for name in MATCHING_WORK_COUNTER_NAMES}
    expected = digests.matching_work_digest(
        tuple(0 for _ in MATCHING_WORK_COUNTER_NAMES)
    )
    contribution["matching_work_digest"] = expected
    patch = {"matching_work_digest": _sha("different-work")}
    assert not audit_module._work_digest_binding_matches(contribution, patch, expected)


def test_live_nonempty_provenance_replay_and_work_digest_corruption() -> None:
    migration017 = cast(
        Any,
        importlib.import_module("tests.m5.postgres_runtime.test_migration_017"),
    )

    prefix = "lane-b-live-provenance"
    with migration017._pre017_schema() as (connection, _):
        snapshot = migration017._seed_b3_activated_snapshot(
            connection,
            prefix,
            complete_owner_survivor=True,
        )
        assert migration017.install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        migration017._register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = migration017._d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.complete_group_id,
            base=snapshot.epoch_id,
        )
        sealed = migration017._seal_d26_event(
            connection,
            prefix=prefix,
            base=snapshot.epoch_id,
            base_revision=snapshot.revision,
            policy=snapshot.policy_version,
            predecessor=predecessor,
            action="RETIRE",
            retire_current_physical=True,
        )
        expected = _projection_from_current(
            connection,
            epoch_id=sealed.epoch_id,
            revision=sealed.revision,
            policy_version=snapshot.policy_version,
        )
        assert sum(len(rows) for _, rows in expected.family_rows) > 0
        assert (
            connection.execute(
                """SELECT
                 (SELECT count(*) FROM groundloop_m5_matching_observation_working
                   WHERE epoch_id=%s AND NOT present) +
                 (SELECT count(*) FROM groundloop_m5_matching_edge_working
                   WHERE epoch_id=%s AND refcount=0) +
                 (SELECT count(*) FROM groundloop_m5_matching_hash_mask_working
                   WHERE epoch_id=%s AND mask=0) +
                 (SELECT count(*) FROM groundloop_m5_matching_hall_working
                   WHERE epoch_id=%s AND NOT present)""",
                (sealed.epoch_id,) * 4,
            ).fetchone()[0]
            > 0
        )

        artifact = audit_matching_actual_image(
            connection.cursor(),
            python_expected=expected,
            sql_expected=expected,
        )
        assert artifact.passed
        assert artifact.provenance_ok

        connection.execute(
            "ALTER TABLE groundloop_m5_matching_work_contribution DISABLE TRIGGER USER"
        )
        assert (
            connection.execute(
                "UPDATE groundloop_m5_matching_work_contribution "
                "SET matching_work_digest=%s "
                "WHERE epoch_id=%s AND source_kind='structural_open'",
                (_sha("corrupt-contribution-work"), sealed.epoch_id),
            ).rowcount
            == 1
        )
        connection.execute(
            "ALTER TABLE groundloop_m5_matching_work_contribution ENABLE TRIGGER USER"
        )
        corrupted = audit_matching_actual_image(
            connection.cursor(),
            python_expected=expected,
            sql_expected=expected,
        )
        assert not corrupted.passed
        assert not corrupted.provenance_ok
        connection.rollback()
