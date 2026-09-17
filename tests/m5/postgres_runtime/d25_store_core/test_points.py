"""Resolved/effective point, shadowing, and representative-query tests."""

from __future__ import annotations

import hashlib
from typing import Any

import pytest

from groundloop.errors import ValidationError
from groundloop.m5.runtime.contracts import (
    M5MatchingEdgeCurrent,
    M5MatchingEdgeWorking,
    M5MatchingHallCurrent,
    M5MatchingHallWorking,
    M5MatchingMaskCurrent,
    M5MatchingMaskWorking,
    M5MatchingObservationCurrent,
    M5MatchingObservationWorking,
    M5PersistedMatchingSourceKind,
)
from groundloop.m5.runtime.postgres_matching import (
    _authorize_checked_prefix,
    apply_matching_transition,
    derive_matching_transition_intent,
    effective_matching_edge,
    effective_matching_hall,
    effective_matching_image,
    effective_matching_mask,
    effective_matching_observation,
    least_effective_observation,
    representative_effective_hashes,
    resolved_matching_edge_point,
    resolved_matching_hall_point,
    resolved_matching_mask_point,
    resolved_matching_observation_point,
)
from tests.m5.postgres_runtime import test_migration_017 as schema_fixture


class _RecordingCursor:
    def __init__(self, cursor: Any) -> None:
        self.cursor = cursor
        self.statements: list[str] = []

    def execute(self, query: Any, params: Any = None) -> Any:
        self.statements.append(str(query))
        return self.cursor.execute(query, params)


def _apply_empty(database: Any) -> None:
    with database.connection.cursor() as cursor:
        intent = derive_matching_transition_intent(
            cursor,
            database.epoch_id,
            1,
            1,
            M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            database.event_id,
        )
        apply_matching_transition(cursor, intent)


def _authorize_read(database: Any) -> None:
    with database.connection.cursor() as cursor:
        _authorize_checked_prefix(
            cursor, epoch_id=database.epoch_id, expected_revision=1
        )


def _closed_alternate_policy(database: Any) -> str:
    connection = database.connection
    future = connection.execute(
        """
        INSERT INTO groundloop_epoch (
          event_id, payload_hash, revision, structural_status, semantic_status,
          evaluation_state, publication_mode, sealed_at
        ) VALUES (%s,%s,0,'committed','sealed','complete','strict',now())
        RETURNING epoch_id
        """,
        (
            f"{database.event_id}-future-policy-boundary",
            hashlib.sha256(
                f"{database.event_id}-future-policy-boundary".encode()
            ).hexdigest(),
        ),
    ).fetchone()
    assert future is not None
    policy = f"{database.policy_version}-mismatch"
    connection.execute(
        """
        INSERT INTO groundloop_decision_policy (
          policy_version, support_threshold, refute_threshold,
          tie_rule_version, valid_from_epoch, valid_to_epoch
        ) VALUES (%s,.5,.5,'v1',%s,%s)
        """,
        (policy, database.epoch_id, int(future[0])),
    )
    return policy


def test_current_fallback_points_and_image_are_lossless(
    rich_structural_database: Any,
) -> None:
    database = rich_structural_database
    _apply_empty(database)
    observation = database.snapshot.requirement_observations[0]
    observation_id, requirement_id, group_id, ordinal, text_hash = observation
    with database.connection.cursor() as cursor:
        image = effective_matching_image(cursor, database.epoch_id)
        observation_point = resolved_matching_observation_point(
            cursor, database.epoch_id, observation_id
        )
        edge_point = resolved_matching_edge_point(
            cursor, database.epoch_id, requirement_id, text_hash
        )
        mask_point = resolved_matching_mask_point(
            cursor, database.epoch_id, group_id, text_hash
        )
        hall_point = resolved_matching_hall_point(cursor, database.epoch_id, group_id)

    assert image.current_installed_epoch_id == database.base_epoch_id
    assert image.current_installed_revision == database.base_revision
    assert image.working_epoch_id == database.epoch_id
    assert image.working_base_epoch_id == database.base_epoch_id
    assert image.working_base_revision == database.base_revision
    assert image.working_decision_policy_version == database.policy_version
    assert image.working_updated_revision == 1
    assert isinstance(observation_point, M5MatchingObservationCurrent)
    assert observation_point.requirement_version_id == requirement_id
    assert observation_point.group_version_id == group_id
    assert observation_point.requirement_ordinal == ordinal
    assert observation_point.text_hash == text_hash
    assert isinstance(edge_point, M5MatchingEdgeCurrent)
    assert isinstance(mask_point, M5MatchingMaskCurrent)
    assert isinstance(hall_point, M5MatchingHallCurrent)


def test_text_identities_preserve_leading_whitespace_without_normalization(
    whitespace_structural_database: Any,
) -> None:
    database = whitespace_structural_database
    assert database.policy_version.startswith("  ")
    assert database.event_id.startswith("  ")
    with database.connection.cursor() as cursor:
        intent = derive_matching_transition_intent(
            cursor,
            database.epoch_id,
            1,
            1,
            M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            database.event_id,
        )
        receipt = apply_matching_transition(cursor, intent)
        image = effective_matching_image(cursor, database.epoch_id)
    assert intent.source_id == database.event_id
    assert intent.decision_policy_version == database.policy_version
    assert receipt.patch.source_id == database.event_id
    assert receipt.patch.decision_policy_version == database.policy_version
    assert image.current_decision_policy_version == database.policy_version
    assert image.working_decision_policy_version == database.policy_version


@pytest.mark.parametrize(
    "mutation",
    ("current_policy", "typed_candidate_policy", "direct_candidate_policy"),
)
def test_policy_mismatch_fails_before_any_physical_point_read(
    empty_structural_database: Any, mutation: str
) -> None:
    database = empty_structural_database
    _apply_empty(database)
    alternate_policy = _closed_alternate_policy(database)
    if mutation == "current_policy":
        table = "groundloop_m5_matching_image_current"
        statement = (
            "UPDATE groundloop_m5_matching_image_current "
            "SET decision_policy_version=%s WHERE singleton"
        )
    elif mutation == "typed_candidate_policy":
        table = "groundloop_m5_candidate_policy"
        statement = (
            "UPDATE groundloop_m5_candidate_policy SET decision_policy_version=%s"
        )
    else:
        table = "groundloop_candidate_policy"
        statement = "UPDATE groundloop_candidate_policy SET decision_policy_version=%s"
    with schema_fixture._b3_user_triggers_disabled(database.connection, table):
        database.connection.execute(statement, (alternate_policy,))

    with database.connection.cursor() as cursor:
        recording = _RecordingCursor(cursor)
        with pytest.raises(ValidationError, match="persisted matching .* inconsistent"):
            resolved_matching_observation_point(
                recording, database.epoch_id, "never-read-observation"
            )
    assert not any(
        "groundloop_m5_matching_observation_" in statement
        for statement in recording.statements
    )


def test_reader_scope_rejects_a_different_epoch_before_acquiring_it(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    _apply_empty(database)
    with database.connection.cursor() as cursor:
        recording = _RecordingCursor(cursor)
        with pytest.raises(ValidationError, match="exact checked epoch prefix"):
            effective_matching_image(recording, database.base_epoch_id)
    assert not any(
        "FROM groundloop_epoch AS epoch" in statement
        for statement in recording.statements
    )


def test_point_locks_current_before_working_for_each_physical_key(
    rich_structural_database: Any,
) -> None:
    database = rich_structural_database
    _apply_empty(database)
    observation_id = database.snapshot.requirement_observations[0][0]
    with database.connection.cursor() as cursor:
        recording = _RecordingCursor(cursor)
        point = resolved_matching_observation_point(
            recording, database.epoch_id, observation_id
        )
    assert isinstance(point, M5MatchingObservationCurrent)
    current_image = next(
        index
        for index, statement in enumerate(recording.statements)
        if "FROM groundloop_m5_matching_image_current" in statement
    )
    working_image = next(
        index
        for index, statement in enumerate(recording.statements)
        if "FROM groundloop_m5_matching_image_working" in statement
    )
    current_point = next(
        index
        for index, statement in enumerate(recording.statements)
        if "FROM groundloop_m5_matching_observation_current" in statement
    )
    working_point = next(
        index
        for index, statement in enumerate(recording.statements)
        if "FROM groundloop_m5_matching_observation_working" in statement
    )
    assert current_image < working_image < current_point < working_point


def test_representatives_use_c_order_and_exact_bounded_cardinality(
    rich_structural_database: Any,
) -> None:
    database = rich_structural_database
    _apply_empty(database)
    row = database.connection.execute(
        """
        SELECT group_version_id, mask, requirement_count, mask_histogram
        FROM groundloop_m5_matching_hash_mask_current AS mask_row
        JOIN groundloop_m5_matching_hall_current AS hall USING(group_version_id)
        ORDER BY group_version_id COLLATE "C", mask, text_hash COLLATE "C"
        LIMIT 1
        """
    ).fetchone()
    assert row is not None
    group_id, mask, requirement_count = str(row[0]), int(row[1]), int(row[2])
    mask_histogram = tuple(int(value) for value in row[3])
    limit = requirement_count
    expected_hashes = tuple(
        str(value[0]).strip()
        for value in database.connection.execute(
            """
            SELECT text_hash::text
            FROM groundloop_m5_matching_hash_mask_current
            WHERE group_version_id = %s AND mask = %s
            ORDER BY text_hash::text COLLATE "C"
            LIMIT %s
            """,
            (group_id, mask, limit),
        ).fetchall()
    )
    first_hash = expected_hashes[0]
    edge = database.connection.execute(
        """
        SELECT requirement_ordinal
        FROM groundloop_m5_matching_edge_current
        WHERE group_version_id = %s AND text_hash = %s
        ORDER BY requirement_ordinal
        LIMIT 1
        """,
        (group_id, first_hash),
    ).fetchone()
    assert edge is not None
    expected_observation = database.connection.execute(
        """
        SELECT observation_id
        FROM groundloop_m5_matching_observation_current
        WHERE group_version_id = %s AND requirement_ordinal = %s
          AND text_hash = %s
        ORDER BY observation_id COLLATE "C"
        LIMIT 1
        """,
        (group_id, int(edge[0]), first_hash),
    ).fetchone()
    assert expected_observation is not None
    with database.connection.cursor() as cursor:
        hashes = representative_effective_hashes(
            cursor, database.epoch_id, group_id, mask, limit
        )
        least = least_effective_observation(
            cursor, database.epoch_id, group_id, int(edge[0]), first_hash
        )
    assert hashes == expected_hashes
    assert len(hashes) == min(mask_histogram[mask], requirement_count)
    assert least == str(expected_observation[0])
    if requirement_count > 1:
        with database.connection.cursor() as cursor:
            with pytest.raises(ValidationError, match="group shape"):
                representative_effective_hashes(
                    cursor,
                    database.epoch_id,
                    group_id,
                    mask,
                    requirement_count - 1,
                )


def test_working_tombstones_shadow_current_before_effective_filtering(
    rich_retirement_database: Any,
) -> None:
    database = rich_retirement_database
    _authorize_read(database)
    epoch_id = database.epoch_id
    group_id = database.snapshot.partial_group_id
    observation = database.connection.execute(
        """
        SELECT observation_id, requirement_version_id, text_hash
        FROM groundloop_m5_matching_observation_working
        WHERE epoch_id = %s AND group_version_id = %s AND NOT present
        ORDER BY observation_id COLLATE "C" LIMIT 1
        """,
        (epoch_id, group_id),
    ).fetchone()
    edge = database.connection.execute(
        """
        SELECT requirement_version_id, text_hash
        FROM groundloop_m5_matching_edge_working
        WHERE epoch_id = %s AND group_version_id = %s AND refcount = 0
        ORDER BY requirement_ordinal, text_hash COLLATE "C" LIMIT 1
        """,
        (epoch_id, group_id),
    ).fetchone()
    mask = database.connection.execute(
        """
        SELECT text_hash
        FROM groundloop_m5_matching_hash_mask_working
        WHERE epoch_id = %s AND group_version_id = %s AND mask = 0
        ORDER BY text_hash COLLATE "C" LIMIT 1
        """,
        (epoch_id, group_id),
    ).fetchone()
    assert observation is not None and edge is not None and mask is not None
    observation_id = str(observation[0])
    requirement_id = str(edge[0])
    edge_hash = str(edge[1]).strip()
    mask_hash = str(mask[0]).strip()
    with database.connection.cursor() as cursor:
        resolved_observation = resolved_matching_observation_point(
            cursor, epoch_id, observation_id
        )
        resolved_edge = resolved_matching_edge_point(
            cursor, epoch_id, requirement_id, edge_hash
        )
        resolved_mask = resolved_matching_mask_point(
            cursor, epoch_id, group_id, mask_hash
        )
        resolved_hall = resolved_matching_hall_point(cursor, epoch_id, group_id)
        assert effective_matching_observation(cursor, epoch_id, observation_id) is None
        assert (
            effective_matching_edge(cursor, epoch_id, requirement_id, edge_hash) is None
        )
        assert effective_matching_mask(cursor, epoch_id, group_id, mask_hash) == 0
        assert effective_matching_hall(cursor, epoch_id, group_id) is None

    assert isinstance(resolved_observation, M5MatchingObservationWorking)
    assert not resolved_observation.present
    assert isinstance(resolved_edge, M5MatchingEdgeWorking)
    assert resolved_edge.refcount == 0
    assert isinstance(resolved_mask, M5MatchingMaskWorking)
    assert resolved_mask.mask == 0
    assert isinstance(resolved_hall, M5MatchingHallWorking)
    assert not resolved_hall.present
