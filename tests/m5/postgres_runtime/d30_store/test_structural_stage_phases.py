"""Lane-R exact D25 structural stage-phase falsifiers."""

from __future__ import annotations

import inspect
from copy import copy
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from groundloop.errors import EventConflictError, ValidationError
from groundloop.m5.runtime import postgres_matching as matching
from groundloop.m5.runtime import postgres_withdrawal as withdrawal
from groundloop.m5.runtime.contracts import M5PersistedMatchingSourceKind
from tests.m5.postgres_runtime.d25_store_core.conftest import (
    authorize_and_derive_matching_transition,
    empty_structural_database,  # noqa: F401
)
from tests.m5.postgres_runtime.d25_store_core.conftest import (
    document_withdrawal_planning_database as _document_withdrawal_database,  # noqa: F401
)


def _family_names(plan: matching._MatchingWritePlan) -> tuple[str, ...]:
    return tuple(
        name
        for name in (
            "observation" if plan.observation_rows else "",
            "edge" if plan.edge_rows else "",
            "mask" if plan.mask_rows else "",
            "hall" if plan.hall_rows else "",
            "requirement" if plan.requirement_state_rows else "",
            "group" if plan.group_state_rows else "",
            "group-artifact" if plan.group_certificate_artifact_rows else "",
            "group-binding" if plan.group_binding_rows else "",
            "claim" if plan.claim_state_rows else "",
            "claim-artifact" if plan.claim_certificate_artifact_rows else "",
            "claim-binding" if plan.claim_binding_rows else "",
            "answer" if plan.answer_state_rows else "",
        )
        if name
    )


def test_direct_before_images_move_from_d29_tier_11a_to_exact_d25_tiers() -> None:
    claim_authority = inspect.getsource(withdrawal._lock_d29_direct_claim_before_images)
    for relation in (
        "groundloop_published_claim_state",
        "groundloop_claim_state_materialized",
        "groundloop_claim_certificate",
    ):
        assert relation in claim_authority
    assert claim_authority.count("FOR UPDATE") == 3
    for field in (
        "image.state_valid_from_epoch",
        "image.materialized_updated_epoch",
        "image.materialized_updated_revision",
        "image.certificate_support_observation_id",
        "image.certificate_refute_observation_id",
        "image.certificate_repaired_epoch",
        "image.certificate_repaired_revision",
    ):
        assert field in claim_authority

    answer_authority = inspect.getsource(
        withdrawal._lock_d29_direct_answer_before_images
    )
    for field in (
        "required_claim_count",
        "supported_count",
        "unsupported_count",
        "refuted_count",
        "conflicted_count",
        "status",
    ):
        assert field in answer_authority
    assert answer_authority.count("FOR UPDATE") == 1
    assert "if locked != expected" in answer_authority

    target_only = inspect.getsource(matching._lock_expected_document_direct_m4_claims)
    for relation in (
        "groundloop_published_claim_state",
        "groundloop_claim_state_materialized",
        "groundloop_claim_certificate",
    ):
        assert relation not in target_only

    lock_order = inspect.getsource(matching._lock_structural_intent_rows)
    tier_12 = lock_order.index("planned_group_binding_digests")
    pure_targets = lock_order.index("direct_claim_target_ids =")
    target_comparison = lock_order.index("!= direct_claim_target_ids")
    authority_validation = lock_order.index("_validate_d29_direct_matching_authority(")
    direct_claim = lock_order.index("_lock_d29_direct_claim_before_images(")
    direct_claim_target = lock_order.index("_lock_expected_document_direct_m4_claims(")
    combined_claim = lock_order.index(
        'relation_name="groundloop_m5_published_claim_state"'
    )
    direct_answer = lock_order.index("_lock_d29_direct_answer_before_images(")
    direct_answer_target = lock_order.index(
        "_lock_expected_document_direct_m4_answers("
    )
    combined_answer = lock_order.index(
        'relation_name="groundloop_m5_published_answer_state"'
    )
    assert (
        tier_12
        < pure_targets
        < target_comparison
        < authority_validation
        < direct_claim
        < direct_claim_target
        < combined_claim
        < direct_answer
        < direct_answer_target
        < combined_answer
    )
    assert "!= direct_claim_target_ids" in lock_order
    assert "!= direct_answer_target_ids" in lock_order


@pytest.mark.parametrize(
    ("helper", "relation_name", "key_columns", "key_parts"),
    (
        (
            matching._lock_expected_document_direct_m4_claims,
            "groundloop_m4_working_claim_state",
            ("epoch_id", "claim_id"),
            (),
        ),
        (
            matching._lock_expected_document_direct_m4_claims,
            "groundloop_m4_working_claim_state",
            ("epoch_id", "claim_id"),
            (7, "claim-a", "extra"),
        ),
        (
            matching._lock_expected_document_direct_m4_answers,
            "groundloop_m4_working_answer_state",
            ("epoch_id", "answer_version_id"),
            (),
        ),
        (
            matching._lock_expected_document_direct_m4_answers,
            "groundloop_m4_working_answer_state",
            ("epoch_id", "answer_version_id"),
            (7, "answer-a", "extra"),
        ),
    ),
)
def test_direct_target_locks_reject_nonexact_coordinates_before_sql(
    helper: Any,
    relation_name: str,
    key_columns: tuple[str, ...],
    key_parts: tuple[object, ...],
) -> None:
    image = matching._DirectStageImage(
        matching._DirectStageCoordinate(relation_name, key_columns, key_parts),
        {},
    )
    with pytest.raises(ValidationError, match="image has another key"):
        helper(
            object(),
            SimpleNamespace(resulting_epoch_id=7),
            (image,),
        )


def test_retained_stage_wrapper_invokes_exact_tier_phases_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    plan = matching._MatchingWritePlan(
        observation_currency_rows=("currency",),  # type: ignore[arg-type]
        observation_rows=("observation",),  # type: ignore[arg-type]
        edge_rows=("edge",),  # type: ignore[arg-type]
        mask_rows=("mask",),  # type: ignore[arg-type]
        hall_rows=("hall",),  # type: ignore[arg-type]
        requirement_state_rows=("requirement",),  # type: ignore[arg-type]
        group_state_rows=("group",),  # type: ignore[arg-type]
        claim_state_rows=("claim",),  # type: ignore[arg-type]
        answer_state_rows=("answer",),  # type: ignore[arg-type]
        group_certificate_artifact_rows=("group-artifact",),  # type: ignore[arg-type]
        claim_certificate_artifact_rows=("claim-artifact",),  # type: ignore[arg-type]
        group_binding_rows=("group-binding",),  # type: ignore[arg-type]
        claim_binding_rows=("claim-binding",),  # type: ignore[arg-type]
        document_direct_plan=matching._DocumentDirectPlan(),
        expected_direct_m4_claim_after_images=("m4-claim",),  # type: ignore[arg-type]
        expected_direct_m4_answer_after_images=("m4-answer",),  # type: ignore[arg-type]
    )
    prepared = SimpleNamespace(
        phase=matching._MatchingPhase.READY_TO_ADVANCE,
        physical_and_logical_write_plan=plan,
        observation_currency_before_images=("before",),
        base_header_after_image="base-header",
        runtime_header_after_image="runtime-header",
    )
    intent = SimpleNamespace(resulting_epoch_id=1)
    cursor = object()

    def validate_identity(
        actual_cursor: object,
        actual_intent: object,
        actual_prepared: object,
        *,
        phase: matching._MatchingPhase,
    ) -> None:
        assert (actual_cursor, actual_intent, actual_prepared) == (
            cursor,
            intent,
            prepared,
        )
        assert prepared.phase is phase
        events.append(("identity", phase.value))

    monkeypatch.setattr(matching, "_validate_prepared_identity", validate_identity)
    monkeypatch.setattr(
        matching,
        "_validate_prepared_matching_write_plan",
        lambda *_args: events.append("original-before"),
    )
    monkeypatch.setattr(
        matching,
        "_header_images",
        lambda *_args: ("base-header", "runtime-header"),
    )
    monkeypatch.setattr(
        matching,
        "_stage_observation_currency_plan",
        lambda *_args: events.append("tier-11a-currency"),
    )
    monkeypatch.setattr(
        matching,
        "_stage_matching_image",
        lambda *_args: events.append("tier-11b-image"),
    )
    monkeypatch.setattr(
        matching,
        "_matching_stage_before_map",
        lambda *_args: {},
    )

    def stage_fragment(
        _cursor: object,
        _intent: object,
        fragment: matching._MatchingWritePlan,
        _before: object,
    ) -> None:
        events.append(("fragment", _family_names(fragment)))

    monkeypatch.setattr(matching, "_stage_matching_write_plan_fragment", stage_fragment)

    def validate_m4(
        _cursor: object,
        images: tuple[object, ...],
        *,
        relation_name: str,
        epoch_id: int,
    ) -> None:
        events.append(("m4", relation_name, epoch_id, images))

    monkeypatch.setattr(
        matching, "_validate_expected_document_direct_m4_images", validate_m4
    )

    result = matching._stage_prepared_matching_transition(
        cursor,  # type: ignore[arg-type]
        intent,  # type: ignore[arg-type]
        prepared,  # type: ignore[arg-type]
    )

    assert result is prepared
    assert prepared.phase is matching._MatchingPhase.STAGED
    assert events == [
        ("identity", "ready_to_advance"),
        "original-before",
        "tier-11a-currency",
        "tier-11b-image",
        (
            "fragment",
            (
                "observation",
                "edge",
                "mask",
                "hall",
                "requirement",
                "group",
                "group-artifact",
                "group-binding",
            ),
        ),
        ("identity", "staged_through_tier_12"),
        (
            "m4",
            "groundloop_m4_working_claim_state",
            1,
            ("m4-claim",),
        ),
        ("fragment", ("claim", "claim-artifact", "claim-binding")),
        ("identity", "staged_through_tier_13"),
        (
            "m4",
            "groundloop_m4_working_answer_state",
            1,
            ("m4-answer",),
        ),
        ("fragment", ("answer",)),
    ]


def test_stage_family_projections_are_exact_and_disjoint() -> None:
    plan = matching._MatchingWritePlan(
        observation_currency_rows=("currency",),  # type: ignore[arg-type]
        observation_rows=("observation",),  # type: ignore[arg-type]
        edge_rows=("edge",),  # type: ignore[arg-type]
        mask_rows=("mask",),  # type: ignore[arg-type]
        hall_rows=("hall",),  # type: ignore[arg-type]
        requirement_state_rows=("requirement",),  # type: ignore[arg-type]
        group_state_rows=("group",),  # type: ignore[arg-type]
        claim_state_rows=("claim",),  # type: ignore[arg-type]
        answer_state_rows=("answer",),  # type: ignore[arg-type]
        group_certificate_artifact_rows=("group-artifact",),  # type: ignore[arg-type]
        claim_certificate_artifact_rows=("claim-artifact",),  # type: ignore[arg-type]
        group_binding_rows=("group-binding",),  # type: ignore[arg-type]
        claim_binding_rows=("claim-binding",),  # type: ignore[arg-type]
    )
    tier_12 = matching._matching_tier_12_write_plan(plan)
    tier_13 = matching._matching_tier_13_write_plan(plan)
    tier_14 = matching._matching_tier_14_write_plan(plan)
    assert _family_names(tier_12) == (
        "observation",
        "edge",
        "mask",
        "hall",
        "requirement",
        "group",
        "group-artifact",
        "group-binding",
    )
    assert _family_names(tier_13) == (
        "claim",
        "claim-artifact",
        "claim-binding",
    )
    assert _family_names(tier_14) == ("answer",)
    assert not tier_12.observation_currency_rows
    assert not tier_13.answer_state_rows
    assert not tier_14.claim_state_rows


class _M4FamilyCursor:
    def __init__(self, rows: tuple[tuple[object, ...], ...]) -> None:
        self.rows = rows
        self.parameters: tuple[object, ...] | None = None

    def execute(
        self, _query: object, parameters: tuple[object, ...]
    ) -> _M4FamilyCursor:
        self.parameters = parameters
        return self

    def fetchall(self) -> tuple[tuple[object, ...], ...]:
        return self.rows


class _RecordingCursor:
    def __init__(self, cursor: Any) -> None:
        self.cursor = cursor
        self.statements: list[str] = []

    def execute(self, query: object, parameters: object = None) -> Any:
        self.statements.append(str(query))
        return self.cursor.execute(query, parameters)


_D25_DML_FAMILIES = (
    ("11a", ("groundloop_m5_working_currency_history",)),
    ("11b", ("groundloop_m5_matching_image_working",)),
    ("11c", ("groundloop_m5_matching_observation_working",)),
    ("11d", ("groundloop_m5_matching_edge_working",)),
    ("11e", ("groundloop_m5_matching_hash_mask_working",)),
    ("12a", ("groundloop_m5_matching_hall_working",)),
    (
        "12b",
        (
            "groundloop_m5_working_requirement_state",
            "groundloop_m5_working_group_state",
            "groundloop_m5_group_certificate_artifact",
            "groundloop_m5_group_certificate_artifact_row",
            "groundloop_m5_working_group_certificate_binding",
        ),
    ),
    (
        "13",
        (
            "groundloop_m5_working_claim_state",
            "groundloop_m5_claim_certificate_artifact",
            "groundloop_m5_working_claim_certificate_binding",
        ),
    ),
    ("14", ("groundloop_m5_working_answer_state",)),
)


def _dml_family(statement: str) -> str | None:
    normalized = " ".join(statement.split())
    if not any(
        verb in normalized for verb in ("INSERT INTO ", "UPDATE ", "DELETE FROM ")
    ):
        return None
    for family, relations in _D25_DML_FAMILIES:
        if any(relation in normalized for relation in relations):
            return family
    return None


def _compressed_dml_families(statements: list[str]) -> tuple[str, ...]:
    result: list[str] = []
    for statement in statements:
        family = _dml_family(statement)
        if family is not None and (not result or result[-1] != family):
            result.append(family)
    return tuple(result)


def test_expected_m4_after_image_validation_is_exact() -> None:
    coordinate = matching._DirectStageCoordinate(
        "groundloop_m4_working_claim_state",
        ("epoch_id", "claim_id"),
        (7, "claim-a"),
    )
    image = matching._DirectStageImage(coordinate, {"claim_id": "claim-a"})
    cursor = _M4FamilyCursor(((7, "claim-a", {"claim_id": "claim-a"}),))
    matching._validate_expected_document_direct_m4_images(
        cursor,  # type: ignore[arg-type]
        (image,),
        relation_name="groundloop_m4_working_claim_state",
        epoch_id=7,
    )
    assert cursor.parameters == (7,)

    cursor.rows = ((7, "claim-a", {"claim_id": "changed"}),)
    with pytest.raises(EventConflictError, match="staged row differs"):
        matching._validate_expected_document_direct_m4_images(
            cursor,  # type: ignore[arg-type]
            (image,),
            relation_name="groundloop_m4_working_claim_state",
            epoch_id=7,
        )


@pytest.mark.parametrize(
    ("images", "rows"),
    (
        (
            (
                matching._DirectStageImage(
                    matching._DirectStageCoordinate(
                        "groundloop_m4_working_claim_state",
                        ("epoch_id", "claim_id"),
                        (7, "claim-a"),
                    ),
                    {"claim_id": "claim-a"},
                ),
            ),
            (
                (7, "claim-a", {"claim_id": "claim-a"}),
                (7, "claim-extra", {"claim_id": "claim-extra"}),
            ),
        ),
        (
            (),
            ((7, "claim-pollution", {"claim_id": "claim-pollution"}),),
        ),
    ),
    ids=("extra-row", "polluted-empty-set"),
)
def test_expected_m4_after_image_validation_rejects_epoch_family_pollution(
    images: tuple[matching._DirectStageImage, ...],
    rows: tuple[tuple[object, ...], ...],
) -> None:
    cursor = _M4FamilyCursor(rows)
    with pytest.raises(EventConflictError, match="staged keys differ"):
        matching._validate_expected_document_direct_m4_images(
            cursor,  # type: ignore[arg-type]
            images,
            relation_name="groundloop_m4_working_claim_state",
            epoch_id=7,
        )


def test_expected_m4_after_image_validation_rejects_wrong_epoch() -> None:
    image = matching._DirectStageImage(
        matching._DirectStageCoordinate(
            "groundloop_m4_working_answer_state",
            ("epoch_id", "answer_version_id"),
            (8, "answer-a"),
        ),
        {"answer_version_id": "answer-a"},
    )
    with pytest.raises(EventConflictError, match="after-image changed"):
        matching._validate_expected_document_direct_m4_images(
            _M4FamilyCursor(()),  # type: ignore[arg-type]
            (image,),
            relation_name="groundloop_m4_working_answer_state",
            epoch_id=7,
        )


def _derive_empty(cursor: Any, database: Any) -> Any:
    return authorize_and_derive_matching_transition(
        cursor,
        epoch_id=database.epoch_id,
        expected_runtime_revision=1,
        resulting_revision=1,
        source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
        source_id=database.event_id,
        expected_source_identity_hash=database.payload_hash,
    )


def _working_count(connection: Any, epoch_id: int) -> int:
    row = connection.execute(
        """
        SELECT count(*)
        FROM groundloop_m5_matching_image_working
        WHERE epoch_id=%s
        """,
        (epoch_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_stage_phases_enforce_order_cursor_identity_copy_and_single_use(
    request: pytest.FixtureRequest,
) -> None:
    database = request.getfixturevalue("empty_structural_database")
    connection = database.connection
    with connection.cursor() as cursor:
        intent = _derive_empty(cursor, database)
        prepared = matching._prepare_matching_transition(cursor, intent)

        with pytest.raises(EventConflictError, match="another phase"):
            matching._stage_prepared_matching_transition_through_tier_13(
                cursor, intent, prepared
            )
        assert _working_count(connection, database.epoch_id) == 0

        matching._stage_prepared_matching_transition_through_tier_12(
            cursor, intent, prepared
        )
        assert prepared.phase is matching._MatchingPhase.STAGED_THROUGH_TIER_12
        assert _working_count(connection, database.epoch_id) == 1
        with pytest.raises(EventConflictError, match="another phase"):
            matching._stage_prepared_matching_transition_through_tier_12(
                cursor, intent, prepared
            )

        copied = copy(prepared)
        with pytest.raises(ValidationError, match="copied or replaced"):
            matching._stage_prepared_matching_transition_through_tier_13(
                cursor, intent, copied
            )
        with connection.cursor() as other_cursor:
            with pytest.raises(ValidationError, match="another cursor"):
                matching._stage_prepared_matching_transition_through_tier_13(
                    other_cursor, intent, prepared
                )

        original_plan = prepared.physical_and_logical_write_plan
        prepared.physical_and_logical_write_plan = replace(
            original_plan,
            expected_direct_m4_claim_after_images=(object(),),  # type: ignore[arg-type]
        )
        with pytest.raises(EventConflictError, match="write plan changed"):
            matching._stage_prepared_matching_transition_through_tier_13(
                cursor, intent, prepared
            )
        prepared.physical_and_logical_write_plan = original_plan

        matching._stage_prepared_matching_transition_through_tier_13(
            cursor, intent, prepared
        )
        assert prepared.phase is matching._MatchingPhase.STAGED_THROUGH_TIER_13
        matching._stage_prepared_matching_transition_through_tier_14(
            cursor, intent, prepared
        )
        assert prepared.phase is matching._MatchingPhase.STAGED
        receipt = matching._finalize_prepared_matching_transition(
            cursor, intent, prepared
        )
        assert prepared.phase is matching._MatchingPhase.CONSUMED
        assert not receipt.exact_replay


def test_live_stage_dml_uses_the_exact_d25_family_order(
    _document_withdrawal_database: Any,  # noqa: F811
) -> None:
    database = _document_withdrawal_database
    connection = database.connection
    with connection.cursor() as raw_cursor:
        cursor = _RecordingCursor(raw_cursor)
        intent = _derive_empty(cursor, database)
        prepared = matching._prepare_matching_transition(cursor, intent)
        plan = prepared.physical_and_logical_write_plan
        assert plan.observation_currency_rows
        assert plan.observation_rows
        assert plan.edge_rows
        assert plan.mask_rows
        assert plan.hall_rows
        assert plan.requirement_state_rows or plan.group_state_rows
        assert plan.claim_state_rows
        assert plan.answer_state_rows

        stage_boundary = len(cursor.statements)
        matching._stage_prepared_matching_transition(cursor, intent, prepared)

    assert _compressed_dml_families(cursor.statements[stage_boundary:]) == (
        "11a",
        "11b",
        "11c",
        "11d",
        "11e",
        "12a",
        "12b",
        "13",
        "14",
    )


def test_later_stage_rejects_post_rollback_new_transaction_before_dml(
    _document_withdrawal_database: Any,  # noqa: F811
) -> None:
    database = _document_withdrawal_database
    connection = database.connection
    raw_cursor = connection.cursor()
    cursor = _RecordingCursor(raw_cursor)
    try:
        intent = _derive_empty(cursor, database)
        prepared = matching._prepare_matching_transition(cursor, intent)
        assert prepared.physical_and_logical_write_plan.claim_state_rows
        matching._stage_prepared_matching_transition_through_tier_12(
            cursor, intent, prepared
        )

        connection.rollback()
        cursor.execute("SELECT 1")
        retry_boundary = len(cursor.statements)
        with pytest.raises(ValidationError, match="transition context changed"):
            matching._stage_prepared_matching_transition_through_tier_13(
                cursor, intent, prepared
            )
        assert not any(
            _dml_family(statement) is not None
            for statement in cursor.statements[retry_boundary:]
        )
    finally:
        raw_cursor.close()


class _RollbackDifferential(Exception):
    pass


def _stage_differential_snapshot(
    connection: Any,
    database: Any,
    *,
    split: bool,
) -> dict[str, object]:
    captured: dict[str, object] = {}
    with pytest.raises(_RollbackDifferential):
        with connection.transaction(), connection.cursor() as cursor:
            intent = _derive_empty(cursor, database)
            prepared = matching._prepare_matching_transition(cursor, intent)
            if split:
                matching._stage_prepared_matching_transition_through_tier_12(
                    cursor, intent, prepared
                )
                matching._stage_prepared_matching_transition_through_tier_13(
                    cursor, intent, prepared
                )
                matching._stage_prepared_matching_transition_through_tier_14(
                    cursor, intent, prepared
                )
            else:
                matching._stage_prepared_matching_transition(cursor, intent, prepared)
            receipt = matching._finalize_prepared_matching_transition(
                cursor, intent, prepared
            )
            captured.update(
                final_rows=matching._expected_matching_stage_rows(cursor, prepared),
                journal=tuple(
                    tuple(row)
                    for row in cursor.execute(
                        """
                        SELECT relation_name, key_preimage, first_old, final_new,
                               first_operation, last_operation, mutation_count,
                               saw_insert, saw_update, saw_delete
                        FROM pg_temp.groundloop_m5_matching_change_journal
                        ORDER BY relation_name COLLATE "C", key_preimage
                        """
                    ).fetchall()
                ),
                patch=prepared.prewrite_patch_artifact.patch,
                work=prepared.d25_contribution_work,
                persisted=tuple(
                    tuple(row)
                    for row in cursor.execute(
                        """
                        SELECT
                          (SELECT to_jsonb(artifact)
                             FROM groundloop_m5_matching_patch_artifact AS artifact
                            WHERE artifact.resulting_epoch_id=%s),
                          (SELECT to_jsonb(contribution)
                             FROM groundloop_m5_matching_work_contribution
                               AS contribution
                            WHERE contribution.epoch_id=%s
                              AND contribution.source_kind=%s
                              AND contribution.source_id=%s),
                          (SELECT to_jsonb(accumulator)
                             FROM groundloop_m5_matching_work_accumulator
                               AS accumulator
                            WHERE accumulator.epoch_id=%s)
                        """,
                        (
                            intent.resulting_epoch_id,
                            intent.resulting_epoch_id,
                            intent.source_kind.value,
                            intent.source_id,
                            intent.resulting_epoch_id,
                        ),
                    ).fetchall()
                ),
                receipt=receipt,
            )
            raise _RollbackDifferential
    assert captured
    return captured


def test_split_stages_and_retained_wrapper_are_identical_base_differential(
    _document_withdrawal_database: Any,  # noqa: F811
) -> None:
    database = _document_withdrawal_database
    wrapper = _stage_differential_snapshot(database.connection, database, split=False)
    split = _stage_differential_snapshot(database.connection, database, split=True)
    assert split == wrapper


def test_each_later_stage_validates_identity_before_its_first_dml() -> None:
    first = inspect.getsource(
        matching._stage_prepared_matching_transition_through_tier_12
    )
    assert first.count("_validate_prepared_matching_write_plan(") == 1
    assert first.index("_validate_prepared_matching_write_plan(") < first.index(
        "_stage_observation_currency_plan("
    )
    for helper_name in (
        "_stage_prepared_matching_transition_through_tier_13",
        "_stage_prepared_matching_transition_through_tier_14",
    ):
        source = inspect.getsource(getattr(matching, helper_name))
        assert "_validate_prepared_matching_write_plan(" not in source
        identity = source.index("_validate_prepared_identity(")
        direct_image = source.index("_validate_expected_document_direct_m4_images(")
        d25_stage = source.index("_stage_matching_write_plan_fragment(")
        assert identity < direct_image < d25_stage
