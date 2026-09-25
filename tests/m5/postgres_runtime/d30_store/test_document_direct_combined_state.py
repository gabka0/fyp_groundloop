"""Lane-R direct-plus-group document withdrawal falsifiers."""

from __future__ import annotations

import inspect
from collections import Counter
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from groundloop.domain import (
    AnswerStatus,
    ClaimStatus,
    DecisionPolicy,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
    normalized_text_hash,
)
from groundloop.errors import EventConflictError, ValidationError
from groundloop.m5.domain import (
    ClaimCertificateArtifact,
    ClaimSupportKind,
    CombinedAnswerState,
)
from groundloop.m5.runtime import postgres_matching as matching
from groundloop.m5.runtime.contracts import M5PersistedMatchingSourceKind
from tests.m5.postgres_runtime.d25_store_core.conftest import (
    authorize_and_derive_matching_transition,
)
from tests.m5.postgres_runtime.d25_store_core.conftest import (
    document_withdrawal_planning_database as _document_withdrawal_database,  # noqa: F401
)

_POLICY = DecisionPolicy("direct-policy-v1", 0.7, 0.7)
_MODEL = ModelStamp("model", "version", "prompt")

_D25_STAGE_RELATIONS = (
    "groundloop_m5_working_currency_history",
    "groundloop_m5_matching_image_working",
    "groundloop_m5_matching_observation_working",
    "groundloop_m5_matching_edge_working",
    "groundloop_m5_matching_hash_mask_working",
    "groundloop_m5_matching_hall_working",
    "groundloop_m5_working_requirement_state",
    "groundloop_m5_working_group_state",
    "groundloop_m5_group_certificate_artifact",
    "groundloop_m5_group_certificate_artifact_row",
    "groundloop_m5_working_group_certificate_binding",
    "groundloop_m5_working_claim_state",
    "groundloop_m5_claim_certificate_artifact",
    "groundloop_m5_working_claim_certificate_binding",
    "groundloop_m5_working_answer_state",
    "groundloop_m5_matching_change_journal",
)


class _D25BoundaryCursor:
    """Record SQL issued through one real PostgreSQL direct revalidation."""

    def __init__(self, cursor: Any) -> None:
        self.cursor = cursor
        self.statements: list[str] = []

    def execute(self, query: object, parameters: object = None) -> Any:
        self.statements.append(" ".join(str(query).split()).lower())
        return self.cursor.execute(query, parameters)


def _d25_stage_dml(statements: list[str]) -> tuple[str, ...]:
    return tuple(
        statement
        for statement in statements
        if any(
            f"{verb} {relation}" in statement
            for verb in ("insert into", "update", "delete from")
            for relation in _D25_STAGE_RELATIONS
        )
    )


def _observation(
    observation_id: str,
    *,
    claim_id: str,
    chunk_id: str,
    support: float,
    refute: float,
    neutral: float,
) -> SemanticObservation:
    return SemanticObservation(
        observation_id=observation_id,
        subject_kind=SubjectKind.CLAIM,
        subject_id=claim_id,
        chunk_version_id=chunk_id,
        task_type="verify_pair",
        support_score=support,
        refute_score=refute,
        neutral_score=neutral,
        producer=_MODEL,
        input_hash="f" * 64,
    )


def _claim_point(
    claim_id: str,
    answer_id: str,
    *,
    observations: dict[str, SemanticObservation],
    text_hashes: dict[str, str],
    support_ids: tuple[str, ...] = (),
    refute_ids: tuple[str, ...] = (),
    required: bool = True,
) -> matching._DocumentDirectClaimBefore:
    status = matching._claim_status(
        supported=bool(support_ids), refuted=bool(refute_ids)
    )
    return matching._DocumentDirectClaimBefore(
        claim_id=claim_id,
        answer_version_id=answer_id,
        required=required,
        support_count=len({text_hashes[value] for value in support_ids}),
        refute_count=len({text_hashes[value] for value in refute_ids}),
        best_support_score=max(
            (observations[value].support_score for value in support_ids),
            default=None,
        ),
        best_refute_score=max(
            (observations[value].refute_score for value in refute_ids),
            default=None,
        ),
        supporting_observation_ids=support_ids,
        refuting_observation_ids=refute_ids,
        status=status,
        certificate_digest=matching._stable_m4_digest(
            "m4-claim-certificate-v1",
            claim_id,
            support_ids[0] if support_ids else "",
            refute_ids[0] if refute_ids else "",
        ),
    )


def _answer_point(
    answer_id: str, statuses: tuple[ClaimStatus, ...]
) -> matching._DocumentDirectAnswerBefore:
    counts = Counter(statuses)
    required_count = len(statuses)
    return matching._DocumentDirectAnswerBefore(
        CombinedAnswerState(
            answer_id,
            required_count,
            counts[ClaimStatus.SUPPORTED],
            counts[ClaimStatus.UNSUPPORTED],
            counts[ClaimStatus.REFUTED],
            counts[ClaimStatus.CONFLICTED],
            matching._answer_status(counts, required_count),
        )
    )


def _direct_plan(
    *,
    claims: tuple[matching._DocumentDirectClaimBefore, ...],
    answers: tuple[matching._DocumentDirectAnswerBefore, ...],
    observations: dict[str, SemanticObservation],
    text_hashes: dict[str, str],
    deactivated_chunks: tuple[str, ...],
    policy: DecisionPolicy = _POLICY,
) -> matching._DocumentDirectPlan:
    return matching._document_direct_plan_from_points(
        claim_points=claims,
        answer_points=answers,
        observations=tuple(observations[key] for key in sorted(observations)),
        observation_text_hashes=tuple(
            (key, text_hashes[key]) for key in sorted(text_hashes)
        ),
        deactivated_chunks=deactivated_chunks,
        policy=policy,
    )


def _install_overlap_direct_support(database: Any) -> tuple[str, str, str]:
    """Add one direct support to the claim whose complete group is withdrawn."""

    connection = database.connection
    claim_id = str(database.snapshot.claim_ids[0])
    answer_id = str(database.snapshot.answer_id)
    observation_id = f"d30-overlap-direct-{database.update_kind}"
    chunk_id = str(database.deactivated_chunk_ids[0])
    task_type = "d30-overlap-direct-v1"
    input_hash = normalized_text_hash(f"input:{observation_id}")
    raw_output_hash = normalized_text_hash(f"output:{observation_id}")
    connection.execute(
        """
        INSERT INTO groundloop_semantic_observation (
            observation_id, subject_kind, subject_id, chunk_version_id,
            task_type, support_score, refute_score, neutral_score,
            model_id, model_version, prompt_version, input_hash,
            produced_epoch, raw_output_hash, eligible_for_currency
        ) VALUES (
            %s, 'claim', %s, %s, %s, 0.96, 0.02, 0.02,
            'd30-overlap-model', 'v1', 'd30-overlap-prompt', %s,
            %s, %s, true
        )
        """,
        (
            observation_id,
            claim_id,
            chunk_id,
            task_type,
            input_hash,
            database.base_epoch_id,
            raw_output_hash,
        ),
    )
    connection.execute(
        """
        INSERT INTO groundloop_observation_currency (
            subject_kind, subject_id, chunk_version_id, task_type,
            observation_id, installed_revision
        ) VALUES ('claim', %s, %s, %s, %s, %s)
        """,
        (
            claim_id,
            chunk_id,
            task_type,
            observation_id,
            database.base_revision,
        ),
    )
    connection.execute(
        """
        INSERT INTO groundloop_published_observation_currency (
            subject_kind, subject_id, chunk_version_id, task_type,
            observation_id, valid_from_epoch, valid_to_epoch
        ) VALUES ('claim', %s, %s, %s, %s, %s, NULL)
        """,
        (claim_id, chunk_id, task_type, observation_id, database.base_epoch_id),
    )

    direct_digest = matching._stable_m4_digest(
        "m4-claim-certificate-v1",
        claim_id,
        observation_id,
        "",
    )
    assert (
        connection.execute(
            """
            UPDATE groundloop_claim_state_materialized
            SET support_count=1, refute_count=0, best_support_score=0.96,
                best_refute_score=NULL, supporting_observation_ids=%s,
                refuting_observation_ids='{}'::text[], status='supported',
                updated_epoch=%s, updated_revision=%s
            WHERE claim_id=%s
            """,
            (
                [observation_id],
                database.base_epoch_id,
                database.base_revision,
                claim_id,
            ),
        ).rowcount
        == 1
    )
    assert (
        connection.execute(
            """
            UPDATE groundloop_published_claim_state
            SET support_count=1, refute_count=0, best_support_score=0.96,
                best_refute_score=NULL, supporting_observation_ids=%s,
                refuting_observation_ids='{}'::text[], status='supported',
                certificate_digest=%s
            WHERE claim_id=%s AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            """,
            (
                [observation_id],
                direct_digest,
                claim_id,
                database.base_epoch_id,
                database.base_epoch_id,
            ),
        ).rowcount
        == 1
    )
    assert (
        connection.execute(
            """
            UPDATE groundloop_claim_certificate
            SET support_observation_id=%s, refute_observation_id=NULL,
                repaired_epoch=%s, repaired_revision=%s
            WHERE claim_id=%s
            """,
            (
                observation_id,
                database.base_epoch_id,
                database.base_revision,
                claim_id,
            ),
        ).rowcount
        == 1
    )
    for relation in (
        "groundloop_answer_state_materialized",
        "groundloop_published_answer_state",
    ):
        assert (
            connection.execute(
                f"""
                UPDATE {relation}
                SET supported_count=1, unsupported_count=0,
                    refuted_count=0, conflicted_count=0, status='valid'
                WHERE answer_version_id=%s
                """,
                (answer_id,),
            ).rowcount
            == 1
        )

    artifact = ClaimCertificateArtifact(
        claim_id=claim_id,
        decision_policy_version=database.policy_version,
        support_kind=ClaimSupportKind.DIRECT,
        direct_support_observation_id=observation_id,
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_claim_certificate_artifact (
            certificate_digest, certificate_version, claim_id,
            decision_policy_version, support_kind,
            direct_support_observation_id, group_version_id,
            group_certificate_digest, direct_refute_observation_id
        ) VALUES (%s,%s,%s,%s,%s,%s,NULL,NULL,NULL)
        """,
        (
            artifact.certificate_digest,
            artifact.certificate_version,
            artifact.claim_id,
            artifact.decision_policy_version,
            artifact.support_kind.value,
            artifact.direct_support_observation_id,
        ),
    )
    assert (
        connection.execute(
            """
            UPDATE groundloop_m5_claim_state_materialized
            SET support_count=1, refute_count=0, best_support_score=0.96,
                best_refute_score=NULL, supporting_observation_ids=%s,
                refuting_observation_ids='{}'::text[], status='supported',
                certificate_digest=%s
            WHERE claim_id=%s
            """,
            ([observation_id], artifact.certificate_digest, claim_id),
        ).rowcount
        == 1
    )
    for relation in (
        "groundloop_m5_published_claim_state",
        "groundloop_m5_published_claim_certificate_binding",
    ):
        connection.execute(f"ALTER TABLE {relation} DISABLE TRIGGER ALL")
    try:
        assert (
            connection.execute(
                """
                UPDATE groundloop_m5_published_claim_state
                SET support_count=1, refute_count=0, best_support_score=0.96,
                    best_refute_score=NULL, supporting_observation_ids=%s,
                    refuting_observation_ids='{}'::text[], status='supported',
                    certificate_digest=%s
                WHERE claim_id=%s AND valid_from_epoch <= %s
                  AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
                """,
                (
                    [observation_id],
                    artifact.certificate_digest,
                    claim_id,
                    database.base_epoch_id,
                    database.base_epoch_id,
                ),
            ).rowcount
            == 1
        )
        assert (
            connection.execute(
                """
                UPDATE groundloop_m5_published_claim_certificate_binding
                SET certificate_digest=%s
                WHERE claim_id=%s AND valid_from_epoch <= %s
                  AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
                """,
                (
                    artifact.certificate_digest,
                    claim_id,
                    database.base_epoch_id,
                    database.base_epoch_id,
                ),
            ).rowcount
            == 1
        )
    finally:
        for relation in (
            "groundloop_m5_published_claim_certificate_binding",
            "groundloop_m5_published_claim_state",
        ):
            connection.execute(f"ALTER TABLE {relation} ENABLE TRIGGER ALL")
    return claim_id, answer_id, observation_id


def test_direct_withdrawal_uses_distinct_text_hash_counts_and_coalesces_answer() -> (
    None
):
    observations = {
        "a-old-support": _observation(
            "a-old-support",
            claim_id="claim-a",
            chunk_id="chunk-old",
            support=0.96,
            refute=0.01,
            neutral=0.03,
        ),
        "b-remaining-support": _observation(
            "b-remaining-support",
            claim_id="claim-a",
            chunk_id="chunk-live",
            support=0.82,
            refute=0.05,
            neutral=0.13,
        ),
        "c-old-refute": _observation(
            "c-old-refute",
            claim_id="claim-a",
            chunk_id="chunk-old",
            support=0.02,
            refute=0.94,
            neutral=0.04,
        ),
        "d-second-support": _observation(
            "d-second-support",
            claim_id="claim-b",
            chunk_id="chunk-old",
            support=0.91,
            refute=0.03,
            neutral=0.06,
        ),
    }
    duplicate_hash = normalized_text_hash("same normalized evidence")
    text_hashes = {
        "a-old-support": duplicate_hash,
        "b-remaining-support": duplicate_hash,
        "c-old-refute": normalized_text_hash("refuting evidence"),
        "d-second-support": normalized_text_hash("second claim evidence"),
    }
    claims = (
        _claim_point(
            "claim-a",
            "answer-a",
            observations=observations,
            text_hashes=text_hashes,
            support_ids=("a-old-support", "b-remaining-support"),
            refute_ids=("c-old-refute",),
        ),
        _claim_point(
            "claim-b",
            "answer-a",
            observations=observations,
            text_hashes=text_hashes,
            support_ids=("d-second-support",),
        ),
    )
    answer = _answer_point("answer-a", (ClaimStatus.CONFLICTED, ClaimStatus.SUPPORTED))

    plan = _direct_plan(
        claims=claims,
        answers=(answer,),
        observations=observations,
        text_hashes=text_hashes,
        deactivated_chunks=("chunk-old",),
    )

    assert tuple(claim.claim_id for claim in plan.claims) == (
        "claim-a",
        "claim-b",
    )
    first, second = plan.claims
    assert first.before_state.support_count == 1
    assert first.after_state.support_count == 1
    assert first.after_state.refute_count == 0
    assert first.after_state.best_support_score == 0.82
    assert first.after_state.best_refute_score is None
    assert first.after_state.supporting_observation_ids == ("b-remaining-support",)
    assert first.after_state.status is ClaimStatus.SUPPORTED
    assert first.after_certificate_digest != first.before_certificate_digest
    assert second.after_state.status is ClaimStatus.UNSUPPORTED
    assert second.after_state.support_count == 0
    assert len(plan.answers) == 1
    assert plan.answers[0].after_state.status is AnswerStatus.PARTIALLY_SUPPORTED
    assert plan.answers[0].after_state.supported_count == 1
    assert plan.answers[0].after_state.unsupported_count == 1
    assert tuple(item.observation_id for item in plan.withdrawn_observations) == (
        "a-old-support",
        "c-old-refute",
        "d-second-support",
    )
    assert tuple(item.observation_id for item in plan.remaining_observations) == (
        "b-remaining-support",
    )

    intent = SimpleNamespace(resulting_epoch_id=41, resulting_revision=1)
    claim_images = matching._document_direct_expected_claim_images(
        intent,
        plan,  # type: ignore[arg-type]
    )
    answer_images = matching._document_direct_expected_answer_images(
        intent,
        plan,  # type: ignore[arg-type]
    )
    assert len(claim_images) == 2
    assert claim_images[0].row_json == {
        "epoch_id": 41,
        "claim_id": "claim-a",
        "support_count": 1,
        "refute_count": 0,
        "best_support_score": 0.82,
        "best_refute_score": None,
        "supporting_observation_ids": ["b-remaining-support"],
        "refuting_observation_ids": [],
        "status": "supported",
        "certificate_digest": first.after_certificate_digest,
        "updated_revision": 1,
    }
    assert len(answer_images) == 1
    assert answer_images[0].row_json["status"] == "partially_supported"  # type: ignore[index]


def test_withdrawn_neutral_current_observation_is_affected_but_state_inert() -> None:
    observations = {
        "neutral-old": _observation(
            "neutral-old",
            claim_id="claim-a",
            chunk_id="chunk-old",
            support=0.1,
            refute=0.1,
            neutral=0.8,
        ),
        "support-live": _observation(
            "support-live",
            claim_id="claim-a",
            chunk_id="chunk-live",
            support=0.8,
            refute=0.1,
            neutral=0.1,
        ),
    }
    text_hashes = {key: normalized_text_hash(key) for key in observations}
    claim = _claim_point(
        "claim-a",
        "answer-a",
        observations=observations,
        text_hashes=text_hashes,
        support_ids=("support-live",),
    )
    plan = _direct_plan(
        claims=(claim,),
        answers=(_answer_point("answer-a", (ClaimStatus.SUPPORTED,)),),
        observations=observations,
        text_hashes=text_hashes,
        deactivated_chunks=("chunk-old",),
    )

    assert len(plan.claims) == 1
    assert plan.claims[0].withdrawn_observation_ids == ("neutral-old",)
    assert plan.claims[0].after_state == plan.claims[0].before_state
    assert (
        plan.claims[0].after_certificate_digest
        == plan.claims[0].before_certificate_digest
    )
    assert len(plan.answers) == 1
    assert plan.answers[0].after_state == plan.answers[0].before_state


def test_candidate_only_without_current_direct_authority_is_state_inert() -> None:
    assert (
        _direct_plan(
            claims=(),
            answers=(),
            observations={},
            text_hashes={},
            deactivated_chunks=("chunk-old",),
        )
        == matching._DocumentDirectPlan()
    )


@pytest.mark.parametrize(
    ("group_owner", "direct_claim_ids", "expected_claims", "expected_answers"),
    (
        (None, ("claim-direct",), ("claim-direct",), ("answer-direct",)),
        (
            ("claim-group", "answer-group"),
            (),
            ("claim-group",),
            ("answer-group",),
        ),
        (
            ("claim-shared", "answer-shared"),
            ("claim-shared",),
            ("claim-shared",),
            ("answer-shared",),
        ),
        (
            ("claim-group", "answer-group"),
            ("claim-direct",),
            ("claim-direct", "claim-group"),
            ("answer-direct", "answer-group"),
        ),
    ),
    ids=("direct-only", "requirement-only", "mixed-coalesced", "mixed-union"),
)
def test_document_affected_projection_unions_direct_and_group_owners(
    group_owner: tuple[str, str] | None,
    direct_claim_ids: tuple[str, ...],
    expected_claims: tuple[str, ...],
    expected_answers: tuple[str, ...],
) -> None:
    group_id = "group-a"
    group = SimpleNamespace(
        shape=SimpleNamespace(
            group_version_id=group_id,
            requirements=((0, "requirement-a"),),
        ),
        certificate_view=SimpleNamespace(selected_observations=(), candidates=()),
        hall_after=SimpleNamespace(complete=False),
    )
    preview = SimpleNamespace(
        removed_observations=(),
        edge_transitions=(),
        mask_transitions=(),
        groups=() if group_owner is None else (group,),
    )

    class _Rows:
        def __init__(self, rows: list[tuple[object, ...]]) -> None:
            self.rows = rows

        def fetchall(self) -> list[tuple[object, ...]]:
            return self.rows

    class _Cursor:
        def execute(self, query: str, parameters: object) -> _Rows:
            if "FROM groundloop_m5_group_validity" in query:
                rows = [] if group_owner is None else [(group_id, *group_owner, True)]
                return _Rows(rows)
            if "FROM groundloop_claim" in query:
                owners = {
                    "claim-direct": "answer-direct",
                    "claim-shared": "answer-shared",
                }
                return _Rows(
                    [
                        (claim_id, owners[claim_id], True)
                        for claim_id in direct_claim_ids
                    ]
                )
            if "SELECT complete_group_ids" in query:
                return _Rows([((),)])
            raise AssertionError(query)

    projected = matching._document_affected_projection(
        _Cursor(),  # type: ignore[arg-type]
        before_epoch_id=11,
        preview=preview,  # type: ignore[arg-type]
        direct_claim_ids=direct_claim_ids,
    )
    assert projected["claim_state_ids"] == expected_claims
    assert projected["claim_certificate_ids"] == expected_claims
    assert projected["answer_state_ids"] == expected_answers


def test_document_application_coalesces_one_direct_and_group_claim_change(
    _document_withdrawal_database: Any,  # noqa: F811
) -> None:
    database = _document_withdrawal_database
    connection = database.connection
    savepoint = f"d30_overlap_{database.update_kind}"
    connection.execute(f"SAVEPOINT {savepoint}")
    try:
        claim_id, answer_id, observation_id = _install_overlap_direct_support(database)
        with connection.cursor() as cursor:
            intent = authorize_and_derive_matching_transition(
                cursor,
                epoch_id=database.epoch_id,
                expected_runtime_revision=1,
                resulting_revision=1,
                source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
                source_id=database.event_id,
                expected_source_identity_hash=database.payload_hash,
            )
            claim_before = matching._document_claim_state(cursor, intent, claim_id)
            answer_before = matching._document_answer_state(cursor, intent, answer_id)
            certificate_before = matching._stored_claim_certificate_artifact(
                cursor,
                claim_before.certificate_digest,
                lock=False,
            )
            direct_plan = matching._document_direct_plan_from_database(
                cursor,
                epoch_id=database.epoch_id,
                before_epoch_id=database.base_epoch_id,
                source_id=database.event_id,
                update_kind=database.update_kind,
                decision_policy_version=database.policy_version,
                candidate_claim_ids=(claim_id,),
            )
            artifact, plan, _write_counts, _requirement_count = (
                matching._document_first_application_plan(
                    cursor,
                    intent,
                    update_kind=database.update_kind,
                    document_direct_plan=direct_plan,
                )
            )

        assert certificate_before is not None
        assert certificate_before.certificate_digest == claim_before.certificate_digest
        assert tuple(value.claim_id for value in direct_plan.claims) == (claim_id,)
        assert direct_plan.claims[0].withdrawn_observation_ids == (observation_id,)
        assert direct_plan.claims[0].after_state.status is ClaimStatus.UNSUPPORTED
        assert database.snapshot.complete_group_id in intent.group_state_ids
        assert (
            tuple(row.state.group_version_id for row in plan.group_state_rows).count(
                database.snapshot.complete_group_id
            )
            == 1
        )

        assert len(plan.claim_state_rows) == 1
        claim_write = plan.claim_state_rows[0]
        claim_after = claim_write.state
        assert claim_after.claim_id == claim_id
        assert claim_after.support_count == 0
        assert claim_after.refute_count == 0
        assert claim_after.supporting_observation_ids == ()
        assert claim_after.refuting_observation_ids == ()
        assert claim_after.complete_group_count == 0
        assert claim_after.complete_group_ids == ()
        assert claim_after.status is ClaimStatus.UNSUPPORTED

        assert len(plan.claim_certificate_artifact_rows) == 1
        certificate_after = plan.claim_certificate_artifact_rows[0]
        assert certificate_after.claim_id == claim_id
        assert certificate_after.support_kind is ClaimSupportKind.NONE
        assert claim_write.decision_policy_version == database.policy_version
        assert claim_write.decision_policy_version == intent.decision_policy_version
        assert claim_write.certificate_digest == certificate_after.certificate_digest
        assert len(plan.claim_binding_rows) == 1
        claim_binding = plan.claim_binding_rows[0]
        assert claim_binding.epoch_id == intent.resulting_epoch_id
        assert claim_binding.claim_id == claim_id
        assert claim_binding.valid_from_revision == intent.resulting_revision
        assert claim_binding.valid_to_revision is None
        assert (
            claim_binding.certificate_digest
            == claim_write.certificate_digest
            == certificate_after.certificate_digest
        )

        assert len(plan.answer_state_rows) == 1
        answer_after = plan.answer_state_rows[0]
        assert answer_after.answer_version_id == answer_id
        assert answer_after.required_claim_count == 1
        assert answer_after.supported_count == 0
        assert answer_after.unsupported_count == 1
        assert answer_after.refuted_count == 0
        assert answer_after.conflicted_count == 0
        assert answer_after.status is AnswerStatus.UNSUPPORTED

        assert len(plan.expected_direct_m4_claim_after_images) == 1
        direct_claim = direct_plan.claims[0]
        m4_claim_image = plan.expected_direct_m4_claim_after_images[0]
        assert m4_claim_image.coordinate.relation_name == (
            "groundloop_m4_working_claim_state"
        )
        assert m4_claim_image.coordinate.key_columns == ("epoch_id", "claim_id")
        assert m4_claim_image.coordinate.key_parts == (
            intent.resulting_epoch_id,
            claim_id,
        )
        assert m4_claim_image.row_json == {
            "epoch_id": intent.resulting_epoch_id,
            "claim_id": claim_id,
            "support_count": direct_claim.after_state.support_count,
            "refute_count": direct_claim.after_state.refute_count,
            "best_support_score": direct_claim.after_state.best_support_score,
            "best_refute_score": direct_claim.after_state.best_refute_score,
            "supporting_observation_ids": list(
                direct_claim.after_state.supporting_observation_ids
            ),
            "refuting_observation_ids": list(
                direct_claim.after_state.refuting_observation_ids
            ),
            "status": direct_claim.after_state.status.value,
            "certificate_digest": direct_claim.after_certificate_digest,
            "updated_revision": intent.resulting_revision,
        }

        assert len(plan.expected_direct_m4_answer_after_images) == 1
        direct_answer = direct_plan.answers[0]
        m4_answer_image = plan.expected_direct_m4_answer_after_images[0]
        assert m4_answer_image.coordinate.relation_name == (
            "groundloop_m4_working_answer_state"
        )
        assert m4_answer_image.coordinate.key_columns == (
            "epoch_id",
            "answer_version_id",
        )
        assert m4_answer_image.coordinate.key_parts == (
            intent.resulting_epoch_id,
            answer_id,
        )
        assert m4_answer_image.row_json == {
            "epoch_id": intent.resulting_epoch_id,
            "answer_version_id": answer_id,
            "required_claim_count": direct_answer.after_state.required_claim_count,
            "supported_count": direct_answer.after_state.supported_count,
            "unsupported_count": direct_answer.after_state.unsupported_count,
            "refuted_count": direct_answer.after_state.refuted_count,
            "conflicted_count": direct_answer.after_state.conflicted_count,
            "status": direct_answer.after_state.status.value,
            "updated_revision": intent.resulting_revision,
        }

        overlap_changes = tuple(
            (change.kind.value, change.object_id)
            for change in artifact.logical_patch.changes
            if change.object_id in {claim_id, answer_id}
        )
        assert overlap_changes.count(("claim_state", claim_id)) == 1
        assert overlap_changes.count(("claim_certificate", claim_id)) == 1
        assert overlap_changes.count(("answer_state", answer_id)) == 1
        assert len(overlap_changes) == 3
        changes_by_key = {
            (change.kind.value, change.object_id): change
            for change in artifact.logical_patch.changes
            if change.object_id in {claim_id, answer_id}
        }
        claim_change = changes_by_key[("claim_state", claim_id)]
        assert claim_change.before_hash == matching._claim_state_artifact_hash(
            claim_before
        )
        assert claim_change.after_hash == matching._claim_state_artifact_hash(
            claim_write
        )
        certificate_change = changes_by_key[("claim_certificate", claim_id)]
        assert certificate_change.before_hash == certificate_before.certificate_digest
        assert certificate_change.after_hash == certificate_after.certificate_digest
        answer_change = changes_by_key[("answer_state", answer_id)]
        assert answer_change.before_hash == matching._answer_state_artifact_hash(
            answer_before
        )
        assert answer_change.after_hash == matching._answer_state_artifact_hash(
            answer_after
        )
    finally:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")


@pytest.mark.parametrize(
    "corruption",
    (
        "missing-hash",
        "extra-hash",
        "wrong-owner",
        "wrong-policy",
        "stale-state",
    ),
)
def test_direct_point_closure_rejects_incomplete_or_changed_authority_before_dml(
    d30_schema: Any,
    corruption: str,
) -> None:
    connection = d30_schema.connection
    base_epoch_id = int(d30_schema.database.base.epoch_id)
    base_policy = str(d30_schema.database.base.policy_version)
    event_id = f"d30-direct-closure-{corruption}"
    savepoint = f"d30_direct_closure_{corruption.replace('-', '_')}"
    connection.execute(f"SAVEPOINT {savepoint}")
    try:
        document = connection.execute(
            """
            SELECT chunk.document_version_id
            FROM groundloop_chunk_version AS chunk
            WHERE chunk.chunk_version_id = %s
            """,
            (d30_schema.database.base.chunk_ids[0],),
        ).fetchone()
        assert document is not None
        document_version_id = str(document[0])
        source = connection.execute(
            """
            SELECT job.candidate_policy_id, policy.claim_registry_snapshot_id
            FROM groundloop_semantic_job AS job
            JOIN groundloop_candidate_policy AS policy
              USING (candidate_policy_id)
            WHERE job.epoch_id = %s
            ORDER BY job.job_id COLLATE "C"
            LIMIT 1
            """,
            (int(d30_schema.first_m4["epoch_id"]),),
        ).fetchone()
        assert source is not None
        epoch = connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (
                %s, %s, 1, 'committed', 'sealed', 'complete',
                'strict', clock_timestamp()
            ) RETURNING epoch_id
            """,
            (event_id, normalized_text_hash(event_id)),
        ).fetchone()
        assert epoch is not None
        epoch_id = int(epoch[0])
        connection.execute(
            "ALTER TABLE groundloop_m4_update "
            "DISABLE TRIGGER groundloop_m4_update_runtime_mode_guard"
        )
        try:
            connection.execute(
                """
                INSERT INTO groundloop_m4_update (
                    epoch_id, update_kind, candidate_policy_id,
                    previous_published_epoch_id, registry_snapshot_id, manifest
                ) VALUES (%s, 'delete', %s, %s, %s, '{}'::jsonb)
                """,
                (epoch_id, str(source[0]), base_epoch_id, str(source[1])),
            )
        finally:
            connection.execute(
                "ALTER TABLE groundloop_m4_update "
                "ENABLE TRIGGER groundloop_m4_update_runtime_mode_guard"
            )
        connection.execute(
            """
            INSERT INTO groundloop_m5_update (
                epoch_id, update_kind, previous_published_epoch_id,
                decision_policy_version
            ) VALUES (%s, 'document_delete', %s, %s)
            """,
            (epoch_id, base_epoch_id, base_policy),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m4_structural_deactivation (
                epoch_id, document_version_id
            ) VALUES (%s, %s)
            """,
            (epoch_id, document_version_id),
        )
        connection.execute(
            """
            UPDATE groundloop_document_version
            SET valid_to_epoch = %s
            WHERE document_version_id = %s AND valid_to_epoch IS NULL
            """,
            (epoch_id, document_version_id),
        )
        closed_chunks = connection.execute(
            """
            UPDATE groundloop_chunk_version
            SET valid_to_epoch = %s
            WHERE document_version_id = %s AND valid_to_epoch IS NULL
            RETURNING chunk_version_id
            """,
            (epoch_id, document_version_id),
        ).fetchall()
        assert closed_chunks

        candidate = connection.execute(
            """
            SELECT state.claim_id
            FROM groundloop_published_claim_state AS state
            WHERE state.valid_from_epoch <= %s
              AND (state.valid_to_epoch IS NULL OR %s < state.valid_to_epoch)
              AND (
                cardinality(state.supporting_observation_ids)
                + cardinality(state.refuting_observation_ids)
              ) > 0
            ORDER BY state.claim_id COLLATE "C"
            LIMIT 1
            """,
            (base_epoch_id, base_epoch_id),
        ).fetchone()
        assert candidate is not None
        claim_id = str(candidate[0])
        call = {
            "epoch_id": epoch_id,
            "before_epoch_id": base_epoch_id,
            "source_id": event_id,
            "update_kind": "document_delete",
            "decision_policy_version": base_policy,
            "candidate_claim_ids": (claim_id,),
        }
        with connection.cursor() as cursor:
            expected = matching._document_direct_plan_from_database(
                cursor,
                **call,  # type: ignore[arg-type]
            )
        assert expected.claims
        assert expected.withdrawn_observations
        assert expected.observation_text_hashes

        if corruption == "missing-hash":
            expected = replace(
                expected,
                observation_text_hashes=expected.observation_text_hashes[:-1],
            )
        elif corruption == "extra-hash":
            expected = replace(
                expected,
                observation_text_hashes=(
                    *expected.observation_text_hashes,
                    ("zz-d30-extra-observation", "f" * 64),
                ),
            )
        elif corruption == "wrong-owner":
            other_claim_id = next(
                value
                for value in d30_schema.database.base.claim_ids
                if value != claim_id
            )
            first, *remaining = expected.withdrawn_observations
            expected = replace(
                expected,
                withdrawn_observations=(
                    replace(first, subject_id=other_claim_id),
                    *remaining,
                ),
            )
        elif corruption == "wrong-policy":
            wrong_policy = f"{base_policy}-d30-wrong"
            connection.execute(
                """
                INSERT INTO groundloop_decision_policy (
                    policy_version, support_threshold, refute_threshold,
                    tie_rule_version, valid_from_epoch, valid_to_epoch
                ) VALUES (%s, 1.0, 1.0, 'v1', %s, %s)
                """,
                (wrong_policy, base_epoch_id, epoch_id),
            )
            call["decision_policy_version"] = wrong_policy
        else:
            assert corruption == "stale-state"
            connection.execute(
                "ALTER TABLE groundloop_published_claim_state DISABLE TRIGGER ALL"
            )
            try:
                assert (
                    connection.execute(
                        """
                        UPDATE groundloop_published_claim_state
                        SET support_count = support_count + 1
                        WHERE claim_id = %s
                          AND valid_from_epoch <= %s
                          AND (
                            valid_to_epoch IS NULL OR %s < valid_to_epoch
                          )
                        """,
                        (claim_id, base_epoch_id, base_epoch_id),
                    ).rowcount
                    == 1
                )
            finally:
                connection.execute(
                    "ALTER TABLE groundloop_published_claim_state ENABLE TRIGGER ALL"
                )

        with connection.cursor() as raw_cursor:
            cursor = _D25BoundaryCursor(raw_cursor)
            with pytest.raises(EventConflictError):
                matching._document_direct_plan_from_database(
                    cursor,  # type: ignore[arg-type]
                    expected_plan=expected,
                    **call,  # type: ignore[arg-type]
                )

        route = "\n".join(cursor.statements)
        for relation in (
            "groundloop_m4_structural_deactivation",
            "groundloop_decision_policy",
            "groundloop_published_claim_state",
            "groundloop_semantic_observation",
            "groundloop_observation_currency",
            "groundloop_published_observation_currency",
            "groundloop_published_answer_state",
        ):
            assert relation in route
        assert _d25_stage_dml(cursor.statements) == ()
    finally:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")


def test_direct_database_revalidation_is_bounded_to_named_points() -> None:
    source = inspect.getsource(matching._document_direct_plan_from_database)
    assert "WHERE claim.claim_id = %s" in source
    assert "WHERE observation.observation_id = ANY(%s)" in source
    assert "WHERE answer_version_id = %s" in source
    assert "JOIN groundloop_chunk_version AS chunk USING (chunk_version_id)" in source
    assert "normalized_text_hash(_text(row[13]))" in source
    assert "provider" not in source
    assert "model(" not in source


def test_public_derivation_has_no_document_authority_channel() -> None:
    signature = inspect.signature(matching.derive_matching_transition_intent)
    assert tuple(signature.parameters) == (
        "cursor",
        "epoch_id",
        "expected_runtime_revision",
        "resulting_revision",
        "source_kind",
        "source_id",
        "expected_source_identity_hash",
    )
    source = inspect.getsource(matching.derive_matching_transition_intent)
    assert "document_direct_authority=None" in source
    with pytest.raises(ValidationError, match="exact D29 direct authority"):
        matching._document_direct_plan_from_d29_authority(
            object(),  # type: ignore[arg-type]
            authority=object(),
            epoch_id=1,
            before_epoch_id=0,
            source_id="event",
            source_identity_hash="a" * 64,
            update_kind="document_delete",
            decision_policy_version="policy",
        )
    assert M5PersistedMatchingSourceKind.STRUCTURAL_OPEN.value == "structural_open"
