"""Integrated M5.3-06 overlay/Python/PostgreSQL checkpoint history."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, get_args

from psycopg import Connection

from groundloop.domain import DecisionPolicy
from groundloop.events import (
    ChunkInput,
    DeleteDocumentVersionEvent,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    ReplaceDocumentVersionEvent,
    apply_event,
)
from groundloop.incremental import MaintenanceStats
from groundloop.m5.domain import ClaimSupportKind
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    ReplaceGroupEvent,
    RetireGroupEvent,
    apply_m5_event,
)
from groundloop.m5.incremental_overlay import (
    CommittedOverlayEvent,
    M5IncrementalOverlay,
    M5OverlayApplyResult,
    M5OverlayWork,
)
from groundloop.m5.reference import (
    compute_reference_states,
    validate_claim_certificate,
    validate_group_certificate,
)
from groundloop.m5.repository import M5Repository
from groundloop.postgres.m5 import (
    M5MismatchCounts,
    M5OracleStates,
    load_m5_repository_snapshot,
    read_m5_mismatch_counts,
    read_m5_oracle_states,
    write_m5_materialized_states,
)
from groundloop.postgres.snapshot import PostgresSnapshot, load_snapshot

from ..incremental.helpers import (
    make_claim_observation,
    make_group,
    make_repository,
    make_requirement_observation,
    sha,
)

GROUP_V1 = "three-oracle-group-v1"
GROUP_V2 = "three-oracle-group-v2"
REQUIREMENT_R0 = "three-oracle-r0"
REQUIREMENT_R1 = "three-oracle-r1"
CHECKPOINT_IDS = (
    "baseline",
    "register-group",
    "insert-duplicate-alpha-document",
    "observe-selected-alpha",
    "observe-backup-alpha",
    "observe-r1-beta",
    "observe-r1-gamma",
    "delete-selected-alpha-document",
    "observe-r0-beta",
    "neutralize-remaining-alpha",
    "observe-direct-claim-support",
    "policy-only-rebind",
    "replace-group",
    "retire-successor",
    "replace-base-document",
    "replay-replace-base-document",
)


def _history_events() -> tuple[CommittedOverlayEvent, ...]:
    group = make_group(
        group_id=GROUP_V1,
        family_id="three-oracle-family",
        texts=("first requirement", "second requirement"),
        requirement_ids=(REQUIREMENT_R0, REQUIREMENT_R1),
    )
    successor = make_group(
        group_id=GROUP_V2,
        family_id=group.group_family_id,
        texts=("first successor requirement", "second successor requirement"),
        requirement_ids=("three-oracle-v2-r0", "three-oracle-v2-r1"),
        predecessors=(REQUIREMENT_R0, REQUIREMENT_R1),
        supersedes_group_id=GROUP_V1,
    )
    replace_base_document = ReplaceDocumentVersionEvent(
        event_id="three-oracle-replace-base-document",
        document_id="document-a",
        old_document_version_id="document-version-a",
        new_document_version_id="three-oracle-base-document-v2",
        content_hash=sha("three-oracle-base-document-v2"),
        chunks=(
            ChunkInput(
                "three-oracle-replacement-chunk",
                0,
                "replacement evidence",
            ),
        ),
    )
    events: tuple[CommittedOverlayEvent, ...] = (
        RegisterGroupEvent("three-oracle-register-group", group),
        InsertDocumentEvent(
            event_id="three-oracle-insert-duplicate-alpha",
            document_id="three-oracle-duplicate-document",
            document_version_id="three-oracle-duplicate-document-v1",
            content_hash=sha("three-oracle-duplicate-document-v1"),
            chunks=(
                ChunkInput(
                    "three-oracle-duplicate-alpha-chunk",
                    0,
                    "alpha evidence",
                ),
            ),
        ),
        ObserveRequirementEvent(
            "three-oracle-observe-selected-alpha",
            make_requirement_observation(
                observation_id="a-three-oracle-selected-alpha",
                requirement_id=REQUIREMENT_R0,
                chunk_id="three-oracle-duplicate-alpha-chunk",
            ),
        ),
        ObserveRequirementEvent(
            "three-oracle-observe-backup-alpha",
            make_requirement_observation(
                observation_id="z-three-oracle-backup-alpha",
                requirement_id=REQUIREMENT_R0,
                chunk_id="chunk-a",
            ),
        ),
        ObserveRequirementEvent(
            "three-oracle-observe-r1-beta",
            make_requirement_observation(
                observation_id="three-oracle-r1-beta",
                requirement_id=REQUIREMENT_R1,
                chunk_id="chunk-b",
            ),
        ),
        ObserveRequirementEvent(
            "three-oracle-observe-r1-gamma",
            make_requirement_observation(
                observation_id="three-oracle-r1-gamma",
                requirement_id=REQUIREMENT_R1,
                chunk_id="chunk-c",
            ),
        ),
        DeleteDocumentVersionEvent(
            "three-oracle-delete-selected-alpha-document",
            "three-oracle-duplicate-document-v1",
        ),
        ObserveRequirementEvent(
            "three-oracle-observe-r0-beta",
            make_requirement_observation(
                observation_id="three-oracle-r0-beta",
                requirement_id=REQUIREMENT_R0,
                chunk_id="chunk-b",
            ),
        ),
        ObserveRequirementEvent(
            "three-oracle-neutralize-remaining-alpha",
            make_requirement_observation(
                observation_id="three-oracle-neutral-alpha",
                requirement_id=REQUIREMENT_R0,
                chunk_id="chunk-a",
                scores=(0.1, 0.1, 0.8),
            ),
        ),
        ObserveEvent(
            "three-oracle-observe-direct-claim-support",
            make_claim_observation(
                observation_id="three-oracle-direct-claim-support",
                chunk_id="chunk-c",
            ),
        ),
        PolicyChangeEvent(
            "three-oracle-policy-only-rebind",
            DecisionPolicy("three-oracle-policy-v2", 0.81, 0.8),
        ),
        ReplaceGroupEvent(
            "three-oracle-replace-group",
            old_group_version_id=GROUP_V1,
            successor=successor,
        ),
        RetireGroupEvent("three-oracle-retire-successor", GROUP_V2),
        replace_base_document,
        replace_base_document,
    )
    assert {type(event) for event in events[:-1]} == set(
        get_args(CommittedOverlayEvent)
    )
    return events


def _apply_committed_event(
    repository: M5Repository,
    overlay: M5IncrementalOverlay,
    event: CommittedOverlayEvent,
) -> M5OverlayApplyResult:
    before = deepcopy(repository)
    if isinstance(
        event,
        (
            RegisterGroupEvent,
            ReplaceGroupEvent,
            RetireGroupEvent,
            ObserveRequirementEvent,
        ),
    ):
        apply_m5_event(repository, event)
    else:
        apply_event(repository.base, event)
        _ = repository.current_point
    return overlay.apply_committed_event(event, before, repository)


def _oracle_states(overlay: M5IncrementalOverlay) -> M5OracleStates:
    return M5OracleStates(
        requirements=overlay.requirement_states,
        groups=overlay.group_states,
        claims=overlay.claim_states,
        answers=overlay.answer_states,
    )


def _assert_python_oracles(
    repository: M5Repository,
    overlay: M5IncrementalOverlay,
    checkpoint: str,
) -> M5OracleStates:
    reference = compute_reference_states(repository)
    expected = M5OracleStates(
        requirements=reference.requirements,
        groups=reference.groups,
        claims=reference.claims,
        answers=reference.answers,
    )
    maintained = _oracle_states(overlay)
    assert maintained == expected, checkpoint
    assert overlay.point == repository.current_point, checkpoint
    assert overlay.matching_audit_issues() == {}, checkpoint

    complete_groups = {
        group_id for group_id, state in reference.groups.items() if state.complete
    }
    assert set(overlay.group_certificates) == complete_groups, checkpoint
    assert all(
        validate_group_certificate(repository, certificate)
        for certificate in overlay.group_certificates.values()
    ), checkpoint
    assert set(overlay.claim_certificates) == set(reference.claims), checkpoint
    assert all(
        validate_claim_certificate(
            repository,
            certificate,
            overlay.group_certificates,
        )
        for certificate in overlay.claim_certificates.values()
    ), checkpoint
    return maintained


def _assert_postgres_checkpoint(
    connection: Connection[Any],
    repository: M5Repository,
    overlay: M5IncrementalOverlay,
    maintained: M5OracleStates,
    checkpoint: str,
) -> None:
    point = repository.current_point
    base_snapshot = PostgresSnapshot.capture(repository.base, overlay.direct_engine)
    assert base_snapshot.revision == repository.base.current_epoch, checkpoint
    load_snapshot(connection, base_snapshot)
    load_m5_repository_snapshot(connection, repository.export_snapshot())

    sql_before_materialization = read_m5_oracle_states(connection)
    assert sql_before_materialization == maintained, checkpoint

    write_m5_materialized_states(
        connection,
        states=maintained,
        decision_policy_version=repository.base.current_policy().policy_version,
        epoch_id=point.epoch_id,
        revision=point.revision,
        group_certificates=overlay.group_certificates,
        claim_certificates=overlay.claim_certificates,
        publish=False,
    )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

    assert read_m5_mismatch_counts(connection) == M5MismatchCounts(
        0, 0, 0, 0, 0, 0, 0
    ), checkpoint


def test_incremental_python_sql_agree_after_every_required_event(
    m5_connection: Connection[Any],
) -> None:
    repository = make_repository()
    overlay = M5IncrementalOverlay.from_repository(repository)
    events = _history_events()

    for index, checkpoint in enumerate(CHECKPOINT_IDS):
        if index:
            _apply_committed_event(repository, overlay, events[index - 1])
        maintained = _assert_python_oracles(repository, overlay, checkpoint)
        with m5_connection.transaction(force_rollback=True):
            _assert_postgres_checkpoint(
                m5_connection,
                repository,
                overlay,
                maintained,
                checkpoint,
            )


@dataclass(frozen=True, slots=True)
class _TransitionImage:
    states: M5OracleStates
    group_certificate_digests: dict[str, str]
    claim_certificate_digests: dict[str, str]
    claim_support_kinds: dict[str, ClaimSupportKind]


def _transition_image(overlay: M5IncrementalOverlay) -> _TransitionImage:
    return _TransitionImage(
        states=_oracle_states(overlay),
        group_certificate_digests={
            key: value.certificate_digest
            for key, value in overlay.group_certificates.items()
        },
        claim_certificate_digests={
            key: value.certificate_digest
            for key, value in overlay.claim_certificates.items()
        },
        claim_support_kinds={
            key: value.support_kind for key, value in overlay.claim_certificates.items()
        },
    )


def _assert_emitted_bindings(
    result: M5OverlayApplyResult,
    image: _TransitionImage,
    *,
    group_ids: tuple[str, ...],
    claim_ids: tuple[str, ...],
) -> None:
    assert (
        tuple(binding.group_version_id for binding in result.published_group_bindings)
        == group_ids
    )
    assert tuple(binding.claim_id for binding in result.published_claim_bindings) == (
        claim_ids
    )
    for group_binding in result.published_group_bindings:
        assert group_binding.epoch_id == result.point.epoch_id
        assert group_binding.valid_from_revision == result.point.revision
        assert group_binding.valid_to_revision is None
        assert (
            group_binding.certificate_digest
            == image.group_certificate_digests[group_binding.group_version_id]
        )
    for claim_binding in result.published_claim_bindings:
        assert claim_binding.epoch_id == result.point.epoch_id
        assert claim_binding.valid_from_revision == result.point.revision
        assert claim_binding.valid_to_revision is None
        assert (
            claim_binding.certificate_digest
            == image.claim_certificate_digests[claim_binding.claim_id]
        )


def _replay_audit_image(
    repository: M5Repository,
    overlay: M5IncrementalOverlay,
) -> tuple[object, ...]:
    return (
        repository.export_snapshot(),
        overlay.point,
        overlay.state_revision,
        _transition_image(overlay),
        overlay.group_certificate_artifacts_by_digest,
        overlay.claim_certificate_artifacts_by_digest,
        {
            group_id: overlay.group_binding_history(group_id)
            for group_id in overlay._group_binding_history
        },
        {
            claim_id: overlay.claim_binding_history(claim_id)
            for claim_id in overlay._claim_binding_history
        },
        dict(overlay._processed_events),
    )


def test_three_oracle_history_exercises_certificate_only_and_replay_surfaces() -> None:
    repository = make_repository()
    overlay = M5IncrementalOverlay.from_repository(repository)
    events = _history_events()
    results: list[M5OverlayApplyResult] = []
    images = [_transition_image(overlay)]

    for event in events[:-1]:
        results.append(_apply_committed_event(repository, overlay, event))
        images.append(_transition_image(overlay))
        _assert_python_oracles(repository, overlay, event.event_id)

    provenance_repair = results[6]
    assert provenance_repair.work.r == 1
    assert provenance_repair.work.y == 0
    assert provenance_repair.deltas == ()
    assert provenance_repair.changed_group_ids == (GROUP_V1,)
    assert provenance_repair.changed_claim_ids == ("claim-a",)
    assert provenance_repair.certificate_only_group_ids == (GROUP_V1,)
    assert provenance_repair.certificate_only_claim_ids == ("claim-a",)
    _assert_emitted_bindings(
        provenance_repair,
        images[7],
        group_ids=(GROUP_V1,),
        claim_ids=("claim-a",),
    )
    assert images[6].states.groups == images[7].states.groups
    assert images[6].states.claims == images[7].states.claims
    assert images[6].states.answers == images[7].states.answers
    assert images[6].group_certificate_digests != images[7].group_certificate_digests
    assert images[6].claim_certificate_digests != images[7].claim_certificate_digests

    alternate_cover_rebuild = results[8]
    assert alternate_cover_rebuild.work.r == 0
    assert alternate_cover_rebuild.work.y == 1
    assert alternate_cover_rebuild.deltas == ()
    assert alternate_cover_rebuild.changed_group_ids == (GROUP_V1,)
    assert alternate_cover_rebuild.changed_claim_ids == ("claim-a",)
    assert alternate_cover_rebuild.certificate_only_group_ids == (GROUP_V1,)
    assert alternate_cover_rebuild.certificate_only_claim_ids == ("claim-a",)
    _assert_emitted_bindings(
        alternate_cover_rebuild,
        images[9],
        group_ids=(GROUP_V1,),
        claim_ids=("claim-a",),
    )
    assert images[8].states.groups == images[9].states.groups
    assert images[8].states.claims == images[9].states.claims
    assert images[8].states.answers == images[9].states.answers
    assert images[8].group_certificate_digests != images[9].group_certificate_digests
    assert images[8].claim_certificate_digests != images[9].claim_certificate_digests

    policy_rebind = results[10]
    assert policy_rebind.work.p > 0
    assert policy_rebind.work.policy_candidates == 0
    assert policy_rebind.deltas == ()
    assert policy_rebind.changed_group_ids == (GROUP_V1,)
    assert policy_rebind.changed_claim_ids == ("claim-a",)
    assert policy_rebind.certificate_only_group_ids == (GROUP_V1,)
    assert policy_rebind.certificate_only_claim_ids == ("claim-a",)
    _assert_emitted_bindings(
        policy_rebind,
        images[11],
        group_ids=(GROUP_V1,),
        claim_ids=("claim-a",),
    )
    assert images[10].states == images[11].states
    assert images[10].group_certificate_digests != images[11].group_certificate_digests
    assert images[10].claim_certificate_digests != images[11].claim_certificate_digests

    direct_preference = results[9]
    assert direct_preference.deltas == ()
    assert direct_preference.changed_claim_ids == ("claim-a",)
    _assert_emitted_bindings(
        direct_preference,
        images[10],
        group_ids=(),
        claim_ids=("claim-a",),
    )
    assert images[9].claim_support_kinds["claim-a"] is ClaimSupportKind.GROUP
    assert images[10].claim_support_kinds["claim-a"] is ClaimSupportKind.DIRECT

    assert results[11].deltas == ()
    assert results[12].deltas == ()
    assert {delta.object_type for delta in results[13].deltas} == {"claim", "answer"}

    before_replay = _replay_audit_image(repository, overlay)
    score_index = overlay._score_index
    direct_score_index = overlay.direct_engine._score_index
    replay = _apply_committed_event(repository, overlay, events[-1])

    assert replay.replayed
    assert replay.deltas == results[13].deltas
    assert replay.logical_output_digest == results[13].logical_output_digest
    assert replay.published_group_bindings == ()
    assert replay.published_claim_bindings == ()
    assert replay.work == M5OverlayWork()
    assert replay.direct_stats == MaintenanceStats()
    assert _replay_audit_image(repository, overlay) == before_replay
    assert overlay._score_index is score_index
    assert overlay.direct_engine._score_index is direct_score_index
