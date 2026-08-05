from __future__ import annotations

from copy import deepcopy

import pytest

from groundloop.domain import DecisionPolicy
from groundloop.errors import ValidationError
from groundloop.events import (
    ChunkInput,
    InsertDocumentEvent,
    PolicyChangeEvent,
    apply_event,
)
from groundloop.m5 import incremental_overlay as overlay_module
from groundloop.m5.claim_certificates import validate_claim_binding_history
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    RetireGroupEvent,
    apply_m5_event,
)
from groundloop.m5.repository import M5Repository

from .helpers import make_group, make_repository, make_requirement_observation, sha


def _complete_repairable_repository() -> M5Repository:
    repository = make_repository()
    apply_event(
        repository.base,
        InsertDocumentEvent(
            event_id="insert-duplicate-document",
            document_id="duplicate-document",
            document_version_id="duplicate-document-v1",
            content_hash=sha("duplicate-document-v1"),
            chunks=(ChunkInput("chunk-duplicate", 0, "alpha evidence"),),
        ),
    )
    apply_m5_event(
        repository,
        RegisterGroupEvent(event_id="register-group", group=make_group()),
    )
    for observation_id, chunk_id in (
        ("a-selected", "chunk-a"),
        ("b-alternative", "chunk-duplicate"),
    ):
        apply_m5_event(
            repository,
            ObserveRequirementEvent(
                event_id=f"observe-{observation_id}",
                observation=make_requirement_observation(
                    observation_id=observation_id,
                    requirement_id="group-a-requirement-0",
                    chunk_id=chunk_id,
                ),
            ),
        )
    return repository


def _apply_m5(
    repository: M5Repository,
    overlay: overlay_module.M5IncrementalOverlay,
    event: ObserveRequirementEvent | RetireGroupEvent,
) -> overlay_module.M5OverlayApplyResult:
    before = deepcopy(repository)
    apply_m5_event(repository, event)
    return overlay.apply_committed_event(event, before, repository)


def _assert_all_history_artifacts_resolve(
    overlay: overlay_module.M5IncrementalOverlay,
) -> None:
    group_ledger = overlay.group_certificate_artifacts_by_digest
    claim_ledger = overlay.claim_certificate_artifacts_by_digest
    assert all(
        key == artifact.certificate_digest for key, artifact in group_ledger.items()
    )
    assert all(
        key == artifact.certificate_digest for key, artifact in claim_ledger.items()
    )

    for group_id in overlay._group_binding_history:
        for binding in overlay.group_binding_history(group_id):
            artifact = group_ledger[binding.certificate_digest]
            assert artifact.group_version_id == binding.group_version_id == group_id
    claim_rows = tuple(
        binding
        for claim_id in overlay._claim_binding_history
        for binding in overlay.claim_binding_history(claim_id)
    )
    assert validate_claim_binding_history(claim_rows, claim_ledger) == ()

    for group_id, group_artifact in overlay.group_certificates.items():
        group_binding = overlay._group_bindings[group_id]
        assert group_binding.certificate_digest == group_artifact.certificate_digest
        assert group_ledger[group_binding.certificate_digest] == group_artifact
    for claim_id, claim_artifact in overlay.claim_certificates.items():
        claim_binding = overlay._claim_bindings[claim_id]
        assert claim_binding.certificate_digest == claim_artifact.certificate_digest
        assert claim_ledger[claim_binding.certificate_digest] == claim_artifact


def test_artifact_ledgers_preserve_repair_rebind_and_retirement_history() -> None:
    repository = _complete_repairable_repository()
    overlay = overlay_module.M5IncrementalOverlay.from_repository(repository)
    initial_group_digest = overlay.group_certificates["group-a"].certificate_digest
    initial_claim_digest = overlay.claim_certificates["claim-a"].certificate_digest

    repair = ObserveRequirementEvent(
        event_id="repair-selected-observation",
        observation=make_requirement_observation(
            observation_id="z-replacement",
            requirement_id="group-a-requirement-0",
            chunk_id="chunk-a",
        ),
    )
    repair_result = _apply_m5(repository, overlay, repair)
    repaired_group_digest = overlay.group_certificates["group-a"].certificate_digest
    repaired_claim_digest = overlay.claim_certificates["claim-a"].certificate_digest
    assert repair_result.work.matching.certificate_repairs == 1
    assert repaired_group_digest != initial_group_digest
    assert repaired_claim_digest != initial_claim_digest
    assert set(overlay.group_certificate_artifacts_by_digest) == {
        initial_group_digest,
        repaired_group_digest,
    }
    assert set(overlay.claim_certificate_artifacts_by_digest) == {
        initial_claim_digest,
        repaired_claim_digest,
    }
    _assert_all_history_artifacts_resolve(overlay)

    policy = PolicyChangeEvent(
        event_id="policy-rebind",
        policy=DecisionPolicy("policy-v2", 0.8, 0.8),
    )
    before_policy = deepcopy(repository)
    apply_event(repository.base, policy)
    _ = repository.current_point
    policy_patch = overlay.prepare_committed_event_patch(
        policy,
        before_policy,
        repository,
    )
    assert len(policy_patch.group_artifact_ledger_changes) == 1
    assert len(policy_patch.claim_artifact_ledger_changes) == 1
    policy_result = overlay.apply_prepared_event(policy_patch)
    rebound_group_digest = overlay.group_certificates["group-a"].certificate_digest
    rebound_claim_digest = overlay.claim_certificates["claim-a"].certificate_digest
    assert rebound_group_digest not in {initial_group_digest, repaired_group_digest}
    assert rebound_claim_digest not in {initial_claim_digest, repaired_claim_digest}
    group_ledger_before_replay = overlay.group_certificate_artifacts_by_digest
    claim_ledger_before_replay = overlay.claim_certificate_artifacts_by_digest
    group_history_before_replay = overlay._group_binding_history["group-a"]
    claim_history_before_replay = overlay._claim_binding_history["claim-a"]
    _assert_all_history_artifacts_resolve(overlay)

    replay_patch = overlay.prepare_committed_event_patch(policy, repository, repository)
    assert replay_patch.group_artifact_ledger_changes == ()
    assert replay_patch.claim_artifact_ledger_changes == ()
    emitted_stages: list[str] = []
    replay = overlay.apply_prepared_event(
        replay_patch,
        failure_injector=emitted_stages.append,
    )
    assert replay.replayed
    assert replay_patch._state == "applied"
    assert replay.changed_group_ids == policy_result.changed_group_ids
    assert replay.changed_claim_ids == policy_result.changed_claim_ids
    assert replay.certificate_only_group_ids == policy_result.certificate_only_group_ids
    assert replay.certificate_only_claim_ids == policy_result.certificate_only_claim_ids
    assert replay.work == overlay_module.M5OverlayWork()
    assert emitted_stages == []
    assert overlay.group_certificate_artifacts_by_digest == group_ledger_before_replay
    assert overlay.claim_certificate_artifacts_by_digest == claim_ledger_before_replay
    assert overlay._group_binding_history["group-a"] is group_history_before_replay
    assert overlay._claim_binding_history["claim-a"] is claim_history_before_replay

    retire = RetireGroupEvent(event_id="retire-group", group_version_id="group-a")
    _apply_m5(repository, overlay, retire)
    assert "group-a" not in overlay.group_certificates
    assert "group-a" not in overlay._group_bindings
    assert overlay.group_certificate_artifacts_by_digest == group_ledger_before_replay
    assert rebound_group_digest in overlay.group_certificate_artifacts_by_digest
    assert rebound_claim_digest in overlay.claim_certificate_artifacts_by_digest
    assert len(overlay.claim_certificate_artifacts_by_digest) == 4
    _assert_all_history_artifacts_resolve(overlay)


def test_prepared_patch_cannot_remove_an_immutable_ledger_artifact() -> None:
    repository = _complete_repairable_repository()
    overlay = overlay_module.M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    event = PolicyChangeEvent(
        event_id="policy-rebind",
        policy=DecisionPolicy("policy-v2", 0.8, 0.8),
    )
    apply_event(repository.base, event)
    _ = repository.current_point
    patch = overlay.prepare_committed_event_patch(event, before, repository)
    group_ledger = overlay.group_certificate_artifacts_by_digest
    digest, artifact = next(iter(group_ledger.items()))
    patch.group_artifact_ledger_changes = (
        overlay_module._PointChange(digest, artifact, None),
    )

    with pytest.raises(ValidationError, match="must be append-only"):
        overlay.apply_prepared_event(patch)

    assert patch._state == "prepared"
    assert overlay.group_certificate_artifacts_by_digest == group_ledger
