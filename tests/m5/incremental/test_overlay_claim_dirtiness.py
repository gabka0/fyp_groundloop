from __future__ import annotations

import ast
import inspect
import textwrap
from copy import deepcopy

import pytest

from groundloop.domain import DecisionPolicy
from groundloop.events import (
    ChunkInput,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    apply_event,
)
from groundloop.m5 import incremental_overlay as overlay_module
from groundloop.m5.claim_certificates import (
    ClaimCertificateTransition,
    WorkingClaimCertificateBinding,
    transition_claim_certificate_for_selected_support,
)
from groundloop.m5.domain import (
    ClaimCertificateArtifact,
    CombinedClaimState,
    GroupMatchingCertificateArtifact,
    SnapshotPoint,
)
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    RetireGroupEvent,
    apply_m5_event,
)
from groundloop.m5.repository import M5Repository

from .helpers import (
    make_claim_observation,
    make_group,
    make_repository,
    make_requirement_observation,
    sha,
)


def _repair_ready_repository(
    group_ids: tuple[str, ...],
    *,
    direct_support: bool = False,
) -> M5Repository:
    repository = make_repository()
    apply_event(
        repository.base,
        InsertDocumentEvent(
            event_id="duplicate-document",
            document_id="duplicate-document",
            document_version_id="duplicate-document-v1",
            content_hash=sha("duplicate-document-v1"),
            chunks=(ChunkInput("chunk-duplicate", 0, "alpha evidence"),),
        ),
    )
    if direct_support:
        apply_event(
            repository.base,
            ObserveEvent(
                event_id="observe-direct-support",
                observation=make_claim_observation(
                    observation_id="direct-support",
                ),
            ),
        )
    for group_id in group_ids:
        requirement_id = f"{group_id}-requirement"
        apply_m5_event(
            repository,
            RegisterGroupEvent(
                event_id=f"register-{group_id}",
                group=make_group(
                    group_id=group_id,
                    family_id=f"{group_id}-family",
                    texts=(f"{group_id} requirement",),
                    requirement_ids=(requirement_id,),
                ),
            ),
        )
        for prefix, chunk_id in (
            ("a-selected", "chunk-a"),
            ("b-alternative", "chunk-duplicate"),
        ):
            apply_m5_event(
                repository,
                ObserveRequirementEvent(
                    event_id=f"observe-{prefix}-{group_id}",
                    observation=make_requirement_observation(
                        observation_id=f"{prefix}-{group_id}",
                        requirement_id=requirement_id,
                        chunk_id=chunk_id,
                    ),
                ),
            )
    return repository


def _prepare_same_epoch_repair(
    repository: M5Repository,
    overlay: overlay_module.M5IncrementalOverlay,
    group_id: str,
) -> overlay_module.PreparedM5OverlayPatch:
    before = deepcopy(repository)
    after = deepcopy(repository)
    event = ObserveRequirementEvent(
        event_id=f"repair-{group_id}",
        observation=make_requirement_observation(
            observation_id=f"z-replacement-{group_id}",
            requirement_id=f"{group_id}-requirement",
            chunk_id="chunk-a",
        ),
    )
    point = after.advance_semantic_revision()
    after.register_requirement_observation(event.observation, point)
    return overlay.prepare_committed_event_patch(event, before, after)


def test_nonselected_group_repair_is_claim_inert_with_many_alternatives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group_ids = (
        "a-group",
        *(f"m-{index:02d}-group" for index in range(16)),
        "z-group",
    )
    repository = _repair_ready_repository(group_ids)
    overlay = overlay_module.M5IncrementalOverlay.from_repository(repository)

    def reject_claim_transition(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("nonselected repair reached claim-certificate work")

    monkeypatch.setattr(
        overlay_module,
        "transition_claim_certificate_for_selected_support",
        reject_claim_transition,
    )
    patch = _prepare_same_epoch_repair(repository, overlay, "z-group")

    assert tuple(change.key for change in patch.group_artifact_changes) == ("z-group",)
    assert patch.claim_state_changes == ()
    assert patch.claim_artifact_changes == ()
    assert patch.claim_binding_changes == ()
    assert patch.claim_history_changes == ()
    assert patch.claim_binding_rows == ()
    assert patch.result.changed_claim_ids == ()
    assert patch.result.certificate_only_claim_ids == ()
    assert patch.result.work.claims == 0


def test_selected_group_repair_republishes_only_claim_certificate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _repair_ready_repository(("a-group", "z-group"))
    overlay = overlay_module.M5IncrementalOverlay.from_repository(repository)
    calls: list[tuple[str, str | None]] = []

    def reject_exhaustive_transition(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("measured event used exhaustive claim transition")

    def record_hot_transition(
        state: CombinedClaimState,
        *,
        point: SnapshotPoint,
        decision_policy_version: str,
        selected_group_certificate: GroupMatchingCertificateArtifact | None,
        prior_binding: WorkingClaimCertificateBinding | None,
        prior_artifact: ClaimCertificateArtifact | None,
    ) -> ClaimCertificateTransition:
        calls.append(
            (
                state.claim_id,
                (
                    selected_group_certificate.group_version_id
                    if selected_group_certificate
                    else None
                ),
            )
        )
        return transition_claim_certificate_for_selected_support(
            state,
            point=point,
            decision_policy_version=decision_policy_version,
            selected_group_certificate=selected_group_certificate,
            prior_binding=prior_binding,
            prior_artifact=prior_artifact,
        )

    monkeypatch.setattr(
        overlay_module,
        "transition_claim_certificate",
        reject_exhaustive_transition,
    )
    monkeypatch.setattr(
        overlay_module,
        "transition_claim_certificate_for_selected_support",
        record_hot_transition,
    )
    patch = _prepare_same_epoch_repair(repository, overlay, "a-group")

    assert calls == [("claim-a", "a-group")]
    assert patch.claim_state_changes == ()
    assert tuple(change.key for change in patch.claim_artifact_changes) == ("claim-a",)
    assert tuple(change.key for change in patch.claim_binding_changes) == ("claim-a",)
    assert tuple(change.key for change in patch.claim_history_changes) == ("claim-a",)
    assert tuple(binding.open for binding in patch.group_binding_rows) == (False, True)
    assert tuple(binding.open for binding in patch.claim_binding_rows) == (False, True)
    assert all(
        binding.valid_to_revision == patch.point.revision
        for binding in (
            patch.group_binding_rows[0],
            patch.claim_binding_rows[0],
        )
    )
    assert all(
        binding.valid_from_revision == patch.point.revision
        for binding in (
            patch.group_binding_rows[1],
            patch.claim_binding_rows[1],
        )
    )
    assert patch.result.changed_claim_ids == ("claim-a",)
    assert patch.result.certificate_only_claim_ids == ("claim-a",)
    assert patch.result.changed_answer_ids == ()
    assert patch.result.deltas == ()
    assert patch.result.work.claims == 1


def test_direct_selected_claim_ignores_group_certificate_repair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _repair_ready_repository(("a-group",), direct_support=True)
    overlay = overlay_module.M5IncrementalOverlay.from_repository(repository)

    def reject_claim_transition(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("DIRECT-selected claim reached group-driven claim work")

    monkeypatch.setattr(
        overlay_module,
        "transition_claim_certificate_for_selected_support",
        reject_claim_transition,
    )
    patch = _prepare_same_epoch_repair(repository, overlay, "a-group")

    assert tuple(change.key for change in patch.group_artifact_changes) == ("a-group",)
    assert patch.claim_state_changes == ()
    assert patch.claim_artifact_changes == ()
    assert patch.claim_binding_changes == ()
    assert patch.claim_history_changes == ()
    assert patch.result.changed_claim_ids == ()
    assert patch.result.work.claims == 0


def test_complete_group_id_change_publishes_constant_status_claim_state() -> None:
    repository = _repair_ready_repository(("a-group", "z-group"))
    overlay = overlay_module.M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    after = deepcopy(repository)
    event = RetireGroupEvent(event_id="retire-z-group", group_version_id="z-group")
    apply_m5_event(after, event)

    patch = overlay.prepare_committed_event_patch(event, before, after)

    assert tuple(change.key for change in patch.claim_state_changes) == ("claim-a",)
    state_change = patch.claim_state_changes[0]
    assert state_change.before is not None and state_change.after is not None
    assert state_change.before.complete_group_ids == ("a-group", "z-group")
    assert state_change.after.complete_group_ids == ("a-group",)
    assert state_change.before.status is state_change.after.status
    assert patch.result.state_only_claim_ids == ("claim-a",)
    assert patch.result.changed_claim_ids == ("claim-a",)
    assert patch.claim_artifact_changes == ()
    assert patch.claim_binding_changes == ()
    assert patch.claim_history_changes == ()
    assert patch.result.changed_answer_ids == ()
    assert patch.result.deltas == ()


def test_selected_group_retirement_reselects_without_status_delta() -> None:
    repository = _repair_ready_repository(("a-group", "z-group"))
    overlay = overlay_module.M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    after = deepcopy(repository)
    event = RetireGroupEvent(event_id="retire-a-group", group_version_id="a-group")
    apply_m5_event(after, event)

    patch = overlay.prepare_committed_event_patch(event, before, after)

    assert tuple(change.key for change in patch.claim_state_changes) == ("claim-a",)
    assert tuple(change.key for change in patch.claim_artifact_changes) == ("claim-a",)
    artifact_change = patch.claim_artifact_changes[0]
    assert artifact_change.before is not None and artifact_change.after is not None
    assert artifact_change.before.group_version_id == "a-group"
    assert artifact_change.after.group_version_id == "z-group"
    assert patch.result.state_only_claim_ids == ("claim-a",)
    assert patch.result.changed_claim_ids == ("claim-a",)
    assert patch.result.certificate_only_claim_ids == ()
    assert patch.result.changed_answer_ids == ()
    assert patch.result.deltas == ()


def test_zero_flip_policy_rebind_skips_unchanged_claim_state_recomputation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = make_repository(claim_ids=("claim-a", "claim-b", "claim-c"))
    apply_m5_event(
        repository,
        RegisterGroupEvent(
            event_id="register-claim-a-group",
            group=make_group(
                group_id="claim-a-group",
                family_id="claim-a-family",
                claim_id="claim-a",
                requirement_ids=("claim-a-requirement",),
            ),
        ),
    )
    apply_m5_event(
        repository,
        ObserveRequirementEvent(
            event_id="observe-claim-a-requirement",
            observation=make_requirement_observation(
                observation_id="claim-a-requirement-support",
                requirement_id="claim-a-requirement",
            ),
        ),
    )
    apply_event(
        repository.base,
        ObserveEvent(
            event_id="observe-claim-b-direct-support",
            observation=make_claim_observation(
                observation_id="claim-b-direct-support",
                claim_id="claim-b",
            ),
        ),
    )
    overlay = overlay_module.M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    after = deepcopy(repository)
    event = PolicyChangeEvent(
        event_id="zero-flip-policy-rebind",
        policy=DecisionPolicy("policy-v2", 0.8, 0.8),
    )
    apply_event(after.base, event)
    _ = after.current_point

    def reject_claim_state_recomputation(*_args: object) -> None:
        raise AssertionError("zero-flip policy recomputed unchanged claim state")

    monkeypatch.setattr(
        overlay_module.M5IncrementalOverlay,
        "_combined_claim_from_parts",
        staticmethod(reject_claim_state_recomputation),
    )
    patch = overlay.prepare_committed_event_patch(event, before, after)

    assert patch.claim_state_changes == ()
    assert tuple(change.key for change in patch.claim_artifact_changes) == (
        "claim-a",
        "claim-b",
        "claim-c",
    )
    assert patch.result.changed_claim_ids == ("claim-a", "claim-b", "claim-c")
    assert patch.result.certificate_only_claim_ids == (
        "claim-a",
        "claim-b",
        "claim-c",
    )
    assert patch.result.changed_answer_ids == ()
    assert patch.result.deltas == ()
    assert patch.result.work.claims == 3


def test_claim_selector_helpers_do_not_scan_alternative_groups() -> None:
    forbidden = (
        ast.For,
        ast.While,
        ast.ListComp,
        ast.SetComp,
        ast.DictComp,
        ast.GeneratorExp,
    )
    for function in (
        overlay_module._selected_group_certificate,
        overlay_module._claim_certificate_inputs_changed,
    ):
        source = textwrap.dedent(inspect.getsource(function))
        tree = ast.parse(source)
        assert not any(isinstance(node, forbidden) for node in ast.walk(tree))
        assert not any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in {"set", "sorted"}
            for node in ast.walk(tree)
        )
