from __future__ import annotations

import ast
import inspect
import textwrap
from copy import deepcopy
from dataclasses import replace

import pytest

from groundloop.domain import AnswerStatus, ClaimStatus, DecisionPolicy
from groundloop.events import ObserveEvent, PolicyChangeEvent, apply_event
from groundloop.m5 import incremental_overlay as overlay_module
from groundloop.m5.claim_certificates import WorkingClaimCertificateBinding
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    apply_m5_event,
)
from groundloop.m5.matching import WorkingGroupCertificateBinding
from groundloop.m5.repository import M5Repository
from groundloop.repository import InMemoryRepository

from .helpers import (
    make_claim_observation,
    make_group,
    make_repository,
    make_requirement_observation,
    sha,
)


def _score_tree_image(
    node: overlay_module._ScoreNode | None,
) -> tuple[tuple[float, str], ...]:
    if node is None:
        return ()
    left = _score_tree_image(node.left)
    right = _score_tree_image(node.right)
    assert all(key < node.key for key in left)
    assert all(node.key < key for key in right)
    expected_height = 1 + max(
        overlay_module._score_height(node.left),
        overlay_module._score_height(node.right),
    )
    assert node.height == expected_height
    assert (
        abs(
            overlay_module._score_height(node.left)
            - overlay_module._score_height(node.right)
        )
        <= 1
    )
    return (*left, node.key, *right)


def test_score_index_two_child_delete_preserves_avl_and_range_membership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scores = (0.50, 0.30, 0.70, 0.20, 0.40, 0.60, 0.80)
    observations = tuple(
        make_requirement_observation(
            observation_id=f"score-{index}",
            requirement_id="group-a-requirement-0",
            scores=(support, 1.0 - support, 0.0),
        )
        for index, support in enumerate(scores)
    )
    index = overlay_module._RequirementScoreIndex()
    by_id = {observation.observation_id: observation for observation in observations}
    for observation in observations:
        index = index.add(observation)

    root = index.support
    assert root is not None and root.left is not None and root.right is not None
    original_image = _score_tree_image(root)
    expected_successor = overlay_module._score_least(root.right).key
    removed_id = root.key[1]
    original_least = overlay_module._score_least
    successor_calls = 0

    def record_successor(node: overlay_module._ScoreNode) -> overlay_module._ScoreNode:
        nonlocal successor_calls
        successor_calls += 1
        return original_least(node)

    monkeypatch.setattr(overlay_module, "_score_least", record_successor)
    updated = index.remove(by_id[removed_id])

    assert successor_calls >= 1
    assert updated.size == index.size - 1
    assert updated.support is not None
    assert updated.support.key == expected_successor
    assert updated.support.left is root.left
    assert _score_tree_image(root) == original_image
    support_image = _score_tree_image(updated.support)
    refute_image = _score_tree_image(updated.refute)
    assert {key[1] for key in support_image} == set(by_id) - {removed_id}
    assert {key[1] for key in refute_image} == set(by_id) - {removed_id}
    candidates = updated.policy_candidates(
        DecisionPolicy("range-low", 0.0, 0.0),
        DecisionPolicy("range-high", 1.0, 1.0),
    )
    assert candidates == set(by_id) - {removed_id}


def _long_group_history(
    length: int,
) -> tuple[
    overlay_module._HistoryNode[WorkingGroupCertificateBinding],
    WorkingGroupCertificateBinding,
]:
    root: overlay_module._HistoryNode[WorkingGroupCertificateBinding] | None = None
    for revision in range(length - 1):
        root = overlay_module._history_append(
            root,
            WorkingGroupCertificateBinding(
                epoch_id=1,
                group_version_id="group-a",
                valid_from_revision=revision,
                valid_to_revision=revision + 1,
                certificate_digest=sha(f"group:{revision}"),
            ),
        )
    prior = WorkingGroupCertificateBinding(
        epoch_id=1,
        group_version_id="group-a",
        valid_from_revision=length - 1,
        valid_to_revision=None,
        certificate_digest=sha("group:prior"),
    )
    root = overlay_module._history_append(root, prior)
    return root, prior


def _long_claim_history(
    length: int,
) -> tuple[
    overlay_module._HistoryNode[WorkingClaimCertificateBinding],
    WorkingClaimCertificateBinding,
]:
    root: overlay_module._HistoryNode[WorkingClaimCertificateBinding] | None = None
    for revision in range(length - 1):
        root = overlay_module._history_append(
            root,
            WorkingClaimCertificateBinding(
                epoch_id=1,
                claim_id="claim-a",
                valid_from_revision=revision,
                valid_to_revision=revision + 1,
                certificate_digest=sha(f"claim:{revision}"),
            ),
        )
    prior = WorkingClaimCertificateBinding(
        epoch_id=1,
        claim_id="claim-a",
        valid_from_revision=length - 1,
        valid_to_revision=None,
        certificate_digest=sha("claim:prior"),
    )
    root = overlay_module._history_append(root, prior)
    return root, prior


def test_long_history_append_and_carry_forward_share_the_prior_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    forbidden = (
        ast.For,
        ast.While,
        ast.ListComp,
        ast.SetComp,
        ast.DictComp,
        ast.GeneratorExp,
    )
    for transition in (
        overlay_module._history_append,
        overlay_module._history_after_group_transition,
        overlay_module._history_after_claim_transition,
    ):
        source = textwrap.dedent(inspect.getsource(transition))
        tree = ast.parse(source)
        assert not any(isinstance(node, forbidden) for node in ast.walk(tree))
        assert "_history_materialize" not in source

    group_root, prior_group = _long_group_history(4096)
    claim_root, prior_claim = _long_claim_history(4096)

    def reject_materialization(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("measured history transition materialized its chain")

    monkeypatch.setattr(
        overlay_module,
        "_history_materialize",
        reject_materialization,
    )

    carried_group = WorkingGroupCertificateBinding(
        epoch_id=2,
        group_version_id="group-a",
        valid_from_revision=0,
        valid_to_revision=None,
        certificate_digest=prior_group.certificate_digest,
    )
    next_group_root, group_rows = overlay_module._history_after_group_transition(
        group_root,
        prior_group,
        None,
        carried_group,
    )
    assert next_group_root is not None
    assert next_group_root.value == carried_group
    assert next_group_root.previous is group_root
    assert next_group_root.length == group_root.length + 1
    assert group_rows == (carried_group,)

    carried_claim = WorkingClaimCertificateBinding(
        epoch_id=2,
        claim_id="claim-a",
        valid_from_revision=0,
        valid_to_revision=None,
        certificate_digest=prior_claim.certificate_digest,
    )
    next_claim_root, claim_rows = overlay_module._history_after_claim_transition(
        claim_root,
        prior_claim,
        None,
        carried_claim,
    )
    assert next_claim_root is not None
    assert next_claim_root.value == carried_claim
    assert next_claim_root.previous is claim_root
    assert next_claim_root.length == claim_root.length + 1
    assert claim_rows == (carried_claim,)

    closed_group = replace(prior_group, valid_to_revision=4096)
    opened_group = replace(
        prior_group,
        valid_from_revision=4096,
        certificate_digest=sha("group:replacement"),
    )
    replaced_group_root, replacement_rows = (
        overlay_module._history_after_group_transition(
            group_root,
            prior_group,
            closed_group,
            opened_group,
        )
    )
    assert replaced_group_root is not None and replaced_group_root.previous is not None
    assert replaced_group_root.value == opened_group
    assert replaced_group_root.previous.value == closed_group
    assert replaced_group_root.previous.previous is group_root.previous
    assert replaced_group_root.length == group_root.length + 1
    assert group_root.value == prior_group and group_root.value.open
    assert replacement_rows == (closed_group, opened_group)

    closed_claim = replace(prior_claim, valid_to_revision=4096)
    opened_claim = replace(
        prior_claim,
        valid_from_revision=4096,
        certificate_digest=sha("claim:replacement"),
    )
    replaced_claim_root, claim_replacement_rows = (
        overlay_module._history_after_claim_transition(
            claim_root,
            prior_claim,
            closed_claim,
            opened_claim,
        )
    )
    assert replaced_claim_root is not None and replaced_claim_root.previous is not None
    assert replaced_claim_root.value == opened_claim
    assert replaced_claim_root.previous.value == closed_claim
    assert replaced_claim_root.previous.previous is claim_root.previous
    assert replaced_claim_root.length == claim_root.length + 1
    assert claim_root.value == prior_claim and claim_root.value.open
    assert claim_replacement_rows == (closed_claim, opened_claim)


def test_measured_overlay_event_never_exports_repository_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = make_repository()
    register = RegisterGroupEvent(event_id="register-group", group=make_group())
    apply_m5_event(repository, register)
    apply_m5_event(
        repository,
        ObserveRequirementEvent(
            event_id="seed-requirement-observation",
            observation=make_requirement_observation(
                observation_id="requirement-observation",
                requirement_id="group-a-requirement-0",
                scores=(0.75, 0.10, 0.15),
            ),
        ),
    )
    apply_event(
        repository.base,
        ObserveEvent(
            event_id="seed-claim-observation",
            observation=make_claim_observation(
                observation_id="claim-observation",
                scores=(0.75, 0.10, 0.15),
            ),
        ),
    )
    overlay = overlay_module.M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    after = deepcopy(repository)
    event = PolicyChangeEvent(
        event_id="lower-support-threshold",
        policy=DecisionPolicy("policy-v2", 0.70, 0.8),
    )
    apply_event(after.base, event)
    _ = after.current_point

    def reject_export(_repository: object) -> None:
        raise AssertionError("measured event path called export_snapshot")

    monkeypatch.setattr(M5Repository, "export_snapshot", reject_export)
    monkeypatch.setattr(InMemoryRepository, "export_snapshot", reject_export)
    result = overlay.apply_committed_event(event, before, after)

    assert result.direct_stats.decision_flips == 1
    assert result.work.policy_candidates == 1
    assert result.changed_requirement_ids == ("group-a-requirement-0",)
    assert result.changed_group_ids == ("group-a",)
    assert result.changed_claim_ids == ("claim-a",)
    assert result.changed_answer_ids == ("answer-a",)
    assert overlay.group_state("group-a").complete
    assert overlay.claim_state("claim-a").status is ClaimStatus.SUPPORTED
    assert overlay.answer_state("answer-a").status is AnswerStatus.VALID
    assert overlay.point == after.current_point
