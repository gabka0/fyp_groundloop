from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import asdict, replace
from pathlib import Path
from typing import cast, get_args

import pytest

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
from groundloop.m5.claim_certificates import validate_claim_binding_history
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
)
from groundloop.m5.reference import (
    M5ReferenceStates,
    compute_reference_states,
    validate_claim_certificate,
    validate_group_certificate,
)
from groundloop.m5.repository import M5Repository

from .helpers import (
    make_claim_observation,
    make_group,
    make_repository,
    make_requirement_observation,
    sha,
)

ROOT = Path(__file__).resolve().parents[3]


def _changed_keys(
    before: Mapping[str, object],
    after: Mapping[str, object],
) -> set[str]:
    return {
        key for key in before.keys() | after.keys() if before.get(key) != after.get(key)
    }


def _state_only_keys(
    before: Mapping[str, object],
    after: Mapping[str, object],
    changed: set[str],
    attribute: str,
) -> set[str]:
    return {
        key
        for key in changed
        if key in before
        and key in after
        and getattr(before[key], attribute) == getattr(after[key], attribute)
    }


def _status_changes(
    before: M5ReferenceStates,
    after: M5ReferenceStates,
) -> set[tuple[str, str, str, str]]:
    result: set[tuple[str, str, str, str]] = set()
    for object_type, old_states, new_states in (
        ("claim", before.claims, after.claims),
        ("answer", before.answers, after.answers),
    ):
        for key in old_states.keys() & new_states.keys():
            old_status = old_states[key].status
            new_status = new_states[key].status
            if old_status is not new_status:
                result.add((object_type, key, old_status.value, new_status.value))
    return result


def _assert_certificate_histories_resolve(overlay: M5IncrementalOverlay) -> None:
    group_ledger = overlay.group_certificate_artifacts_by_digest
    claim_ledger = overlay.claim_certificate_artifacts_by_digest
    assert all(
        key == artifact.certificate_digest for key, artifact in group_ledger.items()
    )
    assert all(
        key == artifact.certificate_digest for key, artifact in claim_ledger.items()
    )

    current_groups = overlay.group_certificates
    assert set(overlay._group_bindings) == set(current_groups)
    for group_id, group_artifact in current_groups.items():
        group_binding = overlay._group_bindings[group_id]
        assert group_binding.certificate_digest == group_artifact.certificate_digest
        assert group_ledger[group_binding.certificate_digest] == group_artifact
    for group_id in overlay._group_binding_history:
        for binding in overlay.group_binding_history(group_id):
            artifact = group_ledger[binding.certificate_digest]
            assert artifact.group_version_id == binding.group_version_id == group_id

    current_claims = overlay.claim_certificates
    assert set(overlay._claim_bindings) == set(current_claims)
    for claim_id, claim_artifact in current_claims.items():
        claim_binding = overlay._claim_bindings[claim_id]
        assert claim_binding.certificate_digest == claim_artifact.certificate_digest
        assert claim_ledger[claim_binding.certificate_digest] == claim_artifact
    claim_history = tuple(
        binding
        for claim_id in overlay._claim_binding_history
        for binding in overlay.claim_binding_history(claim_id)
    )
    assert validate_claim_binding_history(claim_history, claim_ledger) == ()


def _assert_matches_reference(
    repository: M5Repository,
    overlay: M5IncrementalOverlay,
    before_states: M5ReferenceStates,
    result: M5OverlayApplyResult,
) -> None:
    reference = compute_reference_states(repository)
    assert overlay.requirement_states == reference.requirements
    assert overlay.group_states == reference.groups
    assert overlay.claim_states == reference.claims
    assert overlay.answer_states == reference.answers
    assert overlay.point == repository.current_point
    assert overlay.matching_audit_issues() == {}
    _assert_certificate_histories_resolve(overlay)

    complete_groups = {
        group_id for group_id, state in reference.groups.items() if state.complete
    }
    assert set(overlay.group_certificates) == complete_groups
    assert all(
        validate_group_certificate(repository, artifact)
        for artifact in overlay.group_certificates.values()
    )
    assert set(overlay.claim_certificates) == set(reference.claims)
    assert all(
        validate_claim_certificate(
            repository,
            artifact,
            overlay.group_certificates,
        )
        for artifact in overlay.claim_certificates.values()
    )

    requirement_changes = _changed_keys(
        before_states.requirements,
        reference.requirements,
    )
    group_changes = _changed_keys(before_states.groups, reference.groups)
    claim_changes = _changed_keys(before_states.claims, reference.claims)
    answer_changes = _changed_keys(before_states.answers, reference.answers)
    assert set(result.changed_requirement_ids) == requirement_changes
    assert set(result.changed_answer_ids) == answer_changes
    assert set(result.certificate_only_group_ids).isdisjoint(group_changes)
    assert set(result.certificate_only_claim_ids).isdisjoint(claim_changes)
    assert set(result.changed_group_ids) == (
        group_changes | set(result.certificate_only_group_ids)
    )
    assert set(result.changed_claim_ids) == (
        claim_changes | set(result.certificate_only_claim_ids)
    )
    assert set(result.state_only_requirement_ids) == _state_only_keys(
        before_states.requirements,
        reference.requirements,
        requirement_changes,
        "satisfied",
    )
    assert set(result.state_only_group_ids) == _state_only_keys(
        before_states.groups,
        reference.groups,
        group_changes,
        "complete",
    )
    assert set(result.state_only_claim_ids) == _state_only_keys(
        before_states.claims,
        reference.claims,
        claim_changes,
        "status",
    )
    assert {
        (delta.object_type, delta.object_id, delta.old_status, delta.new_status)
        for delta in result.deltas
    } == _status_changes(before_states, reference)
    for values in (
        result.changed_requirement_ids,
        result.changed_group_ids,
        result.changed_claim_ids,
        result.changed_answer_ids,
        result.certificate_only_group_ids,
        result.certificate_only_claim_ids,
    ):
        assert len(values) == len(set(values))
    assert result.work.group_certificate_only_changes == len(
        result.certificate_only_group_ids
    )
    assert result.work.claim_certificate_only_changes == len(
        result.certificate_only_claim_ids
    )
    assert result.work.public_status_deltas == len(result.deltas)
    assert result.work.matching.claim_status_changes == sum(
        delta.object_type == "claim" for delta in result.deltas
    )
    assert result.work.matching.answer_status_changes == sum(
        delta.object_type == "answer" for delta in result.deltas
    )
    assert result.work.output_bytes > 0
    assert len(result.logical_output_digest) == 64
    result.work.assert_nonnegative()


def _apply_and_compare(
    repository: M5Repository,
    overlay: M5IncrementalOverlay,
    event: CommittedOverlayEvent,
) -> M5OverlayApplyResult:
    before = deepcopy(repository)
    before_states = compute_reference_states(before)
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
    result = overlay.apply_committed_event(event, before, repository)
    _assert_matches_reference(repository, overlay, before_states, result)
    return result


def run_differential_trace_summary() -> dict[str, object]:
    repository = make_repository(claim_ids=("claim-a", "claim-b"))
    overlay = M5IncrementalOverlay.from_repository(repository)

    group_a = make_group()
    group_b = make_group(
        group_id="group-b",
        family_id="family-b",
        texts=("group b first", "group b second"),
        requirement_ids=("group-b-r0", "group-b-r1"),
    )
    events: list[CommittedOverlayEvent] = [
        RegisterGroupEvent("register-group-a", group_a),
        ObserveRequirementEvent(
            "observe-group-a",
            make_requirement_observation(
                observation_id="group-a-support",
                requirement_id="group-a-requirement-0",
                chunk_id="chunk-a",
            ),
        ),
        RegisterGroupEvent("register-group-b", group_b),
        ObserveRequirementEvent(
            "observe-group-b-r0",
            make_requirement_observation(
                observation_id="group-b-r0-support",
                requirement_id="group-b-r0",
                chunk_id="chunk-a",
            ),
        ),
        ObserveRequirementEvent(
            "observe-group-b-r1-shared",
            make_requirement_observation(
                observation_id="group-b-r1-shared-support",
                requirement_id="group-b-r1",
                chunk_id="chunk-a",
            ),
        ),
        ObserveRequirementEvent(
            "observe-group-b-r1-distinct",
            make_requirement_observation(
                observation_id="group-b-r1-distinct-support",
                requirement_id="group-b-r1",
                chunk_id="chunk-b",
            ),
        ),
        ObserveEvent(
            "observe-claim-a-support",
            make_claim_observation(
                observation_id="claim-a-support",
                claim_id="claim-a",
                chunk_id="chunk-c",
            ),
        ),
        ObserveEvent(
            "observe-claim-a-refute",
            make_claim_observation(
                observation_id="claim-a-refute",
                claim_id="claim-a",
                chunk_id="chunk-b",
                scores=(0.05, 0.9, 0.05),
            ),
        ),
        PolicyChangeEvent(
            "raise-support-threshold",
            DecisionPolicy("policy-high-support", 0.95, 0.8),
        ),
        PolicyChangeEvent(
            "restore-support-threshold",
            DecisionPolicy("policy-restored", 0.8, 0.8),
        ),
        InsertDocumentEvent(
            event_id="insert-document-d",
            document_id="document-d",
            document_version_id="document-d-v1",
            content_hash=sha("document-d-v1"),
            chunks=(
                ChunkInput("chunk-d1", 0, "alpha evidence"),
                ChunkInput("chunk-d2", 1, "delta evidence"),
            ),
        ),
        ObserveRequirementEvent(
            "observe-group-a-duplicate-hash",
            make_requirement_observation(
                observation_id="group-a-duplicate-hash",
                requirement_id="group-a-requirement-0",
                chunk_id="chunk-d1",
            ),
        ),
        ObserveEvent(
            "observe-claim-b-support",
            make_claim_observation(
                observation_id="claim-b-support",
                claim_id="claim-b",
                chunk_id="chunk-d2",
            ),
        ),
        ObserveRequirementEvent(
            "supersede-group-a-selected",
            make_requirement_observation(
                observation_id="group-a-support-v2",
                requirement_id="group-a-requirement-0",
                chunk_id="chunk-a",
            ),
        ),
        ReplaceDocumentVersionEvent(
            event_id="replace-document-d",
            document_id="document-d",
            old_document_version_id="document-d-v1",
            new_document_version_id="document-d-v2",
            content_hash=sha("document-d-v2"),
            chunks=(ChunkInput("chunk-d3", 0, "epsilon evidence"),),
        ),
        ReplaceGroupEvent(
            event_id="replace-group-b",
            old_group_version_id="group-b",
            successor=make_group(
                group_id="group-b-v2",
                family_id="family-b",
                texts=("group b first v2", "group b second v2"),
                requirement_ids=("group-b-v2-r0", "group-b-v2-r1"),
                predecessors=("group-b-r0", "group-b-r1"),
                supersedes_group_id="group-b",
            ),
        ),
        ObserveRequirementEvent(
            "observe-noncanonical-requirement",
            replace(
                make_requirement_observation(
                    observation_id="noncanonical-requirement",
                    requirement_id="group-b-v2-r0",
                    chunk_id="chunk-a",
                ),
                task_type="noncanonical_requirement_task",
            ),
        ),
        ObserveRequirementEvent(
            "observe-group-b-v2-r0",
            make_requirement_observation(
                observation_id="group-b-v2-r0-support",
                requirement_id="group-b-v2-r0",
                chunk_id="chunk-a",
            ),
        ),
        ObserveRequirementEvent(
            "observe-group-b-v2-r1",
            make_requirement_observation(
                observation_id="group-b-v2-r1-support",
                requirement_id="group-b-v2-r1",
                chunk_id="chunk-b",
            ),
        ),
        RetireGroupEvent("retire-group-a", "group-a"),
        DeleteDocumentVersionEvent("delete-base-document", "document-version-a"),
    ]
    assert {type(event) for event in events} == set(get_args(CommittedOverlayEvent))

    event_results: list[dict[str, object]] = []
    for event in events:
        result = _apply_and_compare(repository, overlay, event)
        event_results.append(
            {
                "event_id": event.event_id,
                "event_kind": type(event).__name__,
                "point": (result.point.epoch_id, result.point.revision),
                "deltas": tuple(asdict(delta) for delta in result.deltas),
                "changed_requirement_ids": result.changed_requirement_ids,
                "changed_group_ids": result.changed_group_ids,
                "changed_claim_ids": result.changed_claim_ids,
                "changed_answer_ids": result.changed_answer_ids,
                "state_only_requirement_ids": result.state_only_requirement_ids,
                "state_only_group_ids": result.state_only_group_ids,
                "state_only_claim_ids": result.state_only_claim_ids,
                "certificate_only_group_ids": result.certificate_only_group_ids,
                "certificate_only_claim_ids": result.certificate_only_claim_ids,
                "published_group_bindings": tuple(
                    asdict(binding) for binding in result.published_group_bindings
                ),
                "published_claim_bindings": tuple(
                    asdict(binding) for binding in result.published_claim_bindings
                ),
                "logical_output_digest": result.logical_output_digest,
                "work": asdict(result.work),
                "direct_stats": asdict(result.direct_stats),
            }
        )
    return {
        "events": event_results,
        "final_group_artifact_digests": tuple(
            overlay.group_certificate_artifacts_by_digest
        ),
        "final_claim_artifact_digests": tuple(
            overlay.claim_certificate_artifacts_by_digest
        ),
        "final_group_binding_digests": {
            group_id: tuple(
                binding.certificate_digest
                for binding in overlay.group_binding_history(group_id)
            )
            for group_id in overlay._group_binding_history
        },
        "final_claim_binding_digests": {
            claim_id: tuple(
                binding.certificate_digest
                for binding in overlay.claim_binding_history(claim_id)
            )
            for claim_id in overlay._claim_binding_history
        },
    }


def test_overlay_matches_independent_reference_after_every_event_class() -> None:
    summary = run_differential_trace_summary()
    events = cast(list[dict[str, object]], summary["events"])

    assert len(events) == 21
    assert {cast(str, event["event_kind"]) for event in events} == {
        "DeleteDocumentVersionEvent",
        "InsertDocumentEvent",
        "ObserveEvent",
        "ObserveRequirementEvent",
        "PolicyChangeEvent",
        "RegisterGroupEvent",
        "ReplaceDocumentVersionEvent",
        "ReplaceGroupEvent",
        "RetireGroupEvent",
    }


@pytest.mark.parametrize("seed", ("0", "1", "42", "8675309"))
def test_whole_differential_trace_is_cross_hash_seed_stable(seed: str) -> None:
    environment = os.environ.copy()
    python_path = (str(ROOT / "src"), str(ROOT))
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = os.pathsep.join(
        (*python_path, *((existing,) if existing else ()))
    )
    environment["PYTHONHASHSEED"] = seed
    completed = subprocess.run(
        [sys.executable, "-m", "tests.m5.incremental.differential_trace_probe"],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess_summary = json.loads(completed.stdout)
    expected = json.loads(json.dumps(run_differential_trace_summary()))
    assert subprocess_summary == expected
