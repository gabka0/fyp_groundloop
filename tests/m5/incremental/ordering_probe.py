from __future__ import annotations

import json
from copy import deepcopy

from groundloop.domain import DecisionPolicy
from groundloop.events import PolicyChangeEvent, apply_event
from groundloop.m5 import incremental_overlay as overlay_module
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    apply_m5_event,
)

from .helpers import make_group, make_repository, make_requirement_observation


def run_policy_rebind_scenario() -> dict[str, object]:
    repository = make_repository()
    for group_id, family_id, requirement_id, chunk_id, text in (
        ("z-group", "z-family", "z-requirement", "chunk-b", "z requirement"),
        ("a-group", "a-family", "a-requirement", "chunk-a", "a requirement"),
    ):
        apply_m5_event(
            repository,
            RegisterGroupEvent(
                event_id=f"register-{group_id}",
                group=make_group(
                    group_id=group_id,
                    family_id=family_id,
                    texts=(text,),
                    requirement_ids=(requirement_id,),
                ),
            ),
        )
        apply_m5_event(
            repository,
            ObserveRequirementEvent(
                event_id=f"observe-{group_id}",
                observation=make_requirement_observation(
                    observation_id=f"observation-{group_id}",
                    requirement_id=requirement_id,
                    chunk_id=chunk_id,
                ),
            ),
        )

    overlay = overlay_module.M5IncrementalOverlay.from_repository(repository)
    before = deepcopy(repository)
    after = deepcopy(repository)
    event = PolicyChangeEvent(
        event_id="policy-rebind",
        policy=DecisionPolicy("policy-v2", 0.8, 0.8),
    )
    apply_event(after.base, event)
    _ = after.current_point
    result = overlay.apply_committed_event(event, before, after)
    claim_state = overlay.claim_state("claim-a")
    claim_certificate = overlay.claim_certificates["claim-a"]
    score_index = overlay_module._RequirementScoreIndex()
    for observation_id, scores in (
        ("candidate-a", (0.45, 0.45, 0.10)),
        ("candidate-b", (0.42, 0.20, 0.38)),
        ("candidate-c", (0.20, 0.44, 0.36)),
        ("candidate-d", (0.90, 0.05, 0.05)),
    ):
        score_index = score_index.add(
            make_requirement_observation(
                observation_id=observation_id,
                requirement_id="probe-requirement",
                scores=scores,
            )
        )
    return {
        "logical_output_digest": result.logical_output_digest,
        "changed_requirement_ids": result.changed_requirement_ids,
        "changed_group_ids": result.changed_group_ids,
        "changed_claim_ids": result.changed_claim_ids,
        "changed_answer_ids": result.changed_answer_ids,
        "certificate_only_group_ids": result.certificate_only_group_ids,
        "certificate_only_claim_ids": result.certificate_only_claim_ids,
        "published_group_ids": tuple(
            binding.group_version_id for binding in result.published_group_bindings
        ),
        "published_claim_ids": tuple(
            binding.claim_id for binding in result.published_claim_bindings
        ),
        "complete_group_ids": claim_state.complete_group_ids,
        "selected_group_id": claim_certificate.group_version_id,
        "canonical_sort_items": result.work.matching.canonical_sort_items,
        "public_deltas": len(result.deltas),
        "overlapping_policy_candidates": score_index.policy_candidates(
            DecisionPolicy("probe-before", 0.5, 0.5),
            DecisionPolicy("probe-after", 0.4, 0.4),
        ),
    }


if __name__ == "__main__":
    print(json.dumps(run_policy_rebind_scenario(), sort_keys=True))
