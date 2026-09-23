"""D30 falsifiers 12 and 15: replay and successful-output non-change."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import fields, is_dataclass, replace
from enum import Enum
from typing import Any

import pytest

from groundloop.events import DeleteDocumentVersionEvent
from groundloop.m4.application import ApplicationExecutionPolicy, DynamicEventPlan
from groundloop.m4.contracts import (
    CorpusUpdateIdentity,
    PairKey,
    UpdateKind,
)
from groundloop.m4.runtime.withdrawal import (
    CandidateDependency,
    ObservationDependency,
)
from groundloop.m5.events import legacy_event_payload_digest
from groundloop.m5.runtime import postgres_withdrawal
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    M5RuntimeWork,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
)
from tests.m5.postgres_runtime.d30_store.test_dynamic_claim_provenance import (
    _valid_dynamic_authority,
)


def _cell(row: tuple[object, ...], index: int, value: object) -> tuple[object, ...]:
    values = list(row)
    values[index] = value
    return tuple(values)


def _private_provenance_variant(
    *, execution: bool, reused_from: str | None, label: str
) -> tuple[Any, Any]:
    """Build one valid provenance image whose private fields differ by case."""

    claim, owner = _valid_dynamic_authority(
        execution=execution,
        reused_from=reused_from,
        task_type=f"opaque.task::{label}",
    )
    observation_id = "opaque observation::kept-output-id"
    observation = claim.observation_row
    for index, value in (
        (0, observation_id),
        (8, f"opaque-model::{label}"),
        (9, f"opaque-revision::{label}"),
        (10, f"opaque-prompt::{label}"),
    ):
        observation = _cell(observation, index, value)
    execution_rows: list[tuple[object, ...] | None] = []
    for row in claim.execution_rows:
        if row is None:
            execution_rows.append(None)
            continue
        rewritten = _cell(row, 0, observation_id)
        rewritten = _cell(rewritten, 3, f"opaque-model-artifact::{label}")
        rewritten = _cell(rewritten, 4, f"opaque-prompt-artifact::{label}")
        execution_rows.append(rewritten)
    return (
        replace(
            claim,
            currency=replace(claim.currency, observation_id=observation_id),
            observation_row=observation,
            delta_row=_cell(claim.delta_row, 6, observation_id),
            execution_rows=tuple(execution_rows),
            model_row=(f"opaque-model-artifact::{label}", "verification")
            if execution
            else None,
            prompt_row=(f"opaque-prompt-artifact::{label}", "verification")
            if execution
            else None,
        ),
        owner,
    )


def _document_delete_plan() -> tuple[M5TypedEventPlan, ApplicationExecutionPolicy]:
    event = DeleteDocumentVersionEvent("opaque-output-event", "opaque-document-version")
    payload_hash = legacy_event_payload_digest(event)
    policy_id = "a" * 64
    direct = DynamicEventPlan(
        update=CorpusUpdateIdentity(
            event_id=event.event_id,
            payload_hash=payload_hash,
            update_kind=UpdateKind.DELETE,
            previous_published_epoch_id=1,
            candidate_policy_id=policy_id,
        ),
        inserted_chunk_version_ids=(),
        deactivated_chunk_version_ids=("chunk-a",),
        registered_claim_ids=("claim-a",),
        claim_registry_snapshot_id="opaque-registry-snapshot",
    )
    plan = M5TypedEventPlan(
        structural_event_id=event.event_id,
        event=event,
        payload_hash=payload_hash,
        direct_plan=direct,
        candidate_policy_id=policy_id,
        candidate_policy_manifest_hash=policy_id,
        requirement_registry_snapshot=RequirementRegistrySnapshot.build(()),
        active_chunk_snapshot=ActiveChunkSnapshot.build(()),
        expected_previous_published_epoch_id=1,
    )
    return plan, ApplicationExecutionPolicy("b" * 64, "c" * 64, "2" * 64)


def _dto_field_bytes(value: object, path: str = "dto") -> tuple[tuple[str, bytes], ...]:
    """Expose every scalar DTO field without inventing another digest recipe."""

    if isinstance(value, Enum):
        return ((path, str(value.value).encode("utf-8")),)
    if isinstance(value, str):
        return ((path, value.encode("utf-8")),)
    if value is None:
        return ((path, b"<none>"),)
    if type(value) is bool:
        return ((path, b"true" if value else b"false"),)
    if type(value) is int:
        return ((path, str(value).encode("ascii")),)
    if is_dataclass(value):
        rendered: list[tuple[str, bytes]] = []
        for descriptor in fields(value):
            rendered.extend(
                _dto_field_bytes(
                    getattr(value, descriptor.name), f"{path}.{descriptor.name}"
                )
            )
        return tuple(rendered)
    if type(value) is tuple:
        rendered = []
        for index, item in enumerate(value):
            rendered.extend(_dto_field_bytes(item, f"{path}[{index}]"))
        return tuple(rendered)
    raise AssertionError(f"unsupported DTO field at {path}: {type(value)!r}")


def _direct_structural_work(
    withdrawal: postgres_withdrawal.StructuralWithdrawal,
) -> M5RuntimeWork:
    plan = withdrawal.plan
    return M5RuntimeWork(
        deactivated_chunk_count=len(plan.deactivated_chunk_ids),
        withdrawn_candidate_edge_count=len(plan.candidate_edge_ids),
        withdrawn_current_observation_count=len(plan.observation_ids),
    )


def test_retained_replay_never_reconstructs_current_claim_provenance(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_load_retained_document_open")
    for forbidden in (
        "_require_d30_read_committed",
        "_gather_d30_claim_authority",
        "_probe_d30_predecessor_currency",
        "_validate_d30_owner_topology_from_held_rows",
        "_validate_d30_m3_bootstrap",
        "_validate_d30_dynamic_claim",
        "groundloop_observation_currency",
        "groundloop_working_observation_delta",
        "groundloop_semantic_observation",
        "groundloop_semantic_job",
    ):
        assert forbidden not in source
    for retained in (
        "_read_existing_document_closure",
        "_stored_direct_open",
        "_stored_requirement_declarations",
        "_raise_if_terminal",
    ):
        assert retained in source


def test_first_application_keeps_existing_withdrawal_dto_boundary(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_derive_locked_document_open")
    for output in (
        "direct_open",
        "requirement_withdrawal",
        "requirement_roots",
        "root_set_hash",
    ):
        assert output in source
    assert "_locked_direct_withdrawal" in source
    assert "plan_requirement_withdrawal" in source
    assert "_direct_open_from_withdrawal" in source
    for forbidden_output in (
        "verification_execution=",
        "working_delta=",
        "source_epoch=",
        "provenance=",
    ):
        assert forbidden_output not in source


def test_claim_provenance_fields_do_not_enter_withdrawal_or_result_digests(
    function_source: Callable[[str], str],
) -> None:
    locked = function_source("_locked_direct_withdrawal")
    assert "observations: tuple[ObservationDependency" in locked
    assert "candidates: tuple[CandidateDependency" in locked
    private_fields = postgres_withdrawal._D30DynamicClaimLocator.__dataclass_fields__
    for private in (
        "delta_row",
        "predecessor_candidate",
        "owner_epoch_id",
        "execution_rows",
        "model_row",
        "prompt_row",
    ):
        assert private in private_fields
        assert private not in locked


def test_d30_planning_adds_no_work_counter_or_digest_recipe(
    function_source: Callable[[str], str],
) -> None:
    sources = "\n".join(
        function_source(name)
        for name in (
            "_gather_d30_claim_authority",
            "_probe_d30_predecessor_currency",
            "_d30_owner_locator",
            "_d30_execution_coordinates",
            "_validate_d30_m3_bootstrap",
            "_validate_d30_dynamic_claim",
        )
    )
    for forbidden in (
        'stable_m5_digest("m5-d30-',
        'stable_m4_digest("m5-d30-',
        'stable_m5_digest("d30-',
        'stable_m4_digest("d30-',
    ):
        assert forbidden not in sources


def test_candidate_and_observation_outcome_matrix_remains_separate(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_derive_locked_document_open")
    assert "candidate_edges" in source
    assert "observation_edges" in source
    assert "direct_observations" in source
    assert "direct_candidates" in source
    assert "active_requirement_ids" in source
    assert "candidate_edges=candidate_edges" in source
    assert "observation_edges=observation_edges" in source
    assert "observations=direct_observations" in source
    assert "candidates=direct_candidates" in source


def test_optional_execution_task_id_and_overlap_never_change_output_builder(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_locked_direct_withdrawal")
    assert "StructuralWithdrawal(" in source
    assert "plan_withdrawal(" in source
    assert "ReverseDependencyIndex.build(observations, candidates)" in source
    assert "fallback_claim_ids" in source
    for forbidden in (
        "task_type",
        "result_artifact_id",
        "producer",
        "reused_from_observation_id",
        "groundloop_impact_channel_hit",
        "groundloop_admitted_pair",
    ):
        assert forbidden not in source


@pytest.mark.parametrize(
    (
        "include_observation",
        "include_candidate",
        "expected_observations",
        "expected_candidates",
    ),
    (
        (False, False, (), ()),
        (True, False, ("observation-a",), ()),
        (False, True, (), ("candidate-a",)),
        (True, True, ("observation-a",), ("candidate-a",)),
    ),
)
def test_direct_candidate_observation_outcome_matrix_is_exact(
    include_observation: bool,
    include_candidate: bool,
    expected_observations: tuple[str, ...],
    expected_candidates: tuple[str, ...],
) -> None:
    pair = PairKey("claim-a", "chunk-a")
    observations = (
        (ObservationDependency("observation-a", pair),) if include_observation else ()
    )
    candidates = (
        (CandidateDependency("candidate-a", pair),) if include_candidate else ()
    )
    withdrawal = postgres_withdrawal._locked_direct_withdrawal(
        source_chunks=("chunk-a",),
        observations=observations,
        candidates=candidates,
    )
    assert withdrawal.plan.observation_ids == expected_observations
    assert withdrawal.plan.candidate_edge_ids == expected_candidates
    assert withdrawal.plan.affected_pairs == (
        () if not observations and not candidates else (pair,)
    )
    assert withdrawal.plan.indexed_operation_count == (
        1 + len(expected_observations) + len(expected_candidates)
    )


def test_successful_output_is_exact_across_optional_private_provenance() -> None:
    event, execution_policy = _document_delete_plan()
    selected_pair = PairKey("claim-a", "chunk-a")
    overlapping_claim_pair = PairKey("claim-a", "unrelated-chunk")
    unrelated_pair = PairKey("unrelated-claim", "unrelated-chunk")
    unrelated_observation_ids = (
        "unrelated-overlap-observation",
        "unrelated-history-observation",
    )
    unrelated_candidate_ids = (
        "unrelated-overlap-candidate",
        "unrelated-history-candidate",
    )
    overlap_variants = {
        "same-claim-different-chunk": (
            ObservationDependency(unrelated_observation_ids[0], overlapping_claim_pair),
            CandidateDependency(unrelated_candidate_ids[0], overlapping_claim_pair),
        ),
        "unrelated-claim-different-chunk": (
            ObservationDependency(unrelated_observation_ids[1], unrelated_pair),
            CandidateDependency(unrelated_candidate_ids[1], unrelated_pair),
        ),
    }
    outputs: list[
        tuple[
            object,
            tuple[tuple[str, bytes], ...],
            dict[str, object],
            tuple[int, ...],
            str,
        ]
    ] = []
    private_inputs: list[tuple[object, ...]] = []
    for execution, reused_from, overlap_kind, label in (
        (False, None, None, "execution-absent"),
        (True, None, None, "execution-present-null-reuse"),
        (
            True,
            "opaque older observation::non-null-reuse",
            None,
            "execution-reused",
        ),
        (
            True,
            "opaque older observation::non-null-reuse",
            "same-claim-different-chunk",
            "execution-reused-same-claim-different-chunk",
        ),
        (
            True,
            "opaque older observation::non-null-reuse",
            "unrelated-claim-different-chunk",
            "execution-reused-unrelated-claim-different-chunk",
        ),
    ):
        claim, owner = _private_provenance_variant(
            execution=execution,
            reused_from=reused_from,
            label=label,
        )
        postgres_withdrawal._validate_d30_dynamic_claim(
            claim,
            owner=owner,
            candidate_policy_id="policy-a",
        )
        execution_rows = tuple(row for row in claim.execution_rows if row is not None)
        private_inputs.append(
            (
                claim.currency.task_type,
                claim.observation_row[8:11],
                bool(execution_rows),
                None if not execution_rows else execution_rows[0][3:5],
                None if not execution_rows else execution_rows[0][12],
                overlap_kind,
            )
        )
        overlap = None if overlap_kind is None else overlap_variants[overlap_kind]
        observations = (
            ObservationDependency("opaque observation::kept-output-id", selected_pair),
            *((overlap[0],) if overlap is not None else ()),
        )
        candidates = (
            CandidateDependency("opaque candidate::kept-output-id", selected_pair),
            *((overlap[1],) if overlap is not None else ()),
        )
        withdrawal = postgres_withdrawal._locked_direct_withdrawal(
            source_chunks=("chunk-a",),
            observations=observations,
            candidates=candidates,
        )
        direct_open = postgres_withdrawal._direct_open_from_withdrawal(
            event,
            execution_policy,
            withdrawal,
        )
        structural_work = _direct_structural_work(withdrawal)
        outputs.append(
            (
                direct_open,
                _dto_field_bytes(direct_open),
                postgres_withdrawal._document_declaration_manifest(direct_open),
                structural_work.counter_values(),
                structural_work.work_digest,
            )
        )

        assert withdrawal.plan.observation_ids == (
            "opaque observation::kept-output-id",
        )
        assert withdrawal.plan.candidate_edge_ids == (
            "opaque candidate::kept-output-id",
        )
        assert withdrawal.plan.affected_pairs == (selected_pair,)
        assert withdrawal.fallback_claim_ids == ("claim-a",)
        assert withdrawal.plan.chunk_lookups == 1
        assert withdrawal.plan.observation_edge_visits == 1
        assert withdrawal.plan.candidate_edge_visits == 1
        assert withdrawal.plan.indexed_operation_count == 3
        assert not set(unrelated_observation_ids) & set(withdrawal.plan.observation_ids)
        assert not set(unrelated_candidate_ids) & set(
            withdrawal.plan.candidate_edge_ids
        )

    baseline = outputs[0]
    assert private_inputs == [
        (
            "opaque.task::execution-absent",
            (
                "opaque-model::execution-absent",
                "opaque-revision::execution-absent",
                "opaque-prompt::execution-absent",
            ),
            False,
            None,
            None,
            None,
        ),
        (
            "opaque.task::execution-present-null-reuse",
            (
                "opaque-model::execution-present-null-reuse",
                "opaque-revision::execution-present-null-reuse",
                "opaque-prompt::execution-present-null-reuse",
            ),
            True,
            (
                "opaque-model-artifact::execution-present-null-reuse",
                "opaque-prompt-artifact::execution-present-null-reuse",
            ),
            None,
            None,
        ),
        (
            "opaque.task::execution-reused",
            (
                "opaque-model::execution-reused",
                "opaque-revision::execution-reused",
                "opaque-prompt::execution-reused",
            ),
            True,
            (
                "opaque-model-artifact::execution-reused",
                "opaque-prompt-artifact::execution-reused",
            ),
            "opaque older observation::non-null-reuse",
            None,
        ),
        (
            "opaque.task::execution-reused-same-claim-different-chunk",
            (
                "opaque-model::execution-reused-same-claim-different-chunk",
                "opaque-revision::execution-reused-same-claim-different-chunk",
                "opaque-prompt::execution-reused-same-claim-different-chunk",
            ),
            True,
            (
                "opaque-model-artifact::execution-reused-same-claim-different-chunk",
                "opaque-prompt-artifact::execution-reused-same-claim-different-chunk",
            ),
            "opaque older observation::non-null-reuse",
            "same-claim-different-chunk",
        ),
        (
            "opaque.task::execution-reused-unrelated-claim-different-chunk",
            (
                "opaque-model::execution-reused-unrelated-claim-different-chunk",
                "opaque-revision::execution-reused-unrelated-claim-different-chunk",
                "opaque-prompt::execution-reused-unrelated-claim-different-chunk",
            ),
            True,
            (
                "opaque-model-artifact::execution-reused-unrelated-claim-different-chunk",
                "opaque-prompt-artifact::execution-reused-unrelated-claim-different-chunk",
            ),
            "opaque older observation::non-null-reuse",
            "unrelated-claim-different-chunk",
        ),
    ]
    assert outputs[1:] == [baseline, baseline, baseline, baseline]
