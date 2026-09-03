from __future__ import annotations

import hashlib
import math
import struct
from enum import StrEnum

import pytest

from groundloop.domain import StatusDelta, SubjectKind, VerificationLabel
from groundloop.errors import ValidationError
from groundloop.m4.contracts import VectorIndexKind
from groundloop.m5.digests import f64_field, stable_m5_digest, text_field
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    M5_NORMALIZER_PROVENANCE_HASH,
    M5DiscoveryDirection,
    M5ExecutionEvidenceDisposition,
    M5JobKind,
    M5JobState,
    M5RequirementAdmissionChannel,
    M5RunFailureReason,
    M5RuntimeSubgraph,
    M5RuntimeWorkContributionKind,
    M5StateReferenceKind,
    M5TerminalReason,
    M5TextNormalizerProvenance,
    M5TypedDirectReturnKind,
    M5TypedDirectScopeKind,
)

H1 = "1" * 64
H2 = "2" * 64
H3 = "3" * 64
H4 = "4" * 64


def _framed_sha256(*fields: str) -> str:
    digest = hashlib.sha256()
    for field in fields:
        encoded = field.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def test_normalizer_provenance_is_the_frozen_external_golden_vector() -> None:
    provenance = M5TextNormalizerProvenance()

    assert len(provenance.whitespace_codepoints) == 29
    assert provenance.whitespace_codepoints == tuple(
        sorted(provenance.whitespace_codepoints)
    )
    assert provenance.normalizer_provenance_hash == M5_NORMALIZER_PROVENANCE_HASH


def test_semantic_pair_recipe_matches_independent_framing() -> None:
    actual = digests.semantic_pair_digest(
        SubjectKind.REQUIREMENT, "req-λ", "chunk-U0001f642"
    )
    expected = _framed_sha256(
        "m5-semantic-pair-v2",
        "enum",
        "requirement",
        "text",
        "req-λ",
        "text",
        "chunk-U0001f642",
    )

    assert actual == expected


def test_cancellation_plan_recipe_matches_independent_framing_and_goldens() -> None:
    subject_inactive = digests.cancellation_plan_digest(
        structural_event_id="event-λ",
        epoch_id=7,
        cancelled_job_ids=(H1, H2),
        reason=M5TerminalReason.SUBJECT_INACTIVE,
    )
    scope_retired = digests.cancellation_plan_digest(
        structural_event_id="event-λ",
        epoch_id=7,
        cancelled_job_ids=(H1, H2),
        reason=M5TerminalReason.SCOPE_RETIRED,
    )
    epoch_failed = digests.cancellation_plan_digest(
        structural_event_id="event-λ",
        epoch_id=7,
        cancelled_job_ids=(H1, H2),
        reason=M5TerminalReason.EPOCH_FAILED,
    )

    assert scope_retired == _framed_sha256(
        "m5-cancellation-plan-v2",
        "text",
        "event-λ",
        "int",
        "7",
        "sequence",
        "int",
        "2",
        "sha256",
        H1,
        "sha256",
        H2,
        "enum",
        "scope_retired",
    )
    assert (
        subject_inactive,
        scope_retired,
        epoch_failed,
    ) == (
        "fc138b8971e2dcc1f540e96149404bf7ded8f595307a1bdc7f109508b289317a",
        "e80e49a4e500ae84f44c35c066726572e84e87eb00c7cb3ec61754d41b78a8f6",
        "1110b91294ebd76349b14f0e3c2e48c72e8d8e546c02f4598655f09d3ff7db16",
    )


def test_cancellation_plan_digest_binds_every_field_and_job_order() -> None:
    base = digests.cancellation_plan_digest(
        structural_event_id="event",
        epoch_id=7,
        cancelled_job_ids=(H1, H2),
        reason=M5TerminalReason.SUBJECT_INACTIVE,
    )

    assert base != digests.cancellation_plan_digest(
        structural_event_id="another-event",
        epoch_id=7,
        cancelled_job_ids=(H1, H2),
        reason=M5TerminalReason.SUBJECT_INACTIVE,
    )
    assert base != digests.cancellation_plan_digest(
        structural_event_id="event",
        epoch_id=8,
        cancelled_job_ids=(H1, H2),
        reason=M5TerminalReason.SUBJECT_INACTIVE,
    )
    assert base != digests.cancellation_plan_digest(
        structural_event_id="event",
        epoch_id=7,
        cancelled_job_ids=(H1, H3),
        reason=M5TerminalReason.SUBJECT_INACTIVE,
    )
    assert base != digests.cancellation_plan_digest(
        structural_event_id="event",
        epoch_id=7,
        cancelled_job_ids=(H2, H1),
        reason=M5TerminalReason.SUBJECT_INACTIVE,
    )
    assert base != digests.cancellation_plan_digest(
        structural_event_id="event",
        epoch_id=7,
        cancelled_job_ids=(H1, H2),
        reason=M5TerminalReason.SCOPE_RETIRED,
    )


def test_cancellation_plan_digest_rejects_invalid_framed_values() -> None:
    with pytest.raises(ValidationError):
        digests.cancellation_plan_digest(
            structural_event_id="",
            epoch_id=7,
            cancelled_job_ids=(H1,),
            reason=M5TerminalReason.SUBJECT_INACTIVE,
        )
    with pytest.raises(ValidationError):
        digests.cancellation_plan_digest(
            structural_event_id="event",
            epoch_id=True,
            cancelled_job_ids=(H1,),
            reason=M5TerminalReason.SUBJECT_INACTIVE,
        )
    with pytest.raises(ValidationError):
        digests.cancellation_plan_digest(
            structural_event_id="event",
            epoch_id=7,
            cancelled_job_ids=("not-a-hash",),
            reason=M5TerminalReason.SUBJECT_INACTIVE,
        )


def test_frozen_runtime_golden_vector_bundle() -> None:
    vectors = {
        "pair": digests.semantic_pair_digest(
            SubjectKind.REQUIREMENT, "req-λ", "chunk-U0001f642"
        ),
        "empty_registry": digests.requirement_registry_snapshot_digest(()),
        "empty_chunks": digests.active_chunk_snapshot_digest(()),
        "forward_scope": digests.discovery_scope_contract_digest(
            direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
            requirement_version_id="req-1",
            inserted_chunk_version_id=None,
            candidate_policy_id="policy",
            requirement_registry_snapshot_digest_value=H1,
            active_chunk_snapshot_digest_value=H2,
        ),
        "empty_closure": digests.discovery_scope_closure_digest(H1, ()),
        "empty_children": digests.child_set_digest(()),
        "empty_roots": digests.requirement_root_set_digest(()),
        "zero_work": digests.runtime_work_digest((0,) * 32),
        "schema_bundle": digests.runtime_schema_bundle_digest(
            migration_sha256=H1, core_schema_bundle_sha256=H2
        ),
    }

    assert vectors == {
        "pair": "3c9d4fe1cc26aa20095c3eb761305f46c4217595345cb4f080f3931f20e66ab2",
        "empty_registry": (
            "50afbb2e35c3cf02a3c82056a8b01884bc66fc8076729b55a310baefbd680e36"
        ),
        "empty_chunks": (
            "289cb32945ad148272fe51ff360996d5dde26aeb09ac2894f0d56407cf7022d2"
        ),
        "forward_scope": (
            "c17956e93bdc8cb584b30b34c8a54c831fe25a546631311c5dd0f39ad634c79d"
        ),
        "empty_closure": (
            "26a7c9c053f495162b1cb2c7bdf01dc51ecf875df3588ff9c97f66ee8494abc6"
        ),
        "empty_children": (
            "e8e12487ac0f52a2731952f0bfeace8566f23effddc8b3912371b0fe9f3faf17"
        ),
        "empty_roots": (
            "c14da31e2b11ee76dba7e5b0ed4f2deaf842f5fb4ecc5d97b25e9125ce0f1ed5"
        ),
        "zero_work": "5304dcf5375d76796e45635d349cf1ddcc00218049f70859c17649bc8ab46bd2",
        "schema_bundle": (
            "943aafc308ece9f36061174e868a00441e156a07e7a56cd02b1597ec0d019f85"
        ),
    }


def test_signed_int_golden_vectors_are_not_string_or_bool_aliases() -> None:
    assert {value: digests.job_attempt_id(H1, value, H2) for value in (-1, 0, 1)} == {
        -1: "be2dfee5354c49fcfe772cf4f5ba4ed3a2ffea936b16d69560774c69d6c7b011",
        0: "b6379ea17c57f5b8d8dc942cac424b0bf1ebe2abb097637b78b78bc30eab2915",
        1: "256cf36cb80779fc480b017a838bba01ac5f546afff66b466f824e5f8e5344df",
    }
    with pytest.raises(ValidationError):
        digests.job_attempt_id(H1, True, H2)


def test_typed_framing_preserves_utf8_boundaries_and_negative_zero() -> None:
    assert stable_m5_digest("d", text_field("ab"), text_field("c")) != (
        stable_m5_digest("d", text_field("a"), text_field("bc"))
    )
    assert struct.pack(">d", -0.0).hex() == "8000000000000000"
    assert f64_field(-0.0) != f64_field(0.0)
    for invalid in (math.inf, -math.inf, math.nan):
        with pytest.raises(ValidationError):
            f64_field(invalid)


def test_candidate_policy_every_field_is_in_content_identity() -> None:
    base: dict[str, object] = {
        "embedding_model_artifact_id": "embedding",
        "requirement_role_template_hash": H1,
        "chunk_role_template_hash": H2,
        "vector_method_version": "vector-v1",
        "vector_index_kind": VectorIndexKind.EXACT,
        "vector_index_build_config_hash": H3,
        "vector_search_config_hash": H4,
        "lexical_method_version": "lex-v1",
        "lexical_config_hash": H1,
        "lexical_postgres_version": "postgres-17",
        "lexical_regconfig_identity": "simple",
        "fusion_version": "rank-interleave-v1",
        "reverse_budget_per_inserted_chunk": 2,
        "forward_budget_per_requirement": 3,
        "verifier_execution_spec_hash": H2,
        "decision_policy_version": "decision-v1",
        "lineage_safety_override": True,
    }
    expected = digests.candidate_policy_manifest_digest(**base)  # type: ignore[arg-type]
    mutations: dict[str, object] = {
        "embedding_model_artifact_id": "embedding-2",
        "requirement_role_template_hash": H2,
        "chunk_role_template_hash": H3,
        "vector_method_version": "vector-v2",
        "vector_index_kind": VectorIndexKind.HNSW,
        "vector_index_build_config_hash": H4,
        "vector_search_config_hash": H1,
        "lexical_method_version": "lex-v2",
        "lexical_config_hash": H2,
        "lexical_postgres_version": "postgres-18",
        "lexical_regconfig_identity": "english",
        "fusion_version": "rank-interleave-v2",
        "reverse_budget_per_inserted_chunk": 4,
        "forward_budget_per_requirement": 4,
        "verifier_execution_spec_hash": H3,
        "decision_policy_version": "decision-v2",
        "lineage_safety_override": False,
    }

    for name, changed in mutations.items():
        values = {**base, name: changed}
        assert (
            digests.candidate_policy_manifest_digest(**values)  # type: ignore[arg-type]
            != expected
        ), name


def test_scope_and_job_nullable_branches_have_distinct_identities() -> None:
    forward = digests.discovery_scope_contract_digest(
        direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
        requirement_version_id="req-1",
        inserted_chunk_version_id=None,
        candidate_policy_id="policy",
        requirement_registry_snapshot_digest_value=H1,
        active_chunk_snapshot_digest_value=H2,
    )
    reverse = digests.discovery_scope_contract_digest(
        direction=M5DiscoveryDirection.REVERSE_CHUNK,
        requirement_version_id=None,
        inserted_chunk_version_id="chunk-1",
        candidate_policy_id="policy",
        requirement_registry_snapshot_digest_value=H1,
        active_chunk_snapshot_digest_value=H2,
    )
    root_payload = digests.job_payload_digest(
        job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
        candidate_policy_id="policy",
        candidate_policy_manifest_hash=H1,
        parent_job_id=None,
        semantic_pair_digest_value=None,
        scope_contract_digest=forward,
        requirement_registry_snapshot_digest_value=H2,
        active_chunk_snapshot_digest_value=H3,
        role_template_hash=H4,
        execution_spec_hash=H1,
        expandable=True,
    )
    child_payload = digests.job_payload_digest(
        job_kind=M5JobKind.VERIFY_REQUIREMENT_PAIR,
        candidate_policy_id="policy",
        candidate_policy_manifest_hash=H1,
        parent_job_id=H2,
        semantic_pair_digest_value=H3,
        scope_contract_digest=forward,
        requirement_registry_snapshot_digest_value=H2,
        active_chunk_snapshot_digest_value=H3,
        role_template_hash=H4,
        execution_spec_hash=H1,
        expandable=False,
    )

    assert forward != reverse
    assert root_payload != child_payload


def test_canonical_set_recipes_are_permutation_stable() -> None:
    assert digests.child_set_digest((H3, H1, H3, H2)) == (
        digests.child_set_digest((H2, H1, H3))
    )
    assert digests.discovery_scope_closure_digest(H4, (H3, H1, H3)) == (
        digests.discovery_scope_closure_digest(H4, (H1, H3))
    )
    assert digests.requirement_root_set_digest((H2, H1, H2)) == (
        digests.requirement_root_set_digest((H1, H2))
    )


def test_ordered_result_sequences_are_not_silently_canonicalized() -> None:
    one = digests.requirement_discovery_result_digest(
        root_job_id="root",
        scope_contract_digest=H1,
        termination="snapshot_exhausted",
        hit_digests=(H2, H3),
        selection_digests=(H4, H1),
        approximate_selection_count=1,
        mandatory_lineage_only_count=1,
    )
    permuted = digests.requirement_discovery_result_digest(
        root_job_id="root",
        scope_contract_digest=H1,
        termination="snapshot_exhausted",
        hit_digests=(H3, H2),
        selection_digests=(H4, H1),
        approximate_selection_count=1,
        mandatory_lineage_only_count=1,
    )

    assert one != permuted


@pytest.mark.parametrize("counter", [-1, 0, 1])
def test_runtime_work_recipe_encodes_signed_int_domain(counter: int) -> None:
    counters = [0] * 32
    counters[0] = counter
    actual = digests.runtime_work_digest(counters)

    assert len(actual) == 64
    if counter != 0:
        assert actual != digests.runtime_work_digest([0] * 32)


def test_receipt_replay_boolean_is_sidecar_but_activation_replay_is_not_identity() -> (
    None
):
    activation = digests.activation_receipt_digest(
        activation_id="activation",
        payload_hash=H1,
        base_m4_epoch_id=1,
        m4_publication_id="publication",
        m5_publication_epoch_id=1,
        mode_revision=2,
        bootstrap_state_hash=H2,
    )
    assert activation == digests.activation_receipt_digest(
        activation_id="activation",
        payload_hash=H1,
        base_m4_epoch_id=1,
        m4_publication_id="publication",
        m5_publication_epoch_id=1,
        mode_revision=2,
        bootstrap_state_hash=H2,
    )
    assert digests.open_event_receipt_binding_digest(
        epoch_id=1,
        replayed=False,
        already_sealed=False,
        publication_id=None,
        already_failed=False,
        failure_reason=None,
    ) != digests.open_event_receipt_binding_digest(
        epoch_id=1,
        replayed=True,
        already_sealed=False,
        publication_id=None,
        already_failed=False,
        failure_reason=None,
    )


def test_runtime_schema_bundle_binds_exact_path_migration_and_core_hash() -> None:
    actual = digests.runtime_schema_bundle_digest(
        migration_sha256=H1, core_schema_bundle_sha256=H2
    )

    assert actual != digests.runtime_schema_bundle_digest(
        migration_sha256=H3, core_schema_bundle_sha256=H2
    )
    assert actual != digests.runtime_schema_bundle_digest(
        migration_sha256=H1, core_schema_bundle_sha256=H4
    )


def test_combined_delta_and_changed_state_recipes_bind_complete_rows() -> None:
    delta = StatusDelta("event", "claim", "claim-1", "a", "b", "reason")
    delta_hash = digests.combined_status_delta_set_digest((delta,))
    changed = digests.changed_state_reference_digest(
        (M5StateReferenceKind.CLAIM_STATE, "claim-1", 4, 2, H1)
    )

    assert delta_hash != digests.combined_status_delta_set_digest(
        (StatusDelta("event", "claim", "claim-1", "a", "b", "reason-2"),)
    )
    assert digests.changed_state_set_digest((changed,)) != (
        digests.changed_state_set_digest(())
    )


def test_requirement_state_artifact_matches_independent_framing() -> None:
    actual = digests.requirement_state_artifact_digest(
        requirement_version_id="requirement-1",
        witness_hashes=(H1,),
        supporting_observation_ids=("observation-1",),
        witness_count=1,
        satisfied=True,
        decision_policy_version="policy-1",
    )

    assert actual == _framed_sha256(
        "m5-requirement-state-artifact-v2",
        "text",
        "requirement-1",
        "sequence",
        "int",
        "1",
        "sha256",
        H1,
        "sequence",
        "int",
        "1",
        "text",
        "observation-1",
        "int",
        "1",
        "bool",
        "1",
        "text",
        "policy-1",
    )


def test_state_artifact_recipes_bind_every_persisted_field() -> None:
    requirement = {
        "requirement_version_id": "requirement-1",
        "witness_hashes": (H1, H2),
        "supporting_observation_ids": ("observation-1", "observation-2"),
        "witness_count": 2,
        "satisfied": True,
        "decision_policy_version": "policy-1",
    }
    requirement_hash = digests.requirement_state_artifact_digest(**requirement)
    requirement_mutations = (
        {"requirement_version_id": "requirement-2"},
        {"witness_hashes": (H2, H1)},
        {"supporting_observation_ids": ("observation-2", "observation-1")},
        {"witness_count": 3},
        {"satisfied": False},
        {"decision_policy_version": "policy-2"},
    )
    for mutation in requirement_mutations:
        assert (
            digests.requirement_state_artifact_digest(**(requirement | mutation))
            != requirement_hash
        )

    group = {
        "group_version_id": "group-1",
        "requirement_count": 2,
        "satisfied_count": 2,
        "matching_size": 2,
        "complete": True,
        "decision_policy_version": "policy-1",
        "certificate_digest": H1,
    }
    group_hash = digests.group_state_artifact_digest(**group)
    group_mutations = (
        {"group_version_id": "group-2"},
        {"requirement_count": 3},
        {"satisfied_count": 1},
        {"matching_size": 1},
        {"complete": False},
        {"decision_policy_version": "policy-2"},
        {"certificate_digest": H2},
        {"certificate_digest": None},
    )
    for mutation in group_mutations:
        assert digests.group_state_artifact_digest(**(group | mutation)) != group_hash

    claim = {
        "claim_id": "claim-1",
        "support_count": 2,
        "refute_count": 1,
        "best_support_score": 0.75,
        "best_refute_score": -0.0,
        "supporting_observation_ids": ("observation-1", "observation-2"),
        "refuting_observation_ids": ("observation-3", "observation-4"),
        "complete_group_count": 2,
        "complete_group_ids": ("group-1", "group-2"),
        "status": "conflicted",
        "decision_policy_version": "policy-1",
        "certificate_digest": H3,
    }
    claim_hash = digests.claim_state_artifact_digest(**claim)
    claim_mutations = (
        {"claim_id": "claim-2"},
        {"support_count": 3},
        {"refute_count": 2},
        {"best_support_score": 0.5},
        {"best_support_score": None},
        {"best_refute_score": 0.0},
        {"best_refute_score": None},
        {"supporting_observation_ids": ("observation-2", "observation-1")},
        {"refuting_observation_ids": ("observation-4", "observation-3")},
        {"complete_group_count": 3},
        {"complete_group_ids": ("group-2", "group-1")},
        {"status": "supported"},
        {"decision_policy_version": "policy-2"},
        {"certificate_digest": H4},
    )
    for mutation in claim_mutations:
        assert digests.claim_state_artifact_digest(**(claim | mutation)) != claim_hash

    answer = {
        "answer_version_id": "answer-1",
        "required_claim_count": 4,
        "supported_count": 1,
        "unsupported_count": 1,
        "refuted_count": 1,
        "conflicted_count": 1,
        "status": "conflicted",
    }
    answer_hash = digests.answer_state_artifact_digest(**answer)
    answer_mutations = (
        {"answer_version_id": "answer-2"},
        {"required_claim_count": 5},
        {"supported_count": 2},
        {"unsupported_count": 2},
        {"refuted_count": 2},
        {"conflicted_count": 2},
        {"status": "valid"},
    )
    for mutation in answer_mutations:
        assert (
            digests.answer_state_artifact_digest(**(answer | mutation)) != answer_hash
        )


def test_completion_reason_and_verifier_negative_zero_mutate_identity() -> None:
    inactive = digests.job_completion_digest(
        logical_job_id_value=H1,
        payload_hash=H2,
        execution_spec_hash=H3,
        terminal_state=M5JobState.COMPLETED_INACTIVE,
        result_artifact_id=H4,
        result_artifact_hash=H1,
        scope_closure_digest=None,
        child_set_hash=None,
        archive_reason=M5TerminalReason.CHUNK_INACTIVE,
    )
    assert inactive != digests.job_completion_digest(
        logical_job_id_value=H1,
        payload_hash=H2,
        execution_spec_hash=H3,
        terminal_state=M5JobState.COMPLETED_INACTIVE,
        result_artifact_id=H4,
        result_artifact_hash=H1,
        scope_closure_digest=None,
        child_set_hash=None,
        archive_reason=M5TerminalReason.SUBJECT_INACTIVE,
    )

    base = dict(
        semantic_pair_digest_value=H1,
        pair_input_hash=H2,
        execution_spec_hash=H3,
        model_artifact_id="artifact",
        model_id="model",
        model_revision="revision",
        prompt_artifact_id="prompt",
        prompt_version="prompt-v1",
        calibration_version="cal-v1",
        calibration_artifact_hash=None,
        temperature=1.0,
        decision_policy_version="decision-v1",
        decision_policy_hash=H4,
        support_score=0.5,
        refute_score=0.25,
        neutral_score=0.25,
        raw_logits=(-0.0, 1.0, 2.0),
        raw_output_hash=H1,
        operational_label=VerificationLabel.SUPPORT,
    )
    negative_zero = digests.requirement_verifier_result_digest(
        **base  # type: ignore[arg-type]
    )
    positive = digests.requirement_verifier_result_digest(
        **{**base, "raw_logits": (0.0, 1.0, 2.0)}  # type: ignore[arg-type]
    )
    assert negative_zero != positive


def test_d24_operational_recipes_match_independent_framing_and_goldens() -> None:
    config = digests.runtime_operational_config_digest(30_000)
    terminal = digests.lease_terminal_projection_digest(
        logical_job_id=H1,
        terminal_state=M5JobState.COMPLETED_ACTIVE,
        terminal_reason=None,
        completion_digest=H2,
    )
    direct_terminal = digests.typed_direct_terminal_projection_digest(
        job_id="direct-job",
        terminal_state="terminal_failed",
        terminal_reason="verifier_error",
        m4_completion_digest=None,
        completed_revision=9,
    )
    dispatch = digests.dispatch_record_digest(
        epoch_id=7,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=H1,
        logical_job_id=H2,
        attempt_ordinal=1,
        job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL.value,
        fallback_required=True,
        dispatched_revision=2,
        maximum_ambiguous_call_work_digest=H3,
    )
    provenance = digests.requirement_root_provenance_digest(
        epoch_id=7, root_job_id=H1, fallback_required=True
    )
    evidence = digests.attempt_execution_evidence_digest(
        epoch_id=7,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=H1,
        disposition=M5ExecutionEvidenceDisposition.RETURNED,
        result_or_error_hash=H2,
        attempt_work_digest=H3,
        attempt_timing_digest=H4,
    )
    contribution = digests.runtime_work_contribution_key_digest(
        epoch_id=7,
        contribution_kind=M5RuntimeWorkContributionKind.M5_ACQUISITION,
        source_id=H1,
    )
    epoch_failure_source = digests.epoch_failure_contribution_source_digest(
        structural_event_id="event-λ",
        failure_reason=M5RunFailureReason.INVALID_ARTIFACT,
    )
    terminal_failure_source = digests.terminal_job_failure_contribution_source_digest(
        logical_job_id=H1,
        terminal_reason=M5TerminalReason.VERIFIER_ERROR,
        error_hash=H2,
    )
    seal_source = digests.seal_contribution_source_digest(
        structural_event_id="event-λ",
        combined_status_delta_set_hash=H1,
        changed_state_set_hash=H2,
        publication_id="publication-7",
    )
    expired = digests.expired_attempt_return_digest(
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        epoch_id=7,
        attempt_id=H1,
        logical_job_id=H2,
        worker_output_digest=H3,
        worker_artifact_hash=H4,
        activity_snapshot_epoch_id=7,
        activity_snapshot_revision=3,
        received_after_terminal=False,
    )

    assert config == _framed_sha256("m5-runtime-operational-config-v1", "int", "30000")
    assert terminal == _framed_sha256(
        "m5-lease-terminal-projection-v1",
        "enum",
        "requirement",
        "text",
        H1,
        "enum",
        "completed_active",
        "null",
        "sha256",
        H2,
    )
    assert direct_terminal == _framed_sha256(
        "m5-typed-direct-terminal-projection-v1",
        "enum",
        "direct",
        "text",
        "direct-job",
        "enum",
        "terminal_failed",
        "text",
        "verifier_error",
        "null",
        "int",
        "9",
    )
    assert dispatch == _framed_sha256(
        "m5-dispatch-record-v1",
        "int",
        "7",
        "enum",
        "requirement",
        "text",
        H1,
        "text",
        H2,
        "int",
        "1",
        "text",
        "forward_requirement_retrieval",
        "bool",
        "1",
        "int",
        "2",
        "sha256",
        H3,
    )
    assert provenance == _framed_sha256(
        "m5-requirement-root-provenance-v1",
        "int",
        "7",
        "text",
        H1,
        "bool",
        "1",
    )
    assert evidence == _framed_sha256(
        "m5-attempt-execution-evidence-v1",
        "int",
        "7",
        "enum",
        "requirement",
        "text",
        H1,
        "enum",
        "returned",
        "sha256",
        H2,
        "sha256",
        H3,
        "sha256",
        H4,
    )
    assert contribution == _framed_sha256(
        "m5-runtime-work-contribution-key-v1",
        "int",
        "7",
        "enum",
        "m5_acquisition",
        "text",
        H1,
    )
    assert epoch_failure_source == _framed_sha256(
        "m5-epoch-failure-contribution-source-v1",
        "text",
        "event-λ",
        "enum",
        "invalid_artifact",
    )
    assert terminal_failure_source == _framed_sha256(
        "m5-terminal-job-failure-contribution-source-v1",
        "text",
        H1,
        "enum",
        "verifier_error",
        "sha256",
        H2,
    )
    assert seal_source == _framed_sha256(
        "m5-seal-contribution-source-v1",
        "text",
        "event-λ",
        "sha256",
        H1,
        "sha256",
        H2,
        "text",
        "publication-7",
    )
    assert expired == _framed_sha256(
        "m5-expired-attempt-return-v1",
        "enum",
        "requirement",
        "int",
        "7",
        "text",
        H1,
        "text",
        H2,
        "sha256",
        H3,
        "sha256",
        H4,
        "int",
        "7",
        "int",
        "3",
        "enum",
        "attempt_expired",
        "bool",
        "0",
    )
    assert (
        config,
        terminal,
        direct_terminal,
        dispatch,
        provenance,
        evidence,
        contribution,
        epoch_failure_source,
        terminal_failure_source,
        seal_source,
        expired,
    ) == (
        "70c8bebd9b5d04a906b0020bb587b538cb9f32b1e3a7e624ebac11440c594c3d",
        "fe7205de5de5b836cfb9d76fb76d2031b5045809afb1b3a78a82f8926246ac02",
        "f7f9379e31982a8a07a7d0c2a315daabb8e547dfc338c10ab71ea69da41eff05",
        "5e3202f6556fd8773939bd3a34aea6604ba1498fe38a12f2be85d1250c08fe71",
        "ac8e9aa3422d6701c276539c45ce56756a49407777c050383e7e55feb414df4f",
        "74c98d15ae0a6339bda4bcdfadae6f731afa9e420beb2478264a46233b0541df",
        "be1d370429d395983ae9b7b8d7d4b9a5750211bc2c83ee69b6043a2a95ec5bab",
        "e42ac737cafa913e3870516b007f7d0b80661b97bf0062528c719daedc018214",
        "7ffb6313c9fe912ce0580e1b8cd64f0ba9f892952a8c1e41fd4abd4c7bbfcfda",
        "a8011f03a39e133c11eaaccca5b597b964b0d03e57653e2b83630e6dc4c1dcf4",
        "bb7f35a704e3d78a4fd7bfadbf6ab83a4db2a1adbe8d0925792b869234eb9b5e",
    )


def test_d24_dispatch_and_evidence_digests_bind_every_field() -> None:
    dispatch_values: dict[str, object] = {
        "epoch_id": 7,
        "subgraph": M5RuntimeSubgraph.REQUIREMENT,
        "attempt_id": H1,
        "logical_job_id": H2,
        "attempt_ordinal": 1,
        "job_kind": M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL.value,
        "fallback_required": True,
        "dispatched_revision": 2,
        "maximum_ambiguous_call_work_digest": H3,
    }
    dispatch = digests.dispatch_record_digest(**dispatch_values)  # type: ignore[arg-type]
    dispatch_mutations = (
        {"epoch_id": 8},
        {"subgraph": M5RuntimeSubgraph.DIRECT},
        {"attempt_id": H4},
        {"logical_job_id": H4},
        {"attempt_ordinal": 2},
        {"job_kind": M5JobKind.REVERSE_REQUIREMENT_DISCOVERY.value},
        {"fallback_required": False},
        {"dispatched_revision": 3},
        {"maximum_ambiguous_call_work_digest": H4},
    )
    for mutation in dispatch_mutations:
        assert (
            digests.dispatch_record_digest(
                **(dispatch_values | mutation)  # type: ignore[arg-type]
            )
            != dispatch
        )

    evidence_values: dict[str, object] = {
        "epoch_id": 7,
        "subgraph": M5RuntimeSubgraph.REQUIREMENT,
        "attempt_id": H1,
        "disposition": M5ExecutionEvidenceDisposition.RETURNED,
        "result_or_error_hash": H2,
        "attempt_work_digest": H3,
        "attempt_timing_digest": H4,
    }
    evidence = digests.attempt_execution_evidence_digest(
        **evidence_values  # type: ignore[arg-type]
    )
    evidence_mutations = (
        {"epoch_id": 8},
        {"subgraph": M5RuntimeSubgraph.DIRECT},
        {"attempt_id": H4},
        {"disposition": M5ExecutionEvidenceDisposition.RETRYABLE_FAILURE},
        {"result_or_error_hash": H1},
        {"attempt_work_digest": H1},
        {"attempt_timing_digest": H1},
    )
    for mutation in evidence_mutations:
        assert (
            digests.attempt_execution_evidence_digest(
                **(evidence_values | mutation)  # type: ignore[arg-type]
            )
            != evidence
        )


def test_d24_timing_recipes_distinguish_missing_measured_zero_and_partial_shape() -> (
    None
):
    missing_values = {
        "required_interval_observed": False,
        "coordinator_non_db_non_neural_ns": None,
        "neural_wall_ns": None,
        "postgres_roundtrip_wall_ns": None,
        "external_io_wall_ns": None,
        "end_to_end_wall_ns": None,
        "postgres_server_execution_ns": None,
        "postgres_lock_wait_ns": None,
        "postgres_wal_bytes": None,
        "postgres_shared_block_reads": None,
    }
    measured_values = {
        **missing_values,
        "required_interval_observed": True,
        "coordinator_non_db_non_neural_ns": 0,
        "neural_wall_ns": 0,
        "postgres_roundtrip_wall_ns": 0,
        "external_io_wall_ns": 0,
        "end_to_end_wall_ns": 0,
    }
    missing = digests.runtime_timing_observation_digest(**missing_values)
    measured_zero = digests.runtime_timing_observation_digest(**measured_values)
    partial = digests.runtime_timing_observation_digest(
        **(missing_values | {"neural_wall_ns": 0})
    )
    assert missing == _framed_sha256(
        "m5-runtime-timing-observation-v1",
        "bool",
        "0",
        *("null" for _ in range(9)),
    )
    assert measured_zero == _framed_sha256(
        "m5-runtime-timing-observation-v1",
        "bool",
        "1",
        "int",
        "0",
        "int",
        "0",
        "int",
        "0",
        "int",
        "0",
        "int",
        "0",
        "null",
        "null",
        "null",
        "null",
    )
    assert len({missing, measured_zero, partial}) == 3
    attempt_timing = digests.attempt_runtime_timing_digest(
        epoch_id=7,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=H1,
        observation_digest=missing,
    )
    transition = digests.transition_call_timing_digest(
        epoch_id=7,
        contribution_kind=M5RuntimeWorkContributionKind.M5_ACQUISITION,
        source_id=H1,
        contribution_key_digest=H2,
        anchor_revision=2,
        observation_digest=measured_zero,
    )
    assert (missing, measured_zero, attempt_timing, transition) == (
        "ac9545e61a49b6882fa04620790b391cc1ae11d1238e135ab037a2289ef7cf88",
        "02c92738c6a0b6e8a8db6946f51fec832f36a7eccb1cfcd06bd502469a435112",
        "4f53691874dabade74c67141897667e1da6721d813a9d169ae18096a7a54a506",
        "8e2b12b89679ed19944f7096d34869def3f74f4ca3b7ad25fa5e33a425a1465d",
    )


def test_d24_typed_direct_binding_order_null_and_f64_are_byte_total() -> None:
    job = digests.typed_direct_late_job_binding_digest(
        job_id="job",
        event_id="event",
        job_kind="verify_pair",
        candidate_policy_id="policy",
        payload_hash=H1,
        execution_spec_hash=H2,
        parent_job_id=None,
        pair_claim_id="claim",
        pair_chunk_version_id="chunk",
        target_claim_id=None,
        target_chunk_version_id=None,
        expandable=False,
    )
    attempt = digests.typed_direct_late_attempt_binding_digest(
        attempt_id="attempt",
        job_id="job",
        execution_spec_hash=H2,
        attempt_ordinal=1,
        lease_token_hash=H3,
    )
    completion_ab = digests.typed_direct_late_completion_binding_digest(
        job_id="job",
        payload_hash=H1,
        execution_spec_hash=H2,
        result_artifact_id="result",
        result_artifact_hash=H3,
        terminal_state="completed_active",
        completion_digest=H4,
        child_parent_job_id="job",
        child_completion_digest=H1,
        child_set_hash=H2,
        child_job_ids=("child-b", "child-a"),
    )
    completion_ba = digests.typed_direct_late_completion_binding_digest(
        job_id="job",
        payload_hash=H1,
        execution_spec_hash=H2,
        result_artifact_id="result",
        result_artifact_hash=H3,
        terminal_state="completed_active",
        completion_digest=H4,
        child_parent_job_id="job",
        child_completion_digest=H1,
        child_set_hash=H2,
        child_job_ids=("child-a", "child-b"),
    )
    completion_null = digests.typed_direct_late_completion_binding_digest(
        job_id="job",
        payload_hash=H1,
        execution_spec_hash=H2,
        result_artifact_id="result",
        result_artifact_hash=H3,
        terminal_state="completed_active",
        completion_digest=H4,
        child_parent_job_id=None,
        child_completion_digest=None,
        child_set_hash=None,
        child_job_ids=(),
    )
    assert completion_ab == completion_ba
    assert completion_ab != completion_null

    hits = (
        (7, "claim-b", "chunk", "policy", "vector", 2, -0.0, H2),
        (7, "claim-a", "chunk", "policy", "lexical", 1, None, H1),
    )
    pairs = (
        (7, "claim-b", "chunk", "policy", 2, ("vector",), False),
        (7, "claim-a", "chunk", "policy", 1, ("lexical",), False),
    )
    discovery = digests.typed_direct_late_discovery_binding_digest(
        root_job_id="job",
        result_artifact_id="result",
        result_artifact_hash=H3,
        fallback_satisfied=True,
        channel_hit_count=2,
        admitted_pair_count=2,
        channel_set_hash=H1,
        admitted_pair_set_hash=H2,
        channel_hits=hits,
        admitted_pairs=pairs,
    )
    reordered = digests.typed_direct_late_discovery_binding_digest(
        root_job_id="job",
        result_artifact_id="result",
        result_artifact_hash=H3,
        fallback_satisfied=True,
        channel_hit_count=2,
        admitted_pair_count=2,
        channel_set_hash=H1,
        admitted_pair_set_hash=H2,
        channel_hits=tuple(reversed(hits)),
        admitted_pairs=tuple(reversed(pairs)),
    )
    positive_zero = digests.typed_direct_late_discovery_binding_digest(
        root_job_id="job",
        result_artifact_id="result",
        result_artifact_hash=H3,
        fallback_satisfied=True,
        channel_hit_count=2,
        admitted_pair_count=2,
        channel_set_hash=H1,
        admitted_pair_set_hash=H2,
        channel_hits=(hits[0][:-2] + (0.0, H2), hits[1]),
        admitted_pairs=pairs,
    )
    assert discovery == reordered
    assert discovery != positive_zero
    scope_null = digests.typed_direct_late_scope_binding_digest(
        root_job_id="job",
        epoch_id=7,
        registry_snapshot_id="registry",
        registered_claim_ids=("claim-a", "claim-b"),
        closed=False,
        persisted_scope_kind=M5TypedDirectScopeKind.ALL_REGISTERED_CLAIMS,
        explicit_claim_ids=None,
        closed_revision=None,
    )
    scope_explicit = digests.typed_direct_late_scope_binding_digest(
        root_job_id="job",
        epoch_id=7,
        registry_snapshot_id="registry",
        registered_claim_ids=("claim-a", "claim-b"),
        closed=True,
        persisted_scope_kind=M5TypedDirectScopeKind.EXPLICIT_CLAIMS,
        explicit_claim_ids=("claim-a", "claim-b"),
        closed_revision=4,
    )
    assert scope_null != scope_explicit
    assert (job, attempt, completion_ab, discovery, scope_null, scope_explicit) == (
        "ebf24133306c2db15d714da20a15883b27e045e461195eab08d887c460705e46",
        "0d76bcce5b7708206f374f36fa183442f10ddd41dddf82b3c58c569bda17e43a",
        "8e39e199a4f5c4ff7fce29984eeb7843ee44ba30e4048b58ee4bc513a2d73d36",
        "fc69e618bc2ec0c6a32b80636211f9e7f5f93639289ea603fe715bca4818fa83",
        "122ab69143303066014f8fe917ffa83c049b9c3d2c79ab8b2d70c2d9e83cfcc4",
        "032e4c07353be263a509276378743b381c2f0729c436e4b7abfdd432d5b2a7ef",
    )


def test_d24_typed_direct_verifier_option_and_f64_members_all_bind() -> None:
    execution: digests.TypedDirectVerificationExecutionValues = (
        "observation",
        "job",
        H1,
        "model-artifact",
        "prompt-artifact",
        H2,
        H3,
        "calibration",
        H4,
        1.0,
        (-0.0, 1.0, 2.0),
        H1,
        None,
    )
    values = {
        "result_artifact_id": "result",
        "result_artifact_hash": H1,
        "verification_execution_present": True,
        "verification_execution": execution,
        "observation_id": "observation",
        "observation_subject_kind": SubjectKind.CLAIM,
        "observation_subject_id": "claim",
        "observation_chunk_version_id": "chunk",
        "observation_task_type": "verify_support_v1",
        "observation_support_score": 0.5,
        "observation_refute_score": 0.25,
        "observation_neutral_score": 0.25,
        "observation_model_id": "model",
        "observation_model_version": "revision",
        "observation_prompt_version": "prompt",
        "observation_input_hash": H2,
        "observation_produced_epoch": 7,
        "observation_raw_output_hash": H1,
        "observation_eligible_for_currency": True,
        "requested_make_effective": True,
    }
    present = digests.typed_direct_late_verifier_binding_digest(
        **values  # type: ignore[arg-type]
    )
    reused = digests.typed_direct_late_verifier_binding_digest(
        **(values | {"verification_execution": execution[:-1] + ("prior",)})  # type: ignore[arg-type]
    )
    positive_zero_execution = execution[:10] + ((0.0, 1.0, 2.0),) + execution[11:]
    positive_zero = digests.typed_direct_late_verifier_binding_digest(
        **(values | {"verification_execution": positive_zero_execution})  # type: ignore[arg-type]
    )
    absent = digests.typed_direct_late_verifier_binding_digest(
        **(
            values
            | {
                "verification_execution_present": False,
                "verification_execution": None,
                "requested_make_effective": False,
            }
        )  # type: ignore[arg-type]
    )
    assert len({present, reused, positive_zero, absent}) == 4
    for mutation in (
        {"result_artifact_id": "other-result"},
        {"result_artifact_hash": H2},
        {"observation_id": "other-observation"},
        {"observation_subject_kind": SubjectKind.REQUIREMENT},
        {"observation_subject_id": "other-claim"},
        {"observation_chunk_version_id": "other-chunk"},
        {"observation_task_type": "other-task"},
        {"observation_support_score": 0.75},
        {"observation_refute_score": 0.5},
        {"observation_neutral_score": 0.0},
        {"observation_model_id": "other-model"},
        {"observation_model_version": "other-revision"},
        {"observation_prompt_version": "other-prompt"},
        {"observation_input_hash": H3},
        {"observation_produced_epoch": 8},
        {"observation_raw_output_hash": H2},
        {"observation_eligible_for_currency": False},
        {"requested_make_effective": False},
    ):
        assert (
            digests.typed_direct_late_verifier_binding_digest(
                **(values | mutation)  # type: ignore[arg-type]
            )
            != present
        )

    envelope = digests.typed_direct_late_return_envelope_digest(
        epoch_id=7,
        return_kind=M5TypedDirectReturnKind.VERIFIER,
        job_binding_digest=H1,
        attempt_binding_digest=H2,
        completion_binding_digest=H3,
        discovery_binding_digest=None,
        scope_binding_digest=None,
        verifier_binding_digest=present,
    )
    assert (present, absent, envelope) == (
        "125b107fa54d6d713d2f7c24875173c8c29078de0a122e677821531ff9617564",
        "b8eb078b3afb631cf46be6d835fb75ba18577cede0975243b1064f6d1fdc1577",
        "0c5ddb0f9477172ecabc3c5fe7415a43fa3762bddeef330e18f89404df9b6910",
    )


class _TestEnum(StrEnum):
    A = "a"


def test_invalid_hash_bool_int_and_nonfinite_runtime_inputs_are_rejected() -> None:
    with pytest.raises(ValidationError):
        digests.semantic_pair_digest(SubjectKind.REQUIREMENT, "req", "")
    with pytest.raises(ValidationError):
        digests.runtime_work_digest((True,))
    with pytest.raises(ValidationError):
        digests.requirement_channel_hit_digest(
            epoch_id=1,
            root_job_id="root",
            scope_contract_digest=H1,
            semantic_pair_digest_value=H2,
            candidate_policy_id="policy",
            channel=M5RequirementAdmissionChannel.VECTOR,
            rank=1,
            score=math.nan,
            channel_artifact_hash=H3,
        )
    with pytest.raises(ValidationError):
        digests.event_run_logical_result_digest(
            event_id="event",
            payload_hash=H1,
            epoch_id=1,
            sealed_or_failed_outcome=_TestEnum.A,
            original_open_receipt_binding_hash=H2,
            original_publication_receipt_binding_hash=None,
            event_work_digest=H3,
            combined_status_delta_set_hash=H4,
            changed_state_set_hash="not-a-hash",
            failure_reason=M5RunFailureReason.INVALID_ARTIFACT,
        )


def test_d25_empty_logical_output_is_frozen_71_byte_image() -> None:
    from groundloop.m5.incremental_overlay import _logical_output_image

    digest, size, preimage = digests.logical_output_digest(())
    assert size == len(preimage) == 71
    assert preimage == _logical_output_image(())
    assert digest == "b4e641b66a06cb7d204377c37cfe031d958ce6d959832620fc2e9441339581c3"


def test_d25_logical_output_enforces_nine_block_order() -> None:
    from groundloop.m5.incremental_overlay import _logical_output_image

    records = (("requirement_state", "r", None), ("status_delta", "s", None))
    assert digests.logical_output_preimage(records) == _logical_output_image(records)
    with pytest.raises(ValidationError):
        digests.logical_output_preimage(tuple(reversed(records)))
    with pytest.raises(ValidationError):
        digests.logical_output_preimage((("requirement-state", "r", None),))
    with pytest.raises(ValidationError):
        digests.logical_output_preimage(list(records))  # type: ignore[arg-type]

    class StringAlias(str):
        pass

    with pytest.raises(ValidationError):
        digests.logical_output_preimage(
            (("requirement_state", "r", StringAlias("value")),)
        )


def test_d25_matching_work_is_exactly_37_ordered_ints() -> None:
    zero = digests.matching_work_digest((0,) * 37)
    changed = digests.matching_work_digest((1,) + (0,) * 36)
    assert zero == "1a1d36b3bb19c2c30cc27b6c4b5967a9f59a86169108b23732e65e4fd382bb89"
    assert changed != zero
    with pytest.raises(ValidationError):
        digests.matching_work_digest((0,) * 36)


def test_d25_point_encoder_rejects_same_named_impostor() -> None:
    impostor = type(
        "M5MatchingMaskWorking",
        (),
        {
            "layer": "working",
            "epoch_id": 1,
            "group_version_id": "g",
            "text_hash": H1,
            "mask": 0,
            "updated_revision": 1,
        },
    )()
    with pytest.raises(ValidationError):
        digests.matching_point_fields(impostor)
    logical_impostor = type(
        "RequirementState",
        (),
        {
            "requirement_version_id": "r",
            "witness_hashes": (),
            "supporting_observation_ids": (),
            "witness_count": 0,
            "satisfied": False,
        },
    )()
    with pytest.raises(ValidationError):
        digests.logical_output_preimage((("requirement_state", "r", logical_impostor),))


def test_d25_schema_bundle_binds_017_path_and_016_bundle() -> None:
    assert digests.persisted_matching_schema_bundle_digest(H1, H2) == (
        "5b194ed6f2a8580f76fbe41d8ce96c24cf2018d60f016165a799905f7f49d42c"
    )


def test_d25_logical_dataclass_allowlist_matches_accepted_encoder() -> None:
    from groundloop.m5.domain import RequirementState
    from groundloop.m5.incremental_overlay import _logical_output_image

    state = RequirementState("requirement", (H1,), ("observation",), 1, True)
    records = (("requirement_state", "requirement", state),)
    assert digests.logical_output_preimage(records) == _logical_output_image(records)
