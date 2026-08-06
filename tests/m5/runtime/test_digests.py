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
    M5JobKind,
    M5JobState,
    M5RequirementAdmissionChannel,
    M5RunFailureReason,
    M5StateReferenceKind,
    M5TerminalReason,
    M5TextNormalizerProvenance,
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
        assert digests.requirement_state_artifact_digest(
            **(requirement | mutation)
        ) != requirement_hash

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
            digests.answer_state_artifact_digest(**(answer | mutation))
            != answer_hash
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
