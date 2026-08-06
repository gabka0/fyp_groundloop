"""Byte-total digest recipes for the GroundLoop M5 typed runtime.

This module deliberately contains no runtime state or persistence logic.  Each
function is a direct spelling of one frozen recipe in M5-D14 or the M5.4
runtime addendum and delegates framing to :mod:`groundloop.m5.digests`.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from enum import Enum

from groundloop.domain import StatusDelta, SubjectKind
from groundloop.m5.digests import (
    bool_field,
    enum_field,
    f64_field,
    hash_field,
    int_field,
    option_field,
    sequence_field,
    stable_m5_digest,
    text_field,
)

RequirementRegistryRow = tuple[str, str, str, str, str, str]
ActiveChunkRow = tuple[str, str]
AdmittedPairSourceRow = tuple[str, str, str]
AttemptResultValues = tuple[
    str,
    str,
    int,
    str | Enum,
    str | Enum,
    str | Enum,
    int,
    int,
    bool,
    bool | None,
    bool | None,
    bool | None,
    str | Enum | None,
    str | None,
    int | None,
    str | Enum | None,
]
ChangedStateReferenceValues = tuple[str | Enum, str, int, int, str]


def text_normalizer_provenance_digest(
    *,
    normalizer_id: str,
    whitespace_codepoints: Iterable[int],
    boundary_rule: str,
    internal_rule: str,
    other_codepoint_rule: str,
    unicode_normalization_rule: str,
    encoding: str,
    hash_algorithm: str,
) -> str:
    return stable_m5_digest(
        "m5-text-normalizer-provenance-v1",
        text_field(normalizer_id),
        sequence_field(int_field(value) for value in whitespace_codepoints),
        text_field(boundary_rule),
        text_field(internal_rule),
        text_field(other_codepoint_rule),
        text_field(unicode_normalization_rule),
        text_field(encoding),
        text_field(hash_algorithm),
    )


def candidate_policy_manifest_digest(
    *,
    embedding_model_artifact_id: str,
    requirement_role_template_hash: str,
    chunk_role_template_hash: str,
    vector_method_version: str,
    vector_index_kind: str | Enum,
    vector_index_build_config_hash: str,
    vector_search_config_hash: str,
    lexical_method_version: str,
    lexical_config_hash: str,
    lexical_postgres_version: str,
    lexical_regconfig_identity: str,
    fusion_version: str,
    reverse_budget_per_inserted_chunk: int,
    forward_budget_per_requirement: int,
    verifier_execution_spec_hash: str,
    decision_policy_version: str,
    lineage_safety_override: bool,
) -> str:
    return stable_m5_digest(
        "m5-candidate-policy-v2",
        text_field(embedding_model_artifact_id),
        hash_field(requirement_role_template_hash),
        hash_field(chunk_role_template_hash),
        text_field(vector_method_version),
        enum_field(vector_index_kind),
        hash_field(vector_index_build_config_hash),
        hash_field(vector_search_config_hash),
        text_field(lexical_method_version),
        hash_field(lexical_config_hash),
        text_field(lexical_postgres_version),
        text_field(lexical_regconfig_identity),
        text_field(fusion_version),
        int_field(reverse_budget_per_inserted_chunk),
        int_field(forward_budget_per_requirement),
        hash_field(verifier_execution_spec_hash),
        text_field(decision_policy_version),
        bool_field(lineage_safety_override),
    )


def semantic_pair_digest(
    subject_kind: SubjectKind | str,
    subject_id: str,
    chunk_version_id: str,
) -> str:
    return stable_m5_digest(
        "m5-semantic-pair-v2",
        enum_field(subject_kind),
        text_field(subject_id),
        text_field(chunk_version_id),
    )


def requirement_registry_snapshot_digest(
    entries: Sequence[RequirementRegistryRow],
) -> str:
    return stable_m5_digest(
        "m5-requirement-registry-snapshot-v2",
        int_field(len(entries)),
        sequence_field(
            sequence_field(
                (
                    text_field(requirement_version_id),
                    text_field(group_version_id),
                    text_field(group_family_id),
                    text_field(owner_claim_id),
                    text_field(normalized_requirement_text),
                    hash_field(requirement_text_hash),
                )
            )
            for (
                requirement_version_id,
                group_version_id,
                group_family_id,
                owner_claim_id,
                normalized_requirement_text,
                requirement_text_hash,
            ) in entries
        ),
    )


def active_chunk_snapshot_digest(entries: Sequence[ActiveChunkRow]) -> str:
    return stable_m5_digest(
        "m5-active-chunk-snapshot-v2",
        int_field(len(entries)),
        sequence_field(
            sequence_field((text_field(chunk_version_id), hash_field(text_hash)))
            for chunk_version_id, text_hash in entries
        ),
    )


def discovery_scope_contract_digest(
    *,
    direction: str | Enum,
    requirement_version_id: str | None,
    inserted_chunk_version_id: str | None,
    candidate_policy_id: str,
    requirement_registry_snapshot_digest_value: str,
    active_chunk_snapshot_digest_value: str,
) -> str:
    return stable_m5_digest(
        "m5-discovery-scope-contract-v2",
        enum_field(direction),
        option_field(
            text_field(requirement_version_id)
            if requirement_version_id is not None
            else None
        ),
        option_field(
            text_field(inserted_chunk_version_id)
            if inserted_chunk_version_id is not None
            else None
        ),
        text_field(candidate_policy_id),
        hash_field(requirement_registry_snapshot_digest_value),
        hash_field(active_chunk_snapshot_digest_value),
    )


def discovery_scope_closure_digest(
    scope_contract_digest: str,
    semantic_pair_digests: Iterable[str],
) -> str:
    canonical = tuple(sorted(set(semantic_pair_digests)))
    return stable_m5_digest(
        "m5-discovery-scope-closure-v2",
        hash_field(scope_contract_digest),
        sequence_field(hash_field(value) for value in canonical),
    )


def job_payload_digest(
    *,
    job_kind: str | Enum,
    candidate_policy_id: str,
    candidate_policy_manifest_hash: str,
    parent_job_id: str | None,
    semantic_pair_digest_value: str | None,
    scope_contract_digest: str | None,
    requirement_registry_snapshot_digest_value: str,
    active_chunk_snapshot_digest_value: str,
    role_template_hash: str,
    execution_spec_hash: str,
    expandable: bool,
) -> str:
    return stable_m5_digest(
        "m5-job-payload-v2",
        enum_field(job_kind),
        text_field(candidate_policy_id),
        hash_field(candidate_policy_manifest_hash),
        option_field(text_field(parent_job_id) if parent_job_id is not None else None),
        option_field(
            hash_field(semantic_pair_digest_value)
            if semantic_pair_digest_value is not None
            else None
        ),
        option_field(
            hash_field(scope_contract_digest)
            if scope_contract_digest is not None
            else None
        ),
        hash_field(requirement_registry_snapshot_digest_value),
        hash_field(active_chunk_snapshot_digest_value),
        hash_field(role_template_hash),
        hash_field(execution_spec_hash),
        bool_field(expandable),
    )


def logical_job_id(structural_event_id: str, payload_hash: str) -> str:
    return stable_m5_digest(
        "m5-logical-job-v2",
        text_field(structural_event_id),
        hash_field(payload_hash),
    )


def child_set_digest(child_job_ids: Iterable[str]) -> str:
    canonical = tuple(sorted(set(child_job_ids)))
    return stable_m5_digest(
        "m5-child-set-v2",
        sequence_field(text_field(value) for value in canonical),
    )


def job_completion_digest(
    *,
    logical_job_id_value: str,
    payload_hash: str,
    execution_spec_hash: str,
    terminal_state: str | Enum,
    result_artifact_id: str | None,
    result_artifact_hash: str | None,
    scope_closure_digest: str | None,
    child_set_hash: str | None,
    archive_reason: str | Enum | None,
) -> str:
    return stable_m5_digest(
        "m5-job-completion-v2",
        text_field(logical_job_id_value),
        hash_field(payload_hash),
        hash_field(execution_spec_hash),
        enum_field(terminal_state),
        option_field(
            text_field(result_artifact_id) if result_artifact_id is not None else None
        ),
        option_field(
            hash_field(result_artifact_hash)
            if result_artifact_hash is not None
            else None
        ),
        option_field(
            hash_field(scope_closure_digest)
            if scope_closure_digest is not None
            else None
        ),
        option_field(
            hash_field(child_set_hash) if child_set_hash is not None else None
        ),
        option_field(
            enum_field(archive_reason) if archive_reason is not None else None
        ),
    )


def forward_retrieval_execution_spec_digest(
    candidate_policy_manifest_hash: str,
    normalizer_provenance_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-forward-requirement-retrieval-execution-v2",
        hash_field(candidate_policy_manifest_hash),
        hash_field(normalizer_provenance_hash),
    )


def reverse_retrieval_execution_spec_digest(
    candidate_policy_manifest_hash: str,
    normalizer_provenance_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-reverse-requirement-discovery-execution-v2",
        hash_field(candidate_policy_manifest_hash),
        hash_field(normalizer_provenance_hash),
    )


def requirement_verifier_role_binding_digest(
    requirement_role_template_hash: str,
    chunk_role_template_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-requirement-verifier-role-binding-v2",
        hash_field(requirement_role_template_hash),
        hash_field(chunk_role_template_hash),
    )


def requirement_channel_hit_digest(
    *,
    epoch_id: int,
    root_job_id: str,
    scope_contract_digest: str,
    semantic_pair_digest_value: str,
    candidate_policy_id: str,
    channel: str | Enum,
    rank: int,
    score: float | None,
    channel_artifact_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-requirement-channel-hit-v2",
        int_field(epoch_id),
        text_field(root_job_id),
        hash_field(scope_contract_digest),
        hash_field(semantic_pair_digest_value),
        text_field(candidate_policy_id),
        enum_field(channel),
        int_field(rank),
        option_field(f64_field(score) if score is not None else None),
        hash_field(channel_artifact_hash),
    )


def requirement_scope_selection_digest(
    *,
    root_job_id: str,
    scope_contract_digest: str,
    semantic_pair_digest_value: str,
    fused_rank: int,
    reasons: Iterable[str | Enum],
    mandatory_lineage: bool,
) -> str:
    return stable_m5_digest(
        "m5-requirement-scope-selection-v2",
        text_field(root_job_id),
        hash_field(scope_contract_digest),
        hash_field(semantic_pair_digest_value),
        int_field(fused_rank),
        sequence_field(enum_field(reason) for reason in reasons),
        bool_field(mandatory_lineage),
    )


def requirement_discovery_result_digest(
    *,
    root_job_id: str,
    scope_contract_digest: str,
    termination: str | Enum,
    hit_digests: Iterable[str],
    selection_digests: Iterable[str],
    approximate_selection_count: int,
    mandatory_lineage_only_count: int,
) -> str:
    return stable_m5_digest(
        "m5-requirement-discovery-result-v2",
        text_field(root_job_id),
        hash_field(scope_contract_digest),
        enum_field(termination),
        sequence_field(hash_field(value) for value in hit_digests),
        sequence_field(hash_field(value) for value in selection_digests),
        int_field(approximate_selection_count),
        int_field(mandatory_lineage_only_count),
    )


def requirement_discovery_artifact_id(
    root_job_id: str, result_artifact_hash: str
) -> str:
    return stable_m5_digest(
        "m5-requirement-discovery-artifact-v2",
        text_field(root_job_id),
        hash_field(result_artifact_hash),
    )


def requirement_admitted_pair_digest(
    *,
    epoch_id: int,
    semantic_pair_digest_value: str,
    candidate_policy_id: str,
    owner_root_job_id: str,
    sources: Iterable[AdmittedPairSourceRow],
    reasons: Iterable[str | Enum],
    mandatory_lineage: bool,
) -> str:
    return stable_m5_digest(
        "m5-requirement-admitted-pair-v2",
        int_field(epoch_id),
        hash_field(semantic_pair_digest_value),
        text_field(candidate_policy_id),
        text_field(owner_root_job_id),
        sequence_field(
            sequence_field(
                (
                    text_field(root_job_id),
                    hash_field(scope_contract_digest),
                    hash_field(selection_digest),
                )
            )
            for root_job_id, scope_contract_digest, selection_digest in sources
        ),
        sequence_field(enum_field(reason) for reason in reasons),
        bool_field(mandatory_lineage),
    )


def requirement_pair_input_digest(
    *,
    semantic_pair_digest_value: str,
    scope_contract_digest: str,
    candidate_policy_id: str,
    owner_claim_id: str,
    group_version_id: str,
    group_family_id: str,
    requirement_ordinal: int,
    normalized_requirement_text: str,
    requirement_text_hash: str,
    document_version_id: str,
    chunk_index: int,
    chunk_text: str,
    stored_chunk_text_hash: str,
    m5_chunk_text_hash: str,
    chunker_artifact_id: str,
    normalizer_id: str,
    normalizer_provenance_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-requirement-pair-input-v2",
        hash_field(semantic_pair_digest_value),
        hash_field(scope_contract_digest),
        text_field(candidate_policy_id),
        text_field(owner_claim_id),
        text_field(group_version_id),
        text_field(group_family_id),
        int_field(requirement_ordinal),
        text_field(normalized_requirement_text),
        hash_field(requirement_text_hash),
        text_field(document_version_id),
        int_field(chunk_index),
        text_field(chunk_text),
        hash_field(stored_chunk_text_hash),
        hash_field(m5_chunk_text_hash),
        text_field(chunker_artifact_id),
        text_field(normalizer_id),
        hash_field(normalizer_provenance_hash),
    )


def requirement_verifier_result_digest(
    *,
    semantic_pair_digest_value: str,
    pair_input_hash: str,
    execution_spec_hash: str,
    model_artifact_id: str,
    model_id: str,
    model_revision: str,
    prompt_artifact_id: str,
    prompt_version: str,
    calibration_version: str,
    calibration_artifact_hash: str | None,
    temperature: float,
    decision_policy_version: str,
    decision_policy_hash: str,
    support_score: float,
    refute_score: float,
    neutral_score: float,
    raw_logits: Iterable[float],
    raw_output_hash: str,
    operational_label: str | Enum,
) -> str:
    return stable_m5_digest(
        "m5-requirement-verifier-result-v2",
        hash_field(semantic_pair_digest_value),
        hash_field(pair_input_hash),
        hash_field(execution_spec_hash),
        text_field(model_artifact_id),
        text_field(model_id),
        text_field(model_revision),
        text_field(prompt_artifact_id),
        text_field(prompt_version),
        text_field(calibration_version),
        option_field(
            hash_field(calibration_artifact_hash)
            if calibration_artifact_hash is not None
            else None
        ),
        f64_field(temperature),
        text_field(decision_policy_version),
        hash_field(decision_policy_hash),
        f64_field(support_score),
        f64_field(refute_score),
        f64_field(neutral_score),
        sequence_field(f64_field(value) for value in raw_logits),
        hash_field(raw_output_hash),
        enum_field(operational_label),
    )


def requirement_verifier_artifact_id(
    semantic_pair_digest_value: str,
    pair_input_hash: str,
    artifact_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-requirement-verifier-artifact-v2",
        hash_field(semantic_pair_digest_value),
        hash_field(pair_input_hash),
        hash_field(artifact_hash),
    )


def requirement_semantic_observation_id(
    verifier_artifact_id: str,
    task_type: str = "verify_requirement_v1",
) -> str:
    return stable_m5_digest(
        "m5-requirement-semantic-observation-v2",
        hash_field(verifier_artifact_id),
        text_field(task_type),
    )


def job_attempt_id(
    logical_job_id_value: str,
    attempt_ordinal: int,
    execution_spec_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-job-attempt-v2",
        text_field(logical_job_id_value),
        int_field(attempt_ordinal),
        hash_field(execution_spec_hash),
    )


def attempt_output_digest(
    *,
    attempt_id: str,
    logical_job_id_value: str,
    job_epoch_id: int,
    payload_hash: str,
    execution_spec_hash: str,
    result_artifact_id: str,
    result_artifact_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-attempt-output-v2",
        text_field(attempt_id),
        text_field(logical_job_id_value),
        int_field(job_epoch_id),
        hash_field(payload_hash),
        hash_field(execution_spec_hash),
        hash_field(result_artifact_id),
        hash_field(result_artifact_hash),
    )


def attempt_result_artifact_digest(
    attempt_output_digest_value: str,
    values: AttemptResultValues,
) -> str:
    (
        attempt_id,
        logical_job_id_value,
        job_epoch_id,
        job_state_at_receipt,
        job_state_after,
        disposition,
        activity_snapshot_epoch_id,
        activity_snapshot_revision,
        epoch_active,
        chunk_active,
        requirement_active,
        group_active,
        archive_reason,
        cancelled_by_event_id,
        cancelled_by_epoch_id,
        cancellation_reason,
    ) = values
    return stable_m5_digest(
        "m5-attempt-result-artifact-v2",
        hash_field(attempt_output_digest_value),
        text_field(attempt_id),
        text_field(logical_job_id_value),
        int_field(job_epoch_id),
        enum_field(job_state_at_receipt),
        enum_field(job_state_after),
        enum_field(disposition),
        int_field(activity_snapshot_epoch_id),
        int_field(activity_snapshot_revision),
        bool_field(epoch_active),
        option_field(bool_field(chunk_active) if chunk_active is not None else None),
        option_field(
            bool_field(requirement_active) if requirement_active is not None else None
        ),
        option_field(bool_field(group_active) if group_active is not None else None),
        option_field(
            enum_field(archive_reason) if archive_reason is not None else None
        ),
        option_field(
            text_field(cancelled_by_event_id)
            if cancelled_by_event_id is not None
            else None
        ),
        option_field(
            int_field(cancelled_by_epoch_id)
            if cancelled_by_epoch_id is not None
            else None
        ),
        option_field(
            enum_field(cancellation_reason) if cancellation_reason is not None else None
        ),
    )


def attempt_result_artifact_id(
    attempt_id: str, attempt_result_artifact_hash: str
) -> str:
    return stable_m5_digest(
        "m5-attempt-result-id-v2",
        text_field(attempt_id),
        hash_field(attempt_result_artifact_hash),
    )


def activation_request_digest(
    *,
    activation_id: str,
    expected_mode_revision: int,
    expected_base_m4_epoch_id: int,
    expected_m4_publication_id: str,
    core_schema_bundle_sha256: str,
    bootstrap_state_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-activation-request-v2",
        text_field(activation_id),
        int_field(expected_mode_revision),
        int_field(expected_base_m4_epoch_id),
        text_field(expected_m4_publication_id),
        hash_field(core_schema_bundle_sha256),
        hash_field(bootstrap_state_hash),
    )


def activation_receipt_digest(
    *,
    activation_id: str,
    payload_hash: str,
    base_m4_epoch_id: int,
    m4_publication_id: str,
    m5_publication_epoch_id: int,
    mode_revision: int,
    bootstrap_state_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-activation-receipt-v2",
        text_field(activation_id),
        hash_field(payload_hash),
        int_field(base_m4_epoch_id),
        text_field(m4_publication_id),
        int_field(m5_publication_epoch_id),
        int_field(mode_revision),
        hash_field(bootstrap_state_hash),
    )


def changed_state_reference_digest(
    values: ChangedStateReferenceValues,
) -> str:
    kind, object_id, epoch_id, revision, state_artifact_hash = values
    return stable_m5_digest(
        "m5-changed-state-reference-v2",
        enum_field(kind),
        text_field(object_id),
        int_field(epoch_id),
        int_field(revision),
        hash_field(state_artifact_hash),
    )


def requirement_state_artifact_digest(
    *,
    requirement_version_id: str,
    witness_hashes: Iterable[str],
    supporting_observation_ids: Iterable[str],
    witness_count: int,
    satisfied: bool,
    decision_policy_version: str,
) -> str:
    return stable_m5_digest(
        "m5-requirement-state-artifact-v2",
        text_field(requirement_version_id),
        sequence_field(hash_field(value) for value in witness_hashes),
        sequence_field(text_field(value) for value in supporting_observation_ids),
        int_field(witness_count),
        bool_field(satisfied),
        text_field(decision_policy_version),
    )


def group_state_artifact_digest(
    *,
    group_version_id: str,
    requirement_count: int,
    satisfied_count: int,
    matching_size: int,
    complete: bool,
    decision_policy_version: str,
    certificate_digest: str | None,
) -> str:
    return stable_m5_digest(
        "m5-group-state-artifact-v2",
        text_field(group_version_id),
        int_field(requirement_count),
        int_field(satisfied_count),
        int_field(matching_size),
        bool_field(complete),
        text_field(decision_policy_version),
        option_field(
            None if certificate_digest is None else hash_field(certificate_digest)
        ),
    )


def claim_state_artifact_digest(
    *,
    claim_id: str,
    support_count: int,
    refute_count: int,
    best_support_score: float | None,
    best_refute_score: float | None,
    supporting_observation_ids: Iterable[str],
    refuting_observation_ids: Iterable[str],
    complete_group_count: int,
    complete_group_ids: Iterable[str],
    status: str | Enum,
    decision_policy_version: str,
    certificate_digest: str,
) -> str:
    return stable_m5_digest(
        "m5-claim-state-artifact-v2",
        text_field(claim_id),
        int_field(support_count),
        int_field(refute_count),
        option_field(
            None if best_support_score is None else f64_field(best_support_score)
        ),
        option_field(
            None if best_refute_score is None else f64_field(best_refute_score)
        ),
        sequence_field(text_field(value) for value in supporting_observation_ids),
        sequence_field(text_field(value) for value in refuting_observation_ids),
        int_field(complete_group_count),
        sequence_field(text_field(value) for value in complete_group_ids),
        enum_field(status),
        text_field(decision_policy_version),
        hash_field(certificate_digest),
    )


def answer_state_artifact_digest(
    *,
    answer_version_id: str,
    required_claim_count: int,
    supported_count: int,
    unsupported_count: int,
    refuted_count: int,
    conflicted_count: int,
    status: str | Enum,
) -> str:
    return stable_m5_digest(
        "m5-answer-state-artifact-v2",
        text_field(answer_version_id),
        int_field(required_claim_count),
        int_field(supported_count),
        int_field(unsupported_count),
        int_field(refuted_count),
        int_field(conflicted_count),
        enum_field(status),
    )


def changed_state_set_digest(reference_digests: Iterable[str]) -> str:
    return stable_m5_digest(
        "m5-changed-state-set-v2",
        sequence_field(hash_field(value) for value in reference_digests),
    )


def runtime_work_digest(counters: Sequence[int]) -> str:
    return stable_m5_digest(
        "m5-runtime-work-v2", *(int_field(value) for value in counters)
    )


def combined_status_delta_set_digest(deltas: Iterable[StatusDelta]) -> str:
    return stable_m5_digest(
        "m5-combined-status-delta-set-v2",
        sequence_field(
            sequence_field(
                (
                    text_field(delta.event_id),
                    text_field(delta.object_type),
                    text_field(delta.object_id),
                    text_field(delta.old_status),
                    text_field(delta.new_status),
                    text_field(delta.reason),
                )
            )
            for delta in deltas
        ),
    )


def open_event_receipt_binding_digest(
    *,
    epoch_id: int,
    replayed: bool,
    already_sealed: bool,
    publication_id: str | None,
    already_failed: bool,
    failure_reason: str | None,
) -> str:
    return stable_m5_digest(
        "m5-open-event-receipt-binding-v2",
        int_field(epoch_id),
        bool_field(replayed),
        bool_field(already_sealed),
        option_field(
            text_field(publication_id) if publication_id is not None else None
        ),
        bool_field(already_failed),
        option_field(
            text_field(failure_reason) if failure_reason is not None else None
        ),
    )


def publication_receipt_binding_digest(
    *, epoch_id: int, publication_id: str, replayed: bool
) -> str:
    return stable_m5_digest(
        "m5-publication-receipt-binding-v2",
        int_field(epoch_id),
        text_field(publication_id),
        bool_field(replayed),
    )


def event_run_logical_result_digest(
    *,
    event_id: str,
    payload_hash: str,
    epoch_id: int,
    sealed_or_failed_outcome: str | Enum,
    original_open_receipt_binding_hash: str,
    original_publication_receipt_binding_hash: str | None,
    event_work_digest: str,
    combined_status_delta_set_hash: str,
    changed_state_set_hash: str,
    failure_reason: str | Enum | None,
) -> str:
    return stable_m5_digest(
        "m5-event-run-logical-result-v2",
        text_field(event_id),
        hash_field(payload_hash),
        int_field(epoch_id),
        enum_field(sealed_or_failed_outcome),
        hash_field(original_open_receipt_binding_hash),
        option_field(
            hash_field(original_publication_receipt_binding_hash)
            if original_publication_receipt_binding_hash is not None
            else None
        ),
        hash_field(event_work_digest),
        hash_field(combined_status_delta_set_hash),
        hash_field(changed_state_set_hash),
        option_field(
            enum_field(failure_reason) if failure_reason is not None else None
        ),
    )


def requirement_withdrawal_plan_digest(
    *,
    event_id: str,
    deactivated_chunk_version_ids: Iterable[str],
    withdrawn_candidate_pair_digests: Iterable[str],
    withdrawn_observation_ids: Iterable[str],
    cancelled_job_ids: Iterable[str],
    fallback_keys: Iterable[tuple[str, str]],
) -> str:
    return stable_m5_digest(
        "m5-requirement-withdrawal-plan-v2",
        text_field(event_id),
        sequence_field(text_field(value) for value in deactivated_chunk_version_ids),
        sequence_field(hash_field(value) for value in withdrawn_candidate_pair_digests),
        sequence_field(text_field(value) for value in withdrawn_observation_ids),
        sequence_field(text_field(value) for value in cancelled_job_ids),
        sequence_field(
            sequence_field(
                (text_field(requirement_version_id), text_field(candidate_policy_id))
            )
            for requirement_version_id, candidate_policy_id in fallback_keys
        ),
    )


def cancellation_plan_digest(
    *,
    structural_event_id: str,
    epoch_id: int,
    cancelled_job_ids: Iterable[str],
    reason: str | Enum,
) -> str:
    return stable_m5_digest(
        "m5-cancellation-plan-v2",
        text_field(structural_event_id),
        int_field(epoch_id),
        sequence_field(hash_field(value) for value in cancelled_job_ids),
        enum_field(reason),
    )


def requirement_root_set_digest(root_job_ids: Iterable[str]) -> str:
    canonical = tuple(sorted(set(root_job_ids)))
    return stable_m5_digest(
        "m5-requirement-root-set-v2",
        sequence_field(text_field(value) for value in canonical),
    )


def requirement_root_barrier_completion_digest(
    *,
    structural_event_id: str,
    requirement_root_set_hash: str,
    root_result_hashes: Iterable[tuple[str, str]],
    admitted_pair_digests: Iterable[tuple[str, str]],
) -> str:
    canonical_roots = tuple(sorted(root_result_hashes, key=lambda item: item[0]))
    canonical_pairs = tuple(sorted(admitted_pair_digests, key=lambda item: item[0]))
    return stable_m5_digest(
        "m5-requirement-root-barrier-completion-v2",
        text_field(structural_event_id),
        hash_field(requirement_root_set_hash),
        sequence_field(
            sequence_field((text_field(root_id), hash_field(result_hash)))
            for root_id, result_hash in canonical_roots
        ),
        sequence_field(hash_field(digest) for _, digest in canonical_pairs),
    )


def runtime_schema_bundle_digest(
    *,
    migration_sha256: str,
    core_schema_bundle_sha256: str,
) -> str:
    return stable_m5_digest(
        "m5-runtime-schema-bundle-v2",
        text_field("migrations/015_m5_runtime.sql"),
        hash_field(migration_sha256),
        hash_field(core_schema_bundle_sha256),
    )


__all__ = [
    "active_chunk_snapshot_digest",
    "activation_receipt_digest",
    "activation_request_digest",
    "answer_state_artifact_digest",
    "attempt_output_digest",
    "attempt_result_artifact_digest",
    "attempt_result_artifact_id",
    "cancellation_plan_digest",
    "candidate_policy_manifest_digest",
    "changed_state_reference_digest",
    "changed_state_set_digest",
    "child_set_digest",
    "combined_status_delta_set_digest",
    "discovery_scope_closure_digest",
    "discovery_scope_contract_digest",
    "event_run_logical_result_digest",
    "forward_retrieval_execution_spec_digest",
    "group_state_artifact_digest",
    "job_attempt_id",
    "job_completion_digest",
    "job_payload_digest",
    "logical_job_id",
    "open_event_receipt_binding_digest",
    "publication_receipt_binding_digest",
    "claim_state_artifact_digest",
    "requirement_admitted_pair_digest",
    "requirement_state_artifact_digest",
    "requirement_channel_hit_digest",
    "requirement_discovery_artifact_id",
    "requirement_discovery_result_digest",
    "requirement_pair_input_digest",
    "requirement_registry_snapshot_digest",
    "requirement_root_barrier_completion_digest",
    "requirement_root_set_digest",
    "requirement_scope_selection_digest",
    "requirement_semantic_observation_id",
    "requirement_verifier_artifact_id",
    "requirement_verifier_result_digest",
    "requirement_verifier_role_binding_digest",
    "requirement_withdrawal_plan_digest",
    "reverse_retrieval_execution_spec_digest",
    "runtime_schema_bundle_digest",
    "runtime_work_digest",
    "semantic_pair_digest",
    "text_normalizer_provenance_digest",
]
