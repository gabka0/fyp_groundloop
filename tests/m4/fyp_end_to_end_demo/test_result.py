from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from groundloop.ai.contracts import AtomicClaim, stable_digest
from groundloop.ai.pipeline import claim_version_id
from groundloop.errors import ValidationError
from groundloop.fyp_end_to_end_demo import (
    LIMITATIONS,
    SCHEMA_VERSION,
    EventEvidence,
    FypEndToEndDemoConfig,
    FypEndToEndResult,
    M3ClaimEvidence,
    M3Evidence,
    ObjectStateEvidence,
    ReplayEvidence,
    format_fyp_end_to_end_summary,
    validate_fyp_end_to_end_result,
)
from groundloop.m4.contracts import stable_m4_digest
from groundloop.m4.m3_activation import M3M4ActivationReceipt
from groundloop.postgres import ServerMetadata

ANSWER_ID = "answer-derived"
RUN_ID = "run-derived"
QUESTION_TEXT = "What does GroundLoop maintain?"
QUESTION_ID = "question-" + stable_digest("m3-question-v1", QUESTION_TEXT)
LOCAL_CLAIM = AtomicClaim(
    "claim-1",
    "GroundLoop maintains versioned grounding state.",
    True,
    ("chunk-a",),
)
GLOBAL_CLAIM_ID = claim_version_id(ANSWER_ID, LOCAL_CLAIM)


def _state(claim: str, answer: str) -> ObjectStateEvidence:
    return ObjectStateEvidence(((GLOBAL_CLAIM_ID, claim),), ANSWER_ID, answer)


def _replay(epoch_id: int, publication_id: str | None) -> ReplayEvidence:
    digest = str(epoch_id) * 64
    return ReplayEvidence(
        state="replayed",
        epoch_id=epoch_id,
        publication_id=publication_id,
        discovery_call_count=0,
        embedding_request_count=0,
        verifier_request_count=0,
        verifier_backend_pair_calls=0,
        projection_before_sha256=digest[:64],
        projection_after_sha256=digest[:64],
        projection_equal=True,
    )


def _event(
    *,
    kind: str,
    previous_epoch: int,
    epoch: int,
    claim: str,
    answer: str,
    embedding_requests: int,
    verifier_calls: int,
) -> EventEvidence:
    publication_id = stable_m4_digest("m4-publication-v1", str(epoch))
    return EventEvidence(
        event_id=(
            f"fyp-e2e-{kind}-"
            + stable_m4_digest("fyp-end-to-end-demo-event-id-v1", RUN_ID, kind)
        ),
        update_kind=kind,
        payload_hash="a" * 64,
        previous_published_epoch_id=previous_epoch,
        epoch_id=epoch,
        publication_id=publication_id,
        state="sealed",
        discovery_call_count=1,
        embedding_request_count=embedding_requests,
        verifier_call_count=verifier_calls,
        verifier_request_count=verifier_calls,
        verifier_backend_pair_calls=verifier_calls,
        observation_artifact_count=verifier_calls,
        effective_observation_count=verifier_calls,
        inactive_completion_count=0,
        python_full_recomputation_equal=True,
        sql_full_recomputation_equal=True,
        persisted_state_equal=True,
        sql_claim_mismatch_count=0,
        sql_answer_mismatch_count=0,
        open_job_count=0,
        open_scope_count=0,
        states=_state(claim, answer),
        replay=_replay(epoch, publication_id),
    )


def _result() -> FypEndToEndResult:
    activation = M3M4ActivationReceipt(
        schema_version="groundloop-m3-m4-activation-v1",
        run_id=RUN_ID,
        base_epoch_id=3,
        answer_version_id=ANSWER_ID,
        claim_registry_snapshot_id="registry-derived",
        candidate_policy_id="candidate-policy",
        candidate_policy_hash="1" * 64,
        claim_count=1,
        chunk_count=2,
        observation_count=2,
        role_artifact_count=3,
        claim_index_count=1,
        claim_ids=(GLOBAL_CLAIM_ID,),
        chunk_ids=("chunk-a", "chunk-b"),
        embedding_model_artifact_id="embedding-model",
        verifier_model_artifact_id="verifier-model",
        verifier_prompt_artifact_id="verifier-prompt",
        calibration_version="calibration-v1",
        calibration_temperature=1.0,
        decision_policy_version="m3-policy-v1",
        verifier_execution_spec_hash="2" * 64,
        vector_method_version="vector-v1",
        vector_index_kind="exact",
        vector_index_build_config_hash="3" * 64,
        vector_search_config_hash="4" * 64,
        lexical_method_version="lexical-v1",
        lexical_config_hash="5" * 64,
        created_role_artifact_count=3,
        reused_role_artifact_count=0,
        activation_embedding_request_count=1,
        baseline_python_mismatch_count=0,
        baseline_sql_claim_mismatch_count=0,
        baseline_sql_answer_mismatch_count=0,
        global_closure_valid=True,
        replayed=False,
    )
    activation_replay_receipt = replace(
        activation,
        created_role_artifact_count=0,
        reused_role_artifact_count=activation.role_artifact_count,
        activation_embedding_request_count=0,
        replayed=True,
    )
    provenance = "f" * 64
    return FypEndToEndResult(
        schema_version=SCHEMA_VERSION,
        backend="deterministic",
        model_downloads_allowed=False,
        backend_status="executed",
        schema_name="groundloop_fyp_e2e_test",
        keep_schema_requested=False,
        schema_removed=True,
        server=ServerMetadata("16.1", 160001, "0.7.0", "0.7.0"),
        m3=M3Evidence(
            manifest_sha256="0" * 64,
            run_id=RUN_ID,
            semantic_epoch_id=3,
            question_id=QUESTION_ID,
            question_text=QUESTION_TEXT,
            answer_version_id=ANSWER_ID,
            answer_text=LOCAL_CLAIM.text,
            answer_cited_chunk_version_ids=("chunk-a",),
            local_claim_ids=("claim-1",),
            global_claim_ids=(GLOBAL_CLAIM_ID,),
            claims=(
                M3ClaimEvidence(
                    local_claim_id=LOCAL_CLAIM.local_claim_id,
                    global_claim_id=GLOBAL_CLAIM_ID,
                    text=LOCAL_CLAIM.text,
                    required=LOCAL_CLAIM.required,
                    cited_chunk_version_ids=LOCAL_CLAIM.cited_chunk_version_ids,
                ),
            ),
            chunk_version_ids=("chunk-a", "chunk-b"),
            model_artifact_ids=("embedding-model", "verifier-model"),
            prompt_artifact_ids=("generation-prompt", "verifier-prompt"),
            decision_policy_version="m3-policy-v1",
            generation_execution_ids=(ANSWER_ID,),
            extraction_execution_ids=(GLOBAL_CLAIM_ID,),
            retrieval_candidate_ids=("candidate-1",),
            verification_observation_ids=("observation-1", "observation-2"),
        ),
        activation=activation,
        baseline=_state("unsupported", "unsupported"),
        activation_replay_receipt=activation_replay_receipt,
        activation_replay=_replay(3, None),
        events=(
            _event(
                kind="insert",
                previous_epoch=3,
                epoch=4,
                claim="supported",
                answer="valid",
                embedding_requests=1,
                verifier_calls=1,
            ),
            _event(
                kind="delete",
                previous_epoch=4,
                epoch=5,
                claim="supported",
                answer="valid",
                embedding_requests=0,
                verifier_calls=0,
            ),
            _event(
                kind="replace",
                previous_epoch=5,
                epoch=6,
                claim="refuted",
                answer="contradicted",
                embedding_requests=1,
                verifier_calls=1,
            ),
        ),
        m3_provenance_before_sha256=provenance,
        m3_provenance_after_sha256=provenance,
        m3_provenance_equal=True,
        limitations=LIMITATIONS,
    )


def _replace_activation_identity(**changes: object) -> FypEndToEndResult:
    result = _result()
    return replace(
        result,
        activation=replace(result.activation, **changes),
        activation_replay_receipt=replace(result.activation_replay_receipt, **changes),
    )


def test_result_validates_serializes_and_formats(tmp_path: Path) -> None:
    result = _result()

    validate_fyp_end_to_end_result(result)
    payload = json.loads(result.to_json())
    rendered = format_fyp_end_to_end_summary(result, output=tmp_path / "result.json")

    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["events"][1]["embedding_request_count"] == 0
    expected_canonical_sha256 = hashlib.sha256(
        json.dumps(result.to_dict(), sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    assert result.canonical_sha256 == expected_canonical_sha256
    assert rendered.startswith("GroundLoop FYP end-to-end demo: PASS\n")
    assert "AI quality is not claimed" in rendered


@pytest.mark.parametrize(
    "mutated",
    (
        replace(_result(), model_downloads_allowed=True),
        replace(_result(), limitations=()),
        replace(
            _result(),
            server=replace(_result().server, postgres_version=""),
        ),
        replace(
            _result(),
            server=replace(_result().server, postgres_version_num=0),
        ),
        replace(
            _result(),
            server=replace(_result().server, pgvector_installed_version=""),
        ),
        replace(_result(), schema_removed=False),
        replace(
            _result(),
            m3=replace(_result().m3, semantic_epoch_id=2),
        ),
        replace(
            _result(),
            m3=replace(_result().m3, question_id="question-not-derived"),
        ),
        replace(
            _result(),
            m3=replace(_result().m3, generation_execution_ids=()),
        ),
        replace(
            _result(),
            m3=replace(_result().m3, extraction_execution_ids=()),
        ),
        replace(
            _result(),
            m3=replace(_result().m3, retrieval_candidate_ids=()),
        ),
        replace(
            _result(),
            m3=replace(_result().m3, verification_observation_ids=()),
        ),
        replace(
            _result(),
            m3=replace(_result().m3, decision_policy_version="other-policy"),
        ),
        replace(_result(), m3_provenance_equal=False),
        replace(
            _result(),
            activation_replay_receipt=replace(
                _result().activation_replay_receipt,
                run_id="other-run",
            ),
        ),
        replace(
            _result(),
            activation_replay_receipt=replace(
                _result().activation_replay_receipt,
                base_epoch_id=4,
            ),
        ),
        _replace_activation_identity(calibration_version=""),
        _replace_activation_identity(calibration_temperature=0.0),
        _replace_activation_identity(vector_method_version=""),
        _replace_activation_identity(vector_index_kind=""),
        _replace_activation_identity(lexical_method_version=""),
        replace(
            _result(),
            baseline=replace(_result().baseline, answer_status="garbage"),
        ),
        replace(
            _result(),
            baseline=replace(
                _result().baseline,
                claim_states=((GLOBAL_CLAIM_ID, "garbage"),),
            ),
        ),
        replace(
            _result(),
            activation_replay=replace(
                _result().activation_replay, embedding_request_count=1
            ),
        ),
        replace(
            _result(),
            events=(
                replace(_result().events[0], event_id="not-run-derived"),
                _result().events[1],
                _result().events[2],
            ),
        ),
        replace(
            _result(),
            events=(
                replace(
                    _result().events[0],
                    publication_id="not-production-derived",
                    replay=replace(
                        _result().events[0].replay,
                        publication_id="not-production-derived",
                    ),
                ),
                _result().events[1],
                _result().events[2],
            ),
        ),
        replace(
            _result(),
            events=(
                replace(_result().events[0], effective_observation_count=-1),
                _result().events[1],
                _result().events[2],
            ),
        ),
        replace(
            _result(),
            events=(
                _result().events[0],
                replace(_result().events[1], verifier_call_count=1),
                _result().events[2],
            ),
        ),
    ),
)
def test_result_validation_fails_closed(mutated: FypEndToEndResult) -> None:
    with pytest.raises(ValidationError):
        validate_fyp_end_to_end_result(mutated)


def test_config_rejects_backend_and_schema_prefix(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="backend"):
        FypEndToEndDemoConfig("postgresql://db", tmp_path, tmp_path, backend="api")
    with pytest.raises(ValidationError, match="schema prefix"):
        FypEndToEndDemoConfig(
            "postgresql://db", tmp_path, tmp_path, schema_prefix="not-safe!"
        )
