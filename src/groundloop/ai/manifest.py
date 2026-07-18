"""Canonical JSON serialization for the versioned M3 run manifest."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from groundloop.ai.contracts import (
    AtomicClaim,
    CitedAnswer,
    ClaimExtractionResult,
    ComponentTiming,
    PipelineRunManifest,
    PipelineRunStatus,
    QueryKind,
    RetrievalCandidate,
    ScoreTriple,
    VerificationResult,
)
from groundloop.domain import (
    AnswerState,
    AnswerStatus,
    ClaimState,
    ClaimStatus,
)


def manifest_to_dict(manifest: PipelineRunManifest) -> dict[str, Any]:
    """Return a JSON-compatible mapping without losing enum semantics."""
    value = asdict(manifest)
    value["status"] = manifest.status.value
    for candidate in value["retrieval_candidates"]:
        candidate["query_kind"] = candidate["query_kind"].value
    for state in value["claim_states"]:
        state["status"] = state["status"].value
    for state in value["answer_states"]:
        state["status"] = state["status"].value
    return value


def canonical_manifest_json(manifest: PipelineRunManifest) -> str:
    """Serialize deterministically for persistence, hashing, and CLI output."""
    return json.dumps(
        manifest_to_dict(manifest),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def manifest_from_dict(value: dict[str, Any]) -> PipelineRunManifest:
    """Reconstruct and revalidate a manifest read from PostgreSQL or JSON."""
    answer_value = value.get("answer")
    answer = (
        None
        if answer_value is None
        else CitedAnswer(
            text=answer_value["text"],
            cited_chunk_version_ids=tuple(
                answer_value["cited_chunk_version_ids"]
            ),
            input_hash=answer_value["input_hash"],
            raw_output_hash=answer_value["raw_output_hash"],
            repair_count=answer_value["repair_count"],
        )
    )
    extraction_value = value.get("extraction")
    extraction = (
        None
        if extraction_value is None
        else ClaimExtractionResult(
            claims=tuple(
                _atomic_claim(item) for item in extraction_value["claims"]
            ),
            input_hash=extraction_value["input_hash"],
            raw_output_hash=extraction_value["raw_output_hash"],
            repair_count=extraction_value["repair_count"],
        )
    )
    candidates = tuple(
        RetrievalCandidate(
            candidate_id=item["candidate_id"],
            query_kind=QueryKind(item["query_kind"]),
            query_id=item["query_id"],
            chunk_version_id=item["chunk_version_id"],
            score=item["score"],
            rank=item["rank"],
            embedding_model_artifact_id=item["embedding_model_artifact_id"],
            method_version=item["method_version"],
        )
        for item in value["retrieval_candidates"]
    )
    verifications = tuple(
        VerificationResult(
            claim_id=item["claim_id"],
            chunk_version_id=item["chunk_version_id"],
            candidate_id=item["candidate_id"],
            model_artifact_id=item["model_artifact_id"],
            prompt_artifact_id=item["prompt_artifact_id"],
            calibration_version=item["calibration_version"],
            temperature=item["temperature"],
            scores=ScoreTriple(**item["scores"]),
            input_hash=item["input_hash"],
            raw_output_hash=item["raw_output_hash"],
            raw_logits=(
                None
                if item["raw_logits"] is None
                else tuple(item["raw_logits"])
            ),
        )
        for item in value["verifications"]
    )
    claim_states = tuple(
        ClaimState(
            claim_id=item["claim_id"],
            support_count=item["support_count"],
            refute_count=item["refute_count"],
            best_support_score=item["best_support_score"],
            best_refute_score=item["best_refute_score"],
            supporting_observation_ids=tuple(item["supporting_observation_ids"]),
            refuting_observation_ids=tuple(item["refuting_observation_ids"]),
            status=ClaimStatus(item["status"]),
        )
        for item in value["claim_states"]
    )
    answer_states = tuple(
        AnswerState(
            answer_version_id=item["answer_version_id"],
            required_claim_count=item["required_claim_count"],
            supported_count=item["supported_count"],
            unsupported_count=item["unsupported_count"],
            refuted_count=item["refuted_count"],
            conflicted_count=item["conflicted_count"],
            status=AnswerStatus(item["status"]),
        )
        for item in value["answer_states"]
    )
    return PipelineRunManifest(
        schema_version=value["schema_version"],
        run_id=value["run_id"],
        status=PipelineRunStatus(value["status"]),
        config_hash=value["config_hash"],
        input_hash=value["input_hash"],
        corpus_hash=value["corpus_hash"],
        question_id=value["question_id"],
        decision_policy_version=value["decision_policy_version"],
        answer_version_id=value["answer_version_id"],
        semantic_epoch_id=value["semantic_epoch_id"],
        confirmed_as_of_epoch=value["confirmed_as_of_epoch"],
        model_artifact_ids=tuple(value["model_artifact_ids"]),
        prompt_artifact_ids=tuple(value["prompt_artifact_ids"]),
        chunk_version_ids=tuple(value["chunk_version_ids"]),
        chunk_text_hashes=tuple(
            (item[0], item[1]) for item in value["chunk_text_hashes"]
        ),
        retrieval_candidates=candidates,
        answer=answer,
        extraction=extraction,
        claims=tuple(_atomic_claim(item) for item in value["claims"]),
        verifications=verifications,
        claim_states=claim_states,
        answer_states=answer_states,
        timings=tuple(ComponentTiming(**item) for item in value["timings"]),
        reused_artifact_ids=tuple(value["reused_artifact_ids"]),
        new_artifact_ids=tuple(value["new_artifact_ids"]),
        failure_code=value.get("failure_code"),
    )


def _atomic_claim(value: dict[str, Any]) -> AtomicClaim:
    return AtomicClaim(
        local_claim_id=value["local_claim_id"],
        text=value["text"],
        required=value["required"],
        cited_chunk_version_ids=tuple(value["cited_chunk_version_ids"]),
    )
