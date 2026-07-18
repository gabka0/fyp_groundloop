"""Manifest JSON round-trip tests."""

from groundloop.ai.contracts import (
    AtomicClaim,
    CitedAnswer,
    ClaimExtractionResult,
    PipelineRunManifest,
    PipelineRunStatus,
)
from groundloop.ai.manifest import (
    canonical_manifest_json,
    manifest_from_dict,
    manifest_to_dict,
)
from groundloop.domain import AnswerState, AnswerStatus, ClaimState, ClaimStatus

HASH = "a" * 64


def test_manifest_round_trip_is_canonical() -> None:
    claim = AtomicClaim("local", "claim", True, ("chunk",))
    manifest = PipelineRunManifest(
        schema_version="m3-v1",
        run_id="run",
        status=PipelineRunStatus.PUBLISHED,
        config_hash=HASH,
        input_hash=HASH,
        corpus_hash=HASH,
        question_id="question",
        decision_policy_version="policy",
        answer_version_id="answer",
        semantic_epoch_id=1,
        confirmed_as_of_epoch=1,
        model_artifact_ids=("model",),
        prompt_artifact_ids=("prompt",),
        chunk_version_ids=("chunk",),
        chunk_text_hashes=(("chunk", HASH),),
        retrieval_candidates=(),
        answer=CitedAnswer("answer", ("chunk",), HASH, HASH),
        extraction=ClaimExtractionResult((claim,), HASH, HASH),
        claims=(claim,),
        verifications=(),
        claim_states=(
            ClaimState(
                "claim",
                0,
                0,
                None,
                None,
                (),
                (),
                ClaimStatus.UNSUPPORTED,
            ),
        ),
        answer_states=(
            AnswerState(
                "answer",
                1,
                0,
                1,
                0,
                0,
                AnswerStatus.UNSUPPORTED,
            ),
        ),
        timings=(),
        reused_artifact_ids=(),
        new_artifact_ids=("answer",),
    )
    restored = manifest_from_dict(manifest_to_dict(manifest))
    assert restored == manifest
    assert canonical_manifest_json(restored) == canonical_manifest_json(manifest)
