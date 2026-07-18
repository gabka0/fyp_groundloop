"""Coordinator-owned tests for the frozen M3 AI boundary."""

import hashlib

import pytest

from groundloop.ai.contracts import (
    AtomicClaim,
    CitedAnswer,
    ClaimExtractionResult,
    ModelArtifact,
    ModelTask,
    PipelineRunManifest,
    PipelineRunStatus,
    PromptArtifact,
    ScoreTriple,
    stable_digest,
)
from groundloop.ai.registry import InMemoryModelRegistry, InMemoryPromptRegistry
from groundloop.domain import AnswerState, AnswerStatus, ClaimState, ClaimStatus
from groundloop.errors import ArtifactConflictError, ValidationError

HASH = "a" * 64


def _model(*, license_id: str = "apache-2.0") -> ModelArtifact:
    return ModelArtifact(
        artifact_id="model-1",
        task=ModelTask.VERIFICATION,
        provider="huggingface",
        model_id="cross-encoder/example",
        immutable_revision="b" * 40,
        tokenizer_revision="b" * 40,
        license_id=license_id,
        config_hash=HASH,
    )


def _prompt(*, version: str = "v1") -> PromptArtifact:
    template = "Judge {claim} against {evidence}."
    return PromptArtifact(
        artifact_id="prompt-1",
        task=ModelTask.VERIFICATION,
        version=version,
        template=template,
        template_hash=hashlib.sha256(template.encode()).hexdigest(),
        decoding_config_hash=HASH,
    )


def test_stable_digest_is_ordered_and_unambiguous() -> None:
    assert stable_digest("ab", "c") != stable_digest("a", "bc")
    assert stable_digest("ab", "c") == stable_digest("ab", "c")


def test_immutable_registries_reuse_identical_payloads_and_reject_conflicts() -> None:
    models = InMemoryModelRegistry()
    prompts = InMemoryPromptRegistry()
    assert models.register(_model())
    assert not models.register(_model())
    assert prompts.register(_prompt())
    assert not prompts.register(_prompt())
    with pytest.raises(ArtifactConflictError):
        models.register(_model(license_id="mit"))
    with pytest.raises(ArtifactConflictError):
        prompts.register(_prompt(version="v2"))


def test_score_triple_requires_normalized_probabilities() -> None:
    assert ScoreTriple(0.7, 0.2, 0.1).support == 0.7
    with pytest.raises(ValidationError):
        ScoreTriple(0.7, 0.2, 0.2)


def test_published_manifest_requires_answer_and_claim() -> None:
    answer = CitedAnswer("Answer [chunk-1].", ("chunk-1",), HASH, HASH)
    claim = AtomicClaim("local-1", "A factual claim.", True, ("chunk-1",))
    extraction = ClaimExtractionResult((claim,), HASH, HASH)
    claim_state = ClaimState(
        claim_id="claim-1",
        support_count=1,
        refute_count=0,
        best_support_score=0.9,
        best_refute_score=None,
        supporting_observation_ids=("observation-1",),
        refuting_observation_ids=(),
        status=ClaimStatus.SUPPORTED,
    )
    answer_state = AnswerState(
        answer_version_id="answer-1",
        required_claim_count=1,
        supported_count=1,
        unsupported_count=0,
        refuted_count=0,
        conflicted_count=0,
        status=AnswerStatus.VALID,
    )
    manifest = PipelineRunManifest(
        schema_version="m3-v1",
        run_id="run-1",
        status=PipelineRunStatus.PUBLISHED,
        config_hash=HASH,
        input_hash=HASH,
        corpus_hash=HASH,
        question_id="question-1",
        decision_policy_version="policy-1",
        answer_version_id="answer-1",
        semantic_epoch_id=1,
        confirmed_as_of_epoch=1,
        model_artifact_ids=("model-1",),
        prompt_artifact_ids=("prompt-1",),
        chunk_version_ids=("chunk-1",),
        chunk_text_hashes=(("chunk-1", HASH),),
        retrieval_candidates=(),
        answer=answer,
        extraction=extraction,
        claims=(claim,),
        verifications=(),
        claim_states=(claim_state,),
        answer_states=(answer_state,),
        timings=(),
        reused_artifact_ids=(),
        new_artifact_ids=("answer-1",),
    )
    assert manifest.answer == answer

    with pytest.raises(ValidationError):
        PipelineRunManifest(
            schema_version="m3-v1",
            run_id="run-bad",
            status=PipelineRunStatus.PUBLISHED,
            config_hash=HASH,
            input_hash=HASH,
            corpus_hash=HASH,
            question_id="question-1",
            decision_policy_version="policy-1",
            answer_version_id=None,
            semantic_epoch_id=1,
            confirmed_as_of_epoch=1,
            model_artifact_ids=(),
            prompt_artifact_ids=(),
            chunk_version_ids=(),
            chunk_text_hashes=(),
            retrieval_candidates=(),
            answer=None,
            extraction=None,
            claims=(),
            verifications=(),
            claim_states=(),
            answer_states=(),
            timings=(),
            reused_artifact_ids=(),
            new_artifact_ids=(),
        )
