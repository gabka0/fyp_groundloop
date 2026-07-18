"""Coordinator-owned tests for the frozen M3 AI boundary."""

import hashlib

import pytest

from groundloop.ai.contracts import (
    AtomicClaim,
    CitedAnswer,
    ModelArtifact,
    ModelTask,
    PipelineRunManifest,
    PipelineRunStatus,
    PromptArtifact,
    ScoreTriple,
    stable_digest,
)
from groundloop.ai.registry import InMemoryModelRegistry, InMemoryPromptRegistry
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
    manifest = PipelineRunManifest(
        schema_version="m3-v1",
        run_id="run-1",
        status=PipelineRunStatus.PUBLISHED,
        config_hash=HASH,
        input_hash=HASH,
        corpus_hash=HASH,
        question_id="question-1",
        answer_version_id="answer-1",
        semantic_epoch_id=1,
        model_artifact_ids=("model-1",),
        prompt_artifact_ids=("prompt-1",),
        chunk_version_ids=("chunk-1",),
        retrieval_candidates=(),
        answer=answer,
        claims=(claim,),
        verifications=(),
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
            answer_version_id=None,
            semantic_epoch_id=1,
            model_artifact_ids=(),
            prompt_artifact_ids=(),
            chunk_version_ids=(),
            retrieval_candidates=(),
            answer=None,
            claims=(),
            verifications=(),
            timings=(),
            reused_artifact_ids=(),
            new_artifact_ids=(),
        )
