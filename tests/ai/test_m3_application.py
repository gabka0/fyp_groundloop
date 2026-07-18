"""Offline end-to-end M3 orchestration and replay tests."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import pytest

from groundloop.ai.application import (
    InMemoryPublicationStore,
    M3Application,
    M3ApplicationConfig,
)
from groundloop.ai.chunking import FixedCharChunker
from groundloop.ai.claim_extraction import DeterministicClaimExtractor
from groundloop.ai.contracts import (
    AtomicClaim,
    ChunkDraft,
    ModelArtifact,
    ModelTask,
    PromptArtifact,
    ScoreTriple,
    VerificationResult,
    stable_digest,
)
from groundloop.ai.embeddings import DeterministicFakeEmbedder
from groundloop.ai.generation import DeterministicAnswerGenerator

HASH = "a" * 64


@dataclass(slots=True)
class FixedVerifier:
    model_artifact: ModelArtifact
    prompt_artifact: PromptArtifact
    calibration_version: str = "test-temperature-v1"
    temperature: float = 1.0
    calls: int = 0

    @classmethod
    def create(cls) -> FixedVerifier:
        template = "premise={evidence}\nhypothesis={claim}"
        return cls(
            model_artifact=ModelArtifact(
                artifact_id="fixed-verifier",
                task=ModelTask.VERIFICATION,
                provider="test",
                model_id="fixed-verifier",
                immutable_revision="v1",
                tokenizer_revision="v1",
                license_id="test",
                config_hash=HASH,
            ),
            prompt_artifact=PromptArtifact(
                artifact_id="fixed-verifier-prompt",
                task=ModelTask.VERIFICATION,
                version="v1",
                template=template,
                template_hash=hashlib.sha256(template.encode()).hexdigest(),
                decoding_config_hash=HASH,
            ),
        )

    def verify(self, claim: AtomicClaim, chunk: ChunkDraft) -> VerificationResult:
        self.calls += 1
        input_hash = stable_digest(claim.text, chunk.text)
        return VerificationResult(
            claim_id=claim.local_claim_id,
            chunk_version_id=chunk.chunk_version_id,
            candidate_id="assigned-by-coordinator",
            model_artifact_id=self.model_artifact.artifact_id,
            prompt_artifact_id=self.prompt_artifact.artifact_id,
            calibration_version=self.calibration_version,
            temperature=self.temperature,
            scores=ScoreTriple(0.9, 0.05, 0.05),
            input_hash=input_hash,
            raw_output_hash=stable_digest("raw", input_hash),
            raw_logits=(0.0, 3.0, 0.0),
        )


def test_static_application_publishes_complete_state_and_reuses_replay(
    tmp_path: object,
) -> None:
    from pathlib import Path

    root = Path(str(tmp_path)) / "corpus"
    root.mkdir()
    (root / "guide.txt").write_text(
        "GroundLoop maintains claim grounding incrementally.",
        encoding="utf-8",
    )
    store = InMemoryPublicationStore()
    verifier = FixedVerifier.create()
    application = M3Application(
        store=store,
        chunker=FixedCharChunker(),
        embedder=DeterministicFakeEmbedder(),
        generator=DeterministicAnswerGenerator(),
        extractor=DeterministicClaimExtractor(),
        verifier=verifier,
        config=M3ApplicationConfig(config_hash=HASH),
    )
    first = application.register(root, "What does GroundLoop maintain?")
    calls_after_first = verifier.calls
    second = application.register(root, "What does GroundLoop maintain?")

    assert first.answer is not None
    assert first.claims and first.verifications
    assert first.claim_states and first.answer_states
    assert first.semantic_epoch_id == first.confirmed_as_of_epoch == 1
    assert first.retrieval_candidates
    assert first.new_artifact_ids
    assert not second.new_artifact_ids
    assert second.reused_artifact_ids
    assert verifier.calls == calls_after_first


def test_model_failure_marks_run_failed_without_publishing_bundle(
    tmp_path: object,
) -> None:
    from pathlib import Path

    root = Path(str(tmp_path)) / "corpus"
    root.mkdir()
    (root / "guide.txt").write_text("Evidence.", encoding="utf-8")
    store = InMemoryPublicationStore()
    generator = DeterministicAnswerGenerator()

    def fail_generate(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RuntimeError("injected generation failure")

    generator.generate = fail_generate  # type: ignore[method-assign,assignment]
    application = M3Application(
        store=store,
        chunker=FixedCharChunker(),
        embedder=DeterministicFakeEmbedder(),
        generator=generator,
        extractor=DeterministicClaimExtractor(),
        verifier=FixedVerifier.create(),
        config=M3ApplicationConfig(config_hash=HASH),
    )
    with pytest.raises(RuntimeError, match="injected generation"):
        application.register(root, "What is present?")
    assert not store.bundles
    assert tuple(store.manifests.values())[0].failure_code == "RuntimeError"


def test_verifier_calibration_changes_pipeline_identity(tmp_path: object) -> None:
    from pathlib import Path

    root = Path(str(tmp_path)) / "corpus"
    root.mkdir()
    (root / "guide.txt").write_text("Evidence.", encoding="utf-8")
    store = InMemoryPublicationStore()

    def register(calibration_version: str, temperature: float) -> str:
        verifier = FixedVerifier.create()
        verifier.calibration_version = calibration_version
        verifier.temperature = temperature
        application = M3Application(
            store=store,
            chunker=FixedCharChunker(),
            embedder=DeterministicFakeEmbedder(),
            generator=DeterministicAnswerGenerator(),
            extractor=DeterministicClaimExtractor(),
            verifier=verifier,
            config=M3ApplicationConfig(config_hash=HASH),
        )
        return application.register(root, "What is present?").run_id

    first = register("temperature-v1:first", 1.0)
    second = register("temperature-v1:second", 1.1)

    assert first != second
