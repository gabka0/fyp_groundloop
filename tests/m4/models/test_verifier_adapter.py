from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import replace

import pytest

from groundloop.ai.contracts import (
    AtomicClaim,
    ChunkDraft,
    PromptArtifact,
    VerificationResult,
)
from groundloop.ai.verification.adapter import (
    DeterministicFakeVerifier,
    logits_to_score_triple,
)
from groundloop.domain import DecisionPolicy, VerificationLabel, normalized_text_hash
from groundloop.errors import ArtifactConflictError, ValidationError
from groundloop.m4.contracts import PairKey
from groundloop.m4.models import (
    M4CalibratedVerifierAdapter,
    PairVerificationInput,
    VerificationAdapterSpec,
)


class _FixedVerifier:
    def __init__(self) -> None:
        delegate = DeterministicFakeVerifier(max_length=64)
        self.model_artifact = delegate.model_artifact
        self.prompt_artifact = delegate.prompt_artifact
        self.calibration_version = delegate.calibration_version
        self.temperature = delegate.temperature
        self.max_length = delegate.max_length
        self.batches: list[tuple[PairKey, ...]] = []

    def verify_batch(
        self, pairs: Sequence[tuple[AtomicClaim, ChunkDraft]]
    ) -> tuple[VerificationResult, ...]:
        self.batches.append(
            tuple(
                PairKey(claim.local_claim_id, chunk.chunk_version_id)
                for claim, chunk in pairs
            )
        )
        results: list[VerificationResult] = []
        for claim, chunk in pairs:
            digest = hashlib.sha256(
                f"{claim.text}\0{chunk.text}".encode()
            ).hexdigest()
            logits = (-1.6094379124341003, -0.5108256237659907, -1.6094379124341003)
            results.append(
                VerificationResult(
                    claim_id=claim.local_claim_id,
                    chunk_version_id=chunk.chunk_version_id,
                    candidate_id=f"candidate-{claim.local_claim_id}-{chunk.chunk_version_id}",
                    model_artifact_id=self.model_artifact.artifact_id,
                    prompt_artifact_id=self.prompt_artifact.artifact_id,
                    calibration_version=self.calibration_version,
                    temperature=self.temperature,
                    scores=logits_to_score_triple(logits, self.temperature),
                    input_hash=digest,
                    raw_output_hash=hashlib.sha256(
                        f"output\0{digest}".encode()
                    ).hexdigest(),
                    raw_logits=logits,
                )
            )
        return tuple(results)


class _DriftedScoresVerifier(_FixedVerifier):
    def verify_batch(
        self, pairs: Sequence[tuple[AtomicClaim, ChunkDraft]]
    ) -> tuple[VerificationResult, ...]:
        results = super().verify_batch(pairs)
        return tuple(
            replace(
                result,
                scores=logits_to_score_triple(
                    (result.raw_logits or (0.0, 0.0, 0.0)),
                    self.temperature * 2.0,
                ),
            )
            for result in results
        )


def _input(
    claim_id: str, chunk_id: str, claim_text: str = "Claim"
) -> PairVerificationInput:
    chunk_text = f"Evidence for {chunk_id}."
    return PairVerificationInput(
        pair=PairKey(claim_id, chunk_id),
        claim_text=claim_text,
        claim_required=True,
        claim_cited_chunk_version_ids=("original-citation",),
        document_version_id="document-version",
        chunk_index=0,
        chunk_text=chunk_text,
        chunk_text_hash=normalized_text_hash(chunk_text),
        chunker_artifact_id="fixed-char-v1-1200-no-overlap",
    )


def _adapter(
    backend: _FixedVerifier | None = None,
    *,
    policy: DecisionPolicy | None = None,
    batch_size: int = 1,
) -> M4CalibratedVerifierAdapter:
    chosen = backend or _FixedVerifier()
    return M4CalibratedVerifierAdapter(
        chosen,
        VerificationAdapterSpec(
            model_artifact=chosen.model_artifact,
            prompt_artifact=chosen.prompt_artifact,
            calibration_version=chosen.calibration_version,
            calibration_artifact_sha256=None,
            temperature=chosen.temperature,
            max_length=chosen.max_length,
            decision_policy=policy or DecisionPolicy("policy", 0.8, 0.8),
        ),
        batch_size=batch_size,
    )


def test_pairkeys_are_sorted_and_batched_deterministically() -> None:
    backend = _FixedVerifier()
    adapter = _adapter(backend, batch_size=1)
    results = adapter.verify_pairs(
        (_input("claim-2", "chunk-2"), _input("claim-1", "chunk-1"))
    )
    assert tuple(item.pair for item in results) == (
        PairKey("claim-1", "chunk-1"),
        PairKey("claim-2", "chunk-2"),
    )
    assert backend.batches == [
        (PairKey("claim-1", "chunk-1"),),
        (PairKey("claim-2", "chunk-2"),),
    ]


def test_operational_label_uses_threshold_policy_not_argmax() -> None:
    result = _adapter().verify_pairs((_input("claim", "chunk"),))[0]
    assert result.result.scores.support == 0.6
    assert result.result.scores.support > result.result.scores.refute
    assert result.operational_label is VerificationLabel.NEUTRAL
    judgment = result.to_pair_judgment(split_id="development")
    assert judgment.derived_label is VerificationLabel.NEUTRAL
    assert judgment.support_score == 0.6
    assert result.result.raw_logits == (
        -1.6094379124341003,
        -0.5108256237659907,
        -1.6094379124341003,
    )


def test_lower_threshold_changes_only_derived_operational_label() -> None:
    high = _adapter(policy=DecisionPolicy("high", 0.8, 0.8)).verify_pairs(
        (_input("claim", "chunk"),)
    )[0]
    low = _adapter(policy=DecisionPolicy("low", 0.5, 0.8)).verify_pairs(
        (_input("claim", "chunk"),)
    )[0]
    assert high.result.scores == low.result.scores
    assert high.operational_label is VerificationLabel.NEUTRAL
    assert low.operational_label is VerificationLabel.SUPPORT
    assert high.artifact_id != low.artifact_id


def test_exact_verification_replay_reuses_artifact_without_model_call() -> None:
    backend = _FixedVerifier()
    adapter = _adapter(backend, batch_size=8)
    pair = _input("claim", "chunk")
    first = adapter.verify_pairs((pair,))
    second = adapter.verify_pairs((pair,))
    assert first == second
    assert len(backend.batches) == 1
    assert adapter.backend_pair_calls == 1
    assert adapter.reused_pair_count == 1


def test_pairkey_reuse_with_changed_text_is_a_conflict() -> None:
    adapter = _adapter()
    adapter.verify_pairs((_input("claim", "chunk", "first claim"),))
    with pytest.raises(ArtifactConflictError, match="immutable verifier input drift"):
        adapter.verify_pairs((_input("claim", "chunk", "changed claim"),))


def test_calibrated_scores_must_match_preserved_raw_logits() -> None:
    backend = _DriftedScoresVerifier()
    with pytest.raises(ArtifactConflictError, match="do not match raw logits"):
        _adapter(backend).verify_pairs((_input("claim", "chunk"),))


def test_verifier_model_prompt_calibration_and_policy_are_bound() -> None:
    backend = _FixedVerifier()
    adapter = _adapter(backend)
    artifact = adapter.verify_pairs((_input("claim", "chunk"),))[0]
    observation = artifact.to_semantic_observation(
        model_id=backend.model_artifact.model_id,
        model_revision=backend.model_artifact.immutable_revision,
        prompt_version=backend.prompt_artifact.version,
    )
    assert artifact.execution_spec_hash == adapter.spec.execution_spec_hash
    assert artifact.result.model_artifact_id == backend.model_artifact.artifact_id
    assert artifact.result.prompt_artifact_id == backend.prompt_artifact.artifact_id
    assert observation.support_score == artifact.result.scores.support
    assert observation.input_hash == artifact.result.input_hash

    original = backend.prompt_artifact
    backend.prompt_artifact = PromptArtifact(
        artifact_id="changed-prompt",
        task=original.task,
        version=original.version,
        template=original.template,
        template_hash=original.template_hash,
        decoding_config_hash=original.decoding_config_hash,
    )
    with pytest.raises(ArtifactConflictError, match="prompt artifact drift"):
        adapter.verify_pairs((_input("other", "other"),))


def test_duplicate_pairkeys_and_invalid_batch_size_are_rejected() -> None:
    backend = _FixedVerifier()
    with pytest.raises(ValidationError, match="batch_size"):
        _adapter(backend, batch_size=0)
    duplicate = _input("claim", "chunk")
    with pytest.raises(ValidationError, match="duplicate PairKey"):
        _adapter().verify_pairs((duplicate, duplicate))
