from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from groundloop.ai.contracts import AtomicClaim, ChunkDraft
from groundloop.ai.verification.adapter import (
    DeterministicFakeVerifier,
    PinnedMiniLMVerifier,
    logits_to_score_triple,
)
from groundloop.domain import (
    DecisionPolicy,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
    VerificationLabel,
    normalized_text_hash,
)
from groundloop.errors import ValidationError
from groundloop.policy import decide


def _pair(
    *, evidence: str = "Nimbus requires Python 3.12."
) -> tuple[AtomicClaim, ChunkDraft]:
    chunk = ChunkDraft(
        chunk_version_id="chunk-1",
        document_version_id="document-version-1",
        chunk_index=0,
        text=evidence,
        text_hash=normalized_text_hash(evidence),
        chunker_artifact_id="test-chunker",
    )
    return AtomicClaim(
        "claim-1", "Nimbus requires Python 3.12.", True, ("chunk-1",)
    ), chunk


def test_base_label_order_maps_entailment_to_stored_support() -> None:
    scores = logits_to_score_triple((-4.0, 8.0, -2.0))
    assert scores.support > 0.999
    assert scores.refute < scores.neutral < scores.support


def test_base_label_order_maps_contradiction_to_stored_refute() -> None:
    scores = logits_to_score_triple((8.0, -4.0, -2.0))
    assert scores.refute > 0.999
    assert scores.support < scores.neutral < scores.refute


def test_equal_logits_remain_a_tie_and_policy_resolves_toward_refute() -> None:
    scores = logits_to_score_triple((0.0, 0.0, 0.0))
    assert scores.support == pytest.approx(1 / 3)
    assert scores.refute == pytest.approx(1 / 3)
    observation = SemanticObservation(
        observation_id="observation-1",
        subject_kind=SubjectKind.CLAIM,
        subject_id="claim-1",
        chunk_version_id="chunk-1",
        task_type="verify",
        support_score=scores.support,
        refute_score=scores.refute,
        neutral_score=scores.neutral,
        producer=ModelStamp("fake", "v1", "pair-v1"),
        input_hash="input-1",
    )
    assert (
        decide(observation, DecisionPolicy("policy-1", 0.3, 0.3))
        is VerificationLabel.REFUTE
    )


@pytest.mark.parametrize(
    "logits,temperature",
    [((1.0, 2.0), 1.0), ((1.0, float("nan"), 2.0), 1.0), ((1.0, 2.0, 3.0), 0.0)],
)
def test_malformed_logits_and_temperature_are_rejected(
    logits: tuple[float, ...], temperature: float
) -> None:
    with pytest.raises(ValidationError):
        logits_to_score_triple(logits, temperature)


def test_fake_is_deterministic_batched_and_records_truncation() -> None:
    pair = _pair(evidence="token " * 50)
    verifier = DeterministicFakeVerifier(max_length=8)
    first = verifier.verify_batch((pair, pair), batch_size=1)
    second = verifier.verify_batch((pair, pair), batch_size=2)
    assert first == second
    assert verifier.last_diagnostics.examples == 2
    assert verifier.last_diagnostics.batches == 1
    assert verifier.last_diagnostics.truncated_examples == 2


def test_empty_evidence_is_rejected_at_adapter_boundary() -> None:
    claim, _ = _pair()
    malformed = cast(
        ChunkDraft,
        SimpleNamespace(text="", chunk_version_id="chunk-x"),
    )
    with pytest.raises(ValidationError, match="evidence text"):
        DeterministicFakeVerifier().verify(claim, malformed)


def test_inactive_late_result_remains_auditable_but_emits_no_delta() -> None:
    claim, chunk = _pair()
    completion = DeterministicFakeVerifier().complete(claim, chunk, chunk_active=False)
    assert completion.result.chunk_version_id == chunk.chunk_version_id
    assert not completion.should_emit_delta


def test_real_adapter_is_lazy_and_missing_checkpoint_fails_without_download(
    tmp_path: Path,
) -> None:
    verifier = PinnedMiniLMVerifier(
        model_path=str(tmp_path),
        local_files_only=True,
        artifact_sha256="a" * 64,
        logical_model_id="groundloop/test-checkpoint",
    )
    claim, chunk = _pair()
    with pytest.raises(FileNotFoundError, match="absent"):
        verifier.verify(claim, chunk)


def test_local_checkpoint_identity_is_stable_across_relocation() -> None:
    common = {
        "artifact_sha256": "b" * 64,
        "logical_model_id": "groundloop/minilm2-m3-bounded-v1",
        "model_revision": "groundloop-m3-bounded-v1",
    }
    first = PinnedMiniLMVerifier(model_path="/first/location", **common)
    second = PinnedMiniLMVerifier(model_path="/relocated/checkpoint", **common)
    assert first.model_artifact == second.model_artifact
