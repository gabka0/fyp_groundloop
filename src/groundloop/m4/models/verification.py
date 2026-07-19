"""Deterministic M4 batching and reuse over the calibrated M3 verifier."""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Protocol

from groundloop.ai.contracts import (
    AtomicClaim,
    ChunkDraft,
    ModelArtifact,
    PromptArtifact,
    VerificationResult,
)
from groundloop.ai.verification.adapter import logits_to_score_triple
from groundloop.errors import ArtifactConflictError, ValidationError
from groundloop.m4.contracts import PairKey
from groundloop.m4.models.contracts import (
    PairVerificationArtifact,
    PairVerificationInput,
    VerificationAdapterSpec,
    decision_policy_hash,
    derive_operational_label,
)


class M3BatchVerifier(Protocol):
    model_artifact: ModelArtifact
    prompt_artifact: PromptArtifact
    calibration_version: str
    temperature: float
    max_length: int

    def verify_batch(
        self, pairs: Sequence[tuple[AtomicClaim, ChunkDraft]]
    ) -> tuple[VerificationResult, ...]: ...


class M4CalibratedVerifierAdapter:
    """Adapt immutable M4 PairKey inputs to the M3 evidence-first verifier."""

    def __init__(
        self,
        backend: M3BatchVerifier,
        spec: VerificationAdapterSpec,
        *,
        batch_size: int,
    ) -> None:
        if batch_size <= 0:
            raise ValidationError("M4 verifier batch_size must be positive")
        self._backend = backend
        self.spec = spec
        self.batch_size = batch_size
        self._bindings: dict[PairKey, str] = {}
        self._cache: dict[PairKey, PairVerificationArtifact] = {}
        self.backend_pair_calls = 0
        self.reused_pair_count = 0
        self._validate_backend()

    def _validate_backend(self) -> None:
        if self._backend.model_artifact != self.spec.model_artifact:
            raise ArtifactConflictError("verifier model artifact drift")
        if self._backend.prompt_artifact != self.spec.prompt_artifact:
            raise ArtifactConflictError("verifier prompt artifact drift")
        if self._backend.calibration_version != self.spec.calibration_version:
            raise ArtifactConflictError("verifier calibration drift")
        if not math.isclose(
            self._backend.temperature,
            self.spec.temperature,
            rel_tol=0.0,
            abs_tol=0.0,
        ):
            raise ArtifactConflictError("verifier temperature drift")
        if self._backend.max_length != self.spec.max_length:
            raise ArtifactConflictError("verifier tokenizer/max-length drift")

    @staticmethod
    def _reject_duplicate_pairs(inputs: tuple[PairVerificationInput, ...]) -> None:
        pairs = tuple(item.pair for item in inputs)
        if len(set(pairs)) != len(pairs):
            raise ValidationError("duplicate PairKey values in verifier batch")

    def _bind_input(self, item: PairVerificationInput) -> None:
        previous = self._bindings.get(item.pair)
        if previous is not None and previous != item.input_hash:
            raise ArtifactConflictError("PairKey immutable verifier input drift")
        self._bindings[item.pair] = item.input_hash

    def _build_artifact(
        self, item: PairVerificationInput, result: VerificationResult
    ) -> PairVerificationArtifact:
        if result.claim_id != item.pair.claim_id:
            raise ArtifactConflictError("verifier returned a different claim")
        if result.chunk_version_id != item.pair.chunk_version_id:
            raise ArtifactConflictError("verifier returned a different chunk")
        if result.model_artifact_id != self.spec.model_artifact.artifact_id:
            raise ArtifactConflictError("verifier result model identity drift")
        if result.prompt_artifact_id != self.spec.prompt_artifact.artifact_id:
            raise ArtifactConflictError("verifier result prompt identity drift")
        if result.calibration_version != self.spec.calibration_version:
            raise ArtifactConflictError("verifier result calibration drift")
        if not math.isclose(
            result.temperature,
            self.spec.temperature,
            rel_tol=0.0,
            abs_tol=0.0,
        ):
            raise ArtifactConflictError("verifier result temperature drift")
        if result.raw_logits is not None:
            recalculated = logits_to_score_triple(
                result.raw_logits, self.spec.temperature
            )
            for recorded, expected in zip(
                (
                    result.scores.support,
                    result.scores.refute,
                    result.scores.neutral,
                ),
                (
                    recalculated.support,
                    recalculated.refute,
                    recalculated.neutral,
                ),
                strict=True,
            ):
                if not math.isclose(recorded, expected, rel_tol=1e-12, abs_tol=1e-12):
                    raise ArtifactConflictError(
                        "verifier calibrated scores do not match raw logits"
                    )
        label = derive_operational_label(result, self.spec.decision_policy)
        policy_hash = decision_policy_hash(self.spec.decision_policy)
        artifact_id = PairVerificationArtifact.build_artifact_id(
            pair=item.pair,
            pair_input_hash=item.input_hash,
            execution_spec_hash=self.spec.execution_spec_hash,
            result=result,
            decision_policy_hash=policy_hash,
            operational_label=label,
        )
        return PairVerificationArtifact(
            artifact_id=artifact_id,
            pair=item.pair,
            pair_input_hash=item.input_hash,
            execution_spec_hash=self.spec.execution_spec_hash,
            decision_policy_version=self.spec.decision_policy.policy_version,
            decision_policy_hash=policy_hash,
            operational_label=label,
            result=result,
        )

    def verify_pairs(
        self, inputs: Sequence[PairVerificationInput]
    ) -> tuple[PairVerificationArtifact, ...]:
        """Return artifacts in canonical PairKey order, independent of input order."""
        self._validate_backend()
        ordered = tuple(sorted(inputs, key=lambda item: item.pair))
        self._reject_duplicate_pairs(ordered)
        missing: list[PairVerificationInput] = []
        for item in ordered:
            self._bind_input(item)
            if item.pair in self._cache:
                self.reused_pair_count += 1
            else:
                missing.append(item)

        for start in range(0, len(missing), self.batch_size):
            batch = tuple(missing[start : start + self.batch_size])
            m3_pairs = tuple(item.to_m3_pair() for item in batch)
            results = self._backend.verify_batch(m3_pairs)
            self.backend_pair_calls += len(batch)
            if len(results) != len(batch):
                raise ArtifactConflictError("verifier returned a partial batch")
            for item, result in zip(batch, results, strict=True):
                artifact = self._build_artifact(item, result)
                previous = self._cache.get(item.pair)
                if previous is not None and previous != artifact:
                    raise ArtifactConflictError(
                        "verification artifact replay changed content"
                    )
                self._cache[item.pair] = artifact
        return tuple(self._cache[item.pair] for item in ordered)
