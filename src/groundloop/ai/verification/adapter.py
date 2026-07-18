"""Deterministic and pinned real adapters for three-way NLI verification.

The upstream MiniLM checkpoint emits logits in contradiction, entailment,
neutral order. GroundLoop stores probabilities in support, refute, neutral
order and derives labels later through its versioned decision policy.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, cast

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
from groundloop.ai.verification.constants import BASE_MODEL_ID, BASE_MODEL_REVISION
from groundloop.errors import ValidationError


@dataclass(frozen=True, slots=True)
class AdapterDiagnostics:
    examples: int
    truncated_examples: int
    batches: int
    max_length: int


@dataclass(frozen=True, slots=True)
class VerificationCompletion:
    """Late-result activation hint without discarding the auditable result."""

    result: VerificationResult
    chunk_active_at_completion: bool

    @property
    def should_emit_delta(self) -> bool:
        return self.chunk_active_at_completion


def _require_temperature(temperature: float) -> None:
    if not math.isfinite(temperature) or temperature <= 0.0:
        raise ValidationError("temperature must be finite and positive")


def logits_to_score_triple(
    base_logits: Sequence[float], temperature: float = 1.0
) -> ScoreTriple:
    """Map base-order logits to a finite GroundLoop score triple."""
    _require_temperature(temperature)
    if len(base_logits) != 3:
        raise ValidationError("verifier must emit exactly three logits")
    logits = tuple(float(value) for value in base_logits)
    if any(not math.isfinite(value) for value in logits):
        raise ValidationError("verifier logits must be finite")
    scaled = tuple(value / temperature for value in logits)
    maximum = max(scaled)
    exponentials = tuple(math.exp(value - maximum) for value in scaled)
    denominator = sum(exponentials)
    if not math.isfinite(denominator) or denominator <= 0.0:
        raise ValidationError("softmax normalization failed")
    contradiction, entailment, neutral = (value / denominator for value in exponentials)
    return ScoreTriple(
        support=entailment,
        refute=contradiction,
        neutral=neutral,
    )


def _prompt_artifact() -> PromptArtifact:
    template = "premise={evidence}\nhypothesis={claim}"
    return PromptArtifact(
        artifact_id="groundloop-verifier-pair-v1",
        task=ModelTask.VERIFICATION,
        version="verifier-pair-v1",
        template=template,
        template_hash=hashlib.sha256(template.encode("utf-8")).hexdigest(),
        decoding_config_hash=stable_digest("three-way-nli", "premise-first"),
    )


def _model_artifact(
    *, artifact_sha256: str | None, config_hash: str, model_id: str, revision: str
) -> ModelArtifact:
    return ModelArtifact(
        artifact_id=stable_digest("verifier", model_id, revision, config_hash),
        task=ModelTask.VERIFICATION,
        provider="huggingface",
        model_id=model_id,
        immutable_revision=revision,
        tokenizer_revision=revision,
        license_id="Apache-2.0",
        config_hash=config_hash,
        artifact_sha256=artifact_sha256,
    )


def _validate_pair(claim: AtomicClaim, chunk: ChunkDraft) -> None:
    if not claim.text.strip():
        raise ValidationError("claim text must be non-empty")
    if not chunk.text.strip():
        raise ValidationError("evidence text must be non-empty")


def _result(
    claim: AtomicClaim,
    chunk: ChunkDraft,
    *,
    logits: tuple[float, float, float],
    scores: ScoreTriple,
    model_artifact: ModelArtifact,
    prompt_artifact: PromptArtifact,
    max_length: int,
    calibration_version: str,
    temperature: float,
) -> VerificationResult:
    candidate_id = stable_digest(
        "verification-candidate-v1",
        claim.local_claim_id,
        chunk.chunk_version_id,
    )
    input_hash = stable_digest(
        "verification-input-v1",
        claim.text.strip(),
        chunk.text.strip(),
        model_artifact.artifact_id,
        prompt_artifact.artifact_id,
        str(max_length),
        calibration_version,
        repr(temperature),
    )
    raw_output = json.dumps(
        {
            "base_logit_order": ["contradiction", "entailment", "neutral"],
            "logits": logits,
            "stored_probability_order": ["support", "refute", "neutral"],
            "scores": [scores.support, scores.refute, scores.neutral],
            "calibration_version": calibration_version,
            "temperature": temperature,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return VerificationResult(
        claim_id=claim.local_claim_id,
        chunk_version_id=chunk.chunk_version_id,
        candidate_id=candidate_id,
        model_artifact_id=model_artifact.artifact_id,
        prompt_artifact_id=prompt_artifact.artifact_id,
        calibration_version=calibration_version,
        temperature=temperature,
        scores=scores,
        input_hash=input_hash,
        raw_output_hash=hashlib.sha256(raw_output.encode("utf-8")).hexdigest(),
        raw_logits=logits,
    )


class DeterministicFakeVerifier:
    """Download-free deterministic verifier used by ordinary tests."""

    def __init__(self, *, temperature: float = 1.0, max_length: int = 64) -> None:
        _require_temperature(temperature)
        if max_length <= 0:
            raise ValidationError("max_length must be positive")
        self.temperature = temperature
        self.max_length = max_length
        config_hash = stable_digest(
            "deterministic-fake-v1", str(temperature), str(max_length)
        )
        self.model_artifact = _model_artifact(
            artifact_sha256=None,
            config_hash=config_hash,
            model_id="groundloop/deterministic-fake-verifier",
            revision="fake-v1",
        )
        self.prompt_artifact = _prompt_artifact()
        self.calibration_version = stable_digest(
            "fake-temperature-v1", repr(temperature)
        )
        self.last_diagnostics = AdapterDiagnostics(0, 0, 0, max_length)

    @staticmethod
    def _logits(claim: str, evidence: str) -> tuple[float, float, float]:
        digest = hashlib.sha256(
            stable_digest("fake-logits-v1", evidence, claim).encode("ascii")
        ).digest()
        values = tuple((digest[index] / 255.0) * 4.0 - 2.0 for index in range(3))
        return cast(tuple[float, float, float], values)

    def verify(self, claim: AtomicClaim, chunk: ChunkDraft) -> VerificationResult:
        return self.verify_batch(((claim, chunk),))[0]

    def verify_batch(
        self,
        pairs: Sequence[tuple[AtomicClaim, ChunkDraft]],
        *,
        batch_size: int = 8,
    ) -> tuple[VerificationResult, ...]:
        if batch_size <= 0:
            raise ValidationError("batch_size must be positive")
        results: list[VerificationResult] = []
        truncated = 0
        for claim, chunk in pairs:
            _validate_pair(claim, chunk)
            approximate_tokens = len((chunk.text + " " + claim.text).split()) + 3
            truncated += int(approximate_tokens > self.max_length)
            logits = self._logits(claim.text, chunk.text)
            results.append(
                _result(
                    claim,
                    chunk,
                    logits=logits,
                    scores=logits_to_score_triple(logits, self.temperature),
                    model_artifact=self.model_artifact,
                    prompt_artifact=self.prompt_artifact,
                    max_length=self.max_length,
                    calibration_version=self.calibration_version,
                    temperature=self.temperature,
                )
            )
        batches = math.ceil(len(pairs) / batch_size) if pairs else 0
        self.last_diagnostics = AdapterDiagnostics(
            len(pairs), truncated, batches, self.max_length
        )
        return tuple(results)

    def complete(
        self, claim: AtomicClaim, chunk: ChunkDraft, *, chunk_active: bool
    ) -> VerificationCompletion:
        return VerificationCompletion(self.verify(claim, chunk), chunk_active)


class PinnedMiniLMVerifier:
    """Lazy-loading adapter for the frozen MiniLM2 verifier checkpoint."""

    def __init__(
        self,
        *,
        model_path: str | None = None,
        model_revision: str = BASE_MODEL_REVISION,
        temperature: float = 1.0,
        max_length: int = 256,
        batch_size: int = 8,
        local_files_only: bool = True,
        artifact_sha256: str | None = None,
        logical_model_id: str | None = None,
        calibration_version: str | None = None,
    ) -> None:
        _require_temperature(temperature)
        if max_length <= 0 or batch_size <= 0:
            raise ValidationError("max_length and batch_size must be positive")
        if model_path is None and model_revision != BASE_MODEL_REVISION:
            raise ValidationError("the base verifier must use its frozen revision")
        if model_path is not None and artifact_sha256 is None:
            raise ValidationError("a local checkpoint requires its tree SHA-256")
        if model_path is not None and logical_model_id is None:
            raise ValidationError("a local checkpoint requires a logical model ID")
        self.model_path = model_path
        self.model_revision = model_revision
        self.temperature = temperature
        self.max_length = max_length
        self.batch_size = batch_size
        self.local_files_only = local_files_only
        model_id = logical_model_id or BASE_MODEL_ID
        config_hash = stable_digest(
            "minilm-verifier-v1",
            model_id,
            model_revision,
            artifact_sha256 or "hub-revision",
            str(max_length),
        )
        self.model_artifact = _model_artifact(
            artifact_sha256=artifact_sha256,
            config_hash=config_hash,
            model_id=model_id,
            revision=model_revision,
        )
        self.prompt_artifact = _prompt_artifact()
        self.calibration_version = calibration_version or stable_digest(
            "uncalibrated-temperature-v1", repr(temperature)
        )
        self.last_diagnostics = AdapterDiagnostics(0, 0, 0, max_length)
        self._tokenizer: Any | None = None
        self._model: Any | None = None

    def _load(self) -> tuple[Any, Any]:
        if self._tokenizer is None or self._model is None:
            try:
                transformers = importlib.import_module("transformers")
            except ImportError as error:
                raise RuntimeError(
                    "real verifier requires the optional ML dependencies"
                ) from error
            source = self.model_path or BASE_MODEL_ID
            revision = None if self.model_path else self.model_revision
            try:
                self._tokenizer = transformers.AutoTokenizer.from_pretrained(
                    source,
                    revision=revision,
                    local_files_only=self.local_files_only,
                )
                self._model = (
                    transformers.AutoModelForSequenceClassification.from_pretrained(
                        source,
                        revision=revision,
                        local_files_only=self.local_files_only,
                    )
                )
            except (OSError, ValueError) as error:
                mode = "local checkpoint" if self.model_path else "pinned base model"
                raise FileNotFoundError(
                    f"{mode} is absent; run the explicit real-smoke/download command"
                ) from error
            self._model.eval()
            if int(self._model.config.num_labels) != 3:
                raise ValidationError(
                    "pinned verifier must expose exactly three labels"
                )
        return self._tokenizer, self._model

    def verify(self, claim: AtomicClaim, chunk: ChunkDraft) -> VerificationResult:
        return self.verify_batch(((claim, chunk),))[0]

    def verify_batch(
        self, pairs: Sequence[tuple[AtomicClaim, ChunkDraft]]
    ) -> tuple[VerificationResult, ...]:
        if not pairs:
            self.last_diagnostics = AdapterDiagnostics(0, 0, 0, self.max_length)
            return ()
        for claim, chunk in pairs:
            _validate_pair(claim, chunk)
        tokenizer, model = self._load()
        try:
            torch = importlib.import_module("torch")
        except ImportError as error:
            raise RuntimeError("real verifier requires PyTorch") from error
        results: list[VerificationResult] = []
        truncated = 0
        for start in range(0, len(pairs), self.batch_size):
            batch = pairs[start : start + self.batch_size]
            premises = [chunk.text for _, chunk in batch]
            hypotheses = [claim.text for claim, _ in batch]
            full = tokenizer(
                premises,
                hypotheses,
                add_special_tokens=True,
                truncation=False,
            )
            truncated += sum(
                int(len(token_ids) > self.max_length) for token_ids in full["input_ids"]
            )
            encoded = tokenizer(
                premises,
                hypotheses,
                padding=True,
                truncation=True,
                max_length=self.max_length,
                return_tensors="pt",
            )
            with torch.inference_mode():
                batch_logits = model(**encoded).logits.detach().cpu().tolist()
            for (claim, chunk), raw in zip(batch, batch_logits, strict=True):
                logits = cast(
                    tuple[float, float, float], tuple(float(value) for value in raw)
                )
                results.append(
                    _result(
                        claim,
                        chunk,
                        logits=logits,
                        scores=logits_to_score_triple(logits, self.temperature),
                        model_artifact=self.model_artifact,
                        prompt_artifact=self.prompt_artifact,
                        max_length=self.max_length,
                        calibration_version=self.calibration_version,
                        temperature=self.temperature,
                    )
                )
        self.last_diagnostics = AdapterDiagnostics(
            len(pairs),
            truncated,
            math.ceil(len(pairs) / self.batch_size),
            self.max_length,
        )
        return tuple(results)

    def complete(
        self, claim: AtomicClaim, chunk: ChunkDraft, *, chunk_active: bool
    ) -> VerificationCompletion:
        return VerificationCompletion(self.verify(claim, chunk), chunk_active)


def batched(items: Iterable[Any], size: int) -> Iterable[tuple[Any, ...]]:
    """Small public helper used by explicit experiment commands."""
    if size <= 0:
        raise ValidationError("batch size must be positive")
    batch: list[Any] = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            yield tuple(batch)
            batch.clear()
    if batch:
        yield tuple(batch)
