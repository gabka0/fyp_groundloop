"""Shared structured-output primitives for M3 generation and extraction."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol, TypeVar

from groundloop.ai.contracts import ModelArtifact, ModelTask, PromptArtifact
from groundloop.errors import ValidationError

_WHITESPACE_RUN = re.compile(r"\s+")
_T = TypeVar("_T")


def normalize_text_v1(text: str) -> str:
    """Return GroundLoop normalization-v1 text without hashing it."""
    return _WHITESPACE_RUN.sub(" ", text.strip())


def canonical_json(value: object) -> str:
    """Serialize a provenance input deterministically without ASCII escaping."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class DecodingConfig:
    """Frozen deterministic decoder settings included in artifact identity."""

    max_new_tokens: int
    do_sample: bool = False
    temperature: float = 0.0
    num_beams: int = 1

    def __post_init__(self) -> None:
        if self.max_new_tokens <= 0:
            raise ValidationError("max_new_tokens must be positive")
        if self.do_sample:
            raise ValidationError("M3 v1 decoding must be deterministic")
        if self.temperature != 0.0:
            raise ValidationError("M3 v1 temperature must be zero")
        if self.num_beams != 1:
            raise ValidationError("M3 v1 num_beams must be one")

    def as_dict(self) -> dict[str, bool | float | int]:
        return {
            "do_sample": self.do_sample,
            "max_new_tokens": self.max_new_tokens,
            "num_beams": self.num_beams,
            "temperature": self.temperature,
        }

    @property
    def config_hash(self) -> str:
        return sha256_text(canonical_json(self.as_dict()))


@dataclass(frozen=True, slots=True)
class CompletionRequest:
    """A task input passed to a decoder backend."""

    task: ModelTask
    system_prompt: str
    payload: Mapping[str, object]
    previous_output: str | None = None
    validation_error: str | None = None

    def __post_init__(self) -> None:
        if self.task not in (ModelTask.GENERATION, ModelTask.CLAIM_EXTRACTION):
            raise ValidationError("unsupported completion task")
        if not self.system_prompt.strip():
            raise ValidationError("system prompt must be non-empty")
        repairing = (
            self.previous_output is not None or self.validation_error is not None
        )
        if repairing and (
            self.previous_output is None or self.validation_error is None
        ):
            raise ValidationError("repair requests require output and validation error")


class CompletionBackend(Protocol):
    """Minimal decoder boundary; real and fake backends share this interface."""

    def complete(
        self,
        request: CompletionRequest,
        decoding_config: DecodingConfig,
    ) -> str: ...


@dataclass(frozen=True, slots=True)
class StructuredOutputProvenance:
    """Immutable execution identity exposed to the coordinator pipeline."""

    task: ModelTask
    model_artifact_id: str
    model_revision: str
    tokenizer_revision: str
    prompt_artifact_id: str
    prompt_hash: str
    decoding_config_hash: str
    ordered_context_ids: tuple[str, ...]
    normalized_input_hash: str
    raw_output_hash: str
    repair_count: int

    def __post_init__(self) -> None:
        if self.task not in (ModelTask.GENERATION, ModelTask.CLAIM_EXTRACTION):
            raise ValidationError("unsupported structured-output provenance task")
        for name, value in (
            ("model_artifact_id", self.model_artifact_id),
            ("model_revision", self.model_revision),
            ("tokenizer_revision", self.tokenizer_revision),
            ("prompt_artifact_id", self.prompt_artifact_id),
        ):
            if not value.strip():
                raise ValidationError(f"{name} must be non-empty")
        for name, value in (
            ("prompt_hash", self.prompt_hash),
            ("decoding_config_hash", self.decoding_config_hash),
            ("normalized_input_hash", self.normalized_input_hash),
            ("raw_output_hash", self.raw_output_hash),
        ):
            is_invalid = len(value) != 64 or any(
                character not in "0123456789abcdef" for character in value
            )
            if is_invalid:
                raise ValidationError(f"{name} must be a lowercase SHA-256 digest")
        if not self.ordered_context_ids:
            raise ValidationError("structured output requires evidence context")
        if len(set(self.ordered_context_ids)) != len(self.ordered_context_ids):
            raise ValidationError("ordered context identifiers must be unique")
        if self.repair_count not in (0, 1):
            raise ValidationError("repair_count must be zero or one")


class StructuredOutputError(ValidationError):
    """A decoder response violates the frozen task schema."""


class RepairExhaustedError(StructuredOutputError):
    """Both the initial response and the single repair response were invalid."""


def parse_json_object(raw_output: str) -> dict[str, object]:
    if not raw_output.strip():
        raise StructuredOutputError("model output must be non-empty JSON")
    try:
        value = json.loads(raw_output)
    except json.JSONDecodeError as error:
        raise StructuredOutputError("model output is malformed JSON") from error
    if not isinstance(value, dict):
        raise StructuredOutputError("model output must be one JSON object")
    return {str(key): item for key, item in value.items()}


def run_with_one_repair(
    *,
    backend: CompletionBackend,
    request: CompletionRequest,
    decoding_config: DecodingConfig,
    validator: Callable[[str], _T],
) -> tuple[_T, str, int]:
    """Validate an initial response and permit exactly one repair attempt."""
    raw_output = backend.complete(request, decoding_config)
    try:
        return validator(raw_output), raw_output, 0
    except StructuredOutputError as first_error:
        repair_request = CompletionRequest(
            task=request.task,
            system_prompt=request.system_prompt,
            payload=request.payload,
            previous_output=raw_output,
            validation_error=str(first_error),
        )
        repaired_output = backend.complete(repair_request, decoding_config)
        try:
            return validator(repaired_output), repaired_output, 1
        except StructuredOutputError as second_error:
            raise RepairExhaustedError(
                "structured output remained invalid after one repair: "
                f"{second_error}"
            ) from second_error


def normalized_complete_input_hash(
    *,
    task: ModelTask,
    model_artifact: ModelArtifact,
    prompt_artifact: PromptArtifact,
    ordered_context_ids: tuple[str, ...],
    payload: Mapping[str, object],
) -> str:
    """Hash all normalized semantic inputs that can affect decoder output."""
    complete_input = {
        "schema_version": "m3-structured-input-v1",
        "task": task.value,
        "model": {
            "artifact_id": model_artifact.artifact_id,
            "model_id": model_artifact.model_id,
            "revision": model_artifact.immutable_revision,
            "tokenizer_revision": model_artifact.tokenizer_revision,
        },
        "prompt": {
            "artifact_id": prompt_artifact.artifact_id,
            "decoding_config_hash": prompt_artifact.decoding_config_hash,
            "template_hash": prompt_artifact.template_hash,
            "version": prompt_artifact.version,
        },
        "ordered_context_ids": list(ordered_context_ids),
        "payload": payload,
    }
    return sha256_text(canonical_json(complete_input))


def make_provenance(
    *,
    task: ModelTask,
    model_artifact: ModelArtifact,
    prompt_artifact: PromptArtifact,
    ordered_context_ids: tuple[str, ...],
    normalized_input_hash: str,
    raw_output: str,
    repair_count: int,
) -> StructuredOutputProvenance:
    return StructuredOutputProvenance(
        task=task,
        model_artifact_id=model_artifact.artifact_id,
        model_revision=model_artifact.immutable_revision,
        tokenizer_revision=model_artifact.tokenizer_revision,
        prompt_artifact_id=prompt_artifact.artifact_id,
        prompt_hash=prompt_artifact.template_hash,
        decoding_config_hash=prompt_artifact.decoding_config_hash,
        ordered_context_ids=ordered_context_ids,
        normalized_input_hash=normalized_input_hash,
        raw_output_hash=sha256_text(raw_output),
        repair_count=repair_count,
    )
