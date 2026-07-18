"""Deterministic doubles and the opt-in pinned Qwen decoder backend."""

from __future__ import annotations

import importlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, cast

from groundloop.ai.contracts import ModelTask
from groundloop.ai.generation._shared import (
    CompletionRequest,
    DecodingConfig,
    canonical_json,
    normalize_text_v1,
)
from groundloop.ai.generation.artifacts import QWEN_MODEL_ID, QWEN_REVISION
from groundloop.errors import ValidationError

_SENTENCE_BOUNDARY = re.compile(r"(?<=[.!?])\s+")


@dataclass(slots=True)
class ScriptedCompletionBackend:
    """A no-network deterministic double returning a fixed output sequence."""

    outputs: tuple[str, ...]
    calls: list[CompletionRequest] = field(default_factory=list)

    def complete(
        self,
        request: CompletionRequest,
        decoding_config: DecodingConfig,
    ) -> str:
        del decoding_config
        self.calls.append(request)
        call_index = len(self.calls) - 1
        if call_index >= len(self.outputs):
            raise RuntimeError("scripted completion output exhausted")
        return self.outputs[call_index]


@dataclass(slots=True)
class DeterministicJsonCompletionBackend:
    """Small semantic-free double for ordinary pipeline and replay tests."""

    calls: list[CompletionRequest] = field(default_factory=list)

    def complete(
        self,
        request: CompletionRequest,
        decoding_config: DecodingConfig,
    ) -> str:
        del decoding_config
        self.calls.append(request)
        if request.task is ModelTask.GENERATION:
            return self._generate(request.payload)
        if request.task is ModelTask.CLAIM_EXTRACTION:
            return self._extract(request.payload)
        raise ValidationError("unsupported deterministic completion task")

    @staticmethod
    def _generate(payload: Mapping[str, object]) -> str:
        passages = payload.get("retrieved_passages")
        if not isinstance(passages, list) or not passages:
            return "{}"
        first = passages[0]
        if not isinstance(first, dict):
            return "{}"
        chunk_id = first.get("chunk_version_id")
        text = first.get("text")
        if not isinstance(chunk_id, str) or not isinstance(text, str):
            return "{}"
        normalized = normalize_text_v1(text)
        sentence = _SENTENCE_BOUNDARY.split(normalized, maxsplit=1)[0]
        return json.dumps(
            {
                "answer_text": sentence,
                "cited_chunk_version_ids": [chunk_id],
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @staticmethod
    def _extract(payload: Mapping[str, object]) -> str:
        answer_text = payload.get("answer_text")
        citations = payload.get("answer_cited_chunk_version_ids")
        if not isinstance(answer_text, str) or not isinstance(citations, list):
            return "{}"
        return json.dumps(
            {
                "claims": [
                    {
                        "local_claim_id": "claim-1",
                        "text": normalize_text_v1(answer_text),
                        "required": True,
                        "cited_chunk_version_ids": citations,
                    }
                ]
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )


class QwenCompletionBackend:
    """Lazy CPU adapter for the exact pinned Qwen checkpoint.

    Downloads are disabled by default. Ordinary tests instantiate no real
    model, and the explicit smoke must pass ``allow_download=True`` when the
    coordinator has released the serialized real-workload window.
    """

    def __init__(
        self,
        *,
        model_id: str = QWEN_MODEL_ID,
        revision: str = QWEN_REVISION,
        allow_download: bool = False,
    ) -> None:
        if model_id != QWEN_MODEL_ID or revision != QWEN_REVISION:
            raise ValidationError("Qwen model and revision must match the M3 freeze")
        self.model_id = model_id
        self.revision = revision
        self.allow_download = allow_download
        self._tokenizer: Any | None = None
        self._model: Any | None = None
        self._torch: Any | None = None

    def _load(self) -> tuple[Any, Any, Any]:
        if self._tokenizer is None or self._model is None or self._torch is None:
            transformers = importlib.import_module("transformers")
            torch = importlib.import_module("torch")
            common = {
                "revision": self.revision,
                "local_files_only": not self.allow_download,
                "trust_remote_code": False,
            }
            tokenizer = transformers.AutoTokenizer.from_pretrained(
                self.model_id,
                **common,
            )
            model = transformers.AutoModelForCausalLM.from_pretrained(
                self.model_id,
                **common,
            )
            model.to("cpu")
            model.eval()
            self._tokenizer = tokenizer
            self._model = model
            self._torch = torch
        return self._tokenizer, self._model, self._torch

    def complete(
        self,
        request: CompletionRequest,
        decoding_config: DecodingConfig,
    ) -> str:
        tokenizer, model, torch = self._load()
        messages: list[dict[str, str]] = [
            {"role": "system", "content": request.system_prompt},
            {"role": "user", "content": canonical_json(request.payload)},
        ]
        if request.previous_output is not None:
            messages.extend(
                (
                    {"role": "assistant", "content": request.previous_output},
                    {
                        "role": "user",
                        "content": (
                            "The response failed schema validation: "
                            f"{request.validation_error}. Return one corrected JSON "
                            "object only, using the same original input."
                        ),
                    },
                )
            )
        rendered = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
        )
        encoded = tokenizer(rendered, return_tensors="pt")
        input_ids = encoded["input_ids"]
        input_length = int(input_ids.shape[-1])
        generation_args = {
            "do_sample": decoding_config.do_sample,
            "max_new_tokens": decoding_config.max_new_tokens,
            "num_beams": decoding_config.num_beams,
            "pad_token_id": tokenizer.eos_token_id,
        }
        with torch.inference_mode():
            output = model.generate(**dict(encoded), **generation_args)
        generated_ids = output[0][input_length:]
        decoded = tokenizer.decode(generated_ids, skip_special_tokens=True)
        return cast(str, decoded).strip()


def qwen_backend_metadata(backend: QwenCompletionBackend) -> dict[str, object]:
    """Return serializable adapter metadata without loading model weights."""
    return {
        "allow_download": backend.allow_download,
        "model_id": backend.model_id,
        "revision": backend.revision,
    }
