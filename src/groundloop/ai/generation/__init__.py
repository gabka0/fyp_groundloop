"""Cited answer generation with frozen M3 provenance."""

from groundloop.ai.generation._shared import (
    DecodingConfig,
    RepairExhaustedError,
    StructuredOutputError,
    StructuredOutputProvenance,
)
from groundloop.ai.generation.artifacts import (
    GENERATION_DECODING_CONFIG,
    GENERATION_PROMPT_TEMPLATE,
    QWEN_MODEL_ID,
    QWEN_REVISION,
    generation_prompt_artifact,
    qwen_model_artifact,
)
from groundloop.ai.generation.backends import (
    DeterministicJsonCompletionBackend,
    QwenCompletionBackend,
    ScriptedCompletionBackend,
)
from groundloop.ai.generation.generator import (
    DeterministicAnswerGenerator,
    GenerationExecution,
    QwenAnswerGenerator,
    StructuredCitedAnswerGenerator,
)

__all__ = [
    "DecodingConfig",
    "DeterministicAnswerGenerator",
    "DeterministicJsonCompletionBackend",
    "GENERATION_DECODING_CONFIG",
    "GENERATION_PROMPT_TEMPLATE",
    "GenerationExecution",
    "QWEN_MODEL_ID",
    "QWEN_REVISION",
    "QwenAnswerGenerator",
    "QwenCompletionBackend",
    "RepairExhaustedError",
    "ScriptedCompletionBackend",
    "StructuredCitedAnswerGenerator",
    "StructuredOutputError",
    "StructuredOutputProvenance",
    "generation_prompt_artifact",
    "qwen_model_artifact",
]
