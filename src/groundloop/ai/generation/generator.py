"""Schema-constrained cited answer generation."""

from __future__ import annotations

from dataclasses import dataclass

from groundloop.ai.contracts import (
    CitedAnswer,
    EvidencePassage,
    ModelArtifact,
    ModelTask,
    PromptArtifact,
    QueryKind,
)
from groundloop.ai.generation._shared import (
    CompletionBackend,
    CompletionRequest,
    DecodingConfig,
    StructuredOutputError,
    StructuredOutputProvenance,
    make_provenance,
    normalize_text_v1,
    normalized_complete_input_hash,
    parse_json_object,
    run_with_one_repair,
    sha256_text,
)
from groundloop.ai.generation.artifacts import (
    GENERATION_DECODING_CONFIG,
    generation_prompt_artifact,
    qwen_model_artifact,
)
from groundloop.ai.generation.backends import (
    DeterministicJsonCompletionBackend,
    QwenCompletionBackend,
)
from groundloop.errors import ValidationError


@dataclass(frozen=True, slots=True)
class GenerationExecution:
    answer: CitedAnswer
    provenance: StructuredOutputProvenance


class StructuredCitedAnswerGenerator:
    """Validate a decoder response against retrieved immutable evidence."""

    def __init__(
        self,
        *,
        backend: CompletionBackend,
        model_artifact: ModelArtifact,
        prompt_artifact: PromptArtifact,
        decoding_config: DecodingConfig,
    ) -> None:
        if model_artifact.task is not ModelTask.GENERATION:
            raise ValidationError("generation model artifact has the wrong task")
        if prompt_artifact.task is not ModelTask.GENERATION:
            raise ValidationError("generation prompt artifact has the wrong task")
        if prompt_artifact.decoding_config_hash != decoding_config.config_hash:
            raise ValidationError("generation decoding config violates prompt artifact")
        self.backend = backend
        self.model_artifact = model_artifact
        self.prompt_artifact = prompt_artifact
        self.decoding_config = decoding_config
        self._last_provenance: StructuredOutputProvenance | None = None

    @property
    def last_provenance(self) -> StructuredOutputProvenance | None:
        return self._last_provenance

    def generate(
        self,
        question: str,
        evidence: tuple[EvidencePassage, ...],
    ) -> CitedAnswer:
        return self.generate_with_provenance(question, evidence).answer

    def generate_with_provenance(
        self,
        question: str,
        evidence: tuple[EvidencePassage, ...],
    ) -> GenerationExecution:
        normalized_question = normalize_text_v1(question)
        if not normalized_question:
            raise ValidationError("generation question must be non-empty")
        if not evidence:
            raise ValidationError("generation requires retrieved evidence")

        ordered_context_ids = tuple(
            passage.chunk.chunk_version_id for passage in evidence
        )
        if len(set(ordered_context_ids)) != len(ordered_context_ids):
            raise ValidationError("generation evidence chunk IDs must be unique")
        for passage in evidence:
            if passage.candidate.query_kind is not QueryKind.QUESTION:
                raise ValidationError(
                    "generation evidence must come from a question query"
                )

        payload: dict[str, object] = {
            "question": normalized_question,
            "retrieved_passages": [
                {
                    "chunk_version_id": passage.chunk.chunk_version_id,
                    "text": normalize_text_v1(passage.chunk.text),
                }
                for passage in evidence
            ],
        }
        input_hash = normalized_complete_input_hash(
            task=ModelTask.GENERATION,
            model_artifact=self.model_artifact,
            prompt_artifact=self.prompt_artifact,
            ordered_context_ids=ordered_context_ids,
            payload=payload,
        )
        request = CompletionRequest(
            task=ModelTask.GENERATION,
            system_prompt=self.prompt_artifact.template,
            payload=payload,
        )

        def validate(raw_output: str) -> tuple[str, tuple[str, ...]]:
            value = parse_json_object(raw_output)
            expected_keys = {"answer_text", "cited_chunk_version_ids"}
            if set(value) != expected_keys:
                raise StructuredOutputError(
                    "generation JSON must contain only answer_text and "
                    "cited_chunk_version_ids"
                )
            answer_text = value["answer_text"]
            citation_value = value["cited_chunk_version_ids"]
            if not isinstance(answer_text, str) or not answer_text.strip():
                raise StructuredOutputError("answer_text must be a nonempty string")
            if not isinstance(citation_value, list) or not citation_value:
                raise StructuredOutputError(
                    "cited_chunk_version_ids must be a nonempty list"
                )
            if not all(
                isinstance(chunk_id, str) and chunk_id.strip()
                for chunk_id in citation_value
            ):
                raise StructuredOutputError("every answer citation must be a string")
            citations = tuple(str(chunk_id) for chunk_id in citation_value)
            if len(set(citations)) != len(citations):
                raise StructuredOutputError("answer citations must be unique")
            unsupported = set(citations) - set(ordered_context_ids)
            if unsupported:
                raise StructuredOutputError(
                    "answer cited unresolved or non-retrieved chunk IDs: "
                    + ", ".join(sorted(unsupported))
                )
            return answer_text.strip(), citations

        parsed, raw_output, repair_count = run_with_one_repair(
            backend=self.backend,
            request=request,
            decoding_config=self.decoding_config,
            validator=validate,
        )
        answer_text, citations = parsed
        answer = CitedAnswer(
            text=answer_text,
            cited_chunk_version_ids=citations,
            input_hash=input_hash,
            raw_output_hash=sha256_text(raw_output),
            repair_count=repair_count,
        )
        provenance = make_provenance(
            task=ModelTask.GENERATION,
            model_artifact=self.model_artifact,
            prompt_artifact=self.prompt_artifact,
            ordered_context_ids=ordered_context_ids,
            normalized_input_hash=input_hash,
            raw_output=raw_output,
            repair_count=repair_count,
        )
        self._last_provenance = provenance
        return GenerationExecution(answer=answer, provenance=provenance)


class DeterministicAnswerGenerator(StructuredCitedAnswerGenerator):
    """No-network deterministic answer generator for ordinary tests."""

    def __init__(self) -> None:
        super().__init__(
            backend=DeterministicJsonCompletionBackend(),
            model_artifact=qwen_model_artifact(ModelTask.GENERATION),
            prompt_artifact=generation_prompt_artifact(),
            decoding_config=GENERATION_DECODING_CONFIG,
        )


class QwenAnswerGenerator(StructuredCitedAnswerGenerator):
    """Pinned Qwen answer generator; downloads remain explicit opt-in."""

    def __init__(
        self,
        *,
        backend: QwenCompletionBackend | None = None,
        allow_download: bool = False,
    ) -> None:
        super().__init__(
            backend=backend or QwenCompletionBackend(allow_download=allow_download),
            model_artifact=qwen_model_artifact(ModelTask.GENERATION),
            prompt_artifact=generation_prompt_artifact(),
            decoding_config=GENERATION_DECODING_CONFIG,
        )
