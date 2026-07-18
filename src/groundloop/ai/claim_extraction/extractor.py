"""Schema-constrained atomic claim extraction."""

from __future__ import annotations

from dataclasses import dataclass

from groundloop.ai.claim_extraction.artifacts import (
    CLAIM_EXTRACTION_DECODING_CONFIG,
    claim_extraction_prompt_artifact,
)
from groundloop.ai.contracts import (
    AtomicClaim,
    CitedAnswer,
    ClaimExtractionResult,
    EvidencePassage,
    ModelArtifact,
    ModelTask,
    PromptArtifact,
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
from groundloop.ai.generation.artifacts import qwen_model_artifact
from groundloop.ai.generation.backends import (
    DeterministicJsonCompletionBackend,
    QwenCompletionBackend,
)
from groundloop.errors import ValidationError


@dataclass(frozen=True, slots=True)
class ClaimExtractionExecution:
    result: ClaimExtractionResult
    provenance: StructuredOutputProvenance


class StructuredClaimExtractor:
    """Extract claims while enforcing answer-citation closure."""

    def __init__(
        self,
        *,
        backend: CompletionBackend,
        model_artifact: ModelArtifact,
        prompt_artifact: PromptArtifact,
        decoding_config: DecodingConfig,
    ) -> None:
        if model_artifact.task is not ModelTask.CLAIM_EXTRACTION:
            raise ValidationError("claim-extraction model artifact has the wrong task")
        if prompt_artifact.task is not ModelTask.CLAIM_EXTRACTION:
            raise ValidationError("claim-extraction prompt artifact has the wrong task")
        if prompt_artifact.decoding_config_hash != decoding_config.config_hash:
            raise ValidationError(
                "claim-extraction decoding config violates prompt artifact"
            )
        self.backend = backend
        self.model_artifact = model_artifact
        self.prompt_artifact = prompt_artifact
        self.decoding_config = decoding_config
        self._last_provenance: StructuredOutputProvenance | None = None

    @property
    def last_provenance(self) -> StructuredOutputProvenance | None:
        return self._last_provenance

    def extract(
        self,
        answer: CitedAnswer,
        evidence: tuple[EvidencePassage, ...],
    ) -> ClaimExtractionResult:
        return self.extract_with_provenance(answer, evidence).result

    def extract_with_provenance(
        self,
        answer: CitedAnswer,
        evidence: tuple[EvidencePassage, ...],
    ) -> ClaimExtractionExecution:
        evidence_by_id: dict[str, EvidencePassage] = {}
        for passage in evidence:
            chunk_id = passage.chunk.chunk_version_id
            if chunk_id in evidence_by_id:
                raise ValidationError("claim-extraction evidence IDs must be unique")
            evidence_by_id[chunk_id] = passage

        ordered_context_ids = answer.cited_chunk_version_ids
        missing = set(ordered_context_ids) - set(evidence_by_id)
        if missing:
            raise ValidationError(
                "answer citations are unresolved for extraction: "
                + ", ".join(sorted(missing))
            )
        resolved = tuple(evidence_by_id[chunk_id] for chunk_id in ordered_context_ids)
        payload: dict[str, object] = {
            "answer_text": normalize_text_v1(answer.text),
            "answer_cited_chunk_version_ids": list(ordered_context_ids),
            "resolved_passages": [
                {
                    "chunk_version_id": passage.chunk.chunk_version_id,
                    "text": normalize_text_v1(passage.chunk.text),
                }
                for passage in resolved
            ],
        }
        input_hash = normalized_complete_input_hash(
            task=ModelTask.CLAIM_EXTRACTION,
            model_artifact=self.model_artifact,
            prompt_artifact=self.prompt_artifact,
            ordered_context_ids=ordered_context_ids,
            payload=payload,
        )
        request = CompletionRequest(
            task=ModelTask.CLAIM_EXTRACTION,
            system_prompt=self.prompt_artifact.template,
            payload=payload,
        )

        def validate(raw_output: str) -> tuple[AtomicClaim, ...]:
            value = parse_json_object(raw_output)
            if set(value) != {"claims"}:
                raise StructuredOutputError(
                    "claim-extraction JSON must contain only claims"
                )
            claim_values = value["claims"]
            if not isinstance(claim_values, list) or not claim_values:
                raise StructuredOutputError("claims must be a nonempty list")
            claims: list[AtomicClaim] = []
            local_ids: set[str] = set()
            allowed_citations = set(answer.cited_chunk_version_ids)
            expected_claim_keys = {
                "local_claim_id",
                "text",
                "required",
                "cited_chunk_version_ids",
            }
            for position, claim_value in enumerate(claim_values, start=1):
                if not isinstance(claim_value, dict):
                    raise StructuredOutputError(f"claim {position} must be an object")
                if set(claim_value) != expected_claim_keys:
                    raise StructuredOutputError(
                        f"claim {position} has missing or unexpected fields"
                    )
                local_id = claim_value["local_claim_id"]
                text = claim_value["text"]
                required = claim_value["required"]
                citation_value = claim_value["cited_chunk_version_ids"]
                if not isinstance(local_id, str) or not local_id.strip():
                    raise StructuredOutputError(
                        f"claim {position} local_claim_id must be nonempty"
                    )
                if local_id in local_ids:
                    raise StructuredOutputError(
                        f"duplicate local claim ID: {local_id}"
                    )
                local_ids.add(local_id)
                if not isinstance(text, str) or not text.strip():
                    raise StructuredOutputError(
                        f"claim {position} text must be nonempty"
                    )
                if not isinstance(required, bool):
                    raise StructuredOutputError(
                        f"claim {position} required must be a boolean"
                    )
                if not isinstance(citation_value, list) or not citation_value:
                    raise StructuredOutputError(
                        f"claim {position} citations must be nonempty"
                    )
                if not all(
                    isinstance(chunk_id, str) and chunk_id.strip()
                    for chunk_id in citation_value
                ):
                    raise StructuredOutputError(
                        f"claim {position} citations must be strings"
                    )
                citations = tuple(str(chunk_id) for chunk_id in citation_value)
                if len(set(citations)) != len(citations):
                    raise StructuredOutputError(
                        f"claim {position} citations must be unique"
                    )
                additions = set(citations) - allowed_citations
                if additions:
                    raise StructuredOutputError(
                        "extractor added citations outside the answer set: "
                        + ", ".join(sorted(additions))
                    )
                claims.append(
                    AtomicClaim(
                        local_claim_id=local_id.strip(),
                        text=text.strip(),
                        required=required,
                        cited_chunk_version_ids=citations,
                    )
                )
            if not any(claim.required for claim in claims):
                raise StructuredOutputError(
                    "claim extraction requires at least one required claim"
                )
            return tuple(claims)

        claims, raw_output, repair_count = run_with_one_repair(
            backend=self.backend,
            request=request,
            decoding_config=self.decoding_config,
            validator=validate,
        )
        result = ClaimExtractionResult(
            claims=claims,
            input_hash=input_hash,
            raw_output_hash=sha256_text(raw_output),
            repair_count=repair_count,
        )
        provenance = make_provenance(
            task=ModelTask.CLAIM_EXTRACTION,
            model_artifact=self.model_artifact,
            prompt_artifact=self.prompt_artifact,
            ordered_context_ids=ordered_context_ids,
            normalized_input_hash=input_hash,
            raw_output=raw_output,
            repair_count=repair_count,
        )
        self._last_provenance = provenance
        return ClaimExtractionExecution(result=result, provenance=provenance)


class DeterministicClaimExtractor(StructuredClaimExtractor):
    """No-network deterministic extractor for ordinary tests."""

    def __init__(self) -> None:
        super().__init__(
            backend=DeterministicJsonCompletionBackend(),
            model_artifact=qwen_model_artifact(ModelTask.CLAIM_EXTRACTION),
            prompt_artifact=claim_extraction_prompt_artifact(),
            decoding_config=CLAIM_EXTRACTION_DECODING_CONFIG,
        )


class QwenClaimExtractor(StructuredClaimExtractor):
    """Pinned Qwen extractor; downloads remain explicit opt-in."""

    def __init__(
        self,
        *,
        backend: QwenCompletionBackend | None = None,
        allow_download: bool = False,
    ) -> None:
        super().__init__(
            backend=backend or QwenCompletionBackend(allow_download=allow_download),
            model_artifact=qwen_model_artifact(ModelTask.CLAIM_EXTRACTION),
            prompt_artifact=claim_extraction_prompt_artifact(),
            decoding_config=CLAIM_EXTRACTION_DECODING_CONFIG,
        )
