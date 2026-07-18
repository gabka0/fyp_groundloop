"""Offline tests for the M3 generation and claim-extraction lane."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from groundloop.ai.claim_extraction import (
    CLAIM_EXTRACTION_DECODING_CONFIG,
    CLAIM_EXTRACTION_PROMPT_TEMPLATE,
    DeterministicClaimExtractor,
    StructuredClaimExtractor,
    claim_extraction_prompt_artifact,
)
from groundloop.ai.contracts import (
    ChunkDraft,
    CitedAnswer,
    EvidencePassage,
    ModelTask,
    QueryKind,
    RetrievalCandidate,
)
from groundloop.ai.generation import (
    GENERATION_DECODING_CONFIG,
    GENERATION_PROMPT_TEMPLATE,
    QWEN_REVISION,
    DeterministicAnswerGenerator,
    QwenCompletionBackend,
    RepairExhaustedError,
    ScriptedCompletionBackend,
    StructuredCitedAnswerGenerator,
    generation_prompt_artifact,
    qwen_model_artifact,
)
from groundloop.domain import normalized_text_hash
from groundloop.errors import ValidationError

ROOT = Path(__file__).parents[2]
HASH = "a" * 64


def _evidence(
    chunk_id: str,
    text: str,
    *,
    rank: int = 1,
    query_kind: QueryKind = QueryKind.QUESTION,
) -> EvidencePassage:
    chunk = ChunkDraft(
        chunk_version_id=chunk_id,
        document_version_id=f"document-{chunk_id}",
        chunk_index=rank - 1,
        text=text,
        text_hash=normalized_text_hash(text),
        chunker_artifact_id="fixed-char-v1",
    )
    candidate = RetrievalCandidate(
        candidate_id=f"candidate-{chunk_id}",
        query_kind=query_kind,
        query_id="question-1",
        chunk_version_id=chunk_id,
        score=1.0 - rank / 100.0,
        rank=rank,
        embedding_model_artifact_id="bge-v1",
        method_version="bge-cosine-v1",
    )
    return EvidencePassage(candidate=candidate, chunk=chunk)


def _answer(
    text: str = "Nimbus 4.0 requires Python 3.12 or later.",
    citations: tuple[str, ...] = ("chunk-1",),
) -> CitedAnswer:
    return CitedAnswer(
        text=text,
        cited_chunk_version_ids=citations,
        input_hash=HASH,
        raw_output_hash=HASH,
        repair_count=0,
    )


def _generator(backend: ScriptedCompletionBackend) -> StructuredCitedAnswerGenerator:
    return StructuredCitedAnswerGenerator(
        backend=backend,
        model_artifact=qwen_model_artifact(ModelTask.GENERATION),
        prompt_artifact=generation_prompt_artifact(),
        decoding_config=GENERATION_DECODING_CONFIG,
    )


def _extractor(backend: ScriptedCompletionBackend) -> StructuredClaimExtractor:
    return StructuredClaimExtractor(
        backend=backend,
        model_artifact=qwen_model_artifact(ModelTask.CLAIM_EXTRACTION),
        prompt_artifact=claim_extraction_prompt_artifact(),
        decoding_config=CLAIM_EXTRACTION_DECODING_CONFIG,
    )


def _claim_output(
    *,
    local_id: str = "claim-1",
    text: str = "Nimbus 4.0 requires Python 3.12 or later.",
    required: bool = True,
    citations: list[str] | None = None,
) -> str:
    return json.dumps(
        {
            "claims": [
                {
                    "local_claim_id": local_id,
                    "text": text,
                    "required": required,
                    "cited_chunk_version_ids": citations or ["chunk-1"],
                }
            ]
        },
        ensure_ascii=False,
    )


def test_frozen_prompt_and_decoding_files_match_runtime_artifacts() -> None:
    generation_prompt = ROOT / "prompts/m3/generation/system_v1.txt"
    extraction_prompt = ROOT / "prompts/m3/claim_extraction/system_v1.txt"
    generation_config = ROOT / "configs/m3/generation/generation_v1.json"
    extraction_config = ROOT / "configs/m3/generation/claim_extraction_v1.json"

    assert generation_prompt.read_text(encoding="utf-8") == GENERATION_PROMPT_TEMPLATE
    assert (
        extraction_prompt.read_text(encoding="utf-8")
        == CLAIM_EXTRACTION_PROMPT_TEMPLATE
    )
    assert json.loads(generation_config.read_text()) == (
        GENERATION_DECODING_CONFIG.as_dict()
    )
    assert json.loads(extraction_config.read_text()) == (
        CLAIM_EXTRACTION_DECODING_CONFIG.as_dict()
    )
    assert generation_prompt_artifact().template_hash == hashlib.sha256(
        GENERATION_PROMPT_TEMPLATE.encode()
    ).hexdigest()


def test_generation_repairs_malformed_json_once_and_records_provenance() -> None:
    valid = json.dumps(
        {
            "answer_text": "Nimbus supports Unicode: café and 東京.",
            "cited_chunk_version_ids": ["chunk-1"],
        },
        ensure_ascii=False,
    )
    backend = ScriptedCompletionBackend(("{malformed", valid))
    execution = _generator(backend).generate_with_provenance(
        "What does Nimbus support?",
        (_evidence("chunk-1", "Nimbus supports Unicode: café and 東京."),),
    )

    assert execution.answer.text == "Nimbus supports Unicode: café and 東京."
    assert execution.answer.repair_count == 1
    assert execution.provenance.repair_count == 1
    assert execution.provenance.ordered_context_ids == ("chunk-1",)
    assert len(backend.calls) == 2
    assert backend.calls[1].previous_output == "{malformed"


@pytest.mark.parametrize(
    "first_output",
    [
        "",
        "I refuse to answer.",
        '{"answer_text":"","cited_chunk_version_ids":["chunk-1"]}',
        '{"answer_text":"x","cited_chunk_version_ids":["missing"]}',
        '{"answer_text":"x","cited_chunk_version_ids":["chunk-1","chunk-1"]}',
    ],
)
def test_generation_rejects_second_invalid_response(
    first_output: str,
) -> None:
    backend = ScriptedCompletionBackend((first_output, "{}"))
    with pytest.raises(RepairExhaustedError):
        _generator(backend).generate(
            "Question?",
            (_evidence("chunk-1", "Evidence."),),
        )
    assert len(backend.calls) == 2


def test_generation_context_order_replay_and_version_identity() -> None:
    evidence = (
        _evidence("chunk-1", "First passage.", rank=1),
        _evidence("chunk-2", "Second passage.", rank=2),
    )
    first = DeterministicAnswerGenerator().generate_with_provenance("Q?", evidence)
    replay = DeterministicAnswerGenerator().generate_with_provenance("Q?", evidence)
    reordered = DeterministicAnswerGenerator().generate_with_provenance(
        "Q?", tuple(reversed(evidence))
    )

    assert first == replay
    assert first.provenance.normalized_input_hash != (
        reordered.provenance.normalized_input_hash
    )
    assert reordered.provenance.ordered_context_ids == ("chunk-2", "chunk-1")

    revised_model = replace(
        qwen_model_artifact(ModelTask.GENERATION),
        artifact_id="qwen-generation-revised",
        immutable_revision="b" * 40,
        tokenizer_revision="b" * 40,
    )
    backend = ScriptedCompletionBackend(
        (
            json.dumps(
                {
                    "answer_text": "First passage.",
                    "cited_chunk_version_ids": ["chunk-1"],
                }
            ),
        )
    )
    revised = StructuredCitedAnswerGenerator(
        backend=backend,
        model_artifact=revised_model,
        prompt_artifact=generation_prompt_artifact(),
        decoding_config=GENERATION_DECODING_CONFIG,
    ).generate_with_provenance("Q?", evidence)
    assert revised.provenance.normalized_input_hash != (
        first.provenance.normalized_input_hash
    )


def test_generation_rejects_duplicate_context_and_non_question_evidence() -> None:
    duplicate = _evidence("chunk-1", "Evidence.")
    with pytest.raises(ValidationError):
        DeterministicAnswerGenerator().generate("Question?", (duplicate, duplicate))
    with pytest.raises(ValidationError):
        DeterministicAnswerGenerator().generate(
            "Question?",
            (_evidence("chunk-1", "Evidence.", query_kind=QueryKind.CLAIM),),
        )


def test_claim_extraction_repairs_malformed_json_and_preserves_unicode() -> None:
    valid = _claim_output(text="Nimbus supports café names and 東京 regions.")
    backend = ScriptedCompletionBackend(("```json", valid))
    execution = _extractor(backend).extract_with_provenance(
        _answer("Nimbus supports café names and 東京 regions."),
        (_evidence("chunk-1", "Nimbus supports café names and 東京 regions."),),
    )

    assert execution.result.claims[0].text == (
        "Nimbus supports café names and 東京 regions."
    )
    assert execution.result.repair_count == 1
    assert execution.provenance.repair_count == 1
    assert len(backend.calls) == 2


@pytest.mark.parametrize(
    "first_output",
    [
        "",
        "I cannot extract claims.",
        '{"claims":[]}',
        _claim_output(citations=["extractor-added"]),
        _claim_output(citations=["chunk-1", "chunk-1"]),
        _claim_output(required=False),
        '{"claims":[{"local_claim_id":"claim-1","text":"A.",'
        '"required":true,"cited_chunk_version_ids":["chunk-1"]},'
        '{"local_claim_id":"claim-1","text":"B.",'
        '"required":true,"cited_chunk_version_ids":["chunk-1"]}]}',
        '{"claims":[{"local_claim_id":"claim-1","text":"Same claim.",'
        '"required":true,"cited_chunk_version_ids":["chunk-1"]},'
        '{"local_claim_id":"claim-2","text":"  Same   claim. ",'
        '"required":true,"cited_chunk_version_ids":["chunk-1"]}]}',
    ],
)
def test_claim_extraction_rejects_invalid_output_after_one_repair(
    first_output: str,
) -> None:
    backend = ScriptedCompletionBackend((first_output, "{}"))
    with pytest.raises(RepairExhaustedError):
        _extractor(backend).extract(
            _answer(),
            (_evidence("chunk-1", "Nimbus requires Python 3.12."),),
        )
    assert len(backend.calls) == 2


def test_extractor_cannot_see_extra_evidence_or_add_citations() -> None:
    backend = ScriptedCompletionBackend((_claim_output(),))
    execution = _extractor(backend).extract_with_provenance(
        _answer(),
        (
            _evidence("chunk-extra", "Uncited and hidden.", rank=2),
            _evidence("chunk-1", "Nimbus requires Python 3.12.", rank=1),
        ),
    )
    payload = backend.calls[0].payload
    passages = payload["resolved_passages"]
    assert isinstance(passages, list)
    assert [passage["chunk_version_id"] for passage in passages] == ["chunk-1"]
    assert execution.provenance.ordered_context_ids == ("chunk-1",)

    with pytest.raises(ValidationError):
        DeterministicClaimExtractor().extract(
            _answer(citations=("chunk-missing",)),
            (_evidence("chunk-1", "Evidence."),),
        )


def test_claim_extraction_replay_context_order_and_prompt_version_identity() -> None:
    evidence = (
        _evidence("chunk-1", "First.", rank=1),
        _evidence("chunk-2", "Second.", rank=2),
    )
    answer = _answer("First and second.", ("chunk-1", "chunk-2"))
    first = DeterministicClaimExtractor().extract_with_provenance(answer, evidence)
    replay = DeterministicClaimExtractor().extract_with_provenance(answer, evidence)
    reordered_answer = _answer("First and second.", ("chunk-2", "chunk-1"))
    reordered = DeterministicClaimExtractor().extract_with_provenance(
        reordered_answer, evidence
    )

    assert first == replay
    assert first.provenance.normalized_input_hash != (
        reordered.provenance.normalized_input_hash
    )

    original_prompt = claim_extraction_prompt_artifact()
    revised_template = original_prompt.template + "\nKeep versioned scope explicit."
    revised_prompt = replace(
        original_prompt,
        artifact_id="m3-claim-extraction-prompt-v2",
        version="m3-claim-extraction-v2",
        template=revised_template,
        template_hash=hashlib.sha256(revised_template.encode()).hexdigest(),
    )
    backend = ScriptedCompletionBackend((_claim_output(),))
    revised = StructuredClaimExtractor(
        backend=backend,
        model_artifact=qwen_model_artifact(ModelTask.CLAIM_EXTRACTION),
        prompt_artifact=revised_prompt,
        decoding_config=CLAIM_EXTRACTION_DECODING_CONFIG,
    ).extract_with_provenance(answer, evidence)
    assert revised.provenance.normalized_input_hash != (
        first.provenance.normalized_input_hash
    )


def test_qwen_adapter_is_pinned_and_lazy_without_downloading() -> None:
    backend = QwenCompletionBackend()
    assert backend.revision == QWEN_REVISION
    assert backend.allow_download is False
    assert backend._model is None
    with pytest.raises(ValidationError):
        QwenCompletionBackend(revision="main")
