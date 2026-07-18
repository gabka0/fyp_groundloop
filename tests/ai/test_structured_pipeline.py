"""Exact-boundary tests for M3 AI products entering M2."""

from groundloop.ai.contracts import (
    AtomicClaim,
    ChunkDraft,
    ModelArtifact,
    ModelTask,
    PromptArtifact,
    ScoreTriple,
    VerificationResult,
)
from groundloop.ai.pipeline import CorpusDocument, build_structured_grounding
from groundloop.domain import (
    AnswerStatus,
    ClaimStatus,
    DecisionPolicy,
    Question,
    normalized_text_hash,
)

HASH = "a" * 64


def _model(artifact_id: str, task: ModelTask) -> ModelArtifact:
    return ModelArtifact(
        artifact_id=artifact_id,
        task=task,
        provider="test",
        model_id=artifact_id,
        immutable_revision="revision",
        tokenizer_revision="revision",
        license_id="test",
        config_hash=HASH,
    )


def _prompt(artifact_id: str, task: ModelTask) -> PromptArtifact:
    import hashlib

    template = f"{artifact_id} template"
    return PromptArtifact(
        artifact_id=artifact_id,
        task=task,
        version="v1",
        template=template,
        template_hash=hashlib.sha256(template.encode()).hexdigest(),
        decoding_config_hash=HASH,
    )


def test_ai_scores_enter_unchanged_reference_and_incremental_engines() -> None:
    generation_model = _model("generator", ModelTask.GENERATION)
    generation_prompt = _prompt("generation-prompt", ModelTask.GENERATION)
    extraction_model = _model("extractor", ModelTask.CLAIM_EXTRACTION)
    extraction_prompt = _prompt("extraction-prompt", ModelTask.CLAIM_EXTRACTION)
    verifier_model = _model("verifier", ModelTask.VERIFICATION)
    verifier_prompt = _prompt("verification-prompt", ModelTask.VERIFICATION)
    chunk = ChunkDraft(
        chunk_version_id="chunk",
        document_version_id="document-version",
        chunk_index=0,
        text="GroundLoop maintains structured state incrementally.",
        text_hash=normalized_text_hash(
            "GroundLoop maintains structured state incrementally."
        ),
        chunker_artifact_id="chunker",
    )
    claim = AtomicClaim(
        local_claim_id="local-claim",
        text="GroundLoop maintains structured state incrementally.",
        required=True,
        cited_chunk_version_ids=("chunk",),
    )
    result = build_structured_grounding(
        question=Question("question", "What does GroundLoop maintain?"),
        answer_version_id="answer",
        answer_text=claim.text,
        atomic_claims=(claim,),
        documents=(
            CorpusDocument(
                document_id="document",
                document_version_id="document-version",
                source_uri="local://document.txt",
                content_hash=HASH,
                chunks=(chunk,),
            ),
        ),
        verifications=(
            VerificationResult(
                claim_id="local-claim",
                chunk_version_id="chunk",
                candidate_id="candidate",
                model_artifact_id="verifier",
                prompt_artifact_id="verification-prompt",
                calibration_version="temperature-v1",
                temperature=1.2,
                scores=ScoreTriple(0.9, 0.05, 0.05),
                input_hash=HASH,
                raw_output_hash=HASH,
                raw_logits=(0.1, 3.0, 0.1),
            ),
        ),
        generation_model=generation_model,
        generation_prompt=generation_prompt,
        extraction_model=extraction_model,
        extraction_prompt=extraction_prompt,
        verifier_models={"verifier": verifier_model},
        verifier_prompts={"verification-prompt": verifier_prompt},
        policy=DecisionPolicy("policy", 0.8, 0.8),
    )
    assert result.claim_states[0].status is ClaimStatus.SUPPORTED
    assert result.answer_states[0].status is AnswerStatus.VALID
    assert result.runner.engine.claim_states == {
        result.claim_states[0].claim_id: result.claim_states[0]
    }
