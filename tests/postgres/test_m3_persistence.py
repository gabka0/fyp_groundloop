"""Live atomic-publication tests for the coordinator-owned M3 store."""

from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest
from psycopg import Connection, errors

from groundloop.ai.contracts import (
    AtomicClaim,
    ChunkDraft,
    CitedAnswer,
    ClaimExtractionResult,
    ComponentTiming,
    EmbeddingRecord,
    ModelArtifact,
    ModelTask,
    PipelineRunManifest,
    PipelineRunStatus,
    PromptArtifact,
    QueryKind,
    RetrievalCandidate,
    ScoreTriple,
    VerificationResult,
)
from groundloop.ai.persistence import (
    ChunkerArtifactRecord,
    M3PublicationBundle,
    PostgresArtifactStore,
)
from groundloop.ai.pipeline import CorpusDocument, build_structured_grounding
from groundloop.domain import DecisionPolicy, Question, normalized_text_hash
from groundloop.errors import ArtifactConflictError
from groundloop.postgres import temporary_m2_schema

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
    template = f"{artifact_id} template"
    return PromptArtifact(
        artifact_id=artifact_id,
        task=task,
        version="v1",
        template=template,
        template_hash=hashlib.sha256(template.encode()).hexdigest(),
        decoding_config_hash=HASH,
    )


def _bundle(
    *,
    run_id: str = "run",
    verifier_id: str = "verifier",
    candidate_id: str | None = None,
) -> M3PublicationBundle:
    generation_model = _model("generator", ModelTask.GENERATION)
    extraction_model = _model("extractor", ModelTask.CLAIM_EXTRACTION)
    embedding_model = _model("embedder", ModelTask.EMBEDDING)
    verifier_model = _model(verifier_id, ModelTask.VERIFICATION)
    generation_prompt = _prompt("generation-prompt", ModelTask.GENERATION)
    extraction_prompt = _prompt("extraction-prompt", ModelTask.CLAIM_EXTRACTION)
    verifier_prompt = _prompt("verification-prompt", ModelTask.VERIFICATION)
    text = "GroundLoop maintains structured state incrementally."
    text_hash = normalized_text_hash(text)
    chunk = ChunkDraft(
        "chunk",
        "document-version",
        0,
        text,
        text_hash,
        "chunker",
    )
    document = CorpusDocument(
        "document",
        "document-version",
        "local://document.txt",
        HASH,
        (chunk,),
    )
    atomic = AtomicClaim("local", text, True, ("chunk",))
    claim_candidate_id = candidate_id or f"{run_id}-claim-candidate"
    verification = VerificationResult(
        claim_id="local",
        chunk_version_id="chunk",
        candidate_id=claim_candidate_id,
        model_artifact_id=verifier_id,
        prompt_artifact_id="verification-prompt",
        calibration_version="temperature-v1",
        temperature=1.1,
        scores=ScoreTriple(0.9, 0.05, 0.05),
        input_hash=HASH,
        raw_output_hash=HASH,
        raw_logits=(0.0, 3.0, 0.0),
    )
    policy = DecisionPolicy("policy", 0.8, 0.8)
    structured = build_structured_grounding(
        question=Question("question", "What does GroundLoop maintain?"),
        answer_version_id="answer",
        answer_text=text,
        atomic_claims=(atomic,),
        documents=(document,),
        verifications=(verification,),
        generation_model=generation_model,
        generation_prompt=generation_prompt,
        extraction_model=extraction_model,
        extraction_prompt=extraction_prompt,
        verifier_models={verifier_id: verifier_model},
        verifier_prompts={"verification-prompt": verifier_prompt},
        policy=policy,
    )
    global_claim_id = structured.claims[0].claim_id
    candidates = (
        RetrievalCandidate(
            f"{run_id}-question-candidate",
            QueryKind.QUESTION,
            "question",
            "chunk",
            0.9,
            1,
            "embedder",
            "bge-cosine-v1",
        ),
        RetrievalCandidate(
            claim_candidate_id,
            QueryKind.CLAIM,
            global_claim_id,
            "chunk",
            0.9,
            1,
            "embedder",
            "bge-cosine-v1",
        ),
    )
    answer = CitedAnswer(text, ("chunk",), HASH, HASH)
    extraction = ClaimExtractionResult((atomic,), HASH, HASH)
    manifest = PipelineRunManifest(
        schema_version="m3-v1",
        run_id=run_id,
        status=PipelineRunStatus.STAGED,
        config_hash=HASH,
        input_hash=hashlib.sha256(run_id.encode()).hexdigest(),
        corpus_hash=HASH,
        question_id="question",
        decision_policy_version="policy",
        answer_version_id="answer",
        semantic_epoch_id=None,
        confirmed_as_of_epoch=None,
        model_artifact_ids=(
            "embedder",
            "generator",
            "extractor",
            verifier_id,
        ),
        prompt_artifact_ids=(
            "generation-prompt",
            "extraction-prompt",
            "verification-prompt",
        ),
        chunk_version_ids=("chunk",),
        chunk_text_hashes=(("chunk", text_hash),),
        retrieval_candidates=candidates,
        answer=answer,
        extraction=extraction,
        claims=(atomic,),
        verifications=(verification,),
        claim_states=structured.claim_states,
        answer_states=structured.answer_states,
        timings=(ComponentTiming("verification", 1.0, False),),
        reused_artifact_ids=(),
        new_artifact_ids=(),
    )
    return M3PublicationBundle(
        manifest=manifest,
        model_artifacts=(
            embedding_model,
            generation_model,
            extraction_model,
            verifier_model,
        ),
        prompt_artifacts=(
            generation_prompt,
            extraction_prompt,
            verifier_prompt,
        ),
        chunker_artifact=ChunkerArtifactRecord("chunker", "fixed-char-v1", "v1", HASH),
        documents=(document,),
        embeddings=(EmbeddingRecord("chunk", "embedder", (1.0,) + (0.0,) * 383, HASH),),
        policy=policy,
        structured=structured,
        generation_model_artifact_id="generator",
        generation_prompt_artifact_id="generation-prompt",
        extraction_model_artifact_id="extractor",
        extraction_prompt_artifact_id="extraction-prompt",
    )


def test_publish_is_atomic_and_replay_reports_reuse(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    with temporary_m2_schema(live_connection):
        store = PostgresArtifactStore(live_connection)
        bundle = _bundle()
        store.stage(bundle.manifest)
        published = store.publish_bundle(bundle)
        assert published.status is PipelineRunStatus.PUBLISHED
        assert published.semantic_epoch_id is not None
        assert published.new_artifact_ids
        assert store.lookup("run") == published
        replay = store.replay_manifest(published)
        assert not replay.new_artifact_ids
        assert replay.reused_artifact_ids
        assert live_connection.execute(
            "SELECT count(*) FROM groundloop_observation_currency"
        ).fetchone() == (1,)


def test_verifier_change_appends_and_supersedes_without_overwrite(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    with temporary_m2_schema(live_connection):
        store = PostgresArtifactStore(live_connection)
        first = _bundle(run_id="first", verifier_id="verifier-v1")
        store.stage(first.manifest)
        store.publish_bundle(first)
        second = _bundle(run_id="second", verifier_id="verifier-v2")
        store.stage(second.manifest)
        store.publish_bundle(second)
        assert live_connection.execute(
            "SELECT count(*) FROM groundloop_semantic_observation"
        ).fetchone() == (2,)
        assert live_connection.execute(
            "SELECT count(*) FROM groundloop_observation_currency"
        ).fetchone() == (1,)
        current_model = live_connection.execute(
            """
            SELECT o.model_id FROM groundloop_semantic_observation o
            JOIN groundloop_observation_currency c USING (observation_id)
            """
        ).fetchone()
        assert current_model == ("verifier-v2",)


def test_failed_publication_leaves_only_staged_audit_row(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    with temporary_m2_schema(live_connection):
        store = PostgresArtifactStore(live_connection)
        bundle = _bundle(candidate_id="missing-candidate")
        broken_manifest = replace(bundle.manifest, retrieval_candidates=())
        broken = replace(bundle, manifest=broken_manifest)
        store.stage(broken.manifest)
        with pytest.raises(errors.ForeignKeyViolation):
            store.publish_bundle(broken)
        assert live_connection.execute(
            "SELECT status FROM groundloop_pipeline_run"
        ).fetchone() == ("staged",)
        assert live_connection.execute(
            "SELECT count(*) FROM groundloop_answer_version"
        ).fetchone() == (0,)


def test_same_run_id_with_different_identity_is_rejected(
    live_connection: Connection[tuple[object, ...]],
) -> None:
    with temporary_m2_schema(live_connection):
        store = PostgresArtifactStore(live_connection)
        bundle = _bundle()
        store.stage(bundle.manifest)
        with pytest.raises(ArtifactConflictError):
            store.stage(replace(bundle.manifest, input_hash="f" * 64))
