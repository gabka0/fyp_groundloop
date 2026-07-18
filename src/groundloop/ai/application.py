"""One-command M3 orchestration over injected deterministic or real models."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Protocol

from groundloop.ai.chunking import FixedCharChunker, prepare_directory
from groundloop.ai.contracts import (
    AnswerGenerator,
    ChunkDraft,
    ClaimExtractor,
    ComponentTiming,
    EmbeddingRecord,
    EvidencePassage,
    EvidenceVerifier,
    ModelArtifact,
    PipelineRunManifest,
    PipelineRunStatus,
    PromptArtifact,
    QueryKind,
    RetrievalCandidate,
    stable_digest,
)
from groundloop.ai.embeddings.common import EmbeddedQuery
from groundloop.ai.persistence import (
    ChunkerArtifactRecord,
    M3PublicationBundle,
)
from groundloop.ai.pipeline import (
    CorpusDocument,
    build_structured_grounding,
    claim_version_id,
)
from groundloop.ai.retrieval import (
    EmbeddingStore,
    InMemoryCosineStore,
    StaticCosineRetriever,
    claim_query,
    question_query,
)
from groundloop.domain import DecisionPolicy, Question
from groundloop.errors import ArtifactConflictError, ValidationError


class StaticEmbeddingProvider(Protocol):
    model_artifact: ModelArtifact
    dimension: int

    def embed(
        self, chunks: tuple[ChunkDraft, ...]
    ) -> tuple[EmbeddingRecord, ...]: ...

    def embed_query(self, text: str, query_kind: QueryKind) -> EmbeddedQuery: ...


class PublicationStore(Protocol):
    def lookup(self, run_id: str) -> PipelineRunManifest | None: ...

    def stage(self, manifest: PipelineRunManifest) -> None: ...

    def prepare_retrieval_store(
        self, embeddings: tuple[EmbeddingRecord, ...]
    ) -> EmbeddingStore: ...

    def fail(self, manifest: PipelineRunManifest) -> None: ...

    def publish_bundle(self, bundle: M3PublicationBundle) -> PipelineRunManifest: ...

    def replay_manifest(self, manifest: PipelineRunManifest) -> PipelineRunManifest: ...


@dataclass(frozen=True, slots=True)
class M3ApplicationConfig:
    config_hash: str
    corpus_namespace: str = "groundloop-local-v1"
    question_top_k: int = 6
    claim_top_k: int = 4
    policy: DecisionPolicy = field(
        default_factory=lambda: DecisionPolicy("m3-policy-v1", 0.8, 0.8)
    )
    cold_start: bool = False

    def __post_init__(self) -> None:
        if len(self.config_hash) != 64:
            raise ValidationError("application config_hash must be SHA-256")
        if not self.corpus_namespace.strip():
            raise ValidationError("corpus namespace must be nonempty")
        if self.question_top_k <= 0 or self.claim_top_k <= 0:
            raise ValidationError("retrieval depths must be positive")


@dataclass(slots=True)
class M3Application:
    store: PublicationStore
    chunker: FixedCharChunker
    embedder: StaticEmbeddingProvider
    generator: AnswerGenerator
    extractor: ClaimExtractor
    verifier: EvidenceVerifier
    config: M3ApplicationConfig

    def register(self, corpus: Path, question_text: str) -> PipelineRunManifest:
        timings: list[ComponentTiming] = []
        question_text = " ".join(question_text.split())
        if not question_text:
            raise ValidationError("question must be nonempty")

        started = time.perf_counter()
        prepared = prepare_directory(
            corpus,
            corpus_namespace=self.config.corpus_namespace,
            chunker=self.chunker,
        )
        documents = tuple(
            CorpusDocument(
                document_id=item.identity.document_id,
                document_version_id=item.identity.document_version_id,
                source_uri=(corpus / item.identity.relative_path).resolve().as_uri(),
                content_hash=item.identity.raw_content_hash,
                chunks=item.chunks,
            )
            for item in prepared
        )
        chunks = tuple(chunk for document in documents for chunk in document.chunks)
        if not chunks:
            raise ValidationError("corpus contains no nonempty UTF-8 evidence chunks")
        timings.append(self._timing("chunking", started))

        corpus_hash = stable_digest(
            "m3-corpus-v1",
            *(
                value
                for item in prepared
                for value in (
                    item.identity.document_id,
                    item.identity.document_version_id,
                    item.identity.raw_content_hash,
                )
            ),
        )
        question_id = "question-" + stable_digest("m3-question-v1", question_text)
        model_artifacts = self._model_artifacts()
        prompt_artifacts = self._prompt_artifacts()
        input_hash = stable_digest(
            "m3-run-input-v1",
            corpus_hash,
            question_id,
            self.config.config_hash,
            self.chunker.artifact_id,
            self.config.policy.policy_version,
            *(item.artifact_id for item in model_artifacts),
            *(item.artifact_id for item in prompt_artifacts),
        )
        run_id = "run-" + input_hash
        staged = PipelineRunManifest(
            schema_version="m3-v1",
            run_id=run_id,
            status=PipelineRunStatus.STAGED,
            config_hash=self.config.config_hash,
            input_hash=input_hash,
            corpus_hash=corpus_hash,
            question_id=question_id,
            decision_policy_version=self.config.policy.policy_version,
            answer_version_id=None,
            semantic_epoch_id=None,
            confirmed_as_of_epoch=None,
            model_artifact_ids=tuple(item.artifact_id for item in model_artifacts),
            prompt_artifact_ids=tuple(item.artifact_id for item in prompt_artifacts),
            chunk_version_ids=tuple(item.chunk_version_id for item in chunks),
            chunk_text_hashes=tuple(
                (item.chunk_version_id, item.text_hash) for item in chunks
            ),
            retrieval_candidates=(),
            answer=None,
            extraction=None,
            claims=(),
            verifications=(),
            claim_states=(),
            answer_states=(),
            timings=tuple(timings),
            reused_artifact_ids=(),
            new_artifact_ids=(),
        )
        existing = self.store.lookup(run_id)
        if existing is not None:
            if existing.status is PipelineRunStatus.PUBLISHED:
                return self.store.replay_manifest(existing)
            raise ArtifactConflictError(
                f"identical run {run_id} is terminal/staged as {existing.status.value}"
            )
        self.store.stage(staged)

        try:
            return self._execute_and_publish(
                staged=staged,
                documents=documents,
                question=Question(question_id, question_text),
                model_artifacts=model_artifacts,
                prompt_artifacts=prompt_artifacts,
                timings=timings,
            )
        except Exception as error:
            failed = replace(
                staged,
                status=PipelineRunStatus.FAILED,
                timings=tuple(timings),
                failure_code=type(error).__name__,
            )
            self.store.fail(failed)
            raise

    def _execute_and_publish(
        self,
        *,
        staged: PipelineRunManifest,
        documents: tuple[CorpusDocument, ...],
        question: Question,
        model_artifacts: tuple[ModelArtifact, ...],
        prompt_artifacts: tuple[PromptArtifact, ...],
        timings: list[ComponentTiming],
    ) -> PipelineRunManifest:
        typed_chunks = tuple(
            document_chunk
            for document in documents
            for document_chunk in document.chunks
        )
        started = time.perf_counter()
        embeddings = self.embedder.embed(typed_chunks)
        timings.append(self._timing("embedding", started))
        embedding_store = self.store.prepare_retrieval_store(embeddings)
        retriever = StaticCosineRetriever(self.embedder, embedding_store)
        chunk_by_id = {item.chunk_version_id: item for item in typed_chunks}

        started = time.perf_counter()
        raw_question_candidates = retriever.retrieve(
            question_query(
                query_id=question.question_id,
                text=question.text,
                embedding_model_artifact_id=self.embedder.model_artifact.artifact_id,
                top_k=self.config.question_top_k,
            )
        )
        if not raw_question_candidates:
            raise ValidationError("question retrieval produced no evidence")
        question_candidates = tuple(
            self._run_candidate(staged.run_id, item, item.query_id)
            for item in raw_question_candidates
        )
        question_evidence = tuple(
            EvidencePassage(raw, chunk_by_id[raw.chunk_version_id])
            for raw in raw_question_candidates
        )
        timings.append(self._timing("question_retrieval", started))

        started = time.perf_counter()
        answer = self.generator.generate(question.text, question_evidence)
        timings.append(self._timing("generation", started))
        started = time.perf_counter()
        extraction = self.extractor.extract(answer, question_evidence)
        timings.append(self._timing("claim_extraction", started))
        answer_version_id = "answer-" + stable_digest(
            "m3-answer-v1",
            question.question_id,
            self.generator.model_artifact.artifact_id,
            self.generator.prompt_artifact.artifact_id,
            answer.input_hash,
            answer.raw_output_hash,
        )

        claim_candidates: list[RetrievalCandidate] = []
        verifications = []
        retrieval_elapsed = 0.0
        verification_elapsed = 0.0
        for atomic in extraction.claims:
            global_claim_id = claim_version_id(answer_version_id, atomic)
            started = time.perf_counter()
            raw_candidates = retriever.retrieve(
                claim_query(
                    query_id=global_claim_id,
                    text=atomic.text,
                    embedding_model_artifact_id=(
                        self.embedder.model_artifact.artifact_id
                    ),
                    top_k=self.config.claim_top_k,
                )
            )
            retrieval_elapsed += time.perf_counter() - started
            if not raw_candidates:
                raise ValidationError(
                    f"claim {atomic.local_claim_id} retrieval produced no evidence"
                )
            published_candidates = tuple(
                self._run_candidate(staged.run_id, item, global_claim_id)
                for item in raw_candidates
            )
            claim_candidates.extend(published_candidates)
            for raw, published in zip(
                raw_candidates, published_candidates, strict=True
            ):
                started = time.perf_counter()
                result = self.verifier.verify(
                    atomic,
                    chunk_by_id[raw.chunk_version_id],
                )
                verification_elapsed += time.perf_counter() - started
                verifications.append(
                    replace(
                        result,
                        claim_id=atomic.local_claim_id,
                        candidate_id=published.candidate_id,
                        model_artifact_id=self.verifier.model_artifact.artifact_id,
                        prompt_artifact_id=self.verifier.prompt_artifact.artifact_id,
                    )
                )
        timings.append(
            ComponentTiming(
                "claim_retrieval",
                retrieval_elapsed * 1_000.0,
                self.config.cold_start,
            )
        )
        timings.append(
            ComponentTiming(
                "verification",
                verification_elapsed * 1_000.0,
                self.config.cold_start,
            )
        )

        started = time.perf_counter()
        verifier_models = {
            self.verifier.model_artifact.artifact_id: self.verifier.model_artifact
        }
        verifier_prompts = {
            self.verifier.prompt_artifact.artifact_id: self.verifier.prompt_artifact
        }
        structured = build_structured_grounding(
            question=question,
            answer_version_id=answer_version_id,
            answer_text=answer.text,
            atomic_claims=extraction.claims,
            documents=documents,
            verifications=tuple(verifications),
            generation_model=self.generator.model_artifact,
            generation_prompt=self.generator.prompt_artifact,
            extraction_model=self.extractor.model_artifact,
            extraction_prompt=self.extractor.prompt_artifact,
            verifier_models=verifier_models,
            verifier_prompts=verifier_prompts,
            policy=self.config.policy,
        )
        timings.append(self._timing("ivm_and_full_recomputation", started))
        complete = replace(
            staged,
            answer_version_id=answer_version_id,
            retrieval_candidates=question_candidates + tuple(claim_candidates),
            answer=answer,
            extraction=extraction,
            claims=extraction.claims,
            verifications=tuple(verifications),
            claim_states=structured.claim_states,
            answer_states=structured.answer_states,
            timings=tuple(timings),
        )
        bundle = M3PublicationBundle(
            manifest=complete,
            model_artifacts=model_artifacts,
            prompt_artifacts=prompt_artifacts,
            chunker_artifact=ChunkerArtifactRecord(
                artifact_id=self.chunker.artifact_id,
                version="fixed-char-v1",
                normalization_version="v1",
                config_hash=stable_digest(
                    "fixed-char-v1",
                    self.chunker.artifact_id,
                    str(self.chunker.max_characters),
                    "no-overlap",
                ),
            ),
            documents=documents,
            embeddings=embeddings,
            policy=self.config.policy,
            structured=structured,
            generation_model_artifact_id=self.generator.model_artifact.artifact_id,
            generation_prompt_artifact_id=self.generator.prompt_artifact.artifact_id,
            extraction_model_artifact_id=self.extractor.model_artifact.artifact_id,
            extraction_prompt_artifact_id=self.extractor.prompt_artifact.artifact_id,
        )
        return self.store.publish_bundle(bundle)

    def _model_artifacts(self) -> tuple[ModelArtifact, ...]:
        values = (
            self.embedder.model_artifact,
            self.generator.model_artifact,
            self.extractor.model_artifact,
            self.verifier.model_artifact,
        )
        by_id = {item.artifact_id: item for item in values}
        if len(by_id) != len(values):
            raise ValidationError("model artifact identifiers collide across tasks")
        return values

    def _prompt_artifacts(self) -> tuple[PromptArtifact, ...]:
        values = (
            self.generator.prompt_artifact,
            self.extractor.prompt_artifact,
            self.verifier.prompt_artifact,
        )
        by_id = {item.artifact_id: item for item in values}
        if len(by_id) != len(values):
            raise ValidationError("prompt artifact identifiers collide across tasks")
        return values

    def _timing(self, component: str, started: float) -> ComponentTiming:
        elapsed = (time.perf_counter() - started) * 1_000.0
        if not math.isfinite(elapsed):
            raise AssertionError("perf_counter produced a non-finite duration")
        return ComponentTiming(component, elapsed, self.config.cold_start)

    @staticmethod
    def _run_candidate(
        run_id: str,
        candidate: RetrievalCandidate,
        query_id: str,
    ) -> RetrievalCandidate:
        return replace(
            candidate,
            candidate_id=stable_digest(
                "m3-run-candidate-v1",
                run_id,
                candidate.candidate_id,
                query_id,
            ),
            query_id=query_id,
        )


@dataclass(slots=True)
class InMemoryPublicationStore:
    """Atomic coordinator double; it never substitutes for PostgreSQL gates."""

    manifests: dict[str, PipelineRunManifest] = field(default_factory=dict)
    bundles: dict[str, M3PublicationBundle] = field(default_factory=dict)
    next_epoch: int = 1

    def lookup(self, run_id: str) -> PipelineRunManifest | None:
        return self.manifests.get(run_id)

    def stage(self, manifest: PipelineRunManifest) -> None:
        existing = self.manifests.get(manifest.run_id)
        if existing is not None and existing != manifest:
            raise ArtifactConflictError("in-memory run payload conflict")
        self.manifests[manifest.run_id] = manifest

    def prepare_retrieval_store(
        self, embeddings: tuple[EmbeddingRecord, ...]
    ) -> EmbeddingStore:
        store = InMemoryCosineStore()
        store.add(embeddings)
        return store

    def fail(self, manifest: PipelineRunManifest) -> None:
        existing = self.manifests.get(manifest.run_id)
        if existing is None or existing.status is not PipelineRunStatus.STAGED:
            raise ArtifactConflictError("run is not staged")
        self.manifests[manifest.run_id] = manifest

    def publish_bundle(self, bundle: M3PublicationBundle) -> PipelineRunManifest:
        existing = self.manifests.get(bundle.manifest.run_id)
        if existing is None or existing.status is not PipelineRunStatus.STAGED:
            raise ArtifactConflictError("run is not staged")
        epoch = self.next_epoch
        self.next_epoch += 1
        artifact_ids = (
            bundle.manifest.model_artifact_ids
            + bundle.manifest.prompt_artifact_ids
            + bundle.manifest.chunk_version_ids
            + tuple(
                item.observation_id for item in bundle.structured.observations
            )
        )
        published = replace(
            bundle.manifest,
            status=PipelineRunStatus.PUBLISHED,
            semantic_epoch_id=epoch,
            confirmed_as_of_epoch=epoch,
            new_artifact_ids=tuple(sorted(set(artifact_ids))),
        )
        self.manifests[published.run_id] = published
        self.bundles[published.run_id] = bundle
        return published

    def replay_manifest(self, manifest: PipelineRunManifest) -> PipelineRunManifest:
        return replace(
            manifest,
            reused_artifact_ids=tuple(
                sorted(set(manifest.new_artifact_ids + manifest.reused_artifact_ids))
            ),
            new_artifact_ids=(),
        )
