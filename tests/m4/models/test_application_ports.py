from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path

import pytest

from groundloop.ai.contracts import (
    AtomicClaim,
    ChunkDraft,
    EmbeddingRecord,
    QueryKind,
)
from groundloop.ai.contracts import (
    VerificationResult as ModelVerificationResult,
)
from groundloop.ai.embeddings.common import EmbeddedQuery, EmbeddingInputAudit
from groundloop.ai.embeddings.fake import DeterministicFakeEmbedder
from groundloop.ai.verification.adapter import DeterministicFakeVerifier
from groundloop.domain import DecisionPolicy, SubjectKind, normalized_text_hash
from groundloop.errors import ArtifactConflictError, ValidationError
from groundloop.m4.application import VerificationPort
from groundloop.m4.contracts import (
    JobKind,
    LogicalJobSpec,
    PairKey,
    stable_m4_digest,
)
from groundloop.m4.models.config import (
    PinnedM3ReuseConfig,
    build_pinned_m3_adapters,
    inspect_local_artifacts,
)
from groundloop.m4.models.contracts import (
    EmbeddingAdapterSpec,
    PairVerificationInput,
    VerificationAdapterSpec,
)
from groundloop.m4.models.embedding import ClaimEmbeddingInput, M4BgeRoleAdapter
from groundloop.m4.models.ports import (
    M4AdmissionEmbeddingService,
    M4VerificationApplicationPort,
    PairVerificationInputResolver,
)
from groundloop.m4.models.verification import M4CalibratedVerifierAdapter


@dataclass(slots=True)
class _Resolver(PairVerificationInputResolver):
    values: dict[PairKey, PairVerificationInput]
    calls: list[tuple[int, PairKey]] = field(default_factory=list)

    def resolve_pair_input(
        self, *, epoch_id: int, pair: PairKey
    ) -> PairVerificationInput:
        self.calls.append((epoch_id, pair))
        return self.values[pair]


class _CountingEmbedder:
    def __init__(self) -> None:
        self._delegate = DeterministicFakeEmbedder()
        self.model_artifact = self._delegate.model_artifact
        self.dimension = self._delegate.dimension
        self.last_audit: tuple[EmbeddingInputAudit, ...] = ()
        self.claim_calls = 0
        self.chunk_pair_calls = 0

    def embed(self, chunks: tuple[ChunkDraft, ...]) -> tuple[EmbeddingRecord, ...]:
        self.chunk_pair_calls += len(chunks)
        result = self._delegate.embed(chunks)
        self.last_audit = self._delegate.last_audit
        return result

    def embed_query(self, text: str, query_kind: QueryKind) -> EmbeddedQuery:
        self.claim_calls += 1
        result = self._delegate.embed_query(text, query_kind)
        self.last_audit = self._delegate.last_audit
        return result


class _CountingVerifier:
    def __init__(self) -> None:
        self._delegate = DeterministicFakeVerifier(max_length=64)
        self.model_artifact = self._delegate.model_artifact
        self.prompt_artifact = self._delegate.prompt_artifact
        self.calibration_version = self._delegate.calibration_version
        self.temperature = self._delegate.temperature
        self.max_length = self._delegate.max_length
        self.pair_calls = 0

    def verify_batch(
        self, pairs: Sequence[tuple[AtomicClaim, ChunkDraft]]
    ) -> tuple[ModelVerificationResult, ...]:
        self.pair_calls += len(pairs)
        return self._delegate.verify_batch(pairs)


def _pair_input(
    pair: PairKey,
    *,
    claim_text: str = "GroundLoop maintains stored observations.",
    chunk_text: str = "GroundLoop incrementally maintains structured state.",
) -> PairVerificationInput:
    return PairVerificationInput(
        pair=pair,
        claim_text=claim_text,
        claim_required=True,
        claim_cited_chunk_version_ids=(),
        document_version_id="document-version-1",
        chunk_index=0,
        chunk_text=chunk_text,
        chunk_text_hash=normalized_text_hash(chunk_text),
        chunker_artifact_id="fixed-char-v1-1200-no-overlap",
    )


def _verification_adapter(
    backend: _CountingVerifier | None = None,
) -> M4CalibratedVerifierAdapter:
    selected = backend or _CountingVerifier()
    return M4CalibratedVerifierAdapter(
        selected,
        VerificationAdapterSpec(
            model_artifact=selected.model_artifact,
            prompt_artifact=selected.prompt_artifact,
            calibration_version=selected.calibration_version,
            calibration_artifact_sha256=None,
            temperature=selected.temperature,
            max_length=selected.max_length,
            decision_policy=DecisionPolicy("m4-port-test-policy", 0.8, 0.8),
        ),
        batch_size=8,
    )


def _verifier_job(
    pair: PairKey,
    execution_spec_hash: str,
    *,
    payload_hash: str | None = None,
) -> LogicalJobSpec:
    event_id = "event-1"
    candidate_policy_id = "candidate-policy-1"
    parent_job_id = "root-job-1"
    job_id = LogicalJobSpec.derive_job_id(
        event_id=event_id,
        kind=JobKind.VERIFY_PAIR,
        candidate_policy_id=candidate_policy_id,
        execution_spec_hash=execution_spec_hash,
        parent_job_id=parent_job_id,
        claim_id=pair.claim_id,
        chunk_version_id=pair.chunk_version_id,
    )
    return LogicalJobSpec(
        job_id=job_id,
        event_id=event_id,
        kind=JobKind.VERIFY_PAIR,
        candidate_policy_id=candidate_policy_id,
        payload_hash=payload_hash or stable_m4_digest("payload", pair.claim_id),
        execution_spec_hash=execution_spec_hash,
        parent_job_id=parent_job_id,
        pair=pair,
    )


def _chunk(chunk_id: str, text: str) -> ChunkDraft:
    return ChunkDraft(
        chunk_version_id=chunk_id,
        document_version_id=f"document-{chunk_id}",
        chunk_index=0,
        text=text,
        text_hash=normalized_text_hash(text),
        chunker_artifact_id="fixed-char-v1-1200-no-overlap",
    )


def test_verification_port_returns_content_addressed_application_result() -> None:
    pair = PairKey("claim-1", "chunk-1")
    adapter = _verification_adapter()
    resolver = _Resolver({pair: _pair_input(pair)})
    concrete = M4VerificationApplicationPort(resolver, adapter)
    port: VerificationPort = concrete

    result = port.verify(11, _verifier_job(pair, adapter.spec.execution_spec_hash))

    assert len(result.result_artifact_id) == 64
    assert len(result.result_artifact_hash) == 64
    assert result.result_artifact_hash != result.result_artifact_id
    assert result.observation.subject_kind is SubjectKind.CLAIM
    assert result.observation.subject_id == pair.claim_id
    assert result.observation.chunk_version_id == pair.chunk_version_id
    assert result.observation.producer.model_id == adapter.spec.model_artifact.model_id
    assert result.observation.producer.model_version == (
        adapter.spec.model_artifact.immutable_revision
    )
    assert result.observation.producer.prompt_version == (
        adapter.spec.prompt_artifact.version
    )
    exact = concrete.artifact_by_id(result.result_artifact_id)
    assert exact.artifact_id == result.result_artifact_id
    assert exact.pair == pair
    with pytest.raises(ValidationError, match="unknown verification artifact_id"):
        concrete.artifact_by_id(stable_m4_digest("absent-artifact"))


def test_verification_replay_resolves_again_but_reuses_exact_model_artifact() -> None:
    pair = PairKey("claim-1", "chunk-1")
    backend = _CountingVerifier()
    adapter = _verification_adapter(backend)
    resolver = _Resolver({pair: _pair_input(pair)})
    port = M4VerificationApplicationPort(resolver, adapter)
    job = _verifier_job(pair, adapter.spec.execution_spec_hash)

    first = port.verify(7, job)
    second = port.verify(7, job)

    assert first == second
    assert resolver.calls == [(7, pair), (7, pair)]
    assert backend.pair_calls == 1
    assert port.backend_pair_calls == 1
    assert port.request_count == 2
    assert port.resolver_call_count == 2
    assert port.successful_result_count == 2
    assert port.new_artifact_count == 1
    assert port.reused_artifact_count == 1


def test_resolver_input_and_logical_job_identity_drift_are_rejected() -> None:
    pair = PairKey("claim-1", "chunk-1")
    adapter = _verification_adapter()
    resolver = _Resolver({pair: _pair_input(pair)})
    port = M4VerificationApplicationPort(resolver, adapter)
    job = _verifier_job(pair, adapter.spec.execution_spec_hash)
    port.verify(3, job)

    resolver.values[pair] = _pair_input(pair, claim_text="Changed immutable claim.")
    with pytest.raises(ArtifactConflictError, match="immutable verifier input drift"):
        port.verify(3, job)

    resolver.values[pair] = _pair_input(pair)
    changed_payload = replace(job, payload_hash=stable_m4_digest("changed-payload"))
    with pytest.raises(ArtifactConflictError, match="logical job identity drift"):
        port.verify(3, changed_payload)


def test_verification_port_rejects_wrong_job_kind_spec_and_resolved_pair() -> None:
    pair = PairKey("claim-1", "chunk-1")
    other = PairKey("claim-2", "chunk-2")
    adapter = _verification_adapter()
    resolver = _Resolver({pair: _pair_input(other)})
    port = M4VerificationApplicationPort(resolver, adapter)

    wrong_hash = stable_m4_digest("wrong-execution-spec")
    with pytest.raises(ArtifactConflictError, match="execution identity drift"):
        port.verify(1, _verifier_job(pair, wrong_hash))

    impact_hash = stable_m4_digest("impact-execution")
    impact_job = LogicalJobSpec(
        job_id=LogicalJobSpec.derive_job_id(
            event_id="event-1",
            kind=JobKind.IMPACT_DISCOVERY,
            candidate_policy_id="policy",
            execution_spec_hash=impact_hash,
            chunk_version_id=pair.chunk_version_id,
        ),
        event_id="event-1",
        kind=JobKind.IMPACT_DISCOVERY,
        candidate_policy_id="policy",
        payload_hash=stable_m4_digest("impact-payload"),
        execution_spec_hash=impact_hash,
        target_chunk_version_id=pair.chunk_version_id,
        expandable=True,
    )
    with pytest.raises(ValidationError, match="VERIFY_PAIR only"):
        port.verify(1, impact_job)

    with pytest.raises(ArtifactConflictError, match="another PairKey"):
        port.verify(1, _verifier_job(pair, adapter.spec.execution_spec_hash))


def test_embedding_service_returns_sorted_role_vectors_and_replay_counts() -> None:
    backend = _CountingEmbedder()
    adapter = M4BgeRoleAdapter(
        backend, EmbeddingAdapterSpec(backend.model_artifact, backend.dimension)
    )
    service = M4AdmissionEmbeddingService(adapter)
    claims = (
        ClaimEmbeddingInput("claim-2", "Second claim."),
        ClaimEmbeddingInput("claim-1", "First claim."),
    )
    chunks = (
        _chunk("chunk-2", "Second passage."),
        _chunk("chunk-1", "First passage."),
    )

    first = service.embed_for_admission(claims=claims, chunks=chunks)
    second = service.embed_for_admission(claims=claims, chunks=chunks)

    assert first == second
    assert tuple(item.claim_id for item in first.claim_vectors) == (
        "claim-1",
        "claim-2",
    )
    assert tuple(item.chunk_version_id for item in first.chunk_vectors) == (
        "chunk-1",
        "chunk-2",
    )
    assert backend.claim_calls == 2
    assert backend.chunk_pair_calls == 2
    assert service.request_count == 2
    assert service.new_claim_artifact_count == 2
    assert service.new_chunk_artifact_count == 2
    assert service.reused_claim_artifact_count == 2
    assert service.reused_chunk_artifact_count == 2


def test_embedding_service_rejects_immutable_subject_drift() -> None:
    backend = _CountingEmbedder()
    service = M4AdmissionEmbeddingService(
        M4BgeRoleAdapter(
            backend, EmbeddingAdapterSpec(backend.model_artifact, backend.dimension)
        )
    )
    service.embed_for_admission(
        claims=(ClaimEmbeddingInput("claim", "Original claim."),),
        chunks=(_chunk("chunk", "Original passage."),),
    )

    with pytest.raises(ArtifactConflictError, match="claim immutable input drift"):
        service.embed_for_admission(
            claims=(ClaimEmbeddingInput("claim", "Changed claim."),)
        )
    with pytest.raises(ArtifactConflictError, match="chunk immutable input drift"):
        service.embed_for_admission(
            chunks=(_chunk("chunk", "Changed passage."),)
        )


def test_opt_in_pinned_local_artifact_application_port_smoke() -> None:
    if os.environ.get("GROUNDLOOP_RUN_M4_REAL_MODEL_SMOKE") != "1":
        pytest.skip("set GROUNDLOOP_RUN_M4_REAL_MODEL_SMOKE=1 for local model smoke")
    root = Path(
        os.environ.get("GROUNDLOOP_M3_ARTIFACT_ROOT", "/home/kassym/Desktop/groundloop")
    )
    config = PinnedM3ReuseConfig.load(Path("configs/m4/models/m3_reuse_v1.json"))
    availability = inspect_local_artifacts(config, artifact_root=root)
    if not availability.available:
        pytest.skip("; ".join(availability.problems))
    bundle = build_pinned_m3_adapters(config, artifact_root=root)
    pair = PairKey("m4-port-smoke-claim", "m4-port-smoke-chunk")
    pair_input = _pair_input(
        pair,
        claim_text="GroundLoop maintains grounding relative to model judgments.",
        chunk_text="GroundLoop stores immutable semantic observations.",
    )
    port = M4VerificationApplicationPort(
        _Resolver({pair: pair_input}), bundle.verifier
    )
    job = _verifier_job(pair, bundle.verifier.spec.execution_spec_hash)

    assert port.verify(1, job) == port.verify(1, job)
    assert port.backend_pair_calls == 1
