from __future__ import annotations

from collections.abc import Sequence

import pytest

from groundloop.ai.chunking import FixedCharChunker
from groundloop.ai.contracts import (
    ChunkDraft,
    EmbeddingRecord,
    ModelArtifact,
    QueryKind,
)
from groundloop.ai.embeddings.common import EmbeddedQuery, EmbeddingInputAudit
from groundloop.ai.embeddings.fake import DeterministicFakeEmbedder
from groundloop.errors import ArtifactConflictError, ValidationError
from groundloop.m4.models import (
    CLAIM_ROLE_TEMPLATE,
    ClaimEmbeddingInput,
    EmbeddingAdapterSpec,
    EmbeddingRole,
    M4BgeRoleAdapter,
)


class _CountingEmbedder:
    def __init__(self) -> None:
        self.delegate = DeterministicFakeEmbedder()
        self.model_artifact = self.delegate.model_artifact
        self.dimension = self.delegate.dimension
        self.last_audit: tuple[EmbeddingInputAudit, ...] = ()
        self.query_calls: list[str] = []
        self.chunk_calls: list[tuple[str, ...]] = []

    def embed(self, chunks: tuple[ChunkDraft, ...]) -> tuple[EmbeddingRecord, ...]:
        self.chunk_calls.append(tuple(chunk.chunk_version_id for chunk in chunks))
        result = self.delegate.embed(chunks)
        self.last_audit = self.delegate.last_audit
        return result

    def embed_query(self, text: str, query_kind: QueryKind) -> EmbeddedQuery:
        self.query_calls.append(text)
        result = self.delegate.embed_query(text, query_kind)
        self.last_audit = self.delegate.last_audit
        return result


def _adapter(backend: _CountingEmbedder | None = None) -> M4BgeRoleAdapter:
    chosen = backend or _CountingEmbedder()
    return M4BgeRoleAdapter(
        chosen,
        EmbeddingAdapterSpec(chosen.model_artifact, chosen.dimension),
    )


def _chunks() -> tuple[ChunkDraft, ...]:
    chunker = FixedCharChunker()
    second = chunker.chunk("dv-2", "Second passage.")[0]
    first = chunker.chunk("dv-1", "First passage.")[0]
    return (
        ChunkDraft(
            "chunk-2",
            second.document_version_id,
            second.chunk_index,
            second.text,
            second.text_hash,
            second.chunker_artifact_id,
        ),
        ChunkDraft(
            "chunk-1",
            first.document_version_id,
            first.chunk_index,
            first.text,
            first.text_hash,
            first.chunker_artifact_id,
        ),
    )


def test_claim_and_passage_roles_are_distinct_and_canonically_ordered() -> None:
    backend = _CountingEmbedder()
    adapter = _adapter(backend)
    claims = adapter.embed_claims(
        (
            ClaimEmbeddingInput("claim-2", "same text"),
            ClaimEmbeddingInput("claim-1", "same text"),
        )
    )
    chunks = adapter.embed_chunks(_chunks())

    assert tuple(item.vector.claim_id for item in claims) == ("claim-1", "claim-2")
    assert tuple(item.vector.chunk_version_id for item in chunks) == (
        "chunk-1",
        "chunk-2",
    )
    assert claims[0].provenance.role is EmbeddingRole.CLAIM_QUERY
    assert chunks[0].provenance.role is EmbeddingRole.CHUNK_PASSAGE
    assert (
        claims[0].provenance.role_template_hash
        != chunks[0].provenance.role_template_hash
    )
    assert claims[0].vector.vector != adapter.embed_chunks(
        (
            ChunkDraft(
                "same-text-chunk",
                "dv",
                0,
                "same text",
                FixedCharChunker().chunk("dv", "same text")[0].text_hash,
                "fixed-char-v1-1200-no-overlap",
            ),
        )
    )[0].vector.vector
    assert (
        claims[0].provenance.model_revision
        == backend.model_artifact.immutable_revision
    )
    assert (
        claims[0].provenance.tokenizer_revision
        == backend.model_artifact.tokenizer_revision
    )
    assert CLAIM_ROLE_TEMPLATE.endswith("{text}")


def test_exact_embedding_artifacts_are_reused_without_backend_calls() -> None:
    backend = _CountingEmbedder()
    adapter = _adapter(backend)
    claims = (ClaimEmbeddingInput("claim-1", "Nimbus is grounded."),)
    chunks = (_chunks()[1],)
    first_claim = adapter.embed_claims(claims)
    first_chunk = adapter.embed_chunks(chunks)
    calls = (tuple(backend.query_calls), tuple(backend.chunk_calls))

    assert adapter.embed_claims(claims) == first_claim
    assert adapter.embed_chunks(chunks) == first_chunk
    assert (tuple(backend.query_calls), tuple(backend.chunk_calls)) == calls


def test_immutable_claim_and_chunk_identifiers_reject_text_drift() -> None:
    adapter = _adapter()
    adapter.embed_claims((ClaimEmbeddingInput("claim-1", "first"),))
    with pytest.raises(ArtifactConflictError, match="claim immutable input drift"):
        adapter.embed_claims((ClaimEmbeddingInput("claim-1", "changed"),))

    chunk = _chunks()[1]
    adapter.embed_chunks((chunk,))
    changed = FixedCharChunker().chunk("dv-1", "Changed passage.")[0]
    with pytest.raises(ArtifactConflictError, match="chunk immutable input drift"):
        adapter.embed_chunks(
            (
                ChunkDraft(
                    chunk.chunk_version_id,
                    changed.document_version_id,
                    changed.chunk_index,
                    changed.text,
                    changed.text_hash,
                    changed.chunker_artifact_id,
                ),
            )
        )


def test_backend_model_identity_drift_is_rejected_after_construction() -> None:
    backend = _CountingEmbedder()
    adapter = _adapter(backend)
    original = backend.model_artifact
    backend.model_artifact = ModelArtifact(
        artifact_id=original.artifact_id + "-drift",
        task=original.task,
        provider=original.provider,
        model_id=original.model_id,
        immutable_revision=original.immutable_revision,
        tokenizer_revision=original.tokenizer_revision,
        license_id=original.license_id,
        config_hash=original.config_hash,
    )
    with pytest.raises(ArtifactConflictError, match="model artifact drift"):
        adapter.embed_claims((ClaimEmbeddingInput("claim", "text"),))


def test_batches_reject_duplicate_subject_identifiers() -> None:
    adapter = _adapter()
    duplicate_claims: Sequence[ClaimEmbeddingInput] = (
        ClaimEmbeddingInput("claim", "one"),
        ClaimEmbeddingInput("claim", "one"),
    )
    with pytest.raises(ValidationError, match="duplicate claim identifiers"):
        adapter.embed_claims(duplicate_claims)
