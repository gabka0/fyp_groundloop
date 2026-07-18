"""Offline deterministic embedding double; never downloads model artifacts."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from groundloop.ai.contracts import (
    ChunkDraft,
    EmbeddingRecord,
    ModelArtifact,
    ModelTask,
    QueryKind,
    stable_digest,
)
from groundloop.ai.embeddings.common import (
    EMBEDDING_DIMENSION,
    EmbeddedQuery,
    EmbeddingInputAudit,
    input_sha256,
    normalized_vector,
)

FAKE_CONFIG_HASH = stable_digest("deterministic-fake-embedding-v1", "384")
FAKE_MODEL_ARTIFACT = ModelArtifact(
    artifact_id=stable_digest("model-artifact-v1", "deterministic-fake-embedding-v1"),
    task=ModelTask.EMBEDDING,
    provider="groundloop-test-double",
    model_id="deterministic-fake-embedding-v1",
    immutable_revision="v1",
    tokenizer_revision="v1",
    license_id="project-test-code",
    config_hash=FAKE_CONFIG_HASH,
)


def _fake_vector(text: str) -> tuple[float, ...]:
    values: list[float] = []
    counter = 0
    while len(values) < EMBEDDING_DIMENSION:
        digest = hashlib.sha256(
            b"groundloop-fake-embedding-v1\0"
            + counter.to_bytes(4, "big")
            + text.encode("utf-8")
        ).digest()
        for offset in range(0, len(digest), 2):
            integer = int.from_bytes(digest[offset : offset + 2], "big")
            values.append((integer / 32_767.5) - 1.0)
            if len(values) == EMBEDDING_DIMENSION:
                break
        counter += 1
    return normalized_vector(tuple(values))


@dataclass(slots=True)
class DeterministicFakeEmbedder:
    """Stable hash-derived passage/query vectors for ordinary tests."""

    model_artifact: ModelArtifact = FAKE_MODEL_ARTIFACT
    dimension: int = EMBEDDING_DIMENSION
    last_audit: tuple[EmbeddingInputAudit, ...] = field(default=(), init=False)

    def embed(self, chunks: tuple[ChunkDraft, ...]) -> tuple[EmbeddingRecord, ...]:
        audits: list[EmbeddingInputAudit] = []
        records: list[EmbeddingRecord] = []
        for chunk in chunks:
            audit = EmbeddingInputAudit(chunk.chunk_version_id, None, 512, False)
            audits.append(audit)
            records.append(
                EmbeddingRecord(
                    chunk_version_id=chunk.chunk_version_id,
                    model_artifact_id=self.model_artifact.artifact_id,
                    vector=_fake_vector(chunk.text),
                    input_hash=input_sha256(chunk.text),
                )
            )
        self.last_audit = tuple(audits)
        return tuple(records)

    def embed_query(self, text: str, query_kind: QueryKind) -> EmbeddedQuery:
        from groundloop.ai.embeddings.bge import BGE_QUERY_PREFIX

        model_input = f"{BGE_QUERY_PREFIX}{text}"
        audit = EmbeddingInputAudit(query_kind.value, None, 512, False)
        self.last_audit = (audit,)
        return EmbeddedQuery(
            _fake_vector(model_input), input_sha256(model_input), audit
        )
