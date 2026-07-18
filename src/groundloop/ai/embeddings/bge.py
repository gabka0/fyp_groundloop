"""Pinned local-only BGE-small embedding adapter."""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

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
    ArtifactUnavailableError,
    EmbeddedQuery,
    EmbeddingInputAudit,
    input_sha256,
    normalized_vector,
)

BGE_MODEL_ID = "BAAI/bge-small-en-v1.5"
BGE_REVISION = "5c38ec7c405ec4b44b94cc5a9bb96e735b38267a"
BGE_QUERY_PREFIX = "Represent this sentence for searching relevant passages: "
BGE_MAX_TOKENS = 512
BGE_CONFIG_HASH = stable_digest(
    "bge-embedding-v1",
    BGE_MODEL_ID,
    BGE_REVISION,
    BGE_QUERY_PREFIX,
    str(EMBEDDING_DIMENSION),
    str(BGE_MAX_TOKENS),
    "normalize_embeddings=true",
)
BGE_MODEL_ARTIFACT = ModelArtifact(
    artifact_id=stable_digest("model-artifact-v1", BGE_MODEL_ID, BGE_REVISION),
    task=ModelTask.EMBEDDING,
    provider="huggingface",
    model_id=BGE_MODEL_ID,
    immutable_revision=BGE_REVISION,
    tokenizer_revision=BGE_REVISION,
    license_id="MIT",
    config_hash=BGE_CONFIG_HASH,
)


@dataclass(slots=True)
class BgeSmallEmbedder:
    """Load only the frozen BGE revision already present on local disk."""

    cache_dir: Path | None = None
    allow_download: bool = False
    model_artifact: ModelArtifact = BGE_MODEL_ARTIFACT
    dimension: int = EMBEDDING_DIMENSION
    last_audit: tuple[EmbeddingInputAudit, ...] = field(default=(), init=False)
    _model: Any = field(default=None, init=False, repr=False)

    def _load(self) -> Any:
        if self._model is not None:
            return self._model
        try:
            sentence_transformers = importlib.import_module("sentence_transformers")
            sentence_transformer = sentence_transformers.SentenceTransformer
            self._model = sentence_transformer(
                BGE_MODEL_ID,
                revision=BGE_REVISION,
                cache_folder=str(self.cache_dir) if self.cache_dir else None,
                local_files_only=not self.allow_download,
                trust_remote_code=False,
                device="cpu",
            )
        except (ImportError, OSError, ValueError) as error:
            raise ArtifactUnavailableError(
                "Pinned BGE artifact is unavailable locally. Acquire "
                f"{BGE_MODEL_ID}@{BGE_REVISION} outside ordinary tests, then "
                "rerun this explicit smoke with its cache directory."
            ) from error
        return self._model

    def _encode(
        self, input_id: str, text: str
    ) -> tuple[tuple[float, ...], EmbeddingInputAudit]:
        model = self._load()
        token_ids = model.tokenizer.encode(
            text,
            add_special_tokens=True,
            truncation=False,
        )
        token_count = len(token_ids)
        encoded = model.encode(
            [text],
            batch_size=1,
            convert_to_numpy=True,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        vector = normalized_vector(tuple(float(value) for value in encoded[0]))
        return vector, EmbeddingInputAudit(
            input_id=input_id,
            token_count=token_count,
            max_tokens=BGE_MAX_TOKENS,
            truncated=token_count > BGE_MAX_TOKENS,
        )

    def embed(self, chunks: tuple[ChunkDraft, ...]) -> tuple[EmbeddingRecord, ...]:
        records: list[EmbeddingRecord] = []
        audits: list[EmbeddingInputAudit] = []
        for chunk in chunks:
            vector, audit = self._encode(chunk.chunk_version_id, chunk.text)
            audits.append(audit)
            records.append(
                EmbeddingRecord(
                    chunk_version_id=chunk.chunk_version_id,
                    model_artifact_id=self.model_artifact.artifact_id,
                    vector=vector,
                    input_hash=input_sha256(chunk.text),
                )
            )
        self.last_audit = tuple(audits)
        return tuple(records)

    def embed_query(self, text: str, query_kind: QueryKind) -> EmbeddedQuery:
        model_input = f"{BGE_QUERY_PREFIX}{text}"
        vector, audit = self._encode(query_kind.value, model_input)
        self.last_audit = (audit,)
        return EmbeddedQuery(vector, input_sha256(model_input), audit)
