from __future__ import annotations

import math
from typing import NoReturn

import pytest

from groundloop.ai.chunking import FixedCharChunker
from groundloop.ai.contracts import QueryKind
from groundloop.ai.embeddings import (
    BGE_QUERY_PREFIX,
    BGE_REVISION,
    BgeSmallEmbedder,
    DeterministicFakeEmbedder,
)
from groundloop.ai.embeddings.bge import BGE_MODEL_ARTIFACT
from groundloop.ai.embeddings.common import ArtifactUnavailableError, normalized_vector
from groundloop.errors import ValidationError


def test_fake_embeddings_are_deterministic_finite_384d_and_unit_length() -> None:
    chunks = FixedCharChunker().chunk("dv", "A passage about Nimbus.")
    first = DeterministicFakeEmbedder().embed(chunks)
    second = DeterministicFakeEmbedder().embed(chunks)
    vector = first[0].vector
    assert first == second
    assert len(vector) == 384
    assert all(math.isfinite(value) for value in vector)
    assert math.isclose(sum(value * value for value in vector), 1.0, abs_tol=1e-12)


def test_queries_receive_exact_prefix_while_passages_do_not() -> None:
    embedder = DeterministicFakeEmbedder()
    text = "Which Python versions does Nimbus support?"
    passage = FixedCharChunker().chunk("dv", text)
    passage_vector = embedder.embed(passage)[0].vector
    question = embedder.embed_query(text, QueryKind.QUESTION)
    claim = embedder.embed_query(text, QueryKind.CLAIM)
    assert question.vector == claim.vector
    assert question.vector != passage_vector


@pytest.mark.parametrize(
    "vector",
    [
        (1.0,) * 383,
        (0.0,) * 384,
        (float("nan"),) + (0.0,) * 383,
        (float("inf"),) + (0.0,) * 383,
    ],
)
def test_embedding_dimension_and_finite_norm_failures(
    vector: tuple[float, ...],
) -> None:
    with pytest.raises(ValidationError):
        normalized_vector(vector)


class _Tokenizer:
    def __init__(self) -> None:
        self.inputs: list[str] = []

    def encode(self, text: str, **_: object) -> list[int]:
        self.inputs.append(text)
        return list(range(513 if text.startswith(BGE_QUERY_PREFIX) else 5))


class _LocalModel:
    def __init__(self) -> None:
        self.tokenizer = _Tokenizer()
        self.inputs: list[str] = []

    def encode(self, texts: list[str], **_: object) -> list[list[float]]:
        self.inputs.extend(texts)
        return [[1.0] + [0.0] * 383]


def test_pinned_adapter_records_truncation_without_loading_a_model() -> None:
    model = _LocalModel()
    embedder = BgeSmallEmbedder()
    embedder._model = model  # type: ignore[assignment]
    result = embedder.embed_query("question", QueryKind.QUESTION)
    assert len(result.vector) == 384
    assert result.audit.token_count == 513
    assert result.audit.truncated
    assert model.inputs == [f"{BGE_QUERY_PREFIX}question"]
    assert BGE_MODEL_ARTIFACT.immutable_revision == BGE_REVISION
    assert BGE_MODEL_ARTIFACT.tokenizer_revision == BGE_REVISION


def test_pinned_adapter_disables_downloads_by_default() -> None:
    assert not BgeSmallEmbedder().allow_download


def test_pinned_adapter_fails_clearly_when_local_artifact_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing(_: str) -> NoReturn:
        raise ImportError("offline test sentinel")

    monkeypatch.setattr(
        "groundloop.ai.embeddings.bge.importlib.import_module",
        missing,
    )
    with pytest.raises(ArtifactUnavailableError, match=BGE_REVISION):
        BgeSmallEmbedder().embed_query("question", QueryKind.QUESTION)
