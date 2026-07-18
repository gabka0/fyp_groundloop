from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import psycopg
import pytest
from psycopg import sql

from groundloop.ai.chunking import FixedCharChunker
from groundloop.ai.contracts import QueryKind, RetrievalQuery
from groundloop.ai.embeddings import DeterministicFakeEmbedder
from groundloop.ai.retrieval import (
    InMemoryCosineStore,
    PgvectorCosineStore,
    StaticCosineRetriever,
    StoredEmbeddingHit,
    claim_query,
    question_query,
)
from groundloop.errors import ValidationError


def _retriever() -> tuple[StaticCosineRetriever, DeterministicFakeEmbedder]:
    embedder = DeterministicFakeEmbedder()
    chunks = FixedCharChunker(max_characters=10).chunk("dv", "a" * 25)
    store = InMemoryCosineStore()
    store.add(embedder.embed(chunks))
    return StaticCosineRetriever(embedder, store), embedder


def test_question_and_claim_defaults_and_top_k_boundaries() -> None:
    retriever, embedder = _retriever()
    question = question_query(
        query_id="q1",
        text="question",
        embedding_model_artifact_id=embedder.model_artifact.artifact_id,
    )
    claim = claim_query(
        query_id="c1",
        text="claim",
        embedding_model_artifact_id=embedder.model_artifact.artifact_id,
    )
    assert question.top_k == 6
    assert claim.top_k == 4
    assert len(retriever.retrieve(question)) == 3
    assert len(retriever.retrieve(replace(claim, top_k=2))) == 2


class _TiedStore:
    def search_cosine(
        self,
        *,
        vector: tuple[float, ...],
        model_artifact_id: str,
        limit: int,
    ) -> tuple[StoredEmbeddingHit, ...]:
        del vector, model_artifact_id, limit
        return (
            StoredEmbeddingHit("chunk-z", 0.25),
            StoredEmbeddingHit("chunk-a", 0.25),
            StoredEmbeddingHit("chunk-b", 0.10),
        )


def test_deterministic_distance_then_chunk_id_ties_and_replay() -> None:
    embedder = DeterministicFakeEmbedder()
    retriever = StaticCosineRetriever(embedder, _TiedStore())
    query = question_query(
        query_id="q",
        text="question",
        embedding_model_artifact_id=embedder.model_artifact.artifact_id,
        top_k=3,
    )
    first = retriever.retrieve(query)
    second = retriever.retrieve(query)
    assert first == second
    assert tuple(candidate.chunk_version_id for candidate in first) == (
        "chunk-b",
        "chunk-a",
        "chunk-z",
    )
    assert tuple(candidate.rank for candidate in first) == (1, 2, 3)
    assert all(candidate.method_version == "bge-cosine-v1" for candidate in first)
    assert all(
        candidate.embedding_model_artifact_id == embedder.model_artifact.artifact_id
        for candidate in first
    )
    assert retriever.last_execution is not None
    assert retriever.last_execution.top_k == 3


def test_candidate_identity_includes_requested_depth() -> None:
    embedder = DeterministicFakeEmbedder()
    retriever = StaticCosineRetriever(embedder, _TiedStore())
    base = question_query(
        query_id="q",
        text="question",
        embedding_model_artifact_id=embedder.model_artifact.artifact_id,
        top_k=2,
    )
    shallow = retriever.retrieve(base)
    deep = retriever.retrieve(replace(base, top_k=3))
    assert shallow[0].chunk_version_id == deep[0].chunk_version_id
    assert shallow[0].candidate_id != deep[0].candidate_id


def test_model_artifact_mismatch_is_rejected() -> None:
    retriever, _ = _retriever()
    query = RetrievalQuery(
        QueryKind.QUESTION,
        "q",
        "question",
        1,
        "wrong-model",
        "bge-cosine-v1",
    )
    with pytest.raises(ValidationError, match="does not match"):
        retriever.retrieve(query)


class _Cursor:
    def fetchall(self) -> list[tuple[str, float]]:
        return [("chunk-a", 0.1), ("chunk-b", 0.2)]


class _Connection:
    def __init__(self) -> None:
        self.query = ""
        self.params: tuple[object, ...] = ()

    def execute(self, query: str, params: tuple[object, ...]) -> _Cursor:
        self.query = query
        self.params = params
        return _Cursor()


def test_pgvector_boundary_uses_cosine_model_filter_and_deterministic_order() -> None:
    connection = _Connection()
    store = PgvectorCosineStore(cast(Any, connection))
    vector = (1.0,) + (0.0,) * 383
    hits = store.search_cosine(vector=vector, model_artifact_id="model", limit=2)
    assert tuple(hit.chunk_version_id for hit in hits) == ("chunk-a", "chunk-b")
    assert "embedding <=> %s::vector" in connection.query
    assert "WHERE model_artifact_id = %s" in connection.query
    assert "ORDER BY distance, chunk_version_id" in connection.query
    assert connection.params[1:] == ("model", 2)


def test_live_pgvector_cosine_ties_when_database_is_configured() -> None:
    database_url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not database_url:
        pytest.skip("live PostgreSQL DSN is not configured")
    database_url = database_url.replace("postgresql+psycopg://", "postgresql://", 1)
    schema = f"groundloop_m3_retrieval_{uuid.uuid4().hex}"
    connection = psycopg.connect(database_url, autocommit=True)
    try:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
        )
        connection.execute(
            """
            CREATE TABLE groundloop_chunk_embedding (
                chunk_version_id text NOT NULL,
                model_artifact_id text NOT NULL,
                embedding vector(384) NOT NULL,
                PRIMARY KEY (chunk_version_id, model_artifact_id)
            )
            """
        )
        axis = "[1," + ",".join("0" for _ in range(383)) + "]"
        orthogonal = "[0,1," + ",".join("0" for _ in range(382)) + "]"
        with connection.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO groundloop_chunk_embedding VALUES (%s, %s, %s::vector)",
                [
                    ("chunk-b", "model", axis),
                    ("chunk-a", "model", axis),
                    ("chunk-c", "model", orthogonal),
                    ("other-model", "other", axis),
                ],
            )
        store = PgvectorCosineStore(cast(Any, connection))
        hits = store.search_cosine(
            vector=(1.0,) + (0.0,) * 383,
            model_artifact_id="model",
            limit=2,
        )
        assert tuple(hit.chunk_version_id for hit in hits) == (
            "chunk-a",
            "chunk-b",
        )
        assert tuple(hit.distance for hit in hits) == (0.0, 0.0)
    finally:
        connection.execute("SET search_path TO public")
        connection.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
        )
        connection.close()


def test_annotated_fixture_evaluation_reports_required_metrics() -> None:
    root = Path(__file__).resolve().parents[3]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(root / "src")
    completed = subprocess.run(
        [sys.executable, "experiments/m3/retrieval/evaluate_fixture.py"],
        cwd=root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    report = json.loads(completed.stdout)
    assert report["document_count"] == 8
    assert report["query_count"] == 4
    assert 0.0 <= report["mean_recall_at_k"] <= 1.0
    assert 0.0 <= report["mrr"] <= 1.0
    assert 0.0 <= report["mean_ndcg_at_k"] <= 1.0
    assert report["retrieval_cold_median_ms"] >= 0.0
    assert report["retrieval_warm_median_ms"] >= 0.0
    assert report["exact_vector_bytes"] == 8 * 384 * 8
