from __future__ import annotations

import hashlib
import math
import os
import uuid
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.errors import ValidationError
from groundloop.m4.admission import (
    CLAIM_ADMISSION_HNSW_INDEX,
    ChunkRoleVector,
    ClaimRoleVector,
    ExactPgvectorConfig,
    ExactReverseVectorIndex,
    HnswPgvectorBuildConfig,
    HnswPgvectorSearchConfig,
    LexicalRegistrySnapshot,
    LexicalV1Config,
    LexicalV1Policy,
    PostgresAdmissionServerIdentity,
    PostgresExactReverseVectorIndex,
    PostgresHnswReverseVectorIndex,
    PostgresLexicalSearchBackend,
    PostgresSimpleLexemeAnalyzer,
    build_candidate_policy_manifest,
    measure_ann_recall,
)
from groundloop.m4.contracts import CandidatePolicyManifest, VectorIndexKind

from .conftest import HASH

CLAIM_ROLE = "Represent this sentence for searching relevant passages: {claim}"
CHUNK_ROLE = "{chunk}"
DIMENSIONS = 3
CLAIM_COUNT = 64


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _database_url() -> str:
    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip(
            "live PostgreSQL test: set GROUNDLOOP_TEST_DATABASE_URL or "
            "GROUNDLOOP_DATABASE_URL"
        )
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


@pytest.fixture
def live_admission_connection() -> Iterator[Connection[tuple[Any, ...]]]:
    connection = psycopg.connect(_database_url(), autocommit=True)
    schema = f"groundloop_m4_admission_{uuid.uuid4().hex}"
    try:
        connection.execute("CREATE EXTENSION IF NOT EXISTS vector")
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
        )
        connection.execute(
            """
            CREATE TABLE groundloop_m4_claim_admission_index (
                claim_registry_snapshot_id text NOT NULL,
                claim_id text NOT NULL,
                embedding_model_artifact_id text NOT NULL,
                claim_role_template_hash char(64) NOT NULL,
                embedding_input_hash char(64) NOT NULL,
                embedding vector(3) NOT NULL,
                lexical_tsv tsvector NOT NULL,
                PRIMARY KEY (claim_registry_snapshot_id, claim_id)
            )
            """
        )
        yield connection
    finally:
        connection.execute("SET search_path TO public")
        connection.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
        )
        connection.close()


def _manifest(
    *,
    server: PostgresAdmissionServerIdentity,
    lexical_config: LexicalV1Config,
    kind: VectorIndexKind,
    build_config: tuple[tuple[str, str], ...],
    search_config: tuple[tuple[str, str], ...],
) -> CandidatePolicyManifest:
    return build_candidate_policy_manifest(
        policy_id=f"policy-{kind.value}",
        embedding_model_artifact_id="embedding-real-boundary-v1",
        claim_role_template=CLAIM_ROLE,
        chunk_role_template=CHUNK_ROLE,
        vector_method_version="reverse-bge-v1",
        vector_index_kind=kind,
        vector_index_build_config=build_config,
        vector_search_config=search_config,
        lexical_method_version="postgres-lexical-v1",
        lexical_config=lexical_config,
        lexical_postgres_version=server.postgres_version,
        lexical_regconfig_identity="simple",
        claim_registry_snapshot_id="registry-live-1",
        claim_count=CLAIM_COUNT,
        fusion_version="rank-interleave-v1",
        approximate_cap_per_inserted_chunk=8,
        frontier_depth=4,
        verifier_execution_spec_hash=HASH,
        decision_policy_version="m3-policy-v1",
    )


def _claim_rows(
    manifest: CandidatePolicyManifest,
) -> list[tuple[str, str, str, str, str, str, str]]:
    rows = []
    for index in range(CLAIM_COUNT):
        vector = _claim_vector(index)
        if index in {0, 1}:
            text = "Alpha common"
        elif index == 2:
            text = "Beta common"
        else:
            text = f"term{index} common"
        rows.append(
            (
                manifest.claim_registry_snapshot_id,
                f"claim-{index:03d}",
                manifest.embedding_model_artifact_id,
                manifest.claim_role_template_hash,
                _digest(f"claim-role-input-{index}"),
                "[" + ",".join(format(value, ".17g") for value in vector) + "]",
                text,
            )
        )
    return rows


def _claim_vector(index: int) -> tuple[float, float, float]:
    if index in {0, 1}:
        return (1.0, 0.0, 0.0)
    angle = (index - 1) * math.pi / (CLAIM_COUNT - 2)
    return (math.cos(angle), math.sin(angle), 0.0)


def _claim_role_vectors() -> tuple[ClaimRoleVector, ...]:
    return tuple(
        ClaimRoleVector(
            f"claim-{index:03d}",
            _claim_vector(index),
            _digest(f"claim-role-input-{index}"),
        )
        for index in range(CLAIM_COUNT)
    )


def _populate(
    connection: Connection[tuple[Any, ...]], manifest: CandidatePolicyManifest
) -> None:
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO groundloop_m4_claim_admission_index (
                claim_registry_snapshot_id, claim_id,
                embedding_model_artifact_id, claim_role_template_hash,
                embedding_input_hash, embedding, lexical_tsv
            ) VALUES (%s, %s, %s, %s, %s, %s::vector,
                      to_tsvector('simple'::regconfig, %s))
            """,
            _claim_rows(manifest),
        )


def _lexical_registry(
    analyzer: PostgresSimpleLexemeAnalyzer,
) -> LexicalRegistrySnapshot:
    rows = []
    for index in range(CLAIM_COUNT):
        if index in {0, 1}:
            text = "Alpha common"
        elif index == 2:
            text = "Beta common"
        else:
            text = f"term{index} common"
        rows.append((f"claim-{index:03d}", analyzer.analyze(text)))
    return LexicalRegistrySnapshot("registry-live-1", tuple(rows))


def test_postgres_config_provenance_is_canonical(
    lexical_config: LexicalV1Config,
) -> None:
    exact = ExactPgvectorConfig(384, pgvector_version="0.8.5")
    build = HnswPgvectorBuildConfig(
        384, m=16, ef_construction=64, pgvector_version="0.8.5"
    )
    search = HnswPgvectorSearchConfig(ef_search=80)
    assert exact.build_config == tuple(sorted(exact.build_config))
    assert build.config_pairs == tuple(sorted(build.config_pairs))
    assert search.config_pairs == tuple(sorted(search.config_pairs))
    server = PostgresAdmissionServerIdentity("16.14", "0.8.5", "simple")
    manifest = _manifest(
        server=server,
        lexical_config=lexical_config,
        kind=VectorIndexKind.HNSW,
        build_config=build.config_pairs,
        search_config=search.config_pairs,
    )
    assert manifest.lexical_postgres_version == "16.14"
    assert manifest.vector_index_kind is VectorIndexKind.HNSW


def test_server_identity_rejects_manifest_drift() -> None:
    server = PostgresAdmissionServerIdentity("16.14", "0.8.5", "simple")
    with pytest.raises(ValidationError, match="version differs"):
        server.validate_lexical_manifest(
            postgres_version="16.13", regconfig_identity="simple"
        )
    with pytest.raises(ValidationError, match="regconfig differs"):
        server.validate_lexical_manifest(
            postgres_version="16.14", regconfig_identity="english"
        )


def test_live_postgres_lexical_v1_and_gin_plan(
    live_admission_connection: Connection[tuple[Any, ...]],
    lexical_config: LexicalV1Config,
) -> None:
    connection = live_admission_connection
    server = PostgresAdmissionServerIdentity.inspect(connection)
    exact_config = ExactPgvectorConfig(DIMENSIONS, server.pgvector_version)
    manifest = _manifest(
        server=server,
        lexical_config=lexical_config,
        kind=VectorIndexKind.EXACT,
        build_config=exact_config.build_config,
        search_config=exact_config.search_config,
    )
    _populate(connection, manifest)
    connection.execute(
        """
        CREATE INDEX groundloop_m4_claim_admission_lexical_gin
        ON groundloop_m4_claim_admission_index USING gin (lexical_tsv)
        """
    )
    connection.execute("ANALYZE groundloop_m4_claim_admission_index")

    analyzer = PostgresSimpleLexemeAnalyzer(connection, server)
    assert analyzer.analyze("The ALPHA alpha") == ("alpha", "the")
    injection = "alpha'); DROP TABLE groundloop_m4_claim_admission_index; --"
    assert analyzer.analyze(injection)

    registry = _lexical_registry(analyzer)
    backend = PostgresLexicalSearchBackend(
        connection=connection, server=server, manifest=manifest
    )
    backend.validate_registry(registry)
    policy = LexicalV1Policy(
        config=lexical_config,
        registry=registry,
        analyzer=analyzer,
        backend=backend,
    )
    hits = policy.search(
        epoch_id=1,
        manifest=manifest,
        chunk_version_id="chunk-live",
        chunk_text="ALPHA common",
    )
    assert tuple(hit.pair.claim_id for hit in hits[:2]) == (
        "claim-000",
        "claim-001",
    )
    assert hits[0].score == hits[1].score
    assert connection.execute(
        "SELECT to_regclass('groundloop_m4_claim_admission_index')"
    ).fetchone()[0]

    connection.execute("SET enable_seqscan = off")
    try:
        plan = backend.explain_search(query_lexemes=("alpha",), limit=8)
    finally:
        connection.execute("RESET enable_seqscan")
    assert any(
        "groundloop_m4_claim_admission_lexical_gin" in line for line in plan
    ), "\n".join(plan)
    connection.execute(
        """
        UPDATE groundloop_m4_claim_admission_index
        SET lexical_tsv = to_tsvector('simple', 'changed')
        WHERE claim_registry_snapshot_id = %s AND claim_id = %s
        """,
        (manifest.claim_registry_snapshot_id, "claim-063"),
    )
    with pytest.raises(ValidationError, match="content differs"):
        backend.validate_registry(registry)


def test_live_exact_and_hnsw_reverse_vector_with_plan_and_recall(
    live_admission_connection: Connection[tuple[Any, ...]],
    lexical_config: LexicalV1Config,
) -> None:
    connection = live_admission_connection
    server = PostgresAdmissionServerIdentity.inspect(connection)
    exact_config = ExactPgvectorConfig(DIMENSIONS, server.pgvector_version)
    exact_manifest = _manifest(
        server=server,
        lexical_config=lexical_config,
        kind=VectorIndexKind.EXACT,
        build_config=exact_config.build_config,
        search_config=exact_config.search_config,
    )
    _populate(connection, exact_manifest)
    build = HnswPgvectorBuildConfig(
        DIMENSIONS, m=4, ef_construction=16, pgvector_version=server.pgvector_version
    )
    search = HnswPgvectorSearchConfig(ef_search=8)
    hnsw_manifest = _manifest(
        server=server,
        lexical_config=lexical_config,
        kind=VectorIndexKind.HNSW,
        build_config=build.config_pairs,
        search_config=search.config_pairs,
    )
    connection.execute(
        """
        CREATE INDEX groundloop_m4_claim_admission_hnsw
        ON groundloop_m4_claim_admission_index
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 4, ef_construction = 16)
        """
    )
    connection.execute("ANALYZE groundloop_m4_claim_admission_index")

    exact = PostgresExactReverseVectorIndex(
        connection=connection,
        server=server,
        manifest=exact_manifest,
        config=exact_config,
    )
    approximate = PostgresHnswReverseVectorIndex(
        connection=connection,
        server=server,
        manifest=hnsw_manifest,
        build_config=build,
        search_config=search,
    )
    exact.validate_registry()
    approximate.validate_registry()
    wrong_build = HnswPgvectorBuildConfig(
        DIMENSIONS,
        m=5,
        ef_construction=16,
        pgvector_version=server.pgvector_version,
    )
    wrong_manifest = _manifest(
        server=server,
        lexical_config=lexical_config,
        kind=VectorIndexKind.HNSW,
        build_config=wrong_build.config_pairs,
        search_config=search.config_pairs,
    )
    with pytest.raises(ValidationError, match="reloptions differ"):
        PostgresHnswReverseVectorIndex(
            connection=connection,
            server=server,
            manifest=wrong_manifest,
            build_config=wrong_build,
            search_config=search,
        )
    chunk = ChunkRoleVector("chunk-live", (1.0, 0.0, 0.0), _digest("chunk"))
    exact_result = exact.search(
        epoch_id=1,
        candidate_policy_id=exact_manifest.policy_id,
        chunk=chunk,
        limit=8,
    )
    approximate_result = approximate.search(
        epoch_id=1,
        candidate_policy_id=hnsw_manifest.policy_id,
        chunk=chunk,
        limit=8,
    )
    assert tuple(hit.pair.claim_id for hit in exact_result.hits[:2]) == (
        "claim-000",
        "claim-001",
    )
    exact_in_memory = ExactReverseVectorIndex(_claim_role_vectors()).search(
        epoch_id=1,
        candidate_policy_id=exact_manifest.policy_id,
        chunk=chunk,
        limit=8,
    )
    assert tuple(hit.pair for hit in exact_result.hits) == tuple(
        hit.pair for hit in exact_in_memory.hits
    )
    ann_reference = ExactReverseVectorIndex(_claim_role_vectors()).search(
        epoch_id=1,
        candidate_policy_id=hnsw_manifest.policy_id,
        chunk=chunk,
        limit=8,
    )
    recall = measure_ann_recall(ann_reference, approximate_result, limit=8)
    assert recall.recall_at_limit == 1.0
    assert approximate.physical_index.access_method == "hnsw"

    exact_plan = exact.explain_search(chunk, limit=8)
    assert any("CTE scored" in line for line in exact_plan)
    assert not any(CLAIM_ADMISSION_HNSW_INDEX in line for line in exact_plan)

    connection.execute("SET enable_seqscan = off")
    try:
        plan = approximate.explain_search(chunk, limit=8)
    finally:
        connection.execute("RESET enable_seqscan")
    assert any(CLAIM_ADMISSION_HNSW_INDEX in line for line in plan), "\n".join(plan)
    connection.execute(
        """
        INSERT INTO groundloop_m4_claim_admission_index
        SELECT 'other-registry', claim_id, embedding_model_artifact_id,
               claim_role_template_hash, embedding_input_hash, embedding,
               lexical_tsv
        FROM groundloop_m4_claim_admission_index
        WHERE claim_registry_snapshot_id = %s AND claim_id = %s
        """,
        (hnsw_manifest.claim_registry_snapshot_id, "claim-000"),
    )
    with pytest.raises(ValidationError, match="one sealed"):
        approximate.validate_registry()
