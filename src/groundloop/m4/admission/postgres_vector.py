"""Exact and explicitly approximate PostgreSQL reverse-vector adapters."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, cast

from psycopg import Connection

from groundloop.errors import ValidationError
from groundloop.m4.admission.manifest import hash_config_pairs
from groundloop.m4.admission.postgres_common import (
    CLAIM_ADMISSION_HNSW_INDEX,
    CLAIM_ADMISSION_RELATION,
    PostgresAdmissionServerIdentity,
)
from groundloop.m4.admission.vector import ChunkRoleVector, ReverseVectorSearch
from groundloop.m4.contracts import (
    AdmissionChannel,
    CandidatePolicyManifest,
    ChannelHit,
    PairKey,
    VectorIndexKind,
    stable_m4_digest,
)

_HEX = frozenset("0123456789abcdef")

_EXACT_SQL = """
WITH scored AS MATERIALIZED (
    SELECT claim_id, embedding_input_hash,
           embedding <=> %s::vector AS distance
    FROM groundloop_m4_claim_admission_index
    WHERE claim_registry_snapshot_id = %s
      AND embedding_model_artifact_id = %s
      AND claim_role_template_hash = %s
)
SELECT claim_id, embedding_input_hash, distance
FROM scored
ORDER BY distance ASC, claim_id ASC
LIMIT %s
"""

_HNSW_SQL = """
WITH approximate AS MATERIALIZED (
    SELECT claim_id, embedding_input_hash,
           embedding <=> %s::vector AS distance
    FROM groundloop_m4_claim_admission_index
    WHERE claim_registry_snapshot_id = %s
      AND embedding_model_artifact_id = %s
      AND claim_role_template_hash = %s
    ORDER BY embedding <=> %s::vector ASC
    LIMIT %s
)
SELECT claim_id, embedding_input_hash, distance
FROM approximate
ORDER BY distance ASC, claim_id ASC
LIMIT %s
"""


def _vector_literal(vector: tuple[float, ...]) -> str:
    return "[" + ",".join(format(value, ".17g") for value in vector) + "]"


@dataclass(frozen=True, slots=True)
class ExactPgvectorConfig:
    dimensions: int
    pgvector_version: str

    def __post_init__(self) -> None:
        if self.dimensions <= 0:
            raise ValidationError("pgvector dimensions must be positive")
        if not self.pgvector_version.strip():
            raise ValidationError("pgvector version must be non-empty")

    @property
    def build_config(self) -> tuple[tuple[str, str], ...]:
        return (
            ("algorithm", "exact-materialized-score-sort-v1"),
            ("dimensions", str(self.dimensions)),
            ("embedding_column", "embedding"),
            ("pgvector_version", self.pgvector_version),
            ("relation", CLAIM_ADMISSION_RELATION),
        )

    @property
    def search_config(self) -> tuple[tuple[str, str], ...]:
        return (
            ("distance", "cosine"),
            ("execution", "materialized-exhaustive"),
            ("tie_rule", "distance-asc-claim-id-asc"),
        )


@dataclass(frozen=True, slots=True)
class HnswPgvectorBuildConfig:
    dimensions: int
    m: int
    ef_construction: int
    pgvector_version: str

    def __post_init__(self) -> None:
        if min(self.dimensions, self.m, self.ef_construction) <= 0:
            raise ValidationError("HNSW build integers must be positive")
        if not self.pgvector_version.strip():
            raise ValidationError("pgvector version must be non-empty")

    @property
    def config_pairs(self) -> tuple[tuple[str, str], ...]:
        return (
            ("algorithm", "hnsw"),
            ("dimensions", str(self.dimensions)),
            ("ef_construction", str(self.ef_construction)),
            ("embedding_column", "embedding"),
            ("index", CLAIM_ADMISSION_HNSW_INDEX),
            ("m", str(self.m)),
            ("opclass", "vector_cosine_ops"),
            ("pgvector_version", self.pgvector_version),
            ("population_scope", "single-sealed-claim-registry"),
            ("relation", CLAIM_ADMISSION_RELATION),
        )


@dataclass(frozen=True, slots=True)
class HnswPgvectorSearchConfig:
    ef_search: int
    iterative_scan: str = "off"
    max_scan_tuples: int = 20_000
    scan_mem_multiplier: float = 1.0

    def __post_init__(self) -> None:
        if self.ef_search <= 0 or self.max_scan_tuples <= 0:
            raise ValidationError("HNSW search integers must be positive")
        if self.iterative_scan not in {"off", "strict_order", "relaxed_order"}:
            raise ValidationError("unsupported HNSW iterative_scan mode")
        if not math.isfinite(self.scan_mem_multiplier) or self.scan_mem_multiplier <= 0:
            raise ValidationError("HNSW scan_mem_multiplier must be positive")

    @property
    def config_pairs(self) -> tuple[tuple[str, str], ...]:
        return (
            ("candidate_pool", "requested-limit"),
            ("distance", "cosine"),
            ("ef_search", str(self.ef_search)),
            ("iterative_scan", self.iterative_scan),
            ("max_scan_tuples", str(self.max_scan_tuples)),
            ("scan_mem_multiplier", format(self.scan_mem_multiplier, ".17g")),
            ("tie_rule", "distance-asc-claim-id-asc"),
        )


@dataclass(frozen=True, slots=True)
class PhysicalHnswIndex:
    access_method: str
    reloptions: tuple[str, ...]
    definition_hash: str


class _PostgresReverseVectorBase:
    def __init__(
        self,
        *,
        connection: Connection[Any],
        server: PostgresAdmissionServerIdentity,
        manifest: CandidatePolicyManifest,
        dimensions: int,
        expected_kind: VectorIndexKind,
        build_config: tuple[tuple[str, str], ...],
        search_config: tuple[tuple[str, str], ...],
        physical_index_hash: str = "none",
    ) -> None:
        if dimensions <= 0:
            raise ValidationError("pgvector dimensions must be positive")
        if manifest.vector_index_kind is not expected_kind:
            raise ValidationError("manifest uses a different vector index kind")
        expected_build = hash_config_pairs(
            "m4-vector-index-build-config-v1", build_config
        )
        expected_search = hash_config_pairs(
            "m4-vector-search-config-v1", search_config
        )
        if manifest.vector_index_build_config_hash != expected_build:
            raise ValidationError("manifest vector build config differs from adapter")
        if manifest.vector_search_config_hash != expected_search:
            raise ValidationError("manifest vector search config differs from adapter")
        self._connection = connection
        self._server = server
        self._manifest = manifest
        self._dimensions = dimensions
        self._kind = expected_kind
        self._artifact_id = stable_m4_digest(
            "m4-postgres-reverse-vector-adapter-v1",
            server.artifact_hash,
            expected_kind.value,
            manifest.embedding_model_artifact_id,
            manifest.claim_role_template_hash,
            manifest.claim_registry_snapshot_id,
            str(manifest.claim_count),
            expected_build,
            expected_search,
            physical_index_hash,
        )

    @property
    def artifact_id(self) -> str:
        return self._artifact_id

    def validate_registry(self) -> None:
        row = self._connection.execute(
            """
            SELECT count(*)
            FROM groundloop_m4_claim_admission_index
            WHERE claim_registry_snapshot_id = %s
              AND embedding_model_artifact_id = %s
              AND claim_role_template_hash = %s
            """,
            (
                self._manifest.claim_registry_snapshot_id,
                self._manifest.embedding_model_artifact_id,
                self._manifest.claim_role_template_hash,
            ),
        ).fetchone()
        if row is None or int(row[0]) != self._manifest.claim_count:
            raise ValidationError("PostgreSQL vector registry count differs")

        type_row = self._connection.execute(
            """
            SELECT format_type(attribute.atttypid, attribute.atttypmod)
            FROM pg_attribute AS attribute
            WHERE attribute.attrelid = to_regclass(%s)
              AND attribute.attname = 'embedding'
              AND NOT attribute.attisdropped
            """,
            (CLAIM_ADMISSION_RELATION,),
        ).fetchone()
        if type_row is None or str(type_row[0]) != f"vector({self._dimensions})":
            raise ValidationError("PostgreSQL vector column dimension differs")

    def _validate_search_inputs(
        self,
        *,
        epoch_id: int,
        candidate_policy_id: str,
        chunk: ChunkRoleVector,
        limit: int,
    ) -> None:
        if epoch_id <= 0:
            raise ValidationError("epoch_id must be positive")
        if candidate_policy_id != self._manifest.policy_id:
            raise ValidationError("search uses a different candidate policy")
        if len(chunk.vector) != self._dimensions:
            raise ValidationError("chunk vector dimension differs from pgvector index")
        if limit <= 0:
            raise ValidationError("reverse-vector limit must be positive")

    def _result(
        self,
        *,
        epoch_id: int,
        candidate_policy_id: str,
        chunk: ChunkRoleVector,
        limit: int,
        rows: list[tuple[object, ...]],
    ) -> ReverseVectorSearch:
        parsed: list[tuple[str, str, float]] = []
        for claim_id, input_hash, distance in rows:
            value = float(cast(Any, distance))
            if not math.isfinite(value):
                raise ValidationError("pgvector returned non-finite distance")
            normalized_hash = str(input_hash).strip()
            if len(normalized_hash) != 64 or any(
                character not in _HEX for character in normalized_hash
            ):
                raise ValidationError("claim embedding input hash is invalid")
            parsed.append((str(claim_id), normalized_hash, value))
        if len({claim_id for claim_id, _hash, _distance in parsed}) != len(parsed):
            raise ValidationError("pgvector returned duplicate claim IDs")
        expected = sorted(parsed, key=lambda item: (item[2], item[0]))
        if parsed != expected:
            raise ValidationError("pgvector results violate deterministic order")
        query_hash = stable_m4_digest(
            "m4-postgres-reverse-vector-query-v1",
            self._artifact_id,
            candidate_policy_id,
            chunk.chunk_version_id,
            chunk.input_hash,
            str(limit),
            *(
                value
                for claim_id, input_hash, distance in parsed
                for value in (claim_id, input_hash, format(distance, ".17g"))
            ),
        )
        hits = tuple(
            ChannelHit(
                epoch_id=epoch_id,
                pair=PairKey(claim_id, chunk.chunk_version_id),
                candidate_policy_id=candidate_policy_id,
                channel=AdmissionChannel.VECTOR,
                rank=rank,
                score=min(1.0, max(-1.0, 1.0 - distance)),
                channel_artifact_hash=stable_m4_digest(
                    "m4-postgres-vector-hit-v1",
                    query_hash,
                    claim_id,
                    input_hash,
                    format(distance, ".17g"),
                    str(rank),
                ),
            )
            for rank, (claim_id, input_hash, distance) in enumerate(parsed, start=1)
        )
        return ReverseVectorSearch(hits, query_hash)


class PostgresExactReverseVectorIndex(_PostgresReverseVectorBase):
    """Exact pgvector score-all/materialize/sort reference."""

    def __init__(
        self,
        *,
        connection: Connection[Any],
        server: PostgresAdmissionServerIdentity,
        manifest: CandidatePolicyManifest,
        config: ExactPgvectorConfig,
    ) -> None:
        if config.pgvector_version != server.pgvector_version:
            raise ValidationError("exact config pgvector version differs from runtime")
        super().__init__(
            connection=connection,
            server=server,
            manifest=manifest,
            dimensions=config.dimensions,
            expected_kind=VectorIndexKind.EXACT,
            build_config=config.build_config,
            search_config=config.search_config,
        )

    def search(
        self,
        *,
        epoch_id: int,
        candidate_policy_id: str,
        chunk: ChunkRoleVector,
        limit: int,
    ) -> ReverseVectorSearch:
        self._validate_search_inputs(
            epoch_id=epoch_id,
            candidate_policy_id=candidate_policy_id,
            chunk=chunk,
            limit=limit,
        )
        rows = self._connection.execute(
            _EXACT_SQL,
            (
                _vector_literal(chunk.vector),
                self._manifest.claim_registry_snapshot_id,
                self._manifest.embedding_model_artifact_id,
                self._manifest.claim_role_template_hash,
                limit,
            ),
        ).fetchall()
        return self._result(
            epoch_id=epoch_id,
            candidate_policy_id=candidate_policy_id,
            chunk=chunk,
            limit=limit,
            rows=rows,
        )

    def explain_search(self, chunk: ChunkRoleVector, *, limit: int) -> tuple[str, ...]:
        if len(chunk.vector) != self._dimensions or limit <= 0:
            raise ValidationError("EXPLAIN vector input is invalid")
        rows = self._connection.execute(
            "EXPLAIN (COSTS OFF) " + _EXACT_SQL,
            (
                _vector_literal(chunk.vector),
                self._manifest.claim_registry_snapshot_id,
                self._manifest.embedding_model_artifact_id,
                self._manifest.claim_role_template_hash,
                limit,
            ),
        ).fetchall()
        return tuple(str(row[0]) for row in rows)


class PostgresHnswReverseVectorIndex(_PostgresReverseVectorBase):
    """Approximate HNSW search whose recall must be measured empirically."""

    def __init__(
        self,
        *,
        connection: Connection[Any],
        server: PostgresAdmissionServerIdentity,
        manifest: CandidatePolicyManifest,
        build_config: HnswPgvectorBuildConfig,
        search_config: HnswPgvectorSearchConfig,
    ) -> None:
        if build_config.pgvector_version != server.pgvector_version:
            raise ValidationError("HNSW config pgvector version differs from runtime")
        physical = self._inspect_physical_index(connection, build_config)
        self._search_config = search_config
        super().__init__(
            connection=connection,
            server=server,
            manifest=manifest,
            dimensions=build_config.dimensions,
            expected_kind=VectorIndexKind.HNSW,
            build_config=build_config.config_pairs,
            search_config=search_config.config_pairs,
            physical_index_hash=physical.definition_hash,
        )
        self.physical_index = physical

    def validate_registry(self) -> None:
        super().validate_registry()
        row = self._connection.execute(
            """
            SELECT count(*), count(DISTINCT claim_registry_snapshot_id),
                   count(DISTINCT embedding_model_artifact_id),
                   count(DISTINCT claim_role_template_hash)
            FROM groundloop_m4_claim_admission_index
            """
        ).fetchone()
        expected = (self._manifest.claim_count, 1, 1, 1)
        if row is None or tuple(int(value) for value in row) != expected:
            raise ValidationError(
                "HNSW relation must contain one sealed claim-registry population"
            )

    @staticmethod
    def _inspect_physical_index(
        connection: Connection[Any], config: HnswPgvectorBuildConfig
    ) -> PhysicalHnswIndex:
        row = connection.execute(
            """
            SELECT access_method.amname, index_class.reloptions,
                   pg_get_indexdef(index_class.oid),
                   format_type(attribute.atttypid, attribute.atttypmod)
            FROM pg_class AS index_class
            JOIN pg_am AS access_method
              ON access_method.oid = index_class.relam
            JOIN pg_index AS index_record
              ON index_record.indexrelid = index_class.oid
            JOIN pg_attribute AS attribute
              ON attribute.attrelid = index_record.indrelid
             AND attribute.attname = 'embedding'
             AND NOT attribute.attisdropped
            WHERE index_class.oid = to_regclass(%s)
              AND index_record.indrelid = to_regclass(%s)
            """,
            (CLAIM_ADMISSION_HNSW_INDEX, CLAIM_ADMISSION_RELATION),
        ).fetchone()
        if row is None:
            raise ValidationError("frozen M4 HNSW index is absent")
        access_method = str(row[0])
        reloptions = tuple(sorted(str(item) for item in (row[1] or ())))
        definition = str(row[2])
        vector_type = str(row[3])
        if access_method != "hnsw":
            raise ValidationError("configured reverse-vector index is not HNSW")
        required_options = {
            f"m={config.m}",
            f"ef_construction={config.ef_construction}",
        }
        if not required_options.issubset(reloptions):
            raise ValidationError("physical HNSW reloptions differ from provenance")
        if "embedding vector_cosine_ops" not in definition:
            raise ValidationError("physical HNSW index does not use cosine opclass")
        if vector_type != f"vector({config.dimensions})":
            raise ValidationError("physical HNSW vector dimension differs")
        return PhysicalHnswIndex(
            access_method,
            reloptions,
            stable_m4_digest("m4-physical-hnsw-index-v1", definition),
        )

    def _install_search_settings(self) -> None:
        config = self._search_config
        self._connection.execute(
            """
            SELECT set_config('hnsw.ef_search', %s, true),
                   set_config('hnsw.iterative_scan', %s, true),
                   set_config('hnsw.max_scan_tuples', %s, true),
                   set_config('hnsw.scan_mem_multiplier', %s, true)
            """,
            (
                str(config.ef_search),
                config.iterative_scan,
                str(config.max_scan_tuples),
                format(config.scan_mem_multiplier, ".17g"),
            ),
        )

    def search(
        self,
        *,
        epoch_id: int,
        candidate_policy_id: str,
        chunk: ChunkRoleVector,
        limit: int,
    ) -> ReverseVectorSearch:
        self._validate_search_inputs(
            epoch_id=epoch_id,
            candidate_policy_id=candidate_policy_id,
            chunk=chunk,
            limit=limit,
        )
        literal = _vector_literal(chunk.vector)
        with self._connection.transaction():
            self._install_search_settings()
            rows = self._connection.execute(
                _HNSW_SQL,
                (
                    literal,
                    self._manifest.claim_registry_snapshot_id,
                    self._manifest.embedding_model_artifact_id,
                    self._manifest.claim_role_template_hash,
                    literal,
                    limit,
                    limit,
                ),
            ).fetchall()
        return self._result(
            epoch_id=epoch_id,
            candidate_policy_id=candidate_policy_id,
            chunk=chunk,
            limit=limit,
            rows=rows,
        )

    def explain_search(self, chunk: ChunkRoleVector, *, limit: int) -> tuple[str, ...]:
        if len(chunk.vector) != self._dimensions or limit <= 0:
            raise ValidationError("EXPLAIN vector input is invalid")
        literal = _vector_literal(chunk.vector)
        with self._connection.transaction():
            self._install_search_settings()
            rows = self._connection.execute(
                "EXPLAIN (COSTS OFF) " + _HNSW_SQL,
                (
                    literal,
                    self._manifest.claim_registry_snapshot_id,
                    self._manifest.embedding_model_artifact_id,
                    self._manifest.claim_role_template_hash,
                    literal,
                    limit,
                    limit,
                ),
            ).fetchall()
        return tuple(str(row[0]) for row in rows)
