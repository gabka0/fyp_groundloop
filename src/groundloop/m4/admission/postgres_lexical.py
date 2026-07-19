"""Real PostgreSQL lexical-v1 analyzer and ranked-search adapters."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from psycopg import Connection

from groundloop.errors import ValidationError
from groundloop.m4.admission.lexical import LexicalRawHit, LexicalRegistrySnapshot
from groundloop.m4.admission.postgres_common import (
    CLAIM_ADMISSION_RELATION,
    PostgresAdmissionServerIdentity,
)
from groundloop.m4.contracts import CandidatePolicyManifest, stable_m4_digest

_ANALYZE_SQL = """
SELECT tsvector_to_array(to_tsvector(%s::regconfig, %s))
"""

_BUILD_TSQUERY_SQL = """
SELECT to_tsquery(
    %s::regconfig,
    string_agg(quote_literal(term), ' | ' ORDER BY ordinal)
)::text
FROM unnest(%s::text[]) WITH ORDINALITY AS selected(term, ordinal)
"""

_SEARCH_SQL = """
SELECT claims.claim_id,
       ts_rank_cd(claims.lexical_tsv, %s::tsquery, %s) AS score
FROM groundloop_m4_claim_admission_index AS claims
WHERE claims.claim_registry_snapshot_id = %s
  AND claims.lexical_tsv @@ %s::tsquery
ORDER BY score DESC, claims.claim_id ASC
LIMIT %s
"""


@dataclass(slots=True)
class PostgresSimpleLexemeAnalyzer:
    """Use the server's real `to_tsvector('simple', text)` parser."""

    connection: Connection[Any]
    server: PostgresAdmissionServerIdentity
    regconfig: str = "simple"

    def __post_init__(self) -> None:
        runtime = self.server.regconfig_identity.rsplit(".", 1)[-1]
        if self.regconfig != "simple" or runtime != self.regconfig:
            raise ValidationError("lexical-v1 analyzer requires runtime simple")

    @property
    def artifact_id(self) -> str:
        return stable_m4_digest(
            "m4-postgres-simple-analyzer-v1",
            self.server.artifact_hash,
            self.regconfig,
        )

    def analyze(self, text: str) -> tuple[str, ...]:
        row = self.connection.execute(_ANALYZE_SQL, (self.regconfig, text)).fetchone()
        if row is None:
            raise ValidationError("PostgreSQL lexical analyzer returned no row")
        raw = row[0]
        if raw is None:
            return ()
        lexemes = tuple(str(item) for item in raw)
        if any(not item.strip() for item in lexemes):
            raise ValidationError("PostgreSQL analyzer emitted an empty lexeme")
        return tuple(sorted(set(lexemes)))


class PostgresLexicalSearchBackend:
    """Parameterized OR tsquery and `ts_rank_cd(..., 32)` over claim tsvectors."""

    def __init__(
        self,
        *,
        connection: Connection[Any],
        server: PostgresAdmissionServerIdentity,
        manifest: CandidatePolicyManifest,
    ) -> None:
        server.validate_lexical_manifest(
            postgres_version=manifest.lexical_postgres_version,
            regconfig_identity=manifest.lexical_regconfig_identity,
        )
        self._connection = connection
        self._server = server
        self._manifest = manifest

    @property
    def artifact_id(self) -> str:
        return stable_m4_digest(
            "m4-postgres-lexical-backend-v1",
            self._server.artifact_hash,
            CLAIM_ADMISSION_RELATION,
            self._manifest.claim_registry_snapshot_id,
            str(self._manifest.claim_count),
            self._manifest.lexical_config_hash,
        )

    def validate_registry(self, registry: LexicalRegistrySnapshot) -> None:
        if registry.snapshot_id != self._manifest.claim_registry_snapshot_id:
            raise ValidationError("lexical registry snapshot differs from manifest")
        if registry.claim_count != self._manifest.claim_count:
            raise ValidationError("lexical registry count differs from manifest")
        rows = self._connection.execute(
            """
            SELECT claim_id, tsvector_to_array(lexical_tsv)
            FROM groundloop_m4_claim_admission_index
            WHERE claim_registry_snapshot_id = %s
            ORDER BY claim_id
            """,
            (self._manifest.claim_registry_snapshot_id,),
        ).fetchall()
        actual = tuple(
            (str(claim_id), tuple(sorted(str(item) for item in lexemes)))
            for claim_id, lexemes in rows
        )
        if actual != registry.claim_lexemes:
            raise ValidationError("PostgreSQL lexical registry content differs")

    def search(
        self,
        *,
        query_lexemes: tuple[str, ...],
        limit: int,
        normalization_mask: int,
    ) -> tuple[LexicalRawHit, ...]:
        if not query_lexemes:
            return ()
        if query_lexemes != tuple(dict.fromkeys(query_lexemes)):
            raise ValidationError("lexical query lexemes must be distinct")
        if limit <= 0:
            raise ValidationError("lexical result limit must be positive")
        if normalization_mask != 32:
            raise ValidationError("lexical-v1 requires normalization mask 32")
        query_row = self._connection.execute(
            _BUILD_TSQUERY_SQL,
            (self._manifest.lexical_regconfig_identity, list(query_lexemes)),
        ).fetchone()
        if query_row is None or query_row[0] is None:
            return ()
        query = str(query_row[0])
        rows = self._connection.execute(
            _SEARCH_SQL,
            (
                query,
                normalization_mask,
                self._manifest.claim_registry_snapshot_id,
                query,
                limit,
            ),
        ).fetchall()
        hits = tuple(
            LexicalRawHit(str(claim_id), float(score)) for claim_id, score in rows
        )
        if any(not math.isfinite(hit.score) for hit in hits):
            raise ValidationError("PostgreSQL lexical search returned non-finite score")
        expected = tuple(sorted(hits, key=lambda hit: (-hit.score, hit.claim_id)))
        if hits != expected:
            raise ValidationError("PostgreSQL lexical results violate frozen order")
        return hits

    def explain_search(
        self,
        *,
        query_lexemes: tuple[str, ...],
        limit: int,
    ) -> tuple[str, ...]:
        if not query_lexemes or limit <= 0:
            raise ValidationError(
                "EXPLAIN requires nonempty lexemes and positive limit"
            )
        query_row = self._connection.execute(
            _BUILD_TSQUERY_SQL,
            (self._manifest.lexical_regconfig_identity, list(query_lexemes)),
        ).fetchone()
        if query_row is None or query_row[0] is None:
            return ()
        query = str(query_row[0])
        rows = self._connection.execute(
            "EXPLAIN (COSTS OFF) " + _SEARCH_SQL,
            (
                query,
                32,
                self._manifest.claim_registry_snapshot_id,
                query,
                limit,
            ),
        ).fetchall()
        return tuple(str(row[0]) for row in rows)
