"""Shared PostgreSQL identity checks for M4 admission adapters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg import Connection

from groundloop.errors import ValidationError
from groundloop.m4.contracts import stable_m4_digest

CLAIM_ADMISSION_RELATION = "groundloop_m4_claim_admission_index"
CLAIM_ADMISSION_HNSW_INDEX = "groundloop_m4_claim_admission_hnsw"


@dataclass(frozen=True, slots=True)
class PostgresAdmissionServerIdentity:
    """Runtime identities that can change PostgreSQL admission results."""

    postgres_version: str
    pgvector_version: str
    regconfig_identity: str

    def __post_init__(self) -> None:
        if not self.postgres_version.strip():
            raise ValidationError("PostgreSQL version must be non-empty")
        if not self.pgvector_version.strip():
            raise ValidationError("pgvector version must be non-empty")
        if not self.regconfig_identity.strip():
            raise ValidationError("PostgreSQL regconfig identity must be non-empty")

    @property
    def artifact_hash(self) -> str:
        return stable_m4_digest(
            "m4-postgres-admission-server-v1",
            self.postgres_version,
            self.pgvector_version,
            self.regconfig_identity,
        )

    @classmethod
    def inspect(
        cls,
        connection: Connection[Any],
        *,
        regconfig: str = "simple",
    ) -> PostgresAdmissionServerIdentity:
        if regconfig != "simple":
            raise ValidationError("M4 lexical-v1 requires PostgreSQL simple")
        row = connection.execute(
            """
            SELECT current_setting('server_version'),
                   (SELECT extversion FROM pg_extension WHERE extname = 'vector'),
                   (%s::regconfig)::text
            """,
            (regconfig,),
        ).fetchone()
        if row is None or row[1] is None:
            raise ValidationError("the pgvector extension must be installed")
        return cls(str(row[0]), str(row[1]), str(row[2]))

    def validate_lexical_manifest(
        self,
        *,
        postgres_version: str,
        regconfig_identity: str,
    ) -> None:
        if postgres_version != self.postgres_version:
            raise ValidationError("manifest PostgreSQL version differs from runtime")
        runtime_regconfig = self.regconfig_identity.rsplit(".", 1)[-1]
        manifest_regconfig = regconfig_identity.rsplit(".", 1)[-1]
        if manifest_regconfig != runtime_regconfig:
            raise ValidationError("manifest regconfig differs from PostgreSQL runtime")
