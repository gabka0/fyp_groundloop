"""Durable, conflict-detecting registries for M4 model artifacts.

The role-embedding rows are empirical model products.  This module makes their
identity, exact input, role, model revision, and stored vector replayable; it
does not make the embedding semantically exact.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from psycopg import Connection

from groundloop.ai.contracts import ModelArtifact, ModelTask, PromptArtifact
from groundloop.ai.embeddings.common import EMBEDDING_DIMENSION
from groundloop.ai.persistence import ChunkerArtifactRecord
from groundloop.errors import ArtifactConflictError, ValidationError
from groundloop.m4.contracts import CandidatePolicyManifest, sha256_text
from groundloop.m4.models.contracts import (
    CHUNK_ROLE_TEMPLATE,
    CLAIM_ROLE_TEMPLATE,
    ChunkVectorArtifact,
    ClaimVectorArtifact,
    EmbeddingRole,
    RoleEmbeddingProvenance,
    vector_sha256,
)

RoleVectorArtifact = ClaimVectorArtifact | ChunkVectorArtifact
_HEX = frozenset("0123456789abcdef")
_PROMPT_TASKS = frozenset(
    {
        ModelTask.GENERATION,
        ModelTask.CLAIM_EXTRACTION,
        ModelTask.VERIFICATION,
    }
)


def _require_text(name: str, value: str) -> None:
    if not value.strip():
        raise ValidationError(f"{name} must be non-empty")


def _require_sha256(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in _HEX for character in value):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


def _hash_value(value: object) -> str:
    """Return a fixed-width SQL hash without CHAR display padding."""
    return str(value).strip()


def _vector_literal(vector: tuple[float, ...]) -> str:
    if len(vector) != EMBEDDING_DIMENSION:
        raise ValidationError(
            "M4 durable role embeddings require exactly "
            f"{EMBEDDING_DIMENSION} dimensions"
        )
    return "[" + ",".join(format(value, ".17g") for value in vector) + "]"


def _role_template(role: EmbeddingRole) -> str:
    if role is EmbeddingRole.CLAIM_QUERY:
        return CLAIM_ROLE_TEMPLATE
    return CHUNK_ROLE_TEMPLATE


@dataclass(frozen=True, slots=True)
class ClaimAdmissionSeedResult:
    """Counts for one all-or-nothing claim-registry population."""

    inserted_role_artifacts: int
    reused_role_artifacts: int
    inserted_index_rows: int
    reused_index_rows: int

    @property
    def claim_count(self) -> int:
        return self.inserted_index_rows + self.reused_index_rows


class PostgresM4ArtifactRegistry:
    """Persist immutable M3/M4 artifacts with exact replay validation."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def register_model(self, artifact: ModelArtifact) -> bool:
        """Register a model artifact; return ``False`` for exact replay."""
        with self._connection.transaction():
            return self._register_model(artifact)

    def _register_model(self, artifact: ModelArtifact) -> bool:
        inserted = self._connection.execute(
            """
            INSERT INTO groundloop_model_artifact (
                model_artifact_id, task, provider, model_id,
                immutable_revision, tokenizer_revision, license_id,
                config_hash, artifact_sha256, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, '{}'::jsonb)
            ON CONFLICT DO NOTHING
            RETURNING true
            """,
            (
                artifact.artifact_id,
                artifact.task.value,
                artifact.provider,
                artifact.model_id,
                artifact.immutable_revision,
                artifact.tokenizer_revision,
                artifact.license_id,
                artifact.config_hash,
                artifact.artifact_sha256,
            ),
        ).fetchone()
        if inserted is not None:
            return True
        row = self._connection.execute(
            """
            SELECT task, provider, model_id, immutable_revision,
                   tokenizer_revision, license_id, config_hash,
                   artifact_sha256, metadata
            FROM groundloop_model_artifact
            WHERE model_artifact_id = %s
            """,
            (artifact.artifact_id,),
        ).fetchone()
        expected: tuple[object, ...] = (
            artifact.task.value,
            artifact.provider,
            artifact.model_id,
            artifact.immutable_revision,
            artifact.tokenizer_revision,
            artifact.license_id,
            artifact.config_hash,
            artifact.artifact_sha256,
            {},
        )
        actual = None
        if row is not None:
            actual = (
                str(row[0]),
                str(row[1]),
                str(row[2]),
                str(row[3]),
                str(row[4]),
                str(row[5]),
                _hash_value(row[6]),
                None if row[7] is None else _hash_value(row[7]),
                row[8],
            )
        if actual != expected:
            raise ArtifactConflictError(
                f"model artifact {artifact.artifact_id} payload conflict"
            )
        return False

    def register_prompt(self, artifact: PromptArtifact) -> bool:
        """Register a prompt artifact; return ``False`` for exact replay."""
        if artifact.task not in _PROMPT_TASKS:
            raise ValidationError("prompt artifact has a non-prompt model task")
        with self._connection.transaction():
            return self._register_prompt(artifact)

    def _register_prompt(self, artifact: PromptArtifact) -> bool:
        inserted = self._connection.execute(
            """
            INSERT INTO groundloop_prompt_artifact (
                prompt_artifact_id, task, version, template, template_hash,
                decoding_config_hash
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING true
            """,
            (
                artifact.artifact_id,
                artifact.task.value,
                artifact.version,
                artifact.template,
                artifact.template_hash,
                artifact.decoding_config_hash,
            ),
        ).fetchone()
        if inserted is not None:
            return True
        row = self._connection.execute(
            """
            SELECT task, version, template, template_hash,
                   decoding_config_hash
            FROM groundloop_prompt_artifact
            WHERE prompt_artifact_id = %s
            """,
            (artifact.artifact_id,),
        ).fetchone()
        expected = (
            artifact.task.value,
            artifact.version,
            artifact.template,
            artifact.template_hash,
            artifact.decoding_config_hash,
        )
        actual = None
        if row is not None:
            actual = (
                str(row[0]),
                str(row[1]),
                str(row[2]),
                _hash_value(row[3]),
                _hash_value(row[4]),
            )
        if actual != expected:
            raise ArtifactConflictError(
                f"prompt artifact {artifact.artifact_id} payload conflict"
            )
        return False

    def register_chunker(self, artifact: ChunkerArtifactRecord) -> bool:
        """Register a fixed chunker artifact with M3-compatible identity."""
        _require_text("chunker artifact_id", artifact.artifact_id)
        _require_text("chunker version", artifact.version)
        if artifact.normalization_version != "v1":
            raise ValidationError("chunker normalization version must be v1")
        _require_sha256("chunker config_hash", artifact.config_hash)
        with self._connection.transaction():
            return self._register_chunker(artifact)

    def _register_chunker(self, artifact: ChunkerArtifactRecord) -> bool:
        inserted = self._connection.execute(
            """
            INSERT INTO groundloop_chunker_artifact (
                chunker_artifact_id, chunker_version,
                normalization_version, config_hash
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING true
            """,
            (
                artifact.artifact_id,
                artifact.version,
                artifact.normalization_version,
                artifact.config_hash,
            ),
        ).fetchone()
        if inserted is not None:
            return True
        row = self._connection.execute(
            """
            SELECT chunker_version, normalization_version, config_hash
            FROM groundloop_chunker_artifact
            WHERE chunker_artifact_id = %s
            """,
            (artifact.artifact_id,),
        ).fetchone()
        expected = (
            artifact.version,
            artifact.normalization_version,
            artifact.config_hash,
        )
        actual = None
        if row is not None:
            actual = (str(row[0]), str(row[1]), _hash_value(row[2]))
        if actual != expected:
            raise ArtifactConflictError(
                f"chunker artifact {artifact.artifact_id} payload conflict"
            )
        return False

    def register_role_embedding(self, artifact: RoleVectorArtifact) -> bool:
        """Register one complete claim-query or chunk-passage artifact."""
        with self._connection.transaction():
            return self._register_role_embedding(artifact)

    def _validate_role_embedding(
        self, artifact: RoleVectorArtifact
    ) -> tuple[RoleEmbeddingProvenance, tuple[float, ...], str]:
        provenance = artifact.provenance
        vector = artifact.vector.vector
        literal = _vector_literal(vector)
        for name, value in (
            ("role template hash", provenance.role_template_hash),
            ("embedding input hash", provenance.input_hash),
            ("embedding vector hash", provenance.vector_hash),
            ("embedding adapter spec hash", provenance.adapter_spec_hash),
        ):
            _require_sha256(name, value)
        for name, value in (
            ("embedding model artifact", provenance.model_artifact_id),
            ("embedding model_id", provenance.model_id),
            ("embedding model revision", provenance.model_revision),
            ("embedding tokenizer revision", provenance.tokenizer_revision),
        ):
            _require_text(name, value)
        if vector_sha256(vector) != provenance.vector_hash:
            raise ArtifactConflictError("role embedding vector hash drift")

        template = _role_template(provenance.role)
        if provenance.role_template_hash != sha256_text(template):
            raise ArtifactConflictError("role embedding template drift")
        if provenance.role is EmbeddingRole.CLAIM_QUERY:
            subject_row = self._connection.execute(
                "SELECT text FROM groundloop_claim WHERE claim_id = %s",
                (provenance.subject_id,),
            ).fetchone()
        else:
            subject_row = self._connection.execute(
                """
                SELECT text FROM groundloop_chunk_version
                WHERE chunk_version_id = %s
                """,
                (provenance.subject_id,),
            ).fetchone()
        if subject_row is None:
            raise ValidationError("role embedding subject is not registered")
        expected_input_hash = sha256_text(template.format(text=str(subject_row[0])))
        if provenance.input_hash != expected_input_hash:
            raise ArtifactConflictError(
                "role embedding input differs from stored subject text"
            )

        model_row = self._connection.execute(
            """
            SELECT task, model_id, immutable_revision, tokenizer_revision
            FROM groundloop_model_artifact
            WHERE model_artifact_id = %s
            """,
            (provenance.model_artifact_id,),
        ).fetchone()
        if model_row is None:
            raise ValidationError("embedding model artifact is not registered")
        model_identity = (
            str(model_row[0]),
            str(model_row[1]),
            str(model_row[2]),
            str(model_row[3]),
        )
        expected_model_identity = (
            ModelTask.EMBEDDING.value,
            provenance.model_id,
            provenance.model_revision,
            provenance.tokenizer_revision,
        )
        if model_identity != expected_model_identity:
            raise ArtifactConflictError(
                "role embedding model provenance differs from registry"
            )
        return provenance, vector, literal

    def _register_role_embedding(self, artifact: RoleVectorArtifact) -> bool:
        provenance, _vector, literal = self._validate_role_embedding(artifact)
        claim_id = (
            provenance.subject_id
            if provenance.role is EmbeddingRole.CLAIM_QUERY
            else None
        )
        chunk_id = (
            provenance.subject_id
            if provenance.role is EmbeddingRole.CHUNK_PASSAGE
            else None
        )
        inserted = self._connection.execute(
            """
            INSERT INTO groundloop_m4_role_embedding_artifact (
                artifact_id, subject_id, embedding_role, claim_id,
                chunk_version_id, model_artifact_id, model_id,
                model_revision, tokenizer_revision, role_template_hash,
                input_hash, vector_hash, adapter_spec_hash, token_count,
                max_tokens, truncated, embedding
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s::vector
            )
            ON CONFLICT DO NOTHING
            RETURNING true
            """,
            (
                provenance.artifact_id,
                provenance.subject_id,
                provenance.role.value,
                claim_id,
                chunk_id,
                provenance.model_artifact_id,
                provenance.model_id,
                provenance.model_revision,
                provenance.tokenizer_revision,
                provenance.role_template_hash,
                provenance.input_hash,
                provenance.vector_hash,
                provenance.adapter_spec_hash,
                provenance.token_count,
                provenance.max_tokens,
                provenance.truncated,
                literal,
            ),
        ).fetchone()
        if inserted is not None:
            return True

        row = self._connection.execute(
            """
            SELECT subject_id, embedding_role, claim_id, chunk_version_id,
                   model_artifact_id, model_id, model_revision,
                   tokenizer_revision, role_template_hash, input_hash,
                   vector_hash, adapter_spec_hash, token_count, max_tokens,
                   truncated, embedding = %s::vector
            FROM groundloop_m4_role_embedding_artifact
            WHERE artifact_id = %s
            """,
            (literal, provenance.artifact_id),
        ).fetchone()
        expected = (
            provenance.subject_id,
            provenance.role.value,
            claim_id,
            chunk_id,
            provenance.model_artifact_id,
            provenance.model_id,
            provenance.model_revision,
            provenance.tokenizer_revision,
            provenance.role_template_hash,
            provenance.input_hash,
            provenance.vector_hash,
            provenance.adapter_spec_hash,
            provenance.token_count,
            provenance.max_tokens,
            provenance.truncated,
            True,
        )
        actual = None
        if row is not None:
            actual = (
                str(row[0]),
                str(row[1]),
                None if row[2] is None else str(row[2]),
                None if row[3] is None else str(row[3]),
                str(row[4]),
                str(row[5]),
                str(row[6]),
                str(row[7]),
                _hash_value(row[8]),
                _hash_value(row[9]),
                _hash_value(row[10]),
                _hash_value(row[11]),
                None if row[12] is None else int(row[12]),
                int(row[13]),
                bool(row[14]),
                bool(row[15]),
            )
        if actual != expected:
            raise ArtifactConflictError(
                f"role embedding {provenance.artifact_id} payload conflict"
            )
        return False

    def seed_claim_admission_index(
        self,
        *,
        manifest: CandidatePolicyManifest,
        artifacts: Sequence[ClaimVectorArtifact],
        claim_texts: Mapping[str, str],
    ) -> ClaimAdmissionSeedResult:
        """Atomically register claim vectors and seed one policy snapshot."""
        ordered = tuple(sorted(artifacts, key=lambda item: item.vector.claim_id))
        claim_ids = tuple(item.vector.claim_id for item in ordered)
        if claim_ids != tuple(sorted(set(claim_ids))):
            raise ValidationError("claim admission artifacts must be unique")
        if len(ordered) != manifest.claim_count:
            raise ValidationError("claim artifact count differs from manifest")
        if set(claim_texts) != set(claim_ids):
            raise ValidationError("claim text registry differs from artifact IDs")
        if manifest.claim_role_template_hash != sha256_text(CLAIM_ROLE_TEMPLATE):
            raise ArtifactConflictError("candidate policy claim role template drift")
        regconfig = manifest.lexical_regconfig_identity.rsplit(".", 1)[-1]
        if regconfig != "simple":
            raise ValidationError("lexical-v1 claim seeding requires simple")

        inserted_artifacts = 0
        reused_artifacts = 0
        inserted_rows = 0
        reused_rows = 0
        with self._connection.transaction():
            stored_texts: dict[str, str] = {}
            if claim_ids:
                stored_texts = {
                    str(claim_id): str(text)
                    for claim_id, text in self._connection.execute(
                        """
                        SELECT claim_id, text FROM groundloop_claim
                        WHERE claim_id = ANY(%s)
                        """,
                        (list(claim_ids),),
                    ).fetchall()
                }
            if set(stored_texts) != set(claim_ids):
                raise ValidationError("claim admission subject is not registered")

            for artifact in ordered:
                claim_id = artifact.vector.claim_id
                claim_text = claim_texts[claim_id]
                if stored_texts[claim_id] != claim_text:
                    raise ArtifactConflictError(
                        "claim text differs from immutable registry content"
                    )
                provenance = artifact.provenance
                if (
                    provenance.model_artifact_id
                    != manifest.embedding_model_artifact_id
                ):
                    raise ArtifactConflictError(
                        "claim artifact uses another embedding model"
                    )
                if (
                    provenance.role_template_hash
                    != manifest.claim_role_template_hash
                ):
                    raise ArtifactConflictError(
                        "claim artifact uses another role template"
                    )
                expected_input_hash = sha256_text(
                    CLAIM_ROLE_TEMPLATE.format(text=claim_text)
                )
                if provenance.input_hash != expected_input_hash:
                    raise ArtifactConflictError(
                        "claim artifact input differs from registered text"
                    )
                created = self._register_role_embedding(artifact)
                if created:
                    inserted_artifacts += 1
                else:
                    reused_artifacts += 1
                index_created = self._insert_claim_admission_row(
                    manifest=manifest,
                    artifact=artifact,
                    claim_text=claim_text,
                )
                if index_created:
                    inserted_rows += 1
                else:
                    reused_rows += 1

        return ClaimAdmissionSeedResult(
            inserted_role_artifacts=inserted_artifacts,
            reused_role_artifacts=reused_artifacts,
            inserted_index_rows=inserted_rows,
            reused_index_rows=reused_rows,
        )

    def _insert_claim_admission_row(
        self,
        *,
        manifest: CandidatePolicyManifest,
        artifact: ClaimVectorArtifact,
        claim_text: str,
    ) -> bool:
        provenance = artifact.provenance
        literal = _vector_literal(artifact.vector.vector)
        inserted = self._connection.execute(
            """
            INSERT INTO groundloop_m4_claim_admission_index (
                claim_registry_snapshot_id, claim_id,
                embedding_model_artifact_id, claim_role_template_hash,
                embedding_input_hash, embedding, lexical_tsv
            ) VALUES (
                %s, %s, %s, %s, %s, %s::vector,
                to_tsvector(%s::regconfig, %s)
            )
            ON CONFLICT DO NOTHING
            RETURNING true
            """,
            (
                manifest.claim_registry_snapshot_id,
                artifact.vector.claim_id,
                provenance.model_artifact_id,
                provenance.role_template_hash,
                provenance.input_hash,
                literal,
                manifest.lexical_regconfig_identity,
                claim_text,
            ),
        ).fetchone()
        if inserted is not None:
            return True
        row = self._connection.execute(
            """
            SELECT embedding_model_artifact_id = %s,
                   claim_role_template_hash = %s,
                   embedding_input_hash = %s,
                   embedding = %s::vector,
                   lexical_tsv = to_tsvector(%s::regconfig, %s)
            FROM groundloop_m4_claim_admission_index
            WHERE claim_registry_snapshot_id = %s AND claim_id = %s
            """,
            (
                provenance.model_artifact_id,
                provenance.role_template_hash,
                provenance.input_hash,
                literal,
                manifest.lexical_regconfig_identity,
                claim_text,
                manifest.claim_registry_snapshot_id,
                artifact.vector.claim_id,
            ),
        ).fetchone()
        if row != (True, True, True, True, True):
            raise ArtifactConflictError(
                "claim admission index key was reused with different content"
            )
        return False
